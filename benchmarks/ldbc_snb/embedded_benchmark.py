#!/usr/bin/env python3
"""LDBC SNB Benchmark Suite for ClickGraph Embedded Mode (chdb).

Loads sf0.003 Parquet data into chdb via the embedded schema, then runs
all LDBC Interactive and BI queries, reporting timing and pass/fail status.

Run:
    cd benchmarks/ldbc_snb
    LD_LIBRARY_PATH=../../target/release PYTHONPATH=../../clickgraph-py \
        python3 embedded_benchmark.py

Options:
    --sql-only      Translate Cypher to SQL without executing (no chdb needed)
    --filter PATTERN  Only run queries matching PATTERN (e.g. "short", "complex-1")
    --verbose       Print generated SQL and full error messages
"""

import argparse
import json
import os
import re
import sys
import tempfile
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data" / "sf0.003" / "graphs" / "parquet" / "raw" / "composite-merged-fk"
SCHEMA_TEMPLATE = SCRIPT_DIR / "schemas" / "ldbc_snb_embedded.yaml"
INTERACTIVE_QUERIES_DIR = SCRIPT_DIR / "queries" / "official" / "interactive"
BI_QUERIES_DIR = SCRIPT_DIR / "queries" / "official" / "bi"
ADAPTED_QUERIES_DIR = SCRIPT_DIR / "queries" / "adapted"

# Queries that have adapted versions (filename mapping).
# The adapted directory uses different naming conventions.
ADAPTED_QUERIES = {
    "bi-17": "bi-17.cypher",
    "complex-14": "interactive-complex-14.cypher",
}

# Queries known to be unsupported or expected to fail.
# bi-16 requires CALL subquery (language feature gap).
# bi-10, bi-15, bi-19, bi-20 have known issues.
EXPECTED_FAILURES = {
    "bi-10": "Requires features not yet supported",
    "bi-15": "Requires features not yet supported",
    "bi-16": "Requires CALL subquery (language feature gap)",
    "bi-19": "Requires features not yet supported",
    "bi-20": "Requires features not yet supported",
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def prepare_schema(data_dir: str) -> str:
    """Read the embedded schema YAML and replace __DATA_DIR__ placeholder.

    Returns the path to a temporary YAML file with resolved paths.
    """
    template_text = SCHEMA_TEMPLATE.read_text()
    resolved = template_text.replace("__DATA_DIR__", data_dir)

    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", prefix="ldbc_embedded_", delete=False
    )
    tmp.write(resolved)
    tmp.flush()
    tmp.close()
    return tmp.name


def load_query(query_id: str) -> tuple:
    """Load a query file and its parameters.

    Returns (cypher_text, params_dict, query_source_label).
    Checks adapted directory first for overrides.
    """
    # Check for adapted version
    if query_id in ADAPTED_QUERIES:
        adapted_file = ADAPTED_QUERIES_DIR / ADAPTED_QUERIES[query_id]
        if adapted_file.exists():
            cypher = adapted_file.read_text()
            # Load params from the official location
            params = _load_params(query_id)
            return cypher, params, "adapted"

    # Load from official directory
    if query_id.startswith("bi-"):
        query_dir = BI_QUERIES_DIR
    else:
        query_dir = INTERACTIVE_QUERIES_DIR

    cypher_file = query_dir / f"{query_id}.cypher"
    if not cypher_file.exists():
        raise FileNotFoundError(f"Query file not found: {cypher_file}")

    cypher = cypher_file.read_text()
    params = _load_params(query_id)
    return cypher, params, "official"


def _load_params(query_id: str) -> dict:
    """Load parameters JSON for a query."""
    if query_id.startswith("bi-"):
        params_file = BI_QUERIES_DIR / f"{query_id}.params.json"
    else:
        params_file = INTERACTIVE_QUERIES_DIR / f"{query_id}.params.json"

    if params_file.exists():
        return json.loads(params_file.read_text())
    return {}


def substitute_params(cypher: str, params: dict) -> str:
    """Remove comment blocks and substitute $paramName with actual values."""
    # Remove /* :params ... */ comment blocks
    cypher = re.sub(r"/\*.*?\*/", "", cypher, flags=re.DOTALL).strip()
    # Remove // line comments
    cypher = re.sub(r"//[^\n]*\n", "\n", cypher).strip()

    for key, value in params.items():
        if isinstance(value, str):
            cypher = cypher.replace(f"${key}", f"'{value}'")
        elif isinstance(value, list):
            # Format list values: [1, 2, 3] or ['a', 'b']
            formatted_items = []
            for item in value:
                if isinstance(item, str):
                    formatted_items.append(f"'{item}'")
                else:
                    formatted_items.append(str(item))
            cypher = cypher.replace(f"${key}", "[" + ", ".join(formatted_items) + "]")
        else:
            cypher = cypher.replace(f"${key}", str(value))

    return cypher


def collect_query_ids() -> list:
    """Collect all query IDs in execution order."""
    ids = []

    # Short queries (short-1 through short-7)
    for i in range(1, 8):
        ids.append(f"short-{i}")

    # Complex queries (complex-1 through complex-14)
    for i in range(1, 15):
        ids.append(f"complex-{i}")

    # BI queries (bi-1 through bi-20)
    for i in range(1, 21):
        ids.append(f"bi-{i}")

    return ids


# ---------------------------------------------------------------------------
# Result tracking
# ---------------------------------------------------------------------------


class QueryResult:
    """Stores the outcome of a single query execution."""

    def __init__(self, query_id: str):
        self.query_id = query_id
        self.status = "NOT_RUN"  # PASS, FAIL, ERROR, SKIP, NOT_RUN
        self.compile_time_ms = 0.0
        self.exec_time_ms = 0.0
        self.row_count = 0
        self.error_message = ""
        self.source = ""  # official or adapted
        self.sql = ""


# ---------------------------------------------------------------------------
# Main benchmark
# ---------------------------------------------------------------------------


def run_benchmark(args):
    """Execute the full benchmark suite."""
    # Validate data directory
    if not DATA_DIR.exists():
        print(f"ERROR: Data directory not found: {DATA_DIR}")
        print("       Download sf0.003 data first.")
        sys.exit(1)

    if not SCHEMA_TEMPLATE.exists():
        print(f"ERROR: Schema template not found: {SCHEMA_TEMPLATE}")
        sys.exit(1)

    # Prepare schema with resolved paths
    data_dir_str = str(DATA_DIR)
    schema_path = prepare_schema(data_dir_str)
    print(f"Schema prepared: {schema_path}")
    print(f"Data directory:  {data_dir_str}")
    print()

    # Import clickgraph
    try:
        import clickgraph
    except ImportError as e:
        print(f"ERROR: Cannot import clickgraph: {e}")
        print("       Set LD_LIBRARY_PATH and PYTHONPATH correctly.")
        print("       Example:")
        print("         LD_LIBRARY_PATH=../../target/release "
              "PYTHONPATH=../../clickgraph-py python3 embedded_benchmark.py")
        sys.exit(1)

    # Create database and connection
    print("Initializing embedded database...")
    t0 = time.time()
    try:
        if args.sql_only:
            db = clickgraph.Database.sql_only(schema_path)
        else:
            db = clickgraph.Database(schema_path)
        conn = db.connect()
    except Exception as e:
        print(f"ERROR: Failed to initialize database: {e}")
        sys.exit(1)
    init_time = time.time() - t0
    print(f"Database initialized in {init_time:.2f}s")
    print()

    # Collect and filter queries
    all_query_ids = collect_query_ids()
    if args.filter:
        all_query_ids = [
            qid for qid in all_query_ids if args.filter in qid
        ]
        print(f"Filter '{args.filter}' matched {len(all_query_ids)} queries")
        print()

    # Run queries
    results = []
    print(f"{'Query':<14} {'Status':<8} {'Compile':>10} {'Execute':>10} "
          f"{'Rows':>6}  {'Source':<10} Notes")
    print("-" * 90)

    for query_id in all_query_ids:
        result = QueryResult(query_id)

        # Skip expected failures
        if query_id in EXPECTED_FAILURES:
            result.status = "SKIP"
            result.error_message = EXPECTED_FAILURES[query_id]
            results.append(result)
            print(f"{query_id:<14} {'SKIP':<8} {'--':>10} {'--':>10} "
                  f"{'--':>6}  {'--':<10} {result.error_message}")
            continue

        # Load query
        try:
            cypher, params, source = load_query(query_id)
            result.source = source
        except FileNotFoundError as e:
            result.status = "ERROR"
            result.error_message = str(e)
            results.append(result)
            print(f"{query_id:<14} {'ERROR':<8} {'--':>10} {'--':>10} "
                  f"{'--':>6}  {'--':<10} File not found")
            continue

        # Substitute parameters
        cypher_resolved = substitute_params(cypher, params)

        if args.verbose:
            print(f"\n--- {query_id} (Cypher) ---")
            print(cypher_resolved[:500])
            if len(cypher_resolved) > 500:
                print(f"... ({len(cypher_resolved)} chars total)")

        # Execute
        try:
            if args.sql_only:
                t_start = time.time()
                sql = conn.query_to_sql(cypher_resolved)
                t_end = time.time()
                result.compile_time_ms = (t_end - t_start) * 1000
                result.sql = sql
                result.status = "PASS"
                result.row_count = 0
                if args.verbose:
                    print(f"\n--- {query_id} (SQL) ---")
                    print(sql[:1000])
            else:
                t_start = time.time()
                qr = conn.query(cypher_resolved)
                t_end = time.time()
                wall_ms = (t_end - t_start) * 1000
                # Try native timing; fall back to wall clock if bindings are stale
                try:
                    result.compile_time_ms = qr.compiling_time
                    result.exec_time_ms = qr.execution_time
                except AttributeError:
                    result.compile_time_ms = wall_ms
                    result.exec_time_ms = 0.0
                result.row_count = len(list(qr))
                result.status = "PASS"
        except Exception as e:
            result.status = "FAIL"
            result.error_message = str(e)
            if args.verbose:
                print(f"\n--- {query_id} (ERROR) ---")
                print(str(e)[:500])

        results.append(result)

        # Print row
        compile_str = f"{result.compile_time_ms:.1f}ms" if result.compile_time_ms else "--"
        exec_str = f"{result.exec_time_ms:.1f}ms" if result.exec_time_ms else "--"
        rows_str = str(result.row_count) if result.status == "PASS" else "--"
        notes = ""
        if result.status == "FAIL":
            # Truncate error for display
            notes = result.error_message[:60]
        print(f"{query_id:<14} {result.status:<8} {compile_str:>10} {exec_str:>10} "
              f"{rows_str:>6}  {result.source:<10} {notes}")

    # Summary
    print()
    print("=" * 90)
    print("SUMMARY")
    print("=" * 90)

    passed = sum(1 for r in results if r.status == "PASS")
    failed = sum(1 for r in results if r.status == "FAIL")
    errors = sum(1 for r in results if r.status == "ERROR")
    skipped = sum(1 for r in results if r.status == "SKIP")
    total = len(results)

    print(f"Total:   {total}")
    print(f"Passed:  {passed}")
    print(f"Failed:  {failed}")
    print(f"Errors:  {errors}")
    print(f"Skipped: {skipped}")
    print()

    if passed > 0:
        pass_results = [r for r in results if r.status == "PASS"]
        compile_times = [r.compile_time_ms for r in pass_results if r.compile_time_ms > 0]
        exec_times = [r.exec_time_ms for r in pass_results if r.exec_time_ms > 0]

        if compile_times:
            print(f"Compile time:  avg={sum(compile_times)/len(compile_times):.1f}ms  "
                  f"min={min(compile_times):.1f}ms  max={max(compile_times):.1f}ms  "
                  f"total={sum(compile_times):.1f}ms")
        if exec_times:
            print(f"Execute time:  avg={sum(exec_times)/len(exec_times):.1f}ms  "
                  f"min={min(exec_times):.1f}ms  max={max(exec_times):.1f}ms  "
                  f"total={sum(exec_times):.1f}ms")

    if failed > 0:
        print()
        print("FAILED QUERIES:")
        for r in results:
            if r.status == "FAIL":
                print(f"  {r.query_id}: {r.error_message[:120]}")

    # Clean up temp schema file
    try:
        os.unlink(schema_path)
    except OSError:
        pass

    print()
    print(f"Result: {passed}/{total - skipped} passed "
          f"({skipped} skipped, {failed} failed, {errors} errors)")

    # Exit code: 0 if all non-skipped queries passed
    return 0 if (failed + errors) == 0 else 1


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="LDBC SNB Benchmark for ClickGraph Embedded Mode"
    )
    parser.add_argument(
        "--sql-only",
        action="store_true",
        help="Translate Cypher to SQL without executing (no chdb needed)",
    )
    parser.add_argument(
        "--filter",
        type=str,
        default=None,
        help="Only run queries matching this pattern (e.g. 'short', 'complex-1')",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print generated SQL and full error messages",
    )
    args = parser.parse_args()
    sys.exit(run_benchmark(args))


if __name__ == "__main__":
    main()
