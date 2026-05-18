"""End-to-end smoke: Cypher → cg (databricks dialect) → Spark SQL → Delta tables.

Skipped unless CLICKGRAPH_SPARK_TESTS=1. Requires:
  - `cg` binary built with `--features databricks`
    (env CG_BIN overrides path; resolved via `cargo metadata`)
  - Docker daemon, image pullable (default pinned via DEFAULT_IMAGE below;
    override with CG_SPARK_IMAGE)
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Optional

import pytest

# Pinned tag rather than `:latest` so upstream image updates can't silently
# change the Spark/Delta versions under the GA correctness gate. Bump
# deliberately when validating against a newer image.
DEFAULT_IMAGE = "deltaio/delta-docker:4.1.0"

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA = REPO_ROOT / "benchmarks" / "ldbc_snb" / "schemas" / "ldbc_snb_complete.yaml"
HARNESS_DIR = Path(__file__).parent
IMAGE = os.environ.get("CG_SPARK_IMAGE", DEFAULT_IMAGE)


def _resolve_cg_bin() -> Optional[Path]:
    """CG_BIN env override → cargo metadata target_directory → repo-relative target."""
    env_override = os.environ.get("CG_BIN")
    if env_override:
        return Path(env_override)
    try:
        meta = subprocess.run(
            ["cargo", "metadata", "--no-deps", "--format-version", "1"],
            cwd=REPO_ROOT, capture_output=True, text=True, check=True, timeout=30,
        )
        target_dir = Path(json.loads(meta.stdout)["target_directory"])
        candidate = target_dir / "release" / "cg"
        if candidate.exists():
            return candidate
    except (subprocess.SubprocessError, json.JSONDecodeError, KeyError, OSError):
        pass
    fallback = REPO_ROOT / "target" / "release" / "cg"
    return fallback if fallback.exists() else None


def _require_env() -> Path:
    if os.environ.get("CLICKGRAPH_SPARK_TESTS") != "1":
        pytest.skip("CLICKGRAPH_SPARK_TESTS=1 not set")
    cg = _resolve_cg_bin()
    if cg is None:
        pytest.skip("cg binary not found — build with `cargo build --release -p clickgraph-tool --features databricks`")
    if shutil.which("docker") is None:
        pytest.skip("docker not on PATH")
    return cg


def _generate_sql(cg_bin: Path, cypher: str) -> str:
    """Invoke cg to translate a Cypher query into Spark SQL."""
    result = subprocess.run(
        [str(cg_bin), "--schema", str(SCHEMA), "--dialect", "databricks", "sql", cypher],
        capture_output=True, text=True, check=True,
    )
    return result.stdout


def _run_in_container(sql: str) -> str:
    """Execute SQL against a freshly-seeded Delta warehouse inside the image."""
    result = subprocess.run(
        [
            "docker", "run", "--rm", "--entrypoint", "bash",
            "-v", f"{HARNESS_DIR}:/workspace:ro",
            "-e", f"SMOKE_SQL={sql}",
            IMAGE,
            "-c", "python3 /workspace/seed_and_query.py",
        ],
        capture_output=True, text=True, timeout=300,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"spark run failed (rc={result.returncode}):\nSTDERR:\n{result.stderr}\nSTDOUT:\n{result.stdout}"
        )
    return result.stdout


def _table_rows(output: str) -> list[list[str]]:
    """Parse spark `show()` ASCII table output into [[cell, ...], ...] (data rows only)."""
    rows: list[list[str]] = []
    in_table = False
    seen_header = False
    for line in output.splitlines():
        if re.match(r"^\+[-+]+\+$", line):
            in_table = True
            continue
        if in_table and line.startswith("|"):
            cells = [c.strip() for c in line.strip("|").split("|")]
            if not seen_header:
                seen_header = True
                continue
            rows.append(cells)
    return rows


def test_short1_flat_join():
    """Flat MATCH with inline id filter + IS_LOCATED_IN traversal.
    Mini dataset: Person id=1 (Alice) lives in Place id=7 (New_York)."""
    cg_bin = _require_env()
    cypher = (
        "MATCH (n:Person {id: 1})-[:IS_LOCATED_IN]->(p:City) "
        "RETURN n.firstName AS firstName, p.id AS cityId"
    )
    sql = _generate_sql(cg_bin, cypher)
    out = _run_in_container(sql)
    rows = _table_rows(out)
    assert rows == [["Alice", "7"]], f"unexpected rows: {rows}\n--- full output ---\n{out}"


def test_knows_vlp_recursive_cte():
    """Undirected KNOWS *1..2 → two recursive CTEs unioned. Stresses the
    biggest local-Spark unknown: recursive CTE on Delta.
    Mini dataset KNOWS edges (anchored at id=1): 1↔2, 1↔3, 2↔3, 3↔4, 4↔5.
    Reachable from 1 at 1..2 hops (excluding self): {2,3,4}."""
    cg_bin = _require_env()
    cypher = (
        "MATCH (p:Person {id: 1})-[:KNOWS*1..2]-(friend:Person) "
        "WHERE friend.id <> p.id "
        "RETURN DISTINCT friend.id AS friendId, friend.firstName AS firstName "
        "ORDER BY friendId"
    )
    sql = _generate_sql(cg_bin, cypher)
    out = _run_in_container(sql)
    rows = _table_rows(out)
    assert rows == [["2", "Bob"], ["3", "Carol"], ["4", "Dave"]], (
        f"unexpected rows: {rows}\n--- full output ---\n{out}"
    )


def test_collect_and_count_aggregation():
    """Exercises FunctionMapper: cypher `collect()` → Spark `collect_list()`,
    plus `count()` with GROUP BY on a directed KNOWS traversal.
    Anchor=1: KNOWS outgoing edges → 2 (Bob), 3 (Carol)."""
    cg_bin = _require_env()
    cypher = (
        "MATCH (p:Person {id: 1})-[:KNOWS]->(friend:Person) "
        "RETURN p.firstName AS anchor, count(friend) AS friendCount, "
        "collect(friend.firstName) AS friends"
    )
    sql = _generate_sql(cg_bin, cypher)
    assert "collect_list" in sql, f"expected collect→collect_list translation:\n{sql}"
    out = _run_in_container(sql)
    rows = _table_rows(out)
    assert len(rows) == 1, f"expected single aggregated row, got: {rows}"
    anchor, count, friends = rows[0]
    assert anchor == "Alice"
    assert count == "2"
    # collect_list order isn't guaranteed — compare as a set.
    parsed = {f.strip() for f in friends.strip("[]").split(",")}
    assert parsed == {"Bob", "Carol"}, f"unexpected friend set: {parsed}"


def test_optional_match_null_safe_filter():
    """OPTIONAL MATCH against a virtual-label target (:University, filter
    type='University') must emit LEFT JOIN with NULL-safe schema filter so
    persons without a STUDY_AT edge still appear.
    Mini dataset STUDY_AT: persons 1→MIT, 2→TU_Berlin, 3→TU_Berlin only;
    persons 4 (Dave) and 5 (Eve) have no STUDY_AT edge."""
    cg_bin = _require_env()
    cypher = (
        "MATCH (p:Person) OPTIONAL MATCH (p)-[:STUDY_AT]->(u:University) "
        "RETURN p.firstName AS firstName, u.name AS uniName ORDER BY p.id"
    )
    sql = _generate_sql(cg_bin, cypher)
    assert "LEFT JOIN" in sql, f"expected LEFT JOIN for OPTIONAL MATCH:\n{sql}"
    assert "IS NULL" in sql, f"expected NULL-safe schema filter:\n{sql}"
    out = _run_in_container(sql)
    rows = _table_rows(out)
    expected = [
        ["Alice", "MIT"],
        ["Bob", "TU_Berlin"],
        ["Carol", "TU_Berlin"],
        ["Dave", "NULL"],
        ["Eve", "NULL"],
    ]
    assert rows == expected, f"unexpected rows: {rows}\n--- full output ---\n{out}"


def test_string_functions_mapping():
    """`toUpper` → `upper`, `length` → `length`, `STARTS WITH` → `startsWith`.
    Only Alice (id=1) has a firstName starting with 'A'."""
    cg_bin = _require_env()
    cypher = (
        'MATCH (p:Person) WHERE p.firstName STARTS WITH "A" '
        "RETURN toUpper(p.firstName) AS upperName, length(p.firstName) AS nameLen "
        "ORDER BY p.id"
    )
    sql = _generate_sql(cg_bin, cypher)
    assert "upper(" in sql.lower(), f"expected upper() translation:\n{sql}"
    assert "startswith(" in sql.lower(), f"expected startsWith translation:\n{sql}"
    out = _run_in_container(sql)
    rows = _table_rows(out)
    assert rows == [["ALICE", "5"]], f"unexpected rows: {rows}\n--- full output ---\n{out}"
