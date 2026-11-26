"""Run the official LDBC SNB queries against a ClickGraph HTTP endpoint."""

from __future__ import annotations

import argparse
import http.client
import json
import platform
import re
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
QUERY_ROOT = ROOT / "queries" / "official"
RESULTS_DIR = ROOT / "results"

CATEGORY_ORDER = {"IS": 0, "IC": 1, "BI": 2}
QUERY_PATTERNS = (
    ("IS", "interactive/short-*.cypher"),
    ("IC", "interactive/complex-*.cypher"),
    ("BI", "bi/bi-*.cypher"),
)


class HarnessError(Exception):
    """A benchmark setup or protocol error."""


@dataclass(frozen=True)
class QueryCase:
    id: str
    category: str
    path: Path


def discover_queries(root: Path = QUERY_ROOT) -> list[QueryCase]:
    cases: list[QueryCase] = []
    for category, pattern in QUERY_PATTERNS:
        for path in root.glob(pattern):
            match = re.fullmatch(r"[a-z]+-(\d+)", path.stem)
            if match is None:
                continue
            cases.append(QueryCase(path.stem, category, path))
    cases.sort(key=lambda case: (CATEGORY_ORDER[case.category], int(case.id.rsplit("-", 1)[1])))
    return cases


def strip_comments(cypher: str) -> str:
    # shortcut: textual stripping, so "//" or "/*" inside a string literal is
    # not safe; upgrade to a tokenizer if an official query needs one.
    text = re.sub(r"/\*.*?\*/", "", cypher, flags=re.DOTALL)
    lines = [line for line in text.splitlines() if not line.lstrip().startswith("//")]
    return "\n".join(lines).strip()


def placeholders(cypher: str) -> set[str]:
    return set(re.findall(r"\$([A-Za-z_][A-Za-z0-9_]*)", strip_comments(cypher)))


def load_sidecar(case: QueryCase) -> dict[str, Any]:
    path = case.path.with_suffix(".params.json")
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise HarnessError(f"cannot read {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise HarnessError(f"{path} must contain a JSON object")
    return data


def load_overrides(path: Path | None) -> dict[str, dict[str, Any]]:
    if path is None:
        return {}
    try:
        data = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise HarnessError(f"cannot read {path}: {exc}") from exc
    if not isinstance(data, dict) or not all(
        isinstance(key, str) and isinstance(value, dict) for key, value in data.items()
    ):
        raise HarnessError(f"{path} must contain an object of query-id objects")
    return data


def resolve_params(
    case: QueryCase,
    overrides: dict[str, dict[str, Any]],
    global_params: dict[str, Any],
) -> dict[str, Any]:
    params = dict(load_sidecar(case))
    params.update(overrides.get(case.id, {}))
    params.update(global_params)
    return params


def validate_params(cypher: str, params: dict[str, Any]) -> None:
    missing = sorted(placeholders(cypher) - set(params))
    if missing:
        raise HarnessError(
            "missing parameter(s): " + ", ".join(f"${name}" for name in missing)
        )


def parse_param_overrides(values: list[str]) -> dict[str, Any]:
    overrides: dict[str, Any] = {}
    for item in values:
        if "=" not in item:
            raise HarnessError("--param must be KEY=VALUE")
        key, raw = item.split("=", 1)
        key = key.strip()
        if not key:
            raise HarnessError("--param must be KEY=VALUE")
        try:
            overrides[key] = json.loads(raw)
        except json.JSONDecodeError:
            overrides[key] = raw
    return overrides


@dataclass
class QueryResult:
    """One query's outcome.

    ``status`` is ``pass`` (ran and returned rows), ``empty`` (ran without an
    error but returned no rows) or ``fail`` (any error). A zero-row answer is
    kept apart from a pass because it can be a plan that cannot match the data,
    in which case its latency measures nothing.
    """

    id: str
    category: str
    status: str
    runs: int = 0
    rows: int | None = None
    min_ms: float | None = None
    median_ms: float | None = None
    max_ms: float | None = None
    error: str | None = None


def _endpoint(base_url: str, path: str) -> str:
    return base_url.rstrip("/") + path


def _post_json(url: str, payload: dict[str, Any], timeout: float) -> tuple[int, Any]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "User-Agent": "clickgraph-ldbc-harness/1",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")
        try:
            return exc.code, json.loads(body)
        except json.JSONDecodeError:
            return exc.code, body
    except urllib.error.URLError as exc:
        raise HarnessError(f"connection error: {exc.reason}") from exc
    except TimeoutError as exc:
        raise HarnessError(f"timeout after {timeout}s") from exc
    except (http.client.HTTPException, UnicodeDecodeError) as exc:
        raise HarnessError(f"malformed HTTP response from {url}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise HarnessError(f"invalid JSON from {url}: {exc}") from exc


def post_query(
    base_url: str,
    cypher: str,
    params: dict[str, Any],
    schema: str | None,
    timeout: float,
) -> tuple[int | None, str | None]:
    payload: dict[str, Any] = {"query": cypher, "parameters": params}
    if schema:
        payload["schema_name"] = schema

    try:
        status, body = _post_json(_endpoint(base_url, "/query"), payload, timeout)
    except HarnessError as exc:
        return None, str(exc)

    if not 200 <= status < 300:
        if isinstance(body, dict) and "error" in body:
            detail = str(body["error"])
        else:
            detail = str(body)
        if not detail.strip():
            detail = http.client.responses.get(status, "empty response body")
        return None, f"HTTP {status}: {detail}"

    if not isinstance(body, dict):
        return None, "invalid response: expected a JSON object"
    if "error" in body:
        return None, str(body["error"])

    results = body.get("results")
    if not isinstance(results, list):
        return None, "invalid response: missing 'results' list"
    return len(results), None


def benchmark_case(
    case: QueryCase,
    cypher: str,
    params: dict[str, Any],
    base_url: str,
    schema: str | None,
    iterations: int,
    warmup: int,
    timeout: float,
) -> QueryResult:
    for _ in range(warmup):
        _, error = post_query(base_url, cypher, params, schema, timeout)
        if error:
            return QueryResult(case.id, case.category, "fail", error=error)

    durations: list[float] = []
    rows: int | None = None
    for _ in range(iterations):
        start = time.perf_counter()
        attempt_rows, error = post_query(base_url, cypher, params, schema, timeout)
        elapsed_ms = (time.perf_counter() - start) * 1000
        if error:
            return QueryResult(
                case.id,
                case.category,
                "fail",
                runs=len(durations),
                rows=rows,
                error=error,
            )
        rows = attempt_rows
        durations.append(elapsed_ms)

    if not durations:
        return QueryResult(case.id, case.category, "fail", error="no measured runs")

    return QueryResult(
        id=case.id,
        category=case.category,
        status="empty" if rows == 0 else "pass",
        runs=len(durations),
        rows=rows,
        min_ms=round(min(durations), 2),
        median_ms=round(statistics.median(durations), 2),
        max_ms=round(max(durations), 2),
    )


def summarize(results: list[QueryResult]) -> dict[str, Any]:
    by_category: dict[str, dict[str, int]] = {}
    for category in ("IS", "IC", "BI"):
        subset = [result for result in results if result.category == category]
        by_category[category] = {
            "total": len(subset),
            "passed": sum(1 for result in subset if result.status == "pass"),
            "empty": sum(1 for result in subset if result.status == "empty"),
        }
    return {
        "total": len(results),
        "passed": sum(1 for result in results if result.status == "pass"),
        "empty": sum(1 for result in results if result.status == "empty"),
        "failed": sum(1 for result in results if result.status == "fail"),
        "by_category": by_category,
    }


def _md_cell(value: Any) -> str:
    if value is None:
        return ""
    return str(value).replace("|", "\\|").replace("\n", "<br>")


def write_json(
    path: Path,
    metadata: dict[str, Any],
    summary: dict[str, Any],
    results: list[QueryResult],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 2,
        "metadata": metadata,
        "summary": summary,
        "queries": [asdict(result) for result in results],
    }
    path.write_text(json.dumps(payload, indent=2) + "\n")


def write_markdown(
    path: Path,
    metadata: dict[str, Any],
    summary: dict[str, Any],
    results: list[QueryResult],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    label = metadata.get("label", "ldbc")
    lines = [f"# LDBC SNB Benchmark — {label}", ""]

    for key, value in metadata.items():
        if key == "schema" and value is None:
            value = "default"
        lines.append(f"- **{key.replace('_', ' ').capitalize()}**: {value}")
    lines.extend(
        [
            "",
            "## Summary",
            "",
            "| Category | Passed | Empty | Total |",
            "| --- | --- | --- | --- |",
        ]
    )
    for category in ("IS", "IC", "BI"):
        counts = summary["by_category"][category]
        lines.append(
            f"| {category} | {counts['passed']} | {counts['empty']} | {counts['total']} |"
        )
    lines.append(
        f"| **Total** | **{summary['passed']}** | **{summary['empty']}** "
        f"| **{summary['total']}** |"
    )
    lines.extend(
        [
            "",
            "Passed: ran and returned rows. Empty: ran without an error but returned "
            "no rows, so its latency may measure nothing (an empty answer, or a plan "
            "that cannot match the schema). The remaining queries failed; see Failures.",
        ]
    )
    lines.extend(
        [
            "",
            "## Queries",
            "",
            "| Query | Category | Status | Runs | Rows | Min (ms) | Median (ms) | Max (ms) | Error |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for result in results:
        status = {"pass": "PASS", "empty": "EMPTY"}.get(result.status, "FAIL")
        error = result.error or ""
        if len(error) > 120:
            error = error[:117] + "..."
        lines.append(
            "| "
            + " | ".join(
                [
                    _md_cell(result.id),
                    _md_cell(result.category),
                    status,
                    _md_cell(result.runs),
                    _md_cell(result.rows),
                    _md_cell(result.min_ms),
                    _md_cell(result.median_ms),
                    _md_cell(result.max_ms),
                    _md_cell(error),
                ]
            )
            + " |"
        )

    lines.extend(["", "## Failures", ""])
    failures = [result for result in results if result.status == "fail"]
    if not failures:
        lines.append("No failures.")
    else:
        for result in failures:
            lines.extend(
                [
                    f"### {result.id}",
                    "",
                    "```",
                    result.error or "unknown error",
                    "```",
                    "",
                ]
            )

    path.write_text("\n".join(lines).rstrip() + "\n")


def _get(url: str, timeout: float) -> tuple[int, bytes]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()
    except urllib.error.URLError as exc:
        raise HarnessError(f"cannot reach {url}: {exc.reason}") from exc
    except TimeoutError as exc:
        raise HarnessError(f"timeout after {timeout}s while reading {url}") from exc
    except (http.client.HTTPException, UnicodeDecodeError) as exc:
        raise HarnessError(f"malformed HTTP response from {url}: {exc}") from exc


def check_server(base_url: str, schema: str | None, timeout: float) -> str | None:
    """Check the server answers, and that ``schema`` is loaded if one is named.

    Returns the version `/health` reports, or None when it reports none.
    """
    health_url = _endpoint(base_url, "/health")
    status, health = _get(health_url, timeout)
    if status != 200:
        raise HarnessError(f"health check failed: HTTP {status} from {health_url}")
    version = _health_version(health)

    if schema:
        schemas_url = _endpoint(base_url, "/schemas")
        status, body = _get(schemas_url, timeout)
        if status != 200:
            raise HarnessError(f"schema check failed: HTTP {status} from {schemas_url}")
        try:
            data = json.loads(body)
        except json.JSONDecodeError as exc:
            raise HarnessError(f"invalid schema response from {schemas_url}: {exc}") from exc
        names = {
            entry.get("name")
            for entry in data.get("schemas", [])
            if isinstance(entry, dict) and entry.get("name")
        }
        if schema not in names:
            raise HarnessError(
                f"schema {schema!r} not found; available: {', '.join(sorted(names)) or 'none'}"
            )
    return version


def _health_version(body: bytes) -> str | None:
    try:
        data = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    version = data.get("version") if isinstance(data, dict) else None
    return str(version) if version else None


def git_commit() -> str | None:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return completed.stdout.strip() or None


METADATA_KEYS = (
    "timestamp",
    "label",
    "url",
    "schema",
    "iterations",
    "warmup",
    "timeout_seconds",
    "server_version",
    "harness_commit",
    "python",
    "platform",
)


def parse_meta(values: list[str]) -> dict[str, str]:
    meta: dict[str, str] = {}
    for item in values:
        key, separator, value = item.partition("=")
        key = key.strip()
        if not separator or not key:
            raise HarnessError("--meta must be KEY=VALUE")
        if key in METADATA_KEYS:
            raise HarnessError(f"--meta cannot reuse the built-in key {key!r}")
        meta[key] = value
    return meta


def _timed_out(result: QueryResult) -> bool:
    error = result.error or ""
    return error.startswith("HTTP 408") or error.startswith("timeout after")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the official LDBC SNB queries against a ClickGraph endpoint."
    )
    parser.add_argument("--url", default="http://localhost:7475")
    parser.add_argument("--schema", default=None)
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=300.0)
    parser.add_argument("--query", action="append", default=[], dest="queries")
    parser.add_argument("--params-file", type=Path, default=None)
    parser.add_argument("--param", action="append", default=[], dest="params")
    parser.add_argument("--out-dir", type=Path, default=RESULTS_DIR)
    parser.add_argument("--label", default=None)
    parser.add_argument("--strict", action="store_true")
    parser.add_argument(
        "--meta",
        action="append",
        default=[],
        dest="meta",
        metavar="KEY=VALUE",
        help="record an extra fact in the report, e.g. clickhouse=25.8.11 "
        "(repeatable; may not reuse a built-in key)",
    )
    return parser.parse_args(argv)


def _safe_label(label: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "-", label).strip(".-")
    return cleaned or "ldbc"


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.iterations < 1:
            raise HarnessError("--iterations must be >= 1")
        if args.warmup < 0:
            raise HarnessError("--warmup must be >= 0")
        if args.timeout <= 0:
            raise HarnessError("--timeout must be > 0")

        args.url = args.url.rstrip("/")
        cases = discover_queries()
        if not cases:
            raise HarnessError(f"no queries found under {QUERY_ROOT}")
        if args.queries:
            by_id = {case.id: case for case in cases}
            unknown = [query for query in args.queries if query not in by_id]
            if unknown:
                raise HarnessError("unknown query: " + ", ".join(unknown))
            cases = [by_id[query] for query in args.queries]

        overrides = load_overrides(args.params_file)
        global_params = parse_param_overrides(args.params)
        extra = parse_meta(args.meta)
        server_version = check_server(args.url, args.schema, args.timeout)

        results: list[QueryResult] = []
        for index, case in enumerate(cases, 1):
            print(f"[{index}/{len(cases)}] {case.id} ... ", end="", flush=True)
            try:
                cypher = strip_comments(case.path.read_text())
                params = resolve_params(case, overrides, global_params)
                validate_params(cypher, params)
                result = benchmark_case(
                    case,
                    cypher,
                    params,
                    args.url,
                    args.schema,
                    args.iterations,
                    args.warmup,
                    args.timeout,
                )
            except (OSError, HarnessError) as exc:
                result = QueryResult(
                    case.id, case.category, "fail", error=str(exc)
                )
            results.append(result)
            if result.status == "fail":
                print(f"FAIL: {result.error}")
            else:
                print(
                    f"{result.status.upper()} {result.median_ms:.2f} ms "
                    f"({result.rows} rows)"
                )

        summary = summarize(results)
        label = args.label or "ldbc"
        timestamp = datetime.now(timezone.utc)
        base = args.out_dir / f"{timestamp.strftime('%Y%m%d_%H%M%S')}_{_safe_label(label)}"
        metadata = {
            "timestamp": timestamp.isoformat(timespec="seconds"),
            "label": label,
            "url": args.url,
            "schema": args.schema,
            "iterations": args.iterations,
            "warmup": args.warmup,
            "timeout_seconds": args.timeout,
            "server_version": server_version,
            "harness_commit": git_commit(),
            "python": platform.python_version(),
            "platform": platform.platform(),
            **extra,
        }
        json_path = base.parent / (base.name + ".json")
        markdown_path = base.parent / (base.name + ".md")
        write_json(json_path, metadata, summary, results)
        write_markdown(markdown_path, metadata, summary, results)

        print(
            f"{summary['passed']}/{summary['total']} returned rows, "
            f"{summary['empty']} empty, {summary['failed']} failed"
        )
        timed_out = [result.id for result in results if _timed_out(result)]
        if timed_out:
            print(
                "note: ClickGraph stopped waiting for " + ", ".join(timed_out)
                + ", but ClickHouse may still be running the query and slowing "
                "later ones; check system.processes (KILL QUERY) before trusting "
                "later timings",
                file=sys.stderr,
            )
        print(f"JSON: {json_path}")
        print(f"Markdown: {markdown_path}")
        return 1 if args.strict and summary["failed"] else 0
    except HarnessError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
