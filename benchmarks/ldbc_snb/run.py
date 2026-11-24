"""Run the official LDBC SNB queries against a ClickGraph HTTP endpoint."""

from __future__ import annotations

import json
import re
import statistics
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
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
        rows, error = post_query(base_url, cypher, params, schema, timeout)
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
        durations.append(elapsed_ms)

    if not durations:
        return QueryResult(case.id, case.category, "fail", error="no measured runs")

    return QueryResult(
        id=case.id,
        category=case.category,
        status="pass",
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
        }
    passed = sum(1 for result in results if result.status == "pass")
    return {
        "total": len(results),
        "passed": passed,
        "failed": len(results) - passed,
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
        "schema_version": 1,
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
            "| Category | Passed | Total |",
            "| --- | --- | --- |",
        ]
    )
    for category in ("IS", "IC", "BI"):
        counts = summary["by_category"][category]
        lines.append(f"| {category} | {counts['passed']} | {counts['total']} |")
    lines.append(
        f"| **Total** | **{summary['passed']}** | **{summary['total']}** |"
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
        status = "PASS" if result.status == "pass" else "FAIL"
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
    failures = [result for result in results if result.status != "pass"]
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
