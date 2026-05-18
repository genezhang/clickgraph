"""LDBC SNB sweep — pytest-parametrized over the official bi / interactive
queries. First-pass gate: "translates and executes without error" against
the seeded Delta mini dataset. Result-set diff vs ClickGraph is M3.

Each test:
  1. reads <query>.cypher and substitutes `$param` placeholders with values
     from the sibling .params.json file
  2. invokes `cg --dialect databricks sql` to translate
  3. ships the SQL into the same `deltaio/delta-docker` container as
     test_smoke.py and asserts non-zero return → failure

Skipped: bi-10/15/16/19/20 — known unstable on this dataset (see MEMORY.md
LDBC status). Skipped via `pytest.mark.skip` rather than removed so the
skip + reason is visible in pytest output.

Gated by CLICKGRAPH_SPARK_TESTS=1 just like test_smoke.py.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from test_smoke import (  # noqa: F401 — reuse harness helpers
    HARNESS_DIR,
    IMAGE,
    REPO_ROOT,
    SCHEMA,
    _generate_sql,
    _require_env,
    _run_in_container,
)

QUERY_ROOT = REPO_ROOT / "benchmarks" / "ldbc_snb" / "queries" / "official"
BI_DIR = QUERY_ROOT / "bi"
INTERACTIVE_DIR = QUERY_ROOT / "interactive"

# Memory says these are unstable / blocked at sf0.003. Keep visible as skips
# rather than dropping silently — re-evaluate when the relevant fixes land.
KNOWN_SKIPS: dict[str, str] = {
    "bi-10": "unstable on small dataset (recursive tag scoring)",
    "bi-15": "TRAIL semantics — not yet implemented in the planner",
    "bi-16": "CALL subquery — language feature gap (shared with ClickGraph)",
    "bi-19": "TRAIL semantics — not yet implemented in the planner",
    "bi-20": "weighted shortestPath — not yet implemented in the planner",
}

# Known DeltaGraph translation gaps surfaced by this sweep. Each entry is
# marked xfail(strict=True) — if a fix lands and the query starts passing,
# pytest reports XPASS as a failure, prompting the marker to be dropped.
# Categories:
#   A. FunctionMapper gaps — CH-native fn name leaking into Spark SQL
#   B. Parse errors — syntax the dialect emitter shouldn't produce on Spark
#   C. CTE column resolution — alias mismatch between CTE def and downstream ref
EXPECTED_FAILURES: dict[str, str] = {
    # A. FunctionMapper gaps
    "bi-1":      "[A] FunctionMapper: toYear not mapped to Spark `year()`",
    "bi-2":      "[A] FunctionMapper: anyLast not mapped to Spark `last_value()`/`last()`",
    "bi-5":      "[A] FunctionMapper: anyLast",
    "bi-6":      "[A] FunctionMapper: tuple not mapped to Spark `struct()`",
    "bi-12":     "[A] FunctionMapper: anyLast",
    "bi-13":     "[A] FunctionMapper: anyLast",
    "bi-14":     "[A] FunctionMapper: anyLast",
    "bi-17":     "[A] FunctionMapper: toUnixTimestamp64Milli not mapped to Spark `unix_millis()`",
    "complex-1": "[A] FunctionMapper: anyLast",
    "complex-3": "[A] FunctionMapper: `has(array, elem)` not mapped to Spark `array_contains()`",
    "complex-4": "[A] FunctionMapper: anyLast",
    "complex-5": "[A] FunctionMapper: anyLast",
    "complex-7": "[A] FunctionMapper: anyLast",
    "complex-12": "[A] FunctionMapper: toString not mapped to Spark `cast(... as string)`",
    "complex-13": "[A] FunctionMapper: countIf not mapped to Spark `count(case when cond then 1 end)`",
    "complex-14": "[A] FunctionMapper: countIf",
    "short-2":   "[A] FunctionMapper: toString",
    # B. Parse errors
    "bi-8":       "[B] PARSE_SYNTAX_ERROR near `ARRAY` (line 23) — VLP CTE generation",
    "complex-10": "[B] PARSE_SYNTAX_ERROR near `\"posts\"` (line 64) — double-quote identifier vs Spark backtick",
    # C. CTE column resolution
    "complex-9":  "[C] CTE alias mismatch: with_friend_cte_N.p6_friend_id vs friend.p6_friend_id",
    "complex-11": "[C] CTE alias mismatch: with_friend_cte_N.p6_friend_id vs friend.p6_friend_id",
}

PARAM_REF = re.compile(r"\$([a-zA-Z_]\w*)")


def _substitute_params(cypher: str, params: dict[str, object]) -> str:
    """Inline-substitute `$ident` references with literal values.

    Strings are single-quoted (with `'` → `''` escaping). Numbers and bools
    are stringified bare. Lists become Spark `ARRAY(...)` literals.
    """
    def render(val: object) -> str:
        if isinstance(val, bool):
            return "TRUE" if val else "FALSE"
        if isinstance(val, (int, float)):
            return str(val)
        if isinstance(val, str):
            return "'" + val.replace("'", "''") + "'"
        if isinstance(val, list):
            return "[" + ", ".join(render(x) for x in val) + "]"
        raise TypeError(f"unsupported param type: {type(val).__name__}")

    def sub(m: re.Match[str]) -> str:
        name = m.group(1)
        if name not in params:
            raise KeyError(f"query references $`{name}` but params.json has no such key")
        return render(params[name])

    return PARAM_REF.sub(sub, cypher)


def _load_query(family_dir: Path, qid: str) -> tuple[str, dict[str, object]]:
    cypher = (family_dir / f"{qid}.cypher").read_text()
    params_path = family_dir / f"{qid}.params.json"
    params: dict[str, object] = json.loads(params_path.read_text()) if params_path.exists() else {}
    return cypher, params


def _collect_query_ids() -> list[str]:
    """List `bi-N` ids 1..18 and `complex-N` / `short-N` from interactive/."""
    ids: list[str] = []
    for p in sorted(BI_DIR.glob("bi-*.cypher")):
        ids.append(p.stem)
    for p in sorted(INTERACTIVE_DIR.glob("*.cypher")):
        ids.append(p.stem)
    return ids


def _family_dir(qid: str) -> Path:
    return BI_DIR if qid.startswith("bi-") else INTERACTIVE_DIR


def _qid_param(qid: str):
    skip_reason = KNOWN_SKIPS.get(qid)
    if skip_reason:
        return pytest.param(qid, marks=pytest.mark.skip(reason=skip_reason), id=qid)
    xfail_reason = EXPECTED_FAILURES.get(qid)
    if xfail_reason:
        # strict=True: when the gap is fixed the test reports XPASS as a hard
        # failure, prompting the dev to drop this marker.
        return pytest.param(qid, marks=pytest.mark.xfail(reason=xfail_reason, strict=True), id=qid)
    return pytest.param(qid, id=qid)


@pytest.mark.parametrize("qid", [_qid_param(q) for q in _collect_query_ids()])
def test_ldbc_query_translates_and_executes(qid: str) -> None:
    """First-pass gate: query translates (cg) and executes (Spark) without error.
    Empty result rows are acceptable — result-set diff vs ClickGraph is M3."""
    cg_bin = _require_env()
    cypher_raw, params = _load_query(_family_dir(qid), qid)
    cypher = _substitute_params(cypher_raw, params)
    sql = _generate_sql(cg_bin, cypher)
    # _run_in_container raises on non-zero rc with full stderr — that's the gate.
    _run_in_container(sql)
