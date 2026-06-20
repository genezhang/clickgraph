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
#   D. Unsupported language features — query uses Cypher constructs we don't
#      translate (GDS procedures, CALL subqueries, etc.); orthogonal to A-C
EXPECTED_FAILURES: dict[str, str] = {
    # A. FunctionMapper gaps — residual unmapped routines after the
    #    anyLast / countIf / temporal-extraction / has / toString sweep.
    "bi-6":       "[A] FunctionMapper: bare `tuple(...)` still emitted (NodeId::sql_tuple / VLP path composite)",
    "bi-13":      "[A] FunctionMapper: `caseWithExpression` not mapped to Spark CASE",
    "bi-17":      "[A] FunctionMapper: `toUnixTimestamp64Milli` leaking in duration-arithmetic path",
    "complex-5":  "[A] FunctionMapper: `countIf(cond, val)` rewritten to `count_if(cond, val)` but Spark `count_if` takes 1 arg",
    "complex-12": "[A] FunctionMapper: `formatRowNoNewline` not mapped (composite-key emission helper)",
    "complex-14": "[D] unsupported: query uses GDS (`gds.graph.project.cypher`, `gds.shortestPath.dijkstra.stream`) which we don't translate; once past shortestPath BFS, the trailing `WITH 42 AS dummy` carries through and downstream column refs fail with INVALID_EXTRACT_BASE_FIELD_TYPE",
    "short-2":    "[A] FunctionMapper: `formatRowNoNewline` not mapped",
    # B. Parse errors
    "bi-8":       "[B] PARSE_SYNTAX_ERROR near `ARRAY` — VLP CTE generation",
    "bi-12":      "[B] PARSE_SYNTAX_ERROR — surfaced after FunctionMapper closure",
    "complex-10": "[B] PARSE_SYNTAX_ERROR near `\"posts\"` — double-quote identifier vs Spark backtick",
    # C. Other resolution issues — distinct from the dialect-agnostic
    # `with_*_cte_N.col` → `alias.col` rewrite (which now passes 8 queries).
    "bi-14":     "[C] CTE chain: same alias `person1` rebound across 5 chained CTEs; final CTE's `person1.score` doesn't resolve against the previous CTE's schema",
    "complex-3": "[C] schema mapping: `t5.CountryId` emitted for Place_isPartOf_Place rel (no such column — Place→Place rel uses PlaceId)",
}

PARAM_REF = re.compile(r"\$([a-zA-Z_]\w*)")


def _substitute_params(cypher: str, params: dict[str, object]) -> str:
    """Inline-substitute `$ident` references with Cypher literal values.

    The substituted query is fed back into `cg`, which then emits Spark SQL —
    so the literals here must be valid Cypher, not SQL. Strings are
    single-quoted (with `'` → `''` escaping), numbers/bools stringified bare,
    lists rendered as Cypher `[a, b, c]` list literals.
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
    """List every `bi-*.cypher` in bi/ and every `*.cypher` in interactive/.
    Filtering of unsupported IDs happens later via KNOWN_SKIPS."""
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
    Empty result rows are acceptable here. The result-set diff vs ClickGraph
    (the M3 gate) is implemented in `test_ldbc_parity.py`."""
    cg_bin = _require_env()
    cypher_raw, params = _load_query(_family_dir(qid), qid)
    cypher = _substitute_params(cypher_raw, params)
    sql = _generate_sql(cg_bin, cypher)
    # _run_in_container raises on non-zero rc with full stderr — that's the gate.
    _run_in_container(sql)
