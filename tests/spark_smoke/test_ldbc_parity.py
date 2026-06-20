"""LDBC SNB result-set parity — the M3 gate the sweep referred to.

For every LDBC query that already *translates and executes* on Delta
(`test_ldbc_sweep.py`), run the same Cypher on BOTH backends over the same mini
dataset and assert the result sets match (content parity, order-insensitive):

  - ClickHouse side : `cg --dialect clickhouse query --format json` against a
    live ClickHouse holding `benchmarks/ldbc_snb/data/mini_dataset.sql`
    (loaded into database `ldbc`, renamed from `ldbc_mini`).
  - Delta side      : `cg --dialect databricks sql` shipped into the
    `deltaio/delta-docker` container seeded by `mini_delta_seed.sql` — the
    Delta translation of the same mini dataset.

Both sides start from the identical Cypher, so a mismatch is a real
dialect/translation divergence, not a data difference.

Scope: only the queries that pass the Delta sweep are parity-tested. Queries in
`KNOWN_SKIPS` / `EXPECTED_FAILURES` don't execute cleanly on Delta yet, so
parity is moot — they're skipped here with a pointer to the sweep.

Comparison is a multiset (order-insensitive) of normalized rows; collect()-style
arrays are compared as sorted element sets since their order isn't guaranteed.

LIMITATION — row ORDER is verified by NEITHER this gate nor the sweep: this gate
compares order-insensitively, and the sweep only checks "executes without
error". So a query whose `ORDER BY` is dropped or mistranslated would pass both.
Ordered assertions for a curated subset are future work; for now this is a
*content* gate (same rows, any order), not an ordering gate.

When both engines return zero rows the case is `skip`ped rather than passed, so
vacuous "[] == []" comparisons (common on the tiny mini dataset) are visible in
the pytest summary instead of inflating the green count.

Gating (all required; skips cleanly otherwise):
  - CLICKGRAPH_SPARK_TESTS=1, `cg` built with --features databricks, docker
    (inherited from test_smoke via _require_env)
  - a reachable ClickHouse with the mini dataset loaded. Configure with:
      CG_PARITY_CH_URL   (default http://localhost:8124)
      CG_PARITY_CH_USER  (default test_user)
      CG_PARITY_CH_PASS  (default test_pass)
    Bring-up (matches this file's expectations):
      docker run -d --rm --name cg-parity-ch -p 8124:8123 \\
        -e CLICKHOUSE_USER=test_user -e CLICKHOUSE_PASSWORD=test_pass \\
        clickhouse/clickhouse-server:25.8.12
      sed 's/ldbc_mini/ldbc/g' benchmarks/ldbc_snb/data/mini_dataset.sql | \\
        docker exec -i cg-parity-ch clickhouse-client \\
          --user test_user --password test_pass --multiquery
"""
from __future__ import annotations

import json
import os
import subprocess
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from test_smoke import (  # noqa: F401 — reuse harness helpers
    REPO_ROOT,
    SCHEMA,
    _generate_sql,
    _require_env,
    _run_in_container,
    _table_rows,
)
from test_ldbc_sweep import (
    EXPECTED_FAILURES,
    KNOWN_SKIPS,
    _collect_query_ids,
    _family_dir,
    _load_query,
    _substitute_params,
)

CH_URL = os.environ.get("CG_PARITY_CH_URL", "http://localhost:8124")
CH_USER = os.environ.get("CG_PARITY_CH_USER", "test_user")
CH_PASS = os.environ.get("CG_PARITY_CH_PASS", "test_pass")

# Queries whose ClickHouse-dialect translation does not execute on the mini
# dataset, so there is no CH baseline to diff against. These are ClickGraph
# (ClickHouse path) issues, NOT DeltaGraph ones — the Delta side executes fine
# (they pass the Delta sweep). Excluded here so the parity gate measures
# DeltaGraph correctness, not the ClickHouse backend's gaps. Revisit if the
# CH-side translation is fixed.
CH_SIDE_EXCLUSIONS: dict[str, str] = {
    "complex-1": "CH Code 47 (UNKNOWN_IDENTIFIER) — ClickHouse-dialect translation; Delta side passes",
    "complex-11": "CH Code 47 (UNKNOWN_IDENTIFIER) — ClickHouse-dialect translation; Delta side passes",
}


def _ch_reachable() -> bool:
    try:
        req = urllib.request.Request(f"{CH_URL}/ping")
        with urllib.request.urlopen(req, timeout=3) as resp:  # noqa: S310 (local)
            return resp.status == 200
    except (urllib.error.URLError, OSError):
        return False


def _require_parity_env() -> Path:
    cg = _require_env()  # CLICKGRAPH_SPARK_TESTS + cg + docker, or skips
    if not _ch_reachable():
        pytest.skip(f"ClickHouse not reachable at {CH_URL} (see module docstring for bring-up)")
    return cg


def _run_clickhouse(cg_bin: Path, cypher: str) -> list[dict]:
    """Translate Cypher → ClickHouse SQL and execute it via `cg query`,
    returning NDJSON rows as a list of column→value dicts."""
    result = subprocess.run(
        [
            str(cg_bin), "--schema", str(SCHEMA),
            "--clickhouse", CH_URL, "--ch-user", CH_USER, "--ch-password", CH_PASS,
            "--dialect", "clickhouse", "query", "--format", "json", cypher,
        ],
        capture_output=True, text=True, timeout=120,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"clickhouse run failed (rc={result.returncode}):\n"
            f"STDERR:\n{result.stderr}\nSTDOUT:\n{result.stdout}"
        )
    rows: list[dict] = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


# ── cross-engine value normalization ─────────────────────────────────────────

# Floats are compared at reduced precision: ClickHouse and Spark diverge in the
# last ULP on the same computation (e.g. 0.4545454545454546 vs
# 0.45454545454545453). 12 significant digits is well inside both engines'
# agreement and far beyond LDBC's meaningful precision.
_FLOAT_SIG_DIGITS = 12


def _canon_float(f: float) -> str:
    if f.is_integer():
        return str(int(f))
    return f"{f:.{_FLOAT_SIG_DIGITS}g}"


def _canon_scalar(v: object) -> str:
    """Canonical string for a scalar so CH-typed JSON and Delta ASCII compare
    equal despite engine rendering differences:
      - NULLs unify (`None`, "NULL", "null", "" → "NULL")
      - booleans unify across the CH 0/1 and Spark true/false spellings, by
        folding to "0"/"1" (a genuine integer 0/1 on both sides still matches)
      - floats compare at `_FLOAT_SIG_DIGITS` significant digits (last-ULP
        divergence between engines is not a real mismatch)."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, float):
        return _canon_float(v)
    if isinstance(v, int):
        return str(v)
    s = str(v).strip()
    # Both engines render SQL NULL as the literal "NULL"; CH JSON null already
    # became Python None above. Empty string is NOT folded into NULL — keeping
    # them distinct lets the gate catch a real NULL-vs-empty-string divergence.
    if s in ("NULL", "null"):
        return "NULL"
    low = s.lower()
    if low == "true":
        return "1"
    if low == "false":
        return "0"
    # Integer-looking strings stay exact — float() would lose precision for
    # 64-bit ids ≥ 2^53 (the Delta side arrives as ASCII, the CH side as a typed
    # JSON int, so a lossy coercion could spuriously mismatch).
    try:
        return str(int(s))
    except ValueError:
        pass
    # Non-integer numeric strings ("7.0", "0.4545…") compare at reduced precision.
    try:
        return _canon_float(float(s))
    except ValueError:
        return s


def _canon_array(v: object) -> str:
    """Canonical for a list-valued cell. Element order isn't guaranteed
    (collect_list / groupArray), so sort. Accepts a JSON list (CH) or a Delta
    ASCII `[a, b, c]` rendering."""
    if isinstance(v, list):
        elems = [_canon_scalar(e) for e in v]
    else:
        s = str(v).strip()
        inner = s[1:-1] if s.startswith("[") and s.endswith("]") else s
        elems = [_canon_scalar(e.strip()) for e in inner.split(",")] if inner else []
    return "[" + ",".join(sorted(elems)) + "]"


def _canon_cell(v: object) -> str:
    is_list = isinstance(v, list) or (isinstance(v, str) and v.strip().startswith("["))
    return _canon_array(v) if is_list else _canon_scalar(v)


def _canon_rows_from_ch(rows: list[dict]) -> list[tuple[str, ...]]:
    return sorted(tuple(_canon_cell(v) for v in r.values()) for r in rows)


def _canon_rows_from_delta(rows: list[list[str]]) -> list[tuple[str, ...]]:
    return sorted(tuple(_canon_cell(v) for v in r) for r in rows)


def _parity_param(qid: str):
    # Skip anything that doesn't execute cleanly on Delta yet — parity is moot.
    reason = KNOWN_SKIPS.get(qid) or EXPECTED_FAILURES.get(qid)
    if reason:
        return pytest.param(qid, marks=pytest.mark.skip(reason=f"not executable on Delta: {reason}"), id=qid)
    ch_reason = CH_SIDE_EXCLUSIONS.get(qid)
    if ch_reason:
        return pytest.param(qid, marks=pytest.mark.skip(reason=f"no CH baseline: {ch_reason}"), id=qid)
    return pytest.param(qid, id=qid)


@pytest.mark.parametrize("qid", [_parity_param(q) for q in _collect_query_ids()])
def test_ldbc_result_parity(qid: str) -> None:
    cg_bin = _require_parity_env()
    cypher_raw, params = _load_query(_family_dir(qid), qid)
    cypher = _substitute_params(cypher_raw, params)

    ch_rows = _canon_rows_from_ch(_run_clickhouse(cg_bin, cypher))

    spark_sql = _generate_sql(cg_bin, cypher)
    delta_rows = _canon_rows_from_delta(_table_rows(_run_in_container(spark_sql)))

    # Assert content parity first — this catches an asymmetry (one side empty,
    # the other not), since the multisets would differ.
    assert delta_rows == ch_rows, (
        f"result parity mismatch for {qid}\n"
        f"  clickhouse ({len(ch_rows)} rows): {ch_rows}\n"
        f"  delta      ({len(delta_rows)} rows): {delta_rows}"
    )
    # Both empty (and therefore equal) is a vacuous pass on this tiny dataset —
    # skip so it doesn't masquerade as real content parity in the summary.
    if not ch_rows:
        pytest.skip(f"{qid}: both engines return 0 rows on the mini dataset (vacuous parity)")
