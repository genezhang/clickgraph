"""DeltaGraph ↔ zeta-databricks transport/integration gate.

Exercises the REAL cross-stack path the Spark docker container can't:

    Cypher → cg (--dialect databricks) → DatabricksSqlExecutor
           → Statement Execution REST → zeta-databricks → rows back

i.e. the executor wiring, submit/poll loop, JSON_ARRAY decoding, and (via the
VLP query) the `array_contains` / array-`concat` builtins added to Zeta. The
Spark smoke harness validates Spark-runtime dialect fidelity; this validates
the transport layer end-to-end without a live Databricks workspace.

Requires (skips cleanly otherwise):
  - CLICKGRAPH_ZETA_TESTS=1
  - a running zeta-databricks REST server, its URL in ZETA_DATABRICKS_URL
    (e.g. http://127.0.0.1:18099). Start one with:
        cargo run -p zeta-server-bin --features wire-databricks -- \\
          --databricks-port 18099
  - cg built with --features databricks (CG_BIN overrides the path)

The harness seeds the LDBC mini dataset by POSTing `zeta_ldbc_seed.sql`
statement-by-statement to the REST API, then drives queries through `cg` with
`CG_DATABRICKS_BASE_URL` pointed at the same server.

Run standalone (no pytest):  python3 test_zeta_transport.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

THIS_DIR = Path(__file__).resolve().parent
REPO_ROOT = THIS_DIR.parents[1]
SCHEMA = REPO_ROOT / "benchmarks" / "ldbc_snb" / "schemas" / "ldbc_snb_complete.yaml"
SEED_SQL = THIS_DIR / "zeta_ldbc_seed.sql"
ZETA_URL = os.environ.get("ZETA_DATABRICKS_URL", "http://127.0.0.1:18099")


def _resolve_cg_bin() -> Path | None:
    env_override = os.environ.get("CG_BIN")
    if env_override:
        return Path(env_override)
    for rel in ("release/cg", "debug/cg"):
        for base in (REPO_ROOT / "target", Path("/mnt/cargo-sd/cargo/target")):
            cand = base / rel
            if cand.exists():
                return cand
    return None


def _split_statements(sql_text: str) -> list[str]:
    """Split on `;`, dropping comments and blank statements."""
    out: list[str] = []
    for raw in sql_text.split(";"):
        lines = [ln for ln in raw.splitlines() if not ln.strip().startswith("--")]
        stmt = "\n".join(lines).strip()
        if stmt:
            out.append(stmt)
    return out


def _submit(stmt: str) -> dict:
    body = json.dumps({"warehouse_id": "w", "statement": stmt}).encode()
    req = urllib.request.Request(
        f"{ZETA_URL}/api/2.0/sql/statements",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 (local)
        return json.loads(resp.read())


def _zeta_reachable() -> bool:
    try:
        _submit("SELECT 1")
        return True
    except (urllib.error.URLError, OSError):
        return False


def _seed() -> None:
    for stmt in _split_statements(SEED_SQL.read_text()):
        r = _submit(stmt)
        state = r.get("status", {}).get("state")
        assert state == "SUCCEEDED", f"seed failed [{state}]: {stmt[:60]}…\n{r}"


def _cg_query(cg_bin: Path, cypher: str) -> list[dict]:
    """Run a Cypher query through cg's Databricks executor pointed at Zeta."""
    env = dict(os.environ)
    env.update(
        CG_DATABRICKS_BASE_URL=ZETA_URL,
        CG_DATABRICKS_HOST="zeta.local",      # ignored (base_url wins) but required
        CG_DATABRICKS_WAREHOUSE_ID="w",
        CG_DATABRICKS_TOKEN="not-used-by-zeta",
    )
    result = subprocess.run(
        [
            str(cg_bin), "--schema", str(SCHEMA), "--dialect", "databricks",
            "query", "--format", "json", cypher,
        ],
        capture_output=True, text=True, timeout=120, env=env,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"cg query failed (rc={result.returncode}):\n"
            f"STDERR:\n{result.stderr}\nSTDOUT:\n{result.stdout}"
        )
    return [json.loads(ln) for ln in result.stdout.splitlines() if ln.strip()]


# ── the cross-stack checks ───────────────────────────────────────────────────

# Cross-stack cases that pass today: they prove the transport layer (cg →
# DatabricksSqlExecutor → REST submit/poll → JSON decode → rows). ORDER BY is
# avoided and rows are compared order-insensitively (see _rows_eq), because
# Zeta lacks Spark's lateral column aliases that DeltaGraph's ORDER BY
# desugaring relies on — one of the documented Zeta VLP gaps below.
CASES = [
    (
        "flat_knows_join",
        "MATCH (n:Person {id:1})-[:KNOWS]->(f:Person) "
        "RETURN f.id AS id, f.firstName AS name",
        [{"id": 2, "name": "Bob"}, {"id": 3, "name": "Carol"}],
    ),
    (
        "count_aggregation",
        "MATCH (p:Person {id:1})-[:KNOWS]->(f:Person) "
        "RETURN count(f) AS friendCount",
        [{"friendCount": 2}],
    ),
]

# Variable-length-path queries do NOT yet execute on Zeta end-to-end. The
# `array_contains` / array-`concat` builtins added for this are necessary but
# not sufficient: DeltaGraph's VLP SQL also needs
#   (a) `CAST(array() AS ARRAY<T>)` at execution — Zeta errors
#       "unsupported CAST target type: ARRAY<STRING>"; and
#   (b) Spark lateral column aliases for the ORDER BY desugaring
#       (`SELECT x AS id, id AS __order_col_0`) — Zeta errors
#       "column not found: id".
# Until those land in Zeta, VLP fidelity is validated on the Spark/Delta docker
# container (tests/spark_smoke), not here. See docs/deltagraph/ZETA_FIDELITY.md.
VLP_KNOWN_GAP = (
    "vlp_two_hops",
    "MATCH (p:Person {id:1})-[:KNOWS*1..2]-(f:Person) WHERE f.id <> p.id "
    "RETURN DISTINCT f.id AS id",
)


def _sort_key(d: dict) -> str:
    return json.dumps(d, sort_keys=True)


def _rows_eq(a: list[dict], b: list[dict]) -> bool:
    """Order-insensitive row comparison (Zeta returns unordered without the
    lateral-alias ORDER BY path)."""
    return sorted(a, key=_sort_key) == sorted(b, key=_sort_key)


def _run_all(cg_bin: Path) -> None:
    _seed()
    for name, cypher, expected in CASES:
        rows = _cg_query(cg_bin, cypher)
        assert _rows_eq(rows, expected), f"[{name}] expected {expected}, got {rows}"
        print(f"[PASS] {name}: {rows}")
    # VLP is a documented Zeta gap — confirm it still fails. An XPASS here means
    # Zeta closed the gaps, so FAIL loudly to prompt updating ZETA_FIDELITY.md.
    name, cypher = VLP_KNOWN_GAP
    try:
        rows = _cg_query(cg_bin, cypher)
    except AssertionError:
        print(f"[KNOWN-GAP] {name}: VLP not yet executable on Zeta (see ZETA_FIDELITY.md)")
        return
    raise AssertionError(
        f"[XPASS] {name}: VLP now runs on Zeta (got {rows}) — close the gap, "
        "drop the known-gap marker, and update ZETA_FIDELITY.md"
    )


# pytest entrypoints ----------------------------------------------------------

# Guarded so the standalone `__main__` path runs without pytest installed.
try:
    import pytest as _pytest
except ImportError:  # pragma: no cover - standalone mode
    _pytest = None


def _xfail_strict(reason: str):
    """`@pytest.mark.xfail(strict=True)` when pytest is present, else a no-op
    (the decorated test funcs aren't invoked in standalone mode)."""
    if _pytest is not None:
        return _pytest.mark.xfail(strict=True, reason=reason)
    return lambda f: f


def _require_env():
    if os.environ.get("CLICKGRAPH_ZETA_TESTS") != "1":
        _pytest.skip("CLICKGRAPH_ZETA_TESTS=1 not set")
    cg = _resolve_cg_bin()
    if cg is None:
        _pytest.skip("cg binary not found (build -p clickgraph-tool --features databricks)")
    if not _zeta_reachable():
        _pytest.skip(f"zeta-databricks not reachable at {ZETA_URL}")
    return cg


def test_zeta_transport_flat_join():
    cg = _require_env()
    _seed()
    assert _rows_eq(_cg_query(cg, CASES[0][1]), CASES[0][2])


def test_zeta_transport_aggregation():
    cg = _require_env()
    _seed()
    assert _rows_eq(_cg_query(cg, CASES[1][1]), CASES[1][2])


@_xfail_strict("VLP not executable on Zeta (array CAST + lateral alias) — see ZETA_FIDELITY.md")
def test_zeta_transport_vlp_known_gap():
    """VLP doesn't execute on Zeta yet (CAST-to-array + lateral-alias gaps).
    The query IS run — it raises today → xfail. When Zeta closes the gaps it
    succeeds → strict xfail turns that into an XPASS failure, prompting the
    marker's removal."""
    cg = _require_env()
    _seed()
    _cg_query(cg, VLP_KNOWN_GAP[1])


if __name__ == "__main__":
    cg = _resolve_cg_bin()
    if cg is None:
        print("cg binary not found", file=sys.stderr)
        sys.exit(2)
    if not _zeta_reachable():
        print(f"zeta-databricks not reachable at {ZETA_URL}", file=sys.stderr)
        sys.exit(2)
    _run_all(cg)
    print("ALL ZETA TRANSPORT CHECKS PASSED")
