# DeltaGraph ↔ zeta-databricks transport gate

End-to-end test of the **transport/integration layer** the Spark/Delta docker
harness can't reach: the real `DatabricksSqlExecutor` talking to a real
Databricks Statement Execution API, served locally by
[`zeta-databricks-rest`](../../../zeta/crates/zeta-databricks-rest) (the
`zeta-server-bin --features wire-databricks` listener).

```
Cypher → cg --dialect databricks → DatabricksSqlExecutor
       → POST /api/2.0/sql/statements → zeta-databricks → rows
```

This complements, not replaces, the other two environments:

| Layer | Where it's validated |
|---|---|
| Spark-runtime dialect fidelity (recursive CTE, explode, …) | `tests/spark_smoke/` (Spark/Delta docker) |
| Result-set parity vs ClickHouse | `tests/spark_smoke/test_ldbc_parity.py` |
| **Executor wiring, REST submit/poll, JSON decode, VLP array builtins** | **here** |
| EXTERNAL_LINKS, OAuth, real perf/cold-start | live Databricks |

The VLP case (`KNOWS*1..2`) deliberately exercises the `array_contains` and
array-`concat` builtins added to Zeta — proving variable-length paths run
through the REST path, not just the Spark container. See
[`../../docs/deltagraph/ZETA_FIDELITY.md`](../../docs/deltagraph/ZETA_FIDELITY.md).

## Running

Gated (skips cleanly) by `CLICKGRAPH_ZETA_TESTS=1` plus a reachable server.

```bash
# 1. Build + start the Zeta Databricks REST listener (separate terminal)
cd ../../../zeta
cargo run -p zeta-server-bin --features wire-databricks -- --databricks-port 18099

# 2. Build cg with the databricks feature (from the clickgraph repo root)
cargo build -p clickgraph-tool --features databricks

# 3. Run the gate
CLICKGRAPH_ZETA_TESTS=1 \
ZETA_DATABRICKS_URL=http://127.0.0.1:18099 \
CG_BIN=$(pwd)/target/debug/cg \
  python3 tests/zeta_integration/test_zeta_transport.py
# (or via pytest, same env)
```

The harness seeds the LDBC mini dataset (`zeta_ldbc_seed.sql`) over the REST
API, then drives queries through `cg` with `CG_DATABRICKS_BASE_URL` pointed at
the listener.

## CI notes

Because Zeta lives in a sibling repo, this gate runs in environments that have
both checked out (the dev box, or a CI job that checks out both). It is not part
of the default `cargo test` run. The pure-executor transport logic (retry,
EXTERNAL_LINKS pagination, 401 handling) is *also* covered by wiremock unit
tests in `src/executor/databricks_sql.rs`, which DO run in clickgraph CI with no
external dependency — this gate adds the real cross-process round-trip on top.
