# DeltaGraph ↔ zeta-databricks: dialect fidelity boundary

`zeta-databricks` (`zeta/crates/zeta-databricks-rest/`) is an axum server that
implements the Databricks **Statement Execution API** (submit / poll / cancel,
warehouse CRUD, catalog/schema discovery) on top of the Zeta SQL engine, so
DeltaGraph can run end-to-end "as if" against a Databricks SQL Warehouse with
no live workspace. It is the local stand-in for the *transport + executor*
layer that the Spark/Delta docker container cannot exercise.

This doc records **what zeta-databricks can validate vs. what it cannot**, so we
don't mistake "zeta-databricks tests pass" for "Spark dialect validated." The
two local environments are complementary, not interchangeable.

Method: static analysis of the Zeta parser/planner/executor plus the actual
Spark-dialect SQL emitted by `cg --dialect databricks sql` for the high-risk
query shapes (flat join, aggregation, OPTIONAL MATCH, undirected VLP recursive
CTE). Not yet confirmed by executing the VLP SQL against a running Zeta — the
two gaps below are code-level absences, definitive enough to act on.

## Three environments, three jobs

| Concern | Spark/Delta docker | zeta-databricks REST | Live Databricks |
|---|---|---|---|
| Spark SQL dialect — flat / agg / string / OPTIONAL MATCH | ✅ | ✅ | ✅ |
| Spark SQL dialect — **VLP / BFS** (`array_contains`, array `concat`, array CAST, lateral aliases / ORDER BY) | ✅ | ✅ (closed, see below) | ✅ |
| Statement Execution REST transport, poll loop, executor wiring | ❌ | ✅ | ✅ |
| Warehouse lifecycle, schema/catalog discovery | ❌ | ✅ (start/stop endpoints) | ✅ |
| EXTERNAL_LINKS, OAuth M2M, real cold-start/auto-stop/429, perf | ❌ | ❌ | ✅ |

## What zeta-databricks DOES validate (Spark dialect features Zeta accepts)

Zeta is deliberately Spark-compatible, not just Postgres:

- **Backtick-quoted identifiers** — `zeta/crates/zeta-parser/src/backtick_identifiers.rs`
  rewrites Spark backticks (`` `friendId` ``) because sqlparser's
  PostgreSqlDialect rejects them. DeltaGraph quotes every alias in backticks.
- **`UNION DISTINCT`** — supported (`zeta-substrait` translate + `pg_regress_union` tests).
- **`WITH RECURSIVE`** — CTE `recursive` flag + `RecursiveCte` logical plan.
- **`collect_list()` / `collect_set()`** — mapped as Spark NULL-skipping aggregates
  (`zeta-planner/src/planner.rs:4001`).
- **`ARRAY<T>` angle-bracket casts** — `zeta-parser/src/datatype.rs` handles the
  `AngleBracket` form (`CAST(array() AS ARRAY<STRING>)`).
- **`array(...)` literal constructor** — `zeta-parser/src/convert.rs`.
- **`||` array concatenation** — `zeta-planner/src/plan.rs:1217`.

Combined with #1932 (alias preservation through OLAP routing, `schema`-field
scoping, path-param routes), this means the **flat-join, aggregation, OPTIONAL
MATCH, and string-function query families translate AND execute end-to-end
through the real REST path** on zeta-databricks. That is the half the docker
container cannot cover.

## What zeta-databricks does NOT validate — two concrete Zeta gaps

DeltaGraph's variable-length-path (VLP) and BFS shortestPath SQL depends on two
array primitives Zeta does not implement:

1. **`array_contains(path_nodes, end_node.id)`** — cycle detection in the
   recursive step. **Absent from Zeta entirely** (`grep -rin array_contains`
   over `zeta/` returns nothing).
2. **`concat(path_nodes, array(end_node.id))`** — path extension. Zeta's
   `CONCAT` (`zeta-server/src/lib.rs:64774`) is **string-only**: it stringifies
   every argument and returns `Value::String`, so it does not array-concat the
   way Spark's `concat` does. (Zeta does array-concat via `||`, but DeltaGraph
   emits the `concat(...)` function form.)

Original impact (historical): undirected/directed VLP (`*1..n`) and BFS
shortestPath did not run on zeta-databricks. **This has since been closed** —
see "VLP on Zeta — CLOSED" below; the four required Zeta features were added and
VLP (including `ORDER BY`) now runs end-to-end through the REST path.

## VLP on Zeta — CLOSED

Four Zeta features were added so DeltaGraph's full variable-length-path SQL
(including `ORDER BY`) executes end-to-end through the zeta-databricks REST path:

- **`array_contains(arr, x)`** scalar builtin — eval in
  `zeta/crates/zeta-server/src/lib.rs` (modeled on `ARRAY_POSITION`), return
  type `Boolean` in `zeta/crates/zeta-planner/src/resolve.rs`. Spark NULL
  semantics (NULL array → NULL; NULL search value → NULL; unprovable absence
  via a NULL element → NULL).
- **Array-overloaded `concat(...)`** — same eval file: when any operand is an
  array it array-concatenates (NULL operand → NULL, element type follows the
  first array); otherwise unchanged string concat. Return type is array-aware
  in `resolve.rs`, so a recursive-CTE column `concat(path_nodes, array(x))`
  unifies with its array anchor instead of degrading to Text.
- **`CAST(... AS ARRAY<T>)` at execution** — `cast_value` in
  `zeta-server/src/lib.rs` now retypes arrays element-wise (the VLP
  `path_relationships` accumulator emits `CAST(array() AS ARRAY<STRING>)`).
- **Spark lateral column aliases** — the planner (`zeta-planner/src/planner.rs`)
  inlines references to an earlier projection alias in the same SELECT list
  (`SELECT x AS id, id AS __order_col_0`), which DeltaGraph's `ORDER BY`
  desugaring relies on. A real FROM column wins over a same-named alias (Spark
  precedence). Affects all ORDER BY-on-expression queries, not just VLP.

Covered by `zeta/crates/zeta-server/tests/array_contains_concat.rs` (12 tests,
incl. a VLP-shaped recursive CTE with `CAST(array() AS ARRAY<STRING>)`) and
`lateral_column_alias.rs` (4 tests, incl. precedence + the DeltaGraph ORDER BY
shape). Validated end-to-end by the cross-stack transport gate
(`tests/zeta_integration/`): `MATCH (p)-[:KNOWS*1..2]-(f) … RETURN DISTINCT
f.id AS id ORDER BY id` runs cg → executor → REST → Zeta and returns the
correct ordered rows. (The unrelated pre-existing `Int32/Int64` failures in
`pg_regress_lateral_column_alias` / `_quoted_alias` reproduce on clean `main`
and are not touched by this change.)

## Bottom line

- **Transport, executor, lifecycle, discovery, flat/agg/string AND VLP/BFS
  (incl. ORDER BY)** → validated on **zeta-databricks** end-to-end
  (`tests/zeta_integration/`, CI-friendly, no warehouse, no docker).
- **Spark-runtime parity at scale** (Photon, exact engine semantics on large
  data) → still best confirmed on the **Spark/Delta docker** sweep; Zeta is a
  different engine that now accepts the same dialect.
- **EXTERNAL_LINKS, OAuth M2M, real cold-start/auto-stop/429, performance** →
  irreducibly **live Databricks**.
