# DeltaGraph — Implementation Plan

A phased plan for porting ClickGraph to Databricks / Delta Lake as **DeltaGraph**, while keeping a single shared codebase. Drafted 2026-05-15.

## TL;DR

- **One repo, one library crate, two binaries** (`clickgraph`, `deltagraph`). Dialect is a runtime choice backed by a small set of traits.
- **~3.4% of code is ClickHouse-specific** (~7,400 of 215,924 LoC). The Cypher parser, query planner, Bolt protocol, and most of the render plan are dialect-neutral. Forking would duplicate 200K LoC to specialize 7K.
- **DeltaGraph v1 = remote-only**, executing against Databricks SQL Warehouse via the Statement Execution REST API. No embedded mode (chDB stays exclusive to ClickGraph).
- The refactor required to make this clean *also* pays back as ClickGraph simplification — most notably splitting `plan_builder_utils.rs` (12K-line regression hotspot) and pulling SQL strings out of `render_plan/`.
- **Estimated effort: 4–6 weeks** with the codebase in head. Phased so ClickGraph stays green throughout.

## 1. Architecture

### Crate layout (unchanged on disk, new traits inside)

```
clickgraph/                  # Core library + clickgraph binary + deltagraph binary
  src/
    open_cypher_parser/      # Unchanged — dialect-neutral
    query_planner/           # Unchanged — dialect-neutral
    render_plan/              # Pulled apart: structure stays here, SQL text moves out
    sql_generator/            # RENAMED from clickhouse_query_generator/
      mod.rs                  # SqlEmitter trait, common emission helpers
      ast.rs                  # Optional: thin SQL AST (see §6)
      emitters/
        clickhouse.rs         # Current logic, behind the trait
        databricks.rs         # New
      function_mapper/
        mod.rs                # FunctionMapper trait
        clickhouse.rs
        databricks.rs
      vlp/                    # CTE structure (neutral) + per-dialect text
        structure.rs          # Anchor + recursive step as a tree (no SQL text)
        clickhouse.rs         # current variable_length_cte.rs lives here
        databricks.rs
    executor/
      mod.rs                  # QueryExecutor trait (already roughly exists)
      clickhouse_http.rs      # ex server/clickhouse_client.rs
      chdb_embedded.rs        # unchanged
      databricks_sql.rs       # NEW — Statement Execution REST
    catalog/                  # ex graph_catalog/
      mod.rs                  # CatalogProbe trait
      clickhouse_probe.rs     # ex engine_detection.rs
      databricks_probe.rs     # Unity Catalog (3-tier)
    server/
      bolt_protocol/          # Unchanged — already dialect-neutral
      http/                   # Unchanged — already dialect-neutral
  bin/
    clickgraph.rs             # defaults Dialect::ClickHouse
    deltagraph.rs             # defaults Dialect::Databricks
```

No new workspace member. Single library crate, two `[[bin]]` entries.

### The traits

```rust
// sql_generator/mod.rs
pub trait SqlEmitter: Send + Sync {
    fn dialect(&self) -> Dialect;
    fn function_mapper(&self) -> &dyn FunctionMapper;
    fn quote_ident(&self, name: &str) -> String;
    fn literal(&self, value: &Value) -> String;
    fn emit_vlp_recursive(&self, plan: &VlpStructure) -> String;
    fn emit_select(&self, select: &RenderSelect) -> String;
    fn emit_join(&self, join: &RenderJoin) -> String;
    fn emit_array_subscript(&self, base: &str, index: &str) -> String; // CH 1-based vs Spark element_at
    fn emit_collect(&self, expr: &str) -> String;                       // groupArray vs collect_list
    fn emit_unnest(&self, array_expr: &str, alias: &str) -> String;     // arrayJoin vs LATERAL VIEW explode
    fn supports_recursive_cte(&self) -> bool { true }
    fn null_safe_equal(&self) -> &'static str;                          // <=> in Spark; isNotNull(...) AND ... in CH
}

// sql_generator/function_mapper/mod.rs
pub trait FunctionMapper: Send + Sync {
    fn map_scalar(&self, name: &str, args: &[String]) -> Result<String, FunctionError>;
    fn map_aggregate(&self, name: &str, args: &[String], distinct: bool) -> Result<String, FunctionError>;
    fn map_temporal(&self, name: &str, args: &[String]) -> Result<String, FunctionError>;
}

// executor/mod.rs (formalize what exists today)
#[async_trait]
pub trait QueryExecutor: Send + Sync {
    async fn execute(&self, sql: &str, params: &Params) -> Result<ResultSet>;
    fn dialect(&self) -> Dialect;
}

// catalog/mod.rs
#[async_trait]
pub trait CatalogProbe: Send + Sync {
    async fn list_tables(&self, namespace: &Namespace) -> Result<Vec<TableInfo>>;
    async fn describe_columns(&self, table: &QualifiedName) -> Result<Vec<ColumnInfo>>;
    async fn detect_engine(&self, table: &QualifiedName) -> Result<EngineHint>;
}
```

### Why one library crate, not two

A `deltagraph` crate that depends on `clickgraph-core` would force every internal refactor in core to be a public API change. With one crate, the trait boundary is internal and you can iterate. The two `bin/*.rs` files are ~20 lines each — they only differ in which `SqlEmitter` and `QueryExecutor` they wire up by default.

## 2. SQL dialect mapping

### Function translation (ClickHouse → Spark SQL)

| ClickHouse | Spark SQL / Databricks | Notes |
|---|---|---|
| `groupArray(x)` | `collect_list(x)` | order not guaranteed in either; same semantics |
| `groupUniqArray(x)` | `collect_set(x)` | |
| `arrayElement(a, i)` | `element_at(a, i)` | both 1-indexed; `a[i]` in Spark is 0-indexed — **do not use `[]`** |
| `arrayMap(x -> f(x), a)` | `transform(a, x -> f(x))` | |
| `arrayFilter(x -> p(x), a)` | `filter(a, x -> p(x))` | |
| `arrayCount(x -> p(x), a)` | `size(filter(a, x -> p(x)))` | |
| `arraySort(a)` | `array_sort(a)` | |
| `arrayJoin(a)` | `LATERAL VIEW explode(a) t AS x` | **structural difference** — needs FROM-clause rewrite |
| `has(a, x)` | `array_contains(a, x)` | |
| `tuple(...)` | `struct(...)` | |
| `toString(x)` | `cast(x as string)` | |
| `toInt64(x)` | `cast(x as bigint)` | |
| `toFloat64(x)` | `cast(x as double)` | |
| `toDateTime(x)` | `to_timestamp(x)` | |
| `toUnixTimestamp(x)` | `unix_timestamp(x)` | |
| `minOrNull(x)` | `min(x)` | Spark `min` already returns NULL for empty |
| `ifNull(x, y)` | `coalesce(x, y)` or `ifnull(x, y)` | |
| `if(c, a, b)` | `if(c, a, b)` | works in both, but Spark also has CASE — prefer `CASE` for portability |
| `length(a)` (array) | `size(a)` | Spark `length` is string-only |
| `lengthUTF8(s)` | `length(s)` | string length |
| `argMin(v, k)` / `argMax(v, k)` | `min_by(v, k)` / `max_by(v, k)` | |
| `countDistinct(x)` | `count(distinct x)` | |
| `countIf(p)` | `count_if(p)` (DBR 13.1+) or `sum(if(p, 1, 0))` | |
| `quantile(0.5)(x)` | `percentile_approx(x, 0.5)` | |
| ``m['k']`` (Map access) | `element_at(m, 'k')` or `m['k']` | both work; prefer `element_at` for NULL-safety |
| `JSONExtract(j, 'p')` | `j:p` (Databricks shorthand) or `get_json_object(j, '$.p')` | |
| `tupleElement(t, 'f')` | `t.f` | for struct fields |
| `LowCardinality(T)` | `T` | drop the wrapper — Spark has no equivalent, transparent |
| `Nullable(T)` | `T` (Spark types are nullable by default) | |

A function-mapping unit test that pins each row of this table is the safest insurance.

### Recursive CTE / VLP

ClickHouse and Spark SQL both accept `WITH RECURSIVE`. Spark 3.4+ / DBR 14.1+ is required. Two practical differences:

1. **Photon does not accelerate recursive CTEs** — VLP queries on DeltaGraph will run on standard Spark, not Photon. Expect 2–10× slower than ClickHouse on equivalent shapes. Communicate this to users.
2. **Path materialization shape** — ClickGraph today uses `groupArray` to accumulate path elements inside the recursive step, then `arrayJoin` to unfold for the final result. In Spark, accumulate with `collect_list` and unfold with `LATERAL VIEW explode`, which moves from the SELECT list into the FROM clause. The neutral `VlpStructure` in `sql_generator/vlp/structure.rs` should expose "accumulator" and "unfolder" as logical operations; the dialect emitter chooses syntax.

`variable_length_cte.rs` (~850 LoC) is centralized — this is the single biggest porting target.

### Types & literals

`graph_catalog/schema_types.rs::to_sql_literal(value, dialect)` already has a PostgreSQL stub. Extend the same pattern:

- `DateTime` → `toDateTime('...')` (CH) vs `timestamp '...'` (Spark)
- `Date` → `toDate('...')` (CH) vs `date '...'` (Spark)
- `UUID` → `toUUID('...')` (CH) vs `'...'` (Spark — no native UUID, store as STRING)
- String escaping: CH backslash-escape, Spark single-quote-double; SQL injection surface is the same.

### NULL & equality semantics

Spark has `IS NOT DISTINCT FROM` and `<=>` for NULL-safe equality. ClickGraph currently leans on ClickHouse's `join_use_nulls=1` session setting for OPTIONAL MATCH. **Spark does not need this** — standard SQL NULL semantics apply to LEFT JOIN. This is a *simplification* on the Databricks side, but watch for tests that assume the `join_use_nulls` semantics.

### Catalog (3-tier vs 2-tier)

- ClickHouse: `database.table`
- Databricks Unity Catalog: `catalog.schema.table`

Add an optional `catalog` field to the YAML schema config, used only by the Databricks probe. Schema YAML stays human-portable; only fully-qualified names change.

## 3. Databricks SQL Warehouse executor

### API choice: Statement Execution API

`POST /api/2.0/sql/statements` — submit; returns statement_id. Poll `GET /api/2.0/sql/statements/{id}` until `state=SUCCEEDED`, then read `result.data_array` (or chunked `external_links` for large results). Async with polling; no JDBC driver, no JVM, no extra deps beyond `reqwest`.

### Crate / module

```
src/executor/databricks_sql.rs
```

Configuration:

```rust
pub struct DatabricksConfig {
    pub workspace_url: String,            // https://<workspace>.cloud.databricks.com
    pub warehouse_id: String,             // SQL Warehouse ID
    pub auth: DatabricksAuth,             // PersonalAccessToken | OAuthM2M
    pub catalog: Option<String>,          // Unity Catalog default
    pub schema: Option<String>,
    pub wait_timeout_seconds: u32,        // default 50 (max allowed by API for sync mode)
    pub disposition: ResultDisposition,   // INLINE for small results, EXTERNAL_LINKS for large
}
```

### Auth

- **PAT** for v1 — simplest, matches ClickHouse `CLICKHOUSE_USER`/`PASSWORD` ergonomics. Env vars: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_WAREHOUSE_ID`.
- **OAuth M2M** as fast-follow — Databricks is steering customers toward service principals.

### Result handling

Result rows arrive as `data_array: [[v, v, v], ...]` with a parallel `schema.columns`. Map Databricks types to ClickGraph `Value` enum:

| Databricks | ClickGraph Value | Note |
|---|---|---|
| BOOLEAN | Bool | |
| TINYINT/SMALLINT/INT/BIGINT | Int64 | upcast to i64 |
| FLOAT/DOUBLE | Float64 | |
| STRING | String | |
| DATE | Date | ISO string |
| TIMESTAMP | DateTime | ISO string |
| DECIMAL | String | preserve precision; or `Decimal` if you add it |
| ARRAY<T> | List<Value> | JSON array in `data_array` |
| MAP<K,V> | Map<Value, Value> | JSON object |
| STRUCT | Map<String, Value> | JSON object with field names |

### Error mapping

Databricks error codes (e.g., `PARSE_SYNTAX_ERROR`, `TABLE_OR_VIEW_NOT_FOUND`) map to existing `ClickGraphError` variants; add a `DatabricksError(code, message)` for the long tail.

### What does *not* port: chDB

chDB is in-process ClickHouse. No Spark equivalent that fits in a Rust binary. **DeltaGraph v1 ships remote-only.** The `--features embedded` flag on `clickgraph-ffi` continues to exist; the Databricks build of FFI / Python / Go bindings simply doesn't enable it.

## 4. Phased plan

Each phase ends with main green: existing ClickGraph tests pass.

### Phase 0 — Decoupling (2 weeks, ClickGraph-only changes)

Goal: introduce the trait boundary without changing behavior. ClickGraph stays the only emitter.

- **0.1** Introduce `Dialect` enum + `SqlEmitter` trait (empty default impls forwarding to existing code).
- **0.2** Move ClickHouse-specific function names out of `render_plan/select_builder.rs`, `render_expr.rs`, etc. into `FunctionMapper::clickhouse`. Audit with `rg 'groupArray|arrayMap|arrayJoin|arrayElement|arrayCount|arrayFilter|argMin|argMax|tuple\(|toString|toInt64|minOrNull|LowCardinality|countIf' src/render_plan/`.
- **0.3** Rename `clickhouse_query_generator/` → `sql_generator/`. Move existing code into `sql_generator/emitters/clickhouse.rs`. Re-export under old paths for one cycle to avoid mass churn.
- **0.4** Extract VLP structure from `variable_length_cte.rs` — split into `vlp/structure.rs` (neutral tree of Anchor/Step/Accumulator/Unfolder) and `vlp/clickhouse.rs` (text emission).
- **0.5** **Split `plan_builder_utils.rs`** (12K lines, regression hotspot). Proposed slices:
  - `with_to_cte/` (WITH→CTE transformation, ~3K)
  - `cte_property_rewrite.rs` (~2K)
  - `optional_match_postprocess.rs` (~2K)
  - `node_identity_rewrite.rs` (~1.5K)
  - `aggregation_helpers.rs` (~1.5K)
  - the rest stays in `plan_builder_utils.rs` as a clearly-named "misc" home for the truly cross-cutting helpers
  This phase is *the* simplification dividend. Even if DeltaGraph never ships, this is worth doing.
- **0.6** Snapshot tests stay against ClickHouse output. Run `cargo test` after each commit.

**Exit criteria:** all 1,600 Rust tests + ~3,026 Python integration tests green. No new dialect added yet.

### Phase 1 — Databricks emitter (1.5 weeks)

- **1.1** `emitters/databricks.rs` skeleton. Implement easy methods first: `quote_ident`, `literal`, basic `emit_select` / `emit_join`.
- **1.2** `function_mapper/databricks.rs` — complete the table in §2 with unit tests pinning each mapping.
- **1.3** `vlp/databricks.rs` — recursive CTE emission, lateral explode for unfolding.
- **1.4** Snapshot tests: add `.expected.databricks.sql` siblings for the ~31 `insta` snapshot tests. Use a `#[parametrize_dialect]` macro or rstest-style helper to run each render test against both emitters.
- **1.5** `cargo test --features databricks_emitter` runs both emitters; CI runs both.

**Exit criteria:** all snapshot tests have a Databricks baseline; both `cargo test`s green. No execution yet.

### Phase 2 — Databricks executor (1 week)

- **2.1** `executor/databricks_sql.rs` — Statement Execution API client over `reqwest`. Submit / poll / fetch.
- **2.2** Result row → `Value` mapping (§3).
- **2.3** Error mapping.
- **2.4** Auth: PAT v1, OAuth M2M behind a feature flag for v1.1.
- **2.5** Integration test harness — small Delta table seeded by a setup script; runs in CI against a Databricks SQL Warehouse (use a free-tier serverless warehouse, gated by `DATABRICKS_TOKEN` secret — skip when unset).

**Exit criteria:** `MATCH (n:Person) RETURN n.name LIMIT 10` works end-to-end against a real Databricks warehouse.

### Phase 3 — Catalog & schema discovery (0.5 week)

- **3.1** `catalog/databricks_probe.rs` — `SHOW TABLES IN catalog.schema`, `DESCRIBE TABLE EXTENDED`.
- **3.2** Optional `catalog:` field in `GraphSchema` YAML.
- **3.3** `cg schema discover` (LLM-assisted) — already provider-pluggable. Test against Unity Catalog tables.

### Phase 4 — Packaging & bindings (0.5 week)

- **4.1** `bin/deltagraph.rs` — defaults to `Dialect::Databricks`, reads `DATABRICKS_*` env vars, exposes `--databricks-host` / `--warehouse-id` flags.
- **4.2** `cg --dialect databricks ...` — `cg` CLI gets a dialect flag.
- **4.3** Bolt protocol works unchanged. Test Neo4j Browser → DeltaGraph → Databricks end-to-end. **This is the killer demo.**
- **4.4** Go / Python / FFI bindings: add `RemoteConfig::Databricks` variant alongside the existing ClickHouse `RemoteConfig`.

### Phase 5 — Docs, release (0.5 week)

- Update `STATUS.md` with DeltaGraph status.
- New top-level `docs/deltagraph/` with quickstart, supported features, perf notes.
- Mark in README that the library has two dialects, with binaries `clickgraph` and `deltagraph`.
- Decide whether DeltaGraph ships from this repo's releases or its own. Recommendation: same repo, separate release artifacts (`deltagraph-0.7.0-linux-x86_64.tar.gz`).

**Total: 5.5 weeks, padded to 6 for slack.**

## 5. Test strategy

- **Snapshot tests:** dual-baseline. ~31 `insta` snapshots × 2 dialects = 62 baselines, plus ~103 VLP/CTE tests duplicated. Use rstest-style parametrization so adding a third dialect later (Postgres? DuckDB?) is one line per test.
- **Integration tests:**
  - ClickGraph: existing 3,026 pytest suite hits ClickHouse — no change.
  - DeltaGraph: a smaller seed-and-run suite against a Databricks workspace, gated on `DATABRICKS_TOKEN` env. ~50–100 queries covering the LDBC SNB benchmark subset. Skip in OSS CI unless secret is present; run nightly with a paid workspace.
- **Function mapping unit tests:** pin every row in the §2 table with an assertion. These are cheap and catch the most common port mistakes.
- **VLP correctness:** for each VLP query in the existing LDBC suite, compare result *sets* (not row order) between ClickGraph and DeltaGraph against equivalent data. Differences indicate semantic bugs.

## 6. Optional: SQL AST layer (defer to Phase 7)

The current generator goes straight from `RenderPlan` to SQL text. An intermediate SQL AST (consider `sqlparser-rs`) would:

- Make dialect emission a `Display` impl per node, with the AST itself dialect-neutral.
- Make CTE flattening (currently in `to_sql_query.rs::flatten_all_ctes`) an AST pass.
- Enable downstream SQL optimization passes (predicate pushdown into CTE bodies, dead column elimination after WITH barriers, etc.) that currently can't run because the input is already a string.

This is **not** required for DeltaGraph v1 and would add 2–3 weeks. List it as a Phase 7 simplification project that the dialect work has *enabled* but doesn't depend on.

## 7. Risks & open questions

| Risk | Likelihood | Mitigation |
|---|---|---|
| VLP performance on Databricks (no Photon) makes it unusable for some shapes | Medium | Document up front. Offer fixed-length VLP path with chained JOINs (already exists in ClickGraph for `*N..N`). Measure on LDBC SNB. |
| Spark recursive CTE limitations vs ClickHouse — undocumented edge cases | Medium | Build the LDBC integration suite early in Phase 2 to surface these. |
| `arrayJoin` → `LATERAL VIEW explode` is a FROM-clause restructure, not a function swap | High | Treat as Phase 1 spike before committing to the function-mapper design. Validate on 2–3 representative queries first. |
| Databricks Statement Execution API rate limits / latency for low-throughput agent use | Low | Per-query overhead is ~300–800ms warm. Document. Offer connection-reuse if it materially helps. |
| Splitting `plan_builder_utils.rs` introduces regressions | Medium | Do it under a single PR with the full 1,600-test suite green at every commit. No behavior changes, only file moves. |
| Two binaries from one crate confuses users about which to install | Low | Clear README, separate release artifacts, separate landing pages if marketed as distinct products. |
| Dialect drift — features land in one emitter and not the other | Medium-High over time | CI must require both dialects green. Treat the parametrized snapshot tests as the contract. |

### Open questions for you

1. **Branding** — DeltaGraph as a sibling product (separate marketing, separate release notes) or as a build mode of ClickGraph ("ClickGraph for Databricks")? My weak preference is sibling — different audiences, different value props (zero-ops embedded vs Databricks-native graph).
2. **Schema YAML** — extend the existing format with optional `catalog:` and `engine: delta` fields, or define a sibling `*.deltagraph.yaml` that imports from the ClickGraph one? Recommendation: extend; one format, dialect-neutral by default.
3. **LDBC benchmark on Databricks** — do you have or want access to a Databricks workspace for the benchmark suite? The numbers will matter for positioning. Worth doing in Phase 2.
4. **Photon-compatible fast path** — for queries that *don't* use recursive CTEs (most non-VLP Cypher), DeltaGraph could be quite competitive. Worth measuring early to inform messaging.
