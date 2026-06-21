# Dialect-neutral SQL rendering — design

Status: **proposal for review** (no code yet). Author: DeltaGraph dialect work.
Companion to `docs/design/DELTAGRAPH_PLAN.md`.

## TL;DR

We do **not** need to build a new IR. `RenderPlan` + `RenderExpr` already *are*
a ~75% dialect-neutral relational/expression IR. The problem is that the
**pretty-printing** of that IR is ClickHouse-baked and **duplicated across four
render paths** that drift apart — which is the recurring root cause of the
Spark dialect bugs (arrayConcat, CONTAINS, tuple, FINAL all had multiple sites).

The evolution is therefore:
1. Promote `FunctionMapper` into a fuller **`Dialect`** trait that owns *every*
   divergence point (operators, type spellings, CAST syntax, literals, quoting,
   the handful of structural rewrites), and
2. Route **one** printer through it, **collapsing the four render paths into one**.

Each step keeps ClickHouse output **byte-identical** and ships as its own
reviewed PR. The structural gaps (types/CAST, intervals, `arraySlice`,
`arrayJoin`, `List`, tuple-equality) stop being scattered `if dialect ==`
branches and become `Dialect` methods.

---

## 1. Current state (grounded)

### What already exists and is neutral (~75%)
- **`RenderPlan`** (`src/render_plan/mod.rs:104`): `ctes`, `select`, `from`,
  `joins`, `array_join`, `filters`, `group_by`, `having`, `order_by`, `skip`,
  `limit`, `union`. Structurally dialect-neutral.
- **`RenderExpr`** (`src/render_plan/render_expr.rs:691`): 17+ structural
  variants — `Literal`, `Column`, `PropertyAccessExp`, `ScalarFnCall`,
  `AggregateFnCall`, `OperatorApplicationExp`, `Case`, `List`, `InSubquery`,
  `ExistsSubquery`, `ReduceExpr`, `MapLiteral`, `ArraySubscript`,
  `ArraySlicing`, `CteEntityRef`, … Expressions are *structured*, not strings.
- **`FunctionMapper`** (`src/sql_generator/function_mapper/`): already abstracts
  ~30 names + a few structural ops (`min_if`, `array_literal`, `quote_alias`,
  the array casts) per dialect, read from the task-local dialect.
- **Function registry** `name_for(dialect)` + `databricks_name` (~20 entries).
- **`SqlEmitter`** boundary (`src/sql_generator/mod.rs:95`) + `emitter_for`.

### What's ClickHouse-specific (~25%) — the actual work
- **Four render paths** that each turn expressions into SQL and drift:
  - **A. `RenderExpr::to_sql()`** (`src/sql_generator/emitters/clickhouse/to_sql_query.rs`) — canonical, all 17
    variants, dialect-aware via FunctionMapper/registry. ~1,300 lines.
  - **B. `render_expr_to_sql_string()`** (`plan_builder_helpers.rs:692`) —
    JOIN dependency sort; 12 variants; **no dialect awareness**; `"TRUE"` fallback.
  - **C. `render_expr_to_sql_string()`** (`cte_extraction.rs:1020`) — VLP CTE
    building; 12 variants; identifier quoting; partial dialect awareness.
  - **D. `LogicalExpr::to_sql()`** (`to_sql.rs:119`) — EXISTS/pattern-count
    generation from the *logical* plan; returns `Result`; predates `RenderExpr`.
- **`RenderExpr::Raw(String)`** and `CteContent::RawSql(String)` escape hatches
  (~5–10% of expressions) — opaque CH strings the printer can't reason about.
- **CH-baked leaves** still inline in path A and elsewhere: operator symbols,
  type names (`Int64`, `Array(...)`, `Nullable(...)`), `CAST(x,'T')` syntax,
  interval functions, JSON idioms.

### The divergence surface the printer must own (full checklist)
From the inventory (see appendix): function names (FunctionMapper + registry),
**type-name spellings + CAST syntax**, tuple/struct construction *and* equality,
JSONPath arg shaping, array-literal syntax, identifier/alias quoting,
structural rewrites (`array_count`→`size(filter(...))`, `min_if`→`CASE`,
`arrayJoin`→`LATERAL VIEW explode`, BFS-undirected CTE shape, `position` arg
order), keyword support (`FINAL`, `SETTINGS`, `SAMPLE`), temporal epoch-millis
wrapping, and `if`/`CASE` + regex `match()` idioms.

---

## 2. The design

### 2.1 The `Dialect` trait (one printer config)
Grow `FunctionMapper` into a `Dialect` trait that is the **single source of
truth** for everything that differs. It is *config + small structural helpers*,
not a visitor — a single printer walks the IR and calls into it.

```
pub(crate) trait Dialect: Send + Sync {
    // ---- naming (mostly exists as FunctionMapper) ----
    fn function_name(&self, canonical: &str) -> Cow<str>;   // registry-backed
    fn operator_symbol(&self, op: Operator) -> &str;        // =, AND, ||/concat, ...
    fn aggregate_name(&self, canonical: &str) -> Cow<str>;

    // ---- literals & identifiers ----
    fn quote_ident(&self, name: &str) -> String;            // `x` vs "x"
    fn quote_alias(&self, name: &str) -> String;            // exists
    fn array_literal(&self, elems: &[String]) -> String;    // [..] vs array(..)  (exists)
    fn string_literal(&self, s: &str) -> String;
    fn bool_literal(&self, b: bool) -> &str;

    // ---- types & casts (NEW — closes the biggest gap) ----
    fn type_name(&self, t: SqlType) -> String;              // Int64 vs BIGINT, Array<T>, Struct<..>
    fn cast(&self, expr: &str, t: SqlType) -> String;       // CAST(x,'T') vs CAST(x AS T)
    fn nullable(&self, t: SqlType) -> SqlType;              // CH Nullable(T) vs Spark (identity)

    // ---- structural rewrites (the handful that aren't name swaps) ----
    fn contains(&self, haystack: &str, needle: &str) -> String;      // exists (position arg order)
    fn tuple(&self, fields: &[String]) -> String;                    // tuple(..) vs struct(..)
    fn min_if(&self, val: &str, cond: &str) -> String;               // exists
    fn array_count(&self, var: &str, pred: &str, arr: &str) -> String; // arrayCount vs size(filter)
    fn array_explode(&self, arr: &str) -> Generator;                 // arrayJoin vs LATERAL VIEW
    fn interval(&self, unit: TimeUnit, n: &str) -> String;           // toIntervalDay vs INTERVAL
    fn json_extract_string(&self, blob: &str, path: &str) -> String; // JSONExtractString vs get_json_object($.x)

    // ---- capabilities ----
    fn supports_final(&self) -> bool;                       // exists
    fn supports_settings(&self) -> bool;
}
```

Two impls: `ClickHouseDialect`, `DatabricksDialect`. `for_dialect(SqlDialect)`
selects one (mirrors `function_mapper::for_dialect`). `SqlType` is a small,
dialect-neutral type enum derived from the existing `SchemaType` (closes the
"no `to_spark_type()`" gap — types become a printer concern, not hardcoded CH).

### 2.2 One printer
A single `print_expr(&RenderExpr, &dyn Dialect) -> String` and
`print_plan(&RenderPlan, &dyn Dialect) -> String`. Path A's logic becomes this
printer with every CH-literal swapped for a `dialect.*` call. Paths B/C/D are
**deleted** and their callers call the one printer (see migration).

The `SqlEmitter` impls collapse to `print_plan(plan, for_dialect(self.dialect))`.
The task-local dialect lookup stays as the default, so callers don't all need
threading changes on day one.

### 2.3 Hard cases
- **Recursive CTEs (VLP)** — already structured in `Cte` with rich metadata
  (`vlp_*`, `columns`). The CTE *body* is a `RenderPlan` (or `RawSql`). Printing
  it is the same `print_plan`; the only dialect-divergent bits inside are
  already enumerated (array funcs, casts, cycle check `has`→`array_contains`,
  undirected branch shape). The undirected 2-child-limit rewrite stays a
  `Dialect`-gated structural choice. **No new IR needed** — VLP is the proof
  that `RenderPlan` is expressive enough.
- **`Raw`/`RawSql` escape hatches** — shrink, don't eliminate on day one. Each
  is a place where structured rendering was skipped; migrate opportunistically
  to real `RenderExpr`/`RenderPlan` so the printer (and Spark) can see them.
  Track the remaining count as a debt metric.
- **`PatternCount` / `ExistsSubquery`** carry pre-rendered SQL today; they get
  printed structurally once Path D (LogicalExpr) is unified.

---

## 3. Migration plan (incremental, CH byte-stable, each a reviewed PR)

The invariant for every phase: **ClickHouse output is byte-identical** (guarded
by the existing ~1,600 tests + a golden-SQL snapshot set we add in Phase 0).

- **Phase 0 — safety net.** Add golden-SQL snapshot tests over a representative
  query corpus (single + composite ID, VLP, OPTIONAL MATCH, aggregates, WITH,
  UNION) for **both** dialects. This is the regression harness for everything
  below. *(Small, high value, do first.)*

- **Phase 1 — grow `Dialect`, migrate Path A leaf-by-leaf.** Add the trait
  (superset of FunctionMapper), give it CH + Databricks impls, and replace the
  CH-literal leaves in `RenderExpr::to_sql()` with `dialect.*` calls **one
  variant at a time**. CH impl returns exactly today's spellings → byte-stable.
  This also lands the **type/CAST** work (`type_name`/`cast`/`nullable`) and the
  remaining leaf gaps (`arraySlice`, intervals, `match`) as `Dialect` methods
  rather than scattered branches.

- **Phase 2 — collapse the four paths.** Point Path B (JOIN dep-sort) and Path
  C (CTE extraction) at the one printer; delete their partial copies. Unify Path
  D by rendering `LogicalExpr` through the same `Dialect` (or converting to
  `RenderExpr` first). This is the highest-leverage step — it's what stops the
  drift that caused the recent bugs — but also the **highest-risk**, so it comes
  after the golden harness and after Path A is fully `Dialect`-routed.

- **Phase 3 — structural idioms.** `arrayJoin`→`LATERAL VIEW explode` (needs a
  FROM-clause `Generator` concept), `if`→`CASE` where required, cross-identifier
  composite equality → per-column `AND`. These are now localized `Dialect`
  decisions.

- **Phase 4 — shrink `Raw`.** Convert the highest-traffic `RenderExpr::Raw` /
  `CteContent::RawSql` sites to structured forms; leave a documented, shrinking
  tail.

Phases 0–1 are mostly mechanical and low-risk; 2 is the architectural payoff; 3–4
are cleanup. We can stop after any phase with a coherent, better-off codebase.

## 4. Risks & mitigations
- **Silent CH regressions** → golden snapshots (Phase 0) + the existing suite;
  every phase asserts byte-identical CH.
- **Path-collapse behavioral drift** (Paths B/C/D have subtle differences, e.g.
  Path C quotes identifiers, Path B falls back to `"TRUE"`) → migrate one caller
  at a time, diff output, keep the quirks that are load-bearing as `Dialect`
  behaviors.
- **Scope creep** → the trait is additive; we never block on "perfect IR." The
  `Raw` tail is allowed to persist.

## 5. Non-goals
- A third dialect (Postgres/DuckDB) — the design *enables* it but we don't build
  it. The `Dialect` trait is the seam where it would slot in.
- Replacing the planner/optimizer — only the RenderPlan→SQL step changes.
- A standalone SQL-AST crate / `sqlparser` adoption — `RenderExpr` is the AST.

## 6. Relationship to the remaining leaf gaps
The not-yet-done leaf fixes (`arraySlice`, type/CAST, intervals) are folded into
**Phase 1** as `Dialect` methods, so we don't patch them twice. If you want one
more standalone leaf PR before starting Phase 0, `arraySlice` is the candidate;
otherwise it rides along in Phase 1.

---

## Appendix — divergence inventory (checklist the `Dialect` trait must cover)
(See the two investigation outputs for file:line detail.)
- **Name swaps** (~30 FunctionMapper + ~20 registry): `groupArray↔collect_list`,
  `arrayElement↔element_at`, `countIf↔count_if`, `has↔array_contains`,
  `arrayConcat↔concat`, casts (`toInt64↔bigint`, …), `tuple↔struct`,
  `anyLast↔any_value`, date parts, …
- **Structural**: `min_if`→CASE, `array_count`→`size(filter)`,
  `arrayJoin`→`LATERAL VIEW explode`, `position` arg order, undirected-BFS CTE,
  array-literal `[..]`↔`array(..)`, empty-array casts, `quote_alias`.
- **Types/CAST** (gap): `Int64↔BIGINT`, `Float64↔DOUBLE`, `String↔STRING`,
  `UInt8↔TINYINT`, `DateTime64(3)↔TIMESTAMP`, `Date32↔DATE`, `UUID↔STRING`,
  `Array(T)↔ARRAY<T>`, `Nullable(T)`→(strip), `CAST(x,'T')↔CAST(x AS T)`.
- **Clauses** (gaps surfaced by Phase 0 goldens): pagination — CH
  `LIMIT {skip}, {n}` vs Spark `LIMIT {n} OFFSET {skip}`; `IN` list — Spark needs
  `IN (a, b)`, not the array-literal swap's `IN array(a, b)`.
- **Keywords/temporal**: `FINAL` (CH only), `SETTINGS`/`SAMPLE` (CH only),
  `toIntervalX`↔`INTERVAL`, epoch-millis wrapping
  (`fromUnixTimestamp64Milli`↔`timestamp_millis`).
- **Idioms** (gap): `if`↔`CASE`, regex `match()`↔`rlike`, JSON
  `JSONExtractString(b,'f')`↔`get_json_object(b,'$.f')`.
- **JSON row construction** (gap surfaced by the `vlp_multi_type` golden):
  multi-type VLP builds `_properties` columns via CH-only
  `formatRowNoNewline('JSONEachRow', ...)` — invalid on Spark, needs a
  `to_json(struct(...))`-style equivalent. The golden locks the current
  (Spark-incomplete) output so the fix shows as a diff.
