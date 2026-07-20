# Stats-informed SQL generation (P-5)

Status: **S1 implemented** (this document + row-count cache + anchor ranking,
flag-gated). S2/S3 are design sketches only. Owner lane: P-5
(`docs/design/PRIORITIES.md` §2). Last updated: 2026-07-19.

## 0. Non-negotiable guardrails (PRIORITIES.md §1.7)

Backend statistics may influence **ordering only**: join order, anchor/FROM
choice, traversal direction. They must **never** change row membership — no
pruning UNION arms, no skipping tables, no changing predicates. Every
stats-driven decision must pick among orderings that are semantically
equivalent (same result multiset).

Stats-driven planning is **off by default** (`CLICKGRAPH_STATS_ENABLED`,
default `false`) and structurally unreachable in sql_only/test paths: the
planner only ever sees stats through a snapshot attached to the task-local
`QueryContext`, and nothing attaches one except the server's query handlers
when the flag installed the cache at startup. The byte-exact golden net
(`tests/rust/integration/sql_golden_tests.rs`, `corpus_sweep.rs`) therefore
stays deterministic and stats-less; a **separate** with-stats golden set locks
the flag-on behavior against a fixed fixture (§5).

## 1. Current heuristics inventory

Everywhere the planner picks among semantically-equal orderings today, the
tie-break is lexical or positional — never informed by data size.

### 1.1 `select_anchor()` — anchor/FROM choice

`src/query_planner/analyzer/graph_join/join_generation.rs` (pre-S1 at :550,
now :562). Chooses which FROM-marker join becomes the outer query's FROM
anchor. Tiers:

1. non-optional FROM markers whose `TableCtx::has_selective_filters()` is true
   (any inline `{prop: v}` property or pushed-down WHERE predicate),
2. non-optional FROM markers without filters,
3. optional (LEFT) FROM markers.

Within each tier the pre-S1 tie-break was **alphabetical by Cypher alias** —
`MATCH (a:BigTable), (b:TinyTable)` anchors on `a` purely because `a < b`.
`has_selective_filters()` is a **boolean**: one filter on a 100M-row table
ties with three filters on a 100-row table. Callers: `inference.rs:1041`
(main Projection path, with `plan_ctx`), `inference.rs:951` (UNION branches,
deliberately without `plan_ctx` — anchor must match branch direction),
`inference.rs:1846` (nested subplans, without `plan_ctx`).

**S1 changes exactly this function** (§3.3). The other heuristics below are
inventory for S2/S3 and are untouched.

### 1.2 `topo_sort_joins()` — join order

Same file, after `select_anchor`. Greedy topological sort over join
dependencies: FROM markers first (sorted by alias), then per round every join
whose dependencies are satisfied, tie-broken by `table_alias` string order.
Any permutation of the same-round-ready joins is semantically equal — a
future stats consumer could order same-round joins by ascending table size so
smaller intermediate results build first. Deliberately untouched in S1: the
render phase's own `sort_joins_by_dependency` (emitter) re-sorts with a
natural-alias-order tie-break (#626), and a planner-side reorder here would
need both layers audited together.

### 1.3 VLP recursion direction

Variable-length paths compile to a recursive CTE whose **base case always
seeds from the pattern's start side**: `render_plan/cte_extraction.rs`
(the `GraphRel` VLP arm, ~:2639 onward) maps `graph_rel.left_connection` →
`start_node` / `right_connection` → `end_node` (aliases fixed at
`"start_node"`/`"end_node"`, see `variable_length_cte.rs`
`start_node_alias`/`end_node_alias` and `generate_base_case`), and the
recursion extends along `relationship_from_column` → `relationship_to_column`
per hop. `left_connection`/`right_connection` are already direction-swapped by
the traversal planner for `<-` patterns — i.e. **the writing direction of the
Cypher pattern decides which endpoint set the BFS grows from**. For
`(a:User {country:'IS'})-[:FOLLOWS*1..3]->(b)` vs the reversed-written
equivalent, one direction may seed from a tiny filtered set and the other
from the whole table; today nothing compares them. (Anchor-gate folding for
OPTIONAL VLP — #621/#645 — keys off this same parsed direction, so any future
direction flip must revisit those gates. That is S2 scope.)

### 1.4 Other lexical/positional tie-breaks (inventory only)

- `helpers::deduplicate_joins` keeps the first-seen join per alias (plan
  order).
- `from_builder.rs` anchor fallbacks (anchor `None` after WITH barriers):
  "latest CTE" / "first join" positional picks.
- The emitter's `sort_joins_by_dependency` natural-order tie-break (#626).
- BidirectionalUnion arm order (forward arm first) — order inside `UNION ALL`
  is semantically free.

None of these consume stats in S1.

## 2. Staging

- **S1 (this slice)**: table row-count cache → `select_anchor` ranking.
- **S2 (sketch, §6)**: column-level selectivity (NDV/min-max) → rank filtered
  anchors properly, pick VLP recursion direction.
- **S3 (sketch, §7)**: feedback loop from existing per-query metrics before
  building more machinery.

## 3. S1 design — table row-count cache feeding anchor selection

### 3.1 Store (`src/graph_catalog/table_stats.rs`)

- `TableStatsSnapshot` — immutable `full "db.table" name → u64` map + fetch
  timestamp. Constructible from raw counts (`from_counts`) so tests inject
  fixtures with no live backend. Lookup normalizes backticks. Unknown table →
  `None`, **never 0** (0 would rank an unknown as "smallest", the exactly
  wrong direction).
- `TableStatsSource` (trait, async) — pluggable backend. Implemented:
  `ClickHouseTableStatsSource`, one query:
  `SELECT database, name, total_rows FROM system.tables WHERE database IN (…)`
  (precedent: `schema_discovery.rs` / `engine_detection.rs` system-table
  introspection; database identifiers are charset-validated before
  interpolation, same discipline as `validate_sql_identifier`).
  `total_rows` is `Nullable(UInt64)` — NULL (non-MergeTree engines, views) is
  dropped as *unknown*, not coerced.
- `TableStatsCache` — TTL-refreshed store (`CLICKGRAPH_STATS_TTL_SECS`,
  default 300). Refresh is **lazy on access**: `snapshot(databases)` returns
  the cached `Arc<TableStatsSnapshot>` when fresh and covering; refetches
  (union of covered + requested databases, for multi-schema servers) when the
  TTL elapsed or a new database appears. Fetch failure keeps serving the
  stale snapshot (stats are a hint, not a correctness dependency) and is not
  retried until the TTL elapses again, so a down backend isn't hammered
  per-query. `schema_databases(&GraphSchema)` derives the database list from
  the schema catalog (nodes + relationships), per the axis rule.

### 3.2 Wiring (the plumbing answer — see also §4)

Server mode (remote ClickHouse) only, in this slice:

1. Startup (`server/mod.rs::run_with_config`, remote branch): when
   `config.stats_enabled` (env `CLICKGRAPH_STATS_ENABLED`, default false) and
   a ClickHouse client exists, install `GLOBAL_TABLE_STATS: OnceCell<Arc<TableStatsCache>>`.
   Never installed in embedded/Databricks/sql-only modes or when the flag is
   off — the flag-off path has **no stats code reachable at all**.
2. Request entry (HTTP `handlers.rs::query_handler_inner` and Bolt
   `bolt_protocol/handler.rs`, right after `set_current_schema`):
   `attach_current_table_stats(&schema).await` — an async call at the
   request boundary that snapshots the cache (possibly refreshing, TTL-gated)
   into the task-local `QueryContext.table_stats`. No-op when the cache was
   never installed or no fetch has succeeded yet.
3. Consumption (`select_anchor`): synchronous read of the task-local snapshot
   via `query_context::get_current_table_stats()` — mirrors how the analyzer
   already reads the schema (`get_current_schema()`) and how render-phase
   channels like `is_adjacent_exact_vlp_reroute` work. No blocking call, no
   `block_on`, no async leakage into the analyzer.

Embedded/remote **library** modes (`clickgraph-embedded`) are not wired in
this slice: `Connection::query` wraps everything in `with_query_context`
already, so wiring is mechanical (construct a cache next to the executor,
attach at query entry), but it expands the test surface (FFI/Go/Py) and is
deferred to a follow-up commit. Because attachment is the only trigger, those
modes are byte-identical to today by construction.

### 3.3 `select_anchor` ranking

Within each existing tier (filtered → unfiltered → optional), candidates are
sorted by `(row_count ascending, alias ascending)` where unknown counts rank
as `u64::MAX` — i.e. known-small first, unknowns last **alphabetically among
themselves**. Consequences:

- No snapshot (flag off / sql_only / tests / embedded) → pure alphabetical →
  **byte-identical to the pre-S1 engine** (proven by the untouched golden +
  corpus suites and an explicit degradation test).
- Snapshot present but all candidates unknown → same.
- Tier order is preserved: stats never promote an unfiltered table over a
  filtered one (a deliberate S1 conservatism; ranking *across* tiers needs
  real selectivity — S2).
- Only which semantically-equal FROM marker anchors the query changes.
  `topo_sort_joins` and everything downstream are untouched.

Table names reach the ranking via `Join.table_name` (already fully qualified
`db.table` by the analyzer from `NodeSchema::full_table_name()` — the join
metadata, not string-parsing of emitted SQL). CTE-typed "tables" simply miss
the map and rank as unknown.

### 3.4 Axis-dispatch compliance (CLAUDE.md §7)

No branching on `is_denormalized`/`is_fk_edge`/table-name comparisons: the
store keys on catalog-provided full table names; `schema_databases` walks
the catalog's node/relationship schemas; `select_anchor` consumes
`Join.table_name` produced by the existing schema-driven join generation.
Whatever schema pattern produced the FROM markers, ranking is uniform. No
dialect-specific SQL is emitted by the planner change; the ClickHouse fetch
query lives behind the `TableStatsSource` trait (the dialect axis for the
*fetch* is the trait, mirroring `schema_discovery` vs `databricks_probe`).
Ratchet test: clean (no new raw-axis tokens).

## 4. The plumbing question, answered honestly

`select_anchor` runs deep in the synchronous analyzer; the row-count fetch is
async/remote. Options considered:

- **(rejected) blocking fetch in the analyzer** — a `block_on` inside the
  planner deadlocks on a current-thread runtime and adds a remote round-trip
  to every plan; also unreachable-by-construction would be lost.
- **(rejected) stats on `GraphSchema`** — schemas are long-lived, shared, and
  content-hashed (#463); mutating them per-TTL would invalidate caching
  assumptions and leak stats into every mode including sql_only.
- **(chosen) task-local snapshot attached at request entry** — the request
  boundary is already async (both HTTP and Bolt), already sets the schema
  into the task-local `QueryContext`, and the analyzer already reads
  task-local state. The TTL refresh cost lands on at most one request per TTL
  window; every other request clones an `Arc`. The default path (no cache
  installed) costs one `OnceCell::get` returning `None` per query.

The one caveat: `select_anchor` reading task-local state means its output
depends on ambient context, not only its arguments. That is already true of
this codebase's architecture (schema access, denormalized alias registry,
VLP reroute channels are all task-local), and the unit tests exercise both
ambient states explicitly.

## 5. Testing strategy

- **Goldens stay stats-less**: nothing in `sql_golden_tests.rs`,
  `corpus_sweep.rs`, or any pre-existing test attaches a snapshot, so those
  suites lock the flag-off behavior byte-exactly. Verified green, unchanged,
  on this branch.
- **New with-stats golden set**
  (`tests/rust/integration/stats_anchor_golden_tests.rs`, goldens under
  `golden/sql_ir/stats_standard/`): renders the production path with a fixed
  programmatic fixture (`posts=100 ≪ users=1M`), locking the flipped anchor.
  Regenerate with `UPDATE_GOLDEN=1 cargo test --test integration stats_anchor`.
- **Flip + degradation test** (`stats_flip_anchor_and_off_is_byte_identical`):
  same query rendered stats-less, with the fixture, and with an *empty*
  snapshot — asserts flag-off determinism, the anchor flip, and that
  empty-stats degrades to byte-identical stats-less SQL.
- **Unit tests**: store (TTL expiry, fresh-hit Arc reuse, new-database
  refresh, failure/no-hammer, NULL-as-unknown semantics, identifier
  validation) and `select_anchor` (flip, unknown-fallback, tier preservation,
  ranking among multiple filtered candidates).
- LDBC-scale benchmarking is explicitly a later, separate task (PRIORITIES.md
  P-5 "LDBC-benchmarked" applies to the staged program, not this slice).

## 6. S2 sketch — column selectivity (future)

Row counts can't rank two filtered candidates on the same table size class;
`has_selective_filters()` stays boolean. S2 adds column-level stats:

- **Sources**: ClickHouse experimental column statistics
  (`ALTER TABLE … ADD STATISTICS`) where declared; otherwise
  `system.parts_columns` aggregates (`uniq_*` when materialized, min/max,
  compressed sizes) — same `TableStatsSource`-style trait, new
  `ColumnStatsSource`, cached alongside row counts.
- **Consumers**:
  - `select_anchor` tier 1: score filtered candidates by estimated post-filter
    cardinality (`rows / NDV` for equality on a column, min/max clamp for
    ranges) instead of boolean-then-size.
  - **VLP direction**: when the recursion could seed from either endpoint
    (undirected, or directed with an inverse walk provably equivalent), start
    BFS from the smaller *estimated* endpoint set (§1.3 — today the writing
    direction decides). This is the first place stats touch traversal
    direction, and the #621/#645 anchor-gate family must be re-audited when it
    lands.
- Same guardrails: estimates pick among equivalent orderings only; flag-gated;
  fixture-locked goldens.

## 7. S3 sketch — feedback loop (future)

Before building more estimation machinery, measure which heuristics actually
cost: `src/server/metrics.rs` already captures per-query `read_rows` /
`read_bytes` / server-side elapsed (opt-in via `CLICKGRAPH_METRICS_CH_SUMMARY`
reading `X-ClickHouse-Summary`) plus the slow-query ring
(`/stats/queries`). S3 correlates plan shape (anchor choice, join count, VLP
presence — recordable as a compact plan fingerprint at render time) with
those observed costs to find where S1/S2 decisions were wrong, and only then
invests further. Explicitly deferred: per-query `EXPLAIN ESTIMATE`
round-trips — not until S3 data says which query classes would pay for the
extra latency.

## 8. Dialect story

The **fetch** is the only dialect-specific piece, isolated behind
`TableStatsSource`:

- ClickHouse (implemented): `system.tables.total_rows`.
- Databricks (future): `DESCRIBE TABLE EXTENDED` / `numRows` from table
  detail — a `DatabricksTableStatsSource` living next to
  `graph_catalog/databricks_probe.rs`, installed by the `--databricks` server
  branch. The snapshot/cache/planner layers are backend-agnostic already
  (keys are catalog `db.table` strings).

The planner-side consumption is dialect-free by construction; nothing routes
through `Dialect`/`FunctionMapper` because no SQL is generated from stats.

## 9. S1 checklist

- [x] `docs/design/STATS_PLANNING.md` (this doc)
- [x] `table_stats.rs` store: snapshot + source trait + CH source + TTL cache
- [x] `CLICKGRAPH_STATS_ENABLED` / `CLICKGRAPH_STATS_TTL_SECS` config (default
      off / 300)
- [x] Server wiring: startup cache install (remote mode), HTTP + Bolt
      request-entry snapshot attach
- [x] `select_anchor` within-tier ascending-row-count ranking, alphabetical
      fallback preserved
- [x] Unit tests (store + anchor), with-stats golden set, flip/degradation
      test
- [x] Flag-off byte-identity: full goldens + corpus sweep + ratchet green,
      untouched
- [ ] Embedded/remote library-mode wiring (follow-up commit)
- [ ] Live end-to-end demo at LDBC scale (separate task per PRIORITIES.md)
