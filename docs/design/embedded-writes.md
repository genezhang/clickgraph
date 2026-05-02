# Cypher Write Operations — Embedded Mode

**Date**: 2026-05-02 (Phase 0 decisions locked 2026-05-02)
**Status**: Phase 0 locked — ready to start Phase 1
**Scope**: CREATE, SET, DELETE, REMOVE in MVP; MERGE in Phase 5
**Non-Goals**: Server mode writes (stays read-only), source-backed-table writes, transactions, Bolt protocol writes

---

## Executive Summary

Add Cypher write support to ClickGraph's embedded (chdb-backed) execution mode. Server mode and source-backed table sources (Parquet/S3/Iceberg/Delta) remain read-only by design. The Cypher parser already produces write-clause AST nodes, and the embedded crate already supports writes via a direct Rust API (`Connection::create_node`/`create_edge`/`delete_nodes`/...). The gap is the **planner → render → executor bridge for Cypher write clauses** — i.e., letting `Connection::query("CREATE (n:Person {name:'x'})")` actually do something. The Cypher path will share the *patterns and validation rules* used by the direct-API SQL builders in `clickgraph-embedded/src/write_helpers.rs`, but will generate SQL through the renderer rather than reusing those builders directly (see Phase 2 rationale).

- **Estimated effort**: 5.5 weeks for MVP (CREATE/SET/DELETE/REMOVE), 6.5 weeks with MERGE
- **Risk profile**: Medium. Highest-risk phase is the planner work (Phase 1). SQL generation reuses proven infrastructure.
- **TCK impact**: Unblocks ~16 of 19 currently-skipped write scenarios (MERGE scenarios remain until Phase 5)

---

## Current State Snapshot

| Layer | State | Evidence |
|---|---|---|
| Parser | **Complete** | `src/open_cypher_parser/{create,set,delete,remove}_clause.rs` — AST variants exist |
| Query type detection | **Complete** | `src/query_planner/mod.rs:42-49` classifies `Delete`/`Update` |
| Planner LogicalPlan | **Missing write variants** | `src/query_planner/logical_plan/mod.rs` — all variants today (`ViewScan`, `GraphNode`, `GraphRel`, `Filter`, `Projection`, `GroupBy`, `OrderBy`, `Skip`, `Limit`, `Cte`, `GraphJoins`, `Union`, `PageRank`, `Unwind`, `CartesianProduct`, `WithClause`, `Empty`) are read-side only |
| Render plan | **Missing write variants** | `src/render_plan/` — no terminal write nodes |
| SQL generation (direct API) | **Exists** | `clickgraph-embedded/src/write_helpers.rs` builds lightweight `INSERT`/`DELETE FROM` for the direct Rust API |
| SQL generation (Cypher path) | **Missing** | No renderer support for write `LogicalPlan` variants |
| Executor wiring (Cypher path) | **Read-only** | `clickgraph-embedded/src/connection.rs::query()` routes Cypher to `evaluate_read_statement`. Direct-API methods (`create_node`/`delete_nodes`/...) bypass this and already write. |
| Hard-stop today | Server rejects Cypher writes | `src/server/handlers.rs:1356` |

---

## Phase 0 — Decisions (must lock before Phase 1)

These shape schema fields, error surfaces, and API contracts. Each row is **open** until accepted; flip to **locked** when agreed.

| # | Decision | Resolution | Status |
|---|---|---|---|
| 0.1 | Where writes are gated | Single planner-level guard rejecting write `LogicalPlan` variants unless executor is `Embedded(chdb)`. Server keeps existing reject at `handlers.rs:1356` as defence-in-depth. | **locked** |
| 0.2 | ID generation strategy | Embedded writable tables already get `String DEFAULT generateUUIDv4()` on ID columns at DDL time (`src/executor/data_loader.rs:198-208`); `Connection::create_node()` auto-generates a UUID when the ID property is absent. Cypher CREATE follows the same model: add `id_generation: Option<IdStrategy>` to `NodeSchema` — values `Uuid` (default, matches existing DDL), `Snowflake`, `Provided` (error if absent). When the column has a `DEFAULT` clause and Cypher omits the property, omit the column from the INSERT and let chdb fill it. | **locked** |
| 0.3 | Source-backed targets | Hard error at plan time if any target node/edge label resolves to a schema with `source:` set. | **locked** |
| 0.4 | Atomicity | Best-effort, statement-scoped. No transactions; partial failures possible under chdb. Documented in user-facing notes. | **locked** |
| 0.5 | Property type coercion | Strict: error if Cypher literal type doesn't match column type. Same discipline as the read path. Permissive coercion can be relaxed later if it bites — flipping the other direction would be a breaking change. | **locked** |
| 0.6 | FK-edge writes | Out of scope for v1. Reject at plan time with actionable error suggesting standard edge-table schema. | **locked** |
| 0.7 | Read-after-write consistency | **Use lightweight UPDATE.** Probe of chdb 26.1.2.1 (`SELECT version()` on `chdb-rust 1.3.1`'s bundled libchdb) confirmed: `UPDATE table SET col = expr WHERE pred` works synchronously, no flag at query time, immediately visible via `FINAL` — **provided the table was created with `SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1`** (without these settings, `Code 48 NOT_IMPLEMENTED`). This makes SET symmetric with lightweight DELETE: both fast, both synchronous, no `mutations_sync` needed. The two block-tracking columns add small per-table overhead but no Cypher/schema surface impact. Phase 3 must extend `data_loader.rs:198-208` to emit the two settings on every writable table DDL. The async `ALTER TABLE … UPDATE` mutation path is **not** the recommended path; documented as a fallback if lightweight UPDATE bites in production. | **locked** |
| 0.8 | Return shape | Neo4j-compatible counters (`nodesCreated`, `propertiesSet`, `nodesDeleted`, …) as a single-row `QueryResult`. Bindings already pass `QueryResult` through; no FFI changes needed. | **locked** |
| 0.9 | `EXPLAIN` for write queries | **Embedded API only.** Cypher `EXPLAIN` is not a parser clause today, and the Bolt server special-cases `EXPLAIN …` as an autocomplete probe (returns empty SUCCESS). This decision adds an embedded-API call (e.g., `Connection::explain(cypher)`) that returns the generated SQL string without executing. Bolt's existing no-op behaviour is preserved unchanged. Add a test asserting `explain()` never reaches the executor. | **locked** |

**Phase 0 status**: complete (all 9 decisions locked 2026-05-02). 0.7 was refined from the original mutations-based recommendation after a chdb-rust 1.3.1 probe confirmed lightweight UPDATE viability with the two block-tracking settings.

**Decisions explicitly considered and rejected:**
- **`read_only` flag on `Database::new(...)`** — rejected. The flag would only refuse queries, never produce different output. Callers who need read-only enforcement can check `query_type()` in three lines themselves; trust-boundary policy belongs at the call site, not as an engine config knob. Every flag is a future maintenance liability and test-matrix multiplier.
- **Mutation-based SET (`ALTER TABLE … UPDATE … SETTINGS mutations_sync = 2`)** — rejected for v1 in favour of lightweight UPDATE (Decision 0.7). Kept as documented fallback if lightweight UPDATE proves problematic in production.

---

## Phase 1 — Planner: Write LogicalPlan Variants

**Estimated**: 1.5 weeks. **Highest-risk phase.**

### Files to add/modify

- **`src/query_planner/logical_plan/mod.rs`** — extend the `LogicalPlan` enum:
  ```rust
  Create { patterns: Vec<CreatePattern>, input: Option<Box<LogicalPlan>> },
  SetProperties { items: Vec<SetItem>, input: Box<LogicalPlan> },
  Delete { targets: Vec<Variable>, detach: bool, input: Box<LogicalPlan> },
  Remove { items: Vec<RemoveItem>, input: Box<LogicalPlan> },
  ```
  `input` carries the optional preceding MATCH (e.g., `MATCH (a) DELETE a`). For CREATE without a preceding MATCH, `input` is `None`.
- **New: `src/query_planner/plan_builder/write_clause_builder.rs`** — converts parsed write AST nodes into the variants above. Resolves labels via `graph_catalog`, validates property names/types against `NodeSchema`/`RelationshipSchema`, rejects FK-edge and source-backed targets.
- **New: `src/query_planner/write_guard.rs`** — single `ensure_write_target_writable(plan, schema, executor)` function called from the planner entry point. Centralises Decisions 0.1, 0.3, 0.6.
- **`src/query_planner/mod.rs:42-49`** — `get_query_type()` already handles this branch; verify no change needed.

### Deliverables
- Unit tests in `write_clause_builder.rs`: parse → expected `LogicalPlan` per variant
- Negative tests: source-backed target, FK-edge target, missing required property, type mismatch
- A short note in `src/query_planner/AGENTS.md` describing the new variants

### Exit criteria
- All four write variants planned end-to-end from Cypher text
- All negative cases produce actionable errors before reaching render
- `cargo test -p clickgraph query_planner::` green

---

## Phase 2 — Render Plan + SQL Generation

**Estimated**: 1.5 weeks. **Reuses existing infrastructure.**

### Files to add/modify

- **`src/render_plan/`** — add terminal variants `RenderInsert`, `RenderDelete`, `RenderUpdate`. Terminal because writes don't compose into SELECTs.
- **New: `src/clickhouse_query_generator/write_to_sql.rs`** — emits chdb-compatible SQL, aligned with the patterns already used by `clickgraph-embedded/src/write_helpers.rs`. All three write paths are lightweight and synchronous:
  - **CREATE** → `INSERT INTO {table} ({cols}) VALUES (...)`. When a property is absent and the column has a `DEFAULT` (e.g., `generateUUIDv4()`), omit the column from the INSERT and let chdb fill it (per Decision 0.2).
  - **SET** → `UPDATE {table} SET {col} = {expr} WHERE {pk} = {id}` (lightweight UPDATE per Decision 0.7 — synchronous, no flag, no `SETTINGS` clause needed at query time; relies on the table being created with the two block-tracking settings — see Phase 3).
  - **DELETE** → `DELETE FROM {table} WHERE {pk} IN ({ids})` (lightweight, synchronous — same path as `write_helpers::build_delete_sql`).
  - **DETACH DELETE** → one lightweight `DELETE FROM` per relationship table referencing the node label, then the node DELETE. Order matters; document.
- **New: `src/clickhouse_query_generator/id_gen.rs`** — emits the right ID expression in INSERT column lists when the schema's `id_generation` is `Snowflake` and there's no DDL default to lean on.
- **Reuse rationale**: the direct-API builders in `write_helpers.rs` operate on `Property` maps; the Cypher path operates on `LogicalPlan` nodes resolved by the planner. We keep two callers in v1 because the input shapes differ enough that a forced merge would slow down the planner work for marginal benefit. Tracked as a follow-up to unify under one builder once both paths stabilise.

### Deliverables
- Snapshot tests under `tests/sql_snapshots/writes/`: Cypher in, deterministic SQL out
- Coverage: each write type × (single node, multiple nodes, with WHERE, with parameters, DETACH DELETE chain)

### Exit criteria
- Snapshot suite green and reviewed for SQL hygiene (no injection vectors, parameters bound correctly)

---

## Phase 3 — Embedded Executor Wiring + ID Generation

**Estimated**: 1 week.

### Files to modify

- **`clickgraph-embedded/src/connection.rs::query()`** — branch on `LogicalPlan` kind:
  - Read variants → existing path
  - Write variants → new `execute_write()` which:
    1. Asserts `Database` was created via `Database::new(...)` (chdb), not `sql_only` or `new_remote`. Return clear error otherwise.
    2. Executes generated SQL via the chdb connection. INSERT, lightweight UPDATE (SET), and lightweight DELETE all return synchronously per Decision 0.7 — no mutation-wait barrier needed.
    3. Returns affected-row counters as a single-row `QueryResult` per Decision 0.8.
- **`src/executor/data_loader.rs:198-208`** — extend writable-table DDL to append `SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1`. This is the prerequisite for lightweight UPDATE per Decision 0.7. Apply to every node and edge table created in embedded mode (i.e., schemas without a `source:` field). Add a unit test asserting the settings appear in the generated DDL.
- **`src/graph_catalog/graph_schema.rs`** — extend `NodeSchema` with optional `id_generation` field. Default `None` → behaves as `Uuid` (existing behaviour). Existing schemas unaffected.
- **`clickgraph-ffi/src/lib.rs`** — verify (don't expect changes); `Connection::query` already returns `QueryResult`. Confirm Python and Go bindings surface the new counter fields.

### Deliverables
- `clickgraph-embedded/tests/writes.rs` — end-to-end Cypher write → chdb → read-back
- Smoke tests in `clickgraph-py/tests/` and `clickgraph-go` confirming counters round-trip through FFI
- Schema doc update in `docs/schema-reference.md` documenting `id_generation`

### Exit criteria
- `cargo test -p clickgraph-embedded` green
- Python and Go binding tests green
- Manual e2e: `cg` CLI executes a Cypher write against a chdb-backed schema and reads the result back

---

## Phase 4 — TCK Unblock + Hardening

**Estimated**: 0.5 week.

### Files to modify

- **`clickgraph-tck/tests/tck.rs:564-572`** — current filter only excludes scenarios tagged `skip|fails|NegativeTests|crash|wip`. Write scenarios aren't excluded by a `@write` tag — they're tagged individually as `@skip`/`@fails`/`@wip` (or fail today because the planner rejects writes). Phase 4 work: identify the ~19 write scenarios currently masked this way, drop their per-scenario skip tags as each becomes implementable, and triage residual failures.
- Aim for ~16 of 19 currently-skipped scenarios passing
- **`STATUS.md`**, **`KNOWN_ISSUES.md`**, **`README.md`** — writes supported in embedded mode only, with caveats from Decisions 0.4 / 0.7 listed
- **`docs/wiki/cypher-language-reference.md`** — write-clause sections (this is the primary feature doc per CLAUDE.md)

### Exit criteria
- TCK count moves from 402/402 (with writes filtered) to ~418/421 with writes included; remaining failures are MERGE scenarios documented as known-pending

---

## Phase 5 — MERGE (optional, +1 week)

MERGE is match-or-create with `ON CREATE SET` / `ON MATCH SET` sub-clauses. Genuinely tricky semantics.

- **New: `src/open_cypher_parser/merge_clause.rs`**
- **`LogicalPlan::Merge { pattern, on_create, on_match, input }`**
- SQL generation: select-then-conditional-insert. chdb has no `INSERT … ON CONFLICT`; emit a guarded two-statement sequence. Race conditions exist but chdb has no concurrency anyway — document.

Defer if Phase 1–4 burns more than estimated. CREATE/SET/DELETE covers the common write workload.

---

## Risks & Mitigations

| Risk | Mitigation |
|---|---|
| Lightweight UPDATE labelled experimental in newer ClickHouse releases | Decision 0.7 confirmed working on chdb 26.1.2.1 without an experimental flag at query time. Mutation-based SET retained as documented fallback if the engine path regresses. Pin chdb-rust version intentionally to limit surprise. |
| Existing chdb-backed tables created before Phase 3 lack `enable_block_number_column`/`enable_block_offset_column` settings | Embedded chdb sessions are single-process and ephemeral, so there's no real "fleet of legacy tables" to migrate. New writable tables get the settings from day one via `data_loader.rs` change. If a long-lived persisted table exists, document the `ALTER TABLE … MODIFY SETTING` migration. |
| Server mode accidentally exposes writes via shared planner | Phase 1 `write_guard` runs before render; server `handlers.rs` keeps existing reject as defence-in-depth |
| FFI binding regressions on counter types | Run full `clickgraph-py` and `clickgraph-go` test suites in Phase 3 |
| Schema migrations break existing read users | `id_generation` is optional with `None` default; existing schemas keep working unchanged |
| chdb SIGABRT-on-Drop interacting with mid-write state | Document; recommend explicit `Connection::close()` in user code; long-term fix is upstream |
| Direct-API and Cypher-API drift in INSERT semantics | Acceptable for v1; track as follow-up to unify under one SQL builder |

---

## Appendix: Phase Breakdown

| Phase | Focus | Effort | Cumulative |
|---|---|---|---|
| 0 | Decisions | 2 days | 0.4w |
| 1 | Planner | 1.5w | 1.9w |
| 2 | Render + SQL | 1.5w | 3.4w |
| 3 | Executor + ID gen | 1w | 4.4w |
| 4 | TCK + hardening | 0.5w | 4.9w |
| Buffer | unknowns, polish | 0.5w | **5.4w MVP** |
| 5 | MERGE | 1w | **6.4w with MERGE** |
