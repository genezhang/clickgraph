# Cypher Write Operations — Embedded Mode

**Date**: 2026-05-02
**Status**: Design Proposal — Phase 0 decisions open
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

| # | Decision | Recommendation | Status |
|---|---|---|---|
| 0.1 | Where writes are gated | Single planner-level guard rejecting write `LogicalPlan` variants unless executor is `Embedded(chdb)`. Server keeps existing reject at `handlers.rs:1356` as defence-in-depth. | open |
| 0.2 | ID generation strategy | Embedded writable tables today already get `String DEFAULT generateUUIDv4()` on ID columns at DDL time (`src/executor/data_loader.rs:198-208`), and `Connection::create_node()` auto-generates a UUID when the ID property is absent. Recommendation: **align Cypher CREATE with this**. Add `id_generation: Option<IdStrategy>` to `NodeSchema` — values `Uuid` (default, matches existing DDL), `Snowflake`, `Provided` (error if absent). When the column has a `DEFAULT` clause and Cypher omits the property, simply omit the column from the INSERT and let chdb fill it. | open |
| 0.3 | Source-backed targets | Hard error at plan time if any target node/edge label resolves to a schema with `source:` set. Honest failure beats partial-write or silent translation. | open |
| 0.4 | Atomicity | Best-effort, statement-scoped. Document explicitly: no transactions, partial failures possible. chdb has no MVCC. | open |
| 0.5 | Property type coercion | Strict: error if Cypher literal type doesn't match column type. Same discipline as the read path. | open |
| 0.6 | FK-edge writes | Out of scope for v1. Reject at plan time with actionable error suggesting standard edge-table schema. | open |
| 0.7 | Read-after-write consistency | Writable tables are non-replicated `ReplacingMergeTree` (`src/graph_catalog/engine_detection.rs`), so `SYSTEM SYNC REPLICA` does not apply. INSERT and lightweight `DELETE FROM` are synchronous. Only `ALTER TABLE … UPDATE` (used for SET) is async. Recommendation: SET issues mutations with `SETTINGS mutations_sync = 2` (or `SYSTEM WAIT MUTATION` after) so the call returns after the mutation lands; INSERT/DELETE keep the lightweight path with no barrier needed. Document the SET-specific cost. | open |
| 0.8 | Return shape | Return Neo4j-compatible counters (`nodesCreated`, `propertiesSet`, `nodesDeleted`, …) as a single-row `QueryResult`. Bindings already pass `QueryResult` through; no FFI changes needed. | open |
| 0.9 | `EXPLAIN` for write queries | **Embedded API only.** Cypher `EXPLAIN` is not a parser clause today, and the Bolt server special-cases `EXPLAIN …` as an autocomplete probe (returns empty SUCCESS). This decision adds an embedded-API call (e.g., `Connection::explain(cypher)`) that returns the generated SQL string without executing. Cheap (pipeline already produces SQL); essential for debugging write failures. Bolt's existing no-op behaviour is preserved unchanged. Add a test asserting `explain()` never reaches the executor. | open |

**Deliverable for Phase 0**: this section, all rows flipped to **locked**, plus any inline notes for choices that diverged from the recommendation.

**Decisions explicitly considered and rejected:**
- **`read_only` flag on `Database::new(...)`** — rejected. The flag would only refuse queries, never produce different output. Callers who need read-only enforcement can check `query_type()` in three lines themselves; trust-boundary policy belongs at the call site, not as an engine config knob. Every flag is a future maintenance liability and test-matrix multiplier.

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
- **New: `src/clickhouse_query_generator/write_to_sql.rs`** — emits chdb-compatible SQL, aligned with the patterns already used by `clickgraph-embedded/src/write_helpers.rs`:
  - **CREATE** → `INSERT INTO {table} ({cols}) VALUES (...)`. When a property is absent and the column has a `DEFAULT` (e.g., `generateUUIDv4()`), omit the column from the INSERT and let chdb fill it (per Decision 0.2).
  - **SET** → `ALTER TABLE {table} UPDATE {col}={expr} WHERE {pk}={id} SETTINGS mutations_sync = 2` (per Decision 0.7).
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
    2. Executes generated SQL via the chdb connection. INSERT and lightweight DELETE return synchronously; SET (`ALTER TABLE … UPDATE`) carries `SETTINGS mutations_sync = 2` per Decision 0.7 so the call returns after the mutation completes.
    3. Returns affected-row counters as a single-row `QueryResult` per Decision 0.8
- **`src/graph_catalog/graph_schema.rs`** — extend `NodeSchema` with optional `id_generation` field. Default `None` → behaves as `Provided`. Existing schemas unaffected.
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
| chdb mutation latency for SET (`ALTER TABLE … UPDATE` is async) | Decision 0.7 — `mutations_sync = 2` on SET. INSERT and lightweight `DELETE FROM` are synchronous and need no barrier. |
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
