# Cypher Write Operations — Embedded Mode

**Date**: 2026-05-02
**Status**: Design Proposal — Phase 0 decisions open
**Scope**: CREATE, SET, DELETE, REMOVE in MVP; MERGE in Phase 5
**Non-Goals**: Server mode writes (stays read-only), source-backed-table writes, transactions, Bolt protocol writes

---

## Executive Summary

Add Cypher write support to ClickGraph's embedded (chdb-backed) execution mode. Server mode and source-backed table sources (Parquet/S3/Iceberg/Delta) remain read-only by design. The work reuses existing parser AST and the `clickgraph-embedded/src/write_helpers.rs` SQL builders; the gap is the planner→render→executor bridge for Cypher write clauses.

- **Estimated effort**: 5.5 weeks for MVP (CREATE/SET/DELETE/REMOVE), 6.5 weeks with MERGE
- **Risk profile**: Medium. Highest-risk phase is the planner work (Phase 1). SQL generation reuses proven infrastructure.
- **TCK impact**: Unblocks ~16 of 19 currently-skipped write scenarios (MERGE scenarios remain until Phase 5)

---

## Current State Snapshot

| Layer | State | Evidence |
|---|---|---|
| Parser | **Complete** | `src/open_cypher_parser/{create,set,delete,remove}_clause.rs` — AST variants exist |
| Query type detection | **Complete** | `src/query_planner/mod.rs:42-49` classifies `Delete`/`Update` |
| Planner LogicalPlan | **Missing write variants** | `src/query_planner/logical_plan/mod.rs:466-508` — 13 read variants, no write |
| Render plan | **Missing write variants** | `src/render_plan/` — no terminal write nodes |
| SQL generation | **Direct-API exists; Cypher-path missing** | `clickgraph-embedded/src/write_helpers.rs` (684 lines) builds INSERT/DELETE for the direct Rust API |
| Executor wiring | **Embedded read-only today** | `clickgraph-embedded/src/connection.rs::query()` routes to read execution only |
| Hard-stop today | Server rejects writes | `src/server/handlers.rs:1356` |

---

## Phase 0 — Decisions (must lock before Phase 1)

These shape schema fields, error surfaces, and API contracts. Each row is **open** until accepted; flip to **locked** when agreed.

| # | Decision | Recommendation | Status |
|---|---|---|---|
| 0.1 | Where writes are gated | Single planner-level guard rejecting write `LogicalPlan` variants unless executor is `Embedded(chdb)`. Server keeps existing reject at `handlers.rs:1356` as defence-in-depth. | open |
| 0.2 | ID generation strategy | Add `id_generation: Option<IdStrategy>` to `NodeSchema` — values: `Snowflake` (chdb `generateSnowflakeID()`), `Uuid` (`generateUUIDv4()`), `Provided` (default; error if Cypher omits the ID property). | open |
| 0.3 | Source-backed targets | Hard error at plan time if any target node/edge label resolves to a schema with `source:` set. Honest failure beats partial-write or silent translation. | open |
| 0.4 | Atomicity | Best-effort, statement-scoped. Document explicitly: no transactions, partial failures possible. chdb has no MVCC. | open |
| 0.5 | Property type coercion | Strict: error if Cypher literal type doesn't match column type. Same discipline as the read path. | open |
| 0.6 | FK-edge writes | Out of scope for v1. Reject at plan time with actionable error suggesting standard edge-table schema. | open |
| 0.7 | Read-after-write consistency | chdb mutations (`ALTER TABLE … UPDATE/DELETE`) are async by default. Either issue a `SYSTEM SYNC` barrier post-write, or document the eventual-consistency window. Recommendation: barrier by default, opt-out env var for batch workloads. | open |
| 0.8 | Return shape | Return Neo4j-compatible counters (`nodesCreated`, `propertiesSet`, `nodesDeleted`, …) as a single-row `QueryResult`. Bindings already pass `QueryResult` through; no FFI changes needed. | open |
| 0.9 | `EXPLAIN` for write queries | Supported in embedded mode — surfaces generated SQL without executing. Cheap (pipeline already produces SQL) and essential for debugging write failures. Add a test asserting `EXPLAIN` never reaches the executor. | open |

**Deliverable for Phase 0**: this section, all rows flipped to **locked**, plus any inline notes for choices that diverged from the recommendation.

**Decisions explicitly considered and rejected:**
- **`read_only` flag on `Database::new(...)`** — rejected. The flag would only refuse queries, never produce different output. Callers who need read-only enforcement can check `query_type()` in three lines themselves; trust-boundary policy belongs at the call site, not as an engine config knob. Every flag is a future maintenance liability and test-matrix multiplier.

---

## Phase 1 — Planner: Write LogicalPlan Variants

**Estimated**: 1.5 weeks. **Highest-risk phase.**

### Files to add/modify

- **`src/query_planner/logical_plan/mod.rs`** — extend the enum (currently lines 466–508):
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
- **New: `src/clickhouse_query_generator/write_to_sql.rs`** — emits chdb-compatible SQL:
  - **CREATE** → `INSERT INTO {table} ({cols}) VALUES (...)`. ID column populated via `generateSnowflakeID()` / `generateUUIDv4()` / provided literal per Decision 0.2.
  - **SET** → `ALTER TABLE {table} UPDATE {col}={expr} WHERE {pk}={id}`
  - **DELETE** → `ALTER TABLE {table} DELETE WHERE {pk} IN ({ids})`
  - **DETACH DELETE** → one DELETE per relationship table referencing the node label, then the node DELETE. Order matters; document.
- **New: `src/clickhouse_query_generator/id_gen.rs`** — emits the right ID expression in INSERT column lists per `NodeSchema.id_generation`.
- **Reuse note**: `clickgraph-embedded/src/write_helpers.rs` already builds INSERT/DELETE SQL for the direct Rust API. We **don't merge the two callers** in v1 — the Cypher path goes through the renderer, and the direct API keeps its bespoke builder. A future refactor could unify them, but conflating them now is yak-shaving.

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
    2. Executes generated SQL via the chdb connection
    3. Issues `SYSTEM SYNC REPLICA`/equivalent barrier per Decision 0.7
    4. Returns affected-row counters as a single-row `QueryResult` per Decision 0.8
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

- **`clickgraph-tck/tests/tck.rs:564-572`** — relax skip filter for write-tagged scenarios (keep MERGE skipped until Phase 5)
- Triage failures; aim for ~16 of 19 currently-skipped scenarios passing
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
| chdb mutation latency (`ALTER TABLE … UPDATE/DELETE` is async) | Decision 0.7 — post-write barrier by default |
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
