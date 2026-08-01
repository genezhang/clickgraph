# Path C → Path A collapse — design

Status: **design cycle, not scheduled**. Author: SQL-IR track. Companion to
`SQL_IR_DESIGN.md` §3.5 (Phase-2 path-collapse investigation).

> **TL;DR.** Path C (`render_expr_to_sql_string`, the CTE-build-stage expression
> renderer) is the last of the four render paths still standing after Phase-2
> steps 1–3 retired B, D, and the dual `Operator` enum. Collapsing it into the
> canonical Path A (`RenderExpr::to_sql`) is the highest-leverage *and*
> highest-risk step in the whole SQL-IR track. It is **blocked on one real
> architecture constraint** — a two-stage timing/aliasing difference, not lazy
> duplication — so it is a design cycle, taken when a trigger (a Path-C
> Databricks bug, or a third dialect) makes the single-printer guarantee pay for
> itself. This doc records the terrain, the two viable options, the byte-identity
> spike that must precede any migration, and the call-site batching plan.

---

## 1. What "collapse" means and why it's wanted

Today two functions turn a `RenderExpr` into a SQL string:

- **Path A** — `RenderExpr::to_sql()`
  (`src/sql_generator/emitters/clickhouse/to_sql_query.rs:6461`). Canonical
  final-SELECT renderer; covers all 20 live `RenderExpr` variants; dialect-aware
  via `FunctionMapper`/registry. Resolves table qualifiers **implicitly** from
  `query_context` task-locals.
- **Path C** — `render_expr_to_sql_string(expr, alias_mapping)`
  (`src/render_plan/cte_extraction.rs:1662`). Runs at **CTE-build time**; covers
  all 20 variants (exhaustive); resolves table qualifiers **explicitly** from a
  caller-supplied `alias_mapping: &[(String, String)]` slice.

"Collapse" = delete Path C, point its callers at Path A, leaving **one**
expression printer. The payoff is the SQL-IR track's whole thesis: a single
printer cannot drift, so the recurring class of Spark dialect bugs (a CH idiom
inlined in one path but dialect-routed in another — arrayConcat, CONTAINS,
tuple, FINAL, and most recently `arrayFold`/#842) becomes structurally
impossible. C still carries latent, corpus-unreached CH-only arms today (`POWER`,
map-literal, simple `CASE`); collapse retires them for free.

**The payoff is architectural, not a bug fix.** Every corpus `reduce`/`map`/
`case`/exponent expression currently reaches the renderer through A, not C (see
`SQL_IR_DESIGN.md` §3.5), so C's latent arms trigger **zero** current-corpus
bugs. That is precisely why this is not urgent.

## 2. The blocker (grounded)

Path C is **not** a lazy copy of A. It encodes a semantic A has no equivalent
for, and it runs at a stage where A's inputs do not yet exist.

### 2.1 Two-stage timing

C runs during CTE construction (`extract_ctes_with_context`,
`plan_builder.rs:237`). A runs during final-SELECT emission. The alias-resolution
task-locals A relies on — the `cte_alias_to_cte_name` registry
(`query_context.rs:1033`), the variable registry (`:685`), the VLP-alias set —
are **populated for the final SELECT, not yet at CTE-build time.** Point C's
call sites at A *as-is today* and a Cypher alias `a` resolves to itself (or
mis-resolves) instead of the CTE-local `start_node`.

Note the constraint is specific: C **does** already read *some* task-locals
(`get_current_schema`, `get_current_dialect`, `register_relationship_cte_name`,
…). It is the **alias-qualifier registry** in particular that is absent at C's
stage — not the whole context.

### 2.2 The `alias_mapping` rewrite is content-dependent

C's callers do a Cypher-alias → CTE-alias table-qualifier rewrite
(`a.name` → `start_node.name`, `f.Origin` → `rel.Origin`). The mapping is
**built locally per call** and, critically, **selected by inspecting the
expression** (`cte_extraction.rs:3159-3210`):

```rust
// FK-edge vs standard: rel maps to a different table alias
let rel_target_alias = if is_fk_edge_early { "start_node" } else { "rel" };
...
// Then per-expression: pick node-mapping or rel-mapping by what the expr references
let start_sql = mapped_start.as_ref().map(|expr| {
    if expr_uses_alias(expr, &rel_alias) {
        render_expr_to_sql_string(expr, &rel_alias_mapping)   // denormalized: f → rel
    } else {
        render_expr_to_sql_string(expr, &alias_mapping)        // standard: a → start_node
    }
});
```

This *selection* logic (standard node-mapping vs denormalized rel-mapping vs
FK-edge fold) has **no home inside A** — A never sees the raw Cypher rel-alias at
this stage. Any collapse must relocate this decision, not just the rendering.

## 3. Call-site inventory (what actually has to move)

**34 call sites across 4 files** bind Path C (`render_expr_to_sql_string`):

| File | Sites | Nature |
|---|---|---|
| `render_plan/cte_extraction.rs` | 28 | 21 are **internal recursion** (the arms of C itself calling C on sub-expressions); ~7 are true external entry points (`:839` node filter, `:3189-3210` the start/end/rel filter selection, `:5322` substituted-expr with empty mapping) |
| `render_plan/cte_manager/mod.rs` | 3 | start/end/rel filter rendering, `alias_mapping` threaded in (`:274/:282/:290`) — the pre-render half of `CategorizedFilters` |
| `render_plan/filter_builder.rs` | 2 | `:939`, `:1187` — both pass `&[]` (empty mapping ⇒ no rewrite needed, plain columns) |
| `render_plan/expression_utils.rs` | 1 | one call |

**Key simplification:** of the 28 in-file sites, only the ~7 external entry
points and the recursion *root* matter for a collapse — once C's body becomes A's
body, the 21 internal recursion sites disappear with it. And 3 of the external
callers already pass an **empty** `alias_mapping` (`&[]`, `filter_builder.rs` ×2
and `cte_extraction.rs:5322`), i.e. they need **no** rewrite — those migrate
trivially and are the natural first batch.

## 4. The two viable options

Both are real design changes. Each keeps the invariant: **ClickHouse output
byte-identical**, guarded by the dual-dialect golden net + 1,119-case corpus.

### Option A — thread an optional alias-mapping arg into the shared printer

Give the canonical printer an optional mapping override:

```rust
// A gains an override arg; final-SELECT passes None (unchanged), CTE-build passes Some.
impl RenderExpr {
    fn to_sql_with_aliases(&self, aliases: Option<&[(String, String)]>) -> String { ... }
    fn to_sql(&self) -> String { self.to_sql_with_aliases(None) }  // existing behavior
}
```

When `Some`, the `PropertyAccessExp` arm consults the override slice **before**
falling back to the implicit `get_cte_name_for_alias` registry; when `None`, it
is byte-identical to today.

- **Pro:** smallest blast radius. Final-SELECT path passes `None` → provably
  unchanged (the byte-identity argument is a one-liner). No change to *when*
  task-locals populate, so no ripple through the render pipeline.
- **Con:** the printer now carries two resolution modes. And the
  **content-dependent mapping *selection*** (§2.2's `expr_uses_alias` switch)
  still has to live at the call sites — so C **thins to a thin shim** (build the
  right mapping, call `to_sql_with_aliases`) rather than vanishing. Not a *full*
  collapse, but it deletes the duplicate 20-arm renderer, which is 90% of the
  drift surface.
- **Risk:** low–medium. The single new branch in the `PropertyAccessExp` arm is
  the only behavior-affecting change; everything else is mechanical.

### Option B — pre-install the alias registry into `query_context` at CTE-build

Populate the `cte_alias_to_cte_name` registry earlier so A's *implicit*
resolution already works during CTE build. **This seam already exists and is
already load-bearing:** `set_cte_alias_scope(mapping)` (`query_context.rs:1042`)
installs a scoped mapping and **returns the previous one for restore**, and Path
A's `PropertyAccessExp` arm **already reads it** via `get_cte_name_for_alias`
(`to_sql_query.rs:327`). So Option B is not inventing a mechanism — it is calling
an existing one at C's call sites:

```rust
// At each CTE-build filter site, instead of passing alias_mapping to C:
let prev = set_cte_alias_scope(mapping.into_iter().collect());
let sql = expr.to_sql();                 // A resolves through the scope we just set
set_cte_alias_scope(prev);               // restore (save/replace/restore, already the API's contract)
```

- **Pro:** true single printer — `alias_mapping` param deleted entirely, C gone.
  Uses the exact registry A already consults, so resolution semantics converge by
  construction.
- **Con / risk:** highest. Two real hazards:
  1. **The content-dependent *selection*** (§2.2) must still happen *before* the
     scope is set — you install `rel_alias_mapping` vs `alias_mapping` based on
     the same `expr_uses_alias` test. So the selection logic survives at the call
     site here too; Option B removes the *renderer* duplication but not the
     *mapping-choice* duplication (same as Option A).
  2. **Scope hygiene under recursion / nested CTEs.** `set_cte_alias_scope` is a
     whole-map replace, not a merge. If a filter expression triggers nested
     rendering that itself sets a scope, the save/restore must nest correctly.
     This is where a silent cross-CTE mis-qualification would hide, and it is why
     Option B needs the full-corpus byte-identity spike before any real
     migration.

**Recommendation: start with Option A.** It captures most of the payoff
(duplicate renderer deleted) at a fraction of the risk, and it does not foreclose
Option B later — once the printer takes an optional mapping, migrating call sites
to set a scope instead is an incremental follow-up if the residual shim proves
worth removing.

## 5. The byte-identity spike (must precede any migration)

Before touching call sites, prove the chosen option is byte-identical on the
**hard cases** — not the easy ones. A dedicated spike branch that:

1. Implements the option behind the scenes but routes **one** hard call site
   through it.
2. Diffs full corpus (`corpus_sweep`) + `sql_golden`, both dialects, expecting
   **zero churn**.
3. Exercises specifically:
   - **VLP + denormalized filter** (`f.Origin = 'LAX'` → `rel.Origin`) — the
     `rel_alias_mapping` branch.
   - **FK-edge VLP** (`is_fk_edge_early` → rel folds to `start_node`) — the
     no-separate-rel-table branch.
   - **Composite-id endpoint** in a CTE filter — multi-column qualifier rewrite.
   - **Nested CTE** where a filter expression's rendering could re-enter (Option
     B scope-hygiene check).

If any hard case churns, the option's resolution differs from C's and the diff
*is* the spec of what must be reconciled before proceeding. A clean spike is the
go/no-go gate.

## 6. Migration batching (only after a clean spike)

Migrate the 34 sites in **byte-checked batches**, never big-bang:

1. **Batch 0 — empty-mapping callers.** The 3 sites passing `&[]`
   (`filter_builder.rs:939/1187`, `cte_extraction.rs:5322`) need no rewrite;
   route them at A directly. Lowest risk, proves the plumbing.
2. **Batch 1 — `cte_manager` filter renderers** (`:274/:282/:290`). Single
   `CategorizedFilters` consumer, well-isolated.
3. **Batch 2 — standard node filters** (`cte_extraction.rs:839`, the
   `alias_mapping`-only sites).
4. **Batch 3 — the denormalized / FK-edge selection** (`:3189-3210`). The
   content-dependent branch — do last, most scrutiny.
5. **Batch 4 — delete C's body + the 21 internal recursion sites** (fall out
   automatically once no external caller remains).

Each batch: fmt · clippy · full lib · `corpus_sweep` + `sql_golden`
byte-identical (0 churn) · ratchet net-zero · adversarial worktree review. Same
per-slice protocol as every SQL-IR PR (`REFACTORING_SAFETY_PLAN.md` §8).

## 7. When to pick this up (trigger conditions)

Hold until one of:

- **A real Databricks bug traces to a Path-C arm** — i.e. C's latent CH-only
  arms (`POWER`/map/`CASE`) stop being corpus-unreached because a new query or
  schema reaches them at CTE-build stage. Then the collapse (or at minimum
  routing that one arm, like #842 did for `arrayFold`) becomes a bug fix, not a
  refactor.
- **A third dialect (Postgres/DuckDB) is on the table.** The single-printer
  guarantee becomes load-bearing the moment there are three spellings to keep in
  sync across two render paths; the drift cost goes superlinear.

Absent a trigger, the current state is a coherent stopping point
(`SQL_IR_DESIGN.md` §3: "We can stop after any phase with a coherent, better-off
codebase"). Steps 1–3 of Phase 2 are done; this is step 4, deferred by design.

## 8. Non-goals

- Not a `sqlparser`/standalone-AST adoption — `RenderExpr` is the AST.
- Not removing the *mapping-selection* logic (§2.2) — both options relocate it,
  neither eliminates it; that is a separate simplification if ever wanted.
- Not touching `RenderExpr::Raw` / `CteContent::RawSql` — that is Phase 4.
