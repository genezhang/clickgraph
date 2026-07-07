# Refactoring safety plan — codebase quality & regression reduction

Status: **proposal for review** (no code yet). Audit date: 2026-07-06.
Companion to `docs/design/SQL_IR_DESIGN.md` (whose Phase-0-golden-net-then-leaf-slices
discipline this plan generalizes) and `src/render_plan/AGENTS.md` (whose §10
forward-resolution design is Phase 4 here).

> **For agents picking up a slice**: read §2 (principles) and §8 (per-slice
> protocol) first, then jump to your phase. Every slice is a small PR with a
> mechanical acceptance criterion — most commonly "generated SQL is
> byte-identical". Line numbers are as of 2026-07-06; re-verify before editing.

## Background: how the debt accrued

The engine was built **one schema variation at a time** (standard → FK-edge →
denormalized → polymorphic → composite-id → coupled). Each new variation was
supported the locally-cheapest way: an `if is_denormalized`/`if is_fk_edge`/
table-name-comparison branch added inline at whatever planner/renderer site
the feature touched — rather than at one dispatch chokepoint. By variation 6,
every pipeline stage carried its own copy of every pattern decision (§1.5:
406 `is_denormalized` sites, ≥7 independent "is this node denormalized"
implementations). Several unification refactors were then attempted and
**failed with large regressions — predictably**, because the byte-exact test
net covers only the *standard* variation (§1.2): any refactor of the shared
code paths was flying blind over 5 of the 6 variations it was unifying.

**Now a second axis is being added**: Databricks/Spark SQL alongside
ClickHouse. Two axes of variation multiply: (6 schema patterns) × (2 dialects)
× (query features). The critical observation is that the **dialect axis is
already being contained the right way** — the SQL-IR workstream
(`docs/design/SQL_IR_DESIGN.md`) put a golden net down *first*, grew a single
`Dialect`/`FunctionMapper` dispatch point, migrated leaf-by-leaf with a
byte-identical invariant, and produced **zero ClickHouse regressions** across
~15 slices while fixing 5 latent Databricks bugs. That is the template. The
schema-pattern axis has a stalled equivalent (`PatternSchemaContext`, §1.5);
this plan applies the same recipe to it — and installs guardrails (§2.1) so
neither axis re-accretes inline branches while the migration proceeds.

**Rule of thumb this plan enforces**: *every axis of variation gets exactly
one dispatch abstraction, and no site may branch on the raw axis directly* —
`Dialect` for the dialect axis, `PatternSchemaContext`/schema-catalog APIs for
the schema-pattern axis.

## TL;DR

Past refactorings regressed for one primary reason and three amplifiers.

**Primary**: the always-on test net cannot *see* SQL changes. The Rust suite
(~2,060 tests, the only thing CI runs) asserts substrings — 1,588 `contains()`
vs 72 exact-equality checks; `tests/rust/integration/ldbc_regression_tests.rs`
mostly asserts `sql.contains("SELECT")`. The only byte-exact lock
(`tests/rust/integration/sql_golden_tests.rs`, 44 cases × 2 dialects) covers
**only the standard schema variation**. CI runs 2 of ~1,030 result-asserting
Python integration tests, and the `push` trigger is disabled. So a refactor
that subtly changes denormalized/polymorphic/FK-edge SQL produces **zero
failing tests** until someone runs the live suite.

**Amplifiers**:
1. **Manual tree traversal** — `LogicalPlan` (21 variants) has no `children()`
   accessor; 200+ hand-rolled walkers with `_ =>` catch-alls silently skip new
   variants (the historical missing-`Unwind`/`Limit` bug class). The five
   WITH-traversal functions (CLAUDE.md §6 invariant) **have already drifted**.
2. **God-file** — `plan_builder_utils.rs` is 18,207 lines / 177 functions / 9
   responsibilities; one function is 5,746 lines.
3. **Schema-pattern dispersion** — pattern dispatch (denormalized/FK-edge/
   polymorphic/composite) is re-derived inline at every pipeline stage; the
   abstraction built to fix it (`PatternSchemaContext`) is half-adopted.

**The plan**: widen the net first (Phase 0) so "byte-identical output" becomes
a provable property; then make traversal compiler-enforced (Phase 1); then
mechanically dedupe/split the god-file (Phase 2); then finish the
pattern-dispatch rollout (Phase 3); and only then touch the entangled WITH→CTE
core and the §10 forward-resolution rewrite (Phase 4). Every slice ships as
its own reviewed PR with ClickHouse output byte-identical (or a reviewed,
intentional golden diff).

---

## 1. Diagnosis (grounded)

### 1.1 Where regressions actually land

210 `fix:` commits since 2026-01. Files most touched by fix commits since
2026-03: `render_plan/plan_builder_utils.rs` (26),
`sql_generator/emitters/clickhouse/to_sql_query.rs` (21, +16 under its
pre-move path), `render_plan/cte_extraction.rs` (19),
`render_plan/plan_builder_helpers.rs` (13), `render_plan/plan_builder.rs` (12).
The render layer absorbs the repair work; that is where the net must be
densest and where structure must be safest.

### 1.2 Safety-net audit

| Layer | Size | Assertion quality | Runs in CI? |
|---|---|---|---|
| Rust unit/integration (`cargo test`) | ~2,060 tests | 1,588 `contains()` / 72 `assert_eq!`; 64 near-vacuous (`contains("SELECT")` etc.) | ✅ every PR (no ClickHouse — CI runs `cargo test` before starting CH) |
| Golden byte-exact SQL (`sql_golden_tests.rs`) | 44 cases × {clickhouse, databricks} = 88 files in `tests/rust/integration/golden/sql_ir/` | exact string equality, alias/CTE counters normalized | ✅ (part of `cargo test`) |
| Python integration (`tests/integration/`) | 99 files / 1,030 tests, needs live CH + server, 6-schema `unified_test_multi_schema.yaml` | result-asserting | ❌ **2 tests only**; `push` trigger commented out in `ci.yml` |
| `tests/sql_generation/` (62), `tests/e2e/` (24) | needs live server | mixed | ❌ |
| Parity harnesses (`scripts/ldbc_parity_sweep.py`, `CG_TEST_BACKEND=databricks` mode, `tests/spark_smoke/`) | CH↔Databricks execution parity | result-set comparison | ❌ manual / env-gated |

Schema-variation coverage of the **byte-exact** layer: standard ✅; FK-edge,
denormalized, polymorphic, composite-id **all ❌** (the golden harness loads
only `benchmarks/social_network/schemas/social_benchmark.yaml`). The gaps the
golden file itself documents (lines ~257–263): EXISTS/pattern predicates,
composite IDs, denormalized, multi-label, UNWIND shapes.

### 1.3 Traversal fan-out and the five-function drift

- `LogicalPlan` has **21 variants** (`src/query_planner/logical_plan/mod.rs:467-525`):
  15 single-input wrappers, 3 multi-child (`GraphRel`=3 children,
  `CartesianProduct`=2, `Union`=N), 3 leaves (`Empty`, `PageRank`, `ViewScan` —
  though `ViewScan` carries an optional `.input`).
- **No** public `children()`/visitor/fold exists. The only exhaustive child
  enumeration is the *private* `fmt_with_tree` (`mod.rs:1712`). Only
  `count_plan_nodes_impl` (`mod.rs:1666`) and `fmt_with_tree` match all 21
  variants; everything else uses `_ =>` catch-alls.
- ~17 analyzer/optimizer passes hand-copy the same
  `match plan { X(x) => x.rebuild_or_clone(...) }` dispatch
  (`group_by_building.rs`, `projection_push_down.rs`, `filter_push_down.rs`,
  `type_inference.rs`, `filter_tagging.rs`, `graph_traversal_planning.rs`,
  `filter_into_graph_rel.rs`, …) — each with a `_ =>` fall-through, so a new
  variant is silently not recursed by every pass.
- **The five WITH functions have drifted** (all in `plan_builder_utils.rs`):

| Function (line) | GraphRel.center | Cte.input | ViewScan.input | write variants |
|---|---|---|---|---|
| `has_with_clause_in_tree` (3914) | ✅ | ✅ | ✅ | ❌ |
| `plan_contains_with_clause` (4476) | ❌ | ❌ | ❌ | ❌ |
| `find_all_with_clauses_grouped` (13536) | ✅ | ✅ | ✅ | ❌ |
| `needs_processing` (14951, nested) | inherits `plan_contains_with_clause` gaps | | | ❌ |
| `replace_with_clause_with_cte_reference_v2` (14403) | helpers use small arm sets with `_ =>` | | | ❌ |

  A WITH reachable only through `GraphRel.center` or under a `Cte` is *found*
  by detection but *invisible* to the guard checks — the exact §6 infinite-
  iteration/lost-WITH bug class, live today.
- Expression trees: proper abstractions exist — `ExpressionVisitor`/
  `walk_expression` for `LogicalExpr` (`query_planner/logical_expr/visitors.rs:43/92`)
  and `ExprVisitor::transform_expr` for `RenderExpr`
  (`render_plan/expression_utils.rs:62`) — but only 3+3 adopters, vs ~22 manual
  `RenderExpr` rewriters and ~14 manual `LogicalExpr` rewriters.

### 1.4 `plan_builder_utils.rs` anatomy

- 18,207 lines; 124 top-level + 53 nested functions; file-wide
  `#![allow(dead_code)]` (line 41); header docstring stale (claims 10,807
  lines / "pure utilities with no LogicalPlan dependency" — 44+ functions take
  `&LogicalPlan`).
- `build_chained_with_match_cte_plan` = lines **7333–13078 (~5,746 lines, 31%
  of the file)**, ~30 nested helper fns, local `RefCell` state mutated by
  nested closures. `replace_with_clause_with_cte_reference_v2` = ~1,089 lines.
- **Narrow inbound API**: only 39 external call sites from 3 files
  (`plan_builder.rs` dominant; `join_builder.rs`, `properties_builder.rs`).
  Everything else is internal to the two giant entry points. This makes the
  split much safer than the raw size suggests.
- **Circular dependency**: utils line 55 imports `RenderPlanBuilder` from
  `plan_builder.rs`, which imports utils functions back (its line 36). Glob
  imports of `plan_builder_helpers::*` and `alias_utils::*` make shadowing
  invisible.
- Nine distinct responsibilities (see §5.1 module map).

Duplication clusters (each is a Phase-2 slice):

| # | Cluster | Sites |
|---|---|---|
| D1 | WITH-key generation — **verbatim triplicate** | `generate_with_key_from_with_clause` :13627, `get_with_key` :13921, `get_with_clause_key` :14471 |
| D2 | CTE property-rewriter family (~4 near-identical) | `rewrite_render_expr_for_cte_simple` :1107, `_operand` :1136, `_with_context` :1196, wrapper :1178; operator pair :1067/:1088 |
| D3 | VLP expression-rewriter generations (4) | :388, :403, :625, :803 (`_legacy`) |
| D4 | WITH-presence predicates (6 overlapping walkers) | :3899, :3914, :3957, :3989, :4476 + `_impl` twins |
| D5 | Twin unwind collectors (nested) | `collect_unwind_aliases` :8773, `find_unwind_aliases` :11598 — NB one deliberately stops at the WithClause barrier; unify with a `stop_at_with: bool` param |
| D6 | Alias→SELECT expansion STEP 2/3/4 duplicated inline | `expand_table_alias_to_select_items` :5394 vs inline copy :8337–8388 inside the giant fn |
| D7 | Edge-id column lookup trio | :16837, :16922, :17352 |
| D8 | Nested re-declarations | `extract_alias_from_expr` :1418 vs :13564; `is_cte_reference` :2348 vs :7807; `contains_aggregate` ×2 (:8506, :9191) |

### 1.5 Schema-pattern dispersion

Predicate occurrence counts across `src`: `is_denormalized` 406,
`from_node_properties` 274, `to_node_properties` 259, `type_column` 206,
`from_label_column` 181, `is_fk_edge` 123. Spread across all four stages
(graph_catalog, query_planner, render_plan, sql_generator); the renderer alone
holds ~330.

What already exists:
- **`PatternSchemaContext`** (`src/graph_catalog/pattern_schema.rs`):
  `NodeAccessStrategy` (:83, `requires_join()`/`is_embedded()`/
  `get_property_column()`), `EdgeAccessStrategy` (:172, `from_id_column()`/
  `to_id_column()`/`is_polymorphic()`/`get_type_filter()`), `JoinStrategy`
  (:395 — Traditional/SingleTableScan/FkEdgeJoin/MixedAccess/EdgeToEdge/
  CoupledSameRow), computed once by `PatternSchemaContext::analyze()` (:599).
  Module header states the exact consolidation intent; `#![allow(dead_code)]`
  at :13 shows it stalled. Adopted by ~13 files (graph_join analyzer,
  cte_manager, parts of cte_extraction) — the renderer **runs it in parallel
  with the legacy flag path** (`cte_extraction.rs` ~3591 marks the unfinished
  "Phase 2").
- **`CteStrategy`** (`render_plan/cte_manager/mod.rs:508-573`) — covers only
  the recursive-CTE/VLP path; single-hop rendering bypasses it.
- **Schema-layer canonical answers** that call sites bypass:
  `is_node_denormalized_on_edge` (`graph_schema.rs:1621`) — **no production
  caller**; `edge_has_node_properties` (:1689) — one caller;
  `classify_edge_table_pattern` (:1712) — only pattern_schema.rs;
  `RelationshipSchema.is_fk_edge` field (authoritative, computed in
  `config.rs:1502/1714`) — yet `is_fk_edge` is **re-derived by table-name
  comparison** at `multi_type_vlp_joins.rs:764`, `:1471`, and
  `cte_extraction.rs:6200`; `Identifier::first_column` (`config.rs:234`) —
  reimplemented as `first_col` at `graph_join/join_generation.rs:718`.
- ≥7 independent implementations of "is this node denormalized (on this
  edge)": `node_classification.rs:54`, `graph_schema.rs:1621`,
  `plan_builder_helpers.rs:335`, `join_builder.rs:177` (`vlp_is_denormalized`),
  two local closures in `cte_extraction.rs` (:5324, :5382) plus
  `is_node_denormalized_from_graph_node` (:6236), and an inline re-derivation
  at `view_scan.rs:963-964`.
- The **composite-id** helpers (`Identifier`/`NodeIdSchema` —
  `is_composite()`/`columns()`/`sql_tuple()`/`sql_equality()`) are the
  well-adopted counterexample proving this consolidation style works here.

---

## 2. Design principles

1. **Net before knife.** No behavior-adjacent refactor lands before Phase 0
   gives us byte-exact coverage of the schema variations that refactor
   touches. This is the single lesson of the SQL-IR refactor's success
   (Phase 0 golden net → zero CH regressions across ~15 slices).
2. **Byte-identical invariant.** Default acceptance for every slice: generated
   SQL for the full golden + corpus sweep is byte-identical. If a slice
   *intends* an SQL change, the golden diff is reviewed explicitly and the
   goldens are regenerated in the same PR.
3. **One slice, one PR.** Never mix a structural move with a behavior change.
   Never mix two duplication-cluster removals.
4. **Compiler over convention.** Where a bug class comes from "N functions
   must agree" (the §6 invariant), replace the convention with a type the
   compiler checks (exhaustive `children()`), not with more documentation.
5. **Transition-assert before switch.** When replacing an inline re-derivation
   with a canonical API (Phase 3), first land a PR where both are computed and
   `debug_assert_eq!`'d (+ corpus sweep); switch only after the sweep is green.
   Inline derivations may encode *intentional* divergence — prove they don't
   before deleting them.
6. **Review agents run with `isolation: worktree`** (a non-isolated review
   agent once ran `git checkout main` mid-slice). Merge process per the
   established subagent-review procedure.

### 2.1 Guardrails against re-accretion (start immediately, independent of phases)

The migration takes months; without a stop-the-bleeding mechanism, new inline
branches accrete faster than old ones are removed. Two cheap mechanisms:

1. **Ratchet test** (a plain `cargo test`): committed baseline counts of raw
   axis predicates per module — occurrences of `is_denormalized` /
   `is_fk_edge` / `type_column` / `from_label_column` /
   `from_node_properties` outside `graph_catalog/`, and of dialect branching
   (`Dialect::`, `if dialect`, `databricks`) outside `sql_generator/`. The
   test fails if any count **increases** (decreases auto-ratchet down by
   updating the baseline file in the same PR). New code must route through
   the dispatch abstraction or consciously bump the baseline with a justifying
   comment — making debt addition visible in review instead of silent.
2. **PR checklist rule** (CLAUDE.md + AGENTS.md): a change that behaves
   differently per schema pattern must consume `PatternSchemaContext`/schema
   APIs (§6.1); a change that emits dialect-specific SQL must go through
   `Dialect`/`FunctionMapper`. Reviewers reject raw-flag branching in new
   code.

---

## 3. Phase 0 — widen the regression net

### 3.1 Golden corpus × schema variations

**Goal**: byte-exact SQL locks for FK-edge, denormalized, polymorphic,
composite-id (and coupled-edge) — not just standard.

**Mechanics** (extend `tests/rust/integration/sql_golden_tests.rs`):
- Add a `schema` dimension to the case table. Today the harness loads one
  YAML; parameterize per-case: `{ name, schema: SchemaId, cypher }` with
  schemas drawn from `schemas/test/` (e.g. `social_polymorphic.yaml`,
  `denormalized_flights.yaml` / `flights_denorm_test`, `composite_node_ids.yaml`,
  the fk_edge schema, `zeek_merged_test` for coupled/virtual-id denorm).
- Queries are **not** portable across schemas (labels/properties differ), so
  each variation gets its own case list mirroring the standard set's feature
  axes: node scan, property projection + renamed property, WHERE, single hop,
  undirected hop, OPTIONAL MATCH, WITH+aggregation, WITH→MATCH chain, VLP
  `*1..3`, multi-type `[:A|B]`, UNWIND, `RETURN n` whole-entity, ORDER/SKIP/
  LIMIT. Target ~25–40 cases per variation; grow opportunistically (every bug
  fix adds its repro as a golden).
- Golden layout: `golden/sql_ir/{schema}/{case}.{dialect}.sql`. Reuse the
  existing alias/CTE-counter normalization and the `UPDATE_GOLDEN=1`
  regeneration flow. Keep the existing anti-vacuous guard
  (`assert!(sql.contains("SELECT"))`).
- Include the known-fragile shapes from the Browser-bug workstream as cases:
  unlabeled `(n)-[r]-(o)` expand, `MATCH p=()-[]->()` path render,
  property-key-probe UNION — these route through `pattern_union`/fixed_path
  renderers that have zero golden coverage today.

**Both dialects, every case.** The harness already emits per-dialect goldens;
every new case locks ClickHouse **and** Databricks output, making the golden
matrix schema-variation × dialect. This is what lets the DeltaGraph work and
the schema-pattern work proceed concurrently without stepping on each other:
either axis's refactor proves no-op-ness on both dimensions at once. For
*new* Databricks goldens covering shapes never executed on Spark, prefer
live-verifying once before locking (`scripts/dbx_run.py` / `tests/spark_smoke`
per the SQL-IR workflow) — a golden-locked wrong SQL is worse than no golden.

**Slices** (one PR each): P0.1 harness schema-dimension + FK-edge set ·
P0.2 denormalized set · P0.3 polymorphic set · P0.4 composite-id set ·
P0.5 Browser-shaped patterns set.

### 3.2 Corpus translation-snapshot sweep

**Goal**: a mass "translate everything, lock the bytes" net so any refactor
can prove no-op-ness over thousands of queries, not 200.

**Design**:
- Harvest the Cypher corpus from the existing test surface (the dual-dialect
  sweep already worked over a ~2,900-query corpus): extract query strings +
  their schema from `tests/integration/**` and `tests/sql_generation/**` into
  `tests/corpus/queries.jsonl` (`{schema, name, cypher}`). A small harvester
  script keeps it regenerable; the JSONL is committed.
- A Rust test binary (fast, no server): for each entry, parse→plan→render with
  the entry's schema, **once per dialect**; write/compare
  `golden/corpus/{schema}/{hash}.{dialect}.sql`.
  Queries that error are locked too (`.err` files with the error string) — an
  error→success transition is also a visible diff, and so is the reverse.
- Runtime budget: translation is milliseconds/query; the whole sweep should be
  well under a minute — cheap enough for `cargo test` inclusion (gate behind a
  feature or `--ignored` + a dedicated CI step if it grows).
- Same `UPDATE_GOLDEN=1` update flow; diffs reviewed like code.

**Known limitation** (accepted): translation-locks miss exec-only bugs
(#397/#398 class). That's what 3.3 is for.

### 3.3 CI wiring

- Re-enable the `push` (main) trigger in `ci.yml`.
- PR gate additions: the corpus sweep (3.2) + a **per-variation Python smoke
  subset** (~50 tests: a handful per schema in
  `unified_test_multi_schema.yaml`, tagged e.g. `@pytest.mark.smoke`) — CI
  already stands up ClickHouse + server for the current 2 tests, so this is
  marker selection, not new infra.
- Nightly scheduled workflow: full `pytest tests/integration/` against CH;
  optionally the Databricks parity sweep when credentials are available.
  KEY LEARNING from #439: string-level assertions cannot guard CTE-scoping
  bugs; only executed parity catches them — that guard must stop being
  manual-only.

**Slices**: P0.6 corpus harvester + sweep test · P0.7 CI push trigger +
smoke markers · P0.8 nightly workflow.

---

## 4. Phase 1 — compiler-enforced traversal

### 4.1 `children()` on `LogicalPlan`

Add to `src/query_planner/logical_plan/mod.rs` (promote the enumeration that
already exists privately in `fmt_with_tree`, `mod.rs:1712`):

```rust
impl LogicalPlan {
    /// ALL structural children, exhaustively. NO catch-all arm — adding a
    /// LogicalPlan variant must fail compilation here until handled.
    /// Includes GraphRel.center, Cte.input, ViewScan.input, and the write
    /// variants' inputs. Walkers with scope/barrier semantics must implement
    /// their stop conditions explicitly at their own match sites.
    pub fn children(&self) -> SmallVec<[&LogicalPlan; 3]>;

    /// Pre-order walk; `f` returns ControlFlow to allow early exit / pruning.
    pub fn walk<B>(&self, f: &mut impl FnMut(&LogicalPlan) -> ControlFlow<B, Descend>) -> Option<B>;

    /// Convenience predicates built on walk():
    pub fn any_node(&self, pred: impl FnMut(&LogicalPlan) -> bool) -> bool;
    pub fn find_map_node<T>(&self, f: impl FnMut(&LogicalPlan) -> Option<T>) -> Option<T>;
}
```

(`Descend` = `Yes | Skip` so scope-aware walkers can prune a subtree — e.g.
"don't descend past a WithClause barrier" — without reintroducing manual
recursion. Exact shape at implementer's discretion; the non-negotiables are:
exhaustive match, prune support, early exit.)

Policy decision baked in: `children()` returns **everything**. The historical
divergences (skip `Cte.input`? skip `center`?) become explicit `Skip` returns
at the call site, visible and greppable, instead of silently-missing match
arms.

**Slices**: P1.1 add API + rewrite the trivially-mechanical read-only walkers
in `logical_plan/mod.rs` itself (`is_optional_pattern`, `has_union_anywhere`,
`contains_variable_length_path`) as proof · P1.2–P1.n migrate walker clusters
file-by-file (each PR corpus-verified). ~44 named walkers in
plan_builder_utils alone (§1.4); prioritize the ones with confirmed missing
arms: `detect_vlp_endpoint_from_plan` (:5223 — still missing
Create/Set/Delete/Remove/PageRank/GroupBy), `find_multi_type_graph_rel`
(`from_builder.rs:1014`), `has_union_anywhere` (misses `Cte`).

### 4.2 The five WITH functions (and D4/D5)

This is the highest-value migration and the one requiring an actual decision:

1. **Characterize first**: add unit tests capturing current answers of all
   five functions over a matrix of synthetic plans (WITH under GraphRel.center
   / under Cte / under ViewScan.input / under Unwind / under CartesianProduct
   / under write variants). This documents the drift as behavior.
2. **Decide the semantics**: most plausibly the narrower traversal of
   `plan_contains_with_clause` is *accidental* (it postdates the Feb-2026 fix
   that added Unwind/CartesianProduct but nobody added center/Cte/ViewScan).
   But do NOT assume: construct a query that puts a WITH under GraphRel.center
   (e.g. WITH-in-subpattern via the chained-MATCH path) and see which answer
   produces correct SQL. If a divergence is intentional, it becomes a named
   parameter (`descend_into_cte: bool`), not an accident.
3. **Unify**: reimplement all five on `walk()`; collapse D4's six predicates
   into at most two (existence check + grouped collection) with explicit
   barrier parameters; merge D5's twin unwind collectors with a
   `stop_at_with: bool`.
4. Handle **write variants** in all of them (today a WITH inside a write
   pipeline's input is invisible to all five — latent §6 bug).

Acceptance: corpus sweep byte-identical, plus the characterization tests
updated to the *decided* semantics with the decision documented in
`render_plan/AGENTS.md` §6 (which then shrinks from "five functions must
agree" to "walk() is exhaustive; barriers are explicit").

### 4.3 Generic transform driver

Collapse the ~17 hand-copied `rebuild_or_clone` dispatch matches into one:

```rust
/// One exhaustive bottom-up rewrite driver. Passes implement only the arms
/// they care about via the visitor; recursion/rebuild is generic.
pub fn transform_up(
    plan: &Arc<LogicalPlan>,
    f: &mut impl FnMut(&Arc<LogicalPlan>) -> Result<Transformed<Arc<LogicalPlan>>, ...>,
) -> Result<Transformed<Arc<LogicalPlan>>, ...>;
```

built on an exhaustive `map_children` (same no-catch-all rule). Migrate passes
one per PR: `projection_push_down`, `filter_push_down`, `group_by_building`,
`cte_schema_resolver`, `cte_column_resolver`, `projection_tagging`,
`plan_sanitization`, `projected_columns_resolver`, `duplicate_scans_removing`,
`filter_into_graph_rel`, `query_validation`, `type_inference`,
`graph_traversal_planning`, `filter_tagging`, `graph_join/inference`,
`view_optimizer`. Each migration is mechanical; the win is that new variants
recurse by default instead of silently not.

### 4.4 Expression rewriters

No new infrastructure — **adopt** the existing `ExprVisitor::transform_expr`
(`expression_utils.rs:62`) and `walk_expression` (`visitors.rs:92`). ~22
manual `RenderExpr` rewriters and ~14 `LogicalExpr` rewriters mostly duplicate
the recursion tail. Notable duplicate to kill in the same effort:
`apply_property_mapping_to_expr` exists in BOTH `plan_builder_helpers.rs:2423`
and `cte_extraction.rs:1633`. Migrate opportunistically (whenever a rewriter
is touched for any reason, it moves to the visitor) plus dedicated slices for
the D2/D3 clusters (§5.2).

---

## 5. Phase 2 — dedupe & split `plan_builder_utils.rs`

### 5.1 Target module map

Split along the audited seams, **pure groups first** (lowest risk):

| New module | Contents (current line ranges) | Purity |
|---|---|---|
| `render_plan/vlp_rewrite.rs` | VLP expr rewriting, 115–812 + `extract_vlp_alias_mappings` :1296 | pure `&mut RenderExpr` transforms — **move first** |
| `render_plan/pattern_comprehension_sql.rs` | 15593–18207 (~2,600 lines, 29 fns) — emits SQL *strings*, a different layer from the rest | cohesive, separable |
| `render_plan/clause_extractors.rs` | the `extract_*` pipeline 1418–3300 (`extract_filters/from/group_by/having/order_by/limit/skip/distinct` + embedded tests) | the only externally-consumed group; mostly pure |
| `render_plan/plan_predicates.rs` | WITH-detection + alias/table lookups 3850–4247, 4418–4519 | pure read-only walkers — natural home for the §4 `walk()` rewrites |
| `render_plan/cte_rewrite.rs` | CTE-ref extraction + CTE/alias rewriting 813–1295, 3323–3849, 6102–6970 | pure-ish (RenderPlan/RenderExpr + maps) |
| `render_plan/with_to_cte/` (dir) | 6999–15491: the two giant builders + their orbit | **entangled core** — moved in Phase 2, decomposed in Phase 4 |

Rules for the move slices: `pub(crate)` re-exports from the old path during
transition; no logic edits in a move PR; corpus sweep byte-identical.

### 5.2 Duplication kill list

One PR per cluster from §1.4's table. Canonical survivors:
- **D1**: keep one `with_clause_key()` in `utils/` next to `cte_naming`;
  delete the other two.
- **D2**: one `rewrite_render_expr_for_cte(expr, ctx)` where `ctx` carries the
  alias-writing policy (keep-alias / cte-alias / cte-name) and the
  double-encoding guard from `_operand` (:1141-1147) — the guard is the only
  semantic difference; verify it's safe to apply universally (transition-assert
  per §2.5).
- **D3**: keep `_with_endpoint_info`, express the other three as thin
  wrappers, delete `_legacy` after a corpus-verified switch.
- **D6**: the inline STEP 2/3/4 copy inside the giant fn calls
  `expand_table_alias_to_select_items` instead. NOTE this is also the site
  entangled with issue **#411** (generic `.id` vs renamed node_id spans STEP
  2.5/3/4 + `extract_select_items` + `variable_scope::resolve`) — dedup here
  *reduces* the number of paths #411 must fix, which is exactly why #411 was
  deemed un-fixable piecemeal. Do the dedup as pure consolidation (behavior
  identical, including the bug); fix #411 after (Phase 4).
- **D7**: one `find_edge_id_column(schema, edge, role)` in graph_catalog.
- **D8**: hoist the nested re-declarations to module level, delete twins.

### 5.3 Import hygiene

- Break the utils↔plan_builder cycle: after the 5.1 split, `plan_builder.rs`
  depends on the new modules; nothing imports the `RenderPlanBuilder` trait
  back into them (the two giant builders take what they need as parameters —
  they already thread `plan_ctx` explicitly; entanglement is intra-function
  `RefCell`s, not globals, so this is mechanical).
- Replace glob imports (`plan_builder_helpers::*`, `alias_utils::*`) with
  named imports so shadowing becomes visible.
- Delete the three files' `#![allow(dead_code)]`; delete what the compiler
  then flags (investigate before deleting, per the late-stage-project rule —
  but the blanket allow has hidden dead surface for months).
- Fix the stale header docstrings; update `render_plan/AGENTS.md`'s module
  diagram in the same PRs.

---

## 6. Phase 3 — finish the `PatternSchemaContext` rollout

**Goal**: pattern dispatch answered in exactly one place per question; the
"fix it 5 times, once per schema pattern" tax ends.

### 6.1 Canonical APIs (already exist)

| Question | Canonical answer |
|---|---|
| Is node N denormalized on edge E in role R? | `GraphSchema::is_node_denormalized_on_edge` (`graph_schema.rs:1621`) |
| Does edge E embed node properties for role R? | `edge_has_node_properties` (:1689) |
| What pattern is this edge table? | `classify_edge_table_pattern` (:1712) |
| Is this an FK-edge? | `RelationshipSchema.is_fk_edge` field (computed `config.rs:1502/1714`) |
| Physical id column(s) for node in role? | `PatternSchemaContext`/`EdgeAccessStrategy::{from,to}_id_column`; `NodeIdSchema`/`Identifier` helpers for composite |
| How do these endpoints join? | `JoinStrategy` via `PatternSchemaContext::analyze` (:599) |

### 6.2 Call-site migration list (one small PR each, transition-assert first)

1. The 7 denormalization checks (§1.5) → `is_node_denormalized_on_edge` /
   `NodeAccessStrategy::is_embedded()`. Note several current copies operate on
   `LogicalPlan`/`ViewScan` flags rather than schema — the migration must keep
   reading plan-carried state where type inference *rewrote* it (ViewScan
   copies of the flags are set during planning; decide per-site whether schema
   or plan is the source of truth, and record the decision in the PR).
2. Inline `is_fk_edge` re-derivations (`multi_type_vlp_joins.rs:764`, `:1471`,
   `cte_extraction.rs:6200`) → the schema field. These compare
   `rel_table == end_table`, which is *not* the same predicate for coupled/
   denormalized edges — transition-assert will tell us whether that divergence
   ever fires; if it does, that's a latent bug to file, not to silently change.
3. `first_col` reimplementation (`graph_join/join_generation.rs:718`) →
   `Identifier::first_column` (`config.rs:234`).
4. `view_scan.rs:963-964` inline denorm re-derivation → classifier.
5. The `start/end_is_denormalized + is_mixed` recomputation cluster
   (`cte_extraction.rs` :3600/:5354, `cte_manager/mod.rs:2728`,
   `variable_length_cte.rs:122/354/397`, `expression_utils.rs:454`,
   `filter_pipeline.rs:448`) → computed once in `PatternSchemaContext`,
   carried in the existing `VlpConfig`/CTE contexts instead of re-derived.

### 6.3 Retire the parallel legacy path

`cte_extraction.rs` ~3591–3645 runs `pattern_ctx` alongside the legacy scalar
flags. Once Phase 0 goldens cover all variations **and** the §6.2 slices are
in, delete the legacy branch (its own PR; goldens are the guard). Then remove
`#![allow(dead_code)]` from `pattern_schema.rs` and delete whatever is still
genuinely unused.

**Explicit non-goal**: forcing single-hop rendering through `CteStrategy`.
That's a bigger architectural bet; this phase only consolidates *predicates
and column resolution*, not join generation strategy. (The denorm-foreign-edge
union-dimension design is a separate, perf-staged workstream that will benefit
from this consolidation but is not part of it.)

---

## 7. Phase 4 — the entangled core (last)

### 7.1 Decompose `build_chained_with_match_cte_plan`

Hoisting protocol, one nested function per PR:
1. Order: leaf helpers with no closure captures first; then helpers capturing
   the `RefCell` locals (captures become explicit `&mut` /context-struct
   params — introduce a `WithCteBuildState` struct holding what the RefCells
   hold today, e.g. `flattened_compound_keys` ~:7355).
2. Each hoist: move to `with_to_cte/` module level, name it, add a doc
   comment, dedupe against any §5.2 survivor it duplicates.
3. Corpus sweep byte-identical every PR.
4. Same treatment afterward for `replace_with_clause_with_cte_reference_v2`.

Exit criterion: no function in the module exceeds ~500 lines; the STEP
pipeline of the WITH→CTE build is readable top-to-bottom in the entry
function.

### 7.2 Forward resolution through CTE scope (§10)

The true architectural fix, already fully designed in
`render_plan/AGENTS.md` §10: resolve `(cypher_alias, cypher_property)` →
CTE column *forward* via `property_mapping`; make `ExistsSubquery`/
`PatternCount`/NOT-EXISTS `Raw` carry structured `RenderExpr` sub-trees
instead of pre-baked SQL strings; then delete `reverse_mapping` (~88 usages)
and the "also add DB column mapping" fallbacks. Its three phases stand as
written there. Preconditions from this plan: Phase 0 net (its blast radius is
the full WITH surface), Phase 1 `walk()` (its rewriters stop silently skipping
variants), Phase 2 dedup (fewer paths to migrate).

### 7.3 #411 (generic `.id` vs renamed node_id)

Explicitly **not** attempted before this phase (two prior attempts reverted;
full notes on the issue). The clean fix — normalize generic `.id` ↔ node_id at
one schema-aware chokepoint while preserving output aliases — becomes
tractable exactly when 7.1/D6 have collapsed the ≥4 expansion paths that each
drop the id differently, and 7.2 has made resolution forward-only.

---

## 8. Per-slice protocol (agents: follow verbatim)

1. Branch off current `main`, `refactor/<slice-id>-<short-name>` (or `test/`
   for Phase-0 slices).
2. Before editing: re-verify every line number you rely on from this doc.
3. The change itself: one slice only. No drive-by fixes — file issues instead.
4. Gate: `cargo fmt --all && cargo clippy --all-targets && cargo test`.
5. Golden discipline: goldens + corpus sweep **byte-identical**, or (for an
   intentional change) regenerate with `UPDATE_GOLDEN=1`, include the diff in
   the PR description, and justify each hunk.
6. If the slice touches render behavior for a schema variation, also run the
   relevant `pytest tests/integration/<files>` against live CH
   (`docker-compose.test.yaml` + `scripts/test/setup_social_integration_data.sh`).
7. Subagent review on the branch (reviewers use `isolation: worktree`); merge
   per the standard review/merge process.
8. Update this doc's checklist (§9) and any affected AGENTS.md in the same PR.

## 9. Slice checklist / status

Guardrails (anytime, first): ☑ G.1 ratchet test + baselines
(`tests/rust/ratchet/`, 2026-07-06) · ☐ G.2 CLAUDE.md/AGENTS.md
dispatch-rule checklist

Phase 0: ☐ P0.1 golden schema-dimension + FK-edge · ☐ P0.2 denormalized ·
☐ P0.3 polymorphic · ☐ P0.4 composite-id · ☐ P0.5 Browser-shaped patterns ·
☐ P0.6 corpus sweep · ☐ P0.7 CI push+smoke · ☐ P0.8 nightly

Phase 1: ☐ P1.1 `children()`/`walk()` + mod.rs walkers · ☐ P1.2 five WITH fns
characterize+unify (D4, D5) · ☐ P1.3 `transform_up` + first 3 passes ·
☐ P1.4+ remaining passes/walkers (batch per file) · ☐ P1.x ExprVisitor
adoption slices

Phase 2: ☐ P2.1 vlp_rewrite move (+D3) · ☐ P2.2 pattern_comprehension_sql
move (+D7) · ☐ P2.3 clause_extractors move · ☐ P2.4 plan_predicates move ·
☐ P2.5 cte_rewrite move (+D2) · ☐ P2.6 with_to_cte move · ☐ P2.7 D1 ·
☐ P2.8 D6 · ☐ P2.9 D8 · ☐ P2.10 import hygiene + dead_code removal

Phase 3: ☐ P3.1–P3.5 per §6.2 (transition-assert PRs then switch PRs) ·
☐ P3.6 legacy-path deletion

Phase 4: ☐ P4.1..n hoists · ☐ P4.x §10 phases 1–3 · ☐ P4.final #411

## 10. Risks / what NOT to do

- **Don't** mix SQL-changing and structure-changing edits in one slice.
- **Don't** big-bang the four-render-path unification (the SQL-IR Phase-2
  investigation already established A/C context-struct unification is not
  cleanly safe; that remains deferred).
- **Don't** "fix" the five-function drift silently — §4.2 step 2 decides
  semantics first; a silent fix is a behavior change wearing a refactor's
  clothes.
- **Don't** delete an inline pattern derivation without a transition-assert
  proving it agrees with the canonical API on the whole corpus.
- **Don't** re-attempt #411 before Phase 4.
- Deep recursion: plan trees can already overflow default stacks (128 MB
  worker stacks configured). `walk()` should be written iteratively (explicit
  stack) or at minimum not increase frame size — this is also the documented
  long-term fix for AGENTS.md §7.
- serde_json `preserve_order`: column order is semantics — byte-identical SQL
  implies identical column order, which is why the invariant is bytes, not
  AST-equivalence.

---

*Sources: 2026-07-06 audit (four parallel deep-dives: safety net,
plan_builder_utils anatomy, LogicalPlan traversal fan-out, schema-pattern
dispersion), `render_plan/AGENTS.md`, `docs/design/SQL_IR_DESIGN.md`, git
fix-commit history since 2026-01.*
