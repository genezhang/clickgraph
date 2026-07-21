# Forward resolution through CTE scope — concrete implementation plan (P-4)

Status: **plan for review** (no production code in this PR). Grounded in the
source as of `ba8106c8` (2026-07-20). This is the PR-sized backlog for
`PRIORITIES.md` **P-4** and the concrete realization of
`REFACTORING_SAFETY_PLAN.md` §7.2 + `render_plan/AGENTS.md` §10.

> **For agents picking up a slice**: read §1 (current state — it corrects a
> stale premise in §10) and §5 (the recommended first slice) first. Every slice
> is one PR with the standard gate: `cargo fmt --all && cargo clippy
> --all-targets && cargo test`, corpus sweep **byte-identical or justified**,
> worktree-isolated review, per `REFACTORING_SAFETY_PLAN.md` §8. Re-verify every
> line number before editing.

---

## 0. TL;DR

The §10 design says "delete `reverse_mapping` (~88 usages)". **That field no
longer exists** — it was removed in the Feb-2026 scope redesign (`6137e1b9`,
#115), along with the ~180-line `intermediate_reverse_mapping` block (a comment
stub remains at `plan_builder_utils.rs:10049`). The debt did not go away; it
**forked into three overlapping mechanisms**, only one of which actually
resolves WITH-CTE properties in production. The architectural fix is unchanged
in spirit — *resolve `(cypher_alias, cypher_property)` → CTE column forward via
`property_mapping`* — but the concrete work is:

1. **Make the forward path live (#592).** `VariableRegistry::define_node/
   define_scalar/define_relationship*/define_collection` **drop the caller's
   `property_mapping`** and rebuild the `VariableSource::Cte` with a fresh empty
   map. The only patch-in (`set_property_mapping`) has **zero callers**. So
   `resolve_with_current_registry` — consulted *first* at the render site — can
   never return `CteColumn` for a WITH-CTE variable, and every such access
   silently falls through to the legacy reparse path. This is the systemic root.

2. **Retire the two legacy mechanisms** once the forward path is authoritative:
   the task-local `cte_property_mappings` column-name reparse
   (`get_cte_property_from_context`) and the `rewrite_render_plan_with_scope`
   in-place rewrite — plus the "also add DB column" reverse hack in
   `build_property_mapping_from_columns` and the `translate_db_columns_to_
   cypher_properties` reverse walker.

3. **De-opaque the three baked-SQL expression types** (`Raw` NOT-EXISTS,
   `ExistsSubquery`, `PatternCount`) so forward rewriting can reach the variable
   references inside them.

The switch is de-risked by a **transition-assert**: populate the forward mapping
and `debug_assert_eq!` its answer against the legacy answer across the whole
corpus *before* making forward authoritative. Expected corpus delta at each
structural slice: **zero**; the intentional diffs (the actual #592/#595/#602/
#613/#643 fixes) land in exactly one reviewed slice each.

---

## 1. Current-state map (verified against source)

### 1.1 There is no `reverse_mapping` field anymore — there are three forward-ish mechanisms

`grep -rn reverse_mapping src/` returns **2 code hits, both comments** referring
to already-deleted code (`plan_builder.rs:5782`, `plan_builder_utils.rs:10049`).
The design-doc figure of "88 usages of `reverse_mapping` in
`plan_builder_utils.rs`" is **stale** — do not size the work from it.

What exists today, resolving `alias.property` for a variable that crossed a WITH
barrier, in the order the render site consults them
(`sql_generator/emitters/clickhouse/to_sql_query.rs`, `PropertyAccessExp`
rendering, ~6914–6951):

| # | Mechanism | Data source | Populated at | Read at | Status |
|---|---|---|---|---|---|
| **M1 (forward, §11)** | `VariableRegistry` → `VariableSource::Cte { cte_name, property_mapping }` → `resolve_with_current_registry` → `registry.resolve` | `property_mapping: HashMap<cypher_prop, cte_col>` | **meant** to be `publish_alias` (`plan_builder_utils.rs:6780`) + `plan_ctx` defines | `to_sql_query.rs:6918` (consulted **first**) | **DEAD for WITH-CTE vars (#592)** — see §1.2 |
| **M2 (legacy reparse)** | task-local `cte_property_mappings` → `get_cte_property_from_context` | reparsed **CTE column names** (`parse_cte_column`) into `from_alias → {prop → col}` | `set_cte_property_mappings` at `plan_builder_utils.rs:5846`, `cte_extraction.rs:6095` | `to_sql_query.rs:6943` (**fallback — the one that actually fires**) | live, load-bearing |
| **M3 (scope rewrite)** | `VariableScope` / `CteVariableInfo { property_mapping }` → `rewrite_render_plan_with_scope` | `property_mapping` (correctly populated at `publish_alias:6773`) | `publish_alias` | in-place render-plan rewrite, `plan_builder.rs:5784` | live, one path only |

So M1 is the intended forward path; M3 already carries the correct forward map
but only rewrites on one code path; **M2 is what keeps production working** by
reverse-engineering the mapping back out of the generated column names. This is
the "premature-resolution-then-undo" cycle §10 describes, just relocated.

### 1.2 #592 — exactly where `property_mapping` is dropped

`src/query_planner/typed_variable.rs`. Every `define_*` that accepts a
`VariableSource` **pattern-matches `VariableSource::Cte { cte_name, .. }`,
discarding `property_mapping`**, then rebuilds via a `*_from_cte` constructor
that hardcodes an empty map:

- `define_node` (`:710`) → `TypedVariable::node_from_cte(labels, cte_name)`
  (`:445`) → `NodeVariable::from_cte` (`:141`) → `property_mapping:
  Box::new(HashMap::new())` (`:146`).
- `define_relationship` (`:739`) / `define_relationship_with_direction` (`:766`)
  → `rel_from_cte` (`:463`) → `RelVariable::from_cte` (`:210`) → empty map
  (`:220`).
- `define_scalar` (`:794`) → `scalar_from_cte` (`:478`) → empty map (`:262`).
- `define_collection` (`:841`) → `collection_from_cte` → empty map.

The **only** way to fill it afterward is `set_property_mapping` (`:1067`), which
has **zero callers** (`grep -rn '\.set_property_mapping(' src/` → none). The
render-side call site documents this defect in a long comment
(`plan_builder_utils.rs:11208–11226`) and even routes EXISTS-correlation
resolution around it via a *fourth*, purpose-built task-local channel
(`set_cte_scope_for_correlation`, `query_context.rs:890`).

Net effect: `registry.resolve` for a `Cte`-sourced variable hits the
`property_mapping.is_empty()` arm (`typed_variable.rs:1003`) → `Unresolved` →
the render site falls through M1 to M2. **The forward path is architecturally
present and wired end-to-end but starved of data at the one function that owns
the data.**

Note: `plan_ctx` also builds `VariableSource::Cte { property_mapping:
HashMap::new() }` during *planning* (`plan_ctx/mod.rs:982/990/1005/1014`) — that
emptiness is legitimate (columns aren't known yet). The load-bearing drop is at
the **render** site (`publish_alias`), where the map *is* known
(`per_alias_mapping`) and is even passed in — then discarded by `define_node`.

### 1.3 The "also add DB column" reverse fallbacks

- `build_property_mapping_from_columns` (`plan_builder_utils.rs:313`, 4 callers:
  `:7785, :7906, :8056, :10996`) builds `(alias, property) → cte_col` from
  select-item **column aliases**, and in three of its four pattern arms *also*
  inserts a second key using the **DB column** pulled from the item's
  `PropertyAccessExp` (`:335–349, :361–375, :389–403`, guarded `expr_col !=
  property`). These six inserts are the "also add DB column mapping" hack §10
  calls to delete — they exist precisely because expressions are resolved to DB
  columns *before* the CTE scope is applied, so the map must accept both keys.
- `translate_db_columns_to_cypher_properties` (`:577`, driver + ~18 recursive
  arms `:600–655`, plus a call at `:8412`) is a genuine reverse walker: it
  rewrites `PropertyAccessExp` DB column names *back* to Cypher property names
  before `build_property_mapping_and_context` runs.
- `plan_builder.rs:4488` builds a `db_column → (target_alias, cypher_property)`
  reverse map (the #590 fix); `plan_builder_helpers.rs:5705` documents it.

### 1.4 The three opaque-string bake sites

`src/render_plan/render_expr.rs`, inside `TryFrom<LogicalExpr> for RenderExpr`:

| Expr type | Bake site | Generator | Struct |
|---|---|---|---|
| `RenderExpr::Raw(sql)` (NOT-EXISTS) | `:1629–1630` | `generate_not_exists_from_path_pattern` (`:978`) | — (bare `String`) |
| `RenderExpr::ExistsSubquery` | `:1652–1659` | `generate_exists_sql` (`:123`) | `ExistsSubquery { sql, correlated_aliases }` (`:1385`) |
| `RenderExpr::PatternCount` | `:1693–1704` | `generate_pattern_count_sql` (`:812`) | `PatternCount { sql, correlated_aliases }` (`:1354`) |

All three run during `TryFrom` — **before any WITH scope processing**. Both
structs already carry a structural `correlated_aliases: Vec<String>` (added for
#614/#596) — a partial retreat from opacity, but the `sql` field is still
pre-baked. Every expression rewriter has an explicit no-op arm for these three:
`plan_builder_utils.rs:12795–12801` (`ExistsSubquery(_) => {}`, `PatternCount(_)
=> {}`, `Raw(_) => {}`), mirrored in `references_alias`, `expression_utils.rs`
(`:298–300, :416–423`), and `remap_cte_names_in_expr` (`:2556`). This is the
`other => other.clone()` skip §10 warns about, made explicit.

### 1.5 Infrastructure already in place (build on it)

- **`walk()` / `children()` / `any_node()` / `find_map_node()` / `map_children()`**
  on `LogicalPlan` — exhaustive, no catch-all — merged as P1.2 (#650,
  `logical_plan/mod.rs:2115–2242`). CLAUDE.md rule 5 now reads "walk() is
  exhaustive; barriers are explicit". Rewriters built during this plan use it,
  so a new `LogicalPlan` variant can no longer be silently skipped.
- **`ExprVisitor::transform_expr`** (`expression_utils.rs:62`) — the RenderExpr
  visitor to adopt when de-opaquing (Phase C), instead of hand-rolling recursion.
- **`ResolvedProperty`** enum (`CteColumn { sql_alias, column }` / `DbColumn` /
  `Unresolved`) — the single resolution result type M1 already returns.
- **Corpus sweep** (`tests/rust/integration/corpus_sweep.rs`, 1,188 queries ×
  2 dialects, `UPDATE_GOLDEN=1` regen) + per-schema goldens
  (`sql_golden_tests.rs`) — the Phase-0 net that makes "byte-identical" provable.
- **Ratchet test** (`tests/rust/ratchet/`) — guards against new raw-flag
  branching; unaffected by this work but must stay green.

---

## 2. The forward path — target design

Per §10, resolution must go **straight** from Cypher space to CTE column, never
DB-column-first-then-undo. Concretely:

### 2.1 `VariableRegistry` (M1) becomes the single authority

`registry.resolve(alias, cypher_property, schema)` already does the right thing
*if* `property_mapping` is populated:

- `Cte { property_mapping }` present → `CteColumn { sql_alias:
  extract_from_alias_from_cte_name(cte_name), column }` (`typed_variable.rs:990`).
- `Match` → schema lookup → `DbColumn` (`:1023`).

The **only** change M1 needs to become live is: `define_*` must **thread the
caller's `property_mapping` through** instead of dropping it (§1.2). This is a
data-plumbing fix, not a new mechanism.

### 2.2 What each surviving mechanism becomes

- **M1**: authoritative. `resolve_with_current_registry` returns `CteColumn`;
  the render site uses it directly.
- **M2 (`cte_property_mappings`)**: **deleted** — its populator
  (`set_cte_property_mappings` + the column-reparse in
  `build_property_mapping_and_context`), its reader
  (`get_cte_property_from_context`), and the task-local field. Reverse
  column-name parsing (`parse_cte_column`) is no longer needed for *resolution*
  (it may survive for other uses — verify per call site).
- **M3 (`rewrite_render_plan_with_scope`)**: reconciled. `CteVariableInfo`
  already carries the correct forward `property_mapping`; either fold M3 into M1
  (preferred — one registry) or keep M3 as the render-plan-rewrite *application*
  of M1's data. Decide during Phase B with a transition-assert that M1 and M3
  agree.
- **`build_property_mapping_from_columns` "also add DB column" arms**: deleted
  once M1 is authoritative — nothing resolves via DB-column keys anymore.
- **`translate_db_columns_to_cypher_properties`**: deleted once expressions are
  no longer resolved to DB columns before scope (the root the reverse walker
  compensates for).

### 2.3 Opaque strings carry structured sub-trees

`ExistsSubquery` / `PatternCount` / NOT-EXISTS `Raw` change from
`{ sql: String }` to carrying `RenderExpr` sub-expressions (pattern + filter),
rendered to SQL in `to_sql_query.rs` where the current scope's variable sources
are known. `TryFrom` stops calling `generate_*_sql`; a `to_sql()` at the end
does. This lets the standard rewriters reach the variable references (they walk
`RenderExpr` children instead of hitting the `=> {}` skip). `correlated_aliases`
becomes derivable from the sub-tree rather than scanned from opaque text.

### 2.4 Reconciliation with CLAUDE.md rules 2 and 5

- **Rule 2 (forward through scope, never reverse)**: this plan *is* rule 2 made
  real. After the switch, there is no DB-column→CTE-column reverse anywhere in
  resolution; `(cypher_alias, cypher_property) → cte_col` is a direct forward
  lookup in `property_mapping`.
- **Rule 5 (walk() is exhaustive; barriers explicit)**: the de-opaque rewriters
  (Phase C) use `ExprVisitor` / `walk()`, so `ExistsSubquery`/`PatternCount`/
  `Raw`-carried sub-trees are traversed by construction — the historical "skip
  the variant" bug cannot recur.

---

## 3. Phasing — PR-sized, individually shippable slices

Ordered. Each slice: one PR, standard gate, corpus **byte-identical unless
marked**. The three §10 phases map to slice groups **F0–F1** (forward path),
**F2** (retire legacy), **F3–F5** (de-opaque), **F6** (final deletion).

### Phase A — make the forward path live (fixes #592, the systemic root)

**F0 — populate `property_mapping` + transition-assert (byte-identical).**
*The recommended first slice — full spec in §5.*
- Thread `property_mapping` through `define_node/scalar/relationship*/
  collection` and the `*_from_cte` constructors (`typed_variable.rs`).
- At the render site (`to_sql_query.rs:6918`), when M1 now returns `CteColumn`,
  **also** compute the M2 answer and `debug_assert_eq!` them — but **keep
  emitting M2's result** (fall through exactly as today) so output is unchanged.
- Acceptance: corpus sweep **byte-identical**; `cargo test` (debug asserts on)
  shows zero divergence panics; new unit test locks M1 returning `CteColumn`.
- Fixes: nothing user-visible yet — it *proves* M1 and M2 agree everywhere they
  both answer, de-risking F1.

> **⚠️ F0 outcome (2026-07-20, verified in review) — READ BEFORE F1.** F0 landed
> the threading + a *working* transition-assert, but it was **relocated** from
> the `to_sql_query.rs:6918` render site to `publish_alias`
> (`plan_builder_utils.rs`), because that render-site M1 arm is effectively
> dead: expressions reaching `to_sql` are **already** rewritten to CTE column
> names by the live M3 resolver (`variable_scope::VariableScope::resolve` /
> `rewrite_render_expr`), so M1's Cypher-property-keyed map never matches there
> (0 hits in corpus/lib; the ~37 integration-test hits are already-M3-rewritten
> columns that fall through to M2 identically). **Consequence for F1: the
> authoritative switch must target the M3 rewrite path, NOT the `to_sql_query.rs`
> render site the bullets below name.** F0 found zero forward-vs-legacy
> disagreements, so F1's data is trustworthy — but retarget it to M3 first.

**F1 — make M1 authoritative (the intentional-diff slice).**
- ~~At the render site, when M1 returns `CteColumn`, **return it**~~ **(see F0
  outcome above — retarget to the M3 rewrite path in `variable_scope`)** and
  stop consulting M2. Remove the transition-assert.
- Acceptance: corpus sweep diff is **exactly** the set of cases where M2 was
  wrong/absent and M1 is right — each hunk reviewed and justified in the PR;
  regenerate goldens. Live-verify the changed queries against CH.
- Fixes: **#592** (forward path now resolves), and whichever of **#595 / #602 /
  #613 / #643** resolve purely through correct forward `property_mapping` of a
  *`PropertyAccessExp`* (not the opaque-string types — those wait for F3–F5).
  Triage which is which during F0 by inspecting the debug-assert divergences and
  the M2-returns-`None` fall-throughs.

> **✅ F1 outcome (2026-07-20, landed) — READ BEFORE F2.** The three bullets
> above are superseded by what F1 actually did, once the render site was
> measured directly (F0 had only inferred it):
>
> 1. **The render-site M1 arm was NOT dead — for scalars.** F0 concluded "M1
>    fires 0× at the render site" from the *node* corpus. A direct probe showed
>    the load-bearing render-site path is the **legacy M2 fallback**
>    (`get_cte_property_from_context`, `to_sql_query.rs:~6995`), which fires 210×.
>    **Every one of those 210 is a scalar / composite CTE FROM-alias reference**
>    (`id.id`, `e_id_n.id` from `WITH u.user_id AS id`). Deleting M2's `return`
>    without a replacement regresses exactly these: a CTE column literally named
>    `id` then falls into the **id-pseudo-property block** (`to_sql_query.rs`,
>    `col_name == "id"`) and is schema-mapped to a node_id column —
>    *alphabetically* `post_id` on a multi-label schema (a #616-class wrong-label
>    bug). `n`/`e` fall through fine; only `.id` corrupts.
>
> 2. **Root cause: scalars carry an EMPTY `property_mapping`.** M1 (registry) and
>    M3 (`VariableScope`) both return `Unresolved` for an empty map, so *neither*
>    forward mechanism resolved a scalar export — M2 was the only one that did
>    (it supplies an identity entry, keyed by FROM alias). F0's transition-assert
>    never caught this because it loops over `info.property_mapping` (empty for
>    scalars → zero iterations). "M1 == M3 everywhere" was true but **silent on
>    the one class that mattered**.
>
> 3. **The fix (byte-identical "scalars-first"):** give the **registry** (M1) an
>    identity self-map (`id → id`) for every empty-mapping export, in
>    `publish_alias` (single) and `publish_composite` (multi). Kept OUT of
>    `scope_cte_variables` (M3) on purpose: M3 keys its scalar-vs-node *expansion*
>    on `property_mapping.is_empty()`, so a non-empty scope map would wrongly
>    expand the scalar as a node. The registry is a separate render-only channel.
>    Then made the render-site M1 arm authoritative (`return`) and **deleted M2's
>    render-site `return`** + its local wrapper `get_cte_property_from_context`.
>
> 4. **#593 guard (mandatory, TWO conditions).** M1's registry is keyed by
>    **Cypher alias** and is **global** — it is NOT scoped per Cypher-UNION arm
>    the way M3 is (`scoped_to_referenced_ctes`). An unguarded authoritative M1
>    leaked a WITH arm's CTE column into a sibling plain arm reusing the same
>    alias. The guard needs BOTH:
>    - **(a) `sql_alias == table_alias.0`** — reproduces M2's FROM-alias keying
>      (M2 always re-qualified with `table_alias.0`, never rewriting the alias);
>      falls through for a *composite* cross-arm alias whose CTE FROM alias
>      differs (`c_u` ≠ `u`).
>    - **(b) `table_alias.0` is NOT a base table in the current branch**
>      (`alias_is_base_table_in_branch`, consulting ONLY the branch-local
>      `alias_label_map`, not the global registry). Condition (a) alone is
>      **insufficient** and F1's first cut shipped without (b) — the adversarial
>      review caught it, and a differential vs the F0 parent **confirmed a real
>      regression**: for a SINGLE-alias WITH arm (`MATCH (u:User) WITH u WHERE …
>      RETURN u.user_id`), the CTE FROM alias is literally `u`, so a plain sibling
>      arm's own `u.user_id` (raw `users_bench` scan) passed (a) and leaked the
>      WITH arm's `p1_u_user_id` (Code 47). The corpus missed it — the shape isn't
>      in it. (b) distinguishes the plain arm's base-table `u` (present in
>      `alias_label_map`) from a genuine CTE FROM alias (`id`, `with_*_cte_*`,
>      absent). This is the arm-locality M2 had implicitly (its map was
>      repopulated per CTE-body scope) and M3 has explicitly.
>
> **Net:** corpus + all goldens **byte-identical**; render-site M2 return + its
> wrapper deleted; #592 forward path now resolves scalars. The **intentional
> user-visible diffs** the bullets promised (#595/#602/#613/#643) did **NOT**
> land here — they are not render-site-M2 cases; they live on the M3 path or in
> the opaque-string types (Phase C). F1 is therefore a **byte-identical
> consolidation**, not the intentional-diff slice; the diff slice is re-scoped to
> a follow-up on the M3 path (tracked as **F1b**, below). The underlying M2
> accessor (`get_cte_property_mapping`) still has other consumers
> (`cte_column_resolver.rs`, `select_builder.rs`) — those are F2a's teardown.
>
> **Lesson for F2b (full per-arm registry scoping):** the global Cypher-alias-
> keyed registry is fundamentally arm-unsafe when made authoritative; the
> two-condition guard is a *containment*, not the real fix. F2b must give the
> render-site registry the same per-arm scoping M3 has
> (`scoped_to_referenced_ctes`) before removing the (b) base-table guard.
>
> **F1b — the real intentional-diff slice (follow-up, not yet done).** Hunt where
> the live **M3** path falls through to a loud error / wrong column for
> #602/#613/#643 and fix on the forward map there. Higher-value, higher-risk;
> each fix a reviewed hunk, not byte-identical. Was folded into "F1" in the
> original plan; split out because F1-as-landed is provably byte-identical.
>
> **F1b outcome (2026-07-21, #602 landed).** First F1b hunk delivered: **#602**
> (post-WITH MATCH continuation joined on the wrong column). Root cause was on the
> M3 path exactly as predicted — the continuation join's `.id` operand is resolved
> by `VariableScope::resolve_generic_id_in_cte`, which needs the node's label in
> `CteVariableInfo.labels`. Across a *second* (passthrough) WITH barrier the label
> was re-derived from the post-barrier plan (source `GraphNode` gone → label
> stripped) and arrived EMPTY, so `u.id` stayed unresolved and the SQL-gen generic
> `.id` fallback alphabetically mis-picked `Post.post_id` (#616 class, Code 47).
> Fix: a persistent `carried_labels` map on `WithBarrierScope` (survives `reset()`)
> that threads the node label forward across every barrier the alias crosses —
> forward data, mirroring `property_mapping`. Gated on a non-empty
> `per_alias_mapping` so a scalar rebind (`WITH u.email AS u`) does not inherit a
> stale node label. The id column is *produced* in both CTEs and merely *pruned*
> when unreferenced, so resolving the operand lets the existing `prune_cte_columns`
> pass retain it — no projection injection. Corpus byte-identical (the fixed shape
> isn't in it); cross-schema standard/denorm/polymorphic all corrected (denorm/poly
> were silent-wrong on main). **Remaining F1b residue: #613** (blocked by CH Code 48
> correlated-subquery limit → really an **F3** PatternCount de-opaque), **#643**
> (chained-VLP endpoint needs planner-topology rework, its own design cycle), **#595**
> (EXISTS → **F4**). #602 composite-id continuation stays loud (single-column-only
> id resolver) — separate follow-up.


### Phase B — retire the legacy resolution machinery (byte-identical)

**F2a — delete M2 (`cte_property_mappings`).** Remove `set_cte_property_mappings`
callers (`plan_builder_utils.rs:5846`, `cte_extraction.rs:6095`) and the
task-local field (`query_context.rs`). The render-site reader wrapper
`get_cte_property_from_context` (`to_sql_query.rs:41`) was **already deleted in
F1** (its only caller was the render-site fallback F1 retired); the underlying
accessor `get_cte_property_mapping` still has two non-render-site consumers
(`cte_column_resolver.rs:132`, `select_builder.rs:2446`) plus `get_all_cte_
properties` (`select_builder.rs:873`) — F2a must retire those too or scope them
out explicitly. Corpus byte-identical (F1 already made M1 authoritative at the
render site).

**F2b — reconcile/fold M3.** Transition-assert M1 vs M3
(`rewrite_render_plan_with_scope`) agree, then either delete M3 or reduce it to
"apply M1's data". Corpus byte-identical.

**F2c — delete the "also add DB column" arms** in
`build_property_mapping_from_columns` (the 6 secondary inserts) and, if now
unreferenced, `translate_db_columns_to_cypher_properties`. Corpus byte-identical
(nothing resolves via DB-column keys post-F1). If a residual reader remains,
transition-assert first.

### Phase C — de-opaque the three baked-SQL types (§10 Phase 2)

Each is independent and byte-identical (same SQL, built later from structure):

**F3 — `PatternCount` structured.** Carry the `PathPattern` + filter as
`RenderExpr` (not `sql: String`); move `generate_pattern_count_sql` to a
`to_sql()` at render. Rewriters recurse into it. Corpus byte-identical.
Fixes the size()-pattern member of the residue (candidate **#613**).

**F4 — `ExistsSubquery` structured.** Same treatment via `generate_exists_sql`.
Candidate **#595**.

**F5 — NOT-EXISTS `Raw` → structured `NotExists` variant.** Replace the
`RenderExpr::Raw(not_exists_sql)` at `:1630` with a structured type carrying the
pattern; render at `to_sql`. Corpus byte-identical.

### Phase D — final cleanup

**F6 — delete residual reverse scaffolding + `set_property_mapping`** (now the
patch-in is unnecessary because `define_*` carries data directly), the
`set_cte_scope_for_correlation` fourth-channel if F4 subsumes it, and the stale
`reverse_mapping`/`intermediate_reverse_mapping` comment stubs. Update
`render_plan/AGENTS.md` §10/§11 to describe the single forward mechanism, and
`PRIORITIES.md` §2/§4. Corpus byte-identical.

### Issue → slice map

| Issue | Fixing slice | Why |
|---|---|---|
| **#592** (registry drops `property_mapping`) | **F0 + F1** | root; F0 plumbs data, F1 switches authority (incl. the scalar/composite identity-map gap F0 missed) |
| #595, #602 | **F1b** if `PropertyAccessExp`-only; else F4 (EXISTS) | NOT render-site-M2 cases (F1 is byte-identical); on the M3 path or opaque strings |
| #613 | **F1b** or **F3** (size/PatternCount) | opaque size() pattern needs structure |
| #643 (chained VLP endpoint alias) | **F1b** (forward map per-endpoint) | see §4 VLP interaction |
| #583 render rework | after Phase C | depends on structured EXISTS/NOT-EXISTS |

---

## 4. Risk, sequencing, blast radius

- **Blast radius = the full WITH surface** (as §7.2 warns). Every WITH→CTE query
  routes through M1/M2. This is exactly why the sequencing is *populate →
  assert → switch*, never a direct rewrite. The **Phase-0 corpus sweep (1,188 ×
  2) + per-schema goldens** are the guard: F0/F2/F3/F4/F5 must be byte-identical;
  only F1 carries a reviewed diff.
- **The transition-assert is mandatory before F1.** F0 computes M1 and M2 and
  `debug_assert_eq!`s them across the whole corpus (debug asserts fire under
  `cargo test`). Only after that sweep is clean do we flip authority. This turns
  "does forward equal legacy everywhere?" from a hope into a test result, and
  the *remaining* divergences (M2 wrong or absent) become F1's reviewed fix list.
- **What must land first / cannot be split:** F0 before F1 (data before switch);
  F1 before F2 (can't delete M2 while it's authoritative); Phase C (F3–F5) is
  independent of A/B and can proceed in parallel, but the opaque-string members
  of the issue residue (#595 EXISTS, #613 size) only *fully* fix after their
  Phase-C slice **and** F1. F6 last.
- **`define_*` signature change (F0) is workspace-wide.** `define_node` etc. are
  called from `plan_ctx/mod.rs` (many sites), `plan_builder_utils.rs`,
  `unwind_clause.rs`, `match_clause/*`. Adding/plumbing a `property_mapping`
  argument touches all of them — but mechanically (planning-phase callers pass
  the empty map they already construct; only `publish_alias` passes a real one).
  Keep it one commit; the compiler enforces completeness.
- **Interaction with in-flight P3 (P2.1 module move).** P3 moves
  `plan_builder_utils.rs` functions into new modules (`vlp_rewrite.rs`,
  `with_to_cte/`, etc.) with `pub(crate)` re-exports, **no logic edits**. F0–F2
  edit `publish_alias`, `build_property_mapping_from_columns`,
  `set_cte_property_mappings` — all in `plan_builder_utils.rs`. **Coordinate
  ordering**: land P4 slices that touch a function *before* P3 moves it, or
  rebase across the move (the re-exports keep call sites compiling). Reserve the
  file and announce on the fleet board. typed_variable.rs (F0's main file) is
  **not** in P3's scope — low collision there.
- **Interaction with P-1 VLP fixes.** #643 (chained VLP endpoint alias) is
  currently loud (Code 47) and in the reverse-mapping class (§1.6 — do **not**
  per-shape patch). Its real fix is F1's per-endpoint forward `property_mapping`.
  VLP CTEs also feed M1 via dotted-column mappings
  (`build_property_mapping_from_columns` Pattern 1); the F0 transition-assert
  must include VLP corpus entries. If a P-1 slice (#647-adjacent) changes VLP
  render between F0 and F1, re-run the F0 assert.
- **Nondeterminism carve-out.** 19 corpus entries flap on HashMap seed
  (`tests/corpus/nondeterministic.txt`); they are excluded from the byte-lock.
  Do not "fix" a diff that is actually a seed flap — check that file first.

---

## 5. Recommended first slice — F0 (precise spec)

**Goal**: populate the forward `property_mapping` end-to-end and prove it equals
the legacy answer across the corpus, **without changing any generated SQL**.

**Branch**: `refactor/f0-forward-mapping-populate-assert` (per §8 protocol).

**Files & functions:**

1. `src/query_planner/typed_variable.rs`
   - `NodeVariable::from_cte` (`:141`), `RelVariable::from_cte` (`:210`),
     `ScalarVariable::from_cte` (`:258`), `CollectionVariable::from_cte`: add a
     `property_mapping: HashMap<String, String>` parameter; store it instead of
     `HashMap::new()`.
   - `TypedVariable::node_from_cte` (`:445`), `rel_from_cte` (`:463`),
     `scalar_from_cte` (`:478`), `collection_from_cte`: forward the new arg.
   - `VariableRegistry::define_node` (`:710`), `define_relationship` (`:739`),
     `define_relationship_with_direction` (`:766`), `define_scalar` (`:794`),
     `define_collection` (`:841`): stop destructuring `Cte { cte_name, .. }`;
     bind `property_mapping` and pass it into the `*_from_cte` constructor.
   - Leave `set_property_mapping` (`:1067`) in place for now (deleted in F6).

2. Call sites of the above `define_*` (compiler will list them): pass the
   `property_mapping` they already hold.
   - `plan_builder_utils.rs:6790/6795` (`publish_alias`) — pass
     `per_alias_mapping.clone()` (the real map — this is the #592 fix point).
   - `plan_ctx/mod.rs` (`:982, :990, :1005, :1014`, and the `define_*` wrappers
     `:248/:264/:280/:1370/:1390/:1420/:1440/:1464/:1476/:1502/:1529`) — pass
     the empty map they already build (planning phase, columns unknown —
     legitimately empty).
   - `unwind_clause.rs`, `match_clause/*` — Unwind/Match sources are unaffected
     (empty/n-a).

3. `src/sql_generator/emitters/clickhouse/to_sql_query.rs` (~`:6918`) — the
   transition-assert. Where M1 currently returns and would short-circuit:
   ```
   if let Some(ResolvedProperty::CteColumn { sql_alias, column }) = m1 {
       // TRANSITION-ASSERT (F0 only): forward must match legacy everywhere
       // both answer. Keep emitting legacy so output is byte-identical.
       if let Some(legacy) = get_cte_property_from_context(&table_alias.0, col_name) {
           debug_assert_eq!(
               format!("{sql_alias}.{column}"),
               format!("{}.{}", table_alias.0, legacy),
               "F0: forward/legacy divergence for {}.{}", table_alias.0, col_name
           );
       }
       // Do NOT return here in F0 — fall through to the legacy branch below.
   }
   ```
   (In F1 this becomes an unconditional `return format!("{sql_alias}.{column}")`
   and the legacy branch is deleted.)

**Acceptance tests:**
- Corpus sweep **byte-identical**: `cargo test -p clickgraph --test integration
  corpus_sweep` passes with no golden change (no `UPDATE_GOLDEN`).
- Full `cargo test` (debug assertions on) — the F0 `debug_assert_eq!` fires zero
  times. If it fires, that alias/property is a genuine M1/M2 divergence: record
  it as an F1 fix candidate (do **not** silence the assert).
- New unit test in `typed_variable.rs`: `define_node("a", vec!["User"],
  Cte { cte_name, property_mapping: {"name" → "p1_a_name"} })` then
  `registry.resolve("a", "name", schema)` returns `CteColumn { column:
  "p1_a_name", .. }` (today it returns `Unresolved`).

**Expected corpus delta: zero.** F0 only adds data + an assert; the emitted SQL
still comes from M2.

**Reviewer focus:** confirm F0 does not early-return on the M1 branch (that would
be F1 smuggled in), and that every planning-phase `define_*` caller passes an
empty map (non-empty there would be wrong — columns aren't assigned yet).

---

## 6. Acceptance criteria for the whole workstream (P-4 exit)

- M1 (`VariableRegistry`) is the single resolution mechanism for CTE-scoped
  properties; M2 and the DB-column reverse fallbacks are deleted.
- `ExistsSubquery` / `PatternCount` / NOT-EXISTS carry structured `RenderExpr`,
  not pre-baked SQL; no expression rewriter has a `=> {}` skip for them.
- #592/#595/#602/#613/#643 closed (or the residue re-triaged with the forward
  path as the substrate).
- Corpus + goldens green; `render_plan/AGENTS.md` §10/§11, `REFACTORING_SAFETY_
  PLAN.md` §9, and `PRIORITIES.md` updated in the closing slice.
