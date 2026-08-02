# Work priorities & dispatch queue

Status: **canonical**. Last reconciled: 2026-07-27.

This is the single source of truth for **what to work on next** across all
workstreams. The design docs say *how* to do each slice
(`REFACTORING_SAFETY_PLAN.md`, `SQL_IR_DESIGN.md`, `DELTAGRAPH_PLAN.md`,
`render_plan/AGENTS.md` §10); this doc says *which one now, and why*.

> **For agents**: before starting work, read §1 (rules) and pick the
> highest-priority unblocked item in §2. When your PR merges, update this
> doc's §2/§4 in the same PR (or a same-day follow-up docs commit). If you
> believe the priority order is wrong, say so in your report — do not
> silently work on something else.

## Why this doc exists

During 2026-07-12..17, agents made real progress but drifted: opportunistic
slices landed across Phases 1–4 of the refactoring plan while its §9
checklist went stale, P1.2 (the plan's own "highest-value migration") was
skipped entirely, the nightly CI went red on ~2026-07-13 and stayed red
unnoticed, and 84 stale xfail markers accumulated. Divergence isn't caused by
bad work — it's caused by no shared, current answer to "what's next".

## 1. Standing rules (apply to every task)

1. **Ground rules are unchanged**: never change query semantics; no
   shortcuts; quality over speed (CLAUDE.md).
2. **Per-slice protocol**: `REFACTORING_SAFETY_PLAN.md` §8 verbatim —
   one slice per PR, byte-identical goldens + 1,082-query corpus sweep (or a
   justified regenerated diff), fmt/clippy/`cargo test` gate, worktree-
   isolated subagent review, standard merge process.
3. **Checklist discipline**: a merged slice updates BOTH this doc and the
   owning design doc's checklist in the same PR. A checklist that is wrong
   is worse than no checklist — the 07-12..17 drift started there.
4. **Nightly is load-bearing**: if the nightly workflow is red, fixing or
   triaging it outranks every P-level below except an active P-0. A red
   nightly that stays red equals no nightly, which silently un-buys the
   whole Phase-0 net. Every failure gets one of: a fix PR, a tracked issue
   + xfail with that issue number, or a revert.
5. **xfail hygiene**: an `xpass` is a bug in the net. When a fix makes a
   test xpass, the merging PR (or nightly triage) removes the stale marker.
6. **Bug-fix vs. refactor lanes**: newly-surfaced silent-wrong bugs are
   always in scope (ground rule 1). But bugs already root-caused to the
   reverse-mapping architecture (#592/#595/#602/#613/#643 class) get
   documented + loud-gated, NOT per-shape patched — the fix vein there is
   mined out, and each new patch adds surface that Phase 4 §7.2 must later
   migrate. When in doubt whether a bug is in that class, check
   `KNOWN_ISSUES`/memory notes or ask.
7. **Stats never change semantics** (for the P-5 workstream): backend
   statistics may influence join order, anchor choice, and traversal
   direction — never row membership (no pruning UNION arms / skipping
   tables based on stats). Stats-driven planning is **off by default** and
   off in sql_only/test paths so the golden net stays deterministic.

## 2. Priority queue

Ordered; work the highest unblocked item. "Owner: open" = unclaimed.

### P-0 — Nightly CI green + net hygiene  ☑ (GREEN — run `30213027096`, 2026-07-26)
The nightly was red from ~2026-07-13. Successive triage narrowed it to zero:
- **#687 (`9a7f9abe`)**: `test_disconnected_pattern` asserted `status == 'error'`
  under an obsolete "comma-separated patterns not supported" limitation. Cartesian
  support landed since — the query renders `JOIN … ON 1 = 1` + WHERE and returns
  the Alice×Bob row. Fixed to assert success + the exact row. +2 stale xfail markers
  cleared (`test_shortest_path_basic`, `test_labels_mixed_typed_untyped`).
- **#690 (`f60be99f`)**: round-2 triage of the next layer — (1) `test_unwind_with_match`
  generator compared an Int UNWIND id to a random String prop → CH String-vs-Int 500;
  fixed generator to `_get_id_prop`. (2) `test_external_users_with_access` 500s live
  (my over-eager #682 xfail removal — `cg validate` passed but live path is a genuine
  VLP-then-fixed-hop-polymorphic bug); restored xfail, filed **#689**. (3)
  `test_vlp_with_and_aggregation` xpassed from #620; marker removed.
- **Confirmed green**: `workflow_dispatch` run `30213027096` (head `f60be99f`) =
  `conclusion: success` — fmt/clippy `-D warnings`/build/full `cargo test`/full live
  pytest all pass. The scheduled weekday run now validates continuously.
Exit met: one fully green nightly run + xpass count 0.
- Follow-ups still open (net hygiene, not blocking): ~~prune stale
  `worktree-agent-*` branches~~ **DONE 2026-07-30** (none remained; pruned 4 stale
  remote-tracking refs); ~~refresh STATUS.md (last updated 2026-05-06)~~ **DONE
  2026-07-30 (#830)** — the "Active workstreams" block was stale (refactor mission
  complete 2026-07-28; SQL-IR added as its own workstream).
  ~~**#689** (live VLP-then-fixed-hop-polymorphic 500)~~ **bug 1 DONE 2026-07-30
  (#828, `c013c07a`)** — from-side-polymorphic VLP recursive arm joined the START
  table (`ds_users`) + base discriminator (`member_type='User'`); now joins the
  END table (`ds_groups`) + END label (`'Group'`), gated on the narrow
  `is_from_side_polymorphic_cross_type` (from-side label present, to-side absent,
  cross-table) so #142/complex-12/multi-type are untouched; 14 data_security
  goldens (2-line delta each) + live oracle (Group→Group fixture). Bug 2 (fixed
  hop to polymorphic target → triplicated identical UNION arms) split to **#827**.
- **#827 defect (a) DONE 2026-07-30 (#831, `494fbfdc`)** — polymorphic `$any`
  endpoint now honors `from_label_values`/`to_label_values` (new
  `GraphSchema::expand_node_type_constrained`, routed into the two
  `generate_union_for_untyped_nodes` sites). Untyped endpoint fans only to the
  declared labels, not every node label. 12 data_security goldens shed illegal
  arms (net −413; several encoded the bug). Root-cause correction: the arms were
  NOT from `infer_pattern_types` (`type_inference.rs:~2163`, banked-but-inert) —
  the live site is `generate_union_for_untyped_nodes` (~1469/~1492). Scoped to the
  one schema declaring endpoint `label_values`; open-polymorphic edges unchanged.
  Adversarial review APPROVE-0. Companion **both-endpoints-untyped over-fan DONE
  2026-07-30 (#833, `30fbc51b`)** — `MATCH (a)-[:HAS_ACCESS]->(b)` (both unlabeled)
  emitted the full 4×4=16 label cartesian, only 4 legal; the real generator was the
  `expand_node_type` cartesian in `traversal.rs::traverse_connected_pattern_with_mode`
  (both-untyped branch) — NOT the `infer_pattern_types` site `~2179` the #831 note
  guessed (inert; found by patch-and-rebuild). Routed through the same helper; now 4
  legal arms; locked by a revert-checked assertion test (fails on full OR either-side
  revert). Review APPROVE-0/1-minor. **#827 defect (b) — BOTH symptoms DONE
  2026-07-30.** Root-cause was NOT the banked render-side reverse-lookup
  (`get_node_schema_by_table` is a fallback never reached here) — both were
  analyzer-pass label-resolution bugs where the correct per-arm label was already
  present and unused. **Symptom 2 (#835, from-side per-arm discriminator/join-key):**
  `get_graph_context`/`compute_pattern_context` resolved the endpoint label from the
  shared PlanCtx (`get_label_str().first()` → same label every arm) instead of the
  arm's own `GraphNode.label`; the `ds_users` MEMBER_OF arm got `m.group_id`/`'Group'`
  (latent Code 47). Fixed via shared `endpoint_label_from_plan` helper. **Symptom 1
  (#837, to-side sibling collapse):** the property-superset "polymorphic parent"
  collapse dropped label-discriminated siblings (Folder/File, same table, distinct
  `label_value`) — File's props ⊃ Folder's silently dropped the Folder arm. Gated to
  skip same-table distinct-`label_value` pairs; genuine parent-collapse (LDBC Message,
  distinct tables) provably preserved. Both reviews APPROVE-0. **#836 DONE 2026-07-30
  (#839):** the VLP-continuation duplicate-arm bug (a fixed polymorphic hop AFTER a VLP
  emitted BOTH outer arms with the FIRST label's join+discriminator). The banked "#593/#619
  systemic render-state contamination cycle" label was WRONG — re-tracing showed the SELECT
  projections were already per-arm-correct; only the FROM/JOIN was shared. It's a BOUNDED
  emitter bug: `rewrite_cte_body_vlp_refs` (`to_sql_query.rs`) cloned the base arm's JOINs
  over every VLP-CTE-reading branch, discarding the branch's own correct joins. Fix =
  discriminate by JOIN-ALIAS COVERAGE — keep the branch's own joins when they cover every
  base-join alias slot (polymorphic per-label arm: same `{t2, item}` aliases, different
  target table per label); else clone base (genuine undirected reverse arm whose auxiliary
  joins use a disjoint alias and still depend on the base's chained join, e.g. LDBC
  complex-1's `friend_p`). NOTE: the naive first cut (keep-own when `!is_empty()`) REGRESSED
  complex-1 (dropped its `friend_p` CROSS JOIN → undefined identifier) — caught by review
  instrumenting the FULL `--test integration` suite (corpus_sweep alone hid it). One
  RenderPlan-level edit fixes both dialects. Full-corpus differential (1229 queries, main vs
  branch) → exactly 1 query changes. Review APPROVE-0. `test_external_users_with_access`
  stays xfail — its status==200-only assertion can't detect the row-drop; a real un-xfail
  needs a live server + Group→Group fixture + content asserts. **Out-of-scope sibling filed
  → #840** (undirected VLP + shortestPath reverse arm drops downstream chained joins;
  pre-existing on main, byte-identical, untouched by #839).

### P-1 — Keep a small silent-wrong bug lane open  (standing, ≤1 agent)
**Lane state (2026-08-01): #523 closed as already-fixed (determinism-lock test
added); #581 shipped — agg-arg NULL-padding validity now
alias-qualified (was matching by bare physical column name, a latent
name-coincidence false-positive).** Since
the 07-19 reconcile this lane shipped ~23 fixes (all live- or SQL-gen-verified,
newest first): **#523 (partial-ref undirected 2-hop golden flake, reported
2026-07-10 — root-caused as already-eliminated by the #480/#481 HashMap-order
fixes + `normalize()` counter anonymization; verified byte-stable across 40
fresh-process renders and 45 isolated test runs; locked by
`partial_ref_undirected_2hop_render_is_deterministic_523`)**,
**#581 (agg-arg NULL-padding validity check matched columns by
unqualified name — a node column sharing a name with an unrelated branch table's
column was deemed valid on every branch; `table_valid_columns` now also keys
columns by render alias and `agg_arg_col_valid_for_branch` checks the
alias-qualified set with a flat-union fallback, so the check can only tighten a
false positive, never introduce a false-negative; 0 corpus churn)**,
**#788 (multi-type VLP aggregate ORDER-BY-on-endpoint →
`__order_col_0` Code 47: `build_outer_aggregate_select`'s expr→alias rewrite map
now excludes `__order_col_*` items exactly as `build_aliased_group_by` already
does — the injected ORDER BY helper column could otherwise hijack an aggregate
argument's rewrite to a column the inner UNION drops)**,
**#716 (multi-type VLP non-id endpoint property → native CTE column,
#787)**, #580 (multi-type VLP endpoint id, #715), #606 denorm + fk-edge
VLP relationship-uniqueness variants (#709/#712), #705 (shared EXISTS predicate
rewriter, #707), #640 shapes 1 & 3 (#694/#704), #642 (VLP multi-sub-CTE union
collision, #698), #672 part 2 (#696), #678 (#692), #636 (#674), #683 r1 (#685),
#659 (#682), #641 (#680), #620 (#677, closed via #700), #635 (#675, not-a-bug),
#646 (#671), #648 (#670), #649 (#669), #595 (closed via #702), #647 (#652).
Every remaining open issue is either **design-cycle-sized** (#604/#627/#643/
#673/#628, #640 shapes 2/4/5, #683 residual-2) or in the **reverse-mapping / systemic class** owned by P-4
(#592/#583/#613/#615). **#504 (coupled OPTIONAL collapse) triaged as design-cycle,
NOT a P-1 pick**: root cause is an array-valued `node_id` never ARRAY-JOIN-flattened
(not CoupledSameRow — scalar coupled OPTIONAL renders a correct LEFT JOIN), which
prior work (`f741fcb1`) already concluded "needs a schema-level array flag + ARRAY
JOIN wiring — out of scope for a bounded fix"; array-ness isn't knowable at plan
time (no column types in schema, no sql_only introspection), so even a loud gate
needs the same infra. #606 remaining variants (weighted=not-a-bug, mixed/hetero-poly/
multi-type-uniqueness) all need purpose-built cyclic fixtures = design-cycle-sized.
See memory `p1-lane-remaining-pool-2026-07-26`. Do not force a per-shape patch of the
systemic class (§1.6).

Historical detail (individually-fixable, NOT reverse-mapping class):
~~#647~~ **DONE (#652, `91475be3`)**, #644 (denorm OPTIONAL-VLP anchor
join, loud — **in flight**), ~~#646~~ **DONE (composite self-ref FK-edge; follow-up
#672 part 2 ~~non-self-ref composite from_id/to_id malformed~~ DONE (#696,
`03d61403`); #672 part 1 loud order/arity guard remains)**, ~~#641~~
**DONE (#680, `fe6de435` — #589 gate holes: swallowed-in-UNION + orientation-asymmetric,
both silent→loud)**, #640 (EXISTS beyond single-hop — ~~shape 1 undirected~~
**DONE (#694, `2fdf98f8`)** + ~~shape 3 both-endpoints-outer~~ **DONE (#704,
`90d5697b` — both-outer correlated subquery + `map_exists_outer_predicate`;
follow-up #705 shared exhaustive rewriter); shapes 2/4/5 (multi-hop/composite/denorm)
remain**),
#636 (4-way shared-anchor),
~~#635~~ **DONE (#675 — not-a-bug: coupled rel-var VLP WHERE-filter already
correct; +3 regression goldens; dangling `RETURN r`/`count(r)` shapes are
schema-agnostic #620)**, ~~#620 id-projection~~ **DONE (#677, `079571fc` — VLP
WITH-item endpoint id-property → `start_id`/`end_id`; corrected a silently-broken
directed-VLP golden; FK-edge/denorm/closed-VLP residue split to #678; the broader
#620 residual family verified-fixed + locked #700, issue closed)**,
~~#678~~ **DONE (#692, `3e6d6078` — denorm VLP `WITH a.code` → `start_id`/`end_id`;
`DenormalizedCteStrategy` was the sole strategy hardcoding id `cypher_property`
to `"id"` vs the shared `build_vlp_column_metadata`'s real logical name;
composite + multi-label-table guards keep loud; FK-edge/closed variants already
render correctly on main via #677)**,
~~#659~~ **DONE (#682, `2a2d900f` — VLP `count(<endpoint>)` id → `start_id`/`end_id`
via miss-fallback only; zeek #545/#558/#559 byte-identical; also fixed standard-schema
+ zeek `count(b)` variants; residuals → #683)**, ~~#648~~
**DONE (untyped count(r) multi-type)**, ~~#649~~ **DONE (leading UNWIND before
MATCH)**. Prefer
silent-wrong over loud-error fixes. Rule §1.6 applies: if root cause lands in
the reverse-mapping class, gate loud + document, move on.

### P-2 — P1.2: the five WITH functions  ☑ (done — `refactor/p12-five-with-fns`)
`REFACTORING_SAFETY_PLAN.md` §4.2. Delivered: the missing P1.1 `walk()` /
`any_node()` / `find_map_node()` API on `LogicalPlan` (pre-order, `ControlFlow`
early-exit + `Descend::Yes/Skip` prune, iterative so deep plans can't overflow);
a synthetic-plan characterization matrix locking the five walkers' current
answers; the decision (documented) that the plan's hypothesized load-bearing
divergence was already closed by `3a3af0bf` so unify is pure consolidation, not
a behavior change; unification of the D4 existence twins onto one `any_node()`
impl and the D5 UNWIND collectors onto one core with an explicit
`cross_with_barrier: bool`; write variants handled throughout. Corpus + goldens
byte-identical; `render_plan/AGENTS.md` §6 and CLAUDE.md rule 5 rewritten to
"walk() is exhaustive; barriers are explicit". This unblocks P-4 (together with
P-3). Latent finding filed in-report: `has_with_clause_in_graph_rel` is
duplicated (utils + helpers) with a DIFFERENT semantic — a future consolidation
candidate, not touched here (§8.3 no-drive-by).

### P-3 — Phase 2 module moves (P2.1 → P2.6, in order)  ☑ (P2.1–P2.6 + P2.7 D1 done; D6/D8 already resolved; P2.10 import hygiene + dead_code tail COMPLETE — no module-level allow(dead_code) remains under src/render_plan/) · ☑ Phase-4 §7.1 `build_chained` decomposition **DONE / CLOSED** (43 PRs #740–#785; tail + main-loop + inner render-loop decomposed AND every over-budget extracted helper sub-decomposed under ~500 ln; `replace_v2` 1070 → 212 ln; `build_chained` itself 5478 → 850 ln is the SOLE remaining >500-ln fn in with_to_cte/mod.rs — its irreducible accumulator frame, exit criterion met, the ~19-param whole-loop lift explicitly declined — see §4 + REFACTORING_SAFETY_PLAN §7.1)
The dead-code sweep shrank plan_builder_utils.rs to ~14.5K lines. §5.1 moves are
now underway. Pure groups first (vlp_rewrite →
pattern_comprehension_sql → clause_extractors → plan_predicates →
cte_rewrite → with_to_cte), one move per PR, no logic edits, `pub(crate)`
re-exports during transition. D-cluster dedups (D1/D2/D3/D6/D8 remainder)
ride with their §5.1 home module per the plan. **P2.1 (vlp_rewrite move) merged
(#657)** — VLP expr-rewriting group extracted to `render_plan/vlp_rewrite.rs`,
byte-identical, D3 dedup deferred (follow-up). **P2.2 (pattern_comprehension_sql
move) MERGED (#660, `3776d0a9`)** — the pattern-comprehension SQL string-emitting
group (31 fns, `render_plan/pattern_comprehension_sql.rs`, 2,629 lines) extracted
verbatim, `pub(crate)` re-exports, byte-identical goldens + corpus, ratchet
net-zero; D7-rest deferred. **P2.3 (clause_extractors move) delivered** — the
pure clause extractors that remained (`extract_having/order_by/limit/skip` +
`extract_sorted_properties`) extracted to `render_plan/clause_extractors.rs`,
`pub(crate)` re-exports, byte-identical goldens + corpus, ratchet net-zero. NOTE:
the original §5.1 group also named `extract_filters/from/group_by/distinct`, but
those had already migrated to `filter_builder.rs`/`group_by_builder.rs` via
incremental work, so P2.3's real scope was the 5-function remainder. **P2.4
(plan_predicates move) delivered** — the WITH-detection predicate group
(`has_with_clause_in_tree`/`has_with_clause_in_graph_rel`/`plan_contains_with_clause`)
extracted to `render_plan/plan_predicates.rs`, `pub(crate)` re-exports,
byte-identical goldens + corpus, ratchet net-zero. Scoped down from §5.1's
premise: the private fresh-scan/with-exported alias walkers named there are
coupled to P2.5's cte_rewrite fns, so they ride with P2.5 (§8.3 no-drive-by); the
`plan_builder_helpers` `has_with_clause_in_graph_rel` D-cluster copy stays
untouched. **P2.5 (cte_rewrite move) in progress — sub-slice A delivered**: the
CTE-expression-rewriting cluster (`rewrite_operator_application_for_cte`/`_join`
+ `rewrite_render_expr_for_cte_simple`/`_operand`) extracted to
`render_plan/cte_rewrite.rs`, `pub(crate)` re-exports, byte-identical (one
`fn`→`pub(crate) fn` visibility widening for the re-export), ratchet net-zero.
P2.5 is being sub-sliced because its §5.1 home is ~13 fns across 3 scattered
bands (much larger/entangled than P2.1–P2.4); **sub-slice B delivered**: the
CTE-name remap pair (`remap_cte_names_in_expr`/`_in_render_plan`) extracted to the
same `cte_rewrite.rs`, byte-identical, ratchet net-zero. **Sub-slice C delivered**:
the join-condition rewrite group (`collect_with_cte_table_aliases`,
`strip_table_alias_from_resolved`, `rewrite_join_conditions_for_cte_aliases`)
extracted to the same module, byte-identical (three `fn`→`pub(crate) fn`
widenings), ratchet net-zero. **Sub-slice D delivered**: the LogicalPlan-level
CTE-ref rewriters (`rewrite_logical_expr_cte_refs`, `update_graph_joins_cte_refs`,
and the 3 alias walkers deferred from P2.4) extracted to a new
`render_plan/cte_graph_joins_rewrite.rs` (the LogicalPlan companion to
`cte_rewrite.rs`), fully byte-identical (no visibility changes needed), ratchet
net-zero. Remaining P2.5: **D2 dedup only**. **D2 dedup delivered**: the
four-function CTE property-rewriter family collapsed to one operator core +
`rewrite_render_expr_for_cte` + a `CteAliasPolicy` (`Keep`/`Rewrite`) enum,
byte-identical (corpus + goldens unchanged) as a behavior-preserving dedup — the
double-encoding guard still fires only under `Rewrite`; the plan's universal-guard
idea is left as an open transition-assert follow-up (would change `Keep`
semantics). **P2.5 COMPLETE.** **P2.6 with_to_cte COMPLETE** — the "entangled
core" (the two giant WITH→CTE builders + orbit) moved verbatim into a new
`render_plan/with_to_cte/mod.rs` across four byte-identical sub-slice PRs: (1) the
#529 property guards, (2) `replace_with_clause_with_cte_reference_v2`, (3) the
WITH-discovery/pruning cluster, (4) `build_chained_with_match_cte_plan` (~5,478
lines) + its `WithBarrierScope`/`CteNameAllocator` orbit structs + widening the 24
private helpers it calls back into. Kept in a single `mod.rs` (at `render_plan`
depth) so the moved bodies' `super::…` paths stay byte-identical — a sub-file split
is a Phase-4 logic edit, not a move. `plan_builder_utils.rs` 13,339 → 4,874 lines.
**Next: P2.7 (D1 `with_clause_key` dedup)** or Phase-4 decomposition of the moved
giants (§7.1).

**P2.7 D1 done**: the three near-duplicate WITH-key helpers that P2.6 co-located
in `render_plan/with_to_cte/mod.rs` collapsed to one canonical `with_clause_key()`
in `src/utils/with_clause_key.rs` (next to `cte_naming`, per §5.2). The D1 "verbatim
triplicate" premise was half-wrong: two were byte-identical simple copies, but
`generate_with_key_from_with_clause` carried an extra item-extraction fallback and
was the version `find_all_with_clauses_grouped` relied on. Unified on that rich
variant; `corpus_sweep` + `sql_golden` byte-identical (529 tests), ratchet net-zero,
`with_to_cte/mod.rs` shed 148 lines (also deleted the now-orphaned nested
`extract_with_alias`, whose logic moved into the util). **Next: P2.8 (D6) / P2.9
(D8)** or Phase-4 decomposition of the moved giants (§7.1).

### P-4 — Phase 4 §7.2: forward resolution through CTE scope  ◐ (F0+F1+F1b/#602+#662+F2a+F3+F4+F6-partial done; **no bounded byte-identical slice remains** — F2b-fold/F2c/F5 are design-cycle/blocked/deferred, taken only when a bug forces them)
**Concrete staged plan written: `docs/design/FORWARD_RESOLUTION_PLAN.md`.** It
supersedes the stale `render_plan/AGENTS.md` §10 premise: the `reverse_mapping`
field §10 says to delete was already removed in #115 (Feb 2026); the debt forked
into three overlapping resolution mechanisms, with **#592** (VariableRegistry
`define_*` drops `property_mapping`; `set_property_mapping` has zero callers) as
the systemic root. The architectural fix for the open-issue residue: #592, #595,
#602, #613, #643, and the #583 render rework. Slices **F0–F6**. **F0 merged**
(#661, thread `property_mapping` + transition-assert). **F1 merged** — made the
render-site forward registry (M1) authoritative and deleted the legacy M2
render-site fallback, byte-identical. F1's key discovery (banked in the plan's
F1-outcome note): the load-bearing render-site path was M2 resolving **scalar /
composite CTE exports** (empty `property_mapping` → both M1 and M3 returned
`Unresolved`, so F0's assert never covered them); fixed with a registry identity
self-map + a `sql_alias == table_alias.0` guard that reproduces M2's FROM-alias
keying and closes the #593 cross-arm leak. **F1 is byte-identical (a
consolidation), NOT the intentional-diff slice** — the #595/#602/#613/#643
user-visible fixes are re-scoped to **F1b** (the M3-path hunt) + Phase C
(opaque strings). **F1b/#602 done** — post-WITH MATCH continuation joined on the
wrong column across a second passthrough WITH barrier (the node label was dropped,
so the `.id` operand mis-resolved to the edge `to_id`, #616-class Code 47); fixed by
carrying the node label forward across barriers (`WithBarrierScope::carried_labels`),
a forward-data thread, +3 regression goldens, corpus byte-identical. **F2a done** —
deleted the M2 legacy mechanism (`cte_property_mappings` task-local field + its
populators + 3 consumers), byte-identical (2,156 tests), de-risked by a stub-probe
experiment that proved M2 fully covered by the forward path. **F3 merged**
(PatternCount carry-pattern) and **F4 merged** (ExistsSubquery carry-subplan) —
both Phase-C de-opaque slices, byte-identical (C1 carry + validated cache), so a
later rewriter can recurse into the pattern/subplan. **#602 follow-up #662 done**
(#663, `17be0351` — carry the node label under the *source* alias so a
rename-at-barrier resolves; the #602 carry was keyed only by published name).
**#595 closed** (#702, verified-fixed as collateral of #587 + carry-id-across-
barrier). **F6-partial done** (#791 — deleted the now-dead `set_property_mapping`;
corrected the stale §1.1/§1.2 docs). **Next: nothing byte-identical remains.**
F2b's assert-half is already satisfied by F0; its fold/delete-M3 half is a
**design cycle** (arm-safe M1 + drop the containment guard → shifts #593-class
output), taken only when a #593-class bug forces it — not a refactor slice. F2c
is blocked (dual-name projection is load-bearing), F5 deferred (needs #583). The
remaining **F1b** residue (#613→F3 machinery in place, #643 planner-topology) is
bug-driven, not lane work. Per-shape patching of this class stays forbidden
(§1.6). Remaining Phase-1 pass migrations (P1.4+) and Phase-3 §6.2 slices are
fill-in work alongside, not blockers.

### P-5 — Stats-informed SQL generation  ◐ (S1 implemented on branch; S2/S3 open)
New. Today all planning is rules/heuristics; the concrete gap is
`select_anchor()` (`analyzer/graph_join/join_generation.rs:550`) breaking
ties **alphabetically**, and `has_selective_filters()` being a boolean.
Staged (each stage its own design-then-implement, LDBC-benchmarked):
- **S1 — table row-count cache**: ✅ implemented (`feature/stats-planning-s1`):
  `graph_catalog/table_stats.rs` (snapshot + pluggable source + TTL cache,
  `CLICKGRAPH_STATS_TTL_SECS`), attached to the task-local `QueryContext` at
  HTTP/Bolt request entry, consumed by `select_anchor()` as a within-tier
  ascending-row-count rank (alphabetical fallback preserved; unknown/NULL
  counts = stats-less). Config-gated (`CLICKGRAPH_STATS_ENABLED`, default
  off); goldens/corpus stay stats-less + new with-stats golden set
  (`stats_anchor_golden_tests.rs`). Remaining S1 follow-ups: embedded/remote
  library-mode wiring; Databricks source via `DESCRIBE TABLE EXTENDED`
  (`databricks_probe.rs`); LDBC-scale benchmark. Design:
  `docs/design/STATS_PLANNING.md`.
- **S2 — column selectivity**: NDV/min-max (ClickHouse column statistics /
  `system.parts_columns` `uniq`) to rank anchors among filtered candidates
  and pick VLP recursion direction (start BFS from the smaller endpoint
  set — today writing direction decides).
- **S3 — feedback loop**: correlate the already-collected per-query
  `read_rows`/latency (metrics module, slow-query ring) with plan shapes
  to find which heuristics actually cost, BEFORE building more machinery
  (no per-query EXPLAIN ESTIMATE round-trips until S3 says where).
Guardrails: rule §1.7 (ordering only, off by default); goldens stay
stats-less; the with-stats golden set locks the flag-on plan against a
fixed stats fixture.

### P-6 — Backlog (do not start without re-prioritizing here)
- **SQL-IR track — ACTIVE (resumed 2026-07-29).** Phase 1 (migrate Path A's
  remaining hardcoded CH leaves through the dialect layer, one leaf per PR, CH
  byte-identical) is the live lane. Reality vs the stale `SQL_IR_DESIGN.md`
  header ("no code yet"): `FunctionMapper` is already a de-facto `Dialect` trait
  (26 methods), dialect is read at emission time, Databricks output is reachable,
  and a 218-case dual-dialect `sql_ir/` golden net + 1,119-case corpus are
  locked. Method: grep the `.databricks.sql` goldens for leaked CH spellings to
  find the empirically-reached bugs, fix the most-reached leaf first via a
  `common.rs` dialect helper. **Done:** simple-CASE (#811), STARTS/ENDS WITH
  (#813, 6 sites), RegexMatch `match(...)`→`rlike` (#815, 2 sites — helper
  pre-existed; 0 golden leak, latent hardening). **Clean mechanical operator/
  function-name leaves are now EXHAUSTED.** What remains behind Databricks
  golden leaks is NOT mechanical:
  - **Exponentiation (`^`)** — `to_sql_query.rs` (×3) emits infix `^`, which is a
    hard `SYNTAX_ERROR` in ClickHouse itself (CH has no `^` operator; Spark `^`
    is bitwise-XOR not power). BUT `Operator::Exponentiation` is **parser-
    unreachable**: `^`→Exponentiation lives only in `parse_operator_symbols`,
    which is called *only from unit tests* — the live precedence chain
    (`parse_multiplicative_expression`) handles `* / %` only. Latent behind a dead
    parser; a tiny consistency fix (route all 4 sites to `power()`) but zero urgency.
  - **`JSONExtractString` / `toFloat64` / `splitByChar` leaks** (3 corpus
    `.databricks.sql`) are **schema-config, not operator/function arms**: raw CH
    SQL fragments authored directly in schema YAML `property_mappings`
    (`score_float: "toFloat64(score_str)"`, `schemas/test/property_expressions.yaml`)
    and passed to SELECT verbatim, never touching `FunctionMapper`. Translating
    them means parsing arbitrary SQL out of user config — a design question
    (arguably not the engine's job; the schema author targeted a backend), NOT a
    leaf slice. `FunctionMapper` mappings for these already exist and are correctly
    dialect-routed for Cypher-level calls (`name_for` accessor); only the
    config-embedded form leaks.
  Net: Phase-1 leaf work is at its boundary. Further SQL-IR progress needs a
  design decision (schema-expression translation) or Phase 2 (path collapse).
  **Phase-2 path-collapse INVESTIGATION DONE (2026-07-29, `SQL_IR_DESIGN.md` §3.5,
  3-agent parallel terrain map).** Ground truth: the "four drifting paths" are NOT
  co-equal. A=`RenderExpr::to_sql` is canonical + covers all 20 variants; B
  (`plan_builder_helpers`, 1 write-fallback caller, 11/20 variants, `"TRUE"`
  catch-all) is near-dead; D=`LogicalExpr::to_sql` is legacy, reachable only
  transitively via function-arg translation + a dead ViewScan path; C
  (`cte_extraction`, ~8 CTE/filter callers) is the one live second path and its
  separateness is FUNDAMENTAL (runs at CTE-build stage before A's `query_context`
  task-locals exist; its `alias_mapping` p1→start_node rewrite has no equivalent
  in A). Recommended shippable Phase-2 order: (1) retire B — **DONE #822
  (`683fb10c`)**, but NOT the "small warm-up" the plan assumed: B's sole caller
  (write-payload rendering) is GENUINELY LIVE, and B-vs-A diverge on exactly one
  arm — `SET x = a.id` on a RENAMED-node_id schema, where B emitted a broken
  literal `a.id` and A resolves the real column under the executor's schema
  context. So B→A was a latent-bug **fix** (#411 family). Review caught a real
  pre-existing escaping bug in A's `Literal::String` arm (embedded quote nested
  in a concat → unescaped SQL, affects read side too) — fixed at root in the
  same PR, 0 golden churn. **(2) unify the dual `Operator` enums — DONE #818 (`188e4507`)**,
  **(3) collapse D→A — DONE #820 (`refactor/retire-path-d`) but as a DELETION,
  not a collapse: a reachability audit (2-agent corroborated) found Path D is
  ENTIRELY DEAD in production (two separate `ToSql` traits — prod uses Path A's
  `render_plan::ToSql`→String; Path D's `to_sql.rs::ToSql`→Result is a closed
  test-only cluster). Deleted `to_sql.rs`+`view_query.rs`+`view_scan.rs`+
  `translate_scalar_function`/`translate_duration_function`+16 tests, −1170 lines;
  ported the FINAL-gating test to the live path + added `fn_datetime_epoch_millis`
  golden to preserve/close coverage; 0 churn.** (4) C full-collapse is a separate
  DESIGN CYCLE (stage/timing constraint), NOT a mechanical step — **written up in
  `docs/design/PATH_C_COLLAPSE.md`** (two viable options — thread an optional
  alias-mapping arg into the shared printer [lower-risk, recommended] vs
  install an alias overlay over A's variable-registry seam at CTE-build [fully
  deletes the param but needs new plumbing, not the one-call `set_cte_alias_scope`
  — that setter only drives IN-subquery FROM generation, not property qualifiers];
  the byte-identity spike gate; the
  34-call-site batching plan; trigger conditions = a Path-C Databricks bug or a
  third dialect). Its drift items (backtick-vs-doublequote quoting,
  latent-unreached hardcoded `POWER`/`arrayFold`/map/CASE arms — verified 0
  Databricks golden leak) can be reconciled as small independent correctness
  slices without full collapse. **`arrayFold`/ReduceExpr routed DONE 2026-08-01
  (#842)** — Path C's ReduceExpr now goes through `common::reduce_fold_sql` (Spark
  `aggregate`), byte-identical CH, 0 golden churn (corpus-unreached via CTE-build).
  **First quoting slice DONE
  (2026-07-30): `quote_identifier` routed through the dialect layer** — Path C's
  `common.rs::quote_identifier` hard-coded backticks for BOTH dialects, so on CH a
  single query emitted both `` t2.`id.orig_h` `` (CTE body, Path C) and
  `t1."id.orig_h"` (SELECT, Path A) for the same physical column. Now special-char
  identifiers route through `FunctionMapper::quote_alias` (CH `"x"` / Spark
  `` `x` ``); `json_builder.rs::quote_column_name` delegates too (`quote_json_key`
  untouched — its backticks are a load-bearing JSON key). The one empirically-
  reachable Path C drift item (23 backtick special-cols in committed CH goldens);
  churn 2 CH goldens, 0 DBX. The hardcoded-arm + InSubquery-placeholder items stay
  latent (verified 0 corpus leak / join-planner-consumed before C renders).
  **Follow-up filed as a note:** `multi_type_vlp_joins.rs` emits
  `toString(n2.id.orig_h)` — an UNquoted dotted column (CH parse error), pre-existing.
  Phase 3 (structural idioms) / 4 (Raw shrink) stay deferred behind Phase 2.
- Phase 3 §6.2/§6.3: **COMPLETE** (slices 3/4/2 resolved 2026-07-28,
  #793/#795/#796; `pattern_schema.rs` dead_code audit done #799). No Phase-3
  items remain.
- Composite-id emission: #604/#713 **DONE** (2026-07-28, #800–#804). Remaining
  composite-id follow-ups are bug-driven / design-cycle: ~~#802 (FK-edge
  exact-bound dup-alias Code 179)~~ **DONE 2026-07-29 (#810)**, #627 (composed/
  adjacent VLP CTE per-column exposure), #672 part-1 (wrong-order silent zip).
- #411 (generic `.id`) — only after P-4, per the plan.
- Denorm foreign-edge union-dimension design (perf-staged, memory notes).
- DeltaGraph live-workspace validation items (`GA_READINESS.md`).

## 3. Capacity split (guideline)

With ~4 concurrent agent lanes: 1× P-0 until green (then it folds into a
standing nightly-triage duty), 1× P-1 standing, 1–2× P-2/P-3 (then P-4
after P-2 merges), 1× P-5 S1. Re-balance here, in writing, not ad hoc.

## 4. Merge log (newest first — append on merge)

- 2026-08-02: **P-1 — negative-index list slicing returned wrong elements
  (silent)** (PR #867, branch `fix/list-slice-negative-index`). Cypher list
  ranges allow negative bounds counting from the end (`list[-2..]` = last two)
  and clamp out-of-range bounds; both SQL-emission paths (Path A
  `RenderExpr::to_sql`, Path C `render_expr_to_sql_string`) applied the
  0→1-based `from + 1` offset shift blindly to every bound, so a negative offset
  landed at the wrong place on CH `arraySlice` / Spark `slice`. Silent wrong
  data, not an error: `[1,2,3,4,5][-2..]`→`[5]` (Neo4j `[4,5]`),
  `[-1..]`→`[]` (`[5]`), `[1..-1]`→`[]` (`[2,3,4]`), `[..-1]`→length `-1`
  (`[1,2,3,4]`). The array-**index** path (`list[-1]`) already normalized
  negatives; the **slice** path never did. Fix: normalize each non-literal /
  negative bound to `i >= 0 ? i : greatest(len + i, 0)`, where `len` is the
  dialect's array-length call. That length spelling is the only dialect-specific
  piece — CH `length` is overloaded for arrays+strings, Spark reserves `length`
  for strings and needs `size` for arrays — so it routes through a new
  `FunctionMapper::array_length` (Rule #7), not inline branching. Statically
  non-negative integer literals bypass normalization → every positive-slice
  golden byte-identical (0 churn). Shared helpers `normalize_slice_bound` /
  `is_nonneg_int_literal` live in `to_sql_query.rs`, reused by Path C so both
  stay in lockstep. Live-verified against CH: 24-case matrix (positive, negative
  from/to, both-negative, over-negative, from>=to, single-bound `[from..]` /
  `[..to]`, empty, column-valued arrays, nested slices, expression bounds,
  slice-in-expr) all match Neo4j; Path C confirmed live via WITH-barrier CTE.
  Adversarial review APPROVE-0 (float bounds `[1.5..]` loud Code 43 = matches
  Neo4j integer-only, not a regression; array re-eval in the guard is cosmetic
  only). +1 golden (both dialects), +2 mapper unit tests; ratchet passes; full
  suite green.
- 2026-08-02: **P-1 — `count(<scalar>)` crash fixed** (PR #865, branch
  `fix/count-scalar-missing-label`). `count(x)` where `x` is a scalar variable
  (UNWIND element, WITH-scalar passthrough, or parameter) crashed loudly in
  ProjectionTagging with `Missing label for node \`x\`` — the count-arg tagging
  path unconditionally rewrites `count(n)` → `count(n.<id_column>)` (correct
  OPTIONAL-MATCH NULL semantics) and called `get_label_str()` on the scalar,
  which has no label. `sum`/`collect`/`min`/`count(*)` were all fine — only the
  count-node-id rewrite has this assumption. Fix: in the count-tagging branch,
  detect a scalar arg via `plan_ctx.lookup_variable(x).is_scalar()` and rewrite
  the arg `TableAlias(x)` → `ColumnAlias(x)` (preserving DISTINCT) — renders to
  the identical bare `x` but dodges the render-phase `AggregateFnCall::try_from`
  fail-fast that (correctly) rejects a bare `count(TableAlias)` node var. Review
  found a BONUS: on main the mixed case silently emitted a WRONG `count(*)` for
  the scalar (not just the loud crash) — so this also closes a silent-wrong.
  Live-verified: `count(x)`=3 (null-skipping), `count(DISTINCT)` dedup=2, mixed
  `count(one),count(u)`=5,5 (scalar→`count(one)`, node→`count(u.user_id)` in one
  query — guard fires only for scalars), OPTIONAL `count(p)`=0 for unmatched
  (node counts unregressed), `count(id(u))` #539 path intact. Adversarial review
  APPROVE-0 (is_scalar precise enum match — no node/rel misfire; column-name
  collision tested clean). +1 golden (both dialects), 0 existing-golden churn.
  Out-of-scope sibling filed → **#864** (scalar RENAMED across a WITH barrier →
  spurious `count(y.y)`, post-WITH resolution family). Also filed during the hunt:
  **#863** (pattern-comprehension COMPUTED projection collapses to `groupArray(1)`,
  dropping the expr + target JOIN — pre-existing, silent-wrong).
- 2026-08-02: **P-1 — exponentiation operator `^` supported end-to-end** (PR
  #862, branch `fix/861-exponentiation-power`, closes #861). Corrects the
  original #861 filing: `^` did not render wrong on Path A — it **never parsed
  at all**. `parse_multiplicative_expression` handled only `*`/`/`/`%`, so any
  query with `^` failed with `Unparsed input`; the operator enum, precedence
  table, and all render sites were unreachable dead code, and a
  `#[cfg(test)]`-only helper (`parse_operator_symbols`) gave false confidence.
  Fix: new `parse_exponent_expression` tier between multiplicative and unary,
  faithful to the checked-in openCypher grammar (`<arithmetic factor> ::=
  <arithmetic unary> | <arithmetic factor> <circumflex> <arithmetic unary>`) —
  `^` binds tighter than `*`/`/`/`%`, looser than unary sign, LEFT-associative
  (`2^3^2` = `(2^3)^2` = 64, per the repo grammar; not the right-assoc math
  convention). Render: neither CH (Code 62 on bare `^`) nor Spark has infix `^`;
  both accept ANSI `POWER(base, exp)`. New `FunctionMapper::power()` (shared
  default — Rule #7) routes the 3 `to_sql_query.rs` infix sites + the
  `pattern_comprehension_sql.rs` site; Path C already emitted POWER. Live-
  verified against CH: `2^3, 1+2^3, 2^3*4, 2^3^2, -2^2` = 8, 9, 32, 64, 4.
  Zero golden churn (`^` was unparseable, so no existing golden used it); +1
  golden (both dialects), +4 parser tests, +2 mapper tests; ratchet passes.
  Adversarial review APPROVE-0 (all 5 render paths audited, no reachable infix
  fallback for `^`, existing-operator precedence unregressed). NOTE surfaced,
  not fixed: `groupArray(1)` drops the projection for *computed* pattern-
  comprehension expressions (`[(n)-[]->(m) | m.id * 2]`) — pre-existing on main,
  file separately.
- 2026-08-02: **P-6 hygiene — #808 deleted the structurally-dead VLP recursive
  generators (mixed arms kept)** (branch `fix/808-dead-vlp-recursive-generators`).
  Follow-up to #606/#807. Adversarial review split the node-unique generators:
  the fully-`denormalized` arms (intercepted upstream by `DenormalizedCteStrategy`,
  `cte_manager/mod.rs:3261`) and the `heterogeneous-polymorphic` recursive arm
  (superseded by the retained `generate_heterogeneous_polymorphic_sql` early-return)
  are structurally unreachable → DELETED, along with the 2 helpers they orphaned
  (`generate_polymorphic_edge_filter_intermediate`, `map_denormalized_property`).
  The `mixed` arms are reachable by construction (`new_mixed`, `mod.rs:3288`) and
  only corpus-empty → KEPT, deferred to option-(b) coverage-first per #808 (deleting
  them would silently reroute a mixed VLP onto the standard arm, unverifiable without
  live CH). Empirically re-confirmed (dispatcher fired 410× in corpus_sweep, the
  deleted arms 0×). Zero behavior change: corpus_sweep + sql_golden 0-churn; ratchet
  baseline improved (−2 in-file axis-flag counts). #808 stays open for the mixed
  slice.
- 2026-08-02: **P-1 — preserve parens on a lower-precedence LEFT operand** (PR
  #859, branch `fix/858-left-parens`, closes #858). Silent-wrong arithmetic on
  main: `(1+2)*3` rendered `1+2*3` = 7 (Neo4j 9), `(5-3)/2` → `5-3/2`, etc. —
  `render_expr.rs` had `needs_right_parens` but no left equivalent, so a
  parenthesized lower-precedence left operand lost its grouping. Fix: new
  `needs_left_parens` (parens iff left-operand arithmetic precedence STRICTLY <
  outer; equal precedence stays bare by left-associativity). Applied at the 3
  Path A binary-render sites + Path C (`cte_extraction.rs`), whose arithmetic
  arms had NO parenthesization at all (both sides missing) — now routed through
  `needs_left_parens`/`needs_right_parens`. Live-verified all sign/precedence
  combos incl. modulo (`%` is same-prec as `*`/`/` and left-assoc on CH, so
  equal-prec-no-parens is safe — checked independently + by review), mul/div
  chains, intDiv interaction (`(7/2)*4`→`intDiv(7,2)*4`=12), deep nesting, column
  arithmetic. 0 corpus/golden churn (latent hardening). New `arith_left_parens`
  golden (both dialects) + 5 `needs_left_parens` unit tests. Adversarial review
  APPROVE-0 (every associativity claim live-verified). Gate: fmt · clippy · 1632
  lib · 250 golden · corpus + ratchet net-zero. **Follow-up filed #861**
  (exponentiation `^` renders infix on Path A → CH Code 62; Path C already emits
  `POWER`; pre-existing, + right-assoc-left-parens caveat if fixed).

- 2026-08-02: **P-1 — integer-constant division truncates toward zero** (PR
  #857, branch `fix/847-intdiv-literal`, closes the bounded half of #847).
  Cypher `int / int` truncates toward zero (`7/2 = 3`, `-7/2 = -3`); CH/Spark
  `/` is always float, so `7/2` returned `3.5`. Fix (Rule #7): when BOTH
  operands are integer CONSTANTS (integer literal or the parser's `-n` negation
  form `Subtraction(0, n)`), emit `intDiv(a, b)` (CH) / `div(a, b)` (Spark) via
  new `FunctionMapper::integer_division`. New `is_integer_constant` (pub(crate),
  conservative — literals + `0-n` only, never misclassifies `5-3`/`8/(4-2)`) +
  `render_integer_division` at the 3 Path A operator sites (alongside
  `render_interval_arithmetic`); Path C (`cte_extraction.rs`) mirrors via the
  shared helper. Bounded: column operands aren't typeable at render time (schema
  carries no column types) so `u.a/u.b` stays `/` — residual kept on #847. Float
  operands correctly stay `/`. Live-verified all 8 sign combos + float
  non-regression + Path A/C. 0 corpus/golden churn (latent hardening). New
  `int_division_literals` golden (both dialects). Adversarial review APPROVE-0
  (float non-regression + false-positive boundary both independently
  live-verified; Databricks `div(a,b)` call-form confirmed valid via Spark
  `IntegralDivide` source). Gate: fmt · clippy · 1626 lib · 249 golden · corpus
  + ratchet net-zero. **Two bugs filed from the same hunt:** #854 (temporal
  component/arithmetic on native Date columns → Code 43, wrap_epoch_millis_arg
  assumes epoch-millis BIGINT; design-cycle, arithmetic half in `to_sql_query.rs`),
  #855 (three-valued IN/NOT IN with a null list element returns false/true not
  null), and **#858 (HIGH-IMPACT silent-wrong: parens dropped on a
  lower-precedence LEFT operand — `(1+2)*3` → 7 not 9; root = missing
  `needs_left_parens` in `render_expr.rs`; clean fix, strong next pick)**.

- 2026-08-02: **P-1 / SQL-IR — Path C (CTE-body) list literal + subscript
  rendering** (PR #853, branch `fix/path-c-list-subscript`). Loud bug (CH Code
  43) in `render_expr_to_sql_string` (`cte_extraction.rs`), the CTE-build-stage
  renderer for per-arm WHERE predicates in multi-type / undirected UNION CTEs —
  the last of the #850-audit siblings. Two defects vs canonical Path A: (1) the
  `List` arm emitted a TUPLE `(a, b, c)` — CH `arrayElement`/`length`/`has`
  reject tuples (Code 43), so `[1,2,3][i]`/`size([1,2,3])` in a multi-type CTE
  WHERE crashed; (2) the `ArraySubscript` arm emitted raw `arr[index]` with no +1
  offset (off-by-one) and no null-safety. Fix mirrors Path A: `List`→array
  literal via `array_literal`; `ArraySubscript`→1-based offset +
  `array_element_or_null` (#850). **New `FunctionMapper::in_list_predicate`
  (Rule #7, ratchet net-zero, shared trait default)** renders the IN operator's
  list RHS as a paren VALUE-LIST `x IN (a, b)` on BOTH dialects — NOT an array:
  a heterogeneous CH array literal (id column `toString`-wrapped) fails
  NO_COMMON_TYPE (Code 386), while SQL `IN (...)` coerces per-element; Spark
  needs a value-list too. **Adversarial review caught a real regression in the
  first cut** (routing IN through the array form → Code 386, working→broken,
  live-proven); the redesign to always-value-list both fixes it AND matches main
  byte-for-byte → **ZERO corpus churn vs main**. Reviewer also confirmed the
  value-list beats an OR-expansion (which ALSO fails Code 386 on
  `UInt64col = toString(col)` — only IN's per-element coercion tolerates the
  heterogeneous mix). Live-verified: subscript in-bounds/OOB→NULL/negative, IN,
  NOT IN, empty IN (→FALSE), size(list) all execute + agree with Path A.
  2 new sql_ir goldens (`multi_type_where_list_subscript`/`_in_list`, both
  dialects) + 2 mapper unit tests. Re-review APPROVE-0. Gate: fmt · clippy · 1626
  lib · 250 golden · corpus + ratchet net-zero. **The `cte_extraction.rs:1904`
  sibling from the #850 audit is now CLOSED** — the empty-list-default + Path C
  loud-crash families are both fully resolved.

- 2026-08-02: **P-1 — `head([])`/`last([])` on empty list return `null` (was
  ClickHouse type default)** (PR #851, branch `fix/head-last-empty-null`).
  Direct sibling of #850, same openCypher fidelity class. `head`/`last` on an
  EMPTY list must return `null` (Neo4j) but lowered to CH `arrayElement(list, 1)`
  / `arrayElement(list, -1)`, which return the element type DEFAULT (`0`/`''`)
  on an empty array. Fix: the `head`/`last` CH function-registry mappings now use
  `arrayElementOrNull` (the accessor #850 added) instead of `arrayElement`;
  Databricks unchanged (`element_at` already NULL-on-OOB, byte-identical).
  Completeness (self + adversarial review, live-verified): the empty-list
  wrong-default family is now CLOSED — `head`/`last` (this PR) + subscript `[i]`
  (#850); `tail([])` correctly returns `[]` not null (arraySlice, left); the two
  internal `array_element()` uses (`select_builder.rs:3297` path_relationships[1],
  `plan_builder_utils.rs:4033/4061` groupArray-head flattening) are
  always-non-empty machinery, correctly left. Live-verified (empty→NULL,
  non-empty→first/last, literal lists, null propagation + coalesce, nested
  head(tail([]))→NULL). 1 CH golden churn (`fn_head_last`), 0 Databricks; new
  `fn_head_last_empty` golden. Adversarial review APPROVE-0. Gate: fmt · clippy ·
  1624 lib · 248 golden · corpus + ratchet net-zero.

- 2026-08-02: **P-1 — out-of-bounds list index returns `null` (was ClickHouse
  type default)** (PR #850, branch `fix/oob-list-index-null`). Silent-wrong
  openCypher fidelity bug from the same function/operator-fidelity vein as
  #848/#847: Cypher `list[i]` with an out-of-range index returned CH's element
  type DEFAULT (`0`/`''`) instead of `null` — and `[0,1,2][10]`→`0` is
  indistinguishable from a real `0` element. Fix (Rule #7 dispatch): new
  `FunctionMapper::array_element_or_null(arr, idx)` — CH `arrayElementOrNull`
  (NULL on OOB), Databricks `element_at` (already NULL on OOB, byte-identical to
  the prior accessor). The `RenderExpr::ArraySubscript` numeric-index arm
  (`to_sql_query.rs`) routes through it; the hardcoded `get_current_dialect()`
  branch collapses into the mapper (ratchet NET-NEGATIVE — one axis token
  removed). Untouched: in-bounds/negative indices, string-literal map-key access
  (`arr['key']`), and the internal `arrayElement(groupArray(...), 1)`
  head-extraction (always non-empty). Reachability audit (self + adversarial
  review, live-verified): the only reachable user-facing subscript is Path A;
  Path C (`cte_extraction.rs:1904` raw `{}[{}]`) is a LOUD Code-43 crash on
  array columns (list-literal-as-tuple + off-by-one), a DIFFERENT pre-existing
  bug — not the silent-type-default family, correctly left. Golden churn is
  behavior-identical: `type(r)` over a VLP path lowers to `path_relationships[1]`
  (always in-bounds index-1), so 13 corpus + 4 sql_ir CH goldens shift
  `arr[1]`→`arrayElementOrNull(arr,1)`; **0 Databricks churn**. New
  `list_index_out_of_bounds` golden + mapper unit tests. Adversarial review
  APPROVE (every claim live-verified: OOB both ends/empty→NULL, in-bounds +
  negatives identical, nullable cascade safe through arithmetic/cmp/IN/GROUP
  BY/ORDER BY/DISTINCT-tuple/nested-subscript, scope-leak clean). Gate: fmt ·
  clippy · 1612 lib · 244 golden · corpus + ratchet net-zero. **Two sibling
  fidelity bugs noted for a follow-up (same class, not filed yet):** `head([])`/
  `last([])` on an empty list → `0`/`''` (should be `null`); the
  `cte_extraction.rs:1904` multi-type-branch subscript (loud Code 43 today).

- 2026-08-01: **#637 — implicit GROUP BY drops grouping keys buried inside
  aggregate-containing items (RETURN + WITH barriers)** (`fix/637-buried-grouping-keys`).
  A grouping key buried inside a RETURN/WITH item that ALSO contains an aggregate
  (`RETURN a.user_id + count(b)`, `RETURN a.city + ':' + toString(count(b))`,
  `WITH a.user_id + count(b) AS x`) was dropped → no GROUP BY emitted → ClickHouse
  Code 215 NOT_AN_AGGREGATE (silent wrong buckets on lenient backends). Root cause
  was **independent aggregate/non-aggregate split sites across phases**, each
  keying GROUP BY on whole aggregate-FREE items only: analyzer
  `group_by_building.rs` (RETURN barrier) and render `with_to_cte/mod.rs:~7852`
  (WITH barrier). Fix: one shared exhaustive `collect_grouping_keys` in
  `logical_expr/visitors.rs` (built on `HasAggregateCheck`/`walk_expression`)
  returning the **maximal aggregate-free sub-expression(s)** that reference a
  column/alias; routed through both barriers; the RETURN-barrier's incomplete
  local `contains_aggregate` (missed the legacy `Operator` variant + array/map
  containers) retired in favor of `HasAggregateCheck`. Maximal-subtree (not
  leaf-fragment) semantics keeps the existing `CASE…END`/`u1.name` corpus grouping
  keys byte-identical AND renders valid CH (`GROUP BY concat(a.city, ':')` matches
  the SELECT's non-aggregate operand). Behavior-preserving split: aggregate-free
  whole items pushed as before (incl. constant keys, `RETURN 'all_users', count(n)`
  → `GROUP BY 'all_users'`), buried keys added; deduped structurally. **0 corpus
  churn** (1,229 queries × 2 dialects — verified independently + by Explore agent;
  no corpus query mixed a column ref with an aggregate in one item). Gate: fmt ·
  clippy clean · full lib+integration+doctests · corpus_sweep + sql_golden (+3 new
  #637 goldens, fail-when-reverted) 0 churn · ratchet net-zero. No live CH in env —
  correctness bar is SQL shape + byte goldens + full gate (the required
  `GROUP BY a.id` with `a.id + count(...)` in SELECT is standard SQL).
  **THIRD SITE DEFERRED:** the UNION-return builder `return_clause.rs`
  `build_union_with_aggregation` (~741) has the same buried-key gap, but a correct
  fix there needs additional inner-UNION projection plumbing (the collected
  grouping expr must reference a column the inner `__union` actually projects) — an
  isolated `collect_grouping_keys` swap is **inert** (byte-identical to main;
  verified via branch-vs-main diff on the `#503` denorm shape). Filed as
  **follow-up #844**; NOT fixed here to keep the change honest and byte-locked.
  Adversarial review (general-purpose subagent): SUBSTANTIVE COUNT 1 (the missing
  site-3 lock), which on follow-through revealed the site-3 change was inert — hence
  the deferral. **Closes #637 (RETURN+WITH); also closes #600** (sub-defects 2
  `name`→`full_name` inline-map WHERE + 3 stDev-post-WITH were already fixed on main
  via #638/#551; sub-defect 1 == #637, now fixed for RETURN+WITH).

- 2026-08-01: **P-1 — `round()` matches Neo4j semantics (was ClickHouse banker's
  rounding)** (PR #848, branch `fix/round-neo4j-half-up-fidelity`). Silent-wrong
  openCypher fidelity bug from the same live hunt: `round`→CH `round` = HALF_EVEN
  (`round(2.5)=2`, `round(0.5)=0`). Verified against Neo4j 5.25
  `CypherFunctions.java`: Neo4j `round()` is TWO branches — 1-arg + 2-arg-`0` use
  `Math.round`=`floor(x+0.5)` (ties toward +∞), 2-arg d≠0 uses
  `BigDecimal.setScale(d, HALF_UP)` (away-from-zero on the shortest decimal
  string). New dialect helper `round_half_up_sql` (common.rs), routed Path A + C
  per Rule #7: CH 1-arg/prec-0 = `floor(x+0.5)`; CH 2-arg = decimal-domain HALF_UP
  via `toDecimal128(toString(x),18)` with a decimal `+0.5` (a Float64 0.5 promotes
  the product back to float → ties collapse down) and a `1e15` lazy-`if` guard
  against Decimal(38,18) overflow (exact — Float64 has no fractional bits past
  2^52 so round(x,d)==x there, as Neo4j returns); 3-arg explicit-mode falls
  through to CH native → LOUD Code 42 (not silent). Databricks byte-identical
  (Spark round already matches Neo4j). TWO rounds of adversarial review vs the
  Neo4j reference corrected the initial away-from-zero formula (wrong on negative
  ties) then a float-0.5 promotion bug (wrong at magnitude/precision). Live sweep
  (negatives/ties/prec-0/decimal/overflow/NULL/columns/WHERE-CTE) all match Neo4j;
  5 unit + 1 golden test assert correct values. Gate: fmt · clippy · 1611 lib · 244
  golden · corpus byte-identical (0 churn) · ratchet net-zero.


- 2026-08-01: **P-1 — OPTIONAL MATCH over FK-edge-to-node table drops unmatched
  anchor rows** (PR #845, branch `fix/optional-fk-edge-anchor-inversion`). Silent
  DATA LOSS found via a live silent-wrong bug hunt: `MATCH (u:User) OPTIONAL MATCH
  (u)-[:AUTHORED]->(p:Post)` returned 5 rows not 6 — post-less users vanished. The
  FROM anchor inverted to `FROM posts LEFT JOIN users` (mandatory `u` on the
  nullable side). Trigger: `AUTHORED` is an FK-edge whose edge table IS the to-node
  table (`table: posts`, `to_node: Post`) → `FkEdgeJoin{join_side:Left}`, whose
  natural FROM node is the RIGHT (optional) node, so `select_anchor` rooted FROM at
  the optional side; latent under non-optional (INNER symmetric), data loss under
  OPTIONAL/LEFT JOIN. Fix (3 files, schema-catalog dispatch per Rule #7, ratchet
  net-zero): new `JoinStrategy::natural_from_node_position()` (pattern_schema.rs);
  "signal 2" in inference.rs re-roots FROM at the required node when
  `natural_from == Some(Right)` && right-optional && left-required (only
  `FkEdgeJoin{join_side:Left}`); generalized the join_builder.rs FK-edge
  phantom-join dedup (skip an input rel join duplicating an edge already under a
  NODE-connection alias, guarded to keep two-hop self-chains + `pre_filter` named
  rels). Live-verified (6 rows w/ Eve NULL; LIKED/non-optional/reverse/self-ref/
  polymorphic all correct); `test_nested_optional_matches` golden had DROPPED the
  mandatory users table (real data loss) — now preserved. 8 goldens, +1 regression
  test. Adversarial review APPROVE-0 (both guards stress-tested live incl. an
  over-fire probe). **Two secondary silent-wrong findings from the same hunt
  (Cypher function-semantics fidelity):** (1) `round()` HALF_EVEN-vs-Neo4j —
  **FIXED #848** (see entry above); (2) integer division `7/2`→3.5 (Cypher int/int
  should be 3 via intDiv) — **FILED #847** (design-cycle: needs operand type
  inference in `Operator::Division`, column operands untypeable without schema
  column types).


- 2026-08-01: **SQL-IR Phase 2 — route Path C `ReduceExpr` through `reduce_fold_sql`**
  (PR #842, branch `refactor/sql-ir-path-c-reduce-fold-dialect`). Another of the
  independent Path-C drift slices the §3.5 investigation flagged as shippable
  without the full (design-cycle) C→A collapse — and the one genuine renderer leaf
  an empirical re-scan of all 1,119 Databricks goldens turned up after the "clean
  leaf pool exhausted" call (the multiIf/toFloat64/splitByChar/JSONExtract golden
  hits are all schema-config raw SQL, already dispositioned; `arrayFold` is real
  renderer output). Path C (`cte_extraction.rs::render_expr_to_sql_string`,
  CTE-build stage) hardcoded CH `arrayFold((acc, x) -> expr, list, init)` for
  `RenderExpr::ReduceExpr`, while Path A (`RenderExpr::to_sql`) already routes the
  identical strings through `common::reduce_fold_sql` (→ Spark `aggregate(list,
  init, (acc, x) -> expr)` on Databricks). Fix: added the one missing sibling
  re-export (`reduce_fold_sql` to `emitters/clickhouse/mod.rs`) + routed Path C's
  arm through it; `init_cast` Int64-wrapping preserved ahead of the call. CH
  byte-identical (helper default arm = same spelling, arg order 1:1); **0 golden
  churn** — no corpus query reaches ReduceExpr via the CTE-build path (the only
  corpus `reduce()` goes through Path A), so latent correctness hardening, same
  profile as #815. New `reduce_fold_sql_tests` module locks both dialect spellings.
  Gate: fmt · clippy · 1606 lib (+2) · corpus_sweep + sql_golden byte-identical ·
  ratchet net-zero. Adversarial review APPROVE-0 (byte-identity, arg order,
  init_cast, re-export reachability, churn honesty all independently verified).
  **Remaining Path-C hardcoded arm:** `POWER` (Exponentiation) at
  `cte_extraction.rs:1750` — parser-unreachable dead code (§6/P-6), zero urgency.


- 2026-07-30: **#689 bug 1 — heterogeneous from-side-polymorphic VLP recursive
  CTE joins the right end table** (#828, `c013c07a`). A directed VLP over a
  from-side-polymorphic edge (MEMBER_OF: `member_type ∈ {User, Group}`, target
  fixed = Group) emitted a malformed `WITH RECURSIVE`: the recursive arm joined
  the START table (`ds_users`, no `group_id` → CH 500) via the #142
  `recursive_end_table` heuristic, and reused the base `member_type='User'`
  filter (Group→Group hops never traversed). Fix gates both on a new narrow
  `is_from_side_polymorphic_cross_type` (cross-table + from-side label present +
  to-side label absent) → recursive arm uses `end_node_table` (ds_groups) + END
  label (`member_type='Group'`); base arm unchanged. Reviewer confirmed the gate
  is STRUCTURALLY safe (fixed target ⇒ end-type intermediates ⇒ END→END
  recursion, so no such shape ever needs start-table recursion) — #142 REPLY_OF
  (no from_label_column), complex-12 (multi_type_vlp_joins.rs), and the
  heterogeneous generator (`to_label_column.is_some()`) are all excluded by
  construction. Consolidated base+recursive filters onto one
  `generate_polymorphic_edge_filter_with_from_label(override)` (net −1
  `from_label_column` read). Golden churn: 14 data_security files, each a 2-line
  delta (`ds_users`→`ds_groups`; recursive `member_type` `'User'`→`'Group'`), 0
  outside `corpus/data_security/`. Unit test fails-when-reverted (both edits);
  live oracle via Group→Group fixture chain (User_5 → Group_5/10/15). Ratchet +1
  `to_label_column` (justified narrow gate). Review APPROVE-0. **Bug 2 (fixed hop
  to polymorphic target → triplicated identical UNION arms) split to #827** —
  design-cycle (type-inference `$any` candidate over-fan-out ignoring
  `to_label_values` + render table→label reverse-lookup picks alphabetically-first).

- 2026-07-30: **SQL-IR Phase 2 step-4 slice — `quote_identifier` routed through
  the dialect layer** (branch `refactor/sql-ir-path-c-quote-identifier-dialect`).
  First of the independent Path-C drift slices the §3.5 investigation flagged as
  shippable without the full (design-cycle) C→A collapse. Path C
  (`cte_extraction.rs`, CTE-build stage) quoted special-char identifiers via
  `common.rs::quote_identifier`, which hard-coded backticks for BOTH dialects;
  Path A (final SELECT) uses `FunctionMapper::quote_alias` (CH `"x"`, Spark
  `` `x` ``). On ClickHouse a single query therefore emitted BOTH `` t2.`id.orig_h` ``
  (CTE body) and `t1."id.orig_h"` (SELECT) for the same column — valid CH, no
  runtime bug, but exactly the dialect-routing drift the SQL-IR track targets (and
  wrong SQL for a future Postgres/DuckDB dialect). Made `quote_identifier`
  dialect-aware (route special-char names through `quote_alias`, plain names stay
  bare); `json_builder.rs::quote_column_name` (parallel column-*ref* quoter)
  delegates to it. `quote_json_key` untouched — its backticks are a load-bearing
  JSON key (verified live on Databricks). The one empirically-reachable C drift
  item (23 backtick special-cols in committed CH goldens); the hardcoded-arm
  (`POWER`/`arrayFold`/CASE) + InSubquery-placeholder items stay latent (0 corpus
  leak / join-planner-consumed before C renders). Churn: 2 CH goldens
  (col-ref backtick→double-quote), 0 Databricks. Updated the
  `pattern_union_quotes_dotted_physical_columns` regression guard +
  `quote_identifier`/`qualified_column` doctests to the CH-default double-quote
  spelling (invariant — dotted cols quoted, plain cols bare — unchanged). Gate:
  fmt · clippy clean · 1601 lib · 531 integration (0 unexpected churn) · ratchet
  net-zero · doctests. **Pre-existing follow-up surfaced:** `multi_type_vlp_joins.rs`
  emits `toString(n2.id.orig_h)` — an UNquoted dotted column (CH parse error),
  present on main, untouched here.

- 2026-07-29: **SQL-IR Phase 2 step 1 — retire Path B; route write payloads
  through canonical Path A** (#822, `683fb10c`). Completes the shippable Phase-2
  path-collapse (steps 1+2+3 done; only step-4 Path C, a separate design cycle,
  remains). NOT the "small warm-up" the plan assumed — investigation (traced +
  probed through the real write harness) overturned the static survey: Path B's
  sole caller (`render_expr_inline`'s `_ =>` fallback for write payloads) is
  GENUINELY LIVE (`SET a.age = a.age+1` → OperatorApplicationExp, `SET a.x = a.y`
  → PropertyAccessExp, both fall through it), and B-vs-A are byte-identical for
  every reachable write shape EXCEPT one: `SET x = a.id` on a RENAMED-node_id
  schema (`user_id`, no literal `id` property) leaks an unresolved `id` column —
  B emitted a broken literal `a.id` (non-existent column → runtime error); A,
  under the executor's task-local schema context (connection.rs:1301), resolves
  it to the real `user_id`. So B→A is strictly a latent-bug **fix** (open #411
  family). Deleted Path B (−95 lines) + unused import; shared
  `has_string_operand`/`flatten_addition_operands` kept (Path C uses them).
  **Adversarial review caught 1 real defect** the "byte-identical" claim missed:
  a single quote embedded in a string literal NESTED inside a concat reaches A's
  `Literal::String` arm (`to_sql_query.rs:6473`) via operand recursion, which did
  NO escaping → broken/injectable SQL. Root-caused DEEPER than the review scoped:
  a PRE-EXISTING latent bug in the canonical renderer (verified via `cg sql` that
  read-side `WHERE u.name = "O'Brien"` already emitted broken `'O'Brien'` on main).
  Fixed at root (`s.replace('\'', "''")`), fixing both the write regression AND
  the read-side injection bug; 0 golden churn (all `''` in corpus are empty
  strings, not escaped quotes); re-review APPROVE-0 (empirically refuted
  double-escape: `"a''b"` → `'a''''b'`, CREATE/params bypass the arm). Gate:
  fmt/clippy · 1601 lib · 238 golden (0 churn) · ratchet net-zero · corpus.
  **Phase-2 architectural payoff fully banked — all three drift sources (dual
  Operator enum, dead Path D, near-dead Path B) gone.** Remaining: step 4
  (Path C full-collapse, separate design cycle — CTE-build-stage timing).

- 2026-07-29: **SQL-IR Phase 2 step 3 — retire dead Path D (`LogicalExpr::to_sql`)**
  (#820, `45651b9d`). The design doc framed this as "collapse D→A via
  `TryFrom<LogicalExpr>`", but a reachability audit (2-agent corroborated) found
  Path D is **entirely dead in production** — there are two different `ToSql`
  traits, and prod uses Path A's (`render_plan::ToSql`→`String`); Path D's
  (`to_sql.rs::ToSql`→`Result`, `#[allow(dead_code)]`) is a closed self-referential
  cluster (`to_sql.rs` ↔ `translate_scalar_function` ↔ `view_query.rs`) reachable
  only from tests. So step 3 became a **deletion**, not a collapse: removed
  `to_sql.rs` + `view_query.rs` + `view_scan.rs` + `translate_scalar_function`/
  `translate_duration_function` + 16 unit tests + the re-export, **−1170 lines**.
  Kept the live `interval_expr_for_unit` (Path A duration handling) + `ch.`/`chagg.`
  machinery. Coverage preserved: **ported** the FINAL dialect-gating test to the
  live `FromTableItem::to_sql` path; the deleted passthrough tests are redundant
  (12 dedicated `passthrough/` tests) and duration is covered by `interval_*`
  goldens; **closed the one genuine gap** with a new `fn_datetime_epoch_millis`
  golden (`datetime({epochMillis:x})`→identity, the removed test's only prior
  coverage). Refreshed 2 stale comments. Review APPROVE-0 (deleted code verified
  dead workspace-wide; no lost coverage; clippy no `dead_code`). CH+DBX
  byte-identical, 0 churn beyond the 2 intended new goldens. Isolated
  `CARGO_TARGET_DIR` (shared target lock-contended). **Net of steps 2+3: the
  operator-render duplication AND the entire dead LogicalExpr-render path are
  gone — the drift source the SQL-IR track targets.** Remaining Phase-2: step 1
  (retire near-dead Path B, smaller) + step 4 (Path C, separate design cycle).

- 2026-07-29: **SQL-IR Phase 2 step 2 — unify the dual `Operator` enums** (#818,
  `188e4507`). `render_expr::Operator` and `query_planner::logical_expr::Operator`
  were byte-identical 24-variant copies (same variants/order/comments/derives).
  Made render's a `pub use` re-export of the planner enum (correct layering —
  render is the leaf that already depends on logical_expr), deleted the duplicate
  definition + the now-reflexive `TryFrom<LogicalOperator> for Operator` + its one
  caller (a plain move). Pure type unification, no output-path change: CH **and**
  Databricks byte-identical, **0 golden churn**; net −52 lines. Review APPROVE-0
  (derive-list identity, `Copy`/serde/name-resolution all verified). Refreshed the
  two now-stale `to_sql.rs`/`to_sql_query.rs` operator-consolidation TODOs (they
  cited the just-removed "two Operator types" blocker) to point at the real
  remaining step-3 work (bake LogicalExpr→RenderExpr, retire Path D). Ran in an
  isolated `CARGO_TARGET_DIR` (shared `/data/cargo-target-shared` was lock-contended
  by another project's build fleet). **Next SQL-IR: Phase-2 step 3** — collapse
  Path D→A via the existing `TryFrom<LogicalExpr>` (`render_expr.rs:1825`);
  re-plumb `function_translator` to bake scalar-fn args first, retire the dead
  ViewScan renderer. Or step 1 (retire near-dead Path B) as a smaller warm-up.

- 2026-07-29: **SQL-IR Phase 1 — RegexMatch routed through dialect helper**
  (#815, `1a57e817`). The last two hardcoded `match({}, {})` RegexMatch emission
  sites (`cte_extraction.rs` Path C, `pattern_comprehension_sql.rs`) now route
  through the pre-existing `common::regex_match_predicate` (CH `match(...)`,
  Databricks `rlike(...)`), matching the already-routed Path A/D sites and their
  own `StartsWith`/`EndsWith`/`Contains` siblings. Added `regex_match_predicate`
  to the `clickhouse` module re-export so `render_plan/` reaches it via the
  `clickhouse_query_generator` alias. CH byte-identical (helper default arm is
  the same `match(...)`), **0 golden churn** (no committed Databricks golden
  reached these sites — latent hardening, not a locked-bad leak). Full gate green
  (1612 lib + corpus_sweep + sql_golden + ratchet net-zero); adversarial review
  APPROVE-0. **This closes the clean mechanical operator/function-name leaf pool
  for Phase 1** — see the P-6 SQL-IR bullet (§2) for why the remaining Databricks
  golden leaks (Exponentiation = parser-unreachable dead code; JSONExtractString/
  toFloat64/splitByChar = schema-config raw-SQL, a design question) are NOT leaf
  slices.

- 2026-07-29: **SQL-IR Phase 1 — STARTS WITH / ENDS WITH routed through dialect
  helpers** (#813, `904924e5`). Cypher `STARTS WITH` / `ENDS WITH` were hardcoded
  to CH `startsWith(...)` / `endsWith(...)` camelCase at **6 emission sites across
  all four render paths** (3× `to_sql_query.rs`, `cte_extraction.rs`, `to_sql.rs`
  LogicalExpr arm, `pattern_comprehension_sql.rs`); Spark spells them lowercase
  (`startswith`/`endswith`, DBR 11.3 LTS+, same arg order). Added
  `common::starts_with_predicate`/`ends_with_predicate`, siblings of the existing
  `contains_predicate`/`regex_match_predicate` that already sit beside these sites.
  CH byte-identical (0 `.clickhouse.sql` churn); 1 `.databricks.sql` golden
  regenerated. **Adversarial review caught a 6th site** (Path D `to_sql.rs`, whose
  `Contains` sibling was already routed via #364) that the initial pass counted as
  5 — fixed + confirming re-review APPROVE-0. Spark resolves builtin names
  case-insensitively so the leak wasn't a hard break, but it closes the
  consistency gap. Gate: fmt/clippy, 1612 lib + 531 integration, ratchet net-zero.
  **Next SQL-IR leaf:** RegexMatch `match(...)` still hardcoded in `cte_extraction.rs`
  (~:1790) + `pattern_comprehension_sql.rs` (~:1747) — a `regex_match_predicate`
  helper already exists and Paths A/D use it, so it's a mechanical 2-site routing
  (Spark needs `rlike`); 0 current golden leak (latent, correctness-hardening).
  `POWER`/`Exponentiation` in `cte_extraction.rs:1750` is a possible bundled
  sibling.

- 2026-07-29: **SQL-IR Phase 1 — simple-CASE routed through a dialect-aware
  helper** (#811, `b2352d00`; P-6 SQL-IR track resumed). `RenderExpr::Case` with
  a subject expr (Cypher *simple* CASE) was hardcoded to ClickHouse's
  `caseWithExpression(expr, v1, r1, …, default)` at its sole emission site
  (`to_sql_query.rs`) — a function that does not exist in Spark, so it leaked
  into **7 committed `.databricks.sql` goldens** (a latent Spark bug of the same
  class as the 2026-06-28 sweep: a CH-only function inlined instead of routed
  through the dialect layer). Added `common::simple_case_sql()`, mirroring the
  existing `reduce_fold_sql`/`contains_predicate`/`regex_match_predicate`
  dialect helpers: CH keeps `caseWithExpression(...)` (byte-identical, 0
  `.clickhouse.sql` churn); Databricks emits standard `CASE expr WHEN v THEN r …
  ELSE d END`. Nested-CASE + empty-ELSE (`ELSE NULL`) + List-fallback preserved.
  New unit tests lock both spellings. Gate: fmt/clippy, 1610 lib + 531
  integration, ratchet net-zero; adversarial review APPROVE-0 (1 non-blocking
  NIT: no golden exercises a Spark simple-CASE returning an array — behavior
  carried over from main, no regression). **State of the track** (verified live,
  not doc-claimed): `FunctionMapper` has already grown into a de-facto `Dialect`
  trait (26 methods), dialect is plumbed via task-local `get_current_dialect()`
  and read at emission time, Databricks output is reachable, 218-case dual-dialect
  `sql_ir/` golden net + 1,119-case corpus both locked. **Next SQL-IR leaf:**
  `startsWith`/`endsWith` (CH camelCase → Spark `startswith`/`endswith`; 5
  emission sites across Paths A + C + pattern_comprehension), then
  `JSON_VALUE`/`splitByChar` single-golden leaks, then the operator-symbol table
  is deliberately SKIPPED (`=`/`<>`/`AND` are dialect-neutral — pure churn).

- 2026-07-29: **#802 — FK-edge exact-bound VLP duplicate endpoint alias (Code
  179)** (composite-id follow-up). `(a)-[:PARENT*2..2]->(b)` on a self-ref
  FK-edge table emitted `FROM t AS b JOIN t AS b` (endpoint alias twice) + start
  alias `a` undefined → Code 179. **Root:** `extract_joins` roots the expander
  chain at the START node, but `select_anchor`'s alphabetical tie-break picks the
  END node as `anchor_table` for a self-ref table (both endpoints = same table),
  and `extract_from` used it — FROM disagreed with the joins. **Fix**
  (`from_builder.rs`): a flat exact VLP (N≥2) derives FROM from the same
  `expand_fixed_length_joins_with_context` that builds its joins, so they agree
  by construction. Normal/Polymorphic already root at start → byte-identical
  (zero corpus churn). **Un-masked** a composite-key defect in the FK-edge/
  multi-type legacy `start != end` guard (`filter_builder.rs`): `start_id`/
  `end_id` came from the single-`String` `ViewScan.id_column`, truncating
  composite keys to `a.region <> b.region` — live-verified to drop a valid
  grandparent path (0 rows vs 1). Made composite-aware via `node_id.columns()`;
  single-col yields one-element vectors → byte-identical. **Known residual**
  (documented): the legacy start!=end guard is stricter than Cypher trail
  semantics on CYCLIC self-ref data — pre-existing, shared single/composite,
  tracked with the #598-followup rel-uniqueness rewrite. Live-verified on
  ClickHouse (single + composite fixtures); adversarial review before merge.
  Goldens: regenerated `composite_self_ref_fk` exact-two-hop (was locking the
  buggy form); added single-col `fk_edge_self_ref` exact two/three-hop.
- 2026-07-28: **#606 VLP relationship-uniqueness — remaining live arms** (USER:
  "finish bug-driven refactoring if more exist"). After #598/#709/#712, three
  Explore passes mapped the residual arms. **Only one was a live bug:**
  - **#807** — multi-type VLP (`multi_type_vlp_joins.rs`) was OVER-counting (flat
    JOIN+UNION with no uniqueness predicate → permitted edge reuse). Fixed with a
    same-rel-type pairwise edge-inequality guard; shortestPath excluded
    (node-unique suffices). Live-verified against a cyclic oracle (9→8, 15→10);
    review APPROVE-0. Follow-up **#806** (flat-path edge identity uses `(from,to)`
    only → parallel-edge collapse; pre-existing, matches single-type flat).
  - **#808 (PARTIALLY SHIPPED — deleted the structurally-dead arms; mixed
    deferred)** — the node-unique VLP recursive generators split into two classes
    under adversarial review:
    - **Structurally dead → DELETED:** `generate_denormalized_{base,recursive}_case`
      and `generate_heterogeneous_polymorphic_recursive_case`, plus the 2 helpers
      they orphaned (`generate_polymorphic_edge_filter_intermediate`,
      `map_denormalized_property`). Fully-denormalized VLP is intercepted by
      `cte_manager`'s `DenormalizedCteStrategy` (`mod.rs:3261`) BEFORE the generator
      is even constructed, so its arm can never fire; heterogeneous-polymorphic
      paths return early via `generate_heterogeneous_polymorphic_sql()` (a separate
      two-CTE builder, RETAINED) before the recursive dispatcher runs. Re-verified
      empirically: instrumented dispatcher fired 410× in corpus_sweep, these arms
      0×, no dispatch ever had `is_denormalized`/`is_heterogeneous_polymorphic_path()`
      true. Zero behavior change (corpus_sweep + sql_golden 0-churn).
    - **Reachable-but-corpus-empty → KEPT:** the `mixed` arms
      (`generate_mixed_{base,recursive}_case`). Correcting the earlier claim that
      "mixed VLP never routes through this struct" — it DOES: when
      `start_is_denormalized != end_is_denormalized`, `cte_manager` builds the
      generator via `new_mixed` (`mod.rs:3288-3313`) and calls `generate_cte()`.
      The arms are dead only because no corpus/test schema exercises a mixed-access
      VLP, NOT structurally. Per #808's own disposition, a reachable shape is
      option-(b) "add coverage first, verify the under-count live, then apply the
      edge-uniqueness recipe" — NOT option-(a) delete. Deleting them would silently
      swap a mixed query onto the standard 3-way-join arm (unverifiable without live
      CH). Deferred until a mixed-VLP fixture exists.
    shortestPath/weighted/zero-hop(#628) legitimately node-unique.
- 2026-07-28: **Composite-id emission threading — abstracted 4 "bug-driven"
  issues into one bounded refactor** (USER: "abstract the bug-driven refactors,
  or SQL-IR"). Root: a composite `Identifier` was stringified (`Display`/
  `to_string` → one bogus `"c1, c2"` column) or truncated (`first_column`) at
  SQL emission instead of threaded as a column vector through the canonical
  helpers (`to_sql_equality`/`emit_id_expr`/`from_comma_separated`, already used
  by #646/#672). Five PRs, each byte-guarded + adversarially reviewed:
  - **#800** (Slice 0) — characterization: composite FK-edge schemas into the
    corpus (they had zero goldens); locks malformed output so each fix's diff is
    the malformed→correct transition.
  - **#801** (Slice 1, closes #713) — FK-edge VLP generators composite-aware
    (`variable_length_cte.rs`); live-verified Code 62 → correct ancestor pairs.
  - **#803** (Slice 2) — flat exact-bound VLP JOIN-ON composite-aware
    (`cte_extraction.rs`: `vlp_join_eq_conditions` + `extract_node_info` sources
    composite `node_id` instead of lossy `ViewScan.id_column`, avoiding a
    loud→silent-wrong regression).
  - **#804** (Slice 3, closes #604) — relationship-uniqueness WHERE guard
    composite-aware (`filter_builder.rs`); live-verified `*2..2` returns exactly
    the correct 2-hop chain.
  - Byte-safety linchpin: `quote_identifier` is a no-op on plain names, so
    single-column ids degrade byte-identically — zero single-col golden churn
    across all slices. Surfaced + filed **#802** (FK-edge exact-bound duplicate
    endpoint alias, Code 179, pre-existing on single-col too). Out of scope
    (separate design cycles): #627 (composed/adjacent composite VLP — CTE must
    expose per-column ids), #672 part-1 (wrong-order silent zip — needs
    undeclared FK↔PK semantics).
- 2026-07-28: **Phase-3 §6.3 — `pattern_schema.rs` `#![allow(dead_code)]`
  audit** (P-3 lane; PR #799). Lifted the blanket module allow; it surfaced
  exactly 5 genuinely-dead private/test items (the superseded
  `node_strategy_for_position` helper — `build_node_strategies` uses an inline
  `match edge_pattern` instead — plus 4 unused `make_*` test-fixture builders).
  The module's public API is `pub`-reachable and lint-exempt, so the allow
  masked nothing there. Deleted the 5 items + 3 now-unused test imports, fixed
  the stale comment that named the deleted helper, removed the blanket allow;
  module now builds warning-free. Net −191 lines, no production behavior change,
  ratchet net-zero (`src/graph_catalog/` is excluded from axis counting).
  **This closes Phase 3** — no §6.2/§6.3 items remain.
- 2026-07-28: **corpus_sweep golden normalize() — anonymize
  `pattern_union_t<n>` counter** (PR #798). The golden normalizer's `\bt\d+\b`
  remap could not reach the render counter inside a `pattern_union_t<n>` CTE
  name (the `t` sits after `_`, no word boundary), so that global counter leaked
  into 2 goldens and made them render-order-dependent — a flaky `corpus_sweep`
  failure (`denorm_selfloop_multitype/path_multitype_expand`, added #795) that
  passed alone but failed mid-suite and on main. Added an explicit
  `pattern_union_t\d+` remap and regenerated the 2 goldens; keeps byte-lock
  coverage instead of excluding the entry.
- 2026-07-28: **Phase-3 §6.2 slice-2 — rename `is_fk_edge` proxy →
  `rel_table_is_end_node`; ratchet 4→1** (P-3 lane; PR #796). The multi-type VLP
  JOIN emitter's local `let is_fk_edge = rel_table == end_table`
  (`multi_type_vlp_joins.rs:~764/~1471`) was misleadingly named — it invited a
  migration to canonical `RelationshipSchema.is_fk_edge` that would emit WRONG
  SQL. A transition-assert over the corpus (13,632 hits) found **14 real
  divergences in BOTH directions**: denorm coupled self-loops (proxy fuses,
  canon says not-FK) and reversed FK hops (proxy two-joins the separate target,
  canon says FK). The proxy is a LOCAL per-hop direction-aware shape test, not a
  schema-level classification — correct-and-different, so NOT migrated. Instead:
  renamed both sites + documented the divergence inline (byte-identical) and
  ratcheted the file's `is_fk_edge` count 4→1 (one intentional comment pointer
  kept). Adversarial review APPROVE-0 (specifically confirmed the divergence is
  real in both directions). Gate: 1608 lib, corpus_sweep + sql_golden
  byte-identical, ratchet down 4→1, clippy/fmt clean.
- 2026-07-28: **Phase-3 §6.2 slice-4 — denorm self-loop multi-type fixture,
  divergence-coverage** (P-3 lane; PR #795). Closed the coverage gap the earlier
  slice-4 transition-assert left (148 vacuous `false/false` hits — the corpus
  never reached the multi-label `pattern_combinations` UNION path with a denorm
  endpoint). Built `schemas/test/denorm_selfloop_multitype.yaml` (reachable
  denorm self-loop, both direction maps, two edge types → multi-type expansion);
  an in-process transition-assert over it shows **inline == canonical on every
  hit**, because the only divergent input (a partial-map self-loop) is
  validator-rejected (`config.rs:2058`). Converts slice-4 from *argued* to
  *positively-covered* non-migration: new lib test + 4 corpus entries / 8
  byte-locked goldens (zero churn to existing goldens). Adversarial review
  APPROVE-0. Test/docs only, no production `src/` change.
- 2026-07-28: **Phase-3 §6.2 slice-3 — `first_col` → `Identifier::first_column`**
  (P-3 lane; PR #793). Deleted a duplicated column-resolution helper, routed 14
  sites through the canonical API. Byte-identical.

- 2026-07-28: **Phase-4 §7.2 F6-partial — delete dead `set_property_mapping` +
  correct stale forward-resolution docs** (P-4 lane; PR #791). Byte-identical
  code deletion: the setter had **zero callers** (since F0 the `property_mapping`
  is threaded at `define_*` construction time, so no post-hoc patch-in is
  needed). Also corrected `FORWARD_RESOLUTION_PLAN.md`, whose §1.1/§1.2 still
  described the **pre-F0** state and mis-scoped F2b: M1 is no longer starved
  (#592 fixed by F0), M3 has 4+2 call sites (not "one path"), and the F0 M1-vs-M3
  transition-assert already exists and passes — so **F2b's assert-half is done
  and its fold-half is a design cycle, not a byte-identical slice**. A scout +
  adversarial reviewer both verified the deletion and every corrected doc claim
  against source (APPROVE, 0 defects). Gate: 1607 lib + 531 integration
  (corpus_sweep + sql_golden byte-identical), ratchet net-zero, clippy clean from
  a wiped incremental cache, fmt clean. **This is the last bounded byte-identical
  slice in the whole refactoring mission** — Phases 0–2, §7.1, and every
  shippable §7.2 slice (F0–F4, F6-partial) are now done; what remains in §7.2 is
  design-cycle/blocked/deferred, bug-driven. See `FORWARD_RESOLUTION_PLAN.md`
  §1.1/F2b/F6 and `REFACTORING_SAFETY_PLAN.md` §7.1/§7.2.
- 2026-07-28: **Phase-4 §7.1 — FORMALLY CLOSED** (P-3 lane; docs-only). The
  entangled-core decomposition is declared complete: across 43 byte-identical/
  behavior-preserving PRs (#740–#785) `replace_v2` went 1070 → 212 ln and
  `build_chained_with_match_cte_plan` went 5478 → 850 ln, with every *extractable*
  helper at/under the ~500-ln target (next-largest 459/445/440). The lone survivor
  — `build_chained` at 850 ln — is its irreducible mutable-state frame (~10 `let mut`
  accumulators + the `while`/`'alias_loop` glue between already-extracted STEP
  helpers + tracing), not an un-decomposed body; the readability half of the exit
  criterion (STEP pipeline readable top-to-bottom) is met. The only path under 500
  is lifting the whole `'alias_loop` into one ~19-param signal-return helper, which
  trades param-plumbing for line count with no readability gain — **explicitly
  declined**. The remaining substantial refactor work is now **Phase-4 §7.2**
  (forward resolution / delete `reverse_mapping`), tracked as the **P-4 lane**
  (`FORWARD_RESOLUTION_PLAN.md`). See `REFACTORING_SAFETY_PLAN.md` §7.1.
- 2026-07-28: **Phase-4 §7.1 — over-budget extracted helpers brought under ~500
  ln** (P-3 lane; PRs #782–#785). The inner-loop decomposition (#770–#780) left
  several of the helpers it produced still over the module-wide exit target; each
  is now sub-decomposed one byte-identical slice per PR (scout-verified seam +
  adversarial review, both agent-driven in parallel). `resolve_cross_table_with_cte_joins`
  775 → **355** (extracted `generate_vlp_with_cte_join_conditions` #782 + the
  two-`return Err` `restructure_post_with_optional_or_insert_cte_join` #783);
  `apply_with_items_projection` 660 → **459** (extracted the pure
  `build_with_projection_select_items` flat_map builder, #784);
  `restructure_post_with_optional_match` 529 → **71** (extracted the 445-ln
  `restructure_optional_cte_bridge` via a borrow-clean pre-clone-`from_name` seam,
  #785). **`build_chained_with_match_cte_plan` (850 ln) is now the SOLE function
  in `with_to_cte/mod.rs` over ~500 ln** — the deliberately-parked accumulator
  frame; every §7.1-extracted helper is at/under target. Module-wide exit
  criterion met except that one frame. Method note banked: whitespace-free
  char-multiset identity is a reliable proof that fmt-reflow-heavy dedents added
  no semantic token. See `REFACTORING_SAFETY_PLAN.md` §7.1.
- 2026-07-28: **Phase-4 §7.1 — `build_chained` inner render-loop FULLY
  DECOMPOSED** (P-3 lane; PRs #770–#780, 11 more since the earlier 2026-07-28
  entry). The last hard core — the `for with_plan in with_plans` render-loop —
  is now a linear pipeline of 14 module-level STEP `fn`s (P1–P14), called
  top-to-bottom. The anticipated `WithCteBuildState`/`ControlFlow` machinery was
  **not needed**: only the passthrough-collapse phase owned the labeled
  `break 'alias_loop`, extracted via a **signal-return** (`try_collapse_passthrough_with
  → RenderPlanBuilderResult<bool>`; `Ok(true)` → caller `if …? { break 'alias_loop; }`,
  label binding stays at the call site). The other phases used the standard
  `&mut`-param + `-> RenderPlanBuilderResult<T>` technique. Notable slices:
  `apply_with_items_projection` (P9, ~660 ln, 16 params — the largest),
  `apply_with_order_by_skip_limit_where` (P10+P11), `extract_with_plan_parts`
  (P2, pure 8-tuple with a borrowed first element). Every slice verified
  byte-identical-modulo-mechanical by corpus_sweep + sql_golden (no golden
  regen) and adversarially subagent-reviewed clean. `build_chained` **5478 →
  850 lines** (84% reduction); the readability half of the exit criterion is met.
  Remaining to reach ~500: the accumulator-heavy pre-loop setup + `'alias_loop`
  glue, most of which is the loop's irreducible mutable-state frame. See
  `REFACTORING_SAFETY_PLAN.md` §7.1.
- 2026-07-28: **Phase-4 §7.1 — `replace_with_clause_with_cte_reference_v2` DONE
  + `build_chained` clean regions all extracted** (P-3 lane; PRs #749–#768, 20
  more since the 2026-07-27 entry). `replace_v2` (the second §7.1 giant) is fully
  decomposed **~1070 → 212 lines** (under the ~500-ln exit criterion): 4 nested
  `fn`s hoisted to module level (#763, one renamed `extract_node_label_from_arc_plan`
  to dodge a `cte_extraction.rs` same-name/different-sig collision) + its 5 large
  `match`-arms (Projection/GraphJoins/GraphRel/Union/WithClause) extracted into
  `replace_v2_<variant>_arm` handlers (#764–#768) — each arm reads only the 4 fn
  params + its binding (pure recursive rewriter, no shared accumulator). SHADOW
  LESSON (#767): the fn's `use logical_plan::*` shadows module-level type imports
  (`logical_plan::Union` ≠ `render_plan::Union`), so hoisted handlers must
  re-import the exact `logical_plan::` types. Meanwhile `build_chained` is 5478 →
  **~2646 lines**: every control-flow-clean main-loop sub-region is now extracted
  (#749–#762, incl. `apply_pattern_comprehensions` ~446 ln and the RefCell-slice
  `build_with_cte_property_mapping` — no `WithCteBuildState` struct needed, pass
  `&refcell.borrow()`). **Remaining:** only `build_chained`'s ~1970-ln inner
  `for with_plan in with_plans` render-loop — its phases mutate shared locals +
  contain `break 'alias_loop`/`?` that escape the loop, needing a
  ControlFlow/action-enum or context-struct (step-change; deferred pending
  go-ahead). See `REFACTORING_SAFETY_PLAN.md` §7.1.
- 2026-07-27: **Phase-4 §7.1 — `build_chained_with_match_cte_plan` decomposition
  IN PROGRESS** (P-3 lane, follows P2.10). 11 PRs merged (#740–#750), each one
  self-contained region → named `fn` sibling, all gate-green (1607 lib + 529
  integration byte-identical, no `UPDATE_GOLDEN`, + ratchet net-zero) and
  adversarial-reviewed clean. `build_chained` **5478 → ~4270 lines**.
  Structural correction: the giant has no nested `fn`s / `RefCell`-capturing
  closures (the protocol's anticipated shape) — it's a linear setup → `while`
  loop → finalization-tail body, so each self-contained region is hoisted
  instead. **Finalization tail: fully decomposed** (9 PRs #740–#748;
  `reconcile_stale_cte_name_references` … `prune_joins_covered_by_last_cte`;
  slices #746/#748 behavior-preserving-not-byte-identical via `return Err`→
  `RenderPlanBuilderResult<()>`+call-site-`?`, goldens still byte-identical).
  **Main loop: teardown started** (2 PRs #749 `build_iteration_worklist`, #750
  `prepare_with_plans_and_pre_aliases`); the `'alias_loop` interior (~3894 ln,
  dense control flow) proceeds one control-flow-clean sub-region per PR.
  Remaining: rest of the `'alias_loop`, then `replace_with_clause_with_cte_
  reference_v2`. See `REFACTORING_SAFETY_PLAN.md` §7.1.
- 2026-07-27: **P2.10 dead_code tail — `cte_extraction` (§5.3 bullet COMPLETE)**
  (P-3 refactor lane) — final slice. Despite being the largest tail file (~8.5K
  lines), `cte_extraction.rs` carried only **4 dead private free fns**:
  `extract_node_alias` (recursive — its self-calls fool naive grep, dead by
  reachability closure), `table_to_id_column_for_label`,
  `get_relationship_columns_from_schema`, `get_node_info_from_schema`. The live
  sibling `get_relationship_columns_by_table` sat *between* the last three and was
  preserved. Removed them + the blanket `#![allow(dead_code)]`; no orphaned imports
  (the deleted fns called only live shared helpers). Verified from a WIPED
  incremental cache (#736 lesson). **No module-level `#![allow(dead_code)]` remains
  anywhere under `src/render_plan/` — the §5.3 dead_code bullet is closed.**
  Behavior-identical: `corpus_sweep` + `sql_golden` byte-identical, 1607 lib + 529
  integration + ratchet net-zero (deleted fns carried no axis tokens), fmt/clippy
  clean, warning-free lib + `--tests`.

- 2026-07-27: **P2.10 dead_code tail — `cte_generation`** (P-3 refactor lane) —
  third slice of the §5.3 tail. Removed the blanket `#![allow(dead_code)]` and
  deleted two fully-dead sub-features on `CteGenerationContext`: the
  `variable_length_properties` field + `get_properties`/`with_properties`, and the
  `start_cypher_alias`/`end_cypher_alias` fields + getters + test-only setters
  (whose only callers were 7 `.with_*_cypher_alias(…)` builder chains in
  `cte_manager` tests — removed with them, as the values were set-but-never-read).
  Also deleted 7 genuinely-dead free fns (`extract_node_label_from_plan`,
  `analyze_property_requirements`, `extract_var_len_properties`,
  `extract_alias_from_plan`, `get_variable_length_info`,
  `map_property_to_column_with_schema_context`, `get_node_schema_by_table` — some
  formed an internal *mutually-dead* cluster reachable only from each other, which
  the reachability closure catches but grep does not). **Scope decision (user):**
  the two half-dead getters `get_filter` / `get_fixed_length_joins` were KEPT
  behind a scoped `#[allow(dead_code)]` with an explanatory comment — their sibling
  setters (`with_filter`, `set_fixed_length_joins`) are live-called from
  `cte_extraction`, so deleting the getters would make the fields write-only and
  cascade a dead-code warning onto production code; that write-path pruning is a
  separate, deferred decision. `with_schema` was gated `#[cfg(test)]`
  (test-only-live — flagged dead by the clean lib compile since its callers are all
  `#[cfg(test)]`). −302 lines. Verified from a WIPED incremental cache (#736
  lesson). Behavior-identical: `corpus_sweep` + `sql_golden` byte-identical, 1607
  lib + 529 integration + ratchet net-zero, fmt/clippy clean, warning-free
  lib + `--tests`. Remaining tail: `cte_extraction.rs` (large central file).

- 2026-07-27: **P2.10 dead_code tail — delete `expression_utils` + `filter_pipeline`
  dead cluster** (P-3 refactor lane) — second slice of the §5.3 tail. The two
  files were a single cross-referencing dead cluster: `filter_pipeline`'s 5 unused
  VLP rewrite fns (`clean_last_node_filters`, `rewrite_expr_for_var_len_cte`,
  `rewrite_vlp_internal_to_cypher_alias`, `rewrite_expr_for_mixed_denormalized_cte`,
  `rewrite_labels_subscript_for_multi_type_vlp`) were the SOLE callers of
  `expression_utils`'s dead `VLPExprRewriter` / `AliasRewriter` /
  `property_access_expr`, so the dead surface only became visible with both blanket
  `#![allow(dead_code)]` removed at once. Deleted those + `CTERewriteContext`,
  `MutablePropertyColumnRewriter`, `create_property_access` (the analyzer-local
  `create_property_access` test helper is a DISTINCT fn — returns `LogicalExpr`,
  untouched), the orphaned imports, and 3 stale comments naming the deleted
  `VLPExprRewriter` (plan_ctx / query_context / cte_extraction). LIVE kept:
  `categorize_filters`, `CategorizedFilters`, `ExprVisitor`, `references_alias`,
  `rewrite_aliases`, `contains_string_literal`, `has_string_operand`,
  `flatten_addition_operands`. −458 lines. **Applied the #736 lesson**: verified
  from a WIPED incremental cache so no newly-orphaned type slips past. Ratchet:
  −10 `is_denormalized` schema-axis tokens (genuine reduction; baseline
  regenerated to lock it). Behavior-identical: `corpus_sweep` + `sql_golden`
  byte-identical, 1607 lib (kept `test_references_alias` for the still-live fn;
  dropped only the 2 dead-fn tests that rode in the deleted block) + 529
  integration + ratchet green, fmt/clippy clean,
  warning-free lib + `--tests`.

- 2026-07-27: **P2.10 hotfix — orphaned `PathFunctionRewriter`** (#736, `8aae2459`)
  — the #734 sweep deleted the path-function rewriter fns but left their sole
  consumer, the `PathFunctionRewriter` visitor struct + its `ExprVisitor` impl,
  orphaned. A never-constructed struct is not an `E0425` name-resolution error, so
  it never surfaced in the list the sweep was driven from, and local *incremental*
  clippy masked it — CI's from-clean `cargo clippy --all-targets -- -D warnings`
  correctly flagged it, turning **main red at `2641ae61`**. Fix: delete the
  orphaned struct + impl + its 2 now-unused imports (`Column`, `ExprVisitor`).
  **Method fix banked**: after any dead-code deletion, also hunt orphaned
  types/impls the deleted code exclusively used, and always re-run the gate under a
  wiped incremental cache before merging. Verified from-clean: clippy clean, 1612
  lib + 529 integration byte-identical, ratchet net-zero.

- 2026-07-27: **P2.10 dead_code tail — delete `feature_flags.rs`** (P-3 refactor
  lane) — first slice of the §5.3 dead_code tail after `plan_builder_helpers`.
  `render_plan/feature_flags.rs` was completed-refactoring rollback scaffolding
  (`PlanBuilderFeatureFlags` + `PLAN_BUILDER_FEATURE_FLAGS` env parser) whose
  blanket `#![allow(dead_code)]` masked a module with **zero production
  references** — `mod feature_flags;` in `render_plan/mod.rs` was its only mention
  outside its own unit tests, and every extraction the flags gated is now a
  permanent module move (so the flags gate nothing and the "rollback" is
  illusory). Deleted the file + its `mod` line wholesale (−141 lines). Behavior-
  identical: `corpus_sweep` + `sql_golden` byte-identical, 1609 lib (−3 = the
  module's own self-tests) + 529 integration + ratchet net-zero, fmt/clippy clean,
  warning-free lib + `--tests`. Remaining tail: `filter_pipeline` +
  `cte_generation` + `expression_utils` (a single cross-referencing dead cluster,
  swept together next) and `cte_extraction` (large central file, its own pass).

- 2026-07-27: **P2.10 dead_code sweep — `plan_builder_helpers.rs`** (P-3 refactor
  lane) — removed the module-level `#![allow(dead_code)]`, which had masked 31
  functions. Triaged rigorously by **call-graph reachability closure** (roots =
  every compiler-live fn ∪ every test fn; a fn is dead iff no path reaches it):
  **29 deleted** (genuinely dead — all `pub(super)`/private, zero qualified-path
  callers, unreachable from any live/test root; notably several were dead
  *duplicates* whose live twins live in `cte_extraction.rs` / `to_sql_query.rs`,
  e.g. `get_node_info_from_schema`, `get_relationship_columns_by_table`,
  `rewrite_fixed_path_functions`), **2 gated `#[cfg(test)]`**
  (`rewrite_with_aliases_to_cte`, `generate_polymorphic_edge_filters` — test-only-live,
  exercised solely by unit tests). **Method note**: first attempts at line-span
  deletion clipped a block comment / left orphaned `}` (brace-counter drifted on
  char literals); the robust cut used the file's column-0 `}` close-brace invariant
  + verified no target span contained another top-level `fn`. File 7,311 → 6,348
  lines. Behavior-identical: `corpus_sweep` + `sql_golden` byte-identical, 1612 lib
  + 529 integration + ratchet net-zero (deleted fns carried no tracked axis tokens,
  so no baseline change), fmt/clippy clean, warning-free lib + `--tests`. Remaining
  §5.3 dead_code tail: blanket allows in `filter_pipeline`, `cte_generation`,
  `expression_utils`, `cte_extraction`, `feature_flags`.

- 2026-07-27: **P2.10 import hygiene — slice 3 (final globs; §5.3 glob bullet
  CLOSED)** (P-3 refactor lane) — replaced the last three `plan_builder_helpers::*`
  / `alias_utils::*` globs (`filter_builder.rs` ×1, `plan_builder.rs` ×2) with named
  imports: filter_builder gets 7 from `plan_builder_helpers`
  (`apply_property_mapping_to_expr`, `collect_graphrel_predicates`,
  `collect_schema_filters`, `collect_schema_filters_with_alias`, `extract_id_column`,
  `extract_table_name`, `is_node_denormalized`); plan_builder gets 5 from
  `plan_builder_helpers` (`apply_property_mapping_to_expr`,
  `combine_predicates_with_and_logical`, `extract_end_node_id_column`,
  `references_only_alias_logical`, `split_and_predicates_logical`) +
  `strip_database_prefix` from `alias_utils`. Also dropped a stale
  `#[allow(unused_imports)]` on plan_builder's old glob. **No
  `plan_builder_helpers::*` / `alias_utils::*` glob remains anywhere in `src/`** —
  §5.3's glob-import bullet is fully closed. Same compiler-as-enumerator method;
  both files have no test module so all names are production. Behavior-identical:
  `corpus_sweep` + `sql_golden` byte-identical, 1612 lib + 529 integration + ratchet
  net-zero, fmt/clippy clean, warning-free lib + `--tests`. Remaining §5.3 items:
  the utils↔plan_builder cycle note (already mechanical/mostly-satisfied) + stale
  header docstrings; the `dead_code` allow sweep is P2.10's remaining tail.

- 2026-07-27: **P2.10 import hygiene — slice 2 (`plan_builder_utils` globs → named)**
  (P-3 refactor lane) — replaced the two `*` globs in
  `render_plan/plan_builder_utils.rs` with the 6 names the file uses:
  `denorm_scan_cte_anchor_id_property`, `denorm_scan_cte_anchor_properties`,
  `extract_parameterized_table_ref`, `extract_table_name`,
  `get_graph_rel_from_plan` (from `plan_builder_helpers`) + `find_label_for_alias`
  (from `alias_utils`), plus `get_anchor_alias_from_plan`/`strip_database_prefix`
  gated `#[cfg(test)]`. **This slice caught the exact shadowing hazard §5.3 warns
  about**: those last two names *looked* glob-provided but are used only by the
  file's test module — the glob masked that they weren't part of the production
  surface. Same compiler-as-enumerator method as slice 1 (grep under-counts the
  `pub(super) fn` surface). Behavior-identical: `corpus_sweep` + `sql_golden`
  byte-identical, 1612 lib + 529 integration + ratchet net-zero, fmt/clippy clean,
  warning-free lib AND `--tests` builds. Remaining target globs live in
  `filter_builder.rs` + `plan_builder.rs` (3 total) — next slice.

- 2026-07-27: **P2.10 import hygiene — slice 1 (`with_to_cte` globs → named)**
  (P-3 refactor lane) — replaced the two `*` glob imports in
  `render_plan/with_to_cte/mod.rs` (`plan_builder_helpers::*`,
  `utils::alias_utils::*`) with the exact 6-name set the moved bodies actually use
  (`combine_optional_filters_with_and`, `extract_predicates_for_alias_logical`,
  `has_with_clause_in_graph_rel`, `rewrite_logical_path_functions`;
  `collect_aliases_from_plan`, `find_cte_reference_alias`), per
  REFACTORING_SAFETY_PLAN §5.3 ("named imports so shadowing becomes visible").
  **Method note**: grep under-counted the glob surface (4 vs the real 6) because
  `plan_builder_helpers` exports many `pub(super) fn` that a `pub fn` grep misses —
  disabled the globs and let the compiler enumerate the unresolved names (the
  E0282 "type annotations needed" cascade was fallout from the unresolved calls and
  vanished once resolved). Pure hygiene, behavior-identical: `corpus_sweep` +
  `sql_golden` byte-identical, 1612 lib + 529 integration + ratchet net-zero,
  fmt/clippy clean, warning-free `--tests`. Scoped to `with_to_cte` only (no
  drive-by); `plan_builder_utils.rs`'s twin globs are the next slice. **Also
  verified D6/D8 are already resolved by attrition** (documented in §9): D6's inline
  `expand_table_alias_to_select_items` twin and D8's three named nested twins were
  already collapsed by prior P2.x moves + dead-code sweeps; remaining same-named
  fns are legitimately distinct (different modules/signatures/semantics, e.g. the
  #596 `Union`-boundary divergence). Nothing left to dedup there.

- 2026-07-27: **P2.7 D1 — `with_clause_key` dedup** (P-3 refactor lane) — the three
  near-duplicate WITH-key helpers that P2.6 co-located in
  `render_plan/with_to_cte/mod.rs` (`generate_with_key_from_with_clause`,
  `get_with_key`, `get_with_clause_key`) collapsed to one canonical
  `with_clause_key()` in `src/utils/with_clause_key.rs` (next to `cte_naming`, per
  §5.2). **Correction to the D1 premise**: the kill-list called this a "verbatim
  triplicate", but it was not — `get_with_key`/`get_with_clause_key` were
  byte-identical *simple* copies (`exported_aliases` → sorted-join, else
  `"with_var"`), whereas `generate_with_key_from_with_clause` had an extra
  item-extraction fallback and was the variant the authoritative
  `find_all_with_clauses_grouped` relied on. Unified on the **rich** variant so
  grouping and barrier-matching can never disagree on a WITH's identity. The corpus
  is the oracle: `corpus_sweep` + `sql_golden` (529 tests) **byte-identical**,
  proving the two simple copies never actually reached their divergent branch on any
  corpus query. Also deleted the now-orphaned nested `extract_with_alias` (its logic
  moved into the util) and its two dead imports. Gates: 1612 lib + 529 integration +
  1 ratchet, 0 failures; goldens/corpus byte-identical; ratchet net-zero (no axis
  tokens moved); fmt/clippy clean; warning-free `--tests`. `with_to_cte/mod.rs` shed
  148 lines. Next: P2.8 (D6) / P2.9 (D8) or Phase-4 §7.1 decomposition.

- 2026-07-27: **P2.6 — with_to_cte move (the entangled core)** (P-3 refactor lane,
  4 stacked PRs) — the WITH→CTE "entangled core" extracted verbatim from the
  ~13.3K-line `plan_builder_utils.rs` into a new `render_plan/with_to_cte/mod.rs`,
  one byte-identical MOVE per sub-slice: **(1)** the #529 shape-1 loud-guard
  property helpers (`table_role_dependent_property_names`,
  `collect_property_accesses` + tests); **(2)**
  `replace_with_clause_with_cte_reference_v2` (~1,080 ln, exactly byte-identical,
  no widening); **(3)** the WITH-discovery/pruning cluster
  (`find_all_with_clauses_grouped`, `collapse_passthrough_with`,
  `node_is_concrete_labeled`, `alias_has_pattern_correlation`,
  `prune_joins_covered_by_cte`; one widening —
  `alias_has_pattern_correlation`); **(4)** the giant
  `build_chained_with_match_cte_plan` (~5,478 ln) + its orbit structs
  `WithBarrierScope`/`CteNameAllocator`, plus widening the **24** private
  `plan_builder_utils` helpers it calls back into (`fn` → `pub(crate) fn`) so the
  moved function can back-import them. **Key mechanic**: everything lands in ONE
  `mod.rs` (kept at `render_plan` depth), so the moved bodies' `super::…` /
  `super::super::…` path-expressions resolve byte-identically — a sub-file split
  would force non-verbatim path edits (that's Phase-4 §7.1 decomposition, out of
  scope). `build_chained` + both structs verified **exactly byte-identical** (diff
  clean vs their pre-move text); the smaller slices byte-identical modulo the
  documented widenings. Re-exports left in `plan_builder_utils` during the
  transition (`build_chained` for `plan_builder.rs`'s 4 sites; the test-only
  `find_all_with_clauses_grouped` / `plan_contains_with_clause` gated `#[cfg(test)]`);
  now-dead re-exports and header imports pruned (each move flips their last
  producer/consumer). Ratchet: slice 2's regeneration relocated the moved
  schema-axis tokens (`is_denormalized` ×4, `from/to_node_properties`,
  `from/to_label_column`, `type_column`) from `plan_builder_utils.rs` to
  `with_to_cte/mod.rs` — a **pure net-zero relocation** (totals conserved, no new
  axis branching, §2.1); slices 1/3/4 net-zero with no baseline change.
  `plan_builder_utils.rs` **13,339 → 4,874 lines**. Gates each slice: 1612 lib +
  529 integration (incl `corpus_sweep` + `sql_golden`) + 1 ratchet + 7 unit, 0
  failures; goldens + corpus byte-identical (no churn); fmt/clippy clean;
  warning-free `--tests` build. **This completes Phase-2 §5.1 module moves.** Next:
  P2.7 (D1 `with_clause_key` dedup) or Phase-4 decomposition of the moved giants.

- 2026-07-27: **P2.5 D2 dedup — CTE property-rewriter family** (P-3 refactor lane)
  — the four near-identical CTE property-rewriters (two operator wrappers
  `rewrite_operator_application_for_cte`/`_join` + two `RenderExpr` helpers
  `_simple`/`_operand`, all colocated in `cte_rewrite.rs` after sub-slices A–C)
  collapsed into: the two public wrappers (unchanged signatures) → one shared
  `rewrite_operator_application` core → one `rewrite_render_expr_for_cte` core,
  parameterized by a `Copy` `CteAliasPolicy` enum (`Keep` = keep the property's
  alias + unconditional encode; `Rewrite(cte_alias)` = rewrite alias + the
  double-encode `is_cte_column` guard + the info log). **Byte-identical**: this is
  a behavior-preserving dedup — each policy reproduces exactly one former
  function's behavior, so corpus + goldens are unchanged (the equivalence proof,
  since source structure changed and byte-diff-vs-main does not apply to a dedup).
  The plan's §5.2 idea of applying the double-encode guard *universally* (under
  `Keep` too) is the still-open transition-assert question — deliberately NOT
  folded in, since it would change `Keep` semantics (ground rule: no semantic
  change in a consolidation). Left as a separate follow-up. Net: −2 functions,
  −~35 lines. **This completes P2.5.** Gates: 1612 lib + 529 integration (incl.
  `corpus_sweep` + sql_golden) + 1 ratchet + 7 unit, 0 failures; no golden churn;
  ratchet net-zero; fmt/clippy clean; warning-free `--tests` build. Next: P2.6
  with_to_cte.
- 2026-07-27: **P2.5 sub-slice D — cte_graph_joins_rewrite (LogicalPlan-level CTE
  rewriters)** (P-3 refactor lane) — the contiguous LogicalPlan-level CTE-ref
  rewrite block (`rewrite_logical_expr_cte_refs`, the 3 alias walkers deferred
  from P2.4 — `find_fresh_table_scan_aliases_in_plan`/`collect_fresh_scan_aliases`/
  `with_exported_aliases_in_branch` — and the ~400-line `update_graph_joins_cte_refs`,
  old lines 3547–4120, ~574 lines) extracted verbatim to a NEW
  `render_plan/cte_graph_joins_rewrite.rs`. Kept as a separate module from
  `cte_rewrite.rs` because it operates on `query_planner::logical_expr`/`logical_plan`
  types, not render types (the LogicalPlan companion). Only
  `update_graph_joins_cte_refs` has in-module callers (2 sites, already `pub(crate)`)
  so it alone is re-exported; the other 4 are called solely within the new module
  and stay private. **Fully byte-identical — zero visibility changes needed**
  (unlike sub-slices A/C). Ruled out a false-positive external caller
  (`Self::collect_fresh_scan_aliases` in analyzer/graph_join/inference.rs is an
  unrelated same-named method). This completes the P2.5 function moves; only the D2
  dedup remains. Gates: 1612 lib + 529 integration (incl. `corpus_sweep` +
  sql_golden) + 1 ratchet + 7 unit, 0 failures; **byte-identical** (moved block
  diff-clean vs main, no golden churn, ratchet net-zero); fmt/clippy clean;
  warning-free `--tests` build. `plan_builder_utils.rs` 13908 → ~13335. Next: P2.5
  D2 dedup or P2.6 with_to_cte.
- 2026-07-27: **P2.5 sub-slice C — cte_rewrite (join-condition rewrite group)**
  (P-3 refactor lane) — the contiguous join-condition CTE-rewrite group
  (`collect_with_cte_table_aliases`, `strip_table_alias_from_resolved`,
  `rewrite_join_conditions_for_cte_aliases`, old lines 1430–1627) extracted
  verbatim into the existing `render_plan/cte_rewrite.rs`. All three had zero
  external callers (earlier grep "hits" in join_builder were comments), so the
  `pub(crate) use` re-export serves only this module's internal call sites (3
  distinct + the one recursive self-call that moved with its fn). The only
  non-verbatim change is widening all three `fn`→`pub(crate) fn` so the re-export
  reaches them (documented at each site). `super::`-qualified deps (`CteSchemas`,
  `render_expr::*`, `UnionItems`, `FromTableItem`, `map_render_expr`) resolve from
  the new sibling module unchanged. Zero logic edits. Gates: 1612 lib + 529
  integration (incl. `corpus_sweep` + sql_golden) + 1 ratchet + 7 unit, 0
  failures; **byte-identical** (moved block diff-clean vs main modulo the 3
  visibility lines, no golden churn, ratchet net-zero); fmt/clippy clean;
  warning-free `--tests` build. Next: P2.5 sub-slice D.
- 2026-07-27: **P2.5 sub-slice B — cte_rewrite (CTE-name remap pair)** (P-3
  refactor lane) — the tightly-coupled CTE-name remap pair
  (`remap_cte_names_in_expr` + `remap_cte_names_in_render_plan`, old lines
  1385–1469 and 1513–1567) extracted verbatim into the existing
  `render_plan/cte_rewrite.rs`. Two non-contiguous deletions (they were separated
  by the unrelated `rewrite_count_to_conditional` / `quote_qualified_col` helpers,
  which stay put). `pub(crate) use` re-export for `remap_cte_names_in_render_plan`
  (2 internal callers); `remap_cte_names_in_expr` has no caller left in
  plan_builder_utils (it was only invoked by the two moved fns) so it is not
  re-exported — it lives on as a `pub fn` used internally by `_in_render_plan`.
  Zero logic edits. Gates: 1612 lib + 529 integration (incl. `corpus_sweep` +
  sql_golden) + 1 ratchet + 7 unit, 0 failures; **byte-identical** (both fn bodies
  diff-clean vs main, no golden churn, ratchet net-zero); fmt/clippy clean;
  warning-free `--tests` build. Next: P2.5 sub-slice C.
- 2026-07-27: **P2.5 sub-slice A — cte_rewrite (expression-rewriting cluster)**
  (P-3 refactor lane) — the self-contained CTE-expression-rewriting cluster
  (`rewrite_operator_application_for_cte`, `rewrite_operator_application_for_cte_join`,
  `rewrite_render_expr_for_cte_simple`, `rewrite_render_expr_for_cte_operand`, old
  lines 342–453) extracted to a new `render_plan/cte_rewrite.rs`. `pub(crate) use`
  re-exports for the externally-called `..._for_cte` (join_builder) and the
  internally-called `..._for_cte_join` (2 sites in plan_builder_utils). The only
  non-verbatim change is widening `rewrite_operator_application_for_cte_join` from
  `fn` to `pub(crate) fn` so the re-export can reach it (documented at the site) —
  same transition-visibility pattern as P2.1/P2.2. **P2.5 is being sub-sliced**
  because its §5.1 home is ~13 fns across 3 scattered bands (much larger/entangled
  than P2.1–P2.4, and some listed fns actually belong to P2.6's giant builders);
  remaining P2.5: `remap_cte_names_*`, `rewrite_logical_expr_cte_refs`,
  `update_graph_joins_cte_refs`, the P2.4-deferred alias walkers, D2 dedup. Gates:
  1612 lib + 529 integration (incl. `corpus_sweep` + sql_golden) + 1 ratchet + 7
  unit, 0 failures; **byte-identical** (moved-fn bodies diff-clean vs main modulo
  the one visibility line, no golden churn, ratchet net-zero); fmt/clippy clean;
  warning-free `--tests` build. Next: P2.5 sub-slice B.
- 2026-07-27: **P2.4 — plan_predicates module move** (P-3 refactor lane) — the
  WITH-detection predicate group (`has_with_clause_in_tree`,
  `has_with_clause_in_graph_rel`, `plan_contains_with_clause`, old lines
  2008–2098) extracted verbatim to a new `render_plan/plan_predicates.rs`.
  `pub(crate) use` re-exports for the two production-called names left in
  `plan_builder_utils`; `has_with_clause_in_tree` has no production caller (only
  the P1.2 characterization tests use it) so it is imported directly by the test
  module instead of re-exported (keeps the lib build warning-free). Zero logic
  edits. **Scoped down from §5.1's premise**: the band also listed the private
  fresh-scan / with-exported alias walkers, but those are coupled to P2.5's
  cte_rewrite fns (`update_graph_joins_cte_refs`/`rewrite_logical_expr_cte_refs`),
  so they ride with P2.5 rather than churn visibility ahead of their callers
  (§8.3 no-drive-by); the `plan_builder_helpers` second copy of
  `has_with_clause_in_graph_rel` (`pub(super)`, different semantics) is the
  flagged D-cluster duplicate and is left untouched (separate dedup). §5.1 table
  + §9 checklist + P-3 updated. Gates: 1612 lib + 529 integration (incl.
  `corpus_sweep` + sql_golden) + 1 ratchet + 7 unit, 0 failures; **byte-identical**
  (moved-fn bodies diff-clean vs main, no golden churn, ratchet net-zero);
  fmt/clippy clean. Next refactor slice: P2.5 cte_rewrite.
- 2026-07-27: **P2.3 — clause_extractors module move** (P-3 refactor lane) — the
  pure clause extractors remaining in `plan_builder_utils.rs`
  (`extract_having`/`extract_order_by`/`extract_limit`/`extract_skip` +
  `extract_sorted_properties`, old lines 1467–1560) extracted verbatim to a new
  `render_plan/clause_extractors.rs`, `pub(crate) use` re-exports left in
  `plan_builder_utils` so all `super::plan_builder_utils::extract_*` call sites
  and `properties_builder`'s direct import keep resolving. Zero logic edits.
  **Stale-premise correction**: §5.1 named an 8-function group
  (`filters/from/group_by/having/order_by/limit/skip/distinct`) but
  `extract_filters/from/group_by/distinct` had already migrated to
  `filter_builder.rs`/`group_by_builder.rs` via incremental work, so P2.3's real
  scope was the 5-function remainder (§5.1 table + §9 checklist updated to match).
  Gates: 1612 lib + 529 integration (incl. `corpus_sweep` + sql_golden) + 1
  ratchet + 7 unit, 0 failures; **byte-identical** (no golden churn, corpus
  snapshot unchanged, ratchet net-zero); fmt/clippy clean. Next refactor slice:
  P2.4 plan_predicates.
- 2026-07-27: **Doc reconcile + branch prune** (housekeeping, no code) — §2 queue
  status lines brought current after the 07-19..27 batch: P-1 marked
  clean-pool-exhausted with the full shipped list (~20 fixes) + the next picks
  (live-DB-oracle #606 variants / #504, ClickHouse now up); P-3 P2.2 marked MERGED
  (#660, `3776d0a9`); P-4 header + Next updated (F3/F4 merged, #602 follow-up #662
  merged as #663, #595 closed). Pruned all merged branches: 5 dead local
  (`fix/662`, `refactor/f2c`, `review-641/659/683` — all already on main via
  squash) + 9 merged remote `fix/*` families (#465/#484/#504/#524/#536/#543/#544/
  #546/#549); `git fetch --prune` cleared the rest. Only `main` + the two
  `release/v0.6.{4,5}-dev` branches remain.
- 2026-07-27: **#580 — multi-type VLP endpoint id-property crash / silent-empty**
  (P-1 bug lane, #715, `c2789908`) — a multi-type VLP (`[:R1|R2*..]`, or untyped
  `[r]`) endpoint's id property (`u.user_id`) was projected as
  `JSONExtractString(t.{start,end}_properties,'user_id')`. Two failures: (1) Code
  215 NOT_AN_AGGREGATE — the GROUP BY builder resolves the same id to native
  `t.start_id` (#538), so SELECT and GROUP BY were different expressions; (2)
  silent-empty non-aggregate — the JSON blob key is qualified `"u_1.user_id"`
  when the column name is ambiguous across the branch's JOINed tables, so
  `JSONExtractString(blob,'user_id')` returns `''`. Fix: when the property is the
  endpoint node's single-column `node_id` AND the endpoint is genuinely
  single-type, project the native `start_id`/`end_id` (matching the working
  single-type path); `build_aliased_group_by` then unifies both sides. The
  single-vs-multi discriminator reads the resolved `TYPE::FROM::TO` composite keys
  in `gr.labels` (orientation-correct side) — the planner collapses a
  genuinely-multi-type endpoint to one inferred GraphNode label, so `TableCtx`/
  `extract_node_labels` can't be trusted. LIVE-VERIFIED: typed agg → Post 1 5 /
  User 1 7; end `u:User` → 2 6; untyped-`[r]` agg → 1/AUTHORED/2, 1/FOLLOWS/3,
  1/LIKED/1 (bonus crash fix); multi-type far endpoint `x.post_id` stays JSON (no
  over-fire); single-type unchanged. Adversarially reviewed (first cut over-fired
  onto the collapsed-label far endpoint; refined to composite-key discriminator +
  boundary test). Net golden diff = the untyped-`[r]` case only, both dialects.
  Pre-existing broader non-id ambiguous-JSON-key silent-empty (e.g. `u.city`
  grouping) + composite-id multi-type mismatch → follow-up #716.

- 2026-07-26: **#606 — self-ref FK-edge VLP relationship-uniqueness** (P-1 bug
  lane, #712, `07268010`) — `generate_fk_edge_base_case` + `_recursive_append` /
  `_recursive_prepend` enforced node-uniqueness and carried no `path_edges` at
  all. Added `build_fk_edge_tuple`; `path_edges` seeds `(from_id,to_id)` in the
  base and extends the per-hop `(child,parent)` tuple in both arms, cycle check on
  the tuple. Same `uses_edge_uniqueness()` gate (shortestPath + zero-hop stay
  node-unique). LIVE-VERIFIED against a cyclic FK fixture: 12 edge-unique trails
  vs old 9 over `*1..3` (recovers 3 cycle-closing paths). Adversarially reviewed
  (0 blocker; rendered every arm, confirmed all tuples canonically (child,parent);
  multi-hop base stub is dead code; wrapper carries path_edges via SELECT *).
  MINOR pre-existing (composite-id FK-edge not composite-aware, identical on main)
  → follow-up #713. Second #606 variant done; weighted/mixed/hetero-poly/
  undirected/multi-type stay open.

- 2026-07-26: **#606 — denormalized VLP relationship-uniqueness** (P-1 bug lane,
  #709, `1b4feba2`) — `DenormalizedCteStrategy` range VLP enforced node-uniqueness
  (`NOT has(vp.path_nodes, next.to)`), silently dropping valid node-revisiting
  paths. Mirrored #598 into the denorm strategy: `path_edges` seeds/extends the
  `(from,to)` edge tuple, cycle check on that tuple. Gated
  `shortest_path_mode.is_none() && effective_min_hops()>=1` (zero-hop + shortestPath
  stay node-unique — verified base/recursive shape-safe). LIVE-VERIFIED against a
  cyclic fixture: 14 edge-unique trails vs the old bug's 7 over `*1..3`. Golden
  corpus regenerated (denorm only). Adversarially reviewed (0 blocker/major; MINOR
  parallel-edge under-count is a pre-existing denorm limitation this PR strictly
  improves → follow-up filed). Denorm variant of #606 done; fk-edge/mixed/
  edge-to-edge/heterogeneous-polymorphic recursive generators stay open.

- 2026-07-26: **#705 — shared exhaustive EXISTS predicate rewriter** (P-1 bug lane,
  #707, `1fe800ca`) — both #587/#640-s3 predicate mappers hand-rolled a partial
  recursion, leaking a CASE/InSubquery-nested property ref as a raw column (Code
  47). Extracted one exhaustive `map_exists_predicate_props`; both delegate.
  Byte-identical for covered shapes. Adversarially reviewed (0 defects; None-vs-
  unwrap equivalence + Operator-merge both confirmed safe). #705 closed.
  bug lane, #704, `90d5697b`) — `MATCH (a),(c) WHERE EXISTS { (a)-[:R]->(c) WHERE
  <pred> }` was loud; now emits the both-outer correlated subquery (no inner JOIN)
  with `map_exists_outer_predicate` mapping refs by each endpoint's schema.
  Direction-aware; VLP/composite/undirected/single-outer guards preserved. I caught
  a function-nested-predicate leak myself before review (added ScalarFnCall/List
  recursion). Adversarially reviewed (0 blockers; 2 pre-existing loud MINORs shared
  with #587 → follow-up #705). Shapes 2/4/5 remain open on #640.
  #702, `e82a5a5a`, test-only) — verified the 3-arm UNION repro renders correctly
  on main (middle post-WITH arm correlates on the CTE column `a_c.p1_a_user_id`,
  fresh-MATCH arm on base-table `a.user_id`, `WHERE c > 0` → HAVING), fixed as
  collateral of #587 `resolve_correlation_id_sql` + carry-id-across-barrier. Added
  a regression test. #595 closed.
  #700, `42e87215`, test-only) — verified all residual shapes (OPTIONAL
  anchor-filter, WITH id-property, WITH+aggregate grain, bound RETURN r,
  DISTINCT, shortestPath+name) now render correctly on main, resolved as
  collateral by #630/#659/#677/#678; the `__order_col` alias-mangling path is
  gone. Added a dedicated regression test locking shapes A/B/D/E. #620 closed.
  `e34cead8`) — a multi-sub-CTE VLP (shortestPath/min-hops: `{name}_inner` +
  `{name}` wrapper in one RawSql blob) in both UNION arms emitted Code 179 on the
  derived `vlp_a_b_inner` (#618 renamed only the outer). Fix discovers the derived
  family (`{base}_{suffix} AS (` headers, string-literal-aware) and renames it with
  the base via the #618 structural rewriter. Two-layer safety (loose discovery +
  strict structural rewrite). Byte-identical #618 single-CTE + single-arm.
  Adversarially reviewed with isolated-build differential (0 defects). #642 closed.
  bug lane, #696, `03d61403`) — a non-self-ref FK-edge with a composite FK emitted
  `f.region = c."forum_region, forum_id"` (comma-joined FK wrapped as one bogus
  identifier → Code 47). The `FkEdgeJoin` non-self-ref Right/Left branches used
  `Identifier::Single(resolve_column(…))`; fixed to `from_comma_separated` +
  `resolve_identifier` (mirrors the #646 self-ref path), zipping into per-column
  equalities. Byte-identical single-column; self-ref untouched; new fixture +
  golden. Adversarially reviewed (0 defects). Part 1 (loud order/arity guard)
  still open on #672.
  #694, `2fdf98f8`) — `EXISTS { (a)-[:R]-(b) WHERE b.p }` failed loud; #587 covered
  only directed. `generate_exists_filtered_graph_rel_sql` now handles
  `Direction::Either` via the OR-of-both-orientations correlation (fused into the
  JOIN ON), mirroring `join_builder.rs`. Homogeneous edges render; heterogeneous
  undirected + composite/denorm/polymorphic stay loud (role-ambiguity + #587
  guards); directed byte-identical. Adversarially reviewed (0 defects). Shapes 2–5
  remain open on #640.
  `3e6d6078`) — `WITH a.code AS x, b.code AS y` over a denorm VLP rendered
  `t.start_code`/`t.end_code` (Code 47); the denorm CTE projects `start_id`/`end_id`.
  `DenormalizedCteStrategy` was the sole VLP strategy hardcoding the id column's
  `cypher_property` to `"id"` (vs the shared `build_vlp_column_metadata`'s real
  logical name), so the #620/#677 WITH-item id pre-pass never matched. Fix resolves
  the logical node_id property name; composite + multi-label-table (zeek `dns_log`)
  guards keep the pre-existing loud behavior (byte-identical to main). FK-edge
  self-ref + closed-VLP variants already render correctly on main (#677). #678 closed.
  Adversarially reviewed (0 blockers; both MINORs — loud→silent edge + comment —
  addressed).
  (head `f60be99f`) = `success`. Closes the red streak open since ~07-13. Final
  layer cleared by **#690** (`f60be99f`): `test_unwind_with_match` generator
  String-vs-Int fix, `test_external_users_with_access` xfail restored (+#689 filed),
  `test_vlp_with_and_aggregation` xpass marker removed. P-0 now folds into standing
  nightly-triage duty (§3).
- 2026-07-26: **#683 residual 1 — denorm anchor count(a)** (P-1 bug lane, #685,
  `f8a69ad7`) — `count(a)` on a denorm OPTIONAL-VLP anchor rendered `count(a.code)`
  (logical node_id) but the `__denorm_scan_a` CTE exposes physical `origin_code` →
  Code 47. The ANCHOR sibling of #659 (which fixed the VLP-endpoint variant); the
  earlier investigation was blocked because the WITH-path anchor-id helper's
  traversal excludes VLP. Fix: extend #644's `rewrite_denorm_optional_vlp_anchor_scan`
  (which already computes the anchor physical id `from_id_col` + rewrites the VLP
  JOIN key) to mirror the `(anchor_alias, logical_id) → physical_id` rewrite across
  SELECT/GROUP BY/HAVING/ORDER BY, via the exhaustive `map_render_expr` (§5 walker
  discipline). ALL #644 guards reused → end-anchored/reversed, composite,
  anchor-gate, chained-VLP stay loud unchanged. Adversarial review 0 blockers
  (guard chain airtight + no over-fire, main-vs-PR differential). +1 curated + 2
  corpus goldens; no existing golden flipped. Residual 2 (composite-VLP malformed
  SQL, #604/#605/#627 family) stays open under #683.

- 2026-07-26: **#659 VLP count(<endpoint>) endpoint-id resolution** (P-1 bug lane,
  #682, `2a2d900f`) — `count(b)` over a VLP endpoint rendered `count(vt0.end_code)`
  (a column the CTE never projects → Code 47). Root: `count(b)` normalizes to
  `count(b.<logical_id>)` (e.g. `b.code`) UN-db-translated, so it misses
  `cte_column_mapping` (keyed on db_column) in `rewrite_render_expr_for_vlp_with_endpoint_info`
  and the prefix-fallback built `end_code`. Fix (miss-fallback ONLY, mirrors
  #620/#677 — NOT the reverted drop-the-redundant-column prototype): thread
  `id_property_by_alias` (built at the `rewrite_vlp_union_branch_aliases` caller via
  `get_node_label_for_alias`/`find_denorm_connection_node_label` + `node_id.column()`,
  composite-safe → stays loud per #605); on the id-property miss, resolve to
  `VLP_START/END_ID_COLUMN`. Broader than the ticket: also fixed the STANDARD-schema
  variant (`data_security` `count(g)`/whole-node `RETURN target` dangled `end_group_id`;
  2 corpus goldens corrected dangling→correct, value-identical) and zeek `count(b)`
  (was `count(t."end_id.orig_h")`, also dangling on main). Removed a now-passing xfail.
  Zeek #545/#558/#559 dotted-column contract BYTE-IDENTICAL (they db-translate → HIT the
  map, never reach the fallback). No metadata-builder/SQL-emitter change. +5 regression
  goldens. Adversarial review: isolated main-vs-PR differential, 0 blockers, no over-fire,
  no zeek regression; caught that the `!is_composite()` guard also averts a
  `NodeId::column()` panic-on-composite. Residuals (denorm anchor `count(a)`,
  composite-VLP malformed SQL) → #683. Gates: 1611 lib + 222 sql_golden + corpus (2
  corrected, 5 new) + ratchet net-zero.

- 2026-07-25: **#641 chained-undirected-optional loud-gate holes** (P-1 bug lane,
  #680, `fe6de435`) — two shapes slipped past the #589 loud gate and stayed
  silently single-direction (dropping the undirected hop's reverse-orientation
  rows). **Hole 1:** the `LogicalPlan::Union` arm of `transform_bidirectional`
  (`bidirectional_union.rs`) caught ALL errors (`Err(_) => input.clone()`),
  swallowing the fatal `UnsupportedPattern`; a chained-undirected-optional inside
  a UNION branch kept rendering single-direction. Fixed by propagating
  `UnsupportedPattern` (matching the analyzer/mod.rs call-site contract) while
  still tolerating other errors — rewrote the `.map()` as a `for` loop so the
  early return is expressible. **Hole 2:** `has_chained_optional_nested_undirected_edge`
  keyed only on the OUTER hop being `Direction::Either`, so undirected-inner +
  directed-outer slipped through; widened to fire when the chained optional pair
  (junction not a CartesianProduct = outcome #2) has the outer OR nested-inner hop
  undirected. Both remain loud-only (real fix needs the anchor-LEFT-JOIN-onto-match-
  union render, #589/#583). Over-fire boundary held by the `inner.is_optional ==
  Some(true)` guard (#492-B3 shared-alias REQUIRED chains stay OK). +2 unit tests,
  +2 corpus `.err` goldens. Adversarial review: full 1201-query differential, only
  the 2 new queries flip OK→loud (zero regressions), loud verdict confirmed
  justified by inspecting main's NULL-anchor-garbage / single-direction SQL. Corpus
  byte-identical otherwise; 1611 lib + ratchet net-zero.

- 2026-07-25: **#620 VLP WITH-item endpoint id-projection** (P-1 bug lane, #677,
  `079571fc`) — `WITH a.user_id AS x` (also DISTINCT / `count(b)` / mixed) over a
  variable-length path projected the endpoint *id* property as
  `t.start_user_id`/`t.end_user_id`, columns the VLP CTE never defines (it
  special-cases the id column as `start_id`/`end_id` = VLP_START/END_ID_COLUMN,
  while non-id props get `start_{db_col}`). The WITH-item rewriter
  (`vlp_rewrite::rewrite_render_expr_for_vlp_with_from_alias`) blindly prefixed
  the column name → only the id property mismatched → Code 47 at execution. Fix:
  targeted pre-pass `rewrite_vlp_id_property_columns` (vlp_rewrite.rs) run before
  the generic prefix rewrite at the `build_chained_with_match_cte_plan` WITH-item
  site, using the VLP CTE metadata's `is_id_column` + `cypher_property` to rewrite
  the endpoint id-property PropertyAccess to `t.start_id`/`t.end_id` (moving it off
  the endpoint alias so the generic rewriter then skips it). Affects directed AND
  undirected: corrected an existing corpus golden (`test_vlp_with_and_aggregation`,
  directed `*1..2->` `COUNT(DISTINCT u2.user_id)`) that was locking silently-broken
  `count(DISTINCT t.end_user_id)` → `count(DISTINCT t.end_id)`. Collision-immunity
  is structural: `build_vlp_column_metadata` sets `is_id_column` true ONLY for the
  real node_id, never via the `_id`-suffix heuristic — so composite-id sub-columns
  (`bank_id`/`account_number` → own `start_bank_id` cols, no truncation to the
  `start_id` concat) and `_id`-suffix property collisions (`author_id`) are left
  alone (adversarially verified). +5 regression goldens. Scope: standard-schema
  single-WITH-barrier id projection. Out: FK-edge self-ref / denorm / closed-VLP
  endpoint id-property (pre-existing, byte-identical to main, still loud → **#678**);
  the outer-barrier `.id` alphabetical fallback (#616/#411); OPTIONAL anchor-gate
  (already #621). Gates: 1609 lib + 222 sql_golden + corpus (2 corrected, 5 new) +
  ratchet net-zero; fmt/clippy clean. Adversarial worktree review 0 findings.

- 2026-07-25: **#635 FK-edge coupled rel-var VLP WHERE-filter** (P-1 bug lane,
  #675, `04ac22da`) — **not-a-bug + regression lock**. #635 asked (its own words)
  whether a coupled FK-edge rel-var in a `WHERE` on a variable-length pattern
  dangles `r`. Investigation: it does NOT — the VLP filter push-down (the
  "HOLISTIC FIX Dec 26, 2025", `vlp_rewrite.rs:784-788` + `cte_extraction.rs`
  `relationship_filters`) binds coupled `r.<col>` to the edge-scan alias
  (`start_node`/`new_start`) inside BOTH the base and recursive CTE scans,
  implementing genuine all-hops semantics. `PLACED_BY` (fk_edge) is non-transitive
  so `*1..2` collapses to single-hop (already #633); a real coupled VLP CTE needs
  a self-ref FK-edge (`filesystem_single` PARENT), where WHERE-r works. Differential
  matrix (coupled `filesystem_single PARENT*` vs standard `social FOLLOWS*`): every
  genuinely-dangling shape (`RETURN r`, `count(r)`, post-WITH `WHERE r`) dangles
  IDENTICALLY on coupled and standard → general VLP rel-var *projection* gap (#620),
  NOT coupled-specific. Change is test-only (no source): +3 regression goldens
  (outgoing / incoming-reversed / compound) on `fk_edge_self_ref`, all binding
  `r.parent_id` to the edge-scan alias (no bare `r.` dangle). Corpus byte-identical
  otherwise; sweep deterministic; ratchet net-zero; 515 integration tests. Adversarial
  worktree review 0 findings (reviewer independently traced the recursion against
  concrete filesystem rows to confirm all-hops semantics). Remaining dangling shapes
  retargeted to #620.

- 2026-07-25: **#646 composite self-ref FK-edge** (P-1 bug lane, follow-up to
  #632) — a self-referencing FK-edge (edge table == node table) with a COMPOSITE
  node_id emitted a malformed single-column join `child."parent_region, parent_id"
  = parent.region` → Code 47. #632's self-ref FK-detection used
  `first_col(node_id)` + a `String` compare, so for composite ids the whole
  comma-joined FK string became one bogus quoted identifier. Fix (Left branch
  only — self-ref always routes join_side=Left): compare full column SETS
  (`Identifier::from_comma_separated(from_id).columns()` vs `node_id.columns()`;
  FK = the {from,to} side ≠ node_id) and emit `Composite` Identifiers through
  `resolve_identifier` + `add_identifier_condition` (zips positionally →
  `child.parent_region = parent.region AND child.parent_id = parent.object_id`).
  Byte-identical for single-column self-ref (filesystem PARENT FK=from_id, ldbc
  REPLY_OF FK=to_id) — corpus_sweep 0 changes. New fixture
  `schemas/test/composite_self_ref_fk.yaml` + executable-oracle sql_golden test
  (both directions × both dialects). VLP/CTE-path composite truncation
  (`cte_manager` get_fk_edge_node_id_column `columns[0]` TODO) is a documented
  follow-up, out of scope. Column-order is a positional author invariant (FK
  cols must align with node_id cols), mirroring the non-self-ref composite path.

- 2026-07-25: **#648 untyped count(r) multi-type** (P-1 bug lane) — regression
  from #502 (bisect a10886aa). `MATCH (u:TestUser)-[r]->(o:TestUser) RETURN
  count(r)` where two rel types connect TestUser→TestUser routes through the
  `vlp_multi_type_*` UNION-ALL CTE (aliased `t`, generic `start_id`/`r_from_id`
  cols) but the projection-tagging aggregate rewrite picked the FIRST type's
  `from_id` and kept the Cypher alias `r` → `count(r.follower_id)` against a
  CTE with no such column and an unbound alias → Code 47. Fix: new
  `rel_alias_uses_multi_type_vlp` predicate (`logical_plan/mod.rs`, gated on
  `rel.labels.len() > 1`) + a guard branch in `projection_tagging.rs` mirroring
  the #526 `pattern_union` branch → emit `count(t.start_id)` (NULL-sensitive,
  always-projected) / DISTINCT label-agnostic tuple over alias `t`. Blast
  radius nil: typed count(r), count(*), single-type-untyped count(r), and
  fully-unlabeled count(r) all unchanged (guard fires only on labels.len()>1).
  Scoped diff: corpus `test_count_relationship_with_node_constraints` golden
  flips from the BROKEN `count(r.follower_id)` to `count(t.start_id)` (both
  dialects) — the only corpus transition. +executable-oracle sql_golden test;
  removed py strict-xfail.

- 2026-07-25: **#649 leading UNWIND** (P-1 bug lane, `fix/649-leading-unwind`,
  PR #669) — `UNWIND [...] AS x MATCH (n) WHERE n.p = x RETURN n` (UNWIND as the
  first clause) was rejected; pre-#516 it "passed" only by silently dropping
  everything from MATCH on. `parse_query_with_nom` now parses leading UNWINDs
  before the reading clauses and threads them (leading-first) into the same flat
  `unwind_clauses` list the trailing form uses, so the planner builds the
  identical `Unwind(Match(...))` plan → byte-identical SQL to `MATCH ... UNWIND`.
  NOT a new `ReadingClause::Unwind` variant (an order-preserving Unwind-wraps-
  Match plan was prototyped and dropped the ARRAY JOIN — renderer only emits it
  when Unwind wraps Match). Scoped diff: corpus `test_unwind_list` .err→.sql
  (ARRAY JOIN / LATERAL VIEW), only transition in the 517-golden sweep. Removed
  matrix strict-xfail `test_unwind_with_match`; ldbc_bi_4 now PARSES and fails
  loud `UnionColumnMismatch` (silent-drop → loud, union-arm alignment deferred).
  +3 parser unit tests. Adversarial worktree review: 0 blockers, 2 nits (one
  "keep as-is"). Gates: corpus/sql_golden/1609+513/clippy/ratchet/fmt.

- 2026-07-25: **P-4 F4** — second Phase-C slice, mirrors F3. Added structural
  `subplan: Arc<LogicalPlan>` to `render_expr::ExistsSubquery` (backs `EXISTS { pattern }`)
  alongside the now validated-`sql` cache, so a later slice (#595/#613) can make rewriters
  recurse into it. Byte-identical (C1 — carry + cache): `generate_exists_sql` is already the
  single producer, bake site keeps the `?` (same error boundary) and stores an Arc clone of
  the subplan; all 6 `es.sql` consumers unchanged; the `EXISTS (...)` wrapper stays at the
  emit sites. serde via `#[serde(with = "serde_arc")]`, mirroring the input type. Corpus +
  220 goldens byte-identical; 2,156 tests; clippy clean; ratchet net-zero. Branch
  `refactor/f4-existssubquery-carry-subplan`.
- 2026-07-25: **P-4 F3** — first Phase-C slice. Added structural `pattern: PathPattern`
  to `render_expr::PatternCount` (backs `size((a)-[:R]->(b))`) alongside the now
  validated-`sql` cache, so a later slice (#613) can make rewriters recurse into it.
  Byte-identical (variant **C1** — carry + cache): scoped DOWN from the plan's
  "drop `sql`, render at emit" because that flip isn't byte-identical and has zero
  payoff until a rewriter mutates the pattern (= #613). Extracted `render_pattern_count_sql`
  as the single source of the `coalesce(...,0)` string; kept the `?` at the bake site so
  invalid patterns still abort render at the same boundary (the infallible emit never
  panics — the error-path crux). All 6 `pc.sql` consumers unchanged. Corpus + 220 goldens
  byte-identical; 2,156 tests; clippy clean; ratchet net-zero; adversarial review 0 real
  findings. Branch `refactor/f3-patterncount-carry-pattern`.
- 2026-07-25: **P-4 F2c BLOCKED (docs, #665)** — recorded that F2c is NOT byte-identical:
  the "also add DB column" reverse arms are load-bearing for whole-node expansion (18
  goldens; property surfaces under both Cypher and DB-column names). Needs its own design
  cycle, not a pure deletion.
- 2026-07-25: **P-4 F2a** — deleted the M2 legacy CTE-property resolution
  mechanism (the task-local `cte_property_mappings` field, its four accessors
  `set_/get_/get_all_/clear_`, both producers — the deep-merge
  `populate_cte_property_mappings_from_render_plan` + `cte_extraction.rs` inline
  block, and the bulk `build_cte_property_mappings`/`set_all_render_contexts`
  path — and all three consumers). **Byte-identical** (2,156 Rust tests + 517
  corpus + 220 goldens unchanged; clippy clean; ratchet net-zero). De-risked
  before deletion with a **stub-probe experiment** (the F1 lesson — a green net
  can be silent on the class that matters): stubbing the read accessors to
  return nothing was byte-identical everywhere, and instrumenting the real
  accessors showed M2 fires 36× but is fully covered by the forward path —
  `cte_column_resolver`'s M2 fallback is never reached (0/36; forward
  `plan_ctx.get_cte_column` covers it), and `select_builder`'s 36 hits are all
  the rule-6 `p{N}_{alias}_{prop}` form that `cte_column_name()` reconstructs
  identically (0 divergences). Hazard re-encountered and avoided: the *local
  parameter* also named `cte_property_mappings` on
  `rewrite_logical_expr_cte_refs`/`update_graph_joins_cte_refs` is a different
  map (M3 `scope_cte_variables`) and was left intact. Branch
  `refactor/f2a-delete-m2-cte-property-mappings`. Next: F2b or Phase C.
- 2026-07-21: **P-4 F1b (#602)** — first F1b intentional-diff hunk. Post-WITH MATCH
  continuation from a WITH-projected node joined on the wrong column
  (`t2.user_id = u.post_id`, Code 47) when a second passthrough WITH barrier was
  present: the node's label was re-derived per barrier and arrived EMPTY after the
  passthrough (source `GraphNode` gone), so the `.id` operand couldn't resolve and
  the SQL-gen `.id` fallback alphabetically mis-picked `Post.post_id` (#616 class).
  Fixed by a persistent `carried_labels` map on `WithBarrierScope` (survives
  `reset()`) threading the label forward across barriers; gated on non-empty
  `per_alias_mapping` so scalar rebinds don't inherit a stale node label (adversarial
  review caught that loud→silent edge, fixed + regression-tested). No projection
  injection — the id column is produced then pruned, so resolving the operand lets
  `prune_cte_columns` retain it. Live-verified (baseline Code 47 → correct rows,
  fc/posts cross-checked); cross-schema standard/denorm/polymorphic fixed (denorm/poly
  silent-wrong on main), composite stays loud (follow-up), fk_edge unchanged. Corpus
  byte-identical; +3 regression goldens; ratchet net-zero. Branch
  `fix/602-postwith-continuation-label-carry`. #595→F4, #613→F3, #643 remain.
- 2026-07-20: **P-4 F1** — made the render-site forward registry (M1)
  authoritative for CTE-scoped property resolution and deleted the legacy M2
  render-site fallback (`get_cte_property_from_context` wrapper) + its import.
  Byte-identical consolidation (corpus 1,082 + all goldens unchanged), NOT the
  intentional-diff slice. Discovery that reshaped the slice: F0's "M1 fires 0× at
  the render site" held only for node exports — a direct probe showed M2's 210
  render-site hits are all **scalar / composite CTE exports** (`id.id`,
  `e_id_n.id` from `WITH u.user_id AS id`), whose empty `property_mapping` left
  BOTH M1 and M3 returning `Unresolved` (so F0's assert, which loops the mapping,
  never covered them). Deleting M2 without a replacement regressed `.id` via the
  render-site id-pseudo-property block (→ alphabetical `post_id`, #616-class).
  Fix: registry identity self-map for empty-mapping exports in
  `publish_alias`/`publish_composite` (kept OUT of the M3 scope map, whose
  empty-test drives node-vs-scalar expansion) + a `sql_alias == table_alias.0`
  render-site guard reproducing M2's FROM-alias keying, which also closes the
  #593 cross-arm alias leak (M1's registry is Cypher-alias-keyed + global, not
  arm-scoped like M3). +2 regression tests (unit + golden). #595/#602/#613/#643
  re-scoped to F1b (M3-path hunt) + Phase C. Branch
  `refactor/f1-scalar-forward-path`.
- 2026-07-20: **P2.2** second Phase-2 module MOVE — pattern-comprehension SQL
  string-emitting group extracted verbatim from plan_builder_utils.rs to
  `render_plan/pattern_comprehension_sql.rs` (31 fns, 2,629 lines, `pub(crate)`
  re-exports, zero logic edits; 12 fns + `PcCteResult` struct/fields +
  2 stay-behind helpers widened to `pub(crate)` as the only changes). Per-function
  byte-diff vs origin/main verified identical (modulo those visibility widenings +
  one benign fmt sig-reflow); 210 goldens + 1,082-query corpus byte-identical;
  ratchet net-zero (schema/dialect axis tokens relocated pbu→new module). Branch
  `refactor/p22-pattern-comprehension-move` (delivered, not yet merged). Next
  refactor slice: P2.3 clause_extractors.


- 2026-07-20: **#657** P2.1 first Phase-2 module MOVE — VLP expr-rewriting group
  extracted verbatim from plan_builder_utils.rs to `render_plan/vlp_rewrite.rs`
  (796 lines, `pub(crate)` re-exports, zero logic edits). Reviewed MERGE (0
  findings, per-function byte-diff verified); 210 goldens + corpus byte-identical;
  ratchet net-zero. Also merged **#655** P-4 forward-resolution plan +
  **#656** PRIORITIES sync. Next refactor slice: P2.2.

- 2026-07-20: **PR flow adopted** (repo has branch protection). Merged to
  `origin/main` (squash, admin — sole dev can't self-approve): **#650** P-2/P1.2
  five-WITH `walk()` (`3562ae0f`), **#652** #647 end-anchored OPTIONAL VLP
  (`91475be3`), **#651** P-5 S1 stats anchor (`65e2008c`), **#653** P-0 nightly
  triage (`ba8106c8`), **#654** STATUS.md refresh (`de865d85`). Also
  fast-forwarded origin/main up from #605 — the whole week's backlog (#607–#645
  + docs) had been local-only. In flight: **#655** P-4 forward-resolution plan
  (docs), P2.1 vlp_rewrite move, #644 denorm OPTIONAL-VLP fix.
- 2026-07-20: filed **#648** (untyped `count(r)` multi-type → Code 47, #502
  regression) and **#649** (leading-UNWIND parser gap) from P-0 triage; **#647**
  fixed (verified vs live Neo4j).

- 2026-07-19: #645 reversed OPTIONAL-VLP anchor gate (68666fda); #632
  self-ref FK-edge join inversion (94e788cb); #621 OPTIONAL-VLP anchor
  gate fold (006ccc0d). Doc created; §9 of REFACTORING_SAFETY_PLAN.md
  reconciled.
