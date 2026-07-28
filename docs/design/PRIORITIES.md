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
- Follow-ups still open (net hygiene, not blocking): prune stale
  `worktree-agent-*` branches; refresh STATUS.md (last updated 2026-05-06); **#689**
  (live VLP-then-fixed-hop-polymorphic 500).

### P-1 — Keep a small silent-wrong bug lane open  (standing, ≤1 agent)
**Lane state (2026-07-28): pool near-exhausted; #716 was the last clean pick.** Since
the 07-19 reconcile this lane shipped ~21 fixes (all live- or SQL-gen-verified,
newest first): **#716 (multi-type VLP non-id endpoint property → native CTE column,
#787)**, #580 (multi-type VLP endpoint id, #715), #606 denorm + fk-edge
VLP relationship-uniqueness variants (#709/#712), #705 (shared EXISTS predicate
rewriter, #707), #640 shapes 1 & 3 (#694/#704), #642 (VLP multi-sub-CTE union
collision, #698), #672 part 2 (#696), #678 (#692), #636 (#674), #683 r1 (#685),
#659 (#682), #641 (#680), #620 (#677, closed via #700), #635 (#675, not-a-bug),
#646 (#671), #648 (#670), #649 (#669), #595 (closed via #702), #647 (#652).
Every remaining open issue is either **design-cycle-sized** (#604/#627/#643/
#673/#628, #640 shapes 2/4/5, #683 residual-2, #788 multi-type-VLP aggregate
ORDER-BY-on-endpoint) or in the **reverse-mapping / systemic class** owned by P-4
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
- SQL-IR Phases 2–4 (path collapse, structural idioms, Raw shrink) —
  `SQL_IR_DESIGN.md`; Phase-2 A/C unification stays deferred per its own
  investigation.
- Phase 3 remaining §6.2 slices + P3.6 legacy-path deletion.
- #411 (generic `.id`) — only after P-4, per the plan.
- Denorm foreign-edge union-dimension design (perf-staged, memory notes).
- DeltaGraph live-workspace validation items (`GA_READINESS.md`).

## 3. Capacity split (guideline)

With ~4 concurrent agent lanes: 1× P-0 until green (then it folds into a
standing nightly-triage duty), 1× P-1 standing, 1–2× P-2/P-3 (then P-4
after P-2 merges), 1× P-5 S1. Re-balance here, in writing, not ad hoc.

## 4. Merge log (newest first — append on merge)

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
