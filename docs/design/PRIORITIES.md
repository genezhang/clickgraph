# Work priorities & dispatch queue

Status: **canonical**. Last reconciled: 2026-07-19.

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

### P-0 — Nightly CI green + net hygiene  ☐ (open)
The nightly has failed every run since ~2026-07-13. Two distinct causes:
- 2026-07-17: `cargo fmt --check` failure from an unformatted merge
  (`graph_schema.rs`) — main is fmt-clean as of 2026-07-19; should
  self-recover. Consider a pre-merge fmt check on the merge queue to
  prevent recurrence.
- 2026-07-14..16: 9 real pytest failures needing triage (fix, issue+xfail,
  or revert): `test_spoke_pattern` (inbound spoke + bowtie, 0 rows),
  `test_optional_match_undirected` (1 expected, got 2 — check vs. #583/#589
  family), `test_graphrag_multi_type` property aggregation (400),
  `test_count_relationship_with_node_constraints` (500),
  `test_invalid_return_syntax` (parser error text drift),
  3× `matrix/test_comprehensive.py::TestUnwind::test_unwind_with_match`.
- Clear the **84 xpassed** stale xfail markers (batch PR, list from the
  nightly log).
- Also: prune the ~25 stale `worktree-agent-*` branches; refresh STATUS.md
  (last updated 2026-05-06, predates this whole workstream).
Exit: one fully green scheduled nightly run + xpass count ~0.

### P-1 — Keep a small silent-wrong bug lane open  (standing, ≤1 agent)
Open issues that are NOT the reverse-mapping class and are individually
fixable: ~~#647~~ **DONE (#652, `91475be3`)**, #644 (denorm OPTIONAL-VLP anchor
join, loud — **in flight**), #646 (composite self-ref FK-edge, loud), #641
(#589 gate holes), #640 (EXISTS beyond single-hop), #636 (4-way shared-anchor),
#635 (FK-edge coupled rel-var on VLP), #648 (untyped count(r) multi-type),
#649 (leading UNWIND). Prefer silent-wrong over loud-error fixes. Rule §1.6
applies: if root cause lands in the reverse-mapping class, gate loud + document,
move on.

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

### P-3 — Phase 2 module moves (P2.1 → P2.6, in order)  ◐ (P2.1, P2.2 done — P2.3 next)
The dead-code sweep shrank plan_builder_utils.rs to ~17.7K lines. §5.1 moves are
now underway. Pure groups first (vlp_rewrite →
pattern_comprehension_sql → clause_extractors → plan_predicates →
cte_rewrite → with_to_cte), one move per PR, no logic edits, `pub(crate)`
re-exports during transition. D-cluster dedups (D1/D2/D3/D6/D8 remainder)
ride with their §5.1 home module per the plan. **P2.1 (vlp_rewrite move) merged
(#657)** — VLP expr-rewriting group extracted to `render_plan/vlp_rewrite.rs`,
byte-identical, D3 dedup deferred (follow-up). **P2.2 (pattern_comprehension_sql move)
delivered** (`refactor/p22-pattern-comprehension-move`) — the pattern-comprehension SQL
string-emitting group (31 fns, `render_plan/pattern_comprehension_sql.rs`, 2,629 lines)
extracted verbatim, `pub(crate)` re-exports, byte-identical goldens + corpus, ratchet
net-zero; D7-rest deferred. **Next: P2.3 clause_extractors move.**

### P-4 — Phase 4 §7.2: forward resolution through CTE scope  ☐ (UNBLOCKED — plan ready, next big rock)
**Concrete staged plan written: `docs/design/FORWARD_RESOLUTION_PLAN.md`.** It
supersedes the stale `render_plan/AGENTS.md` §10 premise: the `reverse_mapping`
field §10 says to delete was already removed in #115 (Feb 2026); the debt forked
into three overlapping resolution mechanisms, with **#592** (VariableRegistry
`define_*` drops `property_mapping`; `set_property_mapping` has zero callers) as
the systemic root. The architectural fix for the open-issue residue: #592, #595,
#602, #613, #643, and the #583 render rework. Slices **F0–F6**; **start with F0**
— thread `property_mapping` through `define_*`/`*_from_cte` in `typed_variable.rs`
+ a corpus-wide `debug_assert_eq!(forward, legacy)` transition-assert (byte-
identical, zero corpus delta). Per-shape patching of this class stays forbidden
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
