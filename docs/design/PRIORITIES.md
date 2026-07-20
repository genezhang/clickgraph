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
fixable: #647 (end-anchored OPTIONAL VLP render), #646 (composite self-ref
FK-edge, loud), #644 (denorm OPTIONAL-VLP anchor join, loud), #641 (#589
gate holes), #640 (EXISTS beyond single-hop), #636 (4-way shared-anchor),
#635 (FK-edge coupled rel-var on VLP). Prefer silent-wrong over loud-error
fixes. Rule §1.6 applies: if root cause lands in the reverse-mapping class,
gate loud + document, move on.

### P-2 — P1.2: the five WITH functions  ☐ (open — next refactor slice)
`REFACTORING_SAFETY_PLAN.md` §4.2, verbatim: characterize current answers
of all five over the synthetic-plan matrix → decide semantics (divergences
become named parameters, never silent fixes) → unify on an exhaustive
`walk()` (building the missing P1.1 `walk()`/`any_node()`/`find_map_node`
API as part of this slice) → handle write variants. This was the plan's
"highest-value migration" and was skipped by the sweep. It is the stated
precondition for P-4 (§7.2), together with P-3.
Exit: corpus byte-identical; characterization tests locked to decided
semantics; `render_plan/AGENTS.md` §6 rewritten from "five functions must
agree" to "walk() is exhaustive; barriers are explicit".

### P-3 — Phase 2 module moves (P2.1 → P2.6, in order)  ☐ (open)
The dead-code sweep shrank plan_builder_utils.rs to 17,249 lines, but no
§5.1 *moves* have happened. Pure groups first (vlp_rewrite →
pattern_comprehension_sql → clause_extractors → plan_predicates →
cte_rewrite → with_to_cte), one move per PR, no logic edits, `pub(crate)`
re-exports during transition. D-cluster dedups (D1/D2/D3/D6/D8 remainder)
ride with their §5.1 home module per the plan. Can proceed in parallel
with P-2 (different files).

### P-4 — Phase 4 §7.2: forward resolution through CTE scope  ☐ (blocked)
**Blocked on P-2 and substantially eased by P-3.** The architectural fix
for the open-issue residue: #592 (VariableRegistry drops property_mapping —
systemic), #595, #602, #613, #643, and the #583 render rework. Design
already written: `render_plan/AGENTS.md` §10 (3 phases). When P-2 merges,
this becomes the main refactor lane and gets the bulk of agent capacity;
per-shape patching of this class stays forbidden (§1.6).
Remaining Phase-1 pass migrations (P1.4+) and Phase-3 §6.2 slices are
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

- 2026-07-19: #645 reversed OPTIONAL-VLP anchor gate (68666fda); #632
  self-ref FK-edge join inversion (94e788cb); #621 OPTIONAL-VLP anchor
  gate fold (006ccc0d). Doc created; §9 of REFACTORING_SAFETY_PLAN.md
  reconciled.
