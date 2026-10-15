# Outcome-Learning Loop on ClickGraph (spec v0.2, repo scope)

Compounding personalization moat: every agent decision is logged with its
propensity, every outcome flows back as a graph edge, policy improves through
measured loops. Competitors can clone code; they cannot clone
`{(context, action, propensity, reward)}` logs at scale.

This document is the buildable subset for this repo. The platform pieces
(Pulsar consumers, MCP serving, bandit services) live in downstream repos and
only their contracts are defined here.

## 1. The loop (generic)

```
decision --DECIDED(propensity)--> action        (write path, batched INSERTs)
action   --OUTCOME(weight)------> result        (days/weeks later, same tables)
person   --FEEDBACK(+1/-1)------> action        (explicit signal)
nightly job: (cohort, action) effectiveness --> score_table  (serving cache)
serve: point lookup, never a live graph walk
```

Same taxonomy across domains: learning `RECOMMENDED→OUTCOME`,
commerce `SHOWN→PURCHASED`, support `SUGGESTED→RESOLVED`, ops
`MITIGATED→RECOVERED`. See `OUTCOME_RECIPES.md`.

## 2. Hyperpersonalization enrichment (per visit)

Three tiers, blended with versioned weights (personal 0.5 / cohort 0.35 /
global 0.15, in `strategy.md`):

1. **Personal** — recent-N `DECIDED/OUTCOME/FEEDBACK` edges for this user.
   Served as one indexed point-range query on `decision_log(user_id, ts)`,
   never a graph walk. Budget: ≤50ms cached, ≤200ms p99 fallback.
2. **Cohort** — top-k score rows for the user's cohorts from `score_table`.
3. **Global priors** — cold start is priors-only; no special-casing
   (hierarchical shrinkage global → cohort → personal handles it).

Inject a ≤15-line block into agent context. Write back every turn's decision
(with propensity) and eventual outcome. Metrics: blend-hit rate, lift vs
cohort-default, enrichment latency, cold-start coverage.

## 3. Policy layer (RL, best-practice mapping)

- **Propensity logging is mandatory** on every decision row. Without it,
  off-policy evaluation is mathematically impossible.
- **Explore by design:** ε-greedy floor (≥5%) or Thompson sampling.
  Deterministic argmax logging makes IPS/DR estimators degenerate.
- **Offline ladder:** SNIPS default (hyperparameter-free, most robust);
  doubly-robust with shrinkage once a reward model exists. Estimator
  selection needs ≥2 logging policies — keep previous policy logs.
- **Reward:** `0.6·outcome + 0.2·satisfaction − 0.2·cost`, time-decayed
  (half-life per domain), 30-day attribution window with interim proxies.
- **Pessimistic promotion:** lower confidence bound must beat baseline;
  ≤5% blast radius; auto-rollback on metric drop (<5 min, flag-off).
- **Non-stationarity:** fixed re-fit windows + change-point monitoring per
  cohort; cohorts capped at ~200.

## 4. Scale contract (1B users; assumes 50M DAU in-loop)

- Writes ~100M rows/day (~1.2K/s avg): batched INSERTs, MergeTree, monthly
  partitions, 13-month TTL (~6TB). No FINAL on event/score tables.
- Serving ~250M lookups/day: (cohort, action) cardinality ≤ ~2M keys →
  cache tier (99% hit) + ClickHouse point-lookup fallback.
- SLA <2s p99 per turn: graph slice ≤200ms p99 (alerted); the LLM call
  dominates, the graph must never be the long pole.
- The serving read shape (selective point lookup on a narrow table) is
  exactly what ClickHouse's automatic PREWHERE move already covers — no
  custom pushdown work (see closed #1385 for the measured evidence).

## 5. Evaluation (agent + LLM metrics, Langfuse)

One metric per dimension to start; expand only after a metric catches a real
regression. Trace hook: `src/server/langfuse.rs` (env-gated, default off,
fire-and-forget, PII redaction default-on). Importable score definitions
live in `langfuse/score-configs/`; the executable regression dataset in
`langfuse/datasets/outcome-regression.jsonl` (run by
`outcome_loop_example_tests.rs`; failing production traces land there as new
items). Operator procedures in `OUTCOME_LOOP_RUNBOOK.md`.

| Dimension | Metric | Langfuse mechanism |
|---|---|---|
| Trajectory | step count vs budget, loop/retry rate | numeric observation score |
| Tool use | correct-tool boolean, arg validity | code evaluator on structured tool calls (free, 100%) |
| Task completion | goal-achievement boolean | observation-level judge on root observation |
| Multi-turn | session resolution, turns-to-resolution | session-level scores → annotation queues → datasets |
| Loop health | score calibration, bandit regret, experiment velocity | dashboards + Metrics API |
| System | per-hop p50/p99, score-freshness lag | trace metadata + alerts |

CI gate: experiment-action on the golden dataset; `RegressionError` fails
the PR. Production failures become dataset items (permanent regressions).

## 6. SOLID + compatibility (new code only)

- **SRP:** event-build / redact / send separated; scoring separate from serving.
- **OCP:** dialect dispatch via existing `FunctionMapper`/`SqlDialect` traits;
  new edge types via schema — zero new core branches.
- **LSP/ISP/DIP:** both mappers honor the trait; hook exposes build/send
  only; renderer reads `get_current_*` accessors, never env.
- **Backward:** flag-off byte-identical SQL (corpus gate); additive-only
  schema; versioned method names (`*_v1`); existing endpoints untouched.
- **Forward:** score columns append-only; unknown edge types ignored, never
  rejected; strategy versions carry migration notes.

## 7. Harness alignment (pattern-level, no dependency)

deepseek-harness precedent applied as shape, not code: everything-is-a-plugin
(schemas, skills, hook module separable), versioned eval datasets under
`tests/`, contributor contract in README/AGENTS. ECC precedent: hook →
observe → instinct → evolve ordering for Loop 2; verifier-as-number before
any second agent (Karpathy autoresearch mechanics).

## 8. What ships in this repo (and what does not)

Ships: this doc, `OUTCOME_RECIPES.md`, `OUTCOME_LOOP_RUNBOOK.md`,
`people_outcome_graph.yaml`, `outcome_score_tables.sql` (backing-table DDL
plus the nightly/serving/decay queries, proven live against ClickHouse
25.8), `contracts/` (versioned JSON Schemas + MCP tool definitions + OTEL
span mapping), `langfuse/` (score configs + executable regression dataset),
`src/server/langfuse.rs` (+ tests), `.github/workflows/outcome-eval.yml`
(dataset + contract gates; Langfuse cloud step optional),
`skills/` (verify + spec-loop), archify diagrams (below), README +
CHANGELOG entries, translation tests for the example and recipes.

Does not ship here: consumers, MCP serving, bandit services, Langfuse
server itself, per-request toggles (cache-key hazard), non-MergeTree
allowlists, hot-path graph walks.

## 9. Diagrams

- `.archify/architecture-outcome-loop-20261015-153000/outcome-loop.html` — system map.
- `.archify/dataflow-outcome-evidence-20261015-153000/outcome-evidence.html` — decision →
  outcome → score → serve lineage.

## 10. Completion gates (push only when ALL green)

G1 `cargo fmt --check` · G2 clippy no new warnings · G3 lib tests (minus
known pre-existing failure) · G4 integration + corpus zero content-diff ·
G5 example/recipe translation tests · G6 archify finalize passed · G7 docs
complete · G8 skills structure checklist.
