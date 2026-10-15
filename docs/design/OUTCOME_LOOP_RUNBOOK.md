# Outcome-Loop Runbook (kill-switch, rollback, sampling)

## Kill-switch (baseline in under 5 minutes)

The loop is default-off everywhere; every stage has a flag-off path:

| Layer | Kill action | Effect |
|---|---|---|
| Serving | Set `min_n` absurdly high (or empty the cache tier) | All reads fall back to cohort-default/global priors |
| Scoring job | Pause the nightly cron / disable the INSERT | `score_table` freezes; serving keeps last good scores (stale, correct shape) |
| Observer | Set sample rate to 0 | No new instincts; existing guidance keeps serving |
| Strategy rollout | Set variant traffic to 0 | 100% baseline policy immediately |
| Langfuse hook | Unset `CLICKGRAPH_LANGFUSE_HOST` + restart | Zero overhead; translation path unchanged (flag-off byte-identical) |

Drill quarterly: flip each switch in staging, confirm serving output equals
the flag-off golden, flip back. Record the drill date in the experiment log.

## Rollback drill

1. Publish a deliberately bad score row to staging `score_table`.
2. Confirm the calibration alert fires (predicted vs realized outcome gap).
3. Roll back to the previous partition snapshot; confirm serving output.
4. Target: detect-to-rollback under 15 minutes.

## Sampling ladder (cost control)

| Stage | Observer | Judge evals |
|---|---|---|
| POC (≤10K convos/day) | 100% | 5% + code evaluators at 100% (free) |
| Growth | 100% rated + 10% unrated | 5%, judges only on disagreements |
| 1B-scale | 100% negatives + 0.1% sample | Same, plus weekly calibration vs humans |

Judge cost dominates: a typical assessment costs $0.01–0.10. Code
evaluators (trajectory budgets, tool-call checks) run at 100% because they
are free and deterministic.

## Incident table

| Signal | Meaning | First action |
|---|---|---|
| Score calibration gap widening | World drifted or pipeline bug | Freeze promotion, check input distributions |
| Experiment lower bound < baseline 3 days | Variant harming users | Traffic to 0, discard variant |
| Enrichment p99 > 200ms | Cache miss storm or CH slowdown | Check cache hit rate, then CH replicas |
| Feedback volume drops >50% | Widget broken or trust lost | Check ingestion, not the model |
| Propensity missing on decisions | Logger regression — OPE impossible | Block promotions until backfilled |
