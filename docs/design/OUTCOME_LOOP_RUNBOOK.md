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

## Measured scenario (ClickHouse 25.8, self-hosted)

30-day learning-platform fixture with known ground truth (matched
cohort-skill 0.8, neutral 0.5, mismatched 0.3, uniform 0.25 exploration
policy, 300K decisions / 210K outcomes / 30K feedback):

- Nightly aggregation recovered the truth exactly: eng-mid→skill-A 0.798,
  eng-senior→skill-B 0.799, mgr→skill-C 0.798; global top skill-D 0.498.
- 7-day holdout: cohort-top policy 0.7995 (n=12,544) vs global-top policy
  0.5000 (n=12,637) → **personalization lift +0.30**.
- Calibration gap (predicted score vs realized holdout rate) ≤ 0.0032 per
  cohort — far under the 10% gate.

Reproduce: apply `schemas/examples/outcome_score_tables.sql`, seed as in
`skills/outcome-loop-verify/references/evidence-protocol.md`, run the
nightly aggregation, then the holdout queries above.

## Langfuse self-host notes (verified on v4.59)

- ClickHouse needs embedded Keeper + `{cluster, shard, replica}` macros or
  the `ON CLUSTER` migrations fail (single-node stock config has neither).
- S3 (Minio) uploads need a real region (`us-east-1`, not `auto`) and a
  pre-created bucket, or ingestion 500s on "Failed to upload JSON to S3".
- Seeding needs `LANGFUSE_INIT_ORG_ID` alongside the project/user vars, or
  everything init-related is silently ignored.
- v4 events-only servers accept only score events on `/api/public/ingestion`;
  traces go through OTLP (`/api/public/otel/v1/traces`). Readable stores are
  `events_full`/`scores`, not the legacy `traces` projection (whose
  dual-write cursor is a known stall point upstream).
- Container clock skew breaks time-partitioned pipelines silently: keep the
  Docker VM clock synced or backdate test spans.

## Incident table

| Signal | Meaning | First action |
|---|---|---|
| Score calibration gap widening | World drifted or pipeline bug | Freeze promotion, check input distributions |
| Experiment lower bound < baseline 3 days | Variant harming users | Traffic to 0, discard variant |
| Enrichment p99 > 200ms | Cache miss storm or CH slowdown | Check cache hit rate, then CH replicas |
| Feedback volume drops >50% | Widget broken or trust lost | Check ingestion, not the model |
| Propensity missing on decisions | Logger regression — OPE impossible | Block promotions until backfilled |
