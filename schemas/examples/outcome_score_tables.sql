-- Outcome-loop backing tables (ClickHouse). Apply once per deployment:
--   clickhouse-client --queries-file schemas/examples/outcome_score_tables.sql
-- Engine choice: MergeTree throughout, monthly partitions, 13-month TTL.
-- No FINAL anywhere on these tables (event/score data is append-only).
-- Mirrors docs/design/OUTCOME_LOOP.md sections 2-4.

CREATE DATABASE IF NOT EXISTS outcome;

-- Every agent decision, propensity logged (without it, IPS/DR evaluation
-- is impossible). Written by consumers in ~1K-row batches, never per message.
CREATE TABLE IF NOT EXISTS outcome.decision_log
(
    ts DateTime64(3, 'UTC'),
    user_id String,
    cohort_id LowCardinality(String),
    context_hash String,
    action_id String,
    propensity Float32,
    policy_version LowCardinality(String),
    trace_id String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (cohort_id, action_id, ts)
TTL ts + INTERVAL 13 MONTH;

-- Measured results, resolving days/weeks after the decision (same rec_id).
CREATE TABLE IF NOT EXISTS outcome.outcomes
(
    ts DateTime64(3, 'UTC'),
    user_id String,
    action_id String,
    rec_id String,
    weight Float32,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (action_id, resolved_at)
TTL ts + INTERVAL 13 MONTH;

-- Explicit +1/-1 signals (maps to Langfuse user_feedback boolean scores).
CREATE TABLE IF NOT EXISTS outcome.feedback
(
    ts DateTime64(3, 'UTC'),
    user_id String,
    action_id String,
    signal Int8,
    trace_id String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (action_id, ts)
TTL ts + INTERVAL 13 MONTH;

-- Serving contract: one row per (cohort, action). Columns are append-only
-- across versions; readers ignore unknown fields, never reject.
-- Rebuilt nightly; serving reads this table (or its cache), never the graph.
CREATE TABLE IF NOT EXISTS outcome.score_table
(
    cohort_id LowCardinality(String),
    action_id String,
    score Float32,
    ci_low Float32,
    ci_high Float32,
    n UInt64,
    policy_version LowCardinality(String),
    updated_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY tuple()
ORDER BY (cohort_id, action_id);

-- Strategy-variant windows. promote verdicts require approved_by (human).
CREATE TABLE IF NOT EXISTS outcome.strategy_experiments
(
    experiment_id String,
    variant_id String,
    cohort_id LowCardinality(String),
    strategy_text_hash String,
    window_start DateTime,
    window_end DateTime,
    traffic_share Float32,
    outcome_rate Float32,
    lower_bound Float32,
    baseline_rate Float32,
    verdict LowCardinality(String),
    approved_by String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (experiment_id, variant_id)
TTL window_start + INTERVAL 13 MONTH;

-- Nightly scoring job (effectiveness per cohort x action, 30-day window).
-- INSERT INTO outcome.score_table SELECT ... ; then refresh the cache tier.
-- INSERT INTO outcome.score_table
-- SELECT
--     d.cohort_id AS cohort_id,
--     d.action_id AS action_id,
--     avg(o.weight) AS score,
--     avg(o.weight) AS ci_low,
--     avg(o.weight) AS ci_high,
--     count() AS n,
--     d.policy_version AS policy_version,
--     now() AS updated_at
-- FROM outcome.decision_log AS d
-- INNER JOIN outcome.outcomes AS o ON o.rec_id = d.trace_id
-- WHERE o.resolved_at >= now() - INTERVAL 30 DAY
-- GROUP BY d.cohort_id, d.action_id, d.policy_version;

-- Serving read (point lookup, the only hot-path query in the loop):
-- SELECT action_id, score FROM outcome.score_table
-- WHERE cohort_id = {cohort:String} ORDER BY score DESC LIMIT 10;

-- Time-decayed personal blend (read-time pattern, no stored procedure):
-- SELECT action_id,
--        sum(weight * exp(-dateDiff('day', resolved_at, now()) / 30.0)) AS decayed
-- FROM outcome.outcomes
-- WHERE user_id = {uid:String} AND resolved_at >= now() - INTERVAL 90 DAY
-- GROUP BY action_id ORDER BY decayed DESC LIMIT 50;
