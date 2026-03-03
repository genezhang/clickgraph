# LDBC SNB Benchmark Results

**Updated**: March 2026 · ClickGraph v0.6.3-dev

## Summary

| Scale | Pass | OOM | Timeout | Language Gap | Total |
|-------|------|-----|---------|-------------|-------|
| **sf0.003** (mini) | 36 | 0 | 0 | 1 | 36/37 |
| **sf1** | 36 | 0 | 0 | 1 | 36/37 |
| **sf10** | 29 | 4 | 2 | 1 | 29/36* |

\* sf10 uses 36-query test suite (bi-10, bi-15, bi-19, bi-20 excluded — no official Cypher queries).

All failures at sf10 are ClickHouse resource limits. The only ClickGraph language gap is bi-16 (CALL subquery).

## Query Status Matrix

| Query | sf0.003 | sf1 | sf10 | Notes |
|-------|---------|-----|------|-------|
| short-1 | OK | OK (19ms) | OK (<1s) | Person profile |
| short-2 | OK | OK (185ms) | OK (10-60s) | Recent messages (VLP chained WITH) |
| short-3 | OK | OK (28ms) | OK (<1s) | Friends |
| short-4 | OK | OK (6ms) | OK (<1s) | Message content |
| short-5 | OK | OK (14ms) | OK (<1s) | Message creator |
| short-6 | OK | OK (91ms) | OK (10-60s) | Forum of message |
| short-7 | OK | OK (104ms) | OK (1-10s) | Replies to message |
| complex-1 | OK | OK (478ms) | OK (10-60s) | Friends by name (VLP + schema filters) |
| complex-2 | OK | OK (362ms) | OK (1-10s) | Recent messages by friends |
| complex-3 | OK | OK (299ms) | OK (1-10s) | Friends in countries (supertype collapse) |
| complex-4 | OK | OK (118ms) | OK (1-10s) | Popular tags in time window |
| complex-5 | OK | OK (88ms) | OK (1-10s) | New groups (OPTIONAL MATCH + collect/IN) |
| complex-6 | OK | OK (73ms) | OK (10-60s) | Tag co-occurrence |
| complex-7 | OK | OK (99ms) | OK (1-10s) | Recent likes (chained map access + NOT EXISTS) |
| complex-8 | OK | OK (191ms) | OK (1-10s) | Recent replies |
| complex-9 | OK | OK (186ms) | **OOM** | Recent messages by friends-of-friends (2-hop VLP) |
| complex-10 | OK | OK (9.7s) | **OOM** | Friend birthday (VLP + arrayCount) |
| complex-11 | OK | OK (53ms) | OK (<1s) | Friends at company (no VLP) |
| complex-12 | OK | OK (385ms) | OK (1-10s) | Expert friends (multi-type VLP) |
| complex-13 | OK | OK (28ms) | **OOM** | shortestPath over full KNOWS graph |
| complex-14 | OK | OK (279ms) | **OOM** | Weighted shortest path (adapted query) |
| bi-1 | OK | OK (80ms) | OK (1-10s) | Message distribution by year/month |
| bi-2 | OK | OK (210ms) | OK (1-10s) | Tag evolution |
| bi-3 | OK | OK (290ms) | **Timeout** | Tag co-occurrence in country (VLP `*0..`) |
| bi-4 | OK | OK (47ms) | OK (1-10s) | Popular moderators |
| bi-5 | OK | OK (269ms) | OK (1-10s) | Active posters by tag |
| bi-6 | OK | OK (0.8s) | **Timeout** | Active users (selective predicate FROM reorder) |
| bi-7 | OK | OK (406ms) | OK (1-10s) | Authoritative users |
| bi-8 | OK | OK (1.2s) | OK (>60s) | Related tags (CTE-based pattern comprehension) |
| bi-9 | OK | OK (285ms) | **Timeout** | Forum with related tags (unbounded `REPLY_OF*0..`) |
| bi-11 | OK | OK (54ms) | OK (>60s) | Unrelated replies |
| bi-12 | OK | OK (99ms) | OK (1-10s) | Trending posts |
| bi-13 | OK | OK (476ms) | OK (8.7s) | Popular zombies (scoping-only WITH optimization) |
| bi-14 | OK | OK (934ms) | OK (>60s) | Top pairs by country (map property access) |
| bi-16 | **Fail** | **Fail** | **Fail** | CALL subquery -- language feature gap |
| bi-17 | OK | OK (1.4s) | **OOM** | Information propagation (adapted, multi-VLP) |
| bi-18 | OK | OK (349ms) | OK (>60s) | Friend recommendation |

## Failure Analysis

### sf1: All 36 Queries Passing

At sf1 scale, all 36 testable queries now pass with 0 timeouts and 0 OOM failures. Key improvements over prior results:

| Query | Previous | Current | Fix |
|-------|----------|---------|-----|
| complex-10 | Timeout (>300s) | 9.7s | Missing WHERE filters for fixed-length VLP + undirected edge UNION queries |
| complex-13 | OOM | 28ms | Bridge node elimination optimization |
| complex-14 | OOM | 279ms | Bridge node elimination optimization |
| bi-6 | Timeout (56.2s) | 0.8s | Selective predicate FROM reordering (70x speedup) |
| bi-9 | Timeout | 285ms | Optimization improvements |

### sf10: OOM and Timeouts

These queries exhaust ClickHouse's ~70GB server memory limit due to recursive CTE traversal over dense KNOWS graphs.

| Query | sf1 | sf10 | Root Cause |
|-------|-----|------|-----------|
| complex-9 | 186ms | 61GB OOM | 2-hop VLP over KNOWS: 9.9K persons OK, 67K persons OOM |
| complex-10 | 9.7s | 68GB OOM | VLP + arrayCount over dense graph |
| complex-13 | 28ms | 61GB OOM | shortestPath over full KNOWS graph |
| complex-14 | 279ms | OOM | Weighted shortestPath over full KNOWS graph |
| bi-17 | 1.4s | 62GB OOM | Multi-VLP adapted query |

| Query | sf1 | sf10 (300s) | Root Cause |
|-------|-----|-------------|-----------|
| bi-3 | 290ms | Timeout | Recursive VLP `*0..` over messages -- fits at sf1, too large at sf10 |
| bi-6 | 0.8s | **Timeout** | 3-level OPTIONAL MATCH — sf1 fixed by FROM reordering, sf10 too large |
| bi-9 | 285ms | Timeout | Unbounded `REPLY_OF*0..` over all messages (25M at sf10) |

### Language Gap

| Query | Issue |
|-------|-------|
| bi-16 | Uses `CALL { ... }` subquery syntax -- not yet implemented in ClickGraph |

## Adapted Queries

Two queries use adapted Cypher (equivalent semantics, different syntax):

| Query | Reason |
|-------|--------|
| complex-14 | Official uses GDS `gds.shortestPath.dijkstra` — adapted to `cost(path)` weighted VLP |
| bi-17 | Official uses `CALL` subquery — adapted to multi-VLP chained WITH pattern |

## Performance Tiers (sf1, 36 passing)

| Tier | Count | Queries |
|------|-------|---------|
| Fast (<1s) | 33 | short-1,2,3,4,5,6,7, complex-1,2,3,4,5,6,7,8,9,11,12,13,14, bi-1,2,3,4,5,6,7,9,11,12,13,14,18 |
| Medium (1-10s) | 3 | complex-10 (9.7s), bi-8 (1.2s), bi-17 (1.4s) |
| **Total** | 36 | 19.7s total, 0.5s avg |

## Data Scales

| Scale | Persons | KNOWS | Messages | Comments | Posts | Forums |
|-------|---------|-------|----------|----------|-------|--------|
| sf0.003 | 12 | 33 | ~300 | ~200 | ~100 | ~10 |
| sf1 | 9,892 | 180,623 | 3,055,774 | 2,052,169 | 1,003,605 | 90,492 |
| sf10 | 67,000 | 1,750,000 | ~25,000,000 | — | — | — |

## Key Optimizations

- **Selective predicate FROM reordering** (Mar 2026): When a WHERE filter references a joined table (e.g., `tag.name = 'value'`), promotes it to FROM position so ClickHouse filters early. Re-roots the join dependency tree and redistributes ON conditions. Fixed bi-6 from 56s to 0.8s (70x speedup) at sf1.
- **Bridge node elimination** (Mar 2026): Post-hoc optimizer removes FK pass-through node tables (e.g., intermediate join tables that only relay foreign keys). Key factor in fixing complex-13 and complex-14 OOM at sf1.
- **Fixed-length VLP WHERE filter propagation** (Mar 2026): WHERE filters now correctly propagate into fixed-length VLP + undirected edge UNION queries. Fixed complex-10 from >300s timeout to 9.7s at sf1.
- **Scoping-only WITH collapse** (Mar 2026): Detects `WITH a, b` clauses that purely pass variables through for scoping and skips CTE creation. Improved bi-13 from >300s timeout to 8.7s at sf10 scale.
- **CTE-based pattern comprehension** (Feb 2026): Pre-aggregated CTEs + LEFT JOINs instead of correlated subqueries. Enabled bi-8 to use official query.
- **Weighted VLP** (Mar 2026): `cost(path)` function support for weighted shortest path traversal (complex-14).

## Reproduction

```bash
# sf0.003 (unit test data, ~300 rows)
cargo test  # 1114+ unit tests including SQL generation tests

# sf1 (~3M messages) — load data into ClickHouse
bash benchmarks/ldbc_snb/schemas/sf1_load_data.sh

# sf10 (~25M messages) — apply column name normalization
curl 'http://localhost:18123/?user=test_user&password=test_pass' \
  --data-binary @benchmarks/ldbc_snb/schemas/sf10_normalize.sql
```

Benchmark measurements were run using custom driver scripts (not shipped in this
repository) that iterate over the 37 LDBC queries, send each to ClickGraph's
`/query` endpoint, and collect execution times and errors. To replicate:

1. Start ClickGraph pointing at the loaded ClickHouse instance
2. For each query in `benchmarks/ldbc_snb/queries/official/`, POST to `/query`
   with appropriate parameters and a per-query timeout (60s for sf1, 300s for sf10)
3. Record pass/fail/timeout/OOM status and execution time
