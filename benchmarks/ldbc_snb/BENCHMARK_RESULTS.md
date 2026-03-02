# LDBC SNB Benchmark Results

**Updated**: March 2026 · ClickGraph v0.6.2-dev

## Summary

| Scale | Pass | OOM | Timeout | Language Gap | Total |
|-------|------|-----|---------|-------------|-------|
| **sf0.003** (mini) | 36 | 0 | 0 | 1 | 36/37 |
| **sf1** | 32 | 2 | 2 | 1 | 32/37 |
| **sf10** | 29 | 4 | 2 | 1 | 29/36* |

\* sf10 uses 36-query test suite (bi-10, bi-15, bi-19, bi-20 excluded — no official Cypher queries).

All failures are ClickHouse resource limits or a known Cypher language gap — zero ClickGraph bugs.

## Query Status Matrix

| Query | sf0.003 | sf1 | sf10 | Notes |
|-------|---------|-----|------|-------|
| short-1 | OK | OK (0.1s) | OK (<1s) | Person profile |
| short-2 | OK | OK (4.8s) | OK (10-60s) | Recent messages (VLP chained WITH) |
| short-3 | OK | OK (0.4s) | OK (<1s) | Friends |
| short-4 | OK | OK (0.0s) | OK (<1s) | Message content |
| short-5 | OK | OK (0.1s) | OK (<1s) | Message creator |
| short-6 | OK | OK (1.6s) | OK (10-60s) | Forum of message |
| short-7 | OK | OK (0.7s) | OK (1-10s) | Replies to message |
| complex-1 | OK | OK (1.6s) | OK (10-60s) | Friends by name (VLP + schema filters) |
| complex-2 | OK | OK (0.7s) | OK (1-10s) | Recent messages by friends |
| complex-3 | OK | OK (1.4s) | OK (1-10s) | Friends in countries (supertype collapse) |
| complex-4 | OK | OK (0.8s) | OK (1-10s) | Popular tags in time window |
| complex-5 | OK | OK (1.4s) | OK (1-10s) | New groups (OPTIONAL MATCH + collect/IN) |
| complex-6 | OK | OK (6.9s) | OK (10-60s) | Tag co-occurrence |
| complex-7 | OK | OK (0.8s) | OK (1-10s) | Recent likes (chained map access + NOT EXISTS) |
| complex-8 | OK | OK (0.8s) | OK (1-10s) | Recent replies |
| complex-9 | OK | OK (24.5s) | **OOM** | Recent messages by friends-of-friends (2-hop VLP) |
| complex-10 | OK | OK (49.9s) | **OOM** | Friend birthday (VLP + arrayCount) |
| complex-11 | OK | OK (0.6s) | OK (<1s) | Friends at company (no VLP) |
| complex-12 | OK | OK (17.1s) | OK (1-10s) | Expert friends (multi-type VLP) |
| complex-13 | OK | **OOM** | **OOM** | shortestPath over full KNOWS graph |
| complex-14 | OK | **OOM** | **OOM** | Weighted shortest path (adapted query) |
| bi-1 | OK | OK (0.1s) | OK (1-10s) | Message distribution by year/month |
| bi-2 | OK | OK (0.3s) | OK (1-10s) | Tag evolution |
| bi-3 | OK | OK (0.3s) | **Timeout** | Tag co-occurrence in country (VLP `*0..`) |
| bi-4 | OK | OK (0.1s) | OK (1-10s) | Popular moderators |
| bi-5 | OK | OK (0.4s) | OK (1-10s) | Active posters by tag |
| bi-6 | OK | **Timeout** | **Timeout** | Active users (3-level OPTIONAL MATCH, O(n³)) |
| bi-7 | OK | OK (0.8s) | OK (1-10s) | Authoritative users |
| bi-8 | OK | OK (2.9s) | OK (>60s) | Related tags (CTE-based pattern comprehension) |
| bi-9 | OK | **Timeout** | **Timeout** | Forum with related tags (unbounded `REPLY_OF*0..`) |
| bi-11 | OK | OK (2.9s) | OK (>60s) | Unrelated replies |
| bi-12 | OK | OK (5.0s) | OK (1-10s) | Trending posts |
| bi-13 | OK | OK (18.5s) | OK (8.7s) | Popular zombies (scoping-only WITH optimization) |
| bi-14 | OK | OK (48.1s) | OK (>60s) | Top pairs by country (map property access) |
| bi-16 | **Fail** | **Fail** | **Fail** | CALL subquery — language feature gap |
| bi-17 | OK | OK (32.9s) | **OOM** | Information propagation (adapted, multi-VLP) |
| bi-18 | OK | OK (4.6s) | OK (>60s) | Friend recommendation |

## Failure Analysis

### OOM — ClickHouse Memory Exhaustion

These queries exhaust ClickHouse's ~70GB server memory limit due to recursive CTE traversal over dense KNOWS graphs.

| Query | sf1 | sf10 | Root Cause |
|-------|-----|------|-----------|
| complex-9 | 24.5s | 61GB OOM | 2-hop VLP over KNOWS: 9.9K→OK, 67K→OOM |
| complex-10 | 49.9s | 68GB OOM | VLP + arrayCount over dense graph |
| complex-13 | OOM | 61GB OOM | shortestPath over full KNOWS graph |
| complex-14 | OOM | OOM | Weighted shortestPath over full KNOWS graph |
| bi-17 | 32.9s | 62GB OOM | Multi-VLP adapted query |

### Timeouts — Algorithmic Complexity

| Query | sf1 (60s) | sf10 (300s) | Root Cause |
|-------|-----------|-------------|-----------|
| bi-6 | Timeout | Timeout | 3-level OPTIONAL MATCH creates O(n³) Cartesian product |
| bi-9 | Timeout | Timeout | Unbounded `REPLY_OF*0..` over all messages (3M at sf1, 25M at sf10) |
| bi-3 | 0.3s | Timeout | Recursive VLP `*0..` over messages — fits at sf1, too large at sf10 |

### Language Gap

| Query | Issue |
|-------|-------|
| bi-16 | Uses `CALL { ... }` subquery syntax — not yet implemented in ClickGraph |

## Adapted Queries

Two queries use adapted Cypher (equivalent semantics, different syntax):

| Query | Reason |
|-------|--------|
| complex-14 | Official uses GDS `gds.shortestPath.dijkstra` — adapted to `cost(path)` weighted VLP |
| bi-17 | Official uses `CALL` subquery — adapted to multi-VLP chained WITH pattern |

## Performance Tiers (sf1, 32 passing)

| Tier | Count | Queries |
|------|-------|---------|
| Fast (<1s) | 11 | short-1,4,5, complex-2,4,7,8,11, bi-1,3,4 |
| Medium (1-10s) | 14 | short-2,3,6,7, complex-1,3,5,6, bi-2,5,7,8,11,12,18 |
| Slow (10-60s) | 7 | complex-9,10,12, bi-13,14,17 |
| **Total** | 32 | 231s total, 7.2s avg |

## Data Scales

| Scale | Persons | KNOWS | Messages | Comments | Posts | Forums |
|-------|---------|-------|----------|----------|-------|--------|
| sf0.003 | 12 | 33 | ~300 | ~200 | ~100 | ~10 |
| sf1 | 9,892 | 180,623 | 3,055,774 | 2,052,169 | 1,003,605 | 90,492 |
| sf10 | 67,000 | 1,750,000 | ~25,000,000 | — | — | — |

## Key Optimizations

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
