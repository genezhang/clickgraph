# Bidirectional Pattern Implementation: UNION ALL Approach

**Date**: November 29, 2025
**Status**: Planned improvement

## Problem

Undirected relationship patterns like `(a)-[r]-(b)` need to match both directions:
- `a -> b` (a is follower, b is followed)
- `b -> a` (b is follower, a is followed)

## Current Approach (OR in JOIN)

The current implementation uses OR conditions in JOIN clauses:

```sql
FROM users AS a
INNER JOIN follows AS r ON (r.follower_id = a.user_id OR r.followed_id = a.user_id)
INNER JOIN users AS b ON (b.user_id = r.followed_id OR b.user_id = r.follower_id) 
                         AND b.user_id != a.user_id
```

### Issues with OR in JOIN

1. **ClickHouse limitation**: OR conditions in JOINs don't work correctly - some rows are missed
2. **Performance**: OR conditions prevent optimal JOIN strategies
3. **Complexity**: Hard to reason about and debug

## Better Approach: UNION ALL

Split into two queries and combine with UNION ALL:

```sql
-- Direction 1: a follows b
SELECT a.name, b.name
FROM users AS a
JOIN follows AS r ON r.follower_id = a.user_id
JOIN users AS b ON b.user_id = r.followed_id

UNION ALL

-- Direction 2: b follows a  
SELECT a.name, b.name
FROM users AS a
JOIN follows AS r ON r.followed_id = a.user_id
JOIN users AS b ON b.user_id = r.follower_id
```

### Advantages

1. **Correct results**: Each direction uses simple equi-joins that ClickHouse handles perfectly
2. **Performance**: Equi-joins are optimized, UNION ALL is efficient
3. **Clarity**: Each branch is simple and easy to understand
4. **Proven**: We tested this manually and got correct results (5 rows including Alice)

## Verified Test Case

Query: `MATCH (a:User)-[r:FOLLOWS]-(b:User) RETURN a.name, b.name`

Expected: Each relationship appears twice (once per direction) = 4 relationships × 2 = 8 rows

UNION ALL approach returns correct 8 rows.

## Implementation Plan

1. In `graph_join_inference.rs`, detect `Direction::Either` patterns
2. Instead of generating OR-based joins, generate two separate join sequences
3. Wrap in UNION ALL at the SQL generation stage
4. Handle deduplication if needed (UNION vs UNION ALL, or add WHERE conditions)

## Related Files

- `src/query_planner/analyzer/graph_join_inference.rs` - JOIN generation
- `src/clickhouse_query_generator/mod.rs` - SQL generation
- `tests/integration/test_relationships.py` - Integration tests

## Test Cases Affected

- `test_undirected_relationship` - Currently failing with OR approach
- `test_relationship_degree` - Returns 4 instead of 5 (Alice missing)
- `test_mutual_follows` - Cyclic pattern, separate issue
- `test_triangle_pattern` - Cyclic pattern, separate issue
