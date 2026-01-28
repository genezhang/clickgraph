# OPTIONAL MATCH + Variable-Length Paths (VLP) Issue

**Date Identified**: January 25, 2026
**Status**: Active Bug
**Priority**: High (affects analytical query correctness)
**Test Case**: `complex_feature_tests::test_optional_match_with_vlp_and_aggregation`

## Problem Description

When combining `OPTIONAL MATCH` with Variable-Length Paths (VLP) and aggregations, the query returns incorrect results. Users with no matches are completely omitted from the result set instead of returning with zero counts.

## Example Query

```cypher
MATCH (u:User)
OPTIONAL MATCH (u)-[:FOLLOWS*1..3]->(f:User)
RETURN u.name, COUNT(f) as follower_count
```

## Expected Behavior

- All users should appear in results
- Users with no followers should have `follower_count = 0`
- This requires LEFT JOIN semantics

## Actual Behavior

- Only users who have followers appear in results
- Users with no followers are completely missing
- This exhibits INNER JOIN semantics

## Root Cause

The VLP implementation uses recursive CTEs which naturally implement INNER JOIN semantics. The CTE only generates rows for paths that actually exist, so users with no followers don't appear in the CTE results.

## Generated SQL (Current)

```sql
WITH RECURSIVE vlp_u_f AS (
    -- CTE generates rows only for users with followers
    SELECT start_node.user_id as start_id, ...
    FROM test.users AS start_node
    JOIN test.user_follows AS rel ON ...
    -- ... recursive joins
)
SELECT t.start_name AS "u.name", count(*) AS "follower_count"
FROM vlp_u_f AS t
```

## Required SQL (Fix)

```sql
WITH RECURSIVE vlp_u_f AS (
    -- CTE for VLP logic
    SELECT start_node.user_id as start_id, ...
    FROM test.users AS start_node
    LEFT JOIN test.user_follows AS rel ON ...
    -- ... recursive joins
)
SELECT u.full_name AS "u.name",
       COALESCE(vlp.follower_count, 0) AS "follower_count"
FROM test.users AS u
LEFT JOIN (
    SELECT start_id, COUNT(*) as follower_count
    FROM vlp_u_f
    GROUP BY start_id
) AS vlp ON u.user_id = vlp.start_id
```

## Technical Solution

Need to modify the VLP rendering logic in `render_plan/` to:

1. **Separate base table access**: Keep the base `users` table scan separate
2. **LEFT JOIN with CTE**: Use LEFT JOIN between base table and VLP CTE
3. **Handle aggregations**: Ensure aggregations work correctly with NULL values from LEFT JOIN
4. **Preserve optional semantics**: Maintain the optional behavior throughout the query pipeline

## Files to Modify

- `src/render_plan/plan_builder.rs` - VLP rendering logic
- `src/render_plan/cte_manager/` - CTE generation for VLP
- `src/clickhouse_query_generator/` - SQL generation for optional VLP

## Impact Assessment

- **High Impact**: Affects correctness of analytical queries
- **Common Pattern**: OPTIONAL MATCH + VLP + aggregation is a standard graph analytics pattern
- **User Experience**: Silent incorrect results (missing rows) are worse than errors

## Test Coverage

Added comprehensive test in `tests/rust/integration/complex_feature_tests.rs`:
- `test_optional_match_with_vlp_and_aggregation()` - validates the fix

## Related Issues

- VLP implementation in general
- Aggregation handling with optional matches
- CTE vs JOIN performance trade-offs

## Next Steps

1. Design the architectural changes needed
2. Implement LEFT JOIN logic for optional VLP
3. Update aggregation handling
4. Test with various VLP + optional combinations
5. Performance validation