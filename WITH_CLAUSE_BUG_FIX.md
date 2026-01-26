# WITH Clause Object Passing Bug Fixes

## Date: January 25, 2026

## Summary

Found and fixed two bugs in WITH clause object passing when the object is used in subsequent MATCH patterns for JOINs.

## Test Query

```cypher
MATCH (u:User)
WITH u as user_obj
MATCH (user_obj)-[:FOLLOWS]->(f:User)
RETURN user_obj.name, f.name
```

## Bugs Identified

### Bug 1: CTE Name Mismatch (Test 2)
**Error**: `Unknown table expression identifier 'with_user_obj_username_cte'`

- CTE created as: `with_user_obj_username_cte_1`
- But FROM references: `with_user_obj_username_cte` (missing `_1` suffix)
- **Root Cause**: CTE name generation includes sequence suffix, but table reference doesn't

### Bug 2: Wrong Column in JOIN Condition (Test 3)
**Error**: `Identifier 'user_obj.user_obj_id' cannot be resolved`

- JOIN condition: `t2.follower_id = user_obj.user_obj_id`  
- But CTE exports: `u_user_id` (not `user_obj_id`)
- **Root Cause**: JOIN column resolution constructs `{alias}_{id_column}` instead of looking up actual CTE column

## Generated SQL (Buggy)

```sql
WITH with_user_obj_cte_1 AS (
  SELECT u.user_id AS "u_user_id", u.full_name AS "u_name", ...
  FROM brahmand.users_bench AS u  
)
SELECT user_obj.name, f.full_name
FROM with_user_obj_cte_1 AS user_obj
INNER JOIN brahmand.user_follows_bench AS t2 ON t2.follower_id = user_obj.user_obj_id  -- ❌ Wrong column!
INNER JOIN brahmand.users_bench AS f ON f.user_id = t2.followed_id
```

## Expected SQL (Fixed)

```sql
WITH with_user_obj_cte_1 AS (
  SELECT u.user_id AS "u_user_id", u.full_name AS "u_name", ...
  FROM brahmand.users_bench AS u
)
SELECT user_obj.u_name, f.full_name  
FROM with_user_obj_cte_1 AS user_obj
INNER JOIN brahmand.user_follows_bench AS t2 ON t2.follower_id = user_obj.u_user_id  -- ✅ Correct column!
INNER JOIN brahmand.users_bench AS f ON f.user_id = t2.followed_id
```

## Fix Strategy

### Fix for Bug 2 (Column Resolution)

The `resolve_column` function in `graph_join_inference.rs` already exists and should work. The problem is that when it's called with the CTE name, it should look up the property mapping.

**Key Insight**: The CTE columns are registered with prefixed names like `u_user_id`, but the schema column is `user_id`. The `resolve_column` needs to properly map `user_id` → `u_user_id` when querying a CTE.

**Files to Fix**:
1. `src/query_planner/analyzer/graph_join_inference.rs` - `resolve_column` method
2. `src/query_planner/plan_ctx/mod.rs` - `get_cte_column` method

The fix is to ensure CTE column mappings are properly registered and looked up.

## Status

- [x] Bugs identified
- [x] Root cause analyzed  
- [ ] Fixes implemented
- [ ] Tests passing

## Next Steps

1. Implement fix for Bug 2 (column resolution)
2. Debug and fix Bug 1 if it still occurs  
3. Run test to verify both bugs are fixed
4. Remove `#[ignore]` from test
