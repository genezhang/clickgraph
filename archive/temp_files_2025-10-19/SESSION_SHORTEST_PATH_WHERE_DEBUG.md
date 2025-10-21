# Shortest Path WHERE Clause Debugging Session

**Date**: October 19, 2025
**Status**: Partial fix applied, root cause identified

## Problem
Queries like `MATCH shortestPath((a:User)-[:FOLLOWS*]-(b:User)) WHERE a.name='Alice' AND b.name='Bob' RETURN a.name, b.name` fail with:
```
Unknown expression identifier `t.start_name` in scope SELECT t.start_name, t.end_name
```

## Root Causes Identified

###Fix 1: ✅ SELECT Item Rewriting (APPLIED)
**Problem**: SELECT expressions weren't being rewritten to use CTE column names
**Location**: `brahmand/src/render_plan/plan_builder.rs` line ~1793
**Fix Applied**: Added rewriting loop for `final_select_items` (same pattern as GROUP BY and ORDER BY)
```rust
// Rewrite SELECT expressions for variable-length paths
if let Some((left_alias, right_alias)) = has_variable_length_rel(self) {
    let path_var = get_path_variable(self);
    final_select_items = final_select_items.into_iter().map(|item| {
        SelectItem {
            expression: rewrite_expr_for_var_len_cte(&item.expression, &left_alias, &right_alias, path_var.as_deref()),
            col_alias: item.col_alias,
        }
    }).collect();
}
```

### Issue #2: 🔧 Property Extraction (NOT YET FIXED)
**Problem**: CTE is generated without property columns because `extract_var_len_properties` can't find them
**Location**: `brahmand/src/render_plan/plan_builder.rs` lines 540-600

**Why it fails**:
1. `get_variable_length_info()` (line 502) returns Cypher aliases ("a", "b") as "labels"
2. `extract_var_len_properties()` tries to use these aliases to lookup schema: `map_property_to_column_with_schema("name", "a")`  ❌
3. Schema lookup by alias fails, fallback mapping is used
4. But even fallback doesn't help because the property list ends up empty

**Root cause**: We're trying to re-derive property mappings from schema using wrong keys

**Solution**: ViewScan already has the `property_mapping` HashMap! Use it directly:
- ViewScan has field: `pub property_mapping: HashMap<String, String>` 
- This maps "name" → "full_name" correctly
- Need to extract this from the ViewScan in GraphNode.input

**Implementation plan**:
1. Create helper: `fn extract_property_mapping_from_plan(plan: &LogicalPlan) -> HashMap<String, String>`
2. In `extract_var_len_properties`, extract mappings from both left and right GraphNodes
3. Use these mappings directly instead of schema lookup

## Test Data
Database: `social`
Config: `social_network.yaml`
Tables:
- `users` (user_id, full_name, email_address, registration_date, is_active)
- `user_follows` (follower_id, followed_id, follow_date)

Test users:
- Alice Johnson, Bob Smith, Carol Brown, David Lee, Eve Martinez, Frank Wilson

## Files Modified
- ✅ `brahmand/src/render_plan/plan_builder.rs` - Added SELECT item rewriting

## Files to Modify Next
- `brahmand/src/render_plan/plan_builder.rs` - Fix `extract_var_len_properties` to use ViewScan's property_mapping
- Add helper function to extract property mappings from ViewScan

## Testing
Current test: `test_where_clause_current.py`
Server start: `cmd /c start_server_social.bat`

## References
- Notes: `notes/shortest-path.md`
- Status: `STATUS.md`
- ViewScan implementation: `brahmand/src/query_planner/logical_plan/view_scan.rs`
