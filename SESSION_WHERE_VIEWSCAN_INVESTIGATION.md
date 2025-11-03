# Session: WHERE Clause ViewScan Investigation

**Date**: November 2, 2025  
**Status**: ❌ Incomplete - Root cause found, fix attempted but not working yet  
**Test Results**: 5/19 basic_queries passing (26%) - no improvement

## Problem Statement

WHERE clauses are ignored for simple MATCH queries:
```cypher
MATCH (u:User) WHERE u.name = 'Alice' RETURN u.name
-- Returns all 5 rows instead of just 1
```

However, WHERE clauses work fine for variable-length paths (318 unit tests passing).

## Root Cause Discovery ✓

### Initial Hypothesis (WRONG)
We thought Filter → ViewScan patterns existed but weren't being optimized.

### Actual Root Cause (CORRECT)
**Filter nodes DON'T EXIST by the time optimizer runs!**

1. **FilterTagging Analyzer** (runs BEFORE optimizer):
   - Extracts filter predicates from Filter nodes
   - Stores them in `plan_ctx.alias_table_ctx_map[alias].filters`
   - **REMOVES the Filter node from the plan tree**
   - See: `brahmand/src/query_planner/analyzer/filter_tagging.rs` lines 82-87

2. **Analyzer → Optimizer Pipeline**:
   ```
   Query Planning → Analyzer (incl. FilterTagging) → Optimizer (incl. FilterIntoGraphRel)
   ```

3. **Why GraphRel Works**:
   - `FilterIntoGraphRel` optimizer reads filters from `plan_ctx`
   - See lines 235-257 in `filter_into_graph_rel.rs`
   - Injects them into `GraphRel.where_predicate`

4. **Why ViewScan Fails**:
   - No code to read filters from `plan_ctx` and inject into `ViewScan.view_filter`
   - Filters exist in plan_ctx but never make it to SQL generation

## Code Changes Made

### File: `brahmand/src/query_planner/optimizer/filter_into_graph_rel.rs`

**Removed old (wrong) approach**: Lines 131-202
- Tried to find Filter → Projection → ViewScan patterns
- Won't work because Filter nodes are already removed

**Added new approach**: Lines 309-373
- Handle `LogicalPlan::ViewScan` case
- Iterate through `plan_ctx.get_alias_table_ctx_map()`
- Find table_ctx with filters
- Inject into `ViewScan.view_filter`
- Added debug println! statements

### Current State
Code compiles but **doesn't work** - still returns all 5 rows.

## Debugging Evidence Needed

The fix is partially implemented but not working. **Before next session**, check:

1. **Does ViewScan handler get called?**
   - Look for: `"FilterIntoGraphRel: ENTERED ViewScan handler"`
   - If NO → ViewScan might be wrapped differently in plan tree
   - If YES → Continue to #2

2. **What's in plan_ctx?**
   - Look for: `"FilterIntoGraphRel: plan_ctx has X aliases"`
   - Look for: `"FilterIntoGraphRel: Checking alias 'u' with label..."`
   - Expected: Should see alias 'u' with filters
   - If filters are empty → FilterTagging issue
   - If filters exist → Filter injection issue

3. **Check generated SQL**:
   - Does `ViewScan.view_filter` contain the predicate?
   - Does SQL generation in `view_query.rs` use `view_filter`?

## Next Steps

### Option A: Debug Current Implementation
1. Start server: `target\release\clickgraph.exe --http-port 8081`
2. Run test: `python test_where_simple.py`
3. Check server output for println! debug messages
4. Determine:
   - Is ViewScan handler called? 
   - Are filters in plan_ctx?
   - Are filters being applied to ViewScan?

### Option B: Check FilterTagging Behavior
Maybe FilterTagging handles ViewScan differently than GraphRel:
- Check `analyzer/filter_tagging.rs` line 64: `LogicalPlan::ViewScan(_) => Transformed::No`
- ViewScan might be skipped by FilterTagging!
- If so, filters never make it to plan_ctx for ViewScan nodes

### Option C: Alternative Architecture
If FilterTagging doesn't work for ViewScan:
- Keep Filter nodes for ViewScan (don't remove them)
- Or: Special case ViewScan in FilterTagging
- Or: Different optimizer pass for ViewScan filters

## Test Infrastructure

**Database**: test_integration (ClickHouse)  
**Schema**: test_graph_schema (YAML config)  
**Test data**: 5 users (Alice, Bob, Charlie, Diana, Eve)  
**Port**: 8081 (8080 has conflicts)

**Quick test**:
```bash
# Start server
target\release\clickgraph.exe --http-port 8081 --disable-bolt

# Run test
python test_where_simple.py
```

**Expected**: 1 row (Alice)  
**Actual**: 5 rows (all users)

## Files Modified

- `brahmand/src/query_planner/optimizer/filter_into_graph_rel.rs`
  - Lines 309-373: ViewScan filter injection (NOT WORKING)
  - Lines 131-202: Old Filter → ViewScan code (REMOVED - wrong approach)

## Key Code References

**FilterTagging removes Filter nodes**:
- `brahmand/src/query_planner/analyzer/filter_tagging.rs:82-87`

**GraphRel reads from plan_ctx** (working example):
- `brahmand/src/query_planner/optimizer/filter_into_graph_rel.rs:235-257`

**ViewScan SQL generation** (uses view_filter):
- `brahmand/src/clickhouse_query_generator/view_query.rs:58-61`

**Analyzer → Optimizer pipeline**:
- `brahmand/src/query_planner/analyzer/mod.rs:61` (FilterTagging)
- `brahmand/src/query_planner/optimizer/mod.rs:62` (FilterIntoGraphRel)

## Lessons Learned

1. **Test coverage gaps matter**: 318 passing tests ALL test GraphRel (variable-length paths), NONE test ViewScan (simple MATCH)

2. **Architecture understanding critical**: Spent hours on wrong approach (looking for Filter nodes that don't exist)

3. **Follow the working code**: GraphRel shows the right pattern - read from plan_ctx, not from plan tree

4. **Debugging is essential**: Can't fix blind - need to see what's actually in plan_ctx when ViewScan handler runs

## Success Criteria

✅ **Done when**: `test_where_simple.py` returns 1 row  
✅ **Validation**: `pytest tests/integration/test_basic_queries.py::TestWhereClause -v` → 5/5 passing  
✅ **Full suite**: Expect 15+/19 passing (up from 5/19)

---

**Recommendation**: Start fresh session focusing on debugging server output to see exactly what's happening in the ViewScan handler.
