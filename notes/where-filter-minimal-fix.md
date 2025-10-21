# WHERE Filter Fix - Minimal Incremental Approach

## Problem Summary

**Infrastructure Status**: ✅ **100% COMPLETE**
- Filter categorization logic exists (`categorize_filters()` in plan_builder.rs)
- Start/end filter split logic works correctly  
- CTE generator supports start_node_filters and end_node_filters parameters
- 3-tier CTE structure exists for end node filtering

**Actual Bug**: WHERE filters aren't reaching the CTE generation code
- `extract_filters()` returns `None` even when WHERE clause exists in query
- Filters are likely being applied in final SELECT instead of being passed to CTE generator
- Need to trace where filters are being "consumed" before reaching GraphRel processing

## Debug Evidence

Query with WHERE clause:
```cypher
MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User))
WHERE a.name = 'Alice Johnson'
RETURN a.name, b.name
```

Debug output shows:
```
[WHERE_FILTER_DEBUG] Before categorization:
  Filter expression exists: false
  
[FILTER_CATEGORIZATION] Categorizing filters:
  Start alias: a
  End alias: b
  No filter expression provided
```

## Root Cause Hypothesis

The logical plan structure is likely:
```
Limit
 └─ Projection (RETURN)
     └─ Filter (WHERE) ← Filters consumed here?
         └─ GraphRel (MATCH pattern)
```

When `extract_filters()` is called on the plan, it should recursively find the Filter node and return its predicate. But it's returning None, which means:

1. **Option A**: Filters are being consumed/applied before CTE generation
2. **Option B**: The plan_builder is processing a sub-tree that doesn't include the Filter node  
3. **Option C**: `extract_filters()` logic has a bug in recursion

## Minimal Fix Strategy

### Step 1: Add Plan Structure Logging (DONE)
- ✅ Added debug logs to see if filters exist
- ✅ Confirmed filters are NOT reaching categorization

### Step 2: Trace Filter Application (NEXT)
Find where filters are being applied to understand the execution flow:
- Check `extract_final_filters()` vs `extract_filters()` usage
- Find where CTEs are being generated in the render pipeline
- Determine if filters are applied before or after CTE extraction

### Step 3: Minimal Code Change
Once we know where filters are being lost, make the SMALLEST possible change:
- If filters are consumed early: Pass them through as parameter
- If wrong extraction method: Use correct method  
- If recursion bug: Fix the recursion logic

**DO NOT**: Rewrite property extraction, context management, or other systems!

## Test Validation

Simple test to verify fix works:
```python
# Query 1: Start node filter
query = "MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WHERE a.name = 'Alice' RETURN a.name"
# Expected: Only paths starting from Alice
# Current: Returns paths starting from anyone

# Query 2: End node filter  
query = "MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WHERE b.name = 'Bob' RETURN b.name"
# Expected: Only paths ending at Bob
# Current: Returns paths ending at anyone
```

## Success Criteria

- ✅ Infrastructure complete (already done)
- ⏳ Filters reach CTE generation code (in progress)
- ⏳ Filters categorized correctly into start/end
- ⏳ Generated SQL has WHERE in correct places
- ⏳ Test queries return correct filtered results
- ✅ No regression in existing 274 tests
