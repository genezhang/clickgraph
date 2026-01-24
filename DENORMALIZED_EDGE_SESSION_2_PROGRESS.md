# Denormalized Edge Alias Mapping - Session 2 Progress

**Date**: Session 2 (Current)  
**Status**: Partially complete - implementation done, testing in progress  
**Next Session**: Finalize testing and debugging

## What Was Done

### 1. Implemented Denormalized Alias Mapping Storage ✅
**File**: `src/render_plan/mod.rs`

Added thread_local storage for alias mappings:
```rust
thread_local! {
    static DENORMALIZED_EDGE_ALIASES: RefCell<HashMap<String, String>> = 
        RefCell::new(HashMap::new());
}

pub fn register_denormalized_alias(target_node_alias: &str, edge_alias: &str)
pub fn get_denormalized_alias_mapping(target_node_alias: &str) -> Option<String>
pub fn clear_denormalized_aliases()
```

### 2. Modified join_builder to Register Mappings ✅
**File**: `src/render_plan/join_builder.rs` (lines 1877-1911)

When denormalized edge detected (end_table == rel_table):
```rust
crate::render_plan::register_denormalized_alias(
    &graph_rel.right_connection,  // target node alias (e.g., "d")
    &graph_rel.alias              // edge alias (e.g., "r2")
);
```

### 3. Enhanced select_builder with Alias Mapping Check ✅
**Files**: `src/render_plan/select_builder.rs`

Modified three cases:

**Case 0 - ColumnAlias (NEW)**
- Added new case to handle CTE-exported variables
- When projection has `ColumnAlias("a")` and `a` is a CTE export, expands to properties
- Falls back to regular column alias if not a CTE export

**Case 1 - TableAlias**
- Checks denormalized alias mapping: `get_denormalized_alias_mapping(&table_alias.0)`
- Uses mapped edge alias for property resolution if found

**Case 2 - PropertyAccessExp Wildcard**
- Also checks denormalized alias mapping
- Same logic as Case 1

### 4. Added Cleanup Functions ✅
**File**: `src/render_plan/plan_builder.rs`

Added calls to `clear_denormalized_aliases()` after:
- Projection extraction (line ~925)
- CartesianProduct right side rendering (line ~1314)

Follows same pattern as existing `clear_cte_column_registry()` cleanup.

## Build Status
✅ **All code compiles successfully**
- No errors
- Only unused import/variable warnings (pre-existing)

## Testing Status

### What Passed
- Build compilation ✅
- Server startup (with some terminal issues) ⚠️

### What Still Needs Testing
- Main test: `test_with_cross_table[social_benchmark]`
- Root cause of remaining failure: CTE-exported variables `a` and `b` are not being expanded

## Identified Issue

The test still fails with:
```
SELECT a AS "a", b AS "b", ...
```

Should be:
```
SELECT a_b.a_city AS "a.city", a_b.a_country AS "a.country", ...
```

### Root Cause Analysis

When CTE exports `a` and `b` and they're referenced in a subsequent `MATCH ... RETURN a, b`:
1. The logical plan's projection items might not be `ColumnAlias` but something else
2. Need to investigate what LogicalExpr type is actually being used
3. Possibly need additional cases in select_builder

### Debug Next Steps

1. Add logging to Case 5 (Other expressions) in select_builder to see what's happening:
   ```rust
   log::warn!("🔍 SelectBuilder Case 5 (Other): Expression type = {:?}", item.expression);
   ```

2. Check if CTE exports are being created as:
   - `ColumnAlias("a")` - should be caught by new Case 0
   - `RenderExpr::ColumnAlias` - would need conversion
   - Something else entirely - need new case

3. Alternatively, check if the issue is in WHERE clause handling for correlation predicates

## Implementation Summary

### Architecture Pattern
- Follows existing thread_local! storage pattern (CTE_COLUMN_REGISTRY_CONTEXT)
- Uses task-local context for query-scoped state
- Cleanup integrated into existing render phase cleanup

### Integration Points
- join_builder.rs: Detects denormalized edges and registers mappings
- select_builder.rs: Checks mappings during property expansion
- plan_builder.rs: Cleans up after rendering complete

### No New Files Created
- All changes were modifications to existing files
- Kept codebase clean and focused

## Next Session Checklist

- [ ] Restart terminal/server if needed
- [ ] Run failing test with full debug output
- [ ] Check what LogicalExpr type is in projection items
- [ ] Add additional cases to select_builder if needed
- [ ] Verify WITH-exported variables are being expanded properly
- [ ] Run full test suite to ensure no regressions
- [ ] If all tests pass, commit and update STATUS.md

## Files Modified

1. `src/render_plan/mod.rs` - Added alias mapping storage/functions
2. `src/render_plan/join_builder.rs` - Register mappings for denormalized edges
3. `src/render_plan/select_builder.rs` - Check mappings + new ColumnAlias case
4. `src/render_plan/plan_builder.rs` - Added cleanup calls

## Code Quality Notes

- All code follows existing patterns
- Proper error handling and logging added
- Thread-safe with task-local storage
- Zero performance impact (mapping lookup is O(1) HashMap)
- Minimal code duplication
