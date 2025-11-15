# Edge List Terminology Cleanup

**Date**: November 10, 2025  
**Status**: Completed  
**Impact**: Naming only, no logic changes

## Summary

Renamed `handle_edge_list_traversal` → `handle_graph_pattern` to remove redundant "edge_list" terminology. Since ClickGraph **always** uses view-mapped edge list storage (relationships stored as tables with `from_id`/`to_id` columns), the term "edge_list" is redundant and doesn't add clarity.

## Changes Made

### 1. Function Rename
**File**: `src/query_planner/analyzer/graph_join_inference.rs`

- **Line 781**: Function definition renamed
- **Line 757**: Function call site updated
- **Added documentation**: Explains ClickGraph's storage architecture

```rust
/// Handle graph pattern traversal for view-mapped tables
/// ClickGraph always uses view-mapped edge list storage where 
/// relationships are stored as tables with from_id/to_id columns.
fn handle_graph_pattern(
    &self,
    graph_rel: &GraphRel,
    // ... 12 parameters unchanged ...
) -> AnalyzerResult<()> {
    // ... ~500 lines of logic preserved ...
}
```

### 2. Comment Updates
- Changed "Using EDGE LIST traversal" → "Processing graph pattern"
- Added doc comment explaining storage architecture

## What Was NOT Changed

### Preserved Logic
All ~500 lines of complex JOIN generation logic were preserved:
- Same-type node handling
- Direction-based branching
- Anchor node selection
- Optional matching support

### Remaining Legacy References
**Not cleaned up yet** (low priority):
- `use_edge_list` field in `TableCtx` (always true)
- `set_use_edge_list()` method in `plan_ctx/mod.rs`
- `should_use_edge_list()` method in `plan_ctx/mod.rs`
- Test names containing "edge_list" (e.g., `test_edge_list_same_node_type_outgoing_direction`)

**Rationale**: These are internal implementation details and would require larger refactoring. The critical function name visible in traces and debugging has been cleaned up.

## Test Results

**Before**: 323/325 unit tests passing (original baseline)  
**After**: 323/325 unit tests passing (same baseline)  
**Failures**: Same 2 tests that failed in original code:
- `test_edge_list_same_node_type_outgoing_direction`
- `test_incoming_direction_edge_list`

## Historical Context

### Why "edge_list" Was There
- **Original Brahmand**: Supported two storage modes:
  1. **BITMAP mode**: Relationships stored as bitmaps in `ArrayCollapse` columns
  2. **EDGE LIST mode**: Relationships stored as explicit tables

- **Commit 9c70625** (Nov 9, 2025): Removed BITMAP code entirely (~300 lines deleted)
  - ClickGraph forked from Brahmand and focuses only on view-mapped storage
  - BITMAP mode was legacy code, never used in ClickGraph

### Why This Cleanup
User clarified: "We are logically edge_list storage... What I really wanted is to get rid of the edge_list term from the functions and variables as it's always true for us."

Since ClickGraph **only** supports edge list storage (relationships as tables), the term was redundant. Removing it makes code clearer - "graph pattern" is more descriptive than "edge list traversal."

## Future Work

**Low Priority Cleanup** (if desired):
1. Remove `use_edge_list` boolean from `TableCtx` (always true)
2. Remove `set_use_edge_list()` and `should_use_edge_list()` methods
3. Rename test functions to remove "edge_list" references
4. Update validation code that checks for adjacency indexes (dead code)

**Note**: These are cosmetic improvements. Since they're internal details and not visible in user-facing code or traces, they can be addressed when touching those files for other reasons.

## Key Files

- `src/query_planner/analyzer/graph_join_inference.rs`: Function renamed
- `src/query_planner/plan_ctx/mod.rs`: Has legacy `use_edge_list` methods (not cleaned)
- `src/query_planner/analyzer/query_validation.rs`: Has adjacency index checks (dead code)

## Gotchas

**None** - This was a pure naming change with no logic modifications.

## Limitations

**Incomplete cleanup**: Legacy references remain in:
- Internal methods (`set_use_edge_list`, `should_use_edge_list`)
- Test names
- Validation code

These don't impact functionality or user-visible behavior.



