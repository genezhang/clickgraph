# Empty → ViewScan Resolution Implementation

## Summary

Successfully implemented Empty placeholder resolution for anonymous nodes and relationships during schema inference. Anonymous patterns in Cypher queries (like `()` or `-[]-`) now correctly resolve to ViewScan nodes that can participate in JOINs.

## Problem

Previously, anonymous graph patterns were represented as `Empty` placeholders during logical planning but were never resolved to actual ViewScan nodes with table information. This meant:

1. **Anonymous nodes**: `MATCH ()-[:REL]->(b)` created `GraphNode { input: Empty }` for the `()` node
2. **Anonymous relationships**: `MATCH (a)-[]-(b)` created `GraphRel { center: Empty }` for the `[]` edge  
3. **Never resolved**: The Empty placeholders stayed Empty through all analyzer passes
4. **Couldn't JOIN**: Empty nodes had no table information, preventing JOIN generation

## Solution Architecture

### Design Decision

- **Empty is the correct placeholder**: Using `LogicalPlan::Empty` for anonymous patterns is good design
- **GraphNode/GraphRel wrappers provide context**: These wrappers disambiguate whether Empty represents a node or edge
- **Resolution in schema_inference.rs**: After label/type inference, Empty is replaced with proper ViewScan

### Implementation

**Files Modified:**

1. **src/query_planner/logical_plan/match_clause.rs**:
   - Made `try_generate_view_scan()` public (line ~390)
   - Made `try_generate_relationship_view_scan()` public (line ~791)
   - These functions create ViewScan from label/type information

2. **src/query_planner/logical_plan/mod.rs**:
   - Made `match_clause` module public for cross-module access

3. **src/query_planner/analyzer/schema_inference.rs**:
   - Modified `push_inferred_table_names_to_scan()` to detect and resolve Empty
   - **GraphNode resolution** (lines ~42-90):
     - Detects `GraphNode { input: Empty }`
     - Gets inferred label from TableCtx
     - Calls `try_generate_view_scan()` to create ViewScan
     - Rebuilds GraphNode with ViewScan input
   - **GraphRel resolution** (lines ~96-160):
     - Detects `GraphRel { center: Empty }`
     - Gets inferred relationship type from TableCtx
     - Gets left/right node labels for context
     - Calls `try_generate_relationship_view_scan()` to create ViewScan
     - Only resolves single-type relationships (multiple types use UNION)

### Resolution Flow

```
1. PARSING (open_cypher_parser):
   NodePattern { alias: "anon", label: None } → AST only, no schema knowledge

2. LOGICAL PLANNING (match_clause.rs):
   generate_scan() → creates Empty for anonymous nodes
   ├─ Labeled nodes (n:User): ViewScan created immediately
   └─ Anonymous nodes (): Empty placeholder with comment "will be inferred"

3. SCHEMA INFERENCE (schema_inference.rs):
   infer_schema() → computes missing labels from relationship constraints
   ├─ Stores inferred labels in TableCtx
   └─ push_inferred_table_names_to_scan() → walks tree

4. RESOLUTION (schema_inference.rs):
   Empty detection → if GraphNode/GraphRel has Empty input/center:
   ├─ Get inferred label from TableCtx
   ├─ Call try_generate_view_scan() to create ViewScan
   └─ Rebuild GraphNode/GraphRel with ViewScan

5. JOIN GENERATION (graph_join_inference.rs):
   ViewScan has table_name → JOINs can be generated correctly
```

## Verification

### Server Log Evidence

Query: `MATCH ()-[:FOLLOWS]->(b:User) RETURN b.name LIMIT 3`

```
[INFO clickgraph::query_planner::analyzer::schema_inference] 
SchemaInference: Resolving Empty → ViewScan for node 't1' with inferred label 'User'

[INFO clickgraph::query_planner::logical_plan::match_clause] 
✓ ViewScan: Resolved label 'User' to table 'users_bench'
```

### Generated SQL

```sql
SELECT b.full_name AS "b.name"
FROM brahmand.users_bench AS t1  -- Anonymous node resolved!
INNER JOIN brahmand.user_follows_bench AS t2 ON t2.follower_id = t1.user_id
INNER JOIN brahmand.users_bench AS b ON b.user_id = t2.followed_id
LIMIT 3
```

### Test Results

- **Before**: 604/662 tests passing (91.3%)
- **After**: 604/662 tests passing (91.3%) ✅ No regressions
- **Anonymous queries**: Working correctly (verified via server execution)

## Edge Cases Handled

1. **Single relationship type**: Resolves to single ViewScan
2. **Multiple relationship types**: Keeps Empty, handled by UNION generation in later passes
3. **No inferred label**: Skips resolution, logs warning
4. **Already ViewScan**: No transformation needed

## Code Quality

- Added detailed logging: `SchemaInference: Resolving Empty → ViewScan for node 'X' with inferred label 'Y'`
- Follows existing patterns: Uses `rebuild_or_clone()` and `Transformed` enum
- Preserves all node properties: `is_denormalized`, `projected_columns`, etc.
- Handles both directions: Outgoing, Incoming, Both

## Benefits

1. **Complete anonymous pattern support**: `()` nodes and `-[]-` relationships work correctly
2. **JOIN generation works**: ViewScan provides table information for JOIN predicates
3. **Clean architecture**: Empty → ViewScan resolution happens in correct phase
4. **Schema inference**: Leverages existing label inference infrastructure
5. **No duplicates**: Single source of ViewScan creation logic (reused from match_clause)

## Future Work

- Consider early resolution: Could resolve during planning if relationship constraints are available
- Performance optimization: Cache label inferences to avoid recomputation
- Enhanced logging: Add more details about resolution decisions

## Related Issues

- Fixes: Anonymous node patterns couldn't generate JOINs
- Completes: Scan elimination (Empty is proper placeholder, not legacy Scan)
- Enables: Full Cypher pattern support without requiring explicit labels

## References

- **Design discussion**: November 22-23, 2025 session
- **Implementation**: PR #XXX (Scan Killer 💀 + Empty Resolution)
- **Testing**: Manual verification via server logs and SQL inspection
- **Documentation**: This note, STATUS.md updates

---

**Implementation Date**: December 23, 2025  
**Status**: ✅ Complete and working  
**Test Coverage**: Verified via manual testing and full test suite  
**Breaking Changes**: None
