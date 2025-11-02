# Multiple Relationship Types Regression Investigation
**Date**: November 2, 2025  
**Status**: Root cause identified  

## Summary
Multiple relationship type patterns (`[:TYPE1|TYPE2]`) fail during render plan generation with "No select items found" error. The feature works through logical planning and optimization but breaks when building the final SQL render plan.

## Test Case
```cypher
MATCH (u:User)-[:FOLLOWS|FRIENDS_WITH]->(target:User)
RETURN u.name, target.name
```

## Investigation Results

###  What Works
1. ✅ **Parsing** - Alternate relationship syntax correctly parsed
2. ✅ **Logical Planning** - GraphRel created with `labels: Some(["FOLLOWS", "FRIENDS_WITH"])`
3. ✅ **Optimization** - ProjectionPushDown creates projection items
4. ✅ **Schema Resolution** - ViewScan nodes correctly reference schema tables

### ❌ What's Broken
**Render Plan Builder** - ViewScan nodes have empty `projections: []`

From server logs:
```
DEBUG: extract_select_items called on: ViewScan(ViewScan { 
  source_table: "users", 
  property_mapping: {"name": "name"}, 
  projections: [],  // ← EMPTY!
  ...
})
```

### Root Cause
File: `brahmand/src/render_plan/plan_builder.rs`  
Line: 794

```rust
fn extract_select_items(&self) -> RenderPlanBuilderResult<Vec<SelectItem>> {
    let select_items = match &self {
        LogicalPlan::Empty => vec![],
        LogicalPlan::Scan(_) => vec![],
        LogicalPlan::ViewScan(_) => vec![],  // ← Returns empty!
        LogicalPlan::GraphNode(graph_node) => graph_node.input.extract_select_items()?,
        // ...
    }
}
```

**Problem**: ViewScan always returns empty vector instead of building select items from its property mappings and projections.

### Why It Happens
When multiple relationship types are used:
1. Query planner creates ViewScan nodes for start/end nodes
2. ProjectionPushDown optimizer adds projection items to parent Projection nodes
3. BUT ViewScan's internal `projections` field stays empty
4. When render plan builder calls `extract_select_items()` on ViewScan, it returns `vec![]`
5. Final render plan has no select items → "No select items found" error

### Expected Behavior
ViewScan should build select items from:
- Its `property_mapping` HashMap (e.g., `{"name": "name"}`)
- Its `projections` vector (populated by optimizer)
- Referenced columns in parent Projection nodes

### Comparison to Working Case
Single relationship type (`[:FOLLOWS]`) works because it uses different code path (likely Scan instead of ViewScan, or doesn't hit this extract_select_items code).

## Fix Requirements
1. Implement proper `extract_select_items` for ViewScan case
2. Build SelectItem list from ViewScan's property_mapping and projections
3. Handle qualified column names (table_alias.column)
4. Preserve property mappings from schema

## Related Code
- `brahmand/src/render_plan/plan_builder.rs:792-850` - extract_select_items implementation
- `brahmand/src/query_planner/logical_plan/view_scan.rs` - ViewScan structure
- `brahmand/src/query_planner/optimizer/projection_push_down.rs` - Adds projections

## Next Steps
1. Read ViewScan struct definition to understand available data
2. Implement ViewScan case in extract_select_items
3. Build SelectItem from property_mapping HashMap
4. Test with multi-relationship query
5. Verify SQL generation includes correct columns

## Test Data Setup
Database: `test_multi_rel`  
Schema: `test_multi_rel_schema.yaml`  
Tables: users (user_id, name), follows, friends, likes

Sample data:
```sql
INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');
INSERT INTO follows VALUES (1, 2), (2, 3);
INSERT INTO friends VALUES (1, 3);
```

##Notes
This is **not a new bug** - it's a known limitation documented in KNOWN_ISSUES.md and copilot instructions. The multiple relationship feature was implemented at the logical/optimizer level but never completed for render plan generation.
