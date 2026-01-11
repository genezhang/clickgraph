# OPTIONAL MATCH Correlation Fix - January 10, 2026

## Problem Statement

When using OPTIONAL MATCH with bidirectional relationships where one endpoint is already bound from a previous MATCH, the generated SQL was missing the correlation condition to the already-bound node.

**Example Query:**
```cypher
MATCH (m:Message)<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(p:Person)
OPTIONAL MATCH (m)-[:HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p)
RETURN c.id, p.id, a.id
```

In this query, `p` is bound in the first MATCH and referenced in the OPTIONAL MATCH. The KNOWS relationship should connect `a` (newly introduced) to `p` (already bound).

## Root Cause

In `src/query_planner/analyzer/graph_join_inference.rs`, the `handle_graph_pattern_v2` function was creating JOINs for graph patterns. When processing a relationship like `(a)-[r:KNOWS]-(p)`:

1. It would create edge table JOIN: `r.Person1Id = a.id`
2. If `p` was already in `joined_entities`, it would skip creating a JOIN for `p`
3. **BUG**: It never added the correlation condition `r.Person2Id = p.id` to the edge JOIN

This resulted in incomplete JOIN conditions where the relationship was only connected to one endpoint.

## Solution

Added logic to detect when a node is already joined and append a correlation condition to the edge JOIN:

**File**: `src/query_planner/analyzer/graph_join_inference.rs`

**Location 1** (~line 3203): When `connect_left_first = true` and right node is already joined:
```rust
} else {
    // CRITICAL FIX: Right node is already joined - add correlation condition to edge JOIN
    log::debug!("🔗 RIGHT node '{}' already joined - adding correlation to edge JOIN", right_alias);
    
    let resolved_right_id = Self::resolve_column(&right_id_col, right_cte_name, plan_ctx);
    let resolved_right_join_col = Self::resolve_column(right_join_col, rel_cte_name, plan_ctx);
    
    // Find the edge JOIN we just added and append the correlation condition
    if let Some(edge_join) = collected_graph_joins.iter_mut()
        .rev()  // Search from end (most recently added)
        .find(|j| j.table_alias == rel_alias)
    {
        let correlation_condition = OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(rel_alias.to_string()),
                    column: PropertyValue::Column(resolved_right_join_col),
                }),
                LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(right_alias.to_string()),
                    column: PropertyValue::Column(resolved_right_id),
                }),
            ],
        };
        edge_join.joining_on.push(correlation_condition);
        log::debug!("✓ Added correlation condition to edge JOIN '{}'", rel_alias);
    }
}
```

**Location 2** (~line 3305): Symmetric fix for when `connect_left_first = false` and left node is already joined.

## Generated SQL

**Before Fix:**
```sql
LEFT JOIN ldbc.Person_knows_Person AS r ON r.Person1Id = a.id
-- Missing: AND r.Person2Id = p.id
```

**After Fix:**
```sql
LEFT JOIN ldbc.Person_knows_Person AS r ON r.Person1Id = a.id AND r.Person2Id = p.id
```

For bidirectional relationships (`-[r:KNOWS]-`), both UNION branches are correctly generated:
- **Branch 1**: `r.Person1Id = a.id AND r.Person2Id = p.id`
- **Branch 2**: `r.Person1Id = p.id AND r.Person2Id = a.id` (direction swapped)

## Test Results

**Working Queries:**
```cypher
-- Simple OPTIONAL MATCH with bidirectional
MATCH (m:Message {id: 1})
OPTIONAL MATCH (m)-[:HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p:Person)
RETURN m.id, a.id, p.id
-- ✅ Works - LEFT JOINs with correct conditions

-- Correlated OPTIONAL MATCH (IS7 pattern)
MATCH (m:Message {id: 1})<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(p:Person)
OPTIONAL MATCH (m)-[:HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p)
RETURN c.id, p.id, a.id, CASE WHEN r IS NULL THEN false ELSE true END AS knows
-- ✅ Works - Proper correlation with already-bound p
```

## Remaining Limitations

The LDBC IS7 benchmark query still fails due to **parser limitations**, not the correlation logic:

1. **Multiple MATCH clauses**: ClickGraph doesn't support consecutive MATCH without WITH
   ```cypher
   MATCH (m:Message)
   WHERE m.id = $messageId
   MATCH (m)<-[:REPLY_OF]-(c)  -- ❌ Parser error: second MATCH not supported
   ```

2. **Parameter syntax**: `$messageId` parameter substitution needs improvement

These are separate issues from the correlation fix implemented here.

## Impact

- **Fixed**: OPTIONAL MATCH with bidirectional relationships and correlated nodes
- **Performance**: No performance impact - only adds necessary correlation conditions
- **Correctness**: Ensures OPTIONAL MATCH correctly filters relationships that connect to already-bound nodes

## Testing

To test the fix:
```bash
# Start server
./target/release/clickgraph --http-port 8080

# Test correlated OPTIONAL MATCH
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (m:Message {id: 1})<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(p:Person) OPTIONAL MATCH (m)-[:HAS_CREATOR]->(a:Person)-[r:KNOWS]-(p) RETURN c.id, p.id, a.id LIMIT 5"
  }'
```

## Related Files

- `src/query_planner/analyzer/graph_join_inference.rs` - Main fix location
- `src/query_planner/analyzer/bidirectional_union.rs` - Handles bidirectional pattern expansion
- `src/query_planner/logical_plan/optional_match_clause.rs` - OPTIONAL MATCH processing

## Future Work

1. Support multiple consecutive MATCH clauses (parser enhancement)
2. Improve parameter substitution for `$param` syntax
3. Add integration tests specifically for correlated OPTIONAL MATCH patterns
