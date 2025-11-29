# Coupled Edges - Implementation Notes

*Created: November 28, 2025*

## Summary

**Coupled edges** are multiple relationships defined on the same physical table that share a "coupling node" in a sequential pattern. ClickGraph automatically detects these and optimizes queries by eliminating self-joins.

## How It Works

### Detection Logic

In `AliasResolverContext::detect_coupled_edges()`:

1. **Collect edge tables**: Scan the query plan for all edge/relationship patterns
2. **Group by table**: Edges on same `database.table` are candidates
3. **Check coupling**: Verify edges connect through shared node (e.g., `r1.to_node = r2.from_node`)
4. **Unify aliases**: Map all coupled edge aliases to the first edge's alias

### Key Files

- `src/render_plan/alias_resolver.rs` - Coupled edge detection and alias unification
- `src/query_planner/analyzer/graph_join_inference.rs` - JOIN skip logic
- `src/query_planner/analyzer/projection_tagging.rs` - UNWIND property mapping

### Alias Unification

```rust
// In AliasResolverContext
pub coupled_edge_aliases: HashMap<String, String>  // e.g., {"r2" -> "r1", "r3" -> "r1"}
```

When resolving aliases:
1. Check if alias is in `coupled_edge_aliases`
2. If yes, use the unified alias instead
3. All SELECT, FROM, WHERE clauses use consistent alias

## Design Decisions

### Why "Coupled" Instead of "Co-located"?

- "Co-located" implies physical proximity (same server/shard)
- "Coupled" emphasizes the logical relationship through the coupling node
- Clearer terminology for users

### Properties on Nodes, Not Edges

For denormalized tables, `from_node_properties` and `to_node_properties` are defined on the **NodeDefinition**, not the edge:

```yaml
nodes:
  - label: IP
    from_node_properties:
      ip: "id.orig_h"
    to_node_properties:
      ip: "id.resp_h"
```

The edge processing looks up these properties from the node definitions based on `from_node` and `to_node` labels.

### UNWIND Property Resolution

For patterns like:
```cypher
UNWIND rip.ips AS resolved_ip
```

The `transform_unwind_expression()` function:
1. Finds the node label for alias `rip`
2. Looks up `to_node_properties` (since `rip` is a target node)
3. Maps `ips` → `answers` (the SQL column name)

## Limitations

1. **Linear chains only**: Branching patterns like `(a)-[r1]->(b), (a)-[r2]->(c)` are not coupled
2. **Same table required**: Both edges must be on exact same `database.table`
3. **Sequential coupling**: `r1.to_node` must equal `r2.from_node`

## Testing

All 10 test patterns verified:
- Basic 2-hop coupled edges
- Single edge (no coupling)
- WHERE filters
- COUNT/aggregations
- ORDER BY
- DISTINCT
- Edge property access
- Middle node only
- Mixed patterns
- UNWIND with arrays

## Future Work

- Support for branching coupled patterns
- Coupled edge detection for variable-length paths
- Performance metrics/logging for optimization hits
