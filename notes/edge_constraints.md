# Edge Constraints

**Status**: ✅ Implemented (v0.6.1)  
**Date**: December 24, 2025  
**Author**: Gene

## Overview

Edge constraints allow defining logical validation rules between source and target nodes in a relationship. Unlike standard graph traversals which only match on IDs (`from_id = id` AND `to_id = id`), edge constraints enable rules like "event A must happen before event B" or "source level must be greater than target level".

These constraints are defined once in the schema and automatically applied to all queries using that relationship, ensuring data integrity and correct semantics without requiring users to manually add `WHERE` clauses to every query.

## Schema Configuration

Constraints are defined in the `edges` section of the schema YAML:

```yaml
edges:
  - type_name: COPIED_BY
    database: lineage
    table: file_lineage
    from_node: DataFile
    to_node: DataFile
    from_id: source_file_id
    to_id: target_file_id
    # The constraint expression
    constraints: "from.timestamp <= to.timestamp"
```

### Syntax
- **Prefixes**: `from.` references the source node, `to.` references the target node
- **Properties**: Must match property names defined in the node's `property_mappings`
- **Operators**: Standard comparison (`<`, `<=`, `>`, `>=`, `=`, `!=`) and logical (`AND`, `OR`) operators

## Implementation Details

### 1. Constraint Compiler (`constraint_compiler.rs`)

The compiler translates the schema expression into SQL by:
1. Parsing the expression string
2. Resolving `from.prop` and `to.prop` to physical column names using node schemas
3. Replacing aliases with the actual SQL table aliases used in the query

**Example**:
- Schema: `from.timestamp <= to.timestamp`
- Node Schema: `timestamp` maps to `created_timestamp`
- Query Aliases: `f` (source), `t` (target)
- Result: `f.created_timestamp <= t.created_timestamp`

### 2. Single-Hop Integration (`plan_builder.rs`)

For simple patterns like `MATCH (a)-[:REL]->(b)`:
- The constraint is compiled using the aliases for `a` and `b`
- It is added to the `ON` clause of the second node's JOIN
- This ensures the constraint is applied as part of the relationship traversal

```sql
SELECT ...
FROM nodes AS a
INNER JOIN edges AS r ON r.from_id = a.id
INNER JOIN nodes AS b ON b.id = r.to_id AND a.timestamp <= b.timestamp  -- Constraint applied here
```

### 3. Variable-Length Path Integration (`variable_length_cte.rs`)

For recursive patterns like `MATCH (a)-[:REL*]->(b)`:
- The constraint is injected into the Recursive CTE generation
- **Base Case**: Applied to the initial seed query
- **Recursive Step**: Applied to the join between the path so far and the next node

```sql
WITH RECURSIVE cte AS (
    -- Base Case
    SELECT ... FROM nodes a JOIN edges r ... JOIN nodes b ...
    WHERE a.timestamp <= b.timestamp
    
    UNION ALL
    
    -- Recursive Step
    SELECT ... FROM cte JOIN edges r ... JOIN nodes b ...
    WHERE cte.last_timestamp <= b.timestamp
)
```

## Limitations

1. **Directional Only**: Constraints apply to the specific direction defined in the schema. Undirected queries or reverse traversals may not apply constraints correctly (future work).
2. **Single Relationship Type**: Currently only supports constraints on single-type relationships. `[:A|B]` patterns with conflicting constraints are not supported.
3. **Simple Expressions**: Supports basic comparisons. Complex expressions involving subqueries or aggregations are not supported.

## Testing

Integration tests in `tests/integration/test_edge_constraints.py` verify:
- Correct SQL generation for single-hop queries
- Correct SQL generation for VLP queries
- Actual data filtering (using ClickHouse)
- Queries without constraints remain unaffected
