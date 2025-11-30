# type(r) Behavior Across Schema Variations

## Summary

The `type(r)` function in Cypher returns the relationship type of relationship variable `r`. ClickGraph supports two schema patterns:

1. **Standard Schema**: Separate tables per edge type
2. **Polymorphic Schema**: Single table with `type_column` discriminator

## Behavior Matrix

| Pattern | Schema Type | type(r) Returns | Status |
|---------|-------------|-----------------|--------|
| Single explicit type `[:FOLLOWS]` | Standard | `'FOLLOWS'` (literal) | ✅ Works |
| Single explicit type `[:FOLLOWS]` | Polymorphic | `r.interaction_type` (column) | ✅ Works |
| Multi-type `[:FOLLOWS\|LIKES]` | Standard | Literal per UNION branch | ✅ Works |
| Multi-type `[:FOLLOWS\|LIKES]` | Polymorphic | `r.interaction_type` (column) | ⚠️ Partial |
| Bidirectional `(a)-[r:FOLLOWS]-(b)` | Standard | `'FOLLOWS'` in each branch | ✅ Fixed (85279e1) |
| Wildcard `[r]` | Standard | Literal per UNION branch | ✅ Works |
| Wildcard `[r]` | Polymorphic | Should be `r.type_column` | ❌ TODO |

## How It Works

### Standard Schema (Separate Tables)

Schema example:
```yaml
edges:
  - type: FOLLOWS
    table: user_follows
    from_id: follower_id
    to_id: followed_id
```

`type(r)` is converted to a string literal at planning time:
```rust
// In projection_tagging.rs
if let Some(type_col) = &rel_schema.type_column {
    // Polymorphic: use actual column
    PropertyAccessExp(r.type_column)
} else {
    // Standard: use literal string
    Literal::String(rel_type.clone())
}
```

Generated SQL:
```sql
SELECT 'FOLLOWS' AS rel_type FROM ...
```

### Polymorphic Schema (Single Table)

Schema example:
```yaml
edges:
  - polymorphic: true
    table: interactions
    type_column: interaction_type
    from_id: from_id
    to_id: to_id
    type_values:
      - FOLLOWS
      - LIKES
```

`type(r)` is converted to a column reference:
```sql
SELECT r.interaction_type FROM ...
```

## Key Code Locations

- **type(r) Resolution**: `src/query_planner/analyzer/projection_tagging.rs` lines 452-481
- **Polymorphic CTE Generation**: `src/render_plan/cte_extraction.rs` lines 882-912
- **Bidirectional Union Fix**: `src/query_planner/analyzer/bidirectional_union.rs` lines 153-195

## Known Issues

### 1. Polymorphic Multi-Type JOIN Filter Bug

**Issue**: When using `[:TYPE1|TYPE2]` with polymorphic schema, the JOIN incorrectly filters to only the first type.

**Example**:
```cypher
MATCH (a:User)-[r:FOLLOWS|LIKES]->(b:User) RETURN type(r)
```

**Generated SQL** (buggy):
```sql
-- CTE is correct
WITH rel_a_b AS (
  SELECT ... FROM interactions WHERE interaction_type IN ('FOLLOWS', 'LIKES')
)
-- But JOIN only uses first type
INNER JOIN interactions AS r ... AND r.interaction_type = 'FOLLOWS'  -- BUG!
```

**Expected**: JOIN should use `IN ('FOLLOWS', 'LIKES')` or omit filter (since CTE already filters).

### 2. Wildcard with Polymorphic and No Target Label

**Issue**: `MATCH (a)-[r]->(b) RETURN b.name` fails when target node has no label.

**Error**: `Property 'name' not found on node 'b'`

**Workaround**: Always specify target label for now: `MATCH (a)-[r]->(b:User)`

## Test Commands

```bash
# Register polymorphic schema
cat ./schemas/examples/social_polymorphic.yaml | \
  jq -Rs '{schema_name: "polymorphic", config_content: .}' | \
  curl -s -X POST http://localhost:8080/schemas/load -H "Content-Type: application/json" -d @-

# Test polymorphic type(r)
curl -s -X POST http://localhost:8080/query/sql \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN type(r), b.name", "schema_name":"polymorphic"}'
```

## Related Documents

- `notes/polymorphic-edge-query-optimization.md` - Design for unified polymorphic edge handling
- Commit `85279e1` - Bidirectional type(r) fix
