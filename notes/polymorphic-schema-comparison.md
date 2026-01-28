# Polymorphic Schema Configuration: Before vs After

## The Problem
User feedback: "The current proposal is already a bit complicated... they have to maintain it once new types are added..."

## Configuration Complexity Comparison

### ❌ BEFORE: Explicit Type Configuration (6 fields per relationship)

```yaml
relationships:
  - name: FOLLOWS
    table: relationships
    type_column: relation_type
    type_value: "FOLLOWS"
    from_type_column: from_type   # ❌ Extra config
    from_type_value: "User"       # ❌ Extra config
    to_type_column: to_type       # ❌ Extra config
    to_type_value: "User"         # ❌ Extra config
    
  - name: LIKES
    table: relationships
    type_column: relation_type
    type_value: "LIKES"
    from_type_column: from_type   # ❌ Extra config
    from_type_value: "User"       # ❌ Extra config
    to_type_column: to_type       # ❌ Extra config
    to_type_value: "Post"         # ❌ Extra config
    
  - name: AUTHORED
    table: relationships
    type_column: relation_type
    type_value: "AUTHORED"
    from_type_column: from_type   # ❌ Extra config
    from_type_value: "User"       # ❌ Extra config
    to_type_column: to_type       # ❌ Extra config
    to_type_value: "Post"         # ❌ Extra config
```

**Pain Points**:
- 6 fields per relationship type
- Must remember to update 4 extra fields when adding new relationship
- Easy to make mistakes (typos, wrong types)
- Verbose and repetitive

### ✅ AFTER: Automatic Inference (2 fields per relationship)

```yaml
relationships:
  - name: FOLLOWS
    table: relationships
    type_column: relation_type
    type_value: "FOLLOWS"
    # ✨ from_type/to_type inferred from query!
    
  - name: LIKES
    table: relationships
    type_column: relation_type
    type_value: "LIKES"
    # ✨ Inferred from (u:User)-[:LIKES]->(p:Post)
    
  - name: AUTHORED
    table: relationships
    type_column: relation_type
    type_value: "AUTHORED"
    # ✨ Inferred from query labels
```

**Benefits**:
- ✅ 2 fields per relationship (67% reduction!)
- ✅ Add new relationship in 3 lines
- ✅ No duplication between config and queries
- ✅ Less error-prone
- ✅ Easier to maintain

## How Inference Works

### Query Example
```cypher
MATCH (u:User)-[:LIKES]->(p:Post)
WHERE u.user_id = 1
RETURN p.name
```

### Automatic Extraction
Query planner extracts:
1. **Source label**: `User` (from `u:User`)
2. **Target label**: `Post` (from `p:Post`)
3. **Relationship type**: `LIKES` (from `[:LIKES]`)

### Generated SQL
```sql
SELECT p.name
FROM entities AS u
WHERE u.node_type = 'User' AND u.user_id = 1
INNER JOIN relationships AS r
  ON r.from_id = u.id
  AND r.relation_type = 'LIKES'
  AND r.from_type = 'User'    -- ✨ Inferred from u:User
  AND r.to_type = 'Post'      -- ✨ Inferred from p:Post
INNER JOIN entities AS p
  ON p.id = r.to_id
  AND p.node_type = 'Post'
```

## Implementation Impact

### Database Schema (Unchanged)
```sql
-- Still stores type information for filtering
CREATE TABLE relationships (
    from_id UInt64,
    to_id UInt64,
    from_type LowCardinality(String),    -- Still needed in DB
    to_type LowCardinality(String),      -- Still needed in DB
    relation_type LowCardinality(String),
    properties String
) ENGINE = MergeTree()
ORDER BY (relation_type, from_type, to_type, from_id);
```

**Key Insight**: Database still has `from_type`/`to_type` columns for efficient filtering. We just don't need to configure them in YAML - they're inferred from queries!

### Code Changes Required

**RelationshipSchema struct** (graph_catalog/config.rs):
```rust
pub struct RelationshipSchema {
    pub name: String,
    pub table: String,
    pub type_column: Option<String>,    // relation_type
    pub type_value: Option<String>,     // 'FOLLOWS'
    // ✨ NO from_type/to_type config fields!
    pub properties: HashMap<String, String>,
}
```

**Match clause analyzer** (query_planner/analyzer/match_clause.rs):
```rust
// Extract labels from pattern
fn analyze_relationship_pattern(pattern: &RelPattern) -> Result<TypeInfo> {
    let from_label = pattern.source_node.labels.first(); // User
    let to_label = pattern.target_node.labels.first();   // Post
    
    Ok(TypeInfo { from_label, to_label })
}
```

**SQL generation** (clickhouse_query_generator/graph_rel.rs):
```rust
// Include inferred types in JOIN
fn build_join_condition(rel_schema: &RelSchema, type_info: &TypeInfo) -> String {
    let mut conditions = vec![
        format!("{}.relation_type = '{}'", rel_alias, rel_schema.type_value),
    ];
    
    if let Some(from) = type_info.from_label {
        conditions.push(format!("{}.from_type = '{}'", rel_alias, from));
    }
    
    if let Some(to) = type_info.to_label {
        conditions.push(format!("{}.to_type = '{}'", rel_alias, to));
    }
    
    conditions.join(" AND ")
}
```

## Metrics

| Aspect | Before | After | Improvement |
|--------|--------|-------|-------------|
| Fields per relationship | 6 | 2 | **67% reduction** |
| Lines to add new relationship | 7-8 | 3-4 | **50% reduction** |
| Risk of typo/error | High | Low | ✅ Safer |
| Duplication | Yes (config + query) | No | ✅ DRY |
| Maintenance burden | High | Low | ✅ Easier |

## Conclusion

**Simplified approach wins!**
- ✅ 67% less configuration
- ✅ Single source of truth (query labels)
- ✅ Database still validates types
- ✅ Same performance benefits
- ✅ Easier to adopt and maintain

The key insight: **The Cypher query already tells us the endpoint types through node labels.** We were duplicating information unnecessarily.

## Important Constraint: Unlabeled Nodes

### The Challenge
```cypher
# ✅ Works great (labels present)
MATCH (u:User)-[:LIKES]->(p:Post)
# Can infer: from_type='User', to_type='Post'

# ⚠️ Ambiguous (no labels)
MATCH (u)-[:LIKES]->(p)
# Cannot infer types!
```

### Solution: Graceful Degradation

**Strategy**: Omit type filters when labels missing (works but slower)

```sql
-- With labels (fast - 4 filters)
WHERE u.node_type = 'User'
  AND r.relation_type = 'LIKES'
  AND r.from_type = 'User'
  AND r.to_type = 'Post'
  
-- Without labels (slower - 1 filter, scans all types)
WHERE r.relation_type = 'LIKES'
  -- Missing type filters!
```

**Recommendation**:
1. ✅ **Always use labeled nodes** for polymorphic relationships: `(u:User)-[:LIKES]->(p:Post)`
2. ⚠️ Unlabeled queries work but emit warning: "Add labels for better performance"
3. System gracefully degrades (scans all types) rather than failing

**Why this is acceptable**:
- Most real queries have labels anyway (Neo4j best practice)
- Clear error messages guide users
- Unlabeled queries still work (just slower)
- Can add validation later if needed: `require_labels: true` config option
