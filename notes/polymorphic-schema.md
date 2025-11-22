# Polymorphic Schema Support Design

**Status:** Proposal  
**Date:** November 20, 2025  
**Author:** Discussion with user

## Overview

Support graph schemas where node labels and relationship types are stored as column values in generic tables, rather than separate tables per type.

## Problem

Current schema requires separate tables for each node label and relationship type:
- `users` table for User nodes
- `posts` table for Post nodes  
- `user_follows` table for FOLLOWS relationships
- `user_likes` table for LIKES relationships
- etc.

This becomes unwieldy with many types and doesn't match some existing database designs.

## Solution: Type-Discriminator Pattern

### Single Node Table
```sql
CREATE TABLE entities (
    id UInt64,
    node_type String,      -- 'User', 'Post', 'Comment'
    name String,
    created_at DateTime,
    -- other properties
) ENGINE = MergeTree()
ORDER BY (node_type, id);
```

### Single Relationship Table
```sql
CREATE TABLE relationships (
    from_id UInt64,
    to_id UInt64,
    from_type LowCardinality(String),  -- 'User', 'Admin', etc. (source node type)
    to_type LowCardinality(String),    -- 'User', 'Post', etc. (target node type)
    relation_type LowCardinality(String),  -- 'FOLLOWS', 'LIKES', 'AUTHORED'
    created_at DateTime,
    -- other properties
) ENGINE = MergeTree()
ORDER BY (relation_type, from_type, to_type, from_id);
```

**Rationale for `from_type` and `to_type` columns**:
- **Problem**: Polymorphic relationships can connect different node types (User→Post, User→User)
- **Solution**: Store endpoint types in relationship row for explicit filtering
- **Performance**: Enables type-based partitioning and index optimization
- **Validation**: Prevents invalid type combinations at insert time

## YAML Schema Configuration

```yaml
graph_name: "polymorphic_graph"
database: "my_db"

nodes:
  - label: User
    table: entities
    id_column: id
    type_column: node_type      # NEW: Column containing label
    type_value: "User"          # NEW: Value to filter by
    properties:
      user_id: id
      name: name
      created_at: created_at

  - label: Post
    table: entities
    id_column: id
    type_column: node_type
    type_value: "Post"
    properties:
      post_id: id
      title: name
      created_at: created_at

relationships:
  - name: FOLLOWS
    from: from_id
    to: to_id
    table: relationships
    type_column: relation_type  # Column containing relationship type
    type_value: "FOLLOWS"       # Value to filter by
    # ✨ NO from_type/to_type config needed!
    # Inferred from query: (u:User)-[:FOLLOWS]->(o:User)

  - name: LIKES
    from: from_id
    to: to_id
    table: relationships
    type_column: relation_type
    type_value: "LIKES"
    # Inferred from query: (u:User)-[:LIKES]->(p:Post)
    properties:
      liked_at: created_at

  - name: AUTHORED
    from: from_id
    to: to_id
    table: relationships
    type_column: relation_type
    type_value: "AUTHORED"
    # Inferred from query: (u:User)-[:AUTHORED]->(p:Post)
    properties:
      created_at: created_at
```

## Query Translation

### Cypher
```cypher
MATCH (u:User)-[:FOLLOWS]->(other:User)
WHERE u.user_id = 1
```

### Generated SQL (with Automatic Type Inference)
```cypher
MATCH (u:User)-[:FOLLOWS]->(other:User)
WHERE u.id = 1
RETURN other.name
```

Query planner automatically extracts:
- Source label: `User` (from `u:User`)
- Target label: `User` (from `other:User`)
- Relationship type: `FOLLOWS`

Generated SQL:
```sql
SELECT other.name AS "other.name"
FROM entities AS u
INNER JOIN relationships AS r
  ON r.from_id = u.id
  AND r.relation_type = 'FOLLOWS'     -- From [:FOLLOWS]
  AND r.from_type = 'User'            -- ✨ INFERRED from u:User
  AND r.to_type = 'User'              -- ✨ INFERRED from other:User
INNER JOIN entities AS other
  ON other.id = r.to_id
  AND other.node_type = 'User'        -- From other:User
WHERE u.node_type = 'User'            -- From u:User
  AND u.id = 1
```

**Key Points**:
- ✨ **Zero config maintenance**: No from_type/to_type in YAML schema!
- ✨ **Automatic inference**: Extract endpoint types from query labels
- Database still stores `from_type` and `to_type` for filtering
- Four-way filtering: `relation_type`, `from_type`, `to_type`, and `node_type`
- Enables heterogeneous relationships (User→Post, User→User) in same table

## Implementation Changes

### 1. Schema Config (graph_catalog/config.rs)

```rust
pub struct NodeSchema {
    pub label: String,
    pub table: String,
    pub id_column: String,
    pub type_column: Option<String>,    // NEW
    pub type_value: Option<String>,     // NEW
    pub properties: HashMap<String, String>,
}

pub struct RelationshipSchema {
    pub name: String,
    pub from_column: String,
    pub to_column: String,
    pub table: String,
    pub type_column: Option<String>,        // Column for relation_type
    pub type_value: Option<String>,         // Value like 'FOLLOWS'
    // ✨ NO from_type/to_type config needed!
    // These are INFERRED from query labels:
    //   (u:User)-[:FOLLOWS]->(o:User)
    //   from_type = 'User', to_type = 'User'
    pub properties: HashMap<String, String>,
}
```

### 2. ViewScan Generation (query_planner/logical_plan/mod.rs)

When generating ViewScan for nodes/relationships with type discriminators, automatically add type filter:

```rust
fn generate_scan(...) -> Result<LogicalPlan> {
    let mut filters = vec![];
    
    // Add type filter if schema specifies type column
    if let (Some(type_col), Some(type_val)) = (schema.type_column, schema.type_value) {
        filters.push(LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(type_col)),
            op: Operator::Eq,
            right: Box::new(LogicalExpr::Literal(Literal::String(type_val))),
        });
    }
    
    // ... rest of scan generation
}
```

### 3. GraphRel Join Conditions (clickhouse_query_generator/graph_rel.rs)

Add type filters to JOIN conditions - **inferred from query labels**:

```rust
fn build_join_condition(..., pattern: &RelPattern) -> String {
    let mut conditions = vec![
        format!("{}.{} = {}.{}", rel_alias, from_col, from_alias, from_id),
        format!("{}.{} = {}.{}", to_alias, to_id, rel_alias, to_col),
    ];
    
    // Add relation_type filter
    if let (Some(type_col), Some(type_val)) = (rel_schema.type_column, rel_schema.type_value) {
        conditions.push(format!("{}.{} = '{}'", rel_alias, type_col, type_val));
    }
    
    // ✨ NEW: Add from_type filter (inferred from source node label)
    if let Some(from_label) = pattern.source_label {
        conditions.push(format!("{}.from_type = '{}'", rel_alias, from_label));
    }
    
    // ✨ NEW: Add to_type filter (inferred from target node label)
    if let Some(to_label) = pattern.target_label {
        conditions.push(format!("{}.to_type = '{}'", rel_alias, to_label));
    }
    
    conditions.join(" AND ")
}
    }
    
    conditions.join(" AND ")
}
```

## Performance Considerations

### The Heterogeneous Node Type Challenge

**Problem**: Polymorphic relationships can connect different node types (User→Post, User→User), but how do we know which types to filter?

**Example**:
```cypher
MATCH (u:User)-[:LIKES]->(p:Post)    // User → Post
MATCH (u:User)-[:FOLLOWS]->(o:User)  // User → User
```

Both use the same `relationships` table, but need different endpoint types!

**Solution**: Store endpoint types in relationship rows + INFER from query labels!
```sql
CREATE TABLE relationships (
    from_id UInt64,
    to_id UInt64,
    from_type LowCardinality(String),    -- 'User', 'Admin', etc.
    to_type LowCardinality(String),      -- 'User', 'Post', 'Comment', etc.
    relation_type LowCardinality(String),
    properties String
) ENGINE = MergeTree()
ORDER BY (relation_type, from_type, to_type, from_id);
```

**Benefits**:
- ✅ **Zero config maintenance** - No from_type/to_type in YAML!
- ✅ **Automatic inference** - Extract types from query labels
- ✅ **Query optimization** - ClickHouse can partition by type combinations
- ✅ **Data validation** - Relationship rows store actual types
- ✅ **Flexible schema** - Can model any type→type relationship

### ClickHouse Optimization
- Use `(relation_type, from_type, to_type, from_id)` as ORDER BY for optimal filtering
- Type columns should be `LowCardinality(String)` for efficiency (typically <100 unique values)
- Consider partitioning by `relation_type` for very large tables

### Index Strategy
```sql
CREATE TABLE entities (
    id UInt64,
    node_type LowCardinality(String),
    name String,
    created_at DateTime
) ENGINE = MergeTree()
PARTITION BY node_type           -- Separate partitions per type
ORDER BY (node_type, id);        -- Type-first ordering

CREATE TABLE relationships (
    from_id UInt64,
    to_id UInt64,
    from_type LowCardinality(String),
    to_type LowCardinality(String),
    relation_type LowCardinality(String),
    properties String
) ENGINE = MergeTree()
PARTITION BY relation_type       -- Separate partitions per relationship type
ORDER BY (relation_type, from_type, to_type, from_id);  -- Multi-level ordering
```

### Query Performance
With proper indexing, ClickHouse can efficiently handle:
```sql
-- Highly selective query (uses all type filters)
SELECT * FROM entities AS u
INNER JOIN relationships AS r
  ON r.from_id = u.id
  AND r.relation_type = 'LIKES'      -- Partition filter
  AND r.from_type = 'User'           -- Index filter
  AND r.to_type = 'Post'             -- Index filter
INNER JOIN entities AS p
  ON p.id = r.to_id
  AND p.node_type = 'Post'           -- Partition filter
WHERE u.node_type = 'User';
```

**Performance characteristics**:
- Partition pruning eliminates irrelevant data
- Index on `(relation_type, from_type, to_type, from_id)` allows efficient range scans
- LowCardinality compression reduces memory and disk I/O

## Benefits

1. **✅ Handles Heterogeneous Relationships** - Different endpoint types in same table
2. **✅ Simpler Schemas** - One table instead of many
3. **✅ Flexible** - Easy to add new types without schema changes
4. **✅ Common Pattern** - Matches existing database designs
5. **✅ Query Pushdown** - ClickHouse can optimize type filters
6. **✅ Data Validation** - Explicit types prevent invalid connections

## Limitations

1. **Mixed Properties** - All node types share same columns (use JSON for flexibility)
2. **Type Safety** - No schema-level type validation (runtime only)
3. **Performance** - Slightly slower than dedicated tables (but ClickHouse handles well)
4. **Requires Type Columns** - Must store `from_type` and `to_type` in relationships table
5. **⚠️ Unlabeled Node Constraint** - Queries MUST use labeled nodes for polymorphic relationships:
   - ✅ Works: `MATCH (u:User)-[:LIKES]->(p:Post)` (labels present)
   - ❌ Fails: `MATCH (u)-[:LIKES]->(p)` (cannot infer types)
   - **Workaround**: Omit type filters when no labels (scans all types)

## Handling Unlabeled Nodes

### The Challenge
```cypher
# Case 1: Labeled nodes (can infer types)
MATCH (u:User)-[:LIKES]->(p:Post)
# ✅ from_type='User', to_type='Post'

# Case 2: Unlabeled nodes (ambiguous!)
MATCH (u)-[:LIKES]->(p)
# ❓ What types to filter? Cannot infer!
```

### Strategy 1: Require Labels (RECOMMENDED) ⭐

**Approach**: Enforce labeled nodes for polymorphic relationships

```rust
// In match_clause analyzer
fn validate_polymorphic_pattern(pattern: &RelPattern) -> Result<()> {
    if rel_schema.is_polymorphic() {
        if pattern.source_node.labels.is_empty() {
            return Err(Error::UnlabeledNodeInPolymorphicRelationship {
                variable: pattern.source_node.variable,
                hint: "Add label like (u:User) for polymorphic relationships"
            });
        }
    }
    Ok(())
}
```

**Pros**: Clear semantics, optimal performance, explicit contracts  
**Cons**: More restrictive, requires query rewrites

### Strategy 2: Omit Type Filters (Fallback)

**Approach**: When labels missing, don't filter by type (scan all)

```rust
fn build_join_condition(rel_schema: &RelSchema, type_info: &TypeInfo) -> String {
    let mut conditions = vec![
        format!("{}.relation_type = '{}'", rel_alias, rel_schema.type_value),
    ];
    
    // Only add type filters if labels present
    if let Some(from) = type_info.from_label {
        conditions.push(format!("{}.from_type = '{}'", rel_alias, from));
    }
    // No from_label? Scan all from_types! (slower but works)
    
    if let Some(to) = type_info.to_label {
        conditions.push(format!("{}.to_type = '{}'", rel_alias, to));
    }
    
    conditions.join(" AND ")
}
```

**SQL without labels**:
```sql
-- Only filters by relation_type (not from_type/to_type)
SELECT *
FROM entities AS u
INNER JOIN relationships AS r
  ON r.from_id = u.id
  AND r.relation_type = 'LIKES'  -- Only this filter!
  -- Missing: AND r.from_type = ? AND r.to_type = ?
INNER JOIN entities AS p
  ON p.id = r.to_id
```

**Pros**: Works with unlabeled queries, backward compatible  
**Cons**: Poor performance (full table scan), no type validation

### Strategy 3: Hybrid with Configuration

**Approach**: Make label requirement configurable per relationship

```yaml
relationships:
  - name: LIKES
    table: relationships
    type_column: relation_type
    type_value: "LIKES"
    require_labels: true      # NEW: Enforce labeled nodes
    
  - name: GENERIC_REL
    table: relationships
    type_column: relation_type
    type_value: "GENERIC_REL"
    require_labels: false     # Allow unlabeled (slower)
```

**Pros**: Flexible, explicit trade-offs  
**Cons**: More configuration

### Recommended Approach

**Use Strategy 2 (Omit Type Filters) with warning**:
1. If labels present → Include type filters (fast)
2. If labels missing → Omit type filters + log warning (slow but works)
3. Add query hint: "Add labels for better performance: (u:User)-[:LIKES]->(p:Post)"

```rust
// Implementation
if type_info.from_label.is_none() || type_info.to_label.is_none() {
    log::warn!(
        "Polymorphic relationship {} without node labels - scanning all types. \
         Add labels for better performance: (u:User)-[:LIKES]->(p:Post)",
        pattern.rel_type
    );
}
```

## Alternative Solutions

### Option A: Explicit Type Columns (RECOMMENDED) ⭐

Store endpoint types in relationship rows (described above). Best for:
- ✅ Production systems requiring validation
- ✅ Heterogeneous relationships (User→Post, User→User in same table)
- ✅ Query optimization via partitioning
- ✅ Explicit data contracts

### Option B: Infer from Cypher Query Labels

Don't store types in data - infer from query:
```cypher
MATCH (u:User)-[:LIKES]->(p:Post)
-- Planner knows: u is User, p is Post from the pattern!
```

**Pros**: No schema changes needed, flexible  
**Cons**: Requires explicit labels in queries, no data validation, poor performance

**When to use**: Development/exploration, flexible schemas without validation

### Option C: Domain-Specific Polymorphic Tables

Multiple polymorphic tables for different domains:
```sql
-- Social relationships (User→User only)
CREATE TABLE user_relationships (
    from_id UInt64,
    to_id UInt64,
    relation_type String  -- 'FOLLOWS', 'BLOCKS', 'FRIENDS'
);

-- Engagement relationships (User→Content)
CREATE TABLE engagement (
    user_id UInt64,
    content_id UInt64,
    content_type String,  -- 'Post', 'Video', 'Comment'
    action_type String    -- 'LIKES', 'SHARES', 'VIEWS'
);
```

**Pros**: Natural boundaries, better performance, domain-specific columns  
**Cons**: Multiple tables (still simpler than one-per-type)

**When to use**: Clear domain boundaries, performance-critical systems

### Recommended Strategy: Hybrid with Smart Defaults

Make type columns **optional** with intelligent fallback:

```yaml
relationships:
  - name: FOLLOWS
    table: relationships
    type_column: relation_type
    type_value: "FOLLOWS"
    # Optional - if specified, use explicit filtering
    from_type_column: from_type
    from_type_value: "User"
    to_type_column: to_type
    to_type_value: "User"
```

**Fallback logic**:
1. If `from_type_column` specified → Use explicit type filters (best)
2. Else if node labels in query → Infer from Cypher pattern (fallback)
3. Else → No type filtering (user's responsibility)

This gives users flexibility to:
- Start simple (no type columns)
- Add validation later (add type columns)
- Mix approaches per relationship type

## Migration Path

### From Dedicated Tables
```sql
-- Old schema: multiple tables
INSERT INTO entities 
SELECT id, 'User' as node_type, name, created_at FROM users
UNION ALL
SELECT id, 'Post' as node_type, title as name, created_at FROM posts;

INSERT INTO relationships
SELECT user_id, followed_id, 'FOLLOWS' as relation_type, follow_date FROM user_follows
UNION ALL  
SELECT user_id, post_id, 'LIKES' as relation_type, like_date FROM user_likes;
```

## Testing Strategy

1. Create polymorphic schema YAML config
2. Load sample data with multiple types
3. Test Cypher queries with type filters
4. Verify SQL generation includes type predicates
5. Benchmark vs dedicated tables
6. Add integration tests

## Open Questions

1. Should we support mixed schemas (some polymorphic, some dedicated)?
2. How to handle type hierarchies (e.g., User -> AdminUser)?
3. Should we auto-detect polymorphic patterns from schema?

## Next Steps

1. ✅ Document design (this file)
2. Add `type_column` and `type_value` to schema structs
3. Update ViewScan generation to include type filters
4. Update GraphRel JOIN conditions
5. Create example polymorphic schema
6. Add integration tests
7. Update documentation

## References

- Generic Relationship Pattern: Common in ORMs (Rails, Django)
- ClickHouse LowCardinality: https://clickhouse.com/docs/en/sql-reference/data-types/lowcardinality
- Single Table Inheritance: https://martinfowler.com/eaaCatalog/singleTableInheritance.html
