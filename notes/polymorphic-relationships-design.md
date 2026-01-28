# Polymorphic Relationships Design - Ultra-Simplified Single Spec

**Date**: November 20, 2025  
**Status**: Proposed Design  
**Estimated Complexity**: Medium (2-3 days implementation)

## Problem Statement

User has:
- ✅ **Separate node tables** (users, posts, comments, etc.)
- ✅ **Single polymorphic relationship table** storing all relationships
- ❌ Current schema requires listing every relationship type separately (verbose!)

## Key Insight

If there's a **single polymorphic relationship table** in the schema, we can use **ONE relationship spec** that works for **unlimited relationship types**!

**Hybrid Flexibility**: Explicit relationships can coexist as exceptions:
- **Explicit relationships** (listed individually) = Higher priority, dedicated tables
- **Polymorphic relationship** (single spec) = Fallback catch-all, handles everything else
- Resolution order: Explicit → Polymorphic → Error

## Proposed Solution: Single Polymorphic Relationship Spec

### Ultra-Simple Configuration

```yaml
database: "my_db"

nodes:
  - label: User
    table: users
    node_id: id
    properties:
      user_id: id
      name: name
      email: email
      
  - label: Post
    table: posts
    node_id: id
    properties:
      post_id: id
      title: title
      content: content
      
  - label: Comment
    table: comments
    node_id: id
    properties:
      comment_id: id
      text: text

relationships:
  - polymorphic: true              # ✨ Single polymorphic spec for ALL relationships
    table: relationships
    from_id: from_id               # Source ID column
    to_id: to_id                   # Target ID column
    type_column: relation_type     # Column storing relationship type
    type_values:                   # ✨ OPTIONAL: Values in type_column (enables validation)
      - FOLLOWS
      - LIKES
      - AUTHORED
      - COMMENTED
      - SHARED
    from_label_column: from_type   # Column storing source label
    to_label_column: to_type       # Column storing target label
    # OR omit type_values = no validation (maximum flexibility)
    properties:
      created_at: created_at
      weight: weight
```

**That's it!** No need to list individual relationship types.

### Database Schema

```sql
-- Separate node tables (unchanged)
CREATE TABLE users (
    id UInt64,
    name String,
    email String
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE posts (
    id UInt64,
    title String,
    content String
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE comments (
    id UInt64,
    text String,
    post_id UInt64
) ENGINE = MergeTree() ORDER BY id;

-- Single polymorphic relationship table
CREATE TABLE relationships (
    from_id UInt64,
    to_id UInt64,
    relation_type LowCardinality(String),   -- 'FOLLOWS', 'LIKES', 'AUTHORED', 'COMMENTED'
    from_type LowCardinality(String),       -- 'User', 'Post', 'Comment'
    to_type LowCardinality(String),         -- 'User', 'Post', 'Comment'
    created_at DateTime,
    weight Float64
) ENGINE = MergeTree()
ORDER BY (relation_type, from_type, to_type, from_id);
```

## How It Works - Automatic Derivation

### Query Example 1: User likes Post
```cypher
MATCH (u:User)-[:LIKES]->(p:Post)
WHERE u.user_id = 1
RETURN p.title
```

**System automatically derives**:
1. Relationship type: `LIKES` (from `[:LIKES]` in query)
2. From label: `User` (from `u:User` in query)
3. From table: `users` (lookup `User` in nodes config)
4. To label: `Post` (from `p:Post` in query)
5. To table: `posts` (lookup `Post` in nodes config)

**Generated SQL**:
```sql
SELECT p.title
FROM users AS u
WHERE u.id = 1
INNER JOIN relationships AS r
  ON r.from_id = u.id
  AND r.relation_type = 'LIKES'      -- From [:LIKES]
  AND r.from_type = 'User'           -- From u:User
  AND r.to_type = 'Post'             -- From p:Post
INNER JOIN posts AS p
  ON p.id = r.to_id
```

### Query Example 2: User follows User
```cypher
MATCH (u:User)-[:FOLLOWS]->(other:User)
WHERE u.user_id = 1
RETURN other.name
```

**System derives** (same table for both endpoints!):
```sql
SELECT other.name
FROM users AS u
WHERE u.id = 1
INNER JOIN relationships AS r
  ON r.from_id = u.id
  AND r.relation_type = 'FOLLOWS'
  AND r.from_type = 'User'
  AND r.to_type = 'User'
INNER JOIN users AS other
  ON other.id = r.to_id
```

### Query Example 3: Multiple relationship types
```cypher
MATCH (u:User)-[:LIKES|AUTHORED]->(p:Post)
WHERE u.user_id = 1
RETURN p.title
```

**Generated SQL** (IN clause for multiple types):
```sql
SELECT p.title
FROM users AS u
WHERE u.id = 1
INNER JOIN relationships AS r
  ON r.from_id = u.id
  AND r.relation_type IN ('LIKES', 'AUTHORED')  -- Multiple types!
  AND r.from_type = 'User'
  AND r.to_type = 'Post'
INNER JOIN posts AS p
  ON p.id = r.to_id
```

## Implementation Changes

### 1. Schema Config (graph_catalog/config.rs)

```rust
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RelationshipSchema {
    pub polymorphic: Option<bool>,           // NEW: Marks single polymorphic spec
    pub table: String,
    pub from_column: String,                 // from_id
    pub to_column: String,                   // to_id
    
    // For polymorphic relationships (all optional)
    pub type_column: Option<String>,         // relation_type column name
    pub type_values: Option<Vec<String>>,    // ✨ NEW: Values in type_column
    pub from_label_column: Option<String>,   // from_type column name
    pub to_label_column: Option<String>,     // to_type column name
    
    // For traditional (dedicated table per type) - DEPRECATED for polymorphic
    pub name: Option<String>,                // 'FOLLOWS' - only for non-polymorphic
    
    pub properties: HashMap<String, String>,
}
```

### 2. Relationship Resolution (graph_catalog/mod.rs)

```rust
impl GraphCatalog {
    /// Resolve relationship type from query against schema
    /// Priority: Explicit relationships > Polymorphic > Error
    pub fn resolve_relationship(
        &self,
        rel_type: &str,              // 'LIKES' from query [:LIKES]
        from_label: &str,            // 'User' from query (u:User)
        to_label: &str,              // 'Post' from query (p:Post)
    ) -> Result<&RelationshipSchema> {
        // 1. ✨ Check explicit relationships FIRST (highest priority)
        if let Some(explicit_rel) = self.relationships.get(rel_type) {
            return Ok(explicit_rel);
        }
        
        // 2. ✨ Fallback to polymorphic relationship (if exists)
        if let Some(poly_rel) = self.find_polymorphic_relationship() {
            // Validate relationship type if type_values specified
            if let Some(type_values) = &poly_rel.type_values {
                if !type_values.contains(&rel_type.to_string()) {
                    return Err(Error::UnknownRelationshipType {
                        rel_type: rel_type.to_string(),
                        known_values: type_values.clone(),
                        hint: format!(
                            "Unknown relationship type '{}'. Known values in {}: {}. Add '{}' to type_values if it exists in data.",
                            rel_type,
                            poly_rel.type_column.as_ref().unwrap_or(&"relation_type".to_string()),
                            type_values.join(", "),
                            rel_type
                        ),
                    });
                }
            }
            
            // Verify labels exist in nodes config
            if !self.has_node_label(from_label) {
                return Err(Error::UnknownNodeLabel(from_label.to_string()));
            }
            if !self.has_node_label(to_label) {
                return Err(Error::UnknownNodeLabel(to_label.to_string()));
            }
            return Ok(poly_rel);
        }
        
        // 3. No match found
        Err(Error::UnknownRelationshipType(rel_type.to_string()))
    }
    
    fn find_polymorphic_relationship(&self) -> Option<&RelationshipSchema> {
        self.relationships.values()
            .find(|r| r.polymorphic == Some(true))
    }
    
    fn has_node_label(&self, label: &str) -> bool {
        self.nodes.contains_key(label)
    }
}
```

### 3. SQL Generation (clickhouse_query_generator/graph_rel.rs)
    
    Some(Arc::new(LogicalPlan::ViewScan(Arc::new(view_scan))))
}
```

#### B. SQL Rendering

**File**: `src/clickhouse_query_generator/view_scan.rs` (or equivalent)

Ensure ViewScan filters are rendered in SQL:

```rust
// Generate: SELECT ... FROM table WHERE relation_type = 'FOLLOWS' AND head_type = 'User'
pub fn render_view_scan_with_filter(scan: &ViewScan) -> String {
### 3. SQL Generation (clickhouse_query_generator/graph_rel.rs)

```rust
fn build_join_condition(
    rel_schema: &RelationshipSchema,
    pattern: &RelPattern,
) -> String {
    let mut conditions = vec![
        format!("{}.{} = {}.{}", 
            rel_alias, rel_schema.from_column,
            from_alias, from_id_col),
        format!("{}.{} = {}.{}",
            to_alias, to_id_col,
            rel_alias, rel_schema.to_column),
    ];
    
    if rel_schema.polymorphic == Some(true) {
        // Add polymorphic filters (all derived from query!)
        
        // 1. Relationship type filter
        if let Some(type_col) = &rel_schema.type_column {
            conditions.push(format!(
                "{}.{} = '{}'",
                rel_alias, type_col, pattern.rel_type  // From [:LIKES]
            ));
        }
        
        // 2. From label filter (derived from query)
        if let (Some(from_label_col), Some(from_label)) = 
            (&rel_schema.from_label_column, &pattern.source_label) {
            conditions.push(format!(
                "{}.{} = '{}'",
                rel_alias, from_label_col, from_label  // From (u:User)
            ));
        }
        
        // 3. To label filter (derived from query)
        if let (Some(to_label_col), Some(to_label)) = 
            (&rel_schema.to_label_column, &pattern.target_label) {
            conditions.push(format!(
                "{}.{} = '{}'",
                rel_alias, to_label_col, to_label  // From (p:Post)
            ));
        }
    }
    
    conditions.join(" AND ")
}
```

### 4. Multiple Relationship Types Optimization

For queries like `[:LIKES|AUTHORED]`:

```rust
// Before: Generate UNION (slow for polymorphic table)
SELECT ... FROM relationships WHERE relation_type = 'LIKES'
UNION ALL
SELECT ... FROM relationships WHERE relation_type = 'AUTHORED'

// After: Use IN clause (fast!)
SELECT ... 
FROM relationships
WHERE relation_type IN ('LIKES', 'AUTHORED')
  AND from_type = 'User'
  AND to_type = 'Post'
```

## Benefits of This Approach

### 🎯 Extreme Simplification

**Configuration Comparison**:

| Approach | Config Lines | Lines per New Type |
|----------|--------------|-------------------|
| Dedicated tables (current) | 10 × N types | 10 lines |
| Polymorphic with type specs | 7 × N types | 7 lines |
| **Polymorphic single spec** | **7 total** | **0 lines!** |

**Example**: For 10 relationship types:
- Before: 100 lines
- After: 7 lines
- **Reduction: 93%**

### ✅ Zero Maintenance (with Optional Validation)

**Without validation** (maximum flexibility):
1. Insert data: `INSERT INTO relationships VALUES (1, 2, 'BLOCKS', 'User', 'User', ...)`
2. Query immediately works: `MATCH (u:User)-[:BLOCKS]->(x:User)`
3. **No config change needed!**

**With validation** (`type_values` list - safer):
1. Insert data first
2. Add `'BLOCKS'` to `type_values` in config
3. Query works with typo protection
4. **Small maintenance, prevents expensive typo queries**

**Recommendation**: Use `type_values` for production, omit for development

### ✅ Type Safety from Data

- Node labels validated against config ✅
- Relationship types come from data ✅
- Invalid combinations caught at query time ✅

### ✅ Works with Separate Node Tables

- No need for single `entities` table
- Respects existing database design
- Each node type has its own optimized schema

### ✅ Hybrid Flexibility

- **Polymorphic as default**: Handles unlimited relationship types
- **Explicit as exception**: Override specific types with dedicated tables
- **Priority-based resolution**: Explicit > Polymorphic > Error
- **Use cases**:
  - High-performance relationships → explicit dedicated table
  - Special validation requirements → explicit with constraints
  - Everything else → polymorphic (zero config)

### ✅ Performance Benefits

```sql
-- Optimized index usage
ORDER BY (relation_type, from_type, to_type, from_id)

-- Query pattern matches index:
WHERE relation_type = 'LIKES'     -- First key
  AND from_type = 'User'          -- Second key
  AND to_type = 'Post'            -- Third key
  AND from_id = 123               -- Fourth key
```

## Edge Cases & Solutions

### Case 1: Unlabeled Nodes
```cypher
MATCH (u)-[:LIKES]->(p)  # No labels!
```

**Solution**: Omit label filters (slower but works)
```sql
WHERE r.relation_type = 'LIKES'
-- Missing: from_type, to_type filters
```
**Warning logged**: "Add labels for better performance"

### Case 2: Invalid Node Label
```cypher
MATCH (u:InvalidLabel)-[:LIKES]->(p:Post)
```

**Solution**: Validate at query time
```rust
if !catalog.has_node_label("InvalidLabel") {
    return Err(Error::UnknownNodeLabel("InvalidLabel"));
}
```

### Case 3: Mixed Schema (Hybrid Approach)
```yaml
relationships:
  - polymorphic: true       # Handles most relationships (fallback)
    table: relationships
    from_id: from_id
    to_id: to_id
    type_column: relation_type
    from_label_column: from_type
    to_label_column: to_type
    
  - name: SPECIAL_REL       # Exception: dedicated table (higher priority!)
    table: special_rels
    from_id: src
    to_id: dst
    
  - name: CRITICAL_EDGE     # Another exception with custom logic
    table: critical_edges
    from_id: source_id
    to_id: target_id
    properties:
      priority: priority_level
```

**Resolution Priority**:
1. ✅ Check explicit relationships FIRST (SPECIAL_REL, CRITICAL_EDGE)
2. ✅ If not found, check polymorphic relationship
3. ❌ If neither found, return error

**Use Cases**:
- Most relationships (FOLLOWS, LIKES, etc.) → polymorphic table
- Performance-critical relationships → dedicated optimized table
- Relationships with special constraints → explicit configuration

## Implementation Plan

### Phase 1: Schema Config (4 hours)
- Add `polymorphic` field to RelationshipSchema
- Add `from_label_column`, `to_label_column` fields
- Update YAML deserialization
- Add validation logic

### Phase 2: Relationship Resolution (4 hours)
- Implement `find_polymorphic_relationship()`
- Implement `resolve_relationship()` with label validation
- Handle fallback to dedicated tables
- Add error messages for invalid labels

### Phase 3: SQL Generation (6 hours)
- Extract labels from query patterns
- Add label filters to JOIN conditions
- Optimize multiple relationship types (IN clause)
- Handle unlabeled nodes gracefully

### Phase 4: Testing (8 hours)
- Unit tests: Schema validation
- Unit tests: Relationship resolution
- Integration tests: Various query patterns
- Integration tests: Edge cases
- Performance benchmarks

### Phase 5: Documentation (2 hours)
- Update schema configuration guide
- Add migration guide
- Document performance best practices
- Add troubleshooting section

**Total Estimated Time**: 2-3 days

## Migration Path

### From Dedicated Tables

```sql
-- Step 1: Create polymorphic table
CREATE TABLE relationships (
    from_id UInt64,
    to_id UInt64,
    relation_type LowCardinality(String),
    from_type LowCardinality(String),
    to_type LowCardinality(String),
    created_at DateTime
) ENGINE = MergeTree()
ORDER BY (relation_type, from_type, to_type, from_id);

-- Step 2: Migrate data
INSERT INTO relationships
SELECT 
    follower_id AS from_id,
    followed_id AS to_id,
    'FOLLOWS' AS relation_type,
    'User' AS from_type,
    'User' AS to_type,
    created_at
FROM user_follows;

-- Step 3: Update schema config (one-time)
relationships:
  - polymorphic: true
    table: relationships
    # ... 4 more lines

-- Step 4: Drop old tables (when ready)
DROP TABLE user_follows;
```

### Gradual Migration

1. Add polymorphic spec (doesn't break existing)
2. Migrate one relationship type at a time
3. Test queries after each migration
4. Drop old tables when complete

## Recommendation

**This design with optional `allowed_types` validation is optimal:**

✅ Separate node tables (existing architecture)  
✅ Single polymorphic relationship table  
✅ **7-10 lines total configuration** (vs 100+ lines before)  
✅ **Optional validation** - Balance flexibility vs safety  
✅ All relationship types derived from query  
✅ Performant with proper indexing  
✅ Clear error messages (especially with validation)  
✅ Backward compatible (explicit relationships as exceptions)

**Recommended Configuration**:
```yaml
relationships:
  - polymorphic: true
    table: relationships
    from_id: from_id
    to_id: to_id
    type_column: relation_type
    type_values:  # ⭐ RECOMMENDED: Values in relation_type column
      - FOLLOWS
      - LIKES
      - AUTHORED
      - COMMENTED
    from_label_column: from_type
    to_label_column: to_type
```

**Trade-off Analysis**:

| Aspect | Without Validation | With Validation (Recommended) |
|--------|-------------------|-------------------------------|
| Config maintenance | Zero | Low (add type to list) |
| Typo protection | None | Immediate clear error |
| Query cost on typo | Expensive table scan | Fast validation error |
| Flexibility | Maximum | High (just update list) |
| Production safety | Low | High |

**Key Innovation**: Since there's ONE polymorphic table handling most cases, listing types is minimal overhead (5-20 types typically) vs huge benefit (prevents expensive typo queries)!

## Example: Hybrid Configuration

```yaml
nodes:
  - label: User
    table: users
  - label: Post
    table: posts
  - label: Product
    table: products

relationships:
  # Polymorphic catch-all (handles 95% of relationships)
  - polymorphic: true
    table: relationships
    from_id: from_id
    to_id: to_id
    type_column: relation_type
    from_label_column: from_type
    to_label_column: to_type
    properties:
      created_at: created_at
      weight: weight
  
  # Exception 1: High-performance recommendation engine
  - name: RECOMMENDS
    table: recommendations_optimized
    from_id: user_id
    to_id: product_id
    properties:
      score: ml_score
      confidence: confidence
      model_version: model_version
    # Custom indexes, materialized views, etc.
  
  # Exception 2: Compliance-critical relationship
  - name: APPROVED_BY
    table: audit_approvals
    from_id: document_id
    to_id: approver_id
    properties:
      timestamp: approval_timestamp
      signature: digital_signature
      compliance_level: level
```

**Query Resolution**:
```cypher
# Uses polymorphic table (not in explicit list)
MATCH (u:User)-[:FOLLOWS]->(other:User)

# Uses polymorphic table
MATCH (u:User)-[:LIKES]->(p:Post)

# Uses explicit dedicated table (exception!)
MATCH (u:User)-[:RECOMMENDS]->(prod:Product)

# Uses explicit audit table (exception!)
MATCH (doc:Document)-[:APPROVED_BY]->(user:User)
```

## Open Questions

1. **Property mappings**: If different relationship types need different property names, how to handle?
   - **Option A**: Use generic property column names (weight, metadata_json)
   - **Option B**: JSON column for flexible properties per type
   - **Option C**: Define property mappings in polymorphic spec (applies to all)

2. **Validation**: Should we validate relationship types exist in data?
   - **Option A**: No validation (pure data-driven, maximum flexibility)
   - **Option B**: Optional `allowed_types: [FOLLOWS, LIKES, ...]` validation list
   - **Option C**: Query-time validation with helpful error if type not found

3. **Performance**: Should we support partitioning by relation_type?
   ```sql
   PARTITION BY relation_type
   ```
   - Pros: Better performance for specific types
   - Cons: More files, more complexity

**Recommended Answers**:
1. Option C (property mappings in spec, shared across types)
2. Option A (no validation, data-driven)
3. Optional (let users add PARTITION if needed)

```yaml
graph_schema:
  relationships:
    - type: FOLLOWS
      table: relationships
      type_column: relation_type
      type_value: "follows"
      from_id: user1_id
      to_id: user2_id
      
    - type: FRIENDS_WITH
      table: relationships
      type_column: relation_type
      type_value: "friends"
      from_id: user1_id
      to_id: user2_id
```

```cypher
-- Single type: Uses WHERE relation_type = 'follows'
MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a, b

-- Multiple types: Uses WHERE relation_type IN ('follows', 'friends')
MATCH (a:User)-[:FOLLOWS|FRIENDS_WITH]->(b:User) RETURN a, b
```

### Use Case 2: Heterogeneous Graph

```yaml
graph_schema:
  relationships:
    - type: LIKES
      table: interactions
      type_column: action_type
      type_value: "like"
      from_label_column: subject_type
      from_label_value: "User"
      to_label_column: object_type
      to_label_value: "Post"
      from_id: subject_id
      to_id: object_id
      
    - type: COMMENTS_ON
      table: interactions
      type_column: action_type
      type_value: "comment"
      from_label_column: subject_type
      from_label_value: "User"
      to_label_column: object_type
      to_label_value: "Post"
      from_id: subject_id
      to_id: object_id
```

```cypher
-- Filters: action_type='like' AND subject_type='User' AND object_type='Post'
MATCH (u:User)-[:LIKES]->(p:Post) RETURN u.name, p.title
```

### Use Case 3: Mixed Architecture

```yaml
graph_schema:
  relationships:
    # Polymorphic table
    - type: FOLLOWS
      table: relationships
      type_column: rel_type
      type_value: "FOLLOWS"
      
    # Dedicated table (no type_column)
    - type: PURCHASED
      table: purchases
      from_id: customer_id
      to_id: product_id
```

Both work seamlessly - polymorphic gets filters, dedicated doesn't.

## Performance Considerations

### Advantages
1. **Single Table Scan**: More efficient for queries spanning multiple types
2. **Index Optimization**: Composite index on `(relation_type, head_id, tail_id)`
3. **Reduced JOIN Overhead**: Fewer UNIONs needed

### Disadvantages
1. **Filter Overhead**: Every query needs type filter
2. **Index Selectivity**: May be lower with mixed types
3. **Data Skew**: Uneven type distribution affects query planning

### Recommended ClickHouse Schema

```sql
CREATE TABLE relationships (
    head_id UInt32,
    tail_id UInt32,
    relation_type LowCardinality(String),  -- Efficient for type column
    head_type LowCardinality(String),
    tail_type LowCardinality(String),
    created_at DateTime,
    properties String  -- JSON for flexible properties
) ENGINE = MergeTree()
PRIMARY KEY (relation_type, head_id, tail_id)  -- Type first for filtering
ORDER BY (relation_type, head_id, tail_id, created_at);

-- Recommended index
ALTER TABLE relationships ADD INDEX idx_type_ids (relation_type, head_id, tail_id) TYPE minmax GRANULARITY 4;
```

## Migration Path

### From Table-per-Type to Polymorphic

**Step 1**: Create unified table
```sql
CREATE TABLE relationships_unified AS 
SELECT 
    follower_id AS head_id,
    followed_id AS tail_id,
    'FOLLOWS' AS relation_type,
    'User' AS head_type,
    'User' AS tail_type,
    follow_date AS created_at
FROM user_follows
UNION ALL
SELECT 
    user1_id AS head_id,
    user2_id AS tail_id,
    'FRIENDS_WITH' AS relation_type,
    'User' AS head_type,
    'User' AS tail_type,
    created_date AS created_at
FROM friendships;
```

**Step 2**: Update schema YAML (add type_column, type_value)

**Step 3**: Test queries (verify results match)

**Step 4**: Drop old tables (when confident)

## Risks & Mitigation

| Risk | Impact | Mitigation |
|------|--------|------------|
| Performance regression | Medium | Add index recommendations to docs |
| Breaking change | Low | Backward compatible (type_column is optional) |
| Complex WHERE clauses | Low | Generate filters at ViewScan creation |
| Type value escaping | Medium | Use parameterized queries or escape strings |

## Success Criteria

- [ ] Can define polymorphic relationship tables in YAML
- [ ] Queries correctly filter by relationship type
- [ ] Queries correctly filter by node types (if specified)
- [ ] Multiple relationship types generate optimized IN clause
- [ ] No regression in existing table-per-type configurations
- [ ] Test coverage: 10+ new tests
- [ ] Documentation complete with examples

## Open Questions

1. **Type Value Escaping**: How to handle special characters in type values?
   - **Answer**: Use ClickHouse string escaping, or consider LowCardinality(Enum)

2. **Dynamic Type Discovery**: Should we auto-discover types from table?
   - **Answer**: No (v1) - explicit configuration only. Future enhancement.

3. **Type Aliasing**: Should `type_value` support aliases (e.g., "FOLLOWS" vs "follows")?
   - **Answer**: No - use exact values. Document case-sensitivity.

4. **NULL Type Values**: How to handle rows with NULL in type columns?
   - **Answer**: NULL rows are filtered out (WHERE ... = 'value' excludes NULLs)

## References

- Current schema: `src/graph_catalog/config.rs` lines 138-195
- ViewScan creation: `src/query_planner/logical_plan/match_clause.rs` lines 163-252
- Alternate types: `notes/alternate-relationships.md`
- WHERE clause: `notes/where-viewscan.md`

## Related Features

- **Variable-Length Paths**: Polymorphic relationships work with `*` patterns
- **Shortest Path**: Type filtering applies to path algorithms
- **PageRank**: Can specify relationship types from polymorphic table
- **Multiple Relationship Types**: Optimized with IN clause for same table

---

**Next Step**: Get user approval on design, then proceed with Phase 1 implementation.
