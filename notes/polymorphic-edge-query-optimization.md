# Polymorphic Edge Query Optimization Design

## Problem Statement

Currently, when querying polymorphic edges (especially with wildcards or alternation), ClickGraph generates UNION ALL queries even when the underlying table structure is unified. This is inefficient for well-designed polymorphic tables.

## Key Insight

A **true polymorphic edge table** has:
- **Single `from_id` column** for all edge types
- **Single `to_id` column** for all edge types  
- **`type_column`** storing the edge type (e.g., `interaction_type = 'FOLLOWS'`)
- **Optional `from_label_column`/`to_label_column`** for node type filtering

When these conditions are met, we can query the table **directly without UNION ALL**.

---

## Schema Pattern Analysis

### Pattern A: True Polymorphic (Unified IDs) ✅ NO UNION NEEDED

```yaml
edges:
  - polymorphic: true
    table: interactions
    from_id: from_id          # Same column for ALL edge types
    to_id: to_id              # Same column for ALL edge types
    type_column: interaction_type
    from_label_column: from_type
    to_label_column: to_type
    type_values: [FOLLOWS, LIKES, AUTHORED, COMMENTED]
```

**Table Structure:**
```sql
CREATE TABLE interactions (
    from_id UInt32,            -- Universal source ID
    to_id UInt32,              -- Universal target ID
    interaction_type String,   -- Edge type discriminator
    from_type String,          -- Source node label
    to_type String,            -- Target node label
    timestamp DateTime,
    weight Float32
);
```

**Query Optimization:**

| Cypher | Optimized SQL |
|--------|--------------|
| `MATCH (a:User)-[:FOLLOWS]->(b:User)` | `SELECT ... FROM interactions WHERE interaction_type = 'FOLLOWS' AND from_type = 'User' AND to_type = 'User'` |
| `MATCH (a:User)-[:FOLLOWS\|LIKES]->(b)` | `SELECT ... FROM interactions WHERE interaction_type IN ('FOLLOWS', 'LIKES') AND from_type = 'User'` |
| `MATCH (a:User)-[r]->(b)` | `SELECT ... FROM interactions WHERE from_type = 'User'` |
| `type(r)` | `r.interaction_type` (column value, NOT literal!) |

### Pattern B: Separate Edge Tables (Standard) → UNION ALL Required

```yaml
edges:
  - type: FOLLOWS
    table: user_follows
    from_id: follower_id       # Different column names
    to_id: followed_id
    
  - type: LIKES
    table: post_likes
    from_id: user_id           # Different table entirely!
    to_id: post_id
```

**Query Pattern:**
- `[:FOLLOWS|LIKES]` → MUST use UNION ALL (different tables, columns)
- `type(r)` → Returns literal per UNION branch (correct!)

### Pattern C: Denormalized Edge Table (Edge + Node Props) → May Need UNION

```yaml
edges:
  - type: FOLLOWS
    table: user_follows_denorm
    from_id: follower_id
    to_id: followed_id
    is_denormalized: true
    from_node_properties: {...}
    to_node_properties: {...}
```

---

## Decision Tree: When to Use UNION ALL

```
Query: MATCH (a)-[r:TYPE1|TYPE2]->(b) RETURN type(r), ...

1. Are TYPE1 and TYPE2 from the SAME polymorphic table?
   ├── YES: Check column compatibility
   │   ├── Same from_id, to_id columns?
   │   │   ├── YES → NO UNION! Use WHERE type_column IN ('TYPE1', 'TYPE2')
   │   │   │         type(r) → r.type_column
   │   │   └── NO → UNION ALL required
   │   │             type(r) → Literal per branch
   └── NO: Different tables
       └── UNION ALL required
           type(r) → Literal per branch

2. Wildcard query: MATCH (a)-[r]->(b)
   ├── All edges from same polymorphic table?
   │   ├── YES → NO UNION! Query full table
   │   │         type(r) → r.type_column
   │   └── NO → UNION ALL of all edge tables
   │             type(r) → Literal per branch
```

---

## Implementation Changes Needed

### 1. Schema Detection: `is_unified_polymorphic()`

Add a method to detect if multiple edge types share the same polymorphic source:

```rust
impl GraphSchema {
    /// Returns true if all given edge types share:
    /// - Same underlying table
    /// - Same from_id column
    /// - Same to_id column  
    /// - A type_column discriminator
    fn is_unified_polymorphic(&self, edge_types: &[&str]) -> Option<UnifiedPolyInfo> {
        // Check all types reference same table with type_column
        // Return UnifiedPolyInfo { table, from_id, to_id, type_column }
    }
}
```

### 2. Query Planning: Avoid UNION for Unified Polymorphic

In `match_clause.rs` or the planner, when processing `[:TYPE1|TYPE2]`:

```rust
if let Some(poly_info) = schema.is_unified_polymorphic(&["TYPE1", "TYPE2"]) {
    // Generate single ViewScan with WHERE type_column IN (...)
    // NOT a Union of separate scans
} else {
    // Fall back to UNION ALL approach
}
```

### 3. `type(r)` Resolution: Use type_column

In `projection_tagging.rs`, for polymorphic edges with `type_column`:

```rust
// Current logic (problematic for unified polymorphic):
if rel_schema.type_column.is_some() {
    // Return type_column value
    item.expression = PropertyAccessExp(r.type_column)
} else {
    // Return literal
    item.expression = Literal(rel_type)
}
```

The issue is the current logic looks up by the FIRST label only. For wildcards, the labels list contains ALL types, but we should still return `type_column` if present.

### 4. Node Type Filtering via from_label_column/to_label_column

For queries like `MATCH (a:User)-[r]->(b:Post)`:

```sql
SELECT ... FROM interactions 
WHERE from_type = 'User'      -- from_label_column filter
  AND to_type = 'Post'        -- to_label_column filter
```

---

## type(r) Behavior Summary

| Schema Pattern | Query | type(r) Returns |
|---------------|-------|-----------------|
| Unified polymorphic | `[:FOLLOWS]` | `r.interaction_type` (column) |
| Unified polymorphic | `[:FOLLOWS\|LIKES]` | `r.interaction_type` (column) |
| Unified polymorphic | `[r]` (wildcard) | `r.interaction_type` (column) |
| Separate tables | `[:FOLLOWS]` | `'FOLLOWS'` (literal) |
| Separate tables | `[:FOLLOWS\|LIKES]` | `'FOLLOWS'` or `'LIKES'` per UNION branch |
| Separate tables | `[r]` (wildcard) | Literal per UNION branch |

---

## Example: Optimal Polymorphic Query

**Cypher:**
```cypher
MATCH (a:User)-[r]->(b) 
WHERE a.user_id = 1 
RETURN type(r), b.name
```

**Current SQL (UNION ALL - inefficient):**
```sql
SELECT 'FOLLOWS' AS type_r, b.name FROM ... WHERE ...
UNION ALL
SELECT 'LIKES' AS type_r, b.name FROM ... WHERE ...
UNION ALL
SELECT 'AUTHORED' AS type_r, b.name FROM ... WHERE ...
```

**Optimized SQL (Single Query):**
```sql
SELECT 
    r.interaction_type AS type_r,
    CASE 
        WHEN r.to_type = 'User' THEN users.username
        WHEN r.to_type = 'Post' THEN posts.title
    END AS "b.name"
FROM interactions r
JOIN users a ON r.from_id = a.user_id AND r.from_type = 'User'
LEFT JOIN users ON r.to_id = users.user_id AND r.to_type = 'User'
LEFT JOIN posts ON r.to_id = posts.post_id AND r.to_type = 'Post'
WHERE a.user_id = 1
```

Or with a unified "entities" approach, even simpler.

---

## Priority Assessment

1. **High Value**: Unified polymorphic → single query (major perf win)
2. **Medium Value**: Correct `type(r)` for polymorphic (return column, not literal)
3. **Lower Priority**: Full subgraph with no labels (complex node resolution)

---

## Current Implementation Status

- [x] Polymorphic schema parsing (type_column, from_label_column, etc.)
- [x] type(r) returns type_column for explicit single type
- [ ] Unified polymorphic detection for multi-type/wildcard
- [ ] Single-query generation for unified polymorphic
- [ ] type(r) returns type_column for wildcards on polymorphic
- [ ] from_label_column/to_label_column filtering in WHERE clause
