# Schema Variations: Comprehensive Analysis

## The Three Schema Patterns

### 1. Standard Schema (Separate Tables)

**Structure**: Each node label → separate table, each edge type → separate table

```yaml
nodes:
  - label: User
    table: users
    id_column: user_id
    
edges:
  - type: FOLLOWS
    table: user_follows
    from_id: follower_id
    to_id: followed_id
    
  - type: LIKES
    table: post_likes
    from_id: user_id
    to_id: post_id
```

**Characteristics**:
- Multiple edge tables for different relationship types
- Each table has its own column names for from/to IDs
- No `type_column` - type is implicit from table choice

**Query Strategy for `[:FOLLOWS|LIKES]`**:
- **UNION ALL** of separate table queries
- Each branch uses its own table with its own column mappings

**`type(r)` Returns**: Literal string injected at planning time
```sql
SELECT 'FOLLOWS' AS rel_type FROM user_follows ...
UNION ALL
SELECT 'LIKES' AS rel_type FROM post_likes ...
```

---

### 2. Denormalized Edge Schema (with Node Properties)

**Structure**: Edge table contains embedded node properties

```yaml
edges:
  - type: FOLLOWS
    table: follows_denormalized
    from_id: follower_id
    to_id: followed_id
    is_denormalized: true
    from_node_properties:
      name: follower_name
      email: follower_email
    to_node_properties:
      name: followed_name
      email: followed_email
```

**Sub-variation - Coupled Edge**: Edge only exists with specific node type

```yaml
edges:
  - type: AUTHORED
    table: posts  # Node table that has author_id
    from_id: author_id
    to_id: post_id
    coupled_node: Post  # Edge is "coupled" to Post node
```

**Characteristics**:
- Edge table contains from/to node properties inline
- No need to JOIN to node tables for property access
- Coupled edges: node table doubles as edge table
- Still separate tables per edge type (no polymorphism)

**Query Strategy for `[:FOLLOWS|LIKES]`**:
- **UNION ALL** (same as standard, just different property access)
- Property access uses denormalized columns instead of JOINs

**`type(r)` Returns**: Literal string (same as standard)
```sql
SELECT 'FOLLOWS' AS rel_type, r.followed_name FROM follows_denormalized r ...
```

---

### 3. Polymorphic Edge Schema (Single Table, Multiple Types)

**Structure**: Single edge table with type discriminator column

```yaml
edges:
  - polymorphic: true
    table: interactions
    from_id: from_id
    to_id: to_id
    type_column: interaction_type
    type_values:
      - FOLLOWS
      - LIKES
      - COMMENTED
```

**With Node Type Discrimination** (optional):
```yaml
edges:
  - polymorphic: true
    table: interactions
    from_id: from_id
    to_id: to_id
    type_column: interaction_type
    from_label_column: from_type    # e.g., 'User', 'Bot'
    to_label_column: to_type        # e.g., 'User', 'Post'
    type_values: [FOLLOWS, LIKES, COMMENTED]
```

**Characteristics**:
- Single table stores all edge types
- `type_column` discriminates edge type
- Optional `from_label_column`/`to_label_column` for node type filtering
- Unified `from_id`/`to_id` columns across all types

**Query Strategy for `[:FOLLOWS|LIKES]`**:
- **Single table query** with `WHERE type_column IN ('FOLLOWS', 'LIKES')`
- **NO UNION ALL needed!** This is the key optimization

**`type(r)` Returns**: Actual column value from database
```sql
SELECT r.interaction_type, ... FROM interactions r 
WHERE r.interaction_type IN ('FOLLOWS', 'LIKES')
```

---

## Comparison Matrix

| Aspect | Standard | Denormalized | Polymorphic |
|--------|----------|--------------|-------------|
| Edge storage | Separate tables | Separate tables (with node props) | Single table |
| Multi-type query | UNION ALL | UNION ALL | Single query + IN |
| `type(r)` value | Literal string | Literal string | Column value |
| Node property access | JOIN required | Inline (no JOIN) | JOIN required |
| Schema complexity | Simple | Medium | Medium |
| Query complexity | Higher for multi-type | Higher for multi-type | Lower for multi-type |

---

## Current Implementation Status

### ✅ Working

| Pattern | Standard | Denormalized | Polymorphic |
|---------|----------|--------------|-------------|
| Single type `[:FOLLOWS]` | ✅ | ✅ | ✅ |
| `type(r)` single type | ✅ | ✅ | ✅ |
| Bidirectional | ✅ | ✅ | ✅ |

### ⚠️ Partial / Bug

| Pattern | Standard | Denormalized | Polymorphic |
|---------|----------|--------------|-------------|
| Multi-type `[:A\|B]` | ✅ UNION | ✅ UNION | ⚠️ BUG: JOIN filters wrong |
| `type(r)` multi-type | ✅ | ✅ | ⚠️ Column correct, JOIN wrong |

### ❌ Not Working

| Pattern | Standard | Denormalized | Polymorphic |
|---------|----------|--------------|-------------|
| Wildcard `[r]` no target | ❌ | ❌ | ❌ Property resolution |

---

## The Polymorphic Multi-Type JOIN Bug

**Current Behavior** (buggy):
```sql
-- CTE correctly uses IN
WITH rel_a_b AS (
  SELECT ... FROM interactions WHERE interaction_type IN ('FOLLOWS', 'LIKES')
)
-- But JOIN incorrectly uses only first type!
INNER JOIN interactions AS r ON ... AND r.interaction_type = 'FOLLOWS'
```

**Root Cause**: In `graph_join_inference.rs`, the `pre_filter` is generated correctly via `generate_polymorphic_edge_filter()` with all types, but somewhere the JOIN generation only uses the first type.

**Fix Location**: Need to trace where the JOIN `pre_filter` gets overwritten or where only `rel_types[0]` is used.

---

## Optimization Opportunities

### Polymorphic Edge Optimization (Not Yet Implemented)

For polymorphic edges with unified ID columns, we can avoid UNION ALL entirely:

**Instead of** (current for non-polymorphic multi-type):
```sql
SELECT ... FROM follows WHERE ...
UNION ALL
SELECT ... FROM likes WHERE ...
```

**Generate** (for polymorphic):
```sql
SELECT ... FROM interactions 
WHERE interaction_type IN ('FOLLOWS', 'LIKES')
```

This is simpler, faster, and what the CTE extraction already does correctly.

### Denormalized Property Access (Working)

For denormalized edges, property access uses inline columns:
```sql
-- Instead of: SELECT u.name FROM follows f JOIN users u ON ...
SELECT f.followed_name FROM follows_denormalized f
```

---

## Key Code Locations

| Component | Standard | Denormalized | Polymorphic |
|-----------|----------|--------------|-------------|
| Schema parsing | `graph_schema.rs` | `graph_schema.rs` | `graph_schema.rs` |
| Edge resolution | `view_resolver.rs` | `view_resolver.rs` | `view_resolver.rs` |
| type(r) | `projection_tagging.rs` | `projection_tagging.rs` | `projection_tagging.rs` |
| CTE generation | `cte_extraction.rs` | `cte_extraction.rs` | `cte_extraction.rs` |
| JOIN generation | `graph_join_inference.rs` | `graph_join_inference.rs` | `graph_join_inference.rs` |
| Property mapping | `filter_tagging.rs` | Special handling | `filter_tagging.rs` |

---

## Test Coverage Needed

1. **Standard multi-type**: `[:FOLLOWS|LIKES]` with separate tables ✅
2. **Denormalized multi-type**: `[:FOLLOWS|LIKES]` with denormalized tables
3. **Polymorphic multi-type**: `[:FOLLOWS|LIKES]` with single polymorphic table ⚠️
4. **Mixed schemas**: Some edges standard, some polymorphic
5. **Wildcard with each schema type**: `[r]` pattern
