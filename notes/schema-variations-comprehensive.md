# Schema Variations: Comprehensive Analysis

## Two Orthogonal Dimensions

ClickGraph supports schema variations across **two orthogonal dimensions**:

1. **Edge Storage Pattern** (How edge types are organized in tables)
2. **Coupled Edge Optimization** (Whether edge and node share a table)

These dimensions are independent - any combination is possible.

---

## Dimension 1: Edge Storage Patterns

### 1.1 Standard Schema (Separate Tables)

**Structure**: Each edge type → separate table

```yaml
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

### 1.2 Denormalized Edge Schema

**Structure**: Edge table contains embedded node properties OR edge table IS the node table

**Variant A - Embedded Node Properties**:
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

**Variant B - Edge Table = Node Table**:
```yaml
nodes:
  - label: Post
    table: posts
    node_id: post_id

edges:
  - type: AUTHORED
    table: posts        # Same table as Post node!
    from_id: author_id
    to_id: post_id
    from_node: User
    to_node: Post
```

**Characteristics**:
- Edge table contains from/to node properties inline, OR
- Edge table IS the same physical table as a node
- No need to JOIN to node tables for property access
- Still separate tables per edge type (like Standard)

**Query Strategy for `[:FOLLOWS|LIKES]`**:
- **UNION ALL** (same as standard)
- Property access uses denormalized columns instead of JOINs

**`type(r)` Returns**: Literal string (same as standard)
```sql
SELECT 'FOLLOWS' AS rel_type, r.followed_name FROM follows_denormalized r ...
```

---

### 1.3 Polymorphic Edge Schema (Single Table, Multiple Types)

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

## Dimension 2: Coupled Edge Optimization

### What Are Coupled Edges?

**Coupled edges** occur when **two or more edges** share the same physical table AND connect through common **coupling nodes**. This creates an opportunity for alias unification and self-join elimination.

**Key insight**: This is ORTHOGONAL to the three edge storage patterns above, but most commonly occurs with denormalized schemas.

### 2.1 Coupled Edges on Denormalized Tables (Most Common)

When multiple edges in the same pattern use the same denormalized table AND connect through a common node, they're "coupled" through that node.

**Example Schema** (DNS logs):
```yaml
nodes:
  - label: IP
    table: dns_logs
    node_id: client_ip
  - label: Domain  
    table: dns_logs
    node_id: query_domain

edges:
  - type: QUERIED         # Edge 1
    table: dns_logs       # Same table!
    from_id: client_ip
    to_id: query_domain
  - type: RESOLVED_TO     # Edge 2
    table: dns_logs       # Same table!
    from_id: query_domain
    to_id: resolved_ip
```

**Query**: `MATCH (ip:IP)-[r1:QUERIED]->(d:Domain)-[r2:RESOLVED_TO]->(resolved:IP)`

Here, `r1` and `r2` are **coupled** because:
1. Both use the same table (`dns_logs`)
2. They share a coupling node (`d:Domain`)

**Without Optimization**:
```sql
SELECT ...
FROM dns_logs r1
JOIN dns_logs d ON r1.query_domain = d.query_domain
JOIN dns_logs r2 ON r2.query_domain = d.query_domain  -- Self-join!
```

**With Coupled Edge Optimization**:
```sql
SELECT ...
FROM dns_logs r1  -- r1, d, and r2 all unified to same alias!
WHERE r1.query_domain IS NOT NULL
```

---

### 2.2 Polymorphic (Not Applicable)

Polymorphic schemas typically don't have coupled edges because:
- There's only ONE edge definition (with multiple `type_values`)
- Multiple edge types are distinguished by `type_column`, not separate edge definitions
- No opportunity for alias unification across different edge definitions

---

## Comparison Matrix

### Edge Storage Patterns

| Aspect | Standard | Denormalized | Polymorphic |
|--------|----------|--------------|-------------|
| Edge storage | Separate tables | Separate tables (with node props) | Single table |
| Multi-type query | UNION ALL | UNION ALL | Single query + IN |
| `type(r)` value | Literal string | Literal string | Column value |
| Node property access | JOIN required | Inline (no JOIN) | JOIN required |
| Schema complexity | Simple | Medium | Medium |

### Coupled Edge Applicability

| Schema Type | Coupled Edges Possible? | Optimization |
|-------------|-------------------------|--------------|
| Standard | No (separate tables per edge) | N/A |
| Denormalized | ✅ Yes (when 2+ edges share table) | Alias unification, self-join elimination |
| Polymorphic | No (single edge definition) | N/A |

---

## Current Implementation Status

### Edge Storage Patterns

| Pattern | Standard | Denormalized | Polymorphic |
|---------|----------|--------------|-------------|
| Single type `[:FOLLOWS]` | ✅ | ✅ | ✅ (requires labels) |
| `type(r)` single type | ✅ | ✅ | ✅ (requires labels) |
| Bidirectional | ✅ | ✅ | ✅ (requires labels) |
| Multi-type `[:A\|B]` | ✅ UNION | N/A (single type) | ✅ IN clause (requires labels) |
| `type(r)` multi-type | ✅ | N/A | ✅ (requires labels) |
| VLP exact `*2` | ✅ | N/A | ✅ (requires labels) |
| VLP range `*1..3` | ✅ | N/A | ✅ (requires labels) |
| WHERE node prop | ✅ | ✅ | ✅ |
| `type(r)` in WHERE | ✅ | N/A | ✅ (requires labels) |
| OPTIONAL MATCH | ✅ | N/A | ✅ |
| COUNT aggregation | ✅ | ✅ | ✅ (requires labels) |
| Wildcard `[r]` no target | ❌ | ❌ | ❌ |

**Note**: Polymorphic schemas require explicit node labels because the edge doesn't have
static `from_node`/`to_node` values - node types are determined at runtime via
`from_label_column`/`to_label_column`.

### Coupled Edge Optimization

| Pattern | Denormalized (with coupled edges) |
|---------|-----------------------------------|
| Multi-hop alias unification | ✅ |
| Self-join elimination | ✅ |
| Bidirectional coupled | ⚠️ Untested |

---

## Optimization Summary

| Optimization | When Applied | Benefit |
|--------------|--------------|---------|
| Polymorphic IN clause | `[:A\|B]` on polymorphic edge | Avoid UNION ALL |
| Denormalized property access | Node property on denormalized edge | Avoid JOIN to node |
| Coupled edge alias unification | 2+ edges on same denormalized table with coupling node | Eliminate self-JOINs |

---

## Testing Checklist

### Edge Storage (Nov 30, 2025 - All passing!)

- [x] Standard: 14/14 tests passing
  - single edge, multi-edge UNION, type(r), VLP, bidirectional, coupled edge
- [x] Denormalized: 7/7 tests passing
  - single edge, type(r), property access without JOIN, coupled edge
- [x] Polymorphic: 11/11 tests passing
  - single edge, multi-edge IN, type(r), VLP, bidirectional (all require labels)

### Test Script

Run comprehensive tests:
```bash
python scripts/test/test_schema_variations.py
python scripts/test/test_schema_variations.py --schema standard  # Test one schema
```

### Coupled Edge (orthogonal - applies to denormalized only)

- [x] Denormalized + Coupled: FLIGHT pattern with Airport nodes
- [ ] Multi-hop DNS pattern with coupling nodes
- [ ] Verify self-JOIN elimination in generated SQL (needs manual review)

---

## Key Design Insight

**Coupled edges require two or more edges sharing the same table.**

They're detected when:
1. Two or more edge definitions use the same `table`
2. A query pattern chains these edges through a common coupling node
3. The optimizer can unify aliases and eliminate self-JOINs

This is a specialized optimization for denormalized schemas where a single physical table (like `dns_logs` or `flights`) contains multiple logical relationships.
