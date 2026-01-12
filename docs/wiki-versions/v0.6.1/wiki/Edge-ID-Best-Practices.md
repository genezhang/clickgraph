> **Note**: This documentation is for ClickGraph v0.6.1. [View latest docs →](../../wiki/Home.md)
# Edge ID Best Practices for ClickGraph

## Overview

Edge uniqueness in variable-length paths requires tracking which edges have been traversed. ClickGraph supports three methods with different performance characteristics:

### Performance Comparison

| Method | Schema Config | Generated SQL | Performance | Use Case |
|--------|---------------|---------------|-------------|----------|
| **Single Column** | `edge_id: follow_id` | `rel.follow_id` | ⚡ **Fastest** | **Recommended default** |
| **Composite Key** | `edge_id: [col1, col2, ...]` | `tuple(rel.col1, rel.col2, ...)` | ⚠️ Slower | Complex edge identities |
| **Default Tuple** | `edge_id: null` (omitted) | `tuple(rel.from_id, rel.to_id)` | ⚠️ Slower | Legacy/fallback |

## Recommended Schema Design

### ✅ Best Practice: Add Dedicated edge_id Column

```yaml
relationships:  # YAML key remains 'relationships:' for backward compatibility
  - type: FOLLOWS
    table: user_follows
    from_id: follower_id
    to_id: followed_id
    edge_id: follow_id  # ← Single-column edge ID (optimal)
    from_node: User
    to_node: User
```

**SQL Schema:**
```sql
CREATE TABLE user_follows (
    follow_id UInt64,         -- ← Dedicated edge ID (PRIMARY KEY or unique)
    follower_id UInt32,
    followed_id UInt32,
    follow_date Date
) ENGINE = MergeTree()
ORDER BY (follower_id, followed_id);
```

### Why Single-Column edge_id is Faster

**Without edge_id (default):**
```sql
-- Uses tuple() for every edge check
arrayConcat(vp.path_edges, [tuple(rel.follower_id, rel.followed_id)])
NOT has(vp.path_edges, tuple(rel.follower_id, rel.followed_id))
-- Array type: Array(Tuple(UInt32, UInt32))
```

**With single-column edge_id:**
```sql
-- Direct value, no tuple overhead
arrayConcat(vp.path_edges, [rel.follow_id])
NOT has(vp.path_edges, rel.follow_id)
-- Array type: Array(UInt64)
```

**Performance Benefits:**
- ✅ Smaller array elements (8 bytes vs 16+ bytes for tuple)
- ✅ Faster `has()` operations (direct value comparison)
- ✅ Better ClickHouse array cache efficiency
- ✅ Reduced memory for path tracking

## When to Use Composite Keys

Use composite keys only when edge uniqueness requires multiple columns:

```yaml
relationships:  # YAML key remains 'relationships:' for backward compatibility
  - type: FLIGHT
    table: flights
    from_id: origin_airport
    to_id: dest_airport
    edge_id: [flight_date, flight_number, origin_airport, dest_airport]
    # Necessary: Same route, different flights = different edges
```

**Generated SQL:**
```sql
tuple(rel.flight_date, rel.flight_number, rel.origin_airport, rel.dest_airport)
```

## Composite Edge IDs with Polymorphic Tables

When using a **polymorphic edge table** (single table with multiple edge types), composite edge IDs are often necessary to ensure uniqueness across different interaction types:

```yaml
edges:
  - polymorphic: true
    table: interactions
    from_id: from_id
    to_id: to_id
    type_column: interaction_type
    type_values: [FOLLOWS, LIKES, AUTHORED, COMMENTED]
    
    # Composite edge ID includes type + timestamp for uniqueness
    edge_id: [from_id, to_id, interaction_type, timestamp]
```

**Why this is needed:**
- The same user pair `(1, 2)` might have multiple interactions (FOLLOWS + LIKES)
- Adding `interaction_type` distinguishes different relationship types
- Adding `timestamp` allows multiple interactions of the same type

**Generated VLP SQL:**
```sql
WITH RECURSIVE variable_path_xxx AS (
    SELECT 
        ...,
        [tuple(rel.from_id, rel.to_id, rel.interaction_type, rel.timestamp)] as path_edges
    FROM brahmand.interactions rel
    WHERE rel.interaction_type = 'FOLLOWS' AND ...
    UNION ALL
    SELECT
        ...,
        arrayConcat(vp.path_edges, [tuple(rel.from_id, rel.to_id, rel.interaction_type, rel.timestamp)])
    WHERE vp.hop_count < max_hops
      AND NOT has(vp.path_edges, tuple(rel.from_id, rel.to_id, rel.interaction_type, rel.timestamp))
      AND rel.interaction_type = 'FOLLOWS'
)
```

**Note:** Both the type filter (`interaction_type = 'FOLLOWS'`) and the composite edge ID work together correctly.

## Migration Guide

### Adding edge_id to Existing Tables

**Option 1: Alter table (persistent storage)**
```sql
-- Add auto-incrementing edge_id
ALTER TABLE user_follows ADD COLUMN follow_id UInt64;

-- Populate with row numbers
ALTER TABLE user_follows UPDATE follow_id = rowNumberInAllBlocks() WHERE 1;
```

**Option 2: Recreate table (Memory engine - Windows)**
```sql
-- Create temp table with edge_id
CREATE TABLE user_follows_temp (
    follow_id UInt64,
    follower_id UInt32,
    followed_id UInt32,
    follow_date Date
) ENGINE = Memory;

-- Copy with auto-generated IDs
INSERT INTO user_follows_temp
SELECT 
    row_number() OVER () as follow_id,
    follower_id,
    followed_id,
    follow_date
FROM user_follows;

-- Swap tables
DROP TABLE user_follows;
RENAME TABLE user_follows_temp TO user_follows;
```

### Update Schema YAML

```yaml
# Before
relationships:  # YAML key remains 'relationships:' for backward compatibility
  - type: FOLLOWS
    from_id: follower_id
    to_id: followed_id
    # No edge_id → Uses tuple(from_id, to_id)

# After
relationships:
  - type: FOLLOWS
    from_id: follower_id
    to_id: followed_id
    edge_id: follow_id  # ← Add this line
```

## Verification

### Check Generated SQL

```python
import requests

response = requests.post("http://localhost:8080/query", json={
    "query": "MATCH (a)-[:FOLLOWS*1..2]->(b) RETURN COUNT(*)",
    "schema_name": "your_schema"
})

sql = response.json()["sql"]

# Look for optimization
if "follow_id" in sql and "tuple(follower_id, followed_id)" not in sql:
    print("✅ Using optimized single-column edge_id")
else:
    print("⚠️ Using default tuple(from_id, to_id)")
```

### Run Tests

```bash
# Unit tests
cargo test --lib edge_uniqueness_tests

# Integration tests
python tests/integration/test_edge_id_optimization.py
```

## Implementation Details

### Code References

**Schema Definition:**
- `src/graph_catalog/config.rs` - `Identifier` enum, `edge_id` field

**SQL Generation:**
- `src/clickhouse_query_generator/variable_length_cte.rs`
  - `build_edge_tuple_base()` - Line 150
  - `build_edge_tuple_recursive()` - Line 177
  - `get_path_edges_array_type()` - Line 207

**Schema Integration:**
- `src/render_plan/cte_extraction.rs` - Line 688-730

### Decision Logic

```rust
match &self.edge_id {
    Some(Identifier::Single(col)) => {
        // ✅ Optimal: Single column, no tuple
        format!("{}.{}", self.edge_alias, col)
    }
    Some(Identifier::Composite(cols)) => {
        // Necessary: Multiple columns form composite key
        let elements = cols.iter().map(...).collect();
        format!("tuple({})", elements.join(", "))
    }
    None => {
        // Fallback: Use (from_id, to_id) as composite
        format!("tuple({}.{}, {}.{})", ...)
    }
}
```

## Summary

**Performance Optimization Checklist:**
- ✅ Add `edge_id` column to edge tables (auto-increment or UUID)
- ✅ Use `UInt64` or `UUID` type (not composite unless necessary)
- ✅ Update schema YAML with `edge_id: column_name`
- ✅ Verify generated SQL avoids tuple() for single columns
- ✅ Use composite keys ONLY when truly needed for uniqueness

**Expected Performance Gain:**
- ~20-40% faster variable-length path queries
- Reduced memory usage for path tracking
- Better scalability for deep path searches (e.g., `*1..10`)

---

*Last updated: November 29, 2025*
