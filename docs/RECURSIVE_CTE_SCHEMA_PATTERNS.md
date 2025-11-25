# Recursive CTE Support for Schema Patterns

**Date**: November 24, 2025  
**Status**: Standard pattern only - Denormalized and Polymorphic require future work

---

## Current State

### ✅ What Works: Standard Schema Pattern

```cypher
MATCH (u1:User)-[:FOLLOWS*1..3]->(u2:User)
WHERE u1.name = 'Alice'
RETURN u2.name
```

**Generated CTE**:
```sql
WITH RECURSIVE variable_path_abc123 AS (
    -- Base case
    SELECT 
        start_node.user_id AS start_id,
        end_node.user_id AS end_id,
        start_node.full_name AS start_full_name,
        end_node.full_name AS end_full_name,
        1 AS hop_count
    FROM users AS start_node         -- ✅ Standard table
    JOIN follows AS rel              -- ✅ Standard table
        ON rel.follower_id = start_node.user_id
    JOIN users AS end_node           -- ✅ Standard table
        ON rel.followed_id = end_node.user_id
    
    UNION ALL
    
    -- Recursive case
    SELECT 
        prev.start_id,
        end_node.user_id AS end_id,
        prev.start_full_name,
        end_node.full_name AS end_full_name,
        prev.hop_count + 1
    FROM variable_path_abc123 AS prev
    JOIN follows AS rel
        ON rel.follower_id = prev.end_id
    JOIN users AS end_node
        ON rel.followed_id = end_node.user_id
    WHERE prev.hop_count < 3
)
SELECT end_full_name AS name
FROM variable_path_abc123
WHERE start_full_name = 'Alice'
```

**Why It Works**:
- All entities have separate tables
- Simple JOINs in both base and recursive cases
- Property access straightforward

---

## ❌ What Doesn't Work: Denormalized Schema Pattern

### The Core Challenge: Role-Dependent Property Mapping

**Critical Insight**: In denormalized schemas, the **same node label** maps to **different columns** depending on its **role** in the relationship!

```cypher
-- Simple 2-hop query that demonstrates the problem
MATCH (a:Airport)-[f:FLIGHT]->(b:Airport)-[g:FLIGHT]->(c:Airport)
WHERE a.city = 'Los Angeles'
RETURN c.city
```

**Role Analysis**:
```
Node  Label    Role(s)              Property Mapping
────  ─────    ─────────────────    ────────────────────────────────
a     Airport  FROM (in f)          city → f.OriginCityName
b     Airport  TO (in f)            city → f.DestCityName
              FROM (in g)           city → g.OriginCityName
c     Airport  TO (in g)            city → g.DestCityName
```

**The Problem**: Node `b` plays **TWO DIFFERENT ROLES**:
- As TO node in `(a)-[f]->(b)`: `b.city` → `f.DestCityName`
- As FROM node in `(b)-[g]->(c)`: `b.city` → `g.OriginCityName`

**Current Property Resolution Fails**:
- PropertyResolver maps `b.city` to ONE column, but it needs TWO different mappings
- In edge `f`: `b` is destination → use `DestCityName`
- In edge `g`: `b` is source → use `OriginCityName`

### Variable-Length Path Problem

```cypher
-- This query will FAIL!
MATCH (a:Airport)-[:FLIGHT*1..2]->(b:Airport)
WHERE a.city = 'Los Angeles'
RETURN b.city
```

**Schema** (`ontime_denormalized.yaml`):
```yaml
nodes:
  - label: Airport
    table: flights              # ← Same as edge table!
    from_node_properties:       # ← Properties depend on position
      code: Origin
      city: OriginCityName
    to_node_properties:
      code: Dest
      city: DestCityName

edges:
  - type: FLIGHT
    table: flights
    from_id: Origin
    to_id: Dest
```

**Current CTE Generation Attempts**:
```sql
WITH RECURSIVE variable_path_abc123 AS (
    SELECT 
        start_node.Origin AS start_id,           -- ❌ WRONG!
        end_node.Dest AS end_id,                 -- ❌ WRONG!
        start_node.OriginCityName AS start_city, -- ❌ WRONG!
        end_node.DestCityName AS end_city,       -- ❌ WRONG!
        1 AS hop_count
    FROM flights AS start_node        -- ❌ No separate Airport table!
    JOIN flights AS rel               -- Actually correct (same table)
        ON rel.Origin = start_node.???   -- ❌ What column to join on?
    JOIN flights AS end_node          -- ❌ Same table again, confusion!
        ON rel.Dest = end_node.???       -- ❌ Can't join flights to flights meaningfully
)
```

**Why It Fails**:
1. **No separate node table**: `Airport` is virtual, no `airports` table exists
2. **Can't JOIN flights to flights**: The FROM node and TO node are both conceptually on the same row
3. **Property mapping confusion**: `start_node.city` doesn't exist - it's `flights.OriginCityName` in FROM position
4. **Recursive case impossible**: Can't chain flights→flights→flights when nodes are virtual

### What Would Be Needed

**Option 1: Subquery Approach** (Complex)
```sql
WITH RECURSIVE variable_path_abc123 AS (
    -- Base case: Select FROM-TO pairs as "hops"
    SELECT 
        Origin AS start_id,
        Dest AS end_id,
        OriginCityName AS start_city,
        DestCityName AS end_city,
        ARRAY[Origin, Dest] AS path,
        1 AS hop_count
    FROM flights
    WHERE OriginCityName = 'Los Angeles'  -- Start filter
    
    UNION ALL
    
    -- Recursive case: Extend path by joining on matching Dest→Origin
    SELECT 
        prev.start_id,
        f.Dest AS end_id,
        prev.start_city,
        f.DestCityName AS end_city,
        arrayConcat(prev.path, [f.Dest]) AS path,
        prev.hop_count + 1
    FROM variable_path_abc123 AS prev
    JOIN flights AS f 
        ON f.Origin = prev.end_id  -- ← Key: Dest of prev = Origin of next
    WHERE prev.hop_count < 2
      AND NOT has(prev.path, f.Dest)  -- Cycle prevention
)
SELECT end_city AS city
FROM variable_path_abc123
```

**Changes Required in `VariableLengthCteGenerator`**:
1. Detect denormalized pattern (check ViewScan)
2. Use single table for both "nodes"
3. Join condition: `prev.end_id = next_flight.Origin` (not node table joins)
4. Property selection from edge table columns with position awareness
5. No separate node JOINs - just extend the path through edge table

**Estimated Effort**: 1-2 days

---

## ❌ What Doesn't Work: Polymorphic Schema Pattern

### The Problem

```cypher
-- This query will FAIL to filter correctly!
MATCH (u1:User)-[:FOLLOWS*1..3]->(u2:User)
RETURN u2.name
```

**Schema** (`social_polymorphic.yaml`):
```yaml
nodes:
  - label: User
    table: users
    id_column: user_id

edges:
  - polymorphic: true
    table: interactions
    type_column: interaction_type
    from_label_column: from_type
    to_label_column: to_type
    type_values:
      - FOLLOWS
      - LIKES
      - AUTHORED
```

**Current CTE Generation** (Missing Type Filters):
```sql
WITH RECURSIVE variable_path_abc123 AS (
    SELECT 
        start_node.user_id AS start_id,
        end_node.user_id AS end_id,
        1 AS hop_count
    FROM users AS start_node
    JOIN interactions AS rel                    -- ❌ Missing type filter!
        ON rel.from_id = start_node.user_id
    JOIN users AS end_node
        ON rel.to_id = end_node.user_id
    -- ❌ Missing: WHERE interaction_type = 'FOLLOWS'
    -- ❌ Missing: AND from_type = 'User'
    -- ❌ Missing: AND to_type = 'User'
    
    UNION ALL
    
    SELECT 
        prev.start_id,
        end_node.user_id AS end_id,
        prev.hop_count + 1
    FROM variable_path_abc123 AS prev
    JOIN interactions AS rel                    -- ❌ Missing type filter!
        ON rel.from_id = prev.end_id
    JOIN users AS end_node
        ON rel.to_id = end_node.user_id
    WHERE prev.hop_count < 3
    -- ❌ Missing: AND interaction_type = 'FOLLOWS'
    -- ❌ Missing: AND from_type = 'User'
    -- ❌ Missing: AND to_type = 'User'
)
```

**What Happens**: CTE returns ALL interaction types (FOLLOWS, LIKES, AUTHORED, etc.), not just FOLLOWS paths!

### What Would Be Needed

**Corrected CTE**:
```sql
WITH RECURSIVE variable_path_abc123 AS (
    SELECT 
        start_node.user_id AS start_id,
        end_node.user_id AS end_id,
        1 AS hop_count
    FROM users AS start_node
    JOIN interactions AS rel
        ON rel.from_id = start_node.user_id
        AND rel.interaction_type = 'FOLLOWS'     -- ✅ Type filter
        AND rel.from_type = 'User'                -- ✅ From node type
        AND rel.to_type = 'User'                  -- ✅ To node type
    JOIN users AS end_node
        ON rel.to_id = end_node.user_id
    
    UNION ALL
    
    SELECT 
        prev.start_id,
        end_node.user_id AS end_id,
        prev.hop_count + 1
    FROM variable_path_abc123 AS prev
    JOIN interactions AS rel
        ON rel.from_id = prev.end_id
        AND rel.interaction_type = 'FOLLOWS'     -- ✅ Type filter
        AND rel.from_type = 'User'                -- ✅ From node type
        AND rel.to_type = 'User'                  -- ✅ To node type
    JOIN users AS end_node
        ON rel.to_id = end_node.user_id
    WHERE prev.hop_count < 3
)
```

**Changes Required in `VariableLengthCteGenerator`**:
1. Detect polymorphic pattern (check `RelationshipSchema.type_column`)
2. Add type filters to JOIN conditions (both base and recursive cases)
3. Include `interaction_type`, `from_type`, `to_type` filters
4. Handle multiple relationship types (e.g., `[:FOLLOWS|LIKES*1..3]`) with UNION

**Estimated Effort**: 1-2 days

---

## Implementation Roadmap

### Phase 1: Current Implementation (Graph→SQL Boundary) ✅
**Focus**: Simple queries (no variable-length paths)
**Duration**: 3-4 days
**Patterns**:
- ✅ Standard: All queries work
- ✅ Denormalized: Simple queries (e.g., `MATCH (a)-[f]->(b)`)
- ✅ Polymorphic: Simple queries with type filters

### Phase 2: Recursive CTE - Denormalized Support 🔮
**Focus**: Variable-length paths with denormalized nodes
**Duration**: 1-2 days
**Changes**:
- Detect denormalized pattern in CTE generator
- Use single edge table for path traversal
- Join on `prev.end_id = next_edge.from_column`
- Property selection from edge table with position awareness

**Example Test**:
```cypher
MATCH (a:Airport)-[:FLIGHT*1..2]->(b:Airport)
WHERE a.city = 'Los Angeles'
RETURN b.city
```

### Phase 3: Recursive CTE - Polymorphic Support 🔮
**Focus**: Variable-length paths with polymorphic edges
**Duration**: 1-2 days
**Changes**:
- Detect polymorphic pattern in CTE generator
- Add type filters to JOINs (base + recursive)
- Handle multiple types with UNION (e.g., `[:FOLLOWS|LIKES*]`)

**Example Test**:
```cypher
MATCH (u1:User)-[:FOLLOWS*1..3]->(u2:User)
RETURN u2.name
```

### Phase 4: Mixed Pattern CTEs 🔮
**Focus**: Variable-length paths mixing patterns
**Duration**: 2-3 days
**Example**:
```cypher
MATCH (u:User)-[:PURCHASED]->(p:Product)
MATCH (a:Airport)-[:FLIGHT*1..2]->(b:Airport)
WHERE u.name = 'Alice' AND a.city = 'Los Angeles'
RETURN p.name, b.city
```

---

## Recommendations

### For Current Implementation

**Do**:
- ✅ Focus on simple queries (single-hop, no `*`)
- ✅ Get denormalized and polymorphic patterns working for basic queries
- ✅ Validate standard pattern still works (regression)
- ✅ Document CTE limitations clearly

**Don't**:
- ❌ Don't try to fix recursive CTEs now (scope creep)
- ❌ Don't test variable-length with new patterns (known to fail)
- ❌ Don't block on CTE work (orthogonal problem)

### Testing Strategy

**Include in Tests**:
```cypher
-- ✅ Test these (simple queries)
MATCH (a:Airport)-[f:FLIGHT]->(b:Airport) WHERE a.city = 'LAX' RETURN b.city
MATCH (u:User)-[i:FOLLOWS]->(u2:User) RETURN u2.name
```

**Exclude from Tests**:
```cypher
-- ⚠️ Skip these (CTE limitations)
MATCH (a:Airport)-[:FLIGHT*1..2]->(b:Airport) RETURN b.city
MATCH (u:User)-[:FOLLOWS*1..3]->(u2:User) RETURN u2.name
```

**Document as Known Limitations**:
```markdown
## Known Limitations

- Variable-length paths (`*`, `*1..3`) only work with standard schema pattern
- Denormalized nodes in recursive CTEs: NOT SUPPORTED (future work)
- Polymorphic edges in recursive CTEs: NOT SUPPORTED (future work)
```

---

## Code Locations

**CTE Generator**: `src/clickhouse_query_generator/variable_length_cte.rs`
- `VariableLengthCteGenerator::new()` - Constructor with table info
- `generate_base_case()` - Base case SQL generation
- `generate_recursive_case()` - Recursive case SQL generation

**To Add Pattern Detection**:
```rust
impl VariableLengthCteGenerator {
    fn detect_schema_pattern(&self) -> SchemaPattern {
        // Check ViewScan metadata
        // - is_denormalized: bool
        // - is_polymorphic: bool
        // - type_column: Option<String>
    }
    
    fn generate_denormalized_cte(&self) -> String {
        // Single table approach
        // Join on prev.end_id = next.from_column
    }
    
    fn generate_polymorphic_cte(&self) -> String {
        // Add type filters to JOINs
        // WHERE interaction_type = '...'
    }
}
```

---

## Summary

**Current State**:
- ✅ Recursive CTEs work perfectly with standard schema pattern
- ❌ Recursive CTEs fail with denormalized pattern (no node tables)
- ❌ Recursive CTEs don't filter correctly with polymorphic pattern (missing WHERE clauses)

**Near-Term Goal** (this implementation):
- Focus on simple queries (no `*` variable-length)
- Get denormalized and polymorphic working for single-hop patterns
- Document CTE limitations clearly

**Future Work**:
- Extend `VariableLengthCteGenerator` for denormalized pattern (~1-2 days)
- Extend `VariableLengthCteGenerator` for polymorphic pattern (~1-2 days)
- Test and validate all pattern combinations (~1 day)

**Realistic**: Acknowledge CTE limitations, don't block current work, plan future phases!
