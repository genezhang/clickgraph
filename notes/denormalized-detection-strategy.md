# Denormalized Edge Table Detection Strategy

**Date**: November 23, 2025

## Detection Logic

### Key Insight
A node uses a denormalized edge table when:
1. ✅ `node.table_name == edge.table_name` (same physical table)
2. ✅ `node.property_mappings` is empty or minimal (properties come from edge)
3. ✅ `edge.from_node_properties` or `edge.to_node_properties` is NOT None (denormalized props exist)
4. ✅ Node's `id_column` is a **logical property name** that maps through edge properties

### Detection Function

```rust
/// Detect if a node is using denormalized edge table pattern
pub fn is_node_denormalized_on_edge(
    node: &NodeSchema,
    edge: &RelationshipSchema,
    is_from_node: bool,
) -> bool {
    // Must use same physical table
    if node.table_name != edge.table_name {
        return false;
    }
    
    // Edge must have denormalized properties for this direction
    let has_denormalized_props = if is_from_node {
        edge.from_node_properties.is_some() && !edge.from_node_properties.as_ref().unwrap().is_empty()
    } else {
        edge.to_node_properties.is_some() && !edge.to_node_properties.as_ref().unwrap().is_empty()
    };
    
    if !has_denormalized_props {
        return false;
    }
    
    // Node should have empty or minimal property_mappings
    // (properties are defined in edge's from/to_node_properties instead)
    let node_has_minimal_props = node.property_mappings.is_empty() 
        || node.property_mappings.len() <= 2; // Allow a few direct mappings
    
    node_has_minimal_props
}

/// Check if BOTH nodes in a relationship use denormalized pattern
pub fn is_fully_denormalized_edge_table(
    left_node: &NodeSchema,
    edge: &RelationshipSchema,
    right_node: &NodeSchema,
) -> bool {
    is_node_denormalized_on_edge(left_node, edge, true)
        && is_node_denormalized_on_edge(right_node, edge, false)
}

/// Check if edge table pattern is mixed (one denormalized, one traditional)
pub enum EdgeTablePattern {
    /// Traditional: Both nodes have separate tables
    Traditional,
    /// Fully denormalized: Both nodes use edge table
    FullyDenormalized,
    /// Mixed: One node uses edge table, other has separate table
    Mixed {
        from_denormalized: bool,
        to_denormalized: bool,
    },
}

pub fn classify_edge_table_pattern(
    left_node: &NodeSchema,
    edge: &RelationshipSchema,
    right_node: &NodeSchema,
) -> EdgeTablePattern {
    let from_denorm = is_node_denormalized_on_edge(left_node, edge, true);
    let to_denorm = is_node_denormalized_on_edge(right_node, edge, false);
    
    match (from_denorm, to_denorm) {
        (true, true) => EdgeTablePattern::FullyDenormalized,
        (false, false) => EdgeTablePattern::Traditional,
        (from_d, to_d) => EdgeTablePattern::Mixed {
            from_denormalized: from_d,
            to_denormalized: to_d,
        },
    }
}
```

---

## Schema Patterns

### Pattern 1: Fully Denormalized (Flights Example)
```yaml
nodes:
  - label: Airport
    table: flights  # ✅ Same as edge
    node_id: code
    property_mappings: {}  # ✅ Empty

edges:
  - type: FLIGHT
    table: flights  # ✅ Same as node
    from_node: Airport
    to_node: Airport
    from_node_properties:  # ✅ Denormalized
      code: origin_code
      city: origin_city
    to_node_properties:  # ✅ Denormalized
      code: dest_code
      city: dest_city
```

**Detection**: `is_fully_denormalized_edge_table() == true`

### Pattern 2: Traditional (Separate Tables)
```yaml
nodes:
  - label: Airport
    table: airports  # ✅ Different table
    node_id: code
    property_mappings:
      code: airport_code
      city: city_name

edges:
  - type: FLIGHT
    table: flights
    from_node: Airport
    to_node: Airport
    from_node_properties: null  # ✅ None
    to_node_properties: null
```

**Detection**: `classify_edge_table_pattern() == Traditional`

### Pattern 3: Mixed (One Denormalized, One Traditional)
```yaml
nodes:
  - label: Airport
    table: flights  # ✅ Denormalized (same as edge)
    node_id: code
    property_mappings: {}
    
  - label: User
    table: users  # ✅ Traditional (separate table)
    node_id: user_id
    property_mappings:
      user_id: id
      name: full_name

edges:
  - type: BOOKED_BY
    table: flights
    from_node: Airport
    to_node: User
    from_node_properties:  # ✅ Airport denormalized
      code: origin_code
      city: origin_city
    to_node_properties: null  # ✅ User traditional
```

**Detection**: `classify_edge_table_pattern() == Mixed { from_denormalized: true, to_denormalized: false }`

---

## Query Planning Strategy

### Fully Denormalized (No JOINs)
```cypher
MATCH (a:Airport)-[r:FLIGHT]->(b:Airport)
RETURN a.code, r.flight_num, b.code
```

**SQL**:
```sql
SELECT 
    f.origin_code AS a_code,
    f.flight_number AS r_flight_num,
    f.dest_code AS b_code
FROM flights AS f  -- NO JOINS!
```

### Traditional (2 JOINs)
```cypher
MATCH (a:Airport)-[r:FLIGHT]->(b:Airport)
RETURN a.code, r.flight_num, b.code
```

**SQL**:
```sql
SELECT 
    a.airport_code AS a_code,
    f.flight_number AS r_flight_num,
    b.airport_code AS b_code
FROM flights AS f
INNER JOIN airports AS a ON a.airport_code = f.origin_code
INNER JOIN airports AS b ON b.airport_code = f.dest_code
```

### Mixed (1 JOIN)
```cypher
MATCH (a:Airport)-[r:BOOKED_BY]->(u:User)
RETURN a.code, u.name
```

**SQL**:
```sql
SELECT 
    f.origin_code AS a_code,
    u.full_name AS u_name
FROM flights AS f
INNER JOIN users AS u ON u.id = f.user_id
-- No JOIN for Airport - it's denormalized in flights!
```

---

## Implementation Checklist

### Phase 1: Detection Functions
- [ ] Add `is_node_denormalized_on_edge()` to `graph_schema.rs`
- [ ] Add `is_fully_denormalized_edge_table()` helper
- [ ] Add `classify_edge_table_pattern()` enum + function
- [ ] Unit tests for all 3 patterns

### Phase 2: Query Planner Integration
- [ ] Update `graph_join_inference.rs:handle_graph_pattern()`
- [ ] Check pattern before generating JOINs
- [ ] Skip node JOINs for denormalized nodes
- [ ] Preserve JOINs for traditional nodes in mixed pattern

### Phase 3: Property Mapping Updates
- [ ] Verify `map_property_to_column_with_relationship_context()` handles all patterns
- [ ] Add tests for mixed pattern property mapping

### Phase 4: Multi-hop Patterns
- [ ] Handle `MATCH (a)-[r1]->(b)-[r2]->(c)` with denormalized
- [ ] Correct table aliasing when same table appears multiple times
- [ ] Test all combinations (denorm→denorm, denorm→trad, trad→denorm)

---

## Test Coverage Requirements

### Detection Unit Tests (8 tests minimum)
1. ✅ Fully denormalized (Airport on flights)
2. ✅ Traditional (Airport on airports)
3. ✅ Mixed - from denormalized, to traditional
4. ✅ Mixed - from traditional, to denormalized
5. ✅ Edge case: empty property_mappings
6. ✅ Edge case: minimal property_mappings (1-2 props)
7. ✅ Edge case: node uses edge table but no from/to_node_properties
8. ✅ Edge case: different databases but same table name

### Query Planning Unit Tests (6 tests minimum)
1. ✅ Single edge - fully denormalized (no JOINs)
2. ✅ Single edge - traditional (2 JOINs)
3. ✅ Single edge - mixed (1 JOIN)
4. ✅ Multi-hop - fully denormalized
5. ✅ Multi-hop - mixed patterns
6. ✅ Variable-length - denormalized

### Integration Tests (existing 18)
- Already created, will pass once query planner is fixed

---

## Notes

**Why empty `property_mappings` is the key signal**:
- Traditional pattern: Node has its own table → properties defined in `property_mappings`
- Denormalized pattern: Node uses edge table → properties defined in `from_node_properties`/`to_node_properties`
- Empty `property_mappings` + denormalized props = denormalized node

**Mixed pattern is important**:
- Real-world schemas often mix patterns (e.g., flights denormalized, users separate)
- Must handle correctly: JOIN traditional nodes, skip denormalized nodes

**Database prefix matters**:
- `node.database + "." + node.table_name` must match `edge.database + "." + edge.table_name`
- Same table name in different databases ≠ denormalized pattern
