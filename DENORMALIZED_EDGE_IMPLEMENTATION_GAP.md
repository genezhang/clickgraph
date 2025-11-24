# Denormalized Edge Table Implementation Gap Analysis

**Date**: November 23, 2025  
**Critical Finding**: Only property mapping implemented, NO query planner/SQL generator support

---

## Executive Summary

**The denormalized edge table feature is NOT implemented for query translation.**

- ✅ **Schema Loading**: Works (`from_node_properties`/`to_node_properties` in YAML)
- ✅ **Property Mapping**: Works (unit tests pass - `map_property_to_column_with_relationship_context`)
- ❌ **Query Planning**: NO special handling for same-table pattern
- ❌ **JOIN Generation**: Always creates JOINs, even when unnecessary
- ❌ **SQL Generation**: Produces invalid SQL (references non-existent columns)

---

## What IS Implemented

### 1. Schema Definition (✅ Complete)
**File**: `src/graph_catalog/graph_schema.rs`

```yaml
# YAML schema supports denormalized pattern
nodes:
  Airport:
    table: flights  # Same table as edge
    property_mappings: {}  # Empty - derived from edge
    id_column: code  # Logical property name

relationships:
  FLIGHT:
    table: flights  # Same table as node
    from_node_properties:  # Denormalized origin properties
      code: origin_code
      city: origin_city
    to_node_properties:  # Denormalized dest properties
      code: dest_code
      city: dest_city
```

**Status**: ✅ Schema loads correctly, fields available

### 2. Property Mapping Function (✅ Complete)
**File**: `src/render_plan/cte_generation.rs:518`

```rust
pub(crate) fn map_property_to_column_with_relationship_context(
    property: &str,
    node_label: &str,
    relationship_type: Option<&str>,
) -> Result<String, String> {
    // This function DOES check from_node_properties/to_node_properties
    // and correctly maps logical properties to physical columns
}
```

**What it does**: Maps `Airport.city` → `origin_city` or `dest_city` based on context  
**Status**: ✅ Works (7/7 unit tests pass)  
**Limitation**: Only called during CTE property selection for variable-length paths!

---

## What IS NOT Implemented

### 1. Query Planner Detection (❌ Missing)
**File**: `src/query_planner/analyzer/graph_join_inference.rs`

**Problem**: The query planner has **ZERO code** to detect denormalized edge tables.

**Evidence**:
```bash
# Search results:
grep -r "denormalized|same.?table|from_node_properties" src/query_planner/
# → NO MATCHES except test setup (where they set None)
```

**What's missing**:
- No check if `node.table_name == relationship.table_name`
- No detection of denormalized pattern in `handle_graph_pattern()`
- No optimization path for same-table traversals

### 2. JOIN Optimization (❌ Missing)
**File**: `src/query_planner/analyzer/graph_join_inference.rs:926-1750`

**Current behavior** (lines 926-1750):
```rust
fn handle_graph_pattern(...) {
    // Always assumes: Node table ≠ Edge table
    
    // STEP 1: Create JOIN for relationship table
    let rel_graph_join = Join {
        table_name: rel_cte_name,  // e.g., "flights"
        table_alias: rel_alias.to_string(),  // e.g., "f1"
        joining_on: vec![...],  // f1.origin_code = a.code
    };
    
    // STEP 2: Create JOIN for left node table  
    let left_graph_join = Join {
        table_name: left_cte_name,  // e.g., "airports" ❌ DOESN'T EXIST!
        table_alias: left_alias.to_string(),  // e.g., "a"
        joining_on: vec![...],  // a.code = f1.origin_code
    };
    
    // STEP 3: Create JOIN for right node table
    let right_graph_join = Join {
        table_name: right_cte_name,  // e.g., "airports" ❌ DOESN'T EXIST!
        table_alias: right_alias.to_string(),  // e.g., "b"
        joining_on: vec![...],  // b.code = f1.dest_code  
    };
    
    // NO CODE PATH TO SKIP JOINS WHEN table_name IS SAME!
}
```

**Result**: Generates invalid SQL:
```sql
-- Generated SQL (WRONG):
SELECT ...
FROM flights AS f1
INNER JOIN airports AS a ON a.code = f1.origin_code  -- ❌ airports doesn't exist!
INNER JOIN airports AS b ON b.code = f1.dest_code    -- ❌ airports doesn't exist!
WHERE ...
```

**Expected SQL** (denormalized pattern):
```sql
-- For: MATCH (a)-[r]->(b) WHERE r.flight_num = 'UA123'
SELECT 
    f1.origin_code AS a_code,
    f1.origin_city AS a_city,
    f1.flight_number AS r_flight_num,
    f1.dest_code AS b_code,
    f1.dest_city AS b_city
FROM flights AS f1  -- NO JOINS!
WHERE f1.flight_number = 'UA123'
```

### 3. SQL Generator (❌ Broken)
**File**: `src/clickhouse_query_generator/to_sql_query.rs`

**Problem**: SQL generator blindly follows JOIN plan without validation.

**No checks for**:
- Table existence
- Column existence  
- Same-table optimization

**Result**: Produces syntactically valid but semantically broken SQL.

---

## Why Unit Tests Pass but Integration Tests Fail

### Unit Tests (7/7 ✅)
**What they test**: Property mapping function only
```rust
// Unit test scope:
map_property_to_column_with_relationship_context("city", "Airport", Some("FLIGHT"))
// → Returns "origin_city" ✅
```

**What they DON'T test**:
- Query planning
- JOIN generation  
- Full SQL generation
- End-to-end query execution

### Integration Tests (3/18 ❌)
**What they test**: Full query pipeline

```python
# Integration test flow:
Cypher Query 
  → Parser ✅ 
  → Query Planner ❌ (creates invalid JOINs)
  → SQL Generator ❌ (produces broken SQL)
  → ClickHouse Execution ❌ (fails - table doesn't exist)
```

**Why they fail**:
1. Query planner generates JOINs to non-existent `airports` table
2. SQL references columns like `origin.code` (should be `f1.origin_code`)
3. ClickHouse returns error: "Table airports doesn't exist"

---

## Implementation Requirements

To make denormalized edge tables work, we need:

### Phase 1: Detection Logic (Query Planner)
**File**: `src/query_planner/analyzer/graph_join_inference.rs`

Add detection function:
```rust
fn is_denormalized_edge_table(
    left_schema: &NodeSchema,
    rel_schema: &RelationshipSchema,
    right_schema: &NodeSchema,
) -> bool {
    // Check if all three use the same physical table
    left_schema.table_name == rel_schema.table_name
        && right_schema.table_name == rel_schema.table_name
        && rel_schema.from_node_properties.is_some()
        && rel_schema.to_node_properties.is_some()
}
```

### Phase 2: JOIN Optimization (Query Planner)
**File**: `src/query_planner/analyzer/graph_join_inference.rs:926`

Modify `handle_graph_pattern()`:
```rust
fn handle_graph_pattern(...) -> AnalyzerResult<()> {
    // NEW: Check for denormalized pattern
    if is_denormalized_edge_table(&left_schema, &rel_schema, &right_schema) {
        // Denormalized path: NO node JOINs needed!
        // Only scan the relationship table (which contains all data)
        
        let rel_join = Join {
            table_name: rel_cte_name,  // flights
            table_alias: rel_alias.to_string(),  // f1
            joining_on: vec![],  // No JOIN condition - it's the base table!
        };
        
        collected_graph_joins.push(rel_join);
        joined_entities.insert(rel_alias.to_string());
        joined_entities.insert(left_alias.to_string());  // Mark as "joined"
        joined_entities.insert(right_alias.to_string());  // Mark as "joined"
        
        return Ok(());  // Early return - skip node JOINs
    }
    
    // Existing code for traditional pattern (separate tables)
    ...
}
```

### Phase 3: Property Selection (Already Works!)
**File**: `src/render_plan/cte_generation.rs:518`

**Status**: ✅ Already implemented!  
The property mapping function correctly uses `from_node_properties`/`to_node_properties`.

### Phase 4: Multi-hop Traversals (Future)
For patterns like `MATCH (a)-[r1]->(b)-[r2]->(c)`:

```rust
// Challenge: Same table appears multiple times
// Solution: Use table aliases correctly

// For: MATCH (a)-[r1:FLIGHT]->(b)-[r2:FLIGHT]->(c)
// Expected SQL:
SELECT 
    f1.origin_code AS a_code,
    f1.dest_code AS b_code,
    f2.dest_code AS c_code
FROM flights AS f1  -- First hop
INNER JOIN flights AS f2 ON f2.origin_code = f1.dest_code  -- Second hop
```

**Complexity**: Medium - need correct alias tracking.

### Phase 5: Variable-Length Paths (Future)
For patterns like `MATCH (a)-[*]->(b)`:

```rust
// Challenge: Recursive CTEs with denormalized properties
// Solution: CTE uses single table with proper column selection

WITH RECURSIVE path AS (
    -- Base case: Direct flights
    SELECT 
        origin_code AS start_node,
        dest_code AS end_node,
        1 AS depth,
        [origin_code, dest_code] AS path
    FROM flights
    WHERE origin_code = 'SEA'
    
    UNION ALL
    
    -- Recursive case: Add one more hop
    SELECT 
        p.start_node,
        f.dest_code AS end_node,
        p.depth + 1,
        arrayConcat(p.path, [f.dest_code])
    FROM path AS p
    INNER JOIN flights AS f ON f.origin_code = p.end_node
    WHERE p.depth < 10
)
SELECT * FROM path WHERE end_node = 'NYC'
```

**Complexity**: High - requires variable-length CTE generator changes.

---

## Test Coverage Status

### Property Mapping (CTE Generation)
- ✅ **7/7 unit tests pass** - `map_property_to_column_with_relationship_context()` works
- ✅ Test coverage: from_node_properties, to_node_properties, fallback logic

### Query Planning (JOIN Generation)
- ❌ **0 tests** - No tests for denormalized pattern in `graph_join_inference.rs`
- ❌ **0 code** - No implementation for same-table detection
- ❌ **0 optimization** - Always generates JOINs

### SQL Generation (End-to-End)
- ❌ **3/18 integration tests pass** (16.7%)
- ❌ **15 tests blocked** by invalid JOIN generation
- ❌ **SQL validation** - No checks for table/column existence

---

## Recommended Implementation Order

### ⚠️ HIGH PRIORITY (Blocks all testing)

**1. Add Same-Table Detection** (2-3 hours)
- File: `src/query_planner/analyzer/graph_join_inference.rs`
- Add: `is_denormalized_edge_table()` function
- Test: Unit test for detection logic

**2. Implement Single-Edge Optimization** (3-4 hours)
- File: `src/query_planner/analyzer/graph_join_inference.rs:926`
- Modify: `handle_graph_pattern()` - skip node JOINs for same-table
- Test: Unit test + integration test for `MATCH (a)-[r]->(b)`

**3. Fix Property Selection** (1 hour)
- Verify: Property mapping already works
- Test: Add integration test for property access

### 🟡 MEDIUM PRIORITY (Expand coverage)

**4. Multi-hop Traversals** (4-5 hours)
- Pattern: `MATCH (a)-[r1]->(b)-[r2]->(c)`
- Challenge: Correct table aliasing
- Test: Integration tests for 2-hop, 3-hop

**5. Filter Optimization** (2-3 hours)
- Pattern: `WHERE a.city = 'Seattle'`
- Challenge: Map filter to correct table/column
- Test: Integration tests for filtered queries

### 🟢 LOW PRIORITY (Advanced features)

**6. Variable-Length Paths** (6-8 hours)
- Pattern: `MATCH (a)-[*]->(b)`
- Challenge: Recursive CTE generation
- Test: Integration tests for var-length

**7. Aggregations** (2-3 hours)
- Pattern: `RETURN count(r), a.city`
- Challenge: GROUP BY with denormalized columns
- Test: Integration tests for aggregations

---

## Conclusion

**Current State**: Only property mapping implemented (narrow use case: variable-length path CTEs).

**Required Work**: Full query planner rewrite for denormalized pattern detection and JOIN optimization.

**Estimated Effort**: 
- Minimum viable (single edge): ~6-8 hours
- Complete feature (multi-hop, var-length): ~20-25 hours

**Impact**: High - this is a fundamental query translation pattern, not a minor optimization.

**Recommendation**: Treat this as a NEW FEATURE implementation, not a bug fix. The property mapping was just groundwork; the real work (query planning) hasn't started yet.
