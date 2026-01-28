# Denormalized Edge Table Pattern Coverage

**Date**: December 2024  
**Feature**: Denormalized edge table (same table for nodes and edges)  
**Status**: Property mapping ✅ Works, SQL generation ❌ Broken (creates unnecessary JOINs)

## Purpose
Assess which query patterns are relevant for denormalized edge table testing and determine current unit test coverage gaps before fixing the SQL generation bug.

---

## Coverage Analysis

### Pattern Relevance for Denormalized Edge Tables

**Highly Relevant** (must test with denormalized pattern):
- ✅ Relationship Patterns (15 patterns)
- ✅ Variable-Length Paths (18 patterns)  
- ✅ Multi-hop traversals
- ✅ Aggregations on relationships
- ⚠️ Shortest Path (8 patterns) - partially relevant

**Moderately Relevant** (may reveal optimization issues):
- 🟡 Path Variable Functions (6 patterns)
- 🟡 OPTIONAL MATCH with relationships (9 patterns)
- 🟡 Multiple MATCH with relationships (8 patterns)

**Not Relevant** (node-only patterns):
- ⬜ Basic Node Patterns (19) - no relationship traversal
- ⬜ Pure aggregations on nodes (subset of 20)
- ⬜ WITH clause without relationships (subset of 12)

---

## Current Unit Test Coverage

### ✅ Tested Patterns (7 unit tests)

**Property Mapping Level** - All in `src/render_plan/tests/denormalized_property_tests.rs`:

1. **`test_denormalized_from_node_property()`**
   - Pattern: Access origin node property in relationship context
   - Schema: Traditional (separate airports + flights tables)
   - Validates: `map_property_to_column_with_relationship_context("city", "Airport", Some("FLIGHT"))` → `"origin_city"`

2. **`test_denormalized_to_node_property()`**
   - Pattern: Access destination node property in relationship context
   - Schema: Traditional (separate tables)
   - Validates: `map_property_to_column_with_relationship_context("city", "Airport", Some("FLIGHT"))` → `"dest_city"`

3. **`test_fallback_to_node_property()`**
   - Pattern: Fall back to node property when not in relationship context
   - Schema: Traditional (separate tables)
   - Validates: Property resolution without relationship context

4. **`test_no_relationship_context()`**
   - Pattern: Node property access without relationship
   - Schema: Traditional (separate tables)
   - Validates: Direct node property mapping

5. **`test_relationship_property()`**
   - Pattern: Access relationship-only property
   - Schema: Traditional (separate tables)
   - Validates: Edge property mapping (e.g., flight_num → flight_number)

6. **`test_multiple_relationships_same_node()`**
   - Pattern: Same property in different relationship types
   - Schema: Traditional (separate tables)
   - Validates: Relationship-specific property resolution

7. **`test_denormalized_edge_table_same_table_for_node_and_edge()`** ⭐
   - Pattern: **SAME TABLE for node and edge** (true denormalized pattern)
   - Schema: Airport + FLIGHT both on `flights` table
   - Validates:
     - `city` → `origin_city` (denormalized property)
     - `code` → `origin_code` (node ID through from_node_properties)
   - **This is the only test for the actual denormalized pattern!**

---

## Integration Test Coverage (18 tests)

**File**: `tests/integration/test_denormalized_edges.py`  
**Status**: 3/18 passing (16.7%) - blocked by SQL generation bug

### Property Access Patterns (5 tests)
1. ✅ `test_simple_flight_query` - Basic edge traversal
2. ✅ `test_denormalized_origin_properties` - Origin node properties
3. ✅ `test_denormalized_dest_properties` - Destination node properties
4. ❌ `test_both_origin_and_dest_properties` - Both in same query
5. ❌ `test_return_all_denormalized_properties` - All properties at once

### Filter Patterns (3 tests)
6. ❌ `test_filter_on_origin_city` - WHERE on denormalized property
7. ❌ `test_filter_on_dest_state` - WHERE on destination property
8. ❌ `test_complex_filter_denormalized_and_edge_props` - Combined filters

### Variable-Length Path Patterns (2 tests)
9. ❌ `test_variable_path_with_denormalized_properties` - `[*]` with properties
10. ❌ `test_variable_path_cte_uses_denormalized_props` - CTE property selection

### Performance/Optimization Patterns (2 tests)
11. ❌ `test_sql_has_no_joins` - **Critical**: Validates no unnecessary JOINs
12. ❌ `test_single_hop_no_joins` - Single hop optimization
13. ❌ `test_filtered_query_no_joins` - Filter + no JOIN optimization

### Edge Cases (2 tests)
14. ❌ `test_denormalized_property_exists_not_in_node_table` - Property only in edge
15. ❌ `test_property_in_both_from_and_to_nodes` - Ambiguous properties

### Composite ID Patterns (3 tests)
16. ❌ `test_composite_edge_id_in_schema` - Schema with composite IDs
17. ❌ `test_variable_path_with_composite_edge_id` - Var-path + composite
18. ❌ `test_composite_id_prevents_duplicate_edges` - Deduplication

---

## Gap Analysis

### Critical Missing Unit Tests

Based on the 139-pattern completeness analysis, these patterns should have unit tests for denormalized edge tables:

#### 1. Single Edge Pattern ⚠️ **MISSING**
```cypher
MATCH (a)-[r:FLIGHT]->(b)
RETURN a.code, r.flight_num, b.code
```
**Why Critical**: Most basic relationship query, must work with same table
**Current Coverage**: ❌ Only traditional pattern tested (separate tables)
**Needed**: Test with `flights` table for both Airport and FLIGHT

#### 2. Multi-hop Pattern ⚠️ **MISSING**
```cypher
MATCH (a)-[r1:FLIGHT]->(b)-[r2:FLIGHT]->(c)
RETURN a.code, b.code, c.code
```
**Why Critical**: Tests JOIN generation when same table used multiple times
**Current Coverage**: ❌ No multi-hop denormalized tests
**Needed**: Test with same table appearing as `flights AS t1`, `flights AS t2`

#### 3. Variable-Length Unbounded ⚠️ **MISSING**
```cypher
MATCH (a)-[*]->(b)
RETURN a.code, b.code
```
**Why Critical**: Recursive CTEs with same table
**Current Coverage**: ❌ No variable-length denormalized unit tests
**Needed**: Test CTE generation with denormalized properties

#### 4. Variable-Length Bounded ⚠️ **MISSING**
```cypher
MATCH (a)-[*1..3]->(b)
RETURN a.code, b.code
```
**Why Critical**: Bounded recursion with same table
**Current Coverage**: ❌ Not tested at unit level

#### 5. Aggregation on Relationship ⚠️ **MISSING**
```cypher
MATCH (a)-[r:FLIGHT]->(b)
RETURN a.code, count(r)
```
**Why Critical**: GROUP BY optimization with same table
**Current Coverage**: ❌ Not tested at unit level

#### 6. Filter on Denormalized Property ⚠️ **MISSING**
```cypher
MATCH (a:Airport)-[r:FLIGHT]->(b:Airport)
WHERE a.city = 'Seattle'
RETURN b.city
```
**Why Critical**: WHERE clause optimization with same table
**Current Coverage**: ❌ Not tested at unit level

#### 7. Shortest Path with Denormalized ⚠️ **MISSING**
```cypher
MATCH p = shortestPath((a)-[*]->(b))
WHERE a.code = 'SEA' AND b.code = 'NYC'
RETURN length(p)
```
**Why Critical**: Path algorithms with same table
**Current Coverage**: ❌ Not tested at unit level

---

## Recommendations

### Phase 1: Add Critical Unit Tests (Immediate)

**Priority Order**:
1. ✅ **Single edge pattern** - Foundation for all other patterns
2. ✅ **Filter on denormalized property** - Common use case
3. ✅ **Multi-hop pattern** - Tests table aliasing
4. 🟡 **Variable-length unbounded** - CTE generation
5. 🟡 **Aggregation on relationship** - GROUP BY optimization

### Phase 2: Fix SQL Generation Bug

Once unit tests are in place, fix the query planner to:
- Detect when node and edge share the same table
- Skip JOIN generation for single-table patterns
- Use table aliases correctly for multi-hop patterns
- Optimize WHERE clauses on denormalized properties

### Phase 3: Validate with Integration Tests

After SQL fix:
- Run all 18 integration tests (currently 3/18 passing)
- Verify `test_sql_has_no_joins` passes
- Confirm performance optimization tests pass

### Phase 4: Extended Pattern Testing (Optional)

Lower priority patterns:
- OPTIONAL MATCH with denormalized relationships
- WITH clause + denormalized properties
- Path variable functions with same table
- Shortest path variations

---

## Summary

**Current Coverage**: 1/7 unit tests use true denormalized pattern (14%)
**Gap**: 6+ critical query patterns not tested with same-table denormalized schema
**Blocker**: SQL generation creates unnecessary JOINs (all integration tests blocked)

**Action Items**:
1. Add 5-7 unit tests for critical patterns (single edge, multi-hop, var-length, aggregation, filter)
2. Fix SQL generation to detect same-table pattern
3. Validate with 18 integration tests
4. Document working patterns in STATUS.md

**Timeline Estimate**:
- Unit tests: 1-2 hours (5-7 new tests)
- SQL generation fix: 2-4 hours (query planner changes)
- Integration validation: 30 minutes (run + verify)
- Documentation: 30 minutes

**Total**: ~4-7 hours to complete denormalized edge table feature
