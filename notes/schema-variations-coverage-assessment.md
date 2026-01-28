# Schema Variations Coverage Assessment

*Created: November 27, 2025*  
*Features: Denormalized Property Access, Polymorphic Edge Type Filters, Composite ID Uniqueness*

## Executive Summary

Today's three schema variation features add **significant complexity** to query generation but currently have **limited cross-pattern testing**. Here's the breakdown:

| Feature | Unit Tests | Integration Tests | Query Pattern Coverage | Status |
|---------|------------|-------------------|------------------------|--------|
| **Denormalized Property Access** | ✅ 6/6 | ❌ 0 tests | ⚠️ **Limited** (15%) | **GAP** |
| **Polymorphic Edge Type Filters** | ⚠️ Minimal | ❌ 0 tests | ⚠️ **None** (0%) | **CRITICAL GAP** |
| **Composite ID Uniqueness** | ✅ 3/3 | ✅ 1 test | ⚠️ **Limited** (10%) | **GAP** |

**Overall Coverage**: **~8%** across 139 query patterns

---

## 1. Denormalized Property Access 🟡

### What It Does
Eliminates JOINs by accessing node properties directly from edge tables when denormalized.

```yaml
relationships:
  - type: FLIGHT
    from_node_properties:
      city: origin_city  # Direct access from flights table
    to_node_properties:
      city: dest_city
```

### Test Coverage Analysis

#### ✅ **Unit Tests** (6/6 passing)
**File**: `src/render_plan/tests/denormalized_property_tests.rs`

1. ✅ `test_denormalized_from_node_property` - Access origin properties
2. ✅ `test_denormalized_to_node_property` - Access destination properties  
3. ✅ `test_fallback_to_node_property` - Non-denormalized fallback
4. ✅ `test_no_relationship_context` - Works without relationship context
5. ✅ `test_relationship_property` - Rejects relationship-only properties
6. ✅ `test_multiple_relationships_same_node` - Multiple edge types

**Coverage**: Property mapping logic only (isolated unit tests)

#### ❌ **Integration Tests** (0 tests)
**Missing**: No end-to-end tests with real queries

#### ⚠️ **Query Pattern Coverage** (~15% of 139 patterns)

| Pattern Category | Coverage | Notes |
|-----------------|----------|-------|
| **Basic Node Patterns** | ❌ 0/19 | Not tested with denormalized schemas |
| **Aggregations** | ❌ 0/20 | COUNT/SUM on denormalized properties untested |
| **Sorting & Pagination** | ❌ 0/12 | ORDER BY on denormalized properties untested |
| **Relationships** | ⚠️ 2/15 | Only unit tests, no integration |
| **Variable-Length Paths** | ⚠️ 6/18 | Unit tests only (no real queries) |
| **Shortest Path** | ❌ 0/8 | Denormalized + shortest path untested |
| **Path Functions** | ❌ 0/6 | `nodes(p)` with denormalized props untested |
| **OPTIONAL MATCH** | ❌ 0/9 | LEFT JOIN + denormalized untested |
| **WITH Clause** | ❌ 0/12 | WITH + denormalized untested |
| **Multiple MATCH** | ❌ 0/8 | Multiple MATCH + denormalized untested |
| **Advanced Combos** | ❌ 0/12 | Complex queries untested |

**Tested Patterns** (20/139):
```cypher
# ✅ Unit tested (isolated)
MATCH (a:Airport)-[f:FLIGHT]->(b:Airport) 
RETURN a.city  # Denormalized property access

# ❌ NOT tested with denormalized schemas:
MATCH (a)-[f:FLIGHT*1..3]->(b) RETURN a.city        # Variable-length
MATCH (a)-[f:FLIGHT]->(b) WHERE a.city = 'NYC'      # WHERE clause
MATCH (a)-[f:FLIGHT]->(b) RETURN count(a.city)      # Aggregation
OPTIONAL MATCH (a)-[f:FLIGHT]->(b) RETURN a.city    # OPTIONAL MATCH
WITH ... denormalized properties                     # WITH clause
shortestPath((a)-[f:FLIGHT*]->(b)) RETURN a.city    # Shortest path
```

### 🔴 **Critical Gaps**

1. **No Integration Tests**: All tests are isolated unit tests
2. **No WHERE Clause Tests**: Filtering on denormalized properties untested
3. **No Aggregation Tests**: COUNT/SUM/AVG on denormalized properties
4. **No Complex Pattern Tests**: Variable-length paths + denormalized
5. **No Performance Validation**: 10-100x speedup claim unverified
6. **No OnTime Schema Test**: Original motivation (OnTime flights) not tested

---

## 2. Polymorphic Edge Type Filters 🔴

### What It Does
Single table with multiple edge types, discovered via type_column filtering.

```yaml
edges:
  - polymorphic: true
    table: interactions
    type_column: interaction_type
    from_label_column: from_type
    to_label_column: to_type
    type_values: [FOLLOWS, LIKES, AUTHORED]
```

### Test Coverage Analysis

#### ⚠️ **Unit Tests** (Minimal)
**Status**: Schema parsing tested, but **NO SQL generation tests**

#### ❌ **Integration Tests** (0 tests)
**Missing**: No end-to-end tests with real queries

#### 🔴 **Query Pattern Coverage** (0% of 139 patterns)

| Pattern Category | Coverage | Notes |
|-----------------|----------|-------|
| **All Categories** | ❌ 0/139 | **ZERO testing** across all query patterns |

**Untested Patterns** (139/139):
```cypher
# ❌ NONE of these tested with polymorphic edges:
MATCH (a)-[r:FOLLOWS]->(b)                          # Basic polymorphic edge
MATCH (a)-[r:FOLLOWS|LIKES]->(b)                    # Multi-type polymorphic
MATCH (a)-[r:FOLLOWS*1..3]->(b)                     # Variable-length polymorphic
MATCH (a)-[r:FOLLOWS]->(b) WHERE r.timestamp > x    # WHERE on polymorphic
MATCH (a)-[r:FOLLOWS]->(b) RETURN count(r)          # Aggregate polymorphic
shortestPath((a)-[r:FOLLOWS*]->(b))                 # Shortest path polymorphic
OPTIONAL MATCH (a)-[r:FOLLOWS]->(b)                 # OPTIONAL MATCH polymorphic
WITH polymorphic edges                               # WITH clause
Multiple MATCH with polymorphic                      # Multiple MATCH
... (130+ more untested patterns)
```

### 🔴 **Critical Gaps**

1. **NO SQL Generation Tests**: Schema parsing exists, but SQL generation completely untested
2. **NO Type Filtering Tests**: `WHERE interaction_type = 'FOLLOWS'` untested
3. **NO Label Matching Tests**: `WHERE from_type = 'User' AND to_type = 'Post'` untested
4. **NO Integration Tests**: Never tested end-to-end with real queries
5. **NO Performance Tests**: No validation of query performance
6. **Schema File Exists But Unused**: `schemas/examples/social_polymorphic.yaml` created but no tests use it

**Risk Level**: **CRITICAL** - Feature implemented but completely untested in practice

---

## 3. Composite ID Uniqueness (Edge Tracking) 🟡

### What It Does
Supports composite edge IDs for uniqueness tracking in variable-length paths.

```yaml
relationships:
  - type: FOLLOWS
    edge_id: follow_id              # Single column (optimized)
  - type: FLIGHT
    edge_id: [flight_id, leg_num]   # Composite (uses tuple)
  - type: AUTHORED
    edge_id: null                   # Default: tuple(from_id, to_id)
```

### Test Coverage Analysis

#### ✅ **Unit Tests** (3/3 passing)
**File**: `src/clickhouse_query_generator/edge_uniqueness_tests.rs`

1. ✅ `test_default_edge_id_tuple` - Default `tuple(from_id, to_id)`
2. ✅ `test_composite_edge_id` - Multi-column composite keys
3. ✅ `test_simple_edge_id` - Single-column optimization

**Coverage**: Edge tracking logic only (isolated unit tests)

#### ✅ **Integration Tests** (1 test)
**File**: `tests/integration/test_edge_id_optimization.py`

1. ✅ `test_single_column_edge_id` - Real query with benchmark schema

**Coverage**: Single integration test with `social_benchmark` schema

#### ⚠️ **Query Pattern Coverage** (~10% of 139 patterns)

| Pattern Category | Coverage | Notes |
|-----------------|----------|-------|
| **Basic Node Patterns** | ❌ 0/19 | Edge IDs not relevant for node-only queries |
| **Aggregations** | ❌ 0/20 | Aggregation with edge_id untested |
| **Sorting & Pagination** | ❌ 0/12 | ORDER BY with edge_id untested |
| **Relationships** | ⚠️ 1/15 | Single integration test only |
| **Variable-Length Paths** | ⚠️ 3/18 | Unit tests only (isolated) |
| **Shortest Path** | ❌ 0/8 | Shortest path + edge_id untested |
| **Path Functions** | ❌ 0/6 | Path functions + edge_id untested |
| **OPTIONAL MATCH** | ❌ 0/9 | OPTIONAL MATCH + edge_id untested |
| **WITH Clause** | ❌ 0/12 | WITH + edge_id untested |
| **Multiple MATCH** | ❌ 0/8 | Multiple MATCH + edge_id untested |
| **Advanced Combos** | ❌ 0/12 | Complex queries untested |

**Tested Patterns** (14/139):
```cypher
# ✅ Unit tested (isolated)
MATCH (a)-[r:FOLLOWS*1..3]->(b)   # Single-column edge_id
MATCH (a)-[r:FLIGHT*1..3]->(b)    # Composite edge_id
MATCH (a)-[r:AUTHORED*1..3]->(b)  # Default tuple(from_id, to_id)

# ✅ Integration tested (1 test)
MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) RETURN b.name

# ❌ NOT tested with edge_id variations:
MATCH (a)-[r*]-(b) WHERE ...                        # WHERE + edge_id
MATCH (a)-[r*1..3]->(b) RETURN count(*)             # Aggregate + edge_id
shortestPath((a)-[*]->(b))                          # Shortest path + edge_id
OPTIONAL MATCH (a)-[r*]->(b)                        # OPTIONAL MATCH + edge_id
WITH ... var-length paths                           # WITH + edge_id
Multiple MATCH with var-length                      # Multiple MATCH + edge_id
```

### 🔴 **Critical Gaps**

1. **Limited Integration Tests**: Only 1 integration test for single-column edge_id
2. **No Composite Edge_id E2E Test**: Composite keys tested in unit tests only
3. **No Default Tuple E2E Test**: Default fallback tested in unit tests only
4. **No Complex Pattern Tests**: Variable-length + WHERE/ORDER BY/aggregation
5. **No Schema Migration Validation**: `add_edge_ids_benchmark.sql` script never tested
6. **No Performance Validation**: Single-column optimization speedup unverified

---

## Combined Feature Testing 🔴

### ❌ **Zero Cross-Feature Testing**

None of these combinations have been tested:

```cypher
# ❌ Denormalized + Polymorphic
MATCH (a)-[r:FOLLOWS]->(b) 
WHERE r.interaction_type = 'FOLLOWS'  # Polymorphic filter
RETURN a.city  # Denormalized property

# ❌ Denormalized + Composite Edge ID
MATCH (a)-[f:FLIGHT*1..3]->(b)
WHERE f.edge_id IN [tuple(1, 1), tuple(1, 2)]  # Composite edge_id
RETURN a.city  # Denormalized property

# ❌ Polymorphic + Composite Edge ID
MATCH (a)-[r*1..3]->(b)
WHERE r.type_column = 'FOLLOWS'  # Polymorphic
AND r.edge_id = composite_key    # Composite edge_id

# ❌ All three combined
MATCH (a)-[r:FOLLOWS*1..3]->(b)
WHERE r.interaction_type = 'FOLLOWS'  # Polymorphic
AND has(path_edges, r.composite_id)   # Composite edge_id
RETURN a.denormalized_city            # Denormalized property
```

---

## Recommended Testing Priorities 🎯

### **Phase 1: Critical Gaps (Week 1)** 🔥

#### 1.1 Polymorphic Edges Integration Tests (3 days)
**Priority**: **CRITICAL** - Feature has ZERO testing

```python
# tests/integration/test_polymorphic_edges.py
def test_basic_polymorphic_edge():
    """MATCH (a)-[r:FOLLOWS]->(b) WHERE type_column = 'FOLLOWS'"""
    
def test_polymorphic_multi_type():
    """MATCH (a)-[r:FOLLOWS|LIKES]->(b)"""
    
def test_polymorphic_with_where():
    """MATCH (a)-[r:FOLLOWS]->(b) WHERE r.timestamp > x"""
    
def test_polymorphic_aggregation():
    """MATCH (a)-[r:FOLLOWS]->(b) RETURN count(r)"""
    
def test_polymorphic_variable_length():
    """MATCH (a)-[r:FOLLOWS*1..3]->(b)"""
```

**Impact**: Validates entire feature that's currently untested

#### 1.2 Denormalized Properties Integration Tests (2 days)
**Priority**: **HIGH** - Only unit tests exist

```python
# tests/integration/test_denormalized_properties.py
def test_denormalized_basic_access():
    """MATCH (a)-[f:FLIGHT]->(b) RETURN a.city"""
    
def test_denormalized_where_clause():
    """MATCH (a)-[f:FLIGHT]->(b) WHERE a.city = 'NYC'"""
    
def test_denormalized_aggregation():
    """MATCH (a)-[f:FLIGHT]->(b) RETURN count(a.city)"""
    
def test_denormalized_variable_length():
    """MATCH (a)-[f:FLIGHT*1..3]->(b) RETURN a.city"""
    
def test_denormalized_order_by():
    """MATCH (a)-[f:FLIGHT]->(b) RETURN a.city ORDER BY a.state"""
```

**Impact**: Validates 10-100x performance claim with real queries

#### 1.3 Composite Edge ID Integration Tests (2 days)
**Priority**: **MEDIUM** - Only 1 integration test exists

```python
# tests/integration/test_composite_edge_ids.py
def test_composite_edge_id_variable_length():
    """MATCH (a)-[f:FLIGHT*1..3]->(b) WHERE edge_id = tuple(x,y)"""
    
def test_default_tuple_fallback():
    """MATCH (a)-[r:AUTHORED*]->(b)  # No edge_id defined"""
    
def test_single_column_optimization():
    """Verify SQL uses direct column, not tuple()"""
    
def test_composite_with_aggregation():
    """MATCH (a)-[f*]->(b) RETURN count(DISTINCT edge_id)"""
```

**Impact**: Validates optimization claims and default fallback

### **Phase 2: Cross-Feature Testing (Week 2)** 🔥

#### 2.1 Combined Feature Integration (5 days)

```python
# tests/integration/test_schema_variations_combined.py
def test_denormalized_plus_polymorphic():
    """Polymorphic edges with denormalized properties"""
    
def test_denormalized_plus_composite_edge_id():
    """Denormalized properties with composite edge tracking"""
    
def test_polymorphic_plus_composite_edge_id():
    """Polymorphic edges with composite uniqueness tracking"""
    
def test_all_three_features():
    """All schema variations in single query"""
```

**Impact**: Validates feature interactions don't break

### **Phase 3: Query Pattern Matrix (Week 3)** 📊

#### 3.1 Systematic Pattern Coverage

For each of 3 features × 11 query pattern categories = **33 test suites**:

```python
# Example: Denormalized × Aggregations
def test_denormalized_count()
def test_denormalized_sum()
def test_denormalized_avg()
def test_denormalized_group_by()
def test_denormalized_having()
# ... 20 tests per feature

# Example: Polymorphic × Variable-Length Paths
def test_polymorphic_unbounded()
def test_polymorphic_bounded_range()
def test_polymorphic_with_where()
# ... 18 tests per feature
```

**Impact**: Full query pattern coverage (95% → 100%)

### **Phase 4: Performance Validation (Week 4)** 🚀

#### 4.1 Benchmark Tests

```python
# benchmarks/schema_variations_benchmark.py
def benchmark_denormalized_vs_joins():
    """Verify 10-100x speedup claim"""
    
def benchmark_polymorphic_filtering():
    """Measure polymorphic WHERE clause overhead"""
    
def benchmark_composite_vs_single_edge_id():
    """Verify tuple() overhead reduction"""
```

**Impact**: Validates performance claims with real data

---

## Test Coverage Tracking

### **Current State**
```
Schema Variations Testing: 11/1251 tests (0.9%)
├─ Denormalized Properties:  6 unit + 0 integration = 6 tests (~5% coverage)
├─ Polymorphic Edges:        0 unit + 0 integration = 0 tests (0% coverage) 🔴
└─ Composite Edge IDs:       3 unit + 1 integration = 4 tests (~10% coverage)

Cross-Feature Testing:       0 tests (0% coverage) 🔴
```

### **Target State** (After 4-week testing sprint)
```
Schema Variations Testing: 200+/1251 tests (16%)
├─ Denormalized Properties:  6 unit + 40 integration = 46 tests (30% coverage)
├─ Polymorphic Edges:        6 unit + 40 integration = 46 tests (30% coverage)
├─ Composite Edge IDs:       3 unit + 40 integration = 43 tests (30% coverage)
└─ Cross-Feature Testing:    20 integration tests
└─ Performance Benchmarks:   10 benchmark tests
```

---

## Risk Assessment 🚨

| Feature | Current Risk | Impact if Broken | Mitigation |
|---------|--------------|------------------|------------|
| **Polymorphic Edges** | 🔴 **CRITICAL** | Queries fail with polymorphic schemas | Phase 1.1 tests (3 days) |
| **Denormalized Props** | 🟡 **HIGH** | Performance claims unverified, WHERE/aggregation untested | Phase 1.2 tests (2 days) |
| **Composite Edge IDs** | 🟡 **MEDIUM** | Composite keys untested E2E, default fallback unverified | Phase 1.3 tests (2 days) |
| **Cross-Feature** | 🔴 **CRITICAL** | Unknown interactions between features | Phase 2 tests (5 days) |

**Overall Risk**: **HIGH** - Features implemented but undertested

---

## Recommendations for Production 📋

### **Immediate Actions** (Before v0.5.0 release)

1. ✅ **Mark Features as Beta**
   - Document in STATUS.md: "Schema Variations (Beta)"
   - Add warning in docs: "Limited testing, use with caution"
   - Recommend standard schemas for production

2. ✅ **Create Warning System**
   ```rust
   // Emit warning when loading polymorphic/denormalized schemas
   log::warn!("Using beta schema variations - limited testing coverage");
   ```

3. ✅ **Document Known Limitations**
   - Update `docs/Known-Limitations.md`
   - Add "Schema Variations Coverage" section
   - List untested query patterns

### **Testing Sprint** (4 weeks)

Follow Phase 1-4 plan above to reach 16% coverage across schema variations.

### **Release Strategy**

**v0.5.0-beta**: Ship with warnings + limited testing
**v0.5.1**: Phase 1 tests complete (critical gaps filled)
**v0.5.2**: Phase 2 tests complete (cross-feature validated)
**v0.5.3**: Phase 3 tests complete (full pattern coverage)
**v0.6.0**: Phase 4 complete (performance validated) → Remove beta tag

---

## Conclusion

**Today's schema variation features add 3× complexity but have <1% test coverage**:

- **Denormalized Properties**: 6 unit tests, 0 integration tests (~5% coverage)
- **Polymorphic Edges**: 0 tests - **CRITICAL GAP** 🔴
- **Composite Edge IDs**: 4 tests total (~10% coverage)
- **Cross-Feature Testing**: None 🔴

**Recommendation**: Execute 4-week testing sprint (Phase 1-4) to reach production-ready state. Until then, mark features as Beta and recommend standard schemas for production workloads.

**LDBC SNB benchmark (Phase 3 of roadmap)** will naturally test many of these patterns, but systematic testing is needed first to ensure correctness.
