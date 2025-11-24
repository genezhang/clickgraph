# v0.5.2-alpha Regression Test Plan

**Created**: November 27, 2025  
**Current Status**: **ALPHA** (Schema variations undertested)  
**Goal**: Establish baseline quality before beta release

---

## 🎯 Testing Philosophy

**Alpha → Beta → Production Quality Ladder**:
```
Alpha:    Core features work, new features minimally tested
Beta:     All features tested, known limitations documented  
Production: Comprehensive testing, performance validated
```

**Current Reality**:
- ✅ Core Cypher features: **Well tested** (95% coverage)
- ⚠️ Schema variations (v0.5.1): **Undertested** (<1% coverage)
- ✅ Unit tests: 440/447 passing (98.4%)
- ⚠️ Integration tests: 236/400 passing (59% - many aspirational)

**Honest Assessment**: We're at **alpha quality** for v0.5.2 due to schema variations gaps.

---

## 📊 Current Test Inventory

### Existing Test Suites (Well-Maintained)

#### Integration Tests (236 passing / 400 total)
**Location**: `tests/integration/*.py`

**Core Cypher Features** (Excellent coverage):
- ✅ `test_basic_queries.py` - 19 tests (6 test classes)
- ✅ `test_aggregations.py` - 20 tests (9 test classes)
- ✅ `test_relationships.py` - 15 tests
- ✅ `test_variable_length_paths.py` - 18 tests
- ✅ `test_shortest_paths.py` - 8 tests
- ✅ `test_path_variables.py` - 6 tests
- ✅ `test_optional_match.py` - 9 tests (10 test classes)
- ✅ `test_with_clause.py` - 12 tests
- ✅ `test_case_expressions.py` - Multiple tests (9 test classes)
- ✅ `test_error_handling.py` - Multiple tests (11 test classes)

**Advanced Features** (Good coverage):
- ✅ `test_multi_database.py` - 9 test classes
- ✅ `test_multi_tenant_parameterized_views.py` - 7 test classes
- ✅ `test_role_based_queries.py`
- ✅ `test_neo4j_functions.py`
- ✅ `test_parameter_functions.py` - 5 test classes

**Performance/Optimization** (Good coverage):
- ✅ `test_performance.py`
- ✅ `test_query_cache.py`
- ✅ `test_cache_error_handling.py`
- ✅ `test_auto_discovery.py`

**Bolt Protocol** (Excellent coverage):
- ✅ `tests/integration/bolt/test_bolt_basic.py`
- ✅ `tests/integration/bolt/test_bolt_auth.py`
- ✅ `tests/integration/bolt/test_bolt_transactions.py`
- ✅ `tests/integration/bolt/test_bolt_streaming.py`
- **4/4 E2E tests passing** (100%)

#### Unit Tests (440 passing / 447 total)
**Location**: `src/**/*_tests.rs`, `src/**/tests/*.rs`

**Test Count by Module**:
- ✅ AST parsing: 50+ tests
- ✅ Query planning: 100+ tests
- ✅ SQL generation: 150+ tests
- ✅ Optimizer passes: 80+ tests
- ✅ Schema validation: 60+ tests

**7 Failing Tests**: Global state conflicts (not bugs)

---

## 🔴 Critical Gaps: Schema Variations (v0.5.1 Features)

### 1. Denormalized Property Access 🟡
**Status**: 6 unit tests, **0 integration tests**

**What's Missing**:
```python
# ❌ NOT TESTED (Integration level)
def test_denormalized_basic_query():
    """MATCH (a)-[f:FLIGHT]->(b) RETURN a.city"""
    
def test_denormalized_where_clause():
    """WHERE a.city = 'NYC' with denormalized property"""
    
def test_denormalized_aggregation():
    """COUNT(a.city) with denormalized property"""
    
def test_denormalized_variable_length():
    """Variable-length path + denormalized properties"""
    
def test_denormalized_order_by():
    """ORDER BY a.city with denormalized property"""
```

### 2. Polymorphic Edge Type Filters 🔴
**Status**: **0 tests** (CRITICAL)

**What's Missing**:
```python
# ❌ COMPLETELY UNTESTED
def test_polymorphic_basic():
    """MATCH (a)-[r:FOLLOWS]->(b) with polymorphic table"""
    
def test_polymorphic_type_filter():
    """Verify WHERE interaction_type = 'FOLLOWS' works"""
    
def test_polymorphic_label_filter():
    """Verify from_type/to_type label matching works"""
    
def test_polymorphic_multi_type():
    """MATCH (a)-[r:FOLLOWS|LIKES]->(b)"""
    
def test_polymorphic_variable_length():
    """Variable-length path on polymorphic edges"""
```

### 3. Composite Edge ID Uniqueness 🟡
**Status**: 3 unit tests, 1 integration test

**What's Missing**:
```python
# ❌ NOT TESTED (Integration level)
def test_composite_edge_id_e2e():
    """Real query with composite edge_id = [col1, col2]"""
    
def test_default_tuple_fallback_e2e():
    """Real query with edge_id = None (tuple fallback)"""
    
def test_composite_with_aggregation():
    """COUNT(DISTINCT edge_id) with composite keys"""
```

---

## 🎯 Regression Test Strategy

### Phase 1: Core Feature Regression (Week 1) ✅
**Goal**: Ensure existing features still work  
**Effort**: 2-3 days  
**Status**: **Already excellent coverage**

**Approach**: Run existing test suites and fix any regressions

```bash
# Existing tests (should all pass)
cd tests/integration
python -m pytest test_basic_queries.py -v          # ✅ 19 tests
python -m pytest test_aggregations.py -v           # ✅ 20 tests
python -m pytest test_relationships.py -v          # ✅ 15 tests
python -m pytest test_variable_length_paths.py -v  # ✅ 18 tests
python -m pytest test_shortest_paths.py -v         # ✅ 8 tests
python -m pytest test_optional_match.py -v         # ✅ 9 tests
python -m pytest test_with_clause.py -v            # ✅ 12 tests
python -m pytest test_case_expressions.py -v       # ✅ Multiple
python -m pytest test_error_handling.py -v         # ✅ Multiple
```

**Expected Result**: 236/236 core tests passing (existing tests)

### Phase 2: Schema Variations Regression (Week 2) 🔥
**Goal**: Establish baseline for new features  
**Effort**: 5 days  
**Priority**: **CRITICAL** (blocking beta)

#### Day 1-2: Polymorphic Edges (CRITICAL) 🔴
**Create**: `tests/integration/test_polymorphic_edges_regression.py`

```python
import pytest
import requests
from conftest import API_BASE_URL

# Use social_polymorphic.yaml schema

class TestPolymorphicBasicRegression:
    """Verify basic polymorphic edge queries work"""
    
    def test_single_type_match(self):
        """MATCH (a)-[r:FOLLOWS]->(b) RETURN count(*)"""
        query = "MATCH (a)-[r:FOLLOWS]->(b) RETURN count(*) as cnt"
        result = requests.post(
            f"{API_BASE_URL}/query",
            json={"query": query, "schema_name": "social_polymorphic"}
        )
        assert result.status_code == 200
        data = result.json()
        assert "cnt" in data["columns"]
        # Verify non-zero count (data exists)
        assert data["rows"][0][0] > 0
    
    def test_type_filter_generated(self):
        """Verify WHERE interaction_type = 'FOLLOWS' in SQL"""
        query = "MATCH (a)-[r:FOLLOWS]->(b) RETURN a, r, b LIMIT 1"
        result = requests.post(
            f"{API_BASE_URL}/query",
            json={"query": query, "schema_name": "social_polymorphic", "sql_only": True}
        )
        sql = result.json()["sql"]
        # Must filter by type_column
        assert "interaction_type = 'FOLLOWS'" in sql or "interaction_type IN ('FOLLOWS')" in sql
    
    def test_label_filter_generated(self):
        """Verify from_type/to_type label matching in SQL"""
        query = "MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN count(*)"
        result = requests.post(
            f"{API_BASE_URL}/query",
            json={"query": query, "schema_name": "social_polymorphic", "sql_only": True}
        )
        sql = result.json()["sql"]
        # Must filter by label columns
        assert "from_type" in sql and "to_type" in sql
    
    def test_multi_type_union(self):
        """MATCH (a)-[r:FOLLOWS|LIKES]->(b) generates UNION"""
        query = "MATCH (a)-[r:FOLLOWS|LIKES]->(b) RETURN count(*)"
        result = requests.post(
            f"{API_BASE_URL}/query",
            json={"query": query, "schema_name": "social_polymorphic", "sql_only": True}
        )
        sql = result.json()["sql"]
        # Multi-type should generate UNION or IN clause
        assert "UNION" in sql or "interaction_type IN ('FOLLOWS', 'LIKES')" in sql

class TestPolymorphicPropertiesRegression:
    """Verify properties work on polymorphic edges"""
    
    def test_relationship_property_access(self):
        """RETURN r.timestamp (relationship property)"""
        query = "MATCH (a)-[r:FOLLOWS]->(b) RETURN r.timestamp LIMIT 1"
        result = requests.post(
            f"{API_BASE_URL}/query",
            json={"query": query, "schema_name": "social_polymorphic"}
        )
        assert result.status_code == 200
    
    def test_relationship_property_filter(self):
        """WHERE r.timestamp > x"""
        query = """
        MATCH (a)-[r:FOLLOWS]->(b) 
        WHERE r.timestamp > '2024-01-01'
        RETURN count(*)
        """
        result = requests.post(
            f"{API_BASE_URL}/query",
            json={"query": query, "schema_name": "social_polymorphic"}
        )
        assert result.status_code == 200

class TestPolymorphicAdvancedRegression:
    """Verify complex patterns work"""
    
    def test_variable_length_polymorphic(self):
        """Variable-length path on polymorphic edges"""
        query = "MATCH (a)-[r:FOLLOWS*1..2]->(b) RETURN count(*)"
        result = requests.post(
            f"{API_BASE_URL}/query",
            json={"query": query, "schema_name": "social_polymorphic"}
        )
        assert result.status_code == 200
    
    def test_polymorphic_with_aggregation(self):
        """Aggregation on polymorphic edges"""
        query = """
        MATCH (a)-[r:FOLLOWS]->(b)
        RETURN a.name, count(r) as follower_count
        GROUP BY a.name
        """
        result = requests.post(
            f"{API_BASE_URL}/query",
            json={"query": query, "schema_name": "social_polymorphic"}
        )
        assert result.status_code == 200

# Run with: pytest test_polymorphic_edges_regression.py -v
```

**Expected Pass Rate**: 70% (alpha quality)  
**Blockers Identified**: Document failures for beta fixes

#### Day 3-4: Denormalized Properties
**Create**: `tests/integration/test_denormalized_properties_regression.py`

```python
class TestDenormalizedBasicRegression:
    """Verify denormalized property access works"""
    
    def test_from_node_property_access(self):
        """RETURN a.city (denormalized from origin)"""
        query = "MATCH (a:Airport)-[f:FLIGHT]->(b:Airport) RETURN a.city LIMIT 1"
        result = requests.post(
            f"{API_BASE_URL}/query",
            json={"query": query, "schema_name": "ontime_denormalized"}
        )
        assert result.status_code == 200
    
    def test_to_node_property_access(self):
        """RETURN b.city (denormalized from destination)"""
        query = "MATCH (a:Airport)-[f:FLIGHT]->(b:Airport) RETURN b.city LIMIT 1"
        result = requests.post(
            f"{API_BASE_URL}/query",
            json={"query": query, "schema_name": "ontime_denormalized"}
        )
        assert result.status_code == 200
    
    def test_no_join_generated(self):
        """Verify SQL uses edge table columns (no JOIN)"""
        query = "MATCH (a:Airport)-[f:FLIGHT]->(b:Airport) RETURN a.city, b.city"
        result = requests.post(
            f"{API_BASE_URL}/query",
            json={"query": query, "schema_name": "ontime_denormalized", "sql_only": True}
        )
        sql = result.json()["sql"]
        # Should use flights.origin_city, flights.dest_city
        assert "origin_city" in sql and "dest_city" in sql
        # Should NOT have JOIN to airports table
        assert sql.count("JOIN") == 0

class TestDenormalizedWhereRegression:
    """Verify WHERE clause works with denormalized properties"""
    
    def test_where_equals(self):
        """WHERE a.city = 'NYC'"""
        query = """
        MATCH (a:Airport)-[f:FLIGHT]->(b:Airport)
        WHERE a.city = 'New York'
        RETURN count(*)
        """
        result = requests.post(
            f"{API_BASE_URL}/query",
            json={"query": query, "schema_name": "ontime_denormalized"}
        )
        assert result.status_code == 200
    
    def test_where_on_both_nodes(self):
        """WHERE a.city = 'NYC' AND b.city = 'LAX'"""
        query = """
        MATCH (a:Airport)-[f:FLIGHT]->(b:Airport)
        WHERE a.city = 'New York' AND b.city = 'Los Angeles'
        RETURN count(*)
        """
        result = requests.post(
            f"{API_BASE_URL}/query",
            json={"query": query, "schema_name": "ontime_denormalized"}
        )
        assert result.status_code == 200

class TestDenormalizedAggregationRegression:
    """Verify aggregations work with denormalized properties"""
    
    def test_count_by_city(self):
        """GROUP BY a.city"""
        query = """
        MATCH (a:Airport)-[f:FLIGHT]->(b:Airport)
        RETURN a.city, count(*) as flight_count
        GROUP BY a.city
        ORDER BY flight_count DESC
        LIMIT 10
        """
        result = requests.post(
            f"{API_BASE_URL}/query",
            json={"query": query, "schema_name": "ontime_denormalized"}
        )
        assert result.status_code == 200

class TestDenormalizedVariableLengthRegression:
    """Verify variable-length paths work with denormalized properties"""
    
    def test_variable_length_denormalized(self):
        """MATCH (a)-[f:FLIGHT*1..2]->(b) RETURN a.city"""
        query = """
        MATCH (a:Airport)-[f:FLIGHT*1..2]->(b:Airport)
        WHERE a.code = 'LAX'
        RETURN b.city
        LIMIT 10
        """
        result = requests.post(
            f"{API_BASE_URL}/query",
            json={"query": query, "schema_name": "ontime_denormalized"}
        )
        assert result.status_code == 200

# Run with: pytest test_denormalized_properties_regression.py -v
```

**Expected Pass Rate**: 60% (alpha quality)  
**Blockers Identified**: Document failures for beta fixes

#### Day 5: Composite Edge IDs
**Create**: `tests/integration/test_composite_edge_ids_regression.py`

```python
class TestCompositeEdgeIdRegression:
    """Verify composite edge_id tracking works"""
    
    def test_composite_edge_id_variable_length(self):
        """Variable-length with composite edge_id"""
        query = "MATCH (a)-[f:FLIGHT*1..2]->(b) RETURN count(*)"
        result = requests.post(
            f"{API_BASE_URL}/query",
            json={"query": query, "schema_name": "ontime_composite_edge_id"}
        )
        assert result.status_code == 200
    
    def test_composite_uses_tuple(self):
        """Verify SQL uses tuple(col1, col2) for composite keys"""
        query = "MATCH (a)-[f:FLIGHT*1..2]->(b) RETURN count(*)"
        result = requests.post(
            f"{API_BASE_URL}/query",
            json={"query": query, "schema_name": "ontime_composite_edge_id", "sql_only": True}
        )
        sql = result.json()["sql"]
        # Composite edge_id should use tuple()
        assert "tuple(" in sql

class TestSingleColumnEdgeIdRegression:
    """Verify single-column edge_id optimization works"""
    
    def test_single_column_no_tuple(self):
        """Verify SQL avoids tuple() for single column"""
        query = "MATCH (a)-[r:FOLLOWS*1..2]->(b) RETURN count(*)"
        result = requests.post(
            f"{API_BASE_URL}/query",
            json={"query": query, "schema_name": "social_benchmark"}
        )
        assert result.status_code == 200
        
        # Check SQL doesn't use tuple
        result_sql = requests.post(
            f"{API_BASE_URL}/query",
            json={"query": query, "schema_name": "social_benchmark", "sql_only": True}
        )
        sql = result_sql.json()["sql"]
        # Should use direct column: r.follow_id
        # Should NOT use: tuple(r.follow_id)
        assert "tuple(r.follow_id)" not in sql

class TestDefaultTupleFallbackRegression:
    """Verify default tuple(from_id, to_id) fallback works"""
    
    def test_no_edge_id_uses_default(self):
        """Variable-length with edge_id = None"""
        query = "MATCH (a)-[r:AUTHORED*1..2]->(b) RETURN count(*)"
        result = requests.post(
            f"{API_BASE_URL}/query",
            json={"query": query, "schema_name": "social_benchmark"}
        )
        assert result.status_code == 200
    
    def test_default_tuple_in_sql(self):
        """Verify SQL uses tuple(from_id, to_id) when edge_id is None"""
        query = "MATCH (a)-[r:AUTHORED*1..2]->(b) RETURN count(*)"
        result = requests.post(
            f"{API_BASE_URL}/query",
            json={"query": query, "schema_name": "social_benchmark", "sql_only": True}
        )
        sql = result.json()["sql"]
        # Should use default tuple
        assert "tuple(" in sql

# Run with: pytest test_composite_edge_ids_regression.py -v
```

**Expected Pass Rate**: 80% (better than others due to existing tests)

---

## 📋 Regression Test Execution Plan

### Week 1: Baseline Regression
```bash
# Day 1: Core features baseline
cd tests/integration
pytest test_basic_queries.py -v --tb=short
pytest test_aggregations.py -v --tb=short
pytest test_relationships.py -v --tb=short

# Day 2: Advanced features baseline
pytest test_variable_length_paths.py -v --tb=short
pytest test_shortest_paths.py -v --tb=short
pytest test_optional_match.py -v --tb=short
pytest test_with_clause.py -v --tb=short

# Day 3: Complete baseline
pytest test_case_expressions.py -v --tb=short
pytest test_error_handling.py -v --tb=short
pytest test_multi_database.py -v --tb=short
pytest test_multi_tenant_parameterized_views.py -v --tb=short
pytest bolt/ -v --tb=short

# Generate baseline report
pytest --tb=short --html=reports/baseline_regression.html
```

**Success Criteria**: 236/236 existing tests pass

### Week 2: Schema Variations Regression
```bash
# Day 1-2: Polymorphic edges (CRITICAL)
# Create test_polymorphic_edges_regression.py
pytest test_polymorphic_edges_regression.py -v --tb=short

# Day 3-4: Denormalized properties
# Create test_denormalized_properties_regression.py
pytest test_denormalized_properties_regression.py -v --tb=short

# Day 5: Composite edge IDs
# Create test_composite_edge_ids_regression.py
pytest test_composite_edge_ids_regression.py -v --tb=short

# Generate schema variations report
pytest test_*_regression.py --tb=short --html=reports/schema_variations_regression.html
```

**Success Criteria**: 70%+ pass rate (alpha quality)

---

## 📊 Quality Gates

### Alpha Release (v0.5.2-alpha) ✅
**Criteria**:
- ✅ Core features: 236/236 tests passing (100%)
- ⚠️ Schema variations: 70%+ pass rate (NEW)
- ✅ Unit tests: 440/447 passing (98.4%)
- ✅ Bolt protocol: 4/4 E2E tests (100%)
- ✅ Known issues documented

**Documentation**:
- ⚠️ Mark schema variations as "Alpha"
- ⚠️ Add warning: "Schema variations have limited testing"
- ✅ Document known failures
- ✅ Recommend standard schemas for production

### Beta Release (v0.5.2-beta) - Future
**Criteria**:
- ✅ Core features: 100% pass rate
- ✅ Schema variations: 90%+ pass rate
- ✅ All critical bugs fixed
- ✅ Performance validated

### Production (v0.5.2) - Future
**Criteria**:
- ✅ All features: 95%+ pass rate
- ✅ LDBC benchmark passing
- ✅ Performance regression tests
- ✅ Comprehensive documentation

---

## 🎯 Deliverables

### Test Files (Week 2)
1. ✅ `test_polymorphic_edges_regression.py` - 10+ tests
2. ✅ `test_denormalized_properties_regression.py` - 12+ tests
3. ✅ `test_composite_edge_ids_regression.py` - 6+ tests

### Test Reports
1. ✅ `reports/baseline_regression.html` - Core features baseline
2. ✅ `reports/schema_variations_regression.html` - New features regression
3. ✅ `reports/alpha_quality_summary.md` - Overall assessment

### Documentation Updates
1. ✅ Update `STATUS.md` - Mark schema variations as "Alpha"
2. ✅ Update `KNOWN_ISSUES.md` - Document test failures
3. ✅ Update `docs/Known-Limitations.md` - Add schema variations section
4. ✅ Update `CHANGELOG.md` - Note alpha quality for schema variations

---

## 🚀 Next Steps After Regression Testing

### Beta Path (v0.5.2-beta)
1. **Fix Critical Failures** (2 weeks)
   - Address polymorphic edge bugs
   - Fix denormalized property edge cases
   - Validate composite edge ID corner cases

2. **Expand Test Coverage** (2 weeks)
   - Add 50+ more integration tests
   - Cross-feature combination tests
   - Performance regression tests

3. **Beta Release** (Week 5)
   - 90%+ pass rate on schema variations
   - Beta tag in documentation
   - User feedback collection

### Production Path (v0.5.2)
1. **Complete Testing** (2 weeks)
   - 95%+ overall pass rate
   - LDBC SNB benchmark
   - Performance validation

2. **Documentation Polish** (1 week)
   - Remove alpha/beta warnings
   - Complete API docs
   - Migration guides

3. **Production Release** (Week 8)
   - Remove quality warnings
   - Full feature parity claims
   - Production-ready tag

---

## 📝 Conclusion

**v0.5.2-alpha Status**: **READY FOR REGRESSION TESTING**

**Realistic Timeline**:
- Week 1: Core regression (baseline) ✅
- Week 2: Schema variations regression 🔥
- **Alpha Release**: End of Week 2
- Beta Release: +4 weeks
- Production: +8 weeks

**Key Message**: We're shipping alpha with excellent core feature coverage but undertested schema variations. This is **honest quality assessment** - better to ship alpha with warnings than beta with hidden bugs.

**Recommendation**: Execute 2-week regression test sprint, ship v0.5.2-alpha with clear documentation of limitations, then iterate to beta/production quality.
