# ClickGraph Testing Guide

**Last Updated**: November 13, 2025

This guide documents all regression tests and testing procedures for ClickGraph.

---

## 📋 Quick Start

### Run All Tests (One Command)
```powershell
# Windows PowerShell
.\scripts\test\run_all_tests.ps1

# Linux/Mac
./scripts/test/run_all_tests.sh  # TODO: Create shell version
```

### Run Specific Test Categories
```powershell
# Rust tests only (fast - ~10s)
.\scripts\test\run_all_tests.ps1 -Quick

# Python tests only
.\scripts\test\run_all_tests.ps1 -Python

# Verbose output
.\scripts\test\run_all_tests.ps1 -Verbose
```

---

## 🧪 Test Inventory

### 1. Rust Unit Tests (434 tests)

**Location**: `src/**/*.rs`  
**Run**: `cargo test --lib`  
**Duration**: ~5-8 seconds

**Coverage**:
- ✅ Query parsing (OpenCypher grammar)
- ✅ Query planning (logical plan generation)
- ✅ SQL generation (ClickHouse query generation)
- ✅ Function mapping (25+ Neo4j functions)
- ✅ Path variable handling
- ✅ Query cache (LRU eviction, hit/miss)
- ✅ Schema validation
- ✅ Property mapping
- ✅ Multi-schema architecture

**Example**:
```rust
// src/query_planner/logical_plan/mod.rs
#[test]
fn test_simple_match_query() { ... }
```

---

### 2. Rust Integration Tests (12 tests)

**Location**: `tests/integration/mod.rs`, `tests/unit/mod.rs`  
**Run**: `cargo test --test '*'`  
**Duration**: ~2-3 seconds

**Coverage**:
- ✅ Parameter function integration
- ✅ Path variable SQL generation
- ✅ Math functions with parameters
- ✅ String functions with parameters
- ✅ Nested functions with properties
- ✅ Case expressions with parameters
- ✅ Aggregation with parameter filters

**Example Tests**:
```rust
#[test]
fn test_string_function_with_parameters_in_return()
#[test]
fn test_case_expression_with_parameters()
#[test]
fn test_path_variable_sql_generation()
```

---

### 3. Python Integration Tests (318 tests)

**Location**: `tests/integration/*.py`  
**Run**: `cd tests/integration && python -m pytest`  
**Duration**: ~20-40 seconds  
**Requirements**: Server running on port 8080

**Test Files**:
- `test_basic_queries.py` - Core MATCH/WHERE/RETURN patterns
- `test_aggregations.py` - COUNT, SUM, AVG, MIN, MAX
- `test_relationships.py` - Relationship traversal patterns
- `test_variable_length_paths.py` - `*`, `*2`, `*1..3` patterns
- `test_shortest_paths.py` - `shortestPath()`, `allShortestPaths()`
- `test_optional_match.py` - LEFT JOIN semantics
- `test_with_clause.py` - WITH projections and pipelines
- `test_case_expressions.py` - CASE/WHEN/THEN/ELSE
- `test_functions_final.py` - Neo4j function mappings
- `test_functions_with_match.py` - Functions in MATCH context
- `test_neo4j_functions.py` - Comprehensive function tests
- `test_parameter_functions.py` - Parameter + function combinations
- `test_param_func.py` - Parameter function edge cases
- `test_path_variables.py` - Path variable handling
- `test_query_cache.py` - Cache hit/miss behavior
- `test_use_clause.py` - Multi-schema USE clause
- `test_http_use_clause.py` - USE via HTTP API
- `test_multi_database.py` - Multiple database support
- `test_multi_hop_fix.py` - Multi-hop JOIN fix validation
- `test_cache_error_handling.py` - Cache error scenarios
- `test_error_handling.py` - Error message validation
- `test_performance.py` - Performance benchmarks

**Bolt Protocol Tests**:
- `test_integration/bolt/test_bolt_connection.py` - Connection handling
- `test_integration/bolt/test_bolt_authentication.py` - Auth schemes
- `test_integration/bolt/test_bolt_queries.py` - Query execution

**Example**:
```python
# tests/integration/test_basic_queries.py
def test_simple_match():
    query = "MATCH (n:User) RETURN n.name LIMIT 5"
    response = requests.post(f"{SERVER_URL}/query", json={"query": query})
    assert response.status_code == 200
```

---

### 4. Python E2E Tests (21 tests)

**Location**: `tests/e2e/*.py`  
**Run**: `cd tests/e2e && python -m pytest`  
**Duration**: ~10-20 seconds  
**Requirements**: Server + ClickHouse running

**Test Files**:
- `test_bolt_e2e.py` - Bolt protocol E2E (5 tests)
  - Connection negotiation
  - Authentication flow
  - Query execution
  - Result streaming
- `test_query_cache_e2e.py` - Query cache E2E (5 tests)
  - Cache miss on first query
  - Cache hit on repeated query
  - Cache invalidation on schema change
  - LRU eviction behavior
  - Cache statistics
- `test_param_func_e2e.py` - Parameter functions E2E (11 tests)
  - Parameters in WHERE clause
  - Parameters in RETURN clause
  - Function + parameter combinations
  - Nested functions with parameters
  - CASE expressions with parameters
  - Relationship traversal with parameters
  - Aggregation with parameters
  - Edge cases (coalesce, multiple functions)

**Example**:
```python
# tests/e2e/test_bolt_e2e.py
def test_bolt_connection_handshake():
    driver = GraphDatabase.driver("bolt://localhost:7687")
    with driver.session() as session:
        result = session.run("MATCH (n) RETURN count(n)")
        assert result.single()[0] >= 0
```

---

## 🎯 Benchmark Tests (14 queries)

**Location**: `benchmarks/queries/suite.py`  
**Run**: `python benchmarks\queries\suite.py --scale 1`  
**Duration**: ~30-60 seconds (scale 1)

**Queries**:
1. ✅ Simple node lookup
2. ✅ Node filter with range
3. ✅ Direct relationship traversal
4. ✅ Friends of friends (named intermediate)
5. ✅ Variable-length exact 2 hops
6. ✅ Variable-length range 1-3 hops
7. ✅ Shortest path
8. ✅ Aggregation follower count
9. ✅ Parameter + filter function
10. ✅ Function + aggregation + parameter
11. ✅ Math function + parameter
12. ✅ Parameter + variable path
13. ✅ User post count
14. ✅ Active users with followers

**Disabled** (requires anonymous node support):
- ⏸️ Multi-hop with anonymous intermediate node
- ⏸️ Mutual follows (cyclic pattern)

---

## 📊 Test Coverage Summary

| Category | Count | Status | Duration |
|----------|-------|--------|----------|
| Rust Unit Tests | 434 | ✅ 100% | ~5-8s |
| Rust Integration Tests | 12 | ✅ 100% | ~2-3s |
| Python Integration Tests | 318 | ✅ 100% | ~20-40s |
| Python E2E Tests | 21 | ✅ 100% | ~10-20s |
| Benchmark Queries | 14/16 | ✅ 87.5% | ~30-60s |
| **Total** | **799** | **✅ 99%** | **~1-2 min** |

---

## 🔄 CI/CD Integration

### GitHub Actions Workflow (TODO)
```yaml
name: Regression Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Setup Rust
        uses: actions-rs/toolchain@v1
      - name: Setup Python
        uses: actions/setup-python@v4
      - name: Start ClickHouse
        run: docker-compose up -d
      - name: Run all tests
        run: ./scripts/test/run_all_tests.sh
```

---

## 🐛 Debugging Failed Tests

### Rust Tests
```powershell
# Run with output
cargo test --lib -- --nocapture

# Run specific test
cargo test test_simple_match_query -- --nocapture

# Run with backtrace
$env:RUST_BACKTRACE=1; cargo test
```

### Python Tests
```powershell
# Verbose output
pytest -v --tb=short

# Run specific test
pytest tests/integration/test_basic_queries.py::test_simple_match -v

# Show print statements
pytest -s

# Stop on first failure
pytest -x
```

---

## 📝 Adding New Tests

### Rust Unit Test
```rust
// src/my_module.rs
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_my_feature() {
        // Arrange
        let input = "MATCH (n) RETURN n";
        
        // Act
        let result = parse_query(input);
        
        // Assert
        assert!(result.is_ok());
    }
}
```

### Python Integration Test
```python
# tests/integration/test_my_feature.py
import requests
import pytest

SERVER_URL = "http://localhost:8080/query"

def test_my_feature():
    """Test description"""
    query = "MATCH (n:User) RETURN n.name LIMIT 1"
    response = requests.post(SERVER_URL, json={"query": query})
    
    assert response.status_code == 200
    result = response.json()
    assert "results" in result
    assert len(result["results"]) == 1
```

---

## 🔍 Test Data Setup

### ClickHouse Test Data
```powershell
# Load benchmark data (scale 1 = 1K users)
python benchmarks\data\setup_unified.py --scale 1

# Load scale 10 (10K users)
python benchmarks\data\setup_unified.py --scale 10
```

### Test Schema
**Standard Schema**: `benchmarks/schemas/social_benchmark.yaml`

**Tables**:
- `users_bench` (node)
- `user_follows_bench` (relationship)
- `posts_bench` (node)
- `post_likes_bench` (relationship)

---

## 📖 Related Documentation

- `DEVELOPMENT_PROCESS.md` - 5-phase feature development workflow
- `STATUS.md` - Current project status and test statistics
- `KNOWN_ISSUES.md` - Known limitations and TODOs
- `benchmarks/README.md` - Benchmark suite documentation
- `tests/integration/README.md` - Integration test setup

---

## ❓ FAQ

**Q: Why do some tests fail locally but pass in CI?**  
A: Check that ClickHouse is running and the test schema is loaded.

**Q: How do I run tests in parallel?**  
A: Rust tests run in parallel by default. For Python: `pytest -n auto` (requires pytest-xdist)

**Q: Can I run tests without starting the server manually?**  
A: Yes, use `.\scripts\test\run_all_tests.ps1` - it auto-starts the server if needed.

**Q: What's the difference between integration and E2E tests?**  
A: Integration tests validate individual features via HTTP API. E2E tests validate complete workflows (Bolt protocol, cache behavior) with real clients.

---

**Questions?** Open an issue on GitHub or see `CONTRIBUTING.md`


