# Python Integration Tests - Status Report
**Date**: November 7, 2025

## Summary
Successfully enabled Python integration tests by loading the correct schema at server startup.

**Progress**: 0% → 36.4% (99/272 tests passing)

## What Was Fixed

### Issue
All 272 Python integration tests were failing with:
```
Schema error: Schema 'test_graph_schema' not found
```

### Root Cause
The ClickGraph server needs to be started with the test schema loaded via the `GRAPH_CONFIG_PATH` environment variable, but this wasn't being done.

### Solution
Start server with test schema:
```powershell
$env:GRAPH_CONFIG_PATH = "tests/integration/test_integration.yaml"
target\release\clickgraph.exe
```

The test schema (`test_integration.yaml`) defines:
- **Nodes**: User, Product
- **Relationships**: FOLLOWS, PURCHASED, FRIENDS_WITH  
- **Database**: test_integration (ClickHouse)
- **Schema Name**: test_graph_schema (logical identifier)

## Current Test Results (36.4% Passing)

| Test File | Passing | Total | % | Status |
|-----------|---------|-------|---|--------|
| test_basic_queries.py | 19 | 19 | 100% | ✅ **PERFECT** |
| test_case_expressions.py | 19 | 25 | 76% | ⚠️ Good |
| test_optional_match.py | 12 | 27 | 44% | ⚠️ Needs work |
| test_aggregations.py | 11 | 29 | 38% | ⚠️ Needs work |
| test_relationships.py | 6 | 19 | 32% | ⚠️ Needs work |
| test_variable_length_paths.py | 7 | 27 | 26% | ⚠️ Needs work |
| test_shortest_paths.py | 6 | 24 | 25% | ⚠️ Needs work |
| test_path_variables.py | 3 | 24 | 13% | ❌ Major issues |
| **TOTAL** | **99** | **272** | **36.4%** | ⚠️ **In Progress** |

## Known Failure Patterns

### 1. Aggregation with Relationships (38% passing)
**Example Query**:
```cypher
MATCH (a:User)-[:FOLLOWS]->(b:User)
RETURN a.name, COUNT(b) as follows_count
ORDER BY a.name
```

**Error**:
```
Unknown expression or function identifier `b.user_id` in scope SELECT ...
```

**Generated SQL** (incorrect):
```sql
SELECT a.name, COUNT(b.user_id) AS follows_count 
FROM test_integration.users AS a 
INNER JOIN test_integration.follows AS a7327a7489 ON a7327a7489.follower_id = a.user_id 
GROUP BY a.name 
ORDER BY a.name ASC
```

**Problem**: Node `b` is referenced in `COUNT(b.user_id)` but not in the FROM clause. The planner needs to either:
1. Add a JOIN to include node `b`, OR
2. Count the relationship itself (`COUNT(*)` or `COUNT(a7327a7489.follower_id)`)

**Root Cause**: Query planner doesn't properly handle aggregations that reference nodes from graph patterns.

### 2. Path Variable Tests (13% passing)
These are failing for reasons we've already identified in the Rust tests - they expect `map()` format but we use `tuple()` format. The Python tests may have outdated expectations similar to the Rust tests we fixed.

### 3. Variable-Length Path Tests (26% passing)
Some variable-length path queries are failing. Need to investigate specific failure modes.

### 4. OPTIONAL MATCH Tests (44% passing)
While we fixed the Rust implementation, some Python tests are still failing. Need to check if they test edge cases not covered by unit tests.

## Next Steps

### Priority 1: Fix Aggregation + Relationship Query Planning
**Impact**: Would fix ~18 failing tests in aggregations.py alone
**Estimated Effort**: 2-3 hours
**Files to modify**:
- `brahmand/src/query_planner/analyzer/` (aggregation handling)
- `brahmand/src/render_plan/plan_builder.rs` (JOIN generation for aggregations)

### Priority 2: Fix Relationship Query Issues  
**Impact**: Would fix 13 failing tests in relationships.py
**Estimated Effort**: 1-2 hours
**Investigation needed**: Check what specific relationship patterns are failing

### Priority 3: Update Path Variable Test Expectations
**Impact**: Would fix potentially 21 failing tests in path_variables.py
**Estimated Effort**: 30 minutes
**Similar to Rust test fix**: Change `map()` expectations to `tuple()` format

### Priority 4: Investigate Variable-Length Path Failures
**Impact**: Would fix 20 failing tests in variable_length_paths.py
**Estimated Effort**: 2-3 hours  
**Investigation needed**: Determine specific failure modes

## How to Run Tests

### Start Server with Test Schema
```powershell
# Stop any running server
Get-Process | Where-Object {$_.ProcessName -eq "clickgraph"} | Stop-Process -Force

# Build release
cargo build --release

# Start with test schema
$env:GRAPH_CONFIG_PATH = "tests/integration/test_integration.yaml"
target\release\clickgraph.exe
```

### Run Tests
```powershell
cd tests/integration

# All tests
python -m pytest -v

# Specific file
python -m pytest test_basic_queries.py -v

# Specific test
python -m pytest test_basic_queries.py::TestBasicMatch::test_match_all_nodes -v

# Quick summary
python -m pytest --tb=no -q
```

### Test Data
The `simple_graph` fixture in `conftest.py` automatically creates test tables and data for each test. The database is cleaned between tests.

## Success Criteria for "All Tests Passing"

To reach 100% (272/272 tests passing), we need to:

1. ✅ **Basic Queries**: Already 100% passing
2. ⚠️ **Aggregations**: Fix query planning for aggregations with relationships
3. ⚠️ **Relationships**: Fix missing relationship query patterns
4. ⚠️ **Variable-Length Paths**: Fix edge cases in variable-length path handling
5. ⚠️ **Shortest Paths**: Similar to variable-length path issues
6. ⚠️ **Path Variables**: Update test expectations to match `tuple()` format
7. ⚠️ **OPTIONAL MATCH**: Fix edge cases not covered by unit tests
8. ⚠️ **Case Expressions**: Fix remaining 6 failing tests (already 76% passing)

**Estimated Total Effort**: 8-12 hours of focused development

## Documentation Updates Needed
- Update STATUS.md with Python test results
- Document the aggregation + relationship limitation in KNOWN_ISSUES.md
- Add server startup instructions to README.md
