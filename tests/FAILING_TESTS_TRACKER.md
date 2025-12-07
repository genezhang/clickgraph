# Failing Tests Tracker

**Created**: December 7, 2025  
**Last Updated**: December 7, 2025
**Purpose**: Track and prioritize remaining failing tests for systematic resolution

---

## Summary

| Category | Passed | Failed | Errors | Total | Notes |
|----------|--------|--------|--------|-------|-------|
| **E2E Tests** | 24 | 0 | 0 | 24 | 100% pass rate ✅ |
| **Integration Tests** | 110 | 8 | - | 118 | 93% pass rate |
| **Unit Tests** | 588 | 0 | 0 | 588 | 100% pass rate ✅ |

---

## E2E Test Failures (Priority 1)

### ~~Issue 1: Bolt E2E Tests - Missing Fixture~~ ✅ FIXED
**Fixed**: December 7, 2025  
**Solution**: Added `tests/e2e/conftest.py` with Neo4j driver fixture

---

### ~~Issue 2: Param Func Tests - Column Name Format~~ ✅ FIXED  
**Fixed**: December 7, 2025  
**Solution**: Updated tests to use `u.age` (with table prefix) instead of `age`

---

### ~~Issue 3: FK-Edge JOIN Direction Bug~~ ✅ FIXED
**Fixed**: December 7, 2025  

**Problem**: For FK-edge patterns where `edge_table == to_node_table`, the JOIN condition was incorrectly generating:
```sql
INNER JOIN orders AS o ON o.id = u.user_id  -- WRONG
```

Instead of the correct:
```sql
INNER JOIN orders AS o ON o.user_id = u.id  -- CORRECT
```

**Root Cause**: The `FkEdgeJoin` strategy didn't track which node table IS the edge table. The key insight is that for FK-edge patterns, there are only 2 physical tables (not 3 like Traditional), so we only need ONE join to the node that ISN'T the edge table.

**Solution**:
1. Added `join_side: NodePosition` to `JoinStrategy::FkEdgeJoin` to indicate which node needs to be JOINed
2. `join_side=Left`: edge IS right node table, JOIN left node
3. `join_side=Right`: edge IS left node table, JOIN right node
4. Uses standard `from_id`/`to_id` semantics from schema (no special `fk_on_right` flag)

**Files Modified**:
- `src/graph_catalog/pattern_schema.rs` - Refactored `FkEdgeJoin` variant
- `src/query_planner/analyzer/graph_join_inference.rs` - Updated JOIN generation

---

## Integration Test Failures (Priority 2)

### Currently Known Issues from KNOWN_ISSUES.md

#### TODO-9: CTE Column Aliasing Bug
**Status**: 🔴 Active  
**File**: `src/clickhouse_query_generator/` (CTE handling)

**Problem**: When RETURN references both WITH aliases AND node properties, the JOIN condition uses wrong column names.

**Example Failing Query**:
```cypher
MATCH (a:User)-[:FOLLOWS]->(b:User)
WITH a, COUNT(b) as follows
WHERE follows > 1
RETURN a.name, follows
ORDER BY a.name
```

**Affected Tests**:
- `test_having_count`
- `test_having_avg`
- `test_having_multiple_conditions`
- `test_where_on_grouped_result`
- `test_case_on_relationship_count`

---

## Test File Locations

| Test Category | Location | Pass Rate |
|---------------|----------|-----------|
| E2E Tests | `tests/e2e/` | 71% (17/24) |
| Integration Tests | `tests/integration/` | 93% (110/118) |
| Unit Tests | `tests/unit/` + `src/**/tests/` | 100% (588/588) |
| Bolt Protocol | `tests/integration/bolt/` | 100% |
| Security Graph | `tests/integration/` | 100% (98/98) |

---

## Priority Queue

### Immediate (Today)
1. [ ] Fix Bolt E2E fixture (Issue 1) - 15 min
2. [ ] Fix Param Func tests (Issue 2 - Option A) - 30 min

### This Week
3. [ ] Investigate CTE column aliasing (TODO-9)
4. [ ] Run full integration test suite and update tracker

### Next Sprint
5. [ ] Consider Neo4j-compatible column naming (Issue 2 - Option C)
6. [ ] Add comprehensive E2E test coverage

---

## How to Run Tests

```bash
# E2E tests only
python3 -m pytest tests/e2e/ -v

# Integration tests
python3 -m pytest tests/integration/ -v --tb=short

# Specific failing test
python3 -m pytest tests/e2e/test_param_func_e2e.py::TestParameterFunctionBasics::test_function_in_return_with_parameter_filter -v

# Unit tests (Rust)
cargo test
```

---

## Update Log

| Date | Update |
|------|--------|
| Dec 7, 2025 | Created tracker with E2E analysis |
