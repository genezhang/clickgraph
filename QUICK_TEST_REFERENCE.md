# Quick Test Reference

## One-Command Test Execution

### Start Services & Run All Tests
```bash
cd /home/gz/clickgraph

# Terminal 1: Start servers
docker-compose up -d                    # ClickHouse
cargo run --bin clickgraph              # ClickGraph

# Terminal 2: Wait 5 seconds, then run tests
sleep 5
./scripts/test/run_with_cte_tests.sh
```

## Test Commands Cheat Sheet

### Run All Tests
```bash
cd /home/gz/clickgraph/tests/integration
pytest test_with_cte_node_expansion.py -v
```

### Run Specific Test Class
```bash
# Example: Basic expansion tests only
pytest test_with_cte_node_expansion.py::TestWithBasicNodeExpansion -v
```

### Run Tests by Scenario
```bash
# Scenario 1: Basic
pytest test_with_cte_node_expansion.py::TestWithBasicNodeExpansion -v

# Scenario 2: Multi-variable
pytest test_with_cte_node_expansion.py::TestWithMultipleVariableExport -v

# Scenario 3: Chaining
pytest test_with_cte_node_expansion.py::TestWithChaining -v

# Scenario 4: Scalars
pytest test_with_cte_node_expansion.py::TestWithScalarExport -v

# Scenario 5: Renames
pytest test_with_cte_node_expansion.py::TestWithPropertyRename -v

# Scenario 6: Cross-table
pytest test_with_cte_node_expansion.py::TestWithCrossTable -v

# Scenario 7: Optional match
pytest test_with_cte_node_expansion.py::TestWithOptionalMatch -v

# Scenario 8: Polymorphic
pytest test_with_cte_node_expansion.py::TestWithPolymorphicLabels -v

# Scenario 9: Denormalized
pytest test_with_cte_node_expansion.py::TestWithDenormalizedEdges -v

# Regression tests
pytest test_with_cte_node_expansion.py::TestWithRegressionCases -v
```

### Run Single Test
```bash
# Example
pytest test_with_cte_node_expansion.py::TestWithBasicNodeExpansion::test_with_single_node_export -v
```

### Run with Verbose Output
```bash
pytest test_with_cte_node_expansion.py -vv
```

### Run with Debug Output (shows stdout)
```bash
pytest test_with_cte_node_expansion.py -v -s
```

### Run with Long Traceback on Failure
```bash
pytest test_with_cte_node_expansion.py -v --tb=long
```

## Using Test Runner Script

### Run All Tests
```bash
./scripts/test/run_with_cte_tests.sh
```

### Verbose Mode
```bash
./scripts/test/run_with_cte_tests.sh --verbose
```

### Show SQL Generation
```bash
./scripts/test/run_with_cte_tests.sh --show-sql
```

### Run Specific Test
```bash
./scripts/test/run_with_cte_tests.sh --test TestWithChaining
./scripts/test/run_with_cte_tests.sh --test test_with_single_node_export
```

### Help
```bash
./scripts/test/run_with_cte_tests.sh --help
```

## Expected Output

### All Tests Passing ✅
```
====== test session starts ======
test_with_cte_node_expansion.py::TestWithBasicNodeExpansion::test_with_single_node_export PASSED
test_with_cte_node_expansion.py::TestWithMultipleVariableExport::test_with_two_node_export PASSED
test_with_cte_node_expansion.py::TestWithMultipleVariableExport::test_with_three_node_export PASSED
... (all tests)
====== 15 passed in 8.32s ======
```

### Test Failing ❌
```
AssertionError: Expected multiple a.* columns, got: ['with_a_cte_0.a']
```

This indicates CTE expansion is not working - the variable is returned as a single alias instead of expanding to properties.

## Quick Debugging

### Check Server Status
```bash
curl http://localhost:8080/health
curl http://localhost:8123/ping
```

### Check Schema is Loaded
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "USE social_benchmark MATCH (n:User) RETURN COUNT(n) LIMIT 1"}'
```

### Run Single Query
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "USE social_benchmark MATCH (a:User) WITH a RETURN a LIMIT 1"}'
```

### Check Generated SQL
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "USE social_benchmark MATCH (a:User) WITH a RETURN a LIMIT 1", "sql_only": true}'
```

## Test Scenarios at a Glance

| # | Scenario | Test Class | Key Test |
|---|----------|-----------|----------|
| 1 | Basic expansion | TestWithBasicNodeExpansion | test_with_single_node_export |
| 2 | Multi-variable | TestWithMultipleVariableExport | test_with_two_node_export |
| 3 | Chaining | TestWithChaining | test_with_chaining_two_levels |
| 4 | Scalars | TestWithScalarExport | test_with_scalar_count |
| 5 | Rename | TestWithPropertyRename | test_with_node_rename |
| 6 | Cross-table | TestWithCrossTable | test_with_cross_table_multi_hop |
| 7 | Optional | TestWithOptionalMatch | test_optional_match_with_export |
| 8 | Polymorphic | TestWithPolymorphicLabels | test_with_multi_label_node |
| 9 | Denormalized | TestWithDenormalizedEdges | test_with_denormalized_properties |

## Pre-Test Checklist

- [ ] ClickHouse running: `docker ps | grep clickhouse`
- [ ] ClickGraph running: `curl http://localhost:8080/health`
- [ ] Proper branch: `git branch | grep fix/with-chaining`
- [ ] Code compiled: `cargo build --release`
- [ ] Tests syntax valid: `python3 -m py_compile tests/integration/test_with_cte_node_expansion.py`

## After Tests

### If All Pass ✅
1. Tests verify the fix is working
2. Ready for documentation update
3. Ready for merge to main
4. Can add to changelog

### If Some Fail ❌
1. Check which scenario fails
2. Review test error message
3. Check generated SQL (use `sql_only`)
4. Debug with print statements or breakpoints
5. See TEST_WITH_CTE_DOCUMENTATION.md for troubleshooting

### If All Fail ❌
1. Check servers are running
2. Verify schema is loaded
3. Check CLICKGRAPH_URL environment variable
4. Review server logs: `docker logs clickhouse` or console output

## Common Issues & Solutions

| Issue | Solution |
|-------|----------|
| `ConnectionError: Cannot connect to http://localhost:8080` | Start ClickGraph: `cargo run --bin clickgraph` |
| `ConnectionError: Cannot connect to http://localhost:8123` | Start ClickHouse: `docker-compose up -d` |
| `AssertionError: Expected multiple a.* columns` | CTE expansion not working - check TypedVariable registration |
| `KeyError: 'results'` | API response format issue - check server logs |
| `AssertionError: status_code = 500` | Query execution failed - check schema and data |
| Timeout waiting for server | Wait longer or check logs for errors |

## Test Execution Timeline

### Scenario 1 (Basic): ~0.5 sec
- Single node expansion
- Simplest case

### Scenario 2-7 (Core): ~1.5 sec each
- 9 sec total for 6 scenarios
- Covers main use cases

### Scenario 8-9 (Edge Cases): ~1 sec each
- 2 sec total
- May gracefully fail if schemas unavailable

### Total Time: ~15 seconds for all tests

---

## Files Reference

| File | Purpose |
|------|---------|
| test_with_cte_node_expansion.py | Test suite (~600 lines) |
| run_with_cte_tests.sh | Test runner script |
| TEST_WITH_CTE_DOCUMENTATION.md | Detailed documentation |
| TEST_CREATION_SUMMARY.md | Creation summary |
| DESIGN_REVIEW_WITH_CTE_FIX.md | Design review |
| PRE_MERGE_VERIFICATION_CHECKLIST.md | Verification steps |

---

Ready to run tests! 🚀
