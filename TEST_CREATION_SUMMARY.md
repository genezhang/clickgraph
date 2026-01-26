# Test Creation Summary: WITH CTE Node Expansion

## What Was Created

### 1. Comprehensive Test Suite 
**File**: `tests/integration/test_with_cte_node_expansion.py`

- **9 test classes** covering all identified scenarios
- **~15 test methods** with detailed assertions
- **~600 lines** of well-documented pytest code
- Uses existing conftest fixtures and helpers
- Follows codebase testing patterns

### 2. Test Runner Script
**File**: `scripts/test/run_with_cte_tests.sh`

- Bash script with colored output
- Checks server availability before running tests
- Supports options: `-v` (verbose), `-t` (specific test), `-s` (show SQL)
- Automatic environment configuration
- Clear help message with examples

### 3. Test Documentation
**File**: `tests/integration/TEST_WITH_CTE_DOCUMENTATION.md`

- Detailed explanation of each test scenario
- Failure modes and troubleshooting guide
- Running instructions with examples
- Integration with CI/CD information
- Links to design documentation

---

## Test Coverage Map

### Scenario → Test Class → Test Methods

```
1. Basic WITH Node Export
   └─ TestWithBasicNodeExpansion
      └─ test_with_single_node_export()

2. Multi-Variable WITH Export
   └─ TestWithMultipleVariableExport
      ├─ test_with_two_node_export()
      └─ test_with_three_node_export()

3. WITH Chaining
   └─ TestWithChaining
      ├─ test_with_chaining_two_levels()
      └─ test_with_chaining_three_levels()

4. WITH Scalar Export
   └─ TestWithScalarExport
      ├─ test_with_scalar_count()
      └─ test_with_scalar_and_node()

5. WITH Property Rename
   └─ TestWithPropertyRename
      ├─ test_with_node_rename()
      └─ test_with_multi_rename()

6. Cross-Table WITH
   └─ TestWithCrossTable
      ├─ test_with_cross_table_multi_hop()
      └─ test_with_where_filter()

7. Optional Match with WITH
   └─ TestWithOptionalMatch
      └─ test_optional_match_with_export()

8. Polymorphic Node Labels
   └─ TestWithPolymorphicLabels
      └─ test_with_multi_label_node()

9. Denormalized Edges
   └─ TestWithDenormalizedEdges
      └─ test_with_denormalized_properties()

Regression Tests
   └─ TestWithRegressionCases
      ├─ test_base_table_expansion_unchanged()
      ├─ test_property_access_unchanged()
      ├─ test_aggregation_without_with()
      └─ test_multi_hop_without_with()
```

---

## Files Created/Modified

### New Files
```
tests/integration/test_with_cte_node_expansion.py     (+600 lines)
scripts/test/run_with_cte_tests.sh                    (+200 lines, executable)
tests/integration/TEST_WITH_CTE_DOCUMENTATION.md      (+400 lines)
```

### Related Documentation (Already Created)
```
DESIGN_REVIEW_WITH_CTE_FIX.md                         (comprehensive review)
PRE_MERGE_VERIFICATION_CHECKLIST.md                   (verification steps)
REVIEW_SUMMARY.md                                     (executive summary)
```

---

## Quick Start

### Run All Tests
```bash
cd /home/gz/clickgraph

# Using pytest directly
cd tests/integration
pytest test_with_cte_node_expansion.py -v

# Using test runner
../scripts/test/run_with_cte_tests.sh
```

### Run Specific Test
```bash
# Run one test class
pytest test_with_cte_node_expansion.py::TestWithBasicNodeExpansion -v

# Run one test method
pytest test_with_cte_node_expansion.py::TestWithBasicNodeExpansion::test_with_single_node_export -v

# Run with verbose output
./scripts/test/run_with_cte_tests.sh --verbose

# Run specific test by name
./scripts/test/run_with_cte_tests.sh --test test_with_chaining
```

### Debug Options
```bash
# Show SQL generation
pytest test_with_cte_node_expansion.py -v -s

# Show detailed errors
pytest test_with_cte_node_expansion.py -v --tb=long

# Show debug output
./scripts/test/run_with_cte_tests.sh --verbose --show-sql
```

---

## Test Design Principles

### 1. Clear Separation of Concerns
- Each test class covers one scenario
- Each test method tests one assertion
- Clear naming: `test_<scenario>_<variation>`

### 2. Use Existing Infrastructure
- Leverages conftest.py fixtures
- Uses execute_cypher() helper
- Follows assertion patterns from other tests

### 3. Comprehensive Coverage
- Tests basic functionality (scenario 1)
- Tests complex scenarios (scenarios 6, 9)
- Tests edge cases (scenarios 8, 9)
- Includes regression tests

### 4. Verifiable Assertions
- Checks column presence: `assert_column_exists()`
- Checks row count: `assert_row_count()`
- Checks specific values: `get_single_value()`
- Checks success: `assert_query_success()`

### 5. Clear Failure Messages
- Each assertion includes failure message
- Column lists printed on failure
- Schema names explicit in queries

---

## Schema Requirements

All tests use **social_benchmark** schema:

**Tables** (in `brahmand` database):
- `users_bench` - User nodes
- `posts_bench` - Post nodes
- `user_follows_bench` - FOLLOWS relationships
- `post_likes_bench` - LIKES relationships

**Mappings**:
- User properties: user_id → user_id, name → full_name, email → email_address
- Available in: `benchmarks/social_network/schemas/social_benchmark.yaml`

---

## Expected Test Results

### Before Fix
```
FAILED test_with_cte_node_expansion.py::TestWithBasicNodeExpansion::test_with_single_node_export
AssertionError: Expected multiple a.* columns, got: ['with_a_cte_0.a']
```

### After Fix
```
PASSED test_with_cte_node_expansion.py::TestWithBasicNodeExpansion::test_with_single_node_export
PASSED test_with_cte_node_expansion.py::TestWithMultipleVariableExport::test_with_two_node_export
PASSED test_with_cte_node_expansion.py::TestWithChaining::test_with_chaining_two_levels
PASSED test_with_cte_node_expansion.py::TestWithScalarExport::test_with_scalar_count
... (all 15 tests passing)
```

---

## Test Execution Flow

### 1. Server Check
```
📋 Checking server availability...
  ClickGraph (http://localhost:8080): ✓ Running
  ClickHouse (http://localhost:8123): ✓ Running
```

### 2. Schema Load
```
✓ Using multi-schema config: schemas/test/unified_test_multi_schema.yaml
✓ Loaded schema: social_benchmark
```

### 3. Test Execution
```
🧪 Running tests...
TestWithBasicNodeExpansion::test_with_single_node_export PASSED
TestWithMultipleVariableExport::test_with_two_node_export PASSED
... (all tests run)
```

### 4. Summary
```
✓ All tests passed!
Test Categories Verified:
  ✓ Basic node expansion
  ✓ Multi-variable exports
  ✓ WITH chaining (nested CTEs)
  ✓ Scalar aggregates (no expansion)
  ✓ Property renames
  ✓ Cross-table patterns
  ✓ Optional match + WITH
  ✓ Polymorphic labels (edge case)
  ✓ Denormalized edges (edge case)
  ✓ Regression: Base table expansion
```

---

## Integration with CI/CD

### GitHub Actions (Example)
```yaml
- name: Run WITH CTE Tests
  run: |
    cd tests/integration
    pytest test_with_cte_node_expansion.py -v --tb=short
```

### Pre-Merge Gate
```bash
# Before merging fix/with-chaining to main
./scripts/test/run_with_cte_tests.sh
```

### Regression Detection
```bash
# After other changes
pytest test_with_cte_node_expansion.py --tb=short -q
```

---

## Next Steps

### Before Running Tests

1. ✅ **Servers running**:
   ```bash
   docker-compose up -d
   cargo run --bin clickgraph
   ```

2. ✅ **Schema available**:
   - social_benchmark loaded (conftest.py handles this)
   - Tables populated with test data

3. ✅ **Environment variables** (optional, defaults work):
   ```bash
   export CLICKGRAPH_URL=http://localhost:8080
   export CLICKHOUSE_URL=http://localhost:8123
   ```

### Running Tests

```bash
# Option 1: Direct pytest
cd tests/integration
pytest test_with_cte_node_expansion.py -v

# Option 2: Test runner script
./scripts/test/run_with_cte_tests.sh

# Option 3: Verbose with SQL
./scripts/test/run_with_cte_tests.sh --verbose --show-sql
```

### Interpreting Results

- ✅ All tests pass → Fix is working correctly
- ❌ Test fails → Issue with CTE expansion, check error message
- ⚠️ Some tests pass → Partial implementation, identify failing scenarios
- 🔴 No tests run → Server not available or schema not loaded

---

## Support & Troubleshooting

### Test Fails: "Expected multiple a.* columns"
- CTE expansion not working
- Check TypedVariable registration during planning
- Verify plan_ctx.lookup_variable() finds variable

### Test Fails: "user_id not found in columns"
- Schema property name mismatch
- Check social_benchmark.yaml for actual names
- Verify schema.get_node_properties() returns correct names

### Test Fails: "No connection to server"
- Start ClickHouse: `docker-compose up -d`
- Start ClickGraph: `cargo run --bin clickgraph`
- Wait 5 seconds for server startup

### Test Times Out
- Check server logs for errors
- Verify ClickHouse has data
- Check network connectivity

---

## Documentation Links

- **Test Code**: [test_with_cte_node_expansion.py](test_with_cte_node_expansion.py)
- **Test Runner**: [run_with_cte_tests.sh](../../scripts/test/run_with_cte_tests.sh)
- **Test Documentation**: [TEST_WITH_CTE_DOCUMENTATION.md](TEST_WITH_CTE_DOCUMENTATION.md)
- **Design Review**: [DESIGN_REVIEW_WITH_CTE_FIX.md](../../DESIGN_REVIEW_WITH_CTE_FIX.md)
- **Pre-Merge Checklist**: [PRE_MERGE_VERIFICATION_CHECKLIST.md](../../PRE_MERGE_VERIFICATION_CHECKLIST.md)

---

## Success Criteria

All tests should pass when:
1. ✅ WITH-exported nodes expand to multiple properties
2. ✅ Multiple variables in same CTE expand correctly
3. ✅ Chained CTEs expand properly
4. ✅ Scalars don't expand (single column)
5. ✅ Renamed variables expand with correct alias
6. ✅ Complex patterns work end-to-end
7. ✅ Optional match + WITH works
8. ✅ Edge cases handled gracefully
9. ✅ No regressions in base functionality

Ready to run tests! 🚀
