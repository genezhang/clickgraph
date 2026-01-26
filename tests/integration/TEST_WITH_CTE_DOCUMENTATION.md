# WITH CTE Node Expansion Tests - Documentation

## Overview

This test suite validates the fix for the **WITH CTE Node Expansion** issue (branch: `fix/with-chaining`).

**Problem**: When using `RETURN` with a variable exported from a WITH clause, the variable was not being expanded to its properties. Only the variable alias was returned.

**Solution**: Use TypedVariable system to determine when a variable comes from a CTE (WITH), then expand its properties using schema lookups (same as base tables).

**Test Location**: `tests/integration/test_with_cte_node_expansion.py`

---

## Test Scenarios (9 Total)

### 1. Basic WITH Node Export ✅
**File**: `TestWithBasicNodeExpansion`

Tests simple WITH export of a single node.

```cypher
MATCH (a:User)
WITH a
RETURN a
```

**Expected**: Variable `a` expands to multiple columns (a.user_id, a.name, a.email, etc.)

**Why it tests the fix**:
- Verifies that WITH-exported nodes expand (not just `with_*_cte_0.a`)
- Single variable, simple case - baseline for more complex scenarios

---

### 2. Multi-Variable WITH Export ✅
**File**: `TestWithMultipleVariableExport`

Tests WITH exporting multiple related nodes in one CTE.

```cypher
MATCH (a:User)-[r:FOLLOWS]->(b:User)
WITH a, b
RETURN a, b
```

**Expected**: Both `a` and `b` expand to properties

**Why it tests the fix**:
- Validates CTE name parsing (with_a_b_cte_1) extracts multiple aliases
- Confirms each alias gets correct CTE column names (a_user_id, b_user_id, etc.)
- Tests the key scenario from the original bug report

---

### 3. WITH Chaining ✅
**File**: `TestWithChaining`

Tests nested WITH clauses (multi-level CTE nesting).

```cypher
MATCH (a:User)
WITH a
MATCH (b:User)
WITH a, b
RETURN a, b
```

**Expected**: Both variables expand (a from nested CTE, b from base table)

**Why it tests the fix**:
- Validates that chained CTEs work correctly
- Tests mixed sources: CTE-sourced (a) + base-table (b)
- Ensures second-level CTE names are parsed correctly
- Covers the `test_with_chaining` regression case

---

### 4. WITH Scalar Export ✅
**File**: `TestWithScalarExport`

Tests aggregation functions in WITH (should NOT expand).

```cypher
MATCH (a:User)
WITH COUNT(a) AS user_count
RETURN user_count
```

**Expected**: Scalar variable is NOT expanded (single column, not multiple)

**Why it tests the fix**:
- Validates TypedVariable.is_scalar() works correctly
- Ensures scalars take different code path than entities
- Prevents incorrect expansion of aggregates
- Regression test for property expansion logic

---

### 5. WITH Property Rename ✅
**File**: `TestWithPropertyRename`

Tests WITH using AS to rename variables.

```cypher
MATCH (a:User)
WITH a AS person
RETURN person
```

**Expected**: Variable expands as `person.*` (not `a.*`)

**Why it tests the fix**:
- Validates that renamed variables expand with correct alias
- Tests CTE column generation (person_user_id, person_name, etc.)
- Ensures aliases are correctly tracked through planning

---

### 6. Cross-Table WITH ✅
**File**: `TestWithCrossTable`

Tests complex WITH with multiple node types and hops.

```cypher
MATCH (a:User)-[:FOLLOWS]->(b:User)
WITH a, b
MATCH (c:Post) WHERE c.user_id = a.user_id
RETURN a, b, c
```

**Expected**: All three variables expand (a, b from CTE + c from base table)

**Why it tests the fix**:
- Complex real-world scenario with mixed sources
- Tests property access on CTE-sourced variables (a.user_id in WHERE)
- Validates multi-hop traversal with WITH
- Regression test for filter compatibility

---

### 7. Optional Match with WITH ✅
**File**: `TestWithOptionalMatch`

Tests OPTIONAL MATCH followed by WITH.

```cypher
MATCH (a:User)
OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
WITH a, b
RETURN a, b
```

**Expected**: Both expand (b may be NULL)

**Why it tests the fix**:
- Validates TypedVariable source detection with optional patterns
- Ensures NULL handling in expansion
- Tests LEFT JOIN + WITH interaction
- Regression test for optional match + WITH

---

### 8. Polymorphic Node Labels ✅
**File**: `TestWithPolymorphicLabels`

Tests WITH when nodes might have multiple labels (edge case).

```cypher
MATCH (a:User)
WITH a
RETURN a

-- or

MATCH (p:Post)
WITH p
RETURN p
```

**Expected**: Correct properties for node type

**Why it tests the fix**:
- Validates schema.get_node_properties() handles labels correctly
- Tests with different node types (User vs Post)
- Edge case: ensures TypedVariable tracks labels properly
- Regression test for polymorphic scenarios

---

## Running the Tests

### Prerequisites

1. **Start ClickHouse**:
   ```bash
   docker-compose up -d
   ```

2. **Start ClickGraph**:
   ```bash
   cargo build && cargo run --bin clickgraph
   ```

3. **Verify servers**:
   ```bash
   curl http://localhost:8080/health
   curl http://localhost:8123/ping
   ```

### Run All Tests

```bash
cd tests/integration
pytest test_with_cte_node_expansion.py -v
```

Or use the test runner:

```bash
./scripts/test/run_with_cte_tests.sh
```

### Run Specific Test Class

```bash
pytest test_with_cte_node_expansion.py::TestWithBasicNodeExpansion -v
```

### Run Specific Test

```bash
pytest test_with_cte_node_expansion.py::TestWithBasicNodeExpansion::test_with_single_node_export -v
```

### Show SQL Generation

```bash
pytest test_with_cte_node_expansion.py -v -s
```

### Run with Verbose Output

```bash
./scripts/test/run_with_cte_tests.sh --verbose
```

---

## Test Results Interpretation

### Success Criteria

Each test verifies:

1. **Query executes successfully** (no HTTP errors)
2. **Expected columns are present** (expansion happened)
3. **Correct number of columns** (all properties included)
4. **Column naming convention** follows `<alias>.<property>`

### Example: Successful Output

```
TestWithBasicNodeExpansion::test_with_single_node_export PASSED
✓ a expands to: a.user_id, a.full_name, a.email_address, a.registration_date, a.is_active, a.country, a.city
```

### Example: Failed Output

```
TestWithBasicNodeExpansion::test_with_single_node_export FAILED
AssertionError: Expected multiple a.* columns, got: ['with_a_cte_1.a']
```

**Indicates**: CTE expansion not working - variable staying as single alias

---

## Schema Used

All tests use **social_benchmark** schema from `benchmarks/social_network/schemas/social_benchmark.yaml`:

**Nodes**:
- `User` (user_id, full_name, email_address, registration_date, is_active, country, city)
- `Post` (post_id, content, created_at, user_id)

**Relationships**:
- `FOLLOWS` (from User to User, follow_date)
- `AUTHORED` (from User to Post, created_at)
- `LIKES` (from User to Post, liked_at)

---

## Expected Behavior by Test

| Test | Variables | Source | Expected Expansion |
|------|-----------|--------|-------------------|
| 1. Basic | a | CTE | ✅ Multiple properties |
| 2. Multi | a, b | CTE | ✅ Both expand |
| 3. Chaining | a (CTE), b (base) | Mixed | ✅ Both expand |
| 4. Scalar | count | CTE | ❌ NOT expanded (scalar) |
| 5. Rename | person (renamed a) | CTE | ✅ Expand as person.* |
| 6. Cross | a, b (CTE), c (base) | Mixed | ✅ All expand |
| 7. Optional | a, b (optional) | Mixed | ✅ Both expand |
| 8. Polymorphic | type-specific nodes | Base | ✅ Type-correct props |
| 9. Denormalized | nodes in edges | Complex | ✅ Expand via mapping |

---

## Failure Modes & Troubleshooting

### Failure: "Expected multiple a.* columns, got: ['with_a_cte_1.a']"

**Cause**: CTE expansion not working - variable returned as single alias

**Debug Steps**:
1. Check that TypedVariable is populated during planning
2. Verify CTE column generation matches assumption: `{alias}_{db_column}`
3. Run with `-s` flag to see generated SQL
4. Check logs for TypedVariable lookup

**Fix**: Verify `expand_cte_entity()` is being called

---

### Failure: "assert len(a_columns) >= 2"

**Cause**: Not enough properties returned

**Debug Steps**:
1. Check schema loading: `pytest conftest.py -v` to verify schemas
2. Verify social_benchmark schema has expected properties
3. Run base table test (test 1 in regression) to confirm schema works
4. Check SQL generation with `--show-sql`

**Fix**: Verify schema is correctly loaded and properties accessible

---

### Failure: "user_id not found in columns"

**Cause**: Property name mismatch in schema

**Debug Steps**:
1. Check social_benchmark.yaml for actual property name
2. Verify schema.get_node_properties() returns correct names
3. Run direct SQL query to check ClickHouse schema

**Fix**: Update test expectations to match actual schema property names

---

### Failure: TypeError/AttributeError in test

**Cause**: API response format issue

**Debug Steps**:
1. Check ClickGraph response structure
2. Verify execute_cypher() helper returns expected format
3. Add print(response) before assertion

**Fix**: Debug helper function or API compatibility

---

## Integration with CI/CD

### Pre-Merge Checklist

- [ ] All 9 tests pass on fix/with-chaining branch
- [ ] No regressions in existing tests
- [ ] CTE column naming verified with debug output
- [ ] Multi-hop patterns (test 3, 6) verified
- [ ] Edge cases (test 8, 9) handled gracefully

### Test Command for CI

```bash
# Run all WITH CTE tests
cd /home/gz/clickgraph/tests/integration
pytest test_with_cte_node_expansion.py -v --tb=short

# Expected output: 9 test classes, ~15 test functions
# Expected result: All passing ✓
```

---

## Test Maintenance

### When to Update Tests

1. **Schema changes**: Update property expectations
2. **API changes**: Update execute_cypher() calls
3. **New patterns**: Add new test class
4. **Bug fixes**: Add regression test

### Adding New Tests

1. Create test class in `test_with_cte_node_expansion.py`
2. Follow naming: `TestWith<Scenario>`
3. Add docstring explaining what's being tested
4. Use existing fixtures and helpers
5. Update this documentation

---

## Documentation Links

- **Design Review**: [DESIGN_REVIEW_WITH_CTE_FIX.md](DESIGN_REVIEW_WITH_CTE_FIX.md)
- **Pre-Merge Checklist**: [PRE_MERGE_VERIFICATION_CHECKLIST.md](PRE_MERGE_VERIFICATION_CHECKLIST.md)
- **Implementation Notes**: [notes/with-cte-node-expansion-issue.md](notes/with-cte-node-expansion-issue.md)
- **Social Network Schema**: [benchmarks/social_network/schemas/social_benchmark.yaml](benchmarks/social_network/schemas/social_benchmark.yaml)

---

## Questions?

If tests fail:

1. Check server logs: `docker logs` and ClickGraph console output
2. Run individual test with `-s` flag to see SQL
3. Use `sql_only` mode to verify generated SQL
4. Check schema loading with `pytest conftest.py -v`

For implementation questions, see DESIGN_REVIEW_WITH_CTE_FIX.md for architecture details.
