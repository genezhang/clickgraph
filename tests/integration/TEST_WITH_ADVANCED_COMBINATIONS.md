# WITH Clause + Advanced Features Test Suite

## Purpose
Comprehensive integration tests for WITH clause combinations with advanced ClickGraph features, based on architectural fragility analysis (Dec 12, 2025).

## Test Categories

### 1. Variable-Length Paths (VLP) + WITH
Tests CTE hoisting for recursive CTEs combined with WITH clauses:
- Basic VLP + WITH aggregation
- VLP + WITH + second MATCH
- Chained WITH after VLP

**Why**: VLP generates recursive CTEs that must be properly hoisted to top level.

### 2. OPTIONAL MATCH + WITH
Tests LEFT JOIN semantics with WITH clauses:
- WITH + OPTIONAL MATCH
- OPTIONAL MATCH + WITH aggregation  
- VLP + WITH + OPTIONAL MATCH (complex combination)

**Why**: OPTIONAL MATCH changes join semantics; WITH + OPTIONAL requires careful CTE ordering.

### 3. Multiple Relationship Types + WITH
Tests UNION-based relationship patterns with WITH:
- Alternate relationship types + WITH
- Multiple patterns + WITH aggregation

**Why**: Multiple rel types use UNION CTEs that interact with WITH CTEs.

### 4. Complex Aggregations + WITH
Tests GROUP BY expansion with TableAlias:
- WITH TableAlias + aggregation (GROUP BY expansion)
- Two-level aggregation (WITH + RETURN)
- WITH filtering on aggregates

**Why**: TableAlias in GROUP BY must expand to actual columns.

### 5. Query Modifiers + WITH
Tests interaction of WITH with ORDER BY, SKIP, LIMIT:
- WITH + ORDER BY + LIMIT
- WITH + SKIP + LIMIT

**Why**: Modifiers affect CTE structure and subquery wrapping.

### 6. CTE Hoisting Validation
Tests edge cases for CTE hoisting:
- Three-level WITH nesting
- VLP within WITH chain

**Why**: Deep nesting tests recursive hoisting logic.

### 7. Parameters + WITH
Tests parameter substitution across WITH boundaries:
- Parameter in WHERE before WITH
- Parameter in WITH WHERE clause
- Multiple parameters with WITH
- Parameter in aggregation expressions
- VLP + WITH + parameters

**Why**: Parameters ($param) should work orthogonally across all query patterns, including WITH clauses.

### 8. Regression Tests
Tests previously failing patterns:
- LDBC IC-1 pattern (VLP + WITH + aggregation)
- TableAlias GROUP BY expansion

**Why**: Prevents regression of fixed bugs.

## Running Tests

### Prerequisites
1. ClickHouse running with test database
2. ClickGraph server running with social network schema
3. Test data loaded

### Setup
```bash
# Start ClickHouse
docker-compose up -d clickhouse

# Load test data (using social_network benchmark)
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_DATABASE="brahmand"
cd benchmarks/social_network/data
python3 setup_data.py

# Start ClickGraph server
export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"
cargo run --release --bin clickgraph
```

### Run Tests
```bash
python3 tests/integration/test_with_advanced_combinations.py
```

## Test Results (Initial Run - Dec 12, 2025)

**Status**: 0/22 passing (test infrastructure works, database setup needed)

**Test Count**: 22 tests across 8 categories
- Category 1 (VLP): 3 tests
- Category 2 (OPTIONAL MATCH): 3 tests
- Category 3 (Multiple rel types): 2 tests
- Category 4 (Aggregations): 3 tests
- Category 5 (Modifiers): 2 tests
- Category 6 (Validation): 2 tests
- Category 7 (Parameters): 5 tests ⭐ NEW
- Category 8 (Regressions): 2 tests

**Known Issues Found**:
1. ✅ Three-level WITH nesting fails: "Cannot render plan with remaining WITH clauses"
   - Root cause: Chained WITH handler doesn't fully process nested WITH
   - Priority: HIGH (breaks valid queries)

2. ✅ TableAlias GROUP BY expansion: Alias like `a_connections` not resolved
   - Root cause: Composite alias handling in WITH clause renderer
   - Priority: MEDIUM

3. ⚠️ Database setup: Tests need `brahmand` database with social network data
   - Action: Add setup instructions

4. ⚠️ Parameters in VLP hop ranges: `*1..$maxHops` not supported
   - Error: "Parameters are not yet supported in properties"
   - Workaround: Use literal hop ranges, parameters work in WHERE/WITH clauses
   - Priority: LOW (rare use case, workaround available)
   - Location: `src/query_planner/logical_plan/match_clause.rs:1082`

## Success Criteria

- [ ] All 22 tests passing
- [ ] VLP CTEs properly hoisted in WITH chains
- [ ] GROUP BY correctly expands TableAlias references
- [ ] Multiple CTE ordering validated
- [ ] Parameters work correctly across WITH boundaries
- [ ] No SQL generation errors
- [ ] Query results match expected patterns

## Future Enhancements

1. Add shortest path + WITH combinations
2. Add EXISTS pattern + WITH tests
3. Add NOT pattern + WITH tests
4. Add parameter arrays with WITH (e.g., `WHERE u.id IN $userIds`)
5. Performance benchmarks for complex combinations
6. Memory usage tests for deeply nested CTEs

## References

- Architectural fragility analysis: `notes/architectural-fragility-analysis.md`
- CTE hoisting fix: Commit 755285f
- CTE validation: Commit d38d9fd
- Parameter substitution: `src/server/parameter_substitution.rs`
- Related issues: `KNOWN_ISSUES.md`
