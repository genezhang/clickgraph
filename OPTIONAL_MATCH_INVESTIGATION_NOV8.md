# OPTIONAL MATCH Investigation - November 8, 2025

## Session Summary

**Goal**: Improve OPTIONAL MATCH test coverage (tests were added but never all passing)  
**Historical Baseline**: 12/27 tests passing (44%) - Nov 7, 2025 (PYTHON_TEST_STATUS.md)  
**Session Start**: 17/27 tests passing (63%) - after optimizer fix  
**Current Status**: 19/27 tests passing (70.4%)  
**Total Progress**: +7 tests from historical baseline (+26% improvement)  
**Session Progress**: +2 tests fixed with join_use_nulls setting

## Fixes Applied

### 1. Anchor Node Selection Removal ✅
- **Action**: Deleted `anchor_node_selection.rs` optimizer (362 lines)
- **Reason**: Was disabled because it broke queries; ClickHouse handles JOIN reordering better
- **Files**: 
  - Deleted: `brahmand/src/query_planner/optimizer/anchor_node_selection.rs`
  - Modified: `mod.rs`, `errors.rs` (cleaned references)
- **Result**: Cleaner codebase, no functionality lost

### 2. Optimizer is_optional Flag Preservation ✅
- **Problem**: `FilterIntoGraphRel` optimizer was destroying `is_optional` flag when pushing filters
- **Root Cause**: When creating new GraphRel nodes, optimizer used `is_optional: None`
- **Fix**: Changed 3 creation sites to preserve flag:
  ```rust
  // OLD:
  is_optional: None,
  
  // NEW:
  is_optional: graph_rel.is_optional,  // Preserve optional flag
  ```
- **Files**: `brahmand/src/query_planner/optimizer/filter_into_graph_rel.rs` (lines 89, 130, 437)
- **Impact**: LEFT JOIN generation now preserved through optimizer passes

### 3. ClickHouse join_use_nulls Configuration ✅
- **Problem**: ClickHouse returns default values (empty strings) instead of NULL for unmatched LEFT JOIN columns
- **User Insight**: "there is a setting for ClickHouse that return NULL instead of empty string"
- **Fix**: Added `.with_option("join_use_nulls", "1")` to ClickHouse client
  ```rust
  Client::default()
      .with_url(url)
      .with_user(user)
      .with_password(password)
      .with_database(database)
      .with_option("join_use_nulls", "1")  // Return NULL for unmatched LEFT JOIN columns
      .with_option("allow_experimental_json_type", "1")
      .with_option("input_format_binary_read_json_as_string", "1")
      .with_option("output_format_binary_write_json_as_string", "1")
  ```
- **File**: `brahmand/src/server/clickhouse_client.rs` (line 21)
- **Impact**: Fixed 2 tests that were expecting NULL but getting empty strings

## Remaining Issues (8 Failures)

### Issue 1: Required MATCH before OPTIONAL (3 failures)
**Failing Tests**:
- `test_optional_match_incoming_relationship`
- `test_optional_then_required`
- `test_interleaved_required_optional`

**Problem**: When query has `MATCH (a) ... OPTIONAL MATCH (b)-[]->(a)`, the SQL starts FROM the wrong node.

**Example**:
```cypher
MATCH (a:User) WHERE a.name = 'Alice'
OPTIONAL MATCH (b:User)-[:FOLLOWS]->(a)
RETURN a.name, b.name
```

**Current SQL** (WRONG):
```sql
SELECT a.name, b.name
FROM test_integration.users AS b          -- ❌ Starting from OPTIONAL node
LEFT JOIN test_integration.follows AS rel ON rel.follower_id = b.user_id
LEFT JOIN test_integration.users AS a ON a.user_id = rel.followed_id
WHERE a.name = 'Alice'                    -- Filter on what should be the base table
```

**Expected SQL** (CORRECT):
```sql
SELECT a.name, b.name
FROM test_integration.users AS a          -- ✅ Start from REQUIRED node
WHERE a.name = 'Alice'
LEFT JOIN test_integration.follows AS rel ON rel.followed_id = a.user_id
LEFT JOIN test_integration.users AS b ON b.user_id = rel.follower_id
```

**Root Cause**: Query planner doesn't track which nodes are from required MATCH vs OPTIONAL MATCH. It treats all GraphRel nodes equally when deciding the FROM table.

**Impact**: Query returns 0 rows instead of 1 row with NULLs because the WHERE filter on a required node happens after the LEFT JOIN.

### Issue 2: Chained OPTIONAL MATCH NULL Propagation (3 failures)
**Failing Tests**:
- `test_optional_match_all_nulls`
- `test_two_optional_matches_one_missing`
- `test_optional_variable_length_no_path`

**Problem**: When first OPTIONAL MATCH fails (returns NULL), second OPTIONAL MATCH still generates rows, creating a Cartesian product.

**Example**:
```cypher
MATCH (a:User) WHERE a.name = 'Eve'
OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
OPTIONAL MATCH (b)-[:FOLLOWS]->(c:User)
RETURN a.name, b.name, c.name
```

**Expected**: 1 row `(Eve, NULL, NULL)` because Eve doesn't follow anyone, so b is NULL, and (b)-[:FOLLOWS]->(c) should also be NULL.

**Current**: 8 rows with all combinations:
```
(Eve, NULL, NULL)
(Alice, Bob, Charlie)
(Alice, Bob, NULL)
... (6 more rows)
```

**Root Cause**: The second OPTIONAL MATCH is being treated independently instead of depending on the first. When b is NULL from the first OPTIONAL MATCH, the second `(b)-[:FOLLOWS]->(c)` should not generate any matches (c should also be NULL).

**Impact**: Cartesian product of all possible matches instead of proper NULL propagation.

### Issue 3: Variable-Length OPTIONAL MATCH (2 failures)
**Failing Tests**:
- `test_optional_variable_length_exists`
- `test_optional_variable_length_no_path`

**Problem**: Combination of OPTIONAL MATCH with variable-length paths.

**Status**: Likely related to one of the above issues, but needs separate investigation.

## Technical Analysis

### The Two-Part Fix That Works ✅
1. **Optimizer Fix**: Preserve `is_optional` flag through optimizer transformations
2. **ClickHouse Config**: Enable `join_use_nulls=1` for proper NULL handling

These fixes addressed the basic OPTIONAL MATCH functionality and got us from 17/27 to 19/27.

### Architectural Gaps Revealed 🔍

The remaining 8 failures reveal two fundamental architectural issues:

#### 1. No Query Planner Context for Required vs Optional Nodes
- **Current Behavior**: All nodes treated equally when generating SQL
- **Missing**: Tracking which nodes come from required MATCH vs OPTIONAL MATCH clauses
- **Impact**: Can't determine correct FROM table when mixing required and optional patterns
- **Fix Required**: 
  - Track node origin (required vs optional) in query planner context
  - Use this info when generating SQL to ensure required nodes are in FROM clause
  - Make optional nodes only appear in LEFT JOINs

#### 2. No NULL Propagation for Chained OPTIONAL MATCH
- **Current Behavior**: Each OPTIONAL MATCH is independent
- **Missing**: Understanding that second OPTIONAL depends on variables from first OPTIONAL
- **Impact**: When first OPTIONAL returns NULL, second still tries to match, causing Cartesian product
- **Fix Required**:
  - Detect dependent OPTIONAL MATCH clauses (second references variables from first)
  - Generate SQL that ensures NULL propagates: if b is NULL, don't try to match (b)-[]->(c)
  - Possible approaches:
    - Nested LEFT JOINs with proper ON conditions
    - LATERAL JOINs
    - Subqueries with WHERE IS NOT NULL checks

## Test Breakdown

**19 Passing Tests** ✅ (Working scenarios):
- Single OPTIONAL MATCH with outgoing relationships
- OPTIONAL MATCH with WHERE filters
- OPTIONAL MATCH with aggregation
- OPTIONAL MATCH with DISTINCT
- Some edge cases (no base match, with LIMIT)
- Chained OPTIONAL when both succeed

**8 Failing Tests** ❌ (Need architectural fixes):
1. `test_optional_match_incoming_relationship` - Required before optional (incoming)
2. `test_two_optional_matches_one_missing` - NULL propagation issue
3. `test_optional_then_required` - Required vs optional order
4. `test_interleaved_required_optional` - Mixed required/optional
5. `test_optional_variable_length_exists` - Variable-length with optional
6. `test_optional_variable_length_no_path` - Variable-length NULL handling
7. `test_optional_match_all_nulls` - Chained optional both NULL
8. `test_optional_match_self_reference` - Self-reference with optional

## Next Steps

### Priority 1: Required MATCH Context Tracking
**Estimated Effort**: 2-3 hours

**Approach**:
1. Add `match_type` field to nodes in query planner: `Required | Optional`
2. When analyzing MATCH clauses, mark nodes as `Required`
3. When analyzing OPTIONAL MATCH clauses, mark nodes as `Optional`
4. In SQL generation:
   - FROM clause: Always use a `Required` node
   - LEFT JOINs: For all `Optional` nodes
5. Test with `test_optional_match_incoming_relationship`

**Files to Modify**:
- `brahmand/src/query_planner/plan_ctx/mod.rs` - Add match_type tracking
- `brahmand/src/query_planner/analyzer/` - Mark nodes during analysis
- `brahmand/src/clickhouse_query_generator/` - Use match_type in SQL generation

### Priority 2: Chained OPTIONAL NULL Propagation
**Estimated Effort**: 3-4 hours

**Approach**:
1. Detect variable dependencies between OPTIONAL MATCH clauses
2. Generate SQL that prevents matching when dependent variable is NULL
3. Options:
   - **Option A**: Add `WHERE prev_optional_var IS NOT NULL` to ON conditions
   - **Option B**: Use LATERAL JOINs (if ClickHouse supports)
   - **Option C**: Nested subqueries with NULL checks
4. Test with `test_optional_match_all_nulls`

**Files to Modify**:
- `brahmand/src/query_planner/analyzer/` - Detect dependencies
- `brahmand/src/clickhouse_query_generator/` - Generate conditional JOINs

### Priority 3: Variable-Length OPTIONAL Integration
**Estimated Effort**: 1-2 hours

**Approach**:
1. Ensure variable-length path logic works with OPTIONAL semantics
2. May be automatically fixed by Priority 1 & 2
3. Test separately to confirm

## Lessons Learned

### What Worked ✅
1. **join_use_nulls discovery**: User's memory of this setting saved significant debugging time
2. **Systematic fix validation**: Each fix was validated with test runs before proceeding
3. **Root cause analysis**: Taking time to understand SQL generation revealed fundamental issues
4. **Two-part approach**: Optimizer fix + ClickHouse config addressed different aspects

### What's Revealed 🔍
1. **OPTIONAL MATCH more complex than expected**: Simple LEFT JOIN not sufficient for all cases
2. **Context tracking essential**: Need to know node origin (required vs optional)
3. **NULL propagation critical**: Dependent OPTIONAL MATCH needs special handling
4. **Test-driven discovery**: Integration tests revealed real-world usage patterns

### Recommendations 📋
1. **Add unit tests for query planner context**: Verify match_type is tracked correctly
2. **Document OPTIONAL MATCH semantics**: Clear spec for how chained OPTIONAL should work
3. **Consider Neo4j test cases**: See how they handle these edge cases
4. **Performance consideration**: Complex NULL propagation may impact query performance

## References

- **ClickHouse join_use_nulls**: https://clickhouse.com/docs/en/operations/settings/settings#join_use_nulls
- **OpenCypher OPTIONAL MATCH**: https://opencypher.org/resources/
- **Test file**: `tests/integration/test_optional_match.py`
- **Modified files**: See sections above for complete file list

---
**Session Duration**: ~2 hours  
**Test Improvement**: 17/27 → 19/27 (+2 tests, +7.4%)  
**Lines Modified**: ~50 lines across 3 files  
**Architecture Insights**: 2 fundamental gaps identified
