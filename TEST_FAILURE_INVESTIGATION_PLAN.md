# Test Failure Investigation Plan

**Status**: 2514/3319 passing (75.7%) - 805 failures remaining  
**Created**: December 21, 2025  
**Last Updated**: December 22, 2025 (Post v0.6.0 Release)  
**Goal**: Systematic investigation and resolution of remaining test failures

## Executive Summary

**Current Status**: After v0.6.0 release, we have achieved **75.7% pass rate** (2514/3319 passing).

**Recent Achievement**: Comma-separated pattern bug FIXED! ✅ Cross-table correlation now working with smart cross-branch JOIN detection. Zeek merged test suite: **22/24 passing (91.7%)**.

**Key Success**: Re-enabled selective cross-branch JOIN generation that:
- Detects shared nodes across different relationship tables (comma patterns)
- Generates proper JOINs: `FROM dns_log AS t1 INNER JOIN conn_log AS t2 ON t1.orig_h = t2.orig_h`
- Avoids duplicate JOINs for linear patterns (the reason it was disabled before)

**Next Priority**: Matrix tests (400 failures, 49.7% of total) - primarily VLP and aggregation edge cases.

---

## Recent Fix (December 21, 2025) - ✅ VERIFIED

### ✅ Comma-Separated Patterns with Cross-Table Correlation - **FIXED & VERIFIED**

**Test Results** (December 22, 2025):
- `test_zeek_merged.py`: **22/24 passing (91.7%)** ✅
- Full matrix suite: **1995/2408 passing (82.9%)**
- **Status**: Fix confirmed stable after v0.6.0 release

**What was fixed**:
```rust
// src/query_planner/analyzer/graph_join_inference.rs
// Re-enabled cross-branch JOIN with smart detection:
if prev_appearance.table_name != current_appearance.table_name {
    // Different relationship tables = comma pattern!
    generate_cross_branch_join(...)
}
```

**Technical details**:
1. Fixed `extract_node_appearance()` to use `get_rel_schema_with_nodes()` (composite key lookup)
2. Detects when shared node appears in different relationship tables
3. Only generates JOIN for true comma patterns, not linear chains
4. Reused existing sophisticated helper functions

**Remaining zeek_merged failures (2/24)**:
- `test_count_dns_requests`: Single table COUNT aggregation issue
- `test_count_connections`: Single table COUNT aggregation issue  
(These are NOT comma pattern issues - different root cause)

---

## Current Failure Analysis (December 22, 2025)

### Test Statistics Summary
| Category | Passing | Total | Pass Rate | Failures |
|----------|---------|-------|-----------|----------|
| **Overall** | 2514 | 3319 | 75.7% | 805 |
| Matrix Tests | 1995 | 2408 | 82.9% | 400 |
| Variable-Length Paths | 11 | 24 | 45.8% | 13 |
| Shortest Paths | 0 | 20 | 0% | 20 |
| Zeek Merged | 22 | 24 | 91.7% | 2 |
| Other Tests | 486 | 843 | 57.7% | 370 |

### 🎉 Recent Wins
- ✅ **Comma-separated patterns**: Fixed cross-table JOINs (December 21)
- ✅ **Matrix tests improved**: 82.9% passing (1995/2408)
- ✅ **Zeek merged schema**: **22/24 passing (91.7%)**
- ✅ **Simple comma patterns**: All basic cross-table correlations working

### 🔍 Remaining Issues by Priority

#### 1. **Matrix Tests** - **400 failures** (49.7% of all failures) - **HIGHEST PRIORITY**
**Current status**: 1995/2408 passing (82.9%)

**Breakdown by pattern type**:
- Variable-length path patterns (~250 failures estimated)
- Aggregation edge cases (~100 failures estimated)
- Complex multi-hop patterns (~50 failures estimated)

**Investigation needed**: Run matrix tests with detailed output to categorize failures:
```bash
pytest tests/integration/matrix/ -v --tb=short -k "vlp" 2>&1 | grep "FAILED" | head -20
pytest tests/integration/matrix/ -v --tb=short -k "aggregat" 2>&1 | grep "FAILED" | head -20
```

**Estimated Effort**: 5-7 days  
**Impact**: Would bring overall pass rate to ~88%

---

#### 2. **Variable-Length Paths** - **13 failures** (High Priority)
**Current status**: 11/24 passing (45.8%)

**Affected test classes**:
- `TestRangePaths`: Range patterns like `*1..3`, `*2..4` failing
- `TestUnboundedPaths`: Unbounded patterns like `*`, `*1..` failing
- `TestVariableLengthWithFilters`: VLP + WHERE clause failing
- `TestVariableLengthProperties`: Property access within VLP failing
- `TestVariableLengthAggregation`: COUNT/GROUP BY on VLP failing

**Root cause hypothesis**:
- CTE generation issues for variable-length patterns in unified schema
- Property selection within recursive CTEs not working correctly
- Aggregation over VLP results generating invalid SQL

**Estimated Effort**: 2-3 days  
**Impact**: Core graph feature, critical for graph analytics

---

#### 3. **Shortest Path Algorithms** - **20 failures** (Medium Priority)
**Current status**: 0/20 passing (0%)

**All test classes failing**:
- `TestShortestPathBasic`: Even basic `shortestPath()` calls failing
- `TestShortestPathProperties`: Property access on paths
- `TestShortestPathAggregation`: Aggregations over shortest paths
- `TestShortestPathDepth`: Depth constraints
- `TestShortestPathEdgeCases`: Multiple start nodes, unreachable paths

**Root cause hypothesis**:
- Shortest path CTE generation completely broken
- May be related to VLP CTE generation issues
- Early termination optimization not working

**Estimated Effort**: 3-4 days  
**Impact**: Specialized algorithm, important for path analysis

---

#### 4. **Other Integration Tests** - **370 failures** (Medium-Low Priority)
**Categories**:
- Wiki/tutorial tests: ~150 failures (different schema - `social_benchmark.yaml`)
- Security graph tests: ~20 failures (schema-specific)
- Aggregation edge cases: ~100 failures (SUM/AVG/COLLECT edge cases)
- Expression/function tests: ~100 failures (various)

**Estimated Effort**: 5-7 days distributed  
**Impact**: Various, many are schema-specific or edge cases

---

#### 5. **Zeek Tests** - **2 failures** (Low Priority - Easy Win!)
**Current status**: 22/24 passing (91.7%)

**Failing tests**:
- `test_count_dns_requests`: Simple COUNT on dns_log table
- `test_count_connections`: Simple COUNT on conn_log table

**Root cause hypothesis**:
- Single-table COUNT aggregation issue (not cross-table)
- May be table alias or GROUP BY issue
- Quick investigation should reveal simple fix

**Estimated Effort**: 0.5 day  
**Impact**: Would make zeek_merged 100% passing!

---

## 🎯 Immediate Next Steps (Post v0.6.0)

### Quick Win: Zeek Test Fixes (Today - 0.5 day)
**Goal**: Fix 2 remaining zeek_merged failures to achieve 100% pass rate

1. ✅ Investigation already complete (COUNT aggregation issue)
2. ⬜ Run failing tests with detailed output:
   ```bash
   pytest tests/integration/test_zeek_merged.py::TestSingleTableRequested::test_count_dns_requests -vv --tb=long
   pytest tests/integration/test_zeek_merged.py::TestSingleTableAccessed::test_count_connections -vv --tb=long
   ```
3. ⬜ Check generated SQL for COUNT queries
4. ⬜ Fix table alias or aggregation generation
5. ⬜ Verify zeek_merged 24/24 passing

**Impact**: Zeek merged suite 100% passing, validates comma-pattern fix

---

### Priority 1: Matrix Test Deep Dive (Week 1 - 5 days)
**Goal**: Categorize and fix matrix test failures (400 remaining)

**Phase 1: Investigation (Day 1-2)**
```bash
# Categorize failures by pattern type
pytest tests/integration/matrix/ -v --tb=short 2>&1 > matrix_failures_full.log

# VLP failures
grep -A 5 "test.*vlp" matrix_failures_full.log | grep "FAILED" > vlp_failures.txt

# Aggregation failures  
grep -A 5 "test.*aggr\|test.*count\|test.*sum" matrix_failures_full.log | grep "FAILED" > agg_failures.txt

# Analyze patterns
wc -l vlp_failures.txt agg_failures.txt
```

**Phase 2: VLP Fixes (Day 3-4)**
- Fix CTE generation for variable-length patterns
- Fix property selection in recursive CTEs
- Test with zeek_merged and filesystem schemas

**Phase 3: Aggregation Fixes (Day 5)**
- Fix COUNT/SUM/AVG edge cases
- Fix GROUP BY with complex expressions
- Test across all schemas

**Target**: 2700+/3319 passing (81%+)

---

### Priority 2: Variable-Length & Shortest Paths (Week 2 - 5 days)
**Goal**: Fix core graph algorithm failures (33 tests)

**VLP Tests (Day 1-2)**
```bash
pytest tests/integration/test_variable_length_paths.py -vv --tb=short 2>&1 | tee vlp_investigation.log
```

**Shortest Path Tests (Day 3-5)**
```bash
pytest tests/integration/test_shortest_paths.py -vv --tb=short 2>&1 | tee shortest_path_investigation.log
```

**Target**: 2750+/3319 passing (83%+)

---

## Investigation Workflow (Per Category)

Follow the **5-Phase Development Process** from `DEVELOPMENT_PROCESS.md`:

### Phase 1: Design (Investigation)
1. **Sample failing test**: Run 3-5 tests from category with `-vv --tb=short`
2. **Extract patterns**: Identify common error messages and SQL patterns
3. **Check logs**: Review server logs for query planning details
4. **Schema validation**: Verify schema configurations are correct
5. **Compare with working tests**: Find similar tests that pass

**Output**: Investigation notes with root cause hypothesis

### Phase 2: Implement
1. **Minimal fix**: Implement smallest change to fix one test
2. **Verify approach**: Run subset of tests to validate fix
3. **Extend fix**: Apply to related failures
4. **Code review**: Check for regressions

**Output**: Code changes committed incrementally

### Phase 3: Test
1. **Category tests**: Run full category test suite
2. **Regression check**: Run previously passing tests
3. **Integration**: Run full integration suite
4. **Edge cases**: Test boundary conditions

**Output**: Test pass rate improvement metrics

### Phase 4: Debug (if needed)
1. **Add debug output**: Log query plans, SQL generation
2. **Use sql_only**: Test SQL against ClickHouse directly
3. **Minimal repro**: Create isolated test case
4. **Binary search**: Disable optimizations to find culprit

**Output**: Root cause identified, fix refined

### Phase 5: Document
1. **Update STATUS.md**: Add fix summary with metrics
2. **Update CHANGELOG.md**: Add entry with date and stats
3. **Create feature note** (if significant): Document in `notes/`
4. **Update KNOWN_ISSUES.md**: Remove fixed items

**Output**: Complete documentation

---

## Prioritized Execution Plan (Revised December 22, 2025)

### **Quick Win: Complete Zeek Suite (Today - 0.5 day)**

**Goal**: Fix 2 remaining zeek_merged test failures → 100% pass rate

**Steps**:
1. Run failing tests with full traceback
2. Check generated SQL for COUNT aggregations
3. Fix table alias or aggregation code
4. Verify all 24 tests passing

**Target**: Zeek merged 24/24 (100%)

---

### **Week 1: Matrix Tests Investigation & VLP Fixes (5 days)**

**Goal**: Fix majority of matrix test failures (currently 400/2408 failing)

**Day 1-2: Deep Investigation**
- Categorize 400 failures by pattern type (VLP, aggregation, multi-hop)
- Run subset of tests with detailed SQL output
- Identify common root causes
- Expected: ~250 VLP, ~100 aggregation, ~50 other

**Day 3-4: Variable-Length Path Fixes**
- Fix CTE generation for VLP patterns
- Fix property selection in recursive CTEs
- Test with multiple schemas (zeek_merged, filesystem)
- Expected: Fix ~200-250 matrix VLP failures

**Day 5: Aggregation Edge Cases**
- Fix COUNT/SUM/AVG with nullable columns
- Fix GROUP BY with complex expressions
- Fix COLLECT edge cases
- Expected: Fix ~80-100 matrix aggregation failures

**Milestone**: 2700+/3319 tests passing (81%+)

---

### **Week 2: Core Graph Algorithms (5 days)**

**Goal**: Fix variable-length paths and shortest path algorithms

**Day 6-7: Variable-Length Path Tests**
- Focus on `test_variable_length_paths.py` (13 failing)
- Fix range patterns (`*1..3`, `*2..4`)
- Fix unbounded patterns (`*`, `*1..`)
- Fix VLP + WHERE clause interactions
- Expected: 20+/24 VLP tests passing

**Day 8-10: Shortest Path Algorithms**
- Focus on `test_shortest_paths.py` (20 failing)
- Fix basic `shortestPath()` CTE generation
- Fix `allShortestPaths()` distinct handling
- Fix early termination optimization
- Fix property access on paths
- Expected: 15+/20 shortest path tests passing

**Milestone**: 2750+/3319 tests passing (83%+)

---

### **Week 3: Schema-Specific & Edge Cases (5 days)**

**Goal**: Fix remaining schema-specific and edge case failures

**Day 11-12: Wiki/Tutorial Tests**
- Add `social_benchmark.yaml` schema to unified setup
- Fix property mappings for tutorial queries
- Expected: Fix ~100-120 wiki test failures

**Day 13: Security Graph Tests**
- Add security graph schema to unified setup
- Fix relationship type mappings
- Expected: Fix ~15-18 security graph failures

**Day 14-15: Miscellaneous Edge Cases**
- Expression/function tests
- Denormalized edge tests (fix setup)
- Optional match edge cases
- Expected: Fix ~80-100 misc failures

**Milestone**: 2950+/3319 tests passing (89%+)

---

### **Week 4: Polish & Documentation (5 days)**

**Goal**: Final bug fixes and comprehensive documentation

**Day 16-18: Final Bug Fixes**
- Address remaining high-impact failures
- Fix any regressions from previous weeks
- Run full suite multiple times for stability

**Day 19-20: Documentation & Release**
- Update STATUS.md with all fixes
- Update CHANGELOG.md with v0.6.1 details
- Create feature notes for major fixes
- Update KNOWN_ISSUES.md
- Prepare release notes

**Final Target**: 3050+/3319 tests passing (92%+)

---

## Success Metrics (Revised)

### Milestones
- ✅ **v0.6.0 Release**: 75.7% (2514/3319) - Comma pattern fix, cross-table JOINs working
- 🎯 **Quick Win** (Day 1): 75.8% (2516/3319) - Zeek merged 100%
- 🎯 **Week 1**: 81%+ (2700+/3319) - Matrix VLP and aggregation fixes
- 🎯 **Week 2**: 83%+ (2750+/3319) - Core graph algorithm fixes
- 🎯 **Week 3**: 89%+ (2950+/3319) - Schema-specific fixes
- 🎯 **Week 4**: 92%+ (3050+/3319) - Polish and edge cases

### Definition of Done (Per Category)
- ✅ 80%+ tests in category passing (or 90%+ for small categories)
- ✅ No regressions in previously passing tests
- ✅ Root cause documented (in STATUS.md or feature note)
- ✅ STATUS.md and CHANGELOG.md updated
- ✅ Code committed with descriptive message
- ✅ Full integration suite run to verify no side effects

---

## Risk Mitigation

### Potential Blockers
1. **Schema incompatibilities**: Some schemas may be fundamentally incompatible with unified approach
   - **Mitigation**: Keep per-test schema loading as fallback option

2. **Complex interactions**: Fixes may have unexpected side effects
   - **Mitigation**: Run full suite after each major fix, commit incrementally

3. **Missing ClickHouse features**: Some queries may not translate to valid ClickHouse SQL
   - **Mitigation**: Document limitations in KNOWN_ISSUES.md, mark tests as xfail

4. **Time estimates wrong**: Categories may take longer than expected
   - **Mitigation**: Re-prioritize weekly, focus on highest impact first

---

## Next Steps (December 22, 2025)

**Immediate** (today - START HERE!):
1. ✅ Update investigation plan with v0.6.0 results
2. ⬜ Investigate 2 failing zeek_merged tests (quick win)
3. ⬜ Fix zeek_merged COUNT aggregation issues
4. ⬜ Verify zeek_merged 24/24 passing

**Tomorrow & This Week**:
1. Deep dive into matrix test failures (categorize 400 failures)
2. Begin VLP fixes in matrix tests
3. Track progress daily in STATUS.md

**Track progress in**: `STATUS.md` (update after each milestone completion)

---

## Commands Reference

### Investigation
```bash
# Run specific category
pytest tests/integration/matrix/ -v --tb=short -k "zeek_merged" 2>&1 | tee matrix_zeek_investigation.log

# Check specific error pattern
pytest tests/integration/matrix/ -v --tb=short 2>&1 | grep -A 10 "AssertionError"

# Run single test with full traceback
pytest tests/integration/matrix/test_comprehensive.py::TestBasicPatterns::test_simple_node -vv --tb=long

# Check server logs for query planning
tail -100 clickgraph_server.log | grep -E "ViewScan|source_table|ViewTableRef"
```

### Validation
```bash
# Run full integration suite with stats
pytest tests/integration/ -v --tb=no -q 2>&1 | tail -5

# Run specific category after fix
pytest tests/integration/test_variable_length_paths.py -v --tb=no

# Quick smoke test (wiki tests should always pass)
pytest tests/integration/wiki/ -v
```

### Schema Debugging
```bash
# Check schema loading
grep -A 20 "label: IP" schemas/test/unified_test_schema.yaml

# Verify node_id field
grep -B 2 -A 5 "node_id:" schemas/examples/zeek_merged.yaml

# List all schemas in unified file
grep "^  - label:" schemas/test/unified_test_schema.yaml
```

---

**Remember**: Follow Boy Scout Rule - leave the code cleaner than you found it!
