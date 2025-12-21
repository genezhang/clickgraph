# Test Failure Investigation Plan

**Status**: 2581/3363 passing (76.7%) - 762 failures remaining  
**Created**: December 21, 2025  
**Goal**: Systematic investigation and resolution of remaining test failures

## Executive Summary

After fixing the database prefix bug (+22% improvement), we have 762 remaining failures across distinct categories. This plan provides a systematic approach to investigate and fix them, prioritized by impact and complexity.

---

## Failure Categories (from pytest output)

### 1. Matrix Tests - **565 failures** (74% of failures)
**Schemas affected**: zeek_merged, filesystem, ontime_benchmark, group_membership

**Sample errors**:
```
zeek_merged: Identifier 'n.id' cannot be resolved
filesystem: No FROM clause found
```

**Root causes to investigate**:
- Schema node_id field misconfiguration (zeek_merged uses 'id' but column might be different)
- Missing table resolution for certain schema types
- FK-based edge schemas may have incomplete metadata

**Priority**: HIGH (bulk of failures)  
**Estimated effort**: 3-5 days

---

### 2. Variable-Length Paths - **26 failures**
**Test file**: `test_variable_length_paths.py`

**Failure patterns**:
- Fixed length paths (exact hops): `*2`, `*3`
- Range paths: `*1..3`, `*2..4`
- Unbounded paths: `*`, `*1..`
- With filters and aggregations

**Root causes to investigate**:
- CTE generation for variable-length patterns
- Recursion depth handling
- Property access within path CTEs
- JOIN conditions for multi-hop traversals

**Priority**: MEDIUM (core graph feature)  
**Estimated effort**: 2-3 days

---

### 3. Shortest Path Algorithms - **45 failures**
**Test file**: `test_shortest_paths.py`

**Failure patterns**:
- `shortestPath()` function
- `allShortestPaths()` function
- With filters, aggregations, depth constraints
- Edge cases (self-loops, unreachable nodes)

**Root causes to investigate**:
- Shortest path CTE generation
- Early termination optimization not working
- Path filtering logic
- Multiple path handling in allShortestPaths

**Priority**: MEDIUM (specialized algorithm)  
**Estimated effort**: 2-3 days

---

### 4. Optional Match - **27 failures**
**Test files**: Various integration tests

**Failure patterns**:
- Optional match with filtering
- Multiple optional matches
- Optional match after required match
- Complex nested patterns

**Root causes to investigate**:
- LEFT JOIN generation edge cases
- Optional alias tracking incomplete
- Interaction with other features (aggregations, paths)
- NULL handling in WHERE clauses

**Priority**: MEDIUM (common pattern)  
**Estimated effort**: 2-3 days

---

### 5. Security Graph Tests - **20 failures**
**Test file**: `test_security_graph.py`

**Failure patterns**:
- Basic node queries (User, Group, File, Folder)
- Relationship queries (MEMBER_OF, CONTAINS)
- Aggregations and GROUP BY

**Root causes to investigate**:
- Schema-specific issues (might use different property names)
- May be using custom schema not in unified_test_schema.yaml
- Possible relationship type mismatches

**Priority**: LOW (domain-specific test suite)  
**Estimated effort**: 1 day

---

### 6. Denormalized Edges - **20 errors**
**Test file**: `test_denormalized_edges.py`

**Error type**: ERROR (not FAILED - indicates test setup issue)

**Failure patterns**:
- Property access on denormalized edges
- Composite edge IDs
- Variable-length paths with denormalized properties
- Mixed denormalized and normal edges

**Root causes to investigate**:
- Test fixture setup failing
- Schema configuration for denormalized edges
- May need special schema in unified_test_schema.yaml

**Priority**: LOW (advanced feature)  
**Estimated effort**: 1-2 days

---

### 7. Zeek/OnTime Domain Tests - **79 failures**
**Test files**: `test_zeek_*.py`, relationship/aggregation tests

**Failure patterns**:
- Domain-specific queries (DNS, connections, flights)
- Cross-table correlations
- WITH + MATCH patterns

**Root causes to investigate**:
- Schema mismatches (zeek.conn_log vs expected)
- Database qualification issues
- Custom property mappings

**Priority**: LOW (can work with other schemas)  
**Estimated effort**: 2 days

---

### 8. Misc Edge Cases - **4 failures**
**Tests**: standalone_return, with_having, role_based_queries

**Priority**: LOW  
**Estimated effort**: 0.5 day

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

## Prioritized Execution Plan

### **Week 1: Matrix Tests (High Impact)**

**Goal**: Fix 565 matrix test failures (74% of all failures)

**Day 1-2: Investigation**
- Run matrix tests with different schemas: `pytest tests/integration/matrix/ -v --tb=short -k "zeek_merged" 2>&1 | head -100`
- Check schema files: `schemas/examples/zeek_merged.yaml`, `filesystem.yaml`
- Identify node_id field mismatches
- Check if schemas are loaded in unified_test_schema.yaml

**Day 3-4: Fix Implementation**
- Add missing schemas to unified_test_schema.yaml OR
- Fix node_id resolution in schema loading code
- Fix FROM clause generation for FK-based edges
- Test incrementally per schema

**Day 5: Validation**
- Run full matrix suite
- Expected: 300-400 tests fixed (50-70% of matrix failures)
- Document findings

**Target**: 2900+/3363 tests passing (86%+)

---

### **Week 2: Graph Algorithms (Medium Impact)**

**Goal**: Fix variable-length paths (26) + shortest paths (45) = 71 failures

**Day 6-7: Variable-Length Paths**
- Run: `pytest tests/integration/test_variable_length_paths.py -v --tb=short`
- Check CTE generation for recursive patterns
- Verify property selection in path CTEs
- Fix JOIN conditions for multi-hop

**Day 8-9: Shortest Paths**
- Run: `pytest tests/integration/test_shortest_paths.py -v --tb=short`
- Check early termination logic
- Verify path filtering
- Fix allShortestPaths distinct handling

**Day 10: Validation**
- Run both test suites
- Expected: 60-70 tests fixed (80-95% of algorithm failures)

**Target**: 2960+/3363 tests passing (88%+)

---

### **Week 3: Optional Match & Domain Tests (Remaining)**

**Goal**: Fix optional match (27) + security (20) + zeek/ontime (79) = 126 failures

**Day 11-12: Optional Match**
- Run: `pytest tests/integration/ -v -k "optional" --tb=short`
- Check LEFT JOIN generation edge cases
- Fix NULL handling in WHERE clauses
- Test nested optional patterns

**Day 13: Security Graph**
- Run: `pytest tests/integration/test_security_graph.py -v --tb=short`
- Add security graph schema to unified_test_schema.yaml
- Fix relationship type mappings

**Day 14-15: Zeek/OnTime Domain**
- Fix remaining domain-specific issues
- Verify database qualification
- Add missing schemas if needed

**Target**: 3080+/3363 tests passing (91.5%+)

---

### **Week 4: Polish & Edge Cases**

**Goal**: Fix denormalized edges (20 errors) + misc (4) = 24 issues

**Day 16-17: Denormalized Edges**
- Investigate ERROR status (test setup issue)
- Add denormalized edge fixtures to conftest.py
- Implement composite edge ID support
- Test variable-length paths with denormalized props

**Day 18: Misc Edge Cases**
- Fix standalone_return, with_having, role_based_queries
- Address any new failures from previous fixes

**Day 19-20: Final Validation**
- Run complete integration suite multiple times
- Fix any regressions
- Update all documentation

**Target**: 3100+/3363 tests passing (92%+)

---

## Success Metrics

### Milestones
- ✅ **Current**: 76.7% (2581/3363) - Database prefix bug fixed
- 🎯 **Week 1**: 86% (2900/3363) - Matrix tests fixed
- 🎯 **Week 2**: 88% (2960/3363) - Algorithm tests fixed
- 🎯 **Week 3**: 91.5% (3080/3363) - Domain tests fixed
- 🎯 **Week 4**: 92%+ (3100+/3363) - Polish complete

### Definition of Done (Per Category)
- ✅ 80%+ tests in category passing
- ✅ No regressions in previously passing tests
- ✅ Root cause documented in feature note
- ✅ STATUS.md and CHANGELOG.md updated
- ✅ Code committed with descriptive message

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

## Next Steps

**Immediate** (today):
1. ✅ Create this plan document
2. ✅ Commit database prefix fix
3. ⬜ Run matrix tests investigation (Day 1 task)

**Tomorrow**:
1. Complete matrix test investigation
2. Identify schema fixes needed
3. Begin implementation

**Track progress in**: `STATUS.md` (update after each category completion)

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
