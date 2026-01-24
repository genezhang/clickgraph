# Integration Test Improvement Roadmap

**Date**: January 22, 2026  
**Goal**: Increase integration test pass rate from 80.8% (2,829/3,496) to 95%+ (3,320+/3,496)  
**Target Gap Closure**: Address 495 failing tests

---

## Executive Roadmap

### Current State
- ✅ **Passing**: 2,829 tests (80.8%)
- 🔴 **Failing**: 495 tests (14.1%)
- ⚠️ **Errors**: 5 tests (0.1%)
- 📊 **Gap**: 491 tests to fix

### Target State (4 weeks)
- ✅ **Passing**: 3,320+ tests (95%+)
- 🔴 **Failing**: <100 tests (2.9%)
- 📊 **Fixed**: 395 tests (+80%)

---

## Root Cause Summary

| Root Cause | Count | Files Affected | Severity |
|-----------|-------|-----------------|----------|
| VLP SQL generation incomplete | 200 | 8 files | 🔴 Critical |
| Denormalized edge handling | 100 | 5 files | 🔴 Critical |
| Test data setup issues | 80 | 3 files | ⚠️ High |
| Path function integration | 80 | 3 files | ⚠️ High |
| Variable renaming in WITH | 40 | 2 files | ⚠️ Medium |
| Complex expression edge cases | 50 | 4 files | ⚠️ Medium |
| Infrastructure/setup errors | 5 | 2 files | 🟢 Low |
| **Total** | **495** | **27 files** | |

---

## Phase 1: Infrastructure & Quick Wins (Days 1-3)

**Goal**: Fix errors, establish baseline, quick wins (should add ~30 passing tests)

### Task 1.1: Fix Property Pruning Infrastructure Errors (1 day)

**Current State**: 5 tests error out in test_property_pruning.py  
**Impact**: ~5 tests  
**Effort**: 1 day

```bash
# Debug action
pytest tests/integration/test_property_pruning.py -v
# Likely issues: module import, fixture setup, or missing schema

# Fix approaches:
1. Check conftest.py setup for property_pruning tests
2. Verify schema loading in test_property_pruning.py
3. Add missing fixtures or skip if not applicable
```

**Expected outcome**: 5 tests move from ERROR to PASS (or SKIP if intentionally inactive)

### Task 1.2: Create Missing Schema Test Data (1 day)

**Current State**: Filesystem and group_membership schemas missing test tables  
**Impact**: ~15 failures in matrix tests  
**Files**:
- `tests/integration/matrix/test_comprehensive.py` (3 failures)
- Need to identify which other tests use these schemas

**Actions**:
```python
# In tests/integration/conftest.py or fixtures/data/

# Add setup for filesystem schema:
- Create fs_objects table (Object nodes)
- Create fs_relationships table (relationships)
- Populate with test data

# Add setup for group_membership schema:
- Create group table
- Create member table
- Create membership relationship table
```

**Expected outcome**: Schema matrix tests improve from 60% to 95% pass rate

### Task 1.3: Register Pytest Markers (2 hours)

**Current State**: 7 pytest warnings about unknown marks (vlp, performance, integration)  
**Files**: `pytest.ini`

**Action**:
```ini
[pytest]
markers =
    vlp: Variable-length path tests
    performance: Performance benchmark tests
    integration: Integration test marker
    slow: Tests that take >5 seconds
    matrix: Schema matrix parametrized tests
```

**Expected outcome**: Clean pytest output, ability to run `pytest -m vlp` to isolate VLP tests

---

## Phase 2: Core VLP Fix (Days 4-7)

**Goal**: Fix VLP SQL generation and path function integration (should fix ~150 tests)

### Task 2.1: Debug VLP CTE Generation (1 day)

**Current State**: VLP patterns not generating valid CTEs when combined with filters  
**Impact**: ~200 VLP test failures  
**Root Cause**: CTE manager not propagating WHERE clause filters into recursive CTE

**Debug approach**:
```bash
# Create minimal failing test
cd /home/gz/clickgraph

# Test case 1: VLP with start node filter
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (a:User)-[*1..3]->(b:User) WHERE a.user_id = 1 RETURN b",
    "schema_name": "social_benchmark",
    "sql_only": true
  }'

# Test case 2: VLP with WHERE on relationship
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (a:User)-[r:FOLLOWS*1..3 {status: active}]->(b:User) RETURN b",
    "schema_name": "social_benchmark",
    "sql_only": true
  }'

# Examine generated SQL - look for missing WHERE clauses
```

**Files to investigate**:
- `src/render_plan/cte_extraction.rs` - VLP CTE generation
- `src/render_plan/cte_manager/mod.rs` - CTE strategy selection
- `src/query_planner/logical_plan/match_clause.rs` - VLP planning

**Expected fix**: Filter propagation into recursive CTE WHERE clause

### Task 2.2: Fix Path Function Integration (1 day)

**Current State**: `length(path)`, `nodes(path)`, `relationships(path)` not accessible in WITH/RETURN  
**Impact**: ~80 path function test failures  
**Root Cause**: Path variables not registered in render context properly

**Debug approach**:
```cypher
# Test: Path function in WITH clause
MATCH path = (a:User)-[*1..3]->(b:User)
WITH path, length(path) AS path_len
RETURN path_len

# Expected SQL: SELECT size(...) AS path_len FROM cte_name

# What's happening: "Unknown property 'path' in context"
```

**Files to investigate**:
- `src/query_planner/typed_variable.rs` - Path variable tracking
- `src/render_plan/plan_builder_utils.rs` - Path variable rendering
- `src/render_plan/render_expr.rs` - Expression rendering for path functions

**Expected fix**: Path variables properly tracked through WITH clause boundaries

### Task 2.3: Validate & Update VLP Tests (1 day)

**Impact**: 150 tests should move to PASS  
**Files**:
- test_vlp_with_comprehensive.py (110 failures → 10)
- test_vlp_aggregation.py (85 failures → 10)
- test_vlp_crossfunctional.py (95 failures → 20)
- test_variable_length_paths.py (25 failures → 5)
- test_path_variables.py (40 failures → 10)

**Action**:
```bash
# After fixes, run VLP test suite
pytest tests/integration/test_vlp_*.py -v

# Should see dramatic improvement in pass rate
```

**Expected outcome**: VLP tests improve from 43% to 90%+ pass rate

---

## Phase 3: Denormalized Edge Model Fix (Days 8-10)

**Goal**: Fix denormalized edge SQL generation (should fix ~100 tests)

### Task 3.1: Debug Denormalized Edge Issues (1 day)

**Current State**: UNION queries on denormalized nodes producing duplicates  
**Impact**: ~100 denormalized edge test failures  
**Root Cause**: Composite key deduplication not working properly

**Debug approach**:
```cypher
# Test case that's likely failing
MATCH (n:IP)
RETURN COUNT(DISTINCT n.ip)

# This should work but has issues with:
# 1. Node appearing in multiple tables (UNION)
# 2. Duplicate rows from UNION not deduplicated by composite key
```

**Files to investigate**:
- `src/query_planner/logical_plan/match_clause.rs` - Node table selection
- `src/render_plan/to_sql_query.rs` - Union rendering
- `src/graph_catalog/graph_schema.rs` - Denormalized node handling

**Expected fix**: UNION queries properly deduplicated by all ID columns

### Task 3.2: Add Denormalized Edge Tests (1 day)

**Create new test file**: `tests/integration/test_denormalized_deep_dive.py`

```python
class TestDenormalizedNodeProperties:
    def test_denormalized_union_basic(self):
        """MATCH (n:NodeInMultipleTables) RETURN n"""
        
    def test_denormalized_distinct(self):
        """MATCH (n) RETURN COUNT(DISTINCT n)"""
        
    def test_denormalized_with_groupby(self):
        """MATCH (n) RETURN n.type, COUNT(*)"""
        
    def test_denormalized_aggregates(self):
        """Multiple aggregations on denormalized nodes"""
```

**Expected outcome**: Denormalized tests improve from 33% to 90%+ pass rate

---

## Phase 4: Multi-Schema Infrastructure (Days 11-12)

**Goal**: Complete test data setup for all schema variations (should fix ~80 tests)

### Task 4.1: Improve Test Data Generators (1 day)

**Create**: `tests/integration/fixtures/data/generators.py`

```python
class TestDataGenerator:
    @staticmethod
    def setup_filesystem_schema():
        """Create and populate filesystem schema test tables"""
        # fs_objects: FILE, DIRECTORY, SYMLINK nodes
        # fs_contains: HAS relationship
        # fs_permissions: ACL relationship
        
    @staticmethod
    def setup_group_membership_schema():
        """Create and populate group membership schema"""
        # group: GROUP nodes
        # user: USER nodes  
        # group_has_member: MEMBERSHIP relationship
        
    @staticmethod
    def setup_multi_tenant_schema():
        """Create parametrized view setup for multi-tenant tests"""
```

**Expected outcome**: All schema parametrized tests have data

### Task 4.2: Update Conftest Schema Loading (1 day)

**Update**: `tests/integration/conftest.py`

```python
@pytest.fixture(scope="session", autouse=True)
def create_test_data():
    """Create all test tables and populate with data"""
    
    # For each schema in SCHEMAS:
    # 1. Load YAML schema
    # 2. Run DDL to create tables
    # 3. Call generator to populate data
    # 4. Verify data with sample queries
```

**Expected outcome**: Multi-schema tests improve from 20% to 90% pass rate

---

## Phase 5: Complex Expression Support (Days 13-14)

**Goal**: Handle edge cases in expressions (should fix ~50 tests)

### Task 5.1: Analyze Expression Failures (1 day)

**Run specific failing tests with debugging**:

```bash
pytest tests/integration/test_property_expressions.py -v --tb=short

# Capture common error patterns
# Likely issues:
# 1. Expressions on edge properties lose context
# 2. Case expressions in WHERE with graph patterns fail
# 3. Nested collections not properly rendered
```

**Files to investigate**:
- `src/render_plan/render_expr.rs` - Expression rendering
- `src/query_planner/logical_expr/mod.rs` - Logical expression handling

### Task 5.2: Fix & Add Tests (1 day)

**Create**: `tests/integration/test_expression_edge_cases.py`

**Test areas**:
1. Edge properties in expressions
2. Case expressions with graph context
3. Nested collection expressions
4. String/arithmetic functions in WHERE

**Expected outcome**: Expression tests improve from 75% to 95%+ pass rate

---

## Phase 6: Remaining Issues & Validation (Days 15-21)

**Goal**: Polish, handle remaining edge cases, final validation

### Task 6.1: Variable Renaming in WITH (1-2 days)

**Current State**: 40 test failures related to variable renaming  
**Files**: test_variable_alias_renaming.py

**Debug approach**:
```cypher
# Test case
MATCH (a:User)
WITH a AS user
MATCH (user)-[:FOLLOWS]->(f)
RETURN user, f

# Issue: Alias 'user' not properly tracked through WITH boundary
```

**Expected fix**: Variable aliases properly maintained through WITH clauses

### Task 6.2: Security & Access Control Tests (1-2 days)

**Current State**: 20 failures in security graph tests  
**Files**: test_security_graph.py

**Issue Areas**:
1. HAVING clause validation
2. Syntax error handling
3. GROUP BY with security filters

### Task 6.3: Final Validation (1-2 days)

**Run full test suite**:
```bash
pytest tests/integration/ --tb=no -q

# Target: 3,320+ passing tests (95%+)
# Maximum 100 failures acceptable
```

**Investigate any stubborn failures**:
- Focus on patterns, not individual tests
- Fix root causes, not symptoms

---

## Implementation Timeline

```
Week 1 (Jan 20-24)
├─ Mon-Tue: Phase 1 - Infrastructure & quick wins ✅ (3 days)
├─ Wed-Fri: Phase 2 - Core VLP fix (4 days)
└─ Checkpoint: 200+ tests fixed, running pass rate ~85%

Week 2 (Jan 27-31)
├─ Mon-Tue: Phase 3 - Denormalized edges (2 days)
├─ Wed-Thu: Phase 4 - Multi-schema setup (2 days)
├─ Fri: Phase 5 - Complex expressions (1 day)
└─ Checkpoint: 400+ tests fixed, running pass rate ~92%

Week 3 (Feb 3-7)
├─ Mon-Tue: Phase 6 - Remaining issues (2 days)
├─ Wed-Fri: Validation & cleanup (3 days)
└─ Final checkpoint: 450+ tests fixed, pass rate 95%+
```

---

## Success Criteria

### Quantitative Targets
- [ ] Pass rate reaches 95%+ (3,320/3,496)
- [ ] Failures < 100 tests
- [ ] Errors = 0
- [ ] All 69 test files have >80% pass rate

### Qualitative Targets
- [ ] VLP features fully functional (95%+ pass)
- [ ] Path functions fully integrated (95%+ pass)
- [ ] Denormalized edges working (90%+ pass)
- [ ] Multi-schema testing complete (90%+ pass)
- [ ] No test infrastructure errors

### Code Quality
- [ ] Dead code in render_plan either used or removed
- [ ] Test failures grouped by root cause (not random)
- [ ] All test files documented
- [ ] CI/CD test pipeline working

---

## Risk Mitigation

### Risk 1: VLP Fix Breaks Other Queries
**Likelihood**: Medium  
**Mitigation**:
- Run full test suite after each VLP change
- Revert if pass rate drops
- Review CTE changes carefully

### Risk 2: Denormalized Edge Complexity
**Likelihood**: High  
**Mitigation**:
- Start with simplest cases
- Add regression tests as you go
- Document expected behavior

### Risk 3: Test Data Setup Issues
**Likelihood**: Medium  
**Mitigation**:
- Create data setup checklist
- Add setup validation tests
- Document schema requirements

### Risk 4: Time Overrun
**Likelihood**: Medium  
**Mitigation**:
- Prioritize by impact (VLP > denormalized > others)
- Focus on root causes (broad impact)
- Consider parallel work on independent issues

---

## Resource Allocation

### Recommended Team Assignment

| Phase | Owner | Days | Notes |
|-------|-------|------|-------|
| Phase 1 - Infrastructure | QA/DevOps | 2 | Quick wins, setup |
| Phase 2 - VLP | Senior Dev | 4 | Complex, high impact |
| Phase 3 - Denormalized | Mid Dev | 2 | Database-specific |
| Phase 4 - Multi-Schema | QA/DevOps | 2 | Data setup focus |
| Phase 5 - Expressions | Junior Dev | 2 | Lower risk learning |
| Phase 6 - Polish | Senior Dev | 3 | Edge cases, debugging |

**Parallel Work Possible**: 
- Phase 1 + 2 can happen together (different code areas)
- Phase 3 + 4 can happen in parallel
- Phase 5 independent of others

---

## Monitoring & Metrics

### Daily Metrics
```bash
# Run daily to track progress
pytest tests/integration/ --tb=no -q

# Capture: total passed, failed, errors
# Plot on dashboard to visualize trend
```

### Weekly Checkpoints
- Week 1 end: 85%+ pass rate (200+ tests fixed)
- Week 2 end: 92%+ pass rate (400+ tests fixed)
- Week 3 end: 95%+ pass rate (450+ tests fixed)

### Post-Completion Maintenance
- Run integration tests in CI/CD pipeline
- Alert on pass rate dropping below 90%
- Add new tests for each bug found
- Review test coverage quarterly

---

## Documentation Updates Needed

After completing roadmap, update:

1. **STATUS.md**
   - Update test statistics: "Integration: 3,320+/3,496 (95%+)"
   - Add section on VLP/denormalized/multi-schema status

2. **TESTING.md**
   - Add integration test category guide
   - Document how to add new parametrized tests
   - Explain test data setup process

3. **KNOWN_ISSUES.md**
   - Clear resolved issues from this roadmap
   - Document any deferred work

4. **Feature Notes**
   - Create notes/integration_testing.md
   - Document testing patterns used
   - List pytest markers and usage

---

## Next Steps

1. **This week**: Start with Phase 1 (infrastructure, 3 days)
2. **Assign team members** to phases
3. **Set up metrics dashboard** to track progress
4. **Daily standup** on test pass rate
5. **Weekly review** of root cause patterns
6. **Adjust timeline** as needed based on actual fixes

---

**Document created**: January 22, 2026  
**Target completion**: February 7, 2026 (3 weeks)  
**Estimated effort**: 18-22 days of development work
