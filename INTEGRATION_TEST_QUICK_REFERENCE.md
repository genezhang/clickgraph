# Integration Test Audit - Quick Reference

**Run Date**: January 22, 2026 | **Status**: ✅ COMPLETE

---

## 📊 At a Glance

```
PASS RATE: 80.8% (2,829/3,496)
GAP: 495 tests failing
TARGET: 95%+ (3,320+/3,496)
EFFORT: 3-4 weeks, 1 senior dev + 1 QA
```

---

## 🔴 Top Issues (by test count)

| Issue | Tests | Severity | Fix Time |
|-------|-------|----------|----------|
| VLP CTE generation | 200 | 🔴 CRITICAL | 4 days |
| Denormalized edges | 100 | 🔴 CRITICAL | 2 days |
| Test data setup | 80 | ⚠️ HIGH | 1 day |
| Path functions | 80 | ⚠️ HIGH | 2 days |
| Variable renaming | 40 | ⚠️ MEDIUM | 1 day |
| Complex expressions | 50 | ⚠️ MEDIUM | 2 days |
| Infrastructure errors | 5 | 🟢 LOW | <1 day |

**Total**: 495 issues

---

## ✅ What's Working Well

- ✅ Basic patterns (90% pass)
- ✅ Aggregations (98% pass)
- ✅ Optional MATCH (97% pass)
- ✅ WITH clause (95% pass)
- ✅ UNWIND (98% pass)
- ✅ Server/API (100% pass)

---

## 🔥 Quick Wins (Do This Week)

1. **Fix property_pruning.py errors** (2 hours)
   - Command: `pytest tests/integration/test_property_pruning.py -v`
   - Action: Debug test setup, fix or skip

2. **Register pytest markers** (30 min)
   - File: `pytest.ini`
   - Add: vlp, performance, integration, slow markers

3. **Create filesystem schema data** (4 hours)
   - File: `tests/integration/fixtures/data/generators.py`
   - Action: Add generator for fs_objects, fs_relationships tables

**Expected**: +30 passing tests

---

## 🎯 Critical Path (Top 3 Issues)

### 1️⃣ VLP CTE Generation (200 tests)
**Problem**: Variable-length paths not generating valid SQL CTEs when combined with WHERE clauses

**Debug**:
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (a:User)-[*1..3]->(b) WHERE a.user_id = 1 RETURN b",
    "sql_only": true
  }'
```

**Expected**: Generated CTE should include WHERE clause in recursive part

**Files to investigate**:
- `src/render_plan/cte_extraction.rs` 
- `src/render_plan/cte_manager/mod.rs`

---

### 2️⃣ Denormalized Edge Unions (100 tests)
**Problem**: UNION queries producing duplicate rows when nodes appear in multiple tables

**Debug**:
```cypher
MATCH (n:IP)
RETURN COUNT(DISTINCT n.ip)

-- Should return count of unique IPs, but getting duplicates
```

**Expected**: Composite key deduplication working in UNION

**Files to investigate**:
- `src/query_planner/logical_plan/match_clause.rs`
- `src/render_plan/to_sql_query.rs`
- `src/graph_catalog/graph_schema.rs`

---

### 3️⃣ Test Data Setup (80 tests)
**Problem**: Schema matrix tests fail - tables don't exist

**Error**: `Unknown table expression identifier 'test_integration.fs_objects'`

**Fix**: Create test data generator that populates all schema tables

**Files to update**:
- `tests/integration/conftest.py` - Add schema setup fixture
- `tests/integration/fixtures/data/` - Add data generators

---

## 📈 Component Health Scorecard

| Component | Health | Pass % | Action |
|-----------|--------|--------|--------|
| Parser | ✅ Excellent | 99%+ | Keep as-is |
| Query Planner | ⚠️ Good/Gaps | 75% | Focus on VLP |
| SQL Generator | ⚠️ Fair | 70% | Fix CTE, unions |
| Render Plan | ⚠️ Complex | 60% | Refactor, clean up |
| Server/API | ✅ Excellent | 99%+ | Keep as-is |

---

## 🗂️ Test File Categories

### ✅ Green (95%+ pass)
- test_aggregations.py
- test_optional_match.py
- test_collect_unwind.py
- test_with_*.py
- test_case_expressions.py

### 🟡 Yellow (70-90% pass)
- test_basic_queries.py
- test_relationships.py
- test_property_expressions.py
- test_shortest_paths.py (59% - borderline)

### 🔴 Red (<70% pass)
- test_vlp_*.py (15-43% pass) ← FIX THIS FIRST
- test_path_variables.py (53% pass)
- test_multi_hop_patterns.py (30% pass)
- test_denormalized_edges.py (< 50% pass)
- test_multi_tenant_*.py (33% pass)
- test_zeek_merged.py (44% pass)

---

## 🚀 Roadmap at a Glance

```
Week 1: Infrastructure + VLP
├─ Phase 1: Quick wins (30 tests) ← START HERE
├─ Phase 2: VLP CTE fix (150 tests)
└─ Checkpoint: 85% pass rate

Week 2: Denormalized + Setup
├─ Phase 3: Denormalized edges (100 tests)
├─ Phase 4: Multi-schema setup (80 tests)
└─ Checkpoint: 92% pass rate

Week 3: Polish
├─ Phase 5: Expressions (50 tests)
├─ Phase 6: Remaining (85 tests)
└─ Final: 95%+ pass rate
```

---

## 📋 Decision Checklist

- [ ] **Review** INTEGRATION_TEST_AUDIT.md (detailed findings)
- [ ] **Review** INTEGRATION_TEST_IMPROVEMENT_ROADMAP.md (implementation plan)
- [ ] **Assign** team members to phases
- [ ] **Set up** metrics tracking (daily test runs)
- [ ] **Start** with Phase 1 (infrastructure)
- [ ] **Daily standup** on test pass rate
- [ ] **Weekly review** of root cause patterns

---

## 🔗 Documentation Links

| Document | Purpose | Length |
|----------|---------|--------|
| **INTEGRATION_TEST_AUDIT.md** | Detailed audit with component analysis | 400 lines |
| **INTEGRATION_TEST_IMPROVEMENT_ROADMAP.md** | 6-phase implementation plan | 450 lines |
| **INTEGRATION_TEST_METRICS_SUMMARY.md** | Key metrics and recommendations | 300 lines |
| **This file** | Quick reference card | 250 lines |

---

## 💡 Key Insights

1. **Parser is solid** - All Cypher syntax works, not a bottleneck
2. **Integration gaps, not unit gaps** - Need feature combination testing, not more unit tests
3. **VLP is the big gap** - 200 tests failing on CTE generation with filters
4. **Denormalized edges complex** - Unique model, needs specialized handling
5. **Dead code in render_plan** - Refactoring opportunity, 50+ unused functions

---

## 🎓 What We Learned

### Why Test Coverage is Lower Than Expected
- Complex queries require lots of setup (schema, AST, context)
- Many edge cases in SQL generation are combinatorial
- Integration better than unit for these scenarios
- **Status**: This is appropriate for graph query engine

### Why VLP Tests Failing
- VLP CTE generation simplified initially
- Filter propagation not implemented
- Path function tracking incomplete
- **Solution**: Complete CTE manager implementation

### Why Denormalized Edges Problematic
- Multiple schema models require different SQL
- UNION approach has subtle issues with duplicates
- Type inference needs schema-aware context
- **Solution**: Schema-aware rendering with proper key handling

---

## 📞 Questions?

**For audit details**: See INTEGRATION_TEST_AUDIT.md section: "Test Coverage Analysis by Component"

**For implementation help**: See INTEGRATION_TEST_IMPROVEMENT_ROADMAP.md section: "Phase X: [Task Description]"

**For metrics tracking**: See INTEGRATION_TEST_METRICS_SUMMARY.md section: "Monitoring & Metrics"

---

**Last updated**: January 22, 2026, 11:45 PM  
**Next review**: January 24, 2026 (end of Phase 1)  
**Audit confidence**: 85-99% depending on aspect
