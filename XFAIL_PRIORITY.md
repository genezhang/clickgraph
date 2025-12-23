# xfail Test Priority List

*Created: December 22, 2025*

**Total xfails: 54 tests** (all need investigation/fixes)

## 🔴 Priority 1: Critical Bugs (High Impact, Core Features)

### 1. Schema Loading Timing Issues (4 tests) - **HIGH IMPACT**
**Files**: `tests/integration/test_multi_hop_patterns.py`
- `test_outgoing_join_uses_dest_to_origin`
- `test_undirected_has_both_join_directions`
- `test_single_hop_no_union`
- `test_4hop_undirected_has_16_branches`

**Issue**: Schema loaded successfully but queries fail with "Schema not found"
**Impact**: Multi-hop queries are core feature
**Evidence**: Schema loaded (10/10), curl works, but pytest gets PLANNING_ERROR
**Root Cause**: Likely race condition or request isolation issue
**Difficulty**: Medium
**Investigation Steps**:
1. Add debug logging to see when schema lookup happens
2. Check if pytest runs in separate process/thread
3. Verify GLOBAL_SCHEMAS is accessible across requests
4. Test with explicit schema parameter vs USE clause

---

### 2. Count Relationships Aggregation (1 test) - **HIGH IMPACT**
**File**: `tests/integration/test_aggregations.py::test_count_relationships`

**Issue**: 500 error when counting relationships
**Impact**: Basic aggregation queries failing
**Difficulty**: Medium
**Investigation Steps**:
1. Get full error message from server logs
2. Test simple `MATCH ()-[r]->() RETURN count(r)` query
3. Check if issue is with relationship variable or aggregation
4. Compare with `RETURN count(*)` behavior

---

### 3. Denormalized VLP Errors (2 tests) - **MEDIUM-HIGH IMPACT**
**File**: `tests/integration/test_denormalized_edges.py`
- `test_variable_path_with_denormalized_properties` 
- `test_variable_path_cte_uses_denormalized_props`

**Issue**: 500 error with VLP on denormalized schemas
**Impact**: Affects advanced denormalized edge use cases
**Difficulty**: Medium-Hard
**Investigation Steps**:
1. Get full error from server logs
2. Test simple VLP: `MATCH (a)-[*1..2]->(b) RETURN a.city`
3. Check if issue is property resolution in CTE
4. Compare generated CTE SQL with denormalized vs standard schema

---

## 🟡 Priority 2: Test Infrastructure (Easy Wins)

### 4. Property Expression Data Mismatches (3 tests) - **LOW IMPACT**
**File**: `tests/integration/test_property_expressions.py`
- `test_case_when_tier_silver` (expects 3, got 5)
- `test_case_when_age_groups` (expects 'minor', got 'adult')
- `test_multi_if_priority_tiers` (expects 'medium', got 'high')

**Issue**: Test expectations don't match actual test data
**Impact**: None - tests work, data just differs
**Difficulty**: Easy
**Fix**: Update test expectations to match actual data OR regenerate test data
**Investigation Steps**:
1. Query test data: `MATCH (u:User) WHERE u.tier = 'silver' RETURN count(*)`
2. Update assertions to match actual counts
3. Document why data differs if intentional

---

### 5. API SQL Field Inconsistency (1 test) - **LOW IMPACT**
**File**: `tests/integration/test_denormalized_edges.py::test_sql_has_no_joins`

**Issue**: Response doesn't include 'sql' field
**Impact**: Only affects SQL inspection tests
**Difficulty**: Easy
**Investigation Steps**:
1. Check if sql_only parameter needed
2. Verify API response format documentation
3. Update test to use correct field name or add sql_only=true

---

### 6. Tenant Isolation Schema Column (1 test) - **LOW IMPACT**
**File**: `tests/integration/test_multi_tenant_parameterized_views.py::test_relationship_tenant_isolation`

**Issue**: Schema column name mismatch (likely friendship_date vs other name)
**Impact**: Only multi-tenant edge case
**Difficulty**: Easy
**Investigation Steps**:
1. Check schema definition for multi_tenant
2. Verify column names in brahmand.multi_tenant_friendships table
3. Fix schema YAML or SQL to match

---

### 7. Performance Baseline Data Setup (1 test) - **LOW IMPACT**
**File**: `tests/integration/test_performance.py::test_baseline_simple_queries`

**Issue**: Performance test needs proper data
**Impact**: Only performance benchmarking
**Difficulty**: Easy
**Fix**: Ensure simple_graph fixture loads enough data for meaningful performance test

---

### 8. Wiki Exercise Query (1 test) - **LOW IMPACT**
**File**: `tests/integration/wiki/test_cypher_basic_patterns.py::test_exercise_2_4_count_total_relationships`

**Issue**: Wiki exercise needs data setup or query fix
**Impact**: Documentation/tutorial only
**Difficulty**: Easy
**Investigation Steps**:
1. Run query manually to see actual error
2. Check if test data matches wiki expectations
3. Fix query or data as needed

---

## 🟢 Priority 3: Advanced Features (Future Work)

### 9. Composite Edge IDs with VLP (2 tests) - **LOW IMPACT**
**File**: `tests/integration/test_denormalized_edges.py`
- `test_variable_path_with_composite_edge_id`
- `test_composite_id_prevents_duplicate_edges`

**Issue**: VLP with composite edge IDs needs implementation
**Impact**: Advanced edge case for denormalized schemas
**Difficulty**: Hard
**Investigation Steps**:
1. Understand composite edge ID requirements
2. Check how VLP CTE handles edge deduplication
3. Design edge ID tracking in recursive CTEs

---

### 10. Mixed Denormalized Expressions (2 tests) - **LOW IMPACT**
**File**: `tests/integration/test_denormalized_mixed_expressions.py`
- `test_denormalized_where_mixed_expression`
- `test_denormalized_return_mixed_expression`

**Issue**: Expressions like `s.x + t.y` where both are denormalized
**Impact**: Advanced expression handling
**Difficulty**: Medium-Hard
**Investigation Steps**:
1. Understand edge context resolution for denormalized props
2. Check how property resolver handles multiple denormalized nodes
3. Design context tracking for mixed expressions

---

### 11. Parameter Function Composition (1 test) - **LOW IMPACT**
**File**: `tests/integration/test_parameter_functions.py::test_multiple_function_composition`

**Issue**: Composing multiple parameter functions
**Impact**: Advanced parameter usage
**Difficulty**: Medium
**Investigation Steps**:
1. Get actual error message
2. Test nested function calls: `param(param(x))`
3. Check if issue is parsing or evaluation

---

## 📊 Summary by Difficulty & Impact

### Quick Wins (Easy + Low Impact) - **Do First**
1. Property expression data fixes (3 tests) - 30 minutes
2. API SQL field fix (1 test) - 15 minutes
3. Tenant isolation column (1 test) - 15 minutes
4. Performance data setup (1 test) - 15 minutes
5. Wiki exercise fix (1 test) - 15 minutes

**Total: ~2 hours, 7 tests fixed**

### Critical Bug Fixes (Medium Difficulty + High Impact) - **Do Next**
1. Schema loading timing (4 tests) - 4 hours investigation + fix
2. Count relationships (1 test) - 2 hours investigation + fix
3. Denormalized VLP (2 tests) - 4 hours investigation + fix

**Total: ~10 hours, 7 tests fixed**

### Advanced Features (Hard + Low Impact) - **Do Later**
1. Composite edge IDs (2 tests) - 8+ hours
2. Mixed expressions (2 tests) - 6+ hours
3. Parameter composition (1 test) - 2+ hours

**Total: ~16+ hours, 5 tests fixed**

---

## 🎯 Recommended Action Plan

### Phase 1: Quick Wins (Week 1)
- Fix all test infrastructure issues
- Goal: 7 more tests passing (554 passed total)
- Time: 1-2 days

### Phase 2: Critical Bugs (Week 2-3)
- Investigate schema timing issue (highest priority)
- Fix aggregation bug
- Fix denormalized VLP
- Goal: 7 more tests passing (561 passed total)
- Time: 1-2 weeks

### Phase 3: Advanced Features (Future)
- Implement as needed based on user demand
- Goal: 5 more tests passing (566 passed total)
- Time: 2-3 weeks

---

## 🔍 Investigation Templates

### For 500 Errors:
```bash
# 1. Get full error from server logs
tail -100 server.log | grep -A 10 "Error\|panic"

# 2. Test with sql_only to see generated SQL
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query":"YOUR_QUERY_HERE","sql_only":true}'

# 3. Run generated SQL directly in ClickHouse
# Check if issue is SQL generation or ClickHouse execution
```

### For Schema Issues:
```bash
# 1. List loaded schemas
curl http://localhost:8080/schemas | jq '.schemas[].name'

# 2. Get specific schema details
curl http://localhost:8080/schemas/unified_test_schema | jq .

# 3. Test query with explicit schema
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query":"USE schema_name MATCH (n) RETURN count(n)","schema_name":"schema_name"}'
```

### For Data Issues:
```bash
# 1. Check what data exists
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH (n:Label) RETURN count(n)","schema_name":"schema_name"}'

# 2. Sample actual data
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH (n:Label) RETURN n LIMIT 5","schema_name":"schema_name"}'
```

---

## 📝 Notes

**Important**: 
- xfails are NOT passing tests - they're documented failures
- Each xfail represents a real issue that needs investigation
- Some are easy fixes (test data), others are real bugs
- Prioritize based on user impact and ease of fix

**Testing After Fixes**:
```bash
# Remove xfail marker and run test
pytest tests/integration/test_file.py::test_name -xvs

# If passes, commit the xfail removal along with fix
```
