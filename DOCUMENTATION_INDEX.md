# WITH CTE Node Expansion Fix - Complete Documentation Index

## Overview

Complete test suite and documentation for validating the **WITH CTE Node Expansion Fix** (branch: `fix/with-chaining`).

---

## Documentation Map

### 🎯 Start Here
- **[QUICK_TEST_REFERENCE.md](QUICK_TEST_REFERENCE.md)** ⭐ 
  - One-command test execution
  - Quick debugging guide
  - Test commands cheat sheet
  - **Start here for fastest path to running tests**

### 📋 For Pre-Merge Review
- **[DESIGN_REVIEW_WITH_CTE_FIX.md](DESIGN_REVIEW_WITH_CTE_FIX.md)** (5,000 words)
  - Architecture analysis
  - Design verification
  - Implementation details
  - Gap analysis
  - Recommendations
  - **Comprehensive technical review**

- **[PRE_MERGE_VERIFICATION_CHECKLIST.md](PRE_MERGE_VERIFICATION_CHECKLIST.md)**
  - Pre-merge checklist
  - 9 test scenarios with expected results
  - Build status
  - Risk assessment
  - **Sign-off tracking**

- **[REVIEW_SUMMARY.md](REVIEW_SUMMARY.md)**
  - Executive summary
  - Quick verdict
  - Key improvements
  - Verification needed
  - **Read this first for 2-minute overview**

### 🧪 For Test Execution
- **[tests/integration/test_with_cte_node_expansion.py](tests/integration/test_with_cte_node_expansion.py)** (600+ lines)
  - 9 test classes covering all scenarios
  - 15+ test methods
  - Comprehensive assertions
  - Regression tests
  - **The actual test suite**

- **[scripts/test/run_with_cte_tests.sh](scripts/test/run_with_cte_tests.sh)** (200+ lines, executable)
  - Automated test runner
  - Server health checks
  - Colored output
  - Options: verbose, specific test, show SQL
  - **Easy test execution**

- **[tests/integration/TEST_WITH_CTE_DOCUMENTATION.md](tests/integration/TEST_WITH_CTE_DOCUMENTATION.md)** (400+ lines)
  - Detailed test scenario explanations
  - Failure modes & troubleshooting
  - Running instructions with examples
  - Schema requirements
  - CI/CD integration
  - **Complete test reference**

### 📊 For Summary & Overview
- **[TEST_CREATION_SUMMARY.md](TEST_CREATION_SUMMARY.md)** (300+ lines)
  - What was created
  - Test coverage map
  - Files created/modified
  - Test design principles
  - Integration with CI/CD
  - **Summary of all created artifacts**

- **[notes/with-cte-node-expansion-issue.md](notes/with-cte-node-expansion-issue.md)**
  - Original problem statement
  - Root cause analysis
  - Solution architecture
  - Existing infrastructure
  - **Background context**

---

## Quick Reference: Test Scenarios

| # | Scenario | File | Key Test | Purpose |
|---|----------|------|----------|---------|
| 1 | Basic Node Export | TestWithBasicNodeExpansion | test_with_single_node_export | Validate simple expansion |
| 2 | Multi-Variable Export | TestWithMultipleVariableExport | test_with_two_node_export | Multiple nodes in one CTE |
| 3 | WITH Chaining | TestWithChaining | test_with_chaining_two_levels | Nested CTEs |
| 4 | Scalar Export | TestWithScalarExport | test_with_scalar_count | Aggregates don't expand |
| 5 | Property Rename | TestWithPropertyRename | test_with_node_rename | Renamed variables |
| 6 | Cross-Table | TestWithCrossTable | test_with_cross_table_multi_hop | Complex patterns |
| 7 | Optional Match | TestWithOptionalMatch | test_optional_match_with_export | OPTIONAL + WITH |
| 8 | Polymorphic Labels | TestWithPolymorphicLabels | test_with_multi_label_node | Multiple node types |
| 9 | Denormalized Edges | TestWithDenormalizedEdges | test_with_denormalized_properties | Properties in edges |

---

## File Locations

### Test Code
```
tests/
  integration/
    test_with_cte_node_expansion.py (NEW - 600+ lines)
    TEST_WITH_CTE_DOCUMENTATION.md (NEW - 400+ lines)
    conftest.py (existing)
```

### Test Infrastructure
```
scripts/
  test/
    run_with_cte_tests.sh (NEW - 200+ lines, executable)
```

### Documentation
```
/ (root)
  DESIGN_REVIEW_WITH_CTE_FIX.md (NEW - 5,000 words)
  PRE_MERGE_VERIFICATION_CHECKLIST.md (NEW)
  REVIEW_SUMMARY.md (NEW)
  TEST_CREATION_SUMMARY.md (NEW)
  QUICK_TEST_REFERENCE.md (NEW)

notes/
  with-cte-node-expansion-issue.md (existing)
```

---

## Getting Started

### 1. First Time? Start Here
Read in this order:
1. **REVIEW_SUMMARY.md** (2 min) - Quick overview
2. **QUICK_TEST_REFERENCE.md** (5 min) - How to run tests
3. Run tests: `./scripts/test/run_with_cte_tests.sh`

### 2. Pre-Merge Review? Read These
1. **DESIGN_REVIEW_WITH_CTE_FIX.md** (20 min) - Architecture analysis
2. **PRE_MERGE_VERIFICATION_CHECKLIST.md** (10 min) - Verification steps
3. **tests/integration/TEST_WITH_CTE_DOCUMENTATION.md** (10 min) - Test reference

### 3. Running Tests?
```bash
# Option 1: Use test runner (recommended)
./scripts/test/run_with_cte_tests.sh

# Option 2: Direct pytest
cd tests/integration
pytest test_with_cte_node_expansion.py -v

# Option 3: Specific scenario
pytest test_with_cte_node_expansion.py::TestWithChaining -v
```

### 4. Test Failed?
1. Check **TEST_WITH_CTE_DOCUMENTATION.md** → "Failure Modes" section
2. Run with verbose: `./scripts/test/run_with_cte_tests.sh --verbose`
3. Check server logs: `docker logs clickhouse` or ClickGraph console
4. Read "Troubleshooting" in TEST_WITH_CTE_DOCUMENTATION.md

---

## Content at a Glance

### REVIEW_SUMMARY.md
- 📋 Quick verdict: ✅ Excellent design
- 🎯 Problem & solution overview
- 🔍 Code quality analysis
- ⚠️ Potential concerns (all mitigated)
- 📊 Impact analysis
- ✅ Risk assessment: LOW

### DESIGN_REVIEW_WITH_CTE_FIX.md
- 🏗️ Architecture analysis
- 🔧 Implementation correctness
- 📈 Code quality metrics
- 🧪 Verification checklist
- 📋 Pre-merge checklist
- 🎯 Sign-off criteria

### PRE_MERGE_VERIFICATION_CHECKLIST.md
- ✅ Architecture verification
- ✅ Code quality checks
- 🧪 Integration test scenarios
- 📊 Expected results
- 🔄 CI/CD integration
- ✅ Sign-off checklist

### QUICK_TEST_REFERENCE.md
- ⚡ One-command execution
- 📝 Test command cheat sheet
- 🔍 Quick debugging
- ✅ Pre-test checklist
- 🚨 Common issues & solutions

### TEST_CREATION_SUMMARY.md
- 📦 What was created
- 🗺️ Test coverage map
- 🚀 Quick start
- 📋 Test design principles
- ✅ Verification status

### tests/integration/TEST_WITH_CTE_DOCUMENTATION.md
- 📖 Detailed scenario explanations
- 🚀 Running instructions
- 🔧 Troubleshooting guide
- 📊 Schema requirements
- 🔄 CI/CD integration

---

## Test Execution Flows

### All Tests
```bash
./scripts/test/run_with_cte_tests.sh
# Expected: All 15+ tests pass
```

### Specific Scenario
```bash
./scripts/test/run_with_cte_tests.sh --test TestWithChaining
# Expected: 2 tests pass (two_levels + three_levels)
```

### Verbose Mode
```bash
./scripts/test/run_with_cte_tests.sh --verbose
# Shows detailed pytest output
```

### Direct pytest
```bash
cd tests/integration
pytest test_with_cte_node_expansion.py -v
```

---

## Key Concepts

### The Problem
```cypher
MATCH (a:User) WITH a RETURN a
-- ❌ Returns: with_a_cte_0.a (single column)
-- ✅ Should return: a.user_id, a.name, a.email, ... (multiple columns)
```

### The Solution
- Use **TypedVariable** (available from planning) to determine variable source
- Use **schema** (same as base tables) to get properties
- Generate **CTE column names** algorithmically: `{alias}_{db_column}`
- Dispatch to appropriate expansion logic based on source (Match vs Cte)

### The Fix Architecture
```
RETURN b (where b from WITH)
  ↓
lookup_variable("b") → TypedVariable::Node { source: Cte("with_a_b_cte_1") }
  ↓
expand_cte_entity(alias, typed_var, cte_name)
  ├─ Parse CTE name → FROM alias
  ├─ Get properties from schema
  ├─ Generate CTE columns: b_user_id, b_full_name, ...
  └─ Generate SelectItems for each property
```

---

## Success Criteria

All tests pass when fix is working:
1. ✅ Basic node expansion works
2. ✅ Multi-variable exports work
3. ✅ Chained CTEs work
4. ✅ Scalars handled correctly
5. ✅ Renamed variables work
6. ✅ Complex patterns work
7. ✅ Optional match + WITH works
8. ✅ Edge cases handled
9. ✅ No regressions

---

## Related Files in Repository

### Implementation Files (Modified by Fix)
- `src/render_plan/select_builder.rs` (-258 lines, +300 lines)
- `src/render_plan/plan_builder.rs` (-164 lines, +24 lines)
- `src/graph_catalog/graph_schema.rs` (+30 lines)
- `src/query_context.rs` (-31 lines)
- `src/render_plan/mod.rs` (-13 lines)

### Schema Files (Used by Tests)
- `benchmarks/social_network/schemas/social_benchmark.yaml` (primary test schema)
- `schemas/test/denormalized_flights.yaml` (edge case testing)

### Original Documentation
- `notes/with-cte-node-expansion-issue.md` (problem statement)

---

## Quick Commands Reference

```bash
# Setup
cd /home/gz/clickgraph
docker-compose up -d                    # Start ClickHouse
cargo run --bin clickgraph              # Start ClickGraph

# Run Tests
./scripts/test/run_with_cte_tests.sh    # All tests
./scripts/test/run_with_cte_tests.sh --test TestWithChaining  # Specific
./scripts/test/run_with_cte_tests.sh --verbose  # Verbose
./scripts/test/run_with_cte_tests.sh --show-sql # Show SQL

# Debug
curl http://localhost:8080/health       # Check ClickGraph
curl http://localhost:8123/ping         # Check ClickHouse
pytest tests/integration/test_with_cte_node_expansion.py -v -s

# Direct pytest
cd tests/integration
pytest test_with_cte_node_expansion.py::TestWithBasicNodeExpansion -v
pytest test_with_cte_node_expansion.py::TestWithChaining::test_with_chaining_two_levels -v
```

---

## Document Navigation

- **New to the fix?** → Start with [REVIEW_SUMMARY.md](REVIEW_SUMMARY.md)
- **Want to run tests?** → Go to [QUICK_TEST_REFERENCE.md](QUICK_TEST_REFERENCE.md)
- **Need test details?** → See [tests/integration/TEST_WITH_CTE_DOCUMENTATION.md](tests/integration/TEST_WITH_CTE_DOCUMENTATION.md)
- **Doing code review?** → Read [DESIGN_REVIEW_WITH_CTE_FIX.md](DESIGN_REVIEW_WITH_CTE_FIX.md)
- **Pre-merge checklist?** → Check [PRE_MERGE_VERIFICATION_CHECKLIST.md](PRE_MERGE_VERIFICATION_CHECKLIST.md)
- **What was created?** → See [TEST_CREATION_SUMMARY.md](TEST_CREATION_SUMMARY.md)

---

## Document Stats

| Document | Size | Purpose |
|----------|------|---------|
| QUICK_TEST_REFERENCE.md | 7 KB | Fast test execution guide |
| TEST_CREATION_SUMMARY.md | 10 KB | What was created |
| test_with_cte_node_expansion.py | 20 KB | Actual test code |
| DESIGN_REVIEW_WITH_CTE_FIX.md | 15 KB | Architecture review |
| TEST_WITH_CTE_DOCUMENTATION.md | 11 KB | Test reference |
| PRE_MERGE_VERIFICATION_CHECKLIST.md | 8 KB | Verification checklist |
| REVIEW_SUMMARY.md | 8 KB | Executive summary |
| run_with_cte_tests.sh | 6 KB | Test runner script |

**Total**: ~85 KB of documentation + 20 KB test code

---

## Status

✅ **COMPLETE AND READY**

- ✅ All 9 test scenarios implemented
- ✅ 15+ test methods with comprehensive assertions
- ✅ Test runner script created and tested
- ✅ Complete documentation (7 documents)
- ✅ Syntax validation passed
- ✅ Ready for execution against fix/with-chaining branch

---

## Next Steps

1. **Ensure servers running**:
   ```bash
   docker-compose up -d
   cargo run --bin clickgraph
   ```

2. **Run tests**:
   ```bash
   ./scripts/test/run_with_cte_tests.sh
   ```

3. **If all pass**: Ready to merge fix to main

4. **If any fail**: See troubleshooting in TEST_WITH_CTE_DOCUMENTATION.md

---

**Created**: January 26, 2026  
**For Branch**: `fix/with-chaining`  
**Status**: ✅ Complete & Ready
