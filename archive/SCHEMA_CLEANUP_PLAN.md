# Schema & Tests Cleanup Plan

**Date**: December 20, 2025
**Issue**: Scattered schema files, inconsistent `relationships:` vs `edges:` usage, messy tests/ directory

---

## 🔍 Current State Analysis

### Schema Files (62 YAML files total)

**Problem Areas**:
1. ❌ **Still using `relationships:`** (should be `edges:`):
   - `benchmarks/social_network/schemas/social_benchmark.yaml` ⚠️ **PRIMARY SCHEMA**
   - `helm/clickgraph/values.yaml`
   - `tests/fixtures/schemas/filter_test.yaml`
   - `tests/integration/test_integration.yaml`
   - `tests/e2e/buckets/param_func/schema.yaml`
   - `tests/integration/suites/*/schema.yaml` (7 files)
   - `tests/integration/fixtures/schemas/*.yaml` (3 files)

2. ❌ **Duplicate/Scattered Locations**:
   - `schemas/demo/` - Demo schemas
   - `schemas/examples/` - Example schemas (15 files)
   - `schemas/test/` - Test schemas (10 files)
   - `schemas/tests/` - More test schemas! (2 files)
   - `tests/fixtures/schemas/` - Test fixtures (2 files)
   - `tests/integration/suites/*/schema.yaml` - Per-suite schemas (7 files)
   - `examples/*.yaml` - Root-level examples (4 files)

3. ❌ **Obsolete Files**:
   - `examples/ecommerce_graph_demo.yaml` - Duplicate of `schemas/demo/ecommerce_graph_demo.yaml`
   - `examples/social_network_view.yaml` - Old views format
   - `schemas/demo/multi_graph_benchmark.yaml` - Moved to `archive/`

### Tests Directory Issues

**Problem Areas**:
1. ❌ **Root-level test files** (should be in subdirectories):
   - `tests/*.md` (5 regression/planning docs) → Should be in `docs/testing/` or `archive/`
   - `tests/debug_*.py` (3 files) → Should be in `scripts/debug/`
   - `tests/test_*.py` (3 files) → Should be in `tests/integration/` or `tests/unit/`
   - `tests/*.log` (1 file) → Should be gitignored

2. ❌ **Confusing structure**:
   - `tests/fixtures/` vs `tests/integration/fixtures/` (why two?)
   - `tests/cypher/`, `tests/sql/`, `tests/data/` - what are these?
   - `tests/private/` - should this exist?
   - `tests/python/` - redundant (all tests are Python!)

---

## ✅ Proposed Directory Structure

### Schemas (Simplified)

```
schemas/
├── benchmarks/           # Production benchmark schemas (canonical sources)
│   ├── social_benchmark.yaml
│   ├── ontime_benchmark.yaml
│   └── ldbc_snb_complete.yaml
│
├── examples/            # Example schemas for documentation
│   ├── ecommerce_simple.yaml
│   ├── filesystem.yaml
│   ├── social_polymorphic.yaml
│   ├── zeek_*.yaml (3 files)
│   ├── multi_tenant_*.yaml (2 files)
│   └── ontime_denormalized.yaml
│
└── test/               # Test-specific schemas only
    ├── composite_node_ids.yaml
    ├── expression_test.yaml
    ├── filter_test.yaml
    └── multi_tenant.yaml

# REMOVE these directories:
# - schemas/demo/ → Merge into examples/ or move to docs/examples/
# - schemas/tests/ → Merge into test/
# - examples/*.yaml → Move to schemas/examples/
```

### Tests (Organized)

```
tests/
├── unit/               # Unit tests (Rust or Python)
├── integration/        # Integration tests
│   ├── conftest.py
│   ├── suites/        # Test suites
│   │   ├── social_benchmark/
│   │   ├── optional_match/
│   │   └── variable_paths/
│   ├── matrix/        # Schema matrix tests
│   ├── wiki/          # Wiki examples as tests
│   └── fixtures/      # Test data & schemas
│       ├── data/
│       └── schemas/
├── e2e/               # End-to-end tests
└── regression/        # Regression test tracking
    └── *.md files

# REMOVE from tests/:
# - debug_*.py → scripts/debug/
# - test_*.py (root level) → tests/integration/
# - *.md (root level) → docs/testing/ or archive/
# - *.log → .gitignore
# - cypher/, sql/, data/, python/ → Consolidate or remove
# - private/ → Review and remove
```

---

## 🔧 Cleanup Actions

### Phase 1: Fix `relationships:` → `edges:` (CRITICAL)

**Files to update** (17 files):
1. ✅ `benchmarks/social_network/schemas/social_benchmark.yaml` - **PRIMARY SCHEMA**
2. ✅ `helm/clickgraph/values.yaml`
3. ✅ `tests/fixtures/schemas/filter_test.yaml`
4. ✅ `tests/integration/test_integration.yaml`
5. ✅ `tests/e2e/buckets/param_func/schema.yaml`
6. ✅ `tests/integration/suites/shortest_paths/schema.yaml`
7. ✅ `tests/integration/suites/variable_paths/schema.yaml`
8. ✅ `tests/integration/suites/optional_match/schema.yaml`
9. ✅ `tests/integration/suites/test_integration/schema.yaml`
10. ✅ `tests/integration/suites/social_benchmark/schema.yaml`
11. ✅ `tests/integration/fixtures/schemas/zeek_conn_test.yaml`
12. ✅ `tests/integration/fixtures/schemas/zeek_merged_test.yaml`
13. ✅ `tests/fixtures/schemas/test_property_expressions.yaml`

**Command**:
```bash
find . -name "*.yaml" -exec grep -l "relationships:" {} \; | grep -v archive | grep -v node_modules
```

### Phase 2: Consolidate Schema Files

**Actions**:
1. ✅ Move `schemas/demo/*.yaml` → `schemas/examples/` (merge directories)
2. ✅ Move `schemas/tests/*.yaml` → `schemas/test/` (merge directories)
3. ✅ Move `examples/*.yaml` → `schemas/examples/` (consolidate)
4. ✅ Remove duplicate files:
   - `examples/ecommerce_graph_demo.yaml` (duplicate)
   - `examples/social_network_view.yaml` (obsolete)
5. ✅ Update references in:
   - Scripts (48 references)
   - Documentation (25 references)
   - Test files (30 references)

### Phase 3: Clean Up Tests Directory

**Actions**:
1. ✅ Move `tests/*.md` → `docs/testing/` or `archive/`
2. ✅ Move `tests/debug_*.py` → `scripts/debug/`
3. ✅ Move `tests/test_*.py` (root) → `tests/integration/`
4. ✅ Review and remove:
   - `tests/cypher/` (unused?)
   - `tests/sql/` (unused?)
   - `tests/data/` (consolidate to fixtures?)
   - `tests/python/` (redundant)
   - `tests/private/` (should be gitignored)
5. ✅ Add to `.gitignore`:
   - `tests/**/*.log`
   - `tests/private/`
   - `tests/__pycache__/`

### Phase 4: Update Documentation

**Files to update**:
- `.github/copilot-instructions.md` - Update schema paths
- `README.md` - Update quick start examples
- `STATUS.md` - Update schema references
- All `benchmarks/*/README.md` files
- Test suite README files

---

## 📊 Risk Assessment

**Low Risk** (safe to proceed):
- ✅ Fixing `relationships:` → `edges:` (already done in 30+ files)
- ✅ Moving files between schema directories
- ✅ Consolidating test directories

**Medium Risk** (test after):
- ⚠️ Updating 100+ references across scripts/docs
- ⚠️ Removing duplicate files (verify no unique content)

**High Risk** (requires validation):
- ❌ Removing `tests/cypher/`, `tests/sql/` without understanding purpose
- ❌ Deleting anything in `tests/private/` without review

---

## ✅ Validation Steps

After each phase:
1. Run full test suite: `pytest tests/integration/ -v`
2. Build Rust: `cargo build`
3. Check schema loading: `scripts/test/load_test_schemas.py`
4. Verify benchmarks still work
5. Test with primary schema: `export GRAPH_CONFIG_PATH=benchmarks/social_network/schemas/social_benchmark.yaml`

---

## 📝 Next Steps

1. **Execute Phase 1** (fix `relationships:` → `edges:`)
2. Run validation tests
3. **Execute Phase 2** (consolidate schemas)
4. Update all references
5. Run full regression tests
6. **Execute Phase 3** (clean tests/)
7. Final validation
8. Commit with detailed message

---

## 🎯 Success Metrics

- ✅ Zero files using `relationships:` (should be `edges:`)
- ✅ Schema files in 3 locations only: `benchmarks/`, `schemas/examples/`, `schemas/test/`
- ✅ Tests directory with clear structure (unit/, integration/, e2e/, regression/)
- ✅ All tests passing after cleanup
- ✅ Documentation updated with new paths
