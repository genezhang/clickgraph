# Schema Cleanup Summary - Phase 1 Complete

**Date**: December 20, 2025
**Status**: ✅ **Phase 1 Complete**

---

## ✅ Phase 1: Fix `relationships:` → `edges:` (COMPLETE)

### Files Updated (13 total)

All YAML schema files have been updated from the deprecated `relationships:` field to the standard `edges:` field:

#### Benchmark Schemas (1 file)
✅ `benchmarks/social_network/schemas/social_benchmark.yaml` - **PRIMARY BENCHMARK SCHEMA**

#### Helm Configuration (1 file)
✅ `helm/clickgraph/values.yaml` - Chart example configuration

#### Test Schemas (11 files)
✅ `tests/fixtures/schemas/filter_test.yaml`
✅ `tests/fixtures/schemas/test_property_expressions.yaml`
✅ `tests/integration/test_integration.yaml`
✅ `tests/integration/fixtures/schemas/zeek_conn_test.yaml`
✅ `tests/integration/fixtures/schemas/zeek_merged_test.yaml`
✅ `tests/integration/suites/test_integration/schema.yaml`
✅ `tests/integration/suites/social_benchmark/schema.yaml`
✅ `tests/integration/suites/shortest_paths/schema.yaml`
✅ `tests/integration/suites/variable_paths/schema.yaml`
✅ `tests/integration/suites/optional_match/schema.yaml`
✅ `tests/e2e/buckets/param_func/schema.yaml`

### Verification

```bash
# Before: 13+ files using "relationships:"
# After: 0 files using "relationships:" (excluding archive/)
find . -name "*.yaml" -not -path "*/archive/*" -exec grep -l "relationships:" {} \;
# Result: No matches
```

---

## 📊 Schema Files Analysis

### Current State

**Total YAML files**: 62 files
**Schema-related files**: ~40 files

### File Distribution

```
benchmarks/
├── ldbc_snb/schemas/ ................ 3 files (✅ uses edges:)
├── social_network/schemas/ .......... 1 file (✅ NOW uses edges:)
└── ontime_flights/schemas/ .......... 1 file (✅ uses edges:)

schemas/
├── demo/ ............................ 4 files (✅ uses edges:)
├── examples/ ........................ 15 files (✅ uses edges:)
├── test/ ............................ 10 files (✅ uses edges:)
└── tests/ ........................... 2 files (✅ uses edges:) [DUPLICATE DIR]

examples/ (root) ..................... 4 files (⚠️ scattered location)

tests/
├── fixtures/schemas/ ................ 2 files (✅ NOW uses edges:)
├── integration/fixtures/schemas/ .... 3 files (✅ NOW uses edges:)
└── integration/suites/*/schema.yaml . 7 files (✅ NOW uses edges:)
```

### Issues Remaining (Phase 2 & 3)

#### Directory Organization
- ❌ **Duplicate**: `schemas/test/` and `schemas/tests/` exist
- ❌ **Scattered**: Schema files in `examples/` (root level)
- ❌ **Mixed**: Test schemas in multiple locations

#### Tests Directory
- ❌ **Root clutter**: `tests/*.md`, `tests/debug_*.py`, `tests/test_*.py`
- ❌ **Unclear structure**: `tests/cypher/`, `tests/sql/`, `tests/data/`, `tests/python/`
- ❌ **Private files**: `tests/private/` (should be gitignored)
- ❌ **Logs**: `tests/*.log` (should be gitignored)

---

## 🎯 Impact Assessment

### What Changed
- **Breaking**: None (field rename is backward compatible in code)
- **Benefit**: All schemas now use consistent, modern `edges:` field name
- **Risk**: Low - code already handles both field names via serde aliases

### Files That Reference These Schemas
- ✅ Benchmark scripts (already using updated schemas)
- ✅ Test suites (using updated schemas)
- ✅ Documentation (references schema files, not field names)
- ✅ Rust code (uses `edges` field with serde alias for `relationships`)

### Validation Needed
1. Run integration tests: `pytest tests/integration/ -v`
2. Run benchmark queries
3. Test schema loading: `scripts/test/load_test_schemas.py`

---

## 📝 Recommended Next Steps

### Phase 2: Consolidate Schema Directories (Est: 30 min)

**Goal**: Reduce schema locations from 6 to 3

**Actions**:
1. Merge `schemas/demo/` → `schemas/examples/` (move 4 files)
2. Merge `schemas/tests/` → `schemas/test/` (move 2 files, remove dir)
3. Move `examples/*.yaml` → `schemas/examples/` (move 4 files)
4. Update 50+ references in scripts/docs

**Result**:
```
schemas/
├── examples/  (19 files - all example schemas)
└── test/      (12 files - all test schemas)

benchmarks/
├── ldbc_snb/schemas/
├── social_network/schemas/
└── ontime_flights/schemas/
```

### Phase 3: Clean Tests Directory (Est: 45 min)

**Goal**: Organize tests/ with clear structure

**Actions**:
1. Move `tests/*.md` → `docs/testing/` or `archive/`
2. Move `tests/debug_*.py` → `scripts/debug/`
3. Move `tests/test_*.py` → `tests/integration/`
4. Review and consolidate/remove:
   - `tests/cypher/`
   - `tests/sql/`
   - `tests/data/`
   - `tests/python/`
5. Add to `.gitignore`:
   - `tests/**/*.log`
   - `tests/private/`
6. Consolidate fixtures (decide between `tests/fixtures/` vs `tests/integration/fixtures/`)

**Result**:
```
tests/
├── unit/          (Rust/Python unit tests)
├── integration/   (Integration test suites)
│   ├── suites/
│   └── fixtures/
├── e2e/          (End-to-end tests)
└── regression/   (Regression tracking)
```

### Phase 4: Update Documentation (Est: 20 min)

**Files to update**:
- `.github/copilot-instructions.md`
- `README.md`
- `DEVELOPMENT_PROCESS.md`
- All benchmark READMEs

---

## ✅ Success Metrics (Phase 1)

- ✅ **Zero files** using deprecated `relationships:` field (goal: 0)
- ✅ **13 schemas updated** to use `edges:` field
- ✅ **PRIMARY BENCHMARK SCHEMA** now consistent (`social_benchmark.yaml`)
- ✅ **All test schemas** standardized
- ✅ **Helm chart example** updated

**Status**: ✅ **PHASE 1 COMPLETE - Ready for testing**

---

## 🧪 Validation Commands

```bash
# 1. Check for any remaining "relationships:" (should be 0)
find . -name "*.yaml" -not -path "*/archive/*" -exec grep -l "relationships:" {} \;

# 2. Build Rust (check for errors)
cd /home/gz/clickgraph && cargo build 2>&1 | tail -10

# 3. Run integration tests
pytest tests/integration/test_basic_queries.py -v
pytest tests/integration/test_optional_match.py -v

# 4. Test schema loading
python3 scripts/test/load_test_schemas.py

# 5. Test benchmark schema
export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"
./target/release/clickgraph &
sleep 3
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH (u:User) RETURN u LIMIT 1"}'
```

---

## 📝 Commit Message

```
fix: Standardize schema field name - relationships → edges (Phase 1)

## Changes
- Updated 13 YAML schema files to use standard `edges:` field
- Includes PRIMARY BENCHMARK SCHEMA (social_benchmark.yaml)
- All test schemas now consistent
- Helm chart example updated

## Impact
- Zero files now using deprecated `relationships:` field
- All schemas use modern `edges:` field name
- Backward compatible (code handles both via serde)

## Validation
- ✅ Build successful
- ✅ No remaining `relationships:` references
- ⏳ Integration tests pending

## Files Changed
### Benchmark
- benchmarks/social_network/schemas/social_benchmark.yaml

### Helm
- helm/clickgraph/values.yaml

### Tests (11 files)
- tests/fixtures/schemas/*.yaml (2)
- tests/integration/test_integration.yaml
- tests/integration/fixtures/schemas/*.yaml (2)
- tests/integration/suites/*/schema.yaml (5)
- tests/e2e/buckets/param_func/schema.yaml

## Next Steps
- Phase 2: Consolidate schema directories
- Phase 3: Clean up tests/ structure
```
