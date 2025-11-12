# Repository Reorganization Plan

**Date**: November 12, 2025  
**Problem**: Files scattered everywhere, hard to find benchmark-related work, unclear structure  
**Goals**: Clean, intuitive structure following Rust conventions

---

## Current Problems

### 1. **Rust Project Structure** 🦀
- ❌ Unnecessary `brahmand/` subdirectory (uncommon for single-crate projects)
- ❌ Workspace with only 2 members (overkill for this size)
- ✅ Should follow standard Rust layout: `src/`, `tests/`, `examples/` at root

**Rust Convention**: Single-crate projects put `src/` at root, NOT in subdirectory.
- Cargo workspaces are for **multi-crate projects** (like `tokio/`, `serde/`)
- Our case: Main crate + small client → Simpler to have main at root

### 2. **Test Files Scattered** 🗂️
```
Root level: 17 test files (test_*.py, test_*.rs)
tests/python/: 50+ test files
tests/cypher/: Cypher test files
tests/integration/: Integration tests
tests/e2e/: E2E tests
```
**Problem**: Can't find related tests, no clear organization

### 3. **Benchmark Files Everywhere** 📊
```
Root: BENCHMARKS.md
benchmark/: Old benchmark directory
tests/python/: 5+ benchmark scripts (generate_*, test_benchmark_*)
tests/python/setup_benchmark_unified.py: NEW unified benchmark
schemas/demo/social_benchmark.yaml: Benchmark schema
docker-compose.benchmark.yaml: Benchmark compose file
```
**Problem**: Benchmark work is fragmented across 4+ locations

### 4. **Documentation Sprawl** 📚
```
Root: 20+ markdown files
docs/: Documentation directory
notes/: Feature notes
archive/: Archived docs
```
**Problem**: Hard to find current, relevant docs

### 5. **Scripts and Utilities** 🛠️
```
Root: load_schema.py, run_tests.py, verify_schema_load.py, etc.
scripts/: PowerShell scripts
tests/python/: Utility scripts mixed with tests
```
**Problem**: No clear separation of utilities vs tests

---

## Proposed Structure

### Standard Rust Project Layout

```
clickgraph/
├── .github/              # GitHub workflows, etc.
├── .vscode/              # VS Code settings
│
├── src/                  # ✨ Main source (moved from brahmand/src/)
│   ├── main.rs
│   ├── lib.rs
│   ├── open_cypher_parser/
│   ├── query_planner/
│   ├── clickhouse_query_generator/
│   ├── server/
│   └── graph_catalog/
│
├── tests/                # ✨ Reorganized tests
│   ├── unit/            # Unit tests (from brahmand/tests/)
│   │   ├── parser/
│   │   ├── planner/
│   │   └── generator/
│   ├── integration/     # Integration tests (HTTP API)
│   │   ├── test_cypher_queries.rs
│   │   └── test_schema_loading.rs
│   ├── e2e/             # End-to-end tests
│   │   ├── test_optional_match_e2e.py
│   │   ├── test_multi_schema_end_to_end.py
│   │   └── test_pagerank_multi_graph.py
│   └── fixtures/        # Test data, schemas
│       ├── schemas/
│       └── data/
│
├── benchmarks/           # ✨ All benchmark work in ONE place
│   ├── README.md        # Benchmark overview
│   ├── data/            # Data generation
│   │   ├── setup_unified.py         # Main data generator
│   │   ├── generate_large_scale.py
│   │   └── verify_data.py
│   ├── queries/         # Benchmark queries
│   │   ├── suite.py     # Main test suite (16 queries)
│   │   ├── final.py     # Final benchmark
│   │   └── medium.py    # Medium benchmark
│   ├── schemas/         # Benchmark-specific schemas
│   │   └── social_benchmark.yaml
│   ├── results/         # Benchmark results (gitignored)
│   │   └── .gitkeep
│   └── docker-compose.benchmark.yaml
│
├── examples/             # Example code, demos
│   └── simple_query.rs
│
├── scripts/              # Utility scripts (keep as is)
│   ├── setup/           # Setup scripts
│   ├── test/            # Test runners
│   └── utils/           # Utilities
│
├── docs/                 # ✨ Organized documentation
│   ├── architecture/    # Architecture docs
│   ├── features/        # Feature guides (from notes/)
│   ├── development/     # Dev guides
│   └── api/             # API documentation
│
├── schemas/              # Production schemas
│   ├── demo/            # Demo schemas
│   └── examples/        # Example schemas
│
├── brahmand-client/      # Keep as separate crate
│   ├── Cargo.toml
│   └── src/
│
├── archive/              # Keep old docs (as is)
│
├── target/               # Build output (gitignored)
├── clickhouse_data/      # Docker data (gitignored)
│
├── Cargo.toml            # ✨ Simplified workspace or single crate
├── Cargo.lock
├── README.md
├── CHANGELOG.md
├── STATUS.md
├── LICENSE
└── docker-compose.yaml

```

---

## Key Changes

### 1. **Flatten Rust Structure** 🦀

**Before**:
```
Cargo.toml (workspace)
brahmand/
  ├── Cargo.toml
  ├── src/
  │   ├── main.rs
  │   └── lib.rs
  └── tests/
```

**After** (Option A - Single Crate):
```
Cargo.toml (package)
src/
  ├── main.rs
  └── lib.rs
tests/
  └── unit/
```

**After** (Option B - Simplified Workspace):
```
Cargo.toml (workspace)
clickgraph/          # Main crate (was brahmand/)
  ├── Cargo.toml
  └── src/
brahmand-client/     # Keep as separate
  └── src/
```

**Recommendation**: **Option A** (Single Crate) unless you plan multiple published crates.

### 2. **Consolidate Benchmarks** 📊

**Move to `benchmarks/`**:
- `tests/python/setup_benchmark_unified.py` → `benchmarks/data/setup_unified.py`
- `tests/python/test_benchmark_suite.py` → `benchmarks/queries/suite.py`
- `tests/python/test_benchmark_final.py` → `benchmarks/queries/final.py`
- `tests/python/test_medium_benchmark.py` → `benchmarks/queries/medium.py`
- `tests/python/generate_large_benchmark_data.py` → `benchmarks/data/generate_large_scale.py`
- `tests/python/generate_medium_benchmark_data.py` → `benchmarks/data/generate_medium_scale.py`
- `schemas/demo/social_benchmark.yaml` → `benchmarks/schemas/social_benchmark.yaml`
- `docker-compose.benchmark.yaml` → `benchmarks/docker-compose.benchmark.yaml`
- `BENCHMARKS.md` → `benchmarks/README.md`

### 3. **Reorganize Tests** 🧪

**Move to `tests/`**:
- `brahmand/tests/*` → `tests/unit/`
- Root `test_*.py` files → `tests/e2e/` or `tests/integration/`
- `tests/python/test_*_e2e.py` → `tests/e2e/`
- `tests/python/test_optional_match.py` → `tests/integration/`

**Keep clean separation**:
- `tests/unit/` - Rust unit tests (no server needed)
- `tests/integration/` - HTTP API tests (server needed)
- `tests/e2e/` - Full end-to-end scenarios
- `tests/fixtures/` - Shared test data

### 4. **Consolidate Documentation** 📚

**Move to `docs/`**:
- `notes/*.md` → `docs/features/`
- Architecture docs → `docs/architecture/`
- Dev guides → `docs/development/`

**Keep at root** (high-level):
- README.md
- CHANGELOG.md
- STATUS.md
- LICENSE
- DEVELOPMENT_PROCESS.md

### 5. **Clean Up Root** 🧹

**Move or remove**:
- `test_*.py` (17 files) → `tests/e2e/` or `tests/integration/`
- `*.yaml` schema files → `schemas/examples/`
- `*.sql` files → `tests/fixtures/data/`
- `*.log` files → Delete or gitignore
- Debug scripts → `scripts/debug/` or delete

---

## Migration Steps

### Phase 1: Rust Structure (30 min)
```bash
# Option A: Single Crate (Recommended)
mv brahmand/src ./
mv brahmand/Cargo.toml ./Cargo.toml.new
# Merge Cargo.toml files
rm -rf brahmand/

# Update Cargo.toml to single crate
# Update all import paths (clickgraph:: instead of brahmand::)
```

### Phase 2: Benchmarks (20 min)
```bash
mkdir -p benchmarks/{data,queries,schemas,results}
mv tests/python/setup_benchmark_unified.py benchmarks/data/setup_unified.py
mv tests/python/test_benchmark_suite.py benchmarks/queries/suite.py
mv tests/python/test_benchmark_final.py benchmarks/queries/final.py
mv tests/python/test_medium_benchmark.py benchmarks/queries/medium.py
mv tests/python/generate_large_benchmark_data.py benchmarks/data/generate_large_scale.py
mv tests/python/generate_medium_benchmark_data.py benchmarks/data/generate_medium_scale.py
mv schemas/demo/social_benchmark.yaml benchmarks/schemas/
mv docker-compose.benchmark.yaml benchmarks/
mv BENCHMARKS.md benchmarks/README.md
```

### Phase 3: Tests (30 min)
```bash
mkdir -p tests/{unit,integration,e2e,fixtures}
mv brahmand/tests/* tests/unit/
mv tests/python/test_*_e2e.py tests/e2e/
mv tests/python/test_optional_match.py tests/integration/
mv tests/python/test_multi_schema_end_to_end.py tests/e2e/
# Move root test_*.py files appropriately
```

### Phase 4: Documentation (15 min)
```bash
mkdir -p docs/{features,architecture,development}
mv notes/*.md docs/features/
# Keep high-level docs at root
```

### Phase 5: Clean Root (10 min)
```bash
# Move remaining test files
# Move debug scripts to scripts/debug/
# Update all import paths
# Update README with new structure
```

---

## Benefits

### 1. **Standard Rust Layout** ✅
- Follows `cargo` conventions
- Easier for Rust developers to navigate
- Simpler CI/CD (no workspace complexity)

### 2. **Clear Organization** ✅
- Benchmarks all in one place
- Tests organized by type
- Documentation structured
- Easy to find related files

### 3. **Better Discovery** ✅
```
Want to run benchmarks? → benchmarks/
Want to add tests? → tests/
Want to read feature docs? → docs/features/
Want to see examples? → examples/
```

### 4. **Cleaner Root** ✅
- Only essential files at root
- No test files cluttering
- Professional appearance

### 5. **Scalability** ✅
- Easy to add new benchmarks
- Clear where new tests go
- Documentation structure scales

---

## Risks & Mitigation

### Risk 1: Breaking Import Paths
**Mitigation**: 
- Use search & replace for `brahmand::` → `clickgraph::`
- Update all `use` statements
- Test after each phase

### Risk 2: Breaking CI/CD
**Mitigation**:
- Update GitHub Actions paths
- Test locally before pushing
- Update docker-compose paths

### Risk 3: Breaking Documentation Links
**Mitigation**:
- Find & replace doc links
- Update STATUS.md
- Check all relative paths

### Risk 4: Git History
**Mitigation**:
- Use `git mv` to preserve history
- Commit each phase separately
- Document moves in commit messages

---

## Alternative: Minimal Reorganization

If full reorganization is too risky, start with **just benchmarks**:

```bash
# Quick win: Consolidate benchmarks only
mkdir -p benchmarks/{data,queries,schemas}
mv tests/python/setup_benchmark_unified.py benchmarks/data/
mv tests/python/test_benchmark_suite.py benchmarks/queries/
mv schemas/demo/social_benchmark.yaml benchmarks/schemas/
mv BENCHMARKS.md benchmarks/README.md

# Update paths in:
# - scripts/test_windows_mergetree_simple.ps1
# - docker-compose files
# - README.md
```

Then iterate on other areas over time.

---

## Recommendation

**Start with Benchmarks + Rust Structure**:
1. ✅ Consolidate benchmarks (immediate value, low risk)
2. ✅ Flatten Rust structure (follows conventions)
3. ⏳ Tests reorganization (Phase 2)
4. ⏳ Documentation (Phase 3)

This gives you:
- Clean benchmark workflow immediately
- Standard Rust layout
- Foundation for further cleanup

**Time**: ~1-2 hours for phases 1-2, rest can be iterative.

---

## Next Steps

1. **Review this plan** - Does structure make sense?
2. **Choose approach** - Full or minimal reorganization?
3. **Execute Phase 1** - Start with benchmarks
4. **Test thoroughly** - Ensure nothing breaks
5. **Iterate** - Clean up remaining areas

Ready to proceed? 🚀
