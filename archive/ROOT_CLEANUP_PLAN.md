# Root Directory Cleanup Analysis

## Current State: 51 Files in Root 😱

### Category Breakdown

#### ✅ KEEP (Essential - 13 files)
**Core Project Files:**
- `.dockerignore`
- `.gitignore`
- `Cargo.lock`
- `Cargo.toml`
- `LICENSE`
- `README.md`

**Docker/Build:**
- `docker-compose.yaml`
- `docker-compose.test.yaml`
- `Dockerfile`
- `Dockerfile.test`

**High-Level Docs (frequently referenced):**
- `CHANGELOG.md`
- `STATUS.md`
- `DEVELOPMENT_PROCESS.md`

---

#### 📂 MOVE TO tests/ (17 test files!)
**Bolt Protocol Tests:**
- `test_bolt_e2e.py` → `tests/e2e/`
- `test_bolt_handshake.py` → `tests/integration/bolt/`
- `test_bolt_hello.py` → `tests/integration/bolt/`
- `test_bolt_integration.py` → `tests/integration/bolt/`
- `test_bolt_protocol.py` → `tests/integration/bolt/`
- `test_bolt_simple.py` → `tests/integration/bolt/`
- `test_run_message.py` → `tests/integration/bolt/`

**Feature Tests:**
- `test_cache_error_handling.py` → `tests/integration/`
- `test_functions_final.py` → `tests/integration/`
- `test_functions_with_match.py` → `tests/integration/`
- `test_neo4j_functions.py` → `tests/integration/`
- `test_query_cache.py` → `tests/integration/`
- `test_query_cache_e2e.py` → `tests/e2e/`
- `test_http_use_clause.py` → `tests/integration/`
- `test_use_clause.py` → `tests/integration/`
- `test_param_func.py` → `tests/integration/`

**Rust Test:**
- `test_neo4rs_api.rs` → `tests/integration/` or `examples/`
- `explore_neo4rs_packstream.rs` → `examples/` or delete

---

#### 📂 MOVE TO scripts/ (3 utility scripts)
- `load_schema.py` → `scripts/utils/`
- `run_tests.py` → `scripts/test/`
- `verify_schema_load.py` → `scripts/utils/`
- `start_server_for_testing.ps1` → `scripts/server/` (already has this dir)

---

#### 📂 MOVE TO schemas/examples/ (1 schema file)
- `ecommerce_simple.yaml` → `schemas/examples/`
- `setup_demo_e2e.sql` → `tests/fixtures/data/`

---

#### 📂 MOVE TO docs/ (11 documentation files)
**Already should be in docs/:**
- `BETA_DISCLAIMER.md` → `docs/BETA_DISCLAIMER.md`
- `BOLT_PROTOCOL_STATUS.md` → `docs/features/bolt-protocol.md`
- `DEV_ENVIRONMENT_CHECKLIST.md` → `docs/development/`
- `GIT_WORKFLOW.md` → `docs/development/`
- `JOURNEY_RETROSPECTIVE.md` → `archive/` (historical)
- `KNOWN_ISSUES.md` → Could stay, but maybe `docs/KNOWN_ISSUES.md`
- `NEO4J_FUNCTIONS_PLAN.md` → `docs/features/` or `archive/`
- `PACKSTREAM_COMPLETE.md` → `docs/features/`
- `ROADMAP.md` → `docs/ROADMAP.md` or keep at root
- `TESTING_GUIDE.md` → `docs/development/`
- `UPGRADING.md` → `docs/UPGRADING.md`

---

#### 📂 MOVE TO archive/ (1 file - completed planning)
- `REORGANIZATION_PLAN.md` → `archive/` (this current doc, once done!)

---

#### 🗑️ CONSIDER MOVING TO assets/ (2 image files)
- `architecture.png` → `docs/images/` or `assets/`
- `cglogo.png` → `docs/images/` or `assets/`

---

## Proposed Clean Root Structure

**After cleanup: ~15 files maximum**

```
clickgraph/
├── .dockerignore              ← Build config
├── .gitignore                 ← Build config
├── Cargo.toml                 ← Rust workspace
├── Cargo.lock                 ← Rust dependencies
├── LICENSE                    ← Legal
├── README.md                  ← Entry point
├── CHANGELOG.md               ← Release history
├── STATUS.md                  ← Current state
├── DEVELOPMENT_PROCESS.md     ← Core dev guide
├── docker-compose.yaml        ← Main compose
├── docker-compose.test.yaml   ← Test compose
├── Dockerfile                 ← Main build
├── Dockerfile.test            ← Test build
├── KNOWN_ISSUES.md           ← Maybe keep at root (frequently checked)
└── ROADMAP.md                ← Maybe keep at root (high-level planning)
```

**Moved to appropriate locations:**
- 17 test files → `tests/`
- 4 utility scripts → `scripts/`
- 2 data files → `schemas/` or `tests/fixtures/`
- 11 docs → `docs/`
- 2 images → `docs/images/`
- 1 planning doc → `archive/`

---

## Migration Commands

### Phase 1: Tests to tests/ (Biggest impact)

```powershell
# Create test directories
mkdir tests/integration/bolt

# Move Bolt tests
git mv test_bolt_handshake.py tests/integration/bolt/
git mv test_bolt_hello.py tests/integration/bolt/
git mv test_bolt_integration.py tests/integration/bolt/
git mv test_bolt_protocol.py tests/integration/bolt/
git mv test_bolt_simple.py tests/integration/bolt/
git mv test_run_message.py tests/integration/bolt/

# Move E2E tests
git mv test_bolt_e2e.py tests/e2e/
git mv test_query_cache_e2e.py tests/e2e/

# Move feature tests
git mv test_cache_error_handling.py tests/integration/
git mv test_functions_final.py tests/integration/
git mv test_functions_with_match.py tests/integration/
git mv test_neo4j_functions.py tests/integration/
git mv test_query_cache.py tests/integration/
git mv test_http_use_clause.py tests/integration/
git mv test_use_clause.py tests/integration/
git mv test_param_func.py tests/integration/

# Move Rust examples
git mv test_neo4rs_api.rs examples/
git mv explore_neo4rs_packstream.rs examples/
```

### Phase 2: Scripts to scripts/

```powershell
mkdir scripts/utils

git mv load_schema.py scripts/utils/
git mv verify_schema_load.py scripts/utils/
git mv run_tests.py scripts/test/
git mv start_server_for_testing.ps1 scripts/server/
```

### Phase 3: Schemas/Data

```powershell
mkdir schemas/examples
mkdir tests/fixtures/data

git mv ecommerce_simple.yaml schemas/examples/
git mv setup_demo_e2e.sql tests/fixtures/data/
```

### Phase 4: Documentation

```powershell
mkdir docs/development
mkdir docs/features
mkdir docs/images

git mv BETA_DISCLAIMER.md docs/
git mv BOLT_PROTOCOL_STATUS.md docs/features/bolt-protocol.md
git mv DEV_ENVIRONMENT_CHECKLIST.md docs/development/
git mv GIT_WORKFLOW.md docs/development/
git mv NEO4J_FUNCTIONS_PLAN.md docs/features/neo4j-functions.md
git mv PACKSTREAM_COMPLETE.md docs/features/packstream.md
git mv TESTING_GUIDE.md docs/development/testing.md
git mv UPGRADING.md docs/

# Images
git mv architecture.png docs/images/
git mv cglogo.png docs/images/

# Optional: Keep these at root or move
# git mv ROADMAP.md docs/
# git mv KNOWN_ISSUES.md docs/
```

### Phase 5: Archive

```powershell
git mv JOURNEY_RETROSPECTIVE.md archive/
# After this cleanup is done:
# git mv REORGANIZATION_PLAN.md archive/
```

---

## Benefits

### Before:
- 51 files in root directory
- Hard to find what you need
- Test files mixed with docs mixed with configs
- Looks messy and unprofessional

### After:
- ~15 essential files in root
- Clear, professional structure
- Easy to navigate
- Follows Rust project conventions
- Tests where tests belong
- Docs organized by category

---

## Open Questions

1. **KNOWN_ISSUES.md** - Keep at root (frequently checked) or move to `docs/`?
   - **Recommendation**: Keep at root for visibility

2. **ROADMAP.md** - Keep at root (high-level) or move to `docs/`?
   - **Recommendation**: Keep at root (like many projects)

3. **Test files** - Some might be one-off debug tests. Delete vs keep?
   - **Recommendation**: Keep all initially, can prune later if unused

4. **REORGANIZATION_PLAN.md** - Archive immediately or after completion?
   - **Recommendation**: Archive after PR is merged

---

## Risks & Mitigation

### Risk 1: Breaking Import Paths in Tests
**Mitigation:**
- Test files might import from relative paths
- Check imports in each file before moving
- Update GitHub Actions workflows if needed

### Risk 2: Breaking Documentation Links
**Mitigation:**
- Update README.md links
- Update STATUS.md references
- Search for absolute path references

### Risk 3: Git History
**Mitigation:**
- Use `git mv` for all moves (preserves history)
- Commit each phase separately with clear messages

---

## Recommendation

**Execute in phases:**

1. **Phase 1: Tests** (17 files) - Biggest visual impact
2. **Phase 2: Scripts** (4 files) - Quick win
3. **Phase 3: Data** (2 files) - Easy
4. **Phase 4: Docs** (11 files) - Requires link updates
5. **Phase 5: Archive** (1-2 files) - Final cleanup

**Time estimate**: 30-45 minutes total

**Ready to proceed?** Start with Phase 1 (tests)?
