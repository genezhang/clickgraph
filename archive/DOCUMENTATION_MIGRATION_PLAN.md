# Documentation Standardization - Migration Plan

**Created**: October 18, 2025  
**Purpose**: Plan for bringing existing docs into compliance with DOCUMENTATION_STANDARDS.md  
**Priority**: Medium (can be done incrementally)

---

## 📋 Current Document Inventory

### Documents That Match Standards ✅
- `DEV_ENVIRONMENT_CHECKLIST.md` - Already follows checklist format
- `CHANGELOG.md` - Good structure, needs ViewScan entry
- `YAML_SCHEMA_INVESTIGATION.md` - Good investigation format

### Documents Needing Rename 🔄

| Current Name | Proposed Name | Type | Priority |
|-------------|---------------|------|----------|
| `SESSION_COMPLETE.md` | `SESSION_2025-10-18.md` | Session Summary | Low |
| `SESSION_SUMMARY_OCT17.md` | `SESSION_2025-10-17.md` | Session Summary | Low |
| `VIEWSCAN_IMPLEMENTATION_SUMMARY.md` | `VIEWSCAN_IMPLEMENTATION.md` | Feature Impl | High |
| `OPTIONAL_MATCH_COMPLETE.md` | `OPTIONAL_MATCH_IMPLEMENTATION.md` | Feature Impl | High |
| `OPTIONAL_MATCH_PROJECT_SUMMARY.md` | *(merge into above or delete)* | Duplicate? | High |

### Documents to Consolidate 📦

**Variable-Length Path Docs** (8 files!):
```
VARIABLE_LENGTH_DESIGN.md ✅ Keep (design doc)
VARIABLE_LENGTH_IMPLEMENTATION_GUIDE.md → Rename to VARIABLE_LENGTH_PATHS_IMPLEMENTATION.md
VARIABLE_LENGTH_FEATURE_COMPLETE.md ⚠️ Merge into above
VARIABLE_LENGTH_SUCCESS.md ⚠️ Merge into above
VARIABLE_LENGTH_STATUS.md ⚠️ Delete or merge
VARIABLE_LENGTH_PROGRESS_REPORT.md ⚠️ Archive
VARIABLE_LENGTH_TESTING.md ⚠️ Merge testing section into implementation
VARIABLE_LENGTH_VALIDATION.md ⚠️ Merge into implementation
```

**Suggested Result**: 2 files
- `VARIABLE_LENGTH_PATHS_DESIGN.md` (design)
- `VARIABLE_LENGTH_PATHS_IMPLEMENTATION.md` (complete implementation doc)

### Documents Needing Updates 📝

| Document | Updates Needed | Priority |
|----------|---------------|----------|
| `CHANGELOG.md` | Add ViewScan to [Unreleased] | High |
| `STATUS_REPORT.md` | Add ViewScan feature | High |
| `README.md` | Add ViewScan examples | Medium |
| `NEXT_STEPS.md` | Review against standards | Low |

---

## 🎯 Migration Tasks

### Phase 1: High Priority (Do First)

#### Task 1.1: Update CHANGELOG.md
```markdown
## [Unreleased] - 2025-10-18

### 🚀 Features

- **ViewScan Implementation**: View-based SQL translation for Cypher node queries
  - Label-to-table resolution via YAML schema (GLOBAL_GRAPH_SCHEMA)
  - Table alias propagation through ViewTableRef
  - Graceful fallback to regular Scan operations
  - Simple node queries fully working: `MATCH (u:User) RETURN u.name`
  - 261/262 tests passing (99.6% coverage)

### ⚙️ Infrastructure

- **HTTP Bind Error Handling**: Added descriptive error messages for port conflicts
- **Logging Framework**: Integrated env_logger for structured logging
- **Development Tools**: Batch files and PowerShell scripts for server startup
- **Environment Documentation**: DEV_ENVIRONMENT_CHECKLIST.md with Docker cleanup procedures

### 📚 Documentation

- Added `VIEWSCAN_IMPLEMENTATION.md` - Complete implementation details
- Added `SESSION_2025-10-18.md` - Development session summary
- Added `DOCUMENTATION_STANDARDS.md` - Project documentation conventions
- Added `NEXT_STEPS.md` - Roadmap and troubleshooting

### 🐛 Bug Fixes

- Fixed test_traverse_node_pattern_new_node to accept ViewScan or Scan

### 🧪 Testing

- Test suite: 261/262 passing (99.6%)
- End-to-end validation with real ClickHouse queries
- Only failure: test_version_string_formatting (Bolt protocol, unrelated)

### 🚧 Known Limitations

- ViewScan currently handles node queries only
- Relationship traversal queries need separate implementation
- OPTIONAL MATCH with relationships not yet tested with ViewScan
```

**Commands**:
```bash
# No Git command needed - just edit the file
```

---

#### Task 1.2: Update STATUS_REPORT.md

Add to "Latest Achievement" section:
```markdown
## 🎉 Latest Achievement: ViewScan Implementation (Oct 18, 2025)

### **✅ Feature: View-Based SQL Translation for Node Queries**
Cypher labels now correctly translate to ClickHouse table names via YAML schema!

#### **What Works**
```cypher
MATCH (u:User) RETURN u.name LIMIT 3
```
**Generates**: `SELECT u.name FROM users AS u LIMIT 3`  
**Returns**: `[{"name":"Alice"},{"name":"Bob"},{"name":"Charlie"}]` ✅

#### **Implementation Highlights**
- **Schema Lookup**: `try_generate_view_scan()` accesses GLOBAL_GRAPH_SCHEMA
- **Alias Propagation**: Cypher variable names flow through ViewTableRef to SQL
- **Graceful Fallback**: Falls back to regular Scan if schema unavailable
- **Test Coverage**: 261/262 tests passing (99.6%)

#### **Architecture**
- Query Planning: `match_clause.rs::generate_scan()` performs schema lookup
- Render Plan: `view_table_ref.rs` carries alias information
- SQL Generation: `to_sql_query.rs` uses explicit alias from ViewTableRef

#### **Current Scope**
✅ Simple node queries  
✅ Property selection  
✅ WHERE clauses on nodes  
❌ Relationship traversal (next priority)

See [VIEWSCAN_IMPLEMENTATION.md](VIEWSCAN_IMPLEMENTATION.md) for details.
```

Update Feature Matrix:
```markdown
| Feature | Status | Tests | Notes |
|---------|--------|-------|-------|
| ViewScan (Nodes) | ✅ Complete | 261/262 | Node queries working |
| ViewScan (Relationships) | 🚧 Planned | - | Next priority |
```

---

#### Task 1.3: Rename Feature Implementation Docs

```powershell
# Rename files to match standards
git mv VIEWSCAN_IMPLEMENTATION_SUMMARY.md VIEWSCAN_IMPLEMENTATION.md
git mv OPTIONAL_MATCH_COMPLETE.md OPTIONAL_MATCH_IMPLEMENTATION.md
git mv SESSION_COMPLETE.md SESSION_2025-10-18.md
git mv SESSION_SUMMARY_OCT17.md SESSION_2025-10-17.md

# Commit the renames
git commit -m "docs: Standardize documentation naming conventions

- Rename feature implementation docs to *_IMPLEMENTATION.md
- Rename session summaries to SESSION_YYYY-MM-DD.md
- Follow DOCUMENTATION_STANDARDS.md conventions"
```

---

### Phase 2: Medium Priority (This Week)

#### Task 2.1: Consolidate Variable-Length Path Docs

**Action Plan**:
1. Create comprehensive `VARIABLE_LENGTH_PATHS_IMPLEMENTATION.md`
2. Merge content from all status/success/validation files
3. Archive old files to `docs/archive/`
4. Update links in other docs

**Commands**:
```powershell
# After creating consolidated doc
mkdir docs\archive
git mv VARIABLE_LENGTH_FEATURE_COMPLETE.md docs\archive\
git mv VARIABLE_LENGTH_SUCCESS.md docs\archive\
git mv VARIABLE_LENGTH_STATUS.md docs\archive\
git mv VARIABLE_LENGTH_PROGRESS_REPORT.md docs\archive\
git mv VARIABLE_LENGTH_TESTING.md docs\archive\
git mv VARIABLE_LENGTH_VALIDATION.md docs\archive\

git commit -m "docs: Consolidate variable-length path documentation

- Created VARIABLE_LENGTH_PATHS_IMPLEMENTATION.md
- Archived incremental status docs to docs/archive/
- Kept VARIABLE_LENGTH_DESIGN.md as design reference"
```

---

#### Task 2.2: Update README.md

Add ViewScan example to main README:

```markdown
### View-Based Queries (YAML Schema)

ClickGraph uses YAML configuration to map Cypher labels to ClickHouse tables:

**YAML Configuration** (`social_network.yaml`):
```yaml
nodes:
  User:
    table: users
    id_column: user_id
    properties:
      - name
      - age
```

**Query**:
```cypher
MATCH (u:User) 
WHERE u.age > 25
RETURN u.name, u.age
```

**Generated SQL**:
```sql
SELECT u.name, u.age 
FROM users AS u 
WHERE u.age > 25
```

See [examples/quick-start.md](examples/quick-start.md) for more.
```

---

#### Task 2.3: Check for Duplicate Content

Review and potentially merge:
- `OPTIONAL_MATCH_PROJECT_SUMMARY.md` vs `OPTIONAL_MATCH_IMPLEMENTATION.md`
- `OPTIONAL_MATCH_DESIGN.md` vs implementation docs
- Multiple "notebook update" or "status" files

**Decision Criteria**:
- If content is identical → Delete duplicate
- If content is complementary → Merge into one doc
- If content is historical → Archive to `docs/archive/`

---

### Phase 3: Low Priority (When Convenient)

#### Task 3.1: Create docs/ Subdirectories

```powershell
# Organize docs by type
mkdir docs\sessions
mkdir docs\design
mkdir docs\archive
mkdir docs\investigations

# Move files (examples)
git mv SESSION_*.md docs\sessions\
git mv *_DESIGN.md docs\design\
git mv *_INVESTIGATION.md docs\investigations\
```

#### Task 3.2: Add Cross-References

Add "Related Documents" sections to major docs:
```markdown
## Related Documents

- Design: [VIEWSCAN_DESIGN.md](docs/design/VIEWSCAN_DESIGN.md) *(if exists)*
- Implementation: [VIEWSCAN_IMPLEMENTATION.md](VIEWSCAN_IMPLEMENTATION.md)
- Session: [SESSION_2025-10-18.md](docs/sessions/SESSION_2025-10-18.md)
```

#### Task 3.3: Create Doc Index

Create `docs/INDEX.md`:
```markdown
# ClickGraph Documentation Index

## Getting Started
- [README.md](../README.md) - Project overview
- [Getting Started Guide](getting-started.md)
- [Quick Start Examples](../examples/quick-start.md)

## Feature Documentation
- [OPTIONAL_MATCH_IMPLEMENTATION.md](../OPTIONAL_MATCH_IMPLEMENTATION.md)
- [VIEWSCAN_IMPLEMENTATION.md](../VIEWSCAN_IMPLEMENTATION.md)
- [VARIABLE_LENGTH_PATHS_IMPLEMENTATION.md](../VARIABLE_LENGTH_PATHS_IMPLEMENTATION.md)

## Development
- [STATUS_REPORT.md](../STATUS_REPORT.md) - Current project status
- [CHANGELOG.md](../CHANGELOG.md) - Release history
- [DOCUMENTATION_STANDARDS.md](../DOCUMENTATION_STANDARDS.md)

## Design Documents
- [design/](design/) - Architecture and design docs

## Session Summaries
- [sessions/](sessions/) - Development session records
```

---

## 🚀 Quick Start Commands

### Do Minimum Viable Standardization (5 minutes)

```powershell
# 1. Rename key docs
git mv VIEWSCAN_IMPLEMENTATION_SUMMARY.md VIEWSCAN_IMPLEMENTATION.md
git mv OPTIONAL_MATCH_COMPLETE.md OPTIONAL_MATCH_IMPLEMENTATION.md
git mv SESSION_COMPLETE.md SESSION_2025-10-18.md

# 2. Commit
git commit -m "docs: Rename to match DOCUMENTATION_STANDARDS.md conventions"

# 3. Edit CHANGELOG.md and STATUS_REPORT.md manually
# (Add ViewScan entries as shown in Task 1.1 and 1.2)

# 4. Commit updates
git add CHANGELOG.md STATUS_REPORT.md
git commit -m "docs: Update CHANGELOG and STATUS_REPORT with ViewScan"

# 5. Push
git push origin graphview1
```

**Time**: ~5 minutes  
**Impact**: High - Makes docs immediately more consistent

---

## ✅ Success Criteria

After migration is complete:

- [ ] All feature docs named `*_IMPLEMENTATION.md`
- [ ] All session docs named `SESSION_YYYY-MM-DD.md`
- [ ] CHANGELOG.md includes all recent features
- [ ] STATUS_REPORT.md reflects current state
- [ ] No duplicate/redundant docs in root directory
- [ ] All docs use consistent emoji conventions
- [ ] Test statistics in consistent format (X/Y passing, Z%)
- [ ] Dates in YYYY-MM-DD format in metadata

---

## 📊 Progress Tracking

| Phase | Tasks | Status | Priority |
|-------|-------|--------|----------|
| Phase 1 | 3 tasks | ⏳ Not Started | High |
| Phase 2 | 3 tasks | ⏳ Not Started | Medium |
| Phase 3 | 3 tasks | ⏳ Not Started | Low |

---

## 🤔 Open Questions

1. **Archive Strategy**: Should we create `docs/archive/` now or later?
2. **Session Docs**: Keep in root or move to `docs/sessions/` immediately?
3. **Variable-Length Consolidation**: Do we need all 8 documents or can we merge more aggressively?
4. **Old Investigation Reports**: Archive YAML_SCHEMA_INVESTIGATION.md or keep visible?

---

**Next Step**: Review this plan and decide which phase to start with!

**Recommendation**: Start with "Quick Start Commands" (5 minutes) to get immediate consistency, then tackle Phase 1 tasks as time permits.
