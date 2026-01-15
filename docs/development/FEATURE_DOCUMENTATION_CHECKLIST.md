# Feature Documentation Checklist

## Purpose

**Prevent documentation inconsistency** that leads to incorrect feature assessments and user confusion.

**Trigger**: Use this checklist for EVERY feature before marking it "complete".

---

## ✅ Documentation Checklist

### 1. Primary Reference Documentation

**Cypher Language Reference** (`docs/wiki/Cypher-Language-Reference.md`):
- [ ] Feature added to Table of Contents
- [ ] Dedicated section with clear title
- [ ] Syntax documented with examples
- [ ] Common patterns shown
- [ ] Common errors documented with solutions
- [ ] Cross-references to related features

**Criteria**: User can learn the feature from this doc alone.

### 2. Implementation Documentation

**Feature Notes** (`notes/<feature>.md`):
- [ ] How It Works (architecture overview)
- [ ] Key Files (implementation locations)
- [ ] Design Decisions (why implemented this way)
- [ ] Gotchas (edge cases, limitations)
- [ ] Future Work (planned improvements)

**Criteria**: Developer can maintain/extend the feature.

### 3. API Documentation

**HTTP API Reference** (`docs/api.md`):
- [ ] New parameters documented
- [ ] Request format shown
- [ ] Response format shown
- [ ] Example curl commands
- [ ] Error cases documented

**Criteria**: User can integrate feature via API.

### 4. Configuration Documentation

**Schema Configuration** (`docs/wiki/Schema-Configuration-Advanced.md`):
- [ ] YAML syntax for feature
- [ ] Configuration options explained
- [ ] Example configurations
- [ ] Interaction with other features

**Criteria**: User can configure feature in schema.

### 5. Testing Documentation

**Test Coverage**:
- [ ] Unit tests exist
- [ ] Integration tests exist
- [ ] Test patterns documented in code comments
- [ ] Known limitations noted in test files

**Criteria**: Tests demonstrate feature works.

### 6. Status Documentation

**STATUS.md**:
- [ ] Feature added to "What Works" section
- [ ] Test statistics updated
- [ ] Examples provided
- [ ] Known limitations noted

**Criteria**: Status doc reflects current capabilities.

### 7. Changelog

**CHANGELOG.md**:
- [ ] Feature added under [Unreleased] or version
- [ ] Clear description
- [ ] Breaking changes noted (if any)
- [ ] Migration guide (if needed)

**Criteria**: Users know feature exists and how to migrate.

---

## 📋 Feature-Specific Checklists

### Query Language Features (e.g., USE clause, OPTIONAL MATCH)

- [ ] Cypher Language Reference (primary)
- [ ] Feature note in `notes/`
- [ ] Test coverage (unit + integration)
- [ ] STATUS.md updated
- [ ] CHANGELOG.md entry
- [ ] Examples in `examples/` (optional)

### API Features (e.g., parameters, view_parameters, role)

- [ ] Cypher Language Reference (if Cypher syntax)
- [ ] API Reference (HTTP endpoints)
- [ ] Enterprise Features section (if multi-tenancy/security)
- [ ] Schema Configuration (if YAML config needed)
- [ ] Test coverage
- [ ] STATUS.md + CHANGELOG.md

### Schema Features (e.g., view_parameters, polymorphic edges)

- [ ] Schema Configuration Advanced
- [ ] Cypher Language Reference (if query syntax changes)
- [ ] Feature note with YAML examples
- [ ] Test schemas in `tests/fixtures/`
- [ ] STATUS.md + CHANGELOG.md

---

## 🚨 Red Flags (Documentation Debt)

**Warning signs of incomplete documentation**:

- ❌ Feature implemented but not in Cypher Language Reference
- ❌ Tests passing but no documentation
- ❌ API parameter exists but not in api.md
- ❌ YAML config option but not documented
- ❌ Users asking "does X feature exist?" for implemented features
- ❌ Tests failing due to incorrect usage (documentation bug)

**If ANY red flag appears**: Stop and complete documentation before proceeding.

---

## 🎯 Pre-Release Checklist

Before marking ANY version as "ready to ship":

- [ ] All implemented features in Cypher Language Reference
- [ ] All API parameters documented
- [ ] All YAML schema options documented
- [ ] STATUS.md "What Works" section is accurate
- [ ] CHANGELOG.md is up to date
- [ ] No documentation red flags
- [ ] Cross-reference check: docs mention features that exist, don't mention features that don't

**Verification Method**: 
1. List all features from STATUS.md "What Works"
2. Verify each has entry in Cypher Language Reference
3. Verify each has test coverage
4. Verify each has API documentation (if API-exposed)

---

## 💡 Documentation Quality Standards

### Good Documentation Has:

1. **Completeness**: All aspects of feature covered
2. **Clarity**: Beginner can understand and use
3. **Examples**: Real, working code samples
4. **Errors**: Common mistakes shown with fixes
5. **Context**: Why/when to use feature
6. **Cross-refs**: Links to related features

### Documentation Review Questions:

- Can a new user learn this feature from the docs alone?
- Are common mistakes documented with solutions?
- Are examples copy-pasteable and working?
- Does documentation match implementation?
- Is terminology consistent across all docs?

---

## 🔧 How to Use This Checklist

### During Feature Development:

1. **Design phase**: Sketch documentation structure
2. **Implementation phase**: Update docs as you code
3. **Testing phase**: Verify examples work
4. **Completion**: Run through full checklist

### During Code Review:

- Reviewer checks documentation checklist
- No feature merged without documentation
- Documentation quality = code quality

### During Release:

- Run pre-release checklist
- Fix any documentation gaps BEFORE release
- Update STATUS.md with release notes

---

## 📚 Reference: November 22, 2025 Incident

**What Went Wrong**:
- USE clause implemented in v0.5.1
- Documented in Multi-Tenancy-RBAC.md and Schema-Configuration-Advanced.md
- **NOT documented in Cypher Language Reference** (primary reference)
- Led to confusion: "Is USE clause implemented?"

**Impact**:
- 160 test failures incorrectly assessed as feature regressions
- Time wasted investigating "missing" features
- Potential user confusion

**Fix**:
- Added comprehensive USE clause section to Cypher Language Reference
- Added Enterprise Features section
- Created quick reference guides

**Lesson**: Primary documentation (Cypher Language Reference) MUST be complete and authoritative. Secondary docs are supplements, not substitutes.

---

## ✅ Success Criteria

**Documentation is complete when**:

1. ✅ Feature appears in Cypher Language Reference with examples
2. ✅ Implementation documented in notes/
3. ✅ API parameters documented (if applicable)
4. ✅ Schema configuration documented (if applicable)
5. ✅ Tests exist and pass
6. ✅ STATUS.md reflects feature
7. ✅ CHANGELOG.md mentions feature
8. ✅ No red flags
9. ✅ Peer review passed
10. ✅ User can successfully use feature from documentation alone

**When in doubt**: If you wouldn't want to support users with just this documentation, it's not ready.

---

## 📞 Questions to Ask

Before marking feature complete:

- "Can I find this feature in Cypher Language Reference?"
- "Can a user learn this feature without asking us?"
- "Will this feature surprise users in 6 months?" (if not documented)
- "Would I ship a product with this documentation?"

If any answer is NO, documentation is incomplete.
