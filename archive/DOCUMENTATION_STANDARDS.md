# Documentation Standards for ClickGraph

**Created**: October 18, 2025  
**Purpose**: Establish consistent documentation conventions across the project

---

## 📁 Document Types & Naming Conventions

### 1. **CHANGELOG.md** (Project-Wide)
**Purpose**: Official release history and version tracking  
**Naming**: Always `CHANGELOG.md` (singular, root directory)  
**Format**: Keep-a-Changelog standard

**Structure**:
```markdown
# Changelog

## [Unreleased] - YYYY-MM-DD

### 🚀 Features
### 🐛 Bug Fixes
### 📚 Documentation
### 🧪 Testing
### ⚙️ Infrastructure
### 💥 Breaking Changes

## [X.Y.Z] - YYYY-MM-DD
```

**Guidelines**:
- Use emoji prefixes for categories
- List items in order of importance
- Link to relevant docs or PR numbers
- Keep entries concise but informative
- Only update when merging to main branch

---

### 2. **Feature Documentation** (Implementation Records)
**Purpose**: Document completed features for permanent reference  
**Naming**: `<FEATURE>_IMPLEMENTATION.md` (e.g., `OPTIONAL_MATCH_IMPLEMENTATION.md`)  
**Location**: Root directory for major features  

**Structure**:
```markdown
# <Feature Name> Implementation

**Date**: YYYY-MM-DD  
**Branch**: branch-name  
**Status**: ✅ Complete / 🚧 In Progress / ⏸️ Paused  
**Test Coverage**: X/Y tests passing (Z%)

---

## Overview

[2-3 sentence summary of what was built]

## Implementation Details

### What Was Built
[Key functionality]

### Architecture
[Design decisions, data flow]

### Code Changes
[Files modified with key changes]

### Testing
[Test results, coverage]

## Usage Examples

[Practical examples with code]

## Known Limitations

[What doesn't work yet]

## Next Steps

[Future enhancements]
```

**Examples**:
- `OPTIONAL_MATCH_IMPLEMENTATION.md`
- `VIEWSCAN_IMPLEMENTATION.md`
- `VARIABLE_LENGTH_PATHS_IMPLEMENTATION.md`

---

### 3. **Session Summaries** (Development Records)
**Purpose**: Document work sessions for development continuity  
**Naming**: `SESSION_<DATE>.md` (e.g., `SESSION_2025-10-18.md`)  
**Location**: Root directory OR `docs/sessions/` if too many  

**Structure**:
```markdown
# Development Session - YYYY-MM-DD

**Duration**: ~X hours  
**Branch**: branch-name  
**Focus**: [Main objective]  
**Status**: ✅ Complete / 🚧 Ongoing

---

## Session Goals

[What we set out to accomplish]

## What We Accomplished

[Bullet points of completed items]

## Key Discoveries

[Important findings, gotchas, debugging insights]

## Commits Created

- `<hash>` - <commit message>
- `<hash>` - <commit message>

## Technical Details

[Important code changes, design decisions]

## Challenges & Solutions

[Problems encountered and how they were solved]

## Testing Results

[Test status, what was verified]

## Next Session

[What to tackle next time]
```

**Guidelines**:
- Create at end of productive sessions
- Include debugging stories (helpful for future)
- Document environment issues
- Link to feature docs if feature completed

---

### 4. **Status Reports** (Current State)
**Purpose**: Living document showing project's current status  
**Naming**: Always `STATUS_REPORT.md` (singular, root directory)  
**Update Frequency**: After major features/milestones  

**Structure**:
```markdown
# ClickGraph Status Report
*Updated: YYYY-MM-DD*

## 🎉 Latest Achievement: <Feature> (MMM DD, YYYY)

[Brief celebration of latest work]

## Feature Matrix

| Feature | Status | Tests | Notes |
|---------|--------|-------|-------|
| Basic Queries | ✅ Complete | 95/95 | Fully working |
| OPTIONAL MATCH | ✅ Complete | 11/11 | LEFT JOIN support |
| ViewScan | 🚧 Partial | 5/8 | Node queries only |

## Current Capabilities

### ✅ What Works
[Detailed list with examples]

### 🚧 In Progress
[What's being worked on]

### ❌ Known Limitations
[What doesn't work yet]

## Test Suite Status

**Overall**: X/Y tests passing (Z%)

## Architecture Overview

[High-level system diagram or description]

## Performance

[Benchmarks if available]

## Roadmap

[Upcoming priorities]
```

**Guidelines**:
- Keep this as THE source of truth for project state
- Update after merging features to main branch
- Include test statistics
- Be honest about limitations

---

### 5. **Design Documents** (Planning)
**Purpose**: Document design decisions before implementation  
**Naming**: `<FEATURE>_DESIGN.md` (e.g., `VIEWSCAN_DESIGN.md`)  
**Location**: Root directory or `docs/design/`  

**Structure**:
```markdown
# <Feature> Design Document

**Author**: [Name]  
**Date**: YYYY-MM-DD  
**Status**: 📝 Draft / 🔍 Review / ✅ Approved / 🏗️ Implemented

---

## Problem Statement

[What problem are we solving?]

## Goals

- Goal 1
- Goal 2

## Non-Goals

- What we're NOT doing
- Out of scope items

## Proposed Solution

### Architecture
[Design overview]

### Data Flow
[How data moves through system]

### API Design
[Public interfaces]

## Alternatives Considered

[Other approaches and why rejected]

## Implementation Plan

1. Step 1
2. Step 2

## Testing Strategy

[How to verify it works]

## Migration Path

[If this changes existing functionality]

## Open Questions

[Unresolved issues]
```

---

### 6. **Investigation Reports** (Research)
**Purpose**: Document research and investigation results  
**Naming**: `<TOPIC>_INVESTIGATION.md` (e.g., `YAML_SCHEMA_INVESTIGATION.md`)  
**Location**: Root directory  

**Structure**:
```markdown
# <Topic> Investigation

**Date**: YYYY-MM-DD  
**Investigator**: [Name if applicable]  
**Status**: 🔍 Investigating / ✅ Resolved / ❌ Blocked

---

## Question

[What we're trying to understand]

## Context

[Why this investigation is needed]

## Findings

### Discovery 1
[What we learned]

### Discovery 2
[What we learned]

## Evidence

[Code snippets, test results, logs]

## Conclusions

[What we determined]

## Recommendations

[What should be done based on findings]

## References

[Links to related docs, issues, PRs]
```

---

### 7. **Developer Checklists** (Process)
**Purpose**: Repeatable procedures and checklists  
**Naming**: `<PURPOSE>_CHECKLIST.md` (e.g., `DEV_ENVIRONMENT_CHECKLIST.md`)  
**Location**: Root directory  

**Structure**:
```markdown
# <Purpose> Checklist

**Purpose**: [What this checklist is for]  
**When to Use**: [Situations where you'd follow this]

---

## Prerequisites

- [ ] Prerequisite 1
- [ ] Prerequisite 2

## Steps

### Step 1: [Name]
```bash
# commands
```
**Expected**: [What should happen]

### Step 2: [Name]
```bash
# commands
```
**Expected**: [What should happen]

## Verification

- [ ] Check 1
- [ ] Check 2

## Troubleshooting

**Issue**: [Common problem]  
**Solution**: [How to fix]

**Issue**: [Common problem]  
**Solution**: [How to fix]
```

---

## 🎨 Formatting Standards

### Emoji Usage

**Status Indicators**:
- ✅ Complete/Success
- 🚧 In Progress/Partial
- ❌ Failed/Blocked/Not Working
- ⏸️ Paused
- 🔍 Investigating
- 📝 Draft
- ⏳ Pending

**Category Prefixes**:
- 🎉 Achievements/Celebrations
- 🚀 Features
- 🐛 Bug Fixes
- 📚 Documentation
- 🧪 Testing
- ⚙️ Infrastructure
- 💥 Breaking Changes
- 🔧 Technical Details
- 💡 Insights/Learnings
- 🎯 Goals/Objectives
- 📦 Deliverables
- 📊 Statistics/Metrics
- 🏆 Major Achievements
- 🔗 References/Links

### Code Block Standards

**Rust Code**:
````markdown
```rust
fn example() {
    // code
}
```
````

**Cypher Queries**:
````markdown
```cypher
MATCH (u:User) RETURN u.name
```
````

**SQL**:
````markdown
```sql
SELECT name FROM users
```
````

**Shell Commands** (Windows PowerShell):
````markdown
```powershell
cargo build
```
````

**Shell Commands** (Cross-platform):
````markdown
```bash
git status
```
````

### Date Format

**Standard**: `YYYY-MM-DD` (e.g., 2025-10-18)  
**Written**: `MMM DD, YYYY` (e.g., Oct 18, 2025)

### Test Statistics

**Format**: `X/Y tests passing (Z%)`  
**Example**: `261/262 tests passing (99.6%)`

### Commit References

**Format**: `` `<hash>` - <message> ``  
**Example**: `` `82401f7` - feat: Implement ViewScan ``

---

## 📂 Document Lifecycle

### When to Create

| Document Type | Trigger |
|--------------|---------|
| Feature Implementation | When feature is complete and merged |
| Session Summary | End of productive dev session (2+ hours) |
| Status Report Update | After major milestone or feature merge |
| Design Document | Before starting significant new feature |
| Investigation Report | After research/debugging that yielded insights |
| Checklist | When repeatable process identified |

### When to Update

| Document | Update Frequency |
|----------|-----------------|
| CHANGELOG.md | On release or feature merge to main |
| STATUS_REPORT.md | After feature completion |
| Feature Implementation | Never (historical record) |
| Session Summary | Never (historical record) |
| Design Document | During review process only |

### When to Archive

- Session summaries older than 6 months → `docs/archive/sessions/`
- Investigation reports after implementation → `docs/archive/investigations/`
- Design docs after implementation → Keep, mark as 🏗️ Implemented

---

## 🔄 Migration Plan for Existing Docs

### Recommended Actions

1. **Rename for Consistency**:
   ```
   SESSION_COMPLETE.md → SESSION_2025-10-18.md
   VIEWSCAN_IMPLEMENTATION_SUMMARY.md → VIEWSCAN_IMPLEMENTATION.md
   OPTIONAL_MATCH_COMPLETE.md → OPTIONAL_MATCH_IMPLEMENTATION.md
   ```

2. **Update CHANGELOG.md**:
   - Add ViewScan implementation to [Unreleased] section
   - Use consistent emoji prefixes
   - Add test statistics

3. **Update STATUS_REPORT.md**:
   - Add ViewScan to feature matrix
   - Update "Latest Achievement" section
   - Include current test count (261/262)

4. **Keep but Don't Replicate**:
   - Investigation reports (YAML_SCHEMA_INVESTIGATION.md) - good as is
   - Variable-length path docs - consolidate if too many

### Priority Order

1. **High Priority** - Public-facing docs:
   - CHANGELOG.md
   - STATUS_REPORT.md
   - README.md

2. **Medium Priority** - Developer reference:
   - Feature implementation docs
   - Design documents

3. **Low Priority** - Historical:
   - Session summaries (can batch rename later)
   - Investigation reports

---

## ✅ Checklist for New Documentation

Before creating a new document, verify:

- [ ] Correct document type chosen
- [ ] Naming convention followed
- [ ] Required sections included
- [ ] Emoji usage consistent
- [ ] Code blocks properly formatted
- [ ] Dates in standard format
- [ ] Test statistics included (if applicable)
- [ ] Status indicator clear
- [ ] Links to related docs included

---

## 📖 Examples

See these documents as good examples:
- ✅ `OPTIONAL_MATCH_IMPLEMENTATION.md` - Good feature doc structure
- ✅ `DEV_ENVIRONMENT_CHECKLIST.md` - Good checklist format
- ✅ `CHANGELOG.md` - Good changelog with emojis

---

**Questions or Suggestions?**  
This is a living document. Propose changes via PR or discussion.
