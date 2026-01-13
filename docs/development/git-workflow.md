# Git Workflow Best Practices

**Last Updated**: January 12, 2026

## \u26a0\ufe0f Critical Rule: Branch-Based Development with Pull Requests

**ALL development MUST follow this workflow**:
1. Create feature branch from main
2. Develop and test on feature branch
3. Submit pull request
4. Get review and approval
5. Merge to main
6. Delete feature branch

**Never commit directly to main branch!**

---

## Quick Reference

### Starting New Work
```bash
# Update main
git checkout main
git pull origin main

# Create branch (most work is bug fixes/improvements)
git checkout -b fix/your-bug-fix        # Most common
git checkout -b perf/your-optimization  # Common
git checkout -b refactor/cleanup        # Common
git checkout -b feature/new-feature     # Rare (late-stage dev)

# Start developing...
```

### During Development
```bash
# Commit frequently
git add <files>
git commit -m "feat: add specific functionality"

# Push to remote regularly
git push origin feature/your-feature-name
```

### Finishing Work
```bash
# Run all tests
cargo test
pytest tests/integration/

# Push final changes
git push origin feature/your-feature-name

# Create PR on GitHub
# Get review, address feedback
# Merge via GitHub UI
```

---

## The Problem We Had
Lost working code during experimentation because we didn't commit incrementally or use git stash.

## Better Workflow Going Forward

### 1. Commit After Each Successful Fix

```bash
# After fixing each bug, commit immediately
git add src/query_planner/analyzer/plan_sanitization.rs
git commit -m "fix: preserve aliases in plan sanitization"

# Run tests
python test_viewscan.py

# If tests pass, commit is safe
# If tests fail, can easily revert: git reset --soft HEAD~1
```

**Benefits**:
- Each fix is isolated and reversible
- Clear history of what changed when
- Easy to bisect if something breaks later

### 2. Use Git Stash for Experiments

**Before trying risky changes**:
```bash
# Save current working state
git stash push -m "Working state before CTE optimization attempt"

# Try the experiment
# ... make changes ...
# ... test ...

# If it fails:
git stash pop  # Restore working code

# If it succeeds:
git stash drop  # Discard the old state
git commit -am "feat: optimize CTE generation for simple patterns"
```

**Example from our session**:
```bash
# Should have done this before attempting CTE removal:
git stash push -m "Working ViewScan with all alias fixes"

# Then try CTE removal...
# When it failed:
git stash pop  # Would have restored everything instantly!
```

### 3. Use Feature Branches for All Development (REQUIRED)

**\u26a0\ufe0f This is now the REQUIRED workflow for all changes!**

```bash
# ALWAYS start from updated main
git checkout main
git pull origin main

# Create feature branch
git checkout -b feature/your-feature-name

# Develop, test, commit
git add <files>
git commit -m "feat: implement core functionality"

# Push to remote
git push origin feature/your-feature-name

# Create PR on GitHub for review
# After approval and merge:
git checkout main
git pull origin main
git branch -d feature/your-feature-name
```

**Branch Naming Convention** (by frequency in late-stage development):
- Bug fixes: `fix/<issue>` (e.g., `fix/null-handling`) - MOST COMMON
- Performance: `perf/<optimization>` (e.g., `perf/join-optimization`) - COMMON
- Refactoring: `refactor/<component>` (e.g., `refactor/query-planner`) - COMMON
- Documentation: `docs/<topic>` (e.g., `docs/api-reference`) - REGULAR
- Tests: `test/<area>` (e.g., `test/edge-cases`) - REGULAR
- Features: `feature/<name>` (e.g., `feature/aggregate-functions`) - RARE

**Why This Matters**:
- \u2705 Keeps main branch stable and deployable
- \u2705 Enables code review before merge
- \u2705 Allows experimentation without risk
- \u2705 Creates clear history of changes
- \u2705 Prevents lost work from failed experiments

### 4. Frequent Status Checks

```bash
# Before making changes
git status
git diff  # See what you're about to lose

# During changes
git diff src/query_planner/analyzer/graph_traversal_planning.rs

# Before committing
git diff --staged
```

## Recommended Workflow for This Project

### Before Starting Any Work

```bash
# Ensure main is up to date
git checkout main
git pull origin main

# Create feature branch
git checkout -b <type>/<description>

# Verify you're on the right branch
git branch --show-current
```

### During Development (Feature Branch Workflow)

```bash
# Make changes and test frequently
# ... edit code ...
cargo test

# Commit after each logical unit
git add <specific-file>
git commit -m "feat: add specific functionality"

# Continue development
# ... more edits ...
git add <files>
git commit -m "feat: implement additional logic"

# Push to remote regularly (backup + collaboration)
git push origin <branch-name>

# Before risky changes within branch:
git stash push -m "Working state before <experiment>"

# Run comprehensive tests before PR
cargo test
pytest tests/integration/
```

### Submitting Pull Request

```bash
# Final push
git push origin <branch-name>

# Create PR on GitHub:
# 1. Go to repository
# 2. Click "Pull Requests" -> "New Pull Request"
# 3. Select your branch
# 4. Fill out PR template with:
#    - What changed
#    - Why it changed
#    - Test results
#    - Documentation updates

# Wait for review
# Address feedback if needed:
git add <revised-files>
git commit -m "fix: address review feedback"
git push origin <branch-name>

# After PR approval and merge:
git checkout main
git pull origin main
git branch -d <branch-name>
```

## What to Commit When

### Always Commit
- Working code with passing tests
- Documentation updates
- Configuration changes

### Never Commit
- `server.pid` (add to .gitignore)
- `target/` directory (should be in .gitignore)
- Temporary debug files
- Failed experiments

### Commit Separately
- Code changes (one commit)
- Documentation (separate commit)
- Test infrastructure (separate commit)

## Commit Message Format

Follow [Conventional Commits](https://www.conventionalcommits.org/) specification:

```
<type>(<scope>): <short description>

<optional detailed description>

<optional breaking changes>
```

**Types**:
- `feat:` New features
- `fix:` Bug fixes
- `docs:` Documentation only
- `test:` Test infrastructure or new tests
- `refactor:` Code restructuring (no functionality change)
- `perf:` Performance improvements
- `chore:` Maintenance tasks (dependency updates, etc.)
- `ci:` CI/CD configuration changes

**Examples**:
```bash
# Simple fix
git commit -m "fix: preserve Cypher variable aliases in plan sanitization"

# Feature with scope
git commit -m "feat(query-planner): add support for aggregate functions"

# With detailed description
git commit -m "feat: add schema lookup for relationship types

Relationships now use GLOBAL_GRAPH_SCHEMA.get_rel_schema() to resolve
Cypher relationship types to ClickHouse table names. Falls back to
hardcoded mappings for backwards compatibility.

Files changed:
- render_plan/plan_builder.rs: rel_type_to_table_name()
- Tests pass: 261/262"

# Documentation update
git commit -m "docs: add ViewScan architecture guide"

# Breaking change
git commit -m "feat!: change schema configuration format

BREAKING CHANGE: Schema YAML format updated to support
polymorphic types. Old format no longer supported."
```

## Pull Request Guidelines

### PR Title Format
```
<type>: Brief description (one line)

Examples:
feat: Add aggregate function support (COUNT, SUM, AVG)
fix: Resolve OPTIONAL MATCH NULL handling in joins
docs: Complete Cypher Language Reference for VLP
perf: Optimize CTE generation for simple patterns
```

### PR Description Template
```markdown
## Summary
[Brief description of what this PR does]

## Changes Made
- Component 1: [specific change]
- Component 2: [specific change]
- ...

## Testing
- Unit tests: XX passing
- Integration tests: XX passing
- Manual testing: [scenarios tested]

## Documentation
- [ ] Cypher Language Reference updated
- [ ] STATUS.md updated
- [ ] CHANGELOG.md updated
- [ ] Feature note added (if major feature)

## Examples
```cypher
# Show example of new functionality
MATCH (n:User) RETURN count(n)
```

## Related Issues
Closes #XX
Related to #YY

## Breaking Changes
- [ ] Yes - [describe changes and migration path]
- [X] No

## Checklist
- [ ] All tests passing
- [ ] No compilation warnings
- [ ] Documentation complete
- [ ] Code follows style guidelines
- [ ] Ready for review
```

### Code Review Guidelines

**For Reviewers**:
- Focus on correctness, maintainability, performance
- Check test coverage and edge cases
- Verify documentation is complete
- Be constructive and respectful
- Approve when ready, request changes clearly

**For Authors**:
- Respond to all comments
- Make requested changes promptly
- Explain design decisions when needed
- Re-request review after changes
- Be open to feedback

## Recovery Commands

### Undo Last Commit (Keep Changes)
```bash
git reset --soft HEAD~1
```

### Undo Last Commit (Discard Changes)
```bash
git reset --hard HEAD~1
```

### Restore File from Last Commit
```bash
git checkout HEAD -- path/to/file.rs
```

### See What Would Be Lost
```bash
git diff  # Unstaged changes
git diff --staged  # Staged changes
```

### Recover from Stash
```bash
git stash list
git stash show stash@{0}
git stash pop stash@{0}
```

## Checklist Before Risky Changes

- [ ] `git status` - Know what's uncommitted
- [ ] `git diff` - Review changes
- [ ] **On feature branch** - Never on main!
- [ ] **`git stash push -m "..."`** or **`git commit`** - Save working state
- [ ] Tests pass - `cargo test && pytest tests/integration/`
- [ ] Now safe to experiment!

## Branch Protection and Main Branch Rules

### Main Branch Protection (ENFORCED)

**The main branch is sacred**:
- \u274c NEVER commit directly to main
- \u274c NEVER push directly to main
- \u274c NEVER force push to main
- \u2705 ALWAYS use pull requests
- \u2705 ALWAYS get code review
- \u2705 ALWAYS ensure tests pass

**Why?**:
- Keeps main branch stable and deployable
- Ensures all code is reviewed
- Maintains clear history
- Prevents accidental breakage
- Enables safe experimentation

### Exception: Emergency Hotfixes Only

```bash
# ONLY for critical production issues
git checkout main
git pull origin main
git checkout -b hotfix/critical-bug

# Fix, test thoroughly
git commit -m "hotfix: resolve critical bug"
git push origin hotfix/critical-bug

# Create PR with "HOTFIX" label
# Fast-track review, then merge
```

**Hotfix criteria**:
- Production is broken
- Security vulnerability
- Data corruption risk
- NOT for: regular bugs, features, optimizations

## After This Session

### Immediate Next Steps (Updated Workflow)

1. **Ensure you're on a feature branch**:
```bash
git branch --show-current

# If on main, create feature branch NOW:
git checkout -b feature/current-work
```

2. **Review and commit current changes on feature branch**:
```bash
git status
git diff  # Review all changes

# Commit in logical groups:
git add src/query_planner/analyzer/plan_sanitization.rs
git commit -m "fix: preserve Cypher variable aliases in plan sanitization"

git add src/query_planner/analyzer/graph_traversal_planning.rs
git commit -m "fix: qualify columns in IN subqueries with table aliases"

# Continue for other logical groups...
```

3. **Push feature branch and create PR**:
```bash
# Push to remote
git push origin feature/current-work

# Create PR on GitHub:
# - Fill out PR template
# - Request review
# - Address feedback
# - Merge after approval
```

4. **After merge, update local main**:
```bash
git checkout main
git pull origin main
git branch -d feature/current-work
```

### If You Have Uncommitted Experimental Changes

```bash
# On feature branch
git status

# Option 1: Commit if successful
git add <files>
git commit -m "feat: successful experiment"

# Option 2: Stash if incomplete
git stash push -m "WIP: experiment in progress"

# Option 3: Discard if failed
git checkout -- <files>
```

---

## Workflow Summary

```
                    Feature Development Lifecycle
                    
┌─────────────────────────────────────────────────────────────────┐
│  1. START: Create feature branch from main                      │
│     git checkout main && git pull && git checkout -b feature/X  │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│  2. DEVELOP: Work on feature branch                             │
│     - Edit code                                                 │
│     - Test frequently (cargo test, pytest)                      │
│     - Commit incrementally (git commit -m "...")                │
│     - Push regularly (git push origin feature/X)                │
│     - Use git stash for experiments                             │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│  3. FINALIZE: Prepare for PR                                    │
│     - Run full test suite                                       │
│     - Update documentation                                      │
│     - Self-review code                                          │
│     - Final push                                                │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│  4. PR: Submit for review                                       │
│     - Create PR on GitHub                                       │
│     - Fill out template                                         │
│     - Request reviewers                                         │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│  5. REVIEW: Address feedback                                    │
│     - Respond to comments                                       │
│     - Make requested changes                                    │
│     - Push additional commits                                   │
│     - Re-request review                                         │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│  6. MERGE: Integrate to main                                    │
│     - Ensure CI passes                                          │
│     - Get approval                                              │
│     - Merge (Squash and Merge recommended)                      │
│     - Delete feature branch                                     │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│  7. CLEANUP: Update local environment                           │
│     git checkout main && git pull                               │
│     git branch -d feature/X                                     │
└─────────────────────────────────────────────────────────────────┘

Main branch is ALWAYS stable and deployable!
```

---

## Key Principles (TL;DR)

1. **\u26a0\ufe0f NEVER commit directly to main** - Always use feature branches
2. **\u2705 ALWAYS create PR** - All code must be reviewed
3. **\ud83d\udd04 Commit frequently** - After each logical unit of work
4. **\ud83d\udcbe Stash liberally** - Before risky experiments
5. **\ud83e\uddea Test thoroughly** - Before submitting PR
6. **\ud83d\udcdd Document completely** - Update all relevant docs
7. **\ud83d\udc40 Review carefully** - Both your code and others'
8. **\ud83e\uddf9 Keep main clean** - Stable and deployable always

**Remember**: Branch early, commit often, PR always. Code is cheap, lost time is expensive, broken main is catastrophic.

---

**Last Updated**: January 12, 2026  
**See Also**:
- `DEVELOPMENT_PROCESS.md` - Complete 6-phase development process (with Phase 0: Branch and Phase 6: PR)
- `.github/copilot-instructions.md` - Project conventions and Git workflow section
- `docs/development/FEATURE_DOCUMENTATION_CHECKLIST.md` - Pre-PR documentation checklist



