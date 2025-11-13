# Git Workflow Best Practices

## The Problem We Had
Lost working code during experimentation because we didn't commit incrementally or use git stash.

## Better Workflow Going Forward

### 1. Commit After Each Successful Fix

```bash
# After fixing each bug, commit immediately
git add brahmand/src/query_planner/analyzer/plan_sanitization.rs
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

### 3. Use Feature Branches for Risky Changes

```bash
# Create branch for experiment
git checkout -b experiment/cte-optimization

# Try changes
# ... make changes ...
# ... test ...

# If it fails:
git checkout graphview1  # Back to main branch
git branch -D experiment/cte-optimization  # Delete failed experiment

# If it succeeds:
git checkout graphview1
git merge experiment/cte-optimization
git branch -d experiment/cte-optimization
```

### 4. Frequent Status Checks

```bash
# Before making changes
git status
git diff  # See what you're about to lose

# During changes
git diff brahmand/src/query_planner/analyzer/graph_traversal_planning.rs

# Before committing
git diff --staged
```

## Recommended Workflow for This Project

### Daily Development

```bash
# Morning: Start fresh
git status
git diff  # Review any uncommitted changes

# After each successful fix:
git add <specific-file>
git commit -m "fix: specific issue"
python test_viewscan.py  # Verify

# Before risky changes:
git stash push -m "Working state before <experiment>"

# End of day:
git status
git log --oneline -5  # Review what you did
```

### For Multi-Step Features

```bash
# Start feature
git checkout -b feature/shortest-path

# Step 1: Add AST support
git add ...
git commit -m "feat(shortest-path): add AST parsing"

# Step 2: Add query planning
git add ...
git commit -m "feat(shortest-path): add query planning"

# Step 3: Add SQL generation
git add ...
git commit -m "feat(shortest-path): add SQL generation"

# Step 4: Tests pass
git checkout graphview1
git merge feature/shortest-path
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

```
<type>: <short description>

<optional detailed description>

<optional breaking changes>
```

**Types**:
- `fix:` Bug fixes
- `feat:` New features
- `docs:` Documentation only
- `test:` Test infrastructure
- `refactor:` Code restructuring
- `perf:` Performance improvements

**Examples**:
```bash
git commit -m "fix: preserve Cypher variable aliases in plan sanitization"

git commit -m "feat: add schema lookup for relationship types

Relationships now use GLOBAL_GRAPH_SCHEMA.get_rel_schema() to resolve
Cypher relationship types to ClickHouse table names. Falls back to
hardcoded mappings for backwards compatibility.

Files changed:
- render_plan/plan_builder.rs: rel_type_to_table_name()
- Tests pass: 261/262"

git commit -m "docs: add ViewScan architecture guide"
```

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
- [ ] **`git stash push -m "..."`** or **`git commit`** - Save working state
- [ ] `python test_viewscan.py` - Verify tests pass
- [ ] Now safe to experiment!

## After This Session

### Immediate Next Steps

1. **Review and commit current changes**:
```bash
git status
git diff  # Review all changes

# Commit in logical groups:
git add brahmand/src/query_planner/analyzer/plan_sanitization.rs
git commit -m "fix: preserve Cypher variable aliases in plan sanitization"

git add brahmand/src/query_planner/analyzer/graph_traversal_planning.rs
git commit -m "fix: qualify columns in IN subqueries with table aliases

- Add table_alias parameter to build_insubquery()
- Qualify columns in get_subplan()
- Use schema columns instead of output aliases in WHERE
- Fix all 7 call sites with correct aliases"

git add brahmand/src/clickhouse_query_generator/to_sql_query.rs
git commit -m "fix: prevent CTE nesting and add SELECT * default"

git add brahmand/src/server/handlers.rs
git commit -m "feat: add debug logging for full SQL queries"

git add brahmand/src/render_plan/plan_builder.rs
git commit -m "feat: add schema lookup for relationship types"

git add brahmand/src/query_planner/logical_plan/match_clause.rs
git commit -m "fix: pass labels to generate_scan for ViewScan resolution"

# Documentation commits
git add docs/ notes/ *.md
git commit -m "docs: add ViewScan completion and testing guides"

# Test infrastructure
git add test_*.py test_server.ps1 docker-compose.test.yaml Dockerfile.test
git commit -m "test: add comprehensive testing infrastructure

- PowerShell test runner with PID tracking
- Python test suite with 5 test cases
- Docker Compose test environment
- Testing guide documentation"
```

2. **Tag the working state**:
```bash
git tag -a viewscan-complete -m "ViewScan implementation complete (nodes + relationships)"
```

3. **Push to remote**:
```bash
git push origin graphview1
git push --tags
```

---

**Key Principle**: Commit early, commit often, use stash liberally. Code is cheap, lost time is expensive.
