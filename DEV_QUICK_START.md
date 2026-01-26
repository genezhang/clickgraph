# Dev Quick Start - Contributing to ClickGraph

**⚡ Essential workflow for developers: bug fixes, improvements, and features.**

**Read this first. Refer to DEVELOPMENT_PROCESS.md only when you need details.**

---

## 🔥 The 30-Second Workflow

```bash
# 1. CREATE BRANCH (always!)
git checkout main && git pull
git checkout -b fix/your-bug-name

# 2. MAKE CHANGES
# ... edit code ...

# 3. TEST
cargo test
pytest tests/integration/

# 4. PRE-PUSH CHECKS (⚠️ DO NOT SKIP!)
cargo fmt --all
cargo clippy --all-targets
cargo test

# 5. COMMIT & PUSH
git add -A
git commit -m "fix: your change description"
git push origin fix/your-bug-name

# 6. CREATE PULL REQUEST
# → Go to GitHub and create PR
```

---

## ⚠️ CRITICAL RULES

### 1. **Never commit to main directly**
```bash
# ✅ CORRECT
git checkout -b fix/bug-name

# ❌ WRONG
git checkout main  # don't work here!
```

### 2. **Always run pre-push checks**
```bash
# These 3 commands are MANDATORY before pushing:
cargo fmt --all           # CI will fail without this
cargo clippy --all-targets  # Check warnings
cargo test                # Verify nothing broke
```

### 3. **Always update documentation**
After fixing/implementing, update these files:
- `docs/wiki/cypher-language-reference.md` ← **MOST IMPORTANT**
- `STATUS.md` - Move feature to "What Works"
- `CHANGELOG.md` - Add entry under `[Unreleased]`

---

## 📋 Branch Naming (Use These)

```bash
# Bug fixes (most common)
fix/optional-match-null-handling
fix/cte-property-resolution

# Performance improvements
perf/join-elimination
perf/cte-optimization

# Code refactoring
refactor/simplify-query-planner
refactor/unify-context

# Tests
test/edge-case-coverage
test/vlp-integration

# Documentation
docs/update-cypher-reference
docs/bolt-protocol-guide

# New features (rare - we're late stage!)
feature/list-comprehensions
```

---

## 🧪 Testing Quick Reference

### Unit Tests
```bash
# Run all unit tests
cargo test

# Run specific test
cargo test test_optional_match

# Run with output
cargo test -- --nocapture
```

### Integration Tests
```bash
# All integration tests
pytest tests/integration/

# Specific test file
pytest tests/integration/matrix/test_comprehensive.py

# Verbose output
pytest tests/integration/ -v
```

### Test Both Before Pushing!
```bash
cargo test && pytest tests/integration/ && echo "✅ ALL TESTS PASSED"
```

---

## 🐛 Debugging Tips

### Check Generated SQL
```bash
# Use sql_only mode for quick debugging
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH (n) RETURN n","sql_only":true}'
```

### Check Server Logs
```bash
# Start server with debug logging
RUST_LOG=debug cargo run --bin clickgraph
```

### Common Issues
- **Test fails**: Check if schema is loaded (`export GRAPH_CONFIG_PATH=...`)
- **SQL looks wrong**: Check logical plan with `RUST_LOG=debug`
- **Clippy warnings**: Run `cargo clippy --all-targets` to see all issues

---

## 📚 When to Read Full Docs

**Read DEVELOPMENT_PROCESS.md when**:
- Implementing a major new Cypher feature
- Need to understand architecture patterns
- Learning from past implementation examples
- Stuck and need detailed troubleshooting

**Read STATUS.md when**:
- Need to know what features work
- Checking test statistics
- Understanding current limitations

**Read CHANGELOG.md when**:
- Need release notes
- Looking for version history

---

## 💡 Common Commands

```bash
# Format code (MANDATORY before push)
cargo fmt --all

# Check warnings
cargo clippy --all-targets

# Run tests
cargo test                    # Unit tests
pytest tests/integration/      # Integration tests

# Build
cargo build                   # Debug build
cargo build --release         # Release build

# Run server
cargo run --bin clickgraph

# Check schema
export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"
```

---

## 🚀 Your First Bug Fix (Step-by-Step)

```bash
# 1. Start from main
git checkout main && git pull origin main

# 2. Create branch
git checkout -b fix/your-bug-description

# 3. Make your changes
# Edit the relevant files...

# 4. Test manually
cargo run --bin clickgraph  # Start server
# Test your query via curl or neo4j client

# 5. Run automated tests
cargo test
pytest tests/integration/

# 6. Pre-push checks
cargo fmt --all
cargo clippy --all-targets
cargo test

# 7. Update docs
# - Update Cypher Language Reference
# - Update STATUS.md
# - Update CHANGELOG.md

# 8. Commit and push
git add -A
git commit -m "fix: describe what you fixed"
git push origin fix/your-bug-description

# 9. Create PR on GitHub
# Fill in PR template with:
# - What changed
# - Why it was needed
# - Test results
# - Breaking changes (if any)

# 10. Address review feedback
# Make changes, commit, push to same branch
git add -A
git commit -m "fix: address review feedback"
git push origin fix/your-bug-description
```

---

## 🎯 Key Principle

> **Quality over speed. Get it right the first time.**
> - Always test before pushing
> - Always format before pushing
> - Always update docs
> - Never skip the checklist

---

**Need more detail?** See [DEVELOPMENT_PROCESS.md](DEVELOPMENT_PROCESS.md)
