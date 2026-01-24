# Phase 6: Quick Reference Card

**Session Status**: ✅ Complete - Ready for handoff  
**Date**: January 23, 2026  
**Next Developer**: Use this card to continue Phase 6

---

## The Problem in 30 Seconds

```cypher
# This query fails:
MATCH (u:User) WITH u AS person RETURN person.name

# Error: Identifier 'person.full_name' cannot be resolved
# Why: CTE has columns u_name, u_city (source alias prefix)
#      but SELECT tries person.full_name (output alias)
```

## What's Been Done

✅ Root cause identified  
✅ Test infrastructure fixed (schema specification)  
✅ Remapping functions added to plan_builder.rs  
✅ Properties builder enhanced for WITH clauses  
✅ Code compiles, server runs, basic tests work  
✅ Documentation complete with continuation guide  

## What's Broken

❌ Remapping not working (functions present, logic needs debugging)  
❌ 7 variable renaming tests failing  
❌ ~30 complex expression tests affected  

## To Fix (Next Developer Steps)

### Step 1: Understand the Bug (10 min)
```bash
# Read this file first:
cat PHASE_6_CONTINUATION.md

# Run one failing test:
pytest tests/integration/test_variable_alias_renaming.py::TestVariableAliasRenaming::test_simple_node_renaming -xvs
```

### Step 2: Debug the Remapping (1 hour)
```bash
# Add debug logging to remap_select_item_aliases()
# Build with debug enabled
cargo build --release

# Run test with logging
RUST_LOG=debug python3 -m pytest tests/integration/test_variable_alias_renaming.py::TestVariableAliasRenaming::test_simple_node_renaming -xvs 2>&1 | grep col_alias

# Understand the actual col_alias format
# (dot vs underscore vs something else)
```

### Step 3: Fix Remapping Logic (30 min)
Adjust `remap_select_item_aliases()` in `src/render_plan/plan_builder.rs` to:
- Correctly identify col_alias format
- Rename all instances of source alias prefix to output alias
- Handle all edge cases

### Step 4: Validate (1 hour)
```bash
# Test all 7 variable renaming tests
pytest tests/integration/test_variable_alias_renaming.py -v

# Expected: 6-7/7 passing
# Then: Run full suite for comprehensive check
pytest tests/integration/ --tb=no -q
```

## Key Files

| File | Purpose | Status |
|------|---------|--------|
| src/render_plan/plan_builder.rs | Where remapping happens | 🚨 Needs debug |
| src/render_plan/properties_builder.rs | Property lookup for WITH | ✅ Done |
| tests/integration/test_variable_alias_renaming.py | Test cases | ✅ Schema fixed |
| PHASE_6_CONTINUATION.md | Detailed guide | ✅ Read this! |

## Quick Commands

```bash
# Build and start server
cargo build --release
nohup ./target/release/clickgraph &

# Run one test
pytest tests/integration/test_variable_alias_renaming.py::TestVariableAliasRenaming::test_simple_node_renaming -xvs

# Run all renaming tests
pytest tests/integration/test_variable_alias_renaming.py -v

# Check health
curl http://localhost:8080/health

# View generated SQL (add to test)
# Use sql_only=true parameter in request

# Git status
git log --oneline -5
git status
```

## Success Criteria

- ✅ test_simple_node_renaming passes
- ✅ All 7 variable renaming tests pass (87%+)
- ✅ Complex expression tests reach 85%+
- ✅ Full suite reaches 95%+ (3,320+ tests)
- ✅ Zero regressions in Phases 1-5

## Timeline

- Debugging: 1 hour
- Fixing: 1 hour  
- Complex expressions: 2-3 hours
- Validation: 1 hour
- **Total: 5-6 hours**

## Got Stuck?

1. Check PHASE_6_CONTINUATION.md - it has detailed debugging steps
2. Add more debug logging to understand data flow
3. Check the error message carefully - it often hints at the issue
4. Look at working OPTIONAL MATCH code - similar pattern
5. Ask for context from previous developer

## Commits So Far

```
49f4511 - docs: Phase 6 session completion summary
f30a3ea - docs: Phase 6 continuation guide + STATUS updates
45bed02 - fix(phase6): Variable renaming + properties_builder
```

**Good luck! You've got this.** The hardest part (root cause analysis) is done. Now it's systematic debugging. 💪
