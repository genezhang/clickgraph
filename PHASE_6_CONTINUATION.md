# Phase 6: Complex Expression Edge Cases - Continuation Guide

**Session Completed**: Comprehensive root cause analysis + partial implementation  
**Last Updated**: Current session  
**Status**: 🚨 In Progress - Remapping needs debugging

---

## Executive Summary

This phase aims to fix variable renaming in WITH clauses and complex expression edge cases to improve test pass rate from **80.8%** to **95%+** (target: 3,320+ tests passing).

### Current Blockers
1. **CTE Column Naming Mismatch** - Renamed variables can't access properties because CTE uses original alias prefix
   - Example: `MATCH (u:User) WITH u AS person` generates CTE columns `u_name`, but SELECT tries `person.full_name`
   - Solution partially implemented: Remapping functions added but not yet effective

2. **Three test infrastructure issues fixed**:
   - ✅ Schema specification in tests (was causing "schema not found" errors)
   - ✅ Added properties_builder support for WITH clauses
   - ✅ Test helper updated for social_benchmark schema

### Estimated Completion Time
- **Variable renaming tests**: 1-2 hours (debug + fix + validate 7 tests)
- **Complex expression fixes**: 2-3 hours (expression handling in various contexts)
- **Full suite validation**: 1 hour (run tests, analyze metrics)
- **Total Phase 6**: 4-6 hours

---

## Problem Statement

### Original Symptom
```
Error: Identifier 'person.full_name' cannot be resolved from subquery
CTE Output: SELECT u.city AS u_city, ..., u.full_name AS u_name
Issue: SELECT tries person.full_name but column is person.u_name
```

### Root Cause Analysis
The CTE generation uses the **source alias** (from MATCH) as column prefix:
- Source: `MATCH (u:User)` → CTE columns: `u_city`, `u_name`, etc.
- After: `WITH u AS person` → User tries: `person.city`, `person.name`
- **Mismatch**: `person` prefix not recognized; only `u_` prefix exists

### Where It Happens
1. **Query Planning**: LogicalPlan correctly tracks aliases in plan_ctx
2. **Render Phase**: RenderPlan's SelectBuilder generates CTE with wrong prefix (the bug)
3. **SQL Generation**: SQL tries to use renamed alias but columns don't exist

---

## Implementation Progress

### ✅ Completed
1. **Root Cause Identified** - CTE column prefix not remapped with alias
2. **Test Schema Fixed** - Tests now use social_benchmark schema (schema_name parameter)
3. **Helper Functions Added**:
   - `build_with_alias_mapping()` - Extracts mapping from WITH items
   - `remap_select_item_aliases()` - Attempts column prefix remapping
4. **Properties Resolution Enhanced** - Added WITH clause case in properties_builder
5. **Code Compiles** - Clean build with no errors
6. **Server Functional** - Health checks passing, basic queries work

### 🚨 In Progress / Blocked
1. **CTE Remapping Not Effective** - Functions present but not producing expected SQL
   - Expected: `person_name` or `person.u_name` in SELECT
   - Actual: Still `u_name` in both CTE and SELECT
   - Likely cause: col_alias format detection in remap_select_item_aliases()

### ❌ Not Started
1. **Complex Expression Cases** (~30+ tests)
   - CASE expressions
   - Math/string operations in properties
   - Function calls in property access
2. **Full Test Suite Validation** - Comprehensive metrics collection

---

## Key Files & Code Locations

### src/render_plan/plan_builder.rs
**Line ~880-920**: `build_with_alias_mapping()`
```rust
// Extracts mapping: {"u" -> "person"} from WITH items
// Returns HashMap<String, String>
```

**Line ~920-970**: `remap_select_item_aliases()`
```rust
// Attempts to rename columns in SelectItems
// Handles both "u.name" (dot) and "u_name" (underscore) formats
// ⚠️ This function may have format detection issues
```

**Line ~1095-1104**: Integration with LogicalPlan::WithClause
```rust
let alias_mapping = build_with_alias_mapping(&with.items, &with.exported_aliases);
if !alias_mapping.is_empty() {
    cte_select_items = remap_select_item_aliases(cte_select_items, &alias_mapping);
}
```

### src/render_plan/properties_builder.rs
**Line ~327+**: New WithClause case
```rust
LogicalPlan::WithClause(wc) => {
    // Maps renamed alias back to source for property lookup
    for item in &wc.items {
        if item.alias.as_ref() == Some(&ta.0) {
            return wc.input.get_properties_with_table_alias(&item.expr)?;
        }
    }
}
```

### tests/integration/test_variable_alias_renaming.py
**Line ~15**: `query_clickgraph()` function
```python
# Now includes: "schema_name": "social_benchmark"
```

---

## How to Continue

### Step 1: Debug Column Alias Format (1 hour)
**Goal**: Understand actual format of col_alias values in SelectItems

Add debug output to remap_select_item_aliases():
```rust
// Before attempting remapping, log actual values
debug!("SelectItem in CTE: {:?}", select_item);
debug!("col_alias format: {:?}", select_item.col_alias);
debug!("Source alias prefixes in mapping: {:?}", alias_mapping);
```

Run test with logging:
```bash
RUST_LOG=debug cargo build --release 2>&1 | grep -i "col_alias"
cargo run --bin clickgraph --release 2>&1 &
RUST_LOG=debug python3 -m pytest tests/integration/test_variable_alias_renaming.py::TestVariableAliasRenaming::test_simple_node_renaming -x -s 2>&1 | grep -i "selectitem\|col_alias" 
```

### Step 2: Refine Remapping Logic (1 hour)
Based on debug output, adjust remap_select_item_aliases() to correctly identify and rename column aliases.

**Key Cases to Handle**:
- If col_alias is "u.name": Rename to "person.name" (dot format)
- If col_alias is "u_name": Rename to "person_name" (underscore format)
- If col_alias is None: Use column name directly
- If source alias "u" not in mapping: Skip (no rename needed)

### Step 3: Test Variable Renaming (30 min)
```bash
# Run all 7 variable renaming tests
python3 -m pytest tests/integration/test_variable_alias_renaming.py -v

# Expected: 6-7/7 passing (87%+ pass rate)
# If <5/7: Adjust remapping logic
# If ≥6/7: Move to Step 4
```

### Step 4: Address Complex Expression Cases (2-3 hours)
Locate and fix similar issues for:
- CASE expressions with property access
- Math operations (e.g., `n.age * 2`)
- String functions (e.g., `upper(n.name)`)
- Function calls in WITH/RETURN

**Strategy**: Use same CTE column remapping approach for non-node variables.

### Step 5: Full Test Suite Validation (1 hour)
```bash
# Comprehensive test run with metrics
python3 -m pytest tests/integration/ --tb=no -q 2>&1 | tail -20

# Compare metrics:
# - Before: 80.8% pass rate (2,660+/3,290 tests)
# - Target: 95%+ pass rate (3,120+/3,290 tests)
# - Success: <100 failing tests remaining
```

---

## Test Files to Watch

### Variable Renaming Tests (7 tests, all failing)
```bash
tests/integration/test_variable_alias_renaming.py::TestVariableAliasRenaming
├── test_simple_node_renaming                  # FAILING
├── test_relationship_renaming                 # FAILING
├── test_multiple_renames                      # FAILING
├── test_renamed_in_where                      # FAILING
├── test_renamed_in_return                     # FAILING
├── test_with_match_rename                     # FAILING
└── test_complex_with_chain                    # FAILING
```

### Complex Expression Tests (partial tracking)
```bash
tests/integration/test_property_expressions.py     # ~15 tests
tests/integration/test_case_expressions.py         # ~12 tests
tests/integration/test_function_expressions.py     # ~8 tests
```

---

## Quick Reference: Test Commands

```bash
# Test one failing case
pytest tests/integration/test_variable_alias_renaming.py::TestVariableAliasRenaming::test_simple_node_renaming -xvs

# Test all renaming cases
pytest tests/integration/test_variable_alias_renaming.py -v

# Quick expression test check
pytest tests/integration/test_property_expressions.py --tb=no -q

# Full suite check (takes ~5 min)
pytest tests/integration/ --tb=no -q

# Get metrics summary
pytest tests/integration/ --tb=no -q 2>&1 | tail -3
```

---

## Git Workflow

**Current Branch**: main (local changes)

**Before pushing**:
```bash
# Ensure all tests pass
pytest tests/integration/ --tb=no -q

# Review changes
git diff --stat

# Commit with clear message
git commit -m "fix(phase6): Complete variable renaming in WITH clauses"

# Push to feature branch for review
git push origin fix/variable-renaming-with
```

---

## Documentation Checklist (When Complete)

- [ ] Update STATUS.md with Phase 6 completion
- [ ] Create feature note: `notes/variable-renaming-with-clauses.md`
- [ ] Update CHANGELOG.md with fixes
- [ ] Add test count summary: X tests fixed, pass rate 80.8% → XX%
- [ ] Document any edge cases or limitations discovered

---

## Success Criteria

### Phase 6 Complete When:
- ✅ All 7 variable renaming tests pass (100%)
- ✅ Complex expression tests: >85% passing
- ✅ Full integration test suite: >95% passing (3,120+/3,290 tests)
- ✅ Zero regressions in Phases 1-5
- ✅ Documentation updated
- ✅ Code reviewed and merged to main

### Current Progress
- Variable renaming: 0/7 passing → Target: 7/7
- Complex expressions: ~50% → Target: 85%+
- Full suite: 80.8% → Target: 95%+

---

## Common Issues & Solutions

| Issue | Cause | Solution |
|-------|-------|----------|
| Remapping not applied | col_alias format differs from expected | Add debug logging to see actual format |
| Some tests still fail | Edge case in alias pattern | Extend remap logic to handle new format |
| SQL generation wrong | Remapping incomplete | Check if all SelectItems are being remapped |
| Properties not found | WithClause case in properties_builder ineffective | Verify item.alias matches lookup alias |

---

## Notes for Next Session

This work started with Phase 6 investigation into variable renaming issues. The root cause is well understood: CTE columns use source alias prefix (u_) while SELECT uses output alias (person.name). Two helper functions were created but need debugging for correct col_alias format handling.

**Recommended starting point**: Add debug logging to understand actual SelectItem col_alias values, then refine remapping logic accordingly. Once remapping works for simple cases, extend to complex expressions and run full validation suite.

**Time estimate**: Complete in 4-6 hours with focused debugging and testing.
