# Phase 6: Variable Renaming & Expression Edge Cases - Status Report

## Date: January 23, 2026

## Summary
Work began on fixing complex expression edge cases and variable renaming in WITH clauses. Root cause analysis completed for the primary issue.

## Issues Investigated

###  1. Variable Renaming in WITH Clauses (Primary Focus)
**Test Files**: `tests/integration/test_variable_alias_renaming.py` (7 tests)

**Problem Identified**:
- Query: `MATCH (u:User) WITH u AS person RETURN person.name LIMIT 1`
- Expected: Property access on renamed alias should work
- Actual Error: ClickHouse "Identifier 'person.full_name' cannot be resolved"

**Root Cause Found**:
```
CTE Definition:    CREATE...AS (SELECT u.city AS u_city, ..., u.full_name AS u_name, ... FROM users AS u)
Outer SELECT:      SELECT person.full_name FROM cte AS person  ← MISMATCH: u_name vs full_name
```

The CTE column names use source alias prefix (`u_`) but the outer SELECT uses output alias (`person.`).

**Fix Attempted**:
- Added `remap_select_item_aliases()` function in `src/render_plan/plan_builder.rs`
- Also added `WithClause` handling to `src/render_plan/properties_builder.rs` for property resolution
- Fixed test file to specify `schema_name: "social_benchmark"` in requests

**Current Status**:  
- Root cause analysis: ✅ COMPLETE
- Code changes: ⚠️ IN PROGRESS (remapping logic needs refinement)
- Testing: ⚠️ PENDING (server restart issues due to extensive debugging logging)

**Next Steps for Full Resolution**:
1. Debug the exact format of col_alias values in SelectItems
2. Ensure remapping function correctly identifies source alias prefixes
3. Test with all 7 variable renaming test cases
4. Extend fix to complex WITH queries (subsequent MATCH after WITH)

### 2. Complex Expression Cases
**Test Files**: 
- `test_property_expressions.py` (~30 tests)
- `test_case_expressions.py` (~25 tests)  
- `test_mixed_expressions.py`

**Status**: NOT YET INVESTIGATED
- Scheduled for Phase 6 continuation
- Likely similar scope to variable renaming issues

## Code Changes Made

### 1. src/render_plan/plan_builder.rs
- Added `build_with_alias_mapping()` helper function
- Added `remap_select_item_aliases()` helper function  
- Integrated remapping into `LogicalPlan::WithClause` render path
- Added debug logging for alias tracking

### 2. src/render_plan/properties_builder.rs
- Added `WithClause` case handling for property resolution
- Enables downstream aliases to resolve properties from renamed variables
- Imports `LogicalExpr` for alias pattern matching

### 3. tests/integration/test_variable_alias_renaming.py
- Fixed `query_clickgraph()` to specify `schema_name: "social_benchmark"`
- Ensures tests use correct schema (was attempting to use non-existent "default" schema)

## Architecture Insights

### WITH Clause Processing Flow:
1. **Logical Plan**: `WithClause` node created with `items` (ProjectionItems) and `exported_aliases`
2. **Schema Inference**: Aliases registered in `plan_ctx` with labels preserved
3. **Render Phase**: CTE generated from input plan, then outer SELECT generated from next clause
4. **Property Resolution**: When accessing `person.name` in outer SELECT, system must:
   - Recognize `person` is CTE alias
   - Find `u` (source alias) in mapping
   - Resolve `name` property to CTE column `u_name`
   - Generate: `SELECT ... person.u_name AS ...`

### Current Implementation Gap:
The CTE column name remapping (attempt to change `u_name` → `person_name`) would solve the column naming mismatch, but relies on correctly identifying SelectItem col_alias formats which may vary depending on whether it's:
- Simple node expansion: likely `u.name` format
- With aggregation: likely computed expression format
- Mixed expressions: various formats

## Test Metrics

**Not yet completed due to implementation in progress**

Target for completion:
- Variable renaming tests: 7/7 passing
- Expression tests: TBD (estimated 60+ tests)
- Overall improvement: 80.8% → 90%+ pass rate

## Recommendations

1. **Complete the remapping fix** by:
   - Adding trace logging to identify exact col_alias formats
   - Testing against individual test cases
   - Potentially handling multiple formats in remapping function

2. **Alternative simpler approach**:
   - Instead of remapping CTE column names, leave them as `u_name`
   - Modify property resolution in outer SELECT to understand aliases come from CTEs
   - Map `person.name` → `person.u_name` at SELECT rendering time

3. **Validation**:
   - Run full test suite after variable renaming is fixed
   - Identify patterns in expression-related failures
   - Design comprehensive fix for expression edge cases

## Timeline for Completion

Given the complexity discovered:
- **Variable Renaming Fix**: 1-2 more hours of debugging + refinement
- **Expression Cases**: 3-5 hours (depends on patterns found)
- **Comprehensive Testing & Validation**: 2-3 hours

**Total Estimated**: 6-10 hours to complete Phase 6
