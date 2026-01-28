# CTE Naming Audit - January 2026

## Executive Summary

We have **centralized CTE naming utilities** (`src/utils/cte_naming.rs`) but they are **NOT consistently used across the codebase**. This creates maintenance burden, inconsistency risks, and potential bug vectors.

**Critical Issues Found: 4**

---

## Issue #1: Duplicate CTE Naming Logic (HIGH PRIORITY)

### Location
`src/query_planner/analyzer/cte_schema_resolver.rs` lines 38-51

### Problem
```rust
// ❌ DUPLICATE IMPLEMENTATION
fn generate_cte_name(with_clause: &WithClause, plan_ctx: &mut PlanCtx) -> String {
    let mut sorted_aliases = with_clause.exported_aliases.clone();
    sorted_aliases.sort();
    
    let cte_counter = plan_ctx.cte_counter;
    plan_ctx.cte_counter += 1;
    
    if sorted_aliases.is_empty() {
        format!("with_cte_{}", cte_counter)
    } else {
        format!("with_{}_cte_{}", sorted_aliases.join("_"), cte_counter)
    }
}
```

This duplicates the logic in `src/utils/cte_naming.rs::generate_cte_name()`, but:
- **Incorrect parameter type**: Takes `&WithClause` instead of `&[impl AsRef<str>]`
- **Redundant counter management**: Handles counter separately instead of using the utility function
- **Inconsistent interface**: Uses `plan_ctx.cte_counter` instead of passing counter parameter
- **Line 60**: Actually calls the correct utility function, making the duplicate function **dead code**

### Impact
- Code duplication and maintenance burden
- Two different interfaces for the same concept
- Easy to introduce inconsistencies when one is updated

### Current Usage
- Line 38-51: **Dead code** - Never called directly
- Line 60: Uses correct utility `generate_cte_name(&with_clause.exported_aliases, cte_counter)`

---

## Issue #2: Missing Counter Parameter (MEDIUM PRIORITY)

### Location
`src/render_plan/plan_builder.rs` line 1370

### Problem
```rust
// ❌ NO COUNTER - Only generates base name pattern
let cte_name = format!("with_{}_cte", with.exported_aliases.join("_"));
```

This generates a CTE name **without** the sequence counter, which is **inconsistent** with:
- Centralized utility function: `generate_cte_name()` always includes counter
- Rest of codebase: All other locations use `with_*_cte_{counter}` format

### Impact
- Name collision risk if multiple CTEs have same aliases in sequence
- Pattern mismatch with CTE naming convention documented in `cte_naming.rs`
- Line extraction utilities expect format `with_*_cte_{counter}` (see `extract_aliases_from_cte_name()`)

### Current Usage
```rust
// Line 1370 in plan_builder.rs
let cte_name = format!("with_{}_cte", with.exported_aliases.join("_"));
let cte = Cte::new(cte_name.clone(), cte_content, false);
```

Should be:
```rust
let cte_name = generate_cte_name(&with.exported_aliases, counter);
```

---

## Issue #3: Indirect CTE Naming (MEDIUM PRIORITY)

### Location
Multiple sites use **pattern matching on CTE names** instead of calling utility functions:

1. **Pattern Detection** (checking if something is a CTE):
   - `src/clickhouse_query_generator/to_sql_query.rs` line 221:
     ```rust
     if from_ref.name.starts_with("with_") && from_ref.name.contains("_cte_") { ... }
     ```
   - `src/render_plan/plan_builder_utils.rs` line 4210: (same pattern)
   - `src/render_plan/join_builder.rs` lines 282, 1466-1467:
     ```rust
     // Comments reference CTE pattern but no utility function
     ```

2. **CTE Name Extraction** (parsing a CTE name):
   - Manual parsing in `src/render_plan/from_builder.rs` lines 862-867
   - Comments reference format but use ad-hoc parsing logic

### Impact
- **Fragile pattern matching**: If naming convention changes, multiple sites break
- **No single source of truth for parsing**: `extract_aliases_from_cte_name()` exists but isn't used
- **Inconsistent handling**: Different sites handle edge cases differently
- **Future maintenance risk**: Adding counter variations or format changes requires finding all pattern matches

### Examples Found
```
src/clickhouse_query_generator/to_sql_query.rs:156 - Comment about CTE format
src/clickhouse_query_generator/to_sql_query.rs:221 - Pattern check
src/render_plan/from_builder.rs:862 - Manual CTE parsing
src/render_plan/from_builder.rs:867 - Format comment
src/render_plan/join_builder.rs:282 - Comment reference
src/render_plan/join_builder.rs:1466 - Manual extraction logic
src/render_plan/plan_builder_utils.rs:2152 - Pattern description
src/render_plan/plan_builder_utils.rs:4210 - Pattern check
```

---

## Issue #4: Inconsistent Counter Management (LOW PRIORITY)

### Location
Various places that call `generate_cte_name()` pass different counter sources:

1. **Via plan_ctx counter**:
   - `src/query_planner/analyzer/variable_resolver.rs` line 198: 
     ```rust
     let name = generate_cte_name(exported_aliases, *counter);
     ```

2. **Hardcoded values**:
   - `src/render_plan/cte_extraction.rs` line 3039:
     ```rust
     let name = generate_cte_name(&wc.exported_aliases, 1);
     ```
   - `src/render_plan/plan_builder_utils.rs` lines 7192-7216: Complex counter logic

3. **Manual counter loop**:
   - `src/render_plan/plan_builder_utils.rs` lines 7213-7216: `next_seq` calculation

### Impact
- **Risk of counter reuse**: Different CTEs might get same counter value
- **Hardcoded `1` might cause collisions**: In some paths, multiple CTEs could get `_cte_1` suffix
- **Counter source varies**: Sometimes from `plan_ctx`, sometimes computed locally

---

## Existing Good Practices

✅ **Centralized utility functions exist** (`src/utils/cte_naming.rs`):
- `generate_cte_name()` - Generate with counter
- `generate_cte_base_name()` - Generate base name
- `extract_aliases_from_cte_name()` - Parse CTE name

✅ **Well-documented** with examples and tests

✅ **Good usage examples**:
- `src/query_planner/analyzer/variable_resolver.rs` - Uses utility correctly
- `src/render_plan/plan_builder_utils.rs` - Mostly uses utility

---

## Proposed Fixes (Priority Order)

### Fix #1: Remove Dead Code in cte_schema_resolver.rs
**Effort: Low | Impact: High**

Remove duplicate `generate_cte_name()` function (lines 38-51) entirely. Already replaced by line 60's call to the utility function.

**Files affected: 1**
- `src/query_planner/analyzer/cte_schema_resolver.rs`

### Fix #2: Fix Missing Counter in plan_builder.rs
**Effort: Low | Impact: Medium**

Replace hardcoded format on line 1370 with utility function call. Need to determine correct counter source.

**Files affected: 1**
- `src/render_plan/plan_builder.rs`

### Fix #3: Centralize CTE Pattern Checking
**Effort: Medium | Impact: Medium**

Add utility functions to `src/utils/cte_naming.rs`:
```rust
pub fn is_cte_name(name: &str) -> bool { ... }
pub fn cte_name_to_alias(name: &str) -> Option<String> { ... }
```

Then replace all pattern matching with these utilities.

**Files affected: 3-4**
- `src/clickhouse_query_generator/to_sql_query.rs`
- `src/render_plan/plan_builder_utils.rs`
- `src/render_plan/from_builder.rs`
- (possibly join_builder.rs)

### Fix #4: Audit Counter Management
**Effort: High | Impact: High (longer-term)**

Ensure counter is always managed consistently through `plan_ctx.cte_counter`. Verify no hardcoded `1` values cause collisions in multi-CTE scenarios.

**Files affected: 2-3**
- `src/render_plan/cte_extraction.rs`
- `src/render_plan/plan_builder_utils.rs`

---

## Testing Recommendations

1. **Unit tests for utility functions** (already exist - solid):
   - `src/utils/cte_naming.rs` has good coverage

2. **Integration tests for consistency**:
   - Create test case with multiple nested WITH clauses
   - Verify no counter collisions
   - Verify all CTE references match created names

3. **Pattern matching tests**:
   - If `is_cte_name()` utility is added, verify it correctly identifies all actual CTE names
   - Edge cases: Names with numbers, underscores, etc.

---

## Cleanup Checklist

- [ ] Remove dead code: `CteSchemaResolver::generate_cte_name()`
- [ ] Fix hardcoded counter: `plan_builder.rs` line 1370
- [ ] Add `is_cte_name()` utility
- [ ] Add `cte_name_to_alias()` utility
- [ ] Replace pattern checks in 3-4 files with utility functions
- [ ] Verify counter management in multi-CTE scenarios
- [ ] Update documentation comments if needed
- [ ] Run full test suite (especially integration tests)

---

## Files Summary

### Utilities (Single Source of Truth)
- `src/utils/cte_naming.rs` ✅ Well-designed, good tests

### Duplicate/Inconsistent Usage
1. `src/query_planner/analyzer/cte_schema_resolver.rs` - Dead code (lines 38-51)
2. `src/render_plan/plan_builder.rs` - Missing counter (line 1370)
3. `src/clickhouse_query_generator/to_sql_query.rs` - Pattern matching (line 221)
4. `src/render_plan/plan_builder_utils.rs` - Pattern matching (line 4210)
5. `src/render_plan/from_builder.rs` - Manual parsing (lines 862-867)
6. `src/render_plan/cte_extraction.rs` - Hardcoded counter (line 3039)

---

## Risk Assessment

| Issue | Severity | Risk | Likelihood |
|-------|----------|------|------------|
| Dead code duplication | Low | Maintenance burden | High |
| Missing counter | Medium | Name collision | Medium |
| Pattern matching spread | High | Format change breaks multiple sites | High |
| Counter management | Medium | Collision in edge cases | Low |

**Overall**: Code is **functional** but **fragile**. Changes to naming convention would require updates in 6+ locations.
