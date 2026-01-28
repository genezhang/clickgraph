# Holistic Fix Methodology - Lessons Learned

**Date**: December 26, 2025  
**Context**: VLP WHERE clause alias bug investigation

## The Problem Pattern

We keep applying "case-by-case" fixes that don't solve the root cause:

1. **Fallback Mapping Fix** (Bad): Added t1-t99 → vlp* mapping
   - Symptoms treated: Some tests pass
   - Root cause ignored: `relationship_filters` never used
   - New bugs created: Incorrect SQL for relationship property filters

2. **Schema Name Fix** (Partial): Fixed `denormalized_flights` vs `denormalized_flights_test`
   - Symptoms treated: Schema not found errors
   - Root cause ignored: Inconsistent schema naming across configs

## Why Our Fixes Are Partial

### Anti-Pattern 1: Fixing Where You See the Error

```
Error appears in: plan_builder.rs (SQL has wrong alias)
Fix applied to:   plan_builder.rs (add fallback mapping)
Root cause in:    filter_pipeline.rs (relationship_filters unused)
                  variable_length_cte.rs (no relationship filter field)
```

**Lesson**: The error location ≠ the bug location. Trace the data flow backward.

### Anti-Pattern 2: Fixing One Case Without Checking Related Cases

We fixed `t2 → vlp2` mapping but didn't ask:
- What about `f` (named relationship alias)?
- What about relationship property filters like `f.flight_number = 123`?
- What about multiple relationship types `[:TYPE1|TYPE2*]`?

**Lesson**: Every filter type in `CategorizedFilters` must have a consumer.

### Anti-Pattern 3: Not Understanding the Full Data Flow

The VLP filter data flow:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           VLP FILTER DATA FLOW                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Cypher: WHERE a.code = 'JFK' AND f.flight_number = 123                     │
│                    │                       │                                │
│                    ▼                       ▼                                │
│  ┌──────────────────────────────────────────────────────────────┐           │
│  │           filter_pipeline.rs: categorize_filters()            │           │
│  │                                                               │           │
│  │   start_node_filters:  a.code = 'JFK'        ← ✅ USED        │           │
│  │   end_node_filters:    (none)                ← ✅ USED        │           │
│  │   relationship_filters: f.flight_number=123  ← ❌ NEVER USED  │           │
│  │   path_function_filters: (none)              ← ✅ USED        │           │
│  └──────────────────────────────────────────────────────────────┘           │
│                    │                                                        │
│                    ▼                                                        │
│  ┌──────────────────────────────────────────────────────────────┐           │
│  │    cte_extraction.rs: Only passes start/end filters          │           │
│  │                                                               │           │
│  │    combined_start_filters → VariableLengthCteGenerator        │           │
│  │    combined_end_filters   → VariableLengthCteGenerator        │           │
│  │    relationship_filters   → DROPPED! Never passed!            │           │
│  └──────────────────────────────────────────────────────────────┘           │
│                    │                                                        │
│                    ▼                                                        │
│  ┌──────────────────────────────────────────────────────────────┐           │
│  │    variable_length_cte.rs: VariableLengthCteGenerator         │           │
│  │                                                               │           │
│  │    Fields:                                                    │           │
│  │      start_node_filters: Option<String>  ← Has field          │           │
│  │      end_node_filters: Option<String>    ← Has field          │           │
│  │      relationship_filters: ???           ← NO FIELD!          │           │
│  └──────────────────────────────────────────────────────────────┘           │
│                    │                                                        │
│                    ▼                                                        │
│  ┌──────────────────────────────────────────────────────────────┐           │
│  │    plan_builder.rs: Tries to fix with fallback mapping        │           │
│  │                                                               │           │
│  │    f → vlp2 (WRONG! Can't apply edge filter to CTE output)    │           │
│  │    Result: WHERE vlp2.flight_number = 123 (column doesn't     │           │
│  │            exist in vlp2!)                                    │           │
│  └──────────────────────────────────────────────────────────────┘           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## The Holistic Fix Checklist

Before implementing ANY fix, answer these questions:

### 1. Trace the Full Data Flow
- [ ] Where does the data originate? (parser, AST)
- [ ] What transformations does it go through? (logical plan, optimizer, render plan)
- [ ] Where does it get consumed? (SQL generator)
- [ ] Is there a break in the chain? (data produced but not consumed)

### 2. Check All Related Cases
- [ ] What similar cases exist? (other filter types, other schemas)
- [ ] Are there tests for all cases? (if not, add them first!)
- [ ] What code paths share this logic? (grep for related functions)

### 3. Identify the True Root Cause
- [ ] Is this a missing feature or a bug?
- [ ] Is the architecture incomplete or just buggy implementation?
- [ ] Does fixing this require adding new fields/parameters?

### 4. Design the Complete Solution
- [ ] List ALL files that need changes
- [ ] Design the data flow for the complete solution
- [ ] Identify what tests need to pass after the fix

### 5. Validate Before Implementing
- [ ] Can you explain the fix without using "fallback" or "workaround"?
- [ ] Does the fix follow the existing architecture patterns?
- [ ] Will this fix ALL related test failures, not just one?

---

## The Complete Fix for VLP Relationship Filters

### Files Requiring Changes

1. **`variable_length_cte.rs`** - Add `relationship_filters: Option<String>` field
2. **`cte_extraction.rs`** - Extract and pass `categorized.relationship_filters`
3. **`variable_length_cte.rs`** - Apply relationship filter in CTE base case
4. **`plan_builder.rs`** - REMOVE the fallback mapping (lines 565-583)

### Proper Solution Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        CORRECT VLP FILTER HANDLING                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Cypher: WHERE a.code = 'JFK' AND f.flight_number = 123                     │
│                    │                       │                                │
│                    ▼                       ▼                                │
│  ┌──────────────────────────────────────────────────────────────┐           │
│  │           filter_pipeline.rs: categorize_filters()            │           │
│  │                                                               │           │
│  │   start_node_filters:    a.code = 'JFK'        → Pass to CTE │           │
│  │   relationship_filters:  f.flight_number=123   → Pass to CTE │           │
│  └──────────────────────────────────────────────────────────────┘           │
│                    │                       │                                │
│                    ▼                       ▼                                │
│  ┌──────────────────────────────────────────────────────────────┐           │
│  │    cte_extraction.rs: Pass ALL filter categories             │           │
│  │                                                               │           │
│  │    combined_start_filters    → VariableLengthCteGenerator     │           │
│  │    combined_end_filters      → VariableLengthCteGenerator     │           │
│  │    relationship_filters_sql  → VariableLengthCteGenerator     │  ← NEW   │
│  └──────────────────────────────────────────────────────────────┘           │
│                    │                                                        │
│                    ▼                                                        │
│  ┌──────────────────────────────────────────────────────────────┐           │
│  │    variable_length_cte.rs: Generate CTE with edge filter      │           │
│  │                                                               │           │
│  │    WITH RECURSIVE vlp_cte AS (                                │           │
│  │      -- Base case                                             │           │
│  │      SELECT ... FROM start_node                               │           │
│  │      JOIN edge_table ON ... AND edge.flight_number = 123 ←    │           │
│  │      WHERE start_node.code = 'JFK'                            │           │
│  │                                                               │           │
│  │      UNION ALL                                                │           │
│  │                                                               │           │
│  │      -- Recursive case                                        │           │
│  │      SELECT ... FROM vlp_cte                                  │           │
│  │      JOIN edge_table ON ... AND edge.flight_number = 123 ←    │           │
│  │    )                                                          │           │
│  └──────────────────────────────────────────────────────────────┘           │
│                                                                             │
│  Result: Relationship filter applied INSIDE CTE, not outside!               │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Why Fallback Mapping is Fundamentally Wrong

The fallback mapping `f → vlp2` assumes:
- "Just rename the alias and it will work"

But VLP CTEs don't expose edge properties in their output:
```sql
-- VLP CTE output columns:
--   start_id, end_id, hop_count, path_edges, path_nodes
--
-- Notice: NO flight_number column!

WHERE vlp2.flight_number = 123  -- ❌ Column doesn't exist!
```

The relationship filter MUST be applied during traversal (inside CTE), not on CTE output.

---

## General Principles for Holistic Fixes

### 1. **Never Add Fallback Logic**
If you find yourself writing "fallback" or "workaround", you're treating symptoms.

### 2. **Follow Data Flow End-to-End**
Map out where data is produced, transformed, and consumed. The bug is in the gap.

### 3. **Check All Enum/Struct Fields Are Used**
If `CategorizedFilters` has 4 fields, all 4 must have consumers.

### 4. **Test the Negative Case**
Don't just test "does it work?" Test "does it fail correctly for invalid input?"

### 5. **Run Full Test Suite Before Declaring Victory**
A fix that breaks other tests is not a fix.

### 6. **Document the Architecture, Not the Workaround**
If the fix is hard to explain, the architecture may need improvement first.

---

## ✅ IMPLEMENTED: Holistic Fix for VLP Relationship Filters

**Date Completed**: December 26, 2025

### Changes Made

#### 1. `filter_pipeline.rs` - Fixed Filter Categorization
```rust
// BEFORE: _rel_alias was ignored
pub fn categorize_filters(..., _rel_alias: &str) -> CategorizedFilters

// AFTER: rel_alias is now used to categorize relationship filters
pub fn categorize_filters(..., rel_alias: &str) -> CategorizedFilters {
    // Now checks: references_alias(&predicate, rel_alias, "rel")
    // Properly routes r.property filters to relationship_filters
}
```

#### 2. `variable_length_cte.rs` - Added Fields to Struct
```rust
pub struct VariableLengthCteGenerator<'a> {
    // ...existing fields...
    pub relationship_cypher_alias: String,  // NEW: e.g., "r" or "f"
    pub relationship_filters: Option<String>, // NEW: e.g., "rel.weight > 0.5"
}
```

#### 3. `variable_length_cte.rs` - Updated ALL 5 Constructors
- `new()` ✅
- `new_with_polymorphic()` ✅
- `new_with_fk_edge()` ✅
- `new_denormalized()` ✅
- `new_mixed()` ✅

#### 4. `cte_extraction.rs` - Pass Relationship Filters
```rust
// BEFORE: rel_alias was empty string
let categorized = categorize_filters(..., ""); // rel_alias not used

// AFTER: Pass actual relationship alias and capture relationship filters
let categorized = categorize_filters(..., &rel_alias);
let rel_sql = categorized.relationship_filters.as_ref()
    .map(|expr| render_expr_to_sql_string(expr, &rel_alias_mapping));

// Then pass to generator:
VariableLengthCteGenerator::new_with_fk_edge(
    ...
    &rel_alias,      // NEW: Pass relationship alias
    ...
    rel_filters_sql, // NEW: Pass relationship filters
    ...
)
```

#### 5. `variable_length_cte.rs` - Apply Filters in SQL Generation

Updated ALL generator methods to include relationship_filters in WHERE clause:
- `generate_base_case()` ✅
- `generate_recursive_case_with_cte_name()` ✅
- `generate_fk_edge_base_case()` ✅
- `generate_fk_edge_recursive_case()` (append) ✅
- `generate_fk_edge_recursive_prepend()` ✅
- `generate_denormalized_base_case()` ✅
- `generate_denormalized_recursive_case()` ✅
- `generate_mixed_base_case()` ✅
- `generate_mixed_recursive_case()` ✅
- `generate_heterogeneous_polymorphic_recursive_case()` ✅

### Remaining Work

1. **Remove Fallback Mapping** - The fallback in `plan_builder.rs` lines 553-590 should now be unnecessary and can be removed after verification
2. **Add Tests** - Add specific tests for relationship property filters in VLP queries
3. **Verify** - Run full test suite to confirm fix doesn't break existing functionality

---

## Applying This to Current VLP Bug

**Current status**: Fallback mapping hides the bug but creates incorrect SQL

### Full Scope of Proper Fix

The proper fix requires changes to **10+ locations** across **4 files**:

#### File 1: `src/clickhouse_query_generator/variable_length_cte.rs`

| Location | Change Required |
|----------|-----------------|
| Line 35 | Add `pub relationship_filters: Option<String>` field |
| Line 78 `new()` | Add parameter and pass to `new_with_polymorphic` |
| Line 126 `new_with_polymorphic()` | Add parameter and assign field |
| Line 181 `new_with_fk_edge()` | Add parameter and assign field |
| Line 254 `new_denormalized()` | Add parameter and assign field |
| Line 326 `new_mixed()` | Add parameter and assign field |
| Line 1420 `generate_base_case()` | Apply relationship filter to JOIN/WHERE |
| Line 1550 `generate_recursive_case()` | Apply relationship filter to recursive JOINs |
| (Similar for denormalized/mixed variants) | Each base/recursive case |

#### File 2: `src/render_plan/cte_extraction.rs`

| Location | Change Required |
|----------|-----------------|
| Line 1063 | Pass `graph_rel.alias` instead of `""` to `categorize_filters()` |
| Line 1071-1078 | Extract `relationship_filters` to SQL string |
| Lines 1341, 1361, 1385 | Pass relationship filters to generator constructors |

#### File 3: `src/render_plan/filter_pipeline.rs`

| Location | Change Required |
|----------|-----------------|
| Line 21 | Actually use the `rel_alias` parameter (currently ignored) |
| Line 45-60 | Add proper check for relationship alias in `references_alias` |

#### File 4: `src/render_plan/plan_builder.rs`

| Location | Change Required |
|----------|-----------------|
| Lines 565-583 | REMOVE fallback mapping (t1-t99, f/r/e/rel) |

### Why This Is Not a Quick Fix

The fallback was added as a 20-line change. The proper fix requires:
- ~50 lines of struct/function signature changes
- ~30 lines of SQL generation logic
- Testing across ALL VLP variants (standard, denormalized, mixed, FK-edge, polymorphic)
- Potential cascading changes if relationship filters interact with other features

### Recommended Approach

1. **First**: Document the full scope (DONE - this document)
2. **Second**: Mark fallback as `// TODO: REMOVE - proper fix in progress`
3. **Third**: Implement changes in order: struct → constructors → SQL generation → call sites
4. **Fourth**: Run tests after EACH major change to catch regressions early
5. **Fifth**: Remove fallback only after all tests pass

### Test Coverage for Proper Fix

Must pass these test patterns:
```cypher
-- Node property filter (existing - should still work)
MATCH (a)-[:REL*1..2]->(b) WHERE a.prop = 'x' RETURN b

-- Relationship property filter (currently broken)  
MATCH (a)-[r:REL*1..2]->(b) WHERE r.prop = 'x' RETURN b

-- Combined filters (currently broken)
MATCH (a)-[r:REL*1..2]->(b) WHERE a.prop = 'x' AND r.prop = 'y' RETURN b

-- Anonymous relationship with filter (currently broken via t2 alias)
MATCH (a)-[:REL*1..2 {prop: 'x'}]->(b) RETURN b
```

**Expected outcome**: ~50 VLP+WHERE failures should be resolved WITH correct SQL.
