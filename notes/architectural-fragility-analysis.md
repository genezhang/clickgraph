# Architectural Fragility Analysis

**Date**: December 11, 2025  
**Author**: Analysis of ClickGraph codebase vulnerabilities

## Executive Summary

ClickGraph has **systemic architectural issues** causing fragility:

1. **Code Duplication**: Same logic implemented 3+ times with inconsistent behavior
2. **Missing Abstractions**: No centralized CTE management or GROUP BY expansion
3. **Implicit Dependencies**: CTEs generated deep in call stack, hoisted manually
4. **Inconsistent Patterns**: Different code paths handle similar cases differently

---

## Problem 1: Fragile CTE Generation

### The Issue

VLP (Variable-Length Path) CTEs were being generated but not appearing in final SQL because:

1. **CTEs generated deep in call stack** (`extract_ctes_with_context` at line 3728)
2. **Manual hoisting required** at multiple levels
3. **Two separate code paths** for WITH clauses:
   - `build_with_match_cte_plan` (line 272) - extracts CTEs correctly
   - `build_chained_with_match_cte_plan` (line 413) - **was missing CTE extraction**
4. **No central CTE registry** - CTEs tracked in `RenderPlan.ctes` field

### Why It Failed

`build_chained_with_match_cte_plan` calls `render_without_with_detection` which calls `to_render_plan`, which generates the VLP CTE and adds it to `rendered.ctes.0`. But then:

```rust
// Line 617: Renders the WITH clause plan
match render_without_with_detection(plan_to_render, schema) {
    Ok(mut rendered) => {
        // ... processes rendered plan
        
        // Line 733: Creates with_cte_render
        let with_cte_render = if rendered_plans.len() == 1 {
            rendered_plans.pop().unwrap()  // ← CTEs are in here!
        }
        
        // Line 768: Creates CTE
        let with_cte = Cte {
            cte_name: cte_name.clone(),
            content: CteContent::Structured(with_cte_render),  // ← Nested!
            is_recursive: false,
        };
        all_ctes.push(with_cte);
        // ← Missing: Extract with_cte_render.ctes.0 and hoist them!
    }
}
```

**The nested CTEs were being wrapped inside the WITH CTE instead of hoisted to top level.**

### The Fix (Applied)

```rust
// Line 771-777: Extract and hoist nested CTEs
let nested_ctes = std::mem::take(&mut with_cte_render.ctes.0);
if !nested_ctes.is_empty() {
    log::info!("🔧 Hoisting {} nested CTEs", nested_ctes.len());
    all_ctes.extend(nested_ctes);
}
```

### Why This Is Fragile

1. **Implicit dependency**: CTEs must be manually hoisted at each level
2. **Easy to forget**: No compiler enforcement, just silently generates bad SQL
3. **Duplicate logic**: Same hoisting code in 2 places (lines 377 and 771)
4. **No validation**: No check that all CTEs are properly hoisted

---

## Problem 2: GROUP BY with TableAlias Not Expanded

### The Issue

```cypher
MATCH (p:Person {id: 933})-[:KNOWS*1..3]-(friend:Person) 
WHERE friend.firstName = 'John' AND friend.id <> 933 
WITH friend, count(*) AS cnt 
RETURN friend.id
```

Generates:
```sql
SELECT friend, count(*) AS cnt 
FROM (...) AS __union 
GROUP BY friend  -- ← ERROR: "friend" is a table alias, not a column!
```

**ClickHouse Error**: `Unknown expression identifier 'friend'`

### Where It Fails

**Location**: `build_chained_with_match_cte_plan` lines 661-667

```rust
if has_aggregation {
    let group_by_exprs: Vec<RenderExpr> = items.iter()
        .filter(|item| !matches!(&item.expression, 
            crate::query_planner::logical_expr::LogicalExpr::AggregateFnCall(_)))
        .filter_map(|item| {
            let expr_result: Result<RenderExpr, _> = item.expression.clone().try_into();
            expr_result.ok()
        })
        .collect();
    rendered.group_by = GroupByExpressions(group_by_exprs);  // ← Direct conversion!
}
```

**Problem**: Converts `LogicalExpr::TableAlias("friend")` directly to `RenderExpr::TableAlias("friend")` without expanding to actual columns.

### Where It Works Correctly

**Location**: `extract_group_by` lines 5878-6050

```rust
if let crate::query_planner::logical_expr::LogicalExpr::TableAlias(alias) = expr {
    // OPTIMIZATION: For node aliases in GROUP BY, we only need the ID column.
    if let Ok((properties, actual_table_alias)) = 
        group_by.input.get_properties_with_table_alias(&alias.0) {
        
        let id_col = properties.iter()
            .find(|(prop_name, _)| prop_name == "id" || prop_name.ends_with("_id"))
            .map(|(_, col_name)| col_name.clone())
            .unwrap_or_else(|| "id".to_string());
        
        result.push(RenderExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias(actual_table_alias.clone()),
            column: Column(PropertyValue::Column(id_col)),
        }));
    }
}
```

**This logic exists but is NOT called by `build_chained_with_match_cte_plan`!**

### Have We Seen This Before?

**YES!** Similar issue was fixed in:
- `extract_group_by` (optimized to use ID-only GROUP BY)
- Multiple other places where TableAlias expansion was needed

**Root cause**: Same transformation logic duplicated in multiple places, with some implementations missing the expansion step.

---

## Problem 3: Code Duplication Across WITH Handlers

### Three Separate WITH Clause Handlers

| Function | Lines | Purpose | GROUP BY Handling |
|----------|-------|---------|-------------------|
| `build_with_match_cte_plan` | 272-410 | Single WITH clause | ✅ Uses `extract_group_by` |
| `build_chained_with_match_cte_plan` | 413-823 | Chained WITH clauses | ❌ Inline conversion |
| `build_with_aggregation_match_cte_plan` | 981-1161 | WITH + aggregation | ✅ Uses `extract_group_by` |

### Shared Logic That Should Be Centralized

1. **CTE name generation**: Now fixed (using `generate_cte_id()`)
2. **Nested CTE hoisting**: Duplicated 2x (lines 377, 771)
3. **GROUP BY expansion**: Implemented 1x, missing 1x
4. **WITH items projection**: Duplicated 3x with variations
5. **Filter scope splitting**: Implemented 1x

### Code Metrics

```
Total WITH handling code: ~1,400 lines
Estimated duplication: ~40% (560 lines)
Functions that should be extracted: 8-10
```

---

## Root Causes

### 1. Lack of Abstraction Layers

**Missing abstractions**:
- `CteManager` - centralized CTE generation and hoisting
- `GroupByExpander` - consistent TableAlias → column expansion
- `WithClauseRenderer` - unified WITH clause rendering

**Current approach**: Inline logic repeated in each code path

### 2. Implicit State Management

**CTEs are implicitly threaded**:
```rust
// CTEs generated here
let cte = generate_vlp_cte(...);

// Must be manually added to render plan
render_plan.ctes.0.push(cte);

// Must be manually hoisted if nested
let nested = std::mem::take(&mut render_plan.ctes.0);
parent_ctes.extend(nested);
```

**No validation** that CTEs are properly propagated.

### 3. Copy-Paste Programming

**Pattern**:
1. Implement feature in `build_with_match_cte_plan`
2. Copy code to `build_chained_with_match_cte_plan`
3. Modify slightly for chained case
4. **Miss subtle details** (like CTE hoisting or GROUP BY expansion)

**Result**: Inconsistent behavior, bugs resurface in different contexts

### 4. Testing Gaps

**Current testing**:
- ✅ Unit tests for individual components
- ✅ Integration tests for common queries
- ❌ **No tests for CTE hoisting correctness**
- ❌ **No tests for GROUP BY with TableAlias in WITH clauses**
- ❌ **No tests covering all code path combinations**

---

## Impact Analysis

### Bugs Found (December 11, 2025)

1. **VLP CTEs not hoisted in chained WITH**: 3 LDBC queries failing (IC-1, IC-3, IC-9)
2. **GROUP BY TableAlias not expanded**: 1 LDBC query failing (IC-1)

### Potential Future Bugs

Similar issues could occur in:
- OPTIONAL MATCH with VLP + WITH
- Multiple relationship types with VLP + WITH
- Shortest path with aggregation
- Any new feature combining WITH clauses with complex patterns

**Estimated future bug likelihood**: **High** (60-80% for new WITH-related features)

---

## Recommended Solutions

### Short-Term (1-2 days)

1. **Fix GROUP BY in `build_chained_with_match_cte_plan`**
   - Replace lines 661-667 with call to centralized expansion logic
   - Extract `expand_group_by_expressions()` helper function

2. **Add validation**
   - Assert all CTEs referenced in SQL are present in `RenderPlan.ctes`
   - Add debug logging for CTE hoisting

3. **Add tests**
   - Test VLP + WITH + aggregation
   - Test GROUP BY with TableAlias in all WITH handlers

### Medium-Term (1 week)

1. **Extract common WITH rendering logic**
   ```rust
   struct WithClauseRenderer<'a> {
       schema: &'a GraphSchema,
       cte_registry: CteRegistry,
   }
   
   impl WithClauseRenderer {
       fn render_with_clause(&mut self, ...) -> Result<RenderPlan>;
       fn hoist_nested_ctes(&mut self, plan: &mut RenderPlan);
       fn expand_group_by(&self, exprs: Vec<LogicalExpr>) -> Vec<RenderExpr>;
   }
   ```

2. **Centralize CTE management**
   ```rust
   struct CteRegistry {
       ctes: Vec<Cte>,
   }
   
   impl CteRegistry {
       fn register(&mut self, cte: Cte) -> String;  // Returns CTE name
       fn get_all(&self) -> &[Cte];
       fn validate(&self, sql: &str) -> Result<(), Vec<String>>;  // Check all refs exist
   }
   ```

3. **Consolidate three WITH handlers into one**
   - Unified logic with flags for different cases
   - Single place to maintain

### Long-Term (2-4 weeks)

1. **Architectural refactoring**
   - Visitor pattern for plan transformation
   - Builder pattern for RenderPlan construction
   - Explicit CTE dependency graph

2. **Comprehensive testing framework**
   - Property-based tests for CTE hoisting
   - Combinatorial tests for feature interactions
   - SQL validation tests

3. **Documentation**
   - Architecture decision records (ADRs)
   - Code path decision trees
   - Common pitfall documentation

---

## Lessons Learned

### What Went Wrong

1. **Grew organically**: Features added without refactoring existing code
2. **Copy-paste encouraged**: Similar code copied rather than extracted
3. **No design reviews**: Code added without architectural discussion
4. **Implicit assumptions**: CTEs "just work" without explicit management

### What Went Right

1. **Consistent naming**: `generate_cte_id()` improvement applied everywhere
2. **Good logging**: Helped diagnose CTE hoisting issue quickly
3. **Modular design**: Problem isolated to specific functions

### Key Takeaways

> **"Code duplication is a future bug waiting to happen."**

- ✅ **DO**: Extract common logic into reusable functions
- ✅ **DO**: Validate assumptions with assertions
- ✅ **DO**: Test edge cases and combinations
- ❌ **DON'T**: Copy-paste code "just this once"
- ❌ **DON'T**: Assume implicit behavior will propagate
- ❌ **DON'T**: Skip testing because "it's similar to existing code"

---

## Priority Recommendation

**IMMEDIATE ACTION REQUIRED**:

1. Fix GROUP BY expansion in `build_chained_with_match_cte_plan` (30 minutes)
2. Add CTE validation assertions (1 hour)
3. Add tests for WITH + aggregation combinations (2 hours)

**SCHEDULE FOR NEXT SPRINT**:

1. Extract common WITH rendering logic (1-2 days)
2. Implement CTE registry (1 day)
3. Add comprehensive test coverage (2 days)

**Technical debt**: Estimated 1 week to eliminate duplication and fragility.

---

## References

- VLP CTE hoisting fix: Commit b212033
- GROUP BY expansion logic: `extract_group_by` lines 5878-6050
- Duplicate WITH handlers: Lines 272, 413, 981
- Related issue: KNOWN_ISSUES.md (similar patterns in relationship type handling)
