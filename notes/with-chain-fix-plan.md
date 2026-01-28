# WITH Chain Rendering Fix - Action Plan

## Problem Statement

**Current State**: Chained WITH clauses (e.g., `WITH a WITH a, n WITH a, n, x`) generate invalid SQL with:
- Nested WITH statements: `WITH cte1 AS (WITH cte2 AS (...))`  
- UNION ALL artifacts
- Duplicate columns
- Wrong CTE references

**Root Cause**: The renderer (`render_plan/plan_builder.rs`) doesn't systematically handle nested `LogicalPlan::WithClause` nodes.

**Scope Impact**:
- ✅ Single WITH: Works  
- ✅ WITH → MATCH: Works (scope barriers implemented correctly)
- ❌ WITH → WITH: Broken
- ❌ WITH → WITH → WITH: Broken

## Solution Architecture

### Core Principle
When rendering a plan containing WithClause nodes, **collect all consecutive WITH clauses** and render them as a flat CTE chain at the top level.

### Logical Plan Structure (Correct)
```
Query: MATCH (a) WITH a, a.name as n WITH a, n WITH a RETURN a.id

Logical Plan:
  Projection(...)
    └─ WithClause#3(items=[a], input=...)           // Third WITH
         └─ WithClause#2(items=[a, n], input=...)   // Second WITH  
              └─ WithClause#1(items=[a, a.name as n], input=...)  // First WITH
                   └─ ViewScan(User as a)
```

### Target SQL Structure (Correct)
```sql
WITH 
  with_a_n_cte1 AS (
    SELECT a.user_id AS "a_user_id", a.name AS "a_name", a.name AS "n"
    FROM users AS a
  ),
  with_a_n_cte2 AS (
    SELECT a_user_id AS "a_user_id", n AS "n"
    FROM with_a_n_cte1
  ),
  with_a_cte3 AS (
    SELECT a_user_id AS "a_user_id"  
    FROM with_a_n_cte2
  )
SELECT a.user_id AS "a.id"
FROM with_a_cte3 AS a
```

## Implementation Plan

### Phase 1: Add CTE Collection Helper (30 min)
**File**: `src/render_plan/plan_builder_helpers.rs`

Add function to collect all consecutive WithClause nodes:
```rust
/// Collect all consecutive WithClause nodes from a plan tree
/// Returns (Vec<WithClause>, base_plan) where base_plan is the non-WITH input
fn collect_with_chain(plan: &LogicalPlan) -> (Vec<&WithClause>, &LogicalPlan) {
    let mut with_clauses = Vec::new();
    let mut current = plan;
    
    while let LogicalPlan::WithClause(wc) = current {
        with_clauses.push(wc);
        current = &wc.input;
    }
    
    (with_clauses, current)
}
```

### Phase 2: Refactor Top-Level Rendering (1-2 hours)
**File**: `src/render_plan/plan_builder.rs` - `to_render_plan()`

Modify the entry point to detect and handle WITH chains:

```rust
pub fn to_render_plan(plan: &LogicalPlan, schema: &GraphSchema) -> Result<RenderPlan> {
    // Check if we have consecutive WITH clauses
    let (with_chain, base_plan) = collect_with_chain(plan);
    
    if !with_chain.is_empty() {
        // Handle WITH chain specially
        return render_with_chain(with_chain, base_plan, schema);
    }
    
    // Existing logic for non-WITH plans
    match plan {
        LogicalPlan::Projection(_) => { ... }
        LogicalPlan::WithClause(wc) => {
            // This should not happen after collect_with_chain
            // but keep as fallback
            render_single_with(wc, schema)
        }
        // ... other cases
    }
}
```

### Phase 3: Implement WITH Chain Renderer (2-3 hours)
**File**: `src/render_plan/plan_builder.rs`

New function to render chains systematically:

```rust
fn render_with_chain(
    with_chain: Vec<&WithClause>,
    base_plan: &LogicalPlan,
    schema: &GraphSchema
) -> Result<RenderPlan> {
    let mut all_ctes = Vec::new();
    let mut previous_cte_name: Option<String> = None;
    
    // Start with base plan
    let mut current_input_plan = render_plan(base_plan, schema)?;
    
    // Process each WITH in order (innermost to outermost)
    for (idx, with_clause) in with_chain.iter().rev().enumerate() {
        // Generate CTE name from exported aliases
        let cte_name = generate_cte_name(&with_clause.exported_aliases, idx);
        
        // Render this WITH as a CTE that references previous CTE or base
        let cte = render_with_as_cte(
            with_clause,
            &current_input_plan,
            previous_cte_name.as_ref(),
            schema
        )?;
        
        all_ctes.push(cte);
        
        // Next WITH will reference this CTE
        previous_cte_name = Some(cte_name.clone());
        current_input_plan = create_cte_reference_plan(&cte_name, &with_clause.exported_aliases);
    }
    
    // Final SELECT references the last CTE
    let final_select = build_final_select(
        &with_chain[0], // Outermost WITH
        &previous_cte_name.unwrap(),
        schema
    )?;
    
    Ok(RenderPlan::with_ctes(all_ctes, final_select))
}
```

### Phase 4: Testing (1 hour)

Test cases:
1. ✅ Single WITH: `MATCH (a) WITH a RETURN a` (regression test)
2. ✅ WITH → MATCH: `MATCH (a) WITH a MATCH (a)-[]->(b) RETURN a, b` (regression test)  
3. ✅ Two WITH: `MATCH (a) WITH a, a.name as n WITH a, n RETURN a.id, n`
4. ✅ Three WITH: `MATCH (a) WITH a, a.name as n WITH a, n, upper(n) as un WITH a, un WHERE len(un)>5 RETURN a.id, un`
5. ✅ WITH with aggregation: `MATCH (a)-[]->(b) WITH a, COUNT(b) as cnt WITH a, cnt WHERE cnt>5 RETURN a, cnt`

## Success Criteria

- [ ] All test cases pass
- [ ] Generated SQL is valid (no nested WITH)
- [ ] CTEs reference previous CTEs correctly
- [ ] No duplicate columns
- [ ] No UNION ALL artifacts
- [ ] Scope barriers still work (variables properly shielded)

## Estimated Time

**Total**: 4-6 hours

## Risk Assessment

**Low Risk**: 
- Changes are isolated to rendering layer
- Planning logic (scope barriers) is proven correct
- Can be implemented incrementally with fallbacks

## Alternative: Quick Fix

If full refactor is too much, we could add a **preprocessing pass** that flattens nested WithClause nodes before rendering:

```rust
fn flatten_with_chain(plan: LogicalPlan) -> LogicalPlan {
    // Transform: WithClause(WithClause(WithClause(base)))
    // Into: FlatWithChain { withs: [w1, w2, w3], base }
    // Add new LogicalPlan variant: FlatWithChain
}
```

This is cleaner separation but requires new LogicalPlan type.
