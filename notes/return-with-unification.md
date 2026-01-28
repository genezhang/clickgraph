# RETURN and WITH Clause Unification

## Date: December 20, 2025

## The Insight

According to OpenCypher grammar (lines 232-233 in `openCypher grammar.bnf`):

```bnf
<return statement> ::= 
  RETURN <return statement body> [ <order by and page clause> ]

<with statement> ::= 
  WITH <return statement body> [ <order by and page clause> ] [ <where clause> ]
```

**They're structurally identical except for one optional WHERE clause!**

Both have:
- ✅ `<return statement body>` (projection items with DISTINCT)
- ✅ `<order by and page clause>` (ORDER BY, SKIP, LIMIT)
- ⚠️ WITH has optional WHERE at the end

## Current Implementation (Problematic)

### RETURN Representation (Nested Tree)
```
Limit { count: 10 
  input: Skip { count: 5
    input: OrderBy { items: [...]
      input: Projection { items: [RETURN items], distinct: false }
    }
  }
}
```

**Structure**: Multiple separate nodes wrapping each other
- `Projection`: Just items + distinct (lines 662-668 in mod.rs)
- `OrderBy`: Separate node (line 699)
- `Skip`: Separate node (line 707)
- `Limit`: Separate node (line 713)

### WITH Representation (All-in-One)
```rust
WithClause {
  items: Vec<ProjectionItem>,
  distinct: bool,
  order_by: Option<Vec<OrderByItem>>,  // ✅ Built-in
  skip: Option<u64>,                    // ✅ Built-in
  limit: Option<u64>,                   // ✅ Built-in
  where_clause: Option<LogicalExpr>,    // ✅ Built-in
  exported_aliases: Vec<String>,
  cte_references: HashMap<String, String>,
}
```

**Structure**: Single node with all clauses (lines 239-274 in mod.rs)

## The Problem: Duplicate Processing Logic

Because of different representations, we have **two separate property expansion paths**:

### Path 1: RETURN Processing
1. `LogicalPlan::Projection` → `extract_select_items()` (line 5393)
2. Expands `TableAlias("r")` to multiple `PropertyAccessExp` (line 5453-5487)
3. Converts `LogicalExpr` → `RenderExpr` (line 5629)
4. Creates final SELECT

**Code**: ~150 lines in `plan_builder.rs` lines 5393-5650

### Path 2: WITH Processing  
1. `WithClause` → `build_chained_with_match_cte_plan()` (line 1673)
2. Gets `items` from `WithClause.items` (still has `TableAlias("r")`)
3. Manually expands `TableAlias` AGAIN (line 1741)
4. Converts `LogicalExpr` → `RenderExpr` (line 1736)
5. Creates CTE

**Code**: ~100 lines in `plan_builder.rs` lines 1673-1800

**Result**: ~250 lines doing essentially the same thing!

## Why This Happened

Historical evolution:
1. Initially: Both used `Projection` (shared structure)
2. Separation: WITH needed scope tracking (`exported_aliases`, `cte_references`)
3. Consequence: Split into different representations → duplicate expansion logic
4. Today: Two paths doing the same work differently

## The Differences (Minimal!)

### Semantic Differences
| Aspect | RETURN | WITH |
|--------|--------|------|
| Position | End of query | Middle of query |
| SQL Output | Final SELECT | CTE (WITH clause) |
| Continuation | Terminates query | Bridges to next clause |
| Scope | Returns to client | Exports to next segment |
| WHERE clause | Before (in WHERE) | After (in WHERE) |

### Metadata Differences (WITH only)
- `exported_aliases`: Which variables are visible downstream
- `cte_references`: Maps aliases to their source CTEs
- (For scope isolation and CTE rendering)

## The Unification Plan

### Short-Term: Consolidate Processing (This Session)

**Goal**: Single property expansion helper for both paths

1. Add to `property_expansion.rs`:
   ```rust
   /// Expand alias to SelectItem (RenderExpr)
   pub fn expand_alias_to_select_items(
       alias: &str,
       properties: Vec<(String, String)>,
       actual_table_alias: Option<String>,
   ) -> Vec<SelectItem>
   ```

2. Both RETURN and WITH use same helper
3. Benefits:
   - ✅ **Single source of truth** for expansion
   - ✅ **~100 lines removed** (consolidate manual expansion)
   - ✅ **Bug fixes apply to both** (like the from_id/to_id fix)

### Long-Term: Unify Structures (Future Refactor)

**Goal**: Single logical plan node for both RETURN and WITH

```rust
pub struct ProjectionClause {
    pub input: Arc<LogicalPlan>,
    pub items: Vec<ProjectionItem>,
    pub distinct: bool,
    
    // Clauses (present in both RETURN and WITH)
    pub order_by: Option<Vec<OrderByItem>>,
    pub skip: Option<u64>,
    pub limit: Option<u64>,
    pub where_clause: Option<LogicalExpr>,  // Only for WITH per grammar
    
    // Discriminator
    pub kind: ProjectionKind,
    
    // WITH-specific metadata (only when kind = WithClause)
    pub exported_aliases: Option<Vec<String>>,
    pub cte_references: Option<HashMap<String, String>>,
}

pub enum ProjectionKind {
    ReturnClause,  // Final SELECT
    WithClause,    // CTE
}
```

Benefits:
- ✅ **Single tree structure** (no nested OrderBy/Skip/Limit)
- ✅ **Single processing path** (one `extract_select_items` implementation)
- ✅ **~500+ lines removed** (~50% code reduction)
- ✅ **Clearer semantics** (matches grammar structure)
- ✅ **Easier maintenance** (change once, works for both)

### Migration Path

1. **Phase 1** (This session): Consolidate helpers
   - Add `expand_alias_to_select_items()` to `property_expansion.rs`
   - Update both RETURN and WITH to use it
   - Test: All existing tests pass

2. **Phase 2** (Future PR): Unify structures
   - Create `ProjectionClause` with `ProjectionKind`
   - Add parser support to build unified structure
   - Update all LogicalPlan match arms
   - Update analyzers to handle unified node
   - Update rendering to check `kind` field

3. **Phase 3** (Future PR): Remove old structures
   - Deprecate separate `OrderBy`/`Skip`/`Limit` nodes
   - Remove duplicate code paths
   - Update documentation

## Key Insight

The OpenCypher grammar already tells us they should be unified:
```bnf
<with statement> ::= WITH <return statement body> [...]
                     ^^^^                          
                     Same structure!
```

The current separation is an **implementation detail**, not a semantic requirement. By unifying them, we align code with grammar and eliminate significant duplication.

## References

- OpenCypher Grammar: `src/open_cypher_parser/open_cypher_specs/openCypher grammar.bnf` lines 232-233
- Current Structures: `src/query_planner/logical_plan/mod.rs` lines 239-274 (WITH), 662-668 (Projection)
- RETURN Processing: `src/render_plan/plan_builder.rs` lines 5393-5650
- WITH Processing: `src/render_plan/plan_builder.rs` lines 1673-1800
- Property Expansion: `src/render_plan/property_expansion.rs` (consolidation target)

## Status

- **Documented**: ✅ December 20, 2025
- **Short-term fix**: ⏳ Pending (consolidate helpers)
- **Long-term refactor**: 📋 Planned (unify structures)

This architectural insight significantly reduces codebase complexity while improving maintainability. The grammar-driven design provides a clear path forward.
