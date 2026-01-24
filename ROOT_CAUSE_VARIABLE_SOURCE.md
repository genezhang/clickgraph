# Root Cause Analysis: Variable Source Resolution

## Problem Statement

When a query like `MATCH (u:User) WITH u AS person RETURN person.name` is executed, the generated SQL tries to access `person.u_full_name` (which doesn't exist in the CTE) instead of `person.u_name` (which the CTE actually exports).

### Error Trace
```
Test: test_simple_node_renaming
CTE generates: WITH with_person_cte_1 AS (SELECT u.full_name AS u_name, ...)
SQL tries: SELECT person.u_full_name AS "person.name" FROM with_person_cte_1 AS person
Error: Identifier 'person.u_full_name' cannot be resolved
```

## Root Cause Map

```
Phase 1: LOGICAL PLANNING
  │
  ├─ MATCH (u:User) WITH u AS person
  └─ Variables registered in VariableRegistry:
     ├─ u → NodeVariable(labels: ["User"], source: Match)
     └─ person → NodeVariable(labels: ["User"], source: Cte { cte_name: "with_person_cte_1" })
  
Phase 2: ANALYSIS (WHERE THE BUG HAPPENS)
  │
  ├─ FilterTagging pass: apply_property_mapping()
  │  │
  │  ├─ Encounters PropertyAccessExp(table_alias: "person", column: "name")
  │  │
  │  ├─ Looks up "person" in plan_ctx → finds TableCtx(alias: "person", label: Some("User"))
  │  │
  │  ├─ Gets label "User" and uses schema lookup:
  │  │  └─ graph_schema.get_node_schema("User").resolve_property("name")
  │  │     └─ Returns "full_name" (the DB column for User.name)
  │  │
  │  ├─ Rewrites to: PropertyAccessExp(table_alias: "person", column: "full_name")
  │  │
  │  └─ ❌ BUG: Never checks that "person" is CTE-sourced!
  │     "person" doesn't have column "full_name"
  │     It only has columns exported by the CTE: "u_name", "u_city", etc.
  │
  ├─ ProjectionTagging pass: Similar rewriting happens for SELECT items
  │
  └─ Result: PropertyAccessExp(table_alias: "person", column: "full_name")
     ✅ Correct if "person" was base table
     ❌ WRONG if "person" is CTE-sourced!

Phase 3: RENDER/SQL GENERATION
  │
  ├─ Generates: SELECT person.u_full_name FROM with_person_cte_1 AS person
  │
  ├─ CTE registry approach attempted (previous attempt):
  │  └─ Created thread-local CTE_COLUMN_REGISTRY in render phase
  │     └─ ❌ TOO LATE - damage already done in analysis phase
  │        Property already incorrectly rewritten to "full_name"
  │
  └─ ❌ Error: person.u_full_name cannot be resolved

```

## The Real Problem: Variable Source Information Lost During Analysis

The system CORRECTLY tracks variables and their sources in Phase 1:
- `person` is registered with `source: Cte { cte_name: "with_person_cte_1" }`
- This information EXISTS in VariableRegistry

But during Phase 2 (Analysis), this information is IGNORED:
- FilterTagging applies schema mapping without checking variable source
- It treats `person` like a regular table alias: `person.column`
- It never thinks: "Wait, is person a CTE? If so, I shouldn't apply schema mapping"

## Why CTE Registry in Render Phase Doesn't Work

The CTE registry approach added to render_plan/mod.rs is too late because:

1. **Analysis Phase Happens First**
   - FilterTagging runs during query planning
   - Already rewrites `person.name` → `person.full_name`

2. **Render Phase Gets Wrong Input**
   - By the time select_builder.rs runs, PropertyAccessExp already has `column: "full_name"`
   - CTE registry can't fix this - it only knows what the CTE actually exports
   - It doesn't know that "full_name" should have been "u_name"

3. **The Mapping is Lost**
   - We need to track: `person.name` → should resolve to → CTE export `u_name`
   - This requires mapping through: Cypher property → DB column → CTE export column
   - Analysis phase needs this three-layer resolution

## The Fix: Two-Layer Approach

### Layer 1: Fix FilterTagging to Check Variable Sources (CRITICAL)

In `src/query_planner/analyzer/filter_tagging.rs`, method `apply_property_mapping()`:

```rust
// BEFORE (line 663):
let label = table_ctx.get_label_opt().ok_or_else(|| {
    // Error handling...
})?;

// Get mapped column using schema
let mapped_column = view_resolver.resolve_node_property(&label, property)?;

// AFTER (FIXED):
// NEW: Check if this is a CTE-sourced variable
if let Some(TypedVariable::Node(node_var)) = plan_ctx.lookup_variable(&alias) {
    if matches!(node_var.source, VariableSource::Cte { .. }) {
        // ✅ This is a CTE variable - DON'T apply schema mapping
        // The CTE's output columns are already the mapped columns
        // Return property as-is for CTE lookup later
        return Ok(LogicalExpr::PropertyAccessExp(property_access));
    }
}

// Only apply schema mapping if this is a base table
let label = table_ctx.get_label_opt().ok_or_else(|| {
    // Error handling...
})?;
let mapped_column = view_resolver.resolve_node_property(&label, property)?;
```

### Layer 2: Add CTE Column Information to PlanCtx

Track what a CTE actually exports:

```rust
// In PlanCtx:
pub cte_column_mapping: HashMap<String, HashMap<String, String>>
// Maps: cte_name → (cypher_property → exported_column_name)
// Example: "with_person_cte_1" → {"name" → "u_name", "city" → "u_city"}

// When processing WITH clause:
plan_ctx.set_cte_exports(cte_name, export_mapping);

// During render phase (NEW):
if let Some(TypedVariable::Node(node_var)) = plan_ctx.lookup_variable(&alias) {
    if let VariableSource::Cte { cte_name } = &node_var.source {
        if let Some(export_map) = plan_ctx.get_cte_exports(cte_name) {
            if let Some(exported_col) = export_map.get(property_name) {
                // Use the exported column name
                return Ok(PropertyAccessExp {
                    table_alias: alias,
                    column: exported_col.clone()
                });
            }
        }
    }
}
```

## Three-Layer Property Resolution

The complete flow should be:

### For Base Table Variables:
```
Cypher Property → Schema Mapping → Database Column
  user.name        →   (resolve "name")   →   full_name
  person.name*     →   (CTE source)       →   user.full_name
```

### For CTE-Sourced Variables:
```
Cypher Property → CTE Export Lookup → Exported Column Name
  person.name  →   (lookup in CTE)  →   u_name
                   (FROM source u's export of u.full_name)
```

## Current Code Status

### ✅ What Already Exists

1. **TypedVariable Infrastructure** (`src/query_planner/typed_variable.rs`):
   - `NodeVariable` tracks labels and source (Match vs Cte)
   - `VariableRegistry` stores all variables with sources
   - `VariableSource::Cte { cte_name }` correctly identifies CTE-sourced

2. **PlanCtx Integration** (`src/query_planner/plan_ctx/mod.rs`):
   - `variables: VariableRegistry` field stores all variables
   - `lookup_variable(name)` retrieves typed variables with sources
   - Already used for type checking

3. **CTE Entity Type Tracking** (Jan 2025 fixes):
   - `TableCtx::new_with_cte_reference()` preserves labels for CTE exports
   - `plan_ctx.get_cte_entity_type()` retrieves entity info for CTEs

### ❌ What's Missing

1. **FilterTagging Variable Source Awareness**:
   - Never calls `plan_ctx.lookup_variable()` to check if variable is CTE-sourced
   - Always applies schema mapping regardless of variable source

2. **CTE Export Column Tracking**:
   - PlanCtx doesn't track what columns a CTE actually exports
   - No mapping from Cypher property → exported column name
   - Render phase can't look up correct CTE output columns

3. **Projection Tagging Variable Source Awareness**:
   - Similar issue as FilterTagging
   - Applies schema mapping without checking if variable is CTE

## Implementation Steps

### Step 1: Add to FilterTagging (5-10 min)
Before applying schema mapping, check if variable is CTE-sourced:
- If CTE: skip schema mapping, return property as-is
- If base table: apply schema mapping as before

### Step 2: Add to ProjectionTagging (5-10 min)
Same fix as FilterTagging

### Step 3: Extend PlanCtx (10-15 min)
Add CTE export mapping tracking:
- Track what columns each CTE exports
- Add lookup methods for render phase

### Step 4: Update Render Phase (15-20 min)
Use CTE export info when resolving properties:
- Check if variable is CTE-sourced
- If yes, use CTE export mapping
- If no, use regular table column lookup

### Step 5: Test (ongoing)
- Run test_simple_node_renaming
- Run full integration test suite
- Check for any property mapping regressions

## Files to Modify

1. `src/query_planner/analyzer/filter_tagging.rs` - Fix apply_property_mapping()
2. `src/query_planner/analyzer/projection_tagging.rs` - Same fix
3. `src/query_planner/plan_ctx/mod.rs` - Add CTE export tracking
4. `src/render_plan/select_builder.rs` - Use CTE export info
5. `src/render_plan/plan_builder.rs` - Populate CTE export info

## Key Insight

The problem isn't that we don't track variable sources. We do! The problem is that we **ignore the tracking information during analysis**. FilterTagging and ProjectionTagging apply schema mappings without consulting the VariableRegistry to see where variables actually come from.

The fix is simple: **check the variable source before applying schema mapping**.

- If source == Match → apply schema mapping (as before)
- If source == Cte → skip schema mapping, let render phase use CTE's actual exports

This is a fundamental architectural principle: **CTE-sourced variables have different column names than their source variables, and we must respect that.**
