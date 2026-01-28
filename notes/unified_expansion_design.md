# Unified Property Expansion Design

**Date**: December 24, 2025  
**Purpose**: Consolidate ~150 lines of duplicate expansion logic before adding property pruning  
**Based on**: Task 1.3 research findings

---

## Problem Statement

Currently we have **4 expansion sites** with ~150 lines of duplicated logic:
1. WITH clause (line ~1820) - RenderExpr
2. RETURN clause (lines ~5508-5615) - LogicalExpr  
3. GroupBy aggregation (lines ~5877-5920) - RenderExpr
4. Wildcard expansion (lines ~5650-5678) - LogicalExpr

**Duplication pattern**: 
```rust
// Repeated 4 times with minor variations:
- Check if alias is TableAlias
- Lookup properties from GraphNode
- Find ID column
- Loop over properties
- Wrap with anyLast() if aggregation
- Format as expression
```

**Goal**: Single implementation with type-specific wrappers

---

## Architecture

### Core Function (Type-Agnostic)

```rust
/// Core property expansion logic - returns intermediate representation
/// 
/// Returns: Vec<(property_name, column_name, needs_anylast_wrap)>
fn expand_alias_properties_core(
    alias: &str,
    plan_ctx: &PlanCtx,
    cte_schemas: &HashMap<String, (Vec<SelectItem>, ...)>,
    needs_aggregation: bool,
    property_requirements: Option<&PropertyRequirements>,  // NEW: for pruning
) -> Result<Vec<(String, String, bool)>, RendererError>
```

**Logic**:
1. Check if alias exists in PlanCtx
2. Determine if it's a CTE reference or base table
3. Get property list (from CTE schema or GraphNode.projected_columns)
4. **NEW**: Filter properties if `property_requirements` is provided
5. Find ID column for this alias
6. For each property:
   - Determine ClickHouse column name
   - Determine if needs anyLast() wrapping (aggregation + non-ID column)
   - Add to result list
7. Return list of (prop, col, needs_wrap) tuples

**Key Features**:
- ✅ Handles both CTE and base table aliases
- ✅ Supports denormalized nodes
- ✅ Determines anyLast() wrapping based on aggregation flag
- ✅ **NEW**: Property pruning via PropertyRequirements

---

### Type-Specific Wrappers

#### For LogicalExpr (Analyzer Phase)

```rust
/// Expand alias to LogicalExpr ProjectionItems for analyzer/logical plan
pub fn expand_alias_to_projection_items(
    alias: &str,
    plan_ctx: &PlanCtx,
    needs_aggregation: bool,
) -> Result<Vec<ProjectionItem>, RendererError> {
    let props = expand_alias_properties_core(
        alias, 
        plan_ctx, 
        &HashMap::new(),  // No CTE schemas in analyzer
        needs_aggregation,
        None,  // No pruning yet (analyzer runs before requirements known)
    )?;
    
    // Convert (prop, col, wrap) tuples to ProjectionItems
    Ok(props.iter().map(|(prop, col, needs_wrap)| {
        let expr = if *needs_wrap {
            LogicalExpr::AggregateFnCall {
                name: "anyLast".to_string(),
                args: vec![LogicalExpr::PropertyAccessExp(...)],
            }
        } else {
            LogicalExpr::PropertyAccessExp(...)
        };
        ProjectionItem {
            alias: Some(format!("{}.{}", alias, prop)),
            expression: expr,
        }
    }).collect())
}
```

**Used by**:
- Early analyzer passes
- Logical plan construction
- Type inference

---

#### For RenderExpr (Renderer Phase)

```rust
/// Expand alias to RenderExpr SelectItems for SQL generation
/// 
/// # Property Pruning
/// If `property_requirements` is provided, only materializes required properties.
/// This is THE optimization point for collect() and WITH aggregations.
pub fn expand_alias_to_select_items(
    alias: &str,
    plan_ctx: &PlanCtx,
    cte_schemas: &HashMap<String, (Vec<SelectItem>, ...)>,
    needs_aggregation: bool,
    property_requirements: Option<&PropertyRequirements>,  // NEW: enables pruning
) -> Result<Vec<SelectItem>, RendererError> {
    let props = expand_alias_properties_core(
        alias, 
        plan_ctx, 
        cte_schemas,
        needs_aggregation,
        property_requirements,  // PASS THROUGH for pruning
    )?;
    
    // Convert (prop, col, wrap) tuples to SelectItems
    Ok(props.iter().map(|(prop, col, needs_wrap)| {
        let expr = if *needs_wrap {
            RenderExpr::FunctionCall {
                name: "anyLast".to_string(),
                args: vec![RenderExpr::PropertyAccess(...)],
            }
        } else {
            RenderExpr::PropertyAccess(...)
        };
        SelectItem {
            alias: Some(format!("{}.{}", alias, prop)),
            expression: expr,
        }
    }).collect())
}
```

**Used by**:
- WITH clause expansion (line ~1820)
- RETURN clause expansion (lines ~5508-5615)
- GroupBy expansion (lines ~5877-5920)
- Wildcard expansion (lines ~5650-5678)

---

## Property Pruning Integration

### How Pruning Works

**Without PropertyRequirements** (current behavior):
```rust
// Expands ALL properties (50-200 columns)
expand_alias_to_select_items("friend", plan_ctx, cte_schemas, false, None)
→ [friend.id, friend.firstName, friend.lastName, ..., friend.field50]
```

**With PropertyRequirements** (optimized):
```rust
// Only expands required properties (2-3 columns)
let mut reqs = PropertyRequirements::new();
reqs.require_property("friend", "firstName");
reqs.require_property("friend", "lastName");

expand_alias_to_select_items("friend", plan_ctx, cte_schemas, false, Some(&reqs))
→ [friend.id, friend.firstName, friend.lastName]  // Only 3 columns!
```

### Implementation in Core Function

```rust
fn expand_alias_properties_core(..., property_requirements: Option<&PropertyRequirements>) {
    // ... get full property list from schema ...
    
    let properties_to_expand = if let Some(reqs) = property_requirements {
        if reqs.requires_all(alias) {
            // Wildcard - use all properties
            all_properties
        } else if let Some(required_props) = reqs.get_requirements(alias) {
            // Filter to only required properties
            all_properties.into_iter()
                .filter(|prop| required_props.contains(&prop.property_name))
                .collect()
        } else {
            // No requirements for this alias - use all properties (safe default)
            all_properties
        }
    } else {
        // No requirements provided - use all properties
        all_properties
    };
    
    // ... continue with filtered list ...
}
```

**Key Decisions**:
- ✅ ID column ALWAYS included (even if not in requirements) - needed for JOINs
- ✅ Wildcard pattern (`RETURN friend`) → expand all properties
- ✅ Unknown alias → expand all properties (safe default)
- ✅ No requirements provided → expand all properties (backward compatible)

---

## Migration Plan

### Phase 1: Extract Core Logic (No Behavior Change)
1. Create `expand_alias_properties_core()` in `property_expansion.rs`
2. Extract common logic from one expansion site (RETURN clause)
3. Add comprehensive unit tests
4. Verify behavior identical to original

### Phase 2: Add Type Wrappers
1. Create `expand_alias_to_projection_items()` for LogicalExpr
2. Create `expand_alias_to_select_items()` for RenderExpr  
3. Unit test each wrapper
4. Verify output matches original functions

### Phase 3: Replace Call Sites (One at a Time)
1. Replace RETURN expansion (lines ~5508-5615)
   - Test with existing integration tests
2. Replace WITH expansion (line ~1820)
   - Test WITH queries
3. Replace GroupBy expansion (lines ~5877-5920)
   - Test aggregation queries
4. Replace Wildcard expansion (lines ~5650-5678)
   - Test RETURN * queries

### Phase 4: Add Property Pruning Parameter
1. Add `property_requirements: Option<&PropertyRequirements>` to core function
2. Implement filtering logic in core
3. Thread parameter through wrappers
4. **Initially pass None everywhere** - no behavior change yet

### Phase 5: Enable Pruning (After PropertyRequirementsAnalyzer is ready)
1. Implement PropertyRequirementsAnalyzer pass (Phase 2)
2. Update renderer call sites to pass `plan_ctx.get_property_requirements()`
3. Benchmark performance improvements

---

## Testing Strategy

### Unit Tests for Core Function
```rust
#[test]
fn test_expand_alias_properties_basic() {
    // Simple node with 3 properties
}

#[test]
fn test_expand_alias_properties_with_aggregation() {
    // Verify anyLast() wrapping for non-ID columns
}

#[test]
fn test_expand_alias_properties_cte_reference() {
    // Alias referencing CTE instead of base table
}

#[test]
fn test_expand_alias_properties_denormalized() {
    // Properties from edge table
}

#[test]
fn test_expand_alias_properties_with_pruning() {
    // PropertyRequirements filters properties
}

#[test]
fn test_expand_alias_properties_wildcard() {
    // PropertyRequirements.requires_all() → all properties
}
```

### Integration Tests
```rust
#[test]
fn test_return_expansion_unchanged() {
    // Verify RETURN expansion produces same SQL before/after consolidation
}

#[test]
fn test_with_expansion_unchanged() {
    // Verify WITH expansion produces same SQL
}

#[test]
fn test_collect_expansion_unchanged() {
    // Verify collect() produces same SQL
}
```

---

## Benefits

### Code Quality
- ✅ Eliminates ~150 lines of duplication
- ✅ Single source of truth for expansion logic
- ✅ Easier to maintain and debug
- ✅ Consistent behavior across all expansion sites

### Performance (After PropertyRequirementsAnalyzer is added)
- ✅ 85-98% reduction in intermediate result size for wide tables
- ✅ 8-16x faster queries with property pruning
- ✅ Reduced memory pressure on ClickHouse

### Developer Experience
- ✅ Simple API: just call wrapper function
- ✅ Clear separation: core logic vs type conversion
- ✅ Easy to add new expansion sites
- ✅ Property pruning automatically available everywhere

---

## File Locations

**Implementation**:
- `src/render_plan/property_expansion.rs` - Core function and wrappers

**Call Sites to Update**:
- `src/render_plan/plan_builder.rs`:
  - Line ~1820 (WITH)
  - Lines ~5508-5615 (RETURN)
  - Lines ~5877-5920 (GroupBy)
  - Lines ~5650-5678 (Wildcard)

**Tests**:
- `src/render_plan/property_expansion.rs` - Unit tests
- `tests/integration/` - Integration tests for each expansion type

---

## Next Steps

1. ✅ **Task 1.4 (This Document)**: Design complete
2. 🔜 **Task 1.5**: Implement core function in `property_expansion.rs`
3. 🔜 **Task 1.6**: Add type-specific wrappers
4. 🔜 **Task 1.7-1.8**: Replace call sites one by one with comprehensive testing

**Timeline**: Phase 1 consolidation should take ~2-3 days, then Phase 2 (PropertyRequirementsAnalyzer) can begin in parallel.
