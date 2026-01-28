# Metadata Preservation Architecture

## Problem

The `tuple_properties` field (and any future metadata fields) must be manually preserved through all query pipeline transformations. Currently this requires updating **19 different files** whenever a metadata field is added. This is error-prone and created bugs during the collect+UNWIND implementation.

### Current Pattern (Error-Prone)

```rust
LogicalPlan::Unwind(u) => {
    let child_tf = self.analyze(u.input.clone(), _plan_ctx)?;
    match child_tf {
        Transformed::Yes(new_input) => Transformed::Yes(Arc::new(LogicalPlan::Unwind(
            Unwind {
                input: new_input,
                expression: u.expression.clone(),
                alias: u.alias.clone(),
                label: u.label.clone(),
                tuple_properties: u.tuple_properties.clone(), // EASY TO FORGET!
            },
        ))),
        Transformed::No(_) => Transformed::No(logical_plan.clone()),
    }
}
```

**Issues**:
1. Must remember to add `.clone()` in 19+ places
2. New metadata fields require updating all 19+ files
3. Easy to forget during refactoring
4. No compiler assistance - silently drops metadata

## Proposed Solutions (Ranked)

### Option 1: Builder Pattern with Default Preservation (RECOMMENDED)

Add a builder method to Unwind that clones all metadata by default:

```rust
impl Unwind {
    /// Create a new Unwind node from an existing one, preserving metadata
    /// while allowing input modification
    pub fn with_new_input(&self, new_input: Arc<LogicalPlan>) -> Self {
        Unwind {
            input: new_input,
            expression: self.expression.clone(),
            alias: self.alias.clone(),
            label: self.label.clone(),
            tuple_properties: self.tuple_properties.clone(),
            // Future metadata fields automatically preserved here
        }
    }
    
    /// Create a new Unwind node from an existing one with modified expression
    pub fn with_new_expression(&self, new_expr: LogicalExpr) -> Self {
        Unwind {
            input: self.input.clone(),
            expression: new_expr,
            alias: self.alias.clone(),
            label: self.label.clone(),
            tuple_properties: self.tuple_properties.clone(),
        }
    }
}
```

**Usage**:
```rust
LogicalPlan::Unwind(u) => {
    let child_tf = self.analyze(u.input.clone(), _plan_ctx)?;
    match child_tf {
        Transformed::Yes(new_input) => {
            Transformed::Yes(Arc::new(LogicalPlan::Unwind(
                u.with_new_input(new_input)
            )))
        }
        Transformed::No(_) => Transformed::No(logical_plan.clone()),
    }
}
```

**Pros**:
- ✅ Simple to implement
- ✅ No proc macros needed
- ✅ Explicit and readable
- ✅ Adding new metadata = update one place (the method)
- ✅ Works with current architecture

**Cons**:
- ❌ Need one method per modification pattern
- ❌ Still manual (but centralized)

### Option 2: Spread Operator Pattern

Use Rust's struct update syntax more consistently:

```rust
Unwind {
    input: new_input,
    ..u.clone() // Preserve all other fields
}
```

**Pros**:
- ✅ Built into Rust
- ✅ Very concise
- ✅ Automatically preserves all fields

**Cons**:
- ❌ Still requires remembering to use `..u.clone()`
- ❌ Less explicit about what's being preserved
- ❌ Doesn't prevent forgetting the pattern

### Option 3: Derive Macro for "Rebuildable"

Create a custom derive macro:

```rust
#[derive(Rebuildable)]
pub struct Unwind {
    #[rebuild(required)]
    pub input: Arc<LogicalPlan>,
    #[rebuild(preserve)]
    pub tuple_properties: Option<Vec<(String, usize)>>,
    // ...
}

// Generated methods:
impl Unwind {
    pub fn rebuild_with_input(&self, input: Arc<LogicalPlan>) -> Self { ... }
}
```

**Pros**:
- ✅ Fully automatic
- ✅ Compiler-enforced
- ✅ Most robust long-term

**Cons**:
- ❌ Complex to implement
- ❌ Requires proc macro knowledge
- ❌ Overkill for current needs

## Recommendation: Start with Option 1

**Phase 1** (Immediate): Implement builder methods for Unwind
- Add `with_new_input()`, `with_new_expression()` methods
- Update 2-3 files as proof of concept
- Validate pattern works well

**Phase 2** (If successful): Extend to other plan nodes
- Add similar methods to Projection, Filter, etc.
- Gradually refactor pipeline code

**Phase 3** (Future): Consider Option 3 if we add many more metadata fields

## Implementation Checklist

- [ ] Add builder methods to Unwind struct
- [ ] Update 3 representative files (analyzer, optimizer, render_plan)
- [ ] Test that metadata is preserved correctly
- [ ] Document pattern in DEVELOPMENT_PROCESS.md
- [ ] (Optional) Extend to other plan nodes

## Files That Currently Need Manual Updates

19 files clone `tuple_properties`:
1. `render_plan/alias_resolver.rs`
2. `query_planner/optimizer/projection_push_down.rs`
3. `query_planner/analyzer/plan_sanitization.rs`
4. `query_planner/analyzer/duplicate_scans_removing.rs`
5. `query_planner/analyzer/filter_tagging.rs`
6. `query_planner/analyzer/projection_tagging.rs` (2 places)
7. `query_planner/analyzer/group_by_building.rs`
8. `query_planner/analyzer/graph_traversal_planning.rs`
9. `query_planner/analyzer/bidirectional_union.rs`
10. `query_planner/analyzer/query_validation.rs`
11. `query_planner/analyzer/unwind_property_rewriter.rs`
12. `query_planner/optimizer/filter_push_down.rs`
13. `query_planner/analyzer/type_inference.rs`
14. `query_planner/analyzer/schema_inference.rs`
15. `query_planner/analyzer/graph_join_inference.rs`
16. `query_planner/optimizer/view_optimizer.rs`
17. `query_planner/optimizer/filter_into_graph_rel.rs`
18. `query_planner/analyzer/variable_resolver.rs`

## See Also

- Original issue discovered during tuple_properties implementation (Dec 20, 2025)
- Similar pattern needed for all metadata fields on all plan nodes
