# Property Pruning: Multi-Scope Processing & Relationship with Existing Resolvers

**Date**: December 24, 2025  
**Context**: Questions about property pruning optimization architecture

---

## Question 1: Multi-Scope Processing with WITH/RETURN

### Short Answer

**Yes, using ROOT-TO-LEAF tree traversal** (from RETURN down to MATCH in the plan tree). The property requirements analysis must work **from final RETURN backwards to initial MATCH** to correctly identify what each scope needs to pass through.

**Terminology Note**: This is "top-down" from tree perspective (RETURN is root) but "bottom-up" from query execution perspective (RETURN executes last). We're on the same page! 😊

### Tree Structure Visualization

```
LogicalPlan Tree (top = root, bottom = leaves):

        ┌─────────────────┐
        │  Projection     │  ← Tree ROOT (RETURN clause)
        │  (RETURN)       │     Step 1: Start here
        └────────┬────────┘
                 │ input
        ┌────────▼────────┐
        │  OrderBy        │  ← Step 2: Extract from ORDER BY
        └────────┬────────┘
                 │ input
        ┌────────▼────────┐
        │  WithClause     │  ← Step 3: Propagate through WITH
        └────────┬────────┘     (scope boundary!)
                 │ input
        ┌────────▼────────┐
        │  GraphJoins     │  ← Step 4: Continue down
        └────────┬────────┘
                 │ input
        ┌────────▼────────┐
        │  GraphNode      │  ← Tree LEAF (MATCH clause)
        │  (MATCH)        │     Step 5: End here
        └─────────────────┘

Analysis Direction: TOP → DOWN (root to leaves) 🌳
Execution Direction: BOTTOM → TOP (MATCH executes first, RETURN last)
```

We traverse from **tree root (RETURN) down to leaves (MATCH)** - this is top-down in tree terminology!

### Detailed Explanation

#### Why Root-to-Leaf Traversal? (RETURN → MATCH)

**Example Query**:
```cypher
MATCH (a:User)-[:FOLLOWS]->(b:User)         -- Scope 1
WITH a, collect(b) as friends               -- Scope 2 boundary
UNWIND friends as friend                    -- Scope 3
MATCH (friend)-[:LIKES]->(p:Post)           -- Scope 4
RETURN friend.firstName, p.title            -- Final projection
```

**Analysis Flow** (Tree Root → Leaves):

```
Step 1: Start at Tree Root - Final RETURN (Scope 4)
├─> friend.firstName needed
├─> p.title needed
└─> Requirements: { friend: {firstName, id}, p: {title, id} }

Step 2: Analyze MATCH in Scope 4
├─> friend.id needed for JOIN (from previous step)
├─> No additional properties
└─> Requirements bubble up: { friend: {firstName, id} }

Step 3: Analyze UNWIND (Scope 3)
├─> friends array must contain firstName (from Step 1)
├─> Must also contain id for JOIN (from Step 2)
└─> Requirements: { friends: contains(firstName, id) }

Step 4: Analyze WITH collect() (Scope 2)
├─> collect(b) must include firstName, id (from Step 3)
├─> a.id needed for GROUP BY
└─> Requirements: { b: {firstName, id}, a: {id} }

Step 5: Analyze MATCH (Scope 1)
├─> a.id needed (from Step 4)
├─> b.firstName, b.id needed (from Step 4)
└─> Final requirements determined!
```

**Result**: Only collect `b.firstName` and `b.id` instead of all 50 properties!

#### Leaf-to-Root Would Fail (MATCH → RETURN)

If we analyzed from leaves upward (MATCH first):
- At MATCH (a)-[:FOLLOWS]->(b): We don't know what properties are needed yet!
- At WITH collect(b): Can't decide what to collect without knowing downstream usage
- **We'd have to collect everything "just in case"** ❌

**Note**: This is "top-down" in query execution order but "bottom-up" in tree structure - either way, it doesn't work!

### Implementation: Recursive Root-to-Leaf Traversal

```rust
impl PropertyRequirementsAnalyzer {
    fn collect_requirements_recursive(
        &self,
        plan: &Arc<LogicalPlan>,
        reqs: &mut PropertyRequirements,
    ) {
        match plan.as_ref() {
            // TREE ROOT: Start from final projection (RETURN)
            LogicalPlan::Projection(p) => {
                // First, extract requirements from this scope's items
                for item in &p.items {
                    self.extract_from_expr(&item.expression, reqs);
                }
                
                // Then, recurse DOWN the tree (towards MATCH leaves)
                self.collect_requirements_recursive(&p.input, reqs);
            }
            
            // SCOPE BOUNDARY: WITH clause
            LogicalPlan::WithClause(wc) => {
                // First, analyze downstream (what comes AFTER this WITH)
                // This tells us what properties we need to pass through
                let mut downstream_reqs = PropertyRequirements::new();
                // (downstream analysis happens in parent Projection)
                
                // Then, analyze WITH items to see what's being collected
                for item in &wc.items {
                    if let LogicalExpr::AggregateFnCall(agg) = &item.expression {
                        if agg.name.eq_ignore_ascii_case("collect") {
                            if let LogicalExpr::TableAlias(alias) = &agg.args[0] {
                                // Found collect(alias) - apply requirements from downstream
                                // This determines what to include in groupArray(tuple(...))
                                self.ensure_downstream_requirements(
                                    &alias.0, 
                                    reqs, 
                                    plan
                                );
                            }
                        }
                    }
                }
                
                // Finally, recurse to input (continues UP towards MATCH)
                self.collect_requirements_recursive(&wc.input, reqs);
            }
            
            // Continue recursing up...
            _ => {
                for child in plan.inputs() {
                    self.collect_requirements_recursive(&child, reqs);
                }
            }
        }
    }
}
```

### Scope Isolation & Propagation

**Key Principle**: Each WITH creates a **scope boundary** that we must respect:

```rust
// PlanCtx already tracks scope boundaries via is_with_scope flag
pub struct PlanCtx {
    is_with_scope: bool,  // true for WITH clause scopes
    parent: Option<Box<PlanCtx>>,
}

// PropertyRequirements must respect these boundaries:
impl PropertyRequirementsAnalyzer {
    fn propagate_requirements_through_scope(
        &self,
        scope_boundary: &WithClause,
        downstream_reqs: &PropertyRequirements,
    ) -> PropertyRequirements {
        let mut upstream_reqs = PropertyRequirements::new();
        
        // For each downstream requirement, check if it's exported by this WITH
        for (alias, props) in &downstream_reqs.required_properties {
            // Check if this alias is in WITH items
            if let Some(with_item) = self.find_with_item(scope_boundary, alias) {
                // Alias is exported - propagate requirements upstream
                match &with_item.expression {
                    // collect(node) - propagate to collected node
                    LogicalExpr::AggregateFnCall(agg) 
                        if agg.name.eq_ignore_ascii_case("collect") => {
                        if let LogicalExpr::TableAlias(source) = &agg.args[0] {
                            // Copy requirements to source alias
                            upstream_reqs.require_properties(&source.0, props);
                        }
                    }
                    
                    // Simple alias passthrough: WITH node
                    LogicalExpr::TableAlias(source) => {
                        upstream_reqs.require_properties(&source.0, props);
                    }
                    
                    // Expression: WITH node.property as alias
                    _ => { /* Analyze expression */ }
                }
            }
            // Alias not in WITH - doesn't propagate upstream (scope boundary!)
        }
        
        upstream_reqs
    }
}
```

---

## Question 2: Relationship with Existing Property Resolvers

### Existing Components Analysis

We have **TWO separate property resolution systems**:

#### 1. `translator/property_resolver.rs`
**Purpose**: Graph-to-SQL **schema mapping** (early in pipeline)
- Resolves **Cypher property names** → **ClickHouse column names**
- Example: `user.name` → `users_table.full_name`
- Handles denormalized patterns (OriginCity vs DestCity)
- Handles polymorphic types (type discriminators)
- **Phase**: During initial plan building
- **Output**: Correct column references in LogicalExpr

#### 2. `analyzer/projected_columns_resolver.rs`
**Purpose**: Pre-compute **available properties** for GraphNodes
- Populates `GraphNode.projected_columns` field
- Eliminates renderer's need to traverse plan tree
- **Phase**: During analyzer pipeline
- **Output**: Cached property lists on GraphNode

```rust
// What it does:
Input:  GraphNode { 
    alias: "p", 
    projected_columns: None,  // ❌ Unknown
}

Output: GraphNode { 
    alias: "p", 
    projected_columns: Some([
        ("firstName", "p.first_name"),  // ✅ Pre-computed
        ("age", "p.age"),
        ("email", "p.email_address")
    ])
}
```

### New Component: `PropertyRequirementsAnalyzer`

**Purpose**: Determine **which properties are actually needed**
- **Phase**: During analyzer pipeline (AFTER projected_columns_resolver)
- **Output**: PropertyRequirements stored in PlanCtx

```rust
// What it adds:
PropertyRequirements {
    required_properties: {
        "p": {"firstName", "id"}  // Only 2 of 3 properties needed!
    }
}
```

---

## Can We Consolidate? Analysis

### Option A: Three Separate Passes (Recommended ✅)

**Keep all three as separate passes with clear responsibilities:**

```
Pipeline Flow:
┌────────────────────────────────────────────────────────┐
│ 1. translator/property_resolver                        │
│    ├─> Schema mapping (Cypher → ClickHouse columns)   │
│    └─> Output: Correct column names in LogicalExpr    │
└────────────────────────────────────────────────────────┘
                      ↓
┌────────────────────────────────────────────────────────┐
│ 2. analyzer/projected_columns_resolver                 │
│    ├─> Pre-compute AVAILABLE properties               │
│    └─> Output: GraphNode.projected_columns populated  │
└────────────────────────────────────────────────────────┘
                      ↓
┌────────────────────────────────────────────────────────┐
│ 3. analyzer/property_requirements_analyzer 🆕          │
│    ├─> Determine REQUIRED properties (bottom-up)      │
│    └─> Output: PropertyRequirements in PlanCtx        │
└────────────────────────────────────────────────────────┘
                      ↓
┌────────────────────────────────────────────────────────┐
│ Renderer uses all three:                               │
│ ├─> property_resolver: knows column mappings          │
│ ├─> projected_columns: knows available properties     │
│ └─> property_requirements: knows which to SELECT      │
└────────────────────────────────────────────────────────┘
```

**Why Keep Separate?**

1. **Single Responsibility**: Each pass has one clear job
2. **Testability**: Can test each independently
3. **Maintainability**: Easy to understand and modify
4. **Composability**: Can disable property_requirements without breaking others
5. **Performance**: Each pass is lightweight (single traversal)

**Example Usage in Renderer**:
```rust
// When expanding collect(node):
fn expand_collect_to_group_array(
    alias: &str,
    plan: &LogicalPlan,
    plan_ctx: &PlanCtx,
) -> LogicalExpr {
    // Step 1: Get AVAILABLE properties (from projected_columns_resolver)
    let all_properties = plan.get_properties_with_table_alias(alias)?;
    
    // Step 2: Get REQUIRED properties (from property_requirements_analyzer)
    let requirements = plan_ctx.get_property_requirements();
    
    // Step 3: Filter available → required
    let properties_to_collect = if let Some(reqs) = requirements {
        if reqs.requires_all(alias) {
            all_properties  // Wildcard - use all
        } else if let Some(required) = reqs.get_requirements(alias) {
            all_properties
                .into_iter()
                .filter(|(prop, _)| required.contains(prop))
                .collect()
        } else {
            all_properties  // No requirements - default to all
        }
    } else {
        all_properties  // Analyzer didn't run - backward compatible
    };
    
    // Step 4: Use property_resolver's column mappings (already in LogicalExpr)
    create_group_array_tuple(alias, properties_to_collect)
}
```

---

### Option B: Consolidate into One Pass (Not Recommended ❌)

**Attempt to merge all three into single "uber-resolver":**

```rust
struct UnifiedPropertyResolver {
    // From property_resolver
    schema_mappings: HashMap<String, PropertyMapping>,
    
    // From projected_columns_resolver
    available_properties: HashMap<String, Vec<Property>>,
    
    // From property_requirements_analyzer
    required_properties: HashMap<String, HashSet<String>>,
}
```

**Why NOT to consolidate:**

1. **Conflicting Traversal Patterns**:
   - property_resolver: Forward pass during planning
   - projected_columns_resolver: Forward pass during analysis
   - property_requirements_analyzer: **Backward pass** (bottom-up)
   - **Can't do forward and backward in single pass!**

2. **Different Phases**:
   - property_resolver: Translator phase (before LogicalPlan)
   - Others: Analyzer phase (after LogicalPlan)
   - **Can't merge across pipeline boundaries**

3. **Different Outputs**:
   - property_resolver: Modifies LogicalExpr (column names)
   - projected_columns_resolver: Modifies GraphNode (cached properties)
   - property_requirements_analyzer: Modifies PlanCtx (requirements map)
   - **Three different output locations!**

4. **Complexity Explosion**:
   - Single component doing 3 jobs = 3x harder to understand
   - Bug in one aspect breaks all three
   - Harder to test, harder to debug

---

## Recommendation: Keep Separate Passes ✅

### Final Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ Parser → AST → Translator                                        │
│   └─> property_resolver: Cypher props → ClickHouse columns     │
└─────────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────────┐
│ Analyzer Pipeline                                                │
│   ├─> [Other passes...]                                         │
│   ├─> projected_columns_resolver: Cache available properties    │
│   ├─> [Type inference, CTE resolution...]                       │
│   └─> property_requirements_analyzer: Determine needed props 🆕 │
└─────────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────────┐
│ Renderer                                                         │
│   └─> Uses all three:                                           │
│      • property_resolver for schema mappings                    │
│      • projected_columns for available properties               │
│      • property_requirements for selective expansion            │
└─────────────────────────────────────────────────────────────────┘
```

### Benefits

- ✅ **Clear separation of concerns**
- ✅ **Each pass is simple and testable**
- ✅ **Can be enabled/disabled independently**
- ✅ **Easy to understand and maintain**
- ✅ **No breaking changes to existing code**
- ✅ **Backward compatible** (if requirements analyzer disabled, falls back to current behavior)

### Code Organization

```
src/query_planner/
├── translator/
│   └── property_resolver.rs          # Schema mapping (Cypher → SQL)
└── analyzer/
    ├── projected_columns_resolver.rs  # Cache available properties
    └── property_requirements_analyzer.rs 🆕  # Determine needed properties
```

---

## Updated Implementation Plan

### Phase 1: Foundation (Week 1)
- [x] Understand existing resolvers
- [x] Design PropertyRequirements as separate component
- [ ] Implement PropertyRequirements data structure
- [ ] Add to PlanCtx (separate from projected_columns)
- [ ] Unit tests

### Phase 2: Bottom-Up Analysis (Week 2)
- [ ] Implement PropertyRequirementsAnalyzer pass
- [ ] **Key**: Bottom-up traversal from RETURN to MATCH
- [ ] **Key**: Scope boundary propagation through WITH
- [ ] Handle multi-scope queries
- [ ] Integration tests with multi-scope patterns

### Phase 3: Renderer Integration (Week 3)
- [ ] Update expand_collect_to_group_array (use requirements)
- [ ] Update expand_table_alias_to_select_items (use requirements)
- [ ] Update anyLast() wrapping (use requirements)
- [ ] Ensure compatibility with existing resolvers

### Phase 4: Testing & Validation (Week 4)
- [ ] Multi-scope query tests
- [ ] Verify correct interaction with projected_columns_resolver
- [ ] Performance benchmarking
- [ ] Documentation

---

## Summary

**Question 1 Answer**: Yes, property pruning works across multiple scopes, but uses **bottom-up analysis** (RETURN→MATCH) to correctly propagate requirements through WITH boundaries.

**Question 2 Answer**: Keep all three resolvers **separate**:
- `property_resolver`: Schema mapping (translator phase)
- `projected_columns_resolver`: Cache available properties (analyzer phase)
- `property_requirements_analyzer`: Determine needed properties (analyzer phase)

They work together cooperatively, each with a single clear responsibility. Consolidation would create complexity without benefits.

---

## Next Steps

1. ✅ Review and approve separation of concerns
2. ✅ Confirm bottom-up traversal approach for multi-scope
3. [ ] Begin implementation of PropertyRequirements data structure
4. [ ] Implement bottom-up traversal in PropertyRequirementsAnalyzer
5. [ ] Add comprehensive multi-scope test cases

**Questions?** Ready to proceed with implementation?
