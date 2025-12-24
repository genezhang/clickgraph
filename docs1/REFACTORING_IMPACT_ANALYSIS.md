# Refactoring Impact Analysis

**Date**: November 24, 2025  
**Purpose**: Identify what changes and what stays the same

---

## What STAYS (No Changes Needed)

### ✅ ViewScan Structure
```rust
// src/query_planner/logical_plan/view_scan.rs
pub struct ViewScan {
    pub source_table: String,
    pub property_mapping: HashMap<String, PropertyValue>,  // ← KEEP
    pub is_denormalized: bool,                             // ← KEEP
    pub from_id: Option<String>,
    pub to_id: Option<String>,
    // ... all other fields stay
}
```
**Why**: ViewScan is the schema adapter - this is correct design.

### ✅ PropertyValue Enum
```rust
// src/graph_catalog/expression_parser.rs
pub enum PropertyValue {
    Column(String),
    Expression(String),
}

impl PropertyValue {
    pub fn to_sql(&self, table_alias: &str) -> String { /* ... */ }
}
```
**Why**: Expression support is essential, implementation is solid.

### ✅ GraphNode/GraphRel Structures
```rust
// src/query_planner/logical_plan/mod.rs
pub struct GraphNode {
    pub input: Arc<LogicalPlan>,
    pub alias: String,
    pub label: Option<String>,
    pub is_denormalized: bool,  // ← KEEP
}

pub struct GraphRel {
    pub left: Arc<LogicalPlan>,
    pub center: Arc<LogicalPlan>,
    pub right: Arc<LogicalPlan>,
    // ... all fields stay
}
```
**Why**: Graph structure representation is correct.

### ✅ GraphContext During Analysis
```rust
// src/query_planner/analyzer/graph_context.rs
pub struct GraphContext<'a> {
    pub left: GraphNodeContext<'a>,
    pub rel: GraphRelContext<'a>,
    pub right: GraphNodeContext<'a>,
}
```
**Why**: Provides necessary context during analyzer passes.

### ✅ RenderPlan Structure
```rust
// src/render_plan/mod.rs
pub struct RenderPlan {
    pub ctes: CteItems,
    pub select: SelectItems,
    pub from: FromTableItem,
    pub joins: JoinItems,
    pub filters: FilterItems,
    // ... all fields stay
}
```
**Why**: SQL structure representation is correct.

### ✅ Schema Loading
```rust
// src/graph_catalog/graph_schema.rs
// All schema loading logic stays unchanged
```
**Why**: Schema structure is solid, handles denormalized patterns correctly.

---

## What CHANGES (Modifications Needed)

### 🔧 NEW FILE: alias_resolution.rs
```rust
// src/query_planner/analyzer/alias_resolution.rs (NEW)
// ~300 lines of new code

pub struct AliasResolutionContext {
    alias_map: HashMap<String, String>,
    view_scan_map: HashMap<String, ViewScanInfo>,
}

impl AliasResolutionContext {
    pub fn build(plan: &LogicalPlan) -> Result<Self, AnalyzerError> { /* ... */ }
    pub fn resolve_alias(&self, cypher_alias: &str) -> &str { /* ... */ }
    pub fn resolve_property(&self, cypher_alias: &str, property: &str) 
        -> Result<(String, PropertyValue), AnalyzerError> { /* ... */ }
}
```
**Impact**: New component, no existing code affected.

### 🔧 MODIFY: filter_tagging.rs
```diff
// src/query_planner/analyzer/filter_tagging.rs

  pub fn tag_filters(
      plan: LogicalPlan,
      ctx: &mut PlanCtx,
+     alias_resolution: &AliasResolutionContext,  // NEW parameter
      schema: &GraphSchema,
  ) -> Result<LogicalPlan, AnalyzerError> {
      // ...
      
      match filter_expr {
          LogicalExpr::PropertyAccess { alias, property } => {
-             // OLD: Direct alias usage
-             PropertyAccess {
-                 table_alias: alias.clone(),
-                 column: property.clone(),
-             }
              
+             // NEW: Resolve through context
+             let (sql_alias, property_value) = 
+                 alias_resolution.resolve_property(&alias, &property)?;
+             
+             PropertyAccess {
+                 table_alias: sql_alias,
+                 column: property_value.column_name(),
+             }
          }
      }
  }
```
**Impact**: ~20 lines modified in one function.

### 🔧 MODIFY: projection_tagging.rs (if exists)
Similar changes to `filter_tagging.rs` - resolve aliases before creating projections.

**Impact**: ~15 lines modified.

### 🔧 MODIFY: Analyzer orchestration
```diff
// src/query_planner/analyzer/mod.rs or analyzer_pass.rs

  pub fn analyze(
      plan: LogicalPlan,
      ctx: &mut PlanCtx,
      schema: &GraphSchema,
  ) -> Result<LogicalPlan, AnalyzerError> {
      let plan = schema_inference(plan, ctx, schema)?;
      
+     // NEW: Build alias resolution context
+     let alias_resolution = AliasResolutionContext::build(&plan)?;
+     ctx.set_alias_resolution(alias_resolution);
      
-     let plan = filter_tagging(plan, ctx, schema)?;
+     let plan = filter_tagging(plan, ctx, ctx.alias_resolution(), schema)?;
      
      // ... rest of passes
  }
```
**Impact**: ~5 lines added.

### 🔧 MODIFY: PlanCtx
```diff
// src/query_planner/plan_ctx/mod.rs

+ use super::analyzer::alias_resolution::AliasResolutionContext;

  pub struct PlanCtx {
      pub table_ctx: HashMap<String, TableCtx>,
      pub optional_aliases: HashSet<String>,
+     alias_resolution: Option<AliasResolutionContext>,
  }
  
  impl PlanCtx {
+     pub fn set_alias_resolution(&mut self, ctx: AliasResolutionContext) {
+         self.alias_resolution = Some(ctx);
+     }
+     
+     pub fn alias_resolution(&self) -> &AliasResolutionContext {
+         self.alias_resolution.as_ref().expect("AliasResolutionContext not built")
+     }
  }
```
**Impact**: ~15 lines added.

---

## What MIGHT NEED ADJUSTMENT (Review Required)

### ⚠️ CTE Generation for Denormalized Nodes
```rust
// src/render_plan/plan_builder.rs or cte_generation.rs

// Current: Generates CTEs for all nodes
// After: Should skip CTEs for denormalized nodes

fn generate_node_cte(node: &GraphNode) -> Option<Cte> {
    if node.is_denormalized {
        return None;  // Skip - properties accessed via edge table
    }
    // ... generate CTE
}
```
**Impact**: Logic may already exist, need to verify it's triggered correctly.

### ⚠️ JOIN Generation for Denormalized Patterns
```rust
// src/render_plan/plan_builder.rs

// Current: May generate JOINs between node and edge
// After: Should skip JOINs when node is denormalized

fn generate_joins(graph_rel: &GraphRel) -> Vec<Join> {
    let mut joins = vec![];
    
    // Check if left node needs JOIN
    if let LogicalPlan::GraphNode(left) = &*graph_rel.left {
        if !left.is_denormalized {
            // Generate JOIN only if not denormalized
            joins.push(/* ... */);
        }
    }
    
    // Same for right node
    // ...
}
```
**Impact**: Logic may already exist, ensure it uses correct aliases.

### ⚠️ Property Access in RenderExpr
```rust
// src/render_plan/render_expr.rs

impl RenderExpr {
    pub fn to_sql(&self) -> String {
        match self {
            RenderExpr::PropertyAccessExp(prop_access) => {
                // Should already use prop_access.table_alias
                // If alias resolution worked correctly, this should be "f" not "a"
                format!("{}.{}", prop_access.table_alias, prop_access.column)
            }
        }
    }
}
```
**Impact**: No changes needed if filter_tagging resolves aliases correctly.

---

## Code Deletion Candidates (AFTER Verification)

### 🗑️ REMOVE: get_denormalized_aliases() (Maybe)
```rust
// src/render_plan/plan_builder_helpers.rs:278

pub(super) fn get_denormalized_aliases(plan: &LogicalPlan) -> HashSet<String> {
    // This function walks the plan tree to find denormalized nodes
    // May become unnecessary if AliasResolutionContext handles this
}
```
**Decision**: Keep for now, remove after confirming not needed.

### 🗑️ REMOVE: Late alias remapping logic (Maybe)
```rust
// src/render_plan/plan_builder.rs (various locations)

// Any logic that tries to remap aliases during RenderPlan building
// Should become unnecessary if resolution happens early
```
**Decision**: Identify and remove after testing.

---

## Testing Strategy

### Unit Tests (New)
```
tests/query_planner/analyzer/test_alias_resolution.rs

1. test_build_context_simple_pattern()
   - (a)-[f]->(b) pattern
   - Verify alias_map correct

2. test_build_context_denormalized()
   - Denormalized Airport nodes
   - Verify "a" → "f", "b" → "f"

3. test_resolve_property_column()
   - PropertyValue::Column case
   - Verify correct mapping

4. test_resolve_property_expression()
   - PropertyValue::Expression case
   - Verify expression preserved

5. test_mixed_denormalized_and_normal()
   - Some nodes denormalized, some not
   - Verify independent resolution
```

### Integration Tests (Modified)
```
tests/integration/test_denormalized_queries.py (NEW or UPDATE)

1. test_lax_query()
   - MATCH (a:Airport)-[f:Flight]->(b:Airport) WHERE a.origin = 'LAX'
   - Assert valid SQL generated
   - Assert query executes successfully

2. test_denormalized_return()
   - RETURN a.code, f.carrier, b.city
   - Assert correct column selection

3. test_mixed_filters()
   - WHERE a.city = 'LA' AND f.distance > 1000 AND b.state = 'NY'
   - Assert all filters use correct aliases

4. test_expression_property()
   - Property mapped to expression
   - Assert expression evaluated with correct alias

5. test_existing_tests_still_pass()
   - Run full test suite
   - Verify no regressions
```

### Manual Testing
```bash
# 1. Start server with denormalized schema
./scripts/server/start_server_background.sh \
    -c schemas/examples/ontime_denormalized.yaml

# 2. Test LAX query
curl -X POST http://localhost:8080/query \
    -H "Content-Type: application/json" \
    -d '{"query": "MATCH (a:Airport)-[f:Flight]->(b:Airport) WHERE a.origin = '\''LAX'\'' RETURN a.code, b.code LIMIT 5"}'

# 3. Check generated SQL
curl -X POST http://localhost:8080/sql-generation \
    -H "Content-Type: application/json" \
    -d '{"cypher_query": "MATCH (a:Airport)-[f:Flight]->(b:Airport) WHERE a.origin = '\''LAX'\'' RETURN a.code"}'

# Expected: SELECT f.Origin AS code FROM flights AS f WHERE f.Origin = 'LAX'
```

---

## Rollback Plan

If implementation fails or causes regressions:

### Step 1: Feature Flag (Optional)
```rust
// Add feature flag to enable/disable new behavior
if ctx.use_alias_resolution {
    // New path with AliasResolutionContext
} else {
    // Old path (current behavior)
}
```

### Step 2: Revert Commits
```bash
# If feature flag not viable, revert changes
git revert <commit-range>
```

### Step 3: Document Issues
- What worked
- What didn't work
- What needs different approach
- Lessons learned

---

## Migration Timeline

### Day 1 (6 hours)
- ✅ Morning: Implement `AliasResolutionContext` core (~3 hours)
- ✅ Afternoon: Write unit tests for context (~3 hours)

### Day 2 (6 hours)
- ✅ Morning: Modify `filter_tagging.rs` (~2 hours)
- ✅ Afternoon: Integrate with analyzer pipeline (~2 hours)
- ✅ Evening: Write integration tests (~2 hours)

### Day 3 (6 hours)
- ✅ Morning: Test LAX query and variants (~2 hours)
- ✅ Afternoon: Fix any issues found (~3 hours)
- ✅ Evening: Run full test suite (~1 hour)

### Day 4 (2 hours)
- ✅ Morning: Documentation and cleanup (~2 hours)

**Total**: 20 hours (2.5 days)

---

## Success Criteria

### Must Have ✅
1. LAX query generates valid SQL
2. All 5 test cases pass (from DENORMALIZED_EDGE_FIX_PLAN.md)
3. No regressions in existing test suite
4. Expression-based properties work

### Nice to Have 🎯
1. Performance metrics (alias resolution overhead)
2. Feature flag for safe rollout
3. Comprehensive documentation
4. Debug logging for troubleshooting

### Not in Scope ❌
1. Removing `is_denormalized` flags (keep for now)
2. Optimizing CTE generation (separate effort)
3. Polymorphic edge improvements (separate feature)
4. Multi-hop denormalized patterns (future work)

---

## Key Takeaways

**Minimal Refactoring Required**:
- ✅ Add 1 new file (~300 lines)
- ✅ Modify 3-4 existing files (~50 lines total)
- ✅ No structural changes to ViewScan, LogicalPlan, or RenderPlan
- ✅ Respects existing design patterns

**Risk Level: LOW**
- Changes are localized to analyzer phase
- Existing structures remain intact
- Easy to revert if needed
- Feature flag possible for staged rollout

**Value: HIGH**
- Fixes broken denormalized edge queries
- Establishes clear graph→SQL boundary
- Makes system more maintainable
- Template for future features
