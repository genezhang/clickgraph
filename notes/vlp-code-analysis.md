# VLP (Variable-Length Path) Code Path Analysis

## Problem Statement

The VLP code is fragile because it has multiple decision points that handle different schema types differently, and fixes in one place break another. This document analyzes the code flow and proposes a consolidated approach.

## Current VLP Decision Points

### 1. Schema Types

| Schema Type | Nodes | Edges | Example |
|-------------|-------|-------|---------|
| **Normal** | Separate table | Separate table | users + follows |
| **Polymorphic** | Separate table | Single table with type_column | users + interactions (with interaction_type) |
| **Denormalized** | Virtual (embedded in edge) | Has node properties | flights (Origin/Dest are columns) |

### 2. VLP Pattern Types

| Pattern | CTE Needed? | JOIN Strategy |
|---------|-------------|---------------|
| `*` (unbounded) | Yes | Recursive CTE |
| `*1..5` (range) | Yes | Recursive CTE with depth limit |
| `*2` (exact) | No | Inline JOINs (r1, r2) |
| `*3` (exact) | No | Inline JOINs (r1, r2, r3) |

### 3. Current Code Flow for Fixed-Length VLP (*2, *3)

```
build_simple_relationship_render_plan()
├── extract_from()           → Decides FROM table/alias
├── extract_joins()          → Generates JOIN clauses  
├── extract_filters()        → WHERE clause including cycle prevention
└── extract_select_items()   → SELECT columns
```

### 4. Issues with Current Implementation

#### Issue 1: FROM Clause Logic (lines ~3847-3905)
- For **normal**: Uses start node (e.g., `FROM users AS a`)
- For **denormalized**: Was incorrectly trying to use start node but denormalized nodes don't have separate tables

#### Issue 2: JOIN Generation in expand_fixed_length_joins()
- Assumes start node is in FROM clause with `start_alias`
- First JOIN: `r1 ON start_alias.id = r1.from_id`
- But for denormalized, there's no `start_alias` table - only the edge table exists

#### Issue 3: Property Resolution
- Normal: `a.user_id` → `users.user_id`
- Denormalized: `origin.code` should → `flights.Origin` (but needs to know position: from_node vs to_node)
- Polymorphic: `a.name` → `users.username` (normal property mapping)

## Proposed Consolidation

### Approach: Schema-Aware VLP Handler

Create a single entry point that dispatches to schema-specific handlers:

```rust
pub fn build_vlp_render_plan(
    &self,
    spec: &VariableLengthSpec,
    graph_rel: &GraphRel,
    schema_type: VlpSchemaType,
) -> Result<RenderPlan, RenderBuildError> {
    match schema_type {
        VlpSchemaType::Normal => self.build_normal_vlp(spec, graph_rel),
        VlpSchemaType::Polymorphic => self.build_polymorphic_vlp(spec, graph_rel),
        VlpSchemaType::Denormalized => self.build_denormalized_vlp(spec, graph_rel),
    }
}
```

### Schema Type Detection

```rust
pub enum VlpSchemaType {
    Normal,           // Separate node and edge tables
    Polymorphic,      // Edge table has type_column  
    Denormalized,     // Nodes embedded in edge (from_node_properties)
}

pub fn detect_vlp_schema_type(graph_rel: &GraphRel) -> VlpSchemaType {
    let left_is_denorm = is_node_denormalized(&graph_rel.left);
    let right_is_denorm = is_node_denormalized(&graph_rel.right);
    
    if left_is_denorm && right_is_denorm {
        return VlpSchemaType::Denormalized;
    }
    
    // Check for polymorphic edge
    if let LogicalPlan::ViewScan(scan) = graph_rel.center.as_ref() {
        if scan.type_column.is_some() {
            return VlpSchemaType::Polymorphic;
        }
    }
    
    VlpSchemaType::Normal
}
```

### Handler Implementations

#### Normal VLP Handler
```rust
fn build_normal_vlp(&self, spec: &VariableLengthSpec, gr: &GraphRel) -> Result<RenderPlan> {
    // FROM: start_node_table AS start_alias
    // JOINs: r1, r2, ..., rN, end_alias
    // Current implementation (works)
}
```

#### Polymorphic VLP Handler  
```rust
fn build_polymorphic_vlp(&self, spec: &VariableLengthSpec, gr: &GraphRel) -> Result<RenderPlan> {
    // Same as normal - node tables exist separately
    // Edge table just has type filtering
    // Current implementation should work (verified working)
}
```

#### Denormalized VLP Handler
```rust
fn build_denormalized_vlp(&self, spec: &VariableLengthSpec, gr: &GraphRel) -> Result<RenderPlan> {
    // DIFFERENT APPROACH:
    // FROM: edge_table AS r1 (first hop IS the from table)
    // JOINs: r2 ON r1.to_id = r2.from_id, r3 ON r2.to_id = r3.from_id, ...
    // No separate node JOINs - properties come from r1 (start) and rN (end)
    
    // Property mapping:
    // start_alias.prop → r1.from_node_properties[prop]
    // end_alias.prop → rN.to_node_properties[prop]
}
```

## Implementation Plan

1. **Phase 1**: Create `VlpSchemaType` enum and detection function
2. **Phase 2**: Refactor `expand_fixed_length_joins` to take schema type
3. **Phase 3**: Create schema-specific FROM clause logic
4. **Phase 4**: Create schema-specific property resolution
5. **Phase 5**: Test all combinations

## Test Matrix

| Schema | *1 | *2 | *3 | Range | Unbounded |
|--------|----|----|----|----- |-----------|
| Normal | ✓  | ✓  | ✓  | TODO | TODO |
| Polymorphic | ✓ | ✓ | ✓ | TODO | TODO |
| Denormalized | ? | ✗ | ✗ | TODO | TODO |

## Files to Modify

1. `src/render_plan/cte_extraction.rs` - Add VlpSchemaType, refactor expand_fixed_length_joins
2. `src/render_plan/plan_builder.rs` - Consolidate VLP handling
3. `src/render_plan/filter_pipeline.rs` - Schema-aware property resolution
