# Graph-to-SQL Translation Architecture Analysis

**Date**: November 24, 2025  
**Status**: Architecture Review & Refactoring Proposal

## Executive Summary

The current architecture has **conceptual boundary violations** where graph concepts (nodes, edges) leak into the SQL generation layer (RenderPlan). This creates ambiguity in alias mapping, especially for denormalized edge tables where node aliases in Cypher must map to the edge table alias in SQL.

## The Problem: Denormalized Edge Case

### Example Query
```cypher
MATCH (a:Airport)-[f:Flight]->(b:Airport) 
WHERE a.origin = 'LAX'
```

### Expected Behavior (Denormalized Schema)
When Airport nodes are **denormalized** (stored on the `flights` table):
- **No JOIN needed** - single table scan
- **Node aliases `a` and `b` map to edge alias `f`**
- Filter becomes: `f.Origin = 'LAX'` (NOT `a.origin = 'LAX'`)

### Current Problem
The system doesn't cleanly translate graph aliases (`a`, `b`) to SQL table aliases (`f`) because:
1. Graph concepts persist into `RenderPlan`
2. Alias mapping logic is scattered across multiple layers
3. Denormalized detection happens too late (in `render_plan`)

---

## Current Architecture: 5-Layer Translation

```
┌─────────────────────────────────────────────────────────────┐
│ 1. PARSING LAYER (open_cypher_parser)                      │
│    Input: Cypher String                                     │
│    Output: OpenCypherQueryAst (Pure Cypher Concepts)       │
│    - Nodes: (a:Airport)                                     │
│    - Edges: [f:Flight]                                      │
│    - Graph properties: a.origin                             │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ 2. LOGICAL PLANNING (query_planner/logical_plan)           │
│    Input: OpenCypherQueryAst + GraphSchema                 │
│    Output: LogicalPlan (Still Graph-Centric)               │
│    - ViewScan nodes (one per Cypher alias)                 │
│    - GraphJoins (graph relationships)                      │
│    - LogicalExpr (graph property access)                   │
│    ⚠️  ISSUE: Still thinks in nodes/edges                   │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ 3. ANALYZER (query_planner/analyzer)                       │
│    - Schema inference (map Cypher to tables)               │
│    - Filter tagging (WHERE clause analysis)                │
│    - Join inference (graph traversal → SQL joins)          │
│    ⚠️  ISSUE: Partially resolves to tables, but maintains   │
│              graph aliases (a, b, f)                        │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ 4. RENDER PLAN (render_plan) - SQL PREPARATION             │
│    Input: LogicalPlan                                       │
│    Output: RenderPlan (Should be Pure SQL)                 │
│    ⚠️  CURRENT STATE: Mixes graph and SQL concepts          │
│    - ViewTableRef (has source_table)                       │
│    - Join (SQL joins)                                       │
│    - PropertyAccess(table_alias, column)                   │
│    - BUT: Still checks is_denormalized flag                │
│    - BUT: Still references node/edge concepts in helpers   │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ 5. SQL GENERATION (to_sql methods)                         │
│    Input: RenderPlan                                        │
│    Output: ClickHouse SQL String                           │
│    - Pure string generation                                 │
└─────────────────────────────────────────────────────────────┘
```

---

## Root Cause: Unclear Boundary Between Graph and SQL

### Where Graph Concepts Should End

**Ideal Boundary**: LogicalPlan → RenderPlan transition should **completely resolve** graph concepts to SQL.

**Current Reality**: Graph concepts leak into RenderPlan:

1. **`ViewScan.is_denormalized` flag** (line 47, `view_scan.rs`)
   - Graph concept: "Is this node denormalized?"
   - Should be resolved earlier

2. **`get_denormalized_aliases()` in `plan_builder_helpers.rs`** (line 278)
   - Walks LogicalPlan tree to find denormalized nodes
   - Happens during SQL generation (too late!)

3. **Alias mapping logic scattered**:
   - `filter_tagging.rs` - tags filters with table aliases
   - `plan_builder_helpers.rs` - remaps aliases for denormalized cases
   - `render_expr.rs` - PropertyAccess has `table_alias`

### The Consequence: LAX Filter Bug

```rust
// In filter_tagging.rs (line ~500)
// Filter: a.origin = 'LAX'
// Tagged as: PropertyAccess { table_alias: "a", column: "origin" }

// Later in plan_builder_helpers.rs
// Detects 'a' is denormalized, tries to remap to 'f'
// BUT: The remapping logic is fragile and misses cases
```

---

## Proposed Refactoring: Clear Separation of Concerns

### Phase 1: Enhance Analyzer to Resolve Aliases Early

**Goal**: By the end of the analyzer phase, all graph aliases should be mapped to SQL table aliases.

```rust
// New: AliasResolutionContext in analyzer/
pub struct AliasResolutionContext {
    /// Maps Cypher alias → SQL table alias
    /// Example: "a" → "f" (if denormalized)
    ///          "b" → "f" (if denormalized)
    ///          "f" → "f" (edge itself)
    cypher_to_sql_aliases: HashMap<String, String>,
    
    /// Track which aliases refer to the same physical table
    /// Example: ["a", "b", "f"] → "flights_table"
    alias_groups: HashMap<String, Vec<String>>,
}
```

**Implementation**:
1. Add new analyzer pass: `alias_resolution.rs`
2. Run **after** `schema_inference` and **before** `filter_tagging`
3. For each pattern `(a)-[f]->(b)`:
   - Check if `a` is denormalized (via schema)
   - Check if `b` is denormalized (via schema)
   - Build mapping: `a → f`, `b → f` if denormalized
4. Store mapping in `PlanCtx` for downstream use

### Phase 2: Clean Up LogicalPlan → RenderPlan Transition

**Goal**: `RenderPlan` should have zero graph concepts.

**Changes**:

1. **Remove from `ViewScan`**:
   ```rust
   // DELETE:
   pub is_denormalized: bool,  // Graph concept
   ```

2. **Add to `PlanCtx`** (query context):
   ```rust
   // ADD:
   pub alias_resolution: AliasResolutionContext,
   ```

3. **Modify `filter_tagging.rs`**:
   ```rust
   // OLD:
   PropertyAccess { table_alias: "a", column: "origin" }
   
   // NEW: Use resolved alias
   let resolved_alias = ctx.alias_resolution.resolve("a"); // → "f"
   PropertyAccess { table_alias: "f", column: "Origin" }
   ```

4. **Remove `get_denormalized_aliases()` from `plan_builder_helpers.rs`**:
   - No longer needed; aliases pre-resolved

### Phase 3: Property Mapping Integration

**Goal**: Apply correct property mappings during alias resolution.

For denormalized nodes, properties depend on position:
```yaml
# From ontime_denormalized.yaml
from_node_properties:
  code: Origin        # When Airport is source
to_node_properties:
  code: Dest          # When Airport is destination
```

**Implementation**:
```rust
// In alias_resolution.rs
fn resolve_property(
    cypher_alias: &str,
    property: &str,
    schema: &GraphSchema,
    pattern_position: NodePosition,  // From/To
) -> (String, String) {  // (table_alias, column_name)
    let sql_alias = resolve_alias(cypher_alias);
    
    if is_denormalized(cypher_alias) {
        let mapping = match pattern_position {
            NodePosition::From => schema.get_from_node_properties(label),
            NodePosition::To => schema.get_to_node_properties(label),
        };
        let column = mapping.get(property).unwrap();
        (sql_alias, column)  // ("f", "Origin")
    } else {
        (cypher_alias, property)  // ("a", "code")
    }
}
```

---

## Detailed Component Analysis

### Current Components and Their Responsibilities

| Component | Current Role | Should It Know About Graphs? | Should It Know About SQL? |
|-----------|--------------|------------------------------|---------------------------|
| `open_cypher_parser` | Parse Cypher to AST | ✅ YES (only layer that should) | ❌ NO |
| `query_planner/logical_plan` | Build logical query plan | ✅ YES (but abstract) | ❌ NO |
| `query_planner/analyzer` | Validate & optimize plan | ⚠️  TRANSITIONAL (resolve to SQL) | ⚠️  TRANSITIONAL |
| `render_plan` | Prepare SQL structures | ❌ NO | ✅ YES (only SQL) |
| `to_sql` methods | Generate SQL strings | ❌ NO | ✅ YES |

### The Transitional Layer: Analyzer

The **analyzer** is the critical layer where translation happens. It should:

1. **Input**: LogicalPlan with graph concepts
2. **Process**: 
   - Map graph labels → SQL tables
   - Map graph properties → SQL columns
   - Map graph aliases → SQL table aliases
   - Resolve denormalized patterns (no JOINs)
3. **Output**: LogicalPlan with **resolved references**

**Key insight**: LogicalPlan nodes don't change structure, but their **metadata** becomes SQL-aware.

---

## Implementation Plan

### Step 1: Add AliasResolutionContext (2-3 hours)

```rust
// New file: src/query_planner/analyzer/alias_resolution.rs

pub struct AliasResolutionContext {
    mappings: HashMap<String, ResolvedAlias>,
}

pub struct ResolvedAlias {
    sql_table_alias: String,
    sql_table_name: String,
    is_denormalized: bool,
    position_in_pattern: Option<NodePosition>,
}

impl AliasResolutionContext {
    pub fn resolve(&self, cypher_alias: &str) -> &str {
        &self.mappings.get(cypher_alias).unwrap().sql_table_alias
    }
    
    pub fn resolve_property(
        &self,
        cypher_alias: &str,
        property: &str,
        schema: &GraphSchema,
    ) -> (String, String) {
        let resolved = &self.mappings[cypher_alias];
        
        if resolved.is_denormalized {
            let column = schema.get_denormalized_column(
                cypher_alias,
                property,
                resolved.position_in_pattern,
            );
            (resolved.sql_table_alias.clone(), column)
        } else {
            (resolved.sql_table_alias.clone(), property.to_string())
        }
    }
}
```

### Step 2: Implement Alias Resolution Pass (4-6 hours)

```rust
// In src/query_planner/analyzer/alias_resolution.rs

pub fn resolve_aliases(
    plan: LogicalPlan,
    schema: &GraphSchema,
) -> Result<(LogicalPlan, AliasResolutionContext), AnalyzerError> {
    let mut ctx = AliasResolutionContext::new();
    
    // Walk the plan tree
    match &plan {
        LogicalPlan::GraphJoins(joins) => {
            // For each (a)-[f]->(b) pattern:
            let left_node = extract_node_info(&joins.left);
            let center_edge = extract_edge_info(&joins.center);
            let right_node = extract_node_info(&joins.right);
            
            // Check if nodes are denormalized
            let left_denorm = schema.is_node_denormalized(&left_node.label);
            let right_denorm = schema.is_node_denormalized(&right_node.label);
            
            if left_denorm {
                // Map left node alias to edge alias
                ctx.add_mapping(
                    left_node.alias,
                    ResolvedAlias {
                        sql_table_alias: center_edge.alias.clone(),
                        sql_table_name: center_edge.table.clone(),
                        is_denormalized: true,
                        position_in_pattern: Some(NodePosition::From),
                    }
                );
            } else {
                // Normal case: node has its own table
                ctx.add_mapping(left_node.alias, ...);
            }
            
            // Same for right node
            if right_denorm {
                ctx.add_mapping(right_node.alias, ...);
            }
            
            // Edge maps to itself
            ctx.add_mapping(center_edge.alias, ...);
        }
        _ => { /* Handle other plan types */ }
    }
    
    Ok((plan, ctx))
}
```

### Step 3: Integrate with Filter Tagging (2-3 hours)

```rust
// Modify src/query_planner/analyzer/filter_tagging.rs

pub fn tag_filters(
    plan: LogicalPlan,
    ctx: &mut PlanCtx,
    alias_resolution: &AliasResolutionContext,  // NEW parameter
    schema: &GraphSchema,
) -> Result<LogicalPlan, AnalyzerError> {
    // When tagging a filter like: a.origin = 'LAX'
    match filter_expr {
        LogicalExpr::PropertyAccess { alias, property } => {
            // OLD:
            // PropertyAccess { table_alias: "a", column: "origin" }
            
            // NEW:
            let (sql_alias, sql_column) = alias_resolution.resolve_property(
                alias,
                property,
                schema,
            );
            PropertyAccess {
                table_alias: sql_alias,   // "f"
                column: sql_column,        // "Origin"
            }
        }
    }
}
```

### Step 4: Remove Graph Concepts from RenderPlan (1-2 hours)

```rust
// 1. Remove is_denormalized from ViewScan
// 2. Delete get_denormalized_aliases() from plan_builder_helpers.rs
// 3. Remove denormalized-specific logic from render_plan/plan_builder.rs
```

### Step 5: Update Tests (3-4 hours)

Update all tests that check for denormalized behavior:
- `tests/integration/fixtures/schemas/ontime_denormalized.yaml`
- `render_plan/tests/where_clause_filter_tests.rs`
- Any test that checks `is_denormalized` flag

---

## Benefits of This Refactoring

### 1. **Clear Conceptual Boundaries**
- Parser → Analyzer: Graph concepts
- Analyzer → RenderPlan: SQL concepts
- No mixing

### 2. **Easier Debugging**
- Alias mappings in one place (`AliasResolutionContext`)
- Print context to see all mappings at once

### 3. **Simpler RenderPlan**
- No need to check `is_denormalized`
- All aliases already resolved
- Just generate SQL from structures

### 4. **Extensibility**
- New graph patterns (e.g., polymorphic edges) can extend `AliasResolutionContext`
- No need to modify RenderPlan

### 5. **Performance**
- Resolve once in analyzer
- No repeated tree walks in render_plan

---

## Migration Strategy

### Phase A: Add Without Breaking (1 week)

1. Implement `AliasResolutionContext`
2. Add as **optional** field in `PlanCtx`
3. New code path uses it, old code path unchanged

### Phase B: Parallel Testing (1 week)

1. Run tests with **both** code paths
2. Compare results
3. Fix discrepancies

### Phase C: Cut Over (2 days)

1. Remove old code path
2. Make `AliasResolutionContext` required
3. Delete `is_denormalized` from `ViewScan`

### Phase D: Cleanup (2 days)

1. Remove helper functions like `get_denormalized_aliases()`
2. Update documentation
3. Archive design docs

---

## Open Questions

### Q1: Should LogicalPlan structure change?
**Answer**: No, keep LogicalPlan as-is. Only add metadata (PlanCtx).

### Q2: What about variable-length paths with denormalized nodes?
**Answer**: AliasResolutionContext handles this:
- CTE generation still uses graph concepts internally
- But final column references use resolved aliases

### Q3: How to handle mixed scenarios (some nodes denorm, some not)?
**Answer**: Each alias has independent `ResolvedAlias` entry. System treats each case independently.

### Q4: Performance impact of alias resolution?
**Answer**: Minimal - single pass over LogicalPlan tree, results cached in context.

---

## Next Steps

1. **Review this document** - Validate architectural approach
2. **Prototype `AliasResolutionContext`** - Core data structure
3. **Implement alias resolution pass** - Key logic
4. **Test with LAX query** - Validate fix works
5. **Expand to all denormalized tests** - Full validation
6. **Refactor RenderPlan** - Remove graph concepts

---

## References

- **Original Issue**: Filter alias mapping for denormalized ontime data
- **Key Files**:
  - `src/query_planner/analyzer/filter_tagging.rs`
  - `src/query_planner/logical_plan/view_scan.rs`
  - `src/render_plan/plan_builder_helpers.rs`
  - `schemas/examples/ontime_denormalized.yaml`
- **Test Case**: `MATCH (a:Airport)-[f:Flight]->(b:Airport) WHERE a.origin = 'LAX'`
