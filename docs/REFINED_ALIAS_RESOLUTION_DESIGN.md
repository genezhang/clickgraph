# Refined Architecture: AliasResolutionContext with ViewScan Integration

**Date**: November 24, 2025  
**Status**: Refined Proposal - Respects Existing Design

---

## Key Insights from Existing Architecture

### 1. **ViewScan is NOT Just a Table Scan**

ViewScan is the **graph-to-relational adapter layer** that carries:
- ✅ `property_mapping: HashMap<String, PropertyValue>` - Handles column AND expression mappings
- ✅ `is_denormalized: bool` - Marks nodes stored on edge tables
- ✅ `from_id`, `to_id` - Relationship connection info
- ✅ `source_table` - Physical table name
- ✅ Schema metadata for later stages

**Design Principle**: ViewScan preserves graph schema information through the pipeline so downstream stages can access it.

### 2. **PropertyValue is More Than a Column**

```rust
pub enum PropertyValue {
    Column(String),           // Simple: user_id
    Expression(String),       // Complex: concat(first_name, ' ', last_name)
}

impl PropertyValue {
    pub fn to_sql(&self, table_alias: &str) -> String {
        // Handles both cases, applies table prefix
    }
}
```

**Design Principle**: Property mappings can be expressions, not just column renames.

### 3. **GraphNode/GraphRel in LogicalPlan**

```rust
pub struct GraphNode {
    pub input: Arc<LogicalPlan>,
    pub alias: String,
    pub label: Option<String>,      // For denormalized property lookup
    pub is_denormalized: bool,      // Set by optimizer
}

pub struct GraphRel {
    pub left: Arc<LogicalPlan>,     // Left node
    pub center: Arc<LogicalPlan>,   // Edge itself
    pub right: Arc<LogicalPlan>,    // Right node
    // ...
}
```

**Design Principle**: LogicalPlan represents graph structure; metadata helps later stages.

### 4. **GraphContext During Analysis**

```rust
pub struct GraphContext<'a> {
    pub left: GraphNodeContext<'a>,
    pub rel: GraphRelContext<'a>,
    pub right: GraphNodeContext<'a>,
    pub schema: &'a GraphSchema,
}

pub struct GraphNodeContext<'a> {
    pub alias: &'a String,
    pub table_ctx: &'a TableCtx,
    pub label: String,
    pub schema: &'a NodeSchema,   // Full schema access
}
```

**Design Principle**: Analyzer has full schema context when processing graph patterns.

---

## The Problem (Refined Understanding)

### Current Flow for Denormalized Case

```
MATCH (a:Airport)-[f:Flight]->(b:Airport) WHERE a.origin = 'LAX'
                    ↓
┌─────────────────────────────────────────────────────────────────┐
│ LogicalPlan Created                                             │
│                                                                 │
│ GraphNode(a):                                                   │
│   - alias: "a"                                                  │
│   - label: "Airport"                                            │
│   - is_denormalized: true  ← Set by schema_inference            │
│   - input: ViewScan {                                           │
│       source_table: "flights",                                  │
│       property_mapping: {                                       │
│         "code": Column("Origin"),  ← from_node_properties       │
│         "city": Column("OriginCityName"),                       │
│       },                                                        │
│       is_denormalized: true                                     │
│     }                                                            │
│                                                                 │
│ GraphRel(f):                                                    │
│   - alias: "f"                                                  │
│   - center: ViewScan {                                          │
│       source_table: "flights",                                  │
│       property_mapping: {                                       │
│         "distance": Column("Distance"),                         │
│         "carrier": Column("Carrier"),                           │
│       }                                                         │
│     }                                                            │
│                                                                 │
│ GraphNode(b):                                                   │
│   - alias: "b"                                                  │
│   - is_denormalized: true                                       │
│   - input: ViewScan {                                           │
│       source_table: "flights",                                  │
│       property_mapping: {                                       │
│         "code": Column("Dest"),    ← to_node_properties         │
│         "city": Column("DestCityName"),                         │
│       }                                                         │
│     }                                                            │
└─────────────────────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────────────────────┐
│ filter_tagging: WHERE a.origin = 'LAX'                          │
│                                                                 │
│ ❌ PROBLEM: Creates PropertyAccess("a", "origin")               │
│                                                                 │
│ Should create: PropertyAccess("f", "Origin")                   │
│ Because:                                                        │
│   - Node "a" is denormalized → maps to edge "f"                │
│   - Property "origin" → "Origin" (from ViewScan mapping)        │
│   - But filter_tagging doesn't know this!                      │
└─────────────────────────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────────────────────────┐
│ RenderPlan: Tries to use PropertyAccess("a", "origin")          │
│                                                                 │
│ ❌ PROBLEM: In SQL, only "f" exists as table alias              │
│   SELECT * FROM flights AS f WHERE a.origin = 'LAX'            │
│                                     ↑                           │
│                              Undefined alias!                   │
└─────────────────────────────────────────────────────────────────┘
```

### Why Current Approach Fails

**Information is available but not used at the right time**:

1. ✅ ViewScan for `"a"` has `property_mapping` with `"origin" → "Origin"`
2. ✅ GraphNode for `"a"` has `is_denormalized: true`
3. ✅ ViewScan for `"a"` has `source_table: "flights"` (same as edge)
4. ❌ **BUT**: `filter_tagging` doesn't consult this information!

**Current filter_tagging logic**:
```rust
// Simplified current behavior
PropertyAccess { alias: "a", property: "origin" }
    ↓
// Tags with literal alias
PropertyAccess { table_alias: "a", column: "origin" }
    ↓
// ❌ Never resolves "a" → "f" mapping
// ❌ Never applies property mapping "origin" → "Origin"
```

---

## Refined Solution: AliasResolutionContext (ViewScan-Aware)

### Design Principle

**Don't replace ViewScan metadata - use it!**

ViewScan is the single source of truth for property mappings. `AliasResolutionContext` is a **lookup index** built from ViewScan data to enable efficient alias resolution during filter tagging.

### New Component: AliasResolutionContext

```rust
// src/query_planner/analyzer/alias_resolution.rs (NEW FILE)

use std::collections::HashMap;
use crate::graph_catalog::expression_parser::PropertyValue;
use crate::query_planner::logical_plan::{LogicalPlan, ViewScan, GraphNode, GraphRel};

/// Context for resolving Cypher aliases to SQL table aliases
/// Built from LogicalPlan's ViewScan and GraphNode metadata
pub struct AliasResolutionContext {
    /// Maps Cypher alias → SQL table alias
    /// Example: "a" → "f", "b" → "f", "f" → "f"
    alias_map: HashMap<String, String>,
    
    /// Maps Cypher alias → ViewScan (for property lookups)
    /// Preserves all ViewScan metadata including PropertyValue expressions
    view_scan_map: HashMap<String, ViewScanInfo>,
}

/// Extracted ViewScan information for property resolution
#[derive(Debug, Clone)]
pub struct ViewScanInfo {
    pub source_table: String,
    pub property_mapping: HashMap<String, PropertyValue>,
    pub is_denormalized: bool,
}

impl AliasResolutionContext {
    /// Create from LogicalPlan (called after schema_inference)
    pub fn build(plan: &LogicalPlan) -> Result<Self, AnalyzerError> {
        let mut alias_map = HashMap::new();
        let mut view_scan_map = HashMap::new();
        
        collect_aliases(plan, &mut alias_map, &mut view_scan_map)?;
        
        Ok(AliasResolutionContext {
            alias_map,
            view_scan_map,
        })
    }
    
    /// Resolve Cypher alias to SQL table alias
    /// Example: "a" → "f" (if denormalized)
    pub fn resolve_alias(&self, cypher_alias: &str) -> &str {
        self.alias_map.get(cypher_alias).map(|s| s.as_str()).unwrap_or(cypher_alias)
    }
    
    /// Resolve property access with PropertyValue support
    /// Returns: (SQL table alias, PropertyValue for that property)
    pub fn resolve_property(
        &self,
        cypher_alias: &str,
        property: &str,
    ) -> Result<(String, PropertyValue), AnalyzerError> {
        // 1. Resolve alias: "a" → "f"
        let sql_alias = self.resolve_alias(cypher_alias).to_string();
        
        // 2. Get ViewScan info for this alias
        let view_info = self.view_scan_map.get(cypher_alias)
            .ok_or_else(|| AnalyzerError::UnknownAlias(cypher_alias.to_string()))?;
        
        // 3. Look up property in ViewScan's property_mapping
        let property_value = view_info.property_mapping.get(property)
            .ok_or_else(|| AnalyzerError::UnknownProperty {
                alias: cypher_alias.to_string(),
                property: property.to_string(),
            })?;
        
        Ok((sql_alias, property_value.clone()))
    }
    
    /// Check if an alias is denormalized
    pub fn is_denormalized(&self, cypher_alias: &str) -> bool {
        self.view_scan_map.get(cypher_alias)
            .map(|info| info.is_denormalized)
            .unwrap_or(false)
    }
}

/// Recursively collect alias mappings from LogicalPlan
fn collect_aliases(
    plan: &LogicalPlan,
    alias_map: &mut HashMap<String, String>,
    view_scan_map: &mut HashMap<String, ViewScanInfo>,
) -> Result<(), AnalyzerError> {
    match plan {
        LogicalPlan::GraphRel(graph_rel) => {
            // Process left node
            if let LogicalPlan::GraphNode(left_node) = &*graph_rel.left {
                collect_from_graph_node(
                    left_node,
                    &graph_rel.alias,  // Edge alias (potential target)
                    alias_map,
                    view_scan_map,
                )?;
            }
            
            // Process center (edge)
            collect_aliases(&graph_rel.center, alias_map, view_scan_map)?;
            if let Some(edge_view_scan) = extract_view_scan(&graph_rel.center) {
                view_scan_map.insert(
                    graph_rel.alias.clone(),
                    ViewScanInfo {
                        source_table: edge_view_scan.source_table.clone(),
                        property_mapping: edge_view_scan.property_mapping.clone(),
                        is_denormalized: false,  // Edges themselves aren't "denormalized"
                    }
                );
                alias_map.insert(graph_rel.alias.clone(), graph_rel.alias.clone());
            }
            
            // Process right node
            if let LogicalPlan::GraphNode(right_node) = &*graph_rel.right {
                collect_from_graph_node(
                    right_node,
                    &graph_rel.alias,
                    alias_map,
                    view_scan_map,
                )?;
            }
            
            Ok(())
        }
        
        LogicalPlan::ViewScan(_) => {
            // Handled by parent GraphNode
            Ok(())
        }
        
        // Recurse into other plan types
        _ => {
            // TODO: Handle other plan types
            Ok(())
        }
    }
}

/// Collect alias mapping for a GraphNode
fn collect_from_graph_node(
    graph_node: &GraphNode,
    edge_alias: &str,
    alias_map: &mut HashMap<String, String>,
    view_scan_map: &mut HashMap<String, ViewScanInfo>,
) -> Result<(), AnalyzerError> {
    // Extract ViewScan from node's input
    if let Some(view_scan) = extract_view_scan(&graph_node.input) {
        let view_info = ViewScanInfo {
            source_table: view_scan.source_table.clone(),
            property_mapping: view_scan.property_mapping.clone(),
            is_denormalized: view_scan.is_denormalized || graph_node.is_denormalized,
        };
        
        // If denormalized, map node alias to edge alias
        if view_info.is_denormalized {
            alias_map.insert(graph_node.alias.clone(), edge_alias.to_string());
        } else {
            // Normal case: node maps to itself
            alias_map.insert(graph_node.alias.clone(), graph_node.alias.clone());
        }
        
        view_scan_map.insert(graph_node.alias.clone(), view_info);
    }
    
    Ok(())
}

/// Extract ViewScan from a LogicalPlan if present
fn extract_view_scan(plan: &LogicalPlan) -> Option<&ViewScan> {
    match plan {
        LogicalPlan::ViewScan(view_scan) => Some(view_scan),
        LogicalPlan::GraphNode(node) => extract_view_scan(&node.input),
        // Add other cases as needed
        _ => None,
    }
}
```

---

## Integration with filter_tagging

### Modified filter_tagging.rs

```rust
// src/query_planner/analyzer/filter_tagging.rs

use super::alias_resolution::AliasResolutionContext;

pub fn tag_filters(
    plan: LogicalPlan,
    ctx: &mut PlanCtx,
    alias_resolution: &AliasResolutionContext,  // ← NEW parameter
    schema: &GraphSchema,
) -> Result<LogicalPlan, AnalyzerError> {
    // ... existing code ...
    
    // When processing property access:
    match filter_expr {
        LogicalExpr::PropertyAccess { alias, property } => {
            // OLD approach (broken):
            // PropertyAccess {
            //     table_alias: alias.clone(),
            //     column: property.clone(),
            // }
            
            // NEW approach (uses AliasResolutionContext):
            let (sql_alias, property_value) = alias_resolution
                .resolve_property(&alias, &property)?;
            
            // Now we have:
            // - sql_alias: "f" (resolved from "a")
            // - property_value: PropertyValue::Column("Origin")
            
            // Create resolved PropertyAccess
            PropertyAccess {
                table_alias: sql_alias,
                column: match property_value {
                    PropertyValue::Column(col) => col,
                    PropertyValue::Expression(expr) => {
                        // For expressions, we need to handle differently
                        // Store the expression for later SQL generation
                        expr
                    }
                }
            }
        }
    }
}
```

---

## Benefits of This Approach

### 1. **Respects Existing Design**
- ✅ ViewScan remains the single source of truth
- ✅ PropertyValue expressions supported
- ✅ GraphNode/GraphRel structure unchanged
- ✅ No need to remove `is_denormalized` flags

### 2. **Clean Separation of Concerns**
- ✅ ViewScan: Stores schema mappings (what it already does)
- ✅ AliasResolutionContext: Provides efficient lookup (new)
- ✅ filter_tagging: Uses context to resolve aliases (modified)

### 3. **Minimal Changes Required**
- ✅ Add `alias_resolution.rs` (new file, ~300 lines)
- ✅ Modify `filter_tagging.rs` (add parameter, use resolution)
- ✅ Update analyzer pipeline (add resolution pass)
- ❌ NO changes to ViewScan structure
- ❌ NO changes to LogicalPlan structure

### 4. **Handles Expression Mappings**
```rust
// Schema has:
property_mapping: {
    "full_name": Expression("concat(first_name, ' ', last_name)")
}

// AliasResolutionContext preserves this:
resolve_property("u", "full_name")
    → ("u", PropertyValue::Expression("concat(first_name, ' ', last_name)"))

// SQL generation uses PropertyValue.to_sql():
property_value.to_sql("u")
    → "concat(u.first_name, ' ', u.last_name)"
```

---

## Implementation Plan (Revised)

### Phase 1: Core AliasResolutionContext (3 hours)
```
✓ Create src/query_planner/analyzer/alias_resolution.rs
✓ Define AliasResolutionContext struct
✓ Define ViewScanInfo struct
✓ Implement resolve_alias() and resolve_property()
✓ Add unit tests
```

### Phase 2: LogicalPlan Traversal (4 hours)
```
✓ Implement collect_aliases()
✓ Implement collect_from_graph_node()
✓ Implement extract_view_scan()
✓ Handle GraphRel patterns
✓ Handle nested plans
✓ Add tests for various plan structures
```

### Phase 3: Integration with filter_tagging (3 hours)
```
✓ Modify filter_tagging.rs to accept AliasResolutionContext
✓ Update PropertyAccess creation logic
✓ Handle PropertyValue::Column vs Expression
✓ Propagate errors properly
✓ Add integration tests
```

### Phase 4: Analyzer Pipeline Integration (2 hours)
```
✓ Add AliasResolutionContext to PlanCtx
✓ Call build() after schema_inference
✓ Pass context to filter_tagging
✓ Update analyzer orchestration
```

### Phase 5: Testing (6 hours)
```
✓ Test LAX query (denormalized Airport)
✓ Test expression-based properties
✓ Test mixed scenarios (denorm + normal)
✓ Test with RETURN clause
✓ Test with multiple filters
✓ Test with variable-length paths
✓ Verify all existing tests still pass
```

### Phase 6: Cleanup & Documentation (2 hours)
```
✓ Add inline documentation
✓ Update STATUS.md
✓ Update CHANGELOG.md
✓ Create migration guide for future features
```

**Total**: ~20 hours (2.5 days)

---

## Key Design Decisions

### Decision 1: Keep is_denormalized in ViewScan
**Rationale**: It's part of the schema metadata that ViewScan carries. AliasResolutionContext *uses* this flag but doesn't replace it.

### Decision 2: Preserve PropertyValue Abstraction
**Rationale**: Expression mappings are a powerful feature. AliasResolutionContext works with PropertyValue, not just column names.

### Decision 3: Build Context in Analyzer, Not in RenderPlan
**Rationale**: Analyzer has full LogicalPlan structure and schema access. RenderPlan is too late (context partially lost).

### Decision 4: Make Context Optional in PlanCtx
**Rationale**: Not all queries need alias resolution (e.g., no denormalized patterns). Build on-demand.

---

## Example: Complete Flow

### Input Query
```cypher
MATCH (a:Airport)-[f:Flight]->(b:Airport) 
WHERE a.city = 'Los Angeles' AND f.distance > 1000
RETURN a.code, f.carrier, b.code
```

### Schema (Denormalized)
```yaml
nodes:
  - label: Airport
    table: flights
    from_node_properties:
      code: Origin
      city: OriginCityName
    to_node_properties:
      code: Dest
      city: DestCityName

relationships:
  - type: FLIGHT
    table: flights
    property_mappings:
      distance: Distance
      carrier: Carrier
```

### Step 1: LogicalPlan Created
```rust
GraphRel {
    alias: "f",
    left: GraphNode {
        alias: "a",
        is_denormalized: true,
        input: ViewScan {
            source_table: "flights",
            property_mapping: {
                "code": Column("Origin"),
                "city": Column("OriginCityName"),
            },
            is_denormalized: true,
        }
    },
    center: ViewScan {
        source_table: "flights",
        property_mapping: {
            "distance": Column("Distance"),
            "carrier": Column("Carrier"),
        }
    },
    right: GraphNode {
        alias: "b",
        is_denormalized: true,
        input: ViewScan {
            source_table: "flights",
            property_mapping: {
                "code": Column("Dest"),
                "city": Column("DestCityName"),
            },
            is_denormalized: true,
        }
    }
}
```

### Step 2: AliasResolutionContext Built
```rust
AliasResolutionContext {
    alias_map: {
        "a" → "f",
        "b" → "f",
        "f" → "f",
    },
    view_scan_map: {
        "a": ViewScanInfo {
            property_mapping: { "code": Column("Origin"), "city": Column("OriginCityName") },
            is_denormalized: true,
        },
        "b": ViewScanInfo {
            property_mapping: { "code": Column("Dest"), "city": Column("DestCityName") },
            is_denormalized: true,
        },
        "f": ViewScanInfo {
            property_mapping: { "distance": Column("Distance"), "carrier": Column("Carrier") },
            is_denormalized: false,
        },
    }
}
```

### Step 3: filter_tagging Uses Context
```rust
// WHERE a.city = 'Los Angeles'
resolve_property("a", "city")
    → ("f", PropertyValue::Column("OriginCityName"))
    → PropertyAccess(table_alias="f", column="OriginCityName")

// AND f.distance > 1000
resolve_property("f", "distance")
    → ("f", PropertyValue::Column("Distance"))
    → PropertyAccess(table_alias="f", column="Distance")
```

### Step 4: projection_tagging Uses Context
```rust
// RETURN a.code
resolve_property("a", "code")
    → ("f", PropertyValue::Column("Origin"))

// f.carrier
resolve_property("f", "carrier")
    → ("f", PropertyValue::Column("Carrier"))

// b.code
resolve_property("b", "code")
    → ("f", PropertyValue::Column("Dest"))
```

### Step 5: RenderPlan (Clean SQL)
```rust
RenderPlan {
    from: ViewTableRef {
        table: "flights",
        alias: "f",
    },
    joins: vec![],  // No joins!
    filters: And(
        PropertyAccess("f", "OriginCityName") = 'Los Angeles',
        PropertyAccess("f", "Distance") > 1000
    ),
    select: vec![
        PropertyAccess("f", "Origin"),
        PropertyAccess("f", "Carrier"),
        PropertyAccess("f", "Dest"),
    ]
}
```

### Step 6: Generated SQL
```sql
SELECT 
    f.Origin AS code,
    f.Carrier AS carrier,
    f.Dest AS code
FROM flights AS f
WHERE f.OriginCityName = 'Los Angeles' 
  AND f.Distance > 1000
```

✅ **Valid SQL - No invalid aliases!**

---

## Open Questions

### Q1: What about CTEs for denormalized nodes?
**A**: Don't generate them! RenderPlan checks `is_denormalized` flag and skips CTE generation for those nodes. This is already in the codebase, we just need to ensure filters use correct aliases.

### Q2: What about variable-length paths with denormalized nodes?
**A**: The existing CTE generation already has property mapping logic. AliasResolutionContext ensures that filters/projections use correct aliases when referencing CTE results.

### Q3: How to handle mixed scenarios (some denorm, some not)?
**A**: AliasResolutionContext treats each alias independently:
```rust
// Mixed: "a" denormalized, "c" normal
alias_map: {
    "a" → "f",  // Maps to edge
    "b" → "f",  // Maps to edge
    "f" → "f",  // Edge itself
    "c" → "c",  // Normal node (maps to itself)
}
```

### Q4: Performance overhead?
**A**: Minimal - one-time traversal of LogicalPlan after schema_inference. Result cached in PlanCtx for rest of pipeline.

---

## Summary

This refined approach:
1. **Respects ViewScan design** - Uses it as single source of truth
2. **Supports PropertyValue** - Handles both columns and expressions
3. **Minimal refactoring** - Add context, modify filter_tagging
4. **Clear separation** - Graph concepts end at analyzer output
5. **Testable** - Context can be inspected/validated independently

The key insight: **Don't replace existing structures, augment them with an efficient lookup index.**
