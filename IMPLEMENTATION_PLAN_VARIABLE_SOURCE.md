# Implementation Plan: Variable Source Resolution

## Phase 1: Add Core Data Structures

### File: src/query_planner/variable_source.rs (NEW)

```rust
use std::collections::HashMap;

/// Tracks where a variable gets its data from
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VariableSource {
    /// Base table scan (node or edge table)
    BaseTable {
        table_name: String,
        // For nodes: Some(label), for edges: None
        label: Option<String>,
    },
    /// CTE export from WITH clause
    CteExport {
        cte_name: String,
        // What variable this CTE is exporting
        source_alias: String,
        // What it's exported as
        export_alias: String,
    },
    /// Multiple inputs (UNION)
    Union {
        inputs: Vec<VariableSource>,
    },
}

/// Registry mapping variable names to their sources
#[derive(Debug, Clone, Default)]
pub struct VariableSourceRegistry {
    pub sources: HashMap<String, VariableSource>,
}

impl VariableSourceRegistry {
    pub fn new() -> Self {
        Self::default()
    }
    
    pub fn register(&mut self, alias: String, source: VariableSource) {
        self.sources.insert(alias, source);
    }
    
    pub fn get(&self, alias: &str) -> Option<&VariableSource> {
        self.sources.get(alias)
    }
}

/// Property resolution result
#[derive(Debug, Clone)]
pub struct PropertyResolution {
    /// The actual column name in the source
    pub column_name: String,
    /// The source where this column lives
    pub source: VariableSource,
}
```

### File: src/query_planner/property_resolver.rs (NEW)

```rust
use crate::graph_catalog::GraphSchema;
use crate::query_planner::variable_source::{VariableSource, VariableSourceRegistry, PropertyResolution};

pub struct PropertyResolver {
    schema: GraphSchema,
    variable_sources: VariableSourceRegistry,
}

impl PropertyResolver {
    pub fn new(schema: GraphSchema, variable_sources: VariableSourceRegistry) -> Self {
        Self { schema, variable_sources }
    }
    
    /// Resolve a Cypher property to its actual database column
    /// 
    /// Example: person.name
    /// 1. person is CTE export from u
    /// 2. u is a User node
    /// 3. User.name maps to full_name in DB
    /// 4. CTE exports full_name as u_name
    /// Result: u_name
    pub fn resolve_property(
        &self,
        variable: &str,
        cypher_property: &str,
        cte_exports: &HashMap<String, Vec<String>>,
    ) -> Result<PropertyResolution, String> {
        // Get the source for this variable
        let source = self.variable_sources
            .get(variable)
            .ok_or(format!("Variable '{}' not found", variable))?
            .clone();
        
        match &source {
            VariableSource::BaseTable { table_name, label } => {
                // Direct table access - use schema mapping
                if let Some(lbl) = label {
                    let node_schema = self.schema.get_node_schema(lbl)?;
                    let db_column = node_schema.resolve_property(cypher_property)?;
                    Ok(PropertyResolution {
                        column_name: db_column,
                        source,
                    })
                } else {
                    Err(format!("Cannot resolve property on edge table: {}.{}", variable, cypher_property))
                }
            }
            
            VariableSource::CteExport { source_alias, export_alias, .. } => {
                // CTE export - resolve through source variable first
                let source_resolution = self.resolve_property(
                    source_alias,
                    cypher_property,
                    cte_exports,
                )?;
                
                // source_resolution.column_name is the DB column (e.g., "full_name")
                // Find what the CTE exports this as (e.g., "u_name")
                let cte_export_name = self.find_cte_export_column(
                    source_alias,
                    &source_resolution.column_name,
                    cte_exports,
                )?;
                
                Ok(PropertyResolution {
                    column_name: cte_export_name,
                    source,
                })
            }
            
            VariableSource::Union { .. } => {
                Err("Union property resolution not yet implemented".to_string())
            }
        }
    }
    
    /// Find what column name the CTE exports for a given source column
    /// Example: CTE exports "full_name AS u_name", so for source "full_name"
    /// we return "u_name"
    fn find_cte_export_column(
        &self,
        source_alias: &str,
        source_column: &str,
        cte_exports: &HashMap<String, Vec<String>>,
    ) -> Result<String, String> {
        // This needs access to the WITH clause's SELECT items to build the mapping
        // For now: assuming naming convention source_alias_property
        // e.g., u_full_name for source u and property full_name
        Ok(format!("{}_{}", source_alias, source_column.replace("_", "_")))
    }
}
```

## Phase 2: Integrate with PlanCtx

### Modifications to: src/query_planner/plan_ctx/mod.rs

```rust
// Add to existing PlanCtx struct:
pub variable_sources: VariableSourceRegistry,

// Add methods:
pub fn register_base_table(&mut self, alias: String, table_name: String, label: Option<String>) {
    let source = VariableSource::BaseTable { table_name, label };
    self.variable_sources.register(alias, source);
}

pub fn register_cte_export(
    &mut self,
    export_alias: String,
    source_alias: String,
    cte_name: String,
) {
    let source = VariableSource::CteExport {
        cte_name,
        source_alias,
        export_alias,
    };
    self.variable_sources.register(export_alias, source);
}

pub fn get_variable_source(&self, alias: &str) -> Option<VariableSource> {
    self.variable_sources.get(alias).cloned()
}
```

## Phase 3: Update Planning for Sources

### Modifications to: src/query_planner/logical_plan/plan_builder.rs

In MATCH clause processing:
```rust
// After creating GraphNode for scanned alias
plan_ctx.register_base_table(
    alias.clone(),
    table_name.clone(),
    Some(label.clone()),
);
```

In WITH clause processing:
```rust
// After creating WithClause
for (idx, exported_alias) in with.exported_aliases.iter().enumerate() {
    if idx < with.items.len() {
        if let LogicalExpr::TableAlias(source_alias_expr) = &with.items[idx].expression {
            plan_ctx.register_cte_export(
                exported_alias.clone(),
                source_alias_expr.0.clone(),
                cte_name.clone(),
            );
        }
    }
}
```

## Phase 4: Update Render Phase

### Modifications to: src/render_plan/select_builder.rs

In property expansion:
```rust
fn resolve_property_column(
    &self,
    variable: &str,
    property: &str,
    plan_ctx: &PlanCtx,
) -> Option<String> {
    // Try to resolve through variable sources first
    if let Some(source) = plan_ctx.get_variable_source(variable) {
        match source {
            VariableSource::CteExport { source_alias, .. } => {
                // This is a CTE variable - need to resolve through layers
                // For now: use the CTE registry approach
                return None; // Fallback to existing logic
            }
            _ => {}
        }
    }
    
    // Fall back to existing logic
    None
}
```

## Phase 5: Test Cases

### test_variable_source_base_table()
- Verify GraphNode registers as BaseTable

### test_variable_source_cte_export()
- MATCH (u:User) WITH u AS person
- Verify person registered as CteExport pointing to u

### test_property_resolution_direct()
- u.name resolves to full_name (DB column)

### test_property_resolution_through_cte()
- person.name → person is CTE of u → u.name → full_name → u_name (CTE export)

### test_multi_layer_resolution()
- MATCH (u:User) WITH u AS person MATCH (p:Post)-[:AUTHORED]->(person)
- Verify person resolved correctly at each layer

## Success Criteria

1. ✅ Variable sources tracked from logical → render phase
2. ✅ Property resolution works through CTE layers
3. ✅ WITH u AS person RETURN person.name generates correct SQL
4. ✅ All existing tests continue to pass
5. ✅ New test cases pass for complex scenarios
