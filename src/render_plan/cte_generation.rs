//! CTE generation utilities for variable-length path queries

use std::collections::HashMap;

use crate::clickhouse_query_generator::variable_length_cte::WeightCteConfig;
use crate::clickhouse_query_generator::NodeProperty;
use crate::graph_catalog::config::Identifier;
use crate::graph_catalog::expression_parser::PropertyValue;
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::query_planner::logical_plan::LogicalPlan;
use crate::query_planner::logical_plan::ShortestPathMode;
use crate::query_planner::logical_plan::VariableLengthSpec;
use crate::render_plan::render_expr::RenderExpr;

/// Context for CTE generation - holds property requirements and other metadata
#[derive(Debug, Clone, Default)]
pub struct CteGenerationContext {
    /// WHERE filter expression to apply to variable-length CTEs
    filter_expr: Option<RenderExpr>,
    /// Graph schema for this query (enables multi-schema support)
    schema: Option<GraphSchema>,
    /// Fixed-length path inline JOINs (from_table, from_alias, joins)
    /// Key: "start_alias-end_alias" for the GraphRel pattern
    fixed_length_joins: HashMap<String, (String, String, Vec<super::Join>)>,
    /// Variable length specification for the path pattern
    pub spec: VariableLengthSpec,
    /// Path variable name (e.g., "p" in `MATCH p = (a)-[*]->(b)`)
    pub path_variable: Option<String>,
    /// Shortest path mode for shortestPath() and allShortestPaths()
    pub shortest_path_mode: Option<ShortestPathMode>,
    /// Relationship types for the VLP pattern (for polymorphic edges)
    pub relationship_types: Option<Vec<String>>,
    /// Edge ID identifier from schema (for RETURN relationships(p))
    pub edge_id: Option<Identifier>,
    /// Relationship Cypher alias (e.g., "r" in `(a)-[r*]->(b)`)
    pub relationship_cypher_alias: Option<String>,
    /// Start node label (for polymorphic heterogeneous paths)
    pub start_node_label: Option<String>,
    /// End node label (for polymorphic heterogeneous paths)
    pub end_node_label: Option<String>,
    /// Whether this VLP is optional (affects start node filter handling)
    pub is_optional: bool,

    /// **NEW (Feb 2026)**: Multi-type pattern combinations for UNION generation
    /// Map: `(from_alias, to_alias) → Vec<TypeCombination>`
    /// When set, CTE generation creates UNION of all pattern combinations
    pub pattern_combinations:
        Option<HashMap<(String, String), Vec<crate::query_planner::plan_ctx::TypeCombination>>>,

    /// Weighted shortest path: pre-computed edge weight CTE configuration
    pub weight_cte: Option<WeightCteConfig>,
    /// Whether the query uses `relationships(path)`. When false, VLP CTE skips
    /// growing path_relationships arrays, saving significant memory.
    pub needs_path_relationships: bool,
    /// Lightweight BFS mode for shortestPath queries that only need length(path).
    /// Generates a global-visited-set BFS instead of per-path tracking.
    pub use_bfs_mode: bool,
    /// True when the original edge direction is Either (undirected).
    /// BFS mode generates two UNION ALL branches for both traversal directions.
    pub is_undirected: bool,
    /// #617: true when this undirected VLP was normalized by the analyzer to a
    /// SINGLE directed walk over a doubled-edge set (instead of the legacy
    /// two-monotone-arm Union split). Distinct from `is_undirected`, which is
    /// also true for the individual arms of a legacy split.
    pub undirected_single_walk: bool,
    /// Root plan reference for checking path variable usage across the entire query.
    /// Set at the top-level to_render_plan call so VLP extraction can check if path
    /// variables are used bare (preventing BFS optimization).
    pub root_plan: Option<std::sync::Arc<LogicalPlan>>,
}

impl CteGenerationContext {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn with_spec(mut self, spec: VariableLengthSpec) -> Self {
        self.spec = spec;
        self
    }

    /// Set path variable name
    pub(crate) fn with_path_variable(mut self, path_var: Option<String>) -> Self {
        self.path_variable = path_var;
        self
    }

    /// Set shortest path mode
    pub(crate) fn with_shortest_path_mode(mut self, mode: Option<ShortestPathMode>) -> Self {
        self.shortest_path_mode = mode;
        self
    }

    /// Set relationship types (for polymorphic edges)
    pub(crate) fn with_relationship_types(mut self, types: Option<Vec<String>>) -> Self {
        self.relationship_types = types;
        self
    }

    /// Set edge ID from schema
    pub(crate) fn with_edge_id(mut self, edge_id: Option<Identifier>) -> Self {
        self.edge_id = edge_id;
        self
    }

    /// Set relationship Cypher alias
    pub(crate) fn with_relationship_cypher_alias(mut self, alias: Option<String>) -> Self {
        self.relationship_cypher_alias = alias;
        self
    }

    /// Set node labels for polymorphic heterogeneous paths
    pub(crate) fn with_node_labels(
        mut self,
        start_label: Option<String>,
        end_label: Option<String>,
    ) -> Self {
        self.start_node_label = start_label;
        self.end_node_label = end_label;
        self
    }

    /// Set whether this VLP is optional
    pub(crate) fn with_is_optional(mut self, is_optional: bool) -> Self {
        self.is_optional = is_optional;
        self
    }

    /// Set weight CTE configuration for weighted shortest path
    pub(crate) fn with_weight_cte(mut self, config: Option<WeightCteConfig>) -> Self {
        self.weight_cte = config;
        self
    }

    /// Set the graph schema (builder pattern)
    pub(crate) fn with_schema_owned(mut self, schema: GraphSchema) -> Self {
        self.schema = Some(schema);
        self
    }

    pub(crate) fn schema(&self) -> Option<&GraphSchema> {
        self.schema.as_ref()
    }

    /// Read accessor for the WHERE filter. Currently unused: `with_filter`
    /// (live, called from `cte_extraction`) populates `filter_expr`, but no
    /// consumer reads it back yet. Kept as the paired getter for that field.
    #[allow(dead_code)]
    pub(crate) fn get_filter(&self) -> Option<&RenderExpr> {
        self.filter_expr.as_ref()
    }

    pub(crate) fn with_filter(mut self, filter: RenderExpr) -> Self {
        self.filter_expr = Some(filter);
        self
    }

    /// Store fixed-length path inline JOINs for later retrieval
    pub(crate) fn set_fixed_length_joins(
        &mut self,
        start_alias: &str,
        end_alias: &str,
        from_table: String,
        from_alias: String,
        joins: Vec<super::Join>,
    ) {
        let key = format!("{}-{}", start_alias, end_alias);
        log::info!(
            "Storing fixed-length JOINs for {}: {} joins",
            key,
            joins.len()
        );
        self.fixed_length_joins
            .insert(key, (from_table, from_alias, joins));
    }

    /// Retrieve fixed-length path inline JOINs if available. Currently unused:
    /// `set_fixed_length_joins` (live, called from `cte_extraction`) populates
    /// `fixed_length_joins`, but no consumer reads it back yet. Kept as the
    /// paired getter for that field.
    #[allow(dead_code)]
    pub(crate) fn get_fixed_length_joins(
        &self,
        start_alias: &str,
        end_alias: &str,
    ) -> Option<&(String, String, Vec<super::Join>)> {
        let key = format!("{}-{}", start_alias, end_alias);
        self.fixed_length_joins.get(&key)
    }
}

/// Extract properties referenced in a RenderExpr (e.g., from filters)
/// Returns a vector of properties that need to be included in the CTE
pub(crate) fn extract_properties_from_filter(
    expr: &RenderExpr,
    node_alias: &str,
    node_label: &str,
) -> Vec<NodeProperty> {
    let mut properties = Vec::new();
    extract_properties_from_expr_recursive(expr, node_alias, node_label, &mut properties);
    properties
}

fn extract_properties_from_expr_recursive(
    expr: &RenderExpr,
    node_alias: &str,
    node_label: &str,
    properties: &mut Vec<NodeProperty>,
) {
    match expr {
        RenderExpr::PropertyAccessExp(prop) => {
            // Check if this property belongs to the target node
            if prop.table_alias.0 == node_alias {
                let property_name = prop.column.raw();
                // Map Cypher property to ClickHouse column
                let column_name = map_property_to_column_with_schema(property_name, node_label)
                    .unwrap_or_else(|_| property_name.to_string());

                // Add if not already in the list
                if !properties.iter().any(|p| p.alias == property_name) {
                    properties.push(NodeProperty {
                        cypher_alias: node_alias.to_string(),
                        column_name,
                        alias: property_name.to_string(),
                    });
                }
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            // Recurse into all operands
            for operand in &op.operands {
                extract_properties_from_expr_recursive(operand, node_alias, node_label, properties);
            }
        }
        RenderExpr::ScalarFnCall(fn_call) => {
            // Recurse into function arguments
            for arg in &fn_call.args {
                extract_properties_from_expr_recursive(arg, node_alias, node_label, properties);
            }
        }
        RenderExpr::List(exprs) => {
            // Recurse into list elements
            for e in exprs {
                extract_properties_from_expr_recursive(e, node_alias, node_label, properties);
            }
        }
        RenderExpr::Case(case_expr) => {
            // Recurse into case expression
            if let Some(expr) = &case_expr.expr {
                extract_properties_from_expr_recursive(expr, node_alias, node_label, properties);
            }
            for (when_expr, then_expr) in &case_expr.when_then {
                extract_properties_from_expr_recursive(
                    when_expr, node_alias, node_label, properties,
                );
                extract_properties_from_expr_recursive(
                    then_expr, node_alias, node_label, properties,
                );
            }
            if let Some(else_expr) = &case_expr.else_expr {
                extract_properties_from_expr_recursive(
                    else_expr, node_alias, node_label, properties,
                );
            }
        }
        RenderExpr::InSubquery(subquery) => {
            extract_properties_from_expr_recursive(
                &subquery.expr,
                node_alias,
                node_label,
                properties,
            );
        }
        // Base cases: literals, columns, etc. don't contain property accesses
        _ => {}
    }
}

/// Schema-aware property mapping using GraphSchema
/// Map a property to column with schema awareness
/// Returns an error if the schema is not available or the property mapping is not found
///
/// # Denormalized Property Access
/// If `relationship_type` is provided, this function checks if the property is denormalized
/// (available directly in the edge table) before falling back to node table lookup.
/// This enables 10-100x faster queries by eliminating JOINs.
pub(crate) fn map_property_to_column_with_schema(
    property: &str,
    node_label: &str,
) -> Result<String, String> {
    map_property_to_column_with_relationship_context(property, node_label, None, None, None)
}

/// Schema-aware property mapping with relationship context
/// Checks denormalized properties first, then falls back to node properties
/// Indicates whether a node is on the FROM (left) or TO (right) side of a relationship
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
    /// Node is on the FROM side (left_connection in GraphRel)
    From,
    /// Node is on the TO side (right_connection in GraphRel)
    To,
}

pub fn map_property_to_column_with_relationship_context(
    property: &str,
    node_label: &str,
    relationship_type: Option<&str>,
    node_role: Option<NodeRole>,
    _schema_name: Option<&str>,
) -> Result<String, String> {
    use std::fs::OpenOptions;
    use std::io::Write;

    // Get schema from task-local context (set at query entry),
    // falling back to GLOBAL_SCHEMAS for backward compatibility (tests).
    let current_schema = crate::server::query_context::get_current_schema_with_fallback();
    let schema = if let Some(ref s) = current_schema {
        s.as_ref()
    } else {
        return Err(format!(
            "No schema available for property '{}' on node '{}'",
            property, node_label
        ));
    };

    log::info!(
        "🔍 map_property_to_column_with_relationship_context: property='{}', node_label='{}'",
        property,
        node_label,
    );

    // Get the node schema first
    let node_schema = schema.all_node_schemas().get(node_label).ok_or_else(|| {
        let available: Vec<String> = schema.all_node_schemas().keys().cloned().collect();
        let msg = format!(
            "Node label '{}' not found in schema. Available labels: {}",
            node_label,
            available.join(", ")
        );
        if let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open("debug_property_mapping.log")
        {
            let _ = writeln!(file, "ERROR: {}", msg);
        }
        msg
    })?;

    // 🆕 DENORMALIZED NODE: Check node-level denormalized properties FIRST
    // ✅ PHASE 2 APPROVED: Queries schema configuration, not plan flags
    if node_schema.is_denormalized {
        if let Some(rel_type) = relationship_type {
            if let Ok(rel_schema) = schema.get_rel_schema(rel_type) {
                // Foreign-edge (id-via-FK) guard: when the node is denormalized on
                // a DIFFERENT table than this edge, its node-level from/to_properties
                // point at columns of its own source table (e.g. flights.Origin),
                // which do NOT exist in the edge table. Skip the node-level mapping
                // and fall through to the edge-level branch below, which carries the
                // correct {node_id → edge FK column} mapping for the foreign edge.
                let coupled_on_this_edge = node_schema.denormalized_source_table.as_deref()
                    == Some(rel_schema.full_table_name().as_str());
                if coupled_on_this_edge {
                    // Use the caller-provided role to determine which property map to use
                    match node_role {
                        Some(NodeRole::From) => {
                            // Node is on the FROM side - use from_properties
                            if let Some(from_props) = &node_schema.from_properties {
                                if let Some(column) = from_props.get(property) {
                                    return Ok(column.clone());
                                }
                            }
                        }
                        Some(NodeRole::To) => {
                            // Node is on the TO side - use to_properties
                            if let Some(to_props) = &node_schema.to_properties {
                                if let Some(column) = to_props.get(property) {
                                    return Ok(column.clone());
                                }
                            }
                        }
                        None => {
                            // Fallback: try to infer from schema (works when labels differ)
                            if rel_schema.from_node == node_label {
                                if let Some(from_props) = &node_schema.from_properties {
                                    if let Some(column) = from_props.get(property) {
                                        return Ok(column.clone());
                                    }
                                }
                            }
                            if rel_schema.to_node == node_label {
                                if let Some(to_props) = &node_schema.to_properties {
                                    if let Some(column) = to_props.get(property) {
                                        return Ok(column.clone());
                                    }
                                }
                            }
                        }
                    }
                } // end if coupled_on_this_edge
            }
        } else {
            // No relationship context (standalone node scan).
            // Try from_properties first, then to_properties as fallback.
            // This handles coupled schemas where nodes have no property_mappings
            // but store column mappings in from/to_node_properties.
            if let Some(from_props) = &node_schema.from_properties {
                if let Some(column) = from_props.get(property) {
                    log::info!(
                        "✓ Denormalized standalone node: {}.{} → {} (from_properties)",
                        node_label,
                        property,
                        column
                    );
                    return Ok(column.clone());
                }
            }
            if let Some(to_props) = &node_schema.to_properties {
                if let Some(column) = to_props.get(property) {
                    log::info!(
                        "✓ Denormalized standalone node: {}.{} → {} (to_properties)",
                        node_label,
                        property,
                        column
                    );
                    return Ok(column.clone());
                }
            }
        }
    }

    // 🆕 DENORMALIZED EDGE: Check edge-level denormalized properties (for backward compatibility)
    if let Some(rel_type) = relationship_type {
        if let Ok(rel_schema) = schema.get_rel_schema(rel_type) {
            // Use the caller-provided role to determine which property map to use
            match node_role {
                Some(NodeRole::From) => {
                    // Node is on the FROM side - use from_node_properties
                    if let Some(from_props) = &rel_schema.from_node_properties {
                        if let Some(column) = from_props.get(property) {
                            return Ok(column.clone());
                        }
                    }
                }
                Some(NodeRole::To) => {
                    // Node is on the TO side - use to_node_properties
                    if let Some(to_props) = &rel_schema.to_node_properties {
                        if let Some(column) = to_props.get(property) {
                            return Ok(column.clone());
                        }
                    }
                }
                None => {
                    // Fallback: try to infer from schema (works when labels differ)
                    if rel_schema.from_node == node_label {
                        if let Some(from_props) = &rel_schema.from_node_properties {
                            if let Some(column) = from_props.get(property) {
                                return Ok(column.clone());
                            }
                        }
                    }
                    if rel_schema.to_node == node_label {
                        if let Some(to_props) = &rel_schema.to_node_properties {
                            if let Some(column) = to_props.get(property) {
                                return Ok(column.clone());
                            }
                        }
                    }
                }
            }
        }
    }

    // Fall back to traditional node property mapping

    let column = node_schema.property_mappings.get(property).ok_or_else(|| {
        // Sorted so the error text is stable across processes (HashMap keys).
        let mut available: Vec<String> = node_schema.property_mappings.keys().cloned().collect();
        available.sort();
        let msg = format!(
            "Property '{}' not found for node label '{}'. Available properties: {}",
            property,
            node_label,
            available.join(", ")
        );
        if let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open("debug_property_mapping.log")
        {
            let _ = writeln!(file, "ERROR: {}", msg);
        }
        msg
    })?;

    Ok(column.raw().to_string())
}

/// Like `map_property_to_column_with_relationship_context` but preserves
/// the `PropertyValue` variant (Column vs Expression). Use this when the
/// caller needs to generate correct SQL for expression-based mappings.
pub fn map_property_to_property_value(
    property: &str,
    node_label: &str,
) -> Result<PropertyValue, String> {
    let current_schema = crate::server::query_context::get_current_schema_with_fallback();
    let schema = current_schema.as_ref().ok_or_else(|| {
        format!(
            "No schema available for property '{}' on node '{}'",
            property, node_label
        )
    })?;

    let node_schema = schema
        .all_node_schemas()
        .get(node_label)
        .ok_or_else(|| format!("Node label '{}' not found in schema", node_label))?;

    node_schema
        .property_mappings
        .get(property)
        .cloned()
        .ok_or_else(|| {
            format!(
                "Property '{}' not found for node label '{}'",
                property, node_label
            )
        })
}

/// Like `map_property_to_property_value` but for relationship properties.
pub fn map_rel_property_to_property_value(
    property: &str,
    relationship_type: &str,
) -> Result<PropertyValue, String> {
    let current_schema = crate::server::query_context::get_current_schema_with_fallback();
    let schema = current_schema.as_ref().ok_or_else(|| {
        format!(
            "No schema available for property '{}' on rel '{}'",
            property, relationship_type
        )
    })?;

    let rel_schema = schema
        .get_rel_schema(relationship_type)
        .map_err(|e| format!("Relationship type '{}' not found: {}", relationship_type, e))?;

    rel_schema
        .property_mappings
        .get(property)
        .cloned()
        .ok_or_else(|| {
            format!(
                "Property '{}' not found for relationship type '{}'",
                property, relationship_type
            )
        })
}

/// Map a relationship property to its corresponding column name in the schema.
/// This is the relationship equivalent of map_property_to_column_with_schema.
///
/// # Arguments
/// * `property` - The Cypher property name (e.g., "since_date")
/// * `relationship_type` - The relationship type (e.g., "FRIENDS_WITH")
/// * `schema_name` - Optional explicit schema name (uses task-local context if None)
///
/// # Returns
/// * `Ok(column_name)` - The mapped column name (e.g., "since")
/// * `Err(msg)` - If the relationship type or property is not found
pub fn map_relationship_property_to_column(
    property: &str,
    relationship_type: &str,
    _schema_name: Option<&str>,
) -> Result<String, String> {
    // Get schema from task-local context, falling back to GLOBAL_SCHEMAS for tests
    let current_schema = crate::server::query_context::get_current_schema_with_fallback();
    let schema = current_schema.as_deref().ok_or_else(|| {
        format!(
            "No schema available for relationship property '{}' on type '{}'",
            property, relationship_type
        )
    })?;

    // Get the relationship schema
    let rel_schema = schema
        .get_relationships_schema_opt(relationship_type)
        .ok_or_else(|| {
            format!(
                "Relationship type '{}' not found in schema",
                relationship_type
            )
        })?;

    // Look up the property in property_mappings
    let column = rel_schema.property_mappings.get(property).ok_or_else(|| {
        // Sorted so the error text is stable across processes (HashMap keys).
        let mut available: Vec<String> = rel_schema.property_mappings.keys().cloned().collect();
        available.sort();
        format!(
            "Property '{}' not found in relationship type '{}'. Available properties: {}",
            property,
            relationship_type,
            available.join(", ")
        )
    })?;

    Ok(column.raw().to_string())
}
