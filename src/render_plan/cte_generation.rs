//! CTE generation utilities for variable-length path queries
//!
//! Some structures and methods in this module are reserved for future use.
// Note: NodeRole enum and some helper methods are intentionally kept for future CTE enhancements
#![allow(dead_code)]

use std::collections::HashMap;

use crate::clickhouse_query_generator::NodeProperty;
use crate::graph_catalog::config::Identifier;
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::query_planner::logical_expr::LogicalExpr;
use crate::query_planner::logical_plan::LogicalPlan;
use crate::query_planner::logical_plan::ShortestPathMode;
use crate::query_planner::logical_plan::VariableLengthSpec;
use crate::render_plan::cte_extraction::extract_node_label_from_viewscan;
use crate::render_plan::render_expr::RenderExpr;

/// Context for CTE generation - holds property requirements and other metadata
#[derive(Debug, Clone, Default)]
pub struct CteGenerationContext {
    /// Properties needed for variable-length paths, keyed by "left_alias-right_alias"
    variable_length_properties: HashMap<String, Vec<NodeProperty>>,
    /// WHERE filter expression to apply to variable-length CTEs
    filter_expr: Option<RenderExpr>,
    /// Cypher aliases for start and end nodes (for filter rewriting)
    start_cypher_alias: Option<String>,
    end_cypher_alias: Option<String>,
    /// Graph schema for this query (enables multi-schema support)
    schema: Option<GraphSchema>,
    /// Fixed-length path inline JOINs (from_table, from_alias, joins)
    /// Key: "start_alias-end_alias" for the GraphRel pattern
    fixed_length_joins: HashMap<String, (String, String, Vec<super::Join>)>,
    /// Variable length specification for the path pattern
    pub spec: VariableLengthSpec,
    /// Path variable name (e.g., "p" in MATCH p = (a)-[*]->(b))
    pub path_variable: Option<String>,
    /// Shortest path mode for shortestPath() and allShortestPaths()
    pub shortest_path_mode: Option<ShortestPathMode>,
    /// Relationship types for the VLP pattern (for polymorphic edges)
    pub relationship_types: Option<Vec<String>>,
    /// Edge ID identifier from schema (for RETURN relationships(p))
    pub edge_id: Option<Identifier>,
    /// Relationship Cypher alias (e.g., "r" in (a)-[r*]->(b))
    pub relationship_cypher_alias: Option<String>,
    /// Start node label (for polymorphic heterogeneous paths)
    pub start_node_label: Option<String>,
    /// End node label (for polymorphic heterogeneous paths)
    pub end_node_label: Option<String>,
    /// Whether this VLP is optional (affects start node filter handling)
    pub is_optional: bool,
}

impl CteGenerationContext {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn with_schema(schema: GraphSchema) -> Self {
        Self {
            variable_length_properties: HashMap::new(),
            filter_expr: None,
            start_cypher_alias: None,
            end_cypher_alias: None,
            schema: Some(schema),
            fixed_length_joins: HashMap::new(),
            spec: VariableLengthSpec::default(),
            path_variable: None,
            shortest_path_mode: None,
            relationship_types: None,
            edge_id: None,
            relationship_cypher_alias: None,
            start_node_label: None,
            end_node_label: None,
            is_optional: false,
        }
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

    /// Set the graph schema (builder pattern)
    pub(crate) fn with_schema_owned(mut self, schema: GraphSchema) -> Self {
        self.schema = Some(schema);
        self
    }

    pub(crate) fn schema(&self) -> Option<&GraphSchema> {
        self.schema.as_ref()
    }

    pub(crate) fn get_properties(&self, left_alias: &str, right_alias: &str) -> Vec<NodeProperty> {
        let key = format!("{}-{}", left_alias, right_alias);
        self.variable_length_properties
            .get(&key)
            .cloned()
            .unwrap_or_default()
    }

    // 🆕 IMMUTABLE BUILDER PATTERN: Returns new context instead of mutating
    pub(crate) fn with_properties(
        mut self,
        left_alias: &str,
        right_alias: &str,
        properties: Vec<NodeProperty>,
    ) -> Self {
        let key = format!("{}-{}", left_alias, right_alias);
        self.variable_length_properties.insert(key, properties);
        self
    }

    // 🔧 DEPRECATED: Keep for compatibility during migration
    #[deprecated(note = "Use with_properties() instead - immutable builder pattern")]
    pub(crate) fn set_properties(
        &mut self,
        left_alias: &str,
        right_alias: &str,
        properties: Vec<NodeProperty>,
    ) {
        let key = format!("{}-{}", left_alias, right_alias);
        self.variable_length_properties.insert(key, properties);
    }

    pub(crate) fn get_filter(&self) -> Option<&RenderExpr> {
        self.filter_expr.as_ref()
    }

    // 🆕 IMMUTABLE: Returns new context
    pub(crate) fn with_filter(mut self, filter: RenderExpr) -> Self {
        self.filter_expr = Some(filter);
        self
    }

    // 🔧 DEPRECATED: Keep for compatibility
    #[deprecated(note = "Use with_filter() instead")]
    pub(crate) fn set_filter(&mut self, filter: RenderExpr) {
        self.filter_expr = Some(filter);
    }

    pub(crate) fn get_start_cypher_alias(&self) -> Option<&str> {
        self.start_cypher_alias.as_deref()
    }

    // 🆕 IMMUTABLE: Returns new context
    pub(crate) fn with_start_cypher_alias(mut self, alias: String) -> Self {
        self.start_cypher_alias = Some(alias);
        self
    }

    // 🔧 DEPRECATED: Keep for compatibility
    #[deprecated(note = "Use with_start_cypher_alias() instead")]
    pub(crate) fn set_start_cypher_alias(&mut self, alias: String) {
        self.start_cypher_alias = Some(alias);
    }

    pub(crate) fn get_end_cypher_alias(&self) -> Option<&str> {
        self.end_cypher_alias.as_deref()
    }

    // 🆕 IMMUTABLE: Returns new context
    pub(crate) fn with_end_cypher_alias(mut self, alias: String) -> Self {
        self.end_cypher_alias = Some(alias);
        self
    }

    // 🔧 DEPRECATED: Keep for compatibility
    #[deprecated(note = "Use with_end_cypher_alias() instead")]
    pub(crate) fn set_end_cypher_alias(&mut self, alias: String) {
        self.end_cypher_alias = Some(alias);
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

    /// Retrieve fixed-length path inline JOINs if available
    pub(crate) fn get_fixed_length_joins(
        &self,
        start_alias: &str,
        end_alias: &str,
    ) -> Option<&(String, String, Vec<super::Join>)> {
        let key = format!("{}-{}", start_alias, end_alias);
        self.fixed_length_joins.get(&key)
    }
}

/// Extract node label from a GraphNode plan
fn extract_node_label_from_plan(plan: &LogicalPlan) -> String {
    match plan {
        LogicalPlan::GraphNode(node) => {
            // Look for ViewScan in the input
            if let Some(label) = extract_node_label_from_viewscan(&node.input) {
                return label;
            }
            // Fallback to alias if no label found
            node.alias.clone()
        }
        _ => "User".to_string(), // fallback
    }
}

/// Analyze the plan to determine what properties are needed for variable-length CTEs
pub(crate) fn analyze_property_requirements(
    plan: &LogicalPlan,
    schema: &GraphSchema,
) -> CteGenerationContext {
    let context = CteGenerationContext::with_schema(schema.clone());

    // Find variable-length relationships and their required properties
    if let Some((left_alias, right_alias, left_label, right_label, rel_type)) =
        get_variable_length_info(plan)
    {
        let properties = extract_var_len_properties(
            plan,
            &left_alias,
            &right_alias,
            &left_label,
            &right_label,
            Some(&rel_type),
        );
        // 🆕 IMMUTABLE PATTERN: Chain the builder method
        return context.with_properties(&left_alias, &right_alias, properties);
    }

    context
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

/// Extract property requirements from projection for variable-length paths
/// Returns a vector of properties that need to be included in the CTE
/// Recursively searches through the plan to find the Projection node
///
/// # Denormalized Property Access
/// If `relationship_type` is provided, properties are checked against denormalized
/// edge tables first before falling back to node tables. This enables 10-100x faster
/// queries by eliminating JOINs in variable-length path traversals.
pub(crate) fn extract_var_len_properties(
    plan: &LogicalPlan,
    left_alias: &str,
    right_alias: &str,
    left_label: &str,
    right_label: &str,
    relationship_type: Option<&str>,
) -> Vec<NodeProperty> {
    let mut properties = Vec::new();

    // Find the projection in the plan (recursively)
    match plan {
        LogicalPlan::Projection(proj) => {
            for item in &proj.items {
                // Check if this is a property access expression
                if let LogicalExpr::PropertyAccessExp(prop_acc) = &item.expression {
                    let node_alias = prop_acc.table_alias.0.as_str();
                    let property_name = prop_acc.column.raw();

                    // Determine if this is for the left or right node
                    if node_alias == left_alias || node_alias == right_alias {
                        // Determine which node label to use
                        let node_label = if node_alias == left_alias {
                            left_label
                        } else {
                            right_label
                        };

                        // Handle wildcard property selection
                        if property_name == "*" {
                            // Expand * to all properties for this node type
                            if let Some(schema_lock) = crate::server::GLOBAL_SCHEMAS.get() {
                                if let Ok(schemas) = schema_lock.try_read() {
                                    // TODO(schema-threading): Hardcoded "default" - should use context.schema
                                    if let Some(schema) = schemas.get("default") {
                                        if let Some(node_schema) =
                                            schema.all_node_schemas().get(node_label)
                                        {
                                            // Create a property for each mapping
                                            for (prop_name, prop_value) in
                                                &node_schema.property_mappings
                                            {
                                                properties.push(NodeProperty {
                                                    cypher_alias: node_alias.to_string(),
                                                    column_name: prop_value.raw().to_string(),
                                                    alias: prop_name.clone(),
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                        } else {
                            // Regular property - use denormalized-aware mapping
                            let column_name = map_property_to_column_with_relationship_context(
                                property_name,
                                node_label,
                                relationship_type,
                                None,
                                None,
                            )
                            .unwrap_or_else(|_| property_name.to_string());
                            let alias = property_name.to_string();

                            properties.push(NodeProperty {
                                cypher_alias: node_alias.to_string(),
                                column_name,
                                alias,
                            });
                        }
                    }
                }
            }
        }
        // Recursively search in child plans
        LogicalPlan::Filter(filter) => {
            return extract_var_len_properties(
                &filter.input,
                left_alias,
                right_alias,
                left_label,
                right_label,
                relationship_type,
            );
        }
        LogicalPlan::OrderBy(order_by) => {
            return extract_var_len_properties(
                &order_by.input,
                left_alias,
                right_alias,
                left_label,
                right_label,
                relationship_type,
            );
        }
        LogicalPlan::Skip(skip) => {
            return extract_var_len_properties(
                &skip.input,
                left_alias,
                right_alias,
                left_label,
                right_label,
                relationship_type,
            );
        }
        LogicalPlan::Limit(limit) => {
            return extract_var_len_properties(
                &limit.input,
                left_alias,
                right_alias,
                left_label,
                right_label,
                relationship_type,
            );
        }
        LogicalPlan::GroupBy(group_by) => {
            return extract_var_len_properties(
                &group_by.input,
                left_alias,
                right_alias,
                left_label,
                right_label,
                relationship_type,
            );
        }
        LogicalPlan::GraphJoins(joins) => {
            return extract_var_len_properties(
                &joins.input,
                left_alias,
                right_alias,
                left_label,
                right_label,
                relationship_type,
            );
        }
        _ => {}
    }

    properties
}

/// Extract alias from a plan node
fn extract_alias_from_plan(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::GraphNode(node) => Some(node.alias.clone()),
        LogicalPlan::ViewScan(_) => None, // ViewScan doesn't have an alias field
        _ => None,
    }
}

/// Get variable length info from plan
fn get_variable_length_info(
    plan: &LogicalPlan,
) -> Option<(String, String, String, String, String)> {
    match plan {
        LogicalPlan::GraphRel(graph_rel) => {
            if graph_rel.variable_length.is_some() {
                let left_alias = extract_alias_from_plan(&graph_rel.left)?;
                let right_alias = extract_alias_from_plan(&graph_rel.right)?;
                let left_label = extract_node_label_from_viewscan(&graph_rel.left)?;
                let right_label = extract_node_label_from_viewscan(&graph_rel.right)?;
                let rel_type = graph_rel.labels.as_ref()?.first()?.clone();
                Some((left_alias, right_alias, left_label, right_label, rel_type))
            } else {
                None
            }
        }
        LogicalPlan::GraphNode(node) => get_variable_length_info(&node.input),
        LogicalPlan::Filter(filter) => get_variable_length_info(&filter.input),
        LogicalPlan::Projection(proj) => get_variable_length_info(&proj.input),
        _ => None,
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

/// Map property to column with explicit schema context (preferred)
/// When schema_name is provided, it searches ONLY that schema (deterministic)
/// When schema_name is None, it searches all schemas (legacy behavior)
pub(crate) fn map_property_to_column_with_schema_context(
    property: &str,
    node_label: &str,
    schema_name: Option<&str>,
) -> Result<String, String> {
    map_property_to_column_with_relationship_context(property, node_label, None, None, schema_name)
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
    schema_name: Option<&str>,
) -> Result<String, String> {
    use std::fs::OpenOptions;
    use std::io::Write;

    // Try to get the schema from the global state
    let schema_lock = crate::server::GLOBAL_SCHEMAS.get().ok_or_else(|| {
        let msg = "GLOBAL_SCHEMAS not initialized".to_string();
        if let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open("debug_property_mapping.log")
        {
            let _ = writeln!(file, "ERROR: {}", msg);
        }
        msg
    })?;

    let schemas = schema_lock.try_read().map_err(|_| {
        let msg = "Failed to acquire read lock on GLOBAL_SCHEMAS".to_string();
        if let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open("debug_property_mapping.log")
        {
            let _ = writeln!(file, "ERROR: {}", msg);
        }
        msg
    })?;

    if schemas.is_empty() {
        let msg = "No schemas loaded in GLOBAL_SCHEMAS".to_string();
        if let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open("debug_property_mapping.log")
        {
            let _ = writeln!(file, "ERROR: {}", msg);
        }
        return Err(msg);
    }

    // ✅ FIXED: Use deterministic schema lookup
    // Priority: explicit schema_name > task-local QUERY_SCHEMA_NAME > search all schemas (fallback)
    let resolved_schema_name = schema_name
        .map(|s| s.to_string())
        .or_else(super::render_expr::get_current_schema_name);

    log::info!(
        "🔍 map_property_to_column_with_relationship_context: property='{}', node_label='{}', resolved_schema_name={:?}",
        property,
        node_label,
        resolved_schema_name
    );

    let schema = if let Some(sname) = resolved_schema_name {
        log::info!("  ✓ Using explicit schema: {}", sname);
        schemas.get(&sname).ok_or_else(|| {
            let available_schemas: Vec<String> = schemas.keys().cloned().collect();
            let msg = format!(
                "Schema '{}' not found. Available schemas: {}",
                sname,
                available_schemas.join(", ")
            );
            if let Ok(mut file) = OpenOptions::new()
                .create(true)
                .append(true)
                .open("debug_property_mapping.log")
            {
                let _ = writeln!(file, "ERROR: {}", msg);
            }
            msg
        })?
    } else {
        // ❌ ARCHITECTURAL ISSUE: Schema context is missing!
        // This should NEVER happen in production. Schema must be explicitly specified.
        // Each query operates within ONE schema scope - never cross-schema search.

        log::error!(
            "🚨 CRITICAL: map_property_to_column called without schema context for property='{}', node_label='{}'",
            property,
            node_label
        );
        log::error!(
            "   Available schemas: {:?}",
            schemas.keys().collect::<Vec<_>>()
        );
        log::error!(
            "   This indicates a bug in schema context propagation through the rendering pipeline."
        );

        // Fallback: Search all schemas (this is a bug - should never happen)
        // Log the schema that is being used (will help debug the root cause)
        schemas
            .values()
            .find(|s| s.all_node_schemas().contains_key(node_label))
            .ok_or_else(|| {
                let available_schemas: Vec<String> = schemas.keys().cloned().collect();
                let msg = format!(
                    "CRITICAL: Node label '{}' not found. Schema context was missing (no explicit schema_name and QUERY_SCHEMA_NAME task_local not set). Available schemas: {}. This is a bug in schema context propagation.",
                    node_label,
                    available_schemas.join(", ")
                );
                log::error!("{}", msg);
                if let Ok(mut file) = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("debug_property_mapping.log")
                {
                    let _ = writeln!(file, "CRITICAL ERROR: {}", msg);
                }
                msg
            })?
    };

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
        let available: Vec<String> = node_schema.property_mappings.keys().cloned().collect();
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

/// Map a relationship property to its corresponding column name in the schema.
/// This is the relationship equivalent of map_property_to_column_with_schema.
///
/// # Arguments
/// * `property` - The Cypher property name (e.g., "since_date")
/// * `relationship_type` - The relationship type (e.g., "FRIENDS_WITH")
///
/// # Returns
/// * `Ok(column_name)` - The mapped column name (e.g., "since")
/// * `Err(msg)` - If the relationship type or property is not found
pub fn map_relationship_property_to_column(
    property: &str,
    relationship_type: &str,
) -> Result<String, String> {
    // Try to get the schema from the global state
    let schema_lock = crate::server::GLOBAL_SCHEMAS.get().ok_or_else(|| {
        "GLOBAL_SCHEMAS not initialized".to_string()
    })?;

    let schemas = schema_lock.try_read().map_err(|_| {
        "Failed to acquire read lock on GLOBAL_SCHEMAS".to_string()
    })?;

    // Try to get schema from task-local context
    let resolved_schema_name = super::render_expr::get_current_schema_name();

    let schema = if let Some(sname) = resolved_schema_name {
        schemas.get(&sname).ok_or_else(|| {
            format!("Schema '{}' not found", sname)
        })?
    } else {
        // Search all schemas for this relationship type
        schemas.values()
            .find(|s| s.get_relationships_schemas().contains_key(relationship_type))
            .ok_or_else(|| {
                format!("Relationship type '{}' not found in any schema", relationship_type)
            })?
    };

    // Get the relationship schema
    let rel_schema = schema.get_relationships_schema_opt(relationship_type)
        .ok_or_else(|| {
            format!("Relationship type '{}' not found in schema", relationship_type)
        })?;

    // Look up the property in property_mappings
    let column = rel_schema.property_mappings.get(property)
        .ok_or_else(|| {
            let available: Vec<String> = rel_schema.property_mappings.keys().cloned().collect();
            format!(
                "Property '{}' not found in relationship type '{}'. Available properties: {}",
                property,
                relationship_type,
                available.join(", ")
            )
        })?;

    Ok(column.raw().to_string())
}

/// Get node schema by table name
fn get_node_schema_by_table<'a>(
    schema: &'a GraphSchema,
    table_name: &str,
) -> Option<(&'a str, &'a crate::graph_catalog::graph_schema::NodeSchema)> {
    for (label, node_schema) in schema.all_node_schemas() {
        if node_schema.table_name == table_name {
            return Some((label.as_str(), node_schema));
        }
    }
    None
}
