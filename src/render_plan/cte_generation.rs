//! CTE generation utilities for variable-length path queries
//!
//! Some structures and methods in this module are reserved for future use.
#![allow(dead_code)]

use std::collections::HashMap;

use crate::clickhouse_query_generator::NodeProperty;
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::query_planner::logical_expr::LogicalExpr;
use crate::query_planner::logical_plan::LogicalPlan;
use crate::render_plan::render_expr::RenderExpr;

/// Context for CTE generation - holds property requirements and other metadata
#[derive(Debug, Clone)]
pub struct CteGenerationContext {
    /// Properties needed for variable-length paths, keyed by "left_alias-right_alias"
    variable_length_properties: HashMap<String, Vec<NodeProperty>>,
    /// WHERE filter expression to apply to variable-length CTEs
    filter_expr: Option<RenderExpr>,
    /// End node filters to be applied in the outer query for variable-length paths
    end_filters_for_outer_query: Option<RenderExpr>,
    /// Cypher aliases for start and end nodes (for filter rewriting)
    start_cypher_alias: Option<String>,
    end_cypher_alias: Option<String>,
    /// Graph schema for this query (enables multi-schema support)
    schema: Option<GraphSchema>,
}

impl Default for CteGenerationContext {
    fn default() -> Self {
        Self {
            variable_length_properties: HashMap::new(),
            filter_expr: None,
            end_filters_for_outer_query: None,
            start_cypher_alias: None,
            end_cypher_alias: None,
            schema: None,
        }
    }
}

impl CteGenerationContext {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn with_schema(schema: GraphSchema) -> Self {
        Self {
            variable_length_properties: HashMap::new(),
            filter_expr: None,
            end_filters_for_outer_query: None,
            start_cypher_alias: None,
            end_cypher_alias: None,
            schema: Some(schema),
        }
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

    pub(crate) fn get_end_filters_for_outer_query(&self) -> Option<&RenderExpr> {
        self.end_filters_for_outer_query.as_ref()
    }

    // 🆕 IMMUTABLE: Returns new context
    pub(crate) fn with_end_filters_for_outer_query(mut self, filters: RenderExpr) -> Self {
        self.end_filters_for_outer_query = Some(filters);
        self
    }

    // 🔧 DEPRECATED: Keep for compatibility
    #[deprecated(note = "Use with_end_filters_for_outer_query() instead")]
    pub(crate) fn set_end_filters_for_outer_query(&mut self, filters: RenderExpr) {
        self.end_filters_for_outer_query = Some(filters);
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

    // 🆕 MERGE HELPER: Merge another context's end filters into this one
    pub(crate) fn merge_end_filters(mut self, other: &CteGenerationContext) -> Self {
        if let Some(filters) = other.get_end_filters_for_outer_query() {
            self.end_filters_for_outer_query = Some(filters.clone());
        }
        self
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

/// Extract node label from a ViewScan plan
fn extract_node_label_from_viewscan(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::ViewScan(view_scan) => {
            // Try to get the label from the schema using the table name
            if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
                if let Ok(schemas) = schemas_lock.try_read() {
                    if let Some(schema) = schemas.get("default") {
                        if let Some((label, _)) =
                            get_node_schema_by_table(schema, &view_scan.source_table)
                        {
                            return Some(label.to_string());
                        }
                    }
                }
            }
            None
        }
        LogicalPlan::Scan(scan) => {
            // For regular scans, try to infer from table name
            if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
                if let Ok(schemas) = schemas_lock.try_read() {
                    if let Some(schema) = schemas.get("default") {
                        if let Some(table_name) = &scan.table_name {
                            if let Some((label, _)) = get_node_schema_by_table(schema, table_name) {
                                return Some(label.to_string());
                            }
                        }
                    }
                }
            }
            None
        }
        LogicalPlan::GraphNode(node) => {
            // First try to get label directly from the GraphNode (for denormalized nodes)
            if let Some(label) = &node.label {
                return Some(label.clone());
            }
            // Otherwise, recurse into input
            extract_node_label_from_viewscan(&node.input)
        }
        LogicalPlan::Filter(filter) => extract_node_label_from_viewscan(&filter.input),
        LogicalPlan::Projection(proj) => extract_node_label_from_viewscan(&proj.input),
        _ => None,
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
        let properties =
            extract_var_len_properties(plan, &left_alias, &right_alias, &left_label, &right_label, Some(&rel_type));
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
                let property_name = prop.column.0.raw();
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
                                    if let Some(schema) = schemas.get("default") {
                                        if let Some(node_schema) =
                                            schema.get_nodes_schemas().get(node_label)
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
                            let column_name =
                                map_property_to_column_with_relationship_context(property_name, node_label, relationship_type, None)
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
        LogicalPlan::Scan(scan) => scan.table_alias.clone(),
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
    map_property_to_column_with_relationship_context(property, node_label, None, None)
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

    // 🔥 PRAGMATIC FIX: Search for the node label in ALL loaded schemas
    // (In the future, we should pass schema_name through the rendering pipeline)
    let schema = schemas
        .values()
        .find(|s| s.get_nodes_schemas().contains_key(node_label))
        .ok_or_else(|| {
            let available_schemas: Vec<String> = schemas.keys().map(|s| s.clone()).collect();
            let msg = format!(
                "Node label '{}' not found in any loaded schema. Available schemas: {}",
                node_label,
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
        })?;

    // Get the node schema first
    let node_schema = schema.get_nodes_schemas().get(node_label).ok_or_else(|| {
        let available: Vec<String> = schema
            .get_nodes_schemas()
            .keys()
            .map(|s| s.clone())
            .collect();
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
        let available: Vec<String> = node_schema
            .property_mappings
            .keys()
            .map(|s| s.clone())
            .collect();
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

/// Get node schema by table name
fn get_node_schema_by_table<'a>(
    schema: &'a GraphSchema,
    table_name: &str,
) -> Option<(&'a str, &'a crate::graph_catalog::graph_schema::NodeSchema)> {
    for (label, node_schema) in schema.get_nodes_schemas() {
        if node_schema.table_name == table_name {
            return Some((label.as_str(), node_schema));
        }
    }
    None
}
