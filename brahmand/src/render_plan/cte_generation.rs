use std::collections::HashMap;

use crate::clickhouse_query_generator::NodeProperty;
use crate::query_planner::logical_plan::LogicalPlan;
use crate::query_planner::logical_expr::LogicalExpr;
use crate::render_plan::render_expr::RenderExpr;
use crate::graph_catalog::graph_schema::GraphSchema;

/// Context for CTE generation - holds property requirements and other metadata
#[derive(Debug, Clone, Default)]
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
}

impl CteGenerationContext {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn get_properties(&self, left_alias: &str, right_alias: &str) -> Vec<NodeProperty> {
        let key = format!("{}-{}", left_alias, right_alias);
        self.variable_length_properties.get(&key).cloned().unwrap_or_default()
    }

    pub(crate) fn set_properties(&mut self, left_alias: &str, right_alias: &str, properties: Vec<NodeProperty>) {
        let key = format!("{}-{}", left_alias, right_alias);
        self.variable_length_properties.insert(key, properties);
    }

    pub(crate) fn get_filter(&self) -> Option<&RenderExpr> {
        self.filter_expr.as_ref()
    }

    pub(crate) fn set_filter(&mut self, filter: RenderExpr) {
        self.filter_expr = Some(filter);
    }

    pub(crate) fn get_end_filters_for_outer_query(&self) -> Option<&RenderExpr> {
        self.end_filters_for_outer_query.as_ref()
    }

    pub(crate) fn set_end_filters_for_outer_query(&mut self, filters: RenderExpr) {
        self.end_filters_for_outer_query = Some(filters);
    }

    pub(crate) fn get_start_cypher_alias(&self) -> Option<&str> {
        self.start_cypher_alias.as_deref()
    }

    pub(crate) fn set_start_cypher_alias(&mut self, alias: String) {
        self.start_cypher_alias = Some(alias);
    }

    pub(crate) fn get_end_cypher_alias(&self) -> Option<&str> {
        self.end_cypher_alias.as_deref()
    }

    pub(crate) fn set_end_cypher_alias(&mut self, alias: String) {
        self.end_cypher_alias = Some(alias);
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
            if let Some(schema_lock) = crate::server::GLOBAL_GRAPH_SCHEMA.get() {
                if let Ok(schema) = schema_lock.try_read() {
                    if let Some((label, _)) = get_node_schema_by_table(&schema, &view_scan.source_table) {
                        return Some(label.to_string());
                    }
                }
            }
            None
        }
        LogicalPlan::Scan(scan) => {
            // For regular scans, try to infer from table name
            if let Some(schema_lock) = crate::server::GLOBAL_GRAPH_SCHEMA.get() {
                if let Ok(schema) = schema_lock.try_read() {
                    if let Some(table_name) = &scan.table_name {
                        if let Some((label, _)) = get_node_schema_by_table(&schema, table_name) {
                            return Some(label.to_string());
                        }
                    }
                }
            }
            None
        }
        LogicalPlan::Filter(filter) => extract_node_label_from_viewscan(&filter.input),
        LogicalPlan::Projection(proj) => extract_node_label_from_viewscan(&proj.input),
        _ => None,
    }
}

/// Analyze the plan to determine what properties are needed for variable-length CTEs
pub(crate) fn analyze_property_requirements(plan: &LogicalPlan) -> CteGenerationContext {
    let mut context = CteGenerationContext::new();

    // Find variable-length relationships and their required properties
    if let Some((left_alias, right_alias, left_label, right_label, _rel_type)) = get_variable_length_info(plan) {
        let properties = extract_var_len_properties(plan, &left_alias, &right_alias, &left_label, &right_label);
        context.set_properties(&left_alias, &right_alias, properties);
    }

    context
}

/// Extract property requirements from projection for variable-length paths
/// Returns a vector of properties that need to be included in the CTE
/// Recursively searches through the plan to find the Projection node
pub(crate) fn extract_var_len_properties(
    plan: &LogicalPlan,
    left_alias: &str,
    right_alias: &str,
    left_label: &str,
    right_label: &str
) -> Vec<NodeProperty> {
    let mut properties = Vec::new();

    // Find the projection in the plan (recursively)
    match plan {
        LogicalPlan::Projection(proj) => {
            for item in &proj.items {
                // Check if this is a property access expression
                if let LogicalExpr::PropertyAccessExp(prop_acc) = &item.expression {
                    let node_alias = prop_acc.table_alias.0.as_str();
                    let property_name = prop_acc.column.0.as_str();

                    // Determine if this is for the left or right node
                    if node_alias == left_alias || node_alias == right_alias {
                        // Determine which node label to use
                        let node_label = if node_alias == left_alias {
                            left_label
                        } else {
                            right_label
                        };

                        // Map property name to actual column name using schema
                        let column_name = map_property_to_column_with_schema(property_name, node_label);
                        // Use property_name for CTE column alias, not Cypher SELECT alias
                        // E.g., for "a.name AS start", use "name" not "start" for CTE column
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
        // Recursively search in child plans
        LogicalPlan::Filter(filter) => return extract_var_len_properties(&filter.input, left_alias, right_alias, left_label, right_label),
        LogicalPlan::OrderBy(order_by) => return extract_var_len_properties(&order_by.input, left_alias, right_alias, left_label, right_label),
        LogicalPlan::Skip(skip) => return extract_var_len_properties(&skip.input, left_alias, right_alias, left_label, right_label),
        LogicalPlan::Limit(limit) => return extract_var_len_properties(&limit.input, left_alias, right_alias, left_label, right_label),
        LogicalPlan::GroupBy(group_by) => return extract_var_len_properties(&group_by.input, left_alias, right_alias, left_label, right_label),
        LogicalPlan::GraphJoins(joins) => return extract_var_len_properties(&joins.input, left_alias, right_alias, left_label, right_label),
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
fn get_variable_length_info(plan: &LogicalPlan) -> Option<(String, String, String, String, String)> {
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

/// Map Cypher property name to actual column name
/// TODO: This should look up the actual schema from GraphSchema
fn map_property_to_column(property: &str) -> String {
    match property {
        "name" => "full_name".to_string(),  // For users
        "email" => "email_address".to_string(),
        _ => property.to_string(), // fallback to property name itself
    }
}

/// Schema-aware property mapping using GraphSchema
/// Map a property to column with schema awareness
pub(crate) fn map_property_to_column_with_schema(property: &str, node_label: &str) -> String {
    // Try to get the view config from the global state
    if let Some(config_lock) = crate::server::GLOBAL_VIEW_CONFIG.get() {
        if let Ok(config) = config_lock.try_read() {
            // Find the node mapping for this label
            for view in &config.views {
                if let Some(node_mapping) = view.nodes.get(node_label) {
                    // Check if there's a property mapping
                    if let Some(column) = node_mapping.property_mappings.get(property) {
                        return column.clone();
                    }
                }
            }
        }
    }

    // Fallback to the hardcoded mapping
    map_property_to_column(property)
}

/// Get node schema by table name
fn get_node_schema_by_table<'a>(schema: &'a GraphSchema, table_name: &str) -> Option<(&'a str, &'a crate::graph_catalog::graph_schema::NodeSchema)> {
    for (label, node_schema) in schema.get_nodes_schemas() {
        if node_schema.table_name == table_name {
            return Some((label.as_str(), node_schema));
        }
    }
    None
}