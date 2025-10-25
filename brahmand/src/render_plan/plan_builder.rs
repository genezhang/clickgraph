use std::sync::Arc;
use std::fs::OpenOptions;
use std::io::Write;
use crate::query_planner::logical_plan::LogicalPlan;
use crate::query_planner::logical_expr::Direction;
use crate::query_planner::plan_ctx::PlanCtx;
use crate::clickhouse_query_generator::variable_length_cte::{VariableLengthCteGenerator, ChainedJoinGenerator};

use super::errors::RenderBuildError;
use super::render_expr::{
    AggregateFnCall, Column, ColumnAlias, Literal, Operator, OperatorApplication, PropertyAccess, RenderExpr, ScalarFnCall, TableAlias,
};
use super::{
    Cte, CteItems, FilterItems, FromTable, FromTableItem, GroupByExpressions, Join, JoinItems, JoinType,
    LimitItem, OrderByItem, OrderByItems, RenderPlan, SelectItem, SelectItems, SkipItem, Union,
    UnionItems, ViewTableRef, view_table_ref::{view_ref_to_from_table, from_table_to_view_ref},
};

pub type RenderPlanBuilderResult<T> = Result<T, super::errors::RenderBuildError>;

/// Helper function to extract the node alias from a GraphNode
fn extract_node_alias(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::GraphNode(node) => Some(node.alias.clone()),
        LogicalPlan::Filter(filter) => extract_node_alias(&filter.input),
        LogicalPlan::Projection(proj) => extract_node_alias(&proj.input),
        _ => None,
    }
}

/// Helper function to extract the actual table name from a LogicalPlan node
/// Recursively traverses the plan tree to find the Scan or ViewScan node
fn extract_table_name(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::Scan(scan) => scan.table_name.clone(),
        LogicalPlan::ViewScan(view_scan) => Some(view_scan.source_table.clone()),
        LogicalPlan::GraphNode(node) => extract_table_name(&node.input),
        LogicalPlan::GraphRel(rel) => extract_table_name(&rel.center),
        LogicalPlan::Filter(filter) => extract_table_name(&filter.input),
        LogicalPlan::Projection(proj) => extract_table_name(&proj.input),
        _ => None,
    }
}

/// Convert a RenderExpr to a SQL string for use in CTE WHERE clauses
fn render_expr_to_sql_string(expr: &RenderExpr, alias_mapping: &[(String, String)]) -> String {
    match expr {
        RenderExpr::Column(col) => col.0.clone(),
        RenderExpr::TableAlias(alias) => alias.0.clone(),
        RenderExpr::ColumnAlias(alias) => alias.0.clone(),
        RenderExpr::Literal(lit) => match lit {
            super::render_expr::Literal::String(s) => format!("'{}'", s.replace("'", "''")),
            super::render_expr::Literal::Integer(i) => i.to_string(),
            super::render_expr::Literal::Float(f) => f.to_string(),
            super::render_expr::Literal::Boolean(b) => b.to_string(),
            super::render_expr::Literal::Null => "NULL".to_string(),
        },
        RenderExpr::PropertyAccessExp(prop) => {
            // Convert property access to table.column format
            // Apply alias mapping to convert Cypher aliases to CTE aliases
            println!("DEBUG: render_expr_to_sql_string PropertyAccessExp - table_alias: '{}', column: '{}'", prop.table_alias.0, prop.column.0);
            println!("DEBUG: alias_mapping: {:?}", alias_mapping);
            let table_alias = alias_mapping.iter()
                .find(|(cypher, _)| {
                    println!("DEBUG: comparing '{}' == '{}'", cypher, &prop.table_alias.0);
                    *cypher == prop.table_alias.0
                })
                .map(|(_, cte)| {
                    println!("DEBUG: found mapping, using CTE alias: '{}'", cte);
                    cte.clone()
                })
                .unwrap_or_else(|| {
                    println!("DEBUG: no mapping found, using original: '{}'", prop.table_alias.0);
                    prop.table_alias.0.clone()
                });
            format!("{}.{}", table_alias, prop.column.0)
        }
        RenderExpr::OperatorApplicationExp(op) => {
            let operands: Vec<String> = op.operands.iter()
                .map(|operand| render_expr_to_sql_string(operand, alias_mapping))
                .collect();
            match op.operator {
                Operator::Equal => format!("{} = {}", operands[0], operands[1]),
                Operator::NotEqual => format!("{} != {}", operands[0], operands[1]),
                Operator::LessThan => format!("{} < {}", operands[0], operands[1]),
                Operator::GreaterThan => format!("{} > {}", operands[0], operands[1]),
                Operator::LessThanEqual => format!("{} <= {}", operands[0], operands[1]),
                Operator::GreaterThanEqual => format!("{} >= {}", operands[0], operands[1]),
                Operator::And => format!("({})", operands.join(" AND ")),
                Operator::Or => format!("({})", operands.join(" OR ")),
                Operator::Not => format!("NOT ({})", operands[0]),
                Operator::Addition => format!("{} + {}", operands[0], operands[1]),
                Operator::Subtraction => format!("{} - {}", operands[0], operands[1]),
                Operator::Multiplication => format!("{} * {}", operands[0], operands[1]),
                Operator::Division => format!("{} / {}", operands[0], operands[1]),
                Operator::ModuloDivision => format!("{} % {}", operands[0], operands[1]),
                _ => format!("{} {:?} {}", operands[0], op.operator, operands[1]), // fallback
            }
        }
        RenderExpr::Parameter(param) => format!("${}", param),
        RenderExpr::ScalarFnCall(func) => {
            let args: Vec<String> = func.args.iter()
                .map(|arg| render_expr_to_sql_string(arg, alias_mapping))
                .collect();
            format!("{}({})", func.name, args.join(", "))
        }
        RenderExpr::AggregateFnCall(agg) => {
            let args: Vec<String> = agg.args.iter()
                .map(|arg| render_expr_to_sql_string(arg, alias_mapping))
                .collect();
            format!("{}({})", agg.name, args.join(", "))
        }
        _ => "TRUE".to_string(), // fallback for unsupported expressions
    }
}

/// Helper to extract ID column name from ViewScan
fn extract_id_column(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::ViewScan(view_scan) => Some(view_scan.id_column.clone()),
        LogicalPlan::GraphNode(node) => extract_id_column(&node.input),
        LogicalPlan::GraphRel(rel) => extract_id_column(&rel.center),
        LogicalPlan::Filter(filter) => extract_id_column(&filter.input),
        LogicalPlan::Projection(proj) => extract_id_column(&proj.input),
        _ => None,
    }
}

/// Structure to hold relationship column information
#[derive(Debug, Clone)]
struct RelationshipColumns {
    from_column: String,
    to_column: String,
}

/// Map Cypher label to actual source table name
/// Tries schema lookup first, then falls back to hardcoded mappings
fn label_to_table_name(label: &str) -> String {
    // Try to get from schema first
    if let Some((table, _id_col)) = get_node_info_from_schema(label) {
        return table;
    }
    
    // Fallback to hardcoded mappings
    match label {
        "user" | "User" => "users".to_string(),
        "customer" | "Customer" => "customers".to_string(),
        "product" | "Product" => "products".to_string(),
        "post" | "Post" => "posts".to_string(),
        _ => label.to_string(), // fallback to label itself
    }
}

/// Map table name to its ID column
/// Tries schema lookup first, then falls back to hardcoded mappings
fn table_to_id_column_for_label(label: &str) -> String {
    // Try to get from schema first
    if let Some((_table, id_col)) = get_node_info_from_schema(label) {
        return id_col;
    }
    
    // Fallback based on label
    let table = label_to_table_name(label);
    table_to_id_column(&table)
}

/// Map table name to its ID column (internal helper)
fn table_to_id_column(table: &str) -> String {
    match table {
        "users" => "user_id".to_string(),
        "customers" => "customer_id".to_string(),
        "products" => "product_id".to_string(),
        "posts" => "post_id".to_string(),
        _ => format!("{}_id", table), // fallback to table_name + "_id"
    }
}

/// Map relationship type to actual relationship table name
/// Looks up the relationship schema from GLOBAL_GRAPH_SCHEMA
/// Falls back to hardcoded mappings for backwards compatibility
fn rel_type_to_table_name(rel_type: &str) -> String {
    // Try to get table name from schema first
    if let Some(schema_lock) = crate::server::GLOBAL_GRAPH_SCHEMA.get() {
        if let Ok(schema) = schema_lock.try_read() {
            if let Ok(rel_schema) = schema.get_rel_schema(rel_type) {
                return rel_schema.table_name.clone();
            }
        }
    }
    
    // Fallback to hardcoded mappings for backwards compatibility
    // This ensures existing queries still work even without YAML config
    match rel_type {
        "FRIEND" | "FRIENDS_WITH" => "friendships".to_string(),
        "FOLLOWS" => "user_follows".to_string(),
        "AUTHORED" => "posts".to_string(),
        "LIKED" => "post_likes".to_string(),
        "PURCHASED" => "orders".to_string(),
        _ => rel_type.to_string(), // ultimate fallback: use type name as table name
    }
}

/// Map multiple relationship types to their actual table names
/// For [:TYPE1|TYPE2] patterns, returns all corresponding table names
fn rel_types_to_table_names(rel_types: &[String]) -> Vec<String> {
    rel_types.iter().map(|rel_type| rel_type_to_table_name(rel_type)).collect()
}

/// Extract relationship columns from plan or table name
/// TODO: This should look up the actual schema from GraphSchema
/// For now, uses hardcoded mappings for known relationship types
fn extract_relationship_columns_from_table(table_name: &str) -> RelationshipColumns {
    // Try to get from schema by table name first
    if let Some((from_col, to_col)) = get_relationship_columns_by_table(table_name) {
        return RelationshipColumns {
            from_column: from_col,
            to_column: to_col,
        };
    }
    
    // Also try by relationship type name (in case table_name is actually a type like "FRIEND")
    if let Some((from_col, to_col)) = get_relationship_columns_from_schema(table_name) {
        return RelationshipColumns {
            from_column: from_col,
            to_column: to_col,
        };
    }
    
    // Fallback to hardcoded mappings for known relationship types
    match table_name {
        "user_follows" | "FOLLOWS" => RelationshipColumns {
            from_column: "follower_id".to_string(),
            to_column: "followed_id".to_string(),
        },
        "friendships" | "FRIEND" => RelationshipColumns {
            from_column: "user1_id".to_string(),
            to_column: "user2_id".to_string(),
        },
        "posts" | "AUTHORED" => RelationshipColumns {
            from_column: "author_id".to_string(),
            to_column: "post_id".to_string(),
        },
        "post_likes" | "LIKED" => RelationshipColumns {
            from_column: "user_id".to_string(),
            to_column: "post_id".to_string(),
        },
        "orders" | "PURCHASED" => RelationshipColumns {
            from_column: "user_id".to_string(),
            to_column: "product_id".to_string(),
        },
        _ => RelationshipColumns {
            from_column: "from_node_id".to_string(),
            to_column: "to_node_id".to_string(),
        },
    }
}

/// Extract relationship columns from ViewScan (for relationship tables)
fn extract_relationship_columns(plan: &LogicalPlan) -> Option<RelationshipColumns> {
    match plan {
        LogicalPlan::ViewScan(view_scan) => {
            // Use actual columns from ViewScan if available
            if let (Some(from_col), Some(to_col)) = (&view_scan.from_column, &view_scan.to_column) {
                return Some(RelationshipColumns {
                    from_column: from_col.clone(),
                    to_column: to_col.clone(),
                });
            }
            
            // Otherwise, look up by table name
            Some(extract_relationship_columns_from_table(&view_scan.source_table))
        },
        LogicalPlan::Scan(scan) => {
            // For Scan nodes, look up by table name
            scan.table_name.as_ref().map(|name| extract_relationship_columns_from_table(name))
        },
        LogicalPlan::GraphRel(rel) => extract_relationship_columns(&rel.center),
        LogicalPlan::Filter(filter) => extract_relationship_columns(&filter.input),
        _ => None,
    }
}

/// Check if the plan contains a variable-length relationship and return node aliases
/// Returns (left_alias, right_alias) if found
fn has_variable_length_rel(plan: &LogicalPlan) -> Option<(String, String)> {
    match plan {
        LogicalPlan::GraphRel(rel) if rel.variable_length.is_some() => {
            Some((rel.left_connection.clone(), rel.right_connection.clone()))
        }
        LogicalPlan::GraphNode(node) => has_variable_length_rel(&node.input),
        LogicalPlan::Filter(filter) => has_variable_length_rel(&filter.input),
        LogicalPlan::Projection(proj) => has_variable_length_rel(&proj.input),
        LogicalPlan::GraphJoins(joins) => has_variable_length_rel(&joins.input),
        LogicalPlan::GroupBy(gb) => has_variable_length_rel(&gb.input),
        LogicalPlan::OrderBy(ob) => has_variable_length_rel(&ob.input),
        LogicalPlan::Skip(skip) => has_variable_length_rel(&skip.input),
        LogicalPlan::Limit(limit) => has_variable_length_rel(&limit.input),
        LogicalPlan::Cte(cte) => has_variable_length_rel(&cte.input),
        _ => None,
    }
}

/// Extract start and end node filters from a filter expression for variable-length paths
fn extract_start_end_filters(
    filter_expr: &RenderExpr,
    left_alias: &str,
    right_alias: &str,
) -> (Option<String>, Option<String>, Option<RenderExpr>) {
    use super::render_expr::{OperatorApplication, Operator};
    
    match filter_expr {
        RenderExpr::OperatorApplicationExp(op_app) if op_app.operator == Operator::And => {
            // AND expression - check each operand
            let mut start_filters = vec![];
            let mut end_filters = vec![];
            let mut other_filters = vec![];
            
            for operand in &op_app.operands {
                let (start_f, end_f, other_f) = extract_start_end_filters(operand, left_alias, right_alias);
                if let Some(sf) = start_f { start_filters.push(sf); }
                if let Some(ef) = end_f { end_filters.push(ef); }
                if let Some(of) = other_f { other_filters.push(of); }
            }
            
            let start_filter = if start_filters.is_empty() { None } else { Some(start_filters.join(" AND ")) };
            let end_filter = if end_filters.is_empty() { None } else { Some(end_filters.join(" AND ")) };
            let remaining_filter = if other_filters.is_empty() { 
                None 
            } else if other_filters.len() == 1 {
                Some(other_filters.into_iter().next().unwrap())
            } else {
                Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::And,
                    operands: other_filters,
                }))
            };
            
            (start_filter, end_filter, remaining_filter)
        }
        _ => {
            // Check if this filter references start or end node
            if references_alias(filter_expr, left_alias) {
                // Convert to SQL string for start filter
                (Some(filter_expr_to_sql(filter_expr, left_alias, "start_")), None, None)
            } else if references_alias(filter_expr, right_alias) {
                // Convert to SQL string for end filter
                (None, Some(filter_expr_to_sql(filter_expr, right_alias, "end_")), None)
            } else {
                // Keep as general filter
                (None, None, Some(filter_expr.clone()))
            }
        }
    }
}

/// Check if a filter expression references a specific alias
fn references_alias(expr: &RenderExpr, alias: &str) -> bool {
    match expr {
        RenderExpr::PropertyAccessExp(prop) => prop.table_alias.0 == alias,
        RenderExpr::OperatorApplicationExp(op_app) => op_app.operands.iter().any(|op| references_alias(op, alias)),
        RenderExpr::ScalarFnCall(fn_call) => fn_call.args.iter().any(|arg| references_alias(arg, alias)),
        _ => false,
    }
}

/// Convert a filter expression to SQL string for CTE filters
fn filter_expr_to_sql(expr: &RenderExpr, alias: &str, prefix: &str) -> String {
    match expr {
        RenderExpr::OperatorApplicationExp(op_app) if op_app.operator == Operator::Equal && op_app.operands.len() == 2 => {
            if let (RenderExpr::PropertyAccessExp(prop), RenderExpr::Literal(lit)) = (&op_app.operands[0], &op_app.operands[1]) {
                if prop.table_alias.0 == alias {
                    let column = format!("{}{}", prefix, prop.column.0);
                    match lit {
                        super::render_expr::Literal::String(s) => format!("{} = '{}'", column, s),
                        super::render_expr::Literal::Integer(n) => format!("{} = {}", column, n),
                        super::render_expr::Literal::Float(f) => format!("{} = {}", column, f),
                        _ => "true".to_string(), // fallback
                    }
                } else {
                    "true".to_string() // fallback
                }
            } else {
                "true".to_string() // fallback
            }
        }
        _ => "true".to_string() // fallback for complex expressions
    }
}

fn get_path_variable(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::GraphRel(rel) if rel.variable_length.is_some() => {
            rel.path_variable.clone()
        }
        LogicalPlan::GraphNode(node) => get_path_variable(&node.input),
        LogicalPlan::Filter(filter) => get_path_variable(&filter.input),
        LogicalPlan::Projection(proj) => get_path_variable(&proj.input),
        LogicalPlan::GraphJoins(joins) => get_path_variable(&joins.input),
        LogicalPlan::GroupBy(gb) => get_path_variable(&gb.input),
        LogicalPlan::OrderBy(ob) => get_path_variable(&ob.input),
        LogicalPlan::Skip(skip) => get_path_variable(&skip.input),
        LogicalPlan::Limit(limit) => get_path_variable(&limit.input),
        LogicalPlan::Cte(cte) => get_path_variable(&cte.input),
        _ => None,
    }
}

/// Rewrite expressions for outer query context in variable-length paths
/// Maps Cypher aliases to table aliases (start_node, end_node)
fn rewrite_expr_for_outer_query(
    expr: &super::render_expr::RenderExpr,
    left_alias: &str,
    right_alias: &str,
) -> super::render_expr::RenderExpr {
    use super::render_expr::{RenderExpr, PropertyAccess, TableAlias, Column};
    
    match expr {
        RenderExpr::PropertyAccessExp(prop_access) => {
            let node_alias = &prop_access.table_alias.0;
            let property = &prop_access.column.0;
            
            // Check if this is referencing the left or right node
            if node_alias == left_alias {
                // Left node reference -> start_node
                return RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("start_node".to_string()),
                    column: Column(property.clone()),
                });
            } else if node_alias == right_alias {
                // Right node reference -> end_node
                return RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("end_node".to_string()),
                    column: Column(property.clone()),
                });
            }
            expr.clone()
        }
        RenderExpr::OperatorApplicationExp(op) => {
            // Recursively rewrite operands
            let rewritten_operands = op.operands.iter()
                .map(|operand| rewrite_expr_for_outer_query(operand, left_alias, right_alias))
                .collect();
            RenderExpr::OperatorApplicationExp(super::render_expr::OperatorApplication {
                operator: op.operator.clone(),
                operands: rewritten_operands,
            })
        }
        RenderExpr::ScalarFnCall(fn_call) => {
            // Recursively rewrite function arguments
            let rewritten_args = fn_call.args.iter()
                .map(|arg| rewrite_expr_for_outer_query(arg, left_alias, right_alias))
                .collect();
            RenderExpr::ScalarFnCall(super::render_expr::ScalarFnCall {
                name: fn_call.name.clone(),
                args: rewritten_args,
            })
        }
        RenderExpr::AggregateFnCall(agg) => {
            // Recursively rewrite aggregate function arguments
            let rewritten_args = agg.args.iter()
                .map(|arg| rewrite_expr_for_outer_query(arg, left_alias, right_alias))
                .collect();
            RenderExpr::AggregateFnCall(super::render_expr::AggregateFnCall {
                name: agg.name.clone(),
                args: rewritten_args,
            })
        }
        // For other expression types (literals, stars, etc.), no rewriting needed
        _ => expr.clone(),
    }
}

use crate::clickhouse_query_generator::NodeProperty;
use crate::query_planner::logical_expr::LogicalExpr;
use std::collections::HashMap;

/// Context for CTE generation - holds property requirements and other metadata
#[derive(Debug, Clone, Default)]
pub(crate) struct CteGenerationContext {
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
    fn new() -> Self {
        Self::default()
    }
    
    fn get_properties(&self, left_alias: &str, right_alias: &str) -> Vec<NodeProperty> {
        let key = format!("{}-{}", left_alias, right_alias);
        self.variable_length_properties.get(&key).cloned().unwrap_or_default()
    }
    
    fn set_properties(&mut self, left_alias: &str, right_alias: &str, properties: Vec<NodeProperty>) {
        let key = format!("{}-{}", left_alias, right_alias);
        self.variable_length_properties.insert(key, properties);
    }
    
    fn get_filter(&self) -> Option<&RenderExpr> {
        self.filter_expr.as_ref()
    }
    
    fn set_filter(&mut self, filter: RenderExpr) {
        self.filter_expr = Some(filter);
    }
    
    fn get_end_filters_for_outer_query(&self) -> Option<&RenderExpr> {
        let result = self.end_filters_for_outer_query.as_ref();
        result
    }
    
    fn set_end_filters_for_outer_query(&mut self, filters: RenderExpr) {
        self.end_filters_for_outer_query = Some(filters);
    }
    
    fn get_start_cypher_alias(&self) -> Option<&str> {
        self.start_cypher_alias.as_deref()
    }
    
    fn set_start_cypher_alias(&mut self, alias: String) {
        self.start_cypher_alias = Some(alias);
    }
    
    fn get_end_cypher_alias(&self) -> Option<&str> {
        self.end_cypher_alias.as_deref()
    }
    
    fn set_end_cypher_alias(&mut self, alias: String) {
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

/// Extract node label from ViewScan in the plan
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
            // For Scan nodes, try to get from table name
            scan.table_name.as_ref().and_then(|table| {
                if let Some(schema_lock) = crate::server::GLOBAL_GRAPH_SCHEMA.get() {
                    if let Ok(schema) = schema_lock.try_read() {
                        if let Some((label, _)) = get_node_schema_by_table(&schema, table) {
                            return Some(label.to_string());
                        }
                    }
                }
                None
            })
        }
        LogicalPlan::GraphNode(node) => extract_node_label_from_viewscan(&node.input),
        LogicalPlan::Filter(filter) => extract_node_label_from_viewscan(&filter.input),
        _ => None,
    }
}

/// Get node schema by table name
fn get_node_schema_by_table<'a>(schema: &'a crate::graph_catalog::graph_schema::GraphSchema, table_name: &str) -> Option<(&'a str, &'a crate::graph_catalog::graph_schema::NodeSchema)> {
    for (label, node_schema) in schema.get_nodes_schemas() {
        if node_schema.table_name == table_name {
            return Some((label.as_str(), node_schema));
        }
    }
    None
}

/// Get variable length info from the plan
/// Returns (left_alias, right_alias, left_label, right_label, rel_type) if a variable-length relationship is found
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

/// Extract alias from a plan node
fn extract_alias_from_plan(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::GraphNode(node) => Some(node.alias.clone()),
        LogicalPlan::Scan(scan) => scan.table_alias.clone(),
        LogicalPlan::ViewScan(_) => None, // ViewScan doesn't have an alias field
        _ => None,
    }
}

/// Analyze the plan to determine what properties are needed for variable-length CTEs
fn analyze_property_requirements(plan: &LogicalPlan) -> CteGenerationContext {
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
fn extract_var_len_properties(
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
/// Looks up the property mapping from the schema for a given node label
fn map_property_to_column_with_schema(property: &str, node_label: &str) -> String {
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

/// Get relationship columns from schema by relationship type
/// Returns (from_column, to_column) for a given relationship type
fn get_relationship_columns_from_schema(rel_type: &str) -> Option<(String, String)> {
    if let Some(schema_lock) = crate::server::GLOBAL_GRAPH_SCHEMA.get() {
        if let Ok(schema) = schema_lock.try_read() {
            if let Ok(rel_schema) = schema.get_rel_schema(rel_type) {
                return Some((
                    rel_schema.from_column.clone(),  // Use column names, not node types!
                    rel_schema.to_column.clone(),
                ));
            }
        }
    }
    None
}

/// Get relationship columns from schema by table name
/// Searches all relationship schemas to find one with matching table name
fn get_relationship_columns_by_table(table_name: &str) -> Option<(String, String)> {
    if let Some(schema_lock) = crate::server::GLOBAL_GRAPH_SCHEMA.get() {
        if let Ok(schema) = schema_lock.try_read() {
            // Search through all relationship schemas for one with matching table name
            for (_key, rel_schema) in schema.get_relationships_schemas().iter() {
                if rel_schema.table_name == table_name {
                    return Some((
                        rel_schema.from_column.clone(),  // Use column names!
                        rel_schema.to_column.clone(),
                    ));
                }
            }
        }
    }
    None
}

/// Get node table name and ID column from schema
/// Returns (table_name, id_column) for a given node label
fn get_node_info_from_schema(node_label: &str) -> Option<(String, String)> {
    if let Some(schema_lock) = crate::server::GLOBAL_GRAPH_SCHEMA.get() {
        if let Ok(schema) = schema_lock.try_read() {
            if let Ok(node_schema) = schema.get_node_schema(node_label) {
                return Some((
                    node_schema.table_name.clone(),
                    node_schema.node_id.column.clone(),
                ));
            }
        }
    }
    None
}

/// Categorized filters for shortest path queries
#[derive(Debug, Clone)]
struct CategorizedFilters {
    start_node_filters: Option<RenderExpr>,
    end_node_filters: Option<RenderExpr>,
    relationship_filters: Option<RenderExpr>,
    path_function_filters: Option<RenderExpr>, // Filters on path functions like length(p), nodes(p)
}

/// Convert RenderExpr to SQL string for end node filters in outer CTE
/// Maps Cypher aliases to database column references (e.g., "a.name" -> "end_node.full_name")
fn render_end_filter_to_column_alias(
    expr: &RenderExpr,
    start_cypher_alias: &str,
    end_cypher_alias: &str,
    start_node_label: &str,
    end_node_label: &str,
) -> String {
    println!("DEBUG: render_end_filter_to_column_alias called with start_cypher_alias='{}', end_cypher_alias='{}'", start_cypher_alias, end_cypher_alias);
    
    match expr {
        RenderExpr::PropertyAccessExp(prop) => {
            let table_alias = &prop.table_alias.0;
            let column = &prop.column.0;
            println!("DEBUG: PropertyAccessExp: table_alias='{}', column='{}', start_cypher_alias='{}', end_cypher_alias='{}'", 
                table_alias, column, start_cypher_alias, end_cypher_alias);
            
            // Map Cypher aliases to database column references
            if table_alias == end_cypher_alias {
                let mapped_column = map_property_to_column_with_schema(column, end_node_label);
                let result = format!("end_node.{}", mapped_column);
                println!("DEBUG: Mapped to end column reference: {}", result);
                result
            } else if table_alias == start_cypher_alias {
                let mapped_column = map_property_to_column_with_schema(column, start_node_label);
                let result = format!("start_node.{}", mapped_column);
                println!("DEBUG: Mapped to start column reference: {}", result);
                result
            } else {
                // Fallback: use as-is
                let result = format!("{}.{}", table_alias, column);
                println!("DEBUG: Fallback: {}", result);
                result
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            use super::render_expr::Operator;
            
            let operator_sql = match op.operator {
                Operator::Equal => "=",
                Operator::NotEqual => "!=",
                Operator::LessThan => "<",
                Operator::GreaterThan => ">",
                Operator::LessThanEqual => "<=",
                Operator::GreaterThanEqual => ">=",
                Operator::And => "AND",
                Operator::Or => "OR",
                Operator::Not => "NOT",
                _ => "=", // Fallback
            };
            
            if op.operands.len() == 1 {
                format!("{} {}", operator_sql, render_end_filter_to_column_alias(&op.operands[0], start_cypher_alias, end_cypher_alias, start_node_label, end_node_label))
            } else if op.operands.len() == 2 {
                format!("{} {} {}", 
                    render_end_filter_to_column_alias(&op.operands[0], start_cypher_alias, end_cypher_alias, start_node_label, end_node_label),
                    operator_sql,
                    render_end_filter_to_column_alias(&op.operands[1], start_cypher_alias, end_cypher_alias, start_node_label, end_node_label)
                )
            } else {
                // Fallback for complex expressions
                format!("{}({})", operator_sql, op.operands.iter()
                    .map(|operand| render_end_filter_to_column_alias(operand, start_cypher_alias, end_cypher_alias, start_node_label, end_node_label))
                    .collect::<Vec<_>>()
                    .join(", "))
            }
        }
        RenderExpr::Literal(lit) => {
            use super::render_expr::Literal;
            match lit {
                Literal::String(s) => format!("'{}'", s),
                Literal::Integer(i) => i.to_string(),
                Literal::Float(f) => f.to_string(),
                Literal::Boolean(b) => b.to_string(),
                Literal::Null => "NULL".to_string(),
            }
        }
        _ => expr.to_sql(), // Fallback to default to_sql()
    }
}

/// Convert RenderExpr to SQL string with node alias mapping for CTE generation
/// Maps Cypher aliases (e.g., "a", "b") to SQL table aliases (e.g., "start_node", "end_node")
fn render_expr_to_sql_for_cte(
    expr: &RenderExpr,
    start_cypher_alias: &str,
    end_cypher_alias: &str,
) -> String {
    
    match expr {
        RenderExpr::PropertyAccessExp(prop) => {
            let table_alias = &prop.table_alias.0;
            let column = &prop.column.0;
            
            // Map Cypher alias to SQL table alias
            if table_alias == start_cypher_alias {
                format!("start_node.{}", column)
            } else if table_alias == end_cypher_alias {
                format!("end_node.{}", column)  // end_node.name, end_node.email, etc.
            } else {
                // Fallback: use as-is
                format!("{}.{}", table_alias, column)
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            use super::render_expr::Operator;
            
            let operator_sql = match op.operator {
                Operator::Equal => "=",
                Operator::NotEqual => "!=",
                Operator::LessThan => "<",
                Operator::GreaterThan => ">",
                Operator::LessThanEqual => "<=",
                Operator::GreaterThanEqual => ">=",
                Operator::And => "AND",
                Operator::Or => "OR",
                Operator::Not => "NOT",
                _ => "=", // Fallback
            };
            
            if op.operands.len() == 2 {
                format!("{} {} {}", 
                    render_expr_to_sql_for_cte(&op.operands[0], start_cypher_alias, end_cypher_alias),
                    operator_sql,
                    render_expr_to_sql_for_cte(&op.operands[1], start_cypher_alias, end_cypher_alias)
                )
            } else if op.operands.len() == 1 {
                format!("{} {}", 
                    operator_sql,
                    render_expr_to_sql_for_cte(&op.operands[0], start_cypher_alias, end_cypher_alias)
                )
            } else {
                // Multiple operands with AND/OR
                let operand_sqls: Vec<String> = op.operands.iter()
                    .map(|operand| render_expr_to_sql_for_cte(operand, start_cypher_alias, end_cypher_alias))
                    .collect();
                format!("({})", operand_sqls.join(&format!(" {} ", operator_sql)))
            }
        }
        RenderExpr::Literal(lit) => {
            use super::render_expr::Literal;
            match lit {
                Literal::String(s) => format!("'{}'", s),
                Literal::Integer(i) => i.to_string(),
                Literal::Float(f) => f.to_string(),
                Literal::Boolean(b) => b.to_string(),
                Literal::Null => "NULL".to_string(),
            }
        }
        _ => expr.to_sql(), // Fallback to default to_sql()
    }
}

/// Rewrite end filters for variable-length CTE outer query
/// Converts Cypher property accesses (e.g., b.name) to CTE column references (e.g., t.end_name)
fn rewrite_end_filters_for_variable_length_cte(
    expr: &RenderExpr,
    cte_table_alias: &str,
    start_cypher_alias: &str,
    end_cypher_alias: &str,
) -> RenderExpr {
    match expr {
        RenderExpr::PropertyAccessExp(prop) => {
            let table_alias = &prop.table_alias.0;
            let column = &prop.column.0;
            
            // Map Cypher aliases to CTE column references
            if table_alias == end_cypher_alias {
                // b.name -> t.end_name
                RenderExpr::PropertyAccessExp(super::render_expr::PropertyAccess {
                    table_alias: super::render_expr::TableAlias(cte_table_alias.to_string()),
                    column: super::render_expr::Column(format!("end_{}", column)),
                })
            } else if table_alias == start_cypher_alias {
                // a.name -> t.start_name
                RenderExpr::PropertyAccessExp(super::render_expr::PropertyAccess {
                    table_alias: super::render_expr::TableAlias(cte_table_alias.to_string()),
                    column: super::render_expr::Column(format!("start_{}", column)),
                })
            } else {
                // Fallback: keep as-is
                expr.clone()
            }
        }
        RenderExpr::TableAlias(alias) => {
            expr.clone()
        }
        RenderExpr::OperatorApplicationExp(op) => {
            RenderExpr::OperatorApplicationExp(super::render_expr::OperatorApplication {
                operator: op.operator.clone(),
                operands: op.operands.iter()
                    .map(|operand| rewrite_end_filters_for_variable_length_cte(operand, cte_table_alias, start_cypher_alias, end_cypher_alias))
                    .collect(),
            })
        }
        _ => expr.clone(), // Literals, etc. stay the same
    }
}

/// Rewrite expressions for variable-length CTE outer query
/// Converts Cypher property accesses to CTE column references for SELECT clauses
fn rewrite_expr_for_var_len_cte(
    expr: &RenderExpr,
    start_cypher_alias: &str,
    end_cypher_alias: &str,
    _path_var: Option<&str>,
) -> RenderExpr {
    use super::render_expr::{PropertyAccess, TableAlias, Column};
    match expr {
        RenderExpr::PropertyAccessExp(prop) => {
            let mut new_prop = prop.clone();
            if prop.table_alias.0 == start_cypher_alias {
                new_prop.table_alias = TableAlias("t".to_string());
                if prop.column.0 == "*" {
                    new_prop.column = prop.column.clone();
                } else {
                    new_prop.column = Column(format!("start_{}", prop.column.0));
                }
            } else if prop.table_alias.0 == end_cypher_alias {
                // End node properties stay as is
            } else {
                // Other properties stay as is
            }
            RenderExpr::PropertyAccessExp(new_prop)
        }
        RenderExpr::TableAlias(alias) => {
            if alias.0 == start_cypher_alias {
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("t".to_string()),
                    column: Column("start_id".to_string()),
                })
            } else if alias.0 == end_cypher_alias {
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(end_cypher_alias.to_string()),
                    column: Column("id".to_string()),
                })
            } else {
                expr.clone()
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: op.operator.clone(),
                operands: op.operands.iter()
                    .map(|operand| rewrite_expr_for_var_len_cte(operand, start_cypher_alias, end_cypher_alias, _path_var))
                    .collect(),
            })
        }
        _ => expr.clone(), // Literals, etc. stay the same
    }
}

/// Categorize WHERE clause filters based on which node/relationship they reference
/// This is critical for shortest path queries where:
/// - Start node filters go in the base case (e.g., WHERE start_node.name = 'Alice')
/// - End node filters go in the outer CTE (e.g., WHERE end_node.name = 'Bob')
/// - Relationship filters go in base + recursive cases (e.g., WHERE rel.weight > 5)
fn categorize_filters(
    filter_expr: Option<&RenderExpr>,
    start_cypher_alias: &str,
    end_cypher_alias: &str,
    _rel_alias: &str, // For future relationship filtering
) -> CategorizedFilters {
    log::debug!("Categorizing filters for start alias '{}' and end alias '{}'", start_cypher_alias, end_cypher_alias);
    
    let mut result = CategorizedFilters {
        start_node_filters: None,
        end_node_filters: None,
        relationship_filters: None,
        path_function_filters: None,
    };
    
    if filter_expr.is_none() {
        log::trace!("No filter expression provided");
        return result;
    }
    
    log::trace!("Filter expression: {:?}", filter_expr.unwrap());
    
    let filter = filter_expr.unwrap();
    
    // Helper to check if an expression references a specific alias (checks both Cypher and SQL aliases)
    fn references_alias(expr: &RenderExpr, cypher_alias: &str, sql_alias: &str) -> bool {
        match expr {
            RenderExpr::PropertyAccessExp(prop) => {
                let table_alias = &prop.table_alias.0;
                table_alias == cypher_alias || table_alias == sql_alias
            }
            RenderExpr::OperatorApplicationExp(op) => {
                op.operands.iter().any(|operand| references_alias(operand, cypher_alias, sql_alias))
            }
            _ => false,
        }
    }
    
    // Helper to check if an expression contains path function calls
    fn contains_path_function(expr: &RenderExpr) -> bool {
        match expr {
            RenderExpr::ScalarFnCall(fn_call) => {
                // Check if this is a path function (length, nodes, relationships)
                matches!(fn_call.name.to_lowercase().as_str(), "length" | "nodes" | "relationships")
            }
            RenderExpr::OperatorApplicationExp(op) => {
                op.operands.iter().any(|operand| contains_path_function(operand))
            }
            _ => false,
        }
    }
    
    // Split AND-connected filters into individual predicates
    fn split_and_filters(expr: &RenderExpr) -> Vec<RenderExpr> {
        match expr {
            RenderExpr::OperatorApplicationExp(op) if matches!(op.operator, Operator::And) => {
                let mut filters = Vec::new();
                for operand in &op.operands {
                    filters.extend(split_and_filters(operand));
                }
                filters
            }
            _ => vec![expr.clone()],
        }
    }
    
    // Split the filter into individual predicates
    let predicates = split_and_filters(filter);
    
    let mut start_filters = Vec::new();
    let mut end_filters = Vec::new();
    let mut rel_filters = Vec::new();
    let mut path_fn_filters = Vec::new();
    
    for predicate in predicates {
        let refs_start = references_alias(&predicate, start_cypher_alias, "start_node");
        let refs_end = references_alias(&predicate, end_cypher_alias, "end_node");
        let has_path_fn = contains_path_function(&predicate);
        
        println!("DEBUG: Categorizing predicate: {:?}", predicate);
        println!("DEBUG: refs_start (alias '{}'): {}", start_cypher_alias, refs_start);
        println!("DEBUG: refs_end (alias '{}'): {}", end_cypher_alias, refs_end);
        println!("DEBUG: has_path_fn: {}", has_path_fn);
        
        if has_path_fn {
            // Path function filters (e.g., WHERE length(p) <= 3) go in path function filters
            println!("DEBUG: Going to path_fn_filters");
            path_fn_filters.push(predicate);
        } else if refs_start && refs_end {
            // Filter references both nodes - can't categorize simply
            // For now, treat as start filter (will be in base case)
            println!("DEBUG: Going to start_filters (refs both)");
            start_filters.push(predicate);
        } else if refs_start {
            println!("DEBUG: Going to start_filters");
            start_filters.push(predicate);
        } else if refs_end {
            println!("DEBUG: Going to end_filters");
            end_filters.push(predicate);
        } else {
            // Doesn't reference nodes - might be relationship filter or constant
            println!("DEBUG: Going to rel_filters");
            rel_filters.push(predicate);
        }
    }
    
    // Combine filters with AND
    fn combine_with_and(filters: Vec<RenderExpr>) -> Option<RenderExpr> {
        if filters.is_empty() {
            return None;
        }
        if filters.len() == 1 {
            return Some(filters.into_iter().next().unwrap());
        }
        Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::And,
            operands: filters,
        }))
    }
    
    result.start_node_filters = combine_with_and(start_filters);
    result.end_node_filters = combine_with_and(end_filters);
    result.relationship_filters = combine_with_and(rel_filters);
    result.path_function_filters = combine_with_and(path_fn_filters);
    
    log::trace!("Filter categorization result:");
    log::trace!("  Start filters: {:?}", result.start_node_filters);
    log::trace!("  End filters: {:?}", result.end_node_filters);
    log::trace!("  Rel filters: {:?}", result.relationship_filters);
    log::trace!("  Path function filters: {:?}", result.path_function_filters);
    
    result
}

pub(crate) trait RenderPlanBuilder {
    fn extract_last_node_cte(&self) -> RenderPlanBuilderResult<Option<Cte>>;

    fn extract_final_filters(&self) -> RenderPlanBuilderResult<Option<RenderExpr>>;

    fn extract_ctes(&self, last_node_alias: &str) -> RenderPlanBuilderResult<Vec<Cte>>;
    
    fn extract_ctes_with_context(&self, last_node_alias: &str, context: &mut CteGenerationContext) -> RenderPlanBuilderResult<Vec<Cte>>;

    fn extract_select_items(&self) -> RenderPlanBuilderResult<Vec<SelectItem>>;

    fn extract_from(&self) -> RenderPlanBuilderResult<Option<FromTable>>;

    fn extract_filters(&self) -> RenderPlanBuilderResult<Option<RenderExpr>>;

    fn extract_joins(&self) -> RenderPlanBuilderResult<Vec<Join>>;

    fn extract_group_by(&self) -> RenderPlanBuilderResult<Vec<RenderExpr>>;

    fn extract_order_by(&self) -> RenderPlanBuilderResult<Vec<OrderByItem>>;

    fn extract_limit(&self) -> Option<i64>;

    fn extract_skip(&self) -> Option<i64>;

    fn extract_union(&self) -> RenderPlanBuilderResult<Option<Union>>;

    fn to_render_plan(&self) -> RenderPlanBuilderResult<RenderPlan>;
}

impl RenderPlanBuilder for LogicalPlan {
    fn extract_last_node_cte(&self) -> RenderPlanBuilderResult<Option<Cte>> {
        let last_node_cte = match &self {
            LogicalPlan::Empty => None,
            LogicalPlan::Scan(_) => None,
            LogicalPlan::ViewScan(_) => None,
            LogicalPlan::GraphNode(graph_node) => graph_node.input.extract_last_node_cte()?,
            LogicalPlan::GraphRel(graph_rel) => {
                // Last node is at the top of the tree.
                // process left node first.
                let left_node_cte_opt = graph_rel.left.extract_last_node_cte()?;

                // If last node is still not found then check at the right tree
                if left_node_cte_opt.is_none() {
                    graph_rel.right.extract_last_node_cte()?
                } else {
                    left_node_cte_opt
                }
            }
            LogicalPlan::Filter(filter) => filter.input.extract_last_node_cte()?,
            LogicalPlan::Projection(projection) => projection.input.extract_last_node_cte()?,
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_last_node_cte()?,
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_last_node_cte()?,
            LogicalPlan::Skip(skip) => skip.input.extract_last_node_cte()?,
            LogicalPlan::Limit(limit) => limit.input.extract_last_node_cte()?,
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_last_node_cte()?,
            LogicalPlan::Cte(logical_cte) => {
                // let filters = logical_cte.input.extract_filters()?;
                // let select_items = logical_cte.input.extract_select_items()?;
                // let from_table = logical_cte.input.extract_from()?;
                let render_cte = Cte {
                    cte_name: logical_cte.name.clone(),
                    content: super::CteContent::Structured(logical_cte.input.to_render_plan()?),
                    is_recursive: false,
                    // select: SelectItems(select_items),
                    // from: from_table,
                    // filters: FilterItems(filters)
                };
                Some(render_cte)
            }
            LogicalPlan::Union(union) => {
                for input_plan in union.inputs.iter() {
                    if let Some(cte) = input_plan.extract_last_node_cte()? {
                        return Ok(Some(cte));
                    }
                }
                None
            }
            LogicalPlan::PageRank(_) => None,
        };
        Ok(last_node_cte)
    }

    fn extract_ctes(&self, last_node_alias: &str) -> RenderPlanBuilderResult<Vec<Cte>> {
        match &self {
            LogicalPlan::Empty => Ok(vec![]),
            LogicalPlan::Scan(_) => Ok(vec![]),
            LogicalPlan::ViewScan(_) => Ok(vec![]),
            LogicalPlan::GraphNode(graph_node) => graph_node.input.extract_ctes(last_node_alias),
            LogicalPlan::GraphRel(graph_rel) => {
                // Extract table names and column information - SAME LOGIC FOR BOTH PATHS
                // Apply mapping even if extract_table_name succeeds, in case it returns a label
                let start_table = label_to_table_name(&extract_table_name(&graph_rel.left)
                    .unwrap_or_else(|| graph_rel.left_connection.clone()));
                let end_table = label_to_table_name(&extract_table_name(&graph_rel.right)
                    .unwrap_or_else(|| graph_rel.right_connection.clone()));
                
                // Handle multiple relationship types
                let rel_tables = if let Some(labels) = &graph_rel.labels {
                    if labels.len() > 1 {
                        // Multiple relationship types: get all table names
                        rel_types_to_table_names(labels)
                    } else if labels.len() == 1 {
                        // Single relationship type
                        vec![rel_type_to_table_name(&labels[0])]
                    } else {
                        // Fallback to old logic
                        vec![rel_type_to_table_name(&extract_table_name(&graph_rel.center)
                            .unwrap_or_else(|| graph_rel.alias.clone()))]
                    }
                } else {
                    // Fallback to old logic
                    vec![rel_type_to_table_name(&extract_table_name(&graph_rel.center)
                        .unwrap_or_else(|| graph_rel.alias.clone()))]
                };
                
                // For now, use the first table for single-table logic
                // TODO: Implement UNION logic for multiple tables
                let mut rel_table = rel_tables.first().unwrap().clone();
                
                // Extract ID columns
                let start_id_col = extract_id_column(&graph_rel.left)
                    .unwrap_or_else(|| table_to_id_column(&start_table));
                let end_id_col = extract_id_column(&graph_rel.right)
                    .unwrap_or_else(|| table_to_id_column(&end_table));
                
                // Extract relationship columns from ViewScan (will use schema-specific names if available)
                let rel_cols = extract_relationship_columns(&graph_rel.center)
                    .unwrap_or(RelationshipColumns {
                        from_column: "from_node_id".to_string(),  // Generic fallback
                        to_column: "to_node_id".to_string(),      // Generic fallback
                    });
                let from_col = rel_cols.from_column;
                let to_col = rel_cols.to_column;
                
                // Handle variable-length paths differently
                if let Some(spec) = &graph_rel.variable_length {
                    // Define aliases that will be used throughout
                    let start_alias = graph_rel.left_connection.clone();
                    let end_alias = graph_rel.right_connection.clone();
                    
                    // Extract node labels for property mapping
                    let start_label = extract_node_label_from_viewscan(&graph_rel.left)
                        .unwrap_or_else(|| "User".to_string()); // fallback
                    let end_label = extract_node_label_from_viewscan(&graph_rel.right)
                        .unwrap_or_else(|| "User".to_string()); // fallback
                    
                    // Extract and categorize filters for variable-length paths from GraphRel.where_predicate
                    let (start_filters_sql, end_filters_sql) = if let Some(where_predicate) = &graph_rel.where_predicate {
                        // Convert LogicalExpr to RenderExpr
                        let mut render_expr = RenderExpr::try_from(where_predicate.clone())
                            .map_err(|e| RenderBuildError::UnsupportedFeature(format!("Failed to convert LogicalExpr to RenderExpr: {}", e)))?;
                        
                        // Apply property mapping to the filter expression before categorization
                        apply_property_mapping_to_expr(&mut render_expr, &LogicalPlan::GraphRel(graph_rel.clone()));
                        
                        println!("DEBUG: After property mapping, render_expr: {:?}", render_expr);
                        
                        // Categorize filters
                        let categorized = categorize_filters(
                            Some(&render_expr),
                            &start_alias,
                            &end_alias,
                            "", // rel_alias not used yet
                        );
                        
                        println!("DEBUG: categorized.start_node_filters: {:?}", categorized.start_node_filters);
                        
                        // Create alias mapping
                        let alias_mapping = [
                            (start_alias.clone(), "start_node".to_string()),
                            (end_alias.clone(), "end_node".to_string()),
                        ];
                        
                        let start_sql = categorized.start_node_filters
                            .map(|expr| render_expr_to_sql_string(&expr, &alias_mapping));
                        let end_sql = categorized.end_node_filters
                            .map(|expr| render_expr_to_sql_string(&expr, &alias_mapping));
                        
                        (start_sql, end_sql)
                    } else {
                        (None, None)
                    };
                    
                    println!("DEBUG: start_filters: {:?}, end_filters: {:?}", start_filters_sql, end_filters_sql);
                    
                    // Extract properties from the projection for variable-length paths
                    let properties = extract_var_len_properties(self, &start_alias, &end_alias, &start_label, &end_label);
                    
                    // Choose between chained JOINs (for exact hop counts) or recursive CTE (for ranges)
                    let var_len_cte = if let Some(exact_hops) = spec.exact_hop_count() {
                        // Exact hop count: use optimized chained JOINs
                        let generator = ChainedJoinGenerator::new(
                            exact_hops,
                            &start_table,
                            &start_id_col,
                            &rel_table,
                            &from_col,
                            &to_col,
                            &end_table,
                            &end_id_col,
                            &graph_rel.left_connection,
                            &graph_rel.right_connection,
                            properties,
                        );
                        generator.generate_cte()
                    } else {
                        // Range or unbounded: use recursive CTE
                        let generator = VariableLengthCteGenerator::new(
                            spec.clone(),
                            &start_table,                    // actual start table name
                            &start_id_col,                   // start node ID column
                            &rel_table,                      // actual relationship table name
                            &from_col,                       // relationship from column
                            &to_col,                         // relationship to column  
                            &end_table,                      // actual end table name
                            &end_id_col,                     // end node ID column
                            &graph_rel.left_connection,      // start node alias (for output)
                            &graph_rel.right_connection,     // end node alias (for output)
                            properties,                      // properties to include in CTE
                            graph_rel.shortest_path_mode.clone().map(|m| m.into()), // convert logical plan mode to SQL mode
                            start_filters_sql,               // start node filters for CTE
                            end_filters_sql,                 // end node filters for CTE
                            graph_rel.path_variable.clone(), // path variable name
                            graph_rel.labels.clone(),        // relationship type labels
                        );
                        generator.generate_cte()
                    };
                    
                    // Also extract CTEs from child plans
                    let mut child_ctes = graph_rel.right.extract_ctes(last_node_alias)?;
                    child_ctes.push(var_len_cte);
                    
                    return Ok(child_ctes);
                }

                // Regular single-hop relationship: still need to use resolved table names!
                // Handle multiple relationship types with UNION if needed
                let mut relationship_ctes = vec![];
                
                if rel_tables.len() > 1 {
                    // Multiple relationship types: create a UNION CTE
                    let union_queries: Vec<String> = rel_tables.iter().map(|table| {
                        // Get the correct column names for this table
                        let (from_col, to_col) = get_relationship_columns_by_table(table)
                            .unwrap_or(("from_node_id".to_string(), "to_node_id".to_string())); // fallback
                        format!("SELECT {} as from_node_id, {} as to_node_id FROM {}", from_col, to_col, table)
                    }).collect();
                    
                    let union_sql = union_queries.join(" UNION ALL ");
                    let cte_name = format!("rel_{}_{}", graph_rel.left_connection, graph_rel.right_connection);
                    
                    // Format as proper CTE: cte_name AS (union_sql)
                    let formatted_union_sql = format!("{} AS (\n{}\n)", cte_name, union_sql);
                    
                    relationship_ctes.push(Cte {
                        cte_name: cte_name.clone(),
                        content: super::CteContent::RawSql(formatted_union_sql),
                        is_recursive: false,
                    });
                    
                    // Update rel_table to use the CTE name for subsequent processing
                    rel_table = cte_name;
                    // PATCH: Ensure join uses the union CTE name
                    // Instead of context, propagate rel_table for join construction
                    // We'll use rel_table (CTE name) directly in join construction below
                }
                
                // TODO: Apply the resolved table/column names to the child CTEs
                // For now, fall back to the old path which doesn't resolve properly
                // first extract the bottom one
                let mut right_cte = graph_rel.right.extract_ctes(last_node_alias)?;
                // then process the center
                let mut center_cte = graph_rel.center.extract_ctes(last_node_alias)?;
                right_cte.append(&mut center_cte);
                // then left
                let left_alias = &graph_rel.left_connection;
                if left_alias != last_node_alias {
                    let mut left_cte = graph_rel.left.extract_ctes(last_node_alias)?;
                    right_cte.append(&mut left_cte);
                }

                // Add relationship CTEs to the result
                relationship_ctes.append(&mut right_cte);
                
                Ok(relationship_ctes)
            }
            LogicalPlan::Filter(filter) => filter.input.extract_ctes(last_node_alias),
            LogicalPlan::Projection(projection) => projection.input.extract_ctes(last_node_alias),
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_ctes(last_node_alias),
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_ctes(last_node_alias),
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_ctes(last_node_alias),
            LogicalPlan::Skip(skip) => skip.input.extract_ctes(last_node_alias),
            LogicalPlan::Limit(limit) => limit.input.extract_ctes(last_node_alias),
            LogicalPlan::Cte(logical_cte) => {
                // let mut select_items = logical_cte.input.extract_select_items()?;

                // for select_item in select_items.iter_mut() {
                //     if let RenderExpr::PropertyAccessExp(pro_acc) = &select_item.expression {
                //         *select_item = SelectItem {
                //             expression: RenderExpr::Column(pro_acc.column.clone()),
                //             col_alias: None,
                //         };
                //     }
                // }

                // let mut from_table = logical_cte.input.extract_from()?;
                // from_table.table_alias = None;
                // let filters = logical_cte.input.extract_filters()?;
                Ok(vec![Cte {
                    cte_name: logical_cte.name.clone(),
                    content: super::CteContent::Structured(logical_cte.input.to_render_plan()?),
                    is_recursive: false,
                    // select: SelectItems(select_items),
                    // from: from_table,
                    // filters: FilterItems(filters)
                }])
            }
            LogicalPlan::Union(union) => {
                let mut ctes = vec![];
                for input_plan in union.inputs.iter() {
                    ctes.append(&mut input_plan.extract_ctes(last_node_alias)?);
                }
                Ok(ctes)
            }
            LogicalPlan::PageRank(_) => Ok(vec![]),
        }
    }

    fn extract_ctes_with_context(&self, last_node_alias: &str, context: &mut CteGenerationContext) -> RenderPlanBuilderResult<Vec<Cte>> {
        match &self {
            LogicalPlan::Empty => Ok(vec![]),
            LogicalPlan::Scan(_) => Ok(vec![]),
            LogicalPlan::ViewScan(_) => Ok(vec![]),
            LogicalPlan::GraphNode(graph_node) => graph_node.input.extract_ctes_with_context(last_node_alias, context),
            LogicalPlan::GraphRel(graph_rel) => {
                // Handle variable-length paths with context
                if let Some(spec) = &graph_rel.variable_length {
                    // Extract actual table names and column information
                    let start_table = label_to_table_name(&extract_table_name(&graph_rel.left)
                        .unwrap_or_else(|| graph_rel.left_connection.clone()));
                    let end_table = label_to_table_name(&extract_table_name(&graph_rel.right)
                        .unwrap_or_else(|| graph_rel.right_connection.clone()));
                    let rel_table = rel_type_to_table_name(&extract_table_name(&graph_rel.center)
                        .unwrap_or_else(|| graph_rel.alias.clone()));
                    
                    // Extract ID columns
                    let start_id_col = extract_id_column(&graph_rel.left)
                        .unwrap_or_else(|| table_to_id_column(&start_table));
                    let end_id_col = extract_id_column(&graph_rel.right)
                        .unwrap_or_else(|| table_to_id_column(&end_table));
                    
                    // Extract relationship columns
                    let rel_cols = extract_relationship_columns(&graph_rel.center)
                        .unwrap_or(RelationshipColumns {
                            from_column: "from_node_id".to_string(),
                            to_column: "to_node_id".to_string(),
                        });
                    let from_col = rel_cols.from_column;
                    let to_col = rel_cols.to_column;
                    
                    // Get properties from context - this is the KEY difference!
                    let properties = context.get_properties(&graph_rel.left_connection, &graph_rel.right_connection);
                    
                    // Define aliases based on traversal direction
                    // For variable-length paths, we need to know which node is the traversal start vs end
                    let (start_alias, end_alias) = match graph_rel.direction {
                        Direction::Outgoing => {
                            (graph_rel.left_connection.clone(), graph_rel.right_connection.clone())
                        }
                        Direction::Incoming => {
                            (graph_rel.right_connection.clone(), graph_rel.left_connection.clone())
                        }
                        Direction::Either => {
                            // For Either, assume left to right traversal
                            (graph_rel.left_connection.clone(), graph_rel.right_connection.clone())
                        }
                    };
                    
                    println!("DEBUG: graph_rel.direction: {:?}", graph_rel.direction);
                    println!("DEBUG: graph_rel.left_connection: {}", graph_rel.left_connection);
                    println!("DEBUG: graph_rel.right_connection: {}", graph_rel.right_connection);
                    println!("DEBUG: start_alias: {}, end_alias: {}", start_alias, end_alias);
                    
                    // Extract and categorize filters for variable-length paths from GraphRel.where_predicate
                    let (start_filters_sql, end_filters_sql) = if let Some(where_predicate) = &graph_rel.where_predicate {
                        // Convert LogicalExpr to RenderExpr
                        let mut render_expr = RenderExpr::try_from(where_predicate.clone())
                            .map_err(|e| RenderBuildError::UnsupportedFeature(format!("Failed to convert LogicalExpr to RenderExpr: {}", e)))?;
                        
                        // Apply property mapping to the filter expression before categorization
                        apply_property_mapping_to_expr(&mut render_expr, &LogicalPlan::GraphRel(graph_rel.clone()));
                        
                        println!("DEBUG extract_ctes_with_context: After property mapping, render_expr: {:?}", render_expr);
                        
                        // Categorize filters
                        let categorized = categorize_filters(
                            Some(&render_expr),
                            &start_alias,
                            &end_alias,
                            "", // rel_alias not used yet
                        );
                        
                        println!("DEBUG extract_ctes_with_context: categorized.start_node_filters: {:?}", categorized.start_node_filters);
                        println!("DEBUG extract_ctes_with_context: categorized.end_node_filters: {:?}", categorized.end_node_filters);
                        
                        // Create alias mapping
                        let alias_mapping = [
                            (start_alias.clone(), "start_node".to_string()),
                            (end_alias.clone(), "end_node".to_string()),
                        ];
                        
                        let start_sql = categorized.start_node_filters
                            .map(|expr| render_expr_to_sql_string(&expr, &alias_mapping));
                        let end_sql = categorized.end_node_filters
                            .as_ref()
                            .map(|expr| render_expr_to_sql_string(expr, &alias_mapping));
                        
                        // For variable-length queries (not shortest path), store end filters in context for outer query
                        if graph_rel.shortest_path_mode.is_none() {
                            if let Some(end_filter_expr) = &categorized.end_node_filters {
                                context.set_end_filters_for_outer_query(end_filter_expr.clone());
                            }
                        }
                        
                        (start_sql, end_sql)
                    } else {
                        (None, None)
                    };
                    
                    println!("DEBUG extract_ctes_with_context: start_filters: {:?}, end_filters: {:?}", start_filters_sql, end_filters_sql);
                    
                    // Generate CTE with filters
                    let var_len_cte = if let Some(exact_hops) = spec.exact_hop_count() {
                        // Exact hop count: use optimized chained JOINs
                        let generator = ChainedJoinGenerator::new(
                            exact_hops,
                            &start_table,
                            &start_id_col,
                            &rel_table,
                            &from_col,
                            &to_col,
                            &end_table,
                            &end_id_col,
                            &graph_rel.left_connection,
                            &graph_rel.right_connection,
                            vec![],  // No properties in SQL_ONLY mode
                        );
                        generator.generate_cte()
                    } else {
                        // Range or unbounded: use recursive CTE
                        let generator = VariableLengthCteGenerator::new(
                            spec.clone(),
                            &start_table,
                            &start_id_col,
                            &rel_table,
                            &from_col,
                            &to_col,
                            &end_table,
                            &end_id_col,
                            &graph_rel.left_connection,
                            &graph_rel.right_connection,
                            vec![],  // No properties in SQL_ONLY mode
                            graph_rel.shortest_path_mode.clone().map(|m| m.into()),
                            start_filters_sql,   // Start filters
                            if graph_rel.shortest_path_mode.is_some() { end_filters_sql } else { None },     // End filters only for shortest path
                            graph_rel.path_variable.clone(),
                            graph_rel.labels.clone(),
                        );
                        generator.generate_cte()
                    };
                    
                    // Also extract CTEs from child plans
                    let mut child_ctes = graph_rel.right.extract_ctes_with_context(last_node_alias, context)?;
                    child_ctes.push(var_len_cte);
                    
                    return Ok(child_ctes);
                }

                // Handle multiple relationship types for regular single-hop relationships
                let mut relationship_ctes = vec![];
                
                if let Some(labels) = &graph_rel.labels {
                    log::debug!("GraphRel labels: {:?}", labels);
                    if labels.len() > 1 {
                        // Multiple relationship types: get all table names
                        let rel_tables = rel_types_to_table_names(labels);
                        log::debug!("Resolved tables for labels {:?}: {:?}", labels, rel_tables);
                        
                        // Create a UNION CTE
                        let union_queries: Vec<String> = rel_tables.iter().map(|table| {
                            // Get the correct column names for this table
                            let (from_col, to_col) = get_relationship_columns_by_table(table)
                                .unwrap_or(("from_node_id".to_string(), "to_node_id".to_string())); // fallback
                            format!("SELECT {} as from_node_id, {} as to_node_id FROM {}", from_col, to_col, table)
                        }).collect();
                        
                        let union_sql = union_queries.join(" UNION ALL ");
                        let cte_name = format!("rel_{}_{}", graph_rel.left_connection, graph_rel.right_connection);
                        
                        // Format as proper CTE: cte_name AS (union_sql)
                        let formatted_union_sql = format!("{} AS (\n{}\n)", cte_name, union_sql);
                        
                        relationship_ctes.push(Cte {
                            cte_name: cte_name.clone(),
                            content: super::CteContent::RawSql(formatted_union_sql),
                            is_recursive: false,
                        });
                    }
                }

                // Normal path - recurse through children
                let mut right_cte = graph_rel.right.extract_ctes_with_context(last_node_alias, context)?;
                let mut center_cte = graph_rel.center.extract_ctes_with_context(last_node_alias, context)?;
                right_cte.append(&mut center_cte);
                let left_alias = &graph_rel.left_connection;
                if left_alias != last_node_alias {
                    let mut left_cte = graph_rel.left.extract_ctes_with_context(last_node_alias, context)?;
                    right_cte.append(&mut left_cte);
                }

                // Add relationship CTEs
                relationship_ctes.append(&mut right_cte);
                
                Ok(relationship_ctes)
            }
            LogicalPlan::Filter(filter) => {
                // Store the filter in context so GraphRel nodes can access it
                log::trace!("Filter node detected, storing filter predicate in context: {:?}", filter.predicate);
                let mut new_context = context.clone();
                let filter_expr: RenderExpr = filter.predicate.clone().try_into()?;
                log::trace!("Converted to RenderExpr: {:?}", filter_expr);
                new_context.set_filter(filter_expr);
                let ctes = filter.input.extract_ctes_with_context(last_node_alias, &mut new_context)?;
                // Merge end filters from the new context back to the original context
                if let Some(end_filters) = new_context.get_end_filters_for_outer_query().cloned() {
                    context.set_end_filters_for_outer_query(end_filters);
                }
                Ok(ctes)
            }
            LogicalPlan::Projection(projection) => {
                log::trace!("Projection node detected, recursing into input type: {}", match &*projection.input {
                    LogicalPlan::Empty => "Empty",
                    LogicalPlan::Scan(_) => "Scan",
                    LogicalPlan::ViewScan(_) => "ViewScan",
                    LogicalPlan::GraphNode(_) => "GraphNode",
                    LogicalPlan::GraphRel(_) => "GraphRel",
                    LogicalPlan::Filter(_) => "Filter",
                    LogicalPlan::Projection(_) => "Projection",
                    LogicalPlan::GraphJoins(_) => "GraphJoins",
                    LogicalPlan::GroupBy(_) => "GroupBy",
                    LogicalPlan::OrderBy(_) => "OrderBy",
                    LogicalPlan::Skip(_) => "Skip",
                    LogicalPlan::Limit(_) => "Limit",
                    LogicalPlan::Cte(_) => "Cte",
                    LogicalPlan::Union(_) => "Union",
                    LogicalPlan::PageRank(_) => "PageRank",
                });
                projection.input.extract_ctes_with_context(last_node_alias, context)
            }
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_ctes_with_context(last_node_alias, context),
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_ctes_with_context(last_node_alias, context),
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_ctes_with_context(last_node_alias, context),
            LogicalPlan::Skip(skip) => skip.input.extract_ctes_with_context(last_node_alias, context),
            LogicalPlan::Limit(limit) => limit.input.extract_ctes_with_context(last_node_alias, context),
            LogicalPlan::Cte(logical_cte) => {
                Ok(vec![Cte {
                    cte_name: logical_cte.name.clone(),
                    content: super::CteContent::Structured(logical_cte.input.to_render_plan()?),
                    is_recursive: false,
                }])
            }
            LogicalPlan::Union(union) => {
                let mut ctes = vec![];
                for input_plan in union.inputs.iter() {
                    ctes.append(&mut input_plan.extract_ctes_with_context(last_node_alias, context)?);
                }
                Ok(ctes)
            }
            LogicalPlan::PageRank(_) => Ok(vec![]),
        }
    }

    fn extract_select_items(&self) -> RenderPlanBuilderResult<Vec<SelectItem>> {
        let select_items = match &self {
            LogicalPlan::Empty => vec![],
            LogicalPlan::Scan(_) => vec![],
            LogicalPlan::ViewScan(_) => vec![],
            LogicalPlan::GraphNode(graph_node) => graph_node.input.extract_select_items()?,
            LogicalPlan::GraphRel(_) => vec![],
            LogicalPlan::Filter(filter) => filter.input.extract_select_items()?,
            LogicalPlan::Projection(projection) => {
                let path_var = get_path_variable(&projection.input);
                let items = projection.items.iter().map(|item| {
                    let mut expr: RenderExpr = item.expression.clone().try_into()?;
                    
                    // Check if this is a path variable that needs to be converted to map construction
                    if let (Some(path_var_name), RenderExpr::TableAlias(TableAlias(alias))) = (&path_var, &expr) {
                        if alias == path_var_name {
                            // Convert path variable to map construction
                            expr = RenderExpr::ScalarFnCall(ScalarFnCall {
                                name: "map".to_string(),
                                args: vec![
                                    RenderExpr::Literal(Literal::String("nodes".to_string())),
                                    RenderExpr::Column(Column("path_nodes".to_string())),
                                    RenderExpr::Literal(Literal::String("length".to_string())),
                                    RenderExpr::Column(Column("hop_count".to_string())),
                                    RenderExpr::Literal(Literal::String("relationships".to_string())),
                                    RenderExpr::Column(Column("path_relationships".to_string())),
                                ],
                            });
                        }
                    }
                    
                    // Apply property mapping to the expression
                    apply_property_mapping_to_expr(&mut expr, &projection.input);
                    let alias = item
                        .col_alias
                        .clone()
                        .map(ColumnAlias::try_from)
                        .transpose()?;
                    Ok(SelectItem {
                        expression: expr,
                        col_alias: alias,
                    })
                });

                items.collect::<Result<Vec<SelectItem>, RenderBuildError>>()?
            }
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_select_items()?,
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_select_items()?,
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_select_items()?,
            LogicalPlan::Skip(skip) => skip.input.extract_select_items()?,
            LogicalPlan::Limit(limit) => limit.input.extract_select_items()?,
            LogicalPlan::Cte(cte) => cte.input.extract_select_items()?,
            LogicalPlan::Union(_) => vec![],
            LogicalPlan::PageRank(_) => vec![],
        };

        Ok(select_items)
    }

    fn extract_from(&self) -> RenderPlanBuilderResult<Option<FromTable>> {
        let from_ref = match &self {
            LogicalPlan::Empty => None,
            LogicalPlan::Scan(scan) => {
                let table_name_raw = scan.table_name.clone().ok_or(RenderBuildError::MissingFromTable)?;
                // Apply relationship type mapping if this might be a relationship scan
                // (Node scans should be ViewScan after our fix, so remaining Scans are likely relationships)
                let table_name = rel_type_to_table_name(&table_name_raw);
                
                // Get the alias - use Scan's table_alias if available
                let alias = if let Some(ref scan_alias) = scan.table_alias {
                    log::info!("✓ Scan has table_alias='{}' for table '{}'", scan_alias, table_name);
                    scan_alias.clone()
                } else {
                    // No alias in Scan - this shouldn't happen for relationship scans!
                    // Generate a warning and use a default
                    let default_alias = "t".to_string();
                    log::error!("❌ BUG: Scan for table '{}' has NO table_alias! Using fallback '{}'", 
                        table_name, default_alias);
                    log::error!("   This indicates the Scan was created without preserving the Cypher variable name!");
                    default_alias
                };
                
                log::info!("✓ Creating ViewTableRef: table='{}', alias='{}'", table_name, alias);
                Some(ViewTableRef::new_view_with_alias(
                    Arc::new(LogicalPlan::Scan(scan.clone())),
                    table_name,
                    alias,
                ))
            },
            LogicalPlan::ViewScan(scan) => Some(ViewTableRef::new_table(
                scan.as_ref().clone(),
                scan.source_table.clone(),
            )),
            LogicalPlan::GraphNode(graph_node) => {
                // For GraphNode, extract FROM from the input but use this GraphNode's alias
                // CROSS JOINs for multiple standalone nodes are handled in extract_joins
                let mut from_ref = from_table_to_view_ref(graph_node.input.extract_from()?);
                // Use this GraphNode's alias
                if let Some(ref mut view_ref) = from_ref {
                    view_ref.alias = Some(graph_node.alias.clone());
                }
                from_ref
            },
            LogicalPlan::GraphRel(_) => None,
            LogicalPlan::Filter(filter) => from_table_to_view_ref(filter.input.extract_from()?),
            LogicalPlan::Projection(projection) => from_table_to_view_ref(projection.input.extract_from()?),
            LogicalPlan::GraphJoins(graph_joins) => from_table_to_view_ref(graph_joins.input.extract_from()?),
            LogicalPlan::GroupBy(group_by) => from_table_to_view_ref(group_by.input.extract_from()?),
            LogicalPlan::OrderBy(order_by) => from_table_to_view_ref(order_by.input.extract_from()?),
            LogicalPlan::Skip(skip) => from_table_to_view_ref(skip.input.extract_from()?),
            LogicalPlan::Limit(limit) => from_table_to_view_ref(limit.input.extract_from()?),
            LogicalPlan::Cte(cte) => from_table_to_view_ref(cte.input.extract_from()?),
            LogicalPlan::Union(_) => None,
            LogicalPlan::PageRank(_) => None,
        };
        Ok(view_ref_to_from_table(from_ref))
    }

    fn extract_filters(&self) -> RenderPlanBuilderResult<Option<RenderExpr>> {
        let filters = match &self {
            LogicalPlan::Empty => None,
            LogicalPlan::Scan(_) => None,
            LogicalPlan::ViewScan(_) => None,
            LogicalPlan::GraphNode(graph_node) => graph_node.input.extract_filters()?,
            LogicalPlan::GraphRel(graph_rel) => {
                log::trace!("GraphRel node detected, extracting filters from left, center, and right sub-plans");
                
                // Try extracting from each sub-plan and combine
                let left_filters = graph_rel.left.extract_filters()?;
                let center_filters = graph_rel.center.extract_filters()?;
                let right_filters = graph_rel.right.extract_filters()?;
                
                log::trace!("Extracted filters - left: {:?}, center: {:?}, right: {:?}", 
                    left_filters, center_filters, right_filters);
                
                // Combine all filters with AND
                let all_filters: Vec<RenderExpr> = vec![left_filters, center_filters, right_filters]
                    .into_iter()
                    .flatten()
                    .collect();
                
                if all_filters.is_empty() {
                    None
                } else if all_filters.len() == 1 {
                    Some(all_filters.into_iter().next().unwrap())
                } else {
                    Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::And,
                        operands: all_filters,
                    }))
                }
            }
            LogicalPlan::Filter(filter) => {
                let mut expr: RenderExpr = filter.predicate.clone().try_into()?;
                // Apply property mapping to the filter expression
                apply_property_mapping_to_expr(&mut expr, &filter.input);
                Some(expr)
            }
            LogicalPlan::Projection(projection) => projection.input.extract_filters()?,
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_filters()?,
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_filters()?,
            LogicalPlan::Skip(skip) => skip.input.extract_filters()?,
            LogicalPlan::Limit(limit) => limit.input.extract_filters()?,
            LogicalPlan::Cte(cte) => cte.input.extract_filters()?,
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_filters()?,
            LogicalPlan::Union(_) => None,
            LogicalPlan::PageRank(_) => None,
        };
        Ok(filters)
    }

    fn extract_final_filters(&self) -> RenderPlanBuilderResult<Option<RenderExpr>> {
        let final_filters = match &self {
            LogicalPlan::Limit(limit) => limit.input.extract_final_filters()?,
            LogicalPlan::Skip(skip) => skip.input.extract_final_filters()?,
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_final_filters()?,
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_final_filters()?,
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_final_filters()?,
            LogicalPlan::Projection(projection) => projection.input.extract_final_filters()?,
            LogicalPlan::Filter(filter) => {
                let mut expr: RenderExpr = filter.predicate.clone().try_into()?;
                // Apply property mapping to the filter expression
                apply_property_mapping_to_expr(&mut expr, &filter.input);
                Some(expr)
            },
            LogicalPlan::GraphRel(graph_rel) => {
                // For GraphRel, extract path function filters that should be applied to the final query
                if let Some(logical_expr) = &graph_rel.where_predicate {
                    let mut filter_expr: RenderExpr = logical_expr.clone().try_into()?;
                    // Apply property mapping to the where predicate
                    apply_property_mapping_to_expr(&mut filter_expr, &LogicalPlan::GraphRel(graph_rel.clone()));
                    let start_alias = graph_rel.left_connection.clone();
                    let end_alias = graph_rel.right_connection.clone();
                    
                    let categorized = categorize_filters(
                        Some(&filter_expr),
                        &start_alias,
                        &end_alias,
                        &graph_rel.alias,
                    );
                    
                    categorized.path_function_filters
                } else {
                    None
                }
            }
            _ => None,
        };
        Ok(final_filters)
    }

    fn extract_joins(&self) -> RenderPlanBuilderResult<Vec<Join>> {
        let joins = match &self {
            LogicalPlan::Limit(limit) => limit.input.extract_joins()?,
            LogicalPlan::Skip(skip) => skip.input.extract_joins()?,
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_joins()?,
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_joins()?,
            LogicalPlan::Filter(filter) => filter.input.extract_joins()?,
            LogicalPlan::Projection(projection) => {
                projection.input.extract_joins()?
            }
            LogicalPlan::GraphNode(graph_node) => {
                // For nested GraphNodes (multiple standalone nodes), create CROSS JOINs
                let mut joins = vec![];
                
                // If this GraphNode has another GraphNode as input, create a CROSS JOIN for the inner node
                if let LogicalPlan::GraphNode(inner_node) = graph_node.input.as_ref() {
                    if let Some(table_name) = extract_table_name(&graph_node.input) {
                        joins.push(Join {
                            table_name,
                            table_alias: inner_node.alias.clone(), // Use the inner GraphNode's alias
                            joining_on: vec![], // Empty for CROSS JOIN
                            join_type: JoinType::Join, // CROSS JOIN
                        });
                    }
                }
                
                // Recursively get joins from the input
                let mut inner_joins = graph_node.input.extract_joins()?;
                joins.append(&mut inner_joins);
                
                joins
            },
            LogicalPlan::GraphJoins(graph_joins) => graph_joins
                .joins
                .iter()
                .cloned()
                .map(|mut join| {
                    // PATCH: If this is a multi-relationship query, use rel_table (CTE name) for join.table_name
                    // This requires propagating rel_table from the CTE generation above
                    // For now, if join.table_name starts with "rel_" (our CTE naming convention), keep it
                    // Otherwise, use as-is
                    if join.table_name.starts_with("rel_") {
                        // Already the CTE name, do nothing
                    }
                    Join::try_from(join)
                })
                .collect::<Result<Vec<Join>, RenderBuildError>>()?,
            _ => vec![],
        };
        Ok(joins)
    }

    fn extract_group_by(&self) -> RenderPlanBuilderResult<Vec<RenderExpr>> {
        let group_by = match &self {
            LogicalPlan::Limit(limit) => limit.input.extract_group_by()?,
            LogicalPlan::Skip(skip) => skip.input.extract_group_by()?,
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_group_by()?,
            LogicalPlan::GroupBy(group_by) => group_by
                .expressions
                .iter()
                .cloned()
                .map(|expr| {
                    let mut render_expr: RenderExpr = expr.try_into()?;
                    // Apply property mapping to the group by expression
                    apply_property_mapping_to_expr(&mut render_expr, &group_by.input);
                    Ok(render_expr)
                })
                .collect::<Result<Vec<RenderExpr>, RenderBuildError>>()?, //.collect::<Vec<RenderExpr>>(),
            _ => vec![],
        };
        Ok(group_by)
    }

    fn extract_order_by(&self) -> RenderPlanBuilderResult<Vec<OrderByItem>> {
        let order_by = match &self {
            LogicalPlan::Limit(limit) => limit.input.extract_order_by()?,
            LogicalPlan::Skip(skip) => skip.input.extract_order_by()?,
            LogicalPlan::OrderBy(order_by) => order_by
                .items
                .iter()
                .cloned()
                .map(|item| {
                    let mut order_item: OrderByItem = item.try_into()?;
                    // Apply property mapping to the order by expression
                    apply_property_mapping_to_expr(&mut order_item.expression, &order_by.input);
                    Ok(order_item)
                })
                .collect::<Result<Vec<OrderByItem>, RenderBuildError>>()?,
            _ => vec![],
        };
        Ok(order_by)
    }

    fn extract_skip(&self) -> Option<i64> {
        match &self {
            LogicalPlan::Limit(limit) => limit.input.extract_skip(),
            LogicalPlan::Skip(skip) => Some(skip.count),
            _ => None,
        }
    }

    fn extract_limit(&self) -> Option<i64> {
        match &self {
            LogicalPlan::Limit(limit) => Some(limit.count),
            _ => None,
        }
    }

    fn extract_union(&self) -> RenderPlanBuilderResult<Option<Union>> {
        let union_opt = match &self {
            LogicalPlan::Union(union) => Some(Union {
                input: union
                    .inputs
                    .iter()
                    .map(|input| input.to_render_plan())
                    .collect::<Result<Vec<RenderPlan>, RenderBuildError>>()?,
                union_type: union.union_type.clone().try_into()?,
            }),
            _ => None,
        };
        Ok(union_opt)
    }

    fn to_render_plan(&self) -> RenderPlanBuilderResult<RenderPlan> {
        eprintln!("DEBUG: to_render_plan called on plan type: {:?}", std::mem::discriminant(self));
        
        // Special case for PageRank - it generates complete SQL directly
        if let LogicalPlan::PageRank(pagerank) = self {
            // For PageRank, we create a minimal RenderPlan that will be handled specially
            // The actual SQL generation happens in the server handler
            return Ok(RenderPlan {
                ctes: CteItems(vec![]),
                select: SelectItems(vec![]),
                from: FromTableItem(None),
                joins: JoinItems(vec![]),
                filters: FilterItems(None),
                group_by: GroupByExpressions(vec![]),
                order_by: OrderByItems(vec![]),
                skip: SkipItem(None),
                limit: LimitItem(None),
                union: UnionItems(None),
            });
        }
        
        // Variable-length paths are now supported via recursive CTE generation
        // Two-pass architecture:
        // 1. Analyze property requirements across the entire plan
        // 2. Generate CTEs with full context including required properties
        
        log::trace!("Starting render plan generation for plan type: {}", match self {
            LogicalPlan::Empty => "Empty",
            LogicalPlan::Scan(_) => "Scan",
            LogicalPlan::ViewScan(_) => "ViewScan",
            LogicalPlan::GraphNode(_) => "GraphNode",
            LogicalPlan::GraphRel(_) => "GraphRel",
            LogicalPlan::Filter(_) => "Filter",
            LogicalPlan::Projection(_) => "Projection",
            LogicalPlan::GraphJoins(_) => "GraphJoins",
            LogicalPlan::GroupBy(_) => "GroupBy",
            LogicalPlan::OrderBy(_) => "OrderBy",
            LogicalPlan::Skip(_) => "Skip",
            LogicalPlan::Limit(_) => "Limit",
            LogicalPlan::Cte(_) => "Cte",
            LogicalPlan::Union(_) => "Union",
            LogicalPlan::PageRank(_) => "PageRank",
        });
        
        // First pass: analyze what properties are needed
        let mut context = analyze_property_requirements(self);
        
        let mut extracted_ctes: Vec<Cte> = vec![];
        let final_from: Option<FromTable>;
        let final_filters: Option<RenderExpr>;

        let last_node_cte_opt = self.extract_last_node_cte()?;
        eprintln!("DEBUG: extract_last_node_cte returned: {:?}", last_node_cte_opt.is_some());

        if let Some(last_node_cte) = last_node_cte_opt {
            let last_node_alias = last_node_cte
                .cte_name
                .split('_')
                .nth(1)
                .ok_or(RenderBuildError::MalformedCTEName)?;

            // Second pass: generate CTEs with full context
            extracted_ctes = self.extract_ctes_with_context(last_node_alias, &mut context)?;
            
            // Check if we have a variable-length CTE (it will be a recursive RawSql CTE)
            let has_variable_length_cte = extracted_ctes.iter().any(|cte| {
                let is_recursive = cte.is_recursive;
                let is_raw_sql = matches!(&cte.content, super::CteContent::RawSql(_));
                eprintln!("DEBUG: Checking CTE '{}': is_recursive={}, is_raw_sql={}, combined={}", 
                    cte.cte_name, is_recursive, is_raw_sql, is_recursive && is_raw_sql);
                is_recursive && is_raw_sql
            });
            
            eprintln!("DEBUG: has_variable_length_cte = {}", has_variable_length_cte);
            eprintln!("DEBUG: extracted_ctes count = {}", extracted_ctes.len());
            for (i, cte) in extracted_ctes.iter().enumerate() {
                eprintln!("DEBUG: CTE {}: name={}, is_recursive={}, content_type={}", 
                    i, cte.cte_name, cte.is_recursive, 
                    match &cte.content {
                        super::CteContent::Structured(_) => "Structured",
                        super::CteContent::RawSql(_) => "RawSql",
                    });
            }
            
            eprintln!("DEBUG: About to check has_variable_length_cte condition: {}", has_variable_length_cte);
            
            if has_variable_length_cte {
                // For variable-length paths, use the CTE itself as the FROM clause
                let var_len_cte = extracted_ctes.iter()
                    .find(|cte| cte.is_recursive)
                    .expect("Variable-length CTE should exist");
                    
                // Create a ViewTableRef that references the CTE by name
                // We'll use an empty LogicalPlan as the source since the CTE is already defined
                final_from = Some(super::FromTable::new(Some(super::ViewTableRef {
                    source: std::sync::Arc::new(crate::query_planner::logical_plan::LogicalPlan::Empty),
                    name: var_len_cte.cte_name.clone(),
                    alias: Some("t".to_string()), // CTE uses 't' as alias
                })));
                
                // Check if there are end filters stored in the context that need to be applied to the outer query
                final_filters = context.get_end_filters_for_outer_query().cloned().map(|filters| {
                    eprintln!("DEBUG: Found end filters in context: {:?}", filters);
                    // Rewrite the filters to use the CTE table alias 't' instead of Cypher aliases
                    let start_alias = context.get_start_cypher_alias().unwrap_or("a");
                    let end_alias = context.get_end_cypher_alias().unwrap_or("b");
                    let rewritten = rewrite_end_filters_for_variable_length_cte(&filters, "t", start_alias, end_alias);
                    eprintln!("DEBUG: Rewritten end filters: {:?}", rewritten);
                    rewritten
                });
                if final_filters.is_some() {
                    eprintln!("DEBUG: Applied end filters to final_filters");
                } else {
                    eprintln!("DEBUG: No end filters found in context");
                }
            } else {
                // Extract from the CTE content (normal path)
                let (cte_from, cte_filters) = match &last_node_cte.content {
                    super::CteContent::Structured(plan) => (plan.from.0.clone(), plan.filters.0.clone()),
                    super::CteContent::RawSql(_) => (None, None), // Raw SQL CTEs don't have structured access
                };
                
                final_from = view_ref_to_from_table(cte_from);

                let last_node_filters_opt = clean_last_node_filters(cte_filters);

                let final_filters_opt = self.extract_final_filters()?;

                let final_combined_filters =
                    if last_node_filters_opt.is_some() && final_filters_opt.is_some() {
                        Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
                            operator: Operator::And,
                            operands: vec![final_filters_opt.unwrap(), last_node_filters_opt.unwrap()],
                        }))
                    } else if final_filters_opt.is_some() {
                        final_filters_opt
                    } else if last_node_filters_opt.is_some() {
                        last_node_filters_opt
                    } else {
                        None
                    };

                final_filters = final_combined_filters;
            }
        } else {
            // No CTE wrapper, but check for variable-length paths which generate CTEs directly
            // Extract CTEs with a dummy alias and context (variable-length doesn't use the alias)
            eprintln!("DEBUG: Taking second path - no last_node_cte");
            extracted_ctes = self.extract_ctes_with_context("_", &mut context)?;
            
            // Check if we have a variable-length CTE
            let has_variable_length_cte = extracted_ctes.iter().any(|cte| 
                cte.is_recursive && matches!(&cte.content, super::CteContent::RawSql(_))
            );
            
            if has_variable_length_cte {
                // For variable-length paths, use the CTE itself as the FROM clause
                let var_len_cte = extracted_ctes.iter()
                    .find(|cte| cte.is_recursive)
                    .expect("Variable-length CTE should exist");
                    
                // Create a ViewTableRef that references the CTE by name
                final_from = Some(super::FromTable::new(Some(super::ViewTableRef {
                    source: std::sync::Arc::new(crate::query_planner::logical_plan::LogicalPlan::Empty),
                    name: var_len_cte.cte_name.clone(),
                    alias: Some("t".to_string()), // CTE uses 't' as alias
                })));
                // For variable-length paths, apply end filters in the outer query
                if let Some((start_alias, end_alias)) = has_variable_length_rel(self) {
                    final_filters = context.get_end_filters_for_outer_query().cloned().map(|expr| {
                        eprintln!("DEBUG: Found end filters in context: {:?}", expr);
                        // Rewrite the filters to use the CTE table alias 't' instead of Cypher aliases
                        let rewritten = rewrite_end_filters_for_variable_length_cte(&expr, "t", &start_alias, &end_alias);
                        eprintln!("DEBUG: Rewritten end filters: {:?}", rewritten);
                        rewritten
                    });
                } else {
                    final_filters = None;
                }
            } else {
                // Normal case: no CTEs, extract FROM, joins, and filters normally
                final_from = self.extract_from()?;
                final_filters = self.extract_filters()?;
            }
        }

        let mut final_select_items = self.extract_select_items()?;
        
        // NOTE: Removed rewrite for select_items in variable-length paths to keep a.*, b.*

        let mut extracted_joins = self.extract_joins()?;
        
        // For variable-length paths, add joins to get full user data
        if has_variable_length_rel(self).is_some() {
            extracted_joins.push(Join {
                table_name: "users".to_string(),
                table_alias: "a".to_string(),
                joining_on: vec![OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias("t".to_string()),
                            column: Column("start_id".to_string()),
                        }),
                        RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias("a".to_string()),
                            column: Column("id".to_string()),
                        }),
                    ],
                }],
                join_type: JoinType::Join,
            });
            extracted_joins.push(Join {
                table_name: "users".to_string(),
                table_alias: "b".to_string(),
                joining_on: vec![OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias("t".to_string()),
                            column: Column("end_id".to_string()),
                        }),
                        RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias("b".to_string()),
                            column: Column("id".to_string()),
                        }),
                    ],
                }],
                join_type: JoinType::Join,
            });
        }
        
        // DEBUG: Log the extracted joins
        if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("debug_property_mapping.log") {
            let _ = writeln!(file, "DEBUG: Full plan structure before extract_joins:");
            let _ = writeln!(file, "DEBUG: Plan type: {:?}", match self {
                LogicalPlan::Empty => "Empty",
                LogicalPlan::Scan(_) => "Scan", 
                LogicalPlan::ViewScan(_) => "ViewScan",
                LogicalPlan::GraphNode(_) => "GraphNode",
                LogicalPlan::GraphRel(_) => "GraphRel",
                LogicalPlan::Filter(_) => "Filter",
                LogicalPlan::Projection(_) => "Projection",
                LogicalPlan::GraphJoins(_) => "GraphJoins",
                LogicalPlan::GroupBy(_) => "GroupBy",
                LogicalPlan::OrderBy(_) => "OrderBy",
                LogicalPlan::Skip(_) => "Skip",
                LogicalPlan::Limit(_) => "Limit",
                LogicalPlan::Cte(_) => "Cte",
                LogicalPlan::Union(_) => "Union",
                LogicalPlan::PageRank(_) => "PageRank",
            });
            let _ = writeln!(file, "DEBUG: extracted_joins count: {}", extracted_joins.len());
            for (i, join) in extracted_joins.iter().enumerate() {
                let _ = writeln!(file, "DEBUG: join {}: table='{}', alias='{}', type={:?}, conditions={}", 
                    i, join.table_name, join.table_alias, join.join_type, join.joining_on.len());
            }
        }
        // PATCH: For multi-relationship queries, ensure relationship joins use rel_table (CTE name) if present
        // This applies to both variable-length and regular multi-relationship queries
        // Find any non-recursive CTE named rel_*_* and update joins that reference it
        if let Some(union_cte) = extracted_ctes.iter().find(|cte| {
            cte.cte_name.starts_with("rel_") && !cte.is_recursive
        }) {
            let cte_name = union_cte.cte_name.clone();
            for join in extracted_joins.iter_mut() {
                // Update joins that are relationship CTEs
                if join.table_name.starts_with("FOLLOWS") || 
                   join.table_name.starts_with("FRIENDS_WITH") ||
                   join.table_name.starts_with("rel_") {
                    join.table_name = cte_name.clone();
                    // Also update joining_on expressions to use standardized column names
                    for op_app in join.joining_on.iter_mut() {
                        update_join_expression_for_union_cte(op_app, &join.table_alias);
                    }
                }
                // Also update any join that references the union CTE in its expressions
                else if references_union_cte_in_join(&join.joining_on, &cte_name) {
                    for op_app in join.joining_on.iter_mut() {
                        update_join_expression_for_union_cte(op_app, &cte_name);
                    }
                }
            }
        }
        // For variable-length (recursive) CTEs, keep previous logic
        if let Some(last_node_cte) = self.extract_last_node_cte().ok().flatten() {
            if let super::CteContent::RawSql(_) = &last_node_cte.content {
                let cte_name = last_node_cte.cte_name.clone();
                if cte_name.starts_with("rel_") {
                    for join in extracted_joins.iter_mut() {
                        join.table_name = cte_name.clone();
                    }
                }
            }
        }
        extracted_joins.sort_by_key(|join| join.joining_on.len());

        let mut extracted_group_by_exprs = self.extract_group_by()?;
        
        // Rewrite GROUP BY expressions for variable-length paths
        if let Some((left_alias, right_alias)) = has_variable_length_rel(self) {
            let path_var = get_path_variable(self);
            extracted_group_by_exprs = extracted_group_by_exprs.into_iter().map(|expr| {
                rewrite_expr_for_var_len_cte(&expr, &left_alias, &right_alias, path_var.as_deref())
            }).collect();
        }

        let mut extracted_order_by = self.extract_order_by()?;
        
        // Rewrite ORDER BY expressions for variable-length paths
        if let Some((left_alias, right_alias)) = has_variable_length_rel(self) {
            let path_var = get_path_variable(self);
            extracted_order_by = extracted_order_by.into_iter().map(|item| {
                OrderByItem {
                    expression: rewrite_expr_for_var_len_cte(&item.expression, &left_alias, &right_alias, path_var.as_deref()),
                    order: item.order,
                }
            }).collect();
        }

        let extracted_limit_item = self.extract_limit();

        let extracted_skip_item = self.extract_skip();

        let extracted_union = self.extract_union()?;

        Ok(RenderPlan {
            ctes: CteItems(extracted_ctes),
            select: SelectItems(final_select_items),
            from: FromTableItem(from_table_to_view_ref(final_from)),
            joins: JoinItems(extracted_joins),
            filters: FilterItems(final_filters),
            group_by: GroupByExpressions(extracted_group_by_exprs),
            order_by: OrderByItems(extracted_order_by),
            skip: SkipItem(extracted_skip_item),
            limit: LimitItem(extracted_limit_item),
            union: UnionItems(extracted_union),
        })
    }
}

fn clean_last_node_filters(filter_opt: Option<RenderExpr>) -> Option<RenderExpr> {
    if let Some(filter_expr) = filter_opt {
        match filter_expr {
            // remove InSubqeuery as we have added it in graph_traversal_planning phase. Since this is for last node, we are going to select that node directly
            // we do not need this InSubquery
            RenderExpr::InSubquery(_sq) => None,
            RenderExpr::OperatorApplicationExp(op) => {
                let mut stripped = Vec::new();
                for operand in op.operands {
                    if let Some(e) = clean_last_node_filters(Some(operand)) {
                        stripped.push(e);
                    }
                }
                match stripped.len() {
                    0 => None,
                    1 => Some(stripped.into_iter().next().unwrap()),
                    _ => Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
                        operator: op.operator,
                        operands: stripped,
                    })),
                }
            }
            RenderExpr::List(list) => {
                let mut stripped = Vec::new();
                for inner in list {
                    if let Some(e) = clean_last_node_filters(Some(inner)) {
                        stripped.push(e);
                    }
                }
                match stripped.len() {
                    0 => None,
                    1 => Some(stripped.into_iter().next().unwrap()),
                    _ => Some(RenderExpr::List(stripped)),
                }
            }
            RenderExpr::AggregateFnCall(agg) => {
                let mut stripped_args = Vec::new();
                for arg in agg.args {
                    if let Some(e) = clean_last_node_filters(Some(arg)) {
                        stripped_args.push(e);
                    }
                }
                if stripped_args.is_empty() {
                    None
                } else {
                    Some(RenderExpr::AggregateFnCall(AggregateFnCall {
                        name: agg.name,
                        args: stripped_args,
                    }))
                }
            }
            RenderExpr::ScalarFnCall(func) => {
                let mut stripped_args = Vec::new();
                for arg in func.args {
                    if let Some(e) = clean_last_node_filters(Some(arg)) {
                        stripped_args.push(e);
                    }
                }
                if stripped_args.is_empty() {
                    None
                } else {
                    Some(RenderExpr::ScalarFnCall(ScalarFnCall {
                        name: func.name,
                        args: stripped_args,
                    }))
                }
            }
            other => Some(other),
            // RenderExpr::PropertyAccessExp(pa) => Some(RenderExpr::PropertyAccessExp(pa)),
            // RenderExpr::Literal(l) => Some(RenderExpr::Literal(l)),
            // RenderExpr::Variable(v) => Some(RenderExpr::Variable(v)),
            // RenderExpr::Star => Some(RenderExpr::Star),
            // RenderExpr::TableAlias(ta) => Some(RenderExpr::TableAlias(ta)),
            // RenderExpr::ColumnAlias(ca) => Some(RenderExpr::ColumnAlias(ca)),
            // RenderExpr::Column(c) => Some(RenderExpr::Column(c)),
            // RenderExpr::Parameter(p) => Some(RenderExpr::Parameter(p)),
        }
    } else {
        None
    }
}

/// Post-process a RenderExpr to apply property mapping based on node labels
/// This function recursively walks the expression tree and maps property names to column names
fn plan_to_string(plan: &LogicalPlan, depth: usize) -> String {
    let indent = "  ".repeat(depth);
    match plan {
        LogicalPlan::GraphNode(node) => format!("{}GraphNode(alias='{}', input={})", indent, node.alias, plan_to_string(&node.input, depth + 1)),
        LogicalPlan::GraphRel(rel) => format!("{}GraphRel(alias='{}', left={}, center={}, right={})", indent, rel.alias,
            plan_to_string(&rel.left, depth + 1), plan_to_string(&rel.center, depth + 1), plan_to_string(&rel.right, depth + 1)),
        LogicalPlan::Filter(filter) => format!("{}Filter(input={})", indent, plan_to_string(&filter.input, depth + 1)),
        LogicalPlan::Projection(proj) => format!("{}Projection(input={})", indent, plan_to_string(&proj.input, depth + 1)),
        LogicalPlan::ViewScan(scan) => format!("{}ViewScan(table='{}')", indent, scan.source_table),
        LogicalPlan::Scan(scan) => format!("{}Scan(table={:?})", indent, scan.table_name),
        _ => format!("{}Other({})", indent, plan_type_name(plan)),
    }
}

fn plan_type_name(plan: &LogicalPlan) -> &'static str {
    match plan {
        LogicalPlan::Empty => "Empty",
        LogicalPlan::Scan(_) => "Scan",
        LogicalPlan::ViewScan(_) => "ViewScan",
        LogicalPlan::GraphNode(_) => "GraphNode",
        LogicalPlan::GraphRel(_) => "GraphRel",
        LogicalPlan::Filter(_) => "Filter",
        LogicalPlan::Projection(_) => "Projection",
        LogicalPlan::GraphJoins(_) => "GraphJoins",
        LogicalPlan::GroupBy(_) => "GroupBy",
        LogicalPlan::OrderBy(_) => "OrderBy",
        LogicalPlan::Skip(_) => "Skip",
        LogicalPlan::Limit(_) => "Limit",
        LogicalPlan::Cte(_) => "Cte",
        LogicalPlan::Union(_) => "Union",
        LogicalPlan::PageRank(_) => "PageRank",
    }
}

fn apply_property_mapping_to_expr(expr: &mut RenderExpr, plan: &LogicalPlan) {
    // Write debug info to a file
    use std::fs::OpenOptions;
    use std::io::Write;
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("debug_property_mapping.log") {
        let _ = writeln!(file, "DEBUG: apply_property_mapping_to_expr called!");
        let _ = writeln!(file, "DEBUG: Plan structure: {}", plan_to_string(plan, 0));
    }
    match expr {
        RenderExpr::PropertyAccessExp(prop) => {
            // Get the node label for this table alias
            if let Some(node_label) = get_node_label_for_alias(&prop.table_alias.0, plan) {
                if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("debug_property_mapping.log") {
                    let _ = writeln!(file, "DEBUG: apply_property_mapping_to_expr: Found node label '{}' for alias '{}', property '{}'",
                        node_label, prop.table_alias.0, prop.column.0);
                }
                // Map the property to the correct column
                let mapped_column = map_property_to_column_with_schema(&prop.column.0, &node_label);
                if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("debug_property_mapping.log") {
                    let _ = writeln!(file, "DEBUG: apply_property_mapping_to_expr: Mapped property '{}' to column '{}'", prop.column.0, mapped_column);
                }
                prop.column = super::render_expr::Column(mapped_column);
            } else {
                if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("debug_property_mapping.log") {
                    let _ = writeln!(file, "DEBUG: apply_property_mapping_to_expr: No node label found for alias '{}'", prop.table_alias.0);
                }
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in &mut op.operands {
                apply_property_mapping_to_expr(operand, plan);
            }
        }
        RenderExpr::ScalarFnCall(func) => {
            for arg in &mut func.args {
                apply_property_mapping_to_expr(arg, plan);
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            for arg in &mut agg.args {
                apply_property_mapping_to_expr(arg, plan);
            }
        }
        RenderExpr::List(list) => {
            for item in list {
                apply_property_mapping_to_expr(item, plan);
            }
        }
        RenderExpr::InSubquery(subq) => {
            apply_property_mapping_to_expr(&mut subq.expr, plan);
        }
        // Other expression types don't contain nested expressions
        _ => {}
    }
}

/// Get the node label for a given Cypher alias by searching the plan
fn get_node_label_for_alias(alias: &str, plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::GraphNode(node) if node.alias == alias => {
            extract_node_label_from_viewscan(&node.input)
        }
        LogicalPlan::GraphNode(node) => get_node_label_for_alias(alias, &node.input),
        LogicalPlan::GraphRel(rel) => {
            get_node_label_for_alias(alias, &rel.left)
                .or_else(|| get_node_label_for_alias(alias, &rel.center))
                .or_else(|| get_node_label_for_alias(alias, &rel.right))
        }
        LogicalPlan::Filter(filter) => get_node_label_for_alias(alias, &filter.input),
        LogicalPlan::Projection(proj) => get_node_label_for_alias(alias, &proj.input),
        LogicalPlan::GraphJoins(joins) => get_node_label_for_alias(alias, &joins.input),
        LogicalPlan::OrderBy(order_by) => get_node_label_for_alias(alias, &order_by.input),
        LogicalPlan::Skip(skip) => get_node_label_for_alias(alias, &skip.input),
        LogicalPlan::Limit(limit) => get_node_label_for_alias(alias, &limit.input),
        LogicalPlan::GroupBy(group_by) => get_node_label_for_alias(alias, &group_by.input),
        LogicalPlan::Cte(cte) => get_node_label_for_alias(alias, &cte.input),
        LogicalPlan::Union(union) => {
            for input in &union.inputs {
                if let Some(label) = get_node_label_for_alias(alias, input) {
                    return Some(label);
                }
            }
            None
        }
        _ => None,
    }
}

fn references_union_cte_in_join(joining_on: &[OperatorApplication], cte_name: &str) -> bool {
    for op_app in joining_on {
        if references_union_cte_in_operand(&op_app.operands[0], cte_name) ||
           references_union_cte_in_operand(&op_app.operands[1], cte_name) {
            return true;
        }
    }
    false
}

fn references_union_cte_in_operand(operand: &RenderExpr, cte_name: &str) -> bool {
    match operand {
        RenderExpr::PropertyAccessExp(prop_access) => {
            // Check if this property access references the union CTE
            // We can't easily check table alias here, but we can check if it references the CTE name
            // For now, just check if it's a property access that might need updating
            prop_access.column.0 == "from_id" || prop_access.column.0 == "to_id"
        }
        RenderExpr::OperatorApplicationExp(op_app) => {
            references_union_cte_in_join(&[op_app.clone()], cte_name)
        }
        _ => false,
    }
}

fn update_join_expression_for_union_cte(op_app: &mut OperatorApplication, table_alias: &str) {
    // Recursively update expressions to use standardized column names for union CTEs
    for operand in op_app.operands.iter_mut() {
        update_operand_for_union_cte(operand, table_alias);
    }
}

fn update_operand_for_union_cte(operand: &mut RenderExpr, table_alias: &str) {
    match operand {
        RenderExpr::Column(col) => {
            // Update column references to use standardized names
            if col.0 == "from_id" {
                *operand = RenderExpr::Column(Column("from_node_id".to_string()));
            } else if col.0 == "to_id" {
                *operand = RenderExpr::Column(Column("to_node_id".to_string()));
            }
        }
        RenderExpr::PropertyAccessExp(prop_access) => {
            // Update property access column references
            if prop_access.column.0 == "from_id" {
                prop_access.column = Column("from_node_id".to_string());
            } else if prop_access.column.0 == "to_id" {
                prop_access.column = Column("to_node_id".to_string());
            }
        }
        RenderExpr::OperatorApplicationExp(inner_op_app) => {
            update_join_expression_for_union_cte(inner_op_app, table_alias);
        }
        _ => {} // Other expression types don't need updating
    }
}
