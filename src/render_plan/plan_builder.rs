use std::sync::Arc;
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::query_planner::logical_plan::{LogicalPlan, GraphRel};
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
use super::cte_generation::{analyze_property_requirements, map_property_to_column_with_schema, extract_var_len_properties};
use super::filter_pipeline::{
    categorize_filters, clean_last_node_filters, extract_start_end_filters, filter_expr_to_sql,
    render_end_filter_to_column_alias, rewrite_end_filters_for_variable_length_cte,
    rewrite_expr_for_outer_query, rewrite_expr_for_var_len_cte, CategorizedFilters,
};
use crate::render_plan::cte_extraction::extract_ctes_with_context;
use crate::render_plan::cte_extraction::{label_to_table_name, rel_types_to_table_names, rel_type_to_table_name, table_to_id_column, extract_relationship_columns, RelationshipColumns, extract_node_label_from_viewscan, has_variable_length_rel, get_path_variable};

// Import helper functions from the dedicated helpers module
use super::plan_builder_helpers;

pub type RenderPlanBuilderResult<T> = Result<T, super::errors::RenderBuildError>;

/// Helper function to extract the actual table name from a LogicalPlan node
/// NOTE: This function is duplicated in plan_builder_helpers.rs
/// TODO: Remove this version and use plan_builder_helpers::extract_table_name() instead
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


/// Helper function to find the table name for a given alias by recursively searching the plan tree
/// Used to find the anchor node's table in multi-hop queries
fn find_table_name_for_alias(plan: &LogicalPlan, target_alias: &str) -> Option<String> {
    match plan {
        LogicalPlan::GraphNode(node) => {
            if node.alias == target_alias {
                // Found the matching GraphNode, extract table name from its input
                match &*node.input {
                    LogicalPlan::ViewScan(scan) => Some(scan.source_table.clone()),
                    _ => None,
                }
            } else {
                // Not a match, recurse into input
                find_table_name_for_alias(&node.input, target_alias)
            }
        },
        LogicalPlan::GraphRel(rel) => {
            // Search in both left and right branches
            find_table_name_for_alias(&rel.left, target_alias)
                .or_else(|| find_table_name_for_alias(&rel.right, target_alias))
        },
        LogicalPlan::Projection(proj) => find_table_name_for_alias(&proj.input, target_alias),
        LogicalPlan::Filter(filter) => find_table_name_for_alias(&filter.input, target_alias),
        LogicalPlan::OrderBy(order) => find_table_name_for_alias(&order.input, target_alias),
        LogicalPlan::GraphJoins(joins) => find_table_name_for_alias(&joins.input, target_alias),
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
        RenderExpr::Raw(raw) => raw.clone(),
        RenderExpr::PropertyAccessExp(prop) => {
            // Convert property access to table.column format
            // Apply alias mapping to convert Cypher aliases to CTE aliases
            let table_alias = alias_mapping.iter()
                .find(|(cypher, _)| *cypher == prop.table_alias.0)
                .map(|(_, cte)| cte.clone())
                .unwrap_or_else(|| prop.table_alias.0.clone());
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

/// Helper function to check if a plan tree contains a GraphRel with multiple relationships
fn has_multiple_relationships(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::GraphRel(graph_rel) => {
            if let Some(labels) = &graph_rel.labels {
                labels.len() > 1
            } else {
                false
            }
        }
        LogicalPlan::Projection(proj) => has_multiple_relationships(&proj.input),
        LogicalPlan::Filter(filter) => has_multiple_relationships(&filter.input),
        LogicalPlan::GraphJoins(graph_joins) => has_multiple_relationships(&graph_joins.input),
        LogicalPlan::GraphNode(graph_node) => has_multiple_relationships(&graph_node.input),
        _ => false,
    }
}

/// Helper function to extract multiple relationship info from a plan tree
fn get_multiple_rel_info(plan: &LogicalPlan) -> Option<(String, String, String)> {
    match plan {
        LogicalPlan::GraphRel(graph_rel) => {
            if let Some(labels) = &graph_rel.labels {
                if labels.len() > 1 {
                    let cte_name = format!("rel_{}_{}", graph_rel.left_connection, graph_rel.right_connection);
                    Some((graph_rel.left_connection.clone(), graph_rel.right_connection.clone(), cte_name))
                } else {
                    None
                }
            } else {
                None
            }
        }
        LogicalPlan::Projection(proj) => get_multiple_rel_info(&proj.input),
        LogicalPlan::Filter(filter) => get_multiple_rel_info(&filter.input),
        LogicalPlan::GraphJoins(graph_joins) => get_multiple_rel_info(&graph_joins.input),
        LogicalPlan::GraphNode(graph_node) => get_multiple_rel_info(&graph_node.input),
        _ => None,
    }
}

/// Helper function to check if an expression is standalone (doesn't reference any table columns)
/// Returns true for literals, parameters, and functions that only use standalone expressions
fn is_standalone_expression(expr: &RenderExpr) -> bool {
    match expr {
        RenderExpr::Literal(_) | RenderExpr::Parameter(_) | RenderExpr::Star => true,
        RenderExpr::ScalarFnCall(fn_call) => {
            // Function is standalone if all its arguments are standalone
            fn_call.args.iter().all(is_standalone_expression)
        }
        RenderExpr::OperatorApplicationExp(op) => {
            // Operator application is standalone if all operands are standalone
            op.operands.iter().all(is_standalone_expression)
        }
        RenderExpr::Case(case_expr) => {
            // CASE is standalone if all branches are standalone
            let when_then_standalone = case_expr.when_then.iter().all(|(cond, result)| {
                is_standalone_expression(cond) && is_standalone_expression(result)
            });
            let else_standalone = case_expr.else_expr.as_ref().map_or(true, |e| is_standalone_expression(e));
            when_then_standalone && else_standalone
        }
        RenderExpr::List(list) => {
            // List is standalone if all elements are standalone
            list.iter().all(is_standalone_expression)
        }
        // Any reference to columns, properties, or aliases means it's not standalone
        RenderExpr::Column(_) | 
        RenderExpr::PropertyAccessExp(_) | 
        RenderExpr::TableAlias(_) | 
        RenderExpr::ColumnAlias(_) |
        RenderExpr::AggregateFnCall(_) |
        RenderExpr::InSubquery(_) => false,
        RenderExpr::Raw(_) => false, // Be conservative with raw SQL
    }
}

/// Helper function to extract all relationship connections from a plan tree
/// Returns a vector of (left_connection, right_connection, relationship_alias) tuples
fn get_all_relationship_connections(plan: &LogicalPlan) -> Vec<(String, String, String)> {
    let mut connections = vec![];
    
    fn collect_connections(plan: &LogicalPlan, connections: &mut Vec<(String, String, String)>) {
        match plan {
            LogicalPlan::GraphRel(graph_rel) => {
                connections.push((
                    graph_rel.left_connection.clone(),
                    graph_rel.right_connection.clone(),
                    graph_rel.alias.clone(),
                ));
                // Recurse into nested GraphRels (multi-hop chains)
                collect_connections(&graph_rel.left, connections);
                collect_connections(&graph_rel.right, connections);
            }
            LogicalPlan::Projection(proj) => collect_connections(&proj.input, connections),
            LogicalPlan::Filter(filter) => collect_connections(&filter.input, connections),
            LogicalPlan::GraphJoins(graph_joins) => collect_connections(&graph_joins.input, connections),
            LogicalPlan::GraphNode(graph_node) => collect_connections(&graph_node.input, connections),
            _ => {}
        }
    }
    
    collect_connections(plan, &mut connections);
    connections
}

/// Helper function to find the anchor/first node in a multi-hop pattern
/// The anchor is the node that should be in the FROM clause
/// Strategy: Prefer required (non-optional) nodes over optional nodes
/// When mixing MATCH and OPTIONAL MATCH, the required node should be the anchor (FROM table)
/// 
/// Algorithm:
/// 1. Collect all unique nodes (from both left and right connections)
/// 2. Prefer nodes that are NOT in optional_aliases (required nodes)
/// 3. Fall back to traditional anchor pattern (left-but-not-right) if no required nodes found
fn find_anchor_node(connections: &[(String, String, String)], optional_aliases: &std::collections::HashSet<String>) -> Option<String> {
    if connections.is_empty() {
        return None;
    }
    
    // Strategy 1: Prefer LEFT connections that are required (not optional)
    // Check LEFT nodes first since they should be the anchor in (a)-[]->(b) patterns
    for (left, _, _) in connections {
        if !optional_aliases.contains(left) {
            log::info!("✓ Found REQUIRED LEFT anchor node: {} (not in optional_aliases)", left);
            return Some(left.clone());
        }
    }
    
    // Strategy 2: If all LEFT nodes are optional, check RIGHT nodes that are required
    for (_, right, _) in connections {
        if !optional_aliases.contains(right) {
            log::info!("✓ Found REQUIRED RIGHT anchor node: {} (not in optional_aliases)", right);
            return Some(right.clone());
        }
    }
    
    // Strategy 3: All nodes are optional - use traditional anchor pattern
    // (left_connection that is NOT in right_nodes)
    let right_nodes: std::collections::HashSet<_> = connections.iter()
        .map(|(_, right, _)| right.clone())
        .collect();
    
    for (left, _, _) in connections {
        if !right_nodes.contains(left) {
            log::warn!("⚠️ All nodes are optional, using anchor pattern: {}", left);
            return Some(left.clone());
        }
    }
    
    // Strategy 4: Fallback to first left_connection
    let fallback = connections.first().map(|(left, _, _)| left.clone());
    if let Some(ref alias) = fallback {
        log::warn!("⚠️ Using fallback anchor: {}", alias);
    }
    fallback
}

/// Helper function to check if a condition references an end node alias
fn references_end_node_alias(condition: &OperatorApplication, connections: &[(String, String, String)]) -> bool {
    let end_aliases: std::collections::HashSet<String> = connections.iter()
        .map(|(_, right_alias, _)| right_alias.clone())
        .collect();
    
    // Check if any operand in the condition references an end node alias
    condition.operands.iter().any(|operand| {
        match operand {
            RenderExpr::PropertyAccessExp(prop) => {
                end_aliases.contains(&prop.table_alias.0)
            }
            _ => false,
        }
    })
}

/// Check if a condition references a specific node alias
fn references_node_alias(condition: &OperatorApplication, node_alias: &str) -> bool {
    condition.operands.iter().any(|operand| {
        match operand {
            RenderExpr::PropertyAccessExp(prop) => {
                prop.table_alias.0 == node_alias
            }
            _ => false,
        }
    })
}

/// Rewrite path function calls (length, nodes, relationships) to CTE column references
/// Converts: length(p) → hop_count, nodes(p) → path_nodes, relationships(p) → path_relationships
fn rewrite_path_functions(expr: &RenderExpr, path_var_name: &str) -> RenderExpr {
    rewrite_path_functions_with_table(expr, path_var_name, "")
}

/// Rewrite path function calls with optional table alias
/// table_alias: if provided, generates PropertyAccessExp (table.column), otherwise Column
fn rewrite_path_functions_with_table(expr: &RenderExpr, path_var_name: &str, table_alias: &str) -> RenderExpr {
    match expr {
        RenderExpr::ScalarFnCall(fn_call) => {
            // Check if this is a path function call with the path variable as argument
            if fn_call.args.len() == 1 {
                if let RenderExpr::TableAlias(TableAlias(alias)) = &fn_call.args[0] {
                    if alias == path_var_name {
                        // Convert path functions to CTE column references
                        let column_name = match fn_call.name.as_str() {
                            "length" => Some("hop_count"),
                            "nodes" => Some("path_nodes"),
                            "relationships" => Some("path_relationships"),
                            _ => None,
                        };
                        
                        if let Some(col_name) = column_name {
                            return if table_alias.is_empty() {
                                RenderExpr::Column(Column(col_name.to_string()))
                            } else {
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(table_alias.to_string()),
                                    column: Column(col_name.to_string()),
                                })
                            };
                        }
                    }
                }
            }
            
            // Recursively rewrite arguments for nested calls
            let rewritten_args: Vec<RenderExpr> = fn_call.args.iter()
                .map(|arg| rewrite_path_functions_with_table(arg, path_var_name, table_alias))
                .collect();
            
            RenderExpr::ScalarFnCall(ScalarFnCall {
                name: fn_call.name.clone(),
                args: rewritten_args,
            })
        }
        RenderExpr::OperatorApplicationExp(op) => {
            // Recursively rewrite operands
            let rewritten_operands: Vec<RenderExpr> = op.operands.iter()
                .map(|operand| rewrite_path_functions_with_table(operand, path_var_name, table_alias))
                .collect();
            
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: op.operator.clone(),
                operands: rewritten_operands,
            })
        }
        RenderExpr::PropertyAccessExp(prop) => {
            // Don't rewrite property access - it's handled separately
            expr.clone()
        }
        RenderExpr::AggregateFnCall(agg) => {
            // Recursively rewrite arguments for aggregate functions
            let rewritten_args: Vec<RenderExpr> = agg.args.iter()
                .map(|arg| rewrite_path_functions_with_table(arg, path_var_name, table_alias))
                .collect();
            
            RenderExpr::AggregateFnCall(AggregateFnCall {
                name: agg.name.clone(),
                args: rewritten_args,
            })
        }
        _ => expr.clone(), // For other expression types, return as-is
    }
}

/// Helper function to get node table name for a given alias
fn get_node_table_for_alias(alias: &str) -> String {
    // Try to get from global schema first (for production/benchmark)
    if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
        if let Ok(schemas) = schemas_lock.try_read() {
            if let Some(schema) = schemas.get("default") {
                // Look up the node type from the alias - this is a simplified lookup
                // In a real implementation, we'd need to track node types per alias
                // For now, assume "User" type for common cases
                if let Some(user_node) = schema.get_node_schema_opt("User") {
                    return user_node.table_name.clone();
                }
            }
        }
    }

    // Fallback for tests and when schema is not available
    // For benchmark environment, use users_bench
    // For tests, use users
    if alias.contains("bench") || std::env::var("BENCHMARK_MODE").is_ok() {
        "users_bench".to_string()
    } else {
        "users".to_string()
    }
}

/// Helper function to get node ID column for a given alias
fn get_node_id_column_for_alias(alias: &str) -> String {
    // Try to get from global schema first (for production/benchmark)
    if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
        if let Ok(schemas) = schemas_lock.try_read() {
            if let Some(schema) = schemas.get("default") {
                // Look up the node type from the alias - this is a simplified lookup
                if let Some(user_node) = schema.get_node_schema_opt("User") {
                    return user_node.node_id.column.clone();
                }
            }
        }
    }

    // Fallback for tests and when schema is not available
    // For benchmark environment, use user_id
    // For tests, use id
    if alias.contains("bench") || std::env::var("BENCHMARK_MODE").is_ok() {
        "user_id".to_string()
    } else {
        "id".to_string()
    }
}

use super::CteGenerationContext;

/// Get relationship columns from schema by relationship type
/// Returns (from_column, to_column) for a given relationship type
fn get_relationship_columns_from_schema(rel_type: &str) -> Option<(String, String)> {
    if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
        if let Ok(schemas) = schemas_lock.try_read() {
            if let Some(schema) = schemas.get("default") {
                if let Ok(rel_schema) = schema.get_rel_schema(rel_type) {
                    return Some((
                        rel_schema.from_id.clone(),  // Use column names, not node types!
                        rel_schema.to_id.clone(),
                    ));
                }
            }
        }
    }
    None
}

/// Get relationship columns from schema by table name
/// Searches all relationship schemas to find one with matching table name
fn get_relationship_columns_by_table(table_name: &str) -> Option<(String, String)> {
    if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
        if let Ok(schemas) = schemas_lock.try_read() {
            if let Some(schema) = schemas.get("default") {
                // Search through all relationship schemas for one with matching table name
                for (_key, rel_schema) in schema.get_relationships_schemas().iter() {
                    if rel_schema.table_name == table_name {
                        return Some((
                            rel_schema.from_id.clone(),  // Use column names!
                            rel_schema.to_id.clone(),
                        ));
                    }
                }
            }
        }
    }
    None
}

/// Get node table name and ID column from schema
/// Returns (table_name, id_column) for a given node label
fn get_node_info_from_schema(node_label: &str) -> Option<(String, String)> {
    if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
        if let Ok(schemas) = schemas_lock.try_read() {
            if let Some(schema) = schemas.get("default") {
                if let Ok(node_schema) = schema.get_node_schema(node_label) {
                    return Some((
                        node_schema.table_name.clone(),
                        node_schema.node_id.column.clone(),
                    ));
                }
            }
        }
    }
    None
}

/// Check if a logical plan contains any GraphRel with multiple relationship types
fn has_multiple_relationship_types(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::GraphRel(graph_rel) => {
            if let Some(labels) = &graph_rel.labels {
                if labels.len() > 1 {
                    return true;
                }
            }
            // Check child plans
            has_multiple_relationship_types(&graph_rel.left) || has_multiple_relationship_types(&graph_rel.right)
        }
        LogicalPlan::GraphJoins(joins) => has_multiple_relationship_types(&joins.input),
        LogicalPlan::Projection(proj) => has_multiple_relationship_types(&proj.input),
        LogicalPlan::Filter(filter) => has_multiple_relationship_types(&filter.input),
        LogicalPlan::GraphNode(node) => has_multiple_relationship_types(&node.input),
        LogicalPlan::GroupBy(gb) => has_multiple_relationship_types(&gb.input),
        LogicalPlan::OrderBy(ob) => has_multiple_relationship_types(&ob.input),
        LogicalPlan::Limit(limit) => has_multiple_relationship_types(&limit.input),
        LogicalPlan::Skip(skip) => has_multiple_relationship_types(&skip.input),
        _ => false,
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
/// Categorize WHERE clause filters based on which node/relationship they reference
/// This is critical for shortest path queries where:


pub(crate) trait RenderPlanBuilder {
    fn extract_last_node_cte(&self) -> RenderPlanBuilderResult<Option<Cte>>;

    fn extract_final_filters(&self) -> RenderPlanBuilderResult<Option<RenderExpr>>;

    fn extract_ctes(&self, last_node_alias: &str) -> RenderPlanBuilderResult<Vec<Cte>>;
    
    fn extract_ctes_with_context(&self, last_node_alias: &str, context: &mut CteGenerationContext) -> RenderPlanBuilderResult<Vec<Cte>>;
    
    /// Find the ID column for a given table alias by traversing the logical plan
    fn find_id_column_for_alias(&self, alias: &str) -> RenderPlanBuilderResult<String>;

    /// Normalize aggregate function arguments: convert TableAlias(a) to PropertyAccess(a.id_column)
    /// This is needed for queries like COUNT(b) where b is a node alias
    fn normalize_aggregate_args(&self, expr: RenderExpr) -> RenderPlanBuilderResult<RenderExpr>;

    fn extract_select_items(&self) -> RenderPlanBuilderResult<Vec<SelectItem>>;

    fn extract_from(&self) -> RenderPlanBuilderResult<Option<FromTable>>;

    fn extract_filters(&self) -> RenderPlanBuilderResult<Option<RenderExpr>>;

    fn extract_joins(&self) -> RenderPlanBuilderResult<Vec<Join>>;

    fn extract_group_by(&self) -> RenderPlanBuilderResult<Vec<RenderExpr>>;

    fn extract_having(&self) -> RenderPlanBuilderResult<Option<RenderExpr>>;

    fn extract_order_by(&self) -> RenderPlanBuilderResult<Vec<OrderByItem>>;

    fn extract_limit(&self) -> Option<i64>;

    fn extract_skip(&self) -> Option<i64>;

    fn extract_union(&self) -> RenderPlanBuilderResult<Option<Union>>;

    fn try_build_join_based_plan(&self) -> RenderPlanBuilderResult<RenderPlan>;

    fn build_simple_relationship_render_plan(&self) -> RenderPlanBuilderResult<RenderPlan>;

    fn to_render_plan(&self, schema: &GraphSchema) -> RenderPlanBuilderResult<RenderPlan>;
}

impl RenderPlanBuilder for LogicalPlan {
    fn find_id_column_for_alias(&self, alias: &str) -> RenderPlanBuilderResult<String> {
        // Traverse the plan tree to find a GraphNode or ViewScan with matching alias
        match self {
            LogicalPlan::GraphNode(node) if node.alias == alias => {
                // Found the matching node - extract ID column from its ViewScan
                if let LogicalPlan::ViewScan(scan) = node.input.as_ref() {
                    return Ok(scan.id_column.clone());
                }
            }
            LogicalPlan::GraphRel(rel) => {
                // Check both left and right branches
                if let Ok(id) = rel.left.find_id_column_for_alias(alias) {
                    return Ok(id);
                }
                if let Ok(id) = rel.right.find_id_column_for_alias(alias) {
                    return Ok(id);
                }
            }
            LogicalPlan::Projection(proj) => {
                return proj.input.find_id_column_for_alias(alias);
            }
            LogicalPlan::Filter(filter) => {
                return filter.input.find_id_column_for_alias(alias);
            }
            LogicalPlan::GroupBy(gb) => {
                return gb.input.find_id_column_for_alias(alias);
            }
            LogicalPlan::GraphJoins(joins) => {
                return joins.input.find_id_column_for_alias(alias);
            }
            LogicalPlan::OrderBy(order) => {
                return order.input.find_id_column_for_alias(alias);
            }
            LogicalPlan::Skip(skip) => {
                return skip.input.find_id_column_for_alias(alias);
            }
            LogicalPlan::Limit(limit) => {
                return limit.input.find_id_column_for_alias(alias);
            }
            _ => {}
        }
        Err(RenderBuildError::InvalidRenderPlan(
            format!("Cannot find ID column for alias '{}'", alias)
        ))
    }

    fn normalize_aggregate_args(&self, expr: RenderExpr) -> RenderPlanBuilderResult<RenderExpr> {
        match expr {
            RenderExpr::AggregateFnCall(mut agg) => {
                // Recursively normalize all arguments
                agg.args = agg.args
                    .into_iter()
                    .map(|arg| self.normalize_aggregate_args(arg))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(RenderExpr::AggregateFnCall(agg))
            }
            RenderExpr::TableAlias(alias) => {
                // Convert COUNT(b) to COUNT(b.user_id)
                let id_col = self.find_id_column_for_alias(&alias.0)?;
                Ok(RenderExpr::PropertyAccessExp(super::render_expr::PropertyAccess {
                    table_alias: alias,
                    column: super::render_expr::Column(id_col),
                }))
            }
            RenderExpr::OperatorApplicationExp(mut op) => {
                // Recursively normalize operands
                op.operands = op.operands
                    .into_iter()
                    .map(|operand| self.normalize_aggregate_args(operand))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(RenderExpr::OperatorApplicationExp(op))
            }
            RenderExpr::ScalarFnCall(mut func) => {
                // Recursively normalize function arguments
                func.args = func.args
                    .into_iter()
                    .map(|arg| self.normalize_aggregate_args(arg))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(RenderExpr::ScalarFnCall(func))
            }
            // Other expressions pass through unchanged
            _ => Ok(expr),
        }
    }
    
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
                use crate::graph_catalog::graph_schema::GraphSchema;
                use std::collections::HashMap;
                let empty_schema = GraphSchema::build(1, "default".to_string(), HashMap::new(), HashMap::new());
                let render_cte = Cte {
                    cte_name: logical_cte.name.clone(),
                    content: super::CteContent::Structured(logical_cte.input.to_render_plan(&empty_schema)?),
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
                // Get node labels first, then convert to table names
                let start_label = extract_node_label_from_viewscan(&graph_rel.left)
                    .unwrap_or_else(|| "User".to_string()); // Fallback to User if not found
                let end_label = extract_node_label_from_viewscan(&graph_rel.right)
                    .unwrap_or_else(|| "User".to_string()); // Fallback to User if not found
                let start_table = label_to_table_name(&start_label);
                let end_table = label_to_table_name(&end_label);
                
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
                let rel_table = rel_tables.first().ok_or(RenderBuildError::NoRelationshipTablesFound)?.clone();                // Extract ID columns
                let start_id_col = extract_id_column(&graph_rel.left)
                    .unwrap_or_else(|| table_to_id_column(&start_table));
                let end_id_col = extract_id_column(&graph_rel.right)
                    .unwrap_or_else(|| table_to_id_column(&end_table));
                
                // Extract relationship columns from ViewScan (will use schema-specific names if available)
                let rel_cols = extract_relationship_columns(&graph_rel.center)
                    .unwrap_or(RelationshipColumns {
                        from_id: "from_node_id".to_string(),  // Generic fallback
                        to_id: "to_node_id".to_string(),      // Generic fallback
                    });
                let from_col = rel_cols.from_id;
                let to_col = rel_cols.to_id;
                
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
                        
                        // Categorize filters
                        let categorized = categorize_filters(
                            Some(&render_expr),
                            &start_alias,
                            &end_alias,
                            "", // rel_alias not used yet
                        );
                        
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

                // Regular single-hop relationship: use JOIN logic instead of CTEs
                // For simple relationships (single type, no variable-length), don't create CTEs
                // Let the normal plan building logic handle JOINs
                if rel_tables.len() == 1 && graph_rel.variable_length.is_none() {
                    // Simple relationship: no CTEs needed, use JOINs
                    return Ok(vec![]);
                }
                
                // Handle multiple relationship types or complex cases with UNION/CTEs
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
                use crate::graph_catalog::graph_schema::GraphSchema;
                use std::collections::HashMap;
                let empty_schema = GraphSchema::build(1, "default".to_string(), HashMap::new(), HashMap::new());
                Ok(vec![Cte {
                    cte_name: logical_cte.name.clone(),
                    content: super::CteContent::Structured(logical_cte.input.to_render_plan(&empty_schema)?),
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
        extract_ctes_with_context(self, last_node_alias, context)
    }

    fn extract_select_items(&self) -> RenderPlanBuilderResult<Vec<SelectItem>> {
        println!("DEBUG: extract_select_items called on: {:?}", self);
        let select_items = match &self {
            LogicalPlan::Empty => vec![],
            LogicalPlan::Scan(_) => vec![],
            LogicalPlan::ViewScan(view_scan) => {
                // Build select items from ViewScan's property mappings and projections
                // This is needed for multiple relationship types where ViewScan nodes are created
                // for start/end nodes but don't have explicit projections
                
                if !view_scan.projections.is_empty() {
                    // Use explicit projections if available
                    view_scan.projections.iter().map(|proj| {
                        let expr: RenderExpr = proj.clone().try_into()?;
                        Ok(SelectItem {
                            expression: expr,
                            col_alias: None,
                        })
                    }).collect::<Result<Vec<SelectItem>, RenderBuildError>>()?
                } else if !view_scan.property_mapping.is_empty() {
                    // Fall back to property mappings - build select items for each property
                    view_scan.property_mapping.iter().map(|(prop_name, col_name)| {
                        Ok(SelectItem {
                            expression: RenderExpr::Column(Column(col_name.clone())),
                            col_alias: Some(ColumnAlias(prop_name.clone())),
                        })
                    }).collect::<Result<Vec<SelectItem>, RenderBuildError>>()?
                } else {
                    // No projections or property mappings - this might be a relationship scan
                    // Return empty for now (relationship CTEs are handled differently)
                    vec![]
                }
            },
            LogicalPlan::GraphNode(graph_node) => {
                // FIX: GraphNode must generate PropertyAccessExp with its own alias, 
                // not delegate to ViewScan which doesn't know the alias.
                // This fixes the bug where "a.name" becomes "u.name" in OPTIONAL MATCH queries.
                
                match graph_node.input.as_ref() {
                    LogicalPlan::ViewScan(view_scan) => {
                        if !view_scan.projections.is_empty() {
                            // Use explicit projections if available
                            view_scan.projections.iter().map(|proj| {
                                let expr: RenderExpr = proj.clone().try_into()?;
                                Ok(SelectItem {
                                    expression: expr,
                                    col_alias: None,
                                })
                            }).collect::<Result<Vec<SelectItem>, RenderBuildError>>()?
                        } else if !view_scan.property_mapping.is_empty() {
                            // Build PropertyAccessExp using GraphNode's alias (e.g., "a")
                            // instead of bare Column which defaults to heuristic "u"
                            view_scan.property_mapping.iter().map(|(prop_name, col_name)| {
                                Ok(SelectItem {
                                    expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(graph_node.alias.clone()),
                                        column: Column(col_name.clone()),
                                    }),
                                    col_alias: Some(ColumnAlias(prop_name.clone())),
                                })
                            }).collect::<Result<Vec<SelectItem>, RenderBuildError>>()?
                        } else {
                            vec![]
                        }
                    },
                    _ => graph_node.input.extract_select_items()?
                }
            },
            LogicalPlan::GraphRel(graph_rel) => {
                // FIX: GraphRel must generate SELECT items for both left and right nodes
                // This fixes OPTIONAL MATCH queries where the right node (b) was being ignored
                let mut items = vec![];
                
                // Get SELECT items from left node
                items.extend(graph_rel.left.extract_select_items()?);
                
                // Get SELECT items from right node (for OPTIONAL MATCH, this is the optional part)
                items.extend(graph_rel.right.extract_select_items()?);
                
                items
            },
            LogicalPlan::Filter(filter) => filter.input.extract_select_items()?,
            LogicalPlan::Projection(projection) => {
                // Check if input is a Projection(With) - if so, collect its aliases for resolution
                // The kind might have been changed from With to Return by analyzer passes, so also check
                // if the inner Projection has aliases (which indicate it was originally a WITH clause)
                let with_aliases: std::collections::HashMap<String, crate::query_planner::logical_expr::LogicalExpr> = 
                    match projection.input.as_ref() {
                        LogicalPlan::Projection(inner_proj) => {
                            // Check if this was a WITH projection (either still marked as With, or has aliases)
                            let has_aliases = inner_proj.items.iter().any(|item| item.col_alias.is_some());
                            if matches!(inner_proj.kind, crate::query_planner::logical_plan::ProjectionKind::With) || has_aliases {
                                println!("DEBUG: Found projection with aliases (possibly WITH) with {} items", inner_proj.items.len());
                                // Collect aliases from projection
                                let aliases: std::collections::HashMap<_, _> = inner_proj.items.iter()
                                    .filter_map(|item| {
                                        item.col_alias.as_ref().map(|alias| {
                                            println!("DEBUG: Registering alias: {} -> {:?}", alias.0, item.expression);
                                            (alias.0.clone(), item.expression.clone())
                                        })
                                    })
                                    .collect();
                                println!("DEBUG: Collected {} aliases", aliases.len());
                                aliases
                            } else {
                                std::collections::HashMap::new()
                            }
                        }
                        LogicalPlan::GraphJoins(graph_joins) => {
                            // Look through GraphJoins to find the inner Projection(With)
                            if let LogicalPlan::Projection(inner_proj) = graph_joins.input.as_ref() {
                                if let LogicalPlan::Projection(with_proj) = inner_proj.input.as_ref() {
                                    let has_aliases = with_proj.items.iter().any(|item| item.col_alias.is_some());
                                    if matches!(with_proj.kind, crate::query_planner::logical_plan::ProjectionKind::With) || has_aliases {
                                        println!("DEBUG: Found projection with aliases (through GraphJoins) with {} items", with_proj.items.len());
                                        let aliases: std::collections::HashMap<_, _> = with_proj.items.iter()
                                            .filter_map(|item| {
                                                item.col_alias.as_ref().map(|alias| {
                                                    println!("DEBUG: Registering alias: {} -> {:?}", alias.0, item.expression);
                                                    (alias.0.clone(), item.expression.clone())
                                                })
                                            })
                                            .collect();
                                        println!("DEBUG: Collected {} aliases through GraphJoins", aliases.len());
                                        aliases
                                    } else {
                                        std::collections::HashMap::new()
                                    }
                                } else {
                                    std::collections::HashMap::new()
                                }
                            } else {
                                std::collections::HashMap::new()
                            }
                        }
                        _ => std::collections::HashMap::new()
                    };
                
                let path_var = get_path_variable(&projection.input);
                let items = projection.items.iter().map(|item| {
                    // Resolve TableAlias references to WITH aliases BEFORE conversion
                    let resolved_expr = if let crate::query_planner::logical_expr::LogicalExpr::TableAlias(ref table_alias) = item.expression {
                        println!("DEBUG: Checking TableAlias: {}", table_alias.0);
                        if let Some(with_expr) = with_aliases.get(&table_alias.0) {
                            // Replace with the actual expression from WITH
                            println!("DEBUG: Resolved {} to {:?}", table_alias.0, with_expr);
                            with_expr.clone()
                        } else {
                            println!("DEBUG: No WITH alias found for {}", table_alias.0);
                            item.expression.clone()
                        }
                    } else {
                        item.expression.clone()
                    };
                    
                    let mut expr: RenderExpr = resolved_expr.try_into()?;
                    
                    // Check if this is a path variable that needs to be converted to tuple construction
                    if let (Some(path_var_name), RenderExpr::TableAlias(TableAlias(alias))) = (&path_var, &expr) {
                        if alias == path_var_name {
                            // Convert path variable to named tuple construction
                            // Use tuple(nodes, length, relationships) instead of map() to avoid type conflicts
                            expr = RenderExpr::ScalarFnCall(ScalarFnCall {
                                name: "tuple".to_string(),
                                args: vec![
                                    RenderExpr::Column(Column("path_nodes".to_string())),
                                    RenderExpr::Column(Column("hop_count".to_string())),
                                    RenderExpr::Column(Column("path_relationships".to_string())),
                                ],
                            });
                        }
                    }
                    
                    // Rewrite path function calls: length(p), nodes(p), relationships(p)
                    // Use table alias "t" to reference CTE columns
                    if let Some(path_var_name) = &path_var {
                        expr = rewrite_path_functions_with_table(&expr, path_var_name, "t");
                    }
                    
                    // IMPORTANT: Property mapping is already done in the analyzer phase by FilterTagging.apply_property_mapping
                    // for schema-based queries (which use ViewScan). Re-mapping here causes errors because the analyzer
                    // has already converted Cypher property names (e.g., "name") to database column names (e.g., "full_name").
                    // Trying to map "full_name" again fails because it's not in the property_mappings.
                    //
                    // DO NOT apply property mapping here for Projection nodes - it's already been done correctly.
                    
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
                
                // Check if this is a CTE placeholder for multiple relationships
                // CTE names start with "rel_" and should not be included in FROM clause
                if table_name_raw.starts_with("rel_") {
                    log::info!("✓ Skipping CTE placeholder '{}' in FROM clause - will be referenced in JOINs", table_name_raw);
                    return Ok(None);
                }
                
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
            LogicalPlan::ViewScan(scan) => {
                // Check if this is a relationship ViewScan (has from_id/to_id)
                if scan.from_id.is_some() && scan.to_id.is_some() {
                    // For relationship ViewScans, use the CTE name instead of table name
                    let cte_name = format!("rel_{}", scan.source_table.replace([' ', '-', '_'], ""));
                    Some(ViewTableRef::new_table(
                        scan.as_ref().clone(),
                        cte_name,
                    ))
                } else {
                    // For node ViewScans, use the table name
                    Some(ViewTableRef::new_table(
                        scan.as_ref().clone(),
                        scan.source_table.clone(),
                    ))
                }
            },
            LogicalPlan::GraphNode(graph_node) => {
                // For GraphNode, extract FROM from the input but use this GraphNode's alias
                // CROSS JOINs for multiple standalone nodes are handled in extract_joins
                println!("DEBUG: GraphNode.extract_from() - alias: {}, input: {:?}", graph_node.alias, graph_node.input);
                match &*graph_node.input {
                    LogicalPlan::ViewScan(scan) => {
                        println!("DEBUG: GraphNode.extract_from() - matched ViewScan, table: {}", scan.source_table);
                        // Check if this is a relationship ViewScan (has from_id/to_id)
                        let table_or_cte_name = if scan.from_id.is_some() && scan.to_id.is_some() {
                            // For relationship ViewScans, use the CTE name instead of table name
                            format!("rel_{}", scan.source_table.replace([' ', '-', '_'], ""))
                        } else {
                            // For node ViewScans, use the table name
                            scan.source_table.clone()
                        };
                        // ViewScan already returns ViewTableRef, just update the alias
                        let mut view_ref = ViewTableRef::new_table(
                            scan.as_ref().clone(),
                            table_or_cte_name,
                        );
                        view_ref.alias = Some(graph_node.alias.clone());
                        println!("DEBUG: GraphNode.extract_from() - created ViewTableRef: {:?}", view_ref);
                        Some(view_ref)
                    },
                    _ => {
                        println!("DEBUG: GraphNode.extract_from() - not a ViewScan, input type: {:?}", graph_node.input);
                        // For other input types, extract FROM and convert
                        let mut from_ref = from_table_to_view_ref(graph_node.input.extract_from()?);
                        // Use this GraphNode's alias
                        if let Some(ref mut view_ref) = from_ref {
                            view_ref.alias = Some(graph_node.alias.clone());
                        }
                        from_ref
                    }
                }
            },
            LogicalPlan::GraphRel(graph_rel) => {
                // Check if both nodes are anonymous (edge-driven query)
                let left_table_name = extract_table_name(&graph_rel.left);
                let right_table_name = extract_table_name(&graph_rel.right);
                
                // If both nodes are anonymous, use the relationship table as FROM
                if left_table_name.is_none() && right_table_name.is_none() {
                    // Edge-driven query: use relationship table directly (not as CTE)
                    // Extract table name from the relationship ViewScan
                    if let LogicalPlan::ViewScan(scan) = graph_rel.center.as_ref() {
                        // Use actual table name, not CTE name
                        return Ok(Some(FromTable::new(Some(ViewTableRef::new_table(
                            scan.as_ref().clone(),
                            scan.source_table.clone(),
                        )))));
                    }
                    // Fallback to normal extraction if not a ViewScan
                    return Ok(None);
                }
                
                // For GraphRel with labeled nodes, we need to include the start node in the FROM clause
                // This handles simple relationship queries where the start node should be FROM
                
                // For OPTIONAL MATCH, prefer the required (non-optional) node as FROM
                // Check if this is an optional relationship (left node optional, right node required)
                let prefer_right_as_from = graph_rel.is_optional == Some(true);
                
                println!("DEBUG: graph_rel.is_optional = {:?}, prefer_right_as_from = {}", 
                         graph_rel.is_optional, prefer_right_as_from);

                let (primary_from, fallback_from) = if prefer_right_as_from {
                    // For optional relationships, use right (required) node as FROM
                    (graph_rel.right.extract_from(), graph_rel.left.extract_from())
                } else {
                    // For required relationships, use left (start) node as FROM
                    (graph_rel.left.extract_from(), graph_rel.right.extract_from())
                };
                
                println!("DEBUG: primary_from = {:?}", primary_from);
                println!("DEBUG: fallback_from = {:?}", fallback_from);

                if let Ok(Some(from_table)) = primary_from {
                    from_table_to_view_ref(Some(from_table))
                } else {
                    // If primary node doesn't have FROM, try fallback
                    let right_from = fallback_from;
                    println!("DEBUG: Using fallback FROM");
                    println!("DEBUG: right_from = {:?}", right_from);

                    if let Ok(Some(from_table)) = right_from {
                        from_table_to_view_ref(Some(from_table))
                    } else {
                        // If right also doesn't have FROM, check if right contains a nested GraphRel
                        if let LogicalPlan::GraphRel(nested_graph_rel) = graph_rel.right.as_ref() {
                            // Extract FROM from the nested GraphRel's left node
                            let nested_left_from = nested_graph_rel.left.extract_from();
                            println!("DEBUG: nested_graph_rel.left = {:?}", nested_graph_rel.left);
                            println!("DEBUG: nested_left_from = {:?}", nested_left_from);

                            if let Ok(Some(nested_from_table)) = nested_left_from {
                                from_table_to_view_ref(Some(nested_from_table))
                            } else {
                                // If nested left also doesn't have FROM, create one from the nested left_connection alias
                                let table_name = extract_table_name(&nested_graph_rel.left)
                                    .ok_or_else(|| super::errors::RenderBuildError::TableNameNotFound(format!(
                                        "Could not resolve table name for alias '{}', plan: {:?}",
                                        nested_graph_rel.left_connection, nested_graph_rel.left
                                    )))?;

                                Some(super::ViewTableRef {
                                    source: std::sync::Arc::new(LogicalPlan::Empty),
                                    name: table_name,
                                    alias: Some(nested_graph_rel.left_connection.clone()),
                                })
                            }
                        } else {
                            // If right doesn't have FROM, we need to determine which node should be the anchor
                            // Use find_anchor_node logic to choose the correct anchor
                            let all_connections = get_all_relationship_connections(&self);
                            let optional_aliases = std::collections::HashSet::new();
                            
                            if let Some(anchor_alias) = find_anchor_node(&all_connections, &optional_aliases) {
                                // Determine which node (left or right) the anchor corresponds to
                                let (table_plan, connection_alias) = if anchor_alias == graph_rel.left_connection {
                                    (&graph_rel.left, &graph_rel.left_connection)
                                } else {
                                    (&graph_rel.right, &graph_rel.right_connection)
                                };
                                
                                let table_name = extract_table_name(table_plan)
                                    .ok_or_else(|| super::errors::RenderBuildError::TableNameNotFound(format!(
                                        "Could not resolve table name for anchor alias '{}', plan: {:?}",
                                        connection_alias, table_plan
                                    )))?;

                                Some(super::ViewTableRef {
                                    source: std::sync::Arc::new(LogicalPlan::Empty),
                                    name: table_name,
                                    alias: Some(connection_alias.clone()),
                                })
                            } else {
                                // Fallback: use left_connection as anchor (traditional behavior)
                                let table_name = extract_table_name(&graph_rel.left)
                                    .ok_or_else(|| super::errors::RenderBuildError::TableNameNotFound(format!(
                                        "Could not resolve table name for alias '{}', plan: {:?}",
                                        graph_rel.left_connection, graph_rel.left
                                    )))?;

                                Some(super::ViewTableRef {
                                    source: std::sync::Arc::new(LogicalPlan::Empty),
                                    name: table_name,
                                    alias: Some(graph_rel.left_connection.clone()),
                                })
                            }
                        }
                    }
                }
            },
            LogicalPlan::Filter(filter) => from_table_to_view_ref(filter.input.extract_from()?),
            LogicalPlan::Projection(projection) => from_table_to_view_ref(projection.input.extract_from()?),
            LogicalPlan::GraphJoins(graph_joins) => {
                // Helper function to unwrap Projection/Filter layers to find GraphRel
                fn find_graph_rel(plan: &LogicalPlan) -> Option<&GraphRel> {
                    match plan {
                        LogicalPlan::GraphRel(gr) => Some(gr),
                        LogicalPlan::Projection(proj) => find_graph_rel(&proj.input),
                        LogicalPlan::Filter(filter) => find_graph_rel(&filter.input),
                        _ => None,
                    }
                }
                
                // Try to find GraphRel through any Projection/Filter wrappers
                if let Some(graph_rel) = find_graph_rel(&graph_joins.input) {
                    if let Some(labels) = &graph_rel.labels {
                        if labels.len() > 1 {
                            // Multiple relationship types: need both start and end nodes in FROM
                            // Get end node from GraphRel
                            let end_from = graph_rel.right.extract_from()?;
                            
                            // Return the end node - start node will be added as CROSS JOIN
                            from_table_to_view_ref(end_from)
                        } else {
                            // Single relationship type: Use anchor node logic for multi-hop
                            // FIX: For multi-hop queries, find the anchor node
                            // Collect all relationship connections to find which node is the anchor
                            let all_connections = get_all_relationship_connections(&graph_joins.input);
                            
                            if let Some(anchor_alias) = find_anchor_node(&all_connections, &graph_joins.optional_aliases) {
                                println!("DEBUG: GraphJoins.extract_from() - found anchor node: {}", anchor_alias);
                                // Get the table name for the anchor node by recursively finding the GraphNode with matching alias
                                if let Some(table_name) = find_table_name_for_alias(&graph_joins.input, &anchor_alias) {
                                    println!("DEBUG: GraphJoins.extract_from() - found table_name for anchor '{}': {}", anchor_alias, table_name);
                                    Some(super::ViewTableRef {
                                        source: std::sync::Arc::new(LogicalPlan::Empty),
                                        name: table_name,
                                        alias: Some(anchor_alias),
                                    })
                                } else {
                                    println!("DEBUG: GraphJoins.extract_from() - could not find table_name for anchor '{}', falling back to first join", anchor_alias);
                                    // Fallback to first join
                                    if let Some(first_join) = graph_joins.joins.first() {
                                        Some(super::ViewTableRef {
                                            source: std::sync::Arc::new(LogicalPlan::Empty),
                                            name: first_join.table_name.clone(),
                                            alias: Some(first_join.table_alias.clone()),
                                        })
                                    } else {
                                        None
                                    }
                                }
                            } else {
                                // No connections found, use first join
                                if let Some(first_join) = graph_joins.joins.first() {
                                    Some(super::ViewTableRef {
                                        source: std::sync::Arc::new(LogicalPlan::Empty),
                                        name: first_join.table_name.clone(),
                                        alias: Some(first_join.table_alias.clone()),
                                    })
                                } else {
                                    None
                                }
                            }
                        }
                    } else {
                        // No labels: Use anchor node logic for multi-hop
                        // FIX: Same logic - find anchor node for multi-hop
                        let all_connections = get_all_relationship_connections(&graph_joins.input);
                        
                        if let Some(anchor_alias) = find_anchor_node(&all_connections, &graph_joins.optional_aliases) {
                            // Get the table name for the anchor node
                            if let Some(table_name) = find_table_name_for_alias(&graph_joins.input, &anchor_alias) {
                                Some(super::ViewTableRef {
                                    source: std::sync::Arc::new(LogicalPlan::Empty),
                                    name: table_name,
                                    alias: Some(anchor_alias),
                                })
                            } else {
                                if let Some(first_join) = graph_joins.joins.first() {
                                    Some(super::ViewTableRef {
                                        source: std::sync::Arc::new(LogicalPlan::Empty),
                                        name: first_join.table_name.clone(),
                                        alias: Some(first_join.table_alias.clone()),
                                    })
                                } else {
                                    None
                                }
                            }
                        } else {
                            // Not a GraphRel input: fallback to first join
                            if let Some(first_join) = graph_joins.joins.first() {
                                Some(super::ViewTableRef {
                                    source: std::sync::Arc::new(LogicalPlan::Empty),
                                    name: first_join.table_name.clone(),
                                    alias: Some(first_join.table_alias.clone()),
                                })
                            } else {
                                None
                            }
                        }
                    }
                } else {
                    // Not a GraphRel input: normal processing
                    // First try to extract FROM from the input
                    let input_from = graph_joins.input.extract_from()?;
                    if input_from.is_some() {
                        from_table_to_view_ref(input_from)
                    } else {
                        // If input has no FROM clause but we have joins, use the first join as FROM
                        // This handles the case of simple relationships where GraphRel returns None
                        if let Some(first_join) = graph_joins.joins.first() {
                            Some(super::ViewTableRef {
                                source: std::sync::Arc::new(LogicalPlan::Empty),
                                name: first_join.table_name.clone(),
                                alias: Some(first_join.table_alias.clone()),
                            })
                        } else {
                            None
                        }
                    }
                }
            },
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
            LogicalPlan::ViewScan(scan) => {
                // Extract view_filter if present (filters injected by optimizer)
                if let Some(ref filter) = scan.view_filter {
                    let mut expr: RenderExpr = filter.clone().try_into()?;
                    // Apply property mapping to the filter expression
                    apply_property_mapping_to_expr(&mut expr, &LogicalPlan::ViewScan(scan.clone()));
                    Some(expr)
                } else {
                    None
                }
            },
            LogicalPlan::GraphNode(graph_node) => graph_node.input.extract_filters()?,
            LogicalPlan::GraphRel(graph_rel) => {
                log::trace!("GraphRel node detected, extracting filters from where_predicate only");
                
                // For GraphRel, check where_predicate first
                // If present, use it (these are predicates that should go in final WHERE clause)
                if let Some(ref predicate) = graph_rel.where_predicate {
                    let filter_expr: RenderExpr = predicate.clone().try_into()?;
                    log::trace!("GraphRel where_predicate filter: {:?}", filter_expr);
                    Some(filter_expr)
                } else {
                    // If no where_predicate, recursively check children for Filter nodes
                    // This handles cases where Filter is nested under GraphRel
                    // (e.g., multiple OPTIONAL MATCH wrapping a WHERE clause)
                    log::trace!("GraphRel has no where_predicate, checking children");
                    
                    // Try left, center, right in order
                    graph_rel.left.extract_filters()?
                        .or_else(|| graph_rel.center.extract_filters().ok().flatten())
                        .or_else(|| graph_rel.right.extract_filters().ok().flatten())
                }
            }
            LogicalPlan::Filter(filter) => {
                println!("DEBUG: extract_filters - Found Filter node with predicate: {:?}", filter.predicate);
                println!("DEBUG: extract_filters - Filter input type: {:?}", std::mem::discriminant(&*filter.input));
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
            LogicalPlan::GraphJoins(graph_joins) => {
                // Use the pre-computed joins from GraphJoinInference analyzer
                // These were carefully constructed to handle OPTIONAL MATCH, multi-hop, etc.
                println!("DEBUG: GraphJoins extract_joins - using pre-computed joins from analyzer");
                println!("DEBUG: graph_joins.joins.len() = {}", graph_joins.joins.len());
                
                // Convert from logical_plan::Join to render_plan::Join
                graph_joins.joins.iter()
                    .map(|j| j.clone().try_into())
                    .collect::<Result<Vec<Join>, RenderBuildError>>()?
            },
            LogicalPlan::GraphRel(graph_rel) => {
                // FIX: GraphRel must generate JOINs for the relationship traversal
                // This fixes OPTIONAL MATCH queries by creating proper JOIN clauses
                
                // MULTI-HOP FIX: If left side is another GraphRel, recursively extract its joins first
                // This handles patterns like (a)-[:FOLLOWS]->(b)-[:FOLLOWS]->(c)
                let mut joins = vec![];
                if let LogicalPlan::GraphRel(_) = graph_rel.left.as_ref() {
                    println!("DEBUG: Multi-hop pattern detected - recursively extracting left GraphRel joins");
                    let mut left_joins = graph_rel.left.extract_joins()?;
                    joins.append(&mut left_joins);
                }
                
                // First, check if the plan_ctx marks this relationship as optional
                // This is set by OPTIONAL MATCH clause processing
                let is_optional = graph_rel.is_optional.unwrap_or(false);
                let join_type = if is_optional { JoinType::Left } else { JoinType::Inner };
                
                // Extract table names and columns
                let start_label = extract_node_label_from_viewscan(&graph_rel.left)
                    .unwrap_or_else(|| "User".to_string());
                let end_label = extract_node_label_from_viewscan(&graph_rel.right)
                    .unwrap_or_else(|| "User".to_string());
                let start_table = label_to_table_name(&start_label);
                let end_table = label_to_table_name(&end_label);
                
                // Get relationship table
                let rel_table = if let Some(labels) = &graph_rel.labels {
                    if !labels.is_empty() {
                        rel_type_to_table_name(&labels[0])
                    } else {
                        extract_table_name(&graph_rel.center).unwrap_or_else(|| graph_rel.alias.clone())
                    }
                } else {
                    extract_table_name(&graph_rel.center).unwrap_or_else(|| graph_rel.alias.clone())
                };
                
                // MULTI-HOP FIX: For ID columns, use table lookup based on connection aliases
                // instead of extract_id_column which fails for nested GraphRel
                // The left_connection tells us which node alias we're connecting from
                let start_id_col = if let LogicalPlan::GraphRel(_) = graph_rel.left.as_ref() {
                    // Multi-hop: left side is another GraphRel, so left_connection points to intermediate node
                    // Look up the node's table and get its ID column
                    println!("DEBUG: Multi-hop - left_connection={}, using table lookup for ID column", graph_rel.left_connection);
                    table_to_id_column(&start_table)
                } else {
                    // Single hop: extract ID column from the node ViewScan
                    extract_id_column(&graph_rel.left)
                        .unwrap_or_else(|| table_to_id_column(&start_table))
                };
                let end_id_col = extract_id_column(&graph_rel.right)
                    .unwrap_or_else(|| table_to_id_column(&end_table));
                
                // Get relationship columns
                let rel_cols = extract_relationship_columns(&graph_rel.center)
                    .unwrap_or(RelationshipColumns {
                        from_id: "from_node_id".to_string(),
                        to_id: "to_node_id".to_string(),
                    });
                
                // OPTIONAL MATCH FIX: For incoming optional relationships like (b:User)-[:FOLLOWS]->(a)
                // where 'a' is required and 'b' is optional, we need to reverse the JOIN order:
                // 1. JOIN b first (optional node)
                // 2. Then JOIN relationship (can reference both a and b)
                //
                // Detect this case: is_optional=true AND FROM clause is right node
                // (The FROM clause selection logic prefers right node when is_optional=true)
                let reverse_join_order = is_optional;
                
                if reverse_join_order {
                    println!("DEBUG: Reversing JOIN order for optional relationship (b)-[:FOLLOWS]->(a) where a is FROM");
                    
                    // JOIN 1: End node (optional left node 'b')
                    //   e.g., LEFT JOIN users AS b ON b.user_id = r.to_node_id
                    joins.push(Join {
                        table_name: start_table.clone(),
                        table_alias: graph_rel.left_connection.clone(),
                        joining_on: vec![OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(graph_rel.left_connection.clone()),
                                    column: Column(start_id_col.clone()),
                                }),
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(graph_rel.alias.clone()),
                                    column: Column(rel_cols.from_id.clone()),
                                }),
                            ],
                        }],
                        join_type: join_type.clone(),
                    });
                    
                    // JOIN 2: Relationship table (can now reference both nodes)
                    //   e.g., LEFT JOIN follows AS r ON r.follower_id = b.user_id AND r.followed_id = a.user_id
                    joins.push(Join {
                        table_name: rel_table.clone(),
                        table_alias: graph_rel.alias.clone(),
                        joining_on: vec![
                            OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(graph_rel.alias.clone()),
                                        column: Column(rel_cols.from_id.clone()),
                                    }),
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(graph_rel.left_connection.clone()),
                                        column: Column(start_id_col),
                                    }),
                                ],
                            },
                            OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(graph_rel.alias.clone()),
                                        column: Column(rel_cols.to_id.clone()),
                                    }),
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(graph_rel.right_connection.clone()),
                                        column: Column(end_id_col.clone()),
                                    }),
                                ],
                            },
                        ],
                        join_type: join_type.clone(),
                    });
                } else {
                    // Normal order: relationship first, then end node
                    // JOIN 1: Start node -> Relationship table
                    //   e.g., INNER JOIN follows AS r ON r.from_node_id = a.user_id
                    joins.push(Join {
                        table_name: rel_table.clone(),
                        table_alias: graph_rel.alias.clone(),
                        joining_on: vec![OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(graph_rel.alias.clone()),
                                    column: Column(rel_cols.from_id.clone()),
                                }),
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(graph_rel.left_connection.clone()),
                                    column: Column(start_id_col),
                                }),
                            ],
                        }],
                        join_type: join_type.clone(),
                    });
                    
                    // JOIN 2: Relationship table -> End node
                    //   e.g., LEFT JOIN users AS b ON b.user_id = r.to_node_id
                    joins.push(Join {
                        table_name: end_table,
                        table_alias: graph_rel.right_connection.clone(),
                        joining_on: vec![OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(graph_rel.right_connection.clone()),
                                    column: Column(end_id_col),
                                }),
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(graph_rel.alias.clone()),
                                    column: Column(rel_cols.to_id),
                                }),
                            ],
                        }],
                        join_type,
                    });
                }
                
                joins
            }
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

    fn extract_having(&self) -> RenderPlanBuilderResult<Option<RenderExpr>> {
        let having_clause = match &self {
            LogicalPlan::Limit(limit) => limit.input.extract_having()?,
            LogicalPlan::Skip(skip) => skip.input.extract_having()?,
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_having()?,
            LogicalPlan::Projection(projection) => projection.input.extract_having()?,
            LogicalPlan::GroupBy(group_by) => {
                if let Some(having) = &group_by.having_clause {
                    let mut render_expr: RenderExpr = having.clone().try_into()?;
                    // Apply property mapping to the HAVING expression
                    apply_property_mapping_to_expr(&mut render_expr, &group_by.input);
                    Some(render_expr)
                } else {
                    None
                }
            }
            _ => None,
        };
        Ok(having_clause)
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
        use crate::graph_catalog::graph_schema::GraphSchema;
        use std::collections::HashMap;
        let empty_schema = GraphSchema::build(1, "default".to_string(), HashMap::new(), HashMap::new());
        
        let union_opt = match &self {
            LogicalPlan::Union(union) => Some(Union {
                input: union
                    .inputs
                    .iter()
                    .map(|input| input.to_render_plan(&empty_schema))
                    .collect::<Result<Vec<RenderPlan>, RenderBuildError>>()?,
                union_type: union.union_type.clone().try_into()?,
            }),
            _ => None,
        };
        Ok(union_opt)
    }

    /// Try to build a JOIN-based render plan for simple queries
    /// Returns Ok(plan) if successful, Err(_) if this query needs CTE-based processing
    fn try_build_join_based_plan(&self) -> RenderPlanBuilderResult<RenderPlan> {
        
        println!("DEBUG: try_build_join_based_plan called");
        
        // First, extract ORDER BY/LIMIT/SKIP if present
        let (core_plan, order_by_items, limit_val, skip_val) = match self {
            LogicalPlan::Limit(limit_node) => {
                match limit_node.input.as_ref() {
                    LogicalPlan::OrderBy(order_node) => {
                        (order_node.input.as_ref(), Some(&order_node.items), Some(limit_node.count), None)
                    }
                    other => (other, None, Some(limit_node.count), None)
                }
            }
            LogicalPlan::OrderBy(order_node) => {
                (order_node.input.as_ref(), Some(&order_node.items), None, None)
            }
            LogicalPlan::Skip(skip_node) => {
                (skip_node.input.as_ref(), None, None, Some(skip_node.count))
            }
            other => (other, None, None, None)
        };
        
        // Check for GraphJoins wrapping Projection(Return) -> GroupBy pattern
        if let LogicalPlan::GraphJoins(graph_joins) = core_plan {
            // Check if there's a multiple-relationship GraphRel anywhere in the tree
            if has_multiple_relationship_types(&graph_joins.input) {
                println!("DEBUG: Multiple relationship types detected in GraphJoins tree, returning Err to use CTE path");
                return Err(RenderBuildError::InvalidRenderPlan(
                    "Multiple relationship types require CTE-based processing with UNION".to_string()
                ));
            }
            
            if let LogicalPlan::Projection(proj) = graph_joins.input.as_ref() {
                if matches!(proj.kind, crate::query_planner::logical_plan::ProjectionKind::Return) {
                    if let LogicalPlan::GroupBy(group_by) = proj.input.as_ref() {
                        if group_by.having_clause.is_some() || !group_by.expressions.is_empty() {
                            println!("DEBUG: GraphJoins wrapping Projection(Return)->GroupBy detected, delegating to child");
                            // Delegate to the inner Projection -> GroupBy for CTE-based processing
                            let mut plan = graph_joins.input.try_build_join_based_plan()?;
                            
                            // Add ORDER BY/LIMIT/SKIP if they were present in the original query
                            if let Some(items) = order_by_items {
                                // Rewrite ORDER BY expressions for CTE context
                                let mut order_by_items_vec = vec![];
                                for item in items {
                                    let rewritten_expr = match &item.expression {
                                        crate::query_planner::logical_expr::LogicalExpr::ColumnAlias(col_alias) => {
                                            // ORDER BY column_alias -> ORDER BY grouped_data.column_alias
                                            RenderExpr::PropertyAccessExp(
                                                PropertyAccess {
                                                    table_alias: TableAlias("grouped_data".to_string()),
                                                    column: Column(col_alias.0.clone()),
                                                }
                                            )
                                        }
                                        other_expr => {
                                            // Try to convert the expression
                                            other_expr.clone().try_into()?
                                        }
                                    };
                                    order_by_items_vec.push(
                                        OrderByItem {
                                            expression: rewritten_expr,
                                            order: item.order.clone().try_into()?,
                                        }
                                    );
                                }
                                plan.order_by = OrderByItems(order_by_items_vec);
                            }
                            
                            if let Some(limit) = limit_val {
                                plan.limit = LimitItem(Some(limit));
                            }
                            
                            if let Some(skip) = skip_val {
                                plan.skip = SkipItem(Some(skip));
                            }
                            
                            return Ok(plan);
                        }
                    }
                }
            }
        }
        
        // Check if this query needs CTE-based processing
        if let LogicalPlan::Projection(proj) = self {
            if let LogicalPlan::GraphRel(graph_rel) = proj.input.as_ref() {
                // Variable-length paths need recursive CTEs
                if graph_rel.variable_length.is_some() {
                    println!("DEBUG: Variable-length path detected, returning Err to use CTE path");
                    return Err(RenderBuildError::InvalidRenderPlan(
                        "Variable-length paths require CTE-based processing".to_string()
                    ));
                }
                
                // Multiple relationship types need UNION CTEs
                if let Some(labels) = &graph_rel.labels {
                    if labels.len() > 1 {
                        println!("DEBUG: Multiple relationship types detected ({}), returning Err to use CTE path", labels.len());
                        return Err(RenderBuildError::InvalidRenderPlan(
                            "Multiple relationship types require CTE-based processing with UNION".to_string()
                        ));
                    }
                }
            }
        }
        
        // Try to build with JOINs - this will work for:
        // - Simple MATCH queries with relationships
        // - OPTIONAL MATCH queries (via GraphRel.extract_joins)
        // - Multiple MATCH clauses (via GraphRel.extract_joins)
        // It will fail (return Err) for:
        // - Variable-length paths (need recursive CTEs)
        // - Multiple relationship types (need UNION CTEs)
        // - Complex nested queries
        // - Queries that don't have extractable JOINs
        
        println!("DEBUG: Calling build_simple_relationship_render_plan");
        self.build_simple_relationship_render_plan()
    }
    
    /// Build render plan for simple relationship queries using direct JOINs
    fn build_simple_relationship_render_plan(&self) -> RenderPlanBuilderResult<RenderPlan> {
        println!("DEBUG: build_simple_relationship_render_plan START - plan type: {:?}", std::mem::discriminant(self));
        
        // Special case: Detect Projection(kind=Return) over GroupBy
        // This can be wrapped in OrderBy/Limit/Skip nodes
        // CTE is needed when RETURN items require data not available from WITH output
        
        // Unwrap OrderBy, Limit, Skip to find the core Projection
        let (core_plan, order_by, limit_val, skip_val) = match self {
            LogicalPlan::Limit(limit_node) => {
                println!("DEBUG: Unwrapping Limit node, count={}", limit_node.count);
                let limit_val = limit_node.count;
                match limit_node.input.as_ref() {
                    LogicalPlan::OrderBy(order_node) => {
                        println!("DEBUG: Found OrderBy inside Limit");
                        (order_node.input.as_ref(), Some(&order_node.items), Some(limit_val), None)
                    }
                    LogicalPlan::Skip(skip_node) => {
                        println!("DEBUG: Found Skip inside Limit");
                        (skip_node.input.as_ref(), None, Some(limit_val), Some(skip_node.count))
                    }
                    other => {
                        println!("DEBUG: Limit contains other type: {:?}", std::mem::discriminant(other));
                        (other, None, Some(limit_val), None)
                    }
                }
            }
            LogicalPlan::OrderBy(order_node) => {
                println!("DEBUG: Unwrapping OrderBy node");
                (order_node.input.as_ref(), Some(&order_node.items), None, None)
            }
            LogicalPlan::Skip(skip_node) => {
                println!("DEBUG: Unwrapping Skip node");
                (skip_node.input.as_ref(), None, None, Some(skip_node.count))
            }
            other => {
                println!("DEBUG: No unwrapping needed, plan type: {:?}", std::mem::discriminant(other));
                (other, None, None, None)
            }
        };
        
        println!("DEBUG: After unwrapping - core_plan type: {:?}, has_order_by: {}, has_limit: {}, has_skip: {}", 
            std::mem::discriminant(core_plan), order_by.is_some(), limit_val.is_some(), skip_val.is_some());
        
        // Now check if core_plan is Projection(Return) over GroupBy
        if let LogicalPlan::Projection(outer_proj) = core_plan {
            println!("DEBUG: core_plan is Projection, kind: {:?}", outer_proj.kind);
            if matches!(outer_proj.kind, crate::query_planner::logical_plan::ProjectionKind::Return) {
                println!("DEBUG: Projection is Return type");
                if let LogicalPlan::GroupBy(group_by) = outer_proj.input.as_ref() {
                    println!("DEBUG: Found GroupBy under Projection(Return)!");
                    // Check if RETURN items need data beyond what WITH provides
                    // CTE is needed if RETURN contains:
                    // 1. Node references (TableAlias that refers to a node, not a WITH alias)
                    // 2. Wildcards (like `a.*`)
                    // 3. References to WITH projection aliases that aren't in the inner projection
                    
                    // Collect all WITH projection aliases from the inner Projection
                    let with_aliases: std::collections::HashSet<String> = if let LogicalPlan::Projection(inner_proj) = group_by.input.as_ref() {
                        inner_proj.items.iter()
                            .filter_map(|item| item.col_alias.as_ref().map(|a| a.0.clone()))
                            .collect()
                    } else {
                        std::collections::HashSet::new()
                    };
                    
                    // CTE is always needed when there are WITH aliases (aggregates)
                    // because the outer query needs to reference them from the CTE
                    let needs_cte = !with_aliases.is_empty() || outer_proj.items.iter().any(|item| {
                        match &item.expression {
                            crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(prop) 
                                if prop.column.0 == "*" => true,
                            _ => false
                        }
                    });
                    
                    if needs_cte {
                        println!("DEBUG: Detected Projection(Return) over GroupBy where RETURN needs data beyond WITH output - using CTE pattern");
                        
                        // Build the GROUP BY subquery as a CTE
                        // Step 1: Build inner query (GROUP BY + HAVING) as a RenderPlan
                        use crate::graph_catalog::graph_schema::GraphSchema;
                        use std::collections::HashMap;
                        let empty_schema = GraphSchema::build(1, "default".to_string(), HashMap::new(), HashMap::new());
                        let inner_render_plan = group_by.input.to_render_plan(&empty_schema)?;
                        
                        // Step 2: Extract GROUP BY expressions and HAVING clause
                        // Fix wildcard grouping: a.* -> a.user_id (use ID column from schema)
                        let group_by_exprs: Vec<RenderExpr> = group_by.expressions
                            .iter()
                            .cloned()
                            .map(|expr| {
                                // Check if this is a wildcard (PropertyAccess with column="*" or TableAlias)
                                let fixed_expr = match &expr {
                                    crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(prop) if prop.column.0 == "*" => {
                                        // Replace a.* with a.{id_column}
                                        // Extract ID column from the schema
                                        let id_column = self.find_id_column_for_alias(&prop.table_alias.0)?;
                                        crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(
                                            crate::query_planner::logical_expr::PropertyAccess {
                                                table_alias: prop.table_alias.clone(),
                                                column: crate::query_planner::logical_expr::Column(id_column),
                                            }
                                        )
                                    }
                                    crate::query_planner::logical_expr::LogicalExpr::TableAlias(alias) => {
                                        // Replace table alias with table_alias.id_column
                                        let id_column = self.find_id_column_for_alias(&alias.0)?;
                                        crate::query_planner::logical_expr::LogicalExpr::PropertyAccessExp(
                                            crate::query_planner::logical_expr::PropertyAccess {
                                                table_alias: alias.clone(),
                                                column: crate::query_planner::logical_expr::Column(id_column),
                                            }
                                        )
                                    }
                                    _ => expr.clone()
                                };
                                fixed_expr.try_into()
                            })
                            .collect::<Result<Vec<RenderExpr>, RenderBuildError>>()?;
                        
                        let having_expr: Option<RenderExpr> = if let Some(having) = &group_by.having_clause {
                            Some(having.clone().try_into()?)
                        } else {
                            None
                        };
                        
                        // Step 2.5: Build SELECT list for CTE (only grouping keys + aggregates, not wildcards)
                        // Extract from the inner Projection (child of GroupBy)
                        let cte_select_items = if let LogicalPlan::Projection(inner_proj) = group_by.input.as_ref() {
                            inner_proj.items
                                .iter()
                                .map(|item| {
                                    // For each projection item, check if it's an aggregate or grouping key
                                    let render_expr: RenderExpr = item.expression.clone().try_into()?;
                                    
                                    // Normalize aggregate arguments: COUNT(b) -> COUNT(b.user_id)
                                    let normalized_expr = self.normalize_aggregate_args(render_expr)?;
                                    
                                    // Replace wildcard expressions with the specific ID column
                                    let (fixed_expr, auto_alias) = match &normalized_expr {
                                        RenderExpr::PropertyAccessExp(prop) if prop.column.0 == "*" => {
                                            // Find the ID column for this alias
                                            let id_col = self.find_id_column_for_alias(&prop.table_alias.0)?;
                                            let expr = RenderExpr::PropertyAccessExp(super::render_expr::PropertyAccess {
                                                table_alias: prop.table_alias.clone(),
                                                column: super::render_expr::Column(id_col.clone()),
                                            });
                                            // Add alias so it can be referenced as grouped_data.user_id
                                            (expr, Some(super::render_expr::ColumnAlias(id_col)))
                                        }
                                        _ => (normalized_expr, None)
                                    };
                                    
                                    // Use existing alias if present, otherwise use auto-generated alias for grouping keys
                                    let col_alias = item.col_alias.as_ref()
                                        .map(|a| super::render_expr::ColumnAlias(a.0.clone()))
                                        .or(auto_alias);
                                    
                                    Ok(super::SelectItem {
                                        expression: fixed_expr,
                                        col_alias,
                                    })
                                })
                                .collect::<Result<Vec<super::SelectItem>, RenderBuildError>>()?
                        } else {
                            // Fallback to original select items
                            inner_render_plan.select.0.clone()
                        };
                        
                        // Step 3: Create CTE with GROUP BY + HAVING
                        let cte_name = "grouped_data".to_string();
                        let cte = Cte {
                            cte_name: cte_name.clone(),
                            content: super::CteContent::Structured(RenderPlan {
                                ctes: CteItems(vec![]),
                                select: SelectItems(cte_select_items),
                                from: inner_render_plan.from.clone(),
                                joins: inner_render_plan.joins.clone(),
                                filters: inner_render_plan.filters.clone(),
                                group_by: GroupByExpressions(group_by_exprs.clone()), // Clone to preserve for later use
                                having_clause: having_expr,
                                order_by: OrderByItems(vec![]),
                                skip: SkipItem(None),
                                limit: LimitItem(None),
                                union: UnionItems(None),
                            }),
                            is_recursive: false,
                        };
                        
                        // Step 4: Build outer query that joins to CTE
                        // Extract the grouping key to use for join (use the FIXED expression with ID column)
                        let grouping_key_render = if let Some(first_expr) = group_by_exprs.first() {
                            first_expr.clone()
                        } else {
                            return Err(RenderBuildError::InvalidRenderPlan(
                                "GroupBy has no grouping expressions after fixing wildcards".to_string()
                            ));
                        };
                        
                        // Extract table alias and column name from the fixed grouping key
                        let (table_alias, key_column) = match &grouping_key_render {
                            RenderExpr::PropertyAccessExp(prop_access) => {
                                (prop_access.table_alias.0.clone(), prop_access.column.0.clone())
                            }
                            _ => {
                                return Err(RenderBuildError::InvalidRenderPlan(
                                    "Grouping expression is not a property access after fixing".to_string()
                                ));
                            }
                        };
                        
                        // Build outer SELECT items from outer_proj
                        // Need to rewrite references to WITH aliases to pull from the CTE
                        let outer_select_items = outer_proj.items
                            .iter()
                            .map(|item| {
                                let expr: RenderExpr = item.expression.clone().try_into()?;
                                
                                // Rewrite TableAlias references that are WITH aliases to reference the CTE
                                let rewritten_expr = match &expr {
                                    RenderExpr::TableAlias(alias) => {
                                        // Check if this is a WITH alias (from the CTE)
                                        if with_aliases.contains(&alias.0) {
                                            // Reference it from the CTE: grouped_data.follows
                                            RenderExpr::PropertyAccessExp(super::render_expr::PropertyAccess {
                                                table_alias: super::render_expr::TableAlias(cte_name.clone()),
                                                column: super::render_expr::Column(alias.0.clone()),
                                            })
                                        } else {
                                            expr
                                        }
                                    }
                                    _ => expr
                                };
                                
                                Ok(super::SelectItem {
                                    expression: rewritten_expr,
                                    col_alias: item.col_alias.as_ref().map(|alias| super::render_expr::ColumnAlias(alias.0.clone())),
                                })
                            })
                            .collect::<Result<Vec<super::SelectItem>, RenderBuildError>>()?;
                        
                        // Extract FROM table for the outer query (from the original table)
                        // NOTE: ClickHouse CTE scoping - we need to be careful about table references
                        let outer_from = inner_render_plan.from.clone();
                        
                        // Create JOIN condition: a.user_id = grouped_data.user_id
                        let cte_key_expr = RenderExpr::PropertyAccessExp(super::render_expr::PropertyAccess {
                            table_alias: super::render_expr::TableAlias(cte_name.clone()),
                            column: super::render_expr::Column(key_column.clone()),
                        });
                        
                        let join_condition = super::render_expr::OperatorApplication {
                            operator: super::render_expr::Operator::Equal,
                            operands: vec![grouping_key_render, cte_key_expr],
                        };
                        
                        // Create a join to the CTE
                        let cte_join = super::Join {
                            table_name: cte_name.clone(),
                            table_alias: cte_name.clone(),
                            joining_on: vec![join_condition],
                            join_type: super::JoinType::Inner,
                        };
                        
                        println!("DEBUG: Created GroupBy CTE pattern with table_alias={}, key_column={}", table_alias, key_column);
                        
                        // Build ORDER BY items, rewriting WITH alias references to CTE references
                        let order_by_items = if let Some(order_items) = order_by {
                            order_items.iter()
                                .map(|item| {
                                    let expr: RenderExpr = item.expression.clone().try_into()?;
                                    
                                    // Rewrite TableAlias references to WITH aliases
                                    let rewritten_expr = match &expr {
                                        RenderExpr::TableAlias(alias) => {
                                            if with_aliases.contains(&alias.0) {
                                                RenderExpr::PropertyAccessExp(super::render_expr::PropertyAccess {
                                                    table_alias: super::render_expr::TableAlias(cte_name.clone()),
                                                    column: super::render_expr::Column(alias.0.clone()),
                                                })
                                            } else {
                                                expr
                                            }
                                        }
                                        _ => expr
                                    };
                                    
                                    Ok(super::OrderByItem {
                                        expression: rewritten_expr,
                                        order: match item.order {
                                            crate::query_planner::logical_plan::OrderByOrder::Asc => super::OrderByOrder::Asc,
                                            crate::query_planner::logical_plan::OrderByOrder::Desc => super::OrderByOrder::Desc,
                                        },
                                    })
                                })
                                .collect::<Result<Vec<_>, RenderBuildError>>()?
                        } else {
                            vec![]
                        };
                        
                        // Return the CTE-based plan with proper JOIN, ORDER BY, and LIMIT
                        return Ok(RenderPlan {
                            ctes: CteItems(vec![cte]),
                            select: SelectItems(outer_select_items),
                            from: outer_from,
                            joins: JoinItems(vec![cte_join]),
                            filters: FilterItems(None),
                            group_by: GroupByExpressions(vec![]),
                            having_clause: None,
                            order_by: OrderByItems(order_by_items),
                            skip: SkipItem(skip_val),
                            limit: LimitItem(limit_val),
                            union: UnionItems(None),
                        });
                    }
                } else {
                    println!("DEBUG: Projection(Return) input is NOT GroupBy, discriminant: {:?}", std::mem::discriminant(outer_proj.input.as_ref()));
                }
            } else {
                println!("DEBUG: Projection is not Return type");
            }
        } else {
            println!("DEBUG: core_plan is NOT Projection, discriminant: {:?}", std::mem::discriminant(core_plan));
        }
        
        let final_select_items = self.extract_select_items()?;
        println!("DEBUG: build_simple_relationship_render_plan - final_select_items: {:?}", final_select_items);
        
        // Validate that we have proper select items
        if final_select_items.is_empty() {
            return Err(RenderBuildError::InvalidRenderPlan(
                "No select items found for relationship query. This usually indicates missing schema information or incomplete query planning.".to_string()
            ));
        }
        
        // Validate that select items are not just literals (which would indicate failed expression conversion)
        for item in &final_select_items {
            if let RenderExpr::Literal(_) = &item.expression {
                return Err(RenderBuildError::InvalidRenderPlan(
                    "Select item is a literal value, indicating failed expression conversion. Check schema mappings and query structure.".to_string()
                ));
            }
        }
        
        let mut final_from = self.extract_from()?;
        println!("DEBUG: build_simple_relationship_render_plan - final_from: {:?}", final_from);
        
        // Validate that we have a FROM clause
        if final_from.is_none() {
            return Err(RenderBuildError::InvalidRenderPlan(
                "No FROM table found for relationship query. Schema inference may have failed.".to_string()
            ));
        }
        
        let final_filters = self.extract_filters()?;
        println!("DEBUG: build_simple_relationship_render_plan - final_filters: {:?}", final_filters);
        
        // Validate that filters don't contain obviously invalid expressions
        if let Some(ref filter_expr) = final_filters {
            if is_invalid_filter_expression(filter_expr) {
                return Err(RenderBuildError::InvalidRenderPlan(
                    "Filter expression appears invalid (e.g., '1 = 0'). This usually indicates schema mapping issues.".to_string()
                ));
            }
        }
        
        let extracted_joins = self.extract_joins()?;
        println!("DEBUG: build_simple_relationship_render_plan - extracted_joins: {:?}", extracted_joins);
        
        // Filter out JOINs that duplicate the FROM table
        // If we're starting FROM node 'a', we shouldn't also have it in the JOINs list
        let from_alias = final_from.as_ref()
            .and_then(|ft| ft.table.as_ref())
            .and_then(|vt| vt.alias.clone());
        let filtered_joins: Vec<Join> = if let Some(ref anchor_alias) = from_alias {
            extracted_joins.into_iter()
                .filter(|join| {
                    if &join.table_alias == anchor_alias {
                        println!("DEBUG: Filtering out JOIN for '{}' because it's already in FROM clause", anchor_alias);
                        false
                    } else {
                        true
                    }
                })
                .collect()
        } else {
            extracted_joins
        };
        println!("DEBUG: build_simple_relationship_render_plan - filtered_joins: {:?}", filtered_joins);
        
        // For simple relationships, we need to ensure proper JOIN ordering
        // The extract_joins should handle this correctly
        
        Ok(RenderPlan {
            ctes: CteItems(vec![]),
            select: SelectItems(final_select_items),
            from: FromTableItem(from_table_to_view_ref(final_from)),
            joins: JoinItems(filtered_joins),
            filters: FilterItems(final_filters),
            group_by: GroupByExpressions(self.extract_group_by()?),
            having_clause: self.extract_having()?,
            order_by: OrderByItems(self.extract_order_by()?),
            skip: SkipItem(self.extract_skip()),
            limit: LimitItem(self.extract_limit()),
            union: UnionItems(None),
        })
    }

    fn to_render_plan(&self, schema: &GraphSchema) -> RenderPlanBuilderResult<RenderPlan> {
        
        println!("DEBUG: to_render_plan called for plan type: {:?}", std::mem::discriminant(self));
        
        // Special case for PageRank - it generates complete SQL directly
        if let LogicalPlan::PageRank(_pagerank) = self {
            // For PageRank, we create a minimal RenderPlan that will be handled specially
            // The actual SQL generation happens in the server handler
            return Ok(RenderPlan {
                ctes: CteItems(vec![]),
                select: SelectItems(vec![]),
                from: FromTableItem(None),
                joins: JoinItems(vec![]),
                filters: FilterItems(None),
                group_by: GroupByExpressions(vec![]),
                having_clause: None,
                order_by: OrderByItems(vec![]),
                skip: SkipItem(None),
                limit: LimitItem(None),
                union: UnionItems(None),
            });
        }
        
        // NEW ARCHITECTURE: Prioritize JOINs over CTEs
        // Only use CTEs for variable-length paths and complex cases
        // Try to build a simple JOIN-based plan first
        println!("DEBUG: Trying try_build_join_based_plan");
        match self.try_build_join_based_plan() {
            Ok(plan) => {
                println!("DEBUG: try_build_join_based_plan succeeded");
                return Ok(plan);
            }
            Err(_) => {
                println!("DEBUG: try_build_join_based_plan failed, falling back to CTE logic");
            }
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
        let mut context = analyze_property_requirements(self, schema);
        
        let extracted_ctes: Vec<Cte>;
        let mut final_from: Option<FromTable>;
        let final_filters: Option<RenderExpr>;

        let last_node_cte_opt = self.extract_last_node_cte()?;

        if let Some(last_node_cte) = last_node_cte_opt {
            // Extract the last part after splitting by '_'
            // This handles both "prefix_alias" and "rel_left_right" formats
            let parts: Vec<&str> = last_node_cte.cte_name.split('_').collect();
            let last_node_alias = parts.last()
                .ok_or(RenderBuildError::MalformedCTEName)?;

            // Second pass: generate CTEs with full context
            extracted_ctes = self.extract_ctes_with_context(last_node_alias, &mut context)?;
            
            // Check if we have a variable-length CTE (it will be a recursive RawSql CTE)
            let has_variable_length_cte = extracted_ctes.iter().any(|cte| {
                let is_recursive = cte.is_recursive;
                let is_raw_sql = matches!(&cte.content, super::CteContent::RawSql(_));
                is_recursive && is_raw_sql
            });
            
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
                final_filters = context.get_end_filters_for_outer_query().cloned();
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
                    if let (Some(final_filters), Some(last_node_filters)) = (&final_filters_opt, &last_node_filters_opt) {
                        Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
                            operator: Operator::And,
                            operands: vec![final_filters.clone(), last_node_filters.clone()],
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
            extracted_ctes = self.extract_ctes_with_context("_", &mut context)?;
            
            // Check if we have a variable-length CTE (recursive or chained join)
            // Both types use RawSql content and need special FROM clause handling
            let has_variable_length_cte = extracted_ctes.iter().any(|cte| 
                matches!(&cte.content, super::CteContent::RawSql(_)) && 
                (cte.cte_name.starts_with("variable_path_") || cte.cte_name.starts_with("chained_path_"))
            );
            
            if has_variable_length_cte {
                // For variable-length paths, use the CTE itself as the FROM clause
                let var_len_cte = extracted_ctes.iter()
                    .find(|cte| cte.cte_name.starts_with("variable_path_") || cte.cte_name.starts_with("chained_path_"))
                    .expect("Variable-length CTE should exist");
                    
                // Create a ViewTableRef that references the CTE by name
                final_from = Some(super::FromTable::new(Some(super::ViewTableRef {
                    source: std::sync::Arc::new(crate::query_planner::logical_plan::LogicalPlan::Empty),
                    name: var_len_cte.cte_name.clone(),
                    alias: Some("t".to_string()), // CTE uses 't' as alias
                })));
                // For variable-length paths, apply end filters in the outer query
                if let Some((_start_alias, _end_alias)) = has_variable_length_rel(self) {
                    final_filters = context.get_end_filters_for_outer_query().cloned();
                } else {
                    final_filters = None;
                }
            } else {
                // Normal case: no CTEs, extract FROM, joins, and filters normally
                final_from = self.extract_from()?;
                final_filters = self.extract_filters()?;
            }
        }

        let final_select_items = self.extract_select_items()?;
        
        // NOTE: Removed rewrite for select_items in variable-length paths to keep a.*, b.*

        let mut extracted_joins = self.extract_joins()?;
        
        // For variable-length paths, add joins to get full user data
        if let Some((start_alias, end_alias)) = has_variable_length_rel(self) {
            // IMPORTANT: Remove any joins that were extracted from the GraphRel itself
            // The CTE already handles the path traversal, so we only want to join the 
            // endpoint nodes to the CTE result. Keeping the GraphRel joins causes 
            // "Multiple table expressions with same alias" errors.
            extracted_joins.clear();
            
            // Get the actual table names and ID columns from the schema
            let start_table = get_node_table_for_alias(&start_alias);
            let end_table = get_node_table_for_alias(&end_alias);
            let start_id_col = get_node_id_column_for_alias(&start_alias);
            let end_id_col = get_node_id_column_for_alias(&end_alias);
            
            extracted_joins.push(Join {
                table_name: start_table,
                table_alias: start_alias.clone(),
                joining_on: vec![OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias("t".to_string()),
                            column: Column("start_id".to_string()),
                        }),
                        RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(start_alias.clone()),
                            column: Column(start_id_col),
                        }),
                    ],
                }],
                join_type: JoinType::Join,
            });
            extracted_joins.push(Join {
                table_name: end_table,
                table_alias: end_alias.clone(),
                joining_on: vec![OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias("t".to_string()),
                            column: Column("end_id".to_string()),
                        }),
                        RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(end_alias.clone()),
                            column: Column(end_id_col),
                        }),
                    ],
                }],
                join_type: JoinType::Join,
            });
        }
        
        // For multiple relationship types (UNION CTE), add joins to connect nodes
        // Similar to variable-length paths, we need to clear and rebuild joins
        if let Some(union_cte) = extracted_ctes.iter().find(|cte| {
            cte.cte_name.starts_with("rel_") && !cte.is_recursive
        }) {
            // Check if this is actually a multi-relationship query (has UNION in plan)
            if has_multiple_relationship_types(self) {
                eprintln!("DEBUG: Multi-relationship query detected! Clearing extracted joins and rebuilding...");
                eprintln!("DEBUG: Before clear: {} joins", extracted_joins.len());
                
                // Clear extracted joins like we do for variable-length paths
                // The GraphRel joins include duplicate source node joins which cause
                // "Multiple table expressions with same alias" errors
                extracted_joins.clear();
                
                // Extract the node aliases from the CTE name (e.g., "rel_u_target" → "u", "target")
                let cte_name = union_cte.cte_name.clone();
                let parts: Vec<&str> = cte_name.strip_prefix("rel_").unwrap_or(&cte_name).split('_').collect();
                
                if parts.len() >= 2 {
                    let source_alias = parts[0].to_string();
                    let target_alias = parts[parts.len() - 1].to_string();
                    
                    // Get table names and ID columns from schema
                    let source_table = get_node_table_for_alias(&source_alias);
                    let target_table = get_node_table_for_alias(&target_alias);
                    let source_id_col = get_node_id_column_for_alias(&source_alias);
                    let target_id_col = get_node_id_column_for_alias(&target_alias);
                    
                    // Generate a random alias for the CTE JOIN
                    let cte_alias = crate::query_planner::logical_plan::generate_id();
                    
                    // Add JOIN from CTE to source node (using CTE's from_node_id)
                    extracted_joins.push(Join {
                        table_name: cte_name.clone(),
                        table_alias: cte_alias.clone(),
                        joining_on: vec![OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(cte_alias.clone()),
                                    column: Column("from_node_id".to_string()),
                                }),
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(source_alias.clone()),
                                    column: Column(source_id_col.clone()),
                                }),
                            ],
                        }],
                        join_type: JoinType::Join,
                    });
                    
                    // Add JOIN from CTE to target node (using CTE's to_node_id)
                    extracted_joins.push(Join {
                        table_name: target_table,
                        table_alias: target_alias.clone(),
                        joining_on: vec![OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(cte_alias.clone()),
                                    column: Column("to_node_id".to_string()),
                                }),
                                RenderExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(target_alias.clone()),
                                    column: Column(target_id_col),
                                }),
                            ],
                        }],
                        join_type: JoinType::Join,
                    });
                }
            } else {
                // Old PATCH code for non-UNION multi-rel (keep for backward compat)
                let cte_name = union_cte.cte_name.clone();
                eprintln!("DEBUG: Found union CTE '{}', updating JOINs", cte_name);
                for join in extracted_joins.iter_mut() {
                eprintln!("DEBUG: Checking JOIN table_name='{}' alias='{}'", join.table_name, join.table_alias);
                // Update joins that are relationship tables
                // Check both with and without schema prefix (e.g., "follows" or "test_integration.follows")
                let table_lower = join.table_name.to_lowercase();
                if table_lower.contains("follow") || 
                   table_lower.contains("friend") ||
                   table_lower.contains("like") ||
                   table_lower.contains("purchase") ||
                   join.table_name.starts_with("rel_") {
                    eprintln!("DEBUG: Updating JOIN to use CTE '{}' (was '{}')", cte_name, join.table_name);
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

        // Validate render plan before construction (for CTE path)
        if final_select_items.is_empty() {
            return Err(RenderBuildError::InvalidRenderPlan(
                "No select items found. This usually indicates missing schema information or incomplete query planning.".to_string()
            ));
        }

        // Check if this is a standalone RETURN query (no MATCH, only literals/parameters/functions)
        let is_standalone_return = final_from.is_none() && 
            final_select_items.iter().all(|item| {
                is_standalone_expression(&item.expression)
            });

        if is_standalone_return {
            // For standalone RETURN queries (e.g., "RETURN 1 + 1", "RETURN toUpper($name)"),
            // use ClickHouse's system.one table as a dummy FROM clause
            log::debug!("Detected standalone RETURN query, using system.one as FROM clause");
            
            // Create a ViewTableRef that references system.one
            // Use an Empty LogicalPlan since we don't need actual view resolution for system tables
            final_from = Some(FromTable::new(Some(ViewTableRef {
                source: std::sync::Arc::new(crate::query_planner::logical_plan::LogicalPlan::Empty),
                name: "system.one".to_string(),
                alias: None,
            })));
        }

        // Validate FROM clause exists (after potentially adding system.one for standalone queries)
        if final_from.is_none() {
            return Err(RenderBuildError::InvalidRenderPlan(
                "No FROM clause found. This usually indicates missing table information or incomplete query planning.".to_string()
            ));
        }

        // Validate filters don't contain invalid expressions like "1 = 0"
        if let Some(filter_expr) = &final_filters {
            if is_invalid_filter_expression(filter_expr) {
                return Err(RenderBuildError::InvalidRenderPlan(
                    "Filter contains invalid expression (e.g., '1 = 0'). This indicates failed schema mapping or expression conversion.".to_string()
                ));
            }
        }

        Ok(RenderPlan {
            ctes: CteItems(extracted_ctes),
            select: SelectItems(final_select_items),
            from: FromTableItem(from_table_to_view_ref(final_from)),
            joins: JoinItems(extracted_joins),
            filters: FilterItems(final_filters),
            group_by: GroupByExpressions(extracted_group_by_exprs),
            having_clause: self.extract_having()?,
            order_by: OrderByItems(extracted_order_by),
            skip: SkipItem(extracted_skip_item),
            limit: LimitItem(extracted_limit_item),
            union: UnionItems(extracted_union),
        })
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

fn plan_contains_view_scan(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::ViewScan(_) => true,
        LogicalPlan::GraphNode(node) => plan_contains_view_scan(&node.input),
        LogicalPlan::GraphRel(rel) => {
            plan_contains_view_scan(&rel.left) || 
            plan_contains_view_scan(&rel.right) ||
            plan_contains_view_scan(&rel.center)
        }
        LogicalPlan::Filter(filter) => plan_contains_view_scan(&filter.input),
        LogicalPlan::Projection(proj) => plan_contains_view_scan(&proj.input),
        LogicalPlan::GraphJoins(joins) => plan_contains_view_scan(&joins.input),
        LogicalPlan::GroupBy(group_by) => plan_contains_view_scan(&group_by.input),
        LogicalPlan::OrderBy(order_by) => plan_contains_view_scan(&order_by.input),
        LogicalPlan::Skip(skip) => plan_contains_view_scan(&skip.input),
        LogicalPlan::Limit(limit) => plan_contains_view_scan(&limit.input),
        LogicalPlan::Cte(cte) => plan_contains_view_scan(&cte.input),
        LogicalPlan::Union(union) => union.inputs.iter().any(|i| plan_contains_view_scan(i.as_ref())),
        _ => false,
    }
}

fn apply_property_mapping_to_expr(_expr: &mut RenderExpr, _plan: &LogicalPlan) {
    // DISABLED: Property mapping is now handled in the FilterTagging analyzer pass
    // The analyzer phase maps Cypher properties → database columns, so we should not
    // attempt to re-map them here in the render phase.
    // Re-mapping causes failures because database column names don't exist in property_mappings.
    
    // The LogicalExpr PropertyAccessExp nodes already have the correct database column names
    // when they arrive here from the analyzer, so we just pass them through unchanged.
}

    /// Check if a filter expression appears to be invalid (e.g., "1 = 0")
    fn is_invalid_filter_expression(expr: &RenderExpr) -> bool {
        match expr {
            RenderExpr::OperatorApplicationExp(op) => {
                // Check for "1 = 0" pattern
                if matches!(op.operator, Operator::Equal) && op.operands.len() == 2 {
                    matches!(
                        (&op.operands[0], &op.operands[1]),
                        (RenderExpr::Literal(Literal::Integer(1)), RenderExpr::Literal(Literal::Integer(0))) |
                        (RenderExpr::Literal(Literal::Integer(0)), RenderExpr::Literal(Literal::Integer(1)))
                    )
                } else {
                    false
                }
            }
            _ => false,
        }
    }

/// Get the node label for a given Cypher alias by searching the plan
fn get_node_label_for_alias(alias: &str, plan: &LogicalPlan) -> Option<String> {
    use std::fs::OpenOptions;
    use std::io::Write;
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("debug_property_mapping.log") {
        let _ = writeln!(file, "DEBUG: get_node_label_for_alias: Searching for alias '{}' in plan type {:?}", alias, std::mem::discriminant(plan));
    }
    match plan {
        LogicalPlan::GraphNode(node) if node.alias == alias => {
            if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("debug_property_mapping.log") {
                let _ = writeln!(file, "DEBUG: get_node_label_for_alias: Found GraphNode with matching alias '{}'", alias);
            }
            extract_node_label_from_viewscan(&node.input)
        }
        LogicalPlan::GraphNode(node) => {
            if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("debug_property_mapping.log") {
                let _ = writeln!(file, "DEBUG: get_node_label_for_alias: GraphNode alias '{}' doesn't match '{}', recursing", node.alias, alias);
            }
            get_node_label_for_alias(alias, &node.input)
        }
        LogicalPlan::GraphRel(rel) => {
            if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("debug_property_mapping.log") {
                let _ = writeln!(file, "DEBUG: get_node_label_for_alias: Searching GraphRel for alias '{}'", alias);
            }
            get_node_label_for_alias(alias, &rel.left)
                .or_else(|| {
                    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("debug_property_mapping.log") {
                        let _ = writeln!(file, "DEBUG: get_node_label_for_alias: Alias '{}' not in left, trying center", alias);
                    }
                    get_node_label_for_alias(alias, &rel.center)
                })
                .or_else(|| {
                    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("debug_property_mapping.log") {
                        let _ = writeln!(file, "DEBUG: get_node_label_for_alias: Alias '{}' not in center, trying right", alias);
                    }
                    get_node_label_for_alias(alias, &rel.right)
                })
        }
        LogicalPlan::Filter(filter) => {
            if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("debug_property_mapping.log") {
                let _ = writeln!(file, "DEBUG: get_node_label_for_alias: Recursing through Filter for alias '{}'", alias);
            }
            get_node_label_for_alias(alias, &filter.input)
        }
        LogicalPlan::Projection(proj) => {
            if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("debug_property_mapping.log") {
                let _ = writeln!(file, "DEBUG: get_node_label_for_alias: Recursing through Projection for alias '{}'", alias);
            }
            get_node_label_for_alias(alias, &proj.input)
        }
        LogicalPlan::GraphJoins(joins) => {
            if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("debug_property_mapping.log") {
                let _ = writeln!(file, "DEBUG: get_node_label_for_alias: Recursing through GraphJoins for alias '{}'", alias);
            }
            get_node_label_for_alias(alias, &joins.input)
        }
        LogicalPlan::OrderBy(order_by) => {
            if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("debug_property_mapping.log") {
                let _ = writeln!(file, "DEBUG: get_node_label_for_alias: Recursing through OrderBy for alias '{}'", alias);
            }
            get_node_label_for_alias(alias, &order_by.input)
        }
        LogicalPlan::Skip(skip) => {
            if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("debug_property_mapping.log") {
                let _ = writeln!(file, "DEBUG: get_node_label_for_alias: Recursing through Skip for alias '{}'", alias);
            }
            get_node_label_for_alias(alias, &skip.input)
        }
        LogicalPlan::Limit(limit) => {
            if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("debug_property_mapping.log") {
                let _ = writeln!(file, "DEBUG: get_node_label_for_alias: Recursing through Limit for alias '{}'", alias);
            }
            get_node_label_for_alias(alias, &limit.input)
        }
        LogicalPlan::GroupBy(group_by) => {
            if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("debug_property_mapping.log") {
                let _ = writeln!(file, "DEBUG: get_node_label_for_alias: Recursing through GroupBy for alias '{}'", alias);
            }
            get_node_label_for_alias(alias, &group_by.input)
        }
        LogicalPlan::Cte(cte) => {
            if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("debug_property_mapping.log") {
                let _ = writeln!(file, "DEBUG: get_node_label_for_alias: Recursing through Cte for alias '{}'", alias);
            }
            get_node_label_for_alias(alias, &cte.input)
        }
        LogicalPlan::Union(union) => {
            if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("debug_property_mapping.log") {
                let _ = writeln!(file, "DEBUG: get_node_label_for_alias: Searching Union for alias '{}'", alias);
            }
            for input in &union.inputs {
                if let Some(label) = get_node_label_for_alias(alias, input) {
                    return Some(label);
                }
            }
            None
        }
        _ => {
            if let Ok(mut file) = OpenOptions::new().create(true).append(true).open("debug_property_mapping.log") {
                let _ = writeln!(file, "DEBUG: get_node_label_for_alias: No match for alias '{}' in plan type {:?}", alias, std::mem::discriminant(plan));
            }
            None
        }
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

