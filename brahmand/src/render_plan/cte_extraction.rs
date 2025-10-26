use std::sync::Arc;
use std::fs::OpenOptions;
use std::io::Write;
use crate::query_planner::logical_plan::LogicalPlan;
use crate::query_planner::logical_expr::Direction;
use crate::query_planner::plan_ctx::PlanCtx;
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::clickhouse_query_generator::variable_length_cte::{VariableLengthCteGenerator, ChainedJoinGenerator};

use super::plan_builder::RenderPlanBuilder;
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
use super::expression_utils::references_alias;

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
                Operator::Exponentiation => format!("POWER({}, {})", operands[0], operands[1]),
                Operator::In => format!("{} IN ({})", operands[0], operands[1]),
                Operator::NotIn => format!("{} NOT IN ({})", operands[0], operands[1]),
                Operator::IsNull => format!("{} IS NULL", operands[0]),
                Operator::IsNotNull => format!("{} IS NOT NULL", operands[0]),
                Operator::Distinct => format!("{} IS DISTINCT FROM {}", operands[0], operands[1]),
            }
        }
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
        RenderExpr::List(list) => {
            let items: Vec<String> = list.iter()
                .map(|item| render_expr_to_sql_string(item, alias_mapping))
                .collect();
            format!("({})", items.join(", "))
        }
        RenderExpr::InSubquery(subq) => {
            format!("{} IN ({})", render_expr_to_sql_string(&subq.expr, alias_mapping), "/* subquery */")
        }
        RenderExpr::Case(case) => {
            let when_clauses: Vec<String> = case.when_then.iter()
                .map(|(condition, result)| format!("WHEN {} THEN {}", render_expr_to_sql_string(condition, alias_mapping), render_expr_to_sql_string(result, alias_mapping)))
                .collect();
            let else_clause = case.else_expr.as_ref()
                .map(|expr| format!(" ELSE {}", render_expr_to_sql_string(expr, alias_mapping)))
                .unwrap_or_default();
            format!("CASE {} {} END", case.expr.as_ref().map(|e| render_expr_to_sql_string(e, alias_mapping)).unwrap_or_default(), when_clauses.join(" ") + &else_clause)
        }
        RenderExpr::Star => "*".to_string(),
        RenderExpr::Parameter(param) => format!("${}", param),
    }
}

/// Relationship column information
#[derive(Debug, Clone)]
pub struct RelationshipColumns {
    pub from_column: String,
    pub to_column: String,
}

/// Convert a label to its corresponding table name
pub fn label_to_table_name(label: &str) -> String {
    match label.to_lowercase().as_str() {
        "user" | "users" => "users".to_string(),
        "person" | "people" => "users".to_string(),
        "post" | "posts" => "posts".to_string(),
        "order" | "orders" => "orders".to_string(),
        "product" | "products" => "products".to_string(),
        _ => label.to_lowercase(),
    }
}

/// Convert a relationship type to its corresponding table name
pub fn rel_type_to_table_name(rel_type: &str) -> String {
    match rel_type.to_uppercase().as_str() {
        "FOLLOWS" => "user_follows".to_string(),
        "FRIEND" | "FRIENDS_WITH" => "friendships".to_string(),
        "AUTHORED" => "posts".to_string(),
        "LIKED" => "post_likes".to_string(),
        "PURCHASED" => "orders".to_string(),
        _ => rel_type.to_string(),
    }
}

/// Convert multiple relationship types to table names
pub fn rel_types_to_table_names(rel_types: &[String]) -> Vec<String> {
    rel_types.iter().map(|rt| rel_type_to_table_name(rt)).collect()
}

/// Extract relationship columns from a table name
pub fn extract_relationship_columns_from_table(table_name: &str) -> RelationshipColumns {
    match table_name {
        "user_follows" | "FOLLOWS" => RelationshipColumns {
            from_column: "follower_id".to_string(),
            to_column: "followed_id".to_string(),
        },
        "friendships" | "FRIEND" => RelationshipColumns {
            from_column: "user_id".to_string(),
            to_column: "friend_id".to_string(),
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

/// Extract relationship columns from a LogicalPlan
pub fn extract_relationship_columns(plan: &LogicalPlan) -> Option<RelationshipColumns> {
    match plan {
        LogicalPlan::Scan(scan) => {
            scan.table_name.as_ref().map(|table| extract_relationship_columns_from_table(table))
        }
        LogicalPlan::ViewScan(view_scan) => {
            Some(extract_relationship_columns_from_table(&view_scan.source_table))
        }
        LogicalPlan::GraphRel(rel) => extract_relationship_columns(&rel.center),
        LogicalPlan::Filter(filter) => extract_relationship_columns(&filter.input),
        LogicalPlan::Projection(proj) => extract_relationship_columns(&proj.input),
        _ => None,
    }
}

/// Extract ID column from a LogicalPlan
fn extract_id_column(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::Scan(scan) => scan.table_name.as_ref().map(|table| table_to_id_column(table)),
        LogicalPlan::ViewScan(view_scan) => Some(table_to_id_column(&view_scan.source_table)),
        LogicalPlan::GraphNode(node) => extract_id_column(&node.input),
        LogicalPlan::Filter(filter) => extract_id_column(&filter.input),
        LogicalPlan::Projection(proj) => extract_id_column(&proj.input),
        _ => None,
    }
}

/// Get ID column for a table
pub fn table_to_id_column(table: &str) -> String {
    match table {
        "users" => "id".to_string(),
        "posts" => "id".to_string(),
        "orders" => "id".to_string(),
        "products" => "id".to_string(),
        _ => "id".to_string(),
    }
}

/// Get ID column for a label
fn table_to_id_column_for_label(label: &str) -> String {
    table_to_id_column(&label_to_table_name(label))
}

/// Get relationship columns from schema
fn get_relationship_columns_from_schema(rel_type: &str) -> Option<(String, String)> {
    let table = rel_type_to_table_name(rel_type);
    let cols = extract_relationship_columns_from_table(&table);
    Some((cols.from_column, cols.to_column))
}

/// Get relationship columns by table name
fn get_relationship_columns_by_table(table_name: &str) -> Option<(String, String)> {
    let cols = extract_relationship_columns_from_table(table_name);
    Some((cols.from_column, cols.to_column))
}

/// Get node info from schema
fn get_node_info_from_schema(node_label: &str) -> Option<(String, String)> {
    let table = label_to_table_name(node_label);
    let id_col = table_to_id_column(&table);
    Some((table, id_col))
}

/// Apply property mapping to an expression
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

/// Convert a plan to string for debugging
fn plan_to_string(plan: &LogicalPlan, depth: usize) -> String {
    let indent = "  ".repeat(depth);
    match plan {
        LogicalPlan::Empty => format!("{}Empty", indent),
        LogicalPlan::Scan(scan) => format!("{}Scan(table: {:?})", indent, scan.table_name),
        LogicalPlan::ViewScan(view_scan) => format!("{}ViewScan(table: {})", indent, view_scan.source_table),
        LogicalPlan::GraphNode(node) => format!("{}GraphNode(alias: {}, input: {})", indent, node.alias, plan_to_string(&node.input, depth + 1)),
        LogicalPlan::GraphRel(rel) => format!("{}GraphRel(left: {}, center: {}, right: {})", indent,
            plan_to_string(&rel.left, depth + 1),
            plan_to_string(&rel.center, depth + 1),
            plan_to_string(&rel.right, depth + 1)),
        LogicalPlan::Filter(filter) => format!("{}Filter(input: {})", indent, plan_to_string(&filter.input, depth + 1)),
        LogicalPlan::Projection(proj) => format!("{}Projection(input: {})", indent, plan_to_string(&proj.input, depth + 1)),
        LogicalPlan::GraphJoins(joins) => format!("{}GraphJoins(input: {})", indent, plan_to_string(&joins.input, depth + 1)),
        LogicalPlan::OrderBy(order_by) => format!("{}OrderBy(input: {})", indent, plan_to_string(&order_by.input, depth + 1)),
        LogicalPlan::Skip(skip) => format!("{}Skip(input: {})", indent, plan_to_string(&skip.input, depth + 1)),
        LogicalPlan::Limit(limit) => format!("{}Limit(input: {})", indent, plan_to_string(&limit.input, depth + 1)),
        LogicalPlan::GroupBy(group_by) => format!("{}GroupBy(input: {})", indent, plan_to_string(&group_by.input, depth + 1)),
        LogicalPlan::Cte(cte) => format!("{}Cte(name: {}, input: {})", indent, cte.name, plan_to_string(&cte.input, depth + 1)),
        LogicalPlan::Union(union) => format!("{}Union(inputs: {})", indent, union.inputs.len()),
        LogicalPlan::PageRank(_) => format!("{}PageRank", indent),
    }
}

/// Extract CTEs with context - the main CTE extraction function
pub fn extract_ctes_with_context(plan: &LogicalPlan, last_node_alias: &str, context: &mut super::cte_generation::CteGenerationContext) -> RenderPlanBuilderResult<Vec<Cte>> {
    match plan {
        LogicalPlan::Empty => Ok(vec![]),
        LogicalPlan::Scan(_) => Ok(vec![]),
        LogicalPlan::ViewScan(_) => Ok(vec![]),
        LogicalPlan::GraphNode(graph_node) => extract_ctes_with_context(&graph_node.input, last_node_alias, context),
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
                let mut child_ctes = extract_ctes_with_context(&graph_rel.right, last_node_alias, context)?;
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
            let mut right_cte = extract_ctes_with_context(&graph_rel.right, last_node_alias, context)?;
            let mut center_cte = extract_ctes_with_context(&graph_rel.center, last_node_alias, context)?;
            right_cte.append(&mut center_cte);
            let left_alias = &graph_rel.left_connection;
            if left_alias != last_node_alias {
                let mut left_cte = extract_ctes_with_context(&graph_rel.left, last_node_alias, context)?;
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
            let ctes = extract_ctes_with_context(&filter.input, last_node_alias, &mut new_context)?;
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
            extract_ctes_with_context(&projection.input, last_node_alias, context)
        }
        LogicalPlan::GraphJoins(graph_joins) => extract_ctes_with_context(&graph_joins.input, last_node_alias, context),
        LogicalPlan::GroupBy(group_by) => extract_ctes_with_context(&group_by.input, last_node_alias, context),
        LogicalPlan::OrderBy(order_by) => extract_ctes_with_context(&order_by.input, last_node_alias, context),
        LogicalPlan::Skip(skip) => extract_ctes_with_context(&skip.input, last_node_alias, context),
        LogicalPlan::Limit(limit) => extract_ctes_with_context(&limit.input, last_node_alias, context),
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
                ctes.append(&mut extract_ctes_with_context(input_plan, last_node_alias, context)?);
            }
            Ok(ctes)
        }
        LogicalPlan::PageRank(_) => Ok(vec![]),
    }
}

/// Check if the plan contains a variable-length relationship and return node aliases
/// Returns (left_alias, right_alias) if found
pub fn has_variable_length_rel(plan: &LogicalPlan) -> Option<(String, String)> {
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

/// Extract path variable from the plan
pub fn get_path_variable(plan: &LogicalPlan) -> Option<String> {
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

/// Extract node label from ViewScan in the plan
pub fn extract_node_label_from_viewscan(plan: &LogicalPlan) -> Option<String> {
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

/// Get node schema information by table name
pub fn get_node_schema_by_table<'a>(schema: &'a GraphSchema, table_name: &str) -> Option<(&'a str, &'a crate::graph_catalog::graph_schema::NodeSchema)> {
    for (label, node_schema) in schema.get_nodes_schemas() {
        if node_schema.table_name == table_name {
            return Some((label.as_str(), node_schema));
        }
    }
    None
}