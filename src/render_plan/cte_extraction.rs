//! CTE extraction utilities for variable-length path handling
//!
//! Some functions in this module are reserved for future use or used only in specific code paths.
#![allow(dead_code)]

use crate::clickhouse_query_generator::variable_length_cte::{VariableLengthCteGenerator, NodeProperty};
use crate::graph_catalog::expression_parser::PropertyValue;
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::query_planner::logical_plan::LogicalPlan;
use crate::utils::cte_naming::generate_cte_base_name;

use super::cte_generation::map_property_to_column_with_schema;
use super::errors::RenderBuildError;
use super::filter_pipeline::categorize_filters;
use super::plan_builder::RenderPlanBuilder;
use super::render_expr::{Literal, Operator, PropertyAccess, RenderExpr};
use super::{Cte, CteContent, Join, JoinType};

pub type RenderPlanBuilderResult<T> = Result<T, super::errors::RenderBuildError>;

/// Check if an expression contains a string literal (recursively for nested + operations)
fn contains_string_literal(expr: &RenderExpr) -> bool {
    match expr {
        RenderExpr::Literal(Literal::String(_)) => true,
        RenderExpr::OperatorApplicationExp(op) if op.operator == Operator::Addition => {
            op.operands.iter().any(|o| contains_string_literal(o))
        }
        _ => false,
    }
}

/// Check if any operand is a string literal (for string concatenation detection)
fn has_string_operand(operands: &[RenderExpr]) -> bool {
    operands.iter().any(|op| contains_string_literal(op))
}

/// Flatten nested + operations into a list of operands for concat()
fn flatten_addition_operands(expr: &RenderExpr, alias_mapping: &[(String, String)]) -> Vec<String> {
    match expr {
        RenderExpr::OperatorApplicationExp(op) if op.operator == Operator::Addition => op
            .operands
            .iter()
            .flat_map(|o| flatten_addition_operands(o, alias_mapping))
            .collect(),
        _ => vec![render_expr_to_sql_string(expr, alias_mapping)],
    }
}

/// Helper function to extract the node alias from a GraphNode
fn extract_node_alias(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::GraphNode(node) => Some(node.alias.clone()),
        LogicalPlan::Filter(filter) => extract_node_alias(&filter.input),
        LogicalPlan::Projection(proj) => extract_node_alias(&proj.input),
        _ => None,
    }
}

/// Extract schema filter from a node's ViewScan (for CTE generation)
/// Returns the raw filter SQL with table alias replaced to match CTE convention
fn extract_schema_filter_from_node(plan: &LogicalPlan, cte_alias: &str) -> Option<String> {
    match plan {
        LogicalPlan::GraphNode(node) => extract_schema_filter_from_node(&node.input, cte_alias),
        LogicalPlan::ViewScan(view_scan) => {
            if let Some(ref schema_filter) = view_scan.schema_filter {
                // Convert schema filter to SQL with the CTE alias
                schema_filter.to_sql(cte_alias).ok()
            } else {
                None
            }
        }
        LogicalPlan::Filter(filter) => extract_schema_filter_from_node(&filter.input, cte_alias),
        _ => None,
    }
}

/// Helper function to extract the actual table name from a LogicalPlan node
/// Recursively traverses the plan tree to find the Scan or ViewScan node
/// Extract filters from a bound node (Filter → GraphNode structure)
/// Returns the filter expression in SQL format suitable for CTE WHERE clauses
fn extract_bound_node_filter(plan: &LogicalPlan, node_alias: &str, cte_alias: &str) -> Option<String> {
    match plan {
        LogicalPlan::Filter(filter) => {
            // Found a filter - convert to RenderExpr and then to SQL
            if let Ok(mut render_expr) = RenderExpr::try_from(filter.predicate.clone()) {
                // Apply property mapping to the filter expression
                apply_property_mapping_to_expr(&mut render_expr, plan);
                
                // Create alias mapping: node_alias → cte_alias (e.g., "p1" → "start_node")
                let alias_mapping = [(node_alias.to_string(), cte_alias.to_string())];
                let filter_sql = render_expr_to_sql_string(&render_expr, &alias_mapping);
                
                log::info!("🔍 Extracted bound node filter: {} → {}", node_alias, filter_sql);
                return Some(filter_sql);
            }
            None
        }
        LogicalPlan::GraphNode(node) => {
            // Recurse into the node's input in case there's a filter there
            extract_bound_node_filter(&node.input, node_alias, cte_alias)
        }
        LogicalPlan::CartesianProduct(cp) => {
            // For CartesianProduct, the filter might be on either side
            // Try right first (most recent pattern), then left
            if let Some(filter) = extract_bound_node_filter(&cp.right, node_alias, cte_alias) {
                Some(filter)
            } else {
                extract_bound_node_filter(&cp.left, node_alias, cte_alias)
            }
        }
        _ => None,
    }
}

/// Extract node labels from a GraphNode plan (supports multi-label nodes)
/// Returns Vec of labels, or None if no labels found
fn extract_node_labels(plan: &LogicalPlan) -> Option<Vec<String>> {
    match plan {
        LogicalPlan::GraphNode(node) => {
            // Check if node has a label
            if let Some(ref label) = node.label {
                Some(vec![label.clone()])
            } else {
                None
            }
        }
        LogicalPlan::Filter(filter) => extract_node_labels(&filter.input),
        LogicalPlan::Projection(proj) => extract_node_labels(&proj.input),
        _ => None,
    }
}

/// Check if variable-length path should use JOIN expansion (for multi-type nodes)
/// instead of recursive CTE
///
/// Returns true if:
/// 1. End node has multiple explicit labels (e.g., (x:User|Post))
/// 2. Multiple relationship types lead to different end node types
///
/// Examples:
/// - `MATCH (u:User)-[:FOLLOWS|AUTHORED*1..2]->(x)` where FOLLOWS→User, AUTHORED→Post
/// - `MATCH (u:User)-[:FOLLOWS*1..2]->(x:User|Post)`
fn should_use_join_expansion(
    graph_rel: &crate::query_planner::logical_plan::GraphRel,
    rel_types: &[String],
    schema: &GraphSchema,
) -> bool {
    log::info!("🔍 VLP: should_use_join_expansion called with {} rel_types: {:?}", rel_types.len(), rel_types);
    
    // Extract end node labels from right node
    let end_node_labels = extract_node_labels(&graph_rel.right);
    log::info!("🔍 VLP: end_node_labels = {:?}", end_node_labels);
    
    // Case 1: End node has multiple explicit labels (x:User|Post)
    if let Some(ref labels) = end_node_labels {
        if labels.len() > 1 {
            log::info!(
                "🎯 CTE: Multi-type VLP detected - end node has {} labels: {:?}",
                labels.len(),
                labels
            );
            return true;
        }
    }
    
    // Case 2: Multiple relationship types that connect to different node types
    // This requires checking the schema to see what to_node each rel_type connects to
    if rel_types.len() > 1 {
        log::info!("🔍 VLP: Checking if {} rel_types lead to different end node types...", rel_types.len());
        let mut end_types = std::collections::HashSet::new();
        for rel_type in rel_types {
            log::info!("🔍 VLP: Looking up schema for rel_type '{}'...", rel_type);
            if let Ok(rel_schema) = schema.get_rel_schema(rel_type) {
                log::info!("🔍 VLP: rel_type '{}' → to_node '{}'", rel_type, rel_schema.to_node);
                end_types.insert(rel_schema.to_node.clone());
            } else {
                log::warn!("⚠️  VLP: Failed to get schema for rel_type '{}'", rel_type);
            }
        }
        
        log::info!("🔍 VLP: Found {} unique end_types: {:?}", end_types.len(), end_types);
        
        if end_types.len() > 1 {
            log::info!(
                "🎯 CTE: Multi-type VLP detected - {} relationship types lead to {} different end node types: {:?}",
                rel_types.len(),
                end_types.len(),
                end_types
            );
            return true;
        }
    }
    
    log::info!(
        "🎯 CTE: Single-type VLP - using recursive CTE (end_labels={:?}, rel_types={:?})",
        end_node_labels,
        rel_types
    );
    false
}

/// Public wrapper for should_use_join_expansion for use in plan_builder.rs
pub(crate) fn should_use_join_expansion_public(
    graph_rel: &crate::query_planner::logical_plan::GraphRel,
    rel_types: &[String],
    schema: &GraphSchema,
) -> bool {
    should_use_join_expansion(graph_rel, rel_types, schema)
}

fn extract_table_name(plan: &LogicalPlan) -> Option<String> {
    let result = match plan {
        LogicalPlan::ViewScan(view_scan) => {
            log::debug!("extract_table_name: ViewScan, table={}", view_scan.source_table);
            Some(view_scan.source_table.clone())
        }
        LogicalPlan::GraphNode(node) => {
            log::debug!("extract_table_name: GraphNode, alias={}, label={:?}", node.alias, node.label);
            // First try to extract from the input (ViewScan/Scan)
            if let Some(table) = extract_table_name(&node.input) {
                return Some(table);
            }
            // 🔧 FIX: Fallback to label-based lookup for bound nodes
            // When a node is bound from an earlier pattern (e.g., MATCH (person1:Person {id: 1}), ...),
            // its input is an empty Scan with no table name. Use the node's label to look up the table.
            log::debug!("🔍 extract_table_name: GraphNode alias='{}', label={:?}", node.alias, node.label);
            if let Some(label) = &node.label {
                let table = label_to_table_name(label);
                log::info!("🔧 extract_table_name: Using label '{}' → table '{}'", label, table);
                return Some(table);
            }
            log::warn!("⚠️  extract_table_name: GraphNode '{}' has no label and no table in input", node.alias);
            None
        }
        LogicalPlan::GraphRel(rel) => {
            log::debug!("extract_table_name: GraphRel, recursing to left");
            // For nested GraphRel (e.g., when VLP connects to another relationship),
            // extract the node table from the LEFT side, not the relationship table from CENTER
            // Example: (person)<-[:HAS_CREATOR]-(message)-[:REPLY_OF*0..]->(post)
            // When processing REPLY_OF, left is HAS_CREATOR GraphRel, need message node table
            extract_table_name(&rel.left)
        }
        LogicalPlan::Filter(filter) => {
            log::debug!("extract_table_name: Filter, recursing to input");
            extract_table_name(&filter.input)
        }
        LogicalPlan::Projection(proj) => {
            log::debug!("extract_table_name: Projection, recursing to input");
            extract_table_name(&proj.input)
        }
        LogicalPlan::CartesianProduct(cp) => {
            log::debug!("extract_table_name: CartesianProduct, checking right side first");
            // For CartesianProduct from comma-separated patterns like:
            // MATCH (p1:Person {id: 1}), (p2:Person {id: 2}), path = shortestPath((p1)-[*]-(p2))
            // The right side contains the most recent pattern (p2), left contains earlier patterns (p1)
            // When extracting table for a bound node, try right first (most likely to be the target)
            if let Some(table) = extract_table_name(&cp.right) {
                return Some(table);
            }
            // If right doesn't work, try left
            extract_table_name(&cp.left)
        }
        other => {
            let plan_type = match other {
                LogicalPlan::Empty => "Empty",
                LogicalPlan::Union(_) => "Union",
                LogicalPlan::PageRank(_) => "PageRank",
                LogicalPlan::Unwind(_) => "Unwind",
                LogicalPlan::CartesianProduct(_) => "CartesianProduct",
                LogicalPlan::WithClause(_) => "WithClause",
                LogicalPlan::GroupBy(_) => "GroupBy",
                LogicalPlan::OrderBy(_) => "OrderBy",
                LogicalPlan::Skip(_) => "Skip",
                LogicalPlan::Limit(_) => "Limit",
                LogicalPlan::Cte(_) => "Cte",
                LogicalPlan::GraphJoins(_) => "GraphJoins",
                _ => "Unknown",
            };
            log::warn!("extract_table_name: Unhandled plan type: {}", plan_type);
            None
        }
    };
    log::debug!("extract_table_name: result={:?}", result);
    result
}

/// Extract table name with parameterized view syntax if applicable.
/// For parameterized views, returns `table(param1='value1', param2='value2')`.
/// For regular tables, returns just the table name.
/// 
/// This is essential for VLP CTE generation where both node tables and relationship
/// tables may be parameterized views (e.g., multi-tenant GraphRAG schemas).
fn extract_parameterized_table_name(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::ViewScan(view_scan) => {
            // Check if this is a parameterized view
            if let (Some(ref param_names), Some(ref param_values)) = 
                (&view_scan.view_parameter_names, &view_scan.view_parameter_values) 
            {
                if !param_names.is_empty() {
                    // Generate parameterized view call: table(param1='value1', param2='value2')
                    let param_pairs: Vec<String> = param_names
                        .iter()
                        .filter_map(|name| {
                            param_values.get(name).map(|value| {
                                // Escape single quotes in value for SQL safety
                                let escaped_value = value.replace('\'', "''");
                                format!("{} = '{}'", name, escaped_value)
                            })
                        })
                        .collect();
                    
                    if !param_pairs.is_empty() {
                        let result = format!("{}({})", view_scan.source_table, param_pairs.join(", "));
                        log::debug!(
                            "extract_parameterized_table_name: ViewScan '{}' → '{}'",
                            view_scan.source_table, result
                        );
                        return Some(result);
                    }
                }
            }
            // No parameters - return plain table name
            log::debug!("extract_parameterized_table_name: ViewScan '{}' (no params)", view_scan.source_table);
            Some(view_scan.source_table.clone())
        }
        LogicalPlan::GraphNode(node) => {
            log::debug!("extract_parameterized_table_name: GraphNode alias='{}', label={:?}", node.alias, node.label);
            // First try to extract from the input (ViewScan/Scan)
            if let Some(table) = extract_parameterized_table_name(&node.input) {
                return Some(table);
            }
            // Fallback: use plain table name if label-based lookup needed
            if let Some(label) = &node.label {
                let table = label_to_table_name(label);
                log::info!("extract_parameterized_table_name: Using label '{}' → table '{}' (no params)", label, table);
                return Some(table);
            }
            None
        }
        LogicalPlan::GraphRel(rel) => {
            log::debug!("extract_parameterized_table_name: GraphRel, recursing to left");
            extract_parameterized_table_name(&rel.left)
        }
        LogicalPlan::Filter(filter) => {
            extract_parameterized_table_name(&filter.input)
        }
        LogicalPlan::Projection(proj) => {
            extract_parameterized_table_name(&proj.input)
        }
        LogicalPlan::CartesianProduct(cp) => {
            if let Some(table) = extract_parameterized_table_name(&cp.right) {
                return Some(table);
            }
            extract_parameterized_table_name(&cp.left)
        }
        _ => None,
    }
}

/// Extract parameterized table name specifically from a ViewScan (used for relationship center)
fn extract_parameterized_rel_table(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::ViewScan(view_scan) => {
            // Check if this is a parameterized view
            if let (Some(ref param_names), Some(ref param_values)) = 
                (&view_scan.view_parameter_names, &view_scan.view_parameter_values) 
            {
                if !param_names.is_empty() {
                    // Generate parameterized view call: table(param1='value1', param2='value2')
                    let param_pairs: Vec<String> = param_names
                        .iter()
                        .filter_map(|name| {
                            param_values.get(name).map(|value| {
                                let escaped_value = value.replace('\'', "''");
                                format!("{} = '{}'", name, escaped_value)
                            })
                        })
                        .collect();
                    
                    if !param_pairs.is_empty() {
                        let result = format!("{}({})", view_scan.source_table, param_pairs.join(", "));
                        log::info!(
                            "extract_parameterized_rel_table: Relationship '{}' → '{}'",
                            view_scan.source_table, result
                        );
                        return Some(result);
                    }
                }
            }
            log::debug!("extract_parameterized_rel_table: '{}' (no params)", view_scan.source_table);
            Some(view_scan.source_table.clone())
        }
        _ => None,
    }
}

/// Extract view_parameter_values from a LogicalPlan (traverses to find ViewScan)
/// This is used for multi-type VLP to get the parameter values from the query context
fn extract_view_parameter_values(plan: &LogicalPlan) -> Option<std::collections::HashMap<String, String>> {
    match plan {
        LogicalPlan::ViewScan(view_scan) => {
            let params = view_scan.view_parameter_values.clone().unwrap_or_default();
            log::debug!("🔍 extract_view_parameter_values: ViewScan '{}' → params {:?}", view_scan.source_table, params);
            if params.is_empty() {
                None
            } else {
                Some(params)
            }
        }
        LogicalPlan::GraphNode(node) => {
            log::debug!("🔍 extract_view_parameter_values: GraphNode '{}' → recursing into input", node.alias);
            extract_view_parameter_values(&node.input)
        }
        LogicalPlan::GraphRel(rel) => {
            // Try left first, then center, then right
            log::debug!("🔍 extract_view_parameter_values: GraphRel → trying left");
            if let Some(left_params) = extract_view_parameter_values(&rel.left) {
                return Some(left_params);
            }
            log::debug!("🔍 extract_view_parameter_values: GraphRel → trying center");
            if let Some(center_params) = extract_view_parameter_values(&rel.center) {
                return Some(center_params);
            }
            log::debug!("🔍 extract_view_parameter_values: GraphRel → trying right");
            extract_view_parameter_values(&rel.right)
        }
        LogicalPlan::Filter(filter) => extract_view_parameter_values(&filter.input),
        LogicalPlan::Projection(proj) => extract_view_parameter_values(&proj.input),
        _ => {
            log::debug!("🔍 extract_view_parameter_values: Unhandled plan type → None");
            None
        }
    }
}

/// Convert a RenderExpr to a SQL string for use in CTE WHERE clauses
fn render_expr_to_sql_string(expr: &RenderExpr, alias_mapping: &[(String, String)]) -> String {
    match expr {
        RenderExpr::Column(col) => col.raw().to_string(),
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
            let table_alias = alias_mapping
                .iter()
                .find(|(cypher, _)| *cypher == prop.table_alias.0)
                .map(|(_, cte)| cte.clone())
                .unwrap_or_else(|| prop.table_alias.0.clone());
            format!("{}.{}", table_alias, prop.column.raw())
        }
        RenderExpr::OperatorApplicationExp(op) => {
            // Special handling for IS NULL / IS NOT NULL with wildcard property access (e.g., r.*)
            // Convert r.* to r.from_id for null checks (LEFT JOIN produces NULL for all columns)
            let operands: Vec<String> = if matches!(op.operator, Operator::IsNull | Operator::IsNotNull) 
                && op.operands.len() == 1 
                && matches!(&op.operands[0], RenderExpr::PropertyAccessExp(prop) if prop.column.raw() == "*")
            {
                // Extract the relationship alias and use from_id column instead of wildcard
                if let RenderExpr::PropertyAccessExp(prop) = &op.operands[0] {
                    let table_alias = alias_mapping
                        .iter()
                        .find(|(cypher, _)| *cypher == prop.table_alias.0)
                        .map(|(_, cte)| cte.clone())
                        .unwrap_or_else(|| prop.table_alias.0.clone());
                    
                    // Use from_id as the representative column for null check
                    // (LEFT JOIN makes all columns NULL together, so checking one is sufficient)
                    vec![format!("{}.from_id", table_alias)]
                } else {
                    op.operands
                        .iter()
                        .map(|operand| render_expr_to_sql_string(operand, alias_mapping))
                        .collect()
                }
            } else {
                op.operands
                    .iter()
                    .map(|operand| render_expr_to_sql_string(operand, alias_mapping))
                    .collect()
            };
            
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
                Operator::Addition => {
                    // Use concat() for string concatenation
                    // Flatten nested + operations for cases like: a + ' - ' + b
                    if has_string_operand(&op.operands) {
                        let flattened: Vec<String> = op
                            .operands
                            .iter()
                            .flat_map(|o| flatten_addition_operands(o, alias_mapping))
                            .collect();
                        format!("concat({})", flattened.join(", "))
                    } else {
                        format!("{} + {}", operands[0], operands[1])
                    }
                }
                Operator::Subtraction => format!("{} - {}", operands[0], operands[1]),
                Operator::Multiplication => format!("{} * {}", operands[0], operands[1]),
                Operator::Division => format!("{} / {}", operands[0], operands[1]),
                Operator::ModuloDivision => format!("{} % {}", operands[0], operands[1]),
                Operator::Exponentiation => format!("POWER({}, {})", operands[0], operands[1]),
                Operator::In => {
                    // Check if right operand is a property access (array column)
                    // Cypher: x IN array_property → ClickHouse: has(array, x)
                    if matches!(&op.operands[1], RenderExpr::PropertyAccessExp(_)) {
                        format!("has({}, {})", operands[1], operands[0])
                    } else {
                        format!("{} IN {}", operands[0], operands[1])
                    }
                }
                Operator::NotIn => {
                    if matches!(&op.operands[1], RenderExpr::PropertyAccessExp(_)) {
                        format!("NOT has({}, {})", operands[1], operands[0])
                    } else {
                        format!("{} NOT IN {}", operands[0], operands[1])
                    }
                }
                Operator::StartsWith => format!("startsWith({}, {})", operands[0], operands[1]),
                Operator::EndsWith => format!("endsWith({}, {})", operands[0], operands[1]),
                Operator::Contains => format!("(position({}, {}) > 0)", operands[0], operands[1]),
                Operator::IsNull => format!("{} IS NULL", operands[0]),
                Operator::IsNotNull => format!("{} IS NOT NULL", operands[0]),
                Operator::Distinct => format!("{} IS DISTINCT FROM {}", operands[0], operands[1]),
                Operator::RegexMatch => format!("match({}, {})", operands[0], operands[1]),
            }
        }
        RenderExpr::ScalarFnCall(func) => {
            let args: Vec<String> = func
                .args
                .iter()
                .map(|arg| render_expr_to_sql_string(arg, alias_mapping))
                .collect();
            format!("{}({})", func.name, args.join(", "))
        }
        RenderExpr::AggregateFnCall(agg) => {
            let args: Vec<String> = agg
                .args
                .iter()
                .map(|arg| render_expr_to_sql_string(arg, alias_mapping))
                .collect();
            format!("{}({})", agg.name, args.join(", "))
        }
        RenderExpr::List(list) => {
            let items: Vec<String> = list
                .iter()
                .map(|item| render_expr_to_sql_string(item, alias_mapping))
                .collect();
            format!("({})", items.join(", "))
        }
        RenderExpr::InSubquery(subq) => {
            format!(
                "{} IN ({})",
                render_expr_to_sql_string(&subq.expr, alias_mapping),
                "/* subquery */"
            )
        }
        RenderExpr::Case(case) => {
            let when_clauses: Vec<String> = case
                .when_then
                .iter()
                .map(|(condition, result)| {
                    format!(
                        "WHEN {} THEN {}",
                        render_expr_to_sql_string(condition, alias_mapping),
                        render_expr_to_sql_string(result, alias_mapping)
                    )
                })
                .collect();
            let else_clause = case
                .else_expr
                .as_ref()
                .map(|expr| format!(" ELSE {}", render_expr_to_sql_string(expr, alias_mapping)))
                .unwrap_or_default();
            format!(
                "CASE {} {} END",
                case.expr
                    .as_ref()
                    .map(|e| render_expr_to_sql_string(e, alias_mapping))
                    .unwrap_or_default(),
                when_clauses.join(" ") + &else_clause
            )
        }
        RenderExpr::ExistsSubquery(exists) => {
            // Use the pre-generated SQL from ExistsSubquery
            format!("EXISTS ({})", exists.sql)
        }
        RenderExpr::ReduceExpr(reduce) => {
            // Convert to ClickHouse arrayFold((acc, x) -> expr, list, init)
            // Cast numeric init to Int64 to prevent type mismatch issues
            let init_sql = render_expr_to_sql_string(&reduce.initial_value, alias_mapping);
            let list_sql = render_expr_to_sql_string(&reduce.list, alias_mapping);
            let expr_sql = render_expr_to_sql_string(&reduce.expression, alias_mapping);

            // Wrap numeric init values in toInt64() to prevent type mismatch
            let init_cast = if matches!(
                *reduce.initial_value,
                RenderExpr::Literal(Literal::Integer(_))
            ) {
                format!("toInt64({})", init_sql)
            } else {
                init_sql
            };

            format!(
                "arrayFold({}, {} -> {}, {}, {})",
                reduce.variable, reduce.accumulator, expr_sql, list_sql, init_cast
            )
        }
        RenderExpr::PatternCount(pc) => {
            // Use the pre-generated SQL from PatternCount
            pc.sql.clone()
        }
        RenderExpr::ArraySubscript { array, index } => {
            let array_sql = render_expr_to_sql_string(array, alias_mapping);
            let index_sql = render_expr_to_sql_string(index, alias_mapping);
            format!("{}[{}]", array_sql, index_sql)
        }
        RenderExpr::Star => "*".to_string(),
        RenderExpr::Parameter(param) => format!("${}", param),
        RenderExpr::MapLiteral(entries) => {
            // Map literals handled specially for duration(), point(), etc.
            let pairs: Vec<String> = entries
                .iter()
                .map(|(k, v)| {
                    let val_sql = render_expr_to_sql_string(v, alias_mapping);
                    format!("'{}': {}", k, val_sql)
                })
                .collect();
            format!("{{{}}}", pairs.join(", "))
        }
    }
}

/// Relationship column information
#[derive(Debug, Clone)]
pub struct RelationshipColumns {
    pub from_id: String,
    pub to_id: String,
}

/// Convert a label to its corresponding table name using provided schema
pub fn label_to_table_name_with_schema(
    label: &str,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> String {
    match schema.get_node_schema(label) {
        Ok(node_schema) => {
            // Use fully qualified table name: database.table_name
            format!("{}.{}", node_schema.database, node_schema.table_name)
        }
        Err(_) => {
            // NO FALLBACK - log error!
            log::error!("❌ SCHEMA ERROR: Node label '{}' not found in schema.", label);
            format!("ERROR_NODE_SCHEMA_MISSING_{}", label)
        }
    }
}

/// Convert a label to its corresponding table name
/// DEPRECATED: Use label_to_table_name_with_schema instead
pub fn label_to_table_name(label: &str) -> String {
    // Get the table name from the schema
    if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
        if let Ok(schemas) = schemas_lock.try_read() {
            if let Some(schema) = schemas.get("default") {
                return label_to_table_name_with_schema(label, schema);
            }
        }
    }

    // NO FALLBACK - log error!
    log::error!("❌ SCHEMA ERROR: GLOBAL_SCHEMAS not initialized. Cannot resolve label '{}'.", label);
    format!("ERROR_SCHEMA_NOT_INITIALIZED_{}", label)
}

/// Convert a relationship type to its corresponding table name using provided schema
/// For polymorphic relationships (multiple tables for same relationship type), specify node types
pub fn rel_type_to_table_name_with_nodes(
    rel_type: &str,
    from_node: Option<&str>,
    to_node: Option<&str>,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> String {
    match schema.get_rel_schema_with_nodes(rel_type, from_node, to_node) {
        Ok(rel_schema) => {
            // Use fully qualified table name: database.table_name
            format!("{}.{}", rel_schema.database, rel_schema.table_name)
        }
        Err(_) => {
            // NO FALLBACK - log error and return marker that will fail in ClickHouse
            log::error!(
                "❌ SCHEMA ERROR: Relationship type '{}' (from_node={:?}, to_node={:?}) not found in schema. This should have been caught during query planning.",
                rel_type, from_node, to_node
            );
            format!("ERROR_SCHEMA_MISSING_{}_FROM_{:?}_TO_{:?}", rel_type, from_node, to_node)
        }
    }
}

/// Convert a relationship type to its corresponding table name with parameterized view support
/// This is used when the relationship doesn't come from a ViewScan (e.g., inferred from schema)
/// but we still need to apply parameterized view syntax if the rel schema defines view_parameters.
pub fn rel_type_to_table_name_with_nodes_and_params(
    rel_type: &str,
    from_node: Option<&str>,
    to_node: Option<&str>,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
    view_parameter_values: &std::collections::HashMap<String, String>,
) -> String {
    match schema.get_rel_schema_with_nodes(rel_type, from_node, to_node) {
        Ok(rel_schema) => {
            let base_table = format!("{}.{}", rel_schema.database, rel_schema.table_name);
            
            // Check if rel_schema has view_parameters and if we have values for them
            if let Some(ref view_params) = rel_schema.view_parameters {
                if !view_params.is_empty() && !view_parameter_values.is_empty() {
                    // Build parameterized view syntax: `db.table`(param1 = 'value1', param2 = 'value2')
                    let params: Vec<String> = view_params
                        .iter()
                        .filter_map(|param| {
                            view_parameter_values.get(param).map(|value| {
                                format!("{} = '{}'", param, value)
                            })
                        })
                        .collect();
                    
                    if !params.is_empty() {
                        let param_str = params.join(", ");
                        log::info!("🔧 Applying parameterized view syntax to rel table: `{}`({})", base_table, param_str);
                        return format!("`{}`({})", base_table, param_str);
                    }
                }
            }
            
            // No parameterized view or no matching values - return plain table name
            base_table
        }
        Err(_) => {
            log::error!(
                "❌ SCHEMA ERROR: Relationship type '{}' (from_node={:?}, to_node={:?}) not found in schema.",
                rel_type, from_node, to_node
            );
            format!("ERROR_SCHEMA_MISSING_{}_FROM_{:?}_TO_{:?}", rel_type, from_node, to_node)
        }
    }
}

/// Convert a relationship type to its corresponding table name using provided schema
pub fn rel_type_to_table_name_with_schema(
    rel_type: &str,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> String {
    match schema.get_rel_schema(rel_type) {
        Ok(rel_schema) => {
            // Use fully qualified table name: database.table_name
            format!("{}.{}", rel_schema.database, rel_schema.table_name)
        }
        Err(_) => {
            // NO FALLBACK - log error and return marker that will fail in ClickHouse
            log::error!(
                "❌ SCHEMA ERROR: Relationship type '{}' not found in schema. For polymorphic relationships with multiple tables, use get_rel_schema_with_nodes() to specify node types.",
                rel_type
            );
            format!("ERROR_SCHEMA_MISSING_{}", rel_type)
        }
    }
}

/// Convert a relationship type to its corresponding table name
/// DEPRECATED: Use rel_type_to_table_name_with_schema instead
pub fn rel_type_to_table_name(rel_type: &str) -> String {
    // Get the table name from the schema
    if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
        if let Ok(schemas) = schemas_lock.try_read() {
            if let Some(schema) = schemas.get("default") {
                return rel_type_to_table_name_with_schema(rel_type, schema);
            }
        }
    }

    // NO FALLBACK - log error and return marker
    log::error!("❌ SCHEMA ERROR: GLOBAL_SCHEMAS not initialized or 'default' schema not found. Cannot resolve relationship type '{}' without schema.", rel_type);
    format!("ERROR_SCHEMA_NOT_INITIALIZED_{}", rel_type)
}

/// Convert multiple relationship types to table names
pub fn rel_types_to_table_names(rel_types: &[String]) -> Vec<String> {
    rel_types
        .iter()
        .map(|rt| rel_type_to_table_name(rt))
        .collect()
}

/// Extract relationship columns from a table name using provided schema
pub fn extract_relationship_columns_from_table_with_schema(
    table_name: &str,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> RelationshipColumns {
    // Extract just the table name without database prefix for matching
    let table_only = table_name.split('.').last().unwrap_or(table_name);

    // Find relationship schema by table name
    for (_key, rel_schema) in schema.get_relationships_schemas() {
        // Match both with full name (db.table) or just table name
        if rel_schema.table_name == table_name
            || rel_schema.table_name == table_only
            || table_name.ends_with(&format!(".{}", rel_schema.table_name))
        {
            return RelationshipColumns {
                from_id: rel_schema.from_id.clone(),
                to_id: rel_schema.to_id.clone(),
            };
        }
    }

    // NO FALLBACK - log error and return generic columns that will cause SQL error
    log::error!("\u{274c} SCHEMA ERROR: Relationship table '{}' not found in schema. Using generic from_id/to_id columns which will likely fail.", table_name);
    RelationshipColumns {
        from_id: "from_id".to_string(),
        to_id: "to_id".to_string(),
    }
}

/// Extract relationship columns from a table name
/// DEPRECATED: Use extract_relationship_columns_from_table_with_schema instead
pub fn extract_relationship_columns_from_table(table_name: &str) -> RelationshipColumns {
    // Get columns from schema - this should be the single source of truth
    if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
        if let Ok(schemas) = schemas_lock.try_read() {
            if let Some(schema) = schemas.get("default") {
                return extract_relationship_columns_from_table_with_schema(table_name, schema);
            }
        }
    }

    // NO FALLBACK - log error and use generic columns
    log::error!("❌ SCHEMA ERROR: GLOBAL_SCHEMAS not initialized. Using generic from_id/to_id for table '{}'.", table_name);
    RelationshipColumns {
        from_id: "from_id".to_string(),
        to_id: "to_id".to_string(),
    }
}

/// Extract relationship columns from a LogicalPlan
pub fn extract_relationship_columns(plan: &LogicalPlan) -> Option<RelationshipColumns> {
    match plan {
        LogicalPlan::ViewScan(view_scan) => {
            // Check if ViewScan already has relationship columns configured
            if let (Some(from_col), Some(to_col)) = (&view_scan.from_id, &view_scan.to_id) {
                Some(RelationshipColumns {
                    from_id: from_col.clone(),
                    to_id: to_col.clone(),
                })
            } else {
                // Fallback to table-based lookup
                Some(extract_relationship_columns_from_table(
                    &view_scan.source_table,
                ))
            }
        }
        LogicalPlan::Cte(cte) => extract_relationship_columns(&cte.input),
        LogicalPlan::GraphRel(rel) => extract_relationship_columns(&rel.center),
        LogicalPlan::Filter(filter) => extract_relationship_columns(&filter.input),
        LogicalPlan::Projection(proj) => extract_relationship_columns(&proj.input),
        _ => None,
    }
}

/// Extract ID column from a LogicalPlan
fn extract_id_column(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::ViewScan(view_scan) => Some(view_scan.id_column.clone()),
        LogicalPlan::GraphNode(node) => extract_id_column(&node.input),
        LogicalPlan::Filter(filter) => extract_id_column(&filter.input),
        LogicalPlan::Projection(proj) => extract_id_column(&proj.input),
        _ => None,
    }
}

/// Get ID column for a table using provided schema
pub fn table_to_id_column_with_schema(
    table: &str,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> Result<String, String> {
    // Find node schema by table name
    // Handle both fully qualified (database.table) and simple (table) names
    for node_schema in schema.get_nodes_schemas().values() {
        let fully_qualified = format!("{}.{}", node_schema.database, node_schema.table_name);
        if node_schema.table_name == table || fully_qualified == table {
            return Ok(node_schema
                .node_id
                .columns()
                .first()
                .ok_or_else(|| format!("Node schema for table '{}' has no ID columns defined", table))?
                .to_string());
        }
    }

    // Node table not found in schema - this is an error
    Err(format!("Node table '{}' not found in schema", table))
}

/// Get ID column for a table
/// DEPRECATED: Use table_to_id_column_with_schema instead
pub fn table_to_id_column(table: &str) -> String {
    // Get the ID column from the schema
    if let Some(schemas_lock) = crate::server::GLOBAL_SCHEMAS.get() {
        if let Ok(schemas) = schemas_lock.try_read() {
            if let Some(schema) = schemas.get("default") {
                match table_to_id_column_with_schema(table, schema) {
                    Ok(col) => return col,
                    Err(e) => {
                        log::error!("❌ SCHEMA ERROR: {}", e);
                        return "id".to_string();
                    }
                }
            }
        }
    }

    // NO FALLBACK - log error and use generic 'id'
    log::error!("❌ SCHEMA ERROR: GLOBAL_SCHEMAS not initialized. Using generic 'id' column for table '{}'.", table);
    "id".to_string()
}

/// Get ID column for a label
fn table_to_id_column_for_label(label: &str) -> String {
    table_to_id_column(&label_to_table_name(label))
}

/// Get relationship columns from schema
fn get_relationship_columns_from_schema(rel_type: &str) -> Option<(String, String)> {
    let table = rel_type_to_table_name(rel_type);
    let cols = extract_relationship_columns_from_table(&table);
    Some((cols.from_id, cols.to_id))
}

/// Get relationship columns by table name
fn get_relationship_columns_by_table(table_name: &str) -> Option<(String, String)> {
    let cols = extract_relationship_columns_from_table(table_name);
    Some((cols.from_id, cols.to_id))
}

/// Get node info from schema
fn get_node_info_from_schema(node_label: &str) -> Option<(String, String)> {
    let table = label_to_table_name(node_label);
    let id_col = table_to_id_column(&table);
    Some((table, id_col))
}

/// Apply property mapping to an expression
fn apply_property_mapping_to_expr(expr: &mut RenderExpr, plan: &LogicalPlan) {
    match expr {
        RenderExpr::PropertyAccessExp(prop) => {
            // Get the node label for this table alias
            if let Some(node_label) = get_node_label_for_alias(&prop.table_alias.0, plan) {
                // Map the property to the correct column
                let mapped_column =
                    map_property_to_column_with_schema(&prop.column.raw(), &node_label)
                        .unwrap_or_else(|_| prop.column.raw().to_string());
                prop.column = PropertyValue::Column(mapped_column);
            }
        }
        RenderExpr::Column(col) => {
            // Check if this column name is actually an alias
            if let Some(node_label) = get_node_label_for_alias(&col.raw(), plan) {
                // Convert Column(alias) to PropertyAccess(alias, "id")
                let id_column = table_to_id_column(&label_to_table_name(&node_label));
                *expr = RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: super::render_expr::TableAlias(col.raw().to_string()),
                    column: PropertyValue::Column(id_column),
                });
            }
        }
        RenderExpr::TableAlias(alias) => {
            // For denormalized nodes, convert TableAlias to PropertyAccess with the ID column
            // This is especially important for GROUP BY expressions
            if let Some((rel_alias, id_column)) = get_denormalized_node_id_reference(&alias.0, plan)
            {
                *expr = RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: super::render_expr::TableAlias(rel_alias),
                    column: PropertyValue::Column(id_column),
                });
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
        LogicalPlan::GraphRel(rel) => get_node_label_for_alias(alias, &rel.left)
            .or_else(|| get_node_label_for_alias(alias, &rel.center))
            .or_else(|| get_node_label_for_alias(alias, &rel.right)),
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

/// For denormalized schemas: get the relationship alias and ID column for a node alias
/// Returns (rel_alias, id_column) if the node is denormalized, None otherwise
fn get_denormalized_node_id_reference(alias: &str, plan: &LogicalPlan) -> Option<(String, String)> {
    match plan {
        LogicalPlan::GraphRel(rel) => {
            // Check if this node alias matches left or right connection
            if let LogicalPlan::ViewScan(scan) = rel.center.as_ref() {
                // Check if node is the "from" node (left_connection)
                if alias == rel.left_connection {
                    if let Some(from_id) = &scan.from_id {
                        return Some((rel.alias.clone(), from_id.clone()));
                    }
                }
                // Check if node is the "to" node (right_connection)
                if alias == rel.right_connection {
                    if let Some(to_id) = &scan.to_id {
                        return Some((rel.alias.clone(), to_id.clone()));
                    }
                }
            }

            // Recursively check left and right branches
            // Check right branch first (more recent relationships take precedence for multi-hop)
            if let Some(result) = get_denormalized_node_id_reference(alias, &rel.right) {
                return Some(result);
            }
            if let Some(result) = get_denormalized_node_id_reference(alias, &rel.left) {
                return Some(result);
            }
            None
        }
        LogicalPlan::GraphNode(node) => {
            // Check if this is a denormalized node
            if node.is_denormalized && node.alias == alias {
                // For standalone denormalized nodes, check their input ViewScan
                if let LogicalPlan::ViewScan(scan) = node.input.as_ref() {
                    if let Some(from_id) = &scan.from_id {
                        // Use a placeholder alias since standalone nodes don't have a rel alias
                        return Some((alias.to_string(), from_id.clone()));
                    }
                }
            }
            get_denormalized_node_id_reference(alias, &node.input)
        }
        LogicalPlan::Filter(filter) => get_denormalized_node_id_reference(alias, &filter.input),
        LogicalPlan::Projection(proj) => get_denormalized_node_id_reference(alias, &proj.input),
        LogicalPlan::GraphJoins(joins) => get_denormalized_node_id_reference(alias, &joins.input),
        LogicalPlan::OrderBy(order_by) => {
            get_denormalized_node_id_reference(alias, &order_by.input)
        }
        LogicalPlan::Skip(skip) => get_denormalized_node_id_reference(alias, &skip.input),
        LogicalPlan::Limit(limit) => get_denormalized_node_id_reference(alias, &limit.input),
        LogicalPlan::GroupBy(group_by) => {
            get_denormalized_node_id_reference(alias, &group_by.input)
        }
        LogicalPlan::Cte(cte) => get_denormalized_node_id_reference(alias, &cte.input),
        LogicalPlan::CartesianProduct(cp) => get_denormalized_node_id_reference(alias, &cp.left)
            .or_else(|| get_denormalized_node_id_reference(alias, &cp.right)),
        _ => None,
    }
}

/// Extract CTEs with context - the main CTE extraction function
pub fn extract_ctes_with_context(
    plan: &LogicalPlan,
    last_node_alias: &str,
    context: &mut super::cte_generation::CteGenerationContext,
    schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> RenderPlanBuilderResult<Vec<Cte>> {
    // Debug: Log the plan type being processed
    let plan_type = match plan {
        LogicalPlan::Empty => "Empty",
        LogicalPlan::ViewScan(_) => "ViewScan",
        LogicalPlan::GraphNode(_) => "GraphNode",
        LogicalPlan::GraphRel(_) => "GraphRel",
        LogicalPlan::Filter(_) => "Filter",
        LogicalPlan::Projection(_) => "Projection",
        LogicalPlan::GraphJoins(_) => "GraphJoins",
        LogicalPlan::CartesianProduct(_) => "CartesianProduct",
        LogicalPlan::WithClause(_) => "WithClause",
        _ => "Other",
    };
    println!("DEBUG extract_ctes_with_context: Processing {} node", plan_type);
    
    match plan {
        LogicalPlan::Empty => Ok(vec![]),
        LogicalPlan::ViewScan(view_scan) => {
            // Check if this is a relationship ViewScan (has from_id/to_id)
            if let (Some(from_col), Some(to_col)) = (&view_scan.from_id, &view_scan.to_id) {
                // This is a relationship ViewScan - create a CTE that selects the relationship columns
                let cte_name = format!(
                    "rel_{}",
                    view_scan.source_table.replace([' ', '-', '_'], "")
                );
                let sql = format!(
                    "SELECT {}, {} FROM {}",
                    from_col, to_col, view_scan.source_table
                );
                let formatted_sql = format!("{} AS (\n{}\n)", cte_name, sql);

                Ok(vec![Cte::new(
                    cte_name,
                    super::CteContent::RawSql(formatted_sql),
                    false, // is_recursive
                )])
            } else {
                // This is a node ViewScan - no CTE needed
                Ok(vec![])
            }
        }
        LogicalPlan::GraphNode(graph_node) => {
            // Skip CTE creation for denormalized nodes - their properties are on the relationship table
            if graph_node.is_denormalized {
                log::debug!(
                    "Skipping CTE for denormalized node '{}' (properties stored on relationship table)",
                    graph_node.alias
                );
                return Ok(vec![]);
            }
            extract_ctes_with_context(&graph_node.input, last_node_alias, context, schema)
        }
        LogicalPlan::GraphRel(graph_rel) => {
            // Handle variable-length paths with context
            if let Some(spec) = &graph_rel.variable_length {
                log::debug!("🔧 VLP: Entering variable-length path handling");
                // Extract actual table names directly from ViewScan - with fallback to label lookup
                let left_plan_desc = match graph_rel.left.as_ref() {
                    LogicalPlan::Empty => "Empty".to_string(),
                    LogicalPlan::ViewScan(_) => "ViewScan".to_string(),
                    LogicalPlan::GraphNode(n) => format!("GraphNode({})", n.alias),
                    LogicalPlan::GraphRel(_) => "GraphRel".to_string(),
                    LogicalPlan::Filter(_) => "Filter".to_string(),
                    LogicalPlan::Projection(_) => "Projection".to_string(),
                    _ => "Other".to_string()
                };
                log::info!("🔍 VLP: Left plan = {}", left_plan_desc);
                // 🔧 PARAMETERIZED VIEW FIX: Use extract_parameterized_table_name for parameterized view support
                let start_table = extract_parameterized_table_name(&graph_rel.left)
                    .ok_or_else(|| RenderBuildError::MissingTableInfo("start node in VLP".to_string()))?;
                
                // 🎯 CHECK: Is this multi-type VLP? (end node has unknown type)
                // If so, end_table will be determined by schema expansion, not from the plan
                let rel_types: Vec<String> = graph_rel.labels.clone().unwrap_or_default();
                let is_multi_type_vlp = should_use_join_expansion(&graph_rel, &rel_types, schema);
                
                let end_table = if is_multi_type_vlp {
                    // For multi-type VLP, end_table isn't in the plan (it's polymorphic)
                    // We'll determine end types from schema later
                    log::info!("🎯 VLP: Multi-type detected, end_table will be determined from schema");
                    None
                } else {
                    // Regular VLP: end_table must be in the plan
                    // 🔧 PARAMETERIZED VIEW FIX: Use extract_parameterized_table_name for parameterized view support
                    Some(extract_parameterized_table_name(&graph_rel.right)
                        .ok_or_else(|| RenderBuildError::MissingTableInfo("end node in VLP".to_string()))?)
                };

                // Also extract labels for filter categorization and property extraction
                // These are optional - not all nodes have labels (e.g., CTEs)
                // ✅ FIX: Use schema-aware label extraction to support multi-schema queries
                let start_label = extract_node_label_from_viewscan_with_schema(&graph_rel.left, schema).unwrap_or_default();
                let end_label = extract_node_label_from_viewscan_with_schema(&graph_rel.right, schema).unwrap_or_default();

                // 🔧 PARAMETERIZED VIEW FIX: Get rel_table with parameterized view syntax if applicable
                // First try to extract parameterized table from ViewScan, fallback to schema lookup
                let center_plan_desc = match graph_rel.center.as_ref() {
                    LogicalPlan::Empty => "Empty".to_string(),
                    LogicalPlan::ViewScan(vs) => format!("ViewScan({})", vs.source_table),
                    LogicalPlan::GraphNode(n) => format!("GraphNode({})", n.alias),
                    LogicalPlan::GraphRel(_) => "GraphRel".to_string(),
                    LogicalPlan::Filter(_) => "Filter".to_string(),
                    LogicalPlan::Projection(_) => "Projection".to_string(),
                    _ => "Other".to_string()
                };
                log::info!("🔍 VLP: Center plan = {}", center_plan_desc);
                
                let rel_table = match graph_rel.center.as_ref() {
                    LogicalPlan::ViewScan(_) => {
                        // Use extract_parameterized_rel_table for parameterized view support
                        let result = extract_parameterized_rel_table(graph_rel.center.as_ref());
                        log::info!("🔍 VLP: extract_parameterized_rel_table returned: {:?}", result);
                        result.unwrap_or_else(|| {
                                log::warn!("Failed to extract parameterized rel table from ViewScan");
                                "unknown_rel_table".to_string()
                            })
                    }
                    _ => {
                        // Schema-based lookup with node types for polymorphic relationships
                        let rel_type = if let Some(labels) = &graph_rel.labels {
                            labels.first().unwrap_or(&graph_rel.alias)
                        } else {
                            &graph_rel.alias
                        };
                        
                        // For VLP with different start/end labels (e.g., Message→Post),
                        // the recursive traversal should use start→start relationship (Message→Message)
                        // Only the initial base case needs start→end
                        let (lookup_from, lookup_to) = if !start_label.is_empty() && !end_label.is_empty() && start_label != end_label {
                            // Different labels: use start→start for recursive traversal
                            log::info!("🔍 VLP with different labels: {}→{}. Using {}→{} for recursive traversal", 
                                start_label, end_label, start_label, start_label);
                            (Some(start_label.as_str()), Some(start_label.as_str()))
                        } else {
                            // Same label or missing: use as-is
                            (Some(start_label.as_str()), Some(end_label.as_str()))
                        };
                        
                        // 🔧 PARAMETERIZED VIEW FIX: Extract view_parameter_values from node ViewScans
                        // The node ViewScans have the parameter values; use them for the relationship table too
                        let view_params = extract_view_parameter_values(&graph_rel.left)
                            .or_else(|| extract_view_parameter_values(&graph_rel.right))
                            .unwrap_or_default();
                        
                        // Use schema lookup with node types and parameterized view support
                        if let Some(schema) = context.schema() {
                            if !view_params.is_empty() {
                                log::info!("🔧 VLP: Using parameterized view lookup with params: {:?}", view_params);
                                rel_type_to_table_name_with_nodes_and_params(
                                    rel_type,
                                    lookup_from,
                                    lookup_to,
                                    schema,
                                    &view_params
                                )
                            } else {
                                rel_type_to_table_name_with_nodes(
                                    rel_type,
                                    lookup_from,
                                    lookup_to,
                                    schema
                                )
                            }
                        } else {
                            log::error!("❌ SCHEMA ERROR: Schema context required for relationship table lookup");
                            format!("ERROR_SCHEMA_CONTEXT_REQUIRED_{}", rel_type)
                        }
                    }
                };

                // For relationship column lookup, we need the plain table name (without parameters or backticks)
                // Extract plain table name for schema lookups:
                // 1. Remove parameterized suffix: `db.table`(param = 'value') → `db.table`
                // 2. Remove backticks: `db.table` → db.table
                let rel_table_plain = {
                    let without_params = if rel_table.contains('(') {
                        rel_table.split('(').next().unwrap_or(&rel_table).to_string()
                    } else {
                        rel_table.clone()
                    };
                    // Remove backticks that may be present from parameterized view syntax
                    without_params.trim_matches('`').to_string()
                };

                // Extract relationship columns from schema using the plain table name
                log::debug!("🔧 VLP: Extract rel columns for table: {} (plain: {})", rel_table, rel_table_plain);
                // Use schema lookup for the relationship table columns
                let rel_cols = extract_relationship_columns_from_table_with_schema(&rel_table_plain, schema);
                let from_col = rel_cols.from_id;
                let to_col = rel_cols.to_id;
                log::debug!("🔧 VLP: Final columns: from_col='{}', to_col='{}' for table '{}'", from_col, to_col, rel_table);

                // ⚠️ CRITICAL: Node ID Column Selection (Multi-Schema Support)
                // ========================================================
                // ClickGraph supports TWO fundamentally different schema patterns:
                //
                // 1. TRADITIONAL SCHEMA (separate node & edge tables):
                //    - Node table exists: users, posts, etc.
                //    - node_schema.node_id points to PHYSICAL column in node table
                //    - Example: User.node_id="user_id" → users.user_id
                //    - VLP Strategy: Use node_schema.node_id.column()
                //
                // 2. DENORMALIZED SCHEMA (virtual nodes in edge table):
                //    - Node table is VIRTUAL (points to edge table)
                //    - node_schema.node_id is LOGICAL property name
                //    - Physical ID is in relationship columns (from_id/to_id)
                //    - Example: Airport.node_id="code" but physical is flights.Origin
                //    - VLP Strategy: Use relationship columns (from_col/to_col)
                //
                // 🚨 BREAKING HISTORY:
                // - Dec 22, 2025: Changed to use node_schema.node_id without checking is_denormalized
                // - Result: Denormalized VLP broke (3 tests marked xfail)
                // - Dec 25, 2025: Fixed by checking is_denormalized flag
                //
                // 🧪 TESTING REQUIREMENT:
                // ANY change to this logic MUST test BOTH schema types:
                // - tests/integration/test_variable_paths.py (traditional)
                // - tests/integration/test_denormalized_edges.py::TestDenormalizedVariableLengthPaths (denormalized)
                //
                // See: docs/development/schema-testing-requirements.md
                let start_id_col = if !start_label.is_empty() {
                    if let Ok(node_schema) = schema.get_node_schema(&start_label) {
                        if node_schema.is_denormalized {
                            // For denormalized nodes, use relationship column
                            from_col.clone()
                        } else {
                            // For traditional nodes, use node schema's node_id
                            node_schema.node_id.column().to_string()
                        }
                    } else {
                        // Fallback: use relationship's from_id
                        log::warn!("⚠️ VLP: Could not find node schema for '{}', using relationship from_id '{}'", start_label, from_col);
                        from_col.clone()
                    }
                } else {
                    // No label available, use relationship columns
                    from_col.clone()
                };

                let end_id_col = if !end_label.is_empty() {
                    if let Ok(node_schema) = schema.get_node_schema(&end_label) {
                        if node_schema.is_denormalized {
                            // For denormalized nodes, use relationship column
                            to_col.clone()
                        } else {
                            // For traditional nodes, use node schema's node_id
                            node_schema.node_id.column().to_string()
                        }
                    } else {
                        // Fallback: use relationship's to_id
                        log::warn!("⚠️ VLP: Could not find node schema for '{}', using relationship to_id '{}'", end_label, to_col);
                        to_col.clone()
                    }
                } else {
                    // No label available, use relationship columns
                    to_col.clone()
                };
                
                log::debug!("🔧 VLP: Node ID columns: start_id_col='{}', end_id_col='{}'", start_id_col, end_id_col);

                // Define aliases for traversal
                // Note: GraphRel.left_connection and right_connection are ALREADY swapped based on direction
                // in match_clause.rs (lines 1088-1092), so we always use them directly:
                // - left_connection = traversal start node alias
                // - right_connection = traversal end node alias
                let start_alias = graph_rel.left_connection.clone();
                let end_alias = graph_rel.right_connection.clone();
                // Relationship alias for property filters (e.g., WHERE r.property = value)
                let rel_alias = graph_rel.alias.clone();

                // 🔧 HOLISTIC FIX: Early detection of FK-edge pattern for proper alias mapping
                // In FK-edge patterns, relationship properties are on the start_node table (not a separate rel table)
                let is_fk_edge_early = if let Some(labels) = &graph_rel.labels {
                    if let Some(first_label) = labels.first() {
                        schema.get_rel_schema(first_label)
                            .map(|rel_schema| rel_schema.is_fk_edge)
                            .unwrap_or(false)
                    } else {
                        false
                    }
                } else {
                    false
                };
                
                if is_fk_edge_early {
                    log::info!("🔧 VLP: Detected FK-edge pattern early - relationship properties map to start_node alias");
                }

                // Extract and categorize filters for variable-length paths from GraphRel.where_predicate
                let (mut start_filters_sql, mut end_filters_sql, rel_filters_sql, categorized_filters_opt) =
                    if let Some(where_predicate) = &graph_rel.where_predicate {
                        log::info!("🔍 GraphRel has where_predicate: {:?}", where_predicate);
                        // Convert LogicalExpr to RenderExpr
                        let mut render_expr = RenderExpr::try_from(where_predicate.clone())
                            .map_err(|e| {
                                RenderBuildError::UnsupportedFeature(format!(
                                    "Failed to convert LogicalExpr to RenderExpr: {}",
                                    e
                                ))
                            })?;

                        // Apply property mapping to the filter expression before categorization
                        apply_property_mapping_to_expr(
                            &mut render_expr,
                            &LogicalPlan::GraphRel(graph_rel.clone()),
                        );

                        // Categorize filters - now passing actual rel_alias for relationship property filtering!
                        let categorized = categorize_filters(
                            Some(&render_expr),
                            &start_alias,
                            &end_alias,
                            &rel_alias, // ✅ FIXED: Pass actual relationship alias to capture relationship filters
                        );

                        // Create alias mapping for node aliases
                        let alias_mapping = [
                            (start_alias.clone(), "start_node".to_string()),
                            (end_alias.clone(), "end_node".to_string()),
                        ];

                        // 🔧 HOLISTIC FIX: Create alias mapping for relationship based on schema pattern
                        // - Standard pattern (3-way join): Maps to "rel" alias (separate edge table)
                        // - FK-edge pattern (2-way join): Maps to "start_node" alias (edge IS start node table)
                        let rel_target_alias = if is_fk_edge_early {
                            log::info!("🔧 VLP FK-edge: Mapping relationship properties to 'start_node' (no separate rel table)");
                            "start_node".to_string()
                        } else {
                            "rel".to_string()
                        };
                        let rel_alias_mapping = [
                            (rel_alias.clone(), rel_target_alias),
                        ];

                        let start_sql = categorized
                            .start_node_filters
                            .as_ref()
                            .map(|expr| render_expr_to_sql_string(expr, &alias_mapping));
                        let end_sql = categorized
                            .end_node_filters
                            .as_ref()
                            .map(|expr| render_expr_to_sql_string(expr, &alias_mapping));
                        // ✅ NEW: Convert relationship filters to SQL
                        let rel_sql = categorized
                            .relationship_filters
                            .as_ref()
                            .map(|expr| render_expr_to_sql_string(expr, &rel_alias_mapping));

                        // For variable-length queries (not shortest path), store end filters in context for outer query
                        if graph_rel.shortest_path_mode.is_none() {
                            if let Some(end_filter_expr) = &categorized.end_node_filters {
                                // 🆕 IMMUTABLE PATTERN: Update context immutably
                                *context = context
                                    .clone()
                                    .with_end_filters_for_outer_query(end_filter_expr.clone());
                            }
                        }

                        (start_sql, end_sql, rel_sql, Some(categorized))
                    } else {
                        (None, None, None, None)
                    };

                // 🔧 BOUND NODE FIX: Extract filters from bound nodes (Filter → GraphNode)
                // For queries like: MATCH (p1:Person {id: 1}), (p2:Person {id: 2}), path = shortestPath((p1)-[:KNOWS*]-(p2))
                // The {id: 1} and {id: 2} filters are in Filter nodes wrapping the GraphNodes, not in where_predicate
                if graph_rel.shortest_path_mode.is_some() {
                    log::info!("🔍 shortestPath: Checking for bound node filters...");
                    log::info!("  Start alias: {}, End alias: {}", start_alias, end_alias);
                    log::info!("  Current start_filters_sql: {:?}", start_filters_sql);
                    log::info!("  Current end_filters_sql: {:?}", end_filters_sql);
                    
                    // Extract start node filter (from left side)
                    if let Some(bound_start_filter) = extract_bound_node_filter(&graph_rel.left, &start_alias, "start_node") {
                        log::info!("🔧 Adding bound start node filter: {}", bound_start_filter);
                        start_filters_sql = Some(match start_filters_sql {
                            Some(existing) => format!("({}) AND ({})", existing, bound_start_filter),
                            None => bound_start_filter,
                        });
                    } else {
                        log::info!("⚠️  No bound start node filter found");
                    }
                    
                    // Extract end node filter (from right side)
                    if let Some(bound_end_filter) = extract_bound_node_filter(&graph_rel.right, &end_alias, "end_node") {
                        log::info!("🔧 Adding bound end node filter: {}", bound_end_filter);
                        end_filters_sql = Some(match end_filters_sql {
                            Some(existing) => format!("({}) AND ({})", existing, bound_end_filter),
                            None => bound_end_filter,
                        });
                    } else {
                        log::info!("⚠️  No bound end node filter found");
                    }
                    
                    log::info!("  Final start_filters_sql: {:?}", start_filters_sql);
                    log::info!("  Final end_filters_sql: {:?}", end_filters_sql);
                }

                // Extract properties from filter expressions for shortest path queries
                // Even in SQL_ONLY mode, we need properties that appear in filters
                let filter_properties = if graph_rel.shortest_path_mode.is_some() {
                    use crate::render_plan::cte_generation::extract_properties_from_filter;

                    let mut props = Vec::new();

                    if let Some(categorized) = categorized_filters_opt {
                        // Extract from start filters
                        if let Some(ref filter_expr) = categorized.start_node_filters {
                            let start_props = extract_properties_from_filter(
                                filter_expr,
                                &start_alias,
                                &start_label,
                            );
                            props.extend(start_props);
                        }

                        // Extract from end filters
                        if let Some(ref filter_expr) = categorized.end_node_filters {
                            let end_props =
                                extract_properties_from_filter(filter_expr, &end_alias, &end_label);
                            props.extend(end_props);
                        }
                    }

                    props
                } else {
                    vec![]
                };

                // Generate CTE with filters
                // For shortest path queries, always use recursive CTE (even for exact hops)
                // because we need proper filtering and shortest path selection logic

                // 🎯 DECISION POINT: CTE or inline JOINs?
                // BUT FIRST: Check if this is multi-type VLP (requires UNION ALL, not chained JOINs)
                let rel_types: Vec<String> = graph_rel.labels.clone().unwrap_or_default();
                let is_multi_type = should_use_join_expansion(&graph_rel, &rel_types, schema);
                
                let use_chained_join =
                    spec.exact_hop_count().is_some() 
                    && graph_rel.shortest_path_mode.is_none()
                    && !is_multi_type;  // Don't use chained JOINs for multi-type VLP

                if use_chained_join {
                    // 🚀 OPTIMIZATION: Fixed-length, non-shortest-path → NO CTE!
                    // Generate inline JOINs instead of recursive CTE
                    let exact_hops = spec.exact_hop_count().unwrap();
                    println!(
                        "CTE BRANCH: Fixed-length pattern (*{}) detected - generating inline JOINs",
                        exact_hops
                    );

                    // Build VlpContext with all necessary information
                    if let Some(vlp_ctx) = build_vlp_context(graph_rel) {
                        // Generate inline JOINs using expand_fixed_length_joins_with_context
                        let (from_table, from_alias, joins) =
                            expand_fixed_length_joins_with_context(&vlp_ctx);

                        // Store the generated JOINs in context for later retrieval
                        context.set_fixed_length_joins(
                            &vlp_ctx.start_alias,
                            &vlp_ctx.end_alias,
                            from_table,
                            from_alias,
                            joins,
                        );

                        println!(
                            "CTE BRANCH: Stored fixed-length JOINs for {}-{} pattern",
                            vlp_ctx.start_alias, vlp_ctx.end_alias
                        );
                    } else {
                        log::warn!(
                            "Failed to build VlpContext for fixed-length pattern - falling back to CTE"
                        );
                        // Fall through to CTE generation below
                    }

                    // Extract CTEs from child plans (if any)
                    let child_ctes =
                        extract_ctes_with_context(&graph_rel.right, last_node_alias, context, schema)?;

                    return Ok(child_ctes);
                } else {
                    // ✅ Truly variable-length or shortest path → Check if multi-type
                    println!("CTE BRANCH: Variable-length pattern detected");
                    log::info!("🔍 VLP: Variable-length or shortest path detected (not using chained JOINs)");
                    
                    // 🎯 CHECK FOR MULTI-TYPE VLP (Part 1D implementation)
                    let rel_types: Vec<String> = graph_rel.labels.clone().unwrap_or_default();
                    log::info!("🔍 VLP: rel_types={:?}", rel_types);
                    
                    let is_multi_type_check = should_use_join_expansion(&graph_rel, &rel_types, schema);
                    log::info!("🔍 VLP: should_use_join_expansion returned: {}", is_multi_type_check);
                    
                    if should_use_join_expansion(&graph_rel, &rel_types, schema) {
                        // Multi-type VLP: Use JOIN expansion with UNION ALL
                        log::info!("🎯 CTE: Using JOIN expansion for multi-type VLP");
                        
                        // Extract start labels from graph pattern
                        let start_labels = extract_node_labels(&graph_rel.left)
                            .unwrap_or_else(|| {
                                // Fallback: extract from ViewScan
                                vec![extract_node_label_from_viewscan_with_schema(&graph_rel.left, schema)
                                    .unwrap_or_else(|| "UnknownStartType".to_string())]
                            });
                        
                        // For multi-type VLP, we need ALL possible end types from the relationship schema
                        // The GraphNode label might only have one type (from type inference), 
                        // but the actual query could reach multiple types
                        let mut end_labels: Vec<String> = Vec::new();
                        
                        // First, try to get explicit labels from the graph pattern
                        if let Some(labels) = extract_node_labels(&graph_rel.right) {
                            end_labels = labels;
                        }
                        
                        // If only one label or no labels, collect all possible end types from relationships
                        if end_labels.len() <= 1 {
                            let mut possible_end_types = std::collections::HashSet::new();
                            for rel_type in &rel_types {
                                if let Ok(rel_schema) = schema.get_rel_schema(rel_type) {
                                    possible_end_types.insert(rel_schema.to_node.clone());
                                }
                            }
                            if possible_end_types.len() > 1 {
                                // Multiple possible end types - use all of them
                                end_labels = possible_end_types.into_iter().collect();
                                log::info!(
                                    "🎯 CTE: Expanded end_labels from relationships: {:?}",
                                    end_labels
                                );
                            } else if end_labels.is_empty() {
                                // Fallback: extract from ViewScan
                                end_labels = vec![extract_node_label_from_viewscan_with_schema(&graph_rel.right, schema)
                                    .unwrap_or_else(|| "UnknownEndType".to_string())];
                            }
                        }
                        
                        log::info!(
                            "🎯 CTE Multi-type VLP: start_labels={:?}, rel_types={:?}, end_labels={:?}",
                            start_labels, rel_types, end_labels
                        );
                        
                        // 🔧 PARAMETERIZED VIEW FIX: Extract view parameters from the graph pattern
                        // Try to get from left (start node) first, then try right (end node)
                        let view_parameter_values = extract_view_parameter_values(&graph_rel.left)
                            .or_else(|| extract_view_parameter_values(&graph_rel.right))
                            .unwrap_or_default();
                        log::debug!(
                            "🔧 Multi-type VLP: Extracted view_parameter_values: {:?}",
                            view_parameter_values
                        );
                        
                        // Create the generator
                        use crate::clickhouse_query_generator::MultiTypeVlpJoinGenerator;
                        
                        // For multi-type VLP, we use start_filters_sql and end_filters_sql directly
                        // The schema filters are handled differently in JOIN expansion
                        let generator = MultiTypeVlpJoinGenerator::new(
                            schema,
                            start_labels,
                            rel_types,
                            end_labels,
                            spec.clone(),
                            start_alias.clone(),
                            end_alias.clone(),
                            start_filters_sql.clone(),
                            end_filters_sql.clone(),
                            rel_filters_sql.clone(),
                            graph_rel.path_variable.clone(),
                            view_parameter_values,
                        );
                        
                        // TODO: Add property projections based on what's needed in RETURN clause
                        // For now, we'll generate without specific property projections
                        // Properties will be handled by the analyzer in Phase 5
                        
                        // Generate CTE name - use vlp_ prefix for proper detection
                        // The plan_builder.rs looks for CTEs starting with "vlp_" or "chained_path_"
                        let cte_name = format!("vlp_multi_type_{}_{}", start_alias, end_alias);
                        
                        // Generate SQL
                        match generator.generate_cte_sql(&cte_name) {
                            Ok(cte_sql) => {
                                log::info!("🎯 CTE Multi-type VLP SQL generated successfully");
                                log::debug!("Generated SQL:\n{}", cte_sql);
                                
                                // Create CTE wrapper with all required fields
                                let cte = Cte {
                                    cte_name: cte_name.clone(),
                                    content: CteContent::RawSql(cte_sql),
                                    is_recursive: false,  // Multi-type VLP uses UNION ALL, not recursive
                                    vlp_start_alias: Some("start_node".to_string()),
                                    vlp_end_alias: Some("end_node".to_string()),
                                    vlp_start_table: None,  // Will be filled by generator if needed
                                    vlp_end_table: None,
                                    vlp_cypher_start_alias: Some(start_alias.clone()),
                                    vlp_cypher_end_alias: Some(end_alias.clone()),
                                    vlp_start_id_col: None,
                                    vlp_end_id_col: None,
                                };
                                
                                // Extract CTEs from child plans
                                let mut child_ctes = extract_ctes_with_context(
                                    &graph_rel.right,
                                    last_node_alias,
                                    context,
                                    schema
                                )?;
                                child_ctes.push(cte);
                                
                                return Ok(child_ctes);
                            }
                            Err(e) => {
                                return Err(RenderBuildError::UnsupportedFeature(format!(
                                    "Failed to generate multi-type VLP SQL: {}. \
                                     This may indicate missing schema information or unsupported path combination.",
                                    e
                                )));
                            }
                        }
                    }
                    
                    // Single-type VLP: Use traditional recursive CTE
                    println!("CTE BRANCH: Single-type VLP - using recursive CTE");

                    // Check if nodes are denormalized (properties embedded in edge table)
                    let start_is_denormalized = match graph_rel.left.as_ref() {
                        LogicalPlan::GraphNode(node) => node.is_denormalized,
                        _ => false,
                    };
                    let end_is_denormalized = match graph_rel.right.as_ref() {
                        LogicalPlan::GraphNode(node) => node.is_denormalized,
                        _ => false,
                    };
                    let both_denormalized = start_is_denormalized && end_is_denormalized;
                    let is_mixed = start_is_denormalized != end_is_denormalized;

                    // 🎯 Extract schema filters from start and end nodes
                    // Schema filters are defined in YAML and should be applied to CTE base/recursive cases
                    let start_schema_filter =
                        extract_schema_filter_from_node(&graph_rel.left, "start_node");
                    let end_schema_filter =
                        extract_schema_filter_from_node(&graph_rel.right, "end_node");

                    // Combine user filters with schema filters using AND
                    let combined_start_filters = match (&start_filters_sql, &start_schema_filter) {
                        (Some(user), Some(schema)) => Some(format!("({}) AND ({})", user, schema)),
                        (Some(user), None) => Some(user.clone()),
                        (None, Some(schema)) => Some(schema.clone()),
                        (None, None) => None,
                    };

                    let combined_end_filters = match (&end_filters_sql, &end_schema_filter) {
                        (Some(user), Some(schema)) => Some(format!("({}) AND ({})", user, schema)),
                        (Some(user), None) => Some(user.clone()),
                        (None, Some(schema)) => Some(schema.clone()),
                        (None, None) => None,
                    };

                    if start_schema_filter.is_some() || end_schema_filter.is_some() {
                        log::info!(
                            "CTE: Applying schema filters - start: {:?}, end: {:?}",
                            start_schema_filter,
                            end_schema_filter
                        );
                    }

                    // Get edge_id from relationship schema if available
                    // Use the first relationship label to look up the schema
                    let (edge_id, type_column, from_label_column, to_label_column, is_fk_edge) =
                        if let Some(labels) = &graph_rel.labels {
                            if let Some(first_label) = labels.first() {
                                // Try to get relationship schema by label (not table name)
                                if let Ok(rel_schema) = schema.get_rel_schema(first_label) {
                                        (
                                            rel_schema.edge_id.clone(),
                                            rel_schema.type_column.clone(),
                                            rel_schema.from_label_column.clone(),
                                            rel_schema.to_label_column.clone(),
                                            rel_schema.is_fk_edge,
                                        )
                                    } else {
                                        (None, None, None, None, false)
                                    }
                                } else {
                                    (None, None, None, None, false)
                                }
                            } else {
                                (None, None, None, None, false)
                            };

                    if is_fk_edge {
                        log::debug!("CTE: Detected FK-edge pattern for relationship type");
                    }

                    // Choose generator based on denormalized status
                    let mut generator = if both_denormalized {
                        log::debug!("🔧 CTE: Using denormalized generator for variable-length path (both nodes virtual)");
                        log::debug!("🔧 CTE: rel_table={}, filter_properties count={}", rel_table, filter_properties.len());
                        
                        // For denormalized nodes, extract ALL properties from the node schema
                        // (not just filter properties, since properties come from the edge table)
                        let mut all_denorm_properties = filter_properties.clone();
                        
                        // Get node schema to extract all from_properties and to_properties
                        // Handle both "table" and "database.table" formats
                        let rel_table_name = rel_table.split('.').last().unwrap_or(&rel_table);
                        
                        if let Some(node_schema) = schema.get_nodes_schemas().values()
                            .find(|n| {
                                let schema_table = n.table_name.split('.').last().unwrap_or(&n.table_name);
                                schema_table == rel_table_name
                            }) {
                            
                            log::debug!("🔧 CTE: Found node schema for table {}", rel_table);
                            
                            // Add all from_node properties
                            if let Some(ref from_props) = node_schema.from_properties {
                                log::debug!("🔧 CTE: Adding {} from_node properties", from_props.len());
                                for (logical_prop, _physical_col) in from_props {
                                    if !all_denorm_properties.iter().any(|p| 
                                        p.cypher_alias == graph_rel.left_connection && p.alias == *logical_prop) {
                                        log::trace!("🔧 CTE: Adding from property: {}", logical_prop);
                                        all_denorm_properties.push(NodeProperty {
                                            cypher_alias: graph_rel.left_connection.clone(),
                                            column_name: logical_prop.clone(),
                                            alias: logical_prop.clone(),
                                        });
                                    }
                                }
                            }
                            
                            // Add all to_node properties
                            if let Some(ref to_props) = node_schema.to_properties {
                                log::debug!("🔧 CTE: Adding {} to_node properties", to_props.len());
                                for (logical_prop, _physical_col) in to_props {
                                    if !all_denorm_properties.iter().any(|p| 
                                        p.cypher_alias == graph_rel.right_connection && p.alias == *logical_prop) {
                                        log::trace!("🔧 CTE: Adding to property: {}", logical_prop);
                                        all_denorm_properties.push(NodeProperty {
                                            cypher_alias: graph_rel.right_connection.clone(),
                                            column_name: logical_prop.clone(),
                                            alias: logical_prop.clone(),
                                        });
                                    }
                                }
                            }
                        } else {
                            log::warn!("❌ CTE: No node schema found for table {}", rel_table);
                        }
                        
                        log::debug!("🔧 CTE: Final all_denorm_properties count: {}", all_denorm_properties.len());
                        
                        VariableLengthCteGenerator::new_denormalized(
                            schema,
                            spec.clone(),
                            &rel_table, // The only table - edge table
                            &from_col,  // From column
                            &to_col,    // To column
                            &graph_rel.left_connection,
                            &graph_rel.right_connection,
                            &rel_alias, // ✅ HOLISTIC FIX: Pass relationship Cypher alias
                            all_denorm_properties, // Pass all denormalized properties
                            graph_rel.shortest_path_mode.clone().map(|m| m.into()),
                            combined_start_filters.clone(),
                            // 🔒 Always pass end filters - schema filters apply to base tables
                            combined_end_filters.clone(),
                            rel_filters_sql.clone(), // ✅ HOLISTIC FIX: Pass relationship filters
                            graph_rel.path_variable.clone(),
                            graph_rel.labels.clone(),
                            edge_id,
                        )
                    } else if is_mixed {
                        log::debug!("CTE: Using mixed generator for variable-length path (start_denorm={}, end_denorm={})",
                                  start_is_denormalized, end_is_denormalized);
                        
                        // For single-type VLP, end_table must exist
                        let end_table_str = end_table.as_ref()
                            .ok_or_else(|| RenderBuildError::MissingTableInfo("end node in single-type VLP".to_string()))?;
                        
                        VariableLengthCteGenerator::new_mixed(
                            schema,
                            spec.clone(),
                            &start_table,
                            &start_id_col,
                            &rel_table,
                            &from_col,
                            &to_col,
                            end_table_str,
                            &end_id_col,
                            &graph_rel.left_connection,
                            &graph_rel.right_connection,
                            &rel_alias, // ✅ HOLISTIC FIX: Pass relationship Cypher alias
                            filter_properties.clone(),
                            graph_rel.shortest_path_mode.clone().map(|m| m.into()),
                            combined_start_filters.clone(),
                            // 🔒 Always pass end filters - schema filters apply to base tables
                            combined_end_filters.clone(),
                            rel_filters_sql.clone(), // ✅ HOLISTIC FIX: Pass relationship filters
                            graph_rel.path_variable.clone(),
                            graph_rel.labels.clone(),
                            edge_id,
                            start_is_denormalized,
                            end_is_denormalized,
                        )
                    } else {
                        // For single-type VLP, end_table must exist
                        let end_table_str = end_table.as_ref()
                            .ok_or_else(|| RenderBuildError::MissingTableInfo("end node in single-type VLP".to_string()))?;
                        
                        VariableLengthCteGenerator::new_with_fk_edge(
                            schema,
                            spec.clone(),
                            &start_table,
                            &start_id_col,
                            &rel_table,
                            &from_col,
                            &to_col,
                            end_table_str,
                            &end_id_col,
                            &graph_rel.left_connection,
                            &graph_rel.right_connection,
                            &rel_alias, // ✅ HOLISTIC FIX: Pass relationship Cypher alias
                            filter_properties, // Use filter properties
                            graph_rel.shortest_path_mode.clone().map(|m| m.into()),
                            combined_start_filters, // Start filters (user + schema)
                            // 🔒 Always pass end filters - schema filters apply to base tables
                            combined_end_filters,
                            rel_filters_sql, // ✅ HOLISTIC FIX: Pass relationship filters
                            graph_rel.path_variable.clone(),
                            graph_rel.labels.clone(),
                            edge_id,                   // Pass edge_id from schema
                            type_column.clone(),       // Polymorphic edge type discriminator
                            from_label_column,         // Polymorphic edge from label column
                            to_label_column.clone(),   // Polymorphic edge to label column
                            Some(start_label.clone()), // Expected from node label
                            Some(end_label.clone()),   // Expected to node label
                            is_fk_edge,                // FK-edge pattern flag
                        )
                    };

                    // For heterogeneous polymorphic paths (start_label != end_label with to_label_column),
                    // set intermediate node info to enable proper recursive traversal.
                    // The intermediate type is the same as start type (Group→Group recursion).
                    if to_label_column.is_some() && start_label != end_label {
                        log::info!(
                            "CTE: Setting intermediate node for heterogeneous polymorphic path"
                        );
                        log::info!("  - start_label: {}, end_label: {}", start_label, end_label);
                        log::info!(
                            "  - intermediate: table={}, id_col={}, label={}",
                            start_table,
                            start_id_col,
                            start_label
                        );
                        generator.set_intermediate_node(&start_table, &start_id_col, &start_label);
                    }

                    let var_len_cte = generator.generate_cte();

                    // Also extract CTEs from child plans
                    let mut child_ctes =
                        extract_ctes_with_context(&graph_rel.right, last_node_alias, context, schema)?;
                    child_ctes.push(var_len_cte);

                    return Ok(child_ctes);
                }
            }

            // Handle multiple relationship types for regular single-hop relationships
            let mut relationship_ctes = vec![];

            if let Some(labels) = &graph_rel.labels {
                crate::debug_print!(
                    "DEBUG cte_extraction: GraphRel labels: {:?} (len={})",
                    labels,
                    labels.len()
                );

                // Deduplicate labels to handle cases like [:FOLLOWS|FOLLOWS]
                let unique_labels: Vec<String> = {
                    let mut seen = std::collections::HashSet::new();
                    labels
                        .iter()
                        .filter(|l| seen.insert((*l).clone()))
                        .cloned()
                        .collect()
                };

                if unique_labels.len() > 1 {
                    // Multiple distinct relationship types: create a UNION CTE
                    // Use schema from context instead of deprecated global schema function
                    let rel_tables: Vec<String> = if let Some(schema) = context.schema() {
                        unique_labels
                            .iter()
                            .map(|label| {
                                if let Ok(rel_schema) = schema.get_rel_schema(label) {
                                    format!("{}.{}", rel_schema.database, rel_schema.table_name)
                                } else {
                                    log::error!("❌ SCHEMA ERROR: Relationship type '{}' not found in schema", label);
                                    format!("ERROR_SCHEMA_MISSING_{}", label)
                                }
                            })
                            .collect()
                    } else {
                        log::error!("❌ SCHEMA ERROR: No schema in context for relationship types {:?}", unique_labels);
                        unique_labels.iter().map(|label| format!("ERROR_SCHEMA_NOT_IN_CONTEXT_{}", label)).collect()
                    };
                    crate::debug_print!(
                        "DEBUG cte_extraction: Resolved tables for labels {:?}: {:?}",
                        unique_labels,
                        rel_tables
                    );

                    // Check if this is a polymorphic edge (all types map to same table with type_column)
                    let is_polymorphic = if let Some(schema) = context.schema() {
                        // Check if the first relationship type has a type_column (indicates polymorphic)
                        if let Ok(rel_schema) = schema.get_rel_schema(&unique_labels[0]) {
                            rel_schema.type_column.is_some()
                        } else {
                            false
                        }
                    } else {
                        false
                    };

                    let union_queries: Vec<String> = if is_polymorphic {
                        // Polymorphic edge: all types share the same table, need type filters
                        // Get schema info from context
                        if let Some(schema) = context.schema() {
                            if let Ok(rel_schema) = schema.get_rel_schema(&unique_labels[0]) {
                                let table_name =
                                    format!("{}.{}", rel_schema.database, rel_schema.table_name);
                                let from_col = &rel_schema.from_id;
                                let to_col = &rel_schema.to_id;
                                let type_col = rel_schema
                                    .type_column
                                    .as_ref()
                                    .expect("polymorphic edge must have type_column");

                                // For polymorphic edges, use a single query with IN clause
                                // This is more efficient than UNION of identical table scans
                                // Include type_column for relationship property access
                                let type_values: Vec<String> =
                                    unique_labels.iter().map(|l| format!("'{}'", l)).collect();
                                let type_in_clause = type_values.join(", ");

                                vec![format!(
                                    "SELECT {from_col} as from_node_id, {to_col} as to_node_id, {type_col} as interaction_type FROM {table_name} WHERE {type_col} IN ({type_in_clause})"
                                )]
                            } else {
                                // Fallback if schema lookup fails
                                rel_tables
                                    .iter()
                                    .map(|table| {
                                        let (from_col, to_col) =
                                            get_relationship_columns_by_table(table).unwrap_or((
                                                "from_node_id".to_string(),
                                                "to_node_id".to_string(),
                                            ));
                                        format!(
                                            "SELECT {} as from_node_id, {} as to_node_id FROM {}",
                                            from_col, to_col, table
                                        )
                                    })
                                    .collect()
                            }
                        } else {
                            // No schema in context, fallback
                            rel_tables
                                .iter()
                                .map(|table| {
                                    let (from_col, to_col) =
                                        get_relationship_columns_by_table(table).unwrap_or((
                                            "from_node_id".to_string(),
                                            "to_node_id".to_string(),
                                        ));
                                    format!(
                                        "SELECT {} as from_node_id, {} as to_node_id FROM {}",
                                        from_col, to_col, table
                                    )
                                })
                                .collect()
                        }
                    } else {
                        // Regular multiple relationship types: UNION of different tables
                        // Use schema to get the correct column names for each relationship type
                        if let Some(schema) = context.schema() {
                            unique_labels
                                .iter()
                                .zip(rel_tables.iter())
                                .map(|(label, table)| {
                                    if let Ok(rel_schema) = schema.get_rel_schema(label) {
                                        let from_col = &rel_schema.from_id;
                                        let to_col = &rel_schema.to_id;
                                        format!(
                                            "SELECT {} as from_node_id, {} as to_node_id FROM {}",
                                            from_col, to_col, table
                                        )
                                    } else {
                                        // Fallback if schema lookup fails
                                        format!(
                                            "SELECT from_id as from_node_id, to_id as to_node_id FROM {}",
                                            table
                                        )
                                    }
                                })
                                .collect()
                        } else {
                            // Fallback if no schema in context
                            rel_tables
                                .iter()
                                .map(|table| {
                                    let (from_col, to_col) = get_relationship_columns_by_table(table)
                                        .unwrap_or((
                                            "from_node_id".to_string(),
                                            "to_node_id".to_string(),
                                        ));
                                    format!(
                                        "SELECT {} as from_node_id, {} as to_node_id FROM {}",
                                        from_col, to_col, table
                                    )
                                })
                                .collect()
                        }
                    };

                    let union_sql = union_queries.join(" UNION ALL ");
                    let cte_name = format!(
                        "rel_{}_{}",
                        graph_rel.left_connection, graph_rel.right_connection
                    );

                    // Format as proper CTE: cte_name AS (union_sql)
                    let formatted_union_sql = format!("{} AS (\n{}\n)", cte_name, union_sql);

                    crate::debug_println!(
                        "DEBUG cte_extraction: Generated UNION CTE: {}",
                        cte_name
                    );

                    relationship_ctes.push(Cte::new(
                    cte_name.clone(),
                    super::CteContent::RawSql(formatted_union_sql),
                    false,
                ));
                } else {
                    crate::debug_println!(
                        "DEBUG cte_extraction: Single relationship type, no UNION needed"
                    );
                }
            } else {
                crate::debug_println!("DEBUG cte_extraction: No labels on GraphRel!");
            }

            // IMPORTANT: Recurse into left and right branches to collect CTEs from nested GraphRels
            // This is needed for multi-hop polymorphic patterns like (u)-[r1]->(m)-[r2]->(t)
            // where both r1 and r2 are wildcard edges needing their own CTEs
            let mut left_ctes =
                extract_ctes_with_context(&graph_rel.left, last_node_alias, context, schema)?;
            let mut right_ctes =
                extract_ctes_with_context(&graph_rel.right, last_node_alias, context, schema)?;

            // Combine all CTEs from left, right, and current relationship
            let mut all_ctes = left_ctes;
            all_ctes.append(&mut right_ctes);
            all_ctes.append(&mut relationship_ctes);

            Ok(all_ctes)
        }
        LogicalPlan::Filter(filter) => {
            // Store the filter in context so GraphRel nodes can access it
            log::trace!(
                "Filter node detected, storing filter predicate in context: {:?}",
                filter.predicate
            );

            // 🆕 IMMUTABLE PATTERN: Create new context with filter instead of mutating
            let filter_expr: RenderExpr = filter.predicate.clone().try_into()?;
            log::trace!("Converted to RenderExpr: {:?}", filter_expr);
            let new_context = context.clone().with_filter(filter_expr);

            // Extract CTEs with the new context
            let ctes = extract_ctes_with_context(
                &filter.input,
                last_node_alias,
                &mut new_context.clone(),
                schema,
            )?;

            // Merge end filters from the new context back to the original context
            *context = context.clone().merge_end_filters(&new_context);

            Ok(ctes)
        }
        LogicalPlan::Projection(projection) => {
            log::trace!(
                "Projection node detected, recursing into input type: {}",
                match &*projection.input {
                    LogicalPlan::Empty => "Empty",
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
                    LogicalPlan::Unwind(_) => "Unwind",
                    LogicalPlan::CartesianProduct(_) => "CartesianProduct",
                    LogicalPlan::WithClause(_) => "WithClause",
                }
            );
            extract_ctes_with_context(&projection.input, last_node_alias, context, schema)
        }
        LogicalPlan::GraphJoins(graph_joins) => {
            extract_ctes_with_context(&graph_joins.input, last_node_alias, context, schema)
        }
        LogicalPlan::GroupBy(group_by) => {
            log::info!("🔍 CTE extraction: Delegating from GroupBy to input plan");
            extract_ctes_with_context(&group_by.input, last_node_alias, context, schema)
        }
        LogicalPlan::OrderBy(order_by) => {
            extract_ctes_with_context(&order_by.input, last_node_alias, context, schema)
        }
        LogicalPlan::Skip(skip) => extract_ctes_with_context(&skip.input, last_node_alias, context, schema),
        LogicalPlan::Limit(limit) => {
            extract_ctes_with_context(&limit.input, last_node_alias, context, schema)
        }
        LogicalPlan::Cte(logical_cte) => {
            // Use schema from context if available, otherwise create empty schema for tests
            let schema = context.schema().cloned().unwrap_or_else(|| {
                use crate::graph_catalog::graph_schema::GraphSchema;
                GraphSchema::build(
                    1,
                    "test".to_string(),
                    std::collections::HashMap::new(),
                    std::collections::HashMap::new(),
                )
            });
            Ok(vec![Cte::new(
                    logical_cte.name.clone(),
                    super::CteContent::Structured(logical_cte.input.to_render_plan(&schema)?),
                    false,
                )])
        }
        LogicalPlan::Union(union) => {
            let mut ctes = vec![];
            for input_plan in union.inputs.iter() {
                ctes.append(&mut extract_ctes_with_context(
                    input_plan,
                    last_node_alias,
                    context,
                    schema,
                )?);
            }
            Ok(ctes)
        }
        LogicalPlan::PageRank(_) => Ok(vec![]),
        LogicalPlan::Unwind(u) => extract_ctes_with_context(&u.input, last_node_alias, context, schema),
        LogicalPlan::CartesianProduct(cp) => {
            println!("DEBUG CTE Extraction: Processing CartesianProduct");
            let mut ctes = extract_ctes_with_context(&cp.left, last_node_alias, context, schema)?;
            println!("DEBUG CTE Extraction: CartesianProduct left side returned {} CTEs", ctes.len());
            ctes.append(&mut extract_ctes_with_context(
                &cp.right,
                last_node_alias,
                context,
                schema,
            )?);
            println!("DEBUG CTE Extraction: CartesianProduct total {} CTEs", ctes.len());
            Ok(ctes)
        }
        LogicalPlan::WithClause(wc) => {
            println!("DEBUG CTE Extraction: Processing WithClause with {} exported aliases", wc.exported_aliases.len());
            // WITH clause should generate a CTE!
            // The CTE contains the SQL from the input plan with the WITH projection
            
            // First, extract any CTEs from the input
            let mut ctes = extract_ctes_with_context(&wc.input, last_node_alias, context, schema)?;
            
            // Generate CTE name using centralized utility (base name without counter)
            let cte_name = generate_cte_base_name(&wc.exported_aliases);
            
            log::info!("🔧 CTE Extraction: Generating CTE '{}' for WITH clause with {} exported aliases", 
                cte_name, wc.exported_aliases.len());
            
            // Build the CTE content by rendering the input plan as a RenderPlan
            // Get schema from context
            let schema = context.schema().ok_or(RenderBuildError::InvalidRenderPlan(
                "Cannot generate WITH CTE: No schema found in context".to_string()
            ))?;
            
            // CRITICAL: Expand collect(node) to groupArray(tuple(...)) BEFORE creating Projection
            // This ensures the CTE has the proper aggregation structure
            use crate::query_planner::logical_expr::LogicalExpr;
            
            // First pass: Check if we have any aggregations
            let has_aggregation_in_items = wc.items.iter().any(|item| 
                matches!(&item.expression, LogicalExpr::AggregateFnCall(_))
            );
            
            let expanded_items: Vec<_> = wc.items.iter().map(|item| {
                let expanded_expr = if let LogicalExpr::AggregateFnCall(ref agg) = item.expression {
                    if agg.name.to_lowercase() == "collect" && agg.args.len() == 1 {
                        if let LogicalExpr::TableAlias(ref alias) = agg.args[0] {
                            log::info!("🔧 CTE Extraction: Expanding collect({}) to groupArray(tuple(...))", alias.0);
                            
                            // Get properties for this alias from the input plan
                            // We need to construct a temporary PlanBuilder to access get_properties_with_table_alias
                            // For now, log and keep as-is (will be handled by plan_builder path)
                            // TODO: Refactor to access schema properties directly
                            log::warn!("⚠️  CTE Extraction path for collect() - need schema access to expand");
                            item.expression.clone()
                        } else {
                            item.expression.clone()
                        }
                    } else {
                        item.expression.clone()
                    }
                } else if has_aggregation_in_items {
                    // If we have aggregations in the items, wrap non-aggregate TableAlias with anyLast()
                    // for all non-ID columns (ID columns will be in GROUP BY)
                    if let LogicalExpr::TableAlias(ref alias) = item.expression {
                        // We'll wrap this later after we know which columns are IDs
                        item.expression.clone()
                    } else {
                        item.expression.clone()
                    }
                } else {
                    item.expression.clone()
                };
                
                crate::query_planner::logical_plan::ProjectionItem {
                    expression: expanded_expr,
                    col_alias: item.col_alias.clone(),
                }
            }).collect();
            
            // Detect if any items contain aggregation functions
            // If so, we need to wrap in GroupBy to generate proper SQL
            let has_aggregation = expanded_items.iter().any(|item| {
                use crate::query_planner::logical_expr::LogicalExpr;
                matches!(&item.expression, LogicalExpr::AggregateFnCall(_))
            });
            
            log::info!("🔧 CTE Extraction: has_aggregation={}", has_aggregation);
            
            // If we have aggregations, we need to:
            // 1. Wrap TableAlias non-ID columns with anyLast()
            // 2. Create GroupBy node with ID columns only
            // Use the same logic as build_chained_with_match_cte_plan (lines 1745-1900)
            
            let final_items = if has_aggregation {
                // Wrap non-ID columns of TableAlias with anyLast()
                expanded_items.into_iter().map(|mut item| {
                    use crate::query_planner::logical_expr::LogicalExpr;
                    
                    // Only wrap TableAlias, not aggregate functions
                    if let LogicalExpr::TableAlias(ref alias) = item.expression {
                        // Find the ID column for this alias
                        if let Ok(id_col) = wc.input.find_id_column_for_alias(&alias.0) {
                            log::info!("🔧 CTE Extraction: Wrapping non-ID columns of '{}' with anyLast()", alias.0);
                            
                            // Expand TableAlias to all properties and wrap non-ID with anyLast()
                            // For now, keep as TableAlias - it will be expanded in plan_builder
                            // where we have access to the schema and can determine which columns are IDs
                            // The anyLast wrapping happens in plan_builder.rs around line 1745-1780
                            item
                        } else {
                            log::warn!("⚠️ CTE Extraction: Could not find ID column for alias '{}', keeping as-is", alias.0);
                            item
                        }
                    } else {
                        item
                    }
                }).collect()
            } else {
                expanded_items
            };
            
            // Create a Projection wrapping the input with the WITH items
            // This ensures the rendered SQL has proper SELECT items
            use crate::query_planner::logical_plan::Projection;
            
            let projection_with_with_items = Projection {
                input: wc.input.clone(),
                items: final_items.clone(),
                distinct: wc.distinct,
            };
            
            // If we have aggregations, wrap in GroupBy node with proper ID column lookup
            // Use the same logic as build_chained_with_match_cte_plan
            let plan_to_render = if has_aggregation {
                // Build GROUP BY expressions using TableAlias → ID column lookup
                // Only group by non-aggregate items
                let group_by_exprs: Vec<_> = final_items.iter()
                    .filter(|item| !matches!(&item.expression, crate::query_planner::logical_expr::LogicalExpr::AggregateFnCall(_)))
                    .flat_map(|item| {
                        use crate::query_planner::logical_expr::LogicalExpr;
                        match &item.expression {
                            LogicalExpr::TableAlias(alias) => {
                                // For TableAlias, find and use ID column only
                                // Try to find the ID column from the input plan
                                if let Ok(id_col) = wc.input.find_id_column_for_alias(&alias.0) {
                                    log::info!("🔧 CTE Extraction: Found ID column '{}' for alias '{}' via find_id_column_for_alias", id_col, alias.0);
                                    vec![LogicalExpr::PropertyAccessExp(
                                        crate::query_planner::logical_expr::PropertyAccess {
                                            table_alias: alias.clone(),
                                            column: crate::graph_catalog::expression_parser::PropertyValue::Column(id_col),
                                        }
                                    )]
                                } else {
                                    log::warn!("⚠️ CTE Extraction: Could not find ID column for alias '{}', skipping from GROUP BY", alias.0);
                                    vec![]
                                }
                            }
                            LogicalExpr::ArraySubscript { array, .. } => {
                                // For array subscripts (e.g., labels(x)[1]), only GROUP BY the array part
                                // ClickHouse can't GROUP BY an array element, only the array itself
                                vec![(**array).clone()]
                            }
                            _ => {
                                // For other expressions, use as-is in GROUP BY
                                vec![item.expression.clone()]
                            }
                        }
                    })
                    .collect();
                
                if !group_by_exprs.is_empty() {
                    log::info!("🔧 CTE Extraction: Creating GroupBy with {} expressions", group_by_exprs.len());
                    use crate::query_planner::logical_plan::GroupBy;
                    use std::sync::Arc;
                    
                    LogicalPlan::GroupBy(GroupBy {
                        input: Arc::new(LogicalPlan::Projection(projection_with_with_items)),
                        expressions: group_by_exprs,
                        having_clause: None,
                        is_materialization_boundary: false,
                        exposed_alias: wc.exported_aliases.first().cloned(),
                    })
                } else {
                    log::warn!("⚠️ CTE Extraction: has_aggregation but no valid GROUP BY expressions, using Projection only");
                    LogicalPlan::Projection(projection_with_with_items)
                }
            } else {
                LogicalPlan::Projection(projection_with_with_items)
            };
            
            let cte_render_plan = plan_to_render.to_render_plan(schema)?;
            
            // Create the CTE
            let with_cte = Cte::new(
                    cte_name.clone(),
                    CteContent::Structured(cte_render_plan),
                    false,
                );
            
            // CRITICAL: Insert WITH clause CTE at the BEGINNING of the list
            // This ensures it's in the first CTE group and doesn't get nested
            // inside subsequent recursive CTE groups (which would make it inaccessible)
            ctes.insert(0, with_cte);
            
            log::info!("🔧 CTE Extraction: Added WITH CTE '{}' at beginning of CTE list", cte_name);
            Ok(ctes)
        }
    }
}

/// Check if a variable-length relationship is optional (for OPTIONAL MATCH semantics)
/// Returns true if the VLP should use LEFT JOIN instead of INNER JOIN
pub fn is_variable_length_optional(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::GraphRel(rel) if rel.variable_length.is_some() => {
            rel.is_optional.unwrap_or(false)
        }
        LogicalPlan::GraphNode(node) => is_variable_length_optional(&node.input),
        LogicalPlan::Filter(filter) => is_variable_length_optional(&filter.input),
        LogicalPlan::Projection(proj) => is_variable_length_optional(&proj.input),
        LogicalPlan::GraphJoins(joins) => is_variable_length_optional(&joins.input),
        LogicalPlan::GroupBy(gb) => is_variable_length_optional(&gb.input),
        LogicalPlan::OrderBy(ob) => is_variable_length_optional(&ob.input),
        LogicalPlan::Skip(skip) => is_variable_length_optional(&skip.input),
        LogicalPlan::Limit(limit) => is_variable_length_optional(&limit.input),
        LogicalPlan::Cte(cte) => is_variable_length_optional(&cte.input),
        _ => false,
    }
}

/// Check if the plan contains a variable-length relationship and return node aliases
/// Returns (left_alias, right_alias) if found
pub fn has_variable_length_rel(plan: &LogicalPlan) -> Option<(String, String)> {
    log::debug!("🔍 has_variable_length_rel: Checking plan type: {:?}", std::mem::discriminant(plan));
    let result = match plan {
        LogicalPlan::GraphRel(rel) if rel.variable_length.is_some() => {
            log::debug!("  ✅ Found VLP in GraphRel: {} -> {}", rel.left_connection, rel.right_connection);
            Some((rel.left_connection.clone(), rel.right_connection.clone()))
        }
        // For GraphRel without variable_length, check nested GraphRels in left branch
        // This handles chained patterns like (u)-[*]->(g)-[:REL]->(f)
        LogicalPlan::GraphRel(rel) => {
            log::debug!("  → GraphRel without VLP, checking left branch");
            has_variable_length_rel(&rel.left)
        }
        LogicalPlan::GraphNode(node) => {
            log::debug!("  → GraphNode, checking input");
            has_variable_length_rel(&node.input)
        }
        LogicalPlan::Filter(filter) => {
            log::debug!("  → Filter, checking input");
            has_variable_length_rel(&filter.input)
        }
        LogicalPlan::Projection(proj) => {
            log::debug!("  → Projection, checking input");
            has_variable_length_rel(&proj.input)
        }
        LogicalPlan::GraphJoins(joins) => {
            log::debug!("  → GraphJoins, checking input");
            has_variable_length_rel(&joins.input)
        }
        LogicalPlan::GroupBy(gb) => has_variable_length_rel(&gb.input),
        LogicalPlan::OrderBy(ob) => has_variable_length_rel(&ob.input),
        LogicalPlan::Skip(skip) => has_variable_length_rel(&skip.input),
        LogicalPlan::Limit(limit) => has_variable_length_rel(&limit.input),
        LogicalPlan::Cte(cte) => {
            log::debug!("  → Cte, checking input");
            has_variable_length_rel(&cte.input)
        }
        _ => {
            log::debug!("  ✗ No VLP found in this branch");
            None
        }
    };
    log::debug!("  Result: {:?}", result);
    result
}

/// Get all VLP-related aliases: (start_node_alias, end_node_alias, relationship_alias)
/// Used to determine if filters should be handled by CTE vs outer query
pub fn get_variable_length_aliases(plan: &LogicalPlan) -> Option<(String, String, String)> {
    match plan {
        LogicalPlan::GraphRel(rel) if rel.variable_length.is_some() => {
            Some((rel.left_connection.clone(), rel.right_connection.clone(), rel.alias.clone()))
        }
        LogicalPlan::GraphRel(rel) => get_variable_length_aliases(&rel.left),
        LogicalPlan::GraphNode(node) => get_variable_length_aliases(&node.input),
        LogicalPlan::Filter(filter) => get_variable_length_aliases(&filter.input),
        LogicalPlan::Projection(proj) => get_variable_length_aliases(&proj.input),
        LogicalPlan::GraphJoins(joins) => get_variable_length_aliases(&joins.input),
        LogicalPlan::GroupBy(gb) => get_variable_length_aliases(&gb.input),
        LogicalPlan::OrderBy(ob) => get_variable_length_aliases(&ob.input),
        LogicalPlan::Skip(skip) => get_variable_length_aliases(&skip.input),
        LogicalPlan::Limit(limit) => get_variable_length_aliases(&limit.input),
        LogicalPlan::Cte(cte) => get_variable_length_aliases(&cte.input),
        _ => None,
    }
}

/// Check if a variable-length pattern uses denormalized edges
/// Returns true if EITHER node is virtual (embedded in edge table)
/// For checking if BOTH are denormalized, use get_variable_length_denorm_info
pub fn is_variable_length_denormalized(plan: &LogicalPlan) -> bool {
    fn check_node_denormalized(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::GraphNode(node) => node.is_denormalized,
            _ => false,
        }
    }

    match plan {
        LogicalPlan::GraphRel(rel) if rel.variable_length.is_some() => {
            // Check if either left or right node is denormalized
            check_node_denormalized(&rel.left) || check_node_denormalized(&rel.right)
        }
        // For GraphRel without variable_length, check nested GraphRels in left branch
        LogicalPlan::GraphRel(rel) => is_variable_length_denormalized(&rel.left),
        LogicalPlan::GraphNode(node) => is_variable_length_denormalized(&node.input),
        LogicalPlan::Filter(filter) => is_variable_length_denormalized(&filter.input),
        LogicalPlan::Projection(proj) => is_variable_length_denormalized(&proj.input),
        LogicalPlan::GraphJoins(joins) => is_variable_length_denormalized(&joins.input),
        LogicalPlan::GroupBy(gb) => is_variable_length_denormalized(&gb.input),
        LogicalPlan::OrderBy(ob) => is_variable_length_denormalized(&ob.input),
        LogicalPlan::Skip(skip) => is_variable_length_denormalized(&skip.input),
        LogicalPlan::Limit(limit) => is_variable_length_denormalized(&limit.input),
        LogicalPlan::Cte(cte) => is_variable_length_denormalized(&cte.input),
        _ => false,
    }
}

/// Detailed denormalization info for a variable-length pattern
#[derive(Debug, Clone)]
pub struct VariableLengthDenormInfo {
    pub start_is_denormalized: bool,
    pub end_is_denormalized: bool,
    // Node table information extracted from the plan (fully qualified)
    pub start_table: Option<String>,
    pub start_id_col: Option<String>,
    pub end_table: Option<String>,
    pub end_id_col: Option<String>,
}

impl VariableLengthDenormInfo {
    pub fn is_fully_denormalized(&self) -> bool {
        self.start_is_denormalized && self.end_is_denormalized
    }

    pub fn is_mixed(&self) -> bool {
        self.start_is_denormalized != self.end_is_denormalized
    }

    pub fn is_any_denormalized(&self) -> bool {
        self.start_is_denormalized || self.end_is_denormalized
    }
}

/// Get detailed denormalization info for a variable-length pattern
pub fn get_variable_length_denorm_info(plan: &LogicalPlan) -> Option<VariableLengthDenormInfo> {
    fn check_node_denormalized(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::GraphNode(node) => node.is_denormalized,
            _ => false,
        }
    }

    match plan {
        LogicalPlan::GraphRel(rel) if rel.variable_length.is_some() => {
            // 🔧 PARAMETERIZED VIEW FIX: Extract table names with parameterized view syntax
            // This ensures outer JOINs also use parameterized views (tenant_id = 'value')
            let start_table = extract_parameterized_table_name(&rel.left);
            let end_table = extract_parameterized_table_name(&rel.right);
            let start_id_col = extract_id_column(&rel.left);
            let end_id_col = extract_id_column(&rel.right);

            Some(VariableLengthDenormInfo {
                start_is_denormalized: check_node_denormalized(&rel.left),
                end_is_denormalized: check_node_denormalized(&rel.right),
                start_table,
                start_id_col,
                end_table,
                end_id_col,
            })
        }
        // For GraphRel without variable_length, check nested GraphRels in left branch
        // This handles chained patterns like (u)-[*]->(g)-[:REL]->(f)
        LogicalPlan::GraphRel(rel) => {
            // Recurse into left branch to find nested VLP
            get_variable_length_denorm_info(&rel.left)
        }
        LogicalPlan::GraphNode(node) => get_variable_length_denorm_info(&node.input),
        LogicalPlan::Filter(filter) => get_variable_length_denorm_info(&filter.input),
        LogicalPlan::Projection(proj) => get_variable_length_denorm_info(&proj.input),
        LogicalPlan::GraphJoins(joins) => get_variable_length_denorm_info(&joins.input),
        LogicalPlan::GroupBy(gb) => get_variable_length_denorm_info(&gb.input),
        LogicalPlan::OrderBy(ob) => get_variable_length_denorm_info(&ob.input),
        LogicalPlan::Skip(skip) => get_variable_length_denorm_info(&skip.input),
        LogicalPlan::Limit(limit) => get_variable_length_denorm_info(&limit.input),
        LogicalPlan::Cte(cte) => get_variable_length_denorm_info(&cte.input),
        _ => None,
    }
}

/// Info about the relationship in a variable-length pattern
/// Used for SELECT rewriting to map f.Origin → t.start_id, f.Dest → t.end_id
#[derive(Debug, Clone)]
pub struct VariableLengthRelInfo {
    pub rel_alias: String, // e.g., "f"
    pub from_col: String,  // e.g., "Origin"
    pub to_col: String,    // e.g., "Dest"
}

/// Extract relationship info (alias, from_col, to_col) from a variable-length path
pub fn get_variable_length_rel_info(plan: &LogicalPlan) -> Option<VariableLengthRelInfo> {
    match plan {
        LogicalPlan::GraphRel(rel) if rel.variable_length.is_some() => {
            // Get the from/to columns from the ViewScan in the center
            let cols = extract_relationship_columns(&rel.center)?;
            Some(VariableLengthRelInfo {
                rel_alias: rel.alias.clone(),
                from_col: cols.from_id,
                to_col: cols.to_id,
            })
        }
        LogicalPlan::GraphNode(node) => get_variable_length_rel_info(&node.input),
        LogicalPlan::Filter(filter) => get_variable_length_rel_info(&filter.input),
        LogicalPlan::Projection(proj) => get_variable_length_rel_info(&proj.input),
        LogicalPlan::GraphJoins(joins) => get_variable_length_rel_info(&joins.input),
        LogicalPlan::GroupBy(gb) => get_variable_length_rel_info(&gb.input),
        LogicalPlan::OrderBy(ob) => get_variable_length_rel_info(&ob.input),
        LogicalPlan::Skip(skip) => get_variable_length_rel_info(&skip.input),
        LogicalPlan::Limit(limit) => get_variable_length_rel_info(&limit.input),
        LogicalPlan::Cte(cte) => get_variable_length_rel_info(&cte.input),
        _ => None,
    }
}

/// Extract path variable from the plan (variable-length paths only, for CTE generation)
pub fn get_path_variable(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::GraphRel(rel) if rel.variable_length.is_some() => rel.path_variable.clone(),
        LogicalPlan::GraphNode(node) => get_path_variable(&node.input),
        LogicalPlan::Filter(filter) => get_path_variable(&filter.input),
        LogicalPlan::Projection(proj) => get_path_variable(&proj.input),
        LogicalPlan::GraphJoins(joins) => get_path_variable(&joins.input),
        LogicalPlan::GroupBy(gb) => get_path_variable(&gb.input),
        LogicalPlan::OrderBy(ob) => get_path_variable(&ob.input),
        LogicalPlan::Skip(skip) => get_path_variable(&skip.input),
        LogicalPlan::Limit(limit) => get_path_variable(&limit.input),
        LogicalPlan::Cte(cte) => get_path_variable(&cte.input),
        LogicalPlan::Unwind(u) => get_path_variable(&u.input),
        LogicalPlan::WithClause(wc) => get_path_variable(&wc.input),
        LogicalPlan::Union(union_plan) => {
            // Check first branch for path variable
            // All branches should have the same path variable if any
            if !union_plan.inputs.is_empty() {
                get_path_variable(&union_plan.inputs[0])
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Extract path variable from fixed multi-hop patterns (no variable_length)
/// Returns (path_variable_name, hop_count) if found
pub fn get_fixed_path_variable(plan: &LogicalPlan) -> Option<(String, u32)> {
    match plan {
        LogicalPlan::GraphRel(rel) => {
            // Only handle fixed patterns (no variable_length)
            if rel.variable_length.is_some() {
                return None;
            }

            if let Some(ref path_var) = rel.path_variable {
                // Count hops by traversing the GraphRel chain
                let hop_count = count_hops_in_graph_rel(plan);
                return Some((path_var.clone(), hop_count));
            }

            // Check nested GraphRels
            if let LogicalPlan::GraphRel(_) = rel.left.as_ref() {
                return get_fixed_path_variable(&rel.left);
            }
            None
        }
        LogicalPlan::GraphNode(node) => get_fixed_path_variable(&node.input),
        LogicalPlan::Filter(filter) => get_fixed_path_variable(&filter.input),
        LogicalPlan::Projection(proj) => get_fixed_path_variable(&proj.input),
        LogicalPlan::GraphJoins(joins) => get_fixed_path_variable(&joins.input),
        LogicalPlan::GroupBy(gb) => get_fixed_path_variable(&gb.input),
        LogicalPlan::OrderBy(ob) => get_fixed_path_variable(&ob.input),
        LogicalPlan::Skip(skip) => get_fixed_path_variable(&skip.input),
        LogicalPlan::Limit(limit) => get_fixed_path_variable(&limit.input),
        LogicalPlan::Cte(cte) => get_fixed_path_variable(&cte.input),
        LogicalPlan::Unwind(u) => get_fixed_path_variable(&u.input),
        _ => None,
    }
}

/// Count the number of hops (relationships) in a GraphRel chain
fn count_hops_in_graph_rel(plan: &LogicalPlan) -> u32 {
    match plan {
        LogicalPlan::GraphRel(rel) => {
            // Count this relationship + any nested ones
            1 + count_hops_in_graph_rel(&rel.left)
        }
        LogicalPlan::GraphNode(node) => count_hops_in_graph_rel(&node.input),
        _ => 0,
    }
}

/// Complete information about a fixed path pattern
/// For `p = (a)-[r1]->(b)-[r2]->(c)`:
/// - path_var_name: "p"
/// - node_aliases: ["a", "b", "c"]
/// - rel_aliases: ["r1", "r2"]
/// - hop_count: 2
/// - node_id_columns: mapping from node alias to (rel_alias, id_column)
///   e.g., {"a" -> ("r1", "Origin"), "b" -> ("r1", "Dest"), "c" -> ("r2", "Dest")}
#[derive(Debug, Clone)]
pub struct FixedPathInfo {
    pub path_var_name: String,
    pub node_aliases: Vec<String>,
    pub rel_aliases: Vec<String>,
    pub hop_count: u32,
    /// Maps node alias to (relationship_alias, id_column) for denormalized schemas
    /// e.g., "a" -> ("r", "Origin"), "b" -> ("r", "Dest")
    pub node_id_columns: std::collections::HashMap<String, (String, String)>,
}

/// Extract complete path information from fixed multi-hop patterns
/// Returns FixedPathInfo with all node and relationship aliases
pub fn get_fixed_path_info(
    plan: &LogicalPlan,
) -> Result<Option<FixedPathInfo>, super::errors::RenderBuildError> {
    // First find the path variable and hop count
    let (path_var_name, hop_count) = match get_fixed_path_variable(plan) {
        Some(info) => info,
        None => return Ok(None),
    };

    // Then extract all aliases and node ID mappings
    let (node_aliases, rel_aliases, node_id_columns) = collect_path_aliases_with_ids(plan)?;

    Ok(Some(FixedPathInfo {
        path_var_name,
        node_aliases,
        rel_aliases,
        hop_count,
        node_id_columns,
    }))
}

/// Collect node and relationship aliases plus ID column mappings
fn collect_path_aliases_with_ids(
    plan: &LogicalPlan,
) -> Result<
    (
        Vec<String>,
        Vec<String>,
        std::collections::HashMap<String, (String, String)>,
    ),
    super::errors::RenderBuildError,
> {
    let mut node_aliases = Vec::new();
    let mut rel_aliases = Vec::new();
    let mut node_id_columns = std::collections::HashMap::new();

    collect_path_aliases_with_ids_recursive(
        plan,
        &mut node_aliases,
        &mut rel_aliases,
        &mut node_id_columns,
    )?;

    Ok((node_aliases, rel_aliases, node_id_columns))
}

/// Recursive helper to collect aliases and ID column mappings
fn collect_path_aliases_with_ids_recursive(
    plan: &LogicalPlan,
    node_aliases: &mut Vec<String>,
    rel_aliases: &mut Vec<String>,
    node_id_columns: &mut std::collections::HashMap<String, (String, String)>,
) -> Result<(), super::errors::RenderBuildError> {
    match plan {
        LogicalPlan::GraphRel(rel) => {
            // Process left side first (may be another GraphRel or the start node)
            collect_path_aliases_with_ids_recursive(
                &rel.left,
                node_aliases,
                rel_aliases,
                node_id_columns,
            )?;

            // Get the from_id and to_id columns from the ViewScan
            if let LogicalPlan::ViewScan(view_scan) = rel.center.as_ref() {
                // ViewScan should ALWAYS have from_id and to_id for relationship scans
                // If missing, this is a query planner bug, not a user error
                let from_id = view_scan.from_id.clone().ok_or_else(|| {
                    super::errors::RenderBuildError::ViewScanMissingRelationshipColumn(
                        "from_id".to_string(),
                    )
                })?;
                let to_id = view_scan.to_id.clone().ok_or_else(|| {
                    super::errors::RenderBuildError::ViewScanMissingRelationshipColumn(
                        "to_id".to_string(),
                    )
                })?;

                // Map left node to this relationship's from_id (if not already mapped)
                if !node_id_columns.contains_key(&rel.left_connection) {
                    node_id_columns.insert(
                        rel.left_connection.clone(),
                        (rel.alias.clone(), from_id.clone()),
                    );
                }

                // Map right node to this relationship's to_id
                node_id_columns.insert(rel.right_connection.clone(), (rel.alias.clone(), to_id));
            }

            // Add this relationship
            rel_aliases.push(rel.alias.clone());

            // Add the right node
            if let LogicalPlan::GraphNode(right_node) = rel.right.as_ref() {
                if !node_aliases.contains(&right_node.alias) {
                    node_aliases.push(right_node.alias.clone());
                }
            }
        }
        LogicalPlan::GraphNode(node) => {
            // Start node - add it if not already present
            if !node_aliases.contains(&node.alias) {
                node_aliases.push(node.alias.clone());
            }
            // Recurse into input
            collect_path_aliases_with_ids_recursive(
                &node.input,
                node_aliases,
                rel_aliases,
                node_id_columns,
            )?;
        }
        LogicalPlan::Filter(filter) => {
            collect_path_aliases_with_ids_recursive(
                &filter.input,
                node_aliases,
                rel_aliases,
                node_id_columns,
            )?;
        }
        LogicalPlan::Projection(proj) => {
            collect_path_aliases_with_ids_recursive(
                &proj.input,
                node_aliases,
                rel_aliases,
                node_id_columns,
            )?;
        }
        LogicalPlan::GraphJoins(joins) => {
            collect_path_aliases_with_ids_recursive(
                &joins.input,
                node_aliases,
                rel_aliases,
                node_id_columns,
            )?;
        }
        LogicalPlan::GroupBy(gb) => {
            collect_path_aliases_with_ids_recursive(
                &gb.input,
                node_aliases,
                rel_aliases,
                node_id_columns,
            )?;
        }
        LogicalPlan::OrderBy(ob) => {
            collect_path_aliases_with_ids_recursive(
                &ob.input,
                node_aliases,
                rel_aliases,
                node_id_columns,
            )?;
        }
        LogicalPlan::Skip(skip) => {
            collect_path_aliases_with_ids_recursive(
                &skip.input,
                node_aliases,
                rel_aliases,
                node_id_columns,
            )?;
        }
        LogicalPlan::Limit(limit) => {
            collect_path_aliases_with_ids_recursive(
                &limit.input,
                node_aliases,
                rel_aliases,
                node_id_columns,
            )?;
        }
        _ => {}
    }
    Ok(())
}

// ============================================================================
// VLP (Variable-Length Path) Schema Types and Consolidated Info
// ============================================================================

/// Schema type classification for VLP query generation
///
/// Different schema types require different SQL generation strategies:
/// - Normal: Separate node and edge tables, standard JOIN patterns
/// - Polymorphic: Single edge table with type_column, nodes still separate
/// - Denormalized: Nodes embedded in edge table (no separate node tables)
/// - FkEdge: FK column on node table represents edge (no separate edge table)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VlpSchemaType {
    /// Standard schema: separate tables for nodes and edges
    /// Example: users table + follows table
    Normal,

    /// Polymorphic edge: single edge table with type_column to distinguish edge types
    /// Example: interactions table with interaction_type column
    /// Nodes still have separate tables
    Polymorphic,

    /// Denormalized: node properties embedded in edge table
    /// Example: flights table with Origin/Dest as node IDs and OriginCity/DestCity as properties
    /// No separate node tables exist
    Denormalized,

    /// FK-Edge: edge is represented by a FK column on the node table
    /// Example: fs_objects table with parent_id FK column
    /// Edge table == Node table (self-referencing)
    FkEdge,
}

/// Consolidated VLP context containing all information needed for SQL generation
///
/// This struct gathers all the scattered VLP-related info into one place,
/// making it easier to reason about and pass through the code.
#[derive(Debug, Clone)]
pub struct VlpContext {
    /// Schema type determines SQL generation strategy
    pub schema_type: VlpSchemaType,

    /// True if exact hop count (e.g., *2, *3), false if range/unbounded
    pub is_fixed_length: bool,

    /// Exact hop count if fixed-length, None otherwise
    pub exact_hops: Option<u32>,

    /// Min/max hops for range patterns
    pub min_hops: Option<u32>,
    pub max_hops: Option<u32>,

    /// Start node information
    pub start_alias: String,
    pub start_table: String,
    pub start_id_col: String,
    /// Parameterized table reference for start node (e.g., `db.table`(param='value'))
    pub start_table_parameterized: Option<String>,

    /// End node information
    pub end_alias: String,
    pub end_table: String,
    pub end_id_col: String,
    /// Parameterized table reference for end node (e.g., `db.table`(param='value'))
    pub end_table_parameterized: Option<String>,

    /// Relationship information
    pub rel_alias: String,
    pub rel_table: String,
    pub rel_from_col: String,
    pub rel_to_col: String,
    /// Parameterized table reference for relationship (e.g., `db.table`(param='value'))
    pub rel_table_parameterized: Option<String>,

    /// For polymorphic edges: type column and value
    pub type_column: Option<String>,
    pub type_value: Option<String>,

    /// For denormalized edges: property mappings (logical_name -> ClickHouse column/expression)
    pub from_node_properties: Option<std::collections::HashMap<String, PropertyValue>>,
    pub to_node_properties: Option<std::collections::HashMap<String, PropertyValue>>,

    /// For FK-edge patterns: true if edge is represented by FK on node table
    pub is_fk_edge: bool,
}

impl VlpContext {
    /// Check if this VLP needs a recursive CTE (true for range/unbounded patterns)
    pub fn needs_cte(&self) -> bool {
        !self.is_fixed_length
    }

    /// Check if nodes have separate tables (not denormalized)
    pub fn has_separate_node_tables(&self) -> bool {
        self.schema_type != VlpSchemaType::Denormalized && self.schema_type != VlpSchemaType::FkEdge
    }

    /// Check if this is an FK-edge pattern
    pub fn is_fk_edge(&self) -> bool {
        self.schema_type == VlpSchemaType::FkEdge || self.is_fk_edge
    }
}

/// Detect VLP schema type from a GraphRel
pub fn detect_vlp_schema_type(
    graph_rel: &crate::query_planner::logical_plan::GraphRel,
) -> VlpSchemaType {
    // Check if nodes are denormalized
    let left_is_denorm = is_node_denormalized_from_graph_node(&graph_rel.left);
    let right_is_denorm = is_node_denormalized_from_graph_node(&graph_rel.right);

    if left_is_denorm && right_is_denorm {
        return VlpSchemaType::Denormalized;
    }

    // Check for FK-edge pattern: edge table == node table (self-referencing FK)
    // This is detected by checking if rel_table == start_table == end_table
    let rel_table = extract_table_name(&graph_rel.center);
    let start_table = extract_node_table(&graph_rel.left);
    let end_table = extract_node_table(&graph_rel.right);

    if let (Some(rt), Some(st), Some(et)) = (rel_table, start_table, end_table) {
        if rt == st && rt == et {
            return VlpSchemaType::FkEdge;
        }
    }

    // Check for polymorphic edge (has type_column)
    if let LogicalPlan::ViewScan(scan) = graph_rel.center.as_ref() {
        if scan.type_column.is_some() {
            return VlpSchemaType::Polymorphic;
        }
    }

    VlpSchemaType::Normal
}

/// Extract table name from a node plan
fn extract_node_table(plan: &LogicalPlan) -> Option<String> {
    match plan {
        LogicalPlan::GraphNode(node) => {
            if let LogicalPlan::ViewScan(scan) = node.input.as_ref() {
                Some(scan.source_table.clone())
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Helper to check if a GraphNode is denormalized
fn is_node_denormalized_from_graph_node(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::GraphNode(node) => node.is_denormalized,
        _ => false,
    }
}

/// Build a complete VlpContext from a GraphRel
///
/// This gathers all VLP-related information into a single struct
pub fn build_vlp_context(
    graph_rel: &crate::query_planner::logical_plan::GraphRel,
) -> Option<VlpContext> {
    let spec = graph_rel.variable_length.as_ref()?;

    let schema_type = detect_vlp_schema_type(graph_rel);
    let is_fixed_length =
        spec.exact_hop_count().is_some() && graph_rel.shortest_path_mode.is_none();
    let exact_hops = spec.exact_hop_count();

    // Extract start node info
    let (start_alias, start_table, start_id_col) =
        extract_node_info(&graph_rel.left, schema_type, &graph_rel.center)?;

    // Extract end node info
    let (end_alias, end_table, end_id_col) =
        extract_node_info(&graph_rel.right, schema_type, &graph_rel.center)?;

    // Extract relationship info
    let rel_alias = graph_rel.alias.clone();
    let rel_table = extract_table_name(&graph_rel.center)?;
    let rel_cols = extract_relationship_columns(&graph_rel.center)?;

    // 🔧 PARAMETERIZED VIEW FIX: Extract parameterized table names for chained join optimization
    let start_table_parameterized = extract_parameterized_table_name(&graph_rel.left);
    let end_table_parameterized = extract_parameterized_table_name(&graph_rel.right);
    let rel_table_parameterized = extract_parameterized_rel_table(&graph_rel.center);
    
    log::debug!(
        "build_vlp_context: start_table='{}' parameterized={:?}, end_table='{}' parameterized={:?}, rel_table='{}' parameterized={:?}",
        start_table, start_table_parameterized, end_table, end_table_parameterized, rel_table, rel_table_parameterized
    );

    // Extract polymorphic type info
    let (type_column, type_value) = if let LogicalPlan::ViewScan(scan) = graph_rel.center.as_ref() {
        (
            scan.type_column.clone(),
            graph_rel.labels.as_ref().and_then(|l| l.first().cloned()),
        )
    } else {
        (None, None)
    };

    // Extract denormalized property mappings
    let (from_node_properties, to_node_properties) =
        if let LogicalPlan::ViewScan(scan) = graph_rel.center.as_ref() {
            (
                scan.from_node_properties.clone(),
                scan.to_node_properties.clone(),
            )
        } else {
            (None, None)
        };

    // Detect FK-edge pattern
    let is_fk_edge = schema_type == VlpSchemaType::FkEdge;

    Some(VlpContext {
        schema_type,
        is_fixed_length,
        exact_hops,
        min_hops: spec.min_hops,
        max_hops: spec.max_hops,
        start_alias,
        start_table,
        start_id_col,
        start_table_parameterized,
        end_alias,
        end_table,
        end_id_col,
        end_table_parameterized,
        rel_alias,
        rel_table,
        rel_from_col: rel_cols.from_id,
        rel_to_col: rel_cols.to_id,
        rel_table_parameterized,
        type_column,
        type_value,
        from_node_properties,
        to_node_properties,
        is_fk_edge,
    })
}

/// Extract node info (alias, table, id_col) handling different schema types
fn extract_node_info(
    node_plan: &LogicalPlan,
    schema_type: VlpSchemaType,
    rel_center: &LogicalPlan,
) -> Option<(String, String, String)> {
    match node_plan {
        LogicalPlan::GraphNode(node) => {
            let alias = node.alias.clone();

            match schema_type {
                VlpSchemaType::Denormalized => {
                    // For denormalized, table comes from relationship
                    let table = extract_table_name(rel_center)?;
                    // ID column is from relationship's from_id or to_id
                    let rel_cols = extract_relationship_columns(rel_center)?;
                    // Determine if this is start or end node by checking if it's the left or right
                    // For now, use from_id - caller should determine correct column
                    Some((alias, table, rel_cols.from_id))
                }
                _ => {
                    // Normal/Polymorphic: get from node's ViewScan
                    if let LogicalPlan::ViewScan(scan) = node.input.as_ref() {
                        let table = scan.source_table.clone();
                        let id_col = scan.id_column.clone();
                        Some((alias, table, id_col))
                    } else if let Some(label) = &node.label {
                        // Fallback: derive from label
                        let table = label_to_table_name(label);
                        let id_col = table_to_id_column(&table);
                        Some((alias, table, id_col))
                    } else {
                        None
                    }
                }
            }
        }
        LogicalPlan::GraphRel(rel) => {
            // Handle case where node_plan is a GraphRel (nested relationship pattern)
            // Extract the actual node from the GraphRel's left connection
            // For patterns like: (person)<-[:HAS_CREATOR]-(message)-[:REPLY_OF*0..]->(post)
            // When analyzing REPLY_OF, the left is HAS_CREATOR GraphRel, need to get message node
            extract_node_info(&rel.left, schema_type, rel_center)
        }
        _ => None,
    }
}

/// Extract variable length spec from the plan
pub fn get_variable_length_spec(
    plan: &LogicalPlan,
) -> Option<crate::query_planner::logical_plan::VariableLengthSpec> {
    match plan {
        LogicalPlan::GraphRel(rel) => {
            // Check if this GraphRel has variable_length
            if rel.variable_length.is_some() {
                return rel.variable_length.clone();
            }
            // Recursively check nested GraphRels (for chained patterns like (a)-[*]->(b)-[:R]->(c))
            get_variable_length_spec(&rel.left).or_else(|| get_variable_length_spec(&rel.right))
        }
        LogicalPlan::GraphNode(node) => get_variable_length_spec(&node.input),
        LogicalPlan::Filter(filter) => get_variable_length_spec(&filter.input),
        LogicalPlan::Projection(proj) => get_variable_length_spec(&proj.input),
        LogicalPlan::GraphJoins(joins) => get_variable_length_spec(&joins.input),
        LogicalPlan::GroupBy(gb) => get_variable_length_spec(&gb.input),
        LogicalPlan::OrderBy(ob) => get_variable_length_spec(&ob.input),
        LogicalPlan::Skip(skip) => get_variable_length_spec(&skip.input),
        LogicalPlan::Limit(limit) => get_variable_length_spec(&limit.input),
        LogicalPlan::Cte(cte) => get_variable_length_spec(&cte.input),
        LogicalPlan::Unwind(u) => get_variable_length_spec(&u.input),
        _ => None,
    }
}

/// Extract shortest path mode from the plan
pub fn get_shortest_path_mode(
    plan: &LogicalPlan,
) -> Option<crate::query_planner::logical_plan::ShortestPathMode> {
    match plan {
        LogicalPlan::GraphRel(rel) => rel.shortest_path_mode.clone(),
        LogicalPlan::GraphNode(node) => get_shortest_path_mode(&node.input),
        LogicalPlan::Filter(filter) => get_shortest_path_mode(&filter.input),
        LogicalPlan::Projection(proj) => get_shortest_path_mode(&proj.input),
        LogicalPlan::GraphJoins(joins) => get_shortest_path_mode(&joins.input),
        LogicalPlan::GroupBy(gb) => get_shortest_path_mode(&gb.input),
        LogicalPlan::OrderBy(ob) => get_shortest_path_mode(&ob.input),
        LogicalPlan::Skip(skip) => get_shortest_path_mode(&skip.input),
        LogicalPlan::Limit(limit) => get_shortest_path_mode(&limit.input),
        LogicalPlan::Cte(cte) => get_shortest_path_mode(&cte.input),
        LogicalPlan::Unwind(u) => get_shortest_path_mode(&u.input),
        _ => None,
    }
}

/// Extract node label from ViewScan in the plan
/// Uses the provided schema for node label lookup
pub fn extract_node_label_from_viewscan_with_schema(
    plan: &LogicalPlan,
    schema: &GraphSchema,
) -> Option<String> {
    match plan {
        LogicalPlan::ViewScan(view_scan) => {
            // Look up node label from the provided schema using table name
            if let Some((label, _)) = get_node_schema_by_table(schema, &view_scan.source_table) {
                return Some(label.to_string());
            }
            None
        }
        LogicalPlan::GraphNode(node) => {
            // First try to get label directly from the GraphNode (for denormalized nodes)
            if let Some(label) = &node.label {
                return Some(label.clone());
            }
            // Otherwise, recurse into input
            extract_node_label_from_viewscan_with_schema(&node.input, schema)
        }
        LogicalPlan::Filter(filter) => extract_node_label_from_viewscan_with_schema(&filter.input, schema),
        _ => None,
    }
}

/// Extract node label from ViewScan in the plan (legacy version using global schemas)
/// ⚠️ DEPRECATED: Use extract_node_label_from_viewscan_with_schema instead
pub fn extract_node_label_from_viewscan(plan: &LogicalPlan) -> Option<String> {
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
        LogicalPlan::GraphNode(node) => {
            // First try to get label directly from the GraphNode (for denormalized nodes)
            if let Some(label) = &node.label {
                return Some(label.clone());
            }
            // Otherwise, recurse into input
            extract_node_label_from_viewscan(&node.input)
        }
        LogicalPlan::Filter(filter) => extract_node_label_from_viewscan(&filter.input),
        _ => None,
    }
}

/// Get node schema information by table name
pub fn get_node_schema_by_table<'a>(
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

/// Expand fixed-length path patterns into inline JOINs
///
/// This function generates JOIN clauses for exact hop-count patterns (*2, *3, etc.)
/// without using CTEs. It directly chains relationship and node JOINs.
///
/// # Arguments
/// * `exact_hops` - Number of hops (e.g., 2 for *2)
/// * `start_table` - Table name for start node
/// * `start_id_col` - ID column for start node
/// * `rel_table` - Table name for relationship
/// * `from_col` - From-node ID column in relationship table
/// * `to_col` - To-node ID column in relationship table
/// * `end_table` - Table name for end node
/// * `end_id_col` - ID column for end node
/// * `start_alias` - Cypher alias for start node
/// * `end_alias` - Cypher alias for end node
///
/// # Returns
/// Vector of JOIN items to be added to the FROM clause
pub fn expand_fixed_length_joins(
    exact_hops: u32,
    _start_table: &str,
    start_id_col: &str,
    rel_table: &str,
    from_col: &str,
    to_col: &str,
    end_table: &str,
    end_id_col: &str,
    start_alias: &str,
    end_alias: &str,
) -> Vec<Join> {
    use super::render_expr::{
        Column, Operator, OperatorApplication, PropertyAccess, RenderExpr, TableAlias,
    };

    let mut joins = Vec::new();

    println!(
        "expand_fixed_length_joins: Generating {} hops from {} to {}",
        exact_hops, start_alias, end_alias
    );

    for hop in 1..=exact_hops {
        let rel_alias = format!("r{}", hop);

        // Determine previous node/relationship alias
        let prev_alias = if hop == 1 {
            start_alias.to_string()
        } else {
            // Bridge directly through previous relationship's to_id
            format!("r{}", hop - 1)
        };

        let prev_id_col = if hop == 1 {
            start_id_col.to_string()
        } else {
            to_col.to_string() // Bridge through previous relationship's to_id
        };

        // Add relationship JOIN
        joins.push(Join {
            table_name: rel_table.to_string(),
            table_alias: rel_alias.clone(),
            joining_on: vec![OperatorApplication {
                operator: Operator::Equal,
                operands: vec![
                    RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(prev_alias),
                        column: PropertyValue::Column(prev_id_col),
                    }),
                    RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(rel_alias.clone()),
                        column: PropertyValue::Column(from_col.to_string()),
                    }),
                ],
            }],
            join_type: JoinType::Inner,
            pre_filter: None,
            from_id_column: None,
            to_id_column: None,
        });

        // TODO: Add intermediate node JOIN only if properties referenced
        // For now, always bridge directly through relationship IDs (optimization!)
    }

    // Always add final node JOIN (the endpoint)
    let last_rel = format!("r{}", exact_hops);
    joins.push(Join {
        table_name: end_table.to_string(),
        table_alias: end_alias.to_string(),
        joining_on: vec![OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(last_rel),
                    column: PropertyValue::Column(to_col.to_string()),
                }),
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(end_alias.to_string()),
                    column: PropertyValue::Column(end_id_col.to_string()),
                }),
            ],
        }],
        join_type: JoinType::Inner,
        pre_filter: None,
        from_id_column: None,
        to_id_column: None,
    });

    println!(
        "expand_fixed_length_joins: Generated {} JOINs (no intermediate nodes)",
        joins.len()
    );

    joins
}

/// Schema-aware fixed-length VLP JOIN generation using VlpContext
///
/// This is the consolidated version that handles all schema types correctly:
/// - Normal: FROM start_node, JOINs through r1...rN, final JOIN to end_node
/// - Polymorphic: Same as Normal (nodes have separate tables)
/// - Denormalized: FROM r1 (first edge), JOINs through r2...rN only (no node JOINs)
///
/// # Returns
/// (from_table, from_alias, joins) - The FROM table info and JOIN clauses
pub fn expand_fixed_length_joins_with_context(ctx: &VlpContext) -> (String, String, Vec<Join>) {
    use super::render_expr::{
        Column, Operator, OperatorApplication, PropertyAccess, RenderExpr, TableAlias,
    };

    let exact_hops = ctx.exact_hops.unwrap_or(1);
    let mut joins = Vec::new();

    // 🔧 PARAMETERIZED VIEW FIX: Use parameterized table names if available, else fallback to plain names
    let start_table_ref = ctx.start_table_parameterized.as_ref()
        .unwrap_or(&ctx.start_table);
    let end_table_ref = ctx.end_table_parameterized.as_ref()
        .unwrap_or(&ctx.end_table);
    let rel_table_ref = ctx.rel_table_parameterized.as_ref()
        .unwrap_or(&ctx.rel_table);

    println!(
        "expand_fixed_length_joins_with_context: schema_type={:?}, {} hops from {} to {}",
        ctx.schema_type, exact_hops, ctx.start_alias, ctx.end_alias
    );
    log::debug!(
        "expand_fixed_length_joins_with_context: start_table='{}', end_table='{}', rel_table='{}'",
        start_table_ref, end_table_ref, rel_table_ref
    );

    match ctx.schema_type {
        VlpSchemaType::Denormalized => {
            // DENORMALIZED: No separate node tables
            // FROM: edge_table AS r1 (the first hop becomes FROM)
            // JOINs: r2 ON r1.to_id = r2.from_id, ..., rN ON r(N-1).to_id = rN.from_id
            // No final node JOIN needed - end node properties come from rN.to_node_properties

            // First hop is the FROM table, not a JOIN
            let from_table = rel_table_ref.clone();
            let from_alias = "r1".to_string();

            // Generate JOINs for hops 2..N
            for hop in 2..=exact_hops {
                let rel_alias = format!("r{}", hop);
                let prev_alias = format!("r{}", hop - 1);

                joins.push(Join {
                    table_name: rel_table_ref.clone(),
                    table_alias: rel_alias.clone(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(prev_alias),
                                column: PropertyValue::Column(ctx.rel_to_col.clone()),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias),
                                column: PropertyValue::Column(ctx.rel_from_col.clone()),
                            }),
                        ],
                    }],
                    join_type: JoinType::Inner,
                    pre_filter: None,
                    from_id_column: None,
                    to_id_column: None,
                });
            }

            println!(
                "expand_fixed_length_joins_with_context [DENORMALIZED]: FROM {} AS {}, {} JOINs",
                from_table,
                from_alias,
                joins.len()
            );

            (from_table, from_alias, joins)
        }

        VlpSchemaType::Normal | VlpSchemaType::Polymorphic => {
            // NORMAL/POLYMORPHIC: Separate node tables exist
            // FROM: start_node_table AS start_alias
            // JOINs: r1 ON start.id = r1.from_id, r2 ON r1.to_id = r2.from_id, ..., end ON rN.to_id = end.id

            let from_table = start_table_ref.clone();
            let from_alias = ctx.start_alias.clone();

            for hop in 1..=exact_hops {
                let rel_alias = format!("r{}", hop);

                let (prev_alias, prev_id_col) = if hop == 1 {
                    (ctx.start_alias.clone(), ctx.start_id_col.clone())
                } else {
                    (format!("r{}", hop - 1), ctx.rel_to_col.clone())
                };

                // Add relationship JOIN
                joins.push(Join {
                    table_name: rel_table_ref.clone(),
                    table_alias: rel_alias.clone(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(prev_alias),
                                column: PropertyValue::Column(prev_id_col),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias),
                                column: PropertyValue::Column(ctx.rel_from_col.clone()),
                            }),
                        ],
                    }],
                    join_type: JoinType::Inner,
                    pre_filter: None,
                    from_id_column: None,
                    to_id_column: None,
                });
            }

            // Add final node JOIN - use parameterized end table
            let last_rel = format!("r{}", exact_hops);
            joins.push(Join {
                table_name: end_table_ref.clone(),
                table_alias: ctx.end_alias.clone(),
                joining_on: vec![OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(last_rel),
                            column: PropertyValue::Column(ctx.rel_to_col.clone()),
                        }),
                        RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(ctx.end_alias.clone()),
                            column: PropertyValue::Column(ctx.end_id_col.clone()),
                        }),
                    ],
                }],
                join_type: JoinType::Inner,
                pre_filter: None,
                from_id_column: None,
                to_id_column: None,
            });

            println!(
                "expand_fixed_length_joins_with_context [NORMAL/POLYMORPHIC]: FROM {} AS {}, {} JOINs",
                from_table, from_alias, joins.len()
            );

            (from_table, from_alias, joins)
        }

        VlpSchemaType::FkEdge => {
            // FK-EDGE: Edge is FK column on node table, no separate edge table
            // FROM: start_node_table AS start_alias
            // JOINs: m1 ON start.fk_col = m1.id_col, m2 ON m1.fk_col = m2.id_col, ..., end ON mN-1.fk_col = end.id_col
            //
            // Example for *2 with parent_id FK:
            // FROM fs_objects AS child
            // JOIN fs_objects AS m1 ON child.parent_id = m1.object_id  -- hop 1
            // JOIN fs_objects AS parent ON m1.parent_id = parent.object_id  -- hop 2

            let from_table = start_table_ref.clone();
            let from_alias = ctx.start_alias.clone();

            for hop in 1..=exact_hops {
                let is_last_hop = hop == exact_hops;
                let current_alias = if is_last_hop {
                    ctx.end_alias.clone()
                } else {
                    format!("m{}", hop)
                };

                let prev_alias = if hop == 1 {
                    ctx.start_alias.clone()
                } else {
                    format!("m{}", hop - 1)
                };

                // FK-edge: prev_node.fk_col = current_node.id_col
                // Example: child.parent_id = m1.object_id
                // 🔧 PARAMETERIZED VIEW FIX: Use start_table_ref for FK-edge (self-referencing table)
                joins.push(Join {
                    table_name: start_table_ref.clone(), // Same table as start (self-referencing)
                    table_alias: current_alias.clone(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(prev_alias),
                                column: PropertyValue::Column(ctx.rel_from_col.clone()), // FK column (parent_id)
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(current_alias),
                                column: PropertyValue::Column(ctx.rel_to_col.clone()), // ID column (object_id)
                            }),
                        ],
                    }],
                    join_type: JoinType::Inner,
                    pre_filter: None,
                    from_id_column: None,
                    to_id_column: None,
                });
            }

            println!(
                "expand_fixed_length_joins_with_context [FK-EDGE]: FROM {} AS {}, {} JOINs",
                from_table,
                from_alias,
                joins.len()
            );

            (from_table, from_alias, joins)
        }
    }
}

/// Generate cycle prevention filters for fixed-length paths
///
/// Prevents nodes from being revisited in a path by ensuring:
/// 1. Start node != End node
/// 2. All intermediate relationship endpoints are unique
///
/// For *2: `a.user_id != c.user_id AND r1.followed_id != r2.follower_id`
/// For *3: `a.user_id != d.user_id AND r1.followed_id != r2.follower_id AND r2.followed_id != r3.follower_id`
///
/// # Arguments
/// * `exact_hops` - Number of relationship hops
/// * `start_id_col` - ID column name for start node
/// * `to_col` - "to" ID column name for relationships
/// * `from_col` - "from" ID column name for relationships
/// * `end_id_col` - ID column name for end node
/// * `start_alias` - Alias for start node (e.g., "a")
/// * `end_alias` - Alias for end node (e.g., "c")
///
/// # Returns
/// RenderExpr combining all cycle prevention conditions with AND
pub fn generate_cycle_prevention_filters(
    exact_hops: u32,
    start_id_col: &str,
    to_col: &str,
    from_col: &str,
    end_id_col: &str,
    start_alias: &str,
    end_alias: &str,
) -> Option<RenderExpr> {
    // Delegate to composite version with single-column IDs
    generate_cycle_prevention_filters_composite(
        exact_hops,
        &[start_id_col],
        &[to_col],
        &[from_col],
        &[end_id_col],
        start_alias,
        end_alias,
    )
}

/// Generate cycle prevention filters for fixed-length paths with composite IDs
///
/// Supports both simple and composite primary keys. For composite keys, generates
/// NOT (col1=col1 AND col2=col2 AND ...) conditions.
///
/// # Examples
///
/// Simple ID: `a.user_id != c.user_id`
///
/// Composite ID: `NOT (a.flight_date = c.flight_date AND a.flight_num = c.flight_num)`
///
/// # Arguments
/// * `exact_hops` - Number of relationship hops
/// * `start_id_cols` - ID column names for start node
/// * `to_cols` - "to" ID column names for relationships
/// * `from_cols` - "from" ID column names for relationships
/// * `end_id_cols` - ID column names for end node
/// * `start_alias` - Alias for start node (e.g., "a")
/// * `end_alias` - Alias for end node (e.g., "c")
///
/// # Returns
/// RenderExpr combining all cycle prevention conditions with AND
pub fn generate_cycle_prevention_filters_composite(
    exact_hops: u32,
    start_id_cols: &[&str],
    _to_cols: &[&str],
    _from_cols: &[&str],
    end_id_cols: &[&str],
    start_alias: &str,
    end_alias: &str,
) -> Option<RenderExpr> {
    use super::render_expr::{
        Column, Operator, OperatorApplication, PropertyAccess, RenderExpr, TableAlias,
    };

    if exact_hops == 0 {
        return None;
    }

    let mut filters = Vec::new();

    // Helper to generate composite equality check: NOT (col1=col1 AND col2=col2 AND ...)
    let generate_composite_not_equal = |left_alias: &str,
                                        left_cols: &[&str],
                                        right_alias: &str,
                                        right_cols: &[&str]|
     -> RenderExpr {
        if left_cols.len() == 1 {
            // Simple ID: a.user_id != c.user_id
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::NotEqual,
                operands: vec![
                    RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(left_alias.to_string()),
                        column: PropertyValue::Column(left_cols[0].to_string()),
                    }),
                    RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(right_alias.to_string()),
                        column: PropertyValue::Column(right_cols[0].to_string()),
                    }),
                ],
            })
        } else {
            // Composite ID: NOT (a.col1 = c.col1 AND a.col2 = c.col2 AND ...)
            let equality_checks: Vec<RenderExpr> = left_cols
                .iter()
                .zip(right_cols.iter())
                .map(|(left_col, right_col)| {
                    RenderExpr::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(left_alias.to_string()),
                                column: PropertyValue::Column(left_col.to_string()),
                            }),
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(right_alias.to_string()),
                                column: PropertyValue::Column(right_col.to_string()),
                            }),
                        ],
                    })
                })
                .collect();

            // Combine equality checks with AND
            let combined_equality = if equality_checks.len() == 1 {
                equality_checks.into_iter().next().unwrap()
            } else {
                equality_checks
                    .into_iter()
                    .reduce(|acc, expr| {
                        RenderExpr::OperatorApplicationExp(OperatorApplication {
                            operator: Operator::And,
                            operands: vec![acc, expr],
                        })
                    })
                    .unwrap()
            };

            // Wrap in NOT
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: Operator::Not,
                operands: vec![combined_equality],
            })
        }
    };

    // 1. Start node != End node (prevents returning to the starting point)
    filters.push(generate_composite_not_equal(
        start_alias,
        start_id_cols,
        end_alias,
        end_id_cols,
    ));

    // NOTE: We previously had cycle prevention for intermediate nodes, but it was WRONG.
    // The condition `r1.to_id != r2.from_id` blocks VALID paths because that's exactly
    // how paths connect (r1.to_id = r2.from_id is the JOIN condition).
    //
    // For proper cycle prevention (no node visited twice), we would need to track all
    // intermediate nodes and ensure they're all different from each other. This is
    // complex for inline JOINs (easy in recursive CTEs with path arrays).
    //
    // For now, we only prevent returning to the start node, which is the most common
    // cycle prevention requirement. Full cycle detection can be added later if needed.

    // Combine all filters with AND
    if filters.is_empty() {
        None
    } else if filters.len() == 1 {
        Some(filters.into_iter().next().unwrap())
    } else {
        // Combine with AND
        Some(
            filters
                .into_iter()
                .reduce(|acc, filter| {
                    RenderExpr::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::And,
                        operands: vec![acc, filter],
                    })
                })
                .unwrap(),
        )
    }
}
