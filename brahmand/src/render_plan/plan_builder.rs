use std::sync::Arc;
use crate::query_planner::logical_plan::LogicalPlan;
use crate::clickhouse_query_generator::variable_length_cte::{VariableLengthCteGenerator, ChainedJoinGenerator};

use super::errors::RenderBuildError;
use super::render_expr::{
    AggregateFnCall, ColumnAlias, Operator, OperatorApplication, RenderExpr, ScalarFnCall,
};
use super::{
    Cte, CteItems, FilterItems, FromTable, FromTableItem, GroupByExpressions, Join, JoinItems,
    LimitItem, OrderByItem, OrderByItems, RenderPlan, SelectItem, SelectItems, SkipItem, Union,
    UnionItems, ViewTableRef, view_table_ref::{view_ref_to_from_table, from_table_to_view_ref},
};

pub type RenderPlanBuilderResult<T> = Result<T, super::errors::RenderBuildError>;

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

/// Extract path variable name from variable-length relationship
/// Returns the path variable name (e.g., "p") if found
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

/// Rewrite expressions to use CTE columns instead of node references
/// Maps u1.user_id -> t.start_id, u2.user_id -> t.end_id, u1.name -> t.start_name, etc.
fn rewrite_expr_for_var_len_cte(
    expr: &super::render_expr::RenderExpr,
    left_alias: &str,
    right_alias: &str,
    path_variable: Option<&str>,
) -> super::render_expr::RenderExpr {
    use super::render_expr::{RenderExpr, PropertyAccess, TableAlias, Column};
    
    match expr {
        RenderExpr::ScalarFnCall(fn_call) => {
            // Check if this is a path function on a path variable
            if let Some(path_var) = path_variable {
                let fn_name_lower = fn_call.name.to_lowercase();
                
                // Check if the first argument is the path variable
                if fn_call.args.len() == 1 {
                    if let RenderExpr::TableAlias(alias) = &fn_call.args[0] {
                        if alias.0 == path_var {
                            // This is a path function - map to CTE columns
                            match fn_name_lower.as_str() {
                                "length" => {
                                    // length(p) -> hop_count
                                    return RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias("t".to_string()),
                                        column: Column("hop_count".to_string()),
                                    });
                                }
                                "nodes" => {
                                    // nodes(p) -> path_nodes array
                                    return RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias("t".to_string()),
                                        column: Column("path_nodes".to_string()),
                                    });
                                }
                                "relationships" => {
                                    // relationships(p) -> construct array from path
                                    // For now, return empty array - will enhance later
                                    use super::render_expr::ScalarFnCall as SF;
                                    return RenderExpr::ScalarFnCall(SF {
                                        name: "array".to_string(),
                                        args: vec![],
                                    });
                                }
                                _ => {
                                    // Not a recognized path function, fall through
                                }
                            }
                        }
                    }
                }
            }
            
            // Recursively rewrite function arguments
            use super::render_expr::ScalarFnCall as SF;
            let rewritten_args: Vec<_> = fn_call.args.iter()
                .map(|arg| rewrite_expr_for_var_len_cte(arg, left_alias, right_alias, path_variable))
                .collect();
            
            RenderExpr::ScalarFnCall(SF {
                name: fn_call.name.clone(),
                args: rewritten_args,
            })
        }
        RenderExpr::TableAlias(alias) => {
            // Check if this is a path variable reference
            if let Some(path_var) = path_variable {
                if alias.0 == path_var {
                    // This is a path variable - construct a path object from CTE columns
                    // Use ClickHouse map() function to create a structure with path data
                    use super::render_expr::{ScalarFnCall, Literal};
                    
                    return RenderExpr::ScalarFnCall(ScalarFnCall {
                        name: "map".to_string(),
                        args: vec![
                            // 'nodes' key
                            RenderExpr::Literal(Literal::String("nodes".to_string())),
                            // path_nodes value - convert to string
                            RenderExpr::ScalarFnCall(ScalarFnCall {
                                name: "toString".to_string(),
                                args: vec![
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias("t".to_string()),
                                        column: Column("path_nodes".to_string()),
                                    }),
                                ],
                            }),
                            // 'length' key
                            RenderExpr::Literal(Literal::String("length".to_string())),
                            // hop_count value - convert to string
                            RenderExpr::ScalarFnCall(ScalarFnCall {
                                name: "toString".to_string(),
                                args: vec![
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias("t".to_string()),
                                        column: Column("hop_count".to_string()),
                                    }),
                                ],
                            }),
                            // 'start' key
                            RenderExpr::Literal(Literal::String("start".to_string())),
                            // start_id value - convert to string
                            RenderExpr::ScalarFnCall(ScalarFnCall {
                                name: "toString".to_string(),
                                args: vec![
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias("t".to_string()),
                                        column: Column("start_id".to_string()),
                                    }),
                                ],
                            }),
                            // 'end' key
                            RenderExpr::Literal(Literal::String("end".to_string())),
                            // end_id value - convert to string
                            RenderExpr::ScalarFnCall(ScalarFnCall {
                                name: "toString".to_string(),
                                args: vec![
                                    RenderExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias("t".to_string()),
                                        column: Column("end_id".to_string()),
                                    }),
                                ],
                            }),
                        ],
                    });
                }
            }
            expr.clone()
        }
        RenderExpr::PropertyAccessExp(prop_access) => {
            let node_alias = &prop_access.table_alias.0;
            let property = &prop_access.column.0;
            
            // Check if this is referencing the left or right node
            if node_alias == left_alias {
                // Left node reference
                let cte_column = if property.ends_with("_id") || property == "user_id" || property == "id" {
                    // ID column -> start_id
                    "start_id".to_string()
                } else {
                    // Property column -> start_{property}
                    // Use the Cypher property name directly (not the database column name)
                    // The CTE already maps column_name to alias, so we use the alias here
                    format!("start_{}", property)
                };
                return RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("t".to_string()),
                    column: Column(cte_column),
                });
            } else if node_alias == right_alias {
                // Right node reference
                let cte_column = if property.ends_with("_id") || property == "user_id" || property == "id" {
                    // ID column -> end_id
                    "end_id".to_string()
                } else {
                    // Property column -> end_{property}
                    // Use the Cypher property name directly (not the database column name)
                    format!("end_{}", property)
                };
                return RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("t".to_string()),
                    column: Column(cte_column),
                });
            }
            expr.clone()
        }
        // Recursively rewrite operator expressions
        RenderExpr::OperatorApplicationExp(op) => {
            use super::render_expr::OperatorApplication;
            let rewritten_operands: Vec<_> = op.operands.iter()
                .map(|operand| rewrite_expr_for_var_len_cte(operand, left_alias, right_alias, path_variable))
                .collect();
            
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: op.operator,
                operands: rewritten_operands,
            })
        }
        // Recursively rewrite list expressions
        RenderExpr::List(items) => {
            let rewritten_items: Vec<_> = items.iter()
                .map(|item| rewrite_expr_for_var_len_cte(item, left_alias, right_alias, path_variable))
                .collect();
            
            RenderExpr::List(rewritten_items)
        }
        // Recursively rewrite aggregate function expressions
        RenderExpr::AggregateFnCall(agg) => {
            use super::render_expr::AggregateFnCall;
            let rewritten_args: Vec<_> = agg.args.iter()
                .map(|arg| rewrite_expr_for_var_len_cte(arg, left_alias, right_alias, path_variable))
                .collect();
            
            RenderExpr::AggregateFnCall(AggregateFnCall {
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
}

/// Get variable-length relationship info including node labels
/// Returns (left_cypher_alias, right_cypher_alias, left_node_label, right_node_label, rel_type)
fn get_variable_length_info(plan: &LogicalPlan) -> Option<(String, String, String, String, String)> {
    match plan {
        LogicalPlan::GraphRel(rel) if rel.variable_length.is_some() => {
            Some((
                rel.left_connection.clone(),  // left node's Cypher alias (u1)
                rel.right_connection.clone(), // right node's Cypher alias (u2)
                rel.left_connection.clone(),  // left node label (user) - same as alias for now
                rel.right_connection.clone(), // right node label (user)
                rel.alias.clone(),            // relationship type (FRIEND)
            ))
        }
        LogicalPlan::GraphNode(node) => get_variable_length_info(&node.input),
        LogicalPlan::Filter(filter) => get_variable_length_info(&filter.input),
        LogicalPlan::Projection(proj) => get_variable_length_info(&proj.input),
        LogicalPlan::GraphJoins(joins) => get_variable_length_info(&joins.input),
        LogicalPlan::GroupBy(gb) => get_variable_length_info(&gb.input),
        LogicalPlan::OrderBy(ob) => get_variable_length_info(&ob.input),
        LogicalPlan::Skip(skip) => get_variable_length_info(&skip.input),
        LogicalPlan::Limit(limit) => get_variable_length_info(&limit.input),
        LogicalPlan::Cte(cte) => get_variable_length_info(&cte.input),
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
    // Try to get the schema from the global state
    if let Some(schema_lock) = crate::server::GLOBAL_GRAPH_SCHEMA.get() {
        if let Ok(schema) = schema_lock.try_read() {
            // Get the node schema for this label
            if let Ok(node_schema) = schema.get_node_schema(node_label) {
                // Check if there's a property mapping in the schema
                // Note: NodeSchema doesn't have property_mappings, so we need to look at ViewConfig
                // For now, use the column names directly if they match
                for column in &node_schema.column_names {
                    // Simple heuristic: if column contains the property name, use it
                    if column.to_lowercase().contains(&property.to_lowercase()) {
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
                format!("end_{}", column)  // end_name, end_email, etc.
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

/// Categorize WHERE clause filters based on which node/relationship they reference
/// This is critical for shortest path queries where:
/// - Start node filters go in the base case (e.g., WHERE start_node.name = 'Alice')
/// - End node filters go in the outer CTE (e.g., WHERE end_node.name = 'Bob')
/// - Relationship filters go in base + recursive cases (e.g., WHERE rel.weight > 5)
fn categorize_filters(
    filter_expr: Option<&RenderExpr>,
    start_node_alias: &str,
    end_node_alias: &str,
    _rel_alias: &str, // For future relationship filtering
) -> CategorizedFilters {
    log::debug!("Categorizing filters for start alias '{}' and end alias '{}'", start_node_alias, end_node_alias);
    
    let mut result = CategorizedFilters {
        start_node_filters: None,
        end_node_filters: None,
        relationship_filters: None,
    };
    
    if filter_expr.is_none() {
        log::trace!("No filter expression provided");
        return result;
    }
    
    log::trace!("Filter expression: {:?}", filter_expr.unwrap());
    
    let filter = filter_expr.unwrap();
    
    // Helper to check if an expression references a specific alias
    fn references_alias(expr: &RenderExpr, target_alias: &str) -> bool {
        match expr {
            RenderExpr::PropertyAccessExp(prop) => {
                prop.table_alias.0 == target_alias
            }
            RenderExpr::OperatorApplicationExp(op) => {
                op.operands.iter().any(|operand| references_alias(operand, target_alias))
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
    
    for predicate in predicates {
        let refs_start = references_alias(&predicate, start_node_alias);
        let refs_end = references_alias(&predicate, end_node_alias);
        
        if refs_start && refs_end {
            // Filter references both nodes - can't categorize simply
            // For now, treat as start filter (will be in base case)
            start_filters.push(predicate);
        } else if refs_start {
            start_filters.push(predicate);
        } else if refs_end {
            end_filters.push(predicate);
        } else {
            // Doesn't reference nodes - might be relationship filter or constant
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
    
    log::trace!("Filter categorization result:");
    log::trace!("  Start filters: {:?}", result.start_node_filters);
    log::trace!("  End filters: {:?}", result.end_node_filters);
    log::trace!("  Rel filters: {:?}", result.relationship_filters);
    
    result
}

pub(crate) trait RenderPlanBuilder {
    fn extract_last_node_cte(&self) -> RenderPlanBuilderResult<Option<Cte>>;

    fn extract_final_filters(&self) -> RenderPlanBuilderResult<Option<RenderExpr>>;

    fn extract_ctes(&self, last_node_alias: &str) -> RenderPlanBuilderResult<Vec<Cte>>;
    
    fn extract_ctes_with_context(&self, last_node_alias: &str, context: &CteGenerationContext) -> RenderPlanBuilderResult<Vec<Cte>>;

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
                    // TODO: Extract properties from the projection
                    // For now, using empty properties - will be populated in a later step
                    let properties = vec![];
                    
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
                            None,                            // no start filters (old path, not used anymore)
                            None,                            // no end filters (old path, not used anymore)
                            graph_rel.path_variable.clone(), // path variable name
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
                        format!("SELECT from_node_id, to_node_id FROM {}", table)
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
        }
    }

    fn extract_ctes_with_context(&self, last_node_alias: &str, context: &CteGenerationContext) -> RenderPlanBuilderResult<Vec<Cte>> {
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
                    
                    // Extract and categorize filters for shortest path queries
                    // This is critical: start node filters go in base case, end node filters in outer CTE
                    // NEW: Get filter from graph_rel.where_predicate (populated by FilterIntoGraphRel optimizer)
                    let filter_expr = graph_rel.where_predicate.as_ref().and_then(|logical_expr| {
                        use std::convert::TryInto;
                        use crate::render_plan::render_expr::RenderExpr;
                        
                        // Convert LogicalExpr to RenderExpr
                        let render_expr: Result<RenderExpr, _> = logical_expr.clone().try_into();
                        match render_expr {
                            Ok(expr) => Some(expr),
                            Err(e) => {
                                log::warn!("Failed to convert LogicalExpr to RenderExpr: {:?}", e);
                                None
                            }
                        }
                    });
                    
                    log::debug!("GraphRel filter extraction: where_predicate exists = {}, converted filter exists = {}", 
                        graph_rel.where_predicate.is_some(), 
                        filter_expr.is_some());
                    if let Some(ref expr) = filter_expr {
                        log::trace!("Filter expression: {:?}", expr);
                    }
                    
                    let categorized = categorize_filters(
                        filter_expr.as_ref(),
                        &graph_rel.left_connection,
                        &graph_rel.right_connection,
                        &graph_rel.alias,
                    );
                    
                    // Convert filter expressions to SQL strings
                    let start_filter_sql = categorized.start_node_filters.as_ref().map(|f| 
                        render_expr_to_sql_for_cte(f, &graph_rel.left_connection, &graph_rel.right_connection)
                    );
                    let end_filter_sql = categorized.end_node_filters.as_ref().map(|f| 
                        render_expr_to_sql_for_cte(f, &graph_rel.left_connection, &graph_rel.right_connection)
                    );
                    
                    log::trace!("Converted filters to SQL:");
                    log::trace!("  Start filter SQL: {:?}", start_filter_sql);
                    log::trace!("  End filter SQL: {:?}", end_filter_sql);
                    
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
                            properties,  // Properties from context!
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
                            properties,  // Properties from context!
                            graph_rel.shortest_path_mode.clone().map(|m| m.into()), // convert logical plan mode to SQL mode
                            start_filter_sql,  // Start node filters for base case
                            end_filter_sql,    // End node filters for outer CTE
                            graph_rel.path_variable.clone(),  // Path variable name
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
                            format!("SELECT from_node_id, to_node_id FROM {}", table)
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
                filter.input.extract_ctes_with_context(last_node_alias, &new_context)
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
                let items = projection.items.iter().map(|item| {
                    let expr = item.expression.clone().try_into()?;
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
                // Extract FROM from the input, but attach the GraphNode's alias
                let mut from_ref = from_table_to_view_ref(graph_node.input.extract_from()?);
                // Attach the Cypher variable name as the alias
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
            LogicalPlan::Filter(filter) => Some(filter.predicate.clone().try_into()?),
            LogicalPlan::Projection(projection) => projection.input.extract_filters()?,
            LogicalPlan::GroupBy(group_by) => group_by.input.extract_filters()?,
            LogicalPlan::OrderBy(order_by) => order_by.input.extract_filters()?,
            LogicalPlan::Skip(skip) => skip.input.extract_filters()?,
            LogicalPlan::Limit(limit) => limit.input.extract_filters()?,
            LogicalPlan::Cte(cte) => cte.input.extract_filters()?,
            LogicalPlan::GraphJoins(graph_joins) => graph_joins.input.extract_filters()?,
            LogicalPlan::Union(_) => None,
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
            LogicalPlan::Filter(filter) => Some(filter.predicate.clone().try_into()?),
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
            LogicalPlan::GraphJoins(graph_joins) => graph_joins
                .joins
                .iter()
                .cloned()
                .map(Join::try_from)
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
                .map(RenderExpr::try_from)
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
                .map(OrderByItem::try_from)
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
        });
        
        // First pass: analyze what properties are needed
        let context = analyze_property_requirements(self);
        
        let mut extracted_ctes: Vec<Cte> = vec![];
        let final_from: Option<FromTable>;
        let final_filters: Option<RenderExpr>;

        if let Some(last_node_cte) = self.extract_last_node_cte()? {
            let last_node_alias = last_node_cte
                .cte_name
                .split('_')
                .nth(1)
                .ok_or(RenderBuildError::MalformedCTEName)?;

            // Second pass: generate CTEs with full context
            extracted_ctes = self.extract_ctes_with_context(last_node_alias, &context)?;
            
            // Check if we have a variable-length CTE (it will be a recursive RawSql CTE)
            let has_variable_length_cte = extracted_ctes.iter().any(|cte| 
                cte.is_recursive && matches!(&cte.content, super::CteContent::RawSql(_))
            );
            
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
                final_filters = None; // Filters are handled within the CTE
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
            extracted_ctes = self.extract_ctes_with_context("_", &context)?;
            
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
                final_filters = None; // Filters are handled within the CTE
            } else {
                // Normal case: no CTEs, extract FROM and filters normally
                final_from = self.extract_from()?;
                final_filters = self.extract_filters()?;
            }
        }

        let mut final_select_items = self.extract_select_items()?;
        
        // If we have a variable-length relationship, rewrite SELECT items to use CTE columns
        if let Some((left_alias, right_alias)) = has_variable_length_rel(self) {
            let path_var = get_path_variable(self);
            final_select_items = final_select_items.into_iter().map(|item| {
                let new_expr = rewrite_expr_for_var_len_cte(
                    &item.expression, 
                    &left_alias, 
                    &right_alias,
                    path_var.as_deref()
                );
                SelectItem {
                    expression: new_expr,
                    col_alias: item.col_alias,
                }
            }).collect();
        }

        let mut extracted_joins = self.extract_joins()?;
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
