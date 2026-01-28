//! Helper functions for MATCH clause processing.
//!
//! This module contains utility functions used during MATCH clause evaluation,
//! including property conversion, denormalization checks, and scan generation.

use std::sync::Arc;

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::open_cypher_parser::ast;
use crate::query_planner::logical_expr::{
    LogicalExpr, Operator, OperatorApplication, Property, PropertyAccess, TableAlias,
};
use crate::query_planner::logical_plan::{
    errors::LogicalPlanError, plan_builder::LogicalPlanResult, GraphRel, LogicalPlan,
    ShortestPathMode, VariableLengthSpec,
};
use crate::query_planner::plan_ctx::{PlanCtx, TableCtx};

/// Generate a scan operation for a node pattern.
///
/// This function creates a ViewScan using schema information from plan_ctx.
/// If the schema lookup fails, it returns an error since node labels should be validated
/// against the schema.
///
/// # Arguments
/// * `alias` - The variable alias for this node (e.g., "a", "user")
/// * `label` - Optional node label (e.g., Some("User"), None for unlabeled)
/// * `plan_ctx` - Planning context with schema information
///
/// # Returns
/// * `Ok(Arc<LogicalPlan>)` - ViewScan plan for the node
/// * `Err(LogicalPlanError)` - If node label not found in schema
pub fn generate_scan(
    alias: String,
    label: Option<String>,
    plan_ctx: &PlanCtx,
) -> LogicalPlanResult<Arc<LogicalPlan>> {
    log::debug!(
        "generate_scan called with alias='{}', label={:?}",
        alias,
        label
    );

    if let Some(label_str) = &label {
        // Handle $any wildcard for polymorphic edges
        if label_str == "$any" {
            log::debug!("Label is $any (polymorphic wildcard), creating Empty plan");
            return Ok(Arc::new(LogicalPlan::Empty));
        }

        log::debug!("Trying to create ViewScan for label '{}'", label_str);
        match super::try_generate_view_scan(&alias, label_str, plan_ctx)? {
            Some(view_scan) => {
                log::info!("✓ Successfully created ViewScan for label '{}'", label_str);
                Ok(view_scan)
            }
            None => {
                // ViewScan creation failed - this is an error (schema not found)
                Err(LogicalPlanError::NodeNotFound(label_str.to_string()))
            }
        }
    } else {
        log::debug!("No label provided - anonymous node, using Empty plan");
        // For anonymous nodes, use Empty plan
        // The node label will be inferred from relationship context during analysis
        Ok(Arc::new(LogicalPlan::Empty))
    }
}

/// Check if a plan contains a denormalized ViewScan.
///
/// Denormalized nodes are virtual nodes whose properties are stored on edge tables
/// rather than having their own dedicated table.
///
/// # Arguments
/// * `plan` - The logical plan to check
///
/// # Returns
/// * `true` if the plan is a ViewScan with `is_denormalized = true`
/// * `false` otherwise
pub fn is_denormalized_scan(plan: &Arc<LogicalPlan>) -> bool {
    let result = match plan.as_ref() {
        LogicalPlan::ViewScan(view_scan) => {
            crate::debug_print!(
                "is_denormalized_scan: ViewScan.is_denormalized = {} for table '{}'",
                view_scan.is_denormalized,
                view_scan.source_table
            );
            view_scan.is_denormalized
        }
        _ => {
            crate::debug_print!("is_denormalized_scan: Not a ViewScan, returning false");
            false
        }
    };
    crate::debug_print!("is_denormalized_scan: returning {}", result);
    result
}

/// Check if a node label is denormalized by looking up the schema.
///
/// Returns true if the node is denormalized (exists only in edge context).
///
/// # Arguments
/// * `label` - Optional node label to check
/// * `plan_ctx` - Planning context with schema information
///
/// # Returns
/// * `true` if the label exists in schema and is marked as denormalized
/// * `false` if label is None, not found, or not denormalized
pub fn is_label_denormalized(label: &Option<String>, plan_ctx: &PlanCtx) -> bool {
    if let Some(label_str) = label {
        let schema = plan_ctx.schema();
        if let Ok(node_schema) = schema.node_schema(label_str) {
            crate::debug_print!(
                "is_label_denormalized: label '{}' is_denormalized = {}",
                label_str,
                node_schema.is_denormalized
            );
            return node_schema.is_denormalized;
        }
    }
    crate::debug_print!(
        "is_label_denormalized: label {:?} not found or no label, returning false",
        label
    );
    false
}

/// Convert property patterns from MATCH clauses into filter expressions.
///
/// Property patterns like `{name: "Alice", age: 30}` are converted to
/// equality filter expressions like `n.name = "Alice" AND n.age = 30`.
///
/// # Arguments
/// * `props` - Vector of Property values from the AST
/// * `node_alias` - The table alias to use for property access
///
/// # Returns
/// * `Ok(Vec<LogicalExpr>)` - Vector of equality filter expressions
/// * `Err(FoundParamInProperties)` - If a parameter reference is found
pub fn convert_properties(
    props: Vec<Property>,
    node_alias: &str,
) -> LogicalPlanResult<Vec<LogicalExpr>> {
    let mut extracted_props: Vec<LogicalExpr> = vec![];

    for prop in props {
        match prop {
            Property::PropertyKV(property_kvpair) => {
                let op_app = LogicalExpr::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::Equal,
                    operands: vec![
                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(node_alias.to_string()),
                            column: PropertyValue::Column(property_kvpair.key.to_string()),
                        }),
                        property_kvpair.value,
                    ],
                });
                extracted_props.push(op_app);
            }
            Property::Param(_) => return Err(LogicalPlanError::FoundParamInProperties),
        }
    }

    Ok(extracted_props)
}

/// Convert all property patterns in plan_ctx to filter expressions.
///
/// Iterates through all table contexts in plan_ctx, extracts property patterns,
/// converts them to filter expressions, and appends them as filters.
///
/// # Arguments
/// * `plan_ctx` - Mutable planning context
///
/// # Returns
/// * `Ok(())` on success
/// * `Err(FoundParamInProperties)` if a parameter reference is found
pub fn convert_properties_to_operator_application(plan_ctx: &mut PlanCtx) -> LogicalPlanResult<()> {
    for (alias, table_ctx) in plan_ctx.get_mut_alias_table_ctx_map().iter_mut() {
        let mut extracted_props = convert_properties(table_ctx.get_and_clear_properties(), alias)?;
        table_ctx.append_filters(&mut extracted_props);
    }
    Ok(())
}

/// Compute the variable_length specification for a relationship pattern.
///
/// This normalizes VLP handling:
/// - `*1` on single-type relationship → None (same as regular)
/// - `*1` on multi-type relationship → Some(*1) (needed for polymorphic handling)
/// - Explicit VLP ranges → Some(spec)
/// - Multi-type without VLP → implicit *1
/// - Single-type without VLP → None
///
/// # Arguments
/// * `rel` - The relationship pattern from the AST
/// * `rel_labels` - The relationship type labels (if any)
pub fn compute_variable_length(
    rel: &ast::RelationshipPattern,
    rel_labels: &Option<Vec<String>>,
) -> Option<VariableLengthSpec> {
    let is_multi_type = rel_labels.as_ref().is_some_and(|labels| labels.len() > 1);

    if let Some(vlp) = rel.variable_length.clone() {
        let spec: VariableLengthSpec = vlp.into();
        let is_exact_one_hop = spec.min_hops == Some(1) && spec.max_hops == Some(1);
        if is_exact_one_hop && !is_multi_type {
            None // *1 single-type is same as regular relationship
        } else {
            Some(spec) // Keep *1 for multi-type or ranges
        }
    } else if is_multi_type {
        // Add implicit *1 for multi-type without VLP (polymorphic end node)
        Some(VariableLengthSpec {
            min_hops: Some(1),
            max_hops: Some(1),
        })
    } else {
        None // Single-type, no VLP
    }
}

/// Compute left/right node labels for relationship lookup based on direction.
///
/// For relationship type inference and polymorphic resolution, we need the
/// labels of nodes in the order [from_node, to_node] regardless of how they
/// appear in the pattern.
///
/// # Arguments
/// * `direction` - The relationship direction from the AST
/// * `start_label` - Label of the pattern's start node (left in pattern)
/// * `end_label` - Label of the pattern's end node (right in pattern)
///
/// # Returns
/// Tuple of (left_node_label, right_node_label) where:
/// - left is the "from" node in the relationship definition
/// - right is the "to" node in the relationship definition
pub fn compute_rel_node_labels(
    direction: &ast::Direction,
    start_label: &Option<String>,
    end_label: &Option<String>,
) -> (Option<String>, Option<String>) {
    match direction {
        ast::Direction::Outgoing => (start_label.clone(), end_label.clone()),
        ast::Direction::Incoming => (end_label.clone(), start_label.clone()),
        ast::Direction::Either => (start_label.clone(), end_label.clone()),
    }
}

/// Compute left/right connection aliases based on relationship direction.
///
/// Similar to `compute_rel_node_labels` but for string aliases rather than Option<String> labels.
/// Used to determine which node alias serves as the "from" and "to" connection for JOIN generation.
///
/// # Arguments
/// * `direction` - The relationship direction from the AST
/// * `start_alias` - Alias of the pattern's start node (left in pattern)
/// * `end_alias` - Alias of the pattern's end node (right in pattern)
///
/// # Returns
/// Tuple of (left_connection, right_connection) based on direction
pub fn compute_connection_aliases(
    direction: &ast::Direction,
    start_alias: &str,
    end_alias: &str,
) -> (String, String) {
    match direction {
        ast::Direction::Outgoing => (start_alias.to_string(), end_alias.to_string()),
        ast::Direction::Incoming => (end_alias.to_string(), start_alias.to_string()),
        ast::Direction::Either => (start_alias.to_string(), end_alias.to_string()),
    }
}

/// Register a node in the planning context's table context map.
///
/// This consolidates the common pattern of `plan_ctx.insert_table_ctx(alias, TableCtx::build(...))`
/// that appears multiple times in MATCH clause processing.
///
/// # Arguments
/// * `plan_ctx` - The planning context
/// * `node_alias` - The node's alias
/// * `node_label` - The node's label (if any)
/// * `node_props` - Properties from the node pattern
/// * `is_explicitly_named` - Whether the node has an explicit name in the query
pub fn register_node_in_context(
    plan_ctx: &mut PlanCtx,
    node_alias: &str,
    node_label: &Option<String>,
    node_props: Vec<Property>,
    is_explicitly_named: bool,
) {
    plan_ctx.insert_table_ctx(
        node_alias.to_string(),
        TableCtx::build(
            node_alias.to_string(),
            node_label.clone().map(|l| vec![l]),
            node_props,
            false, // is_rel
            is_explicitly_named,
        ),
    );
}

/// Generate a scan for a node, handling denormalized cases.
///
/// If the node label is denormalized (embedded in an edge table), returns an Empty scan.
/// Otherwise generates a regular ViewScan via `generate_scan()`.
///
/// # Arguments
/// * `node_alias` - The node's alias
/// * `node_label` - The node's label (if any)
/// * `plan_ctx` - The planning context
///
/// # Returns
/// Tuple of (scan_plan, is_denormalized)
pub fn generate_denormalization_aware_scan(
    node_alias: &str,
    node_label: &Option<String>,
    plan_ctx: &mut PlanCtx,
) -> LogicalPlanResult<(Arc<LogicalPlan>, bool)> {
    if is_label_denormalized(node_label, plan_ctx) {
        crate::debug_print!(
            "=== Node '{}' is DENORMALIZED, creating Empty scan ===",
            node_alias
        );
        Ok((Arc::new(LogicalPlan::Empty), true))
    } else {
        let scan = generate_scan(node_alias.to_string(), node_label.clone(), plan_ctx)?;
        let is_denorm = is_denormalized_scan(&scan);
        Ok((scan, is_denorm))
    }
}

/// Determine anchor connection for OPTIONAL MATCH patterns.
///
/// For OPTIONAL MATCH, we need to identify which node serves as the "anchor" -
/// the node that already exists in the base MATCH pattern. The other node is
/// the one being optionally matched.
///
/// # Arguments
/// * `plan_ctx` - The planning context
/// * `is_optional` - Whether this is an OPTIONAL MATCH pattern
/// * `left_conn` - The left connection alias
/// * `right_conn` - The right connection alias
///
/// # Returns
/// Some(alias) of the anchor connection, or None if not OPTIONAL MATCH or
/// if anchor cannot be determined
pub fn determine_optional_anchor(
    plan_ctx: &PlanCtx,
    is_optional: bool,
    left_conn: &str,
    right_conn: &str,
) -> Option<String> {
    if !is_optional {
        return None;
    }

    let alias_map = plan_ctx.get_alias_table_ctx_map();
    if alias_map.contains_key(left_conn) && !alias_map.contains_key(right_conn) {
        // left_conn exists, right_conn is new -> left_conn is anchor
        Some(left_conn.to_string())
    } else if alias_map.contains_key(right_conn) && !alias_map.contains_key(left_conn) {
        // right_conn exists, left_conn is new -> right_conn is anchor
        Some(right_conn.to_string())
    } else {
        // Both exist or neither exists - shouldn't happen in normal OPTIONAL MATCH
        crate::debug_print!(
            "WARN: OPTIONAL MATCH could not determine anchor: left_conn={}, right_conn={}",
            left_conn,
            right_conn
        );
        None
    }
}

/// Register a path variable in the PlanCtx with full TypedVariable::Path metadata.
///
/// This is extracted from the duplicated code in traverse_connected_pattern_with_mode.
/// It registers both the TypedVariable::Path and a TableCtx for backward compatibility.
///
/// # Arguments
/// * `plan_ctx` - The planning context to register the path variable in
/// * `path_var` - The name of the path variable
/// * `graph_rel` - The GraphRel node containing the path information
/// * `rel_alias` - The relationship alias
/// * `shortest_path_mode` - Whether this is a shortest path query
pub fn register_path_variable(
    plan_ctx: &mut PlanCtx,
    path_var: &str,
    graph_rel: &GraphRel,
    rel_alias: &str,
    shortest_path_mode: Option<&ShortestPathMode>,
) {
    // Extract length bounds from graph_rel.variable_length for TypedVariable::Path
    let length_bounds = graph_rel
        .variable_length
        .as_ref()
        .map(|vlp| (vlp.min_hops, vlp.max_hops));

    // First register TypedVariable::Path with full metadata
    plan_ctx.define_path(
        path_var.to_string(),
        Some(graph_rel.left_connection.clone()), // start_node
        Some(graph_rel.right_connection.clone()), // end_node
        Some(rel_alias.to_string()),             // relationship
        length_bounds,                           // length bounds from VLP spec
        shortest_path_mode.is_some(),            // is_shortest_path
    );

    // Then register TableCtx for backward compatibility with code that uses alias_table_ctx_map
    plan_ctx.insert_table_ctx(
        path_var.to_string(),
        TableCtx::build(
            path_var.to_string(),
            None,   // Path variables don't have labels
            vec![], // Path variables don't have properties
            false,  // Not a relationship
            true,   // Explicitly named by user
        ),
    );

    log::info!(
        "📍 Registered path variable '{}' with TypedVariable::Path (start={}, end={}, bounds={:?})",
        path_var,
        graph_rel.left_connection,
        graph_rel.right_connection,
        length_bounds
    );
}

/// Register a relationship in the plan context with connected node labels.
///
/// This consolidates the common pattern of:
/// 1. insert_table_ctx for the relationship
/// 2. set_connected_nodes for polymorphic resolution
/// 3. register_path_variable if path_variable is present
///
/// # Arguments
/// * `plan_ctx` - The planning context
/// * `rel_alias` - The relationship alias
/// * `rel_labels` - The relationship type labels (if any)
/// * `rel_properties` - Properties from the relationship pattern
/// * `is_named` - Whether the relationship has an explicit name in the query
/// * `left_node_label` - Label of the left/from node (for polymorphic resolution)
/// * `right_node_label` - Label of the right/to node (for polymorphic resolution)
/// * `graph_rel` - The GraphRel node (for path variable registration)
/// * `path_variable` - Optional path variable name
/// * `shortest_path_mode` - Whether this is a shortest path query
pub fn register_relationship_in_context(
    plan_ctx: &mut PlanCtx,
    rel_alias: &str,
    rel_labels: Option<Vec<String>>,
    rel_properties: Vec<Property>,
    is_named: bool,
    left_node_label: &Option<String>,
    right_node_label: &Option<String>,
    graph_rel: &GraphRel,
    path_variable: Option<&str>,
    shortest_path_mode: Option<&ShortestPathMode>,
) {
    // 1. Register the relationship TableCtx
    plan_ctx.insert_table_ctx(
        rel_alias.to_string(),
        TableCtx::build(
            rel_alias.to_string(),
            rel_labels,
            rel_properties,
            true, // is_relation
            is_named,
        ),
    );

    // 2. Set connected node labels for polymorphic relationship resolution
    if let Some(rel_table_ctx) = plan_ctx.get_mut_table_ctx_opt(rel_alias) {
        rel_table_ctx.set_connected_nodes(left_node_label.clone(), right_node_label.clone());
    }

    // 3. Register path variable if present
    if let Some(path_var) = path_variable {
        register_path_variable(plan_ctx, path_var, graph_rel, rel_alias, shortest_path_mode);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_planner::logical_expr::{Literal, PropertyKVPair};

    #[test]
    fn test_convert_properties_with_kv_pairs() {
        let properties = vec![
            Property::PropertyKV(PropertyKVPair {
                key: "name".to_string(),
                value: LogicalExpr::Literal(Literal::String("Alice".to_string())),
            }),
            Property::PropertyKV(PropertyKVPair {
                key: "age".to_string(),
                value: LogicalExpr::Literal(Literal::Integer(30)),
            }),
        ];

        let result = convert_properties(properties, "n").unwrap();
        assert_eq!(result.len(), 2);

        // Check first property conversion
        match &result[0] {
            LogicalExpr::OperatorApplicationExp(op) => {
                assert_eq!(op.operator, Operator::Equal);
                assert_eq!(op.operands.len(), 2);
            }
            _ => panic!("Expected OperatorApplicationExp"),
        }
    }

    #[test]
    fn test_convert_properties_with_param_returns_error() {
        let properties = vec![Property::Param("param1".to_string())];

        let result = convert_properties(properties, "n");
        assert!(result.is_err());
        match result.unwrap_err() {
            LogicalPlanError::FoundParamInProperties => {}
            _ => panic!("Expected FoundParamInProperties error"),
        }
    }

    #[test]
    fn test_convert_properties_empty_list() {
        let properties = vec![];
        let result = convert_properties(properties, "n").unwrap();
        assert_eq!(result.len(), 0);
    }
}
