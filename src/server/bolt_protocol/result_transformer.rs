//! Result Transformer for Neo4j Bolt Protocol
//!
//! This module transforms ClickHouse query results into Neo4j Bolt 5.x graph objects.
//! It handles the conversion of flat result rows into typed Node, Relationship, and Path
//! structures with elementId support.
//!
//! # Architecture
//!
//! 1. **Metadata Extraction**: Analyze query logical plan and variable registry to determine
//!    which return items are graph entities (Node/Relationship/Path) vs scalars
//!
//! 2. **Result Transformation**: Convert flat result rows into appropriate graph objects
//!    or scalar values based on metadata
//!
//! 3. **ElementId Generation**: Use the element_id module to create Neo4j-compatible
//!    element IDs for all graph entities
//!
//! # Example
//!
//! ```text
//! Query: MATCH (n:User) WHERE n.user_id = 1 RETURN n
//!
//! Metadata: [ReturnItemMetadata { field_name: "n", item_type: Node { labels: ["User"] } }]
//!
//! Input Row: {"n.user_id": 123, "n.name": "Alice", "n.email": "alice@example.com"}
//!
//! Output: Node {
//!     id: 0,
//!     labels: ["User"],
//!     properties: {"user_id": 123, "name": "Alice", "email": "alice@example.com"},
//!     element_id: "User:123"
//! }
//! ```

use crate::{
    graph_catalog::{
        element_id::{generate_node_element_id, generate_relationship_element_id},
        graph_schema::GraphSchema,
        schema_types::SchemaType,
    },
    query_planner::{
        logical_expr::{Direction, LogicalExpr},
        logical_plan::{LogicalPlan, Projection},
        plan_ctx::PlanCtx,
        typed_variable::{TypedVariable, VariableSource},
    },
    server::bolt_protocol::{
        graph_objects::{Node, Path, Relationship},
        messages::BoltValue,
    },
};
use serde_json::Value;
use std::collections::HashMap;

/// Metadata about a single return item
#[derive(Debug, Clone)]
pub struct ReturnItemMetadata {
    /// Field name in results (e.g., "n", "r", "n.name")
    pub field_name: String,
    /// Type of return item
    pub item_type: ReturnItemType,
}

/// Type of a return item
#[derive(Debug, Clone)]
pub enum ReturnItemType {
    /// Whole node entity - needs transformation to Node struct
    Node {
        /// Node labels from variable registry
        labels: Vec<String>,
    },
    /// Whole relationship entity - needs transformation to Relationship struct
    Relationship {
        /// Relationship types from variable registry
        rel_types: Vec<String>,
        /// From node label (for polymorphic relationships)
        from_label: Option<String>,
        /// To node label (for polymorphic relationships)
        to_label: Option<String>,
        /// Relationship direction: "Outgoing", "Incoming", or "Either"
        /// Used to determine if start/end nodes should be swapped for undirected queries
        direction: Option<String>,
    },
    /// Path variable - needs transformation to Path struct
    Path {
        /// Start node alias (for looking up component data)
        start_alias: Option<String>,
        /// End node alias
        end_alias: Option<String>,
        /// Relationship alias
        rel_alias: Option<String>,
        /// Start node labels (from variable registry lookup)
        start_labels: Vec<String>,
        /// End node labels (from variable registry lookup)
        end_labels: Vec<String>,
        /// Relationship types (from variable registry lookup)
        rel_types: Vec<String>,
        /// Whether this is a VLP (variable-length path)
        is_vlp: bool,
        /// Relationship direction: "Outgoing", "Incoming", or "Either"
        /// Used to determine if nodes should be swapped in undirected paths
        direction: Option<String>,
    },
    /// Neo4j id() function - compute encoded ID from element_id at result time
    IdFunction {
        /// The variable alias (e.g., "u" in id(u))
        alias: String,
        /// Labels for the entity (if known)
        labels: Vec<String>,
    },
    /// Scalar value (property access, expression, aggregate) - return as-is
    Scalar,
}

/// Extract return metadata from logical plan and plan context
///
/// This function analyzes the final Projection node in the logical plan and
/// looks up each variable in the plan context's variable registry to determine
/// whether it's a graph entity or scalar.
///
/// # Arguments
///
/// * `logical_plan` - The logical plan (should contain a Projection)
/// * `plan_ctx` - The plan context with variable registry
///
/// # Returns
///
/// Vector of metadata for each return item, in the same order as the projection items
///
/// # Example
///
/// ```text
/// Query: MATCH (n:User) RETURN n, n.name
///
/// Result: [
///     ReturnItemMetadata { field_name: "n", item_type: Node { labels: ["User"] } },
///     ReturnItemMetadata { field_name: "n.name", item_type: Scalar }
/// ]
/// ```

/// Parse composite relationship key "FOLLOWS::User::User" into ("FOLLOWS", Some("User"), Some("User"))
/// Simple keys like "FOLLOWS" return ("FOLLOWS", None, None)
fn parse_composite_rel_key(key: &str) -> (String, Option<String>, Option<String>) {
    let parts: Vec<&str> = key.split("::").collect();
    match parts.as_slice() {
        [rel_type, from_label, to_label] => (
            rel_type.to_string(),
            Some(from_label.to_string()),
            Some(to_label.to_string()),
        ),
        _ => (key.to_string(), None, None),
    }
}

/// Strip composite key suffixes from rel_types and infer from/to labels if missing.
/// Converts ["FOLLOWS::User::User"] → (["FOLLOWS"], inferred_from, inferred_to)
fn strip_composite_rel_types(
    rel_types: &[String],
    existing_from: Option<String>,
    existing_to: Option<String>,
) -> (Vec<String>, Option<String>, Option<String>) {
    let mut stripped_types = Vec::new();
    let mut inferred_from = existing_from;
    let mut inferred_to = existing_to;

    for rt in rel_types {
        let (base_type, from_label, to_label) = parse_composite_rel_key(rt);
        stripped_types.push(base_type);
        if inferred_from.is_none() {
            inferred_from = from_label;
        }
        if inferred_to.is_none() {
            inferred_to = to_label;
        }
    }

    (stripped_types, inferred_from, inferred_to)
}

/// Helper function to find the direction of a relationship in a GraphRel within the logical plan
fn find_relationship_direction(plan: &LogicalPlan, rel_alias: &str) -> Option<String> {
    match plan {
        LogicalPlan::GraphRel(graph_rel) => {
            if graph_rel.alias == rel_alias {
                // Found the relationship - return its direction as a string
                let direction_str = match graph_rel.direction {
                    Direction::Outgoing => "Outgoing",
                    Direction::Incoming => "Incoming",
                    Direction::Either => "Either",
                };
                return Some(direction_str.to_string());
            }
            // Recursively search in child plans
            if let Some(dir) = find_relationship_direction(&graph_rel.left, rel_alias) {
                return Some(dir);
            }
            if let Some(dir) = find_relationship_direction(&graph_rel.center, rel_alias) {
                return Some(dir);
            }
            if let Some(dir) = find_relationship_direction(&graph_rel.right, rel_alias) {
                return Some(dir);
            }
            None
        }
        LogicalPlan::Projection(proj) => find_relationship_direction(&proj.input, rel_alias),
        LogicalPlan::Filter(filter) => find_relationship_direction(&filter.input, rel_alias),
        LogicalPlan::WithClause(with_clause) => {
            find_relationship_direction(&with_clause.input, rel_alias)
        }
        LogicalPlan::CartesianProduct(cart) => {
            if let Some(dir) = find_relationship_direction(&cart.left, rel_alias) {
                return Some(dir);
            }
            find_relationship_direction(&cart.right, rel_alias)
        }
        LogicalPlan::Union(union) => {
            for input in &union.inputs {
                if let Some(dir) = find_relationship_direction(input, rel_alias) {
                    return Some(dir);
                }
            }
            None
        }
        LogicalPlan::GroupBy(gb) => find_relationship_direction(&gb.input, rel_alias),
        LogicalPlan::Limit(lim) => find_relationship_direction(&lim.input, rel_alias),
        LogicalPlan::OrderBy(ob) => find_relationship_direction(&ob.input, rel_alias),
        LogicalPlan::Skip(skip) => find_relationship_direction(&skip.input, rel_alias),
        LogicalPlan::Cte(cte) => find_relationship_direction(&cte.input, rel_alias),
        LogicalPlan::PageRank(_pr) => None, // PageRank doesn't have input in our case
        LogicalPlan::Unwind(uw) => find_relationship_direction(&uw.input, rel_alias),
        LogicalPlan::GraphNode(gn) => find_relationship_direction(&gn.input, rel_alias),
        LogicalPlan::GraphJoins(gj) => find_relationship_direction(&gj.input, rel_alias),
        LogicalPlan::ViewScan(_) | LogicalPlan::Empty => None,
    }
}

pub fn extract_return_metadata(
    logical_plan: &LogicalPlan,
    plan_ctx: &PlanCtx,
) -> Result<Vec<ReturnItemMetadata>, String> {
    // Find the final Projection node
    let projection = find_final_projection(logical_plan)?;

    let mut metadata = Vec::new();

    for proj_item in &projection.items {
        let field_name = get_field_name(proj_item);

        // Debug: log what we're seeing
        log::debug!(
            "Projection item: field_name={}, expr={:?}",
            field_name,
            proj_item.expression
        );

        // Check if expression is a simple variable reference
        let item_type = match &proj_item.expression {
            LogicalExpr::TableAlias(table_alias) => {
                // Lookup in plan_ctx.variables
                match plan_ctx.lookup_variable(&table_alias.to_string()) {
                    Some(TypedVariable::Node(node_var)) => {
                        if node_var.labels.is_empty()
                            && !matches!(node_var.source, VariableSource::Match)
                        {
                            // Non-MATCH node with empty labels is a computed alias
                            // (e.g., pattern comprehension result), treat as Scalar
                            log::debug!(
                                "Treating '{}' as Scalar (non-MATCH node with empty labels)",
                                table_alias
                            );
                            ReturnItemType::Scalar
                        } else {
                            // MATCH nodes with empty labels are unlabeled nodes (e.g., MATCH (n))
                            // — still need graph-object transformation
                            ReturnItemType::Node {
                                labels: node_var.labels.clone(),
                            }
                        }
                    }
                    Some(TypedVariable::Relationship(rel_var)) => {
                        // Get direction: first check if it's set in the variable, otherwise look it up in the logical plan
                        let direction = if rel_var.direction.is_some() {
                            rel_var.direction.clone()
                        } else {
                            // Try to find direction from GraphRel in the logical plan
                            find_relationship_direction(logical_plan, &table_alias.to_string())
                        };

                        // Strip composite key suffixes: "FOLLOWS::User::User" → "FOLLOWS"
                        // and infer from/to labels if not already set
                        let (stripped_types, inferred_from, inferred_to) =
                            strip_composite_rel_types(
                                &rel_var.rel_types,
                                rel_var.from_node_label.clone(),
                                rel_var.to_node_label.clone(),
                            );

                        ReturnItemType::Relationship {
                            rel_types: stripped_types,
                            from_label: inferred_from,
                            to_label: inferred_to,
                            direction,
                        }
                    }
                    Some(TypedVariable::Path(path_var)) => {
                        // VLP has length_bounds set, fixed single-hop doesn't
                        let is_vlp = path_var.length_bounds.is_some();

                        // Look up component types from their aliases in plan_ctx
                        let mut start_labels = path_var
                            .start_node
                            .as_ref()
                            .and_then(|alias| plan_ctx.lookup_variable(alias))
                            .and_then(|tv| tv.as_node())
                            .map(|nv| nv.labels.clone())
                            .unwrap_or_default();

                        let mut end_labels = path_var
                            .end_node
                            .as_ref()
                            .and_then(|alias| plan_ctx.lookup_variable(alias))
                            .and_then(|tv| tv.as_node())
                            .map(|nv| nv.labels.clone())
                            .unwrap_or_default();

                        let mut rel_types = path_var
                            .relationship
                            .as_ref()
                            .and_then(|alias| plan_ctx.lookup_variable(alias))
                            .and_then(|tv| tv.as_relationship())
                            .map(|rv| rv.rel_types.clone())
                            .unwrap_or_default();

                        // Parse composite relationship keys: "AUTHORED::User::Post" -> ("AUTHORED", "User", "Post")
                        // This happens when relationships use composite keys for disambiguation
                        let mut inferred_from_labels = Vec::new();
                        let mut inferred_to_labels = Vec::new();
                        let mut actual_rel_types = Vec::new();
                        for rt in &rel_types {
                            let (base_type, from_label, to_label) = parse_composite_rel_key(rt);
                            actual_rel_types.push(base_type);
                            if let Some(fl) = from_label {
                                inferred_from_labels.push(fl);
                            }
                            if let Some(tl) = to_label {
                                inferred_to_labels.push(tl);
                            }
                        }

                        // Use parsed relationship types
                        rel_types = actual_rel_types;

                        // If we have relationship type but missing node labels (anonymous nodes),
                        // try to infer labels from the relationship schema or parsed composite key
                        if !rel_types.is_empty()
                            && (start_labels.is_empty() || end_labels.is_empty())
                        {
                            // First try inferred labels from composite key
                            if start_labels.is_empty() && !inferred_from_labels.is_empty() {
                                start_labels = vec![inferred_from_labels[0].clone()];
                            }
                            if end_labels.is_empty() && !inferred_to_labels.is_empty() {
                                end_labels = vec![inferred_to_labels[0].clone()];
                            }

                            // Fallback to relationship variable metadata
                            if start_labels.is_empty() || end_labels.is_empty() {
                                if let Some(rel_var) = path_var
                                    .relationship
                                    .as_ref()
                                    .and_then(|alias| plan_ctx.lookup_variable(alias))
                                    .and_then(|tv| tv.as_relationship())
                                {
                                    // Use from_node_label and to_node_label if available
                                    if start_labels.is_empty() {
                                        if let Some(from_label) = &rel_var.from_node_label {
                                            start_labels = vec![from_label.clone()];
                                        }
                                    }
                                    if end_labels.is_empty() {
                                        if let Some(to_label) = &rel_var.to_node_label {
                                            end_labels = vec![to_label.clone()];
                                        }
                                    }
                                }
                            }
                        }

                        // Get direction from the relationship variable
                        let direction = path_var
                            .relationship
                            .as_ref()
                            .and_then(|alias| plan_ctx.lookup_variable(alias))
                            .and_then(|tv| tv.as_relationship())
                            .and_then(|rel_var| rel_var.direction.clone())
                            .or_else(|| {
                                // Try to find direction from GraphRel in the logical plan
                                path_var.relationship.as_ref().and_then(|alias| {
                                    find_relationship_direction(logical_plan, alias)
                                })
                            });

                        ReturnItemType::Path {
                            start_alias: path_var.start_node.clone(),
                            end_alias: path_var.end_node.clone(),
                            rel_alias: path_var.relationship.clone(),
                            start_labels,
                            end_labels,
                            rel_types,
                            is_vlp,
                            direction,
                        }
                    }
                    _ => {
                        // Scalar variable or not found
                        ReturnItemType::Scalar
                    }
                }
            }
            LogicalExpr::PropertyAccessExp(prop) => {
                // Check for node wildcard expansion (a.* means all properties of node a)
                let is_wildcard = matches!(
                    &prop.column,
                    crate::graph_catalog::expression_parser::PropertyValue::Column(col) if col == "*"
                );

                if is_wildcard {
                    // This is a node wildcard - look up the table alias as a node
                    let var_name = prop.table_alias.to_string();
                    log::debug!(
                        "PropertyAccessExp wildcard: looking up '{}' in plan_ctx",
                        var_name
                    );
                    match plan_ctx.lookup_variable(&var_name) {
                        Some(TypedVariable::Node(node_var)) => {
                            log::debug!("  Found Node with labels: {:?}", node_var.labels);
                            ReturnItemType::Node {
                                labels: node_var.labels.clone(),
                            }
                        }
                        Some(TypedVariable::Relationship(rel_var)) => {
                            log::debug!("  Found Relationship with types: {:?}", rel_var.rel_types);
                            // Get direction: first check if it's set in the variable, otherwise look it up in the logical plan
                            let direction = if rel_var.direction.is_some() {
                                rel_var.direction.clone()
                            } else {
                                find_relationship_direction(logical_plan, &var_name)
                            };

                            // Strip composite key suffixes: "FOLLOWS::User::User" → "FOLLOWS"
                            let (stripped_types, inferred_from, inferred_to) =
                                strip_composite_rel_types(
                                    &rel_var.rel_types,
                                    rel_var.from_node_label.clone(),
                                    rel_var.to_node_label.clone(),
                                );

                            ReturnItemType::Relationship {
                                rel_types: stripped_types,
                                from_label: inferred_from,
                                to_label: inferred_to,
                                direction,
                            }
                        }
                        _ => ReturnItemType::Scalar,
                    }
                } else {
                    // Regular property access → Scalar
                    ReturnItemType::Scalar
                }
            }
            LogicalExpr::ScalarFnCall(fn_call) => {
                // Check for id() function - needs special handling
                if fn_call.name.eq_ignore_ascii_case("id") && fn_call.args.len() == 1 {
                    // Extract alias from argument
                    if let LogicalExpr::TableAlias(alias) = &fn_call.args[0] {
                        let alias_str = alias.to_string();
                        // Look up labels from plan_ctx
                        let labels = match plan_ctx.lookup_variable(&alias_str) {
                            Some(TypedVariable::Node(node_var)) => node_var.labels.clone(),
                            Some(TypedVariable::Relationship(rel_var)) => rel_var.rel_types.clone(),
                            _ => vec![],
                        };
                        log::debug!(
                            "IdFunction detected: id({}) with labels {:?}",
                            alias_str,
                            labels
                        );
                        ReturnItemType::IdFunction {
                            alias: alias_str,
                            labels,
                        }
                    } else {
                        ReturnItemType::Scalar
                    }
                } else {
                    // Other function calls → Scalar
                    ReturnItemType::Scalar
                }
            }
            _ => {
                // Other expressions → Scalar
                ReturnItemType::Scalar
            }
        };

        metadata.push(ReturnItemMetadata {
            field_name,
            item_type,
        });
    }

    Ok(metadata)
}

/// Find the final Projection node in the logical plan
///
/// Traverses through OrderBy, Limit, Skip, GraphJoins, Union wrappers to find the underlying Projection
fn find_final_projection(plan: &LogicalPlan) -> Result<&Projection, String> {
    match plan {
        LogicalPlan::Projection(proj) => Ok(proj),
        LogicalPlan::OrderBy(order_by) => find_final_projection(&order_by.input),
        LogicalPlan::Limit(limit) => find_final_projection(&limit.input),
        LogicalPlan::Skip(skip) => find_final_projection(&skip.input),
        LogicalPlan::GraphJoins(joins) => find_final_projection(&joins.input),
        LogicalPlan::Union(union_plan) => {
            // For Union plans, check the first branch for Projection
            if let Some(first) = union_plan.inputs.first() {
                find_final_projection(first)
            } else {
                Err("Union plan has no inputs".to_string())
            }
        }
        _ => Err(format!(
            "No projection found in plan, got: {:?}",
            std::mem::discriminant(plan)
        )),
    }
}

/// Extract field name from projection item
///
/// Uses explicit alias if present, otherwise derives from expression
fn get_field_name(proj_item: &crate::query_planner::logical_plan::ProjectionItem) -> String {
    // Use alias if present
    if let Some(alias) = &proj_item.col_alias {
        return alias.to_string();
    }

    // Otherwise extract from expression
    match &proj_item.expression {
        LogicalExpr::TableAlias(table_alias) => table_alias.to_string(),
        LogicalExpr::PropertyAccessExp(prop) => {
            // PropertyValue doesn't implement Display, so convert manually
            match &prop.column {
                crate::graph_catalog::expression_parser::PropertyValue::Column(s) => {
                    format!("{}.{}", prop.table_alias, s)
                }
                crate::graph_catalog::expression_parser::PropertyValue::Expression(expr) => {
                    format!("{}.{}", prop.table_alias, expr)
                }
            }
        }
        _ => "?column?".to_string(),
    }
}

/// Transform a single result row using return metadata
///
/// Converts flat ClickHouse result row into Neo4j Bolt format, transforming
/// graph entities into Node/Relationship/Path structs and leaving scalars as-is.
///
/// # Arguments
///
/// * `row` - Raw result row as HashMap (column_name → value)
/// * `metadata` - Metadata for each return item
/// * `schema` - Graph schema for ID column lookup
///
/// # Returns
///
/// Vector of Values in the same order as metadata, where graph entities are
/// packstream-encoded Node/Relationship/Path structs
///
/// # Example
///
/// ```text
/// Input row: {"n.user_id": 123, "n.name": "Alice"}
/// Metadata: [Node { labels: ["User"] }]
///
/// Output: [Node { id: 0, labels: ["User"], properties: {...}, element_id: "User:123" }]
/// ```
pub fn transform_row(
    row: HashMap<String, Value>,
    metadata: &[ReturnItemMetadata],
    schema: &GraphSchema,
    id_mapper: &mut super::id_mapper::IdMapper,
) -> Result<Vec<BoltValue>, String> {
    log::info!(
        "🔍 transform_row called with {} columns: {:?}",
        row.len(),
        row.keys().collect::<Vec<_>>()
    );
    log::trace!(
        "🔍 Metadata has {} items: {:?}",
        metadata.len(),
        metadata
            .iter()
            .map(|m| (&m.field_name, &m.item_type))
            .collect::<Vec<_>>()
    );

    // Check for multi-label scan results (has {alias}_label, {alias}_id, {alias}_properties columns)
    if let Some(transformed) = try_transform_multi_label_row(&row, metadata, id_mapper)? {
        return Ok(transformed);
    }

    let mut result = Vec::new();

    for meta in metadata {
        match &meta.item_type {
            ReturnItemType::Node { labels } => {
                // Strip ".*" suffix from field_name if present (wildcard expansion)
                let var_name = meta
                    .field_name
                    .strip_suffix(".*")
                    .unwrap_or(&meta.field_name);
                let mut node = transform_to_node(&row, var_name, labels, schema)?;
                // Assign session-scoped integer ID from id_mapper
                node.id = id_mapper.get_or_assign(&node.element_id);
                // Use the Node's packstream encoding
                let packstream_bytes = node.to_packstream();
                result.push(BoltValue::PackstreamBytes(packstream_bytes));
            }
            ReturnItemType::Relationship {
                rel_types,
                from_label,
                to_label,
                direction,
            } => {
                // Strip ".*" suffix from field_name if present (wildcard expansion)
                let var_name = meta
                    .field_name
                    .strip_suffix(".*")
                    .unwrap_or(&meta.field_name);
                let mut rel = transform_to_relationship(
                    &row,
                    var_name,
                    rel_types,
                    from_label.as_deref(),
                    to_label.as_deref(),
                    schema,
                )?;
                // If relationship direction is "Incoming" or "Either", we may need to swap start/end nodes
                // This handles undirected patterns like (a)--(b) where we need to match query semantics
                if let Some(dir) = direction {
                    if dir == "Incoming" || dir == "Either" {
                        // Swap start and end nodes to match query pattern semantics
                        std::mem::swap(
                            &mut rel.start_node_element_id,
                            &mut rel.end_node_element_id,
                        );
                        log::debug!(
                            "Swapped relationship direction for undirected pattern: {} (was {}->{})",
                            var_name,
                            rel.end_node_element_id,
                            rel.start_node_element_id
                        );
                    }
                }
                // Assign session-scoped integer IDs from id_mapper
                rel.id = id_mapper.get_or_assign(&rel.element_id);
                rel.start_node_id = id_mapper.get_or_assign(&rel.start_node_element_id);
                rel.end_node_id = id_mapper.get_or_assign(&rel.end_node_element_id);
                // Use the Relationship's packstream encoding
                let packstream_bytes = rel.to_packstream();
                result.push(BoltValue::PackstreamBytes(packstream_bytes));
            }
            ReturnItemType::Path {
                start_alias,
                end_alias,
                rel_alias,
                start_labels,
                end_labels,
                rel_types,
                is_vlp,
                direction,
            } => {
                // Transform fixed-hop path to Neo4j Path structure
                // For now, we support fixed single-hop paths: (a)-[r]->(b)
                //
                // The SQL returns: tuple('fixed_path', start_alias, end_alias, rel_alias)
                // We use the metadata (labels, types) from query planning

                if *is_vlp {
                    // VLP multi-type paths: the SQL returns a tuple column with all path data
                    let mut path = transform_vlp_path(&row, &meta.field_name, schema)?;

                    // Assign session-scoped integer IDs
                    for node in &mut path.nodes {
                        node.id = id_mapper.get_or_assign(&node.element_id);
                    }
                    for rel in &mut path.relationships {
                        rel.id = id_mapper.get_or_assign(&rel.element_id);
                        rel.start_node_id = id_mapper.get_or_assign(&rel.start_node_element_id);
                        rel.end_node_id = id_mapper.get_or_assign(&rel.end_node_element_id);
                    }

                    let packstream_bytes = path.to_packstream();
                    result.push(BoltValue::PackstreamBytes(packstream_bytes));
                    continue;
                }

                // For fixed-hop paths, construct path using known metadata
                let mut path = transform_to_path(
                    &row,
                    &meta.field_name,
                    start_alias.as_deref(),
                    end_alias.as_deref(),
                    rel_alias.as_deref(),
                    start_labels,
                    end_labels,
                    rel_types,
                    schema,
                    metadata,
                )?;

                // If relationship direction is "Incoming" or "Either", swap nodes in path
                // This handles undirected patterns like (a)--(b) where we need to match query semantics
                if let Some(dir) = direction {
                    if dir == "Incoming" || dir == "Either" {
                        // Swap start and end nodes in the path to match query pattern semantics
                        if path.nodes.len() >= 2 {
                            path.nodes.swap(0, 1);
                            log::debug!(
                                "Swapped path nodes for undirected pattern (nodes now: {} -> {})",
                                path.nodes[0]
                                    .labels
                                    .iter()
                                    .next()
                                    .unwrap_or(&"?".to_string()),
                                path.nodes[1]
                                    .labels
                                    .iter()
                                    .next()
                                    .unwrap_or(&"?".to_string())
                            );
                        }
                        // Also swap the relationship's start/end node element IDs
                        for rel in &mut path.relationships {
                            std::mem::swap(
                                &mut rel.start_node_element_id,
                                &mut rel.end_node_element_id,
                            );
                        }
                    }
                }

                // Assign session-scoped integer IDs to all nodes and relationships in path
                for node in &mut path.nodes {
                    node.id = id_mapper.get_or_assign(&node.element_id);
                }
                for rel in &mut path.relationships {
                    rel.id = id_mapper.get_or_assign(&rel.element_id);
                    rel.start_node_id = id_mapper.get_or_assign(&rel.start_node_element_id);
                    rel.end_node_id = id_mapper.get_or_assign(&rel.end_node_element_id);
                }

                // Use the Path's packstream encoding
                let packstream_bytes = path.to_packstream();
                result.push(BoltValue::PackstreamBytes(packstream_bytes));
            }
            ReturnItemType::IdFunction { alias, labels } => {
                // For id() function, we need to compute the encoded ID from the element_id
                // The element_id is constructed from the label and ID column value
                log::debug!(
                    "IdFunction handler: alias={}, labels={:?}, field_name={}",
                    alias,
                    labels,
                    meta.field_name
                );

                // Find the node/relationship data in the row to build element_id
                let label = labels
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "Unknown".to_string());

                // First, try to get the id value from the field_name (the SQL column alias)
                // When "id(u) as uid" is queried, SQL is "SELECT u.user_id AS uid"
                // and row contains {"uid": 1}, so we look for field_name first
                let element_id = if let Some(id_val) = row.get(&meta.field_name) {
                    let id_str = match id_val {
                        Value::Number(n) => n.to_string(),
                        Value::String(s) => s.clone(),
                        _ => format!("{:?}", id_val),
                    };
                    format!("{}:{}", label, id_str)
                } else {
                    // Fallback: try alias.id_col format (when node is also returned)
                    let id_col = schema
                        .node_schema(&label)
                        .map(|ns| {
                            ns.node_id
                                .columns()
                                .first()
                                .map(|s| s.to_string())
                                .unwrap_or_else(|| "id".to_string())
                        })
                        .unwrap_or_else(|_| "id".to_string());
                    let id_column_key = format!("{}.{}", alias, id_col);

                    if let Some(id_val) = row.get(&id_column_key) {
                        let id_str = match id_val {
                            Value::Number(n) => n.to_string(),
                            Value::String(s) => s.clone(),
                            _ => format!("{:?}", id_val),
                        };
                        format!("{}:{}", label, id_str)
                    } else {
                        // Last fallback: try to construct from any property that looks like an ID
                        let prefix = format!("{}.", alias);
                        let mut element_id = None;
                        for (key, value) in row.iter() {
                            if let Some(prop) = key.strip_prefix(&prefix) {
                                if prop.ends_with("_id") || prop == "id" {
                                    let id_str = match value {
                                        Value::Number(n) => n.to_string(),
                                        Value::String(s) => s.clone(),
                                        _ => continue,
                                    };
                                    element_id = Some(format!("{}:{}", label, id_str));
                                    break;
                                }
                            }
                        }
                        element_id.unwrap_or_else(|| format!("{}:unknown", label))
                    }
                };

                // Use IdMapper's deterministic ID computation
                let encoded_id = super::id_mapper::IdMapper::compute_deterministic_id(&element_id);
                log::debug!("id() encoded: {} -> {}", element_id, encoded_id);
                result.push(BoltValue::Json(Value::Number(encoded_id.into())));
            }
            ReturnItemType::Scalar => {
                // For scalars, just extract the value and wrap in BoltValue::Json
                let value = row.get(&meta.field_name).cloned().unwrap_or(Value::Null);
                result.push(BoltValue::Json(value));
            }
        }
    }

    Ok(result)
}

/// Transform flat result row into a Node struct
///
/// Extracts properties, determines ID columns from schema, and generates elementId
pub(crate) fn transform_to_node(
    row: &HashMap<String, Value>,
    var_name: &str,
    labels: &[String],
    schema: &GraphSchema,
) -> Result<Node, String> {
    // Extract properties for this variable
    // Columns like "n.user_id", "n.name" → properties { user_id: 123, name: "Alice" }
    let prefix = format!("{}.", var_name);
    let mut properties = HashMap::new();

    for (key, value) in row.iter() {
        if let Some(prop_name) = key.strip_prefix(&prefix) {
            // Skip internal __label__ and VLP metadata columns from properties
            if prop_name != "__label__"
                && prop_name != "_label__"
                && prop_name != "id"
                && prop_name != "properties"
            {
                properties.insert(prop_name.to_string(), value.clone());
            }
        }
    }

    // VLP CTE results: o.properties is a JSON blob, o.id is the ID string
    // Parse JSON properties and merge into the properties map
    let _vlp_id_col = format!("{}.id", var_name);
    let vlp_props_col = format!("{}.properties", var_name);
    if let Some(props_val) = row.get(&vlp_props_col) {
        match props_val {
            Value::String(json_str) => {
                if let Ok(parsed) = serde_json::from_str::<HashMap<String, Value>>(json_str) {
                    for (k, v) in parsed {
                        // Strip table alias prefix from JSON keys (e.g., "a_1.user_id" → "user_id")
                        // ClickHouse adds these prefixes when JOINs create ambiguous column names
                        let clean_key = if k.contains('.') {
                            k.split('.').next_back().unwrap_or(&k).to_string()
                        } else {
                            k
                        };
                        properties.entry(clean_key).or_insert(v);
                    }
                } else {
                    log::debug!(
                        "VLP: Failed to parse JSON properties for {}: {}",
                        var_name,
                        json_str
                    );
                }
            }
            other => {
                log::debug!(
                    "VLP: properties column for {} is not a string: {:?}",
                    var_name,
                    other
                );
            }
        }
    } else {
        log::debug!(
            "VLP: No {}.properties column found. Row keys: {:?}",
            var_name,
            row.keys().collect::<Vec<_>>()
        );
    }

    // Get primary label
    // Priority: 1. Per-row {alias}.__label__ (VLP/UNION), 2. Provided labels (metadata), 3. __label__, 4. Infer
    let label = if let Some(label_value) = row.get(&format!("{}.__label__", var_name)) {
        // VLP multi-type queries: {alias}.__label__ has the correct per-row label
        let row_label = value_to_string(label_value).ok_or_else(|| {
            format!(
                "Invalid {}.__label__ value for node: {}",
                var_name, var_name
            )
        })?;
        if row_label != "Unknown" {
            row_label
        } else if let Some(l) = labels.first() {
            l.clone()
        } else {
            infer_node_label_from_properties(&properties, schema)
                .ok_or_else(|| format!("No label for node variable: {}", var_name))?
        }
    } else if let Some(l) = labels.first() {
        l.clone()
    } else if let Some(label_value) = row.get("__label__") {
        let label_str = value_to_string(label_value)
            .ok_or_else(|| format!("Invalid __label__ value for node variable: {}", var_name))?;
        // Skip "Unknown" — it's the outer VLP query's default label, not the node's actual label
        if label_str == "Unknown" {
            infer_node_label_from_properties(&properties, schema)
                .ok_or_else(|| format!("No label for node variable: {}", var_name))?
        } else {
            label_str
        }
    } else {
        // Fallback: infer label from properties by matching schema ID columns
        infer_node_label_from_properties(&properties, schema)
            .ok_or_else(|| format!("No label for node variable: {}", var_name))?
    };

    // Get node schema
    let node_schema = schema
        .node_schema_opt(&label)
        .ok_or_else(|| format!("Node schema not found for label: {}", label))?;

    // Get ID column names from schema
    let id_columns = node_schema.node_id.id.columns();

    // Extract ID values from properties
    let id_values: Vec<String> = id_columns
        .iter()
        .map(|col_name| {
            properties
                .get(*col_name)
                .and_then(value_to_string)
                .ok_or_else(|| format!("Missing ID column '{}' for node '{}'", col_name, var_name))
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Generate elementId using Phase 3 function
    let id_value_refs: Vec<&str> = id_values.iter().map(|s| s.as_str()).collect();
    let element_id = generate_node_element_id(&label, &id_value_refs);

    // Derive integer ID from element_id (ensures uniqueness across labels)
    let id: i64 = generate_id_from_element_id(&element_id);
    log::debug!("🔢 Node created: element_id={}, id={}", element_id, id);

    // Create Node struct
    // Use the resolved label (from per-row __label__ or inference) as the definitive label.
    // This is more accurate than metadata labels for UNION queries where node types vary per row.
    let final_labels = vec![label];

    Ok(Node {
        id,
        labels: final_labels,
        properties,
        element_id,
    })
}

/// Infer node label from properties by matching against schema
///
/// Finds which node schema has the ID column present in the properties.
/// This is used for unlabeled queries like `MATCH (n) RETURN n`.
fn infer_node_label_from_properties(
    properties: &HashMap<String, Value>,
    schema: &GraphSchema,
) -> Option<String> {
    // For each node schema, check if its ID column is present and non-null
    for (label, node_schema) in schema.all_node_schemas() {
        let id_columns = node_schema.node_id.id.columns();

        // Check if ANY of the ID columns is present with a non-null value
        let has_id = id_columns
            .iter()
            .any(|col| properties.get(*col).is_some_and(|v| !v.is_null()));

        if has_id {
            return Some(label.clone());
        }
    }
    None
}

/// Transform raw result row fields into a Relationship struct
///
/// Extracts relationship properties, from/to node IDs, and generates elementIds
/// for the relationship and connected nodes.
///
/// # Arguments
///
/// * `row` - Raw result row (column_name → value)
/// * `var_name` - Relationship variable name (e.g., "r")
/// * `rel_types` - Relationship types (e.g., ["FOLLOWS"])
/// * `from_label` - Optional from node label (for polymorphic)
/// * `to_label` - Optional to node label (for polymorphic)
/// * `schema` - Graph schema for ID column lookup
///
/// # Returns
///
/// Relationship struct with properties and generated elementIds
pub(crate) fn transform_to_relationship(
    row: &HashMap<String, Value>,
    var_name: &str,
    rel_types: &[String],
    from_label: Option<&str>,
    to_label: Option<&str>,
    schema: &GraphSchema,
) -> Result<Relationship, String> {
    // Extract properties for this relationship variable
    // Columns like "r.follow_date", "r.weight" → properties
    let prefix = format!("{}.", var_name);
    let mut properties = HashMap::new();

    for (key, value) in row.iter() {
        if let Some(prop_name) = key.strip_prefix(&prefix) {
            properties.insert(prop_name.to_string(), value.clone());
        }
    }

    // 🔧 FIX: Check for multi-type CTE column pattern
    // Multi-type CTEs return: r.type, r.properties, r.start_id, r.end_id
    // where r.type and r.properties are ARRAYS that need unwrapping
    let has_cte_columns = properties.contains_key("type")
        && properties.contains_key("properties")
        && properties.contains_key("start_id")
        && properties.contains_key("end_id");

    let rel_type = if has_cte_columns {
        // Multi-type CTE case: extract from arrays
        log::info!(
            "🎯 Detected multi-type CTE columns for relationship '{}'",
            var_name
        );

        // Extract relationship type from array: ['FOLLOWS'] → "FOLLOWS"
        let type_value = properties
            .get("type")
            .ok_or_else(|| format!("Missing type column for CTE relationship '{}'", var_name))?;

        let rel_type_str = extract_first_from_array(type_value).ok_or_else(|| {
            format!(
                "Could not extract relationship type from array: {:?}",
                type_value
            )
        })?;

        // Parse relationship properties from JSON array: ['{"follow_date":"..."}'] → {follow_date: ...}
        // Save start_id and end_id before parsing
        let start_id = properties.get("start_id").cloned();
        let end_id = properties.get("end_id").cloned();

        if let Some(props_value) = properties.get("properties") {
            if let Some(json_str) = extract_first_from_array(props_value) {
                // Parse JSON string to get actual properties
                match serde_json::from_str::<HashMap<String, Value>>(&json_str) {
                    Ok(parsed_props) => {
                        log::info!(
                            "✅ Parsed {} relationship properties from CTE JSON",
                            parsed_props.len()
                        );
                        // Replace properties with parsed ones
                        properties = parsed_props;
                    }
                    Err(e) => {
                        log::warn!("⚠️ Failed to parse relationship properties JSON: {}", e);
                        // Clear properties on parse error
                        properties.clear();
                    }
                }
            }
        }

        // Add back start_id and end_id (they were not in the JSON properties)
        if let Some(sid) = start_id {
            properties.insert("start_id".to_string(), sid);
        }
        if let Some(eid) = end_id {
            properties.insert("end_id".to_string(), eid);
        }
        rel_type_str
    } else {
        // Standard case: relationship type from rel_types parameter
        rel_types
            .first()
            .ok_or_else(|| format!("No relationship type for variable: {}", var_name))?
            .clone()
    };

    // Get primary relationship type (already extracted above for CTE case)

    // Get relationship schema
    let rel_schema = schema
        .get_relationships_schema_opt(&rel_type)
        .ok_or_else(|| format!("Relationship schema not found for type: {}", rel_type))?;

    // Get from/to node labels (use provided or schema defaults)
    // For polymorphic edges ($any), resolve actual label from row's start_type/end_type
    let schema_from_label = from_label.unwrap_or(&rel_schema.from_node);
    let schema_to_label = to_label.unwrap_or(&rel_schema.to_node);

    let resolved_from_label: String;
    let resolved_to_label: String;

    if schema_from_label == "$any" || schema_to_label == "$any" {
        // Polymorphic edge: actual node types are in the row's start_type/end_type columns
        resolved_from_label = if schema_from_label == "$any" {
            properties
                .get("start_type")
                .and_then(value_to_string)
                .unwrap_or_else(|| {
                    log::warn!(
                        "Polymorphic relationship missing 'start_type' column; \
                         falling back to first node type in schema"
                    );
                    schema
                        .all_node_schemas()
                        .keys()
                        .next()
                        .cloned()
                        .unwrap_or_default()
                })
        } else {
            schema_from_label.to_string()
        };
        resolved_to_label = if schema_to_label == "$any" {
            properties
                .get("end_type")
                .and_then(value_to_string)
                .unwrap_or_else(|| {
                    log::warn!(
                        "Polymorphic relationship missing 'end_type' column; \
                         falling back to first node type in schema"
                    );
                    schema
                        .all_node_schemas()
                        .keys()
                        .next()
                        .cloned()
                        .unwrap_or_default()
                })
        } else {
            schema_to_label.to_string()
        };
        // Remove start_type/end_type from properties so they don't leak as rel properties
        properties.remove("start_type");
        properties.remove("end_type");
    } else {
        resolved_from_label = schema_from_label.to_string();
        resolved_to_label = schema_to_label.to_string();
    }

    let from_node_label = &resolved_from_label;
    let to_node_label = &resolved_to_label;

    // Extract from_id values using relationship's FK columns (e.g., follower_id, followed_id)
    let from_rel_id_columns = rel_schema.from_id.columns();
    let from_id_values: Vec<String> = from_rel_id_columns
        .iter()
        .enumerate()
        .map(|(i, col_name)| {
            // First try: FK column from relationship schema (e.g., follower_id)
            properties
                .get(*col_name)
                // Second try: generic CTE column names
                .or_else(|| properties.get("start_id"))
                .or_else(|| properties.get("from_id"))
                // Third try: composite variants
                .or_else(|| properties.get(&format!("from_id_{}", i + 1)))
                .and_then(value_to_string)
                .ok_or_else(|| {
                    format!(
                        "Missing from_id column '{}' for relationship '{}'",
                        col_name, var_name
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Extract to_id values using relationship's FK columns
    let to_rel_id_columns = rel_schema.to_id.columns();
    let to_id_values: Vec<String> = to_rel_id_columns
        .iter()
        .enumerate()
        .map(|(i, col_name)| {
            // First try: FK column from relationship schema (e.g., followed_id)
            properties
                .get(*col_name)
                // Second try: generic CTE column names
                .or_else(|| properties.get("end_id"))
                .or_else(|| properties.get("to_id"))
                // Third try: composite variants
                .or_else(|| properties.get(&format!("to_id_{}", i + 1)))
                .and_then(value_to_string)
                .ok_or_else(|| {
                    format!(
                        "Missing to_id column '{}' for relationship '{}'",
                        col_name, var_name
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Join composite IDs with pipe separator
    let from_id_str = from_id_values.join("|");
    let to_id_str = to_id_values.join("|");

    // Remove internal ID keys from properties (they're FK columns, not user properties)
    properties.remove("from_id");
    properties.remove("to_id");
    properties.remove("start_id");
    properties.remove("end_id");
    // Remove relationship FK columns and composite variants
    for col in &from_rel_id_columns {
        properties.remove(*col);
    }
    for col in &to_rel_id_columns {
        properties.remove(*col);
    }
    // Remove any composite ID variants (from_id_1, from_id_2, to_id_1, to_id_2, ...)
    let composite_id_keys: Vec<String> = properties
        .keys()
        .filter(|k| k.starts_with("from_id_") || k.starts_with("to_id_"))
        .cloned()
        .collect();
    for key in composite_id_keys {
        properties.remove(&key);
    }

    // Generate relationship elementId: "FOLLOWS:1->2" or "BELONGS_TO:tenant1|user1->tenant1|org1"
    let element_id = generate_relationship_element_id(&rel_type, &from_id_str, &to_id_str);

    // Generate node elementIds for start and end nodes (with composite ID support)
    let from_id_refs: Vec<&str> = from_id_values.iter().map(|s| s.as_str()).collect();
    let to_id_refs: Vec<&str> = to_id_values.iter().map(|s| s.as_str()).collect();

    let start_node_element_id = generate_node_element_id(from_node_label, &from_id_refs);
    let end_node_element_id = generate_node_element_id(to_node_label, &to_id_refs);

    // Derive integer IDs from element_ids (ensures uniqueness across labels)
    let rel_id = generate_id_from_element_id(&element_id);
    let start_node_id = generate_id_from_element_id(&start_node_element_id);
    let end_node_id = generate_id_from_element_id(&end_node_element_id);

    log::info!(
        "🔗 Bolt Relationship BEFORE: var_name={}, rel_type={}, from_label={}, to_label={}, from_id_str={}, to_id_str={}",
        var_name,
        rel_type,
        from_node_label,
        to_node_label,
        from_id_str,
        to_id_str
    );

    log::debug!(
        "🔗 Bolt Relationship DEBUG: type={}, from_label={}, to_label={}, from_id_str={}, to_id_str={}, from_node_element_id={}, to_node_element_id={}",
        rel_type,
        from_node_label,
        to_node_label,
        from_id_str,
        to_id_str,
        start_node_element_id,
        end_node_element_id
    );

    log::debug!(
        "🔗 Bolt Relationship: type={}, start={}({}) end={}({}), element_id={}",
        rel_type,
        start_node_element_id,
        start_node_id,
        end_node_element_id,
        end_node_id,
        element_id
    );

    // Create Relationship struct
    Ok(Relationship {
        id: rel_id,
        start_node_id,
        end_node_id,
        rel_type: rel_type.to_string(),
        properties,
        element_id,
        start_node_element_id,
        end_node_element_id,
    })
}

/// Transform fixed-hop path data to Neo4j Path structure
///
/// For a path p = (a)-[r]->(b), this function:
/// 1. Uses known labels/types from query planning metadata
/// 2. Tries to find actual data in the row, falls back to metadata-based nodes
/// 3. Constructs a Path with proper structure for Neo4j visualization
///
/// # Arguments
///
/// * `row` - The result row from ClickHouse
/// * `path_name` - The path variable name (e.g., "p")
/// * `start_alias` - Alias of the start node
/// * `end_alias` - Alias of the end node
/// * `rel_alias` - Alias of the relationship
/// * `start_labels` - Known labels for start node from query planning
/// * `end_labels` - Known labels for end node from query planning
/// * `rel_types` - Known relationship types from query planning
/// * `schema` - Graph schema for type information
/// * `return_metadata` - Metadata for all return items (to find component info)
///
/// # Returns
///
/// Path struct with nodes, relationships, and indices
#[allow(clippy::too_many_arguments)]
fn transform_to_path(
    row: &HashMap<String, Value>,
    path_name: &str,
    start_alias: Option<&str>,
    end_alias: Option<&str>,
    rel_alias: Option<&str>,
    start_labels: &[String],
    end_labels: &[String],
    rel_types: &[String],
    schema: &GraphSchema,
    return_metadata: &[ReturnItemMetadata],
) -> Result<Path, String> {
    let start_alias = start_alias.unwrap_or("_start");
    let end_alias = end_alias.unwrap_or("_end");
    let rel_alias = rel_alias.unwrap_or("_rel");

    log::debug!(
        "transform_to_path: path={}, start={}({:?}), end={}({:?}), rel={}({:?})",
        path_name,
        start_alias,
        start_labels,
        end_alias,
        end_labels,
        rel_alias,
        rel_types
    );

    // Check for JSON format first (UNION path queries)
    // Format: _start_properties, _end_properties, _rel_properties, __rel_type__, __start_label__, __end_label__
    if row.contains_key("_start_properties") && row.contains_key("_end_properties") {
        log::trace!("🎯 Detected JSON format for path - using explicit type/label columns");
        return transform_path_from_json(row, schema);
    }

    // For polymorphic UNION path queries, use __start_label__ and __end_label__ columns
    let effective_start_labels = if start_labels.is_empty() {
        if let Some(label_val) = row.get("__start_label__").and_then(value_to_string) {
            if label_val != "Unknown" {
                log::debug!("Using __start_label__ column: {}", label_val);
                vec![label_val]
            } else {
                vec![]
            }
        } else {
            vec![]
        }
    } else {
        start_labels.to_vec()
    };

    let effective_end_labels = if end_labels.is_empty() {
        if let Some(label_val) = row.get("__end_label__").and_then(value_to_string) {
            if label_val != "Unknown" {
                log::debug!("Using __end_label__ column: {}", label_val);
                vec![label_val]
            } else {
                vec![]
            }
        } else {
            vec![]
        }
    } else {
        end_labels.to_vec()
    };

    // If labels are still empty, infer from relationship schema
    let effective_start_labels = if effective_start_labels.is_empty() && !rel_types.is_empty() {
        if let Some(rel_schema) = schema.get_relationships_schema_opt(&rel_types[0]) {
            if rel_schema.from_node != "$any" {
                log::debug!(
                    "Inferred start label '{}' from rel schema '{}'",
                    rel_schema.from_node,
                    rel_types[0]
                );
                vec![rel_schema.from_node.clone()]
            } else {
                effective_start_labels
            }
        } else {
            effective_start_labels
        }
    } else {
        effective_start_labels
    };

    let effective_end_labels = if effective_end_labels.is_empty() && !rel_types.is_empty() {
        if let Some(rel_schema) = schema.get_relationships_schema_opt(&rel_types[0]) {
            if rel_schema.to_node != "$any" {
                log::debug!(
                    "Inferred end label '{}' from rel schema '{}'",
                    rel_schema.to_node,
                    rel_types[0]
                );
                vec![rel_schema.to_node.clone()]
            } else {
                effective_end_labels
            }
        } else {
            effective_end_labels
        }
    } else {
        effective_end_labels
    };

    // Original format: individual columns for each property
    // Extract start node - require either metadata lookup success or known labels
    let start_node = find_node_in_row_with_label(
        row,
        start_alias,
        &effective_start_labels,
        return_metadata,
        schema,
    )
    .or_else(|| {
        // If we have a known label, create node with that label
        effective_start_labels.first().map(|label| {
            log::debug!("Creating start node with known label: {}", label);
            create_node_with_label(label, 0)
        })
    })
    .ok_or_else(|| {
        format!(
            "Internal error: Cannot resolve start node '{}' for path '{}'. \
                     No label metadata available and node not found in row. \
                     start_labels={:?}, row_keys={:?}",
            start_alias,
            path_name,
            effective_start_labels,
            row.keys().collect::<Vec<_>>()
        )
    })?;

    // Extract end node - require either metadata lookup success or known labels
    let end_node = find_node_in_row_with_label(
        row,
        end_alias,
        &effective_end_labels,
        return_metadata,
        schema,
    )
    .or_else(|| {
        // If we have a known label, create node with that label
        effective_end_labels.first().map(|label| {
            log::debug!("Creating end node with known label: {}", label);
            create_node_with_label(label, 1)
        })
    })
    .ok_or_else(|| {
        format!(
            "Internal error: Cannot resolve end node '{}' for path '{}'. \
                     No label metadata available and node not found in row. \
                     end_labels={:?}, row_keys={:?}",
            end_alias,
            path_name,
            effective_end_labels,
            row.keys().collect::<Vec<_>>()
        )
    })?;

    // Determine correct node order for the relationship based on schema direction
    // The relationship should ALWAYS follow schema direction (from -> to), regardless of query order
    let (from_node, to_node, needs_swap) = {
        // Check if there's a schema relationship from start_labels -> end_labels
        let forward_match = rel_types.iter().any(|rel_type| {
            if let Some(rel_schema) = schema.get_relationships_schema_opt(rel_type) {
                (rel_schema.from_node == "$any" || start_labels.contains(&rel_schema.from_node))
                    && (rel_schema.to_node == "$any" || end_labels.contains(&rel_schema.to_node))
            } else {
                false
            }
        });

        if forward_match {
            // Query order matches schema direction
            (start_node.clone(), end_node.clone(), false)
        } else {
            // Try reverse direction
            let reverse_match = rel_types.iter().any(|rel_type| {
                if let Some(rel_schema) = schema.get_relationships_schema_opt(rel_type) {
                    (rel_schema.from_node == "$any" || end_labels.contains(&rel_schema.from_node))
                        && (rel_schema.to_node == "$any"
                            || start_labels.contains(&rel_schema.to_node))
                } else {
                    false
                }
            });

            if reverse_match {
                // Need to swap nodes to match schema direction
                log::debug!(
                    "Swapping nodes for path: schema expects {:?}->{:?}, but query gave {:?}->{:?}",
                    end_labels,
                    start_labels,
                    start_labels,
                    end_labels
                );
                (end_node.clone(), start_node.clone(), true)
            } else {
                // Can't determine - use query order (fallback)
                (start_node.clone(), end_node.clone(), false)
            }
        }
    };

    // Extract relationship with schema-order nodes
    let relationship = find_relationship_in_row_with_type(
        row,
        rel_alias,
        &from_node.element_id,
        &to_node.element_id,
        rel_types,
        &from_node.labels,
        &to_node.labels,
        return_metadata,
        schema,
    )
    .or_else(|| {
        // If we have a known type, create relationship with that type
        rel_types.first().map(|rel_type| {
            log::debug!("Creating relationship with known type: {}", rel_type);
            create_relationship_with_type(rel_type, &from_node.element_id, &to_node.element_id)
        })
    })
    .ok_or_else(|| {
        format!(
            "Internal error: Cannot resolve relationship '{}' for path '{}'. \
             No type metadata available and relationship not found in row. \
             rel_types={:?}, row_keys={:?}",
            rel_alias,
            path_name,
            rel_types,
            row.keys().collect::<Vec<_>>()
        )
    })?;

    log::info!(
        "Path created: start_node.labels={:?}, end_node.labels={:?}, rel.type={}, node_swap={}",
        from_node.labels,
        to_node.labels,
        relationship.rel_type,
        needs_swap
    );

    // Create Path with single-hop structure
    // Indices for single hop: [1, 1] means "relationship index 1, then node index 1"
    // (Neo4j uses 1-based indexing in path indices)
    Ok(Path::single_hop(from_node, relationship, to_node))
}

/// Transform path from JSON format (UNION path queries)
/// Parses _start_properties, _end_properties, _rel_properties JSON strings
/// Used for UNION path queries where each row has explicit type columns:
/// __start_label__, __end_label__, __rel_type__
fn transform_path_from_json(
    row: &HashMap<String, Value>,
    schema: &GraphSchema,
) -> Result<Path, String> {
    // Parse start node properties from JSON
    let start_props: HashMap<String, Value> = match row.get("_start_properties") {
        Some(Value::String(json_str)) => {
            serde_json::from_str(json_str).unwrap_or_else(|_| HashMap::new())
        }
        _ => HashMap::new(),
    };

    // Parse end node properties from JSON
    let end_props: HashMap<String, Value> = match row.get("_end_properties") {
        Some(Value::String(json_str)) => {
            serde_json::from_str(json_str).unwrap_or_else(|_| HashMap::new())
        }
        _ => HashMap::new(),
    };

    // Parse relationship properties from JSON
    let rel_props: HashMap<String, Value> = match row.get("_rel_properties") {
        Some(Value::String(json_str)) => {
            if json_str == "{}" {
                HashMap::new() // Empty for denormalized relationships
            } else {
                serde_json::from_str(json_str).unwrap_or_else(|_| HashMap::new())
            }
        }
        _ => HashMap::new(),
    };

    log::info!(
        "📦 Parsed JSON: start_props keys={:?}, end_props keys={:?}, rel_props keys={:?}",
        start_props.keys().collect::<Vec<_>>(),
        end_props.keys().collect::<Vec<_>>(),
        rel_props.keys().collect::<Vec<_>>()
    );

    // Get explicit node labels from __start_label__ and __end_label__ columns (required for JSON format)
    let start_label = match row.get("__start_label__") {
        Some(Value::String(explicit_label)) if !explicit_label.is_empty() => explicit_label.clone(),
        _ => {
            return Err(
                "Missing __start_label__ column in path JSON format - this is a bug".to_string(),
            );
        }
    };
    let end_label = match row.get("__end_label__") {
        Some(Value::String(explicit_label)) if !explicit_label.is_empty() => explicit_label.clone(),
        _ => {
            return Err(
                "Missing __end_label__ column in path JSON format - this is a bug".to_string(),
            );
        }
    };

    // Get relationship type from explicit __rel_type__ column (required for JSON format)
    // JSON format is only used for UNION path queries, and we always add __rel_type__ there
    let rel_type = match row.get("__rel_type__") {
        Some(Value::String(explicit_type)) if !explicit_type.is_empty() => explicit_type.clone(),
        _ => {
            return Err(
                "Missing __rel_type__ column in path JSON format - this is a bug".to_string(),
            );
        }
    };

    // Create start node - element_id is source of truth, integer id derived from it
    let start_id_str = extract_id_string_from_props(&start_props, Some(schema), Some(&start_label));
    let start_element_id = generate_node_element_id(&start_label, &[&start_id_str]);
    let start_id = generate_id_from_element_id(&start_element_id);
    // Clean property keys (remove table alias prefix like "t1_0.")
    let start_props_clean = clean_property_keys(start_props);
    let start_node = Node::new(
        start_id,
        vec![start_label.clone()],
        start_props_clean,
        start_element_id.clone(),
    );

    // Create end node - element_id is source of truth, integer id derived from it
    let end_id_str = extract_id_string_from_props(&end_props, Some(schema), Some(&end_label));
    let end_element_id = generate_node_element_id(&end_label, &[&end_id_str]);
    let end_id = generate_id_from_element_id(&end_element_id);
    // Clean property keys
    let end_props_clean = clean_property_keys(end_props);
    let end_node = Node::new(
        end_id,
        vec![end_label.clone()],
        end_props_clean,
        end_element_id.clone(),
    );

    // Determine correct node order based on schema direction
    // The relationship should ALWAYS follow schema direction (from -> to), regardless of query order
    let (from_node, to_node, from_id_str, to_id_str) =
        {
            // Check if there's a schema relationship from start_label -> end_label
            let forward_match =
                if let Some(rel_schema) = schema.get_relationships_schema_opt(&rel_type) {
                    (rel_schema.from_node == "$any" || rel_schema.from_node == start_label)
                        && (rel_schema.to_node == "$any" || rel_schema.to_node == end_label)
                } else {
                    false
                };

            if forward_match {
                // Query order matches schema direction
                (
                    start_node.clone(),
                    end_node.clone(),
                    start_id_str.clone(),
                    end_id_str.clone(),
                )
            } else {
                // Try reverse direction
                let reverse_match =
                    if let Some(rel_schema) = schema.get_relationships_schema_opt(&rel_type) {
                        (rel_schema.from_node == "$any" || rel_schema.from_node == end_label)
                            && (rel_schema.to_node == "$any" || rel_schema.to_node == start_label)
                    } else {
                        false
                    };

                if reverse_match {
                    // Need to swap nodes to match schema direction
                    log::debug!(
                    "Swapping nodes for path: schema expects {:?}->{:?}, but query gave {:?}->{:?}",
                    end_label, start_label,
                    start_label, end_label
                );
                    (
                        end_node.clone(),
                        start_node.clone(),
                        end_id_str.clone(),
                        start_id_str.clone(),
                    )
                } else {
                    // Can't determine - use query order (fallback)
                    (
                        start_node.clone(),
                        end_node.clone(),
                        start_id_str.clone(),
                        end_id_str.clone(),
                    )
                }
            }
        };

    // Create relationship with schema-order nodes
    let rel_element_id = generate_relationship_element_id(&rel_type, &from_id_str, &to_id_str);
    let rel_id = generate_id_from_element_id(&rel_element_id);
    let rel_props_clean = clean_property_keys(rel_props);
    let relationship = Relationship::new(
        rel_id,
        from_node.id, // from_node_id - derived from from_element_id
        to_node.id,   // to_node_id - derived from to_element_id
        rel_type.clone(),
        rel_props_clean,
        rel_element_id,
        from_node.element_id.clone(),
        to_node.element_id.clone(),
    );

    log::info!(
        "✅ Path from JSON: from={} ({}), to={} ({}), rel={}",
        from_node.labels[0],
        from_node.id,
        to_node.labels[0],
        to_node.id,
        relationship.rel_type
    );

    Ok(Path::single_hop(from_node, relationship, to_node))
}

/// Transform a VLP multi-type path from its tuple representation.
///
/// The VLP CTE returns a tuple column with these fields (in order):
/// [0] start_properties (JSON string)
/// [1] end_properties (JSON string)
/// [2] rel_properties (array of JSON strings, one per hop)
/// [3] path_relationships (array of rel type strings, e.g., ["FOLLOWS"])
/// [4] start_id (string)
/// [5] end_id (string)
/// [6] hop_count (integer)
/// [7] start_type (string, e.g., "User")
/// [8] end_type (string, e.g., "Post")
fn transform_vlp_path(
    row: &HashMap<String, Value>,
    path_field: &str,
    _schema: &GraphSchema,
) -> Result<Path, String> {
    // The tuple is returned as a JSON array
    let tuple_val = row.get(path_field).ok_or_else(|| {
        format!(
            "VLP path column '{}' not found in row. Available: {:?}",
            path_field,
            row.keys().collect::<Vec<_>>()
        )
    })?;

    let fields = match tuple_val {
        Value::Array(arr) => arr,
        other => {
            return Err(format!(
                "VLP path '{}': expected Array tuple, got {:?}",
                path_field, other
            ));
        }
    };

    if fields.len() < 9 {
        return Err(format!(
            "VLP path '{}': expected 9 tuple fields, got {}",
            path_field,
            fields.len()
        ));
    }

    // Extract fields from the tuple
    let start_props_json = fields[0].as_str().unwrap_or("{}");
    let end_props_json = fields[1].as_str().unwrap_or("{}");

    // rel_properties is an array of JSON strings (one per hop)
    let rel_props_json = match &fields[2] {
        Value::Array(arr) => arr.first().and_then(|v| v.as_str()).unwrap_or("{}"),
        Value::String(s) => s.as_str(),
        _ => "{}",
    };

    // path_relationships is an array of relationship type strings
    let rel_type = match &fields[3] {
        Value::Array(arr) => arr
            .first()
            .and_then(|v| v.as_str())
            .unwrap_or("UNKNOWN")
            .to_string(),
        Value::String(s) => s.clone(),
        _ => "UNKNOWN".to_string(),
    };

    let start_id_str = value_to_id_string(&fields[4]);
    let end_id_str = value_to_id_string(&fields[5]);
    let hop_count = fields[6]
        .as_i64()
        .or_else(|| fields[6].as_str().and_then(|s| s.parse().ok()))
        .unwrap_or(1);
    let start_type = fields[7].as_str().unwrap_or("Unknown").to_string();
    let end_type = fields[8].as_str().unwrap_or("Unknown").to_string();

    log::debug!(
        "VLP path: {}:{} -[{}]-> {}:{}",
        start_type,
        start_id_str,
        rel_type,
        end_type,
        end_id_str
    );

    // Parse property JSON blobs
    let start_props: HashMap<String, Value> =
        serde_json::from_str(start_props_json).unwrap_or_default();
    let end_props: HashMap<String, Value> =
        serde_json::from_str(end_props_json).unwrap_or_default();
    let rel_props: HashMap<String, Value> =
        serde_json::from_str(rel_props_json).unwrap_or_default();

    // Build start node
    let start_element_id = generate_node_element_id(&start_type, &[&start_id_str]);
    let start_id = generate_id_from_element_id(&start_element_id);
    let start_node = Node::new(
        start_id,
        vec![start_type.clone()],
        clean_property_keys(start_props),
        start_element_id.clone(),
    );

    // Build end node
    let end_element_id = generate_node_element_id(&end_type, &[&end_id_str]);
    let end_id = generate_id_from_element_id(&end_element_id);
    let end_node = Node::new(
        end_id,
        vec![end_type.clone()],
        clean_property_keys(end_props),
        end_element_id.clone(),
    );

    // Determine correct node order based on schema direction
    // The relationship should ALWAYS follow schema direction (from -> to), regardless of query order
    let (from_node, to_node, from_id_str, to_id_str) =
        {
            // Check if there's a schema relationship from start_type -> end_type
            let forward_match =
                if let Some(rel_schema) = _schema.get_relationships_schema_opt(&rel_type) {
                    (rel_schema.from_node == "$any" || rel_schema.from_node == start_type)
                        && (rel_schema.to_node == "$any" || rel_schema.to_node == end_type)
                } else {
                    false
                };

            if forward_match {
                // Query order matches schema direction
                (
                    start_node.clone(),
                    end_node.clone(),
                    start_id_str.clone(),
                    end_id_str.clone(),
                )
            } else {
                // Try reverse direction
                let reverse_match =
                    if let Some(rel_schema) = _schema.get_relationships_schema_opt(&rel_type) {
                        (rel_schema.from_node == "$any" || rel_schema.from_node == end_type)
                            && (rel_schema.to_node == "$any" || rel_schema.to_node == start_type)
                    } else {
                        false
                    };

                if reverse_match {
                    // Need to swap nodes to match schema direction
                    log::debug!(
                    "Swapping VLP path nodes: schema expects {:?}->{:?}, but query gave {:?}->{:?}",
                    end_type, start_type,
                    start_type, end_type
                );
                    (
                        end_node.clone(),
                        start_node.clone(),
                        end_id_str.clone(),
                        start_id_str.clone(),
                    )
                } else {
                    // Can't determine - use query order (fallback)
                    (
                        start_node.clone(),
                        end_node.clone(),
                        start_id_str.clone(),
                        end_id_str.clone(),
                    )
                }
            }
        };

    // Build relationship with schema-order nodes
    let rel_element_id = generate_relationship_element_id(&rel_type, &from_id_str, &to_id_str);
    let rel_id = generate_id_from_element_id(&rel_element_id);
    let relationship = Relationship::new(
        rel_id,
        from_node.id,
        to_node.id,
        rel_type,
        clean_property_keys(rel_props),
        rel_element_id,
        from_node.element_id.clone(),
        to_node.element_id.clone(),
    );

    log::debug!(
        "VLP Path: from={} (id={}), to={} (id={}), rel={}",
        from_node.labels[0],
        from_node.id,
        to_node.labels[0],
        to_node.id,
        relationship.rel_type
    );

    // Currently only single-hop VLP paths are serialized to Bolt Path objects.
    // Multi-hop paths (hop_count > 1) would need to construct full Path with
    // all intermediate nodes — not yet implemented for browser expand queries
    // which only use *1 (implicit single-hop) patterns.
    if hop_count > 1 {
        log::warn!(
            "VLP path with hop_count={} truncated to single hop for Bolt serialization",
            hop_count
        );
    }

    Ok(Path::single_hop(from_node, relationship, to_node))
}

/// Clean property keys by removing:
/// 1. Table alias prefixes (e.g., "t1_0.city" -> "city")  
/// 2. JSON uniqueness prefixes (e.g., "_s_city" -> "city", "_e_name" -> "name", "_r_from_id" -> "from_id")
fn clean_property_keys(props: HashMap<String, Value>) -> HashMap<String, Value> {
    props
        .into_iter()
        .map(|(k, v)| {
            let mut clean_key = k.clone();

            // Remove table alias prefix like "t1_0." or "t2_3."
            if let Some(dot_pos) = clean_key.find('.') {
                clean_key = clean_key[dot_pos + 1..].to_string();
            }

            // Remove JSON uniqueness prefixes: _s_, _e_, _r_
            for prefix in &["_s_", "_e_", "_r_"] {
                if clean_key.starts_with(prefix) {
                    clean_key = clean_key[prefix.len()..].to_string();
                    break;
                }
            }

            (clean_key, v)
        })
        .collect()
}

/// Extract ID string from properties HashMap for element_id generation
/// Tries common ID field names and returns the string representation
fn extract_id_string_from_props(
    props: &HashMap<String, Value>,
    schema: Option<&GraphSchema>,
    label: Option<&str>,
) -> String {
    // Try schema-defined ID columns first (most reliable)
    if let (Some(schema), Some(label)) = (schema, label) {
        if let Some(node_schema) = schema.node_schema_opt(label) {
            let id_columns = node_schema.node_id.id.columns();
            let id_values: Vec<String> = id_columns
                .iter()
                .filter_map(|col| {
                    props
                        .get(*col)
                        .or_else(|| {
                            // Try prefixed variants (_s_, _e_)
                            ["_s_", "_e_"]
                                .iter()
                                .find_map(|pfx| props.get(&format!("{}{}", pfx, col)))
                        })
                        .and_then(value_to_string)
                })
                .collect();
            if id_values.len() == id_columns.len() {
                return id_values.join("|");
            }
        }
    }

    // Fallback: common ID column names
    let id_fields = [
        "user_id",
        "post_id",
        "id",
        "code",
        "node_id",
        "origin_code",
        "dest_code",
        "airport_code",
        "flight_id",
    ];

    for id_field in &id_fields {
        if let Some(val) = props.get(*id_field) {
            if let Some(str_val) = value_to_string(val) {
                return str_val;
            }
        }
    }

    // Try prefixed match (_s_, _e_, _r_ prefixes)
    for id_field in &id_fields {
        for prefix in &["_s_", "_e_", "_r_"] {
            let prefixed_key = format!("{}{}", prefix, id_field);
            if let Some(val) = props.get(&prefixed_key) {
                if let Some(str_val) = value_to_string(val) {
                    return str_val;
                }
            }
        }
    }

    // Last resort: any key that looks like an ID
    for (key, val) in props {
        let key_lower = key.to_lowercase();
        if key_lower.ends_with("_id") || key_lower.ends_with("id") || key_lower == "code" {
            if let Some(str_val) = value_to_string(val) {
                return str_val;
            }
        }
    }

    "0".to_string()
}

/// Generate a unique integer node ID from element_id
/// This ensures round-trip: element_id is the source of truth, integer id is derived from it
/// Generate a deterministic ID from an element_id using the single source of truth.
/// This ensures "User:1" and "Post:1" have different IDs by encoding the label.
fn generate_id_from_element_id(element_id: &str) -> i64 {
    // Delegate to IdMapper's compute_deterministic_id for consistent encoding
    super::id_mapper::IdMapper::compute_deterministic_id(element_id)
}

/// Extract first element from a ClickHouse array value
///
/// Multi-type CTE queries return arrays with single elements:
/// - ['FOLLOWS'] → "FOLLOWS"
/// - ['{"follow_date":"2024-02-11"}'] → "{\"follow_date\":\"2024-02-11\"}"
///
/// # Arguments
///
/// * `val` - JSON Value that should be an array
///
/// # Returns
///
/// First element as String, or None if not an array or empty
fn extract_first_from_array(val: &Value) -> Option<String> {
    match val {
        Value::Array(arr) if !arr.is_empty() => {
            // Get first element
            match &arr[0] {
                Value::String(s) => Some(s.clone()),
                Value::Number(n) => Some(n.to_string()),
                Value::Bool(b) => Some(b.to_string()),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Find a node in the result row by its alias, using known labels
fn find_node_in_row_with_label(
    row: &HashMap<String, Value>,
    alias: &str,
    known_labels: &[String],
    return_metadata: &[ReturnItemMetadata],
    schema: &GraphSchema,
) -> Option<Node> {
    log::info!(
        "🔍 find_node_in_row_with_label: alias='{}', known_labels={:?}, row_keys={:?}",
        alias,
        known_labels,
        row.keys().collect::<Vec<_>>()
    );

    // First try the return metadata approach
    for meta in return_metadata {
        if meta.field_name == alias {
            if let ReturnItemType::Node { labels } = &meta.item_type {
                return transform_to_node(row, alias, labels, schema).ok();
            }
        }
    }

    // Check if there are properties in the row with this alias prefix
    let prefix = format!("{}.", alias);
    let mut properties = HashMap::new();

    for (key, value) in row.iter() {
        if let Some(prop_name) = key.strip_prefix(&prefix) {
            properties.insert(prop_name.to_string(), value.clone());
        }
    }

    log::info!(
        "🔍 Found {} properties for alias '{}' with prefix '{}'",
        properties.len(),
        alias,
        prefix
    );

    if properties.is_empty() {
        return None;
    }

    // Use the known label from path metadata (already extracted from composite keys)
    // Fall back to __label__ column from UNION queries if known_labels is empty
    let label = if let Some(l) = known_labels.first() {
        l.clone()
    } else if let Some(label_val) = row.get("__label__").and_then(value_to_string) {
        log::debug!("Using __label__ column for node '{}': {}", alias, label_val);
        label_val
    } else {
        return None;
    };

    // Get node schema
    let node_schema = schema.node_schema_opt(&label)?;

    // Get ID columns from schema
    let id_columns = node_schema.node_id.id.columns();
    let id_values: Vec<String> = id_columns
        .iter()
        .filter_map(|col| properties.get(*col).and_then(value_to_string))
        .collect();

    if id_values.is_empty() {
        log::warn!(
            "No ID values found for node '{}' with label '{}'",
            alias,
            label
        );
        return None;
    }

    let element_id = generate_node_element_id(
        &label,
        &id_values.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
    );
    // Derive integer ID from element_id (ensures uniqueness across labels)
    let id: i64 = generate_id_from_element_id(&element_id);

    log::info!(
        "✅ Found node '{}' in row: label={}, properties={}, element_id={}",
        alias,
        label,
        properties.len(),
        element_id
    );

    Some(Node {
        id,
        labels: vec![label.clone()],
        properties,
        element_id,
    })
}

/// Find or reconstruct a relationship in the result row using its alias, path metadata,
/// and the start/end node element IDs.
fn find_relationship_in_row_with_type(
    row: &HashMap<String, Value>,
    alias: &str,
    start_element_id: &str,
    end_element_id: &str,
    known_rel_types: &[String],
    from_labels: &[String],
    to_labels: &[String],
    return_metadata: &[ReturnItemMetadata],
    schema: &GraphSchema,
) -> Option<Relationship> {
    // First try return metadata
    for meta in return_metadata {
        if meta.field_name == alias {
            if let ReturnItemType::Relationship {
                rel_types,
                from_label,
                to_label,
                direction,
            } = &meta.item_type
            {
                return transform_to_relationship(
                    row,
                    alias,
                    rel_types,
                    from_label.as_deref(),
                    to_label.as_deref(),
                    schema,
                )
                .ok();
            }
        }
    }

    // Check if there are properties in the row with this alias prefix
    let prefix = format!("{}.", alias);
    let mut properties = HashMap::new();

    for (key, value) in row.iter() {
        if let Some(prop_name) = key.strip_prefix(&prefix) {
            properties.insert(prop_name.to_string(), value.clone());
        }
    }

    if properties.is_empty() {
        return None;
    }

    // Use the known relationship type from path metadata
    let rel_type = known_rel_types.first()?;
    let _from_label = from_labels.first()?;
    let _to_label = to_labels.first()?;

    log::info!(
        "✅ Found relationship '{}' in row: type={}, properties={}",
        alias,
        rel_type,
        properties.len()
    );

    // Node IDs are derived from element_id hash (same as the nodes themselves)
    let start_id = generate_id_from_element_id(start_element_id);
    let end_id = generate_id_from_element_id(end_element_id);

    // Generate relationship element_id from type and node element_ids
    let rel_element_id = format!("{}:{}->{}", rel_type, start_element_id, end_element_id);
    let rel_id = generate_id_from_element_id(&rel_element_id);

    // Remove internal from_id/to_id keys from properties (FK columns, not user properties)
    properties.remove("from_id");
    properties.remove("to_id");
    // Remove composite variants
    let composite_keys: Vec<String> = properties
        .keys()
        .filter(|k| {
            (k.starts_with("from_id_") || k.starts_with("to_id_"))
                && k.rsplit('_')
                    .next()
                    .map_or(false, |s| s.parse::<usize>().is_ok())
        })
        .cloned()
        .collect();
    for key in composite_keys {
        properties.remove(&key);
    }

    // Create relationship with extracted properties
    Some(Relationship {
        id: rel_id,
        start_node_id: start_id,
        end_node_id: end_id,
        rel_type: rel_type.clone(),
        properties,
        element_id: rel_element_id,
        start_node_element_id: start_element_id.to_string(),
        end_node_element_id: end_element_id.to_string(),
    })
}

/// Create a node with a known label but no data
fn create_node_with_label(label: &str, idx: i64) -> Node {
    let element_id = format!("{}:{}", label, idx);
    let id = generate_id_from_element_id(&element_id);
    Node {
        id,
        labels: vec![label.to_string()],
        properties: std::collections::HashMap::new(),
        element_id,
    }
}

/// Create a relationship with a known type but no data
fn create_relationship_with_type(
    rel_type: &str,
    start_element_id: &str,
    end_element_id: &str,
) -> Relationship {
    // Generate element_id and derive all integer IDs from element_ids
    let element_id = format!("{}:{}->{}", rel_type, start_element_id, end_element_id);
    let id = generate_id_from_element_id(&element_id);
    let start_node_id = generate_id_from_element_id(start_element_id);
    let end_node_id = generate_id_from_element_id(end_element_id);

    Relationship {
        id,
        start_node_id,
        end_node_id,
        rel_type: rel_type.to_string(),
        properties: std::collections::HashMap::new(),
        element_id,
        start_node_element_id: start_element_id.to_string(),
        end_node_element_id: end_element_id.to_string(),
    }
}

/// Convert a JSON Value to a String for elementId generation
///
/// Handles String, Number, Boolean types. Returns None for complex types.
/// Extract an ID value from a JSON Value, handling both strings and numbers.
/// ClickHouse may return tuple elements as their native types (integer for numeric columns),
/// so we must handle both Value::String("16") and Value::Number(16).
fn value_to_id_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        _ => "0".to_string(),
    }
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Null => None,
        _ => None, // Arrays and Objects not supported for IDs
    }
}

/// Try to transform a multi-label scan result row
///
/// Multi-label scan results have columns in the format: {alias}_label, {alias}_id, {alias}_properties
/// This function detects these columns and transforms them into proper Node objects.
///
/// Returns None if this is not a multi-label scan result.
fn try_transform_multi_label_row(
    row: &HashMap<String, Value>,
    metadata: &[ReturnItemMetadata],
    id_mapper: &mut super::id_mapper::IdMapper,
) -> Result<Option<Vec<BoltValue>>, String> {
    // Find return items that might be multi-label nodes
    // Multi-label results have columns: {alias}_label, {alias}_id, {alias}_properties
    let mut result = Vec::new();
    let mut found_multi_label = false;

    for meta in metadata {
        // Check if we have the special multi-label columns for this alias
        let label_col = format!("{}_label", meta.field_name);
        let id_col = format!("{}_id", meta.field_name);
        let props_col = format!("{}_properties", meta.field_name);

        if row.contains_key(&label_col) && row.contains_key(&id_col) && row.contains_key(&props_col)
        {
            found_multi_label = true;

            // Extract label
            let label = match row.get(&label_col) {
                Some(Value::String(l)) => l.clone(),
                Some(v) => v.to_string().trim_matches('"').to_string(),
                None => return Err(format!("Missing {} column", label_col)),
            };

            // Extract ID
            let id = match row.get(&id_col) {
                Some(Value::String(i)) => i.clone(),
                Some(v) => v.to_string().trim_matches('"').to_string(),
                None => return Err(format!("Missing {} column", id_col)),
            };

            // Extract and parse properties JSON
            let properties: HashMap<String, Value> = match row.get(&props_col) {
                Some(Value::String(json_str)) => {
                    serde_json::from_str(json_str).unwrap_or_else(|_| HashMap::new())
                }
                Some(Value::Object(map)) => {
                    // Already parsed as object
                    map.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
                }
                _ => HashMap::new(),
            };

            // Generate element_id
            let element_id = generate_node_element_id(&label, &[&id]);

            // Get session-scoped integer ID from id_mapper
            let numeric_id = id_mapper.get_or_assign(&element_id);

            // Create Node with mapped integer ID
            let node = Node::new(numeric_id, vec![label], properties, element_id);
            let packstream_bytes = node.to_packstream();
            result.push(BoltValue::PackstreamBytes(packstream_bytes));
        } else {
            // Not a multi-label column set, check for regular handling
            match &meta.item_type {
                ReturnItemType::Scalar => {
                    let value = row.get(&meta.field_name).cloned().unwrap_or(Value::Null);
                    result.push(BoltValue::Json(value));
                }
                _ => {
                    // If we've found some multi-label results but this one isn't,
                    // we can't use this code path
                    if found_multi_label {
                        return Err(format!(
                            "Mixed multi-label and regular results not supported for field: {}",
                            meta.field_name
                        ));
                    }
                    // Not a multi-label result at all
                    return Ok(None);
                }
            }
        }
    }

    if found_multi_label {
        Ok(Some(result))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_catalog::config::Identifier;

    #[test]
    fn test_value_to_string_integer() {
        let value = Value::Number(123.into());
        assert_eq!(value_to_string(&value), Some("123".to_string()));
    }

    #[test]
    fn test_value_to_string_string() {
        let value = Value::String("alice".to_string());
        assert_eq!(value_to_string(&value), Some("alice".to_string()));
    }

    #[test]
    fn test_value_to_string_boolean() {
        let value = Value::Bool(true);
        assert_eq!(value_to_string(&value), Some("true".to_string()));
    }

    #[test]
    fn test_value_to_string_null() {
        let value = Value::Null;
        assert_eq!(value_to_string(&value), None);
    }

    #[test]
    fn test_get_field_name_from_variable() {
        use crate::query_planner::{logical_expr::TableAlias, logical_plan::ProjectionItem};

        let proj_item = ProjectionItem {
            expression: LogicalExpr::TableAlias(TableAlias("n".to_string())),
            col_alias: None,
        };

        assert_eq!(get_field_name(&proj_item), "n");
    }

    #[test]
    fn test_get_field_name_with_alias() {
        use crate::query_planner::{
            logical_expr::{ColumnAlias, TableAlias},
            logical_plan::ProjectionItem,
        };

        let proj_item = ProjectionItem {
            expression: LogicalExpr::TableAlias(TableAlias("n".to_string())),
            col_alias: Some(ColumnAlias("user".to_string())),
        };

        assert_eq!(get_field_name(&proj_item), "user");
    }

    #[test]
    #[ignore] // TODO: Fix test - needs proper row data mapping from relationship columns to node ID columns
    fn test_transform_to_relationship_basic() {
        use crate::graph_catalog::graph_schema::RelationshipSchema;
        use std::collections::HashMap;

        // Create a minimal schema
        let mut schema = GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new());

        // Add relationship schema for FOLLOWS
        schema.insert_relationship_schema(
            "FOLLOWS".to_string(),
            RelationshipSchema {
                database: "test".to_string(),
                table_name: "follows".to_string(),
                column_names: vec![
                    "follower_id".to_string(),
                    "followed_id".to_string(),
                    "follow_date".to_string(),
                ],
                from_node: "User".to_string(),
                to_node: "User".to_string(),
                from_node_table: "users".to_string(),
                to_node_table: "users".to_string(),
                from_id: Identifier::from("follower_id"),
                to_id: Identifier::from("followed_id"),
                from_node_id_dtype: SchemaType::Integer,
                to_node_id_dtype: SchemaType::Integer,
                property_mappings: HashMap::new(),
                view_parameters: None,
                engine: None,
                use_final: None,
                filter: None,
                edge_id: None,
                type_column: None,
                from_label_column: None,
                to_label_column: None,
                from_label_values: None,
                to_label_values: None,
                from_node_properties: None,
                to_node_properties: None,
                is_fk_edge: false,
                constraints: None,
                edge_id_types: None,
            },
        );

        // Create test row data
        let mut row = HashMap::new();
        row.insert("r.follower_id".to_string(), Value::Number(1.into()));
        row.insert("r.followed_id".to_string(), Value::Number(2.into()));
        row.insert(
            "r.follow_date".to_string(),
            Value::String("2024-01-15".to_string()),
        );

        // Transform
        let result = transform_to_relationship(
            &row,
            "r",
            &["FOLLOWS".to_string()],
            Some("User"),
            Some("User"),
            &schema,
        );

        assert!(result.is_ok());
        let rel = result.unwrap();
        assert_eq!(rel.rel_type, "FOLLOWS");
        // Uses relationship's FK columns: follower_id=1, followed_id=2
        assert_eq!(rel.element_id, "FOLLOWS:1->2");
        assert_eq!(rel.start_node_element_id, "User:1");
        assert_eq!(rel.end_node_element_id, "User:2");
        assert_eq!(
            rel.properties.get("follow_date").unwrap(),
            &Value::String("2024-01-15".to_string())
        );
    }

    /// Regression test: parse_composite_rel_key correctly splits "FOLLOWS::User::User" format
    #[test]
    fn test_parse_composite_rel_key_with_labels() {
        let (rel_type, from, to) = parse_composite_rel_key("FOLLOWS::User::User");
        assert_eq!(rel_type, "FOLLOWS");
        assert_eq!(from, Some("User".to_string()));
        assert_eq!(to, Some("User".to_string()));
    }

    #[test]
    fn test_parse_composite_rel_key_different_labels() {
        let (rel_type, from, to) = parse_composite_rel_key("LIKES::User::Post");
        assert_eq!(rel_type, "LIKES");
        assert_eq!(from, Some("User".to_string()));
        assert_eq!(to, Some("Post".to_string()));
    }

    #[test]
    fn test_parse_composite_rel_key_simple() {
        let (rel_type, from, to) = parse_composite_rel_key("FOLLOWS");
        assert_eq!(rel_type, "FOLLOWS");
        assert_eq!(from, None);
        assert_eq!(to, None);
    }

    /// Regression test: strip_composite_rel_types removes ::From::To suffixes from rel_types
    /// and infers from/to labels. Fixes undirected relationship queries leaking composite
    /// rel_type "FOLLOWS::User::User" in Bolt responses.
    #[test]
    fn test_strip_composite_rel_types_infers_labels() {
        let (types, from, to) =
            strip_composite_rel_types(&["FOLLOWS::User::User".to_string()], None, None);
        assert_eq!(types, vec!["FOLLOWS".to_string()]);
        assert_eq!(from, Some("User".to_string()));
        assert_eq!(to, Some("User".to_string()));
    }

    #[test]
    fn test_strip_composite_rel_types_preserves_existing_labels() {
        let (types, from, to) = strip_composite_rel_types(
            &["FOLLOWS::User::User".to_string()],
            Some("Person".to_string()),
            Some("Person".to_string()),
        );
        assert_eq!(types, vec!["FOLLOWS".to_string()]);
        // Existing labels take precedence
        assert_eq!(from, Some("Person".to_string()));
        assert_eq!(to, Some("Person".to_string()));
    }

    #[test]
    fn test_strip_composite_rel_types_simple_passthrough() {
        let (types, from, to) = strip_composite_rel_types(&["FOLLOWS".to_string()], None, None);
        assert_eq!(types, vec!["FOLLOWS".to_string()]);
        assert_eq!(from, None);
        assert_eq!(to, None);
    }

    // Integration-style test (requires more setup)
    // TODO: Add full transform_to_node test with mock schema
}
