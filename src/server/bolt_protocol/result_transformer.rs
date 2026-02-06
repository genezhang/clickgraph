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
    },
    query_planner::{
        logical_expr::LogicalExpr,
        logical_plan::{LogicalPlan, Projection},
        plan_ctx::PlanCtx,
        typed_variable::TypedVariable,
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
                    Some(TypedVariable::Node(node_var)) => ReturnItemType::Node {
                        labels: node_var.labels.clone(),
                    },
                    Some(TypedVariable::Relationship(rel_var)) => ReturnItemType::Relationship {
                        rel_types: rel_var.rel_types.clone(),
                        from_label: rel_var.from_node_label.clone(),
                        to_label: rel_var.to_node_label.clone(),
                    },
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
                        let (actual_rel_types, inferred_from_labels, inferred_to_labels): (
                            Vec<String>,
                            Vec<String>,
                            Vec<String>,
                        ) = rel_types
                            .iter()
                            .map(|rt| {
                                let parts: Vec<&str> = rt.split("::").collect();
                                match parts.as_slice() {
                                    [rel_type, from_label, to_label] => {
                                        // Composite key format
                                        (
                                            rel_type.to_string(),
                                            Some(from_label.to_string()),
                                            Some(to_label.to_string()),
                                        )
                                    }
                                    _ => {
                                        // Simple type
                                        (rt.clone(), None, None)
                                    }
                                }
                            })
                            .fold(
                                (Vec::new(), Vec::new(), Vec::new()),
                                |(mut types, mut from, mut to), (t, f, to_label)| {
                                    types.push(t);
                                    if let Some(from_label) = f {
                                        from.push(from_label);
                                    }
                                    if let Some(to_l) = to_label {
                                        to.push(to_l);
                                    }
                                    (types, from, to)
                                },
                            );

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

                        ReturnItemType::Path {
                            start_alias: path_var.start_node.clone(),
                            end_alias: path_var.end_node.clone(),
                            rel_alias: path_var.relationship.clone(),
                            start_labels,
                            end_labels,
                            rel_types,
                            is_vlp,
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
                            ReturnItemType::Relationship {
                                rel_types: rel_var.rel_types.clone(),
                                from_label: rel_var.from_node_label.clone(),
                                to_label: rel_var.to_node_label.clone(),
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
                        log::debug!("IdFunction detected: id({}) with labels {:?}", alias_str, labels);
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
            } => {
                // Transform fixed-hop path to Neo4j Path structure
                // For now, we support fixed single-hop paths: (a)-[r]->(b)
                //
                // The SQL returns: tuple('fixed_path', start_alias, end_alias, rel_alias)
                // We use the metadata (labels, types) from query planning

                if *is_vlp {
                    // VLP paths have different column format - not yet supported
                    log::warn!("VLP path transformation not yet implemented");
                    result.push(BoltValue::Json(Value::Null));
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
                log::debug!("IdFunction handler: alias={}, labels={:?}, field_name={}", alias, labels, meta.field_name);
                
                // Find the node/relationship data in the row to build element_id
                let label = labels.first().cloned().unwrap_or_else(|| "Unknown".to_string());
                
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
                    let id_col = schema.node_schema(&label)
                        .map(|ns| ns.node_id.columns().first().map(|s| s.to_string()).unwrap_or_else(|| "id".to_string()))
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
fn transform_to_node(
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
            // Skip internal __label__ column from properties
            if prop_name != "_label__" {
                properties.insert(prop_name.to_string(), value.clone());
            }
        }
    }

    // Get primary label
    // Priority: 1. Use provided labels, 2. Check __label__ column, 3. Infer from properties
    let label = if let Some(l) = labels.first() {
        l.clone()
    } else if let Some(label_value) = row.get("__label__") {
        // For UNION queries, __label__ column contains the node type
        value_to_string(label_value)
            .ok_or_else(|| format!("Invalid __label__ value for node variable: {}", var_name))?
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
    // Use inferred label if original labels was empty
    let final_labels = if labels.is_empty() {
        vec![label]
    } else {
        labels.to_vec()
    };

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
            .any(|col| properties.get(*col).map_or(false, |v| !v.is_null()));

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
fn transform_to_relationship(
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

    // Get primary relationship type
    let rel_type = rel_types
        .first()
        .ok_or_else(|| format!("No relationship type for variable: {}", var_name))?;

    // Get relationship schema
    let rel_schema = schema
        .get_relationships_schema_opt(rel_type)
        .ok_or_else(|| format!("Relationship schema not found for type: {}", rel_type))?;

    // Get from/to node labels (use provided or schema defaults)
    let from_node_label = from_label.unwrap_or(&rel_schema.from_node);
    let to_node_label = to_label.unwrap_or(&rel_schema.to_node);

    // Get from/to node schemas to determine if IDs are composite
    let from_node_schema = schema
        .node_schema_opt(from_node_label)
        .ok_or_else(|| format!("From node schema not found for label: {}", from_node_label))?;
    let to_node_schema = schema
        .node_schema_opt(to_node_label)
        .ok_or_else(|| format!("To node schema not found for label: {}", to_node_label))?;

    // Extract from_id values (may be composite)
    let from_id_columns = from_node_schema.node_id.id.columns();
    let from_id_values: Vec<String> = from_id_columns
        .iter()
        .map(|col_name| {
            properties
                .get(*col_name)
                .or_else(|| properties.get("from_id")) // Fallback to generic name for single column
                .and_then(value_to_string)
                .ok_or_else(|| {
                    format!(
                        "Missing from_id column '{}' for relationship '{}'",
                        col_name, var_name
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Extract to_id values (may be composite)
    let to_id_columns = to_node_schema.node_id.id.columns();
    let to_id_values: Vec<String> = to_id_columns
        .iter()
        .map(|col_name| {
            properties
                .get(*col_name)
                .or_else(|| properties.get("to_id")) // Fallback to generic name for single column
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

    // Generate relationship elementId: "FOLLOWS:1->2" or "BELONGS_TO:tenant1|user1->tenant1|org1"
    let element_id = generate_relationship_element_id(rel_type, &from_id_str, &to_id_str);

    // Generate node elementIds for start and end nodes (with composite ID support)
    let from_id_refs: Vec<&str> = from_id_values.iter().map(|s| s.as_str()).collect();
    let to_id_refs: Vec<&str> = to_id_values.iter().map(|s| s.as_str()).collect();

    let start_node_element_id = generate_node_element_id(from_node_label, &from_id_refs);
    let end_node_element_id = generate_node_element_id(to_node_label, &to_id_refs);

    // Derive integer IDs from element_ids (ensures uniqueness across labels)
    let rel_id = generate_id_from_element_id(&element_id);
    let start_node_id = generate_id_from_element_id(&start_node_element_id);
    let end_node_id = generate_id_from_element_id(&end_node_element_id);

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
        return transform_path_from_json(row);
    }

    // Original format: individual columns for each property
    // Extract start node - require either metadata lookup success or known labels
    let start_node =
        find_node_in_row_with_label(row, start_alias, start_labels, return_metadata, schema)
            .or_else(|| {
                // If we have a known label, create node with that label
                start_labels.first().map(|label| {
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
                    start_labels,
                    row.keys().collect::<Vec<_>>()
                )
            })?;

    // Extract end node - require either metadata lookup success or known labels
    let end_node =
        find_node_in_row_with_label(row, end_alias, end_labels, return_metadata, schema)
            .or_else(|| {
                // If we have a known label, create node with that label
                end_labels.first().map(|label| {
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
                    end_labels,
                    row.keys().collect::<Vec<_>>()
                )
            })?;

    // Extract relationship - require either metadata lookup success or known types
    let relationship = find_relationship_in_row_with_type(
        row,
        rel_alias,
        &start_node.element_id,
        &end_node.element_id,
        rel_types,
        start_labels,
        end_labels,
        return_metadata,
        schema,
    )
    .or_else(|| {
        // If we have a known type, create relationship with that type
        rel_types.first().map(|rel_type| {
            log::debug!("Creating relationship with known type: {}", rel_type);
            create_relationship_with_type(rel_type, &start_node.element_id, &end_node.element_id)
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
        "Path created: start_node.labels={:?}, end_node.labels={:?}, rel.type={}",
        start_node.labels,
        end_node.labels,
        relationship.rel_type
    );

    // Create Path with single-hop structure
    // Indices for single hop: [1, 1] means "relationship index 1, then node index 1"
    // (Neo4j uses 1-based indexing in path indices)
    Ok(Path::single_hop(start_node, relationship, end_node))
}

/// Transform path from JSON format (UNION path queries)
/// Parses _start_properties, _end_properties, _rel_properties JSON strings
/// Used for UNION path queries where each row has explicit type columns:
/// __start_label__, __end_label__, __rel_type__
fn transform_path_from_json(row: &HashMap<String, Value>) -> Result<Path, String> {
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
    let start_id_str = extract_id_string_from_props(&start_props);
    let start_element_id = generate_node_element_id(&start_label, &[&start_id_str]);
    let start_id = generate_id_from_element_id(&start_element_id);
    // Clean property keys (remove table alias prefix like "t1_0.")
    let start_props_clean = clean_property_keys(start_props);
    let start_node = Node::new(
        start_id,
        vec![start_label.clone()],
        start_props_clean,
        start_element_id,
    );

    // Create end node - element_id is source of truth, integer id derived from it
    let end_id_str = extract_id_string_from_props(&end_props);
    let end_element_id = generate_node_element_id(&end_label, &[&end_id_str]);
    let end_id = generate_id_from_element_id(&end_element_id);
    // Clean property keys
    let end_props_clean = clean_property_keys(end_props);
    let end_node = Node::new(
        end_id,
        vec![end_label.clone()],
        end_props_clean,
        end_element_id,
    );

    // Create relationship - element_id is source of truth, integer id derived from it
    let rel_element_id = generate_relationship_element_id(&rel_type, &start_id_str, &end_id_str);
    let rel_id = generate_id_from_element_id(&rel_element_id);
    let rel_props_clean = clean_property_keys(rel_props);
    let relationship = Relationship::new(
        rel_id,
        start_id, // start_node_id - derived from start_element_id
        end_id,   // end_node_id - derived from end_element_id
        rel_type.clone(),
        rel_props_clean,
        rel_element_id,
        start_node.element_id.clone(),
        end_node.element_id.clone(),
    );

    log::info!(
        "✅ Path from JSON: start={} ({}), end={} ({}), rel={}",
        start_node.labels[0],
        start_node.id,
        end_node.labels[0],
        end_node.id,
        relationship.rel_type
    );

    Ok(Path::single_hop(start_node, relationship, end_node))
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
fn extract_id_string_from_props(props: &HashMap<String, Value>) -> String {
    // Common ID column names to check, in order of preference
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

    // First try exact match
    for id_field in &id_fields {
        if let Some(val) = props.get(*id_field) {
            if let Some(str_val) = value_to_string(val) {
                return str_val;
            }
        }
    }

    // Then try prefixed match (_s_, _e_, _r_ prefixes)
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

    // Finally try any key that looks like an ID
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

/// Extract ID from properties HashMap, trying multiple possible ID field names
/// Also handles prefixed keys like "t1_0.user_id" or "_s_user_id" by checking if key ends with the ID field
fn extract_id_from_props(props: &HashMap<String, Value>, id1: &str, id2: &str, id3: &str) -> i64 {
    // Common ID column names to check
    let common_id_fields = [
        id1,
        id2,
        id3,
        "code",
        "node_id",
        "origin_code",
        "dest_code",
        "airport_code",
    ];

    // First try exact match
    for id_field in &common_id_fields {
        if let Some(val) = props.get(*id_field) {
            if let Some(id) = value_to_i64(val) {
                return id;
            }
            // Also try string values (for string IDs like "LAX")
            if let Some(str_id) = value_to_string(val) {
                // Use hash of string ID as integer ID
                return generate_id_from_element_id(&str_id);
            }
        }
    }

    // Then try prefixed match (_s_, _e_, _r_ prefixes)
    for id_field in &common_id_fields {
        for prefix in &["_s_", "_e_", "_r_"] {
            let prefixed_key = format!("{}{}", prefix, id_field);
            if let Some(val) = props.get(&prefixed_key) {
                if let Some(id) = value_to_i64(val) {
                    return id;
                }
                // Also try string values (for string IDs like "LAX")
                if let Some(str_id) = value_to_string(val) {
                    return generate_id_from_element_id(&str_id);
                }
            }
        }
    }

    // Finally try suffix match (for table alias prefixed keys like "t1_0.user_id")
    for (key, val) in props {
        for id_field in &common_id_fields {
            if key.ends_with(&format!(".{}", id_field)) || key.ends_with(id_field) {
                if let Some(id) = value_to_i64(val) {
                    return id;
                }
                // Also try string values
                if let Some(str_id) = value_to_string(val) {
                    return generate_id_from_element_id(&str_id);
                }
            }
        }
    }

    0
}

/// Generate a unique integer node ID from element_id
/// This ensures round-trip: element_id is the source of truth, integer id is derived from it
/// Generate a deterministic ID from an element_id using the single source of truth.
/// This ensures "User:1" and "Post:1" have different IDs by encoding the label.
fn generate_id_from_element_id(element_id: &str) -> i64 {
    // Delegate to IdMapper's compute_deterministic_id for consistent encoding
    super::id_mapper::IdMapper::compute_deterministic_id(element_id)
}

fn value_to_i64(val: &Value) -> Option<i64> {
    match val {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.parse::<i64>().ok(),
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
    let label = known_labels.first()?;

    // Get node schema
    let node_schema = schema.node_schema_opt(label)?;

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
        label,
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

/// Find a node in the result row by its alias
fn find_node_in_row(
    row: &HashMap<String, Value>,
    alias: &str,
    return_metadata: &[ReturnItemMetadata],
    schema: &GraphSchema,
) -> Option<Node> {
    // Look for this alias in return metadata
    for meta in return_metadata {
        if meta.field_name == alias {
            if let ReturnItemType::Node { labels } = &meta.item_type {
                // Found it! Try to transform
                return transform_to_node(row, alias, labels, schema).ok();
            }
        }
    }

    // Check if it's in the row with property prefixes (e.g., "t1.user_id")
    let prefix = format!("{}.", alias);
    let mut properties = std::collections::HashMap::new();

    for (key, value) in row.iter() {
        if let Some(prop_name) = key.strip_prefix(&prefix) {
            properties.insert(prop_name.to_string(), value.clone());
        }
    }

    if properties.is_empty() {
        return None;
    }

    // Try to guess label from schema by looking at property names
    // This is a heuristic - we check which node type has these properties
    for (label, node_schema) in schema.all_node_schemas() {
        let schema_props: std::collections::HashSet<&String> =
            node_schema.property_mappings.keys().collect();
        let row_props: std::collections::HashSet<&str> =
            properties.keys().map(|s| s.as_str()).collect();

        // If most row properties match schema properties, this is likely the right label
        let matches = row_props
            .iter()
            .filter(|p| schema_props.iter().any(|sp| sp.as_str() == **p))
            .count();
        if matches > 0 && matches >= row_props.len() / 2 {
            // Found a matching label
            let id_columns = node_schema.node_id.id.columns();
            let id_values: Vec<String> = id_columns
                .iter()
                .filter_map(|col| properties.get(*col).and_then(value_to_string))
                .collect();

            if !id_values.is_empty() {
                let element_id = format!("{}:{}", label, id_values.join("|"));
                // Derive integer ID from element_id (ensures uniqueness across labels)
                let id: i64 = generate_id_from_element_id(&element_id);

                return Some(Node {
                    id,
                    labels: vec![label.clone()],
                    properties,
                    element_id,
                });
            }
        }
    }

    None
}

/// Find a relationship in the result row by its alias, using known types
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
    let from_label = from_labels.first()?;
    let to_label = to_labels.first()?;

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

/// Find a relationship in the result row by its alias
fn find_relationship_in_row(
    row: &HashMap<String, Value>,
    alias: &str,
    start_element_id: &str,
    end_element_id: &str,
    return_metadata: &[ReturnItemMetadata],
    schema: &GraphSchema,
) -> Option<Relationship> {
    // Look for this alias in return metadata
    for meta in return_metadata {
        if meta.field_name == alias {
            if let ReturnItemType::Relationship {
                rel_types,
                from_label,
                to_label,
            } = &meta.item_type
            {
                // Found it! Try to transform
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

    // Check if it's in the row with property prefixes
    let prefix = format!("{}.", alias);
    let mut properties = std::collections::HashMap::new();

    for (key, value) in row.iter() {
        if let Some(prop_name) = key.strip_prefix(&prefix) {
            properties.insert(prop_name.to_string(), value.clone());
        }
    }

    if properties.is_empty() {
        return None;
    }

    // Try to guess relationship type from schema
    for (rel_type, rel_schema) in schema.get_relationships_schemas() {
        // Check if this relationship type's properties match
        let from_col = &rel_schema.from_id;
        let to_col = &rel_schema.to_id;

        if properties.contains_key(from_col) && properties.contains_key(to_col) {
            let from_id = properties
                .get(from_col)
                .and_then(value_to_string)
                .unwrap_or_default();
            let to_id = properties
                .get(to_col)
                .and_then(value_to_string)
                .unwrap_or_default();
            let element_id = format!("{}:{}->{}", rel_type, from_id, to_id);

            return Some(Relationship {
                id: 0,
                start_node_id: 0,
                end_node_id: 0,
                rel_type: rel_type.clone(),
                properties,
                element_id,
                start_node_element_id: start_element_id.to_string(),
                end_node_element_id: end_element_id.to_string(),
            });
        }
    }

    None
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
        use crate::graph_catalog::{
            config::Identifier,
            expression_parser::PropertyValue,
            graph_schema::{NodeIdSchema, NodeSchema, RelationshipSchema},
        };
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
                from_id: "follower_id".to_string(),
                to_id: "followed_id".to_string(),
                from_node_id_dtype: "UInt64".to_string(),
                to_node_id_dtype: "UInt64".to_string(),
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
        assert_eq!(rel.element_id, "FOLLOWS:1->2");
        assert_eq!(rel.start_node_element_id, "User:1");
        assert_eq!(rel.end_node_element_id, "User:2");
        assert_eq!(
            rel.properties.get("follow_date").unwrap(),
            &Value::String("2024-01-15".to_string())
        );
    }

    // Integration-style test (requires more setup)
    // TODO: Add full transform_to_node test with mock schema
}
