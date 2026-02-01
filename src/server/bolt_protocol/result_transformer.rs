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
        /// Whether this is a VLP (variable-length path)
        is_vlp: bool,
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
                        ReturnItemType::Path {
                            start_alias: path_var.start_node.clone(),
                            end_alias: path_var.end_node.clone(),
                            rel_alias: path_var.relationship.clone(),
                            is_vlp,
                        }
                    }
                    _ => {
                        // Scalar variable or not found
                        ReturnItemType::Scalar
                    }
                }
            }
            _ => {
                // Property access, function call, expression → Scalar
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
) -> Result<Vec<BoltValue>, String> {
    // Check for multi-label scan results (has {alias}_label, {alias}_id, {alias}_properties columns)
    if let Some(transformed) = try_transform_multi_label_row(&row, metadata)? {
        return Ok(transformed);
    }

    let mut result = Vec::new();

    for meta in metadata {
        match &meta.item_type {
            ReturnItemType::Node { labels } => {
                let node = transform_to_node(&row, &meta.field_name, labels, schema)?;
                // Use the Node's packstream encoding
                let packstream_bytes = node.to_packstream();
                result.push(BoltValue::PackstreamBytes(packstream_bytes));
            }
            ReturnItemType::Relationship {
                rel_types,
                from_label,
                to_label,
            } => {
                let rel = transform_to_relationship(
                    &row,
                    &meta.field_name,
                    rel_types,
                    from_label.as_deref(),
                    to_label.as_deref(),
                    schema,
                )?;
                // Use the Relationship's packstream encoding
                let packstream_bytes = rel.to_packstream();
                result.push(BoltValue::PackstreamBytes(packstream_bytes));
            }
            ReturnItemType::Path {
                start_alias,
                end_alias,
                rel_alias,
                is_vlp,
            } => {
                // Transform fixed-hop path to Neo4j Path structure
                // For now, we support fixed single-hop paths: (a)-[r]->(b)
                //
                // The SQL returns: tuple('fixed_path', start_alias, end_alias, rel_alias)
                // We need to look up component data from the row and construct Path
                
                if *is_vlp {
                    // VLP paths have different column format - not yet supported
                    log::warn!("VLP path transformation not yet implemented");
                    result.push(BoltValue::Json(Value::Null));
                    continue;
                }
                
                // For fixed-hop paths, try to construct a minimal path
                // The actual node/relationship data might be in the row with prefixed columns
                let path = transform_to_path(
                    &row,
                    &meta.field_name,
                    start_alias.as_deref(),
                    end_alias.as_deref(),
                    rel_alias.as_deref(),
                    schema,
                    metadata,  // Pass the metadata from transform_row
                )?;
                
                // Use the Path's packstream encoding
                let packstream_bytes = path.to_packstream();
                result.push(BoltValue::PackstreamBytes(packstream_bytes));
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
            properties.insert(prop_name.to_string(), value.clone());
        }
    }

    // Get primary label
    let label = labels
        .first()
        .ok_or_else(|| format!("No label for node variable: {}", var_name))?;

    // Get node schema
    let node_schema = schema
        .node_schema_opt(label)
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
    let element_id = generate_node_element_id(label, &id_value_refs);

    // Try to extract numeric ID for legacy `id` field
    // For single-column numeric IDs, parse as i64; otherwise use 0
    let legacy_id: i64 = if id_values.len() == 1 {
        id_values[0].parse().unwrap_or(0)
    } else {
        0 // Composite IDs can't be represented as single i64
    };

    // Create Node struct
    Ok(Node {
        id: legacy_id,
        labels: labels.to_vec(),
        properties,
        element_id,
    })
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

    // Create Relationship struct
    Ok(Relationship {
        id: 0,            // Legacy ID (unused in Neo4j 5.x)
        start_node_id: 0, // Legacy ID
        end_node_id: 0,   // Legacy ID
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
/// 1. Looks up metadata for start/end nodes and relationship from other return items
/// 2. Creates placeholder Node/Relationship objects with IDs derived from path tuple
/// 3. Constructs a Path with proper structure for Neo4j visualization
///
/// # Arguments
///
/// * `row` - The result row from ClickHouse
/// * `path_name` - The path variable name (e.g., "p")
/// * `start_alias` - Alias of the start node
/// * `end_alias` - Alias of the end node
/// * `rel_alias` - Alias of the relationship
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
    schema: &GraphSchema,
    return_metadata: &[ReturnItemMetadata],
) -> Result<Path, String> {
    // Try to find component metadata from the return items
    // This works when the user returns both the path and its components
    
    let start_alias = start_alias.unwrap_or("_start");
    let end_alias = end_alias.unwrap_or("_end");
    let rel_alias = rel_alias.unwrap_or("_rel");
    
    log::debug!(
        "transform_to_path: path={}, start={}, end={}, rel={}",
        path_name, start_alias, end_alias, rel_alias
    );
    
    // Try to get start node data
    let start_node = find_node_in_row(row, start_alias, return_metadata, schema)
        .unwrap_or_else(|| create_placeholder_node(start_alias, 0));
    
    // Try to get end node data
    let end_node = find_node_in_row(row, end_alias, return_metadata, schema)
        .unwrap_or_else(|| create_placeholder_node(end_alias, 1));
    
    // Try to get relationship data
    let relationship = find_relationship_in_row(
        row, rel_alias, &start_node.element_id, &end_node.element_id, return_metadata, schema
    ).unwrap_or_else(|| create_placeholder_relationship(
        rel_alias,
        &start_node.element_id,
        &end_node.element_id,
    ));
    
    // Create Path with single-hop structure
    // Indices for single hop: [1, 1] means "relationship index 1, then node index 1"
    // (Neo4j uses 1-based indexing in path indices)
    Ok(Path::single_hop(start_node, relationship, end_node))
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
        let schema_props: std::collections::HashSet<&String> = node_schema.property_mappings.keys().collect();
        let row_props: std::collections::HashSet<&str> = properties.keys().map(|s| s.as_str()).collect();
        
        // If most row properties match schema properties, this is likely the right label
        let matches = row_props.iter().filter(|p| schema_props.iter().any(|sp| sp.as_str() == **p)).count();
        if matches > 0 && matches >= row_props.len() / 2 {
            // Found a matching label
            let id_columns = node_schema.node_id.id.columns();
            let id_values: Vec<String> = id_columns
                .iter()
                .filter_map(|col| properties.get(*col).and_then(value_to_string))
                .collect();
            
            if !id_values.is_empty() {
                let element_id = format!("{}:{}", label, id_values.join("|"));
                let id: i64 = id_values.first().and_then(|s| s.parse().ok()).unwrap_or(0);
                
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
            if let ReturnItemType::Relationship { rel_types, from_label, to_label } = &meta.item_type {
                // Found it! Try to transform
                return transform_to_relationship(
                    row, alias, rel_types,
                    from_label.as_deref(), to_label.as_deref(), schema
                ).ok();
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
            let from_id = properties.get(from_col).and_then(value_to_string).unwrap_or_default();
            let to_id = properties.get(to_col).and_then(value_to_string).unwrap_or_default();
            let element_id = format!("{}:{}->{}",rel_type, from_id, to_id);
            
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

/// Create a placeholder node when we can't find actual data
fn create_placeholder_node(alias: &str, id: i64) -> Node {
    Node {
        id,
        labels: vec!["_Unknown".to_string()],
        properties: std::collections::HashMap::new(),
        element_id: format!("_Unknown:{}", id),
    }
}

/// Create a placeholder relationship when we can't find actual data
fn create_placeholder_relationship(
    alias: &str,
    start_element_id: &str,
    end_element_id: &str,
) -> Relationship {
    Relationship {
        id: 0,
        start_node_id: 0,
        end_node_id: 0,
        rel_type: "_UNKNOWN".to_string(),
        properties: std::collections::HashMap::new(),
        element_id: format!("_UNKNOWN:{}:{}", start_element_id, end_element_id),
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
                    map.iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect()
                }
                _ => HashMap::new(),
            };

            // Generate element_id
            let element_id = generate_node_element_id(&label, &[&id]);

            // Try to parse ID as numeric for legacy `id` field
            let legacy_id: i64 = id.parse().unwrap_or(0);

            // Create Node
            let node = Node::new(legacy_id, vec![label], properties, element_id);
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
