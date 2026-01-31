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
        graph_objects::{Node, Relationship},
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
    Path,
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
        log::debug!("Projection item: field_name={}, expr={:?}", field_name, proj_item.expression);

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
                    Some(TypedVariable::Path(_)) => ReturnItemType::Path,
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
/// Traverses through OrderBy, Limit, Skip, GraphJoins wrappers to find the underlying Projection
fn find_final_projection(plan: &LogicalPlan) -> Result<&Projection, String> {
    match plan {
        LogicalPlan::Projection(proj) => Ok(proj),
        LogicalPlan::OrderBy(order_by) => find_final_projection(&order_by.input),
        LogicalPlan::Limit(limit) => find_final_projection(&limit.input),
        LogicalPlan::Skip(skip) => find_final_projection(&skip.input),
        LogicalPlan::GraphJoins(joins) => find_final_projection(&joins.input),
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
    let mut result = Vec::new();

    for meta in metadata {
        match &meta.item_type {
            ReturnItemType::Node { labels } => {
                let node = transform_to_node(&row, &meta.field_name, labels, schema)?;
                // Use the Node's packstream encoding
                let packstream_bytes = node.to_packstream();
                result.push(BoltValue::PackstreamBytes(packstream_bytes));
            }
            ReturnItemType::Relationship { rel_types, from_label, to_label } => {
                let rel = transform_to_relationship(
                    &row, 
                    &meta.field_name, 
                    rel_types, 
                    from_label.as_deref(),
                    to_label.as_deref(),
                    schema
                )?;
                // Use the Relationship's packstream encoding
                let packstream_bytes = rel.to_packstream();
                result.push(BoltValue::PackstreamBytes(packstream_bytes));
            }
            ReturnItemType::Path => {
                // TODO: Path transformation requires path variable support in query planner
                result.push(BoltValue::Json(Value::Null));
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

    // Create Node struct
    Ok(Node {
        id: 0, // Legacy ID (unused in Neo4j 5.x)
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
                .ok_or_else(|| format!("Missing from_id column '{}' for relationship '{}'", col_name, var_name))
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
                .ok_or_else(|| format!("Missing to_id column '{}' for relationship '{}'", col_name, var_name))
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
        id: 0, // Legacy ID (unused in Neo4j 5.x)
        start_node_id: 0, // Legacy ID
        end_node_id: 0,   // Legacy ID
        rel_type: rel_type.to_string(),
        properties,
        element_id,
        start_node_element_id,
        end_node_element_id,
    })
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
        use crate::query_planner::{logical_plan::ProjectionItem, logical_expr::TableAlias};

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
        let mut schema = GraphSchema::build(
            1,
            "test".to_string(),
            HashMap::new(),
            HashMap::new(),
        );
        
        // Add relationship schema for FOLLOWS
        schema.insert_relationship_schema(
            "FOLLOWS".to_string(),
            RelationshipSchema {
                database: "test".to_string(),
                table_name: "follows".to_string(),
                column_names: vec!["follower_id".to_string(), "followed_id".to_string(), "follow_date".to_string()],
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
        row.insert("r.follow_date".to_string(), Value::String("2024-01-15".to_string()));

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
        assert_eq!(rel.properties.get("follow_date").unwrap(), &Value::String("2024-01-15".to_string()));
    }

    // Integration-style test (requires more setup)
    // TODO: Add full transform_to_node test with mock schema
}
