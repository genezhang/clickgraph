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
        element_id::generate_node_element_id,
        graph_schema::GraphSchema,
    },
    query_planner::{
        logical_expr::LogicalExpr,
        logical_plan::{LogicalPlan, Projection},
        plan_ctx::PlanCtx,
        typed_variable::TypedVariable,
    },
    server::bolt_protocol::graph_objects::Node,
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
/// Traverses through OrderBy, Limit, Skip wrappers to find the underlying Projection
fn find_final_projection(plan: &LogicalPlan) -> Result<&Projection, String> {
    match plan {
        LogicalPlan::Projection(proj) => Ok(proj),
        LogicalPlan::OrderBy(order_by) => find_final_projection(&order_by.input),
        LogicalPlan::Limit(limit) => find_final_projection(&limit.input),
        LogicalPlan::Skip(skip) => find_final_projection(&skip.input),
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
) -> Result<Vec<Value>, String> {
    let mut result = Vec::new();

    for meta in metadata {
        match &meta.item_type {
            ReturnItemType::Node { labels } => {
                let node = transform_to_node(&row, &meta.field_name, labels, schema)?;
                // TODO: Wrap in packstream Value in next iteration
                // For now, just serialize properties as JSON
                let mut node_props = node.properties.clone();
                node_props.insert("__element_id".to_string(), Value::String(node.element_id.clone()));
                node_props.insert("__labels".to_string(), serde_json::to_value(&node.labels).unwrap());
                result.push(Value::Object(node_props.into_iter().collect()));
            }
            ReturnItemType::Relationship { .. } => {
                // TODO: Relationship transformation in Iteration 2
                result.push(Value::Null);
            }
            ReturnItemType::Path => {
                // TODO: Path transformation in Iteration 3
                result.push(Value::Null);
            }
            ReturnItemType::Scalar => {
                // For scalars, just extract the value directly
                let value = row.get(&meta.field_name).cloned().unwrap_or(Value::Null);
                result.push(value);
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

    // Integration-style test (requires more setup)
    // TODO: Add full transform_to_node test with mock schema
}
