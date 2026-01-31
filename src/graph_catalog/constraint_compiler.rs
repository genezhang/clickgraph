/// Edge Constraint Compiler
///
/// Compiles constraint expressions from schema definitions into SQL predicates.
/// Handles property reference resolution from graph properties (`from.property`, `to.property`)
/// to relational columns (`from_alias.column_name`, `to_alias.column_name`).
///
/// # Purpose
///
/// Edge constraints enable validation beyond basic ID joins:
/// - **Temporal ordering**: `from.timestamp <= to.timestamp`
/// - **Context preservation**: `from.context = to.context`
/// - **Composite constraints**: `from.tenant = to.tenant AND from.timestamp < to.timestamp`
///
/// These constraints are compiled and added to:
/// - Single-hop JOINs: Added to WHERE clause
/// - Variable-length path CTEs: Added to base case and recursive JOIN conditions
///
/// # Examples
///
/// ```no_run
/// use clickgraph::graph_catalog::constraint_compiler::compile_constraint;
/// use clickgraph::graph_catalog::graph_schema::NodeSchema;
/// use std::collections::HashMap;
///
/// // Given: from.timestamp <= to.timestamp
/// // Produces: from_node.created_timestamp <= to_node.created_timestamp
/// //
/// // (assuming property mapping: timestamp -> created_timestamp)
/// ```
use crate::graph_catalog::errors::GraphSchemaError;
use crate::graph_catalog::expression_parser::PropertyValue;
use crate::graph_catalog::graph_schema::NodeSchema;

/// Compile an edge constraint expression into SQL predicate
///
/// # Arguments
/// * `constraint_expr` - Raw constraint from schema (e.g., "from.timestamp <= to.timestamp")
/// * `from_node_schema` - Schema for from node (for property resolution)
/// * `to_node_schema` - Schema for to node (for property resolution)
/// * `from_alias` - SQL alias for from node table (e.g., "from_node")
/// * `to_alias` - SQL alias for to node table (e.g., "to_node")
///
/// # Returns
/// SQL predicate ready to use in WHERE/JOIN condition
///
/// # Errors
/// - Missing property in node schema
/// - Invalid property mapping (non-column expression)
/// - Syntax errors in constraint expression
pub fn compile_constraint(
    constraint_expr: &str,
    from_node_schema: &NodeSchema,
    to_node_schema: &NodeSchema,
    from_alias: &str,
    to_alias: &str,
) -> Result<String, GraphSchemaError> {
    // Simple regex-based property reference replacement
    // Pattern: from.property or to.property

    let mut compiled = constraint_expr.to_string();

    // Find and replace all property references
    // Using simple string processing for now (can upgrade to proper parser if needed)

    // Replace from.property references
    compiled = replace_property_references(&compiled, "from.", from_node_schema, from_alias)?;

    // Replace to.property references
    compiled = replace_property_references(&compiled, "to.", to_node_schema, to_alias)?;

    Ok(compiled)
}

/// Replace property references in constraint expression
///
/// Scans for `prefix.property` patterns and replaces with `alias.column`
fn replace_property_references(
    expr: &str,
    prefix: &str, // "from." or "to."
    node_schema: &NodeSchema,
    alias: &str,
) -> Result<String, GraphSchemaError> {
    let mut result = String::new();
    let mut remaining = expr;

    while let Some(pos) = remaining.find(prefix) {
        // Add everything before the match
        result.push_str(&remaining[..pos]);

        // Skip the prefix
        remaining = &remaining[pos + prefix.len()..];

        // Extract property name (alphanumeric + underscore)
        let property_end = remaining
            .find(|c: char| !c.is_alphanumeric() && c != '_')
            .unwrap_or(remaining.len());

        let property_name = &remaining[..property_end];

        // Resolve property to column
        let column_name = resolve_property_to_column(property_name, node_schema)?;

        // Append alias.column to result
        result.push_str(&format!("{}.{}", alias, column_name));

        // Continue with rest of expression
        remaining = &remaining[property_end..];
    }

    // Add any remaining text
    result.push_str(remaining);

    Ok(result)
}

/// Resolve a graph property name to relational column name(s)
///
/// Uses NodeSchema property_mappings to find the column
fn resolve_property_to_column(
    property: &str,
    node_schema: &NodeSchema,
) -> Result<String, GraphSchemaError> {
    // Look up property in mappings
    match node_schema.property_mappings.get(property) {
        Some(PropertyValue::Column(col)) => Ok(col.clone()),
        Some(PropertyValue::Expression(_)) => Err(GraphSchemaError::InvalidConfig {
            message: format!(
                "Property '{}' mapped to expression, cannot use in edge constraints. \
                     Constraints only support simple column mappings.",
                property
            ),
        }),
        None => {
            // Property not found - provide helpful error
            Err(GraphSchemaError::InvalidConfig {
                message: format!(
                    "Property '{}' not found in node label '{}'. \
                     Available properties: {}",
                    property,
                    node_schema.primary_keys, // Using primary_keys as label proxy
                    node_schema
                        .property_mappings
                        .keys()
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_catalog::graph_schema::NodeIdSchema;
    use std::collections::HashMap;

    fn create_test_node_schema(label: &str, properties: Vec<(&str, &str)>) -> NodeSchema {
        let mut property_mappings = HashMap::new();
        let mut column_names = vec![];

        for (prop, col) in properties {
            property_mappings.insert(prop.to_string(), PropertyValue::Column(col.to_string()));
            column_names.push(col.to_string());
        }

        NodeSchema {
            database: "test_db".to_string(),
            table_name: format!("{}_table", label),
            column_names,
            primary_keys: label.to_string(),
            node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
            property_mappings,
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: false,
            from_properties: None,
            to_properties: None,
            denormalized_source_table: None,
            label_column: None,
            label_value: None,
            node_id_types: None,
        }
    }

    #[test]
    fn test_compile_simple_temporal_constraint() {
        let from_schema = create_test_node_schema("File", vec![("timestamp", "created_at")]);
        let to_schema = from_schema.clone();

        let result = compile_constraint(
            "from.timestamp <= to.timestamp",
            &from_schema,
            &to_schema,
            "f",
            "t",
        )
        .unwrap();

        assert_eq!(result, "f.created_at <= t.created_at");
    }

    #[test]
    fn test_compile_composite_constraint() {
        let from_schema = create_test_node_schema(
            "File",
            vec![("timestamp", "created_at"), ("tenant", "tenant_id")],
        );
        let to_schema = from_schema.clone();

        let result = compile_constraint(
            "from.tenant = to.tenant AND from.timestamp < to.timestamp",
            &from_schema,
            &to_schema,
            "from_node",
            "to_node",
        )
        .unwrap();

        assert_eq!(
            result,
            "from_node.tenant_id = to_node.tenant_id AND from_node.created_at < to_node.created_at"
        );
    }

    #[test]
    fn test_compile_missing_property() {
        let from_schema = create_test_node_schema("File", vec![("timestamp", "created_at")]);
        let to_schema = from_schema.clone();

        let result = compile_constraint(
            "from.missing_prop = to.timestamp",
            &from_schema,
            &to_schema,
            "f",
            "t",
        );

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("missing_prop"));
        assert!(err_msg.contains("not found"));
    }

    #[test]
    fn test_compile_with_literals() {
        let from_schema = create_test_node_schema("Event", vec![("status", "event_status")]);
        let to_schema = from_schema.clone();

        let result = compile_constraint(
            "from.status = 'active' AND to.status = 'completed'",
            &from_schema,
            &to_schema,
            "e1",
            "e2",
        )
        .unwrap();

        assert_eq!(
            result,
            "e1.event_status = 'active' AND e2.event_status = 'completed'"
        );
    }

    #[test]
    fn test_compile_complex_expression() {
        let from_schema =
            create_test_node_schema("Node", vec![("val", "value"), ("limit", "max_value")]);
        let to_schema =
            create_test_node_schema("Node", vec![("val", "value"), ("threshold", "min_value")]);

        let result = compile_constraint(
            "(from.val + 10) <= to.threshold OR from.limit > to.val",
            &from_schema,
            &to_schema,
            "n1",
            "n2",
        )
        .unwrap();

        assert_eq!(
            result,
            "(n1.value + 10) <= n2.min_value OR n1.max_value > n2.value"
        );
    }
}
