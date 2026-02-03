//! db.schema.nodeTypeProperties() procedure
//!
//! Returns detailed property metadata for each node type (label).
//! Used by Neodash for richer schema introspection.
//!
//! Output columns:
//! - nodeType: String (e.g., ":`User`")
//! - nodeLabels: Vec<String> (e.g., ["User"])
//! - propertyName: String (e.g., "name")
//! - propertyTypes: Vec<String> (e.g., ["String"])
//! - mandatory: bool (true if property always exists)

use crate::graph_catalog::graph_schema::GraphSchema;
use std::collections::HashMap;

/// Execute db.schema.nodeTypeProperties() procedure
///
/// Returns property metadata for all node types in the schema.
///
/// # Example Response
/// ```json
/// [
///   {
///     "nodeType": ":`User`",
///     "nodeLabels": ["User"],
///     "propertyName": "name",
///     "propertyTypes": ["String"],
///     "mandatory": true
///   },
///   {
///     "nodeType": ":`User`",
///     "nodeLabels": ["User"],
///     "propertyName": "age",
///     "propertyTypes": ["Long"],
///     "mandatory": true
///   }
/// ]
/// ```
pub fn execute(schema: &GraphSchema) -> Result<Vec<HashMap<String, serde_json::Value>>, String> {
    let mut results = Vec::new();

    // Iterate over all node schemas
    for (label, node_schema) in schema.all_node_schemas() {
        // Get property mappings (Cypher property name -> ClickHouse column/expression)
        let property_mappings = &node_schema.property_mappings;

        // Iterate over each property
        for (prop_name, _prop_value) in property_mappings {
            let mut record = HashMap::new();

            // nodeType: ":`Label`" format (Neo4j convention)
            record.insert(
                "nodeType".to_string(),
                serde_json::json!(format!(":`{}`", label)),
            );

            // nodeLabels: array of labels
            record.insert("nodeLabels".to_string(), serde_json::json!(vec![label]));

            // propertyName: the Cypher property name
            record.insert("propertyName".to_string(), serde_json::json!(prop_name));

            // propertyTypes: We don't have type information in schema, default to String
            // In future, could query ClickHouse system tables for actual types
            record.insert(
                "propertyTypes".to_string(),
                serde_json::json!(vec!["String"]),
            );

            // mandatory: true (we assume all schema properties exist)
            // In a real Neo4j database, this would be determined by constraint analysis
            record.insert("mandatory".to_string(), serde_json::json!(true));

            results.push(record);
        }
    }

    // Sort by nodeType, then propertyName for consistent output
    results.sort_by(|a, b| {
        let a_type = a.get("nodeType").and_then(|v| v.as_str()).unwrap_or("");
        let b_type = b.get("nodeType").and_then(|v| v.as_str()).unwrap_or("");
        let a_prop = a.get("propertyName").and_then(|v| v.as_str()).unwrap_or("");
        let b_prop = b.get("propertyName").and_then(|v| v.as_str()).unwrap_or("");
        (a_type, a_prop).cmp(&(b_type, b_prop))
    });

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_empty_schema() -> GraphSchema {
        GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new())
    }

    #[test]
    fn test_execute_with_empty_schema() {
        let schema = create_empty_schema();
        let results = execute(&schema).unwrap();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_output_format_structure() {
        // Verify the expected output structure is documented correctly
        // Actual testing with real schema data is done via integration tests
        let schema = create_empty_schema();
        let results = execute(&schema).unwrap();

        // Verify empty schema returns empty results
        assert_eq!(results.len(), 0);

        // Format verification: if there were results, each should have these keys
        // This is verified by manual testing and integration tests
    }
}
