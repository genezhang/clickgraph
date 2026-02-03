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

/// Map ClickHouse column types to Neo4j type names
fn map_clickhouse_to_neo4j_type(ch_type: &str) -> String {
    match ch_type {
        // Integer types
        "UInt64" | "Int64" | "UInt32" | "Int32" | "UInt16" | "Int16" | "UInt8" | "Int8" => {
            "Long".to_string()
        }
        // String types
        "String" | "FixedString" => "String".to_string(),
        // Date types
        "Date" | "Date32" => "Date".to_string(),
        // DateTime types
        "DateTime" | "DateTime64" => "DateTime".to_string(),
        // Float types
        "Float32" | "Float64" => "Double".to_string(),
        // Boolean (ClickHouse uses UInt8 for bool)
        "Bool" => "Boolean".to_string(),
        // Array types
        t if t.starts_with("Array(") => "List".to_string(),
        // Default fallback
        _ => "String".to_string(),
    }
}

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

    #[test]
    fn test_map_clickhouse_to_neo4j_type() {
        assert_eq!(map_clickhouse_to_neo4j_type("UInt64"), "Long");
        assert_eq!(map_clickhouse_to_neo4j_type("Int32"), "Long");
        assert_eq!(map_clickhouse_to_neo4j_type("String"), "String");
        assert_eq!(map_clickhouse_to_neo4j_type("DateTime64"), "DateTime");
        assert_eq!(map_clickhouse_to_neo4j_type("Date"), "Date");
        assert_eq!(map_clickhouse_to_neo4j_type("Float64"), "Double");
        assert_eq!(map_clickhouse_to_neo4j_type("Bool"), "Boolean");
        assert_eq!(map_clickhouse_to_neo4j_type("Array(String)"), "List");
        assert_eq!(map_clickhouse_to_neo4j_type("Unknown"), "String");
    }
}
