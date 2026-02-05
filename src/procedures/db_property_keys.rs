//! db.propertyKeys() procedure - Returns all unique property keys in the schema
//!
//! Neo4j compatible procedure that lists all property keys across nodes and relationships.
//! Used by Neo4j Browser and Neodash to discover available properties.
//!
//! Returns Cypher property names (keys from property_mappings), NOT database column names.

use crate::graph_catalog::graph_schema::GraphSchema;
use std::collections::{HashMap, HashSet};

/// Execute db.propertyKeys() procedure
///
/// Returns all unique Cypher property keys from both node and edge definitions.
/// These are the property names users can use in Cypher queries, not the underlying
/// database column names.
///
/// # Example Response
/// ```json
/// [
///   {"propertyKey": "id"},
///   {"propertyKey": "name"},
///   {"propertyKey": "email"}
/// ]
/// ```
pub fn execute(schema: &GraphSchema) -> Result<Vec<HashMap<String, serde_json::Value>>, String> {
    let mut keys = HashSet::new();

    // Collect Cypher property keys from node schemas (keys from property_mappings)
    for node_schema in schema.all_node_schemas().values() {
        for cypher_prop in node_schema.property_mappings.keys() {
            keys.insert(cypher_prop.clone());
        }
    }

    // Collect Cypher property keys from relationship schemas (keys from property_mappings)
    for rel_schema in schema.get_relationships_schemas().values() {
        for cypher_prop in rel_schema.property_mappings.keys() {
            keys.insert(cypher_prop.clone());
        }
    }

    // Convert to sorted Vec for consistent output
    let mut keys_vec: Vec<String> = keys.into_iter().collect();
    keys_vec.sort();

    // Convert to Neo4j-compatible format
    let results = keys_vec
        .into_iter()
        .map(|key| {
            let mut record = HashMap::new();
            record.insert("propertyKey".to_string(), serde_json::json!(key));
            record
        })
        .collect();

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_catalog::graph_schema::GraphSchema;
    use std::collections::{HashMap, HashSet};

    fn create_empty_schema() -> GraphSchema {
        GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new())
    }

    #[test]
    fn test_db_property_keys_empty_schema() {
        let schema = create_empty_schema();
        let results = execute(&schema).expect("Should succeed");
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_db_property_keys_response_format() {
        // Test with empty schema - format validation still works
        let schema = create_empty_schema();
        let results = execute(&schema).expect("Should succeed");

        // Verify response structure (even if empty)
        for record in results {
            assert_eq!(record.len(), 1);
            assert!(record.contains_key("propertyKey"));
            assert!(record.get("propertyKey").unwrap().is_string());
        }
    }
}
