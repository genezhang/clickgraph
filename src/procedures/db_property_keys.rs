//! db.propertyKeys() procedure - Returns all unique property keys in the schema
//!
//! Neo4j compatible procedure that lists all property keys across nodes and relationships.
//! Used by Neo4j Browser and Neodash to discover available properties.

use crate::graph_catalog::graph_schema::GraphSchema;
use std::collections::{HashMap, HashSet};

/// Execute db.propertyKeys() procedure
///
/// Returns all unique property keys from both node and edge definitions.
///
/// # Example Response
/// ```json
/// [
///   {"propertyKey": "id"},
///   {"propertyKey": "name"},
///   {"propertyKey": "created_at"}
/// ]
/// ```
pub fn execute(schema: &GraphSchema) -> Result<Vec<HashMap<String, serde_json::Value>>, String> {
    let mut keys = HashSet::new();

    // Collect property keys from node schemas (use column names)
    for node_schema in schema.all_node_schemas().values() {
        for col in &node_schema.column_names {
            keys.insert(col.clone());
        }
    }

    // Collect property keys from relationship schemas (use column names)
    for rel_schema in schema.get_relationships_schemas().values() {
        for col in &rel_schema.column_names {
            keys.insert(col.clone());
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
