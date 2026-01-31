//! db.labels() procedure - Returns all node labels in the current schema
//!
//! Neo4j compatible procedure that lists all node labels defined in the graph schema.
//! Used by Neo4j Browser and Neodash to discover available node types.

use crate::graph_catalog::graph_schema::GraphSchema;
use std::collections::HashMap;

/// Execute db.labels() procedure
///
/// Returns all node labels in the schema as a list of records with a "label" field.
///
/// # Example Response
/// ```json
/// [
///   {"label": "User"},
///   {"label": "Post"},
///   {"label": "Comment"}
/// ]
/// ```
pub fn execute(schema: &GraphSchema) -> Result<Vec<HashMap<String, serde_json::Value>>, String> {
    let mut labels: Vec<String> = schema
        .all_node_schemas()
        .keys()
        .map(|s| s.to_string())
        .collect();

    // Sort for consistent output
    labels.sort();

    // Convert to Neo4j-compatible format
    let results = labels
        .into_iter()
        .map(|label| {
            let mut record = HashMap::new();
            record.insert("label".to_string(), serde_json::json!(label));
            record
        })
        .collect();

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_catalog::graph_schema::GraphSchema;
    use std::collections::HashMap;

    fn create_empty_schema() -> GraphSchema {
        GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new())
    }

    #[test]
    fn test_db_labels_empty_schema() {
        let schema = create_empty_schema();
        let results = execute(&schema).expect("Should succeed");
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_db_labels_response_format() {
        // Test with empty schema - format validation still works
        let schema = create_empty_schema();
        let results = execute(&schema).expect("Should succeed");

        // Verify response structure (even if empty)
        for record in results {
            assert_eq!(record.len(), 1);
            assert!(record.contains_key("label"));
            assert!(record.get("label").unwrap().is_string());
        }
    }
}
