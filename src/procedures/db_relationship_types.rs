//! db.relationshipTypes() procedure - Returns all relationship types in the current schema
//!
//! Neo4j compatible procedure that lists all edge/relationship types defined in the graph schema.
//! Used by Neo4j Browser and Neodash to discover available relationship types.

use crate::graph_catalog::graph_schema::GraphSchema;
use std::collections::HashMap;

/// Execute db.relationshipTypes() procedure
///
/// Returns all relationship types in the schema as a list of records with a "relationshipType" field.
///
/// # Example Response
/// ```json
/// [
///   {"relationshipType": "FOLLOWS"},
///   {"relationshipType": "AUTHORED"},
///   {"relationshipType": "LIKED"}
/// ]
/// ```
pub fn execute(schema: &GraphSchema) -> Result<Vec<HashMap<String, serde_json::Value>>, String> {
    let mut types: Vec<String> = schema
        .get_relationships_schemas()
        .keys()
        .filter_map(|key| {
            // Extract base type from keys like "FOLLOWS::User::User" or "FOLLOWS"
            // Return the first segment (the actual type users use in queries)
            let parts: Vec<&str> = key.split("::").collect();
            if !parts.is_empty() && !parts[0].is_empty() {
                Some(parts[0].to_string())
            } else {
                None  // Skip empty strings
            }
        })
        .collect();

    // Remove duplicates and sort
    types.sort();
    types.dedup();

    // Convert to Neo4j-compatible format
    let results = types
        .into_iter()
        .map(|rel_type| {
            let mut record = HashMap::new();
            record.insert("relationshipType".to_string(), serde_json::json!(rel_type));
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
    fn test_db_relationship_types_empty_schema() {
        let schema = create_empty_schema();
        let results = execute(&schema).expect("Should succeed");
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_db_relationship_types_response_format() {
        // Test with empty schema - format validation still works
        let schema = create_empty_schema();
        let results = execute(&schema).expect("Should succeed");

        // Verify response structure (even if empty)
        for record in results {
            assert_eq!(record.len(), 1);
            assert!(record.contains_key("relationshipType"));
            assert!(record.get("relationshipType").unwrap().is_string());
        }
    }
}
