//! dbms.components() procedure - Returns ClickGraph version and edition information
//!
//! Neo4j compatible procedure that returns database metadata.
//! Neo4j Browser uses this to display connection information.

use std::collections::HashMap;

/// Execute dbms.components() procedure
///
/// Returns ClickGraph metadata in Neo4j-compatible format.
///
/// # Example Response
/// ```json
/// [
///   {
///     "name": "ClickGraph",
///     "versions": ["0.3.0"],
///     "edition": "community"
///   }
/// ]
/// ```
pub fn execute(
    _schema: &crate::graph_catalog::graph_schema::GraphSchema,
) -> Result<Vec<HashMap<String, serde_json::Value>>, String> {
    let version = env!("CARGO_PKG_VERSION");

    let mut record = HashMap::new();
    record.insert("name".to_string(), serde_json::json!("ClickGraph"));
    record.insert("versions".to_string(), serde_json::json!([version]));
    record.insert("edition".to_string(), serde_json::json!("community"));

    Ok(vec![record])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_catalog::graph_schema::GraphSchema;
    use std::collections::HashMap;

    #[test]
    fn test_dbms_components_response_format() {
        let schema = GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new());

        let results = execute(&schema).expect("Should succeed");

        assert_eq!(results.len(), 1);

        let record = &results[0];
        assert_eq!(record.len(), 3);
        assert!(record.contains_key("name"));
        assert!(record.contains_key("versions"));
        assert!(record.contains_key("edition"));

        // Verify values
        assert_eq!(record.get("name").unwrap().as_str().unwrap(), "ClickGraph");
        assert_eq!(
            record.get("edition").unwrap().as_str().unwrap(),
            "community"
        );

        // Verify versions is an array
        let versions = record.get("versions").unwrap().as_array().unwrap();
        assert_eq!(versions.len(), 1);
        assert!(versions[0].is_string());
    }

    #[test]
    fn test_dbms_components_version_present() {
        let schema = GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new());

        let results = execute(&schema).expect("Should succeed");
        let record = &results[0];

        let versions = record.get("versions").unwrap().as_array().unwrap();
        let version_str = versions[0].as_str().unwrap();

        // Version should be a semantic version (contains dots)
        assert!(
            version_str.contains('.'),
            "Version should be in semantic version format"
        );
    }
}
