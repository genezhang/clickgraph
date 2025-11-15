#[cfg(test)]
mod tests {
    use crate::graph_catalog::SchemaValidator;
    use crate::graph_catalog::testing::mock_clickhouse::create_test_table_schemas;

    #[test]
    fn test_create_schema_validator() {
        // Use clickhouse test utilities for integration tests
        let client = clickhouse::Client::default();
        let validator = SchemaValidator::new(client);
        
        // Basic constructor test
        assert!(!validator.column_cache.is_empty() == false); // Cache starts empty
    }

    #[test]
    fn test_table_schemas() {
        // Test our mock table schemas
        let tables = create_test_table_schemas();
        
        assert!(tables.contains_key("users"));
        assert!(tables.contains_key("posts"));
        assert!(tables.contains_key("follows"));
        
        let users_schema = tables.get("users").unwrap();
        assert_eq!(users_schema.len(), 5);
    }
}
