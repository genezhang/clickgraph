//! Integration tests for schema validation and multi-schema support
//!
//! These tests validate schema loading, validation, and multi-schema functionality
//! without requiring a running ClickHouse instance.

#[cfg(test)]
mod schema_integration_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use clickgraph::graph_catalog::graph_schema::GraphSchema;
    use clickgraph::server::graph_catalog;

    /// Test that schema validation works for basic schemas
    #[tokio::test]
    async fn test_schema_validation_basic() {
        // Create a minimal valid schema
        let yaml_schema = r#"
nodes:
  User:
    database: test
    table: users
    id_column: id
    property_mappings:
      id: id
      name: name

relationships:
  FOLLOWS:
    database: test
    table: follows
    from_column: follower_id
    to_column: followed_id
    property_mappings:
      since: created_at
"#;

        // This should parse and validate successfully
        let result = graph_catalog::load_schemas_from_yaml_content(&yaml_schema).await;
        assert!(result.is_ok(), "Basic schema should validate successfully");

        let schema = result.unwrap();
        assert!(schema.get_nodes_schemas().contains_key("User"));
        assert!(schema.get_relationship_schemas().contains_key("FOLLOWS"));
    }

    /// Test schema validation with invalid node reference
    #[tokio::test]
    async fn test_schema_validation_invalid_relationship() {
        let yaml_schema = r#"
nodes:
  User:
    database: test
    table: users
    id_column: id
    property_mappings:
      id: id

relationships:
  FOLLOWS:
    database: test
    table: follows
    from_column: follower_id
    to_column: followed_id
    # Invalid: references non-existent node type
    from_node: NonExistentNode
    to_node: User
"#;

        let result = graph_catalog::load_schemas_from_yaml_content(&yaml_schema).await;
        // Should fail validation due to invalid node reference
        assert!(result.is_err(), "Schema with invalid node reference should fail validation");
    }

    /// Test multi-schema support
    #[tokio::test]
    async fn test_multi_schema_initialization() {
        // Test that multiple schemas can be loaded
        let schema1_yaml = r#"
nodes:
  User:
    database: db1
    table: users
    id_column: id
    property_mappings:
      id: id
"#;

        let schema2_yaml = r#"
nodes:
  Product:
    database: db2
    table: products
    id_column: product_id
    property_mappings:
      product_id: product_id
"#;

        // Load first schema
        let result1 = graph_catalog::load_schemas_from_yaml_content(&schema1_yaml).await;
        assert!(result1.is_ok());

        // Load second schema
        let result2 = graph_catalog::load_schemas_from_yaml_content(&schema2_yaml).await;
        assert!(result2.is_ok());

        // Schemas should be independent
        let schema1 = result1.unwrap();
        let schema2 = result2.unwrap();

        assert!(schema1.get_nodes_schemas().contains_key("User"));
        assert!(!schema1.get_nodes_schemas().contains_key("Product"));

        assert!(schema2.get_nodes_schemas().contains_key("Product"));
        assert!(!schema2.get_nodes_schemas().contains_key("User"));
    }

    /// Test schema with view parameters
    #[tokio::test]
    async fn test_schema_with_view_parameters() {
        let yaml_schema = r#"
nodes:
  TenantUser:
    database: multi_tenant
    table: users_by_tenant
    view_parameters: [tenant_id]
    id_column: user_id
    property_mappings:
      user_id: user_id
      tenant_id: tenant_id
      name: full_name
"#;

        let result = graph_catalog::load_schemas_from_yaml_content(&yaml_schema).await;
        assert!(result.is_ok(), "Schema with view parameters should validate");

        let schema = result.unwrap();
        let user_node = schema.get_nodes_schemas().get("TenantUser").unwrap();
        assert_eq!(user_node.view_parameters, Some(vec!["tenant_id".to_string()]));
    }

    /// Test denormalized node schema
    #[tokio::test]
    async fn test_denormalized_node_schema() {
        let yaml_schema = r#"
nodes:
  User:
    database: test
    table: users
    id_column: id
    property_mappings:
      id: id

relationships:
  PURCHASED:
    database: test
    table: purchases
    from_column: user_id
    to_column: product_id
    # Denormalized: user properties stored in relationship table
    from_properties:
      user_name: buyer_name
      user_email: buyer_email
"#;

        let result = graph_catalog::load_schemas_from_yaml_content(&yaml_schema).await;
        assert!(result.is_ok(), "Denormalized schema should validate");

        let schema = result.unwrap();
        let rel = schema.get_relationship_schemas().get("PURCHASED").unwrap();
        assert!(rel.from_properties.is_some());
        assert_eq!(rel.from_properties.as_ref().unwrap().get("user_name"),
                   Some(&"buyer_name".to_string()));
    }

    /// Test polymorphic relationship schema
    #[tokio::test]
    async fn test_polymorphic_relationship_schema() {
        let yaml_schema = r#"
nodes:
  User:
    database: test
    table: users
    id_column: id
    property_mappings:
      id: id

  Post:
    database: test
    table: posts
    id_column: id
    property_mappings:
      id: id

relationships:
  INTERACTED:
    database: test
    table: interactions
    from_column: user_id
    to_column: target_id
    type_column: interaction_type
    type_values:
      - LIKED: "Post"
      - SHARED: "Post"
      - FOLLOWED: "User"
"#;

        let result = graph_catalog::load_schemas_from_yaml_content(&yaml_schema).await;
        assert!(result.is_ok(), "Polymorphic schema should validate");

        let schema = result.unwrap();
        let rel = schema.get_relationship_schemas().get("INTERACTED").unwrap();
        assert!(rel.type_column.is_some());
        assert!(rel.type_values.is_some());
    }
}