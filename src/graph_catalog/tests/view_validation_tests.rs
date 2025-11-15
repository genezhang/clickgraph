//! Tests for view schema validation

use super::mock_clickhouse::MockClickHouseClient;
use crate::graph_catalog::{
    GraphViewDefinition, NodeViewMapping, RelationshipViewMapping,
    schema_validator::SchemaValidator,
};

#[test]
fn test_validate_node_view() {
    let client = MockClickHouseClient::new();
    let validator = SchemaValidator::new(&client);

    // Create a view definition
    let mut view_def = GraphViewDefinition::new("social_graph");
    let mut node_mapping = NodeViewMapping::new("users", "id");
    node_mapping.add_property("name", "user_name");
    node_mapping.add_property("age", "age");
    view_def.add_node("User", node_mapping);

    // Validate against mock schema
    let result = validator.validate_view_definition(&view_def);
    assert!(result.is_ok());
}

#[test]
fn test_validate_relationship_view() {
    let client = MockClickHouseClient::new();
    let validator = SchemaValidator::new(&client);

    // Create a view definition with relationships
    let mut view_def = GraphViewDefinition::new("social_graph");
    
    // Add node mappings
    let user_mapping = NodeViewMapping::new("users", "id");
    view_def.add_node("User", user_mapping);
    
    let post_mapping = NodeViewMapping::new("posts", "id");
    view_def.add_node("Post", post_mapping);

    // Add relationship mapping
    let mut rel_mapping = RelationshipViewMapping::new(
        "user_posts",
        "user_id",
        "post_id",
    );
    rel_mapping.add_property("created_at", "timestamp");
    view_def.add_relationship("AUTHORED", rel_mapping);

    // Validate against mock schema
    let result = validator.validate_view_definition(&view_def);
    assert!(result.is_ok());
}

#[test]
fn test_invalid_column_reference() {
    let client = MockClickHouseClient::new();
    let validator = SchemaValidator::new(&client);

    // Create a view definition with invalid column
    let mut view_def = GraphViewDefinition::new("social_graph");
    let mut node_mapping = NodeViewMapping::new("users", "id");
    node_mapping.add_property("name", "nonexistent_column"); // Invalid column
    view_def.add_node("User", node_mapping);

    // Validation should fail
    let result = validator.validate_view_definition(&view_def);
    assert!(result.is_err());
}

#[test]
fn test_invalid_table_reference() {
    let client = MockClickHouseClient::new();
    let validator = SchemaValidator::new(&client);

    // Create a view definition with invalid table
    let mut view_def = GraphViewDefinition::new("social_graph");
    let mut node_mapping = NodeViewMapping::new("nonexistent_table", "id");
    node_mapping.add_property("name", "user_name");
    view_def.add_node("User", node_mapping);

    // Validation should fail
    let result = validator.validate_view_definition(&view_def);
    assert!(result.is_err());
}

#[test]
fn test_invalid_relationship_keys() {
    let client = MockClickHouseClient::new();
    let validator = SchemaValidator::new(&client);

    // Create a view definition with invalid relationship keys
    let mut view_def = GraphViewDefinition::new("social_graph");
    
    // Add node mappings
    let user_mapping = NodeViewMapping::new("users", "id");
    view_def.add_node("User", user_mapping);
    
    let post_mapping = NodeViewMapping::new("posts", "id");
    view_def.add_node("Post", post_mapping);

    // Add relationship with invalid keys
    let mut rel_mapping = RelationshipViewMapping::new(
        "user_posts",
        "nonexistent_from_id",
        "nonexistent_to_id",
    );
    view_def.add_relationship("AUTHORED", rel_mapping);

    // Validation should fail
    let result = validator.validate_view_definition(&view_def);
    assert!(result.is_err());
}
