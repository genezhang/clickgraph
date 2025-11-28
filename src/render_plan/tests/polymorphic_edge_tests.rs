//! Tests for polymorphic edge type filtering
//!
//! These tests verify that when a relationship table uses type discrimination columns
//! (type_column, from_label_column, to_label_column), the query generator adds appropriate
//! WHERE clauses to filter for the correct edge types and node types.

use std::collections::HashMap;

use crate::graph_catalog::graph_schema::{GraphSchema, NodeIdSchema, NodeSchema, RelationshipSchema};
use crate::render_plan::plan_builder_helpers::generate_polymorphic_edge_filters;
use crate::render_plan::render_expr::{Literal, Operator, OperatorApplication, PropertyAccess, RenderExpr};
use crate::server::GLOBAL_SCHEMAS;
use serial_test::serial;

/// Setup test schema with polymorphic relationship table
fn setup_polymorphic_schema() -> GraphSchema {
    let mut nodes = HashMap::new();
    let mut relationships = HashMap::new();

    // User nodes
    let mut user_props = HashMap::new();
    user_props.insert("name".to_string(), crate::graph_catalog::expression_parser::PropertyValue::Column("username".to_string()));
    user_props.insert("email".to_string(), crate::graph_catalog::expression_parser::PropertyValue::Column("email_address".to_string()));

    nodes.insert(
        "User".to_string(),
        NodeSchema {
            database: "test_db".to_string(),
            table_name: "users".to_string(),
            column_names: vec!["user_id".to_string(), "username".to_string(), "email_address".to_string()],
            primary_keys: "user_id".to_string(),
            node_id: NodeIdSchema {
                column: "user_id".to_string(),
                dtype: "UInt64".to_string(),
            },
            property_mappings: user_props,
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: false,
            from_properties: None,
            to_properties: None,
            denormalized_source_table: None,
        },
    );

    // Post nodes
    let mut post_props = HashMap::new();
    post_props.insert("title".to_string(), crate::graph_catalog::expression_parser::PropertyValue::Column("post_title".to_string()));
    post_props.insert("content".to_string(), crate::graph_catalog::expression_parser::PropertyValue::Column("post_content".to_string()));

    nodes.insert(
        "Post".to_string(),
        NodeSchema {
            database: "test_db".to_string(),
            table_name: "posts".to_string(),
            column_names: vec!["post_id".to_string(), "post_title".to_string(), "post_content".to_string()],
            primary_keys: "post_id".to_string(),
            node_id: NodeIdSchema {
                column: "post_id".to_string(),
                dtype: "UInt64".to_string(),
            },
            property_mappings: post_props,
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: false,
            from_properties: None,
            to_properties: None,
            denormalized_source_table: None,
        },
    );

    // Polymorphic relationship: interactions table contains FOLLOWS, LIKES, AUTHORED
    let mut interaction_props = HashMap::new();
    interaction_props.insert("created_at".to_string(), crate::graph_catalog::expression_parser::PropertyValue::Column("timestamp".to_string()));

    // FOLLOWS relationship (User -> User)
    relationships.insert(
        "FOLLOWS".to_string(),
        RelationshipSchema {
            database: "test_db".to_string(),
            table_name: "interactions".to_string(),
            column_names: vec![
                "from_id".to_string(),
                "to_id".to_string(),
                "interaction_type".to_string(),
                "from_type".to_string(),
                "to_type".to_string(),
                "timestamp".to_string(),
            ],
            from_node: "User".to_string(),
            to_node: "User".to_string(),
            from_id: "from_id".to_string(),
            to_id: "to_id".to_string(),
            from_node_id_dtype: "UInt64".to_string(),
            to_node_id_dtype: "UInt64".to_string(),
            property_mappings: interaction_props.clone(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: Some("interaction_type".to_string()),
            from_label_column: Some("from_type".to_string()),
            to_label_column: Some("to_type".to_string()),
            from_node_properties: None,
            to_node_properties: None,
        },
    );

    // LIKES relationship (User -> Post)
    relationships.insert(
        "LIKES".to_string(),
        RelationshipSchema {
            database: "test_db".to_string(),
            table_name: "interactions".to_string(),
            column_names: vec![
                "from_id".to_string(),
                "to_id".to_string(),
                "interaction_type".to_string(),
                "from_type".to_string(),
                "to_type".to_string(),
                "timestamp".to_string(),
            ],
            from_node: "User".to_string(),
            to_node: "Post".to_string(),
            from_id: "from_id".to_string(),
            to_id: "to_id".to_string(),
            from_node_id_dtype: "UInt64".to_string(),
            to_node_id_dtype: "UInt64".to_string(),
            property_mappings: interaction_props.clone(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: Some("interaction_type".to_string()),
            from_label_column: Some("from_type".to_string()),
            to_label_column: Some("to_type".to_string()),
            from_node_properties: None,
            to_node_properties: None,
        },
    );

    // AUTHORED relationship (User -> Post)
    relationships.insert(
        "AUTHORED".to_string(),
        RelationshipSchema {
            database: "test_db".to_string(),
            table_name: "interactions".to_string(),
            column_names: vec![
                "from_id".to_string(),
                "to_id".to_string(),
                "interaction_type".to_string(),
                "from_type".to_string(),
                "to_type".to_string(),
                "timestamp".to_string(),
            ],
            from_node: "User".to_string(),
            to_node: "Post".to_string(),
            from_id: "from_id".to_string(),
            to_id: "to_id".to_string(),
            from_node_id_dtype: "UInt64".to_string(),
            to_node_id_dtype: "UInt64".to_string(),
            property_mappings: interaction_props,
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: Some("interaction_type".to_string()),
            from_label_column: Some("from_type".to_string()),
            to_label_column: Some("to_type".to_string()),
            from_node_properties: None,
            to_node_properties: None,
        },
    );

    GraphSchema::build(1, "test_db".to_string(), nodes, relationships)
}

/// Setup global schema for testing
fn init_test_schema(schema: GraphSchema) {
    use tokio::sync::RwLock;
    
    const SCHEMA_NAME: &str = "default";
    
    // Always recreate for proper test isolation
    
    let mut schemas = HashMap::new();
    schemas.insert(SCHEMA_NAME.to_string(), schema);
    
    // Initialize GLOBAL_SCHEMAS
    if let Some(schemas_lock) = GLOBAL_SCHEMAS.get() {
        // Update existing
        if let Ok(mut schemas_guard) = schemas_lock.try_write() {
            *schemas_guard = schemas;
        }
    } else {
        // Initialize for the first time
        let _ = GLOBAL_SCHEMAS.set(RwLock::new(schemas));
    }
}

#[test]
    #[serial]
fn test_polymorphic_filter_follows_user_to_user() {
    let schema = setup_polymorphic_schema();
    init_test_schema(schema);

    // MATCH (u1:User)-[:FOLLOWS]->(u2:User)
    let filter = generate_polymorphic_edge_filters("r", "FOLLOWS", "User", "User");

    assert!(filter.is_some(), "Should generate filter for polymorphic edge");
    
    let filter_expr = filter.unwrap();
    
    // Verify it's an AND operation with 3 operands (type + from_type + to_type)
    if let RenderExpr::OperatorApplicationExp(op) = &filter_expr {
        assert_eq!(op.operator, Operator::And, "Should combine filters with AND");
        assert_eq!(op.operands.len(), 3, "Should have 3 operands: type_column, from_label_column, to_label_column");
    } else {
        panic!("Filter should be an OperatorApplication");
    }
    
    // Verify SQL contains all three filters
    let sql = filter_expr.to_sql();
    assert!(sql.contains("interaction_type"), "Should filter on type_column");
    assert!(sql.contains("'FOLLOWS'"), "Should filter for FOLLOWS relationship");
    assert!(sql.contains("from_type"), "Should filter on from_label_column");
    assert!(sql.contains("to_type"), "Should filter on to_label_column");
    assert!(sql.contains("'User'"), "Should filter for User node type");
}

#[test]
    #[serial]
fn test_polymorphic_filter_likes_user_to_post() {
    let schema = setup_polymorphic_schema();
    init_test_schema(schema);

    // MATCH (u:User)-[:LIKES]->(p:Post)
    let filter = generate_polymorphic_edge_filters("r", "LIKES", "User", "Post");

    assert!(filter.is_some(), "Should generate filter for polymorphic edge");
    
    let sql = filter.unwrap().to_sql();
    assert!(sql.contains("'LIKES'"), "Should filter for LIKES relationship");
    assert!(sql.contains("'User'"), "Should filter for User source");
    assert!(sql.contains("'Post'"), "Should filter for Post target");
}

#[test]
    #[serial]
fn test_polymorphic_filter_authored_user_to_post() {
    let schema = setup_polymorphic_schema();
    init_test_schema(schema);

    // MATCH (u:User)-[:AUTHORED]->(p:Post)
    let filter = generate_polymorphic_edge_filters("r", "AUTHORED", "User", "Post");

    assert!(filter.is_some(), "Should generate filter for polymorphic edge");
    
    let sql = filter.unwrap().to_sql();
    assert!(sql.contains("'AUTHORED'"), "Should filter for AUTHORED relationship");
    assert!(sql.contains("'User'"), "Should filter for User source");
    assert!(sql.contains("'Post'"), "Should filter for Post target");
}

#[test]
    #[serial]
fn test_non_polymorphic_relationship() {
    // For this test, we need a schema with a non-polymorphic relationship
    let mut nodes = HashMap::new();
    let mut relationships = HashMap::new();

    // Simple User node
    nodes.insert(
        "User".to_string(),
        NodeSchema {
            database: "test_db".to_string(),
            table_name: "users".to_string(),
            column_names: vec!["user_id".to_string()],
            primary_keys: "user_id".to_string(),
            node_id: NodeIdSchema {
                column: "user_id".to_string(),
                dtype: "UInt64".to_string(),
            },
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: false,
            from_properties: None,
            to_properties: None,
            denormalized_source_table: None,
        },
    );

    // Non-polymorphic FOLLOWS relationship (dedicated table)
    relationships.insert(
        "FOLLOWS".to_string(),
        RelationshipSchema {
            database: "test_db".to_string(),
            table_name: "user_follows".to_string(),
            column_names: vec!["follower_id".to_string(), "followed_id".to_string()],
            from_node: "User".to_string(),
            to_node: "User".to_string(),
            from_id: "follower_id".to_string(),
            to_id: "followed_id".to_string(),
            from_node_id_dtype: "UInt64".to_string(),
            to_node_id_dtype: "UInt64".to_string(),
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None, // Not polymorphic!
            from_label_column: None,
            to_label_column: None,
            from_node_properties: None,
            to_node_properties: None,
        },
    );

    let schema = GraphSchema::build(1, "test_db".to_string(), nodes, relationships);
    init_test_schema(schema);

    // Should NOT generate filter for non-polymorphic relationship
    let filter = generate_polymorphic_edge_filters("r", "FOLLOWS", "User", "User");

    assert!(filter.is_none(), "Should NOT generate filter for non-polymorphic edge");
}

#[test]
    #[serial]
fn test_polymorphic_filter_with_different_alias() {
    let schema = setup_polymorphic_schema();
    init_test_schema(schema);

    // Test with different relationship alias
    let filter = generate_polymorphic_edge_filters("my_edge", "FOLLOWS", "User", "User");

    assert!(filter.is_some());
    
    let sql = filter.unwrap().to_sql();
    // Should use the provided alias
    assert!(sql.contains("my_edge"), "Should use custom alias in filter");
}



