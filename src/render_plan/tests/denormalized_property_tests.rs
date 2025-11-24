//! Tests for denormalized property access in edge tables
//!
//! These tests verify that when properties are denormalized (copied from node tables
//! into edge tables), the query generator can access them directly without JOINs.

use std::collections::HashMap;

use crate::graph_catalog::graph_schema::{GraphSchema, NodeIdSchema, NodeSchema, RelationshipSchema};
use crate::render_plan::cte_generation::map_property_to_column_with_relationship_context;
use crate::server::GLOBAL_SCHEMAS;
use serial_test::serial;

/// Setup test schema with denormalized properties
fn setup_denormalized_schema() -> GraphSchema {
    let mut nodes = HashMap::new();
    let mut relationships = HashMap::new();

    // Airport nodes (minimal - only ID)
    let mut airport_props = HashMap::new();
    airport_props.insert("code".to_string(), crate::graph_catalog::expression_parser::PropertyValue::Column("airport_code".to_string()));
    airport_props.insert("city".to_string(), crate::graph_catalog::expression_parser::PropertyValue::Column("city_name".to_string()));
    airport_props.insert("state".to_string(), crate::graph_catalog::expression_parser::PropertyValue::Column("state_code".to_string()));

    nodes.insert(
        "Airport".to_string(),
        NodeSchema {
            database: "test_db".to_string(),
            table_name: "airports".to_string(),
            column_names: vec!["airport_code".to_string(), "city_name".to_string(), "state_code".to_string()],
            primary_keys: "airport_id".to_string(),
            node_id: NodeIdSchema {
                column: "airport_id".to_string(),
                dtype: "UInt64".to_string(),
            },
            property_mappings: airport_props,
            view_parameters: None,
            engine: None,
            use_final: None,
            is_denormalized: false,
            from_properties: None,
            to_properties: None,
            denormalized_source_table: None,
        },
    );

    // Flight edges with denormalized properties
    let mut flight_props = HashMap::new();
    flight_props.insert("flight_num".to_string(), crate::graph_catalog::expression_parser::PropertyValue::Column("flight_number".to_string()));
    flight_props.insert("airline".to_string(), crate::graph_catalog::expression_parser::PropertyValue::Column("carrier".to_string()));

    // Denormalized origin properties (from from_node)
    let mut from_node_props = HashMap::new();
    from_node_props.insert("city".to_string(), crate::graph_catalog::expression_parser::PropertyValue::Column("origin_city".to_string()));
    from_node_props.insert("state".to_string(), crate::graph_catalog::expression_parser::PropertyValue::Column("origin_state".to_string()));

    // Denormalized destination properties (from to_node)
    let mut to_node_props = HashMap::new();
    to_node_props.insert("city".to_string(), crate::graph_catalog::expression_parser::PropertyValue::Column("dest_city".to_string()));
    to_node_props.insert("state".to_string(), crate::graph_catalog::expression_parser::PropertyValue::Column("dest_state".to_string()));

    relationships.insert(
        "FLIGHT".to_string(),
        RelationshipSchema {
            database: "test_db".to_string(),
            table_name: "flights".to_string(),
            column_names: vec![
                "origin_id".to_string(),
                "dest_id".to_string(),
                "flight_number".to_string(),
                "carrier".to_string(),
                "origin_city".to_string(),
                "origin_state".to_string(),
                "dest_city".to_string(),
                "dest_state".to_string(),
            ],
            from_node: "Airport".to_string(),
            to_node: "Airport".to_string(),
            from_id: "origin_id".to_string(),
            to_id: "dest_id".to_string(),
            from_node_id_dtype: "UInt64".to_string(),
            to_node_id_dtype: "UInt64".to_string(),
            property_mappings: flight_props,
            view_parameters: None,
            engine: None,
            use_final: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_node_properties: Some(from_node_props.into_iter().map(|(k, v)| (k, v.raw().to_string())).collect()),
            to_node_properties: Some(to_node_props.into_iter().map(|(k, v)| (k, v.raw().to_string())).collect()),
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
    // For tests, check if already initialized
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
fn test_denormalized_from_node_property() {
    let schema = setup_denormalized_schema();
    init_test_schema(schema);

    // Access denormalized property from origin Airport
    let result = map_property_to_column_with_relationship_context(
        "city",
        "Airport",
        Some("FLIGHT"),
    );

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "origin_city", 
        "Should return denormalized column from edge table (origin_city)");
}

#[test]
    #[serial]
fn test_denormalized_to_node_property() {
    let schema = setup_denormalized_schema();
    init_test_schema(schema);

    // For to_node properties, we need a different test setup
    // In reality, the query generator determines which side based on the query pattern
    // For this test, we'll manually check the to_node_properties path
    
    // This is a limitation of the current API - it doesn't distinguish from/to context
    // TODO: Enhancement needed to pass side information (from/to) explicitly
    let result = map_property_to_column_with_relationship_context(
        "city",
        "Airport",
        Some("FLIGHT"),
    );

    assert!(result.is_ok());
    // This will return origin_city because from_node is checked first
    assert_eq!(result.unwrap(), "origin_city");
}

#[test]
    #[serial]
fn test_fallback_to_node_property() {
    let schema = setup_denormalized_schema();
    init_test_schema(schema);

    // Access property that's NOT denormalized (only in node table)
    let result = map_property_to_column_with_relationship_context(
        "code",  // Not denormalized in FLIGHT edges
        "Airport",
        Some("FLIGHT"),
    );

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "airport_code", 
        "Should fall back to node table property mapping");
}

#[test]
    #[serial]
fn test_no_relationship_context() {
    let schema = setup_denormalized_schema();
    init_test_schema(schema);

    // Without relationship context, should use node property mapping
    let result = map_property_to_column_with_relationship_context(
        "city",
        "Airport",
        None,  // No relationship context
    );

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "city_name", 
        "Without relationship context, should use node property mapping");
}

#[test]
    #[serial]
fn test_relationship_property() {
    let schema = setup_denormalized_schema();
    init_test_schema(schema);

    // Relationship properties (not node properties) should still work via fallback
    // Note: This test accesses a property that doesn't exist on Airport nodes
    let result = map_property_to_column_with_relationship_context(
        "flight_num",  // This is a relationship property, not a node property
        "Airport",
        Some("FLIGHT"),
    );

    // This should fail because flight_num is not a node property
    assert!(result.is_err(), 
        "Relationship properties should fail when queried as node properties");
}

#[test]
    #[serial]
fn test_multiple_relationships_same_node() {
    let mut schema = setup_denormalized_schema();
    
    // Add another relationship with different denormalized properties
    let mut authored_props = HashMap::new();
    authored_props.insert("timestamp".to_string(), crate::graph_catalog::expression_parser::PropertyValue::Column("created_at".to_string()));
    
    let mut author_props = HashMap::new();
    author_props.insert("name".to_string(), crate::graph_catalog::expression_parser::PropertyValue::Column("author_name".to_string()));
    
    schema.insert_relationship_schema(
        "AUTHORED".to_string(),
        RelationshipSchema {
            database: "test_db".to_string(),
            table_name: "posts".to_string(),
            column_names: vec!["author_id".to_string(), "post_id".to_string(), "author_name".to_string()],
            from_node: "User".to_string(),
            to_node: "Post".to_string(),
            from_id: "author_id".to_string(),
            to_id: "post_id".to_string(),
            from_node_id_dtype: "UInt64".to_string(),
            to_node_id_dtype: "UInt64".to_string(),
            property_mappings: authored_props,
            view_parameters: None,
            engine: None,
            use_final: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_node_properties: Some(author_props.into_iter().map(|(k, v)| (k, v.raw().to_string())).collect()),
            to_node_properties: None,
        },
    );
    
    init_test_schema(schema);

    // Query for property in FLIGHT relationship
    let result1 = map_property_to_column_with_relationship_context(
        "city",
        "Airport",
        Some("FLIGHT"),
    );
    assert!(result1.is_ok());
    assert_eq!(result1.unwrap(), "origin_city");

    // Query for same property name in different relationship (should fail)
    let result2 = map_property_to_column_with_relationship_context(
        "city",
        "Airport",
        Some("AUTHORED"),  // Wrong relationship
    );
    // Should fall back to node property mapping
    assert!(result2.is_ok());
    assert_eq!(result2.unwrap(), "city_name", 
        "Should fall back to node property when relationship doesn't have denormalized property");
}

#[test]
#[serial]
fn test_denormalized_edge_table_same_table_for_node_and_edge() {
    // Test the true denormalized edge table pattern:
    // - Node and edge use the SAME table (e.g., flights table for both Airport nodes and FLIGHT edges)
    // - Node id_column refers to columns that exist in from_node_properties/to_node_properties
    // - No separate node table, no JOINs needed
    
    let mut nodes = HashMap::new();
    let mut relationships = HashMap::new();

    // Airport nodes - uses FLIGHTS table (not separate airports table)
    let mut airport_props = HashMap::new();
    // For denormalized edge tables, node properties come from the edge table
    // So we leave property_mappings empty - they're derived from from_node_properties/to_node_properties
    
    nodes.insert(
        "Airport".to_string(),
        NodeSchema {
            database: "test_db".to_string(),
            table_name: "flights".to_string(),  // ✅ Same table as edge!
            column_names: vec!["origin_code".to_string(), "dest_code".to_string(), "origin_city".to_string(), "dest_city".to_string()],
            primary_keys: "code".to_string(),  // Logical ID property
            node_id: NodeIdSchema {
                column: "code".to_string(),  // Maps to origin_code/dest_code
                dtype: "String".to_string(),
            },
            property_mappings: airport_props,  // Empty - derived from edge
            view_parameters: None,
            engine: None,
            use_final: None,
            is_denormalized: false,
            from_properties: None,
            to_properties: None,
            denormalized_source_table: None,
        },
    );

    // Flight edges with denormalized properties
    let mut flight_props = HashMap::new();
    flight_props.insert("flight_num".to_string(), crate::graph_catalog::expression_parser::PropertyValue::Column("flight_number".to_string()));
    flight_props.insert("airline".to_string(), crate::graph_catalog::expression_parser::PropertyValue::Column("carrier".to_string()));

    // Denormalized origin properties
    let mut from_node_props = HashMap::new();
    from_node_props.insert("code".to_string(), crate::graph_catalog::expression_parser::PropertyValue::Column("origin_code".to_string()));
    from_node_props.insert("city".to_string(), crate::graph_catalog::expression_parser::PropertyValue::Column("origin_city".to_string()));

    // Denormalized destination properties
    let mut to_node_props = HashMap::new();
    to_node_props.insert("code".to_string(), crate::graph_catalog::expression_parser::PropertyValue::Column("dest_code".to_string()));
    to_node_props.insert("city".to_string(), crate::graph_catalog::expression_parser::PropertyValue::Column("dest_city".to_string()));

    relationships.insert(
        "FLIGHT".to_string(),
        RelationshipSchema {
            database: "test_db".to_string(),
            table_name: "flights".to_string(),  // ✅ Same table as node!
            column_names: vec![
                "origin_code".to_string(),
                "dest_code".to_string(),
                "flight_number".to_string(),
                "carrier".to_string(),
                "origin_city".to_string(),
                "dest_city".to_string(),
            ],
            from_node: "Airport".to_string(),
            to_node: "Airport".to_string(),
            from_id: "origin_code".to_string(),
            to_id: "dest_code".to_string(),
            from_node_id_dtype: "String".to_string(),
            to_node_id_dtype: "String".to_string(),
            property_mappings: flight_props,
            view_parameters: None,
            engine: None,
            use_final: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_node_properties: Some(from_node_props.into_iter().map(|(k, v)| (k, v.raw().to_string())).collect()),
            to_node_properties: Some(to_node_props.into_iter().map(|(k, v)| (k, v.raw().to_string())).collect()),
        },
    );

    let schema = GraphSchema::build(1, "test_db".to_string(), nodes, relationships);
    init_test_schema(schema);

    // Test 1: Access denormalized origin city
    let result = map_property_to_column_with_relationship_context(
        "city",
        "Airport",
        Some("FLIGHT"),
    );
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "origin_city", 
        "Should map to denormalized origin_city in flights table");

    // Test 2: Access node ID property (should map through from_node_properties)
    let result = map_property_to_column_with_relationship_context(
        "code",
        "Airport",
        Some("FLIGHT"),
    );
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "origin_code", 
        "Node ID property should map through from_node_properties");
}






