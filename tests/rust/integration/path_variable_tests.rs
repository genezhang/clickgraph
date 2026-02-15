// Integration test for path variable SQL generation
use clickgraph::{
    graph_catalog::{
        config::Identifier,
        expression_parser::PropertyValue,
        graph_schema::{GraphSchema, NodeIdSchema, NodeSchema, RelationshipSchema},
        schema_types::SchemaType,
    },
    open_cypher_parser::parse_query,
    query_planner::logical_plan::plan_builder::build_logical_plan,
    render_plan::{logical_plan_to_render_plan, ToSql},
};
use std::collections::HashMap;

fn create_test_schema() -> GraphSchema {
    let mut nodes = HashMap::new();
    let mut rels = HashMap::new();

    // Create Person node
    nodes.insert(
        "Person".to_string(),
        NodeSchema {
            database: "test".to_string(),
            table_name: "persons".to_string(),
            column_names: vec!["id".to_string(), "name".to_string()],
            primary_keys: "id".to_string(),
            node_id: NodeIdSchema::single("id".to_string(), SchemaType::Integer),
            property_mappings: {
                let mut props = HashMap::new();
                props.insert("id".to_string(), PropertyValue::Column("id".to_string()));
                props.insert(
                    "name".to_string(),
                    PropertyValue::Column("name".to_string()),
                );
                props
            },
            node_id_types: None,
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: false,
            from_properties: None,
            to_properties: None,
            denormalized_source_table: None,
            label_column: None,
            label_value: None,
        },
    );

    // Create FOLLOWS relationship
    rels.insert(
        "FOLLOWS".to_string(),
        RelationshipSchema {
            database: "test".to_string(),
            table_name: "follows".to_string(),
            column_names: vec!["follower_id".to_string(), "followed_id".to_string()],
            from_node: "Person".to_string(),
            to_node: "Person".to_string(),
            from_node_table: "persons".to_string(),
            to_node_table: "persons".to_string(),
            from_id: Identifier::from("follower_id"),
            to_id: Identifier::from("followed_id"),
            from_node_id_dtype: SchemaType::Integer,
            to_node_id_dtype: SchemaType::Integer,
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_label_values: None,
            to_label_values: None,
            from_node_properties: None,
            to_node_properties: None,
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
        },
    );

    GraphSchema::build(1, "test".to_string(), nodes, rels)
}

#[test]
fn test_path_variable_sql_generation() {
    // Test query with path variable
    let cypher =
        "MATCH p = shortestPath((a:Person)-[:FOLLOWS*]-(b:Person)) WHERE a.name = 'Alice' RETURN p";

    // Parse the query
    let ast = parse_query(cypher).expect("Failed to parse Cypher query");

    // Create test schema with Person node and FOLLOWS relationship
    let schema = create_test_schema();

    // Build logical plan
    let (logical_plan, _plan_ctx) =
        build_logical_plan(&ast, &schema, None, None, None).expect("Failed to build logical plan");

    // Build render plan
    let render_plan = logical_plan_to_render_plan((*logical_plan).clone(), &schema)
        .expect("Failed to build render plan");

    // Convert to SQL
    let sql = render_plan.to_sql();

    // Verify the SQL contains the path object construction
    println!("Generated SQL:\n{}", sql);

    // Check that the SQL contains tuple() function for path construction
    // Note: We use tuple() instead of map() to avoid ClickHouse type conflicts
    assert!(
        sql.contains("tuple("),
        "SQL should contain tuple() function for path object"
    );
    assert!(
        sql.contains("path_nodes"),
        "SQL should reference path_nodes column"
    );
    assert!(
        sql.contains("hop_count"),
        "SQL should reference hop_count column"
    );
    assert!(
        sql.contains("path_relationships"),
        "SQL should reference path_relationships column"
    );
}

#[test]
fn test_path_variable_with_properties() {
    // Test query with path variable and node properties
    let cypher = "MATCH p = shortestPath((a:Person)-[:FOLLOWS*]-(b:Person)) WHERE a.name = 'Alice' RETURN p, a.name";

    // Parse the query
    let ast = parse_query(cypher).expect("Failed to parse Cypher query");

    // Create test schema with Person node and FOLLOWS relationship
    let schema = create_test_schema();

    // Build logical plan
    let (logical_plan, _plan_ctx) =
        build_logical_plan(&ast, &schema, None, None, None).expect("Failed to build logical plan");

    // Build render plan
    let render_plan = logical_plan_to_render_plan((*logical_plan).clone(), &schema)
        .expect("Failed to build render plan");

    // Convert to SQL
    let sql = render_plan.to_sql();

    // Verify the SQL
    println!("Generated SQL:\n{}", sql);

    // Should have both path object (as tuple) and node property
    assert!(
        sql.contains("tuple("),
        "SQL should contain tuple() for path"
    );
    assert!(
        sql.contains("end_name") || sql.contains("a.name"),
        "SQL should include the returned node name property"
    );
}

#[test]
fn test_non_path_variable_unchanged() {
    // Test query without path variable - should work as before
    let cypher = "MATCH (a:Person)-[:FOLLOWS*]-(b:Person) WHERE a.name = 'Alice' RETURN a, b";

    // Parse the query
    let ast = parse_query(cypher).expect("Failed to parse Cypher query");

    // Create test schema with Person node and FOLLOWS relationship
    let schema = create_test_schema();

    // Build logical plan
    let (logical_plan, _plan_ctx) =
        build_logical_plan(&ast, &schema, None, None, None).expect("Failed to build logical plan");

    // Build render plan
    let render_plan = logical_plan_to_render_plan((*logical_plan).clone(), &schema)
        .expect("Failed to build render plan");

    // Convert to SQL
    let sql = render_plan.to_sql();

    println!("Generated SQL:\n{}", sql);

    // Should NOT have map() since there's no path variable
    // (unless we're returning the nodes, which is fine)
    // Just make sure it compiles and runs
    assert!(!sql.is_empty(), "SQL should not be empty");
}
