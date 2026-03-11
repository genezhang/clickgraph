//! Integration tests for complex feature combinations in ClickGraph
//!
//! These tests validate that multiple advanced Cypher features work correctly
//! when combined together, catching integration bugs that individual feature
//! tests might miss.

use clickgraph::{
    graph_catalog::{
        config::Identifier,
        expression_parser::PropertyValue,
        graph_schema::{GraphSchema, NodeIdSchema, NodeSchema, RelationshipSchema},
        schema_types::SchemaType,
    },
    open_cypher_parser::parse_query,
    query_planner::{evaluate_read_query, logical_plan::plan_builder::build_logical_plan},
    render_plan::{logical_plan_to_render_plan, ToSql},
};
use std::collections::HashMap;

fn create_test_schema() -> GraphSchema {
    let mut nodes = HashMap::new();
    let mut relationships = HashMap::new();

    // Create User node
    nodes.insert(
        "User".to_string(),
        NodeSchema {
            database: "test".to_string(),
            table_name: "users".to_string(),
            column_names: vec![
                "user_id".to_string(),
                "full_name".to_string(),
                "email_address".to_string(),
                "registration_date".to_string(),
                "is_active".to_string(),
            ],
            primary_keys: "user_id".to_string(),
            node_id: NodeIdSchema::single("user_id".to_string(), SchemaType::Integer),
            property_mappings: {
                let mut props = HashMap::new();
                props.insert(
                    "user_id".to_string(),
                    PropertyValue::Column("user_id".to_string()),
                );
                props.insert(
                    "name".to_string(),
                    PropertyValue::Column("full_name".to_string()),
                );
                props.insert(
                    "email".to_string(),
                    PropertyValue::Column("email_address".to_string()),
                );
                props.insert(
                    "registration_date".to_string(),
                    PropertyValue::Column("registration_date".to_string()),
                );
                props.insert(
                    "is_active".to_string(),
                    PropertyValue::Column("is_active".to_string()),
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
            source: None,
        },
    );

    // Create Post node
    nodes.insert(
        "Post".to_string(),
        NodeSchema {
            database: "test".to_string(),
            table_name: "posts".to_string(),
            column_names: vec![
                "post_id".to_string(),
                "author_id".to_string(),
                "post_title".to_string(),
                "post_content".to_string(),
                "post_date".to_string(),
            ],
            primary_keys: "post_id".to_string(),
            node_id: NodeIdSchema::single("post_id".to_string(), SchemaType::Integer),
            property_mappings: {
                let mut props = HashMap::new();
                props.insert(
                    "post_id".to_string(),
                    PropertyValue::Column("post_id".to_string()),
                );
                props.insert(
                    "author_id".to_string(),
                    PropertyValue::Column("author_id".to_string()),
                );
                props.insert(
                    "title".to_string(),
                    PropertyValue::Column("post_title".to_string()),
                );
                props.insert(
                    "content".to_string(),
                    PropertyValue::Column("post_content".to_string()),
                );
                props.insert(
                    "date".to_string(),
                    PropertyValue::Column("post_date".to_string()),
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
            source: None,
        },
    );

    // Create FOLLOWS relationship
    relationships.insert(
        "FOLLOWS".to_string(),
        RelationshipSchema {
            database: "test".to_string(),
            table_name: "user_follows".to_string(),
            column_names: vec![
                "follower_id".to_string(),
                "followed_id".to_string(),
                "follow_date".to_string(),
            ],
            from_node: "User".to_string(),
            to_node: "User".to_string(),
            from_node_table: "users".to_string(),
            to_node_table: "users".to_string(),
            from_id: Identifier::from("follower_id"),
            to_id: Identifier::from("followed_id"),
            from_node_id_dtype: SchemaType::Integer,
            to_node_id_dtype: SchemaType::Integer,
            property_mappings: {
                let mut props = HashMap::new();
                props.insert(
                    "follow_date".to_string(),
                    PropertyValue::Column("follow_date".to_string()),
                );
                props
            },
            edge_id_types: None,
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
            source: None,
        },
    );

    // Create AUTHORED relationship
    relationships.insert(
        "AUTHORED".to_string(),
        RelationshipSchema {
            database: "test".to_string(),
            table_name: "post_authors".to_string(),
            column_names: vec!["author_id".to_string(), "post_id".to_string()],
            from_node: "User".to_string(),
            to_node: "Post".to_string(),
            from_node_table: "users".to_string(),
            to_node_table: "posts".to_string(),
            from_id: Identifier::from("author_id"),
            to_id: Identifier::from("post_id"),
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
            source: None,
        },
    );

    GraphSchema::build(1, "test".to_string(), nodes, relationships)
}

/// Test OPTIONAL MATCH combined with Variable-Length Paths and aggregations
#[tokio::test]
async fn test_optional_match_with_vlp_and_aggregation() {
    let schema = create_test_schema();

    let cypher = r#"
        MATCH (u:User)
        OPTIONAL MATCH (u)-[:FOLLOWS*1..3]->(f:User)
        RETURN u.name, COUNT(f) as follower_count
    "#;

    let ast = parse_query(cypher).expect("Failed to parse complex OPTIONAL MATCH + VLP query");

    // Use evaluate_read_query for full pipeline including projection tagging
    let result = evaluate_read_query(ast, &schema, None, None);
    assert!(
        result.is_ok(),
        "Failed to build logical plan for complex query: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan(logical_plan, &schema);
    assert!(
        render_result.is_ok(),
        "Failed to render SQL for complex query: {:?}",
        render_result.err()
    );

    let render_plan = render_result.unwrap();
    let sql = render_plan.to_sql();
    println!("Generated SQL:\n{}", sql);

    // Verify the query contains expected elements
    assert!(
        sql.to_lowercase().contains("left join"),
        "Should contain LEFT JOIN for OPTIONAL MATCH"
    );
    // count(f) should be resolved to count(f.user_id) for correct LEFT JOIN NULL handling
    assert!(
        sql.to_lowercase().contains("count(") && !sql.to_lowercase().contains("count(*)"),
        "Should contain count(node.id_column) not count(*) for LEFT JOIN correctness"
    );
}

/// Test shortestPath combined with WITH clause and filtering
#[tokio::test]
#[ignore = "shortestPath with WITH clause not yet implemented"]
async fn test_shortest_path_with_with_clause() {
    let schema = create_test_schema();

    let cypher = r#"
        MATCH path = shortestPath((a:User)-[*]-(b:User))
        WITH path, length(path) as dist
        WHERE dist > 2
        RETURN nodes(path)
    "#;

    let ast = parse_query(cypher).expect("Failed to parse shortestPath + WITH + WHERE");

    let result = evaluate_read_query(ast, &schema, None, None);
    assert!(
        result.is_ok(),
        "Failed to build plan for shortestPath + WITH: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan(logical_plan, &schema);
    assert!(
        render_result.is_ok(),
        "Failed to render SQL for shortestPath + WITH: {:?}",
        render_result.err()
    );

    let render_plan = render_result.unwrap();
    let sql = render_plan.to_sql();
    println!("Generated SQL:\n{}", sql);

    // Verify the query contains expected elements
    assert!(
        sql.to_lowercase().contains("with recursive"),
        "Should contain WITH RECURSIVE for shortestPath"
    );
    assert!(
        sql.to_lowercase().contains("length("),
        "Should contain length function"
    );
}

/// Test multiple relationship types combined with VLP
#[tokio::test]
async fn test_multiple_relationship_types_with_vlp() {
    let schema = create_test_schema();

    let cypher = r#"
        MATCH (a:User)-[r:FOLLOWS*1..3]->(b:User)
        RETURN a.name, b.name, type(r)
    "#;

    let ast = parse_query(cypher).expect("Failed to parse multiple rel types + VLP");

    let result = evaluate_read_query(ast, &schema, None, None);
    assert!(
        result.is_ok(),
        "Failed to build plan for multiple rel types + VLP: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan(logical_plan, &schema);
    assert!(
        render_result.is_ok(),
        "Failed to render SQL for multiple rel types + VLP: {:?}",
        render_result.err()
    );

    let render_plan = render_result.unwrap();
    let sql = render_plan.to_sql();
    println!("Generated SQL:\n{}", sql);

    // Verify the query contains expected elements
    assert!(
        sql.to_lowercase().contains("union"),
        "Should contain UNION for multiple relationship types"
    );
}

/// Test WITH clause property renaming and object passing
#[tokio::test]
async fn test_with_clause_property_renaming() {
    let schema = create_test_schema();

    let cypher = r#"
        MATCH (u:User)
        WITH u.name as username, u as user_obj
        MATCH (user_obj)-[:FOLLOWS]->(f:User)
        RETURN username, f.name
    "#;

    let ast = parse_query(cypher).expect("Failed to parse WITH clause property renaming");

    let result = evaluate_read_query(ast, &schema, None, None);
    assert!(
        result.is_ok(),
        "Failed to evaluate read query for WITH clause property renaming: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan(logical_plan, &schema);
    assert!(
        render_result.is_ok(),
        "Failed to render SQL for WITH clause property renaming: {:?}",
        render_result.err()
    );

    let render_plan = render_result.unwrap();
    let sql = render_plan.to_sql();
    println!("Generated SQL:\n{}", sql);

    // Verify the query contains expected elements
    assert!(
        sql.to_lowercase().contains("with"),
        "Should contain WITH clause"
    );
}

/// Test complex aggregation with multiple features
#[tokio::test]
async fn test_complex_aggregation_with_multiple_features() {
    let schema = create_test_schema();

    let cypher = r#"
        MATCH (u:User)
        OPTIONAL MATCH (u)-[:FOLLOWS*1..2]->(f:User)
        WITH u, COUNT(f) as follower_count, COLLECT(f.name) as follower_names
        WHERE follower_count > 0
        RETURN u.name, follower_count, follower_names
        ORDER BY follower_count DESC
        LIMIT 10
    "#;

    let ast = parse_query(cypher).expect("Failed to parse complex aggregation query");

    let result = evaluate_read_query(ast, &schema, None, None);
    assert!(
        result.is_ok(),
        "Failed to build plan for complex aggregation: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan(logical_plan, &schema);
    assert!(
        render_result.is_ok(),
        "Failed to render SQL for complex aggregation: {:?}",
        render_result.err()
    );

    let render_plan = render_result.unwrap();
    let sql = render_plan.to_sql();
    println!("Generated SQL:\n{}", sql);

    // Verify the query contains expected elements
    assert!(
        sql.to_lowercase().contains("grouparray(") || sql.to_lowercase().contains("count("),
        "Should contain aggregation functions (groupArray or COUNT)"
    );
    assert!(
        sql.to_lowercase().contains("limit"),
        "Should contain LIMIT clause"
    );
}

/// Test shortestPath with property filters
#[tokio::test]
async fn test_shortest_path_with_property_filters() {
    let schema = create_test_schema();

    let cypher = r#"
        MATCH path = shortestPath(
            (a:User)-[r:FOLLOWS*]-(b:User)
        )
        WHERE length(path) <= 5
        RETURN path, length(path)
    "#;

    let ast = parse_query(cypher).expect("Failed to parse shortestPath with property filters");

    let result = evaluate_read_query(ast, &schema, None, None);
    assert!(
        result.is_ok(),
        "Failed to build plan for shortestPath with filters: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan(logical_plan, &schema);
    assert!(
        render_result.is_ok(),
        "Failed to render SQL for shortestPath with filters: {:?}",
        render_result.err()
    );

    let render_plan = render_result.unwrap();
    let sql = render_plan.to_sql();
    println!("Generated SQL:\n{}", sql);

    // Verify the query contains expected elements
    assert!(
        sql.to_lowercase().contains("with recursive"),
        "Should contain WITH RECURSIVE for shortestPath"
    );
}

/// Test VLP with relationship property filters
#[tokio::test]
async fn test_vlp_with_relationship_property_filters() {
    let schema = create_test_schema();

    let cypher = r#"
        MATCH (a:User)-[r:FOLLOWS*1..3]->(b:User)
        RETURN a.name, b.name, COUNT(r) as path_length
    "#;

    let ast = parse_query(cypher).expect("Failed to parse VLP with relationship filters");

    let result = evaluate_read_query(ast, &schema, None, None);
    assert!(
        result.is_ok(),
        "Failed to build plan for VLP with relationship filters: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan(logical_plan, &schema);
    assert!(
        render_result.is_ok(),
        "Failed to render SQL for VLP with relationship filters: {:?}",
        render_result.err()
    );

    let render_plan = render_result.unwrap();
    let sql = render_plan.to_sql();
    println!("Generated SQL:\n{}", sql);

    // Verify the query contains expected elements
    assert!(
        sql.to_lowercase().contains("count("),
        "Should contain COUNT aggregation"
    );
}

/// Test error handling for invalid VLP ranges
#[test]
fn test_invalid_vlp_range_error_handling() {
    let cypher = "MATCH (a)-[*0..0]->(b) RETURN a, b"; // Invalid: zero hops

    let result = parse_query(cypher);
    assert!(
        result.is_ok(),
        "Parsing should succeed, validation happens later"
    );

    // TODO: Add validation tests once we have query validation
}

/// Test error handling for invalid shortestPath patterns
#[test]
fn test_invalid_shortest_path_error_handling() {
    let cypher = "MATCH path = shortestPath((a)-[*0..0]->(b)) RETURN path"; // Invalid range

    let result = parse_query(cypher);
    assert!(
        result.is_ok(),
        "Parsing should succeed, validation happens later"
    );

    // TODO: Add validation tests once we have query validation
}

/// Test many concurrent relationship types
#[test]
fn test_many_relationship_types_union() {
    let schema = create_test_schema();

    let cypher = r#"
        MATCH (a:User)-[r:FOLLOWS]->(b:User)
        RETURN a.name, b.name, type(r)
    "#;

    let ast = parse_query(cypher).expect("Failed to parse many relationship types");

    let result = evaluate_read_query(ast, &schema, None, None);
    assert!(
        result.is_ok(),
        "Failed to build plan for many relationship types: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan(logical_plan, &schema);
    assert!(
        render_result.is_ok(),
        "Failed to render SQL for many relationship types: {:?}",
        render_result.err()
    );

    let render_plan = render_result.unwrap();
    let sql = render_plan.to_sql();
    println!("Generated SQL:\n{}", sql);
}

/// Test pattern comprehensions with complex patterns
#[tokio::test]
#[ignore = "pattern comprehensions with SIZE function not yet implemented"]
async fn test_pattern_comprehension_complex() {
    let schema = create_test_schema();

    let cypher = r#"
        MATCH (u:User)
        RETURN u.name,
               size([(u)-[:FOLLOWS*1..2]->(f:User) | f]) as friend_count
    "#;

    let ast = parse_query(cypher).expect("Failed to parse complex pattern comprehensions");

    let result = evaluate_read_query(ast, &schema, None, None);
    assert!(
        result.is_ok(),
        "Failed to build plan for pattern comprehensions: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan(logical_plan, &schema);
    assert!(
        render_result.is_ok(),
        "Failed to render SQL for pattern comprehensions: {:?}",
        render_result.err()
    );

    let render_plan = render_result.unwrap();
    let sql = render_plan.to_sql();
    println!("Generated SQL:\n{}", sql);

    // Verify the query contains expected elements
    assert!(
        sql.to_lowercase().contains("size("),
        "Should contain SIZE function"
    );
}

/// Test multiple OPTIONAL MATCH clauses in a single query
#[tokio::test]
async fn test_multiple_optional_match_clauses() {
    let schema = create_test_schema();

    let cypher = r#"
        MATCH (u:User)
        OPTIONAL MATCH (u)-[:FOLLOWS]->(f:User)
        OPTIONAL MATCH (u)-[:AUTHORED]->(p:Post)
        RETURN u.name, COUNT(f) as follower_count, COUNT(p) as post_count
    "#;

    let ast = parse_query(cypher).expect("Failed to parse multiple OPTIONAL MATCH query");

    let result = evaluate_read_query(ast, &schema, None, None);
    assert!(
        result.is_ok(),
        "Failed to build logical plan for multiple optional matches: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan(logical_plan, &schema);
    assert!(
        render_result.is_ok(),
        "Failed to render SQL for multiple optional matches: {:?}",
        render_result.err()
    );

    let render_plan = render_result.unwrap();
    let sql = render_plan.to_sql();
    println!("Generated SQL:\n{}", sql);

    // Verify the query contains expected elements
    assert!(
        sql.to_lowercase().contains("count("),
        "Should contain COUNT aggregations"
    );
}

/// Test OPTIONAL MATCH with WHERE clauses and complex conditions
#[tokio::test]
async fn test_optional_match_with_where_conditions() {
    let schema = create_test_schema();

    let cypher = r#"
        MATCH (u:User)
        OPTIONAL MATCH (u)-[:FOLLOWS]->(f:User)
        WHERE f.is_active = true AND f.registration_date > '2023-01-01'
        RETURN u.name, COUNT(f) as active_follower_count
    "#;

    let ast = parse_query(cypher).expect("Failed to parse OPTIONAL MATCH with WHERE");

    let result = evaluate_read_query(ast, &schema, None, None);
    assert!(
        result.is_ok(),
        "Failed to build logical plan for optional match with WHERE: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan(logical_plan, &schema);
    assert!(
        render_result.is_ok(),
        "Failed to render SQL for optional match with WHERE: {:?}",
        render_result.err()
    );

    let render_plan = render_result.unwrap();
    let sql = render_plan.to_sql();
    println!("Generated SQL:\n{}", sql);

    // Verify the query contains expected elements
    assert!(
        sql.to_lowercase().contains("where"),
        "Should contain WHERE clause"
    );
    assert!(
        sql.to_lowercase().contains("count("),
        "Should contain COUNT aggregation"
    );
}

/// Test complex aggregations with multiple functions and GROUP BY
#[tokio::test]
async fn test_complex_aggregations_with_group_by() {
    let schema = create_test_schema();

    let cypher = r#"
        MATCH (u:User)-[:FOLLOWS]->(f:User)
        RETURN
            u.name,
            COUNT(f) as follower_count,
            COUNT(DISTINCT f) as distinct_followers,
            MAX(f.registration_date) as latest_follower_date,
            MIN(f.registration_date) as earliest_follower_date
    "#;

    let ast = parse_query(cypher).expect("Failed to parse complex aggregations query");

    let result = evaluate_read_query(ast, &schema, None, None);
    assert!(
        result.is_ok(),
        "Failed to build logical plan for complex aggregations: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan(logical_plan, &schema);
    assert!(
        render_result.is_ok(),
        "Failed to render SQL for complex aggregations: {:?}",
        render_result.err()
    );

    let render_plan = render_result.unwrap();
    let sql = render_plan.to_sql();
    println!("Generated SQL:\n{}", sql);

    // Verify the query contains expected elements
    assert!(
        sql.to_lowercase().contains("count("),
        "Should contain COUNT functions"
    );
    assert!(
        sql.to_lowercase().contains("max("),
        "Should contain MAX function"
    );
    assert!(
        sql.to_lowercase().contains("min("),
        "Should contain MIN function"
    );
    assert!(
        sql.to_lowercase().contains("distinct"),
        "Should contain DISTINCT keyword"
    );
}

/// Test complex WHERE clauses with multiple conditions and operators
#[tokio::test]
async fn test_complex_where_clauses_multiple_conditions() {
    let schema = create_test_schema();

    let cypher = r#"
        MATCH (u:User)-[:FOLLOWS]->(f:User)
        WHERE u.is_active = true
          AND f.is_active = true
          AND u.registration_date >= '2020-01-01'
          AND f.registration_date >= '2020-01-01'
          AND u.user_id <> f.user_id
        RETURN u.name, f.name
    "#;

    let ast = parse_query(cypher).expect("Failed to parse complex WHERE conditions");

    let result = evaluate_read_query(ast, &schema, None, None);
    assert!(
        result.is_ok(),
        "Failed to build logical plan for complex WHERE: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan(logical_plan, &schema);
    assert!(
        render_result.is_ok(),
        "Failed to render SQL for complex WHERE: {:?}",
        render_result.err()
    );

    let render_plan = render_result.unwrap();
    let sql = render_plan.to_sql();
    println!("Generated SQL:\n{}", sql);

    // Verify the query contains expected elements
    assert!(
        sql.to_lowercase().contains("where"),
        "Should contain WHERE clause"
    );
    assert!(
        sql.to_lowercase().contains("and"),
        "Should contain AND operators"
    );
}

/// Test ORDER BY with complex expressions and aggregations
#[tokio::test]
async fn test_order_by_with_complex_expressions() {
    let schema = create_test_schema();

    let cypher = r#"
        MATCH (u:User)-[:FOLLOWS]->(f:User)
        RETURN u.name, COUNT(f) as follower_count
        ORDER BY follower_count DESC, u.name ASC
    "#;

    let ast = parse_query(cypher).expect("Failed to parse ORDER BY with aggregations");

    let result = evaluate_read_query(ast, &schema, None, None);
    assert!(
        result.is_ok(),
        "Failed to build logical plan for ORDER BY: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan(logical_plan, &schema);
    assert!(
        render_result.is_ok(),
        "Failed to render SQL for ORDER BY: {:?}",
        render_result.err()
    );

    let render_plan = render_result.unwrap();
    let sql = render_plan.to_sql();
    println!("Generated SQL:\n{}", sql);

    // Verify the query contains expected elements
    assert!(
        sql.to_lowercase().contains("order by"),
        "Should contain ORDER BY clause"
    );
    assert!(
        sql.to_lowercase().contains("desc"),
        "Should contain DESC keyword"
    );
    assert!(
        sql.to_lowercase().contains("asc"),
        "Should contain ASC keyword"
    );
}

/// Test LIMIT and OFFSET with complex queries
#[tokio::test]
#[ignore = "LIMIT/OFFSET with VLP not yet implemented"]
async fn test_limit_offset_with_complex_queries() {
    let schema = create_test_schema();

    let cypher = r#"
        MATCH (u:User)-[:FOLLOWS*1..2]->(f:User)
        RETURN u.name, COUNT(f) as connection_count
        ORDER BY connection_count DESC
        LIMIT 10 OFFSET 20
    "#;

    let ast = parse_query(cypher).expect("Failed to parse LIMIT/OFFSET query");

    let result = evaluate_read_query(ast, &schema, None, None);
    assert!(
        result.is_ok(),
        "Failed to build logical plan for LIMIT/OFFSET: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan(logical_plan, &schema);
    assert!(
        render_result.is_ok(),
        "Failed to render SQL for LIMIT/OFFSET: {:?}",
        render_result.err()
    );

    let render_plan = render_result.unwrap();
    let sql = render_plan.to_sql();
    println!("Generated SQL:\n{}", sql);

    // Verify the query contains expected elements
    assert!(
        sql.to_lowercase().contains("limit"),
        "Should contain LIMIT clause"
    );
    assert!(
        sql.to_lowercase().contains("offset"),
        "Should contain OFFSET clause"
    );
}

/// Test complex CASE expressions in RETURN clauses
#[tokio::test]
async fn test_case_expressions_in_return() {
    let schema = create_test_schema();

    let cypher = r#"
        MATCH (u:User)
        OPTIONAL MATCH (u)-[:FOLLOWS]->(f:User)
        RETURN u.name,
               CASE
                 WHEN COUNT(f) = 0 THEN 'No followers'
                 WHEN COUNT(f) < 5 THEN 'Few followers'
                 WHEN COUNT(f) < 20 THEN 'Some followers'
                 ELSE 'Popular'
               END as follower_category
    "#;

    let ast = parse_query(cypher).expect("Failed to parse CASE expression query");

    let result = evaluate_read_query(ast, &schema, None, None);
    assert!(
        result.is_ok(),
        "Failed to build logical plan for CASE expressions: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan(logical_plan, &schema);
    assert!(
        render_result.is_ok(),
        "Failed to render SQL for CASE expressions: {:?}",
        render_result.err()
    );

    let render_plan = render_result.unwrap();
    let sql = render_plan.to_sql();
    println!("Generated SQL:\n{}", sql);

    // Verify the query contains expected elements
    assert!(
        sql.to_lowercase().contains("case"),
        "Should contain CASE expression"
    );
    assert!(
        sql.to_lowercase().contains("when"),
        "Should contain WHEN clauses"
    );
    assert!(
        sql.to_lowercase().contains("then"),
        "Should contain THEN clauses"
    );
    assert!(
        sql.to_lowercase().contains("else"),
        "Should contain ELSE clause"
    );
    assert!(
        sql.to_lowercase().contains("end"),
        "Should contain END keyword"
    );
}

/// Test complex property access patterns with nested expressions
#[tokio::test]
async fn test_complex_property_access_patterns() {
    let schema = create_test_schema();

    let cypher = r#"
        MATCH (u:User)-[:FOLLOWS]->(f:User)
        WHERE u.email IS NOT NULL
          AND f.email IS NOT NULL
          AND u.is_active = true
        RETURN u.name, f.name, u.email, f.email
    "#;

    let ast = parse_query(cypher).expect("Failed to parse complex property access");

    let result = evaluate_read_query(ast, &schema, None, None);
    assert!(
        result.is_ok(),
        "Failed to build logical plan for complex property access: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan(logical_plan, &schema);
    assert!(
        render_result.is_ok(),
        "Failed to render SQL for complex property access: {:?}",
        render_result.err()
    );

    let render_plan = render_result.unwrap();
    let sql = render_plan.to_sql();
    println!("Generated SQL:\n{}", sql);

    // Verify the query contains expected elements
    assert!(
        sql.to_lowercase().contains("is not null"),
        "Should contain IS NOT NULL checks"
    );
}

/// Test UNION operations with complex feature combinations
#[tokio::test]
#[ignore = "UNION operations not yet implemented"]
async fn test_union_with_complex_features() {
    let schema = create_test_schema();

    let cypher = r#"
        MATCH (u:User)-[:FOLLOWS]->(f:User)
        WHERE u.is_active = true
        RETURN u.name as name, 'follower' as relationship_type
        UNION ALL
        MATCH (u:User)-[:AUTHORED]->(p:Post)
        WHERE u.is_active = true
        RETURN u.name as name, 'author' as relationship_type
    "#;

    let ast = parse_query(cypher).expect("Failed to parse UNION query");

    let result = evaluate_read_query(ast, &schema, None, None);
    assert!(
        result.is_ok(),
        "Failed to build logical plan for UNION: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan(logical_plan, &schema);
    assert!(
        render_result.is_ok(),
        "Failed to render SQL for UNION: {:?}",
        render_result.err()
    );

    let render_plan = render_result.unwrap();
    let sql = render_plan.to_sql();
    println!("Generated SQL:\n{}", sql);

    // Verify the query contains expected elements
    assert!(
        sql.to_lowercase().contains("union"),
        "Should contain UNION keyword"
    );
}

/// Test deeply nested expressions and function calls
#[tokio::test]
async fn test_deeply_nested_expressions() {
    let schema = create_test_schema();

    let cypher = r#"
        MATCH (u:User)
        OPTIONAL MATCH (u)-[:FOLLOWS]->(f:User)
        RETURN u.name,
               CASE
                 WHEN COUNT(f) > 10 THEN 'high'
                 WHEN COUNT(f) > 5 THEN 'medium'
                 ELSE 'low'
               END as influence_level,
               COUNT(f) * 2 as doubled_count
    "#;

    let ast = parse_query(cypher).expect("Failed to parse deeply nested expressions");

    let result = evaluate_read_query(ast, &schema, None, None);
    assert!(
        result.is_ok(),
        "Failed to build logical plan for nested expressions: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan(logical_plan, &schema);
    assert!(
        render_result.is_ok(),
        "Failed to render SQL for nested expressions: {:?}",
        render_result.err()
    );

    let render_plan = render_result.unwrap();
    let sql = render_plan.to_sql();
    println!("Generated SQL:\n{}", sql);

    // Verify the query contains expected elements
    assert!(
        sql.to_lowercase().contains("case"),
        "Should contain CASE expression"
    );
    assert!(
        sql.to_lowercase().contains("*"),
        "Should contain multiplication operator"
    );
}

/// Regression test: VLP+WITH CTE JOIN must use node's actual ID column (e.g., user_id),
/// not the VLP's generic start_id/end_id.
///
/// The Neo4j Browser click-to-expand sends:
///   MATCH (a) WHERE id(a) = N
///   WITH a, size([(a)--() | 1]) AS allNeighboursCount
///   MATCH path = (a)--(o) RETURN path, allNeighboursCount
///
/// The final SELECT JOINs the VLP CTE to the WITH CTE. The JOIN condition
/// must reference the WITH CTE's actual ID column (a_user_id), not a_start_id.
#[tokio::test]
async fn test_vlp_with_cte_join_uses_node_id_not_start_id() {
    let schema = create_test_schema();

    let cypher = r#"
        MATCH (a:User) WHERE a.user_id = 1
        WITH a, size([(a)--() | 1]) AS allNeighboursCount
        MATCH path = (a)--(o)
        RETURN path, allNeighboursCount
        LIMIT 10
    "#;

    let ast = parse_query(cypher).expect("Failed to parse browser expand query");

    let result = evaluate_read_query(ast, &schema, None, None);
    assert!(
        result.is_ok(),
        "Failed to evaluate browser expand query: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan(logical_plan, &schema);
    assert!(
        render_result.is_ok(),
        "Failed to render SQL for browser expand query: {:?}",
        render_result.err()
    );

    let render_plan = render_result.unwrap();
    let sql = render_plan.to_sql();
    println!("Generated SQL:\n{}", sql);

    // The critical assertion: JOIN must use the node's real ID column, not VLP start_id
    assert!(
        !sql.contains("a_start_id"),
        "BUG REGRESSION: JOIN references a_start_id instead of node's actual ID column.\n\
         The WITH CTE exports a_user_id, not a_start_id.\n\
         SQL:\n{}",
        sql
    );

    // Verify the correct column is used
    assert!(
        sql.contains("a_user_id"),
        "JOIN should reference a_user_id from the WITH CTE.\nSQL:\n{}",
        sql
    );

    // Verify basic structure
    assert!(sql.contains("vlp_"), "Should contain VLP CTE");
    assert!(
        sql.contains("with_a_allNeighboursCount"),
        "Should contain WITH CTE for allNeighboursCount"
    );
}

/// Test reversed-anchor OPTIONAL MATCH with WHERE predicate on optional node.
/// When the anchor (already-matched node) is on the right side of the OPTIONAL MATCH
/// pattern, join order must be reversed. This tests that WHERE predicates on the
/// optional (left) node are still correctly applied.
#[tokio::test]
async fn test_reversed_anchor_optional_match_with_where_predicate() {
    let schema = create_test_schema();

    // Pattern: MATCH establishes 'u', then OPTIONAL MATCH has 'u' as right_connection
    // (anchor on the right) with WHERE filtering on the optional left node 'f'
    let cypher = r#"
        MATCH (u:User)-[:AUTHORED]->(p:Post)
        OPTIONAL MATCH (f:User)-[:FOLLOWS]->(u)
        WHERE f.is_active = true
        RETURN u.name, p.title, f.name AS follower_name
    "#;

    let ast = parse_query(cypher)
        .expect("Failed to parse reversed-anchor OPTIONAL MATCH with WHERE predicate");

    // Use build_logical_plan here because this test validates reversed-anchor
    // WHERE clause handling, not aggregation resolution (no COUNT in query).
    let result = build_logical_plan(&ast, &schema, None, None, None);
    assert!(
        result.is_ok(),
        "Failed to build logical plan: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan((*logical_plan).clone(), &schema);
    assert!(
        render_result.is_ok(),
        "Failed to render SQL: {:?}",
        render_result.err()
    );

    let render_plan = render_result.unwrap();
    let sql = render_plan.to_sql();
    println!("Generated SQL:\n{}", sql);

    let sql_lower = sql.to_lowercase();

    // Must contain LEFT JOIN for the optional relationship and node
    assert!(
        sql_lower.contains("left join"),
        "Should contain LEFT JOIN for OPTIONAL MATCH.\nSQL:\n{}",
        sql
    );

    // The WHERE predicate on the optional node (f.is_active) must appear in the SQL
    assert!(
        sql_lower.contains("is_active"),
        "Should contain is_active predicate from WHERE clause.\nSQL:\n{}",
        sql
    );
}

/// Test VLP path function `length(path)` inside WITH clause
#[tokio::test]
async fn test_vlp_length_path_in_with_clause() {
    let schema = create_test_schema();

    let cypher = r#"
        MATCH path = (a:User)-[:FOLLOWS*1..3]->(b:User)
        WITH a, b, length(path) as hops
        WHERE hops = 2
        RETURN a.name, b.name, hops
    "#;

    let ast = parse_query(cypher).expect("Failed to parse VLP length(path) in WITH");

    let result = evaluate_read_query(ast, &schema, None, None);
    assert!(result.is_ok(), "Failed to build plan: {:?}", result.err());

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan(logical_plan, &schema);
    assert!(
        render_result.is_ok(),
        "Failed to render SQL: {:?}",
        render_result.err()
    );

    let render_plan = render_result.unwrap();
    let sql = render_plan.to_sql();
    println!("Generated SQL:\n{}", sql);

    let sql_lower = sql.to_lowercase();

    // VLP should generate recursive CTE
    assert!(
        sql_lower.contains("recursive"),
        "Should contain RECURSIVE CTE for VLP.\nSQL:\n{}",
        sql
    );

    // length(path) should be rewritten to t.hop_count, not literal "length(path)"
    assert!(
        !sql_lower.contains("length(path)"),
        "length(path) should be rewritten to hop_count, not left as literal.\nSQL:\n{}",
        sql
    );

    // Should contain hop_count reference (from VLP CTE)
    assert!(
        sql_lower.contains("hop_count"),
        "Should contain hop_count from VLP CTE rewriting.\nSQL:\n{}",
        sql
    );
}

/// Test VLP path functions `nodes(path)` and `relationships(path)` inside WITH clause
#[tokio::test]
async fn test_vlp_nodes_relationships_path_in_with_clause() {
    let schema = create_test_schema();

    let cypher = r#"
        MATCH path = (a:User)-[:FOLLOWS*1..2]->(b:User)
        WITH a, b, nodes(path) as path_nodes, relationships(path) as path_rels
        RETURN a.name, size(path_nodes) as node_count, size(path_rels) as rel_count
    "#;

    let ast = parse_query(cypher).expect("Failed to parse VLP nodes/relationships in WITH");

    let result = evaluate_read_query(ast, &schema, None, None);
    assert!(result.is_ok(), "Failed to build plan: {:?}", result.err());

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan(logical_plan, &schema);
    assert!(
        render_result.is_ok(),
        "Failed to render SQL: {:?}",
        render_result.err()
    );

    let render_plan = render_result.unwrap();
    let sql = render_plan.to_sql();
    println!("Generated SQL:\n{}", sql);

    let sql_lower = sql.to_lowercase();

    // nodes(path) should be rewritten to path_nodes CTE column
    assert!(
        !sql_lower.contains("nodes(path)"),
        "nodes(path) should be rewritten, not left as literal.\nSQL:\n{}",
        sql
    );

    // relationships(path) should be rewritten to path_relationships CTE column
    assert!(
        !sql_lower.contains("relationships(path)"),
        "relationships(path) should be rewritten, not left as literal.\nSQL:\n{}",
        sql
    );

    // Should contain path_nodes and path_relationships references
    assert!(
        sql_lower.contains("path_nodes"),
        "Should contain path_nodes from VLP CTE rewriting.\nSQL:\n{}",
        sql
    );
    assert!(
        sql_lower.contains("path_relationships"),
        "Should contain path_relationships from VLP CTE rewriting.\nSQL:\n{}",
        sql
    );
}

/// Regression: VLP + WITH using specific property access (u1.name) must:
/// 1. Include property columns in VLP CTE (property requirements must match DB column names)
/// 2. Reference correct VLP CTE column name (start_name, not start_full_name)
#[tokio::test]
async fn test_vlp_with_specific_property_access() {
    let schema = create_test_schema();

    let cypher = r#"
        MATCH path = (a:User)-[:FOLLOWS*1..2]->(b:User)
        WITH a.name as start_name, b.name as end_name
        WHERE start_name IS NOT NULL
        RETURN start_name, end_name
    "#;

    let ast = parse_query(cypher).expect("Failed to parse VLP WITH property access");

    let result = evaluate_read_query(ast, &schema, None, None);
    assert!(result.is_ok(), "Failed to build plan: {:?}", result.err());

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan(logical_plan, &schema);
    assert!(
        render_result.is_ok(),
        "Failed to render SQL: {:?}",
        render_result.err()
    );

    let render_plan = render_result.unwrap();
    let sql = render_plan.to_sql();
    println!("Generated SQL:\n{}", sql);

    let sql_lower = sql.to_lowercase();

    // VLP CTE must include property columns (start_name, end_name)
    assert!(
        sql_lower.contains("as start_name"),
        "VLP CTE should include start_name property column.\nSQL:\n{}",
        sql
    );
    assert!(
        sql_lower.contains("as end_name"),
        "VLP CTE should include end_name property column.\nSQL:\n{}",
        sql
    );

    // CTE body must reference t.start_name (Cypher name), NOT t.start_full_name (DB name)
    assert!(
        !sql_lower.contains("start_full_name"),
        "CTE body should use Cypher property name (start_name), not DB column name (start_full_name).\nSQL:\n{}",
        sql
    );
}

/// Regression: VLP path_relationships must be an array (not UInt8 placeholder)
/// when a path variable is assigned and relationships(path) is used in WITH
#[tokio::test]
async fn test_vlp_path_relationships_is_array() {
    let schema = create_test_schema();

    let cypher = r#"
        MATCH path = (a:User)-[:FOLLOWS*1..2]->(b:User)
        WITH a, b, relationships(path) as rels
        RETURN a.name, size(rels) as rel_count
    "#;

    let ast = parse_query(cypher).expect("Failed to parse VLP relationships in WITH");

    let result = evaluate_read_query(ast, &schema, None, None);
    assert!(result.is_ok(), "Failed to build plan: {:?}", result.err());

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan(logical_plan, &schema);
    assert!(
        render_result.is_ok(),
        "Failed to render SQL: {:?}",
        render_result.err()
    );

    let render_plan = render_result.unwrap();
    let sql = render_plan.to_sql();
    println!("Generated SQL:\n{}", sql);

    // path_relationships should be an array, not CAST(0 AS UInt8) placeholder
    // Normalize to lowercase for case-insensitive matching
    let sql_lower = sql.to_lowercase();
    assert!(
        !sql_lower.contains("cast(0 as uint8) as path_relationships"),
        "path_relationships should be a proper array, not UInt8 placeholder.\nSQL:\n{}",
        sql
    );
}

/// Test WITH alias rename + WHERE uses the renamed alias
#[tokio::test]
async fn test_with_alias_rename_where_filter() {
    use clickgraph::server::query_context::{set_current_schema, with_query_context, QueryContext};
    use std::sync::Arc;

    let schema = create_test_schema();

    let cypher = r#"
        MATCH (u:User)
        WITH u AS person
        WHERE person.name = 'Alice'
        RETURN person.name
    "#;

    let schema_clone = schema.clone();
    let ctx = QueryContext::new(Some("default".to_string()));
    let sql = with_query_context(ctx, async {
        set_current_schema(Arc::new(schema_clone));

        let ast = parse_query(cypher).expect("Failed to parse WITH alias rename + WHERE");

        let result = evaluate_read_query(ast, &schema, None, None);
        assert!(result.is_ok(), "Failed to build plan: {:?}", result.err());

        let (logical_plan, _plan_ctx) = result.unwrap();
        let render_result = logical_plan_to_render_plan(logical_plan, &schema);
        assert!(
            render_result.is_ok(),
            "Failed to render SQL: {:?}",
            render_result.err()
        );

        let render_plan = render_result.unwrap();
        render_plan.to_sql()
    })
    .await;

    println!("Generated SQL:\n{}", sql);

    let sql_lower = sql.to_lowercase();

    // WHERE clause should NOT reference "person" inside the CTE body
    // It should be rewritten to use the original alias "u"
    assert!(
        !sql_lower.contains("where person."),
        "WHERE should not use renamed alias 'person' inside CTE.\nSQL:\n{}",
        sql
    );

    // Should contain a WHERE filter referencing the table column
    assert!(
        sql_lower.contains("where") && sql_lower.contains("full_name"),
        "WHERE should filter on the mapped column.\nSQL:\n{}",
        sql
    );
}

/// Test: collect+unwind CTE does not produce duplicate columns after elimination
#[tokio::test]
async fn test_collect_unwind_no_duplicate_cte_columns() {
    use clickgraph::server::query_context::{set_current_schema, with_query_context, QueryContext};
    use std::sync::Arc;

    let config = clickgraph::graph_catalog::config::GraphSchemaConfig::from_yaml_file(
        "schemas/test/social_integration.yaml",
    )
    .expect("Failed to load social_integration schema");
    let schema = config.to_graph_schema().expect("Failed to convert schema");

    let cypher = r#"
        MATCH (u:User)
        WITH u, collect(u) as users
        UNWIND users as user
        RETURN user.name, user.email, user.city
        LIMIT 3
    "#;

    let schema_clone = schema.clone();
    let ctx = QueryContext::new(Some("default".to_string()));
    let sql = with_query_context(ctx, async {
        set_current_schema(Arc::new(schema_clone));

        let (_remaining, statement) =
            clickgraph::open_cypher_parser::parse_cypher_statement(cypher)
                .unwrap_or_else(|e| panic!("Failed to parse: {:?}", e));

        let (logical_plan, _plan_ctx) = clickgraph::query_planner::evaluate_read_statement(
            statement, &schema, None, None, None,
        )
        .unwrap_or_else(|e| panic!("Failed to plan: {:?}", e));

        let render_plan =
            clickgraph::render_plan::logical_plan_to_render_plan(logical_plan, &schema)
                .unwrap_or_else(|e| panic!("Failed to render: {:?}", e));
        render_plan.to_sql()
    })
    .await;

    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("select"),
        "Should have SELECT.\nSQL:\n{}",
        sql
    );

    // Verify no duplicate columns in CTE body (case-insensitive split on FROM)
    let sql_upper = sql.to_uppercase();
    let cte_body = sql_upper.split("FROM").next().unwrap_or("");
    let name_count = cte_body.matches("P1_U_NAME").count();
    assert_eq!(
        name_count, 1,
        "P1_U_NAME should appear once in CTE body, found {}.\nSQL:\n{}",
        name_count, sql
    );
}

/// Test: COUNT(p) on a fixed-hop path variable resolves to COUNT(*)
#[tokio::test]
async fn test_count_path_variable_fixed_hop() {
    use clickgraph::server::query_context::{set_current_schema, with_query_context, QueryContext};
    use std::sync::Arc;

    let schema = create_test_schema();

    let cypher = r#"
        MATCH p = (a:User)-[:FOLLOWS]->(b:User)
        RETURN COUNT(p) AS path_count
    "#;

    let schema_clone = schema.clone();
    let ctx = QueryContext::new(Some("default".to_string()));
    let sql = with_query_context(ctx, async {
        set_current_schema(Arc::new(schema_clone));

        let ast = parse_query(cypher).expect("Failed to parse COUNT(p) query");
        let result = evaluate_read_query(ast, &schema, None, None);
        assert!(result.is_ok(), "Failed to plan: {:?}", result.err());

        let (logical_plan, _plan_ctx) = result.unwrap();
        let render_result = logical_plan_to_render_plan(logical_plan, &schema);
        assert!(
            render_result.is_ok(),
            "Failed to render: {:?}",
            render_result.err()
        );

        let render_plan = render_result.unwrap();
        render_plan.to_sql()
    })
    .await;

    println!("Generated SQL for COUNT(p):\n{}", sql);

    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("count(*)") || sql_lower.contains("count(1)"),
        "COUNT(p) should be rewritten to COUNT(*) for fixed-hop path.\nSQL:\n{}",
        sql
    );
}

/// Regression test: Expression-based property mappings must survive through
/// the render-phase property mapping without being double-quoted as column names.
/// See PR #205 for the fix.
#[tokio::test]
async fn test_expression_property_preserved_through_render_phase() {
    // Create a schema where 'tier' is an Expression, not a Column
    let mut nodes = HashMap::new();
    let mut props = HashMap::new();
    props.insert(
        "user_id".to_string(),
        PropertyValue::Column("user_id".to_string()),
    );
    props.insert(
        "name".to_string(),
        PropertyValue::Column("full_name".to_string()),
    );
    props.insert(
        "score".to_string(),
        PropertyValue::Column("score".to_string()),
    );
    props.insert(
        "tier".to_string(),
        PropertyValue::Expression(
            "if(score >= 1000, 'gold', if(score >= 500, 'silver', 'bronze'))".to_string(),
        ),
    );
    nodes.insert(
        "User".to_string(),
        NodeSchema {
            database: "test".to_string(),
            table_name: "users".to_string(),
            column_names: vec![
                "user_id".to_string(),
                "full_name".to_string(),
                "score".to_string(),
            ],
            primary_keys: "user_id".to_string(),
            node_id: NodeIdSchema::single("user_id".to_string(), SchemaType::Integer),
            property_mappings: props,
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
            source: None,
        },
    );

    let schema = GraphSchema::build(1, "test".to_string(), nodes, HashMap::new());

    // Test 1: Expression property in WHERE clause
    let cypher = r#"MATCH (u:User) WHERE u.tier = 'gold' RETURN u.name"#;
    let ast = parse_query(cypher).expect("Failed to parse");
    let result = evaluate_read_query(ast, &schema, None, None);
    assert!(result.is_ok(), "Failed to plan: {:?}", result.err());

    let (plan, _ctx) = result.unwrap();
    let render = logical_plan_to_render_plan(plan, &schema);
    assert!(render.is_ok(), "Failed to render: {:?}", render.err());

    let sql = render.unwrap().to_sql();
    println!("Expression in WHERE:\n{}", sql);

    // Must contain the expression, NOT a quoted column name
    assert!(
        sql.contains("score >= 1000"),
        "Expression property 'tier' should render as the ClickHouse expression, not a quoted column.\nSQL:\n{}",
        sql
    );
    assert!(
        !sql.contains("\"if("),
        "Expression should NOT be double-quoted as a column name.\nSQL:\n{}",
        sql
    );

    // Test 2: Expression property in RETURN clause
    let cypher2 = r#"MATCH (u:User) RETURN u.tier, u.name"#;
    let ast2 = parse_query(cypher2).expect("Failed to parse");
    let result2 = evaluate_read_query(ast2, &schema, None, None);
    assert!(result2.is_ok(), "Failed to plan: {:?}", result2.err());

    let (plan2, _ctx2) = result2.unwrap();
    let render2 = logical_plan_to_render_plan(plan2, &schema);
    assert!(render2.is_ok(), "Failed to render: {:?}", render2.err());

    let sql2 = render2.unwrap().to_sql();
    println!("Expression in RETURN:\n{}", sql2);

    assert!(
        sql2.contains("score >= 1000"),
        "Expression property in RETURN should render as ClickHouse expression.\nSQL:\n{}",
        sql2
    );
}
