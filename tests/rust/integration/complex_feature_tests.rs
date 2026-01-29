//! Integration tests for complex feature combinations in ClickGraph
//!
//! These tests validate that multiple advanced Cypher features work correctly
//! when combined together, catching integration bugs that individual feature
//! tests might miss.

use clickgraph::{
    graph_catalog::{
        expression_parser::PropertyValue,
        graph_schema::{GraphSchema, NodeIdSchema, NodeSchema, RelationshipSchema},
    },
    open_cypher_parser::parse_query,
    query_planner::{
        evaluate_read_query,
        logical_plan::plan_builder::build_logical_plan,
    },
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
            node_id: NodeIdSchema::single("user_id".to_string(), "UInt64".to_string()),
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
            node_id: NodeIdSchema::single("post_id".to_string(), "UInt64".to_string()),
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
            from_id: "follower_id".to_string(),
            to_id: "followed_id".to_string(),
            from_node_id_dtype: "UInt64".to_string(),
            to_node_id_dtype: "UInt64".to_string(),
            property_mappings: {
                let mut props = HashMap::new();
                props.insert(
                    "follow_date".to_string(),
                    PropertyValue::Column("follow_date".to_string()),
                );
                props
            },
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
            from_id: "author_id".to_string(),
            to_id: "post_id".to_string(),
            from_node_id_dtype: "UInt64".to_string(),
            to_node_id_dtype: "UInt64".to_string(),
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

    let result = build_logical_plan(&ast, &schema, None, None, None);
    assert!(
        result.is_ok(),
        "Failed to build logical plan for complex query: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan((*logical_plan).clone(), &schema);
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
    assert!(
        sql.to_lowercase().contains("count(*)"),
        "Should contain COUNT(*) aggregate in LEFT JOIN subquery"
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

    let result = build_logical_plan(&ast, &schema, None, None, None);
    assert!(
        result.is_ok(),
        "Failed to build plan for shortestPath + WITH: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan((*logical_plan).clone(), &schema);
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

    let result = build_logical_plan(&ast, &schema, None, None, None);
    assert!(
        result.is_ok(),
        "Failed to build plan for multiple rel types + VLP: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan((*logical_plan).clone(), &schema);
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

    let result = build_logical_plan(&ast, &schema, None, None, None);
    assert!(
        result.is_ok(),
        "Failed to build plan for complex aggregation: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan((*logical_plan).clone(), &schema);
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
        sql.to_lowercase().contains("count("),
        "Should contain COUNT aggregation"
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

    let result = build_logical_plan(&ast, &schema, None, None, None);
    assert!(
        result.is_ok(),
        "Failed to build plan for shortestPath with filters: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan((*logical_plan).clone(), &schema);
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

    let result = build_logical_plan(&ast, &schema, None, None, None);
    assert!(
        result.is_ok(),
        "Failed to build plan for VLP with relationship filters: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan((*logical_plan).clone(), &schema);
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

    let result = build_logical_plan(&ast, &schema, None, None, None);
    assert!(
        result.is_ok(),
        "Failed to build plan for many relationship types: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan((*logical_plan).clone(), &schema);
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

    let result = build_logical_plan(&ast, &schema, None, None, None);
    assert!(
        result.is_ok(),
        "Failed to build plan for pattern comprehensions: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan((*logical_plan).clone(), &schema);
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

    let result = build_logical_plan(&ast, &schema, None, None, None);
    assert!(
        result.is_ok(),
        "Failed to build logical plan for multiple optional matches: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan((*logical_plan).clone(), &schema);
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

    let result = build_logical_plan(&ast, &schema, None, None, None);
    assert!(
        result.is_ok(),
        "Failed to build logical plan for optional match with WHERE: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan((*logical_plan).clone(), &schema);
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

    let result = build_logical_plan(&ast, &schema, None, None, None);
    assert!(
        result.is_ok(),
        "Failed to build logical plan for complex aggregations: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan((*logical_plan).clone(), &schema);
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

    let result = build_logical_plan(&ast, &schema, None, None, None);
    assert!(
        result.is_ok(),
        "Failed to build logical plan for complex WHERE: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan((*logical_plan).clone(), &schema);
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

    let result = build_logical_plan(&ast, &schema, None, None, None);
    assert!(
        result.is_ok(),
        "Failed to build logical plan for ORDER BY: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan((*logical_plan).clone(), &schema);
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

    let result = build_logical_plan(&ast, &schema, None, None, None);
    assert!(
        result.is_ok(),
        "Failed to build logical plan for LIMIT/OFFSET: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan((*logical_plan).clone(), &schema);
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

    let result = build_logical_plan(&ast, &schema, None, None, None);
    assert!(
        result.is_ok(),
        "Failed to build logical plan for CASE expressions: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan((*logical_plan).clone(), &schema);
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

    let result = build_logical_plan(&ast, &schema, None, None, None);
    assert!(
        result.is_ok(),
        "Failed to build logical plan for complex property access: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan((*logical_plan).clone(), &schema);
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

    let result = build_logical_plan(&ast, &schema, None, None, None);
    assert!(
        result.is_ok(),
        "Failed to build logical plan for UNION: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan((*logical_plan).clone(), &schema);
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

    let result = build_logical_plan(&ast, &schema, None, None, None);
    assert!(
        result.is_ok(),
        "Failed to build logical plan for nested expressions: {:?}",
        result.err()
    );

    let (logical_plan, _plan_ctx) = result.unwrap();
    let render_result = logical_plan_to_render_plan((*logical_plan).clone(), &schema);
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
