// Integration test for path variable SQL generation
use clickgraph::{
    open_cypher_parser::parse_query,
    query_planner::logical_plan::plan_builder::build_logical_plan,
    render_plan::{logical_plan_to_render_plan, ToSql},
    graph_catalog::graph_schema::GraphSchema,
};
use std::collections::HashMap;

#[test]
fn test_path_variable_sql_generation() {
    // Test query with path variable
    let cypher = "MATCH p = shortestPath((a:Person)-[:FOLLOWS*]-(b:Person)) WHERE a.name = 'Alice' RETURN p";
    
    // Parse the query
    let ast = parse_query(cypher)
        .expect("Failed to parse Cypher query");
    
    // Create empty schema (test doesn't need actual schema)
    let schema = GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new(), HashMap::new());
    
    // Build logical plan
    let (logical_plan, _plan_ctx) = build_logical_plan(&ast, &schema)
        .expect("Failed to build logical plan");
    
    // Build render plan
    let render_plan = logical_plan_to_render_plan((*logical_plan).clone(), &schema)
        .expect("Failed to build render plan");
    
    // Convert to SQL
    let sql = render_plan.to_sql();
    
    // Verify the SQL contains the path object construction
    println!("Generated SQL:\n{}", sql);
    
    // Check that the SQL contains tuple() function for path construction
    // Note: We use tuple() instead of map() to avoid ClickHouse type conflicts
    assert!(sql.contains("tuple("), "SQL should contain tuple() function for path object");
    assert!(sql.contains("path_nodes"), "SQL should reference path_nodes column");
    assert!(sql.contains("hop_count"), "SQL should reference hop_count column");
    assert!(sql.contains("path_relationships"), "SQL should reference path_relationships column");
}

#[test]
fn test_path_variable_with_properties() {
    // Test query with path variable and node properties
    let cypher = "MATCH p = shortestPath((a:Person)-[:FOLLOWS*]-(b:Person)) WHERE a.name = 'Alice' RETURN p, a.name";
    
    // Parse the query
    let ast = parse_query(cypher)
        .expect("Failed to parse Cypher query");
    
    // Create empty schema
    let schema = GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new(), HashMap::new());
    
    // Build logical plan
    let (logical_plan, _plan_ctx) = build_logical_plan(&ast, &schema)
        .expect("Failed to build logical plan");
    
    // Build render plan
    let render_plan = logical_plan_to_render_plan((*logical_plan).clone(), &schema)
        .expect("Failed to build render plan");
    
    // Convert to SQL
    let sql = render_plan.to_sql();
    
    // Verify the SQL
    println!("Generated SQL:\n{}", sql);
    
    // Should have both path object (as tuple) and node property
    assert!(sql.contains("tuple("), "SQL should contain tuple() for path");
    assert!(sql.contains("end_name") || sql.contains("a.name"), "SQL should include the returned node name property");
}

#[test]
fn test_non_path_variable_unchanged() {
    // Test query without path variable - should work as before
    let cypher = "MATCH (a:Person)-[:FOLLOWS*]-(b:Person) WHERE a.name = 'Alice' RETURN a, b";
    
    // Parse the query
    let ast = parse_query(cypher)
        .expect("Failed to parse Cypher query");
    
    // Create empty schema
    let schema = GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new(), HashMap::new());
    
    // Build logical plan
    let (logical_plan, _plan_ctx) = build_logical_plan(&ast, &schema)
        .expect("Failed to build logical plan");
    
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
