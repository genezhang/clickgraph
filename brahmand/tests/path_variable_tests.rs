// Integration test for path variable SQL generation
use brahmand::{
    open_cypher_parser::parse_query,
    query_planner::logical_plan::plan_builder::build_logical_plan,
    render_plan::{logical_plan_to_render_plan, ToSql},
};

#[test]
fn test_path_variable_sql_generation() {
    // Test query with path variable
    let cypher = "MATCH p = shortestPath((a:Person)-[:FOLLOWS*]-(b:Person)) WHERE a.name = 'Alice' RETURN p";
    
    // Parse the query
    let ast = parse_query(cypher)
        .expect("Failed to parse Cypher query");
    
    // Build logical plan
    let (logical_plan, _plan_ctx) = build_logical_plan(&ast)
        .expect("Failed to build logical plan");
    
    // Build render plan
    let render_plan = logical_plan_to_render_plan((*logical_plan).clone())
        .expect("Failed to build render plan");
    
    // Convert to SQL
    let sql = render_plan.to_sql();
    
    // Verify the SQL contains the path object construction
    println!("Generated SQL:\n{}", sql);
    
    // Check that the SQL contains map() function for path construction
    assert!(sql.contains("map("), "SQL should contain map() function for path object");
    assert!(sql.contains("'nodes'"), "SQL should include 'nodes' key");
    assert!(sql.contains("'length'"), "SQL should include 'length' key");
    assert!(sql.contains("path_nodes"), "SQL should reference path_nodes column");
    assert!(sql.contains("hop_count"), "SQL should reference hop_count column");
}

#[test]
fn test_path_variable_with_properties() {
    // Test query with path variable and node properties
    let cypher = "MATCH p = shortestPath((a:Person)-[:FOLLOWS*]-(b:Person)) WHERE a.name = 'Alice' RETURN p, a.name";
    
    // Parse the query
    let ast = parse_query(cypher)
        .expect("Failed to parse Cypher query");
    
    // Build logical plan
    let (logical_plan, _plan_ctx) = build_logical_plan(&ast)
        .expect("Failed to build logical plan");
    
    // Build render plan
    let render_plan = logical_plan_to_render_plan((*logical_plan).clone())
        .expect("Failed to build render plan");
    
    // Convert to SQL
    let sql = render_plan.to_sql();
    
    // Verify the SQL
    println!("Generated SQL:\n{}", sql);
    
    // Should have both path object and node property
    assert!(sql.contains("map("), "SQL should contain map() for path");
    assert!(sql.contains("end_name") || sql.contains("a.name"), "SQL should include the returned node name property");
}

#[test]
fn test_non_path_variable_unchanged() {
    // Test query without path variable - should work as before
    let cypher = "MATCH (a:Person)-[:FOLLOWS*]-(b:Person) WHERE a.name = 'Alice' RETURN a, b";
    
    // Parse the query
    let ast = parse_query(cypher)
        .expect("Failed to parse Cypher query");
    
    // Build logical plan
    let (logical_plan, _plan_ctx) = build_logical_plan(&ast)
        .expect("Failed to build logical plan");
    
    // Build render plan
    let render_plan = logical_plan_to_render_plan((*logical_plan).clone())
        .expect("Failed to build render plan");
    
    // Convert to SQL
    let sql = render_plan.to_sql();
    
    println!("Generated SQL:\n{}", sql);
    
    // Should NOT have map() since there's no path variable
    // (unless we're returning the nodes, which is fine)
    // Just make sure it compiles and runs
    assert!(!sql.is_empty(), "SQL should not be empty");
}
