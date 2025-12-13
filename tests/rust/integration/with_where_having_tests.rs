/// Integration test for WITH + aggregation + WHERE (HAVING clause generation)
/// 
/// This test validates that when a WITH clause contains:
/// 1. Aggregation (causes GROUP BY generation)
/// 2. WHERE clause after WITH
/// 
/// The WHERE clause correctly becomes a HAVING clause in the generated SQL.

use clickgraph::{
    graph_catalog::graph_schema::GraphSchema,
    open_cypher_parser::parse_query,
    query_planner::logical_plan::plan_builder::build_logical_plan,
    render_plan::{logical_plan_to_render_plan, ToSql},
};
use std::collections::HashMap;

#[test]
fn test_with_aggregation_where_generates_having() {
    // Create empty schema (test doesn't need actual schema)
    let schema = GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new());

    // Simpler test query: just aggregation with WHERE
    let cypher = "MATCH (a) WITH a.name, COUNT(*) as cnt WHERE cnt > 2 RETURN a.name, cnt";

    // Parse query
    let ast = parse_query(cypher)
        .expect("Failed to parse Cypher query");

    // Build logical plan
    let (logical_plan, _plan_ctx) =
        build_logical_plan(&ast, &schema, None, None)
        .expect("Failed to build logical plan");

    // Render to SQL
    let render_plan = logical_plan_to_render_plan((*logical_plan).clone(), &schema)
        .expect("Failed to render plan");

    let sql = render_plan.to_sql();

    println!("Generated SQL:\n{}", sql);

    // Assertions:
    // 1. SQL must contain "HAVING" keyword
    assert!(sql.contains("HAVING"), 
        "Generated SQL must contain HAVING clause when WITH has WHERE after aggregation. SQL:\n{}", sql);

    // 2. SQL must contain the condition "cnt > 2" in HAVING context
    assert!(sql.to_uppercase().contains("CNT") && sql.contains("> 2"),
        "HAVING clause must contain 'cnt > 2' condition. SQL:\n{}", sql);

    // 3. SQL must contain GROUP BY (aggregation should generate grouping)
    assert!(sql.contains("GROUP BY"),
        "Generated SQL must contain GROUP BY when aggregation is used. SQL:\n{}", sql);

    // 4. The HAVING should come AFTER GROUP BY in the SQL (standard SQL order)
    let group_by_pos = sql.find("GROUP BY").expect("GROUP BY must exist");
    let having_pos = sql.find("HAVING").expect("HAVING must exist");
    assert!(having_pos > group_by_pos,
        "HAVING must come after GROUP BY in generated SQL. SQL:\n{}", sql);

    println!("✓ Test passed: WITH + aggregation + WHERE correctly generates HAVING clause");
}

#[test]
fn test_with_where_without_aggregation() {
    // Create empty schema
    let schema = GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new());

    // Test query: WITH without aggregation, WHERE should remain WHERE
    let cypher = "MATCH (a:User) WITH a WHERE a.user_id > 100 RETURN a.name";

    // Parse query
    let ast = parse_query(cypher)
        .expect("Failed to parse Cypher query");

    // Build logical plan
    let (logical_plan, _plan_ctx) =
        build_logical_plan(&ast, &schema, None, None)
        .expect("Failed to build logical plan");

    // Render to SQL
    let render_plan = logical_plan_to_render_plan((*logical_plan).clone(), &schema)
        .expect("Failed to render plan");

    let sql = render_plan.to_sql();

    println!("Generated SQL (no aggregation):\n{}", sql);

    // Assertions:
    // 1. SQL must contain WHERE clause (not HAVING since no GROUP BY)
    assert!(sql.contains("WHERE"),
        "Generated SQL must contain WHERE clause when no aggregation. SQL:\n{}", sql);

    // 2. SQL must NOT contain HAVING (no aggregation, so WHERE stays WHERE)
    assert!(!sql.contains("HAVING"),
        "Generated SQL must NOT contain HAVING when there's no aggregation. SQL:\n{}", sql);

    // 3. SQL must NOT contain GROUP BY (no aggregation)
    assert!(!sql.contains("GROUP BY"),
        "Generated SQL must NOT contain GROUP BY when no aggregation. SQL:\n{}", sql);

    println!("✓ Test passed: WITH + WHERE (no aggregation) correctly generates WHERE clause");
}

#[test]
fn test_with_aggregation_multiple_conditions() {
    // Create empty schema
    let schema = GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new());

    // Test query: Multiple conditions in WHERE after aggregation
    let cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a, COUNT(b) as cnt WHERE cnt > 2 AND cnt < 100 RETURN a.name, cnt";

    // Parse query
    let ast = parse_query(cypher)
        .expect("Failed to parse Cypher query");

    // Build logical plan
    let (logical_plan, _plan_ctx) =
        build_logical_plan(&ast, &schema, None, None)
        .expect("Failed to build logical plan");

    // Render to SQL
    let render_plan = logical_plan_to_render_plan((*logical_plan).clone(), &schema)
        .expect("Failed to render plan");

    let sql = render_plan.to_sql();

    println!("Generated SQL (multiple conditions):\n{}", sql);

    // Assertions:
    // 1. SQL must contain HAVING
    assert!(sql.contains("HAVING"),
        "Generated SQL must contain HAVING clause. SQL:\n{}", sql);

    // 2. SQL must contain both conditions
    assert!(sql.contains("> 2") && sql.contains("< 100"),
        "HAVING clause must contain both conditions. SQL:\n{}", sql);

    // 3. SQL must contain AND operator
    assert!(sql.to_uppercase().contains("AND"),
        "HAVING clause must contain AND operator for multiple conditions. SQL:\n{}", sql);

    println!("✓ Test passed: Multiple conditions in WITH WHERE correctly generate complex HAVING clause");
}
