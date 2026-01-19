/// Integration test for WITH + aggregation + WHERE (HAVING clause generation)
///
/// This test validates that when a WITH clause contains:
/// 1. Aggregation (causes GROUP BY generation)
/// 2. WHERE clause after WITH
///
/// The WHERE clause correctly becomes a HAVING clause in the generated SQL.
use clickgraph::{
    graph_catalog::{
        expression_parser::PropertyValue,
        graph_schema::{GraphSchema, NodeIdSchema, NodeSchema},
    },
    open_cypher_parser::parse_query,
    query_planner::evaluate_read_query,
    render_plan::{logical_plan_to_render_plan, ToSql},
};
use std::collections::HashMap;

/// Create a minimal test schema with a generic node type
fn create_test_schema() -> GraphSchema {
    let mut nodes = HashMap::new();

    // Create a minimal node schema with required fields
    let mut property_mappings = HashMap::new();
    property_mappings.insert("id".to_string(), PropertyValue::Column("id".to_string()));

    let node_schema = NodeSchema {
        database: "test".to_string(),
        table_name: "nodes".to_string(),
        column_names: vec!["id".to_string()],
        primary_keys: "id".to_string(),
        node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
        property_mappings,
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
    };

    nodes.insert("Node".to_string(), node_schema);

    GraphSchema::build(1, "test".to_string(), nodes, HashMap::new())
}

#[test]
fn test_with_aggregation_where_generates_having() {
    // Create proper schema for testing
    let schema = create_test_schema();

    // Test query: aggregation with WHERE
    let cypher = "MATCH (a) WITH a, COUNT(*) as cnt WHERE cnt > 2 RETURN a, cnt";

    // Parse query
    let ast = parse_query(cypher).expect("Failed to parse Cypher query");

    // Build logical plan
    let logical_plan =
        evaluate_read_query(ast, &schema, None, None).expect("Failed to build logical plan");

    // Render to SQL
    let render_plan =
        logical_plan_to_render_plan(logical_plan, &schema).expect("Failed to render plan");

    let sql = render_plan.to_sql();

    println!("Generated SQL:\n{}", sql);

    // Assertions:
    // 1. SQL must contain "HAVING" keyword
    assert!(
        sql.contains("HAVING"),
        "Generated SQL must contain HAVING clause when WITH has WHERE after aggregation. SQL:\n{}",
        sql
    );

    // 2. SQL must contain the condition "cnt > 2" in HAVING context
    assert!(
        sql.contains("> 2"),
        "HAVING clause must contain 'cnt > 2' condition. SQL:\n{}",
        sql
    );

    // 3. SQL must contain GROUP BY (aggregation should generate grouping)
    assert!(
        sql.contains("GROUP BY"),
        "Generated SQL must contain GROUP BY when aggregation is used. SQL:\n{}",
        sql
    );

    // 4. The HAVING should come AFTER GROUP BY in the SQL (standard SQL order)
    let group_by_pos = sql.find("GROUP BY").expect("GROUP BY must exist");
    let having_pos = sql.find("HAVING").expect("HAVING must exist");
    assert!(
        having_pos > group_by_pos,
        "HAVING must come after GROUP BY in generated SQL. SQL:\n{}",
        sql
    );

    println!("✓ Test passed: WITH + aggregation + WHERE correctly generates HAVING clause");
}

#[test]
fn test_with_where_without_aggregation() {
    // Create proper schema for testing
    let schema = create_test_schema();

    // Test query: WITH without aggregation, WHERE should remain WHERE
    let cypher = "MATCH (a) WITH a WHERE 1=1 RETURN a";

    // Parse query
    let ast = parse_query(cypher).expect("Failed to parse Cypher query");

    // Build logical plan
    let logical_plan =
        evaluate_read_query(ast, &schema, None, None).expect("Failed to build logical plan");

    // Render to SQL
    let render_plan =
        logical_plan_to_render_plan(logical_plan, &schema).expect("Failed to render plan");

    let sql = render_plan.to_sql();

    println!("Generated SQL (no aggregation):\n{}", sql);

    // Assertions:
    // 1. SQL must contain WHERE clause (not HAVING since no GROUP BY)
    assert!(
        sql.contains("WHERE"),
        "Generated SQL must contain WHERE clause when no aggregation. SQL:\n{}",
        sql
    );

    // 2. SQL must NOT contain HAVING (no aggregation, so WHERE stays WHERE)
    assert!(
        !sql.contains("HAVING"),
        "Generated SQL must NOT contain HAVING when there's no aggregation. SQL:\n{}",
        sql
    );

    // 3. SQL must NOT contain GROUP BY (no aggregation)
    assert!(
        !sql.contains("GROUP BY"),
        "Generated SQL must NOT contain GROUP BY when no aggregation. SQL:\n{}",
        sql
    );

    println!("✓ Test passed: WITH + WHERE (no aggregation) correctly generates WHERE clause");
}

#[test]
fn test_with_aggregation_multiple_conditions() {
    // Create proper schema for testing
    let schema = create_test_schema();

    // Test query: Multiple conditions in WHERE after aggregation
    // Using a simpler aggregation pattern that works with our schema
    let cypher = "MATCH (a) WITH a, COUNT(*) as cnt WHERE cnt > 2 AND cnt < 100 RETURN a, cnt";

    // Parse query
    let ast = parse_query(cypher).expect("Failed to parse Cypher query");

    // Build logical plan
    let logical_plan =
        evaluate_read_query(ast, &schema, None, None).expect("Failed to build logical plan");

    // Render to SQL
    let render_plan =
        logical_plan_to_render_plan(logical_plan, &schema).expect("Failed to render plan");

    let sql = render_plan.to_sql();

    println!("Generated SQL (multiple conditions):\n{}", sql);

    // Assertions:
    // 1. SQL must contain HAVING
    assert!(
        sql.contains("HAVING"),
        "Generated SQL must contain HAVING clause. SQL:\n{}",
        sql
    );

    // 2. SQL must contain both conditions
    assert!(
        sql.contains("> 2") && sql.contains("< 100"),
        "HAVING clause must contain both conditions. SQL:\n{}",
        sql
    );

    // 3. SQL must contain AND operator
    assert!(
        sql.to_uppercase().contains("AND"),
        "HAVING clause must contain AND operator for multiple conditions. SQL:\n{}",
        sql
    );

    println!(
        "✓ Test passed: Multiple conditions in WITH WHERE correctly generate complex HAVING clause"
    );
}
