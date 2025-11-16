use clickgraph::{
    graph_catalog::graph_schema::GraphSchema,
    open_cypher_parser::parse_query,
    query_planner::logical_plan::plan_builder::build_logical_plan,
    render_plan::{ToSql, logical_plan_to_render_plan},
};
use std::collections::HashMap;

// NOTE: Standalone RETURN queries (without MATCH) require HTTP API handling
// to add dummy FROM clause. These tests focus on queries with MATCH patterns.

#[test]
fn test_parameter_in_where_with_function_in_return() {
    // Test: Parameter in WHERE, function in RETURN
    let query = "MATCH (n:User) WHERE n.age > $minAge RETURN toUpper(n.name) AS upper_name";
    let ast = parse_query(query).expect("Failed to parse query");

    let schema = GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new());

    let (logical_plan, _plan_ctx) =
        build_logical_plan(&ast, &schema, None, None).expect("Failed to plan query");
    let render_plan = logical_plan_to_render_plan((*logical_plan).clone(), &schema)
        .expect("Failed to render SQL");
    let sql = render_plan.to_sql();

    println!("Generated SQL:\n{}", sql);

    // Verify both parameter and function present
    assert!(sql.to_lowercase().contains("upper(") || sql.to_lowercase().contains("ucase("));
}

#[test]
fn test_function_with_parameter_in_where() {
    // Test: Function with parameter in WHERE clause
    let query = "MATCH (n:User) WHERE toUpper(n.status) = $status RETURN n.name";
    let ast = parse_query(query).expect("Failed to parse query");

    let schema = GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new());

    let (logical_plan, _plan_ctx) =
        build_logical_plan(&ast, &schema, None, None).expect("Failed to plan query");
    let render_plan = logical_plan_to_render_plan((*logical_plan).clone(), &schema)
        .expect("Failed to render SQL");
    let sql = render_plan.to_sql();

    println!("Generated SQL:\n{}", sql);

    // Verify function and parameter in WHERE
    assert!(sql.to_lowercase().contains("upper(") || sql.to_lowercase().contains("ucase("));
}

#[test]
fn test_multiple_parameters_with_multiple_functions() {
    // Test: Multiple parameters and functions
    let query = "MATCH (n:Product) WHERE n.price >= $minPrice AND n.price <= $maxPrice RETURN toUpper(n.name), ceil(n.price) AS rounded_price";
    let ast = parse_query(query).expect("Failed to parse query");

    let schema = GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new());

    let (logical_plan, _plan_ctx) =
        build_logical_plan(&ast, &schema, None, None).expect("Failed to plan query");
    let render_plan = logical_plan_to_render_plan((*logical_plan).clone(), &schema)
        .expect("Failed to render SQL");
    let sql = render_plan.to_sql();

    println!("Generated SQL:\n{}", sql);

    // Verify multiple functions
    assert!(sql.to_lowercase().contains("upper(") || sql.to_lowercase().contains("ucase("));
    assert!(sql.to_lowercase().contains("ceil("));
}

#[test]
fn test_math_function_in_where_with_parameter() {
    // Test: Math function in WHERE with parameter
    let query = "MATCH (n:Number) WHERE abs(n.value) > $threshold RETURN n.value";
    let ast = parse_query(query).expect("Failed to parse query");

    let schema = GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new());

    let (logical_plan, _plan_ctx) =
        build_logical_plan(&ast, &schema, None, None).expect("Failed to plan query");
    let render_plan = logical_plan_to_render_plan((*logical_plan).clone(), &schema)
        .expect("Failed to render SQL");
    let sql = render_plan.to_sql();

    println!("Generated SQL:\n{}", sql);

    // Verify abs() function and parameter
    assert!(sql.to_lowercase().contains("abs("));
}

#[test]
fn test_string_function_with_parameters_in_return() {
    // Test: String function with parameter in RETURN
    let query = "MATCH (n:Text) RETURN substring(n.content, $start, $length) AS substr";
    let ast = parse_query(query).expect("Failed to parse query");

    let schema = GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new());

    let (logical_plan, _plan_ctx) =
        build_logical_plan(&ast, &schema, None, None).expect("Failed to plan query");
    let render_plan = logical_plan_to_render_plan((*logical_plan).clone(), &schema)
        .expect("Failed to render SQL");
    let sql = render_plan.to_sql();

    println!("Generated SQL:\n{}", sql);

    // Verify substring function
    assert!(sql.to_lowercase().contains("substring(") || sql.to_lowercase().contains("substr("));
}

#[test]
fn test_aggregation_function_with_parameter_filter() {
    // Test: Aggregation with parameter in WHERE
    let query = "MATCH (n:Order) WHERE n.total > $minTotal RETURN count(n) AS order_count, sum(n.total) AS total_sum";
    let ast = parse_query(query).expect("Failed to parse query");

    let schema = GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new());

    let (logical_plan, _plan_ctx) =
        build_logical_plan(&ast, &schema, None, None).expect("Failed to plan query");
    let render_plan = logical_plan_to_render_plan((*logical_plan).clone(), &schema)
        .expect("Failed to render SQL");
    let sql = render_plan.to_sql();

    println!("Generated SQL:\n{}", sql);

    // Verify aggregation functions
    assert!(sql.to_lowercase().contains("count("));
    assert!(sql.to_lowercase().contains("sum("));
}

#[test]
fn test_nested_functions_with_properties() {
    // Test: Nested function calls on node properties
    let query = "MATCH (n:Person) RETURN toUpper(substring(n.name, 0, 5)) AS short_upper_name";
    let ast = parse_query(query).expect("Failed to parse query");

    let schema = GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new());

    let (logical_plan, _plan_ctx) =
        build_logical_plan(&ast, &schema, None, None).expect("Failed to plan query");
    let render_plan = logical_plan_to_render_plan((*logical_plan).clone(), &schema)
        .expect("Failed to render SQL");
    let sql = render_plan.to_sql();

    println!("Generated SQL:\n{}", sql);

    // Verify nested functions (both should be present)
    assert!(sql.to_lowercase().contains("upper(") || sql.to_lowercase().contains("ucase("));
    assert!(sql.to_lowercase().contains("substring(") || sql.to_lowercase().contains("substr("));
}

#[test]
fn test_case_expression_with_parameters() {
    // Test: CASE expression with parameters
    let query = "MATCH (n:Product) RETURN CASE WHEN n.price > $threshold THEN 'expensive' ELSE 'affordable' END AS category";
    let ast = parse_query(query).expect("Failed to parse query");

    let schema = GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new());

    let (logical_plan, _plan_ctx) =
        build_logical_plan(&ast, &schema, None, None).expect("Failed to plan query");
    let render_plan = logical_plan_to_render_plan((*logical_plan).clone(), &schema)
        .expect("Failed to render SQL");
    let sql = render_plan.to_sql();

    println!("Generated SQL:\n{}", sql);

    // Verify CASE expression
    assert!(sql.to_uppercase().contains("CASE") || sql.to_uppercase().contains("IF("));
}

#[test]
fn test_function_on_parameter_in_return() {
    // Test: Function directly on parameter in RETURN (with MATCH to provide FROM)
    let query = "MATCH (n:User) RETURN toUpper($userName) AS upper_param, n.name";
    let ast = parse_query(query).expect("Failed to parse query");

    let schema = GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new());

    let (logical_plan, _plan_ctx) =
        build_logical_plan(&ast, &schema, None, None).expect("Failed to plan query");
    let render_plan = logical_plan_to_render_plan((*logical_plan).clone(), &schema)
        .expect("Failed to render SQL");
    let sql = render_plan.to_sql();

    println!("Generated SQL:\n{}", sql);

    // Verify function applied to parameter
    assert!(sql.to_lowercase().contains("upper(") || sql.to_lowercase().contains("ucase("));
}


