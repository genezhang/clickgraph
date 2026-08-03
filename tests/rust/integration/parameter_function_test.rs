use clickgraph::{
    graph_catalog::{
        expression_parser::PropertyValue,
        graph_schema::{GraphSchema, NodeIdSchema, NodeSchema},
        schema_types::SchemaType,
    },
    open_cypher_parser::parse_query,
    query_planner::{evaluate_read_query, logical_plan::plan_builder::build_logical_plan},
    render_plan::{logical_plan_to_render_plan_with_ctx, ToSql},
};
use std::collections::HashMap;

// NOTE: Standalone RETURN queries (without MATCH) require HTTP API handling
// to add dummy FROM clause. These tests focus on queries with MATCH patterns.

fn create_test_schema() -> GraphSchema {
    let mut nodes = HashMap::new();

    // Create User node
    nodes.insert(
        "User".to_string(),
        NodeSchema {
            database: "test".to_string(),
            table_name: "users".to_string(),
            column_names: vec!["id".to_string(), "name".to_string(), "age".to_string()],
            primary_keys: "id".to_string(),
            node_id: NodeIdSchema::single("id".to_string(), SchemaType::Integer),
            property_mappings: {
                let mut props = HashMap::new();
                props.insert("id".to_string(), PropertyValue::Column("id".to_string()));
                props.insert(
                    "name".to_string(),
                    PropertyValue::Column("name".to_string()),
                );
                props.insert("age".to_string(), PropertyValue::Column("age".to_string()));
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
            node_id_types: None,
            source: None,
            property_types: HashMap::new(),
            id_generation: None,
        },
    );

    // Create Order node
    nodes.insert(
        "Order".to_string(),
        NodeSchema {
            database: "test".to_string(),
            table_name: "orders".to_string(),
            column_names: vec!["id".to_string(), "total".to_string()],
            primary_keys: "id".to_string(),
            node_id: NodeIdSchema::single("id".to_string(), SchemaType::Integer),
            property_mappings: {
                let mut props = HashMap::new();
                props.insert("id".to_string(), PropertyValue::Column("id".to_string()));
                props.insert(
                    "total".to_string(),
                    PropertyValue::Column("total".to_string()),
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
            node_id_types: None,
            source: None,
            property_types: HashMap::new(),
            id_generation: None,
        },
    );

    // Create Product node
    nodes.insert(
        "Product".to_string(),
        NodeSchema {
            database: "test".to_string(),
            table_name: "products".to_string(),
            column_names: vec!["id".to_string(), "name".to_string(), "price".to_string()],
            primary_keys: "id".to_string(),
            node_id: NodeIdSchema::single("id".to_string(), SchemaType::Integer),
            property_mappings: {
                let mut props = HashMap::new();
                props.insert("id".to_string(), PropertyValue::Column("id".to_string()));
                props.insert(
                    "name".to_string(),
                    PropertyValue::Column("name".to_string()),
                );
                props.insert(
                    "price".to_string(),
                    PropertyValue::Column("price".to_string()),
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
            node_id_types: None,
            source: None,
            property_types: HashMap::new(),
            id_generation: None,
        },
    );

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
            node_id_types: None,
            source: None,
            property_types: HashMap::new(),
            id_generation: None,
        },
    );

    // Create Number node
    nodes.insert(
        "Number".to_string(),
        NodeSchema {
            database: "test".to_string(),
            table_name: "numbers".to_string(),
            column_names: vec!["id".to_string(), "value".to_string()],
            primary_keys: "id".to_string(),
            node_id: NodeIdSchema::single("id".to_string(), SchemaType::Integer),
            property_mappings: {
                let mut props = HashMap::new();
                props.insert("id".to_string(), PropertyValue::Column("id".to_string()));
                props.insert(
                    "value".to_string(),
                    PropertyValue::Column("value".to_string()),
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
            node_id_types: None,
            source: None,
            property_types: HashMap::new(),
            id_generation: None,
        },
    );

    // Create Text node
    nodes.insert(
        "Text".to_string(),
        NodeSchema {
            database: "test".to_string(),
            table_name: "texts".to_string(),
            column_names: vec!["id".to_string(), "content".to_string()],
            primary_keys: "id".to_string(),
            node_id: NodeIdSchema::single("id".to_string(), SchemaType::Integer),
            property_mappings: {
                let mut props = HashMap::new();
                props.insert("id".to_string(), PropertyValue::Column("id".to_string()));
                props.insert(
                    "content".to_string(),
                    PropertyValue::Column("content".to_string()),
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
            node_id_types: None,
            source: None,
            property_types: HashMap::new(),
            id_generation: None,
        },
    );

    GraphSchema::build(1, "test".to_string(), nodes, HashMap::new())
}

#[test]
fn test_parameter_in_where_with_function_in_return() {
    // Test: Parameter in WHERE, function in RETURN
    let query = "MATCH (n:User) WHERE n.age > $minAge RETURN toUpper(n.name) AS upper_name";
    let ast = parse_query(query).expect("Failed to parse query");

    let schema = create_test_schema();

    let (logical_plan, plan_ctx) =
        build_logical_plan(&ast, &schema, None, None, None).expect("Failed to plan query");
    let render_plan =
        logical_plan_to_render_plan_with_ctx((*logical_plan).clone(), &schema, Some(&plan_ctx))
            .expect("Failed to render SQL");
    let sql = render_plan.to_sql();

    println!("Generated SQL:\n{}", sql);

    // Verify both parameter and function present
    assert!(
        sql.to_lowercase().contains("upperutf8(")
            || sql.to_lowercase().contains("upper(")
            || sql.to_lowercase().contains("ucase(")
    );
}

#[test]
fn test_function_with_parameter_in_where() {
    // Test: Function with parameter in WHERE clause
    let query = "MATCH (n:User) WHERE toUpper(n.status) = $status RETURN n.name";
    let ast = parse_query(query).expect("Failed to parse query");

    let schema = create_test_schema();

    let (logical_plan, plan_ctx) =
        build_logical_plan(&ast, &schema, None, None, None).expect("Failed to plan query");
    let render_plan =
        logical_plan_to_render_plan_with_ctx((*logical_plan).clone(), &schema, Some(&plan_ctx))
            .expect("Failed to render SQL");
    let sql = render_plan.to_sql();

    println!("Generated SQL:\n{}", sql);

    // Verify function and parameter in WHERE
    assert!(
        sql.to_lowercase().contains("upperutf8(")
            || sql.to_lowercase().contains("upper(")
            || sql.to_lowercase().contains("ucase(")
    );
}

#[test]
fn test_multiple_parameters_with_multiple_functions() {
    // Test: Multiple parameters and functions
    let query = "MATCH (n:Product) WHERE n.price >= $minPrice AND n.price <= $maxPrice RETURN toUpper(n.name), ceil(n.price) AS rounded_price";
    let ast = parse_query(query).expect("Failed to parse query");

    let schema = create_test_schema();

    let (logical_plan, plan_ctx) =
        build_logical_plan(&ast, &schema, None, None, None).expect("Failed to plan query");
    let render_plan =
        logical_plan_to_render_plan_with_ctx((*logical_plan).clone(), &schema, Some(&plan_ctx))
            .expect("Failed to render SQL");
    let sql = render_plan.to_sql();

    println!("Generated SQL:\n{}", sql);

    // Verify multiple functions
    assert!(
        sql.to_lowercase().contains("upperutf8(")
            || sql.to_lowercase().contains("upper(")
            || sql.to_lowercase().contains("ucase(")
    );
    assert!(sql.to_lowercase().contains("ceil("));
}

#[test]
fn test_math_function_in_where_with_parameter() {
    // Test: Math function in WHERE with parameter
    let query = "MATCH (n:Number) WHERE abs(n.value) > $threshold RETURN n.value";
    let ast = parse_query(query).expect("Failed to parse query");

    let schema = create_test_schema();

    let (logical_plan, plan_ctx) =
        build_logical_plan(&ast, &schema, None, None, None).expect("Failed to plan query");
    let render_plan =
        logical_plan_to_render_plan_with_ctx((*logical_plan).clone(), &schema, Some(&plan_ctx))
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

    let schema = create_test_schema();

    let (logical_plan, plan_ctx) =
        build_logical_plan(&ast, &schema, None, None, None).expect("Failed to plan query");
    let render_plan =
        logical_plan_to_render_plan_with_ctx((*logical_plan).clone(), &schema, Some(&plan_ctx))
            .expect("Failed to render SQL");
    let sql = render_plan.to_sql();

    println!("Generated SQL:\n{}", sql);

    // Verify substring function
    assert!(
        sql.to_lowercase().contains("substringutf8(")
            || sql.to_lowercase().contains("substring(")
            || sql.to_lowercase().contains("substr(")
    );
}

#[tokio::test]
async fn test_aggregation_function_with_parameter_filter() {
    // Test: Aggregation with parameter in WHERE
    let query = "MATCH (n:Order) WHERE n.total > $minTotal RETURN count(n) AS order_count, sum(n.total) AS total_sum";
    let ast = parse_query(query).expect("Failed to parse query");

    let schema = create_test_schema();

    // Use full pipeline so projection_tagging resolves count(n) → count(n.id)
    let (logical_plan, plan_ctx) =
        evaluate_read_query(ast, &schema, None, None).expect("Failed to plan query");
    let render_plan = logical_plan_to_render_plan_with_ctx(logical_plan, &schema, Some(&plan_ctx))
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

    let schema = create_test_schema();

    let (logical_plan, plan_ctx) =
        build_logical_plan(&ast, &schema, None, None, None).expect("Failed to plan query");
    let render_plan =
        logical_plan_to_render_plan_with_ctx((*logical_plan).clone(), &schema, Some(&plan_ctx))
            .expect("Failed to render SQL");
    let sql = render_plan.to_sql();

    println!("Generated SQL:\n{}", sql);

    // Verify nested functions (both should be present)
    assert!(
        sql.to_lowercase().contains("upperutf8(")
            || sql.to_lowercase().contains("upper(")
            || sql.to_lowercase().contains("ucase(")
    );
    assert!(
        sql.to_lowercase().contains("substringutf8(")
            || sql.to_lowercase().contains("substring(")
            || sql.to_lowercase().contains("substr(")
    );
}

#[test]
fn test_case_expression_with_parameters() {
    // Test: CASE expression with parameters
    let query = "MATCH (n:Product) RETURN CASE WHEN n.price > $threshold THEN 'expensive' ELSE 'affordable' END AS category";
    let ast = parse_query(query).expect("Failed to parse query");

    let schema = create_test_schema();

    let (logical_plan, plan_ctx) =
        build_logical_plan(&ast, &schema, None, None, None).expect("Failed to plan query");
    let render_plan =
        logical_plan_to_render_plan_with_ctx((*logical_plan).clone(), &schema, Some(&plan_ctx))
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

    let schema = create_test_schema();

    let (logical_plan, plan_ctx) =
        build_logical_plan(&ast, &schema, None, None, None).expect("Failed to plan query");
    let render_plan =
        logical_plan_to_render_plan_with_ctx((*logical_plan).clone(), &schema, Some(&plan_ctx))
            .expect("Failed to render SQL");
    let sql = render_plan.to_sql();

    println!("Generated SQL:\n{}", sql);

    // Verify function applied to parameter
    assert!(
        sql.to_lowercase().contains("upperutf8(")
            || sql.to_lowercase().contains("upper(")
            || sql.to_lowercase().contains("ucase(")
    );
}

#[test]
fn test_list_comprehension_in_return_lowers_not_panics() {
    // #866: a scalar list comprehension in RETURN now lowers to nested
    // arrayMap/arrayFilter (previously it reached ProjectionItem conversion with
    // no rewrite pass having lowered it — first PANICKING the worker thread on
    // otherwise-valid Cypher, then #612 downgraded it to a clean planning Err,
    // and #866 finally supports it). It must plan successfully AND never panic.
    let query = "MATCH (n:User) RETURN [x IN [1, 2, 3, 4] WHERE x > 2] AS r";
    let ast = parse_query(query).expect("Failed to parse query");
    let schema = create_test_schema();

    let (logical_plan, plan_ctx) =
        build_logical_plan(&ast, &schema, None, None, None).expect("should plan, not error/panic");
    let render_plan =
        logical_plan_to_render_plan_with_ctx((*logical_plan).clone(), &schema, Some(&plan_ctx))
            .expect("Failed to render SQL");
    let sql = render_plan.to_sql();

    // Lowered to CH arrayFilter(x -> x > 2, [1,2,3,4]) — no lingering raw
    // comprehension, and the lambda-bound `x` stays local (not a graph alias).
    assert!(
        sql.contains("arrayFilter(x -> x > 2"),
        "expected arrayFilter lowering, got:\n{sql}"
    );
}

#[test]
fn test_list_comprehension_with_hidden_pattern_errors_not_panics() {
    // #866 guard: a graph pattern hidden in a CASE inside the comprehension
    // WHERE has no scalar per-element lowering. It must surface as a clean
    // planning Err (ListComprehensionNotRewritten), NOT lower into a lambda body
    // that renders a pattern in expression context — a pre-existing
    // `unimplemented!` PANIC at render_expr.rs. Locks the no-panic behavior at
    // the full-planner level (the classifier guard recurses into CASE/exists/…).
    let query = "MATCH (n:User) WITH collect(n) AS ns \
                 RETURN [p IN ns WHERE CASE WHEN (p)-[:FOLLOWS]->() THEN true ELSE false END] AS r";
    let ast = parse_query(query).expect("Failed to parse query");
    let schema = create_test_schema();

    let result = build_logical_plan(&ast, &schema, None, None, None);
    assert!(
        result.is_err(),
        "a pattern hidden in a list-comprehension CASE should error, not panic"
    );
}

#[test]
fn test_relationship_pattern_in_case_return_errors_not_panics_901() {
    // #901: a relationship pattern in scalar-expression context inside a CASE
    // (`RETURN CASE WHEN (u)-[:FOLLOWS]->() THEN 1 ELSE 0 END`) has no scalar
    // lowering. It plans fine but reaches `RenderExpr::try_from(LogicalExpr)`,
    // where a catch-all `unimplemented!` used to PANIC the tokio worker on
    // otherwise-valid Cypher (taking down the server). It must now surface as a
    // clean render Err (UnsupportedFeature), never a panic.
    let query = "MATCH (u:User) RETURN CASE WHEN (u)-[:FOLLOWS]->() THEN 1 ELSE 0 END AS c";
    let ast = parse_query(query).expect("Failed to parse query");
    let schema = create_test_schema();

    let (logical_plan, plan_ctx) = build_logical_plan(&ast, &schema, None, None, None)
        .expect("should plan (error is at render)");
    let result =
        logical_plan_to_render_plan_with_ctx((*logical_plan).clone(), &schema, Some(&plan_ctx));
    assert!(
        result.is_err(),
        "a relationship pattern in a CASE expression must render-error, not panic"
    );
    let err = format!("{:?}", result.unwrap_err());
    assert!(
        err.contains("UnsupportedFeature"),
        "expected a clean UnsupportedFeature error, got: {err}"
    );
}

#[test]
fn test_relationship_pattern_in_where_case_errors_not_panics_901() {
    // #901 (WHERE form): same panic path via a CASE predicate in WHERE.
    let query = "MATCH (u:User) WHERE CASE WHEN (u)-[:FOLLOWS]->() THEN true ELSE false END \
                 RETURN u.user_id";
    let ast = parse_query(query).expect("Failed to parse query");
    let schema = create_test_schema();

    let (logical_plan, plan_ctx) = build_logical_plan(&ast, &schema, None, None, None)
        .expect("should plan (error is at render)");
    let result =
        logical_plan_to_render_plan_with_ctx((*logical_plan).clone(), &schema, Some(&plan_ctx));
    assert!(
        result.is_err(),
        "a relationship pattern in a WHERE CASE must render-error, not panic"
    );
}
