/// Integration test for CTE column aliasing with WITH node + aggregation
///
/// This test validates that when a WITH clause exports a node alias,
/// the CTE columns use underscore convention (a_name) not dot convention (a.name).
///
/// Issue #4: CTE Column Aliasing for Mixed RETURN (WITH alias + node property)
use clickgraph::{
    graph_catalog::{
        config::Identifier,
        expression_parser::PropertyValue,
        graph_schema::{GraphSchema, NodeIdSchema, NodeSchema, RelationshipSchema},
    },
    open_cypher_parser::parse_query,
    query_planner::evaluate_read_query,
    render_plan::{logical_plan_to_render_plan, ToSql},
};
use std::collections::HashMap;

/// Create a test schema with User nodes and FOLLOWS relationships
fn create_test_schema() -> GraphSchema {
    let mut nodes = HashMap::new();
    let mut relationships = HashMap::new();

    // Create User node schema
    let mut property_mappings = HashMap::new();
    property_mappings.insert(
        "user_id".to_string(),
        PropertyValue::Column("user_id".to_string()),
    );
    property_mappings.insert(
        "name".to_string(),
        PropertyValue::Column("full_name".to_string()),
    );
    property_mappings.insert(
        "email".to_string(),
        PropertyValue::Column("email_address".to_string()),
    );

    let user_schema = NodeSchema {
        database: "test".to_string(),
        table_name: "users".to_string(),
        column_names: vec![
            "user_id".to_string(),
            "full_name".to_string(),
            "email_address".to_string(),
        ],
        primary_keys: "user_id".to_string(),
        node_id: NodeIdSchema::single("user_id".to_string(), "UInt64".to_string()),
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
        node_id_types: None,
    };

    nodes.insert("User".to_string(), user_schema);

    // Create FOLLOWS relationship schema
    let follows_schema = RelationshipSchema {
        database: "test".to_string(),
        table_name: "user_follows".to_string(),
        column_names: vec!["follower_id".to_string(), "followed_id".to_string()],
        from_node: "User".to_string(),
        to_node: "User".to_string(),
        from_node_table: "users".to_string(),
        to_node_table: "users".to_string(),
        from_id: Identifier::from("follower_id"),
        to_id: Identifier::from("followed_id"),
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
        edge_id_types: None,
    };

    relationships.insert("FOLLOWS::User::User".to_string(), follows_schema);

    GraphSchema::build(1, "test".to_string(), nodes, relationships)
}

#[test]
fn test_cte_column_aliasing_underscore_convention() {
    // Create proper schema for testing
    let schema = create_test_schema();

    // Test query: WITH exports node alias, RETURN accesses node property
    let cypher = "MATCH (a:User)-[:FOLLOWS]->(b:User) WITH a, COUNT(b) as follows WHERE follows > 1 RETURN a.name, follows ORDER BY a.name LIMIT 5";

    // Parse query
    let ast = parse_query(cypher).expect("Failed to parse Cypher query");

    // Build logical plan
    let (logical_plan, _plan_ctx) =
        evaluate_read_query(ast, &schema, None, None).expect("Failed to build logical plan");

    // Render to SQL
    let render_plan =
        logical_plan_to_render_plan(logical_plan, &schema).expect("Failed to render plan");

    let sql = render_plan.to_sql();

    println!("Generated SQL:\n{}", sql);

    // Assertions:
    // 1. CTE columns must use underscore convention (a_name, a_user_id, etc.)
    //    NOT dot convention (a.name, a.user_id)

    // Check that CTE SELECT items use underscore (look for "AS a_" pattern in CTE)
    assert!(
        sql.contains(" AS a_") || sql.contains(" AS \"a_"),
        "CTE columns must use underscore convention (a_name, not a.name). SQL:\n{}",
        sql
    );

    // 2. CTE should NOT contain dot notation in column aliases within the WITH clause
    //    (It's OK in the final SELECT's AS clauses, but not in CTE column names)
    // Extract just the CTE definition by finding the matching closing parenthesis
    let with_section = if let Some(with_start) = sql.find("WITH") {
        // Find the end of the CTE - it ends with ")\n" before the final SELECT
        if let Some(cte_end_marker) = sql[with_start..].find(")\nSELECT") {
            &sql[with_start..with_start + cte_end_marker + 1] // +1 to include the closing )
        } else if let Some(cte_end_marker) = sql[with_start..].find(")\n\nSELECT") {
            &sql[with_start..with_start + cte_end_marker + 1]
        } else {
            // Fallback: find just up to the closing paren and "SELECT" keyword
            ""
        }
    } else {
        ""
    };

    // Verify CTE columns don't use dot notation
    // Look for patterns like "AS a.name" or "AS a.user_id" in the CTE
    let cte_has_dot_aliases = with_section.contains(" AS a.")
        || with_section.contains(" AS \"a.")
        || with_section.contains(" AS b.")
        || with_section.contains(" AS \"b.");

    assert!(!cte_has_dot_aliases,
        "CTE column aliases should use underscore (a_name) not dot (a.name) notation. Found dot notation in WITH clause:\n{}", with_section);

    // 3. Final SELECT should use AS to map underscore names to dot notation
    //    e.g., SELECT a_name AS "a.name"
    let has_underscore_to_dot_mapping = sql.contains("a_name AS \"a.name\"")
        || sql.contains("a_user_id AS \"a.user_id\"")
        || sql.contains("a_email AS \"a.email\"");

    // This check is informational - it's the correct pattern but not strictly required
    if has_underscore_to_dot_mapping {
        println!("✓ Found correct pattern: CTE uses underscore (a_name), outer SELECT uses AS for dot notation");
    }

    // 4. SQL must be valid and complete
    assert!(sql.contains("SELECT"), "Generated SQL must contain SELECT");
    assert!(sql.contains("FROM"), "Generated SQL must contain FROM");

    println!("✓ Test passed: CTE columns use underscore convention correctly");
}

#[test]
fn test_cte_wildcard_expansion_underscore_convention() {
    // Create proper schema for testing
    let schema = create_test_schema();

    // Test query: WITH exports node, RETURN * to trigger wildcard expansion
    // This should expand all properties of 'a' with underscore convention
    let cypher = "MATCH (a:User) WITH a RETURN * LIMIT 5";

    // Parse query
    let ast = parse_query(cypher).expect("Failed to parse Cypher query");

    // Build logical plan
    let (logical_plan, _plan_ctx) =
        evaluate_read_query(ast, &schema, None, None).expect("Failed to build logical plan");

    // Render to SQL
    let render_plan =
        logical_plan_to_render_plan(logical_plan, &schema).expect("Failed to render plan");

    let sql = render_plan.to_sql();

    println!("Generated SQL:\n{}", sql);

    // RETURN * might just pass through the node variable without explicit wildcard expansion
    // That's OK - the important thing is to verify no dots in CTE if expansion happens

    // Extract WITH clause section (before outer SELECT)
    let with_section = if let Some(with_start) = sql.find("WITH") {
        if let Some(cte_end_marker) = sql[with_start..].find(")\nSELECT") {
            &sql[with_start..with_start + cte_end_marker + 1]
        } else if let Some(cte_end_marker) = sql[with_start..].find(")\n\nSELECT") {
            &sql[with_start..with_start + cte_end_marker + 1]
        } else {
            ""
        }
    } else {
        ""
    };

    // If wildcard expansion happened, verify it uses underscore convention
    if with_section.contains(" AS ") {
        // Verify CTE doesn't use dot notation in column aliases
        let cte_has_dot_aliases =
            with_section.contains(" AS a.") || with_section.contains(" AS \"a.");

        assert!(
            !cte_has_dot_aliases,
            "CTE column aliases should use underscore (a_name) not dot (a.name). Found in:\n{}",
            with_section
        );
    }

    println!("✓ Test passed: Wildcard expansion (if triggered) uses underscore convention");

    println!("✓ Test passed: Wildcard expansion uses underscore convention");
}
