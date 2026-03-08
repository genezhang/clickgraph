//! Regression tests for SKIP clause → OFFSET SQL generation
//!
//! Bug: SKIP parsed correctly but never emitted OFFSET in generated SQL
//! when used without LIMIT. Fixed by adding `else if` branch for SKIP-only.

use clickgraph::{
    graph_catalog::{
        config::Identifier,
        expression_parser::PropertyValue,
        graph_schema::{GraphSchema, NodeIdSchema, NodeSchema, RelationshipSchema},
        schema_types::SchemaType,
    },
    open_cypher_parser::parse_query,
    query_planner::evaluate_read_query,
    render_plan::{logical_plan_to_render_plan, ToSql},
};
use std::collections::HashMap;

fn create_test_schema() -> GraphSchema {
    let mut nodes = HashMap::new();
    let mut relationships = HashMap::new();

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

    nodes.insert(
        "User".to_string(),
        NodeSchema {
            database: "test".to_string(),
            table_name: "users".to_string(),
            column_names: vec![
                "user_id".to_string(),
                "full_name".to_string(),
                "email_address".to_string(),
            ],
            primary_keys: "user_id".to_string(),
            node_id: NodeIdSchema::single("user_id".to_string(), SchemaType::Integer),
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
            source: None,
        },
    );

    relationships.insert(
        "FOLLOWS::User::User".to_string(),
        RelationshipSchema {
            database: "test".to_string(),
            table_name: "user_follows".to_string(),
            column_names: vec!["follower_id".to_string(), "followed_id".to_string()],
            from_node: "User".to_string(),
            to_node: "User".to_string(),
            from_node_table: "users".to_string(),
            to_node_table: "users".to_string(),
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
            source: None,
        },
    );

    GraphSchema::build(1, "test".to_string(), nodes, relationships)
}

/// Helper: parse Cypher → generate SQL
fn cypher_to_sql(cypher: &str) -> String {
    let schema = create_test_schema();
    let ast = parse_query(cypher).expect("Failed to parse Cypher");
    let (logical_plan, _plan_ctx) =
        evaluate_read_query(ast, &schema, None, None).expect("Failed to build logical plan");
    let render_plan =
        logical_plan_to_render_plan(logical_plan, &schema).expect("Failed to render plan");
    render_plan.to_sql()
}

// --- SKIP only (the bug) ---

/// ClickHouse doesn't support bare OFFSET; SKIP-only emits LIMIT skip, u64::MAX
const LARGE_LIMIT: &str = "18446744073709551615";

#[test]
fn test_skip_only_emits_offset() {
    let sql = cypher_to_sql("MATCH (u:User) RETURN u.name SKIP 5");
    println!("SQL:\n{sql}");
    let lower = sql.to_lowercase();
    assert!(
        lower.contains(&format!("limit 5, {LARGE_LIMIT}").to_lowercase()),
        "SKIP 5 without LIMIT should emit LIMIT 5, <large>. SQL:\n{sql}"
    );
}

#[test]
fn test_skip_only_with_order_by() {
    let sql = cypher_to_sql("MATCH (u:User) RETURN u.name ORDER BY u.name SKIP 10");
    println!("SQL:\n{sql}");
    let lower = sql.to_lowercase();
    assert!(
        lower.contains(&format!("limit 10, {LARGE_LIMIT}").to_lowercase()),
        "SKIP 10 with ORDER BY should emit LIMIT 10, <large>. SQL:\n{sql}"
    );
    assert!(
        lower.contains("order by"),
        "Should still have ORDER BY. SQL:\n{sql}"
    );
}

// --- SKIP + LIMIT ---

#[test]
fn test_skip_with_limit() {
    let sql = cypher_to_sql("MATCH (u:User) RETURN u.name SKIP 5 LIMIT 10");
    println!("SQL:\n{sql}");
    let lower = sql.to_lowercase();
    // ClickHouse syntax: LIMIT skip, limit
    assert!(
        lower.contains("limit 5, 10") || lower.contains("limit 5,10"),
        "SKIP 5 LIMIT 10 should emit LIMIT 5, 10. SQL:\n{sql}"
    );
}

#[test]
fn test_order_by_skip_limit() {
    let sql = cypher_to_sql("MATCH (u:User) RETURN u.name ORDER BY u.name SKIP 3 LIMIT 7");
    println!("SQL:\n{sql}");
    let lower = sql.to_lowercase();
    assert!(
        lower.contains("order by"),
        "Should have ORDER BY. SQL:\n{sql}"
    );
    assert!(
        lower.contains("limit 3, 7") || lower.contains("limit 3,7"),
        "ORDER BY + SKIP 3 LIMIT 7 should emit LIMIT 3, 7. SQL:\n{sql}"
    );
}

// --- LIMIT only (should still work) ---

#[test]
fn test_limit_only() {
    let sql = cypher_to_sql("MATCH (u:User) RETURN u.name LIMIT 10");
    println!("SQL:\n{sql}");
    let lower = sql.to_lowercase();
    assert!(
        lower.contains("limit 10"),
        "LIMIT 10 should emit LIMIT 10. SQL:\n{sql}"
    );
    assert!(
        !lower.contains("offset"),
        "LIMIT-only should not emit OFFSET. SQL:\n{sql}"
    );
}

// --- SKIP with relationship traversal ---

#[test]
fn test_skip_with_relationship() {
    let sql =
        cypher_to_sql("MATCH (u:User)-[:FOLLOWS]->(f:User) RETURN u.name, f.name SKIP 2 LIMIT 5");
    println!("SQL:\n{sql}");
    let lower = sql.to_lowercase();
    assert!(
        lower.contains("limit 2, 5") || lower.contains("limit 2,5"),
        "Relationship query with SKIP+LIMIT should emit LIMIT 2, 5. SQL:\n{sql}"
    );
}

#[test]
fn test_skip_only_with_relationship() {
    let sql = cypher_to_sql("MATCH (u:User)-[:FOLLOWS]->(f:User) RETURN u.name, f.name SKIP 3");
    println!("SQL:\n{sql}");
    let lower = sql.to_lowercase();
    assert!(
        lower.contains(&format!("limit 3, {LARGE_LIMIT}").to_lowercase()),
        "Relationship query with SKIP-only should emit LIMIT 3, <large>. SQL:\n{sql}"
    );
}

// --- SKIP with WHERE ---

#[test]
fn test_skip_with_where_clause() {
    let sql = cypher_to_sql(
        "MATCH (u:User) WHERE u.user_id > 10 RETURN u.name ORDER BY u.name SKIP 2 LIMIT 5",
    );
    println!("SQL:\n{sql}");
    let lower = sql.to_lowercase();
    assert!(
        lower.contains("limit 2, 5") || lower.contains("limit 2,5"),
        "WHERE + SKIP + LIMIT should emit LIMIT 2, 5. SQL:\n{sql}"
    );
}

// --- SKIP with UNION path (undirected relationship) ---

#[test]
fn test_skip_with_undirected_relationship_union() {
    // Undirected relationship produces UNION ALL (forward + reverse directions)
    let sql = cypher_to_sql("MATCH (u:User)-[:FOLLOWS]-(f:User) RETURN u.name, f.name SKIP 4");
    println!("SQL:\n{sql}");
    let lower = sql.to_lowercase();
    assert!(
        lower.contains("union all"),
        "Undirected relationship should produce UNION ALL. SQL:\n{sql}"
    );
    // SKIP should be present in the generated SQL
    assert!(
        lower.contains("limit 4,") || lower.contains("limit 4, "),
        "UNION query with SKIP should emit LIMIT 4, <large>. SQL:\n{sql}"
    );
}

#[test]
fn test_skip_limit_with_undirected_relationship_union() {
    let sql =
        cypher_to_sql("MATCH (u:User)-[:FOLLOWS]-(f:User) RETURN u.name, f.name SKIP 2 LIMIT 10");
    println!("SQL:\n{sql}");
    let lower = sql.to_lowercase();
    assert!(
        lower.contains("union all"),
        "Undirected relationship should produce UNION ALL. SQL:\n{sql}"
    );
    assert!(
        lower.contains("limit 2, 10") || lower.contains("limit 2,10"),
        "UNION query with SKIP+LIMIT should emit LIMIT 2, 10. SQL:\n{sql}"
    );
}
