//! End-to-end snapshot tests: Cypher write clauses → ClickHouse write SQL.
//!
//! Covers the Phase 2 write pipeline:
//!   parse → plan → write_plan_builder → write_to_sql.
//!
//! Each test asserts on stable, low-level SQL fragments rather than the
//! full string, so the suite tolerates orthogonal changes (whitespace, JOIN
//! ordering inside subqueries, etc.) while catching regressions in the
//! shape of the emitted INSERT/UPDATE/DELETE.

use crate::clickhouse_query_generator::write_to_sql::write_render_to_sql;
use crate::graph_catalog::config::Identifier;
use crate::graph_catalog::expression_parser::PropertyValue;
use crate::graph_catalog::graph_schema::{
    GraphSchema, NodeIdSchema, NodeSchema, RelationshipSchema,
};
use crate::graph_catalog::schema_types::SchemaType;
use crate::open_cypher_parser;
use crate::query_planner::analyzer;
use crate::query_planner::logical_plan::plan_builder::build_logical_plan;
use crate::query_planner::optimizer;
use crate::render_plan::write_plan_builder::build_write_plan;
use std::collections::HashMap;

fn prop_col(name: &str) -> PropertyValue {
    PropertyValue::Column(name.to_string())
}

fn person_node() -> NodeSchema {
    NodeSchema {
        database: "test".to_string(),
        table_name: "person".to_string(),
        column_names: vec!["id".to_string(), "name".to_string(), "age".to_string()],
        primary_keys: "id".to_string(),
        node_id: NodeIdSchema::single("id".to_string(), SchemaType::String),
        property_mappings: [
            ("id".to_string(), prop_col("id")),
            ("name".to_string(), prop_col("name")),
            ("age".to_string(), prop_col("age")),
        ]
        .into_iter()
        .collect(),
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
    }
}

fn knows_rel() -> RelationshipSchema {
    RelationshipSchema {
        database: "test".to_string(),
        table_name: "knows".to_string(),
        column_names: vec!["from_id".to_string(), "to_id".to_string()],
        from_node: "Person".to_string(),
        to_node: "Person".to_string(),
        from_node_table: "person".to_string(),
        to_node_table: "person".to_string(),
        from_id: Identifier::from("from_id"),
        to_id: Identifier::from("to_id"),
        from_node_id_dtype: SchemaType::String,
        to_node_id_dtype: SchemaType::String,
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
        property_types: HashMap::new(),
    }
}

fn build_test_schema() -> GraphSchema {
    let mut nodes = HashMap::new();
    nodes.insert("Person".to_string(), person_node());

    let mut rels = HashMap::new();
    rels.insert("KNOWS::Person::Person".to_string(), knows_rel());

    GraphSchema::build(1, "test".to_string(), nodes, rels)
}

fn cypher_to_write_sql(cypher: &str) -> Vec<String> {
    let ast = open_cypher_parser::parse_query(cypher).expect("parse");
    let schema = build_test_schema();
    let (logical_plan, mut plan_ctx) =
        build_logical_plan(&ast, &schema, None, None, None).expect("plan");

    // Run the read-side analyzer/optimizer pipeline so the input read plan
    // (the part below the write variant) is fully resolved before we render.
    let logical_plan = analyzer::initial_analyzing(logical_plan, &mut plan_ctx, &schema).unwrap();
    let logical_plan = optimizer::initial_optimization(logical_plan, &mut plan_ctx).unwrap();
    let logical_plan =
        analyzer::intermediate_analyzing(logical_plan, &mut plan_ctx, &schema).unwrap();
    let logical_plan = optimizer::final_optimization(logical_plan, &mut plan_ctx).unwrap();
    let logical_plan = analyzer::final_analyzing(logical_plan, &mut plan_ctx, &schema).unwrap();

    let plan = std::sync::Arc::try_unwrap(logical_plan).unwrap_or_else(|arc| (*arc).clone());

    let write_plan = build_write_plan(&plan, &schema)
        .expect("write build")
        .expect("write plan present");

    write_render_to_sql(&write_plan)
}

// ---------- CREATE ----------

#[test]
fn create_single_node_emits_insert() {
    let sql = cypher_to_write_sql("CREATE (a:Person {id: 'u1', name: 'Alice', age: 30})");
    assert_eq!(sql.len(), 1);
    let stmt = &sql[0];

    assert!(
        stmt.starts_with("INSERT INTO `test`.`person`"),
        "got: {}",
        stmt
    );
    assert!(stmt.contains("`id`"), "got: {}", stmt);
    assert!(stmt.contains("`name`"), "got: {}", stmt);
    assert!(stmt.contains("`age`"), "got: {}", stmt);
    assert!(stmt.contains("'u1'"), "got: {}", stmt);
    assert!(stmt.contains("'Alice'"), "got: {}", stmt);
    assert!(stmt.contains("30"), "got: {}", stmt);
}

#[test]
fn create_multiple_standalone_nodes_emit_sequence() {
    // Two independent CREATE patterns produce two INSERTs.
    let sql = cypher_to_write_sql(
        "CREATE (a:Person {id: 'u1', name: 'Alice'}), (b:Person {id: 'u2', name: 'Bob'})",
    );
    assert_eq!(sql.len(), 2, "got: {:?}", sql);
    assert!(sql[0].contains("'u1'"));
    assert!(sql[1].contains("'u2'"));
}

#[test]
fn create_relationship_between_aliases_is_rejected_with_clear_message() {
    let ast = open_cypher_parser::parse_query(
        "MATCH (a:Person {id:'u1'}), (b:Person {id:'u2'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .expect("parse");
    let schema = build_test_schema();
    let (plan, _ctx) = build_logical_plan(&ast, &schema, None, None, None).expect("plan");
    let plan = std::sync::Arc::try_unwrap(plan).unwrap_or_else(|arc| (*arc).clone());
    let err = build_write_plan(&plan, &schema).expect_err("must error");
    let msg = format!("{}", err);
    assert!(
        msg.contains("KNOWS") && msg.contains("not supported"),
        "got `{}`",
        msg
    );
}

// ---------- DELETE ----------

#[test]
fn delete_emits_lightweight_delete_with_subquery() {
    let sql = cypher_to_write_sql("MATCH (a:Person) WHERE a.name = 'Alice' DELETE a");
    assert_eq!(sql.len(), 1);
    let stmt = &sql[0];

    assert!(
        stmt.starts_with("DELETE FROM `test`.`person`"),
        "got: {}",
        stmt
    );
    assert!(stmt.contains("WHERE `id` IN ("), "got: {}", stmt);
    // Subquery embeds the rendered read pipeline.
    assert!(stmt.contains("SELECT"), "got: {}", stmt);
    assert!(stmt.contains("'Alice'"), "got: {}", stmt);
    // No mutations_sync — Decision 0.7 explicitly avoided.
    assert!(
        !stmt.to_lowercase().contains("mutations_sync"),
        "must not emit mutations_sync, got: {}",
        stmt
    );
}

#[test]
fn detach_delete_chains_rel_deletes_before_node_delete() {
    let sql = cypher_to_write_sql("MATCH (a:Person) WHERE a.id = 'u1' DETACH DELETE a");
    // KNOWS touches Person on both sides → 2 rel-table deletes (from_id, to_id)
    // + 1 node delete = 3 statements total.
    assert!(sql.len() >= 2, "got {:?}", sql);

    // Order: rel deletes first, node delete last.
    let last = sql.last().unwrap();
    assert!(
        last.contains("`person`"),
        "node delete must come last, got order: {:?}",
        sql
    );
    assert!(
        sql[..sql.len() - 1].iter().all(|s| s.contains("`knows`")),
        "rel deletes must come first, got order: {:?}",
        sql
    );
    // None of the statements emit mutations_sync.
    for s in &sql {
        assert!(!s.to_lowercase().contains("mutations_sync"), "got: {}", s);
    }
}

// ---------- SET ----------

#[test]
fn set_property_emits_lightweight_update_with_subquery() {
    let sql = cypher_to_write_sql("MATCH (a:Person) WHERE a.id = 'u1' SET a.age = 31");
    assert_eq!(sql.len(), 1);
    let stmt = &sql[0];

    assert!(stmt.starts_with("UPDATE `test`.`person`"), "got: {}", stmt);
    assert!(stmt.contains("SET `age` = 31"), "got: {}", stmt);
    assert!(stmt.contains("WHERE `id` IN ("), "got: {}", stmt);
    assert!(stmt.contains("SELECT"), "got: {}", stmt);
    // No SETTINGS clause at query time (Decision 0.7).
    assert!(
        !stmt.to_lowercase().contains("settings"),
        "must not emit SETTINGS, got: {}",
        stmt
    );
}

#[test]
fn set_multiple_properties_on_same_alias_collapse_into_one_update() {
    let sql =
        cypher_to_write_sql("MATCH (a:Person) WHERE a.id = 'u1' SET a.age = 31, a.name = 'Bob'");
    assert_eq!(sql.len(), 1);
    let stmt = &sql[0];

    assert!(stmt.contains("SET `age` = 31"), "got: {}", stmt);
    assert!(stmt.contains("`name` = 'Bob'"), "got: {}", stmt);
}

// ---------- REMOVE ----------

#[test]
fn remove_property_emits_update_setting_null() {
    let sql = cypher_to_write_sql("MATCH (a:Person) WHERE a.id = 'u1' REMOVE a.age");
    assert_eq!(sql.len(), 1);
    let stmt = &sql[0];

    assert!(stmt.starts_with("UPDATE `test`.`person`"), "got: {}", stmt);
    assert!(stmt.contains("SET `age` = NULL"), "got: {}", stmt);
    assert!(stmt.contains("WHERE `id` IN ("), "got: {}", stmt);
}

// ---------- Parameters ----------

#[test]
fn create_with_parameter_emits_dollar_placeholder() {
    let sql = cypher_to_write_sql("CREATE (a:Person {id: $id, name: $name})");
    assert_eq!(sql.len(), 1);
    let stmt = &sql[0];
    assert!(stmt.contains("$id"), "got: {}", stmt);
    assert!(stmt.contains("$name"), "got: {}", stmt);
}

// ---------- Hygiene ----------

#[test]
fn string_literals_with_quotes_are_escaped() {
    let sql = cypher_to_write_sql("CREATE (a:Person {id: 'u1', name: \"O'Brien\"})");
    assert_eq!(sql.len(), 1);
    let stmt = &sql[0];
    // Single quote doubled per ClickHouse / SQL convention.
    assert!(stmt.contains("'O''Brien'"), "got: {}", stmt);
}
