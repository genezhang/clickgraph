//! Phase 1.2 spike: end-to-end Cypher → Databricks SQL.
//!
//! These tests are the load-bearing assertion of the whole DeltaGraph
//! refactor so far: if `current_function_mapper()` correctly reads from
//! the task-local `QueryContext` and gets propagated through every
//! rendering layer, then setting `dialect: Databricks` in the context
//! before running the existing ClickHouse pipeline produces SQL that's
//! correct for Databricks at every FunctionMapper-routed site.
//!
//! What's tested:
//! - VLP query under CH dialect emits CH spellings (`arrayConcat`,
//!   `has`, `CAST([] AS Array(...))`).
//! - The same VLP query under Databricks dialect emits Spark spellings
//!   (`concat`, `array_contains`, `CAST(array() AS ARRAY<...>)`).
//!
//! ## Phase 1.2 spike findings — what's still CH-locked
//!
//! Actual Databricks output for `MATCH (a:User)-[:FOLLOWS*1..3]->(b:User)
//! RETURN b.id` is *mostly* correct — FunctionMapper-routed sites all
//! flip cleanly. Run `print_databricks_vlp_sql` with `--nocapture` to
//! see the full output. The remaining gaps for Phase 1.3+:
//!
//! 1. **Array-literal syntax**. Generated SQL contains
//!    `[start_node.id, end_node.id]` and `concat(vp.path_nodes,
//!    [end_node.id])`. CH supports `[a, b]`; Spark requires
//!    `array(a, b)`. These literals are spelled directly in the
//!    rendering layer (not routed through `FunctionMapper`). Fix
//!    candidate: add `FunctionMapper::array_literal_open/close()` or a
//!    helper `emit_array_literal(elems)` mirroring the pattern from
//!    Phase 0.4b.
//!
//! 2. **Identifier / alias quoting**. The output uses `AS "b.id"` for
//!    aliases with dots. CH treats `"…"` as a quoted identifier; Spark
//!    treats `"…"` as a string literal and requires backticks. Need a
//!    dialect-aware `quote_alias` helper, distinct from the existing
//!    `quote_identifier` which is already CH-shaped.
//!
//! 3. **Aggregate functions via function_registry**. Functions like
//!    Cypher `collect()` are mapped via the hardcoded `clickhouse_name`
//!    field on `FunctionMapping` in
//!    `emitters/clickhouse/function_registry.rs`, not `FunctionMapper`.
//!    So `collect(n.name)` still emits `groupArray(...)` under
//!    Databricks dialect. Phase 1.3 should make the registry
//!    dialect-aware (or split into per-dialect registries).
//!
//! 4. **Type names in non-routed CASTs**. The current spike doesn't
//!    exercise these but they exist — `UInt32`, `Float64`, etc., in
//!    various rendering paths.
//!
//! What this means for the abstraction: Phase 0.1–1.1 was about routing
//! the *function name* layer. The remaining work is routing the
//! *syntactic layer* — array literals, identifier quoting, type names.
//! All three lend themselves to the same pattern (FunctionMapper-style
//! method or call-site helper) so the path is clear, not blocked.

use crate::{
    clickhouse_query_generator,
    graph_catalog::config::Identifier,
    graph_catalog::graph_schema::{GraphSchema, NodeIdSchema, NodeSchema, RelationshipSchema},
    graph_catalog::schema_types::SchemaType,
    open_cypher_parser,
    query_planner::logical_plan::plan_builder::build_logical_plan,
    render_plan::plan_builder::RenderPlanBuilder,
    server::query_context::{with_query_context, QueryContext},
    sql_generator::SqlDialect,
};
use std::collections::HashMap;

/// Render Cypher to SQL using whatever dialect is set in the active
/// `QueryContext`. Mirrors the helper in other test files but doesn't
/// hardcode ClickHouse — the dialect comes from the task-local.
fn cypher_to_sql(cypher: &str) -> String {
    let ast = open_cypher_parser::parse_query(cypher).expect("Failed to parse Cypher query");
    let graph_schema = setup_test_schema();

    let (logical_plan, mut plan_ctx) = build_logical_plan(&ast, &graph_schema, None, None, None)
        .expect("Failed to build logical plan");

    use crate::query_planner::analyzer;
    use crate::query_planner::optimizer;

    let logical_plan =
        analyzer::initial_analyzing(logical_plan, &mut plan_ctx, &graph_schema).unwrap();
    let logical_plan =
        analyzer::intermediate_analyzing(logical_plan, &mut plan_ctx, &graph_schema).unwrap();
    let logical_plan = optimizer::initial_optimization(logical_plan, &mut plan_ctx).unwrap();
    let logical_plan = optimizer::final_optimization(logical_plan, &mut plan_ctx).unwrap();

    let render_plan = logical_plan
        .to_render_plan(&graph_schema)
        .expect("Failed to build render plan");

    clickhouse_query_generator::generate_sql(render_plan, 100)
}

fn setup_test_schema() -> GraphSchema {
    let mut nodes = HashMap::new();
    let mut relationships = HashMap::new();

    let user = NodeSchema {
        database: "test_db".to_string(),
        table_name: "users".to_string(),
        column_names: vec!["id".to_string(), "name".to_string()],
        primary_keys: "id".to_string(),
        node_id: NodeIdSchema::single("id".to_string(), SchemaType::Integer),
        property_mappings: [
            (
                "id".to_string(),
                crate::graph_catalog::expression_parser::PropertyValue::Column("id".to_string()),
            ),
            (
                "name".to_string(),
                crate::graph_catalog::expression_parser::PropertyValue::Column("name".to_string()),
            ),
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
        id_generation: None,
    };
    nodes.insert("User".to_string(), user);

    let follows = RelationshipSchema {
        database: "test_db".to_string(),
        table_name: "follows".to_string(),
        column_names: vec!["from_id".to_string(), "to_id".to_string()],
        from_node: "User".to_string(),
        to_node: "User".to_string(),
        from_node_table: "users".to_string(),
        to_node_table: "users".to_string(),
        from_id: Identifier::from("from_id"),
        to_id: Identifier::from("to_id"),
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
        from_node_properties: None,
        to_node_properties: None,
        from_label_values: None,
        to_label_values: None,
        is_fk_edge: false,
        constraints: None,
        edge_id_types: None,
        source: None,
        property_types: HashMap::new(),
    };
    relationships.insert("FOLLOWS::User::User".to_string(), follows);

    GraphSchema::build(1, "test_db".to_string(), nodes, relationships)
}

/// Sanity check: default dialect (no task-local set) emits ClickHouse
/// spellings — `arrayConcat`, `has`, `CAST([] AS Array(...))`. This
/// proves the baseline still works.
#[tokio::test]
async fn vlp_under_clickhouse_dialect_emits_ch_spellings() {
    let ctx = QueryContext {
        dialect: SqlDialect::ClickHouse,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql("MATCH (a:User)-[:FOLLOWS*1..3]->(b:User) RETURN b.id")
    })
    .await;

    assert!(
        sql.contains("arrayConcat"),
        "expected CH spelling `arrayConcat` in VLP recursive CTE; got:\n{sql}"
    );
    assert!(
        sql.contains("has(vp.path_nodes"),
        "expected CH spelling `has(vp.path_nodes, ...)` for cycle detection; got:\n{sql}"
    );
    assert!(
        sql.contains("CAST([] AS Array(String))"),
        "expected CH empty-array cast in VLP CTE; got:\n{sql}"
    );
    // Spark spellings must NOT appear under CH dialect.
    assert!(!sql.contains("ARRAY<STRING>"), "CH SQL leaked Spark type");
    assert!(
        !sql.contains("array_contains"),
        "CH SQL leaked Spark function name"
    );
}

/// Dump the Databricks output for visual inspection. Always passes —
/// just a fast way to grep for remaining CH-locked surface during the
/// spike. To see the output: `cargo test print_databricks_vlp -- --nocapture`.
#[tokio::test]
async fn print_databricks_vlp_sql() {
    let ctx = QueryContext {
        dialect: SqlDialect::Databricks,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql("MATCH (a:User)-[:FOLLOWS*1..3]->(b:User) RETURN b.id")
    })
    .await;
    println!("\n--- DATABRICKS VLP SQL (Phase 1.2 spike) ---\n{sql}\n--- END ---\n");
}

/// The load-bearing test: under Databricks dialect, the FunctionMapper-
/// routed sites all flip to Spark spellings. No call-site changes
/// outside the two structural-gap helpers (`array_count`,
/// `json_extract_string`) — Phase 0.1–1.1's abstractions do the work.
#[tokio::test]
async fn vlp_under_databricks_dialect_emits_spark_spellings() {
    let ctx = QueryContext {
        dialect: SqlDialect::Databricks,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql("MATCH (a:User)-[:FOLLOWS*1..3]->(b:User) RETURN b.id")
    })
    .await;

    // arrayConcat → concat
    assert!(
        !sql.contains("arrayConcat"),
        "Databricks SQL still has CH `arrayConcat`; got:\n{sql}"
    );
    // Note: bare `concat(` substring would match either dialect — assert
    // a more specific shape Spark would use. The VLP recursive case
    // builds `concat(vp.path_nodes, [end_id])` style calls; under
    // Spark the empty-array cast uses `array()`.
    assert!(
        sql.contains("CAST(array() AS ARRAY<STRING>)"),
        "expected Spark empty-array cast; got:\n{sql}"
    );

    // has(...) → array_contains(...)
    assert!(
        !sql.contains("NOT has(vp.path_nodes"),
        "Databricks SQL still has CH `has(...)` cycle check; got:\n{sql}"
    );
    assert!(
        sql.contains("array_contains(vp.path_nodes"),
        "expected Spark `array_contains` for cycle detection; got:\n{sql}"
    );

    // No CH empty-array cast
    assert!(
        !sql.contains("CAST([] AS Array(String))"),
        "Databricks SQL leaked CH empty-array cast; got:\n{sql}"
    );
}
