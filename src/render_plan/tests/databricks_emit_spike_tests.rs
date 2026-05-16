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
//! 1. **Array-literal syntax (Phase 1.3, done).** `format!("[{...}]")`
//!    sites in the rendering layer now go through
//!    `FunctionMapper::array_literal(&elems)`, so Spark sees
//!    `array(a, b)` and CH sees `[a, b]`. Asserted by
//!    `vlp_under_databricks_dialect_emits_spark_spellings` below.
//!
//! 2. **Identifier / alias quoting (Phase 1.4, done).** `AS "alias"`
//!    sites in the rendering layer now route through
//!    `FunctionMapper::quote_alias(&name)`, so Spark sees
//!    `` AS `b.id` `` and CH sees `AS "b.id"` (existing behavior).
//!    Spark parses double quotes as string literals, so backticks are
//!    mandatory there. Asserted by the spellings test below.
//!
//! 3. **Aggregate functions via function_registry**. Functions like
//!    Cypher `collect()` are mapped via the hardcoded `clickhouse_name`
//!    field on `FunctionMapping` in
//!    `emitters/clickhouse/function_registry.rs`, not `FunctionMapper`.
//!    So `collect(n.name)` still emits `groupArray(...)` under
//!    Databricks dialect. A follow-up phase should make the registry
//!    dialect-aware (or split into per-dialect registries).
//!
//! 4. **Type names in non-routed CASTs**. The current spike doesn't
//!    exercise these but they exist — `UInt32`, `Float64`, etc., in
//!    various rendering paths.
//!
//! What this means for the abstraction: Phase 0.1–1.1 was about routing
//! the *function name* layer; Phase 1.3 routed array-literal shape;
//! Phase 1.4 routed identifier quoting. The remaining work — aggregate
//! registry routing and CAST type names — fits the same shape.

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
    // The VLP recursive case builds `[end_id]` to append onto path_nodes.
    // Under CH dialect this stays as bracket-syntax array literal.
    assert!(
        sql.contains("arrayConcat(vp.path_nodes, [") || sql.contains("[toString("),
        "expected CH bracket-style array literal in VLP path append; got:\n{sql}"
    );
    // Spark spellings must NOT appear under CH dialect.
    assert!(!sql.contains("ARRAY<STRING>"), "CH SQL leaked Spark type");
    assert!(
        !sql.contains("array_contains"),
        "CH SQL leaked Spark function name"
    );
    assert!(
        !sql.contains("array(toString("),
        "CH SQL leaked Spark array() literal"
    );

    // Phase 1.4: alias quoting. CH keeps historical double-quote form
    // for `AS` clauses. Backticks must NOT appear in the AS position
    // (they may appear elsewhere from `quote_identifier`).
    assert!(
        sql.contains("AS \"b.id\""),
        "expected CH double-quoted alias `AS \"b.id\"`; got:\n{sql}"
    );
    assert!(
        !sql.contains("AS `b.id`"),
        "CH SQL leaked Spark backtick alias quoting; got:\n{sql}"
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

    // Phase 1.3: array literals now route through FunctionMapper.
    // The VLP recursive case builds `array(end_id)` to append onto
    // path_nodes under Databricks dialect (CH builds `[end_id]`).
    assert!(
        sql.contains("concat(vp.path_nodes, array("),
        "expected Spark `array(...)` literal in VLP path append; got:\n{sql}"
    );
    // No bracket-style array literals should leak into Databricks SQL.
    // (We allow `[` to appear in other contexts like `Array(String)`
    // type names inside CH-only output, but those shouldn't show up here.)
    assert!(
        !sql.contains(", [toString(") && !sql.contains("vp.path_nodes, ["),
        "Databricks SQL leaked CH bracket-style array literal; got:\n{sql}"
    );

    // Phase 1.4: alias quoting. Spark parses double-quoted identifiers
    // as string literals, so the `AS` clause must use backticks.
    assert!(
        sql.contains("AS `b.id`"),
        "expected Spark backtick alias `AS `b.id``; got:\n{sql}"
    );
    assert!(
        !sql.contains("AS \"b.id\""),
        "Databricks SQL leaked CH double-quoted alias; got:\n{sql}"
    );
}

/// Aggregation path — exercises `build_outer_aggregate_select` and the
/// `extract_outer_aggregation_info` rewrite where ColumnAlias references
/// in GROUP BY / aggregate args get quoted via `quote_alias`. The plain
/// VLP test above only covers the final-SELECT `AS` path, so this test
/// guards the references-side routing from silently regressing back to
/// double-quoted identifiers.
#[tokio::test]
async fn aggregation_under_databricks_uses_backtick_alias_refs() {
    let ctx = QueryContext {
        dialect: SqlDialect::Databricks,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql("MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id AS bid, count(a) AS ct")
    })
    .await;

    // The aggregate result alias `ct` should emit `AS ` + backticks.
    // Non-aggregate alias `bid` likewise. No double-quoted aliases.
    assert!(
        !sql.contains("AS \""),
        "Databricks aggregation SQL leaked CH double-quoted alias; got:\n{sql}"
    );
    // Spot-check that at least one backtick alias appears in the AS
    // position. The exact site varies by plan shape, so use a
    // tolerant pattern.
    assert!(
        sql.contains("AS `"),
        "expected Spark backtick alias in aggregation SQL; got:\n{sql}"
    );
}

/// CH baseline for the aggregation path — guards against the CH side
/// silently flipping to backticks when we touch this code in the future.
#[tokio::test]
async fn aggregation_under_clickhouse_keeps_double_quoted_alias_refs() {
    let ctx = QueryContext {
        dialect: SqlDialect::ClickHouse,
        ..QueryContext::default()
    };
    let sql = with_query_context(ctx, async {
        cypher_to_sql("MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN b.id AS bid, count(a) AS ct")
    })
    .await;

    // CH historically emits double-quoted aliases here. Verify both
    // sites still produce the existing shape.
    assert!(
        sql.contains("AS \""),
        "expected CH double-quoted alias in aggregation SQL; got:\n{sql}"
    );
}
