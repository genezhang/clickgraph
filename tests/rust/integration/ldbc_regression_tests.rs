//! LDBC SNB regression tests
//!
//! These tests load the LDBC SNB schema from YAML and verify that all passing
//! LDBC benchmark queries generate valid SQL through the full pipeline:
//! Parse → Plan → Render → Generate SQL.
//!
//! No ClickHouse connection is needed — these test SQL generation only.
//! Uses tokio for task-local QueryContext required by the render pipeline.

use std::sync::Arc;

use clickgraph::{
    graph_catalog::{config::GraphSchemaConfig, graph_schema::GraphSchema},
    open_cypher_parser::strip_comments,
    query_planner::evaluate_read_statement,
    render_plan::{logical_plan_to_render_plan, ToSql},
    server::query_context::{set_current_schema, with_query_context, QueryContext},
};

fn load_ldbc_schema() -> GraphSchema {
    let config =
        GraphSchemaConfig::from_yaml_file("benchmarks/ldbc_snb/schemas/ldbc_snb_complete.yaml")
            .expect("Failed to load LDBC schema YAML");
    config
        .to_graph_schema()
        .expect("Failed to convert LDBC schema config to GraphSchema")
}

/// Helper: load a Cypher query file, strip comments, parse, plan, render, and return SQL.
/// Uses parse_cypher_statement + evaluate_read_statement (same as HTTP server).
async fn generate_sql(schema: &GraphSchema, cypher_path: &str) -> String {
    let schema = schema.clone();
    let path = cypher_path.to_string();

    let ctx = QueryContext::new(Some("default".to_string()));
    with_query_context(ctx, async {
        set_current_schema(Arc::new(schema.clone()));

        let raw = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("Failed to read {}: {}", path, e));
        let cleaned = strip_comments(&raw);

        let (_remaining, statement) =
            clickgraph::open_cypher_parser::parse_cypher_statement(&cleaned)
                .unwrap_or_else(|e| panic!("Failed to parse {}: {:?}", path, e));

        let (logical_plan, _plan_ctx) =
            evaluate_read_statement(statement, &schema, None, None, None)
                .unwrap_or_else(|e| panic!("Failed to plan {}: {:?}", path, e));

        let render_plan = logical_plan_to_render_plan(logical_plan, &schema)
            .unwrap_or_else(|e| panic!("Failed to render {}: {:?}", path, e));
        render_plan.to_sql()
    })
    .await
}

// ---------------------------------------------------------------------------
// Interactive Short queries (short-1 through short-7)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ldbc_short_1() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/interactive/short-1.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_short_2() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/interactive/short-2.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_short_3() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/interactive/short-3.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_short_4() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/interactive/short-4.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_short_5() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/interactive/short-5.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_short_6() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/interactive/short-6.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_short_7() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/interactive/short-7.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

// ---------------------------------------------------------------------------
// Interactive Complex queries
// Some use adapted versions (complex-3, 5, 7, 10, 12, 13)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ldbc_complex_1() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/interactive/complex-1.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_complex_2() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/interactive/complex-2.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_complex_3() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/adapted/interactive-complex-3.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_complex_4() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/interactive/complex-4.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_complex_5() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/adapted/interactive-complex-5.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_complex_6() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/interactive/complex-6.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_complex_7() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/adapted/interactive-complex-7.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_complex_8() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/interactive/complex-8.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_complex_9() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/interactive/complex-9.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_complex_10() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/adapted/interactive-complex-10.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_complex_11() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/interactive/complex-11.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_complex_12() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/adapted/interactive-complex-12.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
    assert!(
        sql.contains("LEFT JOIN"),
        "complex-12 should use LEFT JOIN for OPTIONAL MATCH"
    );
}

#[tokio::test]
async fn ldbc_complex_13() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/adapted/interactive-complex-13.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

// ---------------------------------------------------------------------------
// BI queries
// Some use adapted versions (bi-3, bi-4 workaround, bi-17)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ldbc_bi_1() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/bi/bi-1.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_bi_2() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/bi/bi-2.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_bi_3() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(&schema, "benchmarks/ldbc_snb/queries/adapted/bi-3.cypher").await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_bi_4() {
    let schema = load_ldbc_schema();
    // Official bi-4 uses CALL subquery; use adapted workaround with UNION ALL
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/adapted/bi-4-workaround.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_bi_5() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/bi/bi-5.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_bi_6() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/bi/bi-6.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_bi_7() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/bi/bi-7.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_bi_8() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/adapted/bi-8-workaround.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
    // Verify 2 CTEs are generated (person scores + friend scores)
    assert!(
        sql.contains("with_person_score_cte"),
        "bi-8 should generate person score CTE"
    );
    assert!(
        sql.contains("with_friend_friendScore_person_score_cte"),
        "bi-8 should generate friend score CTE"
    );
}

#[tokio::test]
async fn ldbc_bi_9() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/bi/bi-9.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_bi_11() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/bi/bi-11.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_bi_12() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/bi/bi-12.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_bi_13() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/bi/bi-13.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_bi_17() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(&schema, "benchmarks/ldbc_snb/queries/adapted/bi-17.cypher").await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}

#[tokio::test]
async fn ldbc_bi_18() {
    let schema = load_ldbc_schema();
    let sql = generate_sql(
        &schema,
        "benchmarks/ldbc_snb/queries/official/bi/bi-18.cypher",
    )
    .await;
    assert!(!sql.is_empty());
    assert!(sql.contains("SELECT"));
}
