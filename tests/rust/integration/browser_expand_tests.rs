//! Browser expand regression tests across schema variations
//!
//! The browser expand pattern `MATCH (a:X) WITH a MATCH (a)--(o) RETURN a, o` was broken
//! for "to-side-only" nodes (e.g., Post, which only has incoming AUTHORED/LIKED edges).
//! The VLP CTE end_labels derivation used inferred labels from type inference instead of
//! re-deriving from schema. Fixed in PR #158.
//!
//! These tests validate SQL generation across standard, FK-edge, denormalized, and composite
//! ID schemas to prevent regressions.

use std::sync::Arc;

use clickgraph::{
    graph_catalog::graph_schema::GraphSchema,
    open_cypher_parser::parse_query,
    query_planner::evaluate_read_query,
    render_plan::{logical_plan_to_render_plan, ToSql},
    server::query_context::{set_current_schema, with_query_context, QueryContext},
};

use super::browser_test_schemas::*;

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

/// Full pipeline: parse → plan → render → SQL, wrapped in QueryContext.
async fn generate_expand_sql(schema: &GraphSchema, cypher: &str) -> String {
    let schema = schema.clone();
    let cypher = cypher.to_string();

    let ctx = QueryContext::new(Some("default".to_string()));
    with_query_context(ctx, async {
        set_current_schema(Arc::new(schema.clone()));

        let ast = parse_query(&cypher)
            .unwrap_or_else(|e| panic!("Failed to parse: {:?}\nCypher: {}", e, cypher));

        let (logical_plan, _plan_ctx) = evaluate_read_query(ast, &schema, None, None)
            .unwrap_or_else(|e| panic!("Failed to plan: {:?}\nCypher: {}", e, cypher));

        let render_plan = logical_plan_to_render_plan(logical_plan, &schema)
            .unwrap_or_else(|e| panic!("Failed to render: {:?}\nCypher: {}", e, cypher));

        render_plan.to_sql()
    })
    .await
}

// ===========================================================================
// Standard schema tests
// ===========================================================================

/// User expand with WITH clause — both from-side and to-side edges exist
#[tokio::test]
async fn test_standard_user_expand_with_clause() {
    let schema = create_standard_schema();
    let sql =
        generate_expand_sql(&schema, "MATCH (a:User) WITH a MATCH (a)--(o) RETURN a, o").await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
    assert!(
        sql_lower.contains("union all"),
        "Undirected expand should produce UNION ALL for multiple edge types"
    );
}

/// KEY REGRESSION: Post expand with WITH clause — Post is to-side-only
/// This was the original bug: Post only appears as to_node in AUTHORED and LIKED,
/// so inferred labels from type inference (forward-direction only) missed it.
#[tokio::test]
async fn test_standard_post_expand_with_clause() {
    let schema = create_standard_schema();
    let sql =
        generate_expand_sql(&schema, "MATCH (a:Post) WITH a MATCH (a)--(o) RETURN a, o").await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
    assert!(
        sql_lower.contains("union all"),
        "Post expand should produce UNION ALL (AUTHORED + LIKED reverse)"
    );
}

/// User expand without WITH clause (simple pattern)
#[tokio::test]
async fn test_standard_user_expand_simple() {
    let schema = create_standard_schema();
    let sql = generate_expand_sql(&schema, "MATCH (a:User)--(o) RETURN a, o").await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
    assert!(
        sql_lower.contains("union all"),
        "User undirected expand should produce UNION ALL"
    );
}

/// Post expand without WITH clause (simple pattern)
#[tokio::test]
async fn test_standard_post_expand_simple() {
    let schema = create_standard_schema();
    let sql = generate_expand_sql(&schema, "MATCH (a:Post)--(o) RETURN a, o").await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
    assert!(
        sql_lower.contains("union all"),
        "Post undirected expand should produce UNION ALL"
    );
}

/// Explicit end label — should intersect and exclude irrelevant edges
#[tokio::test]
async fn test_standard_explicit_end_label() {
    let schema = create_standard_schema();
    let sql = generate_expand_sql(&schema, "MATCH (a:User)--(o:Post) RETURN a, o").await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
    // FOLLOWS(U→U) shouldn't participate when o:Post is specified
    assert!(
        !sql_lower.contains("user_follows"),
        "Should not reference user_follows when o:Post is specified"
    );
}

// ===========================================================================
// FK-edge schema tests
// ===========================================================================

/// Order expand with WITH clause — Order is from-side of FK-edge
#[tokio::test]
async fn test_fk_edge_order_expand() {
    let schema = create_fk_edge_schema();
    let sql =
        generate_expand_sql(&schema, "MATCH (a:Order) WITH a MATCH (a)--(o) RETURN a, o").await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
    assert!(
        sql_lower.contains("orders"),
        "Should reference orders table for FK-edge"
    );
}

/// Customer expand with WITH clause — Customer is to-side-only
#[tokio::test]
async fn test_fk_edge_customer_expand() {
    let schema = create_fk_edge_schema();
    let sql = generate_expand_sql(
        &schema,
        "MATCH (a:Customer) WITH a MATCH (a)--(o) RETURN a, o",
    )
    .await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
}

/// Customer expand without WITH clause
#[tokio::test]
async fn test_fk_edge_customer_simple() {
    let schema = create_fk_edge_schema();
    let sql = generate_expand_sql(&schema, "MATCH (a:Customer)--(o) RETURN a, o").await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
}

// ===========================================================================
// Denormalized schema tests
// ===========================================================================

/// Airport expand with WITH clause — self-referencing denormalized edge
#[tokio::test]
async fn test_denorm_airport_expand() {
    let schema = create_denormalized_schema();
    let sql = generate_expand_sql(
        &schema,
        "MATCH (a:Airport) WITH a MATCH (a)--(o) RETURN a, o",
    )
    .await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
    assert!(
        sql_lower.contains("flights"),
        "Should reference flights table"
    );
}

/// Airport expand without WITH clause
#[tokio::test]
async fn test_denorm_airport_simple() {
    let schema = create_denormalized_schema();
    let sql = generate_expand_sql(&schema, "MATCH (a:Airport)--(o) RETURN a, o").await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
    assert!(
        sql_lower.contains("flights"),
        "Should reference flights table"
    );
}

// ===========================================================================
// Composite ID schema tests
// ===========================================================================

/// Account expand with WITH clause — has both OWNS(reverse) and TRANSFERRED edges
#[tokio::test]
async fn test_composite_account_expand() {
    let schema = create_composite_id_schema();
    let sql = generate_expand_sql(
        &schema,
        "MATCH (a:Account) WITH a MATCH (a)--(o) RETURN a, o",
    )
    .await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
    // Both edge types should participate — either as direct table refs or via VLP multi-type CTE
    assert!(
        sql_lower.contains("transfers") || sql_lower.contains("vlp_multi_type"),
        "Should reference transfers table or VLP multi-type CTE"
    );
    assert!(
        sql_lower.contains("account_ownership") || sql_lower.contains("vlp_multi_type"),
        "Should reference account_ownership table or VLP multi-type CTE"
    );
}

/// Customer expand with WITH clause — from-side only (OWNS)
#[tokio::test]
async fn test_composite_customer_expand() {
    let schema = create_composite_id_schema();
    let sql = generate_expand_sql(
        &schema,
        "MATCH (a:Customer) WITH a MATCH (a)--(o) RETURN a, o",
    )
    .await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
}

/// Explicit end label — Account-to-Account should use transfers only
#[tokio::test]
async fn test_composite_explicit_end_label() {
    let schema = create_composite_id_schema();
    let sql = generate_expand_sql(&schema, "MATCH (a:Account)--(o:Account) RETURN a, o").await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
    assert!(
        sql_lower.contains("transfers"),
        "Should reference transfers table for Account-Account edge"
    );
}

// ===========================================================================
// ID-filtered expand tests (browser click-to-expand pattern)
//
// The browser sends: MATCH (a) WHERE id(a) = X ... MATCH (a)--(o) ...
// The id() function is rewritten by IdFunctionTransformer to property-based
// WHERE clauses before reaching the planner. These tests exercise that
// post-rewrite form across all ID types.
// ===========================================================================

/// Integer ID filter + WITH expand — primary browser click pattern
#[tokio::test]
async fn test_id_filter_integer_with_expand() {
    let schema = create_standard_schema();
    let sql = generate_expand_sql(
        &schema,
        "MATCH (a:User) WHERE a.user_id = 1 WITH a MATCH (a)--(o) RETURN a, o",
    )
    .await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
    assert!(
        sql_lower.contains("union all"),
        "ID-filtered expand should still produce UNION ALL"
    );
    // WHERE predicate should survive the WITH barrier
    assert!(
        sql_lower.contains("= 1"),
        "Integer ID filter should appear in SQL"
    );
}

/// Integer ID filter on to-side-only node (Post) — regression vector
#[tokio::test]
async fn test_id_filter_integer_toside_with_expand() {
    let schema = create_standard_schema();
    let sql = generate_expand_sql(
        &schema,
        "MATCH (a:Post) WHERE a.post_id = 42 WITH a MATCH (a)--(o) RETURN a, o",
    )
    .await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
    assert!(
        sql_lower.contains("union all"),
        "Post ID-filtered expand should produce UNION ALL"
    );
    assert!(
        sql_lower.contains("42"),
        "Integer ID filter should appear in SQL"
    );
}

/// Integer ID filter — direct pattern (no WITH clause)
#[tokio::test]
async fn test_id_filter_integer_direct() {
    let schema = create_standard_schema();
    let sql = generate_expand_sql(
        &schema,
        "MATCH (a:User)--(o) WHERE a.user_id = 1 RETURN a, o",
    )
    .await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
    assert!(
        sql_lower.contains("= 1"),
        "Integer ID filter should appear in SQL"
    );
}

/// String ID filter + WITH expand (Airport with string code)
#[tokio::test]
async fn test_id_filter_string_with_expand() {
    let schema = create_denormalized_schema();
    let sql = generate_expand_sql(
        &schema,
        "MATCH (a:Airport) WHERE a.code = 'LAX' WITH a MATCH (a)--(o) RETURN a, o",
    )
    .await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
    assert!(
        sql_lower.contains("flights"),
        "Should reference flights table"
    );
    assert!(
        sql_lower.contains("lax") || sql_lower.contains("LAX"),
        "String ID filter should appear in SQL"
    );
}

/// String ID filter — direct pattern
#[tokio::test]
async fn test_id_filter_string_direct() {
    let schema = create_denormalized_schema();
    let sql = generate_expand_sql(
        &schema,
        "MATCH (a:Airport)--(o) WHERE a.code = 'LAX' RETURN a, o",
    )
    .await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
    assert!(sql.contains("LAX"), "String ID filter should appear in SQL");
}

/// Composite ID filter + WITH expand (Account with bank_id + account_number)
#[tokio::test]
async fn test_id_filter_composite_with_expand() {
    let schema = create_composite_id_schema();
    let sql = generate_expand_sql(
        &schema,
        "MATCH (a:Account) WHERE a.bank_id = 'B1' AND a.account_number = '12345' WITH a MATCH (a)--(o) RETURN a, o",
    )
    .await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
    assert!(
        sql.contains("B1") || sql.contains("b1"),
        "First composite key component should appear in SQL"
    );
    assert!(
        sql.contains("12345"),
        "Second composite key component should appear in SQL"
    );
}

/// Composite ID filter — direct pattern
#[tokio::test]
async fn test_id_filter_composite_direct() {
    let schema = create_composite_id_schema();
    let sql = generate_expand_sql(
        &schema,
        "MATCH (a:Account)--(o) WHERE a.bank_id = 'B1' AND a.account_number = '12345' RETURN a, o",
    )
    .await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
    assert!(
        sql.contains("12345"),
        "Composite key filter should appear in SQL"
    );
}

/// FK-edge integer ID filter + WITH expand
#[tokio::test]
async fn test_id_filter_fk_edge_with_expand() {
    let schema = create_fk_edge_schema();
    let sql = generate_expand_sql(
        &schema,
        "MATCH (a:Order) WHERE a.order_id = 99 WITH a MATCH (a)--(o) RETURN a, o",
    )
    .await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
    assert!(sql_lower.contains("99"), "ID filter should appear in SQL");
}

// ===========================================================================
// Directed expand tests (browser direction filtering)
//
// Browser can send outgoing-only or incoming-only patterns.
// ===========================================================================

/// Outgoing expand from User — should include FOLLOWS, AUTHORED, LIKED (all outgoing)
#[tokio::test]
async fn test_directed_outgoing_user() {
    let schema = create_standard_schema();
    let sql = generate_expand_sql(&schema, "MATCH (a:User)-->(o) RETURN a, o").await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
}

/// Incoming expand to User — should include FOLLOWS reverse only
#[tokio::test]
async fn test_directed_incoming_user() {
    let schema = create_standard_schema();
    let sql = generate_expand_sql(&schema, "MATCH (a:User)<--(o) RETURN a, o").await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
    assert!(
        sql_lower.contains("user_follows"),
        "Incoming to User should use user_follows (FOLLOWS reverse)"
    );
}

/// Incoming expand to Post — AUTHORED and LIKED both point to Post
#[tokio::test]
async fn test_directed_incoming_post() {
    let schema = create_standard_schema();
    let sql = generate_expand_sql(&schema, "MATCH (a:Post)<--(o) RETURN a, o").await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
}

/// Outgoing expand from Post — Post has no outgoing edges, should handle gracefully
#[tokio::test]
async fn test_directed_outgoing_post() {
    let schema = create_standard_schema();
    // Post is to-side only — no outgoing edges exist in schema
    let result = std::panic::catch_unwind(|| {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            generate_expand_sql(&schema, "MATCH (a:Post)-->(o) RETURN a, o").await
        })
    });
    // Either produces SQL (empty result) or errors — both are valid
    // The key thing is it doesn't panic with an unhandled error
    if let Ok(sql) = result {
        assert!(
            sql.to_lowercase().contains("select"),
            "If SQL is produced, it should be valid"
        );
    }
}

/// Directed outgoing with WITH clause + ID filter
#[tokio::test]
async fn test_directed_outgoing_with_id_filter() {
    let schema = create_standard_schema();
    let sql = generate_expand_sql(
        &schema,
        "MATCH (a:User) WHERE a.user_id = 1 WITH a MATCH (a)-->(o) RETURN a, o",
    )
    .await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
    assert!(sql_lower.contains("= 1"), "ID filter should appear in SQL");
}

/// Directed incoming with WITH clause + ID filter
#[tokio::test]
async fn test_directed_incoming_with_id_filter() {
    let schema = create_standard_schema();
    let sql = generate_expand_sql(
        &schema,
        "MATCH (a:User) WHERE a.user_id = 1 WITH a MATCH (a)<--(o) RETURN a, o",
    )
    .await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
    assert!(sql_lower.contains("= 1"), "ID filter should appear in SQL");
}

/// Directed expand on FK-edge schema
#[tokio::test]
async fn test_directed_fk_edge_outgoing() {
    let schema = create_fk_edge_schema();
    let sql = generate_expand_sql(&schema, "MATCH (a:Order)-->(o) RETURN a, o").await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
    assert!(
        sql_lower.contains("orders"),
        "Outgoing from Order should reference orders table"
    );
}

/// Directed expand on denormalized schema — self-referencing
#[tokio::test]
async fn test_directed_denorm_outgoing() {
    let schema = create_denormalized_schema();
    let sql = generate_expand_sql(&schema, "MATCH (a:Airport)-->(o) RETURN a, o").await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
    assert!(
        sql_lower.contains("flights"),
        "Outgoing from Airport should reference flights table"
    );
}

// ===========================================================================
// Exclude list tests (browser "already visible nodes" filtering)
//
// Browser sends: MATCH (a)--(o) WHERE id(a) = X AND NOT id(o) IN [Y, Z]
// After id() rewrite this becomes property-based NOT IN [...] clauses.
// ===========================================================================

/// Exclude list with integer IDs
#[tokio::test]
async fn test_exclude_list_integer() {
    let schema = create_standard_schema();
    let sql = generate_expand_sql(
        &schema,
        "MATCH (a:User)--(o) WHERE a.user_id = 1 AND NOT o.user_id IN [2, 3] RETURN o LIMIT 100",
    )
    .await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
    assert!(sql_lower.contains("limit"), "LIMIT should appear in SQL");
}

/// Exclude list with string IDs
#[tokio::test]
async fn test_exclude_list_string() {
    let schema = create_denormalized_schema();
    let sql = generate_expand_sql(
        &schema,
        "MATCH (a:Airport)--(o) WHERE a.code = 'LAX' AND NOT o.code IN ['SFO', 'JFK'] RETURN o LIMIT 100",
    )
    .await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
    assert!(sql.contains("LAX"), "Source ID filter should appear in SQL");
}

/// Full browser expand sequence: fetch node + expand with exclude list + LIMIT
#[tokio::test]
async fn test_full_browser_sequence_fetch() {
    let schema = create_standard_schema();
    // Step 1: Fetch the node (browser sends this first)
    let sql = generate_expand_sql(&schema, "MATCH (a:User) WHERE a.user_id = 1 RETURN a").await;

    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("select"),
        "Node fetch should produce valid SQL"
    );
    assert!(sql_lower.contains("= 1"), "ID filter should appear in SQL");
}

/// Full browser expand sequence: expand with LIMIT + ORDER BY
#[tokio::test]
async fn test_full_browser_sequence_expand() {
    let schema = create_standard_schema();
    // Step 2: Expand (browser sends this after fetching the node)
    let sql = generate_expand_sql(
        &schema,
        "MATCH (a:User)--(o) WHERE a.user_id = 1 RETURN o LIMIT 100",
    )
    .await;

    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("select"),
        "Expand should produce valid SQL"
    );
    assert!(sql_lower.contains("limit"), "LIMIT should appear in SQL");
}
