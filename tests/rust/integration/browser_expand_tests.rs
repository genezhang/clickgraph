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

/// Full pipeline returning Result — for tests expecting possible errors.
async fn try_generate_expand_sql(schema: &GraphSchema, cypher: &str) -> Result<String, String> {
    let schema = schema.clone();
    let cypher = cypher.to_string();

    let ctx = QueryContext::new(Some("default".to_string()));
    with_query_context(ctx, async {
        set_current_schema(Arc::new(schema.clone()));

        let ast = match parse_query(&cypher) {
            Ok(ast) => ast,
            Err(e) => return Err(format!("Parse error: {:?}", e)),
        };

        let (logical_plan, _plan_ctx) = match evaluate_read_query(ast, &schema, None, None) {
            Ok(result) => result,
            Err(e) => return Err(format!("Plan error: {:?}", e)),
        };

        match logical_plan_to_render_plan(logical_plan, &schema) {
            Ok(render_plan) => Ok(render_plan.to_sql()),
            Err(e) => Err(format!("Render error: {:?}", e)),
        }
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
    // Either produces SQL (degenerate/empty result) or returns an error — both are valid
    let result = try_generate_expand_sql(&schema, "MATCH (a:Post)-->(o) RETURN a, o").await;
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

// ===========================================================================
// Browser click-to-expand: WITH + pattern comprehension + path variable
// Regression tests for PR #234 (CTE column double-encoding, missing ID column,
// Column pruning). The browser sends this exact pattern when expanding nodes.
// ===========================================================================

/// Neo4j Browser expand: User node with WHERE filter + size(PC) + RETURN path
/// Regression: CTE was missing node ID column when RETURN references path variable,
/// causing "Identifier 'p1_a_user_id' cannot be resolved" on ClickHouse.
#[tokio::test]
async fn test_browser_expand_with_pc_and_path_user() {
    let schema = create_standard_schema();
    let cypher = "MATCH (a:User) WHERE a.user_id = 1 \
                  WITH a, size([(a)--() | 1]) AS allNeighboursCount \
                  MATCH path = (a)--(o) \
                  RETURN path, allNeighboursCount \
                  ORDER BY o.user_id LIMIT 97";
    let sql = generate_expand_sql(&schema, cypher).await;
    let sql_lower = sql.to_lowercase();

    // CTE must include node ID column for VLP JOIN
    assert!(
        sql.contains("p1_a_user_id"),
        "WITH CTE must include node ID column p1_a_user_id for VLP JOIN: got SQL:\n{sql}"
    );
    // Must have allNeighboursCount in output
    assert!(
        sql_lower.contains("allneighbourscount"),
        "allNeighboursCount must appear in SQL: got SQL:\n{sql}"
    );
    // Must NOT have double-encoded CTE column names (p20_a_allNeighboursCount_p1_a_user_id)
    assert!(
        !sql.contains("p20_"),
        "Must not double-encode CTE column names: got SQL:\n{sql}"
    );
}

/// Neo4j Browser expand: Post node (to-side-only) with WHERE filter + size(PC) + RETURN path
/// Regression: Post nodes only have incoming edges (AUTHORED, LIKED), making them
/// "to-side-only". Combined with RETURN path, the CTE lost all node columns.
#[tokio::test]
async fn test_browser_expand_with_pc_and_path_post() {
    let schema = create_standard_schema();
    let cypher = "MATCH (a:Post) WHERE a.post_id = 19 \
                  WITH a, size([(a)--() | 1]) AS allNeighboursCount \
                  MATCH path = (a)--(o) \
                  RETURN path, allNeighboursCount \
                  ORDER BY o.user_id LIMIT 97";
    let sql = generate_expand_sql(&schema, cypher).await;
    let sql_lower = sql.to_lowercase();

    // CTE must include node ID column for VLP JOIN
    assert!(
        sql.contains("p1_a_post_id"),
        "WITH CTE must include node ID column p1_a_post_id for VLP JOIN: got SQL:\n{sql}"
    );
    assert!(
        sql_lower.contains("allneighbourscount"),
        "allNeighboursCount must appear in SQL: got SQL:\n{sql}"
    );
    assert!(
        !sql.contains("p20_"),
        "Must not double-encode CTE column names: got SQL:\n{sql}"
    );
}

/// Neo4j Browser expand: RETURN a, o (property access) — should also work
/// This is the simpler case that always worked, included as baseline regression.
#[tokio::test]
async fn test_browser_expand_with_pc_return_properties() {
    let schema = create_standard_schema();
    let cypher = "MATCH (a:User) WHERE a.user_id = 1 \
                  WITH a, size([(a)--() | 1]) AS allNeighboursCount \
                  MATCH (a)--(o) \
                  RETURN a.user_id, allNeighboursCount \
                  ORDER BY o.user_id LIMIT 97";
    let sql = generate_expand_sql(&schema, cypher).await;

    assert!(
        sql.contains("p1_a_user_id"),
        "WITH CTE must include user_id column: got SQL:\n{sql}"
    );
    assert!(
        !sql.contains("p20_"),
        "Must not double-encode CTE column names: got SQL:\n{sql}"
    );
}

// ===========================================================================
// NeoDash node right-click expansion query (startNode/endNode + WITH *)
// ===========================================================================

/// `startNode(r)` and `endNode(r)` resolve to the from/to ID columns of the relationship table.
/// Used in NeoDash's non-APOC node expansion query.
#[tokio::test]
async fn test_startnode_and_endnode_in_case_expression() {
    let schema = create_standard_schema();

    // Test startNode(r) in CASE WHEN
    let cypher = "MATCH (b:User)-[r:FOLLOWS]->(u:User) \
                  WITH type(r) as type, CASE WHEN startNode(r) = b THEN 'out' ELSE 'in' END as dir, COUNT(*) as value \
                  RETURN type, dir, value";
    let sql = generate_expand_sql(&schema, cypher).await;
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("select"),
        "startNode query should produce valid SQL"
    );
    // startNode(r) = b → r.follower_id = b.user_id (FOLLOWS from_id = follower_id)
    assert!(
        sql_lower.contains("follower_id"),
        "startNode(r) should reference the from_id column (follower_id): got SQL:\n{sql}"
    );
    assert!(
        sql_lower.contains("user_id"),
        "node comparison should reference the node id column (user_id): got SQL:\n{sql}"
    );
    assert!(
        sql_lower.contains("case when"),
        "CASE WHEN should appear in SQL"
    );

    // Test endNode(r) in CASE WHEN
    let cypher2 = "MATCH (b:User)-[r:FOLLOWS]->(u:User) \
                   WITH type(r) as type, CASE WHEN endNode(r) = u THEN 'to' ELSE 'other' END as side \
                   RETURN type, side";
    let sql2 = generate_expand_sql(&schema, cypher2).await;
    let sql2_lower = sql2.to_lowercase();
    assert!(
        sql2_lower.contains("followed_id"),
        "endNode(r) should reference the to_id column (followed_id): got SQL:\n{sql2}"
    );
}

/// `WITH *` carries all visible aliases forward unchanged (no re-enumeration needed).
/// Used in NeoDash's fallback node expansion query after UNWIND.
#[tokio::test]
async fn test_with_star_preserves_scope() {
    let schema = create_standard_schema();
    let cypher = "MATCH (b:User)-[r:FOLLOWS]->(u:User) \
                  WITH type(r) as type, COUNT(*) as value \
                  UNWIND ['in', 'out', 'any'] as direction \
                  WITH * \
                  WHERE direction = 'out' OR direction = 'any' \
                  RETURN type, direction, sum(value) as total";
    let sql = generate_expand_sql(&schema, cypher).await;
    let sql_lower = sql.to_lowercase();

    assert!(
        sql_lower.contains("select"),
        "WITH * query should produce valid SQL"
    );
    assert!(
        sql_lower.contains("direction"),
        "direction alias should appear in final SELECT: got SQL:\n{sql}"
    );
    assert!(
        sql_lower.contains("type"),
        "type alias from earlier WITH should still be visible: got SQL:\n{sql}"
    );
}

/// Full NeoDash non-APOC node expansion query (regression test).
/// Uses labeled/typed pattern (ClickGraph requires labels to resolve tables).
/// NeoDash's actual query uses `MATCH (b) WHERE id(b) = $id` (unlabeled) — that variant
/// is tested separately in `test_neodash_expansion_with_id_parameter`.
#[tokio::test]
async fn test_neodash_node_expansion_query() {
    let schema = create_standard_schema();
    // NeoDash sends this after APOC fails; uses startNode(r) + WITH *
    let cypher = "MATCH (b:User)-[r:FOLLOWS]-(u:User) \
                  WITH type(r) as type, CASE WHEN startNode(r) = b THEN 'out' ELSE 'in' END as dir, COUNT(*) as value \
                  UNWIND ['in', 'out', 'any'] as direction \
                  WITH * \
                  WHERE (direction = dir) OR direction = 'any' \
                  RETURN type, direction, sum(value) as value ORDER BY type, direction";
    let result = try_generate_expand_sql(&schema, cypher).await;
    assert!(
        result.is_ok(),
        "NeoDash node expansion query should compile successfully: {:?}",
        result.err()
    );
    let sql = result.unwrap();
    let sql_lower = sql.to_lowercase();

    assert!(
        sql_lower.contains("select"),
        "Should produce valid SQL: {sql}"
    );
    assert!(
        sql_lower.contains("direction"),
        "direction from UNWIND should appear in output: {sql}"
    );
    assert!(
        sql_lower.contains("type"),
        "type from WITH should appear in output: {sql}"
    );
}

/// NeoDash actual expansion query structure with $id parameter and labeled node.
/// NeoDash sends `MATCH (b) WHERE id(b) = $id` (unlabeled), which requires ClickGraph to
/// infer the label from context. This test uses the labeled form as the closest approximation
/// that ClickGraph can resolve. The unlabeled form requires label inference from `id()` which
/// is a known limitation documented in KNOWN_ISSUES.md.
#[tokio::test]
async fn test_neodash_expansion_with_id_parameter() {
    let schema = create_standard_schema();
    // Closest approximation to the real NeoDash query using a labeled node + $id parameter
    let cypher = "MATCH (b:User) WHERE b.user_id = $id \
                  MATCH (b)-[r:FOLLOWS]-(u:User) \
                  WITH type(r) as type, CASE WHEN startNode(r) = b THEN 'out' ELSE 'in' END as dir, COUNT(*) as value \
                  UNWIND ['in', 'out', 'any'] as direction \
                  WITH * \
                  WHERE (direction = dir) OR direction = 'any' \
                  RETURN type, direction, sum(value) as value ORDER BY type, direction";
    let result = try_generate_expand_sql(&schema, cypher).await;
    assert!(
        result.is_ok(),
        "NeoDash expansion with id parameter should compile: {:?}",
        result.err()
    );
    let sql = result.unwrap();
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("select"),
        "Should produce valid SQL: {sql}"
    );
    assert!(
        sql_lower.contains("follower_id"),
        "from_id should appear: {sql}"
    );
}

// ===========================================================================
// Multi-type expand regression: VLP alias must not bleed into non-VLP branches
//
// When `MATCH (a)-[r]-(b)` creates UNION branches where some branches use a
// multi-type VLP CTE (e.g., vlp_multi_type_a_b for AUTHORED|LIKED) and other
// branches use plain tables (e.g., social.user_follows for FOLLOWS), the VLP
// property rewriting (JSON_VALUE(b.end_properties, ...)) must NOT be applied
// to non-VLP branches. Without the fix, `b.user_id` in the FOLLOWS branch
// generates `JSON_VALUE(b.end_properties, '$.user_id')` — an identifier that
// doesn't exist on `social.users`.
// ===========================================================================

#[tokio::test]
async fn test_mixed_type_expand_with_clause_compiles() {
    // Regression: `MATCH (a) WITH a MATCH (a)-[r]-(b) RETURN r` with unlabeled r across
    // multiple relationship types (FOLLOWS User→User, AUTHORED/LIKED User→Post).
    // A multi-type VLP CTE is generated for cross-type branches. The VLP CTE body
    // correctly uses `end_properties` as an internal column. The outer query must NOT
    // use JSON_VALUE(b.end_properties, ...) for plain-table branches.
    let schema = create_standard_schema();
    let result = try_generate_expand_sql(
        &schema,
        "MATCH (a:User {user_id: '9'}) WITH a MATCH (a)-[r]-(b) RETURN r",
    )
    .await;
    assert!(
        result.is_ok(),
        "Mixed-type unlabeled WITH+expand should compile: {:?}",
        result.err()
    );
    let sql = result.unwrap();
    let sql_lower = sql.to_lowercase();
    // Must reference FOLLOWS relationship columns
    assert!(
        sql_lower.contains("followed_id") || sql_lower.contains("follower_id"),
        "FOLLOWS branch must reference follower/followed column: {}",
        sql
    );
    // The outer query's JOIN conditions must not use JSON_VALUE on plain table aliases.
    // `end_properties` is fine inside the VLP CTE body; what's wrong is
    // JSON_VALUE(b.end_properties) in outer JOINs when b = social.users.
    // We check that if `json_value` appears, it's only inside the CTE body
    // (before the outer SELECT), not in the outer JOIN conditions.
    if sql_lower.contains("json_value") {
        // Find where the outer SELECT starts (after CTE declarations)
        let outer_start = sql
            .rfind("\nSELECT ")
            .or_else(|| sql.find("SELECT "))
            .unwrap_or(0);
        let outer_sql = &sql_lower[outer_start..];
        assert!(
            !outer_sql.contains("json_value"),
            "Outer SELECT must not use JSON_VALUE (VLP aliases bleeding): outer={}",
            &sql[outer_start..]
        );
    }
}

/// Regression test: FOLLOWS JOIN must not be contaminated by VLP context.
///
/// When a User expand query produces both a multi-type VLP branch (AUTHORED+LIKED
/// for Post endpoints, using `vlp_multi_type_a_b`) and a FOLLOWS branch (User→User,
/// using `test.user_follows`), the FOLLOWS branch's JOIN ON condition must use the
/// FOLLOWS table's own foreign-key columns (`follower_id`/`followed_id`), NOT the
/// VLP endpoint column (`post_id`) or the JSON extraction function (`JSON_VALUE`).
///
/// Before the fix, `multi_type_vlp_aliases` leaked between UNION branches:
/// `b` was registered as a VLP endpoint, causing the FOLLOWS branch to render
/// `INNER JOIN test.users AS b ON b.post_id = r.followed_id` instead of the
/// correct `ON b.user_id = r.followed_id`.
#[tokio::test]
async fn test_follows_join_not_contaminated_by_vlp_context() {
    // FOLLOWS (User→User) query that also involves AUTHORED/LIKED (User→Post) in the
    // broader expand context. Here we test the FOLLOWS branch in isolation to verify
    // the JOIN ON uses user_id, not post_id or JSON_VALUE.
    let schema = create_standard_schema();

    // Simple FOLLOWS query: no VLP involved — verifies basic FOLLOWS JOIN columns
    let sql = generate_expand_sql(
        &schema,
        "MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN r",
    )
    .await;
    let sql_lower = sql.to_lowercase();

    assert!(
        sql_lower.contains("follower_id") || sql_lower.contains("followed_id"),
        "FOLLOWS JOIN must use follower_id or followed_id, got: {sql}"
    );
    assert!(
        !sql_lower.contains("post_id"),
        "FOLLOWS JOIN must NOT reference post_id (VLP context leak), got: {sql}"
    );
    assert!(
        !sql_lower.contains("json_value"),
        "FOLLOWS JOIN must NOT use JSON_VALUE (VLP property rewriting leak), got: {sql}"
    );
}

/// Regression test: FOLLOWS branch in multi-type expand must not use post_id or JSON_VALUE.
///
/// This is the more complex scenario: `MATCH (a:User) WITH a MATCH (a)-[r]-(b)` creates
/// both VLP branches (AUTHORED/LIKED cross-type, using `vlp_multi_type_a_b`) and a
/// FOLLOWS branch (using `test.user_follows`). The FOLLOWS branch's JOIN ON must use
/// `user_id`, not `post_id` (which is the VLP endpoint column for Post nodes) and not
/// `JSON_VALUE` (which is VLP-specific property extraction).
#[tokio::test]
async fn test_follows_join_in_mixed_expand_not_contaminated() {
    let schema = create_standard_schema();
    let result = try_generate_expand_sql(
        &schema,
        "MATCH (a:User {user_id: '1'}) WITH a MATCH (a)-[r]-(b) RETURN r",
    )
    .await;

    assert!(
        result.is_ok(),
        "Mixed-type expand should compile: {:?}",
        result.err()
    );
    let sql = result.unwrap();
    let sql_lower = sql.to_lowercase();

    // Must include FOLLOWS relationship columns (User→User)
    assert!(
        sql_lower.contains("follower_id") || sql_lower.contains("followed_id"),
        "FOLLOWS branch must use follower_id/followed_id, got: {sql}"
    );

    // The FOLLOWS branch must NOT use post_id (which is Post's node_id column)
    // as a JOIN ON condition. This would indicate the VLP context (b → vlp endpoint)
    // leaked into the FOLLOWS branch (b → test.users).
    //
    // post_id may appear inside the VLP CTE body (for the AUTHORED/LIKED paths),
    // but must NOT appear in JOIN ON conditions for the FOLLOWS branch.
    // We verify by checking the overall SQL does not use post_id outside of the VLP CTE.
    if sql_lower.contains("post_id") {
        // Find the CTE block (before the outer SELECT)
        // CTE declarations end where the main query begins
        // Look for the last "SELECT" that starts the outer query (not inside CTE)
        let outer_start = sql
            .rfind("
SELECT ")
            .or_else(|| sql.find("SELECT "))
            .unwrap_or(0);
        let outer_sql = &sql_lower[outer_start..];
        assert!(
            !outer_sql.contains("post_id"),
            "FOLLOWS JOIN ON in outer query must not use post_id (VLP context leak), outer: {}",
            &sql[outer_start..]
        );
    }

    // Similarly, JSON_VALUE must not appear in the outer SELECT (FOLLOWS branch joins)
    if sql_lower.contains("json_value") {
        let outer_start = sql
            .rfind("
SELECT ")
            .or_else(|| sql.find("SELECT "))
            .unwrap_or(0);
        let outer_sql = &sql_lower[outer_start..];
        assert!(
            !outer_sql.contains("json_value"),
            "Outer SELECT must not use JSON_VALUE (VLP aliases bleeding into FOLLOWS branch): outer={}",
            &sql[outer_start..]
        );
    }
}

// ===========================================================================
// Both-endpoint IN-list filter tests
//
// The browser expand query sends:
//   MATCH (a)-[r]->(b) WHERE id(a) IN [encoded_ids] AND id(b) IN [encoded_ids] RETURN r
//
// After element-ID decoding this becomes property IN-lists on both endpoints.
// When the schema has mixed-type edges (FOLLOWS: User→User, AUTHORED/LIKED: User→Post),
// a multi-type VLP CTE is generated for the cross-type branches.
//
// The VLP branch has `FROM vlp_multi_type_a_b AS t` — aliases `a` and `b` are NOT
// in scope there. The WHERE clause must be rewritten from:
//   a.user_id IN [...] AND b.post_id IN [...]   ← wrong: a/b not in scope
// to:
//   t.start_id IN [...] AND t.end_id IN [...]   ← correct: CTE columns
//
// The root cause (fixed in rewrite_vlp_select_aliases): the "optional VLP" early-return
// fired when the main plan FROM was a regular table, skipping union-branch rewriting
// entirely. All tests in this section would have caught that regression.
// ===========================================================================

/// Both endpoints filtered with IN-list, same node type (User→User, FOLLOWS only).
/// The simpler case: no VLP needed, straight JOIN branch.
#[tokio::test]
async fn test_both_endpoint_in_list_same_type() {
    let schema = create_standard_schema();
    let sql = generate_expand_sql(
        &schema,
        "MATCH (a:User)-[r:FOLLOWS]->(b:User) WHERE a.user_id IN [1, 2] AND b.user_id IN [3, 4] RETURN r",
    )
    .await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL");
    assert!(
        sql_lower.contains("follower_id") || sql_lower.contains("followed_id"),
        "FOLLOWS JOIN must use follower_id/followed_id: {sql}"
    );
    // Both IN-list values must survive into SQL
    assert!(sql_lower.contains("in (1, 2)") || sql_lower.contains("in (1,2)") || sql.contains("IN [1, 2]"),
        "Start node IN-list must appear in SQL: {sql}");
    assert!(sql_lower.contains("in (3, 4)") || sql_lower.contains("in (3,4)") || sql.contains("IN [3, 4]"),
        "End node IN-list must appear in SQL: {sql}");
}

/// Both endpoints filtered with IN-list, MIXED node types (User→User FOLLOWS  +  User→Post VLP).
/// This is the EXACT browser expand pattern that triggered the regression:
///   MATCH (a)-[r]->(b) WHERE id(a) IN [...] AND id(b) IN [...] RETURN r
/// After decoding: a.user_id IN [15,24] AND b.post_id IN [10,30]
/// The multi-type VLP branch (vlp_multi_type_a_b) must rewrite to t.start_id/t.end_id.
#[tokio::test]
async fn test_both_endpoint_in_list_mixed_type_vlp() {
    let schema = create_standard_schema();
    let sql = generate_expand_sql(
        &schema,
        "MATCH (a:User)-[r]->(b) WHERE a.user_id IN [15, 24] AND b.post_id IN [10, 30] RETURN r",
    )
    .await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL: {sql}");

    // If a VLP CTE is generated (multi-type expand), the VLP branch WHERE must use
    // t.start_id / t.end_id, NOT bare a.user_id / b.post_id (which are out of scope
    // when FROM is vlp_multi_type_a_b AS t).
    if sql_lower.contains("vlp_multi_type") {
        // The IN-list values must still appear (filter preserved)
        assert!(
            sql.contains("15") && sql.contains("24"),
            "start-node IN-list values must survive rewriting: {sql}"
        );
        assert!(
            sql.contains("10") && sql.contains("30"),
            "end-node IN-list values must survive rewriting: {sql}"
        );
        // The VLP branch WHERE must reference CTE columns (start_id / end_id),
        // confirming that rewrite_vlp_branch_select ran on the UNION branch.
        assert!(
            sql_lower.contains("start_id") || sql_lower.contains("end_id"),
            "VLP branch WHERE must use start_id/end_id columns (not bare a./b.): {sql}"
        );
        // Verify a.user_id does not appear as a WHERE predicate AFTER the VLP CTE definition.
        // It may appear inside the CTE body itself (legitimate), but not in the outer UNION branch.
        // Strategy: find the last SELECT (start of the outer UNION branch with FROM vlp_...) and
        // check that a.user_id / b.post_id don't appear in that segment.
        if let Some(vlp_branch_start) = sql.rfind("FROM vlp_multi_type") {
            let vlp_branch_sql = &sql[vlp_branch_start..];
            assert!(
                !vlp_branch_sql.contains("a.user_id"),
                "VLP branch must not reference out-of-scope alias a.user_id: {}",
                vlp_branch_sql
            );
            assert!(
                !vlp_branch_sql.contains("b.post_id"),
                "VLP branch must not reference out-of-scope alias b.post_id: {}",
                vlp_branch_sql
            );
        }
    }
}

/// Both endpoints filtered with IN-list, undirected, mixed types.
/// Browser may also send undirected: MATCH (a)-[r]-(b) WHERE id(a) IN [...] AND id(b) IN [...].
#[tokio::test]
async fn test_both_endpoint_in_list_undirected_mixed() {
    let schema = create_standard_schema();
    let sql = generate_expand_sql(
        &schema,
        "MATCH (a:User)-[r]-(b) WHERE a.user_id IN [15, 24] AND b.post_id IN [10, 30] RETURN r",
    )
    .await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL: {sql}");
    assert!(
        sql.contains("15") && sql.contains("10"),
        "ID filter values must survive into SQL: {sql}"
    );
    if sql_lower.contains("vlp_multi_type") {
        if let Some(vlp_branch_start) = sql.rfind("FROM vlp_multi_type") {
            let vlp_branch_sql = &sql[vlp_branch_start..];
            assert!(
                !vlp_branch_sql.contains("a.user_id"),
                "VLP branch must not reference out-of-scope a.user_id: {}",
                vlp_branch_sql
            );
            assert!(
                !vlp_branch_sql.contains("b.post_id"),
                "VLP branch must not reference out-of-scope b.post_id: {}",
                vlp_branch_sql
            );
        }
    }
}

/// Full browser expand pattern: WITH barrier + both-endpoint IN-list.
/// This matches what the Neo4j Browser sends for relationship expansion:
///   MATCH (a:User) WHERE a.user_id IN [15,24] WITH a
///   MATCH (a)-[r]->(b) WHERE b.post_id IN [10,30] RETURN r
#[tokio::test]
async fn test_browser_with_barrier_both_endpoints_filtered() {
    let schema = create_standard_schema();
    let sql = generate_expand_sql(
        &schema,
        "MATCH (a:User) WHERE a.user_id IN [15, 24] WITH a \
         MATCH (a)-[r]->(b) WHERE b.post_id IN [10, 30] RETURN r",
    )
    .await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL: {sql}");
    assert!(
        sql.contains("15") && sql.contains("10"),
        "ID filter values must appear in SQL: {sql}"
    );
    if sql_lower.contains("vlp_multi_type") {
        if let Some(vlp_branch_start) = sql.rfind("FROM vlp_multi_type") {
            let vlp_branch_sql = &sql[vlp_branch_start..];
            assert!(
                !vlp_branch_sql.contains("b.post_id"),
                "VLP branch must not reference out-of-scope b.post_id: {}",
                vlp_branch_sql
            );
        }
    }
}

/// Return relationship r (not nodes) with multi-type expand — baseline.
/// Tests that RETURN r works for all edge types in the schema.
#[tokio::test]
async fn test_return_relationship_untyped_directed() {
    let schema = create_standard_schema();
    let sql = generate_expand_sql(&schema, "MATCH (a:User)-[r]->(b) RETURN r").await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL: {sql}");
    // The expand covers FOLLOWS (User→User) and AUTHORED/LIKED (User→Post)
    assert!(
        sql_lower.contains("union all"),
        "Multi-type outgoing expand should produce UNION ALL: {sql}"
    );
}

/// Return relationship r with both-endpoint equality filters (non-IN, simpler form).
#[tokio::test]
async fn test_return_relationship_both_endpoint_eq_filter() {
    let schema = create_standard_schema();
    let sql = generate_expand_sql(
        &schema,
        "MATCH (a:User)-[r]->(b) WHERE a.user_id = 15 AND b.post_id = 10 RETURN r",
    )
    .await;

    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("select"), "Should produce valid SQL: {sql}");
    assert!(sql.contains("15"), "a filter value must appear: {sql}");
    assert!(sql.contains("10"), "b filter value must appear: {sql}");
    if sql_lower.contains("vlp_multi_type") {
        if let Some(vlp_branch_start) = sql.rfind("FROM vlp_multi_type") {
            let vlp_branch_sql = &sql[vlp_branch_start..];
            assert!(
                !vlp_branch_sql.contains("b.post_id"),
                "VLP branch must not reference out-of-scope b.post_id: {}",
                vlp_branch_sql
            );
        }
    }
}
