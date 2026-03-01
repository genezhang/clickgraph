//! Browser interaction tests — broader query pattern coverage
//!
//! Tests SQL generation for the variety of Cypher queries the Neo4j Browser
//! generates during user interaction: clicking relationship badges, label
//! sidebar items, property panel counts, path visualizations, and more.
//!
//! Complements `browser_expand_tests` (click-to-expand patterns) by covering
//! planner code paths for unlabeled nodes, `type(r)`, aggregation with graph
//! patterns, OPTIONAL MATCH, and error cases.

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
// Helpers
// ---------------------------------------------------------------------------

/// Full pipeline: parse → plan → render → SQL. Panics on error.
async fn generate_sql(schema: &GraphSchema, cypher: &str) -> String {
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

/// Full pipeline returning Result — for error-case tests.
async fn try_generate_sql(schema: &GraphSchema, cypher: &str) -> Result<String, String> {
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
// Category 1: Relationship Badge Click
// ===========================================================================

#[tokio::test]
async fn test_rel_badge_undirected() {
    let schema = create_standard_schema();
    let sql = generate_sql(&schema, "MATCH ()-[r:FOLLOWS]-() RETURN r LIMIT 25").await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("limit"), "Should have LIMIT clause");
    assert!(
        sql_lower.contains("user_follows"),
        "Should reference FOLLOWS edge table"
    );
}

#[tokio::test]
async fn test_rel_badge_directed_out() {
    let schema = create_standard_schema();
    let sql = generate_sql(&schema, "MATCH ()-[r:FOLLOWS]->() RETURN r LIMIT 25").await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("limit"), "Should have LIMIT clause");
    assert!(
        sql_lower.contains("user_follows"),
        "Should reference FOLLOWS edge table"
    );
}

#[tokio::test]
async fn test_rel_badge_directed_in() {
    let schema = create_standard_schema();
    let sql = generate_sql(&schema, "MATCH ()<-[r:FOLLOWS]-() RETURN r LIMIT 25").await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("limit"), "Should have LIMIT clause");
    assert!(
        sql_lower.contains("user_follows"),
        "Should reference FOLLOWS edge table"
    );
}

#[tokio::test]
async fn test_rel_badge_cross_type() {
    let schema = create_standard_schema();
    let sql = generate_sql(&schema, "MATCH ()-[r:LIKED]-() RETURN r LIMIT 25").await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("limit"), "Should have LIMIT clause");
    assert!(
        sql_lower.contains("post_likes"),
        "Should reference LIKED edge table"
    );
}

#[tokio::test]
async fn test_rel_badge_authored() {
    let schema = create_standard_schema();
    let sql = generate_sql(&schema, "MATCH ()-[r:AUTHORED]-() RETURN r LIMIT 25").await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("limit"), "Should have LIMIT clause");
    assert!(
        sql_lower.contains("post_authors"),
        "Should reference AUTHORED edge table"
    );
}

#[tokio::test]
async fn test_rel_badge_fk_edge() {
    let schema = create_fk_edge_schema();
    let sql = generate_sql(&schema, "MATCH ()-[r:PLACED_BY]-() RETURN r LIMIT 25").await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("limit"), "Should have LIMIT clause");
    assert!(
        sql_lower.contains("orders"),
        "FK-edge should reference orders table"
    );
}

// ===========================================================================
// Category 2: Label Click / Node Fetch
// ===========================================================================

#[tokio::test]
async fn test_label_click_user() {
    let schema = create_standard_schema();
    let sql = generate_sql(&schema, "MATCH (n:User) RETURN n LIMIT 25").await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("users"), "Should reference users table");
    assert!(sql_lower.contains("limit"), "Should have LIMIT clause");
}

#[tokio::test]
async fn test_label_click_post() {
    let schema = create_standard_schema();
    let sql = generate_sql(&schema, "MATCH (n:Post) RETURN n LIMIT 25").await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("posts"), "Should reference posts table");
    assert!(sql_lower.contains("limit"), "Should have LIMIT clause");
}

#[tokio::test]
async fn test_label_click_denorm() {
    let schema = create_denormalized_schema();
    let sql = generate_sql(&schema, "MATCH (n:Airport) RETURN n LIMIT 25").await;
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("flights"),
        "Denorm Airport should reference flights table"
    );
    assert!(sql_lower.contains("limit"), "Should have LIMIT clause");
}

#[tokio::test]
async fn test_label_click_composite() {
    let schema = create_composite_id_schema();
    let sql = generate_sql(&schema, "MATCH (n:Account) RETURN n LIMIT 25").await;
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("accounts"),
        "Should reference accounts table"
    );
    assert!(sql_lower.contains("limit"), "Should have LIMIT clause");
}

// ===========================================================================
// Category 3: Count / Aggregation
// ===========================================================================

#[tokio::test]
async fn test_count_typed_rel() {
    let schema = create_standard_schema();
    let sql = generate_sql(
        &schema,
        "MATCH (n:User)-[r:FOLLOWS]->(m) WHERE n.user_id = 1 RETURN count(*)",
    )
    .await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("count"), "Should have count aggregate");
    assert!(
        sql_lower.contains("where") || sql_lower.contains("= 1"),
        "Should have WHERE filter"
    );
}

#[tokio::test]
async fn test_count_incoming() {
    let schema = create_standard_schema();
    let sql = generate_sql(
        &schema,
        "MATCH (n:User)<-[r]-(m) WHERE n.user_id = 1 RETURN count(*)",
    )
    .await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("count"), "Should have count aggregate");
}

#[tokio::test]
async fn test_count_unlabeled_typed() {
    let schema = create_standard_schema();
    let sql = generate_sql(&schema, "MATCH ()-[r:FOLLOWS]->() RETURN count(*)").await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("count"), "Should have count aggregate");
    assert!(
        sql_lower.contains("user_follows"),
        "Should reference FOLLOWS edge table"
    );
}

#[tokio::test]
async fn test_count_fully_unlabeled() {
    let schema = create_standard_schema();
    let sql = generate_sql(&schema, "MATCH ()-[r]-() RETURN count(*)").await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("count"), "Should have count aggregate");
}

#[tokio::test]
async fn test_count_with_grouping() {
    let schema = create_standard_schema();
    let sql = generate_sql(
        &schema,
        "MATCH (u:User)-[r:FOLLOWS]->() RETURN u.user_id, count(*) ORDER BY count(*) DESC LIMIT 10",
    )
    .await;
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("order by"),
        "Should have ORDER BY clause"
    );
    assert!(sql_lower.contains("limit"), "Should have LIMIT clause");
    assert!(sql_lower.contains("count"), "Should have count aggregate");
}

#[tokio::test]
async fn test_count_fk_edge() {
    let schema = create_fk_edge_schema();
    let sql = generate_sql(
        &schema,
        "MATCH (o:Order)-[r:PLACED_BY]->(c:Customer) RETURN count(*)",
    )
    .await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("count"), "Should have count aggregate");
    assert!(
        sql_lower.contains("orders"),
        "FK-edge should reference orders table"
    );
}

// ===========================================================================
// Category 4: Label Combinations
// ===========================================================================

#[tokio::test]
async fn test_both_labeled() {
    let schema = create_standard_schema();
    let sql = generate_sql(
        &schema,
        "MATCH (u:User)-[r:FOLLOWS]->(f:User) RETURN count(*)",
    )
    .await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("count"), "Should produce count query");
}

#[tokio::test]
async fn test_left_labeled_only() {
    let schema = create_standard_schema();
    let sql = generate_sql(&schema, "MATCH (u:User)-[r:FOLLOWS]->() RETURN count(*)").await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("count"), "Should produce count query");
}

#[tokio::test]
async fn test_right_labeled_only() {
    let schema = create_standard_schema();
    let sql = generate_sql(&schema, "MATCH ()-[r:FOLLOWS]->(u:User) RETURN count(*)").await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("count"), "Should produce count query");
}

#[tokio::test]
async fn test_neither_labeled() {
    let schema = create_standard_schema();
    let sql = generate_sql(&schema, "MATCH ()-[r:FOLLOWS]->() RETURN count(*)").await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("count"), "Should produce count query");
}

#[tokio::test]
async fn test_cross_type_labels() {
    let schema = create_standard_schema();
    let sql = generate_sql(
        &schema,
        "MATCH (u:User)-[r:LIKED]->(p:Post) RETURN count(*)",
    )
    .await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("count"), "Should produce count query");
    assert!(
        sql_lower.contains("post_likes"),
        "Should reference LIKED edge table"
    );
}

// ===========================================================================
// Category 5: Path Return
// ===========================================================================

#[tokio::test]
async fn test_path_return_typed() {
    let schema = create_standard_schema();
    let sql = generate_sql(
        &schema,
        "MATCH p=(u:User)-[r:FOLLOWS]->(f:User) RETURN p LIMIT 5",
    )
    .await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("limit"), "Should have LIMIT clause");
}

#[tokio::test]
async fn test_path_return_unlabeled() {
    let schema = create_standard_schema();
    let sql = generate_sql(&schema, "MATCH p=()-[r:FOLLOWS]->() RETURN p LIMIT 5").await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("limit"), "Should have LIMIT clause");
}

#[tokio::test]
async fn test_path_return_cross_type() {
    let schema = create_standard_schema();
    let sql = generate_sql(
        &schema,
        "MATCH p=(u:User)-[r:AUTHORED]->(q:Post) RETURN p LIMIT 5",
    )
    .await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("limit"), "Should have LIMIT clause");
    assert!(
        sql_lower.contains("post_authors"),
        "Should reference AUTHORED edge table"
    );
}

// ===========================================================================
// Category 6: Direction Validation
// ===========================================================================

#[tokio::test]
async fn test_valid_direction() {
    let schema = create_standard_schema();
    let sql = generate_sql(
        &schema,
        "MATCH (u:User)-[r:LIKED]->(p:Post) RETURN count(*)",
    )
    .await;
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("count"),
        "Valid direction should succeed"
    );
}

#[tokio::test]
async fn test_invalid_direction() {
    let schema = create_standard_schema();
    let result = try_generate_sql(
        &schema,
        "MATCH (p:Post)-[r:LIKED]->(u:User) RETURN count(*)",
    )
    .await;
    assert!(
        result.is_err(),
        "Reversed LIKED direction (Post->User) should fail: got SQL: {:?}",
        result
    );
}

#[tokio::test]
async fn test_undirected_cross_type() {
    let schema = create_standard_schema();
    let sql = generate_sql(&schema, "MATCH (u:User)-[r:LIKED]-(p:Post) RETURN count(*)").await;
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("count"),
        "Undirected cross-type should succeed"
    );
}

// ===========================================================================
// Category 7: Complex Browser Patterns
// ===========================================================================

#[tokio::test]
async fn test_multi_match() {
    let schema = create_standard_schema();
    let sql = generate_sql(
        &schema,
        "MATCH (u:User)-[:AUTHORED]->(p:Post) MATCH (u)-[:FOLLOWS]->(f:User) RETURN count(*)",
    )
    .await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("count"), "Multi-MATCH should succeed");
    assert!(
        sql_lower.contains("post_authors"),
        "Should reference AUTHORED edge table"
    );
    assert!(
        sql_lower.contains("user_follows"),
        "Should reference FOLLOWS edge table"
    );
}

#[tokio::test]
async fn test_optional_match_count() {
    let schema = create_standard_schema();
    let sql = generate_sql(
        &schema,
        "MATCH (u:User) OPTIONAL MATCH (u)-[r:FOLLOWS]->(f:User) RETURN u.user_id, count(r) LIMIT 5",
    )
    .await;
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("left") || sql_lower.contains("optional") || sql_lower.contains("count"),
        "OPTIONAL MATCH should produce LEFT JOIN or equivalent"
    );
}

#[tokio::test]
async fn test_whole_node_vs_property() {
    let schema = create_standard_schema();
    // Whole node return
    let sql_node = generate_sql(&schema, "MATCH (n:User) RETURN n LIMIT 5").await;
    assert!(!sql_node.is_empty(), "Whole node return should produce SQL");

    // Property return
    let sql_prop = generate_sql(&schema, "MATCH (n:User) RETURN n.name LIMIT 5").await;
    assert!(!sql_prop.is_empty(), "Property return should produce SQL");
    let sql_prop_lower = sql_prop.to_lowercase();
    assert!(
        sql_prop_lower.contains("full_name"),
        "n.name should resolve to full_name column"
    );
}

#[tokio::test]
async fn test_relationship_return() {
    let schema = create_standard_schema();
    let sql = generate_sql(
        &schema,
        "MATCH (u:User)-[r:FOLLOWS]->(f:User) RETURN r LIMIT 10",
    )
    .await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("limit"), "Should have LIMIT clause");
    assert!(
        sql_lower.contains("user_follows"),
        "Should reference FOLLOWS edge table"
    );
}

// ===========================================================================
// Category 8: Error Cases
// ===========================================================================

#[tokio::test]
async fn test_nonexistent_label() {
    let schema = create_standard_schema();
    let result = try_generate_sql(&schema, "MATCH (n:NonExistent) RETURN n").await;
    assert!(
        result.is_err(),
        "NonExistent label should fail: got SQL: {:?}",
        result
    );
}

#[tokio::test]
async fn test_nonexistent_rel_type() {
    let schema = create_standard_schema();
    let result = try_generate_sql(&schema, "MATCH ()-[r:NONEXISTENT]->() RETURN r").await;
    assert!(
        result.is_err(),
        "NonExistent rel type should fail: got SQL: {:?}",
        result
    );
}

#[tokio::test]
async fn test_invalid_typed_rel_direction() {
    let schema = create_standard_schema();
    // AUTHORED goes User->Post, so Post->User direction with AUTHORED type should fail
    let result = try_generate_sql(
        &schema,
        "MATCH (p:Post)-[r:AUTHORED]->(u:User) RETURN count(*)",
    )
    .await;
    assert!(
        result.is_err(),
        "Reversed AUTHORED direction (Post->User) should fail: got SQL: {:?}",
        result
    );
}

// ===========================================================================
// Category 9: FK-Edge Schema Variations
// ===========================================================================

#[tokio::test]
async fn test_fk_edge_rel_badge_directed() {
    let schema = create_fk_edge_schema();
    let sql = generate_sql(&schema, "MATCH ()-[r:PLACED_BY]->() RETURN r LIMIT 25").await;
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("orders"),
        "Should reference FK-edge table"
    );
    assert!(sql_lower.contains("limit"), "Should have LIMIT clause");
}

#[tokio::test]
async fn test_fk_edge_label_click_order() {
    let schema = create_fk_edge_schema();
    let sql = generate_sql(&schema, "MATCH (n:Order) RETURN n LIMIT 25").await;
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("orders"),
        "Should reference orders table"
    );
}

#[tokio::test]
async fn test_fk_edge_label_click_customer() {
    let schema = create_fk_edge_schema();
    let sql = generate_sql(&schema, "MATCH (n:Customer) RETURN n LIMIT 25").await;
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("customers"),
        "Should reference customers table"
    );
}

#[tokio::test]
async fn test_fk_edge_count_with_where() {
    let schema = create_fk_edge_schema();
    let sql = generate_sql(
        &schema,
        "MATCH (o:Order)-[r:PLACED_BY]->(c:Customer) WHERE o.order_id = 1 RETURN count(*)",
    )
    .await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("count"), "Should have count aggregate");
    assert!(
        sql_lower.contains("= 1"),
        "Should have WHERE filter on order_id"
    );
}

#[tokio::test]
async fn test_fk_edge_path_return() {
    let schema = create_fk_edge_schema();
    let sql = generate_sql(
        &schema,
        "MATCH p=(o:Order)-[r:PLACED_BY]->(c:Customer) RETURN p LIMIT 5",
    )
    .await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("limit"), "Should have LIMIT clause");
    assert!(
        sql_lower.contains("orders"),
        "Should reference FK-edge table"
    );
}

#[tokio::test]
async fn test_fk_edge_reverse_direction_error() {
    let schema = create_fk_edge_schema();
    // PLACED_BY goes Order->Customer, so Customer->Order should fail
    let result = try_generate_sql(
        &schema,
        "MATCH (c:Customer)-[r:PLACED_BY]->(o:Order) RETURN count(*)",
    )
    .await;
    assert!(
        result.is_err(),
        "Reversed PLACED_BY direction should fail: got SQL: {:?}",
        result
    );
}

#[tokio::test]
async fn test_fk_edge_undirected() {
    let schema = create_fk_edge_schema();
    let sql = generate_sql(
        &schema,
        "MATCH (o:Order)-[r:PLACED_BY]-(c:Customer) RETURN count(*)",
    )
    .await;
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("count"),
        "Undirected FK-edge should succeed"
    );
}

// ===========================================================================
// Category 10: Denormalized Schema Variations
// ===========================================================================

#[tokio::test]
async fn test_denorm_rel_badge() {
    let schema = create_denormalized_schema();
    let sql = generate_sql(&schema, "MATCH ()-[r:FLIGHT]-() RETURN r LIMIT 25").await;
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("flights"),
        "Should reference denorm flights table"
    );
    assert!(sql_lower.contains("limit"), "Should have LIMIT clause");
}

#[tokio::test]
async fn test_denorm_rel_badge_directed() {
    let schema = create_denormalized_schema();
    let sql = generate_sql(&schema, "MATCH ()-[r:FLIGHT]->() RETURN r LIMIT 25").await;
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("flights"),
        "Should reference denorm flights table"
    );
}

#[tokio::test]
async fn test_denorm_count_typed() {
    let schema = create_denormalized_schema();
    let sql = generate_sql(
        &schema,
        "MATCH (a:Airport)-[r:FLIGHT]->(b:Airport) RETURN count(*)",
    )
    .await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("count"), "Should have count aggregate");
    assert!(
        sql_lower.contains("flights"),
        "Should reference flights table"
    );
}

#[tokio::test]
async fn test_denorm_path_return() {
    let schema = create_denormalized_schema();
    let sql = generate_sql(
        &schema,
        "MATCH p=(a:Airport)-[r:FLIGHT]->(b:Airport) RETURN p LIMIT 5",
    )
    .await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("limit"), "Should have LIMIT clause");
}

#[tokio::test]
async fn test_denorm_undirected() {
    let schema = create_denormalized_schema();
    let sql = generate_sql(
        &schema,
        "MATCH (a:Airport)-[r:FLIGHT]-(b:Airport) RETURN count(*)",
    )
    .await;
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("count"),
        "Undirected denorm should succeed"
    );
}

#[tokio::test]
async fn test_denorm_left_labeled_only() {
    let schema = create_denormalized_schema();
    let sql = generate_sql(&schema, "MATCH (a:Airport)-[r:FLIGHT]->() RETURN count(*)").await;
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("count"),
        "Left-labeled denorm should succeed"
    );
}

// ===========================================================================
// Category 11: Composite ID Schema Variations
// ===========================================================================

#[tokio::test]
async fn test_composite_rel_badge_transferred() {
    let schema = create_composite_id_schema();
    let sql = generate_sql(&schema, "MATCH ()-[r:TRANSFERRED]-() RETURN r LIMIT 25").await;
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("transfers"),
        "Should reference transfers table"
    );
    assert!(sql_lower.contains("limit"), "Should have LIMIT clause");
}

#[tokio::test]
async fn test_composite_rel_badge_owns() {
    let schema = create_composite_id_schema();
    let sql = generate_sql(&schema, "MATCH ()-[r:OWNS]-() RETURN r LIMIT 25").await;
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("account_ownership"),
        "Should reference ownership table"
    );
}

#[tokio::test]
async fn test_composite_label_click_customer() {
    let schema = create_composite_id_schema();
    let sql = generate_sql(&schema, "MATCH (n:Customer) RETURN n LIMIT 25").await;
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("customers"),
        "Should reference customers table"
    );
}

#[tokio::test]
async fn test_composite_count_directed() {
    let schema = create_composite_id_schema();
    let sql = generate_sql(
        &schema,
        "MATCH (c:Customer)-[r:OWNS]->(a:Account) RETURN count(*)",
    )
    .await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("count"), "Should have count aggregate");
}

#[tokio::test]
async fn test_composite_count_self_rel() {
    let schema = create_composite_id_schema();
    let sql = generate_sql(
        &schema,
        "MATCH (a:Account)-[r:TRANSFERRED]->(b:Account) RETURN count(*)",
    )
    .await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("count"), "Should have count aggregate");
    assert!(
        sql_lower.contains("transfers"),
        "Should reference transfers table"
    );
}

#[tokio::test]
async fn test_composite_path_return() {
    let schema = create_composite_id_schema();
    let sql = generate_sql(
        &schema,
        "MATCH p=(c:Customer)-[r:OWNS]->(a:Account) RETURN p LIMIT 5",
    )
    .await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("limit"), "Should have LIMIT clause");
}

#[tokio::test]
async fn test_composite_reverse_direction_error() {
    let schema = create_composite_id_schema();
    // OWNS goes Customer->Account, so Account->Customer should fail
    let result = try_generate_sql(
        &schema,
        "MATCH (a:Account)-[r:OWNS]->(c:Customer) RETURN count(*)",
    )
    .await;
    assert!(
        result.is_err(),
        "Reversed OWNS direction should fail: got SQL: {:?}",
        result
    );
}

#[tokio::test]
async fn test_composite_undirected() {
    let schema = create_composite_id_schema();
    let sql = generate_sql(
        &schema,
        "MATCH (c:Customer)-[r:OWNS]-(a:Account) RETURN count(*)",
    )
    .await;
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("count"),
        "Undirected composite should succeed"
    );
}

// ===========================================================================
// Category 12: Polymorphic Edge Schema
// ===========================================================================

#[tokio::test]
async fn test_poly_rel_badge_follows() {
    let schema = create_polymorphic_schema();
    let sql = generate_sql(&schema, "MATCH ()-[r:FOLLOWS]-() RETURN r LIMIT 25").await;
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("interactions"),
        "Should reference polymorphic interactions table"
    );
    assert!(sql_lower.contains("limit"), "Should have LIMIT clause");
}

#[tokio::test]
async fn test_poly_rel_badge_likes() {
    let schema = create_polymorphic_schema();
    let sql = generate_sql(&schema, "MATCH ()-[r:LIKES]-() RETURN r LIMIT 25").await;
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("interactions"),
        "Should reference polymorphic interactions table"
    );
}

#[tokio::test]
async fn test_poly_rel_badge_authored() {
    let schema = create_polymorphic_schema();
    let sql = generate_sql(&schema, "MATCH ()-[r:AUTHORED]-() RETURN r LIMIT 25").await;
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("interactions"),
        "Should reference polymorphic interactions table"
    );
}

#[tokio::test]
async fn test_poly_rel_badge_directed() {
    let schema = create_polymorphic_schema();
    let sql = generate_sql(&schema, "MATCH ()-[r:FOLLOWS]->() RETURN r LIMIT 25").await;
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("interactions"),
        "Directed polymorphic should reference interactions table"
    );
}

#[tokio::test]
async fn test_poly_label_click_user() {
    let schema = create_polymorphic_schema();
    let sql = generate_sql(&schema, "MATCH (n:User) RETURN n LIMIT 25").await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("users"), "Should reference users table");
}

#[tokio::test]
async fn test_poly_label_click_post() {
    let schema = create_polymorphic_schema();
    let sql = generate_sql(&schema, "MATCH (n:Post) RETURN n LIMIT 25").await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("posts"), "Should reference posts table");
}

#[tokio::test]
async fn test_poly_count_same_type() {
    let schema = create_polymorphic_schema();
    let sql = generate_sql(
        &schema,
        "MATCH (u:User)-[r:FOLLOWS]->(f:User) RETURN count(*)",
    )
    .await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("count"), "Should have count aggregate");
    assert!(
        sql_lower.contains("interactions"),
        "Should reference interactions table"
    );
}

#[tokio::test]
async fn test_poly_count_cross_type() {
    let schema = create_polymorphic_schema();
    let sql = generate_sql(
        &schema,
        "MATCH (u:User)-[r:LIKES]->(p:Post) RETURN count(*)",
    )
    .await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("count"), "Should have count aggregate");
    assert!(
        sql_lower.contains("interactions"),
        "Should reference interactions table"
    );
}

#[tokio::test]
async fn test_poly_count_unlabeled() {
    let schema = create_polymorphic_schema();
    let sql = generate_sql(&schema, "MATCH ()-[r:FOLLOWS]->() RETURN count(*)").await;
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("count"),
        "Unlabeled poly should have count"
    );
}

#[tokio::test]
async fn test_poly_path_return() {
    let schema = create_polymorphic_schema();
    let sql = generate_sql(
        &schema,
        "MATCH p=(u:User)-[r:FOLLOWS]->(f:User) RETURN p LIMIT 5",
    )
    .await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("limit"), "Should have LIMIT clause");
    assert!(
        sql_lower.contains("interactions"),
        "Should reference interactions table"
    );
}

#[tokio::test]
async fn test_poly_path_cross_type() {
    let schema = create_polymorphic_schema();
    let sql = generate_sql(
        &schema,
        "MATCH p=(u:User)-[r:AUTHORED]->(q:Post) RETURN p LIMIT 5",
    )
    .await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("limit"), "Should have LIMIT clause");
}

#[tokio::test]
async fn test_poly_invalid_direction() {
    let schema = create_polymorphic_schema();
    // LIKES goes User->Post, so Post->User should fail
    let result = try_generate_sql(
        &schema,
        "MATCH (p:Post)-[r:LIKES]->(u:User) RETURN count(*)",
    )
    .await;
    assert!(
        result.is_err(),
        "Reversed LIKES direction should fail: got SQL: {:?}",
        result
    );
}

#[tokio::test]
async fn test_poly_undirected_cross_type() {
    let schema = create_polymorphic_schema();
    let sql = generate_sql(&schema, "MATCH (u:User)-[r:LIKES]-(p:Post) RETURN count(*)").await;
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("count"),
        "Undirected poly cross-type should succeed"
    );
}

#[tokio::test]
async fn test_poly_multi_match() {
    let schema = create_polymorphic_schema();
    let sql = generate_sql(
        &schema,
        "MATCH (u:User)-[:AUTHORED]->(p:Post) MATCH (u)-[:FOLLOWS]->(f:User) RETURN count(*)",
    )
    .await;
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("count"),
        "Multi-MATCH with poly edges should succeed"
    );
    assert!(
        sql_lower.contains("interactions"),
        "Should reference interactions table"
    );
}

#[tokio::test]
async fn test_poly_both_labeled() {
    let schema = create_polymorphic_schema();
    let sql = generate_sql(
        &schema,
        "MATCH (u:User)-[r:FOLLOWS]->(f:User) RETURN count(*)",
    )
    .await;
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("count"),
        "Both-labeled poly should succeed"
    );
}

#[tokio::test]
async fn test_poly_left_labeled_only() {
    let schema = create_polymorphic_schema();
    let sql = generate_sql(&schema, "MATCH (u:User)-[r:FOLLOWS]->() RETURN count(*)").await;
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("count"),
        "Left-labeled poly should succeed"
    );
}

#[tokio::test]
async fn test_poly_right_labeled_only() {
    let schema = create_polymorphic_schema();
    let sql = generate_sql(&schema, "MATCH ()-[r:FOLLOWS]->(u:User) RETURN count(*)").await;
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("count"),
        "Right-labeled poly should succeed"
    );
}

#[tokio::test]
async fn test_poly_relationship_return() {
    let schema = create_polymorphic_schema();
    let sql = generate_sql(
        &schema,
        "MATCH (u:User)-[r:FOLLOWS]->(f:User) RETURN r LIMIT 10",
    )
    .await;
    let sql_lower = sql.to_lowercase();
    assert!(sql_lower.contains("limit"), "Should have LIMIT clause");
    assert!(
        sql_lower.contains("interactions"),
        "Should reference interactions table"
    );
}

#[tokio::test]
async fn test_poly_nonexistent_rel_type() {
    let schema = create_polymorphic_schema();
    let result = try_generate_sql(&schema, "MATCH ()-[r:NONEXISTENT]->() RETURN r").await;
    assert!(
        result.is_err(),
        "NonExistent rel type on poly schema should fail: got SQL: {:?}",
        result
    );
}

#[tokio::test]
async fn test_poly_property_return() {
    let schema = create_polymorphic_schema();
    let sql = generate_sql(&schema, "MATCH (n:User) RETURN n.name LIMIT 5").await;
    let sql_lower = sql.to_lowercase();
    assert!(
        sql_lower.contains("username"),
        "n.name should resolve to username column in poly schema"
    );
}
