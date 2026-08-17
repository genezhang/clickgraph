//! Cross-Schema Pattern Tests
//!
//! Verify that LDBC-class query patterns generate valid SQL across all 5 schema
//! variations: Standard, FK-edge, Denormalized, Polymorphic, Composite ID.
//!
//! Phase 1: Structural SQL assertions — verify correct tables, columns, JOINs, filters.
//! No ClickHouse connection needed — SQL generation only.

use std::sync::Arc;

use clickgraph::{
    graph_catalog::{config::GraphSchemaConfig, graph_schema::GraphSchema},
    query_planner::evaluate_read_statement,
    render_plan::{logical_plan_to_render_plan_with_ctx, ToSql},
    server::query_context::{set_current_schema, with_query_context, QueryContext},
};

// ---------------------------------------------------------------------------
// Schema paths
// ---------------------------------------------------------------------------

const SCHEMA_STANDARD: &str = "schemas/test/cross_schema/cs_standard.yaml";
const SCHEMA_FK_EDGE: &str = "schemas/test/cross_schema/cs_fk_edge.yaml";
const SCHEMA_DENORMALIZED: &str = "schemas/test/cross_schema/cs_denormalized.yaml";
const SCHEMA_POLYMORPHIC: &str = "schemas/test/cross_schema/cs_polymorphic.yaml";
const SCHEMA_COMPOSITE_ID: &str = "schemas/test/cross_schema/cs_composite_id.yaml";

// ---------------------------------------------------------------------------
// 12 Query Patterns
// ---------------------------------------------------------------------------

/// P01: Multi-hop chain (3-way JOIN)
const P01_MULTI_HOP: &str =
    "MATCH (u:User)-[:AUTHORED]->(p:Post)-[:HAS_TAG]->(t:Tag) RETURN u.name, p.content, t.name LIMIT 10";

/// P02: Undirected edge (BidirectionalUnion UNION ALL)
const P02_UNDIRECTED: &str = "MATCH (a:User)-[:FOLLOWS]-(b:User) RETURN a.name, b.name LIMIT 10";

/// P03: WITH + downstream MATCH (CTE barrier + re-join)
const P03_WITH_MATCH: &str = "\
MATCH (u:User)-[:AUTHORED]->(p:Post) \
WITH u, count(p) AS postCount \
MATCH (u)-[:LIVES_IN]->(c:City) \
RETURN u.name, postCount, c.name";

/// P04: Aggregation + ORDER BY (GROUP BY + ORDER)
const P04_AGG_ORDER: &str = "\
MATCH (u:User)-[:AUTHORED]->(p:Post) \
RETURN u.name, count(p) AS cnt ORDER BY cnt DESC LIMIT 5";

/// P05: OPTIONAL MATCH (LEFT JOIN)
const P05_OPTIONAL: &str = "\
MATCH (u:User) \
OPTIONAL MATCH (u)-[:AUTHORED]->(p:Post) \
RETURN u.name, p.content";

/// P06: Variable-length path (Recursive CTE)
const P06_VLP: &str = "MATCH (a:User)-[:FOLLOWS*1..3]->(b:User) RETURN a.name, b.name LIMIT 10";

/// P07: collect() aggregation (groupArray + DISTINCT)
const P07_COLLECT: &str = "\
MATCH (u:User)-[:AUTHORED]->(p:Post)-[:HAS_TAG]->(t:Tag) \
RETURN u.name, collect(DISTINCT t.name) AS tags";

/// P08: Multi-pattern MATCH (CartesianProduct / multi-pattern)
const P08_MULTI_PATTERN: &str = "\
MATCH (u:User)-[:AUTHORED]->(p:Post), (u)-[:LIVES_IN]->(c:City) \
RETURN u.name, p.content, c.name LIMIT 10";

/// P09: WHERE after WITH (Post-WITH filter)
const P09_WHERE_AFTER_WITH: &str = "\
MATCH (u:User)-[:AUTHORED]->(p:Post) \
WITH u, count(p) AS cnt \
WHERE cnt > 5 \
RETURN u.name, cnt";

/// P10: ORDER BY + LIMIT in WITH (Non-trivial WITH)
const P10_WITH_ORDER_LIMIT: &str = "\
MATCH (u:User)-[:AUTHORED]->(p:Post) \
WITH u, count(p) AS cnt ORDER BY cnt DESC LIMIT 10 \
RETURN u.name, cnt";

/// P11: CASE expression in projection
const P11_CASE: &str = "\
MATCH (u:User)-[:AUTHORED]->(p:Post) \
RETURN u.name, CASE WHEN p.date > '2024-01-01' THEN 'recent' ELSE 'old' END AS status";

/// P12: Inline property filter ({prop: value})
const P12_INLINE_FILTER: &str = "\
MATCH (u:User {name: 'Alice'})-[:AUTHORED]->(p:Post) \
RETURN u.name, p.content";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn load_schema(path: &str) -> GraphSchema {
    let config = GraphSchemaConfig::from_yaml_file(path)
        .unwrap_or_else(|e| panic!("Failed to load schema {}: {:?}", path, e));
    config
        .to_graph_schema()
        .unwrap_or_else(|e| panic!("Failed to convert schema {}: {:?}", path, e))
}

async fn generate_sql(schema: &GraphSchema, cypher: &str) -> String {
    let schema = schema.clone();
    let cypher = cypher.to_string();

    let ctx = QueryContext::new(Some("default".to_string()));
    with_query_context(ctx, async {
        set_current_schema(Arc::new(schema.clone()));

        let (_remaining, statement) =
            clickgraph::open_cypher_parser::parse_cypher_statement(&cypher)
                .unwrap_or_else(|e| panic!("Failed to parse Cypher: {:?}\nQuery: {}", e, cypher));

        let (logical_plan, plan_ctx) =
            evaluate_read_statement(statement, &schema, None, None, None)
                .unwrap_or_else(|e| panic!("Failed to plan: {:?}\nQuery: {}", e, cypher));

        let render_plan =
            logical_plan_to_render_plan_with_ctx(logical_plan, &schema, Some(&plan_ctx))
                .unwrap_or_else(|e| panic!("Failed to render: {:?}\nQuery: {}", e, cypher));
        render_plan.to_sql()
    })
    .await
}

/// Fallible variant of [`generate_sql`]: returns the analyzer/render error as a
/// String instead of panicking, for tests that assert a pattern is loud-gated.
#[allow(dead_code)] // retained for loud-gate assertions (e.g. #544 multi-VLP)
async fn try_generate_sql(schema: &GraphSchema, cypher: &str) -> Result<String, String> {
    let schema = schema.clone();
    let cypher = cypher.to_string();

    let ctx = QueryContext::new(Some("default".to_string()));
    with_query_context(ctx, async {
        set_current_schema(Arc::new(schema.clone()));

        let (_remaining, statement) =
            clickgraph::open_cypher_parser::parse_cypher_statement(&cypher)
                .map_err(|e| format!("parse error: {e:?}"))?;

        let (logical_plan, plan_ctx) =
            evaluate_read_statement(statement, &schema, None, None, None)
                .map_err(|e| format!("{e}"))?;

        let render_plan =
            logical_plan_to_render_plan_with_ctx(logical_plan, &schema, Some(&plan_ctx))
                .map_err(|e| format!("{e}"))?;
        Ok(render_plan.to_sql())
    })
    .await
}

/// Basic assertion: SQL was generated and contains SELECT.
fn assert_valid_sql(sql: &str, schema_name: &str, pattern: &str) {
    assert!(
        !sql.is_empty(),
        "[{}] {} produced empty SQL",
        schema_name,
        pattern
    );
    assert!(
        sql.contains("SELECT"),
        "[{}] {} missing SELECT:\n{}",
        schema_name,
        pattern,
        sql
    );
}

fn assert_contains(sql: &str, ctx: &str, needle: &str) {
    assert!(
        sql.contains(needle),
        "[{}] expected SQL to contain {:?}.\nSQL:\n{}",
        ctx,
        needle,
        sql
    );
}

fn assert_not_contains(sql: &str, ctx: &str, needle: &str) {
    assert!(
        !sql.contains(needle),
        "[{}] expected SQL to NOT contain {:?}.\nSQL:\n{}",
        ctx,
        needle,
        sql
    );
}

// ===========================================================================
// Standard schema tests — baseline table/column name verification
// ===========================================================================

#[tokio::test]
async fn cs_standard_p01_multi_hop() {
    let schema = load_schema(SCHEMA_STANDARD);
    let sql = generate_sql(&schema, P01_MULTI_HOP).await;
    assert_valid_sql(&sql, "standard", "P01");
    // Separate edge tables
    assert_contains(&sql, "standard/P01", "cs_test.authored");
    assert_contains(&sql, "standard/P01", "cs_test.post_tags");
    // Property mappings: Cypher name→column name
    assert_contains(&sql, "standard/P01", "full_name");
    assert_contains(&sql, "standard/P01", "tag_name");
}

#[tokio::test]
async fn cs_standard_p02_undirected() {
    let schema = load_schema(SCHEMA_STANDARD);
    let sql = generate_sql(&schema, P02_UNDIRECTED).await;
    assert_valid_sql(&sql, "standard", "P02");
    assert_contains(&sql, "standard/P02", "UNION ALL");
    assert_contains(&sql, "standard/P02", "cs_test.follows");
    assert_contains(&sql, "standard/P02", "follower_id");
    assert_contains(&sql, "standard/P02", "followed_id");
}

#[tokio::test]
async fn cs_standard_p03_with_match() {
    let schema = load_schema(SCHEMA_STANDARD);
    let sql = generate_sql(&schema, P03_WITH_MATCH).await;
    assert_valid_sql(&sql, "standard", "P03");
    // CTE from WITH barrier
    assert_contains(&sql, "standard/P03", " AS (SELECT");
    // Downstream LIVES_IN join
    assert_contains(&sql, "standard/P03", "cs_test.lives_in");
    assert_contains(&sql, "standard/P03", "cs_test.cities");
    assert_contains(&sql, "standard/P03", "city_name");
}

#[tokio::test]
async fn cs_standard_p04_agg_order() {
    let schema = load_schema(SCHEMA_STANDARD);
    let sql = generate_sql(&schema, P04_AGG_ORDER).await;
    assert_valid_sql(&sql, "standard", "P04");
    assert_contains(&sql, "standard/P04", "cs_test.authored");
    assert_contains(&sql, "standard/P04", "GROUP BY");
    assert_contains(&sql, "standard/P04", "ORDER BY");
    assert_contains(&sql, "standard/P04", "LIMIT");
}

#[tokio::test]
async fn cs_standard_p05_optional() {
    let schema = load_schema(SCHEMA_STANDARD);
    let sql = generate_sql(&schema, P05_OPTIONAL).await;
    assert_valid_sql(&sql, "standard", "P05");
    assert_contains(&sql, "standard/P05", "LEFT JOIN");
    assert_contains(&sql, "standard/P05", "cs_test.authored");
    assert_contains(&sql, "standard/P05", "cs_test.posts");
}

#[tokio::test]
async fn cs_standard_p06_vlp() {
    let schema = load_schema(SCHEMA_STANDARD);
    let sql = generate_sql(&schema, P06_VLP).await;
    assert_valid_sql(&sql, "standard", "P06");
    assert_contains(&sql, "standard/P06", "RECURSIVE");
    assert_contains(&sql, "standard/P06", "vlp_");
    assert_contains(&sql, "standard/P06", "cs_test.follows");
    assert_contains(&sql, "standard/P06", "hop_count");
}

#[tokio::test]
async fn cs_standard_p07_collect() {
    let schema = load_schema(SCHEMA_STANDARD);
    let sql = generate_sql(&schema, P07_COLLECT).await;
    assert_valid_sql(&sql, "standard", "P07");
    assert_contains(&sql, "standard/P07", "groupArray");
    assert_contains(&sql, "standard/P07", "DISTINCT");
    assert_contains(&sql, "standard/P07", "tag_name");
    assert_contains(&sql, "standard/P07", "cs_test.post_tags");
}

#[tokio::test]
async fn cs_standard_p08_multi_pattern() {
    let schema = load_schema(SCHEMA_STANDARD);
    let sql = generate_sql(&schema, P08_MULTI_PATTERN).await;
    assert_valid_sql(&sql, "standard", "P08");
    assert_contains(&sql, "standard/P08", "cs_test.authored");
    assert_contains(&sql, "standard/P08", "cs_test.lives_in");
    assert_contains(&sql, "standard/P08", "cs_test.cities");
    assert_contains(&sql, "standard/P08", "city_name");
}

#[tokio::test]
async fn cs_standard_p09_where_after_with() {
    let schema = load_schema(SCHEMA_STANDARD);
    let sql = generate_sql(&schema, P09_WHERE_AFTER_WITH).await;
    assert_valid_sql(&sql, "standard", "P09");
    // CTE with HAVING for post-WITH filter
    assert_contains(&sql, "standard/P09", " AS (SELECT");
    assert_contains(&sql, "standard/P09", "HAVING");
    assert_contains(&sql, "standard/P09", "> 5");
}

#[tokio::test]
async fn cs_standard_p10_with_order_limit() {
    let schema = load_schema(SCHEMA_STANDARD);
    let sql = generate_sql(&schema, P10_WITH_ORDER_LIMIT).await;
    assert_valid_sql(&sql, "standard", "P10");
    assert_contains(&sql, "standard/P10", " AS (SELECT");
    assert_contains(&sql, "standard/P10", "ORDER BY");
    assert_contains(&sql, "standard/P10", "LIMIT 10");
}

#[tokio::test]
async fn cs_standard_p11_case() {
    let schema = load_schema(SCHEMA_STANDARD);
    let sql = generate_sql(&schema, P11_CASE).await;
    assert_valid_sql(&sql, "standard", "P11");
    assert_contains(&sql, "standard/P11", "CASE");
    assert_contains(&sql, "standard/P11", "created_at");
    assert_contains(&sql, "standard/P11", "'recent'");
    assert_contains(&sql, "standard/P11", "'old'");
}

#[tokio::test]
async fn cs_standard_p12_inline_filter() {
    let schema = load_schema(SCHEMA_STANDARD);
    let sql = generate_sql(&schema, P12_INLINE_FILTER).await;
    assert_valid_sql(&sql, "standard", "P12");
    assert_contains(&sql, "standard/P12", "full_name");
    assert_contains(&sql, "standard/P12", "'Alice'");
    assert_contains(&sql, "standard/P12", "cs_test.authored");
}

// ===========================================================================
// FK-edge schema tests
// AUTHORED uses posts table (FK pattern) — no separate authored edge table.
// ===========================================================================

#[tokio::test]
async fn cs_fk_edge_p01_multi_hop() {
    let schema = load_schema(SCHEMA_FK_EDGE);
    let sql = generate_sql(&schema, P01_MULTI_HOP).await;
    assert_valid_sql(&sql, "fk_edge", "P01");
    // FK-edge: AUTHORED edge IS the posts table — no separate authored table
    assert_not_contains(&sql, "fk_edge/P01", "cs_test.authored");
    assert_contains(&sql, "fk_edge/P01", "cs_test.posts");
    // User JOIN via FK column in posts table
    assert_contains(&sql, "fk_edge/P01", "user_id");
    assert_contains(&sql, "fk_edge/P01", "full_name");
    assert_contains(&sql, "fk_edge/P01", "tag_name");
}

#[tokio::test]
async fn cs_fk_edge_p02_undirected() {
    let schema = load_schema(SCHEMA_FK_EDGE);
    let sql = generate_sql(&schema, P02_UNDIRECTED).await;
    assert_valid_sql(&sql, "fk_edge", "P02");
    assert_contains(&sql, "fk_edge/P02", "UNION ALL");
    // FOLLOWS is still a standard edge table
    assert_contains(&sql, "fk_edge/P02", "cs_test.follows");
}

#[tokio::test]
async fn cs_fk_edge_p03_with_match() {
    let schema = load_schema(SCHEMA_FK_EDGE);
    let sql = generate_sql(&schema, P03_WITH_MATCH).await;
    assert_valid_sql(&sql, "fk_edge", "P03");
    assert_not_contains(&sql, "fk_edge/P03", "cs_test.authored");
    assert_contains(&sql, "fk_edge/P03", "cs_test.posts");
    assert_contains(&sql, "fk_edge/P03", "cs_test.cities");
}

#[tokio::test]
async fn cs_fk_edge_p04_agg_order() {
    let schema = load_schema(SCHEMA_FK_EDGE);
    let sql = generate_sql(&schema, P04_AGG_ORDER).await;
    assert_valid_sql(&sql, "fk_edge", "P04");
    assert_not_contains(&sql, "fk_edge/P04", "cs_test.authored");
    assert_contains(&sql, "fk_edge/P04", "cs_test.posts");
    assert_contains(&sql, "fk_edge/P04", "GROUP BY");
    assert_contains(&sql, "fk_edge/P04", "ORDER BY");
}

#[tokio::test]
async fn cs_fk_edge_p05_optional() {
    let schema = load_schema(SCHEMA_FK_EDGE);
    let sql = generate_sql(&schema, P05_OPTIONAL).await;
    assert_valid_sql(&sql, "fk_edge", "P05");
    assert_contains(&sql, "fk_edge/P05", "LEFT JOIN");
    assert_not_contains(&sql, "fk_edge/P05", "cs_test.authored");
}

#[tokio::test]
async fn cs_fk_edge_p06_vlp() {
    let schema = load_schema(SCHEMA_FK_EDGE);
    let sql = generate_sql(&schema, P06_VLP).await;
    assert_valid_sql(&sql, "fk_edge", "P06");
    // VLP on FOLLOWS (standard edge) — should still generate recursive CTE
    assert_contains(&sql, "fk_edge/P06", "RECURSIVE");
    assert_contains(&sql, "fk_edge/P06", "cs_test.follows");
}

#[tokio::test]
async fn cs_fk_edge_p07_collect() {
    let schema = load_schema(SCHEMA_FK_EDGE);
    let sql = generate_sql(&schema, P07_COLLECT).await;
    assert_valid_sql(&sql, "fk_edge", "P07");
    assert_not_contains(&sql, "fk_edge/P07", "cs_test.authored");
    assert_contains(&sql, "fk_edge/P07", "groupArray");
}

#[tokio::test]
async fn cs_fk_edge_p08_multi_pattern() {
    let schema = load_schema(SCHEMA_FK_EDGE);
    let sql = generate_sql(&schema, P08_MULTI_PATTERN).await;
    assert_valid_sql(&sql, "fk_edge", "P08");
    assert_not_contains(&sql, "fk_edge/P08", "cs_test.authored");
    assert_contains(&sql, "fk_edge/P08", "cs_test.posts");
    assert_contains(&sql, "fk_edge/P08", "cs_test.cities");
}

#[tokio::test]
async fn cs_fk_edge_p09_where_after_with() {
    let schema = load_schema(SCHEMA_FK_EDGE);
    let sql = generate_sql(&schema, P09_WHERE_AFTER_WITH).await;
    assert_valid_sql(&sql, "fk_edge", "P09");
    assert_not_contains(&sql, "fk_edge/P09", "cs_test.authored");
    assert_contains(&sql, "fk_edge/P09", "HAVING");
}

#[tokio::test]
async fn cs_fk_edge_p10_with_order_limit() {
    let schema = load_schema(SCHEMA_FK_EDGE);
    let sql = generate_sql(&schema, P10_WITH_ORDER_LIMIT).await;
    assert_valid_sql(&sql, "fk_edge", "P10");
    assert_not_contains(&sql, "fk_edge/P10", "cs_test.authored");
}

#[tokio::test]
async fn cs_fk_edge_p11_case() {
    let schema = load_schema(SCHEMA_FK_EDGE);
    let sql = generate_sql(&schema, P11_CASE).await;
    assert_valid_sql(&sql, "fk_edge", "P11");
    assert_not_contains(&sql, "fk_edge/P11", "cs_test.authored");
    assert_contains(&sql, "fk_edge/P11", "CASE");
}

#[tokio::test]
async fn cs_fk_edge_p12_inline_filter() {
    let schema = load_schema(SCHEMA_FK_EDGE);
    let sql = generate_sql(&schema, P12_INLINE_FILTER).await;
    assert_valid_sql(&sql, "fk_edge", "P12");
    assert_not_contains(&sql, "fk_edge/P12", "cs_test.authored");
    assert_contains(&sql, "fk_edge/P12", "'Alice'");
}

// ===========================================================================
// Denormalized schema tests
// City is denormalized into lives_in table — uses to_node_properties columns.
// Patterns not involving City are identical to standard.
// ===========================================================================

#[tokio::test]
async fn cs_denormalized_p01_multi_hop() {
    let schema = load_schema(SCHEMA_DENORMALIZED);
    let sql = generate_sql(&schema, P01_MULTI_HOP).await;
    assert_valid_sql(&sql, "denormalized", "P01");
    // No City involved — same as standard
    assert_contains(&sql, "denormalized/P01", "cs_test.authored");
    assert_contains(&sql, "denormalized/P01", "cs_test.post_tags");
}

#[tokio::test]
async fn cs_denormalized_p02_undirected() {
    let schema = load_schema(SCHEMA_DENORMALIZED);
    let sql = generate_sql(&schema, P02_UNDIRECTED).await;
    assert_valid_sql(&sql, "denormalized", "P02");
    assert_contains(&sql, "denormalized/P02", "UNION ALL");
}

#[tokio::test]
async fn cs_denormalized_p03_with_match() {
    let schema = load_schema(SCHEMA_DENORMALIZED);
    let sql = generate_sql(&schema, P03_WITH_MATCH).await;
    assert_valid_sql(&sql, "denormalized", "P03");
    // City is denormalized — properties come from lives_in table columns
    assert_contains(&sql, "denormalized/P03", "to_city_name");
    // No separate cities table
    assert_not_contains(&sql, "denormalized/P03", "cs_test.cities");
}

#[tokio::test]
async fn cs_denormalized_p04_agg_order() {
    let schema = load_schema(SCHEMA_DENORMALIZED);
    let sql = generate_sql(&schema, P04_AGG_ORDER).await;
    assert_valid_sql(&sql, "denormalized", "P04");
    assert_contains(&sql, "denormalized/P04", "GROUP BY");
    assert_contains(&sql, "denormalized/P04", "ORDER BY");
}

#[tokio::test]
async fn cs_denormalized_p05_optional() {
    let schema = load_schema(SCHEMA_DENORMALIZED);
    let sql = generate_sql(&schema, P05_OPTIONAL).await;
    assert_valid_sql(&sql, "denormalized", "P05");
    assert_contains(&sql, "denormalized/P05", "LEFT JOIN");
}

#[tokio::test]
async fn cs_denormalized_p06_vlp() {
    let schema = load_schema(SCHEMA_DENORMALIZED);
    let sql = generate_sql(&schema, P06_VLP).await;
    assert_valid_sql(&sql, "denormalized", "P06");
    assert_contains(&sql, "denormalized/P06", "RECURSIVE");
}

#[tokio::test]
async fn cs_denormalized_p07_collect() {
    let schema = load_schema(SCHEMA_DENORMALIZED);
    let sql = generate_sql(&schema, P07_COLLECT).await;
    assert_valid_sql(&sql, "denormalized", "P07");
    assert_contains(&sql, "denormalized/P07", "groupArray");
}

#[tokio::test]
async fn cs_denormalized_p08_multi_pattern() {
    let schema = load_schema(SCHEMA_DENORMALIZED);
    let sql = generate_sql(&schema, P08_MULTI_PATTERN).await;
    assert_valid_sql(&sql, "denormalized", "P08");
    // City is denormalized — to_node_properties column used
    assert_contains(&sql, "denormalized/P08", "to_city_name");
    // No separate cities table
    assert_not_contains(&sql, "denormalized/P08", "cs_test.cities");
    // Lives_in IS the city table
    assert_contains(&sql, "denormalized/P08", "cs_test.lives_in");
}

#[tokio::test]
async fn cs_denormalized_p09_where_after_with() {
    let schema = load_schema(SCHEMA_DENORMALIZED);
    let sql = generate_sql(&schema, P09_WHERE_AFTER_WITH).await;
    assert_valid_sql(&sql, "denormalized", "P09");
    assert_contains(&sql, "denormalized/P09", "HAVING");
}

#[tokio::test]
async fn cs_denormalized_p10_with_order_limit() {
    let schema = load_schema(SCHEMA_DENORMALIZED);
    let sql = generate_sql(&schema, P10_WITH_ORDER_LIMIT).await;
    assert_valid_sql(&sql, "denormalized", "P10");
    assert_contains(&sql, "denormalized/P10", "ORDER BY");
}

#[tokio::test]
async fn cs_denormalized_p11_case() {
    let schema = load_schema(SCHEMA_DENORMALIZED);
    let sql = generate_sql(&schema, P11_CASE).await;
    assert_valid_sql(&sql, "denormalized", "P11");
    assert_contains(&sql, "denormalized/P11", "CASE");
}

#[tokio::test]
async fn cs_denormalized_p12_inline_filter() {
    let schema = load_schema(SCHEMA_DENORMALIZED);
    let sql = generate_sql(&schema, P12_INLINE_FILTER).await;
    assert_valid_sql(&sql, "denormalized", "P12");
    assert_contains(&sql, "denormalized/P12", "'Alice'");
}

// ===========================================================================
// Polymorphic schema tests
// All edges in single `interactions` table with type_column discrimination.
// No individual edge tables should appear.
// ===========================================================================

#[tokio::test]
async fn cs_polymorphic_p01_multi_hop() {
    let schema = load_schema(SCHEMA_POLYMORPHIC);
    let sql = generate_sql(&schema, P01_MULTI_HOP).await;
    assert_valid_sql(&sql, "polymorphic", "P01");
    // All edges through interactions table
    assert_contains(&sql, "polymorphic/P01", "cs_test.interactions");
    // Type discriminators present
    assert_contains(&sql, "polymorphic/P01", "'AUTHORED'");
    assert_contains(&sql, "polymorphic/P01", "'HAS_TAG'");
    // Label columns used for type safety
    assert_contains(&sql, "polymorphic/P01", "from_type");
    assert_contains(&sql, "polymorphic/P01", "to_type");
    // No individual edge tables
    assert_not_contains(&sql, "polymorphic/P01", "cs_test.authored");
    assert_not_contains(&sql, "polymorphic/P01", "cs_test.post_tags");
}

#[tokio::test]
async fn cs_polymorphic_p02_undirected() {
    let schema = load_schema(SCHEMA_POLYMORPHIC);
    let sql = generate_sql(&schema, P02_UNDIRECTED).await;
    assert_valid_sql(&sql, "polymorphic", "P02");
    assert_contains(&sql, "polymorphic/P02", "UNION ALL");
    assert_contains(&sql, "polymorphic/P02", "cs_test.interactions");
    assert_contains(&sql, "polymorphic/P02", "'FOLLOWS'");
    assert_not_contains(&sql, "polymorphic/P02", "cs_test.follows");
}

#[tokio::test]
async fn cs_polymorphic_p03_with_match() {
    let schema = load_schema(SCHEMA_POLYMORPHIC);
    let sql = generate_sql(&schema, P03_WITH_MATCH).await;
    assert_valid_sql(&sql, "polymorphic", "P03");
    assert_contains(&sql, "polymorphic/P03", "cs_test.interactions");
    assert_contains(&sql, "polymorphic/P03", "'AUTHORED'");
    assert_contains(&sql, "polymorphic/P03", "'LIVES_IN'");
    assert_not_contains(&sql, "polymorphic/P03", "cs_test.authored");
    assert_not_contains(&sql, "polymorphic/P03", "cs_test.lives_in");
}

#[tokio::test]
async fn cs_polymorphic_p04_agg_order() {
    let schema = load_schema(SCHEMA_POLYMORPHIC);
    let sql = generate_sql(&schema, P04_AGG_ORDER).await;
    assert_valid_sql(&sql, "polymorphic", "P04");
    assert_contains(&sql, "polymorphic/P04", "cs_test.interactions");
    assert_contains(&sql, "polymorphic/P04", "'AUTHORED'");
    assert_contains(&sql, "polymorphic/P04", "GROUP BY");
}

#[tokio::test]
async fn cs_polymorphic_p05_optional() {
    let schema = load_schema(SCHEMA_POLYMORPHIC);
    let sql = generate_sql(&schema, P05_OPTIONAL).await;
    assert_valid_sql(&sql, "polymorphic", "P05");
    assert_contains(&sql, "polymorphic/P05", "LEFT JOIN");
    assert_contains(&sql, "polymorphic/P05", "cs_test.interactions");
    assert_contains(&sql, "polymorphic/P05", "'AUTHORED'");
}

#[tokio::test]
async fn cs_polymorphic_p06_vlp() {
    let schema = load_schema(SCHEMA_POLYMORPHIC);
    let sql = generate_sql(&schema, P06_VLP).await;
    assert_valid_sql(&sql, "polymorphic", "P06");
    assert_contains(&sql, "polymorphic/P06", "RECURSIVE");
    assert_contains(&sql, "polymorphic/P06", "cs_test.interactions");
    assert_contains(&sql, "polymorphic/P06", "'FOLLOWS'");
}

#[tokio::test]
async fn cs_polymorphic_p07_collect() {
    let schema = load_schema(SCHEMA_POLYMORPHIC);
    let sql = generate_sql(&schema, P07_COLLECT).await;
    assert_valid_sql(&sql, "polymorphic", "P07");
    assert_contains(&sql, "polymorphic/P07", "cs_test.interactions");
    assert_contains(&sql, "polymorphic/P07", "groupArray");
    assert_not_contains(&sql, "polymorphic/P07", "cs_test.post_tags");
}

#[tokio::test]
async fn cs_polymorphic_p08_multi_pattern() {
    let schema = load_schema(SCHEMA_POLYMORPHIC);
    let sql = generate_sql(&schema, P08_MULTI_PATTERN).await;
    assert_valid_sql(&sql, "polymorphic", "P08");
    assert_contains(&sql, "polymorphic/P08", "cs_test.interactions");
    assert_contains(&sql, "polymorphic/P08", "'AUTHORED'");
    assert_contains(&sql, "polymorphic/P08", "'LIVES_IN'");
}

#[tokio::test]
async fn cs_polymorphic_p09_where_after_with() {
    let schema = load_schema(SCHEMA_POLYMORPHIC);
    let sql = generate_sql(&schema, P09_WHERE_AFTER_WITH).await;
    assert_valid_sql(&sql, "polymorphic", "P09");
    assert_contains(&sql, "polymorphic/P09", "cs_test.interactions");
    assert_contains(&sql, "polymorphic/P09", "HAVING");
}

#[tokio::test]
async fn cs_polymorphic_p10_with_order_limit() {
    let schema = load_schema(SCHEMA_POLYMORPHIC);
    let sql = generate_sql(&schema, P10_WITH_ORDER_LIMIT).await;
    assert_valid_sql(&sql, "polymorphic", "P10");
    assert_contains(&sql, "polymorphic/P10", "cs_test.interactions");
}

#[tokio::test]
async fn cs_polymorphic_p11_case() {
    let schema = load_schema(SCHEMA_POLYMORPHIC);
    let sql = generate_sql(&schema, P11_CASE).await;
    assert_valid_sql(&sql, "polymorphic", "P11");
    assert_contains(&sql, "polymorphic/P11", "cs_test.interactions");
    assert_contains(&sql, "polymorphic/P11", "CASE");
}

#[tokio::test]
async fn cs_polymorphic_p12_inline_filter() {
    let schema = load_schema(SCHEMA_POLYMORPHIC);
    let sql = generate_sql(&schema, P12_INLINE_FILTER).await;
    assert_valid_sql(&sql, "polymorphic", "P12");
    assert_contains(&sql, "polymorphic/P12", "cs_test.interactions");
    assert_contains(&sql, "polymorphic/P12", "'Alice'");
    assert_contains(&sql, "polymorphic/P12", "'AUTHORED'");
}

// ===========================================================================
// Composite ID schema tests
// User has composite [org_id, user_id] key — JOINs must use both columns.
// ===========================================================================

#[tokio::test]
async fn cs_composite_id_p01_multi_hop() {
    let schema = load_schema(SCHEMA_COMPOSITE_ID);
    let sql = generate_sql(&schema, P01_MULTI_HOP).await;
    assert_valid_sql(&sql, "composite_id", "P01");
    // Composite key: AUTHORED join uses both org_id and user_id
    assert_contains(&sql, "composite_id/P01", "org_id");
    assert_contains(&sql, "composite_id/P01", "user_id");
    // Still uses standard edge table names
    assert_contains(&sql, "composite_id/P01", "cs_test.authored");
}

#[tokio::test]
async fn cs_composite_id_p02_undirected() {
    let schema = load_schema(SCHEMA_COMPOSITE_ID);
    let sql = generate_sql(&schema, P02_UNDIRECTED).await;
    assert_valid_sql(&sql, "composite_id", "P02");
    assert_contains(&sql, "composite_id/P02", "UNION ALL");
    // Composite key in FOLLOWS join
    assert_contains(&sql, "composite_id/P02", "org_id");
    assert_contains(&sql, "composite_id/P02", "follower_id");
    assert_contains(&sql, "composite_id/P02", "followed_id");
}

#[tokio::test]
async fn cs_composite_id_p03_with_match() {
    let schema = load_schema(SCHEMA_COMPOSITE_ID);
    let sql = generate_sql(&schema, P03_WITH_MATCH).await;
    assert_valid_sql(&sql, "composite_id", "P03");
    assert_contains(&sql, "composite_id/P03", "org_id");
}

#[tokio::test]
async fn cs_composite_id_p04_agg_order() {
    let schema = load_schema(SCHEMA_COMPOSITE_ID);
    let sql = generate_sql(&schema, P04_AGG_ORDER).await;
    assert_valid_sql(&sql, "composite_id", "P04");
    assert_contains(&sql, "composite_id/P04", "org_id");
    assert_contains(&sql, "composite_id/P04", "GROUP BY");
    assert_contains(&sql, "composite_id/P04", "ORDER BY");
}

#[tokio::test]
async fn cs_composite_id_p05_optional() {
    let schema = load_schema(SCHEMA_COMPOSITE_ID);
    let sql = generate_sql(&schema, P05_OPTIONAL).await;
    assert_valid_sql(&sql, "composite_id", "P05");
    assert_contains(&sql, "composite_id/P05", "LEFT JOIN");
    assert_contains(&sql, "composite_id/P05", "org_id");
}

#[tokio::test]
async fn cs_composite_id_p06_vlp() {
    let schema = load_schema(SCHEMA_COMPOSITE_ID);
    let sql = generate_sql(&schema, P06_VLP).await;
    assert_valid_sql(&sql, "composite_id", "P06");
    // VLP with composite keys
    assert_contains(&sql, "composite_id/P06", "org_id");
    assert_contains(&sql, "composite_id/P06", "cs_test.follows");
}

#[tokio::test]
async fn cs_composite_id_p07_collect() {
    let schema = load_schema(SCHEMA_COMPOSITE_ID);
    let sql = generate_sql(&schema, P07_COLLECT).await;
    assert_valid_sql(&sql, "composite_id", "P07");
    assert_contains(&sql, "composite_id/P07", "groupArray");
    assert_contains(&sql, "composite_id/P07", "org_id");
}

#[tokio::test]
async fn cs_composite_id_p08_multi_pattern() {
    let schema = load_schema(SCHEMA_COMPOSITE_ID);
    let sql = generate_sql(&schema, P08_MULTI_PATTERN).await;
    assert_valid_sql(&sql, "composite_id", "P08");
    assert_contains(&sql, "composite_id/P08", "org_id");
    assert_contains(&sql, "composite_id/P08", "cs_test.lives_in");
}

#[tokio::test]
async fn cs_composite_id_p09_where_after_with() {
    let schema = load_schema(SCHEMA_COMPOSITE_ID);
    let sql = generate_sql(&schema, P09_WHERE_AFTER_WITH).await;
    assert_valid_sql(&sql, "composite_id", "P09");
    assert_contains(&sql, "composite_id/P09", "HAVING");
}

#[tokio::test]
async fn cs_composite_id_p10_with_order_limit() {
    let schema = load_schema(SCHEMA_COMPOSITE_ID);
    let sql = generate_sql(&schema, P10_WITH_ORDER_LIMIT).await;
    assert_valid_sql(&sql, "composite_id", "P10");
    assert_contains(&sql, "composite_id/P10", "ORDER BY");
}

#[tokio::test]
async fn cs_composite_id_p11_case() {
    let schema = load_schema(SCHEMA_COMPOSITE_ID);
    let sql = generate_sql(&schema, P11_CASE).await;
    assert_valid_sql(&sql, "composite_id", "P11");
    assert_contains(&sql, "composite_id/P11", "CASE");
}

#[tokio::test]
async fn cs_composite_id_p12_inline_filter() {
    let schema = load_schema(SCHEMA_COMPOSITE_ID);
    let sql = generate_sql(&schema, P12_INLINE_FILTER).await;
    assert_valid_sql(&sql, "composite_id", "P12");
    assert_contains(&sql, "composite_id/P12", "'Alice'");
    assert_contains(&sql, "composite_id/P12", "org_id");
}

// ===========================================================================
// #1081 — anonymous-relationship alias collision with user variables
// ===========================================================================

/// #1081 root cause: anonymous nodes/edges are auto-aliased from a shared
/// `t{N}` counter (`logical_plan::generate_id`). When a query explicitly names a
/// variable `t2`, the generated alias for an adjacent anonymous relationship
/// collided with it, and that relationship's composite-key label
/// (`AUTHORED::User::Post`) overwrote node `t2`'s real label — surfacing as
/// `AnalyzerError: Invalid relationship pattern:
/// (AUTHORED::User::Post)-[:AUTHORED::User::Post]->(Post)`.
///
/// A variable-length hop forces adjacent anonymous relationships, which is how
/// the GraphRAG demo's `(t)-[:RELATED_TO*1..2]->(t2)-[:DISCUSSES]->(d)` tripped
/// it live. The failure is deterministic whenever a user variable matches a
/// generated `t{N}` alias; it looked "intermittent" in production only because
/// different hand-typed queries used different variable names. The fix makes
/// anonymous alias generation skip any alias the user (or plan context) already
/// uses. Regression guard: this exact shape must plan.
#[tokio::test]
async fn cs_standard_1081_anonymous_alias_collision() {
    let schema = load_schema(SCHEMA_STANDARD);
    // Endpoint deliberately named `t2` — collides with the generated alias for
    // the anonymous `-[:AUTHORED]->` relationship unless generation avoids it.
    // Start node is `a`, NOT `t`: a VLP endpoint literally named `t` is a
    // separate collision with the VLP CTE FROM alias, now handled by the
    // query-scoped alias (#1088, see
    // `cs_standard_1088_vlp_endpoint_named_t_is_query_scoped`); this test
    // isolates the #1081 anonymous-relationship-alias collision.
    let cypher = "MATCH path = (a:User)-[:FOLLOWS*1..2]->(t2:User)-[:AUTHORED]->(d:Post) \
                  RETURN t2.name, d.content, length(path) AS hops LIMIT 5";
    let sql = generate_sql(&schema, cypher).await;
    assert_valid_sql(&sql, "standard", "1081-alias-collision");
    // `t2` must resolve as the User node, and the AUTHORED edge table is present.
    assert_contains(&sql, "standard/1081", "cs_test.authored");
}

/// #1088 (was #1085): a variable-length-path endpoint named exactly `t` used to
/// collide with the VLP CTE FROM alias (`VLP_CTE_FROM_ALIAS = "t"`), which is
/// threaded as a string-level protocol through the render/emit phases — a chained
/// hop after the VLP then silently anchored on the VLP *start* instead of the
/// endpoint. #1085 rejected the shape loudly as an interim measure; #1088 makes
/// the FROM alias query-scoped, so a user variable named `t` bumps it to a
/// collision-free value (`vlpt`) and the query now runs CORRECTLY. Regression
/// guard: the VLP CTE is aliased `vlpt` and the chained AUTHORED hop joins the
/// VLP *endpoint* (`vlpt.end_id`), never a bare `t.end_id`/`t.start_id`.
#[tokio::test]
async fn cs_standard_1088_vlp_endpoint_named_t_is_query_scoped() {
    let schema = load_schema(SCHEMA_STANDARD);
    let cypher = "MATCH (t:User)-[:FOLLOWS*1..2]->(t2:User)-[:AUTHORED]->(d:Post) \
                  RETURN d.content, t2.name";
    let sql = generate_sql(&schema, cypher).await;
    assert_valid_sql(&sql, "standard", "1088-vlp-endpoint-t");
    // FROM alias bumped away from the colliding user variable `t`.
    assert_contains(&sql, "standard/1088", "AS vlpt");
    // The chained hop must anchor on the VLP ENDPOINT via the bumped alias
    // (the #1085 bug anchored on the start instead).
    assert_contains(&sql, "standard/1088", "vlpt.end_id");
    assert_contains(&sql, "standard/1088", "vlpt.end_name");
    // No bare `t`-aliased VLP FROM clause should remain. (`assert_not_contains`
    // on `t.end_id` is unsafe — it is a substring of the correct `vlpt.end_id`;
    // check the FROM alias instead, which cannot be a substring of `AS vlpt`.)
    assert_not_contains(&sql, "standard/1088", "AS t ");
    assert_not_contains(&sql, "standard/1088", "AS t\n");
}

/// #1081-adjacent latent bug: `GraphSchema::rel_type_index` (and the
/// denormalized-node metadata) are `#[serde(skip)]`, so a schema recovered from
/// the `graph_catalog` JSON persistence path / the `monitor_schema_updates`
/// refresh came back with an EMPTY index. A relationship then looked up by
/// SIMPLE type name — as the variable-length recursive-CTE path does, having no
/// endpoint labels to reconstruct the composite key — could not be resolved
/// (`RelationshipTypeNotFound`). Independent of the alias-collision bug above;
/// the aliases here (`u`/`u2`/`p`) do not collide. The fix rebuilds the derived
/// indexes on deserialize.
#[tokio::test]
async fn cs_standard_1081_vlp_chain_after_serde_roundtrip() {
    let built = load_schema(SCHEMA_STANDARD);
    let json = serde_json::to_string(&built).expect("schema should serialize");
    let restored: GraphSchema = serde_json::from_str(&json).expect("schema should deserialize");

    let cypher = "MATCH path = (u:User)-[:FOLLOWS*1..2]->(u2:User)-[:AUTHORED]->(p:Post) \
                  RETURN u.name, p.content LIMIT 5";
    let sql = generate_sql(&restored, cypher).await;
    assert_valid_sql(
        &sql,
        "standard(serde-round-tripped)",
        "1081-serde-vlp-chain",
    );
}

/// #1084: the #1081 fix collected anonymous-alias avoidance only per connected
/// pattern (plus whatever was already bound in `plan_ctx`). A user variable in a
/// *later* comma-separated pattern is not yet bound while an *earlier* pattern is
/// lowered, so its name was not protected — an anonymous alias generated for the
/// first pattern could still collide with it across the comma boundary. Here the
/// first pattern's anonymous end node draws the generated alias `t1` (the counter
/// resets to 1 per query) while the second pattern names a node `t1`. The fix
/// reserves every user-declared variable query-wide up front, so generation skips
/// `t1`. Regression guard: both comma patterns must lower — proven by both edge
/// tables appearing — with the user's `t1` resolving as its own `User` node.
#[tokio::test]
async fn cs_standard_1084_cross_comma_anonymous_alias_collision() {
    let schema = load_schema(SCHEMA_STANDARD);
    let cypher = "MATCH (a:User)-[:FOLLOWS]->(), (t1:User)-[:AUTHORED]->(d:Post) \
                  RETURN t1.name, d.content LIMIT 5";
    let sql = generate_sql(&schema, cypher).await;
    assert_valid_sql(&sql, "standard", "1084-cross-comma-alias");
    // Both relationship tables must be present: proof both comma patterns lowered.
    assert_contains(&sql, "standard/1084", "cs_test.authored");
    assert_contains(&sql, "standard/1084", "cs_test.follows");
    // The two comma-separated patterns are disconnected, so the user's `t1` node
    // must join as an independent cartesian product (`AS t1 ON 1 = 1`). Under the
    // pre-#1084 collision the first pattern's anonymous end node took alias `t1`
    // and FUSED with the user's node — `t1` would instead be joined on the FOLLOWS
    // relationship's `followed_id`. Assert the cartesian join to lock the fix.
    assert_contains(&sql, "standard/1084", "AS t1 ON 1 = 1");
}
