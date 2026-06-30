//! Regression test for unlabeled path/expand patterns over a POLYMORPHIC edge.
//!
//! A polymorphic edge uses one physical table to hold many edge types, with
//! generic (`$any`) endpoints resolved at query time via `from_label_column` /
//! `to_label_column`. When a Neo4j-Browser-style query leaves BOTH endpoints
//! unlabeled — e.g. `MATCH p=()-[:SHARED]->()` — the planner used to fail to
//! bind the endpoints (there is no concrete `from_node`/`to_node` to scan) and
//! pruned the entire pattern to the `SELECT 1 AS "_empty" WHERE false`
//! placeholder, so the query returned nothing regardless of the data.
//!
//! The fix routes single-type polymorphic patterns through the same
//! deferred-UNION path as multi-type patterns: `$any` endpoints expand to the
//! concrete node labels, producing a real query over the edge table filtered by
//! the requested type for each (from, type, to) combination. The result may
//! legitimately be empty for a type with no rows, but the SQL must be a real
//! query against the edge table, never the `_empty` placeholder.

use crate::{
    clickhouse_query_generator, graph_catalog::config::GraphSchemaConfig,
    graph_catalog::graph_schema::GraphSchema, open_cypher_parser,
    query_planner::logical_plan::plan_builder::build_logical_plan,
    render_plan::plan_builder::RenderPlanBuilder,
};

// Mirrors `schemas/test/unified_test_multi_schema.yaml` schema_name
// `social_polymorphic`: one `interactions` table with `$any` endpoints driven by
// from_type/to_type label columns and an interaction_type discriminator.
const SCHEMA_YAML: &str = r#"
name: social_polymorphic
graph_schema:
  nodes:
    - label: User
      database: brahmand
      table: users_bench
      node_id: user_id
      property_mappings:
        user_id: user_id
        name: full_name
        email: email_address
    - label: Post
      database: brahmand
      table: posts_bench
      node_id: post_id
      property_mappings:
        post_id: post_id
        title: content
        content: content
        created: created_at
  edges:
    - polymorphic: true
      database: brahmand
      table: interactions
      from_id: from_id
      to_id: to_id
      type_column: interaction_type
      from_label_column: from_type
      to_label_column: to_type
      type_values:
        - FOLLOWS
        - LIKES
        - AUTHORED
        - COMMENTED
        - SHARED
      edge_id: [from_id, to_id, interaction_type, timestamp]
      property_mappings:
        created_at: timestamp
        weight: interaction_weight
"#;

fn schema() -> GraphSchema {
    GraphSchemaConfig::from_yaml_str(SCHEMA_YAML)
        .expect("parse schema yaml")
        .to_graph_schema()
        .expect("build graph schema")
}

fn cypher_to_sql(cypher: &str) -> String {
    let graph_schema = schema();
    let ast = open_cypher_parser::parse_query(cypher).expect("parse cypher");
    let (logical_plan, mut plan_ctx) =
        build_logical_plan(&ast, &graph_schema, None, None, None).expect("build logical plan");

    use crate::query_planner::{analyzer, optimizer};
    let logical_plan =
        analyzer::initial_analyzing(logical_plan, &mut plan_ctx, &graph_schema).unwrap();
    let logical_plan =
        analyzer::intermediate_analyzing(logical_plan, &mut plan_ctx, &graph_schema).unwrap();
    let logical_plan = optimizer::initial_optimization(logical_plan, &mut plan_ctx).unwrap();
    let logical_plan = optimizer::final_optimization(logical_plan, &mut plan_ctx).unwrap();

    let render_plan = logical_plan
        .to_render_plan(&graph_schema)
        .expect("render plan");
    clickhouse_query_generator::generate_sql(render_plan, 100)
}

/// The pruned placeholder shape produced when a pattern is dropped entirely.
fn is_empty_placeholder(sql: &str) -> bool {
    sql.contains("_empty") && sql.contains("WHERE false")
}

#[test]
fn unlabeled_single_type_polymorphic_path_is_real_query() {
    // `()-[:SHARED]->()` — SHARED has no rows in the data, but the SQL must still
    // be a real query over the edge table, not the `_empty` placeholder.
    let sql = cypher_to_sql("MATCH p=()-[:SHARED]->() RETURN p LIMIT 25");
    assert!(
        !is_empty_placeholder(&sql),
        "unlabeled polymorphic path must NOT prune to the _empty placeholder; SQL:\n{sql}"
    );
    assert!(
        sql.contains("interactions"),
        "expected a real query over the polymorphic edge table `interactions`; SQL:\n{sql}"
    );
    assert!(
        sql.contains("interaction_type") && sql.contains("SHARED"),
        "expected the edge query to be filtered by the requested type; SQL:\n{sql}"
    );
}

#[test]
fn unlabeled_single_type_polymorphic_path_follows() {
    // FOLLOWS HAS data — proves the placeholder was never about missing rows.
    let sql = cypher_to_sql("MATCH p=()-[:FOLLOWS]->() RETURN p LIMIT 5");
    assert!(
        !is_empty_placeholder(&sql),
        "unlabeled polymorphic path must NOT prune to the _empty placeholder; SQL:\n{sql}"
    );
    assert!(
        sql.contains("interactions") && sql.contains("FOLLOWS"),
        "expected a real query over `interactions` filtered by FOLLOWS; SQL:\n{sql}"
    );
}

#[test]
fn unlabeled_anytype_polymorphic_path_is_real_query() {
    // `()-[]->()` — no rel type: real query over all type_values.
    let sql = cypher_to_sql("MATCH p=()-[]->() RETURN p LIMIT 5");
    assert!(
        !is_empty_placeholder(&sql),
        "unlabeled any-type polymorphic path must NOT prune to the _empty placeholder; SQL:\n{sql}"
    );
    assert!(
        sql.contains("interactions"),
        "expected a real query over the polymorphic edge table `interactions`; SQL:\n{sql}"
    );
}

#[test]
fn labeled_polymorphic_pattern_still_renders_real_join() {
    // CONTRAST: the labeled case must keep rendering its real join SQL.
    let sql = cypher_to_sql("MATCH (a:User)-[:SHARED]->(b:Post) RETURN a,b LIMIT 5");
    assert!(
        !is_empty_placeholder(&sql),
        "labeled polymorphic pattern must render real SQL; SQL:\n{sql}"
    );
    assert!(
        sql.contains("users_bench") && sql.contains("interactions") && sql.contains("posts_bench"),
        "labeled pattern must join the concrete node tables through interactions; SQL:\n{sql}"
    );
}
