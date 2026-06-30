//! Regression test for the Neo4j-Browser unlabeled-expand row explosion.
//!
//! An unlabeled undirected expand `MATCH (n)-[r]-(o)` over a multi-edge-type schema
//! renders a per-edge-type UNION CTE (`pattern_union_<alias>`). For a DENORMALIZED
//! edge whose to-node is embedded in the edge table itself (e.g. `AUTHORED` with the
//! `Post` node living in the same `posts` table as the edge), the branch generator
//! used to emit a spurious unaliased self-join of the edge table on its own id
//! (`posts.post_id = posts.post_id`). That tautology join multiplied rows — a single
//! User's expand exploded into >1000 rows. The fix skips the join for an endpoint
//! whose table IS the edge table (its columns already reference the edge table).

use crate::{
    clickhouse_query_generator, graph_catalog::config::GraphSchemaConfig,
    graph_catalog::graph_schema::GraphSchema, open_cypher_parser,
    query_planner::logical_plan::plan_builder::build_logical_plan,
    render_plan::plan_builder::RenderPlanBuilder,
};

const SCHEMA_YAML: &str = r#"
name: denorm_expand_test
graph_schema:
  nodes:
    - label: User
      database: test_db
      table: users
      node_id: user_id
      property_mappings:
        user_id: user_id
        name: full_name
    - label: Post
      database: test_db
      table: posts
      node_id: post_id
      property_mappings:
        post_id: post_id
        title: post_title
  edges:
    # Denormalized edge: the AUTHORED edge AND the Post node both live in `posts`.
    - type: AUTHORED
      database: test_db
      table: posts
      from_node: User
      to_node: Post
      from_id: author_id
      to_id: post_id
      is_denormalized: true
      property_mappings: {}
    # A second edge type forces the multi-type pattern-union expansion path.
    - type: LIKED
      database: test_db
      table: post_likes
      from_node: User
      to_node: Post
      from_id: user_id
      to_id: post_id
      property_mappings: {}
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

#[test]
fn unlabeled_expand_no_denormalized_edge_self_join() {
    let sql = cypher_to_sql("MATCH (n)-[r]-(o) WHERE n.user_id = 1 RETURN r, o LIMIT 25");

    // The denormalized AUTHORED branch must NOT self-join the edge table on its own
    // id — that tautology is the row-explosion bug.
    assert!(
        !sql.contains("test_db.posts.post_id = test_db.posts.post_id"),
        "denormalized edge table must not be self-joined on its own id; SQL:\n{sql}"
    );

    // The non-denormalized LIKED branch should still join its separate node tables.
    assert!(
        sql.contains("test_db.post_likes"),
        "expected the LIKED (traditional) edge branch to be present; SQL:\n{sql}"
    );
}
