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

    // ...but the AUTHORED branch's NON-coupled User endpoint join must SURVIVE.
    // (Post is coupled — embedded in posts — so only its self-join is dropped;
    // User lives in a separate table and must still be joined.)
    assert!(
        sql.contains("INNER JOIN test_db.users ON test_db.users.user_id = test_db.posts.author_id"),
        "AUTHORED branch must keep the non-coupled User join; SQL:\n{sql}"
    );

    // The non-denormalized LIKED branch is traditional — BOTH node tables are
    // separate from the edge table, so BOTH joins must be present.
    assert!(
        sql.contains(
            "INNER JOIN test_db.users ON test_db.users.user_id = test_db.post_likes.user_id"
        ),
        "LIKED branch must join the User table; SQL:\n{sql}"
    );
    assert!(
        sql.contains(
            "INNER JOIN test_db.posts ON test_db.posts.post_id = test_db.post_likes.post_id"
        ),
        "LIKED branch must join the Post table; SQL:\n{sql}"
    );
}

/// A node embedded in the edge table but keyed on a DIFFERENT column than the
/// edge's endpoint id is NOT coupled — the join is a real selective filter and
/// must be retained (regression guard for the table-equality-only coupling bug).
#[test]
fn embedded_node_with_distinct_id_column_keeps_join() {
    const YAML: &str = r#"
name: distinct_id_embed_test
graph_schema:
  nodes:
    - label: User
      database: test_db
      table: users
      node_id: user_id
      property_mappings:
        user_id: user_id
    - label: Session
      database: test_db
      table: events
      node_id: session_id
      property_mappings:
        session_id: session_id
  edges:
    # Session is embedded in the `events` edge table, BUT the edge keys it on
    # `sess_ref` while Session's node_id is `session_id`. Same table, different
    # column → the join is a real selective filter, not the
    # `events.session_id = events.session_id` tautology, so it must be kept.
    # (User lives in its own `users` table, so this is not a self-join.)
    - type: STARTED
      database: test_db
      table: events
      from_node: User
      to_node: Session
      from_id: actor_id
      to_id: sess_ref
      property_mappings: {}
    - type: TOUCHED
      database: test_db
      table: touches
      from_node: User
      to_node: Session
      from_id: user_id
      to_id: session_id
      property_mappings: {}
"#;
    let graph_schema = GraphSchemaConfig::from_yaml_str(YAML)
        .expect("parse schema yaml")
        .to_graph_schema()
        .expect("build graph schema");
    let ast = open_cypher_parser::parse_query("MATCH (n)-[r]-(o) WHERE n.user_id = 1 RETURN r, o")
        .expect("parse cypher");
    let (logical_plan, mut plan_ctx) =
        build_logical_plan(&ast, &graph_schema, None, None, None).expect("build logical plan");
    use crate::query_planner::{analyzer, optimizer};
    let logical_plan =
        analyzer::initial_analyzing(logical_plan, &mut plan_ctx, &graph_schema).unwrap();
    let logical_plan =
        analyzer::intermediate_analyzing(logical_plan, &mut plan_ctx, &graph_schema).unwrap();
    let logical_plan = optimizer::initial_optimization(logical_plan, &mut plan_ctx).unwrap();
    let logical_plan = optimizer::final_optimization(logical_plan, &mut plan_ctx).unwrap();
    let render_plan = logical_plan.to_render_plan(&graph_schema).expect("render");
    let sql = clickhouse_query_generator::generate_sql(render_plan, 100);

    // Session is embedded in `events` but keyed on session_id while the STARTED
    // edge keys on sess_ref — same table, different column → the join is selective
    // and must be kept (NOT dropped as a coupled tautology).
    assert!(
        sql.contains(
            "INNER JOIN test_db.events ON test_db.events.session_id = test_db.events.sess_ref"
        ),
        "embedded node with a distinct id column must keep its selective join; SQL:\n{sql}"
    );
}

/// A DENORMALIZED endpoint with a VIRTUAL node_id — node_id and properties exist
/// only in the role-appropriate denorm property maps (from_properties /
/// to_properties), with NO physical node_id column — must resolve through those
/// maps and emit NO self-join. (Regression for the Zeek/flights
/// `from_node.code = flights.Origin` bug, where the virtual id referenced a
/// non-existent column on a spurious self-join alias.)
#[test]
fn denormalized_virtual_id_resolves_without_self_join() {
    const YAML: &str = r#"
name: virtual_id_denorm_test
graph_schema:
  nodes:
    # Airport is denormalized into the `flights` edge table. Its node_id `code`
    # is virtual: it maps to Origin (from-role) / Dest (to-role). There is NO
    # `code` column in flights.
    - label: Airport
      database: test_db
      table: flights
      node_id: code
      is_denormalized: true
      property_mappings: {}
      from_node_properties:
        code: Origin
        city: OriginCity
      to_node_properties:
        code: Dest
        city: DestCity
  edges:
    # Both edge types are coupled-denormalized self-refs in `flights`. Two types
    # force the multi-type pattern-union path.
    - type: FLIGHT
      database: test_db
      table: flights
      from_node: Airport
      to_node: Airport
      from_id: Origin
      to_id: Dest
      property_mappings:
        flight_num: flight_number
    - type: CODESHARE
      database: test_db
      table: flights
      from_node: Airport
      to_node: Airport
      from_id: Origin
      to_id: Dest
      property_mappings: {}
"#;
    let graph_schema = GraphSchemaConfig::from_yaml_str(YAML)
        .expect("parse schema yaml")
        .to_graph_schema()
        .expect("build graph schema");
    let ast = open_cypher_parser::parse_query("MATCH p=()-[]->() RETURN p LIMIT 5")
        .expect("parse cypher");
    let (logical_plan, mut plan_ctx) =
        build_logical_plan(&ast, &graph_schema, None, None, None).expect("build logical plan");
    use crate::query_planner::{analyzer, optimizer};
    let logical_plan =
        analyzer::initial_analyzing(logical_plan, &mut plan_ctx, &graph_schema).unwrap();
    let logical_plan =
        analyzer::intermediate_analyzing(logical_plan, &mut plan_ctx, &graph_schema).unwrap();
    let logical_plan = optimizer::initial_optimization(logical_plan, &mut plan_ctx).unwrap();
    let logical_plan = optimizer::final_optimization(logical_plan, &mut plan_ctx).unwrap();
    let render_plan = logical_plan.to_render_plan(&graph_schema).expect("render");
    let sql = clickhouse_query_generator::generate_sql(render_plan, 100);

    // The virtual node_id `code` must NOT be referenced — it is not a real column.
    assert!(
        !sql.contains(".code"),
        "virtual node_id `code` must be resolved away, never referenced; SQL:\n{sql}"
    );
    // No spurious self-join aliases for the coupled-denormalized endpoints.
    assert!(
        !sql.contains("AS from_node") && !sql.contains("AS to_node"),
        "coupled denormalized endpoints must not be self-joined; SQL:\n{sql}"
    );
    // start_id/end_id resolve through from_properties/to_properties to the real
    // physical columns (Origin / Dest).
    assert!(
        sql.contains("test_db.flights.Origin") && sql.contains("test_db.flights.Dest"),
        "virtual id must resolve to Origin (from) / Dest (to); SQL:\n{sql}"
    );
}

/// A partially-specified denormalized self-loop (a node embedded in the edge
/// table defining only ONE of from_node_properties / to_node_properties) is
/// rejected at schema-build time, so the renderer never sees it. This documents
/// that the validator is the first line of defense; the renderer's join-skip is
/// additionally decoupled from property-map presence as a belt-and-suspenders
/// guard (see cte_extraction.rs from_denorm/to_denorm).
#[test]
fn partial_denorm_self_loop_rejected_by_schema_validation() {
    const YAML: &str = r#"
name: partial_denorm_test
graph_schema:
  nodes:
    - label: Airport
      database: test_db
      table: flights
      node_id: code
      is_denormalized: true
      property_mappings: {}
      from_node_properties:
        code: Origin
  edges:
    - type: FLIGHT
      database: test_db
      table: flights
      from_node: Airport
      to_node: Airport
      from_id: Origin
      to_id: Dest
      property_mappings: {}
"#;
    let result = GraphSchemaConfig::from_yaml_str(YAML)
        .expect("parse schema yaml")
        .to_graph_schema();
    assert!(
        result.is_err(),
        "a denormalized self-loop missing to_node_properties must be rejected by validation"
    );
}
