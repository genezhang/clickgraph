//! Regression test for dotted physical-column quoting in the pattern-union renderer.
//!
//! An unlabeled path/expand over a multi-edge-type denormalized schema renders a
//! per-edge-type UNION CTE (`pattern_union_<alias>`). The branch generator
//! interpolates raw physical column names into `{table}.{column}` references. When a
//! physical column name contains a dot (e.g. Zeek's `id.orig_h`), the unquoted form
//! `zeek.dns_log.id.orig_h` is parsed by ClickHouse as nested struct access and the
//! identifier fails to resolve. Such columns must be backtick-quoted
//! (`zeek.dns_log.`id.orig_h``), exactly as the labeled renderer already does.

use crate::{
    clickhouse_query_generator, graph_catalog::config::GraphSchemaConfig,
    graph_catalog::graph_schema::GraphSchema, open_cypher_parser,
    query_planner::logical_plan::plan_builder::build_logical_plan,
    render_plan::plan_builder::RenderPlanBuilder,
};

const SCHEMA_YAML: &str = r#"
name: dotted_col_test
graph_schema:
  nodes:
    - label: IP
      database: zeek
      table: dns_log
      node_id: ip
      property_mappings:
        ip: "id.orig_h"
    - label: Domain
      database: zeek
      table: dns_log
      node_id: domain
      property_mappings:
        domain: query
  edges:
    # Denormalized edge: both endpoints live in the dns_log edge table.
    - type: REQUESTED
      database: zeek
      table: dns_log
      from_node: IP
      to_node: Domain
      from_id: "id.orig_h"
      to_id: query
      is_denormalized: true
      property_mappings:
        server: "id.resp_h"
    # A second edge type forces the multi-type pattern-union expansion path.
    - type: RESOLVED_TO
      database: zeek
      table: dns_log
      from_node: Domain
      to_node: IP
      from_id: query
      to_id: "id.resp_h"
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
fn pattern_union_quotes_dotted_physical_columns() {
    let sql = cypher_to_sql("MATCH p=()-[]->() RETURN p LIMIT 25");

    // Dotted physical columns must be backtick-quoted so ClickHouse does not
    // interpret them as nested struct access.
    assert!(
        sql.contains("`id.orig_h`"),
        "dotted column id.orig_h must be backtick-quoted; SQL:\n{sql}"
    );
    assert!(
        sql.contains("`id.resp_h`"),
        "dotted column id.resp_h must be backtick-quoted; SQL:\n{sql}"
    );

    // The unquoted form (parsed as nested access) must NOT appear.
    assert!(
        !sql.contains("dns_log.id.orig_h"),
        "unquoted dotted column dns_log.id.orig_h must not appear; SQL:\n{sql}"
    );
    assert!(
        !sql.contains("dns_log.id.resp_h"),
        "unquoted dotted column dns_log.id.resp_h must not appear; SQL:\n{sql}"
    );

    // Plain columns must remain unquoted (no over-quoting regression).
    assert!(
        sql.contains("dns_log.query"),
        "plain column `query` should remain unquoted; SQL:\n{sql}"
    );
}
