//! Regression test for dotted physical-column quoting in the pattern-union renderer.
//!
//! An unlabeled path/expand over a multi-edge-type denormalized schema renders a
//! per-edge-type UNION CTE (`pattern_union_<alias>`). The branch generator
//! interpolates raw physical column names into `{table}.{column}` references. When a
//! physical column name contains a dot (e.g. Zeek's `id.orig_h`), the unquoted form
//! `zeek.dns_log.id.orig_h` is parsed by ClickHouse as nested struct access and the
//! identifier fails to resolve. Such columns must be delimiter-quoted with the active
//! dialect's identifier delimiter (CH `"id.orig_h"`, Spark `` `id.orig_h` ``) via the
//! shared `quote_identifier` helper, exactly as the labeled renderer (Path A) does.

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

    // Dotted physical columns must be delimiter-quoted so ClickHouse does not
    // interpret them as nested struct access. The default (no query context)
    // dialect is ClickHouse, so the delimiter is a double-quote.
    assert!(
        sql.contains("\"id.orig_h\""),
        "dotted column id.orig_h must be double-quoted; SQL:\n{sql}"
    );
    assert!(
        sql.contains("\"id.resp_h\""),
        "dotted column id.resp_h must be double-quoted; SQL:\n{sql}"
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

/// Multi-type VLP schema: `IP` has TWO outgoing edge types to DIFFERENT node
/// labels, so an unlabeled `-[r]->(target)` end node is genuinely multi-type and
/// the single-hop CTE projects the `r_from_id` / `r_to_id` relationship-endpoint
/// columns (`multi_type_vlp_joins.rs`). Both edges have a dotted FK column
/// (`id.orig_h`, `id.resp_h`) so the projection must quote it.
const MULTI_TYPE_SCHEMA_YAML: &str = r#"
name: dotted_col_multitype_test
graph_schema:
  nodes:
    - label: IP
      database: zeek
      table: conn_log
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
    # IP -> Domain (dotted from_id)
    - type: REQUESTED
      database: zeek
      table: dns_log
      from_node: IP
      to_node: Domain
      from_id: "id.orig_h"
      to_id: query
    # IP -> IP (dotted from_id AND to_id) — the second outgoing edge type from
    # IP that makes `target` multi-type and triggers the r_from_id/r_to_id path.
    - type: CONNECTED_TO
      database: zeek
      table: conn_log
      from_node: IP
      to_node: IP
      from_id: "id.orig_h"
      to_id: "id.resp_h"
"#;

fn multi_type_cypher_to_sql(cypher: &str) -> String {
    let graph_schema = GraphSchemaConfig::from_yaml_str(MULTI_TYPE_SCHEMA_YAML)
        .expect("parse schema yaml")
        .to_graph_schema()
        .expect("build graph schema");
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

/// #825: the multi-type VLP single-hop `r_from_id` / `r_to_id` projection
/// interpolated the raw physical FK column, emitting `toString(n2.id.orig_h)` —
/// which ClickHouse parses as nested struct access (`n2.id`.`orig_h`) and fails
/// to resolve. The FK column must be dialect-quoted like every other column ref.
#[test]
fn multi_type_vlp_rel_endpoint_columns_quote_dotted_fk() {
    let sql = multi_type_cypher_to_sql(
        "MATCH (a:IP)-[r]->(target) WHERE a.ip = '1.2.3.4' RETURN type(r), target LIMIT 10",
    );

    // The projection must exist (guards against the test silently not hitting
    // the r_from_id path if planning changes).
    assert!(
        sql.contains("AS r_from_id"),
        "expected r_from_id projection in multi-type single-hop CTE; SQL:\n{sql}"
    );

    // The dotted FK column must be double-quoted inside the toString() cast
    // (default dialect is ClickHouse), NOT raw.
    assert!(
        sql.contains("toString(n2.\"id.orig_h\") AS r_from_id")
            || sql.contains("toString(ip_1.\"id.orig_h\") AS r_from_id"),
        "r_from_id must quote the dotted FK column; SQL:\n{sql}"
    );

    // The raw unquoted form (the #825 bug) must NOT appear anywhere.
    assert!(
        !sql.contains("(n2.id.orig_h)") && !sql.contains("(n2.id.resp_h)"),
        "unquoted dotted FK column in a cast is a CH parse error; SQL:\n{sql}"
    );
}
