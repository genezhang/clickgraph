//! Regression test for relationship-property resolution over a multi-type
//! `pattern_union` CTE.
//!
//! An unlabeled relationship `()-[r]-()` whose property exists on MULTIPLE
//! relationship types renders a per-edge-type UNION CTE (`pattern_union_<alias>`).
//! That CTE projects each relationship property as a direct column under its
//! PROPERTY name (e.g. `zeek.dns_log.ts AS timestamp`). Per CLAUDE.md §2
//! (forward resolution through a CTE barrier), a downstream `r.<property>`
//! reference in the OUTER query must use the property-named CTE column
//! (`r.timestamp`), NOT the physical schema column (`r.ts`) — the physical
//! column does not exist in the CTE, so ClickHouse fails with Code 47.
//!
//! Contrast: a property that exists on only a SINGLE edge type does NOT build a
//! pattern_union CTE and must keep referencing the physical column directly from
//! the edge table.

use crate::{
    clickhouse_query_generator, graph_catalog::config::GraphSchemaConfig,
    graph_catalog::graph_schema::GraphSchema, open_cypher_parser,
    query_planner::logical_plan::plan_builder::build_logical_plan,
    render_plan::plan_builder::RenderPlanBuilder,
};

// Two denormalized edge types sharing a `timestamp` property (mapped to physical
// column `ts`), plus a `server` property that exists on only ONE edge type.
const SCHEMA_YAML: &str = r#"
name: pattern_union_rel_prop_test
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
    - type: REQUESTED
      database: zeek
      table: dns_log
      from_node: IP
      to_node: Domain
      from_id: "id.orig_h"
      to_id: query
      is_denormalized: true
      property_mappings:
        timestamp: ts
        server: "id.resp_h"
    - type: RESOLVED_TO
      database: zeek
      table: dns_log
      from_node: Domain
      to_node: IP
      from_id: query
      to_id: "id.resp_h"
      property_mappings:
        timestamp: ts
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
fn multi_type_rel_property_resolves_to_cte_column_not_physical() {
    let sql = cypher_to_sql(
        "MATCH ()-[r]-() WHERE r.timestamp IS NOT NULL \
         RETURN DISTINCT 'relationship' AS entity, r.timestamp AS timestamp LIMIT 25",
    );

    // The pattern_union CTE must exist and project the property-named column.
    assert!(
        sql.contains("pattern_union_r"),
        "expected a pattern_union CTE for the multi-type relationship; SQL:\n{sql}"
    );
    assert!(
        sql.contains("AS timestamp"),
        "CTE must project the relationship property under its property name; SQL:\n{sql}"
    );

    // The OUTER query must reference the property-named CTE column `r.timestamp`,
    // never the physical column `r.ts` (which does not exist in the CTE).
    assert!(
        sql.contains("r.timestamp") || sql.contains("r.\"timestamp\""),
        "outer query must reference the property-named CTE column r.timestamp; SQL:\n{sql}"
    );
    assert!(
        !sql.contains("r.ts AS") && !sql.contains("r.\"ts\""),
        "outer query must NOT reference the physical column r.ts; SQL:\n{sql}"
    );
}

// Two edge types sharing a `timestamp` property (mapped to physical column
// `ts`) whose tables are DIFFERENT (`dns_log` vs `dns_resolutions`). The
// multi-type expansion must still build a `pattern_union` CTE even when the rel
// pattern is one branch of a top-level Cypher UNION.
const SCHEMA_YAML_DIFFERENT_TABLES: &str = r#"
name: pattern_union_rel_prop_multitable_test
graph_schema:
  nodes:
    - label: IP
      database: zeek
      table: all_ips
      node_id: ip
      property_mappings:
        ip: ip
    - label: Domain
      database: zeek
      table: dns_log
      node_id: query
      property_mappings:
        name: query
  edges:
    - type: REQUESTED
      database: zeek
      table: dns_log
      from_node: IP
      to_node: Domain
      from_id: "id.orig_h"
      to_id: query
      property_mappings:
        timestamp: ts
        uid: uid
    - type: RESOLVED_TO
      database: zeek
      table: dns_resolutions
      from_node: Domain
      to_node: IP
      from_id: domain
      to_id: resolved_ip
      property_mappings:
        timestamp: ts
"#;

// Full statement path (handles top-level Cypher UNION, which `parse_query`
// does not) reusing the production translator entry point.
fn cypher_to_sql_with(schema_yaml: &str, cypher: &str) -> String {
    let graph_schema = GraphSchemaConfig::from_yaml_str(schema_yaml)
        .expect("parse schema yaml")
        .to_graph_schema()
        .expect("build graph schema");
    let (sql, _lp, _ctx) = crate::sql_generator::emitters::clickhouse::cypher_to_sql_with_metadata(
        cypher,
        &graph_schema,
        100,
    )
    .expect("translate cypher to sql");
    sql
}

#[test]
fn multi_type_rel_property_in_top_level_union_keeps_pattern_union() {
    // Browser property-key probe shape: a node branch (dropped — no node has
    // `timestamp`) UNION ALL a relationship branch. The rel branch's multi-type
    // expansion, whose two edge types live in DIFFERENT tables, must still build
    // a pattern_union CTE and reference the property-named CTE column — NOT
    // collapse to a single raw edge table with a `r.timestamp` column that does
    // not exist there.
    let sql = cypher_to_sql_with(
        SCHEMA_YAML_DIFFERENT_TABLES,
        "MATCH (n) WHERE n.timestamp IS NOT NULL \
         RETURN DISTINCT 'node' AS entity, n.timestamp AS timestamp LIMIT 25\n\
         UNION ALL\n\
         MATCH ()-[r]-() WHERE r.timestamp IS NOT NULL \
         RETURN DISTINCT 'relationship' AS entity, r.timestamp AS timestamp LIMIT 25",
    );

    // The multi-type expansion must survive the enclosing UNION as a CTE with
    // both edge types' tables present.
    assert!(
        sql.contains("pattern_union_r"),
        "expected a pattern_union CTE for the multi-type relationship inside the top-level UNION; SQL:\n{sql}"
    );
    assert!(
        sql.contains("zeek.dns_log") && sql.contains("zeek.dns_resolutions"),
        "pattern_union CTE must span BOTH edge tables; SQL:\n{sql}"
    );

    // The outer query must reference the property-named CTE column, never emit a
    // bare `r.timestamp` against a raw edge table that lacks that column.
    assert!(
        sql.contains("FROM pattern_union_r AS r"),
        "outer query must read from the pattern_union CTE; SQL:\n{sql}"
    );
    assert!(
        !sql.contains("FROM zeek.dns_log AS r"),
        "outer query must NOT collapse to a single raw edge table; SQL:\n{sql}"
    );
}

#[test]
fn single_type_rel_property_still_uses_physical_column() {
    // `server` exists on only the REQUESTED edge → single-type, no pattern_union.
    let sql = cypher_to_sql(
        "MATCH ()-[r]-() WHERE r.server IS NOT NULL \
         RETURN DISTINCT 'relationship' AS entity, r.server AS server LIMIT 25",
    );

    // Single-type resolution maps to the physical column directly from the edge
    // table (quoting style depends on dialect: `id.resp_h` or "id.resp_h").
    assert!(
        sql.contains("id.resp_h"),
        "single-type relationship property must map to its physical column; SQL:\n{sql}"
    );
    assert!(
        !sql.contains(".server"),
        "single-type relationship property must not leak the property name as a column; SQL:\n{sql}"
    );
    assert!(
        !sql.contains("pattern_union_r"),
        "single-type relationship must NOT build a pattern_union CTE; SQL:\n{sql}"
    );
}
