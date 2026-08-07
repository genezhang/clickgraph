//! Regression tests for #1006 — own-table property resolution on
//! foreign-embedded (MixedAccess) endpoints.
//!
//! Shape: `(a:Person)-[:REPORTS_TO*1..1|*1|]->(b:Person)` on a schema where
//! the FROM endpoint `a` embeds ONLY its id in the edge table (`from_node_
//! properties: {pid: mgr_id}` on `reports`), while its other properties live
//! on its OWN table (`people`). The edge's embedded property map is therefore
//! a strict subset of the node's property set ("MixedAccess").
//!
//! Before the fix, `RETURN a.name` fell through to the edge-alias
//! pass-through and emitted `tN.name` on `reports` — a column that does not
//! exist there → ClickHouse Code 47 (unknown identifier). The fix registers a
//! lazy own-table join request (select/group-by/order-by builders) and
//! `join_builder` injects a DEDUPLICATED, row-preserving LEFT JOIN of the
//! node's own table (the same `*_own` pattern the VLP arm uses, #908):
//!
//! ```sql
//! LEFT JOIN (SELECT pid, any(name) as name FROM testdb.people
//!            GROUP BY pid) AS a ON a.pid = tN.mgr_id
//! ```
//!
//! Aliased by the NODE alias, so downstream `a.name` references (SELECT,
//! WHERE, GROUP BY, ORDER BY) resolve without further rewrites.
//!
//! Exclusions locked by these tests:
//! - `node_id` names never resolve through the own table (auto-generated
//!   identity mappings exist even for VIRTUAL ids whose own table has no such
//!   column — e.g. zeek_dns `node_id: ip_address` on `dns_log`). Node ids
//!   resolve via the edge's embedded map instead.
//! - `__denorm_scan_{alias}` anchors keep resolving through their anchor CTE
//!   (#582/#590), never through a lazy own-table join.
//! - Properties already in the edge's embedded map (e.g. `a.pid`) stay on the
//!   edge; no extra join is injected when nothing needs the own table.

use crate::clickhouse_query_generator::cypher_to_sql;
use crate::graph_catalog::config::GraphSchemaConfig;
use crate::server::query_context::{set_current_schema, with_query_context, QueryContext};
use std::sync::Arc;

const SCHEMA_YAML: &str = include_str!("../../../schemas/test/foreign_selfloop.yaml");

/// Translate Cypher → SQL through the same task-local-context path the `cg`
/// tool and embedded API use (the own-table registry is task-local).
fn translate(cypher: &str) -> String {
    let schema = Arc::new(
        GraphSchemaConfig::from_yaml_str(SCHEMA_YAML)
            .expect("parse schema yaml")
            .to_graph_schema()
            .expect("build graph schema"),
    );
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        with_query_context(QueryContext::new(None), async move {
            set_current_schema(Arc::clone(&schema));
            cypher_to_sql(cypher, &schema, 100).expect("translate cypher")
        })
        .await
    })
}

const OWN_TABLE_JOIN: &str =
    "LEFT JOIN (SELECT pid, any(name) as name FROM testdb.people GROUP BY pid) AS a ON a.pid";

#[test]
fn own_table_property_resolves_via_injected_join_flat_arrow() {
    let sql = translate(
        "MATCH (a:Person)-[:REPORTS_TO]->(b:Person) WHERE b.name = 'Alice' RETURN a.name",
    );

    assert!(
        sql.contains(OWN_TABLE_JOIN),
        "must inject the deduplicated own-table LEFT JOIN; SQL:\n{sql}"
    );
    assert!(
        sql.contains(r#"a.name AS "a.name""#),
        "a.name must resolve through the node alias; SQL:\n{sql}"
    );
    assert!(
        !sql.contains(r#"t1.name"#) && !sql.contains(r#"reports AS t1"#),
        "must not reference reports.name (edge table has no name column); SQL:\n{sql}"
    );
}

#[test]
fn own_table_property_resolves_via_injected_join_vlp_1_hop() {
    let sql = translate(
        "MATCH (a:Person)-[:REPORTS_TO*1]->(b:Person) WHERE b.name = 'Alice' RETURN a.name",
    );

    assert!(sql.contains(OWN_TABLE_JOIN), "SQL:\n{sql}");
    assert!(sql.contains(r#"a.name AS "a.name""#), "SQL:\n{sql}");
}

#[test]
fn own_table_property_resolves_via_injected_join_vlp_1_1() {
    let sql = translate(
        "MATCH (a:Person)-[:REPORTS_TO*1..1]->(b:Person) WHERE b.name = 'Alice' RETURN a.name",
    );

    assert!(sql.contains(OWN_TABLE_JOIN), "SQL:\n{sql}");
    assert!(sql.contains(r#"a.name AS "a.name""#), "SQL:\n{sql}");
}

#[test]
fn where_filter_on_own_table_property_uses_node_alias() {
    let sql = translate(
        "MATCH (a:Person)-[:REPORTS_TO*1..1]->(b:Person) WHERE a.name = 'Alice' RETURN a.name",
    );

    assert!(sql.contains(OWN_TABLE_JOIN), "SQL:\n{sql}");
    assert!(
        sql.contains("WHERE a.name = 'Alice'"),
        "the filter must reference the injected join alias a, not the edge alias; SQL:\n{sql}"
    );
    assert!(
        !sql.contains("WHERE t"),
        "filter must not reference the edge alias; SQL:\n{sql}"
    );
}

#[test]
fn group_by_on_own_table_property_uses_node_alias() {
    let sql = translate("MATCH (a:Person)-[:REPORTS_TO]->(b:Person) RETURN a.name, count(*)");

    assert!(sql.contains(OWN_TABLE_JOIN), "SQL:\n{sql}");
    assert!(
        sql.contains("GROUP BY a.name"),
        "GROUP BY must reference the injected join alias; SQL:\n{sql}"
    );
    assert!(
        sql.contains(r#"a.name AS "a.name""#),
        "select must reference the own-table column; SQL:\n{sql}"
    );
}

#[test]
fn order_by_on_own_table_property_uses_node_alias() {
    let sql = translate("MATCH (a:Person)-[:REPORTS_TO]->(b:Person) WHERE a.name = 'Alice' RETURN a.name ORDER BY a.name");

    assert!(sql.contains(OWN_TABLE_JOIN), "SQL:\n{sql}");
    assert!(
        sql.contains("ORDER BY a.name ASC"),
        "ORDER BY must reference the injected join alias; SQL:\n{sql}"
    );
    assert!(
        sql.contains("WHERE a.name = 'Alice'"),
        "filter must reference the injected join alias; SQL:\n{sql}"
    );
}

#[test]
fn mixed_return_resolves_both_sides() {
    let sql = translate("MATCH (a:Person)-[:REPORTS_TO*1..1]->(b:Person) RETURN a.name, b.name");

    assert!(sql.contains(OWN_TABLE_JOIN), "SQL:\n{sql}");
    assert!(sql.contains(r#"a.name AS "a.name""#), "SQL:\n{sql}");
    assert!(
        sql.contains(r#"b.name AS "b.name""#),
        "the own-table endpoint (b) keeps resolving via the people join; SQL:\n{sql}"
    );
}

#[test]
fn no_own_table_join_when_only_standard_side_used() {
    let sql = translate(
        "MATCH (a:Person)-[:REPORTS_TO*1..1]->(b:Person) WHERE b.name = 'Alice' RETURN b.name",
    );

    assert!(
        !sql.contains(OWN_TABLE_JOIN),
        "no own-table join may be injected when only b is referenced; SQL:\n{sql}"
    );
    assert!(sql.contains(r#"b.name AS "b.name""#), "SQL:\n{sql}");
}

#[test]
fn embedded_id_property_stays_on_edge() {
    let sql = translate(
        "MATCH (a:Person)-[:REPORTS_TO*1..1]->(b:Person) WHERE b.name = 'Alice' RETURN a.pid",
    );

    assert!(
        !sql.contains(OWN_TABLE_JOIN),
        "a.pid is in the edge's embedded map and must NOT trigger an own-table join; SQL:\n{sql}"
    );
    assert!(
        sql.contains(r#"mgr_id AS "a.pid""#),
        "a.pid must resolve to the edge's embedded id column mgr_id; SQL:\n{sql}"
    );
}

#[test]
fn vlp_range_keeps_cte_path_unchanged() {
    let sql = translate(
        "MATCH (a:Person)-[:REPORTS_TO*1..2]->(b:Person) WHERE b.name = 'Alice' RETURN a.name",
    );

    assert!(
        sql.contains("WITH RECURSIVE vlp_a_b_inner AS"),
        "the recursive VLP CTE path must be used; SQL:\n{sql}"
    );
    assert!(
        sql.contains("start_own"),
        "the VLP arm's own-table join (start_own) must be used; SQL:\n{sql}"
    );
    assert!(
        sql.contains(r#"t.start_name AS "a.name""#),
        "a.name must resolve through the VLP CTE column; SQL:\n{sql}"
    );
    assert!(
        !sql.contains("GROUP BY pid) AS a ON"),
        "no flat-path own-table join (aliased by the node alias) may be injected for the CTE path; SQL:\n{sql}"
    );
}

#[test]
fn multiple_own_table_properties_deduplicated_in_join() {
    let sql = translate("MATCH (a:Person)-[:REPORTS_TO]->(b:Person) RETURN a.name, a.name");

    assert_eq!(
        sql.matches("any(name) as name").count(),
        1,
        "duplicate property references must not duplicate the join's projection; SQL:\n{sql}"
    );
}

#[test]
fn virtual_node_id_never_resolves_through_own_table() {
    // zeek_dns: node_id `ip_address` has NO column on the own table (dns_log);
    // the identity mapping is auto-generated, so own-table resolution must be
    // skipped — the legacy edge-alias pass-through is kept (locked by the
    // corpus goldens).
    let schema = Arc::new(
        GraphSchemaConfig::from_yaml_file("schemas/examples/zeek_dns_log.yaml")
            .expect("parse zeek schema")
            .to_graph_schema()
            .expect("build zeek schema"),
    );
    let rt = tokio::runtime::Runtime::new().unwrap();
    let sql = rt.block_on(async move {
        with_query_context(QueryContext::new(None), async move {
            set_current_schema(Arc::clone(&schema));
            cypher_to_sql(
                "MATCH (a:Domain)-[r1:REQUESTED]->(b:Domain)-[r2:REQUESTED]->(c:Domain) \
                 RETURN a.domain_name, c.domain_name LIMIT 5",
                &schema,
                100,
            )
            .expect("translate cypher")
        })
        .await
    });

    assert!(
        sql.contains(r#"r1.domain_name AS "a.domain_name""#),
        "virtual node_id property must keep the legacy edge-alias pass-through; SQL:\n{sql}"
    );
    assert!(
        !sql.contains("LEFT JOIN (SELECT"),
        "no own-table join may be injected for virtual node_id properties; SQL:\n{sql}"
    );
}

#[test]
fn where_filter_plain_arrow_resolves_from_side() {
    // The live #1006 repro: plain arrow (non-VLP), WHERE on the FROM-side
    // endpoint whose embedded map covers only its id. Before the filter-path
    // fix the predicate fell through to the denormalized-alias remap and
    // emitted `WHERE t1.name = 'Alice'` on the edge table (Code 47) — the
    // registration must now come from the filter path (a.name is NOT in the
    // SELECT).
    let sql = translate(
        "MATCH (a:Person)-[:REPORTS_TO]->(b:Person) WHERE a.name = 'Alice' RETURN a.pid, b.name",
    );

    assert!(
        sql.contains(OWN_TABLE_JOIN),
        "must inject the deduplicated own-table LEFT JOIN; SQL:\n{sql}"
    );
    assert!(
        sql.contains("WHERE a.name = 'Alice'"),
        "the filter must reference the injected join alias a, not the edge alias; SQL:\n{sql}"
    );
    assert!(
        !sql.contains("t1.name"),
        "must not reference reports.name (edge table has no name column); SQL:\n{sql}"
    );
    assert!(
        sql.contains(r#"mgr_id AS "a.pid""#),
        "the embedded id property a.pid must still resolve through the edge map; SQL:\n{sql}"
    );
}

#[test]
fn order_by_only_plain_arrow_uses_own_table() {
    // ORDER BY referencing a foreign-embedded endpoint property that is not in
    // the SELECT and not in any WHERE — the request must be registered by the
    // order-by path so the own-table join is injected.
    let sql = translate(
        "MATCH (a:Person)-[:REPORTS_TO]->(b:Person) RETURN a.pid, b.name ORDER BY a.name",
    );

    assert!(sql.contains(OWN_TABLE_JOIN), "SQL:\n{sql}");
    assert!(
        sql.contains("ORDER BY a.name ASC"),
        "ORDER BY must reference the injected join alias; SQL:\n{sql}"
    );
    assert!(
        !sql.contains("t1.name"),
        "must not reference reports.name; SQL:\n{sql}"
    );
}

#[test]
fn where_only_on_own_table_property_plain_arrow() {
    // The foreign-embedded endpoint appears ONLY in the WHERE clause; the
    // whole own-table request must come from the filter path (pre-registered
    // before join injection).
    let sql = translate(
        "MATCH (a:Person)-[:REPORTS_TO]->(b:Person) WHERE a.name = 'Alice' RETURN b.name",
    );

    assert!(sql.contains(OWN_TABLE_JOIN), "SQL:\n{sql}");
    assert!(sql.contains("WHERE a.name = 'Alice'"), "SQL:\n{sql}");
    assert!(
        !sql.contains("t1.name"),
        "must not reference reports.name; SQL:\n{sql}"
    );
}

#[test]
fn unlabeled_where_on_own_table_property_plain_arrow() {
    // The live #1006 repro without labels: `MATCH (a)-[r]->(b)` leaves
    // GraphNode.label None and rel.labels empty, so both
    // `get_node_label_for_alias` and the rel-type connection fallback
    // (#551/#560) fail — the embedded-id-key schema match must kick in, or
    // the predicate falls through to `r.name` on reports (Code 47).
    let sql = translate("MATCH (a)-[r]->(b) WHERE a.name = 'Alice' RETURN a.pid, b.name");

    assert!(sql.contains(OWN_TABLE_JOIN), "SQL:\n{sql}");
    assert!(
        sql.contains("WHERE a.name = 'Alice'"),
        "the filter must reference the injected join alias a, not the edge alias; SQL:\n{sql}"
    );
    assert!(
        !sql.contains("r.name"),
        "must not reference reports.name; SQL:\n{sql}"
    );
    assert!(
        sql.contains(r#"mgr_id AS "a.pid""#),
        "the embedded id property a.pid must still resolve through the edge map; SQL:\n{sql}"
    );
}

#[test]
fn unlabeled_select_own_table_property_plain_arrow() {
    // Same unlabeled shape, property only in the SELECT (select-builder #1006
    // branch): the label fallback must fire there too.
    let sql = translate("MATCH (a)-[r]->(b) RETURN a.name, b.name");

    assert!(sql.contains(OWN_TABLE_JOIN), "SQL:\n{sql}");
    assert!(
        sql.contains(r#"a.name AS "a.name""#),
        "a.name must resolve through the node alias; SQL:\n{sql}"
    );
    assert!(
        !sql.contains("r.name"),
        "must not reference reports.name; SQL:\n{sql}"
    );
}

#[test]
fn unlabeled_order_by_own_table_property_plain_arrow() {
    // Unlabeled + WHERE-less ORDER BY — both the order-by registration and the
    // label fallback must fire (live repro variant).
    let sql = translate("MATCH (a)-[r]->(b) RETURN a.pid, b.name ORDER BY a.name");

    assert!(sql.contains(OWN_TABLE_JOIN), "SQL:\n{sql}");
    assert!(
        sql.contains("ORDER BY a.name ASC"),
        "ORDER BY must reference the injected join alias; SQL:\n{sql}"
    );
    assert!(
        !sql.contains("r.name"),
        "must not reference reports.name; SQL:\n{sql}"
    );
}
