//! Regression tests for the Neo4j-Browser unlabeled-node-over-denormalized-schema bug.
//!
//! When a query uses an unlabeled `(n)` and ALL of the schema's candidate node
//! labels are DENORMALIZED (embedded in an edge table as virtual node_ids), the
//! planner used to drop the scan entirely: a standalone (node-only) `(n)` left the
//! GraphNode with an `Empty` input because TypeInference Phase 3 skips ViewScan
//! resolution for denormalized labels (it relies on an enclosing GraphRel for
//! direction context, which a node-only query does not have). The result was
//! invalid SQL with NO FROM clause (`MATCH (n) RETURN n.ip_address` →
//! `SELECT n.ip_address LIMIT 5`) or an empty UNION.
//!
//! The fix materializes the denormalized node-only scan for STANDALONE untyped
//! nodes, mirroring the LABELED form: a `UNION DISTINCT` of the from/to position
//! scans, each `FROM` the denormalized source table, with the enclosing
//! Projection/Filter distributed into each branch.
//!
//! A LABELED denormalized node `(n:IP)` always worked (it materializes that scan);
//! these tests assert the UNLABELED form now produces the same shape.

use crate::{graph_catalog::config::GraphSchemaConfig, graph_catalog::graph_schema::GraphSchema};

/// All three node labels (IP, Domain, ResolvedIP) are DENORMALIZED into the single
/// `zeek.dns_log` edge table — the same shape as `schemas/examples/zeek_dns_log.yaml`.
const SCHEMA_YAML: &str = r#"
name: zeek_dns
graph_schema:
  nodes:
    - label: IP
      database: zeek
      table: dns_log
      node_id: ip_address
      property_mappings: {}
      from_node_properties:
        ip_address: "id.orig_h"
      to_node_properties:
        ip_address: "id.resp_h"
    - label: Domain
      database: zeek
      table: dns_log
      node_id: domain_name
      property_mappings: {}
      from_node_properties:
        domain_name: query
      to_node_properties:
        domain_name: query
    - label: ResolvedIP
      database: zeek
      table: dns_log
      node_id: answers
      property_mappings: {}
      from_node_properties:
        answers: answers
      to_node_properties:
        answers: answers
  edges:
    - type: REQUESTED
      database: zeek
      table: dns_log
      from_id: "id.orig_h"
      to_id: query
      from_node: IP
      to_node: Domain
      edge_id: uid
      property_mappings:
        uid: uid
    - type: RESOLVED_TO
      database: zeek
      table: dns_log
      from_id: query
      to_id: answers
      from_node: Domain
      to_node: ResolvedIP
      edge_id: [uid, answers]
      property_mappings:
        uid: uid
"#;

fn schema() -> GraphSchema {
    GraphSchemaConfig::from_yaml_str(SCHEMA_YAML)
        .expect("parse schema yaml")
        .to_graph_schema()
        .expect("build graph schema")
}

/// Translate Cypher → ClickHouse SQL via the exact entrypoint the `cg` CLI and the
/// embedded `Connection` use (`evaluate_read_statement` + full analyzer/optimizer
/// pipeline), so the asserted SQL matches sql_only output faithfully.
fn cypher_to_sql(cypher: &str) -> String {
    crate::clickhouse_query_generator::cypher_to_sql(cypher, &schema(), 100).expect("cypher_to_sql")
}

/// Count non-overlapping occurrences of `needle` in `haystack`.
fn count(haystack: &str, needle: &str) -> usize {
    haystack.matches(needle).count()
}

#[test]
fn unlabeled_node_property_over_all_denormalized_schema_materializes_union() {
    // `n.ip_address` is only present on the (denormalized) IP label, so type
    // inference narrows to IP and materializes its from/to denormalized scan.
    let sql = cypher_to_sql("MATCH (n) RETURN n.ip_address LIMIT 5");

    // The scan MUST be materialized FROM the denormalized source table — NOT a
    // FROM-less SELECT (the bug).
    assert!(
        sql.contains("FROM zeek.dns_log"),
        "unlabeled denormalized node-only scan must materialize FROM the source table; SQL:\n{sql}"
    );

    // It must be a UNION of the from/to position denormalized scans (≥2 scans of
    // the source table), not a single FROM-less projection or an empty UNION.
    assert!(
        count(&sql, "zeek.dns_log") >= 2,
        "expected a UNION of denormalized scans over zeek.dns_log; SQL:\n{sql}"
    );
    assert!(
        sql.to_uppercase().contains("UNION"),
        "expected a UNION of per-position denormalized scans; SQL:\n{sql}"
    );

    // The from/to denormalized columns must be projected (id.orig_h / id.resp_h).
    assert!(
        sql.contains("id.orig_h") && sql.contains("id.resp_h"),
        "expected both from (id.orig_h) and to (id.resp_h) position columns; SQL:\n{sql}"
    );

    // Guard against the regression shape: a FROM-less `SELECT ... LIMIT` with no
    // table at all.
    assert!(
        !sql.contains("( UNION ALL )"),
        "must not emit an empty UNION; SQL:\n{sql}"
    );
}

#[test]
fn unlabeled_node_is_not_null_filter_over_all_denormalized_schema_materializes_union() {
    // `n.answers` exists only on the (denormalized) ResolvedIP label. The WHERE +
    // DISTINCT projection must be pushed into each materialized denormalized branch.
    let sql = cypher_to_sql(
        "MATCH (n) WHERE n.answers IS NOT NULL RETURN DISTINCT 'node' AS entity, n.answers AS answers LIMIT 25",
    );

    assert!(
        sql.contains("FROM zeek.dns_log"),
        "unlabeled denormalized node-only scan must materialize FROM the source table; SQL:\n{sql}"
    );
    assert!(
        count(&sql, "zeek.dns_log") >= 2,
        "expected a UNION of denormalized scans over zeek.dns_log; SQL:\n{sql}"
    );
    // The IS NOT NULL predicate must be distributed into the branches (not dropped).
    assert!(
        sql.contains("IS NOT NULL"),
        "WHERE predicate must be preserved in the materialized scan; SQL:\n{sql}"
    );
    assert!(
        !sql.contains("( UNION ALL )"),
        "must not emit an empty UNION; SQL:\n{sql}"
    );
}

#[test]
fn whole_node_return_over_heterogeneous_denormalized_schema_has_aligned_union() {
    // Whole-node `RETURN n` over a heterogeneous all-denormalized schema (IP,
    // Domain, ResolvedIP — each with a DIFFERENT property set) expands into a
    // UNION where every label-position leaf branch must project the SAME aligned
    // column set (each label's real columns + NULL for the others), wrapped in a
    // subquery whose OUTER SELECT projects those columns.
    //
    // Two historical defects this guards against:
    //   1. An EMPTY outer projection (`SELECT  FROM (...)`) — ClickHouse Code 62.
    //   2. Nested per-label `UNION DISTINCT` sub-branches padded inconsistently,
    //      giving mismatched column counts across leaves.
    let sql = cypher_to_sql("MATCH (n) RETURN n LIMIT 5");

    // Defect 1: the outer SELECT over the union must NOT be empty.
    assert!(
        !sql.contains("SELECT  FROM"),
        "outer SELECT over the heterogeneous union must not be empty; SQL:\n{sql}"
    );
    assert!(
        sql.contains(") AS __union"),
        "heterogeneous whole-node union must be wrapped in a subquery; SQL:\n{sql}"
    );

    // Defect 2: every leaf branch must project the SAME aligned column set. This
    // heterogeneous schema yields three distinct node properties (ip_address,
    // domain_name, answers); each of those aligned column aliases must appear the
    // same number of times — once per leaf branch. Equal, non-zero counts prove
    // consistent arity across all leaves (padded with NULL where absent).
    let answers = count(&sql, "AS \"n.answers\"");
    let domain = count(&sql, "AS \"n.domain_name\"");
    let ip = count(&sql, "AS \"n.ip_address\"");
    assert!(
        answers > 0 && answers == domain && domain == ip,
        "every leaf branch must project the same aligned column set \
         (answers={answers}, domain_name={domain}, ip_address={ip}); SQL:\n{sql}"
    );
}

#[test]
fn labeled_denormalized_node_property_is_unchanged() {
    // Control: the LABELED form already worked and must be byte-for-byte preserved
    // (the UNLABELED fix must compose with, not alter, the labeled path).
    let sql = cypher_to_sql("MATCH (n:IP) RETURN n.ip_address LIMIT 5");

    assert!(
        sql.contains("FROM zeek.dns_log"),
        "labeled denormalized node must materialize FROM the source table; SQL:\n{sql}"
    );
    assert!(
        sql.contains("id.orig_h") && sql.contains("id.resp_h"),
        "labeled IP scan must project both from/to position columns; SQL:\n{sql}"
    );
}
