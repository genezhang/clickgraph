//! Regression tests for whole-node `RETURN n` visualization over a DENORMALIZED
//! node whose `node_id` is VIRTUAL (its declared name is not itself one of the
//! exposed properties).
//!
//! A denormalized node embeds its properties in a source table via
//! `from_node_properties`/`to_node_properties` and has an empty `property_mappings`.
//! Clicking a node label in Neo4j Browser runs `MATCH (n:Label) RETURN n`.
//!
//! The Bolt result transformer (`server/bolt_protocol/result_transformer.rs`)
//! builds each node's `elementId` by matching the node's declared `node_id` NAME
//! against the projected property names. When `node_id` is virtual — e.g. zeek
//! `Domain` with `node_id: query` exposed only as `name: query` — the whole-node
//! projection used to emit ONLY `n.query AS "n.name"`, so nothing was projected
//! under the id name `query`. The transformer then logged "Missing ID column
//! 'query'" and dropped the node → Browser "unknown error".
//!
//! After the fix, `ProjectedColumnsResolver` injects the node_id column under its
//! declared name (`n.query AS "n.query"`) so the transformer can build the id,
//! mirroring how a normal node (`MATCH (u:User) RETURN u`) projects `user_id`.
//! When `node_id` is already an exposed property (e.g. Airport `node_id: code`),
//! nothing extra is injected.

use crate::clickhouse_query_generator::cypher_to_sql;
use crate::graph_catalog::config::GraphSchemaConfig;
use crate::server::query_context::{set_current_schema, with_query_context, QueryContext};
use std::sync::Arc;

/// Denormalized node `Domain` with a VIRTUAL scalar node_id (`query`) that is not
/// itself an exposed property (only `name: query` is).
const SCHEMA_YAML: &str = r#"
name: zeek_virtual_id_test
version: "1.0"
graph_schema:
  nodes:
    - label: Domain
      database: zeek
      table: dns_log
      node_id: query
      property_mappings: {}
      from_node_properties:
        name: query
      to_node_properties:
        name: query
    - label: IP
      database: zeek
      table: dns_log
      node_id: "id.orig_h"
      property_mappings: {}
      from_node_properties:
        ip: "id.orig_h"
      to_node_properties:
        ip: "id.resp_h"
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
"#;

/// Translate Cypher → SQL through the same task-local-context path the `cg` tool
/// and embedded API use.
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

#[test]
fn whole_node_return_projects_virtual_node_id_column() {
    let sql = translate("MATCH (n:Domain) RETURN n LIMIT 25");

    // The node_id column must be projected under its declared name so the Bolt
    // transformer can build the elementId (previously MISSING → "Missing ID column").
    assert!(
        sql.contains(r#"n.query AS "n.query""#),
        "whole-node RETURN n must project the node_id column as n.query; SQL:\n{sql}"
    );

    // The existing property projection must remain unchanged.
    assert!(
        sql.contains(r#"n.query AS "n.name""#),
        "the `name` property projection must be preserved; SQL:\n{sql}"
    );
}

#[test]
fn property_only_return_is_unchanged() {
    // `RETURN n.name` is a property-specific lookup and must NOT gain an id column.
    let sql = translate("MATCH (n:Domain) RETURN n.name");

    assert!(
        sql.contains(r#"n.query AS "n.name""#),
        "the `name` property projection must be present; SQL:\n{sql}"
    );
    assert!(
        !sql.contains(r#"AS "n.query""#),
        "property-only RETURN n.name must not project a separate id column; SQL:\n{sql}"
    );
}
