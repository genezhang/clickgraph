//! Regression tests for "id-via-FK" access to a denormalized node reached
//! through a FOREIGN edge (Stage 1).
//!
//! A denormalized node (e.g. `Airport`) embeds its properties in a source table
//! (`flights`) via `from_node_properties`/`to_node_properties` and has a VIRTUAL
//! node_id (no own `property_mappings`). When such a node is reached through an
//! edge that lives in a DIFFERENT table (e.g. `LOCATED_IN` in `airport_cities`),
//! the node's node_id is carried by the edge's FK column (`airport_code`) — NOT
//! by the source-table columns (`Origin`/`Dest`).
//!
//! Before the fix the planner copied the node's source-table denormalized props
//! onto the foreign edge and emitted broken SQL:
//!   FROM airport_cities AS a
//!   INNER JOIN airport_cities AS t1 ON t1.airport_code = a.Origin   -- phantom self-join
//!   INNER JOIN cities AS c ON c.city_id = t1.city_id
//! referencing `Origin` (a `flights` column absent from `airport_cities`).
//!
//! The fix treats the foreign-edge denorm node as embedded in the edge with its
//! node_id mapped to the edge FK column: `a.code` → edge `airport_code`, no scan
//! of the node's source table and no phantom self-join.

use crate::clickhouse_query_generator::cypher_to_sql;
use crate::graph_catalog::config::GraphSchemaConfig;
use crate::server::query_context::{set_current_schema, with_query_context, QueryContext};
use std::sync::Arc;

const SCHEMA_YAML: &str = include_str!("../../../schemas/test/mixed_denorm_test.yaml");

/// Translate Cypher → SQL through the same task-local-context path the `cg`
/// tool and embedded API use (denormalized-alias registration is task-local).
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
fn located_in_resolves_id_via_edge_fk_no_phantom_join() {
    let sql = translate("MATCH (a:Airport)-[:LOCATED_IN]->(c:City) RETURN a.code, c.name");

    // a.code resolves to the edge's FK column (airport_code), NOT flights.Origin.
    assert!(
        sql.contains(r#"airport_code AS "a.code""#),
        "a.code must resolve to the edge FK column airport_code; SQL:\n{sql}"
    );

    // City joined normally on its real id column.
    assert!(
        sql.contains("test_integration.cities") && sql.contains("city_id"),
        "City must join on city_id; SQL:\n{sql}"
    );

    // No reference to the node's source-table columns / table, and no phantom
    // self-join of the airport_cities edge table.
    assert!(
        !sql.contains("Origin") && !sql.contains("Dest"),
        "must not reference flights columns Origin/Dest; SQL:\n{sql}"
    );
    assert!(
        !sql.contains("flights"),
        "must not scan the node's source table flights; SQL:\n{sql}"
    );
    // Exactly one occurrence of the edge table (no self-join).
    assert_eq!(
        sql.matches("test_integration.airport_cities").count(),
        1,
        "edge table airport_cities must appear exactly once (no phantom self-join); SQL:\n{sql}"
    );
}

#[test]
fn serves_resolves_to_node_id_via_edge_fk() {
    // Airport is the TO node here (City)-[:SERVES]->(Airport); the to_id column
    // (airport_code) carries the Airport node_id.
    let sql = translate("MATCH (c:City)-[:SERVES]->(a:Airport) RETURN c.name, a.code");

    assert!(
        sql.contains(r#"airport_code AS "a.code""#),
        "a.code must resolve to the edge FK column airport_code; SQL:\n{sql}"
    );
    assert!(
        !sql.contains("Origin") && !sql.contains("Dest"),
        "must not reference flights columns Origin/Dest; SQL:\n{sql}"
    );
    assert!(
        !sql.contains("flights"),
        "must not scan the node's source table flights; SQL:\n{sql}"
    );
    assert_eq!(
        sql.matches("test_integration.city_airports").count(),
        1,
        "edge table city_airports must appear exactly once (no phantom self-join); SQL:\n{sql}"
    );
}

#[test]
fn coupled_flight_edge_unchanged() {
    // The FLIGHT edge is COUPLED (edge AND both Airport endpoints live in
    // `flights`). This must keep using the source-table columns directly.
    let sql = translate("MATCH (a:Airport)-[:FLIGHT]->(b:Airport) RETURN a.code, b.code");

    assert!(
        sql.contains(r#"Origin AS "a.code""#),
        "coupled a.code must resolve to flights.Origin; SQL:\n{sql}"
    );
    assert!(
        sql.contains(r#"Dest AS "b.code""#),
        "coupled b.code must resolve to flights.Dest; SQL:\n{sql}"
    );
    assert_eq!(
        sql.matches("test_integration.flights").count(),
        1,
        "coupled FLIGHT must scan flights exactly once (no extra joins); SQL:\n{sql}"
    );
}
