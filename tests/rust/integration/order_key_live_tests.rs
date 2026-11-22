//! #1491 review: a float list orders numerically on a live ClickHouse.
//!
//! The unit test only sees the order key's SQL text. The old key kept 9
//! decimals, so doubles closer than 1e-9 compared equal and the list came
//! back in insertion order; only a real ClickHouse can pin that.
//!
//! Skipped without `CLICKHOUSE_URL`, like the schema-registry live tier.

use serde_json::Value;

use clickgraph::graph_catalog::config::GraphSchemaConfig;
use clickgraph::graph_catalog::graph_schema::GraphSchema;

fn schema(typed: bool, table: &str) -> GraphSchema {
    // Untyped, `g`'s element kind is unknown: the path that needs the
    // element-order key. Typed, the one known type keeps the native ORDER BY.
    let types = match typed {
        true => "      column_types: {id: UInt64, g: Float64, m: 'Map(String, Int64)'}\n",
        false => "",
    };
    GraphSchemaConfig::from_yaml_str(&format!(
        "graph_schema:\n  nodes:\n    - label: M\n      database: live_order\n      \
         table: {table}\n      node_id: id\n      property_mappings:\n        id: id\n        \
         g: g\n        m: m\n{types}  edges: []\n"
    ))
    .unwrap()
    .to_graph_schema()
    .unwrap()
}

async fn post(sql: &str) -> String {
    let url = std::env::var("CLICKHOUSE_URL").unwrap();
    let response = reqwest::Client::new()
        .post(format!("{url}/?default_format=JSON"))
        .query(&[
            ("user", std::env::var("CLICKHOUSE_USER").unwrap_or_default()),
            (
                "password",
                std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default(),
            ),
        ])
        .body(sql.to_string())
        .send()
        .await
        .unwrap();
    let status = response.status();
    let text = response.text().await.unwrap();
    assert!(status.is_success(), "{sql}: {text}");
    text
}

async fn rows(cypher: &str, typed: bool, table: &str) -> Vec<Value> {
    let sql = clickgraph::clickhouse_query_generator::cypher_to_sql(cypher, &schema(typed, table))
        .unwrap();
    let text = post(&sql).await;
    let json: Value = serde_json::from_str(&text).unwrap();
    json["data"].as_array().unwrap().clone()
}

async fn setup(table: &str) {
    for sql in [
        "CREATE DATABASE IF NOT EXISTS live_order",
        &format!("DROP TABLE IF EXISTS live_order.{table}"),
        &format!("CREATE TABLE live_order.{table} (id UInt64, g Float64, m Map(String, Int64)) ENGINE = MergeTree ORDER BY id"),
        // Inserted out of order, and closer than the old 9-decimal key kept.
        &format!("INSERT INTO live_order.{table} VALUES \
         (1, 0.1234567891239, map('k', 12)), (2, 0.1234567891231, map('k', 3)), \
         (3, 0.1234567891235, map('k', 5))"),
        &format!("INSERT INTO live_order.{table} VALUES (4, 3e-10, map()), (5, 1e-10, map()), (6, 2e-10, map())"),
        &format!("INSERT INTO live_order.{table} VALUES (7, 1e70, map()), (8, 2e70, map())"),
    ] {
        post(sql).await;
    }
}

#[tokio::test]
async fn a_float_list_orders_numerically_on_live_clickhouse() {
    if std::env::var("CLICKHOUSE_URL").is_err() {
        println!("skipping: CLICKHOUSE_URL not set (offline suite)");
        return;
    }
    setup("m_float").await;
    let got = rows(
        "MATCH (n:M) WITH n, [n.g] AS l RETURN l, n.id AS id ORDER BY l",
        false,
        "m_float",
    )
    .await;
    let ids: Vec<i64> = got.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, [5, 6, 4, 2, 3, 1, 7, 8], "{got:?}");
    // With the type known the one-element list keeps the native ORDER BY
    // (review: the key cost 7.5 KB of SQL where main took 3 KB) and orders
    // the same.
    let sql = clickgraph::clickhouse_query_generator::cypher_to_sql(
        "MATCH (n:M) WITH n, [n.g] AS l RETURN l, n.id AS id ORDER BY l",
        &schema(true, "m_float"),
    )
    .unwrap();
    assert!(!sql.contains("arrayMap(x -> tuple("), "{sql}");
    let got = rows(
        "MATCH (n:M) WITH n, [n.g] AS l RETURN l, n.id AS id ORDER BY l",
        true,
        "m_float",
    )
    .await;
    let ids: Vec<i64> = got.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, [5, 6, 4, 2, 3, 1, 7, 8], "{got:?}");
}

/// Review: a list of maps of one value type (`collect(n.m)` of a `Map`
/// column) keeps the native numeric order, not the text order of the key
/// (`{k:12}` before `{k:3}`).
#[tokio::test]
async fn a_list_of_maps_orders_numerically_on_live_clickhouse() {
    if std::env::var("CLICKHOUSE_URL").is_err() {
        println!("skipping: CLICKHOUSE_URL not set (offline suite)");
        return;
    }
    setup("m_maps").await;
    let got = rows(
        "MATCH (n:M) WHERE n.id <= 3 \
         WITH n.id AS id, collect(n.m) AS l RETURN id, l ORDER BY l",
        true,
        "m_maps",
    )
    .await;
    let ids: Vec<u64> = got.iter().map(|r| r["id"].as_u64().unwrap()).collect();
    assert_eq!(ids, [2, 3, 1], "{got:?}");
}
