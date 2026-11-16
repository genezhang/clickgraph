//! #1491 review: a float list orders numerically on a live ClickHouse.
//!
//! The unit test only sees the order key's SQL text. The old key kept 9
//! decimals, so doubles closer than 1e-9 compared equal and the list came
//! back in insertion order; only a real ClickHouse can pin that.
//!
//! Skipped without `CLICKHOUSE_URL`, like the schema-registry live tier.

use serde_json::Value;

use clickgraph::graph_catalog::config::GraphSchemaConfig;

fn schema() -> clickgraph::graph_catalog::graph_schema::GraphSchema {
    // `g` has no declared type: its element kind is unknown, which is the
    // path that needs the element-order key.
    GraphSchemaConfig::from_yaml_str(
        "graph_schema:\n  nodes:\n    - label: M\n      database: live_order\n      \
         table: m\n      node_id: id\n      property_mappings:\n        id: id\n        \
         g: g\n  edges: []\n",
    )
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

async fn rows(cypher: &str) -> Vec<Value> {
    let sql = clickgraph::clickhouse_query_generator::cypher_to_sql(cypher, &schema()).unwrap();
    let text = post(&sql).await;
    let json: Value = serde_json::from_str(&text).unwrap();
    json["data"].as_array().unwrap().clone()
}

#[tokio::test]
async fn a_float_list_orders_numerically_on_live_clickhouse() {
    if std::env::var("CLICKHOUSE_URL").is_err() {
        println!("skipping: CLICKHOUSE_URL not set (offline suite)");
        return;
    }
    for sql in [
        "CREATE DATABASE IF NOT EXISTS live_order",
        "DROP TABLE IF EXISTS live_order.m",
        "CREATE TABLE live_order.m (id UInt64, g Float64) ENGINE = MergeTree ORDER BY id",
        // Inserted out of order, and closer than the old 9-decimal key kept.
        "INSERT INTO live_order.m VALUES (1, 0.1234567891239), (2, 0.1234567891231), \
         (3, 0.1234567891235)",
        "INSERT INTO live_order.m VALUES (4, 3e-10), (5, 1e-10), (6, 2e-10)",
        "INSERT INTO live_order.m VALUES (7, 1e70), (8, 2e70)",
    ] {
        post(sql).await;
    }
    let got = rows("MATCH (n:M) WITH n, [n.g] AS l RETURN l, n.id AS id ORDER BY l").await;
    let ids: Vec<i64> = got.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, [5, 6, 4, 2, 3, 1, 7, 8], "{got:?}");
}
