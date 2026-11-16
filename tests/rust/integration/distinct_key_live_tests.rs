//! #1482 review: the `DISTINCT`/grouping key's SQL runs on a live ClickHouse.
//!
//! The unit tests below only see the SQL text, and a text assertion cannot
//! see a branch ClickHouse refuses: the map branch of the key was Code 43
//! when the value was a list (ClickHouse analyses every `multiIf` branch).
//! This tier runs the generated SQL for a list, a map, and a mixed list.
//!
//! Skipped without `CLICKHOUSE_URL`, like the schema-registry live tier.

use serde_json::Value;

use clickgraph::graph_catalog::config::GraphSchemaConfig;

fn schema() -> clickgraph::graph_catalog::graph_schema::GraphSchema {
    // The queries below read no table (`system.one` only): any one-node
    // schema gives the pipeline a graph to translate against.
    GraphSchemaConfig::from_yaml_str(
        "graph_schema:\n  nodes:\n    - label: User\n      database: live\n      \
         table: users\n      node_id: id\n      property_mappings:\n        id: id\n  \
         edges: []\n",
    )
    .unwrap()
    .to_graph_schema()
    .unwrap()
}

/// The rows of `cypher`'s generated SQL, as ClickHouse's JSON output.
async fn rows(cypher: &str) -> Vec<Value> {
    let url = std::env::var("CLICKHOUSE_URL").unwrap();
    let sql = clickgraph::clickhouse_query_generator::cypher_to_sql(cypher, &schema()).unwrap();
    let response = reqwest::Client::new()
        .post(format!("{url}/?default_format=JSON"))
        .query(&[
            ("user", std::env::var("CLICKHOUSE_USER").unwrap_or_default()),
            (
                "password",
                std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default(),
            ),
        ])
        .body(sql)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let text = response.text().await.unwrap();
    assert!(status.is_success(), "{cypher}: {text}");
    let json: Value = serde_json::from_str(&text).unwrap();
    json["data"].as_array().unwrap().clone()
}

#[tokio::test]
async fn distinct_and_grouping_over_lists_and_maps_run_on_live_clickhouse() {
    if std::env::var("CLICKHOUSE_URL").is_err() {
        println!("skipping: CLICKHOUSE_URL not set (offline suite)");
        return;
    }
    for (cypher, want) in [
        // Code 43 before the fix: the map branch, analysed for a list value.
        ("UNWIND [[1],[1],[2]] AS l RETURN DISTINCT l", 2),
        // #1482: both maps print `{'a':'2020-01-15'}`; their value types
        // (Date and String) are what makes them two rows.
        (
            "UNWIND [{a: date('2020-01-15')}, {a: '2020-01-15'}] AS m RETURN DISTINCT m",
            2,
        ),
        ("UNWIND [1, 1, 2] AS x RETURN DISTINCT x", 2),
    ] {
        assert_eq!(rows(cypher).await.len(), want, "{cypher}");
    }
}
