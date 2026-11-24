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
        // The session every executor runs the SQL in (`join_use_nulls`,
        // `use_variant_as_common_type`).
        .query(&clickgraph::sql_generator::session_settings())
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

fn mixed_schema() -> GraphSchema {
    GraphSchemaConfig::from_yaml_str(
        "graph_schema:\n  nodes:\n    - label: X\n      database: live_order\n      \
         table: m_mix\n      node_id: id\n      property_mappings:\n        id: id\n        \
         s: s\n  edges: []\n",
    )
    .unwrap()
    .to_graph_schema()
    .unwrap()
}

async fn mixed_rows(cypher: &str) -> Vec<Value> {
    let sql =
        clickgraph::clickhouse_query_generator::cypher_to_sql(cypher, &mixed_schema()).unwrap();
    let json: Value = serde_json::from_str(&post(&sql).await).unwrap();
    json["data"].as_array().unwrap().clone()
}

/// #1491: a list of values of no common type works on every ClickHouse the
/// SQL runs on, as a list of literals (`array(…)`, as the constant `[1, 'a']`
/// has no type on 26.7 either) and as a list of columns (25.8 needs
/// `use_variant_as_common_type`, which the session carries).
#[tokio::test]
async fn a_list_of_values_of_no_common_type_works_on_live_clickhouse() {
    if std::env::var("CLICKHOUSE_URL").is_err() {
        println!("skipping: CLICKHOUSE_URL not set (offline suite)");
        return;
    }
    for sql in [
        "CREATE DATABASE IF NOT EXISTS live_order",
        "DROP TABLE IF EXISTS live_order.m_mix",
        "CREATE TABLE live_order.m_mix (id UInt64, s String) ENGINE = MergeTree ORDER BY id",
        "INSERT INTO live_order.m_mix VALUES (1, 'b'), (2, 'a'), (3, 'b')",
    ] {
        post(sql).await;
    }
    let got = mixed_rows("RETURN [1, 'a', true, null, 2.5] AS l").await;
    assert_eq!(
        got[0]["l"],
        serde_json::json!([1, "a", true, null, 2.5]),
        "{got:?}"
    );
    let got = mixed_rows("RETURN [[1, 'a'], ['b', 2]] AS l").await;
    assert_eq!(
        got[0]["l"],
        serde_json::json!([[1, "a"], ["b", 2]]),
        "{got:?}"
    );
    // A list of columns of two types, grouped and ordered by.
    let got =
        mixed_rows("MATCH (n:X) WITH [n.s, n.id] AS l, count(*) AS c RETURN l, c ORDER BY l DESC")
            .await;
    let lists: Vec<Value> = got.iter().map(|r| r["l"].clone()).collect();
    assert_eq!(
        lists,
        [
            serde_json::json!(["b", 3]),
            serde_json::json!(["b", 1]),
            serde_json::json!(["a", 2])
        ],
        "{got:?}"
    );
}

/// #1491: Cypher orders a list element by element, and a string before a
/// number whatever their values; a list of scalars of no one type used to
/// fail in ClickHouse's ORDER BY (Code 44).
#[tokio::test]
async fn a_list_of_scalars_of_no_one_type_orders_by_type_rank_on_live_clickhouse() {
    if std::env::var("CLICKHOUSE_URL").is_err() {
        println!("skipping: CLICKHOUSE_URL not set (offline suite)");
        return;
    }
    let got =
        mixed_rows("UNWIND [[1, 'a'], ['b', 2], [2.5, 'c'], [2, 'a']] AS l RETURN l ORDER BY l")
            .await;
    let lists: Vec<Value> = got.iter().map(|r| r["l"].clone()).collect();
    assert_eq!(
        lists,
        [
            serde_json::json!(["b", 2]),
            serde_json::json!([1, "a"]),
            serde_json::json!([2, "a"]),
            serde_json::json!([2.5, "c"])
        ],
        "{got:?}"
    );
    let got = mixed_rows("UNWIND [1, 'a', 2.5] AS x RETURN [x, 1] AS l ORDER BY l DESC").await;
    let lists: Vec<Value> = got.iter().map(|r| r["l"].clone()).collect();
    assert_eq!(
        lists,
        [
            serde_json::json!([2.5, 1]),
            serde_json::json!([1, 1]),
            serde_json::json!(["a", 1])
        ],
        "{got:?}"
    );
    // Lists of one type keep the native order: `[3, 1]` after `[2, 5]`.
    let got = mixed_rows("UNWIND [[3, 1], [2, 5], [1, 9]] AS l RETURN l ORDER BY l DESC").await;
    let lists: Vec<Value> = got.iter().map(|r| r["l"].clone()).collect();
    assert_eq!(
        lists,
        [
            serde_json::json!([3, 1]),
            serde_json::json!([2, 5]),
            serde_json::json!([1, 9])
        ],
        "{got:?}"
    );
}
