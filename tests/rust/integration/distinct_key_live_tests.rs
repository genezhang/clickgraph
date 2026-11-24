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

/// Runs `sql` on the live ClickHouse and returns its (JSON) response text.
async fn run(sql: String, what: &str) -> String {
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
        .body(sql)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let text = response.text().await.unwrap();
    assert!(status.is_success(), "{what}: {text}");
    text
}

/// The rows of `cypher`'s generated SQL over `schema`, as ClickHouse's JSON.
async fn rows_over(
    cypher: &str,
    schema: &clickgraph::graph_catalog::graph_schema::GraphSchema,
) -> Vec<Value> {
    let sql = clickgraph::clickhouse_query_generator::cypher_to_sql(cypher, schema).unwrap();
    let json: Value = serde_json::from_str(&run(sql, cypher).await).unwrap();
    json["data"].as_array().unwrap().clone()
}

/// The rows of `cypher`'s generated SQL (reading no table), as JSON.
async fn rows(cypher: &str) -> Vec<Value> {
    rows_over(cypher, &schema()).await
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

/// #1482 review: the key carries a container's type so that `[date]` and
/// `['…']` (one text) are two groups, but the same value held in two storage
/// types (`Array(Int32)` and `Array(Int64)`, `LowCardinality(String)` and
/// `String` elements, `Nullable` ones, a `Map` of either width) is one, as
/// for a scalar. A `UNION` of two such sources must not repeat a row.
#[tokio::test]
async fn a_union_of_one_value_in_two_storage_types_is_one_row_on_live_clickhouse() {
    if std::env::var("CLICKHOUSE_URL").is_err() {
        println!("skipping: CLICKHOUSE_URL not set (offline suite)");
        return;
    }
    for ddl in [
        "CREATE DATABASE IF NOT EXISTS dk_live",
        "DROP TABLE IF EXISTS dk_live.a",
        "DROP TABLE IF EXISTS dk_live.b",
        "CREATE TABLE dk_live.a (id UInt64, tags Array(Int32), names Array(String), \
         mp Map(String, Int32), n Array(Int64), dt Array(Date), f Array(Float32)) \
         ENGINE = MergeTree ORDER BY id",
        "CREATE TABLE dk_live.b (id UInt64, tags Array(Int64), \
         names Array(LowCardinality(String)), mp Map(String, Int64), \
         nn Array(Nullable(Int64)), s Array(String), f Array(Float64)) \
         ENGINE = MergeTree ORDER BY id",
        "INSERT INTO dk_live.a VALUES (1, [1, 2], ['a'], {'k': 1}, [1], ['2020-01-15'], [1.5]), \
         (2, [3], ['b'], {'k': 2}, [2], ['2021-02-02'], [2.5])",
        "INSERT INTO dk_live.b VALUES (1, [1, 2], ['a'], {'k': 1}, [1], ['2020-01-15'], [1.5]), \
         (2, [4], ['c'], {'k': 3}, [NULL], ['2021-03-03'], [3.5])",
    ] {
        run(ddl.to_string(), ddl).await;
    }
    let schema = GraphSchemaConfig::from_yaml_str(
        "graph_schema:\n  nodes:\n    - label: A\n      database: dk_live\n      table: a\n      \
         node_id: id\n      property_mappings: {id: id, tags: tags, names: names, mp: mp, n: n, dt: dt, f: f}\n    \
         - label: B\n      database: dk_live\n      table: b\n      node_id: id\n      \
         property_mappings: {id: id, tags: tags, names: names, mp: mp, nn: nn, s: s, f: f}\n  edges: []\n",
    )
    .unwrap()
    .to_graph_schema()
    .unwrap();
    for (cypher, want) in [
        // Array(Int32) / Array(Int64): [1, 2] is held by both.
        (
            "MATCH (n:A) RETURN n.tags AS t UNION MATCH (m:B) RETURN m.tags AS t",
            3,
        ),
        // Array(String) / Array(LowCardinality(String)).
        (
            "MATCH (n:A) RETURN n.names AS t UNION MATCH (m:B) RETURN m.names AS t",
            3,
        ),
        // Map(String, Int32) / Map(String, Int64).
        (
            "MATCH (n:A) RETURN n.mp AS t UNION MATCH (m:B) RETURN m.mp AS t",
            3,
        ),
        // Array(Int64) / Array(Nullable(Int64)): [1] is held by both.
        (
            "MATCH (n:A) RETURN n.n AS t UNION MATCH (m:B) RETURN m.nn AS t",
            3,
        ),
        // Array(Float32) / Array(Float64).
        (
            "MATCH (n:A) RETURN n.f AS t UNION MATCH (m:B) RETURN m.f AS t",
            3,
        ),
        // What is NOT one value: dates and strings that print the same.
        (
            "MATCH (n:A) RETURN n.dt AS t UNION MATCH (m:B) RETURN m.s AS t",
            4,
        ),
    ] {
        assert_eq!(rows_over(cypher, &schema).await.len(), want, "{cypher}");
    }
}

/// #1482: a container inside a container is keyed by what is in it, to the
/// depth the key looks (`KEY_LEVELS`): a date in a map in a map in a list is a
/// date, not the string of its text. A map literal holds `Dynamic` values,
/// which hide their types from the map's own, so each is read in turn.
#[tokio::test]
async fn nested_containers_are_keyed_by_what_is_in_them_on_live_clickhouse() {
    if std::env::var("CLICKHOUSE_URL").is_err() {
        println!("skipping: CLICKHOUSE_URL not set (offline suite)");
        return;
    }
    let nested = |n: usize, leaf: &str| (0..n).fold(leaf.to_string(), |s, _| format!("{{k: {s}}}"));
    let mut cases: Vec<(String, usize)> = vec![
        // A date and a string that print alike, in a map, a list, a map in
        // a list, a list in a map, a map in a map.
        ("RETURN {a: date('2020-01-15')} AS x UNION RETURN {a: '2020-01-15'} AS x".into(), 2),
        ("RETURN [date('2020-01-15')] AS x UNION RETURN ['2020-01-15'] AS x".into(), 2),
        ("RETURN [{a: date('2020-01-15')}] AS x UNION RETURN [{a: '2020-01-15'}] AS x".into(), 2),
        ("RETURN {a: [date('2020-01-15')]} AS x UNION RETURN {a: ['2020-01-15']} AS x".into(), 2),
        // The limit: a list in a list whose elements hide their types (a
        // `Dynamic` is cast to `Array(Dynamic)`, not to a list of lists) is
        // keyed by its type and its text.
        (
            "RETURN [[{a: date('2020-01-15')}]] AS x UNION RETURN [[{a: '2020-01-15'}]] AS x"
                .into(),
            1,
        ),
        // The same value is one row, and numbers of any width are one class.
        (
            "RETURN {a: {b: date('2020-01-15')}} AS x UNION RETURN {a: {b: date('2020-01-15')}} AS x"
                .into(),
            1,
        ),
        ("RETURN {a: {b: 1}} AS x UNION RETURN {a: {b: 1.0}} AS x".into(), 1),
        ("RETURN [{a: 1}] AS x UNION RETURN [{a: toInteger(1)}] AS x".into(), 1),
        // Different values stay different.
        ("RETURN {a: {b: 1}} AS x UNION RETURN {a: {b: 2}} AS x".into(), 2),
        ("RETURN {a: {b: 1}} AS x UNION RETURN {a: {c: 1}} AS x".into(), 2),
        ("RETURN [{a: 'x,'}] AS x UNION RETURN [{a: 'x'}, {a: ''}] AS x".into(), 2),
    ];
    // Maps in maps, as deep as the key looks (`KEY_LEVELS`: eight), and the
    // limit: a ninth map deep is keyed by its type and text again.
    for n in 1..=8 {
        cases.push((
            format!(
                "RETURN {} AS x UNION RETURN {} AS x",
                nested(n, "date('2020-01-15')"),
                nested(n, "'2020-01-15'")
            ),
            2,
        ));
    }
    cases.push((
        format!(
            "RETURN {} AS x UNION RETURN {} AS x",
            nested(9, "date('2020-01-15')"),
            nested(9, "'2020-01-15'")
        ),
        1,
    ));
    for (cypher, want) in cases {
        assert_eq!(rows(&cypher).await.len(), want, "{cypher}");
    }
}

/// #1482: the same instant is one value whether a `DateTime` or a
/// `DateTime64` holds it (a container printed its text, `00:00:00` and
/// `00:00:00.000`), in a list or in a map, as it is for a scalar.
#[tokio::test]
async fn one_instant_in_two_datetime_types_is_one_row_in_a_container_on_live_clickhouse() {
    if std::env::var("CLICKHOUSE_URL").is_err() {
        println!("skipping: CLICKHOUSE_URL not set (offline suite)");
        return;
    }
    for ddl in [
        "CREATE DATABASE IF NOT EXISTS dk_live",
        "DROP TABLE IF EXISTS dk_live.c",
        "DROP TABLE IF EXISTS dk_live.d",
        "CREATE TABLE dk_live.c (id UInt64, dt Array(DateTime('UTC')), \
         dm Map(String, DateTime('UTC'))) ENGINE = MergeTree ORDER BY id",
        "CREATE TABLE dk_live.d (id UInt64, dt Array(DateTime64(3, 'UTC')), \
         dm Map(String, DateTime64(3, 'UTC'))) ENGINE = MergeTree ORDER BY id",
        "INSERT INTO dk_live.c VALUES \
         (1, ['2020-01-01 00:00:00'], {'k': '2020-01-01 00:00:00'}), \
         (2, ['2021-02-02 03:04:05'], {'k': '2021-02-02 03:04:05'})",
        "INSERT INTO dk_live.d VALUES \
         (1, ['2020-01-01 00:00:00.000'], {'k': '2020-01-01 00:00:00.000'}), \
         (2, ['2021-02-02 03:04:05.500'], {'k': '2021-02-02 03:04:05.500'})",
    ] {
        run(ddl.to_string(), ddl).await;
    }
    let schema = GraphSchemaConfig::from_yaml_str(
        "graph_schema:\n  nodes:\n    - label: C\n      database: dk_live\n      table: c\n      \
         node_id: id\n      property_mappings: {id: id, dt: dt, dm: dm}\n    \
         - label: D\n      database: dk_live\n      table: d\n      node_id: id\n      \
         property_mappings: {id: id, dt: dt, dm: dm}\n  edges: []\n",
    )
    .unwrap()
    .to_graph_schema()
    .unwrap();
    for (cypher, want) in [
        // The first instant is held by both; the second differs by 0.5 s.
        (
            "MATCH (n:C) RETURN n.dt AS t UNION MATCH (m:D) RETURN m.dt AS t",
            3,
        ),
        (
            "MATCH (n:C) RETURN n.dm AS t UNION MATCH (m:D) RETURN m.dm AS t",
            3,
        ),
    ] {
        assert_eq!(rows_over(cypher, &schema).await.len(), want, "{cypher}");
    }
}

/// #1482 review: an `Enum` label that reads like a type name (`'Timeout'`,
/// `'DateTime'`) does not make a list one whose elements are looked into: the
/// list is its classed type and its text, as one of `String` is, so the same
/// value in either is one row.
#[tokio::test]
async fn an_enum_label_that_reads_like_a_type_keeps_a_list_plain_on_live_clickhouse() {
    if std::env::var("CLICKHOUSE_URL").is_err() {
        println!("skipping: CLICKHOUSE_URL not set (offline suite)");
        return;
    }
    for ddl in [
        "CREATE DATABASE IF NOT EXISTS dk_live",
        "DROP TABLE IF EXISTS dk_live.e",
        "DROP TABLE IF EXISTS dk_live.s",
        "CREATE TABLE dk_live.e (id UInt64, t Array(Enum8('Timeout' = 1, 'DateTime' = 2, 'a' = 3))) \
         ENGINE = MergeTree ORDER BY id",
        "CREATE TABLE dk_live.s (id UInt64, t Array(String)) ENGINE = MergeTree ORDER BY id",
        "INSERT INTO dk_live.e VALUES (1, ['a']), (2, ['Timeout']), (3, ['DateTime', 'a'])",
        "INSERT INTO dk_live.s VALUES (1, ['a']), (2, ['x']), (3, ['DateTime', 'a'])",
    ] {
        run(ddl.to_string(), ddl).await;
    }
    let schema = GraphSchemaConfig::from_yaml_str(
        "graph_schema:\n  nodes:\n    - label: E\n      database: dk_live\n      table: e\n      \
         node_id: id\n      property_mappings: {id: id, t: t}\n    \
         - label: S\n      database: dk_live\n      table: s\n      node_id: id\n      \
         property_mappings: {id: id, t: t}\n  edges: []\n",
    )
    .unwrap()
    .to_graph_schema()
    .unwrap();
    // ['a'] and ['DateTime', 'a'] are in both; ['Timeout'] and ['x'] in one.
    let cypher = "MATCH (n:E) RETURN n.t AS t UNION MATCH (m:S) RETURN m.t AS t";
    assert_eq!(rows_over(cypher, &schema).await.len(), 4, "{cypher}");
}
