//! P-4c S0.5: every read entry point translates through one seam
//! (`clickgraph::translate::translate_read`), each in its own per-query
//! context. Drives the real router via `tower::ServiceExt::oneshot` with a stub
//! executor, as `sql_generation_handler_comment_tests.rs` does.
//!
//! Locks three defects the seam fixed:
//! - `/query/sql` skipped the analyzer passes and the `id()` rewrite, so it
//!   could return SQL different from what `/query` runs;
//! - `/query/sql` translated outside any query context, so it used the
//!   process-global alias counters, and concurrent requests rewound each
//!   other's counters mid-translation (outputs differed and some requests
//!   failed planning);
//! - SQL handed back for external execution lacked `join_use_nulls` (#1314).

use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tower::ServiceExt; // for `oneshot`

use clickgraph::config::ServerConfig;
use clickgraph::executor::{ExecutorError, QueryExecutor};
use clickgraph::graph_catalog::config::GraphSchemaConfig;
use clickgraph::server::{build_router, AppState, GLOBAL_SCHEMAS};

struct StubExecutor;

#[async_trait]
impl QueryExecutor for StubExecutor {
    async fn execute_json(
        &self,
        _sql: &str,
        _role: Option<&str>,
    ) -> Result<Vec<Value>, ExecutorError> {
        Ok(vec![])
    }
    async fn execute_text(
        &self,
        _sql: &str,
        _format: &str,
        _role: Option<&str>,
    ) -> Result<String, ExecutorError> {
        Ok(String::new())
    }
}

fn test_state() -> AppState {
    AppState {
        executor: Arc::new(StubExecutor),
        clickhouse_client: None,
        config: ServerConfig::default(),
        query_semaphore: None,
        pool: None,
    }
}

/// Same registration as `sql_generation_handler_comment_tests.rs`: the
/// benchmark schema as "default" in the shared process-global registry.
async fn ensure_default_schema_registered() {
    let _ = GLOBAL_SCHEMAS.set(tokio::sync::RwLock::new(std::collections::HashMap::new()));
    let schema = GraphSchemaConfig::from_yaml_file(
        "benchmarks/social_network/schemas/social_benchmark.yaml",
    )
    .expect("load benchmark schema")
    .to_graph_schema()
    .expect("convert benchmark schema");
    let mut map = GLOBAL_SCHEMAS
        .get()
        .expect("GLOBAL_SCHEMAS set above")
        .write()
        .await;
    map.entry("default".to_string()).or_insert(schema);
}

async fn post(uri: &str, body: Value) -> (StatusCode, Value) {
    let app = build_router(test_state(), &ServerConfig::default());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("read body");
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, body)
}

/// POST `body` to `uri`: the status and the body as text.
async fn post_text(uri: &str, body: Value) -> (StatusCode, String) {
    let app = build_router(test_state(), &ServerConfig::default());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("read body");
    (status, String::from_utf8_lossy(&bytes).into_owned())
}

/// Text only the clause-list parser reads that the bound plan does not
/// translate keeps the syntax error, also over a schema that does not exist
/// (not "Schema not found"; P-4c S10b review).
#[tokio::test]
async fn text_only_syntax_over_an_unknown_schema_is_a_syntax_error() {
    ensure_default_schema_registered().await;
    for body in [
        json!({ "query": "MATCH (a:User) LIMIT 1 SET a.x = 1 RETURN a", "schema_name": "nosuch" }),
        json!({ "query": "USE nosuch MATCH (a:User) LIMIT 1 SET a.x = 1 RETURN a" }),
        json!({ "query": "MATCH (a:User) LIMIT 1 SET a.x = 1 RETURN a" }),
    ] {
        let (status, text) = post_text("/query", body.clone()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}: {text}");
        assert!(text.starts_with("Query syntax error"), "{body}: {text}");
    }
}

/// `/query/sql` → its SQL statement, or the error body.
async fn query_sql_endpoint(query: &str) -> Result<String, Value> {
    let (status, body) = post(
        "/query/sql",
        json!({ "query": query, "target_database": "clickhouse" }),
    )
    .await;
    match body["sql"]
        .as_array()
        .and_then(|a| a.last())
        .and_then(|v| v.as_str())
    {
        Some(sql) if status == StatusCode::OK => Ok(sql.to_string()),
        _ => Err(body),
    }
}

/// `/query` with `sql_only` → its SQL.
async fn query_endpoint_sql_only(query: &str) -> String {
    let (status, body) = post("/query", json!({ "query": query, "sql_only": true })).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body["generated_sql"]
        .as_str()
        .expect("generated_sql")
        .to_string()
}

const QUERIES: &[&str] = &[
    "MATCH (u:User)-[:FOLLOWS]->(f:User) RETURN u.name, count(f) AS n ORDER BY n DESC LIMIT 5",
    "MATCH (a:User)-[:FOLLOWS]->()-[:FOLLOWS]->(c:User) WITH c, count(*) AS k \
     MATCH (c)-[:FOLLOWS]->(d:User) RETURN c.name, d.name, k",
    "MATCH (u:User) OPTIONAL MATCH (u)-[:FOLLOWS]->(f:User) WHERE f.user_id > 3 RETURN u.name, f.name",
    "MATCH (u:User) WHERE id(u) = 1 RETURN u.name",
    // Only the clause-list parser reads these (P-4c S10b).
    "MATCH (u:User) WITH u MATCH (u)-[:FOLLOWS]->(f:User) MATCH (f)-[:FOLLOWS]->(g:User) \
     RETURN count(*) AS n",
    "MATCH (u:User) ORDER BY u.name LIMIT 3 RETURN u.name",
];

#[tokio::test]
async fn query_sql_endpoint_returns_the_sql_query_endpoint_runs() {
    ensure_default_schema_registered().await;
    for q in QUERIES {
        let a = query_sql_endpoint(q)
            .await
            .unwrap_or_else(|e| panic!("{q}: {e}"));
        let b = query_endpoint_sql_only(q).await;
        assert_eq!(a, b, "/query/sql and /query sql_only disagree for {q}");
        assert!(
            a.trim_end().ends_with("SETTINGS join_use_nulls = 1"),
            "#1314: SQL handed out must carry join_use_nulls: {a}"
        );
    }
}

/// A query Neo4j also rejects (ORDER BY a name RETURN did not project) is
/// the client's error on both endpoints (P-4c S10a).
#[tokio::test]
async fn a_query_neo4j_rejects_is_a_client_error_on_both_endpoints() {
    ensure_default_schema_registered().await;
    let q = "MATCH (a:User) RETURN a.name ORDER BY name";
    let (status, body) = post(
        "/query/sql",
        json!({ "query": q, "target_database": "clickhouse" }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error_type"], "QueryError", "{body}");
    let (status, body) = post("/query", json!({ "query": q, "sql_only": true })).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
}

/// Concurrent `/query/sql` requests must each produce exactly the SQL a lone
/// request produces. With process-global counters (the endpoint had no query
/// context), a reset from one request rewound another's mid-translation.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_query_sql_requests_do_not_share_alias_counters() {
    ensure_default_schema_registered().await;
    let base = "MATCH (a:User)-[:FOLLOWS]->()-[:FOLLOWS]->(c:User) WITH c, count(*) AS k \
                MATCH (c)-[:FOLLOWS]->(d:User)-[:FOLLOWS]->(e:User) RETURN c.name, e.name, k LIMIT ";
    // Distinct LIMITs keep every request a cache miss.
    let reference = query_sql_endpoint(&format!("{base}1000"))
        .await
        .expect("reference");
    let mut handles = Vec::new();
    for i in 0..96 {
        let q = format!("{base}{}", 2000 + i);
        handles.push(tokio::spawn(
            async move { (i, query_sql_endpoint(&q).await) },
        ));
    }
    for h in handles {
        let (i, got) = h.await.unwrap();
        let got = got.unwrap_or_else(|e| panic!("request {i} failed: {e}"));
        assert_eq!(
            got.replace(&format!("LIMIT {}", 2000 + i), "LIMIT 1000"),
            reference,
            "request {i} translated differently under concurrency"
        );
    }
}

/// What the bound plan does not lower is an error on ClickHouse (P-4c
/// S11j): on `/query` (400, "Not supported"), and in the library. A
/// dialect that keeps the legacy fallback still translates it there.
#[tokio::test]
async fn unsupported_reads_fail_on_clickhouse_and_fall_back_elsewhere() {
    use clickgraph::server::query_context::{
        set_current_dialect, set_current_schema, with_query_context_sync, QueryContext,
    };
    use clickgraph::sql_generator::SqlDialect;
    use clickgraph::translate::{translate_read, ReadOptions, Route, TranslateError};
    let schema = GraphSchemaConfig::from_yaml_file("schemas/test/social_integration.yaml")
        .unwrap()
        .to_graph_schema()
        .unwrap();
    // Neo4j refuses it ("Type mismatch"); the legacy pipeline answered.
    let q = "MATCH (a:User)-[r:FOLLOWS*1..2]->(b:User) RETURN r.follow_date";
    let translate = |dialect: SqlDialect| {
        with_query_context_sync(QueryContext::new(None), || {
            set_current_schema(Arc::new(schema.clone()));
            set_current_dialect(dialect);
            let (_, statement) = clickgraph::open_cypher_parser::parse_cypher_statement(q).unwrap();
            translate_read(
                statement,
                &schema,
                ReadOptions {
                    max_cte_depth: 100,
                    cypher: Some(q.to_string()),
                    ..Default::default()
                },
            )
        })
    };
    let ch = translate(SqlDialect::ClickHouse);
    assert!(matches!(ch, Err(TranslateError::Unsupported(_))), "{ch:?}");
    let databricks = translate(SqlDialect::Databricks).unwrap();
    assert_eq!(databricks.route, Route::Legacy);
    let lib = clickgraph::clickhouse_query_generator::cypher_to_sql(q, &schema, 100);
    assert!(
        matches!(&lib, Err(e) if e.starts_with("Not supported: ")),
        "{lib:?}"
    );

    ensure_default_schema_registered().await;
    let body = json!({
        "query": "MATCH (a:User)-[r:FOLLOWS*1..2]->(b:User) RETURN r.follow_date",
        "sql_only": true
    });
    let (status, text) = post_text("/query", body.clone()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}: {text}");
    assert!(text.starts_with("Not supported: "), "{body}: {text}");
}

/// A query without RETURN is a syntax error, as in Neo4j (user
/// 2026-10-11; the legacy pipeline returned the matched nodes).
#[tokio::test]
async fn a_query_without_return_is_a_syntax_error() {
    ensure_default_schema_registered().await;
    let body = json!({ "query": "MATCH (u:User) WHERE u.user_id > 1", "sql_only": true });
    let (status, text) = post_text("/query", body.clone()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}: {text}");
    assert!(
        text.contains("Invalid syntax") && text.contains("RETURN"),
        "{body}: {text}"
    );
    let schema = GraphSchemaConfig::from_yaml_file("schemas/test/social_integration.yaml")
        .unwrap()
        .to_graph_schema()
        .unwrap();
    let lib = clickgraph::clickhouse_query_generator::cypher_to_sql("MATCH (u:User)", &schema, 100);
    assert!(
        matches!(&lib, Err(e) if e.contains("Invalid syntax") && e.contains("RETURN")),
        "{lib:?}"
    );
}

/// A statement that is not a read reaching the read path on ClickHouse is
/// not supported, never the legacy pipeline (which dropped a CALL inside a
/// query and answered a library CALL with no rows) nor a syntax error (the
/// clause-list parser does not read `YIELD … WHERE`); the server answers a
/// standalone CALL before it (S11j review).
#[tokio::test]
async fn statements_other_than_reads_are_not_supported_on_clickhouse() {
    ensure_default_schema_registered().await;
    for q in [
        "MATCH (u:User) CALL db.labels() YIELD label RETURN count(*) AS c",
        "MATCH (u:User) RETURN u.name AS n UNION ALL CALL db.labels() YIELD label RETURN label AS n",
        "MATCH (u:User) CREATE (x:User) RETURN u.name",
    ] {
        let body = json!({ "query": q, "sql_only": true });
        let (status, text) = post_text("/query", body.clone()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}: {text}");
        assert!(text.contains("Not supported"), "{body}: {text}");
    }
    let schema = GraphSchemaConfig::from_yaml_file("schemas/test/social_integration.yaml")
        .unwrap()
        .to_graph_schema()
        .unwrap();
    for q in [
        "CALL db.labels() YIELD label WHERE label = 'User' RETURN label",
        "MATCH (u:User) CALL db.labels() YIELD label RETURN count(*) AS c",
        "MATCH (u:User) CREATE (x:User)",
    ] {
        let lib = clickgraph::clickhouse_query_generator::cypher_to_sql(q, &schema, 100);
        assert!(
            matches!(&lib, Err(e) if e.starts_with("Not supported")),
            "{q}: {lib:?}"
        );
    }
}

/// The legacy pipeline knows no union label: with one in the schema, a
/// read the bound plan does not translate is an error on a dialect that
/// keeps the fallback too (it read `m:Message` of a Post as false).
#[test]
fn a_union_label_schema_does_not_fall_back() {
    use clickgraph::server::query_context::{
        set_current_dialect, set_current_schema, with_query_context_sync, QueryContext,
    };
    use clickgraph::translate::{translate_read, ReadOptions, TranslateError};
    let schema = GraphSchemaConfig::from_yaml_str(&format!(
        "{}\n  union_labels:\n    - label: Entity\n      labels: [User, Post]\n",
        include_str!("../../../schemas/test/social_integration.yaml").trim_end()
    ))
    .unwrap()
    .to_graph_schema()
    .unwrap();
    let q = "MATCH (u:User) RETURN elementId(u) AS e, u:Entity AS x";
    let got = with_query_context_sync(QueryContext::new(None), || {
        set_current_schema(Arc::new(schema.clone()));
        set_current_dialect(clickgraph::sql_generator::SqlDialect::Databricks);
        let (_, statement) = clickgraph::open_cypher_parser::parse_cypher_statement(q).unwrap();
        translate_read(
            statement,
            &schema,
            ReadOptions {
                max_cte_depth: 100,
                cypher: Some(q.to_string()),
                ..Default::default()
            },
        )
    });
    assert!(
        matches!(got, Err(TranslateError::Unsupported(_))),
        "{got:?}"
    );
}
