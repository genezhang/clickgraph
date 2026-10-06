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
