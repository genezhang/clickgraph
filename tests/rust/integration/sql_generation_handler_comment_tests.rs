//! Regression tests for the #516 adversarial-review finding: `#516` made
//! `parse_cypher_statement` all-consuming, but the production `/query/sql`
//! HTTP handler (`sql_generation_handler.rs`) parsed its `clean_query`
//! WITHOUT stripping comments first (unlike `handlers.rs`,
//! `sql_generator/emitters/clickhouse/mod.rs`, and
//! `clickgraph-embedded/connection.rs::query_async`, which all call
//! `strip_comments()` before parsing). A perfectly standard, spec-legal
//! trailing `//` or `/* */` Cypher comment therefore regressed from 200 OK
//! to a 400 "Unexpected tokens after query" error.
//!
//! Drives the REAL router (`POST /query/sql`) via `tower::ServiceExt::oneshot`
//! with a stub executor (this endpoint only translates Cypher -> SQL, it
//! never executes) — no ClickHouse or live listener required. Mirrors the
//! `build_router` + `AppState` pattern in `metrics_endpoint_tests.rs`.

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

/// The `/query/sql` endpoint only translates Cypher -> SQL; it never calls
/// the executor, so this stub is never actually invoked.
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

/// Register the benchmark schema as "default" in the process-global schema
/// registry the handler resolves `schema_name` against. `GLOBAL_SCHEMAS` is
/// a `OnceCell` shared by the whole `integration` test binary, so this is
/// idempotent: only the first caller across all test files actually sets
/// it, everyone else just inserts/overwrites the "default" entry.
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

async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("read body");
    serde_json::from_slice(&bytes).expect("valid JSON body")
}

async fn post_query_sql(query: &str) -> (StatusCode, Value) {
    ensure_default_schema_registered().await;
    let app = build_router(test_state(), &ServerConfig::default());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/query/sql")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({ "query": query, "target_database": "clickhouse" }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let body = body_json(resp).await;
    (status, body)
}

#[tokio::test]
async fn query_sql_handler_accepts_trailing_line_comment() {
    let (status, body) =
        post_query_sql("MATCH (n:User) RETURN n.user_id AS id // just a trailing note").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a trailing `//` line comment is standard, spec-legal Cypher and must \
         not be rejected as trailing garbage; body: {body}"
    );
    let sql = body["sql"]
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert!(
        sql.contains("user_id"),
        "expected generated SQL to reference user_id; got: {body}"
    );
}

#[tokio::test]
async fn query_sql_handler_accepts_trailing_block_comment() {
    let (status, body) =
        post_query_sql("MATCH (n:User) RETURN n.user_id AS id /* trailing block comment */").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a trailing `/* */` block comment is standard, spec-legal Cypher and \
         must not be rejected as trailing garbage; body: {body}"
    );
}

#[tokio::test]
async fn query_sql_handler_accepts_leading_line_comment_with_use_clause() {
    // The USE-clause quick-extraction parse (a SEPARATE clean_query
    // computation earlier in the handler) must also tolerate comments.
    let (status, body) =
        post_query_sql("// leading note\nUSE default\nMATCH (n:User) RETURN n.user_id AS id").await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a leading `//` comment before a USE clause must not be rejected; body: {body}"
    );
}

#[tokio::test]
async fn query_sql_handler_still_rejects_genuine_trailing_garbage() {
    // #516's actual fix must still hold: this is NOT a comment, it's a typo'd
    // keyword, and must still be a hard parse error.
    let (status, body) = post_query_sql("MATCH (n:User) RETURN n.user_id AS id GARBAGE").await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "genuine trailing garbage (not a comment) must still be rejected; body: {body}"
    );
    assert_eq!(body["error_type"], "ParseError");
}
