//! Integration tests for the observability endpoints (`/metrics`, `/stats`,
//! `/stats/queries`). Drives the real router via `tower::ServiceExt::oneshot`
//! with a stub executor — no ClickHouse or live listener required.

use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt; // for `oneshot`

use clickgraph::config::ServerConfig;
use clickgraph::executor::{ExecutorError, QueryExecutor};
use clickgraph::server::metrics::{ErrorClass, MetricsConfig, Outcome, QuerySample, ServerMetrics};
use clickgraph::server::{build_router, AppState, GLOBAL_SERVER_METRICS};

/// Minimal executor — the observability endpoints never invoke it.
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

/// Ensure the global registry is initialized (idempotent across the shared test
/// binary). Returns a handle for direct recording.
fn ensure_metrics() -> &'static Arc<ServerMetrics> {
    let _ = GLOBAL_SERVER_METRICS.set(Arc::new(ServerMetrics::new(MetricsConfig::default())));
    GLOBAL_SERVER_METRICS.get().expect("metrics initialized")
}

async fn body_string(resp: axum::response::Response) -> String {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("read body");
    String::from_utf8(bytes.to_vec()).expect("utf8")
}

#[tokio::test]
async fn metrics_endpoint_serves_prometheus() {
    let reg = ensure_metrics();
    // Record a query so the counters are non-trivial.
    let m = clickgraph::server::handlers::QueryPerformanceMetrics {
        total_time: 0.012,
        execution_time: 0.008,
        query_type: "read".to_string(),
        result_rows: Some(3),
        ..clickgraph::server::handlers::QueryPerformanceMetrics::default()
    };
    reg.record_query(&QuerySample {
        metrics: &m,
        outcome: Outcome::Ok,
        has_phase_breakdown: true,
        query_text: Some("MATCH (n) RETURN n"),
        ch: None,
    });

    let app = build_router(test_state(), &ServerConfig::default());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(ct.starts_with("text/plain"), "content-type was {ct}");

    let body = body_string(resp).await;
    assert!(body.contains("clickgraph_queries_total"));
    assert!(body.contains("clickgraph_query_duration_seconds_bucket{phase=\"total\""));
    assert!(body.contains("clickgraph_queries_by_type_total{type=\"read\"}"));

    // Every non-comment, non-blank line is a valid `name <value>` exposition.
    for line in body.lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let value = line.rsplit(' ').next().unwrap();
        assert!(value.parse::<f64>().is_ok(), "bad metric line: {line}");
    }
}

#[tokio::test]
async fn stats_endpoint_serves_json() {
    ensure_metrics();
    let app = build_router(test_state(), &ServerConfig::default());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/stats")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp).await;
    let json: Value = serde_json::from_str(&body).expect("valid JSON");
    assert_eq!(json["service"], "clickgraph");
    assert!(json["version"].is_string());
    assert!(json["metrics"]["uptime_secs"].is_u64());
    assert!(json["metrics"]["queries_total"].is_u64());
    assert!(json["metrics"]["latency"].is_array());
}

#[tokio::test]
async fn stats_queries_endpoint_serves_ring() {
    let reg = ensure_metrics();
    let m = clickgraph::server::handlers::QueryPerformanceMetrics {
        total_time: 0.5,
        execution_time: 0.4,
        query_type: "read".to_string(),
        result_rows: Some(1),
        ..clickgraph::server::handlers::QueryPerformanceMetrics::default()
    };
    reg.record_query(&QuerySample {
        metrics: &m,
        outcome: Outcome::Err(ErrorClass::Exec),
        has_phase_breakdown: true,
        query_text: None,
        ch: None,
    });

    let app = build_router(test_state(), &ServerConfig::default());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/stats/queries?recent=5&slowest=5")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let json: Value = serde_json::from_str(&body_string(resp).await).expect("valid JSON");
    assert!(json["recent"].is_array());
    assert!(json["slowest"].is_array());
}
