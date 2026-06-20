//! Integration tests for DeltaGraph schema introspection over HTTP.
//!
//! Drives the real router (`POST /schemas/introspect`) via
//! `tower::ServiceExt::oneshot` with an `AppState` whose executor is a real
//! `DatabricksSqlExecutor` pointed at a `wiremock` mock — exercising the full
//! wiring: handler → `as_any` downcast → catalog resolution → `DatabricksProbe`
//! → `SHOW TABLES` / `DESCRIBE TABLE EXTENDED` / `SELECT … LIMIT 3`.
//!
//! Gated on the `databricks` feature (where the executor/probe compile).

#![cfg(feature = "databricks")]

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tower::ServiceExt; // for `oneshot`
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use clickgraph::config::ServerConfig;
use clickgraph::executor::databricks_sql::{DatabricksConfig, DatabricksSqlExecutor};
use clickgraph::executor::QueryExecutor;
use clickgraph::server::{build_router, AppState};

/// Mount the SHOW TABLES / DESCRIBE / SELECT mocks the probe walks through.
async fn mount_catalog_mocks(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("SHOW TABLES IN"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "stmt-show",
            "status": { "state": "SUCCEEDED" },
            "manifest": { "schema": { "columns": [
                { "name": "database" }, { "name": "tableName" }, { "name": "isTemporary" }
            ]}},
            "result": { "data_array": [
                ["graphs", "users", false],
                ["graphs", "follows", false]
            ]}
        })))
        .mount(server)
        .await;

    for table in ["users", "follows"] {
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .and(body_string_contains(format!(
                "DESCRIBE TABLE EXTENDED `main`.`graphs`.`{table}`"
            )))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "statement_id": format!("stmt-desc-{table}"),
                "status": { "state": "SUCCEEDED" },
                "manifest": { "schema": { "columns": [
                    { "name": "col_name" }, { "name": "data_type" }, { "name": "comment" }
                ]}},
                "result": { "data_array": [
                    ["id", "bigint", null],
                    ["name", "string", null]
                ]}
            })))
            .mount(server)
            .await;
    }

    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("LIMIT 3"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "stmt-sample",
            "status": { "state": "SUCCEEDED" },
            "manifest": { "schema": { "columns": [ { "name": "id" }, { "name": "name" } ]}},
            "result": { "data_array": [[1, "alice"]] }
        })))
        .mount(server)
        .await;
}

fn databricks_state(server: &MockServer, catalog: Option<&str>) -> (AppState, ServerConfig) {
    let mut dbc = DatabricksConfig::new("ignored.cloud.databricks.com", "wh-test", "dapi-test");
    dbc.base_url = Some(server.uri());
    dbc.catalog = catalog.map(String::from);
    let executor: Arc<dyn QueryExecutor> =
        Arc::new(DatabricksSqlExecutor::new(dbc).expect("executor builds"));

    let config = ServerConfig {
        databricks: true,
        ..ServerConfig::default()
    };

    let state = AppState {
        executor,
        clickhouse_client: None,
        config: config.clone(),
        query_semaphore: None,
        pool: None,
    };
    (state, config)
}

async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("read body");
    serde_json::from_slice(&bytes).expect("json body")
}

#[tokio::test]
async fn introspect_routes_to_databricks_probe() {
    let server = MockServer::start().await;
    mount_catalog_mocks(&server).await;

    let (state, config) = databricks_state(&server, Some("main"));
    let app = build_router(state, &config);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/schemas/introspect")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "database": "graphs" }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    // database is the catalog.schema namespace; tables come from the probe.
    assert_eq!(body["database"], json!("main.graphs"));
    let names: Vec<&str> = body["tables"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["follows", "users"]); // sorted by the probe
}

#[tokio::test]
async fn introspect_without_catalog_returns_400() {
    let server = MockServer::start().await;
    // No mocks needed — the handler should reject before any HTTP call.

    let (state, config) = databricks_state(&server, None);
    let app = build_router(state, &config);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/schemas/introspect")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "database": "graphs" }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert!(
        body["error"].as_str().unwrap_or("").contains("catalog"),
        "expected a catalog-required error, got {body}"
    );
}
