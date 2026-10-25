//! ELEMENT_IDENTITY.md I5b: `/query` returns the number of an element id
//! for an `id()` RETURN item whose SQL holds the element id's text (`id(n)`
//! of a node of several labels, `id(r)`), and for a query served from the
//! cache as for one that was not. Drives the real router with a stub
//! executor that answers the element id, as `translate_seam_endpoint_tests.rs`
//! does.

use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tower::ServiceExt; // for `oneshot`

use clickgraph::config::ServerConfig;
use clickgraph::executor::{ExecutorError, QueryExecutor};
use clickgraph::graph_catalog::config::GraphSchemaConfig;
use clickgraph::server::bolt_protocol::id_mapper::IdMapper;
use clickgraph::server::{
    build_router, AppState, GLOBAL_SCHEMAS, GLOBAL_SCHEMA_CONFIGS, GLOBAL_SCHEMA_CONTENT_HASHES,
};
use clickgraph::utils::id_encoding::IdEncoding;

/// Answers one row whose column `i` holds `User:7-`, whatever the SQL.
struct ElementIdExecutor;

#[async_trait]
impl QueryExecutor for ElementIdExecutor {
    async fn execute_json(
        &self,
        _sql: &str,
        _role: Option<&str>,
    ) -> Result<Vec<Value>, ExecutorError> {
        Ok(vec![json!({ "i": "User:7-", "k": "User:7-" })])
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
        executor: Arc::new(ElementIdExecutor),
        clickhouse_client: None,
        config: ServerConfig::default(),
        query_semaphore: None,
        pool: None,
    }
}

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

async fn query(cypher: &str) -> (StatusCode, Value) {
    let app = build_router(test_state(), &ServerConfig::default());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/query")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "query": cypher }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("read body");
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn an_id_item_holding_an_element_id_is_returned_as_its_number() {
    ensure_default_schema_registered().await;
    let number = IdMapper::compute_deterministic_id("User:7-");
    // A node of several labels, and a relationship, each twice: the second
    // answer is the same as the first, whether or not the query was cached.
    for cypher in [
        "MATCH (n) RETURN id(n) AS i",
        "MATCH ()-[r:FOLLOWS]->() RETURN id(r) AS i",
    ] {
        for round in 0..2 {
            let (status, body) = query(cypher).await;
            assert_eq!(status, StatusCode::OK, "{cypher}: {body}");
            assert_eq!(
                body["results"][0]["i"],
                json!(number),
                "{cypher}, round {round}: {body}"
            );
        }
    }
    // The key of a node of one label is still its key (the stub's text).
    let (status, body) = query("MATCH (n:User) RETURN id(n) AS k").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["results"][0]["k"], json!("User:7-"), "{body}");
}

/// A schema registered at runtime gets its labels' id codes then, in the
/// order of their names (the schemas loaded at startup do), not as queries
/// first return them.
#[tokio::test]
async fn a_schema_loaded_at_runtime_registers_its_labels_in_order() {
    ensure_default_schema_registered().await;
    GLOBAL_SCHEMA_CONFIGS
        .get_or_init(|| async { tokio::sync::RwLock::new(std::collections::HashMap::new()) })
        .await;
    GLOBAL_SCHEMA_CONTENT_HASHES
        .get_or_init(|| async { tokio::sync::RwLock::new(std::collections::HashMap::new()) })
        .await;
    let yaml = "name: runtime_codes\ngraph_schema:\n  nodes:\n    \
        - {label: RtZeta, database: d, table: z, node_id: id, property_mappings: {id: id}}\n    \
        - {label: RtAlpha, database: d, table: a, node_id: id, property_mappings: {id: id}}\n    \
        - {label: RtMid, database: d, table: m, node_id: id, property_mappings: {id: id}}\n  edges: []\n";
    let app = build_router(test_state(), &ServerConfig::default());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/schemas/load")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({ "schema_name": "runtime_codes", "config_content": yaml }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let code = |l: &str| IdEncoding::get_label_code(l);
    let (alpha, mid, zeta) = (code("RtAlpha"), code("RtMid"), code("RtZeta"));
    assert!(
        alpha.is_some() && alpha < mid && mid < zeta,
        "{alpha:?} {mid:?} {zeta:?}"
    );
}
