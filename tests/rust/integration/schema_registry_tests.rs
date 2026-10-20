//! Integration tests for the persistent schema registry
//! (`docs/design/SCHEMA_PERSISTENCE.md`).
//!
//! Two tiers:
//! - Router-level tests drive the real HTTP handlers via
//!   `tower::ServiceExt::oneshot` with no ClickHouse — they pin the
//!   opt-in contract: with persistence off (the default) the
//!   `/schemas/load` response stays byte-identical to a server without the
//!   feature, and `DELETE /schemas/{name}` completes the runtime
//!   registration lifecycle.
//! - The store round-trip runs against a live ClickHouse *only* when
//!   `CLICKHOUSE_URL` is set; without it the test skips silently so the
//!   offline suite stays green.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt; // for `oneshot`

use clickgraph::config::ServerConfig;
use clickgraph::executor::{ExecutorError, QueryExecutor};
use clickgraph::server::graph_catalog;
use clickgraph::server::schema_registry::{ClickhouseSchemaStore, SchemaStore};
use clickgraph::server::{build_router, AppState};

// ------------------------------------------------------------- test plumbing

/// Minimal executor — the schema-lifecycle endpoints never invoke it.
struct StubExecutor;

#[async_trait::async_trait]
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

/// A config whose persistence mode is overridden after the fact — the tests
/// construct one per scenario and toggle the flag explicitly.
fn config_with(persistence: clickgraph::config::SchemaPersistenceMode) -> ServerConfig {
    ServerConfig {
        schema_persistence: persistence,
        ..ServerConfig::default()
    }
}

/// Ensure the global registries are initialized the way boot does. Other
/// tests in this binary seed the cells directly (`GLOBAL_SCHEMAS.set(...)`,
/// winning the OnceCell race), which makes `initialize_global_schema`'s own
/// `set` fail and `?`-return BEFORE it reaches `GLOBAL_SCHEMA_CONFIGS` —
/// loads then die with "Global view configs not initialized" for the rest
/// of the process. Pre-seeding both cells with `get_or_init` (idempotent,
/// first-wins) removes that ordering hazard; the boot initializer afterwards
/// best-effort registers "default" and its labels.
async fn ensure_global_schemas() {
    use clickgraph::server::graph_catalog::initialize_global_schema;
    use clickgraph::server::{GLOBAL_SCHEMAS, GLOBAL_SCHEMA_CONFIGS};
    use tokio::sync::RwLock;
    let _ = GLOBAL_SCHEMAS
        .get_or_init(|| async { RwLock::new(std::collections::HashMap::new()) })
        .await;
    let _ = GLOBAL_SCHEMA_CONFIGS
        .get_or_init(|| async { RwLock::new(std::collections::HashMap::new()) })
        .await;
    let _ = initialize_global_schema(None, false).await;
}

/// Server state as a NO-CLIENT (offline) build would carry it.
fn test_state(config: ServerConfig) -> AppState {
    AppState {
        executor: Arc::new(StubExecutor),
        clickhouse_client: None,
        config,
        query_semaphore: None,
        pool: None,
    }
}

async fn body_string(resp: axum::response::Response) -> String {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("read body");
    String::from_utf8(bytes.to_vec()).expect("utf8")
}

/// A tiny valid schema in the list form the config grammar actually accepts
/// (`graph_schema:` → nodes/edges), with its own database name so a live
/// store test never touches real tables.
fn tiny_yaml(name: &str) -> String {
    format!(
        "name: {name}\n\
         graph_schema:\n\
         \x20 nodes:\n\
         \x20   - label: Doc\n\
         \x20     database: it_registry\n\
         \x20     table: it_dummy_table\n\
         \x20     node_id: id\n\
         \x20     property_mappings:\n\
         \x20       id: id\n\
         \x20 edges: []"
    )
}

/// Unique per-call name: process id + a process-monotonic counter, so
/// concurrent tests (and repeated runs) never collide — a simple
/// `nanos % n` does collide when two parallel tests start in the same
/// millisecond bucket, a real failure mode this suite caught (and named).
fn unique_name() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    format!(
        "it_schema_registry_{}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

// ------------------------------------------------------- opt-in / off parity

#[tokio::test]
async fn off_mode_load_response_has_no_persisted_field() {
    ensure_global_schemas().await;
    let name = unique_name();
    let router = build_router(
        test_state(config_with(clickgraph::config::SchemaPersistenceMode::Off)),
        &config_with(clickgraph::config::SchemaPersistenceMode::Off),
    );

    let resp = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/schemas/load")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "schema_name": name,
                        "config_content": tiny_yaml(&name),
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("router answers");

    let status = resp.status();
    let body_raw = body_string(resp).await;
    let body: Value = serde_json::from_str(&body_raw).expect("json body");
    assert_eq!(status, StatusCode::OK, "off-mode load body: {body_raw}");
    let keys: Vec<String> = body
        .as_object()
        .expect("object keys")
        .keys()
        .cloned()
        .collect();
    let mut keys = keys;
    keys.sort();
    assert_eq!(
        keys,
        vec!["message", "schema_name"],
        "off-mode body: {body_raw}"
    );
    assert!(
        body.get("persisted").is_none(),
        "off mode must not add the field: {body}"
    );

    // The registration itself works, exactly as before the feature.
    assert!(
        graph_catalog::get_graph_schema_by_name(&name).await.is_ok(),
        "{name} is queryable"
    );
    graph_catalog::remove_schema(&name).await.expect("cleanup");
}

#[tokio::test]
async fn clickhouse_mode_without_a_client_reports_persisted_false() {
    ensure_global_schemas().await;
    let name = unique_name();
    let config = config_with(clickgraph::config::SchemaPersistenceMode::Clickhouse);
    let router = build_router(test_state(config.clone()), &config);

    let resp = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/schemas/load")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "schema_name": name,
                        "config_content": tiny_yaml(&name),
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .expect("router answers");

    let status = resp.status();
    let body_raw = body_string(resp).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "clickhouse-mode load body: {body_raw}"
    );
    let body: Value = serde_json::from_str(&body_raw).expect("json body");
    // The registration succeeded; the store could not (no client in this
    // mode) and the field says so honestly.
    assert_eq!(body["persisted"], false, "{body}");
    assert_eq!(body["schema_name"], name);

    graph_catalog::remove_schema(&name).await.expect("cleanup");
}

// ------------------------------------------------- DELETE /schemas/{name}

#[tokio::test]
async fn delete_removes_a_registered_schema_and_404s_unknowns() {
    ensure_global_schemas().await;
    let name = unique_name();
    let config = config_with(clickgraph::config::SchemaPersistenceMode::Off);
    let router = build_router(test_state(config), &ServerConfig::default());

    graph_catalog::load_schema_from_content(&name, &tiny_yaml(&name), None, false)
        .await
        .expect("registers without a client");

    // Unknown first: no such schema yet → 404 with the same error shape as
    // GET /schemas/{name}.
    let resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/schemas/it_schema_registry_no_such_schema")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("router answers");
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    let resp = router
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/schemas/{name}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("router answers");
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "DELETE body: {}",
        body_string(resp).await
    );
    let body: Value = serde_json::from_str(&body_string(resp).await).expect("json body");
    assert!(
        body.get("persisted").is_none(),
        "off mode must not add the field: {body}"
    );
    assert!(
        graph_catalog::get_graph_schema_by_name(&name)
            .await
            .is_err(),
        "gone from memory"
    );
}

// ------------------------------------------------------- live ClickHouse tier

fn live_client() -> Option<clickhouse::Client> {
    let url = std::env::var("CLICKHOUSE_URL").ok()?;
    let user = std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "default".into());
    let password = std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_default();
    let database = std::env::var("CLICKHOUSE_DATABASE").unwrap_or_else(|_| "default".into());
    Some(
        clickhouse::Client::default()
            .with_url(url)
            .with_user(user)
            .with_password(password)
            .with_database(database),
    )
}

/// The one-round-trip proof: create table → save → read back → delete →
/// read back empty. This is the store contract every other layer assumes.
#[tokio::test]
async fn store_round_trips_against_live_clickhouse() {
    let Some(client) = live_client() else {
        println!("skipping: CLICKHOUSE_URL not set (offline suite)");
        return;
    };

    let store = SchemaStore::Clickhouse(Box::new(ClickhouseSchemaStore::new(client)));
    let name = unique_name();

    store
        .save_yaml(&name, "name: roundtrip\nnodes: {}")
        .await
        .expect("save");

    let rows = store.load_oldest_first().await.expect("read");
    let mine = rows.iter().find(|r| r.name == name).expect("row present");
    assert_eq!(mine.source, "api-load");
    assert_eq!(mine.format, "yaml");
    assert_eq!(mine.registry_version, 1);
    assert!(mine.payload.contains("roundtrip"));
    assert!(mine.registered_at_ms > 0);

    store.delete(&name).await.expect("delete");
    let rows = store.load_oldest_first().await.expect("read");
    assert!(
        rows.iter().all(|r| r.name != name),
        "deleted row must not read back"
    );
}
