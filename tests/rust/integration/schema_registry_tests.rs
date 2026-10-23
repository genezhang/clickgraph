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
        // No client in this mode, so the shared store is `Off` however the
        // config flag is set (same rule `run_server` applies).
        schema_store: Arc::new(SchemaStore::Off),
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
    // Body shape parity with GET /schemas/{name} (spec criterion 5): one
    // "error" string field, nothing else.
    let body: Value = serde_json::from_str(&body_string(resp).await).expect("json body");
    let keys: Vec<&String> = body.as_object().expect("object").keys().collect();
    assert_eq!(keys, vec!["error"], "404 body: {body}");
    assert!(
        body["error"]
            .as_str()
            .expect("error is a string")
            .contains("no_such_schema"),
        "{body}"
    );

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

/// With `clickhouse` persistence configured but no client (the offline
/// route CI exercises through), an unknown-name DELETE stays the
/// pre-feature 404: the shared store is `Off` and answers the store-only
/// probe `false`, so no DELETE that would create the registry table is
/// ever issued — the store-only path has no write side effect on misses.
#[tokio::test]
async fn delete_unknown_name_stays_404_in_clickhouse_mode_without_client() {
    ensure_global_schemas().await;
    let config = config_with(clickgraph::config::SchemaPersistenceMode::Clickhouse);
    let router = build_router(test_state(config.clone()), &config);

    let resp = router
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
    let body: Value = serde_json::from_str(&body_string(resp).await).expect("json body");
    let keys: Vec<&String> = body.as_object().expect("object").keys().collect();
    assert_eq!(keys, vec!["error"], "404 body: {body}");
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

    let fetch = store
        .load_replay_rows(usize::MAX, u64::MAX)
        .await
        .expect("read");
    let mine = fetch
        .restorable
        .iter()
        .find(|r| r.name == name)
        .expect("row present");
    assert_eq!(mine.source, "api-load");
    assert_eq!(mine.format, "yaml");
    assert_eq!(mine.registry_version, 1);
    assert!(mine.payload.contains("roundtrip"));
    assert!(mine.registered_at_ms > 0);
    assert!(fetch.total_rows >= 1, "count must cover the saved row");

    store.delete(&name).await.expect("delete");
    let fetch = store
        .load_replay_rows(usize::MAX, u64::MAX)
        .await
        .expect("read");
    assert!(
        fetch.restorable.iter().all(|r| r.name != name),
        "deleted row must not read back"
    );
}

/// The row-budget contract replay assumes: a fetch capped below the table's
/// row count returns the cap and states the overage exactly.
#[tokio::test]
async fn replay_fetch_is_bounded_by_the_row_budget() {
    let Some(client) = live_client() else {
        println!("skipping: CLICKHOUSE_URL not set (offline suite)");
        return;
    };

    let store = SchemaStore::Clickhouse(Box::new(ClickhouseSchemaStore::new(client)));
    let name_a = unique_name();
    let name_b = unique_name();

    store
        .save_yaml(&name_a, "name: bounds-a\nnodes: {}")
        .await
        .expect("seed a");
    store
        .save_yaml(&name_b, "name: bounds-b\nnodes: {}")
        .await
        .expect("seed b");

    // Cap at one: exactly one row comes back, and the count states that
    // more exist in the table.
    let fetch = store.load_replay_rows(1, u64::MAX).await.expect("read");
    assert_eq!(fetch.fetched, 1, "the LIMIT is the hard bound");
    assert!(fetch.total_rows > 1, "count sees past the cap");
    let over = fetch.total_rows - fetch.fetched as u64;
    assert!(
        over >= 1,
        "the overage replay's WARN accounts for must be visible: total={} fetched=1",
        fetch.total_rows
    );

    store.delete(&name_a).await.expect("cleanup a");
    store.delete(&name_b).await.expect("cleanup b");
}

/// The byte budget bounds the fetch itself: whatever rows come back, their
/// total payload never exceeds the cap, a zero cap fetches nothing, and a
/// cap that mathematically cannot fit this test's newer row excludes it.
/// The cap-shape invariants are asserted over *all* fetched rows so other
/// tests' rows in the shared database cannot flake them; row identity is
/// only checked where it is order-guaranteed (this test's own rows, seeded
/// last).
#[tokio::test]
async fn replay_fetch_is_bounded_by_the_byte_budget() {
    let Some(client) = live_client() else {
        println!("skipping: CLICKHOUSE_URL not set (offline suite)");
        return;
    };

    let store = SchemaStore::Clickhouse(Box::new(ClickhouseSchemaStore::new(client)));
    let name_a = unique_name();
    let name_b = unique_name();
    let payload_a = "name: bytes-a\nnodes: {}";
    let payload_b = format!("name: bytes-b\nnodes: {{}}\n# {}", "x".repeat(64_000));

    store.save_yaml(&name_a, payload_a).await.expect("seed a");
    store.save_yaml(&name_b, &payload_b).await.expect("seed b");

    let full = store
        .load_replay_rows(usize::MAX, u64::MAX)
        .await
        .expect("read");
    let len_a = full
        .restorable
        .iter()
        .find(|r| r.name == name_a)
        .expect("a seeded and restorable")
        .payload
        .len() as u64;
    let len_b = full
        .restorable
        .iter()
        .find(|r| r.name == name_b)
        .expect("b seeded and restorable")
        .payload
        .len() as u64;
    assert!(len_b > len_a, "the probe fixture changed");

    // 1. A 0-byte budget fetches nothing (but the count still sees the table).
    let zero = store.load_replay_rows(usize::MAX, 0).await.expect("read");
    assert_eq!(zero.fetched, 0, "nothing fits a 0-byte budget");
    assert!(zero.restorable.is_empty());
    assert!(zero.total_rows > 0);

    // 2. Any cap bounds exactly what crosses the wire: the fetched rows'
    //    payloads sum to at most the cap (the fetch is capped server-side;
    //    the invariant holds whatever else shares the table).
    let cap = len_a + len_b - 1;
    let capped = store.load_replay_rows(usize::MAX, cap).await.expect("read");
    let used: u64 = capped
        .restorable
        .iter()
        .map(|r| r.payload.len() as u64)
        .sum();
    assert!(
        used <= cap,
        "fetch would exceed the byte budget: {used} > {cap}"
    );
    // 3. The newer fat row cannot fit this cap in ANY database state: its
    //    contribution to the cumulative window is at least len_a + len_b >
    //    cap, whatever older rows came before it.
    assert!(
        capped.restorable.iter().all(|r| r.name != name_b),
        "the newer fat row must be excluded by the byte cap"
    );

    // 4. With the cap effectively absent, both rows are restorable (the
    //    ordinary replay path reads them).
    assert!(full.restorable.iter().any(|r| r.name == name_b));

    store.delete(&name_a).await.expect("cleanup a");
    store.delete(&name_b).await.expect("cleanup b");
}

/// The advertised remedy for a row that failed boot replay: `DELETE
/// /schemas/{name}` returns 200 and clears the store row even though the
/// name was never loaded into memory (`store_only: true`, additive), and a
/// second DELETE is the pre-feature 404. Driven through the real router —
/// this is the endpoint promise boot replay's WARN makes.
#[tokio::test]
async fn delete_retires_a_store_only_row() {
    let Some(client) = live_client() else {
        println!("skipping: CLICKHOUSE_URL not set (offline suite)");
        return;
    };
    ensure_global_schemas().await;

    let store = SchemaStore::Clickhouse(Box::new(ClickhouseSchemaStore::new(client.clone())));
    let name = unique_name();
    // Seeded directly through the store: exactly the state a row that
    // failed replay leaves behind — in the store, not in memory.
    store
        .save_yaml(&name, "name: store-only\nnodes: {}")
        .await
        .expect("seed");
    assert!(
        graph_catalog::get_graph_schema_by_name(&name)
            .await
            .is_err(),
        "fixture must not be in memory"
    );

    let config = config_with(clickgraph::config::SchemaPersistenceMode::Clickhouse);
    let app_state = AppState {
        executor: Arc::new(StubExecutor),
        clickhouse_client: Some(client.clone()),
        config: config.clone(),
        query_semaphore: None,
        pool: None,
        // Mirrors run_server's hoisting: one store instance inside the
        // state. (The fixture `store` above stays this test's seeding and
        // verification handle; both instances point at the same table.)
        schema_store: Arc::new(SchemaStore::Clickhouse(Box::new(
            ClickhouseSchemaStore::new(client.clone()),
        ))),
    };
    let router = build_router(app_state, &config);

    // First DELETE: 200 with the additive fields, and the store really
    // no longer holds the row.
    let resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/schemas/{name}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("router answers");
    assert_eq!(resp.status(), StatusCode::OK, "store-only DELETE");
    let body: Value = serde_json::from_str(&body_string(resp).await).expect("json body");
    assert_eq!(body["persisted"], true, "{body}");
    assert_eq!(body["store_only"], true, "{body}");
    assert_eq!(body["schema_name"], name);
    assert!(
        !store.exists(&name).await.expect("post-delete probe"),
        "the row must really be gone from the store"
    );

    // Second DELETE: neither memory nor store has anything for the name —
    // the unknown-name 404, unchanged shape.
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
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body: Value = serde_json::from_str(&body_string(resp).await).expect("json body");
    let keys: Vec<&String> = body.as_object().expect("object").keys().collect();
    assert_eq!(keys, vec!["error"], "404 body: {body}");
}

/// Boot replay end-to-end (spec Testing §2): a row seeded as if written by a
/// previous process is restored through the same load path a live
/// registration uses, while a name already registered in memory is shadowed,
/// not overwritten. This is the wiring the Python e2e exercises against the
/// real binary — pinned here so a regression in `replay`'s call site, its
/// `validate_schema=false` restore, or shadowing fails the Rust tier too.
#[tokio::test]
async fn boot_replay_restores_persisted_rows_and_shadows_registered_ones() {
    let Some(client) = live_client() else {
        println!("skipping: CLICKHOUSE_URL not set (offline suite)");
        return;
    };
    ensure_global_schemas().await;

    // The restore path runs the full live load (auto-discovery through the
    // ClickHouse client), so both YAMLs' tables must actually exist: the
    // pre-registration's `it_dummy_table` and the seeded rows' probe table.
    client
        .query("CREATE DATABASE IF NOT EXISTS it_registry")
        .execute()
        .await
        .expect("test database");
    client
        .query("CREATE TABLE IF NOT EXISTS it_registry.it_replay_dummy (id UInt32) ENGINE = Memory")
        .execute()
        .await
        .expect("probe table");
    client
        .query("CREATE TABLE IF NOT EXISTS it_registry.it_dummy_table (id UInt32) ENGINE = Memory")
        .execute()
        .await
        .expect("probe table (tiny_yaml's)");

    // One store, shared between the seeding and the state that boots
    // replay — the same single-instance discipline `run_server` uses.
    let store = Arc::new(SchemaStore::Clickhouse(Box::new(
        ClickhouseSchemaStore::new(client.clone()),
    )));

    // Row A: registered in memory FIRST with one table — the seeded store row
    // points at a DIFFERENT table, so if replay wrongly overwrote the
    // registration, the in-memory node table would flip. Shadowing keeps the
    // original.
    let shadowed_name = unique_name();
    let shadowed_yaml = tiny_yaml(&shadowed_name);
    graph_catalog::load_schema_from_content(
        &shadowed_name,
        &shadowed_yaml,
        Some(client.clone()),
        false,
    )
    .await
    .expect("pre-register shadow victim");

    // Both rows seeded as a previous process would have left them.
    store
        .save_yaml(&shadowed_name, &replay_probe_yaml(shadowed_name.as_str()))
        .await
        .expect("seed shadowed row");
    let restored_name = unique_name();
    store
        .save_yaml(&restored_name, &replay_probe_yaml(&restored_name))
        .await
        .expect("seed restore row");

    let config = config_with(clickgraph::config::SchemaPersistenceMode::Clickhouse);
    let app_state = AppState {
        executor: Arc::new(StubExecutor),
        clickhouse_client: Some(client.clone()),
        config: config.clone(),
        query_semaphore: None,
        pool: None,
        schema_store: store.clone(),
    };
    clickgraph::server::schema_registry::replay(&app_state, &config).await;

    // Restored: gone through the live load path, so a queryable schema
    // exists afterwards with the YAML's own node/relationship tables.
    let restored = graph_catalog::get_graph_schema_by_name(&restored_name)
        .await
        .expect("restored schema is registered");
    assert!(
        restored.all_node_schemas().contains_key("Doc"),
        "restored schema carries the YAML's Doc node, got: {:?}",
        restored.all_node_schemas().keys().collect::<Vec<_>>()
    );

    // Shadowed: still the FIRST registration — its node table is the one the
    // pre-register put there, not the seeded row's table.
    let kept = graph_catalog::get_graph_schema_by_name(&shadowed_name)
        .await
        .expect("shadowed schema stays registered");
    let kept_nodes = kept
        .all_node_schemas()
        .values()
        .map(|n| format!("{:?}", n))
        .collect::<Vec<_>>();
    assert!(
        kept_nodes
            .iter()
            .all(|body| !body.contains("it_replay_dummy")),
        "boot replay must not overwrite a registered schema; got {kept_nodes:?}"
    );

    // Cleanup: store rows must not leak into later live runs' replay, and
    // memory gets back to roughly what it had.
    store.delete(&shadowed_name).await.expect("delete a");
    store.delete(&restored_name).await.expect("delete b");
    let _ = graph_catalog::remove_schema(&shadowed_name).await;
    let _ = graph_catalog::remove_schema(&restored_name).await;
    // The probe tables themselves: repeated live runs used to accumulate
    // them in the shared test database. Drop after use — the next run's
    // CREATE TABLE IF NOT EXISTS re-creates what it needs.
    let _ = client
        .query("DROP TABLE IF EXISTS it_registry.it_replay_dummy")
        .execute()
        .await;
    let _ = client
        .query("DROP TABLE IF EXISTS it_registry.it_dummy_table")
        .execute()
        .await;
}

/// The seeded-row YAML: same shape as `tiny_yaml`, but pointed at the replay
/// probe table this test creates — table identity is what makes shadowing
/// observable (an overwrite of the common `it_dummy_table` registration
/// flips the node to `it_replay_dummy`).
fn replay_probe_yaml(name: &str) -> String {
    format!(
        "name: {name}\n\
         graph_schema:\n\
         \x20 nodes:\n\
         \x20   - label: Doc\n\
         \x20     database: it_registry\n\
         \x20     table: it_replay_dummy\n\
         \x20     node_id: id\n\
         \x20     property_mappings:\n\
         \x20       id: id\n\
         \x20 edges: []"
    )
}
