//! Every request is recorded exactly once, on HTTP and on Bolt (#1500), with
//! its outcome classified (#1399). A test binary of its own: it owns the
//! process-wide metrics registry (with query previews on), and no other test
//! records into it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use tower::ServiceExt; // for `oneshot`

use clickgraph::config::ServerConfig;
use clickgraph::executor::{ExecutorError, QueryExecutor};
use clickgraph::graph_catalog::config::GraphSchemaConfig;
use clickgraph::server::bolt_protocol::handler::BoltHandler;
use clickgraph::server::bolt_protocol::messages::{signatures, BoltMessage, BoltValue};
use clickgraph::server::bolt_protocol::{BoltConfig, BoltContext, BOLT_VERSION_5_1};
use clickgraph::server::metrics::{MetricsConfig, ServerMetrics};
use clickgraph::server::{build_router, AppState, GLOBAL_SCHEMAS, GLOBAL_SERVER_METRICS};

/// Fails SQL that reads `fail_table`, as a ClickHouse error would.
struct Executor;

#[async_trait]
impl QueryExecutor for Executor {
    async fn execute_json(
        &self,
        sql: &str,
        _role: Option<&str>,
    ) -> Result<Vec<Value>, ExecutorError> {
        if sql.contains("fail_table") {
            return Err(ExecutorError::QueryFailed(
                "Code: 60. UNKNOWN_TABLE".to_string(),
            ));
        }
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

fn state() -> AppState {
    AppState {
        executor: Arc::new(Executor),
        clickhouse_client: None,
        config: ServerConfig::default(),
        query_semaphore: None,
        pool: None,
        // No client here, so the registry store is `Off` (as run_server
        // would build it); this suite never touches the endpoints that
        // reach into it.
        schema_store: std::sync::Arc::new(clickgraph::server::schema_registry::SchemaStore::Off),
    }
}

fn user_schema(table: &str) -> clickgraph::graph_catalog::graph_schema::GraphSchema {
    let yaml = format!(
        "graph_schema:\n  nodes:\n    - label: User\n      database: rr\n      table: {table}\n      \
         node_id: user_id\n      property_mappings:\n        user_id: user_id\n        name: \
         full_name\n  edges: []\n"
    );
    GraphSchemaConfig::from_yaml_str(&yaml)
        .unwrap()
        .to_graph_schema()
        .unwrap()
}

/// The ring's records whose preview is `text`: (type, outcome).
fn records_of(reg: &ServerMetrics, text: &str) -> Vec<(String, &'static str)> {
    reg.recent_queries(10_000)
        .into_iter()
        .filter(|r| r.query_preview.as_deref() == Some(text))
        .map(|r| (r.query_type, r.outcome))
        .collect()
}

async fn http(text: &str, sql_only: bool) -> StatusCode {
    build_router(state(), &ServerConfig::default())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/query")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({ "query": text, "sql_only": sql_only }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap()
        .status()
}

async fn bolt_handler(db: Option<&str>) -> BoltHandler {
    let context = Arc::new(Mutex::new(BoltContext::new()));
    context.lock().unwrap().set_version(BOLT_VERSION_5_1);
    let mut handler =
        BoltHandler::new(context, Arc::new(BoltConfig::default()), Arc::new(Executor));
    let mut extra = json!({ "user_agent": "test/1" });
    if let Some(db) = db {
        extra["db"] = json!(db);
    }
    let hello = BoltMessage::new(signatures::HELLO, vec![BoltValue::Json(extra)]);
    handler.handle_message(hello).await.unwrap();
    handler
        .handle_message(BoltMessage::new(signatures::LOGON, vec![]))
        .await
        .unwrap();
    handler
}

/// RUN `text`: the FAILURE's code, or `None` on SUCCESS (then PULLed).
async fn bolt(handler: &mut BoltHandler, text: &str) -> Option<String> {
    let r = handler
        .handle_message(BoltMessage::run(text.to_string(), HashMap::new(), None))
        .await
        .unwrap();
    if r[0].signature == signatures::SUCCESS {
        let _ = handler.handle_message(BoltMessage::pull(-1, None)).await;
        return None;
    }
    let _ = handler.handle_message(BoltMessage::reset()).await;
    match &r[0].fields[0] {
        BoltValue::Json(Value::Object(m)) => {
            m.get("code").and_then(|c| c.as_str()).map(String::from)
        }
        _ => Some(String::new()),
    }
}

#[tokio::test]
async fn every_request_is_recorded_once() {
    let _ = GLOBAL_SERVER_METRICS.set(Arc::new(ServerMetrics::new(MetricsConfig {
        query_preview: true,
        slow_query_capacity: 10_000,
        ..MetricsConfig::default()
    })));
    let reg = GLOBAL_SERVER_METRICS.get().unwrap();
    let _ = GLOBAL_SCHEMAS.set(tokio::sync::RwLock::new(HashMap::from([
        ("default".to_string(), user_schema("users")),
        ("rr_fail".to_string(), user_schema("fail_table")),
    ])));

    // HTTP: (text, sql_only, status, type, outcome). Before #1500 the
    // successful procedure, SHOW DATABASES and COPY TO were not recorded.
    let cases = [
        ("CALL db.labels()", false, StatusCode::OK, "procedure", "ok"),
        ("SHOW DATABASES", false, StatusCode::OK, "procedure", "ok"),
        (
            "COPY (MATCH (u:User) RETURN u.name) TO '/tmp/rr.csv'",
            true,
            StatusCode::OK,
            "read",
            "ok",
        ),
        (
            "MATCH (u:User) RETURN u.name",
            true,
            StatusCode::OK,
            "read",
            "ok",
        ),
        (
            "MATCH (u:User) RETURN u.name AS n",
            false,
            StatusCode::OK,
            "read",
            "ok",
        ),
        (
            "CALL db.nosuch()",
            false,
            StatusCode::NOT_FOUND,
            "procedure",
            "not_found",
        ),
        (
            "MATCH (u:User RETURN u",
            false,
            StatusCode::BAD_REQUEST,
            "other",
            "bad_request",
        ),
    ];
    for (text, sql_only, status, query_type, outcome) in cases {
        assert_eq!(http(text, sql_only).await, status, "{text}");
        assert_eq!(
            records_of(reg, text),
            vec![(query_type.to_string(), outcome)],
            "HTTP {text}"
        );
    }

    // Bolt: (text, FAILURE code, outcome). Before, the intercepts (SHOW
    // DATABASES, EXPLAIN, a refused prelude) were not recorded, and every
    // failure was `exec`; a ClickHouse failure was a SyntaxError (#1399).
    let mut handler = bolt_handler(None).await;
    let cases: [(&str, Option<&str>, &str); 7] = [
        ("SHOW DATABASES ", None, "ok"),
        ("EXPLAIN MATCH (u:User) RETURN u", None, "ok"),
        ("CALL db.labels() ", None, "ok"),
        (
            "CALL db.nosuch() ",
            Some("Neo.ClientError.Procedure.ProcedureNotFound"),
            "not_found",
        ),
        (
            "USE rr_nosuch MATCH (u:User) RETURN u.name AS n",
            Some("Neo.ClientError.Database.DatabaseNotFound"),
            "not_found",
        ),
        (
            "USE rr_fail MATCH (u:User) RETURN u.name AS n",
            Some("Neo.DatabaseError.Statement.ExecutionFailed"),
            "exec",
        ),
        (
            "MATCH (u:User RETURN u ",
            Some("Neo.ClientError.Statement.SyntaxError"),
            "bad_request",
        ),
    ];
    for (text, code, outcome) in cases {
        assert_eq!(bolt(&mut handler, text).await.as_deref(), code, "{text}");
        assert_eq!(
            records_of(reg, text),
            vec![("bolt".to_string(), outcome)],
            "Bolt {text}"
        );
    }

    // A Browser intercept on a session database that is not loaded answers
    // DatabaseNotFound, and the connection stays usable (it was reset).
    let mut handler = bolt_handler(Some("rr_nosuch_session")).await;
    let text = "CALL db.schema.visualization()";
    assert_eq!(
        bolt(&mut handler, text).await.as_deref(),
        Some("Neo.ClientError.Database.DatabaseNotFound")
    );
    assert_eq!(
        records_of(reg, text),
        vec![("bolt".to_string(), "not_found")]
    );
    assert_eq!(bolt(&mut handler, "SHOW DATABASES  ").await, None);
}
