//! Databricks SQL Warehouse executor over the Statement Execution API.
//!
//! Phase 2.1 of the DeltaGraph refactor: a minimal implementation of
//! [`QueryExecutor`] that submits a statement to a Databricks SQL
//! Warehouse, polls until it terminates, and parses INLINE results
//! into the same `Vec<serde_json::Value>` shape that the ClickHouse
//! `execute_json` path produces. With this in place, all the
//! query-rendering code that already routes through `current_dialect`
//! can finally hit a real Databricks endpoint.
//!
//! ## Scope of this phase
//!
//! - PAT auth only (Bearer header). OAuth M2M ships in a later phase
//!   behind a separate feature flag.
//! - INLINE disposition only — the executor reads results from the
//!   submit/poll response body. External-link (Arrow/parquet) chunks
//!   are a follow-up; without them this executor is suitable for
//!   small-result queries only.
//! - `execute_text` rejects `Pretty`/`CSV`-style formats. Adding
//!   format conversion is straightforward but isn't needed yet —
//!   the only consumer that calls `execute_text` is the ClickHouse
//!   passthrough endpoint, which a Databricks deployment wouldn't
//!   expose.
//!
//! ## Statement Execution API reference
//!
//! - Submit: `POST /api/2.0/sql/statements`
//! - Poll: `GET /api/2.0/sql/statements/{id}`
//! - Cancel: `POST /api/2.0/sql/statements/{id}/cancel`
//!
//! Docs: <https://docs.databricks.com/api/workspace/statementexecution>

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::time::Duration;

use super::{ExecutorError, QueryExecutor};

/// Configuration for a Databricks SQL Warehouse executor.
///
/// `hostname` is the workspace host (no scheme, no trailing slash) —
/// e.g. `dbc-abc123-def4.cloud.databricks.com`. `warehouse_id` is the
/// SQL Warehouse target; `token` is a personal access token.
#[derive(Debug, Clone)]
pub struct DatabricksConfig {
    pub hostname: String,
    pub warehouse_id: String,
    pub token: String,
    /// How long the submit call waits server-side before returning
    /// `PENDING`. Bounded by Databricks at 50s; we poll past that
    /// using the same status endpoint.
    pub wait_timeout: Duration,
    /// Catalog and schema to set per statement. Optional — when both
    /// are `None` the request omits them and Databricks uses the
    /// warehouse default.
    pub catalog: Option<String>,
    pub schema: Option<String>,
    /// Override the request base URL. When `None`, the executor sends
    /// to `https://{hostname}`; integration tests set this to a
    /// `wiremock::MockServer::uri()` so the same code paths run against
    /// a localhost mock without touching the network. Production code
    /// should leave this unset.
    pub base_url: Option<String>,
}

impl DatabricksConfig {
    /// Reasonable defaults: 30s server-side wait, no catalog/schema
    /// override. Callers that need different behavior set fields
    /// directly after construction.
    pub fn new(
        hostname: impl Into<String>,
        warehouse_id: impl Into<String>,
        token: impl Into<String>,
    ) -> Self {
        Self {
            hostname: hostname.into(),
            warehouse_id: warehouse_id.into(),
            token: token.into(),
            wait_timeout: Duration::from_secs(30),
            catalog: None,
            schema: None,
            base_url: None,
        }
    }
}

/// Backend executor for Databricks SQL Warehouses.
///
/// Wraps a `reqwest::Client` configured with the PAT bearer header
/// and a base URL derived from the config's hostname. The client is
/// cheap to clone (it shares the underlying connection pool), so
/// callers can stash this in an `Arc` like other executors.
pub struct DatabricksSqlExecutor {
    config: DatabricksConfig,
    client: reqwest::Client,
}

impl DatabricksSqlExecutor {
    pub fn new(config: DatabricksConfig) -> Result<Self, ExecutorError> {
        let client = reqwest::Client::builder()
            .build()
            .map_err(|e| ExecutorError::Io(format!("failed to build HTTP client: {e}")))?;
        Ok(Self { config, client })
    }

    fn base_url(&self) -> String {
        self.config
            .base_url
            .clone()
            .unwrap_or_else(|| format!("https://{}", self.config.hostname))
    }

    fn submit_url(&self) -> String {
        format!("{}/api/2.0/sql/statements", self.base_url())
    }

    fn status_url(&self, statement_id: &str) -> String {
        format!(
            "{}/api/2.0/sql/statements/{}",
            self.base_url(),
            statement_id
        )
    }

    fn build_submit_body(&self, sql: &str) -> SubmitRequest {
        SubmitRequest {
            warehouse_id: self.config.warehouse_id.clone(),
            statement: sql.to_string(),
            disposition: "INLINE",
            format: "JSON_ARRAY",
            wait_timeout: format_wait_timeout(self.config.wait_timeout),
            catalog: self.config.catalog.clone(),
            schema: self.config.schema.clone(),
        }
    }

    async fn submit(&self, sql: &str) -> Result<StatementResponse, ExecutorError> {
        let body = self.build_submit_body(sql);
        let resp = self
            .client
            .post(self.submit_url())
            .bearer_auth(&self.config.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| ExecutorError::Io(format!("submit request failed: {e}")))?;
        decode_statement_response(resp).await
    }

    async fn poll(&self, statement_id: &str) -> Result<StatementResponse, ExecutorError> {
        let resp = self
            .client
            .get(self.status_url(statement_id))
            .bearer_auth(&self.config.token)
            .send()
            .await
            .map_err(|e| ExecutorError::Io(format!("poll request failed: {e}")))?;
        decode_statement_response(resp).await
    }
}

#[async_trait]
impl QueryExecutor for DatabricksSqlExecutor {
    async fn execute_json(
        &self,
        sql: &str,
        _role: Option<&str>,
    ) -> Result<Vec<Value>, ExecutorError> {
        let mut response = self.submit(sql).await?;

        // Poll until the statement reaches a terminal state. The
        // initial submit may already return SUCCEEDED if the query
        // finished within `wait_timeout`; otherwise we poll.
        while !response.status.state.is_terminal() {
            tokio::time::sleep(Duration::from_millis(500)).await;
            response = self.poll(&response.statement_id).await?;
        }

        if response.status.state != StatementState::Succeeded {
            let detail = response
                .status
                .error
                .as_ref()
                .map(|e| {
                    format!(
                        "{}: {}",
                        e.error_code.as_deref().unwrap_or("UNKNOWN"),
                        e.message
                    )
                })
                .unwrap_or_else(|| format!("statement {:?}", response.status.state));
            return Err(ExecutorError::QueryFailed(detail));
        }

        rows_from_response(&response)
    }

    async fn execute_text(
        &self,
        _sql: &str,
        format: &str,
        _role: Option<&str>,
    ) -> Result<String, ExecutorError> {
        // No-op for now — the Databricks API doesn't speak ClickHouse
        // output formats. If a future consumer needs CSV/Pretty
        // output, the right shape is to fetch JSON via `execute_json`
        // and post-format here. Returning an explicit error keeps
        // accidental callers from silently emitting bad output.
        Err(ExecutorError::UnsupportedFormat(format.to_string()))
    }
}

// ---------- Statement Execution API request/response shapes ----------

#[derive(Debug, Serialize)]
struct SubmitRequest {
    warehouse_id: String,
    statement: String,
    disposition: &'static str,
    format: &'static str,
    wait_timeout: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    catalog: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StatementResponse {
    statement_id: String,
    status: StatementStatus,
    #[serde(default)]
    manifest: Option<Manifest>,
    #[serde(default)]
    result: Option<ResultData>,
}

#[derive(Debug, Deserialize)]
struct StatementStatus {
    state: StatementState,
    #[serde(default)]
    error: Option<StatementError>,
}

#[derive(Debug, Deserialize, PartialEq, Eq, Clone, Copy)]
enum StatementState {
    #[serde(rename = "PENDING")]
    Pending,
    #[serde(rename = "RUNNING")]
    Running,
    #[serde(rename = "SUCCEEDED")]
    Succeeded,
    #[serde(rename = "FAILED")]
    Failed,
    #[serde(rename = "CANCELED")]
    Canceled,
    #[serde(rename = "CLOSED")]
    Closed,
}

impl StatementState {
    fn is_terminal(&self) -> bool {
        matches!(
            self,
            StatementState::Succeeded
                | StatementState::Failed
                | StatementState::Canceled
                | StatementState::Closed
        )
    }
}

#[derive(Debug, Deserialize)]
struct StatementError {
    #[serde(default)]
    error_code: Option<String>,
    message: String,
}

#[derive(Debug, Deserialize)]
struct Manifest {
    schema: ManifestSchema,
}

#[derive(Debug, Deserialize)]
struct ManifestSchema {
    columns: Vec<ColumnInfo>,
}

#[derive(Debug, Deserialize)]
struct ColumnInfo {
    name: String,
}

#[derive(Debug, Deserialize)]
struct ResultData {
    #[serde(default)]
    data_array: Option<Vec<Vec<Value>>>,
}

// ---------- helpers ----------

fn format_wait_timeout(d: Duration) -> String {
    // Databricks accepts strings like `"50s"`. We clamp to its
    // documented [5s, 50s] band to avoid 400 INVALID_PARAMETER_VALUE.
    let secs = d.as_secs().clamp(5, 50);
    format!("{secs}s")
}

async fn decode_statement_response(
    resp: reqwest::Response,
) -> Result<StatementResponse, ExecutorError> {
    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(ExecutorError::Remote {
            status: status.as_u16(),
            body,
        });
    }
    resp.json::<StatementResponse>()
        .await
        .map_err(|e| ExecutorError::Parse(format!("decode statement response: {e}")))
}

/// Convert a SUCCEEDED `StatementResponse` into JSONEachRow-style
/// objects: one `serde_json::Value::Object` per row, keyed by column
/// name. Pulled out as a free function so unit tests can feed
/// hand-built responses without an HTTP round trip.
fn rows_from_response(resp: &StatementResponse) -> Result<Vec<Value>, ExecutorError> {
    let manifest = resp
        .manifest
        .as_ref()
        .ok_or_else(|| ExecutorError::Parse("manifest missing from SUCCEEDED response".into()))?;
    let columns: Vec<&str> = manifest
        .schema
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .collect();

    let data = match resp.result.as_ref().and_then(|r| r.data_array.as_ref()) {
        Some(rows) => rows,
        // No data_array is valid for empty result sets — e.g. a DDL
        // statement or a SELECT with zero rows. Return an empty Vec.
        None => return Ok(Vec::new()),
    };

    let mut out = Vec::with_capacity(data.len());
    for row in data {
        if row.len() != columns.len() {
            return Err(ExecutorError::Parse(format!(
                "row width {} doesn't match schema columns {}",
                row.len(),
                columns.len()
            )));
        }
        let mut obj = Map::with_capacity(columns.len());
        for (col, val) in columns.iter().zip(row.iter()) {
            obj.insert((*col).to_string(), val.clone());
        }
        out.push(Value::Object(obj));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cfg() -> DatabricksConfig {
        DatabricksConfig::new("example.cloud.databricks.com", "wh-1234", "dapi-test")
    }

    #[test]
    fn submit_url_format() {
        let exec = DatabricksSqlExecutor::new(cfg()).expect("client builds");
        assert_eq!(
            exec.submit_url(),
            "https://example.cloud.databricks.com/api/2.0/sql/statements"
        );
    }

    #[test]
    fn status_url_format() {
        let exec = DatabricksSqlExecutor::new(cfg()).expect("client builds");
        assert_eq!(
            exec.status_url("stmt-abc"),
            "https://example.cloud.databricks.com/api/2.0/sql/statements/stmt-abc"
        );
    }

    #[test]
    fn submit_body_serializes_minimum_fields() {
        let exec = DatabricksSqlExecutor::new(cfg()).expect("client builds");
        let body = exec.build_submit_body("SELECT 1");
        let v = serde_json::to_value(&body).unwrap();
        assert_eq!(v["warehouse_id"], json!("wh-1234"));
        assert_eq!(v["statement"], json!("SELECT 1"));
        assert_eq!(v["disposition"], json!("INLINE"));
        assert_eq!(v["format"], json!("JSON_ARRAY"));
        assert_eq!(v["wait_timeout"], json!("30s"));
        // catalog / schema must NOT appear when unset — they're
        // optional fields and Databricks 400s if you pass empty strings.
        assert!(
            v.get("catalog").is_none(),
            "catalog should be omitted when unset"
        );
        assert!(
            v.get("schema").is_none(),
            "schema should be omitted when unset"
        );
    }

    #[test]
    fn submit_body_includes_catalog_and_schema_when_set() {
        let mut c = cfg();
        c.catalog = Some("main".into());
        c.schema = Some("default".into());
        let exec = DatabricksSqlExecutor::new(c).expect("client builds");
        let body = exec.build_submit_body("SELECT 1");
        let v = serde_json::to_value(&body).unwrap();
        assert_eq!(v["catalog"], json!("main"));
        assert_eq!(v["schema"], json!("default"));
    }

    #[test]
    fn format_wait_timeout_clamps_to_50s() {
        assert_eq!(format_wait_timeout(Duration::from_secs(100)), "50s");
        assert_eq!(format_wait_timeout(Duration::from_secs(30)), "30s");
        // 5s is the documented floor; below that Databricks rejects
        // with INVALID_PARAMETER_VALUE.
        assert_eq!(format_wait_timeout(Duration::from_secs(1)), "5s");
    }

    #[test]
    fn rows_from_response_zips_columns_and_data() {
        let resp: StatementResponse = serde_json::from_value(json!({
            "statement_id": "stmt-1",
            "status": { "state": "SUCCEEDED" },
            "manifest": { "schema": { "columns": [
                { "name": "id" },
                { "name": "name" }
            ]}},
            "result": { "data_array": [
                [1, "alice"],
                [2, "bob"]
            ]}
        }))
        .unwrap();
        let rows = rows_from_response(&resp).expect("parse");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["id"], json!(1));
        assert_eq!(rows[0]["name"], json!("alice"));
        assert_eq!(rows[1]["id"], json!(2));
        assert_eq!(rows[1]["name"], json!("bob"));
    }

    #[test]
    fn rows_from_response_empty_data_array_yields_empty_vec() {
        // SUCCEEDED with no data_array — empty result set, valid.
        let resp: StatementResponse = serde_json::from_value(json!({
            "statement_id": "stmt-2",
            "status": { "state": "SUCCEEDED" },
            "manifest": { "schema": { "columns": [
                { "name": "id" }
            ]}},
            "result": {}
        }))
        .unwrap();
        let rows = rows_from_response(&resp).expect("parse");
        assert!(rows.is_empty());
    }

    #[test]
    fn rows_from_response_rejects_width_mismatch() {
        let resp: StatementResponse = serde_json::from_value(json!({
            "statement_id": "stmt-3",
            "status": { "state": "SUCCEEDED" },
            "manifest": { "schema": { "columns": [
                { "name": "a" },
                { "name": "b" }
            ]}},
            "result": { "data_array": [[1]] }
        }))
        .unwrap();
        let err = rows_from_response(&resp).expect_err("should fail");
        assert!(
            matches!(err, ExecutorError::Parse(_)),
            "expected Parse error, got {err:?}"
        );
    }

    #[test]
    fn statement_state_terminal_detection() {
        assert!(!StatementState::Pending.is_terminal());
        assert!(!StatementState::Running.is_terminal());
        assert!(StatementState::Succeeded.is_terminal());
        assert!(StatementState::Failed.is_terminal());
        assert!(StatementState::Canceled.is_terminal());
        assert!(StatementState::Closed.is_terminal());
    }

    #[test]
    fn base_url_override_replaces_hostname_derivation() {
        // wiremock-based integration tests rely on `base_url` to point
        // at a localhost mock server. If this regression-test fails
        // (e.g. someone reintroduces the hard-coded `https://` prefix),
        // every integration test in this module silently bypasses
        // the mock and hits the real internet. Lock the contract.
        let mut c = cfg();
        c.base_url = Some("http://127.0.0.1:12345".into());
        let exec = DatabricksSqlExecutor::new(c).expect("client builds");
        assert_eq!(
            exec.submit_url(),
            "http://127.0.0.1:12345/api/2.0/sql/statements"
        );
        assert_eq!(
            exec.status_url("stmt-x"),
            "http://127.0.0.1:12345/api/2.0/sql/statements/stmt-x"
        );
    }
}

/// Integration tests against a `wiremock::MockServer`. These exercise
/// the full submit → poll → parse flow without touching the network:
/// the executor's `base_url` is pointed at a localhost mock that
/// serves canned JSON responses. Kept in a separate module so the
/// unit tests above stay synchronous and dependency-free.
#[cfg(test)]
mod wiremock_tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{bearer_token, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn cfg_for(server: &MockServer) -> DatabricksConfig {
        let mut c = DatabricksConfig::new("ignored-host", "wh-1234", "dapi-test");
        c.base_url = Some(server.uri());
        // Sleep between polls would slow tests down — keep at the
        // default; we control how many polls happen via mock fixtures.
        c
    }

    fn manifest_with_columns(cols: &[&str]) -> Value {
        let cols: Vec<Value> = cols.iter().map(|n| json!({ "name": n })).collect();
        json!({ "schema": { "columns": cols } })
    }

    #[tokio::test]
    async fn submit_returns_succeeded_inline_in_one_call() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .and(bearer_token("dapi-test"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "statement_id": "stmt-001",
                "status": { "state": "SUCCEEDED" },
                "manifest": manifest_with_columns(&["id", "name"]),
                "result": { "data_array": [[1, "alice"], [2, "bob"]] }
            })))
            .expect(1)
            .mount(&server)
            .await;

        let exec = DatabricksSqlExecutor::new(cfg_for(&server)).expect("client builds");
        let rows = exec
            .execute_json("SELECT id, name FROM users", None)
            .await
            .expect("execute_json");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["id"], json!(1));
        assert_eq!(rows[1]["name"], json!("bob"));
    }

    #[tokio::test]
    async fn submit_pending_then_poll_succeeded() {
        let server = MockServer::start().await;
        // Submit returns PENDING — no manifest or result yet.
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "statement_id": "stmt-002",
                "status": { "state": "PENDING" }
            })))
            .expect(1)
            .mount(&server)
            .await;
        // Poll returns SUCCEEDED with the actual result.
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-002"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "statement_id": "stmt-002",
                "status": { "state": "SUCCEEDED" },
                "manifest": manifest_with_columns(&["x"]),
                "result": { "data_array": [[42]] }
            })))
            .expect(1)
            .mount(&server)
            .await;

        let exec = DatabricksSqlExecutor::new(cfg_for(&server)).expect("client builds");
        let rows = exec
            .execute_json("SELECT 42", None)
            .await
            .expect("execute_json");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["x"], json!(42));
    }

    #[tokio::test]
    async fn failed_state_surfaces_error_code_and_message() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "statement_id": "stmt-003",
                "status": {
                    "state": "FAILED",
                    "error": {
                        "error_code": "INVALID_SQL_SYNTAX",
                        "message": "unexpected token at line 1"
                    }
                }
            })))
            .mount(&server)
            .await;

        let exec = DatabricksSqlExecutor::new(cfg_for(&server)).expect("client builds");
        let err = exec
            .execute_json("SELEC 1", None)
            .await
            .expect_err("should fail");
        let msg = err.to_string();
        assert!(
            msg.contains("INVALID_SQL_SYNTAX") && msg.contains("unexpected token"),
            "expected error code + message; got {msg}"
        );
    }

    #[tokio::test]
    async fn http_401_becomes_remote_error_with_body() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(
                ResponseTemplate::new(401)
                    .set_body_string("{\"error_code\":\"PERMISSION_DENIED\"}"),
            )
            .mount(&server)
            .await;

        let exec = DatabricksSqlExecutor::new(cfg_for(&server)).expect("client builds");
        let err = exec
            .execute_json("SELECT 1", None)
            .await
            .expect_err("should fail");
        match err {
            ExecutorError::Remote { status, body } => {
                assert_eq!(status, 401);
                assert!(body.contains("PERMISSION_DENIED"), "body: {body}");
            }
            other => panic!("expected Remote error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn execute_text_rejects_format() {
        // Doesn't hit the wire — `execute_text` errors out before any
        // HTTP call. Test it here so the rejection path is exercised
        // in the same module that documents the policy.
        let server = MockServer::start().await;
        let exec = DatabricksSqlExecutor::new(cfg_for(&server)).expect("client builds");
        let err = exec
            .execute_text("SELECT 1", "Pretty", None)
            .await
            .expect_err("should reject");
        assert!(
            matches!(err, ExecutorError::UnsupportedFormat(ref f) if f == "Pretty"),
            "expected UnsupportedFormat(\"Pretty\"), got {err:?}"
        );
    }
}
