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
//! - PAT (Bearer) or OAuth M2M auth — see [`DatabricksConfig::oauth`].
//!   Under OAuth the executor fetches and caches a short-lived token from
//!   the workspace OIDC endpoint and refreshes it before expiry.
//! - Both INLINE and EXTERNAL_LINKS dispositions (see
//!   [`ResultDisposition`], selected via [`DatabricksConfig::disposition`]).
//!   INLINE reads rows from the submit/poll body — capped at 25 MiB by
//!   Databricks. EXTERNAL_LINKS downloads presigned chunk URLs (JSON
//!   arrays-of-arrays) and follows `next_chunk_index`, so it has no size
//!   ceiling; use it for any result that can exceed the inline cap.
//! - `execute_text` renders `Pretty`/`PrettyCompact`/`CSV`/`CSVWithNames`
//!   client-side (the Databricks API has no server-side tabular renderer),
//!   via [`super::text_format`]. These are the formats the HTTP `/query`
//!   endpoint routes through `execute_text` — e.g. what the interactive
//!   `clickgraph-client` REPL requests. Other formats error out up front.
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
///
/// `Debug` is implemented manually to redact the PAT — matching how
/// `RemoteConfig` redacts its password. Logging a `DatabricksConfig`
/// via `{:?}` is safe and will print `********` in place of the token.
#[derive(Clone)]
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
    /// Result disposition requested from the Statement Execution API.
    /// `Inline` (default) reads rows from the submit/poll body — capped
    /// at 25 MiB by Databricks. `ExternalLinks` makes the warehouse stage
    /// results to cloud storage and return presigned URLs the executor
    /// fetches; required for any result that can exceed the inline cap.
    pub disposition: ResultDisposition,
    /// Retry policy for transient failures (HTTP 429 / 503, connect &
    /// timeout errors). A 401 is never retried; non-transient 4xx/5xx
    /// surface immediately. `max_retries` is the number of *extra*
    /// attempts after the first, with exponential backoff from
    /// `retry_base_delay` (a `Retry-After` header overrides it on 429).
    pub max_retries: u32,
    pub retry_base_delay: Duration,
    /// OAuth machine-to-machine (service-principal) auth. When `Some`, the
    /// executor exchanges the client credentials for a short-lived bearer
    /// token at the workspace OIDC endpoint and refreshes it as it nears
    /// expiry, instead of using the static `token` PAT. `None` (default)
    /// keeps PAT auth. Both never apply at once.
    pub oauth: Option<OAuthM2MConfig>,
}

/// OAuth 2.0 client-credentials (M2M) configuration for a Databricks service
/// principal. The token is fetched from `{base}/oidc/v1/token` with HTTP Basic
/// auth (`client_id:client_secret`) and `grant_type=client_credentials`.
#[derive(Clone)]
pub struct OAuthM2MConfig {
    pub client_id: String,
    pub client_secret: String,
    /// Requested scope; Databricks M2M uses `all-apis`.
    pub scope: String,
    /// Override the token endpoint URL. `None` derives it from the executor
    /// base URL as `{base}/oidc/v1/token`; tests point it at a mock.
    pub token_url: Option<String>,
}

impl OAuthM2MConfig {
    pub fn new(client_id: impl Into<String>, client_secret: impl Into<String>) -> Self {
        Self {
            client_id: client_id.into(),
            client_secret: client_secret.into(),
            scope: "all-apis".to_string(),
            token_url: None,
        }
    }
}

impl std::fmt::Debug for OAuthM2MConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Redact the client secret, mirroring the PAT redaction.
        f.debug_struct("OAuthM2MConfig")
            .field("client_id", &self.client_id)
            .field("client_secret", &"********")
            .field("scope", &self.scope)
            .field("token_url", &self.token_url)
            .finish()
    }
}

/// How the Statement Execution API returns result rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ResultDisposition {
    /// Rows inline in the JSON response body. Simple, but Databricks
    /// rejects results over 25 MiB with the inline disposition.
    #[default]
    Inline,
    /// Rows staged to cloud storage; the response carries presigned
    /// `external_links` (one per chunk) that the executor downloads and
    /// concatenates. No size ceiling.
    ExternalLinks,
}

impl ResultDisposition {
    fn as_api_str(self) -> &'static str {
        match self {
            ResultDisposition::Inline => "INLINE",
            ResultDisposition::ExternalLinks => "EXTERNAL_LINKS",
        }
    }
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
            disposition: ResultDisposition::Inline,
            max_retries: 4,
            retry_base_delay: Duration::from_millis(500),
            oauth: None,
        }
    }
}

impl std::fmt::Debug for DatabricksConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Redact the PAT — matches RemoteConfig's password redaction.
        // Anything reachable via `{:?}` (logs, panics, error chains) must
        // not leak the token.
        f.debug_struct("DatabricksConfig")
            .field("hostname", &self.hostname)
            .field("warehouse_id", &self.warehouse_id)
            .field("token", &"********")
            .field("wait_timeout", &self.wait_timeout)
            .field("catalog", &self.catalog)
            .field("schema", &self.schema)
            .field("base_url", &self.base_url)
            .field("disposition", &self.disposition)
            .field("oauth", &self.oauth)
            .finish()
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
    /// Cached OAuth M2M bearer token + its expiry instant. Empty under PAT
    /// auth. Guarded by a `Mutex` so the shared executor can refresh lazily.
    oauth_token: std::sync::Mutex<Option<CachedToken>>,
}

#[derive(Clone)]
struct CachedToken {
    token: String,
    expires_at: std::time::Instant,
}

impl DatabricksSqlExecutor {
    pub fn new(config: DatabricksConfig) -> Result<Self, ExecutorError> {
        let client = reqwest::Client::builder()
            .build()
            .map_err(|e| ExecutorError::Io(format!("failed to build HTTP client: {e}")))?;
        Ok(Self {
            config,
            client,
            oauth_token: std::sync::Mutex::new(None),
        })
    }

    /// The default Unity Catalog this executor targets, if configured
    /// (`DATABRICKS_CATALOG` or the schema YAML `catalog:` field). Used by
    /// schema introspection, which needs a `catalog.schema` namespace.
    pub fn catalog(&self) -> Option<&str> {
        self.config.catalog.as_deref()
    }

    /// Resolve the bearer token for an API call. Under PAT auth this is the
    /// static token. Under OAuth M2M it returns a cached token if still valid,
    /// otherwise fetches a fresh one from the OIDC endpoint and caches it with
    /// a 60s safety margin before its stated expiry.
    async fn resolve_bearer(&self) -> Result<String, ExecutorError> {
        let Some(oauth) = self.config.oauth.as_ref() else {
            return Ok(self.config.token.clone());
        };
        if let Some(cached) = self.oauth_token.lock().unwrap().as_ref() {
            if cached.expires_at > std::time::Instant::now() {
                return Ok(cached.token.clone());
            }
        }
        let (token, ttl) = self.fetch_oauth_token(oauth).await?;
        let expires_at = std::time::Instant::now()
            + ttl
                .saturating_sub(Duration::from_secs(60))
                .max(Duration::from_secs(1));
        *self.oauth_token.lock().unwrap() = Some(CachedToken {
            token: token.clone(),
            expires_at,
        });
        Ok(token)
    }

    /// Exchange client credentials for an access token at the workspace OIDC
    /// token endpoint (`{base}/oidc/v1/token`), `grant_type=client_credentials`
    /// with HTTP Basic auth. Returns `(access_token, expires_in)`.
    async fn fetch_oauth_token(
        &self,
        oauth: &OAuthM2MConfig,
    ) -> Result<(String, Duration), ExecutorError> {
        let url = oauth
            .token_url
            .clone()
            .unwrap_or_else(|| format!("{}/oidc/v1/token", self.base_url()));
        let resp = self
            .send_with_retry(|| {
                self.client
                    .post(&url)
                    .basic_auth(&oauth.client_id, Some(&oauth.client_secret))
                    .form(&[
                        ("grant_type", "client_credentials"),
                        ("scope", oauth.scope.as_str()),
                    ])
            })
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            // A token-endpoint rejection is an auth failure, surfaced as such.
            return Err(ExecutorError::Auth(format!(
                "OAuth token request failed ({}): {body}",
                status.as_u16()
            )));
        }
        let tok = resp
            .json::<OAuthTokenResponse>()
            .await
            .map_err(|e| ExecutorError::Parse(format!("decode OAuth token: {e}")))?;
        Ok((tok.access_token, Duration::from_secs(tok.expires_in)))
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

    /// Send a request with retry-on-transient. `make` rebuilds the request
    /// each attempt (a `RequestBuilder` can't be cloned once its body is
    /// set). Retries HTTP 429/503 and connect/timeout errors with
    /// exponential backoff (capped); a 429 `Retry-After` header overrides
    /// the backoff. A 401 short-circuits to [`ExecutorError::Auth`] without
    /// retrying. Any other response (success or non-retryable status) is
    /// returned for the caller to decode.
    async fn send_with_retry<F>(&self, make: F) -> Result<reqwest::Response, ExecutorError>
    where
        F: Fn() -> reqwest::RequestBuilder,
    {
        let mut attempt: u32 = 0;
        loop {
            match make().send().await {
                Ok(resp) => {
                    let code = resp.status().as_u16();
                    if code == 401 {
                        let body = resp.text().await.unwrap_or_default();
                        return Err(ExecutorError::Auth(body));
                    }
                    if matches!(code, 429 | 503) && attempt < self.config.max_retries {
                        // Honor `Retry-After`, but cap it at the same 30s ceiling
                        // as the backoff so a bogus/hostile header (e.g.
                        // `Retry-After: 999999`) can't stall a query for days.
                        let delay = retry_after(&resp)
                            .map(|d| d.min(MAX_RETRY_DELAY))
                            .unwrap_or_else(|| backoff(attempt, self.config.retry_base_delay));
                        attempt += 1;
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    return Ok(resp);
                }
                Err(e) => {
                    if is_transient(&e) && attempt < self.config.max_retries {
                        let delay = backoff(attempt, self.config.retry_base_delay);
                        attempt += 1;
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    return Err(ExecutorError::Io(format!("request failed: {e}")));
                }
            }
        }
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
            disposition: self.config.disposition.as_api_str(),
            // JSON_ARRAY for both dispositions: with EXTERNAL_LINKS the
            // staged chunk files are themselves JSON arrays-of-arrays, so
            // the same row decoder handles inline and external rows.
            format: "JSON_ARRAY",
            wait_timeout: format_wait_timeout(self.config.wait_timeout),
            catalog: self.config.catalog.clone(),
            schema: self.config.schema.clone(),
        }
    }

    /// Fetch and concatenate every `EXTERNAL_LINKS` chunk into the same
    /// `Vec<Vec<Value>>` shape an INLINE `data_array` would carry.
    ///
    /// Each chunk's `external_link` is a presigned cloud-storage URL that
    /// must be fetched WITHOUT the workspace bearer token (the signature is
    /// in the query string; adding `Authorization` makes some object stores
    /// reject the request). Chunks are followed via `next_chunk_index`: the
    /// metadata for chunk N is retrieved from the workspace result/chunks
    /// endpoint (which DOES need auth), then its `external_link` downloaded.
    async fn fetch_external_links(
        &self,
        statement_id: &str,
        first: &[ExternalLink],
    ) -> Result<Vec<Vec<Value>>, ExecutorError> {
        let mut rows: Vec<Vec<Value>> = Vec::new();
        let mut batch: Vec<ExternalLink> = first.to_vec();
        // Guard against a server that returns a self-referential or cyclic
        // next_chunk_index — chunk indices are monotonic, so only ever follow
        // forward, and bound the number of hops as a backstop.
        let mut hops = 0u32;
        let mut max_seen: i64 = -1;
        loop {
            for link in &batch {
                rows.extend(self.download_chunk(&link.external_link).await?);
            }
            // The continuation index belongs to the batch as a whole; take it
            // from the LAST link only. Reading it from every link would let an
            // intermediate link's back-pointer re-fetch an already-downloaded
            // chunk and silently duplicate rows.
            let Some(n) = batch.last().and_then(|l| l.next_chunk_index) else {
                break;
            };
            if n <= max_seen {
                // Non-monotonic next index → would revisit a chunk; stop.
                break;
            }
            max_seen = n;
            hops += 1;
            if hops > 100_000 {
                return Err(ExecutorError::Parse(
                    "external-link chunk following exceeded hop limit".into(),
                ));
            }
            batch = self.fetch_chunk_metadata(statement_id, n).await?;
            if batch.is_empty() {
                break;
            }
        }
        Ok(rows)
    }

    /// Download one presigned chunk URL and decode its JSON arrays-of-arrays.
    async fn download_chunk(&self, url: &str) -> Result<Vec<Vec<Value>>, ExecutorError> {
        // No bearer auth — presigned URL (see `fetch_external_links`).
        // A transport error's Display includes the request URL; for a presigned
        // chunk link that URL carries a temporary read credential in its query
        // string, so scrub it from the surfaced error to avoid leaking the
        // signature into logs.
        let resp = self
            .send_with_retry(|| self.client.get(url))
            .await
            .map_err(redact_presigned_url)?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(ExecutorError::Remote {
                status: status.as_u16(),
                body,
            });
        }
        resp.json::<Vec<Vec<Value>>>()
            .await
            .map_err(|e| ExecutorError::Parse(format!("decode chunk data: {e}")))
    }

    /// Retrieve chunk N's metadata (its `external_links`) from the workspace
    /// `result/chunks/{n}` endpoint. This call is authenticated.
    async fn fetch_chunk_metadata(
        &self,
        statement_id: &str,
        chunk_index: i64,
    ) -> Result<Vec<ExternalLink>, ExecutorError> {
        let url = format!(
            "{}/api/2.0/sql/statements/{}/result/chunks/{}",
            self.base_url(),
            statement_id,
            chunk_index
        );
        let bearer = self.resolve_bearer().await?;
        let resp = self
            .send_with_retry(|| self.client.get(&url).bearer_auth(&bearer))
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(ExecutorError::Remote {
                status: status.as_u16(),
                body,
            });
        }
        let meta = resp
            .json::<ChunkMetadata>()
            .await
            .map_err(|e| ExecutorError::Parse(format!("decode chunk metadata: {e}")))?;
        Ok(meta.external_links.unwrap_or_default())
    }

    async fn submit(&self, sql: &str) -> Result<StatementResponse, ExecutorError> {
        let body = self.build_submit_body(sql);
        let bearer = self.resolve_bearer().await?;
        let resp = self
            .send_with_retry(|| {
                self.client
                    .post(self.submit_url())
                    .bearer_auth(&bearer)
                    .json(&body)
            })
            .await?;
        decode_statement_response(resp).await
    }

    async fn poll(&self, statement_id: &str) -> Result<StatementResponse, ExecutorError> {
        let bearer = self.resolve_bearer().await?;
        let resp = self
            .send_with_retry(|| {
                self.client
                    .get(self.status_url(statement_id))
                    .bearer_auth(&bearer)
            })
            .await?;
        decode_statement_response(resp).await
    }

    /// Submit a statement, poll to a terminal state, and return the
    /// SUCCEEDED response together with its result rows as positional
    /// arrays-of-values (column names live in the response manifest).
    ///
    /// Shared by `execute_json` (which keys each row by column name) and
    /// `execute_text` (which renders the rows as a text table). Both
    /// INLINE (`data_array`) and EXTERNAL_LINKS (downloaded chunks) results
    /// resolve to the same positional shape here.
    async fn run(&self, sql: &str) -> Result<(StatementResponse, Vec<Vec<Value>>), ExecutorError> {
        // Observability: time the whole statement and count polls so a slow
        // query is traceable. The Databricks `statement_id` is logged so an
        // oncall can pivot from a ClickGraph log line to the warehouse's
        // query history. Logged on the `deltagraph::databricks` target.
        let started = std::time::Instant::now();
        let mut response = self.submit(sql).await?;
        let statement_id = response.statement_id.clone();
        log::debug!(
            target: "deltagraph::databricks",
            "statement_id={statement_id} submitted (disposition={})",
            self.config.disposition.as_api_str(),
        );

        // Poll until the statement reaches a terminal state. The initial
        // submit may already return SUCCEEDED if the query finished within
        // `wait_timeout`; otherwise we poll.
        let mut polls: u32 = 0;
        while !response.status.state.is_terminal() {
            tokio::time::sleep(Duration::from_millis(500)).await;
            response = self.poll(&statement_id).await?;
            polls += 1;
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
            log::warn!(
                target: "deltagraph::databricks",
                "statement_id={statement_id} state={:?} polls={polls} \
                 duration_ms={} error={detail}",
                response.status.state,
                started.elapsed().as_millis(),
            );
            return Err(ExecutorError::QueryFailed(detail));
        }

        // EXTERNAL_LINKS results carry presigned chunk URLs instead of an
        // inline `data_array`; fetch and concatenate them. INLINE results
        // read straight from the body. A missing `data_array` is valid for
        // empty result sets (DDL or a SELECT with zero rows) → empty Vec.
        let data: Vec<Vec<Value>> = if let Some(links) = response
            .result
            .as_ref()
            .and_then(|r| r.external_links.as_ref())
        {
            self.fetch_external_links(&statement_id, links).await?
        } else {
            response
                .result
                .as_ref()
                .and_then(|r| r.data_array.clone())
                .unwrap_or_default()
        };

        log::info!(
            target: "deltagraph::databricks",
            "statement_id={statement_id} state=SUCCEEDED polls={polls} \
             duration_ms={} rows={}",
            started.elapsed().as_millis(),
            data.len(),
        );
        Ok((response, data))
    }
}

#[async_trait]
impl QueryExecutor for DatabricksSqlExecutor {
    async fn execute_json(
        &self,
        sql: &str,
        _role: Option<&str>,
    ) -> Result<Vec<Value>, ExecutorError> {
        let (response, data) = self.run(sql).await?;
        rows_from_data(&response, &data)
    }

    async fn execute_text(
        &self,
        sql: &str,
        format: &str,
        _role: Option<&str>,
    ) -> Result<String, ExecutorError> {
        // The Databricks API has no server-side pretty/tabular renderer
        // (unlike ClickHouse's `Pretty`/`CSV` output formats), so fetch the
        // structured rows and format them client-side. Only the formats the
        // HTTP `/query` endpoint routes here are supported; reject anything
        // else up front so we don't spend a warehouse query we can't emit.
        if !super::text_format::is_supported(format) {
            return Err(ExecutorError::UnsupportedFormat(format.to_string()));
        }
        let (response, data) = self.run(sql).await?;
        let columns = columns_of(&response);
        super::text_format::format_rows(&columns, &data, format)
    }

    fn as_any(&self) -> Option<&(dyn std::any::Any + 'static)> {
        Some(self)
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
    /// Canonical Spark type enum from the manifest (`INT`, `LONG`, `DOUBLE`,
    /// `DECIMAL`, `BOOLEAN`, `STRING`, `ARRAY`, ...). The Statement Execution
    /// API's `JSON_ARRAY` format returns every value as a JSON string
    /// regardless of SQL type, so this is what [`coerce_scalar`] keys on to
    /// restore native JSON numbers/booleans. Absent on some responses (older
    /// DBR, DDL) — `None` then means "leave the value as the API sent it".
    #[serde(default)]
    type_name: Option<String>,
    /// Full SQL type text, e.g. `ARRAY<STRING>`, `DECIMAL(10,2)`. `type_name`
    /// collapses arrays to the bare enum `ARRAY`, so this is the only place the
    /// element type is available for un-stringing complex columns.
    #[serde(default)]
    type_text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ResultData {
    #[serde(default)]
    data_array: Option<Vec<Vec<Value>>>,
    /// Present only under the EXTERNAL_LINKS disposition — the first batch
    /// of presigned chunk descriptors.
    #[serde(default)]
    external_links: Option<Vec<ExternalLink>>,
}

/// One staged result chunk under the EXTERNAL_LINKS disposition.
#[derive(Debug, Deserialize, Clone)]
struct ExternalLink {
    /// Presigned URL to the chunk's JSON arrays-of-arrays payload. Fetched
    /// without auth (the signature is in the URL).
    external_link: String,
    /// Index of the chunk that follows this batch, if any. `None` on the
    /// final chunk.
    #[serde(default)]
    next_chunk_index: Option<i64>,
}

/// Body of the `result/chunks/{n}` endpoint — the metadata for a later chunk.
#[derive(Debug, Deserialize)]
struct ChunkMetadata {
    #[serde(default)]
    external_links: Option<Vec<ExternalLink>>,
}

/// OIDC token endpoint response (the fields we use).
#[derive(Debug, Deserialize)]
struct OAuthTokenResponse {
    access_token: String,
    /// Lifetime in seconds.
    expires_in: u64,
}

// ---------- helpers ----------

fn format_wait_timeout(d: Duration) -> String {
    // Databricks accepts strings like `"50s"`. We clamp to its
    // documented [5s, 50s] band to avoid 400 INVALID_PARAMETER_VALUE.
    let secs = d.as_secs().clamp(5, 50);
    format!("{secs}s")
}

/// Ceiling on any single retry sleep — bounds both exponential backoff and a
/// server-supplied `Retry-After`, so one slow/bogus response can't stall a
/// query for minutes (or, with an absurd `Retry-After`, days).
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

/// Whether a reqwest transport error is worth retrying — connection refused
/// (warehouse still cold), timeouts, and dropped requests. A decode/redirect
/// error is not retried.
fn is_transient(e: &reqwest::Error) -> bool {
    e.is_timeout() || e.is_connect() || e.is_request()
}

/// Rewrite a transport error that may carry a presigned chunk URL (whose query
/// string holds a temporary read credential) into one that names no URL, so the
/// signature never reaches logs. Only used on the `download_chunk` path.
fn redact_presigned_url(e: ExecutorError) -> ExecutorError {
    match e {
        ExecutorError::Io(_) => {
            ExecutorError::Io("chunk download request failed (presigned URL redacted)".into())
        }
        other => other,
    }
}

/// Exponential backoff: `base * 2^attempt`, capped at [`MAX_RETRY_DELAY`] to
/// bound the wait during a long warehouse cold-start.
fn backoff(attempt: u32, base: Duration) -> Duration {
    let factor = 1u64 << attempt.min(16);
    base.saturating_mul(factor as u32).min(MAX_RETRY_DELAY)
}

/// Parse a `Retry-After` header expressed as integer seconds (the form
/// Databricks sends on 429). HTTP-date form is ignored — we fall back to
/// exponential backoff for it.
fn retry_after(resp: &reqwest::Response) -> Option<Duration> {
    resp.headers()
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
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
/// name. Test-only convenience that bundles inline `data_array` extraction
/// with `rows_from_data` — production resolves rows (inline or external)
/// in `run()` and calls `rows_from_data` directly.
#[cfg(test)]
fn rows_from_response(resp: &StatementResponse) -> Result<Vec<Value>, ExecutorError> {
    let data = match resp.result.as_ref().and_then(|r| r.data_array.as_ref()) {
        Some(rows) => rows.as_slice(),
        // No data_array is valid for empty result sets — e.g. a DDL
        // statement or a SELECT with zero rows. Return an empty Vec.
        None => return Ok(Vec::new()),
    };
    rows_from_data(resp, data)
}

/// Manifest column names, in order. Empty when the response carried no
/// manifest (e.g. a DDL statement) — `execute_text` renders that as no table.
fn columns_of(resp: &StatementResponse) -> Vec<String> {
    resp.manifest
        .as_ref()
        .map(|m| m.schema.columns.iter().map(|c| c.name.clone()).collect())
        .unwrap_or_default()
}

/// Key an already-collected set of row arrays by the response's manifest
/// column names. Shared by the INLINE path (`data_array`) and the
/// EXTERNAL_LINKS path (downloaded chunks).
fn rows_from_data(
    resp: &StatementResponse,
    data: &[Vec<Value>],
) -> Result<Vec<Value>, ExecutorError> {
    // Empty result sets (DDL or a zero-row SELECT) need no manifest — and
    // some may not carry one. Short-circuit before requiring it.
    if data.is_empty() {
        return Ok(Vec::new());
    }
    let manifest = resp
        .manifest
        .as_ref()
        .ok_or_else(|| ExecutorError::Parse("manifest missing from SUCCEEDED response".into()))?;
    // (name, type_name) per column. The type drives scalar coercion below so
    // the rows match ClickHouse's native-typed JSONEachRow output instead of
    // the all-strings shape the JSON_ARRAY API delivers.
    let columns: Vec<(&str, Option<&str>, Option<&str>)> = manifest
        .schema
        .columns
        .iter()
        .map(|c| {
            (
                c.name.as_str(),
                c.type_name.as_deref(),
                c.type_text.as_deref(),
            )
        })
        .collect();

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
        for ((col, type_name, type_text), val) in columns.iter().zip(row.iter()) {
            let coerced = match type_name {
                // ARRAY columns arrive as a JSON-encoded string whose inner
                // scalars are themselves strings; un-string them recursively so
                // the HTTP/Bolt result matches ClickHouse's native array shape.
                Some("ARRAY") => coerce_array(val.clone(), *type_text),
                Some(t) => coerce_scalar(val.clone(), t),
                None => val.clone(),
            };
            obj.insert((*col).to_string(), coerced);
        }
        out.push(Value::Object(obj));
    }
    Ok(out)
}

/// Restore a native JSON scalar from the all-strings shape the Statement
/// Execution API's `JSON_ARRAY` format returns.
///
/// Databricks delivers every value as a JSON string — `"7"`, `"3.14"`,
/// `"true"` — regardless of the SQL type, while ClickHouse's JSONEachRow
/// returns native `7` / `3.14` / `true`. This coerces the numeric and boolean
/// scalar types named in the manifest so both backends present an identical
/// JSON shape to callers (the result-type-fidelity gap from the LDBC parity
/// sweep). Coercion is deliberately conservative:
///
/// - Only `Value::String` payloads are touched; SQL `NULL` already arrives as
///   JSON `null`, and any already-native value passes through unchanged
///   (defensive against a future API/format that returns typed scalars).
/// - `STRING`/`CHAR`/`BINARY`/`DATE`/`TIMESTAMP`/`INTERVAL` stay strings — that
///   matches ClickHouse, and the entity `_properties` columns are `to_json(...)`
///   STRINGs that must remain JSON text.
/// - `ARRAY` columns are handled by [`coerce_array`] (element-typed recursion).
///   `STRUCT`/`MAP` values still arrive as JSON-encoded strings and are left
///   as-is — tracked as a follow-up.
/// - A parse failure falls back to the original string rather than dropping the
///   value, so a surprising payload degrades to "uncoerced" rather than lossy.
fn coerce_scalar(value: Value, type_name: &str) -> Value {
    let s = match &value {
        Value::String(s) => s.as_str(),
        _ => return value,
    };
    let coerced = match type_name {
        "BOOLEAN" => match s {
            "true" => Some(Value::Bool(true)),
            "false" => Some(Value::Bool(false)),
            _ => None,
        },
        "BYTE" | "SHORT" | "INT" | "LONG" => s.parse::<i64>().ok().map(|n| Value::Number(n.into())),
        "FLOAT" | "DOUBLE" | "DECIMAL" => s
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number),
        _ => None,
    };
    coerced.unwrap_or(value)
}

/// Restore a native JSON array from an `ARRAY` column. The Statement Execution
/// API delivers the whole array as one JSON-encoded string (e.g.
/// `"[\"a\",\"b\"]"`) whose inner scalars are themselves strings. Parse it and
/// coerce each leaf by the element type from `type_text` (e.g. `ARRAY<INT>`), so
/// the result matches ClickHouse's native `["a","b"]` / `[1,2]` shape. Nested
/// `ARRAY<ARRAY<…>>` recurses. A parse failure returns the original value.
fn coerce_array(value: Value, type_text: Option<&str>) -> Value {
    let s = match &value {
        Value::String(s) => s,
        _ => return value,
    };
    match serde_json::from_str::<Value>(s) {
        Ok(parsed) => coerce_by_type(parsed, type_text.unwrap_or("ARRAY<STRING>")),
        Err(_) => value,
    }
}

/// Walk an already-parsed JSON value, coercing string leaves per `sql_type`
/// (the `type_text` for this level). Arrays recurse into their element type.
fn coerce_by_type(value: Value, sql_type: &str) -> Value {
    match value {
        Value::Array(items) => {
            let inner = array_element_type(sql_type).unwrap_or("STRING");
            Value::Array(
                items
                    .into_iter()
                    .map(|it| coerce_by_type(it, inner))
                    .collect(),
            )
        }
        Value::String(s) => {
            // Map SQL type names (type_text) onto the enum names coerce_scalar
            // keys on, then reuse it for the leaf scalar.
            let base = sql_type
                .split('(')
                .next()
                .unwrap_or(sql_type)
                .trim()
                .to_ascii_uppercase();
            let enum_name = match base.as_str() {
                "TINYINT" => "BYTE",
                "SMALLINT" => "SHORT",
                "INTEGER" => "INT",
                "BIGINT" => "LONG",
                "REAL" => "FLOAT",
                "NUMERIC" => "DECIMAL",
                other => other,
            };
            coerce_scalar(Value::String(s), enum_name)
        }
        other => other,
    }
}

/// Extract the element type from an `ARRAY<…>` type text (case-insensitive):
/// `ARRAY<STRING>` -> `STRING`, `ARRAY<ARRAY<INT>>` -> `ARRAY<INT>`. Returns
/// `None` for non-array types.
fn array_element_type(type_text: &str) -> Option<&str> {
    let t = type_text.trim();
    // `str::get` (not byte indexing) so a non-ASCII manifest type_text degrades
    // to None instead of panicking on a non-char-boundary slice.
    if t.ends_with('>')
        && t.len() > "ARRAY<>".len()
        && t.get(.."ARRAY<".len())
            .is_some_and(|p| p.eq_ignore_ascii_case("ARRAY<"))
    {
        Some(t["ARRAY<".len()..t.len() - 1].trim())
    } else {
        None
    }
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
    fn catalog_accessor_reflects_config() {
        // Introspection reads the configured catalog through this accessor.
        let exec = DatabricksSqlExecutor::new(cfg()).expect("client builds");
        assert_eq!(exec.catalog(), None);

        let mut c = cfg();
        c.catalog = Some("main".into());
        let exec = DatabricksSqlExecutor::new(c).expect("client builds");
        assert_eq!(exec.catalog(), Some("main"));
    }

    #[test]
    fn as_any_downcasts_to_concrete_executor() {
        // The introspect handler recovers the concrete type from the trait
        // object via this hook to drive `DatabricksProbe`.
        let exec = DatabricksSqlExecutor::new(cfg()).expect("client builds");
        let dynref: &dyn QueryExecutor = &exec;
        assert!(
            dynref
                .as_any()
                .and_then(|a| a.downcast_ref::<DatabricksSqlExecutor>())
                .is_some(),
            "DatabricksSqlExecutor should downcast from &dyn QueryExecutor"
        );
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
    fn coerce_scalar_restores_native_types() {
        // The JSON_ARRAY API delivers every value as a string; coercion keys on
        // the manifest type to restore the native JSON scalar.
        assert_eq!(coerce_scalar(json!("7"), "INT"), json!(7));
        assert_eq!(coerce_scalar(json!("7"), "LONG"), json!(7));
        assert_eq!(coerce_scalar(json!("-3"), "SHORT"), json!(-3));
        assert_eq!(coerce_scalar(json!("5"), "BYTE"), json!(5));
        assert_eq!(coerce_scalar(json!("9.5"), "DOUBLE"), json!(9.5));
        assert_eq!(coerce_scalar(json!("9.5"), "FLOAT"), json!(9.5));
        // DECIMAL coerces to a number, matching ClickHouse's JSONEachRow
        // (which also emits Decimal as a bare number, not a string).
        assert_eq!(coerce_scalar(json!("9.5"), "DECIMAL"), json!(9.5));
        assert_eq!(coerce_scalar(json!("true"), "BOOLEAN"), json!(true));
        assert_eq!(coerce_scalar(json!("false"), "BOOLEAN"), json!(false));
    }

    #[test]
    fn coerce_scalar_leaves_strings_and_complex_untouched() {
        // STRING stays a string (incl. the to_json `_properties` payload).
        assert_eq!(coerce_scalar(json!("hi"), "STRING"), json!("hi"));
        let jprops = json!("{\"x\":7}");
        assert_eq!(coerce_scalar(jprops.clone(), "STRING"), jprops);
        // DATE / TIMESTAMP keep their string rendering — matches ClickHouse.
        assert_eq!(
            coerce_scalar(json!("2020-01-01"), "DATE"),
            json!("2020-01-01")
        );
        // Complex types arrive as JSON-encoded strings and are left as-is
        // (element-typed recursion is a tracked follow-up).
        let arr = json!("[\"1\",\"2\"]");
        assert_eq!(coerce_scalar(arr.clone(), "ARRAY"), arr);
    }

    #[test]
    fn coerce_scalar_passes_through_null_and_native_and_unparseable() {
        // SQL NULL already arrives as JSON null — never stringified.
        assert_eq!(coerce_scalar(json!(null), "INT"), json!(null));
        // An already-native value (defensive: future API/format change) is kept.
        assert_eq!(coerce_scalar(json!(7), "INT"), json!(7));
        // A payload that doesn't parse degrades to the original string rather
        // than being dropped or panicking.
        assert_eq!(
            coerce_scalar(json!("not-a-number"), "INT"),
            json!("not-a-number")
        );
        // An unknown / unmapped type name leaves the value alone.
        assert_eq!(coerce_scalar(json!("x"), "BINARY"), json!("x"));
    }

    #[test]
    fn rows_from_response_coerces_using_manifest_types() {
        // End-to-end: a JSON_ARRAY-style response (all values as strings) keyed
        // by the manifest, with each scalar coerced to its native JSON type.
        let resp: StatementResponse = serde_json::from_value(json!({
            "statement_id": "stmt-coerce",
            "status": { "state": "SUCCEEDED" },
            "manifest": { "schema": { "columns": [
                { "name": "id", "type_name": "LONG" },
                { "name": "score", "type_name": "DOUBLE" },
                { "name": "active", "type_name": "BOOLEAN" },
                { "name": "name", "type_name": "STRING" },
                { "name": "missing", "type_name": "INT" }
            ]}},
            "result": { "data_array": [
                ["1", "9.5", "true", "alice", null]
            ]}
        }))
        .unwrap();
        let rows = rows_from_response(&resp).expect("parse");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["id"], json!(1));
        assert_eq!(rows[0]["score"], json!(9.5));
        assert_eq!(rows[0]["active"], json!(true));
        assert_eq!(rows[0]["name"], json!("alice"));
        assert_eq!(rows[0]["missing"], json!(null));
    }

    #[test]
    fn rows_from_response_without_type_names_passes_values_through() {
        // A manifest that omits type_name (older DBR / DDL) must not alter
        // values — they pass through exactly as the API delivered them.
        let resp: StatementResponse = serde_json::from_value(json!({
            "statement_id": "stmt-no-types",
            "status": { "state": "SUCCEEDED" },
            "manifest": { "schema": { "columns": [
                { "name": "a" },
                { "name": "b" }
            ]}},
            "result": { "data_array": [[1, "x"]] }
        }))
        .unwrap();
        let rows = rows_from_response(&resp).expect("parse");
        assert_eq!(rows[0]["a"], json!(1));
        assert_eq!(rows[0]["b"], json!("x"));
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
    fn debug_redacts_token() {
        // Critical: anything that reaches `{:?}` must NOT contain the
        // raw PAT. This matches RemoteConfig's password redaction.
        let c = DatabricksConfig::new(
            "ws.cloud.databricks.com",
            "wh-1",
            "dapi-SECRET-TOKEN-MUST-NOT-LEAK",
        );
        let s = format!("{c:?}");
        assert!(!s.contains("SECRET"), "token leaked into Debug output: {s}");
        assert!(
            s.contains("********"),
            "expected `********` in redacted Debug; got: {s}"
        );
        // The other fields should still be visible for triage.
        assert!(s.contains("ws.cloud.databricks.com"));
        assert!(s.contains("wh-1"));
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
        // Poll returns SUCCEEDED with the actual result. Assert bearer
        // auth on the GET too: Databricks requires authentication on
        // every Statement Execution API call, including status reads.
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-002"))
            .and(bearer_token("dapi-test"))
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
    async fn http_401_becomes_auth_error_with_body() {
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
        // 401 surfaces as a dedicated Auth error (not Remote) so callers can
        // distinguish an expired credential from a transient backend fault.
        match err {
            ExecutorError::Auth(body) => {
                assert!(body.contains("PERMISSION_DENIED"), "body: {body}");
            }
            other => panic!("expected Auth error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn execute_text_rejects_unsupported_format() {
        // An unsupported format is rejected BEFORE any HTTP call — no mock is
        // registered, so a wire hit would fail the test. (The four supported
        // formats are Pretty/PrettyCompact/CSV/CSVWithNames.)
        let server = MockServer::start().await;
        let exec = DatabricksSqlExecutor::new(cfg_for(&server)).expect("client builds");
        let err = exec
            .execute_text("SELECT 1", "JSONEachRow", None)
            .await
            .expect_err("should reject");
        assert!(
            matches!(err, ExecutorError::UnsupportedFormat(ref f) if f == "JSONEachRow"),
            "expected UnsupportedFormat(\"JSONEachRow\"), got {err:?}"
        );
    }

    #[tokio::test]
    async fn execute_text_renders_pretty_table() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "statement_id": "stmt-text-1",
                "status": { "state": "SUCCEEDED" },
                "manifest": manifest_with_columns(&["id", "name"]),
                "result": { "data_array": [[1, "alice"], [2, "bob"]] }
            })))
            .expect(1)
            .mount(&server)
            .await;

        let exec = DatabricksSqlExecutor::new(cfg_for(&server)).expect("client builds");
        let text = exec
            .execute_text("SELECT id, name FROM users", "PrettyCompact", None)
            .await
            .expect("execute_text");
        let lines: Vec<&str> = text.lines().collect();
        // top, header, separator, 2 data rows, bottom
        assert_eq!(lines.len(), 6, "got:\n{text}");
        assert!(lines[1].contains("id") && lines[1].contains("name"));
        assert!(lines[3].contains("alice"));
        assert!(lines[4].contains("bob"));
    }

    #[tokio::test]
    async fn execute_text_renders_csv_with_names() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "statement_id": "stmt-text-2",
                "status": { "state": "SUCCEEDED" },
                "manifest": manifest_with_columns(&["id", "name"]),
                "result": { "data_array": [[1, "alice"]] }
            })))
            .expect(1)
            .mount(&server)
            .await;

        let exec = DatabricksSqlExecutor::new(cfg_for(&server)).expect("client builds");
        let text = exec
            .execute_text("SELECT id, name FROM users", "CSVWithNames", None)
            .await
            .expect("execute_text");
        assert_eq!(text, "id,name\n1,alice\n");
    }

    fn external_links_cfg(server: &MockServer) -> DatabricksConfig {
        let mut c = cfg_for(server);
        c.disposition = ResultDisposition::ExternalLinks;
        c
    }

    /// Fast retries so failure-mode tests don't sleep the real backoff.
    fn retry_cfg(server: &MockServer, max_retries: u32) -> DatabricksConfig {
        let mut c = cfg_for(server);
        c.max_retries = max_retries;
        c.retry_base_delay = Duration::from_millis(1);
        c
    }

    #[tokio::test]
    async fn rate_limit_429_is_retried_then_succeeds() {
        let server = MockServer::start().await;
        // First POST → 429, second POST → SUCCEEDED. `up_to_n_times(1)` makes
        // the 429 mock serve exactly once; the later mounted 200 mock then
        // handles the retry.
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "0"))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "statement_id": "stmt-429",
                "status": { "state": "SUCCEEDED" },
                "manifest": manifest_with_columns(&["x"]),
                "result": { "data_array": [[7]] }
            })))
            .expect(1)
            .mount(&server)
            .await;

        let exec = DatabricksSqlExecutor::new(retry_cfg(&server, 3)).expect("client builds");
        let rows = exec
            .execute_json("SELECT 7 AS x", None)
            .await
            .expect("retried OK");
        assert_eq!(rows[0]["x"], json!(7));
    }

    #[tokio::test]
    async fn unauthorized_401_surfaces_as_auth_error_without_retry() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(
                ResponseTemplate::new(401).set_body_string("{\"message\":\"expired token\"}"),
            )
            // Exactly one call — a 401 must NOT be retried.
            .expect(1)
            .mount(&server)
            .await;

        let exec = DatabricksSqlExecutor::new(retry_cfg(&server, 3)).expect("client builds");
        let err = exec
            .execute_json("SELECT 1", None)
            .await
            .expect_err("should fail auth");
        assert!(
            matches!(err, ExecutorError::Auth(ref b) if b.contains("expired token")),
            "expected Auth error, got {err:?}"
        );
    }

    #[tokio::test]
    async fn retries_exhausted_returns_last_status() {
        let server = MockServer::start().await;
        // Every POST is 503. With max_retries=2 the executor makes 3 attempts
        // total and then surfaces the 503 as a Remote error.
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(ResponseTemplate::new(503).set_body_string("warehouse starting"))
            .expect(3)
            .mount(&server)
            .await;

        let exec = DatabricksSqlExecutor::new(retry_cfg(&server, 2)).expect("client builds");
        let err = exec
            .execute_json("SELECT 1", None)
            .await
            .expect_err("should exhaust retries");
        assert!(
            matches!(err, ExecutorError::Remote { status: 503, .. }),
            "expected Remote 503 after exhausting retries, got {err:?}"
        );
    }

    #[tokio::test]
    async fn oauth_m2m_fetches_token_then_caches_it() {
        let server = MockServer::start().await;

        // OIDC token endpoint: issued once, then cached for both queries.
        Mock::given(method("POST"))
            .and(path("/oidc/v1/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "access_token": "oauth-abc",
                "token_type": "Bearer",
                "expires_in": 3600
            })))
            .expect(1)
            .mount(&server)
            .await;
        // The Statement Execution call must carry the OAuth-issued bearer,
        // proving the executor used the fetched token, not a PAT.
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .and(bearer_token("oauth-abc"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "statement_id": "stmt-oauth",
                "status": { "state": "SUCCEEDED" },
                "manifest": manifest_with_columns(&["x"]),
                "result": { "data_array": [[1]] }
            })))
            .expect(2)
            .mount(&server)
            .await;

        let mut cfg = cfg_for(&server);
        let mut oauth = OAuthM2MConfig::new("sp-client", "sp-secret");
        oauth.token_url = Some(format!("{}/oidc/v1/token", server.uri()));
        cfg.oauth = Some(oauth);
        let exec = DatabricksSqlExecutor::new(cfg).expect("client builds");

        // Two queries — the token endpoint is hit exactly once (cached), the
        // statements endpoint twice, both with the OAuth bearer.
        for _ in 0..2 {
            let rows = exec.execute_json("SELECT 1 AS x", None).await.expect("ok");
            assert_eq!(rows[0]["x"], json!(1));
        }
    }

    #[tokio::test]
    async fn oauth_m2m_token_endpoint_rejection_is_auth_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/oidc/v1/token"))
            .respond_with(ResponseTemplate::new(401).set_body_string("invalid_client"))
            .mount(&server)
            .await;

        let mut cfg = cfg_for(&server);
        let mut oauth = OAuthM2MConfig::new("sp-client", "wrong-secret");
        oauth.token_url = Some(format!("{}/oidc/v1/token", server.uri()));
        cfg.oauth = Some(oauth);
        let exec = DatabricksSqlExecutor::new(cfg).expect("client builds");

        let err = exec
            .execute_json("SELECT 1", None)
            .await
            .expect_err("bad credentials");
        assert!(
            matches!(err, ExecutorError::Auth(_)),
            "token-endpoint rejection should surface as Auth, got {err:?}"
        );
    }

    #[tokio::test]
    async fn external_links_single_chunk_downloaded_and_keyed() {
        let server = MockServer::start().await;
        let chunk_url = format!("{}/staged/chunk0", server.uri());

        // Submit returns SUCCEEDED with one external link, no data_array.
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .and(bearer_token("dapi-test"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "statement_id": "stmt-ext-1",
                "status": { "state": "SUCCEEDED" },
                "manifest": manifest_with_columns(&["id", "name"]),
                "result": { "external_links": [
                    { "external_link": chunk_url, "chunk_index": 0 }
                ]}
            })))
            .expect(1)
            .mount(&server)
            .await;
        // The presigned chunk URL must be fetched WITHOUT a bearer token —
        // mount it with no bearer matcher; a request carrying auth simply
        // won't be what we assert on, but wiremock has no negative matcher,
        // so we rely on the download path not adding one (covered by review).
        Mock::given(method("GET"))
            .and(path("/staged/chunk0"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!([[1, "alice"], [2, "bob"]])),
            )
            .expect(1)
            .mount(&server)
            .await;

        let exec = DatabricksSqlExecutor::new(external_links_cfg(&server)).expect("client builds");
        let rows = exec
            .execute_json("SELECT id, name FROM users", None)
            .await
            .expect("execute_json");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["id"], json!(1));
        assert_eq!(rows[0]["name"], json!("alice"));
        assert_eq!(rows[1]["name"], json!("bob"));
    }

    #[tokio::test]
    async fn external_links_follows_next_chunk_index() {
        let server = MockServer::start().await;
        let chunk0 = format!("{}/staged/c0", server.uri());
        let chunk1 = format!("{}/staged/c1", server.uri());

        // Chunk 0 in the submit body points forward to chunk 1.
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "statement_id": "stmt-ext-2",
                "status": { "state": "SUCCEEDED" },
                "manifest": manifest_with_columns(&["n"]),
                "result": { "external_links": [
                    { "external_link": chunk0, "chunk_index": 0, "next_chunk_index": 1 }
                ]}
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/staged/c0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([[1], [2]])))
            .expect(1)
            .mount(&server)
            .await;
        // Chunk 1's metadata is fetched from the authenticated chunks
        // endpoint; it carries the final external link (no next index).
        Mock::given(method("GET"))
            .and(path("/api/2.0/sql/statements/stmt-ext-2/result/chunks/1"))
            .and(bearer_token("dapi-test"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "external_links": [ { "external_link": chunk1, "chunk_index": 1 } ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/staged/c1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([[3], [4]])))
            .expect(1)
            .mount(&server)
            .await;

        let exec = DatabricksSqlExecutor::new(external_links_cfg(&server)).expect("client builds");
        let rows = exec
            .execute_json("SELECT n FROM t", None)
            .await
            .expect("execute_json");
        let got: Vec<i64> = rows.iter().map(|r| r["n"].as_i64().unwrap()).collect();
        assert_eq!(got, vec![1, 2, 3, 4], "rows from both chunks, in order");
    }

    #[tokio::test]
    async fn external_links_disposition_sent_in_submit_body() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .and(wiremock::matchers::body_partial_json(
                json!({ "disposition": "EXTERNAL_LINKS" }),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "statement_id": "stmt-ext-3",
                "status": { "state": "SUCCEEDED" },
                "manifest": manifest_with_columns(&["x"]),
                "result": { "external_links": [] }
            })))
            .expect(1)
            .mount(&server)
            .await;

        let exec = DatabricksSqlExecutor::new(external_links_cfg(&server)).expect("client builds");
        let rows = exec
            .execute_json("SELECT 1 AS x", None)
            .await
            .expect("execute_json");
        assert!(rows.is_empty(), "no links → empty rows");
    }

    #[test]
    fn coerce_array_of_strings() {
        // The dominant demo case: ARRAY<STRING> arrives as a JSON string.
        let v = Value::String("[\"Furniture\",\"Books\"]".to_string());
        assert_eq!(
            coerce_array(v, Some("ARRAY<STRING>")),
            json!(["Furniture", "Books"])
        );
    }

    #[test]
    fn coerce_array_of_ints_unstrings_elements() {
        // Inner scalars come stringified; element type drives numeric coercion.
        let v = Value::String("[\"10\",\"20\",\"30\"]".to_string());
        assert_eq!(coerce_array(v, Some("ARRAY<INT>")), json!([10, 20, 30]));
        // SQL spelling (BIGINT) maps onto the LONG coercion path too.
        let v = Value::String("[\"1\",\"2\"]".to_string());
        assert_eq!(coerce_array(v, Some("ARRAY<BIGINT>")), json!([1, 2]));
    }

    #[test]
    fn coerce_array_nested_and_empty() {
        let v = Value::String("[[\"1\",\"2\"],[\"3\"]]".to_string());
        assert_eq!(
            coerce_array(v, Some("ARRAY<ARRAY<INT>>")),
            json!([[1, 2], [3]])
        );
        assert_eq!(
            coerce_array(Value::String("[]".to_string()), Some("ARRAY<STRING>")),
            json!([])
        );
    }

    #[test]
    fn coerce_array_missing_type_text_defaults_to_strings() {
        // No type_text → treat elements as strings (parse to native array still).
        let v = Value::String("[\"a\",\"b\"]".to_string());
        assert_eq!(coerce_array(v, None), json!(["a", "b"]));
    }

    #[test]
    fn coerce_array_malformed_falls_back_to_original() {
        let v = Value::String("not json".to_string());
        assert_eq!(
            coerce_array(v, Some("ARRAY<STRING>")),
            Value::String("not json".to_string())
        );
    }

    #[test]
    fn array_element_type_parsing() {
        assert_eq!(array_element_type("ARRAY<STRING>"), Some("STRING"));
        assert_eq!(array_element_type("array<int>"), Some("int"));
        assert_eq!(array_element_type("ARRAY<ARRAY<INT>>"), Some("ARRAY<INT>"));
        assert_eq!(array_element_type("STRING"), None);
        assert_eq!(array_element_type("MAP<STRING,INT>"), None);
        // Non-ASCII type_text must degrade to None, never panic on a byte slice
        // that lands inside a multi-byte char (e.g. byte 6 inside 'é').
        assert_eq!(array_element_type("ARRAYéxx"), None);
        assert_eq!(array_element_type("«"), None);
    }
}
