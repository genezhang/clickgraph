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
}

#[async_trait]
impl QueryExecutor for DatabricksSqlExecutor {
    async fn execute_json(
        &self,
        sql: &str,
        _role: Option<&str>,
    ) -> Result<Vec<Value>, ExecutorError> {
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

        // Poll until the statement reaches a terminal state. The
        // initial submit may already return SUCCEEDED if the query
        // finished within `wait_timeout`; otherwise we poll.
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
        // inline `data_array`; fetch and concatenate them, then key by the
        // manifest columns exactly as the inline path does.
        let rows = if let Some(links) = response
            .result
            .as_ref()
            .and_then(|r| r.external_links.as_ref())
        {
            let data = self.fetch_external_links(&statement_id, links).await?;
            rows_from_data(&response, &data)?
        } else {
            rows_from_response(&response)?
        };

        log::info!(
            target: "deltagraph::databricks",
            "statement_id={statement_id} state=SUCCEEDED polls={polls} \
             duration_ms={} rows={}",
            started.elapsed().as_millis(),
            rows.len(),
        );
        Ok(rows)
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
/// name. Pulled out as a free function so unit tests can feed
/// hand-built responses without an HTTP round trip.
fn rows_from_response(resp: &StatementResponse) -> Result<Vec<Value>, ExecutorError> {
    let data = match resp.result.as_ref().and_then(|r| r.data_array.as_ref()) {
        Some(rows) => rows.as_slice(),
        // No data_array is valid for empty result sets — e.g. a DDL
        // statement or a SELECT with zero rows. Return an empty Vec.
        None => return Ok(Vec::new()),
    };
    rows_from_data(resp, data)
}

/// Key an already-collected set of row arrays by the response's manifest
/// column names. Shared by the INLINE path (`data_array`) and the
/// EXTERNAL_LINKS path (downloaded chunks).
fn rows_from_data(
    resp: &StatementResponse,
    data: &[Vec<Value>],
) -> Result<Vec<Value>, ExecutorError> {
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
}
