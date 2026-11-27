//! Remote ClickHouse executor backed by `RoleConnectionPool`.
//!
//! This is a thin wrapper that makes the existing connection pool implement
//! the `QueryExecutor` trait, preserving all existing behaviour (role-based
//! pools, cluster round-robin, etc.).
//!
//! It also records observability stats:
//! - **Phase A (always on):** bytes received from ClickHouse per query are
//!   pushed into the per-query metrics slot (a no-op outside a metrics scope).
//! - **Phase B (opt-in, `CLICKGRAPH_METRICS_CH_SUMMARY=1`):** the JSON read
//!   path is executed via a direct HTTP request so the `X-ClickHouse-Summary`
//!   response header (read_rows / read_bytes / elapsed) — which the `clickhouse`
//!   crate drops — can be captured. The request uses the SAME settings as the
//!   crate client (`RoleConnectionPool::get_client_and_endpoint` →
//!   `ConnectionConfig::standard_options`) so results are identical.

use async_trait::async_trait;
use bytes::Bytes;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

use super::{ExecutorError, QueryExecutor};
use crate::server::connection_pool::{ChHttpEndpoint, RoleConnectionPool};
use crate::server::metrics::{record_ch_network_bytes, record_ch_summary};

/// SQL executor that delegates to a remote ClickHouse server via HTTP.
///
/// Wraps [`RoleConnectionPool`] so the rest of the codebase can use the
/// backend-agnostic [`QueryExecutor`] trait.
pub struct RemoteClickHouseExecutor {
    pool: Arc<RoleConnectionPool>,
    /// When true, the JSON read path runs via direct HTTP to capture the
    /// ClickHouse summary header (Phase B). From `CLICKGRAPH_METRICS_CH_SUMMARY`.
    ch_summary: bool,
    /// Reusable HTTP client for the Phase B summary path (shares a connection
    /// pool). Only used when `ch_summary` is set.
    http: reqwest::Client,
    /// Server request timeout. Sent to ClickHouse as `max_execution_time` so a
    /// read query stops even if this process dies before it can send a KILL.
    /// 0 = no ClickHouse-side limit.
    query_timeout_secs: u64,
}

impl RemoteClickHouseExecutor {
    pub fn new(pool: Arc<RoleConnectionPool>) -> Self {
        Self::with_ch_summary(pool, false)
    }

    pub fn with_ch_summary(pool: Arc<RoleConnectionPool>, ch_summary: bool) -> Self {
        Self {
            pool,
            ch_summary,
            http: reqwest::Client::new(),
            query_timeout_secs: 0,
        }
    }

    pub fn with_query_timeout(mut self, query_timeout_secs: u64) -> Self {
        self.query_timeout_secs = query_timeout_secs;
        self
    }

    /// Phase B: execute a SELECT via direct HTTP and capture
    /// `X-ClickHouse-Summary` (read_rows / read_bytes / elapsed). Returns the
    /// same `Vec<Value>` (JSONEachRow) shape as the crate path.
    async fn execute_json_via_http(
        &self,
        sql: &str,
        role: Option<&str>,
    ) -> Result<Vec<Value>, ExecutorError> {
        let (_, ep) = self.pool.get_client_and_endpoint(role).await;
        let query_id = Uuid::new_v4().to_string();

        // Compose the URL exactly as the crate would: database + standard
        // options as query params, plus JSONEachRow output and
        // wait_end_of_query=1 (the latter makes ClickHouse buffer server-side so
        // the summary is a complete response header rather than a trailer).
        let mut url = reqwest::Url::parse(&ep.url)
            .map_err(|e| ExecutorError::Io(format!("invalid ClickHouse URL: {e}")))?;
        {
            let mut q = url.query_pairs_mut();
            q.append_pair("database", &ep.database);
            for (name, value) in &ep.options {
                q.append_pair(name, value);
            }
            q.append_pair("default_format", "JSONEachRow");
            q.append_pair("wait_end_of_query", "1");
            q.append_pair("query_id", &query_id);
            if self.query_timeout_secs > 0 {
                q.append_pair("max_execution_time", &self.query_timeout_secs.to_string());
            }
        }

        let mut cancel = CancelOnDrop::new(self.http.clone(), ep.clone(), query_id);
        let resp = self
            .http
            .post(url)
            .header("X-ClickHouse-User", &ep.user)
            .header("X-ClickHouse-Key", &ep.password)
            .body(sql.to_string())
            .send()
            .await
            .map_err(|e| ExecutorError::Io(format!("request failed: {e}")))?;

        let status = resp.status();
        // Capture the summary header before consuming the body.
        if let Some(summary) = resp
            .headers()
            .get("x-clickhouse-summary")
            .and_then(|v| v.to_str().ok())
        {
            record_summary_header(summary);
        }
        let body = resp
            .bytes()
            .await
            .map_err(|e| ExecutorError::Io(format!("reading response body: {e}")))?;
        cancel.disarm();
        if !status.is_success() {
            let text = String::from_utf8_lossy(&body);
            log::error!("ClickHouse query failed. SQL was:\n{sql}\nError: {text}");
            return Err(ExecutorError::QueryFailed(text.to_string()));
        }
        record_ch_network_bytes(body.len() as u64);

        let mut rows = Vec::new();
        for line in body.split(|&b| b == b'\n') {
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            let value: Value = serde_json::from_slice(line).map_err(|e| {
                log::error!("Failed to parse JSON from ClickHouse response: {e}");
                ExecutorError::Parse(e.to_string())
            })?;
            rows.push(value);
        }
        Ok(rows)
    }
}

/// Cancels one ClickHouse query when its Rust future is dropped before the
/// response completes. The server's HTTP timeout drops the handler future, and
/// ClickHouse does not stop a query when the client closes the connection
/// (`cancel_http_readonly_queries_on_client_close` defaults to 0).
struct CancelOnDrop {
    http: reqwest::Client,
    endpoint: ChHttpEndpoint,
    query_id: String,
    armed: bool,
}

impl CancelOnDrop {
    fn new(http: reqwest::Client, endpoint: ChHttpEndpoint, query_id: String) -> Self {
        Self {
            http,
            endpoint,
            query_id,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let http = self.http.clone();
        let endpoint = self.endpoint.clone();
        let query_id = self.query_id.clone();
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    kill_query(&http, &endpoint, &query_id).await;
                });
            }
            Err(_) => log::warn!(
                "ClickHouse query {} was abandoned, and no Tokio runtime is available to cancel it",
                query_id
            ),
        }
    }
}

/// Send `KILL QUERY ... ASYNC` for an abandoned query. A read-only user can
/// cancel its own query by ID without the `KILL QUERY` privilege. Failures are
/// logged, not returned: the caller is already unwinding.
async fn kill_query(http: &reqwest::Client, ep: &ChHttpEndpoint, query_id: &str) {
    let mut url = match reqwest::Url::parse(&ep.url) {
        Ok(url) => url,
        Err(e) => {
            log::warn!(
                "cannot cancel ClickHouse query {}: invalid URL: {}",
                query_id,
                e
            );
            return;
        }
    };
    {
        let mut q = url.query_pairs_mut();
        q.append_pair("database", &ep.database);
        for (name, value) in &ep.options {
            q.append_pair(name, value);
        }
    }
    // No `kill_throw_if_noop`: ClickHouse 25.8 rejects it as an unknown
    // setting, and `ASYNC` is already a no-op when the query has finished.
    let sql = format!("KILL QUERY WHERE query_id = '{}' ASYNC", query_id);
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        http.post(url)
            .header("X-ClickHouse-User", &ep.user)
            .header("X-ClickHouse-Key", &ep.password)
            .body(sql)
            .send(),
    )
    .await;
    match result {
        Ok(Ok(resp)) if !resp.status().is_success() => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            log::warn!(
                "ClickHouse query {} was abandoned but could not be cancelled: HTTP {}: {}",
                query_id,
                status,
                body
            );
        }
        Ok(Ok(_)) => {}
        Ok(Err(e)) => log::warn!(
            "ClickHouse query {} was abandoned but could not be cancelled: {}",
            query_id,
            e
        ),
        Err(_) => log::warn!(
            "ClickHouse query {} was abandoned; the cancel request timed out",
            query_id
        ),
    }
}

/// Escape literal `?` as `??` for the `clickhouse` crate's `Client::query`.
///
/// The crate treats `?` in the SQL template as a bind-parameter placeholder
/// (and `?fields` as a special field-list placeholder), erroring with
/// "unbound query argument" at execution if no matching `.bind()` supplies a
/// value (see clickhouse-rs `src/sql/mod.rs`). ClickGraph never parameterizes —
/// it hands the crate fully-rendered SQL with all literals inlined — so every
/// `?` is meant to reach ClickHouse verbatim (e.g. a string literal like
/// `'why?'`, a regex `=~ 'a?c'`, or a `(?i)` inline flag). The crate's own
/// escape for a literal `?` is `??`, which its template scanner collapses back
/// to a single `?`; doubling is therefore an exact, reversible transform.
///
/// This MUST be applied only on the crate `Client::query` path — the direct-HTTP
/// path (`execute_json_via_http`) sends the raw body and needs the literal `?`.
fn escape_question_marks(sql: &str) -> std::borrow::Cow<'_, str> {
    if sql.contains('?') {
        std::borrow::Cow::Owned(sql.replace('?', "??"))
    } else {
        std::borrow::Cow::Borrowed(sql)
    }
}

/// Parse the `X-ClickHouse-Summary` JSON (fields are quoted decimal strings) and
/// record read_rows / read_bytes / elapsed_ns into the per-query metrics slot.
fn record_summary_header(header: &str) {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(header) else {
        return;
    };
    let num = |k: &str| -> u64 {
        v.get(k)
            .and_then(|x| x.as_str())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0)
    };
    record_ch_summary(num("read_rows"), num("read_bytes"), num("elapsed_ns"));
}

/// Drain a `fetch_bytes` cursor into one buffer and record the bytes received.
///
/// Reading the raw chunks (rather than `.lines()`, which consumes the cursor)
/// lets us call `received_bytes()` afterwards for the Phase A network-bytes
/// metric. The full response was already buffered before processing, so this
/// is memory-equivalent to the previous line-streaming path.
async fn drain_cursor(
    mut cursor: clickhouse::query::BytesCursor,
    sql: &str,
    cancel: &mut CancelOnDrop,
) -> Result<Vec<u8>, ExecutorError> {
    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = cursor.next().await.map_err(|e| {
        // ClickHouse answered with an exception, so the query is already over:
        // a KILL would be pointless. Any other error (a broken connection, a
        // read that timed out) may leave the query running, so it stays armed.
        if matches!(e, clickhouse::error::Error::BadResponse(_)) {
            cancel.disarm();
        }
        log::error!("ClickHouse read failed. SQL was:\n{}\nError: {}", sql, e);
        ExecutorError::Io(e.to_string())
    })? {
        let chunk: Bytes = chunk;
        buf.extend_from_slice(&chunk);
    }
    record_ch_network_bytes(cursor.received_bytes());
    Ok(buf)
}

#[async_trait]
impl QueryExecutor for RemoteClickHouseExecutor {
    async fn execute_json(
        &self,
        sql: &str,
        role: Option<&str>,
    ) -> Result<Vec<Value>, ExecutorError> {
        // Phase B: capture the ClickHouse summary via a direct HTTP request.
        if self.ch_summary {
            return self.execute_json_via_http(sql, role).await;
        }
        let (client, ep) = self.pool.get_client_and_endpoint(role).await;
        let query_id = Uuid::new_v4().to_string();
        let mut query = client
            .query(&escape_question_marks(sql))
            .with_option("query_id", query_id.as_str());
        if self.query_timeout_secs > 0 {
            query = query.with_option("max_execution_time", self.query_timeout_secs.to_string());
        }
        let cursor = query.fetch_bytes("JSONEachRow").map_err(|e| {
            log::error!("ClickHouse query failed. SQL was:\n{}\nError: {}", sql, e);
            ExecutorError::QueryFailed(e.to_string())
        })?;
        let mut cancel = CancelOnDrop::new(self.http.clone(), ep, query_id);
        let buf = match drain_cursor(cursor, sql, &mut cancel).await {
            Ok(buf) => {
                cancel.disarm();
                buf
            }
            Err(e) => return Err(e),
        };

        let mut rows = Vec::new();
        for line in buf.split(|&b| b == b'\n') {
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            let value: Value = serde_json::from_slice(line).map_err(|e| {
                log::error!("Failed to parse JSON from ClickHouse response: {}", e);
                ExecutorError::Parse(e.to_string())
            })?;
            rows.push(value);
        }
        Ok(rows)
    }

    async fn execute_text(
        &self,
        sql: &str,
        format: &str,
        role: Option<&str>,
    ) -> Result<String, ExecutorError> {
        let (client, ep) = self.pool.get_client_and_endpoint(role).await;
        let query_id = Uuid::new_v4().to_string();
        let mut query = client
            .query(&escape_question_marks(sql))
            .with_option("query_id", query_id.as_str());
        if self.query_timeout_secs > 0 {
            query = query.with_option("max_execution_time", self.query_timeout_secs.to_string());
        }
        let cursor = query.fetch_bytes(format).map_err(|e| {
            log::error!("ClickHouse query failed. SQL was:\n{}\nError: {}", sql, e);
            ExecutorError::QueryFailed(e.to_string())
        })?;
        let mut cancel = CancelOnDrop::new(self.http.clone(), ep, query_id);
        let buf = match drain_cursor(cursor, sql, &mut cancel).await {
            Ok(buf) => {
                cancel.disarm();
                buf
            }
            Err(e) => return Err(e),
        };

        let mut text = String::from_utf8(buf).map_err(|e| ExecutorError::Parse(e.to_string()))?;
        // Preserve the previous behaviour of joining lines without a trailing
        // newline (ClickHouse terminates each row, incl. the last, with `\n`).
        if text.ends_with('\n') {
            text.pop();
        }
        Ok(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::metrics::{current_ch_stats, with_ch_stats_scope};
    use wiremock::matchers::{body_string_contains, method};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn escape_question_marks_doubles_every_literal_question_mark() {
        assert_eq!(escape_question_marks("no marks here"), "no marks here");
        assert_eq!(escape_question_marks("SELECT 'why?'"), "SELECT 'why??'");
        assert_eq!(
            escape_question_marks("replace('a?b', '?', '!')"),
            "replace('a??b', '??', '!')"
        );
        // Regex operands: a `?` quantifier and a `(?i)` inline flag.
        assert_eq!(escape_question_marks("match(x, 'a?c')"), "match(x, 'a??c')");
        assert_eq!(
            escape_question_marks("match(x, '(?i)hello')"),
            "match(x, '(??i)hello')"
        );
    }

    #[test]
    fn escaped_sql_round_trips_through_the_clickhouse_query_builder() {
        // The crate collapses `??` back to a single `?` and treats a bare `?` as
        // an unbound placeholder. `Query::sql_display()` renders the final SQL the
        // crate would send; feeding our escaped form must reproduce the original
        // literal `?` exactly, with no leftover placeholder.
        let client = clickhouse::Client::default();
        for original in [
            "SELECT 'why?' AS s",
            "SELECT replace('a?b', '?', '!') AS s",
            "SELECT match('Hello', '(?i)hello') AS s",
            "SELECT 'no marks' AS s",
        ] {
            let escaped = escape_question_marks(original);
            let rendered = client.query(&escaped).sql_display().to_string();
            assert_eq!(
                rendered, original,
                "escaped SQL did not round-trip back to the literal for {original:?}"
            );
        }
    }

    #[test]
    fn unescaped_question_mark_is_rejected_by_the_builder_but_escaped_is_accepted() {
        // Necessity check: `fetch_bytes` calls the crate's `SqlBuilder::finish()`
        // (which errors on any unbound `?` placeholder) BEFORE any network I/O —
        // `do_execute` only *builds* the request future, it does not await it — so
        // this proves the failure mode, and that the escape fixes it, without a
        // live server. A `?` run of any parity must round-trip.
        //
        // The client needs a syntactically valid URL so the only pre-network
        // failure is the unbound `?` itself (a default client has an empty URL
        // that would fail `Url::parse` regardless).
        let client = clickhouse::Client::default().with_url("http://localhost:8123");
        let is_unbound = |sql: &str| {
            client
                .clone()
                .query(sql)
                .fetch_bytes("TSV")
                .err()
                .map(|e| e.to_string().contains("unbound query argument"))
                .unwrap_or(false)
        };
        for sql in [
            "SELECT 'why?' AS s",
            "SELECT 'a???b' AS s",
            "SELECT '?fields' AS s",
        ] {
            // Raw literal `?` -> the crate reads a placeholder and refuses to build.
            assert!(
                is_unbound(sql),
                "expected an unbound-argument error for raw {sql:?}"
            );
            // Escaped -> no unbound placeholder remains, so the builder succeeds.
            assert!(
                !is_unbound(&escape_question_marks(sql)),
                "escaped {sql:?} should build without an unbound argument"
            );
        }
        // A `?`-free query never trips the placeholder path.
        assert!(!is_unbound("SELECT 1 AS s"));
    }

    #[tokio::test]
    async fn summary_header_parsed_and_recorded() {
        let stats = with_ch_stats_scope(async {
            record_summary_header(
                r#"{"read_rows":"123","read_bytes":"4096","written_rows":"0","elapsed_ns":"777"}"#,
            );
            current_ch_stats()
        })
        .await
        .expect("in scope");
        assert_eq!(stats.read_rows, Some(123));
        assert_eq!(stats.read_bytes, Some(4096));
        assert_eq!(stats.elapsed_ns, Some(777));
    }

    #[tokio::test]
    async fn malformed_summary_header_is_ignored() {
        let stats = with_ch_stats_scope(async {
            record_summary_header("not json");
            current_ch_stats()
        })
        .await
        .expect("in scope");
        // No panic; nothing recorded.
        assert_eq!(stats.read_rows, None);
    }

    #[tokio::test]
    async fn missing_fields_default_to_zero() {
        let stats = with_ch_stats_scope(async {
            record_summary_header(r#"{"read_rows":"5"}"#);
            current_ch_stats()
        })
        .await
        .expect("in scope");
        assert_eq!(stats.read_rows, Some(5));
        assert_eq!(stats.read_bytes, Some(0));
        assert_eq!(stats.elapsed_ns, Some(0));
    }

    async fn pool_for(server: &MockServer) -> Arc<RoleConnectionPool> {
        Arc::new(
            RoleConnectionPool::new_with_params(&server.uri(), "default", "", None, None, 10)
                .await
                .expect("pool"),
        )
    }

    /// The `clickhouse` crate puts read queries up to 8 KiB in the URL and
    /// longer ones in the body. Use a longer statement so the mock can match
    /// the body.
    fn long_select() -> String {
        format!("SELECT 1 /* {} */", "x".repeat(9000))
    }

    fn query_id_of(request: &wiremock::Request) -> String {
        request
            .url
            .query_pairs()
            .find(|(name, _)| name == "query_id")
            .map(|(_, value)| value.into_owned())
            .expect("request carries a query_id")
    }

    async fn wait_for_request(
        server: &MockServer,
        predicate: impl Fn(&wiremock::Request) -> bool,
    ) -> wiremock::Request {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if let Some(requests) = server.received_requests().await {
                    if let Some(request) = requests.into_iter().find(|r| predicate(r)) {
                        return request;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("request did not arrive")
    }

    #[tokio::test]
    async fn dropped_query_sends_kill_with_id_and_execution_limit() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_string_contains("SELECT 1"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(5)))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(body_string_contains("KILL QUERY"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let executor = Arc::new(
            RemoteClickHouseExecutor::with_ch_summary(pool_for(&server).await, false)
                .with_query_timeout(2),
        );
        let run = executor.clone();
        let task = tokio::spawn(async move { run.execute_json(&long_select(), None).await });

        let query = wait_for_request(&server, |r| {
            String::from_utf8_lossy(&r.body).contains("SELECT 1")
        })
        .await;
        let query_string = query.url.query().unwrap_or("");
        assert!(query_string.contains("query_id="), "{query_string}");
        assert!(
            query_string.contains("max_execution_time=2"),
            "{query_string}"
        );

        task.abort();
        let _ = task.await;

        let kill = wait_for_request(&server, |r| {
            String::from_utf8_lossy(&r.body).contains("KILL QUERY")
        })
        .await;
        assert!(
            String::from_utf8_lossy(&kill.body).contains(&query_id_of(&query)),
            "the KILL targets a different query id: {}",
            String::from_utf8_lossy(&kill.body)
        );
    }

    #[tokio::test]
    async fn dropped_summary_query_sends_kill() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_string_contains("SELECT 1"))
            .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(5)))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(body_string_contains("KILL QUERY"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let executor = Arc::new(
            RemoteClickHouseExecutor::with_ch_summary(pool_for(&server).await, true)
                .with_query_timeout(2),
        );
        let run = executor.clone();
        let task = tokio::spawn(async move { run.execute_json(&long_select(), None).await });

        let query = wait_for_request(&server, |r| {
            String::from_utf8_lossy(&r.body).contains("SELECT 1")
        })
        .await;
        let query_string = query.url.query().unwrap_or("");
        assert!(query_string.contains("query_id="), "{query_string}");
        assert!(
            query_string.contains("max_execution_time=2"),
            "{query_string}"
        );

        task.abort();
        let _ = task.await;

        let kill = wait_for_request(&server, |r| {
            String::from_utf8_lossy(&r.body).contains("KILL QUERY")
        })
        .await;
        assert!(
            String::from_utf8_lossy(&kill.body).contains(&query_id_of(&query)),
            "the KILL targets a different query id: {}",
            String::from_utf8_lossy(&kill.body)
        );
    }

    #[tokio::test]
    async fn completed_query_is_not_killed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_string_contains("SELECT 1"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{\"x\":1}\n"))
            .mount(&server)
            .await;

        let executor = RemoteClickHouseExecutor::with_ch_summary(pool_for(&server).await, false)
            .with_query_timeout(2);
        let rows = executor.execute_json(&long_select(), None).await.unwrap();
        assert_eq!(rows.len(), 1);

        tokio::time::sleep(Duration::from_millis(100)).await;
        let requests = server.received_requests().await.unwrap_or_default();
        assert!(!requests
            .iter()
            .any(|r| String::from_utf8_lossy(&r.body).contains("KILL QUERY")));
    }

    #[tokio::test]
    async fn completed_summary_query_is_not_killed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_string_contains("SELECT 1"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{\"x\":1}\n"))
            .mount(&server)
            .await;

        let executor = RemoteClickHouseExecutor::with_ch_summary(pool_for(&server).await, true)
            .with_query_timeout(2);
        let rows = executor.execute_json(&long_select(), None).await.unwrap();
        assert_eq!(rows.len(), 1);

        tokio::time::sleep(Duration::from_millis(100)).await;
        let requests = server.received_requests().await.unwrap_or_default();
        assert!(!requests
            .iter()
            .any(|r| String::from_utf8_lossy(&r.body).contains("KILL QUERY")));
    }

    /// A query ClickHouse itself rejected is over: the guard must not send a
    /// KILL for it, on either path.
    #[tokio::test]
    async fn rejected_query_is_not_killed() {
        for ch_summary in [false, true] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(body_string_contains("SELECT 1"))
                .respond_with(
                    ResponseTemplate::new(500).set_body_string(
                        "Code: 47. DB::Exception: Unknown identifier `nonexistent`",
                    ),
                )
                .mount(&server)
                .await;

            let executor =
                RemoteClickHouseExecutor::with_ch_summary(pool_for(&server).await, ch_summary)
                    .with_query_timeout(2);
            assert!(executor.execute_json(&long_select(), None).await.is_err());

            tokio::time::sleep(Duration::from_millis(100)).await;
            let requests = server.received_requests().await.unwrap_or_default();
            assert!(
                !requests
                    .iter()
                    .any(|r| String::from_utf8_lossy(&r.body).contains("KILL QUERY")),
                "summary={ch_summary}: a rejected query was killed"
            );
        }
    }

    /// A transport failure may leave the query running, so the guard still
    /// sends the KILL.
    #[tokio::test]
    async fn transport_failure_still_sends_kill() {
        let server = MockServer::start().await;
        // The body is shorter than the declared length: reading it fails.
        Mock::given(method("POST"))
            .and(body_string_contains("SELECT 1"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-length", "9999")
                    .set_body_string("{\"x\":1}\n"),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(body_string_contains("KILL QUERY"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let executor = RemoteClickHouseExecutor::with_ch_summary(pool_for(&server).await, false)
            .with_query_timeout(2);
        assert!(executor.execute_json(&long_select(), None).await.is_err());

        wait_for_request(&server, |r| {
            String::from_utf8_lossy(&r.body).contains("KILL QUERY")
        })
        .await;
    }
}
