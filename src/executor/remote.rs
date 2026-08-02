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
//!   crate client (`RoleConnectionPool::http_endpoint` →
//!   `ConnectionConfig::standard_options`) so results are identical.

use async_trait::async_trait;
use bytes::Bytes;
use serde_json::Value;
use std::sync::Arc;

use super::{ExecutorError, QueryExecutor};
use crate::server::connection_pool::RoleConnectionPool;
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
        }
    }

    /// Phase B: execute a SELECT via direct HTTP and capture
    /// `X-ClickHouse-Summary` (read_rows / read_bytes / elapsed). Returns the
    /// same `Vec<Value>` (JSONEachRow) shape as the crate path.
    async fn execute_json_via_http(
        &self,
        sql: &str,
        role: Option<&str>,
    ) -> Result<Vec<Value>, ExecutorError> {
        let ep = self.pool.http_endpoint(role);

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
        }

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
) -> Result<Vec<u8>, ExecutorError> {
    let mut buf: Vec<u8> = Vec::new();
    while let Some(chunk) = cursor.next().await.map_err(|e| {
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
        let client = self.pool.get_client(role).await;
        let cursor = client
            .query(&escape_question_marks(sql))
            .fetch_bytes("JSONEachRow")
            .map_err(|e| {
                log::error!("ClickHouse query failed. SQL was:\n{}\nError: {}", sql, e);
                ExecutorError::QueryFailed(e.to_string())
            })?;
        let buf = drain_cursor(cursor, sql).await?;

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
        let client = self.pool.get_client(role).await;
        let cursor = client
            .query(&escape_question_marks(sql))
            .fetch_bytes(format)
            .map_err(|e| {
                log::error!("ClickHouse query failed. SQL was:\n{}\nError: {}", sql, e);
                ExecutorError::QueryFailed(e.to_string())
            })?;
        let buf = drain_cursor(cursor, sql).await?;

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
}
