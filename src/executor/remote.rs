//! Remote ClickHouse executor backed by `RoleConnectionPool`.
//!
//! This is a thin wrapper that makes the existing connection pool implement
//! the `QueryExecutor` trait, preserving all existing behaviour (role-based
//! pools, cluster round-robin, etc.).
//!
//! It also records observability stats (Phase A): the bytes received from
//! ClickHouse for each query are pushed into the per-query metrics slot (a
//! no-op outside a metrics scope). The richer `X-ClickHouse-Summary`
//! (read_rows/read_bytes) is not exposed by the `clickhouse` crate and is a
//! gated follow-up.

use async_trait::async_trait;
use bytes::Bytes;
use serde_json::Value;
use std::sync::Arc;

use super::{ExecutorError, QueryExecutor};
use crate::server::connection_pool::RoleConnectionPool;
use crate::server::metrics::record_ch_network_bytes;

/// SQL executor that delegates to a remote ClickHouse server via HTTP.
///
/// Wraps [`RoleConnectionPool`] so the rest of the codebase can use the
/// backend-agnostic [`QueryExecutor`] trait.
pub struct RemoteClickHouseExecutor {
    pool: Arc<RoleConnectionPool>,
}

impl RemoteClickHouseExecutor {
    pub fn new(pool: Arc<RoleConnectionPool>) -> Self {
        Self { pool }
    }
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
        let client = self.pool.get_client(role).await;
        let cursor = client.query(sql).fetch_bytes("JSONEachRow").map_err(|e| {
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
        let cursor = client.query(sql).fetch_bytes(format).map_err(|e| {
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
