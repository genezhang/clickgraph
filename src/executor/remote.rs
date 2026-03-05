//! Remote ClickHouse executor backed by `RoleConnectionPool`.
//!
//! This is a thin wrapper that makes the existing connection pool implement
//! the `QueryExecutor` trait, preserving all existing behaviour (role-based
//! pools, cluster round-robin, etc.) with no functional change.

use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;
use tokio::io::AsyncBufReadExt;

use super::{ExecutorError, QueryExecutor};
use crate::server::connection_pool::RoleConnectionPool;

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

#[async_trait]
impl QueryExecutor for RemoteClickHouseExecutor {
    async fn execute_json(
        &self,
        sql: &str,
        role: Option<&str>,
    ) -> Result<Vec<Value>, ExecutorError> {
        let client = self.pool.get_client(role).await;
        let mut lines = client
            .query(sql)
            .fetch_bytes("JSONEachRow")
            .map_err(|e| {
                log::error!("ClickHouse query failed. SQL was:\n{}\nError: {}", sql, e);
                ExecutorError::QueryFailed(e.to_string())
            })?
            .lines();

        let mut rows = Vec::new();
        while let Some(line) = lines
            .next_line()
            .await
            .map_err(|e| ExecutorError::Io(e.to_string()))?
        {
            if line.trim().is_empty() {
                continue;
            }
            let value: Value = serde_json::from_str(&line).map_err(|e| {
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
        let mut lines = client
            .query(sql)
            .fetch_bytes(format)
            .map_err(|e| {
                log::error!("ClickHouse query failed. SQL was:\n{}\nError: {}", sql, e);
                ExecutorError::QueryFailed(e.to_string())
            })?
            .lines();

        let mut rows = Vec::new();
        while let Some(line) = lines
            .next_line()
            .await
            .map_err(|e| ExecutorError::Io(e.to_string()))?
        {
            rows.push(line);
        }
        Ok(rows.join("\n"))
    }
}
