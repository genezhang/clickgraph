//! Backend-agnostic SQL execution layer.
//!
//! The `QueryExecutor` trait abstracts over different SQL execution backends:
//! - [`remote::RemoteClickHouseExecutor`] — wraps the existing `RoleConnectionPool`
//!   for remote ClickHouse server connections.
//! - [`chdb_embedded::ChdbExecutor`] — in-process chdb for embedded deployments
//!   (requires the `embedded` feature).

use async_trait::async_trait;
use serde_json::Value;

pub mod errors;
pub mod remote;
pub use errors::ExecutorError;

#[cfg(feature = "embedded")]
pub mod chdb_embedded;
#[cfg(feature = "embedded")]
pub mod data_loader;
pub mod source_resolver;

/// Backend-agnostic SQL execution interface.
///
/// Implemented by different backends (remote ClickHouse, embedded chdb, etc.).
/// All query-processing code in `handlers.rs` and `bolt_protocol/` should use
/// this trait rather than calling `clickhouse::Client` directly.
#[async_trait]
pub trait QueryExecutor: Send + Sync {
    /// Execute SQL and return parsed JSON rows (one `Value` per result row).
    ///
    /// Uses `JSONEachRow` output format internally.
    /// `role` is an optional RBAC role; ignored by backends that don't support it.
    async fn execute_json(
        &self,
        sql: &str,
        role: Option<&str>,
    ) -> Result<Vec<Value>, ExecutorError>;

    /// Execute SQL and return output as a plain-text string.
    ///
    /// `format` is the ClickHouse output format name, e.g. `"Pretty"`, `"CSV"`,
    /// `"CSVWithNames"`, `"PrettyCompact"`.
    async fn execute_text(
        &self,
        sql: &str,
        format: &str,
        role: Option<&str>,
    ) -> Result<String, ExecutorError>;
}
