//! Embedded chdb backend for `QueryExecutor`.
//!
//! `ChdbExecutor` runs SQL queries in-process via the chdb ClickHouse-compatible
//! embedded engine. No external server is needed. Uses a persistent session so
//! that `CREATE VIEW` statements issued by the data loader survive across queries.
//!
//! chdb is synchronous (C FFI), so every call is wrapped in
//! `tokio::task::spawn_blocking` to avoid blocking the async runtime.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chdb_rust::arg::Arg;
use chdb_rust::format::OutputFormat;
use chdb_rust::session::{Session, SessionBuilder};
use serde_json::Value;

use super::{ExecutorError, QueryExecutor};

/// Storage credentials for remote data sources (S3, GCS, Azure Blob).
///
/// These are applied as ClickHouse session-level `SET` commands at startup,
/// so they apply automatically to all `s3()`, `iceberg()`, and `deltaLake()`
/// table function calls made via chdb.
///
/// # Credential resolution priority
///
/// chdb/ClickHouse resolves credentials in this order:
/// 1. Values set here (highest priority)
/// 2. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, …)
/// 3. Instance profile / pod identity (AWS IMDSv2, GKE Workload Identity, …)
///
/// # Example
///
/// ```ignore
/// // In the clickgraph-embedded crate:
/// use clickgraph_embedded::{SystemConfig, StorageCredentials};
///
/// let config = SystemConfig {
///     credentials: StorageCredentials {
///         s3_access_key_id: Some("AKIAIOSFODNN7EXAMPLE".to_string()),
///         s3_secret_access_key: Some(std::env::var("AWS_SECRET").unwrap()),
///         s3_region: Some("us-east-1".to_string()),
///         ..Default::default()
///     },
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Clone, Default)]
pub struct StorageCredentials {
    // ── S3 / S3-compatible (MinIO, Ceph, …) ─────────────────────────────────
    /// AWS access key ID (maps to `s3_access_key_id`).
    pub s3_access_key_id: Option<String>,
    /// AWS secret access key (maps to `s3_secret_access_key`).
    pub s3_secret_access_key: Option<String>,
    /// AWS session token for temporary credentials (maps to `s3_session_token`).
    pub s3_session_token: Option<String>,
    /// AWS region, e.g. `"us-east-1"` (maps to `s3_region`).
    pub s3_region: Option<String>,
    /// Custom S3-compatible endpoint URL, e.g. `"http://minio:9000"`.
    /// Maps to `s3_endpoint_url` / `s3_endpoint_override`.
    pub s3_endpoint_url: Option<String>,

    // ── Google Cloud Storage (GCS) ────────────────────────────────────────────
    /// GCS HMAC access key (maps to `gcs_access_key_id`).
    pub gcs_access_key_id: Option<String>,
    /// GCS HMAC secret (maps to `gcs_secret_access_key`).
    pub gcs_secret_access_key: Option<String>,

    // ── Azure Blob Storage ────────────────────────────────────────────────────
    /// Azure storage account name (maps to `azure_storage_account_name`).
    pub azure_storage_account_name: Option<String>,
    /// Azure storage account key (maps to `azure_storage_account_key`).
    pub azure_storage_account_key: Option<String>,
    /// Azure storage connection string (maps to `azure_storage_connection_string`).
    pub azure_storage_connection_string: Option<String>,
}

impl StorageCredentials {
    /// Build the list of `SET key = 'value'` SQL statements for this credential set.
    ///
    /// Only entries that are `Some` are emitted.
    pub fn to_set_statements(&self) -> Vec<String> {
        use super::source_resolver::escape_sql_string;

        let mut stmts = Vec::new();
        macro_rules! set_if_some {
            ($field:expr, $ch_key:expr) => {
                if let Some(ref v) = $field {
                    stmts.push(format!("SET {} = '{}'", $ch_key, escape_sql_string(v)));
                }
            };
        }
        set_if_some!(self.s3_access_key_id, "s3_access_key_id");
        set_if_some!(self.s3_secret_access_key, "s3_secret_access_key");
        set_if_some!(self.s3_session_token, "s3_session_token");
        set_if_some!(self.s3_region, "s3_region");
        set_if_some!(self.s3_endpoint_url, "s3_endpoint_url");
        set_if_some!(self.gcs_access_key_id, "gcs_access_key_id");
        set_if_some!(self.gcs_secret_access_key, "gcs_secret_access_key");
        set_if_some!(
            self.azure_storage_account_name,
            "azure_storage_account_name"
        );
        set_if_some!(self.azure_storage_account_key, "azure_storage_account_key");
        set_if_some!(
            self.azure_storage_connection_string,
            "azure_storage_connection_string"
        );
        stmts
    }

    /// Return `true` if no credentials are set (all fields are `None`).
    pub fn is_empty(&self) -> bool {
        self.s3_access_key_id.is_none()
            && self.s3_secret_access_key.is_none()
            && self.s3_session_token.is_none()
            && self.s3_region.is_none()
            && self.s3_endpoint_url.is_none()
            && self.gcs_access_key_id.is_none()
            && self.gcs_secret_access_key.is_none()
            && self.azure_storage_account_name.is_none()
            && self.azure_storage_account_key.is_none()
            && self.azure_storage_connection_string.is_none()
    }
}

/// In-process chdb executor.
///
/// Holds a single chdb `Session` behind a `Mutex` so concurrent async callers
/// each get exclusive access while the blocking FFI call runs.
pub struct ChdbExecutor {
    session: Arc<Mutex<Session>>,
}

impl ChdbExecutor {
    /// Create a new `ChdbExecutor` with a persistent on-disk session.
    ///
    /// `data_path` is the directory where chdb stores its database files.
    /// Set `auto_cleanup = true` for temporary sessions (e.g., in tests).
    pub fn new(data_path: impl AsRef<Path>, auto_cleanup: bool) -> Result<Self, ExecutorError> {
        Self::new_with_credentials(data_path, auto_cleanup, &StorageCredentials::default())
    }

    /// Create a new `ChdbExecutor` with explicit storage credentials.
    ///
    /// Credentials are applied as `SET` commands immediately after session
    /// creation, before any queries or VIEW creation.
    pub fn new_with_credentials(
        data_path: impl AsRef<Path>,
        auto_cleanup: bool,
        credentials: &StorageCredentials,
    ) -> Result<Self, ExecutorError> {
        let path: PathBuf = data_path.as_ref().to_path_buf();
        let session = SessionBuilder::new()
            .with_data_path(path)
            .with_auto_cleanup(auto_cleanup)
            .build()
            .map_err(|e| ExecutorError::QueryFailed(format!("chdb session init failed: {}", e)))?;

        // Apply ClickHouse settings required by ClickGraph's SQL.
        // `join_use_nulls = 1` makes LEFT JOINs return NULL for missing rows
        // (required by OPTIONAL MATCH semantics).
        session
            .execute("SET join_use_nulls = 1", None)
            .map_err(|e| ExecutorError::QueryFailed(format!("chdb SET failed: {}", e)))?;

        // Apply storage credentials as session-level settings.
        for stmt in credentials.to_set_statements() {
            session.execute(&stmt, None).map_err(|e| {
                ExecutorError::QueryFailed(format!("chdb SET credential failed: {}", e))
            })?;
        }

        Ok(Self {
            session: Arc::new(Mutex::new(session)),
        })
    }

    /// Create a temporary in-memory-style session (auto-cleaned on drop).
    pub fn new_ephemeral() -> Result<Self, ExecutorError> {
        let tmp = std::env::temp_dir().join(format!("clickgraph-chdb-{}", uuid_short()));
        Self::new(tmp, true)
    }

    /// Execute SQL inside the session and return the result bytes.
    ///
    /// Called from the blocking task inside `spawn_blocking`.
    fn execute_blocking(
        session: &Mutex<Session>,
        sql: &str,
        format: OutputFormat,
    ) -> Result<String, ExecutorError> {
        let guard = session.lock().map_err(|e| {
            ExecutorError::QueryFailed(format!("chdb session lock poisoned: {}", e))
        })?;

        let result = guard
            .execute(sql, Some(&[Arg::OutputFormat(format)]))
            .map_err(|e| ExecutorError::QueryFailed(format!("chdb query failed: {}", e)))?;

        Ok(result.data_utf8_lossy().to_string())
    }
    /// Execute a DDL statement synchronously (for use during startup, not in async context).
    ///
    /// This is intentionally synchronous — it is called before the async runtime is
    /// serving requests, during schema initialization.
    pub fn execute_blocking_ddl(&self, sql: &str) -> Result<(), ExecutorError> {
        Self::execute_blocking(&self.session, sql, OutputFormat::TabSeparated)?;
        Ok(())
    }
}

#[async_trait]
impl QueryExecutor for ChdbExecutor {
    async fn execute_json(
        &self,
        sql: &str,
        _role: Option<&str>, // roles not supported in embedded mode
    ) -> Result<Vec<Value>, ExecutorError> {
        let sql = sql.to_owned();
        let session = Arc::clone(&self.session);

        let text = tokio::task::spawn_blocking(move || {
            Self::execute_blocking(&session, &sql, OutputFormat::JSONEachRow)
        })
        .await
        .map_err(|e| ExecutorError::QueryFailed(format!("spawn_blocking panicked: {}", e)))??;

        // Parse JSONEachRow: one JSON object per line
        let mut rows = Vec::new();
        for line in text.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let v: Value = serde_json::from_str(trimmed).map_err(|e| {
                ExecutorError::Parse(format!("invalid JSON row '{}': {}", trimmed, e))
            })?;
            rows.push(v);
        }

        Ok(rows)
    }

    async fn execute_text(
        &self,
        sql: &str,
        format: &str,
        _role: Option<&str>,
    ) -> Result<String, ExecutorError> {
        let sql = sql.to_owned();
        let fmt = parse_output_format(format)?;
        let session = Arc::clone(&self.session);

        let text = tokio::task::spawn_blocking(move || Self::execute_blocking(&session, &sql, fmt))
            .await
            .map_err(|e| ExecutorError::QueryFailed(format!("spawn_blocking panicked: {}", e)))??;

        Ok(text)
    }
}

/// Map a ClickHouse format name string to `chdb_rust::format::OutputFormat`.
fn parse_output_format(format: &str) -> Result<OutputFormat, ExecutorError> {
    match format {
        "Pretty" => Ok(OutputFormat::Pretty),
        "PrettyCompact" => Ok(OutputFormat::PrettyCompact),
        "CSV" => Ok(OutputFormat::CSV),
        "CSVWithNames" => Ok(OutputFormat::CSVWithNames),
        "TSV" | "TabSeparated" => Ok(OutputFormat::TabSeparated),
        "JSONEachRow" => Ok(OutputFormat::JSONEachRow),
        "JSON" => Ok(OutputFormat::JSON),
        other => Err(ExecutorError::QueryFailed(format!(
            "unsupported output format '{}' for embedded mode",
            other
        ))),
    }
}

/// Generate a short random suffix for temp directory names.
fn uuid_short() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    format!("{}", nanos)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_credentials_empty_by_default() {
        let creds = StorageCredentials::default();
        assert!(creds.is_empty());
        assert!(creds.to_set_statements().is_empty());
    }

    #[test]
    fn test_credentials_s3_basic() {
        let creds = StorageCredentials {
            s3_access_key_id: Some("AKIAIOSFODNN7EXAMPLE".to_string()),
            s3_secret_access_key: Some("wJalrXUtnFEMI/K7MDENG".to_string()),
            ..Default::default()
        };
        assert!(!creds.is_empty());
        let stmts = creds.to_set_statements();
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("s3_access_key_id"));
        assert!(stmts[0].contains("AKIAIOSFODNN7EXAMPLE"));
        assert!(stmts[1].contains("s3_secret_access_key"));
    }

    #[test]
    fn test_credentials_s3_with_region_and_session_token() {
        let creds = StorageCredentials {
            s3_access_key_id: Some("KEY".to_string()),
            s3_secret_access_key: Some("SECRET".to_string()),
            s3_session_token: Some("TOKEN".to_string()),
            s3_region: Some("eu-west-1".to_string()),
            ..Default::default()
        };
        let stmts = creds.to_set_statements();
        assert_eq!(stmts.len(), 4);
        let joined = stmts.join("\n");
        assert!(joined.contains("s3_session_token"));
        assert!(joined.contains("s3_region"));
        assert!(joined.contains("eu-west-1"));
    }

    #[test]
    fn test_credentials_gcs() {
        let creds = StorageCredentials {
            gcs_access_key_id: Some("gcs-key".to_string()),
            gcs_secret_access_key: Some("gcs-secret".to_string()),
            ..Default::default()
        };
        let stmts = creds.to_set_statements();
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("gcs_access_key_id"));
        assert!(stmts[1].contains("gcs_secret_access_key"));
    }

    #[test]
    fn test_credentials_azure() {
        let creds = StorageCredentials {
            azure_storage_account_name: Some("myaccount".to_string()),
            azure_storage_account_key: Some("mykey".to_string()),
            ..Default::default()
        };
        let stmts = creds.to_set_statements();
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("azure_storage_account_name"));
    }

    #[test]
    fn test_credentials_s3_endpoint_url() {
        let creds = StorageCredentials {
            s3_endpoint_url: Some("http://minio:9000".to_string()),
            ..Default::default()
        };
        let stmts = creds.to_set_statements();
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].contains("s3_endpoint_url"));
        assert!(stmts[0].contains("http://minio:9000"));
    }

    #[test]
    fn test_credentials_single_quote_escaping() {
        // Single quotes in values must be escaped so the SET statement is valid SQL.
        let creds = StorageCredentials {
            s3_secret_access_key: Some("it's/a+weird=secret".to_string()),
            ..Default::default()
        };
        let stmts = creds.to_set_statements();
        assert_eq!(stmts.len(), 1);
        // The single quote in "it's" must be escaped
        assert!(stmts[0].contains("\\'"), "single quote should be escaped");
    }

    #[test]
    fn test_credentials_backslash_escaping() {
        // Backslashes in secret values must be escaped to avoid SQL interpretation issues.
        let creds = StorageCredentials {
            s3_secret_access_key: Some("secret\\with\\backslash".to_string()),
            ..Default::default()
        };
        let stmts = creds.to_set_statements();
        assert_eq!(stmts.len(), 1);
        assert!(
            stmts[0].contains("\\\\"),
            "backslash should be escaped: {}",
            stmts[0]
        );
    }

    #[test]
    fn test_credentials_all_none_is_empty() {
        let creds = StorageCredentials {
            s3_access_key_id: None,
            s3_secret_access_key: None,
            s3_session_token: None,
            s3_region: None,
            s3_endpoint_url: None,
            gcs_access_key_id: None,
            gcs_secret_access_key: None,
            azure_storage_account_name: None,
            azure_storage_account_key: None,
            azure_storage_connection_string: None,
        };
        assert!(creds.is_empty());
    }
}
