//! `Database` — the top-level handle for an embedded ClickGraph database.
//!
//! Analogous to `kuzu::Database`. Holds the schema and the chdb executor.
//! Created once; multiple `Connection`s can be created from a single `Database`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use clickgraph::executor::chdb_embedded::ChdbExecutor;
pub use clickgraph::executor::chdb_embedded::StorageCredentials;
use clickgraph::executor::{ExecutorError, QueryExecutor};
use clickgraph::graph_catalog::config::GraphSchemaConfig;
use clickgraph::graph_catalog::graph_schema::GraphSchema;

use super::error::EmbeddedError;

/// Configuration for an embedded database session.
///
/// Mirrors `kuzu::SystemConfig`.
#[derive(Debug, Clone, Default)]
pub struct SystemConfig {
    /// Directory where chdb stores its session data.
    /// Defaults to a temporary directory (auto-cleaned on drop).
    pub session_dir: Option<PathBuf>,

    /// Base directory for resolving relative `source:` paths in the schema.
    /// If `None`, relative paths are resolved from the current working directory.
    /// Reserved for future use -- not yet wired into source resolution.
    pub data_dir: Option<PathBuf>,

    /// Maximum number of threads for chdb query execution.
    /// `None` uses the chdb default (typically number of CPU cores).
    /// Reserved for future use -- not yet passed to chdb session.
    pub max_threads: Option<usize>,

    /// Storage credentials for remote sources (S3, GCS, Azure Blob, Iceberg).
    ///
    /// Applied as ClickHouse session-level `SET` commands before any VIEWs are
    /// created, so they apply automatically to every `s3()` / `iceberg()` /
    /// `deltaLake()` call inside the session.
    ///
    /// If all fields are `None` (the default), chdb falls back to environment
    /// variables (`AWS_ACCESS_KEY_ID`, etc.) or instance-profile credentials.
    pub credentials: StorageCredentials,
}

/// An embedded ClickGraph database.
///
/// # Example
///
/// ```no_run
/// use clickgraph_embedded::{Database, SystemConfig};
///
/// let db = Database::new("schema.yaml", SystemConfig::default()).unwrap();
/// ```
pub struct Database {
    pub(crate) executor: Arc<dyn QueryExecutor>,
    pub(crate) schema: Arc<GraphSchema>,
    /// Shared Tokio runtime for blocking `Connection::query()` calls.
    /// Created once, reused by all connections -- avoids per-call overhead.
    pub(crate) runtime: tokio::runtime::Runtime,
}

impl Database {
    /// Open a database using a YAML schema file.
    ///
    /// Loads the schema, creates a chdb session, and:
    /// - Creates VIEWs for schema entries WITH a `source:` field
    /// - Creates writable ReplacingMergeTree tables for entries WITHOUT `source:`
    ///
    /// # Arguments
    ///
    /// * `schema_path` -- path to the YAML schema file
    /// * `config` -- session configuration (session dir, data dir, threads)
    pub fn new(schema_path: impl AsRef<Path>, config: SystemConfig) -> Result<Self, EmbeddedError> {
        let graph_schema = load_graph_schema(schema_path.as_ref())?;
        Self::from_schema(Arc::new(graph_schema), config)
    }

    /// Open a database with an already-built `GraphSchema`.
    ///
    /// Useful when you have already loaded and validated a schema.
    pub fn from_schema(
        schema: Arc<GraphSchema>,
        config: SystemConfig,
    ) -> Result<Self, EmbeddedError> {
        // Determine chdb session directory
        let (session_dir, auto_cleanup) = match config.session_dir {
            Some(dir) => (dir, false),
            None => {
                let tmp =
                    std::env::temp_dir().join(format!("clickgraph-{}", pseudo_random_suffix()));
                (tmp, true)
            }
        };

        // Create chdb executor, applying credentials at session init
        let executor =
            ChdbExecutor::new_with_credentials(&session_dir, auto_cleanup, &config.credentials)
                .map_err(|e| EmbeddedError::Executor(e.to_string()))?;

        // Create chdb VIEWs for schema entries with source: URIs
        let view_count = clickgraph::executor::data_loader::load_schema_sources(&executor, &schema)
            .map_err(|e| EmbeddedError::Executor(e.to_string()))?;

        if view_count > 0 {
            log::info!(
                "Created {} chdb VIEW(s) from schema source: entries",
                view_count
            );
        }

        // Create writable ReplacingMergeTree tables for entries without source:
        let table_count =
            clickgraph::executor::data_loader::create_writable_tables(&executor, &schema)
                .map_err(|e| EmbeddedError::Executor(e.to_string()))?;

        if table_count > 0 {
            log::info!(
                "Created {} writable ReplacingMergeTree table(s)",
                table_count
            );
        }

        Ok(Database {
            executor: Arc::new(executor),
            schema,
            runtime: build_runtime()?,
        })
    }

    /// Return a reference to the graph schema.
    pub fn schema(&self) -> &Arc<GraphSchema> {
        &self.schema
    }

    /// Create a `Database` from a pre-built schema and executor.
    ///
    /// Primarily intended for testing -- allows injection of a custom executor
    /// (e.g. a stub) without needing a chdb session.
    pub fn from_executor(
        schema: Arc<GraphSchema>,
        executor: Arc<dyn QueryExecutor>,
    ) -> Result<Self, EmbeddedError> {
        Ok(Database {
            executor,
            schema,
            runtime: build_runtime()?,
        })
    }

    /// Open a database in SQL-only mode -- schema loaded, no chdb session.
    ///
    /// This mode supports `query_to_sql()` and `export_to_sql()` for
    /// Cypher -> SQL translation without requiring the chdb native library.
    /// Calling `query()` or `export()` will return an error.
    ///
    /// Useful for testing, debugging, and build-time SQL validation.
    pub fn sql_only(schema_path: impl AsRef<Path>) -> Result<Self, EmbeddedError> {
        let graph_schema = load_graph_schema(schema_path.as_ref())?;
        Self::from_executor(Arc::new(graph_schema), Arc::new(NullExecutor))
    }
}

/// A no-op executor for SQL-only mode. Returns an error if execution is attempted.
struct NullExecutor;

#[async_trait]
impl QueryExecutor for NullExecutor {
    async fn execute_json(
        &self,
        _sql: &str,
        _role: Option<&str>,
    ) -> Result<Vec<serde_json::Value>, ExecutorError> {
        Err(ExecutorError::QueryFailed(
            "Cannot execute queries in sql_only mode -- no backend is configured".to_string(),
        ))
    }

    async fn execute_text(
        &self,
        _sql: &str,
        _format: &str,
        _role: Option<&str>,
    ) -> Result<String, ExecutorError> {
        Err(ExecutorError::QueryFailed(
            "Cannot execute queries in sql_only mode -- no backend is configured".to_string(),
        ))
    }
}

/// Load and parse a YAML schema file into a `GraphSchema`.
fn load_graph_schema(schema_path: &Path) -> Result<GraphSchema, EmbeddedError> {
    let yaml_content = std::fs::read_to_string(schema_path).map_err(|e| {
        EmbeddedError::Io(format!(
            "Cannot read schema '{}': {}",
            schema_path.display(),
            e
        ))
    })?;

    let schema_config: GraphSchemaConfig = serde_yaml::from_str(&yaml_content)
        .map_err(|e| EmbeddedError::Schema(format!("YAML parse error: {}", e)))?;

    schema_config
        .to_graph_schema()
        .map_err(|e| EmbeddedError::Schema(format!("Schema build error: {}", e)))
}

/// Build a single-threaded Tokio runtime for blocking `Connection` calls.
fn build_runtime() -> Result<tokio::runtime::Runtime, EmbeddedError> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| EmbeddedError::Query(format!("Failed to create runtime: {}", e)))
}

fn pseudo_random_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| format!("{}", d.subsec_nanos()))
        .unwrap_or_else(|_| "0".to_string())
}
