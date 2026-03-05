//! `Database` — the top-level handle for an embedded ClickGraph database.
//!
//! Analogous to `kuzu::Database`. Holds the schema and the chdb executor.
//! Created once; multiple `Connection`s can be created from a single `Database`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use clickgraph::executor::chdb_embedded::ChdbExecutor;
pub use clickgraph::executor::chdb_embedded::StorageCredentials;
use clickgraph::executor::QueryExecutor;
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
    /// Reserved for future use — not yet wired into source resolution.
    pub data_dir: Option<PathBuf>,

    /// Maximum number of threads for chdb query execution.
    /// `None` uses the chdb default (typically number of CPU cores).
    /// Reserved for future use — not yet passed to chdb session.
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
}

impl Database {
    /// Open a database using a YAML schema file.
    ///
    /// Loads the schema, creates a chdb session, and — if any schema entries have
    /// a `source:` field — creates the corresponding chdb VIEWs.
    ///
    /// # Arguments
    ///
    /// * `schema_path` — path to the YAML schema file
    /// * `config` — session configuration (session dir, data dir, threads)
    pub fn new(schema_path: impl AsRef<Path>, config: SystemConfig) -> Result<Self, EmbeddedError> {
        let schema_path = schema_path.as_ref();

        // Load YAML schema
        let yaml_content = std::fs::read_to_string(schema_path).map_err(|e| {
            EmbeddedError::Io(format!(
                "Cannot read schema '{}': {}",
                schema_path.display(),
                e
            ))
        })?;

        let schema_config: GraphSchemaConfig = serde_yaml::from_str(&yaml_content)
            .map_err(|e| EmbeddedError::Schema(format!("YAML parse error: {}", e)))?;

        // Build GraphSchema without ClickHouse auto-discovery (sync/no-client mode)
        let graph_schema = schema_config
            .to_graph_schema()
            .map_err(|e| EmbeddedError::Schema(format!("Schema build error: {}", e)))?;

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

        Ok(Database {
            executor: Arc::new(executor),
            schema,
        })
    }

    /// Return a reference to the graph schema.
    pub fn schema(&self) -> &Arc<GraphSchema> {
        &self.schema
    }

    /// Create a `Database` from a pre-built schema and executor.
    ///
    /// Primarily intended for testing — allows injection of a custom executor
    /// (e.g. a stub) without needing a chdb session.
    pub fn from_executor(schema: Arc<GraphSchema>, executor: Arc<dyn QueryExecutor>) -> Self {
        Database { executor, schema }
    }
}

fn pseudo_random_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| format!("{}", d.subsec_nanos()))
        .unwrap_or_else(|_| "0".to_string())
}
