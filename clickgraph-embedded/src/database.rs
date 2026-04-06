//! `Database` — the top-level handle for an embedded ClickGraph database.
//!
//! Analogous to `kuzu::Database`. Holds the schema and the chdb executor.
//! Created once; multiple `Connection`s can be created from a single `Database`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
#[cfg(feature = "embedded")]
use clickgraph::executor::chdb_embedded::ChdbExecutor;
#[cfg(feature = "embedded")]
pub use clickgraph::executor::chdb_embedded::StorageCredentials;
use clickgraph::executor::remote::RemoteClickHouseExecutor;
use clickgraph::executor::{ExecutorError, QueryExecutor};
use clickgraph::graph_catalog::config::GraphSchemaConfig;
use clickgraph::graph_catalog::graph_schema::GraphSchema;
use clickgraph::server::connection_pool::RoleConnectionPool;

use super::error::EmbeddedError;

/// Default maximum CTE recursion depth for remote ClickHouse queries.
const DEFAULT_REMOTE_MAX_CTE_DEPTH: u32 = 100;

/// Configuration for connecting to a remote ClickHouse cluster.
///
/// When provided in `SystemConfig`, enables `Connection::query_remote()` and
/// `Connection::query_remote_graph()` to execute Cypher queries against a
/// remote ClickHouse instance while storing results locally via chdb.
#[derive(Clone)]
pub struct RemoteConfig {
    /// ClickHouse HTTP endpoint URL (e.g., `"http://ch-cluster:8123"`).
    pub url: String,
    /// ClickHouse username.
    pub user: String,
    /// ClickHouse password.
    pub password: String,
    /// Database name. Defaults to `"default"` if `None`.
    pub database: Option<String>,
    /// Cluster name for multi-node round-robin. If `None`, single-node mode.
    pub cluster_name: Option<String>,
}

impl std::fmt::Debug for RemoteConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteConfig")
            .field("url", &self.url)
            .field("user", &self.user)
            .field("password", &"********")
            .field("database", &self.database)
            .field("cluster_name", &self.cluster_name)
            .finish()
    }
}

/// Configuration for an embedded database session.
///
/// Mirrors `kuzu::SystemConfig`.
///
/// All fields are `Option` so that callers can safely use `..SystemConfig::default()`
/// to forward-compatibly add new fields without breaking struct literals.
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
    pub max_threads: Option<usize>,

    /// Maximum memory a single query may use, in bytes.
    /// `None` uses the chdb/ClickHouse default (no cap).
    /// Set this in test environments to prevent runaway memory usage.
    pub max_memory_usage_bytes: Option<u64>,

    /// Storage credentials for remote sources (S3, GCS, Azure Blob, Iceberg).
    ///
    /// Applied as ClickHouse session-level `SET` commands before any VIEWs are
    /// created, so they apply automatically to every `s3()` / `iceberg()` /
    /// `deltaLake()` call inside the session.
    ///
    /// If all fields are `None` (the default), chdb falls back to environment
    /// variables (`AWS_ACCESS_KEY_ID`, etc.) or instance-profile credentials.
    #[cfg(feature = "embedded")]
    pub credentials: StorageCredentials,

    /// Optional remote ClickHouse connection for hybrid query + local storage.
    ///
    /// When set, `Connection::query_remote()` and `query_remote_graph()` execute
    /// Cypher queries on the remote cluster. Results can then be stored locally
    /// via `store_subgraph()` for fast re-querying.
    pub remote: Option<RemoteConfig>,
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
    /// Optional remote ClickHouse executor for hybrid query + local storage.
    pub(crate) remote_executor: Option<Arc<dyn QueryExecutor>>,
    pub(crate) schema: Arc<GraphSchema>,
    /// Shared Tokio runtime for blocking `Connection::query()` calls.
    /// Created once, reused by all connections -- avoids per-call overhead.
    pub(crate) runtime: tokio::runtime::Runtime,
}

impl Database {
    /// Open a database using a YAML schema file (requires `embedded` feature for chdb).
    ///
    /// Loads the schema, creates a chdb session, and:
    /// - Creates VIEWs for schema entries WITH a `source:` field
    /// - Creates writable ReplacingMergeTree tables for entries WITHOUT `source:`
    #[cfg(feature = "embedded")]
    pub fn new(schema_path: impl AsRef<Path>, config: SystemConfig) -> Result<Self, EmbeddedError> {
        let graph_schema = load_graph_schema(schema_path.as_ref())?;
        Self::from_schema(Arc::new(graph_schema), config)
    }

    /// Open an in-memory database using a YAML schema file (requires `embedded` feature).
    ///
    /// Equivalent to `new()` with a temporary session directory that is
    /// automatically cleaned up when the `Database` is dropped.
    #[cfg(feature = "embedded")]
    pub fn in_memory(
        schema_path: impl AsRef<Path>,
        config: SystemConfig,
    ) -> Result<Self, EmbeddedError> {
        let config = SystemConfig {
            session_dir: None,
            ..config
        };
        Self::new(schema_path, config)
    }

    /// Open a database backed by an existing `GraphSchema` and a chdb session
    /// (requires `embedded` feature).
    #[cfg(feature = "embedded")]
    pub fn from_schema(
        schema: Arc<GraphSchema>,
        config: SystemConfig,
    ) -> Result<Self, EmbeddedError> {
        let runtime = build_runtime()?;

        let (session_dir, auto_cleanup) = match config.session_dir {
            Some(dir) => (dir, false),
            None => {
                let tmp =
                    std::env::temp_dir().join(format!("clickgraph-{}", pseudo_random_suffix()));
                (tmp, true)
            }
        };

        let executor =
            ChdbExecutor::new_with_credentials(&session_dir, auto_cleanup, &config.credentials)
                .map_err(|e| EmbeddedError::Executor(e.to_string()))?;

        if let Some(threads) = config.max_threads {
            executor
                .execute_blocking_ddl(&format!("SET max_threads = {threads}"))
                .map_err(|e| EmbeddedError::Executor(e.to_string()))?;
        }
        if let Some(bytes) = config.max_memory_usage_bytes {
            executor
                .execute_blocking_ddl(&format!("SET max_memory_usage = {bytes}"))
                .map_err(|e| EmbeddedError::Executor(e.to_string()))?;
        }

        let view_count = clickgraph::executor::data_loader::load_schema_sources(&executor, &schema)
            .map_err(|e| EmbeddedError::Executor(e.to_string()))?;
        if view_count > 0 {
            log::info!(
                "Created {} chdb VIEW(s) from schema source: entries",
                view_count
            );
        }

        let table_count =
            clickgraph::executor::data_loader::create_writable_tables(&executor, &schema)
                .map_err(|e| EmbeddedError::Executor(e.to_string()))?;
        if table_count > 0 {
            log::info!(
                "Created {} writable ReplacingMergeTree table(s)",
                table_count
            );
        }

        let remote_executor = Self::build_remote_executor(&runtime, config.remote.as_ref())?;

        Ok(Database {
            executor: Arc::new(executor),
            remote_executor,
            schema,
            runtime,
        })
    }

    /// Open a database connected to a remote ClickHouse cluster (no chdb needed).
    ///
    /// Cypher is translated to SQL locally and executed on the remote ClickHouse.
    /// Use `Connection::query_remote()` to run queries.
    pub fn new_remote(
        schema_path: impl AsRef<Path>,
        remote: RemoteConfig,
    ) -> Result<Self, EmbeddedError> {
        let graph_schema = load_graph_schema(schema_path.as_ref())?;
        let runtime = build_runtime()?;
        let remote_executor =
            Self::build_remote_executor(&runtime, Some(&remote))?.ok_or_else(|| {
                EmbeddedError::Executor("Failed to connect to remote ClickHouse".to_string())
            })?;
        Ok(Database {
            executor: Arc::new(NullExecutor),
            remote_executor: Some(remote_executor),
            schema: Arc::new(graph_schema),
            runtime,
        })
    }

    /// Build a remote executor from an optional `RemoteConfig`.
    fn build_remote_executor(
        runtime: &tokio::runtime::Runtime,
        remote: Option<&RemoteConfig>,
    ) -> Result<Option<Arc<dyn QueryExecutor>>, EmbeddedError> {
        let Some(remote) = remote else {
            return Ok(None);
        };
        let pool = runtime
            .block_on(RoleConnectionPool::new_with_params(
                &remote.url,
                &remote.user,
                &remote.password,
                remote.database.as_deref(),
                remote.cluster_name.as_deref(),
                DEFAULT_REMOTE_MAX_CTE_DEPTH,
            ))
            .map_err(EmbeddedError::Executor)?;
        log::info!("Remote ClickHouse executor initialized: {}", remote.url);
        Ok(Some(
            Arc::new(RemoteClickHouseExecutor::new(Arc::new(pool))) as Arc<dyn QueryExecutor>,
        ))
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
            remote_executor: None,
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
