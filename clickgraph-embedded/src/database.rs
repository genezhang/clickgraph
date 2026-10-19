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
#[cfg(feature = "databricks")]
pub use clickgraph::executor::databricks_sql::{
    DatabricksConfig, DatabricksSqlExecutor, OAuthM2MConfig,
};
use clickgraph::executor::remote::RemoteClickHouseExecutor;
use clickgraph::executor::source_resolver::resolve_source_uri;
use clickgraph::executor::{ExecutorError, QueryExecutor};
use clickgraph::graph_catalog::column_info::{
    column_info_from_rows, columns_by_table_from_rows, describe_columns_sql,
    system_columns_of_tables_sql, system_columns_sql, DiscoveredColumns,
};
use clickgraph::graph_catalog::config::GraphSchemaConfig;
use clickgraph::graph_catalog::graph_schema::GraphSchema;
use clickgraph::server::connection_pool::RoleConnectionPool;
use clickgraph::sql_generator::SqlDialect;

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

/// The execution backend a database runs its SQL on. Cypher writes run on
/// the embedded chdb backend only (Decision 0.1 of the embedded-writes
/// design).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExecutorKind {
    /// Embedded chdb (`Database::new(...)` with the `embedded` feature).
    /// Writes are permitted.
    EmbeddedChdb,
    /// Translation-only mode (`Database::sql_only(...)`). No execution.
    SqlOnly,
    /// External ClickHouse via HTTP (`Database::new_remote(...)`). Writes rejected.
    Remote,
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
    /// What kind of backend the primary executor is: Cypher writes run on
    /// the embedded chdb path only.
    pub(crate) executor_kind: ExecutorKind,
    /// SQL dialect this database emits. `Connection::query_*` paths
    /// stamp this onto the per-query `QueryContext` so that the shared
    /// renderer routes through the right `FunctionMapper`. Default
    /// `ClickHouse`; `new_databricks()` sets it to `Databricks`.
    pub(crate) dialect: SqlDialect,
}

impl Database {
    /// Open a database using a YAML schema file (requires `embedded` feature for chdb).
    ///
    /// Loads the schema, creates a chdb session, and:
    /// - Creates VIEWs for schema entries WITH a `source:` field
    /// - Creates writable ReplacingMergeTree tables for entries WITHOUT `source:`
    #[cfg(feature = "embedded")]
    pub fn new(schema_path: impl AsRef<Path>, config: SystemConfig) -> Result<Self, EmbeddedError> {
        let schema_config = read_schema_config(schema_path.as_ref())?;
        let (runtime, executor) = open_chdb_session(&config)?;
        let remote_executor = Self::build_remote_executor(&runtime, config.remote.as_ref())?;
        // `source:` entries become views over their data only once the schema
        // is built, so their columns are read from the source itself. Entries
        // without one are tables chdb creates from the schema, so only a
        // remote ClickHouse (hybrid mode) can hold their columns.
        let readers = ColumnReaders {
            sources: Some(&executor),
            tables: remote_executor.as_deref(),
        };
        let graph_schema = build_graph_schema(&schema_config, readers, &runtime)?;
        Self::finish_chdb(Arc::new(graph_schema), runtime, executor, remote_executor)
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
        let (runtime, executor) = open_chdb_session(&config)?;
        let remote_executor = Self::build_remote_executor(&runtime, config.remote.as_ref())?;
        Self::finish_chdb(schema, runtime, executor, remote_executor)
    }

    /// Create the schema's views and writable tables in a fresh chdb session.
    #[cfg(feature = "embedded")]
    fn finish_chdb(
        schema: Arc<GraphSchema>,
        runtime: tokio::runtime::Runtime,
        executor: ChdbExecutor,
        remote_executor: Option<Arc<dyn QueryExecutor>>,
    ) -> Result<Self, EmbeddedError> {
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

        Ok(Database {
            executor: Arc::new(executor),
            remote_executor,
            schema,
            runtime,
            executor_kind: ExecutorKind::EmbeddedChdb,
            dialect: SqlDialect::ClickHouse,
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
        let schema_config = read_schema_config(schema_path.as_ref())?;
        let runtime = build_runtime()?;
        let remote_executor =
            Self::build_remote_executor(&runtime, Some(&remote))?.ok_or_else(|| {
                EmbeddedError::Executor("Failed to connect to remote ClickHouse".to_string())
            })?;
        let readers = ColumnReaders {
            sources: None,
            tables: Some(remote_executor.as_ref()),
        };
        let graph_schema = build_graph_schema(&schema_config, readers, &runtime)?;
        Ok(Database {
            executor: Arc::new(NullExecutor),
            remote_executor: Some(remote_executor),
            schema: Arc::new(graph_schema),
            runtime,
            executor_kind: ExecutorKind::Remote,
            dialect: SqlDialect::ClickHouse,
        })
    }

    /// Open a database connected to a Databricks SQL Warehouse.
    ///
    /// Cypher is translated to Spark SQL locally (via the dialect-aware
    /// renderer landed in Phase 1.x) and executed against the warehouse
    /// over the Statement Execution API. Use `Connection::query_remote()`
    /// to run queries — the same path used for the ClickHouse remote
    /// executor; the dialect is selected via `Database::dialect`.
    ///
    /// Available only with the `databricks` feature enabled.
    #[cfg(feature = "databricks")]
    pub fn new_databricks(
        schema_path: impl AsRef<Path>,
        config: DatabricksConfig,
    ) -> Result<Self, EmbeddedError> {
        // Parse the raw config so we can read the optional top-level
        // `catalog:` field (DeltaGraph Phase 3.2). It supplies a catalog
        // default when `DatabricksConfig.catalog` is unset by env/CLI —
        // env/CLI still wins to preserve the existing precedence chain.
        let (graph_schema, schema_catalog) = load_graph_schema_with_catalog(schema_path.as_ref())?;
        let runtime = build_runtime()?;
        let mut effective_config = config;
        if effective_config.catalog.is_none() {
            effective_config.catalog = schema_catalog;
        }
        let executor = DatabricksSqlExecutor::new(effective_config)
            .map_err(|e| EmbeddedError::Executor(format!("Databricks executor: {e}")))?;
        Ok(Database {
            executor: Arc::new(NullExecutor),
            remote_executor: Some(Arc::new(executor) as Arc<dyn QueryExecutor>),
            schema: Arc::new(graph_schema),
            runtime,
            executor_kind: ExecutorKind::Remote,
            dialect: SqlDialect::Databricks,
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
            // Test helper: the executor is opaque, so we cannot route
            // writes through it safely. Tag as SqlOnly so the write
            // guard rejects writes here — tests that need writes should
            // construct a chdb-backed Database via `new` instead.
            executor_kind: ExecutorKind::SqlOnly,
            dialect: SqlDialect::ClickHouse,
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

    /// Open a database in SQL-only mode with a chosen `SqlDialect`.
    ///
    /// Same as [`sql_only`](Self::sql_only) but the resulting `Database`
    /// emits SQL for the requested dialect (e.g. Databricks/Spark) instead
    /// of the default ClickHouse spellings. Used by `cg --dialect …` to
    /// translate without needing a live warehouse.
    ///
    /// Returns `EmbeddedError::Validation` for dialects whose emitter is
    /// not yet implemented (PostgreSQL, DuckDB, MySQL, SQLite). Only
    /// `ClickHouse` and `Databricks` are accepted today.
    pub fn sql_only_with_dialect(
        schema_path: impl AsRef<Path>,
        dialect: SqlDialect,
    ) -> Result<Self, EmbeddedError> {
        match dialect {
            SqlDialect::ClickHouse | SqlDialect::Databricks => {}
            other => {
                return Err(EmbeddedError::Validation(format!(
                    "SqlDialect::{other:?} is not yet implemented for SQL emission; \
                     supported dialects: ClickHouse, Databricks"
                )));
            }
        }
        let mut db = Self::sql_only(schema_path)?;
        db.dialect = dialect;
        Ok(db)
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

/// Read and parse a YAML schema file.
fn read_schema_config(schema_path: &Path) -> Result<GraphSchemaConfig, EmbeddedError> {
    let yaml_content = std::fs::read_to_string(schema_path).map_err(|e| {
        EmbeddedError::Io(format!(
            "Cannot read schema '{}': {}",
            schema_path.display(),
            e
        ))
    })?;
    serde_yaml::from_str(&yaml_content)
        .map_err(|e| EmbeddedError::Schema(format!("YAML parse error: {}", e)))
}

/// Load a YAML schema file into a `GraphSchema` without a database: a schema
/// that asks to discover columns (`auto_discover_columns`) is refused.
fn load_graph_schema(schema_path: &Path) -> Result<GraphSchema, EmbeddedError> {
    read_schema_config(schema_path)?
        .to_graph_schema()
        .map_err(schema_build_error)
}

/// Same as [`load_graph_schema`] but also returns the optional top-level
/// `catalog:` field from the YAML. Used by the Databricks path to honor
/// DeltaGraph Phase 3.2 (schema-embedded Unity Catalog default).
#[cfg(feature = "databricks")]
fn load_graph_schema_with_catalog(
    schema_path: &Path,
) -> Result<(GraphSchema, Option<String>), EmbeddedError> {
    let schema_config = read_schema_config(schema_path)?;
    let catalog = schema_config.catalog.clone();
    // Column discovery reads ClickHouse metadata (`system.columns`), which a
    // SQL Warehouse does not have, so a discovering schema is refused here.
    if let Some(target) = schema_config.column_discovery_targets().first() {
        return Err(EmbeddedError::Schema(format!(
            "{} sets auto_discover_columns: true, which is not supported with Databricks; \
             declare its property_mappings instead",
            target.owner
        )));
    }
    let schema = schema_config
        .to_graph_schema()
        .map_err(schema_build_error)?;
    Ok((schema, catalog))
}

/// Where a mode reads the columns that `auto_discover_columns` asks for.
#[derive(Clone, Copy)]
struct ColumnReaders<'a> {
    /// Reads an element's `source:` table function (chdb, which creates the
    /// element's view only after the schema is built).
    sources: Option<&'a dyn QueryExecutor>,
    /// Reads `system.columns` for the database that holds the tables.
    tables: Option<&'a dyn QueryExecutor>,
}

/// Build a `GraphSchema`, reading the columns of every table it asks to
/// discover (`auto_discover_columns`) through `readers`.
fn build_graph_schema(
    schema_config: &GraphSchemaConfig,
    readers: ColumnReaders<'_>,
    runtime: &tokio::runtime::Runtime,
) -> Result<GraphSchema, EmbeddedError> {
    let mut columns = DiscoveredColumns::default();
    for target in schema_config.column_discovery_targets() {
        if columns.contains(&target.database, &target.table) {
            continue;
        }
        let (executor, sql) = match (&target.source, readers.sources, readers.tables) {
            (Some(source), Some(executor), _) => (
                executor,
                describe_columns_sql(&resolve_source_uri(source).map_err(|e| {
                    EmbeddedError::Schema(format!("Invalid source for {}: {}", target.owner, e))
                })?),
            ),
            (_, _, Some(executor)) => (
                executor,
                system_columns_sql(&target.database, &target.table),
            ),
            (_, _, None) => {
                return Err(EmbeddedError::Schema(format!(
                    "{} sets auto_discover_columns: true, but has no `source:`: embedded chdb \
                     creates its table from the schema, so there are no columns to discover. \
                     Give it a `source:`, or declare its property_mappings instead.",
                    target.owner
                )))
            }
        };
        let read = |e: String| {
            EmbeddedError::Schema(format!(
                "Failed to read the columns of {} for auto_discover_columns: {}",
                target.owner, e
            ))
        };
        let rows = runtime
            .block_on(executor.execute_json(&sql, None))
            .map_err(|e| read(e.to_string()))?;
        let info = column_info_from_rows(&rows).map_err(read)?;
        columns.insert(&target.database, &target.table, info);
    }
    read_column_types(schema_config, readers, runtime, &mut columns);
    schema_config
        .to_graph_schema_with_columns(&columns)
        .map_err(schema_build_error)
}

/// Read the column types of every table `columns` does not hold yet, where
/// `readers` can (docs/design/VALUE_TYPES.md T1): a `source:` through its
/// executor, the others in one `system.columns` read. A table that cannot be
/// read keeps unknown types, which leaves its SQL as it was, so failures are
/// logged, not errors.
fn read_column_types(
    schema_config: &GraphSchemaConfig,
    readers: ColumnReaders<'_>,
    runtime: &tokio::runtime::Runtime,
    columns: &mut DiscoveredColumns,
) {
    let mut tables: Vec<(String, String)> = Vec::new();
    for target in schema_config.type_discovery_targets() {
        if columns.contains(&target.database, &target.table) {
            continue;
        }
        match (&target.source, readers.sources, readers.tables) {
            (Some(source), Some(executor), _) => {
                let read = resolve_source_uri(source)
                    .map_err(|e| e.to_string())
                    .and_then(|uri| {
                        runtime
                            .block_on(executor.execute_json(&describe_columns_sql(&uri), None))
                            .map_err(|e| e.to_string())
                    })
                    .and_then(|rows| column_info_from_rows(&rows));
                match read {
                    Ok(info) => columns.insert(&target.database, &target.table, info),
                    Err(e) => log::warn!(
                        "Could not read the column types of {}; they stay unknown: {}",
                        target.owner,
                        e
                    ),
                }
            }
            (_, _, Some(_)) => {
                let key = (target.database, target.table);
                if !tables.contains(&key) {
                    tables.push(key);
                }
            }
            (_, _, None) => {}
        }
    }
    let Some(executor) = readers.tables else {
        return;
    };
    if tables.is_empty() {
        return;
    }
    let read = runtime
        .block_on(executor.execute_json(&system_columns_of_tables_sql(&tables), None))
        .map_err(|e| e.to_string())
        .and_then(|rows| columns_by_table_from_rows(&rows));
    match read {
        Ok(found) => {
            for ((database, table), info) in found {
                columns.insert(&database, &table, info);
            }
        }
        Err(e) => log::warn!(
            "Could not read the column types of the schema's tables; they stay unknown: {}",
            e
        ),
    }
}

fn schema_build_error(e: impl std::fmt::Display) -> EmbeddedError {
    EmbeddedError::Schema(format!("Schema build error: {}", e))
}

/// Start a chdb session with the configured limits.
#[cfg(feature = "embedded")]
fn open_chdb_session(
    config: &SystemConfig,
) -> Result<(tokio::runtime::Runtime, ChdbExecutor), EmbeddedError> {
    let runtime = build_runtime()?;

    let (session_dir, auto_cleanup) = match &config.session_dir {
        Some(dir) => (dir.clone(), false),
        None => {
            let tmp = std::env::temp_dir().join(format!("clickgraph-{}", pseudo_random_suffix()));
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
    Ok((runtime, executor))
}

/// Build a single-threaded Tokio runtime for blocking `Connection` calls.
fn build_runtime() -> Result<tokio::runtime::Runtime, EmbeddedError> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| EmbeddedError::Query(format!("Failed to create runtime: {}", e)))
}

#[cfg(feature = "embedded")]
fn pseudo_random_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| format!("{}", d.subsec_nanos()))
        .unwrap_or_else(|_| "0".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    const MINIMAL_SCHEMA: &str = r#"
name: db_test
graph_schema:
  nodes:
    - label: User
      database: test_db
      table: users
      node_id: user_id
      property_mappings:
        user_id: user_id
  edges: []
"#;

    fn write_schema() -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().expect("tempfile");
        f.write_all(MINIMAL_SCHEMA.as_bytes()).expect("write");
        f.flush().expect("flush");
        f
    }

    #[test]
    fn sql_only_with_dialect_accepts_supported_dialects() {
        let schema = write_schema();
        let db_ch = Database::sql_only_with_dialect(schema.path(), SqlDialect::ClickHouse);
        assert!(db_ch.is_ok());
        let db_dx = Database::sql_only_with_dialect(schema.path(), SqlDialect::Databricks);
        assert!(db_dx.is_ok());
    }

    /// #1314: SQL handed to the caller for running elsewhere must carry the
    /// settings its results depend on (`join_use_nulls = 1`) for ClickHouse,
    /// and must stay plain SQL for Databricks/Spark.
    #[test]
    fn query_to_sql_carries_semantic_settings_only_for_clickhouse() {
        let schema = write_schema();
        let cypher = "MATCH (u:User) RETURN u.user_id";
        let ch = Database::sql_only_with_dialect(schema.path(), SqlDialect::ClickHouse).unwrap();
        let ch_sql = crate::Connection::new(&ch)
            .unwrap()
            .query_to_sql(cypher)
            .unwrap();
        assert!(
            ch_sql.trim_end().ends_with("SETTINGS join_use_nulls = 1"),
            "{ch_sql}"
        );
        let dx = Database::sql_only_with_dialect(schema.path(), SqlDialect::Databricks).unwrap();
        let dx_sql = crate::Connection::new(&dx)
            .unwrap()
            .query_to_sql(cypher)
            .unwrap();
        assert!(!dx_sql.contains("SETTINGS"), "{dx_sql}");
    }

    #[test]
    fn sql_only_with_dialect_rejects_unimplemented_dialects() {
        // Only ClickHouse and Databricks are translated to: the others are
        // refused up front. Copilot (PR #332) flagged the original
        // unconditional acceptance.
        let schema = write_schema();
        for d in [
            SqlDialect::PostgreSQL,
            SqlDialect::DuckDB,
            SqlDialect::MySQL,
            SqlDialect::SQLite,
        ] {
            match Database::sql_only_with_dialect(schema.path(), d) {
                Ok(_) => panic!("expected validation error for {d:?}"),
                Err(EmbeddedError::Validation(_)) => {}
                Err(e) => panic!("expected Validation error, got {e:?} for {d:?}"),
            }
        }
    }

    /// Answers every query with one column row and records the SQL it saw.
    struct RecordingExecutor {
        seen: std::sync::Mutex<Vec<String>>,
    }

    impl RecordingExecutor {
        fn new() -> Self {
            Self {
                seen: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn seen(&self) -> Vec<String> {
            self.seen.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl QueryExecutor for RecordingExecutor {
        async fn execute_json(
            &self,
            sql: &str,
            _role: Option<&str>,
        ) -> Result<Vec<serde_json::Value>, ExecutorError> {
            self.seen.lock().unwrap().push(sql.to_string());
            Ok(vec![
                serde_json::json!({"name": "id", "type": "UInt64"}),
                serde_json::json!({"name": "city", "type": "String"}),
            ])
        }

        async fn execute_text(
            &self,
            _sql: &str,
            _format: &str,
            _role: Option<&str>,
        ) -> Result<String, ExecutorError> {
            Ok(String::new())
        }
    }

    const DISCOVERING_SCHEMA: &str = r#"
graph_schema:
  nodes:
    - label: Sourced
      database: db
      table: sourced
      node_id: id
      source: "/data/sourced.parquet"
      auto_discover_columns: true
      property_mappings: {}
    - label: Plain
      database: db
      table: plain
      node_id: id
      auto_discover_columns: true
      property_mappings: {}
  edges: []
"#;

    #[test]
    fn discovery_reads_sources_locally_and_tables_remotely() {
        let config: GraphSchemaConfig = serde_yaml::from_str(DISCOVERING_SCHEMA).unwrap();
        let runtime = build_runtime().unwrap();
        let (local, remote) = (RecordingExecutor::new(), RecordingExecutor::new());
        let readers = ColumnReaders {
            sources: Some(&local),
            tables: Some(&remote),
        };
        let schema = build_graph_schema(&config, readers, &runtime).unwrap();
        assert_eq!(
            local.seen(),
            ["DESCRIBE TABLE file('/data/sourced.parquet', 'Parquet')"]
        );
        assert_eq!(remote.seen(), [system_columns_sql("db", "plain")]);
        for label in ["Sourced", "Plain"] {
            let node = schema.node_schema(label).unwrap();
            assert!(node.property_mappings.contains_key("city"), "{label}");
        }
    }

    /// Answers a source's DESCRIBE and the one `system.columns` read of
    /// several tables, as a loader sees them.
    struct TypesExecutor {
        seen: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait]
    impl QueryExecutor for TypesExecutor {
        async fn execute_json(
            &self,
            sql: &str,
            _role: Option<&str>,
        ) -> Result<Vec<serde_json::Value>, ExecutorError> {
            self.seen.lock().unwrap().push(sql.to_string());
            if sql.starts_with("DESCRIBE") {
                return Ok(vec![serde_json::json!({"name": "id", "type": "Date"})]);
            }
            Ok(vec![
                serde_json::json!({"database": "db", "table": "plain", "name": "id", "type": "String"}),
                serde_json::json!({"database": "db", "table": "links", "name": "a", "type": "DateTime"}),
                serde_json::json!({"database": "db", "table": "links", "name": "b", "type": "String"}),
            ])
        }

        async fn execute_text(
            &self,
            _sql: &str,
            _format: &str,
            _role: Option<&str>,
        ) -> Result<String, ExecutorError> {
            Ok(String::new())
        }
    }

    /// VALUE_TYPES.md T1: a schema that discovers nothing still has its
    /// tables' column types read, a source locally and the other tables in
    /// one remote read, without gaining properties.
    #[test]
    fn column_types_are_read_for_declared_elements() {
        let config: GraphSchemaConfig = serde_yaml::from_str(
            r#"
graph_schema:
  nodes:
    - label: Sourced
      database: db
      table: sourced
      node_id: id
      source: "/data/sourced.parquet"
      property_mappings: {}
    - label: Plain
      database: db
      table: plain
      node_id: id
      property_mappings: {}
  edges:
    - type: LINK
      database: db
      table: links
      from_node: Sourced
      to_node: Plain
      from_id: a
      to_id: b
      property_mappings: {}
"#,
        )
        .unwrap();
        let runtime = build_runtime().unwrap();
        let (local, remote) = (
            TypesExecutor {
                seen: Default::default(),
            },
            TypesExecutor {
                seen: Default::default(),
            },
        );
        let readers = ColumnReaders {
            sources: Some(&local),
            tables: Some(&remote),
        };
        let schema = build_graph_schema(&config, readers, &runtime).unwrap();
        assert_eq!(
            *local.seen.lock().unwrap(),
            ["DESCRIBE TABLE file('/data/sourced.parquet', 'Parquet')"]
        );
        assert_eq!(
            *remote.seen.lock().unwrap(),
            [system_columns_of_tables_sql(&[
                ("db".to_string(), "plain".to_string()),
                ("db".to_string(), "links".to_string()),
            ])]
        );
        let sourced = schema.node_schema("Sourced").unwrap();
        assert_eq!(sourced.node_id.part_types[0].raw, "Date");
        assert_eq!(sourced.property_mappings.len(), 1);
        assert_eq!(
            schema.node_schema("Plain").unwrap().node_id.part_types[0].raw,
            "String"
        );
        let link = schema.get_rel_schema("LINK").unwrap();
        assert_eq!(link.from_node_id_part_types[0].raw, "DateTime");
        assert_eq!(link.to_node_id_part_types[0].raw, "String");
    }

    /// A failed read of the types is not an error: they stay unknown.
    #[test]
    fn unreadable_column_types_stay_unknown() {
        let config: GraphSchemaConfig = serde_yaml::from_str(MINIMAL_SCHEMA).unwrap();
        let runtime = build_runtime().unwrap();
        // Rows without `database` / `table`: not a reply to the read.
        let remote = RecordingExecutor::new();
        let readers = ColumnReaders {
            sources: None,
            tables: Some(&remote),
        };
        let schema = build_graph_schema(&config, readers, &runtime).unwrap();
        assert!(schema
            .node_schema("User")
            .unwrap()
            .node_id
            .part_types
            .is_empty());
    }

    #[test]
    fn chdb_alone_cannot_discover_a_table_without_source() {
        let config: GraphSchemaConfig = serde_yaml::from_str(DISCOVERING_SCHEMA).unwrap();
        let runtime = build_runtime().unwrap();
        let local = RecordingExecutor::new();
        let readers = ColumnReaders {
            sources: Some(&local),
            tables: None,
        };
        let err = build_graph_schema(&config, readers, &runtime)
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("node 'Plain'"), "{err}");
        assert!(err.contains("has no `source:`"), "{err}");
    }
}
