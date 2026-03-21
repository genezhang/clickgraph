//! UniFFI bindings for ClickGraph embedded — exports `Database`, `Connection`,
//! `QueryResult`, and `Value` for Go (and other languages) via the C ABI.
//!
//! This crate is a thin wrapper around `clickgraph-embedded` that satisfies
//! UniFFI's ownership model (Arc-based, no lifetimes).

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use clickgraph_embedded::database::{
    Database as RustDatabase, RemoteConfig as RustRemoteConfig, StorageCredentials,
    SystemConfig as RustSystemConfig,
};
use clickgraph_embedded::export::{
    ExportFormat as RustExportFormat, ExportOptions as RustExportOptions,
};
use clickgraph_embedded::graph_result::{
    GraphResult as RustGraphResult, StoreStats as RustStoreStats,
};
use clickgraph_embedded::value::Value as RustValue;

uniffi::setup_scaffolding!();

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum ClickGraphError {
    #[error("{msg}")]
    DatabaseError { msg: String },

    #[error("{msg}")]
    QueryError { msg: String },

    #[error("{msg}")]
    ExportError { msg: String },

    #[error("{msg}")]
    ValidationError { msg: String },
}

impl From<clickgraph_embedded::error::EmbeddedError> for ClickGraphError {
    fn from(e: clickgraph_embedded::error::EmbeddedError) -> Self {
        match e {
            clickgraph_embedded::error::EmbeddedError::Schema(msg) => {
                ClickGraphError::DatabaseError { msg }
            }
            clickgraph_embedded::error::EmbeddedError::Io(msg) => {
                ClickGraphError::DatabaseError { msg }
            }
            clickgraph_embedded::error::EmbeddedError::Executor(msg) => {
                ClickGraphError::DatabaseError { msg }
            }
            clickgraph_embedded::error::EmbeddedError::Query(msg) => {
                ClickGraphError::QueryError { msg }
            }
            clickgraph_embedded::error::EmbeddedError::Validation(msg) => {
                ClickGraphError::ValidationError { msg }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Value — recursive enum for query result cells
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, uniffi::Enum)]
pub enum Value {
    Null,
    Bool { v: bool },
    Int64 { v: i64 },
    Float64 { v: f64 },
    String { v: String },
    List { items: Vec<Value> },
    Map { entries: Vec<MapEntry> },
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct MapEntry {
    pub key: String,
    pub value: Value,
}

impl From<RustValue> for Value {
    fn from(rv: RustValue) -> Self {
        match rv {
            RustValue::Null => Value::Null,
            RustValue::Bool(b) => Value::Bool { v: b },
            RustValue::Int64(n) => Value::Int64 { v: n },
            RustValue::Float64(f) => Value::Float64 { v: f },
            RustValue::String(s) => Value::String { v: s },
            RustValue::List(items) => Value::List {
                items: items.into_iter().map(Value::from).collect(),
            },
            RustValue::Map(pairs) => Value::Map {
                entries: pairs
                    .into_iter()
                    .map(|(k, v)| MapEntry {
                        key: k,
                        value: Value::from(v),
                    })
                    .collect(),
            },
        }
    }
}

/// Convert an FFI `Value` to a Rust `Value`.
fn to_rust_value(v: Value) -> RustValue {
    match v {
        Value::Null => RustValue::Null,
        Value::Bool { v } => RustValue::Bool(v),
        Value::Int64 { v } => RustValue::Int64(v),
        Value::Float64 { v } => RustValue::Float64(v),
        Value::String { v } => RustValue::String(v),
        Value::List { items } => RustValue::List(items.into_iter().map(to_rust_value).collect()),
        Value::Map { entries } => RustValue::Map(
            entries
                .into_iter()
                .map(|e| (e.key, to_rust_value(e.value)))
                .collect(),
        ),
    }
}

/// Convert an FFI property map to a Rust property map.
fn to_rust_properties(props: HashMap<String, Value>) -> HashMap<String, RustValue> {
    props
        .into_iter()
        .map(|(k, v)| (k, to_rust_value(v)))
        .collect()
}

// ---------------------------------------------------------------------------
// RemoteConfig — connection details for a remote ClickHouse cluster
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, uniffi::Record)]
pub struct RemoteConfig {
    pub url: String,
    pub user: String,
    pub password: String,
    pub database: Option<String>,
    pub cluster_name: Option<String>,
}

// ---------------------------------------------------------------------------
// GraphNode / GraphEdge / GraphResult — structured graph output
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, uniffi::Record)]
pub struct GraphNode {
    pub id: String,
    pub labels: Vec<String>,
    pub properties: HashMap<String, Value>,
}

#[derive(Debug, Clone, uniffi::Record)]
pub struct GraphEdge {
    pub id: String,
    pub type_name: String,
    pub from_id: String,
    pub to_id: String,
    pub properties: HashMap<String, Value>,
}

#[derive(uniffi::Object)]
pub struct GraphResult {
    inner: RustGraphResult,
}

#[uniffi::export]
impl GraphResult {
    /// Return all nodes in the graph result.
    pub fn nodes(&self) -> Vec<GraphNode> {
        self.inner
            .nodes()
            .iter()
            .map(|n| GraphNode {
                id: n.id.clone(),
                labels: n.labels.clone(),
                properties: n
                    .properties
                    .iter()
                    .map(|(k, v)| (k.clone(), Value::from(v.clone())))
                    .collect(),
            })
            .collect()
    }

    /// Return all edges in the graph result.
    pub fn edges(&self) -> Vec<GraphEdge> {
        self.inner
            .edges()
            .iter()
            .map(|e| GraphEdge {
                id: e.id.clone(),
                type_name: e.type_name.clone(),
                from_id: e.from_id.clone(),
                to_id: e.to_id.clone(),
                properties: e
                    .properties
                    .iter()
                    .map(|(k, v)| (k.clone(), Value::from(v.clone())))
                    .collect(),
            })
            .collect()
    }

    /// Return the number of nodes.
    pub fn node_count(&self) -> u64 {
        self.inner.node_count() as u64
    }

    /// Return the number of edges.
    pub fn edge_count(&self) -> u64 {
        self.inner.edge_count() as u64
    }
}

// ---------------------------------------------------------------------------
// StoreStats — result of store_subgraph()
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, uniffi::Record)]
pub struct StoreStats {
    pub nodes_stored: u64,
    pub edges_stored: u64,
}

// ---------------------------------------------------------------------------
// EdgeInput — FFI-friendly record for batch edge creation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, uniffi::Record)]
pub struct EdgeInput {
    pub from_id: String,
    pub to_id: String,
    pub properties: HashMap<String, Value>,
}

// ---------------------------------------------------------------------------
// Row — a single result row
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, uniffi::Record)]
pub struct Row {
    pub columns: Vec<String>,
    pub values: Vec<Value>,
}

// ---------------------------------------------------------------------------
// QueryResult — returned by Connection.query()
// ---------------------------------------------------------------------------

#[derive(uniffi::Object)]
pub struct QueryResult {
    columns: Vec<String>,
    rows: Vec<Vec<RustValue>>,
    position: AtomicUsize,
}

#[uniffi::export]
impl QueryResult {
    /// Column names in result order.
    pub fn column_names(&self) -> Vec<String> {
        self.columns.clone()
    }

    /// Total number of rows.
    pub fn num_rows(&self) -> u64 {
        self.rows.len() as u64
    }

    /// Return all rows at once as a list of Row records.
    pub fn get_all_rows(&self) -> Vec<Row> {
        self.rows
            .iter()
            .map(|row| Row {
                columns: self.columns.clone(),
                values: row.iter().cloned().map(Value::from).collect(),
            })
            .collect()
    }

    /// Return true if the cursor has more rows.
    pub fn has_next(&self) -> bool {
        self.position.load(Ordering::Relaxed) < self.rows.len()
    }

    /// Return the next row (cursor-style). Returns None when exhausted.
    pub fn get_next(&self) -> Option<Row> {
        let pos = self.position.fetch_add(1, Ordering::Relaxed);
        if pos >= self.rows.len() {
            // Restore position so repeated calls stay at the end
            self.position.store(self.rows.len(), Ordering::Relaxed);
            return None;
        }
        let row = &self.rows[pos];
        Some(Row {
            columns: self.columns.clone(),
            values: row.iter().cloned().map(Value::from).collect(),
        })
    }

    /// Reset the cursor to the beginning.
    pub fn reset(&self) {
        self.position.store(0, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// SystemConfig — optional database configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, uniffi::Record)]
pub struct SystemConfig {
    pub session_dir: Option<String>,
    pub data_dir: Option<String>,
    pub max_threads: Option<u32>,
    pub s3_access_key_id: Option<String>,
    pub s3_secret_access_key: Option<String>,
    pub s3_region: Option<String>,
    pub s3_endpoint_url: Option<String>,
    pub s3_session_token: Option<String>,
    pub gcs_access_key_id: Option<String>,
    pub gcs_secret_access_key: Option<String>,
    pub azure_storage_account_name: Option<String>,
    pub azure_storage_account_key: Option<String>,
    pub azure_storage_connection_string: Option<String>,
    /// Remote ClickHouse connection for hybrid query + local storage.
    pub remote: Option<RemoteConfig>,
}

// ---------------------------------------------------------------------------
// ExportOptions — export format and compression
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, uniffi::Record)]
pub struct ExportOptions {
    /// Format name: "parquet", "csv", "tsv", "json", "ndjson".
    /// If None, auto-detected from the file extension.
    pub format: Option<String>,
    /// Parquet compression codec: "snappy", "gzip", "lz4", "zstd".
    pub compression: Option<String>,
}

// ---------------------------------------------------------------------------
// Database — the top-level handle
// ---------------------------------------------------------------------------

#[derive(uniffi::Object)]
pub struct Database {
    inner: Arc<RustDatabase>,
}

#[uniffi::export]
impl Database {
    /// Open a database from a YAML schema file with default configuration.
    #[uniffi::constructor]
    pub fn open(schema_path: String) -> Result<Arc<Self>, ClickGraphError> {
        let db = RustDatabase::new(&schema_path, RustSystemConfig::default())
            .map_err(|e| ClickGraphError::DatabaseError { msg: e.to_string() })?;
        Ok(Arc::new(Database {
            inner: Arc::new(db),
        }))
    }

    /// Open a database from a YAML schema file with custom configuration.
    #[uniffi::constructor]
    pub fn open_with_config(
        schema_path: String,
        config: SystemConfig,
    ) -> Result<Arc<Self>, ClickGraphError> {
        let rust_config = RustSystemConfig {
            session_dir: config.session_dir.map(std::path::PathBuf::from),
            data_dir: config.data_dir.map(std::path::PathBuf::from),
            max_threads: config.max_threads.map(|t| t as usize),
            credentials: StorageCredentials {
                s3_access_key_id: config.s3_access_key_id,
                s3_secret_access_key: config.s3_secret_access_key,
                s3_region: config.s3_region,
                s3_endpoint_url: config.s3_endpoint_url,
                s3_session_token: config.s3_session_token,
                gcs_access_key_id: config.gcs_access_key_id,
                gcs_secret_access_key: config.gcs_secret_access_key,
                azure_storage_account_name: config.azure_storage_account_name,
                azure_storage_account_key: config.azure_storage_account_key,
                azure_storage_connection_string: config.azure_storage_connection_string,
            },
            remote: config.remote.map(|r| RustRemoteConfig {
                url: r.url,
                user: r.user,
                password: r.password,
                database: r.database,
                cluster_name: r.cluster_name,
            }),
        };
        let db = RustDatabase::new(&schema_path, rust_config)
            .map_err(|e| ClickGraphError::DatabaseError { msg: e.to_string() })?;
        Ok(Arc::new(Database {
            inner: Arc::new(db),
        }))
    }

    /// Open a database in SQL-only mode (no chdb backend).
    ///
    /// Loads the schema from a YAML file. Supports `query_to_sql()` and
    /// `export_to_sql()` for Cypher → SQL translation. Calling `query()` or
    /// `export()` will return an error.
    #[uniffi::constructor]
    pub fn open_sql_only(schema_path: String) -> Result<Arc<Self>, ClickGraphError> {
        let db = RustDatabase::sql_only(&schema_path)
            .map_err(|e| ClickGraphError::DatabaseError { msg: e.to_string() })?;
        Ok(Arc::new(Database {
            inner: Arc::new(db),
        }))
    }

    /// Create a connection to this database.
    pub fn connect(&self) -> Result<Arc<Connection>, ClickGraphError> {
        Ok(Arc::new(Connection {
            db: Arc::clone(&self.inner),
        }))
    }
}

// ---------------------------------------------------------------------------
// Connection — executes queries and write operations
// ---------------------------------------------------------------------------

#[derive(uniffi::Object)]
pub struct Connection {
    db: Arc<RustDatabase>,
}

#[uniffi::export]
impl Connection {
    /// Execute a Cypher query and return a QueryResult.
    pub fn query(&self, cypher: String) -> Result<Arc<QueryResult>, ClickGraphError> {
        let conn = clickgraph_embedded::Connection::new(&self.db).map_err(ClickGraphError::from)?;
        let result = conn.query(&cypher).map_err(ClickGraphError::from)?;

        let columns = result.get_column_names().to_vec();
        let rows: Vec<Vec<RustValue>> = result.map(|row| row.values().to_vec()).collect();

        Ok(Arc::new(QueryResult {
            columns,
            rows,
            position: AtomicUsize::new(0),
        }))
    }

    /// Translate a Cypher query to ClickHouse SQL without executing it.
    pub fn query_to_sql(&self, cypher: String) -> Result<String, ClickGraphError> {
        let conn = clickgraph_embedded::Connection::new(&self.db).map_err(ClickGraphError::from)?;
        conn.query_to_sql(&cypher).map_err(ClickGraphError::from)
    }

    /// Export Cypher query results directly to a file.
    ///
    /// Supported formats: parquet, csv, tsv, json, ndjson.
    /// Format is auto-detected from the file extension if not specified.
    pub fn export(
        &self,
        cypher: String,
        output_path: String,
        options: ExportOptions,
    ) -> Result<(), ClickGraphError> {
        let rust_opts = RustExportOptions {
            format: options.format.as_deref().map(parse_format).transpose()?,
            compression: options.compression,
        };
        let conn = clickgraph_embedded::Connection::new(&self.db).map_err(ClickGraphError::from)?;
        conn.export(&cypher, &output_path, rust_opts)
            .map_err(ClickGraphError::from)
    }

    /// Generate the export SQL without executing it (for debugging).
    pub fn export_to_sql(
        &self,
        cypher: String,
        output_path: String,
        options: ExportOptions,
    ) -> Result<String, ClickGraphError> {
        let rust_opts = RustExportOptions {
            format: options.format.as_deref().map(parse_format).transpose()?,
            compression: options.compression,
        };
        let conn = clickgraph_embedded::Connection::new(&self.db).map_err(ClickGraphError::from)?;
        conn.export_to_sql(&cypher, &output_path, rust_opts)
            .map_err(ClickGraphError::from)
    }

    /// Execute a raw SQL statement (DDL, DML, or administrative command).
    ///
    /// No Cypher parsing or schema validation; the caller is responsible for
    /// SQL correctness.
    pub fn execute_sql(&self, sql: String) -> Result<(), ClickGraphError> {
        let conn = clickgraph_embedded::Connection::new(&self.db).map_err(ClickGraphError::from)?;
        conn.execute_sql(&sql).map_err(ClickGraphError::from)
    }

    /// Create a node with the given label and properties.
    ///
    /// Returns the node ID (caller-provided or auto-generated UUID).
    pub fn create_node(
        &self,
        label: String,
        properties: HashMap<String, Value>,
    ) -> Result<String, ClickGraphError> {
        let conn = clickgraph_embedded::Connection::new(&self.db).map_err(ClickGraphError::from)?;
        conn.create_node(&label, to_rust_properties(properties))
            .map_err(ClickGraphError::from)
    }

    /// Create an edge between two nodes.
    pub fn create_edge(
        &self,
        edge_type: String,
        from_id: String,
        to_id: String,
        properties: HashMap<String, Value>,
    ) -> Result<(), ClickGraphError> {
        let conn = clickgraph_embedded::Connection::new(&self.db).map_err(ClickGraphError::from)?;
        conn.create_edge(&edge_type, &from_id, &to_id, to_rust_properties(properties))
            .map_err(ClickGraphError::from)
    }

    /// Upsert a node (INSERT with ReplacingMergeTree deduplication).
    ///
    /// The node_id property MUST be present in the properties map.
    pub fn upsert_node(
        &self,
        label: String,
        properties: HashMap<String, Value>,
    ) -> Result<String, ClickGraphError> {
        let conn = clickgraph_embedded::Connection::new(&self.db).map_err(ClickGraphError::from)?;
        conn.upsert_node(&label, to_rust_properties(properties))
            .map_err(ClickGraphError::from)
    }

    /// Upsert an edge (INSERT with ReplacingMergeTree deduplication).
    pub fn upsert_edge(
        &self,
        edge_type: String,
        from_id: String,
        to_id: String,
        properties: HashMap<String, Value>,
    ) -> Result<(), ClickGraphError> {
        let conn = clickgraph_embedded::Connection::new(&self.db).map_err(ClickGraphError::from)?;
        conn.upsert_edge(&edge_type, &from_id, &to_id, to_rust_properties(properties))
            .map_err(ClickGraphError::from)
    }

    /// Create multiple nodes in a single batch INSERT.
    ///
    /// Returns a Vec of node IDs.
    pub fn create_nodes(
        &self,
        label: String,
        batch: Vec<HashMap<String, Value>>,
    ) -> Result<Vec<String>, ClickGraphError> {
        let conn = clickgraph_embedded::Connection::new(&self.db).map_err(ClickGraphError::from)?;
        let rust_batch: Vec<HashMap<String, RustValue>> =
            batch.into_iter().map(to_rust_properties).collect();
        conn.create_nodes(&label, rust_batch)
            .map_err(ClickGraphError::from)
    }

    /// Create multiple edges in a single batch INSERT.
    pub fn create_edges(
        &self,
        edge_type: String,
        batch: Vec<EdgeInput>,
    ) -> Result<(), ClickGraphError> {
        let conn = clickgraph_embedded::Connection::new(&self.db).map_err(ClickGraphError::from)?;
        let rust_batch: Vec<(String, String, HashMap<String, RustValue>)> = batch
            .into_iter()
            .map(|e| (e.from_id, e.to_id, to_rust_properties(e.properties)))
            .collect();
        conn.create_edges(&edge_type, rust_batch)
            .map_err(ClickGraphError::from)
    }

    /// Delete nodes matching the given label and filter criteria.
    pub fn delete_nodes(
        &self,
        label: String,
        filter: HashMap<String, Value>,
    ) -> Result<(), ClickGraphError> {
        let conn = clickgraph_embedded::Connection::new(&self.db).map_err(ClickGraphError::from)?;
        conn.delete_nodes(&label, to_rust_properties(filter))
            .map_err(ClickGraphError::from)
    }

    /// Delete edges matching the given type and filter criteria.
    pub fn delete_edges(
        &self,
        edge_type: String,
        filter: HashMap<String, Value>,
    ) -> Result<(), ClickGraphError> {
        let conn = clickgraph_embedded::Connection::new(&self.db).map_err(ClickGraphError::from)?;
        conn.delete_edges(&edge_type, to_rust_properties(filter))
            .map_err(ClickGraphError::from)
    }

    /// Import nodes from inline newline-delimited JSON (JSONEachRow format).
    pub fn import_json(&self, label: String, json_lines: String) -> Result<(), ClickGraphError> {
        let conn = clickgraph_embedded::Connection::new(&self.db).map_err(ClickGraphError::from)?;
        conn.import_json(&label, &json_lines)
            .map_err(ClickGraphError::from)
    }

    /// Import nodes from a JSON file (JSONEachRow format).
    pub fn import_json_file(
        &self,
        label: String,
        file_path: String,
    ) -> Result<(), ClickGraphError> {
        let conn = clickgraph_embedded::Connection::new(&self.db).map_err(ClickGraphError::from)?;
        conn.import_json_file(&label, &file_path)
            .map_err(ClickGraphError::from)
    }

    /// Execute a Cypher query against the remote ClickHouse cluster.
    ///
    /// Requires `RemoteConfig` to have been provided in `SystemConfig`.
    pub fn query_remote(&self, cypher: String) -> Result<Arc<QueryResult>, ClickGraphError> {
        let conn = clickgraph_embedded::Connection::new(&self.db).map_err(ClickGraphError::from)?;
        let result = conn.query_remote(&cypher).map_err(ClickGraphError::from)?;

        let columns = result.get_column_names().to_vec();
        let rows: Vec<Vec<RustValue>> = result.map(|row| row.values().to_vec()).collect();

        Ok(Arc::new(QueryResult {
            columns,
            rows,
            position: AtomicUsize::new(0),
        }))
    }

    /// Execute a Cypher query locally and return a structured graph result.
    pub fn query_graph(&self, cypher: String) -> Result<Arc<GraphResult>, ClickGraphError> {
        let conn = clickgraph_embedded::Connection::new(&self.db).map_err(ClickGraphError::from)?;
        let result = conn.query_graph(&cypher).map_err(ClickGraphError::from)?;
        Ok(Arc::new(GraphResult { inner: result }))
    }

    /// Execute a Cypher query on the remote cluster and return a structured graph result.
    ///
    /// The returned `GraphResult` can be passed to `store_subgraph()` to persist locally.
    pub fn query_remote_graph(&self, cypher: String) -> Result<Arc<GraphResult>, ClickGraphError> {
        let conn = clickgraph_embedded::Connection::new(&self.db).map_err(ClickGraphError::from)?;
        let result = conn
            .query_remote_graph(&cypher)
            .map_err(ClickGraphError::from)?;
        Ok(Arc::new(GraphResult { inner: result }))
    }

    /// Store a `GraphResult` into local writable tables.
    ///
    /// Decomposes the graph into nodes and edges, then batch-inserts each group.
    pub fn store_subgraph(&self, graph: &GraphResult) -> Result<StoreStats, ClickGraphError> {
        let conn = clickgraph_embedded::Connection::new(&self.db).map_err(ClickGraphError::from)?;
        let stats = conn
            .store_subgraph(&graph.inner)
            .map_err(ClickGraphError::from)?;
        Ok(StoreStats {
            nodes_stored: stats.nodes_stored as u64,
            edges_stored: stats.edges_stored as u64,
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_format(name: &str) -> Result<RustExportFormat, ClickGraphError> {
    match name.to_lowercase().as_str() {
        "parquet" | "pq" => Ok(RustExportFormat::Parquet),
        "csv" => Ok(RustExportFormat::CSVWithNames),
        "csvwithnames" => Ok(RustExportFormat::CSVWithNames),
        "csvnoheader" => Ok(RustExportFormat::CSV),
        "tsv" | "tabseparated" => Ok(RustExportFormat::TSVWithNames),
        "tsvwithnames" => Ok(RustExportFormat::TSVWithNames),
        "json" => Ok(RustExportFormat::JSON),
        "jsoneachrow" | "ndjson" | "jsonl" => Ok(RustExportFormat::JSONEachRow),
        other => Err(ClickGraphError::ExportError {
            msg: format!(
                "Unknown export format '{}'. Supported: parquet, csv, tsv, json, ndjson",
                other
            ),
        }),
    }
}
