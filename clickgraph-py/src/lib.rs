//! Python bindings for ClickGraph embedded graph query engine.
//!
//! Exposes `Database`, `Connection`, `QueryResult` to Python via PyO3.

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use clickgraph_embedded::database::{Database as RustDatabase, SystemConfig as RustSystemConfig};
use clickgraph_embedded::error::EmbeddedError;
use clickgraph_embedded::value::Value as RustValue;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn to_pyerr(e: EmbeddedError) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

fn rust_value_to_py(py: Python<'_>, v: &RustValue) -> PyObject {
    match v {
        RustValue::Null => py.None(),
        RustValue::Bool(b) => b.into_pyobject(py).unwrap().to_owned().into_any().unbind(),
        RustValue::Int64(n) => n.into_pyobject(py).unwrap().into_any().unbind(),
        RustValue::Float64(f) => f.into_pyobject(py).unwrap().into_any().unbind(),
        RustValue::String(s) => s.into_pyobject(py).unwrap().into_any().unbind(),
        RustValue::List(items) => {
            let py_items: Vec<PyObject> = items.iter().map(|v| rust_value_to_py(py, v)).collect();
            PyList::new(py, &py_items).unwrap().into_any().unbind()
        }
        RustValue::Map(pairs) => {
            let dict = PyDict::new(py);
            for (k, v) in pairs {
                dict.set_item(k, rust_value_to_py(py, v)).unwrap();
            }
            dict.into_any().unbind()
        }
    }
}

// ---------------------------------------------------------------------------
// PyQueryResult — returned by Connection.query()
// ---------------------------------------------------------------------------

/// Result of a Cypher query. Iterable and indexable.
///
/// Each row is a list of Python values (str, int, float, bool, None, list, dict).
#[pyclass(name = "QueryResult")]
struct PyQueryResult {
    column_names: Vec<String>,
    rows: Vec<Vec<RustValue>>,
    position: usize,
}

#[pymethods]
impl PyQueryResult {
    /// Column names in result order.
    #[getter]
    fn column_names(&self) -> Vec<String> {
        self.column_names.clone()
    }

    /// Number of rows.
    #[getter]
    fn num_rows(&self) -> usize {
        self.rows.len()
    }

    /// Get all rows as a list of dicts.
    fn as_dicts(&self, py: Python<'_>) -> PyResult<PyObject> {
        let result = PyList::empty(py);
        for row in &self.rows {
            let dict = PyDict::new(py);
            for (i, col) in self.column_names.iter().enumerate() {
                let val = row
                    .get(i)
                    .map(|v| rust_value_to_py(py, v))
                    .unwrap_or_else(|| py.None());
                dict.set_item(col, val)?;
            }
            result.append(dict)?;
        }
        Ok(result.into_pyobject(py).unwrap().into_any().unbind())
    }

    /// Get a single row by index as a dict.
    fn get_row(&self, py: Python<'_>, index: usize) -> PyResult<PyObject> {
        if index >= self.rows.len() {
            return Err(PyRuntimeError::new_err(format!(
                "Row index {} out of range (0..{})",
                index,
                self.rows.len()
            )));
        }
        let dict = PyDict::new(py);
        for (i, col) in self.column_names.iter().enumerate() {
            let val = self.rows[index]
                .get(i)
                .map(|v| rust_value_to_py(py, v))
                .unwrap_or_else(|| py.None());
            dict.set_item(col, val)?;
        }
        Ok(dict.into_pyobject(py).unwrap().into_any().unbind())
    }

    fn __iter__(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.position = 0;
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> Option<PyObject> {
        if self.position >= self.rows.len() {
            return None;
        }
        let row = &self.rows[self.position];
        self.position += 1;

        let dict = PyDict::new(py);
        for (i, col) in self.column_names.iter().enumerate() {
            let val = row
                .get(i)
                .map(|v| rust_value_to_py(py, v))
                .unwrap_or_else(|| py.None());
            dict.set_item(col, val).ok();
        }
        Some(dict.into_pyobject(py).unwrap().into_any().unbind())
    }

    fn __len__(&self) -> usize {
        self.rows.len()
    }

    fn __repr__(&self) -> String {
        format!(
            "<QueryResult columns={:?} rows={}>",
            self.column_names,
            self.rows.len()
        )
    }
}

// ---------------------------------------------------------------------------
// PyConnection — wraps Connection<'db>
// ---------------------------------------------------------------------------

/// A connection to an embedded ClickGraph database.
///
/// Create via ``Database.connect()``.
#[pyclass(name = "Connection")]
struct PyConnection {
    // We own the Database Arc so the connection can outlive Python's GC ordering.
    db: std::sync::Arc<RustDatabase>,
}

#[pymethods]
impl PyConnection {
    /// Execute a Cypher query and return a QueryResult.
    ///
    /// >>> result = conn.query("MATCH (u:User) RETURN u.name LIMIT 5")
    /// >>> for row in result:
    /// ...     print(row["u.name"])
    #[pyo3(text_signature = "(self, cypher)")]
    fn query(&self, cypher: &str) -> PyResult<PyQueryResult> {
        // Create a Rust Connection with the underlying Database reference
        let conn = clickgraph_embedded::Connection::new(&self.db).map_err(to_pyerr)?;
        let result = conn.query(cypher).map_err(to_pyerr)?;

        let column_names = result.get_column_names().to_vec();
        let rows: Vec<Vec<RustValue>> = result.map(|row| row.values().to_vec()).collect();

        Ok(PyQueryResult {
            column_names,
            rows,
            position: 0,
        })
    }

    /// Translate Cypher to SQL without executing.
    ///
    /// Useful for debugging the generated ClickHouse SQL.
    #[pyo3(text_signature = "(self, cypher)")]
    fn query_to_sql(&self, cypher: &str) -> PyResult<String> {
        let conn = clickgraph_embedded::Connection::new(&self.db).map_err(to_pyerr)?;
        conn.query_to_sql(cypher).map_err(to_pyerr)
    }

    fn __repr__(&self) -> String {
        "<Connection>".to_string()
    }
}

// ---------------------------------------------------------------------------
// PyDatabase — wraps Database
// ---------------------------------------------------------------------------

/// An embedded ClickGraph database.
///
/// >>> db = Database("schema.yaml")
/// >>> conn = db.connect()
/// >>> result = conn.query("MATCH (u:User) RETURN u.name")
#[pyclass(name = "Database")]
struct PyDatabase {
    inner: std::sync::Arc<RustDatabase>,
}

#[pymethods]
impl PyDatabase {
    /// Open a database from a YAML schema file.
    ///
    /// Args:
    ///     schema_path: Path to the YAML graph schema.
    ///     session_dir: Optional directory for chdb session data.
    ///     data_dir: Optional base directory for relative source: paths.
    ///     max_threads: Optional maximum threads for chdb.
    ///     s3_access_key_id: AWS access key for S3 sources.
    ///     s3_secret_access_key: AWS secret key for S3 sources.
    ///     s3_region: AWS region for S3 sources.
    ///     s3_endpoint_url: Custom S3 endpoint URL.
    ///     gcs_access_key_id: GCS HMAC access key.
    ///     gcs_secret_access_key: GCS HMAC secret.
    ///     azure_storage_account_name: Azure storage account name.
    ///     azure_storage_account_key: Azure storage account key.
    ///     azure_storage_connection_string: Azure Blob Storage connection string.
    #[new]
    #[pyo3(signature = (
        schema_path,
        *,
        session_dir = None,
        data_dir = None,
        max_threads = None,
        s3_access_key_id = None,
        s3_secret_access_key = None,
        s3_region = None,
        s3_endpoint_url = None,
        s3_session_token = None,
        gcs_access_key_id = None,
        gcs_secret_access_key = None,
        azure_storage_account_name = None,
        azure_storage_account_key = None,
        azure_storage_connection_string = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        schema_path: &str,
        session_dir: Option<String>,
        data_dir: Option<String>,
        max_threads: Option<usize>,
        s3_access_key_id: Option<String>,
        s3_secret_access_key: Option<String>,
        s3_region: Option<String>,
        s3_endpoint_url: Option<String>,
        s3_session_token: Option<String>,
        gcs_access_key_id: Option<String>,
        gcs_secret_access_key: Option<String>,
        azure_storage_account_name: Option<String>,
        azure_storage_account_key: Option<String>,
        azure_storage_connection_string: Option<String>,
    ) -> PyResult<Self> {
        let credentials = clickgraph_embedded::StorageCredentials {
            s3_access_key_id,
            s3_secret_access_key,
            s3_region,
            s3_endpoint_url,
            s3_session_token,
            gcs_access_key_id,
            gcs_secret_access_key,
            azure_storage_account_name,
            azure_storage_account_key,
            azure_storage_connection_string,
        };

        let config = RustSystemConfig {
            session_dir: session_dir.map(std::path::PathBuf::from),
            data_dir: data_dir.map(std::path::PathBuf::from),
            max_threads,
            credentials,
        };

        let db = RustDatabase::new(schema_path, config).map_err(to_pyerr)?;
        Ok(PyDatabase {
            inner: std::sync::Arc::new(db),
        })
    }

    /// Create a connection to this database.
    fn connect(&self) -> PyResult<PyConnection> {
        Ok(PyConnection {
            db: std::sync::Arc::clone(&self.inner),
        })
    }

    /// Shorthand: execute a query directly (creates a temporary connection).
    #[pyo3(text_signature = "(self, cypher)")]
    fn execute(&self, cypher: &str) -> PyResult<PyQueryResult> {
        let conn = self.connect()?;
        conn.query(cypher)
    }

    fn __repr__(&self) -> String {
        "<Database>".to_string()
    }
}

// ---------------------------------------------------------------------------
// Module
// ---------------------------------------------------------------------------

/// ClickGraph — embedded graph query engine.
///
/// Run Cypher queries over Parquet, Iceberg, Delta Lake and S3 data
/// without a ClickHouse server.
///
/// Quick start:
///
/// >>> import clickgraph
/// >>> db = clickgraph.Database("schema.yaml")
/// >>> conn = db.connect()
/// >>> for row in conn.query("MATCH (u:User) RETURN u.name LIMIT 5"):
/// ...     print(row["u.name"])
#[pymodule]
fn _clickgraph(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDatabase>()?;
    m.add_class::<PyConnection>()?;
    m.add_class::<PyQueryResult>()?;
    Ok(())
}
