use thiserror::Error;

#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("Query execution failed: {0}")]
    QueryFailed(String),

    #[error("I/O error while reading results: {0}")]
    Io(String),

    #[error("Failed to parse query result: {0}")]
    Parse(String),

    /// The remote backend rejected the request or returned a non-success
    /// HTTP status. Used by the Databricks executor; ClickHouse paths
    /// keep collapsing onto `QueryFailed` to preserve existing log
    /// shapes.
    #[error("Remote backend error ({status}): {body}")]
    Remote { status: u16, body: String },

    /// The requested output format isn't supported by the active
    /// backend. Databricks' Statement Execution API doesn't have
    /// ClickHouse's `Pretty`/`CSV`/`JSONEachRow` format names; the
    /// executor only supports JSON-shaped results for now.
    #[error("Output format `{0}` is not supported by this executor")]
    UnsupportedFormat(String),
}
