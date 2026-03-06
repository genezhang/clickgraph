use thiserror::Error;

#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("Query execution failed: {0}")]
    QueryFailed(String),

    #[error("I/O error while reading results: {0}")]
    Io(String),

    #[error("Failed to parse query result: {0}")]
    Parse(String),
}
