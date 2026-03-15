//! Error types for the embedded API.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum EmbeddedError {
    #[error("Schema error: {0}")]
    Schema(String),

    #[error("Query error: {0}")]
    Query(String),

    #[error("Executor error: {0}")]
    Executor(String),

    #[error("I/O error: {0}")]
    Io(String),

    #[error("Validation error: {0}")]
    Validation(String),
}

impl From<clickgraph::executor::ExecutorError> for EmbeddedError {
    fn from(e: clickgraph::executor::ExecutorError) -> Self {
        EmbeddedError::Executor(e.to_string())
    }
}
