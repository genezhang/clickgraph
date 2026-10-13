use thiserror::Error;

#[derive(Debug, Error)]
pub enum QueryPlannerError {
    #[error("Unsupported procedure: {procedure}")]
    UnsupportedProcedure { procedure: String },
    #[error("Invalid query: {0}")]
    InvalidQuery(String),
}
