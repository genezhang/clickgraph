use thiserror::Error;

#[derive(Debug, Error)]
pub enum LogicalExprError {
    #[error("PatternComprehension should have been rewritten during query planning")]
    PatternComprehensionNotRewritten,

    #[error("Unsupported expression type: {0}")]
    UnsupportedExpression(String),

    #[error("Invalid conversion: expected {expected}, got {actual}")]
    InvalidConversion { expected: String, actual: String },
}