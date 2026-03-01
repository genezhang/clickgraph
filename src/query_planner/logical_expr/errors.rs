//! Error types for logical expression conversion.
//!
//! These errors occur when Cypher expressions cannot be translated
//! to the internal LogicalExpr representation.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum LogicalExprError {
    #[error("PatternComprehension should have been rewritten during query planning")]
    PatternComprehensionNotRewritten,

    #[error("ListComprehension should have been rewritten during query planning")]
    ListComprehensionNotRewritten,

    #[error("Unsupported expression type: {0}")]
    UnsupportedExpression(String),

    #[error("Invalid conversion: expected {expected}, got {actual}")]
    InvalidConversion { expected: String, actual: String },
}
