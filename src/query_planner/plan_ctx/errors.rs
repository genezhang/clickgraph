//! Error types for planning context operations.
//!
//! These errors occur when required context information is missing
//! during logical plan construction or variable lookup.

use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq)]
pub enum PlanCtxError {
    #[error("No table context for alias `{alias}`")]
    TableCtx { alias: String },

    #[error("No table context for node alias `{alias}`")]
    NodeTableCtx { alias: String },

    #[error("No table context for relationship alias `{alias}`")]
    RelTableCtx { alias: String },

    #[error("Missing label for node `{alias}`")]
    Label { alias: String },

    #[error("Missing type for relationship `{alias}`")]
    Type { alias: String },
}
