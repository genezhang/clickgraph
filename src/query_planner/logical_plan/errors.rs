use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq)]
pub enum LogicalPlanError {
    #[error(
        "Empty node or relationship found. Currently it is not supported. This will change in future."
    )]
    EmptyNode,
    #[error("Parameters are not yet supported in properties.")]
    FoundParamInProperties,
    #[error("Disconnected pattern found.")]
    DisconnectedPatternFound,
    #[error("Node with label {0} not found")]
    NodeNotFound(String),
    #[error("Relationship with type {0} not found")]
    RelationshipNotFound(String),
    #[error("Too many possible types for inference: {count} types found ({types}), max allowed is {max}. Please specify an explicit type to avoid excessive UNION branches.")]
    TooManyInferredTypes {
        count: usize,
        max: usize,
        types: String,
    },
    #[error("Ambiguous pattern: {0}")]
    AmbiguousPattern(String),
    #[error("WITH clause validation error: {0}")]
    WithClauseValidation(String),
    #[error("Query planning error: {0}")]
    QueryPlanningError(String),
}

impl From<crate::query_planner::logical_expr::errors::LogicalExprError> for LogicalPlanError {
    fn from(err: crate::query_planner::logical_expr::errors::LogicalExprError) -> Self {
        LogicalPlanError::QueryPlanningError(format!("Logical expression error: {}", err))
    }
}
