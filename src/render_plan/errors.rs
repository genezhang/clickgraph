use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq)]
pub enum RenderBuildError {
    #[error("No From Table.")]
    MissingFromTable,

    #[error("No Select items.")]
    MissingSelectItems,

    #[error("Malformed CTE name.")]
    MalformedCTEName,

    #[error("Unsupported feature: {0}")]
    UnsupportedFeature(String),

    #[error("No relationship tables found for relationship pattern")]
    NoRelationshipTablesFound,

    #[error("Expected exactly one filter but found none")]
    ExpectedSingleFilterButNoneFound,

    #[error("Query is too complex and requires CTE-based processing")]
    ComplexQueryRequiresCTEs,

    #[error("Could not resolve table name: {0}")]
    TableNameNotFound(String),

    #[error("Invalid render plan: {0}")]
    InvalidRenderPlan(String),

    #[error("Cannot resolve node type for pattern: node alias '{0}' has no label and cannot be inferred from relationship schema")]
    CannotResolveNodeType(String),

    #[error("Node schema not found for type '{0}'")]
    NodeSchemaNotFound(String),

    #[error("Node ID column not configured for node type '{0}'")]
    NodeIdColumnNotConfigured(String),
}
