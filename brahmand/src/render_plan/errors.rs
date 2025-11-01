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
}
