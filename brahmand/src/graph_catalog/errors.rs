use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq)]
pub enum GraphSchemaError {
    #[error("No relationship schema found for `{rel_label}`.")]
    Relation { rel_label: String },
    #[error("No node schema found for `{node_label}`")]
    Node { node_label: String },
    #[error("No relationship index schema found for `{rel_label}`.")]
    RelationIndex { rel_label: String },
    #[error("Invalid view mapping: source table '{table}' does not exist")]
    InvalidSourceTable { table: String },
    #[error("Invalid view mapping: column '{column}' not found in table '{table}'")]
    InvalidColumn { column: String, table: String },
    #[error("Invalid view mapping: ID column '{column}' has incompatible type in table '{table}'")]
    InvalidIdColumnType { column: String, table: String },
    #[error("Invalid view mapping: cannot resolve node reference '{node}' for relationship")]
    InvalidNodeReference { node: String },
    #[error("Failed to read configuration file: {error}")]
    ConfigReadError { error: String },
    #[error("Failed to parse configuration: {error}")]
    ConfigParseError { error: String },
    #[error("Invalid configuration: {message}")]
    InvalidConfig { message: String },
}
