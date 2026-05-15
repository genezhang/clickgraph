use thiserror::Error;

#[derive(Debug, Clone, Error)]
pub enum ClickhouseQueryGeneratorError {
    #[error(
        "Distinct node and connected patterns found. Currently the cross join is not supported."
    )]
    DistinctNodeConnectedPattern,
    #[error("No physical plan generated (likely a query planning phase issue)")]
    NoPhysicalPlan,
    #[error("Column '{0}' not found (check schema configuration)")]
    ColumnNotFound(String),
    #[error("No logical table data found for uid (schema may be misconfigured)")]
    NoLogicalTableDataForUid,
    #[error("WHERE clause is empty (must contain at least one condition)")]
    NoOperandFoundInWhereClause,
    #[error("RETURN clause is empty (must specify at least one expression)")]
    NoOperandFoundInReturnClause,
    #[error("Unsupported expression in WHERE clause (allowed: property access, comparisons, boolean operators)")]
    UnsupportedItemInWhereClause,
    #[error("Unsupported expression in RETURN clause (ensure all referenced variables are bound in MATCH)")]
    UnsupportedItemInReturnClause,
    #[error("Unsupported expression in ORDER BY clause (must reference RETURN expressions)")]
    UnsupportedItemInOrderByClause,
    #[error("Unsupported expression in relationship select clause (relationships can only select properties)")]
    UnsupportedItemInRelSelectClause,
    #[error("Unsupported expression in final node select clause (cannot compute aggregates on final selection)")]
    UnsupportedItemInFinalNodeSelectClause,
    #[error("Unsupported DDL query - ClickGraph is read-only (no CREATE, ALTER, DROP supported)")]
    UnsupportedDDLQuery,
    #[error("Unsupported expression for default value (only literals allowed)")]
    UnsupportedDefaultValue,
    #[error("Primary key is missing in table definition (required for graph operations)")]
    MissingPrimaryKey,
    #[error("Node id column is missing (define 'id' column or specify node_id in schema)")]
    MissingNodeId,
    #[error("Multiple node ids found in table (only one node id column allowed)")]
    MultipleNodeIds,
    #[error("Invalid node id data type (only Int64 and UInt64 allowed)")]
    InvalidNodeIdDType,
    #[error("Invalid node id (ensure id column is properly defined in schema)")]
    InvalidNodeId,
    #[error(
        "Unknown 'From' table in relationship definition (create node table before relationship)"
    )]
    UnknownFromTableInRel,
    #[error(
        "Unknown 'To' table in relationship definition (create node table before relationship)"
    )]
    UnknownToTableInRel,
    #[error("Invalid ClickHouse function name: {0}")]
    InvalidFunctionName(String),
    #[error("Schema error: {0}")]
    SchemaError(String),
}

/// Helper for creating schema errors with context
impl ClickhouseQueryGeneratorError {
    /// Create a SchemaError with context information
    pub fn schema_error_with_context(
        message: impl Into<String>,
        context: impl Into<String>,
    ) -> Self {
        let msg = message.into();
        let ctx = context.into();
        ClickhouseQueryGeneratorError::SchemaError(format!("{}\n  Context: {}", msg, ctx))
    }

    /// Create a ColumnNotFound error with context
    pub fn column_not_found_with_context(
        column: impl Into<String>,
        context: impl Into<String>,
    ) -> Self {
        let col = column.into();
        let ctx = context.into();
        ClickhouseQueryGeneratorError::ColumnNotFound(format!("{} ({})", col, ctx))
    }
}
