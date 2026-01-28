use clickhouse::Row;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub query: String,
    pub format: Option<OutputFormat>,
    /// If true, return the generated SQL without executing it
    pub sql_only: Option<bool>,
    /// Name of the schema to use for this query (defaults to "default")
    pub schema_name: Option<String>,
    /// Parameters for the query (e.g., {"email": "alice@example.com", "minAge": 25})
    pub parameters: Option<HashMap<String, Value>>,
    /// Tenant ID for multi-tenant deployments (passed to parameterized views)
    pub tenant_id: Option<String>,
    /// View parameters for parameterized views (e.g., {"region": "US", "start_date": "2025-01-01"})
    pub view_parameters: Option<HashMap<String, Value>>,
    /// ClickHouse role name for RBAC via SET ROLE (requires database-managed users with granted roles)
    pub role: Option<String>,
    /// Maximum number of inferred edge types for generic patterns like [*1] (default: 4)
    /// Set higher for GraphRAG use cases with many edge types. Reasonable values: 4-20.
    pub max_inferred_types: Option<usize>,
}

// #[derive(Debug, Serialize)]
// #[serde(untagged)]
// pub enum ResponseRows {
//     Value(Vec<Value>),
//     Other(Vec<String>)
// }

// #[derive(Debug, Serialize)]
// pub struct QueryResponse(pub ResponseRows);

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum OutputFormat {
    JSONEachRow,
    Pretty,
    PrettyCompact,
    Csv,
    CSVWithNames,
}

/// SQL dialect for query generation
/// Currently only ClickHouse is supported, but this enum lays the foundation
/// for future multi-database support (PostgreSQL, DuckDB, MySQL, etc.)
#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Clone)]
#[serde(rename_all = "lowercase")]
pub enum SqlDialect {
    #[serde(rename = "clickhouse")]
    #[default]
    ClickHouse,

    // Future supported databases (not yet implemented - will return UnsupportedDialectError)
    #[serde(rename = "postgresql")]
    PostgreSQL,

    #[serde(rename = "duckdb")]
    DuckDB,

    #[serde(rename = "mysql")]
    MySQL,

    #[serde(rename = "sqlite")]
    SQLite,
}

impl SqlDialect {
    /// Get the string representation of the dialect
    pub fn as_str(&self) -> &'static str {
        match self {
            SqlDialect::ClickHouse => "clickhouse",
            SqlDialect::PostgreSQL => "postgresql",
            SqlDialect::DuckDB => "duckdb",
            SqlDialect::MySQL => "mysql",
            SqlDialect::SQLite => "sqlite",
        }
    }

    /// Check if this dialect is currently supported (only ClickHouse in v0.5.1)
    pub fn is_supported(&self) -> bool {
        matches!(self, SqlDialect::ClickHouse)
    }
}

impl From<OutputFormat> for String {
    fn from(value: OutputFormat) -> Self {
        match value {
            OutputFormat::JSONEachRow => "JSONEachRow".to_string(),
            OutputFormat::Pretty => "Pretty".to_string(),
            OutputFormat::PrettyCompact => "PrettyCompact".to_string(),
            OutputFormat::Csv => "CSV".to_string(),
            OutputFormat::CSVWithNames => "CSVWithNames".to_string(),
        }
    }
}

#[derive(Debug, Row, Serialize, Deserialize)]
pub struct GraphCatalog {
    pub id: u64,
    pub schema_json: String,
}

// #[derive(Debug, Serialize, Deserialize, Clone)]
// pub struct NodeSchema {
//     pub table_name: String,
//     pub column_names: Option<String>,
// }

// #[derive(Debug, Serialize, Deserialize, Clone)]
// pub struct RelationshipSchema {
//     pub table_name: String,
//     pub column_names: Option<String>,
//     pub from_node: String,
//     pub to_node: String

/// Response for SQL-only queries (no execution) - DEPRECATED: Use SqlGenerationResponse
#[derive(Debug, Serialize)]
pub struct SqlOnlyResponse {
    pub cypher_query: String,
    pub generated_sql: String,
    pub execution_mode: String,
}

/// Request for SQL generation API (production endpoint)
#[derive(Debug, Deserialize)]
pub struct SqlGenerationRequest {
    /// Cypher query to translate
    pub query: String,

    /// Target SQL dialect (default: "clickhouse")
    /// Currently only "clickhouse" is supported. Future: postgresql, duckdb, mysql, sqlite
    #[serde(default)]
    pub target_database: SqlDialect,

    /// Schema to use (defaults to "default")
    pub schema_name: Option<String>,

    /// Query parameters ($param in Cypher)
    pub parameters: Option<HashMap<String, Value>>,

    /// View parameters for multi-tenancy (ClickHouse-specific)
    pub view_parameters: Option<HashMap<String, Value>>,

    /// ClickHouse role name for RBAC via SET ROLE (ClickHouse-specific)
    pub role: Option<String>,

    /// Pretty-print SQL with indentation (default: false)
    /// Reserved for future SQL formatting feature
    #[allow(dead_code)]
    pub format_sql: Option<bool>,

    /// Include logical plan in response (default: false)
    pub include_plan: Option<bool>,
}

/// Response for SQL generation API (production endpoint)
#[derive(Debug, Serialize)]
pub struct SqlGenerationResponse {
    /// Original Cypher query
    pub cypher_query: String,

    /// Target database dialect ("clickhouse", "postgresql", etc.)
    pub target_database: String,

    /// Array of SQL statements to execute in order
    /// Examples:
    /// - ["SELECT ..."] - simple query
    /// - ["SET ROLE analyst", "SELECT ..."] - with RBAC (ClickHouse)
    /// - ["CREATE TEMP TABLE ...", "SELECT ...", "DROP TABLE ..."] - future multi-step
    pub sql: Vec<String>,

    /// Query parameters (if any)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<HashMap<String, Value>>,

    /// View parameters (ClickHouse-specific)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub view_parameters: Option<HashMap<String, Value>>,

    /// RBAC role (ClickHouse-specific)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,

    /// Query metadata
    pub metadata: SqlGenerationMetadata,

    /// Logical plan (if include_plan=true)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logical_plan: Option<String>,

    /// Dialect-specific notes or warnings (future use)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dialect_notes: Option<Vec<String>>,
}

/// Metadata for SQL generation response
#[derive(Debug, Serialize)]
pub struct SqlGenerationMetadata {
    /// Type of query: "read", "call", etc.
    pub query_type: String,

    /// Cache status: "HIT", "MISS"
    pub cache_status: String,

    /// Parse time in milliseconds
    pub parse_time_ms: f64,

    /// Planning time in milliseconds
    pub planning_time_ms: f64,

    /// SQL generation time in milliseconds
    pub sql_generation_time_ms: f64,

    /// Total time in milliseconds
    pub total_time_ms: f64,
}

/// Error response for SQL generation API
#[derive(Debug, Serialize)]
pub struct SqlGenerationError {
    /// Original Cypher query
    pub cypher_query: String,

    /// Error message
    pub error: String,

    /// Error type: "ParseError", "PlanningError", "RenderError", "SqlGenerationError"
    pub error_type: String,

    /// Additional error details (if available)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_details: Option<ErrorDetails>,
}

/// Additional error details
#[derive(Debug, Serialize)]
pub struct ErrorDetails {
    /// Position in query string
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position: Option<usize>,

    /// Line number (1-indexed)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line: Option<usize>,

    /// Column number (1-indexed)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<usize>,

    /// Helpful hint for fixing the error
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}
// }
