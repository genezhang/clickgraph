use clickhouse::Row;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use serde_json::Value;

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

/// Response for SQL-only queries (no execution)
#[derive(Debug, Serialize)]
pub struct SqlOnlyResponse {
    pub cypher_query: String,
    pub generated_sql: String,
    pub execution_mode: String,
}
// }
