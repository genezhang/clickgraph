//! ClickHouse table column metadata querying
//!
//! This module provides utilities for querying table column information from
//! ClickHouse system tables, used for auto-discovery of schema properties.

use clickhouse::Client;
use std::collections::HashMap;

use crate::executor::source_resolver::escape_sql_string;
use log::debug;
use thiserror::Error;

/// Errors that can occur during column metadata queries
#[derive(Debug, Error)]
pub enum ColumnQueryError {
    #[error("Failed to query columns for {database}.{table}: {source}")]
    QueryError {
        database: String,
        table: String,
        source: clickhouse::error::Error,
    },
}

pub type Result<T> = std::result::Result<T, ColumnQueryError>;

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
}

impl ColumnInfo {
    pub fn new(name: String, data_type: String) -> Self {
        Self { name, data_type }
    }
}

/// Query all column names from a ClickHouse table
///
/// Uses system.columns to retrieve column metadata for auto-discovery.
/// Returns column names in their original order.
///
/// # Arguments
/// * `client` - ClickHouse client
/// * `database` - Database name
/// * `table` - Table name
///
/// # Returns
/// Vec of column names, or error if query fails
///
/// # Example
/// ```ignore
/// let columns = query_table_columns(&client, "my_db", "users").await?;
/// // columns = ["user_id", "name", "email", "created_at", ...]
/// ```
pub async fn query_table_columns(
    client: &Client,
    database: &str,
    table: &str,
) -> Result<Vec<String>> {
    #[derive(Debug, serde::Deserialize, clickhouse::Row)]
    struct ColumnName {
        name: String,
    }

    let query = format!(
        "SELECT name FROM system.columns WHERE {}",
        system_columns_predicate(database, table)
    );

    debug!(
        "Querying columns for table {}.{}: {}",
        database, table, query
    );

    let rows: Vec<ColumnName> =
        client
            .query(&query)
            .fetch_all()
            .await
            .map_err(|e| ColumnQueryError::QueryError {
                database: database.to_string(),
                table: table.to_string(),
                source: e,
            })?;

    let columns: Vec<String> = rows.into_iter().map(|row| row.name).collect();

    debug!(
        "Found {} columns for {}.{}: {:?}",
        columns.len(),
        database,
        table,
        columns
    );

    Ok(columns)
}

/// Query column names AND types from a ClickHouse table
///
/// Uses system.columns to retrieve full column metadata including data types.
/// Returns column info in their original order.
///
/// # Arguments
/// * `client` - ClickHouse client
/// * `database` - Database name
/// * `table` - Table name
///
/// # Returns
/// Vec of ColumnInfo (name + type), or error if query fails
///
/// # Example
/// ```ignore
/// let columns = query_table_column_info(&client, "my_db", "users").await?;
/// // columns = [ColumnInfo { name: "user_id", data_type: "UInt64" }, ...]
/// ```
pub async fn query_table_column_info(
    client: &Client,
    database: &str,
    table: &str,
) -> Result<Vec<ColumnInfo>> {
    #[derive(Debug, serde::Deserialize, clickhouse::Row)]
    struct ColumnRow {
        name: String,
        #[serde(rename = "type")]
        data_type: String,
    }

    let query = system_columns_sql(database, table);

    debug!(
        "Querying column info for table {}.{}: {}",
        database, table, query
    );

    let rows: Vec<ColumnRow> =
        client
            .query(&query)
            .fetch_all()
            .await
            .map_err(|e| ColumnQueryError::QueryError {
                database: database.to_string(),
                table: table.to_string(),
                source: e,
            })?;

    let columns: Vec<ColumnInfo> = rows
        .into_iter()
        .map(|row| ColumnInfo::new(row.name, row.data_type))
        .collect();

    debug!(
        "Found {} columns with types for {}.{}: {:?}",
        columns.len(),
        database,
        table,
        columns
    );

    Ok(columns)
}

/// A table whose columns a schema asks to discover (`auto_discover_columns: true`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDiscoveryTarget {
    /// The schema element that asked, for messages: `node 'User'`, `edge 'FOLLOWS'`.
    pub owner: String,
    pub database: String,
    pub table: String,
    /// The element's `source:` URI (embedded chdb reads data from it).
    pub source: Option<String>,
}

/// The columns read from the database for each discovery target, keyed by
/// `(database, table)`. Each loading mode fills this its own way (the server
/// through its ClickHouse client, embedded through its executor) and hands it
/// to `GraphSchemaConfig::to_graph_schema_with_columns`.
#[derive(Debug, Clone, Default)]
pub struct DiscoveredColumns {
    tables: HashMap<(String, String), Vec<ColumnInfo>>,
}

impl DiscoveredColumns {
    pub fn insert(&mut self, database: &str, table: &str, columns: Vec<ColumnInfo>) {
        self.tables
            .insert((database.to_string(), table.to_string()), columns);
    }

    pub fn contains(&self, database: &str, table: &str) -> bool {
        self.tables
            .contains_key(&(database.to_string(), table.to_string()))
    }

    pub fn get(&self, database: &str, table: &str) -> Option<&[ColumnInfo]> {
        self.tables
            .get(&(database.to_string(), table.to_string()))
            .map(Vec::as_slice)
    }
}

fn system_columns_predicate(database: &str, table: &str) -> String {
    format!(
        "database = '{}' AND table = '{}' ORDER BY position",
        escape_sql_string(database),
        escape_sql_string(table)
    )
}

/// ClickHouse SQL listing a table's columns in order, as rows of `name`, `type`.
/// A table that does not exist yields no rows.
pub fn system_columns_sql(database: &str, table: &str) -> String {
    format!(
        "SELECT name, type FROM system.columns WHERE {}",
        system_columns_predicate(database, table)
    )
}

/// ClickHouse SQL listing the columns of a table expression (a table function
/// such as `file('/data/users.parquet', 'Parquet')`), as rows with `name`, `type`.
pub fn describe_columns_sql(table_expression: &str) -> String {
    format!("DESCRIBE TABLE {}", table_expression)
}

/// Reads the rows of [`system_columns_sql`] or [`describe_columns_sql`],
/// executed as JSON objects, into column infos.
pub fn column_info_from_rows(
    rows: &[serde_json::Value],
) -> std::result::Result<Vec<ColumnInfo>, String> {
    rows.iter()
        .map(|row| {
            let field = |key: &str| {
                row.get(key)
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
                    .ok_or_else(|| format!("column metadata row has no string '{}': {}", key, row))
            };
            Ok(ColumnInfo::new(field("name")?, field("type")?))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_columns_sql_escapes_names() {
        assert_eq!(
            system_columns_sql("db", "o'brien"),
            "SELECT name, type FROM system.columns WHERE database = 'db' AND table = 'o\\'brien' ORDER BY position"
        );
    }

    #[test]
    fn column_info_from_rows_reads_name_and_type() {
        let rows = vec![
            serde_json::json!({"name": "user_id", "type": "UInt32", "default_type": ""}),
            serde_json::json!({"name": "full_name", "type": "String"}),
        ];
        let columns = column_info_from_rows(&rows).unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[1].name, "full_name");
        assert_eq!(columns[1].data_type, "String");
        assert!(column_info_from_rows(&[serde_json::json!({"name": "x"})]).is_err());
    }

    #[test]
    fn test_column_info_creation() {
        let col = ColumnInfo::new("user_id".to_string(), "UInt64".to_string());
        assert_eq!(col.name, "user_id");
        assert_eq!(col.data_type, "UInt64");
    }
}
