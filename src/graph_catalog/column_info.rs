//! ClickHouse table column metadata querying
//!
//! This module provides utilities for querying table column information from
//! ClickHouse system tables, used for auto-discovery of schema properties.

use clickhouse::Client;
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
        "SELECT name FROM system.columns WHERE database = '{}' AND table = '{}' ORDER BY position",
        database, table
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

    let query = format!(
        "SELECT name, type FROM system.columns WHERE database = '{}' AND table = '{}' ORDER BY position",
        database, table
    );

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_info_creation() {
        let col = ColumnInfo::new("user_id".to_string(), "UInt64".to_string());
        assert_eq!(col.name, "user_id");
        assert_eq!(col.data_type, "UInt64");
    }
}
