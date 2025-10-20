//! Mock ClickHouse client for testing graph view validation.
//! 
//! This module provides a mock ClickHouse client that simulates database schema queries
//! for testing purposes. It includes predefined schemas for common test tables and
//! handles error cases for invalid tables.
//! 
//! # Test Schema
//! 
//! ## Users Table
//! ```sql
//! CREATE TABLE users (
//!     user_id UInt64,
//!     full_name String,
//!     email_address String,
//!     registration_date DateTime,
//!     is_active UInt8
//! )
//! ```
//! 
//! ## Posts Table
//! ```sql
//! CREATE TABLE posts (
//!     post_id UInt64,
//!     author_id UInt64,
//!     post_title String,
//!     post_content String,
//!     post_date DateTime
//! )
//! ```
//! 
//! # Usage
//! 
//! ```rust,no_run
//! use crate::graph_catalog::tests::mock_clickhouse::mock_clickhouse_client;
//! 
//! #[tokio::test]
//! async fn test_schema_validation() {
//!     let client = mock_clickhouse_client();
//!     // Use client for testing...
//! }
//! ```

use std::collections::HashMap;
use clickhouse::Client;
use mockall::mock;
use mockall::predicate::*;

use crate::graph_catalog::schema_validator::ColumnInfo;

// Mock the ClickHouse client for testing
mock! {
    pub Client {
        pub fn query(&self, query: &str) -> MockQueryResult;
    }

    pub QueryResult {
        pub async fn fetch_all<T>(&self) -> Result<Vec<T>, clickhouse::error::Error> where T: 'static;
    }
}

/// Test helper to create a mock ClickHouse client with predefined schema responses
pub fn mock_clickhouse_client() -> MockClient {
    let mut client = MockClient::new();
    
    // Set up mock responses for table schemas
    let mut table_schemas = HashMap::new();
    
    // Mock users table schema
    table_schemas.insert(
        "users",
        vec![
            ColumnInfo {
                name: "user_id".to_string(),
                data_type: "UInt64".to_string(),
            },
            ColumnInfo {
                name: "full_name".to_string(),
                data_type: "String".to_string(),
            },
            ColumnInfo {
                name: "email_address".to_string(),
                data_type: "String".to_string(),
            },
            ColumnInfo {
                name: "registration_date".to_string(),
                data_type: "DateTime".to_string(),
            },
            ColumnInfo {
                name: "is_active".to_string(),
                data_type: "UInt8".to_string(),
            },
        ],
    );

    // Mock posts table schema
    table_schemas.insert(
        "posts",
        vec![
            ColumnInfo {
                name: "post_id".to_string(),
                data_type: "UInt64".to_string(),
            },
            ColumnInfo {
                name: "author_id".to_string(),
                data_type: "UInt64".to_string(),
            },
            ColumnInfo {
                name: "post_title".to_string(),
                data_type: "String".to_string(),
            },
            ColumnInfo {
                name: "post_content".to_string(),
                data_type: "String".to_string(),
            },
            ColumnInfo {
                name: "post_date".to_string(),
                data_type: "DateTime".to_string(),
            },
        ],
    );

    // Set up query expectations
    for (table_name, columns) in table_schemas {
        let mut result = MockQueryResult::new();
        result.expect_fetch_all()
            .returning(move |_| Ok(columns.clone()));

        client
            .expect_query()
            .with(eq(format!("DESC TABLE {}", table_name)))
            .returning(move |_| MockQueryResult::new());
    }

    // Add expectation for non-existent table
    let mut error_result = MockQueryResult::new();
    error_result
        .expect_fetch_all::<ColumnInfo>()
        .returning(|_| Err(clickhouse::error::Error::Custom("Table not found".to_string())));

    client
        .expect_query()
        .with(eq("DESC TABLE non_existent_table"))
        .returning(move |_| error_result.clone());

    client
}