//! Enhanced mock ClickHouse client for integration testing
//! 
//! This module provides a mock ClickHouse client that can:
//! 1. Return schema information
//! 2. Store and query mock data
//! 3. Execute basic SQL queries

use std::collections::HashMap;
use async_trait::async_trait;
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct MockClickHouseClient {
    tables: HashMap<String, MockTable>,
}

#[derive(Debug, Clone)]
struct MockTable {
    schema: Vec<MockColumn>,
    data: Vec<HashMap<String, MockValue>>,
}

#[derive(Debug, Clone)]
struct MockColumn {
    name: String,
    data_type: String,
}

#[derive(Debug, Clone)]
enum MockValue {
    String(String),
    Integer(i64),
    Float(f64),
    DateTime(String),
    Boolean(bool),
}

impl MockClickHouseClient {
    pub fn new() -> Self {
        let mut client = Self {
            tables: HashMap::new(),
        };
        client.setup_mock_data();
        client
    }

    fn setup_mock_data(&mut self) {
        // Set up users table
        let mut users = MockTable {
            schema: vec![
                MockColumn { name: "user_id".to_string(), data_type: "UInt64".to_string() },
                MockColumn { name: "full_name".to_string(), data_type: "String".to_string() },
                MockColumn { name: "age".to_string(), data_type: "UInt8".to_string() },
                MockColumn { name: "registration_date".to_string(), data_type: "DateTime".to_string() },
                MockColumn { name: "active".to_string(), data_type: "UInt8".to_string() },
            ],
            data: Vec::new(),
        };

        // Add sample users
        users.data.push(
            vec![
                ("user_id", MockValue::Integer(1)),
                ("full_name", MockValue::String("Alice Smith".to_string())),
                ("age", MockValue::Integer(28)),
                ("registration_date", MockValue::DateTime("2024-01-01 00:00:00".to_string())),
                ("active", MockValue::Integer(1)),
            ].into_iter().collect()
        );
        users.data.push(
            vec![
                ("user_id", MockValue::Integer(2)),
                ("full_name", MockValue::String("Bob Jones".to_string())),
                ("age", MockValue::Integer(35)),
                ("registration_date", MockValue::DateTime("2024-02-15 00:00:00".to_string())),
                ("active", MockValue::Integer(1)),
            ].into_iter().collect()
        );

        self.tables.insert("users".to_string(), users);

        // Set up posts table
        let mut posts = MockTable {
            schema: vec![
                MockColumn { name: "post_id".to_string(), data_type: "UInt64".to_string() },
                MockColumn { name: "post_title".to_string(), data_type: "String".to_string() },
                MockColumn { name: "post_content".to_string(), data_type: "String".to_string() },
                MockColumn { name: "creation_timestamp".to_string(), data_type: "DateTime".to_string() },
            ],
            data: Vec::new(),
        };

        // Add sample posts
        posts.data.push(
            vec![
                ("post_id", MockValue::Integer(101)),
                ("post_title", MockValue::String("First Post".to_string())),
                ("post_content", MockValue::String("Hello World".to_string())),
                ("creation_timestamp", MockValue::DateTime("2024-01-10 00:00:00".to_string())),
            ].into_iter().collect()
        );

        self.tables.insert("posts".to_string(), posts);

        // Set up user_follows table
        let mut follows = MockTable {
            schema: vec![
                MockColumn { name: "follower_id".to_string(), data_type: "UInt64".to_string() },
                MockColumn { name: "following_id".to_string(), data_type: "UInt64".to_string() },
                MockColumn { name: "follow_date".to_string(), data_type: "DateTime".to_string() },
            ],
            data: Vec::new(),
        };

        // Add sample follows
        follows.data.push(
            vec![
                ("follower_id", MockValue::Integer(1)),
                ("following_id", MockValue::Integer(2)),
                ("follow_date", MockValue::DateTime("2024-02-01 00:00:00".to_string())),
            ].into_iter().collect()
        );

        self.tables.insert("user_follows".to_string(), follows);
    }

    pub async fn execute(&self, query: &str) -> Result<QueryResult> {
        // Basic SQL parsing - this is a simplified version
        let query = query.trim().to_lowercase();
        
        if query.starts_with("select") {
            self.handle_select(&query)
        } else if query.starts_with("create table") {
            Ok(QueryResult { rows: Vec::new() })
        } else if query.starts_with("insert into") {
            Ok(QueryResult { rows: Vec::new() })
        } else {
            Err(anyhow::anyhow!("Unsupported query type"))
        }
    }

    fn handle_select(&self, query: &str) -> Result<QueryResult> {
        // Very basic SELECT handling - just returns all rows from the first table
        // In a real implementation, this would need proper SQL parsing
        
        if let Some(table_name) = extract_table_name(query) {
            if let Some(table) = self.tables.get(table_name) {
                let mut rows = Vec::new();
                for row_data in &table.data {
                    let mut row = HashMap::new();
                    for (col, value) in row_data {
                        row.insert(col.clone(), value.clone());
                    }
                    rows.push(row);
                }
                Ok(QueryResult { rows })
            } else {
                Err(anyhow::anyhow!("Table not found: {}", table_name))
            }
        } else {
            Err(anyhow::anyhow!("Could not extract table name from query"))
        }
    }
}

#[derive(Debug)]
pub struct QueryResult {
    rows: Vec<HashMap<String, MockValue>>,
}

impl QueryResult {
    pub fn rows(&self) -> usize {
        self.rows.len()
    }

    pub fn get_row(&self, index: usize) -> Option<&HashMap<String, MockValue>> {
        self.rows.get(index)
    }
}

fn extract_table_name(query: &str) -> Option<&str> {
    // Very basic extraction - assumes "FROM table_name" format
    if let Some(from_idx) = query.find("from") {
        let after_from = &query[from_idx + 4..];
        let end = after_from.find(char::is_whitespace).unwrap_or(after_from.len());
        Some(&after_from[..end].trim())
    } else {
        None
    }
}