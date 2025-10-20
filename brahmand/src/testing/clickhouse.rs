//! Mock ClickHouse client for testing

use std::collections::HashMap;
use async_trait::async_trait;
use mockall::mock;

use crate::graph_catalog::schema_validator::ColumnInfo;

mock! {
    pub ClickHouse {
        pub fn query(&self, query: &str) -> Result<Vec<HashMap<String, String>>, anyhow::Error>;
        pub fn get_table_schema(&self, table: &str) -> Result<Vec<ColumnInfo>, anyhow::Error>;
    }
}

/// Create a mock ClickHouse client with test data
pub fn create_mock_client() -> MockClickHouse {
    let mut mock = MockClickHouse::new();
    
    // Mock table schema responses
    mock.expect_get_table_schema()
        .with(mockall::predicate::eq("users"))
        .returning(|_| {
            Ok(vec![
                ColumnInfo {
                    name: "user_id".to_string(),
                    data_type: "UInt64".to_string(),
                },
                ColumnInfo {
                    name: "full_name".to_string(),
                    data_type: "String".to_string(),
                },
                ColumnInfo {
                    name: "age".to_string(),
                    data_type: "UInt8".to_string(),
                },
            ])
        });

    // Mock query responses
    mock.expect_query()
        .with(mockall::predicate::function(|q: &str| q.contains("users")))
        .returning(|_| {
            Ok(vec![
                vec![
                    ("user_id", "1"),
                    ("full_name", "Alice Smith"),
                    ("age", "28"),
                ].into_iter().map(|(k,v)| (k.to_string(), v.to_string())).collect(),
                vec![
                    ("user_id", "2"),
                    ("full_name", "Bob Jones"),
                    ("age", "35"),
                ].into_iter().map(|(k,v)| (k.to_string(), v.to_string())).collect(),
            ])
        });

    mock
}