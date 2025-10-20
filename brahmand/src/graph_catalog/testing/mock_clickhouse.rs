//! Test utilities for ClickHouse schema validation
use std::collections::HashMap;
use crate::graph_catalog::column_info::ColumnInfo;

/// Create test table schemas for unit tests
pub fn create_test_table_schemas() -> HashMap<String, Vec<ColumnInfo>> {
    let mut tables = HashMap::new();
    
    // Mock users table
    tables.insert(
        "users".to_string(),
        vec![
            ColumnInfo::new("user_id".to_string(), "UInt64".to_string()),
            ColumnInfo::new("full_name".to_string(), "String".to_string()),
            ColumnInfo::new("email_address".to_string(), "String".to_string()),
            ColumnInfo::new("registration_date".to_string(), "DateTime".to_string()),
            ColumnInfo::new("is_active".to_string(), "UInt8".to_string()),
        ],
    );

    // Mock posts table
    tables.insert(
        "posts".to_string(),
        vec![
            ColumnInfo::new("post_id".to_string(), "UInt64".to_string()),
            ColumnInfo::new("author_id".to_string(), "UInt64".to_string()),
            ColumnInfo::new("post_title".to_string(), "String".to_string()),
            ColumnInfo::new("post_content".to_string(), "String".to_string()),
            ColumnInfo::new("post_date".to_string(), "DateTime".to_string()),
        ],
    );

    // Mock follows table
    tables.insert(
        "follows".to_string(),
        vec![
            ColumnInfo::new("follower_id".to_string(), "UInt64".to_string()),
            ColumnInfo::new("followed_id".to_string(), "UInt64".to_string()),
            ColumnInfo::new("created_at".to_string(), "DateTime".to_string()),
        ],
    );

    tables
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_test_table_schemas() {
        let tables = create_test_table_schemas();
        
        assert!(tables.contains_key("users"));
        assert!(tables.contains_key("posts"));
        assert!(tables.contains_key("follows"));
        
        let users_schema = tables.get("users").unwrap();
        assert_eq!(users_schema.len(), 5);
        assert_eq!(users_schema[0].name, "user_id");
        assert_eq!(users_schema[0].data_type, "UInt64");
    }
}