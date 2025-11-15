use super::*;
use crate::graph_catalog::tests::mock_clickhouse::mock_clickhouse_client;

mod mock_clickhouse;

#[tokio::test]
async fn test_valid_node_view_mapping() {
    let client = mock_clickhouse_client();
    let mut validator = SchemaValidator::new(client);

    let mapping = NodeViewMapping {
        source_table: "users".to_string(),
        id_column: "user_id".to_string(),
        property_mappings: {
            let mut map = HashMap::new();
            map.insert("name".to_string(), "full_name".to_string());
            map.insert("email".to_string(), "email_address".to_string());
            map
        },
        label: "User".to_string(),
        filter_condition: Some("is_active = 1".to_string()),
    };

    assert!(validator.validate_node_mapping(&mapping).await.is_ok());
}

#[tokio::test]
async fn test_invalid_table_node_mapping() {
    let client = mock_clickhouse_client();
    let mut validator = SchemaValidator::new(client);

    let mapping = NodeViewMapping {
        source_table: "non_existent_table".to_string(),
        id_column: "id".to_string(),
        property_mappings: HashMap::new(),
        label: "Invalid".to_string(),
        filter_condition: None,
    };

    assert!(matches!(
        validator.validate_node_mapping(&mapping).await,
        Err(GraphSchemaError::InvalidSourceTable { .. })
    ));
}

#[tokio::test]
async fn test_invalid_column_node_mapping() {
    let client = mock_clickhouse_client();
    let mut validator = SchemaValidator::new(client);

    let mapping = NodeViewMapping {
        source_table: "users".to_string(),
        id_column: "non_existent_column".to_string(),
        property_mappings: HashMap::new(),
        label: "User".to_string(),
        filter_condition: None,
    };

    assert!(matches!(
        validator.validate_node_mapping(&mapping).await,
        Err(GraphSchemaError::InvalidColumn { .. })
    ));
}

#[tokio::test]
async fn test_valid_relationship_view_mapping() {
    let client = mock_clickhouse_client();
    let mut validator = SchemaValidator::new(client);

    let mapping = RelationshipViewMapping {
        source_table: "posts".to_string(),
        from_column: "author_id".to_string(),
        to_column: "post_id".to_string(),
        property_mappings: {
            let mut map = HashMap::new();
            map.insert("created_at".to_string(), "post_date".to_string());
            map
        },
        type_name: "AUTHORED".to_string(),
        filter_condition: None,
    };

    assert!(validator.validate_relationship_mapping(&mapping).await.is_ok());
}

#[tokio::test]
async fn test_invalid_relationship_column_mapping() {
    let client = mock_clickhouse_client();
    let mut validator = SchemaValidator::new(client);

    let mapping = RelationshipViewMapping {
        source_table: "posts".to_string(),
        from_column: "non_existent_column".to_string(),
        to_column: "post_id".to_string(),
        property_mappings: HashMap::new(),
        type_name: "INVALID".to_string(),
        filter_condition: None,
    };

    assert!(matches!(
        validator.validate_relationship_mapping(&mapping).await,
        Err(GraphSchemaError::InvalidColumn { .. })
    ));
}
