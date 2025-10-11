use std::fs;
use std::path::PathBuf;
use super::*;
use crate::graph_catalog::tests::mock_clickhouse::mock_clickhouse_client;

#[tokio::test]
async fn test_valid_config_with_schema_validation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let config_path = temp_dir.path().join("test_config.yaml");

    // Create test configuration file
    fs::write(
        &config_path,
        r#"
name: test_view
version: "1.0"
views:
  - name: user_graph
    nodes:
      user:
        source_table: users
        id_column: user_id
        property_mappings:
          name: full_name
          email: email_address
        label: User
        filter_condition: "is_active = 1"
    relationships:
      authored:
        source_table: posts
        from_column: author_id
        to_column: post_id
        property_mappings:
          created_at: post_date
        type_name: AUTHORED
"#,
    )
    .unwrap();

    let client = mock_clickhouse_client();
    let mut validator = SchemaValidator::new(client);

    let config = GraphViewConfig::from_yaml_file_validated(&config_path, &mut validator)
        .await
        .unwrap();

    assert_eq!(config.name, "test_view");
    assert_eq!(config.version, "1.0");
    assert_eq!(config.views.len(), 1);
}

#[tokio::test]
async fn test_invalid_config_with_schema_validation() {
    let temp_dir = tempfile::tempdir().unwrap();
    let config_path = temp_dir.path().join("invalid_config.yaml");

    // Create test configuration with invalid table
    fs::write(
        &config_path,
        r#"
name: test_view
version: "1.0"
views:
  - name: invalid_graph
    nodes:
      user:
        source_table: non_existent_table
        id_column: user_id
        property_mappings: {}
        label: User
    relationships: {}
"#,
    )
    .unwrap();

    let client = mock_clickhouse_client();
    let mut validator = SchemaValidator::new(client);

    let result = GraphViewConfig::from_yaml_file_validated(&config_path, &mut validator).await;
    assert!(matches!(result, Err(GraphSchemaError::InvalidSourceTable { .. })));
}