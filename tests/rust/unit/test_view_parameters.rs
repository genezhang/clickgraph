//! Unit tests for parameterized views feature
//!
//! Tests the view_parameters field parsing and storage
//! without requiring a running ClickHouse instance.

#[cfg(test)]
mod view_parameters_tests {
    use std::collections::HashMap;

    use clickgraph::graph_catalog::config::{NodeDefinition, RelationshipDefinition};

    /// Test that NodeDefinition accepts view_parameters field
    #[test]
    fn test_node_definition_with_view_parameters() {
        let yaml = r#"
label: User
database: brahmand
table: users_by_tenant
view_parameters: [tenant_id]
id_column: user_id
property_mappings:
  user_id: user_id
  name: name
"#;

        let node_def: Result<NodeDefinition, _> = serde_yaml::from_str(yaml);
        assert!(
            node_def.is_ok(),
            "Failed to parse NodeDefinition with view_parameters"
        );

        let node = node_def.unwrap();
        assert_eq!(node.label, "User");
        assert_eq!(node.view_parameters, Some(vec!["tenant_id".to_string()]));
    }

    /// Test that NodeDefinition works without view_parameters (backward compatibility)
    #[test]
    fn test_node_definition_without_view_parameters() {
        let yaml = r#"
label: User
database: brahmand
table: users
id_column: user_id
property_mappings:
  user_id: user_id
  name: name
"#;

        let node_def: Result<NodeDefinition, _> = serde_yaml::from_str(yaml);
        assert!(
            node_def.is_ok(),
            "Failed to parse NodeDefinition without view_parameters"
        );

        let node = node_def.unwrap();
        assert_eq!(node.label, "User");
        assert_eq!(node.view_parameters, None);
    }

    /// Test that RelationshipDefinition accepts view_parameters field
    #[test]
    fn test_relationship_definition_with_view_parameters() {
        let yaml = r#"
type: FRIENDS_WITH
database: brahmand
table: friendships_by_tenant
view_parameters: [tenant_id, region]
from_id: user_id_from
to_id: user_id_to
from_node: User
to_node: User
property_mappings:
  friendship_id: friendship_id
"#;

        let rel_def: Result<RelationshipDefinition, _> = serde_yaml::from_str(yaml);
        assert!(
            rel_def.is_ok(),
            "Failed to parse RelationshipDefinition with view_parameters"
        );

        let rel = rel_def.unwrap();
        assert_eq!(rel.type_name, "FRIENDS_WITH");
        assert_eq!(
            rel.view_parameters,
            Some(vec!["tenant_id".to_string(), "region".to_string()])
        );
    }

    /// Test RelationshipDefinition without view_parameters (backward compatibility)
    #[test]
    fn test_relationship_definition_without_view_parameters() {
        let yaml = r#"
type: FOLLOWS
database: brahmand
table: user_follows
from_id: follower_id
to_id: followed_id
from_node: User
to_node: User
property_mappings:
  follow_date: follow_date
"#;

        let rel_def: Result<RelationshipDefinition, _> = serde_yaml::from_str(yaml);
        assert!(
            rel_def.is_ok(),
            "Failed to parse RelationshipDefinition without view_parameters"
        );

        let rel = rel_def.unwrap();
        assert_eq!(rel.type_name, "FOLLOWS");
        assert_eq!(rel.view_parameters, None);
    }

    /// Test multi-parameter node view
    #[test]
    fn test_node_with_multiple_view_parameters() {
        let yaml = r#"
label: Order
database: brahmand
table: orders_by_tenant_region
view_parameters: [tenant_id, region, department]
id_column: order_id
property_mappings:
  order_id: order_id
  amount: amount
"#;

        let node_def: Result<NodeDefinition, _> = serde_yaml::from_str(yaml);
        assert!(node_def.is_ok());

        let node = node_def.unwrap();
        assert_eq!(
            node.view_parameters,
            Some(vec![
                "tenant_id".to_string(),
                "region".to_string(),
                "department".to_string()
            ])
        );
    }

    /// Test that view_parameters field is preserved during serialization
    #[test]
    fn test_view_parameters_serialization_roundtrip() {
        let mut props = HashMap::new();
        props.insert("user_id".to_string(), "user_id".to_string());
        props.insert("name".to_string(), "name".to_string());

        let node_def = NodeDefinition {
            label: "User".to_string(),
            database: "brahmand".to_string(),
            table: "users_by_tenant".to_string(),
            id_column: "user_id".to_string(),
            properties: props,
            view_parameters: Some(vec!["tenant_id".to_string()]),
            use_final: None,
            filter: None,
            auto_discover_columns: false,
            exclude_columns: vec![],
            naming_convention: "snake_case".to_string(),
            from_node_properties: None,
            to_node_properties: None,
        };

        // Serialize to YAML
        let yaml = serde_yaml::to_string(&node_def).unwrap();

        // Deserialize back
        let deserialized: NodeDefinition = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(
            deserialized.view_parameters,
            Some(vec!["tenant_id".to_string()])
        );
        assert_eq!(deserialized.label, "User");
    }

    /// Test empty view_parameters array (edge case)
    #[test]
    fn test_empty_view_parameters_array() {
        let yaml = r#"
label: User
database: brahmand
table: users
view_parameters: []
id_column: user_id
property_mappings:
  user_id: user_id
"#;

        let node_def: Result<NodeDefinition, _> = serde_yaml::from_str(yaml);
        assert!(node_def.is_ok());

        let node = node_def.unwrap();
        // Empty array should deserialize as Some(vec![])
        assert_eq!(node.view_parameters, Some(vec![]));
    }
}
