#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_catalog::config::{
        EdgeDefinition, GraphSchemaConfig, GraphSchemaDefinition, Identifier, NodeDefinition,
        PolymorphicEdgeDefinition, StandardEdgeDefinition,
    };
    use std::collections::HashMap;

    #[test]
    fn test_denormalized_schema_validation_success() {
        // Valid denormalized schema (OnTime-style)
        let config = GraphSchemaConfig {
            name: Some("ontime".to_string()),
            graph_schema: GraphSchemaDefinition {
                nodes: vec![NodeDefinition {
                    label: "Airport".to_string(),
                    database: "brahmand".to_string(),
                    table: "ontime_flights".to_string(),
                    id_column: "airport_code".to_string(),
                    properties: HashMap::new(),
                    view_parameters: None,
                    use_final: None,
                    auto_discover_columns: false,
                    exclude_columns: vec![],
                    naming_convention: "snake_case".to_string(),
                }],
                relationships: vec![],
                edges: vec![EdgeDefinition::Standard(StandardEdgeDefinition {
                    type_name: "FLIGHT".to_string(),
                    database: "brahmand".to_string(),
                    table: "ontime_flights".to_string(),
                    from_id: "Origin".to_string(),
                    to_id: "Dest".to_string(),
                    from_node: "Airport".to_string(),
                    to_node: "Airport".to_string(),
                    edge_id: Some(Identifier::Composite(vec![
                        "FlightDate".to_string(),
                        "FlightNum".to_string(),
                        "Origin".to_string(),
                        "Dest".to_string(),
                    ])),
                    from_node_properties: Some({
                        let mut props = HashMap::new();
                        props.insert("city".to_string(), "OriginCityName".to_string());
                        props.insert("state".to_string(), "OriginState".to_string());
                        props
                    }),
                    to_node_properties: Some({
                        let mut props = HashMap::new();
                        props.insert("city".to_string(), "DestCityName".to_string());
                        props.insert("state".to_string(), "DestState".to_string());
                        props
                    }),
                    properties: HashMap::new(),
                    view_parameters: None,
                    use_final: None,
                    auto_discover_columns: false,
                    exclude_columns: vec![],
                    naming_convention: "snake_case".to_string(),
                })],
            },
        };

        // Should pass validation
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_denormalized_schema_validation_missing_from_properties() {
        // Invalid: denormalized but missing from_node_properties
        let config = GraphSchemaConfig {
            name: Some("ontime_invalid".to_string()),
            graph_schema: GraphSchemaDefinition {
                nodes: vec![NodeDefinition {
                    label: "Airport".to_string(),
                    database: "brahmand".to_string(),
                    table: "ontime_flights".to_string(),
                    id_column: "airport_code".to_string(),
                    properties: HashMap::new(),
                    view_parameters: None,
                    use_final: None,
                    auto_discover_columns: false,
                    exclude_columns: vec![],
                    naming_convention: "snake_case".to_string(),
                }],
                relationships: vec![],
                edges: vec![EdgeDefinition::Standard(StandardEdgeDefinition {
                    type_name: "FLIGHT".to_string(),
                    database: "brahmand".to_string(),
                    table: "ontime_flights".to_string(),
                    from_id: "Origin".to_string(),
                    to_id: "Dest".to_string(),
                    from_node: "Airport".to_string(),
                    to_node: "Airport".to_string(),
                    edge_id: None,
                    from_node_properties: None, // Missing!
                    to_node_properties: Some({
                        let mut props = HashMap::new();
                        props.insert("city".to_string(), "DestCityName".to_string());
                        props
                    }),
                    properties: HashMap::new(),
                    view_parameters: None,
                    use_final: None,
                    auto_discover_columns: false,
                    exclude_columns: vec![],
                    naming_convention: "snake_case".to_string(),
                })],
            },
        };

        // Should fail validation
        let result = config.validate();
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("missing from_node_properties"));
    }

    #[test]
    fn test_polymorphic_schema_validation_success() {
        let config = GraphSchemaConfig {
            name: Some("social_poly".to_string()),
            graph_schema: GraphSchemaDefinition {
                nodes: vec![
                    NodeDefinition {
                        label: "User".to_string(),
                        database: "brahmand".to_string(),
                        table: "users".to_string(),
                        id_column: "user_id".to_string(),
                        properties: HashMap::new(),
                        view_parameters: None,
                        use_final: None,
                        auto_discover_columns: false,
                        exclude_columns: vec![],
                        naming_convention: "snake_case".to_string(),
                    },
                ],
                relationships: vec![],
                edges: vec![EdgeDefinition::Polymorphic(PolymorphicEdgeDefinition {
                    polymorphic: true,
                    database: "brahmand".to_string(),
                    table: "interactions".to_string(),
                    from_id: "from_id".to_string(),
                    to_id: "to_id".to_string(),
                    type_column: "interaction_type".to_string(),
                    from_label_column: "from_type".to_string(),
                    to_label_column: "to_type".to_string(),
                    type_values: Some(vec!["FOLLOWS".to_string(), "LIKES".to_string()]),
                    edge_id: Some(Identifier::Composite(vec![
                        "from_id".to_string(),
                        "to_id".to_string(),
                        "interaction_type".to_string(),
                        "timestamp".to_string(),
                    ])),
                    properties: HashMap::new(),
                    view_parameters: None,
                    use_final: None,
                })],
            },
        };

        // Should pass validation
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_polymorphic_schema_validation_missing_type_column() {
        let config = GraphSchemaConfig {
            name: Some("social_invalid".to_string()),
            graph_schema: GraphSchemaDefinition {
                nodes: vec![],
                relationships: vec![],
                edges: vec![EdgeDefinition::Polymorphic(PolymorphicEdgeDefinition {
                    polymorphic: true,
                    database: "brahmand".to_string(),
                    table: "interactions".to_string(),
                    from_id: "from_id".to_string(),
                    to_id: "to_id".to_string(),
                    type_column: "".to_string(), // Empty!
                    from_label_column: "from_type".to_string(),
                    to_label_column: "to_type".to_string(),
                    type_values: None,
                    edge_id: None,
                    properties: HashMap::new(),
                    view_parameters: None,
                    use_final: None,
                })],
            },
        };

        // Should fail validation
        let result = config.validate();
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("type_column"));
    }

    #[test]
    fn test_composite_identifier() {
        let single = Identifier::Single("id".to_string());
        assert!(!single.is_composite());
        assert_eq!(single.columns(), vec!["id"]);

        let composite = Identifier::Composite(vec![
            "col1".to_string(),
            "col2".to_string(),
            "col3".to_string(),
        ]);
        assert!(composite.is_composite());
        assert_eq!(composite.columns(), vec!["col1", "col2", "col3"]);
    }
}
