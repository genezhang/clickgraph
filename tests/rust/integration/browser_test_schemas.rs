//! Shared schema builders for browser interaction tests.
//!
//! Provides four schema variations used by both `browser_expand_tests` and
//! `browser_interaction_tests`.

use std::collections::HashMap;

use clickgraph::graph_catalog::{
    config::Identifier,
    expression_parser::PropertyValue,
    graph_schema::{GraphSchema, NodeIdSchema, NodeSchema, RelationshipSchema},
    schema_types::SchemaType,
};

/// Standard schema: User, Post, FOLLOWS(U→U), AUTHORED(U→P), LIKED(U→P)
pub fn create_standard_schema() -> GraphSchema {
    let mut nodes = HashMap::new();
    let mut relationships = HashMap::new();

    nodes.insert(
        "User".to_string(),
        NodeSchema {
            database: "test".to_string(),
            table_name: "users".to_string(),
            column_names: vec![
                "user_id".to_string(),
                "full_name".to_string(),
                "email_address".to_string(),
            ],
            primary_keys: "user_id".to_string(),
            node_id: NodeIdSchema::single("user_id".to_string(), SchemaType::Integer),
            property_mappings: {
                let mut props = HashMap::new();
                props.insert(
                    "user_id".to_string(),
                    PropertyValue::Column("user_id".to_string()),
                );
                props.insert(
                    "name".to_string(),
                    PropertyValue::Column("full_name".to_string()),
                );
                props.insert(
                    "email".to_string(),
                    PropertyValue::Column("email_address".to_string()),
                );
                props
            },
            node_id_types: None,
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: false,
            from_properties: None,
            to_properties: None,
            denormalized_source_table: None,
            label_column: None,
            label_value: None,
        },
    );

    nodes.insert(
        "Post".to_string(),
        NodeSchema {
            database: "test".to_string(),
            table_name: "posts".to_string(),
            column_names: vec![
                "post_id".to_string(),
                "post_title".to_string(),
                "post_content".to_string(),
            ],
            primary_keys: "post_id".to_string(),
            node_id: NodeIdSchema::single("post_id".to_string(), SchemaType::Integer),
            property_mappings: {
                let mut props = HashMap::new();
                props.insert(
                    "post_id".to_string(),
                    PropertyValue::Column("post_id".to_string()),
                );
                props.insert(
                    "title".to_string(),
                    PropertyValue::Column("post_title".to_string()),
                );
                props.insert(
                    "content".to_string(),
                    PropertyValue::Column("post_content".to_string()),
                );
                props
            },
            node_id_types: None,
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: false,
            from_properties: None,
            to_properties: None,
            denormalized_source_table: None,
            label_column: None,
            label_value: None,
        },
    );

    relationships.insert(
        "FOLLOWS::User::User".to_string(),
        RelationshipSchema {
            database: "test".to_string(),
            table_name: "user_follows".to_string(),
            column_names: vec![
                "follower_id".to_string(),
                "followed_id".to_string(),
                "follow_date".to_string(),
            ],
            from_node: "User".to_string(),
            to_node: "User".to_string(),
            from_node_table: "users".to_string(),
            to_node_table: "users".to_string(),
            from_id: Identifier::from("follower_id"),
            to_id: Identifier::from("followed_id"),
            from_node_id_dtype: SchemaType::Integer,
            to_node_id_dtype: SchemaType::Integer,
            property_mappings: {
                let mut props = HashMap::new();
                props.insert(
                    "follow_date".to_string(),
                    PropertyValue::Column("follow_date".to_string()),
                );
                props
            },
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_label_values: None,
            to_label_values: None,
            from_node_properties: None,
            to_node_properties: None,
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
        },
    );

    relationships.insert(
        "AUTHORED::User::Post".to_string(),
        RelationshipSchema {
            database: "test".to_string(),
            table_name: "post_authors".to_string(),
            column_names: vec!["author_id".to_string(), "post_id".to_string()],
            from_node: "User".to_string(),
            to_node: "Post".to_string(),
            from_node_table: "users".to_string(),
            to_node_table: "posts".to_string(),
            from_id: Identifier::from("author_id"),
            to_id: Identifier::from("post_id"),
            from_node_id_dtype: SchemaType::Integer,
            to_node_id_dtype: SchemaType::Integer,
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_label_values: None,
            to_label_values: None,
            from_node_properties: None,
            to_node_properties: None,
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
        },
    );

    relationships.insert(
        "LIKED::User::Post".to_string(),
        RelationshipSchema {
            database: "test".to_string(),
            table_name: "post_likes".to_string(),
            column_names: vec!["user_id".to_string(), "post_id".to_string()],
            from_node: "User".to_string(),
            to_node: "Post".to_string(),
            from_node_table: "users".to_string(),
            to_node_table: "posts".to_string(),
            from_id: Identifier::from("user_id"),
            to_id: Identifier::from("post_id"),
            from_node_id_dtype: SchemaType::Integer,
            to_node_id_dtype: SchemaType::Integer,
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_label_values: None,
            to_label_values: None,
            from_node_properties: None,
            to_node_properties: None,
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
        },
    );

    GraphSchema::build(1, "test".to_string(), nodes, relationships)
}

/// FK-edge schema: Order, Customer, PLACED_BY(O→C) with is_fk_edge: true
pub fn create_fk_edge_schema() -> GraphSchema {
    let mut nodes = HashMap::new();
    let mut relationships = HashMap::new();

    nodes.insert(
        "Order".to_string(),
        NodeSchema {
            database: "test".to_string(),
            table_name: "orders".to_string(),
            column_names: vec![
                "order_id".to_string(),
                "customer_id".to_string(),
                "order_total".to_string(),
            ],
            primary_keys: "order_id".to_string(),
            node_id: NodeIdSchema::single("order_id".to_string(), SchemaType::Integer),
            property_mappings: {
                let mut props = HashMap::new();
                props.insert(
                    "order_id".to_string(),
                    PropertyValue::Column("order_id".to_string()),
                );
                props.insert(
                    "total".to_string(),
                    PropertyValue::Column("order_total".to_string()),
                );
                props
            },
            node_id_types: None,
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: false,
            from_properties: None,
            to_properties: None,
            denormalized_source_table: None,
            label_column: None,
            label_value: None,
        },
    );

    nodes.insert(
        "Customer".to_string(),
        NodeSchema {
            database: "test".to_string(),
            table_name: "customers".to_string(),
            column_names: vec!["customer_id".to_string(), "customer_name".to_string()],
            primary_keys: "customer_id".to_string(),
            node_id: NodeIdSchema::single("customer_id".to_string(), SchemaType::Integer),
            property_mappings: {
                let mut props = HashMap::new();
                props.insert(
                    "customer_id".to_string(),
                    PropertyValue::Column("customer_id".to_string()),
                );
                props.insert(
                    "name".to_string(),
                    PropertyValue::Column("customer_name".to_string()),
                );
                props
            },
            node_id_types: None,
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: false,
            from_properties: None,
            to_properties: None,
            denormalized_source_table: None,
            label_column: None,
            label_value: None,
        },
    );

    // FK-edge: Order table has customer_id FK, so edge table = orders
    relationships.insert(
        "PLACED_BY::Order::Customer".to_string(),
        RelationshipSchema {
            database: "test".to_string(),
            table_name: "orders".to_string(),
            column_names: vec![
                "order_id".to_string(),
                "customer_id".to_string(),
                "order_total".to_string(),
            ],
            from_node: "Order".to_string(),
            to_node: "Customer".to_string(),
            from_node_table: "orders".to_string(),
            to_node_table: "customers".to_string(),
            from_id: Identifier::from("order_id"),
            to_id: Identifier::from("customer_id"),
            from_node_id_dtype: SchemaType::Integer,
            to_node_id_dtype: SchemaType::Integer,
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_label_values: None,
            to_label_values: None,
            from_node_properties: None,
            to_node_properties: None,
            is_fk_edge: true,
            constraints: None,
            edge_id_types: None,
        },
    );

    GraphSchema::build(1, "test".to_string(), nodes, relationships)
}

/// Denormalized schema: Airport, FLIGHT(A→A) with is_denormalized: true
pub fn create_denormalized_schema() -> GraphSchema {
    let mut nodes = HashMap::new();
    let mut relationships = HashMap::new();

    let mut from_props = HashMap::new();
    from_props.insert("code".to_string(), "origin_code".to_string());
    from_props.insert("city".to_string(), "origin_city".to_string());

    let mut to_props = HashMap::new();
    to_props.insert("code".to_string(), "dest_code".to_string());
    to_props.insert("city".to_string(), "dest_city".to_string());

    nodes.insert(
        "Airport".to_string(),
        NodeSchema {
            database: "test".to_string(),
            table_name: "flights".to_string(),
            column_names: vec![],
            primary_keys: "code".to_string(),
            node_id: NodeIdSchema::single("code".to_string(), SchemaType::String),
            property_mappings: HashMap::new(),
            node_id_types: None,
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: true,
            from_properties: Some(from_props.clone()),
            to_properties: Some(to_props.clone()),
            denormalized_source_table: Some("test.flights".to_string()),
            label_column: None,
            label_value: None,
        },
    );

    relationships.insert(
        "FLIGHT::Airport::Airport".to_string(),
        RelationshipSchema {
            database: "test".to_string(),
            table_name: "flights".to_string(),
            column_names: vec![
                "origin_code".to_string(),
                "dest_code".to_string(),
                "flight_number".to_string(),
            ],
            from_node: "Airport".to_string(),
            to_node: "Airport".to_string(),
            from_node_table: "flights".to_string(),
            to_node_table: "flights".to_string(),
            from_id: Identifier::from("origin_code"),
            to_id: Identifier::from("dest_code"),
            from_node_id_dtype: SchemaType::String,
            to_node_id_dtype: SchemaType::String,
            property_mappings: {
                let mut props = HashMap::new();
                props.insert(
                    "flight_number".to_string(),
                    PropertyValue::Column("flight_number".to_string()),
                );
                props
            },
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_label_values: None,
            to_label_values: None,
            from_node_properties: Some(from_props),
            to_node_properties: Some(to_props),
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
        },
    );

    GraphSchema::build(1, "test".to_string(), nodes, relationships)
}

/// Composite ID schema: Account(composite), Customer, OWNS(C→A), TRANSFERRED(A→A)
pub fn create_composite_id_schema() -> GraphSchema {
    let mut nodes = HashMap::new();
    let mut relationships = HashMap::new();

    nodes.insert(
        "Account".to_string(),
        NodeSchema {
            database: "test".to_string(),
            table_name: "accounts".to_string(),
            column_names: vec![
                "bank_id".to_string(),
                "account_number".to_string(),
                "balance".to_string(),
            ],
            primary_keys: "bank_id,account_number".to_string(),
            node_id: NodeIdSchema::composite(
                vec!["bank_id".to_string(), "account_number".to_string()],
                SchemaType::String,
            ),
            property_mappings: {
                let mut props = HashMap::new();
                props.insert(
                    "bank_id".to_string(),
                    PropertyValue::Column("bank_id".to_string()),
                );
                props.insert(
                    "account_number".to_string(),
                    PropertyValue::Column("account_number".to_string()),
                );
                props.insert(
                    "balance".to_string(),
                    PropertyValue::Column("balance".to_string()),
                );
                props
            },
            node_id_types: None,
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: false,
            from_properties: None,
            to_properties: None,
            denormalized_source_table: None,
            label_column: None,
            label_value: None,
        },
    );

    nodes.insert(
        "Customer".to_string(),
        NodeSchema {
            database: "test".to_string(),
            table_name: "customers".to_string(),
            column_names: vec!["customer_id".to_string(), "customer_name".to_string()],
            primary_keys: "customer_id".to_string(),
            node_id: NodeIdSchema::single("customer_id".to_string(), SchemaType::Integer),
            property_mappings: {
                let mut props = HashMap::new();
                props.insert(
                    "customer_id".to_string(),
                    PropertyValue::Column("customer_id".to_string()),
                );
                props.insert(
                    "name".to_string(),
                    PropertyValue::Column("customer_name".to_string()),
                );
                props
            },
            node_id_types: None,
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: false,
            from_properties: None,
            to_properties: None,
            denormalized_source_table: None,
            label_column: None,
            label_value: None,
        },
    );

    // OWNS: Customer → Account
    relationships.insert(
        "OWNS::Customer::Account".to_string(),
        RelationshipSchema {
            database: "test".to_string(),
            table_name: "account_ownership".to_string(),
            column_names: vec![
                "customer_id".to_string(),
                "bank_id".to_string(),
                "account_number".to_string(),
            ],
            from_node: "Customer".to_string(),
            to_node: "Account".to_string(),
            from_node_table: "customers".to_string(),
            to_node_table: "accounts".to_string(),
            from_id: Identifier::from("customer_id"),
            to_id: Identifier::Composite(vec!["bank_id".to_string(), "account_number".to_string()]),
            from_node_id_dtype: SchemaType::Integer,
            to_node_id_dtype: SchemaType::String,
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_label_values: None,
            to_label_values: None,
            from_node_properties: None,
            to_node_properties: None,
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
        },
    );

    // TRANSFERRED: Account → Account
    relationships.insert(
        "TRANSFERRED::Account::Account".to_string(),
        RelationshipSchema {
            database: "test".to_string(),
            table_name: "transfers".to_string(),
            column_names: vec![
                "from_bank_id".to_string(),
                "from_account_number".to_string(),
                "to_bank_id".to_string(),
                "to_account_number".to_string(),
                "amount".to_string(),
            ],
            from_node: "Account".to_string(),
            to_node: "Account".to_string(),
            from_node_table: "accounts".to_string(),
            to_node_table: "accounts".to_string(),
            from_id: Identifier::Composite(vec![
                "from_bank_id".to_string(),
                "from_account_number".to_string(),
            ]),
            to_id: Identifier::Composite(vec![
                "to_bank_id".to_string(),
                "to_account_number".to_string(),
            ]),
            from_node_id_dtype: SchemaType::String,
            to_node_id_dtype: SchemaType::String,
            property_mappings: {
                let mut props = HashMap::new();
                props.insert(
                    "amount".to_string(),
                    PropertyValue::Column("amount".to_string()),
                );
                props
            },
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_label_values: None,
            to_label_values: None,
            from_node_properties: None,
            to_node_properties: None,
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
        },
    );

    GraphSchema::build(1, "test".to_string(), nodes, relationships)
}

/// Polymorphic schema: User, Post, single `interactions` table with type_column/label columns
/// for FOLLOWS(U→U), LIKES(U→P), AUTHORED(U→P)
pub fn create_polymorphic_schema() -> GraphSchema {
    let mut nodes = HashMap::new();
    let mut relationships = HashMap::new();

    nodes.insert(
        "User".to_string(),
        NodeSchema {
            database: "test".to_string(),
            table_name: "users".to_string(),
            column_names: vec![
                "user_id".to_string(),
                "username".to_string(),
                "email_address".to_string(),
            ],
            primary_keys: "user_id".to_string(),
            node_id: NodeIdSchema::single("user_id".to_string(), SchemaType::Integer),
            property_mappings: {
                let mut props = HashMap::new();
                props.insert(
                    "user_id".to_string(),
                    PropertyValue::Column("user_id".to_string()),
                );
                props.insert(
                    "name".to_string(),
                    PropertyValue::Column("username".to_string()),
                );
                props.insert(
                    "email".to_string(),
                    PropertyValue::Column("email_address".to_string()),
                );
                props
            },
            node_id_types: None,
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: false,
            from_properties: None,
            to_properties: None,
            denormalized_source_table: None,
            label_column: None,
            label_value: None,
        },
    );

    nodes.insert(
        "Post".to_string(),
        NodeSchema {
            database: "test".to_string(),
            table_name: "posts".to_string(),
            column_names: vec![
                "post_id".to_string(),
                "post_title".to_string(),
                "post_content".to_string(),
            ],
            primary_keys: "post_id".to_string(),
            node_id: NodeIdSchema::single("post_id".to_string(), SchemaType::Integer),
            property_mappings: {
                let mut props = HashMap::new();
                props.insert(
                    "post_id".to_string(),
                    PropertyValue::Column("post_id".to_string()),
                );
                props.insert(
                    "title".to_string(),
                    PropertyValue::Column("post_title".to_string()),
                );
                props.insert(
                    "content".to_string(),
                    PropertyValue::Column("post_content".to_string()),
                );
                props
            },
            node_id_types: None,
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: false,
            from_properties: None,
            to_properties: None,
            denormalized_source_table: None,
            label_column: None,
            label_value: None,
        },
    );

    let interaction_props = {
        let mut props = HashMap::new();
        props.insert(
            "created_at".to_string(),
            PropertyValue::Column("timestamp".to_string()),
        );
        props
    };

    let poly_columns = vec![
        "from_id".to_string(),
        "to_id".to_string(),
        "interaction_type".to_string(),
        "from_type".to_string(),
        "to_type".to_string(),
        "timestamp".to_string(),
    ];

    // FOLLOWS: User → User (polymorphic, same interactions table)
    relationships.insert(
        "FOLLOWS::User::User".to_string(),
        RelationshipSchema {
            database: "test".to_string(),
            table_name: "interactions".to_string(),
            column_names: poly_columns.clone(),
            from_node: "User".to_string(),
            to_node: "User".to_string(),
            from_node_table: "users".to_string(),
            to_node_table: "users".to_string(),
            from_id: Identifier::from("from_id"),
            to_id: Identifier::from("to_id"),
            from_node_id_dtype: SchemaType::Integer,
            to_node_id_dtype: SchemaType::Integer,
            property_mappings: interaction_props.clone(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: Some("interaction_type".to_string()),
            from_label_column: Some("from_type".to_string()),
            to_label_column: Some("to_type".to_string()),
            from_label_values: None,
            to_label_values: None,
            from_node_properties: None,
            to_node_properties: None,
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
        },
    );

    // LIKES: User → Post (polymorphic, same interactions table)
    relationships.insert(
        "LIKES::User::Post".to_string(),
        RelationshipSchema {
            database: "test".to_string(),
            table_name: "interactions".to_string(),
            column_names: poly_columns.clone(),
            from_node: "User".to_string(),
            to_node: "Post".to_string(),
            from_node_table: "users".to_string(),
            to_node_table: "posts".to_string(),
            from_id: Identifier::from("from_id"),
            to_id: Identifier::from("to_id"),
            from_node_id_dtype: SchemaType::Integer,
            to_node_id_dtype: SchemaType::Integer,
            property_mappings: interaction_props.clone(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: Some("interaction_type".to_string()),
            from_label_column: Some("from_type".to_string()),
            to_label_column: Some("to_type".to_string()),
            from_label_values: None,
            to_label_values: None,
            from_node_properties: None,
            to_node_properties: None,
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
        },
    );

    // AUTHORED: User → Post (polymorphic, same interactions table)
    relationships.insert(
        "AUTHORED::User::Post".to_string(),
        RelationshipSchema {
            database: "test".to_string(),
            table_name: "interactions".to_string(),
            column_names: poly_columns,
            from_node: "User".to_string(),
            to_node: "Post".to_string(),
            from_node_table: "users".to_string(),
            to_node_table: "posts".to_string(),
            from_id: Identifier::from("from_id"),
            to_id: Identifier::from("to_id"),
            from_node_id_dtype: SchemaType::Integer,
            to_node_id_dtype: SchemaType::Integer,
            property_mappings: interaction_props,
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: Some("interaction_type".to_string()),
            from_label_column: Some("from_type".to_string()),
            to_label_column: Some("to_type".to_string()),
            from_label_values: None,
            to_label_values: None,
            from_node_properties: None,
            to_node_properties: None,
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
        },
    );

    GraphSchema::build(1, "test".to_string(), nodes, relationships)
}
