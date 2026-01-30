//! Unit tests for MATCH clause processing.
//!
//! These tests cover:
//! - Property conversion to operator applications
//! - Node pattern traversal
//! - Connected pattern traversal
//! - Relationship type inference
//! - Node label inference from schema
//! - Polymorphic edge handling

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::open_cypher_parser::ast;
use crate::query_planner::logical_expr::{
    Direction, Literal, LogicalExpr, Operator, Property, PropertyKVPair,
};
use crate::query_planner::logical_plan::{errors::LogicalPlanError, generate_id, LogicalPlan};
use crate::query_planner::plan_ctx::{PlanCtx, TableCtx};

// Import type inference functions from analyzer
use crate::query_planner::analyzer::match_type_inference::{
    infer_node_label_from_schema, infer_relationship_type_from_nodes,
};

// Import from parent module (match_clause)
use super::{convert_properties, convert_properties_to_operator_application, generate_scan};

// Import internal functions from traversal module
use super::traversal::{evaluate_match_clause, traverse_connected_pattern, traverse_node_pattern};

#[test]
fn test_convert_properties_with_kv_pairs() {
    let properties = vec![
        Property::PropertyKV(PropertyKVPair {
            key: "name".to_string(),
            value: LogicalExpr::Literal(Literal::String("John".to_string())),
        }),
        Property::PropertyKV(PropertyKVPair {
            key: "age".to_string(),
            value: LogicalExpr::Literal(Literal::Integer(30)),
        }),
    ];

    let result = convert_properties(properties, "n").unwrap();
    assert_eq!(result.len(), 2);

    // Check first property conversion
    match &result[0] {
        LogicalExpr::OperatorApplicationExp(op_app) => {
            assert_eq!(op_app.operator, Operator::Equal);
            assert_eq!(op_app.operands.len(), 2);
            match &op_app.operands[0] {
                LogicalExpr::PropertyAccessExp(prop) => {
                    assert_eq!(prop.table_alias.0, "n");
                    match &prop.column {
                        PropertyValue::Column(col) => assert_eq!(col, "name"),
                        _ => panic!("Expected Column property"),
                    }
                }
                _ => panic!("Expected PropertyAccessExp"),
            }
            match &op_app.operands[1] {
                LogicalExpr::Literal(Literal::String(s)) => assert_eq!(s, "John"),
                _ => panic!("Expected String literal"),
            }
        }
        _ => panic!("Expected OperatorApplication"),
    }

    // Check second property conversion
    match &result[1] {
        LogicalExpr::OperatorApplicationExp(op_app) => {
            assert_eq!(op_app.operator, Operator::Equal);
            match &op_app.operands[1] {
                LogicalExpr::Literal(Literal::Integer(age)) => assert_eq!(*age, 30),
                _ => panic!("Expected Integer literal"),
            }
        }
        _ => panic!("Expected OperatorApplication"),
    }
}

#[test]
fn test_convert_properties_with_param_returns_error() {
    let properties = vec![
        Property::PropertyKV(PropertyKVPair {
            key: "name".to_string(),
            value: LogicalExpr::Literal(Literal::String("Alice".to_string())),
        }),
        Property::Param("param1".to_string()),
    ];

    let result = convert_properties(properties, "n");
    assert!(result.is_err());
    match result.unwrap_err() {
        LogicalPlanError::FoundParamInProperties => (), // Expected error
        _ => panic!("Expected FoundParamInProperties error"),
    }
}

#[test]
fn test_convert_properties_empty_list() {
    let properties = vec![];
    let result = convert_properties(properties, "n").unwrap();
    assert_eq!(result.len(), 0);
}

#[test]
fn test_generate_id_uniqueness() {
    let id1 = generate_id();
    let id2 = generate_id();

    // IDs should be unique
    assert_ne!(id1, id2);

    // IDs should start with 't' (simple format: t1, t2, t3...)
    assert!(id1.starts_with('t'));
    assert!(id2.starts_with('t'));

    // IDs should be reasonable length (t1 to t999999+)
    assert!(id1.len() >= 2 && id1.len() < 10);
    assert!(id2.len() >= 2 && id2.len() < 10);
}

#[test]
fn test_traverse_node_pattern_new_node() {
    let graph_schema = create_test_schema_with_relationships();
    let mut plan_ctx = PlanCtx::new(Arc::new(graph_schema));
    let initial_plan = Arc::new(LogicalPlan::Empty);

    let node_pattern = ast::NodePattern {
        name: Some("customer"),
        labels: Some(vec!["Person"]),
        properties: Some(vec![ast::Property::PropertyKV(ast::PropertyKVPair {
            key: "city",
            value: ast::Expression::Literal(ast::Literal::String("Boston")),
        })]),
    };

    let result = traverse_node_pattern(&node_pattern, initial_plan.clone(), &mut plan_ctx).unwrap();

    // Should return a GraphNode plan
    match result.as_ref() {
        LogicalPlan::GraphNode(graph_node) => {
            assert_eq!(graph_node.alias, "customer");
            // Input should be a ViewScan
            match graph_node.input.as_ref() {
                LogicalPlan::ViewScan(_view_scan) => {
                    // ViewScan created successfully via try_generate_view_scan
                    // This happens when GLOBAL_GRAPH_SCHEMA is available
                }
                _ => panic!("Expected ViewScan as input"),
            }
        }
        _ => panic!("Expected GraphNode"),
    }

    // Should have added entry to plan context
    let table_ctx = plan_ctx.get_table_ctx("customer").unwrap();
    assert_eq!(table_ctx.get_label_opt(), Some("Person".to_string()));
    // Note: properties get moved to filters after convert_properties_to_operator_application
    assert!(table_ctx.is_explicit_alias());
}

#[test]
fn test_traverse_node_pattern_existing_node() {
    let mut plan_ctx = PlanCtx::new_empty();
    let initial_plan = Arc::new(LogicalPlan::Empty);

    // Pre-populate plan context with existing node
    plan_ctx.insert_table_ctx(
        "customer".to_string(),
        TableCtx::build(
            "customer".to_string(),
            Some("User".to_string()).map(|l| vec![l]),
            vec![],
            false,
            true,
        ),
    );

    let node_pattern = ast::NodePattern {
        name: Some("customer"),
        labels: Some(vec!["Person"]), // Different label
        properties: Some(vec![ast::Property::PropertyKV(ast::PropertyKVPair {
            key: "age",
            value: ast::Expression::Literal(ast::Literal::Integer(25)),
        })]),
    };

    let result = traverse_node_pattern(&node_pattern, initial_plan.clone(), &mut plan_ctx).unwrap();

    // Should return the same plan (not create new GraphNode)
    assert_eq!(result, initial_plan);

    // Should have updated the existing table context
    let table_ctx = plan_ctx.get_table_ctx("customer").unwrap();
    assert_eq!(table_ctx.get_label_opt(), Some("Person".to_string())); // Label should be updated
                                                                       // Note: properties get moved to filters after convert_properties_to_operator_application
}

#[test]
fn test_traverse_node_pattern_empty_node_error() {
    let mut plan_ctx = PlanCtx::new_empty();
    let initial_plan = Arc::new(LogicalPlan::Empty);

    let node_pattern = ast::NodePattern {
        name: None, // Empty node
        labels: Some(vec!["Person"]),
        properties: None,
    };

    let result = traverse_node_pattern(&node_pattern, initial_plan, &mut plan_ctx);
    assert!(result.is_err());
    match result.unwrap_err() {
        LogicalPlanError::EmptyNode => (), // Expected error
        _ => panic!("Expected EmptyNode error"),
    }
}

#[test]
fn test_traverse_connected_pattern_new_connection() {
    let graph_schema = create_test_schema_with_relationships();
    let mut plan_ctx = PlanCtx::new(Arc::new(graph_schema));
    let initial_plan = Arc::new(LogicalPlan::Empty);

    let start_node = ast::NodePattern {
        name: Some("user"),
        labels: Some(vec!["Person"]),
        properties: None,
    };

    let end_node = ast::NodePattern {
        name: Some("company"),
        labels: Some(vec!["Organization"]),
        properties: None,
    };

    let relationship = ast::RelationshipPattern {
        name: Some("works_at"),
        direction: ast::Direction::Outgoing,
        labels: Some(vec!["WORKS_AT"]),
        properties: None,
        variable_length: None,
    };

    let connected_pattern = ast::ConnectedPattern {
        start_node: Rc::new(RefCell::new(start_node)),
        relationship,
        end_node: Rc::new(RefCell::new(end_node)),
    };

    let connected_patterns = vec![connected_pattern];

    let result =
        traverse_connected_pattern(&connected_patterns, initial_plan, &mut plan_ctx, 0).unwrap();

    // Should return a GraphRel plan
    match result.as_ref() {
        LogicalPlan::GraphRel(graph_rel) => {
            assert_eq!(graph_rel.alias, "works_at");
            assert_eq!(graph_rel.direction, Direction::Outgoing);
            assert_eq!(graph_rel.left_connection, "user"); // Left node is the start node (user) for outgoing relationships
            assert_eq!(graph_rel.right_connection, "company"); // Right node is the end node (company) for outgoing relationships
            assert!(!graph_rel.is_rel_anchor);

            // Check left side (start node for outgoing relationships)
            match graph_rel.left.as_ref() {
                LogicalPlan::GraphNode(left_node) => {
                    assert_eq!(left_node.alias, "user");
                }
                _ => panic!("Expected GraphNode on left"),
            }

            // Check right side (end node for outgoing relationships)
            match graph_rel.right.as_ref() {
                LogicalPlan::GraphNode(right_node) => {
                    assert_eq!(right_node.alias, "company");
                }
                _ => panic!("Expected GraphNode on right"),
            }
        }
        _ => panic!("Expected GraphRel"),
    }

    // Should have added entries to plan context
    assert!(plan_ctx.get_table_ctx("user").is_ok());
    assert!(plan_ctx.get_table_ctx("company").is_ok());
    assert!(plan_ctx.get_table_ctx("works_at").is_ok());

    let rel_ctx = plan_ctx.get_table_ctx("works_at").unwrap();
    assert!(rel_ctx.is_relation());
}

#[test]
fn test_traverse_connected_pattern_with_existing_start_node() {
    let graph_schema = create_test_schema_with_relationships();
    let mut plan_ctx = PlanCtx::new(Arc::new(graph_schema));
    let initial_plan = Arc::new(LogicalPlan::Empty);

    // Pre-populate with existing start node
    plan_ctx.insert_table_ctx(
        "user".to_string(),
        TableCtx::build(
            "user".to_string(),
            Some("Person".to_string()).map(|l| vec![l]),
            vec![],
            false,
            true,
        ),
    );

    let start_node = ast::NodePattern {
        name: Some("user"),             // This exists in plan_ctx
        labels: Some(vec!["Employee"]), // Different label
        properties: None,
    };

    let end_node = ast::NodePattern {
        name: Some("project"),
        labels: Some(vec!["Project"]),
        properties: None,
    };

    let relationship = ast::RelationshipPattern {
        name: Some("assigned_to"),
        direction: ast::Direction::Incoming,
        labels: Some(vec!["ASSIGNED_TO"]),
        properties: None,
        variable_length: None,
    };

    let connected_pattern = ast::ConnectedPattern {
        start_node: Rc::new(RefCell::new(start_node)),
        relationship,
        end_node: Rc::new(RefCell::new(end_node)),
    };

    let connected_patterns = vec![connected_pattern];

    let result =
        traverse_connected_pattern(&connected_patterns, initial_plan, &mut plan_ctx, 0).unwrap();

    // Should return a GraphRel plan with different structure
    match result.as_ref() {
        LogicalPlan::GraphRel(graph_rel) => {
            assert_eq!(graph_rel.alias, "assigned_to");
            assert_eq!(graph_rel.direction, Direction::Incoming);
            assert_eq!(graph_rel.left_connection, "project");
            assert_eq!(graph_rel.right_connection, "user");

            // Left should be the new end node
            match graph_rel.left.as_ref() {
                LogicalPlan::GraphNode(left_node) => {
                    assert_eq!(left_node.alias, "project");
                }
                _ => panic!("Expected GraphNode on left"),
            }
        }
        _ => panic!("Expected GraphRel"),
    }

    // Existing start node should have updated label
    let user_ctx = plan_ctx.get_table_ctx("user").unwrap();
    assert_eq!(user_ctx.get_label_opt(), Some("Employee".to_string()));
}

// Test removed: DisconnectedPatternFound error no longer exists
// as of commit b015cf0 which allows disconnected comma patterns
// with WHERE clause predicates for cross-table correlation

#[test]
fn test_evaluate_match_clause_with_node_and_connected_pattern() {
    let graph_schema = create_test_schema_with_relationships();
    let mut plan_ctx = PlanCtx::new(Arc::new(graph_schema));
    let initial_plan = Arc::new(LogicalPlan::Empty);

    // Create a match clause with both node pattern and connected pattern
    let node_pattern = ast::NodePattern {
        name: Some("admin"),
        labels: Some(vec!["User"]),
        properties: Some(vec![ast::Property::PropertyKV(ast::PropertyKVPair {
            key: "role",
            value: ast::Expression::Literal(ast::Literal::String("administrator")),
        })]),
    };

    let start_node = ast::NodePattern {
        name: Some("admin"), // Same as above - should connect
        labels: None,
        properties: None,
    };

    let end_node = ast::NodePattern {
        name: Some("system"),
        labels: Some(vec!["System"]),
        properties: None,
    };

    let relationship = ast::RelationshipPattern {
        name: Some("manages"),
        direction: ast::Direction::Outgoing,
        labels: Some(vec!["MANAGES"]),
        properties: None,
        variable_length: None,
    };

    let connected_pattern = ast::ConnectedPattern {
        start_node: Rc::new(RefCell::new(start_node)),
        relationship,
        end_node: Rc::new(RefCell::new(end_node)),
    };

    let match_clause = ast::MatchClause {
        path_patterns: vec![
            (None, ast::PathPattern::Node(node_pattern)),
            (
                None,
                ast::PathPattern::ConnectedPattern(vec![connected_pattern]),
            ),
        ],
        where_clause: None,
    };

    let result = evaluate_match_clause(&match_clause, initial_plan, &mut plan_ctx).unwrap();

    // Should return a GraphRel plan
    match result.as_ref() {
        LogicalPlan::GraphRel(graph_rel) => {
            assert_eq!(graph_rel.alias, "manages");
            assert_eq!(graph_rel.direction, Direction::Outgoing);
        }
        _ => panic!("Expected GraphRel at top level"),
    }

    // Properties should have been converted to filters
    let admin_ctx = plan_ctx.get_table_ctx("admin").unwrap();
    assert_eq!(admin_ctx.get_filters().len(), 1);
}

#[test]
fn test_convert_properties_to_operator_application() {
    let mut plan_ctx = PlanCtx::new_empty();

    // Add table context with properties
    let properties = vec![Property::PropertyKV(PropertyKVPair {
        key: "status".to_string(),
        value: LogicalExpr::Literal(Literal::String("active".to_string())),
    })];

    let table_ctx = TableCtx::build(
        "user".to_string(),
        Some("Person".to_string()).map(|l| vec![l]),
        properties,
        false,
        true,
    );

    plan_ctx.insert_table_ctx("user".to_string(), table_ctx);

    // Before conversion, table should have no filters
    let table_ctx_before = plan_ctx.get_table_ctx("user").unwrap();
    assert_eq!(table_ctx_before.get_filters().len(), 0);

    // Convert properties
    let result = convert_properties_to_operator_application(&mut plan_ctx);
    assert!(result.is_ok());

    // After conversion, properties should be moved to filters
    let table_ctx_after = plan_ctx.get_table_ctx("user").unwrap();
    assert_eq!(table_ctx_after.get_filters().len(), 1); // Filter added

    // Check the filter predicate
    match &table_ctx_after.get_filters()[0] {
        LogicalExpr::OperatorApplicationExp(op_app) => {
            assert_eq!(op_app.operator, Operator::Equal);
            match &op_app.operands[0] {
                LogicalExpr::PropertyAccessExp(prop_access) => {
                    assert_eq!(prop_access.table_alias.0, "user");
                    assert_eq!(prop_access.column.raw(), "status");
                }
                _ => panic!("Expected PropertyAccessExp"),
            }
        }
        _ => panic!("Expected OperatorApplication"),
    }
}

#[test]
fn test_generate_scan() {
    // Create schema with Customer node
    use crate::graph_catalog::graph_schema::{GraphSchema, NodeIdSchema, NodeSchema};
    use std::collections::HashMap;

    let mut nodes = HashMap::new();
    nodes.insert(
        "Customer".to_string(),
        NodeSchema {
            database: "test_db".to_string(),
            table_name: "customers".to_string(),
            column_names: vec!["id".to_string(), "name".to_string()],
            primary_keys: "id".to_string(),
            node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
            property_mappings: HashMap::new(),
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
            node_id_types: None,
        },
    );

    let schema = Arc::new(GraphSchema::build(
        1,
        "test".to_string(),
        nodes,
        HashMap::new(),
    ));
    let plan_ctx = PlanCtx::new(schema);

    let scan = generate_scan(
        "customers".to_string(),
        Some("Customer".to_string()),
        &plan_ctx,
    )
    .unwrap();

    match scan.as_ref() {
        LogicalPlan::ViewScan(scan_plan) => {
            assert_eq!(scan_plan.source_table, "test_db.customers");
            // The label is "Customer" but ViewScan doesn't store it directly
        }
        _ => panic!("Expected ViewScan plan"),
    }
}

// ==========================================
// Tests for relationship type inference
// ==========================================

fn create_test_schema_with_relationships() -> GraphSchema {
    use crate::graph_catalog::graph_schema::{NodeIdSchema, NodeSchema, RelationshipSchema};
    use std::collections::HashMap;

    let mut nodes = HashMap::new();
    nodes.insert(
        "Airport".to_string(),
        NodeSchema {
            database: "test_db".to_string(),
            table_name: "airports".to_string(),
            column_names: vec!["id".to_string(), "code".to_string()],
            primary_keys: "id".to_string(),
            node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
            property_mappings: HashMap::new(),
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
            node_id_types: None,
        },
    );
    nodes.insert(
        "User".to_string(),
        NodeSchema {
            database: "test_db".to_string(),
            table_name: "users".to_string(),
            column_names: vec!["id".to_string(), "name".to_string()],
            primary_keys: "id".to_string(),
            node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
            property_mappings: HashMap::new(),
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
            node_id_types: None,
        },
    );
    nodes.insert(
        "Post".to_string(),
        NodeSchema {
            database: "test_db".to_string(),
            table_name: "posts".to_string(),
            column_names: vec!["id".to_string(), "title".to_string()],
            primary_keys: "id".to_string(),
            node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
            property_mappings: HashMap::new(),
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
            node_id_types: None,
        },
    );

    let mut rels = HashMap::new();
    rels.insert(
        "FLIGHT".to_string(),
        RelationshipSchema {
            database: "test_db".to_string(),
            table_name: "flights".to_string(),
            column_names: vec!["from_airport".to_string(), "to_airport".to_string()],
            from_node: "Airport".to_string(),
            to_node: "Airport".to_string(),
            from_node_table: "airports".to_string(),
            to_node_table: "airports".to_string(),
            from_id: "from_airport".to_string(),
            to_id: "to_airport".to_string(),
            from_node_id_dtype: "UInt64".to_string(),
            to_node_id_dtype: "UInt64".to_string(),
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
    rels.insert(
        "LIKES".to_string(),
        RelationshipSchema {
            database: "test_db".to_string(),
            table_name: "likes".to_string(),
            column_names: vec!["user_id".to_string(), "post_id".to_string()],
            from_node: "User".to_string(),
            to_node: "Post".to_string(),
            from_node_table: "users".to_string(),
            to_node_table: "posts".to_string(),
            from_id: "user_id".to_string(),
            to_id: "post_id".to_string(),
            from_node_id_dtype: "UInt64".to_string(),
            to_node_id_dtype: "UInt64".to_string(),
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
    rels.insert(
        "FOLLOWS".to_string(),
        RelationshipSchema {
            database: "test_db".to_string(),
            table_name: "follows".to_string(),
            column_names: vec!["follower_id".to_string(), "followed_id".to_string()],
            from_node: "User".to_string(),
            to_node: "User".to_string(),
            from_node_table: "users".to_string(),
            to_node_table: "users".to_string(),
            from_id: "follower_id".to_string(),
            to_id: "followed_id".to_string(),
            from_node_id_dtype: "UInt64".to_string(),
            to_node_id_dtype: "UInt64".to_string(),
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

    // Add missing nodes for tests
    nodes.insert(
        "Person".to_string(),
        NodeSchema {
            database: "test_db".to_string(),
            table_name: "persons".to_string(),
            column_names: vec!["id".to_string(), "name".to_string(), "city".to_string()],
            primary_keys: "id".to_string(),
            node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
            property_mappings: HashMap::new(),
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
            node_id_types: None,
        },
    );
    nodes.insert(
        "Organization".to_string(),
        NodeSchema {
            database: "test_db".to_string(),
            table_name: "organizations".to_string(),
            column_names: vec!["id".to_string(), "name".to_string()],
            primary_keys: "id".to_string(),
            node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
            property_mappings: HashMap::new(),
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
            node_id_types: None,
        },
    );
    nodes.insert(
        "Employee".to_string(),
        NodeSchema {
            database: "test_db".to_string(),
            table_name: "employees".to_string(),
            column_names: vec!["id".to_string(), "name".to_string()],
            primary_keys: "id".to_string(),
            node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
            property_mappings: HashMap::new(),
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
            node_id_types: None,
        },
    );
    nodes.insert(
        "Project".to_string(),
        NodeSchema {
            database: "test_db".to_string(),
            table_name: "projects".to_string(),
            column_names: vec!["id".to_string(), "name".to_string()],
            primary_keys: "id".to_string(),
            node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
            property_mappings: HashMap::new(),
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
            node_id_types: None,
        },
    );
    nodes.insert(
        "System".to_string(),
        NodeSchema {
            database: "test_db".to_string(),
            table_name: "systems".to_string(),
            column_names: vec!["id".to_string(), "name".to_string()],
            primary_keys: "id".to_string(),
            node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
            property_mappings: HashMap::new(),
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
            node_id_types: None,
        },
    );

    // Add missing relationships for tests
    rels.insert(
        "WORKS_AT".to_string(),
        RelationshipSchema {
            database: "test_db".to_string(),
            table_name: "works_at".to_string(),
            column_names: vec!["person_id".to_string(), "org_id".to_string()],
            from_node: "Person".to_string(),
            to_node: "Organization".to_string(),
            from_node_table: "persons".to_string(),
            to_node_table: "organizations".to_string(),
            from_id: "person_id".to_string(),
            to_id: "org_id".to_string(),
            from_node_id_dtype: "UInt64".to_string(),
            to_node_id_dtype: "UInt64".to_string(),
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
    rels.insert(
        "ASSIGNED_TO".to_string(),
        RelationshipSchema {
            database: "test_db".to_string(),
            table_name: "assigned_to".to_string(),
            column_names: vec!["emp_id".to_string(), "proj_id".to_string()],
            from_node: "Employee".to_string(),
            to_node: "Project".to_string(),
            from_node_table: "employees".to_string(),
            to_node_table: "projects".to_string(),
            from_id: "emp_id".to_string(),
            to_id: "proj_id".to_string(),
            from_node_id_dtype: "UInt64".to_string(),
            to_node_id_dtype: "UInt64".to_string(),
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
    rels.insert(
        "MANAGES".to_string(),
        RelationshipSchema {
            database: "test_db".to_string(),
            table_name: "manages".to_string(),
            column_names: vec!["user_id".to_string(), "system_id".to_string()],
            from_node: "User".to_string(),
            to_node: "System".to_string(),
            from_node_table: "users".to_string(),
            to_node_table: "systems".to_string(),
            from_id: "user_id".to_string(),
            to_id: "system_id".to_string(),
            from_node_id_dtype: "UInt64".to_string(),
            to_node_id_dtype: "UInt64".to_string(),
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

    GraphSchema::build(1, "test_db".to_string(), nodes, rels)
}

fn create_single_relationship_schema() -> GraphSchema {
    use crate::graph_catalog::graph_schema::{NodeIdSchema, NodeSchema, RelationshipSchema};
    use std::collections::HashMap;

    let mut nodes = HashMap::new();
    nodes.insert(
        "Person".to_string(),
        NodeSchema {
            database: "test_db".to_string(),
            table_name: "persons".to_string(),
            column_names: vec!["id".to_string(), "name".to_string()],
            primary_keys: "id".to_string(),
            node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
            property_mappings: HashMap::new(),
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
            node_id_types: None,
        },
    );

    let mut rels = HashMap::new();
    rels.insert(
        "KNOWS".to_string(),
        RelationshipSchema {
            database: "test_db".to_string(),
            table_name: "knows".to_string(),
            column_names: vec!["person1_id".to_string(), "person2_id".to_string()],
            from_node: "Person".to_string(),
            to_node: "Person".to_string(),
            from_node_table: "persons".to_string(),
            to_node_table: "persons".to_string(),
            from_id: "person1_id".to_string(),
            to_id: "person2_id".to_string(),
            from_node_id_dtype: "UInt64".to_string(),
            to_node_id_dtype: "UInt64".to_string(),
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

    GraphSchema::build(1, "test_db".to_string(), nodes, rels)
}

#[test]
fn test_infer_relationship_type_single_schema() {
    // When schema has only one relationship, use it regardless of node types
    let schema = create_single_relationship_schema();
    let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

    let result = infer_relationship_type_from_nodes(
        &None, // untyped start
        &None, // untyped end
        &ast::Direction::Outgoing,
        &schema,
        &plan_ctx,
    )
    .expect("Should not error");

    assert!(result.is_some());
    let types = result.unwrap();
    assert_eq!(types.len(), 1);
    assert_eq!(types[0], "KNOWS");
}

#[test]
fn test_infer_relationship_type_from_start_node() {
    // (a:Airport)-[r]->() should infer FLIGHT (only edge from Airport)
    let schema = create_test_schema_with_relationships();
    let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

    let result = infer_relationship_type_from_nodes(
        &Some("Airport".to_string()),
        &None,
        &ast::Direction::Outgoing,
        &schema,
        &plan_ctx,
    )
    .expect("Should not error");

    assert!(result.is_some());
    let types = result.unwrap();
    assert_eq!(types.len(), 1);
    assert_eq!(types[0], "FLIGHT");
}

#[test]
fn test_infer_relationship_type_from_end_node() {
    // ()-[r]->(p:Post) should infer LIKES (only edge to Post)
    let schema = create_test_schema_with_relationships();
    let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

    let result = infer_relationship_type_from_nodes(
        &None,
        &Some("Post".to_string()),
        &ast::Direction::Outgoing,
        &schema,
        &plan_ctx,
    )
    .expect("Should not error");

    assert!(result.is_some());
    let types = result.unwrap();
    assert_eq!(types.len(), 1);
    assert_eq!(types[0], "LIKES");
}

#[test]
fn test_infer_relationship_type_from_both_nodes() {
    // (u:User)-[r]->(p:Post) should infer LIKES
    let schema = create_test_schema_with_relationships();
    let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

    let result = infer_relationship_type_from_nodes(
        &Some("User".to_string()),
        &Some("Post".to_string()),
        &ast::Direction::Outgoing,
        &schema,
        &plan_ctx,
    )
    .expect("Should not error");

    assert!(result.is_some());
    let types = result.unwrap();
    assert_eq!(types.len(), 1);
    assert_eq!(types[0], "LIKES");
}

#[test]
fn test_infer_relationship_type_multiple_matches() {
    // (u:User)-[r]->() should return LIKES, FOLLOWS, and MANAGES (multiple edges from User)
    let schema = create_test_schema_with_relationships();
    let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

    let result = infer_relationship_type_from_nodes(
        &Some("User".to_string()),
        &None,
        &ast::Direction::Outgoing,
        &schema,
        &plan_ctx,
    )
    .expect("Should not error");

    assert!(result.is_some());
    let types = result.unwrap();
    assert_eq!(types.len(), 3); // Now 3 relationships: LIKES, FOLLOWS, MANAGES
    assert!(types.contains(&"LIKES".to_string()));
    assert!(types.contains(&"FOLLOWS".to_string()));
    assert!(types.contains(&"MANAGES".to_string()));
}

#[test]
fn test_infer_relationship_type_incoming_direction() {
    // ()<-[r]-(p:Post) should infer LIKES (reversed direction)
    let schema = create_test_schema_with_relationships();
    let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

    let result = infer_relationship_type_from_nodes(
        &None,
        &Some("Post".to_string()),
        &ast::Direction::Incoming,
        &schema,
        &plan_ctx,
    )
    .expect("Should not error");

    // Incoming means: from=end(Post), to=start(None)
    // LIKES has from=User, to=Post
    // So we need to check: from_node=Post? No. LIKES doesn't match.
    // Actually for incoming: from=end, to=start
    // So Post is the end node, meaning we're looking for relationships with to_node=Post
    // But incoming flips it: from_matches_end = "Post" == rel.from_node? No for LIKES
    // Hmm, let me reconsider - for incoming, the arrow points to start
    // So the relationship's to_node should be the pattern's start node
    // And the relationship's from_node should be the pattern's end node
    // In this case: ()<-[r]-(p:Post) means Post→anonymous
    // So we want relationships where from_node=Post - but LIKES has from_node=User
    // This should return None/empty
    assert!(result.is_none() || result.as_ref().unwrap().is_empty());
}

#[test]
fn test_infer_relationship_type_incoming_correct() {
    // (u:User)<-[r]-() should infer FOLLOWS (User is the to_node of FOLLOWS)
    let schema = create_test_schema_with_relationships();
    let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

    let result = infer_relationship_type_from_nodes(
        &Some("User".to_string()),
        &None,
        &ast::Direction::Incoming,
        &schema,
        &plan_ctx,
    )
    .expect("Should not error");

    // Incoming: from=end(None), to=start(User)
    // FOLLOWS: from=User, to=User - matches (to=User checks against start)
    // LIKES: from=User, to=Post - doesn't match (to=Post != User)
    assert!(result.is_some());
    let types = result.unwrap();
    assert_eq!(types.len(), 1);
    assert_eq!(types[0], "FOLLOWS");
}

#[test]
fn test_infer_relationship_type_no_matches() {
    // (a:Airport)-[r]->(u:User) should find no matching relationships
    let schema = create_test_schema_with_relationships();
    let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

    let result = infer_relationship_type_from_nodes(
        &Some("Airport".to_string()),
        &Some("User".to_string()),
        &ast::Direction::Outgoing,
        &schema,
        &plan_ctx,
    )
    .expect("Should not error");

    // FLIGHT: Airport→Airport - doesn't match (to=Airport != User)
    // LIKES: User→Post - doesn't match (from=User != Airport)
    // FOLLOWS: User→User - doesn't match
    assert!(result.is_none());
}

#[test]
fn test_infer_relationship_type_both_untyped_multi_schema() {
    // ()-[r]->() with multiple relationships should return None
    let schema = create_test_schema_with_relationships();
    let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

    let result = infer_relationship_type_from_nodes(
        &None,
        &None,
        &ast::Direction::Outgoing,
        &schema,
        &plan_ctx,
    )
    .expect("Should not error");

    // Both nodes untyped and schema has 3 relationships - cannot infer
    assert!(result.is_none());
}

#[test]
fn test_infer_relationship_type_too_many_matches_error() {
    // Create a schema with many relationship types from User
    use crate::graph_catalog::graph_schema::{NodeIdSchema, NodeSchema, RelationshipSchema};
    use std::collections::HashMap;

    let mut nodes = HashMap::new();
    nodes.insert(
        "User".to_string(),
        NodeSchema {
            database: "test_db".to_string(),
            table_name: "users".to_string(),
            column_names: vec!["id".to_string()],
            primary_keys: "id".to_string(),
            node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
            property_mappings: HashMap::new(),
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
            node_id_types: None,
        },
    );

    let mut rels = HashMap::new();
    // Create 6 relationships from User to User (exceeds MAX_INFERRED_TYPES of 5)
    for i in 1..=6 {
        rels.insert(
            format!("REL_{}", i),
            RelationshipSchema {
                database: "test_db".to_string(),
                table_name: format!("rel_{}", i),
                column_names: vec!["from_id".to_string(), "to_id".to_string()],
                from_node: "User".to_string(),
                to_node: "User".to_string(),
                from_node_table: "users".to_string(),
                to_node_table: "users".to_string(),
                from_id: "from_id".to_string(),
                to_id: "to_id".to_string(),
                from_node_id_dtype: "UInt64".to_string(),
                to_node_id_dtype: "UInt64".to_string(),
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
    }

    let schema = GraphSchema::build(1, "test_db".to_string(), nodes, rels);
    let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

    // (u:User)-[r]->() should fail with TooManyInferredTypes error
    let result = infer_relationship_type_from_nodes(
        &Some("User".to_string()),
        &None,
        &ast::Direction::Outgoing,
        &schema,
        &plan_ctx,
    );

    assert!(result.is_err());
    match result.unwrap_err() {
        LogicalPlanError::TooManyInferredTypes {
            count,
            max,
            types: _,
        } => {
            assert_eq!(count, 6);
            assert_eq!(max, 5); // default max_inferred_types
        }
        other => panic!("Expected TooManyInferredTypes error, got: {:?}", other),
    }
}

// ========================================
// Tests for infer_node_label_from_schema
// ========================================

fn create_single_node_schema() -> GraphSchema {
    use crate::graph_catalog::graph_schema::{NodeIdSchema, NodeSchema};
    use std::collections::HashMap;

    let mut nodes = HashMap::new();
    nodes.insert(
        "Person".to_string(),
        NodeSchema {
            database: "test_db".to_string(),
            table_name: "persons".to_string(),
            column_names: vec!["id".to_string(), "name".to_string()],
            primary_keys: "id".to_string(),
            node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
            property_mappings: HashMap::new(),
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
            node_id_types: None,
        },
    );

    // No relationships needed for node-only inference tests
    let rels = HashMap::new();

    GraphSchema::build(1, "test_db".to_string(), nodes, rels)
}

fn create_multi_node_schema() -> GraphSchema {
    use crate::graph_catalog::graph_schema::{NodeIdSchema, NodeSchema};
    use std::collections::HashMap;

    let mut nodes = HashMap::new();
    for node_type in &["User", "Post", "Comment"] {
        nodes.insert(
            node_type.to_string(),
            NodeSchema {
                database: "test_db".to_string(),
                table_name: format!("{}s", node_type.to_lowercase()),
                column_names: vec!["id".to_string()],
                primary_keys: "id".to_string(),
                node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
                property_mappings: HashMap::new(),
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
                node_id_types: None,
            },
        );
    }

    let rels = HashMap::new();

    GraphSchema::build(1, "test_db".to_string(), nodes, rels)
}

fn create_empty_node_schema() -> GraphSchema {
    use std::collections::HashMap;

    let nodes = HashMap::new();
    let rels = HashMap::new();

    GraphSchema::build(1, "test_db".to_string(), nodes, rels)
}

#[test]
fn test_infer_node_label_single_node_schema() {
    // When schema has only one node type, infer it
    let schema = create_single_node_schema();
    let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

    let result = infer_node_label_from_schema(&schema, &plan_ctx).expect("should not error");

    assert_eq!(result, Some("Person".to_string()));
}

#[test]
fn test_infer_node_label_multi_node_schema() {
    // When schema has multiple node types, cannot infer (returns None)
    let schema = create_multi_node_schema();
    let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

    let result = infer_node_label_from_schema(&schema, &plan_ctx).expect("should not error");

    // Should not auto-infer when multiple types exist
    assert_eq!(result, None);
}

#[test]
fn test_infer_node_label_empty_schema() {
    // When schema has no nodes, cannot infer
    let schema = create_empty_node_schema();
    let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

    let result = infer_node_label_from_schema(&schema, &plan_ctx).expect("should not error");

    assert_eq!(result, None);
}

#[test]
fn test_infer_node_label_many_nodes_no_error() {
    // When schema has many node types, should return None without error
    // (unlike relationships, we don't generate UNION for standalone nodes yet)
    use crate::graph_catalog::graph_schema::{NodeIdSchema, NodeSchema};
    use std::collections::HashMap;

    let mut nodes = HashMap::new();
    for i in 1..=10 {
        nodes.insert(
            format!("Type{}", i),
            NodeSchema {
                database: "test_db".to_string(),
                table_name: format!("type_{}", i),
                column_names: vec!["id".to_string()],
                primary_keys: "id".to_string(),
                node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
                property_mappings: HashMap::new(),
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
                node_id_types: None,
            },
        );
    }

    let schema = GraphSchema::build(1, "test_db".to_string(), nodes, HashMap::new());
    let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

    let result = infer_node_label_from_schema(&schema, &plan_ctx).expect("should not error");

    // Should not auto-infer when many types exist (just return None, no error)
    assert_eq!(result, None);
}

#[test]
fn test_infer_node_label_denormalized_single_node() {
    // Single denormalized node type should still be inferred
    // The inference works at schema level - denormalized handling is done later
    use crate::graph_catalog::graph_schema::{NodeIdSchema, NodeSchema};
    use std::collections::HashMap;

    let mut nodes = HashMap::new();
    nodes.insert(
        "Airport".to_string(),
        NodeSchema {
            database: "test_db".to_string(),
            table_name: "flights".to_string(), // Edge table
            column_names: vec!["Origin".to_string(), "Dest".to_string()],
            primary_keys: "Origin".to_string(),
            node_id: NodeIdSchema::single("Origin".to_string(), "String".to_string()),
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: true, // Denormalized node!
            from_properties: Some({
                let mut m = HashMap::new();
                m.insert("code".to_string(), "Origin".to_string());
                m
            }),
            to_properties: Some({
                let mut m = HashMap::new();
                m.insert("code".to_string(), "Dest".to_string());
                m
            }),
            denormalized_source_table: Some("test_db.flights".to_string()),
            label_column: None,
            label_value: None,
            node_id_types: None,
        },
    );

    let schema = GraphSchema::build(1, "test_db".to_string(), nodes, HashMap::new());
    let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

    // Should still infer the label - denormalized handling happens later
    let result = infer_node_label_from_schema(&schema, &plan_ctx).expect("should not error");
    assert_eq!(result, Some("Airport".to_string()));
}

#[test]
fn test_infer_relationship_type_polymorphic_edge() {
    // Polymorphic edge with from_label_values should match typed nodes
    use crate::graph_catalog::graph_schema::{NodeIdSchema, NodeSchema, RelationshipSchema};
    use std::collections::HashMap;

    let mut nodes = HashMap::new();
    for node_type in &["User", "Group", "Resource"] {
        nodes.insert(
            node_type.to_string(),
            NodeSchema {
                database: "test_db".to_string(),
                table_name: format!("{}s", node_type.to_lowercase()),
                column_names: vec!["id".to_string()],
                primary_keys: "id".to_string(),
                node_id: NodeIdSchema::single("id".to_string(), "UInt64".to_string()),
                property_mappings: HashMap::new(),
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
                node_id_types: None,
            },
        );
    }

    let mut rels = HashMap::new();
    // Polymorphic MEMBER_OF: (User|Group) -> Group
    rels.insert(
        "MEMBER_OF".to_string(),
        RelationshipSchema {
            database: "test_db".to_string(),
            table_name: "memberships".to_string(),
            column_names: vec!["member_id".to_string(), "group_id".to_string()],
            from_node: "$any".to_string(), // Polymorphic
            to_node: "Group".to_string(),
            from_node_table: "$any".to_string(),
            to_node_table: "groups".to_string(),
            from_id: "member_id".to_string(),
            to_id: "group_id".to_string(),
            from_node_id_dtype: "UInt64".to_string(),
            to_node_id_dtype: "UInt64".to_string(),
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: Some("member_type".to_string()),
            to_label_column: None,
            from_label_values: Some(vec!["User".to_string(), "Group".to_string()]), // Polymorphic!
            to_label_values: None,
            from_node_properties: None,
            to_node_properties: None,
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
        },
    );

    let schema = GraphSchema::build(1, "test_db".to_string(), nodes, rels);
    let plan_ctx = PlanCtx::new(Arc::new(schema.clone()));

    // (u:User)-[r]->(g:Group) should infer MEMBER_OF since User is in from_label_values
    let result = infer_relationship_type_from_nodes(
        &Some("User".to_string()),
        &Some("Group".to_string()),
        &ast::Direction::Outgoing,
        &schema,
        &plan_ctx,
    )
    .expect("should not error");

    assert_eq!(result, Some(vec!["MEMBER_OF".to_string()]));
}
