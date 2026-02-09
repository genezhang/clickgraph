//! Tests for graph join inference module.
//!
//! This module contains comprehensive unit tests for the graph join inference
//! system, including tests for:
//! - Edge list traversal (same and different node types)
//! - Directional relationship handling (incoming, outgoing)
//! - Multi-hop patterns
//! - FK-edge patterns (self-referencing and non-self-referencing)
//! - Relationship uniqueness constraints

use std::collections::HashMap;
use std::sync::Arc;

use super::metadata::{PatternEdgeInfo, PatternGraphMetadata};
use super::GraphJoinInference;
use crate::{
    graph_catalog::graph_schema::{GraphSchema, NodeIdSchema, NodeSchema, RelationshipSchema},
    query_planner::{
        analyzer::analyzer_pass::AnalyzerPass,
        logical_expr::{Direction, LogicalExpr, Operator, PropertyAccess, TableAlias},
        logical_plan::{GraphNode, GraphRel, JoinType, LogicalPlan, Projection, ProjectionItem},
        plan_ctx::{PlanCtx, TableCtx},
        transformed::Transformed,
    },
};

fn create_test_graph_schema() -> GraphSchema {
    let mut nodes = HashMap::new();
    let mut relationships = HashMap::new();

    // Create Person node schema
    nodes.insert(
        "Person".to_string(),
        NodeSchema {
            database: "default".to_string(),
            table_name: "Person".to_string(),
            column_names: vec!["id".to_string(), "name".to_string(), "age".to_string()],
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

    // Create Company node schema
    nodes.insert(
        "Company".to_string(),
        NodeSchema {
            database: "default".to_string(),
            table_name: "Company".to_string(),
            column_names: vec!["id".to_string(), "name".to_string(), "founded".to_string()],
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

    // Create FOLLOWS relationship schema (edge list)
    relationships.insert(
        "FOLLOWS::Person::Person".to_string(),
        RelationshipSchema {
            database: "default".to_string(),
            table_name: "FOLLOWS".to_string(),
            column_names: vec![
                "from_id".to_string(),
                "to_id".to_string(),
                "since".to_string(),
            ],
            from_node: "Person".to_string(),
            to_node: "Person".to_string(),
            from_node_table: "Person".to_string(),
            to_node_table: "Person".to_string(),
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

    // Create WORKS_AT relationship schema (edge list)
    relationships.insert(
        "WORKS_AT::Person::Company".to_string(),
        RelationshipSchema {
            database: "default".to_string(),
            table_name: "WORKS_AT".to_string(),
            column_names: vec![
                "from_id".to_string(),
                "to_id".to_string(),
                "position".to_string(),
            ],
            from_node: "Person".to_string(),
            to_node: "Company".to_string(),
            from_node_table: "Person".to_string(),
            to_node_table: "Company".to_string(),
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

    GraphSchema::build(1, "default".to_string(), nodes, relationships)
}

fn setup_plan_ctx_with_graph_entities() -> PlanCtx {
    let mut plan_ctx = PlanCtx::new_empty();

    // Add person nodes
    plan_ctx.insert_table_ctx(
        "p1".to_string(),
        TableCtx::build(
            "p1".to_string(),
            Some(vec!["Person".to_string()]),
            vec![],
            false,
            true,
        ),
    );
    plan_ctx.insert_table_ctx(
        "p2".to_string(),
        TableCtx::build(
            "p2".to_string(),
            Some(vec!["Person".to_string()]),
            vec![],
            false,
            true,
        ),
    );
    plan_ctx.insert_table_ctx(
        "p3".to_string(),
        TableCtx::build(
            "p3".to_string(),
            Some(vec!["Person".to_string()]),
            vec![],
            false,
            true,
        ),
    );

    // Add company node
    plan_ctx.insert_table_ctx(
        "c1".to_string(),
        TableCtx::build(
            "c1".to_string(),
            Some(vec!["Company".to_string()]),
            vec![],
            false,
            true,
        ),
    );

    // Add follows relationships
    plan_ctx.insert_table_ctx(
        "f1".to_string(),
        TableCtx::build(
            "f1".to_string(),
            Some(vec!["FOLLOWS".to_string()]),
            vec![],
            true,
            true,
        ),
    );
    plan_ctx.insert_table_ctx(
        "f2".to_string(),
        TableCtx::build(
            "f2".to_string(),
            Some(vec!["FOLLOWS".to_string()]),
            vec![],
            true,
            true,
        ),
    );

    // Add works_at relationship
    plan_ctx.insert_table_ctx(
        "w1".to_string(),
        TableCtx::build(
            "w1".to_string(),
            Some(vec!["WORKS_AT".to_string()]),
            vec![],
            true,
            true,
        ),
    );

    plan_ctx
}

fn create_scan_plan(_table_alias: &str, _table_name: &str) -> Arc<LogicalPlan> {
    // Use Empty since Scan is removed
    Arc::new(LogicalPlan::Empty)
}

fn create_graph_node(
    input: Arc<LogicalPlan>,
    alias: &str,
    is_denormalized: bool,
) -> Arc<LogicalPlan> {
    Arc::new(LogicalPlan::GraphNode(GraphNode {
        input,
        alias: alias.to_string(),
        label: None,
        is_denormalized,
        projected_columns: None,
    }))
}

fn create_graph_rel(
    left: Arc<LogicalPlan>,
    center: Arc<LogicalPlan>,
    right: Arc<LogicalPlan>,
    alias: &str,
    direction: Direction,
    left_connection: &str,
    right_connection: &str,
    labels: Option<Vec<String>>,
) -> Arc<LogicalPlan> {
    Arc::new(LogicalPlan::GraphRel(GraphRel {
        left,
        center,
        right,
        alias: alias.to_string(),
        direction,
        left_connection: left_connection.to_string(),
        right_connection: right_connection.to_string(),
        is_rel_anchor: false,
        variable_length: None,
        shortest_path_mode: None,
        path_variable: None,
        where_predicate: None, // Will be populated by filter pushdown
        labels,
        is_optional: None,
        anchor_connection: None,
        cte_references: std::collections::HashMap::new(),
    }))
}

#[test]
fn test_no_graph_joins_when_no_graph_rels() {
    let analyzer = GraphJoinInference::new();
    let graph_schema = create_test_graph_schema();
    let mut plan_ctx = setup_plan_ctx_with_graph_entities();

    // Create a plan with only a graph node (no relationships)
    let scan = create_scan_plan("p1", "person");
    let graph_node = create_graph_node(scan, "p1", false);

    let result = analyzer
        .analyze_with_graph_schema(graph_node.clone(), &mut plan_ctx, &graph_schema)
        .unwrap();

    // Should not transform the plan since there are no graph relationships
    match result {
        Transformed::No(plan) => {
            assert_eq!(plan, graph_node);
        }
        _ => panic!("Expected no transformation for plan without relationships"),
    }
}

#[test]
fn test_edge_list_same_node_type_outgoing_direction() {
    let analyzer = GraphJoinInference::new();
    let graph_schema = create_test_graph_schema();
    let mut plan_ctx = setup_plan_ctx_with_graph_entities();

    // Set the relationship to use edge list
    plan_ctx.get_mut_table_ctx("f1").unwrap();

    // Create plan: (p1)-[f1:FOLLOWS]->(p2)
    let p1_scan = create_scan_plan("p1", "Person");
    let p1_node = create_graph_node(p1_scan, "p1", false);

    let f1_scan = create_scan_plan("f1", "FOLLOWS");

    let p2_scan = create_scan_plan("p2", "Person");
    let p2_node = create_graph_node(p2_scan, "p2", false);

    let graph_rel = create_graph_rel(
        p2_node,
        f1_scan,
        p1_node,
        "f1",
        Direction::Outgoing,
        "p2",
        "p1",
        Some(vec!["FOLLOWS".to_string()]),
    );

    let input_logical_plan = Arc::new(LogicalPlan::Projection(Projection {
        input: graph_rel,
        items: vec![ProjectionItem {
            expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias("p1".to_string()),
                column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                    "name".to_string(),
                ),
            }),
            col_alias: None,
        }],
        distinct: false,
    }));

    let result = analyzer
        .analyze_with_graph_schema(input_logical_plan, &mut plan_ctx, &graph_schema)
        .unwrap();

    println!("\n result: {:?}\n", result);

    // Should create graph joins
    match result {
        Transformed::Yes(plan) => {
            match plan.as_ref() {
                LogicalPlan::GraphJoins(graph_joins) => {
                    // Edge list optimization: Since neither node is referenced separately,
                    // PatternSchemaContext uses SingleTableScan strategy.
                    // This puts the edge table (FOLLOWS) in FROM clause with no additional JOINs.
                    assert_eq!(graph_joins.joins.len(), 1);
                    assert!(matches!(
                        graph_joins.input.as_ref(),
                        LogicalPlan::Projection(_)
                    ));
                    // anchor_table is the relationship table (f1) used as FROM
                    assert_eq!(graph_joins.anchor_table, Some("f1".to_string()));

                    // Single join: relationship table (f1) with empty joining_on (FROM marker)
                    let rel_join = &graph_joins.joins[0];
                    assert_eq!(rel_join.table_name, "default.FOLLOWS");
                    assert_eq!(rel_join.table_alias, "f1");
                    assert_eq!(rel_join.join_type, JoinType::Inner);
                    // Empty joining_on indicates this is the FROM clause, not a JOIN
                    assert_eq!(rel_join.joining_on.len(), 0);
                }
                _ => panic!("Expected GraphJoins node"),
            }
        }
        _ => panic!("Expected transformation"),
    }
}

#[test]
fn test_edge_list_different_node_types() {
    let analyzer = GraphJoinInference::new();
    let graph_schema = create_test_graph_schema();
    let mut plan_ctx = setup_plan_ctx_with_graph_entities();

    // Set the relationship to use edge list
    plan_ctx.get_mut_table_ctx("w1").unwrap();

    // Create plan: (p1)-[w1:WORKS_AT]->(c1)
    let p1_scan = create_scan_plan("p1", "Person");
    let p1_node = create_graph_node(p1_scan, "p1", false);

    let w1_scan = create_scan_plan("w1", "WORKS_AT");

    let c1_scan = create_scan_plan("c1", "Company");
    let c1_node = create_graph_node(c1_scan, "c1", false);

    let graph_rel = create_graph_rel(
        p1_node,
        w1_scan,
        c1_node,
        "w1",
        Direction::Outgoing,
        "p1", // left_connection (p1 is the LEFT node)
        "c1", // right_connection (c1 is the RIGHT node)
        Some(vec!["WORKS_AT".to_string()]),
    );

    let input_logical_plan = Arc::new(LogicalPlan::Projection(Projection {
        input: graph_rel,
        items: vec![ProjectionItem {
            expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias("p1".to_string()),
                column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                    "name".to_string(),
                ),
            }),
            col_alias: None,
        }],
        distinct: false,
    }));

    let result = analyzer
        .analyze_with_graph_schema(input_logical_plan, &mut plan_ctx, &graph_schema)
        .unwrap();

    // Should create graph joins for different node types
    match result {
        Transformed::Yes(plan) => {
            match plan.as_ref() {
                LogicalPlan::GraphJoins(graph_joins) => {
                    // Edge list optimization: p1 is referenced, c1 is not.
                    // SingleTableScan strategy puts w1 (edge table) in FROM clause.
                    assert_eq!(graph_joins.joins.len(), 1);
                    assert!(matches!(
                        graph_joins.input.as_ref(),
                        LogicalPlan::Projection(_)
                    ));
                    // anchor_table is the relationship table (w1) used as FROM
                    assert_eq!(graph_joins.anchor_table, Some("w1".to_string()));

                    // Single join: w1 with empty joining_on (FROM marker)
                    let rel_join = &graph_joins.joins[0];
                    assert_eq!(rel_join.table_name, "default.WORKS_AT");
                    assert_eq!(rel_join.table_alias, "w1");
                    assert_eq!(rel_join.join_type, JoinType::Inner);
                    // Empty joining_on indicates this is the FROM clause, not a JOIN
                    assert_eq!(rel_join.joining_on.len(), 0);
                }
                _ => panic!("Expected GraphJoins node"),
            }
        }
        _ => panic!("Expected transformation"),
    }
}

#[test]
#[ignore] // Bitmap indexes not used in current schema - edge lists only (use_edge_list flag removed)
fn test_bitmap_traversal() {
    let analyzer = GraphJoinInference::new();
    let graph_schema = create_test_graph_schema();
    let mut plan_ctx = setup_plan_ctx_with_graph_entities();

    // This test is obsolete - ClickGraph only uses edge lists
    // Bitmap traversal functionality has been removed

    // Create plan: (p1)-[f1:FOLLOWS]->(p2)
    let p1_scan = create_scan_plan("p1", "Person");
    let p1_node = create_graph_node(p1_scan, "p1", false);

    let f1_scan = create_scan_plan("f1", "FOLLOWS");

    // Add follows relationships
    plan_ctx.insert_table_ctx(
        "f1".to_string(),
        TableCtx::build(
            "f1".to_string(),
            Some(vec!["FOLLOWS_outgoing".to_string()]),
            vec![],
            true,
            true,
        ),
    );

    let p2_scan = create_scan_plan("p2", "Person");
    let p2_node = create_graph_node(p2_scan, "p2", false);

    let graph_rel = create_graph_rel(
        p2_node,
        f1_scan,
        p1_node,
        "f1",
        Direction::Outgoing,
        "p2",
        "p1",
        Some(vec!["FOLLOWS".to_string()]),
    );

    let input_logical_plan = Arc::new(LogicalPlan::Projection(Projection {
        input: graph_rel,
        items: vec![ProjectionItem {
            expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias("p1".to_string()),
                column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                    "name".to_string(),
                ),
            }),
            col_alias: None,
        }],
        distinct: false,
    }));

    let result = analyzer
        .analyze_with_graph_schema(input_logical_plan, &mut plan_ctx, &graph_schema)
        .unwrap();

    // Should create graph joins for bitmap traversal
    match result {
        Transformed::Yes(plan) => {
            match plan.as_ref() {
                LogicalPlan::GraphJoins(graph_joins) => {
                    // Assert GraphJoins structure
                    assert_eq!(graph_joins.joins.len(), 1); // Simple relationship: only relationship join, start node is in FROM
                    assert!(matches!(
                        graph_joins.input.as_ref(),
                        LogicalPlan::Projection(_)
                    ));

                    // (p1)-[f1:FOLLOWS]->(p2)
                    // For bitmap traversal, only relationship join is needed (start node in FROM)
                    let rel_join = &graph_joins.joins[0];
                    assert_eq!(rel_join.table_name, "default.FOLLOWS"); // Base table with database prefix
                    assert_eq!(rel_join.table_alias, "f1");
                    assert_eq!(rel_join.join_type, JoinType::Inner);
                    assert_eq!(rel_join.joining_on.len(), 1);

                    // Assert the joining condition for relationship
                    let rel_join_condition = &rel_join.joining_on[0];
                    assert_eq!(rel_join_condition.operator, Operator::Equal);
                    assert_eq!(rel_join_condition.operands.len(), 2);

                    // Check operands are PropertyAccessExp with correct table aliases and columns
                    match (
                        &rel_join_condition.operands[0],
                        &rel_join_condition.operands[1],
                    ) {
                        (
                            LogicalExpr::PropertyAccessExp(rel_prop),
                            LogicalExpr::PropertyAccessExp(right_prop),
                        ) => {
                            assert_eq!(rel_prop.table_alias.0, "f1");
                            assert_eq!(rel_prop.column.raw(), "to_id");
                            assert_eq!(right_prop.table_alias.0, "p2");
                            assert_eq!(right_prop.column.raw(), "id");
                        }
                        _ => panic!("Expected PropertyAccessExp operands"),
                    }
                }
                _ => panic!("Expected GraphJoins node"),
            }
        }
        _ => panic!("Expected transformation"),
    }
}

#[test]
fn test_standalone_relationship_edge_list() {
    let analyzer = GraphJoinInference::new();
    let graph_schema = create_test_graph_schema();
    let mut plan_ctx = setup_plan_ctx_with_graph_entities();

    // Set the relationship to use edge list
    plan_ctx.get_mut_table_ctx("f2").unwrap();

    // Create standalone relationship: (p3)-[f2:FOLLOWS]-(Empty)
    // This simulates a case where left node was already processed/removed
    let empty_left = Arc::new(LogicalPlan::Empty);
    let f2_scan = create_scan_plan("f2", "FOLLOWS");
    let p3_scan = create_scan_plan("p3", "Person");
    let p3_node = create_graph_node(p3_scan, "p3", false);

    let graph_rel = create_graph_rel(
        empty_left,
        f2_scan,
        p3_node,
        "f2",
        Direction::Outgoing,
        "p1", // left connection exists but left plan is Empty
        "p3",
        Some(vec!["FOLLOWS".to_string()]),
    );

    let input_logical_plan = Arc::new(LogicalPlan::Projection(Projection {
        input: graph_rel,
        items: vec![ProjectionItem {
            expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias("p1".to_string()),
                column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                    "name".to_string(),
                ),
            }),
            col_alias: None,
        }],
        distinct: false,
    }));

    let result = analyzer
        .analyze_with_graph_schema(input_logical_plan, &mut plan_ctx, &graph_schema)
        .unwrap();

    // Standalone relationship with Empty left node.
    // Expected: 3 joins (p1 as FROM with empty joining_on, f2, p3)
    match result {
        Transformed::Yes(plan) => {
            match plan.as_ref() {
                LogicalPlan::GraphJoins(graph_joins) => {
                    // Assert GraphJoins structure
                    // Pattern: (p1)-[f2:FOLLOWS]->(p3) where left is Empty
                    // After reordering: f2, p3, p1 (order may vary due to optimization)
                    assert_eq!(graph_joins.joins.len(), 3);
                    assert!(matches!(
                        graph_joins.input.as_ref(),
                        LogicalPlan::Projection(_)
                    ));

                    // Check that all expected aliases are present (order may vary)
                    let join_aliases: Vec<&String> =
                        graph_joins.joins.iter().map(|j| &j.table_alias).collect();
                    assert!(join_aliases.contains(&&"f2".to_string()));
                    assert!(join_aliases.contains(&&"p3".to_string()));
                    assert!(join_aliases.contains(&&"p1".to_string()));

                    // Verify each join has correct structure
                    for join in &graph_joins.joins {
                        assert_eq!(join.join_type, JoinType::Inner);
                        // Joins may have empty or non-empty conditions depending on position
                    }
                }
                _ => panic!("Expected GraphJoins node"),
            }
        }
        _ => panic!("Expected transformation"),
    }
}

#[test]
fn test_incoming_direction_edge_list() {
    let analyzer = GraphJoinInference::new();
    let graph_schema = create_test_graph_schema();
    let mut plan_ctx = setup_plan_ctx_with_graph_entities();

    // Update relationship label for incoming direction
    // plan_ctx.get_mut_table_ctx("f1").unwrap().set_labels(Some(vec!["FOLLOWS_incoming"]));
    plan_ctx.get_mut_table_ctx("f1").unwrap();

    // Create plan: (p2)<-[f1:FOLLOWS]-(p1)
    // This means p1 FOLLOWS p2 (arrow goes from p1 to p2)
    // After GraphRel construction normalization:
    //   - left_connection = p1 (FROM node, the source/follower)
    //   - right_connection = p2 (TO node, the target/followed)
    //   - direction = Incoming (preserved from pattern)
    let p1_scan = create_scan_plan("p1", "Person");
    let p1_node = create_graph_node(p1_scan, "p1", false);

    let f1_scan = create_scan_plan("f1", "FOLLOWS");

    let p2_scan = create_scan_plan("p2", "Person");
    let p2_node = create_graph_node(p2_scan, "p2", false);

    // After construction normalization: left=FROM (p1), right=TO (p2)
    let graph_rel = create_graph_rel(
        p1_node, // left = FROM node (p1 is the follower/source)
        f1_scan,
        p2_node, // right = TO node (p2 is the followed/target)
        "f1",
        Direction::Incoming,
        "p1", // left_connection = FROM node
        "p2", // right_connection = TO node
        Some(vec!["FOLLOWS".to_string()]),
    );
    let input_logical_plan = Arc::new(LogicalPlan::Projection(Projection {
        input: graph_rel,
        items: vec![ProjectionItem {
            expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias("p1".to_string()),
                column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                    "name".to_string(),
                ),
            }),
            col_alias: None,
        }],
        distinct: false,
    }));

    let result = analyzer
        .analyze_with_graph_schema(input_logical_plan, &mut plan_ctx, &graph_schema)
        .unwrap();

    // Should create appropriate joins for incoming direction
    match result {
        Transformed::Yes(plan) => {
            match plan.as_ref() {
                LogicalPlan::GraphJoins(graph_joins) => {
                    // Edge list optimization: Neither p1 nor p2 is referenced separately.
                    // SingleTableScan strategy puts f1 (edge table) in FROM clause.
                    assert_eq!(graph_joins.joins.len(), 1);
                    assert!(matches!(
                        graph_joins.input.as_ref(),
                        LogicalPlan::Projection(_)
                    ));
                    // anchor_table is the relationship table (f1) used as FROM
                    assert_eq!(graph_joins.anchor_table, Some("f1".to_string()));

                    // Single join: f1 with empty joining_on (FROM marker)
                    let rel_join = &graph_joins.joins[0];
                    assert_eq!(rel_join.table_name, "default.FOLLOWS");
                    assert_eq!(rel_join.table_alias, "f1");
                    assert_eq!(rel_join.join_type, JoinType::Inner);
                    // Empty joining_on indicates this is the FROM clause, not a JOIN
                    assert_eq!(rel_join.joining_on.len(), 0);
                }
                _ => panic!("Expected GraphJoins node"),
            }
        }
        _ => panic!("Expected transformation"),
    }
}

#[test]
fn test_complex_nested_plan_with_multiple_graph_rels() {
    let analyzer = GraphJoinInference::new();
    let graph_schema = create_test_graph_schema();
    let mut plan_ctx = setup_plan_ctx_with_graph_entities();

    // Set relationships to use edge list
    plan_ctx.get_mut_table_ctx("f1").unwrap();
    plan_ctx.get_mut_table_ctx("w1").unwrap();

    // Create complex plan: (p1)-[f1:FOLLOWS]->(p2)-[w1:WORKS_AT]->(c1)
    let p1_scan = create_scan_plan("p1", "Person");
    let p1_node = create_graph_node(p1_scan, "p1", false);

    let f1_scan = create_scan_plan("f1", "FOLLOWS");

    let p2_scan = create_scan_plan("p2", "Person");
    let p2_node = create_graph_node(p2_scan, "p2", false);

    let first_rel = create_graph_rel(
        p2_node,
        f1_scan,
        p1_node,
        "f1",
        Direction::Outgoing,
        "p2",
        "p1",
        Some(vec!["FOLLOWS".to_string()]),
    );

    let w1_scan = create_scan_plan("w1", "WORKS_AT");

    let c1_scan = create_scan_plan("c1", "Company");
    let c1_node = create_graph_node(c1_scan, "c1", false);

    // (p1)-[f1:FOLLOWS]->(p2)-[w1:WORKS_AT]->(c1)

    let second_rel = create_graph_rel(
        c1_node,
        w1_scan,
        first_rel,
        "w1",
        Direction::Outgoing,
        "c1",
        "p2",
        Some(vec!["WORKS_AT".to_string()]),
    );

    let input_logical_plan = Arc::new(LogicalPlan::Projection(Projection {
        input: second_rel,
        items: vec![ProjectionItem {
            expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias("p1".to_string()),
                column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                    "name".to_string(),
                ),
            }),
            col_alias: None,
        }],
        distinct: false,
    }));

    let result = analyzer
        .analyze_with_graph_schema(input_logical_plan, &mut plan_ctx, &graph_schema)
        .unwrap();

    // (p1)-[f1:FOLLOWS]->(p2)-[w1:WORKS_AT]->(c1)
    // In this case, c1 is the ending node, we are now joining in reverse order.
    // It means first we will join c1 -> w1, w1 -> p2, p2 -> f1, f1 -> p1.
    // So the tables in the order of joining will be c1, w1, p2, f1, p1.
    // FIX: Multi-hop patterns now correctly generate ALL node JOINs for proper chaining.
    // Previously, SingleTableScan optimization incorrectly removed node JOINs.

    // Should create joins for all relationships in the chain
    match result {
        Transformed::Yes(plan) => {
            match plan.as_ref() {
                LogicalPlan::GraphJoins(graph_joins) => {
                    // Assert GraphJoins structure
                    assert!(graph_joins.joins.len() >= 2);
                    assert!(matches!(
                        graph_joins.input.as_ref(),
                        LogicalPlan::Projection(_)
                    ));

                    // Verify we have joins for both relationship aliases
                    let rel_aliases: Vec<&String> =
                        graph_joins.joins.iter().map(|j| &j.table_alias).collect();

                    // Should contain joins for both relationships
                    assert!(rel_aliases
                        .iter()
                        .any(|&alias| alias == "f1" || alias == "w1"));

                    // Multi-hop pattern: (p1)-[f1:FOLLOWS]->(p2)-[w1:WORKS_AT]->(c1)
                    // CORRECT: 5 joins - all nodes and edges for proper topological JOIN ordering
                    // c1 (anchor/FROM marker), w1 (edge), p2 (intermediate), f1 (edge), p1 (end)
                    println!("Actual joins len: {}", graph_joins.joins.len());
                    let join_aliases: Vec<&String> =
                        graph_joins.joins.iter().map(|j| &j.table_alias).collect();
                    println!("Join aliases: {:?}", join_aliases);
                    assert!(graph_joins.joins.len() == 5);

                    // Verify we have the expected join aliases: c1, w1, p2, f1, p1
                    let join_aliases: Vec<&String> =
                        graph_joins.joins.iter().map(|j| &j.table_alias).collect();

                    println!("Join aliases found: {:?}", join_aliases);
                    assert!(join_aliases.contains(&&"c1".to_string())); // anchor node
                    assert!(join_aliases.contains(&&"w1".to_string())); // first edge
                    assert!(join_aliases.contains(&&"p2".to_string())); // intermediate node
                    assert!(join_aliases.contains(&&"f1".to_string())); // second edge
                    assert!(join_aliases.contains(&&"p1".to_string())); // end node

                    // Verify each join has basic structure (skip detailed checks due to optimization variations)
                    for join in &graph_joins.joins {
                        assert_eq!(join.join_type, JoinType::Inner);
                        assert!(!join.table_name.is_empty());
                        assert!(!join.table_alias.is_empty());
                    }
                }
                _ => panic!("Expected GraphJoins node"),
            }
        }
        _ => panic!("Expected transformation"),
    }
}

// ===== FK-Edge Pattern Tests =====

fn create_self_referencing_fk_schema() -> GraphSchema {
    use crate::graph_catalog::expression_parser::PropertyValue;

    let mut nodes = HashMap::new();
    let mut relationships = HashMap::new();

    // Create Object node (filesystem objects - same table for all)
    nodes.insert(
        "Object".to_string(),
        NodeSchema {
            database: "test".to_string(),
            table_name: "fs_objects".to_string(),
            column_names: vec![
                "object_id".to_string(),
                "name".to_string(),
                "type".to_string(),
                "parent_id".to_string(),
            ],
            primary_keys: "object_id".to_string(),
            node_id: NodeIdSchema::single("object_id".to_string(), "UInt64".to_string()),
            property_mappings: {
                let mut props = HashMap::new();
                props.insert(
                    "object_id".to_string(),
                    PropertyValue::Column("object_id".to_string()),
                );
                props.insert(
                    "name".to_string(),
                    PropertyValue::Column("name".to_string()),
                );
                props.insert(
                    "type".to_string(),
                    PropertyValue::Column("type".to_string()),
                );
                props
            },
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

    // Create PARENT relationship (self-referencing FK)
    // parent_id column on fs_objects points to object_id on same table
    relationships.insert(
        "PARENT".to_string(),
        RelationshipSchema {
            database: "test".to_string(),
            table_name: "fs_objects".to_string(), // Same as node table!
            column_names: vec![],
            from_node: "Object".to_string(),
            to_node: "Object".to_string(), // Self-referencing
            from_node_table: "fs_objects".to_string(),
            to_node_table: "fs_objects".to_string(),
            from_id: "parent_id".to_string(), // FK column
            to_id: "object_id".to_string(),   // PK column
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
            is_fk_edge: true, // Self-referencing FK pattern
            constraints: None,
            edge_id_types: None,
        },
    );

    GraphSchema::build(1, "test".to_string(), nodes, relationships)
}

fn create_non_self_referencing_fk_schema() -> GraphSchema {
    use crate::graph_catalog::expression_parser::PropertyValue;

    let mut nodes = HashMap::new();
    let mut relationships = HashMap::new();

    // Create Order node
    nodes.insert(
        "Order".to_string(),
        NodeSchema {
            database: "test".to_string(),
            table_name: "orders".to_string(),
            column_names: vec![
                "order_id".to_string(),
                "customer_id".to_string(),
                "total".to_string(),
            ],
            primary_keys: "order_id".to_string(),
            node_id: NodeIdSchema::single("order_id".to_string(), "UInt64".to_string()),
            property_mappings: {
                let mut props = HashMap::new();
                props.insert(
                    "order_id".to_string(),
                    PropertyValue::Column("order_id".to_string()),
                );
                props.insert(
                    "total".to_string(),
                    PropertyValue::Column("total".to_string()),
                );
                props
            },
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

    // Create Customer node
    nodes.insert(
        "Customer".to_string(),
        NodeSchema {
            database: "test".to_string(),
            table_name: "customers".to_string(),
            column_names: vec!["customer_id".to_string(), "name".to_string()],
            primary_keys: "customer_id".to_string(),
            node_id: NodeIdSchema::single("customer_id".to_string(), "UInt64".to_string()),
            property_mappings: {
                let mut props = HashMap::new();
                props.insert(
                    "customer_id".to_string(),
                    PropertyValue::Column("customer_id".to_string()),
                );
                props.insert(
                    "name".to_string(),
                    PropertyValue::Column("name".to_string()),
                );
                props
            },
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

    // Create PLACED_BY relationship (non-self-referencing FK)
    // customer_id column on orders points to customer_id on customers
    relationships.insert(
        "PLACED_BY".to_string(),
        RelationshipSchema {
            database: "test".to_string(),
            table_name: "orders".to_string(), // Same as Order node table!
            column_names: vec![],
            from_node: "Order".to_string(),
            to_node: "Customer".to_string(), // Different table
            from_node_table: "orders".to_string(),
            to_node_table: "customers".to_string(),
            from_id: "order_id".to_string(),  // Order's PK
            to_id: "customer_id".to_string(), // FK pointing to Customer
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
            is_fk_edge: true, // FK-edge pattern (non-self-ref)
            constraints: None,
            edge_id_types: None,
        },
    );

    GraphSchema::build(1, "test".to_string(), nodes, relationships)
}

#[test]
fn test_fk_edge_pattern_self_referencing() {
    // Test self-referencing FK: (child:Object)-[:PARENT]->(parent:Object)
    let schema = create_self_referencing_fk_schema();

    // Verify schema detected FK pattern
    let rel_schema = schema.get_relationships_schemas().get("PARENT").unwrap();
    assert!(
        rel_schema.is_fk_edge,
        "PARENT relationship should be FK-edge pattern"
    );
    assert_eq!(rel_schema.from_node, "Object");
    assert_eq!(rel_schema.to_node, "Object");
    assert_eq!(rel_schema.from_id, "parent_id"); // FK column
    assert_eq!(rel_schema.to_id, "object_id"); // PK column
}

#[test]
fn test_fk_edge_pattern_non_self_referencing() {
    // Test non-self-ref FK: (o:Order)-[:PLACED_BY]->(c:Customer)
    let schema = create_non_self_referencing_fk_schema();

    // Verify schema detected FK pattern
    let rel_schema = schema.get_relationships_schemas().get("PLACED_BY").unwrap();
    assert!(
        rel_schema.is_fk_edge,
        "PLACED_BY relationship should be FK-edge pattern"
    );
    assert_eq!(rel_schema.from_node, "Order");
    assert_eq!(rel_schema.to_node, "Customer");
    assert_eq!(rel_schema.from_id, "order_id"); // Order's PK
    assert_eq!(rel_schema.to_id, "customer_id"); // FK to Customer
}

#[test]
fn test_standard_edge_is_not_fk_pattern() {
    // Verify standard edge tables are NOT marked as FK pattern
    let schema = create_test_graph_schema();

    let follows = schema.get_relationships_schemas().get("FOLLOWS::Person::Person").unwrap();
    assert!(!follows.is_fk_edge, "FOLLOWS should NOT be FK-edge pattern");

    let works_at = schema.get_relationships_schemas().get("WORKS_AT::Person::Company").unwrap();
    assert!(
        !works_at.is_fk_edge,
        "WORKS_AT should NOT be FK-edge pattern"
    );
}

// ========================================================================
// Phase 4 Tests: Relationship Uniqueness Constraints
// ========================================================================

#[test]
fn test_no_uniqueness_constraints_for_single_relationship() {
    // Single relationship pattern should not generate constraints
    let _analyzer = GraphJoinInference::new();
    let graph_schema = create_test_graph_schema();

    let metadata = PatternGraphMetadata {
        nodes: HashMap::new(),
        edges: vec![PatternEdgeInfo {
            alias: "r1".to_string(),
            rel_types: vec!["FOLLOWS".to_string()],
            from_node: "a".to_string(),
            to_node: "b".to_string(),
            is_referenced: true,
            is_vlp: false,
            is_shortest_path: false,
            direction: Direction::Outgoing,
            is_optional: false,
        }],
    };

    let constraints =
        crate::query_planner::analyzer::graph_join::cross_branch::generate_relationship_uniqueness_constraints(&metadata, &graph_schema);

    assert_eq!(
        constraints.len(),
        0,
        "Single relationship should not generate constraints"
    );
}

#[test]
fn test_uniqueness_constraints_for_two_relationships() {
    // Two-hop pattern should generate 1 constraint: r1 != r2
    let _analyzer = GraphJoinInference::new();
    let graph_schema = create_test_graph_schema();

    let metadata = PatternGraphMetadata {
        nodes: HashMap::new(),
        edges: vec![
            PatternEdgeInfo {
                alias: "r1".to_string(),
                rel_types: vec!["FOLLOWS".to_string()],
                from_node: "a".to_string(),
                to_node: "b".to_string(),
                is_referenced: true,
                is_vlp: false,
                is_shortest_path: false,
                direction: Direction::Outgoing,
                is_optional: false,
            },
            PatternEdgeInfo {
                alias: "r2".to_string(),
                rel_types: vec!["FOLLOWS".to_string()],
                from_node: "b".to_string(),
                to_node: "c".to_string(),
                is_referenced: true,
                is_vlp: false,
                is_shortest_path: false,
                direction: Direction::Outgoing,
                is_optional: false,
            },
        ],
    };

    let constraints =
        crate::query_planner::analyzer::graph_join::cross_branch::generate_relationship_uniqueness_constraints(&metadata, &graph_schema);

    assert_eq!(
        constraints.len(),
        1,
        "Two relationships should generate 1 constraint"
    );

    // Verify it's a composite constraint (from_id OR to_id inequality)
    match &constraints[0] {
        LogicalExpr::OperatorApplicationExp(op) => {
            assert_eq!(op.operator, Operator::Or, "Composite ID should use OR");
            assert_eq!(
                op.operands.len(),
                2,
                "Should have 2 operands (from_id and to_id)"
            );
        }
        _ => panic!("Expected OperatorApplicationExp with OR"),
    }
}

#[test]
fn test_uniqueness_constraints_for_three_relationships() {
    // Three-hop pattern should generate 3 constraints: r1!=r2, r1!=r3, r2!=r3
    let _analyzer = GraphJoinInference::new();
    let graph_schema = create_test_graph_schema();

    let metadata = PatternGraphMetadata {
        nodes: HashMap::new(),
        edges: vec![
            PatternEdgeInfo {
                alias: "r1".to_string(),
                rel_types: vec!["FOLLOWS".to_string()],
                from_node: "a".to_string(),
                to_node: "b".to_string(),
                is_referenced: true,
                is_vlp: false,
                is_shortest_path: false,
                direction: Direction::Outgoing,
                is_optional: false,
            },
            PatternEdgeInfo {
                alias: "r2".to_string(),
                rel_types: vec!["FOLLOWS".to_string()],
                from_node: "b".to_string(),
                to_node: "c".to_string(),
                is_referenced: true,
                is_vlp: false,
                is_shortest_path: false,
                direction: Direction::Outgoing,
                is_optional: false,
            },
            PatternEdgeInfo {
                alias: "r3".to_string(),
                rel_types: vec!["FOLLOWS".to_string()],
                from_node: "c".to_string(),
                to_node: "d".to_string(),
                is_referenced: true,
                is_vlp: false,
                is_shortest_path: false,
                direction: Direction::Outgoing,
                is_optional: false,
            },
        ],
    };

    let constraints =
        crate::query_planner::analyzer::graph_join::cross_branch::generate_relationship_uniqueness_constraints(&metadata, &graph_schema);

    // Combinatorial: C(3,2) = 3 pairs
    assert_eq!(
        constraints.len(),
        3,
        "Three relationships should generate 3 pairwise constraints"
    );
}

#[test]
fn test_skip_vlp_relationships_in_uniqueness() {
    // VLP relationships should be skipped in uniqueness constraint generation
    let _analyzer = GraphJoinInference::new();
    let graph_schema = create_test_graph_schema();

    let metadata = PatternGraphMetadata {
        nodes: HashMap::new(),
        edges: vec![
            PatternEdgeInfo {
                alias: "r1".to_string(),
                rel_types: vec!["FOLLOWS".to_string()],
                from_node: "a".to_string(),
                to_node: "b".to_string(),
                is_referenced: true,
                is_vlp: true, // VLP edge
                is_shortest_path: false,
                direction: Direction::Outgoing,
                is_optional: false,
            },
            PatternEdgeInfo {
                alias: "r2".to_string(),
                rel_types: vec!["FOLLOWS".to_string()],
                from_node: "b".to_string(),
                to_node: "c".to_string(),
                is_referenced: true,
                is_vlp: false,
                is_shortest_path: false,
                direction: Direction::Outgoing,
                is_optional: false,
            },
        ],
    };

    let constraints =
        crate::query_planner::analyzer::graph_join::cross_branch::generate_relationship_uniqueness_constraints(&metadata, &graph_schema);

    assert_eq!(constraints.len(), 0, "VLP relationships should be skipped");
}

#[test]
fn test_uniqueness_constraints_with_different_relationship_types() {
    // Mixed relationship types should still generate constraints
    let _analyzer = GraphJoinInference::new();
    let graph_schema = create_test_graph_schema();

    let metadata = PatternGraphMetadata {
        nodes: HashMap::new(),
        edges: vec![
            PatternEdgeInfo {
                alias: "f1".to_string(),
                rel_types: vec!["FOLLOWS".to_string()],
                from_node: "a".to_string(),
                to_node: "b".to_string(),
                is_referenced: true,
                is_vlp: false,
                is_shortest_path: false,
                direction: Direction::Outgoing,
                is_optional: false,
            },
            PatternEdgeInfo {
                alias: "w1".to_string(),
                rel_types: vec!["WORKS_AT".to_string()],
                from_node: "b".to_string(),
                to_node: "c".to_string(),
                is_referenced: true,
                is_vlp: false,
                is_shortest_path: false,
                direction: Direction::Outgoing,
                is_optional: false,
            },
        ],
    };

    let constraints =
        crate::query_planner::analyzer::graph_join::cross_branch::generate_relationship_uniqueness_constraints(&metadata, &graph_schema);

    assert_eq!(
        constraints.len(),
        1,
        "Different relationship types should still generate constraints"
    );
}
