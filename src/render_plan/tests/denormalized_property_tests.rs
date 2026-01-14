//! Tests for denormalized property access in edge tables
//!
//! These tests verify that when properties are denormalized (copied from node tables
//! into edge tables), the query generator can access them directly without JOINs.

use std::collections::HashMap;

use crate::graph_catalog::graph_schema::{
    GraphSchema, NodeIdSchema, NodeSchema, RelationshipSchema,
};
use crate::render_plan::cte_generation::{
    map_property_to_column_with_relationship_context, NodeRole,
};
use crate::server::GLOBAL_SCHEMAS;
use serial_test::serial;

/// Setup test schema with denormalized properties
fn setup_denormalized_schema() -> GraphSchema {
    let mut nodes = HashMap::new();
    let mut relationships = HashMap::new();

    // Airport nodes (minimal - only ID)
    let mut airport_props = HashMap::new();
    airport_props.insert(
        "code".to_string(),
        crate::graph_catalog::expression_parser::PropertyValue::Column("airport_code".to_string()),
    );
    airport_props.insert(
        "city".to_string(),
        crate::graph_catalog::expression_parser::PropertyValue::Column("city_name".to_string()),
    );
    airport_props.insert(
        "state".to_string(),
        crate::graph_catalog::expression_parser::PropertyValue::Column("state_code".to_string()),
    );

    nodes.insert(
        "Airport".to_string(),
        NodeSchema {
            database: "test_db".to_string(),
            table_name: "airports".to_string(),
            column_names: vec![
                "airport_code".to_string(),
                "city_name".to_string(),
                "state_code".to_string(),
            ],
            primary_keys: "airport_id".to_string(),
            node_id: NodeIdSchema::single("airport_id".to_string(), "UInt64".to_string()),
            property_mappings: airport_props,
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

    // Flight edges with denormalized properties
    let mut flight_props = HashMap::new();
    flight_props.insert(
        "flight_num".to_string(),
        crate::graph_catalog::expression_parser::PropertyValue::Column("flight_number".to_string()),
    );
    flight_props.insert(
        "airline".to_string(),
        crate::graph_catalog::expression_parser::PropertyValue::Column("carrier".to_string()),
    );

    // Denormalized origin properties (from from_node)
    let mut from_node_props = HashMap::new();
    from_node_props.insert(
        "city".to_string(),
        crate::graph_catalog::expression_parser::PropertyValue::Column("origin_city".to_string()),
    );
    from_node_props.insert(
        "state".to_string(),
        crate::graph_catalog::expression_parser::PropertyValue::Column("origin_state".to_string()),
    );

    // Denormalized destination properties (from to_node)
    let mut to_node_props = HashMap::new();
    to_node_props.insert(
        "city".to_string(),
        crate::graph_catalog::expression_parser::PropertyValue::Column("dest_city".to_string()),
    );
    to_node_props.insert(
        "state".to_string(),
        crate::graph_catalog::expression_parser::PropertyValue::Column("dest_state".to_string()),
    );

    relationships.insert(
        "FLIGHT".to_string(),
        RelationshipSchema {
            database: "test_db".to_string(),
            table_name: "flights".to_string(),
            column_names: vec![
                "origin_id".to_string(),
                "dest_id".to_string(),
                "flight_number".to_string(),
                "carrier".to_string(),
                "origin_city".to_string(),
                "origin_state".to_string(),
                "dest_city".to_string(),
                "dest_state".to_string(),
            ],
            from_node: "Airport".to_string(),
            to_node: "Airport".to_string(),
            from_node_table: "airports".to_string(),
            to_node_table: "airports".to_string(),
            from_id: "origin_id".to_string(),
            to_id: "dest_id".to_string(),
            from_node_id_dtype: "UInt64".to_string(),
            to_node_id_dtype: "UInt64".to_string(),
            property_mappings: flight_props,
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
            from_node_properties: Some(
                from_node_props
                    .into_iter()
                    .map(|(k, v)| (k, v.raw().to_string()))
                    .collect(),
            ),
            to_node_properties: Some(
                to_node_props
                    .into_iter()
                    .map(|(k, v)| (k, v.raw().to_string()))
                    .collect(),
            ),
            is_fk_edge: false,
            constraints: None,
        },
    );

    GraphSchema::build(1, "test_db".to_string(), nodes, relationships)
}

/// Setup global schema for testing
fn init_test_schema(schema: GraphSchema) {
    use tokio::sync::RwLock;

    const SCHEMA_NAME: &str = "default";

    // Always recreate for proper test isolation

    let mut schemas = HashMap::new();
    schemas.insert(SCHEMA_NAME.to_string(), schema);

    // Initialize GLOBAL_SCHEMAS
    // For tests, check if already initialized
    if let Some(schemas_lock) = GLOBAL_SCHEMAS.get() {
        // Update existing
        if let Ok(mut schemas_guard) = schemas_lock.try_write() {
            *schemas_guard = schemas;
        }
    } else {
        // Initialize for the first time
        let _ = GLOBAL_SCHEMAS.set(RwLock::new(schemas));
    }
}

#[test]
#[serial]
fn test_denormalized_from_node_property() {
    let schema = setup_denormalized_schema();
    init_test_schema(schema);

    // Access denormalized property from origin Airport
    let result = map_property_to_column_with_relationship_context(
        "city",
        "Airport",
        Some("FLIGHT"),
        Some(NodeRole::From), // FROM node -> use from_node_properties
    );

    assert!(result.is_ok());
    assert_eq!(
        result.unwrap(),
        "origin_city",
        "Should return denormalized column from edge table (origin_city)"
    );
}

#[test]
#[serial]
fn test_denormalized_to_node_property() {
    let schema = setup_denormalized_schema();
    init_test_schema(schema);

    // For to_node properties, we need a different test setup
    // In reality, the query generator determines which side based on the query pattern
    // For this test, we'll manually check the to_node_properties path

    // Now we can explicitly pass TO role
    let result = map_property_to_column_with_relationship_context(
        "city",
        "Airport",
        Some("FLIGHT"),
        Some(NodeRole::To), // TO node -> use to_node_properties
    );

    assert!(result.is_ok());
    // Now correctly returns dest_city because we passed NodeRole::To
    assert_eq!(result.unwrap(), "dest_city");
}

#[test]
#[serial]
fn test_fallback_to_node_property() {
    let schema = setup_denormalized_schema();
    init_test_schema(schema);

    // Access property that's NOT denormalized (only in node table)
    let result = map_property_to_column_with_relationship_context(
        "code", // Not denormalized in FLIGHT edges
        "Airport",
        Some("FLIGHT"),
        None, // Role doesn't matter for non-denormalized properties
    );

    assert!(result.is_ok());
    assert_eq!(
        result.unwrap(),
        "airport_code",
        "Should fall back to node table property mapping"
    );
}

#[test]
#[serial]
fn test_no_relationship_context() {
    let schema = setup_denormalized_schema();
    init_test_schema(schema);

    // Without relationship context, should use node property mapping
    let result = map_property_to_column_with_relationship_context(
        "city", "Airport", None, // No relationship context
        None, // No role needed without relationship context
    );

    assert!(result.is_ok());
    assert_eq!(
        result.unwrap(),
        "city_name",
        "Without relationship context, should use node property mapping"
    );
}

#[test]
#[serial]
fn test_relationship_property() {
    let schema = setup_denormalized_schema();
    init_test_schema(schema);

    // Relationship properties (not node properties) should still work via fallback
    // Note: This test accesses a property that doesn't exist on Airport nodes
    let result = map_property_to_column_with_relationship_context(
        "flight_num", // This is a relationship property, not a node property
        "Airport",
        Some("FLIGHT"),
        None, // Role irrelevant for this test
    );

    // This should fail because flight_num is not a node property
    assert!(
        result.is_err(),
        "Relationship properties should fail when queried as node properties"
    );
}

#[test]
#[serial]
fn test_multiple_relationships_same_node() {
    let mut schema = setup_denormalized_schema();

    // Add another relationship with different denormalized properties
    let mut authored_props = HashMap::new();
    authored_props.insert(
        "timestamp".to_string(),
        crate::graph_catalog::expression_parser::PropertyValue::Column("created_at".to_string()),
    );

    let mut author_props = HashMap::new();
    author_props.insert(
        "name".to_string(),
        crate::graph_catalog::expression_parser::PropertyValue::Column("author_name".to_string()),
    );

    schema.insert_relationship_schema(
        "AUTHORED".to_string(),
        RelationshipSchema {
            database: "test_db".to_string(),
            table_name: "posts".to_string(),
            column_names: vec![
                "author_id".to_string(),
                "post_id".to_string(),
                "author_name".to_string(),
            ],
            from_node: "User".to_string(),
            to_node: "Post".to_string(),
            from_node_table: "users".to_string(),
            to_node_table: "posts".to_string(),
            from_id: "author_id".to_string(),
            to_id: "post_id".to_string(),
            from_node_id_dtype: "UInt64".to_string(),
            to_node_id_dtype: "UInt64".to_string(),
            property_mappings: authored_props,
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_node_properties: Some(
                author_props
                    .into_iter()
                    .map(|(k, v)| (k, v.raw().to_string()))
                    .collect(),
            ),
            to_node_properties: None,
            from_label_values: None,
            to_label_values: None,
            is_fk_edge: false,
            constraints: None,
        },
    );

    init_test_schema(schema);

    // Query for property in FLIGHT relationship
    let result1 = map_property_to_column_with_relationship_context(
        "city",
        "Airport",
        Some("FLIGHT"),
        Some(NodeRole::From),
    );
    assert!(result1.is_ok());
    assert_eq!(result1.unwrap(), "origin_city");

    // Query for same property name in different relationship (should fail)
    let result2 = map_property_to_column_with_relationship_context(
        "city",
        "Airport",
        Some("AUTHORED"), // Wrong relationship
        None,
    );
    // Should fall back to node property mapping
    assert!(result2.is_ok());
    assert_eq!(
        result2.unwrap(),
        "city_name",
        "Should fall back to node property when relationship doesn't have denormalized property"
    );
}

#[test]
#[serial]
fn test_denormalized_edge_table_same_table_for_node_and_edge() {
    // Test the true denormalized edge table pattern:
    // - Node and edge use the SAME table (e.g., flights table for both Airport nodes and FLIGHT edges)
    // - Node id_column refers to columns that exist in from_node_properties/to_node_properties
    // - No separate node table, no JOINs needed

    let mut nodes = HashMap::new();
    let mut relationships = HashMap::new();

    // Airport nodes - uses FLIGHTS table (not separate airports table)
    let airport_props = HashMap::new();
    // For denormalized edge tables, node properties come from the edge table
    // So we leave property_mappings empty - they're derived from from_node_properties/to_node_properties

    nodes.insert(
        "Airport".to_string(),
        NodeSchema {
            database: "test_db".to_string(),
            table_name: "flights".to_string(), // ✅ Same table as edge!
            column_names: vec![
                "origin_code".to_string(),
                "dest_code".to_string(),
                "origin_city".to_string(),
                "dest_city".to_string(),
            ],
            primary_keys: "code".to_string(), // Logical ID property
            node_id: NodeIdSchema::single(
                "code".to_string(), // Maps to origin_code/dest_code
                "String".to_string(),
            ),
            property_mappings: airport_props, // Empty - derived from edge
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

    // Flight edges with denormalized properties
    let mut flight_props = HashMap::new();
    flight_props.insert(
        "flight_num".to_string(),
        crate::graph_catalog::expression_parser::PropertyValue::Column("flight_number".to_string()),
    );
    flight_props.insert(
        "airline".to_string(),
        crate::graph_catalog::expression_parser::PropertyValue::Column("carrier".to_string()),
    );

    // Denormalized origin properties
    let mut from_node_props = HashMap::new();
    from_node_props.insert(
        "code".to_string(),
        crate::graph_catalog::expression_parser::PropertyValue::Column("origin_code".to_string()),
    );
    from_node_props.insert(
        "city".to_string(),
        crate::graph_catalog::expression_parser::PropertyValue::Column("origin_city".to_string()),
    );

    // Denormalized destination properties
    let mut to_node_props = HashMap::new();
    to_node_props.insert(
        "code".to_string(),
        crate::graph_catalog::expression_parser::PropertyValue::Column("dest_code".to_string()),
    );
    to_node_props.insert(
        "city".to_string(),
        crate::graph_catalog::expression_parser::PropertyValue::Column("dest_city".to_string()),
    );

    relationships.insert(
        "FLIGHT".to_string(),
        RelationshipSchema {
            database: "test_db".to_string(),
            table_name: "flights".to_string(), // ✅ Same table as node!
            column_names: vec![
                "origin_code".to_string(),
                "dest_code".to_string(),
                "flight_number".to_string(),
                "carrier".to_string(),
                "origin_city".to_string(),
                "dest_city".to_string(),
            ],
            from_node: "Airport".to_string(),
            to_node: "Airport".to_string(),
            from_node_table: "flights".to_string(),
            to_node_table: "flights".to_string(),
            from_id: "origin_code".to_string(),
            to_id: "dest_code".to_string(),
            from_node_id_dtype: "String".to_string(),
            to_node_id_dtype: "String".to_string(),
            property_mappings: flight_props,
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
            from_node_properties: Some(
                from_node_props
                    .into_iter()
                    .map(|(k, v)| (k, v.raw().to_string()))
                    .collect(),
            ),
            to_node_properties: Some(
                to_node_props
                    .into_iter()
                    .map(|(k, v)| (k, v.raw().to_string()))
                    .collect(),
            ),
            is_fk_edge: false,
            constraints: None,
        },
    );

    let schema = GraphSchema::build(1, "test_db".to_string(), nodes, relationships);
    init_test_schema(schema);

    // Test 1: Access denormalized origin city
    let result = map_property_to_column_with_relationship_context(
        "city",
        "Airport",
        Some("FLIGHT"),
        Some(NodeRole::From),
    );
    assert!(result.is_ok());
    assert_eq!(
        result.unwrap(),
        "origin_city",
        "Should map to denormalized origin_city in flights table"
    );

    // Test 2: Access node ID property (should map through from_node_properties)
    let result = map_property_to_column_with_relationship_context(
        "code",
        "Airport",
        Some("FLIGHT"),
        Some(NodeRole::From),
    );
    assert!(result.is_ok());
    assert_eq!(
        result.unwrap(),
        "origin_code",
        "Node ID property should map through from_node_properties"
    );
}

/// Integration test: Verify analyzer applies denormalized property mapping through public API
///
/// This test uses ViewScan nodes with from_node_properties/to_node_properties
/// to match the real query execution path for denormalized edge schemas.
#[test]
#[serial]
fn test_analyzer_denormalized_property_integration() {
    use crate::query_planner::analyzer;
    use crate::query_planner::logical_expr::{
        LogicalExpr, Operator, OperatorApplication, PropertyAccess, TableAlias,
    };
    use crate::query_planner::logical_plan::LogicalPlan;
    use crate::query_planner::logical_plan::ViewScan;
    use crate::query_planner::logical_plan::{Filter, Projection, ProjectionItem};
    use crate::query_planner::plan_ctx::PlanCtx;
    use std::sync::Arc;

    let schema = setup_denormalized_schema();
    init_test_schema(schema.clone());

    // Create a logical plan with a denormalized property access
    // Simulates: MATCH (origin:Airport)-[:FLIGHT]->(dest:Airport) WHERE origin.city = 'Los Angeles' RETURN dest.city

    // Build from_node_properties for origin (same as relationship schema)
    let mut from_node_props = HashMap::new();
    from_node_props.insert(
        "city".to_string(),
        crate::graph_catalog::expression_parser::PropertyValue::Column("origin_city".to_string()),
    );
    from_node_props.insert(
        "state".to_string(),
        crate::graph_catalog::expression_parser::PropertyValue::Column("origin_state".to_string()),
    );

    // Build to_node_properties for destination
    let mut to_node_props = HashMap::new();
    to_node_props.insert(
        "city".to_string(),
        crate::graph_catalog::expression_parser::PropertyValue::Column("dest_city".to_string()),
    );
    to_node_props.insert(
        "state".to_string(),
        crate::graph_catalog::expression_parser::PropertyValue::Column("dest_state".to_string()),
    );

    // Origin node using ViewScan with from_node_properties
    let origin_view_scan = Arc::new(LogicalPlan::ViewScan(Arc::new(ViewScan {
        source_table: "flights".to_string(),
        view_filter: None,
        property_mapping: HashMap::new(),
        id_column: "origin_id".to_string(),
        output_schema: vec![],
        projections: vec![],
        from_id: Some("origin_id".to_string()),
        to_id: Some("dest_id".to_string()),
        input: None,
        view_parameter_names: None,
        view_parameter_values: None,
        use_final: false,
        is_denormalized: true,
        from_node_properties: Some(from_node_props.clone()),
        to_node_properties: None,
        type_column: None,
        type_values: None,
        from_label_column: None,
        to_label_column: None,
        schema_filter: None,
    })));

    let origin_node = Arc::new(LogicalPlan::GraphNode(
        crate::query_planner::logical_plan::GraphNode {
            input: origin_view_scan,
            alias: "origin".to_string(),
            label: Some("Airport".to_string()),
            is_denormalized: true,
            projected_columns: None,
        },
    ));

    // Destination node using ViewScan with to_node_properties
    let dest_view_scan = Arc::new(LogicalPlan::ViewScan(Arc::new(ViewScan {
        source_table: "flights".to_string(),
        view_filter: None,
        property_mapping: HashMap::new(),
        id_column: "dest_id".to_string(),
        output_schema: vec![],
        projections: vec![],
        from_id: Some("origin_id".to_string()),
        to_id: Some("dest_id".to_string()),
        input: None,
        view_parameter_names: None,
        view_parameter_values: None,
        use_final: false,
        is_denormalized: true,
        from_node_properties: None,
        to_node_properties: Some(to_node_props.clone()),
        type_column: None,
        type_values: None,
        from_label_column: None,
        to_label_column: None,
        schema_filter: None,
    })));

    let dest_node = Arc::new(LogicalPlan::GraphNode(
        crate::query_planner::logical_plan::GraphNode {
            input: dest_view_scan,
            alias: "dest".to_string(),
            label: Some("Airport".to_string()),
            is_denormalized: true,
            projected_columns: None,
        },
    ));

    let flight_scan = Arc::new(LogicalPlan::Empty);

    let graph_rel = Arc::new(LogicalPlan::GraphRel(
        crate::query_planner::logical_plan::GraphRel {
            left: origin_node,
            center: flight_scan,
            right: dest_node,
            alias: "flight".to_string(),
            direction: crate::query_planner::logical_expr::Direction::Outgoing,
            left_connection: "origin".to_string(),
            right_connection: "dest".to_string(),
            is_rel_anchor: false,
            variable_length: None,
            shortest_path_mode: None,
            path_variable: None,
            where_predicate: None,
            labels: Some(vec!["FLIGHT".to_string()]),
            is_optional: None,
            anchor_connection: None,
            cte_references: std::collections::HashMap::new(),
        },
    ));

    // Add a filter on denormalized property: WHERE origin.city = 'Los Angeles'
    let filter_predicate = LogicalExpr::OperatorApplicationExp(OperatorApplication {
        operator: Operator::Equal,
        operands: vec![
            LogicalExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias("origin".to_string()),
                column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                    "city".to_string(),
                ),
            }),
            LogicalExpr::Literal(crate::query_planner::logical_expr::Literal::String(
                "Los Angeles".to_string(),
            )),
        ],
    });

    let filtered_plan = Arc::new(LogicalPlan::Filter(Filter {
        input: graph_rel,
        predicate: filter_predicate,
    }));

    // Add projection: RETURN dest.city
    let projection = Arc::new(LogicalPlan::Projection(Projection {
        input: filtered_plan,
        items: vec![ProjectionItem {
            expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias("dest".to_string()),
                column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                    "city".to_string(),
                ),
            }),
            col_alias: Some(crate::query_planner::logical_expr::ColumnAlias(
                "dest_city".to_string(),
            )),
        }],
        distinct: false,
    }));

    // Setup plan context
    let mut plan_ctx = PlanCtx::new(Arc::new(schema.clone()));
    plan_ctx.insert_table_ctx(
        "origin".to_string(),
        crate::query_planner::plan_ctx::TableCtx::build(
            "origin".to_string(),
            Some(vec!["Airport".to_string()]),
            vec![],
            false,
            true,
        ),
    );
    plan_ctx.insert_table_ctx(
        "dest".to_string(),
        crate::query_planner::plan_ctx::TableCtx::build(
            "dest".to_string(),
            Some(vec!["Airport".to_string()]),
            vec![],
            false,
            true,
        ),
    );
    plan_ctx.insert_table_ctx(
        "flight".to_string(),
        crate::query_planner::plan_ctx::TableCtx::build(
            "flight".to_string(),
            Some(vec!["FLIGHT".to_string()]),
            vec![],
            true,
            false,
        ),
    );

    // Run analyzer passes (this should apply property mapping)
    let analyzed_plan = analyzer::initial_analyzing(projection, &mut plan_ctx, &schema)
        .expect("Initial analysis should succeed");

    // The filter is extracted into plan_ctx, so check there instead
    let origin_ctx = plan_ctx
        .get_table_ctx("origin")
        .expect("origin table context should exist");

    let filters = origin_ctx.get_filters();
    assert!(
        !filters.is_empty(),
        "Filter should be extracted to table context"
    );

    // Verify the filter was mapped correctly
    match &filters[0] {
        LogicalExpr::OperatorApplicationExp(op) => {
            if let Some(LogicalExpr::PropertyAccessExp(prop)) = op.operands.first() {
                assert_eq!(
                    prop.column.raw(),
                    "origin_city",
                    "Property 'origin.city' should be mapped to 'origin_city' by analyzer"
                );
            } else {
                panic!("Expected PropertyAccessExp in filter predicate");
            }
        }
        _ => panic!("Expected OperatorApplicationExp"),
    }

    // Also verify the projection was mapped correctly
    // Walk the plan to find the Projection node
    fn find_projection_items(plan: &LogicalPlan) -> Option<Vec<ProjectionItem>> {
        match plan {
            LogicalPlan::Projection(proj) => Some(proj.items.clone()),
            LogicalPlan::Filter(filter) => find_projection_items(&filter.input),
            LogicalPlan::GraphRel(rel) => {
                find_projection_items(&rel.left).or_else(|| find_projection_items(&rel.right))
            }
            _ => None,
        }
    }

    if let Some(items) = find_projection_items(&analyzed_plan) {
        if let Some(item) = items.first() {
            match &item.expression {
                LogicalExpr::PropertyAccessExp(prop) => {
                    assert_eq!(
                        prop.column.raw(),
                        "dest_city",
                        "Property 'dest.city' should be mapped to 'dest_city' by analyzer"
                    );
                }
                _ => panic!("Expected PropertyAccessExp in projection"),
            }
        }
    } else {
        panic!("Projection not found in analyzed plan");
    }
}

/// Test: MATCH (a:Airport) RETURN a  (standalone denormalized node with whole-node return)
/// This tests the case where a denormalized node is returned without a relationship pattern.
/// The bug was that get_properties_with_table_alias looked at empty property_mapping
/// instead of from_node_properties/to_node_properties.
#[test]
#[serial]
fn test_denormalized_standalone_node_return_all_properties() {
    use crate::graph_catalog::expression_parser::PropertyValue;
    use crate::query_planner::logical_expr::{LogicalExpr, TableAlias};
    use crate::query_planner::logical_plan::{
        GraphNode, LogicalPlan, Projection, ProjectionItem, ViewScan,
    };
    use crate::render_plan::plan_builder::RenderPlanBuilder; // Import trait!
    use std::sync::Arc;

    // Create a denormalized ViewScan with from_node_properties (single position case)
    let mut from_node_props = HashMap::new();
    from_node_props.insert(
        "code".to_string(),
        PropertyValue::Column("Origin".to_string()),
    );
    from_node_props.insert(
        "city".to_string(),
        PropertyValue::Column("OriginCityName".to_string()),
    );

    let mut view_scan = ViewScan::new(
        "test_db.flights".to_string(),
        None,
        HashMap::new(), // Empty property_mapping - this is the denormalized pattern!
        "code".to_string(),
        vec![],
        vec![],
    );
    view_scan.is_denormalized = true;
    view_scan.from_node_properties = Some(from_node_props);

    // Wrap in GraphNode (as done by match_clause.rs)
    let graph_node = LogicalPlan::GraphNode(GraphNode {
        input: Arc::new(LogicalPlan::ViewScan(Arc::new(view_scan))),
        alias: "a".to_string(),
        label: Some("Airport".to_string()),
        is_denormalized: true,
        projected_columns: None,
    });

    // Create Projection with RETURN a (whole node return)
    let projection = LogicalPlan::Projection(Projection {
        items: vec![ProjectionItem {
            expression: LogicalExpr::TableAlias(TableAlias("a".to_string())),
            col_alias: None,
        }],
        input: Arc::new(graph_node),
        distinct: false,
    });

    // Test: get_properties_with_table_alias should find properties from from_node_properties
    match projection.get_properties_with_table_alias("a") {
        Ok((props, _table_alias)) => {
            assert!(
                !props.is_empty(),
                "Should find properties from from_node_properties for denormalized node"
            );
            assert_eq!(props.len(), 2, "Should find 2 properties (code, city)");

            // Verify the property mappings
            let prop_map: HashMap<_, _> = props.into_iter().collect();
            assert_eq!(
                prop_map.get("code"),
                Some(&"Origin".to_string()),
                "code should map to Origin"
            );
            assert_eq!(
                prop_map.get("city"),
                Some(&"OriginCityName".to_string()),
                "city should map to OriginCityName"
            );

            println!("SUCCESS: Found {} denormalized properties", prop_map.len());
        }
        Err(e) => {
            panic!("get_properties_with_table_alias failed: {:?}", e);
        }
    }

    // Also test extract_select_items - this is where the actual SQL SELECT columns are generated
    match projection.extract_select_items() {
        Ok(select_items) => {
            assert!(
                !select_items.is_empty(),
                "extract_select_items should return non-empty list for RETURN a"
            );
            println!("extract_select_items returned {} items", select_items.len());
            for item in &select_items {
                println!("  {:?}", item);
            }
        }
        Err(e) => {
            panic!("extract_select_items failed: {:?}", e);
        }
    }
}

/// Test: MATCH (a:Airport) RETURN a  with BOTH positions (UNION case)
/// This tests the case where a denormalized node has both from_node_properties AND to_node_properties
/// which results in a UNION ALL structure.
#[test]
#[serial]
fn test_denormalized_standalone_node_both_positions() {
    use crate::graph_catalog::expression_parser::PropertyValue;
    use crate::query_planner::logical_expr::{LogicalExpr, TableAlias};
    use crate::query_planner::logical_plan::{
        GraphNode, LogicalPlan, Projection, ProjectionItem, Union, UnionType, ViewScan,
    };
    use crate::render_plan::plan_builder::RenderPlanBuilder;
    use std::sync::Arc;

    // Create FROM position ViewScan
    let mut from_props = HashMap::new();
    from_props.insert(
        "code".to_string(),
        PropertyValue::Column("Origin".to_string()),
    );
    from_props.insert(
        "city".to_string(),
        PropertyValue::Column("OriginCityName".to_string()),
    );

    let mut from_scan = ViewScan::new(
        "test_db.flights".to_string(),
        None,
        HashMap::new(),
        "code".to_string(),
        vec![],
        vec![],
    );
    from_scan.is_denormalized = true;
    from_scan.from_node_properties = Some(from_props);

    // Create TO position ViewScan
    let mut to_props = HashMap::new();
    to_props.insert(
        "code".to_string(),
        PropertyValue::Column("Dest".to_string()),
    );
    to_props.insert(
        "city".to_string(),
        PropertyValue::Column("DestCityName".to_string()),
    );

    let mut to_scan = ViewScan::new(
        "test_db.flights".to_string(),
        None,
        HashMap::new(),
        "code".to_string(),
        vec![],
        vec![],
    );
    to_scan.is_denormalized = true;
    to_scan.to_node_properties = Some(to_props);

    // Create Union with each branch wrapped in GraphNode (as done by match_clause.rs)
    let from_node = LogicalPlan::GraphNode(GraphNode {
        input: Arc::new(LogicalPlan::ViewScan(Arc::new(from_scan))),
        alias: "a".to_string(),
        label: Some("Airport".to_string()),
        is_denormalized: true,
        projected_columns: None,
    });

    let to_node = LogicalPlan::GraphNode(GraphNode {
        input: Arc::new(LogicalPlan::ViewScan(Arc::new(to_scan))),
        alias: "a".to_string(),
        label: Some("Airport".to_string()),
        is_denormalized: true,
        projected_columns: None,
    });

    let union = LogicalPlan::Union(Union {
        inputs: vec![Arc::new(from_node), Arc::new(to_node)],
        union_type: UnionType::All,
    });

    // Create Projection with RETURN a (whole node return)
    let projection = LogicalPlan::Projection(Projection {
        items: vec![ProjectionItem {
            expression: LogicalExpr::TableAlias(TableAlias("a".to_string())),
            col_alias: None,
        }],
        input: Arc::new(union),
        distinct: false,
    });

    // Test: get_properties_with_table_alias should find properties from the first Union branch
    match projection.get_properties_with_table_alias("a") {
        Ok((props, _table_alias)) => {
            assert!(
                !props.is_empty(),
                "Should find properties from UNION branch for denormalized node"
            );
            println!(
                "SUCCESS (UNION case): Found {} properties: {:?}",
                props.len(),
                props
            );
        }
        Err(e) => {
            panic!(
                "get_properties_with_table_alias failed for UNION case: {:?}",
                e
            );
        }
    }

    // Test extract_select_items for UNION case
    match projection.extract_select_items() {
        Ok(select_items) => {
            assert!(
                !select_items.is_empty(),
                "extract_select_items should return non-empty list for UNION RETURN a"
            );
            println!(
                "UNION extract_select_items returned {} items",
                select_items.len()
            );
            for item in &select_items {
                println!("  {:?}", item);
            }
        }
        Err(e) => {
            panic!("extract_select_items failed for UNION case: {:?}", e);
        }
    }
}
