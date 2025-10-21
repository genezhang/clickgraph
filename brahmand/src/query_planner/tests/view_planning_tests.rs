//! Tests for view-based query functionality

use std::collections::HashMap;
use std::sync::Arc;

use crate::graph_catalog::{GraphViewDefinition, NodeViewMapping, RelationshipViewMapping};
use crate::query_planner::{
    analyzer::view_resolver::ViewResolver,
    logical_expr::{LogicalExpr, ColumnAlias},
    logical_plan::{
        LogicalPlan, ViewScan,
        view_planning::{plan_view_node_scan, plan_view_relationship_scan},
    },
    plan_ctx::PlanCtx,
};

#[test]
fn test_view_node_scan() {
    let mut view_def = GraphViewDefinition::new("test_view");
    let mut node_mapping = NodeViewMapping::new("users_table", "user_id");
    node_mapping.add_property("name", "user_name");
    node_mapping.add_property("age", "user_age");
    view_def.add_node("User", node_mapping);

    let mut plan_ctx = PlanCtx::new();
    let result = plan_view_node_scan(&view_def, "User", &["name", "age"], &mut plan_ctx);
    assert!(result.is_ok());

    let plan = result.unwrap();
    match &*plan {
        LogicalPlan::ViewScan(scan) => {
            assert_eq!(scan.source_table, "users_table");
            assert_eq!(scan.id_column, "user_id");
            assert_eq!(scan.property_mapping.len(), 3); // Including id
            assert_eq!(scan.property_mapping.get("name"), Some(&"user_name".to_string()));
            assert_eq!(scan.property_mapping.get("age"), Some(&"user_age".to_string()));
        }
        _ => panic!("Expected ViewScan plan"),
    }
}

#[test]
fn test_view_relationship_scan() {
    let mut view_def = GraphViewDefinition::new("test_view");
    
    // Add node mappings
    let mut user_mapping = NodeViewMapping::new("users_table", "user_id");
    user_mapping.add_property("name", "user_name");
    view_def.add_node("User", user_mapping);

    let mut post_mapping = NodeViewMapping::new("posts_table", "post_id");
    post_mapping.add_property("title", "post_title");
    view_def.add_node("Post", post_mapping);

    // Add relationship mapping
    let mut rel_mapping = RelationshipViewMapping::new(
        "user_posts_table",
        "user_id",
        "post_id",
    );
    rel_mapping.add_property("created_at", "creation_date");
    view_def.add_relationship("AUTHORED", rel_mapping);

    let mut plan_ctx = PlanCtx::new();
    
    // Create node scan for start node
    let user_scan = plan_view_node_scan(&view_def, "User", &["name"], &mut plan_ctx).unwrap();
    
    // Create relationship scan
    let result = plan_view_relationship_scan(
        &view_def,
        "AUTHORED",
        &["created_at"],
        user_scan,
        &mut plan_ctx,
    );
    assert!(result.is_ok());

    let plan = result.unwrap();
    match &*plan {
        LogicalPlan::ViewScan(scan) => {
            assert_eq!(scan.source_table, "user_posts_table");
            assert_eq!(scan.property_mapping.len(), 3); // Including from_id, to_id
            assert_eq!(scan.property_mapping.get("created_at"), Some(&"creation_date".to_string()));
            assert!(scan.input.is_some());
        }
        _ => panic!("Expected ViewScan plan"),
    }
}

#[test]
fn test_view_sql_generation() {
    let mut view_def = GraphViewDefinition::new("test_view");
    let mut node_mapping = NodeViewMapping::new("users_table", "user_id");
    node_mapping.add_property("name", "user_name");
    node_mapping.add_property("age", "user_age");
    node_mapping.set_filter("age > 18"); // Add a filter condition
    view_def.add_node("User", node_mapping);

    let mut plan_ctx = PlanCtx::new();
    let scan = plan_view_node_scan(&view_def, "User", &["name", "age"], &mut plan_ctx).unwrap();

    // Create projection
    let projection = Arc::new(LogicalPlan::Projection(super::logical_plan::Projection {
        input: scan,
        items: vec![
            super::logical_plan::ProjectionItem {
                expression: LogicalExpr::ColumnAlias(ColumnAlias {
                    table: "users_table".to_string(),
                    column: "user_name".to_string(),
                }),
                col_alias: Some(super::logical_plan::ColumnAlias::new("name")),
            },
        ],
    }));

    // Generate SQL
    let render_plan = projection.to_render_plan().unwrap();
    let sql = render_plan.to_sql();

    // Verify SQL structure
    assert!(sql.contains("SELECT"));
    assert!(sql.contains("FROM users_table"));
    assert!(sql.contains("WHERE age > 18"));
}
