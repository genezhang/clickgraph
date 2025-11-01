//! Tests for multiple relationship types (UNION logic)
//!
//! These tests verify that queries like `MATCH (a)-[:TYPE1|TYPE2]->(b)`
//! generate correct UNION CTEs in the render plan.

use crate::{
    open_cypher_parser,
    query_planner::logical_plan::plan_builder::build_logical_plan,
    render_plan::plan_builder::RenderPlanBuilder,
};
use std::collections::HashMap;
use tokio::sync::RwLock;

// Test schema setup for multiple relationship tests
fn setup_test_schema() {
    use crate::graph_catalog::graph_schema::{GraphSchema, RelationshipSchema};
    use crate::server::GLOBAL_GRAPH_SCHEMA;

    // Check if already set
    if GLOBAL_GRAPH_SCHEMA.get().is_some() {
        return;
    }

    // Create test relationship schemas
    let mut relationships = HashMap::new();
    
    // FOLLOWS -> user_follows
    relationships.insert("FOLLOWS".to_string(), RelationshipSchema {
        table_name: "user_follows".to_string(),
        column_names: vec!["follower_id".to_string(), "followed_id".to_string()],
        from_node: "User".to_string(),
        to_node: "User".to_string(),
        from_column: "follower_id".to_string(),
        to_column: "followed_id".to_string(),
        from_node_id_dtype: "UInt64".to_string(),
        to_node_id_dtype: "UInt64".to_string(),
        property_mappings: HashMap::new(),
    });

    // FRIENDS_WITH -> friendships
    relationships.insert("FRIENDS_WITH".to_string(), RelationshipSchema {
        table_name: "friendships".to_string(),
        column_names: vec!["user1_id".to_string(), "user2_id".to_string()],
        from_node: "User".to_string(),
        to_node: "User".to_string(),
        from_column: "user1_id".to_string(),
        to_column: "user2_id".to_string(),
        from_node_id_dtype: "UInt64".to_string(),
        to_node_id_dtype: "UInt64".to_string(),
        property_mappings: HashMap::new(),
    });

    // PURCHASED -> orders
    relationships.insert("PURCHASED".to_string(), RelationshipSchema {
        table_name: "orders".to_string(),
        column_names: vec!["customer_id".to_string(), "product_id".to_string()],
        from_node: "Customer".to_string(),
        to_node: "Product".to_string(),
        from_column: "customer_id".to_string(),
        to_column: "product_id".to_string(),
        from_node_id_dtype: "UInt64".to_string(),
        to_node_id_dtype: "UInt64".to_string(),
        property_mappings: HashMap::new(),
    });

    // PLACED_ORDER -> orders (same table for this test)
    relationships.insert("PLACED_ORDER".to_string(), RelationshipSchema {
        table_name: "orders".to_string(),
        column_names: vec!["customer_id".to_string(), "order_id".to_string()],
        from_node: "Customer".to_string(),
        to_node: "Order".to_string(),
        from_column: "customer_id".to_string(),
        to_column: "order_id".to_string(),
        from_node_id_dtype: "UInt64".to_string(),
        to_node_id_dtype: "UInt64".to_string(),
        property_mappings: HashMap::new(),
    });

    // ORDER_CONTAINS -> order_items
    relationships.insert("ORDER_CONTAINS".to_string(), RelationshipSchema {
        table_name: "order_items".to_string(),
        column_names: vec!["order_id".to_string(), "product_id".to_string()],
        from_node: "Order".to_string(),
        to_node: "Product".to_string(),
        from_column: "order_id".to_string(),
        to_column: "product_id".to_string(),
        from_node_id_dtype: "UInt64".to_string(),
        to_node_id_dtype: "UInt64".to_string(),
        property_mappings: HashMap::new(),
    });

    // Create empty node and index schemas for now
    let nodes = HashMap::new();
    let relationships_indexes = HashMap::new();

    let schema = GraphSchema::build(1, nodes, relationships, relationships_indexes);
    
    // Try to set the global schema, ignore if already set
    let _ = GLOBAL_GRAPH_SCHEMA.set(RwLock::new(schema));
}

#[cfg(test)]
mod multiple_relationship_tests {
    use super::*;

    #[test]
    fn test_multiple_relationship_types_union() {
        // Setup test schema
        setup_test_schema();
        
        // Test that [:FOLLOWS|FRIENDS_WITH] generates UNION CTE
        let cypher = "MATCH (u1:User)-[:FOLLOWS|FRIENDS_WITH]->(u2:User) RETURN u1, u2";
        let parse_result = open_cypher_parser::parse_query(cypher);
        assert!(parse_result.is_ok(), "Failed to parse multiple relationship types: {:?}", parse_result.err());

        let query = parse_result.unwrap();
        let (logical_plan, _plan_ctx) = build_logical_plan(&query)
            .expect("Failed to build logical plan");

        let render_plan = logical_plan.to_render_plan();
        assert!(render_plan.is_ok(), "Failed to create render plan: {:?}", render_plan.err());

        let render_plan = render_plan.unwrap();

        // Check that we have CTEs
        assert!(!render_plan.ctes.0.is_empty(), "Expected CTEs for multiple relationship types");

        // Find the relationship CTE
        let rel_cte = render_plan.ctes.0.iter().find(|cte| cte.cte_name.starts_with("rel_"));
        assert!(rel_cte.is_some(), "Expected relationship CTE with name starting with 'rel_'");

        let rel_cte = rel_cte.unwrap();

        // Check that the CTE content contains UNION
        match &rel_cte.content {
            crate::render_plan::CteContent::RawSql(sql) => {
                assert!(sql.contains("UNION ALL"), "Expected UNION ALL in CTE SQL: {}", sql);
                assert!(sql.contains("user_follows"), "Expected user_follows table in UNION: {}", sql);
                assert!(sql.contains("friendships"), "Expected friendships table in UNION: {}", sql);
            }
            _ => panic!("Expected RawSql content for relationship CTE"),
        }
    }

    #[test]
    fn test_three_relationship_types_union() {
        // Setup test schema
        setup_test_schema();
        
        // Test that [:PURCHASED|PLACED_ORDER|ORDER_CONTAINS] generates UNION CTE with 3 tables
        let cypher = "MATCH (c:Customer)-[:PURCHASED|PLACED_ORDER|ORDER_CONTAINS]->(target) RETURN c, target";
        let parse_result = open_cypher_parser::parse_query(cypher);
        assert!(parse_result.is_ok(), "Failed to parse three relationship types: {:?}", parse_result.err());

        let query = parse_result.unwrap();
        let (logical_plan, _plan_ctx) = build_logical_plan(&query)
            .expect("Failed to build logical plan");

        let render_plan = logical_plan.to_render_plan();
        assert!(render_plan.is_ok(), "Failed to create render plan: {:?}", render_plan.err());

        let render_plan = render_plan.unwrap();

        // Check that we have CTEs
        assert!(!render_plan.ctes.0.is_empty(), "Expected CTEs for three relationship types");

        // Find the relationship CTE
        let rel_cte = render_plan.ctes.0.iter().find(|cte| cte.cte_name.starts_with("rel_"));
        assert!(rel_cte.is_some(), "Expected relationship CTE with name starting with 'rel_'");

        let rel_cte = rel_cte.unwrap();

        // Check that the CTE content contains UNION and all three tables
        match &rel_cte.content {
            crate::render_plan::CteContent::RawSql(sql) => {
                println!("Generated SQL for 3 relationship types:\n{}", sql);
                assert!(sql.contains("UNION ALL"), "Expected UNION ALL in CTE SQL: {}", sql);

                // Count UNION ALL occurrences - should be 2 for 3 tables (table1 UNION ALL table2 UNION ALL table3)
                let union_count = sql.matches("UNION ALL").count();
                assert_eq!(union_count, 2, "Expected 2 UNION ALL clauses for 3 tables, got {}: {}", union_count, sql);

                // Check all three tables are present
                assert!(sql.contains("orders"), "Expected orders table in UNION: {}", sql);
                assert!(sql.contains("orders"), "Expected orders table for PLACED_ORDER in UNION: {}", sql);
                assert!(sql.contains("order_items"), "Expected order_items table for ORDER_CONTAINS in UNION: {}", sql);
                let select_count = sql.matches("SELECT").count();
                assert_eq!(select_count, 3, "Expected 3 SELECT statements for 3 relationship types, got {}: {}", select_count, sql);
            }
            _ => panic!("Expected RawSql content for relationship CTE"),
        }
    }

    #[test]
    fn test_single_relationship_type_no_union() {
        // Setup test schema
        setup_test_schema();
        
        // Test that single relationship type doesn't generate UNION
        let cypher = "MATCH (u1:User)-[:FOLLOWS]->(u2:User) RETURN u1, u2";
        let parse_result = open_cypher_parser::parse_query(cypher);
        assert!(parse_result.is_ok(), "Failed to parse single relationship type: {:?}", parse_result.err());

        let query = parse_result.unwrap();
        let (logical_plan, _plan_ctx) = build_logical_plan(&query)
            .expect("Failed to build logical plan");

        let render_plan = logical_plan.to_render_plan();
        assert!(render_plan.is_ok(), "Failed to create render plan: {:?}", render_plan.err());

        let render_plan = render_plan.unwrap();

        // For single relationship types, we shouldn't have UNION CTEs
        // (though we might have other CTEs for variable-length paths)
        let union_ctes = render_plan.ctes.0.iter().filter(|cte| {
            matches!(&cte.content, crate::render_plan::CteContent::RawSql(sql) if sql.contains("UNION ALL"))
        });
        assert_eq!(union_ctes.count(), 0, "Expected no UNION CTEs for single relationship type");
    }
}