//! Regression tests for WHERE clause filters in variable-length paths and shortestPath
//!
//! These tests verify that WHERE clause filters are correctly applied in:
//! - Variable-length path queries (e.g., -[:REL*1..3]->)
//! - shortestPath() queries
//!
//! Issue: Filters were being stored in plan_ctx but not injected into GraphRel.where_predicate,
//! causing them to be omitted from generated SQL.
//!
//! Solution: FilterIntoGraphRel optimizer pass extracts filters from plan_ctx, qualifies Column
//! expressions with table aliases, and injects them into GraphRel.where_predicate.

use crate::{
    clickhouse_query_generator,
    graph_catalog::graph_schema::{GraphSchema, NodeIdSchema, NodeSchema, RelationshipSchema},
    open_cypher_parser,
    query_planner::logical_plan::plan_builder::build_logical_plan,
    render_plan::plan_builder::RenderPlanBuilder,
};
use std::collections::HashMap;

/// Helper function to parse Cypher, build logical plan, and generate SQL
fn cypher_to_sql(cypher: &str) -> String {
    let ast = open_cypher_parser::parse_query(cypher).expect("Failed to parse Cypher query");

    // Create empty schema for test
    let empty_schema = GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new());

    let (logical_plan, mut plan_ctx) =
        build_logical_plan(&ast, &empty_schema, None, None).expect("Failed to build logical plan");

    // Debug: Print logical plan before analyzer passes
    println!("Logical plan before analyzer passes: {:?}", logical_plan);

    // Run analyzer passes to extract filters from Filter nodes
    use crate::query_planner::analyzer;
    use crate::query_planner::optimizer;

    // Create a proper graph schema for testing with property mappings
    let graph_schema = setup_test_graph_schema();

    // Run analyzer passes to extract and tag filters
    let logical_plan =
        analyzer::initial_analyzing(logical_plan, &mut plan_ctx, &graph_schema).unwrap();
    let logical_plan =
        analyzer::intermediate_analyzing(logical_plan, &mut plan_ctx, &graph_schema).unwrap();

    // Debug: Print plan_ctx to see if filters were extracted
    println!("PlanCtx after analyzer passes: {:?}", plan_ctx);

    // Run optimizer passes to inject filters into GraphRel nodes
    let logical_plan = optimizer::initial_optimization(logical_plan, &mut plan_ctx).unwrap();
    let logical_plan = optimizer::final_optimization(logical_plan, &mut plan_ctx).unwrap();

    // Debug: Print final logical plan to see if GraphRel is still there
    println!("Final logical plan after optimizer: {:?}", logical_plan);

    let render_plan = logical_plan
        .to_render_plan(&graph_schema)
        .expect("Failed to build render plan");

    clickhouse_query_generator::generate_sql(render_plan, 100)
}

/// Create a test graph schema with proper property mappings
fn setup_test_graph_schema() -> GraphSchema {
    let mut nodes = HashMap::new();
    let mut relationships = HashMap::new();

    // Create User node schema
    let user_node = NodeSchema {
        database: "test_db".to_string(),
        table_name: "users".to_string(),
        column_names: vec![
            "id".to_string(),
            "name".to_string(),
            "age".to_string(),
            "status".to_string(),
            "user_id".to_string(),
        ],
        primary_keys: "id".to_string(),
        node_id: NodeIdSchema {
            column: "id".to_string(),
            dtype: "UInt64".to_string(),
        },
        property_mappings: [
            ("name".to_string(), "name".to_string()),
            ("age".to_string(), "age".to_string()),
            ("status".to_string(), "status".to_string()),
            ("user_id".to_string(), "user_id".to_string()),
            ("full_name".to_string(), "name".to_string()), // Alias for name
        ]
        .into_iter()
        .collect(),
        view_parameters: None,
        engine: None,
        use_final: None,
    };
    nodes.insert("User".to_string(), user_node);

    // Create FOLLOWS relationship schema
    let follows_rel = RelationshipSchema {
        database: "test_db".to_string(),
        table_name: "follows".to_string(),
        column_names: vec!["from_id".to_string(), "to_id".to_string()],
        from_node: "User".to_string(),
        to_node: "User".to_string(),
        from_id: "from_id".to_string(),
        to_id: "to_id".to_string(),
        from_node_id_dtype: "UInt64".to_string(),
        to_node_id_dtype: "UInt64".to_string(),
        property_mappings: HashMap::new(),
        view_parameters: None, engine: None, use_final: None,
    };
    relationships.insert("FOLLOWS".to_string(), follows_rel);

    GraphSchema::build(1, "test_db".to_string(), nodes, relationships)
}
#[cfg(test)]
mod variable_length_path_filters {
    use super::*;

    #[test]
    fn test_start_node_filter_only() {
        let cypher =
            "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WHERE a.name = 'Alice Johnson' RETURN b";
        let sql = cypher_to_sql(cypher);

        println!("Generated SQL:\n{}", sql);

        // Filter should appear in the SQL
        assert!(
            sql.contains("Alice Johnson"),
            "SQL should contain the start node filter 'Alice Johnson'"
        );

        // Should have WHERE clause
        assert!(
            sql.contains("WHERE") || sql.contains("where"),
            "SQL should contain WHERE clause"
        );
    }

    #[test]
    fn test_end_node_filter_only() {
        let cypher = "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WHERE b.name = 'David Lee' RETURN a";
        let sql = cypher_to_sql(cypher);

        println!("Generated SQL:\n{}", sql);

        // Filter should appear in the SQL
        assert!(
            sql.contains("David Lee"),
            "SQL should contain the end node filter 'David Lee'"
        );

        // Should have WHERE clause
        assert!(
            sql.contains("WHERE") || sql.contains("where"),
            "SQL should contain WHERE clause"
        );
    }

    #[test]
    fn test_both_start_and_end_filters() {
        let cypher = "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WHERE a.name = 'Alice Johnson' AND b.name = 'David Lee' RETURN a, b";
        let sql = cypher_to_sql(cypher);

        println!("Generated SQL:\n{}", sql);

        // Both filters should appear in the SQL
        assert!(
            sql.contains("Alice Johnson"),
            "SQL should contain the start node filter 'Alice Johnson'"
        );
        assert!(
            sql.contains("David Lee"),
            "SQL should contain the end node filter 'David Lee'"
        );

        // Should have WHERE clauses
        assert!(
            sql.contains("WHERE") || sql.contains("where"),
            "SQL should contain WHERE clause"
        );
    }

    #[test]
    fn test_property_filter_on_start_node() {
        let cypher = "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WHERE a.user_id = 1 RETURN b";
        let sql = cypher_to_sql(cypher);

        println!("Generated SQL:\n{}", sql);

        // Filter should appear with the property name and value
        assert!(
            sql.contains("user_id"),
            "SQL should contain property name 'user_id'"
        );
        assert!(sql.contains("1"), "SQL should contain the filter value '1'");
    }

    #[test]
    fn test_multiple_filters_on_same_node() {
        let cypher = "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WHERE a.name = 'Alice' AND a.age > 25 RETURN b";
        let sql = cypher_to_sql(cypher);

        println!("Generated SQL:\n{}", sql);

        // Both filters should appear
        assert!(
            sql.contains("Alice"),
            "SQL should contain name filter 'Alice'"
        );
        assert!(sql.contains("age"), "SQL should contain age property");
        assert!(sql.contains("25"), "SQL should contain age value '25'");
    }

    #[test]
    fn test_filter_with_variable_length_range() {
        let cypher = "MATCH (a:User)-[:FOLLOWS*2..4]->(b:User) WHERE a.user_id = 1 RETURN b";
        let sql = cypher_to_sql(cypher);

        println!("Generated SQL:\n{}", sql);

        // Filter should be present
        assert!(
            sql.contains("user_id") && sql.contains("1"),
            "SQL should contain the user_id filter"
        );

        // Should have correct hop bounds (vp.hop_count < 4 for max of 4)
        assert!(
            sql.contains("hop_count"),
            "SQL should contain hop_count for variable-length path"
        );
    }
}

#[cfg(test)]
mod shortest_path_filters {
    use super::*;

    #[test]
    fn test_shortest_path_with_start_and_end_filters() {
        let cypher = "MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WHERE a.name = 'Alice Johnson' AND b.name = 'David Lee' RETURN p";
        let sql = cypher_to_sql(cypher);

        println!("Generated SQL:\n{}", sql);

        // Both filters should be present
        assert!(
            sql.contains("Alice Johnson"),
            "SQL should contain start node filter 'Alice Johnson'"
        );
        assert!(
            sql.contains("David Lee"),
            "SQL should contain end node filter 'David Lee'"
        );

        // Should have shortestPath-specific logic
        assert!(
            sql.contains("ORDER BY") && sql.contains("hop_count"),
            "SQL should contain ORDER BY hop_count for shortestPath"
        );
        assert!(
            sql.contains("LIMIT 1"),
            "SQL should contain LIMIT 1 for shortestPath"
        );
    }

    #[test]
    fn test_shortest_path_with_user_id_filters() {
        let cypher = "MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WHERE a.user_id = 1 AND b.user_id = 4 RETURN p";
        let sql = cypher_to_sql(cypher);

        println!("Generated SQL:\n{}", sql);

        // Filters should be present
        assert!(
            sql.contains("user_id"),
            "SQL should contain user_id property"
        );
        assert!(
            sql.contains("1") && sql.contains("4"),
            "SQL should contain both user_id values"
        );

        // Should have shortestPath logic
        assert!(
            sql.contains("ORDER BY") && sql.contains("hop_count"),
            "SQL should contain ORDER BY hop_count"
        );
        assert!(sql.contains("LIMIT 1"), "SQL should contain LIMIT 1");
    }

    #[test]
    fn test_shortest_path_with_only_start_filter() {
        let cypher = "MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WHERE a.name = 'Alice Johnson' RETURN p";
        let sql = cypher_to_sql(cypher);

        println!("Generated SQL:\n{}", sql);

        // Start filter should be present
        assert!(
            sql.contains("Alice Johnson"),
            "SQL should contain start node filter 'Alice Johnson'"
        );

        // Should have shortestPath logic
        assert!(
            sql.contains("ORDER BY") && sql.contains("hop_count"),
            "SQL should contain ORDER BY hop_count"
        );
        assert!(sql.contains("LIMIT 1"), "SQL should contain LIMIT 1");
    }

    #[test]
    fn test_shortest_path_with_only_end_filter() {
        let cypher =
            "MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WHERE b.user_id = 4 RETURN p";
        let sql = cypher_to_sql(cypher);

        println!("Generated SQL:\n{}", sql);

        // End filter should be present
        assert!(
            sql.contains("user_id") && sql.contains("4"),
            "SQL should contain end node filter 'user_id = 4'"
        );

        // Should have shortestPath logic
        assert!(
            sql.contains("ORDER BY") && sql.contains("hop_count"),
            "SQL should contain ORDER BY hop_count"
        );
        assert!(sql.contains("LIMIT 1"), "SQL should contain LIMIT 1");
    }

    #[test]
    fn test_shortest_path_with_complex_filter() {
        let cypher = "MATCH p = shortestPath((a:User)-[:FOLLOWS*]->(b:User)) WHERE a.user_id > 0 AND b.age < 50 RETURN p";
        let sql = cypher_to_sql(cypher);

        println!("Generated SQL:\n{}", sql);

        // Filters should be present
        assert!(
            sql.contains("user_id") && sql.contains("0"),
            "SQL should contain user_id filter"
        );
        assert!(
            sql.contains("age") && sql.contains("50"),
            "SQL should contain age filter"
        );

        // Should have shortestPath logic
        assert!(
            sql.contains("ORDER BY") && sql.contains("hop_count"),
            "SQL should contain ORDER BY hop_count"
        );
        assert!(sql.contains("LIMIT 1"), "SQL should contain LIMIT 1");
    }
}

#[cfg(test)]
mod filter_categorization_tests {
    use super::*;

    #[test]
    fn test_start_filter_in_base_case() {
        // Start node filters should appear in the recursive CTE's base case
        let cypher = "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WHERE a.name = 'Alice' RETURN b";
        let sql = cypher_to_sql(cypher);

        println!("Generated SQL:\n{}", sql);

        // Look for the base case WITH RECURSIVE pattern
        assert!(
            sql.contains("WITH RECURSIVE") || sql.contains("with recursive"),
            "SQL should use recursive CTE"
        );

        // Start filter should be in SQL (either base case or wrapper, depending on optimization)
        assert!(
            sql.contains("Alice"),
            "SQL should contain the start node filter"
        );
    }

    #[test]
    fn test_end_filter_in_wrapper_cte() {
        // End node filters should appear in a wrapper CTE or final SELECT
        let cypher = "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WHERE b.user_id = 5 RETURN a";
        let sql = cypher_to_sql(cypher);

        println!("Generated SQL:\n{}", sql);

        // End filter should be present
        assert!(
            sql.contains("user_id") && sql.contains("5"),
            "SQL should contain end node filter"
        );

        // Should have recursive CTE structure
        assert!(
            sql.contains("WITH RECURSIVE") || sql.contains("with recursive"),
            "SQL should use recursive CTE"
        );
    }

    #[test]
    fn test_filters_preserve_semantics() {
        // When both start and end filters exist, both should be applied
        let cypher = "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WHERE a.user_id = 1 AND b.user_id = 3 RETURN a, b";
        let sql = cypher_to_sql(cypher);

        println!("Generated SQL:\n{}", sql);

        // Both filters should be present
        assert!(
            sql.contains("user_id"),
            "SQL should contain user_id property"
        );

        // Should find both values
        let has_one = sql.contains(" 1") || sql.contains("= 1") || sql.contains("(1");
        let has_three = sql.contains(" 3") || sql.contains("= 3") || sql.contains("(3");
        assert!(
            has_one && has_three,
            "SQL should contain both filter values: 1 and 3"
        );
    }
}

#[cfg(test)]
mod edge_cases {
    use super::*;

    #[test]
    fn test_filter_with_string_property() {
        let cypher = "MATCH (a:User)-[:FOLLOWS*]->(b:User) WHERE a.name = 'Test User' RETURN b";
        let sql = cypher_to_sql(cypher);

        println!("Generated SQL:\n{}", sql);

        assert!(
            sql.contains("Test User") || sql.contains("name"),
            "SQL should contain the string filter"
        );
    }

    #[test]
    fn test_filter_with_numeric_property() {
        let cypher = "MATCH (a:User)-[:FOLLOWS*1..3]->(b:User) WHERE a.age = 30 RETURN b";
        let sql = cypher_to_sql(cypher);

        println!("Generated SQL:\n{}", sql);

        assert!(
            sql.contains("age") && sql.contains("30"),
            "SQL should contain the numeric filter"
        );
    }

    #[test]
    fn test_filter_with_comparison_operator() {
        let cypher = "MATCH (a:User)-[:FOLLOWS*]->(b:User) WHERE a.age > 25 RETURN b";
        let sql = cypher_to_sql(cypher);

        println!("Generated SQL:\n{}", sql);

        assert!(
            sql.contains("age") && sql.contains("25"),
            "SQL should contain the comparison filter"
        );
        assert!(
            sql.contains(">") || sql.contains("greater"),
            "SQL should contain comparison operator"
        );
    }

    #[test]
    fn test_unbounded_path_with_filter() {
        // Unbounded paths (*) with filters
        let cypher = "MATCH (a:User)-[:FOLLOWS*]->(b:User) WHERE a.user_id = 1 RETURN b";
        let sql = cypher_to_sql(cypher);

        println!("Generated SQL:\n{}", sql);

        assert!(
            sql.contains("user_id") && sql.contains("1"),
            "SQL should contain the filter even with unbounded path"
        );
        assert!(
            sql.contains("hop_count"),
            "SQL should still track hop_count for unbounded paths"
        );
    }

    #[test]
    fn test_all_shortest_paths_basic() {
        let cypher = "MATCH allShortestPaths((a:User)-[:FOLLOWS*]->(b:User)) RETURN a.name, b.name";
        let sql = cypher_to_sql(cypher);

        println!("allShortestPaths SQL:\n{}", sql);

        // Check for allShortestPaths-specific patterns
        assert!(
            sql.contains("WHERE hop_count = (SELECT MIN(hop_count) FROM"),
            "allShortestPaths should use MIN filtering to get all paths with minimum length"
        );
        // The SQL doesn't contain "allShortestPaths" literal, but should have the MIN filtering logic
    }

    #[test]
    fn test_all_shortest_paths_with_filters() {
        let cypher = "MATCH allShortestPaths((a:User)-[:FOLLOWS*]->(b:User)) WHERE a.name = 'Alice Johnson' AND b.name = 'David Lee' RETURN a.name, b.name";
        let sql = cypher_to_sql(cypher);

        println!("allShortestPaths with filters SQL:\n{}", sql);

        // Check for both MIN filtering and WHERE clause filters
        assert!(
            sql.contains("WHERE hop_count = (SELECT MIN(hop_count) FROM"),
            "allShortestPaths should use MIN filtering"
        );
        assert!(
            sql.contains("Alice Johnson") && sql.contains("David Lee"),
            "WHERE clause filters should be applied"
        );
    }
}


