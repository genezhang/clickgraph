#[cfg(test)]
mod edge_uniqueness_tests {
    use crate::clickhouse_query_generator::variable_length_cte::VariableLengthCteGenerator;
    use crate::graph_catalog::config::Identifier;
    use crate::graph_catalog::graph_schema::GraphSchema;
    use crate::query_planner::logical_plan::VariableLengthSpec;
    use std::collections::HashMap;

    /// Helper to create a minimal test schema for VLC tests
    fn create_test_schema() -> GraphSchema {
        GraphSchema::build(1, "test_db".to_string(), HashMap::new(), HashMap::new())
    }

    #[test]
    fn test_default_edge_id_tuple() {
        // Test that cycle detection uses path_nodes (node-uniqueness)
        let schema = create_test_schema();
        let spec = VariableLengthSpec::range(1, 2);
        let generator = VariableLengthCteGenerator::new(
            &schema,
            spec,
            "users",
            "user_id",
            "follows",
            "follower_id",
            "followed_id",
            "users",
            "user_id",
            "u1",
            "u2",
            vec![],
            None,
            None,
            None,
            None,
            None,
            None,
        );

        let cte = generator.generate_cte();
        let sql = match &cte.content {
            crate::render_plan::CteContent::RawSql(s) => s,
            other => panic!(
                "Expected RawSql content in test_default_tuple_edge_id, got: {:?}",
                other
            ),
        };

        // path_nodes is maintained for cycle detection and UNWIND nodes(p) support
        assert!(
            sql.contains("path_nodes"),
            "SQL should contain path_nodes for cycle detection. SQL:\n{}",
            sql
        );

        // Cycle detection uses path_nodes (node-uniqueness), not path_edges
        assert!(
            sql.contains("NOT has(vp.path_nodes,"),
            "SQL should check node uniqueness via path_nodes. SQL:\n{}",
            sql
        );

        // path_edges should NOT be present (removed for memory optimization)
        assert!(
            !sql.contains("path_edges"),
            "SQL should NOT contain path_edges (removed for memory optimization). SQL:\n{}",
            sql
        );

        println!("\n✅ Default Edge ID Test SQL:\n{}", sql);
    }

    #[test]
    fn test_composite_edge_id() {
        // Test composite edge ID — cycle detection still uses path_nodes
        let schema = create_test_schema();
        let spec = VariableLengthSpec::range(1, 2);
        let edge_id = Some(Identifier::Composite(vec![
            "FlightDate".to_string(),
            "FlightNum".to_string(),
            "Origin".to_string(),
            "Dest".to_string(),
        ]));

        let generator = VariableLengthCteGenerator::new(
            &schema,
            spec,
            "airports",
            "airport_code",
            "flights",
            "Origin",
            "Dest",
            "airports",
            "airport_code",
            "a1",
            "a2",
            vec![],
            None,
            None,
            None,
            None,
            None,
            edge_id,
        );

        let cte = generator.generate_cte();
        let sql = match &cte.content {
            crate::render_plan::CteContent::RawSql(s) => s,
            other => panic!(
                "Expected RawSql content in test_custom_edge_id_tuple_construction, got: {:?}",
                other
            ),
        };

        // Cycle detection uses path_nodes (node-uniqueness)
        assert!(
            sql.contains("NOT has(vp.path_nodes,"),
            "SQL should check node uniqueness via path_nodes. SQL:\n{}",
            sql
        );

        // path_edges should NOT be present
        assert!(
            !sql.contains("path_edges"),
            "SQL should NOT contain path_edges (removed for memory optimization). SQL:\n{}",
            sql
        );

        println!("\n✅ Composite Edge ID Test SQL:\n{}", sql);
    }

    #[test]
    fn test_simple_edge_id() {
        // Test single column edge ID — cycle detection still uses path_nodes
        let schema = create_test_schema();
        let spec = VariableLengthSpec::range(1, 2);
        let edge_id = Some(Identifier::Single("transaction_id".to_string()));

        let generator = VariableLengthCteGenerator::new(
            &schema,
            spec,
            "accounts",
            "account_id",
            "transactions",
            "from_account",
            "to_account",
            "accounts",
            "account_id",
            "a1",
            "a2",
            vec![],
            None,
            None,
            None,
            None,
            None,
            edge_id,
        );

        let cte = generator.generate_cte();
        let sql = match &cte.content {
            crate::render_plan::CteContent::RawSql(s) => s,
            other => panic!(
                "Expected RawSql content in test_custom_edge_id_column, got: {:?}",
                other
            ),
        };

        // Cycle detection uses path_nodes (node-uniqueness)
        assert!(
            sql.contains("NOT has(vp.path_nodes,"),
            "SQL should check node uniqueness via path_nodes. SQL:\n{}",
            sql
        );

        // path_edges should NOT be present
        assert!(
            !sql.contains("path_edges"),
            "SQL should NOT contain path_edges (removed for memory optimization). SQL:\n{}",
            sql
        );

        println!("\n✅ Simple Edge ID Test SQL:\n{}", sql);
    }
}
