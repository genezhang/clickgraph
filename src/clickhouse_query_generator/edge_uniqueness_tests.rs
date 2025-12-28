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
        // Test that when edge_id is None, we use tuple(from_id, to_id)
        let schema = create_test_schema();
        let spec = VariableLengthSpec::range(1, 2);
        let generator = VariableLengthCteGenerator::new(
            &schema,  // Add schema as first parameter
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
            None, // No edge_id - should default to tuple(from_id, to_id)
        );

        let cte = generator.generate_cte();
        let sql = match &cte.content {
            crate::render_plan::CteContent::RawSql(s) => s,
            _ => panic!("Expected RawSql"),
        };

        // Check for path_edges (for edge uniqueness)
        assert!(
            sql.contains("path_edges"),
            "SQL should use path_edges for edge uniqueness. SQL:\n{}",
            sql
        );
        // path_nodes is now included for UNWIND nodes(p) support
        assert!(
            sql.contains("path_nodes"),
            "SQL should contain path_nodes for UNWIND nodes(p) support. SQL:\n{}",
            sql
        );

        // Check for tuple construction with from_id/to_id
        assert!(
            sql.contains("tuple(rel.follower_id, rel.followed_id)")
                || sql.contains("tuple(r.follower_id, r.followed_id)"),
            "SQL should use tuple(from_id, to_id) as default edge ID. SQL:\n{}",
            sql
        );

        // Check for edge uniqueness check
        assert!(
            sql.contains("NOT has(vp.path_edges,") || sql.contains("NOT has(path_edges,"),
            "SQL should check edge uniqueness in path_edges. SQL:\n{}",
            sql
        );

        println!("\n✅ Default Edge ID Test SQL:\n{}", sql);
    }

    #[test]
    fn test_composite_edge_id() {
        // Test composite edge ID (like OnTime schema)
        let schema = create_test_schema();
        let spec = VariableLengthSpec::range(1, 2);
        let edge_id = Some(Identifier::Composite(vec![
            "FlightDate".to_string(),
            "FlightNum".to_string(),
            "Origin".to_string(),
            "Dest".to_string(),
        ]));

        let generator = VariableLengthCteGenerator::new(
            &schema,  // Add schema parameter
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
            _ => panic!("Expected RawSql"),
        };

        // Check for path_edges
        assert!(
            sql.contains("path_edges"),
            "SQL should use path_edges. SQL:\n{}",
            sql
        );

        // Check for composite tuple construction
        assert!(
            sql.contains("tuple(rel.FlightDate, rel.FlightNum, rel.Origin, rel.Dest)")
                || sql.contains("tuple(r.FlightDate, r.FlightNum, r.Origin, r.Dest)"),
            "SQL should use composite tuple for edge ID. SQL:\n{}",
            sql
        );

        // Check for edge uniqueness check
        assert!(
            sql.contains("NOT has(vp.path_edges,"),
            "SQL should check edge uniqueness. SQL:\n{}",
            sql
        );

        println!("\n✅ Composite Edge ID Test SQL:\n{}", sql);
    }

    #[test]
    fn test_simple_edge_id() {
        // Test single column edge ID
        let schema = create_test_schema();
        let spec = VariableLengthSpec::range(1, 2);
        let edge_id = Some(Identifier::Single("transaction_id".to_string()));

        let generator = VariableLengthCteGenerator::new(
            &schema,  // Add schema parameter
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
            _ => panic!("Expected RawSql"),
        };

        // Check for path_edges
        assert!(
            sql.contains("path_edges"),
            "SQL should use path_edges. SQL:\n{}",
            sql
        );

        // Check for simple column reference (no tuple for single column)
        assert!(
            sql.contains("rel.transaction_id") || sql.contains("r.transaction_id"),
            "SQL should reference transaction_id column. SQL:\n{}",
            sql
        );

        // Check for edge uniqueness check
        assert!(
            sql.contains("NOT has(vp.path_edges,"),
            "SQL should check edge uniqueness. SQL:\n{}",
            sql
        );

        println!("\n✅ Simple Edge ID Test SQL:\n{}", sql);
    }
}
