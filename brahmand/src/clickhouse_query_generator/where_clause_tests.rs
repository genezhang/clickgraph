// Unit test for WHERE clause SQL generation in shortest path queries
// This verifies that our WHERE clause implementation generates correct SQL

use crate::clickhouse_query_generator::variable_length_cte::{VariableLengthCteGenerator, ShortestPathMode};
use crate::query_planner::logical_plan::VariableLengthSpec;

#[cfg(test)]
mod where_clause_tests {
    use super::*;

    #[test]
    fn test_start_node_filter_in_base_case() {
        let spec = VariableLengthSpec::unbounded();
        let start_filter = Some("start_node.full_name = 'Alice'".to_string());
        
        let generator = VariableLengthCteGenerator::new(
            spec,
            "users",
            "user_id",
            "follows",
            "follower_id",
            "followed_id",
            "users",
            "user_id",
            "a",
            "b",
            vec![],
            Some(ShortestPathMode::Shortest),
            start_filter,
            None,
        );
        
        let cte = generator.generate_cte();
        let sql = match &cte.content {
            crate::render_plan::CteContent::RawSql(s) => s,
            _ => panic!("Expected RawSql"),
        };
        
        // Verify start filter is in base case
        assert!(sql.contains("WHERE start_node.full_name = 'Alice'"), 
            "Start filter should be in base case WHERE clause");
        
        println!("\n✓ Generated SQL with start filter:\n{}\n", sql);
    }

    #[test]
    fn test_end_node_filter_in_outer_cte() {
        let spec = VariableLengthSpec::unbounded();
        let end_filter = Some("end_full_name = 'Bob'".to_string());
        
        let generator = VariableLengthCteGenerator::new(
            spec,
            "users",
            "user_id",
            "follows",
            "follower_id",
            "followed_id",
            "users",
            "user_id",
            "a",
            "b",
            vec![],
            Some(ShortestPathMode::Shortest),
            None,
            end_filter,
        );
        
        let cte = generator.generate_cte();
        let sql = match &cte.content {
            crate::render_plan::CteContent::RawSql(s) => s,
            _ => panic!("Expected RawSql"),
        };
        
        // Verify 3-tier structure
        assert!(sql.contains("_inner AS"), "Should have _inner CTE");
        assert!(sql.contains("_to_target AS"), "Should have _to_target CTE");
        assert!(sql.contains("WHERE end_full_name = 'Bob'"), 
            "End filter should be in _to_target CTE");
        
        println!("\n✓ Generated SQL with end filter (3-tier structure):\n{}\n", sql);
    }

    #[test]
    fn test_both_start_and_end_filters() {
        let spec = VariableLengthSpec::range(1, 5);
        let start_filter = Some("start_node.full_name = 'Alice'".to_string());
        let end_filter = Some("end_full_name = 'Bob'".to_string());
        
        let generator = VariableLengthCteGenerator::new(
            spec,
            "users",
            "user_id",
            "follows",
            "follower_id",
            "followed_id",
            "users",
            "user_id",
            "a",
            "b",
            vec![],
            Some(ShortestPathMode::Shortest),
            start_filter,
            end_filter,
        );
        
        let cte = generator.generate_cte();
        let sql = match &cte.content {
            crate::render_plan::CteContent::RawSql(s) => s,
            _ => panic!("Expected RawSql"),
        };
        
        // Verify both filters present
        assert!(sql.contains("WHERE start_node.full_name = 'Alice'"), 
            "Start filter should be in base case");
        assert!(sql.contains("WHERE end_full_name = 'Bob'"), 
            "End filter should be in outer CTE");
        assert!(sql.contains("_inner AS"), "Should have _inner CTE");
        assert!(sql.contains("_to_target AS"), "Should have _to_target CTE");
        
        println!("\n✓ Generated SQL with both filters:\n{}\n", sql);
    }

    #[test]
    fn test_no_filters_simple_structure() {
        let spec = VariableLengthSpec::fixed(2);
        
        let generator = VariableLengthCteGenerator::new(
            spec,
            "users",
            "user_id",
            "follows",
            "follower_id",
            "followed_id",
            "users",
            "user_id",
            "a",
            "b",
            vec![],
            None,  // No shortest path mode
            None,  // No start filter
            None,  // No end filter
        );
        
        let cte = generator.generate_cte();
        let sql = match &cte.content {
            crate::render_plan::CteContent::RawSql(s) => s,
            _ => panic!("Expected RawSql"),
        };
        
        // Verify simple structure (no _inner, _to_target)
        assert!(!sql.contains("_inner AS"), "Should NOT have _inner CTE without filters/shortest path");
        assert!(!sql.contains("_to_target AS"), "Should NOT have _to_target CTE");
        
        println!("\n✓ Generated SQL without filters (simple structure):\n{}\n", sql);
    }
}
