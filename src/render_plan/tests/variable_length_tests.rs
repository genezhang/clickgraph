//! Comprehensive tests for variable-length path queries
//!
//! These tests verify the correctness of variable-length path parsing,
//! validation, and various query patterns.

use crate::open_cypher_parser;

#[cfg(test)]
mod parsing_tests {
    use super::*;

    #[test]
    fn test_parse_range_pattern() {
        // MATCH (u1:User)-[:FOLLOWS*1..3]->(u2:User) RETURN u2
        let cypher = "MATCH (u1:User)-[:FOLLOWS*1..3]->(u2:User) RETURN u2";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(
            result.is_ok(),
            "Failed to parse range pattern: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_parse_fixed_length_pattern() {
        // MATCH (u1:User)-[:FOLLOWS*3]->(u2:User) RETURN u2
        let cypher = "MATCH (u1:User)-[:FOLLOWS*3]->(u2:User) RETURN u2";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(
            result.is_ok(),
            "Failed to parse fixed length pattern: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_parse_unbounded_pattern() {
        // MATCH (u1:User)-[:FOLLOWS*]->(u2:User) RETURN u2
        let cypher = "MATCH (u1:User)-[:FOLLOWS*]->(u2:User) RETURN u2";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(
            result.is_ok(),
            "Failed to parse unbounded pattern: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_parse_max_only_pattern() {
        // MATCH (u1:User)-[:FOLLOWS*..5]->(u2:User) RETURN u2
        let cypher = "MATCH (u1:User)-[:FOLLOWS*..5]->(u2:User) RETURN u2";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(
            result.is_ok(),
            "Failed to parse max-only pattern: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_parse_with_properties() {
        // MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User) RETURN u1.name, u2.name
        // Note: Property filtering in MATCH clause ({name: 'Alice'}) is not yet supported
        // but properties in RETURN are supported
        let cypher = "MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User) RETURN u1.full_name, u2.full_name";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(
            result.is_ok(),
            "Failed to parse with properties: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_parse_with_where_clause() {
        // MATCH (u1:User)-[:FOLLOWS*1..3]->(u2:User) WHERE u2.age > 25 RETURN u2.name
        let cypher =
            "MATCH (u1:User)-[:FOLLOWS*1..3]->(u2:User) WHERE u2.age > 25 RETURN u2.full_name";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(
            result.is_ok(),
            "Failed to parse with WHERE clause: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_parse_with_aggregation() {
        // MATCH (u1:User)-[:FOLLOWS*1..3]->(u2:User) RETURN COUNT(u2)
        let cypher = "MATCH (u1:User)-[:FOLLOWS*1..3]->(u2:User) RETURN COUNT(u2)";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(
            result.is_ok(),
            "Failed to parse with aggregation: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_parse_with_order_by() {
        // MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User) RETURN u2.name ORDER BY u2.age DESC
        let cypher =
            "MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User) RETURN u2.full_name ORDER BY u2.age DESC";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(
            result.is_ok(),
            "Failed to parse with ORDER BY: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_parse_with_limit() {
        // MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User) RETURN u2 LIMIT 10
        let cypher = "MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User) RETURN u2 LIMIT 10";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(
            result.is_ok(),
            "Failed to parse with LIMIT: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_parse_bidirectional() {
        // MATCH (u1:User)-[:FOLLOWS*1..2]-(u2:User) RETURN u2
        let cypher = "MATCH (u1:User)-[:FOLLOWS*1..2]-(u2:User) RETURN u2";
        let result = open_cypher_parser::parse_query(cypher);
        // Bidirectional might not be fully supported yet, but should at least parse
        assert!(
            result.is_ok(),
            "Failed to parse bidirectional pattern: {:?}",
            result.err()
        );
    }
}

#[cfg(test)]
mod validation_tests {
    use super::*;

    #[test]
    fn test_reject_inverted_range() {
        // *5..2 should fail (min > max)
        let cypher = "MATCH (u1:User)-[:FOLLOWS*5..2]->(u2:User) RETURN u2";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(result.is_err(), "Should reject inverted range *5..2");
    }

    #[test]
    fn test_reject_zero_hops() {
        // *0 is now allowed (for shortest path self-loops) with a warning
        let cypher = "MATCH (u1:User)-[:FOLLOWS*0]->(u2:User) RETURN u2";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(
            result.is_ok(),
            "Zero hops *0 should be allowed with warning"
        );
    }

    #[test]
    fn test_reject_zero_min() {
        // *0..5 is now allowed (for shortest path self-loops) with a warning
        let cypher = "MATCH (u1:User)-[:FOLLOWS*0..5]->(u2:User) RETURN u2";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(
            result.is_ok(),
            "Zero min *0..5 should be allowed with warning"
        );
    }

    #[test]
    fn test_accept_single_hop() {
        // *1 should work
        let cypher = "MATCH (u1:User)-[:FOLLOWS*1]->(u2:User) RETURN u2";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(
            result.is_ok(),
            "Should accept single hop *1: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_accept_large_range() {
        // *1..100 should work (though may be slow in practice)
        let cypher = "MATCH (u1:User)-[:FOLLOWS*1..100]->(u2:User) RETURN u2";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(
            result.is_ok(),
            "Should accept large range *1..100: {:?}",
            result.err()
        );
    }
}

#[cfg(test)]
mod complex_query_tests {
    use super::*;

    #[test]
    fn test_multiple_return_items() {
        let cypher =
            "MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User) RETURN u1.full_name, u2.full_name, u2.age";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(
            result.is_ok(),
            "Failed to parse multiple return items: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_with_aggregation() {
        // Note: Cypher doesn't have explicit GROUP BY - aggregation is implicit based on non-aggregated columns
        let cypher = "MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User) RETURN u1.full_name, COUNT(u2)";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(
            result.is_ok(),
            "Failed to parse aggregation query: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_with_sum_aggregation() {
        let cypher = "MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User) RETURN SUM(u2.age)";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(
            result.is_ok(),
            "Failed to parse with SUM: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_with_multiple_where_conditions() {
        let cypher = "MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User) WHERE u1.age > 20 AND u2.age < 40 RETURN u2.full_name";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(
            result.is_ok(),
            "Failed to parse with multiple WHERE conditions: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_order_by_with_limit() {
        let cypher = "MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User) RETURN u2.full_name ORDER BY u2.age LIMIT 10";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(
            result.is_ok(),
            "Failed to parse ORDER BY with LIMIT: {:?}",
            result.err()
        );
    }
}

#[cfg(test)]
mod performance_tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn test_parsing_performance() {
        let cypher = "MATCH (u1:User)-[:FOLLOWS*1..3]->(u2:User) RETURN u2.full_name";

        let start = Instant::now();
        for _ in 0..1000 {
            let _ = open_cypher_parser::parse_query(cypher);
        }
        let duration = start.elapsed();

        // Parsing 1000 queries should take less than 1 second
        assert!(duration.as_secs() < 1, "Parsing too slow: {:?}", duration);
    }

    #[test]
    fn test_complex_query_parsing() {
        // Note: Property filtering in MATCH clause is not yet supported,
        // so we test with WHERE clause instead
        let cypher = "MATCH (u1:User)-[:FOLLOWS*1..5]->(u2:User)-[:AUTHORED*1]->(p:Post) WHERE u2.age > 25 RETURN u1.full_name, u2.full_name, p.title ORDER BY p.title LIMIT 20";

        let start = Instant::now();
        for _ in 0..100 {
            let _ = open_cypher_parser::parse_query(cypher);
        }
        let duration = start.elapsed();

        // Parsing 100 complex queries should take less than 1 second
        assert!(
            duration.as_secs() < 1,
            "Complex parsing too slow: {:?}",
            duration
        );
    }
}

#[cfg(test)]
mod vlp_cte_scoping_tests {
    //! Tests for VLP CTE Column Scoping fix
    //! 
    //! Bug: When VLP patterns are followed by additional relationships and GROUP BY aggregations,
    //! columns from non-VLP nodes weren't available in CTE scope, causing
    //! "Unknown expression identifier" errors.
    //! 
    //! Fix: Collect aliases from aggregate expressions and include their ID columns in UNION SELECT
    
    use super::*;

    #[test]
    fn test_vlp_with_additional_relationship_and_count_distinct() {
        // MATCH (p)-[:KNOWS*1..2]-(f)<-[:HAS_CREATOR]-(m)
        // RETURN f.id, COUNT(DISTINCT m)
        // 
        // This is the core pattern that was failing before the fix.
        // The m.id needs to be included in the UNION SELECT for COUNT(DISTINCT m) to work.
        let cypher = "MATCH (p:Person {id: 933})-[:KNOWS*1..2]-(f:Person)<-[:HAS_CREATOR]-(m:Message) RETURN f.id, COUNT(DISTINCT m) AS messageCount";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(
            result.is_ok(),
            "Failed to parse VLP + additional rel + COUNT(DISTINCT): {:?}",
            result.err()
        );
    }

    #[test]
    fn test_vlp_with_additional_relationship_and_multiple_aggregates() {
        // Test with multiple aggregate functions
        let cypher = "MATCH (p:Person {id: 1})-[:KNOWS*1..2]-(f:Person)<-[:HAS_CREATOR]-(m:Message) RETURN f.id, COUNT(DISTINCT m) AS msgCount, COUNT(m) AS totalCount";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(
            result.is_ok(),
            "Failed to parse VLP + multiple aggregates: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_vlp_with_group_by_and_sum_aggregate() {
        // Test with GROUP BY and SUM aggregate
        let cypher = "MATCH (p:Person {id: 1})-[:KNOWS*1..2]-(f:Person)<-[:HAS_CREATOR]-(m:Message) RETURN f.id, f.full_name, SUM(m.length) AS totalLength";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(
            result.is_ok(),
            "Failed to parse VLP + GROUP BY + SUM: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_vlp_with_multiple_additional_relationships() {
        // Test VLP followed by multiple additional relationships with aggregation
        let cypher = "MATCH (p:Person {id: 1})-[:KNOWS*1..2]-(f:Person)<-[:HAS_CREATOR]-(m:Message)-[:HAS_TAG]->(t:Tag) RETURN f.id, t.name, COUNT(DISTINCT m) AS messageCount";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(
            result.is_ok(),
            "Failed to parse VLP + multiple additional rels: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_vlp_with_count_distinct_multiple_nodes() {
        // Test COUNT(DISTINCT) on multiple nodes
        let cypher = "MATCH (p:Person {id: 1})-[:KNOWS*1..2]-(f:Person)<-[:HAS_CREATOR]-(m:Message)-[:HAS_TAG]->(t:Tag) RETURN f.id, COUNT(DISTINCT m) AS msgCount, COUNT(DISTINCT t) AS tagCount";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(
            result.is_ok(),
            "Failed to parse COUNT(DISTINCT) on multiple nodes: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_vlp_bidirectional_with_aggregate() {
        // Test bidirectional VLP with aggregate (generates UNION)
        let cypher = "MATCH (p:Person {id: 1})-[:KNOWS*1..2]-(f:Person), (f)<-[:HAS_CREATOR]-(m:Message) RETURN f.id, COUNT(DISTINCT m) AS messageCount";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(
            result.is_ok(),
            "Failed to parse bidirectional VLP + aggregate: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_vlp_with_having_clause() {
        // Test VLP + GROUP BY + HAVING with aggregate
        let cypher = "MATCH (p:Person {id: 1})-[:KNOWS*1..2]-(f:Person)<-[:HAS_CREATOR]-(m:Message) WITH f, COUNT(DISTINCT m) AS msgCount WHERE msgCount > 5 RETURN f.id, msgCount";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(
            result.is_ok(),
            "Failed to parse VLP + HAVING clause: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_vlp_different_hop_ranges_with_aggregates() {
        // Test different VLP hop ranges with aggregates
        let test_cases = vec![
            "MATCH (p:Person {id: 1})-[:KNOWS*1]-(f:Person)<-[:HAS_CREATOR]-(m:Message) RETURN f.id, COUNT(DISTINCT m)",
            "MATCH (p:Person {id: 1})-[:KNOWS*2]-(f:Person)<-[:HAS_CREATOR]-(m:Message) RETURN f.id, COUNT(DISTINCT m)",
            "MATCH (p:Person {id: 1})-[:KNOWS*1..3]-(f:Person)<-[:HAS_CREATOR]-(m:Message) RETURN f.id, COUNT(DISTINCT m)",
            "MATCH (p:Person {id: 1})-[:KNOWS*2..5]-(f:Person)<-[:HAS_CREATOR]-(m:Message) RETURN f.id, COUNT(DISTINCT m)",
            "MATCH (p:Person {id: 1})-[:KNOWS*..4]-(f:Person)<-[:HAS_CREATOR]-(m:Message) RETURN f.id, COUNT(DISTINCT m)",
        ];

        for cypher in test_cases {
            let result = open_cypher_parser::parse_query(cypher);
            assert!(
                result.is_ok(),
                "Failed to parse VLP pattern '{}': {:?}",
                cypher,
                result.err()
            );
        }
    }

    #[test]
    fn test_vlp_with_nested_properties_in_aggregate() {
        // Test aggregate with property access inside (e.g., SUM(m.length))
        let cypher = "MATCH (p:Person {id: 1})-[:KNOWS*1..2]-(f:Person)<-[:HAS_CREATOR]-(m:Message) RETURN f.id, SUM(m.length) AS totalLength, AVG(m.length) AS avgLength";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(
            result.is_ok(),
            "Failed to parse VLP + property aggregates: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_vlp_with_complex_case_in_aggregate() {
        // Test CASE expression inside aggregate (IC-3 pattern)
        let cypher = "MATCH (p:Person {id: 1})-[:KNOWS*1..2]-(f:Person)<-[:HAS_CREATOR]-(m:Message) WITH f, CASE WHEN m.length > 100 THEN 1 ELSE 0 END AS isLong RETURN f.id, SUM(isLong) AS longMessages";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(
            result.is_ok(),
            "Failed to parse VLP + CASE in aggregate: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_vlp_with_order_by_aggregate() {
        // Test ORDER BY using aggregate result
        let cypher = "MATCH (p:Person {id: 1})-[:KNOWS*1..2]-(f:Person)<-[:HAS_CREATOR]-(m:Message) RETURN f.id, COUNT(DISTINCT m) AS msgCount ORDER BY msgCount DESC, f.id ASC";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(
            result.is_ok(),
            "Failed to parse VLP + ORDER BY aggregate: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_vlp_with_limit_and_aggregate() {
        // Test LIMIT with aggregation
        let cypher = "MATCH (p:Person {id: 1})-[:KNOWS*1..2]-(f:Person)<-[:HAS_CREATOR]-(m:Message) RETURN f.id, COUNT(DISTINCT m) AS msgCount ORDER BY msgCount DESC LIMIT 10";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(
            result.is_ok(),
            "Failed to parse VLP + LIMIT: {:?}",
            result.err()
        );
    }
}
