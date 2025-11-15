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
        assert!(result.is_ok(), "Failed to parse range pattern: {:?}", result.err());
    }

    #[test]
    fn test_parse_fixed_length_pattern() {
        // MATCH (u1:User)-[:FOLLOWS*3]->(u2:User) RETURN u2
        let cypher = "MATCH (u1:User)-[:FOLLOWS*3]->(u2:User) RETURN u2";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(result.is_ok(), "Failed to parse fixed length pattern: {:?}", result.err());
    }

    #[test]
    fn test_parse_unbounded_pattern() {
        // MATCH (u1:User)-[:FOLLOWS*]->(u2:User) RETURN u2
        let cypher = "MATCH (u1:User)-[:FOLLOWS*]->(u2:User) RETURN u2";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(result.is_ok(), "Failed to parse unbounded pattern: {:?}", result.err());
    }

    #[test]
    fn test_parse_max_only_pattern() {
        // MATCH (u1:User)-[:FOLLOWS*..5]->(u2:User) RETURN u2
        let cypher = "MATCH (u1:User)-[:FOLLOWS*..5]->(u2:User) RETURN u2";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(result.is_ok(), "Failed to parse max-only pattern: {:?}", result.err());
    }

    #[test]
    fn test_parse_with_properties() {
        // MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User) RETURN u1.name, u2.name
        // Note: Property filtering in MATCH clause ({name: 'Alice'}) is not yet supported
        // but properties in RETURN are supported
        let cypher = "MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User) RETURN u1.full_name, u2.full_name";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(result.is_ok(), "Failed to parse with properties: {:?}", result.err());
    }

    #[test]
    fn test_parse_with_where_clause() {
        // MATCH (u1:User)-[:FOLLOWS*1..3]->(u2:User) WHERE u2.age > 25 RETURN u2.name
        let cypher = "MATCH (u1:User)-[:FOLLOWS*1..3]->(u2:User) WHERE u2.age > 25 RETURN u2.full_name";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(result.is_ok(), "Failed to parse with WHERE clause: {:?}", result.err());
    }

    #[test]
    fn test_parse_with_aggregation() {
        // MATCH (u1:User)-[:FOLLOWS*1..3]->(u2:User) RETURN COUNT(u2)
        let cypher = "MATCH (u1:User)-[:FOLLOWS*1..3]->(u2:User) RETURN COUNT(u2)";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(result.is_ok(), "Failed to parse with aggregation: {:?}", result.err());
    }

    #[test]
    fn test_parse_with_order_by() {
        // MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User) RETURN u2.name ORDER BY u2.age DESC
        let cypher = "MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User) RETURN u2.full_name ORDER BY u2.age DESC";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(result.is_ok(), "Failed to parse with ORDER BY: {:?}", result.err());
    }

    #[test]
    fn test_parse_with_limit() {
        // MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User) RETURN u2 LIMIT 10
        let cypher = "MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User) RETURN u2 LIMIT 10";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(result.is_ok(), "Failed to parse with LIMIT: {:?}", result.err());
    }

    #[test]
    fn test_parse_bidirectional() {
        // MATCH (u1:User)-[:FOLLOWS*1..2]-(u2:User) RETURN u2
        let cypher = "MATCH (u1:User)-[:FOLLOWS*1..2]-(u2:User) RETURN u2";
        let result = open_cypher_parser::parse_query(cypher);
        // Bidirectional might not be fully supported yet, but should at least parse
        assert!(result.is_ok(), "Failed to parse bidirectional pattern: {:?}", result.err());
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
        // *0 should fail
        let cypher = "MATCH (u1:User)-[:FOLLOWS*0]->(u2:User) RETURN u2";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(result.is_err(), "Should reject zero hops *0");
    }

    #[test]
    fn test_reject_zero_min() {
        // *0..5 should fail
        let cypher = "MATCH (u1:User)-[:FOLLOWS*0..5]->(u2:User) RETURN u2";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(result.is_err(), "Should reject zero min *0..5");
    }

    #[test]
    fn test_accept_single_hop() {
        // *1 should work
        let cypher = "MATCH (u1:User)-[:FOLLOWS*1]->(u2:User) RETURN u2";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(result.is_ok(), "Should accept single hop *1: {:?}", result.err());
    }

    #[test]
    fn test_accept_large_range() {
        // *1..100 should work (though may be slow in practice)
        let cypher = "MATCH (u1:User)-[:FOLLOWS*1..100]->(u2:User) RETURN u2";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(result.is_ok(), "Should accept large range *1..100: {:?}", result.err());
    }
}

#[cfg(test)]
mod complex_query_tests {
    use super::*;

    #[test]
    fn test_multiple_return_items() {
        let cypher = "MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User) RETURN u1.full_name, u2.full_name, u2.age";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(result.is_ok(), "Failed to parse multiple return items: {:?}", result.err());
    }

    #[test]
    fn test_with_group_by() {
        let cypher = "MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User) RETURN u1.full_name, COUNT(u2) GROUP BY u1.full_name";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(result.is_ok(), "Failed to parse with GROUP BY: {:?}", result.err());
    }

    #[test]
    fn test_with_sum_aggregation() {
        let cypher = "MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User) RETURN SUM(u2.age)";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(result.is_ok(), "Failed to parse with SUM: {:?}", result.err());
    }

    #[test]
    fn test_with_multiple_where_conditions() {
        let cypher = "MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User) WHERE u1.age > 20 AND u2.age < 40 RETURN u2.full_name";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(result.is_ok(), "Failed to parse with multiple WHERE conditions: {:?}", result.err());
    }

    #[test]
    fn test_order_by_with_limit() {
        let cypher = "MATCH (u1:User)-[:FOLLOWS*1..2]->(u2:User) RETURN u2.full_name ORDER BY u2.age LIMIT 10";
        let result = open_cypher_parser::parse_query(cypher);
        assert!(result.is_ok(), "Failed to parse ORDER BY with LIMIT: {:?}", result.err());
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
        assert!(duration.as_secs() < 1, "Complex parsing too slow: {:?}", duration);
    }
}
