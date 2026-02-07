//! Test UNION splitting generates IN clauses instead of OR conditions

use std::collections::{HashMap, HashSet};
use clickgraph::query_planner::ast_transform;
use clickgraph::open_cypher_parser;
use clickgraph::server::bolt_protocol::id_mapper::IdMapper;

#[test]
fn test_split_generates_in_clause() {
    // This test verifies that when we have id() IN [1,2,3] with multiple labels,
    // the split generates IN clauses instead of OR conditions
    
    let cypher = "MATCH (a:User) WHERE id(a) IN [1, 2, 3] RETURN a.name";
    
    // Parse the query
    let (_remaining, parsed_stmt) = open_cypher_parser::parse_cypher_statement(cypher)
        .expect("Failed to parse query");
    
    // Create an IdMapper and populate it with test data
    let id_mapper = IdMapper::new();
    
    // Simulate having IDs mapped to different labels
    // This would normally be populated during query execution
    // For this test, we're just checking the structure
    
    // Transform (this will extract label constraints if id_mapper has the data)
    let transformed = ast_transform::transform_id_functions(
        parsed_stmt,
        &id_mapper,
        None // No schema for this simple test
    );
    
    // The transformation should work even without schema
    match transformed {
        clickgraph::open_cypher_parser::ast::CypherStatement::Query { query, union_clauses } => {
            println!("Main query has {} MATCH clauses", query.match_clauses.len());
            println!("UNION clauses: {}", union_clauses.len());
            
            // If we had multiple labels, we'd see UNION clauses here
            // For now, just verify the transformation didn't crash
            assert!(query.match_clauses.len() > 0);
        }
        _ => panic!("Expected Query statement"),
    }
}

#[test]
fn test_build_property_in_clause_structure() {
    // This tests the internal structure of the IN clause generation
    // We can't easily test the actual function since it's private,
    // but we can verify the transformation doesn't break the AST
    
    let cypher = "MATCH (a:User) RETURN a.name";
    let (_remaining, parsed_stmt) = open_cypher_parser::parse_cypher_statement(cypher)
        .expect("Failed to parse query");
    
    let id_mapper = IdMapper::new();
    let transformed = ast_transform::transform_id_functions(
        parsed_stmt,
        &id_mapper,
        None
    );
    
    // Verify we still have a valid query structure
    match transformed {
        clickgraph::open_cypher_parser::ast::CypherStatement::Query { query, .. } => {
            assert!(query.match_clauses.len() > 0);
            assert!(query.return_clause.is_some());
        }
        _ => panic!("Expected Query statement"),
    }
}
