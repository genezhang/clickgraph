//! Test fixtures and shared test data
//!
//! This module provides shared test data and utilities for all test types.

/// Sample graph schema for testing
pub const TEST_GRAPH_SCHEMA: &str = r#"
nodes:
  - label: Person
    table: users
    id_column: user_id
    properties:
      name: full_name
      age: age
  - label: Company
    table: companies
    id_column: company_id
    properties:
      name: company_name

relationships:
  - type: WORKS_FOR
    table: employment
    from_id: user_id
    to_id: company_id
    properties:
      role: job_title
"#;

/// Sample Cypher queries for testing
pub const TEST_CYPHER_QUERIES: &[&str] = &[
    "MATCH (p:Person) RETURN p.name",
    "MATCH (p:Person)-[:WORKS_FOR]->(c:Company) RETURN p.name, c.name",
    "MATCH (p:Person) WHERE p.age > 25 RETURN p.name",
];

/// Helper function to create test data
pub fn create_test_data() -> Vec<(String, String)> {
    vec![
        ("user_id".to_string(), "1".to_string()),
        ("full_name".to_string(), "Alice Smith".to_string()),
        ("age".to_string(), "28".to_string()),
    ]
}