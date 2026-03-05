//! End-to-end integration tests for `clickgraph-embedded`.
//!
//! These tests exercise the full embedded API pipeline:
//!   YAML schema → Database → Connection → Cypher → SQL → QueryResult
//!
//! A `StubExecutor` replaces chdb so the tests run without native libraries.
//! The SQL correctness is already verified by unit tests; here we verify
//! that the API contract (schema loading, query routing, result parsing)
//! is satisfied end-to-end.

use std::sync::Arc;

use async_trait::async_trait;
use clickgraph::executor::{ExecutorError, QueryExecutor};
use clickgraph::graph_catalog::config::GraphSchemaConfig;
use clickgraph::graph_catalog::graph_schema::GraphSchema;
use clickgraph_embedded::{Connection, Database};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn build_schema(yaml: &str) -> Arc<GraphSchema> {
    let config: GraphSchemaConfig = serde_yaml::from_str(yaml).expect("valid yaml");
    Arc::new(config.to_graph_schema().expect("valid schema"))
}

/// A stub executor that returns pre-programmed JSON rows.
struct StubExecutor {
    rows: Vec<serde_json::Value>,
}

impl StubExecutor {
    fn returning(rows: Vec<serde_json::Value>) -> Self {
        Self { rows }
    }

    fn empty() -> Self {
        Self { rows: vec![] }
    }
}

#[async_trait]
impl QueryExecutor for StubExecutor {
    async fn execute_json(
        &self,
        _sql: &str,
        _role: Option<&str>,
    ) -> Result<Vec<serde_json::Value>, ExecutorError> {
        Ok(self.rows.clone())
    }

    async fn execute_text(
        &self,
        _sql: &str,
        _format: &str,
        _role: Option<&str>,
    ) -> Result<String, ExecutorError> {
        Ok(String::new())
    }
}

/// Build a `Database` backed by a `StubExecutor`.
fn stub_db(schema: Arc<GraphSchema>, rows: Vec<serde_json::Value>) -> Database {
    Database::from_executor(Arc::clone(&schema), Arc::new(StubExecutor::returning(rows)))
}

// ---------------------------------------------------------------------------
// Schema used across tests
// ---------------------------------------------------------------------------

const SOCIAL_YAML: &str = r#"
name: social_test
graph_schema:
  nodes:
    - label: User
      database: test_db
      table: users
      node_id: user_id
      property_mappings:
        user_id: user_id
        name: full_name
        age: age
  edges:
    - type: FOLLOWS
      database: test_db
      table: follows
      from_node: User
      to_node: User
      from_id: follower_id
      to_id: followed_id
      property_mappings: {}
"#;

// ---------------------------------------------------------------------------
// Database construction
// ---------------------------------------------------------------------------

#[test]
fn test_database_loads_schema() {
    let schema = build_schema(SOCIAL_YAML);
    let db = Database::from_executor(Arc::clone(&schema), Arc::new(StubExecutor::empty()));
    let node_schema = db.schema().all_node_schemas();
    assert!(
        node_schema.contains_key("User"),
        "schema should contain User node"
    );
}

// ---------------------------------------------------------------------------
// Connection — query_to_sql (Cypher → SQL, no execution)
// ---------------------------------------------------------------------------

#[test]
fn test_query_to_sql_returns_clickhouse_sql() {
    let db = stub_db(build_schema(SOCIAL_YAML), vec![]);
    let conn = Connection::new(&db).unwrap();

    let sql = conn
        .query_to_sql("MATCH (u:User) RETURN u.name LIMIT 5")
        .expect("should produce SQL");

    // Verify key structural elements of generated ClickHouse SQL
    assert!(sql.contains("users"), "must reference users table");
    assert!(
        sql.contains("full_name"),
        "property mapping: name → full_name"
    );
    assert!(
        sql.to_uppercase().contains("LIMIT"),
        "should propagate LIMIT"
    );
}

#[test]
fn test_query_to_sql_relationship_generates_join() {
    let db = stub_db(build_schema(SOCIAL_YAML), vec![]);
    let conn = Connection::new(&db).unwrap();

    let sql = conn
        .query_to_sql("MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.name, b.name LIMIT 10")
        .expect("should produce SQL");

    assert!(sql.contains("follows"), "must JOIN via follows table");
    assert!(
        sql.contains("full_name"),
        "property mapping applied to both sides"
    );
}

#[test]
fn test_query_to_sql_where_clause() {
    let db = stub_db(build_schema(SOCIAL_YAML), vec![]);
    let conn = Connection::new(&db).unwrap();

    let sql = conn
        .query_to_sql("MATCH (u:User) WHERE u.age > 30 RETURN u.name")
        .expect("should produce SQL");

    assert!(sql.contains("age"), "age column must appear in WHERE");
    assert!(sql.contains("30"), "literal 30 must appear");
}

// ---------------------------------------------------------------------------
// Connection — query() end-to-end with stub data
// ---------------------------------------------------------------------------

#[test]
fn test_query_returns_correct_rows() {
    let rows = vec![
        serde_json::json!({"u.name": "Alice"}),
        serde_json::json!({"u.name": "Bob"}),
    ];
    let db = stub_db(build_schema(SOCIAL_YAML), rows);
    let conn = Connection::new(&db).unwrap();

    let result = conn
        .query("MATCH (u:User) RETURN u.name")
        .expect("query should succeed");

    assert_eq!(result.num_rows(), 2);
    let rows: Vec<_> = result.collect();
    assert_eq!(rows[0].get("u.name").unwrap().as_str(), Some("Alice"));
    assert_eq!(rows[1].get("u.name").unwrap().as_str(), Some("Bob"));
}

#[test]
fn test_query_empty_result() {
    let db = stub_db(build_schema(SOCIAL_YAML), vec![]);
    let conn = Connection::new(&db).unwrap();

    let result = conn
        .query("MATCH (u:User) RETURN u.name")
        .expect("query should succeed with empty result");

    assert!(result.is_empty());
    assert_eq!(result.num_rows(), 0);
}

#[test]
fn test_query_multiple_columns() {
    let rows = vec![
        serde_json::json!({"u.name": "Alice", "u.age": 30}),
        serde_json::json!({"u.name": "Bob", "u.age": 25}),
    ];
    let db = stub_db(build_schema(SOCIAL_YAML), rows);
    let conn = Connection::new(&db).unwrap();

    let result = conn
        .query("MATCH (u:User) RETURN u.name, u.age")
        .expect("query should succeed");

    assert_eq!(result.get_column_names(), &["u.name", "u.age"]);
    let rows: Vec<_> = result.collect();
    assert_eq!(rows.len(), 2);

    // Row index access
    use clickgraph_embedded::Value;
    assert_eq!(rows[0][0], Value::String("Alice".to_string()));
    assert_eq!(rows[0][1], Value::Int64(30));

    // Row name access
    assert_eq!(rows[1].get("u.name").unwrap().as_str(), Some("Bob"));
    assert_eq!(rows[1].get("u.age").unwrap().as_i64(), Some(25));
}

#[test]
fn test_query_null_values() {
    let rows = vec![serde_json::json!({"u.name": null})];
    let db = stub_db(build_schema(SOCIAL_YAML), rows);
    let conn = Connection::new(&db).unwrap();

    let result = conn
        .query("MATCH (u:User) RETURN u.name")
        .expect("query should succeed");

    let row = result.into_iter().next().unwrap();
    assert!(row.get("u.name").unwrap().is_null());
}

#[test]
fn test_multiple_connections_share_schema() {
    let db = stub_db(
        build_schema(SOCIAL_YAML),
        vec![serde_json::json!({"u.name": "Alice"})],
    );

    // Both connections should work independently
    let conn1 = Connection::new(&db).unwrap();
    let conn2 = Connection::new(&db).unwrap();

    let r1 = conn1.query("MATCH (u:User) RETURN u.name").unwrap();
    let r2 = conn2.query("MATCH (u:User) RETURN u.name").unwrap();

    assert_eq!(r1.num_rows(), 1);
    assert_eq!(r2.num_rows(), 1);
}

#[test]
fn test_parse_error_propagates() {
    let db = stub_db(build_schema(SOCIAL_YAML), vec![]);
    let conn = Connection::new(&db).unwrap();

    let err = conn.query("THIS IS NOT CYPHER !!!").unwrap_err();
    let msg = format!("{}", err);
    assert!(!msg.is_empty(), "error message should be non-empty");
}
