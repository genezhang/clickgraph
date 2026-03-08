//! End-to-end tests with real chdb execution.
//!
//! These tests create CSV test data in a temp directory, build a schema that
//! references them via `table_function:file(…, CSVWithNames)`, then open a
//! real chdb-backed Database, execute Cypher queries, and verify the actual
//! results returned by the embedded ClickHouse engine.
//!
//! Unlike the stub-based tests in `integration.rs`, these exercise the full
//! pipeline including chdb query execution.
//!
//! **Gating**: These tests are skipped by default. Set `CLICKGRAPH_CHDB_TESTS=1`
//! to run them. This keeps `cargo test` fast and avoids requiring a chdb binary
//! for routine development.
//!
//! **Note**: chdb supports only one session per process, so all tests share
//! a single `Database` via `LazyLock`.

use std::sync::LazyLock;

use clickgraph_embedded::{Connection, Database, SystemConfig};

/// Return true if chdb e2e tests are enabled via the environment.
fn chdb_tests_enabled() -> bool {
    std::env::var("CLICKGRAPH_CHDB_TESTS")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Shared fixture — one chdb session for all tests
// ---------------------------------------------------------------------------

struct SharedFixture {
    _dir: tempfile::TempDir,
    db: Database,
}

/// Single shared fixture. chdb can only have one session per process, so
/// we initialise it once and share across all tests.
///
/// The fixture is intentionally leaked (never dropped) to avoid a chdb
/// SIGABRT during session cleanup at process exit.
static FIXTURE: LazyLock<&'static SharedFixture> = LazyLock::new(|| {
    let dir = tempfile::tempdir().expect("create temp dir");

    let users_csv = dir.path().join("users.csv");
    std::fs::write(
        &users_csv,
        "user_id,full_name,age,country\n\
         1,Alice,30,US\n\
         2,Bob,25,UK\n\
         3,Charlie,35,CA\n\
         4,Diana,28,US\n\
         5,Eve,32,DE\n",
    )
    .expect("write users.csv");

    let follows_csv = dir.path().join("follows.csv");
    std::fs::write(
        &follows_csv,
        "follower_id,followed_id,follow_date\n\
         1,2,2024-01-15\n\
         1,3,2024-02-20\n\
         2,3,2024-03-10\n\
         3,1,2024-04-05\n\
         4,1,2024-05-12\n\
         4,2,2024-06-01\n\
         5,1,2024-07-20\n",
    )
    .expect("write follows.csv");

    let schema_yaml = format!(
        r#"name: chdb_e2e
graph_schema:
  nodes:
    - label: User
      database: default
      table: users
      node_id: user_id
      source: "table_function:file('{users}', 'CSVWithNames')"
      property_mappings:
        user_id: user_id
        name: full_name
        age: age
        country: country
  edges:
    - type: FOLLOWS
      database: default
      table: follows
      from_node: User
      to_node: User
      from_id: follower_id
      to_id: followed_id
      source: "table_function:file('{follows}', 'CSVWithNames')"
      property_mappings:
        follow_date: follow_date
"#,
        users = users_csv.display(),
        follows = follows_csv.display(),
    );

    let schema_path = dir.path().join("schema.yaml");
    std::fs::write(&schema_path, &schema_yaml).expect("write schema.yaml");

    let db = Database::new(&schema_path, SystemConfig::default()).expect("open chdb database");
    Box::leak(Box::new(SharedFixture { _dir: dir, db }))
});

fn conn() -> Option<Connection<'static>> {
    if !chdb_tests_enabled() {
        eprintln!("  [skipped] set CLICKGRAPH_CHDB_TESTS=1 to run chdb e2e tests");
        return None;
    }
    Some(Connection::new(&FIXTURE.db).unwrap())
}

// ---------------------------------------------------------------------------
// Node scan tests
// ---------------------------------------------------------------------------

#[test]
fn chdb_basic_node_scan() {
    let Some(conn) = conn() else { return };

    let result = conn
        .query("MATCH (u:User) RETURN u.name ORDER BY u.name")
        .unwrap();

    assert_eq!(result.num_rows(), 5);
    let names: Vec<String> = result
        .map(|row| row.get("u.name").unwrap().as_str().unwrap().to_string())
        .collect();
    assert_eq!(names, vec!["Alice", "Bob", "Charlie", "Diana", "Eve"]);
}

#[test]
fn chdb_where_filter_greater_than() {
    let Some(conn) = conn() else { return };

    let result = conn
        .query("MATCH (u:User) WHERE u.age > 30 RETURN u.name ORDER BY u.name")
        .unwrap();

    let names: Vec<String> = result
        .map(|row| row.get("u.name").unwrap().as_str().unwrap().to_string())
        .collect();
    assert_eq!(names, vec!["Charlie", "Eve"]);
}

#[test]
fn chdb_where_filter_equals() {
    let Some(conn) = conn() else { return };

    let result = conn
        .query("MATCH (u:User) WHERE u.country = 'US' RETURN u.name ORDER BY u.name")
        .unwrap();

    let names: Vec<String> = result
        .map(|row| row.get("u.name").unwrap().as_str().unwrap().to_string())
        .collect();
    assert_eq!(names, vec!["Alice", "Diana"]);
}

// ---------------------------------------------------------------------------
// Aggregation tests
// ---------------------------------------------------------------------------

#[test]
fn chdb_count_aggregation() {
    let Some(conn) = conn() else { return };

    let result = conn.query("MATCH (u:User) RETURN count(u) AS cnt").unwrap();

    assert_eq!(result.num_rows(), 1);
    let row = result.into_iter().next().unwrap();
    assert_eq!(row.get("cnt").unwrap().as_i64(), Some(5));
}

#[test]
fn chdb_count_by_country() {
    let Some(conn) = conn() else { return };

    let result = conn
        .query(
            "MATCH (u:User) RETURN u.country AS country, count(u) AS cnt \
             ORDER BY cnt DESC, country",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 4); // US(2), UK(1), CA(1), DE(1)
    let first = result.into_iter().next().unwrap();
    assert_eq!(first.get("country").unwrap().as_str(), Some("US"));
    assert_eq!(first.get("cnt").unwrap().as_i64(), Some(2));
}

// ---------------------------------------------------------------------------
// ORDER BY and LIMIT
// ---------------------------------------------------------------------------

#[test]
fn chdb_order_by_limit() {
    let Some(conn) = conn() else { return };

    let result = conn
        .query("MATCH (u:User) RETURN u.name ORDER BY u.age DESC LIMIT 3")
        .unwrap();

    assert_eq!(result.num_rows(), 3);
    let names: Vec<String> = result
        .map(|row| row.get("u.name").unwrap().as_str().unwrap().to_string())
        .collect();
    // Charlie(35), Eve(32), Alice(30)
    assert_eq!(names, vec!["Charlie", "Eve", "Alice"]);
}

// ---------------------------------------------------------------------------
// DISTINCT
// ---------------------------------------------------------------------------

#[test]
fn chdb_distinct_values() {
    let Some(conn) = conn() else { return };

    let result = conn
        .query("MATCH (u:User) RETURN DISTINCT u.country ORDER BY u.country")
        .unwrap();

    let countries: Vec<String> = result
        .map(|row| row.get("u.country").unwrap().as_str().unwrap().to_string())
        .collect();
    assert_eq!(countries, vec!["CA", "DE", "UK", "US"]);
}

// ---------------------------------------------------------------------------
// Relationship traversal
// ---------------------------------------------------------------------------

#[test]
fn chdb_relationship_traversal() {
    let Some(conn) = conn() else { return };

    // Alice (user_id=1) follows Bob (2) and Charlie (3)
    let result = conn
        .query(
            "MATCH (a:User)-[:FOLLOWS]->(b:User) \
             WHERE a.user_id = 1 \
             RETURN b.name ORDER BY b.name",
        )
        .unwrap();

    let names: Vec<String> = result
        .map(|row| row.get("b.name").unwrap().as_str().unwrap().to_string())
        .collect();
    assert_eq!(names, vec!["Bob", "Charlie"]);
}

#[test]
fn chdb_follower_count() {
    let Some(conn) = conn() else { return };

    // Followers of Alice (user_id=1): Charlie(3), Diana(4), Eve(5) → 3
    let result = conn
        .query(
            "MATCH (a:User)-[:FOLLOWS]->(b:User) \
             WHERE b.user_id = 1 \
             RETURN b.name, count(a) AS follower_count",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let row = result.into_iter().next().unwrap();
    assert_eq!(row.get("b.name").unwrap().as_str(), Some("Alice"));
    assert_eq!(row.get("follower_count").unwrap().as_i64(), Some(3));
}

// ---------------------------------------------------------------------------
// Multiple properties
// ---------------------------------------------------------------------------

#[test]
fn chdb_multiple_properties() {
    let Some(conn) = conn() else { return };

    let result = conn
        .query("MATCH (u:User) WHERE u.user_id = 1 RETURN u.name, u.age, u.country")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let row = result.into_iter().next().unwrap();
    assert_eq!(row.get("u.name").unwrap().as_str(), Some("Alice"));
    assert_eq!(row.get("u.age").unwrap().as_i64(), Some(30));
    assert_eq!(row.get("u.country").unwrap().as_str(), Some("US"));
}

// ---------------------------------------------------------------------------
// Export SQL generation (no execution — just verify it includes file path)
// ---------------------------------------------------------------------------

#[test]
fn chdb_export_to_sql_parquet() {
    let Some(conn) = conn() else { return };

    let sql = conn
        .export_to_sql(
            "MATCH (u:User) RETURN u.name",
            "/tmp/out.parquet",
            Default::default(),
        )
        .unwrap();

    assert!(sql.contains("/tmp/out.parquet"), "SQL: {}", sql);
    assert!(sql.to_uppercase().contains("PARQUET"), "SQL: {}", sql);
}
