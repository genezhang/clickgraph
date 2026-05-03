//! End-to-end tests for embedded-mode Cypher writes (Phase 3).
//!
//! Exercises the full pipeline:
//!
//!   parse → plan (write variants) → write_guard → write_plan_builder
//!     → write_to_sql → chdb execution → read-back verification
//!
//! Unlike `chdb_e2e.rs`, these tests use a *writable* schema (no `source:`
//! field) so the data_loader emits `CREATE TABLE` instead of `CREATE VIEW`.
//!
//! **Gating**: Skipped by default. Set `CLICKGRAPH_CHDB_TESTS=1` to run.
//! Requires the `embedded` feature (uses `Database::new` which loads chdb).
//! chdb supports only one session per process, so these run in their own
//! integration-test binary (separate from `chdb_e2e.rs`).

#![cfg(feature = "embedded")]

use std::sync::LazyLock;

use clickgraph_embedded::{Connection, Database, SystemConfig};

fn chdb_tests_enabled() -> bool {
    std::env::var("CLICKGRAPH_CHDB_TESTS")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

struct WritesFixture {
    _dir: tempfile::TempDir,
    db: Database,
}

static FIXTURE: LazyLock<&'static WritesFixture> = LazyLock::new(|| {
    let dir = tempfile::tempdir().expect("temp dir");

    // Writable schema — no `source:` fields, so the data_loader emits
    // CREATE TABLE with the lightweight-UPDATE block-tracking SETTINGS.
    let schema_yaml = r#"name: writes_e2e
graph_schema:
  nodes:
    - label: Person
      database: default
      table: persons
      node_id: person_id
      type: string
      property_mappings:
        person_id: person_id
        name: full_name
        age: age
  edges:
    - type: KNOWS
      database: default
      table: knows
      from_node: Person
      to_node: Person
      from_id: from_person_id
      to_id: to_person_id
      property_mappings:
        since: since_year
"#;

    let schema_path = dir.path().join("schema.yaml");
    std::fs::write(&schema_path, schema_yaml).expect("write schema.yaml");

    let db = Database::new(&schema_path, SystemConfig::default()).expect("open chdb database");
    Box::leak(Box::new(WritesFixture { _dir: dir, db }))
});

fn conn() -> Option<Connection<'static>> {
    if !chdb_tests_enabled() {
        eprintln!("  [skipped] set CLICKGRAPH_CHDB_TESTS=1 to run write e2e tests");
        return None;
    }
    Some(Connection::new(&FIXTURE.db).unwrap())
}

#[test]
fn create_node_then_match_back() {
    let Some(conn) = conn() else { return };

    let mut counters = conn
        .query("CREATE (a:Person {person_id: 'u1', name: 'Alice', age: 30})")
        .expect("CREATE");
    let row = counters.next().unwrap();
    assert_eq!(row.get("nodes_created").unwrap().as_i64(), Some(1));

    let mut result = conn
        .query("MATCH (a:Person) WHERE a.person_id = 'u1' RETURN a.name, a.age")
        .expect("MATCH back");
    let row = result.next().unwrap();
    assert_eq!(row.get("a.name").unwrap().as_str(), Some("Alice"));
    assert_eq!(row.get("a.age").unwrap().as_i64(), Some(30));
}

#[test]
fn set_property_changes_value() {
    let Some(conn) = conn() else { return };

    conn.query("CREATE (a:Person {person_id: 'u2', name: 'Bob', age: 25})")
        .expect("CREATE");
    conn.query("MATCH (a:Person) WHERE a.person_id = 'u2' SET a.age = 26")
        .expect("SET");

    let mut result = conn
        .query("MATCH (a:Person) WHERE a.person_id = 'u2' RETURN a.age")
        .expect("MATCH back");
    let row = result.next().unwrap();
    assert_eq!(row.get("a.age").unwrap().as_i64(), Some(26));
}

#[test]
fn delete_removes_node() {
    let Some(conn) = conn() else { return };

    conn.query("CREATE (a:Person {person_id: 'u3', name: 'Charlie'})")
        .expect("CREATE");
    conn.query("MATCH (a:Person) WHERE a.person_id = 'u3' DELETE a")
        .expect("DELETE");

    let result = conn
        .query("MATCH (a:Person) WHERE a.person_id = 'u3' RETURN count(a) AS cnt")
        .expect("count back");
    let row = result.into_iter().next().unwrap();
    assert_eq!(row.get("cnt").unwrap().as_i64(), Some(0));
}

#[test]
fn remove_property_sets_null() {
    let Some(conn) = conn() else { return };

    conn.query("CREATE (a:Person {person_id: 'u4', name: 'Diana', age: 28})")
        .expect("CREATE");
    conn.query("MATCH (a:Person) WHERE a.person_id = 'u4' REMOVE a.age")
        .expect("REMOVE");

    let mut result = conn
        .query("MATCH (a:Person) WHERE a.person_id = 'u4' RETURN a.age")
        .expect("MATCH back");
    let row = result.next().unwrap();
    // After REMOVE, `a.age` should be NULL.
    assert!(row.get("a.age").unwrap().is_null());
}
