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

// ---- S13a: a write of several statements works on a snapshot of the
// MATCH's records (review of PR #1437) ----

/// An integer read back (an Int64 reads back as a string: #1436).
fn int(row: &clickgraph_embedded::Row, column: &str) -> i64 {
    let v = row.get(column).expect("column");
    v.as_i64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        .unwrap_or_else(|| panic!("{column} is not an integer: {v:?}"))
}

fn count(conn: &Connection<'_>, cypher: &str) -> i64 {
    let mut result = conn.query(cypher).expect("count query");
    int(&result.next().expect("a row"), "c")
}

fn person(conn: &Connection<'_>, id: &str, age: i64) {
    conn.query(&format!(
        "CREATE (a:Person {{person_id: '{id}', name: '{id}', age: {age}}})"
    ))
    .expect("CREATE");
}

fn knows(conn: &Connection<'_>, from: &str, to: &str) {
    conn.create_edge("KNOWS", from, to, Default::default())
        .expect("create_edge");
}

/// DETACH DELETE of the nodes a pattern matches: every one, with all its
/// relationships, though deleting the first relationships unmatches them.
#[test]
fn detach_delete_of_matched_nodes_uses_one_snapshot() {
    let Some(conn) = conn() else { return };
    for id in ["sa1", "sa2", "sa3", "sa4"] {
        person(&conn, id, 40);
    }
    knows(&conn, "sa2", "sa1");
    knows(&conn, "sa3", "sa1");
    knows(&conn, "sa4", "sa2");
    knows(&conn, "sa2", "sa3");
    let mut counters = conn
        .query("MATCH (u:Person)-[:KNOWS]->(t:Person) WHERE t.person_id = 'sa1' DETACH DELETE u")
        .expect("DETACH DELETE");
    let row = counters.next().unwrap();
    assert_eq!(int(&row, "nodes_deleted"), 2);
    assert_eq!(int(&row, "relationships_deleted"), 4);
    assert_eq!(
        count(
            &conn,
            "MATCH (a:Person) WHERE a.person_id IN ['sa2', 'sa3'] RETURN count(a) AS c"
        ),
        0
    );
    assert_eq!(
        count(
            &conn,
            "MATCH (a:Person) WHERE a.person_id IN ['sa1', 'sa4'] RETURN count(a) AS c"
        ),
        2
    );
    assert_eq!(
        count(
            &conn,
            "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.person_id STARTS WITH 'sa' \
             RETURN count(*) AS c"
        ),
        0
    );
}

#[test]
fn set_then_delete_deletes() {
    let Some(conn) = conn() else { return };
    person(&conn, "sb1", 40);
    conn.query("MATCH (a:Person) WHERE a.person_id = 'sb1' SET a.age = 1 DELETE a")
        .expect("SET … DELETE");
    assert_eq!(
        count(
            &conn,
            "MATCH (a:Person) WHERE a.person_id = 'sb1' RETURN count(a) AS c"
        ),
        0
    );
}

#[test]
fn detach_delete_of_two_matched_nodes() {
    let Some(conn) = conn() else { return };
    person(&conn, "sc1", 40);
    person(&conn, "sc2", 40);
    knows(&conn, "sc1", "sc2");
    let mut counters = conn
        .query(
            "MATCH (a:Person {person_id: 'sc1'})-[:KNOWS]->(b:Person {person_id: 'sc2'}) \
             DETACH DELETE a, b",
        )
        .expect("DETACH DELETE a, b");
    let row = counters.next().unwrap();
    assert_eq!(int(&row, "nodes_deleted"), 2);
    // Their one relationship, counted once.
    assert_eq!(int(&row, "relationships_deleted"), 1);
    assert_eq!(
        count(
            &conn,
            "MATCH (a:Person) WHERE a.person_id IN ['sc1', 'sc2'] RETURN count(a) AS c"
        ),
        0
    );
}

/// A SET clause reads what the clause before it wrote.
#[test]
fn set_clauses_apply_in_order() {
    let Some(conn) = conn() else { return };
    person(&conn, "sd1", 30);
    conn.query(
        "MATCH (a:Person) WHERE a.person_id = 'sd1' SET a.name = 'renamed' SET a.age = a.name",
    )
    .expect("two SET clauses");
    let mut result = conn
        .query("MATCH (a:Person) WHERE a.person_id = 'sd1' RETURN a.age AS v")
        .expect("read back");
    // The column is a string (this schema declares no property types).
    assert_eq!(
        result.next().unwrap().get("v").unwrap().as_str(),
        Some("renamed")
    );
}

/// A node with relationships is not deleted without DETACH (Neo4j's rule):
/// the statement fails and nothing changes.
#[test]
fn delete_of_a_connected_node_is_refused() {
    let Some(conn) = conn() else { return };
    person(&conn, "se1", 40);
    person(&conn, "se2", 40);
    knows(&conn, "se1", "se2");
    let err = conn
        .query("MATCH (a:Person) WHERE a.person_id = 'se2' DELETE a")
        .expect_err("a connected node");
    assert!(err.to_string().contains("still has relationships"), "{err}");
    assert_eq!(
        count(
            &conn,
            "MATCH (a:Person) WHERE a.person_id = 'se2' RETURN count(a) AS c"
        ),
        1
    );
}
