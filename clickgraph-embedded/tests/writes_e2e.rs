//! End-to-end tests for embedded-mode Cypher writes (Phase 3).
//!
//! Exercises the full pipeline:
//!
//!   the executor gate (chdb only) → `bound_plan::write::plan_write`
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

/// DETACH DELETE of one node and DELETE of another in one statement: the
/// second's only relationship is the first's, which the statement deletes
/// (Neo4j checks at commit: it succeeds); with another relationship it
/// fails, and nothing changes.
#[test]
fn detach_and_delete_in_one_statement() {
    let Some(conn) = conn() else { return };
    person(&conn, "sf1", 40);
    person(&conn, "sf2", 40);
    knows(&conn, "sf1", "sf2");
    conn.query(
        "MATCH (a:Person {person_id: 'sf1'})-[:KNOWS]->(b:Person {person_id: 'sf2'}) \
         DETACH DELETE a DELETE b",
    )
    .expect("b's only relationship is a's");
    assert_eq!(
        count(
            &conn,
            "MATCH (a:Person) WHERE a.person_id IN ['sf1', 'sf2'] RETURN count(a) AS c"
        ),
        0
    );
    person(&conn, "sg1", 40);
    person(&conn, "sg2", 40);
    person(&conn, "sg3", 40);
    knows(&conn, "sg1", "sg2");
    knows(&conn, "sg3", "sg2");
    let err = conn
        .query(
            "MATCH (a:Person {person_id: 'sg1'})-[:KNOWS]->(b:Person {person_id: 'sg2'}) \
             DETACH DELETE a DELETE b",
        )
        .expect_err("b also has sg3's relationship");
    assert!(err.to_string().contains("still has relationships"), "{err}");
    assert_eq!(
        count(
            &conn,
            "MATCH (a:Person) WHERE a.person_id IN ['sg1', 'sg2'] RETURN count(a) AS c"
        ),
        2
    );
}

/// Cypher sets a value once per record: a SET reading the node's own
/// property of a node in several records is refused (one UPDATE would set
/// it once); a constant one is the same either way.
#[test]
fn set_of_a_node_in_several_records() {
    let Some(conn) = conn() else { return };
    for id in ["sh0", "sh1", "sh2"] {
        person(&conn, id, 40);
    }
    knows(&conn, "sh1", "sh0");
    knows(&conn, "sh2", "sh0");
    let err = conn
        .query(
            "MATCH (u:Person)-[:KNOWS]->(t:Person) WHERE t.person_id = 'sh0' \
             SET t.name = t.name + 'x'",
        )
        .expect_err("t is in two records");
    assert!(
        err.to_string().contains("several of the MATCH's records"),
        "{err}"
    );
    conn.query(
        "MATCH (u:Person)-[:KNOWS]->(t:Person) WHERE t.person_id = 'sh0' SET t.name = 'same'",
    )
    .expect("a constant");
    let mut result = conn
        .query("MATCH (a:Person) WHERE a.person_id = 'sh0' RETURN a.name AS n")
        .expect("read back");
    assert_eq!(
        result.next().unwrap().get("n").unwrap().as_str(),
        Some("same")
    );
    // A list comprehension's variable is the value's own.
    conn.query(
        "MATCH (a:Person) WHERE a.person_id = 'sh1' \
         SET a.name = toString(size([x IN [1, 2, 3] WHERE x > 1]))",
    )
    .expect("a comprehension");
}

// ---- S13b: CREATE on the bound plan ----

/// `MATCH … CREATE`: one node per record, its values read in the record.
#[test]
fn match_create_creates_one_node_per_record() {
    let Some(conn) = conn() else { return };
    person(&conn, "ca1", 40);
    person(&conn, "ca2", 41);
    let mut counters = conn
        .query(
            "MATCH (a:Person) WHERE a.person_id IN ['ca1', 'ca2'] \
             CREATE (c:Person {person_id: a.person_id + '_copy', name: a.name})",
        )
        .expect("MATCH … CREATE");
    assert_eq!(int(&counters.next().unwrap(), "nodes_created"), 2);
    let mut result = conn
        .query(
            "MATCH (a:Person) WHERE a.person_id IN ['ca1_copy', 'ca2_copy'] \
             RETURN a.person_id AS id, a.name AS n ORDER BY id",
        )
        .expect("read back");
    let first = result.next().unwrap();
    assert_eq!(first.get("id").unwrap().as_str(), Some("ca1_copy"));
    assert_eq!(first.get("n").unwrap().as_str(), Some("ca1"));
    let second = result.next().unwrap();
    assert_eq!(second.get("id").unwrap().as_str(), Some("ca2_copy"));
    // No record: no node.
    let mut counters = conn
        .query(
            "MATCH (a:Person) WHERE a.person_id = 'nobody' CREATE (:Person {person_id: 'never'})",
        )
        .expect("no record");
    assert_eq!(int(&counters.next().unwrap(), "nodes_created"), 0);
    assert_eq!(
        count(
            &conn,
            "MATCH (a:Person) WHERE a.person_id = 'never' RETURN count(a) AS c"
        ),
        0
    );
}

/// Several nodes in one CREATE clause.
#[test]
fn create_of_several_nodes() {
    let Some(conn) = conn() else { return };
    let mut counters = conn
        .query("CREATE (:Person {person_id: 'cb1', name: 'x'}), (:Person {person_id: 'cb2', name: 'y'})")
        .expect("CREATE of two");
    assert_eq!(int(&counters.next().unwrap(), "nodes_created"), 2);
    assert_eq!(
        count(
            &conn,
            "MATCH (a:Person) WHERE a.person_id IN ['cb1', 'cb2'] RETURN count(a) AS c"
        ),
        2
    );
}

/// A CREATE value reading a node an earlier SET of the statement changed
/// is refused before anything is written (the records are a snapshot: it
/// would read the old value; Neo4j reads the new one).
#[test]
fn create_reading_a_changed_node_is_refused() {
    let Some(conn) = conn() else { return };
    person(&conn, "cc1", 50);
    let err = conn
        .query(
            "MATCH (a:Person) WHERE a.person_id = 'cc1' SET a.name = 'changed' \
             CREATE (:Person {person_id: 'cc2', name: a.name})",
        )
        .expect_err("reads a changed node");
    assert!(err.to_string().contains("an earlier SET"), "{err}");
    assert_eq!(
        count(
            &conn,
            "MATCH (a:Person) WHERE a.person_id = 'cc2' RETURN count(a) AS c"
        ),
        0
    );
    let mut result = conn
        .query("MATCH (a:Person) WHERE a.person_id = 'cc1' RETURN a.name AS n")
        .expect("read back");
    assert_eq!(
        result.next().unwrap().get("n").unwrap().as_str(),
        Some("cc1")
    );
}

// ---- S13c: SET / REMOVE … RETURN on the bound plan ----

/// RETURN after SET reads the matched records with their new values.
#[test]
fn set_then_return_reads_the_new_values() {
    let Some(conn) = conn() else { return };
    person(&conn, "ra1", 40);
    person(&conn, "ra2", 40);
    let mut result = conn
        .query(
            "MATCH (a:Person) WHERE a.person_id IN ['ra1', 'ra2'] SET a.name = 'renamed' \
             RETURN a.person_id AS id, a.name AS n ORDER BY id",
        )
        .expect("SET … RETURN");
    let counters = result.get_write_counters().cloned().expect("counters");
    assert_eq!(counters.get("properties_set"), Some(&2));
    let first = result.next().unwrap();
    assert_eq!(first.get("id").unwrap().as_str(), Some("ra1"));
    assert_eq!(first.get("n").unwrap().as_str(), Some("renamed"));
    let second = result.next().unwrap();
    assert_eq!(second.get("n").unwrap().as_str(), Some("renamed"));
    assert!(result.next().is_none());
}

/// A SET of a property the MATCH reads, then RETURN: refused before any
/// write (re-reading would match other records; Neo4j returns the same
/// ones).
#[test]
fn set_of_a_matched_property_then_return_is_refused() {
    let Some(conn) = conn() else { return };
    person(&conn, "rb1", 40);
    let err = conn
        .query("MATCH (a:Person) WHERE a.name = 'rb1' SET a.name = 'other' RETURN a.person_id")
        .expect_err("re-read would miss it");
    assert!(err.to_string().contains("which the MATCH reads"), "{err}");
    assert_eq!(
        count(
            &conn,
            "MATCH (a:Person) WHERE a.name = 'rb1' RETURN count(a) AS c"
        ),
        1
    );
}

/// Two CREATE clauses; a SET whose read part needs a CTE (a WITH) — its
/// ids read first (chdb's UPDATE resolves no CTE in its subquery).
#[test]
fn create_clauses_and_set_after_with() {
    let Some(conn) = conn() else { return };
    let mut counters = conn
        .query("CREATE (:Person {person_id: 'rc1', name: 'x'}) CREATE (:Person {person_id: 'rc2', name: 'y'})")
        .expect("two CREATE clauses");
    assert_eq!(int(&counters.next().unwrap(), "nodes_created"), 2);
    conn.query(
        "MATCH (a:Person) WHERE a.person_id IN ['rc1', 'rc2'] WITH a WHERE a.name = 'x' \
         SET a.name = 'after_with'",
    )
    .expect("SET after a WITH");
    let mut result = conn
        .query("MATCH (a:Person) WHERE a.person_id = 'rc1' RETURN a.name AS n")
        .expect("read back");
    assert_eq!(
        result.next().unwrap().get("n").unwrap().as_str(),
        Some("after_with")
    );
}

// ---- S13d: DELETE … RETURN as Neo4j ----

/// RETURN after DELETE reads the records as they were: `count(n)` the
/// deleted nodes.
#[test]
fn delete_return_counts_the_deleted() {
    let Some(conn) = conn() else { return };
    person(&conn, "da1", 40);
    person(&conn, "da2", 40);
    let mut result = conn
        .query("MATCH (a:Person) WHERE a.person_id IN ['da1', 'da2'] DELETE a RETURN count(a) AS c")
        .expect("DELETE … RETURN count");
    assert_eq!(int(&result.next().unwrap(), "c"), 2);
    assert_eq!(
        count(
            &conn,
            "MATCH (a:Person) WHERE a.person_id IN ['da1', 'da2'] RETURN count(a) AS c"
        ),
        0
    );
}

/// A deleted node's properties read by the RETURN: Neo4j's error, before
/// anything is deleted; returned whole: no properties; a NULL of an
/// OPTIONAL MATCH: NULL.
#[test]
fn delete_return_of_the_deleted_node() {
    let Some(conn) = conn() else { return };
    person(&conn, "db1", 40);
    let err = conn
        .query("MATCH (a:Person) WHERE a.person_id = 'db1' DELETE a RETURN a.name")
        .expect_err("reads a deleted node");
    assert!(
        err.to_string().contains("deleted in this transaction"),
        "{err}"
    );
    assert_eq!(
        count(
            &conn,
            "MATCH (a:Person) WHERE a.person_id = 'db1' RETURN count(a) AS c"
        ),
        1
    );
    let mut result = conn
        .query("MATCH (a:Person) WHERE a.person_id = 'db1' DELETE a RETURN a")
        .expect("returned whole");
    let row = result.next().expect("one row");
    for column in result.get_column_names().to_vec() {
        let v = row.get(&column).expect("a column");
        assert!(v.is_null(), "{column} = {v:?}");
    }
    assert_eq!(
        count(
            &conn,
            "MATCH (a:Person) WHERE a.person_id = 'db1' RETURN count(a) AS c"
        ),
        0
    );
    let mut result = conn
        .query("OPTIONAL MATCH (a:Person {person_id: 'nobody'}) DELETE a RETURN a.name AS n")
        .expect("a NULL record");
    assert!(result.next().unwrap().get("n").unwrap().is_null());
}

/// A node of another variable read by the RETURN after a DELETE: Neo4j's
/// error when it is a deleted one (the same node, or one another record
/// deletes), before anything is deleted; a node that is not: read.
#[test]
fn delete_return_of_another_variable() {
    let Some(conn) = conn() else { return };
    for id in ["dc1", "dc2", "dc3"] {
        person(&conn, id, 40);
    }
    // dc1 -> dc2 -> dc3: the second record's `b` is the first's `a`.
    knows(&conn, "dc1", "dc2");
    knows(&conn, "dc2", "dc3");
    let err = conn
        .query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.person_id IN ['dc1', 'dc2'] \
             DETACH DELETE a RETURN b.name AS n",
        )
        .expect_err("dc2 is deleted by the second record");
    assert!(
        err.to_string().contains("deleted in this transaction"),
        "{err}"
    );
    assert_eq!(
        count(
            &conn,
            "MATCH (a:Person) WHERE a.person_id STARTS WITH 'dc' RETURN count(a) AS c"
        ),
        3
    );
    let mut result = conn
        .query(
            "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.person_id = 'dc1' \
             DETACH DELETE a RETURN b.name AS n",
        )
        .expect("dc2 is not deleted");
    assert_eq!(
        result.next().unwrap().get("n").unwrap().as_str(),
        Some("dc2")
    );
    assert_eq!(
        count(
            &conn,
            "MATCH (a:Person) WHERE a.person_id STARTS WITH 'dc' RETURN count(a) AS c"
        ),
        2
    );
    // In a list: refused, nothing deleted.
    let err = conn
        .query("MATCH (a:Person) WHERE a.person_id = 'dc2' DETACH DELETE a RETURN collect(a) AS l")
        .expect_err("a list of a deleted node");
    assert!(err.to_string().contains("not supported"), "{err}");
    assert_eq!(
        count(
            &conn,
            "MATCH (a:Person) WHERE a.person_id = 'dc2' RETURN count(a) AS c"
        ),
        1
    );
}

/// #1447: a write in a later UNION arm reaches the write planner, which
/// refuses it before anything runs (dispatch read only the first arm, and
/// the read path answered "ClickGraph is read-only"). The `CYPHER` prelude
/// is read and dropped on the write path too.
#[test]
fn a_write_in_a_later_union_arm_is_refused_by_the_write_planner() {
    let Some(conn) = conn() else { return };
    person(&conn, "un1", 40);
    let err = conn
        .query(
            "MATCH (x:Person) RETURN x.name AS n UNION MATCH (y:Person) WHERE \
             y.person_id = 'un1' SET y.name = 'w' RETURN y.name AS n",
        )
        .expect_err("an update in a UNION");
    assert!(
        err.to_string().contains("an updating clause in a UNION"),
        "{err}"
    );
    let mut result = conn
        .query("MATCH (a:Person) WHERE a.person_id = 'un1' RETURN a.name AS n")
        .expect("read back");
    assert_eq!(
        result.next().unwrap().get("n").unwrap().as_str(),
        Some("un1")
    );
    conn.query(
        "CYPHER replan=force MATCH (a:Person) WHERE a.person_id = 'un1' SET a.name = 'un1b'",
    )
    .expect("a prelude before a write");
    assert_eq!(
        count(
            &conn,
            "MATCH (a:Person) WHERE a.person_id = 'un1' AND a.name = 'un1b' RETURN count(a) AS c"
        ),
        1
    );
}

/// #1373: a long statement is parsed and translated on a thread with the
/// servers' stack, and its SQL runs on chdb from there; a statement of more
/// than 1000 clauses is refused.
#[test]
fn a_long_statement_runs_on_chdb() {
    let Some(conn) = conn() else { return };
    person(&conn, "ls1", 20);
    let chain = |n: usize| {
        format!(
            "MATCH (a:Person) WHERE a.person_id = 'ls1' {}RETURN count(a) AS c",
            "WITH a LIMIT 1000 ".repeat(n)
        )
    };
    // 60 WITHs: 120 clause keywords, past the inline limit; ClickHouse
    // runs at most 100 nested subqueries (`max_subquery_depth`).
    assert_eq!(count(&conn, &chain(60)), 1);
    let err = conn.query(&chain(1200)).expect_err("too many clauses");
    assert!(err.to_string().contains("(at most 1000)"), "{err}");
}
