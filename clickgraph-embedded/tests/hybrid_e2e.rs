//! End-to-end tests for hybrid remote query + local storage.
//!
//! These tests exercise the full round-trip:
//!   Remote ClickHouse (Docker) → query_remote_graph() → store_subgraph() → local query
//!
//! **Requirements**:
//! - A running ClickHouse instance at `http://localhost:8123` with `test_user`/`test_pass`
//! - Set `CLICKGRAPH_HYBRID_TESTS=1` to enable (skipped by default)
//!
//! **Note**: chdb supports only one session per process, so all tests share
//! a single `Database` via `LazyLock`.

#![cfg(feature = "embedded")]

use std::process::Command;
use std::sync::LazyLock;

use clickgraph_embedded::{Connection, Database, RemoteConfig, SystemConfig};
use serial_test::serial;

/// Return true if hybrid e2e tests are enabled via the environment.
fn hybrid_tests_enabled() -> bool {
    std::env::var("CLICKGRAPH_HYBRID_TESTS")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Remote ClickHouse setup — create test database and seed data via curl
// ---------------------------------------------------------------------------

fn ch_exec(sql: &str) {
    let output = Command::new("curl")
        .args([
            "-sf",
            "--data-binary",
            sql,
            "http://localhost:8123/?user=test_user&password=test_pass",
        ])
        .output()
        .expect("curl not found — install curl to run hybrid tests");
    assert!(
        output.status.success(),
        "ClickHouse query failed: {}\nSQL: {}",
        String::from_utf8_lossy(&output.stderr),
        sql
    );
}

fn setup_remote_clickhouse() {
    // Create test tables in remote ClickHouse's `default` database.
    // Schema uses `database: default` so both local chdb and remote CH
    // resolve `default.users` / `default.follows`.
    ch_exec(
        "CREATE TABLE IF NOT EXISTS default.users (
            user_id UInt32, full_name String, age UInt32, country String
        ) ENGINE = ReplacingMergeTree() ORDER BY user_id",
    );
    ch_exec("TRUNCATE TABLE default.users");
    ch_exec(
        "INSERT INTO default.users VALUES
            (1,'Alice',30,'US'),(2,'Bob',25,'UK'),(3,'Charlie',35,'CA'),
            (4,'Diana',28,'US'),(5,'Eve',32,'DE')",
    );
    ch_exec(
        "CREATE TABLE IF NOT EXISTS default.follows (
            follower_id UInt32, followed_id UInt32, follow_date String
        ) ENGINE = ReplacingMergeTree() ORDER BY (follower_id, followed_id)",
    );
    ch_exec("TRUNCATE TABLE default.follows");
    ch_exec(
        "INSERT INTO default.follows VALUES
            (1,2,'2024-01-15'),(1,3,'2024-02-20'),(2,3,'2024-03-10'),
            (3,1,'2024-04-05'),(4,1,'2024-05-12')",
    );
}

// ---------------------------------------------------------------------------
// Shared fixture — one chdb session + remote connection for all tests
// ---------------------------------------------------------------------------

struct HybridFixture {
    _dir: tempfile::TempDir,
    db: Database,
}

static FIXTURE: LazyLock<&'static HybridFixture> = LazyLock::new(|| {
    setup_remote_clickhouse();

    let dir = tempfile::tempdir().expect("create temp dir");
    // Use `default` database so chdb can auto-create local writable tables.
    // The remote ClickHouse also needs these tables — they're in `test_hybrid`
    // database on the remote side. The RemoteConfig sets `database: test_hybrid`
    // so SQL like `SELECT ... FROM default.users` gets routed to the right
    // database on the remote side via the ClickHouse client's database setting.
    let schema_yaml = r#"name: hybrid_e2e
graph_schema:
  nodes:
    - label: User
      database: default
      table: users
      node_id: user_id
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
      property_mappings:
        follow_date: follow_date
"#;
    let schema_path = dir.path().join("schema.yaml");
    std::fs::write(&schema_path, schema_yaml).expect("write schema.yaml");

    let config = SystemConfig {
        session_dir: Some(dir.path().join("chdb_session")),
        remote: Some(RemoteConfig {
            url: "http://localhost:8123".to_string(),
            user: "test_user".to_string(),
            password: "test_pass".to_string(),
            database: Some("default".to_string()),
            cluster_name: None,
        }),
        ..Default::default()
    };

    let db = Database::new(&schema_path, config).expect("open hybrid database");
    Box::leak(Box::new(HybridFixture { _dir: dir, db }))
});

fn conn() -> Option<Connection<'static>> {
    if !hybrid_tests_enabled() {
        eprintln!("  [skipped] set CLICKGRAPH_HYBRID_TESTS=1 to run hybrid e2e tests");
        return None;
    }
    Some(Connection::new(&FIXTURE.db).unwrap())
}

// ---------------------------------------------------------------------------
// Tests: query_remote — tabular results from remote ClickHouse
// ---------------------------------------------------------------------------

#[test]
fn hybrid_query_remote_basic() {
    let Some(conn) = conn() else { return };

    let result = conn
        .query_remote("MATCH (u:User) RETURN u.name ORDER BY u.name")
        .unwrap();

    assert_eq!(result.num_rows(), 5);
    let names: Vec<String> = result
        .map(|row| row.get("u.name").unwrap().as_str().unwrap().to_string())
        .collect();
    assert_eq!(names, vec!["Alice", "Bob", "Charlie", "Diana", "Eve"]);
}

#[test]
fn hybrid_query_remote_with_filter() {
    let Some(conn) = conn() else { return };

    let result = conn
        .query_remote("MATCH (u:User) WHERE u.country = 'US' RETURN u.name ORDER BY u.name")
        .unwrap();

    let names: Vec<String> = result
        .map(|row| row.get("u.name").unwrap().as_str().unwrap().to_string())
        .collect();
    assert_eq!(names, vec!["Alice", "Diana"]);
}

#[test]
fn hybrid_query_remote_relationship() {
    let Some(conn) = conn() else { return };

    let result = conn
        .query_remote(
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

// ---------------------------------------------------------------------------
// Tests: query_remote_graph — structured graph results from remote
// ---------------------------------------------------------------------------

#[test]
fn hybrid_query_remote_graph_nodes() {
    let Some(conn) = conn() else { return };

    let graph = conn
        .query_remote_graph("MATCH (u:User) WHERE u.country = 'US' RETURN u ORDER BY u.name")
        .unwrap();

    assert_eq!(graph.node_count(), 2, "US has 2 users: Alice, Diana");
    assert_eq!(graph.edge_count(), 0, "No edges in node-only query");

    let ids: Vec<&str> = graph.nodes().iter().map(|n| n.id.as_str()).collect();
    assert!(ids.contains(&"User:1"), "Alice (id=1) should be present");
    assert!(ids.contains(&"User:4"), "Diana (id=4) should be present");
}

#[test]
fn hybrid_query_remote_graph_with_edges() {
    let Some(conn) = conn() else { return };

    let graph = conn
        .query_remote_graph(
            "MATCH (a:User)-[r:FOLLOWS]->(b:User) \
             WHERE a.user_id = 1 \
             RETURN a, r, b",
        )
        .unwrap();

    // Alice follows Bob and Charlie → 3 unique nodes, 2 edges
    assert_eq!(graph.node_count(), 3, "Alice + Bob + Charlie");
    assert_eq!(graph.edge_count(), 2, "Alice has 2 FOLLOWS edges");

    for edge in graph.edges() {
        assert_eq!(edge.type_name, "FOLLOWS");
    }
}

// ---------------------------------------------------------------------------
// Tests: full round-trip — remote → store → local query
// ---------------------------------------------------------------------------

#[test]
#[serial]
fn hybrid_full_round_trip() {
    let Some(conn) = conn() else { return };

    // 1. Query remote cluster for a subgraph
    let graph = conn
        .query_remote_graph(
            "MATCH (a:User)-[r:FOLLOWS]->(b:User) \
             WHERE a.country = 'US' \
             RETURN a, r, b",
        )
        .unwrap();

    let remote_node_count = graph.node_count();
    let remote_edge_count = graph.edge_count();
    assert!(remote_node_count > 0, "Should have nodes from remote");
    assert!(remote_edge_count > 0, "Should have edges from remote");

    // 2. Store subgraph locally
    let stats = conn.store_subgraph(&graph).unwrap();
    assert_eq!(stats.nodes_stored, remote_node_count);
    assert_eq!(stats.edges_stored, remote_edge_count);

    // 3. Query locally to verify data was stored
    let local_result = conn
        .query("MATCH (u:User) RETURN u.name ORDER BY u.name")
        .unwrap();

    assert!(
        local_result.num_rows() > 0,
        "Local query should find stored nodes"
    );

    let local_names: Vec<String> = local_result
        .map(|row| row.get("u.name").unwrap().as_str().unwrap().to_string())
        .collect();
    assert!(
        local_names.contains(&"Alice".to_string()),
        "Alice should be in local store, got: {:?}",
        local_names
    );
}

#[test]
#[serial]
fn hybrid_store_subgraph_stats() {
    let Some(conn) = conn() else { return };

    let graph = conn
        .query_remote_graph("MATCH (u:User) WHERE u.user_id = 5 RETURN u")
        .unwrap();

    assert_eq!(graph.node_count(), 1, "Just Eve");
    assert_eq!(graph.edge_count(), 0);

    let stats = conn.store_subgraph(&graph).unwrap();
    assert_eq!(stats.nodes_stored, 1);
    assert_eq!(stats.edges_stored, 0);
}

// ---------------------------------------------------------------------------
// Tests: query_graph — structured graph from local chdb
// ---------------------------------------------------------------------------

#[test]
#[serial]
fn hybrid_query_graph_local() {
    let Some(conn) = conn() else { return };

    // Store some data locally first
    let graph = conn
        .query_remote_graph("MATCH (u:User) WHERE u.user_id <= 2 RETURN u")
        .unwrap();
    conn.store_subgraph(&graph).unwrap();

    // query_graph locally should return structured GraphResult
    let local_graph = conn
        .query_graph("MATCH (u:User) RETURN u ORDER BY u.name")
        .unwrap();

    assert!(
        local_graph.node_count() >= 2,
        "Should have at least Alice and Bob locally"
    );

    for node in local_graph.nodes() {
        assert_eq!(node.labels, vec!["User"]);
    }
}
