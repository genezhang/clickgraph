//! `auto_discover_columns` in every embedded mode (#1322).
//!
//! A schema element with `auto_discover_columns: true` takes its properties
//! from the table's columns, so the mode must read them: chdb from the
//! element's `source:`, remote from ClickHouse's `system.columns`. SQL-only
//! has no database and refuses the schema.
//!
//! **Gating**: the chdb test needs the `embedded` feature and
//! `CLICKGRAPH_CHDB_TESTS=1`; the remote test needs a ClickHouse at
//! `http://localhost:8123` with `test_user`/`test_pass` and
//! `CLICKGRAPH_HYBRID_TESTS=1`. The SQL-only test always runs.

use std::process::Command;

use clickgraph_embedded::{Connection, Database, RemoteConfig};

fn enabled(var: &str) -> bool {
    std::env::var(var)
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// A schema whose User node discovers its columns: camelCase property names,
/// `_version` excluded, and a declared `name` mapping alongside.
fn schema(source: Option<&str>) -> String {
    let source = source
        .map(|s| format!("      source: \"{s}\"\n"))
        .unwrap_or_default();
    format!(
        r#"name: auto_discover_e2e
graph_schema:
  nodes:
    - label: User
      database: cg_auto_discover
      table: users
      node_id: user_id
{source}      auto_discover_columns: true
      naming_convention: camelCase
      exclude_columns: [_version]
      property_mappings:
        name: full_name
  edges: []
"#
    )
}

fn write_schema(dir: &tempfile::TempDir, yaml: &str) -> std::path::PathBuf {
    let path = dir.path().join("schema.yaml");
    std::fs::write(&path, yaml).expect("write schema.yaml");
    path
}

/// `(name, fullName, homeCity)` for every user, ordered by id.
fn read_users(conn: &Connection, remote: bool) -> Vec<(String, String, String)> {
    let cypher = "MATCH (u:User) RETURN u.name AS n, u.fullName AS f, u.homeCity AS c \
                  ORDER BY u.userId";
    let result = if remote {
        conn.query_remote(cypher)
    } else {
        conn.query(cypher)
    }
    .expect("query discovered properties");
    result
        .map(|row| {
            let s = |k: &str| row.get(k).unwrap().as_str().unwrap().to_string();
            (s("n"), s("f"), s("c"))
        })
        .collect()
}

fn expected_users() -> Vec<(String, String, String)> {
    [("Alice", "Paris"), ("Bob", "Oslo")]
        .into_iter()
        .map(|(n, c)| (n.to_string(), n.to_string(), c.to_string()))
        .collect()
}

#[test]
fn sql_only_refuses_a_discovering_schema() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_schema(&dir, &schema(None));
    let err = Database::sql_only(&path)
        .err()
        .expect("sql_only cannot read columns")
        .to_string();
    assert!(
        err.contains("node 'User' sets auto_discover_columns"),
        "{err}"
    );
}

#[cfg(feature = "embedded")]
#[test]
fn chdb_reads_columns_from_the_source() {
    if !enabled("CLICKGRAPH_CHDB_TESTS") {
        eprintln!("  [skipped] set CLICKGRAPH_CHDB_TESTS=1 to run chdb e2e tests");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let csv = dir.path().join("users.csv");
    std::fs::write(
        &csv,
        "user_id,full_name,home_city,_version\n1,Alice,Paris,3\n2,Bob,Oslo,7\n",
    )
    .unwrap();
    let source = format!("table_function:file('{}', 'CSVWithNames')", csv.display());
    let yaml = schema(Some(&source)).replace("database: cg_auto_discover", "database: default");
    let path = write_schema(&dir, &yaml);

    let db = Database::new(&path, clickgraph_embedded::SystemConfig::default())
        .expect("open chdb database");
    let conn = Connection::new(&db).unwrap();
    assert_eq!(read_users(&conn, false), expected_users());
    // chdb cannot run a second session in this process; leak to skip cleanup
    std::mem::forget(db);
}

fn ch_exec(sql: &str) {
    let output = Command::new("curl")
        .args([
            "-sf",
            "--data-binary",
            sql,
            "http://localhost:8123/?user=test_user&password=test_pass",
        ])
        .output()
        .expect("run curl");
    assert!(
        output.status.success(),
        "ClickHouse rejected {sql}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn remote_reads_columns_from_system_columns() {
    if !enabled("CLICKGRAPH_HYBRID_TESTS") {
        eprintln!("  [skipped] set CLICKGRAPH_HYBRID_TESTS=1 to run remote e2e tests");
        return;
    }
    ch_exec("CREATE DATABASE IF NOT EXISTS cg_auto_discover");
    ch_exec("DROP TABLE IF EXISTS cg_auto_discover.users");
    ch_exec(
        "CREATE TABLE cg_auto_discover.users (user_id UInt32, full_name String, \
         home_city String, _version UInt8) ENGINE = MergeTree ORDER BY user_id",
    );
    ch_exec(
        "INSERT INTO cg_auto_discover.users VALUES (1, 'Alice', 'Paris', 3), (2, 'Bob', 'Oslo', 7)",
    );

    let dir = tempfile::tempdir().unwrap();
    let remote = RemoteConfig {
        url: "http://localhost:8123".to_string(),
        user: "test_user".to_string(),
        password: "test_pass".to_string(),
        database: None,
        cluster_name: None,
    };
    let db = Database::new_remote(write_schema(&dir, &schema(None)), remote.clone())
        .expect("open remote database");
    let conn = Connection::new(&db).unwrap();
    assert_eq!(read_users(&conn, true), expected_users());

    // A discovering element whose table does not exist is refused
    let missing = schema(None).replace("table: users", "table: no_such_table");
    let err = Database::new_remote(write_schema(&dir, &missing), remote)
        .err()
        .expect("missing table")
        .to_string();
    assert!(err.contains("no columns were found"), "{err}");
}
