//! `cg sql` / `cg validate` with `--clickhouse` translate with the column
//! types read from it (docs/design/VALUE_TYPES.md T1), and without them,
//! saying so, when they cannot be read. No ClickHouse is needed: the URL
//! names a port nothing listens on.

use std::io::Write;

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::NamedTempFile;

const SCHEMA: &str = r#"
name: cg_column_types_test
graph_schema:
  nodes:
    - label: User
      database: test_db
      table: users
      node_id: user_id
      property_mappings:
        user_id: user_id
        name: full_name
  edges: []
"#;

fn schema(yaml: &str) -> NamedTempFile {
    let mut f = NamedTempFile::new().expect("tempfile");
    f.write_all(yaml.as_bytes()).expect("write");
    f.flush().expect("flush");
    f
}

#[test]
fn unreadable_types_translate_without_them_and_warn() {
    let s = schema(SCHEMA);
    Command::cargo_bin("cg")
        .expect("bin")
        .args(["--schema"])
        .arg(s.path())
        .args(["--clickhouse", "http://127.0.0.1:1", "sql"])
        .arg("MATCH (u:User) RETURN u.name")
        .assert()
        .success()
        .stdout(predicate::str::contains("full_name"))
        .stderr(predicate::str::contains(
            "warning: could not read the column types",
        ));
}

#[test]
fn a_wrong_schema_is_an_error_not_a_connection_warning() {
    let s = schema(&SCHEMA.replace(
        "      node_id: user_id\n",
        "      node_id: user_id\n      column_types: {user_id: 'Array('}\n",
    ));
    Command::cargo_bin("cg")
        .expect("bin")
        .args(["--schema"])
        .arg(s.path())
        .args(["--clickhouse", "http://127.0.0.1:1", "validate"])
        .arg("MATCH (u:User) RETURN u.name")
        .assert()
        .failure()
        .stderr(predicate::str::contains("'Array(' for column 'user_id'"))
        .stderr(predicate::str::contains("could not read column types").not());
}
