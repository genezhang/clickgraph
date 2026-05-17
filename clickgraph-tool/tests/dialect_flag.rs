//! Integration tests for the `cg --dialect …` global flag (Phase 4.2).
//!
//! These spawn the actual `cg` binary so they exercise the same clap +
//! config + Database wiring an end user hits. They do NOT need a live
//! ClickHouse or Databricks warehouse — `cg sql` translates only, and
//! `cg validate` plans only.

use std::io::Write;

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::NamedTempFile;

const SOCIAL_YAML: &str = r#"
name: cg_dialect_test
graph_schema:
  nodes:
    - label: User
      database: test_db
      table: users
      node_id: user_id
      property_mappings:
        user_id: user_id
        name: full_name
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

fn write_schema() -> NamedTempFile {
    let mut f = NamedTempFile::new().expect("tempfile");
    f.write_all(SOCIAL_YAML.as_bytes()).expect("write");
    f.flush().expect("flush");
    f
}

#[test]
fn cg_sql_default_emits_clickhouse_spellings() {
    let schema = write_schema();
    Command::cargo_bin("cg")
        .expect("bin")
        .arg("--schema")
        .arg(schema.path())
        .arg("sql")
        .arg("MATCH (u:User) RETURN collect(u.user_id) AS ids")
        .assert()
        .success()
        .stdout(predicate::str::contains("groupArray("));
}

#[test]
fn cg_sql_dialect_databricks_emits_spark_spellings() {
    // `cg sql --dialect databricks` is the contract: same Cypher,
    // Spark spelling. If FunctionMapper routing breaks for any
    // reason this test fails before it ever leaves the build.
    let schema = write_schema();
    let assert = Command::cargo_bin("cg")
        .expect("bin")
        .arg("--schema")
        .arg(schema.path())
        .arg("--dialect")
        .arg("databricks")
        .arg("sql")
        .arg("MATCH (u:User) RETURN collect(u.user_id) AS ids")
        .assert()
        .success();
    assert
        .stdout(predicate::str::contains("collect_list("))
        .stdout(predicate::str::contains("groupArray(").not());
}

#[test]
fn cg_dialect_databricks_via_env() {
    let schema = write_schema();
    Command::cargo_bin("cg")
        .expect("bin")
        .env("CG_DIALECT", "databricks")
        .arg("--schema")
        .arg(schema.path())
        .arg("sql")
        .arg("MATCH (u:User) RETURN collect(u.user_id) AS ids")
        .assert()
        .success()
        .stdout(predicate::str::contains("collect_list("));
}

#[test]
fn cg_validate_accepts_dialect_flag() {
    let schema = write_schema();
    Command::cargo_bin("cg")
        .expect("bin")
        .arg("--schema")
        .arg(schema.path())
        .arg("--dialect")
        .arg("databricks")
        .arg("validate")
        .arg("MATCH (u:User) RETURN u.name")
        .assert()
        .success()
        .stdout(predicate::str::contains("OK"));
}

#[cfg(not(feature = "databricks"))]
#[test]
fn cg_query_databricks_without_databricks_feature_errors_clearly() {
    // Default `cg` build doesn't include the Databricks executor. The
    // user gets a clear pointer to rebuild with `--features databricks`
    // or fall back to `--sql-only`, instead of a confusing
    // ClickHouse-URL error. The execution-path tests (with the feature
    // compiled in) live in `tests/databricks_query.rs`.
    let schema = write_schema();
    Command::cargo_bin("cg")
        .expect("bin")
        .arg("--schema")
        .arg(schema.path())
        .arg("--dialect")
        .arg("databricks")
        .arg("query")
        .arg("MATCH (u:User) RETURN u.name")
        .assert()
        .failure()
        .stderr(predicate::str::contains("--features databricks"))
        .stderr(predicate::str::contains("--sql-only"));
}

#[test]
fn cg_config_file_unknown_dialect_warns_and_falls_back() {
    // A typo in `dialect = …` in config.toml previously fell back to
    // ClickHouse silently. Now `cg` warns on stderr so misconfiguration
    // is visible. We override the XDG config directory via env so the
    // user's real config is not touched.
    let schema = write_schema();
    let tmp = tempfile::tempdir().expect("tmpdir");
    let cfg_dir = tmp.path().join("cg");
    std::fs::create_dir_all(&cfg_dir).expect("mkdir");
    std::fs::write(cfg_dir.join("config.toml"), "dialect = \"sparksql\"\n").expect("cfg");

    Command::cargo_bin("cg")
        .expect("bin")
        .env("XDG_CONFIG_HOME", tmp.path())
        .arg("--schema")
        .arg(schema.path())
        .arg("sql")
        .arg("MATCH (u:User) RETURN u.name")
        .assert()
        .success()
        // Still produces SQL — fall-back is non-fatal.
        .stderr(predicate::str::contains("sparksql"));
}

#[test]
fn cg_query_databricks_sql_only_prints_spark_sql() {
    let schema = write_schema();
    Command::cargo_bin("cg")
        .expect("bin")
        .arg("--schema")
        .arg(schema.path())
        .arg("--dialect")
        .arg("databricks")
        .arg("query")
        .arg("--sql-only")
        .arg("MATCH (u:User) RETURN collect(u.user_id) AS ids")
        .assert()
        .success()
        .stdout(predicate::str::contains("collect_list("));
}
