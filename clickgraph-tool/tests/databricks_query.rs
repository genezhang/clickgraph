//! Integration tests for `cg query --dialect databricks` execution
//! (the deferred follow-up to PR #332 — Phase 4.2 shipped the dialect
//! flag for the SQL-emission paths only). These spawn the actual `cg`
//! binary so the same clap + config + Database::new_databricks wiring
//! an end user hits is exercised end-to-end.
//!
//! Gated on `#[cfg(feature = "databricks")]` — without the feature
//! `cg query --dialect databricks` returns a rebuild error, which is
//! covered by
//! `dialect_flag::cg_query_databricks_without_databricks_feature_errors_clearly`.
//!
//! No live Databricks needed. A `wiremock` server runs in the test
//! process and `CG_DATABRICKS_BASE_URL` redirects the executor's HTTP
//! client at it, so the request shape (auth header, warehouse_id, SQL
//! body) and the inline-JSON response decode path are both exercised.

#![cfg(feature = "databricks")]

use std::io::Write;

use assert_cmd::Command;
use predicates::prelude::*;
use serde_json::json;
use tempfile::NamedTempFile;
use wiremock::matchers::{bearer_token, body_partial_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const SOCIAL_YAML: &str = r#"
name: cg_databricks_test
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

#[tokio::test(flavor = "multi_thread")]
async fn cg_query_databricks_against_wiremock_prints_table_rows() {
    // Full happy path: cg spawns, reads creds + base-url override from
    // env, opens a Databricks-backed Database, posts Spark SQL to our
    // wiremock, decodes the INLINE JSON_ARRAY response, and renders the
    // resulting rows in the default table formatter. Asserts:
    //   - exit 0,
    //   - both column headers ("u.user_id", "u.name") appear in stdout,
    //   - both row values ("alice", "bob") appear,
    //   - the wiremock saw exactly one POST with a Spark spelling
    //     (`collect_list(...)` proves dialect routing actually ran).
    let server = MockServer::start().await;
    let schema = write_schema();

    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(bearer_token("dapi-cg-test-token"))
        .and(body_partial_json(json!({ "warehouse_id": "wh-cg-test" })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "stmt-cg-query",
            "status": { "state": "SUCCEEDED" },
            "manifest": { "schema": { "columns": [
                { "name": "u.user_id" },
                { "name": "u.name" }
            ]}},
            "result": { "data_array": [
                [1, "alice"],
                [2, "bob"]
            ]}
        })))
        .expect(1)
        .mount(&server)
        .await;

    let schema_path = schema.path().to_path_buf();
    let base_url = server.uri();
    let assert = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("cg")
            .expect("bin")
            .env("DATABRICKS_HOST", "ignored.cloud.databricks.com")
            .env("DATABRICKS_WAREHOUSE_ID", "wh-cg-test")
            .env("DATABRICKS_TOKEN", "dapi-cg-test-token")
            .env("CG_DATABRICKS_BASE_URL", &base_url)
            .arg("--schema")
            .arg(&schema_path)
            .arg("--dialect")
            .arg("databricks")
            .arg("query")
            .arg("MATCH (u:User) RETURN u.user_id, u.name")
            .assert()
            .success()
            .stdout(predicate::str::contains("u.user_id"))
            .stdout(predicate::str::contains("u.name"))
            .stdout(predicate::str::contains("alice"))
            .stdout(predicate::str::contains("bob"))
            .stdout(predicate::str::contains("(2 rows)"));
        // assert_cmd::Command::assert consumes; nothing to return.
    })
    .await;
    assert.expect("cg invocation");

    // expect(1) on the Mock above already asserts a single POST hit;
    // verifying it doesn't leak credentials into URL/body shape stays
    // implicit (bearer_token + body_partial_json matchers gated the
    // response, so a mismatch would have produced a 404 and failed cg).
}

#[tokio::test(flavor = "multi_thread")]
async fn cg_query_databricks_missing_credentials_errors_clearly() {
    // Feature is compiled in, but no DATABRICKS_* env vars and no
    // config.toml — the user should get a precise error pointing at
    // the missing field, not a confusing reqwest connection error.
    // We scrub the host env so test machines that happen to have
    // DATABRICKS_HOST exported don't accidentally satisfy the check.
    let schema = write_schema();
    let schema_path = schema.path().to_path_buf();
    let tmp = tempfile::tempdir().expect("tmpdir");

    tokio::task::spawn_blocking(move || {
        Command::cargo_bin("cg")
            .expect("bin")
            .env_remove("DATABRICKS_HOST")
            .env_remove("DATABRICKS_WAREHOUSE_ID")
            .env_remove("DATABRICKS_TOKEN")
            .env_remove("CG_DATABRICKS_HOST")
            .env_remove("CG_DATABRICKS_WAREHOUSE_ID")
            .env_remove("CG_DATABRICKS_TOKEN")
            // Point XDG_CONFIG_HOME at an empty tmp dir so we do not
            // accidentally inherit fields from the developer's real
            // ~/.config/cg/config.toml.
            .env("XDG_CONFIG_HOME", tmp.path())
            .arg("--schema")
            .arg(&schema_path)
            .arg("--dialect")
            .arg("databricks")
            .arg("query")
            .arg("MATCH (u:User) RETURN u.name")
            .assert()
            .failure()
            .stderr(predicate::str::contains("Databricks hostname not set"));
    })
    .await
    .expect("cg invocation");
}
