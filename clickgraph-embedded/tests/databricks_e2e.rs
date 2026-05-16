//! End-to-end Databricks test for `clickgraph-embedded`.
//!
//! Builds a `Database::new_databricks(...)` pointed at a `wiremock`
//! server, then runs a Cypher query through `Connection::query_remote`.
//! Verifies:
//! - The Cypher → SQL translation happens under the Spark dialect
//!   (so `Database::dialect` is honored end-to-end).
//! - The SQL is POSTed to the Statement Execution API with PAT auth.
//! - The INLINE JSON response is parsed back into a `QueryResult` with
//!   the right columns and values.
//!
//! Gated on `#[cfg(feature = "databricks")]` — non-databricks builds
//! skip the file entirely.

#![cfg(feature = "databricks")]

use std::io::Write;

use clickgraph_embedded::{Connection, Database, DatabricksConfig};
use serde_json::json;
use tempfile::NamedTempFile;
use wiremock::matchers::{bearer_token, body_partial_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

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

fn write_schema_to_tempfile() -> NamedTempFile {
    let mut f = NamedTempFile::new().expect("tempfile");
    f.write_all(SOCIAL_YAML.as_bytes()).expect("write");
    f.flush().expect("flush");
    f
}

fn cfg_for(server: &MockServer) -> DatabricksConfig {
    let mut c = DatabricksConfig::new("ignored.cloud.databricks.com", "wh-test", "dapi-token");
    c.base_url = Some(server.uri());
    c
}

#[tokio::test(flavor = "multi_thread")]
async fn query_remote_against_databricks_mock_returns_rows() {
    // End-to-end happy path: assert that columns, row count, AND
    // individual cell values all flow correctly through the
    // Spark-JSON → `Value` conversion that
    // `clickgraph-embedded::Connection` does after `execute_json`
    // hands back the mocked response.
    //
    // (SQL-side dialect verification — that the request body uses
    // Spark spellings — lives in
    // `databricks_database_emits_spark_sql_for_collect` below. Here
    // we only check the request/response wiring.)
    let server = MockServer::start().await;
    let schema_file = write_schema_to_tempfile();

    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(bearer_token("dapi-token"))
        .and(body_partial_json(json!({ "warehouse_id": "wh-test" })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "stmt-e2e",
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

    let db = tokio::task::spawn_blocking({
        let path = schema_file.path().to_path_buf();
        let config = cfg_for(&server);
        move || Database::new_databricks(path, config).expect("Database::new_databricks")
    })
    .await
    .expect("spawn_blocking");

    let result = tokio::task::spawn_blocking(move || {
        let conn = Connection::new(&db).expect("Connection::new");
        let result = conn
            .query_remote("MATCH (u:User) RETURN u.user_id, u.name LIMIT 2")
            .expect("query_remote");
        let col_names: Vec<String> = result.get_column_names().to_vec();
        let row_values: Vec<Vec<clickgraph_embedded::Value>> =
            result.map(|row| row.values().to_vec()).collect();
        (col_names, row_values)
    })
    .await
    .expect("spawn_blocking");

    let (cols, rows) = result;
    assert_eq!(cols, vec!["u.user_id", "u.name"]);
    assert_eq!(rows.len(), 2);

    // Cell-level assertions — guard the JSON→Value conversion path.
    // Without these a regression that mis-typed cells (e.g., int→string)
    // would only break downstream consumers, not this test.
    use clickgraph_embedded::Value;
    assert_eq!(rows[0][0], Value::Int64(1));
    assert_eq!(rows[0][1], Value::String("alice".to_string()));
    assert_eq!(rows[1][0], Value::Int64(2));
    assert_eq!(rows[1][1], Value::String("bob".to_string()));
}

#[tokio::test(flavor = "multi_thread")]
async fn query_remote_graph_under_databricks_uses_spark_dialect() {
    // Regression guard for the dialect plumbing in `query_graph_async`:
    // Connection::query_remote_graph() goes through a different
    // codepath than query_remote() — if it doesn't stamp the dialect
    // on the QueryContext, generated SQL falls back to ClickHouse
    // even when the Database is Databricks-backed.
    let server = MockServer::start().await;
    let schema_file = write_schema_to_tempfile();

    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_partial_json(json!({ "warehouse_id": "wh-test" })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "stmt-graph",
            "status": { "state": "SUCCEEDED" },
            "manifest": { "schema": { "columns": [
                { "name": "u" }
            ]}},
            "result": { "data_array": [] }
        })))
        .mount(&server)
        .await;

    tokio::task::spawn_blocking({
        let path = schema_file.path().to_path_buf();
        let config = cfg_for(&server);
        move || {
            let db = Database::new_databricks(path, config).expect("Database::new_databricks");
            let conn = Connection::new(&db).expect("Connection::new");
            // A plain node return is enough — it flows through
            // query_remote_graph → query_graph_async (the second path
            // that needed dialect plumbing) and emits `AS` clauses
            // for the projected columns. Under Spark those use
            // backtick quoting; under CH double quotes. The body
            // inspection below pins which one we got.
            let _ = conn
                .query_remote_graph("MATCH (u:User) RETURN u")
                .expect("query_remote_graph");
        }
    })
    .await
    .expect("spawn_blocking");

    let received = server.received_requests().await.expect("received_requests");
    let body_json: serde_json::Value =
        serde_json::from_slice(&received[0].body).expect("body json");
    let sql = body_json["statement"]
        .as_str()
        .expect("statement is a string");

    // Spark uses backtick alias quoting; CH uses double quotes.
    // Either form proves SQL was generated; the choice of quotes
    // proves the dialect actually flipped in query_graph_async.
    assert!(
        sql.contains("AS `"),
        "expected Spark backtick alias quoting in query_remote_graph SQL — \
         dialect plumbing missing in query_graph_async? got:\n{sql}"
    );
    assert!(
        !sql.contains("AS \""),
        "query_remote_graph leaked CH double-quoted aliases; got:\n{sql}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn databricks_database_emits_spark_sql_for_collect() {
    // Tighter contract test: pin that the SQL actually crossing the
    // wire uses Spark spellings the FunctionMapper routes (e.g.
    // `collect_list` for Cypher `collect()`). If a future regression
    // drops the dialect-stamping step in `query_with_executor_async`
    // this assertion fails before the `expect(1)` does.
    let server = MockServer::start().await;
    let schema_file = write_schema_to_tempfile();

    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_partial_json(json!({ "warehouse_id": "wh-test" })))
        // The submitted SQL is the `statement` field. wiremock's
        // body_partial_json only does exact-match for nested values,
        // so we use a custom matcher via the path/method combo above
        // and verify the SQL shape by extracting it from request
        // log below.
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "stmt-collect",
            "status": { "state": "SUCCEEDED" },
            "manifest": { "schema": { "columns": [
                { "name": "ids" }
            ]}},
            "result": { "data_array": [[ [1, 2, 3] ]] }
        })))
        .mount(&server)
        .await;

    tokio::task::spawn_blocking({
        let path = schema_file.path().to_path_buf();
        let config = cfg_for(&server);
        move || {
            let db = Database::new_databricks(path, config).expect("Database::new_databricks");
            let conn = Connection::new(&db).expect("Connection::new");
            let _ = conn
                .query_remote("MATCH (u:User) RETURN collect(u.user_id) AS ids")
                .expect("query_remote");
        }
    })
    .await
    .expect("spawn_blocking");

    // Inspect the request body that wiremock captured.
    let received = server.received_requests().await.expect("received_requests");
    assert_eq!(received.len(), 1, "expected exactly one POST");
    let body = std::str::from_utf8(&received[0].body).expect("utf8 body");
    let body_json: serde_json::Value = serde_json::from_str(body).expect("body json");
    let sql = body_json["statement"]
        .as_str()
        .expect("statement is a string");

    assert!(
        sql.contains("collect_list("),
        "expected Spark `collect_list(...)` in SQL crossing the wire; got:\n{sql}"
    );
    assert!(
        !sql.contains("groupArray("),
        "Databricks-bound SQL leaked CH `groupArray`; got:\n{sql}"
    );
}
