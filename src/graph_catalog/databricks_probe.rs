//! Schema discovery against a Databricks SQL Warehouse (DeltaGraph Phase 3).
//!
//! Mirrors [`crate::graph_catalog::schema_discovery::SchemaDiscovery`]
//! but drives the [`DatabricksSqlExecutor`] over Databricks' standard
//! catalog DDL — `SHOW TABLES IN catalog.schema` and `DESCRIBE TABLE
//! EXTENDED catalog.schema.table` — instead of ClickHouse's
//! `system.tables` / `system.columns`. The returned `IntrospectResponse`
//! shape is identical so downstream `cg schema discover` LLM prompts
//! and `IntrospectHandler` HTTP responses work unchanged.
//!
//! ## What we deliberately skip
//!
//! - **Row count.** `SELECT count(*)` on a Databricks table can spin
//!   up the warehouse and scan TBs. Discovery should be cheap; users
//!   who want row counts can pull them post-hoc.
//! - **Primary-key / sort-key metadata.** Databricks tables don't
//!   surface enforced PKs via `DESCRIBE`. We leave `is_primary_key` /
//!   `is_in_order_by` as `false` — `generate_suggestions` then falls
//!   back to its name-heuristic path (`_id` / `_key` columns).
//! - **Unity Catalog metadata** (owner, comment, properties). Not
//!   needed for graph-schema authoring; would belong in a richer
//!   `DESCRIBE TABLE EXTENDED` parser if and when downstream tooling
//!   wants it.

use crate::executor::databricks_sql::DatabricksSqlExecutor;
use crate::executor::QueryExecutor;
use crate::graph_catalog::schema_discovery::{
    ColumnMetadata, IntrospectResponse, SchemaDiscovery, TableMetadata,
};
use serde_json::Value;

/// Reject anything outside the conservative SQL-identifier shape that
/// also matches the catalog/schema/table grammar Databricks uses for
/// unquoted names. Quoted backtick names are rejected up front — the
/// LLM-driven `cg schema discover` flow always passes plain names, and
/// keeping the validator strict means there is no SQL-injection
/// surface for callers that forget to scrub user input.
fn validate_identifier(id: &str) -> Result<&str, String> {
    if id.is_empty() {
        return Err("identifier is empty".to_string());
    }
    if !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(format!(
            "identifier {id:?} contains characters outside [A-Za-z0-9_]; \
             quote your name or pass an ASCII alphanumeric one"
        ));
    }
    Ok(id)
}

pub struct DatabricksProbe;

impl DatabricksProbe {
    /// Discover tables in `catalog.schema` via SHOW TABLES, then for
    /// each one fetch column metadata via DESCRIBE TABLE EXTENDED and
    /// a 3-row sample via SELECT. Returns the same `IntrospectResponse`
    /// shape `SchemaDiscovery::introspect` returns so cg and the HTTP
    /// `schemas/draft` handler can consume either source.
    pub async fn introspect(
        executor: &DatabricksSqlExecutor,
        catalog: &str,
        schema: &str,
    ) -> Result<IntrospectResponse, String> {
        let catalog = validate_identifier(catalog)?;
        let schema = validate_identifier(schema)?;

        let tables = list_tables(executor, catalog, schema).await?;

        let mut table_metadata = Vec::with_capacity(tables.len());
        let mut suggestions = Vec::new();

        for table_name in tables {
            let columns = describe_columns(executor, catalog, schema, &table_name).await?;
            // Sample failures are non-fatal — a permission-denied SELECT
            // on a single table shouldn't kill discovery for the rest.
            let sample = sample_rows(executor, catalog, schema, &table_name)
                .await
                .unwrap_or_default();

            suggestions.extend(SchemaDiscovery::generate_suggestions(&table_name, &columns));

            table_metadata.push(TableMetadata {
                name: table_name,
                columns,
                row_count: None,
                sample,
            });
        }

        let next_step = format!(
            "Review tables and columns above, then create your schema.\n\
             To generate a YAML draft, point `cg schema discover` (or the LLM-assisted \
             /schemas/draft endpoint) at the same catalog/schema:\n  \
             cg --schema <yaml> --dialect databricks schema discover --catalog {catalog} --database {schema}"
        );

        Ok(IntrospectResponse {
            database: format!("{catalog}.{schema}"),
            tables: table_metadata,
            next_step,
            suggestions,
        })
    }
}

/// `SHOW TABLES IN catalog.schema` on Databricks returns rows shaped:
///   { database: "schema_name", tableName: "users", isTemporary: false }
/// We only need `tableName`. Temporary tables are filtered out — they
/// won't survive past the SQL Warehouse session anyway.
async fn list_tables(
    executor: &DatabricksSqlExecutor,
    catalog: &str,
    schema: &str,
) -> Result<Vec<String>, String> {
    let sql = format!("SHOW TABLES IN `{catalog}`.`{schema}`");
    let rows = executor
        .execute_json(&sql, None)
        .await
        .map_err(|e| format!("SHOW TABLES failed: {e}"))?;

    let mut tables = Vec::new();
    for row in rows {
        // `tableName` is the canonical column name in Spark/Databricks.
        // We tolerate a single-column row shape (just the name) too,
        // in case a forked warehouse returns the older `Tables_in_*`
        // ClickHouse-like header.
        let name = row
            .get("tableName")
            .and_then(Value::as_str)
            .or_else(|| row.get("table_name").and_then(Value::as_str))
            .or_else(|| {
                // Last-resort fallback for forked engines that return
                // a single-column row without the canonical `tableName`
                // header (e.g. a ClickHouse-style `Tables_in_<db>`).
                row.as_object()
                    .and_then(|m| m.values().next())
                    .and_then(Value::as_str)
            });
        let is_temp = row
            .get("isTemporary")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if let (Some(name), false) = (name, is_temp) {
            // Skip system / Spark-internal listings if any leak through.
            if !name.is_empty() {
                tables.push(name.to_string());
            }
        }
    }
    tables.sort();
    Ok(tables)
}

/// `DESCRIBE TABLE EXTENDED catalog.schema.table` returns one row per
/// table column followed by separator / metadata rows. The separator
/// is conventionally a row with `col_name = ""` and then sections
/// starting with `# Detailed Table Information`, `# Partition Information`,
/// `# Constraints`, etc. We stop at the first marker so the column
/// vector contains exactly the table's real columns.
async fn describe_columns(
    executor: &DatabricksSqlExecutor,
    catalog: &str,
    schema: &str,
    table: &str,
) -> Result<Vec<ColumnMetadata>, String> {
    let table = validate_identifier(table)?;
    let sql = format!("DESCRIBE TABLE EXTENDED `{catalog}`.`{schema}`.`{table}`");
    let rows = executor
        .execute_json(&sql, None)
        .await
        .map_err(|e| format!("DESCRIBE TABLE EXTENDED failed for {table}: {e}"))?;

    let mut columns = Vec::new();
    for row in rows {
        let col_name = row.get("col_name").and_then(Value::as_str).unwrap_or("");
        // Empty `col_name` or a `#`-prefixed marker signals the start
        // of the metadata block (Partition Information / Detailed
        // Table Information / Constraints, etc.). Stop here so we
        // don't try to parse those as columns.
        if col_name.is_empty() || col_name.starts_with('#') {
            break;
        }
        let data_type = row
            .get("data_type")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        columns.push(ColumnMetadata {
            name: col_name.to_string(),
            data_type,
            // Databricks `DESCRIBE` doesn't surface PK / sort-key
            // metadata. Leaving these false makes `generate_suggestions`
            // fall back to name-heuristic detection (_id / _key
            // columns) which works the same on both backends.
            is_primary_key: false,
            is_in_order_by: false,
        });
    }
    Ok(columns)
}

/// Fetch up to three sample rows for context. Returns each row as a
/// JSON object keyed by column name — identical shape to what
/// [`SchemaDiscovery::get_sample_data`] returns from ClickHouse, so
/// downstream consumers don't branch on backend.
async fn sample_rows(
    executor: &DatabricksSqlExecutor,
    catalog: &str,
    schema: &str,
    table: &str,
) -> Result<Vec<Value>, String> {
    let table = validate_identifier(table)?;
    let sql = format!("SELECT * FROM `{catalog}`.`{schema}`.`{table}` LIMIT 3");
    executor
        .execute_json(&sql, None)
        .await
        .map_err(|e| format!("sample SELECT failed for {table}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_identifier_accepts_ascii_alnum_underscore() {
        assert!(validate_identifier("users").is_ok());
        assert!(validate_identifier("my_schema_2").is_ok());
        assert!(validate_identifier("CamelCase").is_ok());
        assert!(validate_identifier("_underscore_start").is_ok());
    }

    #[test]
    fn validate_identifier_rejects_unsafe_input() {
        // The whole point of the validator: anything that could break
        // out of the backticked SQL identifier must fail loudly.
        assert!(validate_identifier("").is_err());
        assert!(validate_identifier("foo bar").is_err());
        assert!(validate_identifier("foo`bar").is_err());
        assert!(validate_identifier("foo;DROP TABLE x").is_err());
        assert!(validate_identifier("a.b").is_err()); // qualified names pass each part separately
    }

    /// End-to-end probe over wiremock — covers the SHOW TABLES →
    /// DESCRIBE TABLE EXTENDED → SELECT LIMIT 3 sequence and asserts
    /// the response shape downstream consumers expect.
    #[tokio::test(flavor = "multi_thread")]
    async fn introspect_against_wiremock_returns_expected_shape() {
        use crate::executor::databricks_sql::{DatabricksConfig, DatabricksSqlExecutor};
        use serde_json::json;
        use wiremock::matchers::{body_string_contains, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;

        // Mount three matchers, each scoped to the SQL substring of the
        // statement we expect. Order doesn't matter — wiremock dispatches
        // on whichever matcher fits the incoming request.

        // 1. SHOW TABLES IN `main`.`graphs`
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .and(body_string_contains("SHOW TABLES IN"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "statement_id": "stmt-show",
                "status": { "state": "SUCCEEDED" },
                "manifest": { "schema": { "columns": [
                    { "name": "database" },
                    { "name": "tableName" },
                    { "name": "isTemporary" }
                ]}},
                "result": { "data_array": [
                    ["graphs", "users", false],
                    ["graphs", "follows", false],
                    ["graphs", "_temp_scratch", true]
                ]}
            })))
            .mount(&server)
            .await;

        // 2. DESCRIBE TABLE EXTENDED for `users` — three real columns,
        // then the metadata block we should stop parsing at.
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .and(body_string_contains(
                "DESCRIBE TABLE EXTENDED `main`.`graphs`.`users`",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "statement_id": "stmt-desc-users",
                "status": { "state": "SUCCEEDED" },
                "manifest": { "schema": { "columns": [
                    { "name": "col_name" },
                    { "name": "data_type" },
                    { "name": "comment" }
                ]}},
                "result": { "data_array": [
                    ["user_id", "bigint", null],
                    ["full_name", "string", null],
                    ["created_at", "timestamp", null],
                    ["", "", ""],
                    ["# Detailed Table Information", "", ""],
                    ["Catalog", "main", ""],
                    ["Database", "graphs", ""]
                ]}
            })))
            .mount(&server)
            .await;

        // 3. DESCRIBE for `follows` — edge-shaped table to exercise
        // `generate_suggestions`' edge_candidate path.
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .and(body_string_contains(
                "DESCRIBE TABLE EXTENDED `main`.`graphs`.`follows`",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "statement_id": "stmt-desc-follows",
                "status": { "state": "SUCCEEDED" },
                "manifest": { "schema": { "columns": [
                    { "name": "col_name" },
                    { "name": "data_type" },
                    { "name": "comment" }
                ]}},
                "result": { "data_array": [
                    ["follower_id", "bigint", null],
                    ["followed_id", "bigint", null]
                ]}
            })))
            .mount(&server)
            .await;

        // 4. SELECT * FROM ... LIMIT 3 — single matcher serves both
        // sample queries since we don't care about the per-table data
        // for the response-shape assertion.
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .and(body_string_contains("SELECT * FROM"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "statement_id": "stmt-sample",
                "status": { "state": "SUCCEEDED" },
                "manifest": { "schema": { "columns": [
                    { "name": "user_id" },
                    { "name": "full_name" }
                ]}},
                "result": { "data_array": [
                    [1, "alice"]
                ]}
            })))
            .mount(&server)
            .await;

        let mut config =
            DatabricksConfig::new("ignored.cloud.databricks.com", "wh-probe", "dapi-probe");
        config.base_url = Some(server.uri());
        let executor = DatabricksSqlExecutor::new(config).expect("executor");

        let resp = DatabricksProbe::introspect(&executor, "main", "graphs")
            .await
            .expect("introspect");

        // database is `catalog.schema`, mirroring the way Databricks
        // identifies a namespace in three-tier nomenclature.
        assert_eq!(resp.database, "main.graphs");

        // The temp table was filtered out; the rest survived and are
        // sorted alphabetically (regression guard for the `tables.sort()`).
        let names: Vec<&str> = resp.tables.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, vec!["follows", "users"]);

        // Columns for `users` stopped at the metadata block — we
        // should see exactly the three real columns, no `# Detailed…`
        // or `Catalog` rows leaking through.
        let users = resp
            .tables
            .iter()
            .find(|t| t.name == "users")
            .expect("users table");
        let user_cols: Vec<&str> = users.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(user_cols, vec!["user_id", "full_name", "created_at"]);
        assert_eq!(users.columns[0].data_type, "bigint");
        assert_eq!(users.row_count, None, "we explicitly skip row_count");

        // Suggestions: follows has two _id columns → edge_candidate.
        // Pin this so a regression in `generate_suggestions` visibility
        // (we bumped it to pub(crate) for this module) shows up here.
        let follows_suggestions: Vec<&str> = resp
            .suggestions
            .iter()
            .filter(|s| s.table == "follows")
            .map(|s| s.suggestion_type.as_str())
            .collect();
        assert!(
            follows_suggestions.contains(&"edge_candidate"),
            "expected edge_candidate for two-id table; got {follows_suggestions:?}"
        );
    }
}
