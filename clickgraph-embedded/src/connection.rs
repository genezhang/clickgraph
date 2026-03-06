//! `Connection` — executes Cypher queries against a `Database`.
//!
//! Analogous to `kuzu::Connection`. Multiple connections can share one `Database`.
//! Each `Connection` holds a reference to the `Database`'s executor and schema.

use std::sync::Arc;

use clickgraph::executor::QueryExecutor;
use clickgraph::graph_catalog::graph_schema::GraphSchema;

use super::database::Database;
use super::error::EmbeddedError;
use super::export::{build_export_sql, ExportOptions};
use super::query_result::QueryResult;
use super::value::Value;

/// A connection to an embedded ClickGraph database.
///
/// # Example
///
/// ```no_run
/// use clickgraph_embedded::{Database, Connection, SystemConfig};
///
/// let db = Database::new("schema.yaml", SystemConfig::default()).unwrap();
/// let conn = Connection::new(&db).unwrap();
///
/// let mut result = conn.query("MATCH (u:User) RETURN u.name LIMIT 10").unwrap();
/// for row in result {
///     println!("{}", row[0]);
/// }
/// ```
pub struct Connection<'db> {
    executor: Arc<dyn QueryExecutor>,
    schema: Arc<GraphSchema>,
    db: &'db Database,
}

impl<'db> Connection<'db> {
    /// Create a new connection to `db`.
    pub fn new(db: &'db Database) -> Result<Self, EmbeddedError> {
        Ok(Connection {
            executor: Arc::clone(&db.executor),
            schema: Arc::clone(&db.schema),
            db,
        })
    }

    /// Execute a Cypher query and return an iterator over the result rows.
    ///
    /// This is synchronous — it blocks until the query completes.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use clickgraph_embedded::{Database, Connection, SystemConfig};
    /// # let db = Database::new("schema.yaml", SystemConfig::default()).unwrap();
    /// # let conn = Connection::new(&db).unwrap();
    /// let mut result = conn.query("MATCH (u:User) RETURN u.name").unwrap();
    /// while let Some(row) = result.next() {
    ///     println!("{}", row[0]);
    /// }
    /// ```
    pub fn query(&self, cypher: &str) -> Result<QueryResult, EmbeddedError> {
        self.db.runtime.block_on(self.query_async(cypher))
    }

    /// Execute a Cypher query and return the generated SQL without executing it.
    ///
    /// Useful for debugging and understanding what SQL ClickGraph generates.
    pub fn query_to_sql(&self, cypher: &str) -> Result<String, EmbeddedError> {
        use clickgraph::clickhouse_query_generator::cypher_to_sql;
        use clickgraph::server::query_context::{
            set_current_schema, with_query_context, QueryContext,
        };

        let schema = Arc::clone(&self.schema);
        let cypher = cypher.to_string();

        self.db.runtime.block_on(async move {
            let context = QueryContext::new(None);
            with_query_context(context, async move {
                set_current_schema(Arc::clone(&schema));
                cypher_to_sql(&cypher, &schema, 100).map_err(EmbeddedError::Query)
            })
            .await
        })
    }

    /// Export Cypher query results to a file.
    ///
    /// Translates the Cypher query to SQL, wraps it in
    /// `INSERT INTO FUNCTION file(...)`, and executes via chdb.
    /// The file is written directly by chdb — results are streamed to disk
    /// without buffering the full result set in memory.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use clickgraph_embedded::{Database, Connection, SystemConfig, ExportOptions};
    /// # let db = Database::new("schema.yaml", SystemConfig::default()).unwrap();
    /// # let conn = Connection::new(&db).unwrap();
    /// // Auto-detect format from extension
    /// conn.export("MATCH (u:User) RETURN u.name", "users.parquet", ExportOptions::default()).unwrap();
    ///
    /// // CSV with explicit options
    /// conn.export("MATCH (u:User) RETURN u.name", "users.csv", ExportOptions::default()).unwrap();
    /// ```
    pub fn export(
        &self,
        cypher: &str,
        output_path: &str,
        options: ExportOptions,
    ) -> Result<(), EmbeddedError> {
        self.db
            .runtime
            .block_on(self.export_async(cypher, output_path, options))
    }

    /// Generate the export SQL without executing it (for debugging).
    pub fn export_to_sql(
        &self,
        cypher: &str,
        output_path: &str,
        options: ExportOptions,
    ) -> Result<String, EmbeddedError> {
        let select_sql = self.query_to_sql(cypher)?;
        build_export_sql(&select_sql, output_path, &options).map_err(EmbeddedError::Query)
    }

    async fn export_async(
        &self,
        cypher: &str,
        output_path: &str,
        options: ExportOptions,
    ) -> Result<(), EmbeddedError> {
        use clickgraph::clickhouse_query_generator::cypher_to_sql;
        use clickgraph::server::query_context::{
            set_current_schema, with_query_context, QueryContext,
        };

        let schema = Arc::clone(&self.schema);
        let executor = Arc::clone(&self.executor);
        let cypher = cypher.to_string();
        let output_path = output_path.to_string();

        with_query_context(QueryContext::new(None), async move {
            set_current_schema(Arc::clone(&schema));

            let select_sql = cypher_to_sql(&cypher, &schema, 100).map_err(EmbeddedError::Query)?;
            let export_sql = build_export_sql(&select_sql, &output_path, &options)
                .map_err(EmbeddedError::Query)?;

            // Execute the INSERT INTO FUNCTION file(...) — no result rows expected
            executor
                .execute_text(&export_sql, "TabSeparated", None)
                .await
                .map_err(EmbeddedError::from)?;

            Ok(())
        })
        .await
    }

    async fn query_async(&self, cypher: &str) -> Result<QueryResult, EmbeddedError> {
        use clickgraph::clickhouse_query_generator::cypher_to_sql;
        use clickgraph::server::query_context::{
            set_current_schema, with_query_context, QueryContext,
        };

        let schema = Arc::clone(&self.schema);
        let executor = Arc::clone(&self.executor);
        let cypher = cypher.to_string();

        with_query_context(QueryContext::new(None), async move {
            set_current_schema(Arc::clone(&schema));

            let final_sql = cypher_to_sql(&cypher, &schema, 100).map_err(EmbeddedError::Query)?;

            let json_rows = executor
                .execute_json(&final_sql, None)
                .await
                .map_err(EmbeddedError::from)?;

            // Build column names from the first row (preserve insertion order via serde_json)
            let mut column_names: Vec<String> = Vec::new();
            let mut rows: Vec<Vec<Value>> = Vec::new();

            for json_row in json_rows {
                if let serde_json::Value::Object(obj) = json_row {
                    if column_names.is_empty() {
                        column_names = obj.keys().cloned().collect();
                    }
                    let row_vals: Vec<Value> = column_names
                        .iter()
                        .map(|col| {
                            obj.get(col)
                                .cloned()
                                .map(Value::from)
                                .unwrap_or(Value::Null)
                        })
                        .collect();
                    rows.push(row_vals);
                }
            }

            Ok(QueryResult::new(column_names, rows))
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clickgraph::graph_catalog::config::GraphSchemaConfig;

    fn build_test_schema() -> Arc<GraphSchema> {
        let yaml = r#"
name: test
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
        let config: GraphSchemaConfig = serde_yaml::from_str(yaml).expect("valid yaml");
        Arc::new(config.to_graph_schema().expect("valid schema"))
    }

    fn make_stub_db() -> Database {
        use async_trait::async_trait;
        use clickgraph::executor::ExecutorError;
        use clickgraph::executor::QueryExecutor;

        struct StubExecutor;

        #[async_trait]
        impl QueryExecutor for StubExecutor {
            async fn execute_json(
                &self,
                _sql: &str,
                _role: Option<&str>,
            ) -> Result<Vec<serde_json::Value>, ExecutorError> {
                Ok(vec![])
            }

            async fn execute_text(
                &self,
                _sql: &str,
                _format: &str,
                _role: Option<&str>,
            ) -> Result<String, ExecutorError> {
                Ok(String::new())
            }
        }

        Database {
            executor: Arc::new(StubExecutor),
            schema: build_test_schema(),
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        }
    }

    #[test]
    fn test_query_to_sql_basic_match() {
        let db = make_stub_db();
        let conn = Connection::new(&db).unwrap();
        let sql = conn
            .query_to_sql("MATCH (u:User) RETURN u.name")
            .expect("should generate SQL");
        assert!(sql.contains("users"), "SQL should reference users table");
        assert!(
            sql.contains("full_name"),
            "property mapping should resolve name -> full_name"
        );
    }

    #[test]
    fn test_query_to_sql_relationship() {
        let db = make_stub_db();
        let conn = Connection::new(&db).unwrap();
        let sql = conn
            .query_to_sql("MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.name, b.name")
            .expect("should generate SQL");
        assert!(
            sql.contains("follows"),
            "SQL should reference follows table"
        );
        assert!(sql.contains("full_name"), "property mapping should apply");
    }

    #[test]
    fn test_query_to_sql_parse_error() {
        let db = make_stub_db();
        let conn = Connection::new(&db).unwrap();
        let result = conn.query_to_sql("NOT VALID CYPHER @@@@");
        assert!(result.is_err(), "invalid Cypher should return error");
    }

    #[test]
    fn test_export_to_sql_parquet() {
        use crate::export::ExportOptions;
        let db = make_stub_db();
        let conn = Connection::new(&db).unwrap();
        let sql = conn
            .export_to_sql(
                "MATCH (u:User) RETURN u.name",
                "output.parquet",
                ExportOptions::default(),
            )
            .expect("should generate export SQL");
        assert!(
            sql.starts_with("INSERT INTO FUNCTION file('output.parquet', 'Parquet')"),
            "should wrap in INSERT INTO FUNCTION file: {}",
            sql
        );
        assert!(sql.contains("full_name"), "property mapping should apply");
    }

    #[test]
    fn test_export_to_sql_csv() {
        use crate::export::ExportOptions;
        let db = make_stub_db();
        let conn = Connection::new(&db).unwrap();
        let sql = conn
            .export_to_sql(
                "MATCH (u:User) RETURN u.name",
                "results.csv",
                ExportOptions::default(),
            )
            .expect("should generate export SQL");
        assert!(
            sql.contains("CSVWithNames"),
            "CSV should include header: {}",
            sql
        );
    }

    #[test]
    fn test_export_to_sql_explicit_format() {
        use crate::export::{ExportFormat, ExportOptions};
        let db = make_stub_db();
        let conn = Connection::new(&db).unwrap();
        let opts = ExportOptions {
            format: Some(ExportFormat::JSONEachRow),
            ..Default::default()
        };
        let sql = conn
            .export_to_sql("MATCH (u:User) RETURN u.name", "data.txt", opts)
            .expect("should generate export SQL");
        assert!(
            sql.contains("JSONEachRow"),
            "explicit format should apply: {}",
            sql
        );
    }

    #[test]
    fn test_export_to_sql_unknown_extension() {
        use crate::export::ExportOptions;
        let db = make_stub_db();
        let conn = Connection::new(&db).unwrap();
        let result = conn.export_to_sql(
            "MATCH (u:User) RETURN u.name",
            "output.xyz",
            ExportOptions::default(),
        );
        assert!(
            result.is_err(),
            "unknown extension without format should error"
        );
    }
}
