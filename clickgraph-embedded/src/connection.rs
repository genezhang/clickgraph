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

    /// Handle `CALL apoc.export.{csv|json|parquet}.query(...)` in embedded mode.
    ///
    /// Parses arguments, translates inner Cypher → SQL, builds export SQL, executes.
    /// Returns a single-row result with export status.
    async fn handle_export_call(&self, cypher: &str) -> Result<QueryResult, EmbeddedError> {
        use clickgraph::clickhouse_query_generator::cypher_to_sql;
        use clickgraph::open_cypher_parser;
        use clickgraph::open_cypher_parser::ast::CypherStatement;
        use clickgraph::procedures::apoc_export;
        use clickgraph::server::query_context::{
            set_current_schema, with_query_context, QueryContext,
        };

        let schema = Arc::clone(&self.schema);
        let executor = Arc::clone(&self.executor);
        let cypher = cypher.to_string();

        with_query_context(QueryContext::new(None), async move {
            set_current_schema(Arc::clone(&schema));

            // Parse the CALL statement
            let (_, stmt) = open_cypher_parser::parse_cypher_statement(&cypher)
                .map_err(|e| EmbeddedError::Query(format!("Parse error: {}", e)))?;

            // Extract procedure name and arguments
            let (proc_name, expressions): (String, Vec<_>) = match &stmt {
                CypherStatement::ProcedureCall(pc) => {
                    (pc.procedure_name.to_string(), pc.arguments.iter().collect())
                }
                CypherStatement::Query { query, .. } => {
                    let cc = query
                        .call_clause
                        .as_ref()
                        .ok_or_else(|| EmbeddedError::Query("No CALL clause found".to_string()))?;
                    (
                        cc.procedure_name.to_string(),
                        cc.arguments.iter().map(|a| &a.value).collect(),
                    )
                }
                CypherStatement::CopyTo(_) => {
                    return Err(EmbeddedError::Query(
                        "COPY TO should be handled before reaching APOC export path".to_string(),
                    ));
                }
            };

            let ch_format = apoc_export::format_from_procedure_name(&proc_name)
                .map_err(EmbeddedError::Query)?;

            let args =
                apoc_export::parse_export_call(&expressions).map_err(EmbeddedError::Query)?;

            // Translate inner Cypher → SQL
            let inner_sql =
                cypher_to_sql(&args.cypher_query, &schema, 100).map_err(EmbeddedError::Query)?;

            // Build export SQL using the full destination resolver
            let export_sql = apoc_export::build_export_sql(
                &inner_sql,
                &args.destination,
                ch_format,
                &args.config,
            )
            .map_err(EmbeddedError::Query)?;

            // Execute
            executor
                .execute_text(&export_sql, "TabSeparated", None)
                .await
                .map_err(EmbeddedError::from)?;

            // Return status as a single-row result
            let columns = vec![
                "file".to_string(),
                "format".to_string(),
                "source".to_string(),
            ];
            let rows = vec![vec![
                Value::String(args.destination),
                Value::String(ch_format.to_string()),
                Value::String(args.cypher_query),
            ]];
            Ok(QueryResult::new(columns, rows))
        })
        .await
    }

    /// Handle `COPY (<cypher>) TO '<destination>' [FORMAT <fmt>] [(options)]` in embedded mode.
    async fn handle_copy_to(
        &self,
        inner_cypher: &str,
        destination: &str,
        format: Option<&str>,
        options: &[(&str, clickgraph::open_cypher_parser::ast::Expression<'_>)],
    ) -> Result<QueryResult, EmbeddedError> {
        use clickgraph::clickhouse_query_generator::cypher_to_sql;
        use clickgraph::procedures::apoc_export;
        use clickgraph::server::query_context::{
            set_current_schema, with_query_context, QueryContext,
        };

        let ch_format = if let Some(fmt) = format {
            apoc_export::format_from_copy_format(fmt).map_err(EmbeddedError::Query)?
        } else {
            apoc_export::format_from_extension(destination).ok_or_else(|| {
                EmbeddedError::Query(format!(
                    "Cannot determine format from '{}'. Use FORMAT clause.",
                    destination
                ))
            })?
        };

        let config = apoc_export::ExportConfig::from_copy_options(options);

        let schema = Arc::clone(&self.schema);
        let executor = Arc::clone(&self.executor);
        let inner_cypher = inner_cypher.to_string();
        let destination = destination.to_string();

        with_query_context(QueryContext::new(None), async move {
            set_current_schema(Arc::clone(&schema));

            let inner_sql =
                cypher_to_sql(&inner_cypher, &schema, 100).map_err(EmbeddedError::Query)?;

            let export_sql =
                apoc_export::build_export_sql(&inner_sql, &destination, ch_format, &config)
                    .map_err(EmbeddedError::Query)?;

            executor
                .execute_text(&export_sql, "TabSeparated", None)
                .await
                .map_err(EmbeddedError::from)?;

            let columns = vec![
                "file".to_string(),
                "format".to_string(),
                "source".to_string(),
            ];
            let rows = vec![vec![
                Value::String(destination),
                Value::String(ch_format.to_string()),
                Value::String(inner_cypher),
            ]];
            Ok(QueryResult::new(columns, rows))
        })
        .await
    }

    /// Handle `CALL db.index.vector.queryNodes(...)` in embedded mode.
    async fn handle_vector_search_call(&self, cypher: &str) -> Result<QueryResult, EmbeddedError> {
        use clickgraph::open_cypher_parser;
        use clickgraph::open_cypher_parser::ast::CypherStatement;
        use clickgraph::procedures::vector_search;

        let schema = Arc::clone(&self.schema);
        let executor = Arc::clone(&self.executor);
        let cypher = cypher.to_string();

        // Parse the CALL statement
        let (_, stmt) = open_cypher_parser::parse_cypher_statement(&cypher)
            .map_err(|e| EmbeddedError::Query(format!("Parse error: {}", e)))?;

        // Extract arguments
        let expressions: Vec<_> = match &stmt {
            CypherStatement::ProcedureCall(pc) => pc.arguments.iter().collect(),
            CypherStatement::Query { query, .. } => {
                let cc = query
                    .call_clause
                    .as_ref()
                    .ok_or_else(|| EmbeddedError::Query("No CALL clause found".to_string()))?;
                cc.arguments.iter().map(|a| &a.value).collect()
            }
            CypherStatement::CopyTo(_) => {
                return Err(EmbeddedError::Query(
                    "Unexpected COPY TO in vector search context".to_string(),
                ));
            }
        };

        let search_args =
            vector_search::parse_vector_search_args(&expressions).map_err(EmbeddedError::Query)?;

        let index_config = vector_search::resolve_vector_index(&schema, &search_args.index_name)
            .map_err(EmbeddedError::Query)?;

        let search_sql = vector_search::build_vector_search_sql(&search_args, index_config)
            .map_err(EmbeddedError::Query)?;

        // Execute and parse results
        let json_rows = executor
            .execute_json(&search_sql, None)
            .await
            .map_err(EmbeddedError::from)?;

        // Derive columns from the first row, or use defaults for empty results.
        // The SQL is `SELECT *, <score_expr> AS score` so columns come from the table
        // schema plus `score`. We derive from the first row to stay schema-agnostic.
        let columns: Vec<String> = if let Some(first_row) = json_rows.first() {
            if let serde_json::Value::Object(map) = first_row {
                map.keys().cloned().collect()
            } else {
                vec!["result".to_string()]
            }
        } else {
            // Empty results — return minimal columns consistent with YIELD node, score
            return Ok(QueryResult::new(
                vec!["node".to_string(), "score".to_string()],
                vec![],
            ));
        };

        let rows: Vec<Vec<Value>> = json_rows
            .into_iter()
            .map(|row| {
                if let serde_json::Value::Object(map) = row {
                    columns
                        .iter()
                        .map(|col| {
                            Value::from(map.get(col).cloned().unwrap_or(serde_json::Value::Null))
                        })
                        .collect()
                } else {
                    vec![Value::from(row)]
                }
            })
            .collect();

        Ok(QueryResult::new(columns, rows))
    }

    async fn query_async(&self, cypher: &str) -> Result<QueryResult, EmbeddedError> {
        // Intercept CALL apoc.export.* and COPY TO — parse first to avoid false positives
        if let Ok((_, stmt)) = clickgraph::open_cypher_parser::parse_cypher_statement(cypher) {
            // COPY TO intercept
            if let clickgraph::open_cypher_parser::ast::CypherStatement::CopyTo(ref copy_stmt) =
                stmt
            {
                return self
                    .handle_copy_to(
                        copy_stmt.query,
                        copy_stmt.destination,
                        copy_stmt.format,
                        &copy_stmt.options,
                    )
                    .await;
            }

            // APOC export intercept
            let proc_name = match &stmt {
                clickgraph::open_cypher_parser::ast::CypherStatement::ProcedureCall(pc) => {
                    Some(pc.procedure_name.to_string())
                }
                clickgraph::open_cypher_parser::ast::CypherStatement::Query { query, .. } => query
                    .call_clause
                    .as_ref()
                    .map(|cc| cc.procedure_name.to_string()),
                clickgraph::open_cypher_parser::ast::CypherStatement::CopyTo(_) => None,
            };
            if let Some(name) = proc_name {
                if clickgraph::procedures::apoc_export::is_export_procedure(&name) {
                    return self.handle_export_call(cypher).await;
                }
                if clickgraph::procedures::vector_search::is_vector_search_procedure(&name) {
                    return self.handle_vector_search_call(cypher).await;
                }
            }
        }

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

    #[test]
    fn test_call_export_via_query() {
        // Verify that CALL apoc.export.*.query() is intercepted and routed
        // to the export handler (returns a status result, not SQL error)
        let db = make_stub_db();
        let conn = Connection::new(&db).unwrap();
        let result = conn.query(
            r#"CALL apoc.export.parquet.query("MATCH (u:User) RETURN u.name", "/tmp/users.parquet", {})"#,
        );
        // With stub executor, this should succeed (StubExecutor returns empty string)
        assert!(
            result.is_ok(),
            "CALL export should be handled: {:?}",
            result.err()
        );
        let qr = result.unwrap();
        assert_eq!(qr.get_column_names(), &["file", "format", "source"]);
    }

    #[test]
    fn test_call_export_csv_via_query() {
        let db = make_stub_db();
        let conn = Connection::new(&db).unwrap();
        let result = conn.query(
            r#"CALL apoc.export.csv.query("MATCH (u:User) RETURN u.name", "/tmp/users.csv", {})"#,
        );
        assert!(result.is_ok(), "CSV export should work: {:?}", result.err());
    }

    #[test]
    fn test_call_export_s3_destination() {
        let db = make_stub_db();
        let conn = Connection::new(&db).unwrap();
        let result = conn.query(
            r#"CALL apoc.export.json.query("MATCH (u:User) RETURN u.name", "s3://mybucket/users.json", {})"#,
        );
        assert!(result.is_ok(), "S3 export should work: {:?}", result.err());
    }
}
