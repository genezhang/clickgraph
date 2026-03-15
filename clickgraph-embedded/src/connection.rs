//! `Connection` — executes Cypher queries against a `Database`.
//!
//! Analogous to `kuzu::Connection`. Multiple connections can share one `Database`.
//! Each `Connection` holds a reference to the `Database`'s executor and schema.

use std::collections::HashMap;
use std::sync::Arc;

use clickgraph::executor::QueryExecutor;
use clickgraph::graph_catalog::graph_schema::GraphSchema;

use super::database::Database;
use super::error::EmbeddedError;
use super::export::{build_export_sql, ExportOptions};
use super::query_result::QueryResult;
use super::value::Value;
use super::write_helpers;

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

    /// Execute a raw SQL statement (DDL, DML, or administrative command).
    ///
    /// No Cypher parsing or schema validation; the caller is responsible for
    /// SQL correctness. Delegates to the executor's `execute_text` method.
    pub fn execute_sql(&self, sql: &str) -> Result<(), EmbeddedError> {
        self.db.runtime.block_on(async {
            self.executor
                .execute_text(sql, "TabSeparated", None)
                .await
                .map_err(EmbeddedError::from)?;
            Ok(())
        })
    }

    /// Create a node with the given label and properties.
    ///
    /// Returns the node ID (caller-provided or auto-generated UUID).
    /// Properties use Cypher names and are mapped to ClickHouse columns via schema.
    pub fn create_node(
        &self,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<String, EmbeddedError> {
        let node_schema = self.get_node_schema(label)?;
        let id_columns = node_schema.node_id.id.columns();
        let id_col_strs: Vec<&str> = id_columns.iter().copied().collect();
        let property_mappings =
            write_helpers::extract_property_mappings(&node_schema.property_mappings);

        write_helpers::validate_properties(&properties, &property_mappings, &id_col_strs)?;

        // Resolve node ID: use caller-provided value or generate UUID client-side.
        // Client-side UUID ensures the caller can use the returned ID for edge creation.
        let id_key = id_columns.first().copied().unwrap_or("id");
        let node_id = if let Some(v) = properties.get(id_key) {
            match v {
                Value::String(s) => s.clone(),
                other => other
                    .to_sql_literal()
                    .map_err(EmbeddedError::Validation)?
                    .trim_matches('\'')
                    .to_string(),
            }
        } else {
            uuid::Uuid::new_v4().to_string()
        };

        let mut columns = vec![id_key.to_string()];
        let mut values = vec![Value::String(node_id.clone())
            .to_sql_literal()
            .map_err(EmbeddedError::Validation)?];

        // Map properties to columns (excluding the ID column which we handle above)
        for (cypher_name, value) in &properties {
            if cypher_name == id_key {
                continue;
            }
            let col_name = property_mappings
                .get(cypher_name.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| cypher_name.clone());
            columns.push(col_name);
            values.push(value.to_sql_literal().map_err(EmbeddedError::Validation)?);
        }

        let sql = write_helpers::build_insert_sql(
            &node_schema.database,
            &node_schema.table_name,
            &columns,
            &[values],
        );
        self.execute_sql(&sql)?;

        Ok(node_id)
    }

    /// Create an edge between two nodes.
    ///
    /// Properties use Cypher names and are mapped to ClickHouse columns via schema.
    pub fn create_edge(
        &self,
        edge_type: &str,
        from_id: &str,
        to_id: &str,
        properties: HashMap<String, Value>,
    ) -> Result<(), EmbeddedError> {
        let rel_schema = self.get_rel_schema(edge_type)?;
        let from_id_cols = rel_schema.from_id.columns();
        let to_id_cols = rel_schema.to_id.columns();
        let mut id_col_strs: Vec<&str> = Vec::new();
        id_col_strs.extend(from_id_cols.iter().copied());
        id_col_strs.extend(to_id_cols.iter().copied());
        let property_mappings =
            write_helpers::extract_property_mappings(&rel_schema.property_mappings);

        write_helpers::validate_properties(&properties, &property_mappings, &id_col_strs)?;

        let mut columns = Vec::new();
        let mut values = Vec::new();

        // Add from_id and to_id
        let from_col = from_id_cols.first().copied().unwrap_or("from_id");
        let to_col = to_id_cols.first().copied().unwrap_or("to_id");
        columns.push(from_col.to_string());
        values.push(
            Value::String(from_id.to_string())
                .to_sql_literal()
                .map_err(EmbeddedError::Validation)?,
        );
        columns.push(to_col.to_string());
        values.push(
            Value::String(to_id.to_string())
                .to_sql_literal()
                .map_err(EmbeddedError::Validation)?,
        );

        // Map properties to columns
        for (cypher_name, value) in &properties {
            let col_name = property_mappings
                .get(cypher_name.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| cypher_name.clone());
            columns.push(col_name);
            values.push(value.to_sql_literal().map_err(EmbeddedError::Validation)?);
        }

        let sql = write_helpers::build_insert_sql(
            &rel_schema.database,
            &rel_schema.table_name,
            &columns,
            &[values],
        );
        self.execute_sql(&sql)
    }

    /// Upsert a node (INSERT with ReplacingMergeTree deduplication).
    ///
    /// The node_id property MUST be present in the properties map.
    pub fn upsert_node(
        &self,
        label: &str,
        properties: HashMap<String, Value>,
    ) -> Result<String, EmbeddedError> {
        let node_schema = self.get_node_schema(label)?;
        let id_columns = node_schema.node_id.id.columns();
        let id_key = id_columns.first().copied().unwrap_or("id");

        if !properties.contains_key(id_key) {
            return Err(EmbeddedError::Validation(format!(
                "Missing required node_id property '{}' for upsert",
                id_key
            )));
        }

        self.create_node(label, properties)
    }

    /// Upsert an edge (INSERT with ReplacingMergeTree deduplication).
    ///
    /// Same INSERT semantics as `create_edge`; ReplacingMergeTree handles
    /// deduplication by the ORDER BY key.
    pub fn upsert_edge(
        &self,
        edge_type: &str,
        from_id: &str,
        to_id: &str,
        properties: HashMap<String, Value>,
    ) -> Result<(), EmbeddedError> {
        self.create_edge(edge_type, from_id, to_id, properties)
    }

    /// Create multiple nodes in a single batch INSERT.
    ///
    /// Returns a Vec of node IDs (caller-provided or placeholder for auto-generated).
    pub fn create_nodes(
        &self,
        label: &str,
        batch: Vec<HashMap<String, Value>>,
    ) -> Result<Vec<String>, EmbeddedError> {
        if batch.is_empty() {
            return Ok(vec![]);
        }

        let node_schema = self.get_node_schema(label)?;
        let id_columns = node_schema.node_id.id.columns();
        let id_col_strs: Vec<&str> = id_columns.iter().copied().collect();
        let id_key = id_columns.first().copied().unwrap_or("id");
        let property_mappings =
            write_helpers::extract_property_mappings(&node_schema.property_mappings);

        // Validate all rows first
        for row_props in &batch {
            write_helpers::validate_properties(row_props, &property_mappings, &id_col_strs)?;
        }

        // Collect all unique column names across all rows (deterministic order)
        let mut all_columns: Vec<String> = Vec::new();
        let mut seen_columns = std::collections::HashSet::new();
        for row_props in &batch {
            if row_props.contains_key(id_key) && !seen_columns.contains(id_key) {
                all_columns.push(id_key.to_string());
                seen_columns.insert(id_key.to_string());
            }
            for cypher_name in row_props.keys() {
                if cypher_name == id_key {
                    continue;
                }
                let col_name = property_mappings
                    .get(cypher_name.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| cypher_name.clone());
                if !seen_columns.contains(&col_name) {
                    all_columns.push(col_name.clone());
                    seen_columns.insert(col_name);
                }
            }
        }

        // Build reverse mapping: CH column -> Cypher name for lookup
        let mut reverse_map: HashMap<String, String> = HashMap::new();
        for (cypher_name, ch_col) in &property_mappings {
            reverse_map.insert(ch_col.to_string(), cypher_name.to_string());
        }

        let mut all_values_rows = Vec::new();
        let mut ids = Vec::new();
        for row_props in &batch {
            // Resolve ID: caller-provided or client-side UUID
            let node_id = if let Some(v) = row_props.get(id_key) {
                match v {
                    Value::String(s) => s.clone(),
                    other => other
                        .to_sql_literal()
                        .map_err(EmbeddedError::Validation)?
                        .trim_matches('\'')
                        .to_string(),
                }
            } else {
                uuid::Uuid::new_v4().to_string()
            };
            ids.push(node_id.clone());

            let mut row_values = Vec::new();
            for col in &all_columns {
                if col == id_key {
                    row_values.push(
                        Value::String(node_id.clone())
                            .to_sql_literal()
                            .map_err(EmbeddedError::Validation)?,
                    );
                } else {
                    let cypher_name = reverse_map.get(col).unwrap_or(col);
                    if let Some(val) = row_props.get(cypher_name) {
                        row_values.push(val.to_sql_literal().map_err(EmbeddedError::Validation)?);
                    } else {
                        row_values.push("DEFAULT".to_string());
                    }
                }
            }
            all_values_rows.push(row_values);
        }

        let sql = write_helpers::build_insert_sql(
            &node_schema.database,
            &node_schema.table_name,
            &all_columns,
            &all_values_rows,
        );
        self.execute_sql(&sql)?;
        Ok(ids)
    }

    /// Create multiple edges in a single batch INSERT.
    pub fn create_edges(
        &self,
        edge_type: &str,
        batch: Vec<(String, String, HashMap<String, Value>)>,
    ) -> Result<(), EmbeddedError> {
        if batch.is_empty() {
            return Ok(());
        }

        let rel_schema = self.get_rel_schema(edge_type)?;
        let from_id_cols = rel_schema.from_id.columns();
        let to_id_cols = rel_schema.to_id.columns();
        let mut id_col_strs: Vec<&str> = Vec::new();
        id_col_strs.extend(from_id_cols.iter().copied());
        id_col_strs.extend(to_id_cols.iter().copied());
        let from_col = from_id_cols.first().copied().unwrap_or("from_id");
        let to_col = to_id_cols.first().copied().unwrap_or("to_id");
        let property_mappings =
            write_helpers::extract_property_mappings(&rel_schema.property_mappings);

        // Validate all rows first
        for (_, _, row_props) in &batch {
            write_helpers::validate_properties(row_props, &property_mappings, &id_col_strs)?;
        }

        // Collect all unique property columns
        let mut prop_columns: Vec<String> = Vec::new();
        let mut seen_columns = std::collections::HashSet::new();
        for (_, _, row_props) in &batch {
            for cypher_name in row_props.keys() {
                let col_name = property_mappings
                    .get(cypher_name.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| cypher_name.clone());
                if !seen_columns.contains(&col_name) {
                    prop_columns.push(col_name.clone());
                    seen_columns.insert(col_name);
                }
            }
        }

        let mut columns = vec![from_col.to_string(), to_col.to_string()];
        columns.extend(prop_columns.iter().cloned());

        let mut reverse_map: HashMap<String, String> = HashMap::new();
        for (cypher_name, ch_col) in &property_mappings {
            reverse_map.insert(ch_col.to_string(), cypher_name.to_string());
        }

        let mut all_values_rows = Vec::new();
        for (from_id, to_id, row_props) in &batch {
            let mut row_values = vec![
                Value::String(from_id.clone())
                    .to_sql_literal()
                    .map_err(EmbeddedError::Validation)?,
                Value::String(to_id.clone())
                    .to_sql_literal()
                    .map_err(EmbeddedError::Validation)?,
            ];
            for col in &prop_columns {
                let cypher_name = reverse_map.get(col).unwrap_or(col);
                if let Some(val) = row_props.get(cypher_name) {
                    row_values.push(val.to_sql_literal().map_err(EmbeddedError::Validation)?);
                } else {
                    row_values.push("DEFAULT".to_string());
                }
            }
            all_values_rows.push(row_values);
        }

        let sql = write_helpers::build_insert_sql(
            &rel_schema.database,
            &rel_schema.table_name,
            &columns,
            &all_values_rows,
        );
        self.execute_sql(&sql)
    }

    // --- Private schema lookup helpers ---

    fn get_node_schema(
        &self,
        label: &str,
    ) -> Result<&clickgraph::graph_catalog::graph_schema::NodeSchema, EmbeddedError> {
        self.schema.all_node_schemas().get(label).ok_or_else(|| {
            EmbeddedError::Validation(format!(
                "Unknown node label '{}'. Valid labels: {:?}",
                label,
                self.schema.all_node_schemas().keys().collect::<Vec<_>>()
            ))
        })
    }

    fn get_rel_schema(
        &self,
        edge_type: &str,
    ) -> Result<&clickgraph::graph_catalog::graph_schema::RelationshipSchema, EmbeddedError> {
        // Use the schema's get_rel_schema which handles both exact keys
        // (e.g., "KNOWS::Person::Person") and simple type names (e.g., "KNOWS")
        self.schema.get_rel_schema(edge_type).map_err(|_| {
            EmbeddedError::Validation(format!(
                "Unknown relationship type '{}'. Valid types: {:?}",
                edge_type,
                self.schema
                    .get_relationships_schemas()
                    .keys()
                    .collect::<Vec<_>>()
            ))
        })
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

            executor
                .execute_text(&export_sql, "TabSeparated", None)
                .await
                .map_err(EmbeddedError::from)?;

            Ok(())
        })
        .await
    }

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

            let (_, stmt) = open_cypher_parser::parse_cypher_statement(&cypher)
                .map_err(|e| EmbeddedError::Query(format!("Parse error: {}", e)))?;

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

            let inner_sql =
                cypher_to_sql(&args.cypher_query, &schema, 100).map_err(EmbeddedError::Query)?;

            let export_sql = apoc_export::build_export_sql(
                &inner_sql,
                &args.destination,
                ch_format,
                &args.config,
            )
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
                Value::String(args.destination),
                Value::String(ch_format.to_string()),
                Value::String(args.cypher_query),
            ]];
            Ok(QueryResult::new(columns, rows))
        })
        .await
    }

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

    async fn handle_vector_search_call(&self, cypher: &str) -> Result<QueryResult, EmbeddedError> {
        use clickgraph::open_cypher_parser;
        use clickgraph::open_cypher_parser::ast::CypherStatement;
        use clickgraph::procedures::vector_search;

        let schema = Arc::clone(&self.schema);
        let executor = Arc::clone(&self.executor);
        let cypher = cypher.to_string();

        let (_, stmt) = open_cypher_parser::parse_cypher_statement(&cypher)
            .map_err(|e| EmbeddedError::Query(format!("Parse error: {}", e)))?;

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

        let json_rows = executor
            .execute_json(&search_sql, None)
            .await
            .map_err(EmbeddedError::from)?;

        let columns: Vec<String> = if let Some(first_row) = json_rows.first() {
            if let serde_json::Value::Object(map) = first_row {
                map.keys().cloned().collect()
            } else {
                vec!["result".to_string()]
            }
        } else {
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

    async fn handle_fulltext_search_call(
        &self,
        cypher: &str,
    ) -> Result<QueryResult, EmbeddedError> {
        use clickgraph::open_cypher_parser;
        use clickgraph::open_cypher_parser::ast::CypherStatement;
        use clickgraph::procedures::fulltext_search;

        let schema = Arc::clone(&self.schema);
        let executor = Arc::clone(&self.executor);
        let cypher = cypher.to_string();

        let (_, stmt) = open_cypher_parser::parse_cypher_statement(&cypher)
            .map_err(|e| EmbeddedError::Query(format!("Parse error: {}", e)))?;

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
                    "Unexpected COPY TO in fulltext search context".to_string(),
                ));
            }
        };

        let search_args = fulltext_search::parse_fulltext_search_args(&expressions)
            .map_err(EmbeddedError::Query)?;

        let index_config =
            fulltext_search::resolve_fulltext_index(&schema, &search_args.index_name)
                .map_err(EmbeddedError::Query)?;

        let search_sql = fulltext_search::build_fulltext_search_sql(&search_args, index_config);

        let json_rows = executor
            .execute_json(&search_sql, None)
            .await
            .map_err(EmbeddedError::from)?;

        let columns: Vec<String> = if let Some(first_row) = json_rows.first() {
            if let serde_json::Value::Object(map) = first_row {
                map.keys().cloned().collect()
            } else {
                vec!["result".to_string()]
            }
        } else {
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
        if let Ok((_, stmt)) = clickgraph::open_cypher_parser::parse_cypher_statement(cypher) {
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
                if clickgraph::procedures::fulltext_search::is_fulltext_search_procedure(&name) {
                    return self.handle_fulltext_search_call(cypher).await;
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

    fn build_writable_test_schema() -> Arc<GraphSchema> {
        let yaml = r#"
name: test_writable
graph_schema:
  nodes:
    - label: Person
      database: test_db
      table: persons
      node_id: person_id
      property_mappings:
        person_id: person_id
        name: full_name
        age: age
  edges:
    - type: KNOWS
      database: test_db
      table: knows
      from_node: Person
      to_node: Person
      from_id: from_person_id
      to_id: to_person_id
      property_mappings:
        since: since_year
"#;
        let config: GraphSchemaConfig = serde_yaml::from_str(yaml).expect("valid yaml");
        Arc::new(config.to_graph_schema().expect("valid schema"))
    }

    fn make_stub_db() -> Database {
        make_stub_db_with_schema(build_test_schema())
    }

    fn make_stub_db_with_schema(schema: Arc<GraphSchema>) -> Database {
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
            schema,
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        }
    }

    /// Create a stub DB whose executor captures executed SQL for inspection.
    fn make_capturing_db(
        schema: Arc<GraphSchema>,
    ) -> (Database, Arc<std::sync::Mutex<Vec<String>>>) {
        use async_trait::async_trait;
        use clickgraph::executor::ExecutorError;
        use clickgraph::executor::QueryExecutor;

        struct CapturingExecutor {
            captured: Arc<std::sync::Mutex<Vec<String>>>,
        }

        #[async_trait]
        impl QueryExecutor for CapturingExecutor {
            async fn execute_json(
                &self,
                _sql: &str,
                _role: Option<&str>,
            ) -> Result<Vec<serde_json::Value>, ExecutorError> {
                Ok(vec![])
            }

            async fn execute_text(
                &self,
                sql: &str,
                _format: &str,
                _role: Option<&str>,
            ) -> Result<String, ExecutorError> {
                self.captured.lock().unwrap().push(sql.to_string());
                Ok(String::new())
            }
        }

        let captured = Arc::new(std::sync::Mutex::new(Vec::new()));
        let executor = Arc::new(CapturingExecutor {
            captured: Arc::clone(&captured),
        });

        let db = Database {
            executor,
            schema,
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        };

        (db, captured)
    }

    /// Create a stub DB whose executor returns errors.
    fn make_error_db(schema: Arc<GraphSchema>) -> Database {
        use async_trait::async_trait;
        use clickgraph::executor::ExecutorError;
        use clickgraph::executor::QueryExecutor;

        struct ErrorExecutor;

        #[async_trait]
        impl QueryExecutor for ErrorExecutor {
            async fn execute_json(
                &self,
                _sql: &str,
                _role: Option<&str>,
            ) -> Result<Vec<serde_json::Value>, ExecutorError> {
                Err(ExecutorError::QueryFailed("test error".to_string()))
            }

            async fn execute_text(
                &self,
                _sql: &str,
                _format: &str,
                _role: Option<&str>,
            ) -> Result<String, ExecutorError> {
                Err(ExecutorError::QueryFailed("test error".to_string()))
            }
        }

        Database {
            executor: Arc::new(ErrorExecutor),
            schema,
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
        let db = make_stub_db();
        let conn = Connection::new(&db).unwrap();
        let result = conn.query(
            r#"CALL apoc.export.parquet.query("MATCH (u:User) RETURN u.name", "/tmp/users.parquet", {})"#,
        );
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

    // --- execute_sql tests ---

    #[test]
    fn test_execute_sql_ddl_returns_ok() {
        let db = make_stub_db();
        let conn = Connection::new(&db).unwrap();
        let result = conn.execute_sql(
            "CREATE TABLE IF NOT EXISTS test_db.foo (id String) ENGINE = MergeTree() ORDER BY id",
        );
        assert!(result.is_ok(), "DDL should succeed: {:?}", result.err());
    }

    #[test]
    fn test_execute_sql_insert_returns_ok() {
        let db = make_stub_db();
        let conn = Connection::new(&db).unwrap();
        let result = conn.execute_sql("INSERT INTO test_db.foo (id) VALUES ('abc')");
        assert!(result.is_ok(), "INSERT should succeed: {:?}", result.err());
    }

    #[test]
    fn test_execute_sql_propagates_executor_errors() {
        let db = make_error_db(build_test_schema());
        let conn = Connection::new(&db).unwrap();
        let result = conn.execute_sql("SELECT 1");
        assert!(result.is_err(), "should propagate executor error");
        let err = result.unwrap_err();
        assert!(
            matches!(err, EmbeddedError::Executor(_)),
            "should be Executor error: {:?}",
            err
        );
    }

    // --- create_node tests ---

    #[test]
    fn test_create_node_with_caller_provided_id() {
        let schema = build_writable_test_schema();
        let (db, captured) = make_capturing_db(schema);
        let conn = Connection::new(&db).unwrap();

        let mut props = HashMap::new();
        props.insert("person_id".to_string(), Value::String("p1".to_string()));
        props.insert("name".to_string(), Value::String("Alice".to_string()));

        let result = conn.create_node("Person", props);
        assert!(
            result.is_ok(),
            "create_node should succeed: {:?}",
            result.err()
        );
        assert_eq!(result.unwrap(), "p1");

        let sqls = captured.lock().unwrap();
        assert_eq!(sqls.len(), 1);
        let sql = &sqls[0];
        assert!(sql.contains("INSERT INTO"), "should be INSERT");
        assert!(sql.contains("test_db"), "should reference database");
        assert!(sql.contains("persons"), "should reference table");
        assert!(sql.contains("person_id"), "should include ID column");
        assert!(sql.contains("'p1'"), "should include ID value");
        assert!(sql.contains("full_name"), "should map name -> full_name");
        assert!(sql.contains("'Alice'"), "should include property value");
    }

    #[test]
    fn test_create_node_without_id_omits_id_column() {
        let schema = build_writable_test_schema();
        let (db, captured) = make_capturing_db(schema);
        let conn = Connection::new(&db).unwrap();

        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Bob".to_string()));

        let result = conn.create_node("Person", props);
        assert!(
            result.is_ok(),
            "create_node should succeed: {:?}",
            result.err()
        );
        let id = result.unwrap();
        // Client-side UUID should be a valid UUID (36 chars with hyphens)
        assert_eq!(id.len(), 36, "auto-generated ID should be a UUID: {}", id);
        assert!(id.contains('-'), "UUID should contain hyphens: {}", id);

        let sqls = captured.lock().unwrap();
        let sql = &sqls[0];
        assert!(sql.contains("full_name"), "should map name -> full_name");
        assert!(
            sql.contains(&id),
            "INSERT should contain the generated UUID"
        );
        assert!(
            sql.contains("person_id"),
            "should include ID column with auto-generated UUID"
        );
    }

    #[test]
    fn test_create_node_unknown_property_returns_validation_error() {
        let schema = build_writable_test_schema();
        let db = make_stub_db_with_schema(schema);
        let conn = Connection::new(&db).unwrap();

        let mut props = HashMap::new();
        props.insert("nonexistent".to_string(), Value::String("val".to_string()));

        let result = conn.create_node("Person", props);
        assert!(result.is_err(), "unknown property should return error");
        let err = result.unwrap_err();
        assert!(
            matches!(err, EmbeddedError::Validation(_)),
            "should be Validation error: {:?}",
            err
        );
        let msg = err.to_string();
        assert!(msg.contains("nonexistent"), "error should list unknown key");
    }

    // --- create_edge tests ---

    #[test]
    fn test_create_edge_generates_correct_insert() {
        let schema = build_writable_test_schema();
        let (db, captured) = make_capturing_db(schema);
        let conn = Connection::new(&db).unwrap();

        let mut props = HashMap::new();
        props.insert("since".to_string(), Value::Int64(2020));

        let result = conn.create_edge("KNOWS", "p1", "p2", props);
        assert!(
            result.is_ok(),
            "create_edge should succeed: {:?}",
            result.err()
        );

        let sqls = captured.lock().unwrap();
        let sql = &sqls[0];
        assert!(sql.contains("INSERT INTO"), "should be INSERT");
        assert!(sql.contains("knows"), "should reference table");
        assert!(
            sql.contains("from_person_id"),
            "should include from_id column"
        );
        assert!(sql.contains("to_person_id"), "should include to_id column");
        assert!(sql.contains("'p1'"), "should include from_id value");
        assert!(sql.contains("'p2'"), "should include to_id value");
        assert!(sql.contains("since_year"), "should map since -> since_year");
        assert!(sql.contains("2020"), "should include property value");
    }

    // --- upsert_node tests ---

    #[test]
    fn test_upsert_node_without_id_returns_validation_error() {
        let schema = build_writable_test_schema();
        let db = make_stub_db_with_schema(schema);
        let conn = Connection::new(&db).unwrap();

        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));

        let result = conn.upsert_node("Person", props);
        assert!(result.is_err(), "upsert without ID should fail");
        let err = result.unwrap_err();
        assert!(
            matches!(err, EmbeddedError::Validation(_)),
            "should be Validation error: {:?}",
            err
        );
        assert!(
            err.to_string().contains("person_id"),
            "should mention the required ID property"
        );
    }

    #[test]
    fn test_upsert_node_with_id_generates_insert() {
        let schema = build_writable_test_schema();
        let (db, captured) = make_capturing_db(schema);
        let conn = Connection::new(&db).unwrap();

        let mut props = HashMap::new();
        props.insert("person_id".to_string(), Value::String("p1".to_string()));
        props.insert("name".to_string(), Value::String("Alice".to_string()));

        let result = conn.upsert_node("Person", props);
        assert!(
            result.is_ok(),
            "upsert_node should succeed: {:?}",
            result.err()
        );
        assert_eq!(result.unwrap(), "p1");

        let sqls = captured.lock().unwrap();
        assert!(!sqls.is_empty(), "should have executed INSERT");
    }

    // --- batch tests ---

    #[test]
    fn test_create_nodes_batch_generates_single_insert() {
        let schema = build_writable_test_schema();
        let (db, captured) = make_capturing_db(schema);
        let conn = Connection::new(&db).unwrap();

        let mut row1 = HashMap::new();
        row1.insert("person_id".to_string(), Value::String("p1".to_string()));
        row1.insert("name".to_string(), Value::String("Alice".to_string()));

        let mut row2 = HashMap::new();
        row2.insert("person_id".to_string(), Value::String("p2".to_string()));
        row2.insert("name".to_string(), Value::String("Bob".to_string()));

        let result = conn.create_nodes("Person", vec![row1, row2]);
        assert!(
            result.is_ok(),
            "batch create should succeed: {:?}",
            result.err()
        );
        let ids = result.unwrap();
        assert_eq!(ids, vec!["p1", "p2"]);

        let sqls = captured.lock().unwrap();
        assert_eq!(sqls.len(), 1, "should be a single INSERT");
        let sql = &sqls[0];
        assert!(sql.contains("'p1'"), "should include first row");
        assert!(sql.contains("'p2'"), "should include second row");
    }

    #[test]
    fn test_create_edges_batch_generates_single_insert() {
        let schema = build_writable_test_schema();
        let (db, captured) = make_capturing_db(schema);
        let conn = Connection::new(&db).unwrap();

        let batch = vec![
            ("p1".to_string(), "p2".to_string(), HashMap::new()),
            ("p2".to_string(), "p3".to_string(), HashMap::new()),
        ];

        let result = conn.create_edges("KNOWS", batch);
        assert!(
            result.is_ok(),
            "batch create edges should succeed: {:?}",
            result.err()
        );

        let sqls = captured.lock().unwrap();
        assert_eq!(sqls.len(), 1, "should be a single INSERT");
        let sql = &sqls[0];
        assert!(sql.contains("'p1'"), "should include first edge from_id");
        assert!(sql.contains("'p3'"), "should include second edge to_id");
    }
}
