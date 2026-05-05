//! `Connection` — executes Cypher queries against a `Database`.
//!
//! Analogous to `kuzu::Connection`. Multiple connections can share one `Database`.
//! Each `Connection` holds a reference to the `Database`'s executor and schema.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use clickgraph::executor::QueryExecutor;
use clickgraph::graph_catalog::graph_schema::GraphSchema;

use super::database::Database;
use super::error::EmbeddedError;
use super::export::{build_export_sql, ExportOptions};
use super::graph_result::{parse_element_id, transform_rows_to_graph, GraphResult, StoreStats};
use super::query_result::QueryResult;
use super::value::Value;
use super::write_helpers;

/// Default maximum CTE recursion depth for Cypher→SQL translation.
const DEFAULT_MAX_CTE_DEPTH: u32 = 100;

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
    remote_executor: Option<Arc<dyn QueryExecutor>>,
    schema: Arc<GraphSchema>,
    db: &'db Database,
    /// Query timeout in milliseconds. 0 = no timeout (default).
    query_timeout_ms: u64,
}

impl<'db> Connection<'db> {
    /// Create a new connection to `db`.
    pub fn new(db: &'db Database) -> Result<Self, EmbeddedError> {
        Ok(Connection {
            executor: Arc::clone(&db.executor),
            remote_executor: db.remote_executor.as_ref().map(Arc::clone),
            schema: Arc::clone(&db.schema),
            db,
            query_timeout_ms: 0,
        })
    }

    /// Set the query timeout in milliseconds. 0 = no timeout (default).
    ///
    /// Mirrors `kuzu::Connection::set_query_timeout()`. Applies to
    /// `query()`, `query_remote()`, `query_graph()`, and `query_remote_graph()`.
    pub fn set_query_timeout(&mut self, timeout_ms: u64) {
        self.query_timeout_ms = timeout_ms;
    }

    /// Get the current query timeout in milliseconds. 0 = no timeout.
    pub fn get_query_timeout(&self) -> u64 {
        self.query_timeout_ms
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
        self.db
            .runtime
            .block_on(self.with_timeout(self.query_async(cypher)))
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

    /// Execute a raw SQL statement (DDL, DML, or administrative command).
    pub fn execute_sql(&self, sql: &str) -> Result<(), EmbeddedError> {
        self.db.runtime.block_on(async {
            self.executor
                .execute_text(sql, "TabSeparated", None)
                .await
                .map_err(EmbeddedError::from)?;
            Ok(())
        })
    }

    /// Execute a Cypher query against the remote ClickHouse cluster.
    ///
    /// Requires `RemoteConfig` to have been provided when opening the database.
    /// Returns an error if no remote executor is configured.
    pub fn query_remote(&self, cypher: &str) -> Result<QueryResult, EmbeddedError> {
        let remote = self.get_remote_executor()?;
        self.db
            .runtime
            .block_on(self.with_timeout(self.query_with_executor_async(cypher, remote)))
    }

    /// Execute a Cypher query locally and return a structured graph result.
    ///
    /// Uses `cypher_to_sql_with_metadata()` to get plan metadata, then
    /// transforms the result rows into `GraphNode`s and `GraphEdge`s.
    pub fn query_graph(&self, cypher: &str) -> Result<GraphResult, EmbeddedError> {
        self.db
            .runtime
            .block_on(self.with_timeout(self.query_graph_async(cypher, &self.executor)))
    }

    /// Execute a Cypher query on the remote cluster and return a structured graph result.
    ///
    /// Combines remote execution with graph decomposition. The returned
    /// `GraphResult` can be passed to `store_subgraph()` to persist locally.
    pub fn query_remote_graph(&self, cypher: &str) -> Result<GraphResult, EmbeddedError> {
        let remote = self.get_remote_executor()?;
        self.db
            .runtime
            .block_on(self.with_timeout(self.query_graph_async(cypher, remote)))
    }

    /// Store a `GraphResult` (from `query_graph` or `query_remote_graph`) into
    /// local writable tables.
    ///
    /// Decomposes the graph into nodes grouped by label and edges grouped by
    /// type, then batch-inserts each group via `create_nodes()` / `create_edges()`.
    ///
    /// **Note**: Multi-labeled nodes are stored under their first label only.
    /// This matches ClickGraph's schema model where each node belongs to exactly
    /// one label (table).
    pub fn store_subgraph(&self, graph: &GraphResult) -> Result<StoreStats, EmbeddedError> {
        let mut nodes_stored = 0usize;
        let mut edges_stored = 0usize;

        // Group nodes by label
        let mut nodes_by_label: HashMap<String, Vec<HashMap<String, Value>>> = HashMap::new();
        for node in graph.nodes() {
            let label = node
                .labels
                .first()
                .ok_or_else(|| EmbeddedError::Validation("Node has no labels".to_string()))?;
            nodes_by_label
                .entry(label.clone())
                .or_default()
                .push(node.properties.clone());
        }

        // Group edges by type, extracting raw IDs from element_id strings
        let mut edges_by_type: HashMap<String, Vec<(String, String, HashMap<String, Value>)>> =
            HashMap::new();
        for edge in graph.edges() {
            let (_, from_raw_id) = parse_element_id(&edge.from_id).ok_or_else(|| {
                EmbeddedError::Validation(format!("Invalid from element_id: {}", edge.from_id))
            })?;
            let (_, to_raw_id) = parse_element_id(&edge.to_id).ok_or_else(|| {
                EmbeddedError::Validation(format!("Invalid to element_id: {}", edge.to_id))
            })?;
            edges_by_type
                .entry(edge.type_name.clone())
                .or_default()
                .push((
                    from_raw_id.to_string(),
                    to_raw_id.to_string(),
                    edge.properties.clone(),
                ));
        }

        // Batch-insert nodes
        for (label, batch) in nodes_by_label {
            nodes_stored += batch.len();
            self.create_nodes(&label, batch)?;
        }

        // Batch-insert edges
        for (edge_type, batch) in edges_by_type {
            edges_stored += batch.len();
            self.create_edges(&edge_type, batch)?;
        }

        Ok(StoreStats {
            nodes_stored,
            edges_stored,
        })
    }

    /// Create a node with the given label and properties.
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

    /// Parse and load graph data from a Cypher CREATE block.
    ///
    /// Handles the subset of CREATE syntax used in test fixtures and data loading:
    /// - Labeled nodes with properties: `(n:Person {name: 'Alice', age: 30})`
    /// - Directed edges: `(n)-[:KNOWS {since: 2020}]->(m)`
    /// - Multi-statement blocks (multiple CREATE statements in one string)
    ///
    /// Returns [`LoadStats`](crate::cypher_loader::LoadStats) with counts of nodes and edges inserted.
    ///
    /// # Notes
    ///
    /// Edges whose endpoint variables are not defined in the same CREATE block
    /// are silently skipped (no error is returned). Ensure all referenced node
    /// variables appear earlier in the same block.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use clickgraph_embedded::{Database, Connection, SystemConfig};
    /// # let db = Database::new("schema.yaml", SystemConfig::default()).unwrap();
    /// # let conn = Connection::new(&db).unwrap();
    /// let stats = conn.load_cypher_create(
    ///     "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})"
    /// ).unwrap();
    /// assert_eq!(stats.nodes_loaded, 2);
    /// assert_eq!(stats.edges_loaded, 1);
    /// ```
    pub fn load_cypher_create(
        &self,
        cypher: &str,
    ) -> Result<crate::cypher_loader::LoadStats, EmbeddedError> {
        use crate::cypher_loader::{parse_create_block, LoadStats};
        let mut var_map = std::collections::HashMap::new();
        let parsed = parse_create_block(cypher, &mut var_map);

        let mut stats = LoadStats::default();

        // Insert nodes; track var → assigned ID for edge resolution.
        let mut node_ids: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        for node in &parsed.nodes {
            let label = node.label.as_deref().unwrap_or("__Unlabeled");
            let var = node.var.as_deref().unwrap_or("").to_string();
            let props: HashMap<String, Value> = node
                .props
                .iter()
                .map(|(k, v)| (k.clone(), v.to_value()))
                .collect();
            let node_id = self.create_node(label, props)?;
            if !var.is_empty() {
                node_ids.insert(var, node_id);
            }
            stats.nodes_loaded += 1;
        }

        // Insert edges using the resolved node IDs.
        for edge in &parsed.edges {
            let from_id = match node_ids.get(&edge.from_var) {
                Some(id) => id.clone(),
                None => continue, // unresolved variable — skip
            };
            let to_id = match node_ids.get(&edge.to_var) {
                Some(id) => id.clone(),
                None => continue,
            };
            let props: HashMap<String, Value> = edge
                .props
                .iter()
                .map(|(k, v)| (k.clone(), v.to_value()))
                .collect();
            self.create_edge(&edge.rel_type, &from_id, &to_id, props)?;
            stats.edges_loaded += 1;
        }

        Ok(stats)
    }

    /// Upsert a node (INSERT with ReplacingMergeTree deduplication).
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
        for row_props in &batch {
            write_helpers::validate_properties(row_props, &property_mappings, &id_col_strs)?;
        }
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
        let mut reverse_map: HashMap<String, String> = HashMap::new();
        for (cypher_name, ch_col) in &property_mappings {
            reverse_map.insert(ch_col.to_string(), cypher_name.to_string());
        }
        let mut all_values_rows = Vec::new();
        let mut ids = Vec::new();
        for row_props in &batch {
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
        for (_, _, row_props) in &batch {
            write_helpers::validate_properties(row_props, &property_mappings, &id_col_strs)?;
        }
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

    /// Delete nodes matching the given label and filter criteria.
    ///
    /// Uses lightweight `DELETE FROM` — synchronous and low-overhead compared to
    /// the old `ALTER TABLE DELETE` mutation path.
    pub fn delete_nodes(
        &self,
        label: &str,
        filters: HashMap<String, Value>,
    ) -> Result<(), EmbeddedError> {
        let node_schema = self.get_node_schema(label)?;
        write_helpers::check_writable(&node_schema.source, label)?;
        let id_columns = node_schema.node_id.id.columns();
        let id_col_strs: Vec<&str> = id_columns.iter().copied().collect();
        let property_mappings =
            write_helpers::extract_property_mappings(&node_schema.property_mappings);
        write_helpers::validate_properties(&filters, &property_mappings, &id_col_strs)?;
        let sql = write_helpers::build_delete_sql(
            &node_schema.database,
            &node_schema.table_name,
            &filters,
            &property_mappings,
            &id_col_strs,
        )?;
        self.execute_sql(&sql)?;
        Ok(())
    }

    /// Delete edges matching the given type and filter criteria.
    pub fn delete_edges(
        &self,
        edge_type: &str,
        filters: HashMap<String, Value>,
    ) -> Result<(), EmbeddedError> {
        let rel_schema = self.get_rel_schema(edge_type)?;
        write_helpers::check_writable(&rel_schema.source, edge_type)?;
        let from_id_cols = rel_schema.from_id.columns();
        let to_id_cols = rel_schema.to_id.columns();
        let mut id_col_strs: Vec<&str> = Vec::new();
        id_col_strs.extend(from_id_cols.iter().copied());
        id_col_strs.extend(to_id_cols.iter().copied());
        let property_mappings =
            write_helpers::extract_property_mappings(&rel_schema.property_mappings);
        let mut extended_mappings = property_mappings.clone();
        let from_col = from_id_cols.first().copied().unwrap_or("from_id");
        let to_col = to_id_cols.first().copied().unwrap_or("to_id");
        extended_mappings.insert("from_id", from_col);
        extended_mappings.insert("to_id", to_col);
        write_helpers::validate_properties(&filters, &extended_mappings, &id_col_strs)?;
        let sql = write_helpers::build_delete_sql(
            &rel_schema.database,
            &rel_schema.table_name,
            &filters,
            &extended_mappings,
            &id_col_strs,
        )?;
        self.execute_sql(&sql)?;
        Ok(())
    }

    /// Import nodes from inline newline-delimited JSON (JSONEachRow format).
    pub fn import_json(&self, label: &str, json_lines: &str) -> Result<(), EmbeddedError> {
        let node_schema = self.get_node_schema(label)?;
        write_helpers::check_writable(&node_schema.source, label)?;
        let id_columns = node_schema.node_id.id.columns();
        let id_col_strs: Vec<&str> = id_columns.iter().copied().collect();
        let property_mappings =
            write_helpers::extract_property_mappings(&node_schema.property_mappings);
        let (transformed_json, line_count) =
            write_helpers::transform_json_keys(json_lines, &property_mappings, &id_col_strs)?;
        if line_count == 0 {
            return Ok(());
        }
        let sql = format!(
            "INSERT INTO `{}`.`{}` FORMAT JSONEachRow\n{}",
            node_schema.database, node_schema.table_name, transformed_json
        );
        self.execute_sql(&sql)?;
        Ok(())
    }

    /// Import nodes from a JSON file (JSONEachRow format).
    pub fn import_json_file(&self, label: &str, file_path: &str) -> Result<(), EmbeddedError> {
        self.import_file_with_format(label, file_path, "JSONEachRow")
    }

    /// Import nodes from a CSV file (CSVWithNames format — first row is header).
    pub fn import_csv_file(&self, label: &str, file_path: &str) -> Result<(), EmbeddedError> {
        self.import_file_with_format(label, file_path, "CSVWithNames")
    }

    /// Import nodes from a Parquet file.
    pub fn import_parquet_file(&self, label: &str, file_path: &str) -> Result<(), EmbeddedError> {
        self.import_file_with_format(label, file_path, "Parquet")
    }

    /// Import nodes from a file, auto-detecting the format from the extension.
    ///
    /// Supported extensions: `.parquet`/`.pq`, `.csv`, `.tsv`/`.tab`,
    /// `.json`/`.ndjson`/`.jsonl`.
    ///
    /// **Column mapping**: File columns should use Cypher property names (mapped
    /// automatically via the schema's `property_mappings`) or ClickHouse column
    /// names directly (used as-is when no mapping applies).
    ///
    /// **Note**: This imports nodes only. For edge import from files, use
    /// `execute_sql()` with a manual `INSERT INTO ... SELECT ... FROM file()`.
    pub fn import_file(&self, label: &str, file_path: &str) -> Result<(), EmbeddedError> {
        let format = write_helpers::import_format_from_extension(file_path).ok_or_else(|| {
            EmbeddedError::Validation(format!(
                "Cannot determine import format from '{}'. \
                 Use import_csv_file(), import_parquet_file(), or import_json_file() instead.",
                file_path
            ))
        })?;
        self.import_file_with_format(label, file_path, format)
    }

    /// Internal: import nodes from a file with an explicit ClickHouse format name.
    fn import_file_with_format(
        &self,
        label: &str,
        file_path: &str,
        format: &str,
    ) -> Result<(), EmbeddedError> {
        if !std::path::Path::new(file_path).exists() {
            return Err(EmbeddedError::Io(format!("File not found: {}", file_path)));
        }
        write_helpers::validate_file_path(file_path)?;
        let node_schema = self.get_node_schema(label)?;
        write_helpers::check_writable(&node_schema.source, label)?;
        let id_columns = node_schema.node_id.id.columns();
        let id_col_strs: Vec<&str> = id_columns.iter().copied().collect();
        let property_mappings =
            write_helpers::extract_property_mappings(&node_schema.property_mappings);
        let sql = write_helpers::build_import_file_sql(
            &node_schema.database,
            &node_schema.table_name,
            file_path,
            format,
            &property_mappings,
            &id_col_strs,
        );
        self.execute_sql(&sql)?;
        Ok(())
    }

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

    /// Handle `CALL apoc.export.{csv|json|parquet}.query(...)` in embedded mode.
    ///
    /// Parses arguments, translates inner Cypher to SQL, builds export SQL, executes.
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
            Ok(QueryResult::new(
                vec![
                    "file".to_string(),
                    "format".to_string(),
                    "source".to_string(),
                ],
                vec![vec![
                    Value::String(args.destination),
                    Value::String(ch_format.to_string()),
                    Value::String(args.cypher_query),
                ]],
            ))
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
            Ok(QueryResult::new(
                vec![
                    "file".to_string(),
                    "format".to_string(),
                    "source".to_string(),
                ],
                vec![vec![
                    Value::String(destination),
                    Value::String(ch_format.to_string()),
                    Value::String(inner_cypher),
                ]],
            ))
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
            .map(|row| match row {
                serde_json::Value::Object(map) => columns
                    .iter()
                    .map(|col| {
                        Value::from(map.get(col).cloned().unwrap_or(serde_json::Value::Null))
                    })
                    .collect(),
                other => vec![Value::from(other)],
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
            .map(|row| match row {
                serde_json::Value::Object(map) => columns
                    .iter()
                    .map(|col| {
                        Value::from(map.get(col).cloned().unwrap_or(serde_json::Value::Null))
                    })
                    .collect(),
                other => vec![Value::from(other)],
            })
            .collect();
        Ok(QueryResult::new(columns, rows))
    }

    /// Wrap a future with the configured query timeout (if any).
    async fn with_timeout<T>(
        &self,
        fut: impl std::future::Future<Output = Result<T, EmbeddedError>>,
    ) -> Result<T, EmbeddedError> {
        if self.query_timeout_ms > 0 {
            tokio::time::timeout(std::time::Duration::from_millis(self.query_timeout_ms), fut)
                .await
                .map_err(|_| {
                    EmbeddedError::Query(format!(
                        "Query timed out after {}ms",
                        self.query_timeout_ms
                    ))
                })?
        } else {
            fut.await
        }
    }

    fn get_remote_executor(&self) -> Result<&Arc<dyn QueryExecutor>, EmbeddedError> {
        self.remote_executor.as_ref().ok_or_else(|| {
            EmbeddedError::Query(
                "No remote executor configured. Provide RemoteConfig when opening the database."
                    .to_string(),
            )
        })
    }

    /// Execute a Cypher query using the specified executor and return a tabular result.
    async fn query_with_executor_async(
        &self,
        cypher: &str,
        executor: &Arc<dyn QueryExecutor>,
    ) -> Result<QueryResult, EmbeddedError> {
        use clickgraph::clickhouse_query_generator::cypher_to_sql;
        use clickgraph::server::query_context::{
            set_current_schema, with_query_context, QueryContext,
        };
        let schema = Arc::clone(&self.schema);
        let executor = Arc::clone(executor);
        let cypher = cypher.to_string();
        with_query_context(QueryContext::new(None), async move {
            set_current_schema(Arc::clone(&schema));
            let compile_start = Instant::now();
            let final_sql = cypher_to_sql(&cypher, &schema, DEFAULT_MAX_CTE_DEPTH)
                .map_err(EmbeddedError::Query)?;
            let compile_time_ms = compile_start.elapsed().as_secs_f64() * 1000.0;
            let exec_start = Instant::now();
            let json_rows = executor
                .execute_json(&final_sql, None)
                .await
                .map_err(EmbeddedError::from)?;
            let execution_time_ms = exec_start.elapsed().as_secs_f64() * 1000.0;
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
            Ok(QueryResult::with_timing(
                column_names,
                rows,
                compile_time_ms,
                execution_time_ms,
            ))
        })
        .await
    }

    /// Execute a Cypher query using the specified executor and return a graph result.
    async fn query_graph_async(
        &self,
        cypher: &str,
        executor: &Arc<dyn QueryExecutor>,
    ) -> Result<GraphResult, EmbeddedError> {
        use clickgraph::clickhouse_query_generator::cypher_to_sql_with_metadata;
        use clickgraph::server::query_context::{
            set_current_schema, with_query_context, QueryContext,
        };
        let schema = Arc::clone(&self.schema);
        let executor = Arc::clone(executor);
        let cypher = cypher.to_string();
        with_query_context(QueryContext::new(None), async move {
            set_current_schema(Arc::clone(&schema));
            let (sql, logical_plan, plan_ctx) =
                cypher_to_sql_with_metadata(&cypher, &schema, DEFAULT_MAX_CTE_DEPTH)
                    .map_err(EmbeddedError::Query)?;
            let json_rows = executor
                .execute_json(&sql, None)
                .await
                .map_err(EmbeddedError::from)?;
            transform_rows_to_graph(&json_rows, &logical_plan, &plan_ctx, &schema)
                .map_err(EmbeddedError::Query)
        })
        .await
    }

    async fn query_async(&self, cypher: &str) -> Result<QueryResult, EmbeddedError> {
        // Strip comments before dispatch parsing so write detection matches
        // what `handle_write_async` sees during its own parse below — comments
        // (e.g., `// ...` or `/* ... */`) inside the query would otherwise
        // break parsing here and silently bypass write routing.
        let dispatch_input = clickgraph::open_cypher_parser::strip_comments(cypher);
        if let Ok((_, stmt)) =
            clickgraph::open_cypher_parser::parse_cypher_statement(&dispatch_input)
        {
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
            // Cypher write clauses route to a separate executor path that
            // emits lightweight INSERT / UPDATE / DELETE per Phase 2's
            // WriteRenderPlan. Read queries fall through to the regular
            // SELECT path below.
            //
            // Any query carrying a write clause — CREATE / SET / DELETE /
            // REMOVE — must enter the write pipeline so we either execute it
            // or reject it with a clear error. Falling through to the read
            // path produces confusing render-time errors. Note that
            // `get_query_type` only inspects SET / DELETE / REMOVE, so we
            // also need an explicit `create_clause.is_some()` check to cover
            // `CREATE`, `MATCH ... CREATE`, and `CREATE ... RETURN` variants.
            if let clickgraph::open_cypher_parser::ast::CypherStatement::Query { query, .. } = &stmt
            {
                use clickgraph::query_planner::types::QueryType;
                let is_write = matches!(
                    clickgraph::query_planner::get_query_type(query),
                    QueryType::Update | QueryType::Delete
                ) || query.create_clause.is_some();
                if is_write {
                    return self.handle_write_async(cypher).await;
                }
            }
        }
        self.query_with_executor_async(cypher, &self.executor).await
    }

    /// Plan, render, and execute a Cypher write query (CREATE / SET /
    /// DELETE / REMOVE). Returns Neo4j-compatible counters as a single-row
    /// `QueryResult` per Decision 0.8 of the embedded-writes design — or,
    /// when the statement also carries a RETURN clause (Phase 5d), the
    /// row payload from re-running the read pipeline against the modified
    /// state with the write counters attached via `QueryResult::get_write_counters()`.
    async fn handle_write_async(&self, cypher: &str) -> Result<QueryResult, EmbeddedError> {
        use clickgraph::clickhouse_query_generator::cypher_to_sql_read_only;
        use clickgraph::clickhouse_query_generator::write_to_sql::write_render_to_sql;
        use clickgraph::open_cypher_parser::ast::CypherStatement;
        use clickgraph::query_planner::logical_plan::LogicalPlan;
        use clickgraph::query_planner::write_guard::ensure_write_target_writable;
        use clickgraph::render_plan::write_plan_builder::build_write_plan;
        use clickgraph::server::query_context::{
            set_current_schema, with_query_context, QueryContext,
        };

        let schema = Arc::clone(&self.schema);
        let executor = Arc::clone(&self.executor);
        let executor_kind = self.db.executor_kind;
        let cypher = cypher.to_string();

        with_query_context(QueryContext::new(None), async move {
            set_current_schema(Arc::clone(&schema));

            let compile_start = Instant::now();

            // Parse, plan, run the regular analyzer/optimizer pipeline so
            // any read pipeline below the write variant is fully resolved
            // before we render.
            let cleaned = clickgraph::open_cypher_parser::strip_comments(&cypher);
            let (_remaining, stmt) =
                clickgraph::open_cypher_parser::parse_cypher_statement(&cleaned)
                    .map_err(|e| EmbeddedError::Query(format!("Parse error: {:?}", e)))?;

            // Capture whether the statement carries a RETURN clause before
            // ownership of `stmt` moves into the planner. write+RETURN
            // routes through the Phase 5d branch below; pure writes keep
            // the synthetic counter-row response.
            let has_return = matches!(
                &stmt,
                CypherStatement::Query { query, .. } if query.return_clause.is_some()
            );

            let (logical_plan, _plan_ctx) =
                clickgraph::query_planner::evaluate_read_statement(stmt, &schema, None, None, None)
                    .map_err(|e| EmbeddedError::Query(format!("Plan error: {}", e)))?;

            // Decision 0.1 / 0.3 / 0.6 admission check.
            ensure_write_target_writable(&logical_plan, &schema, executor_kind)
                .map_err(|e| EmbeddedError::Query(format!("Write rejected: {}", e)))?;

            // Walk past read-only wrappers (Projection / OrderBy / Skip /
            // Limit) to find the write subplan. write+RETURN puts a
            // Projection above the write; pure-write statements have the
            // write at the root. The subplan retains its full read input
            // (the MATCH/WHERE/etc. that bound the variables we mutate).
            let write_subplan = find_write_subplan(&logical_plan).ok_or_else(|| {
                EmbeddedError::Query(
                    "Cypher write clause is not supported in this combination yet \
                     (e.g. write clauses inside subqueries). Use a separate \
                     write statement followed by a MATCH for now."
                        .to_string(),
                )
            })?;

            let write_plan = build_write_plan(write_subplan, &schema)
                .map_err(|e| EmbeddedError::Query(format!("Write render error: {}", e)))?
                .ok_or_else(|| {
                    EmbeddedError::Query(
                        "Internal error: write subplan resolved but `build_write_plan` \
                         returned None. This is a planner/rendering bug; please report."
                            .to_string(),
                    )
                })?;

            // Phase 5d v1 supports DELETE / SET / REMOVE + RETURN by
            // executing the write, then re-running the read pipeline with
            // the write clauses stripped. CREATE + RETURN is *not*
            // supported by that path: the read pipeline can't see freshly
            // inserted rows by alias (the alias was never bound by a
            // MATCH and the inserted row has no MATCH-able identity yet),
            // so referencing the create-bound alias from RETURN would
            // fail with "undefined variable" *after* the INSERT has
            // already executed. Rejecting up-front keeps the database
            // unchanged on failure and surfaces a clear, deterministic
            // error rather than a partially-applied write. Pinned by
            // `cypher_create_with_return_rejected_before_insert` below.
            if has_return && matches!(write_subplan, LogicalPlan::Create(_)) {
                return Err(EmbeddedError::Query(
                    "CREATE … RETURN is not supported in this build of ClickGraph yet. \
                     The write would execute but the RETURN cannot reference the \
                     newly created node by alias from the read pipeline. \
                     Run CREATE as a separate statement, then MATCH … RETURN."
                        .to_string(),
                ));
            }

            let stmts = write_render_to_sql(&write_plan);
            let compile_time_ms = compile_start.elapsed().as_secs_f64() * 1000.0;

            // Resolve counters via `count()` probes against the same
            // WHERE that each lightweight DELETE / UPDATE will use, then
            // execute the mutations themselves. The probes run *before*
            // the writes so the count reflects the matched-but-not-yet-
            // mutated row set — DELETE / UPDATE are naturally idempotent
            // against the probe's snapshot for the chdb workload here.
            // INSERT counts are exact and don't need a probe.
            let exec_start = Instant::now();
            let counters = resolve_write_counters(&write_plan, &executor).await?;
            for sql in &stmts {
                executor
                    .execute_json(sql, None)
                    .await
                    .map_err(EmbeddedError::from)?;
            }
            let execution_time_ms = exec_start.elapsed().as_secs_f64() * 1000.0;

            if !has_return {
                // Pure-write path: surface counters as a synthetic 4-column
                // single-row result for back-compat with Phase 5a/5b.
                let column_names = vec![
                    "nodes_created".to_string(),
                    "properties_set".to_string(),
                    "nodes_deleted".to_string(),
                    "relationships_deleted".to_string(),
                ];
                let row = vec![
                    Value::Int64(counters.nodes_created as i64),
                    Value::Int64(counters.properties_set as i64),
                    Value::Int64(counters.nodes_deleted as i64),
                    Value::Int64(counters.relationships_deleted as i64),
                ];
                return Ok(QueryResult::with_timing(
                    column_names,
                    vec![row],
                    compile_time_ms,
                    execution_time_ms,
                ));
            }

            // Phase 5d: write+RETURN. Render the Cypher through the
            // read-only pipeline (which clears CREATE / SET / DELETE /
            // REMOVE on the AST internally so the planner re-derives just
            // the read shape), execute it against the now-modified state,
            // and attach the write counters via the side-channel.
            let read_compile_start = Instant::now();
            let read_sql = cypher_to_sql_read_only(&cypher, &schema, DEFAULT_MAX_CTE_DEPTH)
                .map_err(EmbeddedError::Query)?;
            let read_compile_ms = read_compile_start.elapsed().as_secs_f64() * 1000.0;

            let read_exec_start = Instant::now();
            let json_rows = executor
                .execute_json(&read_sql, None)
                .await
                .map_err(EmbeddedError::from)?;
            let read_exec_ms = read_exec_start.elapsed().as_secs_f64() * 1000.0;

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

            let mut counter_map: HashMap<String, i64> = HashMap::new();
            counter_map.insert("nodes_created".to_string(), counters.nodes_created as i64);
            counter_map.insert("properties_set".to_string(), counters.properties_set as i64);
            counter_map.insert("nodes_deleted".to_string(), counters.nodes_deleted as i64);
            counter_map.insert(
                "relationships_deleted".to_string(),
                counters.relationships_deleted as i64,
            );

            Ok(QueryResult::with_timing_and_counters(
                column_names,
                rows,
                compile_time_ms + read_compile_ms,
                execution_time_ms + read_exec_ms,
                counter_map,
            ))
        })
        .await
    }
}

/// Walk past read-only wrappers (Projection / OrderBy / Skip / Limit) to
/// find the topmost write subplan inside `plan`. Returns `None` if the
/// plan tree contains no write node, or if a write is buried under a
/// shape we don't yet descend through (UNION, CartesianProduct, etc.).
///
/// This lets `handle_write_async` peel off the RETURN/ORDER BY/SKIP/LIMIT
/// chain that sits above a write in `MATCH … DELETE … RETURN …` shapes
/// without coupling that knowledge to the write_plan_builder, which only
/// matches root-level write nodes.
fn find_write_subplan(
    plan: &clickgraph::query_planner::logical_plan::LogicalPlan,
) -> Option<&clickgraph::query_planner::logical_plan::LogicalPlan> {
    use clickgraph::query_planner::logical_plan::LogicalPlan;
    match plan {
        LogicalPlan::Create(_)
        | LogicalPlan::SetProperties(_)
        | LogicalPlan::Delete(_)
        | LogicalPlan::Remove(_) => Some(plan),
        LogicalPlan::Projection(p) => find_write_subplan(&p.input),
        LogicalPlan::OrderBy(o) => find_write_subplan(&o.input),
        LogicalPlan::Skip(s) => find_write_subplan(&s.input),
        LogicalPlan::Limit(l) => find_write_subplan(&l.input),
        LogicalPlan::Filter(f) => find_write_subplan(&f.input),
        LogicalPlan::WithClause(w) => find_write_subplan(&w.input),
        LogicalPlan::GroupBy(g) => find_write_subplan(&g.input),
        // The graph-join inference pass wraps the entire planned tree in a
        // `GraphJoins` node even when the join list is empty (it carries
        // anchor/correlation metadata). Walk through it to reach a write.
        LogicalPlan::GraphJoins(gj) => find_write_subplan(&gj.input),
        _ => None,
    }
}

/// Counters surfaced as `nodes_created` / `properties_set` /
/// `nodes_deleted` / `relationships_deleted` on the write `QueryResult`.
/// `INSERT` counts come straight from the rendered op (rows are exact);
/// `DELETE` and `UPDATE` counts come from `probe_*_count_sql` probes
/// run against chdb just before the mutation, since the lightweight
/// write path doesn't return affected-row counts and a static "+= 1
/// per op" approximation drifts from the openCypher side-effect contract
/// when the WHERE matches zero rows (e.g. `OPTIONAL MATCH … DELETE` on
/// an empty graph) or many rows (e.g. `MATCH (n:X) SET n.k = …` with
/// multiple `:X`).
#[derive(Default)]
struct WriteCounters {
    nodes_created: u64,
    properties_set: u64,
    nodes_deleted: u64,
    relationships_deleted: u64,
}

/// Flat list of count probes derived from a `WriteRenderPlan`. The async
/// pass over the executor accumulates each probe's affected-row count
/// into the right `WriteCounters` field; static (INSERT) counts go
/// straight in without a probe.
enum ProbeAction {
    NodesCreatedStatic(u64),
    NodesDeletedProbe(String),
    RelationshipsDeletedProbe(String),
    /// `properties_set` is per-property-per-row: `assignments * affected_rows`.
    PropertiesSetProbe {
        assignments: u64,
        sql: String,
    },
}

fn collect_counter_probes(plan: &clickgraph::render_plan::WriteRenderPlan) -> Vec<ProbeAction> {
    let mut out = Vec::new();
    push_probes(plan, &mut out);
    out
}

fn push_probes(plan: &clickgraph::render_plan::WriteRenderPlan, out: &mut Vec<ProbeAction>) {
    use clickgraph::clickhouse_query_generator::write_to_sql::{
        probe_delete_count_sql, probe_update_count_sql,
    };
    use clickgraph::render_plan::WriteRenderPlan;
    match plan {
        WriteRenderPlan::Insert(op) => {
            // INSERT counts are exact: one row per VALUES tuple.
            // Whether it's a node or relationship is encoded in the table,
            // but rel CREATE is currently rejected by build_write_plan, so
            // every Insert here is a node create.
            out.push(ProbeAction::NodesCreatedStatic(op.rows.len() as u64));
        }
        WriteRenderPlan::Update(op) => {
            out.push(ProbeAction::PropertiesSetProbe {
                assignments: op.assignments.len() as u64,
                sql: probe_update_count_sql(op),
            });
        }
        WriteRenderPlan::Delete(op) => {
            out.push(ProbeAction::NodesDeletedProbe(probe_delete_count_sql(op)));
        }
        WriteRenderPlan::Sequence(seq) => {
            // DETACH DELETE renders as a Sequence of rel-cleanup DELETEs
            // followed by the final node DELETE; `Sequence(Delete, …,
            // Delete)` thus splits N-1 → relationships_deleted and last
            // → nodes_deleted. Other Sequence shapes (multi-Insert from
            // CREATE patterns; multi-Update from multi-target SET) walk
            // recursively under the same rules.
            let len = seq.len();
            for (i, inner) in seq.iter().enumerate() {
                match inner {
                    WriteRenderPlan::Delete(op) if i + 1 < len => {
                        out.push(ProbeAction::RelationshipsDeletedProbe(
                            probe_delete_count_sql(op),
                        ));
                    }
                    _ => push_probes(inner, out),
                }
            }
        }
    }
}

async fn run_count_probe(
    executor: &Arc<dyn QueryExecutor>,
    sql: &str,
) -> Result<u64, EmbeddedError> {
    let rows = executor
        .execute_json(sql, None)
        .await
        .map_err(EmbeddedError::from)?;
    // The probe SQL is fully under our control (`SELECT count() AS n …`),
    // so a missing row, missing `n` column, or non-numeric value is a
    // contract violation — most likely a regression in the probe SQL
    // builder or in the executor's JSON shape. Surface as an error
    // rather than silently bottoming out at 0, which would let an
    // inaccurate-counter regression pass tests instead of failing them.
    let Some(serde_json::Value::Object(obj)) = rows.first() else {
        return Err(EmbeddedError::Query(format!(
            "count probe `{sql}` returned no rows; expected one `{{\"n\": <N>}}` row"
        )));
    };
    let Some(val) = obj.get("n") else {
        return Err(EmbeddedError::Query(format!(
            "count probe `{sql}` row is missing the `n` column; got: {obj:?}"
        )));
    };
    match val {
        serde_json::Value::Number(n) => n.as_u64().ok_or_else(|| {
            EmbeddedError::Query(format!(
                "count probe `{sql}` returned non-u64 number: {n:?}"
            ))
        }),
        // ClickHouse JSONEachRow surfaces `count()` as a string for
        // values that don't fit i64; accept the string form and parse it.
        serde_json::Value::String(s) => s.parse::<u64>().map_err(|e| {
            EmbeddedError::Query(format!(
                "count probe `{sql}` returned unparseable string `{s}`: {e}"
            ))
        }),
        other => Err(EmbeddedError::Query(format!(
            "count probe `{sql}` returned unexpected `n` type: {other:?}"
        ))),
    }
}

async fn resolve_write_counters(
    plan: &clickgraph::render_plan::WriteRenderPlan,
    executor: &Arc<dyn QueryExecutor>,
) -> Result<WriteCounters, EmbeddedError> {
    let mut c = WriteCounters::default();
    for probe in collect_counter_probes(plan) {
        match probe {
            ProbeAction::NodesCreatedStatic(n) => c.nodes_created += n,
            ProbeAction::NodesDeletedProbe(sql) => {
                c.nodes_deleted += run_count_probe(executor, &sql).await?;
            }
            ProbeAction::RelationshipsDeletedProbe(sql) => {
                c.relationships_deleted += run_count_probe(executor, &sql).await?;
            }
            ProbeAction::PropertiesSetProbe { assignments, sql } => {
                let affected = run_count_probe(executor, &sql).await?;
                c.properties_set += assignments * affected;
            }
        }
    }
    Ok(c)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clickgraph::graph_catalog::config::GraphSchemaConfig;

    fn build_test_schema() -> Arc<GraphSchema> {
        let yaml = "name: test\ngraph_schema:\n  nodes:\n    - label: User\n      database: test_db\n      table: users\n      node_id: user_id\n      property_mappings:\n        user_id: user_id\n        name: full_name\n  edges:\n    - type: FOLLOWS\n      database: test_db\n      table: follows\n      from_node: User\n      to_node: User\n      from_id: follower_id\n      to_id: followed_id\n      property_mappings: {}\n";
        let config: GraphSchemaConfig = serde_yaml::from_str(yaml).expect("valid yaml");
        Arc::new(config.to_graph_schema().expect("valid schema"))
    }

    fn build_tck_fan_in_schema() -> Arc<GraphSchema> {
        let yaml = r#"name: tck
graph_schema:
  nodes:
    - label: __Unlabeled
      database: default
      table: tck_n___unlabeled
      node_id: _tck_id
      type: string
      property_mappings:
        _tck_id: _tck_id
        name: name
  edges:
    - type: A
      database: default
      table: tck_e_a___unlabeled___unlabeled
      from_node: __Unlabeled
      to_node: __Unlabeled
      from_id: from_id
      to_id: to_id
      property_mappings: {}
    - type: KNOWS
      database: default
      table: tck_e_knows___unlabeled___unlabeled
      from_node: __Unlabeled
      to_node: __Unlabeled
      from_id: from_id
      to_id: to_id
      property_mappings: {}
"#;
        let config: GraphSchemaConfig = serde_yaml::from_str(yaml).expect("valid yaml");
        Arc::new(config.to_graph_schema().expect("valid schema"))
    }

    #[test]
    fn test_chained_traversal_sql() {
        let db = make_stub_db_with_schema(build_tck_fan_in_schema());
        let conn = Connection::new(&db).unwrap();
        // Chained: n-->a-->b
        let sql = conn.query_to_sql("MATCH (n)-->(a)-->(b) RETURN b").unwrap();
        println!("Chained SQL:\n{}", sql);
        // The SQL must reference b somehow (either as a column or CTE)
        assert!(!sql.is_empty(), "should generate SQL");
    }

    #[test]
    fn test_fan_in_pattern_sql() {
        let db = make_stub_db_with_schema(build_tck_fan_in_schema());
        let conn = Connection::new(&db).unwrap();
        // Fan-in: three pre-bound nodes pointing to same target (scenario [21] uses lowercase)
        let sql = conn
            .query_to_sql(
                "MATCH (a {name: 'a'}), (b {name: 'b'}), (c {name: 'c'}) MATCH (a)-->(x), (b)-->(x), (c)-->(x) RETURN x",
            )
            .unwrap();
        println!("Fan-in SQL:\n{}", sql);
        // Fan-in: all three VLP CTEs should appear, joined on end_id
        assert!(
            sql.contains("vlp_multi_type_a_x"),
            "SQL should include a→x CTE: {}",
            sql
        );
        assert!(
            sql.contains("vlp_multi_type_b_x"),
            "SQL should include b→x CTE: {}",
            sql
        );
        assert!(
            sql.contains("vlp_multi_type_c_x"),
            "SQL should include c→x CTE: {}",
            sql
        );
        // The outer SELECT should join the CTEs on end_id
        assert!(sql.contains("end_id"), "SQL should join on end_id: {}", sql);
    }

    fn build_writable_test_schema() -> Arc<GraphSchema> {
        let yaml = "name: test_writable\ngraph_schema:\n  nodes:\n    - label: Person\n      database: test_db\n      table: persons\n      node_id: person_id\n      property_mappings:\n        person_id: person_id\n        name: full_name\n        age: age\n  edges:\n    - type: KNOWS\n      database: test_db\n      table: knows\n      from_node: Person\n      to_node: Person\n      from_id: from_person_id\n      to_id: to_person_id\n      property_mappings:\n        since: since_year\n";
        let config: GraphSchemaConfig = serde_yaml::from_str(yaml).expect("valid yaml");
        Arc::new(config.to_graph_schema().expect("valid schema"))
    }

    /// Phase 5e schema: three writable node labels with distinct ID
    /// columns, no relationships. Lets the multi-label fan-out test
    /// assert that `MATCH (n) DELETE n` emits one DELETE per node table.
    fn build_multi_label_writable_schema() -> Arc<GraphSchema> {
        let yaml = r#"name: test_multi_label
graph_schema:
  nodes:
    - label: A
      database: test_db
      table: a_nodes
      node_id: a_id
      property_mappings:
        a_id: a_id
    - label: B
      database: test_db
      table: b_nodes
      node_id: b_id
      property_mappings:
        b_id: b_id
    - label: C
      database: test_db
      table: c_nodes
      node_id: c_id
      property_mappings:
        c_id: c_id
  edges: []
"#;
        let config: GraphSchemaConfig = serde_yaml::from_str(yaml).expect("valid yaml");
        Arc::new(config.to_graph_schema().expect("valid schema"))
    }

    fn build_source_backed_schema() -> Arc<GraphSchema> {
        let yaml = "name: test_source\ngraph_schema:\n  nodes:\n    - label: ReadOnlyUser\n      database: test_db\n      table: readonly_users\n      node_id: user_id\n      source: \"s3://bucket/users.parquet\"\n      property_mappings:\n        user_id: user_id\n        name: full_name\n  edges:\n    - type: READ_ONLY_FOLLOWS\n      database: test_db\n      table: readonly_follows\n      from_node: ReadOnlyUser\n      to_node: ReadOnlyUser\n      from_id: follower_id\n      to_id: followed_id\n      source: \"s3://bucket/follows.parquet\"\n      property_mappings: {}\n";
        let config: GraphSchemaConfig = serde_yaml::from_str(yaml).expect("valid yaml");
        Arc::new(config.to_graph_schema().expect("valid schema"))
    }

    fn make_stub_db() -> Database {
        make_stub_db_with_schema(build_test_schema())
    }

    fn make_stub_db_with_schema(schema: Arc<GraphSchema>) -> Database {
        use async_trait::async_trait;
        use clickgraph::executor::{ExecutorError, QueryExecutor};
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
            remote_executor: None,
            schema,
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
            executor_kind: clickgraph::query_planner::write_guard::ExecutorKind::EmbeddedChdb,
        }
    }

    fn make_capturing_db(
        schema: Arc<GraphSchema>,
    ) -> (Database, Arc<std::sync::Mutex<Vec<String>>>) {
        use async_trait::async_trait;
        use clickgraph::executor::{ExecutorError, QueryExecutor};
        struct CapturingExecutor {
            captured: Arc<std::sync::Mutex<Vec<String>>>,
        }
        #[async_trait]
        impl QueryExecutor for CapturingExecutor {
            async fn execute_json(
                &self,
                sql: &str,
                _role: Option<&str>,
            ) -> Result<Vec<serde_json::Value>, ExecutorError> {
                self.captured.lock().unwrap().push(sql.to_string());
                // Phase 5d: write-counter probes (`SELECT count() AS n`)
                // are now part of the write path. The fake executor has
                // no underlying data, so we hand back a single-row `n=1`
                // response — that's the count the *legacy static-count*
                // approximation implied (one match per Delete / Update),
                // and it keeps the dispatch-focused unit tests below
                // pinning the same counter values they pinned pre-Phase-5d
                // without making them depend on a real chdb session.
                let trimmed = sql.trim_start();
                if trimmed.starts_with("SELECT count() AS n") {
                    return Ok(vec![serde_json::json!({"n": 1})]);
                }
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
        let db = Database {
            executor: Arc::new(CapturingExecutor {
                captured: Arc::clone(&captured),
            }),
            remote_executor: None,
            schema,
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
            executor_kind: clickgraph::query_planner::write_guard::ExecutorKind::EmbeddedChdb,
        };
        (db, captured)
    }

    fn make_error_db(schema: Arc<GraphSchema>) -> Database {
        use async_trait::async_trait;
        use clickgraph::executor::{ExecutorError, QueryExecutor};
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
            remote_executor: None,
            schema,
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
            executor_kind: clickgraph::query_planner::write_guard::ExecutorKind::EmbeddedChdb,
        }
    }

    #[test]
    fn test_query_to_sql_basic_match() {
        let db = make_stub_db();
        let conn = Connection::new(&db).unwrap();
        let sql = conn.query_to_sql("MATCH (u:User) RETURN u.name").unwrap();
        assert!(sql.contains("users") && sql.contains("full_name"));
    }

    #[test]
    fn test_query_to_sql_relationship() {
        let db = make_stub_db();
        let conn = Connection::new(&db).unwrap();
        let sql = conn
            .query_to_sql("MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.name, b.name")
            .unwrap();
        assert!(sql.contains("follows") && sql.contains("full_name"));
    }

    #[test]
    fn test_query_to_sql_parse_error() {
        let db = make_stub_db();
        let conn = Connection::new(&db).unwrap();
        assert!(conn.query_to_sql("NOT VALID CYPHER @@@@").is_err());
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
            .unwrap();
        assert!(sql.starts_with("INSERT INTO FUNCTION file('output.parquet', 'Parquet')"));
    }

    #[test]
    fn test_execute_sql_propagates_executor_errors() {
        let db = make_error_db(build_test_schema());
        let conn = Connection::new(&db).unwrap();
        assert!(matches!(
            conn.execute_sql("SELECT 1").unwrap_err(),
            EmbeddedError::Executor(_)
        ));
    }

    #[test]
    fn test_create_node_with_caller_provided_id() {
        let (db, captured) = make_capturing_db(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        let mut props = HashMap::new();
        props.insert("person_id".to_string(), Value::String("p1".to_string()));
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        assert_eq!(conn.create_node("Person", props).unwrap(), "p1");
        let sqls = captured.lock().unwrap();
        assert!(
            sqls[0].contains("INSERT INTO")
                && sqls[0].contains("full_name")
                && sqls[0].contains("'Alice'")
        );
    }

    #[test]
    fn test_create_node_unknown_property_returns_validation_error() {
        let db = make_stub_db_with_schema(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        let mut props = HashMap::new();
        props.insert("nonexistent".to_string(), Value::String("val".to_string()));
        assert!(matches!(
            conn.create_node("Person", props).unwrap_err(),
            EmbeddedError::Validation(_)
        ));
    }

    #[test]
    fn test_create_edge_generates_correct_insert() {
        let (db, captured) = make_capturing_db(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        let mut props = HashMap::new();
        props.insert("since".to_string(), Value::Int64(2020));
        assert!(conn.create_edge("KNOWS", "p1", "p2", props).is_ok());
        let sqls = captured.lock().unwrap();
        assert!(sqls[0].contains("from_person_id") && sqls[0].contains("since_year"));
    }

    #[test]
    fn test_upsert_node_without_id_returns_validation_error() {
        let db = make_stub_db_with_schema(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        assert!(conn
            .upsert_node("Person", props)
            .unwrap_err()
            .to_string()
            .contains("person_id"));
    }

    #[test]
    fn test_create_nodes_batch() {
        let (db, captured) = make_capturing_db(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        let mut row1 = HashMap::new();
        row1.insert("person_id".to_string(), Value::String("p1".to_string()));
        let mut row2 = HashMap::new();
        row2.insert("person_id".to_string(), Value::String("p2".to_string()));
        let ids = conn.create_nodes("Person", vec![row1, row2]).unwrap();
        assert_eq!(ids, vec!["p1", "p2"]);
        assert_eq!(captured.lock().unwrap().len(), 1);
    }

    // --- delete_nodes tests ---

    #[test]
    fn test_delete_nodes_generates_correct_sql() {
        let (db, captured) = make_capturing_db(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        let mut filters = HashMap::new();
        filters.insert("name".to_string(), Value::String("Alice".to_string()));
        assert!(conn.delete_nodes("Person", filters).is_ok());
        let sqls = captured.lock().unwrap();
        let sql = &sqls[0];
        assert!(sql.contains("DELETE FROM") && sql.contains("WHERE"));
        assert!(sql.contains("full_name") && sql.contains("'Alice'"));
    }

    #[test]
    fn test_delete_nodes_unknown_label() {
        let db = make_stub_db_with_schema(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        let mut f = HashMap::new();
        f.insert("name".to_string(), Value::String("x".to_string()));
        assert!(conn
            .delete_nodes("NonExistent", f)
            .unwrap_err()
            .to_string()
            .contains("NonExistent"));
    }

    #[test]
    fn test_delete_nodes_source_backed() {
        let db = make_stub_db_with_schema(build_source_backed_schema());
        let conn = Connection::new(&db).unwrap();
        let mut f = HashMap::new();
        f.insert("name".to_string(), Value::String("x".to_string()));
        assert!(conn
            .delete_nodes("ReadOnlyUser", f)
            .unwrap_err()
            .to_string()
            .contains("source-backed"));
    }

    #[test]
    fn test_delete_nodes_unknown_filter_key() {
        let db = make_stub_db_with_schema(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        let mut f = HashMap::new();
        f.insert("bad_key".to_string(), Value::String("x".to_string()));
        assert!(conn
            .delete_nodes("Person", f)
            .unwrap_err()
            .to_string()
            .contains("bad_key"));
    }

    // --- delete_edges tests ---

    #[test]
    fn test_delete_edges_with_from_to_id() {
        let (db, captured) = make_capturing_db(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        let mut f = HashMap::new();
        f.insert("from_id".to_string(), Value::String("p1".to_string()));
        f.insert("to_id".to_string(), Value::String("p2".to_string()));
        assert!(conn.delete_edges("KNOWS", f).is_ok());
        let sqls = captured.lock().unwrap();
        assert!(sqls[0].contains("from_person_id") && sqls[0].contains("to_person_id"));
    }

    #[test]
    fn test_delete_edges_source_backed() {
        let db = make_stub_db_with_schema(build_source_backed_schema());
        let conn = Connection::new(&db).unwrap();
        let mut f = HashMap::new();
        f.insert("from_id".to_string(), Value::String("p1".to_string()));
        assert!(conn
            .delete_edges("READ_ONLY_FOLLOWS", f)
            .unwrap_err()
            .to_string()
            .contains("source-backed"));
    }

    #[test]
    fn test_delete_edges_combined_filters() {
        let (db, captured) = make_capturing_db(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        let mut f = HashMap::new();
        f.insert("from_id".to_string(), Value::String("p1".to_string()));
        f.insert("to_id".to_string(), Value::String("p2".to_string()));
        f.insert("since".to_string(), Value::Int64(2020));
        assert!(conn.delete_edges("KNOWS", f).is_ok());
        let sqls = captured.lock().unwrap();
        assert!(
            sqls[0].contains("from_person_id")
                && sqls[0].contains("since_year")
                && sqls[0].contains("AND")
        );
    }

    // --- import_json tests ---

    #[test]
    fn test_import_json_maps_keys() {
        let (db, captured) = make_capturing_db(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        let json_data = "{\"person_id\": \"p1\", \"name\": \"Alice\"}\n{\"person_id\": \"p2\", \"name\": \"Bob\"}";
        // import_json returns () — verify it succeeds without error
        conn.import_json("Person", json_data).unwrap();
        let sqls = captured.lock().unwrap();
        assert!(sqls[0].contains("FORMAT JSONEachRow") && sqls[0].contains("full_name"));
    }

    #[test]
    fn test_import_json_unknown_label() {
        let db = make_stub_db_with_schema(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        assert!(conn.import_json("NonExistent", "{}").is_err());
    }

    #[test]
    fn test_import_json_source_backed() {
        let db = make_stub_db_with_schema(build_source_backed_schema());
        let conn = Connection::new(&db).unwrap();
        assert!(conn
            .import_json("ReadOnlyUser", "{\"name\":\"x\"}")
            .unwrap_err()
            .to_string()
            .contains("source-backed"));
    }

    #[test]
    fn test_import_json_unknown_keys_skipped() {
        let (db, captured) = make_capturing_db(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        assert!(conn
            .import_json(
                "Person",
                "{\"person_id\":\"p1\",\"name\":\"Alice\",\"unknown\":\"x\"}"
            )
            .is_ok());
        let sqls = captured.lock().unwrap();
        assert!(!sqls[0].contains("unknown") && sqls[0].contains("full_name"));
    }

    // --- import_json_file tests ---

    #[test]
    fn test_import_json_file_nonexistent() {
        let db = make_stub_db_with_schema(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        assert!(matches!(
            conn.import_json_file("Person", "/nonexistent/data.json")
                .unwrap_err(),
            EmbeddedError::Io(_)
        ));
    }

    #[test]
    fn test_import_json_file_generates_sql() {
        let (db, captured) = make_capturing_db(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        let tmp = tempfile::NamedTempFile::new().unwrap();
        assert!(conn
            .import_json_file("Person", tmp.path().to_str().unwrap())
            .is_ok());
        let sqls = captured.lock().unwrap();
        assert!(
            sqls[0].contains("INSERT INTO")
                && sqls[0].contains("FROM file(")
                && sqls[0].contains("JSONEachRow")
        );
    }

    #[test]
    fn test_import_json_file_source_backed() {
        let db = make_stub_db_with_schema(build_source_backed_schema());
        let conn = Connection::new(&db).unwrap();
        let tmp = tempfile::NamedTempFile::new().unwrap();
        assert!(conn
            .import_json_file("ReadOnlyUser", tmp.path().to_str().unwrap())
            .unwrap_err()
            .to_string()
            .contains("source-backed"));
    }

    // --- import_csv_file / import_parquet_file / import_file tests ---

    #[test]
    fn test_import_csv_file_generates_sql() {
        let (db, captured) = make_capturing_db(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        let tmp = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
        conn.import_csv_file("Person", tmp.path().to_str().unwrap())
            .unwrap();
        let sqls = captured.lock().unwrap();
        assert!(
            sqls[0].contains("INSERT INTO") && sqls[0].contains("CSVWithNames"),
            "SQL: {}",
            sqls[0]
        );
    }

    #[test]
    fn test_import_parquet_file_generates_sql() {
        let (db, captured) = make_capturing_db(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        let tmp = tempfile::Builder::new()
            .suffix(".parquet")
            .tempfile()
            .unwrap();
        conn.import_parquet_file("Person", tmp.path().to_str().unwrap())
            .unwrap();
        let sqls = captured.lock().unwrap();
        assert!(
            sqls[0].contains("INSERT INTO") && sqls[0].contains("Parquet"),
            "SQL: {}",
            sqls[0]
        );
    }

    #[test]
    fn test_import_file_auto_detect_csv() {
        let (db, captured) = make_capturing_db(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        let tmp = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
        conn.import_file("Person", tmp.path().to_str().unwrap())
            .unwrap();
        let sqls = captured.lock().unwrap();
        assert!(sqls[0].contains("CSVWithNames"), "SQL: {}", sqls[0]);
    }

    #[test]
    fn test_import_file_auto_detect_parquet() {
        let (db, captured) = make_capturing_db(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        let tmp = tempfile::Builder::new()
            .suffix(".parquet")
            .tempfile()
            .unwrap();
        conn.import_file("Person", tmp.path().to_str().unwrap())
            .unwrap();
        let sqls = captured.lock().unwrap();
        assert!(sqls[0].contains("Parquet"), "SQL: {}", sqls[0]);
    }

    #[test]
    fn test_import_file_unknown_extension() {
        let db = make_stub_db_with_schema(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        let tmp = tempfile::Builder::new().suffix(".xyz").tempfile().unwrap();
        let err = conn
            .import_file("Person", tmp.path().to_str().unwrap())
            .unwrap_err();
        assert!(
            err.to_string().contains("Cannot determine import format"),
            "Error: {}",
            err
        );
    }

    // --- remote executor tests ---

    #[test]
    fn test_query_remote_without_config_returns_error() {
        let db = make_stub_db();
        let conn = Connection::new(&db).unwrap();
        let err = conn
            .query_remote("MATCH (u:User) RETURN u.name")
            .unwrap_err();
        assert!(
            err.to_string().contains("No remote executor configured"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn test_query_remote_graph_without_config_returns_error() {
        let db = make_stub_db();
        let conn = Connection::new(&db).unwrap();
        let err = conn
            .query_remote_graph("MATCH (u:User) RETURN u.name")
            .unwrap_err();
        assert!(
            err.to_string().contains("No remote executor configured"),
            "unexpected error: {}",
            err
        );
    }

    // --- store_subgraph tests ---

    #[test]
    fn test_store_subgraph_empty_graph() {
        let db = make_stub_db_with_schema(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        let graph = GraphResult::empty();
        let stats = conn.store_subgraph(&graph).unwrap();
        assert_eq!(stats.nodes_stored, 0);
        assert_eq!(stats.edges_stored, 0);
    }

    #[test]
    fn test_store_subgraph_nodes_only() {
        use crate::graph_result::{GraphNode, GraphResultBuilder};
        let (db, captured) = make_capturing_db(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();

        let mut builder = GraphResultBuilder::new();
        let mut props = HashMap::new();
        props.insert("person_id".to_string(), Value::String("p1".to_string()));
        props.insert("name".to_string(), Value::String("Alice".to_string()));
        builder.add_node(GraphNode {
            id: "Person:p1".to_string(),
            labels: vec!["Person".to_string()],
            properties: props,
        });
        let graph = builder.build();
        let stats = conn.store_subgraph(&graph).unwrap();
        assert_eq!(stats.nodes_stored, 1);
        assert_eq!(stats.edges_stored, 0);

        let sqls = captured.lock().unwrap();
        assert!(sqls[0].contains("INSERT INTO"), "SQL: {}", sqls[0]);
    }

    #[test]
    fn test_store_subgraph_edges() {
        use crate::graph_result::{GraphEdge, GraphResultBuilder};
        let (db, captured) = make_capturing_db(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();

        let mut builder = GraphResultBuilder::new();
        builder.add_edge(GraphEdge {
            id: "KNOWS:p1:p2".to_string(),
            type_name: "KNOWS".to_string(),
            from_id: "Person:p1".to_string(),
            to_id: "Person:p2".to_string(),
            properties: HashMap::new(),
        });
        let graph = builder.build();
        let stats = conn.store_subgraph(&graph).unwrap();
        assert_eq!(stats.nodes_stored, 0);
        assert_eq!(stats.edges_stored, 1);

        let sqls = captured.lock().unwrap();
        assert!(
            sqls[0].contains("from_person_id") && sqls[0].contains("to_person_id"),
            "SQL: {}",
            sqls[0]
        );
    }

    // --- SQL injection tests ---

    #[test]
    fn test_delete_filter_values_escaped() {
        let (db, captured) = make_capturing_db(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        let mut f = HashMap::new();
        f.insert(
            "name".to_string(),
            Value::String("'; DROP TABLE users; --".to_string()),
        );
        assert!(conn.delete_nodes("Person", f).is_ok());
        let sqls = captured.lock().unwrap();
        // The single quote is escaped to '' so ClickHouse treats it as a string literal
        assert!(
            sqls[0].contains("''"),
            "single quote should be escaped: {}",
            sqls[0]
        );
        // The value is safely inside a string literal, not executable SQL
        assert!(
            sqls[0].contains("''';"),
            "escaped quote should precede semicolon: {}",
            sqls[0]
        );
    }

    // -----------------------------------------------------------------------
    // Cypher write dispatch (Phase 3)
    // -----------------------------------------------------------------------

    #[test]
    fn cypher_create_routes_to_write_path_and_emits_insert() {
        let (db, captured) = make_capturing_db(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        let mut result = conn
            .query("CREATE (a:Person {person_id: 'p1', name: 'Alice'})")
            .expect("CREATE should succeed via write dispatch");

        // Counters surface as a single-row QueryResult per Decision 0.8.
        assert_eq!(result.get_column_names().len(), 4);
        assert_eq!(result.num_rows(), 1);
        let row = result.next().unwrap();
        assert_eq!(row.get("nodes_created").unwrap().as_i64(), Some(1));

        let sqls = captured.lock().unwrap();
        assert_eq!(sqls.len(), 1, "expected one INSERT, got {:?}", sqls);
        assert!(
            sqls[0].starts_with("INSERT INTO"),
            "expected INSERT, got: {}",
            sqls[0]
        );
        // No SETTINGS at query time per Decision 0.7.
        assert!(
            !sqls[0].to_lowercase().contains("settings"),
            "must not emit SETTINGS at query time, got: {}",
            sqls[0]
        );
    }

    /// `find_first_with_prefix` lets dispatch tests probe the captured
    /// SQL stream without depending on the exact ordering between
    /// count-probes (Phase 5d) and the lightweight DELETE/UPDATE itself.
    fn find_first_with_prefix<'a>(sqls: &'a [String], prefix: &str) -> Option<&'a String> {
        sqls.iter().find(|s| s.starts_with(prefix))
    }

    #[test]
    fn cypher_set_routes_to_write_path_and_emits_update() {
        let (db, captured) = make_capturing_db(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        let result = conn
            .query("MATCH (a:Person) WHERE a.person_id = 'p1' SET a.name = 'Bob'")
            .expect("SET should succeed via write dispatch");
        let mut iter = result.into_iter();
        let row = iter.next().unwrap();
        // CapturingExecutor returns `n=1` for count probes (one match
        // per write op), so properties_set = 1 assignment * 1 row.
        assert_eq!(row.get("properties_set").unwrap().as_i64(), Some(1));

        let sqls = captured.lock().unwrap();
        let update = find_first_with_prefix(&sqls, "UPDATE")
            .unwrap_or_else(|| panic!("expected UPDATE in captured SQL, got: {:?}", *sqls));
        // Lightweight UPDATE — no mutations_sync.
        assert!(
            !update.to_lowercase().contains("mutations_sync"),
            "must not emit mutations_sync, got: {}",
            update
        );
    }

    #[test]
    fn cypher_delete_routes_to_write_path_and_emits_delete() {
        let (db, captured) = make_capturing_db(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        let result = conn
            .query("MATCH (a:Person) WHERE a.person_id = 'p1' DELETE a")
            .expect("DELETE should succeed via write dispatch");
        let mut iter = result.into_iter();
        let row = iter.next().unwrap();
        // CapturingExecutor's count probe returns `n=1` → nodes_deleted=1.
        assert_eq!(row.get("nodes_deleted").unwrap().as_i64(), Some(1));

        let sqls = captured.lock().unwrap();
        find_first_with_prefix(&sqls, "DELETE FROM")
            .unwrap_or_else(|| panic!("expected DELETE FROM in captured SQL, got: {:?}", *sqls));
    }

    #[test]
    fn cypher_detach_delete_emits_rel_then_node_delete_sequence() {
        let (db, captured) = make_capturing_db(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        let result = conn
            .query("MATCH (a:Person) WHERE a.person_id = 'p1' DETACH DELETE a")
            .expect("DETACH DELETE should succeed");
        let mut iter = result.into_iter();
        let row = iter.next().unwrap();
        // KNOWS touches Person on both sides → 2 rel-cleanup deletes,
        // then 1 node delete. With the per-op `n=1` mock, that's
        // relationships_deleted >= 1 and nodes_deleted = 1.
        assert!(row.get("relationships_deleted").unwrap().as_i64().unwrap() >= 1);
        assert_eq!(row.get("nodes_deleted").unwrap().as_i64(), Some(1));

        let sqls = captured.lock().unwrap();
        // Last DELETE statement is the node delete (rel cleanups precede).
        let last_delete = sqls
            .iter()
            .rev()
            .find(|s| s.starts_with("DELETE FROM"))
            .unwrap_or_else(|| panic!("expected DELETE in captured SQL, got: {:?}", *sqls));
        assert!(
            last_delete.contains("`persons`") || last_delete.contains("`person`"),
            "node DELETE must come last, got: {}",
            last_delete
        );
    }

    #[test]
    fn cypher_remove_routes_to_write_path() {
        let (db, captured) = make_capturing_db(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        conn.query("MATCH (a:Person) WHERE a.person_id = 'p1' REMOVE a.name")
            .expect("REMOVE should succeed");
        let sqls = captured.lock().unwrap();
        let update = find_first_with_prefix(&sqls, "UPDATE").unwrap_or_else(|| {
            panic!(
                "REMOVE should emit an UPDATE setting NULL, got: {:?}",
                *sqls
            )
        });
        assert!(
            update.contains("SET `full_name` = NULL"),
            "REMOVE should emit SET … = NULL, got: {}",
            update
        );
    }

    #[test]
    fn cypher_write_rejected_in_sql_only_mode() {
        // `from_executor` tags the Database as SqlOnly. Writes must reject.
        let schema = build_writable_test_schema();
        let db = Database::from_executor(
            schema,
            Arc::new({
                use async_trait::async_trait;
                use clickgraph::executor::{ExecutorError, QueryExecutor};
                struct Stub;
                #[async_trait]
                impl QueryExecutor for Stub {
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
                Stub
            }),
        )
        .unwrap();
        let conn = Connection::new(&db).unwrap();
        let err = conn
            .query("CREATE (a:Person {person_id: 'p1'})")
            .expect_err("must reject write in sql_only mode");
        let msg = err.to_string();
        assert!(
            msg.contains("embedded chdb mode") || msg.contains("EmbeddedChdb"),
            "expected executor-rejection error, got: {}",
            msg
        );
    }

    /// `CREATE` with leading line comments must still route to the write
    /// path. Regression for the dispatch parser silently bypassing writes
    /// when the input contained comments.
    #[test]
    fn cypher_create_with_comments_routes_to_write_path() {
        let (db, captured) = make_capturing_db(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        let cypher = "// leading comment\n/* block comment */\nCREATE (a:Person {person_id: 'p1'})";
        let result = conn
            .query(cypher)
            .expect("CREATE with comments must route to write dispatch");
        let mut iter = result.into_iter();
        let row = iter.next().unwrap();
        assert_eq!(row.get("nodes_created").unwrap().as_i64(), Some(1));

        let sqls = captured.lock().unwrap();
        assert!(
            sqls[0].starts_with("INSERT INTO"),
            "comment-prefixed CREATE must still emit INSERT, got: {}",
            sqls[0]
        );
    }

    /// `MATCH … CREATE` (read query feeding a write) must enter the write
    /// pipeline rather than silently fall through to the read path. We pin
    /// only that dispatch reaches the write pipeline and produces a
    /// counter-shaped `QueryResult` — exact semantics for MATCH-driven
    /// CREATE may keep evolving.
    #[test]
    fn cypher_match_create_routes_to_write_path() {
        let (db, _captured) = make_capturing_db(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        let result = conn
            .query("MATCH (a:Person) CREATE (b:Person {person_id: 'p2'})")
            .expect("MATCH … CREATE must reach write pipeline");
        // Counter-shape ensures we routed to handle_write_async; if it
        // had fallen through to the read path, the QueryResult columns
        // would not be the four counter columns.
        let cols = result.get_column_names();
        assert_eq!(
            cols.len(),
            4,
            "expected counter result from write path, got cols={:?}",
            cols
        );
        assert!(cols.contains(&"nodes_created".to_string()));
    }

    /// Phase 5e: `MATCH (n) DELETE n` over a schema with multiple writable
    /// node labels must fan out into one DELETE per node table (and one
    /// `count()` probe per DELETE for accurate counters), not pick a
    /// single label via `find_alias_label` and silently leave the other
    /// tables intact.
    #[test]
    fn cypher_untyped_match_delete_fans_out_across_node_tables() {
        let (db, captured) = make_capturing_db(build_multi_label_writable_schema());
        let conn = Connection::new(&db).unwrap();
        conn.query("MATCH (n) DELETE n")
            .expect("untyped DELETE should succeed via Phase 5e fan-out");

        let sqls = captured.lock().unwrap();
        // Three labels in the schema → three DELETEs, each targeting a
        // distinct node table.
        let deletes: Vec<&String> = sqls
            .iter()
            .filter(|s| s.starts_with("DELETE FROM"))
            .collect();
        assert_eq!(
            deletes.len(),
            3,
            "expected one DELETE per node label (3), got {} of {:?}",
            deletes.len(),
            *sqls
        );
        for table in ["`a_nodes`", "`b_nodes`", "`c_nodes`"] {
            assert!(
                deletes.iter().any(|s| s.contains(table)),
                "no DELETE targeting {table}, got: {deletes:?}"
            );
        }

        // Each DELETE is preceded by a `count()` probe so the per-table
        // counter is accurate (Phase 5d) — three probes total here.
        let probes: Vec<&String> = sqls
            .iter()
            .filter(|s| s.starts_with("SELECT count() AS n"))
            .collect();
        assert_eq!(
            probes.len(),
            3,
            "expected one count probe per DELETE, got: {:?}",
            *sqls
        );
    }

    /// Phase 5e: `MATCH (n) SET n.k = 'v'` likewise fans out to one
    /// UPDATE per node table when `n` is bound across multiple labels.
    /// The schema below maps the property `k` to the same column name
    /// on every label so the UPDATE is renderable on each table.
    #[test]
    fn cypher_untyped_match_set_fans_out_across_node_tables() {
        // Custom schema: same property key (`k`) writable on three
        // labels, each with a distinct ID column.
        let yaml = r#"name: test_multi_label_set
graph_schema:
  nodes:
    - label: A
      database: test_db
      table: a_nodes
      node_id: a_id
      property_mappings:
        a_id: a_id
        k: k_col
    - label: B
      database: test_db
      table: b_nodes
      node_id: b_id
      property_mappings:
        b_id: b_id
        k: k_col
    - label: C
      database: test_db
      table: c_nodes
      node_id: c_id
      property_mappings:
        c_id: c_id
        k: k_col
  edges: []
"#;
        let config: GraphSchemaConfig = serde_yaml::from_str(yaml).expect("valid yaml");
        let schema = Arc::new(config.to_graph_schema().expect("valid schema"));
        let (db, captured) = make_capturing_db(schema);
        let conn = Connection::new(&db).unwrap();
        conn.query("MATCH (n) SET n.k = 'v'")
            .expect("untyped SET should succeed via Phase 5e fan-out");

        let sqls = captured.lock().unwrap();
        let updates: Vec<&String> = sqls.iter().filter(|s| s.starts_with("UPDATE")).collect();
        assert_eq!(
            updates.len(),
            3,
            "expected one UPDATE per node label (3), got {} of {:?}",
            updates.len(),
            *sqls
        );
        for table in ["`a_nodes`", "`b_nodes`", "`c_nodes`"] {
            assert!(
                updates.iter().any(|s| s.contains(table)),
                "no UPDATE targeting {table}, got: {updates:?}"
            );
        }
    }

    /// Phase 5d: `OPTIONAL MATCH … DELETE … RETURN` must run the write
    /// pipeline, then re-run the read pipeline and surface the user-visible
    /// row payload plus the side-effect counters via the new
    /// `QueryResult::get_write_counters()` side-channel. The pure-write
    /// 4-column synthetic counter row is *not* used in this path — the
    /// row payload comes from the read pipeline.
    ///
    /// We pin dispatch-shape (side-channel populated, read columns
    /// surfaced, both write and read SQL reach the executor) here. End-
    /// to-end counter accuracy on an empty graph (the actual TCK
    /// `Delete1` [5] expectation of zero side effects) is exercised
    /// against a real chdb session in `clickgraph-tck`.
    #[test]
    fn cypher_delete_with_return_executes_writes_and_runs_read_pipeline() {
        let (db, captured) = make_capturing_db(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        let result = conn
            .query("OPTIONAL MATCH (a:Person) DELETE a RETURN a")
            .expect("DELETE … RETURN must succeed via Phase 5d write+RETURN path");

        // Side-channel must be populated; the pure-write synthetic row
        // must NOT be the surfaced row payload.
        let counters = result
            .get_write_counters()
            .cloned()
            .expect("write+RETURN must populate get_write_counters()");
        let cols = result.get_column_names().to_vec();
        assert!(
            !cols.iter().any(|c| c == "nodes_created"),
            "write+RETURN columns must come from the read pipeline, \
             not the synthetic counter row; got {:?}",
            cols
        );
        // The four canonical counters must all be present in the side-
        // channel map even when zero, so downstream consumers (e.g. the
        // TCK harness) can read every key without `Option` juggling.
        for key in [
            "nodes_created",
            "properties_set",
            "nodes_deleted",
            "relationships_deleted",
        ] {
            assert!(
                counters.contains_key(key),
                "side-channel must include `{key}`, got: {counters:?}"
            );
        }

        // Both the write (DELETE) and the re-run read pipeline (SELECT)
        // must reach the executor; the count-probe (`SELECT count() AS n`)
        // also runs alongside the DELETE per Phase 5d's accurate-counters
        // pass.
        let sqls = captured.lock().unwrap();
        assert!(
            sqls.iter().any(|s| s.starts_with("DELETE FROM")),
            "write+RETURN must emit a DELETE statement, got: {:?}",
            *sqls
        );
        assert!(
            sqls.iter().any(|s| s.starts_with("SELECT count() AS n")),
            "write+RETURN must probe the count for accurate counters, got: {:?}",
            *sqls
        );
        assert!(
            sqls.iter()
                .any(|s| s.to_uppercase().contains("SELECT")
                    && !s.starts_with("SELECT count() AS n")),
            "write+RETURN must run the read pipeline (non-probe SELECT), got: {:?}",
            *sqls
        );
    }

    /// `CREATE … RETURN` likewise must enter the write pipeline. Phase 5d
    /// supports DELETE/SET/REMOVE + RETURN via re-running the read pipeline,
    /// but CREATE + RETURN remains rejected up-front because the alias
    /// bound by CREATE has no MATCH-able identity for the read pipeline to
    /// reference; rejecting before any execution keeps the database
    /// unchanged on failure rather than leaving a partial INSERT.
    #[test]
    fn cypher_create_with_return_rejected_before_insert() {
        let (db, captured) = make_capturing_db(build_writable_test_schema());
        let conn = Connection::new(&db).unwrap();
        let err = conn
            .query("CREATE (a:Person {person_id: 'p3'}) RETURN a")
            .expect_err("CREATE … RETURN must surface a write-pipeline error");
        let msg = err.to_string();
        assert!(
            msg.contains("not supported") && msg.to_lowercase().contains("create"),
            "expected write-pipeline rejection naming CREATE, got: {}",
            msg
        );
        // No INSERT must have been emitted: rejection happens before exec.
        let sqls = captured.lock().unwrap();
        assert!(
            !sqls.iter().any(|s| s.to_uppercase().contains("INSERT")),
            "CREATE … RETURN rejection must not emit any INSERT, got: {:?}",
            *sqls
        );
    }
}
