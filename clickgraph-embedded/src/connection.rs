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
    pub fn query(&self, cypher: &str) -> Result<QueryResult, EmbeddedError> {
        self.db.runtime.block_on(self.query_async(cypher))
    }

    /// Execute a Cypher query and return the generated SQL without executing it.
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
    pub fn delete_nodes(
        &self,
        label: &str,
        filters: HashMap<String, Value>,
    ) -> Result<u64, EmbeddedError> {
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
        Ok(0)
    }

    /// Delete edges matching the given type and filter criteria.
    pub fn delete_edges(
        &self,
        edge_type: &str,
        filters: HashMap<String, Value>,
    ) -> Result<u64, EmbeddedError> {
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
        Ok(0)
    }

    /// Import nodes from inline newline-delimited JSON (JSONEachRow format).
    pub fn import_json(&self, label: &str, json_lines: &str) -> Result<u64, EmbeddedError> {
        let node_schema = self.get_node_schema(label)?;
        write_helpers::check_writable(&node_schema.source, label)?;
        let id_columns = node_schema.node_id.id.columns();
        let id_col_strs: Vec<&str> = id_columns.iter().copied().collect();
        let property_mappings =
            write_helpers::extract_property_mappings(&node_schema.property_mappings);
        let (transformed_json, line_count) =
            write_helpers::transform_json_keys(json_lines, &property_mappings, &id_col_strs)?;
        if line_count == 0 {
            return Ok(0);
        }
        let sql = format!(
            "INSERT INTO `{}`.`{}` FORMAT JSONEachRow\n{}",
            node_schema.database, node_schema.table_name, transformed_json
        );
        self.execute_sql(&sql)?;
        Ok(line_count as u64)
    }

    /// Import nodes from a JSON file (JSONEachRow format).
    pub fn import_json_file(&self, label: &str, file_path: &str) -> Result<u64, EmbeddedError> {
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
            &property_mappings,
            &id_col_strs,
        );
        self.execute_sql(&sql)?;
        Ok(0)
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
        let yaml = "name: test\ngraph_schema:\n  nodes:\n    - label: User\n      database: test_db\n      table: users\n      node_id: user_id\n      property_mappings:\n        user_id: user_id\n        name: full_name\n  edges:\n    - type: FOLLOWS\n      database: test_db\n      table: follows\n      from_node: User\n      to_node: User\n      from_id: follower_id\n      to_id: followed_id\n      property_mappings: {}\n";
        let config: GraphSchemaConfig = serde_yaml::from_str(yaml).expect("valid yaml");
        Arc::new(config.to_graph_schema().expect("valid schema"))
    }

    fn build_writable_test_schema() -> Arc<GraphSchema> {
        let yaml = "name: test_writable\ngraph_schema:\n  nodes:\n    - label: Person\n      database: test_db\n      table: persons\n      node_id: person_id\n      property_mappings:\n        person_id: person_id\n        name: full_name\n        age: age\n  edges:\n    - type: KNOWS\n      database: test_db\n      table: knows\n      from_node: Person\n      to_node: Person\n      from_id: from_person_id\n      to_id: to_person_id\n      property_mappings:\n        since: since_year\n";
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
            schema,
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
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
        let db = Database {
            executor: Arc::new(CapturingExecutor {
                captured: Arc::clone(&captured),
            }),
            schema,
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
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
        assert_eq!(conn.delete_nodes("Person", filters).unwrap(), 0);
        let sqls = captured.lock().unwrap();
        let sql = &sqls[0];
        assert!(sql.contains("ALTER TABLE") && sql.contains("DELETE WHERE"));
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
        assert_eq!(conn.import_json("Person", json_data).unwrap(), 2);
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
}
