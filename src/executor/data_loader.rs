//! Data loader for the chdb embedded executor.
//!
//! At startup (when using `ChdbExecutor`), this module iterates over all node and
//! relationship schemas that have a `source:` field set and issues
//! `CREATE OR REPLACE VIEW {database}.{table} AS SELECT * FROM {resolved_fn}`
//! in the chdb session.
//!
//! Because the SQL generator always references tables by their schema-defined names
//! (`database.table`), no changes are needed downstream — the VIEWs make the data
//! transparently accessible.

use crate::graph_catalog::graph_schema::GraphSchema;

use super::chdb_embedded::ChdbExecutor;
use super::source_resolver::resolve_source_uri;
use super::ExecutorError;

/// Create chdb VIEWs for every schema entry that has a `source:` URI.
///
/// Idempotent: uses `CREATE OR REPLACE VIEW` so it is safe to call on reconnect.
///
/// Returns the number of views created.
pub fn load_schema_sources(
    executor: &ChdbExecutor,
    schema: &GraphSchema,
) -> Result<usize, ExecutorError> {
    let mut count = 0;

    // Process node schemas
    for node_schema in schema.all_node_schemas().values() {
        if let Some(source_uri) = &node_schema.source {
            let table_fn = resolve_source_uri(source_uri).map_err(|e| {
                ExecutorError::QueryFailed(format!(
                    "Invalid source URI for node '{}': {}",
                    node_schema.table_name, e
                ))
            })?;

            let view_sql = format!(
                "CREATE OR REPLACE VIEW `{}`.`{}` AS SELECT * FROM {}",
                node_schema.database, node_schema.table_name, table_fn
            );

            log::info!(
                "Creating chdb VIEW for node '{}': {}",
                node_schema.table_name,
                view_sql
            );

            executor.execute_blocking_ddl(&view_sql)?;
            count += 1;
        }
    }

    // Process relationship schemas
    for rel_schema in schema.get_relationships_schemas().values() {
        if let Some(source_uri) = &rel_schema.source {
            let table_fn = resolve_source_uri(source_uri).map_err(|e| {
                ExecutorError::QueryFailed(format!(
                    "Invalid source URI for relationship '{}': {}",
                    rel_schema.table_name, e
                ))
            })?;

            let view_sql = format!(
                "CREATE OR REPLACE VIEW `{}`.`{}` AS SELECT * FROM {}",
                rel_schema.database, rel_schema.table_name, table_fn
            );

            log::info!(
                "Creating chdb VIEW for relationship '{}': {}",
                rel_schema.table_name,
                view_sql
            );

            executor.execute_blocking_ddl(&view_sql)?;
            count += 1;
        }
    }

    log::info!("Created {} chdb VIEW(s) from schema source: entries", count);
    Ok(count)
}
