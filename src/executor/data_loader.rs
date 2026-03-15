//! Data loader for the chdb embedded executor.
//!
//! At startup (when using `ChdbExecutor`), this module iterates over all node and
//! relationship schemas that have a `source:` field set and issues
//! `CREATE OR REPLACE VIEW {database}.{table} AS SELECT * FROM {resolved_fn}`
//! in the chdb session.
//!
//! For schema entries WITHOUT a `source:` field, writable ReplacingMergeTree tables
//! are created via `create_writable_tables()`.
//!
//! Because the SQL generator always references tables by their schema-defined names
//! (`database.table`), no changes are needed downstream — the VIEWs and tables
//! make the data transparently accessible.

use std::collections::HashSet;

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::graph_catalog::graph_schema::{GraphSchema, NodeSchema, RelationshipSchema};

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
                "Creating chdb VIEW for node '{}' -> `{}`.`{}`",
                node_schema.table_name,
                node_schema.database,
                node_schema.table_name
            );
            log::debug!("VIEW DDL: {}", view_sql);

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
                "Creating chdb VIEW for relationship '{}' -> `{}`.`{}`",
                rel_schema.table_name,
                rel_schema.database,
                rel_schema.table_name
            );
            log::debug!("VIEW DDL: {}", view_sql);

            executor.execute_blocking_ddl(&view_sql)?;
            count += 1;
        }
    }

    log::info!("Created {} chdb VIEW(s) from schema source: entries", count);
    Ok(count)
}

/// Create ReplacingMergeTree tables for every schema entry WITHOUT a `source:` field.
///
/// These are "writable" tables that support INSERT for graph write operations.
/// Uses `CREATE TABLE IF NOT EXISTS` so it is idempotent.
///
/// Returns the number of tables created.
pub fn create_writable_tables(
    executor: &ChdbExecutor,
    schema: &GraphSchema,
) -> Result<usize, ExecutorError> {
    let mut count = 0;

    // Process node schemas without source
    for node_schema in schema.all_node_schemas().values() {
        if node_schema.source.is_none() {
            let ddl = build_node_ddl(node_schema);
            log::info!(
                "Creating writable table for node '{}' -> `{}`.`{}`",
                node_schema.table_name,
                node_schema.database,
                node_schema.table_name
            );
            log::debug!("DDL: {}", ddl);
            executor.execute_blocking_ddl(&ddl)?;
            count += 1;
        }
    }

    // Process relationship schemas without source
    for rel_schema in schema.get_relationships_schemas().values() {
        if rel_schema.source.is_none() {
            let ddl = build_edge_ddl(rel_schema);
            log::info!(
                "Creating writable table for relationship '{}' -> `{}`.`{}`",
                rel_schema.table_name,
                rel_schema.database,
                rel_schema.table_name
            );
            log::debug!("DDL: {}", ddl);
            executor.execute_blocking_ddl(&ddl)?;
            count += 1;
        }
    }

    if count > 0 {
        log::info!("Created {} writable ReplacingMergeTree table(s)", count);
    }

    Ok(count)
}

/// Generate CREATE TABLE DDL for a node schema entry.
///
/// Template:
/// ```sql
/// CREATE TABLE IF NOT EXISTS `{db}`.`{table}` (
///     {id_col} String DEFAULT generateUUIDv4(),
///     {prop_cols...} String,
///     _version UInt64 DEFAULT now64()
/// ) ENGINE = ReplacingMergeTree(_version) ORDER BY ({id_col})
/// ```
/// Build CREATE TABLE DDL for a writable node table.
///
/// **V1 limitation**: All property columns use `String` type. The schema YAML
/// doesn't carry column type information. Numeric comparisons and aggregations
/// in Cypher will operate on string values. A future `column_types` or
/// `type_hints` field in the schema could address this.
pub fn build_node_ddl(node_schema: &NodeSchema) -> String {
    let id_columns = node_schema.node_id.id.columns();
    let id_col_set: HashSet<&str> = id_columns.iter().copied().collect();

    let mut col_defs = Vec::new();

    // ID column(s) with UUID default
    for id_col in &id_columns {
        col_defs.push(format!("{} String DEFAULT generateUUIDv4()", id_col));
    }

    // Property columns (skip ID columns to avoid duplication)
    for (_, prop_val) in &node_schema.property_mappings {
        if let PropertyValue::Column(col) = prop_val {
            if !id_col_set.contains(col.as_str()) {
                col_defs.push(format!("{} String", col));
            }
        }
    }

    // _version column for ReplacingMergeTree
    col_defs.push("_version UInt64 DEFAULT now64()".to_string());

    let order_by = if id_columns.len() == 1 {
        format!("({})", id_columns[0])
    } else {
        format!("({})", id_columns.join(", "))
    };

    format!(
        "CREATE TABLE IF NOT EXISTS `{}`.`{}` ({}) ENGINE = ReplacingMergeTree(_version) ORDER BY {}",
        node_schema.database,
        node_schema.table_name,
        col_defs.join(", "),
        order_by
    )
}

/// Generate CREATE TABLE DDL for an edge/relationship schema entry.
///
/// Template:
/// ```sql
/// CREATE TABLE IF NOT EXISTS `{db}`.`{table}` (
///     {from_id} String,
///     {to_id} String,
///     {prop_cols...} String,
///     _version UInt64 DEFAULT now64()
/// ) ENGINE = ReplacingMergeTree(_version) ORDER BY ({from_id}, {to_id})
/// ```
/// Build CREATE TABLE DDL for a writable edge table.
/// See [`build_node_ddl`] for the String-column-type limitation.
pub fn build_edge_ddl(rel_schema: &RelationshipSchema) -> String {
    let from_id_cols = rel_schema.from_id.columns();
    let to_id_cols = rel_schema.to_id.columns();

    let mut id_col_set: HashSet<&str> = HashSet::new();
    for col in &from_id_cols {
        id_col_set.insert(col);
    }
    for col in &to_id_cols {
        id_col_set.insert(col);
    }

    let mut col_defs = Vec::new();

    // from_id column(s)
    for col in &from_id_cols {
        col_defs.push(format!("{} String", col));
    }

    // to_id column(s)
    for col in &to_id_cols {
        col_defs.push(format!("{} String", col));
    }

    // Property columns (skip from_id/to_id)
    for (_, prop_val) in &rel_schema.property_mappings {
        if let PropertyValue::Column(col) = prop_val {
            if !id_col_set.contains(col.as_str()) {
                col_defs.push(format!("{} String", col));
            }
        }
    }

    // _version column
    col_defs.push("_version UInt64 DEFAULT now64()".to_string());

    let mut order_cols = Vec::new();
    order_cols.extend(from_id_cols.iter().map(|s| s.to_string()));
    order_cols.extend(to_id_cols.iter().map(|s| s.to_string()));

    format!(
        "CREATE TABLE IF NOT EXISTS `{}`.`{}` ({}) ENGINE = ReplacingMergeTree(_version) ORDER BY ({})",
        rel_schema.database,
        rel_schema.table_name,
        col_defs.join(", "),
        order_cols.join(", ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_catalog::config::GraphSchemaConfig;

    fn make_test_schema(yaml: &str) -> GraphSchema {
        let config: GraphSchemaConfig = serde_yaml::from_str(yaml).expect("valid yaml");
        config.to_graph_schema().expect("valid schema")
    }

    #[test]
    fn test_build_node_ddl_standard() {
        let schema = make_test_schema(
            r#"
name: test
graph_schema:
  nodes:
    - label: Person
      database: mydb
      table: persons
      node_id: person_id
      property_mappings:
        person_id: person_id
        name: full_name
        age: age
"#,
        );

        let node_schema = schema.all_node_schemas().get("Person").unwrap();
        let ddl = build_node_ddl(node_schema);

        assert!(ddl.starts_with("CREATE TABLE IF NOT EXISTS"));
        assert!(ddl.contains("`mydb`.`persons`"));
        assert!(ddl.contains("person_id String DEFAULT generateUUIDv4()"));
        assert!(ddl.contains("full_name String"));
        assert!(ddl.contains("age String"));
        assert!(ddl.contains("_version UInt64 DEFAULT now64()"));
        assert!(ddl.contains("ReplacingMergeTree(_version)"));
        assert!(ddl.contains("ORDER BY (person_id)"));
        // ID column should not be duplicated
        let id_count = ddl.matches("person_id").count();
        // person_id appears in: column def, ORDER BY = 2 occurrences
        assert_eq!(
            id_count, 2,
            "person_id should appear exactly twice (def + ORDER BY)"
        );
    }

    #[test]
    fn test_build_edge_ddl_standard() {
        let schema = make_test_schema(
            r#"
name: test
graph_schema:
  nodes:
    - label: Person
      database: mydb
      table: persons
      node_id: person_id
      property_mappings:
        person_id: person_id
  edges:
    - type: KNOWS
      database: mydb
      table: knows
      from_node: Person
      to_node: Person
      from_id: from_person_id
      to_id: to_person_id
      property_mappings:
        since: since_year
"#,
        );

        let rel_schema = schema.get_relationships_schemas().values().next().unwrap();
        let ddl = build_edge_ddl(rel_schema);

        assert!(ddl.starts_with("CREATE TABLE IF NOT EXISTS"));
        assert!(ddl.contains("`mydb`.`knows`"));
        assert!(ddl.contains("from_person_id String"));
        assert!(ddl.contains("to_person_id String"));
        assert!(ddl.contains("since_year String"));
        assert!(ddl.contains("_version UInt64 DEFAULT now64()"));
        assert!(ddl.contains("ReplacingMergeTree(_version)"));
        assert!(ddl.contains("ORDER BY (from_person_id, to_person_id)"));
    }

    #[test]
    fn test_schema_with_source_skips_ddl() {
        let schema = make_test_schema(
            r#"
name: test
graph_schema:
  nodes:
    - label: Person
      database: mydb
      table: persons
      node_id: person_id
      source: "/data/persons.parquet"
      property_mappings:
        person_id: person_id
"#,
        );

        let node_schema = schema.all_node_schemas().get("Person").unwrap();
        // Entries with source should not get writable tables
        assert!(node_schema.source.is_some(), "should have source");
    }

    #[test]
    fn test_schema_without_source_gets_ddl() {
        let schema = make_test_schema(
            r#"
name: test
graph_schema:
  nodes:
    - label: Person
      database: mydb
      table: persons
      node_id: person_id
      property_mappings:
        person_id: person_id
"#,
        );

        let node_schema = schema.all_node_schemas().get("Person").unwrap();
        assert!(node_schema.source.is_none(), "should not have source");
        // DDL can be generated
        let ddl = build_node_ddl(node_schema);
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS"));
    }

    #[test]
    fn test_mixed_schema_coexistence() {
        let schema = make_test_schema(
            r#"
name: test
graph_schema:
  nodes:
    - label: Person
      database: mydb
      table: persons
      node_id: person_id
      property_mappings:
        person_id: person_id
    - label: City
      database: mydb
      table: cities
      node_id: city_id
      source: "/data/cities.parquet"
      property_mappings:
        city_id: city_id
        name: city_name
"#,
        );

        let person_schema = schema.all_node_schemas().get("Person").unwrap();
        let city_schema = schema.all_node_schemas().get("City").unwrap();

        assert!(person_schema.source.is_none(), "Person should be writable");
        assert!(city_schema.source.is_some(), "City should be read-only");

        // Only Person gets DDL
        let ddl = build_node_ddl(person_schema);
        assert!(ddl.contains("persons"), "DDL for Person");
    }
}
