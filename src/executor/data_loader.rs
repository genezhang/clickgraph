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

use std::collections::{HashMap, HashSet};

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

/// Build a reverse mapping from column name to Cypher property name.
///
/// Used by DDL generation to look up property_types by column name.
fn build_column_to_property_map(
    property_mappings: &HashMap<String, PropertyValue>,
) -> HashMap<&str, &str> {
    let mut map = HashMap::new();
    for (cypher_name, prop_val) in property_mappings {
        if let PropertyValue::Column(col) = prop_val {
            map.insert(col.as_str(), cypher_name.as_str());
        }
    }
    map
}

/// Resolve the ClickHouse type for a column, using property_types if available.
///
/// Looks up the column name in the reverse mapping to find the Cypher property name,
/// then checks property_types for a SchemaType. Falls back to "String" if not found.
fn resolve_column_type<'a>(
    column_name: &str,
    col_to_prop: &HashMap<&str, &str>,
    property_types: &'a HashMap<String, crate::graph_catalog::schema_types::SchemaType>,
) -> &'a str {
    if let Some(cypher_name) = col_to_prop.get(column_name) {
        if let Some(schema_type) = property_types.get(*cypher_name) {
            return schema_type.to_clickhouse_type();
        }
    }
    "String"
}

/// Build CREATE TABLE DDL for a writable node table.
///
/// Uses `property_types` to determine ClickHouse column types. Properties not
/// in `property_types` default to String. ID columns use the node_id dtype
/// (from the schema `type` field), falling back to String with UUID default.
pub fn build_node_ddl(node_schema: &NodeSchema) -> String {
    let id_columns = node_schema.node_id.id.columns();
    let id_col_set: HashSet<&str> = id_columns.iter().copied().collect();

    // Build reverse mapping: column_name -> cypher_property_name
    let col_to_prop = build_column_to_property_map(&node_schema.property_mappings);

    let mut col_defs = Vec::new();

    // ID column(s): use explicit type field if provided, otherwise default to String with UUID
    if let Some(ref id_types) = node_schema.node_id_types {
        // Explicit type(s) from schema YAML type/types field
        for (i, id_col) in id_columns.iter().enumerate() {
            let schema_type = id_types.get(i).unwrap_or(&id_types[0]);
            let ch_type = schema_type.to_clickhouse_type();
            if ch_type == "String" {
                col_defs.push(format!("{} String DEFAULT generateUUIDv4()", id_col));
            } else {
                col_defs.push(format!("{} {}", id_col, ch_type));
            }
        }
    } else {
        // No explicit type — default to String with UUID auto-generation
        for id_col in &id_columns {
            col_defs.push(format!("{} String DEFAULT generateUUIDv4()", id_col));
        }
    }

    // Property columns (skip ID columns to avoid duplication).
    // Use Nullable(T) so that nodes without a given property return NULL
    // rather than a default value (e.g., 0 or ""). This matches Cypher
    // semantics where accessing a missing property returns null.
    for (_, prop_val) in &node_schema.property_mappings {
        if let PropertyValue::Column(col) = prop_val {
            if !id_col_set.contains(col.as_str()) {
                let base_type = resolve_column_type(col, &col_to_prop, &node_schema.property_types);
                // Wrap in Nullable unless it's already Nullable or UUID
                let ch_type = if base_type.starts_with("Nullable") || base_type == "UUID" {
                    base_type.to_string()
                } else {
                    format!("Nullable({})", base_type)
                };
                col_defs.push(format!("{} {}", col, ch_type));
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

    // Lightweight UPDATE prerequisite (Decision 0.7 of the embedded-writes
    // design): the table must be created with the two block-tracking
    // columns enabled. Without these, chdb returns Code 48 NOT_IMPLEMENTED
    // when a Cypher SET clause runs against the table.
    format!(
        "CREATE TABLE IF NOT EXISTS `{}`.`{}` ({}) ENGINE = ReplacingMergeTree(_version) ORDER BY {} SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1",
        node_schema.database,
        node_schema.table_name,
        col_defs.join(", "),
        order_by
    )
}

/// Build CREATE TABLE DDL for a writable edge table.
///
/// Uses `property_types` to determine ClickHouse column types for edge properties.
/// Properties not in `property_types` default to String.
/// The `_version UInt64 DEFAULT now64()` column is unchanged.
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

    // Build reverse mapping: column_name -> cypher_property_name
    let col_to_prop = build_column_to_property_map(&rel_schema.property_mappings);

    let mut col_defs = Vec::new();

    // from_id column(s) — default to String (FK columns match node ID types)
    for col in &from_id_cols {
        col_defs.push(format!("{} String", col));
    }

    // to_id column(s) — default to String
    for col in &to_id_cols {
        col_defs.push(format!("{} String", col));
    }

    // Property columns (skip from_id/to_id). Use Nullable for Cypher null semantics.
    for (_, prop_val) in &rel_schema.property_mappings {
        if let PropertyValue::Column(col) = prop_val {
            if !id_col_set.contains(col.as_str()) {
                let base_type = resolve_column_type(col, &col_to_prop, &rel_schema.property_types);
                let ch_type = if base_type.starts_with("Nullable") || base_type == "UUID" {
                    base_type.to_string()
                } else {
                    format!("Nullable({})", base_type)
                };
                col_defs.push(format!("{} {}", col, ch_type));
            }
        }
    }

    // _version column
    col_defs.push("_version UInt64 DEFAULT now64()".to_string());

    let mut order_cols = Vec::new();
    order_cols.extend(from_id_cols.iter().map(|s| s.to_string()));
    order_cols.extend(to_id_cols.iter().map(|s| s.to_string()));

    // Lightweight UPDATE prerequisite (Decision 0.7) — see node DDL above.
    format!(
        "CREATE TABLE IF NOT EXISTS `{}`.`{}` ({}) ENGINE = ReplacingMergeTree(_version) ORDER BY ({}) SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1",
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
        assert!(ddl.contains("full_name Nullable(String)"));
        assert!(ddl.contains("age Nullable(String)"));
        assert!(ddl.contains("_version UInt64 DEFAULT now64()"));
        assert!(ddl.contains("ReplacingMergeTree(_version)"));
        assert!(ddl.contains("ORDER BY (person_id)"));
        // Lightweight UPDATE prerequisites (Decision 0.7) — block-tracking
        // columns must be enabled. Without these, chdb returns Code 48
        // NOT_IMPLEMENTED on Cypher SET.
        assert!(
            ddl.contains("enable_block_number_column = 1"),
            "missing enable_block_number_column SETTING: {}",
            ddl
        );
        assert!(
            ddl.contains("enable_block_offset_column = 1"),
            "missing enable_block_offset_column SETTING: {}",
            ddl
        );
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
        assert!(ddl.contains("since_year Nullable(String)"));
        assert!(ddl.contains("_version UInt64 DEFAULT now64()"));
        assert!(ddl.contains("ReplacingMergeTree(_version)"));
        assert!(ddl.contains("ORDER BY (from_person_id, to_person_id)"));
        // Lightweight UPDATE prerequisites (Decision 0.7).
        assert!(
            ddl.contains("enable_block_number_column = 1"),
            "missing enable_block_number_column: {}",
            ddl
        );
        assert!(
            ddl.contains("enable_block_offset_column = 1"),
            "missing enable_block_offset_column: {}",
            ddl
        );
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

    #[test]
    fn test_build_node_ddl_with_property_types() {
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
        age: age_col
        score: score_col
        active: is_active
        joined: join_date
      property_types:
        age: integer
        score: float
        active: boolean
        joined: date
"#,
        );

        let node_schema = schema.all_node_schemas().get("Person").unwrap();
        let ddl = build_node_ddl(node_schema);

        // ID column defaults to String with UUID (no explicit type field)
        assert!(
            ddl.contains("person_id String DEFAULT generateUUIDv4()"),
            "ID column should be String with UUID default: {}",
            ddl
        );
        // Typed properties
        assert!(
            ddl.contains("age_col Nullable(Int64)"),
            "age should be Nullable(Int64): {}",
            ddl
        );
        assert!(
            ddl.contains("score_col Nullable(Float64)"),
            "score should be Nullable(Float64): {}",
            ddl
        );
        assert!(
            ddl.contains("is_active Nullable(UInt8)"),
            "active should be Nullable(UInt8) (boolean): {}",
            ddl
        );
        assert!(
            ddl.contains("join_date Nullable(Date32)"),
            "joined should be Nullable(Date32): {}",
            ddl
        );
        // Untyped property defaults to Nullable(String)
        assert!(
            ddl.contains("full_name Nullable(String)"),
            "untyped name should be Nullable(String): {}",
            ddl
        );
        // _version column unchanged
        assert!(
            ddl.contains("_version UInt64 DEFAULT now64()"),
            "_version should be unchanged: {}",
            ddl
        );
    }

    #[test]
    fn test_build_node_ddl_without_property_types() {
        // Backward compatibility: no property_types → all columns String
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
        age: age_col
"#,
        );

        let node_schema = schema.all_node_schemas().get("Person").unwrap();
        let ddl = build_node_ddl(node_schema);

        assert!(
            ddl.contains("person_id String DEFAULT generateUUIDv4()"),
            "ID should be String: {}",
            ddl
        );
        assert!(
            ddl.contains("full_name Nullable(String)"),
            "name should be Nullable(String): {}",
            ddl
        );
        assert!(
            ddl.contains("age_col Nullable(String)"),
            "age should be Nullable(String): {}",
            ddl
        );
    }

    #[test]
    fn test_build_edge_ddl_with_property_types() {
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
        weight: weight_col
      property_types:
        since: integer
        weight: float
"#,
        );

        let rel_schema = schema.get_relationships_schemas().values().next().unwrap();
        let ddl = build_edge_ddl(rel_schema);

        // FK columns default to String
        assert!(
            ddl.contains("from_person_id String"),
            "from_id should be String: {}",
            ddl
        );
        assert!(
            ddl.contains("to_person_id String"),
            "to_id should be String: {}",
            ddl
        );
        // Typed edge properties
        assert!(
            ddl.contains("since_year Nullable(Int64)"),
            "since should be Nullable(Int64): {}",
            ddl
        );
        assert!(
            ddl.contains("weight_col Nullable(Float64)"),
            "weight should be Nullable(Float64): {}",
            ddl
        );
        // _version unchanged
        assert!(
            ddl.contains("_version UInt64 DEFAULT now64()"),
            "_version should be unchanged: {}",
            ddl
        );
    }

    #[test]
    fn test_build_node_ddl_with_id_type() {
        // When type field is set on node, ID column uses that type
        let schema = make_test_schema(
            r#"
name: test
graph_schema:
  nodes:
    - label: Person
      database: mydb
      table: persons
      node_id: person_id
      type: integer
      property_mappings:
        person_id: person_id
        name: full_name
"#,
        );

        let node_schema = schema.all_node_schemas().get("Person").unwrap();
        let ddl = build_node_ddl(node_schema);

        // ID column should use Int64 (from type: integer), no UUID default
        assert!(
            ddl.contains("person_id Int64") && !ddl.contains("generateUUIDv4"),
            "ID with type:integer should be Int64 without UUID: {}",
            ddl
        );
    }
}
