//! Helpers for write operations: property validation, column mapping, INSERT SQL generation.

use std::collections::HashMap;

use clickgraph::graph_catalog::expression_parser::PropertyValue;

use super::error::EmbeddedError;
use super::value::Value;

/// Validate that all property keys in `properties` are known to the schema.
///
/// Accepts Cypher property names (keys of `property_mappings`) and ID column names.
/// Rejects unknown keys with `EmbeddedError::Validation`.
pub fn validate_properties(
    properties: &HashMap<String, Value>,
    property_mappings: &HashMap<&str, &str>,
    id_columns: &[&str],
) -> Result<(), EmbeddedError> {
    let mut unknown_keys = Vec::new();
    for key in properties.keys() {
        if !property_mappings.contains_key(key.as_str()) && !id_columns.contains(&key.as_str()) {
            unknown_keys.push(key.clone());
        }
    }

    if !unknown_keys.is_empty() {
        unknown_keys.sort();
        let mut valid_keys: Vec<&str> = property_mappings.keys().copied().collect();
        valid_keys.extend(id_columns.iter().copied());
        valid_keys.sort();
        valid_keys.dedup();
        return Err(EmbeddedError::Validation(format!(
            "Unknown property key(s): {:?}. Valid keys: {:?}",
            unknown_keys, valid_keys
        )));
    }

    Ok(())
}

/// Extract a simple `&str -> &str` mapping from the schema's `HashMap<String, PropertyValue>`.
///
/// Only `PropertyValue::Column` entries produce mappings. Expression entries are skipped
/// since they cannot be written to.
pub fn extract_property_mappings(
    schema_mappings: &HashMap<String, PropertyValue>,
) -> HashMap<&str, &str> {
    let mut result = HashMap::new();
    for (cypher_name, prop_val) in schema_mappings {
        match prop_val {
            PropertyValue::Column(col) => {
                result.insert(cypher_name.as_str(), col.as_str());
            }
            PropertyValue::Expression(_) => {
                // Expression properties are computed, not writable
            }
        }
    }
    result
}

/// Build an INSERT SQL statement for one or more rows.
///
/// Generates:
/// - Single row: `INSERT INTO {db}.{table} ({cols}) VALUES ({vals})`
/// - Multiple rows: `INSERT INTO {db}.{table} ({cols}) VALUES ({vals1}), ({vals2}), ...`
pub fn build_insert_sql(
    db: &str,
    table: &str,
    columns: &[String],
    values_rows: &[Vec<String>],
) -> String {
    let cols = columns
        .iter()
        .map(|c| format!("`{}`", c))
        .collect::<Vec<_>>()
        .join(", ");
    let rows: Vec<String> = values_rows
        .iter()
        .map(|row| format!("({})", row.join(", ")))
        .collect();
    format!(
        "INSERT INTO `{}`.`{}` ({}) VALUES {}",
        db,
        table,
        cols,
        rows.join(", ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_properties_accepts_known_keys() {
        let mut mappings = HashMap::new();
        mappings.insert("name", "full_name");
        mappings.insert("age", "age");

        let mut props = HashMap::new();
        props.insert("name".to_string(), Value::String("Alice".to_string()));

        let result = validate_properties(&props, &mappings, &["user_id"]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_properties_accepts_id_columns() {
        let mappings = HashMap::new();

        let mut props = HashMap::new();
        props.insert("user_id".to_string(), Value::String("u1".to_string()));

        let result = validate_properties(&props, &mappings, &["user_id"]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_properties_rejects_unknown_keys() {
        let mut mappings = HashMap::new();
        mappings.insert("name", "full_name");

        let mut props = HashMap::new();
        props.insert("bad_key".to_string(), Value::String("val".to_string()));

        let result = validate_properties(&props, &mappings, &["user_id"]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("bad_key"));
    }

    #[test]
    fn test_build_insert_sql_single_row() {
        let cols = vec!["id".to_string(), "name".to_string()];
        let rows = vec![vec!["'abc'".to_string(), "'Alice'".to_string()]];
        let sql = build_insert_sql("mydb", "mytable", &cols, &rows);
        assert_eq!(
            sql,
            "INSERT INTO `mydb`.`mytable` (`id`, `name`) VALUES ('abc', 'Alice')"
        );
    }

    #[test]
    fn test_build_insert_sql_batch() {
        let cols = vec!["id".to_string(), "name".to_string()];
        let rows = vec![
            vec!["'a'".to_string(), "'Alice'".to_string()],
            vec!["'b'".to_string(), "'Bob'".to_string()],
        ];
        let sql = build_insert_sql("mydb", "mytable", &cols, &rows);
        assert_eq!(
            sql,
            "INSERT INTO `mydb`.`mytable` (`id`, `name`) VALUES ('a', 'Alice'), ('b', 'Bob')"
        );
    }

    #[test]
    fn test_extract_property_mappings() {
        let mut schema_map = HashMap::new();
        schema_map.insert(
            "name".to_string(),
            PropertyValue::Column("full_name".to_string()),
        );
        schema_map.insert(
            "computed".to_string(),
            PropertyValue::Expression("col1 + col2".to_string()),
        );

        let result = extract_property_mappings(&schema_map);
        assert_eq!(result.get("name"), Some(&"full_name"));
        assert!(!result.contains_key("computed"));
    }
}
