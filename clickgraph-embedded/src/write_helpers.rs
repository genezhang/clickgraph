//! Helpers for write operations: property validation, column mapping, INSERT/DELETE SQL generation.

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

/// Check that a table is writable (has no `source:` field set).
///
/// Tables with a `source:` field are backed by external data (Parquet, S3, etc.)
/// and cannot be modified via INSERT, DELETE, or UPDATE operations.
pub fn check_writable(source: &Option<String>, label: &str) -> Result<(), EmbeddedError> {
    if source.is_some() {
        return Err(EmbeddedError::Validation(format!(
            "Cannot modify source-backed table for '{}'. \
             Only writable tables (without a 'source:' field) support write operations.",
            label
        )));
    }
    Ok(())
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

/// Build a `DELETE FROM ... WHERE ...` SQL statement (lightweight delete).
///
/// Maps filter keys from Cypher property names to ClickHouse column names using
/// `property_mappings`, and renders filter values via `Value::to_sql_literal()`.
///
/// The filter map must not be empty — at least one condition is required to prevent
/// accidental deletion of all rows.
pub fn build_delete_sql(
    db: &str,
    table: &str,
    filters: &HashMap<String, Value>,
    property_mappings: &HashMap<&str, &str>,
    id_columns: &[&str],
) -> Result<String, EmbeddedError> {
    if filters.is_empty() {
        return Err(EmbeddedError::Validation(
            "DELETE requires at least one filter condition to prevent accidental deletion of all rows."
                .to_string(),
        ));
    }

    // Build WHERE clauses in sorted key order for deterministic output
    let mut conditions: Vec<String> = Vec::new();
    let mut sorted_keys: Vec<&String> = filters.keys().collect();
    sorted_keys.sort();

    for key in sorted_keys {
        let value = &filters[key];
        // Resolve column name: check property_mappings first, then id_columns, then use as-is
        let col_name = if let Some(mapped) = property_mappings.get(key.as_str()) {
            mapped.to_string()
        } else if id_columns.contains(&key.as_str()) {
            key.clone()
        } else {
            // Should not reach here if validate_properties was called first
            key.clone()
        };

        let literal = value.to_sql_literal().map_err(EmbeddedError::Validation)?;
        conditions.push(format!("`{}` = {}", col_name, literal));
    }

    Ok(format!(
        "DELETE FROM `{}`.`{}` WHERE {}",
        db,
        table,
        conditions.join(" AND ")
    ))
}

/// Transform JSON keys from Cypher property names to ClickHouse column names.
///
/// Parses each line of newline-delimited JSON, remaps keys using `property_mappings`,
/// and re-serializes with ClickHouse column names. Unknown keys are skipped with a
/// warning log.
///
/// Returns the transformed JSON lines and the count of lines processed.
pub fn transform_json_keys(
    json_lines: &str,
    property_mappings: &HashMap<&str, &str>,
    id_columns: &[&str],
) -> Result<(String, usize), EmbeddedError> {
    let mut output_lines = Vec::new();
    let mut line_count = 0;

    for (line_idx, line) in json_lines.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let parsed: serde_json::Value = serde_json::from_str(trimmed).map_err(|e| {
            EmbeddedError::Validation(format!("Invalid JSON on line {}: {}", line_idx + 1, e))
        })?;

        let obj = parsed.as_object().ok_or_else(|| {
            EmbeddedError::Validation(format!(
                "Expected JSON object on line {}, got: {}",
                line_idx + 1,
                trimmed
            ))
        })?;

        let mut mapped_obj = serde_json::Map::new();
        for (key, val) in obj {
            if let Some(mapped_col) = property_mappings.get(key.as_str()) {
                mapped_obj.insert(mapped_col.to_string(), val.clone());
            } else if id_columns.contains(&key.as_str()) {
                mapped_obj.insert(key.clone(), val.clone());
            } else {
                log::warn!(
                    "import_json: skipping unknown key '{}' on line {}",
                    key,
                    line_idx + 1
                );
            }
        }

        let mapped_line = serde_json::to_string(&serde_json::Value::Object(mapped_obj))
            .map_err(|e| EmbeddedError::Validation(format!("JSON serialization error: {}", e)))?;
        output_lines.push(mapped_line);
        line_count += 1;
    }

    Ok((output_lines.join("\n"), line_count))
}

/// Validate a file path for safety (no SQL injection via metacharacters).
pub fn validate_file_path(path: &str) -> Result<(), EmbeddedError> {
    // Reject paths containing SQL metacharacters that could enable injection
    let forbidden = [';', '\'', '"', '\\', '\0', '`', '\n', '\r', '$'];
    for ch in &forbidden {
        if path.contains(*ch) {
            return Err(EmbeddedError::Validation(format!(
                "File path contains forbidden character '{}': {}",
                ch, path
            )));
        }
    }
    Ok(())
}

/// Build an `INSERT INTO ... SELECT ... FROM file(...)` SQL statement for file-based import.
///
/// If all Cypher property names match ClickHouse column names (identity mapping),
/// generates `SELECT * FROM file()`. Otherwise generates explicit column aliases.
///
/// `format` is the ClickHouse format name (e.g., `"JSONEachRow"`, `"CSVWithNames"`,
/// `"Parquet"`).
pub fn build_import_file_sql(
    db: &str,
    table: &str,
    file_path: &str,
    format: &str,
    property_mappings: &HashMap<&str, &str>,
    id_columns: &[&str],
) -> String {
    // Check if mapping is identity (Cypher names == CH column names)
    let needs_mapping = property_mappings
        .iter()
        .any(|(cypher, ch_col)| cypher != ch_col);

    if needs_mapping {
        // Build explicit SELECT with aliases: SELECT cypher_col AS ch_col
        let mut select_parts: Vec<String> = Vec::new();

        // Add ID columns as-is (they pass through without mapping)
        for id_col in id_columns {
            select_parts.push(format!("`{}`", id_col));
        }

        // Add mapped property columns
        let mut sorted_mappings: Vec<(&&str, &&str)> = property_mappings.iter().collect();
        sorted_mappings.sort_by_key(|(k, _)| *k);
        for (cypher_name, ch_col) in sorted_mappings {
            if cypher_name == ch_col {
                select_parts.push(format!("`{}`", ch_col));
            } else {
                select_parts.push(format!("`{}` AS `{}`", cypher_name, ch_col));
            }
        }

        format!(
            "INSERT INTO `{}`.`{}` SELECT {} FROM file('{}', '{}')",
            db,
            table,
            select_parts.join(", "),
            file_path,
            format
        )
    } else {
        format!(
            "INSERT INTO `{}`.`{}` SELECT * FROM file('{}', '{}')",
            db, table, file_path, format
        )
    }
}

/// Detect the ClickHouse import format name from a file extension.
///
/// Returns `None` for unrecognized extensions.
pub fn import_format_from_extension(path: &str) -> Option<&'static str> {
    let ext = path.rsplit('.').next()?.to_lowercase();
    match ext.as_str() {
        "parquet" | "pq" => Some("Parquet"),
        "csv" => Some("CSVWithNames"),
        "tsv" | "tab" => Some("TabSeparatedWithNames"),
        "json" | "ndjson" | "jsonl" => Some("JSONEachRow"),
        _ => None,
    }
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

    // --- DELETE SQL generation tests ---

    #[test]
    fn test_build_delete_sql_with_property_mapping() {
        let mut mappings = HashMap::new();
        mappings.insert("name", "full_name");
        mappings.insert("age", "user_age");

        let mut filters = HashMap::new();
        filters.insert("name".to_string(), Value::String("Alice".to_string()));

        let sql = build_delete_sql("mydb", "users", &filters, &mappings, &["user_id"]).unwrap();
        assert_eq!(
            sql,
            "DELETE FROM `mydb`.`users` WHERE `full_name` = 'Alice'"
        );
    }

    #[test]
    fn test_build_delete_sql_with_id_column() {
        let mut mappings = HashMap::new();
        mappings.insert("name", "full_name");

        let mut filters = HashMap::new();
        filters.insert("user_id".to_string(), Value::String("u123".to_string()));

        let sql = build_delete_sql("mydb", "users", &filters, &mappings, &["user_id"]).unwrap();
        assert_eq!(sql, "DELETE FROM `mydb`.`users` WHERE `user_id` = 'u123'");
    }

    #[test]
    fn test_build_delete_sql_multiple_filters() {
        let mut mappings = HashMap::new();
        mappings.insert("name", "full_name");
        mappings.insert("age", "user_age");

        let mut filters = HashMap::new();
        filters.insert("name".to_string(), Value::String("Bob".to_string()));
        filters.insert("age".to_string(), Value::Int64(30));

        let sql = build_delete_sql("mydb", "users", &filters, &mappings, &["user_id"]).unwrap();
        // Keys are sorted for deterministic output
        assert_eq!(
            sql,
            "DELETE FROM `mydb`.`users` WHERE `user_age` = 30 AND `full_name` = 'Bob'"
        );
    }

    #[test]
    fn test_build_delete_sql_empty_filters_rejected() {
        let mappings = HashMap::new();
        let filters = HashMap::new();
        let result = build_delete_sql("mydb", "users", &filters, &mappings, &[]);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("at least one filter"));
    }

    #[test]
    fn test_build_delete_sql_filter_keys_validated() {
        let mut mappings = HashMap::new();
        mappings.insert("name", "full_name");

        let mut filters = HashMap::new();
        filters.insert("unknown_key".to_string(), Value::String("val".to_string()));

        // validate_properties should catch this before build_delete_sql is called
        let result = validate_properties(&filters, &mappings, &["user_id"]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown_key"));
    }

    // --- Writable guard tests ---

    #[test]
    fn test_check_writable_allows_no_source() {
        assert!(check_writable(&None, "User").is_ok());
    }

    #[test]
    fn test_check_writable_rejects_source_backed() {
        let source = Some("s3://bucket/data.parquet".to_string());
        let result = check_writable(&source, "User");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("source-backed"));
    }

    // --- JSON key transformation tests ---

    #[test]
    fn test_transform_json_keys_maps_cypher_to_ch_columns() {
        let mut mappings = HashMap::new();
        mappings.insert("name", "full_name");
        mappings.insert("email", "email_address");

        let input = r#"{"name": "Alice", "email": "alice@test.com"}"#;
        let (output, count) = transform_json_keys(input, &mappings, &[]).unwrap();
        assert_eq!(count, 1);

        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        let obj = parsed.as_object().unwrap();
        assert!(obj.contains_key("full_name"));
        assert!(obj.contains_key("email_address"));
        assert!(!obj.contains_key("name"));
        assert!(!obj.contains_key("email"));
    }

    #[test]
    fn test_transform_json_keys_skips_unknown_keys() {
        let mut mappings = HashMap::new();
        mappings.insert("name", "full_name");

        let input = r#"{"name": "Alice", "unknown_field": "ignored"}"#;
        let (output, count) = transform_json_keys(input, &mappings, &[]).unwrap();
        assert_eq!(count, 1);

        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        let obj = parsed.as_object().unwrap();
        assert!(obj.contains_key("full_name"));
        assert!(!obj.contains_key("unknown_field"));
    }

    #[test]
    fn test_transform_json_keys_preserves_id_columns() {
        let mut mappings = HashMap::new();
        mappings.insert("name", "full_name");

        let input = r#"{"user_id": "u1", "name": "Alice"}"#;
        let (output, count) = transform_json_keys(input, &mappings, &["user_id"]).unwrap();
        assert_eq!(count, 1);

        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        let obj = parsed.as_object().unwrap();
        assert!(obj.contains_key("user_id"));
        assert!(obj.contains_key("full_name"));
    }

    #[test]
    fn test_transform_json_keys_multiple_lines() {
        let mut mappings = HashMap::new();
        mappings.insert("name", "full_name");

        let input = r#"{"name": "Alice"}
{"name": "Bob"}"#;
        let (output, count) = transform_json_keys(input, &mappings, &[]).unwrap();
        assert_eq!(count, 2);

        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), 2);
    }

    #[test]
    fn test_transform_json_keys_invalid_json() {
        let mappings = HashMap::new();
        let input = "not valid json";
        let result = transform_json_keys(input, &mappings, &[]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid JSON"));
    }

    // --- File path validation tests ---

    #[test]
    fn test_validate_file_path_accepts_normal_paths() {
        assert!(validate_file_path("/tmp/data.json").is_ok());
        assert!(validate_file_path("data/import.jsonl").is_ok());
    }

    #[test]
    fn test_validate_file_path_rejects_semicolons() {
        let result = validate_file_path("/tmp/data.json; DROP TABLE users");
        assert!(result.is_err());
    }

    // --- File import SQL tests ---

    #[test]
    fn test_build_import_file_sql_no_mapping() {
        let mut mappings = HashMap::new();
        mappings.insert("name", "name");
        mappings.insert("age", "age");

        let sql = build_import_file_sql(
            "mydb",
            "users",
            "/tmp/data.json",
            "JSONEachRow",
            &mappings,
            &["id"],
        );
        assert_eq!(
            sql,
            "INSERT INTO `mydb`.`users` SELECT * FROM file('/tmp/data.json', 'JSONEachRow')"
        );
    }

    #[test]
    fn test_build_import_file_sql_with_mapping() {
        let mut mappings = HashMap::new();
        mappings.insert("name", "full_name");
        mappings.insert("email", "email_address");

        let sql = build_import_file_sql(
            "mydb",
            "users",
            "/tmp/data.json",
            "JSONEachRow",
            &mappings,
            &["id"],
        );
        assert!(sql.contains("INSERT INTO `mydb`.`users`"));
        assert!(sql.contains("`email` AS `email_address`"));
        assert!(sql.contains("`name` AS `full_name`"));
        assert!(sql.contains("FROM file('/tmp/data.json', 'JSONEachRow')"));
    }

    #[test]
    fn test_build_import_file_sql_with_id_columns() {
        let mut mappings = HashMap::new();
        mappings.insert("name", "full_name");

        let sql = build_import_file_sql(
            "mydb",
            "users",
            "/tmp/data.json",
            "JSONEachRow",
            &mappings,
            &["user_id"],
        );
        assert!(sql.contains("`user_id`"));
        assert!(sql.contains("`name` AS `full_name`"));
    }

    #[test]
    fn test_build_import_file_sql_parquet() {
        let mut mappings = HashMap::new();
        mappings.insert("name", "name");

        let sql = build_import_file_sql(
            "mydb",
            "users",
            "/tmp/data.parquet",
            "Parquet",
            &mappings,
            &["id"],
        );
        assert_eq!(
            sql,
            "INSERT INTO `mydb`.`users` SELECT * FROM file('/tmp/data.parquet', 'Parquet')"
        );
    }

    #[test]
    fn test_build_import_file_sql_csv() {
        let mut mappings = HashMap::new();
        mappings.insert("name", "full_name");

        let sql = build_import_file_sql(
            "mydb",
            "users",
            "/tmp/data.csv",
            "CSVWithNames",
            &mappings,
            &["id"],
        );
        assert!(sql.contains("FROM file('/tmp/data.csv', 'CSVWithNames')"));
        assert!(sql.contains("`name` AS `full_name`"));
    }

    // --- import format detection tests ---

    #[test]
    fn test_import_format_from_extension() {
        assert_eq!(
            import_format_from_extension("data.parquet"),
            Some("Parquet")
        );
        assert_eq!(import_format_from_extension("data.pq"), Some("Parquet"));
        assert_eq!(
            import_format_from_extension("data.csv"),
            Some("CSVWithNames")
        );
        assert_eq!(
            import_format_from_extension("data.tsv"),
            Some("TabSeparatedWithNames")
        );
        assert_eq!(
            import_format_from_extension("data.json"),
            Some("JSONEachRow")
        );
        assert_eq!(
            import_format_from_extension("data.ndjson"),
            Some("JSONEachRow")
        );
        assert_eq!(
            import_format_from_extension("data.jsonl"),
            Some("JSONEachRow")
        );
        assert_eq!(import_format_from_extension("data.unknown"), None);
    }
}
