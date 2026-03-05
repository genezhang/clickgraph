//! Source URI resolver for chdb embedded mode.
//!
//! Translates the `source:` field from a YAML schema entry into a chdb/ClickHouse
//! table function expression that can be used in `CREATE VIEW ... AS SELECT * FROM ...`.

/// Resolve a `source:` URI string to a chdb table function expression.
///
/// # Supported URI patterns
///
/// | Pattern | Maps to |
/// |---------|---------|
/// | `/abs/path.parquet` | `file('/abs/path.parquet', Parquet)` |
/// | `./rel/path.csv` | `file('./rel/path.csv', CSV)` |
/// | `s3://bucket/key.parquet` | `s3('s3://bucket/key.parquet', Parquet)` |
/// | `iceberg+s3://bucket/table/` | `iceberg('s3://bucket/table/')` |
/// | `iceberg+local:///path/table/` | `icebergLocal('/path/table/')` |
/// | `delta+s3://bucket/table/` | `deltaLake('s3://bucket/table/')` |
/// | `table_function:<raw>` | `<raw>` (escape hatch) |
///
/// Returns an error if the URI scheme is not recognised.
pub fn resolve_source_uri(source: &str) -> Result<String, String> {
    // Escape hatch: pass raw function through verbatim (deliberately unescaped)
    if let Some(raw) = source.strip_prefix("table_function:") {
        return Ok(raw.trim().to_string());
    }

    // iceberg+local:///abs/path/table/ → icebergLocal('/abs/path/table/')
    if let Some(rest) = source.strip_prefix("iceberg+local://") {
        let path = rest.trim_start_matches('/');
        return Ok(format!("icebergLocal('/{}')", escape_sql_string(path)));
    }

    // iceberg+s3://bucket/prefix/ → iceberg('s3://bucket/prefix/')
    if let Some(rest) = source.strip_prefix("iceberg+") {
        return Ok(format!("iceberg('{}')", escape_sql_string(rest)));
    }

    // delta+s3://bucket/prefix/ → deltaLake('s3://bucket/prefix/')
    if let Some(rest) = source.strip_prefix("delta+") {
        return Ok(format!("deltaLake('{}')", escape_sql_string(rest)));
    }

    // s3://bucket/key.ext → s3('s3://bucket/key.ext', Format)
    if source.starts_with("s3://") || source.starts_with("gs://") || source.starts_with("azure://")
    {
        let fmt = detect_format_from_path(source);
        return Ok(format!("s3('{}', '{}')", escape_sql_string(source), fmt));
    }

    // Local file path (absolute or relative)
    if source.starts_with('/') || source.starts_with('.') || source.starts_with("file://") {
        let path = source.strip_prefix("file://").unwrap_or(source);
        let fmt = detect_format_from_path(path);
        return Ok(format!("file('{}', '{}')", escape_sql_string(path), fmt));
    }

    Err(format!(
        "Unrecognised source URI scheme: '{}'. \
         Supported schemes: file://, s3://, iceberg+s3://, iceberg+local://, delta+s3://, \
         table_function:<raw>",
        source
    ))
}

/// Escape single quotes and backslashes in a string for safe embedding in SQL literals.
fn escape_sql_string(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

/// Infer a ClickHouse format string from a file extension.
fn detect_format_from_path(path: &str) -> &'static str {
    // Strip query string / fragment if present (e.g. s3:// URLs with ?params)
    let base = path.split('?').next().unwrap_or(path);
    let lower = base.to_lowercase();

    if lower.ends_with(".parquet") || lower.ends_with(".parq") {
        "Parquet"
    } else if lower.ends_with(".csv") {
        "CSV"
    } else if lower.ends_with(".tsv") {
        "TSV"
    } else if lower.ends_with(".json") || lower.ends_with(".ndjson") || lower.ends_with(".jsonl") {
        "JSONEachRow"
    } else if lower.ends_with(".orc") {
        "ORC"
    } else if lower.ends_with(".avro") {
        "Avro"
    } else {
        // Default to Parquet for directory-style paths (Iceberg-like)
        "Parquet"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_parquet() {
        let result = resolve_source_uri("/data/users.parquet").unwrap();
        assert_eq!(result, "file('/data/users.parquet', 'Parquet')");
    }

    #[test]
    fn test_s3_parquet() {
        let result = resolve_source_uri("s3://mybucket/users.parquet").unwrap();
        assert_eq!(result, "s3('s3://mybucket/users.parquet', 'Parquet')");
    }

    #[test]
    fn test_iceberg_s3() {
        let result = resolve_source_uri("iceberg+s3://mybucket/tables/users/").unwrap();
        assert_eq!(result, "iceberg('s3://mybucket/tables/users/')");
    }

    #[test]
    fn test_iceberg_local() {
        let result = resolve_source_uri("iceberg+local:///tmp/tables/users/").unwrap();
        assert_eq!(result, "icebergLocal('/tmp/tables/users/')");
    }

    #[test]
    fn test_delta_s3() {
        let result = resolve_source_uri("delta+s3://bucket/delta/users/").unwrap();
        assert_eq!(result, "deltaLake('s3://bucket/delta/users/')");
    }

    #[test]
    fn test_table_function_escape_hatch() {
        let result =
            resolve_source_uri("table_function:s3('s3://b/p', 'key', 'secret', 'Parquet')")
                .unwrap();
        assert_eq!(result, "s3('s3://b/p', 'key', 'secret', 'Parquet')");
    }

    #[test]
    fn test_unknown_scheme() {
        assert!(resolve_source_uri("ftp://server/path").is_err());
    }

    #[test]
    fn test_single_quote_in_path_is_escaped() {
        // Defense-in-depth: even though schema YAML is trusted, paths with
        // special characters should not break the generated SQL.
        let result = resolve_source_uri("/data/it's-a-file.parquet").unwrap();
        assert!(
            result.contains("\\'"),
            "single quote must be escaped: {}",
            result
        );
        assert_eq!(result, "file('/data/it\\'s-a-file.parquet', 'Parquet')");
    }

    #[test]
    fn test_backslash_in_s3_key_is_escaped() {
        let result = resolve_source_uri("s3://bucket/path\\with\\backslash.csv").unwrap();
        assert!(
            result.contains("\\\\"),
            "backslash must be escaped: {}",
            result
        );
    }
}
