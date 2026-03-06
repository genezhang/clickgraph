//! APOC-compatible export procedures.
//!
//! Implements `apoc.export.{csv|json|parquet}.query(cypher, destination, config)`
//! for writing Cypher query results to files, S3, or HTTP endpoints.
//!
//! # Architecture
//!
//! Unlike schema-introspection procedures (which use `ProcedureRegistry`),
//! export procedures need SQL execution access. They follow the PageRank
//! pattern: intercept in the handler, generate SQL, execute via `QueryExecutor`.
//!
//! # Execution Flow
//!
//! 1. Handler detects `apoc.export.*` prefix → routes here
//! 2. [`parse_export_call`] extracts inner Cypher, destination URI, config
//! 3. Caller translates inner Cypher → SQL via normal pipeline
//! 4. [`build_export_sql`] wraps the SELECT in `INSERT INTO FUNCTION <dest>`
//! 5. Caller executes via `AppState.executor`

use crate::executor::source_resolver::escape_sql_string;
use crate::open_cypher_parser::ast::{Expression, Literal};

// ───────────────────────────────────────────────────────────────────────
// Format mapping
// ───────────────────────────────────────────────────────────────────────

/// ClickHouse format name derived from the APOC procedure name.
///
/// The procedure name determines the output format; the destination URI
/// only determines *where* to write.
pub fn format_from_procedure_name(procedure_name: &str) -> Result<&'static str, String> {
    let lower = procedure_name.to_lowercase();
    if lower.contains("apoc.export.csv") {
        Ok("CSVWithNames")
    } else if lower.contains("apoc.export.json") {
        Ok("JSONEachRow")
    } else if lower.contains("apoc.export.parquet") {
        Ok("Parquet")
    } else {
        Err(format!(
            "Unsupported export procedure: '{}'. \
             Supported: apoc.export.csv.query, apoc.export.json.query, apoc.export.parquet.query",
            procedure_name
        ))
    }
}

/// Check whether a procedure name is an export procedure.
pub fn is_export_procedure(name: &str) -> bool {
    let lower = name.to_lowercase();
    lower.starts_with("apoc.export.") && lower.ends_with(".query")
}

// ───────────────────────────────────────────────────────────────────────
// Destination resolver
// ───────────────────────────────────────────────────────────────────────

/// Resolve a destination URI to a ClickHouse table function for writing.
///
/// Returns the `INSERT INTO FUNCTION <table_fn>` prefix (without the SELECT).
///
/// # Supported URI patterns
///
/// | Pattern | Generated SQL prefix |
/// |---------|---------------------|
/// | `/path/file.parquet` | `INSERT INTO FUNCTION file('/path/file.parquet', 'Parquet')` |
/// | `./rel/out.csv` | `INSERT INTO FUNCTION file('./rel/out.csv', 'CSVWithNames')` |
/// | `s3://bucket/out.parquet` | `INSERT INTO FUNCTION s3('s3://bucket/out.parquet', 'Parquet')` |
/// | `gs://bucket/out.csv` | `INSERT INTO FUNCTION s3('https://storage.googleapis.com/...', 'CSVWithNames')` |
/// | `https://host/endpoint` | `INSERT INTO FUNCTION url('https://host/endpoint', 'JSONEachRow')` |
/// | `http://host/endpoint` | `INSERT INTO FUNCTION url('http://host/endpoint', 'JSONEachRow')` |
pub fn resolve_destination(uri: &str, format: &str) -> Result<String, String> {
    // S3
    if uri.starts_with("s3://") {
        return Ok(format!(
            "INSERT INTO FUNCTION s3('{}', '{}')",
            escape_sql_string(uri),
            format
        ));
    }

    // GCS (S3-compatible via HMAC credentials)
    if let Some(rest) = uri.strip_prefix("gs://") {
        let url = format!("https://storage.googleapis.com/{}", rest);
        return Ok(format!(
            "INSERT INTO FUNCTION s3('{}', '{}')",
            escape_sql_string(&url),
            format
        ));
    }

    // Azure Blob Storage
    if let Some(rest) = uri.strip_prefix("azure://") {
        let (container, blob_path) = rest.split_once('/').unwrap_or((rest, ""));
        return Ok(format!(
            "INSERT INTO FUNCTION azureBlobStorage(\
             getenv('AZURE_STORAGE_CONNECTION_STRING'), '{}', '{}', '{}')",
            escape_sql_string(container),
            escape_sql_string(blob_path),
            format,
        ));
    }

    // HTTP/HTTPS endpoint
    if uri.starts_with("https://") || uri.starts_with("http://") {
        return Ok(format!(
            "INSERT INTO FUNCTION url('{}', '{}')",
            escape_sql_string(uri),
            format
        ));
    }

    // Local file (absolute, relative, or file:// URI)
    if uri.starts_with('/')
        || uri.starts_with('.')
        || uri.starts_with("file://")
        || !uri.contains("://")
    {
        let path = uri.strip_prefix("file://").unwrap_or(uri);
        return Ok(format!(
            "INSERT INTO FUNCTION file('{}', '{}')",
            escape_sql_string(path),
            format
        ));
    }

    Err(format!(
        "Unsupported destination URI scheme: '{}'. \
         Supported: local paths, file://, s3://, gs://, azure://, http://, https://",
        uri
    ))
}

// ───────────────────────────────────────────────────────────────────────
// Export SQL builder
// ───────────────────────────────────────────────────────────────────────

/// Configuration extracted from the APOC config map argument.
#[derive(Debug, Clone, Default)]
pub struct ExportConfig {
    /// Parquet compression codec (snappy, gzip, lz4, zstd, brotli).
    pub compression: Option<String>,
}

/// Valid Parquet compression codecs.
const VALID_PARQUET_CODECS: &[&str] = &["none", "snappy", "gzip", "lz4", "zstd", "brotli"];

/// Build the complete export SQL from a SELECT statement.
///
/// Combines `resolve_destination` output with optional SETTINGS and the SELECT.
pub fn build_export_sql(
    select_sql: &str,
    destination_uri: &str,
    format: &str,
    config: &ExportConfig,
) -> Result<String, String> {
    let mut sql = resolve_destination(destination_uri, format)?;

    // Add SETTINGS for Parquet compression
    if let Some(ref codec) = config.compression {
        if format == "Parquet" {
            let lower = codec.to_lowercase();
            if !VALID_PARQUET_CODECS.contains(&lower.as_str()) {
                return Err(format!(
                    "Unsupported Parquet compression codec '{}'. Supported: {}",
                    codec,
                    VALID_PARQUET_CODECS.join(", ")
                ));
            }
            sql.push_str(&format!(
                " SETTINGS output_format_parquet_compression_method = '{}'",
                lower
            ));
        }
    }

    sql.push(' ');
    sql.push_str(select_sql);
    Ok(sql)
}

// ───────────────────────────────────────────────────────────────────────
// Argument extraction
// ───────────────────────────────────────────────────────────────────────

/// Parsed arguments from an `apoc.export.*.query(cypher, dest, config)` call.
#[derive(Debug)]
pub struct ExportCallArgs {
    /// The inner Cypher query to execute and export.
    pub cypher_query: String,
    /// Destination URI (file path, S3 URI, HTTP URL, etc.).
    pub destination: String,
    /// Export configuration (compression, etc.).
    pub config: ExportConfig,
}

/// Extract a string value from an AST Expression.
fn extract_string(expr: &Expression<'_>, arg_name: &str) -> Result<String, String> {
    match expr {
        Expression::Literal(Literal::String(s)) => Ok(s.to_string()),
        _ => Err(format!(
            "Expected a string literal for {}, got {:?}",
            arg_name, expr
        )),
    }
}

/// Parse the config map expression into ExportConfig.
fn parse_config_map(expr: &Expression<'_>) -> ExportConfig {
    let mut config = ExportConfig::default();
    if let Expression::MapLiteral(entries) = expr {
        for (key, value) in entries {
            let key_lower = key.to_lowercase();
            if key_lower == "compression" {
                if let Expression::Literal(Literal::String(s)) = value {
                    config.compression = Some(s.to_string());
                }
            }
        }
    }
    config
}

/// Parse export procedure arguments from the StandaloneProcedureCall AST.
///
/// Expected signature: `apoc.export.{format}.query(cypher, destination, config)`
/// - `cypher`: string — inner Cypher query
/// - `destination`: string — output URI
/// - `config`: map (optional) — `{compression: "zstd"}` etc.
pub fn parse_export_call(args: &[&Expression<'_>]) -> Result<ExportCallArgs, String> {
    if args.len() < 2 {
        return Err(format!(
            "apoc.export.*.query requires at least 2 arguments (cypher, destination), got {}",
            args.len()
        ));
    }

    let cypher_query = extract_string(args[0], "cypher query")?;
    let destination = extract_string(args[1], "destination")?;

    let config = if args.len() >= 3 {
        parse_config_map(args[2])
    } else {
        ExportConfig::default()
    };

    Ok(ExportCallArgs {
        cypher_query,
        destination,
        config,
    })
}

// ───────────────────────────────────────────────────────────────────────
// Tests
// ───────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // --- Parser integration: verify CALL apoc.export.*.query(...) parses ---

    #[test]
    fn test_parser_handles_export_call_syntax() {
        let input = r#"CALL apoc.export.csv.query("MATCH (n) RETURN n", "/tmp/out.csv", {})"#;
        let result = crate::open_cypher_parser::parse_cypher_statement(input);
        assert!(
            result.is_ok(),
            "Parser should handle export CALL: {:?}",
            result.err()
        );
        let (_, stmt) = result.unwrap();
        // With the fixed parser, standalone CALL with positional args is parsed correctly
        match &stmt {
            crate::open_cypher_parser::ast::CypherStatement::ProcedureCall(pc) => {
                assert_eq!(pc.procedure_name, "apoc.export.csv.query");
                assert_eq!(pc.arguments.len(), 3, "Expected 3 positional args");
            }
            other => panic!(
                "Expected ProcedureCall, got {:?}",
                std::mem::discriminant(other)
            ),
        };
        assert!(is_export_procedure("apoc.export.csv.query"));
    }

    #[test]
    fn test_parser_export_parquet_with_config() {
        let input = r#"CALL apoc.export.parquet.query("MATCH (u:User) RETURN u.name", "s3://bucket/out.parquet", {compression: "zstd"})"#;
        let result = crate::open_cypher_parser::parse_cypher_statement(input);
        assert!(
            result.is_ok(),
            "Parser should handle S3 export: {:?}",
            result.err()
        );
    }

    // --- format_from_procedure_name ---

    #[test]
    fn test_format_csv() {
        assert_eq!(
            format_from_procedure_name("apoc.export.csv.query").unwrap(),
            "CSVWithNames"
        );
    }

    #[test]
    fn test_format_json() {
        assert_eq!(
            format_from_procedure_name("apoc.export.json.query").unwrap(),
            "JSONEachRow"
        );
    }

    #[test]
    fn test_format_parquet() {
        assert_eq!(
            format_from_procedure_name("apoc.export.parquet.query").unwrap(),
            "Parquet"
        );
    }

    #[test]
    fn test_format_unknown() {
        assert!(format_from_procedure_name("apoc.export.orc.query").is_err());
    }

    #[test]
    fn test_format_case_insensitive() {
        assert_eq!(
            format_from_procedure_name("APOC.EXPORT.CSV.QUERY").unwrap(),
            "CSVWithNames"
        );
    }

    // --- is_export_procedure ---

    #[test]
    fn test_is_export_procedure_true() {
        assert!(is_export_procedure("apoc.export.csv.query"));
        assert!(is_export_procedure("apoc.export.json.query"));
        assert!(is_export_procedure("apoc.export.parquet.query"));
    }

    #[test]
    fn test_is_export_procedure_false() {
        assert!(!is_export_procedure("apoc.meta.schema"));
        assert!(!is_export_procedure("db.labels"));
        assert!(!is_export_procedure("apoc.export.csv")); // missing .query
    }

    // --- resolve_destination ---

    #[test]
    fn test_dest_local_absolute() {
        let result = resolve_destination("/tmp/output.parquet", "Parquet").unwrap();
        assert_eq!(
            result,
            "INSERT INTO FUNCTION file('/tmp/output.parquet', 'Parquet')"
        );
    }

    #[test]
    fn test_dest_local_relative() {
        let result = resolve_destination("./data/results.csv", "CSVWithNames").unwrap();
        assert_eq!(
            result,
            "INSERT INTO FUNCTION file('./data/results.csv', 'CSVWithNames')"
        );
    }

    #[test]
    fn test_dest_local_bare_filename() {
        let result = resolve_destination("output.parquet", "Parquet").unwrap();
        assert_eq!(
            result,
            "INSERT INTO FUNCTION file('output.parquet', 'Parquet')"
        );
    }

    #[test]
    fn test_dest_file_uri() {
        let result = resolve_destination("file:///tmp/data.json", "JSONEachRow").unwrap();
        assert_eq!(
            result,
            "INSERT INTO FUNCTION file('/tmp/data.json', 'JSONEachRow')"
        );
    }

    #[test]
    fn test_dest_s3() {
        let result = resolve_destination("s3://mybucket/results.parquet", "Parquet").unwrap();
        assert_eq!(
            result,
            "INSERT INTO FUNCTION s3('s3://mybucket/results.parquet', 'Parquet')"
        );
    }

    #[test]
    fn test_dest_gs() {
        let result = resolve_destination("gs://mybucket/data.csv", "CSVWithNames").unwrap();
        assert_eq!(
            result,
            "INSERT INTO FUNCTION s3('https://storage.googleapis.com/mybucket/data.csv', 'CSVWithNames')"
        );
    }

    #[test]
    fn test_dest_azure() {
        let result =
            resolve_destination("azure://mycontainer/path/data.parquet", "Parquet").unwrap();
        assert_eq!(
            result,
            "INSERT INTO FUNCTION azureBlobStorage(\
             getenv('AZURE_STORAGE_CONNECTION_STRING'), 'mycontainer', 'path/data.parquet', 'Parquet')"
        );
    }

    #[test]
    fn test_dest_https() {
        let result =
            resolve_destination("https://webhook.example.com/ingest", "JSONEachRow").unwrap();
        assert_eq!(
            result,
            "INSERT INTO FUNCTION url('https://webhook.example.com/ingest', 'JSONEachRow')"
        );
    }

    #[test]
    fn test_dest_http() {
        let result = resolve_destination("http://localhost:8080/sink", "CSVWithNames").unwrap();
        assert_eq!(
            result,
            "INSERT INTO FUNCTION url('http://localhost:8080/sink', 'CSVWithNames')"
        );
    }

    #[test]
    fn test_dest_path_with_quotes_escaped() {
        let result = resolve_destination("/tmp/it's a file.parquet", "Parquet").unwrap();
        assert!(result.contains("it\\'s a file.parquet"));
    }

    #[test]
    fn test_dest_path_with_backslashes_escaped() {
        let result = resolve_destination("C:\\tmp\\out.parquet", "Parquet").unwrap();
        assert!(result.contains("C:\\\\tmp\\\\out.parquet"));
    }

    // --- build_export_sql ---

    #[test]
    fn test_build_export_sql_simple() {
        let sql = build_export_sql(
            "SELECT name FROM users",
            "/tmp/users.parquet",
            "Parquet",
            &ExportConfig::default(),
        )
        .unwrap();
        assert_eq!(
            sql,
            "INSERT INTO FUNCTION file('/tmp/users.parquet', 'Parquet') SELECT name FROM users"
        );
    }

    #[test]
    fn test_build_export_sql_s3_with_compression() {
        let config = ExportConfig {
            compression: Some("zstd".to_string()),
        };
        let sql =
            build_export_sql("SELECT 1", "s3://bucket/out.parquet", "Parquet", &config).unwrap();
        assert!(sql.contains("INSERT INTO FUNCTION s3("));
        assert!(sql.contains("output_format_parquet_compression_method = 'zstd'"));
    }

    #[test]
    fn test_build_export_sql_compression_ignored_for_csv() {
        let config = ExportConfig {
            compression: Some("gzip".to_string()),
        };
        let sql = build_export_sql("SELECT 1", "/tmp/out.csv", "CSVWithNames", &config).unwrap();
        assert!(!sql.contains("compression_method"));
    }

    #[test]
    fn test_build_export_sql_invalid_compression() {
        let config = ExportConfig {
            compression: Some("lzma".to_string()),
        };
        let result = build_export_sql("SELECT 1", "/tmp/out.parquet", "Parquet", &config);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("lzma"));
    }

    // --- parse_export_call ---

    #[test]
    fn test_parse_export_call_basic() {
        let arg0 = Expression::Literal(Literal::String("MATCH (n) RETURN n"));
        let arg1 = Expression::Literal(Literal::String("/tmp/output.parquet"));
        let args: Vec<&Expression> = vec![&arg0, &arg1];
        let result = parse_export_call(&args).unwrap();
        assert_eq!(result.cypher_query, "MATCH (n) RETURN n");
        assert_eq!(result.destination, "/tmp/output.parquet");
        assert!(result.config.compression.is_none());
    }

    #[test]
    fn test_parse_export_call_with_config() {
        let arg0 = Expression::Literal(Literal::String("MATCH (n) RETURN n"));
        let arg1 = Expression::Literal(Literal::String("/tmp/output.parquet"));
        let arg2 = Expression::MapLiteral(vec![(
            "compression",
            Expression::Literal(Literal::String("zstd")),
        )]);
        let args: Vec<&Expression> = vec![&arg0, &arg1, &arg2];
        let result = parse_export_call(&args).unwrap();
        assert_eq!(result.config.compression.as_deref(), Some("zstd"));
    }

    #[test]
    fn test_parse_export_call_too_few_args() {
        let arg0 = Expression::Literal(Literal::String("MATCH (n) RETURN n"));
        let args: Vec<&Expression> = vec![&arg0];
        assert!(parse_export_call(&args).is_err());
    }

    #[test]
    fn test_parse_export_call_non_string_arg() {
        let arg0 = Expression::Literal(Literal::Integer(42));
        let arg1 = Expression::Literal(Literal::String("/tmp/output.parquet"));
        let args: Vec<&Expression> = vec![&arg0, &arg1];
        assert!(parse_export_call(&args).is_err());
    }

    #[test]
    fn test_parse_export_call_empty_config_map() {
        let arg0 = Expression::Literal(Literal::String("MATCH (n) RETURN n"));
        let arg1 = Expression::Literal(Literal::String("/tmp/out.csv"));
        let arg2 = Expression::MapLiteral(vec![]);
        let args: Vec<&Expression> = vec![&arg0, &arg1, &arg2];
        let result = parse_export_call(&args).unwrap();
        assert!(result.config.compression.is_none());
    }
}
