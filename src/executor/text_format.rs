//! Render Databricks query results as ClickHouse-style text output.
//!
//! The Databricks Statement Execution API only returns structured data
//! (JSON arrays-of-arrays plus a column manifest) — it has no server-side
//! tabular/pretty renderer like ClickHouse's `PrettyCompact`. So when a
//! caller asks the `DatabricksSqlExecutor` for a text format, we format the
//! rows here, client-side.
//!
//! The only caller is the HTTP `/query` endpoint, which routes
//! `Pretty`/`PrettyCompact`/`CSV`/`CSVWithNames` through `execute_text`
//! (see `server::handlers::execute_cte_queries`) — notably what the
//! interactive `clickgraph-client` REPL requests. Those four formats are
//! the entire supported set; anything else is an explicit
//! `UnsupportedFormat` error rather than silently wrong output.
//!
//! Cells are left-aligned (ClickHouse right-aligns numerics; we keep it
//! simple) and `NULL` renders as an empty cell.

use serde_json::Value;

use super::ExecutorError;

/// Whether `format` can be rendered here. Lets callers fail fast (before
/// running a warehouse query) on a format they can't emit.
pub(crate) fn is_supported(format: &str) -> bool {
    matches!(format, "CSV" | "CSVWithNames" | "Pretty" | "PrettyCompact")
}

/// Render `rows` (positional arrays, column names in `columns`) as `format`.
pub(crate) fn format_rows(
    columns: &[String],
    rows: &[Vec<Value>],
    format: &str,
) -> Result<String, ExecutorError> {
    match format {
        "CSV" => Ok(csv(columns, rows, false)),
        "CSVWithNames" => Ok(csv(columns, rows, true)),
        "Pretty" | "PrettyCompact" => Ok(pretty(columns, rows)),
        other => Err(ExecutorError::UnsupportedFormat(other.to_string())),
    }
}

/// Render one JSON value as a plain cell string. Strings are unquoted;
/// arrays/objects fall back to compact JSON; `NULL` is empty.
fn cell(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        // Arrays / objects: compact JSON (e.g. collect() results).
        other => other.to_string(),
    }
}

fn csv(columns: &[String], rows: &[Vec<Value>], with_names: bool) -> String {
    let mut out = String::new();
    if with_names {
        out.push_str(
            &columns
                .iter()
                .map(|c| csv_escape(c))
                .collect::<Vec<_>>()
                .join(","),
        );
        out.push('\n');
    }
    for row in rows {
        let line = row
            .iter()
            .map(|v| csv_escape(&cell(v)))
            .collect::<Vec<_>>()
            .join(",");
        out.push_str(&line);
        out.push('\n');
    }
    out
}

/// Quote per RFC 4180 when the field contains a delimiter, quote, or newline.
fn csv_escape(s: &str) -> String {
    if s.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// Box-drawn table in ClickHouse `PrettyCompact` style.
fn pretty(columns: &[String], rows: &[Vec<Value>]) -> String {
    if columns.is_empty() {
        // No manifest columns (e.g. a DDL statement) — nothing to tabulate.
        return String::new();
    }
    let n = columns.len();

    // Pre-render every cell so width math and emission agree on the text.
    let cells: Vec<Vec<String>> = rows.iter().map(|r| r.iter().map(cell).collect()).collect();

    let mut widths: Vec<usize> = columns.iter().map(|c| c.chars().count()).collect();
    for row in &cells {
        for (i, c) in row.iter().enumerate() {
            if i < n {
                widths[i] = widths[i].max(c.chars().count());
            }
        }
    }

    // A horizontal rule: left/mid/right corners with `─` runs padded to
    // width+2 (one space of padding either side of each cell).
    let bar = |left: &str, mid: &str, right: &str| -> String {
        let mut s = String::from(left);
        for (i, w) in widths.iter().enumerate() {
            s.push_str(&"─".repeat(w + 2));
            s.push_str(if i + 1 < n { mid } else { right });
        }
        s.push('\n');
        s
    };

    let emit_row = |out: &mut String, vals: &[String]| {
        out.push('│');
        for (i, w) in widths.iter().enumerate() {
            let c = vals.get(i).map(String::as_str).unwrap_or("");
            let pad = w.saturating_sub(c.chars().count());
            out.push(' ');
            out.push_str(c);
            out.push_str(&" ".repeat(pad));
            out.push(' ');
            out.push('│');
        }
        out.push('\n');
    };

    let mut out = String::new();
    out.push_str(&bar("┌", "┬", "┐"));
    emit_row(&mut out, columns);
    out.push_str(&bar("├", "┼", "┤"));
    for row in &cells {
        emit_row(&mut out, row);
    }
    out.push_str(&bar("└", "┴", "┘"));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cols() -> Vec<String> {
        vec!["name".to_string(), "age".to_string()]
    }

    fn rows() -> Vec<Vec<Value>> {
        vec![
            vec![json!("Alice"), json!(30)],
            vec![json!("Bob"), json!(25)],
        ]
    }

    #[test]
    fn csv_no_header() {
        let out = format_rows(&cols(), &rows(), "CSV").unwrap();
        assert_eq!(out, "Alice,30\nBob,25\n");
    }

    #[test]
    fn csv_with_names_has_header() {
        let out = format_rows(&cols(), &rows(), "CSVWithNames").unwrap();
        assert_eq!(out, "name,age\nAlice,30\nBob,25\n");
    }

    #[test]
    fn csv_escapes_commas_quotes_newlines() {
        let r = vec![vec![json!("Smith, Jr"), json!("a\"b")]];
        let out = format_rows(&cols(), &r, "CSV").unwrap();
        assert_eq!(out, "\"Smith, Jr\",\"a\"\"b\"\n");
    }

    #[test]
    fn csv_null_is_empty_field() {
        let r = vec![vec![json!("Alice"), Value::Null]];
        let out = format_rows(&cols(), &r, "CSV").unwrap();
        assert_eq!(out, "Alice,\n");
    }

    #[test]
    fn pretty_aligns_and_boxes() {
        // col0 width = max("name"=4, "Alice"=5) = 5; col1 = max("age"=3, 2) = 3.
        let out = format_rows(&cols(), &rows(), "PrettyCompact").unwrap();
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 6); // top, header, separator, 2 rows, bottom
        assert_eq!(lines[1], "│ name  │ age │");
        assert_eq!(lines[3], "│ Alice │ 30  │");
        assert_eq!(lines[4], "│ Bob   │ 25  │");
        assert!(lines[0].starts_with('┌') && lines[0].ends_with('┐'));
        assert!(lines[5].starts_with('└') && lines[5].ends_with('┘'));
    }

    #[test]
    fn pretty_empty_columns_is_empty_string() {
        let out = format_rows(&[], &[], "Pretty").unwrap();
        assert_eq!(out, "");
    }

    #[test]
    fn unsupported_format_errors() {
        let err = format_rows(&cols(), &rows(), "JSONEachRow").unwrap_err();
        assert!(matches!(err, ExecutorError::UnsupportedFormat(f) if f == "JSONEachRow"));
    }
}
