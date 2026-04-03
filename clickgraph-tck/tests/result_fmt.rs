//! TCK-specific result formatting helpers.
//!
//! Core formatting functions (`format_value`, `format_row`, `format_node_from_cols`,
//! `format_rel_from_cols`, `extract_var_labels`, `extract_var_rel_types`) are provided
//! by `clickgraph_embedded::result_display` and re-exported here for convenience.
//!
//! This module adds the TCK-specific layer on top:
//! - [`normalize_row`] — normalizes ClickHouse output for TCK comparison
//!   (maps `0`/`1` → `false`/`true`, empty strings → `null`)
//! - [`parse_expected_table`] — parses a Gherkin data table into expected row strings

// Re-export core formatting from the promoted embedded module.
pub use clickgraph_embedded::result_display::{
    extract_var_labels, extract_var_rel_types, format_node_from_cols, format_rel_from_cols,
    format_row, format_value,
};

// Re-export gherkin types used for TCK table parsing.
pub use cucumber::gherkin::Table as GherkinTable;

/// Separator between column values within a row string.
pub const COL_SEP: &str = " | ";

/// Normalize a row string for TCK comparison.
///
/// ClickHouse boolean expressions return UInt8 (0/1), but the TCK expects `true`/`false`.
/// Also normalizes `"''"` → `"null"` (empty string returned for missing nullable properties)
/// and whole-number float strings (e.g. `"2.0"` → `"2"` when the TCK uses integer display).
pub fn normalize_row(row: &str) -> String {
    row.split(COL_SEP)
        .map(|cell| {
            let c = cell.trim();
            let c = normalize_float_cell(c);
            match c.as_str() {
                "0" => "false".to_string(),
                "1" => "true".to_string(),
                "''" => "null".to_string(),
                other => other.to_string(),
            }
        })
        .collect::<Vec<_>>()
        .join(COL_SEP)
}

/// Strip trailing `.0` from whole-number float strings (e.g. `"-2.0"` → `"-2"`).
fn normalize_float_cell(cell: &str) -> String {
    if let Some(prefix) = cell.strip_suffix(".0") {
        let digits = prefix.strip_prefix('-').unwrap_or(prefix);
        if !digits.is_empty() && digits.chars().all(|c| c.is_ascii_digit()) {
            return prefix.to_string();
        }
    }
    cell.to_string()
}

/// Parse a Gherkin data table into a list of row strings for comparison.
///
/// The first row is the header (column names). Each subsequent row becomes
/// a row string with values joined by [`COL_SEP`].
///
/// Normalizations applied to expected values:
/// - Trim whitespace around cells
/// - `COUNT_STAR()` header → `count(*)`
/// - `\\n` in string cells → `\n`
pub fn parse_expected_table(table: &GherkinTable) -> (Vec<String>, Vec<String>) {
    let mut rows = table.rows.iter();

    let headers: Vec<String> = rows
        .next()
        .map(|r| r.iter().map(|c| normalize_header(c.trim())).collect())
        .unwrap_or_default();

    let data_rows: Vec<String> = rows
        .map(|row| {
            let cells: Vec<String> = row
                .iter()
                .map(|c| normalize_expected_value(c.trim()))
                .collect();
            cells.join(COL_SEP)
        })
        .collect();

    (headers, data_rows)
}

fn normalize_header(h: &str) -> String {
    h.replace("COUNT_STAR()", "count(*)")
}

fn normalize_expected_value(val: &str) -> String {
    val.replace("\\n", "\n")
}
