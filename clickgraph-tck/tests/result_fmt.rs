//! Format QueryResult rows for comparison against openCypher TCK expected tables.
//!
//! TCK expected values use a Cypher-literal display format:
//! - Strings: `'hello'`  (single-quoted)
//! - Integers: `42`
//! - Floats: `3.14`  (trailing `.0` stripped when whole number)
//! - Booleans: `true` / `false`
//! - Null: `null`
//! - Nodes: `(:Label {prop: val})`
//! - Relationships: `[:TYPE]` or `[:TYPE {prop: val}]`
//! - Lists: `[1, 2, 3]`
//!
//! Rows are compared as strings where column values are joined with ` | `.

use std::collections::HashMap;

use clickgraph_embedded::Value;

// Re-export gherkin types used for TCK table parsing
pub use cucumber::gherkin::Table as GherkinTable;

/// Separator between column values within a row string.
pub const COL_SEP: &str = " | ";

/// Normalize a row string for comparison.
///
/// In ClickHouse, boolean expressions (IS NULL, =, <, etc.) return UInt8 (0/1),
/// but the TCK expects `true`/`false`. Normalize `"0"` → `"false"` and
/// `"1"` → `"true"` so boolean results compare correctly.
///
/// Also normalizes `"''"` → `"null"` (empty string returned for missing nullable
/// properties) and strips surrounding whitespace from each cell.
pub fn normalize_row(row: &str) -> String {
    row.split(COL_SEP)
        .map(|cell| {
            let c = cell.trim();
            // Normalize whole-number float strings (e.g. "2.0" → "2") so that
            // Float64(2.0) from ClickHouse JSON (serialized as integer 2) compares
            // equal to the TCK expected "2.0". ClickHouse JSON output cannot
            // distinguish Float64(2.0) from Int64(2) for whole-number values.
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

/// If `cell` is a whole-number float string like `"-2.0"` or `"42.0"`, strip the `.0`.
/// This normalizes Float64/Int64 display differences from ClickHouse JSON output.
fn normalize_float_cell(cell: &str) -> String {
    // Must end with ".0" and have no other decimal places
    if let Some(prefix) = cell.strip_suffix(".0") {
        // prefix should be an integer (possibly negative)
        let digits = if let Some(p) = prefix.strip_prefix('-') {
            p
        } else {
            prefix
        };
        if !digits.is_empty() && digits.chars().all(|c| c.is_ascii_digit()) {
            return prefix.to_string();
        }
    }
    cell.to_string()
}

// ---------------------------------------------------------------------------
// Value formatting
// ---------------------------------------------------------------------------

/// Format a `Value` as a TCK display string.
pub fn format_value(val: &Value) -> String {
    match val {
        Value::Null => "null".to_string(),
        Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
        Value::Int64(n) => n.to_string(),
        Value::Float64(f) => format_float(*f),
        Value::String(s) => {
            // If the string looks like a JSON array, parse and format as a Cypher list.
            // This handles cases where ClickHouse returns arrays as string representations.
            if s.starts_with('[') {
                if let Ok(serde_json::Value::Array(items)) =
                    serde_json::from_str::<serde_json::Value>(s)
                {
                    let parts: Vec<String> = items.iter().map(format_json_value).collect();
                    return format!("[{}]", parts.join(", "));
                }
            }
            format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'"))
        }
        Value::List(items) => {
            let parts: Vec<String> = items.iter().map(format_value).collect();
            format!("[{}]", parts.join(", "))
        }
        Value::Date(d) => format!("'{d}'"),
        Value::Timestamp(t) => format!("'{t}'"),
        Value::UUID(u) => format!("'{u}'"),
        Value::Map(m) => {
            // Detect ClickGraph packed node format: {__label__: "A", properties: "{...}"}
            let has_label = m.iter().any(|(k, _)| k == "__label__");
            if has_label {
                return format_packed_node_value(m);
            }
            // Detect packed relationship format: {type: [...], start_id: ..., end_id: ...}
            if m.iter().any(|(k, _)| k == "type") && m.iter().any(|(k, _)| k == "start_id") {
                return format_packed_edge_value(m);
            }
            let mut pairs: Vec<_> = m.iter().collect();
            pairs.sort_by_key(|(k, _)| k.clone());
            let s: Vec<String> = pairs
                .iter()
                .map(|(k, v)| format!("{}: {}", k, format_map_value(v)))
                .collect();
            format!("{{{}}}", s.join(", "))
        }
    }
}

/// Format a map value, trying to restore numeric types that were stringified by toString().
/// ClickHouse map() requires uniform value types, so integer/float literals are wrapped
/// in toString() during SQL generation. This function reverses that for display.
fn format_map_value(val: &Value) -> String {
    match val {
        Value::String(s) => {
            // Try to detect numbers that were stringified by toString()
            if let Ok(n) = s.parse::<i64>() {
                return format!("{}", n);
            }
            if let Ok(f) = s.parse::<f64>() {
                return format_float(f);
            }
            format_value(val)
        }
        _ => format_value(val),
    }
}

/// Format a packed ClickGraph node value: `{__label__: "A", properties: "{...}"}`.
fn format_packed_node_value(m: &[(String, Value)]) -> String {
    let label = m
        .iter()
        .find(|(k, _)| k == "__label__")
        .and_then(|(_, v)| {
            if let Value::String(s) = v {
                Some(s.as_str())
            } else {
                None
            }
        })
        .unwrap_or("");

    let props = m
        .iter()
        .find(|(k, _)| k == "properties")
        .and_then(|(_, v)| {
            if let Value::String(s) = v {
                Some(s.as_str())
            } else {
                None
            }
        })
        .unwrap_or("{}");

    format_packed_node(label, props)
}

/// Format a packed ClickGraph edge value: `{type: ["T"], start_id: ..., end_id: ..., properties: [...]}`.
fn format_packed_edge_value(m: &[(String, Value)]) -> String {
    let rel_type = m
        .iter()
        .find(|(k, _)| k == "type")
        .map(|(_, v)| match v {
            Value::List(items) => items
                .first()
                .and_then(|i| {
                    if let Value::String(s) = i {
                        Some(s.as_str())
                    } else {
                        None
                    }
                })
                .unwrap_or(""),
            Value::String(s) => s.as_str(),
            _ => "",
        })
        .unwrap_or("");

    // Edge properties
    let props_list = m
        .iter()
        .find(|(k, _)| k == "properties")
        .and_then(|(_, v)| {
            if let Value::List(items) = v {
                Some(items)
            } else {
                None
            }
        });

    let prop_str = if let Some(items) = props_list {
        items
            .first()
            .and_then(|i| {
                if let Value::String(s) = i {
                    Some(s.as_str())
                } else {
                    None
                }
            })
            .unwrap_or("{}")
    } else {
        "{}"
    };

    format_packed_rel(rel_type, prop_str)
}

/// Parse a JSON properties blob and build a TCK node string.
fn format_packed_node(label: &str, properties_json: &str) -> String {
    // __Unlabeled is a synthetic schema label for nodes with no user-visible label
    let label = if label == "__Unlabeled" { "" } else { label };
    let props = parse_props_json(properties_json, Some(label));

    let visible_props: Vec<_> = props
        .iter()
        .filter(|(k, v)| is_visible_prop(k, v))
        .collect();

    if visible_props.is_empty() {
        if label.is_empty() {
            "()".to_string()
        } else {
            format!("(:{label})")
        }
    } else {
        let prop_str = visible_props
            .iter()
            .map(|(k, v)| format!("{k}: {v}"))
            .collect::<Vec<_>>()
            .join(", ");
        if label.is_empty() {
            format!("({{{}}})", prop_str)
        } else {
            format!("(:{label} {{{}}})", prop_str)
        }
    }
}

/// Parse a JSON properties blob for a relationship and build a TCK rel string.
fn format_packed_rel(rel_type: &str, properties_json: &str) -> String {
    let props = parse_props_json(properties_json, None);
    let visible_props: Vec<_> = props
        .iter()
        .filter(|(k, v)| is_visible_prop(k, v))
        .collect();

    if visible_props.is_empty() {
        format!("[:{rel_type}]")
    } else {
        let prop_str = visible_props
            .iter()
            .map(|(k, v)| format!("{k}: {v}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("[:{rel_type} {{{}}}]", prop_str)
    }
}

/// Internal field names to always hide from TCK node output.
/// Note: "id" is intentionally NOT listed here — it is a user-defined property in TCK tests
/// (the internal node ID column is "_tck_id", not "id").
const INTERNAL_FIELDS: &[&str] = &[
    "_tck_id",
    "_version",
    "from_id",
    "to_id",
    "__label__",
    "start_id",
    "end_id",
];

/// Return true if a property should appear in TCK output.
/// Hides internal fields and null/empty values (explicit 0/false are shown).
fn is_visible_prop(key: &str, val: &str) -> bool {
    if INTERNAL_FIELDS
        .iter()
        .any(|f| key == *f || key.ends_with(&format!(".{f}")))
    {
        return false;
    }
    is_visible_prop_str(val)
}

/// Return true if a formatted property value is non-default.
/// Only suppress null/empty-string values — explicit 0 and false are meaningful.
fn is_visible_prop_str(val: &str) -> bool {
    !matches!(val, "" | "''" | "null")
}

/// Return true if a Value is an unset (null) property.
/// With Nullable columns, unset properties are NULL; explicit 0/false are real values.
fn is_default_value(val: &Value) -> bool {
    matches!(val, Value::Null)
}

/// Parse a JSON object string into sorted `(key, formatted_value)` pairs.
/// Strips table-name prefixes like `tck_n_a.` from keys.
fn parse_props_json(json: &str, node_label: Option<&str>) -> Vec<(String, String)> {
    // Fast-path: empty or trivial
    let json = json.trim();
    if json == "{}" || json.is_empty() {
        return Vec::new();
    }

    // Use serde_json to parse
    if let Ok(serde_json::Value::Object(map)) = serde_json::from_str::<serde_json::Value>(json) {
        let mut result: Vec<(String, String)> = map
            .iter()
            .map(|(k, v)| {
                let clean_key = strip_table_prefix(k, node_label);
                let formatted = format_json_value(v);
                (clean_key, formatted)
            })
            .collect();
        result.sort_by(|(a, _), (b, _)| a.cmp(b));
        result
    } else {
        Vec::new()
    }
}

/// Strip ClickHouse table prefix from a property key.
/// e.g. `tck_n_a.num` → `num`, `num` → `num`
fn strip_table_prefix(key: &str, _node_label: Option<&str>) -> String {
    if let Some(dot) = key.find('.') {
        let prefix = &key[..dot];
        // Strip if it looks like a table prefix (starts with "tck_")
        if prefix.starts_with("tck_") {
            return key[dot + 1..].to_string();
        }
    }
    key.to_string()
}

/// Format a serde_json Value as a TCK display string.
fn format_json_value(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.to_string()
            } else if let Some(f) = n.as_f64() {
                format_float(f)
            } else {
                n.to_string()
            }
        }
        serde_json::Value::String(s) => {
            // If the string looks like a JSON array, parse and format as a Cypher list.
            if s.starts_with('[') {
                if let Ok(serde_json::Value::Array(items)) =
                    serde_json::from_str::<serde_json::Value>(s)
                {
                    let parts: Vec<String> = items.iter().map(format_json_value).collect();
                    return format!("[{}]", parts.join(", "));
                }
            }
            format!("'{}'", s.replace('\'', "\\'"))
        }
        serde_json::Value::Array(items) => {
            let parts: Vec<String> = items.iter().map(format_json_value).collect();
            format!("[{}]", parts.join(", "))
        }
        serde_json::Value::Object(m) => {
            let mut pairs: Vec<_> = m.iter().collect();
            pairs.sort_by_key(|(k, _)| k.clone());
            let s: Vec<String> = pairs
                .iter()
                .map(|(k, v)| format!("{k}: {}", format_json_value(v)))
                .collect();
            format!("{{{}}}", s.join(", "))
        }
    }
}

/// Format a float: openCypher floats always show a decimal point (e.g. `2.0`, `0.5`).
/// Only strip `.0` when the caller is specifically asked to match integer format.
fn format_float(f: f64) -> String {
    if f.is_nan() {
        return "NaN".to_string();
    }
    if f.is_infinite() {
        return if f > 0.0 { "Infinity" } else { "-Infinity" }.to_string();
    }
    if f.fract() == 0.0 && f.abs() < 1e15 {
        // Whole number float — preserve the decimal point per openCypher semantics
        return format!("{:.1}", f);
    }
    // Use default float formatting
    format!("{f}")
}

// ---------------------------------------------------------------------------
// Node reconstruction
// ---------------------------------------------------------------------------

/// Reconstruct a `(:Label {prop: val})` string from a set of flat columns
/// belonging to the same node variable (`var.prop → value`).
///
/// `col_values` contains only columns for this variable (already extracted).
/// `label` is the node label looked up from the query's MATCH clause.
/// The `_tck_id` column is excluded from the output.
pub fn format_node_from_cols(cols_and_vals: &[(String, Value)], label: Option<&str>) -> String {
    // Detect packed format: {__label__: "A", properties: "{json}", id: UUID}
    let has_packed = cols_and_vals.iter().any(|(k, _)| k == "__label__");
    let has_props_col = cols_and_vals.iter().any(|(k, _)| k == "properties");
    if has_packed && has_props_col {
        let packed_label = cols_and_vals
            .iter()
            .find(|(k, _)| k == "__label__")
            .and_then(|(_, v)| {
                if let Value::String(s) = v {
                    Some(s.as_str())
                } else {
                    None
                }
            })
            .unwrap_or("");
        let props_json = cols_and_vals
            .iter()
            .find(|(k, _)| k == "properties")
            .and_then(|(_, v)| {
                if let Value::String(s) = v {
                    Some(s.as_str())
                } else {
                    None
                }
            })
            .unwrap_or("{}");
        let effective_label = if packed_label.is_empty() {
            label.unwrap_or("")
        } else {
            packed_label
        };
        return format_packed_node(effective_label, props_json);
    }

    // Try to get label from __label__ column if present (non-packed format)
    let col_label = cols_and_vals
        .iter()
        .find(|(k, _)| k == "__label__")
        .and_then(|(_, v)| {
            if let Value::String(s) = v {
                Some(s.as_str())
            } else {
                None
            }
        })
        .filter(|l| !l.is_empty() && *l != "__Unlabeled");
    let label_str = col_label.or(label).unwrap_or("");

    let mut props: Vec<(String, String)> = cols_and_vals
        .iter()
        .filter(|(col, val)| !INTERNAL_FIELDS.iter().any(|f| *col == *f) && !is_default_value(val))
        .map(|(col, val)| (col.clone(), format_map_value(val)))
        .collect();
    props.sort_by_key(|(k, _)| k.clone());

    // Filter out props whose formatted value is hidden (e.g. null/empty)
    props.retain(|(_, v)| is_visible_prop_str(v));

    if props.is_empty() {
        if label_str.is_empty() {
            "()".to_string()
        } else {
            format!("(:{label_str})")
        }
    } else {
        let prop_str = props
            .iter()
            .map(|(k, v)| format!("{k}: {v}"))
            .collect::<Vec<_>>()
            .join(", ");
        if label_str.is_empty() {
            format!("({{{}}})", prop_str)
        } else {
            format!("(:{label_str} {{{}}})", prop_str)
        }
    }
}

/// Format a relationship: `[:TYPE]` or `[:TYPE {prop: val}]`.
pub fn format_rel_from_cols(cols_and_vals: &[(String, Value)], rel_type: &str) -> String {
    // Detect packed relationship format: {type: List, properties: List, start_id, end_id}
    let type_col = cols_and_vals.iter().find(|(k, _)| k == "type");
    let props_col = cols_and_vals.iter().find(|(k, _)| k == "properties");
    if type_col.is_some() && props_col.is_some() {
        // Extract rel type from List
        let effective_type = if rel_type.is_empty() {
            type_col
                .and_then(|(_, v)| match v {
                    Value::List(items) => items.first().and_then(|i| {
                        if let Value::String(s) = i {
                            Some(s.as_str())
                        } else {
                            None
                        }
                    }),
                    Value::String(s) => Some(s.as_str()),
                    _ => None,
                })
                .unwrap_or("")
        } else {
            rel_type
        };

        // Extract properties JSON from List
        let props_json = props_col
            .and_then(|(_, v)| match v {
                Value::List(items) => items.first().and_then(|i| {
                    if let Value::String(s) = i {
                        Some(s.as_str())
                    } else {
                        None
                    }
                }),
                Value::String(s) => Some(s.as_str()),
                _ => None,
            })
            .unwrap_or("{}");

        return format_packed_rel(effective_type, props_json);
    }

    let mut props: Vec<(String, String)> = cols_and_vals
        .iter()
        .filter(|(col, _)| col != "from_id" && col != "to_id" && col != "_tck_id")
        .map(|(col, val)| (col.clone(), format_value(val)))
        .filter(|(_, v)| is_visible_prop_str(v))
        .collect();
    props.sort_by_key(|(k, _)| k.clone());

    if props.is_empty() {
        format!("[:{rel_type}]")
    } else {
        let prop_str = props
            .iter()
            .map(|(k, v)| format!("{k}: {v}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("[:{rel_type} {{{}}}]", prop_str)
    }
}

// ---------------------------------------------------------------------------
// Row formatting
// ---------------------------------------------------------------------------

/// Format a result row (as a list of `Value` with their column names) into the
/// TCK row comparison string.
///
/// `col_names` is the ordered list of column names from the query result.
/// `values` is the row data in the same order.
/// `var_labels` maps alias names (e.g. `n`) to node labels (e.g. `Person`).
/// `var_rel_types` maps alias names (e.g. `r`) to relationship types.
pub fn format_row(
    col_names: &[String],
    values: &[Value],
    var_labels: &HashMap<String, String>,
    var_rel_types: &HashMap<String, String>,
) -> String {
    assert_eq!(col_names.len(), values.len(), "column count mismatch");

    // Group columns by variable prefix.
    // e.g. "n.name", "n.age" → prefix "n", props ["name", "age"]
    // e.g. "count(*)" → scalar (no prefix)
    let mut groups: Vec<(String, Vec<usize>)> = Vec::new(); // (prefix, [col_index])

    let mut assigned: Vec<bool> = vec![false; col_names.len()];

    for i in 0..col_names.len() {
        if assigned[i] {
            continue;
        }
        let col = &col_names[i];
        if let Some(dot) = col.find('.') {
            let prefix = &col[..dot];
            // Collect all columns with the same prefix
            let mut group_idxs = vec![i];
            for j in (i + 1)..col_names.len() {
                if !assigned[j] && col_names[j].starts_with(&format!("{prefix}.")) {
                    group_idxs.push(j);
                    assigned[j] = true;
                }
            }
            assigned[i] = true;
            groups.push((prefix.to_string(), group_idxs));
        } else {
            assigned[i] = true;
            groups.push((col.clone(), vec![i]));
        }
    }

    // Now format each group
    let parts: Vec<String> = groups
        .iter()
        .map(|(prefix, idxs)| {
            if idxs.len() == 1 {
                let idx = idxs[0];
                let col = &col_names[idx];
                let val = &values[idx];

                // Check if this single column looks like a node var (e.g. `n` with no dot)
                // — this shouldn't happen for ClickGraph since RETURN n expands to n.prop*
                // but handle gracefully
                if !col.contains('.') {
                    // It's a scalar or aggregate column
                    format_value(val)
                } else {
                    // Single property access: `n.name` → just format the value
                    // Unless it's the only column for a node (e.g. node with one property)
                    // and the caller wants node representation — check var_labels
                    let prop = &col[col.find('.').unwrap() + 1..];
                    if prop == "_tck_id" || prop == "*" {
                        // Internal ID column — part of a node but shown as ID
                        // This means RETURN n was used but node has no user properties
                        let label = var_labels.get(prefix.as_str()).map(|s| s.as_str());
                        format_node_from_cols(&[], label)
                    } else {
                        format_value(val)
                    }
                }
            } else {
                // Multiple columns with same prefix → node or relationship
                let prop_vals: Vec<(String, Value)> = idxs
                    .iter()
                    .map(|&idx| {
                        let prop = col_names[idx]
                            .find('.')
                            .map(|d| col_names[idx][d + 1..].to_string())
                            .unwrap_or_else(|| col_names[idx].clone());
                        (prop, values[idx].clone())
                    })
                    .collect();

                // Detect relationship by column structure: has "type" + ("start_id" or "end_id")
                let is_rel = prop_vals.iter().any(|(k, _)| k == "type")
                    && prop_vals
                        .iter()
                        .any(|(k, _)| k == "start_id" || k == "end_id");

                // Detect whether this is a node return (RETURN n) or individual property
                // accesses (RETURN n.name, n.age). A node return always includes at least
                // one internal/structural column (_tck_id, __label__, properties, start_id,
                // end_id). If no such column is present, the columns are explicit property
                // projections and should be formatted as individual scalars.
                let has_structural = prop_vals.iter().any(|(k, _)| {
                    matches!(
                        k.as_str(),
                        "_tck_id" | "__label__" | "properties" | "start_id" | "end_id" | "*"
                    )
                });

                if let Some(rel_type) = var_rel_types.get(prefix.as_str()) {
                    format_rel_from_cols(&prop_vals, rel_type)
                } else if is_rel {
                    format_rel_from_cols(&prop_vals, "")
                } else if has_structural {
                    let label = var_labels.get(prefix.as_str()).map(|s| s.as_str());
                    format_node_from_cols(&prop_vals, label)
                } else {
                    // Explicit property projections: format each as a scalar value.
                    prop_vals
                        .iter()
                        .map(|(_, v)| format_value(v))
                        .collect::<Vec<_>>()
                        .join(COL_SEP)
                }
            }
        })
        .collect();

    parts.join(COL_SEP)
}

// ---------------------------------------------------------------------------
// TCK expected table parsing
// ---------------------------------------------------------------------------

/// Parse a Gherkin data table into a list of row strings for comparison.
///
/// The first row is the header (column names).  Each subsequent row becomes
/// a row string with values joined by `COL_SEP`.
///
/// Normalizations applied to expected values:
/// - Trim whitespace around cells
/// - `COUNT_STAR()` header → `count(*)`
/// - `\\n` in string cells → `\n`
pub fn parse_expected_table(table: &GherkinTable) -> (Vec<String>, Vec<String>) {
    let mut rows = table.rows.iter();

    // First row: headers
    let headers: Vec<String> = rows
        .next()
        .map(|r| r.iter().map(|c| normalize_header(c.trim())).collect())
        .unwrap_or_default();

    // Remaining rows: data
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
    let h = h.replace("COUNT_STAR()", "count(*)");
    h
}

/// Normalize an expected cell value from the Gherkin table.
fn normalize_expected_value(val: &str) -> String {
    // Replace escaped newlines
    val.replace("\\n", "\n")
}

// ---------------------------------------------------------------------------
// Variable label extraction
// ---------------------------------------------------------------------------

/// Extract variable→label mappings from a Cypher MATCH/CREATE clause.
///
/// Parses patterns like `(n:Person)`, `(m:Movie)` from the query.
/// Returns a map from variable name to first label.
pub fn extract_var_labels(query: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    // Simple regex-free approach: scan for `(var:Label` patterns
    let chars: Vec<char> = query.chars().collect();
    let len = chars.len();
    let mut pos = 0;

    while pos < len {
        if chars[pos] == '(' {
            pos += 1;
            // Skip whitespace
            while pos < len && chars[pos].is_whitespace() {
                pos += 1;
            }
            // Try to read variable name
            let var_start = pos;
            while pos < len && (chars[pos].is_ascii_alphanumeric() || chars[pos] == '_') {
                pos += 1;
            }
            let var_name = chars[var_start..pos].iter().collect::<String>();
            // Skip whitespace
            while pos < len && chars[pos].is_whitespace() {
                pos += 1;
            }
            // Check for label
            if pos < len && chars[pos] == ':' {
                pos += 1;
                while pos < len && chars[pos].is_whitespace() {
                    pos += 1;
                }
                let lbl_start = pos;
                while pos < len && (chars[pos].is_ascii_alphanumeric() || chars[pos] == '_') {
                    pos += 1;
                }
                let label = chars[lbl_start..pos].iter().collect::<String>();
                if !var_name.is_empty() && !label.is_empty() {
                    map.insert(var_name, label);
                }
            }
        } else {
            pos += 1;
        }
    }
    map
}

/// Extract variable→rel_type mappings from a Cypher MATCH clause.
///
/// Parses patterns like `[r:KNOWS]` from the query.
pub fn extract_var_rel_types(query: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let chars: Vec<char> = query.chars().collect();
    let len = chars.len();
    let mut pos = 0;

    while pos < len {
        if chars[pos] == '[' {
            pos += 1;
            while pos < len && chars[pos].is_whitespace() {
                pos += 1;
            }
            let var_start = pos;
            while pos < len && (chars[pos].is_ascii_alphanumeric() || chars[pos] == '_') {
                pos += 1;
            }
            let var_name = chars[var_start..pos].iter().collect::<String>();
            while pos < len && chars[pos].is_whitespace() {
                pos += 1;
            }
            if pos < len && chars[pos] == ':' {
                pos += 1;
                while pos < len && chars[pos].is_whitespace() {
                    pos += 1;
                }
                let type_start = pos;
                while pos < len && (chars[pos].is_ascii_alphanumeric() || chars[pos] == '_') {
                    pos += 1;
                }
                let rel_type = chars[type_start..pos].iter().collect::<String>();
                // For multi-type patterns like [r:KNOWS|HATES], skip static type assignment
                // so the actual type is read from the row's path_relationships column.
                let is_multi_type = pos < len && chars[pos] == '|';
                if !var_name.is_empty() && !rel_type.is_empty() && !is_multi_type {
                    map.insert(var_name, rel_type);
                }
            }
        } else {
            pos += 1;
        }
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_value_int() {
        assert_eq!(format_value(&Value::Int64(42)), "42");
    }

    #[test]
    fn test_format_value_float_whole() {
        assert_eq!(format_value(&Value::Float64(1.0)), "1.0");
    }

    #[test]
    fn test_format_value_float_frac() {
        assert_eq!(format_value(&Value::Float64(3.14)), "3.14");
    }

    #[test]
    fn test_format_value_string() {
        assert_eq!(format_value(&Value::String("hello".into())), "'hello'");
    }

    #[test]
    fn test_extract_var_labels() {
        let m = extract_var_labels("MATCH (n:Person)-[:KNOWS]->(m:Movie)");
        assert_eq!(m.get("n").map(|s| s.as_str()), Some("Person"));
        assert_eq!(m.get("m").map(|s| s.as_str()), Some("Movie"));
    }

    #[test]
    fn test_extract_var_rel_types() {
        let m = extract_var_rel_types("MATCH (:A)-[r:KNOWS]->(:B)");
        assert_eq!(m.get("r").map(|s| s.as_str()), Some("KNOWS"));
    }
}
