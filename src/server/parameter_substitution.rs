/// Parameter substitution and SQL escaping for ClickHouse
///
/// This module provides safe parameter substitution by replacing $paramName placeholders
/// with properly escaped values in SQL strings.
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, thiserror::Error)]
pub enum ParameterSubstitutionError {
    #[error("Missing required parameter: {0}")]
    MissingParameter(String),

    #[error("Invalid parameter name: {0} (must be alphanumeric or underscore)")]
    InvalidParameterName(String),

    #[error("Unsupported parameter type for value: {0}")]
    UnsupportedType(String),
}

/// Escape a string value for use in ClickHouse SQL
///
/// ClickHouse string escaping rules:
/// - Backslash \ escapes special characters
/// - Single quotes must be escaped as \'
/// - Backslashes must be escaped as \\
/// - Newlines, tabs, etc. must be escaped
fn escape_string(s: &str) -> String {
    s.replace('\\', "\\\\") // Must be first!
        .replace('\'', "\\'")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
        .replace('\0', "\\0")
}

/// Format a parameter value as SQL literal
///
/// # Arguments
/// * `value` - The JSON value to format
///
/// # Returns
/// String representation suitable for direct SQL insertion
fn format_parameter(value: &Value) -> Result<String, ParameterSubstitutionError> {
    match value {
        Value::String(s) => Ok(format!("'{}'", escape_string(s))),

        Value::Number(n) if n.is_i64() => n.as_i64().map(|num| num.to_string()).ok_or_else(|| {
            ParameterSubstitutionError::UnsupportedType(format!(
                "Failed to convert JSON number to i64: {}",
                n
            ))
        }),

        Value::Number(n) if n.is_u64() => n.as_u64().map(|num| num.to_string()).ok_or_else(|| {
            ParameterSubstitutionError::UnsupportedType(format!(
                "Failed to convert JSON number to u64: {}",
                n
            ))
        }),

        Value::Number(n) if n.is_f64() => {
            let f = n.as_f64().ok_or_else(|| {
                ParameterSubstitutionError::UnsupportedType(format!(
                    "Failed to convert JSON number to f64: {}",
                    n
                ))
            })?;
            if f.is_finite() {
                Ok(f.to_string())
            } else {
                Err(ParameterSubstitutionError::UnsupportedType(format!(
                    "Non-finite float: {}",
                    f
                )))
            }
        }

        Value::Bool(b) => Ok(if *b { "1".to_string() } else { "0".to_string() }),

        Value::Array(arr) => {
            let items: Result<Vec<String>, _> = arr.iter().map(format_parameter).collect();
            Ok(format!("[{}]", items?.join(", ")))
        }

        Value::Null => Ok("NULL".to_string()),

        Value::Object(_) => Err(ParameterSubstitutionError::UnsupportedType(
            "Object/Map parameters not supported. Consider converting to JSON string.".to_string(),
        )),

        _ => Err(ParameterSubstitutionError::UnsupportedType(format!(
            "Unsupported value type: {:?}",
            value
        ))),
    }
}

/// Validate parameter name (alphanumeric + underscore only)
fn is_valid_parameter_name(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c.is_alphanumeric() || c == '_')
}

/// A `$name` placeholder of `sql`, or a run of text: a placeholder is read
/// only outside string literals and quoted identifiers. A `$` inside one is
/// text — a Cypher string (`'cost $x'`) or a name (`` `a$b` ``, an alias
/// `"k$x"`) is no parameter, and putting a value there would end the literal
/// or the identifier (#1489 review).
enum SqlPiece<'s> {
    Text(&'s str),
    Placeholder(&'s str),
}

/// The pieces of `sql`, in order. Quoting as the SQL printer writes it:
/// `'…'` with `\` escapes and `''`; `"…"` (double-quoted identifiers) with `\`
/// escapes and `""`; `` `…` `` (backtick-quoted identifiers) with a doubled
/// backtick and no escapes.
fn sql_pieces(sql: &str) -> Vec<SqlPiece<'_>> {
    let mut pieces = Vec::new();
    let mut text_start = 0;
    let mut chars = sql.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        match c {
            '\'' | '"' | '`' => {
                // Skip to the closing quote.
                while let Some((_, d)) = chars.next() {
                    if d == '\\' && c != '`' {
                        chars.next();
                    } else if d == c {
                        if chars.peek().is_some_and(|&(_, e)| e == c) {
                            chars.next(); // a doubled quote
                        } else {
                            break;
                        }
                    }
                }
            }
            '$' => {
                let start = i + 1;
                let mut end = start;
                while let Some(&(j, d)) = chars.peek() {
                    if d.is_alphanumeric() || d == '_' {
                        end = j + d.len_utf8();
                        chars.next();
                    } else {
                        break;
                    }
                }
                if end > start {
                    pieces.push(SqlPiece::Text(&sql[text_start..i]));
                    pieces.push(SqlPiece::Placeholder(&sql[start..end]));
                    text_start = end;
                }
            }
            _ => {}
        }
    }
    pieces.push(SqlPiece::Text(&sql[text_start..]));
    pieces
}

/// Check if SQL contains unsubstituted $param placeholders
///
/// Returns Some(param_name) if an unsubstituted placeholder is found,
/// None if the SQL is clean. A `$` inside a string literal or a quoted
/// identifier is no placeholder.
///
/// # Example
/// ```ignore
/// assert_eq!(find_unsubstituted_parameter("SELECT * FROM t"), None);
/// assert_eq!(find_unsubstituted_parameter("SELECT * FROM t(x = $foo)"), Some("foo".to_string()));
/// ```
pub fn find_unsubstituted_parameter(sql: &str) -> Option<String> {
    sql_pieces(sql).into_iter().find_map(|p| match p {
        SqlPiece::Placeholder(name) if is_valid_parameter_name(name) => Some(name.to_string()),
        _ => None,
    })
}

/// Substitute parameters in SQL string
///
/// Replaces all $paramName placeholders with properly escaped values. A
/// `$` inside a string literal or a quoted identifier is left as it is.
///
/// # Arguments
/// * `sql` - SQL string with $paramName placeholders
/// * `parameters` - HashMap of parameter names to values
///
/// # Returns
/// SQL string with all parameters substituted
///
/// # Errors
/// - `MissingParameter` if a placeholder is found but no value provided
/// - `InvalidParameterName` if a parameter name contains invalid characters
/// - `UnsupportedType` if a value cannot be formatted as SQL
///
/// # Example
/// ```ignore
/// use serde_json::json;
/// use std::collections::HashMap;
///
/// let mut params = HashMap::new();
/// params.insert("email".to_string(), json!("alice@example.com"));
/// params.insert("minAge".to_string(), json!(25));
///
/// let sql = "SELECT * FROM users WHERE email = $email AND age > $minAge";
/// let result = substitute_parameters(sql, &params).unwrap();
/// // Result: "SELECT * FROM users WHERE email = 'alice@example.com' AND age > 25"
/// ```
pub fn substitute_parameters(
    sql: &str,
    parameters: &HashMap<String, Value>,
) -> Result<String, ParameterSubstitutionError> {
    let mut result = String::with_capacity(sql.len() * 2); // Pre-allocate
    for piece in sql_pieces(sql) {
        match piece {
            SqlPiece::Text(t) => result.push_str(t),
            SqlPiece::Placeholder(name) => {
                if !is_valid_parameter_name(name) {
                    return Err(ParameterSubstitutionError::InvalidParameterName(
                        name.to_string(),
                    ));
                }
                match parameters.get(name) {
                    Some(value) => result.push_str(&format_parameter(value)?),
                    None => {
                        return Err(ParameterSubstitutionError::MissingParameter(
                            name.to_string(),
                        ));
                    }
                }
            }
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_escape_string() {
        assert_eq!(escape_string("hello"), "hello");
        assert_eq!(escape_string("hello'world"), "hello\\'world");
        assert_eq!(escape_string("path\\to\\file"), "path\\\\to\\\\file");
        assert_eq!(escape_string("line1\nline2"), "line1\\nline2");
        assert_eq!(escape_string("tab\there"), "tab\\there");
    }

    #[test]
    fn test_format_parameter_string() {
        let value = json!("alice@example.com");
        assert_eq!(format_parameter(&value).unwrap(), "'alice@example.com'");
    }

    #[test]
    fn test_format_parameter_string_with_quotes() {
        let value = json!("O'Brien");
        assert_eq!(format_parameter(&value).unwrap(), "'O\\'Brien'");
    }

    #[test]
    fn test_format_parameter_integer() {
        let value = json!(42);
        assert_eq!(format_parameter(&value).unwrap(), "42");

        let value = json!(-123);
        assert_eq!(format_parameter(&value).unwrap(), "-123");
    }

    #[test]
    fn test_format_parameter_float() {
        let value = json!(2.72);
        assert_eq!(format_parameter(&value).unwrap(), "2.72");
    }

    #[test]
    fn test_format_parameter_boolean() {
        let value = json!(true);
        assert_eq!(format_parameter(&value).unwrap(), "1");

        let value = json!(false);
        assert_eq!(format_parameter(&value).unwrap(), "0");
    }

    #[test]
    fn test_format_parameter_null() {
        let value = json!(null);
        assert_eq!(format_parameter(&value).unwrap(), "NULL");
    }

    #[test]
    fn test_format_parameter_array() {
        let value = json!([1, 2, 3]);
        assert_eq!(format_parameter(&value).unwrap(), "[1, 2, 3]");

        let value = json!(["alice", "bob"]);
        assert_eq!(format_parameter(&value).unwrap(), "['alice', 'bob']");
    }

    #[test]
    fn test_substitute_parameters_simple() {
        let mut params = HashMap::new();
        params.insert("email".to_string(), json!("alice@example.com"));

        let sql = "SELECT * FROM users WHERE email = $email";
        let result = substitute_parameters(sql, &params).unwrap();
        assert_eq!(
            result,
            "SELECT * FROM users WHERE email = 'alice@example.com'"
        );
    }

    #[test]
    fn test_substitute_parameters_multiple() {
        let mut params = HashMap::new();
        params.insert("email".to_string(), json!("alice@example.com"));
        params.insert("minAge".to_string(), json!(25));

        let sql = "SELECT * FROM users WHERE email = $email AND age > $minAge";
        let result = substitute_parameters(sql, &params).unwrap();
        assert_eq!(
            result,
            "SELECT * FROM users WHERE email = 'alice@example.com' AND age > 25"
        );
    }

    #[test]
    fn test_substitute_parameters_in_clause() {
        let mut params = HashMap::new();
        params.insert("ids".to_string(), json!([1, 2, 3]));

        let sql = "SELECT * FROM users WHERE id IN $ids";
        let result = substitute_parameters(sql, &params).unwrap();
        assert_eq!(result, "SELECT * FROM users WHERE id IN [1, 2, 3]");
    }

    #[test]
    fn test_substitute_parameters_missing() {
        let params = HashMap::new();

        let sql = "SELECT * FROM users WHERE email = $email";
        let result = substitute_parameters(sql, &params);
        assert!(result.is_err());
        assert!(matches!(
            result,
            Err(ParameterSubstitutionError::MissingParameter(_))
        ));
    }

    #[test]
    fn test_substitute_parameters_sql_injection_prevention() {
        let mut params = HashMap::new();
        params.insert("email".to_string(), json!("' OR '1'='1"));

        let sql = "SELECT * FROM users WHERE email = $email";
        let result = substitute_parameters(sql, &params).unwrap();
        // Single quotes should be escaped
        assert_eq!(
            result,
            "SELECT * FROM users WHERE email = '\\' OR \\'1\\'=\\'1'"
        );
    }

    #[test]
    fn test_substitute_parameters_no_parameters() {
        let params = HashMap::new();

        let sql = "SELECT * FROM users";
        let result = substitute_parameters(sql, &params).unwrap();
        assert_eq!(result, "SELECT * FROM users");
    }

    #[test]
    fn test_lone_dollar_sign() {
        let params = HashMap::new();

        let sql = "SELECT price * 1.1 AS price_with_tax WHERE currency = '$'";
        let result = substitute_parameters(sql, &params).unwrap();
        assert_eq!(
            result,
            "SELECT price * 1.1 AS price_with_tax WHERE currency = '$'"
        );
    }

    /// A `$` inside a string literal or a quoted identifier is no
    /// placeholder: a value put there would end the literal or the name and
    /// run as SQL (#1489 review: `{`k$x`: 1}`, `RETURN 1 AS a$b`,
    /// `'cost $x'`).
    #[test]
    fn a_dollar_inside_quotes_is_no_placeholder_1489() {
        let mut params = HashMap::new();
        params.insert(
            "x".to_string(),
            json!(" , 1) AS m0, currentUser() AS i, map("),
        );
        params.insert("b".to_string(), json!("zz"));
        params.insert("p".to_string(), json!(7));
        for sql in [
            r#"SELECT map('k$x', 1) AS "m""#,
            r#"SELECT 1 AS "a$b""#,
            "SELECT 'cost $x' AS c",
            r"SELECT 'it\'s $x' AS c",
            "SELECT 'it''s $x' AS c",
            r#"SELECT 1 AS "a\"$x""#,
            r#"SELECT 1 AS "a""$x""#,
            "SELECT 1 AS `a$b`, 2 AS `c``$x`",
        ] {
            assert_eq!(substitute_parameters(sql, &params).unwrap(), sql, "{sql}");
            assert_eq!(find_unsubstituted_parameter(sql), None, "{sql}");
        }
        // Outside the quotes a placeholder is one, also right after them.
        assert_eq!(
            substitute_parameters(r#"SELECT 'a$x' AS "b$x", $p AS q, '\\'||$p"#, &params).unwrap(),
            r#"SELECT 'a$x' AS "b$x", 7 AS q, '\\'||7"#
        );
        assert_eq!(
            find_unsubstituted_parameter(r#"SELECT 'a$x' AS "b", $p"#),
            Some("p".to_string())
        );
        // An unterminated literal hides the rest (no placeholder is read
        // inside it).
        assert_eq!(find_unsubstituted_parameter("SELECT 'a $p"), None);
    }
}
