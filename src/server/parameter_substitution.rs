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

        Value::Number(n) if n.is_i64() => Ok(n.as_i64().unwrap().to_string()),

        Value::Number(n) if n.is_u64() => Ok(n.as_u64().unwrap().to_string()),

        Value::Number(n) if n.is_f64() => {
            let f = n.as_f64().unwrap();
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
            let items: Result<Vec<String>, _> = arr.iter().map(|v| format_parameter(v)).collect();
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

/// Substitute parameters in SQL string
///
/// Replaces all $paramName placeholders with properly escaped values.
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
    let mut chars = sql.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '$' {
            // Found potential parameter
            let mut param_name = String::new();

            // Collect parameter name (alphanumeric + underscore)
            while let Some(&next_ch) = chars.peek() {
                if next_ch.is_alphanumeric() || next_ch == '_' {
                    param_name.push(next_ch);
                    chars.next();
                } else {
                    break;
                }
            }

            if param_name.is_empty() {
                // Just a lone $ character
                result.push('$');
            } else {
                // Validate parameter name
                if !is_valid_parameter_name(&param_name) {
                    return Err(ParameterSubstitutionError::InvalidParameterName(param_name));
                }

                // Look up parameter value
                match parameters.get(&param_name) {
                    Some(value) => {
                        let formatted = format_parameter(value)?;
                        result.push_str(&formatted);
                    }
                    None => {
                        return Err(ParameterSubstitutionError::MissingParameter(param_name));
                    }
                }
            }
        } else {
            result.push(ch);
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
        let value = json!(3.14);
        assert_eq!(format_parameter(&value).unwrap(), "3.14");
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
}
