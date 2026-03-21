//! `Value` type for query result cells.

use serde_json::Value as JsonValue;

/// A single value in a query result row.
///
/// Mirrors `kuzu::types::Value` but maps to ClickHouse/chdb types.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    String(String),
    /// Date in `YYYY-MM-DD` format (e.g., `"2024-01-15"`).
    Date(String),
    /// Timestamp in `YYYY-MM-DD HH:MM:SS` format (e.g., `"2024-01-15 10:30:00"`).
    Timestamp(String),
    /// UUID in standard `8-4-4-4-12` hex format.
    UUID(String),
    List(Vec<Value>),
    Map(Vec<(String, Value)>),
}

impl Value {
    /// Create a `Value::String` without type detection.
    ///
    /// Use this when you explicitly want a string value regardless of content.
    /// `Value::from(json!("2024-01-15"))` would auto-detect as `Value::Date`,
    /// but `Value::string("2024-01-15")` always produces `Value::String`.
    pub fn string(s: impl Into<String>) -> Self {
        Value::String(s.into())
    }

    /// Return the value as a `&str`, or `None` if not a string-like type.
    ///
    /// Returns the inner string for `String`, `Date`, `Timestamp`, and `UUID`.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) | Value::Date(s) | Value::Timestamp(s) | Value::UUID(s) => {
                Some(s.as_str())
            }
            _ => None,
        }
    }

    /// Return the value as an `i64`, or `None` if not an integer.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int64(n) => Some(*n),
            _ => None,
        }
    }

    /// Return the value as an `f64`, or `None` if not a float.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float64(f) => Some(*f),
            _ => None,
        }
    }

    /// Return the value as a `bool`, or `None` if not a boolean.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Return true if this value is `Null`.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Return the type name of this value as a static string.
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "Null",
            Value::Bool(_) => "Bool",
            Value::Int64(_) => "Int64",
            Value::Float64(_) => "Float64",
            Value::String(_) => "String",
            Value::Date(_) => "Date",
            Value::Timestamp(_) => "Timestamp",
            Value::UUID(_) => "UUID",
            Value::List(_) => "List",
            Value::Map(_) => "Map",
        }
    }

    /// Render this value as a SQL literal for use in INSERT statements.
    ///
    /// - `String` values are single-quoted with backslashes and quotes escaped.
    /// - `Date` renders as `toDate('YYYY-MM-DD')`.
    /// - `Timestamp` renders as `toDateTime('YYYY-MM-DD HH:MM:SS')`.
    /// - `UUID` renders as `toUUID('...')`.
    /// - `Int64` and `Float64` render as bare numeric literals.
    /// - `Bool` renders as `1` (true) or `0` (false).
    /// - `Null` renders as `NULL`.
    /// - `List` and `Map` return `Err` (not supported in INSERT).
    pub fn to_sql_literal(&self) -> Result<String, String> {
        match self {
            Value::Null => Ok("NULL".to_string()),
            Value::Bool(true) => Ok("1".to_string()),
            Value::Bool(false) => Ok("0".to_string()),
            Value::Int64(n) => Ok(n.to_string()),
            Value::Float64(f) => Ok(f.to_string()),
            // Escape backslashes first, then single quotes, to prevent
            // backslash-quote injection (e.g., `\' OR 1=1--`).
            Value::String(s) => {
                let escaped = s.replace('\\', "\\\\").replace('\'', "''");
                Ok(format!("'{}'", escaped))
            }
            Value::Date(s) => {
                let escaped = s.replace('\\', "\\\\").replace('\'', "''");
                Ok(format!("toDate('{}')", escaped))
            }
            Value::Timestamp(s) => {
                let escaped = s.replace('\\', "\\\\").replace('\'', "''");
                Ok(format!("toDateTime('{}')", escaped))
            }
            Value::UUID(s) => {
                let escaped = s.replace('\\', "\\\\").replace('\'', "''");
                Ok(format!("toUUID('{}')", escaped))
            }
            Value::List(_) => Err("List values are not supported in INSERT statements".to_string()),
            Value::Map(_) => Err("Map values are not supported in INSERT statements".to_string()),
        }
    }
}

/// Check if a string matches the `YYYY-MM-DD` date format.
fn is_date_string(s: &str) -> bool {
    if s.len() != 10 {
        return false;
    }
    let b = s.as_bytes();
    // YYYY-MM-DD: digits at 0-3, '-' at 4, digits at 5-6, '-' at 7, digits at 8-9
    b[4] == b'-'
        && b[7] == b'-'
        && b[0..4].iter().all(|c| c.is_ascii_digit())
        && b[5..7].iter().all(|c| c.is_ascii_digit())
        && b[8..10].iter().all(|c| c.is_ascii_digit())
}

/// Check if a string matches the `YYYY-MM-DD HH:MM:SS` datetime format.
fn is_timestamp_string(s: &str) -> bool {
    if s.len() < 19 {
        return false;
    }
    let b = s.as_bytes();
    // YYYY-MM-DD HH:MM:SS — validate date prefix, space, and full HH:MM:SS
    is_date_string(&s[..10])
        && b[10] == b' '
        && b[11].is_ascii_digit()
        && b[12].is_ascii_digit()
        && b[13] == b':'
        && b[14].is_ascii_digit()
        && b[15].is_ascii_digit()
        && b[16] == b':'
        && b[17].is_ascii_digit()
        && b[18].is_ascii_digit()
}

/// Check if a string matches the UUID `8-4-4-4-12` hex format.
fn is_uuid_string(s: &str) -> bool {
    if s.len() != 36 {
        return false;
    }
    let b = s.as_bytes();
    b[8] == b'-'
        && b[13] == b'-'
        && b[18] == b'-'
        && b[23] == b'-'
        && b.iter()
            .enumerate()
            .all(|(i, c)| i == 8 || i == 13 || i == 18 || i == 23 || c.is_ascii_hexdigit())
}

/// Convert a `serde_json::Value` to an embedded `Value`.
///
/// **Type detection**: JSON strings are automatically classified as
/// `Date`, `Timestamp`, or `UUID` when they match the expected format
/// from ClickHouse's `JSONEachRow` output. Use `Value::string()` to
/// bypass detection when you need a plain `Value::String`.
impl From<JsonValue> for Value {
    fn from(v: JsonValue) -> Self {
        match v {
            JsonValue::Null => Value::Null,
            JsonValue::Bool(b) => Value::Bool(b),
            JsonValue::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Int64(i)
                } else {
                    Value::Float64(n.as_f64().unwrap_or(f64::NAN))
                }
            }
            JsonValue::String(s) => {
                // Detect typed strings from ClickHouse JSON output.
                // Order matters: check timestamp before date (timestamp starts with date).
                if is_uuid_string(&s) {
                    Value::UUID(s)
                } else if is_timestamp_string(&s) {
                    Value::Timestamp(s)
                } else if is_date_string(&s) {
                    Value::Date(s)
                } else {
                    Value::String(s)
                }
            }
            JsonValue::Array(arr) => Value::List(arr.into_iter().map(Value::from).collect()),
            JsonValue::Object(obj) => {
                Value::Map(obj.into_iter().map(|(k, v)| (k, Value::from(v))).collect())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_null_conversion() {
        assert_eq!(Value::from(json!(null)), Value::Null);
        assert!(Value::Null.is_null());
    }

    #[test]
    fn test_bool_conversion() {
        assert_eq!(Value::from(json!(true)), Value::Bool(true));
        assert_eq!(Value::from(json!(false)), Value::Bool(false));
        assert_eq!(Value::Bool(true).as_bool(), Some(true));
        assert_eq!(Value::Bool(false).as_bool(), Some(false));
        assert_eq!(Value::Int64(1).as_bool(), None);
    }

    #[test]
    fn test_integer_conversion() {
        let v = Value::from(json!(42));
        assert_eq!(v, Value::Int64(42));
        assert_eq!(v.as_i64(), Some(42));
        assert_eq!(v.as_f64(), None);
    }

    #[test]
    fn test_float_conversion() {
        let v = Value::from(json!(1.5));
        assert!(matches!(v, Value::Float64(_)));
        assert!(v.as_f64().is_some());
        assert_eq!(v.as_i64(), None);
    }

    #[test]
    fn test_string_conversion() {
        let v = Value::from(json!("hello"));
        assert_eq!(v, Value::String("hello".to_string()));
        assert_eq!(v.as_str(), Some("hello"));
    }

    #[test]
    fn test_date_detection() {
        let v = Value::from(json!("2024-01-15"));
        assert_eq!(v, Value::Date("2024-01-15".to_string()));
        assert_eq!(v.type_name(), "Date");
        assert_eq!(v.as_str(), Some("2024-01-15"));
    }

    #[test]
    fn test_timestamp_detection() {
        let v = Value::from(json!("2024-01-15 10:30:00"));
        assert_eq!(v, Value::Timestamp("2024-01-15 10:30:00".to_string()));
        assert_eq!(v.type_name(), "Timestamp");
        assert_eq!(v.as_str(), Some("2024-01-15 10:30:00"));
    }

    #[test]
    fn test_uuid_detection() {
        let v = Value::from(json!("550e8400-e29b-41d4-a716-446655440000"));
        assert_eq!(
            v,
            Value::UUID("550e8400-e29b-41d4-a716-446655440000".to_string())
        );
        assert_eq!(v.type_name(), "UUID");
        assert_eq!(v.as_str(), Some("550e8400-e29b-41d4-a716-446655440000"));
    }

    #[test]
    fn test_value_string_bypasses_detection() {
        // Value::string() always produces String, even for date-like input
        assert_eq!(
            Value::string("2024-01-15"),
            Value::String("2024-01-15".to_string())
        );
        assert_eq!(
            Value::string("550e8400-e29b-41d4-a716-446655440000"),
            Value::String("550e8400-e29b-41d4-a716-446655440000".to_string())
        );
    }

    #[test]
    fn test_timestamp_partial_time_not_detected() {
        // Only digits + colon at position 13, but rest is garbage
        assert_eq!(
            Value::from(json!("2024-01-15 12:XXXXX")),
            Value::String("2024-01-15 12:XXXXX".to_string())
        );
    }

    #[test]
    fn test_date_like_strings_not_misdetected() {
        // Too short
        assert_eq!(
            Value::from(json!("2024-01")),
            Value::String("2024-01".to_string())
        );
        // Wrong separators
        assert_eq!(
            Value::from(json!("2024/01/15")),
            Value::String("2024/01/15".to_string())
        );
        // Not digits
        assert_eq!(
            Value::from(json!("abcd-ef-gh")),
            Value::String("abcd-ef-gh".to_string())
        );
    }

    #[test]
    fn test_uuid_like_strings_not_misdetected() {
        // Wrong length
        assert_eq!(
            Value::from(json!("550e8400-e29b-41d4")),
            Value::String("550e8400-e29b-41d4".to_string())
        );
        // Non-hex character
        assert_eq!(
            Value::from(json!("550e8400-e29b-41d4-a716-44665544000g")),
            Value::String("550e8400-e29b-41d4-a716-44665544000g".to_string())
        );
    }

    #[test]
    fn test_array_conversion() {
        let v = Value::from(json!([1, 2, 3]));
        assert!(matches!(v, Value::List(_)));
        if let Value::List(items) = v {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], Value::Int64(1));
        }
    }

    #[test]
    fn test_object_conversion() {
        let v = Value::from(json!({"key": "val"}));
        assert!(matches!(v, Value::Map(_)));
        if let Value::Map(pairs) = v {
            assert_eq!(pairs.len(), 1);
            assert_eq!(pairs[0].0, "key");
            assert_eq!(pairs[0].1, Value::String("val".to_string()));
        }
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", Value::Null), "NULL");
        assert_eq!(format!("{}", Value::Bool(true)), "true");
        assert_eq!(format!("{}", Value::Int64(42)), "42");
        assert_eq!(format!("{}", Value::String("hi".to_string())), "hi");
        assert_eq!(
            format!("{}", Value::Date("2024-01-15".to_string())),
            "2024-01-15"
        );
        assert_eq!(
            format!("{}", Value::Timestamp("2024-01-15 10:30:00".to_string())),
            "2024-01-15 10:30:00"
        );
        assert_eq!(
            format!(
                "{}",
                Value::UUID("550e8400-e29b-41d4-a716-446655440000".to_string())
            ),
            "550e8400-e29b-41d4-a716-446655440000"
        );
        assert_eq!(
            format!("{}", Value::List(vec![Value::Int64(1), Value::Int64(2)])),
            "[1, 2]"
        );
    }

    // --- to_sql_literal tests ---

    #[test]
    fn test_sql_literal_string_with_escaping() {
        assert_eq!(
            Value::String("O'Brien".to_string())
                .to_sql_literal()
                .unwrap(),
            "'O''Brien'"
        );
        assert_eq!(
            Value::String("hello".to_string()).to_sql_literal().unwrap(),
            "'hello'"
        );
        assert_eq!(
            Value::String("".to_string()).to_sql_literal().unwrap(),
            "''"
        );
        // Backslash escaping prevents SQL injection
        assert_eq!(
            Value::String("test\\' OR 1=1--".to_string())
                .to_sql_literal()
                .unwrap(),
            "'test\\\\'' OR 1=1--'"
        );
    }

    #[test]
    fn test_sql_literal_int64_and_float64() {
        assert_eq!(Value::Int64(42).to_sql_literal().unwrap(), "42");
        assert_eq!(Value::Int64(-1).to_sql_literal().unwrap(), "-1");
        assert_eq!(Value::Float64(3.14).to_sql_literal().unwrap(), "3.14");
        assert_eq!(Value::Float64(0.0).to_sql_literal().unwrap(), "0");
    }

    #[test]
    fn test_sql_literal_bool() {
        assert_eq!(Value::Bool(true).to_sql_literal().unwrap(), "1");
        assert_eq!(Value::Bool(false).to_sql_literal().unwrap(), "0");
    }

    #[test]
    fn test_sql_literal_null() {
        assert_eq!(Value::Null.to_sql_literal().unwrap(), "NULL");
    }

    #[test]
    fn test_sql_literal_date() {
        assert_eq!(
            Value::Date("2024-01-15".to_string())
                .to_sql_literal()
                .unwrap(),
            "toDate('2024-01-15')"
        );
    }

    #[test]
    fn test_sql_literal_timestamp() {
        assert_eq!(
            Value::Timestamp("2024-01-15 10:30:00".to_string())
                .to_sql_literal()
                .unwrap(),
            "toDateTime('2024-01-15 10:30:00')"
        );
    }

    #[test]
    fn test_sql_literal_uuid() {
        assert_eq!(
            Value::UUID("550e8400-e29b-41d4-a716-446655440000".to_string())
                .to_sql_literal()
                .unwrap(),
            "toUUID('550e8400-e29b-41d4-a716-446655440000')"
        );
    }

    #[test]
    fn test_sql_literal_list_and_map_return_err() {
        assert!(Value::List(vec![]).to_sql_literal().is_err());
        assert!(Value::Map(vec![]).to_sql_literal().is_err());
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Int64(n) => write!(f, "{}", n),
            Value::Float64(n) => write!(f, "{}", n),
            Value::String(s) | Value::Date(s) | Value::Timestamp(s) | Value::UUID(s) => {
                write!(f, "{}", s)
            }
            Value::List(items) => {
                write!(f, "[")?;
                for (i, v) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", v)?;
                }
                write!(f, "]")
            }
            Value::Map(pairs) => {
                write!(f, "{{")?;
                for (i, (k, v)) in pairs.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", k, v)?;
                }
                write!(f, "}}")
            }
        }
    }
}
