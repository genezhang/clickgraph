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
    List(Vec<Value>),
    Map(Vec<(String, Value)>),
}

impl Value {
    /// Return the value as a `&str`, or `None` if not a string.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s.as_str()),
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

    /// Return true if this value is `Null`.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

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
            JsonValue::String(s) => Value::String(s),
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
            format!("{}", Value::List(vec![Value::Int64(1), Value::Int64(2)])),
            "[1, 2]"
        );
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Int64(n) => write!(f, "{}", n),
            Value::Float64(n) => write!(f, "{}", n),
            Value::String(s) => write!(f, "{}", s),
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
