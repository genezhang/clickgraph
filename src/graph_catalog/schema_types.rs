//! Simple, database-agnostic type system for schema configuration
//!
//! This module provides a minimal set of types that map cleanly across different
//! database systems (ClickHouse, PostgreSQL, DuckDB, etc.) for use in schema YAML
//! configuration files.
//!
//! # Supported Types
//!
//! - `integer` - Whole numbers (ClickHouse: Int*/UInt*, PostgreSQL: BIGINT)
//! - `float` - Decimal numbers (ClickHouse: Float*/Decimal*, PostgreSQL: DOUBLE PRECISION)
//! - `string` - Text (ClickHouse: String, PostgreSQL: VARCHAR/TEXT)
//! - `boolean` - True/False (ClickHouse: Bool/UInt8, PostgreSQL: BOOLEAN)
//! - `datetime` - Timestamps (ClickHouse: DateTime*, PostgreSQL: TIMESTAMP)
//! - `date` - Dates (ClickHouse: Date*, PostgreSQL: DATE)
//! - `uuid` - UUIDs (ClickHouse: UUID, PostgreSQL: UUID)
//!
//! # Example
//!
//! ```yaml
//! node_id:
//!   column: user_id
//!   type: integer    # Simple, database-agnostic
//! ```

use serde::{Deserialize, Serialize};
use std::fmt;

use crate::server::models::SqlDialect;

/// Simple, database-agnostic type for schema configuration
///
/// These types are intentionally minimal and map cleanly to equivalent types
/// across different database systems. This supports ClickGraph's multi-database
/// vision (ClickHouse, PostgreSQL, DuckDB, MySQL, SQLite).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SchemaType {
    /// Whole numbers (Int8/16/32/64, UInt8/16/32/64, BIGINT, etc.)
    Integer,
    
    /// Decimal numbers (Float32/64, Decimal, DOUBLE PRECISION, etc.)
    Float,
    
    /// Text (String, FixedString, VARCHAR, TEXT, etc.)
    String,
    
    /// True/False (Bool, UInt8, BOOLEAN, etc.)
    Boolean,
    
    /// Timestamps (DateTime, DateTime64, TIMESTAMP, etc.)
    DateTime,
    
    /// Dates (Date, Date32, DATE, etc.)
    Date,
    
    /// UUIDs (UUID)
    Uuid,
}

impl SchemaType {
    /// Parse a type string from YAML configuration
    ///
    /// Case-insensitive and supports common aliases for convenience.
    ///
    /// # Supported aliases
    ///
    /// - `integer`: int, long, Integer, INT
    /// - `float`: double, decimal, Float, FLOAT
    /// - `string`: text, String, TEXT
    /// - `boolean`: bool, Boolean, BOOL
    /// - `datetime`: timestamp, DateTime, TIMESTAMP
    /// - `date`: Date, DATE
    /// - `uuid`: UUID
    ///
    /// # Example
    ///
    /// ```ignore
    /// let t = SchemaType::from_str("int")?;
    /// assert_eq!(t, SchemaType::Integer);
    ///
    /// let t = SchemaType::from_str("Integer")?;
    /// assert_eq!(t, SchemaType::Integer);
    /// ```
    pub fn from_str(s: &str) -> Result<Self, String> {
        match s.to_lowercase().trim() {
            "integer" | "int" | "long" => Ok(SchemaType::Integer),
            "float" | "double" | "decimal" => Ok(SchemaType::Float),
            "string" | "text" => Ok(SchemaType::String),
            "boolean" | "bool" => Ok(SchemaType::Boolean),
            "datetime" | "timestamp" => Ok(SchemaType::DateTime),
            "date" => Ok(SchemaType::Date),
            "uuid" => Ok(SchemaType::Uuid),
            _ => Err(format!(
                "Unknown type: '{}'. Supported: integer, float, string, boolean, datetime, date, uuid",
                s
            )),
        }
    }

    /// Convert a string value to a SQL literal with correct type
    ///
    /// This is used to generate performant SQL predicates from elementId values.
    /// The generated SQL uses native types (not toString) for index usage.
    ///
    /// # Arguments
    ///
    /// * `value` - String representation of the value (from elementId)
    /// * `dialect` - SQL dialect for database-specific formatting
    ///
    /// # Returns
    ///
    /// SQL literal string ready for use in WHERE clauses
    ///
    /// # Example
    ///
    /// ```ignore
    /// let t = SchemaType::Integer;
    /// let sql = t.to_sql_literal("123", SqlDialect::ClickHouse)?;
    /// assert_eq!(sql, "123");
    ///
    /// let t = SchemaType::String;
    /// let sql = t.to_sql_literal("alice", SqlDialect::ClickHouse)?;
    /// assert_eq!(sql, "'alice'");
    /// ```
    pub fn to_sql_literal(&self, value: &str, dialect: SqlDialect) -> Result<String, String> {
        match self {
            SchemaType::Integer => {
                value.parse::<i64>()
                    .map(|i| i.to_string())
                    .map_err(|_| format!("Invalid integer: '{}'", value))
            }
            
            SchemaType::Float => {
                value.parse::<f64>()
                    .map(|f| f.to_string())
                    .map_err(|_| format!("Invalid float: '{}'", value))
            }
            
            SchemaType::String | SchemaType::Uuid => {
                // Escape single quotes by doubling them (SQL standard)
                let escaped = value.replace('\'', "''");
                Ok(format!("'{}'", escaped))
            }
            
            SchemaType::Boolean => {
                match value.to_lowercase().trim() {
                    "true" | "1" => Ok(match dialect {
                        SqlDialect::ClickHouse => "1".to_string(),
                        SqlDialect::PostgreSQL => "TRUE".to_string(),
                        _ => "1".to_string(),
                    }),
                    "false" | "0" => Ok(match dialect {
                        SqlDialect::ClickHouse => "0".to_string(),
                        SqlDialect::PostgreSQL => "FALSE".to_string(),
                        _ => "0".to_string(),
                    }),
                    _ => Err(format!("Invalid boolean: '{}' (expected: true, false, 1, or 0)", value)),
                }
            }
            
            SchemaType::DateTime | SchemaType::Date => {
                // Dates and timestamps are quoted strings in SQL
                let escaped = value.replace('\'', "''");
                Ok(format!("'{}'", escaped))
            }
        }
    }

    /// Get the type name as a lowercase string
    pub fn as_str(&self) -> &'static str {
        match self {
            SchemaType::Integer => "integer",
            SchemaType::Float => "float",
            SchemaType::String => "string",
            SchemaType::Boolean => "boolean",
            SchemaType::DateTime => "datetime",
            SchemaType::Date => "date",
            SchemaType::Uuid => "uuid",
        }
    }
}

impl fmt::Display for SchemaType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Map a ClickHouse type to a SchemaType
///
/// This function is used during automatic type detection when querying
/// ClickHouse's system.columns table. It strips wrapper types like
/// Nullable() and LowCardinality() and maps ClickHouse-specific types
/// to our simple type system.
///
/// # Example
///
/// ```ignore
/// assert_eq!(map_clickhouse_type("UInt64"), SchemaType::Integer);
/// assert_eq!(map_clickhouse_type("Nullable(String)"), SchemaType::String);
/// assert_eq!(map_clickhouse_type("DateTime64"), SchemaType::DateTime);
/// ```
pub fn map_clickhouse_type(ch_type: &str) -> SchemaType {
    // Normalize: lowercase, strip wrapper types
    let normalized = ch_type.to_lowercase()
        .replace("nullable(", "")
        .replace("lowcardinality(", "")
        .replace(')', "")
        .trim()
        .to_string();

    if normalized.starts_with("int") || normalized.starts_with("uint") {
        SchemaType::Integer
    } else if normalized.starts_with("float") || normalized.starts_with("decimal") {
        SchemaType::Float
    } else if normalized == "string" || normalized.starts_with("fixedstring") {
        SchemaType::String
    } else if normalized == "uuid" {
        SchemaType::Uuid
    } else if normalized.starts_with("datetime") {
        SchemaType::DateTime
    } else if normalized.starts_with("date") && !normalized.starts_with("datetime") {
        SchemaType::Date
    } else if normalized == "bool" {
        SchemaType::Boolean
    } else {
        // Default to string for unknown types (with warning in caller)
        SchemaType::String
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_str_basic() {
        assert_eq!(SchemaType::from_str("integer").unwrap(), SchemaType::Integer);
        assert_eq!(SchemaType::from_str("float").unwrap(), SchemaType::Float);
        assert_eq!(SchemaType::from_str("string").unwrap(), SchemaType::String);
        assert_eq!(SchemaType::from_str("boolean").unwrap(), SchemaType::Boolean);
        assert_eq!(SchemaType::from_str("datetime").unwrap(), SchemaType::DateTime);
        assert_eq!(SchemaType::from_str("date").unwrap(), SchemaType::Date);
        assert_eq!(SchemaType::from_str("uuid").unwrap(), SchemaType::Uuid);
    }

    #[test]
    fn test_from_str_aliases() {
        // Integer aliases
        assert_eq!(SchemaType::from_str("int").unwrap(), SchemaType::Integer);
        assert_eq!(SchemaType::from_str("long").unwrap(), SchemaType::Integer);
        
        // Float aliases
        assert_eq!(SchemaType::from_str("double").unwrap(), SchemaType::Float);
        assert_eq!(SchemaType::from_str("decimal").unwrap(), SchemaType::Float);
        
        // String aliases
        assert_eq!(SchemaType::from_str("text").unwrap(), SchemaType::String);
        
        // Boolean aliases
        assert_eq!(SchemaType::from_str("bool").unwrap(), SchemaType::Boolean);
        
        // DateTime aliases
        assert_eq!(SchemaType::from_str("timestamp").unwrap(), SchemaType::DateTime);
    }

    #[test]
    fn test_from_str_case_insensitive() {
        assert_eq!(SchemaType::from_str("INTEGER").unwrap(), SchemaType::Integer);
        assert_eq!(SchemaType::from_str("Integer").unwrap(), SchemaType::Integer);
        assert_eq!(SchemaType::from_str("INT").unwrap(), SchemaType::Integer);
        assert_eq!(SchemaType::from_str("String").unwrap(), SchemaType::String);
        assert_eq!(SchemaType::from_str("FLOAT").unwrap(), SchemaType::Float);
    }

    #[test]
    fn test_from_str_whitespace() {
        assert_eq!(SchemaType::from_str(" integer ").unwrap(), SchemaType::Integer);
        assert_eq!(SchemaType::from_str("  string  ").unwrap(), SchemaType::String);
    }

    #[test]
    fn test_from_str_invalid() {
        assert!(SchemaType::from_str("unknown").is_err());
        assert!(SchemaType::from_str("int64").is_err());
        assert!(SchemaType::from_str("varchar").is_err());
    }

    #[test]
    fn test_to_sql_literal_integer() {
        let t = SchemaType::Integer;
        assert_eq!(t.to_sql_literal("123", SqlDialect::ClickHouse).unwrap(), "123");
        assert_eq!(t.to_sql_literal("0", SqlDialect::ClickHouse).unwrap(), "0");
        assert_eq!(t.to_sql_literal("-456", SqlDialect::ClickHouse).unwrap(), "-456");
        
        // Invalid integers
        assert!(t.to_sql_literal("abc", SqlDialect::ClickHouse).is_err());
        assert!(t.to_sql_literal("12.34", SqlDialect::ClickHouse).is_err());
    }

    #[test]
    fn test_to_sql_literal_float() {
        let t = SchemaType::Float;
        assert_eq!(t.to_sql_literal("123.45", SqlDialect::ClickHouse).unwrap(), "123.45");
        assert_eq!(t.to_sql_literal("0.0", SqlDialect::ClickHouse).unwrap(), "0");
        assert_eq!(t.to_sql_literal("-3.14", SqlDialect::ClickHouse).unwrap(), "-3.14");
        
        // Invalid floats
        assert!(t.to_sql_literal("abc", SqlDialect::ClickHouse).is_err());
    }

    #[test]
    fn test_to_sql_literal_string() {
        let t = SchemaType::String;
        assert_eq!(t.to_sql_literal("hello", SqlDialect::ClickHouse).unwrap(), "'hello'");
        assert_eq!(t.to_sql_literal("alice@example.com", SqlDialect::ClickHouse).unwrap(), "'alice@example.com'");
        
        // SQL injection protection: single quotes are escaped
        assert_eq!(t.to_sql_literal("O'Reilly", SqlDialect::ClickHouse).unwrap(), "'O''Reilly'");
    }

    #[test]
    fn test_to_sql_literal_uuid() {
        let t = SchemaType::Uuid;
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        assert_eq!(
            t.to_sql_literal(uuid, SqlDialect::ClickHouse).unwrap(),
            format!("'{}'", uuid)
        );
    }

    #[test]
    fn test_to_sql_literal_boolean() {
        let t = SchemaType::Boolean;
        
        // ClickHouse uses 0/1
        assert_eq!(t.to_sql_literal("true", SqlDialect::ClickHouse).unwrap(), "1");
        assert_eq!(t.to_sql_literal("false", SqlDialect::ClickHouse).unwrap(), "0");
        assert_eq!(t.to_sql_literal("1", SqlDialect::ClickHouse).unwrap(), "1");
        assert_eq!(t.to_sql_literal("0", SqlDialect::ClickHouse).unwrap(), "0");
        
        // PostgreSQL uses TRUE/FALSE
        assert_eq!(t.to_sql_literal("true", SqlDialect::PostgreSQL).unwrap(), "TRUE");
        assert_eq!(t.to_sql_literal("false", SqlDialect::PostgreSQL).unwrap(), "FALSE");
        
        // Case insensitive
        assert_eq!(t.to_sql_literal("TRUE", SqlDialect::ClickHouse).unwrap(), "1");
        assert_eq!(t.to_sql_literal("False", SqlDialect::ClickHouse).unwrap(), "0");
        
        // Invalid booleans
        assert!(t.to_sql_literal("yes", SqlDialect::ClickHouse).is_err());
        assert!(t.to_sql_literal("2", SqlDialect::ClickHouse).is_err());
    }

    #[test]
    fn test_to_sql_literal_datetime() {
        let t = SchemaType::DateTime;
        assert_eq!(
            t.to_sql_literal("2025-01-30 12:34:56", SqlDialect::ClickHouse).unwrap(),
            "'2025-01-30 12:34:56'"
        );
    }

    #[test]
    fn test_to_sql_literal_date() {
        let t = SchemaType::Date;
        assert_eq!(
            t.to_sql_literal("2025-01-30", SqlDialect::ClickHouse).unwrap(),
            "'2025-01-30'"
        );
    }

    #[test]
    fn test_map_clickhouse_type_integers() {
        assert_eq!(map_clickhouse_type("Int8"), SchemaType::Integer);
        assert_eq!(map_clickhouse_type("Int16"), SchemaType::Integer);
        assert_eq!(map_clickhouse_type("Int32"), SchemaType::Integer);
        assert_eq!(map_clickhouse_type("Int64"), SchemaType::Integer);
        assert_eq!(map_clickhouse_type("UInt8"), SchemaType::Integer);
        assert_eq!(map_clickhouse_type("UInt16"), SchemaType::Integer);
        assert_eq!(map_clickhouse_type("UInt32"), SchemaType::Integer);
        assert_eq!(map_clickhouse_type("UInt64"), SchemaType::Integer);
    }

    #[test]
    fn test_map_clickhouse_type_floats() {
        assert_eq!(map_clickhouse_type("Float32"), SchemaType::Float);
        assert_eq!(map_clickhouse_type("Float64"), SchemaType::Float);
        assert_eq!(map_clickhouse_type("Decimal(18,2)"), SchemaType::Float);
        assert_eq!(map_clickhouse_type("Decimal128(10)"), SchemaType::Float);
    }

    #[test]
    fn test_map_clickhouse_type_strings() {
        assert_eq!(map_clickhouse_type("String"), SchemaType::String);
        assert_eq!(map_clickhouse_type("FixedString(16)"), SchemaType::String);
    }

    #[test]
    fn test_map_clickhouse_type_special() {
        assert_eq!(map_clickhouse_type("UUID"), SchemaType::Uuid);
        assert_eq!(map_clickhouse_type("Bool"), SchemaType::Boolean);
        assert_eq!(map_clickhouse_type("Date"), SchemaType::Date);
        assert_eq!(map_clickhouse_type("Date32"), SchemaType::Date);
        assert_eq!(map_clickhouse_type("DateTime"), SchemaType::DateTime);
        assert_eq!(map_clickhouse_type("DateTime64"), SchemaType::DateTime);
    }

    #[test]
    fn test_map_clickhouse_type_nullable() {
        assert_eq!(map_clickhouse_type("Nullable(UInt64)"), SchemaType::Integer);
        assert_eq!(map_clickhouse_type("Nullable(String)"), SchemaType::String);
        assert_eq!(map_clickhouse_type("Nullable(DateTime)"), SchemaType::DateTime);
    }

    #[test]
    fn test_map_clickhouse_type_lowcardinality() {
        assert_eq!(map_clickhouse_type("LowCardinality(String)"), SchemaType::String);
        assert_eq!(map_clickhouse_type("LowCardinality(FixedString(10))"), SchemaType::String);
    }

    #[test]
    fn test_map_clickhouse_type_nested_wrappers() {
        assert_eq!(map_clickhouse_type("Nullable(LowCardinality(String))"), SchemaType::String);
    }

    #[test]
    fn test_map_clickhouse_type_unknown() {
        // Unknown types default to String
        assert_eq!(map_clickhouse_type("IPv4"), SchemaType::String);
        assert_eq!(map_clickhouse_type("Array(String)"), SchemaType::String);
    }

    #[test]
    fn test_as_str() {
        assert_eq!(SchemaType::Integer.as_str(), "integer");
        assert_eq!(SchemaType::Float.as_str(), "float");
        assert_eq!(SchemaType::String.as_str(), "string");
        assert_eq!(SchemaType::Boolean.as_str(), "boolean");
        assert_eq!(SchemaType::DateTime.as_str(), "datetime");
        assert_eq!(SchemaType::Date.as_str(), "date");
        assert_eq!(SchemaType::Uuid.as_str(), "uuid");
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", SchemaType::Integer), "integer");
        assert_eq!(format!("{}", SchemaType::String), "string");
    }
}
