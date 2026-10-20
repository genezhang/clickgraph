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
use std::str::FromStr;

use crate::server::models::SqlDialect;

/// Simple, database-agnostic type for schema configuration
///
/// These types are intentionally minimal and map cleanly to equivalent types
/// across different database systems. This supports ClickGraph's multi-database
/// vision (ClickHouse, PostgreSQL, DuckDB, MySQL, SQLite).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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

/// Parse a type string from YAML configuration.
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
/// use std::str::FromStr;
/// let t = SchemaType::from_str("int")?;
/// assert_eq!(t, SchemaType::Integer);
/// ```
impl FromStr for SchemaType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
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
}

impl SchemaType {
    /// Return the Nullable ClickHouse type string (e.g., "Nullable(Int64)").
    /// Used for TCK schemas where missing properties must return NULL.
    pub fn to_nullable_clickhouse_type(&self) -> String {
        format!("Nullable({})", self.to_clickhouse_type())
    }

    /// Spark / Databricks SQL type name. Spark has no unsigned or UUID types and
    /// all columns are nullable, so there is no `Nullable(...)` wrapper.
    pub fn to_spark_type(&self) -> &'static str {
        match self {
            SchemaType::Integer => "BIGINT",
            SchemaType::Float => "DOUBLE",
            SchemaType::String => "STRING",
            SchemaType::Boolean => "BOOLEAN",
            SchemaType::DateTime => "TIMESTAMP",
            SchemaType::Date => "DATE",
            SchemaType::Uuid => "STRING", // no native UUID type
        }
    }

    /// Dialect-aware SQL type name for use inside a CAST. ClickHouse wraps in
    /// `Nullable(T)` when `nullable`; Spark types are implicitly nullable, so the
    /// flag is ignored there.
    pub fn sql_type_name(&self, dialect: SqlDialect, nullable: bool) -> String {
        match dialect {
            SqlDialect::Databricks => self.to_spark_type().to_string(),
            _ if nullable => self.to_nullable_clickhouse_type(),
            _ => self.to_clickhouse_type().to_string(),
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
            SchemaType::Integer => value
                .parse::<i64>()
                .map(|i| i.to_string())
                .map_err(|_| format!("Invalid integer: '{}'", value)),

            SchemaType::Float => value
                .parse::<f64>()
                .map(|f| f.to_string())
                .map_err(|_| format!("Invalid float: '{}'", value)),

            SchemaType::String | SchemaType::Uuid => {
                // Escape single quotes by doubling them (SQL standard)
                let escaped = value.replace('\'', "''");
                Ok(format!("'{}'", escaped))
            }

            SchemaType::Boolean => match value.to_lowercase().trim() {
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
                _ => Err(format!(
                    "Invalid boolean: '{}' (expected: true, false, 1, or 0)",
                    value
                )),
            },

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

    /// Get the ClickHouse type name for this type
    ///
    /// Returns the appropriate ClickHouse type for use in CAST expressions.
    /// For ID columns, this ensures proper type matching for UNION compatibility.
    ///
    /// # Example
    ///
    /// ```ignore
    /// assert_eq!(SchemaType::Integer.to_clickhouse_type(), "Int64");
    /// assert_eq!(SchemaType::String.to_clickhouse_type(), "String");
    /// assert_eq!(SchemaType::Uuid.to_clickhouse_type(), "UUID");
    /// ```
    pub fn to_clickhouse_type(&self) -> &'static str {
        match self {
            SchemaType::Integer => "Int64",          // Generic 64-bit integer
            SchemaType::Float => "Float64",          // Generic 64-bit float
            SchemaType::String => "String",          // Variable-length string
            SchemaType::Boolean => "UInt8",          // ClickHouse uses UInt8 for boolean
            SchemaType::DateTime => "DateTime64(3)", // DateTime with millisecond precision
            SchemaType::Date => "Date32",            // Extended date range
            SchemaType::Uuid => "UUID",              // UUID type
        }
    }

    /// Get a default/placeholder value for this type
    ///
    /// Used for generating empty CTE placeholders that maintain type compatibility.
    /// Returns NULL for all types since property columns are Nullable(T) in writable
    /// schemas, matching Cypher semantics where missing properties return null.
    pub fn default_value(&self) -> &'static str {
        match self {
            SchemaType::Integer => "NULL",
            SchemaType::Float => "NULL",
            SchemaType::String => "NULL",
            SchemaType::Boolean => "NULL",
            SchemaType::DateTime => "NULL",
            SchemaType::Date => "NULL",
            SchemaType::Uuid => "NULL",
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
/// A key column's type in the detail a walk-key comparison needs.
///
/// [`SchemaType`] is not enough on its own: it collapses `DateTime` and
/// `DateTime64(3)` to the same variant, and it has no notion of nullability.
/// Both matter, because a walk compares its keys as *text* and casts the two
/// sides to a common type first:
/// - `toString(Bool)` is `'true'` but `toString(UInt8)` is `'1'`, so those two
///   can never match without a cast;
/// - `toString(DateTime64(3))` always carries `.000`, so a `DateTime` column
///   never matches a `DateTime64(3)` one without a cast;
/// - casting a `Nullable` column to a non-nullable type raises
///   `Code: 349 Cannot convert NULL value to non-Nullable type`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyPartType {
    /// The generic type, from [`map_clickhouse_type`].
    pub schema: SchemaType,
    /// Whether the column is `Nullable(..)`.
    pub nullable: bool,
    /// The ClickHouse spelling as discovered, carrying what `SchemaType`
    /// drops: `DateTime64`'s scale, `FixedString`'s length, timezones. The
    /// `Nullable(..)` wrapper is NOT part of it — nullability is the flag
    /// above, so a target built from this never comes out doubly wrapped.
    pub raw: String,
}

impl Default for KeyPartType {
    /// An undiscovered column, matching `map_clickhouse_type`'s own fallback.
    fn default() -> Self {
        Self {
            schema: SchemaType::Integer,
            nullable: false,
            raw: String::new(),
        }
    }
}

impl KeyPartType {
    /// Parse a discovered ClickHouse column type. An empty or unrecognised
    /// spelling maps to [`SchemaType::Integer`], which is the no-cast path.
    pub fn parse(ch_type: &str) -> Self {
        // `Nullable (String)` is ClickHouse's `Nullable(String)` (a declared
        // `column_types` may spell it so; review finding: read as not
        // nullable, and `DateTime64 (3)` without its scale).
        let spelled = no_space_before_parens(ch_type);
        let trimmed = spelled.trim();
        // Detect the wrappers case-insensitively but keep the spelling's own
        // casing: `DateTime64(3)` has to survive as itself. A `Nullable` may
        // sit inside a `LowCardinality` — `LowCardinality(Nullable(String))`
        // IS a nullable part, and reading it non-nullable folded its key to a
        // plain `String`, so a NULL row's `CAST(<NULL> AS String)` failed
        // (ClickHouse Code 349) instead of comparing — and a `Nullable` may
        // itself wrap another wrapper, or a `LowCardinality` inside it.
        let mut base = trimmed;
        let mut nullable = false;
        loop {
            let lowered = base.to_ascii_lowercase();
            if lowered.starts_with("nullable(") && base.ends_with(')') {
                nullable = true;
                base = base["nullable(".len()..base.len() - 1].trim();
                continue;
            }
            if lowered.starts_with("lowcardinality(") && base.ends_with(')') {
                base = base["lowcardinality(".len()..base.len() - 1].trim();
                continue;
            }
            break;
        }
        Self {
            schema: map_clickhouse_type(trimmed),
            nullable,
            raw: base.to_string(),
        }
    }

    /// The type `self` and `other` must both be compared in, or `None` when
    /// they already agree or the mix is outside the table (in which case the
    /// SQL is kept exactly as it was).
    ///
    /// Each arm is a pair whose *text* forms disagree today — which is the
    /// whole problem, since a walk key is compared as text — and each was
    /// checked against what ClickHouse's `=` does for the same values. The
    /// target is always the **wider** type: casting a `DateTime` end down to a
    /// `Date` node widens the Date to midnight and manufactures matches that
    /// `=` correctly refuses.
    pub fn widen_to(&self, other: &Self) -> Option<Self> {
        use SchemaType::*;
        if self.raw.eq_ignore_ascii_case(&other.raw) {
            return None;
        }
        // A zone-less target keeps each column's own zone, so the texts are
        // local times: two sides in different zones (or one in the server's,
        // unknown here) would spell one local text for two instants and
        // match what `=` refuses. Leave such a pair uncast, as before.
        if let (Some(a), Some(b)) = (self.time_zone(), other.time_zone()) {
            if a != b {
                return None;
            }
        }
        let nullable = self.nullable || other.nullable;
        let raw = match (self.schema, other.schema) {
            // Date and DateTime read as different text ('2020-01-01' vs
            // '2020-01-01 00:00:00'), so the wider type decides. Widen to a
            // DateTime64 only when one side already is one, so its scale
            // survives; otherwise spell through the enum, which owns that.
            (Date, DateTime) | (DateTime, Date) => match finer_scale(self, other) {
                Some(s) => format!("DateTime64({s})"),
                None => DateTime.to_clickhouse_type().to_string(),
            },
            // DateTime vs DateTime64(scale): `=` compares in the finer type,
            // so the fold target must be the finer side's scale — folding to
            // either side's own scale would truncate the other's digits
            // (a DateTime64(6) `.123456` becomes `.123`) and match text `=`
            // does not (a DateTime64(3) node against a DateTime64(6) edge).
            (DateTime, DateTime) if self.is_datetime64() || other.is_datetime64() => {
                format!("DateTime64({})", finer_scale(self, other).unwrap_or(3))
            }
            // `toString(FixedString)` already trims its padding, but cast
            // anyway so the spelling is explicit rather than incidental.
            (String, String) if self.is_fixed_string() || other.is_fixed_string() => {
                String.to_clickhouse_type().to_string()
            }
            // This is the review's "Bool against UInt8" row, folded to `Int64`
            // rather than the narrower direction its review sketched
            // (casting the node down). The texts come out the same — `true`
            // spells as Int64's `1`, which is what a UInt8 1 spells as — so
            // both readings key alike; widening is the lattice's one rule for
            // every pair, and narrowing's hazard (widening a value to
            // something the narrow type cannot spell) has no Bool/UInt8 case.
            (Boolean, Integer) | (Integer, Boolean) => Integer.to_clickhouse_type().to_string(),
            // `=` compares a Float32 with a Float64 in Float64, and that
            // widening changes the text ("0.3" becomes
            // '0.30000001192092896'): unequal values whose texts are equal.
            // Keying both sides in the compared-in type makes the texts tell
            // the same story the values do (the issue's Float32/Float64
            // row). A `Decimal` spelling is exact and outside this table.
            (Float, Float) if float_raw(&self.raw) && float_raw(&other.raw) => {
                Float.to_clickhouse_type().to_string()
            }
            _ => return None,
        };
        Some(Self {
            schema: map_clickhouse_type(&raw),
            nullable,
            raw,
        })
    }

    /// The element type of an `Array(T)` column (VALUE_TYPES.md T2), or
    /// `None` for any other.
    pub fn array_element(&self) -> Option<KeyPartType> {
        let inner = self
            .raw
            .strip_prefix("Array(")
            .or_else(|| self.raw.strip_prefix("array("))?
            .strip_suffix(')')?;
        Some(KeyPartType::parse(inner))
    }

    /// Whether this is an integer of more than 64 bits (`Int128`, `UInt256`).
    pub fn is_wider_than_64_bits(&self) -> bool {
        let raw = self.raw.to_ascii_lowercase();
        ["int128", "uint128", "int256", "uint256"].contains(&raw.as_str())
    }

    /// The scale of a `DateTime64` column, if that is what this is.
    fn datetime64_scale(&self) -> Option<u32> {
        let digits: String = self
            .raw
            .to_ascii_lowercase()
            .strip_prefix("datetime64(")?
            .chars()
            .take_while(char::is_ascii_digit)
            .collect();
        digits.parse().ok()
    }

    fn is_datetime64(&self) -> bool {
        self.datetime64_scale().is_some()
    }

    /// The zone a `DateTime` / `DateTime64` column spells its text in:
    /// `Some(Some(zone))` when the type names one, `Some(None)` for the
    /// server's (unknown at translation), `None` for any other type (a
    /// `Date` has no time of day, so no zone).
    fn time_zone(&self) -> Option<Option<&str>> {
        if !self.raw.to_ascii_lowercase().starts_with("datetime") {
            return None;
        }
        Some(self.raw.split('\'').nth(1))
    }

    fn is_fixed_string(&self) -> bool {
        self.raw.to_ascii_lowercase().contains("fixedstring(")
    }
}

/// Whether `parts` (of known types) are of more than one kind of value: an
/// integer and a string, a date and a number. One column of them (a union's)
/// has no common type in ClickHouse, which makes it a `Variant`, and casts a
/// string `'01'` into it as the integer `1` (#1393); so such ids are held as
/// texts (docs/design/ELEMENT_IDENTITY.md I2).
pub fn of_several_kinds(parts: &[KeyPartType]) -> bool {
    let mut kinds = parts.iter().filter(|p| !p.raw.is_empty()).map(|p| p.schema);
    match kinds.next() {
        Some(first) => kinds.any(|k| k != first),
        None => false,
    }
}

/// The type values of `parts` meet in where ClickHouse has no common type
/// for them, or `None` where it has one (docs/design/VALUE_TYPES.md T4a,
/// #1396). Its own common type compares them as Cypher does; the gap is a
/// `UInt64` with a signed integer of at most 64 bits (an array of them is a
/// `Variant`, in which `1` and `1` are two values; joins and `UNION ALL` of
/// them fail, Code 386): `Int128` holds every value of each, and spells each
/// as the same digits. Only a set of integers is cast (a boolean is its own
/// value, not `1`: review finding); a `UInt256` with a signed integer has no
/// type to meet in, and is left.
pub fn common_type_where_none(parts: &[KeyPartType]) -> Option<KeyPartType> {
    // Integers of at most 64 bits: `Int128` would wrap a wider one (a
    // `UInt128` of 2^127 or more), which meets the others in itself or
    // `Int256` (review finding).
    let integers = parts.iter().all(|p| {
        p.schema == SchemaType::Integer
            && !p.raw.eq_ignore_ascii_case("Bool")
            && !p.is_wider_than_64_bits()
    });
    let gap = parts
        .iter()
        .enumerate()
        .any(|(i, a)| parts[i + 1..].iter().any(|b| unsigned_signed_64(a, b)));
    (integers && gap).then(|| KeyPartType {
        schema: SchemaType::Integer,
        nullable: parts.iter().any(|p| p.nullable),
        raw: "Int128".to_string(),
    })
}

/// One side a `UInt64` and the other a signed integer of at most 64 bits:
/// the pair ClickHouse has no supertype for (`Int128` / `Int256` with a
/// `UInt64` meet in themselves).
fn unsigned_signed_64(a: &KeyPartType, b: &KeyPartType) -> bool {
    let signed = |t: &KeyPartType| {
        ["Int8", "Int16", "Int32", "Int64"]
            .iter()
            .any(|s| t.raw.eq_ignore_ascii_case(s))
    };
    let uint64 = |t: &KeyPartType| t.raw.eq_ignore_ascii_case("UInt64");
    (uint64(a) && signed(b)) || (uint64(b) && signed(a))
}

/// `ty` with the spaces before each `(` removed (outside quotes).
fn no_space_before_parens(ty: &str) -> String {
    let mut out = String::with_capacity(ty.len());
    let mut quoted = false;
    for c in ty.chars() {
        match c {
            '\'' => quoted = !quoted,
            '(' if !quoted => {
                let kept = out.trim_end().len();
                out.truncate(kept);
            }
            _ => {}
        }
        out.push(c);
    }
    out
}

/// A raw `Float32` / `Float64` spelling (`Decimal` is not: exact, and no
/// table entry says how `=` sells it against a binary float).
fn float_raw(raw: &str) -> bool {
    raw.to_ascii_lowercase().starts_with("float")
}

/// The finer of the two sides' `DateTime64` scales — `=` compares in the
/// finer type, so the fold must cast to it; taking one side's own scale
/// would truncate the other's digits and match text `=` refuses.
fn finer_scale(a: &KeyPartType, b: &KeyPartType) -> Option<u32> {
    match (a.datetime64_scale(), b.datetime64_scale()) {
        (Some(x), Some(y)) => Some(x.max(y)),
        (x, y) => x.or(y),
    }
}

/// One type every side of a walk key part is compared in, or `None` when they
/// already agree (or the mix is outside the table, so no cast is emitted).
///
/// A walk's node key is one CTE shared by every relationship type in the walk,
/// so the sides cannot each pick their own target: if one arm widened a `Date`
/// to `DateTime64(3)`, every other arm's ends have to widen too or that arm
/// silently stops joining. Folding the whole set makes that agreement
/// structural instead of something each arm re-derives.
pub fn fold_common(parts: &[KeyPartType]) -> Option<KeyPartType> {
    // Order-independent. The lattice's gaps make the fold's result depend
    // on the input's order when a pair sits outside it ([Date, String,
    // DateTime] folds the Date/DateTime pair while [String, Date, DateTime]
    // folds nothing — the reach differs only by which arms discovered the
    // sides in which order). Folding in a canonical order makes the fold a
    // function of the SET, and its target one answer.
    // Zones (`KeyPartType::widen_to`): the fold's target is zone-less, so a
    // set whose time columns spell their texts in more than one zone stays
    // uncast — checked over the whole set, since a widened accumulator no
    // longer carries the zone of the side it came from.
    let mut zones = parts.iter().filter_map(KeyPartType::time_zone);
    if let Some(first) = zones.next() {
        if zones.any(|z| z != first) {
            return None;
        }
    }
    let mut sorted: Vec<&KeyPartType> = parts.iter().collect();
    sorted.sort_by(|a, b| a.raw.to_ascii_lowercase().cmp(&b.raw.to_ascii_lowercase()));
    let mut iter = sorted.into_iter();
    let mut acc = iter.next()?.clone();
    let mut widened = false;
    for part in iter {
        if let Some(next) = acc.widen_to(part) {
            acc = next;
            widened = true;
        }
    }
    // Every side is cast to the target, so it is nullable when ANY side is:
    // `widen_to` skips a side whose spelling the target already has, and
    // with it that side's `Nullable` (String + FixedString + Nullable(String)
    // folded to a plain `String`, and the NULL end's CAST raised Code 349).
    acc.nullable = parts.iter().any(|p| p.nullable);
    widened.then_some(acc)
}

/// Whether `ty` is spelled as a column type (`column_types:` in the YAML,
/// VALUE_TYPES.md T1): a type name, with arguments in balanced parentheses
/// (`DateTime64(3, 'UTC')`, `Map(String, Array(UInt8))`). The family is not
/// checked: ClickGraph reads what it knows of it (`KeyPartType::parse`); the
/// casts it spells are its own types (only a `DateTime64`'s scale comes from
/// the column's, which ClickHouse checks).
pub fn is_column_type_spelling(ty: &str) -> bool {
    let ty = ty.trim();
    let name_len = ty
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(ty.len());
    if name_len == 0 || !ty.starts_with(|c: char| c.is_ascii_alphabetic()) {
        return false;
    }
    let args = ty[name_len..].trim();
    if args.is_empty() {
        return true;
    }
    if !(args.starts_with('(') && args.ends_with(')')) {
        return false;
    }
    let mut depth = 0i32;
    let mut quoted = false;
    for (i, c) in args.char_indices() {
        match c {
            '\'' => quoted = !quoted,
            '(' if !quoted => depth += 1,
            ')' if !quoted => {
                depth -= 1;
                // Closed before the end: `Array(String) x`.
                if depth == 0 && i + 1 != args.len() {
                    return false;
                }
            }
            _ => {}
        }
    }
    depth == 0 && !quoted
}

pub fn map_clickhouse_type(ch_type: &str) -> SchemaType {
    // Normalize: lowercase, strip wrapper types
    let normalized = ch_type
        .to_lowercase()
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
    fn an_array_column_has_its_element_type() {
        let e = KeyPartType::parse("Array(Nullable(String))")
            .array_element()
            .unwrap();
        assert_eq!((e.raw.as_str(), e.nullable), ("String", true));
        let nested = KeyPartType::parse("Array(Array(UInt8))")
            .array_element()
            .unwrap();
        assert_eq!(nested.array_element().unwrap().raw, "UInt8");
        assert_eq!(
            KeyPartType::parse("Map(String, UInt8)").array_element(),
            None
        );
        assert_eq!(KeyPartType::parse("String").array_element(), None);
    }

    /// A space before a parenthesis is ClickHouse's same type.
    #[test]
    fn a_spaced_spelling_parses_as_its_type() {
        let t = KeyPartType::parse("Nullable (String)");
        assert_eq!((t.raw.as_str(), t.nullable), ("String", true));
        assert_eq!(
            KeyPartType::parse("DateTime64 (3)").datetime64_scale(),
            Some(3)
        );
        let t = KeyPartType::parse("LowCardinality (Nullable (FixedString(2)))");
        assert_eq!((t.raw.as_str(), t.nullable), ("FixedString(2)", true));
        assert_eq!(
            KeyPartType::parse("Enum8('a (b' = 1)").raw,
            "Enum8('a (b' = 1)"
        );
    }

    #[test]
    fn column_type_spellings_are_recognized() {
        for ok in [
            "UInt64",
            "String",
            "FixedString(3)",
            "DateTime('UTC')",
            "DateTime64(3, 'Asia/Tokyo')",
            "Decimal(10, 2)",
            "LowCardinality(Nullable(String))",
            "Map(String, Array(UInt64))",
            "Enum8('a(' = 1)",
            "JSON",
            "INTEGER",
        ] {
            assert!(is_column_type_spelling(ok), "{ok}");
        }
        for bad in [
            "",
            "Array(",
            "Array(String) x",
            "(String)",
            "1Int",
            "Map(String))",
        ] {
            assert!(!is_column_type_spelling(bad), "{bad}");
        }
    }

    #[test]
    fn test_sql_type_name_dialect_aware() {
        // ClickHouse: existing spellings; Nullable wrapper when requested.
        assert_eq!(
            SchemaType::Integer.sql_type_name(SqlDialect::ClickHouse, false),
            "Int64"
        );
        assert_eq!(
            SchemaType::Integer.sql_type_name(SqlDialect::ClickHouse, true),
            "Nullable(Int64)"
        );
        assert_eq!(
            SchemaType::Boolean.sql_type_name(SqlDialect::ClickHouse, false),
            "UInt8"
        );

        // Databricks: Spark spellings, nullable flag ignored (implicitly nullable).
        assert_eq!(
            SchemaType::Integer.sql_type_name(SqlDialect::Databricks, true),
            "BIGINT"
        );
        assert_eq!(
            SchemaType::String.sql_type_name(SqlDialect::Databricks, false),
            "STRING"
        );
        assert_eq!(
            SchemaType::Boolean.sql_type_name(SqlDialect::Databricks, false),
            "BOOLEAN"
        );
        assert_eq!(
            SchemaType::DateTime.sql_type_name(SqlDialect::Databricks, false),
            "TIMESTAMP"
        );
        assert_eq!(
            SchemaType::Uuid.sql_type_name(SqlDialect::Databricks, false),
            "STRING"
        );
    }

    #[test]
    fn test_from_str_basic() {
        assert_eq!(
            SchemaType::from_str("integer").unwrap(),
            SchemaType::Integer
        );
        assert_eq!(SchemaType::from_str("float").unwrap(), SchemaType::Float);
        assert_eq!(SchemaType::from_str("string").unwrap(), SchemaType::String);
        assert_eq!(
            SchemaType::from_str("boolean").unwrap(),
            SchemaType::Boolean
        );
        assert_eq!(
            SchemaType::from_str("datetime").unwrap(),
            SchemaType::DateTime
        );
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
        assert_eq!(
            SchemaType::from_str("timestamp").unwrap(),
            SchemaType::DateTime
        );
    }

    #[test]
    fn test_from_str_case_insensitive() {
        assert_eq!(
            SchemaType::from_str("INTEGER").unwrap(),
            SchemaType::Integer
        );
        assert_eq!(
            SchemaType::from_str("Integer").unwrap(),
            SchemaType::Integer
        );
        assert_eq!(SchemaType::from_str("INT").unwrap(), SchemaType::Integer);
        assert_eq!(SchemaType::from_str("String").unwrap(), SchemaType::String);
        assert_eq!(SchemaType::from_str("FLOAT").unwrap(), SchemaType::Float);
    }

    #[test]
    fn test_from_str_whitespace() {
        assert_eq!(
            SchemaType::from_str(" integer ").unwrap(),
            SchemaType::Integer
        );
        assert_eq!(
            SchemaType::from_str("  string  ").unwrap(),
            SchemaType::String
        );
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
        assert_eq!(
            t.to_sql_literal("123", SqlDialect::ClickHouse).unwrap(),
            "123"
        );
        assert_eq!(t.to_sql_literal("0", SqlDialect::ClickHouse).unwrap(), "0");
        assert_eq!(
            t.to_sql_literal("-456", SqlDialect::ClickHouse).unwrap(),
            "-456"
        );

        // Invalid integers
        assert!(t.to_sql_literal("abc", SqlDialect::ClickHouse).is_err());
        assert!(t.to_sql_literal("12.34", SqlDialect::ClickHouse).is_err());
    }

    #[test]
    fn test_to_sql_literal_float() {
        let t = SchemaType::Float;
        assert_eq!(
            t.to_sql_literal("123.45", SqlDialect::ClickHouse).unwrap(),
            "123.45"
        );
        assert_eq!(
            t.to_sql_literal("0.0", SqlDialect::ClickHouse).unwrap(),
            "0"
        );
        assert_eq!(
            t.to_sql_literal("-3.14", SqlDialect::ClickHouse).unwrap(),
            "-3.14"
        );

        // Invalid floats
        assert!(t.to_sql_literal("abc", SqlDialect::ClickHouse).is_err());
    }

    #[test]
    fn test_to_sql_literal_string() {
        let t = SchemaType::String;
        assert_eq!(
            t.to_sql_literal("hello", SqlDialect::ClickHouse).unwrap(),
            "'hello'"
        );
        assert_eq!(
            t.to_sql_literal("alice@example.com", SqlDialect::ClickHouse)
                .unwrap(),
            "'alice@example.com'"
        );

        // SQL injection protection: single quotes are escaped
        assert_eq!(
            t.to_sql_literal("O'Reilly", SqlDialect::ClickHouse)
                .unwrap(),
            "'O''Reilly'"
        );
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
        assert_eq!(
            t.to_sql_literal("true", SqlDialect::ClickHouse).unwrap(),
            "1"
        );
        assert_eq!(
            t.to_sql_literal("false", SqlDialect::ClickHouse).unwrap(),
            "0"
        );
        assert_eq!(t.to_sql_literal("1", SqlDialect::ClickHouse).unwrap(), "1");
        assert_eq!(t.to_sql_literal("0", SqlDialect::ClickHouse).unwrap(), "0");

        // PostgreSQL uses TRUE/FALSE
        assert_eq!(
            t.to_sql_literal("true", SqlDialect::PostgreSQL).unwrap(),
            "TRUE"
        );
        assert_eq!(
            t.to_sql_literal("false", SqlDialect::PostgreSQL).unwrap(),
            "FALSE"
        );

        // Case insensitive
        assert_eq!(
            t.to_sql_literal("TRUE", SqlDialect::ClickHouse).unwrap(),
            "1"
        );
        assert_eq!(
            t.to_sql_literal("False", SqlDialect::ClickHouse).unwrap(),
            "0"
        );

        // Invalid booleans
        assert!(t.to_sql_literal("yes", SqlDialect::ClickHouse).is_err());
        assert!(t.to_sql_literal("2", SqlDialect::ClickHouse).is_err());
    }

    #[test]
    fn test_to_sql_literal_datetime() {
        let t = SchemaType::DateTime;
        assert_eq!(
            t.to_sql_literal("2025-01-30 12:34:56", SqlDialect::ClickHouse)
                .unwrap(),
            "'2025-01-30 12:34:56'"
        );
    }

    #[test]
    fn test_to_sql_literal_date() {
        let t = SchemaType::Date;
        assert_eq!(
            t.to_sql_literal("2025-01-30", SqlDialect::ClickHouse)
                .unwrap(),
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
        assert_eq!(
            map_clickhouse_type("Nullable(DateTime)"),
            SchemaType::DateTime
        );
    }

    #[test]
    fn test_map_clickhouse_type_lowcardinality() {
        assert_eq!(
            map_clickhouse_type("LowCardinality(String)"),
            SchemaType::String
        );
        assert_eq!(
            map_clickhouse_type("LowCardinality(FixedString(10))"),
            SchemaType::String
        );
    }

    #[test]
    fn test_map_clickhouse_type_nested_wrappers() {
        assert_eq!(
            map_clickhouse_type("Nullable(LowCardinality(String))"),
            SchemaType::String
        );
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
#[cfg(test)]
mod common_type_where_none_tests {
    use super::*;

    fn kp(raw: &str) -> KeyPartType {
        KeyPartType::parse(raw)
    }

    /// Only a `UInt64` with a signed integer of at most 64 bits has no
    /// ClickHouse common type; every other set keeps its SQL.
    #[test]
    fn only_uint64_and_a_narrow_signed_integer_meet_in_int128() {
        let t = common_type_where_none(&[kp("UInt64"), kp("Int64")]).unwrap();
        assert_eq!((t.raw.as_str(), t.nullable), ("Int128", false));
        let t = common_type_where_none(&[kp("Int8"), kp("Nullable(UInt64)")]).unwrap();
        assert_eq!((t.raw.as_str(), t.nullable), ("Int128", true));
        for set in [
            vec![kp("UInt64"), kp("UInt64")],
            vec![kp("UInt32"), kp("Int64")],
            vec![kp("UInt64"), kp("Int128")],
            vec![kp("UInt64"), kp("Int256")],
            vec![kp("UInt256"), kp("Int8")],
            vec![kp("UInt64"), kp("Bool")],
            vec![kp("UInt64"), kp("Int64"), kp("Bool")],
            vec![kp("UInt64"), kp("Int64"), kp("UInt128")],
            vec![kp("UInt64"), kp("Int64"), kp("String")],
            vec![kp("Date"), kp("DateTime")],
            vec![],
        ] {
            assert_eq!(common_type_where_none(&set), None, "{set:?}");
        }
    }
}

/// #1360: the walk-key common-type table. Every arm below was checked against
/// ClickHouse 25.8: for each pair, casting both sides to the target makes their
/// *text* agree exactly when `=` agrees on the values.
///
/// These assert the type decision itself. Spelling a target for a given
/// dialect is the dialect axis's job, so it is tested where that is
/// dispatched rather than here.
#[cfg(test)]
mod walk_key_lct_tests {
    use super::{fold_common, KeyPartType};

    fn kp(raw: &str) -> KeyPartType {
        KeyPartType::parse(raw)
    }

    /// Nullability and the raw spelling are both load-bearing, and
    /// `SchemaType` carries neither.
    #[test]
    fn parses_nullability_and_keeps_the_raw_spelling() {
        let a = kp("Nullable(DateTime64(3))");
        assert!(a.nullable, "Nullable( is remembered, not stripped away");
        assert_eq!(a.datetime64_scale(), Some(3), "the scale survives");
        // The wrapper is a flag, not part of the spelling, so a target built
        // from it can never come out as Nullable(Nullable(..)).
        assert_eq!(a.raw, "DateTime64(3)");
        assert_eq!(kp("Nullable(String)").raw, "String");
        assert!(kp("FixedString(5)").is_fixed_string());
        assert!(!kp("String").nullable);
        assert_eq!(
            kp("LowCardinality(String)").schema,
            super::SchemaType::String
        );
        // Wrapper order does not hide the nullability: a LowCardinality of a
        // Nullable is nullable (round 3), and so is the other nesting.
        let lc_nullable = kp("LowCardinality(Nullable(String))");
        assert!(lc_nullable.nullable, "LowCardinality(Nullable( is nullable");
        assert_eq!(lc_nullable.raw, "String", "both wrappers are stripped");
        assert!(
            kp("Nullable(LowCardinality(String))").nullable,
            "Nullable(LowCardinality( is nullable too"
        );
        // A LowCardinality alone is not one, and the inner spelling survives.
        assert!(!kp("LowCardinality(String)").nullable);
        let lc_fixed = kp("LowCardinality(FixedString(8))");
        assert!(!lc_fixed.nullable && lc_fixed.is_fixed_string());
        assert_eq!(lc_fixed.raw, "FixedString(8)");

        // The fold keeps it: a nullable String part against a plain one
        // already agrees (no cast whose target could drop the flag), and
        // against a FixedString it folds to Nullable(String) -- never a
        // plain String, which a NULL row's CAST would die on (Code 349).
        let plain = kp("String");
        assert!(
            a_from_widen(lc_nullable.clone(), plain).is_none(),
            "a nullable String against a String needs no cast"
        );
        let folded = a_from_widen(kp("LowCardinality(Nullable(String))"), kp("FixedString(8)"));
        assert_eq!(
            folded.as_ref().map(|t| (t.raw.as_str(), t.nullable)),
            Some(("String", true)),
            "the fold's target keeps Nullable"
        );
    }

    /// `KeyPartType::widen_to`, named so a test reads as its intent.
    fn a_from_widen(a: super::KeyPartType, b: super::KeyPartType) -> Option<super::KeyPartType> {
        a.widen_to(&b)
    }

    /// The target is always the WIDER type. Casting a DateTime end down to a
    /// Date node widens the Date to midnight and manufactures matches that `=`
    /// refuses: `toDate(x) = toDateTime(x 12:00)` is 0, but
    /// `toDate(toDateTime(x 12:00)) = toDate(x)` is 1.
    #[test]
    fn arms_widen_and_never_narrow() {
        for (a, b, want) in [
            ("Date", "DateTime", "DateTime64(3)"),
            ("DateTime", "Date", "DateTime64(3)"),
            ("DateTime", "DateTime64(6)", "DateTime64(6)"),
            ("DateTime64(6)", "DateTime", "DateTime64(6)"),
            // Review round 3: the fold's target is the FINER scale — `=`
            // compares in it, and either side's own scale would truncate the
            // other's digits (a DateTime64(6) `.123456` becomes `.123`).
            ("DateTime64(3)", "DateTime64(6)", "DateTime64(6)"),
            ("DateTime64(6)", "DateTime64(3)", "DateTime64(6)"),
            ("Date", "DateTime64(3)", "DateTime64(3)"),
            ("Date", "DateTime64(6)", "DateTime64(6)"),
            ("FixedString(5)", "String", "String"),
            ("String", "FixedString(5)", "String"),
            ("Bool", "UInt8", "Int64"),
            ("UInt8", "Bool", "Int64"),
            // The issue's Float32/Float64 row: `=` compares them in Float64,
            // and the widening changes the text of a narrow value (0.3
            // becomes '0.30000001192092896'), so the text keys have to
            // compare in that type too.
            ("Float32", "Float64", "Float64"),
        ] {
            assert_eq!(kp(a).widen_to(&kp(b)).unwrap().raw, want, "{a} with {b}");
            assert_eq!(
                kp(b).widen_to(&kp(a)).unwrap().raw,
                want,
                "{b} with {a} (symmetric)"
            );
        }
    }

    /// A `Decimal` is exact: no table entry says how `=` compares it with a
    /// binary float, so the pair keeps today's SQL (no cast either way).
    #[test]
    fn decimal_against_a_float_stays_uncast() {
        assert_eq!(kp("Decimal(10,2)").widen_to(&kp("Float64")), None);
        assert_eq!(kp("Float64").widen_to(&kp("Decimal(10,2)")), None);
    }

    /// No cast when the two already spell the same thing — which also retires
    /// the blanket `CAST(... AS String)` the first version put on every
    /// discovered String key.
    #[test]
    fn matching_spellings_need_no_cast() {
        for raw in [
            "String",
            "Int64",
            "UInt64",
            "Date",
            "DateTime",
            "DateTime64(3)",
        ] {
            assert_eq!(kp(raw).widen_to(&kp(raw)), None, "{raw} against itself");
        }
        // Same generic type, different spellings `=` already agrees on.
        assert_eq!(kp("Int64").widen_to(&kp("UInt64")), None, "same digits");
        assert_eq!(kp("String").widen_to(&kp("LowCardinality(String)")), None);
    }

    /// Every pair outside the table keeps today's SQL.
    #[test]
    fn unlisted_pairs_stay_unchanged() {
        for (a, b) in [
            ("Int64", "Float64"),
            ("Float64", "Int64"),
            ("Decimal(10,2)", "Int64"),
            ("UUID", "String"),
            ("String", "UUID"),
            ("Int64", "Date"),
            ("Bool", "String"),
            ("UUID", "Int64"),
        ] {
            assert_eq!(kp(a).widen_to(&kp(b)), None, "{a}/{b} is not in the table");
        }
    }

    /// Casting a Nullable column to a non-nullable type raises
    /// `Code: 349`, so the widened target keeps the wrapper.
    #[test]
    fn a_nullable_side_keeps_the_wrapper() {
        for (a, b, want_nullable) in [
            ("Nullable(DateTime)", "Date", true),
            ("Date", "Nullable(DateTime)", true),
            ("Nullable(DateTime)", "DateTime64(6)", true),
            ("Nullable(UInt8)", "Bool", true),
            ("Date", "DateTime", false),
        ] {
            let got = kp(a).widen_to(&kp(b)).expect("widened");
            assert_eq!(got.nullable, want_nullable, "{a} with {b}");
        }
    }

    /// A zone-less target keeps each column's zone, so its texts are local
    /// times: a Tokyo midnight and a UTC midnight spell alike though `=`
    /// says they differ. Sides in different zones (the server's counts as
    /// one, unknown here) stay uncast; one zone, or a `Date`, still folds.
    #[test]
    fn sides_in_different_zones_stay_uncast() {
        for (a, b) in [
            ("DateTime('Asia/Tokyo')", "DateTime64(3, 'UTC')"),
            ("DateTime('Asia/Tokyo')", "DateTime64(3)"),
            ("DateTime('UTC')", "DateTime64(3, 'Asia/Tokyo')"),
            ("DateTime64(3, 'Asia/Tokyo')", "DateTime64(6, 'UTC')"),
        ] {
            assert_eq!(kp(a).widen_to(&kp(b)), None, "{a}/{b}");
            assert_eq!(kp(b).widen_to(&kp(a)), None, "{b}/{a}");
            assert_eq!(fold_common(&[kp(a), kp(b)]), None, "{a}/{b}");
        }
        // The accumulator loses the zone it came from: checked over the set.
        assert_eq!(
            fold_common(&[
                kp("Date"),
                kp("DateTime('Asia/Tokyo')"),
                kp("DateTime64(3, 'UTC')")
            ]),
            None
        );
        let f = fold_common(&[
            kp("DateTime('Asia/Tokyo')"),
            kp("DateTime64(3, 'Asia/Tokyo')"),
        ])
        .expect("one zone folds");
        assert_eq!(f.raw, "DateTime64(3)");
        assert!(fold_common(&[kp("Date"), kp("DateTime('Asia/Tokyo')")]).is_some());
    }

    /// The node key is one CTE shared by every arm, so all sides must agree.
    /// Folding is what makes that structural rather than something each arm
    /// re-derives: a Date node, one arm ending DateTime and another ending Date
    /// all settle on one target.
    #[test]
    fn fold_gives_every_side_one_target() {
        for parts in [
            vec![kp("Date"), kp("DateTime"), kp("Date")],
            vec![kp("DateTime"), kp("Date"), kp("Date")],
            vec![kp("Date"), kp("Date"), kp("DateTime")],
        ] {
            assert_eq!(fold_common(&parts).expect("folded").raw, "DateTime64(3)");
        }
        // One type needs no cast; so does an empty or unsupported set.
        assert_eq!(fold_common(&[]), None);
        assert_eq!(fold_common(&[kp("String"), kp("String")]), None);
        assert_eq!(fold_common(&[kp("Date"), kp("Date")]), None);
        assert_eq!(fold_common(&[kp("Int64"), kp("Float64")]), None);
        // Bool/UInt8 across a fold.
        assert_eq!(
            fold_common(&[kp("Bool"), kp("UInt8"), kp("UInt8")])
                .expect("folded")
                .raw,
            "Int64"
        );
        // Nullable anywhere in the fold keeps the wrapper on the target.
        let f = fold_common(&[kp("Date"), kp("Nullable(DateTime)")]).expect("folded");
        assert_eq!(f.raw, "DateTime64(3)");
        assert!(f.nullable, "a dangling end must not raise Code 349");
        // ...also when the nullable side already has the target's spelling:
        // `widen_to` skips it, and must not drop its Nullable with it.
        for set in [
            vec![kp("String"), kp("FixedString(3)"), kp("Nullable(String)")],
            vec![
                kp("DateTime64(3)"),
                kp("Date"),
                kp("Nullable(DateTime64(3))"),
            ],
        ] {
            let f = fold_common(&set).expect("folded");
            assert!(f.nullable, "{set:?}: a NULL end's CAST raises Code 349");
        }
        // The fold is order-independent (round 3, "fold order"): with a pair
        // outside the lattice in the middle, one input order folded the
        // Date/DateTime pair and another folded nothing — the reach must be
        // one function of the set, not of the sides' discovery order. (The
        // target stays masked today: CAST('x' AS DateTime64(3)) is Code 386.)
        let folded = fold_common(&[kp("Date"), kp("String"), kp("DateTime")]);
        for order in [
            vec![kp("String"), kp("Date"), kp("DateTime")],
            vec![kp("Date"), kp("DateTime"), kp("String")],
            vec![kp("DateTime"), kp("String"), kp("Date")],
        ] {
            assert_eq!(
                fold_common(&order).map(|t| t.raw),
                folded.clone().map(|t| t.raw),
                "every input order folds to one target"
            );
        }
        assert_eq!(folded.expect("folded").raw, "DateTime64(3)");
    }
}
