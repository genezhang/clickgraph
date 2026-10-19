//! SQL generation: the dialect ([`SqlDialect`]), the per-dialect function
//! spellings ([`function_mapper`]) and the printer (`emitters`), which the
//! bound plan's lowering reads through the task-local `QueryContext`'s
//! dialect.

use serde::{Deserialize, Serialize};

pub(crate) mod emitters;
pub(crate) mod function_mapper;
pub(crate) mod passthrough;

/// SQL dialect for query generation.
///
/// Currently only `ClickHouse` is implemented; the other variants exist for
/// API forward-compatibility (they will return `UnsupportedDialectError` at
/// the executor / emitter boundary).
///
/// This type lives here — not in `server::models` — because the SQL layer and
/// the schema layer (`graph_catalog::schema_types`) both need it independently
/// of the HTTP API. `server::models` re-exports it for backward compatibility.
#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Eq, Hash, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum SqlDialect {
    #[serde(rename = "clickhouse")]
    #[default]
    ClickHouse,

    #[serde(rename = "postgresql")]
    PostgreSQL,

    #[serde(rename = "duckdb")]
    DuckDB,

    #[serde(rename = "mysql")]
    MySQL,

    #[serde(rename = "sqlite")]
    SQLite,

    /// Databricks SQL Warehouse / Spark SQL, through its
    /// [`FunctionMapper`] (see [`function_mapper::for_dialect`]).
    ///
    /// [`FunctionMapper`]: function_mapper::FunctionMapper
    #[serde(rename = "databricks")]
    Databricks,
}

impl SqlDialect {
    /// String identifier for the dialect (lowercase).
    pub fn as_str(&self) -> &'static str {
        match self {
            SqlDialect::ClickHouse => "clickhouse",
            SqlDialect::PostgreSQL => "postgresql",
            SqlDialect::DuckDB => "duckdb",
            SqlDialect::MySQL => "mysql",
            SqlDialect::SQLite => "sqlite",
            SqlDialect::Databricks => "databricks",
        }
    }

    /// Whether an emitter is implemented for this dialect today.
    pub fn is_supported(&self) -> bool {
        matches!(self, SqlDialect::ClickHouse)
    }

    /// #1360: the type name a walk-key part is cast to, spelled for this
    /// dialect.
    ///
    /// Lives here rather than in `graph_catalog::schema_types` because how a
    /// type is spelled is the dialect axis, and this module is its canonical
    /// dispatch point (`SchemaType::sql_type_name` answers the same question
    /// for a type with no arguments; a column whose discovered spelling carries
    /// them — `DateTime64(3)`, whose scale `SchemaType` drops — needs the raw
    /// form kept).
    ///
    /// `Nullable(..)` is ClickHouse-only: Spark types are implicitly nullable.
    /// A ClickHouse target is its spelling (`Int128` is no `Int64`, #1358);
    /// a target with none is the `SchemaType`'s.
    pub fn cast_target(&self, kp: &crate::graph_catalog::schema_types::KeyPartType) -> String {
        match self {
            SqlDialect::Databricks if kp.raw.contains('(') => kp.schema.to_spark_type().to_string(),
            SqlDialect::ClickHouse if !kp.raw.is_empty() => match kp.nullable {
                true => format!("Nullable({})", kp.raw),
                false => kp.raw.clone(),
            },
            _ => kp.schema.sql_type_name(*self, kp.nullable),
        }
    }

    /// The type values of `parts` meet in where this dialect has no common
    /// type for them (`schema_types::common_type_where_none`, VALUE_TYPES.md
    /// T4a). Spark has no unsigned integers: none.
    pub fn common_type_where_none(
        &self,
        parts: &[crate::graph_catalog::schema_types::KeyPartType],
    ) -> Option<crate::graph_catalog::schema_types::KeyPartType> {
        match self {
            SqlDialect::ClickHouse => {
                crate::graph_catalog::schema_types::common_type_where_none(parts)
            }
            _ => None,
        }
    }

    /// Whether this dialect supports the `FINAL` table modifier. `FINAL` is a
    /// ClickHouse MergeTree-family read-time dedup hint; it is invalid SQL on
    /// other backends (notably Databricks/Spark, which has no such storage
    /// engine), so the renderer must not emit it there regardless of a schema's
    /// `use_final` setting.
    pub fn supports_final_keyword(&self) -> bool {
        matches!(self, SqlDialect::ClickHouse)
    }
}

/// ClickHouse session settings that generated SQL depends on for **result
/// correctness** (not limits or formats). `join_use_nulls = 1` makes the
/// unmatched side of a LEFT JOIN NULL instead of the type default (`''`, `0`),
/// which OPTIONAL MATCH, `IS NULL`, `count(x)` and `coalesce` rely on.
///
/// Single source of truth: the server connection pool, the chdb executor and
/// [`portable_sql`] all apply this list.
pub const SEMANTIC_SESSION_SETTINGS: &[(&str, &str)] = &[("join_use_nulls", "1")];

/// SQL that is correct on any session of `dialect`: for ClickHouse, appends a
/// `SETTINGS` clause carrying [`SEMANTIC_SESSION_SETTINGS`], so SQL-only output
/// that a user runs outside ClickGraph gives the same rows (#1314). Other
/// dialects already have standard NULL semantics and are returned unchanged.
///
/// Executors must NOT use this: they apply the settings at session level and
/// the executed SQL stays exactly what the emitter produced.
pub fn portable_sql(sql: &str, dialect: SqlDialect) -> String {
    match dialect {
        SqlDialect::ClickHouse => {
            let settings = SEMANTIC_SESSION_SETTINGS
                .iter()
                .map(|(k, v)| format!("{} = {}", k, v))
                .collect::<Vec<_>>()
                .join(", ");
            format!("{}\nSETTINGS {}", sql.trim_end(), settings)
        }
        _ => sql.to_string(),
    }
}

#[cfg(test)]
mod portable_sql_tests {
    use super::*;

    #[test]
    fn clickhouse_sql_carries_join_use_nulls() {
        let sql = "SELECT a.x\nFROM t AS a\nLEFT JOIN u AS b ON b.k = a.k\n";
        assert_eq!(
            portable_sql(sql, SqlDialect::ClickHouse),
            "SELECT a.x\nFROM t AS a\nLEFT JOIN u AS b ON b.k = a.k\nSETTINGS join_use_nulls = 1"
        );
    }

    #[test]
    fn databricks_sql_is_unchanged() {
        let sql = "SELECT a.x FROM t AS a";
        assert_eq!(portable_sql(sql, SqlDialect::Databricks), sql);
    }
}

/// #1360: a walk-key CAST target keeps the discovered spelling's arguments
/// (a `DateTime64`'s scale, which `SchemaType` drops), and `Nullable(..)` is
/// ClickHouse-only because Spark types are implicitly nullable.
#[cfg(test)]
mod walk_key_cast_target_tests {
    use super::SqlDialect;
    use crate::graph_catalog::schema_types::KeyPartType;

    fn kp(raw: &str) -> KeyPartType {
        KeyPartType::parse(raw)
    }

    #[test]
    fn spelling_keeps_arguments_and_nullable_is_clickhouse_only() {
        assert_eq!(
            SqlDialect::ClickHouse.cast_target(&kp("DateTime64(6)")),
            "DateTime64(6)"
        );
        assert_eq!(
            SqlDialect::ClickHouse.cast_target(&kp("Nullable(DateTime64(3))")),
            "Nullable(DateTime64(3))"
        );
        // Spark has neither a wrapper nor scale-carrying names.
        assert_eq!(
            SqlDialect::Databricks.cast_target(&kp("Nullable(DateTime64(3))")),
            "TIMESTAMP"
        );
        assert_eq!(
            SqlDialect::Databricks.cast_target(&kp("DateTime64(6)")),
            "TIMESTAMP"
        );
    }

    /// A target with no arguments goes through the enum's own mapping, so the
    /// dialect owns that spelling too.
    #[test]
    fn spelling_without_arguments_uses_the_enum_mapping() {
        assert_eq!(SqlDialect::ClickHouse.cast_target(&kp("Int64")), "Int64");
        assert_eq!(SqlDialect::Databricks.cast_target(&kp("Int64")), "BIGINT");
        assert_eq!(SqlDialect::Databricks.cast_target(&kp("String")), "STRING");
        assert_eq!(
            SqlDialect::ClickHouse.cast_target(&kp("Nullable(String)")),
            "Nullable(String)"
        );
    }

    /// #1358: a ClickHouse target is its own spelling, which `SchemaType`
    /// would collapse (`Int128` is no `Int64`).
    #[test]
    fn a_clickhouse_target_keeps_its_spelling() {
        assert_eq!(SqlDialect::ClickHouse.cast_target(&kp("Int128")), "Int128");
        assert_eq!(
            SqlDialect::ClickHouse.cast_target(&kp("Nullable(Int128)")),
            "Nullable(Int128)"
        );
        assert_eq!(SqlDialect::Databricks.cast_target(&kp("Int128")), "BIGINT");
    }
}
