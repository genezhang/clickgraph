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

    /// Whether ids of `parts` held in one column (a union of labels') are
    /// held as texts there, as their values have no common type in this
    /// dialect (`schema_types::of_several_kinds`, ELEMENT_IDENTITY.md I2).
    /// Spark: as before.
    pub fn ids_held_as_text(
        &self,
        parts: &[crate::graph_catalog::schema_types::KeyPartType],
    ) -> bool {
        match self {
            SqlDialect::ClickHouse => crate::graph_catalog::schema_types::of_several_kinds(parts),
            _ => false,
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
/// The server connection pool, the chdb executor and [`portable_sql`] apply
/// these through [`session_settings`], which adds the opt-in
/// [`DATETIME_ISO8601_SETTING`] when it is on.
pub const SEMANTIC_SESSION_SETTINGS: &[(&str, &str)] = &[("join_use_nulls", "1")];

/// The opt-in output-format setting (`CLICKGRAPH_DATETIME_ISO8601`, #1068):
/// ClickHouse prints DateTime / DateTime64 as ISO-8601 (`2024-03-15T10:30:00Z`).
/// A serialization setting, so it is applied only when asked for, never as
/// part of [`SEMANTIC_SESSION_SETTINGS`].
pub const DATETIME_ISO8601_SETTING: (&str, &str) = ("date_time_output_format", "iso");

/// The settings every ClickHouse session and SQL-only output carries:
/// [`SEMANTIC_SESSION_SETTINGS`], and [`DATETIME_ISO8601_SETTING`] when
/// `datetime_iso8601` (`query_context::datetime_iso8601`).
pub fn session_settings_with(datetime_iso8601: bool) -> Vec<(&'static str, &'static str)> {
    let mut settings = SEMANTIC_SESSION_SETTINGS.to_vec();
    if datetime_iso8601 {
        settings.push(DATETIME_ISO8601_SETTING);
    }
    settings
}

/// [`session_settings_with`] for the process-wide opt-in.
pub fn session_settings() -> Vec<(&'static str, &'static str)> {
    session_settings_with(crate::server::query_context::datetime_iso8601())
}

/// A setting's value spelled for a SQL `SETTINGS`/`SET` clause: string values
/// need quotes (`date_time_output_format = 'iso'`: unquoted is a syntax
/// error), numbers don't (`join_use_nulls = 1`). Client option values stay
/// raw (`?name=value`).
pub fn setting_sql(k: &str, v: &str) -> String {
    if v.chars().all(|c| c.is_ascii_digit()) {
        format!("{k} = {v}")
    } else {
        format!("{k} = '{v}'")
    }
}

/// SQL that is correct on any session of `dialect`: for ClickHouse, appends a
/// `SETTINGS` clause carrying [`session_settings`], so SQL-only output
/// that a user runs outside ClickGraph gives the same rows (#1314). Other
/// dialects already have standard NULL semantics and are returned unchanged.
///
/// Executors must NOT use this: they apply the settings at session level and
/// the executed SQL stays exactly what the emitter produced.
pub fn portable_sql(sql: &str, dialect: SqlDialect) -> String {
    portable_sql_with(
        sql,
        dialect,
        crate::server::query_context::datetime_iso8601(),
    )
}

/// [`portable_sql`] with ISO-8601 datetime output on or off.
pub fn portable_sql_with(sql: &str, dialect: SqlDialect, datetime_iso8601: bool) -> String {
    match dialect {
        SqlDialect::ClickHouse => {
            let settings = session_settings_with(datetime_iso8601)
                .iter()
                .map(|(k, v)| setting_sql(k, v))
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

    /// #1068: ISO-8601 datetime output is an opt-in. Off, the settings and the
    /// SQL-only suffix are exactly what they were; on, the output-format
    /// setting is added to both (quoted, a string value).
    #[test]
    fn iso_datetime_output_is_an_opt_in_1068() {
        assert_eq!(session_settings_with(false), vec![("join_use_nulls", "1")]);
        assert_eq!(
            session_settings_with(true),
            vec![("join_use_nulls", "1"), ("date_time_output_format", "iso")]
        );
        assert_eq!(
            portable_sql_with("SELECT 1", SqlDialect::ClickHouse, false),
            "SELECT 1\nSETTINGS join_use_nulls = 1"
        );
        assert_eq!(
            portable_sql_with("SELECT 1", SqlDialect::ClickHouse, true),
            "SELECT 1\nSETTINGS join_use_nulls = 1, date_time_output_format = 'iso'"
        );
        // The process-wide opt-in is what `portable_sql` / `session_settings` read.
        crate::server::query_context::with_datetime_iso8601(true, || {
            assert_eq!(session_settings().len(), 2);
            assert!(portable_sql("SELECT 1", SqlDialect::ClickHouse)
                .ends_with("date_time_output_format = 'iso'"));
        });
        crate::server::query_context::with_datetime_iso8601(false, || {
            assert_eq!(session_settings().len(), 1);
        });
    }

    #[test]
    fn databricks_sql_is_unchanged() {
        let sql = "SELECT a.x FROM t AS a";
        assert_eq!(portable_sql(sql, SqlDialect::Databricks), sql);
        assert_eq!(portable_sql_with(sql, SqlDialect::Databricks, true), sql);
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
