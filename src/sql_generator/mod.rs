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
