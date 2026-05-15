//! SQL generation — dialect-aware entry point.
//!
//! This module is the gradual replacement for `clickhouse_query_generator`. It
//! introduces a `SqlEmitter` trait that lets the SQL layer be swapped per
//! target database (ClickHouse today; Databricks/Spark SQL planned — see
//! `docs/design/DELTAGRAPH_PLAN.md`).
//!
//! ## Phase 0.1 status
//! The trait is in place but the only emitter (`ClickhouseEmitter`) currently
//! delegates to the existing `clickhouse_query_generator` so behavior is
//! unchanged. The trait surface is `pub(crate)` so it can evolve freely in
//! subsequent phases without forming a semver commitment to external users —
//! only `SqlDialect` and `generate_sql` are public.

use crate::render_plan::RenderPlan;
use serde::{Deserialize, Serialize};

pub(crate) mod clickhouse;

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
    // Databricks,  // planned — see docs/design/DELTAGRAPH_PLAN.md
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
        }
    }

    /// Whether an emitter is implemented for this dialect today.
    pub fn is_supported(&self) -> bool {
        matches!(self, SqlDialect::ClickHouse)
    }
}

/// Renders a `RenderPlan` into SQL text for a target dialect.
///
/// `pub(crate)` because the method surface will widen in later phases
/// (function mapping, recursive CTE shape, quote rules, NULL semantics).
/// External consumers should call the free function [`generate_sql`] instead.
pub(crate) trait SqlEmitter: Send + Sync {
    fn emit(&self, plan: RenderPlan, max_cte_depth: u32) -> String;
}

/// Returns a borrowed static emitter for the given dialect.
///
/// No heap allocation: each emitter is a zero-sized type held in a `static`.
pub(crate) fn emitter_for(dialect: SqlDialect) -> &'static dyn SqlEmitter {
    static CLICKHOUSE: clickhouse::ClickhouseEmitter = clickhouse::ClickhouseEmitter;
    match dialect {
        SqlDialect::ClickHouse => &CLICKHOUSE,
        d => unimplemented!("SQL emitter for dialect {:?} is not yet implemented", d),
    }
}

/// Convenience: render a plan using the default dialect (ClickHouse).
///
/// Existing call sites in `clickhouse_query_generator::generate_sql` continue
/// to work unchanged; new call sites should prefer this entry point so the
/// dialect can be chosen at runtime in a future phase.
pub fn generate_sql(plan: RenderPlan, max_cte_depth: u32) -> String {
    emitter_for(SqlDialect::default()).emit(plan, max_cte_depth)
}
