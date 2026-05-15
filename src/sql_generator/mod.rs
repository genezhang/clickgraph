//! SQL generation — dialect-aware entry point.
//!
//! This module is the gradual replacement for `clickhouse_query_generator`. It
//! introduces a `SqlEmitter` trait that lets the SQL layer be swapped per
//! target database (ClickHouse today; Databricks/Spark SQL planned — see
//! `docs/design/DELTAGRAPH_PLAN.md`).
//!
//! ## Phase 0.1 status
//! The trait is in place but every method currently delegates to the existing
//! `clickhouse_query_generator` so behavior is unchanged. Subsequent phases
//! will (a) move ClickHouse-specific logic behind the trait and (b) add a
//! Databricks emitter alongside.

use crate::render_plan::RenderPlan;

pub mod clickhouse;

/// Target SQL dialect. New variants are added as emitters are implemented.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Dialect {
    #[default]
    ClickHouse,
    // Databricks,  // planned — see docs/design/DELTAGRAPH_PLAN.md
}

impl Dialect {
    pub fn name(self) -> &'static str {
        match self {
            Dialect::ClickHouse => "clickhouse",
        }
    }
}

/// Renders a `RenderPlan` into SQL text for a target dialect.
///
/// During Phase 0.1 there is exactly one implementor (`ClickhouseEmitter`)
/// that forwards to `clickhouse_query_generator::generate_sql`. Future phases
/// will widen this surface area (function mapping, recursive CTE shape,
/// quote rules, NULL semantics) so the trait can host real per-dialect logic.
pub trait SqlEmitter: Send + Sync {
    fn dialect(&self) -> Dialect;

    /// Render a full `RenderPlan` into a SQL string.
    fn emit(&self, plan: RenderPlan, max_cte_depth: u32) -> String;
}

/// Returns the emitter for a given dialect.
pub fn emitter_for(dialect: Dialect) -> Box<dyn SqlEmitter> {
    match dialect {
        Dialect::ClickHouse => Box::new(clickhouse::ClickhouseEmitter),
    }
}

/// Convenience: render a plan using the default dialect (ClickHouse).
///
/// Existing call sites in `clickhouse_query_generator::generate_sql` continue
/// to work unchanged; new call sites should prefer this entry point so the
/// dialect can be chosen at runtime.
pub fn generate_sql(plan: RenderPlan, max_cte_depth: u32) -> String {
    emitter_for(Dialect::default()).emit(plan, max_cte_depth)
}
