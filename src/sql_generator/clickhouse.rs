//! ClickHouse `SqlEmitter` implementation.
//!
//! Phase 0.1: a thin facade over the existing `clickhouse_query_generator`
//! module so the trait can be introduced without moving any code yet.
//! Later phases move the real logic into this module and reduce
//! `clickhouse_query_generator` to a deprecated re-export.

use super::{Dialect, SqlEmitter};
use crate::render_plan::RenderPlan;

pub struct ClickhouseEmitter;

impl SqlEmitter for ClickhouseEmitter {
    fn dialect(&self) -> Dialect {
        Dialect::ClickHouse
    }

    fn emit(&self, plan: RenderPlan, max_cte_depth: u32) -> String {
        crate::clickhouse_query_generator::generate_sql(plan, max_cte_depth)
    }
}
