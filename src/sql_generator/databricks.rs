//! Databricks `SqlEmitter` implementation.
//!
//! Phase 1.2 spike: rather than write a parallel renderer from scratch,
//! this delegates to the existing `clickhouse_query_generator` pipeline
//! and relies on the abstractions built in Phase 0.1–1.1 to do dialect
//! switching — `current_function_mapper()` reads from the task-local
//! `QueryContext`, structural-gap call sites branch on dialect, etc.
//!
//! ## What works
//! The ~50 `FunctionMapper`-routed sites and the two structural-gap
//! call sites (`array_count`, `json_extract_string`) emit Databricks-
//! correct SQL. See `Cypher → SQL diff` tests in this module.
//!
//! ## What doesn't yet
//! The rest of the renderer is still ClickHouse-shaped: identifier
//! quoting rules, type names embedded in CASTs/DDL-ish snippets,
//! expression idioms like `arrayJoin` (Spark needs `LATERAL VIEW
//! explode`), `if(...)` (Spark prefers `CASE`), and tuple comparisons.
//! Phase 1.3+ will tackle these as the diff tests surface them.
//!
//! ## Calling contract
//! The caller MUST have set `dialect: Databricks` in the task-local
//! `QueryContext` before invoking `emit`. Without it, the underlying
//! `current_function_mapper()` calls default to ClickHouse and the
//! output is mixed-dialect garbage. The server-side executor is the
//! natural place to set this; tests use `with_query_context` directly.

use super::SqlEmitter;
use crate::render_plan::RenderPlan;

pub(crate) struct DatabricksEmitter;

impl SqlEmitter for DatabricksEmitter {
    fn emit(&self, plan: RenderPlan, max_cte_depth: u32) -> String {
        // Delegating to the CH pipeline is correct here, NOT a copy-paste
        // shortcut — the pipeline already reads dialect from the
        // task-local. The "ClickHouse" in the module name is historical.
        // Once Phase 0.3+ refactoring stabilizes, this whole module
        // becomes `sql_generator::emit(plan, dialect)`.
        crate::clickhouse_query_generator::generate_sql(plan, max_cte_depth)
    }
}
