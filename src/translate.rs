//! The single read-query translation seam: Cypher statement → SQL.
//!
//! Every read entry point (HTTP `/query` and `/query/sql`, Bolt, `COPY TO` /
//! `apoc.export` inner queries, embedded `cypher_to_sql*`, and through it
//! FFI / Go / Python / `cg`) translates through [`translate_read`]. Before this
//! seam each caller ran its own copy of parse-result → plan → render → SQL, and
//! the copies drifted (`/query/sql` skipped the analyzer passes; counters were
//! reset on some paths and not others). P-4c (`docs/design/EXPLICIT_SCOPE.md`)
//! routes queries to the bound-plan path here, so this is the only place that
//! needs to know which pipeline produced the SQL.
//!
//! The SQL returned here is meant to run on a session that applies
//! [`crate::sql_generator::SEMANTIC_SESSION_SETTINGS`] (every executor does).
//! SQL handed to a user for running elsewhere goes through
//! [`crate::sql_generator::portable_sql`], which carries those settings in the
//! statement itself (#1314).

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::graph_catalog::graph_schema::GraphSchema;
use crate::open_cypher_parser::ast::CypherStatement;
use crate::query_planner::{self, logical_plan::LogicalPlan, plan_ctx::PlanCtx};
use crate::render_plan::plan_builder::RenderPlanBuilder;

/// Caller-supplied inputs that change how a read query is planned or rendered.
#[derive(Debug, Clone, Default)]
pub struct ReadOptions {
    pub tenant_id: Option<String>,
    pub view_parameter_values: Option<HashMap<String, String>>,
    pub max_inferred_types: Option<usize>,
    /// `WHERE`-derived label constraints computed by the Bolt `id()` rewrite
    /// (second pass); injected into the plan context before rendering.
    pub where_label_constraints: Option<HashMap<String, std::collections::HashSet<String>>>,
    pub max_cte_depth: u32,
}

/// Wall-clock time spent in each translation stage (HTTP metrics).
#[derive(Debug, Clone, Copy, Default)]
pub struct TranslateTimings {
    pub planning: Duration,
    pub render: Duration,
    pub sql_generation: Duration,
}

/// A translated read query.
#[derive(Debug)]
pub struct ReadTranslation {
    /// SQL for an executor that applies the semantic session settings.
    pub sql: String,
    /// The analyzed plan and its context, for result-shape metadata
    /// (`extract_return_metadata`, graph output) and `include_plan` debugging.
    pub logical_plan: LogicalPlan,
    pub plan_ctx: PlanCtx,
    pub timings: TranslateTimings,
}

/// The stage a translation failed in. Callers map planning errors to client
/// errors and render errors to internal errors, as before.
#[derive(Debug)]
pub enum TranslateError {
    Planning(query_planner::QueryPlannerError),
    Render(crate::render_plan::errors::RenderBuildError),
}

impl std::fmt::Display for TranslateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TranslateError::Planning(e) => write!(f, "{}", e),
            TranslateError::Render(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for TranslateError {}

/// Translate a parsed read statement to SQL.
///
/// The statement must already have gone through the caller's AST pre-passes
/// (`USE` extraction, the `id()` rewrite). Must run inside the caller's
/// `with_query_context` scope, with the schema (and dialect, where it is not
/// the server default) set on it.
pub fn translate_read(
    statement: CypherStatement<'_>,
    schema: &GraphSchema,
    options: ReadOptions,
) -> Result<ReadTranslation, TranslateError> {
    // Deterministic generated aliases for every entry point.
    crate::query_planner::logical_plan::reset_all_counters();

    let planning_start = Instant::now();
    let (logical_plan, mut plan_ctx) = query_planner::evaluate_read_statement(
        statement,
        schema,
        options.tenant_id,
        options.view_parameter_values,
        options.max_inferred_types,
    )
    .map_err(TranslateError::Planning)?;
    if let Some(constraints) = options.where_label_constraints {
        if !constraints.is_empty() {
            plan_ctx.set_where_label_constraints(constraints);
        }
    }
    let planning = planning_start.elapsed();

    let render_start = Instant::now();
    let render_plan = logical_plan
        .to_render_plan_with_ctx(schema, Some(&plan_ctx), None)
        .map_err(TranslateError::Render)?;
    let render = render_start.elapsed();

    let sql_start = Instant::now();
    let sql = crate::clickhouse_query_generator::generate_sql(render_plan, options.max_cte_depth);
    let sql_generation = sql_start.elapsed();

    Ok(ReadTranslation {
        sql,
        logical_plan,
        plan_ctx,
        timings: TranslateTimings {
            planning,
            render,
            sql_generation,
        },
    })
}

#[cfg(test)]
mod tests {
    use crate::query_planner::logical_plan::{generate_cte_id, generate_id, reset_all_counters};
    use crate::server::query_context::{with_query_context, QueryContext};

    /// A query's generated aliases are numbered by its OWN counters: another
    /// query resetting its counters (every translation does) must not rewind
    /// them. With process-global counters, the inner reset below made the outer
    /// query issue `t2` a second time.
    #[tokio::test]
    async fn generated_alias_counters_are_per_query() {
        let outer = with_query_context(QueryContext::new(None), async {
            reset_all_counters();
            let a = generate_id();
            let b = generate_id();
            let inner = with_query_context(QueryContext::new(None), async {
                reset_all_counters();
                (generate_id(), generate_cte_id())
            })
            .await;
            let c = generate_id();
            let cte = generate_cte_id();
            (a, b, inner, c, cte)
        })
        .await;
        assert_eq!(
            outer,
            (
                "t1".to_string(),
                "t2".to_string(),
                ("t1".to_string(), "cte1".to_string()),
                "t3".to_string(),
                "cte1".to_string(),
            )
        );
    }
}
