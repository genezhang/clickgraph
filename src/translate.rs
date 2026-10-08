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

use crate::bound_plan::lower::{ResultColumn, ResultKind};
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::open_cypher_parser::ast::CypherStatement;
use crate::query_planner::{self, logical_plan::LogicalPlan, plan_ctx::PlanCtx};
use crate::render_plan::plan_builder::RenderPlanBuilder;
use crate::server::bolt_protocol::result_transformer::{
    extract_return_metadata, ReturnItemMetadata, ReturnItemType,
};

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
    /// The query text `statement` was parsed from (comments stripped). The
    /// bound-plan path (P-4c) parses it with the clause-list parser; a caller
    /// that leaves it `None` always gets the legacy pipeline.
    pub cypher: Option<String>,
    /// Override of `CLICKGRAPH_BOUND_PLAN` (tests).
    pub bound_plan: Option<BoundPlanMode>,
}

/// Which pipeline translates read queries (`CLICKGRAPH_BOUND_PLAN`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundPlanMode {
    /// The legacy pipeline only (default).
    Off,
    /// The bound-plan path for every query it lowers; the legacy pipeline
    /// for the rest (`Unsupported`, or anything it cannot bind).
    On,
}

impl BoundPlanMode {
    /// From `CLICKGRAPH_BOUND_PLAN` (`on` / `off`, default off).
    pub fn from_env() -> Self {
        match std::env::var("CLICKGRAPH_BOUND_PLAN") {
            Ok(v) if v.eq_ignore_ascii_case("on") => BoundPlanMode::On,
            _ => BoundPlanMode::Off,
        }
    }
}

/// Which pipeline produced a translation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    Legacy,
    BoundPlan,
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
    /// The analyzed plan and its context (legacy route), for the result
    /// shape and `include_plan` debugging. `LogicalPlan::Empty` and an empty
    /// context when `route` is `BoundPlan`.
    pub logical_plan: LogicalPlan,
    pub plan_ctx: PlanCtx,
    /// What each RETURN item is, when `route` is `BoundPlan`.
    pub shape: Option<Vec<ResultColumn>>,
    pub timings: TranslateTimings,
    pub route: Route,
}

impl ReadTranslation {
    /// What each RETURN item is and which columns hold it (§4.13): Bolt, the
    /// HTTP graph output and embedded `query_graph` build nodes and
    /// relationships from it, whichever pipeline translated the query.
    pub fn return_metadata(&self) -> Result<Vec<ReturnItemMetadata>, String> {
        match &self.shape {
            Some(shape) => Ok(shape.iter().map(return_item_metadata).collect()),
            None => extract_return_metadata(&self.logical_plan, &self.plan_ctx),
        }
    }
}

/// The result transformer's view of a lowered RETURN item.
fn return_item_metadata(c: &ResultColumn) -> ReturnItemMetadata {
    let item_type = match &c.kind {
        ResultKind::Value => ReturnItemType::Scalar,
        ResultKind::Node { label } => ReturnItemType::Node {
            labels: vec![label.clone()],
        },
        // The columns are in the stored orientation (`from_id` is the
        // relationship's start node), whatever the pattern's direction.
        ResultKind::Rel {
            rel_type,
            from_label,
            to_label,
        } => ReturnItemType::Relationship {
            rel_types: vec![rel_type.clone()],
            from_label: Some(from_label.clone()),
            to_label: Some(to_label.clone()),
            direction: Some("Outgoing".to_string()),
        },
        // `alias` names the column of the transformer's fallbacks, which an
        // item with explicit columns never reaches.
        ResultKind::NodeId { label } => ReturnItemType::IdFunction {
            alias: c.name.clone(),
            labels: vec![label.clone()],
        },
        ResultKind::Graph(ty) => ReturnItemType::Graph(ty.clone()),
    };
    ReturnItemMetadata {
        field_name: c.name.clone(),
        item_type,
        columns: Some(c.columns.clone()),
    }
}

/// A query translated by the bound-plan path.
#[derive(Debug)]
pub struct BoundTranslation {
    pub sql: String,
    pub shape: Vec<ResultColumn>,
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
/// (`USE` extraction, the `id()` rewrite). Server entry points call this
/// inside their `with_query_context` scope (schema, dialect, table stats set).
/// A caller without one (library code, unit tests) gets a fresh per-query
/// context holding `schema`, so a translation never runs on process-global
/// state: in particular the generated-alias counters are always this query's
/// own, and resetting them cannot rewind a concurrent query's.
pub fn translate_read(
    statement: CypherStatement<'_>,
    schema: &GraphSchema,
    options: ReadOptions,
) -> Result<ReadTranslation, TranslateError> {
    use crate::server::query_context::{
        has_query_context, set_current_schema, with_query_context_sync, QueryContext,
    };
    if has_query_context() {
        return translate_in_context(statement, schema, options);
    }
    with_query_context_sync(QueryContext::new(None), || {
        set_current_schema(std::sync::Arc::new(schema.clone()));
        translate_in_context(statement, schema, options)
    })
}

fn translate_in_context(
    statement: CypherStatement<'_>,
    schema: &GraphSchema,
    options: ReadOptions,
) -> Result<ReadTranslation, TranslateError> {
    // Deterministic generated aliases: restart THIS query's counters.
    crate::query_planner::logical_plan::reset_all_counters();

    let mode = options.bound_plan.unwrap_or_else(BoundPlanMode::from_env);
    // Label constraints come from `id() = N` predicates, which the bound
    // plan does not lower (the `id()` rewrite turned them into key
    // predicates in `statement`, not in `cypher`).
    let id_constraints = options
        .where_label_constraints
        .as_ref()
        .is_some_and(|c| !c.is_empty());
    if mode == BoundPlanMode::On && !id_constraints {
        if let Some(cypher) = options.cypher.as_deref() {
            let start = Instant::now();
            match translate_bound_plan(cypher, schema, &options) {
                Ok(BoundTranslation { sql, shape }) => {
                    return Ok(ReadTranslation {
                        sql,
                        logical_plan: LogicalPlan::Empty,
                        plan_ctx: PlanCtx::new_empty(),
                        shape: Some(shape),
                        timings: TranslateTimings {
                            planning: start.elapsed(),
                            ..Default::default()
                        },
                        route: Route::BoundPlan,
                    })
                }
                Err(reason) => log::debug!("bound plan: legacy pipeline ({reason})"),
            }
        }
    }

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
        shape: None,
        timings: TranslateTimings {
            planning,
            render,
            sql_generation,
        },
        route: Route::Legacy,
    })
}

/// The bound-plan path (P-4c): clause list → bind → lower → plain print.
/// `Err` carries why the legacy pipeline translates the query instead.
pub fn translate_bound_plan(
    cypher: &str,
    schema: &GraphSchema,
    options: &ReadOptions,
) -> Result<BoundTranslation, String> {
    use crate::server::query_context::{
        has_query_context, set_current_schema, with_query_context_sync, QueryContext,
    };
    // Printing reads the query context (plain printing, the dialect); a
    // caller without one gets a fresh one, as in `translate_read`.
    if !has_query_context() {
        return with_query_context_sync(QueryContext::new(None), || {
            set_current_schema(std::sync::Arc::new(schema.clone()));
            translate_bound_plan(cypher, schema, options)
        });
    }
    use crate::bound_plan::lower::{lower_statement, LowerOptions};
    let (rest, stmt) = crate::open_cypher_parser::clause_list::parse_clause_statement(cypher)
        .map_err(|e| format!("clause-list parse: {e:?}"))?;
    if !rest.trim().trim_end_matches(';').trim().is_empty() {
        return Err(format!("clause-list parse stopped at: {rest}"));
    }
    let bound = crate::bound_plan::bind_statement(&stmt, schema).map_err(|e| e.to_string())?;
    let lowered = lower_statement(
        &bound,
        schema,
        &LowerOptions {
            view_parameter_values: merged_view_parameters(
                options.tenant_id.as_deref(),
                options.view_parameter_values.as_ref(),
            ),
            neo4j_compat: crate::server::query_context::server_neo4j_compat(),
        },
    )
    .map_err(|e| e.to_string())?;
    Ok(BoundTranslation {
        sql: crate::clickhouse_query_generator::to_sql_query::render_plan_to_sql_plain(
            lowered.plan,
        ),
        shape: lowered.shape,
    })
}

/// The view-parameter values a translation applies: the request's, plus
/// `tenant_id` from the request's tenant unless a `tenant_id` view parameter
/// was given explicitly. The legacy planner merges them the same way
/// (`PlanCtx::with_all_parameters`); tenant isolation of parameterized views
/// depends on it.
fn merged_view_parameters(
    tenant_id: Option<&str>,
    view_parameter_values: Option<&HashMap<String, String>>,
) -> Option<HashMap<String, String>> {
    match (tenant_id, view_parameter_values) {
        (None, values) => values.cloned(),
        (Some(tenant), values) => {
            let mut merged = values.cloned().unwrap_or_default();
            merged
                .entry("tenant_id".to_string())
                .or_insert_with(|| tenant.to_string());
            Some(merged)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::query_planner::logical_plan::{generate_cte_id, generate_id, reset_all_counters};
    use crate::server::query_context::{with_query_context, QueryContext};

    /// A translation without a caller-provided context still runs on its own
    /// per-query counters: the process-global fallback is neither reset nor
    /// advanced (it was, and concurrent `/query/sql` requests, which had no
    /// context, rewound each other's aliases mid-translation).
    #[test]
    fn translation_without_context_leaves_global_counters_alone() {
        let schema = crate::graph_catalog::config::GraphSchemaConfig::from_yaml_str(include_str!(
            "../schemas/test/social_integration.yaml"
        ))
        .unwrap()
        .to_graph_schema()
        .unwrap();
        let translate = || {
            let cypher = "MATCH (a:User)-[:FOLLOWS]->()-[:FOLLOWS]->(c:User) RETURN count(*)";
            let (_, stmt) = crate::open_cypher_parser::parse_cypher_statement(cypher).unwrap();
            super::translate_read(
                stmt,
                &schema,
                super::ReadOptions {
                    max_cte_depth: 100,
                    ..Default::default()
                },
            )
            .unwrap()
            .sql
        };
        let before: u32 = generate_id()[1..].parse().unwrap();
        let first = translate();
        let after: u32 = generate_id()[1..].parse().unwrap();
        assert_eq!(after, before + 1, "translation touched the global counter");
        assert_eq!(first, translate(), "translation is not deterministic");
        // Numbered from this query's own counter (`t1` is the pruned middle
        // node), whatever the global counter's value.
        assert!(
            first.contains("AS t2 ") && first.contains("AS t3 "),
            "{first}"
        );
    }

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
    /// Bolt, the HTTP graph output and embedded `query_graph` read the
    /// result shape through `return_metadata`, whichever pipeline translated
    /// the query (§4.13).
    #[test]
    fn return_metadata_on_both_routes() {
        use crate::server::bolt_protocol::result_transformer::ReturnItemType as T;
        let schema = crate::graph_catalog::config::GraphSchemaConfig::from_yaml_str(include_str!(
            "../schemas/test/social_integration.yaml"
        ))
        .unwrap()
        .to_graph_schema()
        .unwrap();
        let cypher = "MATCH (a:User)<-[r:FOLLOWS]-(b:User) RETURN a AS x, r, id(b), b.name";
        let translate = |mode| {
            let (_, stmt) = crate::open_cypher_parser::parse_cypher_statement(cypher).unwrap();
            super::translate_read(
                stmt,
                &schema,
                super::ReadOptions {
                    max_cte_depth: 100,
                    cypher: Some(cypher.to_string()),
                    bound_plan: Some(mode),
                    // Bolt passes the (empty) constraints of its id() rewrite.
                    where_label_constraints: Some(Default::default()),
                    ..Default::default()
                },
            )
            .unwrap()
        };
        let bound = translate(super::BoundPlanMode::On);
        assert_eq!(bound.route, super::Route::BoundPlan);
        let metadata = bound.return_metadata().unwrap();
        let x_columns = metadata[0].columns.as_ref().expect("explicit columns");
        assert!(
            x_columns.contains(&("name".to_string(), "x.name".to_string())),
            "{x_columns:?}"
        );
        let items: Vec<(String, String)> = bound
            .return_metadata()
            .unwrap()
            .into_iter()
            .map(|m| (m.field_name, format!("{:?}", m.item_type)))
            .collect();
        assert_eq!(
            items,
            [
                (
                    "x",
                    format!(
                        "{:?}",
                        T::Node {
                            labels: vec!["User".into()]
                        }
                    )
                ),
                (
                    "r",
                    format!(
                        "{:?}",
                        T::Relationship {
                            rel_types: vec!["FOLLOWS".into()],
                            from_label: Some("User".into()),
                            to_label: Some("User".into()),
                            // The columns are in the stored orientation.
                            direction: Some("Outgoing".into()),
                        }
                    )
                ),
                (
                    "id(b)",
                    format!(
                        "{:?}",
                        T::IdFunction {
                            alias: "id(b)".into(),
                            labels: vec!["User".into()],
                        }
                    )
                ),
                ("b.name", format!("{:?}", T::Scalar)),
            ]
            .map(|(n, t)| (n.to_string(), t))
        );
        let legacy = translate(super::BoundPlanMode::Off);
        assert_eq!(legacy.route, super::Route::Legacy);
        let legacy_metadata = legacy.return_metadata().unwrap();
        assert_eq!(legacy_metadata.len(), 4);
        assert!(legacy_metadata.iter().all(|m| m.columns.is_none()));
    }
}
