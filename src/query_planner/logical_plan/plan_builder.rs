use std::collections::HashMap;
use std::sync::Arc;

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    open_cypher_parser::ast::OpenCypherQueryAst,
    query_planner::{
        logical_plan::{
            errors::LogicalPlanError, match_clause, optional_match_clause, order_by_clause,
            return_clause, skip_n_limit_clause, unwind_clause, where_clause, with_clause,
            LogicalPlan,
        },
        plan_ctx::PlanCtx,
    },
};

pub type LogicalPlanResult<T> = Result<T, LogicalPlanError>;

pub fn build_logical_plan(
    query_ast: &OpenCypherQueryAst,
    schema: &GraphSchema,
    tenant_id: Option<String>,
    view_parameter_values: Option<HashMap<String, String>>,
    max_inferred_types: Option<usize>,
) -> LogicalPlanResult<(Arc<LogicalPlan>, PlanCtx)> {
    let mut logical_plan: Arc<LogicalPlan> = Arc::new(LogicalPlan::Empty);
    let mut plan_ctx = PlanCtx::with_all_parameters(
        Arc::new(schema.clone()),
        tenant_id,
        view_parameter_values,
        max_inferred_types.unwrap_or(5),
    );

    log::debug!(
        "build_logical_plan: Processing query with {} MATCH clauses, {} optional_match_clauses",
        query_ast.match_clauses.len(),
        query_ast.optional_match_clauses.len()
    );

    // 🚨 DIAGNOSTIC: Check if query is completely empty
    if query_ast.match_clauses.is_empty()
        && query_ast.optional_match_clauses.is_empty()
        && query_ast.unwind_clauses.is_empty()
        && query_ast.return_clause.is_none()
        && query_ast.with_clause.is_none()
    {
        log::error!("❌ EMPTY QUERY DETECTED: Parser returned empty AST!");
        log::error!("   This usually means:");
        log::error!("   1. Query has unsupported syntax (e.g., multi-line comments with -- style)");
        log::error!("   2. Parser failed silently without returning error");
        log::error!("   3. Query might be using features not yet supported");
        log::error!("   Raw query AST dump:");
        log::error!("     - MATCH clauses: {}", query_ast.match_clauses.len());
        log::error!(
            "     - OPTIONAL MATCH: {}",
            query_ast.optional_match_clauses.len()
        );
        log::error!(
            "     - RETURN clause: {}",
            query_ast.return_clause.is_some()
        );
        log::error!("     - WITH clause: {}", query_ast.with_clause.is_some());
        log::error!("     - WHERE clause: {}", query_ast.where_clause.is_some());
        log::error!("     - ORDER BY: {}", query_ast.order_by_clause.is_some());
        log::error!("     - LIMIT: {}", query_ast.limit_clause.is_some());
        log::error!("     - SKIP: {}", query_ast.skip_clause.is_some());
        log::error!("     - UNWIND: {}", !query_ast.unwind_clauses.is_empty());
        log::error!("     - CALL: {}", query_ast.call_clause.is_some());

        return Err(LogicalPlanError::QueryPlanningError(
            "Parser returned empty query AST. This indicates unsupported syntax or parser failure. \
            Common causes: 1) Multi-line SQL-style comments (use /* */ instead of --), \
            2) Unsupported Cypher features, 3) Query syntax errors not caught by parser. \
            Enable DEBUG logging to see more details.".to_string()
        ));
    }

    // Process all MATCH clauses in sequence
    for (idx, match_clause) in query_ast.match_clauses.iter().enumerate() {
        log::debug!("build_logical_plan: Processing MATCH clause {}", idx);
        logical_plan =
            match_clause::evaluate_match_clause(match_clause, logical_plan, &mut plan_ctx)?;
    }

    // Process OPTIONAL MATCH clauses after regular MATCH
    log::debug!(
        "build_logical_plan: About to process {} OPTIONAL MATCH clauses",
        query_ast.optional_match_clauses.len()
    );
    for (idx, optional_match) in query_ast.optional_match_clauses.iter().enumerate() {
        log::debug!(
            "build_logical_plan: Processing OPTIONAL MATCH clause {}",
            idx
        );
        logical_plan = optional_match_clause::evaluate_optional_match_clause(
            optional_match,
            logical_plan,
            &mut plan_ctx,
        )?;
    }

    // Process UNWIND clauses after MATCH/OPTIONAL MATCH, before WITH
    // UNWIND transforms array values into individual rows
    // Multiple UNWIND clauses create cartesian product
    for unwind_clause_ast in &query_ast.unwind_clauses {
        log::debug!(
            "build_logical_plan: Processing UNWIND clause with alias {}",
            unwind_clause_ast.alias
        );
        logical_plan =
            unwind_clause::evaluate_unwind_clause(unwind_clause_ast, logical_plan, &mut plan_ctx);
    }

    // Process WITH clause before WHERE to create intermediate projections
    // WITH creates a projection that can be referenced by subsequent clauses (including WHERE)
    // This now handles chained WITH...MATCH...WITH patterns via recursion
    if let Some(with_clause_ast) = &query_ast.with_clause {
        logical_plan = process_with_clause_chain(with_clause_ast, logical_plan, &mut plan_ctx)?;
    }

    // Process WHERE clause after WITH so it can reference WITH projection aliases
    // For "WITH a, COUNT(b) as follows WHERE follows > 1", the WHERE can now reference "follows"
    if let Some(where_clause) = &query_ast.where_clause {
        logical_plan = where_clause::evaluate_where_clause(where_clause, logical_plan);
    }

    if let Some(return_clause) = &query_ast.return_clause {
        logical_plan =
            return_clause::evaluate_return_clause(return_clause, logical_plan, &mut plan_ctx);
    }

    if let Some(order_clause) = &query_ast.order_by_clause {
        logical_plan = order_by_clause::evaluate_order_by_clause(order_clause, logical_plan);
    }

    if let Some(skip_clause) = &query_ast.skip_clause {
        logical_plan = skip_n_limit_clause::evaluate_skip_clause(skip_clause, logical_plan);
    }

    if let Some(limit_clause) = &query_ast.limit_clause {
        logical_plan = skip_n_limit_clause::evaluate_limit_clause(limit_clause, logical_plan);
    }

    // 🚨 DIAGNOSTIC: Final check if plan is still Empty after processing
    if matches!(*logical_plan, LogicalPlan::Empty) {
        log::warn!("⚠️  WARNING: Logical plan is Empty after processing all clauses!");
        log::warn!("   This means query parsed but produced no plan. Possible causes:");
        log::warn!("   1. All MATCH clauses failed to generate nodes/relationships");
        log::warn!("   2. Schema mismatch (labels/relationships not in YAML schema)");
        log::warn!("   3. Query pattern not yet supported by planner");
        log::warn!("   Plan type: Empty (no operations)");

        return Err(LogicalPlanError::QueryPlanningError(
            "Query produced Empty logical plan. This indicates query parsed successfully \
            but planner could not generate a valid execution plan. Common causes: \
            1) Node labels or relationship types not defined in schema YAML, \
            2) Complex query patterns not yet supported, \
            3) All MATCH patterns filtered out. \
            Check that all labels and relationship types exist in your schema."
                .to_string(),
        ));
    }

    Ok((logical_plan, plan_ctx))
}

/// Process a chain of WITH clauses recursively
/// Handles patterns like: WITH a MATCH ... WITH a, b MATCH ... WITH a, b, c ...
///
/// Key Implementation Detail: Creates a child scope after WITH clause evaluation.
/// This child scope contains ONLY the exported aliases from WITH, ensuring proper
/// scope isolation as per OpenCypher semantics.
fn process_with_clause_chain<'a>(
    with_clause_ast: &crate::open_cypher_parser::ast::WithClause<'a>,
    mut logical_plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
) -> LogicalPlanResult<Arc<LogicalPlan>> {
    log::debug!(
        "process_with_clause_chain: Processing WITH clause with {} items",
        with_clause_ast.with_items.len()
    );

    // Process the WITH projection itself - returns error if items lack required aliases
    logical_plan = with_clause::evaluate_with_clause(with_clause_ast, logical_plan)?;

    // Extract exported aliases from the WITH clause node
    let exported_aliases = if let LogicalPlan::WithClause(ref with_node) = *logical_plan {
        with_node.exported_aliases.clone()
    } else {
        vec![] // Should not happen, but handle gracefully
    };

    log::debug!(
        "process_with_clause_chain: WITH exports {} aliases: {:?}",
        exported_aliases.len(),
        exported_aliases
    );

    // Create a child scope for processing subsequent clauses
    // CRITICAL: is_with_scope=true makes this scope a BARRIER
    // Variables not in exported_aliases cannot be accessed from parent scope
    // This implements WITH's shielding semantics per OpenCypher spec
    let mut child_ctx = PlanCtx::with_parent_scope(plan_ctx, true);

    // Register each exported alias in the child scope
    // These aliases reference the WITH output (will be CTE columns)
    for alias in &exported_aliases {
        // Check if we can get table context from parent scope
        if let Ok(parent_table_ctx) = plan_ctx.get_table_ctx(alias) {
            // Clone the table context into child scope
            // This preserves labels, properties, etc. from parent scope
            log::debug!(
                "process_with_clause_chain: Copying alias '{}' from parent scope to child scope",
                alias
            );
            child_ctx.insert_table_ctx(alias.clone(), parent_table_ctx.clone());
        } else {
            // Alias might be a new alias (e.g., COUNT(b) AS follows)
            // Create a minimal TableCtx for it
            log::debug!(
                "process_with_clause_chain: Creating new TableCtx for computed alias '{}' in child scope",
                alias
            );
            child_ctx.insert_table_ctx(
                alias.clone(),
                crate::query_planner::plan_ctx::TableCtx::build(
                    alias.clone(),
                    None,   // No label for computed expressions
                    vec![], // No properties yet
                    false,  // Not a relationship
                    true,   // This is an explicit alias from WITH
                ),
            );
        }
    }

    // Process subsequent UNWIND clause if present (e.g., WITH d, rip UNWIND rip.ips AS ip)
    if let Some(subsequent_unwind) = &with_clause_ast.subsequent_unwind {
        log::debug!("process_with_clause_chain: Processing subsequent UNWIND clause after WITH");
        logical_plan =
            unwind_clause::evaluate_unwind_clause(subsequent_unwind, logical_plan, &mut child_ctx);
    }

    // Process subsequent MATCH clause if present (e.g., WITH u MATCH (u)-[:FOLLOWS]->(f))
    if let Some(subsequent_match) = &with_clause_ast.subsequent_match {
        log::debug!("process_with_clause_chain: Processing subsequent MATCH clause after WITH");
        logical_plan =
            match_clause::evaluate_match_clause(subsequent_match, logical_plan, &mut child_ctx)?;
    }

    // Process subsequent OPTIONAL MATCH clauses if present
    for (idx, optional_match) in with_clause_ast
        .subsequent_optional_matches
        .iter()
        .enumerate()
    {
        log::debug!(
            "process_with_clause_chain: Processing subsequent OPTIONAL MATCH clause {} after WITH",
            idx
        );
        logical_plan = optional_match_clause::evaluate_optional_match_clause(
            optional_match,
            logical_plan,
            &mut child_ctx,
        )?;
    }

    // Recursively process subsequent WITH clause if present (chained WITH...MATCH...WITH patterns)
    if let Some(subsequent_with) = &with_clause_ast.subsequent_with {
        log::debug!(
            "process_with_clause_chain: Processing subsequent WITH clause (chained pattern)"
        );
        logical_plan = process_with_clause_chain(subsequent_with, logical_plan, &mut child_ctx)?;
    }

    // Copy child scope back to parent so subsequent clauses can see new aliases
    // Note: This includes aliases from both exported (from WITH) and newly created (from MATCH)
    for (alias, table_ctx) in child_ctx.iter_aliases() {
        plan_ctx.insert_table_ctx(alias.clone(), table_ctx.clone());
    }

    Ok(logical_plan)
}
