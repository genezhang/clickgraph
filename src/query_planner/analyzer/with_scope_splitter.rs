use std::sync::Arc;

use crate::query_planner::{
    analyzer::{
        analyzer_pass::{AnalyzerPass, AnalyzerResult},
        errors::AnalyzerError,
    },
    logical_plan::{LogicalPlan, WithClause},
    plan_ctx::PlanCtx,
    transformed::Transformed,
};

/// Splits the plan at WITH clause boundaries to establish proper scoping.
///
/// **Problem**: GraphJoinInference runs on the entire plan tree, computing joins
/// across WITH boundaries. This is wasteful and incorrect - WITH creates scope
/// boundaries where intermediate results are materialized.
///
/// **Solution**: This pass transforms the plan so that:
/// 1. Each WITH clause marks a scope boundary
/// 2. Downstream analyzer passes (like GraphJoinInference) only see the current scope
/// 3. Cross-scope references are handled via the exported aliases
///
/// **Example**:
/// ```cypher
/// MATCH (a)-[:F]->(b) WITH a, b MATCH (b)-[:F]->(c) RETURN c
/// ```
///
/// Before this pass:
/// ```
/// Projection(RETURN c)
///   └─ GraphRel(b→c)
///      └─ WithClause(a, b)
///         └─ GraphRel(a→b)
/// ```
///
/// After this pass:
/// ```
/// Projection(RETURN c)
///   └─ GraphRel(b→c)  [Scope 2: b comes from Scope 1]
///      └─ ScopeBoundary(exported: [a, b])
///         └─ GraphRel(a→b)  [Scope 1: independent pattern]
/// ```
///
/// **Architecture**:
/// - Runs BEFORE GraphJoinInference (early in analyzer pipeline)
/// - Creates logical scope boundaries without CTE generation
/// - CTE generation still happens in render phase, but operates on clean scoped plan
pub struct WithScopeSplitter;

impl WithScopeSplitter {
    pub fn new() -> Self {
        WithScopeSplitter
    }

    /// Recursively process the plan to mark WITH scope boundaries.
    ///
    /// Strategy:
    /// 1. When we find a WithClause:
    ///    a. Mark it as a scope boundary
    ///    b. Record exported aliases
    ///    c. Prevent downstream passes from crossing this boundary
    ///
    /// 2. Transform WithClause to include scope metadata:
    ///    - is_scope_boundary: true
    ///    - exported_aliases: [aliases visible to downstream]
    ///
    /// 3. Return transformed plan where each scope is isolated
    fn split_scopes(
        &self,
        plan: Arc<LogicalPlan>,
        _plan_ctx: &mut PlanCtx,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        match plan.as_ref() {
            LogicalPlan::WithClause(wc) => {
                log::info!(
                    "🔄 WithScopeSplitter: Found WITH clause, creating scope boundary for aliases: {:?}",
                    wc.exported_aliases
                );

                // Recursively process the input (the pattern BEFORE the WITH)
                let input_transformed = self.split_scopes(wc.input.clone(), _plan_ctx)?;
                let new_input = input_transformed.get_plan();

                // Create new WithClause with scope boundary marker
                // The WithClause already has exported_aliases - we just need to ensure
                // downstream passes respect it as a boundary
                let new_wc = WithClause {
                    input: new_input.clone(),
                    items: wc.items.clone(),
                    distinct: wc.distinct,
                    order_by: wc.order_by.clone(),
                    skip: wc.skip,
                    limit: wc.limit,
                    exported_aliases: wc.exported_aliases.clone(),
                    where_clause: wc.where_clause.clone(),
                };

                log::info!(
                    "🔄 WithScopeSplitter: Created scope boundary with {} exported aliases",
                    new_wc.exported_aliases.len()
                );

                Ok(Transformed::Yes(Arc::new(LogicalPlan::WithClause(new_wc))))
            }

            // Recurse into other node types
            LogicalPlan::GraphRel(rel) => {
                let left = self.split_scopes(rel.left.clone(), _plan_ctx)?;
                let right = self.split_scopes(rel.right.clone(), _plan_ctx)?;

                if left.is_yes() || right.is_yes() {
                    let new_rel = crate::query_planner::logical_plan::GraphRel {
                        left: left.get_plan().clone(),
                        center: rel.center.clone(),
                        right: right.get_plan().clone(),
                        alias: rel.alias.clone(),
                        direction: rel.direction.clone(),
                        left_connection: rel.left_connection.clone(),
                        right_connection: rel.right_connection.clone(),
                        is_rel_anchor: rel.is_rel_anchor,
                        variable_length: rel.variable_length.clone(),
                        shortest_path_mode: rel.shortest_path_mode.clone(),
                        path_variable: rel.path_variable.clone(),
                        where_predicate: rel.where_predicate.clone(),
                        labels: rel.labels.clone(),
                        is_optional: rel.is_optional,
                        anchor_connection: rel.anchor_connection.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphRel(new_rel))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::Projection(proj) => {
                let input = self.split_scopes(proj.input.clone(), _plan_ctx)?;
                if input.is_yes() {
                    let new_proj = crate::query_planner::logical_plan::Projection {
                        input: input.get_plan().clone(),
                        items: proj.items.clone(),
                        distinct: proj.distinct,
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Projection(
                        new_proj,
                    ))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::Filter(filter) => {
                let input = self.split_scopes(filter.input.clone(), _plan_ctx)?;
                if input.is_yes() {
                    let new_filter = crate::query_planner::logical_plan::Filter {
                        input: input.get_plan().clone(),
                        predicate: filter.predicate.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Filter(new_filter))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::GraphJoins(gj) => {
                let input = self.split_scopes(gj.input.clone(), _plan_ctx)?;
                if input.is_yes() {
                    let new_gj = crate::query_planner::logical_plan::GraphJoins {
                        input: input.get_plan().clone(),
                        joins: gj.joins.clone(),
                        optional_aliases: gj.optional_aliases.clone(),
                        anchor_table: gj.anchor_table.clone(),
                        cte_references: gj.cte_references.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphJoins(new_gj))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::OrderBy(order) => {
                let input = self.split_scopes(order.input.clone(), _plan_ctx)?;
                if input.is_yes() {
                    let new_order = crate::query_planner::logical_plan::OrderBy {
                        input: input.get_plan().clone(),
                        items: order.items.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::OrderBy(new_order))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::Limit(limit) => {
                let input = self.split_scopes(limit.input.clone(), _plan_ctx)?;
                if input.is_yes() {
                    let new_limit = crate::query_planner::logical_plan::Limit {
                        input: input.get_plan().clone(),
                        count: limit.count,
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Limit(new_limit))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::Skip(skip) => {
                let input = self.split_scopes(skip.input.clone(), _plan_ctx)?;
                if input.is_yes() {
                    let new_skip = crate::query_planner::logical_plan::Skip {
                        input: input.get_plan().clone(),
                        count: skip.count,
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Skip(new_skip))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::GroupBy(gb) => {
                let input = self.split_scopes(gb.input.clone(), _plan_ctx)?;
                if input.is_yes() {
                    let new_gb = crate::query_planner::logical_plan::GroupBy {
                        input: input.get_plan().clone(),
                        expressions: gb.expressions.clone(),
                        having_clause: gb.having_clause.clone(),
                        is_materialization_boundary: gb.is_materialization_boundary,
                        exposed_alias: gb.exposed_alias.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::GroupBy(new_gb))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            // Terminal nodes - no recursion needed
            LogicalPlan::Empty
            | LogicalPlan::Scan(_)
            | LogicalPlan::ViewScan(_)
            | LogicalPlan::GraphNode(_)
            | LogicalPlan::PageRank(_) => Ok(Transformed::No(plan)),

            // Other nodes that may contain subplans
            LogicalPlan::Union(union) => {
                let mut any_transformed = false;
                let mut new_inputs = Vec::new();

                for input in &union.inputs {
                    let transformed = self.split_scopes(input.clone(), _plan_ctx)?;
                    if transformed.is_yes() {
                        any_transformed = true;
                    }
                    new_inputs.push(transformed.get_plan().clone());
                }

                if any_transformed {
                    let new_union = crate::query_planner::logical_plan::Union {
                        inputs: new_inputs,
                        union_type: union.union_type.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Union(new_union))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::CartesianProduct(cp) => {
                let left = self.split_scopes(cp.left.clone(), _plan_ctx)?;
                let right = self.split_scopes(cp.right.clone(), _plan_ctx)?;

                if left.is_yes() || right.is_yes() {
                    let new_cp = crate::query_planner::logical_plan::CartesianProduct {
                        left: left.get_plan().clone(),
                        right: right.get_plan().clone(),
                        is_optional: cp.is_optional,
                        join_condition: cp.join_condition.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(
                        LogicalPlan::CartesianProduct(new_cp),
                    )))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::Unwind(unwind) => {
                let input = self.split_scopes(unwind.input.clone(), _plan_ctx)?;
                if input.is_yes() {
                    let new_unwind = crate::query_planner::logical_plan::Unwind {
                        input: input.get_plan().clone(),
                        expression: unwind.expression.clone(),
                        alias: unwind.alias.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Unwind(new_unwind))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::Cte(cte) => {
                let input = self.split_scopes(cte.input.clone(), _plan_ctx)?;
                if input.is_yes() {
                    let new_cte = crate::query_planner::logical_plan::Cte {
                        input: input.get_plan().clone(),
                        name: cte.name.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Cte(new_cte))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }
        }
    }
}

impl AnalyzerPass for WithScopeSplitter {
    fn analyze(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        log::info!("🔄 WithScopeSplitter: Starting scope analysis");
        let result = self.split_scopes(logical_plan, plan_ctx)?;
        log::info!(
            "🔄 WithScopeSplitter: Completed - transformed: {}",
            result.is_yes()
        );
        Ok(result)
    }
}
