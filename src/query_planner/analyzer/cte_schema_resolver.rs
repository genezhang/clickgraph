//! CTE Schema Resolver - Registers CTE schemas in PlanCtx for analyzer/planner use
//!
//! This pass processes WithClause nodes and registers their exported schemas in plan_ctx.
//! This makes CTE column information available to downstream analyzer passes (join planning,
//! column validation, optimization) without requiring plan tree traversal.
//!
//! Scope: WithClause CTEs only (not VLP recursive CTEs, which are renderer-generated)
//!
//! Example:
//!   Input:  WITH p AS (MATCH (p:Person) RETURN p.firstName AS name, p.age)
//!   Output: plan_ctx.cte_columns["with_p_cte_1"] = {"firstName" → "name", "age" → "p_age"}
//!
//! This enables the analyzer to answer "what columns does this CTE export?" without
//! traversing back through the plan, making join planning and validation more efficient.

use std::sync::Arc;

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    query_planner::{
        analyzer::analyzer_pass::{AnalyzerPass, AnalyzerResult},
        logical_plan::{LogicalPlan, WithClause},
        plan_ctx::PlanCtx,
        transformed::Transformed,
    },
    utils::cte_naming::generate_cte_name,
};

pub struct CteSchemaResolver;

impl CteSchemaResolver {
    pub fn new() -> Self {
        Self
    }

    /// Generate CTE name for a WithClause based on its exported aliases
    /// This matches the naming convention used by the renderer
    fn generate_cte_name(with_clause: &WithClause, plan_ctx: &mut PlanCtx) -> String {
        // Sort aliases to ensure consistent naming
        let mut sorted_aliases = with_clause.exported_aliases.clone();
        sorted_aliases.sort();

        let cte_counter = plan_ctx.cte_counter;
        plan_ctx.cte_counter += 1;

        // Format: with_{aliases}_cte_{counter}
        // Example: with_p_cte_1, with_a_b_cte_2
        if sorted_aliases.is_empty() {
            format!("with_cte_{}", cte_counter)
        } else {
            format!("with_{}_cte_{}", sorted_aliases.join("_"), cte_counter)
        }
    }

    /// Process a WithClause and register its schema
    fn register_with_clause_schema(with_clause: &WithClause, plan_ctx: &mut PlanCtx) {
        // Generate CTE name using centralized utility
        let cte_counter = plan_ctx.cte_counter;
        plan_ctx.cte_counter += 1;
        let cte_name = generate_cte_name(&with_clause.exported_aliases, cte_counter);

        log::debug!(
            "🔧 CteSchemaResolver: Registering schema for CTE '{}' with {} items",
            cte_name,
            with_clause.items.len()
        );

        // Register the columns from projection items
        plan_ctx.register_cte_columns(&cte_name, &with_clause.items);

        // NEW: Register entity types for exported aliases
        // This preserves node/relationship type information across WITH boundaries
        plan_ctx.register_cte_entity_types(&cte_name, &with_clause.exported_aliases);

        log::info!(
            "✅ CteSchemaResolver: Registered CTE '{}' with {} columns",
            cte_name,
            with_clause.items.len()
        );
    }
}

impl AnalyzerPass for CteSchemaResolver {
    fn analyze_with_graph_schema(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        _graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        log::debug!(
            "🔧 CteSchemaResolver: Processing plan type: {:?}",
            std::mem::discriminant(&*logical_plan)
        );

        Ok(match logical_plan.as_ref() {
            // WithClause - register its schema!
            LogicalPlan::WithClause(wc) => {
                // First, recurse into the input (to handle nested WITHs)
                let child_tf =
                    self.analyze_with_graph_schema(wc.input.clone(), plan_ctx, _graph_schema)?;

                // Register this WITH clause's schema
                Self::register_with_clause_schema(wc, plan_ctx);

                // Return unchanged plan (we only modify plan_ctx, not the plan itself)
                if child_tf.is_yes() {
                    Transformed::Yes(Arc::new(LogicalPlan::WithClause(
                        crate::query_planner::logical_plan::WithClause {
                            input: child_tf.get_plan(),
                            ..wc.clone()
                        },
                    )))
                } else {
                    Transformed::No(logical_plan.clone())
                }
            }

            // Recursively process other plan types
            LogicalPlan::GraphNode(node) => {
                let child_tf =
                    self.analyze_with_graph_schema(node.input.clone(), plan_ctx, _graph_schema)?;
                node.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphRel(rel) => {
                let left_tf =
                    self.analyze_with_graph_schema(rel.left.clone(), plan_ctx, _graph_schema)?;
                let center_tf =
                    self.analyze_with_graph_schema(rel.center.clone(), plan_ctx, _graph_schema)?;
                let right_tf =
                    self.analyze_with_graph_schema(rel.right.clone(), plan_ctx, _graph_schema)?;

                if left_tf.is_yes() || center_tf.is_yes() || right_tf.is_yes() {
                    Transformed::Yes(Arc::new(LogicalPlan::GraphRel(
                        crate::query_planner::logical_plan::GraphRel {
                            left: left_tf.get_plan(),
                            center: center_tf.get_plan(),
                            right: right_tf.get_plan(),
                            ..rel.clone()
                        },
                    )))
                } else {
                    Transformed::No(logical_plan.clone())
                }
            }
            LogicalPlan::Projection(proj) => {
                let child_tf =
                    self.analyze_with_graph_schema(proj.input.clone(), plan_ctx, _graph_schema)?;
                proj.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Filter(filter) => {
                let child_tf =
                    self.analyze_with_graph_schema(filter.input.clone(), plan_ctx, _graph_schema)?;
                filter.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GroupBy(group_by) => {
                let child_tf = self.analyze_with_graph_schema(
                    group_by.input.clone(),
                    plan_ctx,
                    _graph_schema,
                )?;
                group_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::OrderBy(order_by) => {
                let child_tf = self.analyze_with_graph_schema(
                    order_by.input.clone(),
                    plan_ctx,
                    _graph_schema,
                )?;
                order_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphJoins(joins) => {
                let child_tf =
                    self.analyze_with_graph_schema(joins.input.clone(), plan_ctx, _graph_schema)?;
                joins.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Limit(limit) => {
                let child_tf =
                    self.analyze_with_graph_schema(limit.input.clone(), plan_ctx, _graph_schema)?;
                limit.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Skip(skip) => {
                let child_tf =
                    self.analyze_with_graph_schema(skip.input.clone(), plan_ctx, _graph_schema)?;
                skip.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Union(union) => {
                let mut transformed = false;
                let resolved_inputs: Vec<Arc<LogicalPlan>> = union
                    .inputs
                    .iter()
                    .map(|input| {
                        let child_tf = self
                            .analyze_with_graph_schema(input.clone(), plan_ctx, _graph_schema)
                            .unwrap();
                        if child_tf.is_yes() {
                            transformed = true;
                        }
                        child_tf.get_plan()
                    })
                    .collect();

                if transformed {
                    Transformed::Yes(Arc::new(LogicalPlan::Union(
                        crate::query_planner::logical_plan::Union {
                            inputs: resolved_inputs,
                            union_type: union.union_type.clone(),
                        },
                    )))
                } else {
                    Transformed::No(logical_plan.clone())
                }
            }

            // Leaf nodes - no recursion needed
            LogicalPlan::Empty
            | LogicalPlan::ViewScan(_)
            | LogicalPlan::Cte(_)
            | LogicalPlan::PageRank(_)
            | LogicalPlan::Unwind(_)
            | LogicalPlan::CartesianProduct(_) => Transformed::No(logical_plan.clone()),
        })
    }
}
