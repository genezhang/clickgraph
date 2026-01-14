//! CTE Reference Populator Pass
//!
//! **Purpose**: Populate GraphRel.cte_references after VariableResolver runs.
//! This tells the renderer which node connections come from CTEs vs base tables.
//!
//! **Why a separate pass**: VariableResolver handles complex scope semantics,
//! but for CTE references we just need to track which aliases are exported by
//! WITH clauses and propagate that information down the tree.

use std::collections::HashMap;
use std::sync::Arc;

use crate::query_planner::{
    analyzer::{analyzer_pass::AnalyzerPass, errors::AnalyzerError},
    logical_plan::{LogicalPlan, WithClause},
    plan_ctx::PlanCtx,
    transformed::Transformed,
};

pub struct CteReferencePopulator;

impl CteReferencePopulator {
    pub fn new() -> Self {
        CteReferencePopulator
    }

    /// Recursively populate cte_references in GraphRel nodes
    ///
    /// @param plan: The plan to process
    /// @param available_ctes: Map of alias -> CTE name for all CTEs visible at this point
    fn populate(
        &self,
        plan: Arc<LogicalPlan>,
        available_ctes: &HashMap<String, String>,
    ) -> Result<Transformed<Arc<LogicalPlan>>, AnalyzerError> {
        match plan.as_ref() {
            LogicalPlan::WithClause(wc) => {
                // CRITICAL: Add this WITH's exports to available_ctes BEFORE processing input
                // This way, GraphRels inside the input can find the CTE references
                let mut input_ctes = available_ctes.clone();
                for alias in &wc.exported_aliases {
                    if let Some(cte_name) = wc.cte_references.get(alias) {
                        log::info!("🔍 CteReferencePopulator: Adding WITH export '{}' -> '{}' for input processing",
                                   alias, cte_name);
                        input_ctes.insert(alias.clone(), cte_name.clone());
                    }
                }

                // Process the input with updated CTE context
                let input_resolved = self.populate(wc.input.clone(), &input_ctes)?;

                if input_resolved.is_yes() {
                    let new_wc = WithClause {
                        input: input_resolved.get_plan(),
                        ..wc.clone()
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::WithClause(new_wc))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::GraphRel(rel) => {
                // Check if left_connection or right_connection are in available CTEs
                let mut cte_refs = rel.cte_references.clone();
                let mut found_new_refs = false;

                if available_ctes.contains_key(&rel.left_connection) {
                    if let Some(cte_name) = available_ctes.get(&rel.left_connection) {
                        log::info!("🔍 CteReferencePopulator: GraphRel '{}' left_connection '{}' -> CTE '{}'",
                                   rel.alias, rel.left_connection, cte_name);
                        cte_refs.insert(rel.left_connection.clone(), cte_name.clone());
                        found_new_refs = true;
                    }
                }

                if available_ctes.contains_key(&rel.right_connection) {
                    if let Some(cte_name) = available_ctes.get(&rel.right_connection) {
                        log::info!("🔍 CteReferencePopulator: GraphRel '{}' right_connection '{}' -> CTE '{}'",
                                   rel.alias, rel.right_connection, cte_name);
                        cte_refs.insert(rel.right_connection.clone(), cte_name.clone());
                        found_new_refs = true;
                    }
                }

                // Process children with updated CTE context (add this WITH's exports if left is a WITH)
                let mut child_ctes = available_ctes.clone();
                if let LogicalPlan::WithClause(wc) = rel.left.as_ref() {
                    for alias in &wc.exported_aliases {
                        if let Some(cte_name) = wc.cte_references.get(alias) {
                            child_ctes.insert(alias.clone(), cte_name.clone());
                        }
                    }
                }

                let left_resolved = self.populate(rel.left.clone(), &child_ctes)?;
                let center_resolved = self.populate(rel.center.clone(), &child_ctes)?;
                let right_resolved = self.populate(rel.right.clone(), &child_ctes)?;

                if left_resolved.is_yes()
                    || center_resolved.is_yes()
                    || right_resolved.is_yes()
                    || found_new_refs
                {
                    let new_rel = crate::query_planner::logical_plan::GraphRel {
                        left: left_resolved.get_plan(),
                        center: center_resolved.get_plan(),
                        right: right_resolved.get_plan(),
                        cte_references: cte_refs,
                        ..rel.clone()
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphRel(new_rel))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            // For all other node types, recursively process children
            LogicalPlan::Projection(proj) => {
                let input_resolved = self.populate(proj.input.clone(), available_ctes)?;
                if input_resolved.is_yes() {
                    let new_proj = crate::query_planner::logical_plan::Projection {
                        input: input_resolved.get_plan(),
                        ..proj.clone()
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Projection(
                        new_proj,
                    ))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::Filter(filter) => {
                let input_resolved = self.populate(filter.input.clone(), available_ctes)?;
                if input_resolved.is_yes() {
                    let new_filter = crate::query_planner::logical_plan::Filter {
                        input: input_resolved.get_plan(),
                        ..filter.clone()
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Filter(new_filter))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::GraphJoins(gj) => {
                let input_resolved = self.populate(gj.input.clone(), available_ctes)?;
                if input_resolved.is_yes() {
                    let new_gj = crate::query_planner::logical_plan::GraphJoins {
                        input: input_resolved.get_plan(),
                        ..gj.clone()
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphJoins(new_gj))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            // Other node types don't contain GraphRels, so no changes needed
            _ => Ok(Transformed::No(plan)),
        }
    }
}

impl AnalyzerPass for CteReferencePopulator {
    fn analyze(
        &self,
        logical_plan: Arc<LogicalPlan>,
        _plan_ctx: &mut PlanCtx,
    ) -> Result<Transformed<Arc<LogicalPlan>>, AnalyzerError> {
        log::info!("🔍 CteReferencePopulator: Starting CTE reference population");

        let empty_ctes = HashMap::new();
        let result = self.populate(logical_plan, &empty_ctes)?;

        log::info!(
            "🔍 CteReferencePopulator: Completed - transformed: {}",
            result.is_yes()
        );

        Ok(result)
    }
}
