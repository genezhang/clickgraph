//! WHERE clause processing.
//!
//! Converts Cypher WHERE conditions into [`Filter`] logical plan nodes.
//! Handles filter pushdown into UNION branches for optimized execution.
//!
//! # SQL Translation
//!
//! ```text
//! WHERE u.active = true AND u.age > 18
//! → WHERE users.is_active = 1 AND users.age > 18
//! ```
//!
//! # Union Handling
//!
//! When the input plan is a UNION, the filter is pushed into each branch
//! individually, allowing branch-specific column mapping.
//!
//! # Branch Alias Rewriting
//!
//! UNION branches often have aliased node names (e.g., `o_0`, `o_1` instead of `o`)
//! to avoid conflicts. When pushing filters into branches, we detect and rewrite
//! the predicates to use branch-specific aliases.

use std::collections::HashMap;
use std::sync::Arc;

use crate::{
    open_cypher_parser::ast::WhereClause,
    query_planner::{
        logical_expr::LogicalExpr,
        logical_plan::{errors::LogicalPlanError, Filter, LogicalPlan, Union},
    },
};

pub fn evaluate_where_clause<'a>(
    where_clause: &WhereClause<'a>,
    plan: Arc<LogicalPlan>,
) -> Result<Arc<LogicalPlan>, LogicalPlanError> {
    let predicates: LogicalExpr =
        LogicalExpr::try_from(where_clause.conditions.clone()).map_err(|e| {
            LogicalPlanError::QueryPlanningError(format!(
                "Failed to convert WHERE clause expression: {}",
                e
            ))
        })?;
    log::debug!(
        "evaluate_where_clause: WHERE predicate after conversion: {:?}",
        predicates
    );

    // If input is a Union, push Filter into each branch
    // Each branch needs its own copy of the filter, with aliases rewritten to match branch aliases
    if let LogicalPlan::Union(union) = plan.as_ref() {
        let filtered_branches: Vec<Arc<LogicalPlan>> = union
            .inputs
            .iter()
            .map(|branch| {
                // Extract alias mappings from this branch (e.g., "o" -> "o_0")
                let alias_mappings = extract_branch_alias_mappings(branch.as_ref());
                
                // Rewrite the predicate to use branch-specific aliases
                let branch_predicate = if alias_mappings.is_empty() {
                    predicates.clone()
                } else {
                    log::debug!(
                        "WHERE clause: Rewriting filter aliases for UNION branch: {:?}",
                        alias_mappings
                    );
                    rewrite_predicate_aliases(&predicates, &alias_mappings)
                };
                
                Arc::new(LogicalPlan::Filter(Filter {
                    input: branch.clone(),
                    predicate: branch_predicate,
                }))
            })
            .collect();

        return Ok(Arc::new(LogicalPlan::Union(Union {
            inputs: filtered_branches,
            union_type: union.union_type.clone(),
        })));
    }

    Ok(Arc::new(LogicalPlan::Filter(Filter {
        input: plan,
        predicate: predicates,
    })))
}

/// Extract alias mappings from a UNION branch.
/// 
/// UNION branches created for untyped patterns (e.g., `(a)--(o)`) use suffixed aliases
/// like `o_0`, `o_1` to avoid conflicts. This function detects these and returns
/// a mapping from base alias → branch alias.
/// 
/// Example: If branch contains `GraphNode { alias: "o_0", ... }`, returns `{"o" -> "o_0"}`
fn extract_branch_alias_mappings(plan: &LogicalPlan) -> HashMap<String, String> {
    let mut mappings = HashMap::new();
    collect_branch_aliases(plan, &mut mappings);
    mappings
}

fn collect_branch_aliases(plan: &LogicalPlan, mappings: &mut HashMap<String, String>) {
    match plan {
        LogicalPlan::GraphNode(gn) => {
            // Check if alias has branch suffix (e.g., "o_0")
            if let Some(base_alias) = extract_base_alias(&gn.alias) {
                mappings.insert(base_alias, gn.alias.clone());
            }
            collect_branch_aliases(&gn.input, mappings);
        }
        LogicalPlan::GraphRel(gr) => {
            // Check relationship alias
            if let Some(base_alias) = extract_base_alias(&gr.alias) {
                mappings.insert(base_alias, gr.alias.clone());
            }
            // Check left/right connection aliases
            if let Some(base_alias) = extract_base_alias(&gr.left_connection) {
                mappings.insert(base_alias, gr.left_connection.clone());
            }
            if let Some(base_alias) = extract_base_alias(&gr.right_connection) {
                mappings.insert(base_alias, gr.right_connection.clone());
            }
            collect_branch_aliases(&gr.left, mappings);
            collect_branch_aliases(&gr.center, mappings);
            collect_branch_aliases(&gr.right, mappings);
        }
        LogicalPlan::Filter(f) => collect_branch_aliases(&f.input, mappings),
        LogicalPlan::Projection(p) => collect_branch_aliases(&p.input, mappings),
        LogicalPlan::Union(_u) => {
            // Don't recurse into nested unions - they have their own branch handling
        }
        LogicalPlan::CartesianProduct(cp) => {
            collect_branch_aliases(&cp.left, mappings);
            collect_branch_aliases(&cp.right, mappings);
        }
        _ => {}
    }
}

/// Extract base alias from a branch-suffixed alias.
/// 
/// Returns Some(base) if alias matches pattern `{base}_{digit}`, else None.
/// Example: "o_0" → Some("o"), "o_10" → Some("o"), "user" → None
fn extract_base_alias(alias: &str) -> Option<String> {
    // Find last underscore
    if let Some(underscore_pos) = alias.rfind('_') {
        let suffix = &alias[underscore_pos + 1..];
        // Check if suffix is all digits (branch index)
        if !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit()) {
            let base = &alias[..underscore_pos];
            // Avoid treating things like "user_id" as branch aliases
            // by requiring the base to be short (1-3 chars, typical for a, b, o, r, etc.)
            // or checking that this looks like a generated suffix
            if !base.is_empty() && base.len() <= 10 {
                return Some(base.to_string());
            }
        }
    }
    None
}

/// Rewrite a predicate's aliases using the provided mapping.
/// 
/// For predicates like `o.post_id = '3'`, rewrites `o` → `o_0` based on mappings.
fn rewrite_predicate_aliases(expr: &LogicalExpr, mappings: &HashMap<String, String>) -> LogicalExpr {
    use crate::query_planner::logical_expr::{
        OperatorApplication, PropertyAccess, ScalarFnCall, TableAlias, AggregateFnCall, LogicalCase,
    };
    
    match expr {
        LogicalExpr::PropertyAccessExp(pa) => {
            // Rewrite table_alias if it's in the mapping
            if let Some(new_alias) = mappings.get(&pa.table_alias.0) {
                LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(new_alias.clone()),
                    column: pa.column.clone(),
                })
            } else {
                expr.clone()
            }
        }
        LogicalExpr::TableAlias(ta) => {
            // Rewrite standalone table alias
            if let Some(new_alias) = mappings.get(&ta.0) {
                LogicalExpr::TableAlias(TableAlias(new_alias.clone()))
            } else {
                expr.clone()
            }
        }
        LogicalExpr::OperatorApplicationExp(op) | LogicalExpr::Operator(op) => {
            // Recursively rewrite operands
            let rewritten = OperatorApplication {
                operator: op.operator.clone(),
                operands: op.operands.iter()
                    .map(|o| rewrite_predicate_aliases(o, mappings))
                    .collect(),
            };
            // Preserve the original variant
            if matches!(expr, LogicalExpr::OperatorApplicationExp(_)) {
                LogicalExpr::OperatorApplicationExp(rewritten)
            } else {
                LogicalExpr::Operator(rewritten)
            }
        }
        LogicalExpr::ScalarFnCall(fc) => {
            // Recursively rewrite function arguments
            LogicalExpr::ScalarFnCall(ScalarFnCall {
                name: fc.name.clone(),
                args: fc.args.iter()
                    .map(|a| rewrite_predicate_aliases(a, mappings))
                    .collect(),
            })
        }
        LogicalExpr::AggregateFnCall(afc) => {
            LogicalExpr::AggregateFnCall(AggregateFnCall {
                name: afc.name.clone(),
                args: afc.args.iter()
                    .map(|a| rewrite_predicate_aliases(a, mappings))
                    .collect(),
            })
        }
        LogicalExpr::Case(case) => {
            LogicalExpr::Case(LogicalCase {
                expr: case.expr.as_ref().map(|e| Box::new(rewrite_predicate_aliases(e, mappings))),
                when_then: case.when_then.iter()
                    .map(|(w, t)| (rewrite_predicate_aliases(w, mappings), rewrite_predicate_aliases(t, mappings)))
                    .collect(),
                else_expr: case.else_expr.as_ref().map(|e| Box::new(rewrite_predicate_aliases(e, mappings))),
            })
        }
        LogicalExpr::List(items) => {
            LogicalExpr::List(items.iter()
                .map(|i| rewrite_predicate_aliases(i, mappings))
                .collect())
        }
        // Literals and other expressions don't contain aliases to rewrite
        _ => expr.clone(),
    }
}
