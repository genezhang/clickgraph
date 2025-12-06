//! Optimizer pass that extracts cross-pattern filters from Filter nodes above CartesianProduct
//! and moves them into the CartesianProduct's join_condition field.
//!
//! This enables generation of proper JOIN ... ON clauses instead of CROSS JOIN + WHERE.
//!
//! Example transformation:
//! ```text
//! Filter(ip1.ip = ip2.ip)
//!   └── CartesianProduct(left: ..., right: ...)
//! ```
//! becomes:
//! ```text
//! CartesianProduct(left: ..., right: ..., join_condition: Some(ip1.ip = ip2.ip))
//! ```

use std::collections::HashSet;
use std::sync::Arc;

use crate::query_planner::{
    logical_expr::LogicalExpr,
    logical_plan::{CartesianProduct, Filter, LogicalPlan},
    optimizer::optimizer_pass::{OptimizerPass, OptimizerResult},
    plan_ctx::PlanCtx,
    transformed::Transformed,
};

/// Optimizer pass that extracts cross-pattern filters into CartesianProduct join conditions
pub struct CartesianJoinExtraction;

impl OptimizerPass for CartesianJoinExtraction {
    fn optimize(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
    ) -> OptimizerResult<Transformed<Arc<LogicalPlan>>> {
        let transformed_plan = match logical_plan.as_ref() {
            // When we find a Filter above a CartesianProduct, check if the predicate
            // references aliases from both sides - if so, it's a join condition
            LogicalPlan::Filter(filter) => {
                // First, recursively optimize the child
                let child_tf = self.optimize(filter.input.clone(), plan_ctx)?;
                let child_plan = match &child_tf {
                    Transformed::Yes(p) | Transformed::No(p) => p.clone(),
                };

                // Check if the child is a CartesianProduct
                if let LogicalPlan::CartesianProduct(cp) = child_plan.as_ref() {
                    // Get aliases from left and right sides
                    let left_aliases = collect_aliases_from_plan(&cp.left);
                    let right_aliases = collect_aliases_from_plan(&cp.right);
                    
                    eprintln!("CartesianJoinExtraction: left_aliases = {:?}", left_aliases);
                    eprintln!("CartesianJoinExtraction: right_aliases = {:?}", right_aliases);
                    
                    // Check if the filter predicate references both sides
                    let filter_aliases = collect_aliases_from_expr(&filter.predicate);
                    eprintln!("CartesianJoinExtraction: filter_aliases = {:?}", filter_aliases);
                    
                    let refs_left = filter_aliases.iter().any(|a| left_aliases.contains(a));
                    let refs_right = filter_aliases.iter().any(|a| right_aliases.contains(a));
                    
                    eprintln!("CartesianJoinExtraction: refs_left={}, refs_right={}", refs_left, refs_right);
                    
                    if refs_left && refs_right {
                        // This filter bridges both sides - extract it as a join condition
                        // and potentially split if there are multiple conditions
                        let (join_conditions, remaining_filters) = 
                            partition_filter_conditions(&filter.predicate, &left_aliases, &right_aliases);
                        
                        eprintln!("CartesianJoinExtraction: Extracted {} join conditions, {} remaining filters",
                            join_conditions.as_ref().map(|_| 1).unwrap_or(0),
                            remaining_filters.as_ref().map(|_| 1).unwrap_or(0));
                        
                        // Create new CartesianProduct with join_condition
                        let new_cp = CartesianProduct {
                            left: cp.left.clone(),
                            right: cp.right.clone(),
                            is_optional: cp.is_optional,
                            join_condition: join_conditions.or_else(|| cp.join_condition.clone()),
                        };
                        
                        let new_cp_plan = Arc::new(LogicalPlan::CartesianProduct(new_cp));
                        
                        // If there are remaining filters, wrap the CartesianProduct
                        if let Some(remaining) = remaining_filters {
                            Transformed::Yes(Arc::new(LogicalPlan::Filter(Filter {
                                input: new_cp_plan,
                                predicate: remaining,
                            })))
                        } else {
                            // No remaining filters - just the CartesianProduct
                            Transformed::Yes(new_cp_plan)
                        }
                    } else {
                        // Filter doesn't bridge both sides, keep it as-is
                        match child_tf {
                            Transformed::Yes(new_child) => {
                                Transformed::Yes(Arc::new(LogicalPlan::Filter(Filter {
                                    input: new_child,
                                    predicate: filter.predicate.clone(),
                                })))
                            }
                            Transformed::No(_) => Transformed::No(logical_plan.clone()),
                        }
                    }
                } else {
                    // Child is not a CartesianProduct, just pass through the transformation
                    match child_tf {
                        Transformed::Yes(new_child) => {
                            Transformed::Yes(Arc::new(LogicalPlan::Filter(Filter {
                                input: new_child,
                                predicate: filter.predicate.clone(),
                            })))
                        }
                        Transformed::No(_) => Transformed::No(logical_plan.clone()),
                    }
                }
            }
            
            // Recursively process other node types
            LogicalPlan::Projection(projection) => {
                let child_tf = self.optimize(projection.input.clone(), plan_ctx)?;
                match child_tf {
                    Transformed::Yes(new_child) => {
                        let mut new_proj = projection.clone();
                        new_proj.input = new_child;
                        Transformed::Yes(Arc::new(LogicalPlan::Projection(new_proj)))
                    }
                    Transformed::No(_) => Transformed::No(logical_plan.clone()),
                }
            }
            
            LogicalPlan::Limit(limit) => {
                let child_tf = self.optimize(limit.input.clone(), plan_ctx)?;
                match child_tf {
                    Transformed::Yes(new_child) => {
                        let mut new_limit = limit.clone();
                        new_limit.input = new_child;
                        Transformed::Yes(Arc::new(LogicalPlan::Limit(new_limit)))
                    }
                    Transformed::No(_) => Transformed::No(logical_plan.clone()),
                }
            }
            
            LogicalPlan::Skip(skip) => {
                let child_tf = self.optimize(skip.input.clone(), plan_ctx)?;
                match child_tf {
                    Transformed::Yes(new_child) => {
                        let mut new_skip = skip.clone();
                        new_skip.input = new_child;
                        Transformed::Yes(Arc::new(LogicalPlan::Skip(new_skip)))
                    }
                    Transformed::No(_) => Transformed::No(logical_plan.clone()),
                }
            }
            
            LogicalPlan::OrderBy(order_by) => {
                let child_tf = self.optimize(order_by.input.clone(), plan_ctx)?;
                match child_tf {
                    Transformed::Yes(new_child) => {
                        let mut new_ob = order_by.clone();
                        new_ob.input = new_child;
                        Transformed::Yes(Arc::new(LogicalPlan::OrderBy(new_ob)))
                    }
                    Transformed::No(_) => Transformed::No(logical_plan.clone()),
                }
            }
            
            LogicalPlan::GroupBy(group_by) => {
                let child_tf = self.optimize(group_by.input.clone(), plan_ctx)?;
                match child_tf {
                    Transformed::Yes(new_child) => {
                        let mut new_gb = group_by.clone();
                        new_gb.input = new_child;
                        Transformed::Yes(Arc::new(LogicalPlan::GroupBy(new_gb)))
                    }
                    Transformed::No(_) => Transformed::No(logical_plan.clone()),
                }
            }
            
            LogicalPlan::GraphJoins(gj) => {
                let child_tf = self.optimize(gj.input.clone(), plan_ctx)?;
                match child_tf {
                    Transformed::Yes(new_child) => {
                        let mut new_gj = gj.clone();
                        new_gj.input = new_child;
                        Transformed::Yes(Arc::new(LogicalPlan::GraphJoins(new_gj)))
                    }
                    Transformed::No(_) => Transformed::No(logical_plan.clone()),
                }
            }
            
            LogicalPlan::Cte(cte) => {
                let child_tf = self.optimize(cte.input.clone(), plan_ctx)?;
                match child_tf {
                    Transformed::Yes(new_child) => {
                        let mut new_cte = cte.clone();
                        new_cte.input = new_child;
                        Transformed::Yes(Arc::new(LogicalPlan::Cte(new_cte)))
                    }
                    Transformed::No(_) => Transformed::No(logical_plan.clone()),
                }
            }
            
            LogicalPlan::GraphRel(gr) => {
                // GraphRel has left, center, right children
                let left_tf = self.optimize(gr.left.clone(), plan_ctx)?;
                let right_tf = self.optimize(gr.right.clone(), plan_ctx)?;
                let center_tf = self.optimize(gr.center.clone(), plan_ctx)?;
                
                match (&left_tf, &center_tf, &right_tf) {
                    (Transformed::No(_), Transformed::No(_), Transformed::No(_)) => {
                        Transformed::No(logical_plan.clone())
                    }
                    _ => {
                        let mut new_gr = gr.clone();
                        new_gr.left = match left_tf {
                            Transformed::Yes(p) => p,
                            Transformed::No(p) => p,
                        };
                        new_gr.center = match center_tf {
                            Transformed::Yes(p) => p,
                            Transformed::No(p) => p,
                        };
                        new_gr.right = match right_tf {
                            Transformed::Yes(p) => p,
                            Transformed::No(p) => p,
                        };
                        Transformed::Yes(Arc::new(LogicalPlan::GraphRel(new_gr)))
                    }
                }
            }
            
            LogicalPlan::GraphNode(gn) => {
                let child_tf = self.optimize(gn.input.clone(), plan_ctx)?;
                match child_tf {
                    Transformed::Yes(new_child) => {
                        let mut new_gn = gn.clone();
                        new_gn.input = new_child;
                        Transformed::Yes(Arc::new(LogicalPlan::GraphNode(new_gn)))
                    }
                    Transformed::No(_) => Transformed::No(logical_plan.clone()),
                }
            }
            
            LogicalPlan::CartesianProduct(cp) => {
                let left_tf = self.optimize(cp.left.clone(), plan_ctx)?;
                let right_tf = self.optimize(cp.right.clone(), plan_ctx)?;
                
                match (&left_tf, &right_tf) {
                    (Transformed::No(_), Transformed::No(_)) => Transformed::No(logical_plan.clone()),
                    _ => {
                        let new_cp = CartesianProduct {
                            left: match left_tf {
                                Transformed::Yes(p) => p,
                                Transformed::No(p) => p,
                            },
                            right: match right_tf {
                                Transformed::Yes(p) => p,
                                Transformed::No(p) => p,
                            },
                            is_optional: cp.is_optional,
                            join_condition: cp.join_condition.clone(),
                        };
                        Transformed::Yes(Arc::new(LogicalPlan::CartesianProduct(new_cp)))
                    }
                }
            }
            
            LogicalPlan::Union(union) => {
                let mut any_transformed = false;
                let mut new_inputs = Vec::new();
                
                for input in &union.inputs {
                    let tf = self.optimize(input.clone(), plan_ctx)?;
                    match tf {
                        Transformed::Yes(p) => {
                            any_transformed = true;
                            new_inputs.push(p);
                        }
                        Transformed::No(p) => {
                            new_inputs.push(p);
                        }
                    }
                }
                
                if any_transformed {
                    Transformed::Yes(Arc::new(LogicalPlan::Union(
                        crate::query_planner::logical_plan::Union { 
                            inputs: new_inputs,
                            union_type: union.union_type.clone(),
                        }
                    )))
                } else {
                    Transformed::No(logical_plan.clone())
                }
            }
            
            LogicalPlan::Unwind(u) => {
                let child_tf = self.optimize(u.input.clone(), plan_ctx)?;
                match child_tf {
                    Transformed::Yes(new_child) => {
                        let mut new_u = u.clone();
                        new_u.input = new_child;
                        Transformed::Yes(Arc::new(LogicalPlan::Unwind(new_u)))
                    }
                    Transformed::No(_) => Transformed::No(logical_plan.clone()),
                }
            }
            
            // Leaf nodes - no transformation
            LogicalPlan::Empty
            | LogicalPlan::Scan(_)
            | LogicalPlan::ViewScan(_)
            | LogicalPlan::PageRank(_) => Transformed::No(logical_plan.clone()),
        };
        
        Ok(transformed_plan)
    }
}

impl CartesianJoinExtraction {
    pub fn new() -> Self {
        CartesianJoinExtraction
    }
}

/// Collect all table aliases referenced in a logical expression
fn collect_aliases_from_expr(expr: &LogicalExpr) -> HashSet<String> {
    let mut aliases = HashSet::new();
    collect_aliases_from_expr_inner(expr, &mut aliases);
    aliases
}

fn collect_aliases_from_expr_inner(expr: &LogicalExpr, aliases: &mut HashSet<String>) {
    match expr {
        LogicalExpr::PropertyAccessExp(prop) => {
            aliases.insert(prop.table_alias.0.clone());
        }
        LogicalExpr::OperatorApplicationExp(op) => {
            for operand in &op.operands {
                collect_aliases_from_expr_inner(operand, aliases);
            }
        }
        LogicalExpr::ScalarFnCall(func) => {
            for arg in &func.args {
                collect_aliases_from_expr_inner(arg, aliases);
            }
        }
        LogicalExpr::AggregateFnCall(agg) => {
            for arg in &agg.args {
                collect_aliases_from_expr_inner(arg, aliases);
            }
        }
        LogicalExpr::Case(case) => {
            if let Some(e) = &case.expr {
                collect_aliases_from_expr_inner(e, aliases);
            }
            for (when, then) in &case.when_then {
                collect_aliases_from_expr_inner(when, aliases);
                collect_aliases_from_expr_inner(then, aliases);
            }
            if let Some(else_e) = &case.else_expr {
                collect_aliases_from_expr_inner(else_e, aliases);
            }
        }
        LogicalExpr::List(list) => {
            for item in list {
                collect_aliases_from_expr_inner(item, aliases);
            }
        }
        LogicalExpr::InSubquery(in_sub) => {
            collect_aliases_from_expr_inner(&in_sub.expr, aliases);
        }
        LogicalExpr::TableAlias(ta) => {
            aliases.insert(ta.0.clone());
        }
        // Other types don't reference table aliases
        _ => {}
    }
}

/// Collect all node aliases defined in a logical plan
fn collect_aliases_from_plan(plan: &Arc<LogicalPlan>) -> HashSet<String> {
    let mut aliases = HashSet::new();
    collect_aliases_from_plan_inner(plan.as_ref(), &mut aliases);
    aliases
}

fn collect_aliases_from_plan_inner(plan: &LogicalPlan, aliases: &mut HashSet<String>) {
    match plan {
        LogicalPlan::GraphNode(gn) => {
            aliases.insert(gn.alias.clone());
            collect_aliases_from_plan_inner(&gn.input, aliases);
        }
        LogicalPlan::GraphRel(gr) => {
            aliases.insert(gr.alias.clone());
            aliases.insert(gr.left_connection.clone());
            aliases.insert(gr.right_connection.clone());
            collect_aliases_from_plan_inner(&gr.left, aliases);
            collect_aliases_from_plan_inner(&gr.center, aliases);
            collect_aliases_from_plan_inner(&gr.right, aliases);
        }
        LogicalPlan::Projection(proj) => {
            // Collect aliases from projection items:
            // 1. TableAlias expressions (e.g., RETURN a)
            // 2. col_alias (e.g., WITH src.ip AS source_ip)
            for item in &proj.items {
                if let LogicalExpr::TableAlias(ta) = &item.expression {
                    aliases.insert(ta.0.clone());
                }
                // CRITICAL: Also collect column aliases defined in WITH clauses
                // These are the aliases that can be referenced in subsequent MATCH clauses
                if let Some(col_alias) = &item.col_alias {
                    aliases.insert(col_alias.0.clone());
                }
            }
            collect_aliases_from_plan_inner(&proj.input, aliases);
        }
        LogicalPlan::Filter(f) => {
            collect_aliases_from_plan_inner(&f.input, aliases);
        }
        LogicalPlan::Limit(l) => {
            collect_aliases_from_plan_inner(&l.input, aliases);
        }
        LogicalPlan::Skip(s) => {
            collect_aliases_from_plan_inner(&s.input, aliases);
        }
        LogicalPlan::OrderBy(o) => {
            collect_aliases_from_plan_inner(&o.input, aliases);
        }
        LogicalPlan::GroupBy(g) => {
            collect_aliases_from_plan_inner(&g.input, aliases);
        }
        LogicalPlan::Cte(c) => {
            collect_aliases_from_plan_inner(&c.input, aliases);
        }
        LogicalPlan::GraphJoins(gj) => {
            collect_aliases_from_plan_inner(&gj.input, aliases);
        }
        LogicalPlan::CartesianProduct(cp) => {
            collect_aliases_from_plan_inner(&cp.left, aliases);
            collect_aliases_from_plan_inner(&cp.right, aliases);
        }
        LogicalPlan::Union(u) => {
            for input in &u.inputs {
                collect_aliases_from_plan_inner(input, aliases);
            }
        }
        LogicalPlan::Unwind(u) => {
            collect_aliases_from_plan_inner(&u.input, aliases);
        }
        LogicalPlan::ViewScan(vs) => {
            // ViewScans may have an alias in the context
            if let Some(alias) = &vs.input {
                collect_aliases_from_plan_inner(alias, aliases);
            }
        }
        // Leaf nodes without aliases
        LogicalPlan::Empty | LogicalPlan::Scan(_) | LogicalPlan::PageRank(_) => {}
    }
}

/// Partition filter conditions into join conditions (those that bridge left and right)
/// and remaining filters (those that don't).
/// 
/// For now, we take the simple approach: if the entire expression references both sides,
/// it becomes the join condition. A more sophisticated implementation would split AND
/// conditions.
fn partition_filter_conditions(
    predicate: &LogicalExpr,
    left_aliases: &HashSet<String>,
    right_aliases: &HashSet<String>,
) -> (Option<LogicalExpr>, Option<LogicalExpr>) {
    // Simple case: just check if the whole predicate is a cross-pattern condition
    let pred_aliases = collect_aliases_from_expr(predicate);
    
    let refs_left = pred_aliases.iter().any(|a| left_aliases.contains(a));
    let refs_right = pred_aliases.iter().any(|a| right_aliases.contains(a));
    
    if refs_left && refs_right {
        // This is a join condition
        // TODO: For AND expressions, we could split into join conditions and remaining filters
        (Some(predicate.clone()), None)
    } else {
        // Not a join condition - keep as regular filter
        (None, Some(predicate.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_collect_aliases_from_expr() {
        use crate::query_planner::logical_expr::{PropertyAccess, TableAlias, OperatorApplication, Operator};
        use crate::graph_catalog::expression_parser::PropertyValue;
        
        // ip1.ip = ip2.ip
        let expr = LogicalExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("ip1".to_string()),
                    column: PropertyValue::Column("ip".to_string()),
                }),
                LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("ip2".to_string()),
                    column: PropertyValue::Column("ip".to_string()),
                }),
            ],
        });
        
        let aliases = collect_aliases_from_expr(&expr);
        assert!(aliases.contains("ip1"));
        assert!(aliases.contains("ip2"));
        assert_eq!(aliases.len(), 2);
    }
}
