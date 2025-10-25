use std::sync::Arc;

use crate::query_planner::{
    logical_expr::{LogicalExpr, PropertyAccess, Column, TableAlias},
    logical_plan::{GraphRel, LogicalPlan, Projection},
    optimizer::optimizer_pass::{OptimizerPass, OptimizerResult},
    plan_ctx::PlanCtx,
    transformed::Transformed,
};

/// Helper function to qualify Column expressions with a table alias
/// Converts Column("name") to PropertyAccessExp(a.name)
fn qualify_columns_with_alias(expr: LogicalExpr, alias: &str) -> LogicalExpr {
    match expr {
        LogicalExpr::Column(col) => {
            LogicalExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(alias.to_string()),
                column: col,
            })
        }
        LogicalExpr::OperatorApplicationExp(mut op) => {
            op.operands = op.operands
                .into_iter()
                .map(|operand| qualify_columns_with_alias(operand, alias))
                .collect();
            LogicalExpr::OperatorApplicationExp(op)
        }
        // For other expression types, recurse into sub-expressions
        other => other,
    }
}

/// Optimizer pass that pushes Filter predicates into GraphRel.where_predicate
/// 
/// This pass looks for patterns like:
///   Filter -> ... -> GraphRel
/// 
/// And moves the filter predicate into the GraphRel's where_predicate field,
/// so it can be properly categorized during CTE generation (start node filters
/// in base case, end node filters in final SELECT).
pub struct FilterIntoGraphRel;

impl OptimizerPass for FilterIntoGraphRel {
    fn optimize(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
    ) -> OptimizerResult<Transformed<Arc<LogicalPlan>>> {
        let transformed_plan = match logical_plan.as_ref() {
            // When we find a Filter node, check if it wraps a GraphRel (possibly through Projection)
            LogicalPlan::Filter(filter) => {
                log::trace!("FilterIntoGraphRel: Found Filter node with predicate: {:?}", filter.predicate);                // First, recursively optimize the child
                let child_tf = self.optimize(filter.input.clone(), plan_ctx)?;
                
                // Check if the child is a GraphRel (either transformed or not)
                let child_plan = match &child_tf {
                    Transformed::Yes(p) | Transformed::No(p) => p.clone(),
                };
                
                // Check if child is Projection wrapping GraphRel
                if let LogicalPlan::Projection(proj) = child_plan.as_ref() {
                    if let LogicalPlan::GraphRel(graph_rel) = proj.input.as_ref() {
                        // Create new GraphRel with filter
                        let new_graph_rel = Arc::new(LogicalPlan::GraphRel(GraphRel {
                            left: graph_rel.left.clone(),
                            center: graph_rel.center.clone(),
                            right: graph_rel.right.clone(),
                            alias: graph_rel.alias.clone(),
                            direction: graph_rel.direction.clone(),
                            left_connection: graph_rel.left_connection.clone(),
                            right_connection: graph_rel.right_connection.clone(),
                            is_rel_anchor: graph_rel.is_rel_anchor,
                            variable_length: graph_rel.variable_length.clone(),
                            shortest_path_mode: graph_rel.shortest_path_mode.clone(),
                            path_variable: graph_rel.path_variable.clone(),
                            where_predicate: Some(if let Some(existing) = &graph_rel.where_predicate {
                                use crate::query_planner::logical_expr::{LogicalExpr, Operator, OperatorApplication};
                                LogicalExpr::OperatorApplicationExp(OperatorApplication {
                                    operator: Operator::And,
                                    operands: vec![existing.clone(), filter.predicate.clone()],
                                })
                            } else {
                                filter.predicate.clone()
                            }),
                            labels: graph_rel.labels.clone(),
                        }));
                        
                        // Rebuild projection with new GraphRel, and return without Filter wrapper
                        let new_proj = Arc::new(LogicalPlan::Projection(crate::query_planner::logical_plan::Projection {
                            input: new_graph_rel,
                            items: proj.items.clone(),
                        }));
                        
                        return Ok(Transformed::Yes(new_proj));
                    }
                }
                
                // Direct Filter → GraphRel case
                if let LogicalPlan::GraphRel(graph_rel) = child_plan.as_ref() {
                    // Push the filter predicate into the GraphRel
                    let new_graph_rel = Arc::new(LogicalPlan::GraphRel(GraphRel {
                        left: graph_rel.left.clone(),
                        center: graph_rel.center.clone(),
                        right: graph_rel.right.clone(),
                        alias: graph_rel.alias.clone(),
                        direction: graph_rel.direction.clone(),
                        left_connection: graph_rel.left_connection.clone(),
                        right_connection: graph_rel.right_connection.clone(),
                        is_rel_anchor: graph_rel.is_rel_anchor,
                        variable_length: graph_rel.variable_length.clone(),
                        shortest_path_mode: graph_rel.shortest_path_mode.clone(),
                        path_variable: graph_rel.path_variable.clone(),
                        // Merge predicates if GraphRel already has one
                        where_predicate: Some(if let Some(existing) = &graph_rel.where_predicate {
                            // Combine with AND
                            use crate::query_planner::logical_expr::{LogicalExpr, Operator, OperatorApplication};
                            LogicalExpr::OperatorApplicationExp(OperatorApplication {
                                operator: Operator::And,
                                operands: vec![existing.clone(), filter.predicate.clone()],
                            })
                        } else {
                            filter.predicate.clone()
                        }),
                        labels: graph_rel.labels.clone(),
                    }));
                    
                    // Return the GraphRel directly, removing the Filter wrapper
                    return Ok(Transformed::Yes(new_graph_rel));
                }
                
                // If child is not GraphRel, rebuild with optimized child
                filter.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            
            // For all other node types, recurse through children
            LogicalPlan::GraphNode(graph_node) => {
                let child_tf = self.optimize(graph_node.input.clone(), plan_ctx)?;
                graph_node.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Projection(proj) => {
                let child_tf = self.optimize(proj.input.clone(), plan_ctx)?;
                match child_tf {
                    Transformed::Yes(new_input) => {
                        let new_proj = LogicalPlan::Projection(Projection {
                            input: new_input,
                            items: proj.items.clone(),
                        });
                        Transformed::Yes(Arc::new(new_proj))
                    }
                    Transformed::No(_) => Transformed::No(logical_plan.clone()),
                }
            }
            LogicalPlan::GraphRel(graph_rel) => {
                // Skip if already has filters injected
                if graph_rel.where_predicate.is_some() {
                    log::debug!("FilterIntoGraphRel: GraphRel already has where_predicate, skipping");
                    return Ok(Transformed::No(logical_plan.clone()));
                }
                
                println!("FilterIntoGraphRel: Processing GraphRel with left_connection='{}', right_connection='{}'",
                         graph_rel.left_connection, graph_rel.right_connection);
                // Extract filters from plan_ctx for this GraphRel's aliases
                let mut combined_filters: Vec<LogicalExpr> = vec![];                // Check left connection for filters
                if let Ok(table_ctx) = plan_ctx.get_table_ctx_from_alias_opt(&Some(graph_rel.left_connection.clone())) {
                    let filters = table_ctx.get_filters().clone();
                    if !filters.is_empty() {
                        log::debug!("FilterIntoGraphRel: Found {} filters for left connection alias '{}' in GraphRel", 
                                   filters.len(), graph_rel.left_connection);
                        log::trace!("FilterIntoGraphRel: Left alias filters: {:?}", filters);
                        // Qualify filters with the left alias
                        let qualified_filters: Vec<LogicalExpr> = filters.into_iter()
                            .map(|f| qualify_columns_with_alias(f, &graph_rel.left_connection))
                            .collect();
                        combined_filters.extend(qualified_filters);
                    }
                }
                
                // Check right connection for filters
                if let Ok(table_ctx) = plan_ctx.get_table_ctx_from_alias_opt(&Some(graph_rel.right_connection.clone())) {
                    let filters = table_ctx.get_filters().clone();
                    if !filters.is_empty() {
                        println!("FilterIntoGraphRel: Found {} filters for right connection alias '{}' in GraphRel", 
                                   filters.len(), graph_rel.right_connection);
                        println!("FilterIntoGraphRel: Right alias filters: {:?}", filters);
                        // Qualify filters with the right alias
                        let qualified_filters: Vec<LogicalExpr> = filters.into_iter()
                            .map(|f| qualify_columns_with_alias(f, &graph_rel.right_connection))
                            .collect();
                        combined_filters.extend(qualified_filters);
                    }
                }
                
                // If we found filters, create new GraphRel with them
                if !combined_filters.is_empty() {
                    use crate::query_planner::logical_expr::{Operator, OperatorApplication};
                    
                    // Combine all filters with AND
                    let combined_predicate = combined_filters.into_iter().reduce(|acc, filter| {
                        LogicalExpr::OperatorApplicationExp(OperatorApplication {
                            operator: Operator::And,
                            operands: vec![acc, filter],
                        })
                    });
                    
                    if let Some(predicate) = combined_predicate {
                        println!("FilterIntoGraphRel: Injecting combined filter into GraphRel.where_predicate");
                        println!("FilterIntoGraphRel: Combined predicate: {:?}", predicate);
                        
                        let new_graph_rel = Arc::new(LogicalPlan::GraphRel(GraphRel {
                            left: graph_rel.left.clone(),
                            center: graph_rel.center.clone(),
                            right: graph_rel.right.clone(),
                            alias: graph_rel.alias.clone(),
                            direction: graph_rel.direction.clone(),
                            left_connection: graph_rel.left_connection.clone(),
                            right_connection: graph_rel.right_connection.clone(),
                            is_rel_anchor: graph_rel.is_rel_anchor,
                            variable_length: graph_rel.variable_length.clone(),
                            shortest_path_mode: graph_rel.shortest_path_mode.clone(),
                            path_variable: graph_rel.path_variable.clone(),
                            where_predicate: Some(predicate),
                            labels: graph_rel.labels.clone(),
                        }));
                        
                        return Ok(Transformed::Yes(new_graph_rel));
                    }
                }
                
                // No filters found, process children normally
                let left_tf = self.optimize(graph_rel.left.clone(), plan_ctx)?;
                let center_tf = self.optimize(graph_rel.center.clone(), plan_ctx)?;
                let right_tf = self.optimize(graph_rel.right.clone(), plan_ctx)?;
                graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone())
            }
            LogicalPlan::Cte(cte) => {
                let child_tf = self.optimize(cte.input.clone(), plan_ctx)?;
                cte.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Scan(_) => Transformed::No(logical_plan.clone()),
            LogicalPlan::ViewScan(_) => Transformed::No(logical_plan.clone()),
            LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
            LogicalPlan::GraphJoins(graph_joins) => {
                let child_tf = self.optimize(graph_joins.input.clone(), plan_ctx)?;
                graph_joins.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Projection(projection) => {
                // Check if projection wraps a Filter that wraps GraphRel
                if let LogicalPlan::Filter(filter) = projection.input.as_ref() {
                    // Recursively process the filter's input
                    let filter_input_tf = self.optimize(filter.input.clone(), plan_ctx)?;
                    let filter_input_plan = match &filter_input_tf {
                        Transformed::Yes(p) | Transformed::No(p) => p.clone(),
                    };
                    
                    // Check if filter wraps GraphRel
                    if let LogicalPlan::GraphRel(graph_rel) = filter_input_plan.as_ref() {
                        // Create new GraphRel with filter pushed into it
                        let new_graph_rel = Arc::new(LogicalPlan::GraphRel(GraphRel {
                            left: graph_rel.left.clone(),
                            center: graph_rel.center.clone(),
                            right: graph_rel.right.clone(),
                            alias: graph_rel.alias.clone(),
                            direction: graph_rel.direction.clone(),
                            left_connection: graph_rel.left_connection.clone(),
                            right_connection: graph_rel.right_connection.clone(),
                            is_rel_anchor: graph_rel.is_rel_anchor,
                            variable_length: graph_rel.variable_length.clone(),
                            shortest_path_mode: graph_rel.shortest_path_mode.clone(),
                            path_variable: graph_rel.path_variable.clone(),
                            where_predicate: Some(if let Some(existing) = &graph_rel.where_predicate {
                                use crate::query_planner::logical_expr::{LogicalExpr, Operator, OperatorApplication};
                                LogicalExpr::OperatorApplicationExp(OperatorApplication {
                                    operator: Operator::And,
                                    operands: vec![existing.clone(), filter.predicate.clone()],
                                })
                            } else {
                                filter.predicate.clone()
                            }),
                            labels: graph_rel.labels.clone(),
                        }));
                        
                        // Return Projection with GraphRel (no Filter wrapper)
                        let new_proj = Arc::new(LogicalPlan::Projection(crate::query_planner::logical_plan::Projection {
                            input: new_graph_rel,
                            items: projection.items.clone(),
                        }));
                        
                        log::debug!("FilterIntoGraphRel: Pushed filter from Projection→Filter→GraphRel into GraphRel.where_predicate");
                        return Ok(Transformed::Yes(new_proj));
                    }
                }
                
                // Default: recursively process projection's input
                let child_tf = self.optimize(projection.input.clone(), plan_ctx)?;
                projection.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GroupBy(group_by) => {
                let child_tf = self.optimize(group_by.input.clone(), plan_ctx)?;
                group_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::OrderBy(order_by) => {
                let child_tf = self.optimize(order_by.input.clone(), plan_ctx)?;
                order_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Skip(skip) => {
                let child_tf = self.optimize(skip.input.clone(), plan_ctx)?;
                skip.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Limit(limit) => {
                let child_tf = self.optimize(limit.input.clone(), plan_ctx)?;
                limit.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Union(union) => {
                let mut inputs_tf: Vec<Transformed<Arc<LogicalPlan>>> = vec![];
                for input_plan in union.inputs.iter() {
                    let child_tf = self.optimize(input_plan.clone(), plan_ctx)?;
                    inputs_tf.push(child_tf);
                }
                union.rebuild_or_clone(inputs_tf, logical_plan.clone())
            }
            LogicalPlan::PageRank(_) => Transformed::No(logical_plan.clone()),
        };
        Ok(transformed_plan)
    }
}

impl FilterIntoGraphRel {
    pub fn new() -> Self {
        FilterIntoGraphRel
    }
}
