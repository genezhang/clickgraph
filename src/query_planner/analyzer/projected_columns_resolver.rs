//! Projected Columns Resolver - Pre-computes projected columns for GraphNodes
//!
//! This pass populates the `projected_columns` field in GraphNode, eliminating the need
//! for the renderer to traverse the plan tree at render time to find properties.
//!
//! Example:
//!   Input:  GraphNode { alias: "p", projected_columns: None, input: ViewScan { property_mapping: {"firstName" -> "first_name", "age" -> "age"} } }
//!   Output: GraphNode { alias: "p", projected_columns: Some([("firstName", "p.first_name"), ("age", "p.age")]), ... }
//!
//! This allows the renderer to directly use node.projected_columns instead of calling
//! get_properties_with_table_alias() which requires complex plan traversal.

use std::sync::Arc;

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    query_planner::{
        analyzer::{
            analyzer_pass::{AnalyzerPass, AnalyzerResult},
            errors::{AnalyzerError, Pass},
        },
        logical_plan::{GraphNode, GraphRel, LogicalPlan, ViewScan},
        plan_ctx::PlanCtx,
        transformed::Transformed,
    },
};

pub struct ProjectedColumnsResolver;

impl ProjectedColumnsResolver {
    pub fn new() -> Self {
        Self
    }

    /// Compute projected columns for a GraphNode based on its input (ViewScan)
    fn compute_projected_columns_for_node(
        node: &GraphNode,
    ) -> Option<Vec<(String, String)>> {
        // The input should be a ViewScan (or through Filters, etc.)
        let view_scan = Self::find_view_scan(&node.input)?;

        // Handle denormalized nodes specially
        if view_scan.is_denormalized {
            return Self::compute_denormalized_properties(&node.alias, view_scan);
        }

        // Standard node: property_mapping contains property_name -> db_column
        // We want to return (property_name, qualified_column)
        let mut properties: Vec<(String, String)> = view_scan
            .property_mapping
            .iter()
            .map(|(prop_name, prop_value)| {
                let qualified = format!("{}.{}", node.alias, prop_value.raw());
                (prop_name.clone(), qualified)
            })
            .collect();

        properties.sort_by(|a, b| a.0.cmp(&b.0));
        Some(properties)
    }

    /// Find ViewScan in the plan (might be wrapped in Filters, etc.)
    fn find_view_scan(plan: &Arc<LogicalPlan>) -> Option<&ViewScan> {
        match plan.as_ref() {
            LogicalPlan::ViewScan(scan) => Some(scan),
            LogicalPlan::Filter(filter) => Self::find_view_scan(&filter.input),
            _ => None,
        }
    }

    /// Compute projected columns for denormalized nodes
    fn compute_denormalized_properties(
        alias: &str,
        scan: &ViewScan,
    ) -> Option<Vec<(String, String)>> {
        // For denormalized nodes, properties are in from_node_properties or to_node_properties
        if let Some(from_props) = &scan.from_node_properties {
            let mut properties: Vec<(String, String)> = from_props
                .iter()
                .map(|(prop_name, prop_value)| {
                    let qualified = format!("{}.{}", alias, prop_value.raw());
                    (prop_name.clone(), qualified)
                })
                .collect();
            properties.sort_by(|a, b| a.0.cmp(&b.0));
            if !properties.is_empty() {
                return Some(properties);
            }
        }

        if let Some(to_props) = &scan.to_node_properties {
            let mut properties: Vec<(String, String)> = to_props
                .iter()
                .map(|(prop_name, prop_value)| {
                    let qualified = format!("{}.{}", alias, prop_value.raw());
                    (prop_name.clone(), qualified)
                })
                .collect();
            properties.sort_by(|a, b| a.0.cmp(&b.0));
            if !properties.is_empty() {
                return Some(properties);
            }
        }

        None
    }
}

impl AnalyzerPass for ProjectedColumnsResolver {
    fn analyze_with_graph_schema(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        _graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        log::debug!(
            "🔧 ProjectedColumnsResolver: Processing plan type: {:?}",
            std::mem::discriminant(&*logical_plan)
        );

        Ok(match logical_plan.as_ref() {
            // GraphNode - compute and populate projected_columns
            LogicalPlan::GraphNode(node) => {
                // First, recurse into the input
                let child_tf =
                    self.analyze_with_graph_schema(node.input.clone(), plan_ctx, _graph_schema)?;

                // Compute projected columns for this node
                let projected_columns = Self::compute_projected_columns_for_node(node);

                if projected_columns.is_some() {
                    log::debug!(
                        "🔧 ProjectedColumnsResolver: Computed {} columns for node '{}'",
                        projected_columns.as_ref().unwrap().len(),
                        node.alias
                    );
                }

                // Rebuild if child changed or we computed new projected_columns
                if child_tf.is_yes() || projected_columns.is_some() {
                    Transformed::Yes(Arc::new(LogicalPlan::GraphNode(GraphNode {
                        input: child_tf.get_plan(),
                        alias: node.alias.clone(),
                        label: node.label.clone(),
                        is_denormalized: node.is_denormalized,
                        projected_columns,
                    })))
                } else {
                    Transformed::No(logical_plan.clone())
                }
            }

            // GraphRel - recurse into children
            LogicalPlan::GraphRel(rel) => {
                let left_tf =
                    self.analyze_with_graph_schema(rel.left.clone(), plan_ctx, _graph_schema)?;
                let center_tf =
                    self.analyze_with_graph_schema(rel.center.clone(), plan_ctx, _graph_schema)?;
                let right_tf =
                    self.analyze_with_graph_schema(rel.right.clone(), plan_ctx, _graph_schema)?;

                if left_tf.is_yes() || center_tf.is_yes() || right_tf.is_yes() {
                    Transformed::Yes(Arc::new(LogicalPlan::GraphRel(GraphRel {
                        left: left_tf.get_plan(),
                        center: center_tf.get_plan(),
                        right: right_tf.get_plan(),
                        ..rel.clone()
                    })))
                } else {
                    Transformed::No(logical_plan.clone())
                }
            }

            // Recursively process other plan types
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
            LogicalPlan::WithClause(wc) => {
                let child_tf =
                    self.analyze_with_graph_schema(wc.input.clone(), plan_ctx, _graph_schema)?;

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
            | LogicalPlan::Scan(_)
            | LogicalPlan::ViewScan(_)
            | LogicalPlan::Cte(_)
            | LogicalPlan::PageRank(_)
            | LogicalPlan::Unwind(_)
            | LogicalPlan::CartesianProduct(_) => Transformed::No(logical_plan.clone()),
        })
    }
}
