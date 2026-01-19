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
    graph_catalog::{
        graph_schema::GraphSchema,
        pattern_schema::{NodeAccessStrategy, NodePosition},
    },
    query_planner::{
        analyzer::{
            analyzer_pass::{AnalyzerPass, AnalyzerResult},

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
    ///
    /// # Arguments
    /// * `node` - The GraphNode to compute columns for
    /// * `plan_ctx` - PlanCtx for accessing PatternSchemaContext
    /// * `rel_alias` - Optional relationship alias (for denormalized nodes)
    /// * `position` - Optional node position in pattern (Left/Right, for denormalized nodes)
    fn compute_projected_columns_for_node(
        node: &GraphNode,
        plan_ctx: &PlanCtx,
        rel_alias: Option<&str>,
        position: Option<NodePosition>,
    ) -> Option<Vec<(String, String)>> {
        // The input should be a ViewScan (or through Filters, etc.)
        let view_scan = Self::find_view_scan(&node.input)?;

        // Use PatternSchemaContext to determine node access strategy
        match plan_ctx.get_node_strategy(&node.alias, rel_alias) {
            Some(NodeAccessStrategy::EmbeddedInEdge { .. }) => {
                // Denormalized node: properties come from edge table
                Self::compute_denormalized_properties(
                    &node.alias,
                    view_scan,
                    plan_ctx,
                    rel_alias,
                    position,
                )
            }
            Some(NodeAccessStrategy::OwnTable { properties, .. }) => {
                // Standard node: use property mappings from ViewScan
                let mut result: Vec<(String, String)> = properties
                    .iter()
                    .map(|(prop_name, prop_value)| {
                        let qualified = format!("{}.{}", node.alias, prop_value);
                        (prop_name.clone(), qualified)
                    })
                    .collect();
                result.sort_by(|a, b| a.0.cmp(&b.0));
                Some(result)
            }
            Some(NodeAccessStrategy::Virtual { .. }) => {
                // Virtual node: no properties to project
                Some(vec![])
            }
            None => {
                // No strategy found: fall back to ViewScan properties (legacy behavior)
                // This maintains compatibility during transition
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
        }
    }

    /// Find ViewScan in the plan (might be wrapped in Filters, etc.)
    fn find_view_scan(plan: &Arc<LogicalPlan>) -> Option<&ViewScan> {
        match plan.as_ref() {
            LogicalPlan::ViewScan(scan) => Some(scan),
            LogicalPlan::Filter(filter) => Self::find_view_scan(&filter.input),
            _ => None,
        }
    }

    /// Compute projected columns for denormalized nodes using PatternSchemaContext
    ///
    /// Uses explicit role information from PatternSchemaContext instead of checking both property sets.
    ///
    /// Note: `_position` parameter is currently unused because `get_node_strategy` resolves
    /// the role from `rel_alias`, but it is kept for API consistency and potential future use.
    fn compute_denormalized_properties(
        alias: &str,
        scan: &ViewScan,
        plan_ctx: &PlanCtx,
        rel_alias: Option<&str>,
        _position: Option<NodePosition>,
    ) -> Option<Vec<(String, String)>> {
        // Use PatternSchemaContext for explicit role-based resolution
        if let (Some(rel), Some(_pos)) = (rel_alias, _position) {
            if let Some(strategy) = plan_ctx.get_node_strategy(alias, Some(rel)) {
                // Use the strategy to get properties for this node's role
                let properties = strategy.get_all_properties();
                let mut result: Vec<(String, String)> = properties
                    .into_iter()
                    .map(|(prop_name, prop_value)| {
                        let qualified = format!("{}.{}", alias, prop_value);
                        (prop_name, qualified)
                    })
                    .collect();
                result.sort_by(|a, b| a.0.cmp(&b.0));
                if !result.is_empty() {
                    return Some(result);
                }
            }
        }

        // No pattern context = bug in GraphJoinInference, don't hide it
        None
    }

    /// Process a node within a relationship context, providing position information
    fn process_node_in_rel_context(
        &self,
        node_plan: &Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
        rel_alias: &str,
        position: NodePosition,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        // If it's a GraphNode, process with context
        if let LogicalPlan::GraphNode(node) = node_plan.as_ref() {
            // Recurse into input
            let child_tf =
                self.analyze_with_graph_schema(node.input.clone(), plan_ctx, graph_schema)?;

            // Compute projected columns with relationship context
            let projected_columns = Self::compute_projected_columns_for_node(
                node,
                plan_ctx,
                Some(rel_alias),
                Some(position),
            );

            if projected_columns.is_some() {
                log::debug!(
                    "🔧 ProjectedColumnsResolver: Computed {} columns for node '{}' ({:?} in rel '{}')",
                    projected_columns.as_ref().unwrap().len(),
                    node.alias,
                    position,
                    rel_alias
                );
            }

            // Rebuild if changed
            if child_tf.is_yes() || projected_columns.is_some() {
                Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphNode(
                    GraphNode {
                        input: child_tf.get_plan(),
                        alias: node.alias.clone(),
                        label: node.label.clone(),
                        is_denormalized: node.is_denormalized,
                        projected_columns,
                    },
                ))))
            } else {
                Ok(Transformed::No(node_plan.clone()))
            }
        } else {
            // Not a GraphNode, recurse normally
            self.analyze_with_graph_schema(node_plan.clone(), plan_ctx, graph_schema)
        }
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
                // Standalone nodes don't have relationship context
                let projected_columns = Self::compute_projected_columns_for_node(
                    node, plan_ctx, None, // No relationship context for standalone nodes
                    None,
                );

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

            // GraphRel - process nodes with relationship context
            LogicalPlan::GraphRel(rel) => {
                // Process left node with Left position context
                let left_tf = self.process_node_in_rel_context(
                    &rel.left,
                    plan_ctx,
                    _graph_schema,
                    &rel.alias,
                    NodePosition::Left,
                )?;

                // Process relationship center (ViewScan for the edge table)
                let center_tf =
                    self.analyze_with_graph_schema(rel.center.clone(), plan_ctx, _graph_schema)?;

                // Process right node with Right position context
                let right_tf = self.process_node_in_rel_context(
                    &rel.right,
                    plan_ctx,
                    _graph_schema,
                    &rel.alias,
                    NodePosition::Right,
                )?;

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
            | LogicalPlan::ViewScan(_)
            | LogicalPlan::Cte(_)
            | LogicalPlan::PageRank(_)
            | LogicalPlan::Unwind(_)
            | LogicalPlan::CartesianProduct(_) => Transformed::No(logical_plan.clone()),
        })
    }
}
