//! Pattern Graph Metadata Construction
//!
//! This module builds a lightweight "index" over the GraphRel tree,
//! caching information that's currently computed repeatedly throughout the algorithm.
//! This enables cleaner join inference logic without rewriting the entire system.
//!
//! ## Key Types
//!
//! - [`PatternGraphMetadata`] - Complete metadata for a MATCH clause
//! - [`PatternNodeInfo`] - Cached information about a node variable
//! - [`PatternEdgeInfo`] - Cached information about a relationship variable
//! - [`PatternMetadataBuilder`] - Builder for constructing metadata from logical plans

use std::collections::HashMap;

use crate::query_planner::{
    analyzer::{
        analyzer_pass::AnalyzerResult,
        errors::{AnalyzerError, Pass},
    },
    logical_expr::{Direction, LogicalExpr},
    logical_plan::LogicalPlan,
    plan_ctx::PlanCtx,
};

/// Metadata about a node in the MATCH pattern graph.
/// Cached information to avoid repeated traversals and reference checking.
#[derive(Debug, Clone)]
pub struct PatternNodeInfo {
    /// Node variable alias (e.g., "a", "b", "person")
    pub alias: String,
    /// Optional label constraint (e.g., Some("User"), None for unlabeled nodes)
    pub label: Option<String>,
    /// Whether this node is referenced in SELECT/WHERE/ORDER BY/etc.
    /// Cached result of is_node_referenced() to avoid repeated tree traversals.
    pub is_referenced: bool,
    /// How many edges (relationships) use this node.
    /// appearance_count > 1 indicates cross-branch pattern (needs JOIN between edges)
    pub appearance_count: usize,
    /// Whether this node has an explicit label in Cypher (e.g., (a:User) vs (a))
    /// Used for SingleTableScan optimization decisions.
    pub has_explicit_label: bool,
}

/// Metadata about an edge (relationship) in the MATCH pattern graph.
/// Represents a single relationship pattern like -[r:TYPE]->
#[derive(Debug, Clone)]
pub struct PatternEdgeInfo {
    /// Edge variable alias (e.g., "r", "follows", "t1")
    pub alias: String,
    /// Relationship types (e.g., ["FOLLOWS"], or ["FOLLOWS", "FRIENDS"] for [:FOLLOWS|FRIENDS])
    pub rel_types: Vec<String>,
    /// Source node variable (e.g., "a" in (a)-[r]->(b))
    pub from_node: String,
    /// Target node variable (e.g., "b" in (a)-[r]->(b))
    pub to_node: String,
    /// Whether this edge's properties are referenced in the query
    pub is_referenced: bool,
    /// Whether this is a variable-length path (e.g., *1..3, *)
    pub is_vlp: bool,
    /// Whether this is a shortest path pattern
    pub is_shortest_path: bool,
    /// Direction: Outgoing (-[r]->), Incoming (<-[r]-), Either (-[r]-)
    pub direction: Direction,
    /// Whether this edge is part of an OPTIONAL MATCH
    pub is_optional: bool,
}

/// Complete pattern graph metadata extracted from a MATCH clause.
/// Provides a "map" view of the pattern structure to enable cleaner join inference.
#[derive(Debug, Clone, Default)]
pub struct PatternGraphMetadata {
    /// All nodes in the pattern, indexed by alias
    pub nodes: HashMap<String, PatternNodeInfo>,
    /// All edges in the pattern (in order of appearance)
    pub edges: Vec<PatternEdgeInfo>,
}

impl PatternGraphMetadata {
    /// Get edge metadata by alias
    pub fn get_edge_by_alias(&self, alias: &str) -> Option<&PatternEdgeInfo> {
        self.edges.iter().find(|e| e.alias == alias)
    }

    /// Get all edges that use a specific node (by node alias)
    pub fn edges_using_node(&self, node_alias: &str) -> Vec<&PatternEdgeInfo> {
        self.edges
            .iter()
            .filter(|e| e.from_node == node_alias || e.to_node == node_alias)
            .collect()
    }

    /// Check if a node appears in multiple edges (cross-branch pattern indicator)
    pub fn is_cross_branch_node(&self, node_alias: &str) -> bool {
        self.nodes
            .get(node_alias)
            .map(|n| n.appearance_count > 1)
            .unwrap_or(false)
    }
}

/// Builder for constructing PatternGraphMetadata from a logical plan.
pub struct PatternMetadataBuilder;

impl PatternMetadataBuilder {
    /// Build pattern graph metadata by traversing the GraphRel tree.
    ///
    /// Phase 1: Extract pattern info (nodes and edges)
    /// Phase 2: Compute node references (which nodes are used in SELECT/WHERE/etc)
    /// Phase 3: Compute edge references (which edges are used)
    /// Phase 4: Count node appearances (for cross-branch detection)
    pub fn build(
        logical_plan: &LogicalPlan,
        plan_ctx: &PlanCtx,
    ) -> AnalyzerResult<PatternGraphMetadata> {
        let mut metadata = PatternGraphMetadata::default();

        // Phase 1: Extract pattern structure from GraphRel tree
        Self::extract_pattern_info(logical_plan, plan_ctx, &mut metadata)?;

        // Phase 2: Compute which nodes are referenced in the query
        Self::compute_node_references(logical_plan, &mut metadata);

        // Phase 3: Compute which edges are referenced
        Self::compute_edge_references(logical_plan, &mut metadata);

        // Phase 4: Count node appearances (appearance_count)
        Self::compute_node_appearances(&mut metadata);

        log::debug!(
            "📊 Built PatternGraphMetadata: {} nodes, {} edges",
            metadata.nodes.len(),
            metadata.edges.len()
        );

        Ok(metadata)
    }

    /// Phase 1: Extract pattern info from GraphRel nodes
    fn extract_pattern_info(
        plan: &LogicalPlan,
        plan_ctx: &PlanCtx,
        metadata: &mut PatternGraphMetadata,
    ) -> AnalyzerResult<()> {
        match plan {
            LogicalPlan::GraphRel(graph_rel) => {
                let edge_info = PatternEdgeInfo {
                    alias: graph_rel.alias.clone(),
                    rel_types: graph_rel.labels.clone().unwrap_or_default(),
                    from_node: graph_rel.left_connection.clone(),
                    to_node: graph_rel.right_connection.clone(),
                    is_referenced: false,
                    is_vlp: graph_rel.variable_length.is_some(),
                    is_shortest_path: graph_rel.shortest_path_mode.is_some(),
                    direction: graph_rel.direction.clone(),
                    is_optional: graph_rel.is_optional.unwrap_or(false),
                };
                metadata.edges.push(edge_info);

                Self::extract_node_info(&graph_rel.left_connection, plan_ctx, metadata)?;
                Self::extract_node_info(&graph_rel.right_connection, plan_ctx, metadata)?;

                Self::extract_pattern_info(&graph_rel.left, plan_ctx, metadata)?;
                Self::extract_pattern_info(&graph_rel.right, plan_ctx, metadata)?;
            }
            LogicalPlan::GraphNode(graph_node) => {
                Self::extract_node_info(&graph_node.alias, plan_ctx, metadata)?;
                Self::extract_pattern_info(&graph_node.input, plan_ctx, metadata)?;
            }
            LogicalPlan::Projection(p) => Self::extract_pattern_info(&p.input, plan_ctx, metadata)?,
            LogicalPlan::Filter(f) => Self::extract_pattern_info(&f.input, plan_ctx, metadata)?,
            LogicalPlan::GraphJoins(gj) => {
                Self::extract_pattern_info(&gj.input, plan_ctx, metadata)?
            }
            LogicalPlan::GroupBy(gb) => Self::extract_pattern_info(&gb.input, plan_ctx, metadata)?,
            LogicalPlan::OrderBy(ob) => Self::extract_pattern_info(&ob.input, plan_ctx, metadata)?,
            LogicalPlan::Skip(s) => Self::extract_pattern_info(&s.input, plan_ctx, metadata)?,
            LogicalPlan::Limit(l) => Self::extract_pattern_info(&l.input, plan_ctx, metadata)?,
            LogicalPlan::Cte(cte) => Self::extract_pattern_info(&cte.input, plan_ctx, metadata)?,
            LogicalPlan::Union(u) => {
                for input in &u.inputs {
                    Self::extract_pattern_info(input, plan_ctx, metadata)?;
                }
            }
            LogicalPlan::CartesianProduct(cp) => {
                Self::extract_pattern_info(&cp.left, plan_ctx, metadata)?;
                Self::extract_pattern_info(&cp.right, plan_ctx, metadata)?;
            }
            LogicalPlan::Unwind(uw) => Self::extract_pattern_info(&uw.input, plan_ctx, metadata)?,
            LogicalPlan::WithClause(wc) => {
                Self::extract_pattern_info(&wc.input, plan_ctx, metadata)?
            }
            LogicalPlan::ViewScan(_) | LogicalPlan::Empty | LogicalPlan::PageRank(_) => {}
        }
        Ok(())
    }

    /// Extract node info from an alias if not already present
    fn extract_node_info(
        alias: &str,
        plan_ctx: &PlanCtx,
        metadata: &mut PatternGraphMetadata,
    ) -> AnalyzerResult<()> {
        if metadata.nodes.contains_key(alias) {
            return Ok(());
        }

        let table_ctx = plan_ctx
            .get_table_ctx_from_alias_opt(&Some(alias.to_string()))
            .map_err(|e| AnalyzerError::PlanCtx {
                pass: Pass::GraphJoinInference,
                source: e,
            })?;

        let label = table_ctx.get_label_str().ok();
        let has_explicit_label = false; // TODO: Extract from TableCtx

        let node_info = PatternNodeInfo {
            alias: alias.to_string(),
            label,
            is_referenced: false,
            appearance_count: 0,
            has_explicit_label,
        };

        metadata.nodes.insert(alias.to_string(), node_info);
        Ok(())
    }

    /// Phase 2: Compute which nodes are referenced in SELECT/WHERE/ORDER BY/etc
    fn compute_node_references(plan: &LogicalPlan, metadata: &mut PatternGraphMetadata) {
        for (alias, node_info) in metadata.nodes.iter_mut() {
            node_info.is_referenced = plan_references_alias(plan, alias);
        }
    }

    /// Phase 3: Compute which edges are referenced
    fn compute_edge_references(plan: &LogicalPlan, metadata: &mut PatternGraphMetadata) {
        for edge_info in metadata.edges.iter_mut() {
            edge_info.is_referenced = plan_references_alias(plan, &edge_info.alias);
        }
    }

    /// Phase 4: Count how many edges use each node (for cross-branch detection)
    fn compute_node_appearances(metadata: &mut PatternGraphMetadata) {
        for node_info in metadata.nodes.values_mut() {
            let count = metadata
                .edges
                .iter()
                .filter(|e| e.from_node == node_info.alias || e.to_node == node_info.alias)
                .count();
            node_info.appearance_count = count;
        }
    }
}

/// Recursively search a logical plan tree for references to an alias
pub fn plan_references_alias(plan: &LogicalPlan, alias: &str) -> bool {
    match plan {
        LogicalPlan::Projection(proj) => {
            for item in &proj.items {
                if expr_references_alias(&item.expression, alias) {
                    return true;
                }
            }
            plan_references_alias(&proj.input, alias)
        }
        LogicalPlan::GroupBy(group_by) => {
            for expr in &group_by.expressions {
                if expr_references_alias(expr, alias) {
                    return true;
                }
            }
            plan_references_alias(&group_by.input, alias)
        }
        LogicalPlan::Filter(filter) => {
            if expr_references_alias(&filter.predicate, alias) {
                return true;
            }
            plan_references_alias(&filter.input, alias)
        }
        LogicalPlan::GraphRel(graph_rel) => {
            if let Some(where_pred) = &graph_rel.where_predicate {
                if expr_references_alias(where_pred, alias) {
                    return true;
                }
            }
            false
        }
        LogicalPlan::GraphNode(_) => false,
        LogicalPlan::GraphJoins(gj) => plan_references_alias(&gj.input, alias),
        LogicalPlan::Cte(cte) => plan_references_alias(&cte.input, alias),
        LogicalPlan::OrderBy(ob) => {
            for sort_expr in &ob.items {
                if expr_references_alias(&sort_expr.expression, alias) {
                    return true;
                }
            }
            plan_references_alias(&ob.input, alias)
        }
        LogicalPlan::Skip(s) => plan_references_alias(&s.input, alias),
        LogicalPlan::Limit(l) => plan_references_alias(&l.input, alias),
        LogicalPlan::Union(u) => u
            .inputs
            .iter()
            .any(|input| plan_references_alias(input, alias)),
        LogicalPlan::Unwind(uw) => {
            expr_references_alias(&uw.expression, alias) || plan_references_alias(&uw.input, alias)
        }
        LogicalPlan::CartesianProduct(cp) => {
            plan_references_alias(&cp.left, alias) || plan_references_alias(&cp.right, alias)
        }
        LogicalPlan::WithClause(wc) => plan_references_alias(&wc.input, alias),
        _ => false,
    }
}

/// Recursively check if an expression references a given alias
pub fn expr_references_alias(expr: &LogicalExpr, alias: &str) -> bool {
    match expr {
        LogicalExpr::TableAlias(table_alias) => table_alias.0 == alias,
        LogicalExpr::PropertyAccessExp(prop) => prop.table_alias.0 == alias,
        LogicalExpr::AggregateFnCall(agg) => {
            agg.args.iter().any(|arg| expr_references_alias(arg, alias))
        }
        LogicalExpr::ScalarFnCall(fn_call) => fn_call
            .args
            .iter()
            .any(|arg| expr_references_alias(arg, alias)),
        LogicalExpr::OperatorApplicationExp(op) => op
            .operands
            .iter()
            .any(|operand| expr_references_alias(operand, alias)),
        LogicalExpr::List(list) => list.iter().any(|item| expr_references_alias(item, alias)),
        LogicalExpr::Case(case) => {
            if let Some(expr) = &case.expr {
                if expr_references_alias(expr, alias) {
                    return true;
                }
            }
            for (when_expr, then_expr) in &case.when_then {
                if expr_references_alias(when_expr, alias)
                    || expr_references_alias(then_expr, alias)
                {
                    return true;
                }
            }
            if let Some(else_expr) = &case.else_expr {
                if expr_references_alias(else_expr, alias) {
                    return true;
                }
            }
            false
        }
        _ => false,
    }
}

/// Check if a node is referenced in the query (SELECT, WHERE, ORDER BY, etc.)
pub fn is_node_referenced(alias: &str, plan_ctx: &PlanCtx, logical_plan: &LogicalPlan) -> bool {
    crate::debug_print!("        DEBUG: is_node_referenced('{}') called", alias);

    if plan_references_alias(logical_plan, alias) {
        crate::debug_print!("        DEBUG: '{}' IS referenced in logical plan", alias);
        return true;
    }

    for (_ctx_alias, table_ctx) in plan_ctx.get_alias_table_ctx_map().iter() {
        for filter in table_ctx.get_filters() {
            if expr_references_alias(filter, alias) {
                crate::debug_print!("        DEBUG: '{}' IS referenced in filters", alias);
                return true;
            }
        }
    }

    crate::debug_print!("        DEBUG: '{}' is NOT referenced", alias);
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_planner::logical_expr::{
        AggregateFnCall, LogicalExpr, PropertyAccess, TableAlias,
    };

    #[test]
    fn expr_references_alias_finds_aggregate_arg() {
        // count(reply) should reference "reply"
        let expr = LogicalExpr::AggregateFnCall(AggregateFnCall {
            name: "count".to_string(),
            args: vec![LogicalExpr::TableAlias(TableAlias("reply".to_string()))],
        });
        assert!(expr_references_alias(&expr, "reply"));
        assert!(!expr_references_alias(&expr, "message"));
    }

    #[test]
    fn expr_references_alias_finds_property_access() {
        // reply.creationDate should reference "reply"
        let expr = LogicalExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias("reply".to_string()),
            column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                "creationDate".to_string(),
            ),
        });
        assert!(expr_references_alias(&expr, "reply"));
        assert!(!expr_references_alias(&expr, "message"));
    }

    #[test]
    fn plan_references_alias_skips_with_clause_items() {
        // Verifies that plan_references_alias on a WithClause only checks
        // wc.input, NOT wc.items/where_clause/order_by. This is the
        // pre-existing behavior that the inference.rs call-site fix
        // compensates for.
        use crate::query_planner::logical_plan::{
            GraphNode, LogicalPlan, ProjectionItem, WithClause,
        };
        use std::sync::Arc;

        let inner_node = Arc::new(LogicalPlan::GraphNode(GraphNode {
            input: Arc::new(LogicalPlan::Empty),
            alias: "message".to_string(),
            label: Some("Message".to_string()),
            is_denormalized: false,
            projected_columns: None,
            node_types: None,
        }));
        let wc = LogicalPlan::WithClause(WithClause {
            input: inner_node,
            items: vec![ProjectionItem {
                expression: LogicalExpr::AggregateFnCall(AggregateFnCall {
                    name: "count".to_string(),
                    args: vec![LogicalExpr::TableAlias(TableAlias("reply".to_string()))],
                }),
                col_alias: None,
            }],
            distinct: false,
            order_by: None,
            skip: None,
            limit: None,
            where_clause: Some(LogicalExpr::TableAlias(TableAlias(
                "filtered_node".to_string(),
            ))),
            exported_aliases: vec!["replyCount".to_string()],
            cte_name: None,
            cte_references: std::collections::HashMap::new(),
            pattern_comprehensions: Vec::new(),
        });
        // "reply" is only in items — plan_references_alias won't find it
        assert!(!plan_references_alias(&wc, "reply"));
        // "filtered_node" is only in where_clause — also not found
        assert!(!plan_references_alias(&wc, "filtered_node"));
    }
}
