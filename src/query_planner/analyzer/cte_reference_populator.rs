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

    /// Recursively collect `alias -> cte_name` export mappings from every
    /// `WithClause` reachable anywhere within `plan`, exhaustively walking
    /// every child via [`LogicalPlan::children()`].
    ///
    /// Multi-relationship / comma-pattern (CartesianProduct) queries sharing
    /// a WITH-exported anchor get chained by GraphJoinInference into nested
    /// `GraphRel`s (e.g. `t2.left = GraphRel(t1)`, `t1.left =
    /// WithClause(p)`). A shallow "is my *immediate* left child a
    /// WithClause?" check only finds the WithClause when it is exactly one
    /// hop away and misses it when buried two-plus hops down — silently
    /// leaving the outer GraphRel's `cte_references` empty and producing an
    /// invalid JOIN condition at render time (see regression test /
    /// `docs` for the live repro). Searching the whole subtree fixes this
    /// for any nesting depth and shape, not just the one pattern observed.
    ///
    /// Precedence: entries already present in `out` are NOT overwritten, so
    /// the first (closest / outermost) WithClause found for a given alias
    /// wins, matching the "most local WITH wins" semantics used elsewhere
    /// in this pass.
    fn collect_reachable_cte_exports(plan: &LogicalPlan, out: &mut HashMap<String, String>) {
        if let LogicalPlan::WithClause(wc) = plan {
            if let Some(cte_name) = &wc.cte_name {
                for alias in &wc.exported_aliases {
                    out.entry(alias.clone()).or_insert_with(|| cte_name.clone());
                }
            }
        }
        for child in plan.children() {
            Self::collect_reachable_cte_exports(child, out);
        }
    }

    /// Recursively populate cte_references in GraphRel nodes
    ///
    /// @param plan: The plan to process
    /// @param available_ctes: Map of alias -> CTE name for all CTEs visible at this point
    /// @param plan_ctx: Plan context for looking up CTE names
    #[allow(clippy::only_used_in_recursion)] // plan_ctx threaded for analyzer-pass API symmetry
    fn populate(
        &self,
        plan: Arc<LogicalPlan>,
        available_ctes: &HashMap<String, String>,
        plan_ctx: &PlanCtx,
    ) -> Result<Transformed<Arc<LogicalPlan>>, AnalyzerError> {
        match plan.as_ref() {
            LogicalPlan::WithClause(wc) => {
                // CRITICAL: Add this WITH's exports to available_ctes BEFORE processing input
                // This way, GraphRels inside the input can find the CTE references
                let mut input_ctes = available_ctes.clone();
                if let Some(cte_name) = &wc.cte_name {
                    for alias in &wc.exported_aliases {
                        log::info!("🔍 CteReferencePopulator: Adding WITH export '{}' -> '{}' for input processing",
                                   alias, cte_name);
                        input_ctes.insert(alias.clone(), cte_name.clone());
                    }
                }

                // Process the input with updated CTE context
                let input_resolved = self.populate(wc.input.clone(), &input_ctes, plan_ctx)?;

                if input_resolved.is_yes() {
                    let new_wc = WithClause {
                        cte_name: wc.cte_name.clone(),
                        input: input_resolved.get_plan(),
                        ..wc.clone()
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::WithClause(new_wc))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::GraphRel(rel) => {
                // Collect CTE exports reachable ANYWHERE within this GraphRel's
                // children (left/center/right), not just when the immediate left
                // child happens to be a WithClause — see collect_reachable_cte_exports
                // doc comment for why a shallow one-hop check is insufficient.
                let mut child_ctes = available_ctes.clone();
                for child in plan.children() {
                    Self::collect_reachable_cte_exports(child, &mut child_ctes);
                }

                // Check if left_connection or right_connection are in available CTEs
                // Use child_ctes which includes exports reachable from this GraphRel's children
                let mut cte_refs = rel.cte_references.clone();
                let mut found_new_refs = false;

                if child_ctes.contains_key(&rel.left_connection) {
                    if let Some(cte_name) = child_ctes.get(&rel.left_connection) {
                        log::info!("🔍 CteReferencePopulator: GraphRel '{}' left_connection '{}' -> CTE '{}'",
                                   rel.alias, rel.left_connection, cte_name);
                        cte_refs.insert(rel.left_connection.clone(), cte_name.clone());
                        found_new_refs = true;
                    }
                }

                if child_ctes.contains_key(&rel.right_connection) {
                    if let Some(cte_name) = child_ctes.get(&rel.right_connection) {
                        log::info!("🔍 CteReferencePopulator: GraphRel '{}' right_connection '{}' -> CTE '{}'",
                                   rel.alias, rel.right_connection, cte_name);
                        cte_refs.insert(rel.right_connection.clone(), cte_name.clone());
                        found_new_refs = true;
                    }
                }

                let left_resolved = self.populate(rel.left.clone(), &child_ctes, plan_ctx)?;
                let center_resolved = self.populate(rel.center.clone(), &child_ctes, plan_ctx)?;
                let right_resolved = self.populate(rel.right.clone(), &child_ctes, plan_ctx)?;

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

            // For all other node types, recursively process children.
            // Uses the exhaustive LogicalPlan::children()/map_children() API so
            // that every variant (GraphNode, GroupBy, OrderBy, Skip, Limit, Cte,
            // Union, Unwind, CartesianProduct, Create, SetProperties, Delete,
            // Remove, ViewScan, ...) is visited — a hand-picked catch-all here
            // previously skipped several of these, silently failing to propagate
            // cte_references into any GraphRel/WithClause nested underneath them
            // (e.g. a WITH inside one arm of a UNION).
            other => {
                let mut first_err: Option<AnalyzerError> = None;
                let mut any_yes = false;
                let new_plan = other.map_children(|child| {
                    if first_err.is_some() {
                        return child.clone();
                    }
                    match self.populate(Arc::new(child.clone()), available_ctes, plan_ctx) {
                        Ok(Transformed::Yes(new_child)) => {
                            any_yes = true;
                            (*new_child).clone()
                        }
                        Ok(Transformed::No(_)) => child.clone(),
                        Err(e) => {
                            first_err = Some(e);
                            child.clone()
                        }
                    }
                });

                if let Some(e) = first_err {
                    return Err(e);
                }

                if any_yes {
                    Ok(Transformed::Yes(Arc::new(new_plan)))
                } else {
                    Ok(Transformed::No(plan))
                }
            }
        }
    }
}

impl AnalyzerPass for CteReferencePopulator {
    fn analyze(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
    ) -> Result<Transformed<Arc<LogicalPlan>>, AnalyzerError> {
        log::info!("🔍 CteReferencePopulator: Starting CTE reference population");

        let empty_ctes = HashMap::new();
        let result = self.populate(logical_plan, &empty_ctes, plan_ctx)?;

        log::info!(
            "🔍 CteReferencePopulator: Completed - transformed: {}",
            result.is_yes()
        );

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_planner::logical_expr::{Direction, LogicalExpr, TableAlias};
    use crate::query_planner::logical_plan::{GraphNode, GraphRel, ProjectionItem};

    fn leaf_node(alias: &str) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::GraphNode(GraphNode {
            input: Arc::new(LogicalPlan::Empty),
            alias: alias.to_string(),
            label: None,
            is_denormalized: false,
            projected_columns: None,
            node_types: None,
        }))
    }

    fn with_clause_exporting(alias: &str, cte_name: &str, input: Arc<LogicalPlan>) -> WithClause {
        WithClause {
            input,
            items: vec![ProjectionItem {
                expression: LogicalExpr::TableAlias(TableAlias(alias.to_string())),
                col_alias: None,
            }],
            distinct: false,
            order_by: None,
            skip: None,
            limit: None,
            where_clause: None,
            exported_aliases: vec![alias.to_string()],
            cte_name: Some(cte_name.to_string()),
            cte_references: HashMap::new(),
            pattern_comprehensions: Vec::new(),
        }
    }

    fn graph_rel(
        alias: &str,
        left: Arc<LogicalPlan>,
        right: Arc<LogicalPlan>,
        left_connection: &str,
        right_connection: &str,
    ) -> GraphRel {
        GraphRel {
            left,
            center: Arc::new(LogicalPlan::Empty),
            right,
            alias: alias.to_string(),
            direction: Direction::Outgoing,
            left_connection: left_connection.to_string(),
            right_connection: right_connection.to_string(),
            is_rel_anchor: false,
            variable_length: None,
            shortest_path_mode: None,
            path_variable: None,
            where_predicate: None,
            labels: None,
            is_optional: None,
            anchor_connection: None,
            cte_references: HashMap::new(),
            pattern_combinations: None,
            was_undirected: None,
            match_clause_index: 0, // #586 (synthetic/test)
            optional_anchor_where: None,
        }
    }

    /// Regression for the live bug found while inventorying BUG1's unvisited
    /// `populate()` positions: a comma-pattern (`MATCH (p) WITH p MATCH
    /// (p)-[:A]->(x), (p)-[:B]->(y)`) gets chained by GraphJoinInference into
    /// NESTED GraphRels — `t2.left = GraphRel(t1)`, `t1.left =
    /// WithClause(p)` — rather than `t2.left` being the WithClause directly.
    /// The old code only checked `rel.left.as_ref()` for being a WithClause
    /// ONE hop down, so the OUTER GraphRel (`t2`) never saw the WITH export
    /// and its `cte_references` stayed empty — live-verified against
    /// ClickHouse to produce an invalid JOIN condition
    /// (`t2.user_id = p.post_id` instead of `p.p1_p_user_id`, "Identifier
    /// 'p.post_id' cannot be resolved" / UNKNOWN_IDENTIFIER).
    ///
    /// The fix searches the ENTIRE subtree reachable from a GraphRel's
    /// children (via the exhaustive `LogicalPlan::children()` API) for any
    /// WithClause export, not just the immediate left child.
    #[test]
    fn graph_rel_two_hops_below_with_clause_gets_cte_reference() {
        let with_p = Arc::new(LogicalPlan::WithClause(with_clause_exporting(
            "p",
            "with_p_cte_0",
            leaf_node("p"),
        )));

        // t1: (p)-[:A]->(post) — WithClause is t1's IMMEDIATE left child.
        let t1 = Arc::new(LogicalPlan::GraphRel(graph_rel(
            "t1",
            with_p,
            leaf_node("post"),
            "p",
            "post",
        )));

        // t2: (p)-[:B]->(post2) — chained so t2's left is t1 (a GraphRel),
        // NOT the WithClause directly. This is the shape GraphJoinInference
        // produces for the comma-pattern / shared-anchor fan-out.
        let root = Arc::new(LogicalPlan::GraphRel(graph_rel(
            "t2",
            t1,
            leaf_node("post2"),
            "p",
            "post2",
        )));

        let mut plan_ctx = PlanCtx::new_empty();
        let result = CteReferencePopulator::new()
            .analyze(root, &mut plan_ctx)
            .expect("populate should not error");

        assert!(
            result.is_yes(),
            "expected the plan to be transformed (cte_references populated on both GraphRels)"
        );
        let new_root = result.get_plan();

        let t2_rel = match new_root.as_ref() {
            LogicalPlan::GraphRel(r) => r,
            other => panic!("expected root to remain a GraphRel, got {other:?}"),
        };
        assert_eq!(
            t2_rel.cte_references.get("p"),
            Some(&"with_p_cte_0".to_string()),
            "outer GraphRel 't2' (two hops below the WithClause) must have \
             'p' resolved to its CTE — this is the exact live bug: {:?}",
            t2_rel.cte_references
        );

        let t1_rel = match t2_rel.left.as_ref() {
            LogicalPlan::GraphRel(r) => r,
            other => panic!("expected t2.left to remain a GraphRel, got {other:?}"),
        };
        assert_eq!(
            t1_rel.cte_references.get("p"),
            Some(&"with_p_cte_0".to_string()),
            "inner GraphRel 't1' (immediate child of the WithClause) must \
             also have 'p' resolved to its CTE"
        );
    }

    /// Regression for BUG1's catch-all gap: a WithClause nested under an
    /// unvisited variant (here, Skip — standing in for the whole family of
    /// previously-unvisited wrapper nodes: GroupBy/OrderBy/Skip/Limit/Cte/
    /// Union/Unwind/CartesianProduct/GraphNode) must still be walked so any
    /// GraphRel further down gets its cte_references populated. Before the
    /// fix, `populate()`'s catch-all returned `Transformed::No` immediately
    /// for `Skip`, never descending into its `input`.
    #[test]
    fn with_clause_under_previously_unvisited_variant_is_still_populated() {
        let with_p = Arc::new(LogicalPlan::WithClause(with_clause_exporting(
            "p",
            "with_p_cte_0",
            leaf_node("p"),
        )));

        let t1 = Arc::new(LogicalPlan::GraphRel(graph_rel(
            "t1",
            with_p,
            leaf_node("post"),
            "p",
            "post",
        )));

        // Wrap the whole thing in a Skip node — one of the variants the old
        // catch-all silently skipped instead of descending into.
        let root = Arc::new(LogicalPlan::Skip(
            crate::query_planner::logical_plan::Skip {
                input: t1,
                count: 5,
            },
        ));

        let mut plan_ctx = PlanCtx::new_empty();
        let result = CteReferencePopulator::new()
            .analyze(root, &mut plan_ctx)
            .expect("populate should not error");

        assert!(
            result.is_yes(),
            "expected the plan to be transformed (cte_references populated under Skip)"
        );
        let new_root = result.get_plan();

        let skip = match new_root.as_ref() {
            LogicalPlan::Skip(s) => s,
            other => panic!("expected root to remain Skip, got {other:?}"),
        };
        let t1_rel = match skip.input.as_ref() {
            LogicalPlan::GraphRel(r) => r,
            other => panic!("expected Skip.input to remain a GraphRel, got {other:?}"),
        };
        assert_eq!(
            t1_rel.cte_references.get("p"),
            Some(&"with_p_cte_0".to_string()),
            "GraphRel nested under Skip must have its cte_references \
             populated — the old catch-all never descended past Skip"
        );
    }
}
