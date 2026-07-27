//! Plan predicates — pure, read-only walkers that answer structural questions
//! about a `LogicalPlan` (currently: WITH-clause existence).
//!
//! Extracted verbatim from `plan_builder_utils.rs` in P2.4
//! (`REFACTORING_SAFETY_PLAN.md` §5.1). No logic edits — the functions are
//! re-exported `pub(crate)` from `plan_builder_utils` during the transition so
//! existing `super::plan_builder_utils::*` call sites keep resolving.
//!
//! Scope note: §5.1 also grouped the fresh-scan / with-exported alias walkers
//! here, but those are private helpers tightly coupled to the P2.5 `cte_rewrite`
//! functions (`update_graph_joins_cte_refs` / `rewrite_logical_expr_cte_refs`),
//! so they ride with P2.5 rather than churn visibility ahead of their callers
//! (§8.3 no-drive-by). The second `has_with_clause_in_graph_rel` copy in
//! `plan_builder_helpers.rs` (`pub(super)`, different semantics) is the flagged
//! D-cluster duplicate — a separate dedup, left untouched here.

use crate::query_planner::logical_plan::LogicalPlan;

/// Check if there's a `WithClause` anywhere in the logical plan tree.
///
/// This is the single canonical WITH-existence predicate (§4.2 unify). It is
/// built on the exhaustive [`LogicalPlan::any_node`] walk, so it reaches EVERY
/// child edge — `GraphRel.center`, `Cte.input`, `ViewScan.input`, and the
/// write-op inputs — and can never drift out of sync with the plan structure.
/// The walk is iterative (explicit stack), so it cannot overflow on deep plans;
/// the old manual `depth`-guard recursion is therefore gone.
///
/// [`plan_contains_with_clause`] is a synonym that delegates here — they were
/// two hand-rolled copies that had historically drifted (the §6 bug class);
/// they are now one implementation with two names kept for call-site clarity.
pub fn has_with_clause_in_tree(plan: &LogicalPlan) -> bool {
    plan.any_node(|node| matches!(node, LogicalPlan::WithClause(_)))
}

/// Check if plan has WITH clause in GraphRel.right (WITH+MATCH pattern)
pub fn has_with_clause_in_graph_rel(plan: &LogicalPlan) -> bool {
    log::debug!(
        "🔍 has_with_clause_in_graph_rel: Called with plan type: {:?}",
        std::mem::discriminant(plan)
    );
    fn check_graph_rel_right(plan: &LogicalPlan) -> bool {
        log::debug!(
            "🔍 check_graph_rel_right: Checking plan type: {:?}",
            std::mem::discriminant(plan)
        );
        match plan {
            LogicalPlan::GraphRel(gr) => {
                log::debug!(
                    "🔍 check_graph_rel: Found GraphRel, checking left: {:?}, right: {:?}",
                    std::mem::discriminant(&*gr.left),
                    std::mem::discriminant(&*gr.right)
                );
                // Check BOTH left and right sides for WITH clauses
                let has_in_left = has_with_clause_in_tree(&gr.left);
                let has_in_right = has_with_clause_in_tree(&gr.right);
                let recursive_left = check_graph_rel_right(&gr.left);
                let recursive_right = check_graph_rel_right(&gr.right);
                log::debug!(
            "🔍 check_graph_rel: has_in_left: {}, has_in_right: {}, recursive_left: {}, recursive_right: {}",
                    has_in_left, has_in_right, recursive_left, recursive_right
                );
                has_in_left || has_in_right || recursive_left || recursive_right
            }
            LogicalPlan::GraphJoins(gj) => {
                log::debug!(
                    "🔍 check_graph_rel_right: Found GraphJoins, checking input: {:?}",
                    std::mem::discriminant(&*gj.input)
                );
                check_graph_rel_right(&gj.input)
            }
            LogicalPlan::Projection(p) => {
                log::debug!(
                    "🔍 check_graph_rel_right: Found Projection, checking input: {:?}",
                    std::mem::discriminant(&*p.input)
                );
                check_graph_rel_right(&p.input)
            }
            LogicalPlan::Filter(f) => {
                log::debug!(
                    "🔍 check_graph_rel_right: Found Filter, checking input: {:?}",
                    std::mem::discriminant(&*f.input)
                );
                check_graph_rel_right(&f.input)
            }
            // Handle the unknown Discriminant(7) case - assume it might contain WITH clauses
            _ => {
                log::debug!("🔍 check_graph_rel_right: Unknown plan type {:?}, checking with has_with_clause_in_tree", std::mem::discriminant(plan));
                has_with_clause_in_tree(plan)
            }
        }
    }
    let result = check_graph_rel_right(plan);
    log::debug!("🔍 has_with_clause_in_graph_rel: Final result: {}", result);
    result
}

/// Check if plan contains a `WithClause` node — synonym for
/// [`has_with_clause_in_tree`].
///
/// These were two independently hand-rolled walkers that drifted apart (the §6
/// infinite-iteration / lost-WITH bug class: `plan_contains_with_clause` had at
/// one point missed `GraphRel.center`, `Cte`, and `ViewScan.input`). They are
/// now the SAME implementation — this one delegates — so the CLAUDE.md rule-5
/// "these must agree" invariant is structural, not a convention to police. The
/// name is retained because call sites in the WITH→CTE builder read more
/// clearly as "does this sub-tree still contain a WITH to process?".
pub fn plan_contains_with_clause(plan: &LogicalPlan) -> bool {
    has_with_clause_in_tree(plan)
}
