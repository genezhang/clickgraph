//! Bridge Node Elimination Optimization
//!
//! Removes unnecessary node table JOINs that only serve as FK bridges between
//! edge tables. When a node table is joined solely to connect two edge tables
//! (e.g., edge1.PersonId = person.id AND person.id = edge2.PersonId), the node
//! table can be eliminated and the edges joined directly (edge1.PersonId = edge2.PersonId).
//!
//! This is critical for performance with `join_use_nulls=1` (ClickGraph's default),
//! where chaining LEFT JOINs through unnecessary node tables causes ClickHouse
//! to become extremely slow.

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::render_plan::expression_utils::references_alias;
use crate::render_plan::render_expr::{
    Operator, OperatorApplication, PropertyAccess, RenderExpr, TableAlias,
};
use crate::render_plan::{CteContent, Join, RenderPlan};

/// Check if a table name is a generated CTE reference.
fn is_cte_table(table_name: &str) -> bool {
    table_name.starts_with("with_")
        || table_name.starts_with("vlp_")
        || table_name.starts_with("pc_")
        || table_name.starts_with("bidi_")
}

/// Upstream FK reference extracted from a bridge node's ON condition.
/// When a node JOIN has `ON node.id = edge.FK`, the upstream reference
/// is `edge.FK` (the alias and column from the other side of the equality).
struct UpstreamRef {
    alias: String,
    column: String,
}

/// Info about a bridge node candidate to be eliminated.
struct BridgeCandidate {
    /// Index into the joins vec
    join_idx: usize,
    /// The alias of the bridge node (e.g., "person2")
    alias: String,
    /// The ID column on the bridge node side (e.g., "id")
    id_column: String,
    /// The upstream FK reference (e.g., edge.PersonId)
    upstream: UpstreamRef,
}

/// Top-level entry point: eliminate unnecessary JOINs in all parts of the plan.
/// Two passes:
/// 1. Remove unreferenced JOINs (e.g., CROSS JOINs with `ON 1=1` where alias is unused)
/// 2. Remove bridge node JOINs (FK bridges between edge tables)
pub fn eliminate_bridge_nodes(plan: &mut RenderPlan) {
    // Apply both passes to main plan, UNION branches, and CTE bodies
    optimize_joins_in_plan(plan);

    if let Some(ref mut union) = plan.union.0 {
        for branch in union.input.iter_mut() {
            optimize_joins_in_plan(branch);
        }
    }

    for cte in plan.ctes.0.iter_mut() {
        if let CteContent::Structured(ref mut cte_plan) = cte.content {
            optimize_joins_in_plan(cte_plan);

            if let Some(ref mut union) = cte_plan.union.0 {
                for branch in union.input.iter_mut() {
                    optimize_joins_in_plan(branch);
                }
            }
        }
    }
}

/// Apply all join optimizations to a single plan.
fn optimize_joins_in_plan(plan: &mut RenderPlan) {
    remove_unreferenced_joins(plan);
    eliminate_bridge_nodes_in_plan(plan);
}

/// Remove JOINs whose alias is completely unreferenced in the plan.
/// Catches CROSS JOINs (ON 1=1), spurious node JOINs, etc.
fn remove_unreferenced_joins(plan: &mut RenderPlan) {
    // Collect indices to remove (in reverse order for safe removal)
    let mut to_remove = Vec::new();

    for (idx, join) in plan.joins.0.iter().enumerate().rev() {
        let alias = &join.table_alias;

        // Never remove edge tables — they provide the traversal
        if join.from_id_column.is_some() || join.to_id_column.is_some() {
            continue;
        }

        // Never remove VLP joins
        if join.graph_rel.is_some() {
            continue;
        }

        // Never remove CTE joins
        if is_cte_table(&join.table_name) {
            continue;
        }

        // Never remove joins in fixed path metadata
        if let Some(ref fpm) = plan.fixed_path_info {
            if fpm.node_aliases.contains(alias) {
                continue;
            }
        }

        // Keep if alias is referenced in SELECT/WHERE/ORDER BY/GROUP BY/HAVING/ARRAY JOIN
        if is_alias_referenced_in_plan(plan, alias) {
            continue;
        }

        // Keep if any OTHER join's ON condition or pre_filter references this alias
        // (unlike bridge elimination, we can't rewrite those here)
        let alias_used_in_other_joins = plan.joins.0.iter().enumerate().any(|(i, other)| {
            if i == idx {
                return false;
            }
            for cond in &other.joining_on {
                for operand in &cond.operands {
                    if references_alias(operand, alias) {
                        return true;
                    }
                }
            }
            if let Some(ref pf) = other.pre_filter {
                if references_alias(pf, alias) {
                    return true;
                }
            }
            false
        });

        if alias_used_in_other_joins {
            continue;
        }

        log::debug!(
            "Unreferenced join elimination: removing {} ({})",
            alias,
            join.table_name
        );
        to_remove.push(idx);
    }

    // Remove in reverse index order (already reversed from the loop)
    for idx in &to_remove {
        plan.joins.0.remove(*idx);
    }
}

/// Eliminate bridge nodes within a single plan's join list.
/// Iterates until no more eliminations are found (handles chained bridges).
fn eliminate_bridge_nodes_in_plan(plan: &mut RenderPlan) {
    loop {
        let candidates = find_bridge_candidates(plan);
        if candidates.is_empty() {
            break;
        }

        // Pass 2: Rewrite downstream JOINs to bypass eliminated nodes
        for candidate in &candidates {
            rewrite_joins_for_bridge(
                &mut plan.joins.0,
                &candidate.alias,
                &candidate.id_column,
                &candidate.upstream,
            );
        }

        // Pass 3: Remove eliminated JOINs (indices are in reverse order from find_bridge_candidates)
        for candidate in &candidates {
            plan.joins.0.remove(candidate.join_idx);
        }

        log::debug!(
            "Bridge node elimination: removed {} bridge JOINs",
            candidates.len()
        );
    }
}

/// Pass 1: Identify bridge node candidates by iterating joins in reverse.
/// Returns candidates sorted by descending index (safe for sequential removal).
fn find_bridge_candidates(plan: &RenderPlan) -> Vec<BridgeCandidate> {
    let mut candidates = Vec::new();

    for (idx, join) in plan.joins.0.iter().enumerate().rev() {
        // Guard: must not be an edge table (edge tables have from_id/to_id columns)
        if join.from_id_column.is_some() || join.to_id_column.is_some() {
            continue;
        }

        // Guard: must not be a VLP join
        if join.graph_rel.is_some() {
            continue;
        }

        // Guard: must not have a pre-filter (schema/view filter)
        if join.pre_filter.is_some() {
            continue;
        }

        // Guard: must have exactly one ON condition with Operator::Equal
        if join.joining_on.len() != 1 {
            continue;
        }
        let condition = &join.joining_on[0];
        if condition.operator != Operator::Equal || condition.operands.len() != 2 {
            continue;
        }

        // Guard: both operands must be PropertyAccessExp
        let (left, right) = (&condition.operands[0], &condition.operands[1]);
        let (left_pa, right_pa) = match (left, right) {
            (RenderExpr::PropertyAccessExp(l), RenderExpr::PropertyAccessExp(r)) => (l, r),
            _ => continue,
        };

        // Guard: table_name must not be a CTE reference
        if join.table_name.starts_with("with_")
            || join.table_name.starts_with("vlp_")
            || join.table_name.starts_with("pc_")
        {
            continue;
        }

        // Guard: alias must not be in FixedPathMetadata.node_aliases
        if let Some(ref fpm) = plan.fixed_path_info {
            if fpm.node_aliases.contains(&join.table_alias) {
                continue;
            }
        }

        // Determine which side is "self" (the node being joined) and which is "upstream"
        let (id_column, upstream) = if left_pa.table_alias.0 == join.table_alias {
            // Pattern: node.id = edge.FK
            (
                left_pa.column.raw().to_string(),
                UpstreamRef {
                    alias: right_pa.table_alias.0.clone(),
                    column: right_pa.column.raw().to_string(),
                },
            )
        } else if right_pa.table_alias.0 == join.table_alias {
            // Pattern: edge.FK = node.id
            (
                right_pa.column.raw().to_string(),
                UpstreamRef {
                    alias: left_pa.table_alias.0.clone(),
                    column: left_pa.column.raw().to_string(),
                },
            )
        } else {
            // Neither operand references this join's alias — skip
            continue;
        };

        let alias = &join.table_alias;

        // Guard: alias must not be referenced in SELECT, WHERE, ORDER BY, GROUP BY, HAVING
        if is_alias_referenced_in_plan(plan, alias) {
            continue;
        }

        candidates.push(BridgeCandidate {
            join_idx: idx,
            alias: alias.clone(),
            id_column,
            upstream,
        });
    }

    candidates
}

/// Check if an alias is referenced in non-rewritable parts of the plan
/// (SELECT, WHERE, ORDER BY, GROUP BY, HAVING, ARRAY JOIN).
/// JOIN ON conditions and pre_filters are NOT checked because Pass 2 rewrites them.
fn is_alias_referenced_in_plan(plan: &RenderPlan, alias: &str) -> bool {
    // Check SELECT items
    for item in &plan.select.items {
        if references_alias(&item.expression, alias) {
            return true;
        }
    }

    // Check WHERE clause
    if let Some(ref filter) = plan.filters.0 {
        if references_alias(filter, alias) {
            return true;
        }
    }

    // Check ORDER BY
    for item in &plan.order_by.0 {
        if references_alias(&item.expression, alias) {
            return true;
        }
    }

    // Check GROUP BY
    for expr in &plan.group_by.0 {
        if references_alias(expr, alias) {
            return true;
        }
    }

    // Check HAVING
    if let Some(ref having) = plan.having_clause {
        if references_alias(having, alias) {
            return true;
        }
    }

    // Note: We do NOT check other JOINs' ON conditions or pre_filters here.
    // Those are rewritten in Pass 2 (rewrite_joins_for_bridge), so references
    // to the bridge alias in ON conditions are expected and handled.

    // Check UNION branches (SELECT, WHERE, ORDER BY, GROUP BY only — not JOIN ON conditions)
    if let Some(ref union) = plan.union.0 {
        for branch in &union.input {
            for item in &branch.select.items {
                if references_alias(&item.expression, alias) {
                    return true;
                }
            }
            if let Some(ref filter) = branch.filters.0 {
                if references_alias(filter, alias) {
                    return true;
                }
            }
            for item in &branch.order_by.0 {
                if references_alias(&item.expression, alias) {
                    return true;
                }
            }
            for expr in &branch.group_by.0 {
                if references_alias(expr, alias) {
                    return true;
                }
            }
        }
    }

    // Check ARRAY JOIN expressions
    for aj in &plan.array_join.0 {
        if references_alias(&aj.expression, alias) {
            return true;
        }
    }

    false
}

/// Pass 2: Rewrite downstream JOIN ON conditions to bypass an eliminated bridge node.
/// Replaces `PropertyAccessExp(eliminated_alias, id_col)` with `PropertyAccessExp(upstream_alias, upstream_col)`.
fn rewrite_joins_for_bridge(
    joins: &mut [Join],
    eliminated_alias: &str,
    eliminated_id_col: &str,
    upstream: &UpstreamRef,
) {
    for join in joins.iter_mut() {
        // Rewrite ON conditions
        for cond in join.joining_on.iter_mut() {
            for operand in cond.operands.iter_mut() {
                rewrite_bridge_in_expr(operand, eliminated_alias, eliminated_id_col, upstream);
            }
        }
        // Rewrite pre_filter if present
        if let Some(ref mut pre_filter) = join.pre_filter {
            rewrite_bridge_in_expr(pre_filter, eliminated_alias, eliminated_id_col, upstream);
        }
    }
}

/// Rewrite a single expression: replace references to the eliminated bridge node's
/// ID column with the upstream FK reference.
fn rewrite_bridge_in_expr(
    expr: &mut RenderExpr,
    eliminated_alias: &str,
    eliminated_id_col: &str,
    upstream: &UpstreamRef,
) {
    match expr {
        RenderExpr::PropertyAccessExp(pa) => {
            if pa.table_alias.0 == eliminated_alias && pa.column.raw() == eliminated_id_col {
                *expr = RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(upstream.alias.clone()),
                    column: PropertyValue::Column(upstream.column.clone()),
                });
            }
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in op.operands.iter_mut() {
                rewrite_bridge_in_expr(operand, eliminated_alias, eliminated_id_col, upstream);
            }
        }
        RenderExpr::ScalarFnCall(func) => {
            for arg in func.args.iter_mut() {
                rewrite_bridge_in_expr(arg, eliminated_alias, eliminated_id_col, upstream);
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            for arg in agg.args.iter_mut() {
                rewrite_bridge_in_expr(arg, eliminated_alias, eliminated_id_col, upstream);
            }
        }
        RenderExpr::Case(case) => {
            if let Some(ref mut e) = case.expr {
                rewrite_bridge_in_expr(e, eliminated_alias, eliminated_id_col, upstream);
            }
            for (when, then) in case.when_then.iter_mut() {
                rewrite_bridge_in_expr(when, eliminated_alias, eliminated_id_col, upstream);
                rewrite_bridge_in_expr(then, eliminated_alias, eliminated_id_col, upstream);
            }
            if let Some(ref mut e) = case.else_expr {
                rewrite_bridge_in_expr(e, eliminated_alias, eliminated_id_col, upstream);
            }
        }
        RenderExpr::InSubquery(subq) => {
            rewrite_bridge_in_expr(
                &mut subq.expr,
                eliminated_alias,
                eliminated_id_col,
                upstream,
            );
        }
        RenderExpr::ArraySubscript { array, index } => {
            rewrite_bridge_in_expr(array, eliminated_alias, eliminated_id_col, upstream);
            rewrite_bridge_in_expr(index, eliminated_alias, eliminated_id_col, upstream);
        }
        RenderExpr::ArraySlicing { array, from, to } => {
            rewrite_bridge_in_expr(array, eliminated_alias, eliminated_id_col, upstream);
            if let Some(ref mut f) = from {
                rewrite_bridge_in_expr(f, eliminated_alias, eliminated_id_col, upstream);
            }
            if let Some(ref mut t) = to {
                rewrite_bridge_in_expr(t, eliminated_alias, eliminated_id_col, upstream);
            }
        }
        RenderExpr::List(items) => {
            for item in items.iter_mut() {
                rewrite_bridge_in_expr(item, eliminated_alias, eliminated_id_col, upstream);
            }
        }
        RenderExpr::ReduceExpr(reduce) => {
            rewrite_bridge_in_expr(
                &mut reduce.initial_value,
                eliminated_alias,
                eliminated_id_col,
                upstream,
            );
            rewrite_bridge_in_expr(
                &mut reduce.list,
                eliminated_alias,
                eliminated_id_col,
                upstream,
            );
            rewrite_bridge_in_expr(
                &mut reduce.expression,
                eliminated_alias,
                eliminated_id_col,
                upstream,
            );
        }
        RenderExpr::MapLiteral(entries) => {
            for (_, value) in entries.iter_mut() {
                rewrite_bridge_in_expr(value, eliminated_alias, eliminated_id_col, upstream);
            }
        }
        // Leaf nodes — no rewriting needed
        RenderExpr::TableAlias(_)
        | RenderExpr::Literal(_)
        | RenderExpr::Raw(_)
        | RenderExpr::Star
        | RenderExpr::Column(_)
        | RenderExpr::ColumnAlias(_)
        | RenderExpr::ExistsSubquery(_)
        | RenderExpr::PatternCount(_)
        | RenderExpr::CteEntityRef(_)
        | RenderExpr::Parameter(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render_plan::render_expr::{Column, Operator};
    use crate::render_plan::*;

    /// Helper to create a simple PropertyAccessExp
    fn prop(alias: &str, col: &str) -> RenderExpr {
        RenderExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias(alias.to_string()),
            column: PropertyValue::Column(col.to_string()),
        })
    }

    /// Helper to create an equality ON condition
    fn eq_on(left: RenderExpr, right: RenderExpr) -> OperatorApplication {
        OperatorApplication {
            operator: Operator::Equal,
            operands: vec![left, right],
        }
    }

    /// Helper to create a minimal RenderPlan with given joins and select items
    fn make_plan(joins: Vec<Join>, select_exprs: Vec<RenderExpr>) -> RenderPlan {
        RenderPlan {
            ctes: CteItems(vec![]),
            select: SelectItems {
                items: select_exprs
                    .into_iter()
                    .map(|e| SelectItem {
                        expression: e,
                        col_alias: None,
                    })
                    .collect(),
                distinct: false,
            },
            from: FromTableItem(None),
            joins: JoinItems(joins),
            array_join: ArrayJoinItem(vec![]),
            filters: FilterItems(None),
            group_by: GroupByExpressions(vec![]),
            having_clause: None,
            order_by: OrderByItems(vec![]),
            skip: SkipItem(None),
            limit: LimitItem(None),
            union: UnionItems(None),
            fixed_path_info: None,
            is_multi_label_scan: false,
            variable_registry: None,
        }
    }

    /// Helper to create a node join (no from_id/to_id)
    fn node_join(alias: &str, table: &str, on: Vec<OperatorApplication>) -> Join {
        Join {
            table_name: table.to_string(),
            table_alias: alias.to_string(),
            joining_on: on,
            join_type: JoinType::Left,
            pre_filter: None,
            from_id_column: None,
            to_id_column: None,
            graph_rel: None,
        }
    }

    /// Helper to create an edge join (has from_id/to_id)
    fn edge_join(
        alias: &str,
        table: &str,
        on: Vec<OperatorApplication>,
        from_id: &str,
        to_id: &str,
    ) -> Join {
        Join {
            table_name: table.to_string(),
            table_alias: alias.to_string(),
            joining_on: on,
            join_type: JoinType::Left,
            pre_filter: None,
            from_id_column: Some(from_id.to_string()),
            to_id_column: Some(to_id.to_string()),
            graph_rel: None,
        }
    }

    #[test]
    fn test_simple_bridge_elimination() {
        // Pattern:
        //   edge1(t3) ON t3.MessageId = message1.id
        //   person2   ON person2.id = t3.PersonId         <- BRIDGE
        //   edge2(t4) ON t4.PersonId = person2.id
        //
        // After elimination:
        //   edge1(t3) ON t3.MessageId = message1.id
        //   edge2(t4) ON t4.PersonId = t3.PersonId        <- rewritten

        let joins = vec![
            edge_join(
                "t3",
                "Person_likes_Message",
                vec![eq_on(prop("t3", "MessageId"), prop("message1", "id"))],
                "PersonId",
                "MessageId",
            ),
            node_join(
                "person2",
                "Person",
                vec![eq_on(prop("person2", "id"), prop("t3", "PersonId"))],
            ),
            edge_join(
                "t4",
                "Message_hasCreator_Person",
                vec![eq_on(prop("t4", "PersonId"), prop("person2", "id"))],
                "PersonId",
                "MessageId",
            ),
        ];

        // SELECT only references t3 and t4, not person2
        let select = vec![prop("t3", "PersonId"), prop("t4", "MessageId")];
        let mut plan = make_plan(joins, select);

        eliminate_bridge_nodes_in_plan(&mut plan);

        // person2 should be eliminated
        assert_eq!(plan.joins.0.len(), 2);
        assert_eq!(plan.joins.0[0].table_alias, "t3");
        assert_eq!(plan.joins.0[1].table_alias, "t4");

        // t4's ON condition should now reference t3.PersonId instead of person2.id
        let t4_on = &plan.joins.0[1].joining_on[0];
        if let RenderExpr::PropertyAccessExp(ref pa) = t4_on.operands[1] {
            assert_eq!(pa.table_alias.0, "t3");
            assert_eq!(pa.column.raw(), "PersonId");
        } else {
            panic!("Expected PropertyAccessExp after bridge elimination");
        }
    }

    #[test]
    fn test_no_elimination_when_referenced_in_select() {
        // person2 is referenced in SELECT — should NOT be eliminated
        let joins = vec![
            edge_join(
                "t3",
                "Person_likes_Message",
                vec![eq_on(prop("t3", "MessageId"), prop("message1", "id"))],
                "PersonId",
                "MessageId",
            ),
            node_join(
                "person2",
                "Person",
                vec![eq_on(prop("person2", "id"), prop("t3", "PersonId"))],
            ),
        ];

        let select = vec![prop("person2", "name")];
        let mut plan = make_plan(joins, select);

        eliminate_bridge_nodes_in_plan(&mut plan);

        // person2 should NOT be eliminated
        assert_eq!(plan.joins.0.len(), 2);
    }

    #[test]
    fn test_no_elimination_for_edge_tables() {
        // Edge tables (with from_id/to_id) should never be eliminated
        let joins = vec![edge_join(
            "t3",
            "Person_likes_Message",
            vec![eq_on(prop("t3", "MessageId"), prop("message1", "id"))],
            "PersonId",
            "MessageId",
        )];

        let select = vec![prop("message1", "id")];
        let mut plan = make_plan(joins, select);

        eliminate_bridge_nodes_in_plan(&mut plan);

        assert_eq!(plan.joins.0.len(), 1);
    }

    #[test]
    fn test_chained_bridge_elimination() {
        // Pattern:
        //   edge1(t3) ON t3.MessageId = message1.id
        //   person2   ON person2.id = t3.PersonId         <- BRIDGE
        //   edge2(t4) ON t4.PersonId = person2.id
        //   message2  ON message2.id = t4.MessageId       <- BRIDGE
        //   edge3     ON edge3.MessageId = message2.id
        //
        // After elimination (iterative):
        //   edge1(t3)  ON t3.MessageId = message1.id
        //   edge2(t4)  ON t4.PersonId = t3.PersonId
        //   edge3      ON edge3.MessageId = t4.MessageId

        let joins = vec![
            edge_join(
                "t3",
                "Person_likes_Message",
                vec![eq_on(prop("t3", "MessageId"), prop("message1", "id"))],
                "PersonId",
                "MessageId",
            ),
            node_join(
                "person2",
                "Person",
                vec![eq_on(prop("person2", "id"), prop("t3", "PersonId"))],
            ),
            edge_join(
                "t4",
                "Message_hasCreator_Person",
                vec![eq_on(prop("t4", "PersonId"), prop("person2", "id"))],
                "PersonId",
                "MessageId",
            ),
            node_join(
                "message2",
                "Message",
                vec![eq_on(prop("message2", "id"), prop("t4", "MessageId"))],
            ),
            edge_join(
                "edge3",
                "Person_likes_Message",
                vec![eq_on(prop("edge3", "MessageId"), prop("message2", "id"))],
                "PersonId",
                "MessageId",
            ),
        ];

        let select = vec![prop("edge3", "PersonId")];
        let mut plan = make_plan(joins, select);

        eliminate_bridge_nodes_in_plan(&mut plan);

        // Both person2 and message2 should be eliminated
        assert_eq!(plan.joins.0.len(), 3);
        assert_eq!(plan.joins.0[0].table_alias, "t3");
        assert_eq!(plan.joins.0[1].table_alias, "t4");
        assert_eq!(plan.joins.0[2].table_alias, "edge3");

        // t4 should reference t3.PersonId
        if let RenderExpr::PropertyAccessExp(ref pa) = plan.joins.0[1].joining_on[0].operands[1] {
            assert_eq!(pa.table_alias.0, "t3");
            assert_eq!(pa.column.raw(), "PersonId");
        } else {
            panic!("Expected rewritten t4 ON condition");
        }

        // edge3 should reference t4.MessageId
        if let RenderExpr::PropertyAccessExp(ref pa) = plan.joins.0[2].joining_on[0].operands[1] {
            assert_eq!(pa.table_alias.0, "t4");
            assert_eq!(pa.column.raw(), "MessageId");
        } else {
            panic!("Expected rewritten edge3 ON condition");
        }
    }

    #[test]
    fn test_no_elimination_with_pre_filter() {
        // Node join with pre_filter (schema filter) should NOT be eliminated
        let joins = vec![node_join(
            "person2",
            "Person",
            vec![eq_on(prop("person2", "id"), prop("t3", "PersonId"))],
        )];

        let mut plan = make_plan(joins, vec![]);
        plan.joins.0[0].pre_filter = Some(RenderExpr::Raw("type = 'Person'".to_string()));

        eliminate_bridge_nodes_in_plan(&mut plan);

        // Should NOT be eliminated due to pre_filter
        assert_eq!(plan.joins.0.len(), 1);
    }

    #[test]
    fn test_no_elimination_for_cte_tables() {
        // Joins referencing CTE tables (with_ prefix) should NOT be eliminated
        let joins = vec![node_join(
            "w1",
            "with_cte_1",
            vec![eq_on(prop("w1", "id"), prop("t3", "PersonId"))],
        )];

        let mut plan = make_plan(joins, vec![]);

        eliminate_bridge_nodes_in_plan(&mut plan);

        assert_eq!(plan.joins.0.len(), 1);
    }

    #[test]
    fn test_unreferenced_tail_node_elimination() {
        // A node at the end of a chain that's completely unreferenced should be eliminated
        // Pattern:
        //   edge1(t3) ON t3.MessageId = message1.id
        //   person3   ON person3.id = t3.PersonId     <- UNREFERENCED
        //
        // After: person3 removed entirely

        let joins = vec![
            edge_join(
                "t3",
                "Person_likes_Message",
                vec![eq_on(prop("t3", "MessageId"), prop("message1", "id"))],
                "PersonId",
                "MessageId",
            ),
            node_join(
                "person3",
                "Person",
                vec![eq_on(prop("person3", "id"), prop("t3", "PersonId"))],
            ),
        ];

        let select = vec![prop("t3", "PersonId")];
        let mut plan = make_plan(joins, select);

        eliminate_bridge_nodes_in_plan(&mut plan);

        // person3 should be eliminated (unreferenced)
        assert_eq!(plan.joins.0.len(), 1);
        assert_eq!(plan.joins.0[0].table_alias, "t3");
    }
}
