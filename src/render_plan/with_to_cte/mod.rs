//! WITH → CTE builders and their orbit (the "entangled core").
//!
//! P2.6 (`REFACTORING_SAFETY_PLAN.md` §5.1) moves the two giant WITH→CTE
//! builders (`build_chained_with_match_cte_plan`,
//! `replace_with_clause_with_cte_reference_v2`) plus their WITH-discovery /
//! pruning orbit out of `plan_builder_utils.rs` into this module, one
//! byte-identical sub-slice per PR. Decomposing the giants into smaller
//! functions is Phase 4 (§7.1) — NOT done here.
//!
//! Layout note: everything lands in this single `mod.rs` (a direct child of
//! `render_plan`, same module depth as `plan_builder_utils`), so the moved
//! bodies' `super::…` / `super::super::…` path-expressions resolve
//! byte-identically. A later Phase-4 decomposition may split this into
//! sub-files (which is a logic-edit slice, not a move).
//!
//! Extracted so far (P2.6 slice 1): the #529 shape-1 loud-guard property
//! helpers `table_role_dependent_property_names` + `collect_property_accesses`
//! (with their unit tests). Both are re-exported `pub(crate)` from
//! `plan_builder_utils` during the transition so the giant builder's call
//! sites keep resolving.

use crate::graph_catalog::GraphSchema;
use crate::render_plan::render_expr::RenderExpr;
// Types referenced by bare name from the `#[cfg(test)]` module below via its
// `use super::*` — kept here so that glob keeps resolving after the move.
#[cfg(test)]
use crate::render_plan::render_expr::{Literal, Operator, OperatorApplication, RenderCase};
#[cfg(test)]
use crate::render_plan::{
    ArrayJoinItem, CteItems, FilterItems, FromTableItem, GroupByExpressions, JoinItems, LimitItem,
    OrderByItems, RenderPlan, SelectItems, SkipItem, UnionItems,
};

/// #529 shape 1, bug 3 (loud guard — NOT a fix): an undirected self-
/// referencing edge over a DENORMALIZED/embedded node (no separate physical
/// node table — identity is a role-dependent column embedded on the SAME
/// row as the relationship, e.g. Zeek's `IP` node on `conn_log` via
/// `id.orig_h`/`id.resp_h`) needs its per-role UNION branches to alternate
/// which embedded column identifies the anchor in each branch. That
/// alternation is NOT implemented — `find_id_column_for_alias`
/// (`render_plan/plan_builder.rs`) resolves a node's identity via the SAME
/// static `NodeSchema::node_id` field regardless of which role (from or to)
/// the node plays in a given direction branch, and `bidirectional_union.rs`'s
/// per-branch `column_swaps` map (built specifically to correct this for
/// Incoming-direction denormalized branches) is only ever threaded through
/// `Projection` items, never `WithClause` items — so it never reaches a bare
/// WITH-aggregate's pass-through/aggregate columns. The live CTE builder
/// (`build_chained_with_match_cte_plan`) compounds this: once a WITH
/// segment's pattern renders as a `Union` of 2+ direction branches (the
/// "non-denormalized-union" path — see its own doc comment there), it
/// attaches the WITH's projection (`select_items`, e.g. `a`, `count(r) AS c`)
/// ONLY to the base branch (`rendered.select`) and never to the other UNION
/// branches — `to_sql_query.rs`'s aggregate-UNION renderer then deliberately
/// reuses that ONE shared `plan.select` for every branch's inner SELECT
/// (`render_union_branch_sql` shows this codebase DOES have a working
/// per-branch-select convention elsewhere; the aggregate-UNION path just
/// doesn't use it). So every UNION branch ends up projecting the IDENTICAL
/// column, silently dropping rows that only ever appear in the un-selected
/// role and inflating counts for rows appearing in both roles (live-verified
/// against the zeek fixture:
/// `MATCH (a:IP)-[r:ACCESSED]-(b:IP) WITH a, count(r) AS c RETURN a.ip, c`
/// returns 3 wrong rows — missing `10.0.0.99` and `142.250.80.46` entirely,
/// and double-counting `93.184.216.34` — vs. the true 5 distinct IPs with
/// `count`s `{10.0.0.99: 1, 142.250.80.46: 1, 192.168.1.10: 1,
/// 192.168.1.20: 1, 93.184.216.34: 2}`).
///
/// Detects the schema-level precondition for this unfixed case: the set of
/// Cypher property names that are role-dependent (their physical column
/// genuinely differs between from/to role — see `NodeSchema::
/// role_dependent_property_names`, the canonical schema-catalog accessor
/// this routes through) for any node backed by `table`. A NORMALIZED
/// self-referencing edge (e.g. `(p:Person)-[:KNOWS]-(p2:Person)` via a
/// separate node table) is deliberately UNAFFECTED and must not trigger
/// this: its identity column is the same real physical ID regardless of
/// role, so sharing one SELECT across UNION branches there is correct, not
/// a bug — the accessor returns an empty set immediately for a normal node
/// schema.
///
/// Checking PROPERTY NAMES (not just "does this table back a role-dependent
/// node at all") matters: in a coupled/embedded schema, a relationship alias
/// (e.g. `r`) and a node alias (e.g. `a`) can share the exact same physical
/// table, but the relationship's OWN properties (e.g. `r`'s edge_id `uid`)
/// are role-INDEPENDENT even though the table also backs a role-dependent
/// node — flagging any reference to that TABLE regardless of which property
/// is accessed was an earlier draft's false positive, caught by
/// `undirected_optional_with_aggregate_coupled_anchor_group_by_529`.
// P2.6 slice 1: widened `fn` → `pub(crate) fn` (the sole change vs the
// original) so the transition re-export in `plan_builder_utils` reaches it;
// body is verbatim.
pub(crate) fn table_role_dependent_property_names(
    schema: &GraphSchema,
    table: &str,
) -> std::collections::HashSet<String> {
    schema
        .all_node_schemas()
        .values()
        .filter(|ns| ns.full_table_name() == table)
        .flat_map(|ns| ns.role_dependent_identifiers())
        .collect()
}

/// #529 (R6, post-adversarial-review): recursively collect
/// `(table_alias, property_name)` pairs from every `PropertyAccessExp`
/// reachable within `expr` — used by the #529 shape-1 loud guard to check
/// exactly which properties (not just which aliases) a WITH projection
/// references, since alias-level checking alone conflates a role-dependent
/// node's properties with a same-table relationship's own, role-independent
/// ones (see `table_role_dependent_property_names`'s doc comment).
///
/// R6 history: the ORIGINAL version of this function hand-rolled a partial
/// `match` covering only `PropertyAccessExp`/`OperatorApplicationExp`/
/// `AggregateFnCall`/`ScalarFnCall`/`Case`'s `when_then`+`else_expr`, with a
/// `_ => {}` catch-all silently dropping everything else — including
/// `List`, `MapLiteral`, `ArraySubscript`/`ArraySlicing`, `ReduceExpr`,
/// `InSubquery`, and `Case`'s own `expr` field (simple-CASE). Adversarial
/// review found this exploitable and non-theoretical: `WITH count(r) AS c,
/// [a.ip] AS tags RETURN c, tags` on the coupled zeek schema rendered and
/// executed with NO guard firing — the exact same non-alternating-branch
/// corruption, just reached through a `[a.ip]` list literal instead of a
/// bare `a.ip` select item.
///
/// Fixed by making this match EXHAUSTIVE over every `RenderExpr` variant —
/// no `_` catch-all, so the compiler forces this function to be updated
/// whenever a new variant is added, closing off the whole "one more shape
/// slips through" bug class rather than patching individual instances of
/// it. Deliberately mirrors (and is verified against) `references_alias`
/// (`render_plan/expression_utils.rs`), this codebase's existing exhaustive
/// `RenderExpr` walker for "does this expression reference alias X" — with
/// one correction: `references_alias`'s own `Case` arm ALSO never checks
/// `RenderCase.expr` (the scrutinee of a simple `CASE x WHEN ...`, as
/// opposed to a searched `CASE WHEN cond ...`) — a latent gap in that
/// function too, found while auditing this one arm-for-arm. Fixed HERE
/// (this function checks `case_expr.expr`) since a simple-CASE repro is
/// exactly what this round's verification requires; `references_alias`
/// itself is used by several other call sites with unverified blast
/// radius, so correcting its own gap is left as a follow-up rather than
/// bundled into this fix.
// P2.6 slice 1: widened `fn` → `pub(crate) fn` (the sole change vs the
// original) so the transition re-export in `plan_builder_utils` reaches it;
// body is verbatim.
pub(crate) fn collect_property_accesses<'a>(
    expr: &'a RenderExpr,
    out: &mut Vec<(&'a str, &'a str)>,
) {
    match expr {
        RenderExpr::PropertyAccessExp(pa) => {
            out.push((pa.table_alias.0.as_str(), pa.column.raw()));
        }
        RenderExpr::OperatorApplicationExp(op) => {
            for operand in &op.operands {
                collect_property_accesses(operand, out);
            }
        }
        RenderExpr::AggregateFnCall(agg) => {
            for arg in &agg.args {
                collect_property_accesses(arg, out);
            }
        }
        RenderExpr::ScalarFnCall(f) => {
            for arg in &f.args {
                collect_property_accesses(arg, out);
            }
        }
        RenderExpr::List(exprs) => {
            for e in exprs {
                collect_property_accesses(e, out);
            }
        }
        RenderExpr::Case(c) => {
            // `expr` is the scrutinee of a simple `CASE x WHEN v1 THEN ...`
            // (`None` for a searched `CASE WHEN cond THEN ...`) — missing
            // from the original partial match (and from `references_alias`
            // too, see doc comment above).
            if let Some(scrutinee) = &c.expr {
                collect_property_accesses(scrutinee, out);
            }
            for (when, then) in &c.when_then {
                collect_property_accesses(when, out);
                collect_property_accesses(then, out);
            }
            if let Some(else_expr) = &c.else_expr {
                collect_property_accesses(else_expr, out);
            }
        }
        RenderExpr::InSubquery(subquery) => {
            collect_property_accesses(&subquery.expr, out);
        }
        RenderExpr::ReduceExpr(reduce) => {
            collect_property_accesses(&reduce.initial_value, out);
            collect_property_accesses(&reduce.list, out);
            collect_property_accesses(&reduce.expression, out);
        }
        RenderExpr::MapLiteral(entries) => {
            for (_, v) in entries {
                collect_property_accesses(v, out);
            }
        }
        RenderExpr::ArraySubscript { array, index } => {
            collect_property_accesses(array, out);
            collect_property_accesses(index, out);
        }
        RenderExpr::ArraySlicing { array, from, to } => {
            collect_property_accesses(array, out);
            if let Some(f) = from {
                collect_property_accesses(f, out);
            }
            if let Some(t) = to {
                collect_property_accesses(t, out);
            }
        }
        // EXISTS subqueries and pre-rendered pattern-count SQL are
        // self-contained — no outer-scope alias/property reference to
        // collect (mirrors `references_alias`'s treatment of the same two
        // variants).
        RenderExpr::ExistsSubquery(_) => {}
        RenderExpr::PatternCount(_) => {}
        // Raw SQL text — opaque, can't reliably extract (alias, property)
        // pairs from arbitrary text (mirrors `references_alias`, which only
        // does a best-effort substring check here since it just needs a
        // bool, not a structured property name).
        RenderExpr::Raw(_) => {}
        // A bare alias reference (e.g. whole-node `RETURN a`, or `count(a)`
        // before normalization) has no property name attached — nothing to
        // collect as a (alias, property) pair.
        RenderExpr::TableAlias(_) => {}
        // CteEntityRef references CTE columns, not a source alias/property.
        RenderExpr::CteEntityRef(_) => {}
        // No sub-expressions to recurse into.
        RenderExpr::Literal(_)
        | RenderExpr::Star
        | RenderExpr::ColumnAlias(_)
        | RenderExpr::Column(_)
        | RenderExpr::Parameter(_) => {}
    }
}

/// #529 R6: per-variant coverage tests for `collect_property_accesses`,
/// directly exercising the exact shapes adversarial review found (and
/// verified) slip past a partial `match` — a list literal, map literal,
/// array subscript/slicing, simple-CASE's `expr` scrutinee, and
/// `ReduceExpr`/`InSubquery`. Unit-testing the function directly (rather
/// than only fishing for Cypher syntax that happens to reach it) is the
/// more reliable check: some of these `RenderExpr` variants are hard or
/// impossible to trigger from real Cypher syntax specifically INSIDE a
/// WITH-clause projection item (e.g. `InSubquery` is normally a WHERE-level
/// construct), but the guard's correctness depends on the traversal
/// function itself being exhaustive regardless of which caller reaches it.
#[cfg(test)]
mod collect_property_accesses_tests {
    use super::*;
    use crate::graph_catalog::expression_parser::PropertyValue;
    use crate::render_plan::render_expr::{ColumnAlias, PropertyAccess, TableAlias};

    fn prop(alias: &str, col: &str) -> RenderExpr {
        RenderExpr::PropertyAccessExp(PropertyAccess {
            table_alias: TableAlias(alias.to_string()),
            column: PropertyValue::Column(col.to_string()),
        })
    }

    fn minimal_render_plan() -> RenderPlan {
        RenderPlan {
            ctes: CteItems(vec![]),
            select: SelectItems {
                items: vec![],
                distinct: false,
            },
            from: FromTableItem(None),
            joins: JoinItems(vec![]),
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

    fn collect(expr: &RenderExpr) -> Vec<(&str, &str)> {
        let mut out = Vec::new();
        collect_property_accesses(expr, &mut out);
        out
    }

    #[test]
    fn list_literal_recurses_into_elements() {
        let expr = RenderExpr::List(vec![prop("a", "ip"), prop("b", "port")]);
        assert_eq!(collect(&expr), vec![("a", "ip"), ("b", "port")]);
    }

    #[test]
    fn map_literal_recurses_into_values() {
        let expr = RenderExpr::MapLiteral(vec![
            ("k1".to_string(), prop("a", "ip")),
            ("k2".to_string(), prop("b", "port")),
        ]);
        assert_eq!(collect(&expr), vec![("a", "ip"), ("b", "port")]);
    }

    #[test]
    fn array_subscript_recurses_into_array_and_index() {
        let expr = RenderExpr::ArraySubscript {
            array: Box::new(prop("a", "ip")),
            index: Box::new(prop("b", "idx")),
        };
        assert_eq!(collect(&expr), vec![("a", "ip"), ("b", "idx")]);
    }

    #[test]
    fn array_slicing_recurses_into_array_from_and_to() {
        let expr = RenderExpr::ArraySlicing {
            array: Box::new(prop("a", "ip")),
            from: Some(Box::new(prop("b", "lo"))),
            to: Some(Box::new(prop("c", "hi"))),
        };
        assert_eq!(collect(&expr), vec![("a", "ip"), ("b", "lo"), ("c", "hi")]);
    }

    #[test]
    fn array_slicing_with_no_bounds_still_recurses_into_array() {
        let expr = RenderExpr::ArraySlicing {
            array: Box::new(prop("a", "ip")),
            from: None,
            to: None,
        };
        assert_eq!(collect(&expr), vec![("a", "ip")]);
    }

    #[test]
    fn simple_case_recurses_into_expr_scrutinee() {
        // `CASE a.ip WHEN 'x' THEN b.port ELSE c.name END` — the scrutinee
        // `a.ip` (the `expr` field) is exactly the field the pre-R6 guard
        // (and `references_alias`, incidentally) never checked.
        let expr = RenderExpr::Case(RenderCase {
            expr: Some(Box::new(prop("a", "ip"))),
            when_then: vec![(
                RenderExpr::Literal(Literal::String("x".to_string())),
                prop("b", "port"),
            )],
            else_expr: Some(Box::new(prop("c", "name"))),
        });
        let found = collect(&expr);
        assert!(
            found.contains(&("a", "ip")),
            "simple-CASE scrutinee (expr field) must be collected: {found:?}"
        );
        assert!(found.contains(&("b", "port")));
        assert!(found.contains(&("c", "name")));
    }

    #[test]
    fn searched_case_recurses_into_when_then_and_else() {
        let expr = RenderExpr::Case(RenderCase {
            expr: None,
            when_then: vec![(prop("a", "flag"), prop("b", "port"))],
            else_expr: Some(Box::new(prop("c", "name"))),
        });
        let found = collect(&expr);
        assert!(found.contains(&("a", "flag")));
        assert!(found.contains(&("b", "port")));
        assert!(found.contains(&("c", "name")));
    }

    #[test]
    fn reduce_expr_recurses_into_initial_value_list_and_expression() {
        let expr = RenderExpr::ReduceExpr(super::super::render_expr::ReduceExpr {
            accumulator: "acc".to_string(),
            initial_value: Box::new(prop("a", "init")),
            variable: "x".to_string(),
            list: Box::new(prop("b", "items")),
            expression: Box::new(prop("c", "step")),
        });
        let found = collect(&expr);
        assert!(found.contains(&("a", "init")));
        assert!(found.contains(&("b", "items")));
        assert!(found.contains(&("c", "step")));
    }

    #[test]
    fn in_subquery_recurses_into_expr() {
        let expr = RenderExpr::InSubquery(super::super::render_expr::InSubquery {
            expr: Box::new(prop("a", "ip")),
            subplan: Box::new(minimal_render_plan()),
        });
        assert_eq!(collect(&expr), vec![("a", "ip")]);
    }

    #[test]
    fn operator_application_still_recurses() {
        let expr = RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Equal,
            operands: vec![prop("a", "ip"), prop("b", "ip")],
        });
        assert_eq!(collect(&expr), vec![("a", "ip"), ("b", "ip")]);
    }

    #[test]
    fn nested_list_of_case_of_map_all_recurse() {
        // A deliberately nested combination: List[ Case[ Map{ .. } ] ] — if
        // any single arm silently drops instead of recursing, this loses
        // the innermost access.
        let inner_map = RenderExpr::MapLiteral(vec![("k".to_string(), prop("deep", "prop"))]);
        let case_expr = RenderExpr::Case(RenderCase {
            expr: None,
            when_then: vec![(RenderExpr::Literal(Literal::Boolean(true)), inner_map)],
            else_expr: None,
        });
        let list_expr = RenderExpr::List(vec![case_expr]);
        assert_eq!(collect(&list_expr), vec![("deep", "prop")]);
    }

    #[test]
    fn leaf_variants_with_no_references_collect_nothing() {
        for expr in [
            RenderExpr::Literal(Literal::Integer(1)),
            RenderExpr::Star,
            RenderExpr::ColumnAlias(ColumnAlias("x".to_string())),
            RenderExpr::Column(crate::render_plan::render_expr::Column(
                PropertyValue::Column("x".to_string()),
            )),
            RenderExpr::Parameter("p".to_string()),
            RenderExpr::TableAlias(TableAlias("a".to_string())),
            RenderExpr::Raw("a.foo".to_string()),
        ] {
            assert!(
                collect(&expr).is_empty(),
                "expected no property accesses collected from {expr:?}"
            );
        }
    }
}
