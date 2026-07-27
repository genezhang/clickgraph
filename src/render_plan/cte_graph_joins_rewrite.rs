//! Logical-plan CTE-reference rewriting — updates `GraphJoins`/`GraphRel`
//! `cte_references`, anchor tables, and join `table_name`s after WITH→CTE
//! finalization, plus the `LogicalExpr` alias/column rewriter and the fresh-scan
//! / with-exported alias walkers it relies on.
//!
//! Extracted verbatim from `plan_builder_utils.rs` in P2.5 (sub-slice D,
//! `REFACTORING_SAFETY_PLAN.md` §5.1). No logic edits — `update_graph_joins_cte_refs`
//! is re-exported `pub(crate)` from `plan_builder_utils` during the transition
//! (its 2 in-module call sites); the other four functions are called only within
//! this module, so they stay private here.
//!
//! Scope note: this is the LogicalPlan-level companion to `cte_rewrite.rs` (which
//! handles the RenderExpr/RenderPlan level). Kept as a separate module because it
//! operates on `query_planner::logical_expr`/`logical_plan` types, not render
//! types. The `cte_property_mappings` parameter threaded through here is the M3
//! scope map passed BY VALUE (not the F2a-deleted task-local) — P-4's F2b will
//! reconcile M3, but that is a logic change tracked separately; this slice only
//! relocates.

use crate::query_planner::logical_plan::LogicalPlan;
use crate::render_plan::errors::RenderBuildError;

type RenderPlanBuilderResult<T> = Result<T, RenderBuildError>;

/// Helper: Rewrite LogicalExpr to update PropertyAccessExp table aliases with updated CTE names
fn rewrite_logical_expr_cte_refs(
    expr: &crate::query_planner::logical_expr::LogicalExpr,
    cte_references: &std::collections::HashMap<String, String>,
    cte_property_mappings: &std::collections::HashMap<
        String,
        std::collections::HashMap<String, String>,
    >,
) -> crate::query_planner::logical_expr::LogicalExpr {
    use crate::query_planner::logical_expr::LogicalExpr;

    match expr {
        LogicalExpr::PropertyAccessExp(prop) => {
            // Check if the table_alias references an old CTE name that needs updating
            if let Some(new_cte_name) = cte_references.get(&prop.table_alias.0) {
                // Also resolve the column name to the CTE column name if mapping exists
                let resolved_column =
                    cte_property_mappings
                        .get(&prop.table_alias.0)
                        .and_then(|mapping| {
                            let prop_name = match &prop.column {
                            crate::graph_catalog::expression_parser::PropertyValue::Column(c) => {
                                c.as_str()
                            }
                            crate::graph_catalog::expression_parser::PropertyValue::Expression(
                                e,
                            ) => e.as_str(),
                        };
                            mapping.get(prop_name).cloned()
                        });

                let new_column = if let Some(ref cte_col) = resolved_column {
                    log::info!(
                        "🔧 rewrite_logical_expr_cte_refs: {}.{} → {}.{} (alias + column resolved)",
                        prop.table_alias.0,
                        prop.column.raw(),
                        new_cte_name,
                        cte_col
                    );
                    crate::graph_catalog::expression_parser::PropertyValue::Column(cte_col.clone())
                } else {
                    log::info!(
                        "🔧 rewrite_logical_expr_cte_refs: Updating table_alias '{}' → '{}' (column '{}' not in CTE mapping)",
                        prop.table_alias.0,
                        new_cte_name,
                        prop.column.raw()
                    );
                    prop.column.clone()
                };

                LogicalExpr::PropertyAccessExp(crate::query_planner::logical_expr::PropertyAccess {
                    table_alias: crate::query_planner::logical_expr::TableAlias(
                        new_cte_name.clone(),
                    ),
                    column: new_column,
                })
            } else {
                expr.clone()
            }
        }
        LogicalExpr::OperatorApplicationExp(op) => {
            let new_operands: Vec<_> = op
                .operands
                .iter()
                .map(|operand| {
                    rewrite_logical_expr_cte_refs(operand, cte_references, cte_property_mappings)
                })
                .collect();
            LogicalExpr::OperatorApplicationExp(
                crate::query_planner::logical_expr::OperatorApplication {
                    operator: op.operator,
                    operands: new_operands,
                },
            )
        }
        LogicalExpr::ScalarFnCall(func) => {
            let new_args: Vec<_> = func
                .args
                .iter()
                .map(|arg| {
                    rewrite_logical_expr_cte_refs(arg, cte_references, cte_property_mappings)
                })
                .collect();
            LogicalExpr::ScalarFnCall(crate::query_planner::logical_expr::ScalarFnCall {
                name: func.name.clone(),
                args: new_args,
            })
        }
        LogicalExpr::AggregateFnCall(agg) => {
            let new_args: Vec<_> = agg
                .args
                .iter()
                .map(|arg| {
                    rewrite_logical_expr_cte_refs(arg, cte_references, cte_property_mappings)
                })
                .collect();
            LogicalExpr::AggregateFnCall(crate::query_planner::logical_expr::AggregateFnCall {
                name: agg.name.clone(),
                args: new_args,
            })
        }
        LogicalExpr::List(items) => {
            let new_items: Vec<_> = items
                .iter()
                .map(|item| {
                    rewrite_logical_expr_cte_refs(item, cte_references, cte_property_mappings)
                })
                .collect();
            LogicalExpr::List(new_items)
        }
        // Other expression types don't contain PropertyAccessExp, so clone as-is
        _ => expr.clone(),
    }
}

/// Find aliases that are fresh table scans (GraphNode → ViewScan) in a plan tree.
/// Used to filter CTE references when propagating into inner scopes — fresh scans
/// should use raw table columns, not CTE column names.
fn find_fresh_table_scan_aliases_in_plan(plan: &LogicalPlan) -> std::collections::HashSet<String> {
    let mut aliases = std::collections::HashSet::new();
    collect_fresh_scan_aliases(plan, &mut aliases);
    aliases
}

fn collect_fresh_scan_aliases(plan: &LogicalPlan, aliases: &mut std::collections::HashSet<String>) {
    match plan {
        LogicalPlan::GraphNode(gn) => {
            if matches!(gn.input.as_ref(), LogicalPlan::ViewScan(_)) {
                aliases.insert(gn.alias.clone());
            }
            collect_fresh_scan_aliases(&gn.input, aliases);
        }
        LogicalPlan::GraphRel(gr) => {
            collect_fresh_scan_aliases(&gr.left, aliases);
            collect_fresh_scan_aliases(&gr.right, aliases);
        }
        LogicalPlan::Projection(p) => collect_fresh_scan_aliases(&p.input, aliases),
        LogicalPlan::Filter(f) => collect_fresh_scan_aliases(&f.input, aliases),
        LogicalPlan::GroupBy(gb) => collect_fresh_scan_aliases(&gb.input, aliases),
        LogicalPlan::OrderBy(ob) => collect_fresh_scan_aliases(&ob.input, aliases),
        LogicalPlan::CartesianProduct(cp) => {
            collect_fresh_scan_aliases(&cp.left, aliases);
            collect_fresh_scan_aliases(&cp.right, aliases);
        }
        LogicalPlan::Unwind(uw) => collect_fresh_scan_aliases(&uw.input, aliases),
        LogicalPlan::GraphJoins(gj) => collect_fresh_scan_aliases(&gj.input, aliases),
        LogicalPlan::Skip(s) => collect_fresh_scan_aliases(&s.input, aliases),
        LogicalPlan::Limit(l) => collect_fresh_scan_aliases(&l.input, aliases),
        LogicalPlan::WithClause(_) => {} // Stop at WITH boundary
        _ => {}
    }
}

/// Collect every alias exported by a `WithClause` anywhere within `plan`
/// (#517). Used by `update_graph_joins_cte_refs`'s Union arm to scope a
/// CTE-reference lookahead to a single Cypher UNION branch — see the
/// comment there for why an unscoped lookup leaks across arms.
fn with_exported_aliases_in_branch(plan: &LogicalPlan) -> std::collections::HashSet<String> {
    use crate::query_planner::logical_plan::LogicalPlan as LP;
    let mut out = std::collections::HashSet::new();
    fn walk(plan: &LP, out: &mut std::collections::HashSet<String>) {
        match plan {
            LP::WithClause(wc) => {
                out.extend(wc.exported_aliases.iter().cloned());
                walk(&wc.input, out);
            }
            LP::GraphNode(n) => walk(&n.input, out),
            LP::GraphRel(r) => {
                walk(&r.left, out);
                walk(&r.center, out);
                walk(&r.right, out);
            }
            LP::Projection(p) => walk(&p.input, out),
            LP::Filter(f) => walk(&f.input, out),
            LP::GroupBy(gb) => walk(&gb.input, out),
            LP::OrderBy(ob) => walk(&ob.input, out),
            LP::GraphJoins(gj) => walk(&gj.input, out),
            LP::Limit(l) => walk(&l.input, out),
            LP::Skip(s) => walk(&s.input, out),
            LP::CartesianProduct(cp) => {
                walk(&cp.left, out);
                walk(&cp.right, out);
            }
            LP::Union(u) => {
                for input in &u.inputs {
                    walk(input, out);
                }
            }
            LP::ViewScan(vs) => {
                if let Some(input) = &vs.input {
                    walk(input, out);
                }
            }
            LP::Cte(c) => walk(&c.input, out),
            LP::Unwind(u) => walk(&u.input, out),
            _ => {}
        }
    }
    walk(plan, &mut out);
    out
}

pub(crate) fn update_graph_joins_cte_refs(
    plan: &LogicalPlan,
    cte_references: &std::collections::HashMap<String, String>,
    cte_property_mappings: &std::collections::HashMap<
        String,
        std::collections::HashMap<String, String>,
    >,
) -> RenderPlanBuilderResult<LogicalPlan> {
    use crate::query_planner::logical_plan::*;
    use std::sync::Arc;

    match plan {
        LogicalPlan::GraphJoins(gj) => {
            log::info!(
                "🔧 update_graph_joins_cte_refs: Updating GraphJoins.cte_references from {:?} to {:?}",
                gj.cte_references,
                cte_references
            );

            let new_input =
                update_graph_joins_cte_refs(&gj.input, cte_references, cte_property_mappings)?;

            // CRITICAL FIX: Update anchor_table considering WITH clause scope barriers
            // Problem: After WITH clauses, only exported variables remain in scope.
            // The anchor_table may reference a variable that's no longer accessible (scope barrier violation).
            //
            // Solution Strategy:
            // 1. If NO cte_references → no WITH clauses, keep anchor as-is (no scope barriers)
            // 2. If anchor_table is in cte_references → it's valid, keep it
            // 3. If anchor_table is NOT in cte_references → scope violation, try to find replacement:
            //    a. Look for a join whose table_alias IS in cte_references (visible variable)
            //    b. Pick the first such join as the new anchor
            //    c. If no valid replacement found, set to None (FROM will be determined from joins)
            let new_anchor_table = if cte_references.is_empty() {
                // No CTE references means no WITH clauses - keep anchor unchanged
                log::debug!("🔧 update_graph_joins_cte_refs: No CTE references, keeping anchor_table as-is: {:?}", gj.anchor_table);
                gj.anchor_table.clone()
            } else if let Some(ref anchor) = gj.anchor_table {
                if cte_references.contains_key(anchor) {
                    // Anchor IS in cte_references - it's a valid variable in current scope
                    log::info!(
                        "🔧 update_graph_joins_cte_refs: anchor_table '{}' is in scope (cte_references: {:?})",
                        anchor,
                        cte_references.keys().collect::<Vec<_>>()
                    );
                    Some(anchor.clone())
                } else {
                    // Anchor NOT in cte_references — check if it's a valid variable
                    // from the current MATCH clause (not a scope-violated WITH variable).
                    // If anchor matches a join's table_alias, it's a new variable from the
                    // second MATCH and should be kept.
                    let anchor_in_joins = gj.joins.iter().any(|j| &j.table_alias == anchor);
                    if anchor_in_joins {
                        log::info!(
                            "🔧 update_graph_joins_cte_refs: anchor_table '{}' not in CTE scope but exists in joins — keeping as valid new variable",
                            anchor,
                        );
                        Some(anchor.clone())
                    } else {
                        // Anchor NOT in joins either — scope violation
                        log::debug!(
                            "🔧 update_graph_joins_cte_refs: anchor_table '{}' NOT in scope. \
                             Scope barrier violation! Available CTEs: {:?}",
                            anchor,
                            cte_references.keys().collect::<Vec<_>>()
                        );

                        // Search joins for a valid anchor (table_alias must be in cte_references)
                        let replacement_anchor = gj.joins.iter()
                            .find(|j| cte_references.contains_key(&j.table_alias))
                            .map(|j| {
                                log::info!(
                                    "🔧 update_graph_joins_cte_refs: Found replacement anchor '{}' from joins",
                                    j.table_alias
                                );
                                j.table_alias.clone()
                            });

                        if replacement_anchor.is_none() {
                            log::debug!(
                                "🔧 update_graph_joins_cte_refs: No valid replacement anchor found in joins. \
                                 Setting to None (will be determined during extraction)."
                            );
                        }

                        replacement_anchor
                    }
                }
            } else {
                None
            };

            // 🔧 FIX: Update Join.table_name for CTEs in the joins array
            // When a CTE is finalized during rendering (e.g., "with_user_obj_cte" → "with_user_obj_cte_1"),
            // we need to update the table_name in joins that reference it.
            let updated_joins: Vec<_> = gj.joins.iter().map(|j| {
                // Check if this join's table_alias references a CTE with an updated name
                if let Some(new_cte_name) = cte_references.get(&j.table_alias) {
                    // Check if the table_name needs updating (it's a CTE reference)
                    // CTE table names don't have database prefix, regular tables do
                    if !j.table_name.contains('.') && &j.table_name != new_cte_name {
                        log::info!(
                            "🔧 update_graph_joins_cte_refs: Updating Join.table_name '{}' → '{}' for alias '{}'",
                            j.table_name,
                            new_cte_name,
                            j.table_alias
                        );
                        let mut updated_join = j.clone();
                        updated_join.table_name = new_cte_name.clone();
                        return updated_join;
                    }
                }
                j.clone()
            }).collect();

            Ok(LogicalPlan::GraphJoins(GraphJoins {
                input: Arc::new(new_input),
                joins: updated_joins,
                optional_aliases: gj.optional_aliases.clone(),
                anchor_table: new_anchor_table,
                cte_references: cte_references.clone(), // UPDATE HERE!
                correlation_predicates: gj.correlation_predicates.clone(),
            }))
        }
        LogicalPlan::GraphRel(gr) => {
            log::info!(
                "🔧 update_graph_joins_cte_refs: Updating GraphRel.cte_references from {:?} to {:?}",
                gr.cte_references,
                cte_references
            );

            // Recursively update children
            let new_left =
                update_graph_joins_cte_refs(&gr.left, cte_references, cte_property_mappings)?;
            let new_center =
                update_graph_joins_cte_refs(&gr.center, cte_references, cte_property_mappings)?;
            let new_right =
                update_graph_joins_cte_refs(&gr.right, cte_references, cte_property_mappings)?;

            Ok(LogicalPlan::GraphRel(GraphRel {
                left: Arc::new(new_left),
                center: Arc::new(new_center),
                right: Arc::new(new_right),
                cte_references: cte_references.clone(), // UPDATE HERE!
                ..gr.clone()
            }))
        }
        LogicalPlan::Projection(proj) => {
            let new_input =
                update_graph_joins_cte_refs(&proj.input, cte_references, cte_property_mappings)?;

            // 🔧 FIX: Update PropertyAccessExp expressions in projection items with updated CTE names
            let updated_items: Vec<_> = proj
                .items
                .iter()
                .map(|item| {
                    let updated_expr = rewrite_logical_expr_cte_refs(
                        &item.expression,
                        cte_references,
                        cte_property_mappings,
                    );
                    crate::query_planner::logical_plan::ProjectionItem {
                        expression: updated_expr,
                        col_alias: item.col_alias.clone(),
                    }
                })
                .collect();

            Ok(LogicalPlan::Projection(Projection {
                input: Arc::new(new_input),
                items: updated_items,
                distinct: proj.distinct,
                pattern_comprehensions: proj.pattern_comprehensions.clone(),
            }))
        }
        LogicalPlan::WithClause(wc) => {
            // CRITICAL: Filter CTE references for inner scope.
            // Aliases that are fresh table scans (GraphNode → ViewScan) in the inner scope
            // should NOT inherit outer CTE references. Otherwise, join conditions for fresh
            // scans get rewritten to use CTE column names (e.g., country.p7_country_id
            // instead of country.id), causing resolution failures.
            let fresh_aliases = find_fresh_table_scan_aliases_in_plan(&wc.input);
            let inner_cte_refs: std::collections::HashMap<String, String> = if fresh_aliases
                .is_empty()
            {
                cte_references.clone()
            } else {
                log::debug!(
                    "🔧 update_graph_joins_cte_refs: Filtering CTE refs for fresh scans in inner scope: {:?}",
                    fresh_aliases
                );
                cte_references
                    .iter()
                    .filter(|(alias, _)| !fresh_aliases.contains(*alias))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            };
            let inner_prop_mappings: std::collections::HashMap<
                String,
                std::collections::HashMap<String, String>,
            > = if fresh_aliases.is_empty() {
                cte_property_mappings.clone()
            } else {
                cte_property_mappings
                    .iter()
                    .filter(|(alias, _)| !fresh_aliases.contains(*alias))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            };
            let new_input =
                update_graph_joins_cte_refs(&wc.input, &inner_cte_refs, &inner_prop_mappings)?;

            // Check if this WithClause's cte_name needs updating
            let updated_cte_name = if let Some(ref old_cte_name) = wc.cte_name {
                // Check if any alias exported by this WITH has a new CTE name
                wc.exported_aliases
                    .iter()
                    .find_map(|alias| cte_references.get(alias))
                    .cloned()
                    .or(Some(old_cte_name.clone()))
            } else {
                None
            };

            log::info!(
                "🔧 update_graph_joins_cte_refs: Updating WithClause.cte_name from {:?} to {:?}",
                wc.cte_name,
                updated_cte_name
            );

            Ok(LogicalPlan::WithClause(WithClause {
                input: Arc::new(new_input),
                cte_name: updated_cte_name,
                cte_references: cte_references.clone(), // UPDATE HERE!
                ..wc.clone()
            }))
        }
        LogicalPlan::Filter(f) => {
            let new_input =
                update_graph_joins_cte_refs(&f.input, cte_references, cte_property_mappings)?;
            let updated_predicate =
                rewrite_logical_expr_cte_refs(&f.predicate, cte_references, cte_property_mappings);
            Ok(LogicalPlan::Filter(Filter {
                input: Arc::new(new_input),
                predicate: updated_predicate,
            }))
        }
        LogicalPlan::GroupBy(gb) => {
            let new_input =
                update_graph_joins_cte_refs(&gb.input, cte_references, cte_property_mappings)?;
            let updated_expressions: Vec<_> = gb
                .expressions
                .iter()
                .map(|expr| {
                    rewrite_logical_expr_cte_refs(expr, cte_references, cte_property_mappings)
                })
                .collect();
            let updated_having = gb
                .having_clause
                .as_ref()
                .map(|h| rewrite_logical_expr_cte_refs(h, cte_references, cte_property_mappings));
            Ok(LogicalPlan::GroupBy(GroupBy {
                input: Arc::new(new_input),
                expressions: updated_expressions,
                having_clause: updated_having,
                is_materialization_boundary: gb.is_materialization_boundary,
                exposed_alias: gb.exposed_alias.clone(),
            }))
        }
        LogicalPlan::OrderBy(ob) => {
            let new_input =
                update_graph_joins_cte_refs(&ob.input, cte_references, cte_property_mappings)?;
            let updated_items: Vec<_> = ob
                .items
                .iter()
                .map(|item| crate::query_planner::logical_plan::OrderByItem {
                    expression: rewrite_logical_expr_cte_refs(
                        &item.expression,
                        cte_references,
                        cte_property_mappings,
                    ),
                    order: item.order.clone(),
                })
                .collect();
            Ok(LogicalPlan::OrderBy(OrderBy {
                input: Arc::new(new_input),
                items: updated_items,
            }))
        }
        LogicalPlan::Limit(lim) => {
            let new_input =
                update_graph_joins_cte_refs(&lim.input, cte_references, cte_property_mappings)?;
            Ok(LogicalPlan::Limit(Limit {
                input: Arc::new(new_input),
                count: lim.count,
            }))
        }
        LogicalPlan::Skip(skip) => {
            let new_input =
                update_graph_joins_cte_refs(&skip.input, cte_references, cte_property_mappings)?;
            Ok(LogicalPlan::Skip(Skip {
                input: Arc::new(new_input),
                count: skip.count,
            }))
        }
        LogicalPlan::Union(union) => {
            let new_inputs: Vec<Arc<LogicalPlan>> = union
                .inputs
                .iter()
                .map(|input| {
                    // #517: a genuine Cypher UNION's arms are independent
                    // scopes. `cte_references`/`cte_property_mappings` are
                    // built ONCE for the whole query (from whichever arm(s)
                    // had a WITH clause), so blindly recursing into every
                    // branch with the SAME maps lets one arm's WITH-derived
                    // CTE substitution leak into a sibling arm that reuses
                    // the same Cypher variable name but never had that WITH
                    // clause at all (e.g. `u` bound fresh in both arms of
                    // `MATCH (u) WITH u... RETURN ... UNION MATCH (u)
                    // RETURN ...`) — `rewrite_logical_expr_cte_refs` below
                    // would then rewrite the untouched arm's `u.prop` to
                    // reference the OTHER arm's raw CTE table name. Scope
                    // the maps down to only the aliases THIS branch's own
                    // subtree actually exports via its own WITH clause(s).
                    // BidirectionalUnion (`is_cypher_union == false`)
                    // represents a single logical MATCH scope split purely
                    // for SQL rendering, so every branch legitimately
                    // shares the same maps there and must keep receiving
                    // them unscoped.
                    let (scoped_refs, scoped_props) = if union.is_cypher_union {
                        let locally_exported = with_exported_aliases_in_branch(input);
                        let refs: std::collections::HashMap<String, String> = cte_references
                            .iter()
                            .filter(|(alias, _)| locally_exported.contains(alias.as_str()))
                            .map(|(a, b)| (a.clone(), b.clone()))
                            .collect();
                        let props: std::collections::HashMap<
                            String,
                            std::collections::HashMap<String, String>,
                        > = cte_property_mappings
                            .iter()
                            .filter(|(alias, _)| locally_exported.contains(alias.as_str()))
                            .map(|(a, b)| (a.clone(), b.clone()))
                            .collect();
                        (refs, props)
                    } else {
                        (cte_references.clone(), cte_property_mappings.clone())
                    };
                    update_graph_joins_cte_refs(input, &scoped_refs, &scoped_props).map(Arc::new)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(LogicalPlan::Union(Union {
                inputs: new_inputs,
                union_type: union.union_type.clone(),
                is_cypher_union: union.is_cypher_union,
            }))
        }
        LogicalPlan::CartesianProduct(cp) => {
            let new_left =
                update_graph_joins_cte_refs(&cp.left, cte_references, cte_property_mappings)?;
            let new_right =
                update_graph_joins_cte_refs(&cp.right, cte_references, cte_property_mappings)?;
            Ok(LogicalPlan::CartesianProduct(CartesianProduct {
                left: Arc::new(new_left),
                right: Arc::new(new_right),
                is_optional: cp.is_optional,
                join_condition: cp.join_condition.clone(),
            }))
        }
        other => Ok(other.clone()),
    }
}
