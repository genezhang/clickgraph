//! Expression rewriting utilities for property mapping
//!
//! This module provides functions to rewrite LogicalExpr trees, particularly for:
//! - Mapping Cypher property names to database column names
//! - Handling nested expressions (functions, operators, etc.)
//!
//! These utilities are shared between WITH and RETURN clause processing to ensure
//! consistent expression handling throughout the query pipeline.

use super::{
    AggregateFnCall, LogicalCase, LogicalExpr, OperatorApplication, PropertyAccess, ReduceExpr,
    ScalarFnCall, TableAlias,
};
use crate::graph_catalog::expression_parser::PropertyValue;
use crate::query_planner::logical_plan::LogicalPlan;
use crate::render_plan::variable_scope::VariableScope;

/// Context for expression rewriting.
///
/// Contains the information needed to resolve property mappings:
/// - The input plan (for alias → label resolution)
/// - Optional scope for CTE-aware resolution
pub struct ExpressionRewriteContext<'a> {
    /// The input plan from which aliases can be resolved to labels
    pub input_plan: &'a LogicalPlan,
    /// Optional scope for CTE-aware variable resolution.
    /// When Some, CTE variables resolve to CTE columns (forward resolution).
    /// When None, falls back to schema-only resolution (backward compatible).
    pub scope: Option<&'a VariableScope<'a>>,
}

impl<'a> ExpressionRewriteContext<'a> {
    pub fn new(input_plan: &'a LogicalPlan) -> Self {
        log::debug!(
            "🔍 ExpressionRewriteContext: Created with plan type: {:?}",
            std::mem::discriminant(input_plan)
        );
        Self {
            input_plan,
            scope: None,
        }
    }

    pub fn with_scope(input_plan: &'a LogicalPlan, scope: &'a VariableScope<'a>) -> Self {
        log::debug!(
            "🔍 ExpressionRewriteContext: Created with scope, plan type: {:?}",
            std::mem::discriminant(input_plan)
        );
        Self {
            input_plan,
            scope: Some(scope),
        }
    }

    /// Find the label for a given alias by searching the input plan
    pub fn find_label_for_alias(&self, alias: &str) -> Option<String> {
        let result = find_label_for_alias_in_plan(self.input_plan, alias);
        log::debug!(
            "🔍 ExpressionRewriteContext: find_label_for_alias('{}') = {:?}",
            alias,
            result
        );
        result
    }
}

/// Find the label for an alias by recursively searching the plan tree
pub(crate) fn find_label_for_alias_in_plan(
    plan: &LogicalPlan,
    target_alias: &str,
) -> Option<String> {
    match plan {
        LogicalPlan::GraphNode(node) => {
            if node.alias == target_alias {
                node.label.clone()
            } else {
                find_label_for_alias_in_plan(&node.input, target_alias)
            }
        }
        // ViewScan doesn't have an alias field - alias is in the wrapping GraphNode
        LogicalPlan::ViewScan(_) => None,
        LogicalPlan::GraphRel(rel) => {
            // Check left and right plans
            find_label_for_alias_in_plan(&rel.left, target_alias)
                .or_else(|| find_label_for_alias_in_plan(&rel.right, target_alias))
                .or_else(|| find_label_for_alias_in_plan(&rel.center, target_alias))
        }
        LogicalPlan::CartesianProduct(cp) => find_label_for_alias_in_plan(&cp.left, target_alias)
            .or_else(|| find_label_for_alias_in_plan(&cp.right, target_alias)),
        LogicalPlan::GraphJoins(gj) => find_label_for_alias_in_plan(&gj.input, target_alias),
        LogicalPlan::Filter(filter) => find_label_for_alias_in_plan(&filter.input, target_alias),
        LogicalPlan::Projection(proj) => find_label_for_alias_in_plan(&proj.input, target_alias),
        LogicalPlan::GroupBy(gb) => find_label_for_alias_in_plan(&gb.input, target_alias),
        LogicalPlan::OrderBy(ob) => find_label_for_alias_in_plan(&ob.input, target_alias),
        LogicalPlan::Skip(s) => find_label_for_alias_in_plan(&s.input, target_alias),
        LogicalPlan::Limit(l) => find_label_for_alias_in_plan(&l.input, target_alias),
        LogicalPlan::Cte(cte) => find_label_for_alias_in_plan(&cte.input, target_alias),
        LogicalPlan::Unwind(u) => find_label_for_alias_in_plan(&u.input, target_alias),
        LogicalPlan::WithClause(wc) => find_label_for_alias_in_plan(&wc.input, target_alias),
        LogicalPlan::Union(union) => {
            for input in &union.inputs {
                if let Some(label) = find_label_for_alias_in_plan(input, target_alias) {
                    return Some(label);
                }
            }
            None
        }
        LogicalPlan::Empty | LogicalPlan::PageRank(_) => None,

        // Write variants — recurse into preceding read pipeline.
        LogicalPlan::Create(c) => find_label_for_alias_in_plan(&c.input, target_alias),
        LogicalPlan::SetProperties(sp) => find_label_for_alias_in_plan(&sp.input, target_alias),
        LogicalPlan::Delete(d) => find_label_for_alias_in_plan(&d.input, target_alias),
        LogicalPlan::Remove(r) => find_label_for_alias_in_plan(&r.input, target_alias),
    }
}

/// Map a Cypher property to its corresponding database column using the schema.
///
/// This is the core property mapping function that consults GLOBAL_SCHEMAS.
pub(crate) fn map_property_to_db_column(
    cypher_property: &str,
    node_label: &str,
) -> Result<String, PropertyMappingError> {
    // Use the existing map_property_to_column_with_schema function
    crate::render_plan::cte_generation::map_property_to_column_with_schema(
        cypher_property,
        node_label,
    )
    .map_err(PropertyMappingError::MappingFailed)
}

/// Errors that can occur during property mapping
#[derive(Debug, Clone)]
pub enum PropertyMappingError {
    /// Label not found for the given alias
    LabelNotFound(String),
    /// Property mapping failed (schema lookup error)
    MappingFailed(String),
}

impl std::fmt::Display for PropertyMappingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PropertyMappingError::LabelNotFound(alias) => {
                write!(f, "Could not find label for alias '{}'", alias)
            }
            PropertyMappingError::MappingFailed(msg) => {
                write!(f, "Property mapping failed: {}", msg)
            }
        }
    }
}

impl std::error::Error for PropertyMappingError {}

/// Rewrite a LogicalExpr, mapping Cypher properties to database columns.
///
/// This function recursively traverses the expression tree and for each
/// `PropertyAccessExp`, it:
/// 1. Looks up the label for the table alias
/// 2. Maps the Cypher property name to the database column name
/// 3. Returns a new expression with the mapped column
///
/// Other expression types are recursively processed to handle nested expressions
/// like `substring(u.name, 1, 10)` or `u.age + 1`.
///
/// # Arguments
/// * `expr` - The expression to rewrite
/// * `ctx` - Context containing the input plan for alias resolution
///
/// # Returns
/// A new LogicalExpr with properties mapped to database columns.
/// If a property cannot be mapped (alias not found or schema lookup fails),
/// the original expression is returned unchanged with a warning logged.
pub fn rewrite_expression_with_property_mapping(
    expr: &LogicalExpr,
    ctx: &ExpressionRewriteContext,
) -> LogicalExpr {
    match expr {
        // PropertyAccessExp: Map Cypher property to DB column (or CTE column if scope-aware)
        LogicalExpr::PropertyAccessExp(prop) => {
            let alias = &prop.table_alias.0;
            let cypher_property = prop.column.raw();

            log::debug!(
                "🔍 Expression rewriter: Processing PropertyAccessExp {}.{}",
                alias,
                cypher_property
            );

            // Scope-aware resolution: check scope FIRST if available
            if let Some(scope) = ctx.scope {
                use crate::render_plan::variable_scope::ResolvedProperty;
                match scope.resolve(alias, cypher_property) {
                    ResolvedProperty::CteColumn { cte_name, column } => {
                        log::debug!(
                            "✓ Scope resolution (CTE): {}.{} → {}.{}",
                            alias,
                            cypher_property,
                            cte_name,
                            column
                        );
                        return LogicalExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(cte_name),
                            column: PropertyValue::Column(column),
                        });
                    }
                    ResolvedProperty::DbColumn(db_col) => {
                        log::debug!(
                            "✓ Scope resolution (DB): {}.{} → {}.{}",
                            alias,
                            cypher_property,
                            alias,
                            db_col
                        );
                        if db_col == cypher_property {
                            return expr.clone();
                        }
                        return LogicalExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: prop.table_alias.clone(),
                            column: PropertyValue::Column(db_col),
                        });
                    }
                    ResolvedProperty::Unresolved => {
                        log::debug!(
                            "🔍 Scope could not resolve {}.{}, falling through to schema",
                            alias,
                            cypher_property
                        );
                    }
                }
            }

            // Fallback: schema-only resolution (original behavior)
            match ctx.find_label_for_alias(alias) {
                Some(label) => {
                    log::debug!("🔍 TRACING: Expression rewriter found label '{}' for alias '{}', property '{}'", label, alias, cypher_property);
                    // Map the property to DB column
                    match map_property_to_db_column(cypher_property, &label) {
                        Ok(db_column) => {
                            log::debug!(
                                "🔍 TRACING: Mapped property '{}' to DB column '{}' for label '{}'",
                                cypher_property,
                                db_column,
                                label
                            );
                            // IDEMPOTENCY CHECK: If the mapped column is the same as the input,
                            // the property is already a DB column name. Return as-is.
                            // This handles cases where the expression was already rewritten.
                            if db_column == cypher_property {
                                log::debug!(
                                    "🔍 Expression rewriter: '{}' maps to itself - already a DB column, keeping as-is",
                                    cypher_property
                                );
                                return expr.clone();
                            }

                            log::debug!(
                                "✓ Property mapping: {}.{} → {}.{} (label={})",
                                alias,
                                cypher_property,
                                alias,
                                db_column,
                                label
                            );
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: prop.table_alias.clone(),
                                column: PropertyValue::Column(db_column),
                            })
                        }
                        Err(e) => {
                            log::warn!(
                                "⚠️ Property mapping failed for {}.{}: {}. Using original.",
                                alias,
                                cypher_property,
                                e
                            );
                            expr.clone()
                        }
                    }
                }
                None => {
                    log::warn!(
                        "⚠️ Could not find label for alias '{}'. Using original expression.",
                        alias
                    );
                    expr.clone()
                }
            }
        }

        // ScalarFnCall: Recursively rewrite arguments
        LogicalExpr::ScalarFnCall(fn_call) => {
            let rewritten_args: Vec<LogicalExpr> = fn_call
                .args
                .iter()
                .map(|arg| rewrite_expression_with_property_mapping(arg, ctx))
                .collect();

            LogicalExpr::ScalarFnCall(ScalarFnCall {
                name: fn_call.name.clone(),
                args: rewritten_args,
            })
        }

        // AggregateFnCall: Recursively rewrite arguments
        LogicalExpr::AggregateFnCall(agg) => {
            let rewritten_args: Vec<LogicalExpr> = agg
                .args
                .iter()
                .map(|arg| rewrite_expression_with_property_mapping(arg, ctx))
                .collect();

            LogicalExpr::AggregateFnCall(AggregateFnCall {
                name: agg.name.clone(),
                args: rewritten_args,
            })
        }

        // OperatorApplicationExp: Recursively rewrite operands
        LogicalExpr::OperatorApplicationExp(op) => {
            let rewritten_operands: Vec<LogicalExpr> = op
                .operands
                .iter()
                .map(|operand| rewrite_expression_with_property_mapping(operand, ctx))
                .collect();

            LogicalExpr::OperatorApplicationExp(OperatorApplication {
                operator: op.operator,
                operands: rewritten_operands,
            })
        }

        // Case: Recursively rewrite all parts
        LogicalExpr::Case(case) => {
            let new_expr = case
                .expr
                .as_ref()
                .map(|e| Box::new(rewrite_expression_with_property_mapping(e, ctx)));

            let new_when_then: Vec<(LogicalExpr, LogicalExpr)> = case
                .when_then
                .iter()
                .map(|(when, then)| {
                    (
                        rewrite_expression_with_property_mapping(when, ctx),
                        rewrite_expression_with_property_mapping(then, ctx),
                    )
                })
                .collect();

            let new_else = case
                .else_expr
                .as_ref()
                .map(|e| Box::new(rewrite_expression_with_property_mapping(e, ctx)));

            LogicalExpr::Case(LogicalCase {
                expr: new_expr,
                when_then: new_when_then,
                else_expr: new_else,
            })
        }

        // List: Recursively rewrite elements
        LogicalExpr::List(items) => {
            let rewritten_items: Vec<LogicalExpr> = items
                .iter()
                .map(|item| rewrite_expression_with_property_mapping(item, ctx))
                .collect();

            LogicalExpr::List(rewritten_items)
        }

        // ReduceExpr: Recursively rewrite components
        LogicalExpr::ReduceExpr(reduce) => LogicalExpr::ReduceExpr(ReduceExpr {
            accumulator: reduce.accumulator.clone(),
            initial_value: Box::new(rewrite_expression_with_property_mapping(
                &reduce.initial_value,
                ctx,
            )),
            variable: reduce.variable.clone(),
            list: Box::new(rewrite_expression_with_property_mapping(&reduce.list, ctx)),
            expression: Box::new(rewrite_expression_with_property_mapping(
                &reduce.expression,
                ctx,
            )),
        }),

        // MapLiteral: Recursively rewrite values
        LogicalExpr::MapLiteral(entries) => {
            let rewritten_entries: Vec<(String, LogicalExpr)> = entries
                .iter()
                .map(|(k, v)| (k.clone(), rewrite_expression_with_property_mapping(v, ctx)))
                .collect();

            LogicalExpr::MapLiteral(rewritten_entries)
        }

        // ArraySubscript: Recursively rewrite array and index
        LogicalExpr::ArraySubscript { array, index } => LogicalExpr::ArraySubscript {
            array: Box::new(rewrite_expression_with_property_mapping(array, ctx)),
            index: Box::new(rewrite_expression_with_property_mapping(index, ctx)),
        },

        // ArraySlicing: Recursively rewrite array and bounds
        LogicalExpr::ArraySlicing { array, from, to } => LogicalExpr::ArraySlicing {
            array: Box::new(rewrite_expression_with_property_mapping(array, ctx)),
            from: from
                .as_ref()
                .map(|f| Box::new(rewrite_expression_with_property_mapping(f, ctx))),
            to: to
                .as_ref()
                .map(|t| Box::new(rewrite_expression_with_property_mapping(t, ctx))),
        },

        // InSubquery: Rewrite the expression part (subplan is a full plan, not expr)
        LogicalExpr::InSubquery(subq) => {
            LogicalExpr::InSubquery(crate::query_planner::logical_expr::InSubquery {
                expr: Box::new(rewrite_expression_with_property_mapping(&subq.expr, ctx)),
                subplan: subq.subplan.clone(),
            })
        }

        // Leaf expressions that don't need rewriting
        LogicalExpr::Literal(_)
        | LogicalExpr::Raw(_)
        | LogicalExpr::Star
        | LogicalExpr::TableAlias(_)
        | LogicalExpr::ColumnAlias(_)
        | LogicalExpr::Column(_)
        | LogicalExpr::Parameter(_)
        | LogicalExpr::PathPattern(_)
        | LogicalExpr::ExistsSubquery(_)
        | LogicalExpr::LabelExpression { .. }
        | LogicalExpr::Lambda(_)
        | LogicalExpr::CteEntityRef(_)
        | LogicalExpr::Operator(_)
        | LogicalExpr::PatternCount(_)
        | LogicalExpr::PatternComprehension(_) => expr.clone(),
    }
}

/// #530: Rewrite a `PropertyAccessExp` for `target_alias` using an already-CONCRETE,
/// per-branch `property_mapping` (Cypher property name -> physical DB column) instead
/// of a schema/label lookup.
///
/// Used when a Filter/predicate is being pushed down into ONE specific branch of an
/// already-materialized denormalized-node Union (e.g. an anchor's own standalone
/// `origin`/`dest`-role scans over the same physical table, each branch already
/// carrying its OWN resolved `ViewScan.property_mapping` — see
/// `materialize_standalone_denorm_scans` in `type_inference.rs`). Unlike
/// `rewrite_expression_with_property_mapping` (which re-derives the mapping from the
/// alias's LABEL via the schema catalog — the same physical column for every branch,
/// since a label alone cannot distinguish "this node as an edge's FROM" from "this
/// node as the edge's TO"), this takes the mapping that already correctly
/// distinguishes the two roles and applies it directly — routing through the same
/// `PropertyValue`-keyed map the SELECT-list rendering for that exact branch already
/// uses, so a symmetric or asymmetric role split is handled identically (no branch-role
/// guessing here at all).
///
/// Only rewrites `PropertyAccessExp` nodes whose `table_alias` matches `target_alias`;
/// other aliases in a multi-alias predicate are left untouched (this function is only
/// ever applied to a single node's own inline-map/filter predicate). If a referenced
/// property has no entry in `mapping` (shouldn't happen for a well-formed inline map
/// filter, since both roles expose the same Cypher property set), the original
/// (unmapped) expression is kept — matching prior behavior rather than fabricating a
/// column, so a schema/property mismatch surfaces as a loud unresolved-column error
/// instead of silently substituting the wrong data.
pub fn rewrite_expression_with_concrete_property_map(
    expr: &LogicalExpr,
    target_alias: &str,
    mapping: &std::collections::HashMap<String, PropertyValue>,
) -> LogicalExpr {
    match expr {
        LogicalExpr::PropertyAccessExp(prop) => {
            if prop.table_alias.0 != target_alias {
                return expr.clone();
            }
            let cypher_property = prop.column.raw();
            match mapping.get(cypher_property) {
                Some(mapped) => LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: prop.table_alias.clone(),
                    column: mapped.clone(),
                }),
                None => expr.clone(),
            }
        }
        LogicalExpr::ScalarFnCall(fn_call) => LogicalExpr::ScalarFnCall(ScalarFnCall {
            name: fn_call.name.clone(),
            args: fn_call
                .args
                .iter()
                .map(|arg| {
                    rewrite_expression_with_concrete_property_map(arg, target_alias, mapping)
                })
                .collect(),
        }),
        LogicalExpr::AggregateFnCall(agg) => LogicalExpr::AggregateFnCall(AggregateFnCall {
            name: agg.name.clone(),
            args: agg
                .args
                .iter()
                .map(|arg| {
                    rewrite_expression_with_concrete_property_map(arg, target_alias, mapping)
                })
                .collect(),
        }),
        LogicalExpr::OperatorApplicationExp(op) => {
            LogicalExpr::OperatorApplicationExp(OperatorApplication {
                operator: op.operator,
                operands: op
                    .operands
                    .iter()
                    .map(|operand| {
                        rewrite_expression_with_concrete_property_map(
                            operand,
                            target_alias,
                            mapping,
                        )
                    })
                    .collect(),
            })
        }
        LogicalExpr::Case(case) => LogicalExpr::Case(LogicalCase {
            expr: case.expr.as_ref().map(|e| {
                Box::new(rewrite_expression_with_concrete_property_map(
                    e,
                    target_alias,
                    mapping,
                ))
            }),
            when_then: case
                .when_then
                .iter()
                .map(|(when, then)| {
                    (
                        rewrite_expression_with_concrete_property_map(when, target_alias, mapping),
                        rewrite_expression_with_concrete_property_map(then, target_alias, mapping),
                    )
                })
                .collect(),
            else_expr: case.else_expr.as_ref().map(|e| {
                Box::new(rewrite_expression_with_concrete_property_map(
                    e,
                    target_alias,
                    mapping,
                ))
            }),
        }),
        LogicalExpr::List(items) => LogicalExpr::List(
            items
                .iter()
                .map(|item| {
                    rewrite_expression_with_concrete_property_map(item, target_alias, mapping)
                })
                .collect(),
        ),
        LogicalExpr::ArraySubscript { array, index } => LogicalExpr::ArraySubscript {
            array: Box::new(rewrite_expression_with_concrete_property_map(
                array,
                target_alias,
                mapping,
            )),
            index: Box::new(rewrite_expression_with_concrete_property_map(
                index,
                target_alias,
                mapping,
            )),
        },
        LogicalExpr::ArraySlicing { array, from, to } => LogicalExpr::ArraySlicing {
            array: Box::new(rewrite_expression_with_concrete_property_map(
                array,
                target_alias,
                mapping,
            )),
            from: from.as_ref().map(|f| {
                Box::new(rewrite_expression_with_concrete_property_map(
                    f,
                    target_alias,
                    mapping,
                ))
            }),
            to: to.as_ref().map(|t| {
                Box::new(rewrite_expression_with_concrete_property_map(
                    t,
                    target_alias,
                    mapping,
                ))
            }),
        },
        // Leaf / not-yet-needed variants: left unchanged. An inline-map filter's
        // predicate is built purely from comparisons over property accesses, so these
        // shapes are not expected here — if a future caller needs them, extend
        // analogous to `rewrite_expression_with_property_mapping` above.
        _ => expr.clone(),
    }
}

/// #530: remap a predicate being pushed down into ONE branch of an
/// already-materialized denormalized-node `Union` (each branch a `GraphNode` wrapping
/// a `ViewScan` with its OWN concrete, role-specific `property_mapping` — e.g. the
/// `origin`/`dest` role split for a denormalized `Airport` node over `flights_denorm`).
///
/// Both `union_distribution.rs` and `type_inference.rs`'s
/// `materialize_standalone_denorm_scans` distribute a `Filter`'s predicate over such a
/// Union's branches; before this helper, both cloned the predicate UNCHANGED into
/// every branch — fine for a plain (non-denormalized) Union, but wrong here: an
/// inline-map filter's predicate still holds the RAW, unmapped Cypher property name
/// (e.g. `code`) at this point, and each branch needs it resolved through ITS OWN
/// role-specific mapping (`origin_code` vs `dest_code`), not the same raw name
/// rendered verbatim into a WHERE clause where no such column exists (#530).
///
/// Falls back to returning `predicate` completely UNCHANGED when `branch` isn't a
/// denormalized `GraphNode(ViewScan)` — i.e. for every non-denormalized
/// Union-distribution shape (the overwhelming majority of callers), this is a
/// complete no-op, identical to the prior behavior.
pub fn remap_predicate_for_denorm_union_branch(
    predicate: &LogicalExpr,
    branch: &LogicalPlan,
) -> LogicalExpr {
    if let LogicalPlan::GraphNode(gn) = branch {
        if crate::graph_catalog::pattern_schema::node_denormalized_flag(gn) {
            if let LogicalPlan::ViewScan(vs) = gn.input.as_ref() {
                if crate::graph_catalog::pattern_schema::scan_denormalized_flag(vs) {
                    return rewrite_expression_with_concrete_property_map(
                        predicate,
                        &gn.alias,
                        &vs.property_mapping,
                    );
                }
            }
        }
    }
    predicate.clone()
}

/// #566: build a reverse lookup (physical DB column -> Cypher property name)
/// spanning BOTH the `from`- and `to`-role property maps of a denormalized
/// node's schema.
///
/// Used to "undo" a predicate column that was already committed to ONE
/// role's physical column (e.g. `origin_city`) by an EARLIER, edge-role-aware
/// resolution — `FilterTagging`'s `find_owning_edge_for_node` path, which
/// fires whenever the referenced alias is ALSO an endpoint of a relationship
/// pattern elsewhere in the query (the common case for an anchor bound by an
/// earlier plain `MATCH` and then referenced again in a later `OPTIONAL
/// MATCH` edge, e.g. `MATCH (a:Airport) OPTIONAL MATCH (a)-[:FLIGHT]->(b)
/// WHERE a.city = 'Chicago'`). That resolution commits to ONE role (the
/// role `a` happens to play on the specific edge it's ALSO connected to) —
/// correct for the ordinary "join straight through the edge" rendering, but
/// wrong once `a` is instead rendered as a role-agnostic standalone-scan
/// Union (spanning EVERY role the label can appear in) for the special
/// OPTIONAL denormalized CTE + LEFT JOIN path (`optional_denorm_union_anchor_is_left`,
/// `plan_builder.rs`): every branch of that Union needs the predicate
/// resolved through its OWN role, not the one role FilterTagging happened to
/// pick first.
///
/// Where two roles map the SAME Cypher property to the SAME physical column
/// name (a coincidence for some schemas), the `from` role's entry wins
/// (arbitrary but deterministic — `entry().or_insert` keeps the first
/// insertion).
pub fn denorm_role_reverse_lookup(
    node_schema: &crate::graph_catalog::graph_schema::NodeSchema,
) -> std::collections::HashMap<String, PropertyValue> {
    let mut reverse = std::collections::HashMap::new();
    for props in [
        node_schema.from_properties.as_ref(),
        node_schema.to_properties.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        for (cypher_name, physical_col) in props {
            reverse
                .entry(physical_col.clone())
                .or_insert_with(|| PropertyValue::Column(cypher_name.clone()));
        }
    }
    reverse
}

/// #566: like `rewrite_expression_with_concrete_property_map`, but resilient to a
/// predicate whose column has ALREADY been resolved to a role-committed physical
/// column (see `denorm_role_reverse_lookup`'s doc) rather than the raw Cypher
/// property name #530's original helper expects.
///
/// Splits `expr` into its top-level AND conjuncts and resolves each independently
/// (a multi-condition WHERE can legitimately mix a conjunct FilterTagging left raw
/// — e.g. one it couldn't resolve at all — with one it role-committed). For each
/// conjunct: try the direct raw-Cypher-name mapping first (#530's exact behavior,
/// so the inline-map case — already raw — is completely unaffected); if that's a
/// no-op, recover the raw Cypher name via `reverse_lookup` and retry the forward
/// mapping through it. Falls back to the conjunct unchanged if neither succeeds.
pub fn rewrite_expression_with_concrete_property_map_role_aware(
    expr: &LogicalExpr,
    target_alias: &str,
    mapping: &std::collections::HashMap<String, PropertyValue>,
    reverse_lookup: Option<&std::collections::HashMap<String, PropertyValue>>,
) -> LogicalExpr {
    fn split_and(expr: &LogicalExpr, out: &mut Vec<LogicalExpr>) {
        if let LogicalExpr::OperatorApplicationExp(op) = expr {
            if op.operator == super::Operator::And {
                for operand in &op.operands {
                    split_and(operand, out);
                }
                return;
            }
        }
        out.push(expr.clone());
    }
    fn combine_and(mut conjuncts: Vec<LogicalExpr>) -> LogicalExpr {
        match conjuncts.len() {
            0 => unreachable!("split_and always produces at least one conjunct"),
            1 => conjuncts.remove(0),
            _ => conjuncts
                .into_iter()
                .reduce(|acc, next| {
                    LogicalExpr::OperatorApplicationExp(OperatorApplication {
                        operator: super::Operator::And,
                        operands: vec![acc, next],
                    })
                })
                .expect("reduce over a non-empty Vec always returns Some"),
        }
    }

    let mut conjuncts = Vec::new();
    split_and(expr, &mut conjuncts);

    let resolved = conjuncts
        .into_iter()
        .map(|conjunct| {
            let direct =
                rewrite_expression_with_concrete_property_map(&conjunct, target_alias, mapping);
            if direct != conjunct {
                return direct;
            }
            // Direct raw-Cypher-name lookup was a no-op — the conjunct's column
            // may already be a role-committed physical column (#566). Recover
            // the raw Cypher name via the reverse lookup, then retry forward.
            if let Some(reverse_lookup) = reverse_lookup {
                let normalized = rewrite_expression_with_concrete_property_map(
                    &conjunct,
                    target_alias,
                    reverse_lookup,
                );
                if normalized != conjunct {
                    return rewrite_expression_with_concrete_property_map(
                        &normalized,
                        target_alias,
                        mapping,
                    );
                }
            }
            conjunct
        })
        .collect();

    combine_and(resolved)
}

/// Rewrite all expressions in a list of ProjectionItems.
///
/// This is a convenience function for WITH/RETURN clause processing.
pub fn rewrite_projection_items_with_property_mapping(
    items: &[crate::query_planner::logical_plan::ProjectionItem],
    ctx: &ExpressionRewriteContext,
) -> Vec<crate::query_planner::logical_plan::ProjectionItem> {
    items
        .iter()
        .map(|item| crate::query_planner::logical_plan::ProjectionItem {
            expression: rewrite_expression_with_property_mapping(&item.expression, ctx),
            col_alias: item.col_alias.clone(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_planner::logical_expr::{Literal, TableAlias};

    #[test]
    fn test_rewrite_literal_unchanged() {
        // Literals should pass through unchanged
        let expr = LogicalExpr::Literal(Literal::Integer(42));
        let plan = LogicalPlan::Empty;
        let ctx = ExpressionRewriteContext::new(&plan);

        let result = rewrite_expression_with_property_mapping(&expr, &ctx);

        assert_eq!(result, expr);
    }

    #[test]
    fn test_rewrite_table_alias_unchanged() {
        // TableAlias (variable reference) should pass through unchanged
        let expr = LogicalExpr::TableAlias(TableAlias("u".to_string()));
        let plan = LogicalPlan::Empty;
        let ctx = ExpressionRewriteContext::new(&plan);

        let result = rewrite_expression_with_property_mapping(&expr, &ctx);

        assert_eq!(result, expr);
    }

    #[test]
    fn test_rewrite_nested_function_call() {
        // Function calls should have their arguments recursively processed
        let inner_expr = LogicalExpr::Literal(Literal::Integer(1));
        let fn_call = LogicalExpr::ScalarFnCall(ScalarFnCall {
            name: "abs".to_string(),
            args: vec![inner_expr.clone()],
        });

        let plan = LogicalPlan::Empty;
        let ctx = ExpressionRewriteContext::new(&plan);

        let result = rewrite_expression_with_property_mapping(&fn_call, &ctx);

        // The result should be structurally identical (literals don't change)
        match result {
            LogicalExpr::ScalarFnCall(sf) => {
                assert_eq!(sf.name, "abs");
                assert_eq!(sf.args.len(), 1);
            }
            _ => panic!("Expected ScalarFnCall"),
        }
    }
}
