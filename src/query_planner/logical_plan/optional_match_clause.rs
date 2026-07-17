//! OPTIONAL MATCH clause processing.
//!
//! Handles Cypher's OPTIONAL MATCH which provides LEFT JOIN semantics -
//! all rows from the base pattern are preserved, with NULL values
//! where optional patterns don't match.
//!
//! # SQL Translation
//!
//! ```text
//! MATCH (a) OPTIONAL MATCH (a)-[:FOLLOWS]->(b)
//! → SELECT ... FROM a LEFT JOIN follows ON ... LEFT JOIN b ON ...
//! ```
//!
//! # Implementation
//!
//! 1. Sets optional mode flag in [`PlanCtx`]
//! 2. Processes patterns via standard MATCH logic
//! 3. Aliases are auto-marked as optional for JOIN generation
//! 4. Restores normal mode after processing

use std::sync::Arc;

use crate::{
    open_cypher_parser::ast,
    query_planner::{
        logical_plan::{plan_builder::LogicalPlanResult, LogicalPlan},
        plan_ctx::PlanCtx,
    },
};

/// Evaluate an OPTIONAL MATCH clause
///
/// OPTIONAL MATCH uses LEFT JOIN semantics - all rows from the input are preserved,
/// with NULL values for unmatched optional patterns.
///
/// Strategy:
/// 1. Set the optional match mode flag in PlanCtx
/// 2. Process patterns using regular MATCH logic (which now auto-marks aliases as optional)
/// 3. GraphJoinInference will generate LEFT JOINs for optional aliases
/// 4. Restore normal mode after processing
pub fn evaluate_optional_match_clause<'a>(
    optional_match_clause: &ast::OptionalMatchClause<'a>,
    input_plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
) -> LogicalPlanResult<Arc<LogicalPlan>> {
    log::debug!(
        "OPTIONAL_MATCH: evaluate_optional_match_clause called with {} path patterns",
        optional_match_clause.path_patterns.len()
    );

    // #597: snapshot the aliases bound BEFORE this OPTIONAL MATCH clause.
    // A WHERE conjunct referencing only these mandatory variables must gate
    // the match (LEFT JOIN ON), never filter the joined rows — and lowering
    // is the only stage where "this WHERE belongs to the OPTIONAL clause" is
    // unambiguous (FilterIntoGraphRel later merges base-MATCH and
    // OPTIONAL-clause predicates indistinguishably into `where_predicate`).
    let pre_bound_mandatory: std::collections::HashSet<String> = plan_ctx
        .get_alias_table_ctx_map()
        .keys()
        .filter(|a| !plan_ctx.get_optional_aliases().contains(*a))
        .cloned()
        .collect();
    // #611: ALSO snapshot every alias bound before this clause (mandatory or
    // optional). The gate conjunct is classified against the MANDATORY set
    // above, but its PLACEMENT target is this clause's ENTRY GraphRel — the
    // one whose connection to the accumulated plan forms the clause's gating
    // LEFT JOIN. In the chained shape `OPTIONAL MATCH (a)-->(b) OPTIONAL
    // MATCH (b)-->(c) WHERE a.x` the entry connection is `b` (optional,
    // bound by the EARLIER clause) — requiring adjacency to a mandatory
    // alias made the tag never land, leaving the predicate in the outer
    // WHERE where it dropped the NULL-extended anchor rows.
    let pre_bound_all: std::collections::HashSet<String> =
        plan_ctx.get_alias_table_ctx_map().keys().cloned().collect();
    // #597: capture THIS clause's index before lowering (the counter advances
    // at the end of evaluate_match_clause_with_optional). The tag must land
    // only on a GraphRel THIS clause lowered — the accumulated plan also
    // contains earlier OPTIONAL clauses' GraphRels, and tagging one of those
    // would gate the WRONG clause's LEFT JOIN (suppressing its legitimate,
    // un-WHERE'd matches).
    let this_clause_index = plan_ctx.current_match_clause_index();

    // SIMPLE FIX: Set the optional match mode flag BEFORE processing patterns
    // This will automatically mark all new aliases as optional during planning
    plan_ctx.set_optional_match_mode(true);

    crate::debug_print!("🔔 DEBUG OPTIONAL_MATCH: Enabled optional match mode");

    // Create a temporary MatchClause from the OptionalMatchClause
    // This allows us to reuse the existing match clause logic
    let temp_match_clause = ast::MatchClause {
        path_patterns: optional_match_clause
            .path_patterns
            .iter()
            .map(|p| (None, p.clone())) // Wrap each pattern with None for path_variable
            .collect(),
        where_clause: None, // WHERE clause handled separately for OPTIONAL MATCH
    };

    // Process the patterns using the _with_optional variant and pass is_optional=true
    // This ensures GraphRel structures are created with is_optional=Some(true)
    use crate::query_planner::logical_plan::match_clause::evaluate_match_clause_with_optional;
    let mut plan =
        evaluate_match_clause_with_optional(&temp_match_clause, input_plan, plan_ctx, true)?;

    // Restore normal mode
    plan_ctx.set_optional_match_mode(false);

    crate::debug_print!(
        "🔕 DEBUG OPTIONAL_MATCH: Disabled optional match mode, plan type: {:?}",
        std::mem::discriminant(&*plan)
    );

    // If there's a WHERE clause specific to this OPTIONAL MATCH,
    // it should be applied as part of the JOIN condition, not as a final filter
    if let Some(where_clause) = &optional_match_clause.where_clause {
        // #597: record the conjuncts that reference ONLY pre-bound mandatory
        // variables on the OPTIONAL GraphRel itself, so the render side can
        // fold them into the gating LEFT JOIN ON (never the outer WHERE,
        // which would drop the NULL-extended anchor rows). The full WHERE
        // still flows through evaluate_where_clause → FilterIntoGraphRel →
        // `where_predicate` as before; this field only CLASSIFIES.
        if let Ok(pred) = crate::query_planner::logical_expr::LogicalExpr::try_from(
            where_clause.conditions.clone(),
        ) {
            let anchor_only: Vec<_> = split_and_conjuncts(&pred)
                .into_iter()
                .filter(|c| {
                    let mut aliases = std::collections::HashSet::new();
                    collect_expr_aliases(c, &mut aliases);
                    !aliases.is_empty() && aliases.iter().all(|a| pre_bound_mandatory.contains(a))
                })
                .collect();
            if !anchor_only.is_empty() {
                plan = tag_optional_anchor_where(
                    plan,
                    combine_with_and(anchor_only),
                    &pre_bound_all,
                    this_clause_index,
                );
            }
        }

        // Store the WHERE clause in the plan context for later processing
        // During SQL generation, this will become part of the LEFT JOIN ON condition
        // For now, we'll add it as a regular filter
        // TODO: Properly handle WHERE clauses in OPTIONAL MATCH
        use crate::query_planner::logical_plan::where_clause::evaluate_where_clause;
        plan = evaluate_where_clause(where_clause, plan)?;
    }

    Ok(plan)
}

/// #597: split a LogicalExpr into top-level AND conjuncts.
fn split_and_conjuncts(
    expr: &crate::query_planner::logical_expr::LogicalExpr,
) -> Vec<crate::query_planner::logical_expr::LogicalExpr> {
    use crate::query_planner::logical_expr::{LogicalExpr, Operator};
    match expr {
        LogicalExpr::OperatorApplicationExp(op) if matches!(op.operator, Operator::And) => {
            op.operands.iter().flat_map(split_and_conjuncts).collect()
        }
        other => vec![other.clone()],
    }
}

/// #597: recombine conjuncts with AND (single conjunct returned as-is).
fn combine_with_and(
    mut conjuncts: Vec<crate::query_planner::logical_expr::LogicalExpr>,
) -> crate::query_planner::logical_expr::LogicalExpr {
    use crate::query_planner::logical_expr::{LogicalExpr, Operator, OperatorApplication};
    if conjuncts.len() == 1 {
        return conjuncts.remove(0);
    }
    LogicalExpr::OperatorApplicationExp(OperatorApplication {
        operator: Operator::And,
        operands: conjuncts,
    })
}

/// #597: collect all table aliases referenced in a LogicalExpr.
fn collect_expr_aliases(
    expr: &crate::query_planner::logical_expr::LogicalExpr,
    aliases: &mut std::collections::HashSet<String>,
) {
    use crate::query_planner::logical_expr::LogicalExpr;
    match expr {
        LogicalExpr::PropertyAccessExp(prop) => {
            aliases.insert(prop.table_alias.0.clone());
        }
        LogicalExpr::TableAlias(ta) => {
            aliases.insert(ta.0.clone());
        }
        LogicalExpr::OperatorApplicationExp(op) => {
            for operand in &op.operands {
                collect_expr_aliases(operand, aliases);
            }
        }
        LogicalExpr::ScalarFnCall(f) => {
            for arg in &f.args {
                collect_expr_aliases(arg, aliases);
            }
        }
        LogicalExpr::AggregateFnCall(f) => {
            for arg in &f.args {
                collect_expr_aliases(arg, aliases);
            }
        }
        LogicalExpr::Case(case) => {
            if let Some(e) = &case.expr {
                collect_expr_aliases(e, aliases);
            }
            for (w, t) in &case.when_then {
                collect_expr_aliases(w, aliases);
                collect_expr_aliases(t, aliases);
            }
            if let Some(e) = &case.else_expr {
                collect_expr_aliases(e, aliases);
            }
        }
        LogicalExpr::List(items) => {
            for item in items {
                collect_expr_aliases(item, aliases);
            }
        }
        // Conservative: any variant not enumerated contributes no aliases,
        // which can only cause a conjunct to be treated as anchor-only if it
        // ALSO has an explicit anchor PropertyAccess — plus the render-side
        // helper re-validates convertibility before folding.
        _ => {}
    }
}

/// #597/#611: set `optional_anchor_where` on this clause's ENTRY GraphRel —
/// the one lowered by THIS clause (`match_clause_index == this_clause_index`)
/// whose left or right connection was already bound BEFORE the clause (by the
/// base MATCH or any earlier OPTIONAL clause) — traversing the wrappers
/// lowering may add. That GraphRel owns the clause's gating LEFT JOIN, which
/// is where the render-side `optional_anchor_gate_conjuncts` consumers fold
/// the conjuncts.
///
/// The clause-index guard is essential: the accumulated plan also contains
/// EARLIER OPTIONAL clauses' GraphRels, which are equally optional and
/// entry-adjacent — tagging one of those would gate the wrong clause's LEFT
/// JOIN, suppressing its legitimate un-WHERE'd matches (e.g. `OPTIONAL MATCH
/// (a)-->(b) OPTIONAL MATCH (b)-->(c) WHERE a.x` must gate ONLY the second
/// clause).
///
/// #611: adjacency is tested against ALL pre-clause-bound aliases, not just
/// mandatory ones — in the chained shape above, the second clause's entry
/// connection is the OPTIONAL `b`, and requiring a mandatory-adjacent
/// GraphRel made the tag never land (the conjunct then stayed in the outer
/// WHERE and dropped the NULL-extended anchor rows). The conjunct itself is
/// still classified mandatory-only by the caller.
///
/// For a multi-hop optional pattern `(a)-[:R1]->(x)-[:R2]->(y)` the pattern
/// nests as `GraphRel(t2){ left: GraphRel(t1){a,x}, right: y }` — the gating
/// LEFT JOIN belongs to the entry-adjacent `t1`, not the topmost `t2`, and
/// the descent below reaches it before any other candidate (only `t1` has a
/// pre-clause-bound connection).
fn tag_optional_anchor_where(
    plan: Arc<LogicalPlan>,
    anchor_where: crate::query_planner::logical_expr::LogicalExpr,
    pre_bound: &std::collections::HashSet<String>,
    this_clause_index: usize,
) -> Arc<LogicalPlan> {
    use crate::query_planner::logical_plan::{Filter, GraphRel, Projection};
    match plan.as_ref() {
        LogicalPlan::GraphRel(gr) if gr.is_optional.unwrap_or(false) => {
            let this_clause = gr.match_clause_index == this_clause_index;
            let adjacent =
                pre_bound.contains(&gr.left_connection) || pre_bound.contains(&gr.right_connection);
            if this_clause && adjacent {
                Arc::new(LogicalPlan::GraphRel(GraphRel {
                    optional_anchor_where: Some(anchor_where),
                    ..gr.clone()
                }))
            } else {
                // Not this clause's anchor-adjacent GraphRel: descend into the
                // nested pattern legs. Only one leg can contain it, but
                // recursing left-then-right with a found-check keeps it simple
                // and side-effect free.
                let new_left = tag_optional_anchor_where(
                    gr.left.clone(),
                    anchor_where.clone(),
                    pre_bound,
                    this_clause_index,
                );
                if !Arc::ptr_eq(&new_left, &gr.left) {
                    return Arc::new(LogicalPlan::GraphRel(GraphRel {
                        left: new_left,
                        ..gr.clone()
                    }));
                }
                let new_right = tag_optional_anchor_where(
                    gr.right.clone(),
                    anchor_where,
                    pre_bound,
                    this_clause_index,
                );
                if !Arc::ptr_eq(&new_right, &gr.right) {
                    return Arc::new(LogicalPlan::GraphRel(GraphRel {
                        right: new_right,
                        ..gr.clone()
                    }));
                }
                plan
            }
        }
        LogicalPlan::Projection(p) => Arc::new(LogicalPlan::Projection(Projection {
            input: tag_optional_anchor_where(
                p.input.clone(),
                anchor_where,
                pre_bound,
                this_clause_index,
            ),
            items: p.items.clone(),
            distinct: p.distinct,
            pattern_comprehensions: p.pattern_comprehensions.clone(),
        })),
        LogicalPlan::Filter(f) => Arc::new(LogicalPlan::Filter(Filter {
            input: tag_optional_anchor_where(
                f.input.clone(),
                anchor_where,
                pre_bound,
                this_clause_index,
            ),
            predicate: f.predicate.clone(),
        })),
        // Unknown wrapper: leave untouched — the conjunct then simply keeps
        // its previous (outer WHERE) placement, no predicate is lost.
        _ => plan,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_catalog::config::Identifier;
    use crate::graph_catalog::schema_types::SchemaType;
    use crate::graph_catalog::{GraphSchema, NodeIdSchema, NodeSchema, RelationshipSchema};
    use crate::open_cypher_parser::ast;

    /// Create a test graph schema with User nodes and FOLLOWS relationships
    fn setup_test_graph_schema() -> GraphSchema {
        use crate::graph_catalog::expression_parser::PropertyValue;
        use std::collections::HashMap;

        let mut nodes = HashMap::new();
        let mut relationships = HashMap::new();

        // Create User node schema
        let user_node = NodeSchema {
            database: "test_db".to_string(),
            table_name: "users".to_string(),
            column_names: vec![
                "id".to_string(),
                "name".to_string(),
                "age".to_string(),
                "status".to_string(),
                "user_id".to_string(),
            ],
            primary_keys: "id".to_string(),
            node_id: NodeIdSchema::single("id".to_string(), SchemaType::Integer),
            property_mappings: [
                (
                    "name".to_string(),
                    PropertyValue::Column("name".to_string()),
                ),
                ("age".to_string(), PropertyValue::Column("age".to_string())),
                (
                    "status".to_string(),
                    PropertyValue::Column("status".to_string()),
                ),
                (
                    "user_id".to_string(),
                    PropertyValue::Column("user_id".to_string()),
                ),
                (
                    "full_name".to_string(),
                    PropertyValue::Column("name".to_string()),
                ),
            ]
            .into_iter()
            .collect(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            is_denormalized: false,
            from_properties: None,
            to_properties: None,
            denormalized_source_table: None,
            label_column: None,
            label_value: None,
            node_id_types: None,
            source: None,
            property_types: HashMap::new(),
            id_generation: None,
        };
        nodes.insert("User".to_string(), user_node);

        // Create FOLLOWS relationship schema
        let follows_rel = RelationshipSchema {
            database: "test_db".to_string(),
            table_name: "follows".to_string(),
            column_names: vec!["from_id".to_string(), "to_id".to_string()],
            from_node: "User".to_string(),
            to_node: "User".to_string(),
            from_node_table: "users".to_string(),
            to_node_table: "users".to_string(),
            from_id: Identifier::from("from_id"),
            to_id: Identifier::from("to_id"),
            from_node_id_dtype: SchemaType::Integer,
            to_node_id_dtype: SchemaType::Integer,
            property_mappings: HashMap::new(),
            view_parameters: None,
            engine: None,
            use_final: None,
            filter: None,
            edge_id: None,
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_label_values: None,
            to_label_values: None,
            from_node_properties: None,
            to_node_properties: None,
            is_fk_edge: false,
            constraints: None,
            edge_id_types: None,
            source: None,
            property_types: HashMap::new(),
        };
        relationships.insert("FOLLOWS::User::User".to_string(), follows_rel);

        GraphSchema::build(1, "test_db".to_string(), nodes, relationships)
    }

    #[test]
    fn test_evaluate_optional_match_simple_node() {
        let optional_match = ast::OptionalMatchClause {
            path_patterns: vec![ast::PathPattern::Node(ast::NodePattern {
                name: Some("a"),
                labels: Some(vec!["User"]),
                properties: None,
            })],
            where_clause: None,
        };

        let input_plan = Arc::new(LogicalPlan::Empty);

        // Set up test schema for the test
        let graph_schema = setup_test_graph_schema();
        let mut plan_ctx = PlanCtx::new(Arc::new(graph_schema));

        let result = evaluate_optional_match_clause(&optional_match, input_plan, &mut plan_ctx);
        assert!(result.is_ok());
    }

    #[test]
    fn test_evaluate_optional_match_with_where() {
        let optional_match = ast::OptionalMatchClause {
            path_patterns: vec![ast::PathPattern::Node(ast::NodePattern {
                name: Some("a"),
                labels: Some(vec!["User"]),
                properties: None,
            })],
            where_clause: Some(ast::WhereClause {
                conditions: ast::Expression::OperatorApplicationExp(ast::OperatorApplication {
                    operator: ast::Operator::GreaterThan,
                    operands: vec![
                        ast::Expression::PropertyAccessExp(ast::PropertyAccess {
                            base: "a",
                            key: "age",
                        }),
                        ast::Expression::Literal(ast::Literal::Integer(25)),
                    ],
                }),
            }),
        };

        let input_plan = Arc::new(LogicalPlan::Empty);

        // Set up test schema for the test
        let graph_schema = setup_test_graph_schema();
        let mut plan_ctx = PlanCtx::new(Arc::new(graph_schema));

        let result = evaluate_optional_match_clause(&optional_match, input_plan, &mut plan_ctx);
        assert!(result.is_ok());
    }
}
