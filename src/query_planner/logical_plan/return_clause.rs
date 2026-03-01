//! RETURN clause processing.
//!
//! Handles Cypher's RETURN clause which projects and optionally aggregates results.
//! Creates [`Projection`] and [`GroupBy`] logical plan nodes.
//!
//! # Features
//!
//! - Simple projections: `RETURN u.name, u.email`
//! - Aggregations: `RETURN count(*), avg(score)`
//! - Aliasing: `RETURN u.name AS userName`
//! - DISTINCT: `RETURN DISTINCT u.country`
//! - Whole-entity: `RETURN u` (returns all properties as JSON)
//!
//! # Aggregation Handling
//!
//! When aggregates are detected, non-aggregate columns are automatically
//! added to GROUP BY (ClickHouse requires explicit grouping).

use crate::{
    open_cypher_parser::ast::{Expression, ReturnClause, ReturnItem},
    query_planner::logical_expr::{
        AggregateFnCall, ColumnAlias, LogicalExpr, PropertyAccess, TableAlias,
    },
    query_planner::logical_plan::{LogicalPlan, Projection, ProjectionItem, Union, UnionType},
    query_planner::plan_ctx::PlanCtx,
};
use std::collections::HashSet;

/// Type alias for pattern comprehension tuple to reduce complexity
type PatternComprehension<'a> = (
    crate::open_cypher_parser::ast::PathPattern<'a>,
    Option<Box<Expression<'a>>>,
    Box<Expression<'a>>,
);
use std::sync::Arc;

/// Check if an expression contains any aggregate function calls (recursively).
fn contains_aggregate(expr: &LogicalExpr) -> bool {
    match expr {
        LogicalExpr::AggregateFnCall(_) => true,
        LogicalExpr::OperatorApplicationExp(op) => op.operands.iter().any(contains_aggregate),
        LogicalExpr::ScalarFnCall(func) => func.args.iter().any(contains_aggregate),
        LogicalExpr::List(list) => list.iter().any(contains_aggregate),
        LogicalExpr::Case(case_expr) => {
            if let Some(expr) = &case_expr.expr {
                if contains_aggregate(expr) {
                    return true;
                }
            }
            for (when_cond, then_val) in &case_expr.when_then {
                if contains_aggregate(when_cond) || contains_aggregate(then_val) {
                    return true;
                }
            }
            if let Some(else_expr) = &case_expr.else_expr {
                if contains_aggregate(else_expr) {
                    return true;
                }
            }
            false
        }
        _ => false,
    }
}

/// Extract all PropertyAccess expressions from an expression (for columns needed in subquery).
/// For aggregate functions, extract from their arguments (except COUNT(*)).
fn extract_property_accesses(expr: &LogicalExpr, properties: &mut Vec<PropertyAccess>) {
    match expr {
        LogicalExpr::PropertyAccessExp(prop) => {
            properties.push(prop.clone());
        }
        LogicalExpr::AggregateFnCall(agg) => {
            // For aggregates, extract from arguments
            for arg in &agg.args {
                if !matches!(arg, LogicalExpr::Star) {
                    extract_property_accesses(arg, properties);
                }
            }
        }
        LogicalExpr::OperatorApplicationExp(op) => {
            for operand in &op.operands {
                extract_property_accesses(operand, properties);
            }
        }
        LogicalExpr::ScalarFnCall(func) => {
            for arg in &func.args {
                extract_property_accesses(arg, properties);
            }
        }
        LogicalExpr::List(list) => {
            for item in list {
                extract_property_accesses(item, properties);
            }
        }
        LogicalExpr::Case(case_expr) => {
            if let Some(e) = &case_expr.expr {
                extract_property_accesses(e, properties);
            }
            for (when_cond, then_val) in &case_expr.when_then {
                extract_property_accesses(when_cond, properties);
                extract_property_accesses(then_val, properties);
            }
            if let Some(else_expr) = &case_expr.else_expr {
                extract_property_accesses(else_expr, properties);
            }
        }
        // TableAlias (like `a`) doesn't give us specific columns - skip
        // Star is handled in aggregate case
        _ => {}
    }
}

/// Extract TableAliases from aggregate function arguments (for count(node) patterns).
/// These will need to be expanded to include the node's ID property.
fn extract_table_aliases_from_aggregates(expr: &LogicalExpr, aliases: &mut Vec<String>) {
    match expr {
        LogicalExpr::AggregateFnCall(agg) => {
            for arg in &agg.args {
                extract_table_aliases_inner(arg, aliases);
            }
        }
        LogicalExpr::OperatorApplicationExp(op) => {
            for operand in &op.operands {
                extract_table_aliases_from_aggregates(operand, aliases);
            }
        }
        LogicalExpr::ScalarFnCall(func) => {
            for arg in &func.args {
                extract_table_aliases_from_aggregates(arg, aliases);
            }
        }
        _ => {}
    }
}

/// Helper to extract TableAlias from expression (used for aggregate args)
fn extract_table_aliases_inner(expr: &LogicalExpr, aliases: &mut Vec<String>) {
    match expr {
        LogicalExpr::TableAlias(alias) => {
            aliases.push(alias.0.clone());
        }
        LogicalExpr::OperatorApplicationExp(op) => {
            // Handle DISTINCT node case: OperatorApplication { operator: Distinct, operands: [TableAlias] }
            for operand in &op.operands {
                extract_table_aliases_inner(operand, aliases);
            }
        }
        _ => {}
    }
}

/// Look up ID property for a node alias from a Union plan.
/// Returns the first available ID property found in the GraphNode's ViewScan.
fn lookup_node_id_property(alias: &str, union: &Union) -> Option<String> {
    // Look in first branch for the GraphNode with this alias
    for branch in &union.inputs {
        if let Some(id_prop) = find_node_id_in_plan(alias, branch) {
            return Some(id_prop);
        }
    }
    None
}

/// Recursively search for a GraphNode with the given alias and extract its ID property name.
fn find_node_id_in_plan(alias: &str, plan: &Arc<LogicalPlan>) -> Option<String> {
    match plan.as_ref() {
        LogicalPlan::GraphNode(gn) => {
            if gn.alias == alias {
                // Found the node - get first property from from_node_properties or to_node_properties
                if let LogicalPlan::ViewScan(vs) = gn.input.as_ref() {
                    // The first property in from/to_node_properties is typically the ID
                    // Look for a property that matches a common ID pattern or just return the first one
                    if let Some(ref props) = vs.from_node_properties {
                        // Return the first property name (which is the graph property, e.g., "ip")
                        return props.keys().next().cloned();
                    }
                    if let Some(ref props) = vs.to_node_properties {
                        return props.keys().next().cloned();
                    }
                }
            }
            // Recurse into input
            find_node_id_in_plan(alias, &gn.input)
        }
        LogicalPlan::Projection(proj) => find_node_id_in_plan(alias, &proj.input),
        LogicalPlan::ViewScan(_) => None,
        LogicalPlan::Union(u) => {
            for branch in &u.inputs {
                if let Some(id) = find_node_id_in_plan(alias, branch) {
                    return Some(id);
                }
            }
            None
        }
        _ => None,
    }
}

/// Check if any projection item contains an aggregate function
fn has_aggregation(items: &[ProjectionItem]) -> bool {
    items
        .iter()
        .any(|item| contains_aggregate(&item.expression))
}

/// Build a canonical key for a PropertyAccess for deduplication
fn property_key(prop: &PropertyAccess) -> String {
    format!("{}.{}", prop.table_alias.0, prop.column.raw())
}

/// Extract target node label and projected property from a pattern comprehension.
/// For `[(u)-[:FOLLOWS]->(f:User) | f.name]`:
///   - target_label = Some("User")
///   - target_property = Some("name")
fn extract_target_info(
    pattern: &crate::open_cypher_parser::ast::PathPattern<'_>,
    projection: &crate::open_cypher_parser::ast::Expression<'_>,
    correlation_var: &str,
) -> (Option<String>, Option<String>) {
    use crate::open_cypher_parser::ast::PathPattern;

    // Extract target node label from the pattern (the node that is NOT the correlation var)
    let target_label = match pattern {
        PathPattern::ConnectedPattern(connected) => {
            connected.iter().find_map(|conn| {
                let start = conn.start_node.borrow();
                let end = conn.end_node.borrow();
                // Target is the node that is NOT the correlation variable
                if start.name.map(|n| n == correlation_var).unwrap_or(false) {
                    end.first_label().map(|l| l.to_string())
                } else if end.name.map(|n| n == correlation_var).unwrap_or(false) {
                    start.first_label().map(|l| l.to_string())
                } else {
                    None
                }
            })
        }
        _ => None,
    };

    // Extract property from the projection expression (e.g., f.name → "name")
    let target_property = match projection {
        crate::open_cypher_parser::ast::Expression::PropertyAccessExp(pa) => {
            Some(pa.key.to_string())
        }
        _ => None,
    };

    (target_label, target_property)
}

/// Rewrite pattern comprehensions in return items
/// Modifies the plan by adding OPTIONAL MATCH nodes and replaces pattern comprehensions with collect()
/// Recursively rewrite pattern comprehensions within an expression.
/// Returns (transformed_expression, pattern_comprehensions_found) where pattern_comprehensions_found
/// is a list of (pattern, where_clause, projection) tuples that need OPTIONAL MATCH nodes added.
fn rewrite_expression_pattern_comprehensions<'a>(
    expr: Expression<'a>,
) -> (Expression<'a>, Vec<PatternComprehension<'a>>) {
    use crate::open_cypher_parser::ast::*;

    match expr {
        Expression::PatternComprehension(pc) => {
            // Found a pattern comprehension - collect it and replace with collect(projection)
            let collect_call = Expression::FunctionCallExp(FunctionCall {
                name: "collect".to_string(),
                args: vec![(*pc.projection).clone()],
            });
            (
                collect_call,
                vec![(
                    (*pc.pattern).clone(),
                    pc.where_clause.clone(),
                    pc.projection.clone(),
                )],
            )
        }
        Expression::FunctionCallExp(func) => {
            // Special case: size(PatternComprehension) should become count(*)
            // NOT size(collect(projection)) which is semantically wrong
            let func_lower = func.name.to_lowercase();
            if (func_lower == "size" || func_lower == "length") && func.args.len() == 1 {
                if let Expression::PatternComprehension(pc) = &func.args[0] {
                    log::info!(
                        "🔄 Found size/length(PatternComprehension), replacing with count(*)"
                    );
                    // Replace size([(pattern) | proj]) with count(*)
                    // The pattern will be added as OPTIONAL MATCH
                    let count_call = Expression::FunctionCallExp(FunctionCall {
                        name: "count".to_string(),
                        args: vec![Expression::Literal(Literal::String("*"))],
                    });
                    return (
                        count_call,
                        vec![(
                            (*pc.pattern).clone(),
                            pc.where_clause.clone(),
                            pc.projection.clone(),
                        )],
                    );
                }
            }

            // Default: Recursively process function arguments
            let mut all_pcs = Vec::new();
            let new_args: Vec<Expression<'a>> = func
                .args
                .into_iter()
                .map(|arg| {
                    let (new_arg, pcs) = rewrite_expression_pattern_comprehensions(arg);
                    all_pcs.extend(pcs);
                    new_arg
                })
                .collect();
            (
                Expression::FunctionCallExp(FunctionCall {
                    name: func.name,
                    args: new_args,
                }),
                all_pcs,
            )
        }
        Expression::OperatorApplicationExp(op) => {
            let mut all_pcs = Vec::new();
            let new_operands: Vec<Expression<'a>> = op
                .operands
                .into_iter()
                .map(|operand| {
                    let (new_op, pcs) = rewrite_expression_pattern_comprehensions(operand);
                    all_pcs.extend(pcs);
                    new_op
                })
                .collect();
            (
                Expression::OperatorApplicationExp(OperatorApplication {
                    operator: op.operator,
                    operands: new_operands,
                }),
                all_pcs,
            )
        }
        Expression::List(items) => {
            let mut all_pcs = Vec::new();
            let new_items: Vec<Expression<'a>> = items
                .into_iter()
                .map(|item| {
                    let (new_item, pcs) = rewrite_expression_pattern_comprehensions(item);
                    all_pcs.extend(pcs);
                    new_item
                })
                .collect();
            (Expression::List(new_items), all_pcs)
        }
        Expression::Case(case_expr) => {
            let mut all_pcs = Vec::new();

            let new_expr = case_expr.expr.map(|e| {
                let (new_e, pcs) = rewrite_expression_pattern_comprehensions(*e);
                all_pcs.extend(pcs);
                Box::new(new_e)
            });

            let new_when_then: Vec<(Expression<'a>, Expression<'a>)> = case_expr
                .when_then
                .into_iter()
                .map(|(when, then)| {
                    let (new_when, pcs1) = rewrite_expression_pattern_comprehensions(when);
                    let (new_then, pcs2) = rewrite_expression_pattern_comprehensions(then);
                    all_pcs.extend(pcs1);
                    all_pcs.extend(pcs2);
                    (new_when, new_then)
                })
                .collect();

            let new_else = case_expr.else_expr.map(|e| {
                let (new_e, pcs) = rewrite_expression_pattern_comprehensions(*e);
                all_pcs.extend(pcs);
                Box::new(new_e)
            });

            (
                Expression::Case(Case {
                    expr: new_expr,
                    when_then: new_when_then,
                    else_expr: new_else,
                }),
                all_pcs,
            )
        }
        // For all other expression types, return as-is with no pattern comprehensions
        other => (other, vec![]),
    }
}

fn rewrite_pattern_comprehensions<'a>(
    return_items: Vec<ReturnItem<'a>>,
    plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
) -> (
    Vec<ReturnItem<'a>>,
    Arc<LogicalPlan>,
    Vec<crate::query_planner::logical_plan::PatternComprehensionMeta>,
) {
    let mut rewritten_items = Vec::new();
    let mut all_metas = Vec::new();
    let mut pc_counter = 0usize;

    for item in return_items {
        // Recursively rewrite pattern comprehensions in the expression
        let (rewritten_expr, pattern_comprehensions) =
            rewrite_expression_pattern_comprehensions(item.expression);

        // Extract metadata for CTE+JOIN generation (same approach as WITH clause)
        for (pattern, _where_clause, projection) in pattern_comprehensions {
            use crate::query_planner::logical_plan::with_clause::{
                extract_correlation_variable_from_pattern, extract_direction_and_rel_types,
            };
            use crate::query_planner::logical_plan::AggregationType;

            let correlation_var = extract_correlation_variable_from_pattern(&pattern, plan_ctx);
            if correlation_var.is_none() {
                log::warn!(
                    "⚠️  RETURN pattern comprehension has no correlation variable - skipping"
                );
                continue;
            }
            let correlation_var = correlation_var.unwrap();

            let correlation_label = plan_ctx
                .get_table_ctx(&correlation_var)
                .ok()
                .and_then(|ctx| ctx.get_labels().cloned())
                .and_then(|labels| labels.into_iter().next())
                .unwrap_or_default();

            let (direction, rel_types) = extract_direction_and_rel_types(&pattern);

            // Extract target node label and projected property from the pattern
            let (target_label, target_property) =
                extract_target_info(&pattern, &projection, &correlation_var);

            // Determine aggregation type from the rewritten expression
            let agg_type = match &rewritten_expr {
                Expression::FunctionCallExp(fc) if fc.name.eq_ignore_ascii_case("collect") => {
                    AggregationType::GroupArray
                }
                Expression::FunctionCallExp(fc) if fc.name.eq_ignore_ascii_case("count") => {
                    AggregationType::Count
                }
                Expression::FunctionCallExp(fc) if fc.name.eq_ignore_ascii_case("sum") => {
                    AggregationType::Sum
                }
                Expression::FunctionCallExp(fc) if fc.name.eq_ignore_ascii_case("avg") => {
                    AggregationType::Avg
                }
                Expression::FunctionCallExp(fc) if fc.name.eq_ignore_ascii_case("min") => {
                    AggregationType::Min
                }
                Expression::FunctionCallExp(fc) if fc.name.eq_ignore_ascii_case("max") => {
                    AggregationType::Max
                }
                _ => AggregationType::Count,
            };

            let result_alias = item
                .alias
                .map(|a| a.to_string())
                .unwrap_or_else(|| format!("__pc_{}", pc_counter));

            log::info!(
                "🔧 RETURN pattern comprehension meta: var='{}', label='{}', dir={:?}, rels={:?}, alias='{}', target={:?}/{:?}",
                correlation_var, correlation_label, direction, rel_types, result_alias, target_label, target_property
            );

            all_metas.push(
                crate::query_planner::logical_plan::PatternComprehensionMeta {
                    correlation_var: correlation_var.clone(),
                    correlation_label,
                    direction,
                    rel_types,
                    agg_type,
                    result_alias: result_alias.clone(),
                    target_label,
                    target_property,
                    correlation_vars: vec![],
                    pattern_hops: vec![],
                    where_clause: None,
                    position_index: pc_counter,
                    list_constraint: None,
                },
            );

            pc_counter += 1;
        }

        let new_item = ReturnItem {
            expression: rewritten_expr,
            alias: item.alias,
            original_text: item.original_text,
        };
        rewritten_items.push(new_item);
    }

    (rewritten_items, plan, all_metas)
}

pub fn evaluate_return_clause<'a>(
    return_clause: &ReturnClause<'a>,
    plan: Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
) -> Arc<LogicalPlan> {
    crate::debug_print!("========================================");
    crate::debug_print!("⚠️ RETURN CLAUSE DISTINCT = {}", return_clause.distinct);
    crate::debug_print!(
        "⚠️ RETURN AST items count = {}",
        return_clause.return_items.len()
    );
    for (_i, _item) in return_clause.return_items.iter().enumerate() {
        crate::debug_print!(
            "⚠️ RETURN AST item {}: expr={:?}, alias={:?}",
            _i,
            _item.expression,
            _item.alias
        );
    }
    crate::debug_print!("========================================");

    // Rewrite pattern comprehensions before converting to ProjectionItems
    let (rewritten_return_items, plan, pattern_comp_metas) =
        rewrite_pattern_comprehensions(return_clause.return_items.clone(), plan, plan_ctx);

    let projection_items: Vec<ProjectionItem> = rewritten_return_items
        .iter()
        .map(|item| ProjectionItem::from(item.clone()))
        .collect();

    // If input is a Union, handle specially
    if let LogicalPlan::Union(union) = plan.as_ref() {
        crate::debug_println!("DEBUG: Input is Union with {} branches", union.inputs.len());

        // Check if we have aggregations
        if has_aggregation(&projection_items) {
            crate::debug_println!("DEBUG: Union + aggregation detected - using subquery pattern");
            return build_union_with_aggregation(union, &projection_items, return_clause.distinct);
        }

        // No aggregation - push Projection into each branch as before
        crate::debug_println!(
            "DEBUG: No aggregation, pushing Projection into {} branches",
            union.inputs.len()
        );
        let projected_branches: Vec<Arc<LogicalPlan>> = union
            .inputs
            .iter()
            .map(|branch| {
                Arc::new(LogicalPlan::Projection(Projection {
                    input: branch.clone(),
                    items: projection_items.clone(),
                    distinct: return_clause.distinct,
                    pattern_comprehensions: vec![],
                }))
            })
            .collect();

        // For RETURN DISTINCT with Union:
        // - Use UNION (not UNION ALL) to deduplicate across branches
        let union_type = if return_clause.distinct {
            UnionType::Distinct
        } else {
            union.union_type.clone()
        };

        return Arc::new(LogicalPlan::Union(Union {
            inputs: projected_branches,
            union_type,
        }));
    }

    let result = Arc::new(LogicalPlan::Projection(Projection {
        input: plan,
        items: projection_items,
        distinct: return_clause.distinct,
        pattern_comprehensions: pattern_comp_metas,
    }));
    crate::debug_println!(
        "DEBUG evaluate_return_clause: Created Projection with distinct={}",
        if let LogicalPlan::Projection(p) = result.as_ref() {
            p.distinct
        } else {
            false
        }
    );
    result
}

/// Build a Union with aggregation using subquery pattern.
///
/// For `MATCH (a:Airport) RETURN a.code, count(*) as cnt`, generates:
/// ```text
/// Projection(outer) [a.code, count(*)]
///   └── GroupBy [a.code]
///         └── Union
///               ├── Projection(branch1) [a.code]  -- only needed columns
///               └── Projection(branch2) [a.code]
/// ```
///
/// This keeps aggregation at the outer level, with Union providing the combined rows.
fn build_union_with_aggregation(
    union: &Union,
    projection_items: &[ProjectionItem],
    distinct: bool,
) -> Arc<LogicalPlan> {
    // Step 1: Collect all property accesses needed from projection items
    // These are the columns we need in the inner SELECT (subquery)
    let mut all_properties: Vec<PropertyAccess> = Vec::new();
    let mut seen_keys: HashSet<String> = HashSet::new();

    for item in projection_items {
        let mut item_props: Vec<PropertyAccess> = Vec::new();
        extract_property_accesses(&item.expression, &mut item_props);

        for prop in item_props {
            let key = property_key(&prop);
            if !seen_keys.contains(&key) {
                seen_keys.insert(key);
                all_properties.push(prop);
            }
        }
    }

    // Step 1b: Also collect TableAliases from aggregate function arguments
    // These represent count(node) patterns that need the node's ID property
    let mut table_aliases_in_aggs: Vec<String> = Vec::new();
    for item in projection_items {
        extract_table_aliases_from_aggregates(&item.expression, &mut table_aliases_in_aggs);
    }

    // For each TableAlias in an aggregate, look up its ID property and add to all_properties
    for alias in &table_aliases_in_aggs {
        if let Some(id_prop) = lookup_node_id_property(alias, union) {
            let key = format!("{}.{}", alias, id_prop);
            if !seen_keys.contains(&key) {
                crate::debug_println!(
                    "DEBUG: Adding ID property '{}.{}' for count({})",
                    alias,
                    id_prop,
                    alias
                );
                seen_keys.insert(key);
                all_properties.push(PropertyAccess {
                    table_alias: TableAlias(alias.clone()),
                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(id_prop),
                });
            }
        }
    }

    crate::debug_println!(
        "DEBUG: Collected {} unique properties for inner SELECT",
        all_properties.len()
    );
    for prop in &all_properties {
        println!("  - {}.{}", prop.table_alias.0, prop.column.raw());
    }

    // Step 2: Build inner projection items for each Union branch
    // If no properties needed (e.g., COUNT(*) only), use constant 1
    let inner_items: Vec<ProjectionItem> = if all_properties.is_empty() {
        crate::debug_println!("DEBUG: No properties needed, using constant 1");
        vec![ProjectionItem {
            expression: LogicalExpr::Literal(crate::query_planner::logical_expr::Literal::Integer(
                1,
            )),
            col_alias: Some(ColumnAlias("__const".to_string())),
        }]
    } else {
        all_properties
            .iter()
            .map(|prop| {
                // Create alias like "a.code" for the property
                let alias = format!("{}.{}", prop.table_alias.0, prop.column.raw());
                ProjectionItem {
                    expression: LogicalExpr::PropertyAccessExp(prop.clone()),
                    col_alias: Some(ColumnAlias(alias)),
                }
            })
            .collect()
    };

    // Step 3: Create projected branches (inner SELECT for each Union branch)
    let projected_branches: Vec<Arc<LogicalPlan>> = union
        .inputs
        .iter()
        .map(|branch| {
            Arc::new(LogicalPlan::Projection(Projection {
                input: branch.clone(),
                items: inner_items.clone(),
                distinct: false, // No DISTINCT on inner - UNION will handle dedup if needed
                pattern_comprehensions: vec![],
            }))
        })
        .collect();

    // Step 4: Create the inner Union with projected branches
    let inner_union = Arc::new(LogicalPlan::Union(Union {
        inputs: projected_branches,
        union_type: union.union_type.clone(),
    }));

    // Step 5: Collect non-aggregate items for GROUP BY
    let grouping_exprs: Vec<LogicalExpr> = projection_items
        .iter()
        .filter(|item| !contains_aggregate(&item.expression))
        .map(|item| {
            // Rewrite PropertyAccess to ColumnAlias referencing the inner projection
            rewrite_to_column_alias(&item.expression)
        })
        .collect();

    crate::debug_println!(
        "DEBUG: {} grouping expressions for outer GROUP BY",
        grouping_exprs.len()
    );

    // Step 6: Create outer projection items (rewritten to reference inner aliases)
    let outer_items: Vec<ProjectionItem> = projection_items
        .iter()
        .map(|item| ProjectionItem {
            expression: rewrite_to_column_alias(&item.expression),
            col_alias: item.col_alias.clone(),
        })
        .collect();

    // Step 7: Build the complete plan
    // If we have grouping expressions, wrap in GroupBy
    if !grouping_exprs.is_empty()
        || projection_items
            .iter()
            .any(|item| contains_aggregate(&item.expression))
    {
        use crate::query_planner::logical_plan::GroupBy;

        // Create outer projection over GroupBy over Union
        let group_by = Arc::new(LogicalPlan::GroupBy(GroupBy {
            input: inner_union,
            expressions: grouping_exprs,
            having_clause: None,
            is_materialization_boundary: false,
            exposed_alias: None,
        }));

        Arc::new(LogicalPlan::Projection(Projection {
            input: group_by,
            items: outer_items,
            distinct,
            pattern_comprehensions: vec![],
        }))
    } else {
        // No aggregation after all (shouldn't happen if we got here, but safe fallback)
        Arc::new(LogicalPlan::Projection(Projection {
            input: inner_union,
            items: outer_items,
            distinct,
            pattern_comprehensions: vec![],
        }))
    }
}

/// Rewrite an expression to use ColumnAlias references instead of PropertyAccess.
/// For example, `a.code` becomes a reference to the column alias "a.code" from the subquery.
/// Aggregate functions are preserved but their arguments are rewritten.
fn rewrite_to_column_alias(expr: &LogicalExpr) -> LogicalExpr {
    match expr {
        LogicalExpr::PropertyAccessExp(prop) => {
            // Convert to column alias reference
            let alias = format!("{}.{}", prop.table_alias.0, prop.column.raw());
            LogicalExpr::ColumnAlias(ColumnAlias(alias))
        }
        LogicalExpr::AggregateFnCall(agg) => {
            // Rewrite aggregate arguments
            let new_args: Vec<LogicalExpr> = agg
                .args
                .iter()
                .map(|arg| {
                    if matches!(arg, LogicalExpr::Star) {
                        arg.clone() // Keep Star as-is
                    } else {
                        rewrite_to_column_alias(arg)
                    }
                })
                .collect();

            LogicalExpr::AggregateFnCall(AggregateFnCall {
                name: agg.name.clone(),
                args: new_args,
            })
        }
        LogicalExpr::OperatorApplicationExp(op) => {
            use crate::query_planner::logical_expr::OperatorApplication;
            LogicalExpr::OperatorApplicationExp(OperatorApplication {
                operator: op.operator,
                operands: op.operands.iter().map(rewrite_to_column_alias).collect(),
            })
        }
        LogicalExpr::ScalarFnCall(func) => {
            use crate::query_planner::logical_expr::ScalarFnCall;
            LogicalExpr::ScalarFnCall(ScalarFnCall {
                name: func.name.clone(),
                args: func.args.iter().map(rewrite_to_column_alias).collect(),
            })
        }
        // For other expressions, return as-is
        other => other.clone(),
    }
}
