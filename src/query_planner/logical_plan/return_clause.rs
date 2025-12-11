use crate::{
    open_cypher_parser::ast::ReturnClause,
    query_planner::logical_expr::{AggregateFnCall, ColumnAlias, LogicalExpr, PropertyAccess, TableAlias},
    query_planner::logical_plan::{LogicalPlan, Projection, ProjectionItem, ProjectionKind, Union, UnionType},
};
use std::sync::Arc;
use std::collections::HashSet;

/// Check if an expression contains any aggregate function calls (recursively).
fn contains_aggregate(expr: &LogicalExpr) -> bool {
    match expr {
        LogicalExpr::AggregateFnCall(_) => true,
        LogicalExpr::OperatorApplicationExp(op) => op
            .operands
            .iter()
            .any(|operand| contains_aggregate(operand)),
        LogicalExpr::ScalarFnCall(func) => {
            func.args.iter().any(|arg| contains_aggregate(arg))
        }
        LogicalExpr::List(list) => list.iter().any(|item| contains_aggregate(item)),
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
    items.iter().any(|item| contains_aggregate(&item.expression))
}

/// Build a canonical key for a PropertyAccess for deduplication
fn property_key(prop: &PropertyAccess) -> String {
    format!("{}.{}", prop.table_alias.0, prop.column.raw())
}

pub fn evaluate_return_clause<'a>(
    return_clause: &ReturnClause<'a>,
    plan: Arc<LogicalPlan>,
) -> Arc<LogicalPlan> {
    crate::debug_print!("========================================");
    crate::debug_print!("⚠️ RETURN CLAUSE DISTINCT = {}", return_clause.distinct);
    crate::debug_print!("⚠️ RETURN AST items count = {}", return_clause.return_items.len());
    for (_i, _item) in return_clause.return_items.iter().enumerate() {
        crate::debug_print!("⚠️ RETURN AST item {}: expr={:?}, alias={:?}", _i, _item.expression, _item.alias);
    }
    crate::debug_print!("========================================");
    let projection_items: Vec<ProjectionItem> = return_clause
        .return_items
        .iter()
        .map(|item| item.clone().into())
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
        crate::debug_println!("DEBUG: No aggregation, pushing Projection into {} branches", union.inputs.len());
        let projected_branches: Vec<Arc<LogicalPlan>> = union.inputs.iter().map(|branch| {
            Arc::new(LogicalPlan::Projection(Projection {
                input: branch.clone(),
                items: projection_items.clone(),
                kind: ProjectionKind::Return,
                distinct: return_clause.distinct,
            }))
        }).collect();
        
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
        kind: ProjectionKind::Return,
        distinct: return_clause.distinct,
    }));
    crate::debug_println!("DEBUG evaluate_return_clause: Created Projection with distinct={}", 
        if let LogicalPlan::Projection(p) = result.as_ref() { p.distinct } else { false });
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
                crate::debug_println!("DEBUG: Adding ID property '{}.{}' for count({})", alias, id_prop, alias);
                seen_keys.insert(key);
                all_properties.push(PropertyAccess {
                    table_alias: TableAlias(alias.clone()),
                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(id_prop),
                });
            }
        }
    }
    
    crate::debug_println!("DEBUG: Collected {} unique properties for inner SELECT", all_properties.len());
    for prop in &all_properties {
        println!("  - {}.{}", prop.table_alias.0, prop.column.raw());
    }
    
    // Step 2: Build inner projection items for each Union branch
    // If no properties needed (e.g., COUNT(*) only), use constant 1
    let inner_items: Vec<ProjectionItem> = if all_properties.is_empty() {
        crate::debug_println!("DEBUG: No properties needed, using constant 1");
        vec![ProjectionItem {
            expression: LogicalExpr::Literal(crate::query_planner::logical_expr::Literal::Integer(1)),
            col_alias: Some(ColumnAlias("__const".to_string())),
        }]
    } else {
        all_properties.iter().map(|prop| {
            // Create alias like "a.code" for the property
            let alias = format!("{}.{}", prop.table_alias.0, prop.column.raw());
            ProjectionItem {
                expression: LogicalExpr::PropertyAccessExp(prop.clone()),
                col_alias: Some(ColumnAlias(alias)),
            }
        }).collect()
    };
    
    // Step 3: Create projected branches (inner SELECT for each Union branch)
    let projected_branches: Vec<Arc<LogicalPlan>> = union.inputs.iter().map(|branch| {
        Arc::new(LogicalPlan::Projection(Projection {
            input: branch.clone(),
            items: inner_items.clone(),
            kind: ProjectionKind::With, // Mark as inner projection (like WITH)
            distinct: false, // No DISTINCT on inner - UNION will handle dedup if needed
        }))
    }).collect();
    
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
    
    crate::debug_println!("DEBUG: {} grouping expressions for outer GROUP BY", grouping_exprs.len());
    
    // Step 6: Create outer projection items (rewritten to reference inner aliases)
    let outer_items: Vec<ProjectionItem> = projection_items
        .iter()
        .map(|item| {
            ProjectionItem {
                expression: rewrite_to_column_alias(&item.expression),
                col_alias: item.col_alias.clone(),
            }
        })
        .collect();
    
    // Step 7: Build the complete plan
    // If we have grouping expressions, wrap in GroupBy
    if !grouping_exprs.is_empty() || projection_items.iter().any(|item| contains_aggregate(&item.expression)) {
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
            kind: ProjectionKind::Return,
            distinct,
        }))
    } else {
        // No aggregation after all (shouldn't happen if we got here, but safe fallback)
        Arc::new(LogicalPlan::Projection(Projection {
            input: inner_union,
            items: outer_items,
            kind: ProjectionKind::Return,
            distinct,
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
            let new_args: Vec<LogicalExpr> = agg.args.iter().map(|arg| {
                if matches!(arg, LogicalExpr::Star) {
                    arg.clone() // Keep Star as-is
                } else {
                    rewrite_to_column_alias(arg)
                }
            }).collect();
            
            LogicalExpr::AggregateFnCall(AggregateFnCall {
                name: agg.name.clone(),
                args: new_args,
            })
        }
        LogicalExpr::OperatorApplicationExp(op) => {
            use crate::query_planner::logical_expr::OperatorApplication;
            LogicalExpr::OperatorApplicationExp(OperatorApplication {
                operator: op.operator.clone(),
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

