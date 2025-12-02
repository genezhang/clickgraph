use std::{collections::HashSet, sync::Arc};

use crate::{
    graph_catalog::graph_schema::{GraphSchema, is_node_denormalized_on_edge, NodeSchema, RelationshipSchema},
    query_planner::{
        analyzer::{
            analyzer_pass::{AnalyzerPass, AnalyzerResult},
            errors::Pass,
            graph_context,
        },
        logical_expr::{
            Direction, LogicalExpr, Operator, OperatorApplication, PropertyAccess, TableAlias,
        },
        logical_plan::{GraphJoins, GraphRel, Join, JoinType, LogicalPlan},
        plan_ctx::PlanCtx,
        transformed::Transformed,
    },
    render_plan::cte_extraction::extract_relationship_columns,
};

/// Generate a polymorphic edge filter for relationships stored in a unified table
/// with type discriminator columns.
///
/// For polymorphic edges, we need to filter by:
/// - `type_column = 'rel_type'` - The relationship type (e.g., 'FOLLOWS')
/// - `type_column IN ('TYPE1', 'TYPE2')` - For multiple types (alternate relationship types)
/// - `from_label_column = 'FromLabel'` - The source node type (if configured)
/// - `to_label_column = 'ToLabel'` - The target node type (if configured)
fn generate_polymorphic_edge_filter(
    rel_alias: &str,
    rel_types: &[String],
    rel_schema: &RelationshipSchema,
    left_label: &str,
    right_label: &str,
) -> Option<LogicalExpr> {
    let mut filter_parts = Vec::new();

    // Add type filter if type_column is defined
    if let Some(ref type_col) = rel_schema.type_column {
        if rel_types.len() == 1 {
            // Single type: use = 'TYPE'
            filter_parts.push(format!("{}.{} = '{}'", rel_alias, type_col, rel_types[0]));
        } else if rel_types.len() > 1 {
            // Multiple types: use IN ('TYPE1', 'TYPE2')
            let types_list = rel_types.iter()
                .map(|t| format!("'{}'", t))
                .collect::<Vec<_>>()
                .join(", ");
            filter_parts.push(format!("{}.{} IN ({})", rel_alias, type_col, types_list));
        }
    }

    // Add from_label filter if from_label_column is defined and we're filtering by from type
    // Skip if schema uses $any wildcard (polymorphic from any source)
    if let Some(ref from_label_col) = rel_schema.from_label_column {
        if rel_schema.from_node != "$any" && !left_label.is_empty() {
            filter_parts.push(format!("{}.{} = '{}'", rel_alias, from_label_col, left_label));
        }
    }

    // Add to_label filter if to_label_column is defined and we're filtering by to type
    // Skip if schema uses $any wildcard (polymorphic to any target)
    if let Some(ref to_label_col) = rel_schema.to_label_column {
        if rel_schema.to_node != "$any" && !right_label.is_empty() {
            filter_parts.push(format!("{}.{} = '{}'", rel_alias, to_label_col, right_label));
        }
    }

    if filter_parts.is_empty() {
        None
    } else {
        let filter_sql = filter_parts.join(" AND ");
        eprintln!("    🔹 Polymorphic edge filter: {}", filter_sql);
        Some(LogicalExpr::Raw(filter_sql))
    }
}

/// Generate a join condition for the relationship to anchor node (undirected).
/// 
/// For undirected patterns, the relationship can connect to the anchor in either direction:
/// r.from_id = anchor.id OR r.to_id = anchor.id
fn generate_rel_to_anchor_bidirectional(
    rel_alias: &str,
    rel_from_col: &str,
    rel_to_col: &str,
    anchor_alias: &str,
    anchor_id_col: &str,
) -> OperatorApplication {
    use crate::graph_catalog::expression_parser::PropertyValue;
    
    // r.from_id = anchor.id
    let forward = LogicalExpr::OperatorApplicationExp(OperatorApplication {
        operator: Operator::Equal,
        operands: vec![
            LogicalExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(rel_alias.to_string()),
                column: PropertyValue::Column(rel_from_col.to_string()),
            }),
            LogicalExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(anchor_alias.to_string()),
                column: PropertyValue::Column(anchor_id_col.to_string()),
            }),
        ],
    });
    
    // r.to_id = anchor.id
    let reverse = LogicalExpr::OperatorApplicationExp(OperatorApplication {
        operator: Operator::Equal,
        operands: vec![
            LogicalExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(rel_alias.to_string()),
                column: PropertyValue::Column(rel_to_col.to_string()),
            }),
            LogicalExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(anchor_alias.to_string()),
                column: PropertyValue::Column(anchor_id_col.to_string()),
            }),
        ],
    });
    
    eprintln!("    🔹 Bidirectional rel->anchor: {}.{} = {}.{} OR {}.{} = {}.{}",
        rel_alias, rel_from_col, anchor_alias, anchor_id_col,
        rel_alias, rel_to_col, anchor_alias, anchor_id_col);
    
    // Combine with OR
    OperatorApplication {
        operator: Operator::Or,
        operands: vec![forward, reverse],
    }
}

/// Generate a join condition for target node to relationship (undirected).
/// 
/// For undirected patterns, the target connects to the relationship's other end:
/// (b.id = r.from_id OR b.id = r.to_id) AND b.id <> anchor.id
fn generate_target_to_rel_bidirectional(
    target_alias: &str,
    target_id_col: &str,
    rel_alias: &str,
    rel_from_col: &str,
    rel_to_col: &str,
    anchor_alias: &str,
    anchor_id_col: &str,
) -> OperatorApplication {
    use crate::graph_catalog::expression_parser::PropertyValue;
    
    // b.id = r.from_id
    let to_from = LogicalExpr::OperatorApplicationExp(OperatorApplication {
        operator: Operator::Equal,
        operands: vec![
            LogicalExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(target_alias.to_string()),
                column: PropertyValue::Column(target_id_col.to_string()),
            }),
            LogicalExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(rel_alias.to_string()),
                column: PropertyValue::Column(rel_from_col.to_string()),
            }),
        ],
    });
    
    // b.id = r.to_id
    let to_to = LogicalExpr::OperatorApplicationExp(OperatorApplication {
        operator: Operator::Equal,
        operands: vec![
            LogicalExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(target_alias.to_string()),
                column: PropertyValue::Column(target_id_col.to_string()),
            }),
            LogicalExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(rel_alias.to_string()),
                column: PropertyValue::Column(rel_to_col.to_string()),
            }),
        ],
    });
    
    // (b.id = r.from_id OR b.id = r.to_id)
    let either_end = LogicalExpr::OperatorApplicationExp(OperatorApplication {
        operator: Operator::Or,
        operands: vec![to_from, to_to],
    });
    
    // b.id <> anchor.id (exclude self-loops)
    let not_anchor = LogicalExpr::OperatorApplicationExp(OperatorApplication {
        operator: Operator::NotEqual,
        operands: vec![
            LogicalExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(target_alias.to_string()),
                column: PropertyValue::Column(target_id_col.to_string()),
            }),
            LogicalExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(anchor_alias.to_string()),
                column: PropertyValue::Column(anchor_id_col.to_string()),
            }),
        ],
    });
    
    eprintln!("    🔹 Bidirectional target->rel: ({}.{} = {}.{} OR {}.{} = {}.{}) AND {}.{} <> {}.{}",
        target_alias, target_id_col, rel_alias, rel_from_col,
        target_alias, target_id_col, rel_alias, rel_to_col,
        target_alias, target_id_col, anchor_alias, anchor_id_col);
    
    // Combine with AND
    OperatorApplication {
        operator: Operator::And,
        operands: vec![either_end, not_anchor],
    }
}

pub struct GraphJoinInference;

impl AnalyzerPass for GraphJoinInference {
    fn analyze_with_graph_schema(
        &self,
        logical_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        println!(
            "DEBUG GraphJoinInference: analyze_with_graph_schema called, plan type: {:?}",
            std::mem::discriminant(&*logical_plan)
        );

        let mut collected_graph_joins: Vec<Join> = vec![];
        let mut joined_entities: HashSet<String> = HashSet::new();
        self.collect_graph_joins(
            logical_plan.clone(),
            logical_plan.clone(), // Pass root plan for reference checking
            plan_ctx,
            graph_schema,
            &mut collected_graph_joins,
            &mut joined_entities,
        )?;

        println!(
            "DEBUG GraphJoinInference: collected_graph_joins.len() = {}",
            collected_graph_joins.len()
        );

        // CRITICAL: Always wrap in GraphJoins, even if empty!
        // Empty joins vector = fully denormalized pattern (no JOINs needed)
        // Without this wrapper, RenderPlan will try to generate JOINs from raw GraphRel
        let optional_aliases = plan_ctx.get_optional_aliases().clone();
        Self::build_graph_joins(logical_plan, &mut collected_graph_joins, optional_aliases, plan_ctx, graph_schema)
    }
}

impl GraphJoinInference {
    pub fn new() -> Self {
        GraphJoinInference
    }

    /// Determines the appropriate join type based on whether the table alias
    /// is part of an OPTIONAL MATCH pattern. Returns LEFT for optional aliases,
    /// INNER for regular aliases.
    fn determine_join_type(is_optional: bool) -> JoinType {
        if is_optional {
            JoinType::Left
        } else {
            JoinType::Inner
        }
    }

    /// Check if a node is actually referenced in the query (SELECT, WHERE, ORDER BY, etc.)
    /// Returns true if the node has any projections or filters, meaning it must be joined.
    fn is_node_referenced(alias: &str, plan_ctx: &PlanCtx, logical_plan: &LogicalPlan) -> bool {
        eprintln!("        DEBUG: is_node_referenced('{}') called", alias);

        // Search the logical plan tree for any Projection nodes
        if Self::plan_references_alias(logical_plan, alias) {
            eprintln!("        DEBUG: '{}' IS referenced in logical plan", alias);
            return true;
        }

        // Also check filters in plan_ctx
        for (_ctx_alias, table_ctx) in plan_ctx.get_alias_table_ctx_map().iter() {
            for filter in table_ctx.get_filters() {
                if Self::expr_references_alias(filter, alias) {
                    eprintln!("        DEBUG: '{}' IS referenced in filters", alias);
                    return true;
                }
            }
        }

        eprintln!("        DEBUG: '{}' is NOT referenced", alias);
        false
    }

    /// Recursively search a logical plan tree for references to an alias
    fn plan_references_alias(plan: &LogicalPlan, alias: &str) -> bool {
        match plan {
            LogicalPlan::Projection(proj) => {
                // Check projection items
                for item in &proj.items {
                    if Self::expr_references_alias(&item.expression, alias) {
                        return true;
                    }
                }
                // Recurse into input
                Self::plan_references_alias(&proj.input, alias)
            }
            LogicalPlan::GroupBy(group_by) => {
                // Check group expressions
                for expr in &group_by.expressions {
                    if Self::expr_references_alias(expr, alias) {
                        return true;
                    }
                }
                // Recurse into input
                Self::plan_references_alias(&group_by.input, alias)
            }
            LogicalPlan::Filter(filter) => {
                // Check filter expression
                if Self::expr_references_alias(&filter.predicate, alias) {
                    return true;
                }
                // Recurse into input
                Self::plan_references_alias(&filter.input, alias)
            }
            LogicalPlan::GraphRel(graph_rel) => {
                // Don't recurse into graph structure - just because a node appears in MATCH
                // doesn't mean it's referenced in SELECT/WHERE/etc.
                // Only check if there are filters on the relationship itself
                if let Some(where_pred) = &graph_rel.where_predicate {
                    if Self::expr_references_alias(where_pred, alias) {
                        return true;
                    }
                }
                false
            }
            LogicalPlan::GraphNode(graph_node) => {
                // Don't recurse into graph structure nodes
                // These represent the MATCH pattern, not actual data references
                false
            }
            LogicalPlan::GraphJoins(graph_joins) => {
                Self::plan_references_alias(&graph_joins.input, alias)
            }
            LogicalPlan::Cte(cte) => Self::plan_references_alias(&cte.input, alias),
            LogicalPlan::OrderBy(order_by) => {
                // Check order expressions
                for sort_expr in &order_by.items {
                    if Self::expr_references_alias(&sort_expr.expression, alias) {
                        return true;
                    }
                }
                // Recurse into input
                Self::plan_references_alias(&order_by.input, alias)
            }
            LogicalPlan::Skip(skip) => {
                // Skip doesn't have expressions, just recurse
                Self::plan_references_alias(&skip.input, alias)
            }
            LogicalPlan::Limit(limit) => {
                // Limit doesn't have expressions, just recurse
                Self::plan_references_alias(&limit.input, alias)
            }
            _ => false, // ViewScan, Scan, Empty, etc.
        }
    }

    /// Recursively check if an expression references a given alias
    /// This handles cases like COUNT(b) where 'b' is inside an aggregation function
    fn expr_references_alias(expr: &LogicalExpr, alias: &str) -> bool {
        match expr {
            LogicalExpr::TableAlias(table_alias) => table_alias.0 == alias,
            LogicalExpr::PropertyAccessExp(prop) => prop.table_alias.0 == alias,
            LogicalExpr::AggregateFnCall(agg) => {
                // Check arguments of aggregation functions (e.g., COUNT(b))
                agg.args
                    .iter()
                    .any(|arg| Self::expr_references_alias(arg, alias))
            }
            LogicalExpr::ScalarFnCall(fn_call) => {
                // Check arguments of scalar functions
                fn_call
                    .args
                    .iter()
                    .any(|arg| Self::expr_references_alias(arg, alias))
            }
            LogicalExpr::OperatorApplicationExp(op) => {
                // Check operands of operators
                op.operands
                    .iter()
                    .any(|operand| Self::expr_references_alias(operand, alias))
            }
            LogicalExpr::List(list) => {
                // Check elements in lists
                list.iter()
                    .any(|item| Self::expr_references_alias(item, alias))
            }
            LogicalExpr::Case(case) => {
                // Check CASE expressions
                if let Some(expr) = &case.expr {
                    if Self::expr_references_alias(expr, alias) {
                        return true;
                    }
                }
                for (when_expr, then_expr) in &case.when_then {
                    if Self::expr_references_alias(when_expr, alias)
                        || Self::expr_references_alias(then_expr, alias)
                    {
                        return true;
                    }
                }
                if let Some(else_expr) = &case.else_expr {
                    if Self::expr_references_alias(else_expr, alias) {
                        return true;
                    }
                }
                false
            }
            // Literals, columns, parameters, etc. don't reference table aliases
            _ => false,
        }
    }

    /// Reorder JOINs so that each JOIN only references tables that are already available
    /// (either from the FROM clause or from previous JOINs in the sequence)
    fn reorder_joins_by_dependencies(
        joins: Vec<Join>,
        optional_aliases: &std::collections::HashSet<String>,
        _plan_ctx: &PlanCtx,
    ) -> (Option<String>, Vec<Join>) {
        if joins.is_empty() {
            // No joins means denormalized pattern - no anchor needed (will use relationship table)
            return (None, joins);
        }

        eprintln!("\n🔄 REORDERING {} JOINS by dependencies", joins.len());

        // Collect all aliases that are being joined
        let mut join_aliases: std::collections::HashSet<String> = std::collections::HashSet::new();
        for join in &joins {
            join_aliases.insert(join.table_alias.clone());
        }

        // Find all tables referenced in JOIN conditions
        let mut referenced_tables: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        for join in &joins {
            for condition in &join.joining_on {
                for operand in &condition.operands {
                    Self::extract_table_refs_from_expr(operand, &mut referenced_tables);
                }
            }
        }

        // CRITICAL FIX: The ONLY tables that should start as "available" are those that:
        // 1. Are referenced in JOIN conditions (needed by some JOIN)
        // 2. Are NOT themselves being joined (they go in FROM clause)
        // This is the true anchor - the table that other JOINs depend on but doesn't need a JOIN itself
        let mut available_tables: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        
        for table in &referenced_tables {
            if !join_aliases.contains(table) {
                available_tables.insert(table.clone());
                eprintln!(
                    "  ✅ Found TRUE ANCHOR table (referenced but not joined): {}",
                    table
                );
            }
        }

        // Track if we pulled a join out to be the FROM clause (for cyclic patterns)
        let mut from_join_index: Option<usize> = None;
        
        // If no anchor found (all referenced tables are also being joined = cyclic pattern),
        // we need to pick a starting point and move it from JOIN list to FROM clause.
        if available_tables.is_empty() {
            eprintln!("  ⚠️ No natural anchor - picking FROM table from joins...");
            
            // Strategy: Find a join that can start the chain
            // Priority 1: Node tables (short aliases like 'a', 'b') - they're likely to be JOIN targets
            // Priority 2: Any required table
            
            // First, find the best candidate for FROM clause
            for (i, join) in joins.iter().enumerate() {
                // Skip optional tables - they can't be FROM
                if optional_aliases.contains(&join.table_alias) {
                    continue;
                }
                
                // Prefer short aliases (likely node tables like 'a', 'b')
                let is_likely_node_table = !join.table_alias.starts_with("a") 
                    || join.table_alias.len() < 5;
                
                if is_likely_node_table {
                    from_join_index = Some(i);
                    eprintln!("  📌 Moving '{}' to FROM clause (node table)", join.table_alias);
                    break;
                }
            }
            
            // If no node table found, use first required table
            if from_join_index.is_none() {
                for (i, join) in joins.iter().enumerate() {
                    if !optional_aliases.contains(&join.table_alias) {
                        from_join_index = Some(i);
                        eprintln!("  📌 Moving '{}' to FROM clause (first required)", join.table_alias);
                        break;
                    }
                }
            }
        }

        eprintln!(
            "  🔍 Initial available tables (anchors): {:?}",
            available_tables
        );

        let mut ordered_joins = Vec::new();
        let mut remaining_joins = joins;
        
        // Track the FROM clause table separately (for cyclic patterns where we pick from joins)
        let mut from_clause_alias: Option<String> = None;
        
        // If we're pulling a join out for FROM clause, do that first
        if let Some(idx) = from_join_index {
            let from_join = remaining_joins.remove(idx);
            eprintln!("  🏠 Extracted '{}' for FROM clause (will NOT be in JOINs list)", from_join.table_alias);
            from_clause_alias = Some(from_join.table_alias.clone());
            available_tables.insert(from_join.table_alias.clone());
            // DON'T push to ordered_joins - the FROM table should not appear as a JOIN!
            // The anchor return value will specify the FROM table.
        }

        // Keep trying to add joins until we can't make progress
        let mut made_progress = true;
        while made_progress && !remaining_joins.is_empty() {
            made_progress = false;
            let mut i = 0;

            while i < remaining_joins.len() {
                // Check if all tables referenced by this JOIN are available
                let mut referenced_tables = std::collections::HashSet::new();
                let table_alias = remaining_joins[i].table_alias.clone();

                for condition in &remaining_joins[i].joining_on {
                    for operand in &condition.operands {
                        Self::extract_table_refs_from_expr(operand, &mut referenced_tables);
                    }
                }

                // Remove self-reference (the table being joined)
                referenced_tables.remove(&table_alias);

                // Check if all referenced tables are available
                let all_available = referenced_tables
                    .iter()
                    .all(|t| available_tables.contains(t));

                if all_available {
                    eprintln!(
                        "  ? JOIN '{}' can be added (references: {:?})",
                        table_alias, referenced_tables
                    );
                    // This JOIN can be added now
                    let join = remaining_joins.remove(i);
                    available_tables.insert(table_alias.clone());
                    ordered_joins.push(join);
                    made_progress = true;
                    // Don't increment i - we removed an element
                } else {
                    eprintln!(
                        "  ? JOIN '{}' must wait (needs: {:?}, have: {:?})",
                        table_alias, referenced_tables, available_tables
                    );
                    i += 1;
                }
            }
        }

        // If there are still remaining joins, we have a circular dependency or missing anchor
        if !remaining_joins.is_empty() {
            eprintln!(
                "  ??  WARNING: {} JOINs could not be ordered (circular dependency?)",
                remaining_joins.len()
            );
            // Just append them at the end
            ordered_joins.extend(remaining_joins);
        }

        eprintln!(
            "  ? Final JOIN order: {:?}\n",
            ordered_joins
                .iter()
                .map(|j| &j.table_alias)
                .collect::<Vec<_>>()
        );

        // CRITICAL FIX: For cyclic patterns, we extracted a FROM table from the joins list.
        // Use that directly if available. Otherwise, compute the anchor from join conditions.
        let anchor = if let Some(from_alias) = from_clause_alias {
            // We explicitly picked this table for FROM clause
            Some(from_alias)
        } else if let Some(first_join) = ordered_joins.first() {
            // Compute anchor from first join's references
            let mut refs = std::collections::HashSet::new();
            for condition in &first_join.joining_on {
                for operand in &condition.operands {
                    Self::extract_table_refs_from_expr(operand, &mut refs);
                }
            }
            // Remove the table being joined (it shouldn't be the anchor)
            refs.remove(&first_join.table_alias);
            
            // Find a reference that is not being joined anywhere else (this is the anchor)
            refs.into_iter()
                .find(|r| !ordered_joins.iter().any(|j| &j.table_alias == r))
                .or_else(|| available_tables.iter().next().cloned())
        } else {
            None
        };

        eprintln!("  ?? ANCHOR TABLE for FROM clause: {:?}\n", anchor);
        (anchor, ordered_joins)
    }

    /// Extract table aliases referenced in an expression
    fn extract_table_refs_from_expr(
        expr: &LogicalExpr,
        refs: &mut std::collections::HashSet<String>,
    ) {
        match expr {
            LogicalExpr::PropertyAccessExp(prop) => {
                refs.insert(prop.table_alias.0.clone());
            }
            LogicalExpr::Column(_col) => {
                // Columns without table references are ignored
            }
            LogicalExpr::OperatorApplicationExp(op_app) => {
                for operand in &op_app.operands {
                    Self::extract_table_refs_from_expr(operand, refs);
                }
            }
            LogicalExpr::ScalarFnCall(func) => {
                for arg in &func.args {
                    Self::extract_table_refs_from_expr(arg, refs);
                }
            }
            LogicalExpr::AggregateFnCall(func) => {
                for arg in &func.args {
                    Self::extract_table_refs_from_expr(arg, refs);
                }
            }
            // Other expression types don't contain table references
            _ => {}
        }
    }
    
    /// Attach pre_filter predicates to LEFT JOINs for optional aliases.
    /// This extracts predicates from GraphRel.where_predicate that reference ONLY
    /// the optional alias, and moves them into the JOIN's pre_filter field.
    fn attach_pre_filters_to_joins(
        joins: Vec<Join>,
        optional_aliases: &std::collections::HashSet<String>,
        logical_plan: &Arc<LogicalPlan>,
    ) -> Vec<Join> {
        use crate::query_planner::logical_expr::{LogicalExpr, Operator, OperatorApplication as LogicalOpApp};
        
        // First, collect all predicates from GraphRel.where_predicate nodes
        fn collect_graphrel_predicates(plan: &LogicalPlan) -> Vec<(LogicalExpr, String, String, String)> {
            // Returns (predicate, left_connection, alias, right_connection) tuples
            let mut results = Vec::new();
            match plan {
                LogicalPlan::GraphRel(gr) => {
                    if let Some(ref pred) = gr.where_predicate {
                        if gr.is_optional.unwrap_or(false) {
                            results.push((
                                pred.clone(),
                                gr.left_connection.clone(),
                                gr.alias.clone(),
                                gr.right_connection.clone(),
                            ));
                        }
                    }
                    results.extend(collect_graphrel_predicates(&gr.left));
                    results.extend(collect_graphrel_predicates(&gr.center));
                    results.extend(collect_graphrel_predicates(&gr.right));
                }
                LogicalPlan::GraphNode(gn) => {
                    results.extend(collect_graphrel_predicates(&gn.input));
                }
                LogicalPlan::Projection(proj) => {
                    results.extend(collect_graphrel_predicates(&proj.input));
                }
                LogicalPlan::Filter(filter) => {
                    results.extend(collect_graphrel_predicates(&filter.input));
                }
                _ => {}
            }
            results
        }
        
        // Helper: check if expression references ONLY a single alias
        fn references_only_alias(expr: &LogicalExpr, alias: &str) -> bool {
            let mut refs = std::collections::HashSet::new();
            GraphJoinInference::extract_table_refs_from_expr(expr, &mut refs);
            refs.len() == 1 && refs.contains(alias)
        }
        
        // Split AND-connected predicates
        fn split_and_predicates(expr: &LogicalExpr) -> Vec<LogicalExpr> {
            match expr {
                LogicalExpr::OperatorApplicationExp(op) if matches!(op.operator, Operator::And) => {
                    let mut result = Vec::new();
                    for operand in &op.operands {
                        result.extend(split_and_predicates(operand));
                    }
                    result
                }
                _ => vec![expr.clone()],
            }
        }
        
        // Combine predicates with AND
        fn combine_with_and(predicates: Vec<LogicalExpr>) -> Option<LogicalExpr> {
            if predicates.is_empty() {
                None
            } else if predicates.len() == 1 {
                Some(predicates.into_iter().next().unwrap())
            } else {
                Some(LogicalExpr::OperatorApplicationExp(LogicalOpApp {
                    operator: Operator::And,
                    operands: predicates,
                }))
            }
        }
        
        // Collect predicates from all optional GraphRels
        let graphrel_preds = collect_graphrel_predicates(logical_plan);
        
        // Build a map of alias -> predicates for optional aliases
        // Only include predicates that reference the optional parts (rel alias or right_connection)
        let mut alias_predicates: std::collections::HashMap<String, Vec<LogicalExpr>> = std::collections::HashMap::new();
        
        for (predicate, _left_conn, rel_alias, right_conn) in graphrel_preds {
            let all_preds = split_and_predicates(&predicate);
            
            for pred in all_preds {
                // Only extract predicates for optional aliases (rel and right, not left which is anchor)
                if references_only_alias(&pred, &rel_alias) && optional_aliases.contains(&rel_alias) {
                    alias_predicates.entry(rel_alias.clone()).or_default().push(pred.clone());
                }
                if references_only_alias(&pred, &right_conn) && optional_aliases.contains(&right_conn) {
                    alias_predicates.entry(right_conn.clone()).or_default().push(pred.clone());
                }
            }
        }
        
        // Now attach predicates to the corresponding LEFT JOINs
        joins.into_iter().map(|mut join| {
            if matches!(join.join_type, crate::query_planner::logical_plan::JoinType::Left) {
                if let Some(preds) = alias_predicates.get(&join.table_alias) {
                    if !preds.is_empty() {
                        let combined = combine_with_and(preds.clone());
                        if combined.is_some() {
                            eprintln!(
                                "DEBUG: Attaching pre_filter to LEFT JOIN on '{}': {:?}",
                                join.table_alias, combined
                            );
                            join.pre_filter = combined;
                        }
                    }
                }
            }
            join
        }).collect()
    }

    fn build_graph_joins(
        logical_plan: Arc<LogicalPlan>,
        collected_graph_joins: &mut Vec<Join>,
        optional_aliases: std::collections::HashSet<String>,
        plan_ctx: &PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        let transformed_plan = match logical_plan.as_ref() {
            // If input is a Union, process each branch INDEPENDENTLY
            // Each branch needs its own collect_graph_joins + build_graph_joins pass
            LogicalPlan::Union(union) => {
                log::info!("🔄 Union detected in build_graph_joins, processing {} branches independently", union.inputs.len());
                let mut any_transformed = false;
                let graph_join_inference = GraphJoinInference::new();
                
                let transformed_branches: Result<Vec<Arc<LogicalPlan>>, _> = union.inputs.iter().map(|branch| {
                    // CRITICAL: Each branch needs fresh state - collect and build separately
                    let mut branch_joins: Vec<Join> = vec![];
                    let mut branch_joined_entities: HashSet<String> = HashSet::new();
                    
                    // Collect joins for this specific branch only
                    graph_join_inference.collect_graph_joins(
                        branch.clone(),
                        branch.clone(),
                        &mut plan_ctx.clone(),  // Clone PlanCtx for each branch
                        graph_schema,
                        &mut branch_joins,
                        &mut branch_joined_entities,
                    )?;
                    
                    eprintln!("🔹 Union branch collected {} joins", branch_joins.len());
                    
                    // Build GraphJoins for this branch with its own collected joins
                    let result = Self::build_graph_joins(
                        branch.clone(),
                        &mut branch_joins,
                        optional_aliases.clone(),
                        plan_ctx,
                        graph_schema,
                    )?;
                    if matches!(result, Transformed::Yes(_)) {
                        any_transformed = true;
                    }
                    Ok(result.get_plan())
                }).collect();
                
                let branches = transformed_branches?;
                if any_transformed {
                    Transformed::Yes(Arc::new(LogicalPlan::Union(crate::query_planner::logical_plan::Union {
                        inputs: branches,
                        union_type: union.union_type.clone(),
                    })))
                } else {
                    Transformed::No(logical_plan.clone())
                }
            }
            LogicalPlan::Projection(_) => {
                // Reorder JOINs before creating GraphJoins to ensure proper dependency order
                let (anchor_table, reordered_joins) = Self::reorder_joins_by_dependencies(
                    collected_graph_joins.clone(),
                    &optional_aliases,
                    plan_ctx,
                );
                
                // Extract predicates for optional aliases and attach them to LEFT JOINs
                let joins_with_pre_filter = Self::attach_pre_filters_to_joins(
                    reordered_joins,
                    &optional_aliases,
                    &logical_plan,
                );

                // wrap the outer projection i.e. first occurance in the tree walk with Graph joins
                Transformed::Yes(Arc::new(LogicalPlan::GraphJoins(GraphJoins {
                    input: logical_plan.clone(),
                    joins: joins_with_pre_filter,
                    optional_aliases,
                    anchor_table,
                })))
            }
            LogicalPlan::GraphNode(graph_node) => {
                let child_tf = Self::build_graph_joins(
                    graph_node.input.clone(),
                    collected_graph_joins,
                    optional_aliases.clone(),
                    plan_ctx,
                    graph_schema,
                )?;
                
                // is_denormalized flag is set by view_optimizer pass - just rebuild
                graph_node.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphRel(graph_rel) => {
                let left_tf = Self::build_graph_joins(
                    graph_rel.left.clone(),
                    collected_graph_joins,
                    optional_aliases.clone(),
                    plan_ctx,
                    graph_schema,
                )?;
                let center_tf = Self::build_graph_joins(
                    graph_rel.center.clone(),
                    collected_graph_joins,
                    optional_aliases.clone(),
                    plan_ctx,
                    graph_schema,
                )?;
                let right_tf = Self::build_graph_joins(
                    graph_rel.right.clone(),
                    collected_graph_joins,
                    optional_aliases.clone(),
                    plan_ctx,
                    graph_schema,
                )?;

                graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone())
            }
            LogicalPlan::Cte(cte) => {
                let child_tf = Self::build_graph_joins(
                    cte.input.clone(),
                    collected_graph_joins,
                    optional_aliases,
                    plan_ctx,
                    graph_schema,
                )?;
                cte.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Scan(_) => Transformed::No(logical_plan.clone()),
            LogicalPlan::Empty => Transformed::No(logical_plan.clone()),
            LogicalPlan::GraphJoins(graph_joins) => {
                let child_tf = Self::build_graph_joins(
                    graph_joins.input.clone(),
                    collected_graph_joins,
                    optional_aliases,
                    plan_ctx,
                    graph_schema,
                )?;
                graph_joins.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Filter(filter) => {
                let child_tf = Self::build_graph_joins(
                    filter.input.clone(),
                    collected_graph_joins,
                    optional_aliases,
                    plan_ctx,
                    graph_schema,
                )?;
                filter.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GroupBy(group_by) => {
                let child_tf = Self::build_graph_joins(
                    group_by.input.clone(),
                    collected_graph_joins,
                    optional_aliases,
                    plan_ctx,
                    graph_schema,
                )?;
                group_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::OrderBy(order_by) => {
                let child_tf = Self::build_graph_joins(
                    order_by.input.clone(),
                    collected_graph_joins,
                    optional_aliases,
                    plan_ctx,
                    graph_schema,
                )?;
                order_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Skip(skip) => {
                let child_tf = Self::build_graph_joins(
                    skip.input.clone(),
                    collected_graph_joins,
                    optional_aliases,
                    plan_ctx,
                    graph_schema,
                )?;
                skip.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Limit(limit) => {
                let child_tf = Self::build_graph_joins(
                    limit.input.clone(),
                    collected_graph_joins,
                    optional_aliases,
                    plan_ctx,
                    graph_schema,
                )?;
                limit.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Union(union) => {
                let mut inputs_tf: Vec<Transformed<Arc<LogicalPlan>>> = vec![];
                for input_plan in union.inputs.iter() {
                    let child_tf = Self::build_graph_joins(
                        input_plan.clone(),
                        collected_graph_joins,
                        optional_aliases.clone(),
                        plan_ctx,
                        graph_schema,
                    )?;
                    inputs_tf.push(child_tf);
                }
                union.rebuild_or_clone(inputs_tf, logical_plan.clone())
            }
            LogicalPlan::PageRank(_) => Transformed::No(logical_plan.clone()),
            LogicalPlan::ViewScan(_) => Transformed::No(logical_plan.clone()),
            LogicalPlan::Unwind(u) => {
                let child_tf = Self::build_graph_joins(
                    u.input.clone(),
                    collected_graph_joins,
                    optional_aliases,
                    plan_ctx,
                    graph_schema,
                )?;
                match child_tf {
                    Transformed::Yes(new_input) => Transformed::Yes(Arc::new(LogicalPlan::Unwind(crate::query_planner::logical_plan::Unwind {
                        input: new_input,
                        expression: u.expression.clone(),
                        alias: u.alias.clone(),
                    }))),
                    Transformed::No(_) => Transformed::No(logical_plan.clone()),
                }
            }
        };
        Ok(transformed_plan)
    }

    fn collect_graph_joins(
        &self,
        logical_plan: Arc<LogicalPlan>,
        root_plan: Arc<LogicalPlan>, // Root plan for reference checking
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
        collected_graph_joins: &mut Vec<Join>,
        joined_entities: &mut HashSet<String>,
    ) -> AnalyzerResult<()> {
        eprintln!("\n+- collect_graph_joins ENTER");
        eprintln!(
            "� Plan variant: {:?}",
            std::mem::discriminant(&*logical_plan)
        );
        eprintln!(
            "� Joins before: {}, Entities: {:?}",
            collected_graph_joins.len(),
            joined_entities
        );

        let result = match logical_plan.as_ref() {
            LogicalPlan::Projection(projection) => {
                eprintln!("� ? Projection, recursing into input");
                self.collect_graph_joins(
                    projection.input.clone(),
                    root_plan.clone(),
                    plan_ctx,
                    graph_schema,
                    collected_graph_joins,
                    joined_entities,
                )
            }
            LogicalPlan::GraphNode(graph_node) => {
                eprintln!("� ? GraphNode({}), recursing into input", graph_node.alias);
                self.collect_graph_joins(
                    graph_node.input.clone(),
                    root_plan.clone(),
                    plan_ctx,
                    graph_schema,
                    collected_graph_joins,
                    joined_entities,
                )
            }
            LogicalPlan::ViewScan(_) => {
                eprintln!("� ? ViewScan, nothing to collect");
                Ok(())
            }
            LogicalPlan::GraphRel(graph_rel) => {
                eprintln!("� --- GraphRel({}) ---", graph_rel.alias);
                eprintln!("�   left_connection: {}", graph_rel.left_connection);
                eprintln!("�   right_connection: {}", graph_rel.right_connection);
                eprintln!(
                    "�   left type: {:?}",
                    std::mem::discriminant(&*graph_rel.left)
                );
                eprintln!(
                    "�   right type: {:?}",
                    std::mem::discriminant(&*graph_rel.right)
                );

                // Process LEFT branch (may contain nested GraphRels)
                eprintln!("�   ? Processing LEFT branch...");
                self.collect_graph_joins(
                    graph_rel.left.clone(),
                    root_plan.clone(),
                    plan_ctx,
                    graph_schema,
                    collected_graph_joins,
                    joined_entities,
                )?;
                eprintln!(
                    "�   ? LEFT done. Joins now: {}",
                    collected_graph_joins.len()
                );

                // Process CURRENT relationship
                eprintln!("�   ? Processing CURRENT relationship...");
                self.infer_graph_join(
                    graph_rel,
                    root_plan.clone(),
                    plan_ctx,
                    graph_schema,
                    collected_graph_joins,
                    joined_entities,
                )?;
                eprintln!(
                    "�   ? CURRENT done. Joins now: {}",
                    collected_graph_joins.len()
                );

                // Process RIGHT branch
                eprintln!("�   ? Processing RIGHT branch...");
                let result = self.collect_graph_joins(
                    graph_rel.right.clone(),
                    root_plan.clone(),
                    plan_ctx,
                    graph_schema,
                    collected_graph_joins,
                    joined_entities,
                );
                eprintln!(
                    "�   ? RIGHT done. Joins now: {}",
                    collected_graph_joins.len()
                );
                result
            }
            LogicalPlan::Cte(cte) => {
                eprintln!("� ? Cte, recursing into input");
                self.collect_graph_joins(
                    cte.input.clone(),
                    root_plan.clone(),
                    plan_ctx,
                    graph_schema,
                    collected_graph_joins,
                    joined_entities,
                )
            }
            LogicalPlan::Scan(_) => {
                eprintln!("� ? Scan, nothing to collect");
                Ok(())
            }
            LogicalPlan::Empty => {
                eprintln!("� ? Empty, nothing to collect");
                Ok(())
            }
            LogicalPlan::GraphJoins(graph_joins) => {
                eprintln!("� ? GraphJoins, recursing into input");
                self.collect_graph_joins(
                    graph_joins.input.clone(),
                    root_plan.clone(),
                    plan_ctx,
                    graph_schema,
                    collected_graph_joins,
                    joined_entities,
                )
            }
            LogicalPlan::Filter(filter) => {
                eprintln!("� ? Filter, recursing into input");
                self.collect_graph_joins(
                    filter.input.clone(),
                    root_plan.clone(),
                    plan_ctx,
                    graph_schema,
                    collected_graph_joins,
                    joined_entities,
                )
            }
            LogicalPlan::GroupBy(group_by) => {
                eprintln!("� ? GroupBy, recursing into input");
                self.collect_graph_joins(
                    group_by.input.clone(),
                    root_plan.clone(),
                    plan_ctx,
                    graph_schema,
                    collected_graph_joins,
                    joined_entities,
                )
            }
            LogicalPlan::OrderBy(order_by) => {
                eprintln!("� ? OrderBy, recursing into input");
                self.collect_graph_joins(
                    order_by.input.clone(),
                    root_plan.clone(),
                    plan_ctx,
                    graph_schema,
                    collected_graph_joins,
                    joined_entities,
                )
            }
            LogicalPlan::Skip(skip) => {
                eprintln!("� ? Skip, recursing into input");
                self.collect_graph_joins(
                    skip.input.clone(),
                    root_plan.clone(),
                    plan_ctx,
                    graph_schema,
                    collected_graph_joins,
                    joined_entities,
                )
            }
            LogicalPlan::Limit(limit) => {
                eprintln!("� ? Limit, recursing into input");
                self.collect_graph_joins(
                    limit.input.clone(),
                    root_plan.clone(),
                    plan_ctx,
                    graph_schema,
                    collected_graph_joins,
                    joined_entities,
                )
            }
            LogicalPlan::Union(union) => {
                // CRITICAL: Don't recurse into UNION branches here!
                // Each branch will be processed independently by build_graph_joins,
                // which properly clones the state for each branch.
                // If we recurse here with shared state, branches pollute each other.
                eprintln!("🔀 Union detected in collect_graph_joins - skipping recursion (handled by build_graph_joins)");
                Ok(())
            }
            LogicalPlan::PageRank(_) => {
                eprintln!("� ? PageRank, nothing to collect");
                Ok(())
            }
            LogicalPlan::Unwind(u) => {
                eprintln!("� ? Unwind, recursing into input");
                self.collect_graph_joins(
                    u.input.clone(),
                    root_plan.clone(),
                    plan_ctx,
                    graph_schema,
                    collected_graph_joins,
                    joined_entities,
                )
            }
        };

        eprintln!("+- collect_graph_joins EXIT");
        eprintln!(
            "   Joins after: {}, Entities: {:?}\n",
            collected_graph_joins.len(),
            joined_entities
        );

        result
    }

    fn infer_graph_join(
        &self,
        graph_rel: &GraphRel,
        root_plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
        collected_graph_joins: &mut Vec<Join>,
        joined_entities: &mut HashSet<String>,
    ) -> AnalyzerResult<()> {
        eprintln!(
            "    +- infer_graph_join ENTER for GraphRel({})",
            graph_rel.alias
        );
        eprintln!(
            "    � left_connection: {}, right_connection: {}",
            graph_rel.left_connection, graph_rel.right_connection
        );
        eprintln!("    � joined_entities before: {:?}", joined_entities);

        // Skip join inference for TRULY variable-length paths (need recursive CTEs)
        // But DO process fixed-length patterns (*1, *2, *3) - they use inline JOINs
        if let Some(spec) = &graph_rel.variable_length {
            let is_fixed_length = spec.exact_hop_count().is_some() 
                && graph_rel.shortest_path_mode.is_none();
            
            if !is_fixed_length {
                // Truly variable-length (*1..3, *, etc.) - skip, will use CTE path
                eprintln!("    � ? SKIP: Variable-length path detected (not fixed-length)");
                eprintln!("    +- infer_graph_join EXIT\n");
                return Ok(());
            }
            // Fixed-length (*1, *2, *3) - continue to generate JOINs
            eprintln!("    � Fixed-length pattern (*{}) detected - will generate inline JOINs", 
                spec.exact_hop_count().unwrap());
        }

        // Check if nodes have labels - skip for anonymous nodes like ()-[r]->()
        let left_alias = &graph_rel.left_connection;
        let right_alias = &graph_rel.right_connection;

        let left_ctx_opt = plan_ctx.get_table_ctx_from_alias_opt(&Some(left_alias.clone()));
        let right_ctx_opt = plan_ctx.get_table_ctx_from_alias_opt(&Some(right_alias.clone()));

        // FIX: Don't skip anonymous nodes - they still need JOINs created
        // because relationship JOIN conditions reference their aliases
        // Old logic: Skip if either node is anonymous (no context or no label)
        // New logic: Only skip if nodes truly don't exist in plan_ctx
        if left_ctx_opt.is_err() || right_ctx_opt.is_err() {
            eprintln!("    � ? SKIP: Node context missing entirely");
            eprintln!("    +- infer_graph_join EXIT\n");
            return Ok(());
        }

        // Check for $any nodes - only skip if LEFT is $any (nothing to join FROM)
        // If RIGHT is $any, we still need to:
        // 1. Join the relationship CTE to the left node
        // 2. Just skip creating a join for the $any target node table itself
        let left_is_polymorphic_any = if let Ok(left_ctx) = &left_ctx_opt {
            if let Ok(left_label) = left_ctx.get_label_str() {
                left_label == "$any"
            } else {
                false
            }
        } else {
            false
        };
        
        let right_is_polymorphic_any = if let Ok(right_ctx) = &right_ctx_opt {
            if let Ok(right_label) = right_ctx.get_label_str() {
                eprintln!("    🔍 DEBUG: right_label = '{}'", right_label);
                right_label == "$any"
            } else {
                eprintln!("    🔍 DEBUG: right_ctx.get_label_str() failed");
                false
            }
        } else {
            eprintln!("    🔍 DEBUG: right_ctx_opt is Err");
            false
        };
        
        eprintln!("    🔍 DEBUG: right_is_polymorphic_any = {}", right_is_polymorphic_any);
        
        if left_is_polymorphic_any {
            eprintln!("    🚫 SKIP: Polymorphic $any left node - nothing to join from");
            eprintln!("    +- infer_graph_join EXIT\n");
            return Ok(());
        }
        
        // For polymorphic right nodes ($any), skip relationship join creation entirely
        // The CTE will handle the relationship join in plan_builder.rs
        // When right node is $any, we know this is a polymorphic/wildcard edge
        // because $any is only set for edges that:
        // 1. Have no explicit target type (wildcard like [r]->)
        // 2. Use polymorphic edge table with $any in schema
        if right_is_polymorphic_any {
            eprintln!("    🎯 SKIP: Polymorphic $any right node - CTE will handle relationship join");
            eprintln!("    +- infer_graph_join EXIT\n");
            // Mark the relationship as "joined" to avoid issues in subsequent processing
            joined_entities.insert(graph_rel.alias.clone());
            return Ok(());
        }

        // FIX: Don't check for labels - anonymous nodes don't have labels but still need JOINs
        // let left_has_label = left_ctx_opt.as_ref().unwrap().get_label_opt().is_some();
        // let right_has_label = right_ctx_opt.as_ref().unwrap().get_label_opt().is_some();
        // if !left_has_label || !right_has_label {
        //     eprintln!("    � ? SKIP: Anonymous node (no label)");
        //     eprintln!("    +- infer_graph_join EXIT\n");
        //     return Ok(());
        // }

        // FIX: Keep table checks for debugging but don't skip on them
        let _left_has_table = match graph_rel.left.as_ref() {
            LogicalPlan::GraphNode(gn) => match gn.input.as_ref() {
                LogicalPlan::Scan(scan) => scan.table_name.is_some(),
                LogicalPlan::ViewScan(_) => true,
                _ => true,
            },
            _ => true,
        };

        let _right_has_table = match graph_rel.right.as_ref() {
            LogicalPlan::GraphNode(gn) => match gn.input.as_ref() {
                LogicalPlan::Scan(scan) => scan.table_name.is_some(),
                LogicalPlan::ViewScan(_) => true,
                _ => true,
            },
            _ => true,
        };

        // FIX: Don't skip anonymous nodes - they need table/ViewScan for JOIN generation
        // Anonymous nodes like `()` in `()-[r:FOLLOWS]->()` will have:
        // - Generated aliases (ab19d09e4b)
        // - ViewScans created from schema
        // - No explicit table_name but ViewScan provides it
        // Old logic: Skip if BOTH nodes have no table names
        // New logic: Always proceed - ViewScan will provide table info
        // if (!left_has_table && !right_has_table) {
        //     return Ok(());
        // }

        // Clone the optional_aliases set before calling get_graph_context
        // to avoid borrow checker issues
        let optional_aliases = plan_ctx.get_optional_aliases().clone();

        // Check if nodes are actually referenced in the query BEFORE calling get_graph_context
        // to avoid borrow checker issues (get_graph_context takes &mut plan_ctx)
        eprintln!(
            "    � Checking if LEFT '{}' is referenced...",
            graph_rel.left_connection
        );
        let left_is_referenced =
            Self::is_node_referenced(&graph_rel.left_connection, plan_ctx, &root_plan);
        eprintln!(
            "    � LEFT '{}' referenced: {}",
            graph_rel.left_connection, left_is_referenced
        );

        eprintln!(
            "    � Checking if RIGHT '{}' is referenced...",
            graph_rel.right_connection
        );
        let right_is_referenced =
            Self::is_node_referenced(&graph_rel.right_connection, plan_ctx, &root_plan);
        eprintln!(
            "    � RIGHT '{}' referenced: {}",
            graph_rel.right_connection, right_is_referenced
        );

        // Extract all necessary data from graph_context BEFORE passing plan_ctx mutably
        let (
            left_alias_str,
            rel_alias_str,
            right_alias_str,
            left_node_id_column,
            right_node_id_column,
            left_label,
            right_label,
            rel_labels,
            left_node_schema,
            right_node_schema,
            rel_schema,
            left_alias,
            rel_alias,
            right_alias,
            left_cte_name,
            rel_cte_name,
            right_cte_name,
        ) = {
            let graph_context = graph_context::get_graph_context(
                graph_rel,
                plan_ctx,
                graph_schema,
                Pass::GraphJoinInference,
            )?;
            
            (
                graph_context.left.alias.to_string(),
                graph_context.rel.alias.to_string(),
                graph_context.right.alias.to_string(),
                graph_context.left.schema.node_id.column.clone(),
                graph_context.right.schema.node_id.column.clone(),
                graph_context.left.label.clone(),
                graph_context.right.label.clone(),
                // Get all labels from table_ctx for polymorphic IN clause support
                graph_context.rel.table_ctx.get_labels()
                    .cloned()
                    .unwrap_or_else(|| vec![graph_context.rel.label.clone()]),
                graph_context.left.schema.clone(),
                graph_context.right.schema.clone(),
                graph_context.rel.schema.clone(),
                graph_context.left.alias.clone(),
                graph_context.rel.alias.clone(),
                graph_context.right.alias.clone(),
                graph_context.left.cte_name.clone(),
                graph_context.rel.cte_name.clone(),
                graph_context.right.cte_name.clone(),
            )
            // graph_context drops here, releasing the borrow of plan_ctx
        };

        // Check which aliases are optional
        // Check BOTH plan_ctx (for pre-marked optionals) AND graph_rel.is_optional (for marked patterns)
        let left_is_optional = optional_aliases.contains(&left_alias_str);
        let rel_is_optional =
            optional_aliases.contains(&rel_alias_str) || graph_rel.is_optional.unwrap_or(false);
        let right_is_optional = optional_aliases.contains(&right_alias_str);

        eprintln!(
            "    � OPTIONAL CHECK: left='{}' optional={}, rel='{}' optional={} (graph_rel.is_optional={:?}), right='{}' optional={}",
            left_alias_str,
            left_is_optional,
            rel_alias_str,
            rel_is_optional,
            graph_rel.is_optional,
            right_alias_str,
            right_is_optional
        );
        eprintln!("    � optional_aliases set: {:?}", optional_aliases);

        // Check for standalone relationship join.
        // e.g. MATCH (a)-[f1:Follows]->(b)-[f2:Follows]->(c), (a)-[f3:Follows]->(c)
        // In the duplicate scan removing pass, we remove the already scanned nodes. We do this from bottom to up.
        // So there could be a graph_rel who has LogicalPlan::Empty as left. In such case just join the relationship but on both nodes columns.
        // In case of f3, both of its nodes a and b are already joined. So just join f3 on both a and b's joining keys.
        let is_standalone_rel: bool = matches!(graph_rel.left.as_ref(), LogicalPlan::Empty);

        eprintln!("    � Creating joins for relationship...");
        let joins_before = collected_graph_joins.len();

        // ClickGraph uses view-mapped graph storage where relationships are tables
        // with from_id/to_id columns. Process the graph pattern to generate JOINs.
        eprintln!("    � ? Processing graph pattern");
        let result = self.handle_graph_pattern(
            graph_rel,
            &left_alias,
            &rel_alias,
            &right_alias,
            &left_cte_name,
            &rel_cte_name,
            &right_cte_name,
            &left_node_schema,
            &rel_schema,
            &right_node_schema,
            left_node_id_column,
            right_node_id_column,
            is_standalone_rel,
            left_is_optional,
            rel_is_optional,
            right_is_optional,
            left_is_referenced,
            right_is_referenced,
            left_label,
            right_label,
            rel_labels,
            plan_ctx,
            graph_schema,
            collected_graph_joins,
            joined_entities,
        );

        let joins_added = collected_graph_joins.len() - joins_before;
        eprintln!("    � ? Added {} joins", joins_added);
        eprintln!("    � joined_entities after: {:?}", joined_entities);
        eprintln!("    +- infer_graph_join EXIT\n");

        result
    }

    /// Handle graph pattern traversal for view-mapped tables
    ///
    /// ClickGraph always uses view-mapped edge list storage where relationships are stored
    /// as tables with from_id/to_id columns connecting to node tables.
    /// The function name reflects that we traverse graph patterns, not the storage format.
    #[allow(clippy::too_many_arguments)]
    fn handle_graph_pattern(
        &self,
        graph_rel: &GraphRel,
        left_alias: &String,
        rel_alias: &String,
        right_alias: &String,
        left_cte_name: &String,
        rel_cte_name: &String,
        right_cte_name: &String,
        left_node_schema: &NodeSchema,
        rel_schema: &RelationshipSchema,
        right_node_schema: &NodeSchema,
        left_node_id_column: String,
        right_node_id_column: String,
        is_standalone_rel: bool,
        left_is_optional: bool,
        rel_is_optional: bool,
        right_is_optional: bool,
        left_is_referenced: bool,
        right_is_referenced: bool,
        left_label: String,
        right_label: String,
        rel_types: Vec<String>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
        collected_graph_joins: &mut Vec<Join>,
        joined_entities: &mut HashSet<String>,
    ) -> AnalyzerResult<()> {
        // Aliases and CTE names are now passed as parameters
        
        // For coupled edge checking and other single-type operations, use the first type
        // For polymorphic edge filters, we pass all types to generate IN clause if needed
        let rel_type = rel_types.first().map(|s| s.as_str()).unwrap_or("");
        
        // Extract relationship column names from the ViewScan
        let rel_cols = extract_relationship_columns(&graph_rel.center).unwrap_or(
            crate::render_plan::cte_extraction::RelationshipColumns {
                from_id: "from_node_id".to_string(),
                to_id: "to_node_id".to_string(),
            },
        );
        
        // For Direction::Incoming (from BidirectionalUnion), swap the columns
        // so that the "from" side of the relationship connects to the "to" node
        let (rel_from_col, rel_to_col) = if graph_rel.direction == Direction::Incoming {
            (rel_cols.to_id, rel_cols.from_id)  // Swapped for incoming direction
        } else {
            (rel_cols.from_id, rel_cols.to_id)  // Normal for outgoing/either
        };

        eprintln!(
            "    🔹 DEBUG REL COLUMNS: direction={:?}, rel_from_col = '{}', rel_to_col = '{}'",
            graph_rel.direction, rel_from_col, rel_to_col
        );

        // If both nodes are of the same type then check the direction to determine where are the left and right nodes present in the edgelist.
        if left_node_schema.table_name == right_node_schema.table_name {
            eprintln!(
                "    SAME-TYPE NODES PATH (left={}, right={})",
                left_node_schema.table_name, right_node_schema.table_name
            );
            
            // Check for undirected pattern (Direction::Either) - needs bidirectional join
            let is_bidirectional = graph_rel.direction == Direction::Either;
            eprintln!("    Direction: {:?}, is_bidirectional: {}", graph_rel.direction, is_bidirectional);
            
            if joined_entities.contains(right_alias) {
                eprintln!("    Branch: RIGHT already joined");
                // join the rel with right first and then join the left with rel
                let rel_conn_with_right_node = rel_to_col.clone();
                let left_conn_with_rel = rel_from_col.clone();
                let polymorphic_filter = generate_polymorphic_edge_filter(
                    rel_alias,
                    &rel_types,
                    rel_schema,
                    &left_label,
                    &right_label,
                );
                
                // For bidirectional/undirected patterns, use OR condition for both directions
                let joining_on = if is_bidirectional {
                    // For undirected: rel connects to anchor (right) in either direction
                    vec![generate_rel_to_anchor_bidirectional(
                        rel_alias,
                        &rel_from_col,
                        &rel_to_col,
                        right_alias,
                        &right_node_id_column,
                    )]
                } else {
                    // Standard single-direction join
                    vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: crate::graph_catalog::expression_parser::PropertyValue::Column(rel_conn_with_right_node),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(right_alias.to_string()),
                                column: crate::graph_catalog::expression_parser::PropertyValue::Column(right_node_id_column.clone()),
                            }),
                        ],
                    }]
                };
                
                let mut rel_graph_join = Join {
                    table_name: rel_cte_name.clone(),
                    table_alias: rel_alias.to_string(),
                    joining_on,
                    join_type: Self::determine_join_type(rel_is_optional),
                    pre_filter: polymorphic_filter,
                };

                // Node join not needed for edge list with same-type nodes
                // let left_graph_join = Join {
                //     table_name: left_cte_name,
                //     table_alias: left_alias.to_string(),
                //     joining_on: vec![OperatorApplication {
                //         operator: Operator::Equal,
                //         operands: vec![
                //             LogicalExpr::PropertyAccessExp(PropertyAccess {
                //                 table_alias: TableAlias(left_alias.to_string()),
                //                 column: crate::graph_catalog::expression_parser::PropertyValue::Column(left_node_id_column.clone()),
                //             }),
                //             LogicalExpr::PropertyAccessExp(PropertyAccess {
                //                 table_alias: TableAlias(rel_alias.to_string()),
                //                 column: crate::graph_catalog::expression_parser::PropertyValue::Column(left_conn_with_rel.clone()),
                //             }),
                //         ],
                //     }],
                //     join_type: Self::determine_join_type(left_is_optional),
                // };

                if is_standalone_rel {
                    let rel_to_right_graph_join_keys = OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: crate::graph_catalog::expression_parser::PropertyValue::Column(left_conn_with_rel),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(left_alias.to_string()),
                                column: crate::graph_catalog::expression_parser::PropertyValue::Column(left_node_id_column),
                            }),
                        ],
                    };
                    rel_graph_join.joining_on.push(rel_to_right_graph_join_keys);

                    collected_graph_joins.push(rel_graph_join);
                    joined_entities.insert(rel_alias.to_string());
                    // in this case we will only join relation so early return without pushing the other joins
                    return Ok(());
                }

                // push the relation first
                collected_graph_joins.push(rel_graph_join);
                joined_entities.insert(rel_alias.to_string());

                // MULTI-HOP FIX: Always join LEFT node for same-type patterns
                // The relationship JOIN references LEFT, so it must be in the FROM/JOIN chain
                
                // DENORMALIZED EDGE CHECK: Register alias if node is on edge table
                let left_is_denormalized = is_node_denormalized_on_edge(
                    &left_node_schema,
                    &rel_schema,
                    true  // is_from_node = true
                );
                
                if left_is_denormalized {
                    // Register denormalized alias so renderer can resolve properties correctly
                    plan_ctx.register_denormalized_alias(
                        left_alias.to_string(),
                        rel_alias.to_string(),
                        true,  // is_from_node
                        left_label.clone(),
                        rel_type.to_string(),
                    );
                    eprintln!(
                        "    DENORMALIZED: Registered LEFT alias '{}' → rel '{}' (from_node)",
                        left_alias, rel_alias
                    );
                    // DON'T add to joined_entities - denormalized nodes don't exist as physical tables
                    // The relationship table will be the physical table
                } else {
                    // Traditional: create JOIN
                    let left_graph_join = Join {
                        table_name: left_cte_name.clone(),
                        table_alias: left_alias.to_string(),
                        joining_on: vec![OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                LogicalExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(left_alias.to_string()),
                                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(left_node_id_column.clone()),
                                }),
                                LogicalExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(rel_alias.to_string()),
                                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(left_conn_with_rel.clone()),
                                }),
                            ],
                        }],
                        join_type: Self::determine_join_type(left_is_optional),
                    pre_filter: None,
                    };
                    collected_graph_joins.push(left_graph_join);
                    eprintln!("    TRADITIONAL: Created JOIN for LEFT alias '{}'", left_alias);
                }
                joined_entities.insert(left_alias.to_string());

                // Right is already joined (see condition above)
                // No need to insert again
                Ok(())
            } else {
                eprintln!("    � ?? Branch: LEFT already joined (or start of join)");
                eprintln!("    � ?? left_alias: {}", left_alias);
                eprintln!("    � ?? left_node_id_column: {:?}", left_node_id_column);
                eprintln!("    � ?? rel_alias: {}", rel_alias);
                eprintln!(
                    "    � ?? LEFT in joined_entities: {}",
                    joined_entities.contains(left_alias)
                );

                // CRITICAL FIX: Check if LEFT is ACTUALLY joined yet
                // If LEFT is not joined, we must connect the relationship to RIGHT (the anchor) instead!
                let left_is_joined = joined_entities.contains(left_alias);
                let right_is_joined = joined_entities.contains(right_alias);

                // Check if LEFT or RIGHT is the anchor (first relationship AND required)
                let is_first_relationship = collected_graph_joins.is_empty();
                let left_is_anchor = is_first_relationship && !left_is_optional;
                let right_is_anchor = is_first_relationship && !right_is_optional;

                let rel_conn_with_left_node = rel_from_col.clone();
                let right_conn_with_rel = rel_to_col.clone();

                // Choose which node to connect the relationship to (priority order)
                let (rel_connect_column, node_alias, node_id_column) = if left_is_joined {
                    eprintln!("    � LEFT joined - connecting to LEFT");
                    (
                        rel_conn_with_left_node.clone(),
                        left_alias.to_string(),
                        left_node_id_column.clone(),
                    )
                } else if right_is_joined {
                    eprintln!("    � RIGHT joined - connecting to RIGHT");
                    (
                        right_conn_with_rel.clone(),
                        right_alias.to_string(),
                        right_node_id_column.clone(),
                    )
                } else if left_is_anchor {
                    eprintln!("    � LEFT is ANCHOR - connecting to LEFT");
                    (
                        rel_conn_with_left_node.clone(),
                        left_alias.to_string(),
                        left_node_id_column.clone(),
                    )
                } else if right_is_anchor {
                    eprintln!("    � RIGHT is ANCHOR - connecting to RIGHT");
                    (
                        right_conn_with_rel.clone(),
                        right_alias.to_string(),
                        right_node_id_column.clone(),
                    )
                } else {
                    eprintln!("    🔹 FALLBACK - connecting to LEFT");
                    (
                        rel_conn_with_left_node.clone(),
                        left_alias.to_string(),
                        left_node_id_column.clone(),
                    )
                };

                let polymorphic_filter = generate_polymorphic_edge_filter(
                    rel_alias,
                    &rel_types,
                    rel_schema,
                    &left_label,
                    &right_label,
                );
                let mut rel_graph_join = Join {
                    table_name: rel_cte_name.clone(),
                    table_alias: rel_alias.to_string(),
                    joining_on: if is_bidirectional {
                        // For undirected: rel connects to anchor (left) in either direction
                        vec![generate_rel_to_anchor_bidirectional(
                            rel_alias,
                            &rel_from_col,
                            &rel_to_col,
                            left_alias,
                            &left_node_id_column,
                        )]
                    } else {
                        vec![OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                LogicalExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(rel_alias.to_string()),
                                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(rel_connect_column),
                                }),
                                LogicalExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(node_alias),
                                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(node_id_column),
                                }),
                            ],
                        }]
                    },
                    join_type: Self::determine_join_type(rel_is_optional),
                    pre_filter: polymorphic_filter,
                };

                eprintln!(
                    "    � ?? rel_graph_join.joining_on.len() after creation: {}",
                    rel_graph_join.joining_on.len()
                );
                eprintln!("    � ?? is_standalone_rel: {}", is_standalone_rel);

                // Node join not needed for edge list with same-type nodes
                // let right_graph_join = Join {
                //     table_name: right_cte_name,
                //     table_alias: right_alias.to_string(),
                //     joining_on: vec![OperatorApplication {
                //         operator: Operator::Equal,
                //         operands: vec![
                //             LogicalExpr::PropertyAccessExp(PropertyAccess {
                //                 table_alias: TableAlias(right_alias.to_string()),
                //                 column: crate::graph_catalog::expression_parser::PropertyValue::Column(right_node_id_column.clone()),
                //             }),
                //             LogicalExpr::PropertyAccessExp(PropertyAccess {
                //                 table_alias: TableAlias(rel_alias.to_string()),
                //                 column: crate::graph_catalog::expression_parser::PropertyValue::Column(right_conn_with_rel.clone()),
                //             }),
                //         ],
                //     }],
                //     join_type: Self::determine_join_type(right_is_optional),
                // };

                if is_standalone_rel {
                    let rel_to_right_graph_join_keys = OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: crate::graph_catalog::expression_parser::PropertyValue::Column(right_conn_with_rel),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(right_alias.to_string()),
                                column: crate::graph_catalog::expression_parser::PropertyValue::Column(right_node_id_column),
                            }),
                        ],
                    };
                    rel_graph_join.joining_on.push(rel_to_right_graph_join_keys);

                    collected_graph_joins.push(rel_graph_join);
                    joined_entities.insert(rel_alias.to_string());
                    // in this case we will only join relation so early return without pushing the other joins
                    return Ok(());
                }

                // For edge list with same-type nodes: only join the right node if it's referenced
                eprintln!(
                    "    � ?? RIGHT BEFORE PUSH: rel_graph_join.table_alias = {}",
                    rel_graph_join.table_alias
                );
                eprintln!(
                    "    � ?? RIGHT BEFORE PUSH: rel_graph_join.joining_on.len() = {}",
                    rel_graph_join.joining_on.len()
                );
                for (i, cond) in rel_graph_join.joining_on.iter().enumerate() {
                    eprintln!("    � ??   [{}]: {:?}", i, cond);
                }

                // Check if this is the first relationship (before pushing the rel)
                let is_first_relationship = collected_graph_joins.is_empty();

                // JOIN ORDER FIX: The rel_graph_join created above references LEFT node in its condition.
                // If LEFT is not yet joined, we MUST join LEFT before the relationship!
                // This happens when:
                // 1. This is the first relationship (joined_entities is empty)
                // 2. LEFT was not the anchor node (not in FROM clause)
                // Solution: Check if LEFT is in joined_entities. If NOT, join LEFT first, then rel.
                eprintln!(
                    "    � ?? DEBUG: left_is_optional={}, !joined_entities.contains(left_alias)={}, left_is_referenced={}",
                    left_is_optional,
                    !joined_entities.contains(left_alias),
                    left_is_referenced
                );
                eprintln!(
                    "    � ?? DEBUG: joined_entities={:?}, left_alias={}",
                    joined_entities, left_alias
                );

                // FIX: Always join LEFT if rel references it (even for anonymous nodes)
                // The relationship JOIN condition references left_alias, so it MUST be in scope
                // BUT: If LEFT is the anchor (required, first relationship), it should go to FROM, not JOIN!
                let left_is_anchor = is_first_relationship && !left_is_optional;
                let reverse_join_order = !joined_entities.contains(left_alias) && !left_is_anchor;
                eprintln!("    🔹 ?? DEBUG: reverse_join_order={}, left_is_anchor={}", reverse_join_order, left_is_anchor);
                eprintln!("    🔹 ?? FIX: Joining LEFT regardless of is_referenced for JOIN scope");

                // OPTIONAL MATCH FIX: When left is the anchor, just mark it as joined (for FROM clause)
                // without creating an actual JOIN entry - the anchor goes in FROM, not JOIN!
                if left_is_anchor && !joined_entities.contains(left_alias) {
                    eprintln!(
                        "    🔹 ?? LEFT ANCHOR: Marking '{}' as joined (will be FROM table, not JOIN)",
                        left_alias
                    );
                    joined_entities.insert(left_alias.to_string());
                }

                if reverse_join_order {
                    eprintln!(
                        "    🔹 ?? REVERSING JOIN ORDER: Joining LEFT node '{}' BEFORE relationship",
                        left_alias
                    );
                    
                    // DENORMALIZED EDGE CHECK: Register alias if node is on edge table
                    let left_is_denormalized = is_node_denormalized_on_edge(
                        &left_node_schema,
                        &rel_schema,
                        true  // is_from_node = true (LEFT connects to rel's from_node)
                    );
                    
                    if left_is_denormalized {
                        // Register denormalized alias so renderer can resolve properties correctly
                        plan_ctx.register_denormalized_alias(
                            left_alias.to_string(),
                            rel_alias.to_string(),
                            true,  // is_from_node
                            left_label.clone(),
                            rel_type.to_string(),
                        );
                        eprintln!(
                            "    DENORMALIZED: Registered LEFT alias '{}' → rel '{}' (from_node)",
                            left_alias, rel_alias
                        );
                        // DON'T mark as joined - denormalized nodes are virtual, not physical tables
                    } else {
                        // Traditional: Join LEFT node first
                        eprintln!("    🔹 CREATING LEFT JOIN: u1 ON r.{}", rel_from_col);
                        let left_graph_join = Join {
                            table_name: left_cte_name.clone(),
                            table_alias: left_alias.to_string(),
                            joining_on: vec![OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    LogicalExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(left_alias.to_string()),
                                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(left_node_id_column.clone()),
                                    }),
                                    LogicalExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(rel_alias.to_string()),
                                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(rel_from_col.clone()),
                                    }),
                                ],
                            }],
                            join_type: Self::determine_join_type(left_is_optional),
                    pre_filter: None,
                        };
                        collected_graph_joins.push(left_graph_join);
                        joined_entities.insert(left_alias.to_string());
                        eprintln!("    ✓ LEFT node '{}' joined first", left_alias);
                    }
                }

                // DENORMALIZED EDGE PATTERN: Check if BOTH nodes are denormalized
                // If so, the relationship table IS the entire pattern (no JOINs needed!)
                let left_is_denormalized = is_node_denormalized_on_edge(
                    &left_node_schema,
                    &rel_schema,
                    true  // is_from_node
                );
                let right_is_denormalized = is_node_denormalized_on_edge(
                    &right_node_schema,
                    &rel_schema,
                    false  // is_from_node
                );
                
                if left_is_denormalized && right_is_denormalized {
                    // FULLY DENORMALIZED EDGE: Both nodes are virtual on this edge table
                    // But we STILL may need a JOIN if the left node was ALREADY on a PREVIOUS edge!
                    
                    // Check if left_alias was already registered on a different edge
                    let prev_edge_info = plan_ctx.get_denormalized_alias_info(left_alias);
                    
                    if let Some((prev_rel_alias, is_from_node, prev_node_label, prev_rel_type)) = prev_edge_info {
                        if prev_rel_alias != *rel_alias {
                            // =========================================================
                            // COUPLED EDGE CHECK
                            // =========================================================
                            // If the previous edge and current edge are COUPLED (same table,
                            // coupling node), they exist in the SAME ROW - NO JOIN needed!
                            
                            eprintln!(
                                "    ?? COUPLED CHECK: prev_rel_type='{}', current_rel_type='{}'",
                                prev_rel_type, rel_type
                            );
                            let edges_are_coupled = graph_schema.are_edges_coupled(&prev_rel_type, rel_type);
                            eprintln!("    ?? COUPLED CHECK RESULT: {}", edges_are_coupled);
                            
                            if edges_are_coupled {
                                eprintln!(
                                    "    ✓ COUPLED EDGES: '{}' and '{}' share same table row via coupling node - NO JOIN needed!",
                                    prev_rel_type, rel_type
                                );
                                // Don't create a JOIN - just register this edge and continue
                                // The previous edge's table scan will provide all columns
                                joined_entities.insert(rel_alias.to_string());
                                
                                // CRITICAL: For coupled edges, the right_alias (rip) should point to 
                                // the PREVIOUS edge's alias, since they're the same row!
                                // This ensures all property references resolve to the edge in FROM clause.
                                plan_ctx.register_denormalized_alias(
                                    right_alias.to_string(),
                                    prev_rel_alias.clone(),  // Use PREVIOUS edge alias, not current!
                                    false, // right is TO node
                                    right_label.clone(),
                                    rel_type.to_string(),  // But keep current rel_type for property mapping
                                );
                                
                                // Coupled edges - no JOIN needed, return early
                                return Ok(());
                            }
                            
                            // MULTI-HOP DENORMALIZED: left node is on a DIFFERENT previous edge
                            // AND edges are NOT coupled - we need to JOIN this edge to the previous edge
                            // Join condition: current_edge.from_id = prev_edge.(from_id or to_id depending on role)
                            eprintln!(
                                "    🔗 MULTI-HOP DENORMALIZED: '{}' already on edge '{}', now on '{}' - creating edge-to-edge JOIN",
                                left_alias, prev_rel_alias, rel_alias
                            );
                            
                            // Get the previous edge's relationship type from plan_ctx
                            let prev_edge_type = plan_ctx.get_rel_table_ctx(&prev_rel_alias)
                                .ok()
                                .and_then(|ctx| ctx.get_labels().cloned())
                                .and_then(|labels| labels.first().cloned());
                            
                            // Look up the previous edge's schema to get its from_id/to_id
                            let prev_edge_col = if let Some(ref prev_type) = prev_edge_type {
                                if let Ok(prev_rel_schema_found) = graph_schema.get_rel_schema(prev_type) {
                                    if is_from_node {
                                        eprintln!("    ?? MULTI-HOP: node was FROM on prev edge, using prev edge's from_id: {}", prev_rel_schema_found.from_id);
                                        prev_rel_schema_found.from_id.clone()
                                    } else {
                                        eprintln!("    ?? MULTI-HOP: node was TO on prev edge, using prev edge's to_id: {}", prev_rel_schema_found.to_id);
                                        prev_rel_schema_found.to_id.clone()
                                    }
                                } else {
                                    // Fallback: use current edge's columns if prev schema not found
                                    eprintln!("    ?? MULTI-HOP: Could not find prev edge schema, using fallback");
                                    if is_from_node { rel_from_col.clone() } else { rel_to_col.clone() }
                                }
                            } else {
                                // Fallback: use current edge's columns if prev type not found
                                eprintln!("    ?? MULTI-HOP: Could not find prev edge type, using fallback");
                                if is_from_node { rel_from_col.clone() } else { rel_to_col.clone() }
                            };
                            
                            // This edge's column is from_id (left node connects to from_id)
                            let current_edge_col = rel_from_col.clone();
                            
                            eprintln!("    🔹 MULTI-HOP JOIN: {}.{} = {}.{}", rel_alias, current_edge_col, prev_rel_alias, prev_edge_col);
                            
                            let polymorphic_filter = generate_polymorphic_edge_filter(
                                rel_alias,
                                &rel_types,
                                rel_schema,
                                &left_label,
                                &right_label,
                            );
                            let edge_to_edge_join = Join {
                                table_name: rel_cte_name.clone(),
                                table_alias: rel_alias.to_string(),
                                joining_on: vec![OperatorApplication {
                                    operator: Operator::Equal,
                                    operands: vec![
                                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: TableAlias(rel_alias.to_string()),
                                            column: crate::graph_catalog::expression_parser::PropertyValue::Column(current_edge_col),
                                        }),
                                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: TableAlias(prev_rel_alias.clone()),
                                            column: crate::graph_catalog::expression_parser::PropertyValue::Column(prev_edge_col),
                                        }),
                                    ],
                                }],
                                join_type: JoinType::Inner,
                    pre_filter: polymorphic_filter,
                            };
                            collected_graph_joins.push(edge_to_edge_join);
                            joined_entities.insert(rel_alias.to_string());
                        } else {
                            // Same edge - no additional JOIN needed
                            eprintln!(
                                "    ✓ FULLY DENORMALIZED: Both '{}' and '{}' are on edge '{}' - NO JOINs needed!",
                                left_alias, right_alias, rel_alias
                            );
                            joined_entities.insert(rel_alias.to_string());
                        }
                    } else {
                        // First denormalized edge - this becomes the FROM anchor
                        eprintln!(
                            "    ✓ FULLY DENORMALIZED: Both '{}' and '{}' are on edge '{}' - NO JOINs needed!",
                            left_alias, right_alias, rel_alias
                        );
                        joined_entities.insert(rel_alias.to_string());
                    }
                } else {
                    // Traditional or Mixed: Push the relationship JOIN
                    collected_graph_joins.push(rel_graph_join);
                    joined_entities.insert(rel_alias.to_string());
                }

                // Check if left node needs to be joined (if we didn't already do it above)
                if !reverse_join_order {
                    eprintln!(
                        "    � ?? Checking if LEFT node ({}) needs to be joined...",
                        left_alias
                    );
                    eprintln!("    � ?? left_is_referenced: {}", left_is_referenced);
                    eprintln!("    � ?? left_is_optional: {}", left_is_optional);
                    eprintln!(
                        "    � ?? left already in joined_entities: {}",
                        joined_entities.contains(left_alias)
                    );
                    eprintln!("    � ?? is_first_relationship: {}", is_first_relationship);

                    if !joined_entities.contains(left_alias) && left_is_referenced {
                        // Check if this is the anchor node (first relationship AND left is required)
                        let is_anchor = is_first_relationship && !left_is_optional;

                        if is_anchor {
                            // This is the anchor node - it should go in FROM clause, not as a JOIN
                            eprintln!(
                                "    � ?? LEFT node '{}' is the ANCHOR (required + first) - will go in FROM, not JOIN",
                                left_alias
                            );
                            joined_entities.insert(left_alias.to_string());
                        } else {
                            // LEFT is not yet joined but is referenced - create a JOIN for it
                            eprintln!(
                                "    � ? LEFT is referenced but not joined, creating JOIN for '{}'",
                                left_alias
                            );
                            
                            // DENORMALIZED EDGE CHECK: Register alias if node is on edge table
                            let left_is_denormalized = is_node_denormalized_on_edge(
                                &left_node_schema,
                                &rel_schema,
                                true  // is_from_node = true
                            );
                            
                            if left_is_denormalized {
                                plan_ctx.register_denormalized_alias(
                                    left_alias.to_string(),
                                    rel_alias.to_string(),
                                    true,  // is_from_node
                                    left_label.clone(),
                                    rel_type.to_string(),
                                );
                                eprintln!(
                                    "    DENORMALIZED: Registered LEFT alias '{}' → rel '{}' (from_node)",
                                    left_alias, rel_alias
                                );
                                joined_entities.insert(left_alias.to_string());
                            } else {
                                let left_graph_join = Join {
                                    table_name: left_cte_name.clone(),
                                    table_alias: left_alias.to_string(),
                                    joining_on: vec![OperatorApplication {
                                        operator: Operator::Equal,
                                        operands: vec![
                                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                                table_alias: TableAlias(left_alias.to_string()),
                                                column: crate::graph_catalog::expression_parser::PropertyValue::Column(left_node_id_column.clone()),
                                            }),
                                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                                table_alias: TableAlias(rel_alias.to_string()),
                                                column: crate::graph_catalog::expression_parser::PropertyValue::Column(rel_from_col.clone()),
                                            }),
                                        ],
                                    }],
                                    join_type: Self::determine_join_type(left_is_optional),
                    pre_filter: None,
                                };
                                collected_graph_joins.push(left_graph_join);
                                joined_entities.insert(left_alias.to_string());
                            }
                        }
                    } else {
                        // Left is already joined (from FROM clause or previous JOIN)
                        joined_entities.insert(left_alias.to_string());
                    }
                }

                eprintln!(
                    "    � ?? Checking if RIGHT node ({}) should be joined...",
                    right_alias
                );
                eprintln!("    � ?? right_is_referenced: {}", right_is_referenced);
                eprintln!("    � ?? right_is_optional: {}", right_is_optional);

                // MULTI-HOP FIX: Always join RIGHT node for same-type patterns
                // Even if not referenced in SELECT/WHERE, it may be needed for subsequent relationships
                // Check if RIGHT is the anchor node
                let left_is_anchor = is_first_relationship && !left_is_optional;
                let is_anchor = is_first_relationship && !right_is_optional && !left_is_anchor;

                if is_anchor {
                    // This is the anchor node - it should go in FROM clause, not as a JOIN
                    eprintln!(
                        "    � ?? RIGHT node '{}' is the ANCHOR (required + first) - will go in FROM, not JOIN",
                        right_alias
                    );
                    joined_entities.insert(right_alias.to_string());
                } else if right_label == "$any" {
                    // RIGHT is $any (polymorphic wildcard) - skip creating node table JOIN
                    // The relationship CTE join handles the data; target type is in to_label_column
                    eprintln!(
                        "    � ?? RIGHT node '{}' is $any (polymorphic) - skipping node table JOIN",
                        right_alias
                    );
                    // Mark as "joined" to avoid duplicate processing
                    joined_entities.insert(right_alias.to_string());
                } else {
                    eprintln!("    � ? Creating JOIN for RIGHT '{}'", right_alias);
                    
                    // DENORMALIZED EDGE CHECK: Register alias if node is on edge table
                    let right_is_denormalized = is_node_denormalized_on_edge(
                        &right_node_schema,
                        &rel_schema,
                        false  // is_from_node = false (RIGHT connects to rel's to_node)
                    );
                    
                    if right_is_denormalized {
                        plan_ctx.register_denormalized_alias(
                            right_alias.to_string(),
                            rel_alias.to_string(),
                            false,  // is_from_node
                            right_label.clone(),
                            rel_type.to_string(),
                        );
                        eprintln!(
                            "    DENORMALIZED: Registered RIGHT alias '{}' → rel '{}' (to_node)",
                            right_alias, rel_alias
                        );
                        joined_entities.insert(right_alias.to_string());
                    } else {
                        // For bidirectional patterns, target connects to either end of relationship
                        // and excludes the anchor node (to avoid self-match)
                        let right_joining_on = if is_bidirectional {
                            vec![generate_target_to_rel_bidirectional(
                                right_alias,
                                &right_node_id_column,
                                rel_alias,
                                &rel_from_col,
                                &rel_to_col,
                                left_alias,
                                &left_node_id_column,
                            )]
                        } else {
                            // Standard directed join
                            vec![OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    LogicalExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(right_alias.to_string()),
                                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(right_node_id_column.clone()),
                                    }),
                                    LogicalExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(rel_alias.to_string()),
                                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(right_conn_with_rel.clone()),
                                    }),
                                ],
                            }]
                        };
                        
                        let right_graph_join = Join {
                            table_name: right_cte_name.clone(),
                            table_alias: right_alias.to_string(),
                            joining_on: right_joining_on,
                            join_type: Self::determine_join_type(right_is_optional),
                            pre_filter: None,
                        };
                        collected_graph_joins.push(right_graph_join);
                        joined_entities.insert(right_alias.to_string());
                    }
                }
                Ok(())
            }
        } else
        // check if right is connected with edge list's from_node
        if rel_schema.from_node == right_node_schema.table_name {
            // this means rel.from_node = right and to_node = left

            // check if right is already joined
            if joined_entities.contains(right_alias) {
                // join the rel with right first and then join the left with rel
                // NOTE: left_connection and right_connection in GraphRel are ALREADY adjusted for direction
                // in match_clause.rs lines 341-345. So we just connect:
                //   - RIGHT node to rel.to_id (the target of the relationship)
                //   - LEFT node to rel.from_id (the source of the relationship)
                // No need to check direction here - it's already encoded in left_conn/right_conn!

                let polymorphic_filter = generate_polymorphic_edge_filter(
                    rel_alias,
                    &rel_types,
                    rel_schema,
                    &left_label,
                    &right_label,
                );
                let mut rel_graph_join = Join {
                    table_name: rel_cte_name.clone(),
                    table_alias: rel_alias.to_string(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: crate::graph_catalog::expression_parser::PropertyValue::Column(rel_to_col.clone()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(right_alias.to_string()),
                                column: crate::graph_catalog::expression_parser::PropertyValue::Column(right_node_id_column.clone()),
                            }),
                        ],
                    }],
                    join_type: Self::determine_join_type(rel_is_optional),
                    pre_filter: polymorphic_filter,
                };

                // Node join not needed for edge list (different node types)
                // let left_graph_join = Join {
                //     table_name: left_cte_name,
                //     table_alias: left_alias.to_string(),
                //     joining_on: vec![OperatorApplication {
                //         operator: Operator::Equal,
                //         operands: vec![
                //             LogicalExpr::PropertyAccessExp(PropertyAccess {
                //                 table_alias: TableAlias(left_alias.to_string()),
                //                 column: crate::graph_catalog::expression_parser::PropertyValue::Column(left_node_id_column.clone()),
                //             }),
                //             LogicalExpr::PropertyAccessExp(PropertyAccess {
                //                 table_alias: TableAlias(rel_alias.to_string()),
                //                 column: crate::graph_catalog::expression_parser::PropertyValue::Column(rel_to_col.clone()),
                //             }),
                //         ],
                //     }],
                //     join_type: Self::determine_join_type(left_is_optional),
                // };

                if is_standalone_rel {
                    let rel_to_left_graph_join_keys = OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: crate::graph_catalog::expression_parser::PropertyValue::Column(rel_from_col.clone()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(left_alias.to_string()),
                                column: crate::graph_catalog::expression_parser::PropertyValue::Column(left_node_id_column),
                            }),
                        ],
                    };
                    rel_graph_join.joining_on.push(rel_to_left_graph_join_keys);

                    collected_graph_joins.push(rel_graph_join);
                    joined_entities.insert(rel_alias.to_string());
                    // in this case we will only join relation so early return without pushing the other joins
                    return Ok(());
                }

                // For edge list (different node types, right in joined_entities): always join left
                // MULTI-HOP FIX: The relationship JOIN we're about to push references LEFT in its ON condition,
                // so LEFT MUST be joined first, regardless of whether it's explicitly referenced in SELECT/WHERE.
                // This fixes multi-hop patterns like (u)-[:FOLLOWS]->(friend)-[:FOLLOWS]->(fof)
                // where 'friend' is an intermediate node.
                collected_graph_joins.push(rel_graph_join);
                joined_entities.insert(rel_alias.to_string());

                // Right is already joined (it was the anchor or previous RIGHT node)
                // No need to insert again

                // Always create JOIN for LEFT since the relationship references it
                
                // DENORMALIZED EDGE CHECK: Register alias if node is on edge table
                let left_is_denormalized = is_node_denormalized_on_edge(
                    &left_node_schema,
                    &rel_schema,
                    true  // is_from_node = true
                );
                
                if left_is_denormalized {
                    plan_ctx.register_denormalized_alias(
                        left_alias.to_string(),
                        rel_alias.to_string(),
                        true,  // is_from_node
                        left_label.clone(),
                        rel_type.to_string(),
                    );
                    eprintln!(
                        "    DENORMALIZED: Registered LEFT alias '{}' → rel '{}' (from_node)",
                        left_alias, rel_alias
                    );
                } else {
                    let left_graph_join = Join {
                        table_name: left_cte_name.clone(),
                        table_alias: left_alias.to_string(),
                        joining_on: vec![OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                LogicalExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(left_alias.to_string()),
                                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(left_node_id_column.clone()),
                                }),
                                LogicalExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(rel_alias.to_string()),
                                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(rel_from_col),
                                }),
                            ],
                        }],
                        join_type: Self::determine_join_type(left_is_optional),
                    pre_filter: None,
                    };
                    collected_graph_joins.push(left_graph_join);
                }
                joined_entities.insert(left_alias.to_string());
                Ok(())
            } else {
                // When left is already joined or start of the join

                // join the relation with left side first and then
                // the join the right side with relation
                // NOTE: left_connection and right_connection in GraphRel are ALREADY adjusted for direction
                // in match_clause.rs lines 341-345. So we just connect:
                //   - LEFT node to rel.from_id (the source of the relationship)
                //   - RIGHT node to rel.to_id (the target of the relationship)
                // No need to check direction here - it's already encoded in left_conn/right_conn!

                let polymorphic_filter = generate_polymorphic_edge_filter(
                    rel_alias,
                    &rel_types,
                    rel_schema,
                    &left_label,
                    &right_label,
                );
                let mut rel_graph_join = Join {
                    table_name: rel_cte_name.clone(),
                    table_alias: rel_alias.to_string(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: crate::graph_catalog::expression_parser::PropertyValue::Column(rel_from_col.clone()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(left_alias.to_string()),
                                column: crate::graph_catalog::expression_parser::PropertyValue::Column(left_node_id_column.clone()),
                            }),
                        ],
                    }],
                    join_type: Self::determine_join_type(rel_is_optional),
                    pre_filter: polymorphic_filter,
                };

                // Node join not needed for edge list (different node types)
                // let right_graph_join = Join {
                //     table_name: right_cte_name,
                //     table_alias: right_alias.to_string(),
                //     joining_on: vec![OperatorApplication {
                //         operator: Operator::Equal,
                //         operands: vec![
                //             LogicalExpr::PropertyAccessExp(PropertyAccess {
                //                 table_alias: TableAlias(right_alias.to_string()),
                //                 column: crate::graph_catalog::expression_parser::PropertyValue::Column(right_node_id_column.clone()),
                //             }),
                //             LogicalExpr::PropertyAccessExp(PropertyAccess {
                //                 table_alias: TableAlias(rel_alias.to_string()),
                //                 column: crate::graph_catalog::expression_parser::PropertyValue::Column(rel_from_col.clone()),
                //             }),
                //         ],
                //     }],
                //     join_type: Self::determine_join_type(right_is_optional),
                // };

                if is_standalone_rel {
                    let rel_to_right_graph_join_keys = OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: crate::graph_catalog::expression_parser::PropertyValue::Column(rel_to_col.clone()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(right_alias.to_string()),
                                column: crate::graph_catalog::expression_parser::PropertyValue::Column(right_node_id_column),
                            }),
                        ],
                    };
                    rel_graph_join.joining_on.push(rel_to_right_graph_join_keys);

                    collected_graph_joins.push(rel_graph_join);
                    joined_entities.insert(rel_alias.to_string());
                    // in this case we will only join relation so early return without pushing the other joins
                    return Ok(());
                }

                // For edge list (different node types, left in joined_entities): always join right
                // MULTI-HOP FIX: The relationship JOIN references LEFT, and then RIGHT must be joined
                // to complete the chain. Always create the RIGHT JOIN for consistency.
                collected_graph_joins.push(rel_graph_join);
                joined_entities.insert(rel_alias.to_string());

                // Left is already joined (it was the anchor or previous LEFT node)
                // No need to insert again

                // Always create JOIN for RIGHT to complete the relationship chain
                
                // DENORMALIZED EDGE CHECK: Register alias if node is on edge table
                let right_is_denormalized = is_node_denormalized_on_edge(
                    &right_node_schema,
                    &rel_schema,
                    true  // is_from_node = true (reversed branch: right connects to from_node)
                );
                
                if right_is_denormalized {
                    plan_ctx.register_denormalized_alias(
                        right_alias.to_string(),
                        rel_alias.to_string(),
                        true,  // is_from_node (reversed)
                        right_label.clone(),
                        rel_type.to_string(),
                    );
                    eprintln!(
                        "    DENORMALIZED: Registered RIGHT alias '{}' → rel '{}' (from_node, reversed)",
                        right_alias, rel_alias
                    );
                } else {
                    let right_graph_join = Join {
                        table_name: right_cte_name.clone(),
                        table_alias: right_alias.to_string(),
                        joining_on: vec![OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                LogicalExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(right_alias.to_string()),
                                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(right_node_id_column.clone()),
                                }),
                                LogicalExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(rel_alias.to_string()),
                                    column: crate::graph_catalog::expression_parser::PropertyValue::Column(rel_to_col),
                                }),
                            ],
                        }],
                        join_type: Self::determine_join_type(right_is_optional),
                    pre_filter: None,
                    };
                    collected_graph_joins.push(right_graph_join);
                }
                joined_entities.insert(right_alias.to_string());
                Ok(())
            }
        } else {
            // this means rel.from_node = left and to_node = right

            // check if right is already joined
            if joined_entities.contains(right_alias) {
                // join the rel with right first and then join the left with rel
                let polymorphic_filter = generate_polymorphic_edge_filter(
                    rel_alias,
                    &rel_types,
                    rel_schema,
                    &left_label,
                    &right_label,
                );
                let mut rel_graph_join = Join {
                    table_name: rel_cte_name.clone(),
                    table_alias: rel_alias.to_string(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: crate::graph_catalog::expression_parser::PropertyValue::Column("to_id".to_string()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(right_alias.to_string()),
                                column: crate::graph_catalog::expression_parser::PropertyValue::Column(right_node_id_column.clone()),
                            }),
                        ],
                    }],
                    join_type: Self::determine_join_type(rel_is_optional),
                    pre_filter: polymorphic_filter,
                };

                // Node join not needed for edge list (different node types)
                // let left_graph_join = Join {
                //     table_name: left_cte_name,
                //     table_alias: left_alias.to_string(),
                //     joining_on: vec![OperatorApplication {
                //         operator: Operator::Equal,
                //         operands: vec![
                //             LogicalExpr::PropertyAccessExp(PropertyAccess {
                //                 table_alias: TableAlias(left_alias.to_string()),
                //                 column: crate::graph_catalog::expression_parser::PropertyValue::Column(left_node_id_column.clone()),
                //             }),
                //             LogicalExpr::PropertyAccessExp(PropertyAccess {
                //                 table_alias: TableAlias(rel_alias.to_string()),
                //                 column: crate::graph_catalog::expression_parser::PropertyValue::Column(rel_from_col.clone()),
                //             }),
                //         ],
                //     }],
                //     join_type: Self::determine_join_type(left_is_optional),
                // };

                if is_standalone_rel {
                    let rel_to_right_graph_join_keys = OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: crate::graph_catalog::expression_parser::PropertyValue::Column(rel_from_col.clone()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(left_alias.to_string()),
                                column: crate::graph_catalog::expression_parser::PropertyValue::Column(left_node_id_column),
                            }),
                        ],
                    };
                    rel_graph_join.joining_on.push(rel_to_right_graph_join_keys);

                    collected_graph_joins.push(rel_graph_join);
                    joined_entities.insert(rel_alias.to_string());
                    // in this case we will only join relation so early return without pushing the other joins
                    return Ok(());
                }

                // For edge list (different node types, from/to, right already joined): only join left if referenced
                collected_graph_joins.push(rel_graph_join);
                joined_entities.insert(rel_alias.to_string());

                // Right is already joined
                joined_entities.insert(right_alias.to_string());

                // Only join the left node if it's actually referenced in the query
                if left_is_referenced {
                    // DENORMALIZED EDGE CHECK: Register alias if node is on edge table
                    let left_is_denormalized = is_node_denormalized_on_edge(
                        &left_node_schema,
                        &rel_schema,
                        false  // is_from_node = false (since this is the reversed direction branch)
                    );
                    
                    if left_is_denormalized {
                        plan_ctx.register_denormalized_alias(
                            left_alias.to_string(),
                            rel_alias.to_string(),
                            false,  // is_from_node (reversed)
                            left_label.clone(),
                            rel_type.to_string(),
                        );
                        eprintln!(
                            "    DENORMALIZED: Registered LEFT alias '{}' → rel '{}' (to_node, reversed)",
                            left_alias, rel_alias
                        );
                    } else {
                        let left_graph_join = Join {
                            table_name: left_cte_name.clone(),
                            table_alias: left_alias.to_string(),
                            joining_on: vec![OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    LogicalExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(left_alias.to_string()),
                                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(left_node_id_column.clone()),
                                    }),
                                    LogicalExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(rel_alias.to_string()),
                                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(rel_from_col.clone()),
                                    }),
                                ],
                            }],
                            join_type: Self::determine_join_type(left_is_optional),
                    pre_filter: None,
                        };
                        collected_graph_joins.push(left_graph_join);
                    }
                    joined_entities.insert(left_alias.to_string());
                } else {
                    // Mark as joined even though we didn't create a JOIN
                    joined_entities.insert(left_alias.to_string());
                }
                Ok(())
            } else {
                // When left is already joined or start of the join

                // join the relation with left side first and then
                // the join the right side with relation
                let polymorphic_filter = generate_polymorphic_edge_filter(
                    rel_alias,
                    &rel_types,
                    rel_schema,
                    &left_label,
                    &right_label,
                );
                let mut rel_graph_join = Join {
                    table_name: rel_cte_name.clone(),
                    table_alias: rel_alias.to_string(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: crate::graph_catalog::expression_parser::PropertyValue::Column(rel_from_col.clone()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(left_alias.to_string()),
                                column: crate::graph_catalog::expression_parser::PropertyValue::Column(left_node_id_column.clone()),
                            }),
                        ],
                    }],
                    join_type: Self::determine_join_type(rel_is_optional),
                    pre_filter: polymorphic_filter,
                };

                // Node join not needed for edge list (different node types)
                // let right_graph_join = Join {
                //     table_name: right_cte_name,
                //     table_alias: right_alias.to_string(),
                //     joining_on: vec![OperatorApplication {
                //         operator: Operator::Equal,
                //         operands: vec![
                //             LogicalExpr::PropertyAccessExp(PropertyAccess {
                //                 table_alias: TableAlias(right_alias.to_string()),
                //                 column: crate::graph_catalog::expression_parser::PropertyValue::Column(right_node_id_column.clone()),
                //             }),
                //             LogicalExpr::PropertyAccessExp(PropertyAccess {
                //                 table_alias: TableAlias(rel_alias.to_string()),
                //                 column: crate::graph_catalog::expression_parser::PropertyValue::Column(rel_to_col.clone()),
                //             }),
                //         ],
                //     }],
                //     join_type: Self::determine_join_type(right_is_optional),
                // };

                if is_standalone_rel {
                    let rel_to_right_graph_join_keys = OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: crate::graph_catalog::expression_parser::PropertyValue::Column("to_id".to_string()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(right_alias.to_string()),
                                column: crate::graph_catalog::expression_parser::PropertyValue::Column(right_node_id_column),
                            }),
                        ],
                    };
                    rel_graph_join.joining_on.push(rel_to_right_graph_join_keys);

                    collected_graph_joins.push(rel_graph_join);
                    joined_entities.insert(rel_alias.to_string());
                    // in this case we will only join relation so early return without pushing the other joins
                    return Ok(());
                }

                // For edge list (different node types, left already joined): only join right if referenced
                collected_graph_joins.push(rel_graph_join);
                joined_entities.insert(rel_alias.to_string());

                // Left is already joined
                joined_entities.insert(left_alias.to_string());

                // FIX: Always join RIGHT if rel references it (even for anonymous nodes)
                // The relationship JOIN condition references right_alias, so it MUST be in scope
                eprintln!("    � ?? FIX: Joining RIGHT regardless of is_referenced for JOIN scope");
                if true {
                    // Was: right_is_referenced
                    
                    // DENORMALIZED EDGE CHECK: Register alias if node is on edge table
                    let right_is_denormalized = is_node_denormalized_on_edge(
                        &right_node_schema,
                        &rel_schema,
                        false  // is_from_node = false (RIGHT connects to to_node)
                    );
                    
                    if right_is_denormalized {
                        plan_ctx.register_denormalized_alias(
                            right_alias.to_string(),
                            rel_alias.to_string(),
                            false,  // is_from_node
                            right_label.clone(),
                            rel_type.to_string(),
                        );
                        eprintln!(
                            "    DENORMALIZED: Registered RIGHT alias '{}' → rel '{}' (to_node)",
                            right_alias, rel_alias
                        );
                    } else {
                        let right_graph_join = Join {
                            table_name: right_cte_name.clone(),
                            table_alias: right_alias.to_string(),
                            joining_on: vec![OperatorApplication {
                                operator: Operator::Equal,
                                operands: vec![
                                    LogicalExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(right_alias.to_string()),
                                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(right_node_id_column.clone()),
                                    }),
                                    LogicalExpr::PropertyAccessExp(PropertyAccess {
                                        table_alias: TableAlias(rel_alias.to_string()),
                                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(rel_to_col.clone()),
                                    }),
                                ],
                            }],
                            join_type: Self::determine_join_type(right_is_optional),
                    pre_filter: None,
                        };
                        collected_graph_joins.push(right_graph_join);
                    }
                    joined_entities.insert(right_alias.to_string());
                } else {
                    // Mark as joined even though we didn't create a JOIN
                    joined_entities.insert(right_alias.to_string());
                }
                Ok(())
            }
        }
    }

    // BITMAP traversal removed - ClickGraph only supports EDGE LIST (relationship as explicit table)
    // Legacy BITMAP code from upstream Brahmand has been removed for simplicity
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        graph_catalog::graph_schema::{GraphSchema, NodeIdSchema, NodeSchema, RelationshipSchema},
        query_planner::{
            logical_expr::{Direction, LogicalExpr, Operator, PropertyAccess, TableAlias},
            logical_plan::{
                GraphNode, GraphRel, JoinType, LogicalPlan, Projection, ProjectionItem, Scan,
            },
            plan_ctx::{PlanCtx, TableCtx},
        },
    };
    use std::collections::HashMap;

    fn create_test_graph_schema() -> GraphSchema {
        let mut nodes = HashMap::new();
        let mut relationships = HashMap::new();

        // Create Person node schema
        nodes.insert(
            "Person".to_string(),
            NodeSchema {
                database: "default".to_string(),
                table_name: "Person".to_string(),
                column_names: vec!["id".to_string(), "name".to_string(), "age".to_string()],
                primary_keys: "id".to_string(),
                node_id: NodeIdSchema {
                    column: "id".to_string(),
                    dtype: "UInt64".to_string(),
                },
                property_mappings: HashMap::new(),
                view_parameters: None,
                engine: None,
                use_final: None,
            filter: None,
                is_denormalized: false,
                from_properties: None,
                to_properties: None,
                denormalized_source_table: None,
            },
        );

        // Create Company node schema
        nodes.insert(
            "Company".to_string(),
            NodeSchema {
                database: "default".to_string(),
                table_name: "Company".to_string(),
                column_names: vec!["id".to_string(), "name".to_string(), "founded".to_string()],
                primary_keys: "id".to_string(),
                node_id: NodeIdSchema {
                    column: "id".to_string(),
                    dtype: "UInt64".to_string(),
                },
                property_mappings: HashMap::new(),
                view_parameters: None,
                engine: None,
                use_final: None,
            filter: None,
                is_denormalized: false,
                from_properties: None,
                to_properties: None,
                denormalized_source_table: None,
            },
        );

        // Create FOLLOWS relationship schema (edge list)
        relationships.insert(
            "FOLLOWS".to_string(),
            RelationshipSchema {
                database: "default".to_string(),
                table_name: "FOLLOWS".to_string(),
                column_names: vec![
                    "from_id".to_string(),
                    "to_id".to_string(),
                    "since".to_string(),
                ],
                from_node: "Person".to_string(),
                to_node: "Person".to_string(),
                from_id: "from_id".to_string(),
                to_id: "to_id".to_string(),
                from_node_id_dtype: "UInt64".to_string(),
                to_node_id_dtype: "UInt64".to_string(),
                property_mappings: HashMap::new(),
                view_parameters: None,
                engine: None,
                use_final: None,
            filter: None,
                edge_id: None,
                type_column: None,
                from_label_column: None,
                to_label_column: None,
                from_node_properties: None,
                to_node_properties: None,
            },
        );

        // Create WORKS_AT relationship schema (edge list)
        relationships.insert(
            "WORKS_AT".to_string(),
            RelationshipSchema {
                database: "default".to_string(),
                table_name: "WORKS_AT".to_string(),
                column_names: vec![
                    "from_id".to_string(),
                    "to_id".to_string(),
                    "position".to_string(),
                ],
                from_node: "Person".to_string(),
                to_node: "Company".to_string(),
                from_id: "from_id".to_string(),
                to_id: "to_id".to_string(),
                from_node_id_dtype: "UInt64".to_string(),
                to_node_id_dtype: "UInt64".to_string(),
                property_mappings: HashMap::new(),
                view_parameters: None,
                engine: None,
                use_final: None,
            filter: None,
                edge_id: None,
                type_column: None,
                from_label_column: None,
                to_label_column: None,
                from_node_properties: None,
                to_node_properties: None,
            },
        );

        GraphSchema::build(1, "default".to_string(), nodes, relationships)
    }

    fn setup_plan_ctx_with_graph_entities() -> PlanCtx {
        let mut plan_ctx = PlanCtx::default();

        // Add person nodes
        plan_ctx.insert_table_ctx(
            "p1".to_string(),
            TableCtx::build(
                "p1".to_string(),
                Some(vec!["Person".to_string()]),
                vec![],
                false,
                true,
            ),
        );
        plan_ctx.insert_table_ctx(
            "p2".to_string(),
            TableCtx::build(
                "p2".to_string(),
                Some(vec!["Person".to_string()]),
                vec![],
                false,
                true,
            ),
        );
        plan_ctx.insert_table_ctx(
            "p3".to_string(),
            TableCtx::build(
                "p3".to_string(),
                Some(vec!["Person".to_string()]),
                vec![],
                false,
                true,
            ),
        );

        // Add company node
        plan_ctx.insert_table_ctx(
            "c1".to_string(),
            TableCtx::build(
                "c1".to_string(),
                Some(vec!["Company".to_string()]),
                vec![],
                false,
                true,
            ),
        );

        // Add follows relationships
        plan_ctx.insert_table_ctx(
            "f1".to_string(),
            TableCtx::build(
                "f1".to_string(),
                Some(vec!["FOLLOWS".to_string()]),
                vec![],
                true,
                true,
            ),
        );
        plan_ctx.insert_table_ctx(
            "f2".to_string(),
            TableCtx::build(
                "f2".to_string(),
                Some(vec!["FOLLOWS".to_string()]),
                vec![],
                true,
                true,
            ),
        );

        // Add works_at relationship
        plan_ctx.insert_table_ctx(
            "w1".to_string(),
            TableCtx::build(
                "w1".to_string(),
                Some(vec!["WORKS_AT".to_string()]),
                vec![],
                true,
                true,
            ),
        );

        plan_ctx
    }

    fn create_scan_plan(table_alias: &str, table_name: &str) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::Scan(Scan {
            table_alias: Some(table_alias.to_string()),
            table_name: Some(table_name.to_string()),
        }))
    }

    fn create_graph_node(input: Arc<LogicalPlan>, alias: &str, is_denormalized: bool) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::GraphNode(GraphNode {
            input,
            alias: alias.to_string(),
            label: None,
            is_denormalized,
        }))
    }

    fn create_graph_rel(
        left: Arc<LogicalPlan>,
        center: Arc<LogicalPlan>,
        right: Arc<LogicalPlan>,
        alias: &str,
        direction: Direction,
        left_connection: &str,
        right_connection: &str,
    ) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::GraphRel(GraphRel {
            left,
            center,
            right,
            alias: alias.to_string(),
            direction,
            left_connection: left_connection.to_string(),
            right_connection: right_connection.to_string(),
            is_rel_anchor: false,
            variable_length: None,
            shortest_path_mode: None,
            path_variable: None,
            where_predicate: None, // Will be populated by filter pushdown
            labels: None,
            is_optional: None,
            anchor_connection: None,
        }))
    }

    #[test]
    fn test_no_graph_joins_when_no_graph_rels() {
        let analyzer = GraphJoinInference::new();
        let graph_schema = create_test_graph_schema();
        let mut plan_ctx = setup_plan_ctx_with_graph_entities();

        // Create a plan with only a graph node (no relationships)
        let scan = create_scan_plan("p1", "person");
        let graph_node = create_graph_node(scan, "p1", false);

        let result = analyzer
            .analyze_with_graph_schema(graph_node.clone(), &mut plan_ctx, &graph_schema)
            .unwrap();

        // Should not transform the plan since there are no graph relationships
        match result {
            Transformed::No(plan) => {
                assert_eq!(plan, graph_node);
            }
            _ => panic!("Expected no transformation for plan without relationships"),
        }
    }

    #[test]
    fn test_edge_list_same_node_type_outgoing_direction() {
        let analyzer = GraphJoinInference::new();
        let graph_schema = create_test_graph_schema();
        let mut plan_ctx = setup_plan_ctx_with_graph_entities();

        // Set the relationship to use edge list
        plan_ctx.get_mut_table_ctx("f1").unwrap();

        // Create plan: (p1)-[f1:FOLLOWS]->(p2)
        let p1_scan = create_scan_plan("p1", "Person");
        let p1_node = create_graph_node(p1_scan, "p1", false);

        let f1_scan = create_scan_plan("f1", "FOLLOWS");

        let p2_scan = create_scan_plan("p2", "Person");
        let p2_node = create_graph_node(p2_scan, "p2", false);

        let graph_rel = create_graph_rel(
            p2_node,
            f1_scan,
            p1_node,
            "f1",
            Direction::Outgoing,
            "p2",
            "p1",
        );

        let input_logical_plan = Arc::new(LogicalPlan::Projection(Projection {
            input: graph_rel,
            items: vec![ProjectionItem {
                expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("p1".to_string()),
                    column: crate::graph_catalog::expression_parser::PropertyValue::Column("name".to_string()),
                }),
                col_alias: None,
            }],
            kind: crate::query_planner::logical_plan::ProjectionKind::Return,
            distinct: false,
        }));

        let result = analyzer
            .analyze_with_graph_schema(input_logical_plan, &mut plan_ctx, &graph_schema)
            .unwrap();

        println!("\n result: {:?}\n", result);

        // Should create graph joins
        match result {
            Transformed::Yes(plan) => {
                match plan.as_ref() {
                    LogicalPlan::GraphJoins(graph_joins) => {
                        // Assert GraphJoins structure
                        // Anchor node (p2) goes to FROM clause, not JOIN
                        // Pattern: (p2)-[f1:FOLLOWS]->(p1) creates 2 joins: f1, p1
                        // p2 is in anchor_table, not in joins list
                        assert_eq!(graph_joins.joins.len(), 2);
                        assert!(matches!(
                            graph_joins.input.as_ref(),
                            LogicalPlan::Projection(_)
                        ));
                        assert_eq!(graph_joins.anchor_table, Some("p2".to_string()));

                        // First join: relationship (f1)
                        let rel_join = &graph_joins.joins[0];
                        assert_eq!(rel_join.table_name, "default.FOLLOWS");
                        assert_eq!(rel_join.table_alias, "f1");
                        assert_eq!(rel_join.join_type, JoinType::Inner);
                        assert_eq!(rel_join.joining_on.len(), 1);

                        // Assert the joining condition for relationship
                        let rel_join_condition = &rel_join.joining_on[0];
                        assert_eq!(rel_join_condition.operator, Operator::Equal);
                        assert_eq!(rel_join_condition.operands.len(), 2);

                        // Check operands are PropertyAccessExp with correct table aliases and columns
                        match (
                            &rel_join_condition.operands[0],
                            &rel_join_condition.operands[1],
                        ) {
                            (
                                LogicalExpr::PropertyAccessExp(rel_prop),
                                LogicalExpr::PropertyAccessExp(left_prop),
                            ) => {
                                assert_eq!(rel_prop.table_alias.0, "f1");
                                // For outgoing relationship (p2)-[:FOLLOWS]->(p1),
                                // p2 is the source (left), so it connects to from_id
                                assert_eq!(rel_prop.column.raw(), "from_id");
                                assert_eq!(left_prop.table_alias.0, "p2");
                                assert_eq!(left_prop.column.raw(), "id");
                            }
                            _ => panic!("Expected PropertyAccessExp operands"),
                        }

                        // Second join: right node (p1)
                        let p1_join = &graph_joins.joins[1];
                        assert_eq!(p1_join.table_name, "default.Person");
                        assert_eq!(p1_join.table_alias, "p1");
                        assert_eq!(p1_join.join_type, JoinType::Inner);
                        assert_eq!(p1_join.joining_on.len(), 1);

                        let p1_join_condition = &p1_join.joining_on[0];
                        assert_eq!(p1_join_condition.operator, Operator::Equal);
                        match (
                            &p1_join_condition.operands[0],
                            &p1_join_condition.operands[1],
                        ) {
                            (
                                LogicalExpr::PropertyAccessExp(p1_prop),
                                LogicalExpr::PropertyAccessExp(rel_prop),
                            ) => {
                                assert_eq!(p1_prop.table_alias.0, "p1");
                                assert_eq!(p1_prop.column.raw(), "id");
                                assert_eq!(rel_prop.table_alias.0, "f1");
                                assert_eq!(rel_prop.column.raw(), "to_id");
                            }
                            _ => panic!("Expected PropertyAccessExp operands for p1 join"),
                        }
                    }
                    _ => panic!("Expected GraphJoins node"),
                }
            }
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_edge_list_different_node_types() {
        let analyzer = GraphJoinInference::new();
        let graph_schema = create_test_graph_schema();
        let mut plan_ctx = setup_plan_ctx_with_graph_entities();

        // Set the relationship to use edge list
        plan_ctx.get_mut_table_ctx("w1").unwrap();

        // Create plan: (p1)-[w1:WORKS_AT]->(c1)
        let p1_scan = create_scan_plan("p1", "Person");
        let p1_node = create_graph_node(p1_scan, "p1", false);

        let w1_scan = create_scan_plan("w1", "WORKS_AT");

        let c1_scan = create_scan_plan("c1", "Company");
        let c1_node = create_graph_node(c1_scan, "c1", false);

        let graph_rel = create_graph_rel(
            p1_node,
            w1_scan,
            c1_node,
            "w1",
            Direction::Outgoing,
            "p1", // left_connection (p1 is the LEFT node)
            "c1", // right_connection (c1 is the RIGHT node)
        );

        let input_logical_plan = Arc::new(LogicalPlan::Projection(Projection {
            input: graph_rel,
            items: vec![ProjectionItem {
                expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("p1".to_string()),
                    column: crate::graph_catalog::expression_parser::PropertyValue::Column("name".to_string()),
                }),
                col_alias: None,
            }],
            kind: crate::query_planner::logical_plan::ProjectionKind::Return,
            distinct: false,
        }));

        let result = analyzer
            .analyze_with_graph_schema(input_logical_plan, &mut plan_ctx, &graph_schema)
            .unwrap();

        // Should create graph joins for different node types
        match result {
            Transformed::Yes(plan) => {
                match plan.as_ref() {
                    LogicalPlan::GraphJoins(graph_joins) => {
                        // Assert GraphJoins structure
                        // Multi-hop fix: Now creates joins for both relationship and end node
                        assert_eq!(graph_joins.joins.len(), 2); // w1 (relationship) + c1 (end node)
                        assert!(matches!(
                            graph_joins.input.as_ref(),
                            LogicalPlan::Projection(_)
                        ));

                        // (p1)-[w1:WORKS_AT]->(c1)
                        // Multi-hop fix: Creates joins for both w1 (relationship) and c1 (end node)
                        let rel_join = &graph_joins.joins[0];
                        assert_eq!(rel_join.table_name, "default.WORKS_AT"); // CTE name includes database prefix
                        assert_eq!(rel_join.table_alias, "w1");
                        assert_eq!(rel_join.join_type, JoinType::Inner);
                        assert_eq!(rel_join.joining_on.len(), 1);

                        // Assert the joining condition for relationship
                        let rel_join_condition = &rel_join.joining_on[0];
                        assert_eq!(rel_join_condition.operator, Operator::Equal);
                        assert_eq!(rel_join_condition.operands.len(), 2);

                        // Check operands are PropertyAccessExp with correct table aliases and columns
                        // For pattern (p1)-[w1:WORKS_AT]->(c1) with Direction::Outgoing,
                        // p1 is the source (LEFT), so it connects to from_id
                        match (
                            &rel_join_condition.operands[0],
                            &rel_join_condition.operands[1],
                        ) {
                            (
                                LogicalExpr::PropertyAccessExp(rel_prop),
                                LogicalExpr::PropertyAccessExp(left_prop),
                            ) => {
                                assert_eq!(rel_prop.table_alias.0, "w1");
                                assert_eq!(rel_prop.column.raw(), "from_id");
                                assert_eq!(left_prop.table_alias.0, "p1");
                                assert_eq!(left_prop.column.raw(), "id");
                            }
                            _ => panic!("Expected PropertyAccessExp operands"),
                        }
                    }
                    _ => panic!("Expected GraphJoins node"),
                }
            }
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    #[ignore] // Bitmap indexes not used in current schema - edge lists only (use_edge_list flag removed)
    fn test_bitmap_traversal() {
        let analyzer = GraphJoinInference::new();
        let graph_schema = create_test_graph_schema();
        let mut plan_ctx = setup_plan_ctx_with_graph_entities();

        // This test is obsolete - ClickGraph only uses edge lists
        // Bitmap traversal functionality has been removed

        // Create plan: (p1)-[f1:FOLLOWS]->(p2)
        let p1_scan = create_scan_plan("p1", "Person");
        let p1_node = create_graph_node(p1_scan, "p1", false);

        let f1_scan = create_scan_plan("f1", "FOLLOWS");

        // Add follows relationships
        plan_ctx.insert_table_ctx(
            "f1".to_string(),
            TableCtx::build(
                "f1".to_string(),
                Some(vec!["FOLLOWS_outgoing".to_string()]),
                vec![],
                true,
                true,
            ),
        );

        let p2_scan = create_scan_plan("p2", "Person");
        let p2_node = create_graph_node(p2_scan, "p2", false);

        let graph_rel = create_graph_rel(
            p2_node,
            f1_scan,
            p1_node,
            "f1",
            Direction::Outgoing,
            "p2",
            "p1",
        );

        let input_logical_plan = Arc::new(LogicalPlan::Projection(Projection {
            input: graph_rel,
            items: vec![ProjectionItem {
                expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("p1".to_string()),
                    column: crate::graph_catalog::expression_parser::PropertyValue::Column("name".to_string()),
                }),
                col_alias: None,
            }],
            kind: crate::query_planner::logical_plan::ProjectionKind::Return,
            distinct: false,
        }));

        let result = analyzer
            .analyze_with_graph_schema(input_logical_plan, &mut plan_ctx, &graph_schema)
            .unwrap();

        // Should create graph joins for bitmap traversal
        match result {
            Transformed::Yes(plan) => {
                match plan.as_ref() {
                    LogicalPlan::GraphJoins(graph_joins) => {
                        // Assert GraphJoins structure
                        assert_eq!(graph_joins.joins.len(), 1); // Simple relationship: only relationship join, start node is in FROM
                        assert!(matches!(
                            graph_joins.input.as_ref(),
                            LogicalPlan::Projection(_)
                        ));

                        // (p1)-[f1:FOLLOWS]->(p2)
                        // For bitmap traversal, only relationship join is needed (start node in FROM)
                        let rel_join = &graph_joins.joins[0];
                        assert_eq!(rel_join.table_name, "FOLLOWS"); // Now uses actual table name
                        assert_eq!(rel_join.table_alias, "f1");
                        assert_eq!(rel_join.join_type, JoinType::Inner);
                        assert_eq!(rel_join.joining_on.len(), 1);

                        // Assert the joining condition for relationship
                        let rel_join_condition = &rel_join.joining_on[0];
                        assert_eq!(rel_join_condition.operator, Operator::Equal);
                        assert_eq!(rel_join_condition.operands.len(), 2);

                        // Check operands are PropertyAccessExp with correct table aliases and columns
                        match (
                            &rel_join_condition.operands[0],
                            &rel_join_condition.operands[1],
                        ) {
                            (
                                LogicalExpr::PropertyAccessExp(rel_prop),
                                LogicalExpr::PropertyAccessExp(right_prop),
                            ) => {
                                assert_eq!(rel_prop.table_alias.0, "f1");
                                assert_eq!(rel_prop.column.raw(), "to_id");
                                assert_eq!(right_prop.table_alias.0, "p2");
                                assert_eq!(right_prop.column.raw(), "id");
                            }
                            _ => panic!("Expected PropertyAccessExp operands"),
                        }
                    }
                    _ => panic!("Expected GraphJoins node"),
                }
            }
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_standalone_relationship_edge_list() {
        let analyzer = GraphJoinInference::new();
        let graph_schema = create_test_graph_schema();
        let mut plan_ctx = setup_plan_ctx_with_graph_entities();

        // Set the relationship to use edge list
        plan_ctx.get_mut_table_ctx("f2").unwrap();

        // Create standalone relationship: (p3)-[f2:FOLLOWS]-(Empty)
        // This simulates a case where left node was already processed/removed
        let empty_left = Arc::new(LogicalPlan::Empty);
        let f2_scan = create_scan_plan("f2", "FOLLOWS");
        let p3_scan = create_scan_plan("p3", "Person");
        let p3_node = create_graph_node(p3_scan, "p3", false);

        let graph_rel = create_graph_rel(
            empty_left,
            f2_scan,
            p3_node,
            "f2",
            Direction::Outgoing,
            "p1", // left connection exists but left plan is Empty
            "p3",
        );

        let input_logical_plan = Arc::new(LogicalPlan::Projection(Projection {
            input: graph_rel,
            items: vec![ProjectionItem {
                expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("p1".to_string()),
                    column: crate::graph_catalog::expression_parser::PropertyValue::Column("name".to_string()),
                }),
                col_alias: None,
            }],
            kind: crate::query_planner::logical_plan::ProjectionKind::Return,
            distinct: false,
        }));

        let result = analyzer
            .analyze_with_graph_schema(input_logical_plan, &mut plan_ctx, &graph_schema)
            .unwrap();

        // Should create only relationship join with both node connections
        match result {
            Transformed::Yes(plan) => {
                match plan.as_ref() {
                    LogicalPlan::GraphJoins(graph_joins) => {
                        // Assert GraphJoins structure
                        assert_eq!(graph_joins.joins.len(), 1); // Only relationship join
                        assert!(matches!(
                            graph_joins.input.as_ref(),
                            LogicalPlan::Projection(_)
                        ));

                        let rel_join = &graph_joins.joins[0];
                        assert_eq!(rel_join.table_name, "default.FOLLOWS"); // CTE name includes database prefix
                        assert_eq!(rel_join.table_alias, "f2");
                        assert_eq!(rel_join.join_type, JoinType::Inner);
                        // Should have 2 join conditions for standalone rel
                        assert_eq!(rel_join.joining_on.len(), 2);

                        // Assert the first joining condition (connection to left node)
                        let first_join_condition = &rel_join.joining_on[0];
                        assert_eq!(first_join_condition.operator, Operator::Equal);
                        assert_eq!(first_join_condition.operands.len(), 2);

                        match (
                            &first_join_condition.operands[0],
                            &first_join_condition.operands[1],
                        ) {
                            (
                                LogicalExpr::PropertyAccessExp(rel_prop),
                                LogicalExpr::PropertyAccessExp(left_prop),
                            ) => {
                                assert_eq!(rel_prop.table_alias.0, "f2");
                                // For outgoing relationship (p1)-[:FOLLOWS]->(p3),
                                // p1 is the source (left_connection), so it connects to from_id
                                assert_eq!(rel_prop.column.raw(), "from_id");
                                assert_eq!(left_prop.table_alias.0, "p1");
                                assert_eq!(left_prop.column.raw(), "id");
                            }
                            _ => panic!("Expected PropertyAccessExp operands"),
                        }

                        // Assert the second joining condition (connection to right node - standalone relationship)
                        let second_join_condition = &rel_join.joining_on[1];
                        assert_eq!(second_join_condition.operator, Operator::Equal);
                        assert_eq!(second_join_condition.operands.len(), 2);

                        match (
                            &second_join_condition.operands[0],
                            &second_join_condition.operands[1],
                        ) {
                            (
                                LogicalExpr::PropertyAccessExp(rel_prop),
                                LogicalExpr::PropertyAccessExp(right_prop),
                            ) => {
                                assert_eq!(rel_prop.table_alias.0, "f2");
                                // For outgoing relationship (p1)-[:FOLLOWS]->(p3),
                                // p3 is the target (right_connection), so it connects to to_id
                                assert_eq!(rel_prop.column.raw(), "to_id");
                                assert_eq!(right_prop.table_alias.0, "p3");
                                assert_eq!(right_prop.column.raw(), "id");
                            }
                            _ => panic!("Expected PropertyAccessExp operands"),
                        }
                    }
                    _ => panic!("Expected GraphJoins node"),
                }
            }
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_incoming_direction_edge_list() {
        let analyzer = GraphJoinInference::new();
        let graph_schema = create_test_graph_schema();
        let mut plan_ctx = setup_plan_ctx_with_graph_entities();

        // Update relationship label for incoming direction
        // plan_ctx.get_mut_table_ctx("f1").unwrap().set_labels(Some(vec!["FOLLOWS_incoming"]));
        plan_ctx.get_mut_table_ctx("f1").unwrap();

        // Create plan: (p1)<-[f1:FOLLOWS]-(p2)
        let p1_scan = create_scan_plan("p1", "Person");
        let p1_node = create_graph_node(p1_scan, "p1", false);

        let f1_scan = create_scan_plan("f1", "FOLLOWS");

        let p2_scan = create_scan_plan("p2", "Person");
        let p2_node = create_graph_node(p2_scan, "p2", false);

        let graph_rel = create_graph_rel(
            p2_node,
            f1_scan,
            p1_node,
            "f1",
            Direction::Incoming,
            "p2",
            "p1",
        );
        let input_logical_plan = Arc::new(LogicalPlan::Projection(Projection {
            input: graph_rel,
            items: vec![ProjectionItem {
                expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("p1".to_string()),
                    column: crate::graph_catalog::expression_parser::PropertyValue::Column("name".to_string()),
                }),
                col_alias: None,
            }],
            kind: crate::query_planner::logical_plan::ProjectionKind::Return,
            distinct: false,
        }));

        let result = analyzer
            .analyze_with_graph_schema(input_logical_plan, &mut plan_ctx, &graph_schema)
            .unwrap();

        // Should create appropriate joins for incoming direction
        match result {
            Transformed::Yes(plan) => {
                match plan.as_ref() {
                    LogicalPlan::GraphJoins(graph_joins) => {
                        // Assert GraphJoins structure
                        // Anchor node (p2) goes to FROM clause, not JOIN
                        // Pattern: (p1)<-[f1:FOLLOWS]-(p2) creates 2 joins: f1, p1
                        // p2 is in anchor_table, not in joins list
                        assert_eq!(graph_joins.joins.len(), 2);
                        assert!(matches!(
                            graph_joins.input.as_ref(),
                            LogicalPlan::Projection(_)
                        ));
                        assert_eq!(graph_joins.anchor_table, Some("p2".to_string()));

                        // First join: relationship (f1)
                        let rel_join = &graph_joins.joins[0];
                        assert_eq!(rel_join.table_name, "default.FOLLOWS");
                        assert_eq!(rel_join.table_alias, "f1");
                        assert_eq!(rel_join.join_type, JoinType::Inner);
                        assert_eq!(rel_join.joining_on.len(), 1);

                        // Assert the joining condition for relationship (incoming direction)
                        let rel_join_condition = &rel_join.joining_on[0];
                        assert_eq!(rel_join_condition.operator, Operator::Equal);
                        assert_eq!(rel_join_condition.operands.len(), 2);

                        // For incoming direction, the relationship connects differently
                        // Pattern (p2)<-[f1]-(p1) means p1 FOLLOWS p2, so:
                        // - f1.from_id = p1.id (source)
                        // - f1.to_id = p2.id (target) ← p2 is the anchor, connects via to_id
                        match (
                            &rel_join_condition.operands[0],
                            &rel_join_condition.operands[1],
                        ) {
                            (
                                LogicalExpr::PropertyAccessExp(rel_prop),
                                LogicalExpr::PropertyAccessExp(right_prop),
                            ) => {
                                assert_eq!(rel_prop.table_alias.0, "f1");
                                assert_eq!(rel_prop.column.raw(), "to_id");  // p2 is target, connects via to_id
                                assert_eq!(right_prop.table_alias.0, "p2");
                                assert_eq!(right_prop.column.raw(), "id");
                            }
                            _ => panic!("Expected PropertyAccessExp operands"),
                        }

                        // Second join: right node (p1)
                        let p1_join = &graph_joins.joins[1];
                        assert_eq!(p1_join.table_name, "default.Person");
                        assert_eq!(p1_join.table_alias, "p1");
                        assert_eq!(p1_join.join_type, JoinType::Inner);
                        assert_eq!(p1_join.joining_on.len(), 1);

                        let p1_join_condition = &p1_join.joining_on[0];
                        assert_eq!(p1_join_condition.operator, Operator::Equal);
                        match (
                            &p1_join_condition.operands[0],
                            &p1_join_condition.operands[1],
                        ) {
                            (
                                LogicalExpr::PropertyAccessExp(p1_prop),
                                LogicalExpr::PropertyAccessExp(rel_prop),
                            ) => {
                                assert_eq!(p1_prop.table_alias.0, "p1");
                                assert_eq!(p1_prop.column.raw(), "id");
                                assert_eq!(rel_prop.table_alias.0, "f1");
                                assert_eq!(rel_prop.column.raw(), "from_id");  // p1 is source, connects via from_id
                            }
                            _ => panic!("Expected PropertyAccessExp operands for p1 join"),
                        }
                    }
                    _ => panic!("Expected GraphJoins node"),
                }
            }
            _ => panic!("Expected transformation"),
        }
    }

    #[test]
    fn test_complex_nested_plan_with_multiple_graph_rels() {
        let analyzer = GraphJoinInference::new();
        let graph_schema = create_test_graph_schema();
        let mut plan_ctx = setup_plan_ctx_with_graph_entities();

        // Set relationships to use edge list
        plan_ctx.get_mut_table_ctx("f1").unwrap();
        plan_ctx.get_mut_table_ctx("w1").unwrap();

        // Create complex plan: (p1)-[f1:FOLLOWS]->(p2)-[w1:WORKS_AT]->(c1)
        let p1_scan = create_scan_plan("p1", "Person");
        let p1_node = create_graph_node(p1_scan, "p1", false);

        let f1_scan = create_scan_plan("f1", "FOLLOWS");

        let p2_scan = create_scan_plan("p2", "Person");
        let p2_node = create_graph_node(p2_scan, "p2", false);

        let first_rel = create_graph_rel(
            p2_node,
            f1_scan,
            p1_node,
            "f1",
            Direction::Outgoing,
            "p2",
            "p1",
        );

        let w1_scan = create_scan_plan("w1", "WORKS_AT");

        let c1_scan = create_scan_plan("c1", "Company");
        let c1_node = create_graph_node(c1_scan, "c1", false);

        // (p1)-[f1:FOLLOWS]->(p2)-[w1:WORKS_AT]->(c1)

        let second_rel = create_graph_rel(
            c1_node,
            w1_scan,
            first_rel,
            "w1",
            Direction::Outgoing,
            "c1",
            "p2",
        );

        let input_logical_plan = Arc::new(LogicalPlan::Projection(Projection {
            input: second_rel,
            items: vec![ProjectionItem {
                expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias("p1".to_string()),
                    column: crate::graph_catalog::expression_parser::PropertyValue::Column("name".to_string()),
                }),
                col_alias: None,
            }],
            kind: crate::query_planner::logical_plan::ProjectionKind::Return,
            distinct: false,
        }));

        let result = analyzer
            .analyze_with_graph_schema(input_logical_plan, &mut plan_ctx, &graph_schema)
            .unwrap();

        // (p1)-[f1:FOLLOWS]->(p2)-[w1:WORKS_AT]->(c1)
        // In this case, c1 is the ending node, we are now joining in reverse order.
        // It means first we will join c1 -> w1, w1 -> p2, p2 -> f1, f1 -> p1.
        // So the tables in the order of joining will be w1, p2, f1, p1.
        // Note that c1 is not a part of the join, it is just the ending node.

        // Should create joins for all relationships in the chain
        match result {
            Transformed::Yes(plan) => {
                match plan.as_ref() {
                    LogicalPlan::GraphJoins(graph_joins) => {
                        // Assert GraphJoins structure
                        assert!(graph_joins.joins.len() >= 2);
                        assert!(matches!(
                            graph_joins.input.as_ref(),
                            LogicalPlan::Projection(_)
                        ));

                        // Verify we have joins for both relationship aliases
                        let rel_aliases: Vec<&String> =
                            graph_joins.joins.iter().map(|j| &j.table_alias).collect();

                        // Should contain joins for both relationships
                        assert!(
                            rel_aliases
                                .iter()
                                .any(|&alias| alias == "f1" || alias == "w1")
                        );

                        // Should have joins for both relationships in the chain: (p1)-[f1:FOLLOWS]->(p2)-[w1:WORKS_AT]->(c1)
                        // Plus the referenced node (p1) and intermediate node (p2)
                        println!("Actual joins len: {}", graph_joins.joins.len());
                        let join_aliases: Vec<&String> =
                            graph_joins.joins.iter().map(|j| &j.table_alias).collect();
                        println!("Join aliases: {:?}", join_aliases);
                        assert!(graph_joins.joins.len() == 4); // 2 relationship joins + 2 nodes (p1 referenced, p2 intermediate)

                        // Verify we have the expected join aliases for the new structure: (p1)-[f1:FOLLOWS]->(p2)-[w1:WORKS_AT]->(c1)
                        let join_aliases: Vec<&String> =
                            graph_joins.joins.iter().map(|j| &j.table_alias).collect();

                        println!("Join aliases found: {:?}", join_aliases);
                        assert!(join_aliases.contains(&&"w1".to_string()));
                        assert!(join_aliases.contains(&&"f1".to_string()));
                        assert!(join_aliases.contains(&&"p1".to_string())); // p1 is referenced in RETURN
                        assert!(join_aliases.contains(&&"p2".to_string())); // p2 is intermediate node

                        // Verify each join has the correct structure
                        for join in &graph_joins.joins {
                            assert_eq!(join.join_type, JoinType::Inner);
                            assert!(!join.joining_on.is_empty());

                            // (p1)-[f1:FOLLOWS]->(p2)-[w1:WORKS_AT]->(c1)
                            // Join order = c1 -> w1, w1 -> p2, p2 -> f1, f1 -> p1.
                            // Verify specific join details based on alias
                            match join.table_alias.as_str() {
                                "w1" => {
                                    assert_eq!(join.table_name, "default.WORKS_AT"); // CTE name includes database prefix
                                    assert_eq!(join.joining_on.len(), 1);

                                    let join_condition = &join.joining_on[0];
                                    assert_eq!(join_condition.operator, Operator::Equal);
                                    assert_eq!(join_condition.operands.len(), 2);

                                    println!("Join condition: {:?}", join_condition);

                                    // Verify the join condition connects w1 with c1
                                    // For (c1)-[w1:WORKS_AT]->(p2) with Direction::Outgoing,
                                    // c1 is the source, so it connects to from_id
                                    match (&join_condition.operands[0], &join_condition.operands[1])
                                    {
                                        (
                                            LogicalExpr::PropertyAccessExp(rel_prop),
                                            LogicalExpr::PropertyAccessExp(left_prop),
                                        ) => {
                                            assert_eq!(rel_prop.table_alias.0, "w1");
                                            assert_eq!(rel_prop.column.raw(), "from_id");
                                            assert_eq!(left_prop.table_alias.0, "c1");
                                            assert_eq!(left_prop.column.raw(), "id");
                                        }
                                        _ => panic!(
                                            "Expected PropertyAccessExp operands for w1 join"
                                        ),
                                    }
                                }
                                "p2" => {
                                    // Table name includes database prefix in test context
                                    assert!(
                                        join.table_name == "Person"
                                            || join.table_name == "default.Person"
                                    );
                                    assert_eq!(join.joining_on.len(), 1);

                                    let join_condition = &join.joining_on[0];
                                    assert_eq!(join_condition.operator, Operator::Equal);
                                    assert_eq!(join_condition.operands.len(), 2);

                                    // Verify the join condition connects p2 with w1
                                    // For (c1)-[w1:WORKS_AT]->(p2) with Direction::Outgoing,
                                    // p2 is the target, so it connects to to_id
                                    match (&join_condition.operands[0], &join_condition.operands[1])
                                    {
                                        (
                                            LogicalExpr::PropertyAccessExp(left_prop),
                                            LogicalExpr::PropertyAccessExp(rel_prop),
                                        ) => {
                                            assert_eq!(left_prop.table_alias.0, "p2");
                                            assert_eq!(left_prop.column.raw(), "id");
                                            assert_eq!(rel_prop.table_alias.0, "w1");
                                            assert_eq!(rel_prop.column.raw(), "to_id");
                                        }
                                        _ => panic!(
                                            "Expected PropertyAccessExp operands for p2 join"
                                        ),
                                    }
                                }
                                "f1" => {
                                    assert_eq!(join.table_name, "default.FOLLOWS"); // CTE name includes database prefix
                                    assert_eq!(join.joining_on.len(), 1);

                                    let join_condition = &join.joining_on[0];
                                    assert_eq!(join_condition.operator, Operator::Equal);
                                    assert_eq!(join_condition.operands.len(), 2);

                                    // Verify the join condition connects f1 with p2
                                    match (&join_condition.operands[0], &join_condition.operands[1])
                                    {
                                        (
                                            LogicalExpr::PropertyAccessExp(rel_prop),
                                            LogicalExpr::PropertyAccessExp(left_prop),
                                        ) => {
                                            assert_eq!(rel_prop.table_alias.0, "f1");
                                            // For (p2)-[f1:FOLLOWS]->(p1) with Direction::Outgoing,
                                            // p2 is the source, so it connects to from_id
                                            assert_eq!(rel_prop.column.raw(), "from_id");
                                            assert_eq!(left_prop.table_alias.0, "p2");
                                            assert_eq!(left_prop.column.raw(), "id");
                                        }
                                        _ => panic!(
                                            "Expected PropertyAccessExp operands for f1 join"
                                        ),
                                    }
                                }
                                "p1" => {
                                    assert_eq!(join.table_name, "default.Person"); // Table name includes database prefix
                                    assert_eq!(join.joining_on.len(), 1);

                                    let join_condition = &join.joining_on[0];
                                    assert_eq!(join_condition.operator, Operator::Equal);
                                    assert_eq!(join_condition.operands.len(), 2);

                                    // Verify the join condition connects p1 with f1
                                    match (&join_condition.operands[0], &join_condition.operands[1])
                                    {
                                        (
                                            LogicalExpr::PropertyAccessExp(left_prop),
                                            LogicalExpr::PropertyAccessExp(rel_prop),
                                        ) => {
                                            assert_eq!(left_prop.table_alias.0, "p1");
                                            assert_eq!(left_prop.column.raw(), "id");
                                            assert_eq!(rel_prop.table_alias.0, "f1");
                                            // For (p2)-[f1:FOLLOWS]->(p1) with Direction::Outgoing,
                                            // p1 is the target, so it connects to to_id
                                            assert_eq!(rel_prop.column.raw(), "to_id");
                                        }
                                        _ => panic!(
                                            "Expected PropertyAccessExp operands for p1 join"
                                        ),
                                    }
                                }
                                _ => {
                                    // Allow other joins but ensure they have basic structure
                                    assert!(!join.table_name.is_empty());
                                    for join_condition in &join.joining_on {
                                        assert_eq!(join_condition.operator, Operator::Equal);
                                        assert_eq!(join_condition.operands.len(), 2);
                                    }
                                }
                            }
                        }
                    }
                    _ => panic!("Expected GraphJoins node"),
                }
            }
            _ => panic!("Expected transformation"),
        }
    }
}



