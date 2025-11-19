use std::{collections::HashSet, sync::Arc};

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    query_planner::{
        analyzer::{
            analyzer_pass::{AnalyzerPass, AnalyzerResult},
            errors::Pass,
            graph_context::{self, GraphContext},
        },
        logical_expr::{
            Column, LogicalExpr, Operator, OperatorApplication, PropertyAccess, TableAlias,
        },
        logical_plan::{GraphJoins, GraphRel, Join, JoinType, LogicalPlan},
        plan_ctx::PlanCtx,
        transformed::Transformed,
    },
    render_plan::cte_extraction::extract_relationship_columns,
};

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

        if !collected_graph_joins.is_empty() {
            // Get optional_aliases from plan_ctx to pass to GraphJoins
            let optional_aliases = plan_ctx.get_optional_aliases().clone();
            Self::build_graph_joins(logical_plan, &mut collected_graph_joins, optional_aliases)
        } else {
            println!("DEBUG GraphJoinInference: No joins collected, returning original plan");
            Ok(Transformed::No(logical_plan.clone()))
        }
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
    ) -> Vec<Join> {
        if joins.is_empty() {
            return joins;
        }

        eprintln!("\n?? REORDERING {} JOINS by dependencies", joins.len());

        // Start with tables available from FROM clause (anchor nodes - required nodes)
        let mut available_tables: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        // CRITICAL FIX: Find anchor by identifying which table is REQUIRED (not optional)
        // The anchor is the table that should go in FROM clause
        // It's the table alias that is NOT in optional_aliases

        // Strategy 1: Check joins list for required tables
        for join in &joins {
            if !optional_aliases.contains(&join.table_alias) {
                // This is a required table - it's a candidate for FROM clause (anchor)
                available_tables.insert(join.table_alias.clone());
                eprintln!("  ?? Found REQUIRED table in joins: {}", join.table_alias);
            }
        }

        // Strategy 2: Find tables referenced in JOIN conditions but NOT in the joins list
        // These are the anchors that were correctly identified during JOIN inference
        let mut join_aliases: std::collections::HashSet<String> = std::collections::HashSet::new();
        for join in &joins {
            join_aliases.insert(join.table_alias.clone());
        }

        let mut referenced_tables: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        for join in &joins {
            for condition in &join.joining_on {
                for operand in &condition.operands {
                    Self::extract_table_refs_from_expr(operand, &mut referenced_tables);
                }
            }
        }

        // Tables that are referenced but not in joins list are anchors
        for table in &referenced_tables {
            if !join_aliases.contains(table) {
                available_tables.insert(table.clone());
                eprintln!(
                    "  ?? Found ANCHOR table (referenced but not joined): {}",
                    table
                );
            }
        }

        // If we found anchor tables, they will be in FROM clause
        if !available_tables.is_empty() {
            eprintln!(
                "  ?? Anchor tables (will be in FROM, not JOIN): {:?}",
                available_tables
            );
        } else {
            eprintln!("  ??  No anchor tables found - all are optional!");
        }

        let mut ordered_joins = Vec::new();
        let mut remaining_joins = joins;

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
        ordered_joins
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

    fn build_graph_joins(
        logical_plan: Arc<LogicalPlan>,
        collected_graph_joins: &mut Vec<Join>,
        optional_aliases: std::collections::HashSet<String>,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        let transformed_plan = match logical_plan.as_ref() {
            LogicalPlan::Projection(_) => {
                // Reorder JOINs before creating GraphJoins to ensure proper dependency order
                let reordered_joins = Self::reorder_joins_by_dependencies(
                    collected_graph_joins.clone(),
                    &optional_aliases,
                );

                // wrap the outer projection i.e. first occurance in the tree walk with Graph joins
                Transformed::Yes(Arc::new(LogicalPlan::GraphJoins(GraphJoins {
                    input: logical_plan.clone(),
                    joins: reordered_joins,
                    optional_aliases,
                })))
            }
            LogicalPlan::GraphNode(graph_node) => {
                let child_tf = Self::build_graph_joins(
                    graph_node.input.clone(),
                    collected_graph_joins,
                    optional_aliases.clone(),
                )?;
                graph_node.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GraphRel(graph_rel) => {
                let left_tf = Self::build_graph_joins(
                    graph_rel.left.clone(),
                    collected_graph_joins,
                    optional_aliases.clone(),
                )?;
                let center_tf = Self::build_graph_joins(
                    graph_rel.center.clone(),
                    collected_graph_joins,
                    optional_aliases.clone(),
                )?;
                let right_tf = Self::build_graph_joins(
                    graph_rel.right.clone(),
                    collected_graph_joins,
                    optional_aliases.clone(),
                )?;

                graph_rel.rebuild_or_clone(left_tf, center_tf, right_tf, logical_plan.clone())
            }
            LogicalPlan::Cte(cte) => {
                let child_tf = Self::build_graph_joins(
                    cte.input.clone(),
                    collected_graph_joins,
                    optional_aliases,
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
                )?;
                graph_joins.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Filter(filter) => {
                let child_tf = Self::build_graph_joins(
                    filter.input.clone(),
                    collected_graph_joins,
                    optional_aliases,
                )?;
                filter.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::GroupBy(group_by) => {
                let child_tf = Self::build_graph_joins(
                    group_by.input.clone(),
                    collected_graph_joins,
                    optional_aliases,
                )?;
                group_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::OrderBy(order_by) => {
                let child_tf = Self::build_graph_joins(
                    order_by.input.clone(),
                    collected_graph_joins,
                    optional_aliases,
                )?;
                order_by.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Skip(skip) => {
                let child_tf = Self::build_graph_joins(
                    skip.input.clone(),
                    collected_graph_joins,
                    optional_aliases,
                )?;
                skip.rebuild_or_clone(child_tf, logical_plan.clone())
            }
            LogicalPlan::Limit(limit) => {
                let child_tf = Self::build_graph_joins(
                    limit.input.clone(),
                    collected_graph_joins,
                    optional_aliases,
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
                    )?;
                    inputs_tf.push(child_tf);
                }
                union.rebuild_or_clone(inputs_tf, logical_plan.clone())
            }
            LogicalPlan::PageRank(_) => Transformed::No(logical_plan.clone()),
            LogicalPlan::ViewScan(_) => Transformed::No(logical_plan.clone()),
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
                eprintln!("� ? Union, recursing into {} inputs", union.inputs.len());
                for input_plan in union.inputs.iter() {
                    self.collect_graph_joins(
                        input_plan.clone(),
                        root_plan.clone(),
                        plan_ctx,
                        graph_schema,
                        collected_graph_joins,
                        joined_entities,
                    )?;
                }
                Ok(())
            }
            LogicalPlan::PageRank(_) => {
                eprintln!("� ? PageRank, nothing to collect");
                Ok(())
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

        // Skip join inference for variable-length paths
        if graph_rel.variable_length.is_some() {
            eprintln!("    � ? SKIP: Variable-length path detected");
            eprintln!("    +- infer_graph_join EXIT\n");
            return Ok(());
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

        let graph_context = graph_context::get_graph_context(
            graph_rel,
            plan_ctx,
            graph_schema,
            Pass::GraphJoinInference,
        )?;

        // Extract alias strings
        let left_alias_str = graph_context.left.alias.to_string();
        let rel_alias_str = graph_context.rel.alias.to_string();
        let right_alias_str = graph_context.right.alias.to_string();

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

        let left_node_id_column = graph_context.left.schema.node_id.column.clone(); //  left_schema.node_id.column.clone();
        let right_node_id_column = graph_context.right.schema.node_id.column.clone(); //right_schema.node_id.column.clone();

        eprintln!("    � Creating joins for relationship...");
        let joins_before = collected_graph_joins.len();

        // ClickGraph uses view-mapped graph storage where relationships are tables
        // with from_id/to_id columns. Process the graph pattern to generate JOINs.
        eprintln!("    � ? Processing graph pattern");
        let result = self.handle_graph_pattern(
            graph_rel,
            graph_context,
            left_node_id_column,
            right_node_id_column,
            is_standalone_rel,
            left_is_optional,
            rel_is_optional,
            right_is_optional,
            left_is_referenced,
            right_is_referenced,
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
        graph_context: GraphContext,
        left_node_id_column: String,
        right_node_id_column: String,
        is_standalone_rel: bool,
        left_is_optional: bool,
        rel_is_optional: bool,
        right_is_optional: bool,
        left_is_referenced: bool,
        right_is_referenced: bool,
        collected_graph_joins: &mut Vec<Join>,
        joined_entities: &mut HashSet<String>,
    ) -> AnalyzerResult<()> {
        let left_alias = graph_context.left.alias;
        let rel_alias = graph_context.rel.alias;
        let right_alias = graph_context.right.alias;

        let left_cte_name = graph_context.left.cte_name;
        let rel_cte_name = graph_context.rel.cte_name;
        let right_cte_name = graph_context.right.cte_name;

        // Extract relationship column names from the ViewScan
        let rel_cols = extract_relationship_columns(&graph_rel.center).unwrap_or(
            crate::render_plan::cte_extraction::RelationshipColumns {
                from_id: "from_node_id".to_string(),
                to_id: "to_node_id".to_string(),
            },
        );
        let rel_from_col = rel_cols.from_id;
        let rel_to_col = rel_cols.to_id;

        eprintln!(
            "    � ?? DEBUG REL COLUMNS: rel_from_col = '{}', rel_to_col = '{}'",
            rel_from_col, rel_to_col
        );

        // If both nodes are of the same type then check the direction to determine where are the left and right nodes present in the edgelist.
        if graph_context.left.schema.table_name == graph_context.right.schema.table_name {
            eprintln!(
                "    � ?? SAME-TYPE NODES PATH (left={}, right={})",
                graph_context.left.schema.table_name, graph_context.right.schema.table_name
            );
            if joined_entities.contains(right_alias) {
                eprintln!("    � ?? Branch: RIGHT already joined");
                // join the rel with right first and then join the left with rel
                // Since GraphRel structure is already adjusted for direction,
                // we don't need direction-based logic here
                let rel_conn_with_right_node = rel_to_col.clone();
                let left_conn_with_rel = rel_from_col.clone();
                let mut rel_graph_join = Join {
                    table_name: rel_cte_name,
                    table_alias: rel_alias.to_string(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: Column(rel_conn_with_right_node),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(right_alias.to_string()),
                                column: Column(right_node_id_column.clone()),
                            }),
                        ],
                    }],
                    join_type: Self::determine_join_type(rel_is_optional),
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
                //                 column: Column(left_node_id_column.clone()),
                //             }),
                //             LogicalExpr::PropertyAccessExp(PropertyAccess {
                //                 table_alias: TableAlias(rel_alias.to_string()),
                //                 column: Column(left_conn_with_rel.clone()),
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
                                column: Column(left_conn_with_rel),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(left_alias.to_string()),
                                column: Column(left_node_id_column),
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
                let left_graph_join = Join {
                    table_name: left_cte_name.clone(),
                    table_alias: left_alias.to_string(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(left_alias.to_string()),
                                column: Column(left_node_id_column.clone()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: Column(left_conn_with_rel.clone()),
                            }),
                        ],
                    }],
                    join_type: Self::determine_join_type(left_is_optional),
                };
                collected_graph_joins.push(left_graph_join);
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
                    eprintln!("    � FALLBACK - connecting to LEFT");
                    (
                        rel_conn_with_left_node.clone(),
                        left_alias.to_string(),
                        left_node_id_column.clone(),
                    )
                };

                let mut rel_graph_join = Join {
                    table_name: rel_cte_name,
                    table_alias: rel_alias.to_string(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: Column(rel_connect_column),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(node_alias),
                                column: Column(node_id_column),
                            }),
                        ],
                    }],
                    join_type: Self::determine_join_type(rel_is_optional),
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
                //                 column: Column(right_node_id_column.clone()),
                //             }),
                //             LogicalExpr::PropertyAccessExp(PropertyAccess {
                //                 table_alias: TableAlias(rel_alias.to_string()),
                //                 column: Column(right_conn_with_rel.clone()),
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
                                column: Column(right_conn_with_rel),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(right_alias.to_string()),
                                column: Column(right_node_id_column),
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
                let reverse_join_order = !joined_entities.contains(left_alias);
                eprintln!("    � ?? DEBUG: reverse_join_order={}", reverse_join_order);
                eprintln!("    � ?? FIX: Joining LEFT regardless of is_referenced for JOIN scope");

                if reverse_join_order {
                    eprintln!(
                        "    � ?? REVERSING JOIN ORDER: Joining LEFT node '{}' BEFORE relationship",
                        left_alias
                    );
                    // Join LEFT node first
                    let left_graph_join = Join {
                        table_name: left_cte_name.clone(),
                        table_alias: left_alias.to_string(),
                        joining_on: vec![OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                LogicalExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(left_alias.to_string()),
                                    column: Column(left_node_id_column.clone()),
                                }),
                                LogicalExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(rel_alias.to_string()),
                                    column: Column(rel_from_col.clone()),
                                }),
                            ],
                        }],
                        join_type: Self::determine_join_type(left_is_optional),
                    };
                    collected_graph_joins.push(left_graph_join);
                    joined_entities.insert(left_alias.to_string());
                    eprintln!("    � ? LEFT node '{}' joined first", left_alias);
                }

                // Now push the relationship JOIN
                collected_graph_joins.push(rel_graph_join);
                joined_entities.insert(rel_alias.to_string());

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
                            let left_graph_join = Join {
                                table_name: left_cte_name.clone(),
                                table_alias: left_alias.to_string(),
                                joining_on: vec![OperatorApplication {
                                    operator: Operator::Equal,
                                    operands: vec![
                                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: TableAlias(left_alias.to_string()),
                                            column: Column(left_node_id_column.clone()),
                                        }),
                                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                                            table_alias: TableAlias(rel_alias.to_string()),
                                            column: Column(rel_from_col.clone()),
                                        }),
                                    ],
                                }],
                                join_type: Self::determine_join_type(left_is_optional),
                            };
                            collected_graph_joins.push(left_graph_join);
                            joined_entities.insert(left_alias.to_string());
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
                } else {
                    eprintln!("    � ? Creating JOIN for RIGHT '{}'", right_alias);
                    let right_graph_join = Join {
                        table_name: right_cte_name.clone(),
                        table_alias: right_alias.to_string(),
                        joining_on: vec![OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                LogicalExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(right_alias.to_string()),
                                    column: Column(right_node_id_column.clone()),
                                }),
                                LogicalExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(rel_alias.to_string()),
                                    column: Column(right_conn_with_rel.clone()),
                                }),
                            ],
                        }],
                        join_type: Self::determine_join_type(right_is_optional),
                    };
                    collected_graph_joins.push(right_graph_join);
                    joined_entities.insert(right_alias.to_string());
                }
                Ok(())
            }
        } else
        // check if right is connected with edge list's from_node
        if graph_context.rel.schema.from_node == graph_context.right.schema.table_name {
            // this means rel.from_node = right and to_node = left

            // check if right is already joined
            if joined_entities.contains(right_alias) {
                // join the rel with right first and then join the left with rel
                // NOTE: left_connection and right_connection in GraphRel are ALREADY adjusted for direction
                // in match_clause.rs lines 341-345. So we just connect:
                //   - RIGHT node to rel.to_id (the target of the relationship)
                //   - LEFT node to rel.from_id (the source of the relationship)
                // No need to check direction here - it's already encoded in left_conn/right_conn!

                let mut rel_graph_join = Join {
                    table_name: rel_cte_name,
                    table_alias: rel_alias.to_string(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: Column(rel_to_col.clone()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(right_alias.to_string()),
                                column: Column(right_node_id_column.clone()),
                            }),
                        ],
                    }],
                    join_type: Self::determine_join_type(rel_is_optional),
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
                //                 column: Column(left_node_id_column.clone()),
                //             }),
                //             LogicalExpr::PropertyAccessExp(PropertyAccess {
                //                 table_alias: TableAlias(rel_alias.to_string()),
                //                 column: Column(rel_to_col.clone()),
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
                                column: Column(rel_from_col.clone()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(left_alias.to_string()),
                                column: Column(left_node_id_column),
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
                let left_graph_join = Join {
                    table_name: left_cte_name.clone(),
                    table_alias: left_alias.to_string(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(left_alias.to_string()),
                                column: Column(left_node_id_column.clone()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: Column(rel_from_col),
                            }),
                        ],
                    }],
                    join_type: Self::determine_join_type(left_is_optional),
                };
                collected_graph_joins.push(left_graph_join);
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

                let mut rel_graph_join = Join {
                    table_name: rel_cte_name,
                    table_alias: rel_alias.to_string(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: Column(rel_from_col.clone()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(left_alias.to_string()),
                                column: Column(left_node_id_column.clone()),
                            }),
                        ],
                    }],
                    join_type: Self::determine_join_type(rel_is_optional),
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
                //                 column: Column(right_node_id_column.clone()),
                //             }),
                //             LogicalExpr::PropertyAccessExp(PropertyAccess {
                //                 table_alias: TableAlias(rel_alias.to_string()),
                //                 column: Column(rel_from_col.clone()),
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
                                column: Column(rel_to_col.clone()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(right_alias.to_string()),
                                column: Column(right_node_id_column),
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
                let right_graph_join = Join {
                    table_name: right_cte_name.clone(),
                    table_alias: right_alias.to_string(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(right_alias.to_string()),
                                column: Column(right_node_id_column.clone()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: Column(rel_to_col),
                            }),
                        ],
                    }],
                    join_type: Self::determine_join_type(right_is_optional),
                };
                collected_graph_joins.push(right_graph_join);
                joined_entities.insert(right_alias.to_string());
                Ok(())
            }
        } else {
            // this means rel.from_node = left and to_node = right

            // check if right is already joined
            if joined_entities.contains(right_alias) {
                // join the rel with right first and then join the left with rel
                let mut rel_graph_join = Join {
                    table_name: rel_cte_name,
                    table_alias: rel_alias.to_string(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: Column("to_id".to_string()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(right_alias.to_string()),
                                column: Column(right_node_id_column.clone()),
                            }),
                        ],
                    }],
                    join_type: Self::determine_join_type(rel_is_optional),
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
                //                 column: Column(left_node_id_column.clone()),
                //             }),
                //             LogicalExpr::PropertyAccessExp(PropertyAccess {
                //                 table_alias: TableAlias(rel_alias.to_string()),
                //                 column: Column(rel_from_col.clone()),
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
                                column: Column(rel_from_col.clone()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(left_alias.to_string()),
                                column: Column(left_node_id_column),
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
                    let left_graph_join = Join {
                        table_name: left_cte_name.clone(),
                        table_alias: left_alias.to_string(),
                        joining_on: vec![OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                LogicalExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(left_alias.to_string()),
                                    column: Column(left_node_id_column.clone()),
                                }),
                                LogicalExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(rel_alias.to_string()),
                                    column: Column(rel_from_col.clone()),
                                }),
                            ],
                        }],
                        join_type: Self::determine_join_type(left_is_optional),
                    };
                    collected_graph_joins.push(left_graph_join);
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
                let mut rel_graph_join = Join {
                    table_name: rel_cte_name,
                    table_alias: rel_alias.to_string(),
                    joining_on: vec![OperatorApplication {
                        operator: Operator::Equal,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(rel_alias.to_string()),
                                column: Column(rel_from_col.clone()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(left_alias.to_string()),
                                column: Column(left_node_id_column.clone()),
                            }),
                        ],
                    }],
                    join_type: Self::determine_join_type(rel_is_optional),
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
                //                 column: Column(right_node_id_column.clone()),
                //             }),
                //             LogicalExpr::PropertyAccessExp(PropertyAccess {
                //                 table_alias: TableAlias(rel_alias.to_string()),
                //                 column: Column(rel_to_col.clone()),
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
                                column: Column("to_id".to_string()),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(right_alias.to_string()),
                                column: Column(right_node_id_column),
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
                    let right_graph_join = Join {
                        table_name: right_cte_name,
                        table_alias: right_alias.to_string(),
                        joining_on: vec![OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                LogicalExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(right_alias.to_string()),
                                    column: Column(right_node_id_column.clone()),
                                }),
                                LogicalExpr::PropertyAccessExp(PropertyAccess {
                                    table_alias: TableAlias(rel_alias.to_string()),
                                    column: Column(rel_to_col.clone()),
                                }),
                            ],
                        }],
                        join_type: Self::determine_join_type(right_is_optional),
                    };
                    collected_graph_joins.push(right_graph_join);
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
            logical_expr::{Column, Direction, LogicalExpr, Operator, PropertyAccess, TableAlias},
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

    fn create_graph_node(input: Arc<LogicalPlan>, alias: &str) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::GraphNode(GraphNode {
            input,
            alias: alias.to_string(),
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
        }))
    }

    #[test]
    fn test_no_graph_joins_when_no_graph_rels() {
        let analyzer = GraphJoinInference::new();
        let graph_schema = create_test_graph_schema();
        let mut plan_ctx = setup_plan_ctx_with_graph_entities();

        // Create a plan with only a graph node (no relationships)
        let scan = create_scan_plan("p1", "person");
        let graph_node = create_graph_node(scan, "p1");

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
        let p1_node = create_graph_node(p1_scan, "p1");

        let f1_scan = create_scan_plan("f1", "FOLLOWS");

        let p2_scan = create_scan_plan("p2", "Person");
        let p2_node = create_graph_node(p2_scan, "p2");

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
                    column: Column("name".to_string()),
                }),
                col_alias: None,
            }],
            kind: crate::query_planner::logical_plan::ProjectionKind::Return,
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
                        // Multi-hop fix: Creates joins for both nodes (p2, p1) + relationship (f1)
                        // Pattern: (p2)-[f1:FOLLOWS]->(p1) creates 3 joins in order: p2, f1, p1
                        assert_eq!(graph_joins.joins.len(), 3);
                        assert!(matches!(
                            graph_joins.input.as_ref(),
                            LogicalPlan::Projection(_)
                        ));

                        // First join: left node (p2)
                        let p2_join = &graph_joins.joins[0];
                        assert_eq!(p2_join.table_name, "default.Person");
                        assert_eq!(p2_join.table_alias, "p2");

                        // Second join: relationship (f1)
                        let rel_join = &graph_joins.joins[1];
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
                                assert_eq!(rel_prop.column.0, "from_id");
                                assert_eq!(left_prop.table_alias.0, "p2");
                                assert_eq!(left_prop.column.0, "id");
                            }
                            _ => panic!("Expected PropertyAccessExp operands"),
                        }

                        // Third join: right node (p1)
                        let p1_join = &graph_joins.joins[2];
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
                                assert_eq!(p1_prop.column.0, "id");
                                assert_eq!(rel_prop.table_alias.0, "f1");
                                assert_eq!(rel_prop.column.0, "to_id");
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
        let p1_node = create_graph_node(p1_scan, "p1");

        let w1_scan = create_scan_plan("w1", "WORKS_AT");

        let c1_scan = create_scan_plan("c1", "Company");
        let c1_node = create_graph_node(c1_scan, "c1");

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
                    column: Column("name".to_string()),
                }),
                col_alias: None,
            }],
            kind: crate::query_planner::logical_plan::ProjectionKind::Return,
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
                                assert_eq!(rel_prop.column.0, "from_id");
                                assert_eq!(left_prop.table_alias.0, "p1");
                                assert_eq!(left_prop.column.0, "id");
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
        let p1_node = create_graph_node(p1_scan, "p1");

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
        let p2_node = create_graph_node(p2_scan, "p2");

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
                    column: Column("name".to_string()),
                }),
                col_alias: None,
            }],
            kind: crate::query_planner::logical_plan::ProjectionKind::Return,
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
                                assert_eq!(rel_prop.column.0, "to_id");
                                assert_eq!(right_prop.table_alias.0, "p2");
                                assert_eq!(right_prop.column.0, "id");
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
        let p3_node = create_graph_node(p3_scan, "p3");

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
                    column: Column("name".to_string()),
                }),
                col_alias: None,
            }],
            kind: crate::query_planner::logical_plan::ProjectionKind::Return,
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
                                assert_eq!(rel_prop.column.0, "from_id");
                                assert_eq!(left_prop.table_alias.0, "p1");
                                assert_eq!(left_prop.column.0, "id");
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
                                assert_eq!(rel_prop.column.0, "to_id");
                                assert_eq!(right_prop.table_alias.0, "p3");
                                assert_eq!(right_prop.column.0, "id");
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
        let p1_node = create_graph_node(p1_scan, "p1");

        let f1_scan = create_scan_plan("f1", "FOLLOWS");

        let p2_scan = create_scan_plan("p2", "Person");
        let p2_node = create_graph_node(p2_scan, "p2");

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
                    column: Column("name".to_string()),
                }),
                col_alias: None,
            }],
            kind: crate::query_planner::logical_plan::ProjectionKind::Return,
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
                        // Multi-hop fix: Creates joins for both nodes + relationship
                        // Pattern: (p1)<-[f1:FOLLOWS]-(p2) creates 3 joins in order: p2, f1, p1
                        assert_eq!(graph_joins.joins.len(), 3);
                        assert!(matches!(
                            graph_joins.input.as_ref(),
                            LogicalPlan::Projection(_)
                        ));

                        // First join: left node (p2)
                        let p2_join = &graph_joins.joins[0];
                        assert_eq!(p2_join.table_name, "default.Person");
                        assert_eq!(p2_join.table_alias, "p2");

                        // Second join: relationship (f1)
                        let rel_join = &graph_joins.joins[1];
                        assert_eq!(rel_join.table_name, "default.FOLLOWS");
                        assert_eq!(rel_join.table_alias, "f1");
                        assert_eq!(rel_join.join_type, JoinType::Inner);
                        assert_eq!(rel_join.joining_on.len(), 1);

                        // Assert the joining condition for relationship (incoming direction)
                        let rel_join_condition = &rel_join.joining_on[0];
                        assert_eq!(rel_join_condition.operator, Operator::Equal);
                        assert_eq!(rel_join_condition.operands.len(), 2);

                        // For incoming direction, the relationship connects differently
                        match (
                            &rel_join_condition.operands[0],
                            &rel_join_condition.operands[1],
                        ) {
                            (
                                LogicalExpr::PropertyAccessExp(rel_prop),
                                LogicalExpr::PropertyAccessExp(right_prop),
                            ) => {
                                assert_eq!(rel_prop.table_alias.0, "f1");
                                assert_eq!(rel_prop.column.0, "from_id");
                                assert_eq!(right_prop.table_alias.0, "p2");
                                assert_eq!(right_prop.column.0, "id");
                            }
                            _ => panic!("Expected PropertyAccessExp operands"),
                        }

                        // Third join: right node (p1)
                        let p1_join = &graph_joins.joins[2];
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
                                assert_eq!(p1_prop.column.0, "id");
                                assert_eq!(rel_prop.table_alias.0, "f1");
                                assert_eq!(rel_prop.column.0, "to_id");
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
        let p1_node = create_graph_node(p1_scan, "p1");

        let f1_scan = create_scan_plan("f1", "FOLLOWS");

        let p2_scan = create_scan_plan("p2", "Person");
        let p2_node = create_graph_node(p2_scan, "p2");

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
        let c1_node = create_graph_node(c1_scan, "c1");

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
                    column: Column("name".to_string()),
                }),
                col_alias: None,
            }],
            kind: crate::query_planner::logical_plan::ProjectionKind::Return,
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
                                            assert_eq!(rel_prop.column.0, "from_id");
                                            assert_eq!(left_prop.table_alias.0, "c1");
                                            assert_eq!(left_prop.column.0, "id");
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
                                            assert_eq!(left_prop.column.0, "id");
                                            assert_eq!(rel_prop.table_alias.0, "w1");
                                            assert_eq!(rel_prop.column.0, "to_id");
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
                                            assert_eq!(rel_prop.column.0, "from_id");
                                            assert_eq!(left_prop.table_alias.0, "p2");
                                            assert_eq!(left_prop.column.0, "id");
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
                                            assert_eq!(left_prop.column.0, "id");
                                            assert_eq!(rel_prop.table_alias.0, "f1");
                                            // For (p2)-[f1:FOLLOWS]->(p1) with Direction::Outgoing,
                                            // p1 is the target, so it connects to to_id
                                            assert_eq!(rel_prop.column.0, "to_id");
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
