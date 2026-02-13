//! Helper utilities for Graph Join Inference
//!
//! This module contains utility functions extracted from the legacy GraphJoinInference
//! implementation. These are stateless helper functions that can be used throughout
//! the graph join inference process.

use std::sync::Arc;

use crate::query_planner::{
    logical_expr::{LogicalExpr, OperatorApplication, TableAlias},
    logical_plan::{Join, JoinType, LogicalPlan},
    plan_ctx::PlanCtx,
};

use crate::graph_catalog::graph_schema::GraphSchema;

// =============================================================================
// Join Type Helpers
// =============================================================================

/// Determines the appropriate join type based on whether the table alias
/// is part of an OPTIONAL MATCH pattern. Returns LEFT for optional aliases,
/// INNER for regular aliases.
pub fn determine_join_type(is_optional: bool) -> JoinType {
    if is_optional {
        JoinType::Left
    } else {
        JoinType::Inner
    }
}

// =============================================================================
// Column Resolution
// =============================================================================

/// Resolve a schema column name to the actual column name in the target table/CTE
///
/// For base tables, returns the schema column unchanged.
/// For CTE references, looks up the exported column name.
///
/// # Arguments
/// * `schema_column` - The column name from schema (e.g., "firstName")
/// * `table_name` - The table or CTE name (e.g., "with_p_cte_1" or "ldbc.Person")
/// * `plan_ctx` - The planning context with CTE column mappings
///
/// # Returns
/// The resolved column name (e.g., "p_firstName" for CTE, "firstName" for base table)
pub fn resolve_column(schema_column: &str, table_name: &str, plan_ctx: &PlanCtx) -> String {
    // Check if this is a CTE reference
    if plan_ctx.is_cte(table_name) {
        // Look up the exported column name from registered mappings
        if let Some(cte_column) = plan_ctx.get_cte_column(table_name, schema_column) {
            log::debug!(
                "  ✅ Resolved CTE column: {} (schema) → {} (CTE '{}')",
                schema_column,
                cte_column,
                table_name
            );
            return cte_column.to_string();
        }
    }

    // Base table or unmapped - use schema column as-is
    schema_column.to_string()
}

/// Resolve an Identifier's columns through CTE mappings.
/// Returns a new Identifier with resolved column names.
pub fn resolve_identifier(
    id: &crate::graph_catalog::config::Identifier,
    table_name: &str,
    plan_ctx: &PlanCtx,
) -> crate::graph_catalog::config::Identifier {
    use crate::graph_catalog::config::Identifier;
    match id {
        Identifier::Single(col) => Identifier::Single(resolve_column(col, table_name, plan_ctx)),
        Identifier::Composite(cols) => Identifier::Composite(
            cols.iter()
                .map(|c| resolve_column(c, table_name, plan_ctx))
                .collect(),
        ),
    }
}

// =============================================================================
// Join Deduplication
// =============================================================================

/// Deduplicate joins by table_alias
///
/// When there are multiple joins for the same alias, prefer the one that:
/// 1. References TableAlias (WITH clause alias like client_ip) over PropertyAccessExp (like src2.ip)
/// 2. Has fewer PropertyAccessExp operands (simpler join condition)
///
/// This handles the case where both infer_graph_join and cross-table extraction create joins
/// for the same fully denormalized table.
pub fn deduplicate_joins(joins: Vec<Join>) -> Vec<Join> {
    use std::collections::HashMap;
    // Use (alias, join_condition) as key to allow multiple joins to same table with different conditions
    let mut seen_joins: HashMap<(String, String), Join> = HashMap::new();

    for join in joins {
        let alias = join.table_alias.clone();

        // Create a stable key from the join condition
        let join_condition_key = format!("{:?}", join.joining_on);
        let key = (alias.clone(), join_condition_key);

        if let Some(existing) = seen_joins.get(&key) {
            // Compare joins - prefer one with TableAlias in joining_on (cross-table join)
            let new_has_table_alias = join_references_table_alias(&join);
            let existing_has_table_alias = join_references_table_alias(existing);

            log::debug!(
                "🔄 deduplicate_joins: key='{:?}' has duplicate. new_has_table_alias={}, existing_has_table_alias={}",
                key, new_has_table_alias, existing_has_table_alias
            );

            if new_has_table_alias && !existing_has_table_alias {
                // Prefer the new join (it references WITH aliases)
                log::debug!("🔄 deduplicate_joins: replacing with new join (has TableAlias)");
                seen_joins.insert(key, join);
            }
            // Otherwise keep existing
        } else {
            seen_joins.insert(key, join);
        }
    }

    seen_joins.into_values().collect()
}

/// Check if a join's joining_on condition references a TableAlias (WITH clause alias)
pub fn join_references_table_alias(join: &Join) -> bool {
    for condition in &join.joining_on {
        if operator_application_references_table_alias(condition) {
            return true;
        }
    }
    false
}

/// Check if an OperatorApplication contains a TableAlias reference
pub fn operator_application_references_table_alias(op_app: &OperatorApplication) -> bool {
    for operand in &op_app.operands {
        if matches!(operand, LogicalExpr::TableAlias(_)) {
            return true;
        }
        if let LogicalExpr::OperatorApplicationExp(nested) = operand {
            if operator_application_references_table_alias(nested) {
                return true;
            }
        }
    }
    false
}

// =============================================================================
// Plan Traversal Helpers
// =============================================================================

/// Extract the right-side anchor table info from a plan
/// For fully denormalized patterns, this finds the edge table that serves as the anchor
/// Returns (table_name, alias) for the right-side table
pub fn extract_right_table_from_plan(
    plan: &Arc<LogicalPlan>,
    _graph_schema: &GraphSchema,
) -> Option<(String, String)> {
    match plan.as_ref() {
        LogicalPlan::GraphRel(rel) => {
            // For GraphRel, the center ViewScan contains the edge table
            if let LogicalPlan::ViewScan(scan) = rel.center.as_ref() {
                // For denormalized schemas, use the relationship alias since that's what
                // property mappings resolve to. The relationship alias is what the SELECT
                // clause will use for property references on nodes that belong to this edge.
                // This ensures consistency between JOIN alias and SELECT column aliases.
                return Some((scan.source_table.clone(), rel.alias.clone()));
            }
            None
        }
        LogicalPlan::Projection(proj) => extract_right_table_from_plan(&proj.input, _graph_schema),
        LogicalPlan::Filter(filter) => extract_right_table_from_plan(&filter.input, _graph_schema),
        LogicalPlan::GraphNode(node) => extract_right_table_from_plan(&node.input, _graph_schema),
        _ => None,
    }
}

/// Collect all node aliases from a plan (left_connection, right_connection from GraphRel)
pub fn collect_node_aliases_from_plan(plan: &Arc<LogicalPlan>) -> Vec<String> {
    let mut aliases = Vec::new();
    collect_node_aliases_recursive(plan, &mut aliases);
    aliases
}

fn collect_node_aliases_recursive(plan: &Arc<LogicalPlan>, aliases: &mut Vec<String>) {
    match plan.as_ref() {
        LogicalPlan::GraphRel(rel) => {
            aliases.push(rel.left_connection.clone());
            aliases.push(rel.right_connection.clone());
            collect_node_aliases_recursive(&rel.left, aliases);
            collect_node_aliases_recursive(&rel.right, aliases);
        }
        LogicalPlan::GraphNode(node) => {
            aliases.push(node.alias.clone());
            collect_node_aliases_recursive(&node.input, aliases);
        }
        LogicalPlan::Projection(proj) => collect_node_aliases_recursive(&proj.input, aliases),
        LogicalPlan::Filter(filter) => collect_node_aliases_recursive(&filter.input, aliases),
        _ => {}
    }
}

// =============================================================================
// Alias Remapping
// =============================================================================

/// Remap node aliases in a join condition to use the relationship alias
/// For denormalized patterns where the filter references src2.column but we're aliasing as c
pub fn remap_node_aliases_to_relationship(
    op_app: OperatorApplication,
    right_plan: &Arc<LogicalPlan>,
    target_alias: &str,
) -> OperatorApplication {
    // Collect all node aliases from the right-side plan that should be remapped
    let node_aliases = collect_node_aliases_from_plan(right_plan);
    crate::debug_print!(
        "📦 remap_node_aliases: target_alias='{}', node_aliases={:?}",
        target_alias,
        node_aliases
    );

    // Remap operands
    let remapped_operands: Vec<LogicalExpr> = op_app
        .operands
        .iter()
        .map(|operand| remap_alias_in_expr(operand.clone(), &node_aliases, target_alias))
        .collect();

    OperatorApplication {
        operator: op_app.operator,
        operands: remapped_operands,
    }
}

/// Remap table aliases in an expression
pub fn remap_alias_in_expr(
    expr: LogicalExpr,
    source_aliases: &[String],
    target_alias: &str,
) -> LogicalExpr {
    match expr {
        LogicalExpr::PropertyAccessExp(mut prop_acc) => {
            if source_aliases.contains(&prop_acc.table_alias.0) {
                crate::debug_print!(
                    "📦 remap_alias_in_expr: remapping '{}' -> '{}'",
                    prop_acc.table_alias.0,
                    target_alias
                );
                prop_acc.table_alias = TableAlias(target_alias.to_string());
            }
            LogicalExpr::PropertyAccessExp(prop_acc)
        }
        LogicalExpr::OperatorApplicationExp(op_app) => {
            let remapped_operands: Vec<LogicalExpr> = op_app
                .operands
                .into_iter()
                .map(|operand| remap_alias_in_expr(operand, source_aliases, target_alias))
                .collect();
            LogicalExpr::OperatorApplicationExp(OperatorApplication {
                operator: op_app.operator,
                operands: remapped_operands,
            })
        }
        LogicalExpr::ScalarFnCall(mut fn_call) => {
            fn_call.args = fn_call
                .args
                .into_iter()
                .map(|arg| remap_alias_in_expr(arg, source_aliases, target_alias))
                .collect();
            LogicalExpr::ScalarFnCall(fn_call)
        }
        // Other expression types pass through unchanged
        other => other,
    }
}

// =============================================================================
// Join Utilities
// =============================================================================

/// Push a join to the collection if it's not a duplicate.
/// Duplicates are detected by comparing table_alias (which must be unique).
pub fn push_join_if_not_duplicate(collected_graph_joins: &mut Vec<Join>, new_join: Join) {
    // Check if this alias already exists
    if collected_graph_joins
        .iter()
        .any(|j| j.table_alias == new_join.table_alias)
    {
        log::debug!(
            "   ⏭️  Skipping duplicate JOIN: {} AS {} (already in collection)",
            new_join.table_name,
            new_join.table_alias
        );
        return;
    }

    log::debug!(
        "   ✅ Adding JOIN: {} AS {}",
        new_join.table_name,
        new_join.table_alias
    );
    collected_graph_joins.push(new_join);
}

/// Find GraphRel in a logical plan (helper for CartesianProduct shared node processing)
pub fn find_graph_rel_in_plan(
    plan: &LogicalPlan,
) -> Option<&crate::query_planner::logical_plan::GraphRel> {
    match plan {
        LogicalPlan::GraphRel(gr) => Some(gr),
        LogicalPlan::Projection(p) => find_graph_rel_in_plan(p.input.as_ref()),
        LogicalPlan::Filter(f) => find_graph_rel_in_plan(f.input.as_ref()),
        _ => None,
    }
}

// =============================================================================
// Table Name Helpers
// =============================================================================

/// Get table name with database prefix
pub fn get_table_name_with_prefix(database: &str, table_name: &str) -> String {
    if database.is_empty() {
        table_name.to_string()
    } else {
        format!("{}.{}", database, table_name)
    }
}

/// Get relationship table name with database prefix
pub fn get_rel_table_name_with_prefix(
    rel_schema: &crate::graph_catalog::graph_schema::RelationshipSchema,
) -> String {
    get_table_name_with_prefix(&rel_schema.database, &rel_schema.table_name)
}

// =============================================================================
// Expression Builders - Re-exported from combinators for convenience
// =============================================================================

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::query_planner::logical_expr::{Operator, PropertyAccess};

// Re-export common expression builders from the central combinators module
pub use crate::query_planner::logical_expr::combinators::{col_eq, prop};

/// Alias for col_eq - Create an equality condition: left_alias.left_col = right_alias.right_col
///
/// This is the local name used in graph_join module. Delegates to combinators::col_eq.
#[inline]
pub fn eq_condition(
    left_alias: impl Into<String>,
    left_col: impl Into<String>,
    right_alias: impl Into<String>,
    right_col: impl Into<String>,
) -> OperatorApplication {
    col_eq(left_alias, left_col, right_alias, right_col)
}

/// Alias for prop - Create a PropertyAccess expression: alias.column
///
/// This is the local name used in graph_join module. Delegates to combinators::prop.
#[inline]
pub fn prop_access(alias: impl Into<String>, col: impl Into<String>) -> LogicalExpr {
    prop(alias, col)
}

/// Create an AND expression combining multiple conditions.
/// Uses combinators::and internally.
pub fn and_conditions(conditions: Vec<LogicalExpr>) -> LogicalExpr {
    crate::query_planner::logical_expr::combinators::and(conditions).unwrap_or_else(|| {
        LogicalExpr::Literal(crate::query_planner::logical_expr::Literal::Boolean(true))
    })
}

// =============================================================================
// JoinBuilder - Fluent Builder for Join Construction
// =============================================================================

/// Builder for constructing Join objects with less boilerplate.
///
/// # Example
/// ```ignore
/// let join = JoinBuilder::new("users", "u")
///     .join_type(JoinType::Inner)
///     .add_condition("u", "id", "r", "user_id")
///     .build();
/// ```
pub struct JoinBuilder {
    table_name: String,
    table_alias: String,
    joining_on: Vec<OperatorApplication>,
    join_type: JoinType,
    pre_filter: Option<LogicalExpr>,
    from_id_column: Option<String>,
    to_id_column: Option<String>,
}

impl JoinBuilder {
    /// Create a new JoinBuilder with table name and alias
    pub fn new(table_name: impl Into<String>, table_alias: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            table_alias: table_alias.into(),
            joining_on: Vec::new(),
            join_type: JoinType::Inner,
            pre_filter: None,
            from_id_column: None,
            to_id_column: None,
        }
    }

    /// Create a FROM marker join (no conditions, serves as anchor)
    pub fn from_marker(table_name: impl Into<String>, table_alias: impl Into<String>) -> Self {
        Self::new(table_name, table_alias)
    }

    /// Set the join type
    pub fn join_type(mut self, jt: JoinType) -> Self {
        self.join_type = jt;
        self
    }

    /// Set join type based on optionality flag
    pub fn optional(mut self, is_optional: bool) -> Self {
        self.join_type = determine_join_type(is_optional);
        self
    }

    /// Add an equality condition: left_alias.left_col = right_alias.right_col
    pub fn add_condition(
        mut self,
        left_alias: impl Into<String>,
        left_col: impl Into<String>,
        right_alias: impl Into<String>,
        right_col: impl Into<String>,
    ) -> Self {
        self.joining_on.push(OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(left_alias.into()),
                    column: PropertyValue::Column(left_col.into()),
                }),
                LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(right_alias.into()),
                    column: PropertyValue::Column(right_col.into()),
                }),
            ],
        });
        self
    }

    /// Add a raw OperatorApplication condition
    pub fn add_raw_condition(mut self, condition: OperatorApplication) -> Self {
        self.joining_on.push(condition);
        self
    }

    /// Add equality condition(s) for Identifier (handles single and composite).
    /// For single: adds one condition. For composite: adds one condition per column pair.
    pub fn add_identifier_condition(
        mut self,
        left_alias: &str,
        left_id: &crate::graph_catalog::config::Identifier,
        right_alias: &str,
        right_id: &crate::graph_catalog::config::Identifier,
    ) -> Self {
        let left_cols = left_id.columns();
        let right_cols = right_id.columns();
        assert_eq!(
            left_cols.len(),
            right_cols.len(),
            "Identifier column count mismatch in JOIN: {} vs {}",
            left_cols.len(),
            right_cols.len()
        );
        for (l, r) in left_cols.iter().zip(right_cols.iter()) {
            self.joining_on.push(OperatorApplication {
                operator: Operator::Equal,
                operands: vec![
                    LogicalExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(left_alias.to_string()),
                        column: PropertyValue::Column(l.to_string()),
                    }),
                    LogicalExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(right_alias.to_string()),
                        column: PropertyValue::Column(r.to_string()),
                    }),
                ],
            });
        }
        self
    }

    /// Set pre-filter expression
    pub fn pre_filter(mut self, filter: Option<LogicalExpr>) -> Self {
        self.pre_filter = filter;
        self
    }

    /// Set from_id column (for relationship joins)
    pub fn from_id(mut self, col: impl Into<String>) -> Self {
        self.from_id_column = Some(col.into());
        self
    }

    /// Set to_id column (for relationship joins)
    pub fn to_id(mut self, col: impl Into<String>) -> Self {
        self.to_id_column = Some(col.into());
        self
    }

    /// Build the final Join object
    pub fn build(self) -> Join {
        Join {
            table_name: self.table_name,
            table_alias: self.table_alias,
            joining_on: self.joining_on,
            join_type: self.join_type,
            pre_filter: self.pre_filter,
            from_id_column: self.from_id_column,
            to_id_column: self.to_id_column,
            graph_rel: None,
        }
    }

    /// Build and push to collection if not duplicate
    pub fn build_and_push(self, collected_joins: &mut Vec<Join>) {
        push_join_if_not_duplicate(collected_joins, self.build());
    }

    /// Build and insert at specific position
    pub fn build_and_insert_at(self, collected_joins: &mut Vec<Join>, index: usize) {
        collected_joins.insert(index, self.build());
    }
}
