//! Scope-aware variable resolution for the rendering pipeline.
//!
//! `VariableScope` is the single source of truth for resolving variable references
//! at any point during rendering. It is built iteratively — one scope advancement
//! per WITH barrier — and queried for ALL property resolution.
//!
//! Resolution order:
//! 1. CTE variables (from previous WITH exports) → CTE column names
//! 2. Table variables (from current MATCH) → DB column names via schema
//! 3. Unresolved → fall through to existing logic

use std::collections::HashMap;

use crate::graph_catalog::graph_schema::GraphSchema;
use crate::query_planner::logical_expr::expression_rewriter::{
    find_label_for_alias_in_plan, map_property_to_db_column,
};
use crate::query_planner::logical_plan::LogicalPlan;

/// Information about a variable that was exported through a CTE (WITH clause).
#[derive(Debug, Clone)]
pub struct CteVariableInfo {
    /// The CTE name this variable comes from
    pub cte_name: String,
    /// Maps Cypher property name → CTE column name
    /// Example: "name" → "p6_friend_name", "id" → "p6_friend_id"
    pub property_mapping: HashMap<String, String>,
    /// Original node/relationship labels (preserved across WITH for schema lookups)
    pub labels: Vec<String>,
    /// Override for FROM alias (used by VLP CTEs where the FROM alias is "t"
    /// but `extract_from_alias_from_cte_name` can't derive it from the CTE name)
    pub from_alias_override: Option<String>,
}

impl CteVariableInfo {
    /// Get the effective FROM alias for SQL references.
    /// Uses `from_alias_override` if set, otherwise derives from CTE name.
    pub fn effective_from_alias(&self) -> String {
        self.from_alias_override
            .clone()
            .unwrap_or_else(|| extract_from_alias_from_cte_name(&self.cte_name))
    }
}

/// Result of resolving a variable property reference.
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedProperty {
    /// Variable is CTE-scoped: use this CTE column name, qualified by CTE alias
    CteColumn { cte_name: String, column: String },
    /// Variable is table-scoped: use this DB column name
    DbColumn(String),
    /// Cannot resolve in current scope
    Unresolved,
}

/// The single source of truth for variable resolution at any point in rendering.
///
/// Built iteratively in `build_chained_with_match_cte_plan()`:
/// - Start with `new(schema, plan)` — all variables are table variables
/// - After each WITH: call `advance_with()` to register CTE exports
/// - Query with `resolve()` — CTE variables checked first, then schema
#[derive(Debug)]
pub struct VariableScope<'a> {
    /// Schema for resolving table variable properties (Cypher prop → DB column)
    schema: &'a GraphSchema,

    /// Variables exported through CTEs (alias → CTE info).
    /// Queried FIRST during resolution — CTE variables take precedence.
    cte_variables: HashMap<String, CteVariableInfo>,

    /// The current plan tree (for alias → label resolution of table variables)
    plan: &'a LogicalPlan,
}

impl<'a> VariableScope<'a> {
    /// Create a new scope with no CTE variables (all variables are table-scoped).
    pub fn new(schema: &'a GraphSchema, plan: &'a LogicalPlan) -> Self {
        Self {
            schema,
            cte_variables: HashMap::new(),
            plan,
        }
    }

    /// Create a scope with pre-accumulated CTE variables.
    /// Used when rebuilding scope after plan tree mutation.
    pub fn with_cte_variables(
        schema: &'a GraphSchema,
        plan: &'a LogicalPlan,
        cte_variables: HashMap<String, CteVariableInfo>,
    ) -> Self {
        Self {
            schema,
            cte_variables,
            plan,
        }
    }

    /// Resolve alias.property → actual column reference.
    ///
    /// Resolution order:
    /// 1. CTE variable (by Cypher alias)? → forward CTE lookup (CTE column name)
    /// 2. CTE variable (by CTE name)? → handles JOIN conditions using CTE table names
    /// 3. Table variable? → schema lookup (DB column name)
    /// 4. Neither → Unresolved (caller falls back to existing logic)
    pub fn resolve(&self, alias: &str, cypher_property: &str) -> ResolvedProperty {
        // 1. Check CTE variables by Cypher alias (e.g., "friend" → with_friend_cte_0)
        if let Some(cte_info) = self.cte_variables.get(alias) {
            if let Some(cte_col) = cte_info.property_mapping.get(cypher_property) {
                // Use FROM alias (e.g., "post_tag") not CTE name (e.g., "with_post_tag_cte_1")
                // because SQL requires referencing the FROM alias when one is defined
                let from_alias = cte_info.effective_from_alias();
                log::debug!(
                    "VariableScope: {}.{} → CTE column {}.{}",
                    alias,
                    cypher_property,
                    from_alias,
                    cte_col
                );
                return ResolvedProperty::CteColumn {
                    cte_name: from_alias,
                    column: cte_col.clone(),
                };
            }
            // Property not in Cypher→CTE mapping by key, but it might already be
            // a resolved CTE column name (e.g., join_builder produced b.p1_b_id
            // where p1_b_id is already the CTE column). Fix the table alias.
            if cte_info
                .property_mapping
                .values()
                .any(|v| v == cypher_property)
            {
                let from_alias = cte_info.effective_from_alias();
                log::debug!(
                    "VariableScope: {}.{} → already-resolved CTE column, fixing alias to {}",
                    alias,
                    cypher_property,
                    from_alias
                );
                return ResolvedProperty::CteColumn {
                    cte_name: from_alias,
                    column: cypher_property.to_string(),
                };
            }
            log::debug!(
                "VariableScope: {}.{} is CTE-scoped but property not found in mapping",
                alias,
                cypher_property
            );
            return ResolvedProperty::Unresolved;
        }

        // 2. Check if alias is a CTE name (e.g., "with_friend_cte_0.id" in JOIN conditions)
        // Multiple Cypher aliases may map to the same CTE name (e.g., person, messageCount, likeCount
        // all come from with_person_messageCount_likeCount_cte_1). Each has its own per-alias property
        // mapping, so we must search ALL entries with matching CTE name.
        let mut found_cte = false;
        for cte_info in self.cte_variables.values() {
            if cte_info.cte_name != alias {
                continue;
            }
            found_cte = true;
            if let Some(cte_col) = cte_info.property_mapping.get(cypher_property) {
                let from_alias = cte_info.effective_from_alias();
                log::debug!(
                    "VariableScope: {}.{} → CTE column (by cte_name) {}.{}",
                    alias,
                    cypher_property,
                    from_alias,
                    cte_col
                );
                return ResolvedProperty::CteColumn {
                    cte_name: from_alias,
                    column: cte_col.clone(),
                };
            }
        }
        if found_cte {
            log::debug!(
                "VariableScope: {}.{} found CTE by name but property not in any alias mapping",
                alias,
                cypher_property
            );
            return ResolvedProperty::Unresolved;
        }

        // 3. Check table variables via schema
        if let Some(label) = find_label_for_alias_in_plan(self.plan, alias) {
            if let Ok(db_col) = map_property_to_db_column(cypher_property, &label) {
                log::debug!(
                    "VariableScope: {}.{} → DB column {} (label: {})",
                    alias,
                    cypher_property,
                    db_col,
                    label
                );
                return ResolvedProperty::DbColumn(db_col);
            }
        }

        ResolvedProperty::Unresolved
    }

    /// Check if an alias is a CTE-scoped variable.
    pub fn is_cte_variable(&self, alias: &str) -> bool {
        self.cte_variables.contains_key(alias)
    }

    /// Get labels for an alias (needed by opaque generators for node ID lookups).
    /// Works for both CTE and table variables.
    pub fn get_labels(&self, alias: &str) -> Option<Vec<String>> {
        if let Some(cte_info) = self.cte_variables.get(alias) {
            if !cte_info.labels.is_empty() {
                return Some(cte_info.labels.clone());
            }
        }
        find_label_for_alias_in_plan(self.plan, alias).map(|l| vec![l])
    }

    /// Advance scope past a WITH barrier.
    ///
    /// Registers the WITH's exported variable as a CTE variable. After this call,
    /// `resolve(alias, prop)` will return CTE column names instead of DB columns.
    pub fn advance_with(
        &mut self,
        alias: &str,
        cte_name: &str,
        property_mapping: &HashMap<String, String>,
        labels: Vec<String>,
        new_plan: &'a LogicalPlan,
    ) {
        log::debug!(
            "VariableScope: advance_with('{}', cte='{}', {} properties, labels={:?})",
            alias,
            cte_name,
            property_mapping.len(),
            labels
        );
        self.cte_variables.insert(
            alias.to_string(),
            CteVariableInfo {
                cte_name: cte_name.to_string(),
                property_mapping: property_mapping.clone(),
                labels,
                from_alias_override: None,
            },
        );
        self.plan = new_plan;
    }

    /// Update the plan reference (for when the plan tree changes between iterations).
    pub fn update_plan(&mut self, new_plan: &'a LogicalPlan) {
        self.plan = new_plan;
    }

    /// Get the current plan reference.
    pub fn plan(&self) -> &'a LogicalPlan {
        self.plan
    }

    /// Get the schema reference.
    pub fn schema(&self) -> &'a GraphSchema {
        self.schema
    }

    /// Get CTE variable info for an alias (if it's CTE-scoped).
    pub fn get_cte_info(&self, alias: &str) -> Option<&CteVariableInfo> {
        self.cte_variables.get(alias)
    }

    /// Get a clone of all accumulated CTE variables.
    /// Used for rebuilding scope after plan tree mutation.
    pub fn cte_variables(&self) -> &HashMap<String, CteVariableInfo> {
        &self.cte_variables
    }
}

// --- Scope-aware RenderPlan rewriting ---

use super::render_expr::{
    Column, ColumnAlias, OperatorApplication, PropertyAccess, RenderExpr, TableAlias,
};
use super::{FilterItems, GroupByExpressions, OrderByItems, RenderPlan, SelectItem, UnionItems};
use crate::graph_catalog::expression_parser::PropertyValue;

/// Rewrite all expressions in a RenderPlan using scope-based resolution.
/// CTE-scoped variables get their table_alias rewritten to the CTE name
/// and their column rewritten to the CTE column name.
pub fn rewrite_render_plan_with_scope(plan: &mut RenderPlan, scope: &VariableScope) {
    // Rewrite SELECT items — expand bare node CTE variables into individual columns
    let mut expanded_items = Vec::new();
    for item in &plan.select.items {
        if let RenderExpr::TableAlias(TableAlias(alias_name)) = &item.expression {
            if let Some(cte_info) = scope.get_cte_info(alias_name) {
                let from_alias = cte_info.effective_from_alias();
                if !cte_info.property_mapping.is_empty() {
                    // Expand node CTE variable into individual property columns
                    let mut props: Vec<_> = cte_info.property_mapping.iter().collect();
                    props.sort_by_key(|(k, _)| k.clone());
                    for (cypher_prop, cte_col) in props {
                        expanded_items.push(SelectItem {
                            expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(from_alias.clone()),
                                column: PropertyValue::Column(cte_col.clone()),
                            }),
                            col_alias: Some(ColumnAlias(format!("{}.{}", alias_name, cypher_prop))),
                        });
                    }
                    continue;
                } else {
                    // Scalar CTE variable: rewrite to direct CTE column reference
                    expanded_items.push(SelectItem {
                        expression: RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(from_alias),
                            column: PropertyValue::Column(alias_name.clone()),
                        }),
                        col_alias: Some(ColumnAlias(alias_name.clone())),
                    });
                    continue;
                }
            }
        }
        let mut new_item = item.clone();
        new_item.expression = rewrite_render_expr(&item.expression, scope);
        expanded_items.push(new_item);
    }
    plan.select.items = expanded_items;

    // Rewrite WHERE filters
    if let FilterItems(Some(ref filter)) = plan.filters {
        let rewritten = rewrite_render_expr(filter, scope);
        plan.filters = FilterItems(Some(rewritten));
    }

    // Rewrite ORDER BY
    for item in &mut plan.order_by.0 {
        item.expression = rewrite_render_expr(&item.expression, scope);
    }

    // Rewrite GROUP BY
    let rewritten_gb: Vec<RenderExpr> = plan
        .group_by
        .0
        .iter()
        .map(|expr| rewrite_render_expr(expr, scope))
        .collect();
    plan.group_by = GroupByExpressions(rewritten_gb);

    // Rewrite HAVING
    if let Some(ref having) = plan.having_clause {
        plan.having_clause = Some(rewrite_render_expr(having, scope));
    }

    // Rewrite JOIN conditions
    for join in &mut plan.joins.0 {
        for cond in &mut join.joining_on {
            cond.operands = cond
                .operands
                .iter()
                .map(|op| rewrite_render_expr(op, scope))
                .collect();
        }
        if let Some(ref pre_filter) = join.pre_filter {
            join.pre_filter = Some(rewrite_render_expr(pre_filter, scope));
        }
    }

    // Rewrite UNION branches (e.g., bidirectional relationship expansions)
    if let UnionItems(Some(ref mut union)) = plan.union {
        for branch in &mut union.input {
            rewrite_render_plan_with_scope(branch, scope);
        }
    }
}

/// Rewrite only bare variable references (TableAlias, ColumnAlias, Column) in a RenderPlan,
/// without touching PropertyAccessExp. This is for CTE body rendering where
/// `fix_orphan_table_aliases` already handles PropertyAccessExp resolution.
pub fn rewrite_bare_variables_in_plan(plan: &mut RenderPlan, scope: &VariableScope) {
    // Rewrite SELECT item expressions
    for item in &mut plan.select.items {
        item.expression = rewrite_bare_variables(&item.expression, scope);
    }

    // Rewrite WHERE filters
    if let FilterItems(Some(ref filter)) = plan.filters {
        plan.filters = FilterItems(Some(rewrite_bare_variables(filter, scope)));
    }

    // Rewrite ORDER BY
    for item in &mut plan.order_by.0 {
        item.expression = rewrite_bare_variables(&item.expression, scope);
    }

    // Rewrite GROUP BY
    let rewritten_gb: Vec<RenderExpr> = plan
        .group_by
        .0
        .iter()
        .map(|expr| rewrite_bare_variables(expr, scope))
        .collect();
    plan.group_by = GroupByExpressions(rewritten_gb);

    // Rewrite HAVING
    if let Some(ref having) = plan.having_clause {
        plan.having_clause = Some(rewrite_bare_variables(having, scope));
    }

    // Rewrite JOIN conditions
    for join in &mut plan.joins.0 {
        for cond in &mut join.joining_on {
            cond.operands = cond
                .operands
                .iter()
                .map(|op| rewrite_bare_variables(op, scope))
                .collect();
        }
        if let Some(ref pre_filter) = join.pre_filter {
            join.pre_filter = Some(rewrite_bare_variables(pre_filter, scope));
        }
    }

    // Rewrite UNION branches
    if let UnionItems(Some(ref mut union)) = plan.union {
        for branch in &mut union.input {
            rewrite_bare_variables_in_plan(branch, scope);
        }
    }
}

/// Recursively rewrite only bare variable references (TableAlias, ColumnAlias, Column)
/// to qualified CTE column references, leaving PropertyAccessExp untouched.
fn rewrite_bare_variables(expr: &RenderExpr, scope: &VariableScope) -> RenderExpr {
    match expr {
        // Wildcard PropertyAccessExp (alias.*) = bare node reference from ProjectionTagging.
        // Resolve to node ID column, same as TableAlias handling below.
        RenderExpr::PropertyAccessExp(pa) if pa.column.raw() == "*" => {
            let alias_name = &pa.table_alias.0;
            if let Some(cte_info) = scope.get_cte_info(alias_name) {
                let from_alias = cte_info.effective_from_alias();
                if cte_info.property_mapping.is_empty() {
                    RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(from_alias),
                        column: PropertyValue::Column(alias_name.clone()),
                    })
                } else if let Some(id_col) = cte_info.property_mapping.get("id") {
                    RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(from_alias),
                        column: PropertyValue::Column(id_col.clone()),
                    })
                } else {
                    expr.clone()
                }
            } else {
                // Not a CTE variable — check if it's a node alias in the plan.
                if let Some(label) = crate::query_planner::logical_expr::expression_rewriter::find_label_for_alias_in_plan(
                    scope.plan(), alias_name,
                ) {
                    if let Some(node) = scope.schema().node_schema_opt(&label) {
                        if let Ok(id_col) = node.node_id.column_or_error() {
                            log::debug!(
                                "rewrite_bare_variables: wildcard PropertyAccessExp '{}.*' ({}) -> {}.{}",
                                alias_name, label, alias_name, id_col
                            );
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(alias_name.clone()),
                                column: PropertyValue::Column(id_col.to_string()),
                            })
                        } else {
                            expr.clone()
                        }
                    } else {
                        expr.clone()
                    }
                } else {
                    expr.clone()
                }
            }
        }
        // Leave other PropertyAccessExp untouched — fix_orphan_table_aliases handles these
        RenderExpr::PropertyAccessExp(_) => expr.clone(),
        // Recurse into compound expressions
        RenderExpr::OperatorApplicationExp(oa) => {
            let rewritten_operands: Vec<RenderExpr> = oa
                .operands
                .iter()
                .map(|op| rewrite_bare_variables(op, scope))
                .collect();
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: oa.operator.clone(),
                operands: rewritten_operands,
            })
        }
        RenderExpr::AggregateFnCall(agg) => {
            let rewritten_args: Vec<RenderExpr> = agg
                .args
                .iter()
                .map(|arg| rewrite_bare_variables(arg, scope))
                .collect();
            RenderExpr::AggregateFnCall(super::render_expr::AggregateFnCall {
                name: agg.name.clone(),
                args: rewritten_args,
            })
        }
        RenderExpr::ScalarFnCall(sf) => {
            let rewritten_args: Vec<RenderExpr> = sf
                .args
                .iter()
                .map(|arg| rewrite_bare_variables(arg, scope))
                .collect();
            RenderExpr::ScalarFnCall(super::render_expr::ScalarFnCall {
                name: sf.name.clone(),
                args: rewritten_args,
            })
        }
        RenderExpr::Case(case) => {
            let rewritten_expr = case
                .expr
                .as_ref()
                .map(|e| Box::new(rewrite_bare_variables(e, scope)));
            let rewritten_when_then: Vec<(RenderExpr, RenderExpr)> = case
                .when_then
                .iter()
                .map(|(cond, val)| {
                    (
                        rewrite_bare_variables(cond, scope),
                        rewrite_bare_variables(val, scope),
                    )
                })
                .collect();
            let rewritten_else = case
                .else_expr
                .as_ref()
                .map(|e| Box::new(rewrite_bare_variables(e, scope)));
            RenderExpr::Case(super::render_expr::RenderCase {
                expr: rewritten_expr,
                when_then: rewritten_when_then,
                else_expr: rewritten_else,
            })
        }
        RenderExpr::List(items) => {
            let rewritten: Vec<RenderExpr> = items
                .iter()
                .map(|item| rewrite_bare_variables(item, scope))
                .collect();
            RenderExpr::List(rewritten)
        }
        RenderExpr::ArraySubscript { array, index } => RenderExpr::ArraySubscript {
            array: Box::new(rewrite_bare_variables(array, scope)),
            index: Box::new(rewrite_bare_variables(index, scope)),
        },
        RenderExpr::ArraySlicing { array, from, to } => RenderExpr::ArraySlicing {
            array: Box::new(rewrite_bare_variables(array, scope)),
            from: from
                .as_ref()
                .map(|f| Box::new(rewrite_bare_variables(f, scope))),
            to: to
                .as_ref()
                .map(|t| Box::new(rewrite_bare_variables(t, scope))),
        },
        RenderExpr::MapLiteral(entries) => {
            let rewritten: Vec<(String, RenderExpr)> = entries
                .iter()
                .map(|(k, v)| (k.clone(), rewrite_bare_variables(v, scope)))
                .collect();
            RenderExpr::MapLiteral(rewritten)
        }
        // Bare variable references — rewrite CTE variables to qualified column references
        RenderExpr::TableAlias(TableAlias(alias_name))
        | RenderExpr::ColumnAlias(ColumnAlias(alias_name)) => {
            if let Some(cte_info) = scope.get_cte_info(alias_name) {
                let from_alias = cte_info.effective_from_alias();
                if cte_info.property_mapping.is_empty() {
                    RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(from_alias),
                        column: PropertyValue::Column(alias_name.clone()),
                    })
                } else if let Some(id_col) = cte_info.property_mapping.get("id") {
                    RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(from_alias),
                        column: PropertyValue::Column(id_col.clone()),
                    })
                } else {
                    expr.clone()
                }
            } else {
                // Not a CTE variable — check if it's a node alias in the plan.
                // Bare node references (e.g., `likerZombie` in `has(list, likerZombie)`)
                // should resolve to `{alias}.{node_id_column}` (typically `{alias}.id`).
                if let Some(label) = crate::query_planner::logical_expr::expression_rewriter::find_label_for_alias_in_plan(
                    scope.plan(), alias_name,
                ) {
                    if let Some(node) = scope.schema().node_schema_opt(&label) {
                        if let Ok(id_col) = node.node_id.column_or_error() {
                            log::debug!(
                                "rewrite_bare_variables: non-CTE node '{}' ({}) -> {}.{}",
                                alias_name, label, alias_name, id_col
                            );
                            RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(alias_name.clone()),
                                column: PropertyValue::Column(id_col.to_string()),
                            })
                        } else {
                            expr.clone()
                        }
                    } else {
                        expr.clone()
                    }
                } else {
                    expr.clone()
                }
            }
        }
        RenderExpr::Column(col) => {
            if let PropertyValue::Column(col_name) = &col.0 {
                if let Some(cte_info) = scope.get_cte_info(col_name) {
                    let from_alias = cte_info.effective_from_alias();
                    if cte_info.property_mapping.is_empty() {
                        return RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(from_alias),
                            column: PropertyValue::Column(col_name.clone()),
                        });
                    } else if let Some(id_col) = cte_info.property_mapping.get("id") {
                        return RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(from_alias),
                            column: PropertyValue::Column(id_col.clone()),
                        });
                    }
                }
            }
            expr.clone()
        }
        // Leaf nodes — no rewriting needed
        _ => expr.clone(),
    }
}

/// Extract the FROM alias from a CTE name.
/// E.g., "with_post_tag_cte_1" → "post_tag", "with_a_cte" → "a"
fn extract_from_alias_from_cte_name(cte_name: &str) -> String {
    let base = cte_name.strip_prefix("with_").unwrap_or(cte_name);

    // Handle unnumbered suffix "_cte"
    if let Some(stripped) = base.strip_suffix("_cte") {
        return stripped.to_string();
    }

    // Handle numbered suffixes like "_cte_1", "_cte_2", ..., "_cte_<digits>"
    if let Some(pos) = base.rfind("_cte_") {
        let suffix = &base[pos + "_cte_".len()..];
        if !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit()) {
            return base[..pos].to_string();
        }
    }

    // CTE name doesn't follow the expected "with_<alias>_cte[_N]" convention.
    // Fall back to the base name. This should not happen with CTEs generated by
    // build_chained_with_match_cte_plan(); log a warning if it does.
    log::debug!(
        "extract_from_alias_from_cte_name: unexpected CTE name format '{}', using '{}' as FROM alias",
        cte_name, base
    );
    base.to_string()
}

/// Recursively rewrite a RenderExpr using scope resolution.
/// PropertyAccessExp(alias, property) is resolved via scope:
/// - CteColumn → PropertyAccessExp(cte_name, cte_column)
/// - DbColumn → PropertyAccessExp(alias, db_column)
/// - Unresolved → unchanged
pub fn rewrite_render_expr(expr: &RenderExpr, scope: &VariableScope) -> RenderExpr {
    match expr {
        RenderExpr::PropertyAccessExp(pa) => {
            let alias = &pa.table_alias.0;
            let property_name = match &pa.column {
                PropertyValue::Column(col) => col.as_str(),
                PropertyValue::Expression(_) => return expr.clone(),
            };
            match scope.resolve(alias, property_name) {
                ResolvedProperty::CteColumn { cte_name, column } => {
                    RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(cte_name),
                        column: PropertyValue::Column(column),
                    })
                }
                ResolvedProperty::DbColumn(db_col) => {
                    RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: pa.table_alias.clone(),
                        column: PropertyValue::Column(db_col),
                    })
                }
                ResolvedProperty::Unresolved => expr.clone(),
            }
        }
        RenderExpr::OperatorApplicationExp(oa) => {
            let rewritten_operands: Vec<RenderExpr> = oa
                .operands
                .iter()
                .map(|op| rewrite_render_expr(op, scope))
                .collect();
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: oa.operator.clone(),
                operands: rewritten_operands,
            })
        }
        RenderExpr::AggregateFnCall(agg) => {
            let rewritten_args: Vec<RenderExpr> = agg
                .args
                .iter()
                .map(|arg| rewrite_render_expr(arg, scope))
                .collect();
            RenderExpr::AggregateFnCall(super::render_expr::AggregateFnCall {
                name: agg.name.clone(),
                args: rewritten_args,
            })
        }
        RenderExpr::ScalarFnCall(sf) => {
            let rewritten_args: Vec<RenderExpr> = sf
                .args
                .iter()
                .map(|arg| rewrite_render_expr(arg, scope))
                .collect();
            RenderExpr::ScalarFnCall(super::render_expr::ScalarFnCall {
                name: sf.name.clone(),
                args: rewritten_args,
            })
        }
        RenderExpr::Case(case) => {
            let rewritten_expr = case
                .expr
                .as_ref()
                .map(|e| Box::new(rewrite_render_expr(e, scope)));
            let rewritten_when_then: Vec<(RenderExpr, RenderExpr)> = case
                .when_then
                .iter()
                .map(|(cond, val)| {
                    (
                        rewrite_render_expr(cond, scope),
                        rewrite_render_expr(val, scope),
                    )
                })
                .collect();
            let rewritten_else = case
                .else_expr
                .as_ref()
                .map(|e| Box::new(rewrite_render_expr(e, scope)));
            RenderExpr::Case(super::render_expr::RenderCase {
                expr: rewritten_expr,
                when_then: rewritten_when_then,
                else_expr: rewritten_else,
            })
        }
        RenderExpr::List(items) => {
            let rewritten: Vec<RenderExpr> = items
                .iter()
                .map(|item| rewrite_render_expr(item, scope))
                .collect();
            RenderExpr::List(rewritten)
        }
        RenderExpr::ArraySubscript { array, index } => RenderExpr::ArraySubscript {
            array: Box::new(rewrite_render_expr(array, scope)),
            index: Box::new(rewrite_render_expr(index, scope)),
        },
        RenderExpr::ArraySlicing { array, from, to } => RenderExpr::ArraySlicing {
            array: Box::new(rewrite_render_expr(array, scope)),
            from: from
                .as_ref()
                .map(|f| Box::new(rewrite_render_expr(f, scope))),
            to: to.as_ref().map(|t| Box::new(rewrite_render_expr(t, scope))),
        },
        RenderExpr::MapLiteral(entries) => {
            let rewritten: Vec<(String, RenderExpr)> = entries
                .iter()
                .map(|(k, v)| (k.clone(), rewrite_render_expr(v, scope)))
                .collect();
            RenderExpr::MapLiteral(rewritten)
        }
        // Bare variable references — rewrite CTE variables to qualified column references
        RenderExpr::TableAlias(TableAlias(alias_name))
        | RenderExpr::ColumnAlias(ColumnAlias(alias_name)) => {
            if let Some(cte_info) = scope.get_cte_info(alias_name) {
                let from_alias = cte_info.effective_from_alias();
                if cte_info.property_mapping.is_empty() {
                    // Scalar CTE variable: rewrite to from_alias.variable_name
                    RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(from_alias),
                        column: PropertyValue::Column(alias_name.clone()),
                    })
                } else if let Some(id_col) = cte_info.property_mapping.get("id") {
                    // Node CTE variable: rewrite bare reference to its ID column
                    RenderExpr::PropertyAccessExp(PropertyAccess {
                        table_alias: TableAlias(from_alias),
                        column: PropertyValue::Column(id_col.clone()),
                    })
                } else {
                    expr.clone()
                }
            } else {
                expr.clone()
            }
        }
        // Column references — may also be bare CTE variable references
        RenderExpr::Column(col) => {
            if let PropertyValue::Column(col_name) = &col.0 {
                if let Some(cte_info) = scope.get_cte_info(col_name) {
                    let from_alias = cte_info.effective_from_alias();
                    if cte_info.property_mapping.is_empty() {
                        return RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(from_alias),
                            column: PropertyValue::Column(col_name.clone()),
                        });
                    } else if let Some(id_col) = cte_info.property_mapping.get("id") {
                        return RenderExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(from_alias),
                            column: PropertyValue::Column(id_col.clone()),
                        });
                    }
                }
            }
            expr.clone()
        }
        // Leaf nodes — no rewriting needed
        RenderExpr::Literal(_)
        | RenderExpr::Raw(_)
        | RenderExpr::Star
        | RenderExpr::Parameter(_)
        | RenderExpr::InSubquery(_)
        | RenderExpr::ExistsSubquery(_)
        | RenderExpr::ReduceExpr(_)
        | RenderExpr::PatternCount(_)
        | RenderExpr::CteEntityRef(_) => expr.clone(),
    }
}

/// Post-process a RenderPlan to fix orphaned composite alias references.
///
/// After scope-based rewriting, some expressions may reference composite aliases
/// (e.g., "countWindow1_tag") that don't match any FROM/JOIN alias in the render plan.
/// This happens because CTE body rendering uses individual aliases (e.g., "tag") as
/// FROM aliases, while the logical plan's expressions use composite aliases.
///
/// This function:
/// 1. Collects all valid FROM/JOIN aliases
/// 2. For any expression table_alias not in FROM/JOINs, checks scope CTE variables
/// 3. If the CTE name matches a FROM/JOIN entry, replaces the table_alias with the FROM/JOIN alias
pub fn fix_orphan_table_aliases(plan: &mut RenderPlan, scope: &VariableScope) {
    fix_orphan_table_aliases_impl(plan, scope, true);
}

fn fix_orphan_table_aliases_impl(
    plan: &mut RenderPlan,
    scope: &VariableScope,
    add_cross_joins: bool,
) {
    use std::collections::HashMap;

    // 1. Build mapping: CTE name → FROM/JOIN alias
    let mut cte_name_to_from_alias: HashMap<String, String> = HashMap::new();
    let mut valid_aliases: std::collections::HashSet<String> = std::collections::HashSet::new();

    // Check main FROM
    if let super::FromTableItem(Some(ref from_table)) = plan.from {
        if let Some(ref alias) = from_table.alias {
            valid_aliases.insert(alias.clone());
            if from_table.name.starts_with("with_") {
                cte_name_to_from_alias.insert(from_table.name.clone(), alias.clone());
            }
        } else {
            valid_aliases.insert(from_table.name.clone());
        }
    }

    // Check JOINs
    for join in &plan.joins.0 {
        valid_aliases.insert(join.table_alias.clone());
        if join.table_name.starts_with("with_") {
            cte_name_to_from_alias.insert(join.table_name.clone(), join.table_alias.clone());
        }
    }

    // 2. Find CTE-scoped variables whose CTE is NOT in FROM/JOINs and add CROSS JOINs.
    // This handles cases like: WITH knownTag.id AS knownTagId ... WHERE t.id = knownTagId
    // where knownTagId comes from a previous CTE but the current CTE body doesn't reference it.
    // Skip in UNION branches (add_cross_joins=false) to avoid spurious joins.
    if add_cross_joins {
        let mut missing_ctes: Vec<(String, String)> = Vec::new(); // (cte_name, from_alias)
        let mut seen_cte_names: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        for (_alias, cte_info) in scope.cte_variables() {
            if cte_name_to_from_alias.contains_key(&cte_info.cte_name) {
                continue;
            }
            if seen_cte_names.contains(&cte_info.cte_name) {
                continue;
            }
            // Skip VLP CTEs — they should only appear when explicitly referenced
            // through VLP Union branches, not as spurious CROSS JOINs in downstream CTEs.
            if cte_info.cte_name.starts_with("vlp_") {
                continue;
            }
            let from_alias = cte_info.effective_from_alias();
            // Don't add if from_alias collides with an existing valid alias
            if valid_aliases.contains(&from_alias) {
                continue;
            }
            seen_cte_names.insert(cte_info.cte_name.clone());
            missing_ctes.push((cte_info.cte_name.clone(), from_alias));
        }
        // Sort for deterministic ordering
        missing_ctes.sort();
        for (cte_name, from_alias) in &missing_ctes {
            log::info!(
                "🔧 fix_orphan_table_aliases: Adding CROSS JOIN for missing CTE {} AS {}",
                cte_name,
                from_alias
            );
            plan.joins.0.push(super::Join {
                table_name: cte_name.clone(),
                table_alias: from_alias.clone(),
                joining_on: vec![],
                join_type: super::JoinType::Join,
                pre_filter: None,
                from_id_column: None,
                to_id_column: None,
                graph_rel: None,
            });
            valid_aliases.insert(from_alias.clone());
            cte_name_to_from_alias.insert(cte_name.clone(), from_alias.clone());
        }
    }

    if cte_name_to_from_alias.is_empty() {
        return; // No CTE references in FROM/JOINs, nothing to fix
    }

    // 3. Build mapping: orphaned composite alias → correct FROM alias
    let mut alias_replacements: HashMap<String, String> = HashMap::new();
    for (alias, cte_info) in scope.cte_variables() {
        if valid_aliases.contains(alias) {
            continue; // This alias is already a valid FROM/JOIN alias
        }
        if let Some(from_alias) = cte_name_to_from_alias.get(&cte_info.cte_name) {
            alias_replacements.insert(alias.clone(), from_alias.clone());
        }
    }

    if alias_replacements.is_empty() {
        return; // No orphaned aliases to fix
    }

    log::info!(
        "🔧 fix_orphan_table_aliases: Fixing {} orphaned aliases: {:?}",
        alias_replacements.len(),
        alias_replacements
    );

    // 4. Rewrite all expressions
    let rewrite =
        |expr: &RenderExpr| -> RenderExpr { rewrite_expr_table_aliases(expr, &alias_replacements) };

    // SELECT
    for item in &mut plan.select.items {
        item.expression = rewrite(&item.expression);
    }

    // WHERE
    if let FilterItems(Some(ref filter)) = plan.filters {
        plan.filters = FilterItems(Some(rewrite(filter)));
    }

    // GROUP BY
    let new_gb: Vec<RenderExpr> = plan.group_by.0.iter().map(|e| rewrite(e)).collect();
    plan.group_by = GroupByExpressions(new_gb);

    // ORDER BY
    for item in &mut plan.order_by.0 {
        item.expression = rewrite(&item.expression);
    }

    // HAVING
    if let Some(ref having) = plan.having_clause {
        plan.having_clause = Some(rewrite(having));
    }

    // JOIN conditions
    for join in &mut plan.joins.0 {
        for cond in &mut join.joining_on {
            cond.operands = cond.operands.iter().map(|op| rewrite(op)).collect();
        }
        if let Some(ref pf) = join.pre_filter {
            join.pre_filter = Some(rewrite(pf));
        }
    }

    // UNION branches - don't add CROSS JOINs in nested branches
    if let UnionItems(Some(ref mut union)) = plan.union {
        for branch in &mut union.input {
            fix_orphan_table_aliases_impl(branch, scope, false);
        }
    }
}

/// Rewrite CTE property columns in a rendered plan.
/// For each PropertyAccessExp where the table alias is a CTE variable,
/// map the column name through the CTE's property_mapping.
/// E.g., `zombie.creationDate` → `zombie.p6_zombie_creationDate`
pub fn rewrite_cte_property_columns(plan: &mut RenderPlan, scope: &VariableScope) {
    use std::collections::HashMap;

    // Build mapping: valid FROM/JOIN alias → property mapping
    let mut alias_prop_map: HashMap<String, &HashMap<String, String>> = HashMap::new();

    // Collect valid FROM/JOIN aliases
    let mut valid_aliases: std::collections::HashSet<String> = std::collections::HashSet::new();
    if let super::FromTableItem(Some(ref from_table)) = plan.from {
        if let Some(ref alias) = from_table.alias {
            valid_aliases.insert(alias.clone());
        }
    }
    for join in &plan.joins.0 {
        valid_aliases.insert(join.table_alias.clone());
    }

    // For each CTE variable, if its from_alias is a valid FROM/JOIN alias,
    // use its property mapping for column rewriting
    for (alias, cte_info) in scope.cte_variables() {
        let from_alias = cte_info.effective_from_alias();
        let target_alias = if valid_aliases.contains(&from_alias) {
            from_alias
        } else if valid_aliases.contains(alias) {
            alias.clone()
        } else {
            continue;
        };
        if !cte_info.property_mapping.is_empty() {
            alias_prop_map.insert(target_alias, &cte_info.property_mapping);
        }
    }

    // Also check FROM/JOIN entries that reference CTEs but whose aliases aren't
    // in the scope. Find the CTE's property mapping from any scope variable that
    // maps to the same CTE name.
    let mut cte_name_to_prop_map: std::collections::HashMap<
        String,
        &std::collections::HashMap<String, String>,
    > = std::collections::HashMap::new();
    for (_alias, cte_info) in scope.cte_variables() {
        if !cte_info.property_mapping.is_empty() {
            cte_name_to_prop_map
                .entry(cte_info.cte_name.clone())
                .or_insert(&cte_info.property_mapping);
        }
    }
    // Collect aliases that reference CTEs (starts_with("with_") or starts_with("vlp_"))
    let mut cte_aliases: std::collections::HashSet<String> = std::collections::HashSet::new();
    if let super::FromTableItem(Some(ref from_table)) = plan.from {
        if from_table.name.starts_with("with_") || from_table.name.starts_with("vlp_") {
            if let Some(ref alias) = from_table.alias {
                cte_aliases.insert(alias.clone());
                if !alias_prop_map.contains_key(alias) {
                    if let Some(prop_map) = cte_name_to_prop_map.get(&from_table.name) {
                        alias_prop_map.insert(alias.clone(), prop_map);
                    }
                }
            }
        }
    }
    for join in &plan.joins.0 {
        if join.table_name.starts_with("with_") || join.table_name.starts_with("vlp_") {
            cte_aliases.insert(join.table_alias.clone());
            if !alias_prop_map.contains_key(&join.table_alias) {
                if let Some(prop_map) = cte_name_to_prop_map.get(&join.table_name) {
                    alias_prop_map.insert(join.table_alias.clone(), prop_map);
                }
            }
        }
    }

    // Remove any alias_prop_map entries for aliases that reference non-CTE tables.
    // This prevents rewriting regular table columns (e.g., Tag.id) with CTE property mappings.
    alias_prop_map.retain(|alias, _| cte_aliases.contains(alias));

    if alias_prop_map.is_empty() {
        return;
    }

    let rewrite =
        |expr: &RenderExpr| -> RenderExpr { rewrite_expr_cte_columns(expr, &alias_prop_map) };

    // SELECT
    for item in &mut plan.select.items {
        item.expression = rewrite(&item.expression);
    }
    // WHERE
    if let FilterItems(Some(ref filter)) = plan.filters {
        plan.filters = FilterItems(Some(rewrite(filter)));
    }
    // GROUP BY
    let new_gb: Vec<RenderExpr> = plan.group_by.0.iter().map(|e| rewrite(e)).collect();
    plan.group_by = GroupByExpressions(new_gb);
    // ORDER BY
    for item in &mut plan.order_by.0 {
        item.expression = rewrite(&item.expression);
    }
    // HAVING
    if let Some(ref having) = plan.having_clause {
        plan.having_clause = Some(rewrite(having));
    }
    // JOIN conditions
    for join in &mut plan.joins.0 {
        for cond in &mut join.joining_on {
            cond.operands = cond.operands.iter().map(|op| rewrite(op)).collect();
        }
        if let Some(ref pf) = join.pre_filter {
            join.pre_filter = Some(rewrite(pf));
        }
    }
}

/// Extract the raw column name from a PropertyValue.
fn property_value_name(
    pv: &crate::graph_catalog::expression_parser::PropertyValue,
) -> Option<&str> {
    match pv {
        crate::graph_catalog::expression_parser::PropertyValue::Column(s) => Some(s.as_str()),
        crate::graph_catalog::expression_parser::PropertyValue::Expression(s) => Some(s.as_str()),
    }
}

/// Recursively rewrite CTE column names in a RenderExpr.
fn rewrite_expr_cte_columns(
    expr: &RenderExpr,
    alias_prop_map: &std::collections::HashMap<String, &std::collections::HashMap<String, String>>,
) -> RenderExpr {
    match expr {
        RenderExpr::PropertyAccessExp(pa) => {
            if let Some(prop_map) = alias_prop_map.get(&pa.table_alias.0) {
                if let Some(col_name) = property_value_name(&pa.column) {
                    if let Some(cte_col) = prop_map.get(col_name) {
                        // Only rewrite if column differs from CTE column name
                        if col_name != cte_col.as_str() {
                            log::debug!(
                                "🔧 rewrite_cte_property_columns: {}.{} → {}.{}",
                                pa.table_alias.0,
                                col_name,
                                pa.table_alias.0,
                                cte_col
                            );
                            return RenderExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: pa.table_alias.clone(),
                                column:
                                    crate::graph_catalog::expression_parser::PropertyValue::Column(
                                        cte_col.clone(),
                                    ),
                            });
                        }
                    }
                }
            }
            expr.clone()
        }
        RenderExpr::OperatorApplicationExp(oa) => {
            let new_operands: Vec<RenderExpr> = oa
                .operands
                .iter()
                .map(|op| rewrite_expr_cte_columns(op, alias_prop_map))
                .collect();
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: oa.operator.clone(),
                operands: new_operands,
            })
        }
        RenderExpr::AggregateFnCall(agg) => {
            let new_args: Vec<RenderExpr> = agg
                .args
                .iter()
                .map(|arg| rewrite_expr_cte_columns(arg, alias_prop_map))
                .collect();
            RenderExpr::AggregateFnCall(super::render_expr::AggregateFnCall {
                name: agg.name.clone(),
                args: new_args,
            })
        }
        RenderExpr::ScalarFnCall(sf) => {
            let new_args: Vec<RenderExpr> = sf
                .args
                .iter()
                .map(|arg| rewrite_expr_cte_columns(arg, alias_prop_map))
                .collect();
            RenderExpr::ScalarFnCall(super::render_expr::ScalarFnCall {
                name: sf.name.clone(),
                args: new_args,
            })
        }
        RenderExpr::Case(case) => {
            let new_expr = case
                .expr
                .as_ref()
                .map(|e| Box::new(rewrite_expr_cte_columns(e, alias_prop_map)));
            let new_when_then: Vec<(RenderExpr, RenderExpr)> = case
                .when_then
                .iter()
                .map(|(c, v)| {
                    (
                        rewrite_expr_cte_columns(c, alias_prop_map),
                        rewrite_expr_cte_columns(v, alias_prop_map),
                    )
                })
                .collect();
            let new_else = case
                .else_expr
                .as_ref()
                .map(|e| Box::new(rewrite_expr_cte_columns(e, alias_prop_map)));
            RenderExpr::Case(super::render_expr::RenderCase {
                expr: new_expr,
                when_then: new_when_then,
                else_expr: new_else,
            })
        }
        _ => expr.clone(),
    }
}

/// Recursively rewrite table aliases in a RenderExpr using a replacement map.
fn rewrite_expr_table_aliases(
    expr: &RenderExpr,
    replacements: &std::collections::HashMap<String, String>,
) -> RenderExpr {
    match expr {
        RenderExpr::PropertyAccessExp(pa) => {
            if let Some(new_alias) = replacements.get(&pa.table_alias.0) {
                RenderExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: TableAlias(new_alias.clone()),
                    column: pa.column.clone(),
                })
            } else {
                expr.clone()
            }
        }
        RenderExpr::OperatorApplicationExp(oa) => {
            let new_operands: Vec<RenderExpr> = oa
                .operands
                .iter()
                .map(|op| rewrite_expr_table_aliases(op, replacements))
                .collect();
            RenderExpr::OperatorApplicationExp(OperatorApplication {
                operator: oa.operator.clone(),
                operands: new_operands,
            })
        }
        RenderExpr::AggregateFnCall(agg) => {
            let new_args: Vec<RenderExpr> = agg
                .args
                .iter()
                .map(|arg| rewrite_expr_table_aliases(arg, replacements))
                .collect();
            RenderExpr::AggregateFnCall(super::render_expr::AggregateFnCall {
                name: agg.name.clone(),
                args: new_args,
            })
        }
        RenderExpr::ScalarFnCall(sf) => {
            let new_args: Vec<RenderExpr> = sf
                .args
                .iter()
                .map(|arg| rewrite_expr_table_aliases(arg, replacements))
                .collect();
            RenderExpr::ScalarFnCall(super::render_expr::ScalarFnCall {
                name: sf.name.clone(),
                args: new_args,
            })
        }
        RenderExpr::Case(case) => {
            let new_expr = case
                .expr
                .as_ref()
                .map(|e| Box::new(rewrite_expr_table_aliases(e, replacements)));
            let new_when_then: Vec<(RenderExpr, RenderExpr)> = case
                .when_then
                .iter()
                .map(|(c, v)| {
                    (
                        rewrite_expr_table_aliases(c, replacements),
                        rewrite_expr_table_aliases(v, replacements),
                    )
                })
                .collect();
            let new_else = case
                .else_expr
                .as_ref()
                .map(|e| Box::new(rewrite_expr_table_aliases(e, replacements)));
            RenderExpr::Case(super::render_expr::RenderCase {
                expr: new_expr,
                when_then: new_when_then,
                else_expr: new_else,
            })
        }
        RenderExpr::List(items) => {
            let new_items: Vec<RenderExpr> = items
                .iter()
                .map(|item| rewrite_expr_table_aliases(item, replacements))
                .collect();
            RenderExpr::List(new_items)
        }
        _ => expr.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolved_property_variants() {
        let cte = ResolvedProperty::CteColumn {
            cte_name: "cte_0".to_string(),
            column: "p6_friend_name".to_string(),
        };
        let db = ResolvedProperty::DbColumn("full_name".to_string());
        let unresolved = ResolvedProperty::Unresolved;

        assert_ne!(cte, db);
        assert_ne!(cte, unresolved);
        assert_ne!(db, unresolved);
    }

    #[test]
    fn test_cte_variable_info_clone() {
        let info = CteVariableInfo {
            cte_name: "cte_0".to_string(),
            property_mapping: HashMap::from([
                ("name".to_string(), "p6_friend_name".to_string()),
                ("id".to_string(), "p6_friend_id".to_string()),
            ]),
            labels: vec!["Person".to_string()],
            from_alias_override: None,
        };
        let cloned = info.clone();
        assert_eq!(cloned.cte_name, "cte_0");
        assert_eq!(cloned.property_mapping.len(), 2);
        assert_eq!(cloned.labels, vec!["Person"]);
    }

    #[test]
    fn test_extract_from_alias_from_cte_name() {
        assert_eq!(
            extract_from_alias_from_cte_name("with_post_tag_cte_1"),
            "post_tag"
        );
        assert_eq!(extract_from_alias_from_cte_name("with_a_cte_1"), "a");
        assert_eq!(extract_from_alias_from_cte_name("with_a_b_cte"), "a_b");
        assert_eq!(extract_from_alias_from_cte_name("plain_name"), "plain_name");
    }
}
