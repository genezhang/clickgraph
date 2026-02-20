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
                let from_alias = extract_from_alias_from_cte_name(&cte_info.cte_name);
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
                let from_alias = extract_from_alias_from_cte_name(&cte_info.cte_name);
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
                let from_alias = extract_from_alias_from_cte_name(&cte_info.cte_name);
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
    ColumnAlias, OperatorApplication, PropertyAccess, RenderExpr, TableAlias,
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
                let from_alias = extract_from_alias_from_cte_name(&cte_info.cte_name);
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
        // Leaf nodes — no rewriting needed
        RenderExpr::Literal(_)
        | RenderExpr::Raw(_)
        | RenderExpr::Star
        | RenderExpr::TableAlias(_)
        | RenderExpr::ColumnAlias(_)
        | RenderExpr::Column(_)
        | RenderExpr::Parameter(_)
        | RenderExpr::InSubquery(_)
        | RenderExpr::ExistsSubquery(_)
        | RenderExpr::ReduceExpr(_)
        | RenderExpr::PatternCount(_)
        | RenderExpr::CteEntityRef(_) => expr.clone(),
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
