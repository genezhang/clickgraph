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
    /// 1. CTE variable? → forward CTE lookup (CTE column name)
    /// 2. Table variable? → schema lookup (DB column name)
    /// 3. Neither → Unresolved (caller falls back to existing logic)
    pub fn resolve(&self, alias: &str, cypher_property: &str) -> ResolvedProperty {
        // 1. Check CTE variables first (post-WITH scope)
        if let Some(cte_info) = self.cte_variables.get(alias) {
            if let Some(cte_col) = cte_info.property_mapping.get(cypher_property) {
                log::debug!(
                    "VariableScope: {}.{} → CTE column {}.{}",
                    alias,
                    cypher_property,
                    cte_info.cte_name,
                    cte_col
                );
                return ResolvedProperty::CteColumn {
                    cte_name: cte_info.cte_name.clone(),
                    column: cte_col.clone(),
                };
            }
            log::debug!(
                "VariableScope: {}.{} is CTE-scoped but property not found in mapping",
                alias,
                cypher_property
            );
            return ResolvedProperty::Unresolved;
        }

        // 2. Check table variables via schema
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
}
