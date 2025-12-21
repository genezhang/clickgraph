//! Property Expansion Utilities
//! 
//! Centralized functions for expanding node/edge aliases to their properties.
//! This consolidates the property expansion logic that was previously duplicated
//! across multiple locations in the codebase.
//!
//! ## Architecture Note
//!
//! RETURN and WITH clauses have identical structure per OpenCypher grammar:
//! - Both have: projection items + ORDER BY + SKIP + LIMIT
//! - Difference: WITH has optional WHERE clause
//! 
//! Current implementation uses two paths:
//! 1. RETURN: `expand_alias_to_properties()` → LogicalExpr/ProjectionItem
//! 2. WITH: `expand_alias_to_select_items()` → RenderExpr/SelectItem
//!
//! Future: Unify into single structure (see notes/return-with-unification.md)

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::query_planner::{
    logical_expr::{
        AggregateFnCall, ColumnAlias, LogicalExpr, PropertyAccess, 
        ScalarFnCall, TableAlias,
    },
    logical_plan::ProjectionItem,
};

use super::render_expr::{Column, PropertyAccess as RenderPropertyAccess, RenderExpr, TableAlias as RenderTableAlias};
use super::SelectItem;
use super::render_expr::ColumnAlias as RenderColumnAlias;

/// Configuration for how to format the expanded property aliases
#[derive(Clone, Debug)]
pub enum PropertyAliasFormat {
    /// Use underscore format: "alias_property" (for CTEs)
    Underscore,
    /// Use dot format: "alias.property" (for final SELECT)
    Dot,
    /// No prefix, just property name
    PropertyOnly,
}

/// Result of expanding a table alias to its properties
#[derive(Clone, Debug)]
pub struct ExpandedProperties {
    /// The projection items for each property
    pub items: Vec<ProjectionItem>,
    /// The table alias used for the properties (may differ from input for denormalized nodes)
    pub actual_table_alias: Option<String>,
}

/// Expand a table alias to projection items for all its properties
///
/// # Arguments
/// * `alias` - The table alias to expand (e.g., "p", "f")
/// * `properties` - Vec of (property_name, column_name) tuples from schema
/// * `actual_table_alias` - The actual table alias to use in SQL (for denormalized nodes)
/// * `alias_format` - How to format the column aliases
///
/// # Returns
/// Vector of ProjectionItems, one for each property
pub fn expand_alias_to_properties(
    alias: &str,
    properties: Vec<(String, String)>,
    actual_table_alias: Option<String>,
    alias_format: PropertyAliasFormat,
) -> Vec<ProjectionItem> {
    let table_alias_to_use = actual_table_alias
        .as_ref()
        .map(|s| TableAlias(s.clone()))
        .unwrap_or_else(|| TableAlias(alias.to_string()));

    properties
        .into_iter()
        .map(|(prop_name, col_name)| {
            let col_alias_name = match alias_format {
                PropertyAliasFormat::Underscore => format!("{}_{}", alias, prop_name),
                PropertyAliasFormat::Dot => format!("{}.{}", alias, prop_name),
                PropertyAliasFormat::PropertyOnly => prop_name.clone(),
            };

            ProjectionItem {
                expression: LogicalExpr::PropertyAccessExp(PropertyAccess {
                    table_alias: table_alias_to_use.clone(),
                    column: PropertyValue::Column(col_name),
                }),
                col_alias: Some(ColumnAlias(col_alias_name)),
            }
        })
        .collect()
}

/// Expand a table alias to SELECT items for all its properties (RenderExpr version)
///
/// This is the RenderExpr equivalent of `expand_alias_to_properties()`.
/// Used during WITH clause processing where we need SelectItem (RenderExpr) instead of
/// ProjectionItem (LogicalExpr).
///
/// # Arguments
/// * `alias` - The table alias to expand (e.g., "r", "u")
/// * `properties` - Vec of (property_name, column_name) tuples from schema
/// * `actual_table_alias` - The actual table alias to use in SQL (for denormalized nodes)
///
/// # Returns
/// Vector of SelectItems, one for each property, with underscore-format aliases
///
/// # Example
/// ```ignore
/// // For relationship "r" with properties [("from_id", "follower_id"), ("to_id", "followed_id")]
/// expand_alias_to_select_items("r", properties, None)
/// // Returns:
/// // [
/// //   SelectItem { expr: r.follower_id, alias: "r_from_id" },
/// //   SelectItem { expr: r.followed_id, alias: "r_to_id" }
/// // ]
/// ```
pub fn expand_alias_to_select_items(
    alias: &str,
    properties: Vec<(String, String)>,
    actual_table_alias: Option<String>,
) -> Vec<SelectItem> {
    let table_alias_to_use = actual_table_alias.unwrap_or_else(|| alias.to_string());

    properties
        .into_iter()
        .map(|(prop_name, col_name)| {
            SelectItem {
                expression: RenderExpr::PropertyAccessExp(RenderPropertyAccess {
                    table_alias: RenderTableAlias(table_alias_to_use.clone()),
                    column: Column(PropertyValue::Column(col_name)),
                }),
                col_alias: Some(RenderColumnAlias(format!("{}_{}", alias, prop_name))),
            }
        })
        .collect()
}

/// Expand a collect(node) aggregate to groupArray(tuple(properties...))
///
/// # Arguments
/// * `alias` - The node alias being collected (e.g., "f")
/// * `properties` - Vec of (property_name, column_name) tuples from schema
///
/// # Returns
/// LogicalExpr for groupArray(tuple(prop1, prop2, ...))
///
/// # TODO: Performance Optimization
/// Currently collects ALL properties, which is expensive for wide tables (100+ columns).
/// Should analyze downstream usage and collect only referenced properties.
/// See: notes/collect_unwind_optimization.md
/// - Optimization 1: Column projection (only collect used properties)
/// - Optimization 2: Detect collect+UNWIND no-ops and eliminate
/// Impact: 85-98% performance improvement for wide tables
pub fn expand_collect_to_group_array(
    alias: &str,
    properties: Vec<(String, String)>,
) -> LogicalExpr {
    // Create property access expressions for each property
    let prop_exprs: Vec<LogicalExpr> = properties
        .into_iter()
        .map(|(_prop_name, col_name)| {
            LogicalExpr::PropertyAccessExp(PropertyAccess {
                table_alias: TableAlias(alias.to_string()),
                column: PropertyValue::Column(col_name),
            })
        })
        .collect();

    // Create tuple(...) expression
    let tuple_expr = LogicalExpr::ScalarFnCall(ScalarFnCall {
        name: "tuple".to_string(),
        args: prop_exprs,
    });

    // Wrap in groupArray
    LogicalExpr::AggregateFnCall(AggregateFnCall {
        name: "groupArray".to_string(),
        args: vec![tuple_expr],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_alias_underscore_format() {
        let properties = vec![
            ("name".to_string(), "full_name".to_string()),
            ("age".to_string(), "age".to_string()),
        ];

        let items = expand_alias_to_properties(
            "p",
            properties,
            None,
            PropertyAliasFormat::Underscore,
        );

        assert_eq!(items.len(), 2);
        assert_eq!(items[0].col_alias.as_ref().unwrap().0, "p_name");
        assert_eq!(items[1].col_alias.as_ref().unwrap().0, "p_age");
    }

    #[test]
    fn test_expand_alias_dot_format() {
        let properties = vec![
            ("name".to_string(), "full_name".to_string()),
        ];

        let items = expand_alias_to_properties(
            "p",
            properties,
            None,
            PropertyAliasFormat::Dot,
        );

        assert_eq!(items[0].col_alias.as_ref().unwrap().0, "p.name");
    }

    #[test]
    fn test_expand_alias_with_actual_table_alias() {
        let properties = vec![
            ("name".to_string(), "full_name".to_string()),
        ];

        let items = expand_alias_to_properties(
            "p",
            properties,
            Some("edge_table".to_string()),
            PropertyAliasFormat::Underscore,
        );

        // Should use edge_table as the table alias
        if let LogicalExpr::PropertyAccessExp(prop) = &items[0].expression {
            assert_eq!(prop.table_alias.0, "edge_table");
        } else {
            panic!("Expected PropertyAccessExp");
        }
    }

    #[test]
    fn test_expand_collect_to_group_array() {
        let properties = vec![
            ("name".to_string(), "full_name".to_string()),
            ("age".to_string(), "age".to_string()),
        ];

        let expr = expand_collect_to_group_array("f", properties);

        // Should be AggregateFnCall(groupArray(...))
        if let LogicalExpr::AggregateFnCall(agg) = &expr {
            assert_eq!(agg.name, "groupArray");
            assert_eq!(agg.args.len(), 1);

            // First arg should be ScalarFnCall(tuple(...))
            if let LogicalExpr::ScalarFnCall(tuple_call) = &agg.args[0] {
                assert_eq!(tuple_call.name, "tuple");
                assert_eq!(tuple_call.args.len(), 2);
            } else {
                panic!("Expected tuple ScalarFnCall");
            }
        } else {
            panic!("Expected groupArray AggregateFnCall");
        }
    }
}
