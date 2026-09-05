//! Filter pipeline utilities for categorizing and processing filters

use super::render_expr::{Operator, OperatorApplication, RenderExpr};
use crate::graph_catalog::expression_parser::PropertyValue;
use crate::graph_catalog::graph_schema::GraphSchema;

/// Represents categorized filters for different parts of a query
///
/// This struct supports two modes:
/// 1. RenderExpr-based: Filters as AST (start_node_filters, end_node_filters, etc.)
/// 2. Pre-rendered SQL: Filters already rendered to SQL strings (start_sql, end_sql, etc.)
///
/// Pre-rendered SQL is used for backward compatibility with VariableLengthCteGenerator
/// during the transition to CteManager. Once migration is complete, we can remove
/// the pre-rendered SQL fields.
#[derive(Debug, Clone, Default)]
pub struct CategorizedFilters {
    // RenderExpr-based filters (preferred - supports re-rendering with different alias mappings)
    pub start_node_filters: Option<RenderExpr>,
    pub end_node_filters: Option<RenderExpr>,
    pub relationship_filters: Option<RenderExpr>,
    pub path_function_filters: Option<RenderExpr>,
    /// #1103: predicates referencing BOTH VLP endpoints (e.g. `friend <> root`,
    /// `friend.id <> root.id`). These are neither start-only nor end-only: they
    /// constrain the (start, FINAL endpoint) pair of a whole path, so they must
    /// be applied on the post-recursion wrapper — never in the base case (which
    /// sees only hop-1 rows) and never per-hop on the recursive arm (whose
    /// `end_node` is an INTERMEDIATE node, and which Cypher's TRAIL semantics
    /// allow to revisit any node). Kept separate from `start_node_filters` so
    /// the base case cannot pick them up.
    pub both_endpoint_filters: Option<RenderExpr>,

    // Pre-rendered SQL strings (for backward compatibility during CteManager transition)
    // These take precedence over RenderExpr when present
    pub start_sql: Option<String>,
    pub end_sql: Option<String>,
    pub relationship_sql: Option<String>,
    /// Pre-rendered SQL for [`Self::both_endpoint_filters`] (#1103).
    pub both_endpoint_sql: Option<String>,
}

/// Categorize filters based on which nodes/relationships they reference
///
/// This function properly separates WHERE clause predicates into:
/// - start_node_filters: `WHERE a.prop = value` (start node)
/// - end_node_filters: `WHERE b.prop = value` (end node)
/// - relationship_filters: `WHERE r.prop = value` (relationship)
/// - path_function_filters: `WHERE length(p) < 5` (path functions)
///
/// ⚠️ CRITICAL (Jan 10, 2026): Schema-aware categorization for ALL schema variations!
///
/// For denormalized edge tables, BOTH node and edge properties have the same table alias (rel alias).
/// After property mapping: origin.code → f.Origin, dest.code → f.Dest (both use 'f' alias)
/// We CANNOT categorize by table alias alone!
///
/// Solution: Check the COLUMN NAME against schema property mappings:
/// - from_node_properties (e.g., Origin, OriginCity) → start_node_filters
/// - to_node_properties (e.g., Dest, DestCity) → end_node_filters  
/// - property_mappings in edge schema → relationship_filters
pub fn categorize_filters(
    filter_expr: Option<&RenderExpr>,
    start_cypher_alias: &str,
    end_cypher_alias: &str,
    rel_alias: &str,
    schema: &GraphSchema,
    rel_labels: &[String], // Relationship type(s) to check schema
) -> CategorizedFilters {
    log::debug!(
        "Categorizing filters for start alias '{}', end alias '{}', rel alias '{}', rel_labels: {:?}",
        start_cypher_alias,
        end_cypher_alias,
        rel_alias,
        rel_labels
    );

    let mut result = CategorizedFilters {
        start_node_filters: None,
        end_node_filters: None,
        relationship_filters: None,
        path_function_filters: None,
        both_endpoint_filters: None,
        start_sql: None,
        end_sql: None,
        relationship_sql: None,
        both_endpoint_sql: None,
    };

    if filter_expr.is_none() {
        log::trace!("No filter expression provided");
        return result;
    }

    log::trace!("Filter expression: {:?}", filter_expr.unwrap());

    let filter = filter_expr.unwrap();

    // Helper to check if column belongs to from_node_properties, to_node_properties, or edge properties
    // This is CRITICAL for denormalized edges where all properties share the same table alias!
    fn check_column_ownership(
        column_name: &str,
        rel_labels: &[String],
        schema: &GraphSchema,
    ) -> ColumnOwnership {
        // Try each relationship label
        for rel_label in rel_labels {
            if let Ok(rel_schema) = schema.get_rel_schema(rel_label) {
                // Check from_node_properties (start node)
                if let Some(from_props) = &rel_schema.from_node_properties {
                    if from_props.values().any(|col| col == column_name) {
                        log::debug!(
                            "Column '{}' found in from_node_properties → start node",
                            column_name
                        );
                        return ColumnOwnership::FromNode;
                    }
                }

                // Check to_node_properties (end node)
                if let Some(to_props) = &rel_schema.to_node_properties {
                    if to_props.values().any(|col| col == column_name) {
                        log::debug!(
                            "Column '{}' found in to_node_properties → end node",
                            column_name
                        );
                        return ColumnOwnership::ToNode;
                    }
                }

                // Check property_mappings (relationship) - these are PropertyValue
                for col_value in rel_schema.property_mappings.values() {
                    if col_value.raw() == column_name {
                        log::debug!(
                            "Column '{}' found in property_mappings → relationship",
                            column_name
                        );
                        return ColumnOwnership::Relationship;
                    }
                }
            }
        }

        log::debug!(
            "Column '{}' ownership unknown, defaulting to relationship",
            column_name
        );
        ColumnOwnership::Unknown
    }

    #[derive(Debug, PartialEq)]
    enum ColumnOwnership {
        FromNode,
        ToNode,
        Relationship,
        Unknown,
    }

    // Helper to check if an expression references a specific alias (checks both Cypher and SQL aliases)
    fn references_alias(expr: &RenderExpr, cypher_alias: &str, sql_alias: &str) -> bool {
        match expr {
            RenderExpr::PropertyAccessExp(prop) => {
                let table_alias = &prop.table_alias.0;
                table_alias == cypher_alias || table_alias == sql_alias
            }
            RenderExpr::OperatorApplicationExp(op) => op
                .operands
                .iter()
                .any(|operand| references_alias(operand, cypher_alias, sql_alias)),
            _ => false,
        }
    }

    // Helper to check if an expression contains path function calls
    fn contains_path_function(expr: &RenderExpr) -> bool {
        match expr {
            RenderExpr::ScalarFnCall(fn_call) => {
                // Check if this is a path function (length, nodes, relationships)
                matches!(
                    fn_call.name.to_lowercase().as_str(),
                    "length" | "nodes" | "relationships"
                )
            }
            RenderExpr::OperatorApplicationExp(op) => {
                op.operands.iter().any(contains_path_function)
            }
            _ => false,
        }
    }

    // Split AND-connected filters into individual predicates
    fn split_and_filters(expr: &RenderExpr) -> Vec<RenderExpr> {
        match expr {
            RenderExpr::OperatorApplicationExp(op) if matches!(op.operator, Operator::And) => {
                let mut filters = Vec::new();
                for operand in &op.operands {
                    filters.extend(split_and_filters(operand));
                }
                filters
            }
            _ => vec![expr.clone()],
        }
    }

    // Split the filter into individual predicates
    let predicates = split_and_filters(filter);

    let mut start_filters = Vec::new();
    let mut end_filters = Vec::new();
    let mut rel_filters = Vec::new();
    let mut path_fn_filters = Vec::new();
    let mut both_endpoint_filters = Vec::new();

    for predicate in predicates {
        let refs_start = references_alias(&predicate, start_cypher_alias, "start_node");
        let refs_end = references_alias(&predicate, end_cypher_alias, "end_node");
        let refs_rel = if !rel_alias.is_empty() {
            references_alias(&predicate, rel_alias, "rel")
        } else {
            false
        };
        let has_path_fn = contains_path_function(&predicate);

        // ⚠️ CRITICAL: For denormalized edges, check column ownership!
        // If predicate references rel_alias, check if column belongs to from/to node or relationship
        let column_ownership = if refs_rel && !rel_labels.is_empty() {
            // Extract column name from predicate
            if let Some(column_name) = extract_column_name(&predicate) {
                check_column_ownership(&column_name, rel_labels, schema)
            } else {
                ColumnOwnership::Unknown
            }
        } else {
            ColumnOwnership::Unknown
        };

        crate::debug_println!("DEBUG: Categorizing predicate: {:?}", predicate);
        log::debug!(
            "Categorize predicate - refs_start: {}, refs_end: {}, refs_rel: {}, column_ownership: {:?}, has_path_fn: {}",
            refs_start, refs_end, refs_rel, column_ownership, has_path_fn
        );

        if has_path_fn {
            // Path function filters (e.g., WHERE length(p) <= 3) go in path function filters
            crate::debug_println!("DEBUG: Going to path_fn_filters");
            path_fn_filters.push(predicate);
        } else if refs_rel && column_ownership == ColumnOwnership::FromNode {
            // Column belongs to from_node_properties → start node filter
            crate::debug_println!(
                "DEBUG: Going to start_filters (denormalized from_node property)"
            );
            log::debug!("  -> start_node_filters (column in from_node_properties)");
            start_filters.push(predicate);
        } else if refs_rel && column_ownership == ColumnOwnership::ToNode {
            // Column belongs to to_node_properties → end node filter
            crate::debug_println!("DEBUG: Going to end_filters (denormalized to_node property)");
            log::debug!("  -> end_node_filters (column in to_node_properties)");
            end_filters.push(predicate);
        } else if refs_rel && column_ownership == ColumnOwnership::Relationship {
            // Column belongs to relationship property_mappings → relationship filter
            crate::debug_println!("DEBUG: Going to rel_filters (edge property)");
            log::debug!("  -> relationship_filters (column in property_mappings)");
            rel_filters.push(predicate);
        } else if refs_rel {
            // refs_rel but ownership unknown (fallback for non-denormalized or missing schema)
            crate::debug_println!(
                "DEBUG: Going to rel_filters (references relationship alias, ownership unknown)"
            );
            log::debug!(
                "  -> relationship_filters (refs rel alias '{}', ownership unknown)",
                rel_alias
            );
            rel_filters.push(predicate);
        } else if refs_start && refs_end && start_cypher_alias != end_cypher_alias {
            // #1103: references BOTH endpoints. This is a whole-path predicate
            // on the (start, FINAL endpoint) pair — it belongs in neither the
            // base case nor the per-hop recursive arm. Routing it to
            // `start_filters` (the pre-#1103 behavior) put it in the base case
            // only, which was wrong in both directions: it pruned valid paths
            // whose INTERMEDIATE hop-1 node failed the predicate, and it let
            // paths whose FINAL endpoint fails the predicate survive past hop 1.
            // Its own category defers it to the post-recursion wrapper.
            crate::debug_println!("DEBUG: Going to both_endpoint_filters (refs both)");
            log::debug!("  -> both_endpoint_filters (references start AND end)");
            both_endpoint_filters.push(predicate);
        } else if refs_start {
            // NOTE (#1103): a CLOSED pattern `(a)-[*]->(a)` has
            // start_cypher_alias == end_cypher_alias, so `refs_start` and
            // `refs_end` are the SAME test on ONE variable — not a two-endpoint
            // comparison. The guard above excludes it so `a.prop = v` keeps its
            // pre-#1103 start-filter placement (base case), which is correct:
            // the closed pattern's endpoint identity is enforced structurally,
            // not by this predicate.
            crate::debug_println!("DEBUG: Going to start_filters");
            start_filters.push(predicate);
        } else if refs_end {
            crate::debug_println!("DEBUG: Going to end_filters");
            end_filters.push(predicate);
        } else {
            // Doesn't reference any known alias - might be a constant or unrelated
            // ✅ HOLISTIC FIX: Previously we put uncategorized filters here, which was wrong
            crate::debug_println!(
                "DEBUG: Uncategorized predicate (no alias match), treating as rel filter"
            );
            log::warn!(
                "Filter predicate doesn't match any known alias: {:?}",
                predicate
            );
            rel_filters.push(predicate);
        }
    }

    // Helper to extract column name from a predicate (e.g., Origin from f.Origin = 'LAX')
    fn extract_column_name(expr: &RenderExpr) -> Option<String> {
        match expr {
            RenderExpr::PropertyAccessExp(prop) => {
                // PropertyAccess.column is directly a PropertyValue
                match &prop.column {
                    PropertyValue::Column(s) => Some(s.clone()),
                    PropertyValue::Expression(s) => Some(s.clone()),
                }
            }
            RenderExpr::OperatorApplicationExp(op) => {
                // For comparison operators, check first operand (usually the property access)
                if matches!(
                    op.operator,
                    Operator::Equal
                        | Operator::NotEqual
                        | Operator::LessThan
                        | Operator::LessThanEqual
                        | Operator::GreaterThan
                        | Operator::GreaterThanEqual
                ) {
                    if let Some(first) = op.operands.first() {
                        return extract_column_name(first);
                    }
                }
                // For AND/OR, recursively check operands
                for operand in &op.operands {
                    if let Some(col) = extract_column_name(operand) {
                        return Some(col);
                    }
                }
                None
            }
            _ => None,
        }
    }

    // Combine filters with AND
    fn combine_with_and(filters: Vec<RenderExpr>) -> Option<RenderExpr> {
        if filters.is_empty() {
            return None;
        }
        if filters.len() == 1 {
            return Some(filters.into_iter().next().unwrap());
        }
        Some(RenderExpr::OperatorApplicationExp(OperatorApplication {
            operator: Operator::And,
            operands: filters,
        }))
    }

    result.both_endpoint_filters = combine_with_and(both_endpoint_filters);
    result.start_node_filters = combine_with_and(start_filters);
    result.end_node_filters = combine_with_and(end_filters);
    result.relationship_filters = combine_with_and(rel_filters);
    result.path_function_filters = combine_with_and(path_fn_filters);

    log::trace!("Filter categorization result:");
    log::trace!("  Start filters: {:?}", result.start_node_filters);
    log::trace!("  End filters: {:?}", result.end_node_filters);
    log::trace!("  Rel filters: {:?}", result.relationship_filters);
    log::trace!(
        "  Path function filters: {:?}",
        result.path_function_filters
    );

    result
}
