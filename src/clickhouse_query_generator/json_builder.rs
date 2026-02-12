//! JSON Builder - Type-preserving JSON construction for ClickHouse
//!
//! This module provides utilities to generate ClickHouse SQL that produces
//! type-preserving JSON strings using `formatRowNoNewline('JSONEachRow', ...)`.
//!
//! Unlike `toJSONString(map(...))` which requires all values to be strings,
//! `formatRowNoNewline` preserves native types (integers, booleans, dates, etc.).
//!
//! # Example
//!
//! ```sql
//! -- Old approach (loses types):
//! SELECT toJSONString(map('user_id', toString(user_id), 'is_active', toString(is_active)))
//! -- Result: {"user_id":"1","is_active":"1"}
//!
//! -- New approach (preserves types):
//! SELECT formatRowNoNewline('JSONEachRow', user_id, is_active)  
//! -- Result: {"user_id":1,"is_active":1}
//! ```

use crate::graph_catalog::expression_parser::PropertyValue;
use crate::graph_catalog::graph_schema::NodeSchema;
use std::collections::HashMap;

/// Generate SQL for type-preserving JSON properties using formatRowNoNewline
///
/// This function generates ClickHouse SQL that produces a JSON string with proper types
/// using `formatRowNoNewline('JSONEachRow', col1, col2, ...)`.
///
/// # Arguments
///
/// * `property_mappings` - Map of Cypher property names to PropertyValue (column references)
/// * `table_alias` - SQL table alias to use for column references (e.g., "n", "a", "t1")
///
/// # Returns
///
/// SQL expression string like: `formatRowNoNewline('JSONEachRow', n.col1, n.col2, ...)`
///
/// # Example
///
/// ```ignore
/// // Example usage (not runnable in doctest):
/// let mappings = hashmap!{
///     "user_id" => PropertyValue::Column("user_id"),
///     "name" => PropertyValue::Column("full_name"),
///     "is_active" => PropertyValue::Column("is_active"),
/// };
/// let sql = generate_json_properties_sql(&mappings, "n");
/// // Result: "formatRowNoNewline('JSONEachRow', n.user_id AS user_id, n.full_name AS name, n.is_active AS is_active)"
/// ```
pub fn generate_json_properties_sql(
    property_mappings: &HashMap<String, PropertyValue>,
    table_alias: &str,
) -> String {
    if property_mappings.is_empty() {
        return "'{}'".to_string(); // Empty JSON object
    }

    let mut columns = Vec::new();
    for (cypher_prop, prop_value) in property_mappings {
        let column_name = match prop_value {
            PropertyValue::Column(col) => col.clone(),
            _ => continue, // Skip non-column property mappings (expressions, etc.)
        };

        // Format: table_alias.column_name AS cypher_property_name
        // The AS clause ensures the JSON uses Cypher property names, not ClickHouse column names
        columns.push(format!(
            "{}.{} AS {}",
            table_alias, column_name, cypher_prop
        ));
    }

    if columns.is_empty() {
        return "'{}'".to_string(); // No valid columns
    }

    // formatRowNoNewline('JSONEachRow', col1, col2, ...) produces type-preserving JSON
    format!("formatRowNoNewline('JSONEachRow', {})", columns.join(", "))
}

/// Generate SQL for type-preserving JSON properties from a NodeSchema
///
/// Convenience wrapper that extracts property mappings from a NodeSchema.
///
/// # Arguments
///
/// * `node_schema` - The node schema containing property mappings
/// * `table_alias` - SQL table alias to use for column references
///
/// # Returns
///
/// SQL expression string for formatRowNoNewline
pub fn generate_json_properties_from_schema(node_schema: &NodeSchema, table_alias: &str) -> String {
    generate_json_properties_sql(&node_schema.property_mappings, table_alias)
}

/// Generate JSON properties with prefixed aliases to avoid conflicts in UNION ALL
///
/// Generate SQL for type-preserving JSON without aliases (for CTEs)
///
/// Uses unqualified column references without AS aliases. When the SELECT has
/// JOINs with same-named columns, ClickHouse may prefix the table alias in the
/// JSON key (e.g., `a_1.user_id` instead of `user_id`). The result transformer
/// handles this by stripping table alias prefixes.
///
/// We avoid AS aliases here because ClickHouse treats AS inside formatRowNoNewline
/// as SELECT-level aliases, which conflict when both start_properties and
/// end_properties reference same-named columns (User→User JOINs).
///
/// # Arguments
///
/// * `property_mappings` - Map of Cypher property names to PropertyValue
/// * `table_alias` - SQL table alias to use for column references
///
/// # Returns
///
/// SQL expression: `formatRowNoNewline('JSONEachRow', t.col1, t.col2, ...)`
pub fn generate_json_properties_without_aliases(
    property_mappings: &HashMap<String, PropertyValue>,
    table_alias: &str,
) -> String {
    if property_mappings.is_empty() {
        return "'{}'".to_string();
    }

    let mut columns = Vec::new();
    for prop_value in property_mappings.values() {
        let column_name = match prop_value {
            PropertyValue::Column(col) => col.clone(),
            _ => continue,
        };

        // No AS clause — see doc comment for why
        columns.push(format!("{}.{}", table_alias, column_name));
    }

    if columns.is_empty() {
        return "'{}'".to_string();
    }

    format!("formatRowNoNewline('JSONEachRow', {})", columns.join(", "))
}

/// Generate SQL for type-preserving JSON from NodeSchema without aliases (for CTEs)
///
/// Convenience wrapper for generate_json_properties_without_aliases.
pub fn generate_json_properties_from_schema_without_aliases(
    node_schema: &NodeSchema,
    table_alias: &str,
) -> String {
    generate_json_properties_without_aliases(&node_schema.property_mappings, table_alias)
}

/// Generate JSON properties from denormalized node property mappings.
///
/// Denormalized nodes have `from_node_properties` / `to_node_properties` on the
/// relationship schema (HashMap<String, String>: cypher_name → physical_column),
/// instead of `property_mappings` (HashMap<String, PropertyValue>).
///
/// Uses `formatRowNoNewline('JSONEachRow', alias.col AS prefix_cypher_name, ...)` to produce
/// JSON with prefixed Cypher property names as keys. The prefix (e.g., `_s_`, `_e_`) avoids
/// duplicate expression alias errors in ClickHouse when start and end properties share names.
/// The transformer strips these prefixes when building node properties.
pub fn generate_json_from_denormalized_properties(
    denorm_props: &HashMap<String, String>,
    table_alias: &str,
    key_prefix: &str,
) -> String {
    if denorm_props.is_empty() {
        return "'{}'".to_string();
    }

    let columns: Vec<String> = denorm_props
        .iter()
        .map(|(cypher_name, physical_col)| {
            format!(
                "{}.{} AS {}{}",
                table_alias, physical_col, key_prefix, cypher_name
            )
        })
        .collect();

    format!("formatRowNoNewline('JSONEachRow', {})", columns.join(", "))
}

/// Generate SQL for a UNION query across all node types
///
/// Creates a UNION ALL query that returns (_label, _id, _properties) for all node types.
/// This is used for label-less queries like `MATCH (n) RETURN n`.
///
/// # Arguments
///
/// * `all_node_schemas` - Map of all node schemas (label -> NodeSchema)
/// * `limit` - Optional LIMIT clause value
///
/// # Returns
///
/// SQL string with UNION of all node types
///
/// # Example Output
///
/// ```sql
/// SELECT 'User' as _label, toString(user_id) as _id,
///        formatRowNoNewline('JSONEachRow', user_id AS user_id, full_name AS name, ...) as _properties
/// FROM users_bench
/// UNION ALL
/// SELECT 'Post' as _label, toString(post_id) as _id,
///        formatRowNoNewline('JSONEachRow', post_id AS post_id, content AS content, ...) as _properties
/// FROM posts_bench
/// LIMIT 25
/// ```
pub fn generate_multi_type_union_sql(
    all_node_schemas: &HashMap<String, NodeSchema>,
    limit: Option<usize>,
) -> String {
    let mut branches = Vec::new();
    let mut seen_labels = std::collections::HashSet::new();

    for (label, node_schema) in all_node_schemas {
        // Extract the base label (remove database::table:: prefix if present)
        let base_label = if label.contains("::") {
            label.split("::").last().unwrap_or(label)
        } else {
            label
        };

        // Skip if we've already added this base label (dedup qualified and simple keys)
        if !seen_labels.insert(base_label.to_string()) {
            log::debug!(
                "json_builder: Skipping duplicate label '{}' (already have '{}')",
                label,
                base_label
            );
            continue;
        }

        // Get node ID column
        let node_id_col = match &node_schema.node_id.id {
            crate::graph_catalog::config::Identifier::Single(column) => column.clone(),
            crate::graph_catalog::config::Identifier::Composite(columns) => {
                // For composite IDs, concatenate with pipe separator
                format!(
                    "concat({})",
                    columns
                        .iter()
                        .map(|c| format!("toString({})", c))
                        .collect::<Vec<_>>()
                        .join(", '|', ")
                )
            }
        };

        // Build the SELECT for this node type
        let table_ref = format!("{}.{}", node_schema.database, node_schema.table_name);
        let json_props = generate_json_properties_from_schema(node_schema, &node_schema.table_name);

        branches.push(format!(
            "SELECT '{}' as _label, toString({}) as _id, {} as _properties FROM {}",
            base_label, node_id_col, json_props, table_ref
        ));
    }

    let mut sql = branches.join("\nUNION ALL\n");

    if let Some(limit_val) = limit {
        sql.push_str(&format!("\nLIMIT {}", limit_val));
    }

    sql
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_catalog::expression_parser::PropertyValue;
    use std::collections::HashMap;

    #[test]
    fn test_generate_json_properties_sql() {
        let mut mappings = HashMap::new();
        mappings.insert(
            "user_id".to_string(),
            PropertyValue::Column("user_id".to_string()),
        );
        mappings.insert(
            "name".to_string(),
            PropertyValue::Column("full_name".to_string()),
        );
        mappings.insert(
            "is_active".to_string(),
            PropertyValue::Column("is_active".to_string()),
        );

        let sql = generate_json_properties_sql(&mappings, "n");

        // Should contain formatRowNoNewline
        assert!(sql.contains("formatRowNoNewline('JSONEachRow'"));

        // Should map Cypher properties to ClickHouse columns with AS clauses
        assert!(sql.contains("n.user_id AS user_id"));
        assert!(sql.contains("n.full_name AS name"));
        assert!(sql.contains("n.is_active AS is_active"));
    }

    #[test]
    fn test_empty_properties() {
        let mappings = HashMap::new();
        let sql = generate_json_properties_sql(&mappings, "n");
        assert_eq!(sql, "'{}'");
    }

    #[test]
    fn test_skip_non_column_properties() {
        let mut mappings = HashMap::new();
        mappings.insert(
            "user_id".to_string(),
            PropertyValue::Column("user_id".to_string()),
        );
        // Expression properties should be skipped
        mappings.insert(
            "computed".to_string(),
            PropertyValue::Expression("1 + 1".to_string()),
        );

        let sql = generate_json_properties_sql(&mappings, "n");

        assert!(sql.contains("n.user_id AS user_id"));
        assert!(!sql.contains("computed"));
    }
}
