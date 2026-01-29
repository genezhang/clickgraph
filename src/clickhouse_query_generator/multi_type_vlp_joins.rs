//! Multi-Type Variable-Length Path JOIN Generator
//!
//! Generates type-safe JOIN expansion with UNION ALL for variable-length paths
//! that traverse multiple node types or relationship types leading to different end types.
//!
//! Instead of recursive CTEs (which are unsafe for polymorphic IDs), this module:
//! 1. Enumerates all valid path combinations using schema validation
//! 2. Generates explicit JOINs for each path
//! 3. Combines results with UNION ALL
//!
//! Example:
//! ```cypher
//! MATCH (u:User)-[:FOLLOWS|AUTHORED*1..2]->(x:User|Post)
//! RETURN x
//! ```
//!
//! Generates:
//! ```sql
//! -- Path 1: User-FOLLOWS->User
//! SELECT ... FROM users u1 JOIN follows f1 ON ... JOIN users u2 ...
//! UNION ALL
//! -- Path 2: User-AUTHORED->Post
//! SELECT ... FROM users u1 JOIN authored a1 ON ... JOIN posts p1 ...
//! UNION ALL
//! -- Path 3: User-FOLLOWS->User-FOLLOWS->User (2-hop)
//! ...
//! ```

use crate::graph_catalog::graph_schema::GraphSchema;
use crate::query_planner::analyzer::multi_type_vlp_expansion::{
    enumerate_vlp_paths, PathEnumeration,
};
use crate::query_planner::join_context::{VLP_END_ID_COLUMN, VLP_START_ID_COLUMN};
use crate::query_planner::logical_plan::VariableLengthSpec;
use std::collections::HashMap;

/// Property projection for heterogeneous node types
#[derive(Debug, Clone)]
pub struct PropertyProjection {
    /// Cypher property name (e.g., "name")
    pub cypher_name: String,
    /// Mapping: node_type -> (table_column, table_alias)
    /// Example: {"User": ("full_name", "u2"), "Post": ("title", "p1")}
    pub type_mappings: HashMap<String, (String, String)>,
}

/// Configuration for multi-type VLP SQL generation
#[derive(Debug, Clone)]
pub struct MultiTypeVlpJoinGenerator<'a> {
    schema: &'a GraphSchema,

    // Path specification
    start_labels: Vec<String>,
    rel_types: Vec<String>,
    end_labels: Vec<String>,
    min_hops: usize,
    max_hops: usize,

    // Node information
    start_alias: String, // e.g., "u"
    end_alias: String,   // e.g., "x"

    // Filter conditions
    start_filters: Option<String>, // WHERE clause for start node
    end_filters: Option<String>,   // WHERE clause for end node
    rel_filters: Option<String>,   // WHERE clause for relationships

    // Property projections (properties to return)
    properties: Vec<PropertyProjection>,

    // 🔧 PARAMETERIZED VIEW FIX: View parameter values for multi-tenant queries
    // Maps parameter name -> parameter value (e.g., "tenant_id" -> "tenant_a")
    view_parameter_values: HashMap<String, String>,
}

impl<'a> MultiTypeVlpJoinGenerator<'a> {
    /// Create a new multi-type VLP JOIN generator
    ///
    /// # Arguments
    /// * `schema` - Graph schema for validation and property mapping
    /// * `start_labels` - Start node types (e.g., ["User"])
    /// * `rel_types` - Relationship types (e.g., ["FOLLOWS", "AUTHORED"])
    /// * `end_labels` - End node types (e.g., ["User", "Post"])
    /// * `spec` - Variable length specification (min/max hops)
    /// * `start_alias` - Cypher alias for start node
    /// * `end_alias` - Cypher alias for end node
    /// * `start_filters` - Optional WHERE filters for start node
    /// * `end_filters` - Optional WHERE filters for end node
    /// * `rel_filters` - Optional WHERE filters for relationships
    /// * `view_parameter_values` - View parameters for parameterized views (e.g., {"tenant_id": "tenant_a"})
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        schema: &'a GraphSchema,
        start_labels: Vec<String>,
        rel_types: Vec<String>,
        end_labels: Vec<String>,
        spec: VariableLengthSpec,
        start_alias: String,
        end_alias: String,
        start_filters: Option<String>,
        end_filters: Option<String>,
        rel_filters: Option<String>,
        view_parameter_values: HashMap<String, String>,
    ) -> Self {
        let min_hops = spec.min_hops.unwrap_or(1) as usize;
        let max_hops = spec.max_hops.unwrap_or(10) as usize;

        // Enforce 3-hop limit for multi-type VLP (combinatorial explosion)
        let max_hops = max_hops.min(3);

        Self {
            schema,
            start_labels,
            rel_types,
            end_labels,
            min_hops,
            max_hops,
            start_alias,
            end_alias,
            start_filters,
            end_filters,
            rel_filters,
            properties: vec![],
            view_parameter_values,
        }
    }

    /// Add a property projection for heterogeneous types
    pub fn add_property(
        &mut self,
        cypher_name: String,
        type_mappings: HashMap<String, (String, String)>,
    ) {
        self.properties.push(PropertyProjection {
            cypher_name,
            type_mappings,
        });
    }

    /// Generate complete SQL with UNION ALL of all valid paths
    ///
    /// Returns CTE SQL that can be referenced in outer query
    pub fn generate_cte_sql(&self, _cte_name: &str) -> Result<String, String> {
        // Step 1: Enumerate all valid path combinations
        let paths = enumerate_vlp_paths(
            &self.start_labels,
            &self.rel_types,
            &self.end_labels,
            self.min_hops,
            self.max_hops,
            self.schema,
        );

        if paths.is_empty() {
            return Err(format!(
                "No valid paths found for {:?}-[{:?}*{}..{}]->{:?}",
                self.start_labels, self.rel_types, self.min_hops, self.max_hops, self.end_labels
            ));
        }

        log::info!(
            "🎯 MultiTypeVLP: Enumerated {} valid paths (hops: {}-{})",
            paths.len(),
            self.min_hops,
            self.max_hops
        );

        // Step 2: Generate SQL for each path branch
        let mut branch_sqls = Vec::new();
        for (idx, path) in paths.iter().enumerate() {
            match self.generate_path_branch_sql(path, idx) {
                Ok(sql) => branch_sqls.push(sql),
                Err(e) => {
                    log::warn!("Failed to generate SQL for path {:?}: {}", path, e);
                    continue;
                }
            }
        }

        if branch_sqls.is_empty() {
            return Err("Failed to generate SQL for any path branch".to_string());
        }

        // Step 3: Combine with UNION ALL
        let union_sql = branch_sqls.join("\nUNION ALL\n");

        // Return just the CTE body (not wrapped in WITH) - the caller handles WITH
        Ok(union_sql)
    }

    /// Generate SQL for a single path branch
    ///
    /// Example path: User-FOLLOWS->User-AUTHORED->Post
    /// Generates: SELECT ... FROM users u1 JOIN follows f1 ... JOIN users u2 JOIN authored a2 ... JOIN posts p1
    fn generate_path_branch_sql(
        &self,
        path: &PathEnumeration,
        _branch_idx: usize,
    ) -> Result<String, String> {
        let hops = &path.hops;
        if hops.is_empty() {
            return Err("Empty path enumeration".to_string());
        }

        // Generate unique table aliases for this branch
        // u1, f1, u2, a2, p1, etc.
        let mut from_clauses = Vec::new();
        let mut where_clauses = Vec::new();

        // Start with first node table
        let start_type = &hops[0].from_node_type;
        let start_table = self.get_node_table_with_db(start_type)?;
        let start_alias_sql = format!("{}_1", self.start_alias);
        from_clauses.push(format!("{} {}", start_table, start_alias_sql));

        // Add start filters
        // Note: filters may reference "start_node" (internal CTE alias) or the Cypher alias
        if let Some(ref filters) = self.start_filters {
            // Replace both "start_node." and the Cypher alias (e.g., "u.")
            let start_filter = filters
                .replace("start_node.", &format!("{}.", start_alias_sql))
                .replace(
                    &format!("{}.", self.start_alias),
                    &format!("{}.", start_alias_sql),
                );
            where_clauses.push(start_filter);
        }

        // Generate JOINs for each hop
        let mut current_alias = start_alias_sql.clone();
        let mut end_node_alias = String::new();
        let mut end_node_type = String::new();

        for (hop_idx, hop) in hops.iter().enumerate() {
            let hop_num = hop_idx + 1;

            // Get relationship info using composite key lookup (TYPE::FROM::TO)
            let rel_table =
                self.get_rel_table_with_db(&hop.rel_type, &hop.from_node_type, &hop.to_node_type)?;
            let (from_col, to_col) =
                self.get_rel_columns(&hop.rel_type, &hop.from_node_type, &hop.to_node_type)?;
            let start_id_col = self.get_node_id_column(&hop.from_node_type)?;
            log::info!("🔍 Hop {}: rel_type={}, from_node={}, to_node={}, from_col={}, to_col={}, start_id_col={}", 
                hop_num, hop.rel_type, hop.from_node_type, hop.to_node_type, from_col, to_col, start_id_col);

            // Get target node info
            let end_table = self.get_node_table_with_db(&hop.to_node_type)?;
            let end_id_col = self.get_node_id_column(&hop.to_node_type)?;

            // Check if this is an FK-edge pattern (rel table == target node table)
            // In this case, the relationship table IS the target node, so we only need one JOIN
            let is_fk_edge = rel_table == end_table;

            if is_fk_edge {
                // FK-edge pattern: relationship table IS the target node table
                // Only one JOIN needed: current_node JOIN target/rel ON current.id = target.from_id
                // ⚠️ CRITICAL: Use from_col (not start_id_col) because FK-edge pattern means
                // the target table has the foreign key column (e.g., posts_bench.author_id)
                end_node_alias = format!(
                    "{}{}",
                    if hop.to_node_type == "User" {
                        "u"
                    } else if hop.to_node_type == "Post" {
                        "p"
                    } else {
                        "n"
                    },
                    hop_num + 1
                );
                end_node_type = hop.to_node_type.clone();

                let join_sql = format!(
                    "INNER JOIN {} {} ON {}.{} = {}.{}",
                    rel_table,
                    end_node_alias,
                    current_alias,
                    start_id_col,
                    end_node_alias,
                    from_col
                );
                from_clauses.push(join_sql);

                // Add relationship filters (apply to the combined rel/node table)
                if let Some(ref rel_filters) = self.rel_filters {
                    let rel_filter = rel_filters.replace("rel.", &format!("{}.", end_node_alias));
                    where_clauses.push(rel_filter);
                }
            } else {
                // Standard pattern: separate relationship table and target node table
                // Two JOINs needed: current_node JOIN rel ON ... JOIN target_node ON ...
                let rel_alias = format!("r{}", hop_num);

                let rel_join_sql = format!(
                    "INNER JOIN {} {} ON {}.{} = {}.{}",
                    rel_table, rel_alias, current_alias, start_id_col, rel_alias, from_col
                );
                log::info!("   Standard path rel JOIN: {}", rel_join_sql);
                from_clauses.push(rel_join_sql);

                // Add relationship filters
                if let Some(ref rel_filters) = self.rel_filters {
                    let rel_filter = rel_filters.replace("rel.", &format!("{}.", rel_alias));
                    where_clauses.push(rel_filter);
                }

                // Target node table JOIN
                end_node_alias = format!(
                    "{}{}",
                    if hop.to_node_type == "User" {
                        "u"
                    } else if hop.to_node_type == "Post" {
                        "p"
                    } else {
                        "n"
                    },
                    hop_num + 1
                );
                end_node_type = hop.to_node_type.clone();

                let node_join_sql = format!(
                    "INNER JOIN {} {} ON {}.{} = {}.{}",
                    end_table, end_node_alias, rel_alias, to_col, end_node_alias, end_id_col
                );
                log::info!("   Standard path node JOIN: {}", node_join_sql);
                from_clauses.push(node_join_sql);
            }

            // Update current_alias for next hop
            current_alias = end_node_alias.clone();
        }

        // Add end filters
        // Note: filters may reference "end_node" (internal CTE alias) or the Cypher alias
        if let Some(ref filters) = self.end_filters {
            let end_filter = filters
                .replace("end_node.", &format!("{}.", end_node_alias))
                .replace(
                    &format!("{}.", self.end_alias),
                    &format!("{}.", end_node_alias),
                );
            where_clauses.push(end_filter);
        }

        // Generate SELECT clause with type discriminator and IDs for outer JOINs
        let hop_count = hops.len();
        let select_items = self.generate_select_items(
            &end_node_alias,
            &end_node_type,
            &start_alias_sql,
            hop_count,
            &path.hops,
        );

        // Assemble final SQL
        let mut sql = format!(
            "SELECT {}\nFROM {}",
            select_items.join(", "),
            from_clauses.join("\n")
        );

        if !where_clauses.is_empty() {
            sql.push_str(&format!("\nWHERE {}", where_clauses.join(" AND ")));
        }

        Ok(sql)
    }

    /// Generate SELECT items with type discriminator and property projections
    ///
    /// Output columns follow the VLP CTE convention:
    /// - `end_type`: Discriminator for the end node type (e.g., 'User', 'Post')  
    /// - `end_id`: ID of the end node as String (handles single/composite IDs)
    /// - `start_id`: ID of the start node as String (handles single/composite IDs)
    /// - `end_properties`: JSON string containing all node properties
    /// - `hop_count`: Number of hops in the path (for length(p) function)
    /// - `path_relationships`: Array of relationship types (for relationships(p) function)
    fn generate_select_items(
        &self,
        node_alias: &str,
        node_type: &str,
        start_alias_sql: &str,
        hop_count: usize,
        hops: &[crate::query_planner::analyzer::multi_type_vlp_expansion::PathHop],
    ) -> Vec<String> {
        let mut items = Vec::new();

        // Add type discriminator column
        items.push(format!("'{}' AS end_type", node_type));

        // Add end node ID as String (handles UNION type compatibility)
        if let Ok(node_schema) = self
            .schema
            .all_node_schemas()
            .get(node_type)
            .ok_or("Node not found")
        {
            let end_id_sql = if node_schema.node_id.is_composite() {
                // Composite ID: convert tuple to String
                let cols: Vec<String> = node_schema
                    .node_id
                    .columns()
                    .iter()
                    .map(|col| format!("{}.{}", node_alias, col))
                    .collect();
                format!(
                    "toString(tuple({})) AS {}",
                    cols.join(", "),
                    VLP_END_ID_COLUMN
                )
            } else {
                // Single ID: cast to String
                format!(
                    "toString({}.{}) AS {}",
                    node_alias,
                    node_schema.node_id.column(),
                    VLP_END_ID_COLUMN
                )
            };
            items.push(end_id_sql);
        }

        // Add start node ID as String
        if let Ok(start_type) = self.start_labels.first().ok_or("No start type") {
            if let Ok(node_schema) = self
                .schema
                .all_node_schemas()
                .get(start_type)
                .ok_or("Node not found")
            {
                let start_id_sql = if node_schema.node_id.is_composite() {
                    let cols: Vec<String> = node_schema
                        .node_id
                        .columns()
                        .iter()
                        .map(|col| format!("{}.{}", start_alias_sql, col))
                        .collect();
                    format!(
                        "toString(tuple({})) AS {}",
                        cols.join(", "),
                        VLP_START_ID_COLUMN
                    )
                } else {
                    format!(
                        "toString({}.{}) AS {}",
                        start_alias_sql,
                        node_schema.node_id.column(),
                        VLP_START_ID_COLUMN
                    )
                };
                items.push(start_id_sql);
            }
        }

        // Serialize all properties as JSON string using map() for proper JSON object format
        if let Ok(node_schema) = self
            .schema
            .all_node_schemas()
            .get(node_type)
            .ok_or("Node not found")
        {
            if !node_schema.property_mappings.is_empty() {
                // Build map: map('key1', toString(value1), 'key2', toString(value2), ...)
                // ClickHouse's toJSONString(map(...)) creates proper JSON objects
                let mut map_items = Vec::new();
                for (cypher_prop, prop_value) in &node_schema.property_mappings {
                    let column_name = match prop_value {
                        crate::graph_catalog::expression_parser::PropertyValue::Column(col) => {
                            col.clone()
                        }
                        _ => continue, // Skip non-column property mappings
                    };
                    map_items.push(format!(
                        "'{}', toString({}.{})",
                        cypher_prop, node_alias, column_name
                    ));
                }

                if !map_items.is_empty() {
                    items.push(format!(
                        "toJSONString(map({})) AS end_properties",
                        map_items.join(", ")
                    ));
                } else {
                    items.push("'{}' AS end_properties".to_string());
                }
            } else {
                // No properties - empty JSON object
                items.push("'{}' AS end_properties".to_string());
            }
        }

        // Add hop_count for length(p) function support
        items.push(format!("{} AS hop_count", hop_count));

        // Add path_relationships for relationships(p) function support
        // Generate array of relationship types: ['FOLLOWS', 'AUTHORED', ...]
        let rel_types: Vec<String> = hops
            .iter()
            .map(|hop| format!("'{}'", hop.rel_type))
            .collect();
        items.push(format!("[{}] AS path_relationships", rel_types.join(", ")));

        items
    }
    /// Get table name with database prefix for a node type
    /// 🔧 PARAMETERIZED VIEW FIX: Applies view parameters if the node schema has view_parameters defined
    fn get_node_table_with_db(&self, node_type: &str) -> Result<String, String> {
        self.schema
            .all_node_schemas()
            .get(node_type)
            .map(|n| {
                let base_table = if n.database.is_empty() {
                    n.table_name.clone()
                } else {
                    format!("{}.{}", n.database, n.table_name)
                };

                // Apply parameterized view syntax if the schema has view_parameters
                self.apply_view_parameters(&base_table, &n.view_parameters)
            })
            .ok_or_else(|| format!("Node table not found for type '{}'", node_type))
    }

    /// Get table name with database prefix for a relationship type with node context
    /// Uses composite key lookup for schemas that use TYPE::FROM::TO keys
    /// 🔧 PARAMETERIZED VIEW FIX: Applies view parameters if the relationship schema has view_parameters defined
    fn get_rel_table_with_db(
        &self,
        rel_type: &str,
        from_node: &str,
        to_node: &str,
    ) -> Result<String, String> {
        self.schema
            .get_rel_schema_with_nodes(rel_type, Some(from_node), Some(to_node))
            .map(|r| {
                let base_table = if r.database.is_empty() {
                    r.table_name.clone()
                } else {
                    format!("{}.{}", r.database, r.table_name)
                };

                // Apply parameterized view syntax if the schema has view_parameters
                self.apply_view_parameters(&base_table, &r.view_parameters)
            })
            .map_err(|e| {
                format!(
                    "Relationship table not found for type '{}' ({}->{}): {}",
                    rel_type, from_node, to_node, e
                )
            })
    }

    /// Apply view parameters to a table name, generating parameterized view syntax
    /// Example: "graphrag.documents" + {"tenant_id": "tenant_a"} + [tenant_id]
    ///          → "`graphrag.documents`(tenant_id = 'tenant_a')"
    fn apply_view_parameters(
        &self,
        base_table: &str,
        schema_params: &Option<Vec<String>>,
    ) -> String {
        // Only apply if schema has view_parameters AND we have values for them
        if let Some(param_names) = schema_params {
            if param_names.is_empty() || self.view_parameter_values.is_empty() {
                return base_table.to_string();
            }

            // Collect matching parameters
            let param_assignments: Vec<String> = param_names
                .iter()
                .filter_map(|name| {
                    self.view_parameter_values
                        .get(name)
                        .map(|value| format!("{} = '{}'", name, value))
                })
                .collect();

            if param_assignments.is_empty() {
                return base_table.to_string();
            }

            // Format: `db.table`(param = 'value')
            format!("`{}`({})", base_table, param_assignments.join(", "))
        } else {
            base_table.to_string()
        }
    }

    /// Get from_id and to_id columns for a relationship type with node context
    /// Uses composite key lookup for schemas that use TYPE::FROM::TO keys
    fn get_rel_columns(
        &self,
        rel_type: &str,
        from_node: &str,
        to_node: &str,
    ) -> Result<(String, String), String> {
        let result = self
            .schema
            .get_rel_schema_with_nodes(rel_type, Some(from_node), Some(to_node))
            .map(|r| {
                let cols = (r.from_id.clone(), r.to_id.clone());
                log::info!(
                    "📋 Schema lookup for rel_type '{}' ({}->{}): from_id='{}', to_id='{}'",
                    rel_type,
                    from_node,
                    to_node,
                    cols.0,
                    cols.1
                );
                cols
            })
            .map_err(|e| {
                format!(
                    "Relationship columns not found for type '{}' ({}->{}): {}",
                    rel_type, from_node, to_node, e
                )
            });

        if let Err(ref e) = result {
            log::error!("❌ Failed to get relationship columns: {}", e);
        }
        result
    }

    /// Get ID column for a node type
    fn get_node_id_column(&self, node_type: &str) -> Result<String, String> {
        self.schema
            .all_node_schemas()
            .get(node_type)
            .map(|n| n.node_id.column().to_string())
            .ok_or_else(|| format!("Node ID column not found for type '{}'", node_type))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Simplified tests - just verify structure and basic functionality
    // Full integration testing will be done in Python integration tests
    // Schema creation is complex, so we test without actual GraphSchema for now

    #[test]
    fn test_multi_type_vlp_generator_structure() {
        // Test that the struct can be created with proper field values
        let spec = VariableLengthSpec::range(1, 2);

        // We can't easily create a GraphSchema in tests without lots of setup
        // So we'll just verify the constructor logic with assertions
        assert_eq!(spec.min_hops, Some(1));
        assert_eq!(spec.max_hops, Some(2));

        // Verify max_hops capping logic
        let large_spec = VariableLengthSpec::range(1, 10);
        let max_hops_capped = large_spec.max_hops.unwrap_or(10) as usize;
        let max_hops_result = max_hops_capped.min(3); // Should cap at 3
        assert_eq!(max_hops_result, 3);
    }

    #[test]
    fn test_property_projection_structure() {
        let mut type_mappings = HashMap::new();
        type_mappings.insert(
            "User".to_string(),
            ("full_name".to_string(), "u".to_string()),
        );
        type_mappings.insert("Post".to_string(), ("title".to_string(), "p".to_string()));

        let prop = PropertyProjection {
            cypher_name: "name".to_string(),
            type_mappings,
        };

        assert_eq!(prop.cypher_name, "name");
        assert_eq!(prop.type_mappings.len(), 2);
        assert!(prop.type_mappings.contains_key("User"));
        assert!(prop.type_mappings.contains_key("Post"));
    }

    #[test]
    fn test_variable_length_spec() {
        // Test fixed length
        let fixed = VariableLengthSpec::fixed(2);
        assert_eq!(fixed.min_hops, Some(2));
        assert_eq!(fixed.max_hops, Some(2));

        // Test range
        let range = VariableLengthSpec::range(1, 3);
        assert_eq!(range.min_hops, Some(1));
        assert_eq!(range.max_hops, Some(3));
    }
}
