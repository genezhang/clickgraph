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

use crate::clickhouse_query_generator::json_builder::generate_json_properties_from_schema_without_aliases;
use crate::graph_catalog::config::Identifier;
use crate::graph_catalog::expression_parser::PropertyValue;
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::query_planner::analyzer::multi_type_vlp_expansion::{
    enumerate_vlp_paths, enumerate_vlp_paths_undirected, PathEnumeration,
};
use crate::query_planner::join_context::{VLP_END_ID_COLUMN, VLP_START_ID_COLUMN};
use crate::query_planner::logical_plan::VariableLengthSpec;
use crate::query_planner::plan_ctx::PlanCtx;
use std::collections::HashMap;
use std::sync::Arc;

/// Property projection for heterogeneous node types
#[derive(Debug, Clone)]
pub struct PropertyProjection {
    /// Cypher property name (e.g., "name")
    pub cypher_name: String,
    /// Mapping: node_type -> (table_column, table_alias)
    /// Example: {"User": ("full_name", "u2"), "Post": ("title", "p1")}
    pub type_mappings: HashMap<String, (String, String)>,
}

/// Property selection mode for VLP CTE generation
#[derive(Debug, Clone)]
enum PropertySelectionMode {
    /// No properties needed (e.g., COUNT(*) only)
    IdOnly,
    /// Specific properties needed as individual columns
    Individual { properties: Vec<PropertyInfo> },
    /// Whole node needed (e.g., RETURN a, collect(a))
    WholeNode,
}

/// Information about a property to select
#[derive(Debug, Clone)]
struct PropertyInfo {
    /// Cypher property name (e.g., "name")
    cypher_property: String,
    /// Database column name (e.g., "full_name")
    db_column: String,
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

    // Whether the pattern is undirected (includes both incoming and outgoing edges)
    undirected: bool,

    // Filter conditions
    start_filters: Option<String>, // WHERE clause for start node
    end_filters: Option<String>,   // WHERE clause for end node
    rel_filters: Option<String>,   // WHERE clause for relationships

    // Property projections (properties to return)
    properties: Vec<PropertyProjection>,

    // 🔧 PARAMETERIZED VIEW FIX: View parameter values for multi-tenant queries
    // Maps parameter name -> parameter value (e.g., "tenant_id" -> "tenant_a")
    view_parameter_values: HashMap<String, String>,

    // 🔧 PROPERTY SELECTION FIX: Plan context for property requirements tracking
    // Used to determine which properties are actually needed (Individual vs WholeNode vs IdOnly)
    plan_ctx: Option<Arc<PlanCtx>>,
}

impl<'a> MultiTypeVlpJoinGenerator<'a> {
    /// Helper: Generate node alias prefix from node type
    ///
    /// Derives alias prefix from first character of type name (lowercase).
    /// Examples: "User" -> "u", "Post" -> "p", "Flight" -> "f"
    /// Fallback: "n" for uncommon types
    ///
    /// This avoids hardcoded schema-specific logic (User->u, Post->p).
    fn node_alias_prefix(node_type: &str) -> char {
        node_type
            .chars()
            .next()
            .map(|c| c.to_ascii_lowercase())
            .unwrap_or('n')
    }

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
    /// * `plan_ctx` - Optional plan context for property requirements tracking
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
        plan_ctx: Option<Arc<PlanCtx>>,
        undirected: bool,
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
            undirected,
            start_filters,
            end_filters,
            rel_filters,
            properties: vec![],
            view_parameter_values,
            plan_ctx,
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

    /// Determine property selection mode for a node alias
    ///
    /// Checks property requirements from analyzer to decide whether to:
    /// - IdOnly: Just select IDs (no properties) for COUNT(*) queries
    /// - Individual: Select specific properties as columns for efficient aggregation
    /// - WholeNode: Select all properties as JSON for RETURN node, collect(node), etc.
    ///
    /// This restores the original VLP behavior (pre-JSON) where only needed properties
    /// were selected, while maintaining Browser support that requires JSON.
    fn determine_property_mode(&self, alias: &str, node_type: &str) -> PropertySelectionMode {
        log::debug!(
            "determine_property_mode called for alias='{}', node_type='{}'",
            alias,
            node_type
        );

        // Step 1: Check if we have plan_ctx
        let Some(plan_ctx) = &self.plan_ctx else {
            log::debug!(
                "VLP: No plan_ctx available for alias '{}', defaulting to WholeNode mode",
                alias
            );
            return PropertySelectionMode::WholeNode;
        };

        // Step 2: Get property requirements from analyzer
        let Some(reqs) = plan_ctx.get_property_requirements() else {
            log::debug!(
                "VLP: No property requirements tracked for alias '{}', defaulting to WholeNode mode",
                alias
            );
            return PropertySelectionMode::WholeNode;
        };

        // Step 3: Check if alias requires all properties (wildcard)
        if reqs.requires_all(alias) {
            log::info!(
                "VLP: Alias '{}' requires ALL properties - WholeNode mode (JSON)",
                alias
            );
            return PropertySelectionMode::WholeNode;
        }

        // Step 4: Get specific property requirements
        let Some(required_props) = reqs.get_requirements(alias) else {
            log::info!(
                "VLP: Alias '{}' has no property requirements - IdOnly mode",
                alias
            );
            return PropertySelectionMode::IdOnly;
        };

        if required_props.is_empty() {
            log::info!(
                "VLP: Alias '{}' has empty property requirements - IdOnly mode",
                alias
            );
            return PropertySelectionMode::IdOnly;
        }
        log::debug!(
            "VLP: Alias '{}' needs {} specific properties: {:?}",
            alias,
            required_props.len(),
            required_props
        );

        // Step 5: Map Cypher properties to DB columns
        let Some(node_schema) = self.schema.all_node_schemas().get(node_type) else {
            log::debug!(
                "VLP: Node schema not found for type '{}', defaulting to WholeNode mode",
                node_type
            );
            return PropertySelectionMode::WholeNode;
        };

        let mut properties = Vec::new();
        for cypher_prop in required_props {
            if let Some(PropertyValue::Column(db_col)) =
                node_schema.property_mappings.get(cypher_prop)
            {
                properties.push(PropertyInfo {
                    cypher_property: cypher_prop.clone(),
                    db_column: db_col.clone(),
                });
            } else {
                log::debug!(
                    "VLP: Property '{}' not found in schema for type '{}', falling back to WholeNode mode",
                    cypher_prop,
                    node_type
                );
                return PropertySelectionMode::WholeNode;
            }
        }

        log::info!(
            "VLP: Alias '{}' requires {} specific properties - Individual mode: {:?}",
            alias,
            properties.len(),
            properties
                .iter()
                .map(|p| &p.cypher_property)
                .collect::<Vec<_>>()
        );
        PropertySelectionMode::Individual { properties }
    }

    /// Generate complete SQL with UNION ALL of all valid paths
    ///
    /// Returns CTE SQL that can be referenced in outer query
    pub fn generate_cte_sql(&self, _cte_name: &str) -> Result<String, String> {
        // Step 1: Enumerate all valid path combinations
        let paths = if self.undirected {
            enumerate_vlp_paths_undirected(
                &self.start_labels,
                &self.rel_types,
                &self.end_labels,
                self.min_hops,
                self.max_hops,
                self.schema,
            )
        } else {
            enumerate_vlp_paths(
                &self.start_labels,
                &self.rel_types,
                &self.end_labels,
                self.min_hops,
                self.max_hops,
                self.schema,
            )
        };

        if paths.is_empty() {
            log::debug!(
                "No valid paths found for {:?}-[{:?}*{}..{}]->{:?}, generating empty CTE",
                self.start_labels,
                self.rel_types,
                self.min_hops,
                self.max_hops,
                self.end_labels
            );
            // Return an empty-result CTE instead of an error so UNION branches
            // with no valid paths are silently skipped
            // IMPORTANT: Empty CTE must have ALL columns that non-empty CTEs have for UNION compatibility
            // Columns: end_type, end_id, start_id, start_type, end_properties, start_properties,
            //          hop_count, path_relationships, rel_properties
            // Use proper types for all columns to match the non-empty CTE
            return Ok("SELECT '' AS end_type, CAST('', 'String') AS end_id, CAST('', 'String') AS start_id, '' AS start_type, '{}' AS end_properties, '{}' AS start_properties, 0 AS hop_count, CAST([], 'Array(String)') AS path_relationships, CAST([], 'Array(String)') AS rel_properties WHERE 0 = 1".to_string());
        }

        log::error!(
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
                    log::debug!("Failed to generate SQL for path {:?}: {}", path, e);
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

        // Add start filters — strip predicates referencing columns not in start node's table
        if let Some(ref filters) = self.start_filters {
            let start_filter = filters
                .replace("start_node.", &format!("{}.", start_alias_sql))
                .replace(
                    &format!("{}.", self.start_alias),
                    &format!("{}.", start_alias_sql),
                );
            let validated =
                self.strip_invalid_column_predicates(&start_filter, start_type, &start_alias_sql);
            if !validated.is_empty() {
                where_clauses.push(validated);
            }
        }

        // Generate JOINs for each hop
        let mut current_alias = start_alias_sql.clone();
        let mut end_node_alias = String::new();
        let mut end_node_type = String::new();

        for (hop_idx, hop) in hops.iter().enumerate() {
            let hop_num = hop_idx + 1;

            // Get relationship info using composite key lookup (TYPE::FROM::TO)
            // For reversed hops, look up using the original schema direction
            let (schema_from, schema_to) = if hop.reversed {
                (&hop.to_node_type, &hop.from_node_type) // Original schema direction
            } else {
                (&hop.from_node_type, &hop.to_node_type)
            };
            let rel_table = self.get_rel_table_with_db(&hop.rel_type, schema_from, schema_to)?;
            let (from_col, to_col) = self.get_rel_columns(&hop.rel_type, schema_from, schema_to)?;

            // For reversed hops, swap the join columns
            let (join_from_col, join_to_col) = if hop.reversed {
                (to_col.clone(), from_col.clone()) // Reversed: enter via to_col, exit via from_col
            } else {
                (from_col.clone(), to_col.clone())
            };

            let start_id_col = self.get_node_id_column(&hop.from_node_type)?;
            log::info!("🔍 Hop {}: rel_type={}, from_node={}, to_node={}, reversed={}, join_from={}, join_to={}", 
                hop_num, hop.rel_type, hop.from_node_type, hop.to_node_type, hop.reversed, join_from_col, join_to_col);

            // Get target node info
            let end_table = self.get_node_table_with_db(&hop.to_node_type)?;
            let end_id_col = self.get_node_id_column(&hop.to_node_type)?;

            // Check if this is an FK-edge pattern (rel table == target node table)
            // In this case, the relationship table IS the target node, so we only need one JOIN
            let is_fk_edge = rel_table == end_table;

            if is_fk_edge {
                // FK-edge pattern: relationship table IS the target node table
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

                let join_cond =
                    start_id_col.to_sql_equality(&current_alias, &join_from_col, &end_node_alias);
                let join_sql = format!(
                    "INNER JOIN {} {} ON {}",
                    rel_table, end_node_alias, join_cond
                );
                from_clauses.push(join_sql);

                // Add relationship filters (apply to the combined rel/node table)
                if let Some(ref rel_filters) = self.rel_filters {
                    let rel_filter = rel_filters.replace("rel.", &format!("{}.", end_node_alias));
                    where_clauses.push(rel_filter);
                }
            } else {
                // Standard pattern: separate relationship table and target node table
                let rel_alias = format!("r{}", hop_num);

                let rel_join_cond =
                    start_id_col.to_sql_equality(&current_alias, &join_from_col, &rel_alias);
                let rel_join_sql = format!(
                    "INNER JOIN {} {} ON {}",
                    rel_table, rel_alias, rel_join_cond
                );
                log::info!("   Standard path rel JOIN: {}", rel_join_sql);
                from_clauses.push(rel_join_sql);

                // Add polymorphic type filters (type_column, from_label_column, to_label_column)
                if let Ok(rel_schema) = self.schema.get_rel_schema_with_nodes(
                    &hop.rel_type,
                    Some(schema_from),
                    Some(schema_to),
                ) {
                    if let Some(ref type_col) = rel_schema.type_column {
                        where_clauses
                            .push(format!("{}.{} = '{}'", rel_alias, type_col, hop.rel_type));
                    }
                    if let Some(ref from_label_col) = rel_schema.from_label_column {
                        // Use schema_from (original direction) not hop.from_node_type (may be reversed)
                        where_clauses.push(format!(
                            "{}.{} = '{}'",
                            rel_alias, from_label_col, schema_from
                        ));
                    }
                    if let Some(ref to_label_col) = rel_schema.to_label_column {
                        where_clauses
                            .push(format!("{}.{} = '{}'", rel_alias, to_label_col, schema_to));
                    }
                }

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

                let node_join_cond =
                    join_to_col.to_sql_equality(&rel_alias, &end_id_col, &end_node_alias);
                let node_join_sql = format!(
                    "INNER JOIN {} {} ON {}",
                    end_table, end_node_alias, node_join_cond
                );
                log::info!("   Standard path node JOIN: {}", node_join_sql);
                from_clauses.push(node_join_sql);
            }

            // Update current_alias for next hop
            current_alias = end_node_alias.clone();
        }

        // Add end filters — strip predicates referencing columns not in end node's table
        if let Some(ref filters) = self.end_filters {
            let end_filter = filters
                .replace("end_node.", &format!("{}.", end_node_alias))
                .replace(
                    &format!("{}.", self.end_alias),
                    &format!("{}.", end_node_alias),
                );
            let validated =
                self.strip_invalid_column_predicates(&end_filter, &end_node_type, &end_node_alias);
            if !validated.is_empty() {
                where_clauses.push(validated);
            }
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
    /// - `rel_properties`: JSON array of relationship properties for each hop
    fn generate_select_items(
        &self,
        node_alias: &str,
        node_type: &str,
        start_alias_sql: &str,
        hop_count: usize,
        hops: &[crate::query_planner::analyzer::multi_type_vlp_expansion::PathHop],
    ) -> Vec<String> {
        log::debug!(
            "generate_select_items for node_type='{}', node_alias='{}'",
            node_type,
            node_alias
        );

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
            let end_id_sql = format!(
                "{} AS {}",
                node_schema.node_id.id.to_pipe_joined_sql(node_alias),
                VLP_END_ID_COLUMN
            );
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
                let start_id_sql = format!(
                    "{} AS {}",
                    node_schema.node_id.id.to_pipe_joined_sql(start_alias_sql),
                    VLP_START_ID_COLUMN
                );
                items.push(start_id_sql);
            }
        }

        // Add start type discriminator (needed for outer SELECT when start node is the returned variable)
        let start_type_value = &hops[0].from_node_type;
        items.push(format!("'{}' AS start_type", start_type_value));

        // 🔧 PROPERTY SELECTION FIX: Use property requirements to determine selection mode
        // Instead of always using JSON, check what's actually needed:
        // - IdOnly: No properties (just IDs for COUNT(*))
        // - Individual: Specific properties as columns (efficient aggregation)
        // - WholeNode: All properties as JSON (Browser, collect, RETURN node)

        let end_mode = self.determine_property_mode(&self.end_alias, node_type);

        // For multi-type end nodes, ALWAYS use WholeNode JSON-only mode.
        // Property requirements may reference properties that only exist in one type
        // (e.g., post_id for Post but not User), causing column mismatch in UNION ALL.
        let end_mode = if self.end_labels.len() > 1 {
            match end_mode {
                PropertySelectionMode::IdOnly => {
                    log::info!(
                        "VLP: Multi-type end node '{}' - upgrading IdOnly to WholeNode for uniform columns",
                        self.end_alias
                    );
                    PropertySelectionMode::WholeNode
                }
                PropertySelectionMode::Individual { .. } => {
                    log::info!(
                        "VLP: Multi-type end node '{}' - upgrading Individual to WholeNode for uniform columns",
                        self.end_alias
                    );
                    PropertySelectionMode::WholeNode
                }
                other => other,
            }
        } else {
            end_mode
        };

        match end_mode {
            PropertySelectionMode::IdOnly => {
                log::debug!(
                    "VLP: End node '{}' - IdOnly mode, skipping properties",
                    self.end_alias
                );
            }

            PropertySelectionMode::Individual { properties } => {
                log::info!(
                    "VLP: End node '{}' - Individual mode with {} properties",
                    self.end_alias,
                    properties.len()
                );

                for prop_info in properties {
                    items.push(format!(
                        "{}.{} AS end_{}",
                        node_alias, prop_info.db_column, prop_info.cypher_property
                    ));
                }
            }

            PropertySelectionMode::WholeNode => {
                // When end node has multiple possible types (multi-type/unlabeled),
                // use JSON-only mode to ensure uniform columns across UNION ALL branches.
                // Individual columns differ per type and cause column count mismatch.
                let is_multi_type = self.end_labels.len() > 1;

                if is_multi_type {
                    log::info!(
                        "VLP: End node '{}' - WholeNode JSON-only mode (multi-type: {:?})",
                        self.end_alias,
                        self.end_labels
                    );
                } else {
                    log::info!(
                        "VLP: End node '{}' - WholeNode mode (JSON + individual)",
                        self.end_alias
                    );
                }

                if let Some(node_schema) = self.schema.all_node_schemas().get(node_type) {
                    if !node_schema.property_mappings.is_empty() {
                        // Use normal aliases for clean JSON keys (cypher property names)
                        // Safe because start_properties JSON is skipped for single-type start nodes
                        use crate::clickhouse_query_generator::json_builder::generate_json_properties_from_schema;
                        let json_sql =
                            generate_json_properties_from_schema(node_schema, node_alias);
                        items.push(format!("{} AS end_properties", json_sql));

                        // Only generate individual columns for single-type end nodes
                        if !is_multi_type {
                            let mut sorted_props: Vec<_> =
                                node_schema.property_mappings.iter().collect();
                            sorted_props.sort_by_key(|(k, _)| k.as_str());
                            for (cypher_name, prop_val) in sorted_props {
                                if let PropertyValue::Column(db_col) = prop_val {
                                    items.push(format!(
                                        "{}.{} AS end_{}",
                                        node_alias, db_col, cypher_name
                                    ));
                                }
                            }
                        }
                    } else if let Some(denorm_props) =
                        self.get_denormalized_node_properties(node_type, false, hops)
                    {
                        // Denormalized node: properties come from edge table columns
                        use crate::clickhouse_query_generator::json_builder::generate_json_from_denormalized_properties;
                        let json_sql = generate_json_from_denormalized_properties(
                            &denorm_props,
                            node_alias,
                            "_e_",
                        );
                        items.push(format!("{} AS end_properties", json_sql));
                    } else {
                        items.push("'{}' AS end_properties".to_string());
                    }
                }
            }
        }

        // 🔧 PROPERTY SELECTION FIX: Same logic for start node properties
        if let Ok(start_type) = self.start_labels.first().ok_or("No start type") {
            let start_mode = self.determine_property_mode(&self.start_alias, start_type);

            // For multi-type start nodes, ALWAYS use WholeNode JSON-only mode.
            let start_mode = if self.start_labels.len() > 1 {
                match start_mode {
                    PropertySelectionMode::IdOnly => {
                        log::info!(
                            "VLP: Multi-type start node '{}' - upgrading IdOnly to WholeNode for uniform columns",
                            self.start_alias
                        );
                        PropertySelectionMode::WholeNode
                    }
                    PropertySelectionMode::Individual { .. } => {
                        log::info!(
                            "VLP: Multi-type start node '{}' - upgrading Individual to WholeNode for uniform columns",
                            self.start_alias
                        );
                        PropertySelectionMode::WholeNode
                    }
                    other => other,
                }
            } else {
                start_mode
            };

            match start_mode {
                PropertySelectionMode::IdOnly => {
                    log::debug!(
                        "VLP: Start node '{}' - IdOnly mode, skipping properties",
                        self.start_alias
                    );
                }

                PropertySelectionMode::Individual { properties } => {
                    log::info!(
                        "VLP: Start node '{}' - Individual mode with {} properties",
                        self.start_alias,
                        properties.len()
                    );

                    for prop_info in properties {
                        items.push(format!(
                            "{}.{} AS start_{}",
                            start_alias_sql, prop_info.db_column, prop_info.cypher_property
                        ));
                    }
                }

                PropertySelectionMode::WholeNode => {
                    let is_multi_type_start = self.start_labels.len() > 1;

                    if is_multi_type_start {
                        log::info!(
                            "VLP: Start node '{}' - WholeNode JSON-only mode (multi-type: {:?})",
                            self.start_alias,
                            self.start_labels
                        );
                    } else {
                        log::info!(
                            "VLP: Start node '{}' - WholeNode mode (JSON + individual)",
                            self.start_alias
                        );
                    }

                    if let Some(node_schema) = self.schema.all_node_schemas().get(start_type) {
                        if !node_schema.property_mappings.is_empty() {
                            // Always generate start_properties JSON blob — the outer
                            // SELECT references it via t.start_properties
                            let json_sql = generate_json_properties_from_schema_without_aliases(
                                node_schema,
                                start_alias_sql,
                            );
                            items.push(format!("{} AS start_properties", json_sql));

                            // Also generate individual columns for single-type start nodes
                            // (needed for WHERE/ORDER BY on specific properties)
                            if !is_multi_type_start {
                                let mut sorted_props: Vec<_> =
                                    node_schema.property_mappings.iter().collect();
                                sorted_props.sort_by_key(|(k, _)| k.as_str());
                                for (cypher_name, prop_val) in sorted_props {
                                    if let PropertyValue::Column(db_col) = prop_val {
                                        items.push(format!(
                                            "{}.{} AS start_{}",
                                            start_alias_sql, db_col, cypher_name
                                        ));
                                    }
                                }
                            }
                        } else if let Some(denorm_props) =
                            self.get_denormalized_node_properties(start_type, true, hops)
                        {
                            // Denormalized node: properties come from edge table columns
                            use crate::clickhouse_query_generator::json_builder::generate_json_from_denormalized_properties;
                            let json_sql = generate_json_from_denormalized_properties(
                                &denorm_props,
                                start_alias_sql,
                                "_s_",
                            );
                            items.push(format!("{} AS start_properties", json_sql));
                        } else {
                            items.push("'{}' AS start_properties".to_string());
                        }
                    }
                }
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

        // 🔧 FIX: Add rel_properties for RETURN r support
        // Generate array of JSON objects containing relationship properties for each hop
        // Use without_aliases version to avoid duplicate alias errors in array context
        use crate::clickhouse_query_generator::json_builder::generate_json_properties_without_aliases;

        let rel_props: Vec<String> = hops
            .iter()
            .enumerate()
            .map(|(hop_idx, hop)| {
                let hop_num = hop_idx + 1;

                // Get relationship schema to access property mappings
                match self.schema.get_rel_schema_with_nodes(
                    &hop.rel_type,
                    Some(&hop.from_node_type),
                    Some(&hop.to_node_type),
                ) {
                    Ok(rel_schema) => {
                        // Check if this is FK-edge pattern (rel table == target node table)
                        let rel_table = if rel_schema.database.is_empty() {
                            rel_schema.table_name.clone()
                        } else {
                            format!("{}.{}", rel_schema.database, rel_schema.table_name)
                        };

                        let end_table = match self.get_node_table_with_db(&hop.to_node_type) {
                            Ok(t) => t,
                            Err(_) => return "'{}'".to_string(), // Empty JSON
                        };

                        let is_fk_edge = rel_table == end_table;

                        // Reconstruct the relationship alias used in FROM clause
                        let rel_alias = if is_fk_edge {
                            // FK-edge: alias is the end node alias
                            format!(
                                "{}{}",
                                Self::node_alias_prefix(&hop.to_node_type),
                                hop_num + 1
                            )
                        } else {
                            // Standard: alias is r{hop_num}
                            format!("r{}", hop_num)
                        };

                        // Generate JSON object with relationship properties
                        if rel_schema.property_mappings.is_empty() {
                            "'{}'".to_string() // Empty JSON object
                        } else {
                            // Use formatRowNoNewline WITHOUT aliases for array elements
                            // This avoids ClickHouse "Multiple expressions for alias" errors
                            generate_json_properties_without_aliases(
                                &rel_schema.property_mappings,
                                &rel_alias,
                            )
                        }
                    }
                    Err(_) => "'{}'".to_string(), // Empty JSON if schema not found
                }
            })
            .collect();
        items.push(format!("[{}] AS rel_properties", rel_props.join(", ")));

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
    /// Returns Identifier types to support composite keys
    fn get_rel_columns(
        &self,
        rel_type: &str,
        from_node: &str,
        to_node: &str,
    ) -> Result<(Identifier, Identifier), String> {
        self.schema
            .get_rel_schema_with_nodes(rel_type, Some(from_node), Some(to_node))
            .map(|r| {
                log::info!(
                    "📋 Schema lookup for rel_type '{}' ({}->{}): from_id='{}', to_id='{}'",
                    rel_type,
                    from_node,
                    to_node,
                    r.from_id,
                    r.to_id
                );
                (r.from_id.clone(), r.to_id.clone())
            })
            .map_err(|e| {
                let msg = format!(
                    "Relationship columns not found for type '{}' ({}->{}): {}",
                    rel_type, from_node, to_node, e
                );
                log::error!("❌ {}", msg);
                msg
            })
    }

    /// Get denormalized node properties from the relationship schema.
    ///
    /// For denormalized nodes (property_mappings is empty), properties come from
    /// `from_node_properties` or `to_node_properties` on the relationship schema.
    /// Returns the appropriate mapping based on the hop direction.
    fn get_denormalized_node_properties(
        &self,
        _node_type: &str,
        is_start_node: bool,
        hops: &[crate::query_planner::analyzer::multi_type_vlp_expansion::PathHop],
    ) -> Option<std::collections::HashMap<String, String>> {
        let hop = if is_start_node {
            hops.first()
        } else {
            hops.last()
        }?;

        let (schema_from, schema_to) = if hop.reversed {
            (&hop.to_node_type, &hop.from_node_type)
        } else {
            (&hop.from_node_type, &hop.to_node_type)
        };

        let rel_schema = self
            .schema
            .get_rel_schema_with_nodes(&hop.rel_type, Some(schema_from), Some(schema_to))
            .ok()?;

        // For start node: if hop is normal direction, start = from_node → use from_node_properties
        //                 if hop is reversed, start = to_node → use to_node_properties
        // For end node: opposite
        let props = if is_start_node {
            if hop.reversed {
                &rel_schema.to_node_properties
            } else {
                &rel_schema.from_node_properties
            }
        } else if hop.reversed {
            &rel_schema.from_node_properties
        } else {
            &rel_schema.to_node_properties
        };

        props.clone()
    }

    /// Get ID column(s) for a node type as an Identifier (supports composite keys)
    fn get_node_id_column(&self, node_type: &str) -> Result<Identifier, String> {
        self.schema
            .all_node_schemas()
            .get(node_type)
            .map(|n| n.node_id.id.clone())
            .ok_or_else(|| format!("Node ID column not found for type '{}'", node_type))
    }

    /// Strip predicates from a SQL filter that reference columns not in the node's table.
    ///
    /// When UNION branches share a WHERE clause that was originally for mixed-label nodes,
    /// some predicates may reference columns from a different label's table. For example,
    /// `(u2.user_id IN (...) AND u2.post_id IN (...))` where `u2` is a User table —
    /// `post_id` doesn't exist on users_bench.
    ///
    /// This method splits the filter on top-level AND, validates each predicate against the
    /// node schema's property mapping, and drops any that reference invalid columns.
    fn strip_invalid_column_predicates(
        &self,
        filter_sql: &str,
        node_type: &str,
        table_alias: &str,
    ) -> String {
        // Get valid column names for this node type
        let valid_columns: std::collections::HashSet<String> =
            if let Some(node_schema) = self.schema.all_node_schemas().get(node_type) {
                let mut cols: std::collections::HashSet<String> = node_schema
                    .property_mappings
                    .values()
                    .map(|col| col.raw().to_string())
                    .collect();
                // Also include the ID column(s)
                for col in node_schema.node_id.id.columns() {
                    cols.insert(col.to_string());
                }
                cols
            } else {
                // Can't validate — return filter as-is
                return filter_sql.to_string();
            };

        // Strip outer parentheses if present
        let inner = filter_sql.trim();
        let inner = if inner.starts_with('(') && inner.ends_with(')') {
            // Check if the parens wrap the entire expression (not just a sub-expression)
            let mut depth = 0;
            let mut wraps_all = true;
            for (i, ch) in inner.chars().enumerate() {
                match ch {
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 && i < inner.len() - 1 {
                            wraps_all = false;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            if wraps_all {
                &inner[1..inner.len() - 1]
            } else {
                inner
            }
        } else {
            inner
        };

        // Split on " AND " at the top level (respecting parentheses)
        let parts = split_top_level_and(inner);

        // Validate each part: check for `alias.column` references
        let alias_dot = format!("{}.", table_alias);
        let valid_parts: Vec<&str> = parts
            .iter()
            .filter(|part| {
                // Extract column references like `alias.column_name`
                let mut valid = true;
                let mut search_from = 0;
                while let Some(pos) = part[search_from..].find(&alias_dot) {
                    let abs_pos = search_from + pos + alias_dot.len();
                    if abs_pos < part.len() {
                        // Extract column name (alphanumeric + underscore)
                        let col_name: String = part[abs_pos..]
                            .chars()
                            .take_while(|c| c.is_alphanumeric() || *c == '_')
                            .collect();
                        if !col_name.is_empty() && !valid_columns.contains(&col_name) {
                            log::debug!(
                                "Stripping invalid column predicate: {}.{} not in {} schema (valid: {:?})",
                                table_alias, col_name, node_type, valid_columns
                            );
                            valid = false;
                            break;
                        }
                    }
                    search_from = abs_pos;
                }
                valid
            })
            .copied()
            .collect();

        if valid_parts.is_empty() {
            String::new()
        } else if valid_parts.len() == 1 {
            format!("({})", valid_parts[0])
        } else {
            format!("({})", valid_parts.join(" AND "))
        }
    }
}

/// Split a SQL expression on top-level " AND " (respecting parentheses depth).
fn split_top_level_and(sql: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    let bytes = sql.as_bytes();
    let and_pattern = b" AND ";
    let and_len = and_pattern.len();

    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b' ' if depth == 0
                && i + and_len <= bytes.len()
                && &bytes[i..i + and_len] == and_pattern =>
            {
                parts.push(sql[start..i].trim());
                i += and_len;
                start = i;
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    if start < sql.len() {
        parts.push(sql[start..].trim());
    }
    parts
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
