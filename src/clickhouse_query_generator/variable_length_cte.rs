use crate::graph_catalog::config::Identifier;
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::query_planner::join_context::VLP_END_ID_COLUMN;
use crate::query_planner::logical_plan::VariableLengthSpec;
use crate::render_plan::Cte;

// ===== VLP Performance and Safety Constants =====

/// Default maximum hops when unspecified in VLP queries.
/// Used as fallback when VariableLengthSpec.max_hops is None.
/// This prevents unbounded recursion in dense graphs.
/// Original value: 10, reduced to 5 for memory safety in dense graphs.
const DEFAULT_MAX_HOPS: u32 = 5;

/// Property to include in the CTE (column name and which node it belongs to)
#[derive(Debug, Clone)]
pub struct NodeProperty {
    pub cypher_alias: String, // "u1" or "u2" - which node this property is for
    pub column_name: String,  // Actual column name in the table (e.g., "full_name")
    pub alias: String,        // Output alias (e.g., "name" or "u1_name")
}

/// Generates recursive CTE SQL for variable-length path traversal
pub struct VariableLengthCteGenerator<'a> {
    pub schema: &'a GraphSchema, // Schema for constraint compilation and property resolution
    pub spec: VariableLengthSpec,
    pub cte_name: String,
    pub start_node_table: String,
    pub start_node_id_column: String, // ID column for start node (e.g., "user_id")
    pub start_node_alias: String,
    pub relationship_table: String,
    pub relationship_from_column: String, // From column in relationship table
    pub relationship_to_column: String,   // To column in relationship table
    pub relationship_alias: String,
    pub end_node_table: String,
    pub end_node_id_column: String, // ID column for end node
    pub end_node_alias: String,
    pub start_cypher_alias: String, // Original Cypher query alias (e.g., "u1")
    pub end_cypher_alias: String,   // Original Cypher query alias (e.g., "u2")
    pub relationship_cypher_alias: String, // Original Cypher relationship alias (e.g., "r" in [r:FOLLOWS*])
    pub properties: Vec<NodeProperty>,     // Properties to include in the CTE
    pub database: Option<String>,          // Optional database prefix
    pub shortest_path_mode: Option<ShortestPathMode>, // Shortest path optimization mode
    pub start_node_filters: Option<String>, // WHERE clause for start node (e.g., "start_node.full_name = 'Alice'")
    pub end_node_filters: Option<String>, // WHERE clause for end node (e.g., "end_full_name = 'Bob'")
    pub relationship_filters: Option<String>, // WHERE clause for relationship (e.g., "rel.weight > 0.5")
    pub path_variable: Option<String>, // Path variable name from MATCH clause (e.g., "p" in "MATCH p = ...")
    pub relationship_types: Option<Vec<String>>, // Relationship type labels (e.g., ["FOLLOWS", "FRIENDS_WITH"])
    pub edge_id: Option<Identifier>, // Edge ID columns for relationship uniqueness (None = use from_id, to_id)
    pub is_denormalized: bool,       // True if BOTH nodes are virtual (for backward compat)
    pub start_is_denormalized: bool, // True if start node is virtual (properties come from edge table)
    pub end_is_denormalized: bool, // True if end node is virtual (properties come from edge table)
    // FK-edge pattern: edge table = node table with FK column (e.g., parent_id -> object_id)
    pub is_fk_edge: bool, // True if relationship is via FK on node table (no separate edge table)
    // Polymorphic edge fields - for filtering unified edge tables by type
    pub type_column: Option<String>, // Discriminator column for relationship type (e.g., "interaction_type")
    pub from_label_column: Option<String>, // Discriminator column for source node type
    pub to_label_column: Option<String>, // Discriminator column for target node type
    pub from_node_label: Option<String>, // Expected value for from_label_column (e.g., "User")
    pub to_node_label: Option<String>, // Expected value for to_label_column (e.g., "Post")
    // Heterogeneous polymorphic path fields - for paths like Group→*→User where
    // intermediate hops traverse Group→Group and only the final hop goes to User
    pub intermediate_node_table: Option<String>, // Table for intermediate nodes (e.g., "groups")
    pub intermediate_node_id_column: Option<String>, // ID column for intermediate nodes (e.g., "group_id")
    pub intermediate_node_label: Option<String>, // Label value for intermediate hops (e.g., "Group")
}

/// Mode for shortest path queries
#[derive(Debug, Clone, PartialEq)]
pub enum ShortestPathMode {
    /// shortestPath() - return one shortest path
    Shortest,
    /// allShortestPaths() - return all paths with minimum length
    AllShortest,
}

// Conversion from logical plan's ShortestPathMode to SQL generator's ShortestPathMode
impl From<crate::query_planner::logical_plan::ShortestPathMode> for ShortestPathMode {
    fn from(mode: crate::query_planner::logical_plan::ShortestPathMode) -> Self {
        use crate::query_planner::logical_plan::ShortestPathMode as LogicalMode;
        match mode {
            LogicalMode::Shortest => ShortestPathMode::Shortest,
            LogicalMode::AllShortest => ShortestPathMode::AllShortest,
        }
    }
}

impl<'a> VariableLengthCteGenerator<'a> {
    pub fn new(
        schema: &'a GraphSchema, // Schema for constraint compilation
        spec: VariableLengthSpec,
        start_table: &str,             // Actual table name (e.g., "users")
        start_id_col: &str,            // ID column name (e.g., "user_id")
        relationship_table: &str,      // Actual relationship table name
        rel_from_col: &str,            // Relationship from column (e.g., "follower_id")
        rel_to_col: &str,              // Relationship to column (e.g., "followed_id")
        end_table: &str,               // Actual table name (e.g., "users")
        end_id_col: &str,              // ID column name (e.g., "user_id")
        start_alias: &str,             // Cypher alias (e.g., "u1")
        end_alias: &str,               // Cypher alias (e.g., "u2")
        properties: Vec<NodeProperty>, // Properties to include in CTE
        shortest_path_mode: Option<ShortestPathMode>, // Shortest path mode
        start_node_filters: Option<String>, // WHERE clause for start node
        end_node_filters: Option<String>, // WHERE clause for end node
        path_variable: Option<String>, // Path variable name (e.g., "p")
        relationship_types: Option<Vec<String>>, // Relationship type labels (e.g., ["FOLLOWS", "FRIENDS_WITH"])
        edge_id: Option<Identifier>,             // Edge ID for relationship uniqueness
    ) -> Self {
        Self::new_with_polymorphic(
            schema,
            spec,
            start_table,
            start_id_col,
            relationship_table,
            rel_from_col,
            rel_to_col,
            end_table,
            end_id_col,
            start_alias,
            end_alias,
            "", // relationship_cypher_alias - default empty for backward compat
            properties,
            shortest_path_mode,
            start_node_filters,
            end_node_filters,
            None, // relationship_filters - default None for backward compat
            path_variable,
            relationship_types,
            edge_id,
            None, // type_column
            None, // from_label_column
            None, // to_label_column
            None, // from_node_label
            None, // to_node_label
        )
    }

    /// Create a generator with polymorphic edge support
    pub fn new_with_polymorphic(
        schema: &'a GraphSchema, // Schema for constraint compilation
        spec: VariableLengthSpec,
        start_table: &str,
        start_id_col: &str,
        relationship_table: &str,
        rel_from_col: &str,
        rel_to_col: &str,
        end_table: &str,
        end_id_col: &str,
        start_alias: &str,
        end_alias: &str,
        relationship_cypher_alias: &str, // Cypher relationship alias (e.g., "r" in [r:FOLLOWS*])
        properties: Vec<NodeProperty>,
        shortest_path_mode: Option<ShortestPathMode>,
        start_node_filters: Option<String>,
        end_node_filters: Option<String>,
        relationship_filters: Option<String>, // Filters on relationship properties
        path_variable: Option<String>,
        relationship_types: Option<Vec<String>>,
        edge_id: Option<Identifier>,
        type_column: Option<String>,
        from_label_column: Option<String>,
        to_label_column: Option<String>,
        from_node_label: Option<String>,
        to_node_label: Option<String>,
    ) -> Self {
        Self::new_with_fk_edge(
            schema,
            spec,
            start_table,
            start_id_col,
            relationship_table,
            rel_from_col,
            rel_to_col,
            end_table,
            end_id_col,
            start_alias,
            end_alias,
            relationship_cypher_alias,
            properties,
            shortest_path_mode,
            start_node_filters,
            end_node_filters,
            relationship_filters,
            path_variable,
            relationship_types,
            edge_id,
            type_column,
            from_label_column,
            to_label_column,
            from_node_label,
            to_node_label,
            false, // is_fk_edge defaults to false
        )
    }

    /// Create a generator with polymorphic edge support and FK-edge flag
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_fk_edge(
        schema: &'a GraphSchema, // Schema for constraint compilation
        spec: VariableLengthSpec,
        start_table: &str,
        start_id_col: &str,
        relationship_table: &str,
        rel_from_col: &str,
        rel_to_col: &str,
        end_table: &str,
        end_id_col: &str,
        start_alias: &str,
        end_alias: &str,
        relationship_cypher_alias: &str, // Cypher relationship alias
        properties: Vec<NodeProperty>,
        shortest_path_mode: Option<ShortestPathMode>,
        start_node_filters: Option<String>,
        end_node_filters: Option<String>,
        relationship_filters: Option<String>, // Filters on relationship properties
        path_variable: Option<String>,
        relationship_types: Option<Vec<String>>,
        edge_id: Option<Identifier>,
        type_column: Option<String>,
        from_label_column: Option<String>,
        to_label_column: Option<String>,
        from_node_label: Option<String>,
        to_node_label: Option<String>,
        is_fk_edge: bool,
    ) -> Self {
        // Try to get database from environment
        let database = std::env::var("CLICKHOUSE_DATABASE").ok();

        Self {
            schema,
            spec,
            cte_name: format!("vlp_{}_{}", start_alias, end_alias),
            start_node_table: start_table.to_string(),
            start_node_id_column: start_id_col.to_string(),
            start_node_alias: "start_node".to_string(),
            relationship_table: relationship_table.to_string(),
            relationship_from_column: rel_from_col.to_string(),
            relationship_to_column: rel_to_col.to_string(),
            relationship_alias: "rel".to_string(),
            end_node_table: end_table.to_string(),
            end_node_id_column: end_id_col.to_string(),
            end_node_alias: "end_node".to_string(),
            start_cypher_alias: start_alias.to_string(),
            end_cypher_alias: end_alias.to_string(),
            relationship_cypher_alias: relationship_cypher_alias.to_string(),
            properties,
            database,
            shortest_path_mode,
            start_node_filters,
            end_node_filters,
            relationship_filters,
            path_variable,
            relationship_types,
            edge_id,
            is_denormalized: false,
            start_is_denormalized: false,
            end_is_denormalized: false,
            is_fk_edge,
            type_column,
            from_label_column,
            to_label_column,
            from_node_label,
            to_node_label,
            // Heterogeneous polymorphic path fields - set later via setter method
            intermediate_node_table: None,
            intermediate_node_id_column: None,
            intermediate_node_label: None,
        }
    }

    /// Create a generator for denormalized edges (node properties embedded in edge table)
    pub fn new_denormalized(
        schema: &'a GraphSchema, // Schema for constraint compilation
        spec: VariableLengthSpec,
        relationship_table: &str, // The only table - edge table with node properties
        rel_from_col: &str,       // From column (e.g., "Origin")
        rel_to_col: &str,         // To column (e.g., "Dest")
        start_alias: &str,        // Cypher alias (e.g., "a")
        end_alias: &str,          // Cypher alias (e.g., "b")
        relationship_cypher_alias: &str, // Cypher relationship alias (e.g., "r")
        properties: Vec<NodeProperty>, // Properties to include in CTE
        shortest_path_mode: Option<ShortestPathMode>,
        start_node_filters: Option<String>,
        end_node_filters: Option<String>,
        relationship_filters: Option<String>, // Filters on relationship properties
        path_variable: Option<String>,
        relationship_types: Option<Vec<String>>,
        edge_id: Option<Identifier>,
    ) -> Self {
        let database = std::env::var("CLICKHOUSE_DATABASE").ok();

        log::debug!(
            "new_denormalized: {} properties, start='{}', end='{}'",
            properties.len(),
            start_alias,
            end_alias
        );

        Self {
            schema,
            spec,
            cte_name: format!("vlp_{}_{}", start_alias, end_alias),
            // For denormalized: node tables are NOT used, only relationship table
            start_node_table: relationship_table.to_string(), // Will be ignored
            start_node_id_column: rel_from_col.to_string(),   // Use from_col as start ID
            start_node_alias: "start_node".to_string(),
            relationship_table: relationship_table.to_string(),
            relationship_from_column: rel_from_col.to_string(),
            relationship_to_column: rel_to_col.to_string(),
            relationship_alias: "rel".to_string(),
            end_node_table: relationship_table.to_string(), // Will be ignored
            end_node_id_column: rel_to_col.to_string(),     // Use to_col as end ID
            end_node_alias: "end_node".to_string(),
            start_cypher_alias: start_alias.to_string(),
            end_cypher_alias: end_alias.to_string(),
            relationship_cypher_alias: relationship_cypher_alias.to_string(),
            properties, // Denormalized properties from edge table
            database,
            shortest_path_mode,
            start_node_filters,
            end_node_filters,
            relationship_filters,
            path_variable,
            relationship_types,
            edge_id,
            is_denormalized: true, // Enable denormalized mode (both nodes)
            start_is_denormalized: true, // Start node is denormalized
            end_is_denormalized: true, // End node is denormalized
            is_fk_edge: false,     // Denormalized edges are not FK-edges
            // Polymorphic edge fields - not used for denormalized edges
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_node_label: None,
            to_node_label: None,
            // Heterogeneous polymorphic path fields - not used for denormalized edges
            intermediate_node_table: None,
            intermediate_node_id_column: None,
            intermediate_node_label: None,
        }
    }

    /// Create a generator for mixed patterns (one node denormalized, one standard)
    #[allow(clippy::too_many_arguments)]
    pub fn new_mixed(
        schema: &'a GraphSchema, // Schema for constraint compilation
        spec: VariableLengthSpec,
        start_table: &str,  // Start node table (or rel table if start is denorm)
        start_id_col: &str, // Start ID column
        relationship_table: &str, // Relationship table
        rel_from_col: &str, // Relationship from column
        rel_to_col: &str,   // Relationship to column
        end_table: &str,    // End node table (or rel table if end is denorm)
        end_id_col: &str,   // End ID column
        start_alias: &str,  // Cypher alias for start node
        end_alias: &str,    // Cypher alias for end node
        relationship_cypher_alias: &str, // Cypher relationship alias
        properties: Vec<NodeProperty>, // Properties to include
        shortest_path_mode: Option<ShortestPathMode>,
        start_node_filters: Option<String>,
        end_node_filters: Option<String>,
        relationship_filters: Option<String>, // Filters on relationship properties
        path_variable: Option<String>,
        relationship_types: Option<Vec<String>>,
        edge_id: Option<Identifier>,
        start_is_denormalized: bool, // Whether start node is denormalized
        end_is_denormalized: bool,   // Whether end node is denormalized
    ) -> Self {
        let database = std::env::var("CLICKHOUSE_DATABASE").ok();

        Self {
            schema,
            spec,
            cte_name: format!("vlp_{}_{}", start_alias, end_alias),
            start_node_table: start_table.to_string(),
            start_node_id_column: start_id_col.to_string(),
            start_node_alias: "start_node".to_string(),
            relationship_table: relationship_table.to_string(),
            relationship_from_column: rel_from_col.to_string(),
            relationship_to_column: rel_to_col.to_string(),
            relationship_alias: "rel".to_string(),
            end_node_table: end_table.to_string(),
            end_node_id_column: end_id_col.to_string(),
            end_node_alias: "end_node".to_string(),
            start_cypher_alias: start_alias.to_string(),
            end_cypher_alias: end_alias.to_string(),
            relationship_cypher_alias: relationship_cypher_alias.to_string(),
            properties,
            database,
            shortest_path_mode,
            start_node_filters,
            end_node_filters,
            relationship_filters,
            path_variable,
            relationship_types,
            edge_id,
            is_denormalized: start_is_denormalized && end_is_denormalized, // Both must be denorm for full denorm mode
            start_is_denormalized,
            end_is_denormalized,
            is_fk_edge: false, // Mixed mode is not FK-edge
            // Polymorphic edge fields - not used for mixed mode yet
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_node_label: None,
            to_node_label: None,
            // Heterogeneous polymorphic path fields - not used for mixed mode
            intermediate_node_table: None,
            intermediate_node_id_column: None,
            intermediate_node_label: None,
        }
    }

    /// Helper to format table name with optional database prefix
    /// If table already contains a dot (already qualified), return as-is
    fn format_table_name(&self, table: &str) -> String {
        // If table is already qualified (contains a dot), don't add prefix again
        if table.contains('.') {
            return table.to_string();
        }

        if let Some(db) = &self.database {
            format!("{}.{}", db, table)
        } else {
            table.to_string()
        }
    }

    /// Generate polymorphic edge filter condition for JOIN ON clause
    /// For polymorphic edges (unified table with type discriminator), adds filters like:
    /// - `rel.interaction_type = 'FOLLOWS'` (type filter)
    /// - `rel.from_label = 'User'` (source node type filter)
    /// - `rel.to_label = 'User'` (target node type filter)
    ///
    /// For multiple relationship types (e.g., [:FOLLOWS|LIKES]):
    /// - `rel.interaction_type IN ('FOLLOWS', 'LIKES')`
    fn generate_polymorphic_edge_filter(&self) -> Option<String> {
        let mut filter_parts = Vec::new();

        // Add type filter if type_column is defined
        if let Some(ref type_col) = self.type_column {
            if let Some(ref rel_types) = self.relationship_types {
                if rel_types.len() == 1 {
                    // Single type: use equality
                    filter_parts.push(format!(
                        "{}.{} = '{}'",
                        self.relationship_alias, type_col, rel_types[0]
                    ));
                } else if rel_types.len() > 1 {
                    // Multiple types: use IN clause
                    let types_list = rel_types
                        .iter()
                        .map(|t| format!("'{}'", t))
                        .collect::<Vec<_>>()
                        .join(", ");
                    filter_parts.push(format!(
                        "{}.{} IN ({})",
                        self.relationship_alias, type_col, types_list
                    ));
                }
            }
        }

        // Add from_label filter if from_label_column is defined
        if let Some(ref from_label_col) = self.from_label_column {
            if let Some(ref from_label) = self.from_node_label {
                filter_parts.push(format!(
                    "{}.{} = '{}'",
                    self.relationship_alias, from_label_col, from_label
                ));
            }
        }

        // Add to_label filter if to_label_column is defined
        if let Some(ref to_label_col) = self.to_label_column {
            if let Some(ref to_label) = self.to_node_label {
                filter_parts.push(format!(
                    "{}.{} = '{}'",
                    self.relationship_alias, to_label_col, to_label
                ));
            }
        }

        if filter_parts.is_empty() {
            None
        } else {
            let filter = filter_parts.join(" AND ");
            crate::debug_print!("    🔹 VLP polymorphic edge filter: {}", filter);
            Some(filter)
        }
    }

    /// Generate edge constraint expression for JOIN/WHERE clause
    /// Compiles constraint from schema (e.g., "from.timestamp <= to.timestamp")
    /// into SQL (e.g., "start_node.created_at <= end_node.created_at")
    ///
    /// Constraints are added to:
    /// - Base case: WHERE clause (after node JOINs)
    /// - Recursive case: WHERE clause (validates each hop)
    ///
    /// Generate edge constraint filter with dynamic alias support
    ///
    /// For recursive cases, pass the actual aliases used in that SQL block.
    /// If None, defaults to self.start_node_alias and self.end_node_alias.
    fn generate_edge_constraint_filter(
        &self,
        from_alias: Option<&str>,
        to_alias: Option<&str>,
    ) -> Option<String> {
        // Get the first relationship type (multi-type not supported for constraints)
        if let Some(rel_types) = &self.relationship_types {
            if let Some(rel_type) = rel_types.first() {
                // Look up relationship schema
                if let Some(rel_schema) = self.schema.get_relationships_schema_opt(rel_type) {
                    // Check if constraints are defined
                    if let Some(ref constraint_expr) = rel_schema.constraints {
                        // Get node schemas for from/to nodes
                        if let (Some(from_node_schema), Some(to_node_schema)) = (
                            self.schema.node_schema_opt(&rel_schema.from_node),
                            self.schema.node_schema_opt(&rel_schema.to_node),
                        ) {
                            // Use provided aliases or fall back to defaults
                            let actual_from_alias = from_alias.unwrap_or(&self.start_node_alias);
                            let actual_to_alias = to_alias.unwrap_or(&self.end_node_alias);

                            // Compile the constraint expression
                            match crate::graph_catalog::constraint_compiler::compile_constraint(
                                constraint_expr,
                                from_node_schema,
                                to_node_schema,
                                actual_from_alias,
                                actual_to_alias,
                            ) {
                                Ok(compiled_sql) => {
                                    log::debug!(
                                        "✅ Compiled VLP edge constraint for {} (from={}, to={}): {} → {}",
                                        rel_type, actual_from_alias, actual_to_alias, constraint_expr, compiled_sql
                                    );
                                    return Some(compiled_sql);
                                }
                                Err(e) => {
                                    log::warn!(
                                        "⚠️  Failed to compile VLP edge constraint for {}: {}",
                                        rel_type,
                                        e
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
        None
    }

    /// Generate edge constraint filter for recursive case
    ///
    /// In the recursive case, we don't have a `current_node` join (removed for performance).
    /// Instead, the "from" node properties come from the CTE (vp.end_<alias>) and
    /// the "to" node properties come from the joined end_node table.
    ///
    /// Maps:
    /// - `from.property` → `vp.end_<property_alias>` (from CTE columns)
    /// - `to.property` → `end_node.<column_name>` (from joined table)
    fn generate_edge_constraint_filter_recursive(&self) -> Option<String> {
        // Get the first relationship type (multi-type not supported for constraints)
        let rel_types = self.relationship_types.as_ref()?;
        let rel_type = rel_types.first()?;

        // Look up relationship schema
        let rel_schema = self.schema.get_relationships_schema_opt(rel_type)?;

        // Check if constraints are defined
        let constraint_expr = rel_schema.constraints.as_ref()?;

        // Get node schemas for property resolution
        let from_node_schema = self.schema.node_schema_opt(&rel_schema.from_node)?;
        let to_node_schema = self.schema.node_schema_opt(&rel_schema.to_node)?;

        // Build the constraint by replacing property references
        // Pattern: from.property -> vp.end_<alias>, to.property -> end_node.<column>
        let mut compiled = constraint_expr.clone();

        // Replace from.property references with vp.end_<property_alias>
        // We need to find the property alias from self.properties
        for (property_name, mapping) in &from_node_schema.property_mappings {
            let from_pattern = format!("from.{}", property_name);
            if compiled.contains(&from_pattern) {
                // Find the corresponding property alias in self.properties
                // The property alias is based on the cypher property name, not column name
                let column_name = match mapping {
                    crate::graph_catalog::expression_parser::PropertyValue::Column(col) => {
                        col.clone()
                    }
                    crate::graph_catalog::expression_parser::PropertyValue::Expression(_) => {
                        continue
                    }
                };

                // Find the alias used in CTE for this column
                // Properties in CTE are stored as end_<alias> where alias is the property's output name
                let cte_alias = self
                    .properties
                    .iter()
                    .find(|p| {
                        p.column_name == column_name && p.cypher_alias == self.end_cypher_alias
                    })
                    .map(|p| p.alias.clone())
                    .unwrap_or_else(|| property_name.clone());

                let replacement = format!("vp.end_{}", cte_alias);
                compiled = compiled.replace(&from_pattern, &replacement);
            }
        }

        // Replace to.property references with end_node.<column_name>
        for (property_name, mapping) in &to_node_schema.property_mappings {
            let to_pattern = format!("to.{}", property_name);
            if compiled.contains(&to_pattern) {
                let column_name = match mapping {
                    crate::graph_catalog::expression_parser::PropertyValue::Column(col) => {
                        col.clone()
                    }
                    crate::graph_catalog::expression_parser::PropertyValue::Expression(_) => {
                        continue
                    }
                };

                let replacement = format!("{}.{}", self.end_node_alias, column_name);
                compiled = compiled.replace(&to_pattern, &replacement);
            }
        }

        log::debug!(
            "✅ Compiled VLP edge constraint for recursive case: {} → {}",
            constraint_expr,
            compiled
        );

        Some(compiled)
    }

    /// Generate relationship type expression for a given hop
    fn generate_relationship_type_for_hop(&self, _hop_count: u32) -> String {
        // For now, return the first relationship type if available, otherwise a placeholder
        if let Some(ref types) = self.relationship_types {
            if let Some(first_type) = types.first() {
                format!("['{}'] as path_relationships", first_type)
            } else {
                "[] as path_relationships".to_string()
            }
        } else {
            "[] as path_relationships".to_string()
        }
    }

    /// Get relationship type array for appending in recursive case
    fn get_relationship_type_array(&self) -> String {
        if let Some(ref types) = self.relationship_types {
            if let Some(first_type) = types.first() {
                format!("['{}']", first_type)
            } else {
                "[]".to_string()
            }
        } else {
            "[]".to_string()
        }
    }

    /// Check if this is a heterogeneous polymorphic path (e.g., Group→*→User)
    /// where intermediate hops traverse through one type and final hop goes to another type.
    ///
    /// Conditions for heterogeneous polymorphic path:
    /// 1. has to_label_column (polymorphic edge with target type discriminator)
    /// 2. start_node_table != end_node_table (different node types)
    /// 3. intermediate_node_table is set (specifies intermediate traversal type)
    fn is_heterogeneous_polymorphic_path(&self) -> bool {
        self.to_label_column.is_some()
            && self.start_node_table != self.end_node_table
            && self.intermediate_node_table.is_some()
    }

    /// Generate polymorphic edge filter for INTERMEDIATE hops (Group→Group)
    /// Uses the intermediate_node_label instead of to_node_label
    fn generate_polymorphic_edge_filter_intermediate(&self) -> Option<String> {
        let mut filter_parts = Vec::new();

        // Add type filter if type_column is defined
        if let Some(ref type_col) = self.type_column {
            if let Some(ref rel_types) = self.relationship_types {
                if rel_types.len() == 1 {
                    filter_parts.push(format!(
                        "{}.{} = '{}'",
                        self.relationship_alias, type_col, rel_types[0]
                    ));
                } else if rel_types.len() > 1 {
                    let types_list = rel_types
                        .iter()
                        .map(|t| format!("'{}'", t))
                        .collect::<Vec<_>>()
                        .join(", ");
                    filter_parts.push(format!(
                        "{}.{} IN ({})",
                        self.relationship_alias, type_col, types_list
                    ));
                }
            }
        }

        // For intermediate hops: use intermediate_node_label for to_label filter
        if let Some(ref to_label_col) = self.to_label_column {
            if let Some(ref intermediate_label) = self.intermediate_node_label {
                filter_parts.push(format!(
                    "{}.{} = '{}'",
                    self.relationship_alias, to_label_col, intermediate_label
                ));
            }
        }

        if filter_parts.is_empty() {
            None
        } else {
            let filter = filter_parts.join(" AND ");
            crate::debug_print!(
                "    🔹 VLP polymorphic edge filter (intermediate): {}",
                filter
            );
            Some(filter)
        }
    }

    /// Set intermediate node info for heterogeneous polymorphic paths
    pub fn set_intermediate_node(&mut self, table: &str, id_column: &str, label: &str) {
        self.intermediate_node_table = Some(table.to_string());
        self.intermediate_node_id_column = Some(id_column.to_string());
        self.intermediate_node_label = Some(label.to_string());
    }

    /// Build edge tuple expression for the base case (first hop)
    /// Returns SQL expression like: `tuple(rel.from_id, rel.to_id)` or `tuple(rel.date, rel.num, ...)`
    fn build_edge_tuple_base(&self) -> String {
        match &self.edge_id {
            Some(Identifier::Single(col)) => {
                // Single column edge ID: just use that column
                format!("{}.{}", self.relationship_alias, col)
            }
            Some(Identifier::Composite(cols)) => {
                // Multi-column composite key: build tuple
                let tuple_elements: Vec<String> = cols
                    .iter()
                    .map(|col| format!("{}.{}", self.relationship_alias, col))
                    .collect();
                format!("tuple({})", tuple_elements.join(", "))
            }
            None => {
                // Default: use (from_id, to_id) as edge identity
                format!(
                    "tuple({}.{}, {}.{})",
                    self.relationship_alias,
                    self.relationship_from_column,
                    self.relationship_alias,
                    self.relationship_to_column
                )
            }
        }
    }

    /// Build edge tuple expression for recursive case
    /// Returns SQL expression like: `tuple(r.from_id, r.to_id)` or `tuple(r.date, r.num, ...)`
    fn build_edge_tuple_recursive(&self, rel_alias: &str) -> String {
        match &self.edge_id {
            Some(Identifier::Single(col)) => {
                format!("{}.{}", rel_alias, col)
            }
            Some(Identifier::Composite(cols)) => {
                let tuple_elements: Vec<String> = cols
                    .iter()
                    .map(|col| format!("{}.{}", rel_alias, col))
                    .collect();
                format!("tuple({})", tuple_elements.join(", "))
            }
            None => {
                format!(
                    "tuple({}.{}, {}.{})",
                    rel_alias,
                    self.relationship_from_column,
                    rel_alias,
                    self.relationship_to_column
                )
            }
        }
    }

    /// Get the ClickHouse array type for path_edges
    /// Returns type like: `Array(Tuple(UInt32, UInt32))` or `Array(Tuple(String, String, ...))`
    fn get_path_edges_array_type(&self) -> String {
        match &self.edge_id {
            Some(Identifier::Single(_)) => {
                // For single column, we don't know the type - assume UInt64 for now
                // TODO: Get actual column type from schema
                "Array(UInt64)".to_string()
            }
            Some(Identifier::Composite(cols)) => {
                // For composite keys, build tuple type
                // TODO: Get actual column types from schema - assuming String for now
                let type_elements = vec!["String"; cols.len()].join(", ");
                format!("Array(Tuple({}))", type_elements)
            }
            None => {
                // Default (from_id, to_id) - assume both are UInt64
                "Array(Tuple(UInt64, UInt64))".to_string()
            }
        }
    }

    /// Map a logical property name to physical column name for denormalized nodes.
    /// Uses from_properties or to_properties mappings from schema.
    fn map_denormalized_property(
        &self,
        logical_prop: &str,
        is_from_node: bool,
    ) -> Result<String, String> {
        // For denormalized nodes, find the node schema that points to our relationship table
        let node_schemas = self.schema.all_node_schemas();

        // Strip database prefix for comparison (handles both "flights" and "db.flights")
        let rel_table_name = self
            .relationship_table
            .rsplit('.')
            .next()
            .unwrap_or(&self.relationship_table);

        let node_schema = node_schemas
            .values()
            .find(|n| {
                let schema_table = n.table_name.rsplit('.').next().unwrap_or(&n.table_name);
                schema_table == rel_table_name
            })
            .ok_or_else(|| format!("No node schema found for table '{}'", rel_table_name))?;

        let property_map = if is_from_node {
            node_schema.from_properties.as_ref()
        } else {
            node_schema.to_properties.as_ref()
        };

        property_map
            .and_then(|map| map.get(logical_prop))
            .map(|col| col.to_string())
            .ok_or_else(|| {
                format!(
                    "Property '{}' not found in {} for denormalized node in table '{}'",
                    logical_prop,
                    if is_from_node {
                        "from_properties"
                    } else {
                        "to_properties"
                    },
                    self.relationship_table
                )
            })
    }

    /// Generate the recursive CTE for variable-length traversal
    pub fn generate_cte(&self) -> Cte {
        let cte_sql = self.generate_recursive_sql();

        Cte::new_vlp(
            self.cte_name.clone(),
            crate::render_plan::CteContent::RawSql(cte_sql),
            true, // is_recursive
            self.start_node_alias.clone(),
            self.end_node_alias.clone(),
            self.start_node_table.clone(),
            self.end_node_table.clone(),
            self.start_cypher_alias.clone(),   // Add Cypher alias
            self.end_cypher_alias.clone(),     // Add Cypher alias
            self.start_node_id_column.clone(), // 🔧 FIX: Pass actual ID columns (from rel schema)
            self.end_node_id_column.clone(),
            self.path_variable.clone(), // Path variable for length(p), nodes(p) rewriting
        )
    }

    /// Rewrite end node filter for use in intermediate CTEs
    /// Transforms "end_node.property" references to "end_property" column names
    fn rewrite_end_filter_for_cte(&self, filter: &str) -> String {
        // Replace end_node.{id_column} with end_id (uses VLP_END_ID_COLUMN constant)
        let mut rewritten = filter.replace(
            &format!("{}.{}", self.end_node_alias, self.end_node_id_column),
            VLP_END_ID_COLUMN,
        );

        // Replace end_node.{property} with end_{property} for each property
        // Try both ClickHouse column name and Cypher alias since filters can use either
        for prop in &self.properties {
            if prop.cypher_alias == self.end_cypher_alias {
                // Try ClickHouse column name (e.g., end_node.full_name → end_name)
                let pattern_col = format!("{}.{}", self.end_node_alias, prop.column_name);
                let replacement = format!("end_{}", prop.alias);
                rewritten = rewritten.replace(&pattern_col, &replacement);

                // Also try Cypher alias (e.g., end_node.name → end_name)
                let pattern_alias = format!("{}.{}", self.end_node_alias, prop.alias);
                rewritten = rewritten.replace(&pattern_alias, &replacement);
            }
        }

        rewritten
    }

    // Note: extract_simple_equality_filter was removed as dead code (never called)

    /// Generate the actual recursive SQL string
    fn generate_recursive_sql(&self) -> String {
        // For heterogeneous polymorphic paths, use special two-CTE structure
        if self.is_heterogeneous_polymorphic_path() {
            return self.generate_heterogeneous_polymorphic_sql();
        }

        let min_hops = self.spec.effective_min_hops();
        let max_hops = self.spec.max_hops;

        // Determine if we need an _inner CTE wrapper
        // This is needed when we have:
        // 1. Shortest path mode (which requires post-processing)
        // 2. min_hops > 1 (base case generates hop 1, but we need to filter)
        // 3. Denormalized VLP with end_node_filters (can't filter in base case, must wrap)
        let denorm_needs_end_filter_wrapper = self.is_denormalized
            && self.end_node_filters.is_some()
            && self.shortest_path_mode.is_none();
        let needs_inner_cte =
            self.shortest_path_mode.is_some() || min_hops > 1 || denorm_needs_end_filter_wrapper;
        let recursive_cte_name = if needs_inner_cte {
            format!("{}_inner", self.cte_name)
        } else {
            self.cte_name.clone()
        };

        // Generate the core recursive query body (without CTE name wrapper)
        let mut query_body = String::new();

        // Special case: For shortest path self-loops (a to a), only zero-hop is needed
        let is_shortest_self_loop = self.shortest_path_mode.is_some()
            && min_hops == 0
            && self.start_cypher_alias == self.end_cypher_alias;

        // Base case: ONE base case, either zero-hop or 1-hop depending on min_hops
        if min_hops == 0 {
            // Zero-hop base case for patterns like *0.., *0..5
            query_body.push_str(&self.generate_zero_hop_base_case());
        } else {
            // 1-hop base case for patterns like *, *1.., *2..
            // (recursion will extend to 2+ hops)
            query_body.push_str(&self.generate_base_case(1));
        }

        // Recursive case: Add if we need more than just the base case
        // Skip for shortest path self-loops (zero-hop is always the answer)
        // Skip if max_hops == Some(0) (only zero-hop allowed)
        let needs_recursion = !is_shortest_self_loop
            && max_hops != Some(0)
            && (max_hops.is_none() || max_hops.unwrap() > min_hops);

        if needs_recursion {
            // Note: UNION DISTINCT is not supported in ClickHouse recursive CTEs.
            // Using UNION ALL means duplicate edges in the data can cause exponential
            // row explosion. The path_edges tracking with `NOT has()` prevents cycles
            // but not duplicate edges between the same nodes.
            //
            // Mitigation: Ensure edge tables have unique (from_id, to_id) pairs,
            // or the application should enforce this constraint before loading data.
            query_body.push_str("\n    UNION ALL\n");

            let default_depth = max_hops.unwrap_or_else(|| {
                // Unbounded case: use conservative default to prevent memory exhaustion
                // In dense graphs, each hop can multiply rows exponentially
                // Users who need longer paths should specify explicit bounds
                if self.shortest_path_mode.is_some() {
                    // For shortestPath queries, use lower limit
                    // Most social graphs have small-world property (6 degrees of separation)
                    5
                } else if min_hops == 0 {
                    3 // Lower limit for zero-hop base queries
                } else {
                    // Standard default for regular variable-length paths
                    // Reduced from 10 to 5 to prevent row explosion in dense graphs
                    5
                }
            });

            query_body.push_str(
                &self.generate_recursive_case_with_cte_name(default_depth, &recursive_cte_name),
            );
        }

        // Build CTE structure based on shortest path mode and filters
        // For shortest path queries, end filters are now applied during path generation
        // in the inner CTE, so we don't need separate filtering steps
        let sql = match (&self.shortest_path_mode, &self.end_node_filters) {
            (Some(ShortestPathMode::Shortest), Some(end_filters)) => {
                // Rewrite end filter for use in intermediate CTE
                // Replace "end_node.property" with "end_property" (column names in CTE)
                let rewritten_filter = self.rewrite_end_filter_for_cte(end_filters);

                // Add min_hops and max_hops constraints if needed
                let min_hops = self.spec.effective_min_hops();
                let max_hops = self.spec.max_hops;

                let mut filter_with_bounds = rewritten_filter.clone();
                if min_hops > 1 {
                    filter_with_bounds =
                        format!("({}) AND hop_count >= {}", filter_with_bounds, min_hops);
                }
                if let Some(max) = max_hops {
                    filter_with_bounds =
                        format!("({}) AND hop_count <= {}", filter_with_bounds, max);
                }

                // CORRECT ORDER: Filter to target FIRST (with min/max_hops), then find shortest path from EACH start node
                // This ensures we get the shortest path TO THE TARGET within hop bounds from each source
                format!(
                    "{}_inner AS (\n{}\n),\n{}_to_target AS (\n    SELECT * FROM {}_inner WHERE {}\n),\n{} AS (\n    SELECT * FROM (\n        SELECT *, ROW_NUMBER() OVER (PARTITION BY start_id ORDER BY hop_count ASC) as rn\n        FROM {}_to_target\n    ) WHERE rn = 1\n)",
                    self.cte_name,
                    query_body,
                    self.cte_name,
                    self.cte_name,
                    filter_with_bounds,
                    self.cte_name,
                    self.cte_name
                )
            }
            (Some(ShortestPathMode::AllShortest), Some(end_filters)) => {
                // Rewrite end filter for use in intermediate CTE
                let rewritten_filter = self.rewrite_end_filter_for_cte(end_filters);

                // Add min_hops and max_hops constraints if needed
                let min_hops = self.spec.effective_min_hops();
                let max_hops = self.spec.max_hops;

                let mut filter_with_bounds = rewritten_filter.clone();
                if min_hops > 1 {
                    filter_with_bounds =
                        format!("({}) AND hop_count >= {}", filter_with_bounds, min_hops);
                }
                if let Some(max) = max_hops {
                    filter_with_bounds =
                        format!("({}) AND hop_count <= {}", filter_with_bounds, max);
                }

                // CORRECT ORDER: Filter to target FIRST (with min/max_hops), then find shortest path from EACH start node
                format!(
                    "{}_inner AS (\n{}\n),\n{}_to_target AS (\n    SELECT * FROM {}_inner WHERE {}\n),\n{} AS (\n    SELECT * FROM (\n        SELECT *, ROW_NUMBER() OVER (PARTITION BY start_id ORDER BY hop_count ASC) as rn\n        FROM {}_to_target\n    ) WHERE rn = 1\n)",
                    self.cte_name,
                    query_body,
                    self.cte_name,
                    self.cte_name,
                    filter_with_bounds,
                    self.cte_name,
                    self.cte_name
                )
            }
            (Some(ShortestPathMode::Shortest), None) => {
                // 2-tier: inner → select shortest path to EACH end node (no target filter)
                // Use window function to get the shortest path to each distinct end_id
                format!(
                    "{}_inner AS (\n{}\n),\n{} AS (\n    SELECT * FROM (\n        SELECT *, ROW_NUMBER() OVER (PARTITION BY end_id ORDER BY hop_count ASC) as rn\n        FROM {}_inner\n    ) WHERE rn = 1\n)",
                    self.cte_name, query_body, self.cte_name, self.cte_name
                )
            }
            (Some(ShortestPathMode::AllShortest), None) => {
                // 2-tier: inner → select all shortest (no target filter)
                format!(
                    "{}_inner AS (\n{}\n),\n{} AS (\n    SELECT * FROM {}_inner WHERE hop_count = (SELECT MIN(hop_count) FROM {}_inner)\n)",
                    self.cte_name, query_body, self.cte_name, self.cte_name, self.cte_name
                )
            }
            (None, Some(end_filters)) => {
                // For denormalized VLP, end filters are NOT applied in base/recursive cases
                // (to allow multi-hop paths). They must be applied in a wrapper CTE.
                //
                // For standard VLP, end filters ARE applied in base/recursive cases,
                // so we don't need to filter again here.
                if self.is_denormalized {
                    // Denormalized: Apply end filter in wrapper CTE
                    // The end_filters string uses "end_node.X" which maps to the CTE's output columns
                    // But for denormalized, CTE columns use physical names (e.g., "Dest" not "end_node.code")
                    // The filter was already rewritten during categorization, so it should use the rel alias
                    // which maps to the CTE alias in the wrapper
                    // 🔧 FIX: Rewrite end_node filter for denormalized VLP with prefixed columns
                    // Replace "end_node.property" with "vlp_inner.end_property" (prefixed columns)
                    let rewritten_filter =
                        end_filters.replace("end_node.", &format!("{}_inner.end_", self.cte_name));
                    // Also handle rel alias replacement (e.g., "rel.Dest" -> "vlp_inner.end_Dest")
                    // For denormalized schemas, the end node ID is in the "end_" prefixed column
                    let rewritten_filter = rewritten_filter.replace(
                        &format!("{}.", self.relationship_alias),
                        &format!("{}_inner.end_", self.cte_name),
                    );

                    let min_hops_filter = if min_hops > 1 {
                        format!(" AND hop_count >= {}", min_hops)
                    } else {
                        String::new()
                    };

                    format!(
                        "{}_inner AS (\n{}\n),\n{} AS (\n    SELECT * FROM {}_inner WHERE ({}){}\n)",
                        self.cte_name, query_body, self.cte_name, self.cte_name, rewritten_filter, min_hops_filter
                    )
                } else {
                    // Standard VLP: end filters already applied in base/recursive cases
                    if min_hops > 1 {
                        format!(
                            "{}_inner AS (\n{}\n),\n{} AS (\n    SELECT * FROM {}_inner WHERE hop_count >= {}\n)",
                            self.cte_name, query_body, self.cte_name, self.cte_name, min_hops
                        )
                    } else {
                        format!("{} AS (\n{}\n)", self.cte_name, query_body)
                    }
                }
            }
            (None, None) => {
                // Apply min_hops filtering if needed
                // Base case starts at hop 1 to allow recursion, but we need to filter
                // results to respect min_hops (e.g., *2.. should only return hop_count >= 2)
                // max_hops is already enforced by recursion termination condition
                if min_hops > 1 {
                    format!(
                        "{}_inner AS (\n{}\n),\n{} AS (\n    SELECT * FROM {}_inner WHERE hop_count >= {}\n)",
                        self.cte_name, query_body, self.cte_name, self.cte_name, min_hops
                    )
                } else {
                    // No filtering needed (min_hops <= 1)
                    format!("{} AS (\n{}\n)", self.cte_name, query_body)
                }
            }
        };

        sql
    }

    /// Generate SQL for heterogeneous polymorphic paths (e.g., Group→*→User)
    ///
    /// Uses a two-phase CTE structure:
    /// 1. `reachable_intermediates`: Recursively finds all intermediate nodes (Groups) reachable from start
    /// 2. Main CTE: Joins ALL reachable intermediates to end nodes (Users) via the relationship
    ///
    /// Key insight: At each intermediate node, we can either:
    /// - Continue recursion (target is another intermediate/Group)
    /// - Collect terminal result (target is end node/User)
    ///
    /// The final result includes Users reachable from ANY intermediate Group at ANY depth.
    fn generate_heterogeneous_polymorphic_sql(&self) -> String {
        let intermediate_table = self
            .intermediate_node_table
            .as_ref()
            .expect("intermediate_node_table must be set");
        let intermediate_id_col = self
            .intermediate_node_id_column
            .as_ref()
            .expect("intermediate_node_id_column must be set");
        let intermediate_label = self
            .intermediate_node_label
            .as_ref()
            .expect("intermediate_node_label must be set");

        let min_hops = self.spec.effective_min_hops();
        let max_hops = self.spec.max_hops.unwrap_or(DEFAULT_MAX_HOPS);

        crate::debug_print!("    🔸 Generating heterogeneous polymorphic SQL (two-phase):");
        crate::debug_print!(
            "      - start_table: {}, intermediate_table: {}, end_table: {}",
            self.start_node_table,
            intermediate_table,
            self.end_node_table
        );
        crate::debug_print!(
            "      - intermediate_label: {}, to_node_label: {:?}",
            intermediate_label,
            self.to_node_label
        );
        crate::debug_print!("      - min_hops: {}, max_hops: {}", min_hops, max_hops);

        let reachable_cte_name = format!("{}_reachable", self.cte_name);

        // Build qualified table names
        let start_table_qualified = self.format_table_name(&self.start_node_table);
        let rel_table_qualified = self.format_table_name(&self.relationship_table);
        let intermediate_table_qualified = self.format_table_name(intermediate_table);
        let end_table_qualified = self.format_table_name(&self.end_node_table);

        // Build start node filter if exists
        // Replace "start_node." with the actual table name for the base case
        let start_filter = if let Some(ref filter) = self.start_node_filters {
            let rewritten = filter.replace("start_node.", &format!("{}.", start_table_qualified));
            format!("\n    WHERE {}", rewritten)
        } else {
            String::new()
        };

        // Build polymorphic filter for intermediate hops (member_type = 'Group')
        let intermediate_poly_filter = if let Some(ref to_label_col) = self.to_label_column {
            format!(
                "{}.{} = '{}'",
                self.relationship_alias, to_label_col, intermediate_label
            )
        } else {
            "1=1".to_string()
        };

        // Build polymorphic filter for final hop to end nodes (member_type = 'User')
        let end_poly_filter = if let Some(ref to_label_col) = self.to_label_column {
            if let Some(ref to_label) = self.to_node_label {
                format!(
                    "{}.{} = '{}'",
                    self.relationship_alias, to_label_col, to_label
                )
            } else {
                "1=1".to_string()
            }
        } else {
            "1=1".to_string()
        };

        // ============================================================
        // CTE 1: Find all reachable intermediate nodes (groups)
        // This includes the start node at depth 0, then recurses through
        // intermediate->intermediate relationships (Group->Group)
        // ============================================================
        let reachable_cte = format!(
            "{reachable_cte} AS (\n\
            -- Base case: Start nodes at depth 0\n\
            SELECT \n\
                {start_table}.{start_id} as node_id,\n\
                0 as depth\n\
            FROM {start_table}{start_filter}\n\
            \n\
            UNION ALL\n\
            \n\
            -- Recursive case: Traverse to child intermediates (Group->Group)\n\
            SELECT \n\
                {intermediate_table}.{intermediate_id} as node_id,\n\
                r.depth + 1 as depth\n\
            FROM {reachable_cte} r\n\
            JOIN {rel_table} {rel} ON r.node_id = {rel}.{from_col}\n\
            JOIN {intermediate_table} ON {rel}.{to_col} = {intermediate_table}.{intermediate_id}\n\
            WHERE r.depth < {max_hops}\n\
                AND {intermediate_poly_filter}\n\
        )",
            reachable_cte = reachable_cte_name,
            start_table = start_table_qualified,
            start_id = self.start_node_id_column,
            start_filter = start_filter,
            intermediate_table = intermediate_table_qualified,
            intermediate_id = intermediate_id_col,
            rel_table = rel_table_qualified,
            rel = self.relationship_alias,
            from_col = self.relationship_from_column,
            to_col = self.relationship_to_column,
            max_hops = max_hops,
            intermediate_poly_filter = intermediate_poly_filter,
        );

        // ============================================================
        // CTE 2: Collect end nodes (Users) from ALL reachable intermediates
        // Users at depth+1 from each reachable Group are included
        // This is the main CTE that produces the final result
        // ============================================================

        // Build property selections for end nodes
        let mut prop_selects = Vec::new();
        for prop in &self.properties {
            if prop.cypher_alias == self.end_cypher_alias {
                prop_selects.push(format!(
                    "{}.{} as end_{}",
                    self.end_node_alias, prop.column_name, prop.alias
                ));
            }
        }
        let props_clause = if prop_selects.is_empty() {
            String::new()
        } else {
            format!(",\n        {}", prop_selects.join(",\n        "))
        };

        // Build end node filter if exists (e.g., user property filters)
        let end_filter = if let Some(ref filter) = self.end_node_filters {
            format!("\n    AND {}", filter)
        } else {
            String::new()
        };

        // Apply min_hops and max_hops filters
        // The User is at depth+1 from the Group that contains them
        let hop_filter = format!(
            "\n    AND r.depth + 1 >= {} AND r.depth + 1 <= {}",
            min_hops, max_hops
        );

        let main_cte = format!(
            "{main_cte} AS (\n\
            -- Collect end nodes (Users) from all reachable intermediates (Groups)\n\
            SELECT \n\
                r.node_id as start_id,\n\
                {end_table}.{end_id} as end_id,\n\
                r.depth + 1 as hop_count,\n\
                CAST([] AS Array(Tuple(UInt64, UInt64))) as path_edges,\n\
                CAST([] AS Array(String)) as path_relationships,\n\
                CAST([] AS Array(UInt64)) as path_nodes{props_clause}\n\
            FROM {reachable_cte} r\n\
            JOIN {rel_table} {rel} ON r.node_id = {rel}.{from_col}\n\
            JOIN {end_table} {end} ON {rel}.{to_col} = {end}.{end_id}\n\
            WHERE {end_poly_filter}{end_filter}{hop_filter}\n\
        )",
            main_cte = self.cte_name,
            reachable_cte = reachable_cte_name,
            end_table = end_table_qualified,
            end_id = self.end_node_id_column,
            props_clause = props_clause,
            rel_table = rel_table_qualified,
            rel = self.relationship_alias,
            from_col = self.relationship_from_column,
            to_col = self.relationship_to_column,
            end = self.end_node_alias,
            end_poly_filter = end_poly_filter,
            end_filter = end_filter,
            hop_filter = hop_filter,
        );

        format!("{},\n{}", reachable_cte, main_cte)
    }

    /// Generate base case for zero hops (self-loop)
    /// Used with shortest path functions when pattern is *0..
    fn generate_zero_hop_base_case(&self) -> String {
        let path_edges_type = self.get_path_edges_array_type();

        let mut select_items = vec![
            format!(
                "{}.{} as start_id",
                self.start_node_alias, self.start_node_id_column
            ),
            format!(
                "{}.{} as end_id",
                self.start_node_alias,
                self.start_node_id_column // Same node for self-loop
            ),
            "0 as hop_count".to_string(), // Zero hops
            format!("CAST([] AS {}) as path_edges", path_edges_type), // Empty edge array
            "CAST([] AS Array(String)) as path_relationships".to_string(), // Empty array with explicit type
            // Add path_nodes for UNWIND nodes(p) support - for zero hop, just the start node
            format!(
                "[{}.{}] as path_nodes",
                self.start_node_alias, self.start_node_id_column
            ),
        ];

        // Add properties for start node (which is also the end node)
        for prop in &self.properties {
            if prop.cypher_alias == self.start_cypher_alias {
                // Skip ID column when it's the actual id property (already added as start_id)
                // But keep it if it's a different property that happens to be the ID column
                // (e.g., "node_id" as a separate property in some schemas)
                if prop.column_name == self.start_node_id_column && prop.alias == "id" {
                    continue;
                }
                
                select_items.push(format!(
                    "{}.{} as start_{}",
                    self.start_node_alias, prop.column_name, prop.alias
                ));
            }
            // For zero-hop, end properties are same as start properties
            if prop.cypher_alias == self.end_cypher_alias {
                // Skip ID column when it's the actual id property (already added as end_id)
                if prop.column_name == self.end_node_id_column && prop.alias == "id" {
                    continue;
                }
                
                select_items.push(format!(
                    "{}.{} as end_{}",
                    self.start_node_alias, prop.column_name, prop.alias
                ));
            }
        }

        let select_clause = select_items.join(",\n        ");

        // Build the zero-hop query - just select from start table
        let mut query = format!(
            "    SELECT \n        {}\n    FROM {} AS {}",
            select_clause,
            self.format_table_name(&self.start_node_table),
            self.start_node_alias
        );

        // Apply start_node_filters (WHERE clause)
        let mut where_conditions = Vec::new();
        if let Some(ref filters) = self.start_node_filters {
            where_conditions.push(filters.clone());
        }

        // For zero-hop self-loops, the end node is the same as start node
        // So end_node_filters should also be applied, but rewritten for start node
        if let Some(ref filters) = self.end_node_filters {
            // Rewrite end_node references to start_node references
            let rewritten = filters.replace(
                &format!("{}.", self.end_node_alias),
                &format!("{}.", self.start_node_alias),
            );
            where_conditions.push(rewritten);
        }

        if !where_conditions.is_empty() {
            query.push_str("\n    WHERE ");
            query.push_str(&where_conditions.join(" AND "));
        }

        query
    }

    fn generate_base_case(&self, hop_count: u32) -> String {
        // Determine which pattern to use based on denormalization flags
        // Full denormalized: both nodes virtual → use denormalized generator
        // Mixed: one node virtual, one standard → use mixed generator
        // FK-edge: edge table = node table with FK column → 2-way join (no separate rel)
        // Full standard: both nodes standard → use standard generator

        if self.is_denormalized {
            // Both nodes denormalized (fully virtual)
            return self.generate_denormalized_base_case(hop_count);
        }

        // Check for mixed patterns (one side denormalized)
        if self.start_is_denormalized || self.end_is_denormalized {
            return self.generate_mixed_base_case(hop_count);
        }

        // FK-edge pattern: edge table = node table with FK column
        // Use direct 2-way join: start_node.fk_col = end_node.id_col
        if self.is_fk_edge {
            return self.generate_fk_edge_base_case(hop_count);
        }

        // Standard case: both nodes have their own tables
        if hop_count == 1 {
            // Build edge tuple for the base case
            let edge_tuple = self.build_edge_tuple_base();

            // Build property selections
            let mut select_items = vec![
                format!(
                    "{}.{} as start_id",
                    self.start_node_alias, self.start_node_id_column
                ),
                format!(
                    "{}.{} as end_id",
                    self.end_node_alias, self.end_node_id_column
                ),
                "1 as hop_count".to_string(),
                format!("[{}] as path_edges", edge_tuple), // Track edge IDs, not node IDs
                self.generate_relationship_type_for_hop(1), // path_relationships for single hop
                // Add path_nodes array for UNWIND nodes(p) support
                format!(
                    "[{}.{}, {}.{}] as path_nodes",
                    self.start_node_alias,
                    self.start_node_id_column,
                    self.end_node_alias,
                    self.end_node_id_column
                ),
            ];

            // Add properties for start and end nodes
            // CRITICAL: Use separate if statements (not else-if) for self-loops
            // When start_cypher_alias == end_cypher_alias, both conditions are true
            for prop in &self.properties {
                // Skip the ID column when it's the actual "id" property (already added as start_id/end_id)
                // But keep it if it's a different property that uses the ID column (e.g., "node_id")
                let is_start_id = prop.column_name == self.start_node_id_column && prop.alias == "id";
                let is_end_id = prop.column_name == self.end_node_id_column && prop.alias == "id";
                
                if prop.cypher_alias == self.start_cypher_alias && !is_start_id {
                    // Property belongs to start node
                    select_items.push(format!(
                        "{}.{} as start_{}",
                        self.start_node_alias, prop.column_name, prop.alias
                    ));
                }
                if prop.cypher_alias == self.end_cypher_alias && !is_end_id {
                    // Property belongs to end node
                    select_items.push(format!(
                        "{}.{} as end_{}",
                        self.end_node_alias, prop.column_name, prop.alias
                    ));
                }
            }

            let select_clause = select_items.join(",\n        ");

            // Build the base query without WHERE clause
            let mut query = format!(
                "    SELECT \n        {select}\n    FROM {start_table} AS {start}\n    JOIN {rel_table} AS {rel} ON {start}.{start_id_col} = {rel}.{from_col}\n    JOIN {end_table} AS {end} ON {rel}.{to_col} = {end}.{end_id_col}",
                select = select_clause,
                start = self.start_node_alias,
                start_id_col = self.start_node_id_column,
                end = self.end_node_alias,
                end_id_col = self.end_node_id_column,
                rel = self.relationship_alias,
                start_table = self.format_table_name(&self.start_node_table),
                rel_table = self.format_table_name(&self.relationship_table),
                from_col = self.relationship_from_column,
                to_col = self.relationship_to_column,
                end_table = self.format_table_name(&self.end_node_table)
            );

            // Add WHERE clause with start and end node filters
            // For shortest path queries, only include start filters in base case
            // End filters are applied in the _to_target wrapper CTE
            let mut where_conditions = Vec::new();

            // Add polymorphic edge filter if this is a polymorphic edge table
            if let Some(poly_filter) = self.generate_polymorphic_edge_filter() {
                where_conditions.push(poly_filter);
            }

            // Add edge constraints if defined in schema (base case uses default aliases)
            if let Some(constraint_filter) = self.generate_edge_constraint_filter(None, None) {
                where_conditions.push(constraint_filter);
            }

            if let Some(ref filters) = self.start_node_filters {
                where_conditions.push(filters.clone());
            }
            // Only add end_node_filters in base case if NOT using shortest path mode
            if self.shortest_path_mode.is_none() {
                if let Some(ref filters) = self.end_node_filters {
                    where_conditions.push(filters.clone());
                }
            }

            // ✅ HOLISTIC FIX: Add relationship filters (e.g., WHERE r.weight > 0.5)
            // These filters apply to the relationship/edge table properties and must be applied
            // during traversal, not on the CTE output (which doesn't have these columns)
            if let Some(ref filters) = self.relationship_filters {
                log::debug!("Adding relationship filters to base case: {}", filters);
                where_conditions.push(filters.clone());
            }

            if !where_conditions.is_empty() {
                query.push_str(&format!("\n    WHERE {}", where_conditions.join(" AND ")));
            }

            query
        } else {
            // Multi-hop base case (for min_hops > 1)
            self.generate_multi_hop_base_case(hop_count)
        }
    }

    /// Generate multi-hop base case (more complex, chains multiple relationships)
    fn generate_multi_hop_base_case(&self, hop_count: u32) -> String {
        // This is a simplified version - in practice, we'd need to handle
        // different relationship types and intermediate node types
        format!(
            "    -- Multi-hop base case for {} hops (simplified)\n    SELECT NULL as start_id, NULL as end_id, {} as hop_count, [] as path_edges, [] as path_relationships\n    WHERE false  -- Placeholder",
            hop_count, hop_count
        )
    }
    /// Generate recursive case that extends existing paths
    /// Reserved for backward compatibility when default CTE name is used
    #[allow(dead_code)]
    fn generate_recursive_case(&self, max_hops: u32) -> String {
        // Delegate to the version that accepts CTE name
        // This maintains backward compatibility
        self.generate_recursive_case_with_cte_name(max_hops, &self.cte_name)
    }

    fn generate_recursive_case_with_cte_name(&self, max_hops: u32, cte_name: &str) -> String {
        // For fully denormalized edges, use simplified generation
        if self.is_denormalized {
            return self.generate_denormalized_recursive_case(max_hops, cte_name);
        }

        // Check for mixed patterns (one side denormalized)
        if self.start_is_denormalized || self.end_is_denormalized {
            return self.generate_mixed_recursive_case(max_hops, cte_name);
        }

        // FK-edge pattern: edge table = node table with FK column
        if self.is_fk_edge {
            return self.generate_fk_edge_recursive_case(max_hops, cte_name);
        }

        // Heterogeneous polymorphic path: recurse through intermediate type
        // e.g., Group→*→User should recurse through Group→Group, not User→User
        if self.is_heterogeneous_polymorphic_path() {
            return self.generate_heterogeneous_polymorphic_recursive_case(max_hops, cte_name);
        }

        // Standard case: both nodes have their own tables
        // Build edge tuple for recursive case
        let edge_tuple_recursive = self.build_edge_tuple_recursive(&self.relationship_alias);

        // Build property selections for recursive case
        let mut select_items = vec![
            "vp.start_id".to_string(),
            format!(
                "{}.{} as end_id",
                self.end_node_alias, self.end_node_id_column
            ),
            "vp.hop_count + 1 as hop_count".to_string(),
            format!(
                "arrayConcat(vp.path_edges, [{}]) as path_edges",
                edge_tuple_recursive
            ), // Append edge ID/tuple, not node ID
            format!(
                "arrayConcat(vp.path_relationships, {}) as path_relationships",
                self.get_relationship_type_array()
            ),
            // Add path_nodes array for UNWIND nodes(p) support
            format!(
                "arrayConcat(vp.path_nodes, [{}.{}]) as path_nodes",
                self.end_node_alias, self.end_node_id_column
            ),
        ];

        // Add properties: start properties come from CTE, end properties from new joined node
        // CRITICAL: Use separate if statements (not else-if) for self-loops
        // When start_cypher_alias == end_cypher_alias, both conditions are true
        for prop in &self.properties {
            // Skip the ID column when it's the actual "id" property (already added as start_id/end_id)
            let is_start_id = prop.column_name == self.start_node_id_column && prop.alias == "id";
            let is_end_id = prop.column_name == self.end_node_id_column && prop.alias == "id";
            
            if prop.cypher_alias == self.start_cypher_alias && !is_start_id {
                // Start node properties pass through from CTE
                select_items.push(format!("vp.start_{} as start_{}", prop.alias, prop.alias));
            }
            if prop.cypher_alias == self.end_cypher_alias && !is_end_id {
                // End node properties come from the newly joined node
                select_items.push(format!(
                    "{}.{} as end_{}",
                    self.end_node_alias, prop.column_name, prop.alias
                ));
            }
        }

        let select_clause = select_items.join(",\n        ");

        // Build edge tuple check for cycle prevention
        let edge_tuple_check = self.build_edge_tuple_recursive(&self.relationship_alias);

        let mut where_conditions = vec![
            format!("vp.hop_count < {}", max_hops),
            format!("NOT has(vp.path_edges, {})", edge_tuple_check), // Edge uniqueness check (Neo4j semantics)
        ];

        // Add polymorphic edge filter if this is a polymorphic edge table
        if let Some(poly_filter) = self.generate_polymorphic_edge_filter() {
            where_conditions.push(poly_filter);
        }

        // Add edge constraints if defined in schema
        // Uses vp.end_* columns for the "from" node (previous end node) and
        // end_node.* columns for the "to" node (newly joined node)
        if let Some(constraint_filter) = self.generate_edge_constraint_filter_recursive() {
            where_conditions.push(constraint_filter);
        }

        // Note: We no longer skip zero-hop rows in recursion.
        // The recursion can now start from zero-hop base case and expand from there.
        // Cycle detection (NOT has) prevents infinite loops.

        // For shortest path queries, do NOT add end_node_filters in recursive case
        // End filters are applied in the _to_target wrapper CTE after recursion completes
        // This allows the recursion to explore all paths until the target is found
        //
        // Note: Early termination via NOT EXISTS is not practical because:
        // 1. It creates circular reference (checking CTE being built)
        // 2. ClickHouse evaluates EXISTS after generating all rows in that iteration
        // Better approach: Use reasonable max_hops or explicit hop constraints in query
        if self.shortest_path_mode.is_none() {
            if let Some(ref filters) = self.end_node_filters {
                where_conditions.push(filters.clone());
            }
        }

        // ✅ HOLISTIC FIX: Add relationship filters in recursive case too
        // This ensures relationship property filters (e.g., r.weight > 0.5) are applied
        // at every hop of the traversal, not just the base case
        if let Some(ref filters) = self.relationship_filters {
            log::debug!("Adding relationship filters to recursive case: {}", filters);
            where_conditions.push(filters.clone());
        }

        let where_clause = where_conditions.join("\n      AND ");

        // PERF: Removed redundant current_node JOIN - vp.end_id already contains the ID
        // we need to join with the relationship table. The extra JOIN was causing
        // ClickHouse to hang on recursive CTEs due to inefficient query planning.
        format!(
            "    SELECT\n        {select}\n    FROM {cte_name} vp\n    JOIN {rel_table} AS {rel} ON vp.end_id = {rel}.{from_col}\n    JOIN {end_table} AS {end} ON {rel}.{to_col} = {end}.{end_id_col}\n    WHERE {where_clause}",
            select = select_clause,
            end = self.end_node_alias,
            end_id_col = self.end_node_id_column,
            cte_name = cte_name, // Use the passed parameter instead of self.cte_name
            rel_table = self.format_table_name(&self.relationship_table),
            from_col = self.relationship_from_column,
            to_col = self.relationship_to_column,
            rel = self.relationship_alias,
            end_table = self.format_table_name(&self.end_node_table),
            where_clause = where_clause
        )
    }

    // ======================================================================
    // HETEROGENEOUS POLYMORPHIC PATH GENERATION
    // ======================================================================
    // For paths like Group→*→User where intermediate hops traverse through
    // one type (Group→Group) and only the final hop goes to a different type (Group→User).
    // The recursive case uses the intermediate table (groups), not the end table (users).

    /// Generate recursive case for heterogeneous polymorphic paths
    /// Recurses through intermediate_node_table (e.g., groups) with intermediate_node_label filter
    fn generate_heterogeneous_polymorphic_recursive_case(
        &self,
        max_hops: u32,
        cte_name: &str,
    ) -> String {
        // Get intermediate table info (must be set for heterogeneous polymorphic paths)
        let intermediate_table = self
            .intermediate_node_table
            .as_ref()
            .expect("intermediate_node_table must be set for heterogeneous polymorphic paths");
        let intermediate_id_col = self
            .intermediate_node_id_column
            .as_ref()
            .expect("intermediate_node_id_column must be set for heterogeneous polymorphic paths");

        crate::debug_print!("    🔸 Generating heterogeneous polymorphic recursive case:");
        crate::debug_print!(
            "      - start_table: {}, end_table: {}, intermediate_table: {}",
            self.start_node_table,
            self.end_node_table,
            intermediate_table
        );

        // Build edge tuple for recursive case (using rel alias)
        let edge_tuple_recursive = self.build_edge_tuple_recursive(&self.relationship_alias);

        // Build property selections for recursive case
        // Note: For heterogeneous polymorphic paths, we track intermediate nodes in path
        // End properties are not available until the final join (in the outer SELECT)
        let mut select_items = vec![
            "vp.start_id".to_string(),
            // end_id comes from the intermediate node (Group), not the end table (User)
            format!("intermediate_node.{} as end_id", intermediate_id_col),
            "vp.hop_count + 1 as hop_count".to_string(),
            format!(
                "arrayConcat(vp.path_edges, [{}]) as path_edges",
                edge_tuple_recursive
            ),
            format!(
                "arrayConcat(vp.path_relationships, {}) as path_relationships",
                self.get_relationship_type_array()
            ),
            // Track intermediate node IDs in path_nodes
            format!(
                "arrayConcat(vp.path_nodes, [intermediate_node.{}]) as path_nodes",
                intermediate_id_col
            ),
        ];

        // Add properties: start properties pass through, end properties NOT available yet
        // (end properties will be populated in the final outer SELECT that joins to end_node_table)
        for prop in &self.properties {
            if prop.cypher_alias == self.start_cypher_alias {
                select_items.push(format!("vp.start_{} as start_{}", prop.alias, prop.alias));
            }
            // Note: We don't have end properties in the recursive traversal
            // They'll be added in the outer SELECT when we join to the actual end table
        }

        let select_clause = select_items.join(",\n        ");

        // Build edge tuple check for cycle prevention
        let edge_tuple_check = self.build_edge_tuple_recursive(&self.relationship_alias);

        let mut where_conditions = vec![
            format!("vp.hop_count < {}", max_hops),
            format!("NOT has(vp.path_edges, {})", edge_tuple_check),
        ];

        // Add polymorphic edge filter for INTERMEDIATE hops (e.g., member_type = 'Group')
        if let Some(poly_filter) = self.generate_polymorphic_edge_filter_intermediate() {
            where_conditions.push(poly_filter);
        }

        // Add edge constraints if defined in schema
        // Uses current_node as from_alias since recursive case references current row
        if let Some(constraint_filter) =
            self.generate_edge_constraint_filter(Some("current_node"), None)
        {
            where_conditions.push(constraint_filter);
        }

        // ✅ HOLISTIC FIX: Add relationship filters in heterogeneous polymorphic recursive case
        if let Some(ref filters) = self.relationship_filters {
            log::debug!(
                "Adding relationship filters to heterogeneous polymorphic recursive case: {}",
                filters
            );
            where_conditions.push(filters.clone());
        }

        let where_clause = where_conditions.join("\n      AND ");

        // Recursive case joins through INTERMEDIATE table, not end table
        // Pattern: vp → current_node (intermediate) → rel → intermediate_node (intermediate)
        format!(
            "    SELECT\n        {select}\n    FROM {cte_name} vp\n    JOIN {intermediate_table} current_node ON vp.end_id = current_node.{intermediate_id_col}\n    JOIN {rel_table} {rel} ON current_node.{intermediate_id_col} = {rel}.{from_col}\n    JOIN {intermediate_table} intermediate_node ON {rel}.{to_col} = intermediate_node.{intermediate_id_col}\n    WHERE {where_clause}",
            select = select_clause,
            intermediate_id_col = intermediate_id_col,
            cte_name = cte_name,
            intermediate_table = self.format_table_name(intermediate_table),
            rel_table = self.format_table_name(&self.relationship_table),
            from_col = self.relationship_from_column,
            to_col = self.relationship_to_column,
            rel = self.relationship_alias,
            where_clause = where_clause
        )
    }

    // ======================================================================
    // FK-EDGE PATTERN GENERATION
    // ======================================================================
    // For FK-edge patterns, the edge is a foreign key column on the node table.
    // Both nodes come from the same table, and the relationship is:
    // start_node.fk_col = end_node.id_col (e.g., child.parent_id = parent.object_id)
    // No separate relationship table exists.

    /// Generate base case for FK-edge patterns (first hop)
    /// For FK-edge: FROM node_table start JOIN node_table end ON start.fk = end.id
    fn generate_fk_edge_base_case(&self, hop_count: u32) -> String {
        if hop_count != 1 {
            // Multi-hop base case not yet supported for FK-edge
            return format!(
                "    -- Multi-hop base case for {} hops (FK-edge - not yet supported)\n    SELECT NULL as start_id, NULL as end_id, {} as hop_count, [] as path_edges, [] as path_relationships, [] as path_nodes\n    WHERE false",
                hop_count, hop_count
            );
        }

        // Build edge tuple for cycle detection
        // For FK-edge, the edge is (start_node.fk_col, end_node.id_col)
        let edge_tuple = format!(
            "tuple({}.{}, {}.{})",
            self.start_node_alias,
            self.relationship_from_column,
            self.end_node_alias,
            self.end_node_id_column
        );

        // Build property selections
        let mut select_items = vec![
            format!(
                "{}.{} as start_id",
                self.start_node_alias, self.start_node_id_column
            ),
            format!(
                "{}.{} as end_id",
                self.end_node_alias, self.end_node_id_column
            ),
            "1 as hop_count".to_string(),
            format!("[{}] as path_edges", edge_tuple),
            self.generate_relationship_type_for_hop(1),
            format!(
                "[{}.{}, {}.{}] as path_nodes",
                self.start_node_alias,
                self.start_node_id_column,
                self.end_node_alias,
                self.end_node_id_column
            ),
        ];

        // Add properties for start and end nodes
        for prop in &self.properties {
            if prop.cypher_alias == self.start_cypher_alias {
                select_items.push(format!(
                    "{}.{} as start_{}",
                    self.start_node_alias, prop.column_name, prop.alias
                ));
            }
            if prop.cypher_alias == self.end_cypher_alias {
                select_items.push(format!(
                    "{}.{} as end_{}",
                    self.end_node_alias, prop.column_name, prop.alias
                ));
            }
        }

        let select_clause = select_items.join(",\n        ");

        // FK-edge pattern: direct 2-way join between start and end nodes
        // start_node.fk_col = end_node.id_col (e.g., child.parent_id = parent.object_id)
        let mut query = format!(
            "    SELECT \n        {select}\n    FROM {start_table} {start}\n    JOIN {end_table} {end} ON {start}.{fk_col} = {end}.{end_id_col}",
            select = select_clause,
            start = self.start_node_alias,
            start_table = self.format_table_name(&self.start_node_table),
            end = self.end_node_alias,
            fk_col = self.relationship_from_column,  // FK column on start node
            end_id_col = self.end_node_id_column,     // ID column on end node
            end_table = self.format_table_name(&self.end_node_table)
        );

        // Add WHERE clause with start and end node filters
        let mut where_conditions = Vec::new();

        // Add edge constraints if defined in schema
        // Uses default aliases (start_node, end_node) for base case
        if let Some(constraint_filter) = self.generate_edge_constraint_filter(None, None) {
            where_conditions.push(constraint_filter);
        }

        if let Some(ref filters) = self.start_node_filters {
            where_conditions.push(filters.clone());
        }
        if self.shortest_path_mode.is_none() {
            if let Some(ref filters) = self.end_node_filters {
                where_conditions.push(filters.clone());
            }
        }

        // ✅ HOLISTIC FIX: Add relationship filters in FK-edge base case
        // Note: In FK-edge patterns, relationship properties are typically embedded in the
        // start node table (the table with the FK column), so this filter will reference
        // the start_node_alias (or a separate rel alias if one is defined)
        if let Some(ref filters) = self.relationship_filters {
            log::debug!(
                "Adding relationship filters to FK-edge base case: {}",
                filters
            );
            where_conditions.push(filters.clone());
        }

        if !where_conditions.is_empty() {
            query.push_str(&format!("\n    WHERE {}", where_conditions.join(" AND ")));
        }

        query
    }

    /// Generate recursive case for FK-edge patterns
    ///
    /// The expansion direction depends on which side is filtered:
    ///
    /// **ANCESTORS query** (filter on start/child node, e.g., WHERE child.name = 'notes.txt'):
    /// - We want to find all ancestors (parents) of notes.txt
    /// - Base: notes.txt→Work (notes.txt.parent_id = Work.object_id)
    /// - Recurse: Work→Documents (Work.parent_id = Documents.object_id)
    /// - Strategy: APPEND expansion - add new edges at the END of the path
    /// - Anchor on end_id (the parent side), find their parents
    ///
    /// **DESCENDANTS query** (filter on end/parent node, e.g., WHERE parent.name = 'root'):
    /// - We want to find all descendants (children) of root
    /// - Base: Documents→root (Documents.parent_id = root.object_id)
    /// - Recurse: Work→Documents (Work.parent_id = Documents.object_id)
    /// - Strategy: PREPEND expansion - add new edges at the START of the path
    /// - Anchor on start_id (the child side), find their children
    fn generate_fk_edge_recursive_case(&self, max_hops: u32, cte_name: &str) -> String {
        // Determine expansion direction based on which side has filters
        // If start_node_filters is set, we're finding ancestors (APPEND expansion)
        // If end_node_filters is set, we're finding descendants (PREPEND expansion)
        let expand_toward_parents = self.start_node_filters.is_some();

        if expand_toward_parents {
            self.generate_fk_edge_recursive_append(max_hops, cte_name)
        } else {
            self.generate_fk_edge_recursive_prepend(max_hops, cte_name)
        }
    }

    /// APPEND expansion: Find ancestors by following parent_id chain
    /// Used when start_node has a filter (e.g., WHERE child.name = 'notes.txt')
    fn generate_fk_edge_recursive_append(&self, max_hops: u32, cte_name: &str) -> String {
        // Edge tuple for the NEW edge being added (current → new_end)
        // Use node IDs rather than FK column to avoid referencing columns that may not exist
        // on the target node type (e.g., Folder doesn't have parent_folder_id)
        let edge_tuple_recursive = format!(
            "tuple(vp.end_id, {}.{})",
            "new_end", self.end_node_id_column
        );

        // Build property selections
        // start_id stays the same (notes.txt), end_id becomes new_end
        let mut select_items = vec![
            "vp.start_id".to_string(), // start stays the same
            format!("{}.{} as end_id", "new_end", self.end_node_id_column), // new parent
            "vp.hop_count + 1 as hop_count".to_string(),
            // APPEND the new edge to path_edges
            format!(
                "arrayConcat(vp.path_edges, [{}]) as path_edges",
                edge_tuple_recursive
            ),
            format!(
                "arrayConcat(vp.path_relationships, {}) as path_relationships",
                self.get_relationship_type_array()
            ),
            // APPEND the new node to path_nodes
            format!(
                "arrayConcat(vp.path_nodes, [{}.{}]) as path_nodes",
                "new_end", self.end_node_id_column
            ),
        ];

        // Add properties: start properties from CTE, end properties from new joined node
        for prop in &self.properties {
            if prop.cypher_alias == self.start_cypher_alias {
                select_items.push(format!("vp.start_{} as start_{}", prop.alias, prop.alias));
            }
            if prop.cypher_alias == self.end_cypher_alias {
                select_items.push(format!(
                    "{}.{} as end_{}",
                    "new_end", prop.column_name, prop.alias
                ));
            }
        }

        let select_clause = select_items.join(",\n        ");

        let edge_tuple_check = format!(
            "tuple(current_node.{}, new_end.{})",
            self.end_node_id_column, self.end_node_id_column
        );

        let mut where_conditions = vec![
            format!("vp.hop_count < {}", max_hops),
            format!("NOT has(vp.path_edges, {})", edge_tuple_check),
        ];

        // Add edge constraints if defined in schema
        // FK-edge APPEND: from=current_node (previous end), to=new_end (parent)
        if let Some(constraint_filter) =
            self.generate_edge_constraint_filter(Some("current_node"), Some("new_end"))
        {
            where_conditions.push(constraint_filter);
        }

        // ✅ HOLISTIC FIX: Add relationship filters in FK-edge recursive (append) case
        // In APPEND expansion, relationship properties are on current_node (the edge/FK table)
        // Rewrite filter alias from 'start_node' to 'current_node'
        if let Some(ref filters) = self.relationship_filters {
            let rewritten_filter = filters.replace("start_node.", "current_node.");
            log::debug!(
                "Adding relationship filters to FK-edge recursive (append) case: {} -> {}",
                filters,
                rewritten_filter
            );
            where_conditions.push(rewritten_filter);
        }

        let where_clause = where_conditions.join("\n      AND ");

        // APPEND expansion: anchor on end_id, find its parent
        // current_node = previous end (e.g., Work)
        // new_end = current_node's parent (e.g., Documents)
        format!(
            "    SELECT\n        {select}\n    FROM {cte_name} vp\n    JOIN {current_table} current_node ON vp.end_id = current_node.{current_id_col}\n    JOIN {end_table} new_end ON current_node.{fk_col} = new_end.{end_id_col}\n    WHERE {where_clause}",
            select = select_clause,
            cte_name = cte_name,
            current_table = self.format_table_name(&self.end_node_table),
            current_id_col = self.end_node_id_column,
            end_table = self.format_table_name(&self.end_node_table),
            fk_col = self.relationship_from_column,
            end_id_col = self.end_node_id_column,
            where_clause = where_clause
        )
    }

    /// PREPEND expansion: Find descendants by finding nodes whose parent_id points to current
    /// Used when end_node has a filter (e.g., WHERE parent.name = 'root')
    fn generate_fk_edge_recursive_prepend(&self, max_hops: u32, cte_name: &str) -> String {
        // Edge tuple for the NEW edge being added (new_start → current)
        // new_start.parent_id = current.object_id
        let edge_tuple_recursive = format!(
            "tuple({}.{}, {}.{})",
            "new_start", self.relationship_from_column, "current_node", self.end_node_id_column
        );

        // Build property selections
        // The NEW start_id is new_start, end_id stays the same (root)
        let mut select_items = vec![
            format!("{}.{} as start_id", "new_start", self.start_node_id_column),
            "vp.end_id".to_string(), // end_id stays the same (root)
            "vp.hop_count + 1 as hop_count".to_string(),
            // PREPEND the new edge to path_edges
            format!(
                "arrayConcat([{}], vp.path_edges) as path_edges",
                edge_tuple_recursive
            ),
            format!(
                "arrayConcat({}, vp.path_relationships) as path_relationships",
                self.get_relationship_type_array()
            ),
            // PREPEND the new node to path_nodes
            format!(
                "arrayConcat([{}.{}], vp.path_nodes) as path_nodes",
                "new_start", self.start_node_id_column
            ),
        ];

        // Add properties: end properties from CTE, start properties from new joined node
        for prop in &self.properties {
            if prop.cypher_alias == self.start_cypher_alias {
                select_items.push(format!(
                    "{}.{} as start_{}",
                    "new_start", prop.column_name, prop.alias
                ));
            }
            if prop.cypher_alias == self.end_cypher_alias {
                select_items.push(format!("vp.end_{} as end_{}", prop.alias, prop.alias));
            }
        }

        let select_clause = select_items.join(",\n        ");

        let edge_tuple_check = format!(
            "tuple(new_start.{}, current_node.{})",
            self.relationship_from_column, self.end_node_id_column
        );

        let mut where_conditions = vec![
            format!("vp.hop_count < {}", max_hops),
            format!("NOT has(vp.path_edges, {})", edge_tuple_check),
        ];

        // Add edge constraints if defined in schema
        // FK-edge PREPEND: from=new_start (child), to=current_node (previous start)
        if let Some(constraint_filter) =
            self.generate_edge_constraint_filter(Some("new_start"), Some("current_node"))
        {
            where_conditions.push(constraint_filter);
        }

        // ✅ HOLISTIC FIX: Add relationship filters in FK-edge recursive (prepend) case
        // In PREPEND expansion, relationship properties are on new_start (the edge/FK table)
        // Rewrite filter alias from 'start_node' to 'new_start'
        if let Some(ref filters) = self.relationship_filters {
            let rewritten_filter = filters.replace("start_node.", "new_start.");
            log::debug!(
                "Adding relationship filters to FK-edge recursive (prepend) case: {} -> {}",
                filters,
                rewritten_filter
            );
            where_conditions.push(rewritten_filter);
        }

        let where_clause = where_conditions.join("\n      AND ");

        // PREPEND expansion: anchor on start_id, find nodes whose parent_id points to it
        // current_node = previous start (e.g., Documents)
        // new_start = a child of current (e.g., Work where Work.parent_id = Documents.object_id)
        format!(
            "    SELECT\n        {select}\n    FROM {cte_name} vp\n    JOIN {current_table} current_node ON vp.start_id = current_node.{current_id_col}\n    JOIN {start_table} new_start ON new_start.{fk_col} = current_node.{current_id_col}\n    WHERE {where_clause}",
            select = select_clause,
            cte_name = cte_name,
            current_table = self.format_table_name(&self.start_node_table),
            current_id_col = self.start_node_id_column,
            start_table = self.format_table_name(&self.start_node_table),
            fk_col = self.relationship_from_column,
            where_clause = where_clause
        )
    }

    // ======================================================================
    // DENORMALIZED EDGE GENERATION
    // ======================================================================
    // For denormalized edges, node properties are embedded in the edge table.
    // No separate node tables exist - all data comes from the relationship table.

    /// Generate base case for denormalized edges (first hop)
    /// For denormalized: FROM rel_table only (no node tables)
    fn generate_denormalized_base_case(&self, hop_count: u32) -> String {
        log::debug!(
            "generate_denormalized_base_case: start_alias='{}', end_alias='{}', rel_table='{}'",
            self.start_cypher_alias,
            self.end_cypher_alias,
            self.relationship_table
        );

        if hop_count != 1 {
            // Multi-hop base case not yet supported for denormalized
            return format!(
                "    -- Multi-hop base case for {} hops (denormalized - not yet supported)\n    SELECT NULL as start_id, NULL as end_id, {} as hop_count, [] as path_edges, [] as path_relationships, [] as path_nodes\n    WHERE false",
                hop_count, hop_count
            );
        }

        // Build edge tuple for cycle detection
        let edge_tuple = self.build_edge_tuple_base();

        // Build SELECT clause with denormalized properties
        let mut select_items = vec![
            format!(
                "{}.{} as start_id",
                self.relationship_alias, self.relationship_from_column
            ),
            format!(
                "{}.{} as end_id",
                self.relationship_alias, self.relationship_to_column
            ),
            "1 as hop_count".to_string(),
            format!("[{}] as path_edges", edge_tuple),
            self.generate_relationship_type_for_hop(1),
            format!(
                "[{}.{}, {}.{}] as path_nodes",
                self.relationship_alias,
                self.relationship_from_column,
                self.relationship_alias,
                self.relationship_to_column
            ),
        ];

        // Generate JSON property blobs for start and end nodes (denormalized)
        // Instead of flat columns, generate formatRowNoNewline JSON to match
        // the multi-type VLP tuple format expected by transform_vlp_path()
        {
            use crate::clickhouse_query_generator::json_builder::generate_json_from_denormalized_properties;

            // Find the denormalized node schema using relationship type and from/to labels
            // for deterministic lookup, falling back to table name matching.
            let node_schema = {
                let mut found = None;

                // Prefer lookup via relationship type → from_node label → node schema
                if let Some(ref rel_types) = self.relationship_types {
                    if let Some(rel_type) = rel_types.first() {
                        let rel_schemas = self.schema.get_relationships_schemas();
                        if let Some(rel_schema) = rel_schemas.get(rel_type) {
                            let from_label = &rel_schema.from_node;
                            found = self
                                .schema
                                .all_node_schemas()
                                .iter()
                                .find(|(key, _)| {
                                    *key == from_label
                                        || key.ends_with(&format!("::{}", from_label))
                                })
                                .map(|(_, v)| v);
                        }
                    }
                }

                // Fallback: match by table name (legacy behavior)
                if found.is_none() {
                    let rel_table_name = self
                        .relationship_table
                        .rsplit('.')
                        .next()
                        .unwrap_or(&self.relationship_table);
                    found = self.schema.all_node_schemas().values().find(|n| {
                        let t = n.table_name.rsplit('.').next().unwrap_or(&n.table_name);
                        t == rel_table_name
                    });
                }

                found
            };

            if let Some(ns) = node_schema {
                // Start node properties (from_properties for normal direction)
                if let Some(ref from_props) = ns.from_properties {
                    let json_sql = generate_json_from_denormalized_properties(
                        from_props,
                        &self.relationship_alias,
                        "_s_",
                    );
                    select_items.push(format!("{} AS start_properties", json_sql));
                } else {
                    select_items.push("'{}' AS start_properties".to_string());
                }

                // End node properties (to_properties for normal direction)
                if let Some(ref to_props) = ns.to_properties {
                    let json_sql = generate_json_from_denormalized_properties(
                        to_props,
                        &self.relationship_alias,
                        "_e_",
                    );
                    select_items.push(format!("{} AS end_properties", json_sql));
                } else {
                    select_items.push("'{}' AS end_properties".to_string());
                }
            } else {
                select_items.push("'{}' AS start_properties".to_string());
                select_items.push("'{}' AS end_properties".to_string());
            }
        }

        // Add relationship properties JSON
        {
            let rel_schemas = self.schema.get_relationships_schemas();
            // Prefer lookup by relationship type name for deterministic selection
            // when multiple relationship types share the same table.
            let rel_schema = self
                .relationship_types
                .as_ref()
                .and_then(|types| types.first())
                .and_then(|rel_type| rel_schemas.get(rel_type))
                .or_else(|| {
                    // Fallback: match by table name (legacy behavior)
                    let rel_table_name = self
                        .relationship_table
                        .rsplit('.')
                        .next()
                        .unwrap_or(&self.relationship_table);
                    rel_schemas.values().find(|r| {
                        let t = r.table_name.rsplit('.').next().unwrap_or(&r.table_name);
                        t == rel_table_name
                    })
                });
            let rel_props_json = rel_schema
                .map(|r| {
                    if r.property_mappings.is_empty() {
                        "'{}'".to_string()
                    } else {
                        use crate::clickhouse_query_generator::json_builder::generate_json_properties_sql;
                        generate_json_properties_sql(
                            &r.property_mappings,
                            &self.relationship_alias,
                        )
                    }
                })
                .unwrap_or_else(|| "'{}'".to_string());
            select_items.push(format!("[{}] AS rel_properties", rel_props_json));
        }

        // Add start_type and end_type discriminators for transform_vlp_path()
        if let Some(ref start_label) = self.from_node_label {
            select_items.push(format!("'{}' AS start_type", start_label));
        } else {
            select_items.push("'Unknown' AS start_type".to_string());
        }
        if let Some(ref end_label) = self.to_node_label {
            select_items.push(format!("'{}' AS end_type", end_label));
        } else {
            select_items.push("'Unknown' AS end_type".to_string());
        }

        let select_clause = select_items.join(",\n        ");

        // Simple FROM - just the relationship table, no node tables
        let mut query = format!(
            "    SELECT \n        {select}\n    FROM {rel_table} AS {rel}",
            select = select_clause,
            rel_table = self.format_table_name(&self.relationship_table),
            rel = self.relationship_alias
        );

        // Add WHERE clause for start node filters (rewritten for rel table)
        let mut where_conditions = Vec::new();

        // Add edge constraints if defined in schema (FK-edge base case uses default aliases)
        if let Some(constraint_filter) = self.generate_edge_constraint_filter(None, None) {
            where_conditions.push(constraint_filter);
        }

        if let Some(ref filters) = self.start_node_filters {
            // Rewrite start_node references to rel references
            let rewritten =
                filters.replace("start_node.", &format!("{}.", self.relationship_alias));
            where_conditions.push(rewritten);
        }

        // ⚠️ CRITICAL FIX (Jan 10, 2026): Don't add end_node_filters to denormalized VLP base case
        //
        // Problem: For multi-hop VLP (e.g., LAX→ORD→ATL), adding end_node filters to base case
        // prevents intermediate paths from being generated.
        //
        // Example:
        //   Query: MATCH (a:Airport)-[:FLIGHT*1..2]->(b:Airport) WHERE a.code='LAX' AND b.code='ATL'
        //   Base case SQL: SELECT ... FROM flights WHERE Origin='LAX' AND Dest='ATL'
        //   Result: 0 rows (no direct LAX→ATL flight exists!)
        //   Issue: LAX→ORD edge excluded because Dest='ORD' != 'ATL', recursion never starts
        //
        // Solution: Apply end_node_filters in OUTER query after VLP recursion completes:
        //   Base case: SELECT ... FROM flights WHERE Origin='LAX'  (generates LAX→SFO, LAX→ORD)
        //   Recursive: Extends to LAX→ORD→ATL
        //   Outer query: SELECT * FROM vlp WHERE end_id='ATL'  (filters final result)
        //
        // This matches shortest_path_mode behavior where only start filters are in base case.
        //
        // if self.shortest_path_mode.is_none() {
        //     if let Some(ref filters) = self.end_node_filters {
        //         let rewritten =
        //             filters.replace("end_node.", &format!("{}.", self.relationship_alias));
        //         where_conditions.push(rewritten);
        //     }
        // }

        // ✅ HOLISTIC FIX: Add relationship filters in denormalized base case
        // In denormalized patterns, relationship properties are on the same edge table
        if let Some(ref filters) = self.relationship_filters {
            log::debug!(
                "Adding relationship filters to denormalized base case: {}",
                filters
            );
            where_conditions.push(filters.clone());
        }

        if !where_conditions.is_empty() {
            query.push_str(&format!("\n    WHERE {}", where_conditions.join(" AND ")));
        }

        query
    }

    /// Generate recursive case for denormalized edges
    /// For denormalized: JOIN rel_table only (no node tables in between)
    fn generate_denormalized_recursive_case(&self, max_hops: u32, cte_name: &str) -> String {
        // Build edge tuple for cycle detection
        let edge_tuple_recursive = self.build_edge_tuple_recursive(&self.relationship_alias);

        // Build SELECT clause with denormalized properties
        let mut select_items = vec![
            "vp.start_id".to_string(),
            format!(
                "{}.{} as end_id",
                self.relationship_alias, self.relationship_to_column
            ),
            "vp.hop_count + 1 as hop_count".to_string(),
            format!(
                "arrayConcat(vp.path_edges, [{}]) as path_edges",
                edge_tuple_recursive
            ),
            format!(
                "arrayConcat(vp.path_relationships, {}) as path_relationships",
                self.get_relationship_type_array()
            ),
            format!(
                "arrayConcat(vp.path_nodes, [{}.{}]) as path_nodes",
                self.relationship_alias, self.relationship_to_column
            ),
        ];

        // Add denormalized properties as JSON blobs matching base case columns.
        // Carry forward start_properties from CTE, generate new end_properties from joined edge.
        {
            use crate::clickhouse_query_generator::json_builder::generate_json_from_denormalized_properties;

            // start_properties: carry forward from CTE (unchanged through recursion)
            select_items.push("vp.start_properties as start_properties".to_string());

            // end_properties: generate from new edge's to_node columns
            let node_schema = {
                let mut found = None;
                if let Some(ref rel_types) = self.relationship_types {
                    if let Some(rel_type) = rel_types.first() {
                        let rel_schemas = self.schema.get_relationships_schemas();
                        if let Some(rel_schema) = rel_schemas.get(rel_type) {
                            let from_label = &rel_schema.from_node;
                            found = self
                                .schema
                                .all_node_schemas()
                                .iter()
                                .find(|(key, _)| {
                                    *key == from_label
                                        || key.ends_with(&format!("::{}", from_label))
                                })
                                .map(|(_, v)| v);
                        }
                    }
                }
                found
            };

            if let Some(ns) = node_schema {
                if let Some(ref to_props) = ns.to_properties {
                    let json_sql = generate_json_from_denormalized_properties(
                        to_props,
                        &self.relationship_alias,
                        "_e_",
                    );
                    select_items.push(format!("{} AS end_properties", json_sql));
                } else {
                    select_items.push("'{}' AS end_properties".to_string());
                }
            } else {
                select_items.push("'{}' AS end_properties".to_string());
            }

            // rel_properties: generate from new edge's relationship columns
            let rel_schemas = self.schema.get_relationships_schemas();
            let rel_schema = self
                .relationship_types
                .as_ref()
                .and_then(|types| types.first())
                .and_then(|rel_type| rel_schemas.get(rel_type));
            let rel_props_json = rel_schema
                .map(|r| {
                    if r.property_mappings.is_empty() {
                        "'{}'".to_string()
                    } else {
                        use crate::clickhouse_query_generator::json_builder::generate_json_properties_sql;
                        generate_json_properties_sql(
                            &r.property_mappings,
                            &self.relationship_alias,
                        )
                    }
                })
                .unwrap_or_else(|| "'{}'".to_string());
            select_items.push(format!(
                "arrayConcat(vp.rel_properties, [{}]) as rel_properties",
                rel_props_json
            ));

            // start_type / end_type: carry forward from CTE
            select_items.push("vp.start_type as start_type".to_string());
            select_items.push("vp.end_type as end_type".to_string());
        }

        // Also carry forward flat start_/end_ columns for backward compatibility
        // with property selection in outer queries
        for prop in &self.properties {
            if prop.cypher_alias == self.start_cypher_alias {
                if let Ok(physical_col) = self.map_denormalized_property(&prop.alias, true) {
                    select_items.push(format!(
                        "vp.start_{} as start_{}",
                        physical_col, physical_col
                    ));
                }
            }
            if prop.cypher_alias == self.end_cypher_alias {
                if let Ok(physical_col) = self.map_denormalized_property(&prop.alias, false) {
                    select_items.push(format!(
                        "{}.{} as end_{}",
                        self.relationship_alias, physical_col, physical_col
                    ));
                } else {
                    log::warn!(
                        "Could not map end property {} in recursive case",
                        prop.alias
                    );
                }
            }
        }

        let select_clause = select_items.join(",\n        ");

        let mut where_conditions = vec![
            format!("vp.hop_count < {}", max_hops),
            format!("NOT has(vp.path_edges, {})", edge_tuple_recursive),
        ];

        // Add edge constraints if defined in schema
        // Denormalized recursive: no separate node tables, constraints not applicable
        if let Some(constraint_filter) = self.generate_edge_constraint_filter(None, None) {
            where_conditions.push(constraint_filter);
        }

        // ⚠️ CRITICAL FIX (Jan 10, 2026): Don't add end_node_filters to recursive case either!
        //
        // Removing end_node_filters from base case alone isn't enough. The recursive case
        // also filters new edges, preventing intermediate path extensions.
        //
        // Example: LAX→ORD (hop 1) trying to extend to ATL
        //   Recursive JOIN: ... JOIN flights AS rel ON vp.end_id = rel.Origin WHERE rel.Dest='ATL'
        //   This correctly finds ORD→ATL, giving us LAX→ORD→ATL ✓
        //
        // But if we filter in recursive, we miss other extensions:
        //   LAX→SFO trying to extend: JOIN flights WHERE rel.Dest='ATL'
        //   Finds SFO→? edges, but only if they end at ATL - limits exploration
        //
        // Solution: Let recursion explore ALL paths, filter end nodes in OUTER query.
        //   Recursive: Extends all paths freely (generates full graph traversal)
        //   Outer: SELECT * FROM vlp WHERE end_id='ATL' (filters final destinations)
        //
        // if self.shortest_path_mode.is_none() {
        //     if let Some(ref filters) = self.end_node_filters {
        //         let rewritten =
        //             filters.replace("end_node.", &format!("{}.", self.relationship_alias));
        //         where_conditions.push(rewritten);
        //     }
        // }

        // ✅ HOLISTIC FIX: Add relationship filters in denormalized recursive case
        if let Some(ref filters) = self.relationship_filters {
            log::debug!(
                "Adding relationship filters to denormalized recursive case: {}",
                filters
            );
            where_conditions.push(filters.clone());
        }

        let where_clause = where_conditions.join("\n      AND ");

        // For denormalized: join directly from CTE end_id to new rel's from_col
        // No intermediate node table needed
        format!(
            "    SELECT\n        {select}\n    FROM {cte_name} vp\n    JOIN {rel_table} AS {rel} ON vp.end_id = {rel}.{from_col}\n    WHERE {where_clause}",
            select = select_clause,
            cte_name = cte_name,
            rel_table = self.format_table_name(&self.relationship_table),
            rel = self.relationship_alias,
            from_col = self.relationship_from_column,
            where_clause = where_clause
        )
    }

    // ======================================================================
    // MIXED PATTERN GENERATION
    // ======================================================================
    // For mixed patterns where one node is denormalized and the other is standard.
    // - Denorm → Standard: Start from rel table (no start table), end with standard table JOIN
    // - Standard → Denorm: Start from standard table, but end is denormalized (no end table JOIN)

    /// Generate base case for mixed patterns
    fn generate_mixed_base_case(&self, hop_count: u32) -> String {
        if hop_count != 1 {
            // Multi-hop base case not yet supported for mixed
            return format!(
                "    -- Multi-hop base case for {} hops (mixed - not yet supported)\n    SELECT NULL as start_id, NULL as end_id, {} as hop_count, [] as path_edges, [] as path_relationships, [] as path_nodes\n    WHERE false",
                hop_count, hop_count
            );
        }

        let edge_tuple = self.build_edge_tuple_base();

        // Determine start_id and end_id based on which side is denormalized
        let start_id_expr = if self.start_is_denormalized {
            // Start is denorm: ID comes from relationship table from_col
            format!(
                "{}.{}",
                self.relationship_alias, self.relationship_from_column
            )
        } else {
            // Start is standard: ID comes from start node table
            format!("{}.{}", self.start_node_alias, self.start_node_id_column)
        };

        let end_id_expr = if self.end_is_denormalized {
            // End is denorm: ID comes from relationship table to_col
            format!(
                "{}.{}",
                self.relationship_alias, self.relationship_to_column
            )
        } else {
            // End is standard: ID comes from end node table
            format!("{}.{}", self.end_node_alias, self.end_node_id_column)
        };

        let mut select_items = vec![
            format!("{} as start_id", start_id_expr),
            format!("{} as end_id", end_id_expr),
            "1 as hop_count".to_string(),
            format!("[{}] as path_edges", edge_tuple),
            self.generate_relationship_type_for_hop(1),
            // Add path_nodes for UNWIND nodes(p) support
            format!("[{}, {}] as path_nodes", start_id_expr, end_id_expr),
        ];

        // Add properties for non-denormalized nodes
        for prop in &self.properties {
            if prop.cypher_alias == self.start_cypher_alias && !self.start_is_denormalized {
                select_items.push(format!(
                    "{}.{} as start_{}",
                    self.start_node_alias, prop.column_name, prop.alias
                ));
            }
            if prop.cypher_alias == self.end_cypher_alias && !self.end_is_denormalized {
                select_items.push(format!(
                    "{}.{} as end_{}",
                    self.end_node_alias, prop.column_name, prop.alias
                ));
            }
        }

        let select_clause = select_items.join(",\n        ");

        // Build FROM clause based on which nodes are denormalized
        let from_clause = if self.start_is_denormalized && !self.end_is_denormalized {
            // Denorm → Standard: FROM rel_table JOIN end_table
            format!(
                "FROM {rel_table} {rel}\n    JOIN {end_table} {end} ON {rel}.{to_col} = {end}.{end_id_col}",
                rel_table = self.format_table_name(&self.relationship_table),
                rel = self.relationship_alias,
                end_table = self.format_table_name(&self.end_node_table),
                end = self.end_node_alias,
                to_col = self.relationship_to_column,
                end_id_col = self.end_node_id_column
            )
        } else if !self.start_is_denormalized && self.end_is_denormalized {
            // Standard → Denorm: FROM start_table JOIN rel_table
            format!(
                "FROM {start_table} {start}\n    JOIN {rel_table} {rel} ON {start}.{start_id_col} = {rel}.{from_col}",
                start_table = self.format_table_name(&self.start_node_table),
                start = self.start_node_alias,
                rel_table = self.format_table_name(&self.relationship_table),
                rel = self.relationship_alias,
                start_id_col = self.start_node_id_column,
                from_col = self.relationship_from_column
            )
        } else {
            // Shouldn't reach here - handled by is_denormalized check
            format!(
                "FROM {rel_table} {rel}",
                rel_table = self.format_table_name(&self.relationship_table),
                rel = self.relationship_alias
            )
        };

        let mut query = format!(
            "    SELECT \n        {select}\n    {from_clause}",
            select = select_clause,
            from_clause = from_clause
        );

        // Add WHERE conditions
        let mut where_conditions = Vec::new();
        if let Some(ref filters) = self.start_node_filters {
            // Rewrite for denorm start if needed
            let rewritten = if self.start_is_denormalized {
                filters.replace("start_node.", &format!("{}.", self.relationship_alias))
            } else {
                filters.clone()
            };
            where_conditions.push(rewritten);
        }

        if self.shortest_path_mode.is_none() {
            if let Some(ref filters) = self.end_node_filters {
                // Rewrite for denorm end if needed
                let rewritten = if self.end_is_denormalized {
                    filters.replace("end_node.", &format!("{}.", self.relationship_alias))
                } else {
                    filters.clone()
                };
                where_conditions.push(rewritten);
            }
        }

        // ✅ HOLISTIC FIX: Add relationship filters in mixed base case
        if let Some(ref filters) = self.relationship_filters {
            log::debug!(
                "Adding relationship filters to mixed base case: {}",
                filters
            );
            where_conditions.push(filters.clone());
        }

        if !where_conditions.is_empty() {
            query.push_str(&format!("\n    WHERE {}", where_conditions.join(" AND ")));
        }

        query
    }

    /// Generate recursive case for mixed patterns
    fn generate_mixed_recursive_case(&self, max_hops: u32, cte_name: &str) -> String {
        let edge_tuple_recursive = self.build_edge_tuple_recursive(&self.relationship_alias);

        // End ID expression based on denormalization
        let end_id_expr = if self.end_is_denormalized {
            format!(
                "{}.{}",
                self.relationship_alias, self.relationship_to_column
            )
        } else {
            format!("{}.{}", self.end_node_alias, self.end_node_id_column)
        };

        let mut select_items = vec![
            "vp.start_id".to_string(),
            format!("{} as end_id", end_id_expr),
            "vp.hop_count + 1 as hop_count".to_string(),
            format!(
                "arrayConcat(vp.path_edges, [{}]) as path_edges",
                edge_tuple_recursive
            ),
            format!(
                "arrayConcat(vp.path_relationships, {}) as path_relationships",
                self.get_relationship_type_array()
            ),
            // Add path_nodes for UNWIND nodes(p) support
            format!(
                "arrayConcat(vp.path_nodes, [{}]) as path_nodes",
                end_id_expr
            ),
        ];

        // Add properties - start from CTE, end from joined node (if not denorm)
        for prop in &self.properties {
            if prop.cypher_alias == self.start_cypher_alias && !self.start_is_denormalized {
                select_items.push(format!("vp.start_{} as start_{}", prop.alias, prop.alias));
            }
            if prop.cypher_alias == self.end_cypher_alias && !self.end_is_denormalized {
                select_items.push(format!(
                    "{}.{} as end_{}",
                    self.end_node_alias, prop.column_name, prop.alias
                ));
            }
        }

        let select_clause = select_items.join(",\n        ");

        // Build FROM/JOIN clause based on denormalization
        let from_clause = if self.start_is_denormalized && !self.end_is_denormalized {
            // Denorm → Standard: CTE → rel → end_table
            format!(
                "FROM {cte_name} vp\n    JOIN {rel_table} {rel} ON vp.end_id = {rel}.{from_col}\n    JOIN {end_table} {end} ON {rel}.{to_col} = {end}.{end_id_col}",
                cte_name = cte_name,
                rel_table = self.format_table_name(&self.relationship_table),
                rel = self.relationship_alias,
                from_col = self.relationship_from_column,
                end_table = self.format_table_name(&self.end_node_table),
                end = self.end_node_alias,
                to_col = self.relationship_to_column,
                end_id_col = self.end_node_id_column
            )
        } else if !self.start_is_denormalized && self.end_is_denormalized {
            // Standard → Denorm: CTE → rel (no end table)
            // PERF: Removed redundant current_node JOIN - vp.end_id already contains the ID
            format!(
                "FROM {cte_name} vp\n    JOIN {rel_table} {rel} ON vp.end_id = {rel}.{from_col}",
                cte_name = cte_name,
                rel_table = self.format_table_name(&self.relationship_table),
                rel = self.relationship_alias,
                from_col = self.relationship_from_column
            )
        } else {
            // Shouldn't reach here
            format!(
                "FROM {cte_name} vp\n    JOIN {rel_table} {rel} ON vp.end_id = {rel}.{from_col}",
                cte_name = cte_name,
                rel_table = self.format_table_name(&self.relationship_table),
                rel = self.relationship_alias,
                from_col = self.relationship_from_column
            )
        };

        let mut where_conditions = vec![
            format!("vp.hop_count < {}", max_hops),
            format!("NOT has(vp.path_edges, {})", edge_tuple_recursive),
        ];

        if self.shortest_path_mode.is_none() {
            if let Some(ref filters) = self.end_node_filters {
                let rewritten = if self.end_is_denormalized {
                    filters.replace("end_node.", &format!("{}.", self.relationship_alias))
                } else {
                    filters.clone()
                };
                where_conditions.push(rewritten);
            }
        }

        // ✅ HOLISTIC FIX: Add relationship filters in mixed recursive case
        if let Some(ref filters) = self.relationship_filters {
            log::debug!(
                "Adding relationship filters to mixed recursive case: {}",
                filters
            );
            where_conditions.push(filters.clone());
        }

        let where_clause = where_conditions.join("\n      AND ");

        format!(
            "    SELECT\n        {select}\n    {from_clause}\n    WHERE {where_clause}",
            select = select_clause,
            from_clause = from_clause,
            where_clause = where_clause
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Helper to create a minimal test schema for VLC tests
    fn create_test_schema() -> GraphSchema {
        GraphSchema::build(1, "test_db".to_string(), HashMap::new(), HashMap::new())
    }

    #[test]
    fn test_variable_length_cte_generation() {
        let schema = create_test_schema();
        let spec = VariableLengthSpec::range(1, 3);
        let generator = VariableLengthCteGenerator::new(
            &schema, // Add schema parameter
            spec,
            "users",     // start table
            "user_id",   // start id column
            "authored",  // relationship table
            "author_id", // from column
            "post_id",   // to column
            "posts",     // end table
            "post_id",   // end id column
            "u",         // start alias
            "p",         // end alias
            vec![],      // no properties for test
            None,        // no shortest path mode
            None,        // no start node filters
            None,        // no end node filters
            None,        // no path variable
            None,        // no relationship types
            None,        // no edge_id (use default from_id, to_id)
        );

        let cte = generator.generate_cte();
        println!("Generated CTE: {}", cte.cte_name);

        // Test that CTE was created
        assert!(!cte.cte_name.is_empty());
        assert!(cte.cte_name.starts_with("vlp_"));
    }

    #[test]
    fn test_unbounded_variable_length() {
        let schema = create_test_schema();
        let spec = VariableLengthSpec::unbounded();
        let generator = VariableLengthCteGenerator::new(
            &schema, // Add schema parameter
            spec,
            "users",       // start table
            "user_id",     // start id column
            "follows",     // relationship table
            "follower_id", // from column
            "followed_id", // to column
            "users",       // end table
            "user_id",     // end id column
            "u1",          // start alias
            "u2",          // end alias
            vec![],        // no properties for test
            None,          // no shortest path mode
            None,          // no start node filters
            None,          // no end node filters
            None,          // no path variable
            None,          // no relationship types
            None,          // no edge_id (use default from_id, to_id)
        );

        let sql = generator.generate_recursive_sql();
        println!("Unbounded SQL:\n{}", sql);

        // Should contain recursive case
        assert!(sql.contains("UNION ALL"));
        assert!(sql.contains("hop_count < 5")); // DEFAULT_MAX_HOPS = 5 (reduced from 10 for memory safety)
    }
    #[test]
    fn test_fixed_length_spec() {
        let spec = VariableLengthSpec::fixed(2);
        assert_eq!(spec.effective_min_hops(), 2);
        assert_eq!(spec.max_hops, Some(2));
        assert!(!spec.is_single_hop());
    }

    #[test]
    fn test_polymorphic_edge_filter() {
        // Test single relationship type with polymorphic edge
        let schema = create_test_schema();
        let spec = VariableLengthSpec::range(1, 3);
        let generator = VariableLengthCteGenerator::new_with_polymorphic(
            &schema, // Add schema parameter
            spec,
            "users",                              // start table
            "user_id",                            // start id column
            "interactions",                       // relationship table (polymorphic)
            "from_id",                            // from column
            "to_id",                              // to column
            "users",                              // end table
            "user_id",                            // end id column
            "u1",                                 // start alias
            "u2",                                 // end alias
            "r",                                  // relationship_cypher_alias (missing parameter)
            vec![],                               // no properties for test
            None,                                 // no shortest path mode
            None,                                 // no start node filters
            None,                                 // no end node filters
            None,                                 // no relationship filters (missing parameter)
            None,                                 // no path variable
            Some(vec!["FOLLOWS".to_string()]),    // relationship type
            None,                                 // no edge_id
            Some("interaction_type".to_string()), // type_column
            None,                                 // no from_label_column
            None,                                 // no to_label_column
            Some("User".to_string()),             // from_node_label
            Some("User".to_string()),             // to_node_label
        );

        let sql = generator.generate_recursive_sql();
        println!("Polymorphic edge SQL:\n{}", sql);

        // Should contain the polymorphic type filter
        assert!(
            sql.contains("interaction_type = 'FOLLOWS'"),
            "Expected polymorphic filter in base case. SQL: {}",
            sql
        );
    }

    #[test]
    fn test_polymorphic_edge_filter_multiple_types() {
        // Test multiple relationship types with polymorphic edge
        let schema = create_test_schema();
        let spec = VariableLengthSpec::range(1, 3);
        let generator = VariableLengthCteGenerator::new_with_polymorphic(
            &schema, // Add schema parameter
            spec,
            "users",                                                // start table
            "user_id",                                              // start id column
            "interactions", // relationship table (polymorphic)
            "from_id",      // from column
            "to_id",        // to column
            "users",        // end table
            "user_id",      // end id column
            "u1",           // start alias
            "u2",           // end alias
            "r",            // relationship_cypher_alias (missing parameter)
            vec![],         // no properties for test
            None,           // no shortest path mode
            None,           // no start node filters
            None,           // no end node filters
            None,           // no relationship filters (missing parameter)
            None,           // no path variable
            Some(vec!["FOLLOWS".to_string(), "LIKES".to_string()]), // multiple types
            None,           // no edge_id
            Some("interaction_type".to_string()), // type_column
            None,           // no from_label_column
            None,           // no to_label_column
            Some("User".to_string()), // from_node_label
            Some("User".to_string()), // to_node_label
        );

        let sql = generator.generate_recursive_sql();
        println!("Polymorphic edge multiple types SQL:\n{}", sql);

        // Should contain the polymorphic type filter with IN clause
        assert!(
            sql.contains("interaction_type IN ('FOLLOWS', 'LIKES')"),
            "Expected polymorphic IN filter in base case. SQL: {}",
            sql
        );
    }
}

/// Generates optimized chained JOIN SQL for exact hop count queries
/// This is much more efficient than recursive CTEs for fixed-length paths
pub struct ChainedJoinGenerator {
    pub hop_count: u32,
    pub start_node_table: String,
    pub start_node_id_column: String,
    pub relationship_table: String,
    pub relationship_from_column: String,
    pub relationship_to_column: String,
    pub end_node_table: String,
    pub end_node_id_column: String,
    pub start_cypher_alias: String,
    pub end_cypher_alias: String,
    pub properties: Vec<NodeProperty>,
    pub database: Option<String>,
}

impl ChainedJoinGenerator {
    pub fn new(
        hop_count: u32,
        start_table: &str,
        start_id_col: &str,
        relationship_table: &str,
        rel_from_col: &str,
        rel_to_col: &str,
        end_table: &str,
        end_id_col: &str,
        start_alias: &str,
        end_alias: &str,
        properties: Vec<NodeProperty>,
    ) -> Self {
        let database = std::env::var("CLICKHOUSE_DATABASE").ok();

        Self {
            hop_count,
            start_node_table: start_table.to_string(),
            start_node_id_column: start_id_col.to_string(),
            relationship_table: relationship_table.to_string(),
            relationship_from_column: rel_from_col.to_string(),
            relationship_to_column: rel_to_col.to_string(),
            end_node_table: end_table.to_string(),
            end_node_id_column: end_id_col.to_string(),
            start_cypher_alias: start_alias.to_string(),
            end_cypher_alias: end_alias.to_string(),
            properties,
            database,
        }
    }

    /// Generate a CTE containing the chained JOIN query
    /// Even though it's not recursive, we wrap it in a CTE for consistency
    pub fn generate_cte(&self) -> Cte {
        let cte_name = format!(
            "chain_{}",
            crate::query_planner::logical_plan::generate_cte_id()
        );
        let cte_sql = self.generate_query();

        // Wrap the query body with CTE name, like recursive CTE does
        let wrapped_sql = format!("{} AS (\n{}\n)", cte_name, cte_sql);

        Cte::new(
            cte_name,
            crate::render_plan::CteContent::RawSql(wrapped_sql),
            false, // Chained JOINs don't need recursion
        )
    }

    fn format_table_name(&self, table: &str) -> String {
        // If table is already qualified (contains a dot), don't add prefix again
        if table.contains('.') {
            return table.to_string();
        }

        if let Some(db) = &self.database {
            format!("{}.{}", db, table)
        } else {
            table.to_string()
        }
    }

    /// Generate a SELECT query with chained JOINs for exact hop count
    pub fn generate_query(&self) -> String {
        if self.hop_count == 0 {
            // Special case: 0 hops means start node == end node
            return self.generate_zero_hop_query();
        }

        let mut sql = String::new();

        // Build SELECT clause with properties
        let mut select_items = vec![
            format!("s.{} as start_id", self.start_node_id_column),
            format!("e.{} as end_id", self.end_node_id_column),
        ];

        // Add start node properties
        for prop in &self.properties {
            if prop.cypher_alias == self.start_cypher_alias {
                select_items.push(format!("s.{} as start_{}", prop.column_name, prop.alias));
            }
        }

        // Add end node properties
        for prop in &self.properties {
            if prop.cypher_alias == self.end_cypher_alias {
                select_items.push(format!("e.{} as end_{}", prop.column_name, prop.alias));
            }
        }

        sql.push_str("SELECT \n    ");
        sql.push_str(&select_items.join(",\n    "));
        sql.push_str("\nFROM ");
        sql.push_str(&self.format_table_name(&self.start_node_table));
        sql.push_str(" s\n");

        // Generate chain of JOINs
        for hop in 1..=self.hop_count {
            let rel_alias = format!("r{}", hop);
            let node_alias = if hop == self.hop_count {
                "e".to_string()
            } else {
                format!("m{}", hop)
            };

            let prev_node = if hop == 1 {
                "s".to_string()
            } else {
                format!("m{}", hop - 1)
            };

            // Add relationship JOIN
            sql.push_str(&format!(
                "JOIN {} {} ON {}.{} = {}.{}\n",
                self.format_table_name(&self.relationship_table),
                rel_alias,
                prev_node,
                self.start_node_id_column,
                rel_alias,
                self.relationship_from_column
            ));

            // Add node JOIN
            let node_table = if hop == self.hop_count {
                &self.end_node_table
            } else {
                &self.start_node_table // Intermediate nodes are same type as start
            };

            sql.push_str(&format!(
                "JOIN {} {} ON {}.{} = {}.{}\n",
                self.format_table_name(node_table),
                node_alias,
                rel_alias,
                self.relationship_to_column,
                node_alias,
                if hop == self.hop_count {
                    &self.end_node_id_column
                } else {
                    &self.start_node_id_column
                }
            ));
        }

        // Add WHERE clause for cycle prevention
        if self.hop_count > 1 {
            sql.push_str("WHERE ");
            let mut conditions = vec![];

            // Prevent start == end
            conditions.push(format!(
                "s.{} != e.{}",
                self.start_node_id_column, self.end_node_id_column
            ));

            // Prevent intermediate nodes from being start or end
            for hop in 1..self.hop_count {
                let mid_alias = format!("m{}", hop);
                conditions.push(format!(
                    "s.{} != {}.{}",
                    self.start_node_id_column, mid_alias, self.start_node_id_column
                ));
                conditions.push(format!(
                    "e.{} != {}.{}",
                    self.end_node_id_column, mid_alias, self.start_node_id_column
                ));
            }

            // Prevent intermediate nodes from repeating
            if self.hop_count > 2 {
                for i in 1..self.hop_count {
                    for j in (i + 1)..self.hop_count {
                        conditions.push(format!(
                            "m{}.{} != m{}.{}",
                            i, self.start_node_id_column, j, self.start_node_id_column
                        ));
                    }
                }
            }

            sql.push_str(&conditions.join("\n  AND "));
        }

        sql
    }

    fn generate_zero_hop_query(&self) -> String {
        let mut select_items = vec![
            format!("s.{} as start_id", self.start_node_id_column),
            format!("s.{} as end_id", self.start_node_id_column),
        ];

        // Add properties (both start and end reference same node)
        for prop in &self.properties {
            if prop.cypher_alias == self.start_cypher_alias {
                select_items.push(format!("s.{} as start_{}", prop.column_name, prop.alias));
            }
            if prop.cypher_alias == self.end_cypher_alias {
                select_items.push(format!("s.{} as end_{}", prop.column_name, prop.alias));
            }
        }

        format!(
            "SELECT \n    {}\nFROM {} s",
            select_items.join(",\n    "),
            self.format_table_name(&self.start_node_table)
        )
    }
}

#[cfg(test)]
mod chained_join_tests {
    use super::*;

    #[test]
    fn test_chained_join_2_hops() {
        let generator = ChainedJoinGenerator::new(
            2,
            "users",
            "user_id",
            "friendships",
            "user1_id",
            "user2_id",
            "users",
            "user_id",
            "u1",
            "u2",
            vec![],
        );

        let sql = generator.generate_query();
        println!("2-hop chained JOIN:\n{}", sql);

        assert!(sql.contains("FROM") && sql.contains("users"));
        assert!(sql.contains("JOIN") && sql.contains("friendships"));
        assert!(sql.contains("r1") && sql.contains("r2")); // 2 relationship aliases
        assert!(sql.contains("m1")); // 1 intermediate node
        assert!(sql.contains("WHERE")); // Cycle prevention
    }

    #[test]
    fn test_chained_join_3_hops() {
        let generator = ChainedJoinGenerator::new(
            3,
            "users",
            "user_id",
            "friendships",
            "user1_id",
            "user2_id",
            "users",
            "user_id",
            "u1",
            "u2",
            vec![],
        );

        let sql = generator.generate_query();
        println!("3-hop chained JOIN:\n{}", sql);

        assert!(sql.contains("r1") && sql.contains("r2") && sql.contains("r3"));
        assert!(sql.contains("m1") && sql.contains("m2")); // 2 intermediate nodes
    }

    #[test]
    fn test_chained_join_with_properties() {
        let properties = vec![
            NodeProperty {
                cypher_alias: "u1".to_string(),
                column_name: "full_name".to_string(),
                alias: "name".to_string(),
            },
            NodeProperty {
                cypher_alias: "u2".to_string(),
                column_name: "email_address".to_string(),
                alias: "email".to_string(),
            },
        ];

        let generator = ChainedJoinGenerator::new(
            2,
            "users",
            "user_id",
            "friendships",
            "user1_id",
            "user2_id",
            "users",
            "user_id",
            "u1",
            "u2",
            properties,
        );

        let sql = generator.generate_query();
        println!("2-hop with properties:\n{}", sql);

        assert!(sql.contains("s.full_name as start_name"));
        assert!(sql.contains("e.email_address as end_email"));
    }
}
