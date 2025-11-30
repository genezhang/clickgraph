use crate::graph_catalog::config::Identifier;
use crate::query_planner::logical_plan::VariableLengthSpec;
use crate::render_plan::Cte;

/// Property to include in the CTE (column name and which node it belongs to)
#[derive(Debug, Clone)]
pub struct NodeProperty {
    pub cypher_alias: String, // "u1" or "u2" - which node this property is for
    pub column_name: String,  // Actual column name in the table (e.g., "full_name")
    pub alias: String,        // Output alias (e.g., "name" or "u1_name")
}

/// Generates recursive CTE SQL for variable-length path traversal
pub struct VariableLengthCteGenerator {
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
    pub properties: Vec<NodeProperty>, // Properties to include in the CTE
    pub database: Option<String>,   // Optional database prefix
    pub shortest_path_mode: Option<ShortestPathMode>, // Shortest path optimization mode
    pub start_node_filters: Option<String>, // WHERE clause for start node (e.g., "start_node.full_name = 'Alice'")
    pub end_node_filters: Option<String>, // WHERE clause for end node (e.g., "end_full_name = 'Bob'")
    pub path_variable: Option<String>, // Path variable name from MATCH clause (e.g., "p" in "MATCH p = ...")
    pub relationship_types: Option<Vec<String>>, // Relationship type labels (e.g., ["FOLLOWS", "FRIENDS_WITH"])
    pub edge_id: Option<Identifier>, // Edge ID columns for relationship uniqueness (None = use from_id, to_id)
    pub is_denormalized: bool, // True if BOTH nodes are virtual (for backward compat)
    pub start_is_denormalized: bool, // True if start node is virtual (properties come from edge table)
    pub end_is_denormalized: bool, // True if end node is virtual (properties come from edge table)
    // Polymorphic edge fields - for filtering unified edge tables by type
    pub type_column: Option<String>, // Discriminator column for relationship type (e.g., "interaction_type")
    pub from_label_column: Option<String>, // Discriminator column for source node type
    pub to_label_column: Option<String>, // Discriminator column for target node type
    pub from_node_label: Option<String>, // Expected value for from_label_column (e.g., "User")
    pub to_node_label: Option<String>, // Expected value for to_label_column (e.g., "Post")
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

impl VariableLengthCteGenerator {
    pub fn new(
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
        edge_id: Option<Identifier>,   // Edge ID for relationship uniqueness
    ) -> Self {
        Self::new_with_polymorphic(
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
            properties,
            shortest_path_mode,
            start_node_filters,
            end_node_filters,
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
        properties: Vec<NodeProperty>,
        shortest_path_mode: Option<ShortestPathMode>,
        start_node_filters: Option<String>,
        end_node_filters: Option<String>,
        path_variable: Option<String>,
        relationship_types: Option<Vec<String>>,
        edge_id: Option<Identifier>,
        type_column: Option<String>,
        from_label_column: Option<String>,
        to_label_column: Option<String>,
        from_node_label: Option<String>,
        to_node_label: Option<String>,
    ) -> Self {
        // Try to get database from environment
        let database = std::env::var("CLICKHOUSE_DATABASE").ok();

        Self {
            spec,
            cte_name: format!("variable_path_{}", uuid::Uuid::new_v4().simple()),
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
            properties,
            database,
            shortest_path_mode,
            start_node_filters,
            end_node_filters,
            path_variable,
            relationship_types,
            edge_id,
            is_denormalized: false,
            start_is_denormalized: false,
            end_is_denormalized: false,
            type_column,
            from_label_column,
            to_label_column,
            from_node_label,
            to_node_label,
        }
    }

    /// Create a generator for denormalized edges (node properties embedded in edge table)
    pub fn new_denormalized(
        spec: VariableLengthSpec,
        relationship_table: &str,      // The only table - edge table with node properties
        rel_from_col: &str,            // From column (e.g., "Origin")
        rel_to_col: &str,              // To column (e.g., "Dest")
        start_alias: &str,             // Cypher alias (e.g., "a")
        end_alias: &str,               // Cypher alias (e.g., "b")
        shortest_path_mode: Option<ShortestPathMode>,
        start_node_filters: Option<String>,
        end_node_filters: Option<String>,
        path_variable: Option<String>,
        relationship_types: Option<Vec<String>>,
        edge_id: Option<Identifier>,
    ) -> Self {
        let database = std::env::var("CLICKHOUSE_DATABASE").ok();

        Self {
            spec,
            cte_name: format!("variable_path_{}", uuid::Uuid::new_v4().simple()),
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
            properties: vec![], // Denormalized doesn't need property mapping
            database,
            shortest_path_mode,
            start_node_filters,
            end_node_filters,
            path_variable,
            relationship_types,
            edge_id,
            is_denormalized: true, // Enable denormalized mode (both nodes)
            start_is_denormalized: true, // Start node is denormalized
            end_is_denormalized: true, // End node is denormalized
            // Polymorphic edge fields - not used for denormalized edges
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_node_label: None,
            to_node_label: None,
        }
    }

    /// Create a generator for mixed patterns (one node denormalized, one standard)
    #[allow(clippy::too_many_arguments)]
    pub fn new_mixed(
        spec: VariableLengthSpec,
        start_table: &str,             // Start node table (or rel table if start is denorm)
        start_id_col: &str,            // Start ID column
        relationship_table: &str,      // Relationship table
        rel_from_col: &str,            // Relationship from column
        rel_to_col: &str,              // Relationship to column
        end_table: &str,               // End node table (or rel table if end is denorm)
        end_id_col: &str,              // End ID column
        start_alias: &str,             // Cypher alias for start node
        end_alias: &str,               // Cypher alias for end node
        properties: Vec<NodeProperty>, // Properties to include
        shortest_path_mode: Option<ShortestPathMode>,
        start_node_filters: Option<String>,
        end_node_filters: Option<String>,
        path_variable: Option<String>,
        relationship_types: Option<Vec<String>>,
        edge_id: Option<Identifier>,
        start_is_denormalized: bool,   // Whether start node is denormalized
        end_is_denormalized: bool,     // Whether end node is denormalized
    ) -> Self {
        let database = std::env::var("CLICKHOUSE_DATABASE").ok();

        Self {
            spec,
            cte_name: format!("variable_path_{}", uuid::Uuid::new_v4().simple()),
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
            properties,
            database,
            shortest_path_mode,
            start_node_filters,
            end_node_filters,
            path_variable,
            relationship_types,
            edge_id,
            is_denormalized: start_is_denormalized && end_is_denormalized, // Both must be denorm for full denorm mode
            start_is_denormalized,
            end_is_denormalized,
            // Polymorphic edge fields - not used for mixed mode yet
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_node_label: None,
            to_node_label: None,
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
            eprintln!("    🔹 VLP polymorphic edge filter: {}", filter);
            Some(filter)
        }
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

    /// Generate the recursive CTE for variable-length traversal
    pub fn generate_cte(&self) -> Cte {
        let cte_sql = self.generate_recursive_sql();

        Cte {
            cte_name: self.cte_name.clone(),
            content: crate::render_plan::CteContent::RawSql(cte_sql),
            is_recursive: true,
        }
    }

    /// Rewrite end node filter for use in intermediate CTEs
    /// Transforms "end_node.property" references to "end_property" column names
    fn rewrite_end_filter_for_cte(&self, filter: &str) -> String {
        // Replace end_node.{id_column} with end_id
        let mut rewritten = filter.replace(
            &format!("{}.{}", self.end_node_alias, self.end_node_id_column),
            "end_id",
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

    /// Generate the actual recursive SQL string
    fn generate_recursive_sql(&self) -> String {
        let min_hops = self.spec.effective_min_hops();
        let max_hops = self.spec.max_hops;

        // Determine if we need an _inner CTE wrapper
        // This is needed when we have:
        // 1. Shortest path mode (which requires post-processing)
        // 2. min_hops > 1 (base case generates hop 1, but we need to filter)
        let needs_inner_cte = self.shortest_path_mode.is_some() || min_hops > 1;
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
            query_body.push_str("\n    UNION ALL\n");
            
            let default_depth = if max_hops.is_none() {
                // Unbounded case: use reasonable default
                if self.shortest_path_mode.is_some() && min_hops == 0 {
                    3 // Lower limit for shortest path from a to a queries
                } else {
                    10 // Standard default
                }
            } else {
                max_hops.unwrap()
            };
            
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
            (None, Some(_end_filters)) => {
                // For non-shortest-path mode, end filters are ALREADY applied in base/recursive cases
                // (see generate_base_case and generate_recursive_case_with_cte_name)
                // But we still need to apply min_hops filtering if min_hops > 1
                if min_hops > 1 {
                    format!(
                        "{}_inner AS (\n{}\n),\n{} AS (\n    SELECT * FROM {}_inner WHERE hop_count >= {}\n)",
                        self.cte_name, query_body, self.cte_name, self.cte_name, min_hops
                    )
                } else {
                    format!("{} AS (\n{}\n)", self.cte_name, query_body)
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
            format!("[{}.{}] as path_nodes", self.start_node_alias, self.start_node_id_column),
        ];

        // Add properties for start node (which is also the end node)
        for prop in &self.properties {
            if prop.cypher_alias == self.start_cypher_alias {
                select_items.push(format!(
                    "{}.{} as start_{}",
                    self.start_node_alias, prop.column_name, prop.alias
                ));
            }
            // For zero-hop, end properties are same as start properties
            if prop.cypher_alias == self.end_cypher_alias {
                select_items.push(format!(
                    "{}.{} as end_{}",
                    self.start_node_alias, prop.column_name, prop.alias
                ));
            }
        }

        let select_clause = select_items.join(",\n        ");

        // Build the zero-hop query - just select from start table
        let mut query = format!(
            "    SELECT \n        {}\n    FROM {} {}",
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
        // Full standard: both nodes standard → use standard generator
        
        if self.is_denormalized {
            // Both nodes denormalized (fully virtual)
            return self.generate_denormalized_base_case(hop_count);
        }
        
        // Check for mixed patterns (one side denormalized)
        if self.start_is_denormalized || self.end_is_denormalized {
            return self.generate_mixed_base_case(hop_count);
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
                    self.start_node_alias, self.start_node_id_column,
                    self.end_node_alias, self.end_node_id_column
                ),
            ];

            // Add properties for start and end nodes
            // CRITICAL: Use separate if statements (not else-if) for self-loops
            // When start_cypher_alias == end_cypher_alias, both conditions are true
            for prop in &self.properties {
                if prop.cypher_alias == self.start_cypher_alias {
                    // Property belongs to start node
                    select_items.push(format!(
                        "{}.{} as start_{}",
                        self.start_node_alias, prop.column_name, prop.alias
                    ));
                }
                if prop.cypher_alias == self.end_cypher_alias {
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
                "    SELECT \n        {select}\n    FROM {start_table} {start}\n    JOIN {rel_table} {rel} ON {start}.{start_id_col} = {rel}.{from_col}\n    JOIN {end_table} {end} ON {rel}.{to_col} = {end}.{end_id_col}",
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
            
            if let Some(ref filters) = self.start_node_filters {
                where_conditions.push(filters.clone());
            }
            // Only add end_node_filters in base case if NOT using shortest path mode
            if self.shortest_path_mode.is_none() {
                if let Some(ref filters) = self.end_node_filters {
                    where_conditions.push(filters.clone());
                }
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
            if prop.cypher_alias == self.start_cypher_alias {
                // Start node properties pass through from CTE
                select_items.push(format!("vp.start_{} as start_{}", prop.alias, prop.alias));
            }
            if prop.cypher_alias == self.end_cypher_alias {
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
            format!(
                "NOT has(vp.path_edges, {})",
                edge_tuple_check
            ), // Edge uniqueness check (Neo4j semantics)
        ];

        // Add polymorphic edge filter if this is a polymorphic edge table
        if let Some(poly_filter) = self.generate_polymorphic_edge_filter() {
            where_conditions.push(poly_filter);
        }

        // Note: We no longer skip zero-hop rows in recursion.
        // The recursion can now start from zero-hop base case and expand from there.
        // Cycle detection (NOT has) prevents infinite loops.

        // For shortest path queries, do NOT add end_node_filters in recursive case
        // End filters are applied in the _to_target wrapper CTE after recursion completes
        // This allows the recursion to explore all paths until the target is found
        if self.shortest_path_mode.is_none() {
            if let Some(ref filters) = self.end_node_filters {
                where_conditions.push(filters.clone());
            }
        }

        let where_clause = where_conditions.join("\n      AND ");

        format!(
            "    SELECT\n        {select}\n    FROM {cte_name} vp\n    JOIN {current_table} current_node ON vp.end_id = current_node.{current_id_col}\n    JOIN {rel_table} {rel} ON current_node.{current_id_col} = {rel}.{from_col}\n    JOIN {end_table} {end} ON {rel}.{to_col} = {end}.{end_id_col}\n    WHERE {where_clause}",
            select = select_clause,
            end = self.end_node_alias,
            end_id_col = self.end_node_id_column,
            current_id_col = self.end_node_id_column,
            cte_name = cte_name, // Use the passed parameter instead of self.cte_name
            current_table = self.format_table_name(&self.end_node_table),
            rel_table = self.format_table_name(&self.relationship_table),
            from_col = self.relationship_from_column,
            to_col = self.relationship_to_column,
            rel = self.relationship_alias,
            end_table = self.format_table_name(&self.end_node_table),
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
        if hop_count != 1 {
            // Multi-hop base case not yet supported for denormalized
            return format!(
                "    -- Multi-hop base case for {} hops (denormalized - not yet supported)\n    SELECT NULL as start_id, NULL as end_id, {} as hop_count, [] as path_edges, [] as path_relationships, [] as path_nodes\n    WHERE false",
                hop_count, hop_count
            );
        }

        // Build edge tuple for cycle detection
        let edge_tuple = self.build_edge_tuple_base();

        // For denormalized, start_id and end_id come directly from the relationship columns
        // Include path_nodes for UNWIND nodes(p) support
        let select_clause = format!(
            "{rel}.{from_col} as start_id,\n        {rel}.{to_col} as end_id,\n        1 as hop_count,\n        [{edge_tuple}] as path_edges,\n        {path_rels},\n        [{rel}.{from_col}, {rel}.{to_col}] as path_nodes",
            rel = self.relationship_alias,
            from_col = self.relationship_from_column,
            to_col = self.relationship_to_column,
            edge_tuple = edge_tuple,
            path_rels = self.generate_relationship_type_for_hop(1)
        );

        // Simple FROM - just the relationship table, no node tables
        let mut query = format!(
            "    SELECT \n        {select}\n    FROM {rel_table} {rel}",
            select = select_clause,
            rel_table = self.format_table_name(&self.relationship_table),
            rel = self.relationship_alias
        );

        // Add WHERE clause for start node filters (rewritten for rel table)
        let mut where_conditions = Vec::new();
        if let Some(ref filters) = self.start_node_filters {
            // Rewrite start_node references to rel references
            let rewritten = filters.replace("start_node.", &format!("{}.", self.relationship_alias));
            where_conditions.push(rewritten);
        }
        
        if self.shortest_path_mode.is_none() {
            if let Some(ref filters) = self.end_node_filters {
                // Rewrite end_node references to rel references
                let rewritten = filters.replace("end_node.", &format!("{}.", self.relationship_alias));
                where_conditions.push(rewritten);
            }
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

        // For denormalized, the recursive case:
        // - Takes start_id from the CTE (previous path start)
        // - Takes end_id from the new relationship's to_col
        // Include path_nodes for UNWIND nodes(p) support
        let select_clause = format!(
            "vp.start_id,\n        {rel}.{to_col} as end_id,\n        vp.hop_count + 1 as hop_count,\n        arrayConcat(vp.path_edges, [{edge_tuple}]) as path_edges,\n        arrayConcat(vp.path_relationships, {path_rels}) as path_relationships,\n        arrayConcat(vp.path_nodes, [{rel}.{to_col}]) as path_nodes",
            rel = self.relationship_alias,
            to_col = self.relationship_to_column,
            edge_tuple = edge_tuple_recursive,
            path_rels = self.get_relationship_type_array()
        );

        let mut where_conditions = vec![
            format!("vp.hop_count < {}", max_hops),
            format!("NOT has(vp.path_edges, {})", edge_tuple_recursive),
        ];

        if self.shortest_path_mode.is_none() {
            if let Some(ref filters) = self.end_node_filters {
                let rewritten = filters.replace("end_node.", &format!("{}.", self.relationship_alias));
                where_conditions.push(rewritten);
            }
        }

        let where_clause = where_conditions.join("\n      AND ");

        // For denormalized: join directly from CTE end_id to new rel's from_col
        // No intermediate node table needed
        format!(
            "    SELECT\n        {select}\n    FROM {cte_name} vp\n    JOIN {rel_table} {rel} ON vp.end_id = {rel}.{from_col}\n    WHERE {where_clause}",
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
            format!("{}.{}", self.relationship_alias, self.relationship_from_column)
        } else {
            // Start is standard: ID comes from start node table
            format!("{}.{}", self.start_node_alias, self.start_node_id_column)
        };

        let end_id_expr = if self.end_is_denormalized {
            // End is denorm: ID comes from relationship table to_col
            format!("{}.{}", self.relationship_alias, self.relationship_to_column)
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
            format!("{}.{}", self.relationship_alias, self.relationship_to_column)
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
            // Standard → Denorm: CTE → current → rel (no end table)
            // We join through current node to find the next relationship
            format!(
                "FROM {cte_name} vp\n    JOIN {current_table} current_node ON vp.end_id = current_node.{current_id_col}\n    JOIN {rel_table} {rel} ON current_node.{current_id_col} = {rel}.{from_col}",
                cte_name = cte_name,
                current_table = self.format_table_name(&self.end_node_table),
                current_id_col = self.end_node_id_column,
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

    #[test]
    fn test_variable_length_cte_generation() {
        let spec = VariableLengthSpec::range(1, 3);
        let generator = VariableLengthCteGenerator::new(
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
        assert!(cte.cte_name.starts_with("variable_path_"));
    }

    #[test]
    fn test_unbounded_variable_length() {
        let spec = VariableLengthSpec::unbounded();
        let generator = VariableLengthCteGenerator::new(
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
        assert!(sql.contains("hop_count < 10")); // Default max
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
        let spec = VariableLengthSpec::range(1, 3);
        let generator = VariableLengthCteGenerator::new_with_polymorphic(
            spec,
            "users",         // start table
            "user_id",       // start id column
            "interactions",  // relationship table (polymorphic)
            "from_id",       // from column
            "to_id",         // to column
            "users",         // end table
            "user_id",       // end id column
            "u1",            // start alias
            "u2",            // end alias
            vec![],          // no properties for test
            None,            // no shortest path mode
            None,            // no start node filters
            None,            // no end node filters
            None,            // no path variable
            Some(vec!["FOLLOWS".to_string()]), // relationship type
            None,            // no edge_id
            Some("interaction_type".to_string()), // type_column
            None,            // no from_label_column
            None,            // no to_label_column
            Some("User".to_string()), // from_node_label
            Some("User".to_string()), // to_node_label
        );

        let sql = generator.generate_recursive_sql();
        println!("Polymorphic edge SQL:\n{}", sql);

        // Should contain the polymorphic type filter
        assert!(sql.contains("interaction_type = 'FOLLOWS'"), 
               "Expected polymorphic filter in base case. SQL: {}", sql);
    }

    #[test]
    fn test_polymorphic_edge_filter_multiple_types() {
        // Test multiple relationship types with polymorphic edge
        let spec = VariableLengthSpec::range(1, 3);
        let generator = VariableLengthCteGenerator::new_with_polymorphic(
            spec,
            "users",         // start table
            "user_id",       // start id column
            "interactions",  // relationship table (polymorphic)
            "from_id",       // from column
            "to_id",         // to column
            "users",         // end table
            "user_id",       // end id column
            "u1",            // start alias
            "u2",            // end alias
            vec![],          // no properties for test
            None,            // no shortest path mode
            None,            // no start node filters
            None,            // no end node filters
            None,            // no path variable
            Some(vec!["FOLLOWS".to_string(), "LIKES".to_string()]), // multiple types
            None,            // no edge_id
            Some("interaction_type".to_string()), // type_column
            None,            // no from_label_column
            None,            // no to_label_column
            Some("User".to_string()), // from_node_label
            Some("User".to_string()), // to_node_label
        );

        let sql = generator.generate_recursive_sql();
        println!("Polymorphic edge multiple types SQL:\n{}", sql);

        // Should contain the polymorphic type filter with IN clause
        assert!(sql.contains("interaction_type IN ('FOLLOWS', 'LIKES')"), 
               "Expected polymorphic IN filter in base case. SQL: {}", sql);
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
        let cte_name = format!("chained_path_{}", uuid::Uuid::new_v4().simple());
        let cte_sql = self.generate_query();

        // Wrap the query body with CTE name, like recursive CTE does
        let wrapped_sql = format!("{} AS (\n{}\n)", cte_name, cte_sql);

        Cte {
            cte_name,
            content: crate::render_plan::CteContent::RawSql(wrapped_sql),
            is_recursive: false, // Chained JOINs don't need recursion
        }
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
