use crate::query_planner::logical_plan::VariableLengthSpec;
use crate::render_plan::Cte;

/// Property to include in the CTE (column name and which node it belongs to)
#[derive(Debug, Clone)]
pub struct NodeProperty {
    pub cypher_alias: String,  // "u1" or "u2" - which node this property is for
    pub column_name: String,    // Actual column name in the table (e.g., "full_name")
    pub alias: String,          // Output alias (e.g., "name" or "u1_name")
}

/// Generates recursive CTE SQL for variable-length path traversal
pub struct VariableLengthCteGenerator {
    pub spec: VariableLengthSpec,
    pub cte_name: String,
    pub start_node_table: String,
    pub start_node_id_column: String,    // ID column for start node (e.g., "user_id")
    pub start_node_alias: String, 
    pub relationship_table: String,
    pub relationship_from_column: String, // From column in relationship table
    pub relationship_to_column: String,   // To column in relationship table
    pub relationship_alias: String,
    pub end_node_table: String,
    pub end_node_id_column: String,      // ID column for end node
    pub end_node_alias: String,
    pub start_cypher_alias: String,      // Original Cypher query alias (e.g., "u1")
    pub end_cypher_alias: String,        // Original Cypher query alias (e.g., "u2")
    pub properties: Vec<NodeProperty>,   // Properties to include in the CTE
    pub database: Option<String>,        // Optional database prefix
    pub shortest_path_mode: Option<ShortestPathMode>, // Shortest path optimization mode
    pub start_node_filters: Option<String>, // WHERE clause for start node (e.g., "start_node.full_name = 'Alice'")
    pub end_node_filters: Option<String>,   // WHERE clause for end node (e.g., "end_full_name = 'Bob'")
    pub path_variable: Option<String>,   // Path variable name from MATCH clause (e.g., "p" in "MATCH p = ...")
    pub relationship_types: Option<Vec<String>>, // Relationship type labels (e.g., ["FOLLOWS", "FRIENDS_WITH"])
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
        start_table: &str,         // Actual table name (e.g., "users")
        start_id_col: &str,        // ID column name (e.g., "user_id")
        relationship_table: &str,  // Actual relationship table name
        rel_from_col: &str,        // Relationship from column (e.g., "follower_id")
        rel_to_col: &str,          // Relationship to column (e.g., "followed_id")
        end_table: &str,           // Actual table name (e.g., "users")
        end_id_col: &str,          // ID column name (e.g., "user_id")
        start_alias: &str,         // Cypher alias (e.g., "u1")
        end_alias: &str,           // Cypher alias (e.g., "u2")
        properties: Vec<NodeProperty>, // Properties to include in CTE
        shortest_path_mode: Option<ShortestPathMode>, // Shortest path mode
        start_node_filters: Option<String>, // WHERE clause for start node
        end_node_filters: Option<String>,   // WHERE clause for end node
        path_variable: Option<String>,      // Path variable name (e.g., "p")
        relationship_types: Option<Vec<String>>, // Relationship type labels (e.g., ["FOLLOWS", "FRIENDS_WITH"])
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
        }
    }
    
    /// Helper to format table name with optional database prefix
    fn format_table_name(&self, table: &str) -> String {
        if let Some(db) = &self.database {
            format!("{}.{}", db, table)
        } else {
            table.to_string()
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

    /// Generate the recursive CTE for variable-length traversal
    pub fn generate_cte(&self) -> Cte {
        let cte_sql = self.generate_recursive_sql();
        
        Cte {
            cte_name: self.cte_name.clone(),
            content: crate::render_plan::CteContent::RawSql(cte_sql),
            is_recursive: true,
        }
    }

    /// Generate the actual recursive SQL string
    fn generate_recursive_sql(&self) -> String {
        let min_hops = self.spec.effective_min_hops();
        let max_hops = self.spec.max_hops;

        // Generate the core recursive query body (without CTE name wrapper)
        let mut query_body = String::new();

        // Base case: Generate for each required hop level from 1 to min_hops
        for hop in 1..=min_hops {
            if hop > 1 {
                query_body.push_str("\n    UNION ALL\n");
            }
            query_body.push_str(&self.generate_base_case(hop));
        }

        // Recursive case: If max_hops > min_hops, add recursive traversal
        if let Some(max) = max_hops {
            if max > min_hops {
                query_body.push_str("\n    UNION ALL\n");
                query_body.push_str(&self.generate_recursive_case(max));
            }
        } else {
            // Unbounded case: add recursive traversal with reasonable default limit
            query_body.push_str("\n    UNION ALL\n");
            query_body.push_str(&self.generate_recursive_case(10)); // Default max depth
        }

        // Build CTE structure based on shortest path mode and filters
        // For shortest path queries, end filters are now applied during path generation
        // in the inner CTE, so we don't need separate filtering steps
        let sql = match (&self.shortest_path_mode, &self.end_node_filters) {
            (Some(ShortestPathMode::Shortest), Some(end_filters)) => {
                // Find shortest path first, then apply end filters
                format!(
                    "{}_inner AS (\n{}\n),\n{}_shortest AS (\n    SELECT * FROM {}_inner ORDER BY hop_count ASC LIMIT 1\n),\n{}_to_target AS (\n    SELECT * FROM {}_shortest WHERE {}\n),\n{} AS (\n    SELECT * FROM {}_to_target\n)",
                    self.cte_name, query_body, self.cte_name, self.cte_name, self.cte_name, self.cte_name, end_filters, self.cte_name, self.cte_name
                )
            }
            (Some(ShortestPathMode::AllShortest), Some(end_filters)) => {
                // Find all shortest paths first, then apply end filters
                format!(
                    "{}_inner AS (\n{}\n),\n{}_all_shortest AS (\n    SELECT * FROM {}_inner WHERE hop_count = (SELECT MIN(hop_count) FROM {}_inner)\n),\n{}_to_target AS (\n    SELECT * FROM {}_all_shortest WHERE {}\n),\n{} AS (\n    SELECT * FROM {}_to_target\n)",
                    self.cte_name, query_body, self.cte_name, self.cte_name, self.cte_name, self.cte_name, self.cte_name, end_filters, self.cte_name, self.cte_name
                )
            }
            (Some(ShortestPathMode::Shortest), None) => {
                // 2-tier: inner → select shortest (no target filter)
                format!(
                    "{}_inner AS (\n{}\n),\n{} AS (\n    SELECT * FROM {}_inner ORDER BY hop_count ASC LIMIT 1\n)",
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
                // End filters are applied in separate _to_target CTE
                format!(
                    "{}_inner AS (\n{}\n),\n{}_to_target AS (\n    SELECT * FROM {}_inner WHERE {}\n),\n{} AS (\n    SELECT * FROM {}_to_target\n)",
                    self.cte_name, query_body, self.cte_name, self.cte_name, end_filters, self.cte_name, self.cte_name
                )
            }
            (None, None) => {
                // Simple: just wrap with CTE name (no filtering)
                format!("{} AS (\n{}\n)", self.cte_name, query_body)
            }
        };
        
        sql
    }

    /// Generate base case for a specific hop count
    fn generate_base_case(&self, hop_count: u32) -> String {
        if hop_count == 1 {
            // Build property selections
            let mut select_items = vec![
                format!("{}.{} as start_id", self.start_node_alias, self.start_node_id_column),
                format!("{}.{} as end_id", self.end_node_alias, self.end_node_id_column),
                "1 as hop_count".to_string(),
                format!("[{}.{}] as path_nodes", self.start_node_alias, self.start_node_id_column),
                self.generate_relationship_type_for_hop(1), // path_relationships for single hop
            ];
            
            // Add properties for start and end nodes
            for prop in &self.properties {
                if prop.cypher_alias == self.start_cypher_alias {
                    // Property belongs to start node
                    select_items.push(format!("{}.{} as start_{}", 
                        self.start_node_alias, prop.column_name, prop.alias));
                } else if prop.cypher_alias == self.end_cypher_alias {
                    // Property belongs to end node
                    select_items.push(format!("{}.{} as end_{}", 
                        self.end_node_alias, prop.column_name, prop.alias));
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
            let mut where_conditions = Vec::new();
            if let Some(ref filters) = self.start_node_filters {
                where_conditions.push(filters.clone());
            }
            if let Some(ref filters) = self.end_node_filters {
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
            "    -- Multi-hop base case for {} hops (simplified)\n    SELECT NULL as start_id, NULL as end_id, {} as hop_count, [] as path_nodes, [] as path_relationships\n    WHERE false  -- Placeholder",
            hop_count, hop_count
        )
    }

    /// Generate recursive case that extends existing paths
    fn generate_recursive_case(&self, max_hops: u32) -> String {
        // Build property selections for recursive case
        let mut select_items = vec![
            "vp.start_id".to_string(),
            format!("{}.{} as end_id", self.end_node_alias, self.end_node_id_column),
            "vp.hop_count + 1 as hop_count".to_string(),
            format!("arrayConcat(vp.path_nodes, [current_node.{}]) as path_nodes", self.end_node_id_column),
            format!("arrayConcat(vp.path_relationships, {}) as path_relationships", self.get_relationship_type_array()),
        ];
        
        // Add properties: start properties come from CTE, end properties from new joined node
        for prop in &self.properties {
            if prop.cypher_alias == self.start_cypher_alias {
                // Start node properties pass through from CTE
                select_items.push(format!("vp.start_{} as start_{}", prop.alias, prop.alias));
            } else if prop.cypher_alias == self.end_cypher_alias {
                // End node properties come from the newly joined node
                select_items.push(format!("{}.{} as end_{}", 
                    self.end_node_alias, prop.column_name, prop.alias));
            }
        }
        
        let select_clause = select_items.join(",\n        ");
        
        let mut where_conditions = vec![
            format!("vp.hop_count < {}", max_hops),
            format!("NOT has(vp.path_nodes, current_node.{})", self.end_node_id_column),  // Cycle detection
        ];
        
        // Add end node filters if present
        if let Some(ref filters) = self.end_node_filters {
            where_conditions.push(filters.clone());
        }
        
        let where_clause = where_conditions.join("\n      AND ");
        
        format!(
            "    SELECT\n        {select}\n    FROM {cte_name} vp\n    JOIN {current_table} current_node ON vp.end_id = current_node.{current_id_col}\n    JOIN {rel_table} {rel} ON current_node.{current_id_col} = {rel}.{from_col}\n    JOIN {end_table} {end} ON {rel}.{to_col} = {end}.{end_id_col}\n    WHERE {where_clause}",
            select = select_clause,
            end = self.end_node_alias,
            end_id_col = self.end_node_id_column,
            current_id_col = self.end_node_id_column,
            cte_name = self.cte_name,
            current_table = self.format_table_name(&self.end_node_table),
            rel_table = self.format_table_name(&self.relationship_table),
            from_col = self.relationship_from_column,
            to_col = self.relationship_to_column,
            rel = self.relationship_alias,
            end_table = self.format_table_name(&self.end_node_table),
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
            "users",       // start table
            "user_id",     // start id column
            "authored",    // relationship table
            "author_id",   // from column
            "post_id",     // to column
            "posts",       // end table
            "post_id",     // end id column
            "u",           // start alias
            "p",           // end alias
            vec![],        // no properties for test
            None,          // no shortest path mode
            None,          // no start node filters
            None,          // no end node filters
            None,          // no path variable
            None,          // no relationship types
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
            "users",        // start table
            "user_id",      // start id column
            "follows",      // relationship table
            "follower_id",  // from column
            "followed_id",  // to column
            "users",        // end table
            "user_id",      // end id column
            "u1",           // start alias
            "u2",           // end alias
            vec![],         // no properties for test
            None,           // no shortest path mode
            None,           // no start node filters
            None,           // no end node filters
            None,           // no path variable
            None,           // no relationship types
        );

        let sql = generator.generate_recursive_sql();
        println!("Unbounded SQL:\n{}", sql);
        
        // Should contain recursive case
        assert!(sql.contains("UNION ALL"));
        assert!(sql.contains("hop_count < 10")); // Default max
    }    #[test]
    fn test_fixed_length_spec() {
        let spec = VariableLengthSpec::fixed(2);
        assert_eq!(spec.effective_min_hops(), 2);
        assert_eq!(spec.max_hops, Some(2));
        assert!(!spec.is_single_hop());
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
        
        Cte {
            cte_name,
            content: crate::render_plan::CteContent::RawSql(cte_sql),
            is_recursive: false, // Chained JOINs don't need recursion
        }
    }

    fn format_table_name(&self, table: &str) -> String {
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
                if hop == self.hop_count { &self.end_node_id_column } else { &self.start_node_id_column }
            ));
        }

        // Add WHERE clause for cycle prevention
        if self.hop_count > 1 {
            sql.push_str("WHERE ");
            let mut conditions = vec![];
            
            // Prevent start == end
            conditions.push(format!("s.{} != e.{}", self.start_node_id_column, self.end_node_id_column));
            
            // Prevent intermediate nodes from being start or end
            for hop in 1..self.hop_count {
                let mid_alias = format!("m{}", hop);
                conditions.push(format!("s.{} != {}.{}", self.start_node_id_column, mid_alias, self.start_node_id_column));
                conditions.push(format!("e.{} != {}.{}", self.end_node_id_column, mid_alias, self.start_node_id_column));
            }
            
            // Prevent intermediate nodes from repeating
            if self.hop_count > 2 {
                for i in 1..self.hop_count {
                    for j in (i+1)..self.hop_count {
                        conditions.push(format!("m{}.{} != m{}.{}", 
                            i, self.start_node_id_column, 
                            j, self.start_node_id_column));
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
