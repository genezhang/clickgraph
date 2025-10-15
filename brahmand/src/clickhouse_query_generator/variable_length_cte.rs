use crate::query_planner::logical_plan::VariableLengthSpec;
use crate::render_plan::Cte;

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
    pub database: Option<String>,        // Optional database prefix
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
            database,
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

        let mut sql = String::new();

        // CTE Header
        sql.push_str(&format!("{} AS (\n", self.cte_name));

        // Base case: Generate for each required hop level from 1 to min_hops
        for hop in 1..=min_hops {
            if hop > 1 {
                sql.push_str("\n    UNION ALL\n");
            }
            sql.push_str(&self.generate_base_case(hop));
        }

        // Recursive case: If max_hops > min_hops, add recursive traversal
        if let Some(max) = max_hops {
            if max > min_hops {
                sql.push_str("\n    UNION ALL\n");
                sql.push_str(&self.generate_recursive_case(max));
            }
        } else {
            // Unbounded case: add recursive traversal with reasonable default limit
            sql.push_str("\n    UNION ALL\n");
            sql.push_str(&self.generate_recursive_case(10)); // Default max depth
        }

        sql.push_str("\n)");
        sql
    }

    /// Generate base case for a specific hop count
    fn generate_base_case(&self, hop_count: u32) -> String {
        if hop_count == 1 {
            // Direct single-hop connection using actual column names
            // TODO: Make property selection dynamic based on query needs
            format!(
                "    SELECT \n        {start}.{start_id_col} as start_id,\n        {end}.{end_id_col} as end_id,\n        1 as hop_count,\n        [{start}.{start_id_col}] as path_nodes\n    FROM {start_table} {start}\n    JOIN {rel_table} {rel} ON {start}.{start_id_col} = {rel}.{from_col}\n    JOIN {end_table} {end} ON {rel}.{to_col} = {end}.{end_id_col}",
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
            )
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
            "    -- Multi-hop base case for {} hops (simplified)\n    SELECT NULL as start_id, NULL as end_id, {} as hop_count, [] as path_nodes\n    WHERE false  -- Placeholder",
            hop_count, hop_count
        )
    }

    /// Generate recursive case that extends existing paths
    fn generate_recursive_case(&self, max_hops: u32) -> String {
        // TODO: Make property selection dynamic based on query needs
        format!(
            "    SELECT\n        vp.start_id,\n        {end}.{end_id_col} as end_id,\n        vp.hop_count + 1 as hop_count,\n        arrayConcat(vp.path_nodes, [{current}.{current_id_col}]) as path_nodes\n    FROM {cte_name} vp\n    JOIN {current_table} {current} ON vp.end_id = {current}.{current_id_col}\n    JOIN {rel_table} {rel} ON {current}.{current_id_col} = {rel}.{from_col}\n    JOIN {end_table} {end} ON {rel}.{to_col} = {end}.{end_id_col}\n    WHERE vp.hop_count < {max_hops}\n      AND NOT has(vp.path_nodes, {current}.{current_id_col})  -- Cycle detection",
            end = self.end_node_alias,
            end_id_col = self.end_node_id_column,
            current = "current_node",
            current_id_col = self.end_node_id_column, // Use end node's column since we're extending from last node
            cte_name = self.cte_name,
            current_table = self.format_table_name(&self.end_node_table), // For recursive, join with same type of table
            rel_table = self.format_table_name(&self.relationship_table),
            from_col = self.relationship_from_column,
            to_col = self.relationship_to_column,
            rel = self.relationship_alias,
            end_table = self.format_table_name(&self.end_node_table),
            max_hops = max_hops
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
            "p"            // end alias
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
            "u2"            // end alias
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
}