use crate::query_planner::logical_plan::VariableLengthSpec;
use crate::render_plan::Cte;

/// Generates recursive CTE SQL for variable-length path traversal
pub struct VariableLengthCteGenerator {
    pub spec: VariableLengthSpec,
    pub cte_name: String,
    pub start_node_table: String,
    pub start_node_alias: String, 
    pub relationship_table: String,
    pub relationship_alias: String,
    pub end_node_table: String,
    pub end_node_alias: String,
}

impl VariableLengthCteGenerator {
    pub fn new(
        spec: VariableLengthSpec,
        start_node: &str,
        relationship: &str, 
        end_node: &str,
    ) -> Self {
        Self {
            spec,
            cte_name: format!("variable_path_{}", uuid::Uuid::new_v4().simple()),
            start_node_table: start_node.to_string(),
            start_node_alias: "start_node".to_string(),
            relationship_table: relationship.to_string(), 
            relationship_alias: "rel".to_string(),
            end_node_table: end_node.to_string(),
            end_node_alias: "end_node".to_string(),
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
            // Direct single-hop connection
            format!(
                "    SELECT \n        {start}.{start}_id as start_id,\n        {start}.name as start_name,\n        {end}.{end}_id as end_id,\n        {end}.title as end_name,\n        1 as hop_count,\n        [{start}.{start}_id] as path_nodes\n    FROM {start_table} {start}\n    JOIN {rel_table} {rel} ON {start}.{start}_id = {rel}.from_{start}\n    JOIN {end_table} {end} ON {rel}.to_{end} = {end}.{end}_id",
                start = self.start_node_alias,
                end = self.end_node_alias,
                rel = self.relationship_alias,
                start_table = self.start_node_table,
                rel_table = self.relationship_table,
                end_table = self.end_node_table
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
            "    -- Multi-hop base case for {} hops (simplified)\n    SELECT NULL as start_id, NULL as start_name, NULL as end_id, NULL as end_name, {} as hop_count, [] as path_nodes\n    WHERE false  -- Placeholder",
            hop_count, hop_count
        )
    }

    /// Generate recursive case that extends existing paths
    fn generate_recursive_case(&self, max_hops: u32) -> String {
        format!(
            "    SELECT\n        vp.start_id,\n        vp.start_name,\n        {end}.{end}_id as end_id,\n        {end}.title as end_name,\n        vp.hop_count + 1 as hop_count,\n        arrayConcat(vp.path_nodes, [{current}.{current}_id]) as path_nodes\n    FROM {cte_name} vp\n    JOIN {current_table} {current} ON vp.end_id = {current}.{current}_id\n    JOIN {rel_table} {rel} ON {current}.{current}_id = {rel}.from_{current}\n    JOIN {end_table} {end} ON {rel}.to_{end} = {end}.{end}_id\n    WHERE vp.hop_count < {max_hops}\n      AND NOT has(vp.path_nodes, {current}.{current}_id)  -- Cycle detection",
            end = self.end_node_alias,
            current = "current_node",
            cte_name = self.cte_name,
            current_table = self.start_node_table, // In practice, this could be different
            rel_table = self.relationship_table,
            end_table = self.end_node_table,
            rel = self.relationship_alias,
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
            "user",
            "AUTHORED", 
            "post"
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
            "user",
            "FOLLOWS",
            "user"
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