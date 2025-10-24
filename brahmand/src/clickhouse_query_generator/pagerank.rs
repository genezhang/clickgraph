//! PageRank algorithm implementation for ClickGraph
//!
//! This module implements the PageRank algorithm using ClickHouse SQL.
//! PageRank assigns importance scores to nodes based on their connectivity.
//!
//! # Algorithm Overview
//!
//! PageRank computes iterative importance scores using:
//! PR(A) = (1-d) + d * Σ(PR(Ti)/C(Ti)) for all pages Ti linking to A
//!
//! Where:
//! - d = damping factor (typically 0.85)
//! - C(Ti) = out-degree of node Ti
//! - (1-d) = random jump probability
//!
//! # SQL Implementation
//!
//! Uses recursive CTE with iteration count for convergence:
//!
//! ```sql
//! WITH RECURSIVE pagerank_iterations AS (
//!     -- Initial PageRank: 1/N for each node
//!     SELECT node_id, 1.0 / total_nodes AS pagerank, 0 AS iteration
//!     FROM nodes, (SELECT count(*) AS total_nodes FROM nodes) AS totals
//!
//!     UNION ALL
//!
//!     -- Iterative PageRank computation
//!     SELECT
//!         target_node,
//!         (1 - 0.85) + 0.85 * sum(source_pr / source_out_degree) AS pagerank,
//!         iteration + 1
//!     FROM (
//!         -- Join nodes with their incoming relationships and source PageRank
//!         SELECT
//!             r.to_node_id AS target_node,
//!             r.from_node_id AS source_node,
//!             pr.pagerank AS source_pr,
//!             out_degrees.out_degree AS source_out_degree
//!         FROM relationships r
//!         JOIN pagerank_iterations pr ON pr.node_id = r.from_node_id
//!         JOIN node_out_degrees out_degrees ON out_degrees.node_id = r.from_node_id
//!         WHERE pr.iteration = (SELECT MAX(iteration) FROM pagerank_iterations)
//!     ) contributions
//!     GROUP BY target_node
//! )
//!
//! SELECT node_id, pagerank
//! FROM pagerank_iterations
//! WHERE iteration = (SELECT MAX(iteration) FROM pagerank_iterations)
//! ```

use crate::graph_catalog::graph_schema::{GraphSchema, NodeViewMapping, RelationshipViewMapping};
use crate::clickhouse_query_generator::errors::ClickhouseQueryGeneratorError;

/// Configuration for PageRank computation
#[derive(Debug, Clone)]
pub struct PageRankConfig {
    /// Number of iterations to run
    pub iterations: usize,
    /// Damping factor (typically 0.85)
    pub damping_factor: f64,
    /// Convergence threshold (optional)
    pub convergence_threshold: Option<f64>,
}

impl Default for PageRankConfig {
    fn default() -> Self {
        Self {
            iterations: 10,
            damping_factor: 0.85,
            convergence_threshold: None,
        }
    }
}

/// Generates PageRank SQL for a given graph schema
pub struct PageRankGenerator<'a> {
    schema: &'a GraphSchema,
    config: PageRankConfig,
}

impl<'a> PageRankGenerator<'a> {
    pub fn new(schema: &'a GraphSchema, config: PageRankConfig) -> Self {
        Self { schema, config }
    }

    /// Generate the complete PageRank SQL query
    pub fn generate_pagerank_sql(&self) -> Result<String, ClickhouseQueryGeneratorError> {
        let (node_table, id_column) = self.get_node_info()?;
        let relationship_tables = self.get_relationship_tables()?;
        let iterations_sql = self.generate_iterations_sql(&node_table, &id_column)?;

        let sql = format!(
            r#"
WITH RECURSIVE
-- All relationships union
all_relationships AS (
    {}
),

-- Calculate out-degrees for all nodes
node_out_degrees AS (
    SELECT
        from_node_id AS node_id,
        count(*) AS out_degree
    FROM all_relationships
    GROUP BY from_node_id
),

-- Initial PageRank values
initial_pagerank AS (
    SELECT
        {} AS node_id,
        1.0 / (SELECT count(*) FROM {}) AS pagerank,
        0 AS iteration
    FROM {}
),

-- Iterative PageRank computation (non-recursive approach)
pagerank_iterations AS (
    -- Iteration 0: initial values
    SELECT node_id, pagerank, iteration
    FROM initial_pagerank

    UNION ALL

    -- Generate iterations 1 to N
    {}
)

-- Final result: PageRank values from last iteration
SELECT
    node_id,
    pagerank,
    iteration
FROM pagerank_iterations
WHERE iteration = (SELECT MAX(iteration) FROM pagerank_iterations)
ORDER BY pagerank DESC
"#,
            self.generate_union_relationships_sql(&relationship_tables),
            id_column,
            node_table,
            node_table,
            iterations_sql,
        );

        Ok(sql)
    }

    /// Generate iterative PageRank calculations for each iteration
    fn generate_iterations_sql(&self, node_table: &str, id_column: &str) -> Result<String, ClickhouseQueryGeneratorError> {
        let mut iterations = Vec::new();

        for i in 1..=self.config.iterations {
            let iteration_sql = format!(
                r#"    -- Iteration {}
    SELECT
        r.to_node_id AS node_id,
        (1 - {}) + {} * sum(pr.pagerank / coalesce(nod.out_degree, 1)) AS pagerank,
        {} AS iteration
    FROM all_relationships r
    JOIN pagerank_iterations pr ON pr.node_id = r.from_node_id AND pr.iteration = {}
    LEFT JOIN node_out_degrees nod ON nod.node_id = r.from_node_id
    GROUP BY r.to_node_id"#,
                i,
                self.config.damping_factor,
                self.config.damping_factor,
                i,
                i - 1
            );
            iterations.push(iteration_sql);
        }

        Ok(iterations.join("\n\n    UNION ALL\n\n"))
    }

    /// Get the primary node table and ID column from schema
    fn get_node_info(&self) -> Result<(String, String), ClickhouseQueryGeneratorError> {
        // For simplicity, assume we want User nodes
        // In practice, this would be determined from the query
        let user_nodes = self.schema.get_nodes_schemas()
            .get("User")
            .ok_or_else(|| ClickhouseQueryGeneratorError::SchemaError(
                "No User node type found in schema".to_string()
            ))?;

        Ok((user_nodes.table_name.clone(), user_nodes.node_id.column.clone()))
    }

    /// Get all relationship tables that connect nodes
    fn get_relationship_tables(&self) -> Result<Vec<String>, ClickhouseQueryGeneratorError> {
        let mut tables = Vec::new();

        for (_type_name, rel_schema) in self.schema.get_relationships_schemas() {
            tables.push(rel_schema.table_name.clone());
        }

        if tables.is_empty() {
            return Err(ClickhouseQueryGeneratorError::SchemaError(
                "No relationship tables found in schema".to_string()
            ));
        }

        Ok(tables)
    }

    /// Generate UNION ALL SQL for all relationship tables
    fn generate_union_relationships_sql(&self, tables: &[String]) -> String {
        tables.iter()
            .map(|table| {
                // Find the relationship schema for this table
                let rel_schema = self.schema.get_relationships_schemas()
                    .values()
                    .find(|schema| schema.table_name == *table)
                    .ok_or_else(|| ClickhouseQueryGeneratorError::SchemaError(
                        format!("No relationship schema found for table: {}", table)
                    ));

                match rel_schema {
                    Ok(schema) => format!(
                        "SELECT {} AS from_node_id, {} AS to_node_id FROM {}",
                        schema.from_column, schema.to_column, table
                    ),
                    Err(_) => format!(
                        "SELECT from_node_id, to_node_id FROM {} -- Error: schema not found",
                        table
                    )
                }
            })
            .collect::<Vec<_>>()
            .join("\n        UNION ALL\n        ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pagerank_config_defaults() {
        let config = PageRankConfig::default();
        assert_eq!(config.iterations, 10);
        assert_eq!(config.damping_factor, 0.85);
        assert!(config.convergence_threshold.is_none());
    }

    #[test]
    fn test_pagerank_sql_generation() {
        // This would need a mock schema to test fully
        // For now, just test that the config works
        let config = PageRankConfig {
            iterations: 5,
            damping_factor: 0.8,
            convergence_threshold: Some(0.001),
        };
        assert_eq!(config.iterations, 5);
        assert_eq!(config.damping_factor, 0.8);
    }
}