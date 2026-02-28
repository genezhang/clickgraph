//! LLM prompt formatting for schema discovery
//!
//! Formats ClickHouse introspection metadata into a prompt for an LLM
//! to generate a ClickGraph schema YAML.

use serde::{Deserialize, Serialize};

use super::schema_discovery::TableMetadata;

/// Maximum tables per prompt batch to stay within context limits
const MAX_TABLES_PER_BATCH: usize = 40;

/// Formatted prompt ready to send to an LLM
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryPrompt {
    pub system_prompt: String,
    pub user_prompt: String,
    pub table_count: usize,
    pub estimated_tokens: usize,
}

/// Response containing one or more prompts (batched for large schemas)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryPromptResponse {
    pub database: String,
    pub prompts: Vec<DiscoveryPrompt>,
    pub total_tables: usize,
}

const SYSTEM_PROMPT: &str = r#"You are a database schema analyst for ClickGraph, a graph query engine for ClickHouse.

Given ClickHouse table metadata, generate a graph schema YAML that maps these tables to graph nodes and edges.

## ClickGraph Schema Format

```yaml
name: schema_name
version: "1.0"
description: "Graph schema for database_name"

graph_schema:
  nodes:
    - label: User
      database: mydb
      table: users
      node_id: user_id
      property_mappings:
        full_name: name       # clickhouse_column: cypher_property
        email_addr: email

  edges:
    # Pure edge table (junction/association)
    - type: FOLLOWS
      database: mydb
      table: user_follows
      from_id: follower_id
      to_id: followed_id
      from_node: User
      to_node: User

    # FK-edge: node table also serves as edge source
    - type: IN_DEPARTMENT
      database: mydb
      table: users            # same table as User node
      from_id: user_id        # node's own PK
      to_id: dept_id          # FK column
      from_node: User
      to_node: Department
```

## Key Rules
1. A table can be BOTH a node AND a source of FK-edge relationships (e.g., a tickets table is a Ticket node, but also has edges to User via reporter/assignee columns)
2. Junction/association tables with composite PKs are usually pure edge tables
3. FK-edges: when a node table has a column referencing another table's PK, create an edge entry using the SAME table
4. Use meaningful relationship type names (REPORTED_BY, ASSIGNED_TO, not HAS_REPORTER)
5. Self-referential FKs create self-edges (e.g., manager → same user table)
6. property_mappings maps ClickHouse column names to clean Cypher property names — omit columns used as IDs/FKs
7. Polymorphic references (object_type + object_id) should be noted but may need multiple edge entries with filters
8. Omit internal/audit columns from property_mappings unless they carry domain meaning
9. Tables with no PK and only event data (audit logs, event streams) are typically not modeled as graph entities — skip them or add a comment

Return ONLY the YAML, no explanation."#;

/// Format introspection data into LLM prompt(s) for schema discovery.
pub fn format_discovery_prompt(
    database: &str,
    tables: &[TableMetadata],
) -> DiscoveryPromptResponse {
    if tables.len() <= MAX_TABLES_PER_BATCH {
        let prompt = build_single_prompt(database, tables, None);
        DiscoveryPromptResponse {
            database: database.to_string(),
            prompts: vec![prompt],
            total_tables: tables.len(),
        }
    } else {
        let mut prompts = Vec::new();
        let chunks: Vec<&[TableMetadata]> = tables.chunks(MAX_TABLES_PER_BATCH).collect();
        let total_batches = chunks.len();

        for (i, chunk) in chunks.iter().enumerate() {
            let batch_header = if total_batches > 1 {
                let continuation = if i > 0 {
                    "\nIMPORTANT: This is a continuation batch. Return ONLY the nodes and edges \
                     arrays (no name/version/description/graph_schema wrapper). Example:\n\
                     nodes:\n  - label: Foo\n    ...\nedges:\n  - type: BAR\n    ...\n"
                } else {
                    ""
                };
                Some(format!(
                    "Batch {}/{} — {} tables in this batch, {} total in database.\n\
                     Cross-reference: tables from other batches may be referenced as FK targets.\n\
                     Use the table names listed below; assume any unrecognized FK target \
                     is defined in another batch.{}",
                    i + 1,
                    total_batches,
                    chunk.len(),
                    tables.len(),
                    continuation
                ))
            } else {
                None
            };
            prompts.push(build_single_prompt(
                database,
                chunk,
                batch_header.as_deref(),
            ));
        }

        DiscoveryPromptResponse {
            database: database.to_string(),
            prompts,
            total_tables: tables.len(),
        }
    }
}

fn build_single_prompt(
    database: &str,
    tables: &[TableMetadata],
    batch_header: Option<&str>,
) -> DiscoveryPrompt {
    let mut user_prompt = String::with_capacity(tables.len() * 500);

    if let Some(header) = batch_header {
        user_prompt.push_str(header);
        user_prompt.push_str("\n\n");
    }

    user_prompt.push_str(&format!(
        "## ClickHouse Tables (database: {})\n\n",
        database
    ));

    for table in tables {
        user_prompt.push_str(&format!("### {}\n", table.name));

        // Columns
        user_prompt.push_str("Columns:\n");
        for col in &table.columns {
            let mut flags = Vec::new();
            if col.is_primary_key {
                flags.push("PK");
            }
            if col.is_in_order_by {
                flags.push("OrderBy");
            }
            let flag_str = if flags.is_empty() {
                String::new()
            } else {
                format!(" [{}]", flags.join(", "))
            };
            user_prompt.push_str(&format!("  - {} {}{}\n", col.name, col.data_type, flag_str));
        }

        // Row count
        if let Some(count) = table.row_count {
            user_prompt.push_str(&format!("Rows: {}\n", count));
        }

        // Sample data
        if !table.sample.is_empty() {
            user_prompt.push_str("Sample (3 rows):\n");
            for row in &table.sample {
                user_prompt.push_str(&format!("  {}\n", row));
            }
        }

        user_prompt.push('\n');
    }

    user_prompt.push_str(&format!(
        "## Task\nGenerate the complete graph_schema YAML for database \"{}\". Include:\n\
         1. All node definitions with property_mappings (use clean property names)\n\
         2. All edge definitions (both junction table edges and FK-edges)\n\
         3. Comments explaining non-obvious mappings\n\n\
         Return ONLY the YAML, no explanation.",
        database
    ));

    // Rough token estimation: ~4 chars per token for English text
    let estimated_tokens = (SYSTEM_PROMPT.len() + user_prompt.len()) / 4;

    DiscoveryPrompt {
        system_prompt: SYSTEM_PROMPT.to_string(),
        user_prompt,
        table_count: tables.len(),
        estimated_tokens,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_catalog::schema_discovery::ColumnMetadata;

    fn make_table(name: &str, cols: &[(&str, &str, bool)]) -> TableMetadata {
        TableMetadata {
            name: name.to_string(),
            columns: cols
                .iter()
                .map(|(n, t, pk)| ColumnMetadata {
                    name: n.to_string(),
                    data_type: t.to_string(),
                    is_primary_key: *pk,
                    is_in_order_by: false,
                })
                .collect(),
            row_count: Some(100),
            sample: vec![],
        }
    }

    #[test]
    fn test_single_prompt_basic() {
        let tables = vec![
            make_table(
                "users",
                &[("id", "UInt64", true), ("name", "String", false)],
            ),
            make_table(
                "follows",
                &[
                    ("follower_id", "UInt64", true),
                    ("followed_id", "UInt64", true),
                ],
            ),
        ];

        let result = format_discovery_prompt("testdb", &tables);
        assert_eq!(result.database, "testdb");
        assert_eq!(result.prompts.len(), 1);
        assert_eq!(result.total_tables, 2);

        let prompt = &result.prompts[0];
        assert_eq!(prompt.table_count, 2);
        assert!(prompt.system_prompt.contains("ClickGraph"));
        assert!(prompt.user_prompt.contains("### users"));
        assert!(prompt.user_prompt.contains("### follows"));
        assert!(prompt.user_prompt.contains("[PK]"));
        assert!(prompt.estimated_tokens > 0);
    }

    #[test]
    fn test_batching_large_schema() {
        let tables: Vec<TableMetadata> = (0..50)
            .map(|i| make_table(&format!("table_{}", i), &[("id", "UInt64", true)]))
            .collect();

        let result = format_discovery_prompt("bigdb", &tables);
        assert_eq!(result.total_tables, 50);
        assert_eq!(result.prompts.len(), 2);
        assert_eq!(result.prompts[0].table_count, 40);
        assert_eq!(result.prompts[1].table_count, 10);
        assert!(result.prompts[0].user_prompt.contains("Batch 1/2"));
        assert!(result.prompts[1].user_prompt.contains("Batch 2/2"));
    }

    #[test]
    fn test_column_flags() {
        let tables = vec![TableMetadata {
            name: "t".to_string(),
            columns: vec![ColumnMetadata {
                name: "ts".to_string(),
                data_type: "DateTime".to_string(),
                is_primary_key: false,
                is_in_order_by: true,
            }],
            row_count: None,
            sample: vec![],
        }];

        let result = format_discovery_prompt("db", &tables);
        assert!(result.prompts[0].user_prompt.contains("[OrderBy]"));
    }

    #[test]
    fn test_sample_data_included() {
        let tables = vec![TableMetadata {
            name: "users".to_string(),
            columns: vec![],
            row_count: Some(42),
            sample: vec![serde_json::json!({"id": 1, "name": "Alice"})],
        }];

        let result = format_discovery_prompt("db", &tables);
        let prompt = &result.prompts[0].user_prompt;
        assert!(prompt.contains("Rows: 42"));
        assert!(prompt.contains("Sample"));
        assert!(prompt.contains("Alice"));
    }

    #[test]
    fn test_no_batching_at_boundary() {
        let tables: Vec<TableMetadata> = (0..40)
            .map(|i| make_table(&format!("t{}", i), &[("id", "UInt64", true)]))
            .collect();

        let result = format_discovery_prompt("db", &tables);
        assert_eq!(result.prompts.len(), 1);
    }
}
