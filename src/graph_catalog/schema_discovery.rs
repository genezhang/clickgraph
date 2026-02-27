//! Schema discovery module for introspecting ClickHouse databases
//!
//! This module provides functionality to discover table structures in ClickHouse databases
//! and generate graph schema suggestions.

#[cfg(feature = "gliner")]
use crate::graph_catalog::schema_pattern::{
    try_create_nlp, SchemaNlp, TableSchemaInfo, ColumnInfo as NlpColumnInfo, TableSuggestion,
};
use clickhouse::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::io::AsyncBufReadExt;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMetadata {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
    pub is_primary_key: bool,
    pub is_in_order_by: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    pub name: String,
    pub columns: Vec<ColumnMetadata>,
    pub row_count: Option<u64>,
    pub sample: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Suggestion {
    pub table: String,
    #[serde(rename = "type")]
    pub suggestion_type: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntrospectResponse {
    pub database: String,
    pub tables: Vec<TableMetadata>,
    pub next_step: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHint {
    pub table: String,
    pub label: String,
    pub node_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeHint {
    pub table: String,
    #[serde(rename = "type")]
    pub edge_type: String,
    pub from_node: String,
    pub to_node: String,
    pub from_id: String,
    pub to_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FkEdgeHint {
    pub table: String,
    #[serde(rename = "type")]
    pub edge_type: String,
    pub from_node: String,
    pub to_node: String,
    pub from_id: String,
    pub to_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DraftRequest {
    pub database: String,
    pub schema_name: String,
    pub nodes: Vec<NodeHint>,
    pub edges: Vec<EdgeHint>,
    pub fk_edges: Vec<FkEdgeHint>,
    pub options: Option<DraftOptions>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DraftOptions {
    #[serde(rename = "auto_discover_columns")]
    pub auto_discover_columns: Option<bool>,
}

pub struct SchemaDiscovery;

impl SchemaDiscovery {
    /// Introspect a database and return table metadata
    pub async fn introspect(
        client: &Client,
        database: &str,
    ) -> Result<IntrospectResponse, String> {
        let tables = Self::list_tables(client, database).await?;
        
        #[cfg(feature = "gliner")]
        let nlp = SchemaNlp::new("").ok();
        #[cfg(not(feature = "gliner"))]
        let nlp: Option<crate::graph_catalog::schema_pattern::SchemaNlp> = None;
        
        let mut table_metadata = Vec::new();
        let mut suggestions = Vec::new();
        
        for table_name in tables {
            let columns = Self::get_columns(client, database, &table_name).await?;
            let row_count = Self::get_row_count(client, database, &table_name).await.ok();
            let sample = Self::get_sample_data(client, database, &table_name).await.unwrap_or_default();
            
            // Generate structural suggestions
            let table_suggestions = Self::generate_suggestions(&table_name, &columns);
            suggestions.extend(table_suggestions);
            
            // Add NLP-based classification and pattern detection
            #[cfg(feature = "gliner")]
            if let Some(ref nlp_model) = nlp {
                let column_info: Vec<(&str, bool)> = columns
                    .iter()
                    .map(|c| (c.name.as_str(), c.is_primary_key))
                    .collect();
                
                // Classification
                let nlp_suggestion = nlp_model.classify_table_with_columns(&table_name, &column_info);
                suggestions.push(Suggestion {
                    table: nlp_suggestion.table_name.clone(),
                    suggestion_type: format!("nlp_{}", nlp_suggestion.suggestion_type),
                    reason: nlp_suggestion.reason,
                });
                
                // Pattern detection
                let pattern = nlp_model.detect_schema_pattern(&table_name, &column_info);
                suggestions.push(Suggestion {
                    table: nlp_suggestion.table_name,
                    suggestion_type: format!("pattern_{}", pattern.pattern),
                    reason: pattern.details,
                });
            }
            
            table_metadata.push(TableMetadata {
                name: table_name,
                columns,
                row_count,
                sample,
            });
        }

        let help = format!(
            "Review tables and columns above, then create your schema.\n\
To generate YAML draft:\n\
curl -X POST http://localhost:8080/schemas/draft -H 'Content-Type: application/json' -d '{{\n\
  \"database\": \"{}\",\n\
  \"schema_name\": \"my_graph\",\n\
  \"nodes\": [{{\"table\": \"users\", \"label\": \"User\", \"node_id\": \"user_id\"}}],\n\
  \"edges\": [{{\"table\": \"follows\", \"type\": \"FOLLOWS\", \"from_node\": \"User\", \"to_node\": \"User\", \"from_id\": \"follower_id\", \"to_id\": \"followed_id\"}}]\n}}'",
            database
        );

        Ok(IntrospectResponse {
            database: database.to_string(),
            tables: table_metadata,
            next_step: help,
        })
    }

    /// Introspect a database with GLiNER-powered suggestions
    #[cfg(feature = "gliner")]
    pub async fn introspect_with_nlp(
        client: &Client,
        database: &str,
    ) -> Result<IntrospectResponse, String> {
        use crate::graph_catalog::schema_pattern::{TableSchemaInfo, ColumnInfo as NlpColumnInfo};
        
        let tables = Self::list_tables(client, database).await?;
        
        let mut table_metadata = Vec::new();
        let mut suggestions = Vec::new();
        
        // Try to create GLiNER model
        let nlp = try_create_nlp();
        
        for table_name in tables {
            let columns = Self::get_columns(client, database, &table_name).await?;
            let row_count = Self::get_row_count(client, database, &table_name).await.ok();
            let sample = Self::get_sample_data(client, database, &table_name).await.unwrap_or_default();
            
            // Generate basic suggestions (always available)
            let table_suggestions = Self::generate_suggestions(&table_name, &columns);
            suggestions.extend(table_suggestions);
            
            // If GLiNER is available, add NLP-powered suggestions
            if let Some(ref nlp_model) = nlp {
                let table_info = TableSchemaInfo {
                    name: table_name.clone(),
                    columns: columns.iter().map(|c| NlpColumnInfo {
                        name: c.name.clone(),
                        is_pk: c.is_primary_key,
                    }).collect(),
                };
                
                match nlp_model.suggest_from_tables(&[table_info]) {
                    Ok(nlp_suggestions) => {
                        for sugg in nlp_suggestions {
                            // Add NLP suggestion with confidence
                            let nlp_reason = format!(
                                "[NLP] {} (confidence: {:.2}) - {}",
                                sugg.suggestion_type, sugg.confidence, sugg.reason
                            );
                            suggestions.push(Suggestion {
                                table: sugg.table_name,
                                suggestion_type: format!("nlp_{}", sugg.suggestion_type),
                                reason: nlp_reason,
                            });
                        }
                    }
                    Err(e) => {
                        log::warn!("GLiNER suggestion failed for {}: {}", table_name, e);
                    }
                }
            }
            
            table_metadata.push(TableMetadata {
                name: table_name,
                columns,
                row_count,
                sample,
            });
        }

        // For now, skip all suggestions - unreliable. Just return tables.
        let help = format!(
            "Review tables and columns above, then create your schema.\n\
To generate YAML draft:\n\
curl -X POST http://localhost:8080/schemas/draft -H 'Content-Type: application/json' -d '{{\n\
  \"database\": \"{}\",\n\
  \"schema_name\": \"my_graph\",\n\
  \"nodes\": [{{\"table\": \"users\", \"label\": \"User\", \"node_id\": \"user_id\"}}],\n\
  \"edges\": [{{\"table\": \"follows\", \"type\": \"FOLLOWS\", \"from_node\": \"User\", \"to_node\": \"User\", \"from_id\": \"follower_id\", \"to_id\": \"followed_id\"}}]\n}}'",
            database
        );

        Ok(IntrospectResponse {
            database: database.to_string(),
            tables: table_metadata,
            next_step: help,
        })
    }
    
    /// Introspect a database with GLiNER-powered suggestions (stub when GLiNER not enabled)
    #[cfg(not(feature = "gliner"))]
    pub async fn introspect_with_nlp(
        _client: &Client,
        database: &str,
    ) -> Result<IntrospectResponse, String> {
        // When GLiNER is not enabled, just call regular introspect
        Self::introspect(_client, database).await
    }
    
    /// List all tables in a database
    
    /// List all tables in a database
    async fn list_tables(client: &Client, database: &str) -> Result<Vec<String>, String> {
        #[derive(Debug, clickhouse::Row, Deserialize)]
        struct TableName {
            name: String,
        }
        
        let query = format!(
            "SELECT name FROM system.tables WHERE database = '{}' AND engine NOT IN ('SystemTable', 'MaterializedView') ORDER BY name",
            database
        );
        
        let rows: Vec<TableName> = client
            .query(&query)
            .fetch_all()
            .await
            .map_err(|e| format!("Failed to list tables: {}", e))?;
        
        Ok(rows.into_iter().map(|t| t.name).collect())
    }
    
    /// Get columns for a table
    async fn get_columns(
        client: &Client,
        database: &str,
        table: &str,
    ) -> Result<Vec<ColumnMetadata>, String> {
        #[derive(Debug, clickhouse::Row, Deserialize)]
        struct ColumnRow {
            name: String,
            #[serde(rename = "type")]
            data_type: String,
            is_in_primary_key: u8,
            is_in_sorting_key: u8,
        }
        
        let query = format!(
            "SELECT name, type, is_in_primary_key, is_in_sorting_key FROM system.columns WHERE database = '{}' AND table = '{}' ORDER BY position",
            database, table
        );
        
        let rows: Vec<ColumnRow> = client
            .query(&query)
            .fetch_all()
            .await
            .map_err(|e| format!("Failed to get columns: {}", e))?;
        
        Ok(rows
            .into_iter()
            .map(|c| ColumnMetadata {
                name: c.name,
                data_type: c.data_type,
                is_primary_key: c.is_in_primary_key == 1,
                is_in_order_by: c.is_in_sorting_key == 1,
            })
            .collect())
    }
    
    /// Get row count for a table
    async fn get_row_count(client: &Client, database: &str, table: &str) -> Result<u64, String> {
        let query = format!("SELECT count() FROM {}.{}", database, table);
        let count: u64 = client
            .query(&query)
            .fetch_one()
            .await
            .map_err(|e| format!("Failed to get row count: {}", e))?;
        Ok(count)
    }
    
    /// Get sample data (3 rows) for a table
    /// Returns sample as array of column_name:value maps
    async fn get_sample_data(
        client: &Client,
        database: &str,
        table: &str,
    ) -> Result<Vec<serde_json::Value>, String> {
        // First get column names
        #[derive(Debug, Clone, serde::Deserialize, clickhouse::Row)]
        struct ColName {
            name: String,
        }
        
        let columns: Vec<String> = client
            .query(&format!(
                "SELECT name FROM system.columns WHERE database = '{}' AND table = '{}' ORDER BY position",
                database, table
            ))
            .fetch_all()
            .await
            .map_err(|e| format!("Failed to fetch columns: {}", e))?
            .iter()
            .map(|row: &ColName| row.name.clone())
            .collect();
        
        if columns.is_empty() {
            return Ok(vec![]);
        }
        
        // Query sample data using JSONEachRow format
        let query = format!(
            "SELECT {} FROM {}.{} LIMIT 3",
            columns.join(", "),
            database,
            table
        );
        
        // Use fetch_bytes for JSONEachRow format
        let mut lines = client
            .query(&query)
            .fetch_bytes("JSONEachRow")
            .map_err(|e| format!("Failed to fetch sample: {}", e))?
            .lines();
        
        let mut result = Vec::new();
        while let Some(line) = lines.next_line().await.map_err(|e| format!("Row error: {}", e))? {
            match serde_json::de::from_str::<serde_json::Value>(&line) {
                Ok(value) => result.push(value),
                Err(_) => {}
            }
        }
        
        Ok(result)
    }
    
    /// Generate suggestions based on table structure
    fn generate_suggestions(table_name: &str, columns: &[ColumnMetadata]) -> Vec<Suggestion> {
        let mut suggestions = Vec::new();
        
        // Check for primary key
        let pk_columns: Vec<_> = columns.iter().filter(|c| c.is_primary_key).collect();
        if !pk_columns.is_empty() {
            let pk_names: Vec<_> = pk_columns.iter().map(|c| c.name.as_str()).collect();
            suggestions.push(Suggestion {
                table: table_name.to_string(),
                suggestion_type: "node_candidate".to_string(),
                reason: format!("has primary key: {}", pk_names.join(", ")),
            });
        }
        
        // Check for ID columns (potential FKs)
        let id_columns: Vec<_> = columns
            .iter()
            .filter(|c| {
                let name_lower = c.name.to_lowercase();
                name_lower.ends_with("_id") || name_lower.ends_with("_key")
            })
            .collect();
        
        if id_columns.len() == 1 && pk_columns.is_empty() {
            // Single ID column - could be FK-edge
            let col = id_columns[0];
            let base_name = col.name.trim_end_matches("_id").trim_end_matches("_key");
            suggestions.push(Suggestion {
                table: table_name.to_string(),
                suggestion_type: "fk_edge_candidate".to_string(),
                reason: format!("column {} may reference {} table", col.name, base_name),
            });
        } else if id_columns.len() == 2 {
            // Two ID columns - likely edge table
            suggestions.push(Suggestion {
                table: table_name.to_string(),
                suggestion_type: "edge_candidate".to_string(),
                reason: format!("has two id columns: {} and {}", 
                    id_columns[0].name, id_columns[1].name),
            });
        } else if id_columns.len() > 2 {
            // Multiple ID columns - ambiguous
            suggestions.push(Suggestion {
                table: table_name.to_string(),
                suggestion_type: "ambiguous".to_string(),
                reason: format!("has {} id-like columns - may need manual review", id_columns.len()),
            });
        }
        
        // Check for denormalized patterns (origin_*, dest_*, etc.)
        let has_origin = columns.iter().any(|c| c.name.starts_with("origin_") || c.name.starts_with("src_"));
        let has_dest = columns.iter().any(|c| c.name.starts_with("dest_") || c.name.starts_with("dst_"));
        
        if has_origin && has_dest {
            suggestions.push(Suggestion {
                table: table_name.to_string(),
                suggestion_type: "denormalized_candidate".to_string(),
                reason: "has origin_* and dest_* columns - possible denormalized nodes".to_string(),
            });
        }
        
        // Check for polymorphic indicator (type column)
        let has_type = columns.iter().any(|c| {
            let name_lower = c.name.to_lowercase();
            name_lower.ends_with("_type") || name_lower == "type" || name_lower == "interaction_type"
        });
        
        if has_type && id_columns.len() >= 2 {
            suggestions.push(Suggestion {
                table: table_name.to_string(),
                suggestion_type: "polymorphic_candidate".to_string(),
                reason: "has type column - possible polymorphic edge table".to_string(),
            });
        }
        
        suggestions
    }
    
    /// Analyze sample data values to detect patterns (emails, URLs - rare but useful)
    fn analyze_sample_values(
        table_name: &str,
        columns: &[ColumnMetadata],
        sample_rows: &[serde_json::Value],
    ) -> Vec<Suggestion> {
        let mut suggestions = Vec::new();
        
        if sample_rows.is_empty() {
            return suggestions;
        }
        
        for col in columns {
            let values: Vec<&str> = sample_rows.iter()
                .filter_map(|row| row.get(&col.name).and_then(|v| v.as_str()))
                .collect();
            
            if values.is_empty() {
                continue;
            }
            
            let sample = values.first().copied().unwrap_or("");
            
            // Email pattern - rare but useful if found
            if sample.contains('@') && sample.contains('.') && !sample.contains(' ') {
                suggestions.push(Suggestion {
                    table: table_name.to_string(),
                    suggestion_type: "value_email".to_string(),
                    reason: format!("column '{}' contains email: {}", col.name, sample),
                });
            }
            
            // URL pattern - rare but useful if found
            if sample.starts_with("http://") || sample.starts_with("https://") {
                suggestions.push(Suggestion {
                    table: table_name.to_string(),
                    suggestion_type: "value_url".to_string(),
                    reason: format!("column '{}' contains URL: {}", col.name, sample),
                });
            }
            
            // UUID pattern - could indicate FK to UUID-based tables
            if sample.len() == 36 && sample.matches('-').count() == 4 {
                suggestions.push(Suggestion {
                    table: table_name.to_string(),
                    suggestion_type: "value_uuid".to_string(),
                    reason: format!("column '{}' contains UUID", col.name),
                });
            }
        }
        
        suggestions
    }
    
    /// Generate YAML draft from hints
    pub fn generate_draft(request: &DraftRequest) -> String {
        let auto_discover = request
            .options
            .as_ref()
            .and_then(|o| o.auto_discover_columns)
            .unwrap_or(true);
        
        let mut yaml = format!(
            "name: {}\nversion: \"1.0\"\ndescription: \"Graph schema for {} - TODO: review and edit labels/types\"\n\ngraph_schema:\n",
            request.schema_name, request.database
        );
        
        // Nodes
        yaml.push_str("  nodes:\n");
        for node in &request.nodes {
            yaml.push_str(&format!(
                "    - label: {}\n      database: {}\n      table: {}\n      node_id: {}\n",
                node.label, request.database, node.table, node.node_id
            ));
            if auto_discover {
                yaml.push_str("      auto_discover_columns: true\n");
            }
            yaml.push_str("\n");
        }
        
        // Regular edges
        if !request.edges.is_empty() {
            yaml.push_str("  edges:\n");
            for edge in &request.edges {
                yaml.push_str(&format!(
                    "    - type: {}\n      database: {}\n      table: {}\n      from_id: {}\n      to_id: {}\n      from_node: {}\n      to_node: {}\n\n",
                    edge.edge_type,
                    request.database,
                    edge.table,
                    edge.from_id,
                    edge.to_id,
                    edge.from_node,
                    edge.to_node
                ));
            }
        }
        
        // FK edges (edges that use a node table as the edge table)
        if !request.fk_edges.is_empty() {
            // If we don't have regular edges yet, add edges header
            if request.edges.is_empty() {
                yaml.push_str("  edges:\n");
            }
            for fk_edge in &request.fk_edges {
                // FK edge: the table is both source and edge
                // from_id is the node's PK, to_id is the FK
                yaml.push_str(&format!(
                    "    - type: {}\n      database: {}\n      table: {}\n      from_id: {}\n      to_id: {}\n      from_node: {}\n      to_node: {}\n      # Note: This is an FK-edge pattern - table serves as both node and edge\n\n",
                    fk_edge.edge_type,
                    request.database,
                    fk_edge.table,
                    fk_edge.from_id,
                    fk_edge.to_id,
                    fk_edge.from_node,
                    fk_edge.to_node
                ));
            }
        }
        
        yaml
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_generate_draft_simple() {
        let request = DraftRequest {
            database: "testdb".to_string(),
            schema_name: "testdb".to_string(),
            nodes: vec![NodeHint {
                table: "users".to_string(),
                label: "User".to_string(),
                node_id: "user_id".to_string(),
            }],
            edges: vec![],
            fk_edges: vec![FkEdgeHint {
                table: "orders".to_string(),
                edge_type: "PLACED_BY".to_string(),
                from_node: "Order".to_string(),
                to_node: "User".to_string(),
                from_id: "order_id".to_string(),
                to_id: "customer_id".to_string(),
            }],
            options: Some(DraftOptions {
                auto_discover_columns: Some(true),
            }),
        };
        
        let yaml = SchemaDiscovery::generate_draft(&request);
        assert!(yaml.contains("name: testdb"));
        assert!(yaml.contains("label: User"));
        assert!(yaml.contains("type: PLACED_BY"));
    }
}
