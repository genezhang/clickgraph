//! GLiNER-powered NLP suggestions for schema discovery
//!
//! This module provides intelligent suggestions for graph schema design using
//! zero-shot Named Entity Recognition (NER) from the GLiNER model.

use serde::{Deserialize, Serialize};

/// Entity types for schema discovery
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SchemaEntity {
    NodeTable,
    EdgeTable,
    ForeignKey,
    PrimaryKey,
    DenormalizedPattern,
    PolymorphicIndicator,
    Unknown,
}

impl SchemaEntity {
    pub fn as_str(&self) -> &'static str {
        match self {
            SchemaEntity::NodeTable => "NODE_TABLE",
            SchemaEntity::EdgeTable => "EDGE_TABLE",
            SchemaEntity::ForeignKey => "FOREIGN_KEY",
            SchemaEntity::PrimaryKey => "PRIMARY_KEY",
            SchemaEntity::DenormalizedPattern => "DENORMALIZED_PATTERN",
            SchemaEntity::PolymorphicIndicator => "POLYMORPHIC_INDICATOR",
            SchemaEntity::Unknown => "UNKNOWN",
        }
    }
}

/// Result from GLiNER NER prediction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NlpPrediction {
    pub text: String,
    pub entity: SchemaEntity,
    pub confidence: f32,
}

/// Information about a table's columns
#[derive(Debug, Clone)]
pub struct TableSchemaInfo {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
}

/// Information about a column
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub is_pk: bool,
}

/// Suggestion for a single table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSuggestion {
    pub table_name: String,
    #[serde(rename = "type")]
    pub suggestion_type: String,
    pub confidence: f32,
    pub reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nlp_scores: Option<NlpScores>,
}

/// NLP scores from GLiNER
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NlpScores {
    pub node: f32,
    pub edge: f32,
    pub foreign_key: f32,
    pub primary_key: f32,
    pub denormalized: f32,
    pub polymorphic: f32,
}

/// GLiNER model wrapper - uses simple heuristics when model not available
pub struct SchemaNlp;

impl SchemaNlp {
    /// Create a new SchemaNlp instance (placeholder)
    pub fn new(_model_dir: &str) -> Result<Self, String> {
        Ok(Self)
    }

    /// Generate suggestions based on table structure (simple heuristic fallback)
    pub fn suggest_from_tables(
        &self,
        tables: &[TableSchemaInfo],
    ) -> Result<Vec<TableSuggestion>, String> {
        let mut suggestions = Vec::new();

        for table in tables {
            let id_columns: Vec<_> = table
                .columns
                .iter()
                .filter(|c| {
                    c.name.to_lowercase().ends_with("_id")
                        || c.name.to_lowercase().ends_with("_key")
                })
                .collect();

            let pk_columns: Vec<_> = table.columns.iter().filter(|c| c.is_pk).collect();

            let has_origin = table
                .columns
                .iter()
                .any(|c| c.name.starts_with("origin_") || c.name.starts_with("src_"));
            let has_dest = table
                .columns
                .iter()
                .any(|c| c.name.starts_with("dest_") || c.name.starts_with("dst_"));

            let has_type = table.columns.iter().any(|c| {
                let n = c.name.to_lowercase();
                n.ends_with("_type") || n == "type" || n == "interaction_type"
            });

            // Determine best classification
            let (suggestion_type, confidence, reason) = if !pk_columns.is_empty() {
                let pk_names: Vec<_> = pk_columns.iter().map(|c| c.name.as_str()).collect();
                (
                    "node_candidate".to_string(),
                    0.7,
                    format!("primary key detected: {}", pk_names.join(", ")),
                )
            } else if id_columns.len() == 2 {
                (
                    "edge_candidate".to_string(),
                    0.6,
                    "edge table pattern detected".to_string(),
                )
            } else if has_origin && has_dest {
                (
                    "denormalized_candidate".to_string(),
                    0.5,
                    "denormalized column pattern detected".to_string(),
                )
            } else if has_type && id_columns.len() >= 2 {
                (
                    "polymorphic_candidate".to_string(),
                    0.5,
                    "polymorphic indicator detected".to_string(),
                )
            } else if id_columns.len() == 1 {
                (
                    "fk_edge_candidate".to_string(),
                    0.4,
                    "foreign key columns detected".to_string(),
                )
            } else {
                (
                    "node_candidate".to_string(),
                    0.3,
                    "default classification - needs review".to_string(),
                )
            };

            suggestions.push(TableSuggestion {
                table_name: table.name.clone(),
                suggestion_type,
                confidence,
                reason,
                nlp_scores: None,
            });
        }

        Ok(suggestions)
    }
}

/// Try to load GLiNER model if available
/// Note: Full GLiNER integration requires the model to be loaded at runtime
/// This is a placeholder that uses heuristic-based suggestions
pub fn try_create_nlp() -> Option<SchemaNlp> {
    // For now, return a heuristic-based schema nlp
    // Full GLiNER integration would require async model loading
    log::info!("Using heuristic-based schema suggestions");
    Some(SchemaNlp)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_entity_conversion() {
        use super::SchemaEntity;
        assert_eq!(SchemaEntity::NodeTable.as_str(), "NODE_TABLE");
        assert_eq!(SchemaEntity::ForeignKey.as_str(), "FOREIGN_KEY");
    }
}
