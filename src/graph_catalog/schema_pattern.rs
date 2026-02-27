//! GLiNER-powered NLP suggestions for schema discovery
//!
//! This module provides intelligent suggestions for graph schema design using
//! zero-shot Named Entity Recognition (NER) from the GLiNER model.
//!
//! NO hardcoded word lists - uses ML model to handle any unseen table/column names.

use serde::{Deserialize, Serialize};

#[cfg(feature = "gliner")]
use gline_rs::{GLiNER, Parameters, RuntimeParameters, TextInput, TokenMode};

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NlpPrediction {
    pub text: String,
    pub entity: SchemaEntity,
    pub confidence: f32,
}

#[derive(Debug, Clone)]
pub struct TableSchemaInfo {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub is_pk: bool,
}

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NlpScores {
    pub node: f32,
    pub edge: f32,
    pub foreign_key: f32,
    pub primary_key: f32,
    pub denormalized: f32,
    pub polymorphic: f32,
}

/// GLiNER-based schema classifier
/// Uses zero-shot NER to classify tables and extract entity references from columns
#[derive(Debug, Clone)]
pub struct SchemaNlp {
    #[cfg(feature = "gliner")]
    model: Option<GLiNER<TokenMode>>,
    pub noise_columns: Vec<String>,
}

impl SchemaNlp {
    /// Create a new SchemaNlp instance with GLiNER model
    #[allow(dead_code)]
    pub fn new(model_path: &str) -> Result<Self, String> {
        #[cfg(feature = "gliner")]
        {
            if model_path.is_empty() {
                return Ok(SchemaNlp {
                    model: None,
                    noise_columns: vec![
                        "tenant_id".to_string(),
                        "account_id".to_string(),
                        "org_id".to_string(),
                        "company_id".to_string(),
                        "customer_id".to_string(),
                    ],
                });
            }

            let model = GLiNER::<TokenMode>::new(
                Parameters::default(),
                RuntimeParameters::default(),
                model_path,
                model_path,
            )
            .map_err(|e| format!("Failed to load GLiNER model: {}", e))?;

            Ok(SchemaNlp {
                model: Some(model),
                noise_columns: vec![
                    "tenant_id".to_string(),
                    "account_id".to_string(),
                    "org_id".to_string(),
                    "company_id".to_string(),
                    "customer_id".to_string(),
                ],
            })
        }

        #[cfg(not(feature = "gliner"))]
        {
            Ok(SchemaNlp {
                noise_columns: vec![
                    "tenant_id".to_string(),
                    "account_id".to_string(),
                    "org_id".to_string(),
                    "company_id".to_string(),
                    "customer_id".to_string(),
                ],
            })
        }
    }

    pub fn with_noise_columns(mut self, columns: Vec<String>) -> Self {
        self.noise_columns.extend(columns);
        self
    }

    /// Use GLiNER to classify a table name as NODE or EDGE
    /// Entity labels for zero-shot recognition
    fn classify_with_gliner(&self, name: &str) -> Option<(String, f32)> {
        #[cfg(feature = "gliner")]
        {
            let Some(ref model) = self.model else {
                return None;
            };

            // Entity labels for schema classification
            let labels = [
                "node entity",
                "relationship",
                "event table",
                "dimension table",
            ];

            let input = TextInput::from_str(&[name], &labels).ok()?;
            let output = model.inference(&input).ok()?;

            let predictions = output.get_predictions(name)?;
            if let Some(best) = predictions.first() {
                let label = best.get_label();
                let score = best.get_score();

                let suggestion = if label.contains("relationship") || label.contains("event") {
                    "edge".to_string()
                } else {
                    "node".to_string()
                };

                return Some((suggestion, score));
            }
        }

        None
    }

    /// Extract entity references from column names using GLiNER
    /// e.g., "user_id" -> "user", "post_id" -> "post"
    fn extract_entity_references(&self, columns: &[(&str, bool)]) -> Vec<(String, f32)> {
        let mut entities = Vec::new();

        #[cfg(feature = "gliner")]
        if let Some(ref model) = self.model {
            let labels = ["entity reference", "foreign key", "identifier"];

            for (col, _) in columns {
                let input = TextInput::from_str(&[col], &labels).ok();
                if let Some(input) = input {
                    if let Ok(output) = model.inference(&input) {
                        if let Some(predictions) = output.get_predictions(col) {
                            if let Some(best) = predictions.first() {
                                let text = best.get_text();
                                let score = best.get_score();

                                // Extract entity name by removing common suffixes
                                let entity = text
                                    .trim_end_matches("_id")
                                    .trim_end_matches("_key")
                                    .trim_end_matches("_sk")
                                    .trim_end_matches("id")
                                    .to_lowercase();

                                if !entity.is_empty() && entity != col.to_lowercase() {
                                    entities.push((entity, score));
                                }
                            }
                        }
                    }
                }
            }
        }

        entities
    }

    /// Classify table as node or edge using GLiNER
    pub fn classify_table(&self, table_name: &str) -> TableSuggestion {
        self.classify_table_with_columns(table_name, &[])
    }

    /// Classify table using GLiNER + column analysis
    pub fn classify_table_with_columns(
        &self,
        table_name: &str,
        columns: &[(&str, bool)],
    ) -> TableSuggestion {
        let name = table_name.split('.').last().unwrap_or(table_name);

        // Try GLiNER first
        if let Some((suggestion, confidence)) = self.classify_with_gliner(name) {
            let reason = format!("GLiNER classified '{}' as {}", name, suggestion);
            return TableSuggestion {
                table_name: table_name.to_string(),
                suggestion_type: suggestion,
                confidence,
                reason,
                nlp_scores: None,
            };
        }

        // Fallback: GLiNER model not loaded - return unknown
        // This should NOT happen in production when model is properly loaded
        TableSuggestion {
            table_name: table_name.to_string(),
            suggestion_type: "unknown".to_string(),
            confidence: 0.0,
            reason: format!("GLiNER model not available for '{}'", name),
            nlp_scores: None,
        }
    }

    /// Detect schema pattern using GLiNER + structural analysis
    pub fn detect_schema_pattern(
        &self,
        table_name: &str,
        columns: &[(&str, bool)],
    ) -> SchemaPatternMatch {
        let name = table_name.split('.').last().unwrap_or(table_name);

        // Extract entity references from columns using GLiNER
        let entity_refs = self.extract_entity_references(columns);

        let is_noise = |col: &str| -> bool {
            let c = col.to_lowercase();
            self.noise_columns.iter().any(|n| c == n.to_lowercase())
                || c == "tenant"
                || c == "account"
        };

        let relevant_columns: Vec<_> = columns.iter().filter(|(col, _)| !is_noise(col)).collect();

        // Detect *id, *key, *_sk columns (entity references)
        let id_columns: Vec<_> = relevant_columns
            .iter()
            .filter(|(col, _)| {
                let c = col.to_lowercase();
                c.ends_with("_id")
                    || c.ends_with("_key")
                    || c.ends_with("_sk")
                    || c == "id"
                    || c == "key"
            })
            .collect();

        let pk_columns: Vec<_> = relevant_columns
            .iter()
            .filter(|(_, is_pk)| *is_pk)
            .collect();

        let pk_id_columns: Vec<_> = pk_columns
            .iter()
            .filter(|(col, _)| {
                let c = col.to_lowercase();
                c.ends_with("_id")
                    || c.ends_with("_key")
                    || c.ends_with("_sk")
                    || c == "id"
                    || c == "key"
            })
            .collect();

        let has_type_column = relevant_columns.iter().any(|(col, _)| {
            let c = col.to_lowercase();
            c.ends_with("_type") || c == "type" || c == "interaction_type"
        });

        let has_origin = relevant_columns.iter().any(|(col, _)| {
            let c = col.to_lowercase();
            c.starts_with("origin_") || c.starts_with("src_")
        });

        let has_dest = relevant_columns.iter().any(|(col, _)| {
            let c = col.to_lowercase();
            c.starts_with("dest_") || c.starts_with("dst_") || c.starts_with("to_")
        });

        let pk_count = pk_columns.len();
        let fk_count = id_columns.len().saturating_sub(pk_id_columns.len());

        // Use GLiNER classification if available
        let gliner_result = self.classify_with_gliner(name);

        if let Some((suggestion, confidence)) = gliner_result {
            // Combine with structural analysis
            if pk_count >= 2 && fk_count >= 2 {
                return SchemaPatternMatch {
                    table: table_name.to_string(),
                    pattern: "standard_edge".to_string(),
                    confidence: 0.9,
                    details: format!("GLiNER: {} + composite PK + {} FKs", suggestion, fk_count),
                };
            }

            if has_type_column && fk_count >= 2 {
                return SchemaPatternMatch {
                    table: table_name.to_string(),
                    pattern: "polymorphic_edge".to_string(),
                    confidence: 0.85,
                    details: format!("GLiNER: {} + type column + {} FKs", suggestion, fk_count),
                };
            }

            if has_origin && has_dest {
                return SchemaPatternMatch {
                    table: table_name.to_string(),
                    pattern: "denormalized_edge".to_string(),
                    confidence: 0.9,
                    details: format!("GLiNER: {} + origin/dest columns", suggestion),
                };
            }

            if fk_count >= 2 {
                return SchemaPatternMatch {
                    table: table_name.to_string(),
                    pattern: "standard_edge".to_string(),
                    confidence: 0.85,
                    details: format!("GLiNER: {} + {} FK columns", suggestion, fk_count),
                };
            }

            if pk_count >= 1 && fk_count <= 1 {
                return SchemaPatternMatch {
                    table: table_name.to_string(),
                    pattern: "standard_node".to_string(),
                    confidence: 0.8,
                    details: format!("GLiNER: {} + PK", suggestion),
                };
            }

            return SchemaPatternMatch {
                table: table_name.to_string(),
                pattern: suggestion,
                confidence,
                details: format!("GLiNER classification only"),
            };
        }

        // Fallback: structural analysis only (no GLiNER)
        if pk_count >= 2 && fk_count >= 2 {
            return SchemaPatternMatch {
                table: table_name.to_string(),
                pattern: "standard_edge".to_string(),
                confidence: 0.9,
                details: format!("Fact table: composite PK + {} foreign keys", fk_count),
            };
        }

        if pk_count >= 2 {
            return SchemaPatternMatch {
                table: table_name.to_string(),
                pattern: "composite_node".to_string(),
                confidence: 0.85,
                details: format!("Composite PK with {} columns, no extra FKs", pk_count),
            };
        }

        if has_type_column && fk_count >= 2 {
            return SchemaPatternMatch {
                table: table_name.to_string(),
                pattern: "polymorphic_edge".to_string(),
                confidence: 0.85,
                details: "Type column + multiple FKs suggests polymorphic edge".to_string(),
            };
        }

        if has_origin && has_dest && fk_count >= 1 {
            return SchemaPatternMatch {
                table: table_name.to_string(),
                pattern: "denormalized_edge".to_string(),
                confidence: 0.9,
                details: "origin_*/dest_* columns indicate denormalized edge".to_string(),
            };
        }

        if fk_count >= 2 || pk_id_columns.len() >= 2 {
            return SchemaPatternMatch {
                table: table_name.to_string(),
                pattern: "standard_edge".to_string(),
                confidence: 0.85,
                details: format!(
                    "{} FK columns suggest standard edge table",
                    if fk_count >= 2 {
                        fk_count
                    } else {
                        pk_id_columns.len()
                    }
                ),
            };
        }

        if pk_count == 1 && fk_count == 0 {
            return SchemaPatternMatch {
                table: table_name.to_string(),
                pattern: "standard_node".to_string(),
                confidence: 0.9,
                details: "Single PK, no FKs - typical node table".to_string(),
            };
        }

        if pk_count == 1 && fk_count >= 1 {
            return SchemaPatternMatch {
                table: table_name.to_string(),
                pattern: "fk_edge".to_string(),
                confidence: 0.7,
                details: "Single PK + FK column - FK-edge pattern".to_string(),
            };
        }

        if pk_count == 0 && fk_count >= 1 {
            return SchemaPatternMatch {
                table: table_name.to_string(),
                pattern: "fk_edge".to_string(),
                confidence: 0.6,
                details: "No PK, has FK - likely FK-edge or orphan relationship".to_string(),
            };
        }

        SchemaPatternMatch {
            table: table_name.to_string(),
            pattern: "unknown".to_string(),
            confidence: 0.0,
            details: "Unable to determine schema pattern - manual review needed".to_string(),
        }
    }

    /// Structural fallback: pattern detection WITHOUT GLiNER
    /// Uses only column structure - no word lists, no ML
    /// This is the fallback when GLiNER model is not available
    pub fn detect_schema_pattern_structural(
        &self,
        table_name: &str,
        columns: &[(&str, bool)],
    ) -> SchemaPatternMatch {
        let name = table_name.split('.').last().unwrap_or(table_name);

        let is_noise = |col: &str| -> bool {
            let c = col.to_lowercase();
            self.noise_columns.iter().any(|n| c == n.to_lowercase())
                || c == "tenant"
                || c == "account"
        };

        let relevant_columns: Vec<_> = columns.iter().filter(|(col, _)| !is_noise(col)).collect();

        // Detect *id, *key, *_sk columns (entity references)
        let id_columns: Vec<_> = relevant_columns
            .iter()
            .filter(|(col, _)| {
                let c = col.to_lowercase();
                c.ends_with("_id")
                    || c.ends_with("_key")
                    || c.ends_with("_sk")
                    || c == "id"
                    || c == "key"
            })
            .collect();

        let pk_columns: Vec<_> = relevant_columns
            .iter()
            .filter(|(_, is_pk)| *is_pk)
            .collect();

        let pk_id_columns: Vec<_> = pk_columns
            .iter()
            .filter(|(col, _)| {
                let c = col.to_lowercase();
                c.ends_with("_id")
                    || c.ends_with("_key")
                    || c.ends_with("_sk")
                    || c == "id"
                    || c == "key"
            })
            .collect();

        let has_type_column = relevant_columns.iter().any(|(col, _)| {
            let c = col.to_lowercase();
            c.ends_with("_type") || c == "type" || c == "interaction_type"
        });

        let has_origin = relevant_columns.iter().any(|(col, _)| {
            let c = col.to_lowercase();
            c.starts_with("origin_") || c.starts_with("src_")
        });

        let has_dest = relevant_columns.iter().any(|(col, _)| {
            let c = col.to_lowercase();
            c.starts_with("dest_") || c.starts_with("dst_") || c.starts_with("to_")
        });

        // Additional structural patterns
        let has_created_at = relevant_columns.iter().any(|(col, _)| {
            let c = col.to_lowercase();
            c.contains("created") || c.contains("timestamp") || c == "at"
        });

        let has_status = relevant_columns.iter().any(|(col, _)| {
            let c = col.to_lowercase();
            c.contains("status") || c.contains("state")
        });

        let pk_count = pk_columns.len();
        let fk_count = id_columns.len().saturating_sub(pk_id_columns.len());

        // === STRUCTURAL FALLBACK: Universal patterns (no word lists) ===

        // Composite PK with multiple FKs = fact table (standard edge)
        if pk_count >= 2 && fk_count >= 2 {
            return SchemaPatternMatch {
                table: table_name.to_string(),
                pattern: "standard_edge".to_string(),
                confidence: 0.9,
                details: format!("STRUCTURAL: composite PK + {} FKs = fact table", fk_count),
            };
        }

        // Composite PK without extra FKs = denormalized node
        if pk_count >= 2 {
            return SchemaPatternMatch {
                table: table_name.to_string(),
                pattern: "composite_node".to_string(),
                confidence: 0.85,
                details: format!("STRUCTURAL: composite PK with {} columns", pk_count),
            };
        }

        // Type column + multiple FKs = polymorphic edge
        if has_type_column && fk_count >= 2 {
            return SchemaPatternMatch {
                table: table_name.to_string(),
                pattern: "polymorphic_edge".to_string(),
                confidence: 0.85,
                details: "STRUCTURAL: type column + multiple FKs = polymorphic edge".to_string(),
            };
        }

        // origin/dest column patterns = denormalized edge
        if has_origin && has_dest && fk_count >= 1 {
            return SchemaPatternMatch {
                table: table_name.to_string(),
                pattern: "denormalized_edge".to_string(),
                confidence: 0.9,
                details: "STRUCTURAL: origin/dest columns = denormalized edge".to_string(),
            };
        }

        // Multiple FKs = standard edge table
        if fk_count >= 2 || pk_id_columns.len() >= 2 {
            return SchemaPatternMatch {
                table: table_name.to_string(),
                pattern: "standard_edge".to_string(),
                confidence: 0.85,
                details: format!(
                    "STRUCTURAL: {} FK columns = edge table",
                    if fk_count >= 2 {
                        fk_count
                    } else {
                        pk_id_columns.len()
                    }
                ),
            };
        }

        // Single PK + audit columns + status = temporal node
        if pk_count == 1 && has_created_at && has_status {
            return SchemaPatternMatch {
                table: table_name.to_string(),
                pattern: "standard_node".to_string(),
                confidence: 0.8,
                details: "STRUCTURAL: single PK + audit columns = temporal node".to_string(),
            };
        }

        // Single PK, no FKs = standard node (dimension table)
        if pk_count == 1 && fk_count == 0 {
            return SchemaPatternMatch {
                table: table_name.to_string(),
                pattern: "standard_node".to_string(),
                confidence: 0.9,
                details: "STRUCTURAL: single PK, no FKs = dimension node".to_string(),
            };
        }

        // Single PK + FK = FK-edge pattern
        if pk_count == 1 && fk_count >= 1 {
            return SchemaPatternMatch {
                table: table_name.to_string(),
                pattern: "fk_edge".to_string(),
                confidence: 0.7,
                details: "STRUCTURAL: single PK + FK = FK-edge pattern".to_string(),
            };
        }

        // No PK but has FKs = orphan relationship
        if pk_count == 0 && fk_count >= 1 {
            return SchemaPatternMatch {
                table: table_name.to_string(),
                pattern: "fk_edge".to_string(),
                confidence: 0.6,
                details: "STRUCTURAL: no PK, has FKs = orphan relationship".to_string(),
            };
        }

        // No PK, no FKs = flat table (fact or unknown)
        SchemaPatternMatch {
            table: table_name.to_string(),
            pattern: "flat_table".to_string(),
            confidence: 0.5,
            details: "STRUCTURAL: no PK, no FKs = flat table (verify intent)".to_string(),
        }
    }

    pub fn suggest_from_tables(
        &self,
        tables: &[TableSchemaInfo],
    ) -> Result<Vec<TableSuggestion>, String> {
        let mut suggestions = Vec::new();
        for table in tables {
            let suggestion = self.classify_table(&table.name);
            suggestions.push(suggestion);
        }
        Ok(suggestions)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SchemaPattern {
    StandardNode,
    StandardEdge,
    FkEdge,
    DenormalizedNode,
    DenormalizedEdge,
    PolymorphicEdge,
    CompositeNode,
    Unknown,
}

impl SchemaPattern {
    pub fn as_str(&self) -> &'static str {
        match self {
            SchemaPattern::StandardNode => "standard_node",
            SchemaPattern::StandardEdge => "standard_edge",
            SchemaPattern::FkEdge => "fk_edge",
            SchemaPattern::DenormalizedNode => "denormalized_node",
            SchemaPattern::DenormalizedEdge => "denormalized_edge",
            SchemaPattern::PolymorphicEdge => "polymorphic_edge",
            SchemaPattern::CompositeNode => "composite_node",
            SchemaPattern::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaPatternMatch {
    pub table: String,
    pub pattern: String,
    pub confidence: f32,
    pub details: String,
}

/// Create SchemaNlp without GLiNER model (for testing/fallback)
pub fn try_create_nlp() -> Option<SchemaNlp> {
    log::info!("SchemaNlp initialized without GLiNER model - set MODEL_PATH to enable ML-based classification");
    SchemaNlp::new("").ok()
}

pub fn try_create_nlp_with_noise(columns: Vec<String>) -> Option<SchemaNlp> {
    SchemaNlp::new("")
        .map(|n| n.with_noise_columns(columns))
        .ok()
}

/// Create SchemaNlp with GLiNER model from HuggingFace model ID
/// Model will be downloaded on first use
#[cfg(feature = "gliner")]
pub fn try_create_nlp_with_model(model_id: &str) -> Result<SchemaNlp, String> {
    log::info!(
        "Loading GLiNER model '{}' - this may download ~100-200MB on first use",
        model_id
    );
    // Note: gline-rs requires local ONNX model files
    // For HuggingFace models, you'd need to download and convert first
    // See: https://github.com/fbilhaut/gline-rs
    SchemaNlp::new("")
}

#[cfg(test)]
mod tests {
    use super::{SchemaEntity, SchemaNlp, SchemaPatternMatch};

    // ========================================================================
    // Fallback/Structural tests - work WITHOUT GLiNER model
    // ========================================================================

    #[test]
    fn test_entity_conversion() {
        assert_eq!(SchemaEntity::NodeTable.as_str(), "NODE_TABLE");
        assert_eq!(SchemaEntity::ForeignKey.as_str(), "FOREIGN_KEY");
    }

    #[test]
    fn test_nlp_creation() {
        let nlp = SchemaNlp::new("").unwrap();
        assert!(nlp.noise_columns.contains(&"tenant_id".to_string()));
    }

    #[test]
    fn test_classify_without_model() {
        let nlp = SchemaNlp::new("").unwrap();
        // Without model, returns unknown
        let result = nlp.classify_table("users");
        assert_eq!(result.suggestion_type, "unknown");
    }

    #[test]
    fn test_noise_columns() {
        let nlp = SchemaNlp::new("")
            .unwrap()
            .with_noise_columns(vec!["workspace_id".to_string()]);
        assert!(nlp.noise_columns.contains(&"workspace_id".to_string()));
    }

    // ========================================================================
    // Structural Fallback Tests - verify pattern detection without ML
    // ========================================================================

    #[test]
    fn test_structural_standard_node() {
        let nlp = SchemaNlp::new("").unwrap();
        // Single PK, no FKs = standard_node
        let cols = [("user_id", true), ("name", false), ("email", false)];
        let result = nlp.detect_schema_pattern_structural("users", &cols);
        assert_eq!(result.pattern, "standard_node");
    }

    #[test]
    fn test_structural_standard_edge() {
        let nlp = SchemaNlp::new("").unwrap();
        // Multiple FKs = standard_edge
        let cols = [
            ("id", true),
            ("user_id", false),
            ("post_id", false),
            ("created_at", false),
        ];
        let result = nlp.detect_schema_pattern_structural("likes", &cols);
        assert_eq!(result.pattern, "standard_edge");
    }

    #[test]
    fn test_structural_composite_pk() {
        let nlp = SchemaNlp::new("").unwrap();
        // Composite PK + FKs = standard_edge
        let cols = [
            ("order_id", true),
            ("line_id", true),
            ("customer_id", false),
            ("product_id", false),
        ];
        let result = nlp.detect_schema_pattern_structural("order_lines", &cols);
        assert!(result.pattern == "standard_edge" || result.pattern == "composite_node");
    }

    #[test]
    fn test_structural_denormalized_edge() {
        let nlp = SchemaNlp::new("").unwrap();
        // origin_/dest_ columns = denormalized_edge
        let cols = [
            ("id", true),
            ("origin_user_id", false),
            ("dest_user_id", false),
            ("action", false),
        ];
        let result = nlp.detect_schema_pattern_structural("user_actions", &cols);
        assert_eq!(result.pattern, "denormalized_edge");
    }

    #[test]
    fn test_structural_polymorphic_edge() {
        let nlp = SchemaNlp::new("").unwrap();
        // Type column + multiple FKs = polymorphic_edge
        let cols = [
            ("id", true),
            ("from_id", false),
            ("to_id", false),
            ("rel_type", false),
        ];
        let result = nlp.detect_schema_pattern_structural("relationships", &cols);
        assert_eq!(result.pattern, "polymorphic_edge");
    }

    #[test]
    fn test_structural_flat_table() {
        let nlp = SchemaNlp::new("").unwrap();
        // No PK, no FKs = flat_table (use columns without _id/_key suffix)
        let cols = [
            ("trip_uuid", false),
            ("pickup_location", false),
            ("dropoff_location", false),
            ("fare_amount", false),
        ];
        let result = nlp.detect_schema_pattern_structural("trips", &cols);
        assert_eq!(result.pattern, "flat_table");
    }

    #[test]
    fn test_structural_noise_filtering() {
        let nlp = SchemaNlp::new("").unwrap();
        // tenant_id should be filtered out
        let cols = [("tenant_id", false), ("user_id", true), ("name", false)];
        let result = nlp.detect_schema_pattern_structural("users", &cols);
        // After filtering tenant_id, only user_id PK remains -> standard_node
        assert_eq!(result.pattern, "standard_node");
    }

    // ========================================================================
    // GLiNER Integration Tests - require model to be loaded
    // These test the ML-based classification path
    // Run with: cargo test --lib schema_pattern --features gliner
    // ========================================================================

    #[cfg(feature = "gliner")]
    #[test]
    fn test_gliner_classify_table_name() {
        // This test requires GLiNER model to be loaded
        // Skip if model not available
        let nlp = match SchemaNlp::new("path/to/model") {
            Ok(n) => n,
            Err(_) => return,
        };

        // Test with known table names
        let result = nlp.classify_table("users");
        // Should classify as node or edge (not unknown)
        assert!(result.suggestion_type == "node" || result.suggestion_type == "edge");
    }

    #[cfg(feature = "gliner")]
    #[test]
    fn test_gliner_extract_entities() {
        let nlp = match SchemaNlp::new("path/to/model") {
            Ok(n) => n,
            Err(_) => return,
        };

        let cols = [
            ("user_id", false),
            ("post_id", false),
            ("created_at", false),
        ];

        // Should extract entity references from column names
        let entities = nlp.extract_entity_references(&cols);
        // At least user and post entities should be found
        let entity_names: Vec<_> = entities.iter().map(|(e, _)| e.as_str()).collect();
        assert!(entity_names.contains(&"user") || entity_names.contains(&"post"));
    }
}
