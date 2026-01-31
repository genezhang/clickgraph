//! Variable-Length Path Transitivity Check
//!
//! Validates that variable-length path patterns are semantically meaningful.
//!
//! A VLP pattern `(a)-[r:TYPE*]->(b)` can only recurse if the relationship
//! is transitive, meaning the TO node can also be a FROM node for the same
//! relationship type.
//!
//! Example:
//!   ✓ Valid:   (Person)-[KNOWS*]->(Person)  - Person can KNOW another Person
//!   ✗ Invalid: (IP)-[DNS_REQUESTED*]->(Domain) - Domain cannot DNS_REQUEST anything
//!
//! For non-transitive patterns, this pass converts them to fixed-length (min_hops only):
//!   `(a)-[r:TYPE*]->(b)` → `(a)-[r:TYPE*1]->(b)` (exactly 1 hop)
//!   `(a)-[r:TYPE*2..]->(b)` → Semantic error (impossible, min_hops > 1 but non-transitive)

use std::sync::Arc;

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    query_planner::{
        logical_plan::{GraphRel, LogicalPlan, VariableLengthSpec},
        plan_ctx::PlanCtx,
        transformed::Transformed,
    },
};

use super::{
    analyzer_pass::{AnalyzerPass, AnalyzerResult},
    errors::AnalyzerError,
};

pub struct VlpTransitivityCheck;

impl VlpTransitivityCheck {
    pub fn new() -> Self {
        Self
    }

    /// Extract the base type name from a potentially composite key
    /// "KNOWS::Person::Person" -> "KNOWS"
    /// "KNOWS" -> "KNOWS"
    fn extract_type_name(key: &str) -> &str {
        // Composite keys have format "TYPE::FROM::TO"
        // Split by "::" and take the first part
        key.split("::").next().unwrap_or(key)
    }

    /// Check if a relationship can be transitive (recursive)
    /// Returns true if the TO node can also be a FROM node for the same relationship type
    fn is_transitive_relationship(
        rel_type: &str,
        schema: &GraphSchema,
    ) -> Result<bool, AnalyzerError> {
        // Extract base type name from potentially composite key
        // "KNOWS::Person::Person" -> "KNOWS"
        let base_type = Self::extract_type_name(rel_type);

        // Get all relationship schemas for this type
        let rel_schemas = schema.rel_schemas_for_type(base_type);

        if rel_schemas.is_empty() {
            return Err(AnalyzerError::RelationshipTypeNotFound(
                rel_type.to_string(),
            ));
        }

        // Check if ANY variant of this relationship type allows transitivity
        // A relationship is transitive if:
        // 1. from_node == to_node (self-loop like Person-KNOWS->Person), OR
        // 2. The to_node of one variant can be the from_node of another variant
        // 3. For polymorphic relationships: check if any to_label_value overlaps with from_label_values

        // Collect all (from_node, to_node) pairs for this relationship type
        // For polymorphic relationships, use from_label_values and to_label_values
        let mut from_nodes = std::collections::HashSet::new();
        let mut to_nodes = std::collections::HashSet::new();

        for rel_schema in &rel_schemas {
            // For polymorphic FROM side, use from_label_values if available
            if let Some(ref values) = rel_schema.from_label_values {
                for v in values {
                    from_nodes.insert(v.clone());
                }
            } else if rel_schema.from_node != "$any" {
                from_nodes.insert(rel_schema.from_node.clone());
            }

            // For polymorphic TO side, use to_label_values if available
            if let Some(ref values) = rel_schema.to_label_values {
                for v in values {
                    to_nodes.insert(v.clone());
                }
            } else if rel_schema.to_node != "$any" {
                to_nodes.insert(rel_schema.to_node.clone());
            }

            // Check for self-loop (from == to) - with polymorphic support
            let from_set: std::collections::HashSet<_> = rel_schema
                .from_label_values
                .as_ref()
                .map(|v| v.iter().cloned().collect())
                .unwrap_or_else(|| {
                    let mut s = std::collections::HashSet::new();
                    if rel_schema.from_node != "$any" {
                        s.insert(rel_schema.from_node.clone());
                    }
                    s
                });
            let to_set: std::collections::HashSet<_> = rel_schema
                .to_label_values
                .as_ref()
                .map(|v| v.iter().cloned().collect())
                .unwrap_or_else(|| {
                    let mut s = std::collections::HashSet::new();
                    if rel_schema.to_node != "$any" {
                        s.insert(rel_schema.to_node.clone());
                    }
                    s
                });

            // If any value is in both from and to, it's a self-loop (transitive)
            if from_set.intersection(&to_set).next().is_some() {
                log::info!(
                    "✓ VLP transitivity: '{}' is transitive (self-loop found in from={:?}, to={:?})",
                    rel_type,
                    from_set,
                    to_set
                );
                return Ok(true);
            }
        }

        // Check if any to_node can also be a from_node (allows chaining)
        let can_chain = to_nodes.iter().any(|to| from_nodes.contains(to));

        if can_chain {
            log::info!(
                "✓ VLP transitivity: '{}' is transitive (to_nodes {:?} overlap with from_nodes {:?})",
                rel_type,
                to_nodes,
                from_nodes
            );
        } else {
            log::warn!(
                "⚠ VLP transitivity: '{}' is NON-transitive! from_nodes: {:?}, to_nodes: {:?}. Converting to fixed-length.",
                rel_type,
                from_nodes,
                to_nodes
            );
        }

        Ok(can_chain)
    }

    /// Check if non-transitive VLP should error (min_hops > 1)
    fn validate_non_transitive(
        vlp_spec: &VariableLengthSpec,
        rel_type: &str,
    ) -> Result<(), AnalyzerError> {
        // If min_hops is Some and > 1, this is a semantic error
        // You can't have a path of length 2+ if the relationship is non-transitive!
        if let Some(min) = vlp_spec.min_hops {
            if min > 1 {
                return Err(AnalyzerError::InvalidPlan(
                    format!(
                        "Variable-length path pattern [{}*{}..] is semantically invalid: \
                         relationship '{}' is non-transitive (cannot recurse). \
                         The TO node never appears as a FROM node, so paths longer than 1 hop are impossible.",
                        rel_type,
                        min,
                        rel_type
                    )
                ));
            }
        }
        Ok(())
    }
}

impl AnalyzerPass for VlpTransitivityCheck {
    fn analyze_with_graph_schema(
        &self,
        plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        self.check_transitivity_recursive(plan, plan_ctx, graph_schema)
    }
}

impl VlpTransitivityCheck {
    fn check_transitivity_recursive(
        &self,
        plan: Arc<LogicalPlan>,
        plan_ctx: &mut PlanCtx,
        graph_schema: &GraphSchema,
    ) -> AnalyzerResult<Transformed<Arc<LogicalPlan>>> {
        match plan.as_ref() {
            LogicalPlan::GraphRel(rel) => {
                log::info!(
                    "🔍 VLP Transitivity Check: Found GraphRel, variable_length={:?}",
                    rel.variable_length
                );
                // Check if this is a variable-length path
                if let Some(ref vlp_spec) = rel.variable_length {
                    log::info!("✓ VLP Transitivity Check: Has VLP spec: {:?}", vlp_spec);
                    // Only check unbounded or multi-hop patterns
                    // Single fixed-hop patterns (e.g., *1) don't need transitivity
                    let needs_transitivity = match (vlp_spec.min_hops, vlp_spec.max_hops) {
                        (Some(min), Some(max)) if min == max && min == 1 => false, // *1 is fine
                        _ => true, // *, *2, *1.., *2..5, etc. all need transitivity check
                    };

                    log::info!(
                        "🔍 VLP Transitivity Check: needs_transitivity={}",
                        needs_transitivity
                    );
                    if needs_transitivity {
                        log::info!("🔍 VLP Transitivity Check: Checking transitivity...");
                        
                        // 🔧 FIX: Shortest path queries don't require explicit relationship types
                        // They can traverse any relationship, and the schema will be resolved dynamically
                        if rel.shortest_path_mode.is_some() {
                            log::info!("🔧 VLP: Shortest path query - skipping relationship type requirement");
                            // For shortest path, we don't enforce relationship type requirement
                            // The query will traverse all available relationships
                            return Ok(Transformed::No(plan));
                        }
                        
                        // Get relationship type(s) - required for non-shortest-path VLP
                        let rel_types = rel.labels.as_ref().ok_or_else(|| {
                            AnalyzerError::InvalidPlan(
                                "Variable-length path missing relationship type".to_string(),
                            )
                        })?;
                        log::info!("🔍 VLP Transitivity Check: rel_types={:?}", rel_types);

                        // For simplicity, check the first relationship type
                        // TODO: Handle multiple types (TYPE1|TYPE2)
                        let rel_type = rel_types.first().ok_or_else(|| {
                            AnalyzerError::InvalidPlan(
                                "Variable-length path has empty relationship type list".to_string(),
                            )
                        })?;

                        // Check if this relationship is transitive
                        log::info!(
                            "⚠ VLP Transitivity Check: Checking if '{}' is transitive...",
                            rel_type
                        );
                        let is_transitive =
                            Self::is_transitive_relationship(rel_type, graph_schema)?;
                        log::info!(
                            "⚠ VLP transitivity: '{}' is {}!",
                            rel_type,
                            if is_transitive {
                                "TRANSITIVE"
                            } else {
                                "NON-TRANSITIVE"
                            }
                        );

                        if !is_transitive {
                            // Validate - error if min_hops > 1
                            Self::validate_non_transitive(vlp_spec, rel_type)?;

                            // Remove variable_length entirely - becomes simple single-hop
                            log::info!(
                                "→ Removing VLP from non-transitive [{}*] - converting to simple single-hop pattern",
                                rel_type
                            );

                            // Create new GraphRel WITHOUT variable_length
                            let new_rel = GraphRel {
                                variable_length: None, // Remove VLP - just a normal edge
                                ..rel.clone()
                            };

                            return Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphRel(new_rel))));
                        }
                    }
                }

                // Recursively check child nodes
                let left_result =
                    self.check_transitivity_recursive(rel.left.clone(), plan_ctx, graph_schema)?;
                let center_result =
                    self.check_transitivity_recursive(rel.center.clone(), plan_ctx, graph_schema)?;
                let right_result =
                    self.check_transitivity_recursive(rel.right.clone(), plan_ctx, graph_schema)?;

                if left_result.is_yes() || center_result.is_yes() || right_result.is_yes() {
                    let new_rel = GraphRel {
                        left: left_result.get_plan().clone(),
                        center: center_result.get_plan().clone(),
                        right: right_result.get_plan().clone(),
                        ..rel.clone()
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphRel(new_rel))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            // Recursively traverse other plan types
            LogicalPlan::Projection(proj) => {
                let input_result =
                    self.check_transitivity_recursive(proj.input.clone(), plan_ctx, graph_schema)?;
                if input_result.is_yes() {
                    let new_proj = crate::query_planner::logical_plan::Projection {
                        input: input_result.get_plan().clone(),
                        ..proj.clone()
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Projection(
                        new_proj,
                    ))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::Filter(filter) => {
                let input_result = self.check_transitivity_recursive(
                    filter.input.clone(),
                    plan_ctx,
                    graph_schema,
                )?;
                if input_result.is_yes() {
                    let new_filter = crate::query_planner::logical_plan::Filter {
                        input: input_result.get_plan().clone(),
                        ..filter.clone()
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Filter(new_filter))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::GraphJoins(joins) => {
                let input_result =
                    self.check_transitivity_recursive(joins.input.clone(), plan_ctx, graph_schema)?;
                if input_result.is_yes() {
                    let new_joins = crate::query_planner::logical_plan::GraphJoins {
                        input: input_result.get_plan().clone(),
                        ..joins.clone()
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphJoins(
                        new_joins,
                    ))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::Limit(limit) => {
                let input_result =
                    self.check_transitivity_recursive(limit.input.clone(), plan_ctx, graph_schema)?;
                if input_result.is_yes() {
                    let new_limit = crate::query_planner::logical_plan::Limit {
                        input: input_result.get_plan().clone(),
                        ..limit.clone()
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Limit(new_limit))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            // Other plan types don't contain GraphRel, pass through
            _ => Ok(Transformed::No(plan)),
        }
    }
}
