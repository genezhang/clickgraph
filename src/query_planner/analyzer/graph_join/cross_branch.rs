//! Cross-Branch Join Detection and Generation
//!
//! This module handles detecting and generating JOINs for cross-branch patterns
//! in Cypher queries. Cross-branch patterns occur when a node appears in multiple
//! independent relationship patterns (comma-separated MATCH clauses).
//!
//! ## Examples
//!
//! **Cross-branch (needs JOIN):**
//! ```cypher
//! MATCH (a)-[:R1]->(b), (a)-[:R2]->(c)
//! ```
//! Node 'a' is the source in both patterns - requires JOIN between R1 and R2 tables.
//!
//! **Linear chain (no cross-branch JOIN):**
//! ```cypher
//! MATCH (a)-[:R1]->(b)-[:R2]->(c)
//! ```
//! Node 'b' is the target of R1 and source of R2 - sequential, not branching.
//!
//! ## Key Types
//!
//! - [`NodeAppearance`] - Tracks where a node variable appears in the query
//! - Functions for generating relationship uniqueness constraints

use std::collections::HashMap;

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    query_planner::{
        analyzer::errors::{AnalyzerError, Pass},
        logical_expr::{LogicalExpr, Operator, OperatorApplication, PropertyAccess, TableAlias},
        logical_plan::{GraphRel, Join, LogicalPlan},
        plan_ctx::PlanCtx,
    },
};

use super::helpers;
use super::metadata::PatternGraphMetadata;

/// Tracks where a node variable appears in the query plan.
/// Used for detecting cross-branch shared nodes that require JOINs.
#[derive(Debug, Clone)]
pub struct NodeAppearance {
    /// The table alias to use in JOIN conditions.
    /// For regular patterns: the relationship alias (e.g., "t1")
    /// For VLP patterns: the node alias (e.g., "g") since VLP CTE replaces the relationship
    pub rel_alias: String,
    /// Node label (e.g., "IP", "Domain")
    pub node_label: String,
    /// Table where this node's data lives
    /// For regular patterns: edge table (from relationship schema)
    /// For VLP patterns: node table (from node schema)
    pub table_name: String,
    /// Database where the table lives
    pub database: String,
    /// Column name for node ID in the table
    pub column_name: String,
    /// Whether this is the from-side (true) or to-side (false) of the relationship
    pub is_from_side: bool,
    /// Whether this appearance is from a VLP (Variable-Length Path) pattern
    pub is_vlp: bool,
}

// =============================================================================
// Relationship Uniqueness Constraints
// =============================================================================

/// Generate relationship uniqueness constraints to prevent the same physical
/// relationship from being traversed multiple times.
///
/// This can happen with bidirectional edges: `(a)-[r1]-(b)-[r2]-(c)`
/// Generates WHERE clauses like: `r1.id != r2.id` or composite checks for multi-column IDs.
pub fn generate_relationship_uniqueness_constraints(
    pattern_metadata: &PatternGraphMetadata,
    graph_schema: &GraphSchema,
) -> Vec<LogicalExpr> {
    let mut constraints = Vec::new();

    // Only generate constraints if we have 2+ relationships
    if pattern_metadata.edges.len() < 2 {
        return constraints;
    }

    crate::debug_print!(
        "🔐 Phase 4: Generating relationship uniqueness constraints for {} edges",
        pattern_metadata.edges.len()
    );

    // For each pair of edges, generate r_i.id != r_j.id constraint
    for i in 0..pattern_metadata.edges.len() {
        for j in (i + 1)..pattern_metadata.edges.len() {
            let edge1 = &pattern_metadata.edges[i];
            let edge2 = &pattern_metadata.edges[j];

            // Skip if either edge is VLP (handled differently in CTE)
            if edge1.is_vlp || edge2.is_vlp {
                continue;
            }

            // Skip if either edge has no relationship types (filtered by property filtering)
            if edge1.rel_types.is_empty() || edge2.rel_types.is_empty() {
                continue;
            }

            // Get relationship schemas to determine edge ID columns
            let rel1_schema = match graph_schema.get_rel_schema(&edge1.rel_types[0]) {
                Ok(schema) => schema,
                Err(_) => continue, // Skip if schema not found
            };
            let rel2_schema = match graph_schema.get_rel_schema(&edge2.rel_types[0]) {
                Ok(schema) => schema,
                Err(_) => continue,
            };

            // Determine edge ID columns (use edge_id if specified, else [from_id, to_id])
            let edge1_id_cols: Vec<String> = rel1_schema
                .edge_id
                .as_ref()
                .map(|id| id.columns().into_iter().map(|s| s.to_string()).collect())
                .unwrap_or_else(|| {
                    vec![
                        rel1_schema.from_id.to_string(),
                        rel1_schema.to_id.to_string(),
                    ]
                });
            let edge2_id_cols: Vec<String> = rel2_schema
                .edge_id
                .as_ref()
                .map(|id| id.columns().into_iter().map(|s| s.to_string()).collect())
                .unwrap_or_else(|| {
                    vec![
                        rel2_schema.from_id.to_string(),
                        rel2_schema.to_id.to_string(),
                    ]
                });

            // Generate inequality constraint
            // For single column: r1.id != r2.id
            // For composite: (r1.col1 != r2.col1) OR (r1.col2 != r2.col2) OR ...
            let constraint = if edge1_id_cols.len() == 1 && edge2_id_cols.len() == 1 {
                // Simple case: single column ID
                LogicalExpr::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::NotEqual,
                    operands: vec![
                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(edge1.alias.clone()),
                            column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                                edge1_id_cols[0].to_string(),
                            ),
                        }),
                        LogicalExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(edge2.alias.clone()),
                            column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                                edge2_id_cols[0].to_string(),
                            ),
                        }),
                    ],
                })
            } else {
                // Composite case: (r1.col1, r1.col2) != (r2.col1, r2.col2)
                // SQL: (r1.col1 != r2.col1) OR (r1.col2 != r2.col2) OR ...
                let mut or_operands = Vec::new();
                for (col1, col2) in edge1_id_cols.iter().zip(edge2_id_cols.iter()) {
                    or_operands.push(LogicalExpr::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::NotEqual,
                        operands: vec![
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(edge1.alias.clone()),
                                column:
                                    crate::graph_catalog::expression_parser::PropertyValue::Column(
                                        col1.to_string(),
                                    ),
                            }),
                            LogicalExpr::PropertyAccessExp(PropertyAccess {
                                table_alias: TableAlias(edge2.alias.clone()),
                                column:
                                    crate::graph_catalog::expression_parser::PropertyValue::Column(
                                        col2.to_string(),
                                    ),
                            }),
                        ],
                    }));
                }

                // Combine with OR
                if or_operands.len() == 1 {
                    or_operands
                        .into_iter()
                        .next()
                        .expect("Vector with len==1 must have element")
                } else {
                    LogicalExpr::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::Or,
                        operands: or_operands,
                    })
                }
            };

            crate::debug_print!(
                "   🔐 Adding uniqueness constraint: {} != {}",
                edge1.alias,
                edge2.alias
            );
            constraints.push(constraint);
        }
    }

    crate::debug_print!("✅ Generated {} uniqueness constraints", constraints.len());
    constraints
}

// =============================================================================
// Cross-Branch Join Generation (Metadata-Based)
// =============================================================================

/// Generate cross-branch joins using pattern metadata.
///
/// **Key Insight**: Cross-branch patterns have a node in the SAME ROLE (from/to) in multiple edges.
/// - Linear: `(a)-[:R1]->(b)-[:R2]->(c)` - 'b' is to_node of R1, from_node of R2 (chain) → NO cross-branch
/// - Comma: `(a)-[:R1]->(b), (a)-[:R2]->(c)` - 'a' is from_node in BOTH (branches) → YES cross-branch
pub fn generate_cross_branch_joins_from_metadata(
    pattern_metadata: &PatternGraphMetadata,
    _plan_ctx: &PlanCtx,
    graph_schema: &GraphSchema,
) -> Result<Vec<Join>, AnalyzerError> {
    let mut joins = Vec::new();

    // Find all nodes that appear in multiple edges (potential cross-branch candidates)
    for (node_alias, node_info) in &pattern_metadata.nodes {
        if node_info.appearance_count <= 1 {
            continue; // Not a cross-branch node
        }

        // CRITICAL FIX: If the shared node IS REFERENCED, skip cross-branch join!
        // When the node is referenced (used in RETURN/WHERE), we'll JOIN the node table,
        // which provides the connection between edges.
        if node_info.is_referenced {
            log::debug!("🔍 Skipping cross-branch for '{}' - node IS REFERENCED (node table JOIN provides connection)",
                node_alias);
            continue;
        }

        log::debug!(
            "🔍 Potential cross-branch node '{}' appears in {} edges (NOT referenced)",
            node_alias,
            node_info.appearance_count
        );

        // Get all edges that use this node
        let edges = pattern_metadata.edges_using_node(node_alias);
        if edges.len() < 2 {
            continue; // Need at least 2 edges to generate cross-branch join
        }

        // Group edges by (table_name, role)
        let mut from_edges: HashMap<String, Vec<&super::metadata::PatternEdgeInfo>> =
            HashMap::new();
        let mut to_edges: HashMap<String, Vec<&super::metadata::PatternEdgeInfo>> = HashMap::new();

        for edge in &edges {
            // Skip edges with no relationship types (filtered out by property filtering)
            if edge.rel_types.is_empty() {
                log::debug!(
                    "🔍 Skipping edge {}->{}->{}  with no relationship types (filtered)",
                    edge.from_node,
                    edge.alias,
                    edge.to_node
                );
                continue;
            }
            let rel_schema = graph_schema
                .get_rel_schema(&edge.rel_types[0])
                .map_err(|e| AnalyzerError::GraphSchema {
                    pass: Pass::GraphJoinInference,
                    source: e,
                })?;

            // Determine the role of this node in this edge
            if edge.from_node == *node_alias {
                from_edges
                    .entry(rel_schema.table_name.clone())
                    .or_default()
                    .push(*edge);
            } else if edge.to_node == *node_alias {
                to_edges
                    .entry(rel_schema.table_name.clone())
                    .or_default()
                    .push(*edge);
            }
        }

        // Cross-branch pattern: node is from_node in multiple DIFFERENT tables
        let has_from_branch = from_edges.len() > 1;
        let has_to_branch = to_edges.len() > 1;

        if has_from_branch {
            log::debug!(
                "   ✅ Node '{}' is CROSS-BRANCH (from_node in {} different tables)",
                node_alias,
                from_edges.len()
            );

            let table_edges: Vec<_> = from_edges.values().collect();
            let edge1 = table_edges[0][0];
            let edge2 = table_edges[1][0];

            joins.push(create_cross_branch_join_from_edges(
                edge1,
                edge2,
                node_alias,
                true,
                graph_schema,
            )?);
        }

        if has_to_branch {
            log::debug!(
                "   ✅ Node '{}' is CROSS-BRANCH (to_node in {} different tables)",
                node_alias,
                to_edges.len()
            );

            let table_edges: Vec<_> = to_edges.values().collect();
            let edge1 = table_edges[0][0];
            let edge2 = table_edges[1][0];

            joins.push(create_cross_branch_join_from_edges(
                edge1,
                edge2,
                node_alias,
                false,
                graph_schema,
            )?);
        }

        if !has_from_branch && !has_to_branch {
            log::debug!(
                "   ⏭️  Node '{}' NOT cross-branch (different roles in edges - linear chain)",
                node_alias
            );
        }
    }

    Ok(joins)
}

/// Helper to create a cross-branch JOIN between two edges sharing a node.
fn create_cross_branch_join_from_edges(
    edge1: &super::metadata::PatternEdgeInfo,
    edge2: &super::metadata::PatternEdgeInfo,
    node_alias: &str,
    is_from_side: bool,
    graph_schema: &GraphSchema,
) -> Result<Join, AnalyzerError> {
    log::debug!(
        "   Generating cross-branch JOIN between '{}' and '{}' on shared node '{}' ({})",
        edge1.alias,
        edge2.alias,
        node_alias,
        if is_from_side { "from_node" } else { "to_node" }
    );

    // Guard: Skip if either edge has no relationship types (filtered by property filtering)
    if edge1.rel_types.is_empty() {
        return Err(AnalyzerError::SchemaNotFound(format!(
            "Edge '{}' has no relationship types (filtered by property filtering)",
            edge1.alias
        )));
    }
    if edge2.rel_types.is_empty() {
        return Err(AnalyzerError::SchemaNotFound(format!(
            "Edge '{}' has no relationship types (filtered by property filtering)",
            edge2.alias
        )));
    }

    // Get relationship schemas
    let rel1_schema = graph_schema
        .get_rel_schema(&edge1.rel_types[0])
        .map_err(|e| AnalyzerError::GraphSchema {
            pass: Pass::GraphJoinInference,
            source: e,
        })?;
    let rel2_schema = graph_schema
        .get_rel_schema(&edge2.rel_types[0])
        .map_err(|e| AnalyzerError::GraphSchema {
            pass: Pass::GraphJoinInference,
            source: e,
        })?;

    // Determine join columns based on shared node's role
    let edge1_col = if is_from_side {
        &rel1_schema.from_id
    } else {
        &rel1_schema.to_id
    };

    let edge2_col = if is_from_side {
        &rel2_schema.from_id
    } else {
        &rel2_schema.to_id
    };

    // Create the cross-branch join using JoinBuilder
    let join = helpers::JoinBuilder::new(rel2_schema.full_table_name(), &edge2.alias)
        .add_condition(
            &edge2.alias,
            edge2_col.to_string(),
            &edge1.alias,
            edge1_col.to_string(),
        )
        .build();

    log::debug!(
        "   ➕ Adding cross-branch JOIN: {} AS {} ON {}.{} = {}.{}",
        join.table_name,
        join.table_alias,
        edge2.alias,
        edge2_col,
        edge1.alias,
        edge1_col
    );

    Ok(join)
}

// =============================================================================
// Legacy Cross-Branch Detection (Runtime-based)
// =============================================================================

/// Check for cross-branch shared nodes and generate JOINs where needed.
///
/// This handles branching patterns like: `(a)-[:R1]->(b), (a)-[:R2]->(c)`
/// where node 'a' appears in multiple GraphRel branches and requires
/// a JOIN between the edge tables.
pub fn check_and_generate_cross_branch_joins(
    graph_rel: &GraphRel,
    plan_ctx: &PlanCtx,
    graph_schema: &GraphSchema,
    node_appearances: &mut HashMap<String, Vec<NodeAppearance>>,
    collected_graph_joins: &mut Vec<Join>,
) -> Result<(), AnalyzerError> {
    log::debug!(
        "🔍 check_and_generate_cross_branch_joins for GraphRel({})",
        graph_rel.alias
    );
    log::debug!(
        "   left_connection: {}, right_connection: {}",
        graph_rel.left_connection,
        graph_rel.right_connection
    );

    // Process left_connection (source node)
    check_node_for_cross_branch_join(
        &graph_rel.left_connection,
        graph_rel,
        true, // is_from_side
        plan_ctx,
        graph_schema,
        node_appearances,
        collected_graph_joins,
    )?;

    // Process right_connection (target node)
    check_node_for_cross_branch_join(
        &graph_rel.right_connection,
        graph_rel,
        false, // is_from_side
        plan_ctx,
        graph_schema,
        node_appearances,
        collected_graph_joins,
    )?;

    Ok(())
}

/// Check a single node for cross-branch sharing and generate JOIN if needed.
fn check_node_for_cross_branch_join(
    node_alias: &str,
    graph_rel: &GraphRel,
    is_from_side: bool,
    plan_ctx: &PlanCtx,
    graph_schema: &GraphSchema,
    node_appearances: &mut HashMap<String, Vec<NodeAppearance>>,
    collected_graph_joins: &mut Vec<Join>,
) -> Result<(), AnalyzerError> {
    log::debug!(
        "   📍 check_node_for_cross_branch_join: node='{}', GraphRel({}), is_from_side={}",
        node_alias,
        graph_rel.alias,
        is_from_side
    );

    // Extract node appearance info
    let current_appearance = match extract_node_appearance(
        node_alias,
        graph_rel,
        is_from_side,
        plan_ctx,
        graph_schema,
    ) {
        Ok(appearance) => appearance,
        Err(e) => {
            log::debug!(
                "   ⚠️  Cannot extract node appearance for '{}': {}",
                node_alias,
                e
            );
            return Ok(()); // Skip if we can't extract info
        }
    };

    log::debug!(
        "   📍 Node '{}' in GraphRel({}) → {}.{}",
        node_alias,
        current_appearance.rel_alias,
        current_appearance.table_name,
        current_appearance.column_name
    );

    // Check for cross-branch pattern (shared node in different relationship tables)
    if let Some(prev_appearances) = node_appearances.get(node_alias) {
        log::debug!(
            "   🔍 Node '{}' seen before - checking if cross-branch JOIN needed",
            node_alias
        );

        for prev_appearance in prev_appearances {
            if prev_appearance.table_name != current_appearance.table_name {
                // Different relationship tables - this is a comma pattern!
                log::info!("   ✅ COMMA PATTERN: Node '{}' appears in different relationship tables: {} vs {}",
                    node_alias, prev_appearance.table_name, current_appearance.table_name);

                generate_cross_branch_join(
                    node_alias,
                    &current_appearance,
                    prev_appearance,
                    collected_graph_joins,
                )?;

                break; // Only need one JOIN per shared node
            }
        }
    }

    // Record this appearance for future checks
    node_appearances
        .entry(node_alias.to_string())
        .or_default()
        .push(current_appearance);

    Ok(())
}

/// Extract node appearance information from a GraphRel.
pub fn extract_node_appearance(
    node_alias: &str,
    graph_rel: &GraphRel,
    is_from_side: bool,
    plan_ctx: &PlanCtx,
    graph_schema: &GraphSchema,
) -> Result<NodeAppearance, AnalyzerError> {
    log::debug!(
        "      🔎 extract_node_appearance: node='{}', GraphRel({}), is_from_side={}",
        node_alias,
        graph_rel.alias,
        is_from_side
    );

    // Check if this is a VLP (Variable-Length Path) pattern
    let is_vlp = graph_rel.variable_length.is_some();

    // 1. Get node label for the current node from plan_ctx
    let table_ctx = plan_ctx
        .get_table_ctx_from_alias_opt(&Some(node_alias.to_string()))
        .map_err(|e| AnalyzerError::PlanCtx {
            pass: Pass::GraphJoinInference,
            source: e,
        })?;

    let node_label = table_ctx
        .get_label_str()
        .map_err(|e| AnalyzerError::PlanCtx {
            pass: Pass::GraphJoinInference,
            source: e,
        })?;

    // 2. Get left and right node labels from GraphRel
    let left_label_opt = plan_ctx
        .get_table_ctx_from_alias_opt(&Some(graph_rel.left_connection.clone()))
        .ok()
        .and_then(|ctx| ctx.get_label_str().ok());

    let right_label_opt = plan_ctx
        .get_table_ctx_from_alias_opt(&Some(graph_rel.right_connection.clone()))
        .ok()
        .and_then(|ctx| ctx.get_label_str().ok());

    // 3. Get relationship schema
    let rel_types: Vec<String> = graph_rel.labels.clone().unwrap_or_default();

    if rel_types.is_empty() {
        return Err(AnalyzerError::SchemaNotFound(format!(
            "No relationship types found for GraphRel({})",
            graph_rel.alias
        )));
    }

    let rel_schema = graph_schema
        .get_rel_schema_with_nodes(
            &rel_types[0],
            left_label_opt.as_deref(),
            right_label_opt.as_deref(),
        )
        .map_err(|e| {
            AnalyzerError::SchemaNotFound(format!(
                "Failed to get rel schema for {}::{}::{}: {}",
                rel_types[0],
                left_label_opt.as_deref().unwrap_or("None"),
                right_label_opt.as_deref().unwrap_or("None"),
                e
            ))
        })?;

    // Build composite key and get node schema
    let composite_key = format!(
        "{}::{}::{}",
        rel_schema.database, rel_schema.table_name, node_label
    );

    let node_schema = graph_schema
        .node_schema_opt(&composite_key)
        .or_else(|| graph_schema.node_schema_opt(&node_label))
        .ok_or_else(|| {
            AnalyzerError::NodeLabelNotFound(format!(
                "{} (composite: {})",
                node_label, composite_key
            ))
        })?;

    // VLP FIX: For Variable-Length Paths, use node alias and node table
    if is_vlp {
        log::info!("🔧 VLP NodeAppearance: Using node alias '{}' instead of rel alias '{}' for cross-branch JOIN",
                   node_alias, graph_rel.alias);

        let column_name = node_schema
            .node_id
            .id
            .columns()
            .first()
            .map(|s| s.to_string())
            .unwrap_or_else(|| "id".to_string());

        return Ok(NodeAppearance {
            rel_alias: node_alias.to_string(),
            node_label: node_label.clone(),
            table_name: node_schema.table_name.clone(),
            database: node_schema.database.clone(),
            column_name,
            is_from_side,
            is_vlp: true,
        });
    }

    // Determine which column to use based on side
    let column_name = if is_from_side {
        rel_schema.from_id.to_string()
    } else {
        rel_schema.to_id.to_string()
    };

    // Determine actual table name (may be CTE)
    let (table_name, database) = if let LogicalPlan::Cte(cte) = graph_rel.center.as_ref() {
        log::info!(
            "🔍 NodeAppearance: REL '{}' wrapped in CTE '{}' - using CTE name without database",
            graph_rel.alias,
            cte.name
        );
        (cte.name.clone(), String::new())
    } else if let Some(labels) = &graph_rel.labels {
        if labels.len() > 1 {
            let cte_name = format!(
                "rel_{}_{}",
                graph_rel.left_connection, graph_rel.right_connection
            );
            log::info!(
                "🔍 NodeAppearance: REL '{}' has {} labels - using multi-variant CTE: '{}'",
                graph_rel.alias,
                labels.len(),
                cte_name
            );
            (cte_name, String::new())
        } else {
            (rel_schema.table_name.clone(), rel_schema.database.clone())
        }
    } else {
        (rel_schema.table_name.clone(), rel_schema.database.clone())
    };

    Ok(NodeAppearance {
        rel_alias: graph_rel.alias.clone(),
        node_label: node_label.clone(),
        table_name,
        database,
        column_name,
        is_from_side,
        is_vlp: false,
    })
}

/// Generate a cross-branch JOIN between two GraphRels that share a node.
fn generate_cross_branch_join(
    node_alias: &str,
    current_appearance: &NodeAppearance,
    prev_appearance: &NodeAppearance,
    collected_graph_joins: &mut Vec<Join>,
) -> Result<(), AnalyzerError> {
    log::debug!(
        "   🔗 Generating cross-branch JOIN for node '{}': {} ({}.{}) ↔ {} ({}.{})",
        node_alias,
        prev_appearance.rel_alias,
        prev_appearance.table_name,
        prev_appearance.column_name,
        current_appearance.rel_alias,
        current_appearance.table_name,
        current_appearance.column_name,
    );

    // Skip if both GraphRels use the SAME table (coupled edges)
    let same_table = prev_appearance.database == current_appearance.database
        && prev_appearance.table_name == current_appearance.table_name;

    if same_table {
        log::debug!(
            "   ⏭️  Skipping cross-branch JOIN: both GraphRels use same table {}.{}",
            prev_appearance.database,
            prev_appearance.table_name
        );
        return Ok(());
    }

    // Create JOIN
    let table_name = if prev_appearance.database.is_empty() {
        prev_appearance.table_name.clone()
    } else {
        format!(
            "{}.{}",
            prev_appearance.database, prev_appearance.table_name
        )
    };

    let join = helpers::JoinBuilder::new(&table_name, &prev_appearance.rel_alias)
        .add_condition(
            &current_appearance.rel_alias,
            &current_appearance.column_name,
            &prev_appearance.rel_alias,
            &prev_appearance.column_name,
        )
        .build();

    helpers::push_join_if_not_duplicate(collected_graph_joins, join);

    crate::debug_print!(
        "       ✅ Generated: {} JOIN {} ON {}.{} = {}.{}",
        current_appearance.rel_alias,
        prev_appearance.rel_alias,
        current_appearance.rel_alias,
        current_appearance.column_name,
        prev_appearance.rel_alias,
        prev_appearance.column_name,
    );

    Ok(())
}
