/// Alias resolution for translating graph concepts to SQL table references
/// 
/// This module handles the translation layer between LogicalPlan (graph concepts)
/// and RenderPlan (pure SQL concepts). It builds a context that maps Cypher aliases
/// to their corresponding SQL table references, with special handling for:
/// - Denormalized nodes (nodes that don't have their own table)
/// - Multi-hop patterns (same node alias in different relationship contexts)
/// - Property mappings (Cypher property name → SQL column name)

use std::collections::HashMap;
use std::sync::Arc;
use crate::query_planner::logical_plan::LogicalPlan;
use crate::query_planner::logical_expr::{LogicalExpr, TableAlias};

/// Position of a denormalized node in a relationship pattern
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NodePosition {
    From,
    To,
}

/// Information about how to resolve a Cypher alias to SQL
#[derive(Debug, Clone)]
pub enum AliasResolution {
    /// Standard node/relationship with its own table
    StandardTable {
        /// The SQL table alias to use
        table_alias: String,
    },
    
    /// Denormalized node (properties stored in edge table)
    DenormalizedNode {
        /// The relationship alias that contains this node's data
        relationship_alias: String,
        /// Whether this is the FROM or TO node in the relationship
        position: NodePosition,
    },
}

/// Context for resolving Cypher aliases during LogicalPlan → RenderPlan translation
#[derive(Debug, Clone, Default)]
pub struct AliasResolverContext {
    /// Maps Cypher alias → how to resolve it in SQL
    resolutions: HashMap<String, AliasResolution>,
    /// For coupled edges: maps table name → unified alias to use
    coupled_edge_aliases: HashMap<String, String>,
}

impl AliasResolverContext {
    /// Create a new empty context
    pub fn new() -> Self {
        Self {
            resolutions: HashMap::new(),
            coupled_edge_aliases: HashMap::new(),
        }
    }
    
    /// Build context by analyzing a LogicalPlan tree
    pub fn from_logical_plan(plan: &LogicalPlan) -> Self {
        let mut context = Self::new();
        
        // First pass: detect coupled edges (multiple relationships on same table)
        context.detect_coupled_edges(plan);
        
        // Second pass: analyze plan and register aliases
        context.analyze_plan(plan);
        context
    }
    
    /// Get the SQL table alias for a Cypher alias
    pub fn get_table_alias(&self, cypher_alias: &str) -> Option<&str> {
        // First check if this alias has a coupled edge override
        if let Some(unified) = self.coupled_edge_aliases.get(cypher_alias) {
            return Some(unified.as_str());
        }
        
        match self.resolutions.get(cypher_alias)? {
            AliasResolution::StandardTable { table_alias } => {
                // Check if this table_alias has a coupled edge override
                self.coupled_edge_aliases.get(table_alias)
                    .map(|s| s.as_str())
                    .or(Some(table_alias))
            },
            AliasResolution::DenormalizedNode { relationship_alias, .. } => {
                Some(relationship_alias)
            }
        }
    }
    
    /// Get the resolution details for a Cypher alias
    pub fn get_resolution(&self, cypher_alias: &str) -> Option<&AliasResolution> {
        self.resolutions.get(cypher_alias)
    }
    
    /// Check if an alias refers to a denormalized node
    pub fn is_denormalized(&self, cypher_alias: &str) -> bool {
        matches!(
            self.resolutions.get(cypher_alias),
            Some(AliasResolution::DenormalizedNode { .. })
        )
    }
    
    /// Transform a LogicalPlan to rewrite denormalized node aliases to relationship aliases
    /// This should be called BEFORE converting to RenderPlan
    pub fn transform_plan(&self, plan: LogicalPlan) -> LogicalPlan {
        use crate::query_planner::logical_plan::*;
        
        match plan {
            LogicalPlan::Projection(mut proj) => {
                // Rewrite property access in projection items
                for item in &mut proj.items {
                    item.expression = self.transform_expr(item.expression.clone());
                }
                proj.input = Arc::new(self.transform_plan(proj.input.as_ref().clone()));
                LogicalPlan::Projection(proj)
            }
            
            LogicalPlan::Filter(mut filter) => {
                filter.predicate = self.transform_expr(filter.predicate);
                filter.input = Arc::new(self.transform_plan(filter.input.as_ref().clone()));
                LogicalPlan::Filter(filter)
            }
            
            LogicalPlan::GroupBy(mut group_by) => {
                // Rewrite expressions in GROUP BY
                group_by.expressions = group_by.expressions.into_iter()
                    .map(|e| self.transform_expr(e))
                    .collect();
                
                // Rewrite HAVING clause if present
                if let Some(having) = group_by.having_clause {
                    group_by.having_clause = Some(self.transform_expr(having));
                }
                
                group_by.input = Arc::new(self.transform_plan(group_by.input.as_ref().clone()));
                LogicalPlan::GroupBy(group_by)
            }
            
            LogicalPlan::OrderBy(mut order_by) => {
                // Rewrite expressions in ORDER BY
                for item in &mut order_by.items {
                    item.expression = self.transform_expr(item.expression.clone());
                }
                order_by.input = Arc::new(self.transform_plan(order_by.input.as_ref().clone()));
                LogicalPlan::OrderBy(order_by)
            }
            
            LogicalPlan::GraphRel(mut rel) => {
                rel.left = Arc::new(self.transform_plan(rel.left.as_ref().clone()));
                rel.center = Arc::new(self.transform_plan(rel.center.as_ref().clone()));
                rel.right = Arc::new(self.transform_plan(rel.right.as_ref().clone()));
                
                // Transform WHERE predicate if present (this is the correct location for filters)
                if let Some(predicate) = rel.where_predicate {
                    rel.where_predicate = Some(self.transform_expr(predicate));
                }
                
                LogicalPlan::GraphRel(rel)
            }
            
            LogicalPlan::GraphNode(mut node) => {
                node.input = Arc::new(self.transform_plan(node.input.as_ref().clone()));
                LogicalPlan::GraphNode(node)
            }
            
            LogicalPlan::GraphJoins(mut joins) => {
                joins.input = Arc::new(self.transform_plan(joins.input.as_ref().clone()));
                LogicalPlan::GraphJoins(joins)
            }
            
            LogicalPlan::Limit(mut limit) => {
                limit.input = Arc::new(self.transform_plan(limit.input.as_ref().clone()));
                LogicalPlan::Limit(limit)
            }
            
            LogicalPlan::Skip(mut skip) => {
                skip.input = Arc::new(self.transform_plan(skip.input.as_ref().clone()));
                LogicalPlan::Skip(skip)
            }
            
            LogicalPlan::Cte(mut cte) => {
                cte.input = Arc::new(self.transform_plan(cte.input.as_ref().clone()));
                LogicalPlan::Cte(cte)
            }
            
            LogicalPlan::Union(mut union) => {
                union.inputs = union.inputs.into_iter()
                    .map(|input| Arc::new(self.transform_plan(input.as_ref().clone())))
                    .collect();
                LogicalPlan::Union(union)
            }
            
            LogicalPlan::ViewScan(scan) => {
                // Don't clear view_filter - it contains edge property filters that we need.
                // Deduplication of node filters happens during rendering in collect_graphrel_predicates().
                let transformed_filter = scan.view_filter.as_ref()
                    .map(|filter| self.transform_expr(filter.clone()));
                
                // Transform input if present
                let transformed_input = scan.input.as_ref()
                    .map(|input| Arc::new(self.transform_plan(input.as_ref().clone())));
                
                // Create new ViewScan with transformed values
                LogicalPlan::ViewScan(Arc::new(crate::query_planner::logical_plan::ViewScan {
                    view_filter: transformed_filter,
                    input: transformed_input,
                    ..scan.as_ref().clone()
                }))
            }
            
            // Leaf nodes - no transformation needed
            LogicalPlan::Empty => LogicalPlan::Empty,
            LogicalPlan::Scan(scan) => LogicalPlan::Scan(scan),
            LogicalPlan::PageRank(pr) => LogicalPlan::PageRank(pr),
            LogicalPlan::Unwind(u) => {
                let transformed_input = Arc::new(self.transform_plan((*u.input).clone()));
                LogicalPlan::Unwind(crate::query_planner::logical_plan::Unwind {
                    input: transformed_input,
                    expression: self.transform_expr(u.expression.clone()),
                    alias: u.alias.clone(),
                })
            }
            LogicalPlan::CartesianProduct(cp) => {
                let transformed_left = Arc::new(self.transform_plan((*cp.left).clone()));
                let transformed_right = Arc::new(self.transform_plan((*cp.right).clone()));
                LogicalPlan::CartesianProduct(crate::query_planner::logical_plan::CartesianProduct {
                    left: transformed_left,
                    right: transformed_right,
                    is_optional: cp.is_optional,
                    join_condition: cp.join_condition.clone(),
                })
            }
        }
    }
    
    /// Transform a LogicalExpr to rewrite denormalized node aliases
    fn transform_expr(&self, expr: LogicalExpr) -> LogicalExpr {
        match expr {
            LogicalExpr::PropertyAccessExp(mut prop) => {
                // Check if this table alias refers to a denormalized node
                if let Some(sql_alias) = self.get_table_alias(&prop.table_alias.0) {
                    // Rewrite to use the SQL table alias (relationship alias for denormalized)
                    prop.table_alias = TableAlias(sql_alias.to_string());
                }
                LogicalExpr::PropertyAccessExp(prop)
            }
            
            LogicalExpr::AggregateFnCall(mut agg) => {
                agg.args = agg.args.into_iter()
                    .map(|a| self.transform_expr(a))
                    .collect();
                LogicalExpr::AggregateFnCall(agg)
            }
            
            LogicalExpr::ScalarFnCall(mut fn_call) => {
                fn_call.args = fn_call.args.into_iter()
                    .map(|a| self.transform_expr(a))
                    .collect();
                LogicalExpr::ScalarFnCall(fn_call)
            }
            
            LogicalExpr::OperatorApplicationExp(mut op) => {
                op.operands = op.operands.into_iter()
                    .map(|o| self.transform_expr(o))
                    .collect();
                LogicalExpr::OperatorApplicationExp(op)
            }
            
            LogicalExpr::List(list) => {
                LogicalExpr::List(
                    list.into_iter()
                        .map(|e| self.transform_expr(e))
                        .collect()
                )
            }
            
            LogicalExpr::Case(mut case) => {
                if let Some(expr) = case.expr {
                    case.expr = Some(Box::new(self.transform_expr(*expr)));
                }
                case.when_then = case.when_then.into_iter()
                    .map(|(when, then)| (self.transform_expr(when), self.transform_expr(then)))
                    .collect();
                if let Some(else_expr) = case.else_expr {
                    case.else_expr = Some(Box::new(self.transform_expr(*else_expr)));
                }
                LogicalExpr::Case(case)
            }
            
            LogicalExpr::InSubquery(mut subq) => {
                subq.expr = Box::new(self.transform_expr(*subq.expr));
                // Note: subplan would need schema to transform, skip for now
                LogicalExpr::InSubquery(subq)
            }
            
            LogicalExpr::ExistsSubquery(subq) => {
                // EXISTS subqueries have their own subplan that doesn't need alias transformation
                LogicalExpr::ExistsSubquery(subq)
            }
            
            // These don't contain PropertyAccess
            LogicalExpr::Literal(_) |
            LogicalExpr::Raw(_) |
            LogicalExpr::Star |
            LogicalExpr::TableAlias(_) |
            LogicalExpr::ColumnAlias(_) |
            LogicalExpr::Column(_) |
            LogicalExpr::Parameter(_) |
            LogicalExpr::Operator(_) |
            LogicalExpr::PathPattern(_) => expr,
        }
    }
    
    /// Register a standard table alias
    fn register_standard(&mut self, cypher_alias: String, table_alias: String) {
        self.resolutions.insert(
            cypher_alias,
            AliasResolution::StandardTable { table_alias },
        );
    }
    
    /// Register a denormalized node
    fn register_denormalized(
        &mut self,
        node_alias: String,
        relationship_alias: String,
        position: NodePosition,
    ) {
        // If this table has a coupled edge alias override, use that instead
        // (This happens when multiple edges share the same table)
        let actual_alias = if let Some(unified_alias) = self.coupled_edge_aliases.get(&relationship_alias) {
            log::debug!("Coupled edge: Using unified alias {} instead of {} for node {}", 
                     unified_alias, relationship_alias, node_alias);
            unified_alias.clone()
        } else {
            relationship_alias
        };
        
        self.resolutions.insert(
            node_alias,
            AliasResolution::DenormalizedNode {
                relationship_alias: actual_alias,
                position,
            },
        );
    }
    
    /// Detect coupled edges (multiple relationships sharing the same table)
    /// When detected, all nodes should use the same (first) relationship's alias
    /// 
    /// IMPORTANT: Only truly "coupled" edges should be unified - these are DIFFERENT
    /// edge types that share the same underlying table (e.g., DNS REQUESTED + RESOLVED_TO).
    /// Multi-hop on the SAME edge type (e.g., FLIGHT -> FLIGHT) is NOT coupled and each
    /// edge instance needs its own alias for proper JOIN generation.
    fn detect_coupled_edges(&mut self, plan: &LogicalPlan) {
        // Collect table_name -> [(alias, labels)]
        let mut edge_info: HashMap<String, Vec<(String, Option<Vec<String>>)>> = HashMap::new();
        
        self.collect_edge_tables_with_labels(plan, &mut edge_info);
        
        log::debug!("Coupled edge detection - edge info: {:?}", edge_info);
        
        // For tables with multiple edges, check if they have DIFFERENT edge types
        // Only unify aliases for truly coupled edges (different types on same table)
        for (table_name, entries) in edge_info {
            if entries.len() > 1 {
                // Check if all entries have the same edge type
                let first_labels = &entries[0].1;
                let all_same_type = entries.iter().all(|(_, labels)| labels == first_labels);
                
                if all_same_type {
                    // MULTI-HOP on same edge type - do NOT unify aliases
                    // Each edge instance needs its own alias for proper JOINs
                    log::debug!("Multi-hop detected on table {}: {} edges of same type {:?} - keeping separate aliases",
                             table_name, entries.len(), first_labels);
                    continue;
                }
                
                // Different edge types on same table - truly coupled, unify aliases
                let first_alias = entries[0].0.clone();
                log::info!("Coupled edges detected: Table {} has {} different edge types, unifying aliases to {}", 
                         table_name, entries.len(), first_alias);
                for (alias, _) in entries.iter().skip(1) {
                    self.coupled_edge_aliases.insert(alias.clone(), first_alias.clone());
                }
                // Also need to map the edge alias itself (not just node aliases)
                // The first alias maps to itself, others map to first
                for (alias, _) in &entries {
                    self.coupled_edge_aliases.insert(alias.clone(), first_alias.clone());
                }
            }
        }
    }
    
    /// Collect all edge tables, their aliases, and their labels from the plan
    fn collect_edge_tables_with_labels(&self, plan: &LogicalPlan, edge_info: &mut HashMap<String, Vec<(String, Option<Vec<String>>)>>) {
        match plan {
            LogicalPlan::GraphRel(rel) => {
                // Extract table name from ViewScan and labels from GraphRel
                if let LogicalPlan::ViewScan(scan) = rel.center.as_ref() {
                    edge_info.entry(scan.source_table.clone())
                        .or_default()
                        .push((rel.alias.clone(), rel.labels.clone()));
                }
                
                // Recurse into nested GraphRels
                self.collect_edge_tables_with_labels(&rel.left, edge_info);
                self.collect_edge_tables_with_labels(&rel.right, edge_info);
            }
            
            LogicalPlan::GraphNode(node) => {
                self.collect_edge_tables_with_labels(&node.input, edge_info);
            }
            
            LogicalPlan::Projection(proj) => {
                self.collect_edge_tables_with_labels(&proj.input, edge_info);
            }
            
            LogicalPlan::Filter(filter) => {
                self.collect_edge_tables_with_labels(&filter.input, edge_info);
            }
            
            LogicalPlan::GraphJoins(joins) => {
                self.collect_edge_tables_with_labels(&joins.input, edge_info);
            }
            
            LogicalPlan::OrderBy(ob) => {
                self.collect_edge_tables_with_labels(&ob.input, edge_info);
            }
            
            LogicalPlan::Skip(skip) => {
                self.collect_edge_tables_with_labels(&skip.input, edge_info);
            }
            
            LogicalPlan::Limit(limit) => {
                self.collect_edge_tables_with_labels(&limit.input, edge_info);
            }
            
            LogicalPlan::GroupBy(gb) => {
                self.collect_edge_tables_with_labels(&gb.input, edge_info);
            }
            
            LogicalPlan::Unwind(u) => {
                self.collect_edge_tables_with_labels(&u.input, edge_info);
            }
            
            _ => {}
        }
    }
    
    /// Analyze a LogicalPlan tree and build resolutions
    fn analyze_plan(&mut self, plan: &LogicalPlan) {
        match plan {
            LogicalPlan::GraphNode(node) => {
                // Standard node with its own table (unless denormalized)
                if !node.is_denormalized {
                    self.register_standard(node.alias.clone(), node.alias.clone());
                }
                // Note: Denormalized nodes are handled in GraphRel
                self.analyze_plan(&node.input);
            }
            
            LogicalPlan::GraphRel(rel) => {
                // Register the relationship itself
                self.register_standard(rel.alias.clone(), rel.alias.clone());
                
                // Check if the edge has from_node_properties or to_node_properties
                // If so, those nodes get their properties from the edge table
                let (has_from_props, has_to_props) = if let LogicalPlan::ViewScan(scan) = rel.center.as_ref() {
                    (scan.from_node_properties.is_some(), scan.to_node_properties.is_some())
                } else {
                    (false, false)
                };
                
                // Check if left node is denormalized OR has properties from edge
                if let LogicalPlan::GraphNode(left_node) = rel.left.as_ref() {
                    if left_node.is_denormalized || has_from_props {
                        self.register_denormalized(
                            left_node.alias.clone(),
                            rel.alias.clone(),
                            NodePosition::From,
                        );
                    }
                }
                
                // Check if right node is denormalized OR has properties from edge  
                if let LogicalPlan::GraphNode(right_node) = rel.right.as_ref() {
                    if right_node.is_denormalized || has_to_props {
                        self.register_denormalized(
                            right_node.alias.clone(),
                            rel.alias.clone(),
                            NodePosition::To,
                        );
                    }
                }
                
                // Recursively analyze children
                self.analyze_plan(&rel.left);
                self.analyze_plan(&rel.center);
                self.analyze_plan(&rel.right);
            }
            
            LogicalPlan::ViewScan(_) => {
                // ViewScans are leaf nodes, nothing to analyze
            }
            
            LogicalPlan::Scan(_) => {
                // Scans are leaf nodes
            }
            
            LogicalPlan::Empty => {
                // Empty plans have nothing to analyze
            }
            
            LogicalPlan::Filter(filter) => {
                self.analyze_plan(&filter.input);
            }
            
            LogicalPlan::Projection(proj) => {
                self.analyze_plan(&proj.input);
            }
            
            LogicalPlan::GroupBy(group_by) => {
                self.analyze_plan(&group_by.input);
            }
            
            LogicalPlan::OrderBy(order_by) => {
                self.analyze_plan(&order_by.input);
            }
            
            LogicalPlan::Skip(skip) => {
                self.analyze_plan(&skip.input);
            }
            
            LogicalPlan::Limit(limit) => {
                self.analyze_plan(&limit.input);
            }
            
            LogicalPlan::Cte(cte) => {
                self.analyze_plan(&cte.input);
            }
            
            LogicalPlan::GraphJoins(joins) => {
                self.analyze_plan(&joins.input);
            }
            
            LogicalPlan::Union(union) => {
                for input in &union.inputs {
                    self.analyze_plan(input);
                }
            }
            
            LogicalPlan::PageRank(_) => {
                // PageRank is handled specially
            }
            
            LogicalPlan::Unwind(u) => {
                self.analyze_plan(&u.input);
            }
            LogicalPlan::CartesianProduct(cp) => {
                self.analyze_plan(&cp.left);
                self.analyze_plan(&cp.right);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_planner::logical_plan::{GraphNode, GraphRel};
    use std::sync::Arc;
    
    #[test]
    fn test_standard_node_resolution() {
        let node = LogicalPlan::GraphNode(GraphNode {
            input: Arc::new(LogicalPlan::Empty),
            alias: "u".to_string(),
            label: Some("User".to_string()),
            is_denormalized: false,
        });
        
        let context = AliasResolverContext::from_logical_plan(&node);
        
        assert_eq!(context.get_table_alias("u"), Some("u"));
        assert!(!context.is_denormalized("u"));
    }
    
    #[test]
    fn test_denormalized_node_resolution() {
        use crate::query_planner::logical_expr::Direction;
        
        let left_node = LogicalPlan::GraphNode(GraphNode {
            input: Arc::new(LogicalPlan::Empty),
            alias: "a".to_string(),
            label: Some("Airport".to_string()),
            is_denormalized: true,
        });
        
        let right_node = LogicalPlan::GraphNode(GraphNode {
            input: Arc::new(LogicalPlan::Empty),
            alias: "b".to_string(),
            label: Some("Airport".to_string()),
            is_denormalized: true,
        });
        
        let graph_rel = LogicalPlan::GraphRel(GraphRel {
            left: Arc::new(left_node),
            center: Arc::new(LogicalPlan::Empty),
            right: Arc::new(right_node),
            alias: "f".to_string(),
            direction: Direction::Outgoing,
            left_connection: "a".to_string(),
            right_connection: "b".to_string(),
            is_rel_anchor: false,
            variable_length: None,
            shortest_path_mode: None,
            path_variable: None,
            where_predicate: None,
            labels: Some(vec!["FLIGHT".to_string()]),
            is_optional: None,
            anchor_connection: None,
        });
        
        let context = AliasResolverContext::from_logical_plan(&graph_rel);
        
        // Both denormalized nodes should resolve to the relationship alias
        assert_eq!(context.get_table_alias("a"), Some("f"));
        assert_eq!(context.get_table_alias("b"), Some("f"));
        assert!(context.is_denormalized("a"));
        assert!(context.is_denormalized("b"));
        
        // Check positions
        match context.get_resolution("a") {
            Some(AliasResolution::DenormalizedNode { position, .. }) => {
                assert_eq!(*position, NodePosition::From);
            }
            _ => panic!("Expected DenormalizedNode resolution"),
        }
        
        match context.get_resolution("b") {
            Some(AliasResolution::DenormalizedNode { position, .. }) => {
                assert_eq!(*position, NodePosition::To);
            }
            _ => panic!("Expected DenormalizedNode resolution"),
        }
    }
}
