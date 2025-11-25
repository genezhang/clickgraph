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
use crate::query_planner::logical_expr::{LogicalExpr, PropertyAccess as LogicalPropertyAccess, TableAlias};

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
}

impl AliasResolverContext {
    /// Create a new empty context
    pub fn new() -> Self {
        Self {
            resolutions: HashMap::new(),
        }
    }
    
    /// Build context by analyzing a LogicalPlan tree
    pub fn from_logical_plan(plan: &LogicalPlan) -> Self {
        let mut context = Self::new();
        context.analyze_plan(plan);
        context
    }
    
    /// Get the SQL table alias for a Cypher alias
    pub fn get_table_alias(&self, cypher_alias: &str) -> Option<&str> {
        match self.resolutions.get(cypher_alias)? {
            AliasResolution::StandardTable { table_alias } => Some(table_alias),
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
        self.resolutions.insert(
            node_alias,
            AliasResolution::DenormalizedNode {
                relationship_alias,
                position,
            },
        );
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
                
                // Check if left node is denormalized
                if let LogicalPlan::GraphNode(left_node) = rel.left.as_ref() {
                    if left_node.is_denormalized {
                        self.register_denormalized(
                            left_node.alias.clone(),
                            rel.alias.clone(),
                            NodePosition::From,
                        );
                    }
                }
                
                // Check if right node is denormalized
                if let LogicalPlan::GraphNode(right_node) = rel.right.as_ref() {
                    if right_node.is_denormalized {
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
        use crate::query_planner::logical_plan::Direction;
        
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
