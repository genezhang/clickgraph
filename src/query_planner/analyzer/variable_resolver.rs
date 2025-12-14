//! Variable Resolution Analyzer Pass
//!
//! **Purpose**: Resolve all variable references (TableAlias) to their sources during analysis.
//! This makes the renderer "dumb" - it only needs to emit SQL for fully-resolved expressions.
//!
//! **Problem This Solves**:
//! ```cypher
//! MATCH (p:Person)-[:KNOWS]-(friend) WITH count(friend) as cnt RETURN cnt
//! ```
//!
//! Before this pass:
//! - RETURN has `ProjectionItem { expr: TableAlias("cnt") }`
//! - Renderer doesn't know `cnt` is from WITH, expands to all properties
//! - Generates wrong SQL: `SELECT cnt."friend.id", cnt."p.id", ...` (16 columns!)
//!
//! After this pass:
//! - RETURN has `ProjectionItem { expr: PropertyAccessExp("cnt_cte", "cnt") }`
//! - Renderer just emits: `SELECT cnt_cte."cnt"`
//! - Correct SQL!
//!
//! **Architecture**:
//! - Runs AFTER `WithScopeSplitter` (which marks scope boundaries)
//! - Runs BEFORE `GraphJoinInference` (which needs resolved variables)
//! - Traverses plan tree, maintaining scope context stack
//! - Resolves TableAlias references to:
//!   - CTE column references (if from previous WITH)
//!   - Schema entities (if from current scope MATCH)
//!   - Parameters
//!
//! **Scope Rules**:
//! 1. WITH creates a scope boundary
//! 2. Variables visible in a scope:
//!    - Exported aliases from previous WITH in same chain
//!    - Schema entities from current scope's MATCH patterns
//!    - Global parameters
//! 3. Variables are resolved to the NEAREST enclosing scope
//!
//! **Example**:
//! ```cypher
//! MATCH (a:Person)
//! WITH a.name AS name
//! MATCH (a)-[:KNOWS]->(b)
//! RETURN name, b
//! ```
//!
//! Scope 1 (before WITH): {a: GraphNode}
//! Scope 2 (after WITH): {name: CteColumn("with_name_cte", "name")}
//! Scope 3 (after second MATCH): {name: CteColumn, b: GraphNode}
//! - `name` resolves to CTE column
//! - `b` resolves to GraphNode from current MATCH

use std::collections::HashMap;
use std::sync::Arc;

use crate::{
    graph_catalog::expression_parser::PropertyValue,
    query_planner::{
        analyzer::{analyzer_pass::AnalyzerPass, errors::AnalyzerError},
        logical_expr::{ColumnAlias, LogicalExpr, PropertyAccess, TableAlias},
        logical_plan::{LogicalPlan, ProjectionItem, WithClause},
        plan_ctx::PlanCtx,
        transformed::Transformed,
    },
};

/// Tracks variable sources within a scope
#[derive(Debug, Clone)]
pub enum VarSource {
    /// Variable comes from a previous WITH clause (CTE column)
    /// Example: `WITH count(x) as cnt` → cnt maps to CteColumn
    CteColumn {
        /// Name of the CTE (e.g., "with_cnt_cte_1")
        cte_name: String,
        /// Column name in the CTE (e.g., "cnt")
        column_name: String,
    },

    /// Variable is a schema entity from current scope MATCH
    /// Example: `MATCH (a:Person)` → a maps to SchemaEntity
    SchemaEntity {
        /// Alias in the query (e.g., "a")
        alias: String,
        /// Whether it's a node or relationship
        entity_type: EntityType,
    },

    /// Variable is a query parameter
    /// Example: `$userId`
    Parameter { name: String },
}

#[derive(Debug, Clone, PartialEq)]
pub enum EntityType {
    Node,
    Relationship,
}

/// Scope context for variable resolution
///
/// Maintains a stack of visible variables as we traverse the plan tree.
/// Each WITH clause creates a new scope layer.
#[derive(Debug, Clone)]
pub struct ScopeContext {
    /// Variables visible in this scope
    /// Key: variable name (e.g., "cnt", "a", "friend")
    /// Value: source of the variable
    pub visible_vars: HashMap<String, VarSource>,

    /// Parent scope (for nested WITH)
    /// None for root scope
    pub parent: Option<Box<ScopeContext>>,

    /// Current CTE name (if we're inside a WITH scope)
    /// Used to generate qualified column references
    pub current_cte_name: Option<String>,
}

impl ScopeContext {
    /// Create root scope with no variables
    pub fn root() -> Self {
        ScopeContext {
            visible_vars: HashMap::new(),
            parent: None,
            current_cte_name: None,
        }
    }

    /// Create child scope inheriting from parent
    pub fn with_parent(parent: ScopeContext, cte_name: Option<String>) -> Self {
        ScopeContext {
            visible_vars: HashMap::new(),
            parent: Some(Box::new(parent)),
            current_cte_name: cte_name,
        }
    }

    /// Add a variable to this scope
    pub fn add_variable(&mut self, name: String, source: VarSource) {
        log::debug!("🔍 ScopeContext: Adding variable '{}' → {:?}", name, source);
        self.visible_vars.insert(name, source);
    }

    /// Look up a variable in this scope or parent scopes
    pub fn lookup(&self, name: &str) -> Option<&VarSource> {
        // Check current scope first
        if let Some(source) = self.visible_vars.get(name) {
            return Some(source);
        }

        // Check parent scopes (recursive)
        if let Some(ref parent) = self.parent {
            return parent.lookup(name);
        }

        None
    }

}

/// Variable Resolution Analyzer Pass
pub struct VariableResolver {
    /// Counter for generating unique CTE names
    cte_counter: std::cell::RefCell<usize>,
}

impl VariableResolver {
    pub fn new() -> Self {
        VariableResolver {
            cte_counter: std::cell::RefCell::new(1),
        }
    }

    /// Generate unique CTE name for a WITH clause
    fn generate_cte_name(&self, alias: &str) -> String {
        let mut counter = self.cte_counter.borrow_mut();
        let name = format!("with_{}_cte_{}", alias, *counter);
        *counter += 1;
        name
    }

    /// Resolve variables in the plan tree
    ///
    /// This is the main recursive function that:
    /// 1. Maintains scope context as we traverse
    /// 2. Resolves TableAlias to proper sources
    /// 3. Populates cte_references in WithClause
    fn resolve(
        &self,
        plan: Arc<LogicalPlan>,
        scope: &ScopeContext,
    ) -> Result<Transformed<Arc<LogicalPlan>>, AnalyzerError> {
        match plan.as_ref() {
            LogicalPlan::WithClause(wc) => {
                log::info!(
                    "🔍 VariableResolver: Processing WITH clause, {} exported aliases",
                    wc.exported_aliases.len()
                );

                // Step 1: Resolve variables in INPUT using PARENT scope
                let input_resolved = self.resolve(wc.input.clone(), scope)?;
                let new_input = input_resolved.get_plan();

                // Step 2: Generate CTE name for this WITH
                // Use first exported alias as base name
                let alias_base = wc
                    .exported_aliases
                    .first()
                    .map(|s| s.as_str())
                    .unwrap_or("unknown");
                let cte_name = self.generate_cte_name(alias_base);

                log::info!("🔍 VariableResolver: Generated CTE name '{}'", cte_name);

                // Step 3: Create NEW scope for downstream
                // Exported aliases from this WITH are visible downstream
                let mut new_scope = ScopeContext::with_parent(scope.clone(), Some(cte_name.clone()));

                for alias in &wc.exported_aliases {
                    new_scope.add_variable(
                        alias.clone(),
                        VarSource::CteColumn {
                            cte_name: cte_name.clone(),
                            column_name: alias.clone(),
                        },
                    );
                }

                // Step 4: Resolve WITH items expressions (use parent scope, not new scope!)
                // The expressions in WITH items reference variables from BEFORE the WITH
                let resolved_items = wc
                    .items
                    .iter()
                    .map(|item| self.resolve_projection_item(item, scope))
                    .collect::<Result<Vec<_>, _>>()?;

                // Step 5: Build cte_references map for this WITH clause
                // This tells downstream what CTEs we're referencing
                let mut cte_references = HashMap::new();
                for alias in &wc.exported_aliases {
                    cte_references.insert(alias.clone(), cte_name.clone());
                }

                // Step 6: Return new WithClause with resolved data
                let new_wc = WithClause {
                    input: new_input,
                    items: resolved_items,
                    distinct: wc.distinct,
                    order_by: wc.order_by.clone(),
                    skip: wc.skip,
                    limit: wc.limit,
                    exported_aliases: wc.exported_aliases.clone(),
                    where_clause: wc.where_clause.clone(),
                    cte_references,
                };

                log::info!(
                    "🔍 VariableResolver: Completed WITH resolution, {} cte_references",
                    new_wc.cte_references.len()
                );

                Ok(Transformed::Yes(Arc::new(LogicalPlan::WithClause(new_wc))))
            }

            LogicalPlan::Projection(proj) => {
                log::info!(
                    "🔍 VariableResolver: Processing Projection, {} items",
                    proj.items.len()
                );

                // CRITICAL FIX: Check if input is WithClause
                // If so, we need to use the scope that WithClause creates for resolving projection items
                let (input_changed, new_input, projection_scope) = if let LogicalPlan::WithClause(wc) = proj.input.as_ref() {
                    log::info!("🔍 VariableResolver: Projection input is WithClause - will use exported scope");

                    // First, resolve the WithClause input
                    let input_resolved = self.resolve(proj.input.clone(), scope)?;
                    let input_changed = input_resolved.is_yes();
                    let new_input = input_resolved.get_plan();

                    // Extract the WithClause from resolved input to get its exported aliases
                    if let LogicalPlan::WithClause(resolved_wc) = new_input.as_ref() {
                        // Create a NEW scope with the exported aliases from WITH
                        let mut with_scope = scope.clone();

                        log::info!("🔍 VariableResolver: Creating projection scope with {} exported aliases from WITH",
                                   resolved_wc.exported_aliases.len());
                        log::info!("🔍 VariableResolver: WithClause has {} cte_references",
                                   resolved_wc.cte_references.len());

                        // Use the CTE references that were already populated by WithClause
                        for alias in &resolved_wc.exported_aliases {
                            if let Some(cte_name) = resolved_wc.cte_references.get(alias) {
                                log::info!("🔍 VariableResolver: Adding '{}' to projection scope as CTE column from '{}'",
                                           alias, cte_name);
                                with_scope.add_variable(
                                    alias.clone(),
                                    VarSource::CteColumn {
                                        cte_name: cte_name.clone(),
                                        column_name: alias.clone(),
                                    },
                                );
                            } else {
                                log::warn!("🔍 VariableResolver: Exported alias '{}' not found in cte_references!", alias);
                            }
                        }

                        (input_changed, new_input, with_scope)
                    } else {
                        // Shouldn't happen, but fallback to original scope
                        log::warn!("🔍 VariableResolver: Resolved input is not WithClause anymore!");
                        (input_changed, new_input, scope.clone())
                    }
                } else {
                    // Input is not WithClause, use current scope
                    let input_resolved = self.resolve(proj.input.clone(), scope)?;
                    let input_changed = input_resolved.is_yes();
                    let new_input = input_resolved.get_plan();
                    (input_changed, new_input, scope.clone())
                };

                // Resolve projection items using the appropriate scope
                // (either current scope or WITH's exported scope)
                let resolved_items = proj
                    .items
                    .iter()
                    .map(|item| self.resolve_projection_item(item, &projection_scope))
                    .collect::<Result<Vec<_>, _>>()?;

                // Check if anything changed
                let items_changed = resolved_items
                    .iter()
                    .zip(&proj.items)
                    .any(|(new, old)| !std::ptr::eq(new as *const _, old as *const _));

                if input_changed || items_changed {
                    let new_proj = crate::query_planner::logical_plan::Projection {
                        input: new_input,
                        items: resolved_items,
                        distinct: proj.distinct,
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Projection(
                        new_proj,
                    ))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::Filter(filter) => {
                log::debug!("🔍 VariableResolver: Processing Filter");

                // Resolve input first
                let input_resolved = self.resolve(filter.input.clone(), scope)?;
                let input_changed = input_resolved.is_yes();
                let new_input = input_resolved.get_plan();

                // Resolve filter predicate
                let resolved_predicate = self.resolve_expression(&filter.predicate, scope)?;

                let pred_changed = !std::ptr::eq(&resolved_predicate as *const _, &filter.predicate as *const _);

                if input_changed || pred_changed {
                    let new_filter = crate::query_planner::logical_plan::Filter {
                        input: new_input,
                        predicate: resolved_predicate,
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Filter(new_filter))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::OrderBy(order) => {
                log::debug!("🔍 VariableResolver: Processing OrderBy");

                let input_resolved = self.resolve(order.input.clone(), scope)?;
                let input_changed = input_resolved.is_yes();
                let new_input = input_resolved.get_plan();

                // Resolve order expressions
                let resolved_items = order
                    .items
                    .iter()
                    .map(|item| {
                        let resolved_expr = self.resolve_expression(&item.expression, scope)?;
                        Ok(crate::query_planner::logical_plan::OrderByItem {
                            expression: resolved_expr,
                            order: item.order.clone(),
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let items_changed = resolved_items
                    .iter()
                    .zip(&order.items)
                    .any(|(new, old)| !std::ptr::eq(new as *const _, old as *const _));

                if input_changed || items_changed {
                    let new_order = crate::query_planner::logical_plan::OrderBy {
                        input: new_input,
                        items: resolved_items,
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::OrderBy(new_order))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::GroupBy(gb) => {
                log::debug!("🔍 VariableResolver: Processing GroupBy");

                let input_resolved = self.resolve(gb.input.clone(), scope)?;
                let input_changed = input_resolved.is_yes();
                let new_input = input_resolved.get_plan();

                // Resolve group expressions
                let resolved_exprs = gb
                    .expressions
                    .iter()
                    .map(|expr| self.resolve_expression(expr, scope))
                    .collect::<Result<Vec<_>, _>>()?;

                // Resolve having clause if present
                let resolved_having = if let Some(ref having) = gb.having_clause {
                    Some(self.resolve_expression(having, scope)?)
                } else {
                    None
                };

                let exprs_changed = resolved_exprs
                    .iter()
                    .zip(&gb.expressions)
                    .any(|(new, old)| !std::ptr::eq(new as *const _, old as *const _));

                if input_changed || exprs_changed || resolved_having.is_some() {
                    let new_gb = crate::query_planner::logical_plan::GroupBy {
                        input: new_input,
                        expressions: resolved_exprs,
                        having_clause: resolved_having.or_else(|| gb.having_clause.clone()),
                        is_materialization_boundary: gb.is_materialization_boundary,
                        exposed_alias: gb.exposed_alias.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::GroupBy(new_gb))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::GraphNode(gn) => {
                log::debug!("🔍 VariableResolver: Found GraphNode with alias '{}'", gn.alias);

                // CRITICAL: Add this node to the scope!
                // This populates the scope with schema entities from MATCH
                // We need to do this in a new scope that includes this node
                let mut new_scope = scope.clone();
                new_scope.add_variable(
                    gn.alias.clone(),
                    VarSource::SchemaEntity {
                        alias: gn.alias.clone(),
                        entity_type: EntityType::Node,
                    },
                );

                // Recurse into input with updated scope
                let input_resolved = self.resolve(gn.input.clone(), &new_scope)?;

                if input_resolved.is_yes() {
                    let new_gn = crate::query_planner::logical_plan::GraphNode {
                        input: input_resolved.get_plan(),
                        alias: gn.alias.clone(),
                        label: gn.label.clone(),
                        is_denormalized: gn.is_denormalized,
            projected_columns: None,
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphNode(new_gn))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::GraphRel(rel) => {
                log::debug!("🔍 VariableResolver: Found GraphRel with alias '{}'", rel.alias);

                // Add relationship to scope
                let mut new_scope = scope.clone();
                new_scope.add_variable(
                    rel.alias.clone(),
                    VarSource::SchemaEntity {
                        alias: rel.alias.clone(),
                        entity_type: EntityType::Relationship,
                    },
                );

                // Recurse into left, center, right
                let left_resolved = self.resolve(rel.left.clone(), &new_scope)?;
                let center_resolved = self.resolve(rel.center.clone(), &new_scope)?;
                let right_resolved = self.resolve(rel.right.clone(), &new_scope)?;

                if left_resolved.is_yes() || center_resolved.is_yes() || right_resolved.is_yes() {
                    let new_rel = crate::query_planner::logical_plan::GraphRel {
                        left: left_resolved.get_plan(),
                        center: center_resolved.get_plan(),
                        right: right_resolved.get_plan(),
                        alias: rel.alias.clone(),
                        direction: rel.direction.clone(),
                        left_connection: rel.left_connection.clone(),
                        right_connection: rel.right_connection.clone(),
                        is_rel_anchor: rel.is_rel_anchor,
                        variable_length: rel.variable_length.clone(),
                        shortest_path_mode: rel.shortest_path_mode.clone(),
                        path_variable: rel.path_variable.clone(),
                        where_predicate: rel.where_predicate.clone(),
                        labels: rel.labels.clone(),
                        is_optional: rel.is_optional,
                        anchor_connection: rel.anchor_connection.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphRel(new_rel))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::GraphJoins(gj) => {
                log::debug!("🔍 VariableResolver: Processing GraphJoins with {} joins", gj.joins.len());

                // Build scope with all table aliases from joins
                let mut new_scope = scope.clone();
                for join in &gj.joins {
                    new_scope.add_variable(
                        join.table_alias.clone(),
                        VarSource::SchemaEntity {
                            alias: join.table_alias.clone(),
                            entity_type: EntityType::Node, // Assume node for now
                        },
                    );
                }

                // Recurse into input with updated scope
                let input_resolved = self.resolve(gj.input.clone(), &new_scope)?;

                if input_resolved.is_yes() {
                    let new_gj = crate::query_planner::logical_plan::GraphJoins {
                        input: input_resolved.get_plan(),
                        joins: gj.joins.clone(),
                        optional_aliases: gj.optional_aliases.clone(),
                        anchor_table: gj.anchor_table.clone(),
                        cte_references: gj.cte_references.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::GraphJoins(new_gj))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::Skip(skip) => {
                let input_resolved = self.resolve(skip.input.clone(), scope)?;
                if input_resolved.is_yes() {
                    let new_skip = crate::query_planner::logical_plan::Skip {
                        input: input_resolved.get_plan(),
                        count: skip.count,
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Skip(new_skip))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::Limit(limit) => {
                let input_resolved = self.resolve(limit.input.clone(), scope)?;
                if input_resolved.is_yes() {
                    let new_limit = crate::query_planner::logical_plan::Limit {
                        input: input_resolved.get_plan(),
                        count: limit.count,
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Limit(new_limit))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::Union(union) => {
                let mut any_transformed = false;
                let mut new_inputs = Vec::new();

                for input in &union.inputs {
                    let transformed = self.resolve(input.clone(), scope)?;
                    if transformed.is_yes() {
                        any_transformed = true;
                    }
                    new_inputs.push(transformed.get_plan());
                }

                if any_transformed {
                    let new_union = crate::query_planner::logical_plan::Union {
                        inputs: new_inputs,
                        union_type: union.union_type.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(LogicalPlan::Union(new_union))))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            LogicalPlan::CartesianProduct(cp) => {
                let left = self.resolve(cp.left.clone(), scope)?;
                let right = self.resolve(cp.right.clone(), scope)?;

                if left.is_yes() || right.is_yes() {
                    let new_cp = crate::query_planner::logical_plan::CartesianProduct {
                        left: left.get_plan(),
                        right: right.get_plan(),
                        is_optional: cp.is_optional,
                        join_condition: cp.join_condition.clone(),
                    };
                    Ok(Transformed::Yes(Arc::new(
                        LogicalPlan::CartesianProduct(new_cp),
                    )))
                } else {
                    Ok(Transformed::No(plan))
                }
            }

            // Terminal nodes - no recursion, no changes
            LogicalPlan::Empty
            | LogicalPlan::Scan(_)
            | LogicalPlan::ViewScan(_)
            | LogicalPlan::PageRank(_) => Ok(Transformed::No(plan)),

            // Other node types - TODO
            _ => {
                log::debug!(
                    "🔍 VariableResolver: Skipping node type {:?} (not yet implemented)",
                    std::mem::discriminant(plan.as_ref())
                );
                Ok(Transformed::No(plan))
            }
        }
    }

    /// Resolve a single projection item
    fn resolve_projection_item(
        &self,
        item: &ProjectionItem,
        scope: &ScopeContext,
    ) -> Result<ProjectionItem, AnalyzerError> {
        let resolved_expr = self.resolve_expression(&item.expression, scope)?;

        Ok(ProjectionItem {
            expression: resolved_expr,
            col_alias: item.col_alias.clone(),
        })
    }

    /// Resolve an expression
    ///
    /// This is where the magic happens: TableAlias("cnt") → PropertyAccessExp("cnt_cte", "cnt")
    fn resolve_expression(
        &self,
        expr: &LogicalExpr,
        scope: &ScopeContext,
    ) -> Result<LogicalExpr, AnalyzerError> {
        match expr {
            LogicalExpr::TableAlias(alias) => {
                log::debug!("🔍 VariableResolver: Resolving TableAlias '{}'", alias.0);

                // Look up the alias in scope
                match scope.lookup(&alias.0) {
                    Some(VarSource::CteColumn {
                        cte_name,
                        column_name,
                    }) => {
                        log::info!(
                            "✅ VariableResolver: Resolved '{}' → CTE column '{}.{}'",
                            alias.0,
                            cte_name,
                            column_name
                        );

                        // Transform TableAlias to PropertyAccessExp
                        // This is the key transformation that fixes the bug!
                        Ok(LogicalExpr::PropertyAccessExp(PropertyAccess {
                            table_alias: TableAlias(cte_name.clone()),
                            column: PropertyValue::Column(column_name.clone()),
                        }))
                    }

                    Some(VarSource::SchemaEntity { .. }) => {
                        // This is a schema entity (node/rel from MATCH)
                        // Leave as TableAlias - it will be expanded by renderer
                        log::debug!("🔍 VariableResolver: '{}' is schema entity, keeping as TableAlias", alias.0);
                        Ok(expr.clone())
                    }

                    Some(VarSource::Parameter { .. }) => {
                        // Parameter reference
                        log::debug!("🔍 VariableResolver: '{}' is parameter", alias.0);
                        Ok(expr.clone())
                    }

                    None => {
                        // Not found in scope - this might be OK if it's defined later
                        // or if it's a special case. Log warning but don't error.
                        log::warn!(
                            "⚠️ VariableResolver: TableAlias '{}' not found in scope (might be OK)",
                            alias.0
                        );
                        Ok(expr.clone())
                    }
                }
            }

            LogicalExpr::PropertyAccessExp(prop) => {
                // Property access like friend.firstName
                // The table_alias might need resolution (though usually it doesn't)
                // For now, leave unchanged - property mapping happens in FilterTagging
                Ok(expr.clone())
            }

            LogicalExpr::OperatorApplicationExp(op) => {
                // Operator: resolve all operands
                let resolved_operands = op
                    .operands
                    .iter()
                    .map(|operand| self.resolve_expression(operand, scope))
                    .collect::<Result<Vec<_>, _>>()?;

                let changed = resolved_operands
                    .iter()
                    .zip(&op.operands)
                    .any(|(new, old)| !std::ptr::eq(new as *const _, old as *const _));

                if changed {
                    use crate::query_planner::logical_expr::OperatorApplication;
                    Ok(LogicalExpr::OperatorApplicationExp(OperatorApplication {
                        operator: op.operator.clone(),
                        operands: resolved_operands,
                    }))
                } else {
                    Ok(expr.clone())
                }
            }

            LogicalExpr::AggregateFnCall(agg) => {
                // Aggregate function: resolve arguments
                let resolved_args = agg
                    .args
                    .iter()
                    .map(|arg| self.resolve_expression(arg, scope))
                    .collect::<Result<Vec<_>, _>>()?;

                // Check if anything changed
                let changed = resolved_args
                    .iter()
                    .zip(&agg.args)
                    .any(|(new, old)| !std::ptr::eq(new as *const _, old as *const _));

                if changed {
                    use crate::query_planner::logical_expr::AggregateFnCall;
                    Ok(LogicalExpr::AggregateFnCall(AggregateFnCall {
                        name: agg.name.clone(),
                        args: resolved_args,
                    }))
                } else {
                    Ok(expr.clone())
                }
            }

            LogicalExpr::ScalarFnCall(func) => {
                // Scalar function: resolve arguments
                let resolved_args = func
                    .args
                    .iter()
                    .map(|arg| self.resolve_expression(arg, scope))
                    .collect::<Result<Vec<_>, _>>()?;

                let changed = resolved_args
                    .iter()
                    .zip(&func.args)
                    .any(|(new, old)| !std::ptr::eq(new as *const _, old as *const _));

                if changed {
                    use crate::query_planner::logical_expr::ScalarFnCall;
                    Ok(LogicalExpr::ScalarFnCall(ScalarFnCall {
                        name: func.name.clone(),
                        args: resolved_args,
                    }))
                } else {
                    Ok(expr.clone())
                }
            }

            LogicalExpr::List(items) => {
                // List: resolve each element
                let resolved_items = items
                    .iter()
                    .map(|item| self.resolve_expression(item, scope))
                    .collect::<Result<Vec<_>, _>>()?;

                let changed = resolved_items
                    .iter()
                    .zip(items)
                    .any(|(new, old)| !std::ptr::eq(new as *const _, old as *const _));

                if changed {
                    Ok(LogicalExpr::List(resolved_items))
                } else {
                    Ok(expr.clone())
                }
            }

            LogicalExpr::Case(case_expr) => {
                // CASE expression: resolve condition, when/then branches, else branch
                let resolved_expr = if let Some(ref e) = case_expr.expr {
                    Some(Box::new(self.resolve_expression(e, scope)?))
                } else {
                    None
                };

                let resolved_when_then = case_expr
                    .when_then
                    .iter()
                    .map(|(when, then)| {
                        let resolved_when = self.resolve_expression(when, scope)?;
                        let resolved_then = self.resolve_expression(then, scope)?;
                        Ok((resolved_when, resolved_then))
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let resolved_else = if let Some(ref e) = case_expr.else_expr {
                    Some(Box::new(self.resolve_expression(e, scope)?))
                } else {
                    None
                };

                use crate::query_planner::logical_expr::LogicalCase;
                Ok(LogicalExpr::Case(LogicalCase {
                    expr: resolved_expr,
                    when_then: resolved_when_then,
                    else_expr: resolved_else,
                }))
            }

            // Leaf expressions - no recursion needed
            LogicalExpr::Literal(_)
            | LogicalExpr::Raw(_)
            | LogicalExpr::Star
            | LogicalExpr::ColumnAlias(_)
            | LogicalExpr::Column(_)
            | LogicalExpr::Parameter(_) => Ok(expr.clone()),

            // Other expressions - for now, leave unchanged
            // TODO: Handle PathPattern, InSubquery, ExistsSubquery, Reduce, etc.
            _ => {
                log::debug!(
                    "🔍 VariableResolver: Skipping expression type {:?} (not yet implemented)",
                    std::mem::discriminant(expr)
                );
                Ok(expr.clone())
            }
        }
    }
}

impl AnalyzerPass for VariableResolver {
    fn analyze(
        &self,
        logical_plan: Arc<LogicalPlan>,
        _plan_ctx: &mut PlanCtx,
    ) -> Result<Transformed<Arc<LogicalPlan>>, AnalyzerError> {
        log::info!("🔍 VariableResolver: Starting variable resolution");

        // Start with root scope (no variables)
        let root_scope = ScopeContext::root();

        // Resolve the entire plan tree
        let result = self.resolve(logical_plan, &root_scope)?;

        log::info!(
            "🔍 VariableResolver: Completed - transformed: {}",
            result.is_yes()
        );

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scope_context_lookup() {
        let mut root = ScopeContext::root();
        root.add_variable(
            "a".to_string(),
            VarSource::SchemaEntity {
                alias: "a".to_string(),
                entity_type: EntityType::Node,
            },
        );

        // Lookup in root scope
        assert!(root.lookup("a").is_some());
        assert!(root.lookup("b").is_none());

        // Child scope inherits parent variables
        let child = ScopeContext::with_parent(root.clone(), Some("cte1".to_string()));
        assert!(child.lookup("a").is_some());
    }

    #[test]
    fn test_cte_name_generation() {
        let resolver = VariableResolver::new();
        let name1 = resolver.generate_cte_name("cnt");
        let name2 = resolver.generate_cte_name("cnt");

        assert_eq!(name1, "with_cnt_cte_1");
        assert_eq!(name2, "with_cnt_cte_2");
    }
}
