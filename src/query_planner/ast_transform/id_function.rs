//! AST Transformation for id() Function
//!
//! Neo4j uses `id(node)` to reference nodes by integer ID. ClickGraph uses element_id strings.
//! This module transforms id() function calls in the AST before logical planning, using the
//! session's IdMapper to convert integer IDs back to element_ids and then to proper predicates.
//!
//! # Why AST-Level vs String-Level?
//!
//! String rewriting breaks for complex queries with UNION ALL because:
//! - Can't understand alias scoping (o becomes o_0, o_1 in UNION branches)
//! - ORDER BY references become invalid after UNION ALL
//! - No structural understanding of the query
//!
//! AST transformation happens before planning, so the planner sees clean predicates.
//!
//! # Transformations
//!
//! - `id(a) = 5` → `(a:Label AND a.id_property = 'value')`
//! - `id(a) IN [1, 2]` → `((a:L1 AND a.id = 'v1') OR (a:L2 AND a.id = 'v2'))`
//! - `NOT id(a) IN []` → `true` (tautology for empty exclusion list)
//! - `ORDER BY id(a)` → `ORDER BY a.id_property`
//!
//! # Memory Management
//!
//! This module uses a `StringArena` to allocate string references for AST nodes.
//! The arena is owned by the caller and dropped after query planning, ensuring
//! proper memory cleanup.
//! to `Cow<'a, str>` or `Arc<str>` would require extensive refactoring of the parser
//! and all downstream consumers. The leaked memory is small (< 1KB per typical query)
//! and acceptable for the current use case. Future optimization could use an arena
//! allocator (e.g., `bumpalo`) cleared after query execution.

use crate::{
    graph_catalog::element_id::parse_node_element_id,
    open_cypher_parser::ast::{
        Expression, FunctionCall, Literal, Operator, OperatorApplication, OrderByItem,
    },
    server::bolt_protocol::id_mapper::IdMapper,
};
use std::collections::{HashMap, HashSet};

/// Transforms id() function calls in Cypher AST using session's IdMapper
pub struct IdFunctionTransformer<'a> {
    arena: &'a crate::query_planner::ast_transform::StringArena,
    id_mapper: &'a IdMapper,
    schema: Option<&'a crate::graph_catalog::GraphSchema>,
    /// Extracted label constraints for UNION pruning (variable → set of labels)
    pub label_constraints: HashMap<String, HashSet<String>>,
    /// ID values grouped by variable and label: variable → label → [id_values]
    pub id_values_by_label: HashMap<String, HashMap<String, Vec<String>>>,
}

impl<'a> IdFunctionTransformer<'a> {
    pub fn new(
        arena: &'a crate::query_planner::ast_transform::StringArena,
        id_mapper: &'a IdMapper,
        schema: Option<&'a crate::graph_catalog::GraphSchema>,
    ) -> Self {
        Self {
            arena,
            id_mapper,
            schema,
            label_constraints: HashMap::new(),
            id_values_by_label: HashMap::new(),
        }
    }

    /// Get extracted label constraints for UNION pruning
    pub fn get_label_constraints(&self) -> HashMap<String, HashSet<String>> {
        self.label_constraints.clone()
    }

    /// Get ID values grouped by label for IN clause generation
    pub fn get_id_values_by_label(&self) -> HashMap<String, HashMap<String, Vec<String>>> {
        self.id_values_by_label.clone()
    }

    /// Transform all id() calls in a WHERE expression
    pub fn transform_where(&mut self, expr: Expression<'a>) -> Expression<'a> {
        log::debug!("  transform_where called");
        let result = self.transform_expression(expr);
        log::debug!("  transform_where complete");
        result
    }

    /// Transform all id() calls in ORDER BY items
    pub fn transform_order_by(&mut self, items: Vec<OrderByItem<'a>>) -> Vec<OrderByItem<'a>> {
        items
            .into_iter()
            .map(|item| OrderByItem {
                expression: self.transform_expression(item.expression),
                order: item.order,
            })
            .collect()
    }

    /// Recursively transform an expression
    fn transform_expression(&mut self, expr: Expression<'a>) -> Expression<'a> {
        match expr {
            // Transform operator applications (=, IN, NOT, etc.)
            Expression::OperatorApplicationExp(op_app) => {
                log::debug!(
                    "    Transforming OperatorApplication: {:?}",
                    op_app.operator
                );
                self.transform_operator_application(op_app)
            }

            // Transform id() function calls standalone (ORDER BY, SELECT)
            Expression::FunctionCallExp(func) => {
                log::debug!("    Found FunctionCall: {}", func.name);

                // Special handling for id(var) in ORDER BY / SELECT
                // Transform to var.id_property (let SQL generator handle it)
                if func.name.eq_ignore_ascii_case("id") && func.args.len() == 1 {
                    if let Expression::Variable(var) = &func.args[0] {
                        log::info!(
                            "    🔄 Transforming standalone id({}) to property access",
                            var
                        );

                        // Determine the ID property name from schema
                        let id_property = if let Some(_schema) = self.schema {
                            // We don't know the label here, so use a generic approach:
                            // Return a PropertyAccess that the SQL generator can resolve
                            // The SQL generator will need to handle this for multi-type patterns
                            "id"
                        } else {
                            "id"
                        };

                        let property_str = self.arena.alloc_str(id_property);
                        return Expression::PropertyAccessExp(
                            crate::open_cypher_parser::ast::PropertyAccess {
                                base: var,
                                key: property_str,
                            },
                        );
                    }
                }

                // For other functions, recursively transform arguments
                Expression::FunctionCallExp(FunctionCall {
                    name: func.name.clone(),
                    args: func
                        .args
                        .into_iter()
                        .map(|arg| self.transform_expression(arg))
                        .collect(),
                })
            }

            // Recursively transform lists
            Expression::List(items) => Expression::List(
                items
                    .into_iter()
                    .map(|item| self.transform_expression(item))
                    .collect(),
            ),

            // Recursively transform property access
            Expression::PropertyAccessExp(prop) => {
                // PropertyAccess has base: &str and key: &str, no transformation needed
                Expression::PropertyAccessExp(prop)
            }

            // Recursively transform CASE expressions
            Expression::Case(case) => Expression::Case(crate::open_cypher_parser::ast::Case {
                expr: case.expr.map(|e| Box::new(self.transform_expression(*e))),
                when_then: case
                    .when_then
                    .into_iter()
                    .map(|(when, then)| {
                        (
                            self.transform_expression(when),
                            self.transform_expression(then),
                        )
                    })
                    .collect(),
                else_expr: case
                    .else_expr
                    .map(|e| Box::new(self.transform_expression(*e))),
            }),

            // For all other expression types, return as-is
            _ => expr,
        }
    }

    /// Transform operator applications - this is where id() magic happens
    fn transform_operator_application(
        &mut self,
        op_app: OperatorApplication<'a>,
    ) -> Expression<'a> {
        match op_app.operator {
            // id(a) = 5 → (a:Label AND a.id = 'value')
            Operator::Equal => {
                log::debug!("      Checking Equal operator for id()");
                if let Some((var, id_value)) = self.extract_id_equals(&op_app.operands) {
                    log::info!("      ✅ Found id({}) = {} - transforming!", var, id_value);
                    return self.rewrite_id_equals(var, id_value);
                }
                // labels(a) = "Label" → true (label is tracked via MATCH pattern or CTE)
                if let Some(var) = self.extract_labels_equals(&op_app.operands) {
                    log::info!(
                        "      🏷️ Removing labels({}) predicate (handled by MATCH pattern/CTE)",
                        var
                    );
                    return Expression::Literal(Literal::Boolean(true));
                }
            }

            // id(a) IN [1, 2, 3] → ((a:L1 AND a.id = 'v1') OR ...)
            Operator::In => {
                if let Some((var, ids)) = self.extract_id_in(&op_app.operands) {
                    return self.rewrite_id_in(var, ids, false);
                }
            }

            // NOT (...)
            Operator::Not => {
                if op_app.operands.len() == 1 {
                    // Check if it's NOT id(...) IN [...]
                    if let Expression::OperatorApplicationExp(inner_op) = &op_app.operands[0] {
                        if inner_op.operator == Operator::In {
                            if let Some((var, ids)) = self.extract_id_in(&inner_op.operands) {
                                return self.rewrite_id_in(var, ids, true);
                            }
                        }
                    }
                }
            }

            _ => {}
        }

        // Not an id() pattern - recursively transform operands
        Expression::OperatorApplicationExp(OperatorApplication {
            operator: op_app.operator,
            operands: op_app
                .operands
                .into_iter()
                .map(|operand| self.transform_expression(operand))
                .collect(),
        })
    }

    /// Extract `id(var) = N` pattern
    fn extract_id_equals(&self, operands: &[Expression<'a>]) -> Option<(&'a str, i64)> {
        if operands.len() != 2 {
            return None;
        }

        // Check for id(var) = N
        if let (Some(var), Some(id)) = (
            self.extract_id_function(&operands[0]),
            self.extract_integer(&operands[1]),
        ) {
            return Some((var, id));
        }

        // Check for N = id(var)
        if let (Some(id), Some(var)) = (
            self.extract_integer(&operands[0]),
            self.extract_id_function(&operands[1]),
        ) {
            return Some((var, id));
        }

        None
    }

    /// Extract `labels(var) = "Label"` pattern → returns the variable name
    fn extract_labels_equals(&self, operands: &[Expression<'a>]) -> Option<&'a str> {
        if operands.len() != 2 {
            return None;
        }
        // Check labels(var) = "Label"
        if let Expression::FunctionCallExp(func) = &operands[0] {
            if (func.name.eq_ignore_ascii_case("labels") || func.name.eq_ignore_ascii_case("label"))
                && func.args.len() == 1
            {
                if let Expression::Variable(var) = &func.args[0] {
                    if matches!(&operands[1], Expression::Literal(Literal::String(_))) {
                        return Some(var);
                    }
                }
            }
        }
        // Check "Label" = labels(var)
        if let Expression::FunctionCallExp(func) = &operands[1] {
            if (func.name.eq_ignore_ascii_case("labels") || func.name.eq_ignore_ascii_case("label"))
                && func.args.len() == 1
            {
                if let Expression::Variable(var) = &func.args[0] {
                    if matches!(&operands[0], Expression::Literal(Literal::String(_))) {
                        return Some(var);
                    }
                }
            }
        }
        None
    }

    /// Extract `id(var) IN [...]` pattern
    fn extract_id_in(&self, operands: &[Expression<'a>]) -> Option<(&'a str, Vec<i64>)> {
        if operands.len() != 2 {
            return None;
        }

        let var = self.extract_id_function(&operands[0])?;
        let ids = self.extract_integer_list(&operands[1])?;

        Some((var, ids))
    }

    /// Extract variable name from id(var) function call
    fn extract_id_function(&self, expr: &Expression<'a>) -> Option<&'a str> {
        if let Expression::FunctionCallExp(func) = expr {
            log::debug!(
                "        Checking function: {}, args: {}",
                func.name,
                func.args.len()
            );
            if func.name.eq_ignore_ascii_case("id") && func.args.len() == 1 {
                if let Expression::Variable(var) = &func.args[0] {
                    log::debug!("        ✅ Matched id({}) pattern", var);
                    return Some(var);
                }
            }
        }
        None
    }

    /// Extract integer from literal
    fn extract_integer(&self, expr: &Expression<'a>) -> Option<i64> {
        if let Expression::Literal(Literal::Integer(n)) = expr {
            Some(*n)
        } else {
            None
        }
    }

    /// Extract list of integers
    fn extract_integer_list(&self, expr: &Expression<'a>) -> Option<Vec<i64>> {
        if let Expression::List(items) = expr {
            let mut ids = Vec::new();
            for item in items {
                ids.push(self.extract_integer(item)?);
            }
            Some(ids)
        } else {
            None
        }
    }

    /// Rewrite `id(var) = N` to `(var:Label AND var.id = 'value')`
    ///
    /// For cross-session id() lookups (when the integer ID isn't in the session cache),
    /// we generate a predicate that checks all possible node types with that ID.
    fn rewrite_id_equals(&mut self, var: &'a str, id_value: i64) -> Expression<'a> {
        // Try to lookup from cache (local + global for cross-session support)
        if let Some(element_id) = self.id_mapper.get_element_id(id_value) {
            if let Ok((label, id_parts)) = parse_node_element_id(&element_id) {
                let id_str = id_parts.join("|");
                log::info!(
                    "id() transform: id({}) = {} → {}:{} (from cache)",
                    var,
                    id_value,
                    label,
                    id_str
                );
                // Record label constraint for UNION pruning / label injection
                self.label_constraints
                    .entry(var.to_string())
                    .or_default()
                    .insert(label.to_string());
                self.id_values_by_label
                    .entry(var.to_string())
                    .or_default()
                    .entry(label.to_string())
                    .or_default()
                    .push(id_str.clone());
                return self.build_label_and_id_check(var, &label, &id_str);
            }
        }

        // ID not in cache - this happens when looking up an ID never seen before
        // We cannot resolve it without knowing which table/label it belongs to
        log::warn!(
            "id() transform: id({}) = {} cannot be resolved (not in global cache)",
            var,
            id_value
        );

        // Return false to indicate no match
        // The user should use label filters like MATCH (n:User) WHERE id(n) = X
        Expression::Literal(Literal::Boolean(false))
    }

    /// Rewrite `id(var) IN [...]` or `NOT id(var) IN [...]`
    fn rewrite_id_in(&mut self, var: &'a str, ids: Vec<i64>, is_negated: bool) -> Expression<'a> {
        log::info!(
            "🔍 rewrite_id_in: Processing {} IDs for variable '{}'",
            ids.len(),
            var
        );
        let mut filters = Vec::new();
        let mut labels_for_var = HashSet::new();
        let mut ids_by_label: HashMap<String, Vec<String>> = HashMap::new();

        for id_value in ids {
            // Try lookup from cache (local + global)
            if let Some(element_id) = self.id_mapper.get_element_id(id_value) {
                if let Ok((label, id_parts)) = parse_node_element_id(&element_id) {
                    let id_str = id_parts.join("|");
                    labels_for_var.insert(label.to_string());
                    ids_by_label
                        .entry(label.to_string())
                        .or_default()
                        .push(id_str.clone());
                    filters.push(self.build_label_and_id_check(var, &label, &id_str));
                    continue;
                }
            }

            // Fallback: Decode label from bit pattern
            // NOTE: Bit-pattern decoding is unreliable across server restarts because
            // label codes depend on registration order. Only the element_id cache
            // (populated during the current session) is reliable. Skip unknown IDs
            // rather than risk wrong-label SQL predicates.
            log::warn!(
                "id() transform: id({}) = {} cannot be resolved (not in session cache, skipping)",
                var,
                id_value
            );
        }

        // Store extracted labels and ID values for UNION splitting
        // 🔧 CRITICAL: Only store for non-negated IN clauses!
        // For "NOT id(o) IN [...]", the list represents exclusions, not the type of o.
        // o can be ANY type except those specific IDs.
        if !labels_for_var.is_empty() && !is_negated {
            log::info!(
                "🎯 UNION Pruning: Extracted labels for '{}': {:?}",
                var,
                labels_for_var
            );
            self.label_constraints
                .insert(var.to_string(), labels_for_var);
            self.id_values_by_label
                .insert(var.to_string(), ids_by_label);
        } else if !labels_for_var.is_empty() && is_negated {
            log::info!("🔧 SKIPPING label extraction for '{}' (negated IN clause - exclusion list, not type constraint)", var);
        }

        if filters.is_empty() {
            // Empty list or all IDs not found
            if is_negated {
                // NOT id(x) IN [] → true (exclude nothing = include everything)
                Expression::Literal(Literal::Boolean(true))
            } else {
                // id(x) IN [] → false (match nothing)
                Expression::Literal(Literal::Boolean(false))
            }
        } else if filters.len() == 1 {
            // Single filter
            if is_negated {
                Expression::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::Not,
                    operands: vec![filters.into_iter().next().unwrap()],
                })
            } else {
                filters.into_iter().next().unwrap()
            }
        } else {
            // Multiple filters - combine with OR
            let combined = Expression::OperatorApplicationExp(OperatorApplication {
                operator: Operator::Or,
                operands: filters,
            });

            if is_negated {
                Expression::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::Not,
                    operands: vec![combined],
                })
            } else {
                combined
            }
        }
    }

    /// Build property check expression for node ID (supports composite keys).
    ///
    /// For single-column IDs: `var.id_property = 'value'`
    /// For composite IDs: `var.col1 = 'val1' AND var.col2 = 'val2'`
    ///
    /// Label information is tracked separately and added to the MATCH pattern
    /// by split_query_by_labels().
    fn build_label_and_id_check(
        &self,
        var: &'a str,
        label: &str,
        id_value: &str,
    ) -> Expression<'a> {
        // Look up the ID columns from schema
        let id_columns: Vec<&str> = if let Some(schema) = self.schema {
            if let Some(node_schema) = schema.node_schema_opt(label) {
                let columns = node_schema.node_id.id.columns();
                if columns.is_empty() {
                    log::warn!("Label {} has no node_id columns, using 'id'", label);
                    vec!["id"]
                } else {
                    columns
                }
            } else {
                log::warn!("Label {} not found in schema, using 'id'", label);
                vec!["id"]
            }
        } else {
            log::warn!("No schema provided to transformer, using 'id'");
            vec!["id"]
        };

        // Split composite ID value by '|' separator
        let id_parts: Vec<&str> = id_value.split('|').collect();

        // Build one equality predicate per ID column
        let mut predicates = Vec::new();
        for (i, col) in id_columns.iter().enumerate() {
            let val = if i < id_parts.len() {
                id_parts[i]
            } else {
                log::warn!(
                    "Composite ID for {} has fewer parts ({}) than columns ({})",
                    label,
                    id_parts.len(),
                    id_columns.len()
                );
                id_value
            };

            log::debug!(
                "        Generating predicate: {}.{} = '{}' (label '{}' tracked separately)",
                var,
                col,
                val,
                label
            );

            let val_static: &'a str = self.arena.alloc_str(val);
            let col_static: &'a str = self.arena.alloc_str(col);

            predicates.push(Expression::OperatorApplicationExp(OperatorApplication {
                operator: Operator::Equal,
                operands: vec![
                    Expression::PropertyAccessExp(crate::open_cypher_parser::ast::PropertyAccess {
                        base: var,
                        key: col_static,
                    }),
                    Expression::Literal(Literal::String(val_static)),
                ],
            }));
        }

        // Single predicate or AND-chain for composite
        if predicates.len() == 1 {
            predicates.remove(0)
        } else {
            Expression::OperatorApplicationExp(OperatorApplication {
                operator: Operator::And,
                operands: predicates,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_planner::ast_transform::StringArena;
    use crate::server::bolt_protocol::id_mapper::IdMapper;

    #[test]
    fn test_extract_id_function() {
        let arena = StringArena::new();
        let id_mapper = IdMapper::new();
        let transformer = IdFunctionTransformer::new(&arena, &id_mapper, None);

        // id(a) function call
        let func = Expression::FunctionCallExp(FunctionCall {
            name: "id".to_string(),
            args: vec![Expression::Variable("a")],
        });

        assert_eq!(transformer.extract_id_function(&func), Some("a"));
    }

    #[test]
    fn test_extract_id_equals() {
        let arena = StringArena::new();
        let id_mapper = IdMapper::new();
        let transformer = IdFunctionTransformer::new(&arena, &id_mapper, None);

        let operands = vec![
            Expression::FunctionCallExp(FunctionCall {
                name: "id".to_string(),
                args: vec![Expression::Variable("a")],
            }),
            Expression::Literal(Literal::Integer(5)),
        ];

        assert_eq!(transformer.extract_id_equals(&operands), Some(("a", 5)));
    }

    #[test]
    fn test_rewrite_empty_list_negated() {
        let arena = StringArena::new();
        let id_mapper = IdMapper::new();
        let mut transformer = IdFunctionTransformer::new(&arena, &id_mapper, None);

        // NOT id(a) IN [] → true
        let result = transformer.rewrite_id_in("a", vec![], true);
        assert!(matches!(
            result,
            Expression::Literal(Literal::Boolean(true))
        ));
    }

    #[test]
    fn test_rewrite_empty_list_non_negated() {
        let arena = StringArena::new();
        let id_mapper = IdMapper::new();
        let mut transformer = IdFunctionTransformer::new(&arena, &id_mapper, None);

        // id(a) IN [] → false
        let result = transformer.rewrite_id_in("a", vec![], false);
        assert!(matches!(
            result,
            Expression::Literal(Literal::Boolean(false))
        ));
    }
}
