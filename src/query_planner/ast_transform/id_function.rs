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

use crate::{
    graph_catalog::element_id::parse_node_element_id,
    open_cypher_parser::ast::{
        Expression, FunctionCall, Literal, Operator, OperatorApplication, OrderByItem,
    },
    server::bolt_protocol::id_mapper::IdMapper,
};

/// Transforms id() function calls in Cypher AST using session's IdMapper
pub struct IdFunctionTransformer<'a> {
    id_mapper: &'a IdMapper,
    schema: Option<&'a crate::graph_catalog::GraphSchema>,
}

impl<'a> IdFunctionTransformer<'a> {
    pub fn new(
        id_mapper: &'a IdMapper,
        schema: Option<&'a crate::graph_catalog::GraphSchema>,
    ) -> Self {
        Self { id_mapper, schema }
    }

    /// Transform all id() calls in a WHERE expression
    pub fn transform_where(&self, expr: Expression<'a>) -> Expression<'a> {
        log::debug!("  transform_where called");
        let result = self.transform_expression(expr);
        log::debug!("  transform_where complete");
        result
    }

    /// Transform all id() calls in ORDER BY items
    pub fn transform_order_by(&self, items: Vec<OrderByItem<'a>>) -> Vec<OrderByItem<'a>> {
        items
            .into_iter()
            .map(|item| OrderByItem {
                expression: self.transform_expression(item.expression),
                order: item.order,
            })
            .collect()
    }

    /// Recursively transform an expression
    fn transform_expression(&self, expr: Expression<'a>) -> Expression<'a> {
        match expr {
            // Transform operator applications (=, IN, NOT, etc.)
            Expression::OperatorApplicationExp(op_app) => {
                log::debug!(
                    "    Transforming OperatorApplication: {:?}",
                    op_app.operator
                );
                self.transform_operator_application(op_app)
            }

            // Recursively transform function calls (but don't transform id() here - handled in operators)
            Expression::FunctionCallExp(func) => {
                log::debug!("    Found FunctionCall: {}", func.name);
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
    fn transform_operator_application(&self, op_app: OperatorApplication<'a>) -> Expression<'a> {
        match op_app.operator {
            // id(a) = 5 → (a:Label AND a.id = 'value')
            Operator::Equal => {
                log::debug!("      Checking Equal operator for id()");
                if let Some((var, id_value)) = self.extract_id_equals(&op_app.operands) {
                    log::info!("      ✅ Found id({}) = {} - transforming!", var, id_value);
                    return self.rewrite_id_equals(var, id_value);
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
    fn rewrite_id_equals(&self, var: &'a str, id_value: i64) -> Expression<'a> {
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
    fn rewrite_id_in(&self, var: &'a str, ids: Vec<i64>, is_negated: bool) -> Expression<'a> {
        let mut filters = Vec::new();

        for id_value in ids {
            // Try lookup from cache (local + global)
            if let Some(element_id) = self.id_mapper.get_element_id(id_value) {
                if let Ok((label, id_parts)) = parse_node_element_id(&element_id) {
                    let id_str = id_parts.join("|");
                    filters.push(self.build_label_and_id_check(var, &label, &id_str));
                    continue;
                }
            }

            // ID not found in cache - skip with warning
            log::warn!(
                "id() transform: id({}) = {} cannot be resolved (not in global cache)",
                var,
                id_value
            );
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

    /// Build property check expression: `var.id_property = 'value'`
    /// 
    /// For `MATCH (n) WHERE id(n) = X` (no label), we only check the id property.
    /// The UNION branching handles routing to the correct table.
    /// For `MATCH (n:Label) WHERE id(n) = X`, the label is already in the MATCH pattern.
    fn build_label_and_id_check(
        &self,
        var: &'a str,
        label: &str,
        id_value: &str,
    ) -> Expression<'a> {
        // Determine which property to use for ID check
        let id_property = if let Some(schema) = self.schema {
            // Look up the node_id property from schema
            if let Some(node_schema) = schema.node_schema_opt(label) {
                // Get the first column of the node_id (for composite keys, use first)
                let columns = node_schema.node_id.id.columns();
                if !columns.is_empty() {
                    columns[0]
                } else {
                    log::warn!("Label {} has no node_id columns, using 'id'", label);
                    "id"
                }
            } else {
                log::warn!("Label {} not found in schema, using 'id'", label);
                "id"
            }
        } else {
            log::warn!("No schema provided to transformer, using 'id'");
            "id"
        };

        log::debug!(
            "        Generating predicate: {}.{} = '{}'",
            var,
            id_property,
            id_value
        );

        // Property check: var.id_property = 'value'
        // We don't include label check because UNION branching handles table routing
        let id_value_static: &'a str = Box::leak(id_value.to_string().into_boxed_str());
        let id_property_static: &'a str = Box::leak(id_property.to_string().into_boxed_str());

        Expression::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                Expression::PropertyAccessExp(crate::open_cypher_parser::ast::PropertyAccess {
                    base: var,
                    key: id_property_static,
                }),
                Expression::Literal(Literal::String(id_value_static)),
            ],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::bolt_protocol::id_mapper::IdMapper;

    #[test]
    fn test_extract_id_function() {
        let id_mapper = IdMapper::new();
        let transformer = IdFunctionTransformer::new(&id_mapper, None);

        // id(a) function call
        let func = Expression::FunctionCallExp(FunctionCall {
            name: "id".to_string(),
            args: vec![Expression::Variable("a")],
        });

        assert_eq!(transformer.extract_id_function(&func), Some("a"));
    }

    #[test]
    fn test_extract_id_equals() {
        let id_mapper = IdMapper::new();
        let transformer = IdFunctionTransformer::new(&id_mapper, None);

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
        let id_mapper = IdMapper::new();
        let transformer = IdFunctionTransformer::new(&id_mapper, None);

        // NOT id(a) IN [] → true
        let result = transformer.rewrite_id_in("a", vec![], true);
        assert!(matches!(
            result,
            Expression::Literal(Literal::Boolean(true))
        ));
    }

    #[test]
    fn test_rewrite_empty_list_non_negated() {
        let id_mapper = IdMapper::new();
        let transformer = IdFunctionTransformer::new(&id_mapper, None);

        // id(a) IN [] → false
        let result = transformer.rewrite_id_in("a", vec![], false);
        assert!(matches!(
            result,
            Expression::Literal(Literal::Boolean(false))
        ));
    }
}
