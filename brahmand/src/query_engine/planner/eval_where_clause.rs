use std::collections::HashSet;

use crate::{
    open_cypher_parser::ast::{
        Expression, FunctionCall, Operator, OperatorApplication, PropertyAccess, ReturnItem,
        WhereClause,
    },
    query_engine::types::LogicalPlan,
};

fn process_expr<'a>(
    expr: Expression<'a>,
    extracted: &mut Vec<OperatorApplication<'a>>,
    multi_node_conditions: &mut Vec<PropertyAccess<'a>>,
    in_or: bool,
) -> Option<Expression<'a>> {
    match expr {
        // When we have an operator application, process it separately.
        Expression::OperatorApplicationExp(mut op_app) => {
            // Check if the current operator is an Or.
            let current_is_or = op_app.operator == Operator::Or;
            // Update our flag: once inside an Or, we stay inside.
            let new_in_or = in_or || current_is_or;

            // Process each operand recursively, passing the flag.
            let mut new_operands = Vec::new();
            for operand in op_app.operands {
                if let Some(new_operand) =
                    process_expr(operand, extracted, multi_node_conditions, new_in_or)
                {
                    new_operands.push(new_operand);
                }
            }
            // Update the operator application with the processed operands.
            op_app.operands = new_operands;

            // If we are not inside an Or, and at least one immediate operand is a PropertyAccessExp,
            // then we want to extract this operator application.
            // if !new_in_or
            //    && op_app.operands.iter().any(|e| matches!(e, Expression::PropertyAccessExp(_))) {
            //     // Add the current operator application to the extracted list.
            //     extracted.push(op_app);
            //     // Return None so that it is removed from the parent's operands.
            //     return None;
            // }

            // TODO ALl aggregated functions will be evaluated in final where clause. We have to check what kind of fns we can put here.
            // because if we put aggregated fns like count() then it will mess up the final result because we want the count of all joined entries in the set,
            // in case of anchor node this could lead incorrect answers.
            if !new_in_or {
                let mut should_extract: bool = false;
                let mut temp_prop_acc: Vec<PropertyAccess<'a>> = vec![];
                let mut condition_belongs_to: HashSet<&str> = HashSet::new();

                for operand in &op_app.operands {
                    // if any of the fn argument belongs to one table then extract it.
                    if let Expression::FunctionCallExp(fc) = operand {
                        // TODO add other aggregating fns here
                        if !fc.name.to_lowercase().contains("count") {
                            for arg in &fc.args {
                                if let Expression::PropertyAccessExp(prop_acc) = arg {
                                    condition_belongs_to.insert(prop_acc.base);
                                    temp_prop_acc.push(prop_acc.clone());
                                    should_extract = true;
                                }
                            }
                        }
                    } else if let Expression::PropertyAccessExp(prop_acc) = operand {
                        condition_belongs_to.insert(prop_acc.base);
                        temp_prop_acc.push(prop_acc.clone());
                        should_extract = true;
                    }
                }

                // if it is a multinode condition then we are not extracting. It will be kept at overall conditions
                // and applied at the end in the final query.
                if should_extract && condition_belongs_to.len() == 1 {
                    extracted.push(op_app);
                    return None;
                } else if condition_belongs_to.len() > 1 {
                    multi_node_conditions.append(&mut temp_prop_acc);
                }
            }

            // If after processing there is only one operand left and it is not unary then collapse the operator application.
            if op_app.operands.len() == 1 && op_app.operator != Operator::Not {
                return Some(op_app.operands.into_iter().next().unwrap()); // unwrap is safe we are checking the len in condition
            }

            // if both operands has been extracted then remove the parent op
            if op_app.operands.is_empty() {
                return None;
            }

            // Otherwise, return the rebuilt operator application.
            Some(Expression::OperatorApplicationExp(op_app))
        }

        // If we have a function call, process each argument.
        Expression::FunctionCallExp(fc) => {
            let mut new_args = Vec::new();
            for arg in fc.args {
                if let Some(new_arg) = process_expr(arg, extracted, multi_node_conditions, in_or) {
                    new_args.push(new_arg);
                }
            }
            Some(Expression::FunctionCallExp(FunctionCall {
                name: fc.name,
                args: new_args,
            }))
        }

        // For a list, process each element.
        Expression::List(exprs) => {
            let mut new_exprs = Vec::new();
            for sub_expr in exprs {
                if let Some(new_expr) =
                    process_expr(sub_expr, extracted, multi_node_conditions, in_or)
                {
                    new_exprs.push(new_expr);
                }
            }
            Some(Expression::List(new_exprs))
        }

        // Base cases – literals, variables, and property accesses remain unchanged.
        other => Some(other),
    }
}

pub fn evaluate_where_clause<'a>(
    mut logical_plan: LogicalPlan<'a>,
    where_clause: WhereClause<'a>,
) -> LogicalPlan<'a> {
    // check_if_operator_application_is_single_table(where_clause.constraints, logical_plan.table_data_by_name);
    let mut extracted: Vec<OperatorApplication<'a>> = vec![];
    let mut multi_node_conditions: Vec<PropertyAccess<'a>> = vec![];
    let remaining = process_expr(
        where_clause.conditions,
        &mut extracted,
        &mut multi_node_conditions,
        false,
    );

    logical_plan.overall_condition = remaining;

    // println!("\n\n extracted {:?} \n\n",extracted);

    // add extracted conditions to respective table data
    for extracted_condition in extracted {
        let mut table_name = "";
        for operand in &extracted_condition.operands {
            if let Expression::PropertyAccessExp(property_access) = operand {
                table_name = property_access.base;
            }
            // in case of fn, we check for any argument is of type prop access
            if let Expression::FunctionCallExp(fc) = operand {
                for arg in &fc.args {
                    if let Expression::PropertyAccessExp(property_access) = arg {
                        table_name = property_access.base;
                    }
                }
            }
        }
        if let Some(uid) = logical_plan.entity_name_uid_map.get(table_name) {
            if let Some(table_data) = logical_plan.table_data_by_uid.get_mut(uid) {
                table_data.where_conditions.push(extracted_condition);
            }
        }
    }

    // add multi node conditions to their respective nodes.
    for prop_acc in multi_node_conditions {
        if let Some(uid) = logical_plan.entity_name_uid_map.get(prop_acc.base) {
            if let Some(table_data) = logical_plan.table_data_by_uid.get_mut(uid) {
                let return_item = ReturnItem {
                    expression: Expression::PropertyAccessExp(prop_acc),
                    alias: None,
                };
                table_data.return_items.push(return_item);
            }
        }
    }

    logical_plan
}

#[cfg(test)]
mod tests {

    use super::*;
    // process_expr

    use uuid::Uuid;

    use crate::{
        open_cypher_parser::ast::{
            Expression, FunctionCall, Literal, Operator, OperatorApplication, PropertyAccess,
            WhereClause,
        },
        query_engine::types::{LogicalPlan, TableData},
    };

    /// Helper to build a simple PropertyAccessExp
    fn make_prop<'a>(base: &'a str, key: &'a str) -> Expression<'a> {
        Expression::PropertyAccessExp(PropertyAccess { base, key })
    }

    #[test]
    fn base_cases_literal_variable_property() {
        let mut extracted = vec![];
        let mut multi_node_conditions: Vec<PropertyAccess> = vec![];

        // Literal
        let lit = Expression::Literal(Literal::Integer(42));
        assert_eq!(
            process_expr(
                lit.clone(),
                &mut extracted,
                &mut multi_node_conditions,
                false
            ),
            Some(lit)
        );
        assert!(extracted.is_empty());

        // Variable
        let var = Expression::Variable("foo");
        assert_eq!(
            process_expr(
                var.clone(),
                &mut extracted,
                &mut multi_node_conditions,
                false
            ),
            Some(var)
        );
        assert!(extracted.is_empty());

        // PropertyAccessExp
        let prop = make_prop("x", "bar");
        assert_eq!(
            process_expr(
                prop.clone(),
                &mut extracted,
                &mut multi_node_conditions,
                false
            ),
            Some(prop)
        );
        assert!(extracted.is_empty());
    }

    #[test]
    fn simple_operator_extraction() {
        let mut extracted = vec![];
        let mut multi_node_conditions: Vec<PropertyAccess> = vec![];

        // x.prop = 7
        let op_app = OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                make_prop("x", "prop"),
                Expression::Literal(Literal::Integer(7)),
            ],
        };
        let expr = Expression::OperatorApplicationExp(op_app.clone());

        // Since it's not inside an OR, encountering a PropertyAccessExp should pull it out.
        let out = process_expr(expr, &mut extracted, &mut multi_node_conditions, false);
        assert!(out.is_none(), "should be removed from parent");
        assert_eq!(extracted.len(), 1);
        assert_eq!(extracted[0], op_app);
    }

    #[test]
    fn collapse_single_operand() {
        let mut extracted = vec![];
        let mut multi_node_conditions: Vec<PropertyAccess> = vec![];

        // A unary Distinct over a literal; should collapse to inner literal
        let op_app = OperatorApplication {
            operator: Operator::Distinct,
            operands: vec![Expression::Literal(Literal::Boolean(true))],
        };
        let expr = Expression::OperatorApplicationExp(op_app.clone());

        let out = process_expr(expr, &mut extracted, &mut multi_node_conditions, false);
        // Not an extraction scenario, and one operand => collapse
        assert_eq!(out, Some(Expression::Literal(Literal::Boolean(true))));
        assert!(extracted.is_empty());
    }

    #[test]
    fn function_call_extraction_non_count() {
        let mut extracted = vec![];
        let mut multi_node_conditions: Vec<PropertyAccess> = vec![];
        // foo(x.prop)
        let fc = FunctionCall {
            name: "SUM".to_string(),
            args: vec![make_prop("x", "p")],
        };
        let expr = Expression::FunctionCallExp(fc.clone());

        // wrap in = operator so we hit the operator-extraction logic
        let op_app = OperatorApplication {
            operator: Operator::Equal,
            operands: vec![expr.clone(), Expression::Literal(Literal::Integer(0))],
        };
        let wrapped = Expression::OperatorApplicationExp(op_app.clone());

        let out = process_expr(wrapped, &mut extracted, &mut multi_node_conditions, false);
        // the operator-application itself contains a function-call with prop => extracted
        assert!(out.is_none());
        assert_eq!(extracted.len(), 1);
        assert_eq!(extracted[0], op_app);
    }

    #[test]
    fn function_call_count_is_skipped() {
        let mut extracted = vec![];
        let mut multi_node_conditions: Vec<PropertyAccess> = vec![];

        // count(x.prop)
        let fc = FunctionCall {
            name: "COUNT".to_string(),
            args: vec![make_prop("x", "p")],
        };
        let expr = Expression::FunctionCallExp(fc.clone());

        let op_app = OperatorApplication {
            operator: Operator::Equal,
            operands: vec![expr, Expression::Literal(Literal::Integer(0))],
        };
        let wrapped = Expression::OperatorApplicationExp(op_app.clone());

        let out = process_expr(
            wrapped.clone(),
            &mut extracted,
            &mut multi_node_conditions,
            false,
        );
        // Since it's a count*, it should not extract, and no collapse => preserved
        assert_eq!(out, Some(Expression::OperatorApplicationExp(op_app)));
        assert!(extracted.is_empty());
    }

    // evaluate_where_clause

    #[test]
    fn no_extraction_literal_condition() {
        let mut plan = LogicalPlan::default();
        let uid = Uuid::new_v4();
        plan.entity_name_uid_map.insert("x".to_string(), uid);
        plan.table_data_by_uid.insert(
            uid,
            TableData {
                entity_name: Some("x"),
                table_name: Some("X"),
                return_items: vec![],
                where_conditions: vec![],
                order_by_items: vec![],
            },
        );

        // WHERE TRUE — nothing to extract
        let where_clause = WhereClause {
            conditions: Expression::Literal(Literal::Boolean(true)),
        };
        let result = evaluate_where_clause(plan.clone(), where_clause);

        // overall_condition should be Some(Literal(true))
        assert_eq!(
            result.overall_condition,
            Some(Expression::Literal(Literal::Boolean(true)))
        );
        // No new where_conditions on the table
        let td = result.table_data_by_uid.get(&uid).unwrap();
        assert!(td.where_conditions.is_empty());
    }

    #[test]
    fn simple_extraction_equal_op() {
        let mut plan = LogicalPlan::default();
        let uid = Uuid::new_v4();
        plan.entity_name_uid_map.insert("x".to_string(), uid);
        plan.table_data_by_uid.insert(
            uid,
            TableData {
                entity_name: Some("x"),
                table_name: Some("X"),
                return_items: vec![],
                where_conditions: vec![],
                order_by_items: vec![],
            },
        );

        // WHERE x.prop = 5
        let op_app = OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                make_prop("x", "prop"),
                Expression::Literal(Literal::Integer(5)),
            ],
        };
        let where_clause = WhereClause {
            conditions: Expression::OperatorApplicationExp(op_app.clone()),
        };
        let result = evaluate_where_clause(plan.clone(), where_clause);

        // Expression was extracted, so overall_condition is None
        assert_eq!(result.overall_condition, None);
        // And the table's where_conditions should contain our op_app
        let td = result.table_data_by_uid.get(&uid).unwrap();
        assert_eq!(td.where_conditions, vec![op_app]);
    }

    #[test]
    fn skip_count_function() {
        let mut plan = LogicalPlan::default();
        let uid = Uuid::new_v4();
        plan.entity_name_uid_map.insert("f".to_string(), uid);
        plan.table_data_by_uid.insert(
            uid,
            TableData {
                entity_name: Some("f"),
                table_name: Some("F"),
                return_items: vec![],
                where_conditions: vec![],
                order_by_items: vec![],
            },
        );

        // WHERE COUNT(f.prop) = 0   -- count should *not* be extracted
        let fc = FunctionCall {
            name: "COUNT".to_string(),
            args: vec![make_prop("f", "prop")],
        };
        let op_app = OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                Expression::FunctionCallExp(fc.clone()),
                Expression::Literal(Literal::Integer(0)),
            ],
        };
        let where_clause = WhereClause {
            conditions: Expression::OperatorApplicationExp(op_app.clone()),
        };
        let result = evaluate_where_clause(plan.clone(), where_clause);

        // No extraction: overall_condition retains the operator application
        assert_eq!(
            result.overall_condition,
            Some(Expression::OperatorApplicationExp(op_app.clone()))
        );
        // And table's where_conditions remains empty
        let td = result.table_data_by_uid.get(&uid).unwrap();
        assert!(td.where_conditions.is_empty());
    }

    #[test]
    fn or_suppresses_extraction() {
        let mut plan = LogicalPlan::default();
        let uid = Uuid::new_v4();
        plan.entity_name_uid_map.insert("x".to_string(), uid);
        plan.table_data_by_uid.insert(
            uid,
            TableData {
                entity_name: Some("x"),
                table_name: Some("X"),
                return_items: vec![],
                where_conditions: vec![],
                order_by_items: vec![],
            },
        );

        // WHERE (x.prop = 1) OR (x.prop = 2)  — inside an OR no extraction
        let left = OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                make_prop("x", "prop"),
                Expression::Literal(Literal::Integer(1)),
            ],
        };
        let right = OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                make_prop("x", "prop"),
                Expression::Literal(Literal::Integer(2)),
            ],
        };
        let or_app = OperatorApplication {
            operator: Operator::Or,
            operands: vec![
                Expression::OperatorApplicationExp(left.clone()),
                Expression::OperatorApplicationExp(right.clone()),
            ],
        };
        let where_clause = WhereClause {
            conditions: Expression::OperatorApplicationExp(or_app.clone()),
        };
        let result = evaluate_where_clause(plan.clone(), where_clause);

        // No extraction: overall_condition retains the OR expression
        assert_eq!(
            result.overall_condition,
            Some(Expression::OperatorApplicationExp(or_app.clone()))
        );
        // And table's where_conditions remains empty
        let td = result.table_data_by_uid.get(&uid).unwrap();
        assert!(td.where_conditions.is_empty());
    }

    #[test]
    fn function_extraction_non_count() {
        let mut plan = LogicalPlan::default();
        let uid = Uuid::new_v4();
        plan.entity_name_uid_map.insert("f".to_string(), uid);
        plan.table_data_by_uid.insert(
            uid,
            TableData {
                entity_name: Some("f"),
                table_name: Some("F"),
                return_items: vec![],
                where_conditions: vec![],
                order_by_items: vec![],
            },
        );

        // WHERE SUM(f.prop) = 0  — non-count function should extract
        let fc = FunctionCall {
            name: "SUM".to_string(),
            args: vec![make_prop("f", "p")],
        };
        let op_app = OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                Expression::FunctionCallExp(fc.clone()),
                Expression::Literal(Literal::Integer(0)),
            ],
        };
        let where_clause = WhereClause {
            conditions: Expression::OperatorApplicationExp(op_app.clone()),
        };
        let result = evaluate_where_clause(plan.clone(), where_clause);

        // Extracted, so overall_condition is None
        assert_eq!(result.overall_condition, None);
        // And table's where_conditions contains our op_app
        let td = result.table_data_by_uid.get(&uid).unwrap();
        assert_eq!(td.where_conditions, vec![op_app]);
    }

    #[test]
    fn multi_node_conditions_check() {
        let mut plan = LogicalPlan::default();
        let a_uid = Uuid::new_v4();
        plan.entity_name_uid_map.insert("a".to_string(), a_uid);
        plan.table_data_by_uid.insert(
            a_uid,
            TableData {
                entity_name: Some("a"),
                table_name: Some("User"),
                return_items: vec![],
                where_conditions: vec![],
                order_by_items: vec![],
            },
        );

        let b_uid = Uuid::new_v4();
        plan.entity_name_uid_map.insert("b".to_string(), b_uid);
        plan.table_data_by_uid.insert(
            b_uid,
            TableData {
                entity_name: Some("b"),
                table_name: Some("User"),
                return_items: vec![],
                where_conditions: vec![],
                order_by_items: vec![],
            },
        );

        let op_app = OperatorApplication {
            operator: Operator::NotEqual,
            operands: vec![
                Expression::PropertyAccessExp(PropertyAccess {
                    base: "a",
                    key: "username",
                }),
                Expression::PropertyAccessExp(PropertyAccess {
                    base: "b",
                    key: "username",
                }),
            ],
        };

        let where_clause = WhereClause {
            conditions: Expression::OperatorApplicationExp(op_app.clone()),
        };

        let result = evaluate_where_clause(plan.clone(), where_clause);
        // as it is a multi node condition, it should not extract and the condition should be in the result.overall_conditions
        assert_eq!(
            result.overall_condition,
            Some(Expression::OperatorApplicationExp(op_app))
        );

        // table data must be updated for each node
        let a_return_item = ReturnItem {
            expression: Expression::PropertyAccessExp(PropertyAccess {
                base: "a",
                key: "username",
            }),
            alias: None,
        };
        let a_td = result.table_data_by_uid.get(&a_uid).unwrap();
        assert_eq!(a_td.return_items, vec![a_return_item]);

        let b_return_item = ReturnItem {
            expression: Expression::PropertyAccessExp(PropertyAccess {
                base: "b",
                key: "username",
            }),
            alias: None,
        };
        let b_td = result.table_data_by_uid.get(&b_uid).unwrap();
        assert_eq!(b_td.return_items, vec![b_return_item]);
    }
}
