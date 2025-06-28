use crate::{open_cypher_parser::ast::OpenCypherQueryAst, query_engine::types::LogicalPlan};

use super::{
    errors::PlannerError, eval_match_clause, eval_order_by_clause, eval_return_clause,
    eval_skip_n_limit_clause, eval_where_clause,
};

pub fn evaluate_query(query_ast: OpenCypherQueryAst<'_>) -> Result<LogicalPlan<'_>, PlannerError> {
    let mut logical_plan = LogicalPlan::default();
    // println!("\n\n query_ast {:?}", query_ast);

    if let Some(match_clause) = query_ast.match_clause {
        logical_plan = eval_match_clause::evaluate_match_clause(logical_plan, match_clause)?;
    }

    if let Some(where_clause) = query_ast.where_clause {
        logical_plan = eval_where_clause::evaluate_where_clause(logical_plan, where_clause);
    }

    if let Some(return_clause) = query_ast.return_clause {
        logical_plan =
            eval_return_clause::evaluate_return_clause(logical_plan, return_clause, true)?;
    }

    if let Some(order_clause) = query_ast.order_by_clause {
        logical_plan = eval_order_by_clause::evaluate_order_by_clause(logical_plan, order_clause)?;
    }

    if let Some(skip_clause) = query_ast.skip_clause {
        logical_plan = eval_skip_n_limit_clause::evaluate_skip_clause(logical_plan, skip_clause);
    }

    if let Some(limit_clause) = query_ast.limit_clause {
        logical_plan = eval_skip_n_limit_clause::evaluate_limit_clause(logical_plan, limit_clause);
    }

    // println!("\n \n logical_plan in evaluate_query {:}", logical_plan);
    Ok(logical_plan)
}

#[cfg(test)]
mod tests {

    use super::*;

    // evaluate_query

    // unified test case

    #[test]
    fn evaluate_query_match_where_return() {
        use crate::open_cypher_parser::ast::{
            Expression, Literal, MatchClause, NodePattern, OpenCypherQueryAst, Operator,
            OperatorApplication, PathPattern, Property, PropertyAccess, PropertyKVPair,
            ReturnClause, ReturnItem, WhereClause,
        };

        // 1) MATCH (n:Person { age: 30 })
        let node_pattern = NodePattern {
            name: Some("n"),
            label: Some("Person"),
            properties: Some(vec![Property::PropertyKV(PropertyKVPair {
                key: "age",
                value: Expression::Literal(Literal::Integer(30)),
            })]),
        };
        let match_clause = MatchClause {
            path_patterns: vec![PathPattern::Node(node_pattern)],
        };

        // 2) WHERE n.name = 'Alice'
        let where_clause = WhereClause {
            conditions: Expression::OperatorApplicationExp(OperatorApplication {
                operator: Operator::Equal,
                operands: vec![
                    Expression::PropertyAccessExp(PropertyAccess {
                        base: "n",
                        key: "name",
                    }),
                    Expression::Literal(Literal::String("Alice")),
                ],
            }),
        };

        // 3) RETURN n.name
        let return_clause = ReturnClause {
            return_items: vec![ReturnItem {
                expression: Expression::PropertyAccessExp(PropertyAccess {
                    base: "n",
                    key: "name",
                }),
                alias: None,
            }],
        };

        let ast = OpenCypherQueryAst {
            match_clause: Some(match_clause),
            with_clause: None,
            where_clause: Some(where_clause),
            create_clause: None,
            create_node_table_clause: None,
            create_rel_table_clause: None,
            set_clause: None,
            remove_clause: None,
            delete_clause: None,
            return_clause: Some(return_clause),
            order_by_clause: None,
            skip_clause: None,
            limit_clause: None,
        };

        let plan = evaluate_query(ast).expect("evaluate_query should succeed");

        // It should register exactly one entity "n"
        assert_eq!(plan.entity_name_uid_map.len(), 1);
        let uid = *plan.entity_name_uid_map.get("n").unwrap();

        // TableData must exist for that uid
        let td = plan
            .table_data_by_uid
            .get(&uid)
            .expect("TableData for `n` should be present");

        // Entity metadata
        assert_eq!(td.entity_name, Some("n"));
        assert_eq!(td.table_name, Some("Person"));

        // Two WHERE conditions: age=30 then name='Alice'
        assert_eq!(td.where_conditions.len(), 2);
        // first from the MATCH clause
        assert_eq!(
            td.where_conditions[0],
            OperatorApplication {
                operator: Operator::Equal,
                operands: vec![
                    Expression::PropertyAccessExp(PropertyAccess {
                        base: "n",
                        key: "age"
                    }),
                    Expression::Literal(Literal::Integer(30)),
                ],
            }
        );
        // second from the WHERE clause
        assert_eq!(
            td.where_conditions[1],
            OperatorApplication {
                operator: Operator::Equal,
                operands: vec![
                    Expression::PropertyAccessExp(PropertyAccess {
                        base: "n",
                        key: "name"
                    }),
                    Expression::Literal(Literal::String("Alice")),
                ],
            }
        );

        // RETURN items should be attached to the same table
        assert_eq!(td.return_items.len(), 1);
        assert_eq!(
            td.return_items[0].expression,
            Expression::PropertyAccessExp(PropertyAccess {
                base: "n",
                key: "name"
            })
        );

        // overall_return_items should also record it
        assert_eq!(plan.overall_return_items.len(), 1);
        assert_eq!(
            plan.overall_return_items[0].return_item.expression,
            Expression::PropertyAccessExp(PropertyAccess {
                base: "n",
                key: "name"
            })
        );
        assert_eq!(plan.overall_return_items[0].belongs_to_table, "n");
    }
}
