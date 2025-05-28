use crate::{
    open_cypher_parser::ast::{Expression, OrderByClause},
    query_engine::types::LogicalPlan,
};

use super::errors::PlannerError;

pub fn evaluate_order_by_clause<'a>(
    mut logical_plan: LogicalPlan<'a>,
    order_by_clause: OrderByClause<'a>,
) -> Result<LogicalPlan<'a>, PlannerError> {
    // order by clause has to be either Expression::Variable() which willbe alias from return clause or a property access.
    // any other would result in error.
    for current_order_by_item in order_by_clause.order_by_items {
        match &current_order_by_item.expression {
            Expression::Variable(variable) => {
                // check for alias.
                if logical_plan.return_item_by_alias.contains_key(variable) {
                    // add it in IR
                    logical_plan.order_by_items.push(current_order_by_item);
                } else {
                    return Err(PlannerError::InvalidVariableInOrderByClause);
                }
            }
            Expression::PropertyAccessExp(property_access) => {
                // here we will check if the base of the property access present in the IR table lookup.
                let uid = logical_plan
                    .entity_name_uid_map
                    .get(property_access.base)
                    .ok_or(PlannerError::InvalidPropAccessInOrderByClause)?;
                if let Some(table_data) = logical_plan.table_data_by_uid.get_mut(uid) {
                    table_data
                        .order_by_items
                        .push(current_order_by_item.clone());
                    logical_plan.order_by_items.push(current_order_by_item);
                } else {
                    return Err(PlannerError::OrphanPropertyAccessInOrderByClause);
                }
            }
            _ => {
                // throw error
                return Err(PlannerError::UnsupportedtemInOrderByClause);
            }
        }
    }

    Ok(logical_plan)
}

#[cfg(test)]
mod tests {

    use super::*;

    use uuid::Uuid;

    use crate::{
        open_cypher_parser::ast::{
            Expression, OrderByItem, OrerByOrder, PropertyAccess, ReturnItem,
        },
        query_engine::types::{LogicalPlan, TableData},
    };

    // evaluate_order_by_clause

    /// Helper to build a simple PropertyAccessExp
    fn make_prop<'a>(base: &'a str, key: &'a str) -> Expression<'a> {
        Expression::PropertyAccessExp(PropertyAccess { base, key })
    }

    #[test]
    fn variable_valid_alias() {
        let mut plan = LogicalPlan::default();
        // Register an alias "a"
        plan.return_item_by_alias.insert(
            "a",
            ReturnItem {
                expression: Expression::Variable("a"),
                alias: None,
            },
        );
        let obi = OrderByItem {
            expression: Expression::Variable("a"),
            order: OrerByOrder::Asc,
        };
        let result = evaluate_order_by_clause(
            plan,
            OrderByClause {
                order_by_items: vec![obi.clone()],
            },
        )
        .unwrap();

        assert_eq!(result.order_by_items, vec![obi]);
    }

    #[test]
    fn variable_invalid_alias() {
        let plan = LogicalPlan::default();
        let obi = OrderByItem {
            expression: Expression::Variable("b"),
            order: OrerByOrder::Desc,
        };
        let err = evaluate_order_by_clause(
            plan,
            OrderByClause {
                order_by_items: vec![obi],
            },
        )
        .unwrap_err();
        assert!(matches!(err, PlannerError::InvalidVariableInOrderByClause));
    }

    #[test]
    fn property_valid() {
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

        let obi = OrderByItem {
            expression: make_prop("x", "f"),
            order: OrerByOrder::Desc,
        };
        let result = evaluate_order_by_clause(
            plan.clone(),
            OrderByClause {
                order_by_items: vec![obi.clone()],
            },
        )
        .unwrap();

        // Should record in plan.order_by_items
        assert_eq!(result.order_by_items, vec![obi.clone()]);
        // And also in the table's order_by_items
        let td = result.table_data_by_uid.get(&uid).unwrap();
        assert_eq!(td.order_by_items, vec![obi]);
    }

    #[test]
    fn property_invalid_base() {
        let plan = LogicalPlan::default();
        let obi = OrderByItem {
            expression: make_prop("y", "p"),
            order: OrerByOrder::Asc,
        };
        let err = evaluate_order_by_clause(
            plan,
            OrderByClause {
                order_by_items: vec![obi],
            },
        )
        .unwrap_err();
        assert!(matches!(
            err,
            PlannerError::InvalidPropAccessInOrderByClause
        ));
    }

    #[test]
    fn property_orphan_access() {
        let mut plan = LogicalPlan::default();
        let uid = Uuid::new_v4();
        // Base in entity_name_uid_map but no table_data_by_uid
        plan.entity_name_uid_map.insert("y".to_string(), uid);

        let obi = OrderByItem {
            expression: make_prop("y", "p"),
            order: OrerByOrder::Asc,
        };
        let err = evaluate_order_by_clause(
            plan,
            OrderByClause {
                order_by_items: vec![obi],
            },
        )
        .unwrap_err();
        assert!(matches!(
            err,
            PlannerError::OrphanPropertyAccessInOrderByClause
        ));
    }
}
