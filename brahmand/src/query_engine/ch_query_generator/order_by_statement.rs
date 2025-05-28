use crate::open_cypher_parser::ast::{Expression, OrderByItem};

use super::errors::ChQueryGeneratorError;

pub fn generate_order_by(
    order_by_items: Vec<OrderByItem>,
) -> Result<String, ChQueryGeneratorError> {
    let mut order_by_items_vec: Vec<String> = vec![];
    for current_order_by_item in order_by_items {
        match &current_order_by_item.expression {
            Expression::Variable(variable) => {
                let order_string: String = current_order_by_item.order.into();
                let order_by_string = format!("{} {}", variable, order_string);
                order_by_items_vec.push(order_by_string);
            }
            Expression::PropertyAccessExp(property_access) => {
                let order_string: String = current_order_by_item.order.into();
                // if current_order_by_item.order == OrerByOrder::Desc {
                // order_string = current_order_by_item.order.into();
                // }
                let order_by_string = format!(
                    "{}.{} {}",
                    property_access.base, property_access.key, order_string
                );
                order_by_items_vec.push(order_by_string);
            }
            _ => return Err(ChQueryGeneratorError::UnsupportedItemInOrderByClause),
        }
    }
    if !order_by_items_vec.is_empty() {
        Ok(format!("ORDER BY {}", order_by_items_vec.join(", ")))
    } else {
        Ok("".to_string())
    }
}

#[cfg(test)]
mod tests {
    use crate::open_cypher_parser::ast::{OrerByOrder, PropertyAccess};

    use super::*;

    #[test]
    fn order_by_user_example() {
        let items = vec![
            OrderByItem {
                expression: Expression::PropertyAccessExp(PropertyAccess {
                    base: "users",
                    key: "name",
                }),
                order: OrerByOrder::Desc,
            },
            OrderByItem {
                expression: Expression::Variable("age"),
                order: OrerByOrder::Asc,
            },
        ];
        let sql = generate_order_by(items).unwrap();
        assert_eq!(sql, "ORDER BY users.name DESC, age ASC");
    }

    #[test]
    fn order_by_empty() {
        let sql = generate_order_by(vec![]).unwrap();
        assert_eq!(sql, "");
    }
}
