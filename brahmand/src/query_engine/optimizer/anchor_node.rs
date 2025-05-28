use std::{cmp::Ordering, collections::HashMap};

use uuid::Uuid;

use crate::query_engine::types::{ConnectedTraversal, TableData};

use super::errors::OptimizerError;

pub fn get_anchor_node(
    table_data_by_uid: HashMap<Uuid, TableData<'_>>,
) -> Result<Option<Uuid>, OptimizerError> {
    let mut max_conditions = 0;
    let static_uid = Uuid::new_v4();
    let mut max_condition_table = &static_uid;
    let mut max_condition_table_return_count = 0;

    for table_data_key in table_data_by_uid.keys() {
        let table_data = table_data_by_uid
            .get(table_data_key)
            .ok_or(OptimizerError::NoLogicalTableDataForUid)?;

        if max_conditions != 0 && max_conditions == table_data.where_conditions.len() {
            if max_condition_table_return_count < table_data.return_items.len() {
                max_conditions = table_data.where_conditions.len();
                max_condition_table = table_data_key;
                max_condition_table_return_count = table_data.return_items.len();
            }
        } else if max_conditions < table_data.where_conditions.len() {
            max_conditions = table_data.where_conditions.len();
            max_condition_table = table_data_key;
            max_condition_table_return_count = table_data.return_items.len();
        }
    }

    if max_condition_table.cmp(&static_uid) != Ordering::Equal {
        Ok(Some(*max_condition_table))
    } else {
        Ok(None)
    }
}

pub fn get_anchor_node_graph(
    anchor_node: Uuid,
    connected_traversals: Vec<ConnectedTraversal>,
) -> Result<ConnectedTraversal, OptimizerError> {
    for connected_traversal in &connected_traversals {
        if connected_traversal.start_node == anchor_node
            || connected_traversal.relationship == anchor_node
            || connected_traversal.end_node == anchor_node
        {
            return Ok(connected_traversal.clone());
        }
    }
    // throw error here.
    Err(OptimizerError::MissingAnchorNodeGraphTraversal)
}

#[cfg(test)]
mod tests {
    use crate::open_cypher_parser::ast::{
        Direction, Expression, Literal, Operator, OperatorApplication, ReturnItem,
    };

    use super::*;

    // Build a dummy OperatorApplication for testing.
    fn dummy_condition() -> OperatorApplication<'static> {
        OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                Expression::Literal(Literal::Integer(1)),
                Expression::Literal(Literal::Integer(2)),
            ],
        }
    }

    // Build a dummy ReturnItem for testing.
    fn dummy_return_item() -> ReturnItem<'static> {
        ReturnItem {
            expression: Expression::Variable("x"),
            alias: None,
        }
    }

    // get_anchor_node

    #[test]
    fn picks_table_with_max_conditions() {
        // Build two tables: one with 1 condition, one with 2
        let mut map1 = HashMap::new();
        let uid1 = Uuid::new_v4();
        let td1 = TableData {
            entity_name: None,
            table_name: None,
            return_items: vec![],
            where_conditions: vec![dummy_condition()],
            order_by_items: vec![],
        };
        let uid2 = Uuid::new_v4();
        let td2 = TableData {
            entity_name: None,
            table_name: None,
            return_items: vec![],
            where_conditions: vec![dummy_condition(), dummy_condition()],
            order_by_items: vec![],
        };
        map1.insert(uid1, td1.clone());
        map1.insert(uid2, td2.clone());
        let map2 = map1.clone();

        let selected = get_anchor_node(map1).unwrap().unwrap();
        let selected_td = map2.get(&selected).unwrap();
        // Should pick the table with 2 conditions
        assert_eq!(selected_td.where_conditions.len(), 2);
    }

    #[test]
    fn picks_table_by_return_count_when_tie_on_conditions() {
        // Build two tables: both with 2 conditions, but one has more return_items
        let mut map1 = HashMap::new();
        let uid1 = Uuid::new_v4();
        let td1 = TableData {
            entity_name: None,
            table_name: None,
            return_items: vec![dummy_return_item()],
            where_conditions: vec![dummy_condition(), dummy_condition()],
            order_by_items: vec![],
        };
        let uid2 = Uuid::new_v4();
        let td2 = TableData {
            entity_name: None,
            table_name: None,
            return_items: vec![dummy_return_item(), dummy_return_item()],
            where_conditions: vec![dummy_condition(), dummy_condition()],
            order_by_items: vec![],
        };
        map1.insert(uid1, td1.clone());
        map1.insert(uid2, td2.clone());
        let map2 = map1.clone();

        let selected = get_anchor_node(map1).unwrap().unwrap();
        let selected_td = map2.get(&selected).unwrap();
        // Tie on conditions (2 each), but td2 has 2 return_items vs td1's 1
        assert_eq!(selected_td.return_items.len(), 2);
    }

    // get_anchor_node_graph

    #[test]
    fn finds_traversal_for_anchor_node() {
        let anchor = Uuid::new_v4();
        let other = Uuid::new_v4();

        // A traversal that does NOT match the anchor
        let ct1 = ConnectedTraversal {
            id: Uuid::new_v4(),
            start_node: other,
            relationship: other,
            direction: Direction::Outgoing,
            end_node: other,
        };
        // A traversal where the start_node matches the anchor
        let ct2 = ConnectedTraversal {
            id: Uuid::new_v4(),
            start_node: anchor,
            relationship: other,
            direction: Direction::Incoming,
            end_node: other,
        };

        let input = vec![ct1.clone(), ct2.clone()];
        let result = get_anchor_node_graph(anchor, input).expect("should find matching traversal");
        assert_eq!(result, ct2);
    }
}
