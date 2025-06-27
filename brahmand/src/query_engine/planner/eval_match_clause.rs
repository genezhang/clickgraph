use uuid::Uuid;

use crate::{
    open_cypher_parser::ast::{
        ConnectedPattern, Expression, MatchClause, NodePattern, Operator, OperatorApplication,
        PathPattern, Property,
    },
    query_engine::types::{ConnectedTraversal, LogicalPlan, TableData},
};

use super::errors::PlannerError;

fn evaluate_node_relation_properties_as_equal_operator_application(
    properties: Option<Vec<Property>>,
) -> Vec<OperatorApplication> {
    match properties {
        Some(props) => {
            let mut prop_conditions: Vec<OperatorApplication> = vec![];
            for prop in props {
                // For now lets focus on property kv first
                if let Property::PropertyKV(property_kv) = prop {
                    // lets focus on literal values first. Keeping params for later.

                    if let Expression::Literal(literal) = property_kv.value {
                        let operator_application = OperatorApplication {
                            operator: Operator::Equal,
                            operands: vec![
                                Expression::Variable(property_kv.key),
                                Expression::Literal(literal),
                            ],
                        };

                        prop_conditions.push(operator_application);
                    }
                }
            }

            prop_conditions
        }
        None => vec![],
    }
}

fn traverse_connected_pattern<'a>(
    mut logical_plan: LogicalPlan<'a>,
    connected_patterns: Vec<ConnectedPattern<'a>>,
) -> Result<LogicalPlan<'a>, PlannerError> {
    for connected_pattern in connected_patterns {
        // node name or label should be present

        // if name is present then map it to the uid

        let mut start_node_uid = Uuid::new_v4();

        if let Some(node_name) = connected_pattern.start_node.borrow().name {
            if let Some(node_uid) = logical_plan.entity_name_uid_map.get(node_name) {
                start_node_uid = *node_uid;
            } else {
                logical_plan
                    .entity_name_uid_map
                    .insert(node_name.to_string(), start_node_uid);
            }
        }

        let start_node_props = connected_pattern.start_node.borrow_mut().properties.take();
        let mut start_node_where_conditions =
            evaluate_node_relation_properties_as_equal_operator_application(start_node_props);

        // check if the start node is already present in the table data lookups
        // if it is already present then check for the conditions. If the new one has conditions and old is None then assign the new condtions on the old one

        if let Some(existing_start_node_table_data) =
            logical_plan.table_data_by_uid.get_mut(&start_node_uid)
        {
            // TODO check for names and labels as well.
            if existing_start_node_table_data.table_name.is_none()
                && connected_pattern.start_node.borrow().label.is_some()
            {
                existing_start_node_table_data.table_name =
                    connected_pattern.start_node.borrow().label;
            }
            if !start_node_where_conditions.is_empty() {
                existing_start_node_table_data
                    .where_conditions
                    .append(&mut start_node_where_conditions);
            }
            // if the start node is present in standalone node patterns then remove it from the standalone node pattern as we are going to add this node in the connected pattern
            if let Some(pos) = logical_plan
                .node_traversals
                .iter()
                .position(|x| *x == start_node_uid)
            {
                logical_plan.node_traversals.swap_remove(pos);
            }
        } else {
            let start_table_data = TableData {
                entity_name: connected_pattern.start_node.borrow().name,
                table_name: connected_pattern.start_node.borrow().label,
                return_items: vec![],
                where_conditions: start_node_where_conditions,
                order_by_items: vec![],
            };

            logical_plan
                .table_data_by_uid
                .insert(start_node_uid, start_table_data);
        }

        // same for relation

        let mut rel_uid = Uuid::new_v4();

        if let Some(rel_name) = connected_pattern.relationship.name {
            if let Some(added_rel_uid) = logical_plan.entity_name_uid_map.get(rel_name) {
                rel_uid = *added_rel_uid;
            } else {
                logical_plan
                    .entity_name_uid_map
                    .insert(rel_name.to_string(), rel_uid);
            }
        }

        let mut rel_where_conditions =
            evaluate_node_relation_properties_as_equal_operator_application(
                connected_pattern.relationship.properties,
            );

        if let Some(existing_rel_table_data) = logical_plan.table_data_by_uid.get_mut(&rel_uid) {
            if existing_rel_table_data.table_name.is_none()
                && connected_pattern.relationship.label.is_some()
            {
                existing_rel_table_data.table_name = connected_pattern.relationship.label;
            }

            if !rel_where_conditions.is_empty() {
                existing_rel_table_data
                    .where_conditions
                    .append(&mut rel_where_conditions);
            }
        } else {
            let relationship_table_data = TableData {
                entity_name: connected_pattern.relationship.name,
                table_name: connected_pattern.relationship.label,
                return_items: vec![],
                where_conditions: rel_where_conditions,
                order_by_items: vec![],
            };

            logical_plan
                .table_data_by_uid
                .insert(rel_uid, relationship_table_data);
        }

        // do the same for end node
        let mut end_node_uid = Uuid::new_v4();

        if let Some(node_name) = connected_pattern.end_node.borrow().name {
            if let Some(added_node_uid) = logical_plan.entity_name_uid_map.get(node_name) {
                end_node_uid = *added_node_uid;
            } else {
                logical_plan
                    .entity_name_uid_map
                    .insert(node_name.to_string(), end_node_uid);
            }
        }

        let end_node_props = connected_pattern.end_node.borrow_mut().properties.take();
        let mut end_node_where_conditions =
            evaluate_node_relation_properties_as_equal_operator_application(end_node_props);

        // check if the end node is already present in the table data lookups
        // if it is already present then check for the conditions. If the new one has conditions and old is None then assign the new condtions on the old one

        if let Some(existing_end_node_table_data) =
            logical_plan.table_data_by_uid.get_mut(&end_node_uid)
        {
            if existing_end_node_table_data.table_name.is_none()
                && connected_pattern.end_node.borrow().label.is_some()
            {
                existing_end_node_table_data.table_name = connected_pattern.end_node.borrow().label;
            }

            if !end_node_where_conditions.is_empty() {
                existing_end_node_table_data
                    .where_conditions
                    .append(&mut end_node_where_conditions);
            }
            // if the end node is present in standalone node patterns then remove it from the standalone node pattern as we are going to add this node in the connected pattern
            if let Some(pos) = logical_plan
                .node_traversals
                .iter()
                .position(|x| *x == end_node_uid)
            {
                logical_plan.node_traversals.swap_remove(pos);
            }
        } else {
            let end_table_data = TableData {
                entity_name: connected_pattern.end_node.borrow().name,
                table_name: connected_pattern.end_node.borrow().label,
                return_items: vec![],
                where_conditions: end_node_where_conditions,
                order_by_items: vec![],
            };

            logical_plan
                .table_data_by_uid
                .insert(end_node_uid, end_table_data);
        }

        // add connected traversal

        let connected_traversal = ConnectedTraversal {
            id: Uuid::new_v4(),
            start_node: start_node_uid,
            relationship: rel_uid,
            direction: connected_pattern.relationship.direction,
            end_node: end_node_uid,
        };

        logical_plan.connected_traversals.push(connected_traversal);
    }

    Ok(logical_plan)
}

// fn to traverse standalone node without a connected pattern
fn traverse_node_pattern<'a>(
    mut logical_plan: LogicalPlan<'a>,
    node_pattern: NodePattern<'a>,
) -> Result<LogicalPlan<'a>, PlannerError> {
    // For now we are not supporting empty node. standalone node with name is supported.
    let node_name = node_pattern.name.ok_or(PlannerError::EmptyNode)?;

    let mut node_where_conditions =
        evaluate_node_relation_properties_as_equal_operator_application(node_pattern.properties);

    if let Some(added_node_uid) = logical_plan.entity_name_uid_map.get(node_name) {
        // If this node is present already then just add its conditions and do not add it in the logical plan

        let existing_node_table_data = logical_plan
            .table_data_by_uid
            .get_mut(added_node_uid)
            .ok_or(PlannerError::Unexpected)?;

        if existing_node_table_data.table_name.is_none() && node_pattern.label.is_some() {
            existing_node_table_data.table_name = node_pattern.label;
        }

        if !node_where_conditions.is_empty() {
            existing_node_table_data
                .where_conditions
                .append(&mut node_where_conditions);
        }
    } else {
        // As this node is not present already, add it in the logical plan
        let node_uid = Uuid::new_v4();
        logical_plan
            .entity_name_uid_map
            .insert(node_name.to_string(), node_uid);

        let node_table_data = TableData {
            entity_name: node_pattern.name,
            table_name: node_pattern.label,
            return_items: vec![],
            where_conditions: node_where_conditions,
            order_by_items: vec![],
        };

        logical_plan
            .table_data_by_uid
            .insert(node_uid, node_table_data);

        logical_plan.node_traversals.push(node_uid);
    }

    Ok(logical_plan)
}

pub fn evaluate_match_clause<'a>(
    mut logical_plan: LogicalPlan<'a>,
    match_clause: MatchClause<'a>,
) -> Result<LogicalPlan<'a>, PlannerError> {
    for path_pattern in match_clause.path_patterns {
        match path_pattern {
            PathPattern::Node(node_pattern) => {
                logical_plan = traverse_node_pattern(logical_plan, node_pattern)?;
            }
            PathPattern::ConnectedPattern(connected_patterns) => {
                logical_plan = traverse_connected_pattern(logical_plan, connected_patterns)?;
            }
        }
    }

    Ok(logical_plan)
}

#[cfg(test)]
mod tests {
    use crate::open_cypher_parser::ast::{
        Direction, Literal, NodePattern, PropertyKVPair, RelationshipPattern,
    };

    use super::*;
    use std::{cell::RefCell, rc::Rc};

    // evaluate_node_relation_properties_as_equal_operator_application
    // Update this test case when support for other expressions are added
    #[test]
    fn ignores_non_literal_property_kv() {
        let kv = PropertyKVPair {
            key: "foo",
            value: Expression::Variable("bar"),
        };
        let props = vec![Property::PropertyKV(kv)];
        let result = evaluate_node_relation_properties_as_equal_operator_application(Some(props));
        assert!(result.is_empty());
    }

    #[test]
    fn multiple_literal_properties_yield_multiple_ops() {
        let kv1 = PropertyKVPair {
            key: "name",
            value: Expression::Literal(Literal::String("Alice")),
        };
        let kv2 = PropertyKVPair {
            key: "active",
            value: Expression::Literal(Literal::Boolean(true)),
        };
        let props = vec![
            Property::PropertyKV(kv1.clone()),
            Property::PropertyKV(kv2.clone()),
        ];
        let result = evaluate_node_relation_properties_as_equal_operator_application(Some(props));
        let expected = vec![
            OperatorApplication {
                operator: Operator::Equal,
                operands: vec![
                    Expression::Variable("name"),
                    Expression::Literal(Literal::String("Alice")),
                ],
            },
            OperatorApplication {
                operator: Operator::Equal,
                operands: vec![
                    Expression::Variable("active"),
                    Expression::Literal(Literal::Boolean(true)),
                ],
            },
        ];
        assert_eq!(result, expected);
    }

    // traverse_connected_pattern

    fn make_node<'a>(
        name: Option<&'a str>,
        label: Option<&'a str>,
        props: Option<Vec<Property<'a>>>,
    ) -> Rc<RefCell<NodePattern<'a>>> {
        Rc::new(RefCell::new(NodePattern {
            name,
            label,
            properties: props,
        }))
    }

    fn make_rel<'a>(
        name: Option<&'a str>,
        label: Option<&'a str>,
        props: Option<Vec<Property<'a>>>,
        dir: Direction,
    ) -> RelationshipPattern<'a> {
        RelationshipPattern {
            name,
            label,
            properties: props,
            direction: dir,
        }
    }

    #[test]
    fn single_pattern_populates_plan() {
        let initial = LogicalPlan::default();
        let start = make_node(Some("A"), Some("Person"), None);
        let end = make_node(Some("B"), Some("Person"), None);
        let rel = make_rel(Some("R"), Some("KNOWS"), None, Direction::Outgoing);
        let cp = ConnectedPattern {
            start_node: start,
            relationship: rel,
            end_node: end,
        };

        let result = traverse_connected_pattern(initial, vec![cp]);
        assert!(result.is_ok());
        let plan = result.unwrap();

        // One connected traversal
        assert_eq!(plan.connected_traversals.len(), 1);
        let ct = &plan.connected_traversals[0];

        // Entities mapped
        assert_eq!(plan.entity_name_uid_map.len(), 3);
        assert_eq!(plan.entity_name_uid_map.get("A"), Some(&ct.start_node));
        assert_eq!(plan.entity_name_uid_map.get("R"), Some(&ct.relationship));
        assert_eq!(plan.entity_name_uid_map.get("B"), Some(&ct.end_node));

        // Table data entries exist for each UID
        assert!(plan.table_data_by_uid.contains_key(&ct.start_node));
        assert!(plan.table_data_by_uid.contains_key(&ct.relationship));
        assert!(plan.table_data_by_uid.contains_key(&ct.end_node));

        // No where_conditions by default
        for uid in [&ct.start_node, &ct.relationship, &ct.end_node] {
            let td = plan.table_data_by_uid.get(uid).unwrap();
            assert!(td.where_conditions.is_empty());
        }
    }

    #[test]
    fn literal_properties_generate_conditions() {
        let initial = LogicalPlan::default();
        // property on start node
        let kv = Property::PropertyKV(PropertyKVPair {
            key: "x",
            value: Expression::Literal(Literal::Integer(1)),
        });
        let start = make_node(Some("A"), None, Some(vec![kv]));
        let end = make_node(Some("B"), None, None);
        let rel = make_rel(Some("R"), None, None, Direction::Outgoing);
        let cp = ConnectedPattern {
            start_node: start,
            relationship: rel,
            end_node: end,
        };

        let plan = traverse_connected_pattern(initial, vec![cp]).unwrap();
        let ct = &plan.connected_traversals[0];
        let td = plan.table_data_by_uid.get(&ct.start_node).unwrap();
        assert_eq!(td.where_conditions.len(), 1);
        let op = &td.where_conditions[0];
        assert_eq!(op.operator, Operator::Equal);
        if let Expression::Variable(k) = &op.operands[0] {
            assert_eq!(k, &"x");
        } else {
            panic!("Expected variable operand");
        }
        if let Expression::Literal(Literal::Integer(v)) = &op.operands[1] {
            assert_eq!(*v, 1);
        } else {
            panic!("Expected literal operand");
        }
    }

    // Tests for traverse_node_pattern
    #[test]
    fn error_on_empty_node_name() {
        let initial = LogicalPlan::default();
        let node = NodePattern {
            name: None,
            label: Some("L"),
            properties: None,
        };
        let err = traverse_node_pattern(initial, node).unwrap_err();
        assert_eq!(err, PlannerError::EmptyNode);
    }

    #[test]
    fn new_node_inserts_into_plan() {
        let initial = LogicalPlan::default();
        let node = NodePattern {
            name: Some("X"),
            label: Some("L"),
            properties: None,
        };
        let plan = traverse_node_pattern(initial, node).unwrap();
        assert_eq!(plan.entity_name_uid_map.len(), 1);
        let uid = plan.entity_name_uid_map["X"];
        assert!(plan.table_data_by_uid.contains_key(&uid));
        assert!(plan.node_traversals.contains(&uid));
        let td = &plan.table_data_by_uid[&uid];
        assert_eq!(td.entity_name, Some("X"));
        assert_eq!(td.table_name, Some("L"));
        assert!(td.where_conditions.is_empty());
    }

    #[test]
    fn existing_node_appends_conditions_and_label() {
        let mut initial = LogicalPlan::default();
        let uid = Uuid::new_v4();
        initial.entity_name_uid_map.insert("Y".to_string(), uid);
        initial.table_data_by_uid.insert(
            uid,
            TableData {
                entity_name: Some("Y"),
                table_name: None,
                return_items: vec![],
                where_conditions: vec![],
                order_by_items: vec![],
            },
        );

        let kv = PropertyKVPair {
            key: "p",
            value: Expression::Literal(Literal::Boolean(false)),
        };
        let node = NodePattern {
            name: Some("Y"),
            label: Some("LabelY"),
            properties: Some(vec![Property::PropertyKV(kv.clone())]),
        };
        let plan = traverse_node_pattern(initial.clone(), node).unwrap();
        let td = plan.table_data_by_uid.get(&uid).unwrap();
        assert_eq!(td.table_name, Some("LabelY"));
        assert_eq!(td.where_conditions.len(), 1);
        assert_eq!(td.where_conditions[0].operator, Operator::Equal);
    }
}
