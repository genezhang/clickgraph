// to generate a physical plan, we need a sequence of node traversals.
// start with the node with highest no of conditions and then with highest no of return types.
// give precedence to conditions
// Once we got the anchor node, start the graph traversal with finding a graph where that anchor node is
// While traversing relationship, check if it has any conditions. If there are any conditions then use the relationship table directly.
// If there are no conditions then based on the direction use appropriate adjecency table
// Traverse the remaining graphs
// At the end, we will have a sequence of nodes to traverse.

use std::collections::HashMap;

use uuid::Uuid;

use crate::{
    open_cypher_parser::ast::Direction,
    query_engine::types::{
        ConnectedTraversal, GraphSchema, LogicalPlan, PhysicalConnectedTraversal, PhysicalPlan,
        PhysicalPlanTableData,
    },
};

use super::{anchor_node, errors::OptimizerError, traversal_sequence};

pub fn generate_physical_plan<'a>(
    logical_plan: LogicalPlan<'a>,
    graph_schema: &GraphSchema,
) -> Result<PhysicalPlan<'a>, OptimizerError> {
    let mut traversal_sequence: Vec<Uuid> = vec![];

    let mut physical_table_data_by_uid: HashMap<Uuid, PhysicalPlanTableData> = HashMap::new();

    let mut entity_name_node_id_map: HashMap<String, String> = HashMap::new();

    let mut physcial_node_traversals: Vec<PhysicalPlanTableData<'a>> = vec![];

    let mut physical_connected_traversals: Vec<PhysicalConnectedTraversal> = vec![];

    // If both standalone node and connected patterns are present then it should return the cartesian product of both patterns
    // For now if standalone node is present then directly traverse the standalone node pattern.
    // Later on add support for multi pattern cartesian products

    for standalone_node_uid in logical_plan.node_traversals {
        let standalone_node_logical_table_data = logical_plan
            .table_data_by_uid
            .get(&standalone_node_uid)
            .ok_or(OptimizerError::NoLogicalTableDataForUid)?;
        let standalone_node_table_name = standalone_node_logical_table_data
            .table_name
            .ok_or(OptimizerError::MissingNodeLabel)?
            .to_string();

        let standalone_node_table_alias = standalone_node_logical_table_data
            .entity_name
            .ok_or(OptimizerError::MissingNodeName)?
            .to_string();

        let standalone_node_schema = graph_schema
            .nodes
            .get(&standalone_node_table_name)
            .ok_or(OptimizerError::NoNodeSchemaFound)?;

        let standalone_node_phy_table_data = PhysicalPlanTableData {
            id: standalone_node_uid,
            node_id: standalone_node_schema.node_id.column.clone(),
            table_alias: standalone_node_table_alias.clone(),
            table_name: standalone_node_table_name.clone(),
            temp_table_name: format!(
                "{}_{}",
                standalone_node_table_name, standalone_node_table_alias
            ),
            is_eagerly_evaluated: true,
            is_relationship: false,
            join_condition: None,
            forward_join_condition: None,
        };
        physical_table_data_by_uid
            .insert(standalone_node_uid, standalone_node_phy_table_data.clone());

        entity_name_node_id_map.insert(
            standalone_node_table_alias,
            standalone_node_phy_table_data.node_id.to_string(),
        );

        physcial_node_traversals.push(standalone_node_phy_table_data);
    }

    if !logical_plan.connected_traversals.is_empty() {
        let mut anchor_node = logical_plan
            .connected_traversals
            .first()
            .ok_or(OptimizerError::NoTravelsalGraph)?
            .start_node;
        if let Some(anchor_node_found) =
            anchor_node::get_anchor_node(logical_plan.table_data_by_uid.clone())?
        {
            anchor_node = anchor_node_found;
        };
        // println!("anchor_node {:?} ", anchor_node);

        let mut anchor_connected_traversal = anchor_node::get_anchor_node_graph(
            anchor_node,
            logical_plan.connected_traversals.clone(),
        )?;

        let anchor_graph_id = anchor_connected_traversal.id;

        // // For each traversal, we need to add 3 table data in the traversal_sequence
        // // (p)-[:acted_in]->(m)

        // this if for when anchor node is found at the start node
        let mut start_node_uid_key = anchor_connected_traversal.start_node;
        let relation_uid_key = anchor_connected_traversal.relationship;
        let mut end_node_uid_key = anchor_connected_traversal.end_node;

        // if anchor node found at end node of relation then reverse the order
        if anchor_connected_traversal.end_node == anchor_node {
            start_node_uid_key = anchor_connected_traversal.end_node;
            end_node_uid_key = anchor_connected_traversal.start_node;
            if anchor_connected_traversal.direction == Direction::Incoming {
                anchor_connected_traversal.direction = Direction::Outgoing;
            } else if anchor_connected_traversal.direction == Direction::Outgoing {
                anchor_connected_traversal.direction = Direction::Incoming;
            }
        }

        let new_anchor_connected_traversal = ConnectedTraversal {
            id: anchor_connected_traversal.id,
            start_node: start_node_uid_key,
            relationship: relation_uid_key,
            direction: anchor_connected_traversal.direction,
            end_node: end_node_uid_key,
        };

        let logical_table_data_by_uid = &logical_plan.table_data_by_uid;

        let mut physical_connected_traversal;

        (
            physical_table_data_by_uid,
            entity_name_node_id_map,
            traversal_sequence,
            physical_connected_traversal,
        ) = traversal_sequence::get_seq_from_connected_traversal(
            logical_table_data_by_uid,
            &new_anchor_connected_traversal,
            physical_table_data_by_uid,
            entity_name_node_id_map,
            traversal_sequence,
            graph_schema,
        )?;

        physical_connected_traversals.push(physical_connected_traversal);

        let other_connected_traversals: Vec<&ConnectedTraversal> = logical_plan
            .connected_traversals
            .iter()
            .filter(|graph| graph.id != anchor_graph_id)
            .collect();

        for connected_traversal in other_connected_traversals {
            if let Some(new_connected_traversal) = traversal_sequence::get_next_traversal(
                &traversal_sequence,
                connected_traversal.clone(),
            ) {
                (
                    physical_table_data_by_uid,
                    entity_name_node_id_map,
                    traversal_sequence,
                    physical_connected_traversal,
                ) = traversal_sequence::get_seq_from_connected_traversal(
                    logical_table_data_by_uid,
                    &new_connected_traversal,
                    physical_table_data_by_uid,
                    entity_name_node_id_map,
                    traversal_sequence,
                    graph_schema,
                )?;
                physical_connected_traversals.push(physical_connected_traversal);
            } else {
                // TODO
                // Could it be possible that the current graph traversal is not connected now to an already traversed graph but it will be connected later on.
                // Not sure.
            }
        }
    }

    Ok(PhysicalPlan {
        physical_connected_traversals,
        physcial_node_traversals,
        entity_name_node_id_map,
    })
}

#[cfg(test)]
mod tests {
    use crate::{
        open_cypher_parser::ast::{Expression, Literal, Operator, OperatorApplication, ReturnItem},
        query_engine::types::{NodeIdSchema, NodeSchema, RelationshipSchema, TableData},
    };

    use super::*;

    // helper to build an “empty” TableData with a label and optional entity_name
    fn td(label: &'static str, entity_name: Option<&'static str>) -> TableData<'static> {
        TableData {
            entity_name,
            table_name: Some(label),
            return_items: vec![ReturnItem {
                expression: Expression::Variable("x"),
                alias: None,
            }],
            where_conditions: vec![OperatorApplication {
                operator: Operator::Equal,
                operands: vec![
                    Expression::Literal(Literal::Integer(1)),
                    Expression::Literal(Literal::Integer(1)),
                ],
            }],
            order_by_items: vec![],
        }
    }

    // generate_physical_plan
    // Helpers

    fn make_full_schema() -> GraphSchema {
        let mut nodes = HashMap::new();
        nodes.insert(
            "Start".to_string(),
            NodeSchema {
                table_name: "Start".to_string(),
                column_names: vec!["c".to_string()],
                primary_keys: "c".to_string(),
                node_id: NodeIdSchema {
                    column: "start_id".to_string(),
                    dtype: "UUID".to_string(),
                },
            },
        );
        nodes.insert(
            "End".to_string(),
            NodeSchema {
                table_name: "End".to_string(),
                column_names: vec!["c".to_string()],
                primary_keys: "c".to_string(),
                node_id: NodeIdSchema {
                    column: "end_id".to_string(),
                    dtype: "UUID".to_string(),
                },
            },
        );

        let mut relationships = HashMap::new();
        relationships.insert(
            "Rel".to_string(),
            RelationshipSchema {
                table_name: "rel_tbl".to_string(),
                column_names: vec!["from_id".to_string(), "to_id".to_string()],
                from_node: "Start".to_string(),
                to_node: "End".to_string(),
                from_node_id_dtype: "UUID".to_string(),
                to_node_id_dtype: "UUID".to_string(),
            },
        );

        GraphSchema {
            version: 1,
            nodes,
            relationships,
        }
    }

    // Standalone-only plan
    #[test]
    fn standalone_only_plan() {
        let id = Uuid::new_v4();
        let mut table_data = HashMap::new();
        table_data.insert(id, td("Start", Some("sA")));

        let lp = LogicalPlan {
            connected_traversals: vec![],
            node_traversals: vec![id],
            overall_condition: None,
            overall_return_items: vec![],
            table_data_by_uid: table_data,
            entity_name_uid_map: HashMap::new(),
            return_item_by_alias: HashMap::new(),
            order_by_items: vec![],
            skip: None,
            limit: None,
        };

        let plan = generate_physical_plan(lp, &make_full_schema()).unwrap();
        // Should have one standalone, zero connected
        assert_eq!(plan.physcial_node_traversals.len(), 1);
        assert!(plan.physical_connected_traversals.is_empty());
        // Alias map should contain "sA" → "start_id"
        assert_eq!(plan.entity_name_node_id_map.get("sA").unwrap(), "start_id");
    }

    // Connected-only plan
    #[test]
    fn connected_only_plan() {
        let start = Uuid::new_v4();
        let rel = Uuid::new_v4();
        let end = Uuid::new_v4();

        let mut table_data = HashMap::new();
        table_data.insert(start, td("Start", Some("sA")));
        table_data.insert(rel, td("Rel", Some("rA")));
        table_data.insert(end, td("End", Some("eA")));

        let conn = ConnectedTraversal {
            id: Uuid::new_v4(),
            start_node: start,
            relationship: rel,
            direction: Direction::Outgoing,
            end_node: end,
        };

        let lp = LogicalPlan {
            connected_traversals: vec![conn.clone()],
            node_traversals: vec![],
            overall_condition: None,
            overall_return_items: vec![],
            table_data_by_uid: table_data,
            entity_name_uid_map: HashMap::new(),
            return_item_by_alias: HashMap::new(),
            order_by_items: vec![],
            skip: None,
            limit: None,
        };

        let plan = generate_physical_plan(lp, &make_full_schema()).unwrap();
        // No standalone
        assert!(plan.physcial_node_traversals.is_empty());
        // One connected
        assert_eq!(plan.physical_connected_traversals.len(), 1);
        // All three aliases in map
        assert_eq!(plan.entity_name_node_id_map.get("sA").unwrap(), "start_id");
        assert_eq!(plan.entity_name_node_id_map.get("rA").unwrap(), "from_id");
        assert_eq!(plan.entity_name_node_id_map.get("eA").unwrap(), "end_id");
    }

    // Mixed standalone + connected
    #[test]
    fn mixed_plan() {
        let single = Uuid::new_v4();
        let start = Uuid::new_v4();
        let rel = Uuid::new_v4();
        let end = Uuid::new_v4();

        // Standalone has zero conditions and zero return_items
        let mut table_data = HashMap::new();
        table_data.insert(
            single,
            TableData {
                entity_name: Some("solo"),
                table_name: Some("Start"),
                return_items: vec![],
                where_conditions: vec![],
                order_by_items: vec![],
            },
        );
        // Connected ones still have one of each
        table_data.insert(start, td("Start", Some("sA")));
        table_data.insert(rel, td("Rel", Some("rA")));
        table_data.insert(end, td("End", Some("eA")));

        let conn = ConnectedTraversal {
            id: Uuid::new_v4(),
            start_node: start,
            relationship: rel,
            direction: Direction::Outgoing,
            end_node: end,
        };

        let lp = LogicalPlan {
            connected_traversals: vec![conn.clone()],
            node_traversals: vec![single],
            overall_condition: None,
            overall_return_items: vec![],
            table_data_by_uid: table_data,
            entity_name_uid_map: HashMap::new(),
            return_item_by_alias: HashMap::new(),
            order_by_items: vec![],
            skip: None,
            limit: None,
        };

        let plan = generate_physical_plan(lp, &make_full_schema()).unwrap();
        // One standalone + one connected
        assert_eq!(plan.physcial_node_traversals.len(), 1);
        assert_eq!(plan.physical_connected_traversals.len(), 1);
        // Check both alias maps exist
        assert!(plan.entity_name_node_id_map.contains_key("solo"));
        assert!(plan.entity_name_node_id_map.contains_key("sA"));
        assert!(plan.entity_name_node_id_map.contains_key("rA"));
        assert!(plan.entity_name_node_id_map.contains_key("eA"));
    }
}
