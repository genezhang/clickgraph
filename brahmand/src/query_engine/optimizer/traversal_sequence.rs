use std::collections::HashMap;

use uuid::Uuid;

use crate::{
    open_cypher_parser::ast::Direction,
    query_engine::types::{
        ConnectedTraversal, GraphSchema, JoinCondition, PhysicalConnectedTraversal,
        PhysicalPlanTableData, TableData,
    },
};

use super::errors::OptimizerError;

// We will get the correct table name for relation based start_node, end_node and relation schema stored in graph schema
// Post --CREATED_BY---> User
// CREATED_BY_outgoing = (Post, User)
// CREATED_BY_incoming = (User, Post)
// If from_node of schema == start_node -> outgoing with start_node = from_node and end_node = to_node
// If from_node of schema == end_node  -> incoming with start_node = from_node and end_node = to_node
pub fn get_relationship_table_name(
    start_node_label: String,
    end_node_label: String,
    rel_label: String,
    graph_schema: &GraphSchema,
) -> Result<String, OptimizerError> {
    let rel_table_schema = graph_schema
        .relationships
        .get(&rel_label)
        .ok_or(OptimizerError::NoRelationSchemaFound)?;

    if rel_table_schema.from_node == start_node_label {
        return Ok(format!("{}_outgoing", rel_label));
    }

    if rel_table_schema.from_node == end_node_label {
        return Ok(format!("{}_incoming", rel_label));
    }

    Err(OptimizerError::NoRelationSchemaFound)
}

// (physical_table_data_by_uid, entity_name_node_id_map, traversal_sequence, physical_connected_traversal)
type SeqFromConnectedTravReturnType<'a> = (
    HashMap<Uuid, PhysicalPlanTableData<'a>>,
    HashMap<String, String>,
    Vec<Uuid>,
    PhysicalConnectedTraversal<'a>,
);
pub fn get_seq_from_connected_traversal<'a>(
    logical_table_data_by_uid: &HashMap<Uuid, TableData<'a>>,
    connected_traversal: &ConnectedTraversal,
    mut physical_table_data_by_uid: HashMap<Uuid, PhysicalPlanTableData<'a>>,
    mut entity_name_node_id_map: HashMap<String, String>,
    mut traversal_sequence: Vec<Uuid>,
    graph_schema: &GraphSchema,
) -> Result<SeqFromConnectedTravReturnType<'a>, OptimizerError> {
    let start_node_phy: PhysicalPlanTableData;
    let mut rel_node_phy: PhysicalPlanTableData;
    let end_node_phy: PhysicalPlanTableData;

    let start_node_logical_table_data = logical_table_data_by_uid
        .get(&connected_traversal.start_node)
        .ok_or(OptimizerError::NoLogicalTableDataForUid)?;
    let start_table_name = start_node_logical_table_data
        .table_name
        .ok_or(OptimizerError::MissingLabel)?
        .to_string();

    let relation_logical_table_data = logical_table_data_by_uid
        .get(&connected_traversal.relationship)
        .ok_or(OptimizerError::NoLogicalTableDataForUid)?;
    let relation_label = relation_logical_table_data
        .table_name
        .ok_or(OptimizerError::MissingLabel)?
        .to_string();

    let end_node_logical_table_data = logical_table_data_by_uid
        .get(&connected_traversal.end_node)
        .ok_or(OptimizerError::NoLogicalTableDataForUid)?;
    let end_table_name = end_node_logical_table_data
        .table_name
        .ok_or(OptimizerError::MissingLabel)?
        .to_string();

    if let Some(start_node_phy_table_data) =
        physical_table_data_by_uid.get(&connected_traversal.start_node)
    {
        start_node_phy = start_node_phy_table_data.clone();
    } else {
        // TODO check for better ways to generate alphanumeric unique ids. Clickhouse is yelling when all first 10 digits from uuid are numbers.
        let mut start_table_alias = format!(
            "a{}",
            connected_traversal.start_node.to_string()[..10]
                .to_string()
                .replace("-", "")
        );
        if let Some(entity_name) = start_node_logical_table_data.entity_name {
            start_table_alias = entity_name.to_string();
        }

        let start_node_schema = graph_schema
            .nodes
            .get(&start_table_name)
            .ok_or(OptimizerError::NoNodeSchemaFound)?;

        let start_node_phy_table_data = PhysicalPlanTableData {
            id: connected_traversal.start_node,
            node_id: start_node_schema.node_id.column.clone(),
            table_alias: start_table_alias.clone(),
            table_name: start_table_name.clone(),
            temp_table_name: format!("{}_{}", start_table_name, start_table_alias),
            is_eagerly_evaluated: true,
            is_relationship: false,
            join_condition: None,
            forward_join_condition: None,
        };
        physical_table_data_by_uid.insert(
            connected_traversal.start_node,
            start_node_phy_table_data.clone(),
        );
        entity_name_node_id_map
            .insert(start_table_alias, start_node_phy_table_data.node_id.clone());
        start_node_phy = start_node_phy_table_data;
    }

    let relation_phy_table_name = get_relationship_table_name(
        start_table_name,
        end_table_name.clone(),
        relation_label,
        graph_schema,
    )?;

    let mut rel_table_alias = format!(
        "a{}",
        connected_traversal.relationship.to_string()[..10]
            .to_string()
            .replace("-", "")
    );
    if let Some(entity_name) = relation_logical_table_data.entity_name {
        rel_table_alias = entity_name.to_string();
    }

    let relation_phy_table_data = PhysicalPlanTableData {
        id: connected_traversal.relationship,
        node_id: "from_id".to_string(),
        table_alias: rel_table_alias.clone(),
        table_name: relation_phy_table_name.clone(),
        temp_table_name: format!("{}_{}", relation_phy_table_name, rel_table_alias),
        is_eagerly_evaluated: true,
        is_relationship: true,
        join_condition: Some(JoinCondition {
            node_id: start_node_phy.node_id.clone(),
            table_alias: start_node_phy.table_alias.clone(),
            table_name: start_node_phy.table_name.clone(),
            temp_table_name: start_node_phy.temp_table_name.clone(),
            table_uid: connected_traversal.start_node,
            column_name: "from_id",
        }),
        forward_join_condition: None, // add it at the end
    };
    physical_table_data_by_uid.insert(
        connected_traversal.relationship,
        relation_phy_table_data.clone(),
    );
    entity_name_node_id_map.insert(rel_table_alias, relation_phy_table_data.node_id.clone());
    rel_node_phy = relation_phy_table_data;

    if let Some(end_node_phy_table_data) =
        physical_table_data_by_uid.get(&connected_traversal.end_node)
    {
        end_node_phy = end_node_phy_table_data.clone();
    } else {
        let mut end_table_alias = format!(
            "a{}",
            connected_traversal.end_node.to_string()[..10]
                .to_string()
                .replace("-", "")
        );
        if let Some(entity_name) = end_node_logical_table_data.entity_name {
            end_table_alias = entity_name.to_string();
        }

        let end_node_schema = graph_schema
            .nodes
            .get(&end_table_name)
            .ok_or(OptimizerError::NoNodeSchemaFound)?;

        let end_node_phy_table_data = PhysicalPlanTableData {
            id: connected_traversal.end_node,
            node_id: end_node_schema.node_id.column.clone(),
            table_alias: end_table_alias.clone(),
            table_name: end_table_name.clone(),
            temp_table_name: format!("{}_{}", end_table_name, end_table_alias),
            is_eagerly_evaluated: true,
            is_relationship: false,
            join_condition: Some(JoinCondition {
                node_id: rel_node_phy.node_id.clone(),
                table_alias: rel_node_phy.table_alias.clone(),
                table_name: rel_node_phy.table_name.clone(),
                temp_table_name: rel_node_phy.temp_table_name.clone(),
                table_uid: connected_traversal.relationship,
                column_name: "to_id",
                // reverse_column_name: "Id",
            }),
            forward_join_condition: None,
        };
        physical_table_data_by_uid.insert(
            connected_traversal.end_node,
            end_node_phy_table_data.clone(),
        );
        entity_name_node_id_map.insert(end_table_alias, end_node_phy_table_data.node_id.clone());
        end_node_phy = end_node_phy_table_data;
    }

    let forward_join_condition_for_relation = Some(JoinCondition {
        node_id: end_node_phy.node_id.clone(),
        table_alias: end_node_phy.table_alias.clone(),
        table_name: end_node_phy.table_name.clone(),
        temp_table_name: end_node_phy.temp_table_name.clone(),
        table_uid: connected_traversal.end_node,
        column_name: "to_id",
    });

    rel_node_phy.forward_join_condition = forward_join_condition_for_relation;

    let physical_connected_traversal = PhysicalConnectedTraversal {
        id: connected_traversal.id,
        start_node: start_node_phy,
        relationship: rel_node_phy,
        direction: connected_traversal.direction.clone(),
        end_node: end_node_phy,
    };

    traversal_sequence.push(connected_traversal.start_node);
    traversal_sequence.push(connected_traversal.relationship);
    traversal_sequence.push(connected_traversal.end_node);

    Ok((
        physical_table_data_by_uid,
        entity_name_node_id_map,
        traversal_sequence,
        physical_connected_traversal,
    ))
}

pub fn get_next_traversal(
    traversal_sequence: &[Uuid],
    current_connected_traversal: ConnectedTraversal,
) -> Option<ConnectedTraversal> {
    // check how this current_graph_traversal is connected with existing traversed graph

    if traversal_sequence.contains(&current_connected_traversal.start_node) {
        // no need to change the direction of the graph traversal
        return Some(current_connected_traversal);
    }

    if traversal_sequence.contains(&current_connected_traversal.end_node) {
        //change the direction of the graph traversal and return
        let new_direction;
        if current_connected_traversal.direction == Direction::Incoming {
            new_direction = Direction::Outgoing;
        } else if current_connected_traversal.direction == Direction::Outgoing {
            new_direction = Direction::Incoming;
        } else {
            new_direction = Direction::Either;
        }

        let new_graph_traversal = ConnectedTraversal {
            id: current_connected_traversal.id,
            start_node: current_connected_traversal.end_node,
            relationship: current_connected_traversal.relationship,
            direction: new_direction,
            end_node: current_connected_traversal.start_node,
        };
        return Some(new_graph_traversal);
    }

    None
}

#[cfg(test)]
mod tests {
    use crate::{
        open_cypher_parser::ast::{
            Direction, Expression, Literal, Operator, OperatorApplication, ReturnItem,
        },
        query_engine::types::{NodeIdSchema, NodeSchema, RelationshipSchema},
    };

    use super::*;

    // get_relationship_table_name

    fn make_dummy_graph_schema() -> GraphSchema {
        let mut relationships = HashMap::new();
        relationships.insert(
            "CREATED_BY".to_string(),
            RelationshipSchema {
                table_name: "created_by_tbl".to_string(),
                column_names: vec!["id".to_string()],
                from_node: "Post".to_string(),
                to_node: "User".to_string(),
                from_node_id_dtype: "UUID".to_string(),
                to_node_id_dtype: "UUID".to_string(),
            },
        );
        GraphSchema {
            version: 1,
            nodes: HashMap::new(),
            relationships,
        }
    }

    #[test]
    fn outgoing_when_from_node_matches_start_label() {
        let schema = make_dummy_graph_schema();
        let result = get_relationship_table_name(
            "Post".to_string(),
            "User".to_string(),
            "CREATED_BY".to_string(),
            &schema,
        )
        .expect("should find outgoing relation");
        assert_eq!(result, "CREATED_BY_outgoing");
    }

    #[test]
    fn incoming_when_from_node_matches_end_label() {
        let schema = make_dummy_graph_schema();
        let result = get_relationship_table_name(
            "User".to_string(),
            "Post".to_string(),
            "CREATED_BY".to_string(),
            &schema,
        )
        .expect("should find incoming relation");
        assert_eq!(result, "CREATED_BY_incoming");
    }

    #[test]
    fn error_when_relation_label_not_in_schema() {
        let schema = make_dummy_graph_schema();
        let err = get_relationship_table_name(
            "Post".to_string(),
            "User".to_string(),
            "UNKNOWN_REL".to_string(),
            &schema,
        )
        .unwrap_err();
        assert!(matches!(err, OptimizerError::NoRelationSchemaFound));
    }

    #[test]
    fn error_when_labels_do_not_match_from_node() {
        let schema = make_dummy_graph_schema();
        // Neither start nor end matches the schema's from_node
        let err = get_relationship_table_name(
            "Comment".to_string(),
            "Like".to_string(),
            "CREATED_BY".to_string(),
            &schema,
        )
        .unwrap_err();
        assert!(matches!(err, OptimizerError::NoRelationSchemaFound));
    }

    // get_seq_from_connected_traversal

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

    fn make_schema() -> GraphSchema {
        // two node schemas: "Start" and "End"
        let mut nodes = std::collections::HashMap::new();
        nodes.insert(
            "Start".to_string(),
            NodeSchema {
                table_name: "Start".to_string(),
                column_names: vec!["col".to_string()],
                primary_keys: "col".to_string(),
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
                column_names: vec!["col".to_string()],
                primary_keys: "col".to_string(),
                node_id: NodeIdSchema {
                    column: "end_id".to_string(),
                    dtype: "UUID".to_string(),
                },
            },
        );

        // one relationship schema: "Rel" from Start → End
        let mut rels = std::collections::HashMap::new();
        rels.insert(
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
            relationships: rels,
        }
    }

    #[test]
    fn test_get_seq_happy_path_creates_all_three() {
        let start = Uuid::new_v4();
        let rel = Uuid::new_v4();
        let end = Uuid::new_v4();

        let mut logical = std::collections::HashMap::new();
        // entity_name Some so aliases come from these, not from UUID
        logical.insert(start, td("Start", Some("startAlias")));
        logical.insert(rel, td("Rel", Some("relAlias")));
        logical.insert(end, td("End", Some("endAlias")));

        let conn = ConnectedTraversal {
            id: Uuid::new_v4(),
            start_node: start,
            relationship: rel,
            direction: Direction::Outgoing,
            end_node: end,
        };

        let (phys_map, name_map, seq, pc) = get_seq_from_connected_traversal(
            &logical,
            &conn,
            std::collections::HashMap::new(),
            std::collections::HashMap::new(),
            Vec::new(),
            &make_schema(),
        )
        .unwrap();

        // should have inserted exactly 3 entries
        assert_eq!(phys_map.len(), 3);
        // sequence order
        assert_eq!(seq, vec![start, rel, end]);

        // aliases → node_ids
        assert_eq!(name_map.get("startAlias").unwrap(), "start_id");
        assert_eq!(name_map.get("relAlias").unwrap(), "from_id");
        assert_eq!(name_map.get("endAlias").unwrap(), "end_id");

        // PhysicalConnectedTraversal should preserve ids & direction
        assert_eq!(pc.id, conn.id);
        assert_eq!(pc.direction, conn.direction);

        // start node should have no join_conditions
        assert!(pc.start_node.join_condition.is_none());
        assert!(pc.start_node.forward_join_condition.is_none());

        // relationship entry: join_condition on from_id, and forward_join_condition set
        assert!(pc.relationship.join_condition.is_some());
        assert!(pc.relationship.forward_join_condition.is_some());

        // end node: has a join_condition pointing back to rel, but no forward
        assert!(pc.end_node.join_condition.is_some());
        assert!(pc.end_node.forward_join_condition.is_none());
    }

    #[test]
    fn test_get_seq_reuses_existing_physical_entry() {
        let start = Uuid::new_v4();
        let rel = Uuid::new_v4();
        let end = Uuid::new_v4();

        let mut logical = std::collections::HashMap::new();
        logical.insert(start, td("Start", Some("SA")));
        logical.insert(rel, td("Rel", Some("RA")));
        logical.insert(end, td("End", Some("EA")));

        let conn = ConnectedTraversal {
            id: Uuid::new_v4(),
            start_node: start,
            relationship: rel,
            direction: Direction::Either,
            end_node: end,
        };

        // prepopulate physical_map with a custom start entry
        let mut existing_phys = std::collections::HashMap::new();
        let custom = PhysicalPlanTableData {
            id: start,
            node_id: "xxx".into(),
            table_alias: "custom".into(),
            table_name: "CUSTOM".into(),
            temp_table_name: "CUSTOM_CUSTOM".into(),
            is_eagerly_evaluated: false,
            is_relationship: false,
            join_condition: None,
            forward_join_condition: None,
        };
        existing_phys.insert(start, custom.clone());

        let (phys_map, _, seq, pc) = get_seq_from_connected_traversal(
            &logical,
            &conn,
            existing_phys,
            std::collections::HashMap::new(),
            Vec::new(),
            &make_schema(),
        )
        .unwrap();

        // it should have preserved our custom start entry
        assert_eq!(phys_map.get(&start).unwrap(), &custom);
        // and the final traversal start_node should also be that same custom
        assert_eq!(pc.start_node, custom);
        // sequence still populated
        assert_eq!(seq, vec![start, rel, end]);
    }

    #[test]
    fn test_get_seq_err_missing_logical_node() {
        let mut logical = std::collections::HashMap::new();
        // only insert start, miss relationship & end
        let start = Uuid::new_v4();
        logical.insert(start, td("Start", None));

        let conn = ConnectedTraversal {
            id: Uuid::new_v4(),
            start_node: start,
            relationship: Uuid::new_v4(),
            direction: Direction::Outgoing,
            end_node: Uuid::new_v4(),
        };

        let err = get_seq_from_connected_traversal(
            &logical,
            &conn,
            std::collections::HashMap::new(),
            std::collections::HashMap::new(),
            Vec::new(),
            &make_schema(),
        )
        .unwrap_err();

        assert!(matches!(err, OptimizerError::NoLogicalTableDataForUid));
    }

    #[test]
    fn test_get_seq_err_missing_label() {
        let mut logical = std::collections::HashMap::new();
        let id = Uuid::new_v4();
        // table_name = None triggers MissingLabel
        logical.insert(
            id,
            TableData {
                entity_name: None,
                table_name: None,
                return_items: vec![],
                where_conditions: vec![],
                order_by_items: vec![],
            },
        );
        let conn = ConnectedTraversal {
            id: Uuid::new_v4(),
            start_node: id,
            relationship: id,
            direction: Direction::Incoming,
            end_node: id,
        };

        let err = get_seq_from_connected_traversal(
            &logical,
            &conn,
            std::collections::HashMap::new(),
            std::collections::HashMap::new(),
            Vec::new(),
            &make_schema(),
        )
        .unwrap_err();

        assert!(matches!(err, OptimizerError::MissingLabel));
    }

    #[test]
    fn test_get_seq_err_no_node_schema() {
        let mut logical = std::collections::HashMap::new();
        let id = Uuid::new_v4();
        logical.insert(id, td("UnknownNode", None));

        let conn = ConnectedTraversal {
            id: Uuid::new_v4(),
            start_node: id,
            relationship: id,
            direction: Direction::Incoming,
            end_node: id,
        };

        let err = get_seq_from_connected_traversal(
            &logical,
            &conn,
            std::collections::HashMap::new(),
            std::collections::HashMap::new(),
            Vec::new(),
            &make_schema(), // make_schema only has Start/End
        )
        .unwrap_err();

        assert!(matches!(err, OptimizerError::NoNodeSchemaFound));
    }

    // get_next_traversal
    #[test]
    fn returns_original_if_start_in_sequence() {
        let start = Uuid::new_v4();
        let rel = Uuid::new_v4();
        let end = Uuid::new_v4();
        let conn = ConnectedTraversal {
            id: Uuid::new_v4(),
            start_node: start,
            relationship: rel,
            direction: Direction::Outgoing,
            end_node: end,
        };

        let seq = vec![start];
        let result = get_next_traversal(&seq, conn.clone());
        assert_eq!(result, Some(conn));
    }

    #[test]
    fn flips_direction_when_only_end_in_sequence_incoming_to_outgoing() {
        let start = Uuid::new_v4();
        let rel = Uuid::new_v4();
        let end = Uuid::new_v4();
        let conn = ConnectedTraversal {
            id: Uuid::new_v4(),
            start_node: start,
            relationship: rel,
            direction: Direction::Incoming,
            end_node: end,
        };

        let seq = vec![end];
        let expected = ConnectedTraversal {
            id: conn.id,
            start_node: end,
            relationship: rel,
            direction: Direction::Outgoing,
            end_node: start,
        };
        assert_eq!(get_next_traversal(&seq, conn), Some(expected));
    }

    #[test]
    fn flips_direction_when_only_end_in_sequence_outgoing_to_incoming() {
        let start = Uuid::new_v4();
        let rel = Uuid::new_v4();
        let end = Uuid::new_v4();
        let conn = ConnectedTraversal {
            id: Uuid::new_v4(),
            start_node: start,
            relationship: rel,
            direction: Direction::Outgoing,
            end_node: end,
        };

        let seq = vec![end];
        let expected = ConnectedTraversal {
            id: conn.id,
            start_node: end,
            relationship: rel,
            direction: Direction::Incoming,
            end_node: start,
        };
        assert_eq!(get_next_traversal(&seq, conn), Some(expected));
    }

    #[test]
    fn retains_either_when_only_end_in_sequence_and_direction_either() {
        let start = Uuid::new_v4();
        let rel = Uuid::new_v4();
        let end = Uuid::new_v4();
        let conn = ConnectedTraversal {
            id: Uuid::new_v4(),
            start_node: start,
            relationship: rel,
            direction: Direction::Either,
            end_node: end,
        };

        let seq = vec![end];
        let expected = ConnectedTraversal {
            id: conn.id,
            start_node: end,
            relationship: rel,
            direction: Direction::Either,
            end_node: start,
        };
        assert_eq!(get_next_traversal(&seq, conn), Some(expected));
    }

    #[test]
    fn returns_none_if_neither_node_in_sequence() {
        let start = Uuid::new_v4();
        let rel = Uuid::new_v4();
        let end = Uuid::new_v4();
        let conn = ConnectedTraversal {
            id: Uuid::new_v4(),
            start_node: start,
            relationship: rel,
            direction: Direction::Outgoing,
            end_node: end,
        };

        let seq = vec![Uuid::new_v4(), Uuid::new_v4()];
        assert_eq!(get_next_traversal(&seq, conn), None);
    }
}
