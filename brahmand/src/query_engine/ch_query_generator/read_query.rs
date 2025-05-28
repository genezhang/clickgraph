use std::collections::HashSet;

use uuid::Uuid;

use crate::query_engine::types::{QueryIR, TraversalMode};

use super::{
    errors::ChQueryGeneratorError, graph_traversal, order_by_statement, select_statement,
    where_statement,
};

pub fn generate_query(
    mut query_ir: QueryIR,
    travesal_mode: &TraversalMode,
) -> Result<Vec<String>, ChQueryGeneratorError> {
    // println!("query_ir in generate_query - {:}", query_ir);

    if !query_ir.physical_plan.physcial_node_traversals.is_empty()
        && !query_ir
            .physical_plan
            .physical_connected_traversals
            .is_empty()
    {
        return Err(ChQueryGeneratorError::DistinctNodeConnectedPattern);
    }

    if query_ir.physical_plan.physcial_node_traversals.is_empty()
        && query_ir
            .physical_plan
            .physical_connected_traversals
            .is_empty()
    {
        // throw error
        return Err(ChQueryGeneratorError::NoPhysicalPlan);
    }

    let mut table_traversal_sql_strings: Vec<String> = vec![];
    let mut visited: HashSet<Uuid> = HashSet::new();

    let final_physical_table_data = if !query_ir
        .physical_plan
        .physical_connected_traversals
        .is_empty()
    {
        query_ir
            .physical_plan
            .physical_connected_traversals
            .last()
            .unwrap()
            .end_node
            .clone()
    } else {
        query_ir
            .physical_plan
            .physcial_node_traversals
            .first()
            .unwrap()
            .clone()
    };

    let last_in_sequence = final_physical_table_data.id;

    let final_logical_table_data = query_ir
        .logical_plan
        .table_data_by_uid
        .get(&last_in_sequence)
        .ok_or(ChQueryGeneratorError::NoLogicalTableDataForUid)?;

    for connected_trav in &query_ir.physical_plan.physical_connected_traversals {
        let start_node_logical_data = query_ir
            .logical_plan
            .table_data_by_uid
            .get(&connected_trav.start_node.id)
            .ok_or(ChQueryGeneratorError::NoLogicalTableDataForUid)?;
        graph_traversal::process_traversal(
            start_node_logical_data,
            &connected_trav.start_node,
            &mut visited,
            &mut table_traversal_sql_strings,
            travesal_mode,
        )?;

        let rel_node_logical_data = query_ir
            .logical_plan
            .table_data_by_uid
            .get(&connected_trav.relationship.id)
            .ok_or(ChQueryGeneratorError::NoLogicalTableDataForUid)?;
        graph_traversal::process_traversal(
            rel_node_logical_data,
            &connected_trav.relationship,
            &mut visited,
            &mut table_traversal_sql_strings,
            travesal_mode,
        )?;

        if !connected_trav.end_node.id.eq(&last_in_sequence) {
            let end_node_logical_data = query_ir
                .logical_plan
                .table_data_by_uid
                .get(&connected_trav.end_node.id)
                .ok_or(ChQueryGeneratorError::NoLogicalTableDataForUid)?;
            graph_traversal::process_traversal(
                end_node_logical_data,
                &connected_trav.end_node,
                &mut visited,
                &mut table_traversal_sql_strings,
                travesal_mode,
            )?;
        }
    }

    let order_by_statement =
        order_by_statement::generate_order_by(query_ir.logical_plan.order_by_items)?;

    let mut limit_statement = "".to_string();
    if let Some(limit) = query_ir.logical_plan.limit {
        limit_statement = format!("LIMIT {}", limit);
    }

    let mut skip_statement = "".to_string();
    if let Some(skip) = query_ir.logical_plan.skip {
        skip_statement = format!("SKIP {}", skip);
    }

    let from_table = final_physical_table_data.table_name.clone();
    let final_table_alias = final_physical_table_data.table_alias.clone();

    let (select_statement, group_by_statement) =
        select_statement::generate_final_select_statements(
            query_ir.logical_plan.overall_return_items,
            &query_ir.physical_plan.entity_name_node_id_map,
        )?;
    let where_statement = where_statement::generate_where_statements(
        final_logical_table_data.where_conditions.clone(),
        true,
    )?;

    let mut reverse_visited: HashSet<Uuid> = HashSet::new();

    let mut join_statements = "".to_string();

    query_ir
        .physical_plan
        .physical_connected_traversals
        .reverse();
    for connected_trav in query_ir.physical_plan.physical_connected_traversals {
        let new_join_statement =
            graph_traversal::process_reverse_joins(connected_trav, &mut reverse_visited);
        join_statements = format!(" {} {} ", join_statements, new_join_statement);
    }

    let final_table_string = format!(
        " SELECT {} FROM {} AS {} {} {} {} {} {} {}",
        select_statement,
        from_table,
        final_table_alias,
        join_statements,
        where_statement,
        group_by_statement,
        order_by_statement,
        skip_statement,
        limit_statement
    );

    table_traversal_sql_strings.push(final_table_string);

    // println!(
    //     "table_traversal_sql_strings {:#?}",
    //     table_traversal_sql_strings.join("")
    // );

    Ok(table_traversal_sql_strings)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{
        open_cypher_parser::ast::{Expression, PropertyAccess, ReturnItem},
        query_engine::types::{
            ConnectedTraversal, LogicalPlan, PhysicalConnectedTraversal, PhysicalPlan,
            PhysicalPlanTableData, QueryType, ReturnItemData,
        },
    };

    use super::*;

    // helpers
    fn return_item_builder_with_alias<'a>(
        expr: Expression<'a>,
        alias: Option<&'a str>,
    ) -> ReturnItem<'a> {
        ReturnItem {
            expression: expr,
            alias,
        }
    }

    // generate query

    #[test]
    fn generate_query_two_hop_example() {
        // IDs for the three tables
        let u_id = Uuid::new_v4();
        let f_id = Uuid::new_v4();
        let p_id = Uuid::new_v4();

        // Logical table data: users (u), follows (f), posts (p)
        let mut table_data_by_uid = HashMap::new();
        table_data_by_uid.insert(
            u_id,
            crate::query_engine::types::TableData {
                entity_name: Some("u"),
                table_name: Some("users"),
                return_items: vec![return_item_builder_with_alias(
                    Expression::PropertyAccessExp(PropertyAccess {
                        base: "u",
                        key: "name",
                    }),
                    Some("userName"),
                )],
                where_conditions: vec![],
                order_by_items: vec![],
            },
        );
        table_data_by_uid.insert(
            f_id,
            crate::query_engine::types::TableData {
                entity_name: Some("f"),
                table_name: Some("follows"),
                return_items: vec![],
                where_conditions: vec![],
                order_by_items: vec![],
            },
        );
        table_data_by_uid.insert(
            p_id,
            crate::query_engine::types::TableData {
                entity_name: Some("p"),
                table_name: Some("posts"),
                return_items: vec![return_item_builder_with_alias(
                    Expression::PropertyAccessExp(PropertyAccess {
                        base: "p",
                        key: "title",
                    }),
                    Some("postTitle"),
                )],
                where_conditions: vec![],
                order_by_items: vec![],
            },
        );

        // Overall return items in the same order as a MATCH ... RETURN u.name AS userName, p.title AS postTitle
        let overall_return_items = vec![
            ReturnItemData {
                return_item: return_item_builder_with_alias(
                    Expression::PropertyAccessExp(PropertyAccess {
                        base: "u",
                        key: "name",
                    }),
                    Some("userName"),
                ),
                belongs_to_table: "u",
            },
            ReturnItemData {
                return_item: return_item_builder_with_alias(
                    Expression::PropertyAccessExp(PropertyAccess {
                        base: "p",
                        key: "title",
                    }),
                    Some("postTitle"),
                ),
                belongs_to_table: "p",
            },
        ];

        let logical_plan = LogicalPlan {
            connected_traversals: vec![ConnectedTraversal {
                id: Uuid::new_v4(),
                start_node: u_id,
                relationship: f_id,
                direction: crate::open_cypher_parser::ast::Direction::Outgoing,
                end_node: p_id,
            }],
            node_traversals: vec![],
            overall_condition: None,
            overall_return_items: overall_return_items.clone(),
            table_data_by_uid: table_data_by_uid.clone(),
            entity_name_uid_map: HashMap::new(),
            return_item_by_alias: HashMap::new(),
            order_by_items: vec![],
            skip: None,
            limit: None,
        };

        // Physical plan tables
        let start_phy = PhysicalPlanTableData {
            id: u_id,
            node_id: "user_id".to_string(),
            table_alias: "u".to_string(),
            table_name: "users".to_string(),
            temp_table_name: "users_u".to_string(),
            is_eagerly_evaluated: true,
            is_relationship: false,
            join_condition: None,
            forward_join_condition: None,
        };
        let rel_phy = PhysicalPlanTableData {
            id: f_id,
            node_id: "from_id".to_string(),
            table_alias: "f".to_string(),
            table_name: "follows".to_string(),
            temp_table_name: "follows_f".to_string(),
            is_eagerly_evaluated: true,
            is_relationship: true,
            join_condition: Some(crate::query_engine::types::JoinCondition {
                node_id: "user_id".to_string(),
                table_uid: u_id,
                table_alias: "u".to_string(),
                table_name: "users".to_string(),
                temp_table_name: "users_u".to_string(),
                column_name: "user_id",
            }),
            forward_join_condition: Some(crate::query_engine::types::JoinCondition {
                node_id: "post_id".to_string(),
                table_uid: p_id,
                table_alias: "p".to_string(),
                table_name: "posts".to_string(),
                temp_table_name: "posts_p".to_string(),
                column_name: "to_id",
            }),
        };
        let end_phy = PhysicalPlanTableData {
            id: p_id,
            node_id: "post_id".to_string(),
            table_alias: "p".to_string(),
            table_name: "posts".to_string(),
            temp_table_name: "posts_p".to_string(),
            is_eagerly_evaluated: true,
            is_relationship: false,
            join_condition: None,
            forward_join_condition: None,
        };

        let physical_plan = PhysicalPlan {
            physical_connected_traversals: vec![PhysicalConnectedTraversal {
                id: Uuid::new_v4(),
                start_node: start_phy.clone(),
                relationship: rel_phy.clone(),
                direction: crate::open_cypher_parser::ast::Direction::Outgoing,
                end_node: end_phy.clone(),
            }],
            physcial_node_traversals: vec![],
            entity_name_node_id_map: {
                let mut m = HashMap::new();
                m.insert("u".to_string(), "user_id".to_string());
                m.insert("f".to_string(), "from_id".to_string());
                m.insert("p".to_string(), "post_id".to_string());
                m
            },
        };

        let query_ir = QueryIR {
            query_type: QueryType::Read,
            logical_plan,
            physical_plan,
        };

        // Generate
        let queries = generate_query(query_ir, &TraversalMode::TempTable).unwrap();

        // We should get exactly three statements
        assert_eq!(queries.len(), 3);

        // 1. users temp table
        assert_eq!(
            queries[0],
            "CREATE TEMPORARY TABLE users_u AS SELECT user_id, name FROM users "
        );
        // 2. follows temp table
        assert_eq!(
            queries[1],
            "CREATE TEMPORARY TABLE follows_f AS SELECT from_id, arrayJoin(bitmapToArray(to_id)) AS to_id FROM follows WHERE from_id IN (SELECT user_id FROM users_u)"
        );
        // 3. final SELECT joining back through the edge
        assert_eq!(
            queries[2],
            " SELECT u.name AS userName, p.title AS postTitle FROM posts AS p    JOIN users_u AS u ON u.user_id = f.user_id   GROUP BY userName, postTitle   "
        );
    }
}
