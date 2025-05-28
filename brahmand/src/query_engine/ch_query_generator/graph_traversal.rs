use std::collections::HashSet;

use uuid::Uuid;

use crate::query_engine::types::{
    PhysicalConnectedTraversal, PhysicalPlanTableData, TableData, TraversalMode,
};

use super::{errors::ChQueryGeneratorError, select_statement, where_statement};

pub fn generate_node_traversal(
    logical_table_data: &TableData,
    physical_table_data: &PhysicalPlanTableData,
    join_string: String,
    travesal_mode: &TraversalMode,
    is_first_node: bool,
) -> Result<String, ChQueryGeneratorError> {
    let from_table = &physical_table_data.table_name;

    let table_alias = &physical_table_data.temp_table_name;
    let node_id = &physical_table_data.node_id;

    let select_statement = if physical_table_data.is_relationship {
        select_statement::generate_relationship_select_statements(
            logical_table_data.return_items.clone(),
            node_id,
        )?
    } else {
        select_statement::generate_node_select_statements(
            logical_table_data.return_items.clone(),
            node_id,
        )?
    };
    let mut where_statement = where_statement::generate_where_statements(
        logical_table_data.where_conditions.clone(),
        false,
    )?;

    if physical_table_data.join_condition.is_some() {
        if !where_statement.is_empty() {
            where_statement = format!("{} AND {}", where_statement, join_string);
        } else {
            where_statement = format!("WHERE {}", join_string);
        }
    }

    let table_string;

    if travesal_mode == &TraversalMode::Cte {
        if is_first_node {
            table_string = format!(
                "WITH {} AS (SELECT {} FROM {} {})",
                table_alias, select_statement, from_table, where_statement
            );
        } else {
            table_string = format!(
                ", {} AS (SELECT {} FROM {} {})",
                table_alias, select_statement, from_table, where_statement
            );
        }
    } else {
        table_string = format!(
            "CREATE TEMPORARY TABLE {} AS SELECT {} FROM {} {}",
            table_alias, select_statement, from_table, where_statement
        );
    }

    Ok(table_string)
}

pub fn process_traversal(
    node_logical_table_data: &TableData,
    node_physical_data: &PhysicalPlanTableData,
    visited: &mut HashSet<Uuid>,
    table_traversal_sql_strings: &mut Vec<String>,
    travesal_mode: &TraversalMode,
) -> Result<(), ChQueryGeneratorError> {
    if !visited.contains(&node_physical_data.id) {
        let mut forward_join = "".to_string();
        if let Some(join_condition) = &node_physical_data.join_condition {
            if node_physical_data.is_relationship {
                // relationships will always join on nodes so it will always be Id in the inner select statement
                forward_join = format!(
                    "from_id IN (SELECT {joining_node_id} FROM {joining_table})",
                    joining_node_id = join_condition.node_id,
                    joining_table = join_condition.temp_table_name
                );
            } else {
                forward_join = format!(
                    "{node_id} IN (SELECT {joining_column} FROM {joining_table})",
                    node_id = node_physical_data.node_id,
                    joining_column = join_condition.column_name,
                    joining_table = join_condition.temp_table_name
                );
            }
        }

        let mut is_first_node = false;
        if visited.is_empty() {
            is_first_node = true;
        }

        let table_traversal_sql = generate_node_traversal(
            node_logical_table_data,
            node_physical_data,
            forward_join,
            travesal_mode,
            is_first_node,
        )?;
        table_traversal_sql_strings.push(table_traversal_sql);
        visited.insert(node_physical_data.id);
    }
    Ok(())
}

pub fn process_reverse_joins(
    physical_connected_traversal: PhysicalConnectedTraversal,
    reverse_visited: &mut HashSet<Uuid>,
) -> String {
    let mut join_statements: Vec<String> = vec![];

    let start_node_physical_table_data = &physical_connected_traversal.start_node;
    let rel_physical_table_data = &physical_connected_traversal.relationship;
    let end_node_physical_table_data = &physical_connected_traversal.end_node;

    let start_node_table_alias = &start_node_physical_table_data.table_alias;
    let rel_table_alias = &rel_physical_table_data.table_alias;
    let end_node_table_alias = &end_node_physical_table_data.table_alias;

    let start_node_temp_table_name = &start_node_physical_table_data.temp_table_name;
    let rel_temp_table_name = &rel_physical_table_data.temp_table_name;
    let end_node_temp_table_name = &end_node_physical_table_data.temp_table_name;

    let start_node_id = &start_node_physical_table_data.node_id;
    let end_node_id = &end_node_physical_table_data.node_id;

    // in this case both nodes are alread joined. We need to make sure that the current relationship joins correctly with both of these nodes.
    // In this case join the relation on both start node and the end node.
    if reverse_visited.contains(&start_node_physical_table_data.id)
        && reverse_visited.contains(&end_node_physical_table_data.id)
    {
        // get start node join column name here
        let mut start_node_join_column_name = "";
        if let Some(join_condition) = &rel_physical_table_data.join_condition {
            start_node_join_column_name = join_condition.column_name;
        }

        // get end node join column name here
        let mut end_node_join_column_name = "";
        if let Some(forward_join_condition) = &rel_physical_table_data.forward_join_condition {
            end_node_join_column_name = forward_join_condition.column_name;
        }

        let reverse_rel_join = format!(
            " JOIN {rel_temp_table_name} AS {rel_table_alias} ON {rel_table_alias}.{start_node_join_column_name} = {start_node_table_alias}.{start_node_id} AND {rel_table_alias}.{end_node_join_column_name} = {end_node_table_alias}.{end_node_id}"
        );

        join_statements.push(reverse_rel_join);
        reverse_visited.insert(rel_physical_table_data.id);
    } else
    // if the start node is present in the reverse_visited then
    // use join clause for relationship and use forward join for end node
    if reverse_visited.contains(&start_node_physical_table_data.id) {
        // join relationship

        if let Some(join_condition) = &rel_physical_table_data.join_condition {
            let joining_column = join_condition.column_name;

            let reverse_rel_join = format!(
                " JOIN {rel_temp_table_name} AS {rel_table_alias} ON {rel_table_alias}.{joining_column} = {start_node_table_alias}.{start_node_id}"
            );
            join_statements.push(reverse_rel_join);
            reverse_visited.insert(rel_physical_table_data.id);
        }

        // join end node if it is not present
        if let Some(forward_join_condition) = &rel_physical_table_data.forward_join_condition {
            // here joined table is end node
            let forward_joining_column = forward_join_condition.column_name;

            let reverse_end_node_join = format!(
                " JOIN {end_node_temp_table_name} AS {end_node_table_alias} ON {end_node_table_alias}.{end_node_id} = {rel_table_alias}.{forward_joining_column}"
            );
            join_statements.push(reverse_end_node_join);
            reverse_visited.insert(end_node_physical_table_data.id);
        }
    } else
    // if the end node is present in the reverse_visited then
    // use forward join for relationship joining and use join clause to join the start node
    if reverse_visited.contains(&end_node_physical_table_data.id) {
        if let Some(forward_join_condition) = &rel_physical_table_data.forward_join_condition {
            // here joined table is end node table
            let forward_joining_column = forward_join_condition.column_name;

            let reverse_rel_join = format!(
                " JOIN {rel_temp_table_name} AS {rel_table_alias} ON {rel_table_alias}.{forward_joining_column} = {end_node_table_alias}.{end_node_id}"
            );
            join_statements.push(reverse_rel_join);
            reverse_visited.insert(rel_physical_table_data.id);
        }

        // join start node here
        if let Some(join_condition) = &rel_physical_table_data.join_condition {
            // here joined table is start node
            let joining_column = join_condition.column_name;

            let reverse_start_node_join = format!(
                " JOIN {start_node_temp_table_name} AS {start_node_table_alias} ON {start_node_table_alias}.{start_node_id} = {rel_table_alias}.{joining_column}"
            );
            join_statements.push(reverse_start_node_join);
            reverse_visited.insert(start_node_physical_table_data.id);
        }
    }
    // if both nodes are not in visited then we can assume that it is the starting point of reverse traversal.
    //  Here join relationship with end node and start node with relation.
    else {
        // join relationship here
        if let Some(join_condition) = &end_node_physical_table_data.join_condition {
            // here joined table is start node
            let joining_column = join_condition.column_name;

            let reverse_start_node_join = format!(
                " JOIN {rel_temp_table_name} AS {rel_table_alias} ON {rel_table_alias}.{joining_column} = {end_node_table_alias}.{end_node_id}"
            );
            join_statements.push(reverse_start_node_join);
            reverse_visited.insert(rel_physical_table_data.id);
        }

        // join start node here
        if let Some(join_condition) = &rel_physical_table_data.join_condition {
            // here joined table is start node

            let joining_column = join_condition.column_name;

            let reverse_start_node_join = format!(
                " JOIN {start_node_temp_table_name} AS {start_node_table_alias} ON {start_node_table_alias}.{start_node_id} = {rel_table_alias}.{joining_column}"
            );
            join_statements.push(reverse_start_node_join);
            reverse_visited.insert(start_node_physical_table_data.id);
        }
    }

    join_statements.join("")
}

#[cfg(test)]
mod tests {
    use crate::{
        open_cypher_parser::ast::{
            Direction, Expression, Literal, Operator, OperatorApplication, PropertyAccess,
            ReturnItem,
        },
        query_engine::types::JoinCondition,
    };

    use super::*;

    // Build a ReturnItem with given expression and no alias
    fn return_item_builder<'a>(expr: Expression<'a>) -> ReturnItem<'a> {
        ReturnItem {
            expression: expr,
            alias: None,
        }
    }

    // generate_node_traversal

    /// Build a TableData for the `users` table, returning only `u.age`.
    fn make_user_table_data(where_conds: Vec<OperatorApplication<'static>>) -> TableData<'static> {
        TableData {
            entity_name: Some("u"),
            table_name: Some("users"),
            return_items: vec![return_item_builder(Expression::PropertyAccessExp(
                PropertyAccess {
                    base: "u",
                    key: "age",
                },
            ))],
            where_conditions: where_conds,
            order_by_items: vec![],
        }
    }

    /// Build a PhysicalPlanTableData for `users_u`.
    fn make_physical_user(
        user_id: Uuid,
        join_cond: Option<JoinCondition<'static>>,
    ) -> PhysicalPlanTableData<'static> {
        PhysicalPlanTableData {
            id: user_id,
            node_id: "user_id".to_string(),
            table_alias: "u".to_string(),
            table_name: "users".to_string(),
            temp_table_name: "users_u".to_string(),
            is_eagerly_evaluated: true,
            is_relationship: false,
            join_condition: join_cond,
            forward_join_condition: None,
        }
    }

    #[test]
    fn user_temp_table_no_where_no_join() {
        let td = make_user_table_data(vec![]);
        let pd = make_physical_user(Uuid::new_v4(), None);
        let sql =
            generate_node_traversal(&td, &pd, "ignored".into(), &TraversalMode::TempTable, true)
                .unwrap();
        assert_eq!(
            sql,
            "CREATE TEMPORARY TABLE users_u AS SELECT user_id, age FROM users "
        );
    }

    #[test]
    fn user_temp_table_with_where() {
        let cond = OperatorApplication {
            operator: Operator::GreaterThan,
            operands: vec![
                Expression::PropertyAccessExp(PropertyAccess {
                    base: "u",
                    key: "age",
                }),
                Expression::Literal(Literal::Integer(18)),
            ],
        };
        let td = make_user_table_data(vec![cond]);
        let pd = make_physical_user(Uuid::new_v4(), None);
        let sql =
            generate_node_traversal(&td, &pd, "".into(), &TraversalMode::TempTable, false).unwrap();
        assert_eq!(
            sql,
            "CREATE TEMPORARY TABLE users_u AS SELECT user_id, age FROM users WHERE age > 18"
        );
    }

    #[test]
    fn user_temp_table_with_join() {
        let cond = OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                Expression::PropertyAccessExp(PropertyAccess {
                    base: "u",
                    key: "age",
                }),
                Expression::Literal(Literal::Integer(21)),
            ],
        };
        let user_id = Uuid::new_v4();
        let join_cond = JoinCondition {
            node_id: "user_id".to_string(),
            table_alias: "u".to_string(),
            table_name: "users".to_string(),
            temp_table_name: "users_u".to_string(),
            table_uid: user_id,
            column_name: "user_id",
        };
        let td = make_user_table_data(vec![cond]);
        let pd = make_physical_user(user_id, Some(join_cond));
        let sql = generate_node_traversal(
            &td,
            &pd,
            "u.user_id = other.user_id".into(),
            &TraversalMode::TempTable,
            false,
        )
        .unwrap();
        assert_eq!(
            sql,
            "CREATE TEMPORARY TABLE users_u AS SELECT user_id, age FROM users WHERE age = 21 AND u.user_id = other.user_id"
        );
    }

    #[test]
    fn user_cte_first_and_subsequent() {
        let td = make_user_table_data(vec![]);
        let pd = make_physical_user(Uuid::new_v4(), None);

        let first =
            generate_node_traversal(&td, &pd, "".into(), &TraversalMode::Cte, true).unwrap();
        assert_eq!(first, "WITH users_u AS (SELECT user_id, age FROM users )");

        let next =
            generate_node_traversal(&td, &pd, "".into(), &TraversalMode::Cte, false).unwrap();
        assert_eq!(next, ", users_u AS (SELECT user_id, age FROM users )");
    }

    #[test]
    fn process_traversal_user_basic() {
        let td = make_user_table_data(vec![]);
        let pd = make_physical_user(Uuid::new_v4(), None);
        let mut visited = HashSet::new();
        let mut sqls = Vec::new();

        process_traversal(&td, &pd, &mut visited, &mut sqls, &TraversalMode::TempTable).unwrap();
        assert_eq!(sqls.len(), 1);
        assert!(visited.contains(&pd.id));
        assert_eq!(
            sqls[0],
            "CREATE TEMPORARY TABLE users_u AS SELECT user_id, age FROM users "
        );
    }

    #[test]
    fn process_traversal_user_skip_visited() {
        let user_id = Uuid::new_v4();
        let td = make_user_table_data(vec![]);
        let pd = make_physical_user(user_id, None);
        let mut visited = {
            let mut s = HashSet::new();
            s.insert(user_id);
            s
        };
        let mut sqls = Vec::new();

        process_traversal(&td, &pd, &mut visited, &mut sqls, &TraversalMode::TempTable).unwrap();
        assert!(sqls.is_empty());
    }

    #[test]
    fn process_traversal_user_with_where_and_join() {
        let cond = OperatorApplication {
            operator: Operator::GreaterThan,
            operands: vec![
                Expression::PropertyAccessExp(PropertyAccess {
                    base: "u",
                    key: "age",
                }),
                Expression::Literal(Literal::Integer(30)),
            ],
        };
        let user_id = Uuid::new_v4();
        let join_cond = JoinCondition {
            node_id: "user_id".to_string(),
            table_alias: "u".to_string(),
            table_name: "users".to_string(),
            temp_table_name: "users_u".to_string(),
            table_uid: user_id,
            column_name: "user_id",
        };
        let td = make_user_table_data(vec![cond]);
        let pd = make_physical_user(user_id, Some(join_cond));
        let mut visited = HashSet::new();
        let mut sqls = Vec::new();

        process_traversal(&td, &pd, &mut visited, &mut sqls, &TraversalMode::TempTable).unwrap();
        assert_eq!(
            sqls[0],
            "CREATE TEMPORARY TABLE users_u AS SELECT user_id, age FROM users WHERE age > 30 AND user_id IN (SELECT user_id FROM users_u)"
        );
    }

    #[test]
    fn process_traversal_user_where_error() {
        // provoke UnsupportedItemInWhereClause via a Parameter
        let op = OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                Expression::PropertyAccessExp(PropertyAccess {
                    base: "u",
                    key: "age",
                }),
                Expression::Parameter("p"),
            ],
        };
        let td = make_user_table_data(vec![op.clone()]);
        let pd = make_physical_user(Uuid::new_v4(), None);
        let mut visited = HashSet::new();
        let mut sqls = Vec::new();

        let err = process_traversal(&td, &pd, &mut visited, &mut sqls, &TraversalMode::TempTable)
            .unwrap_err();
        assert!(matches!(
            err,
            ChQueryGeneratorError::UnsupportedItemInWhereClause
        ));
    }

    // process reverse join

    // Helper to build a PhysicalPlanTableData with given params
    fn physical_plan_table_data_builder(
        id: Uuid,
        node_id: &str,
        table_alias: &str,
        table_name: &str,
        temp_table: &str,
        join_cond: Option<JoinCondition<'static>>,
        forward_join: Option<JoinCondition<'static>>,
        is_rel: bool,
    ) -> PhysicalPlanTableData<'static> {
        PhysicalPlanTableData {
            id,
            node_id: node_id.to_string(),
            table_alias: table_alias.to_string(),
            table_name: table_name.to_string(),
            temp_table_name: temp_table.to_string(),
            is_eagerly_evaluated: true,
            is_relationship: is_rel,
            join_condition: join_cond,
            forward_join_condition: forward_join,
        }
    }

    fn make_social_traversal() -> PhysicalConnectedTraversal<'static> {
        // Simulate: User -[FOLLOWS]-> Post
        let user_id = Uuid::new_v4();
        let rel_id = Uuid::new_v4();
        let post_id = Uuid::new_v4();

        // Start node: users table
        let start = physical_plan_table_data_builder(
            user_id, "user_id", "u", "users", "users_ut", None, None, false,
        );

        // Relationship: follows table
        let rel_join = JoinCondition {
            column_name: "user_id",
            node_id: "".to_string(),
            table_alias: "".to_string(),
            table_name: "".to_string(),
            temp_table_name: "".to_string(),
            table_uid: user_id,
        };
        let rel_fwd = JoinCondition {
            column_name: "post_id",
            node_id: "".to_string(),
            table_alias: "".to_string(),
            table_name: "".to_string(),
            temp_table_name: "".to_string(),
            table_uid: post_id,
        };
        let rel = physical_plan_table_data_builder(
            rel_id,
            "from_id",
            "f",
            "follows",
            "follows_ft",
            Some(rel_join),
            Some(rel_fwd),
            true,
        );

        // End node: posts table
        let post_join = JoinCondition {
            column_name: "post_id",
            node_id: "".to_string(),
            table_alias: "".to_string(),
            table_name: "".to_string(),
            temp_table_name: "".to_string(),
            table_uid: rel_id,
        };
        let end = physical_plan_table_data_builder(
            post_id,
            "post_id",
            "p",
            "posts",
            "posts_pt",
            Some(post_join),
            None,
            false,
        );

        PhysicalConnectedTraversal {
            id: Uuid::new_v4(),
            start_node: start,
            relationship: rel,
            direction: Direction::Outgoing,
            end_node: end,
        }
    }

    #[test]
    fn neither_visited_social_example() {
        let pct = make_social_traversal();
        let mut visited = HashSet::new();
        let sql = process_reverse_joins(pct.clone(), &mut visited);

        let expected = " JOIN follows_ft AS f ON f.post_id = p.post_id JOIN users_ut AS u ON u.user_id = f.user_id";
        assert_eq!(sql, expected);
        assert!(visited.contains(&pct.relationship.id));
    }

    #[test]
    fn only_user_visited_social_example() {
        let pct = make_social_traversal();
        let mut visited = {
            let mut s = HashSet::new();
            s.insert(pct.start_node.id);
            s
        };
        let sql = process_reverse_joins(pct.clone(), &mut visited);

        let expected = " JOIN follows_ft AS f ON f.user_id = u.user_id JOIN posts_pt AS p ON p.post_id = f.post_id";
        assert_eq!(sql, expected);
        assert!(visited.contains(&pct.relationship.id));
        assert!(visited.contains(&pct.end_node.id));
    }

    #[test]
    fn only_post_visited_social_example() {
        let pct = make_social_traversal();
        let mut visited = {
            let mut s = HashSet::new();
            s.insert(pct.end_node.id);
            s
        };
        let sql = process_reverse_joins(pct.clone(), &mut visited);

        let expected = " JOIN follows_ft AS f ON f.post_id = p.post_id JOIN users_ut AS u ON u.user_id = f.user_id";
        assert_eq!(sql, expected);
        assert!(visited.contains(&pct.relationship.id));
        assert!(visited.contains(&pct.start_node.id));
    }

    #[test]
    fn both_visited_social_example() {
        let pct = make_social_traversal();
        let mut visited = {
            let mut s = HashSet::new();
            s.insert(pct.start_node.id);
            s.insert(pct.end_node.id);
            s
        };
        let sql = process_reverse_joins(pct.clone(), &mut visited);

        let expected = " JOIN follows_ft AS f ON f.user_id = u.user_id AND f.post_id = p.post_id";
        assert_eq!(sql, expected);
        assert!(visited.contains(&pct.relationship.id));
    }
}
