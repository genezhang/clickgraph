use crate::{open_cypher_parser::ast::{Expression, OperatorApplication, ReturnItem}, query_engine::types::{GraphSchema, RelationshipSchema, TableData}};
use super::errors::OptimizerError;


fn get_column_name_from_expression(exp: &Expression) -> Option<String> {
    match exp {
        Expression::OperatorApplicationExp(op_ex) =>{
            for operand in &op_ex.operands {
                if let Some(column_name) =  get_column_name_from_expression(operand){
                    return Some(column_name)
                }
            }
            None
        }
        Expression::Literal(_) => None,
        Expression::Variable(_) => None,
        Expression::Parameter(_) => None,
        Expression::List(_) => None,
        Expression::FunctionCallExp(function_call) => {
            for arg in &function_call.args {
                if let Some(column_name) =  get_column_name_from_expression(arg){
                    return Some(column_name)
                }
            }
            None
        },
        Expression::PropertyAccessExp(property_access) => {
            Some(property_access.key.to_string())
        },
        Expression::PathPattern(_) => None,
    }
}

fn get_column_name_from_return_items(return_items: &Vec<ReturnItem>) -> Option<String>{
    for return_item in return_items.iter() {
        if let Some(column_name) = get_column_name_from_expression(&return_item.expression) {
            return Some(column_name)
        }
    }
    None
}

fn get_column_name_from_where_conditions(where_conditions: &Vec<OperatorApplication>)-> Option<String>{
    for where_condition in where_conditions.iter() {
        for operand in &where_condition.operands {
            if let Some(column_name) =  get_column_name_from_expression(operand){
                return Some(column_name)
            }
        }
    }
    None
}

fn get_table_name_from_where_and_return(graph_schema: &GraphSchema, node_table_data: &TableData,) -> Option<String> {
    let column_name = if let Some(extracted_column) =  get_column_name_from_where_conditions(&node_table_data.where_conditions) {
        extracted_column
    }else if let Some(extracted_column) = get_column_name_from_return_items(&node_table_data.return_items){
        extracted_column
    }else{
        "".to_string()
    };
    if !column_name.is_empty() {
        for (_, node_schema) in graph_schema.nodes.iter() {
            if node_schema.column_names.contains(&column_name) {
                return Some(node_schema.table_name.clone());
            }
        } 
    }
    None
}

pub fn get_table_names(graph_schema: &GraphSchema, start_node_table_data: &TableData, rel_table_data: &TableData, end_node_table_data: &TableData) ->  Result<(String, String, String), OptimizerError>{
    
    // println!("\n\nstart_node_table_name {:?}", start_node_table_data.table_name);
    // println!("rel_table_name {:?}", rel_table_data.table_name);
    // println!("end_node_table_name {:?}", end_node_table_data.table_name);
    
    // if all present
    if start_node_table_data.table_name.is_some() && rel_table_data.table_name.is_some() && end_node_table_data.table_name.is_some() {
        let start_node_table_name = start_node_table_data.table_name.ok_or(OptimizerError::MissingNodeLabel)?.to_string();
        let end_node_table_name = end_node_table_data.table_name.ok_or(OptimizerError::MissingNodeLabel)?.to_string();
        let rel_table_name = rel_table_data.table_name.ok_or(OptimizerError::MissingRelationLabel)?.to_string();
        return Ok((start_node_table_name, rel_table_name, end_node_table_name));
    }


    // only start node missing
    if start_node_table_data.table_name.is_none() && rel_table_data.table_name.is_some() && end_node_table_data.table_name.is_some() {
        // check relation table name and infer the node
        let rel_table_name = rel_table_data.table_name.ok_or(OptimizerError::MissingRelationLabel)?; // redundant ok_or
        let rel_schema = graph_schema.relationships.get(rel_table_name).ok_or(OptimizerError::NoRelationSchemaFound)?;

        let end_table_name = end_node_table_data.table_name.ok_or(OptimizerError::MissingNodeLabel)?;
        
        let start_table_name = if end_table_name == rel_schema.from_node {
            rel_schema.to_node.clone()
        } else {
            rel_schema.from_node.clone()
        };
        return Ok((start_table_name, rel_table_name.to_string(), end_table_name.to_string() ))
    }

    // only end node missing
    if start_node_table_data.table_name.is_some() && rel_table_data.table_name.is_some() && end_node_table_data.table_name.is_none() {
        // check relation table name and infer the node
        let rel_table_name = rel_table_data.table_name.ok_or(OptimizerError::MissingRelationLabel)?; // redundant ok_or
        let rel_schema = graph_schema.relationships.get(rel_table_name).ok_or(OptimizerError::NoRelationSchemaFound)?;

        let start_table_name = start_node_table_data.table_name.ok_or(OptimizerError::MissingNodeLabel)?;

        let end_table_name = if start_table_name == rel_schema.from_node {
            rel_schema.to_node.clone()
        } else {
            rel_schema.from_node.clone()
        };
        return Ok((start_table_name.to_string(), rel_table_name.to_string(), end_table_name ))
    }

    // only relation missing
    if start_node_table_data.table_name.is_some() && rel_table_data.table_name.is_none() && end_node_table_data.table_name.is_some() {
        let start_table_name = start_node_table_data.table_name.ok_or(OptimizerError::MissingNodeLabel)?;
        let end_table_name = end_node_table_data.table_name.ok_or(OptimizerError::MissingNodeLabel)?;
        for (_, relation_schema ) in graph_schema.relationships.iter() {
            if (relation_schema.from_node == start_table_name && relation_schema.to_node == end_table_name) || 
            (relation_schema.from_node == end_table_name && relation_schema.to_node == start_table_name) {
                return Ok((start_table_name.to_string(), relation_schema.table_name.clone(), end_table_name.to_string()))
            }
        }
        return Err(OptimizerError::MissingRelationLabel);
    }

    // both start and end nodes are missing but relation is present
    if start_node_table_data.table_name.is_none() && rel_table_data.table_name.is_some() && end_node_table_data.table_name.is_none() {
        let rel_table_name = rel_table_data.table_name.ok_or(OptimizerError::MissingRelationLabel)?;
        let relation_schema = graph_schema.relationships.get(rel_table_name).ok_or(OptimizerError::NoRelationSchemaFound)?;

        let extracted_start_node_table_result = get_table_name_from_where_and_return(graph_schema, start_node_table_data);
        let extracted_end_node_table_result = get_table_name_from_where_and_return(graph_schema, end_node_table_data);
        // Check the location of extracted nodes in the rel schema because the start and end of a graph changes with direction
        if extracted_start_node_table_result.is_some() {
            let start_table_name = extracted_start_node_table_result.unwrap();
            
            let end_table_name = if relation_schema.from_node == start_table_name {
                &graph_schema.nodes.get(&relation_schema.to_node).ok_or(OptimizerError::NoNodeSchemaFound)?.table_name
            } else {
                &graph_schema.nodes.get(&relation_schema.from_node).ok_or(OptimizerError::NoNodeSchemaFound)?.table_name
            };
            return Ok((start_table_name, rel_table_name.to_string(), end_table_name.to_string()))
        }else if extracted_end_node_table_result.is_some() {
            let end_table_name = extracted_end_node_table_result.unwrap();
            
            let start_table_name = if relation_schema.from_node == end_table_name {
                &graph_schema.nodes.get(&relation_schema.to_node).ok_or(OptimizerError::NoNodeSchemaFound)?.table_name
            } else {
                &graph_schema.nodes.get(&relation_schema.from_node).ok_or(OptimizerError::NoNodeSchemaFound)?.table_name
            };
            return Ok((start_table_name.to_string(), rel_table_name.to_string(), end_table_name))
        } else {
            // assign default start and end from rel schema. 
            let start_table_name = &graph_schema.nodes.get(&relation_schema.from_node).ok_or(OptimizerError::NoNodeSchemaFound)?.table_name;
            let end_table_name = &graph_schema.nodes.get(&relation_schema.to_node).ok_or(OptimizerError::NoNodeSchemaFound)?.table_name;
            return Ok((start_table_name.to_string(), rel_table_name.to_string(), end_table_name.to_string()))
        }

    }


    // end and relation missing
    if start_node_table_data.table_name.is_some() && rel_table_data.table_name.is_none() && end_node_table_data.table_name.is_none() {
        // If the relation is absent and other node is present then check for a relation with one node = other node which is present.
        // If multiple such relations are found then use current nodes where conditions and return items like above to infer the table name of current node
        // We do this to correctly identify the correct node. We will utilize all available data to infer the current node. 
        // e.g. Suppose there are nodes USER, PLANET, TOWN, SHIP. and both PLANET and TOWN has property 'name'.
        // QUERY: (b:USER)-[]->(a) Where a.name = 'Mars'.
        // If we directly go for node's where conditions and return items then we will get two nodes PLANET and TOWN and we won't be able to decide.
        // If our graph has (USER)-[DRIVES]->(CAR) and (USER)-[IS_FROM]-(TOWN). In this case how to decide DRIVES or IS_FROM relation?
        // Now we will check if CAR or TOWN has property 'name' and infer that as a current node
        let start_table_name = start_node_table_data.table_name.ok_or(OptimizerError::MissingNodeLabel)?;
        let mut relations_found: Vec<&RelationshipSchema> = vec![];
        
        for (_, relation_schema ) in graph_schema.relationships.iter() {
            if relation_schema.from_node == start_table_name || relation_schema.to_node == start_table_name {
                relations_found.push(relation_schema);
            }
        }


        let extracted_end_node_table_result = get_table_name_from_where_and_return(graph_schema, end_node_table_data);


        if relations_found.len() > 1 && extracted_end_node_table_result.is_some(){
            let extracted_end_node_table_name = extracted_end_node_table_result.unwrap();
            for relation_schema in relations_found {    
                let rel_table_name = &relation_schema.table_name;
                // if the existing start node and extracted end node table is present in the current relation 
                // then use the current relation and new end node name
                if (relation_schema.from_node == start_table_name && relation_schema.to_node == extracted_end_node_table_name) 
                    || relation_schema.to_node == start_table_name && relation_schema.from_node == extracted_end_node_table_name{
                    let end_table_name = extracted_end_node_table_name;
                    return Ok((start_table_name.to_string(), rel_table_name.to_string(), end_table_name.to_string()))
                }
            }

        }else {
            let relation_schema = relations_found.first().ok_or(OptimizerError::MissingRelationLabel)?;
            
            let end_table_name = if relation_schema.from_node == start_table_name {
                &graph_schema.nodes.get(&relation_schema.to_node).ok_or(OptimizerError::NoNodeSchemaFound)?.table_name
            }else {
                &graph_schema.nodes.get(&relation_schema.from_node).ok_or(OptimizerError::NoNodeSchemaFound)?.table_name
            };
            let rel_table_name = &relation_schema.table_name;
            return Ok((start_table_name.to_string(), rel_table_name.to_string(), end_table_name.to_string()))
        }



    }

    // start and relation missing
    // Do the same as above but for end node 
    if start_node_table_data.table_name.is_none() && rel_table_data.table_name.is_none() && end_node_table_data.table_name.is_some() {
        let end_table_name = end_node_table_data.table_name.ok_or(OptimizerError::MissingNodeLabel)?;
        let mut relations_found: Vec<&RelationshipSchema> = vec![];
        
        for (_, relation_schema ) in graph_schema.relationships.iter() {
            if relation_schema.from_node == end_table_name || relation_schema.to_node == end_table_name {
                relations_found.push(relation_schema);
            }
        }

        let extracted_start_node_table_result = get_table_name_from_where_and_return(graph_schema, start_node_table_data);


        if relations_found.len() > 1 && extracted_start_node_table_result.is_some(){
            
            let extracted_start_node_table_name = extracted_start_node_table_result.unwrap();
            for relation_schema in relations_found {    
                let rel_table_name = &relation_schema.table_name;
                // if the existing end node is present at from_node in relation 
                // and the start node's extracted column is present in curren found relation's column names 
                // then use the current relation and new start node name

                if (relation_schema.from_node == end_table_name && relation_schema.to_node == extracted_start_node_table_name) 
                    || relation_schema.to_node == end_table_name && relation_schema.from_node == extracted_start_node_table_name{
                    let start_table_name = extracted_start_node_table_name;
                    return Ok((start_table_name.to_string(), rel_table_name.to_string(), end_table_name.to_string()))
                }
            }

        }else {
            let relation_schema = relations_found.first().ok_or(OptimizerError::MissingRelationLabel)?;
        
            let start_table_name = if relation_schema.from_node == end_table_name {
                &graph_schema.nodes.get(&relation_schema.to_node).ok_or(OptimizerError::NoNodeSchemaFound)?.table_name
            }else {
                &graph_schema.nodes.get(&relation_schema.from_node).ok_or(OptimizerError::NoNodeSchemaFound)?.table_name
            };
            let rel_table_name = &relation_schema.table_name;
            return Ok((start_table_name.to_string(), rel_table_name.to_string(), end_table_name.to_string()));
        }
    }


    // if all labels are missing 
    if start_node_table_data.table_name.is_none() && rel_table_data.table_name.is_none() && end_node_table_data.table_name.is_none() {
        let extracted_start_node_table_result = get_table_name_from_where_and_return(graph_schema, start_node_table_data);
        let extracted_end_node_table_result = get_table_name_from_where_and_return(graph_schema, end_node_table_data);
        // if both extracted nodes are present
        if extracted_start_node_table_result.is_some() && extracted_end_node_table_result.is_some() {
            let start_table_name = extracted_start_node_table_result.unwrap();
            let end_table_name = extracted_end_node_table_result.unwrap();

            for (_, relation_schema ) in graph_schema.relationships.iter() {
                if (relation_schema.from_node == start_table_name && relation_schema.to_node == end_table_name)
                || (relation_schema.from_node == end_table_name && relation_schema.to_node == start_table_name){
                    let rel_table_name = &relation_schema.table_name;
                    return Ok((start_table_name, rel_table_name.to_string(), end_table_name));
                }
            }
        } 
        // only start node is extracted but not able to extract the end node
        else if extracted_start_node_table_result.is_some() && extracted_end_node_table_result.is_none() {
            let start_table_name = extracted_start_node_table_result.unwrap();
            for (_, relation_schema ) in graph_schema.relationships.iter() {

                if relation_schema.from_node == start_table_name {
                    let end_table_name = &graph_schema.nodes.get(&relation_schema.to_node).ok_or(OptimizerError::NoNodeSchemaFound)?.table_name;
                    let rel_table_name = &relation_schema.table_name;
                    return Ok((start_table_name, rel_table_name.to_string(), end_table_name.to_string()));
                }else if relation_schema.to_node == start_table_name {
                    let end_table_name = &graph_schema.nodes.get(&relation_schema.from_node).ok_or(OptimizerError::NoNodeSchemaFound)?.table_name;
                    let rel_table_name = &relation_schema.table_name;
                    return Ok((start_table_name, rel_table_name.to_string(), end_table_name.to_string()));
                }
                
            }
        }
        // only end node is extracted but not able to extract the start node
        else if extracted_start_node_table_result.is_none() && extracted_end_node_table_result.is_some() {
            let end_table_name = extracted_end_node_table_result.unwrap();
            for (_, relation_schema ) in graph_schema.relationships.iter() {
                if relation_schema.from_node == end_table_name {
                    let start_table_name = &graph_schema.nodes.get(&relation_schema.to_node).ok_or(OptimizerError::NoNodeSchemaFound)?.table_name;
                    let rel_table_name = &relation_schema.table_name;
                    return Ok((start_table_name.to_string(), rel_table_name.to_string(), end_table_name));
                }else if relation_schema.to_node == end_table_name  {
                    let start_table_name = &graph_schema.nodes.get(&relation_schema.from_node).ok_or(OptimizerError::NoNodeSchemaFound)?.table_name;
                    let rel_table_name = &relation_schema.table_name;
                    return Ok((start_table_name.to_string(), rel_table_name.to_string(), end_table_name));
                }
                
            }
        }

    }


    Err(OptimizerError::NotEnoughLabels)

    
}




#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{open_cypher_parser::ast::{FunctionCall, Literal, Operator, PropertyAccess}, query_engine::types::{NodeIdSchema, NodeSchema}};

    use super::*;

    #[test]
    fn test_get_column_name_from_property_access() {
        let expr = Expression::PropertyAccessExp(PropertyAccess { base: "n", key: "col1" });
        assert_eq!(get_column_name_from_expression(&expr), Some("col1".to_string()));
    }

    #[test]
    fn test_get_column_name_from_operator_application() {
        let nested = Expression::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Addition,
            operands: vec![
                Expression::Literal(Literal::Integer(42)),
                Expression::PropertyAccessExp(PropertyAccess { base: "x", key: "col2" }),
            ],
        });
        assert_eq!(get_column_name_from_expression(&nested), Some("col2".to_string()));
    }

    #[test]
    fn test_get_column_name_from_function_call() {
        let func = Expression::FunctionCallExp(FunctionCall {
            name: "foo".into(),
            args: vec![Expression::PropertyAccessExp(PropertyAccess { base: "y", key: "col3" })],
        });
        assert_eq!(get_column_name_from_expression(&func), Some("col3".to_string()));
    }

    #[test]
    fn test_get_column_name_none_for_literals_and_others() {
        let lit = Expression::Literal(Literal::String("value"));
        let var = Expression::Variable("v");
        let param = Expression::Parameter("p");
        let list = Expression::List(vec![Expression::Literal(Literal::Boolean(true))]);
        
        assert!(get_column_name_from_expression(&lit).is_none());
        assert!(get_column_name_from_expression(&var).is_none());
        assert!(get_column_name_from_expression(&param).is_none());
        assert!(get_column_name_from_expression(&list).is_none());
    }

    #[test]
    fn test_get_column_name_from_return_items() {
        let items = vec![
            ReturnItem { expression: Expression::Literal(Literal::Integer(1)), alias: None },
            ReturnItem { expression: Expression::PropertyAccessExp(PropertyAccess { base: "z", key: "col4" }), alias: Some("alias") },
        ];
        assert_eq!(get_column_name_from_return_items(&items), Some("col4".to_string()));

        let empty = vec![ReturnItem { expression: Expression::Literal(Literal::Null), alias: None }];
        assert!(get_column_name_from_return_items(&empty).is_none());
    }

    #[test]
    fn test_get_column_name_from_where_conditions() {
        let conds = vec![
            OperatorApplication {
                operator: Operator::Equal,
                operands: vec![
                    Expression::Literal(Literal::Float(3.14)),
                    Expression::PropertyAccessExp(PropertyAccess { base: "w", key: "col5" }),
                ],
            }
        ];
        assert_eq!(get_column_name_from_where_conditions(&conds), Some("col5".to_string()));

        let no_props = vec![OperatorApplication { operator: Operator::Not, operands: vec![Expression::Literal(Literal::Boolean(false))] }];
        assert!(get_column_name_from_where_conditions(&no_props).is_none());
    }


    fn sample_person_car_schema() -> GraphSchema {
        let mut nodes = HashMap::new();
        nodes.insert(
            "Person".into(),
            NodeSchema {
                table_name: "person_table".into(),
                column_names: vec!["name".into(), "age".into()],
                primary_keys: "id".into(),
                node_id: NodeIdSchema { column: "id".into(), dtype: "Int".into() },
            },
        );
        nodes.insert(
            "Car".into(),
            NodeSchema {
                table_name: "car_table".into(),
                column_names: vec!["model".into(), "year".into()],
                primary_keys: "vin".into(),
                node_id: NodeIdSchema { column: "vin".into(), dtype: "String".into() },
            },
        );
        GraphSchema { version: 1, nodes, relationships: HashMap::new() }
    }

    fn make_where_condition(key: &'static str) -> OperatorApplication<'static> {
        OperatorApplication {
            operator: Operator::Equal,
            operands: vec![
                Expression::PropertyAccessExp(PropertyAccess { base: "n", key }),
                Expression::Literal(Literal::Integer(0)),
            ],
        }
    }

    fn make_return_item(key: &'static str) -> ReturnItem<'static> {
        ReturnItem {
            expression: Expression::PropertyAccessExp(PropertyAccess { base: "n", key }),
            alias: None,
        }
    }

    #[test]
    fn where_conditions_match() {
        let schema = sample_person_car_schema();
        let td = TableData {
            entity_name: None,
            table_name: None,
            return_items: vec![],
            where_conditions: vec![ make_where_condition("year") ],
            order_by_items: vec![],
        };
        assert_eq!(get_table_name_from_where_and_return(&schema, &td), Some("car_table".to_string()));
    }

    #[test]
    fn falls_back_to_return_items() {
        let schema = sample_person_car_schema();
        let td = TableData {
            entity_name: None,
            table_name: None,
            return_items: vec![ make_return_item("name") ],
            where_conditions: vec![],
            order_by_items: vec![],
        };
        assert_eq!(get_table_name_from_where_and_return(&schema, &td), Some("person_table".to_string()));
    }

    #[test]
    fn where_precedence_over_return() {
        let schema = sample_person_car_schema();
        let td = TableData {
            entity_name: None,
            table_name: None,
            return_items: vec![ make_return_item("model") ], // matches Car
            where_conditions: vec![ make_where_condition("name") ], // matches Person
            order_by_items: vec![],
        };
        // Should pick from where_conditions first => Person
        assert_eq!(get_table_name_from_where_and_return(&schema, &td), Some("person_table".to_string()));
    }

    #[test]
    fn no_match_returns_none() {
        let schema = sample_person_car_schema();
        let td = TableData {
            entity_name: None,
            table_name: None,
            return_items: vec![ make_return_item("foo") ],
            where_conditions: vec![ make_where_condition("bar") ],
            order_by_items: vec![],
        };
        assert_eq!(get_table_name_from_where_and_return(&schema, &td), None);
    }

    #[test]
    fn empty_inputs_return_none() {
        let schema = sample_person_car_schema();
        let td = TableData {
            entity_name: None,
            table_name: None,
            return_items: vec![],
            where_conditions: vec![],
            order_by_items: vec![],
        };
        assert_eq!(get_table_name_from_where_and_return(&schema, &td), None);
    }

    fn sample_user_order_purchase_schema() -> GraphSchema {
        let mut nodes = HashMap::new();
        nodes.insert(
            "User".to_string(),
            NodeSchema {
                table_name: "user_table".to_string(),
                column_names: vec!["name".to_string()],
                primary_keys: "id".to_string(),
                node_id: NodeIdSchema { column: "id".to_string(), dtype: "Int".to_string() },
            },
        );
        nodes.insert(
            "Order".to_string(),
            NodeSchema {
                table_name: "order_table".to_string(),
                column_names: vec!["amount".to_string()],
                primary_keys: "order_id".to_string(),
                node_id: NodeIdSchema { column: "order_id".to_string(), dtype: "Int".to_string() },
            },
        );
        let mut relationships = HashMap::new();
        relationships.insert(
            "purchases".to_string(),
            RelationshipSchema {
                table_name: "purchases".to_string(),
                column_names: vec![],
                from_node: "User".to_string(),
                to_node: "Order".to_string(),
                from_node_id_dtype: "Int".to_string(),
                to_node_id_dtype: "Int".to_string(),
            },
        );
        GraphSchema { version: 1, nodes, relationships }
    }

    fn td(name: Option<&'static str>) -> TableData<'static> {
        TableData { entity_name: None, table_name: name, return_items: vec![], where_conditions: vec![], order_by_items: vec![] }
    }

    #[test]
    fn all_present() {
        let schema = sample_user_order_purchase_schema();
        let start = td(Some("User"));
        let rel = td(Some("purchases"));
        let end = td(Some("Order"));
        let result = get_table_names(&schema, &start, &rel, &end).unwrap();
        assert_eq!(result, ("User".to_string(), "purchases".to_string(), "Order".to_string()));
    }

    #[test]
    fn only_start_missing() {
        let schema = sample_user_order_purchase_schema();
        let start = td(None);
        let rel = td(Some("purchases"));
        let end = td(Some("Order"));
        let result = get_table_names(&schema, &start, &rel, &end).unwrap();
        assert_eq!(result, ("User".to_string(), "purchases".to_string(), "Order".to_string()));
    }

    #[test]
    fn only_end_missing() {
        let schema = sample_user_order_purchase_schema();
        let start = td(Some("User"));
        let rel = td(Some("purchases"));
        let end = td(None);
        let result = get_table_names(&schema, &start, &rel, &end).unwrap();
        assert_eq!(result, ("User".to_string(), "purchases".to_string(), "Order".to_string()));
    }

    #[test]
    fn only_relation_missing() {
        let schema = sample_user_order_purchase_schema();
        let start = td(Some("User"));
        let rel = td(None);
        let end = td(Some("Order"));
        let result = get_table_names(&schema, &start, &rel, &end).unwrap();
        assert_eq!(result, ("User".to_string(), "purchases".to_string(), "Order".to_string()));
    }

    #[test]
    fn only_relation_missing_reversed() {
        let schema = sample_user_order_purchase_schema();
        let start = td(Some("Order"));
        let rel = td(None);
        let end = td(Some("User"));
        let result = get_table_names(&schema, &start, &rel, &end).unwrap();
        assert_eq!(result, ("Order".to_string(), "purchases".to_string(), "User".to_string()));
    }

    #[test]
    fn missing_relation_error() {
        let mut schema = sample_user_order_purchase_schema();
        schema.relationships.clear();
        let start = td(Some("User"));
        let rel = td(None);
        let end = td(Some("Order"));
        let err = get_table_names(&schema, &start, &rel, &end).unwrap_err();
        assert_eq!(err, OptimizerError::MissingRelationLabel);
    }

    #[test]
    fn not_enough_labels_error() {
        let schema = sample_user_order_purchase_schema();
        let start = td(None);
        let rel = td(None);
        let end = td(None);
        let err = get_table_names(&schema, &start, &rel, &end).unwrap_err();
        assert_eq!(err, OptimizerError::NotEnoughLabels);
    }


}
