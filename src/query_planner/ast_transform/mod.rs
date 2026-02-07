//! AST Transformation Module
//!
//! Pre-planning transformations on the Cypher AST before logical plan generation.
//! This includes rewriting functions like `id()` that need session context.

pub mod id_function;

pub use id_function::IdFunctionTransformer;

use crate::open_cypher_parser::ast::{
    CypherStatement, Expression, Literal, MatchClause, OpenCypherQueryAst, ReadingClause,
    WithClause, Operator, OperatorApplication,
};
use crate::server::bolt_protocol::id_mapper::IdMapper;
use std::collections::{HashMap, HashSet};

/// Transform all id() function calls in a CypherStatement
pub fn transform_id_functions<'a>(
    stmt: CypherStatement<'a>,
    id_mapper: &'a IdMapper,
    schema: Option<&'a crate::graph_catalog::GraphSchema>,
) -> (CypherStatement<'a>, HashMap<String, HashSet<String>>) {
    log::info!("🔄 AST id() transformation starting");
    let mut transformer = IdFunctionTransformer::new(id_mapper, schema);

    let transformed_stmt = match stmt {
        CypherStatement::Query {
            query,
            union_clauses,
        } => {
            log::debug!(
                "  Transforming main query with {} MATCH clauses",
                query.match_clauses.len()
            );
            let transformed_query = transform_query(query, &mut transformer);
            
            CypherStatement::Query {
                query: transformed_query,
                union_clauses: union_clauses
                    .into_iter()
                    .map(|uc| crate::open_cypher_parser::ast::UnionClause {
                        union_type: uc.union_type,
                        query: transform_query(uc.query, &mut transformer),
                    })
                    .collect(),
            }
        }
        CypherStatement::ProcedureCall(pc) => {
            log::debug!("  Skipping transformation for procedure call");
            // Procedure calls don't have id() functions
            CypherStatement::ProcedureCall(pc)
        }
    };
    
    // Get collected label constraints and id values
    let label_constraints = transformer.get_label_constraints();
    let id_values_by_label = transformer.get_id_values_by_label();
    
    // Apply UNION splitting if we have label constraints AND a schema
    let result = if !label_constraints.is_empty() && schema.is_some() {
        split_query_by_labels(transformed_stmt, &label_constraints, &id_values_by_label, schema)
    } else {
        transformed_stmt
    };
    
    log::info!("✅ AST id() transformation complete");
    (result, label_constraints)
}

/// Transform id() functions in a single query
fn transform_query<'a>(
    mut query: OpenCypherQueryAst<'a>,
    transformer: &mut IdFunctionTransformer<'a>,
) -> OpenCypherQueryAst<'a> {
    // Transform global WHERE clause
    if let Some(where_clause) = query.where_clause {
        query.where_clause = Some(crate::open_cypher_parser::ast::WhereClause {
            conditions: transformer.transform_where(where_clause.conditions),
        });
    }

    // Transform WHERE in MATCH clauses
    query.match_clauses = query
        .match_clauses
        .into_iter()
        .map(|mc| transform_match_clause(mc, transformer))
        .collect();

    // Transform WHERE in OPTIONAL MATCH clauses
    query.optional_match_clauses = query
        .optional_match_clauses
        .into_iter()
        .map(|omc| crate::open_cypher_parser::ast::OptionalMatchClause {
            path_patterns: omc.path_patterns,
            where_clause: omc
                .where_clause
                .map(|wc| crate::open_cypher_parser::ast::WhereClause {
                    conditions: transformer.transform_where(wc.conditions),
                }),
        })
        .collect();

    // Transform WHERE in reading_clauses (unified MATCH/OPTIONAL MATCH)
    query.reading_clauses = query
        .reading_clauses
        .into_iter()
        .map(|rc| match rc {
            ReadingClause::Match(mc) => {
                ReadingClause::Match(transform_match_clause(mc, transformer))
            }
            ReadingClause::OptionalMatch(omc) => {
                ReadingClause::OptionalMatch(crate::open_cypher_parser::ast::OptionalMatchClause {
                    path_patterns: omc.path_patterns,
                    where_clause: omc.where_clause.map(|wc| {
                        crate::open_cypher_parser::ast::WhereClause {
                            conditions: transformer.transform_where(wc.conditions),
                        }
                    }),
                })
            }
        })
        .collect();

    // Transform WITH clause (it can contain subsequent MATCH/OPTIONAL MATCH)
    query.with_clause = query
        .with_clause
        .map(|wc| transform_with_clause(wc, transformer));

    // Transform ORDER BY clause (it's separate from RETURN)
    if let Some(order_by_clause) = query.order_by_clause {
        log::debug!(
            "  Transforming ORDER BY with {} items",
            order_by_clause.order_by_items.len()
        );
        let transformed_items = transformer.transform_order_by(order_by_clause.order_by_items);
        for (idx, item) in transformed_items.iter().enumerate() {
            log::debug!("    ORDER BY[{}]: {:?}", idx, item.expression);
        }
        query.order_by_clause = Some(crate::open_cypher_parser::ast::OrderByClause {
            order_by_items: transformed_items,
        });
    }

    query
}

/// Transform id() functions in a MATCH clause
fn transform_match_clause<'a>(
    mc: MatchClause<'a>,
    transformer: &mut IdFunctionTransformer<'a>,
) -> MatchClause<'a> {
    MatchClause {
        path_patterns: mc.path_patterns,
        where_clause: mc.where_clause.and_then(|wc| {
            let transformed = transformer.transform_where(wc.conditions);
            // Remove trivially true WHERE clauses (e.g., "WHERE true" from "WHERE NOT id(x) IN []")
            match transformed {
                Expression::Literal(Literal::Boolean(true)) => {
                    log::info!("  Removed trivially true WHERE clause (was: NOT id(x) IN [])");
                    None
                }
                _ => Some(crate::open_cypher_parser::ast::WhereClause {
                    conditions: transformed,
                }),
            }
        }),
    }
}

/// Transform id() functions in a WITH clause (including subsequent MATCH/OPTIONAL MATCH)
fn transform_with_clause<'a>(
    mut wc: WithClause<'a>,
    transformer: &mut IdFunctionTransformer<'a>,
) -> WithClause<'a> {
    // Transform WHERE in WITH clause itself
    wc.where_clause =
        wc.where_clause
            .map(|where_clause| crate::open_cypher_parser::ast::WhereClause {
                conditions: transformer.transform_where(where_clause.conditions),
            });

    // Transform ORDER BY in WITH clause
    wc.order_by = wc
        .order_by
        .map(|order_by| crate::open_cypher_parser::ast::OrderByClause {
            order_by_items: transformer.transform_order_by(order_by.order_by_items),
        });

    // Transform subsequent MATCH after WITH
    wc.subsequent_match = wc
        .subsequent_match
        .map(|mc| Box::new(transform_match_clause(*mc, transformer)));

    // Transform subsequent OPTIONAL MATCH after WITH
    wc.subsequent_optional_matches = wc
        .subsequent_optional_matches
        .into_iter()
        .map(|omc| crate::open_cypher_parser::ast::OptionalMatchClause {
            path_patterns: omc.path_patterns,
            where_clause: omc.where_clause.map(|where_clause| {
                crate::open_cypher_parser::ast::WhereClause {
                    conditions: transformer.transform_where(where_clause.conditions),
                }
            }),
        })
        .collect();

    // Recursively transform subsequent WITH (for chained WITH...MATCH...WITH)
    wc.subsequent_with = wc
        .subsequent_with
        .map(|nested_wc| Box::new(transform_with_clause(*nested_wc, transformer)));

    wc
}

/// Split a query into UNION branches based on label constraints from id() expressions.
///
/// When id(var) IN [1,2,3] resolves to multiple node labels (e.g., User and Post),
/// this function:
/// 1. Creates separate query branches for each label
/// 2. Adds IN clauses with label-specific ID values (e.g., a.user_id IN ['1', '3'])
/// 3. Combines branches with UNION ALL
///
/// Example transformation:
/// ```cypher
/// MATCH (a) WHERE id(a) IN [1, 2, 3] RETURN a
/// ```
/// If IDs 1,3 map to User and ID 2 maps to Post, generates:
/// ```sql
/// -- Branch 1: User
/// SELECT ... FROM users WHERE user_id IN ('1', '3')
/// UNION ALL
/// -- Branch 2: Post  
/// SELECT ... FROM posts WHERE post_id IN ('2')
/// ```
fn split_query_by_labels<'a>(
    stmt: CypherStatement<'a>,
    label_constraints: &HashMap<String, HashSet<String>>,
    id_values_by_label: &HashMap<String, HashMap<String, Vec<String>>>,
    schema: Option<&'a crate::graph_catalog::GraphSchema>,
) -> CypherStatement<'a> {
    log::info!("🔀 Splitting query by labels: {:?}", label_constraints);
    
    match stmt {
        CypherStatement::Query { query, union_clauses } => {
            // For now, only split if there are no existing UNION clauses
            if !union_clauses.is_empty() {
                log::debug!("  Skipping split - query already has UNION clauses");
                return CypherStatement::Query { query, union_clauses };
            }
            
            // Find variables with multiple labels
            let multi_label_vars: Vec<_> = label_constraints
                .iter()
                .filter(|(_, labels)| labels.len() > 1)
                .collect();
            
            if multi_label_vars.is_empty() {
                log::debug!("  No multi-label variables - checking for single-label injection");
                
                // Even without splits, inject single labels into the query
                // This ensures type inference works correctly
                let mut modified_query = query;
                for (var_name, labels) in label_constraints {
                    if labels.len() == 1 {
                        let label = labels.iter().next().unwrap();
                        log::info!("  Injecting single label '{}' into variable '{}'", label, var_name);
                        
                        // Get ID values for IN clause generation
                        let id_values = id_values_by_label
                            .get(var_name.as_str())
                            .and_then(|by_label| by_label.get(label))
                            .cloned();
                        
                        // Clone and modify the query to add the label
                        modified_query = clone_query_with_label(&modified_query, var_name, label, id_values, schema);
                    }
                }
                
                return CypherStatement::Query { 
                    query: modified_query, 
                    union_clauses 
                };
            }
            
            // For simplicity, split on the first multi-label variable
            let (var_name, labels) = multi_label_vars[0];
            log::info!("  Splitting on variable '{}' with labels: {:?}", var_name, labels);
            
            // Get ID values for this variable
            let var_id_values = id_values_by_label.get(var_name.as_str());
            
            // Create a query for each label
            let mut queries = Vec::new();
            for label in labels {
                let id_values = var_id_values
                    .and_then(|by_label| by_label.get(label))
                    .cloned();
                
                let cloned = clone_query_with_label(&query, var_name, label, id_values, schema);
                queries.push(cloned);
            }
            
            if queries.is_empty() {
                return CypherStatement::Query { query, union_clauses };
            }
            
            // First query is the main query, rest become UNION ALL clauses
            let main_query = queries.remove(0);
            let new_union_clauses: Vec<crate::open_cypher_parser::ast::UnionClause<'a>> = queries
                .into_iter()
                .map(|q| crate::open_cypher_parser::ast::UnionClause {
                    union_type: crate::open_cypher_parser::ast::UnionType::All,
                    query: q,
                })
                .collect();
            
            log::info!("✅ Split complete - {} UNION branches", new_union_clauses.len() + 1);
            CypherStatement::Query {
                query: main_query,
                union_clauses: new_union_clauses,
            }
        }
        other => other,
    }
}

/// Clone a query and add label constraint + IN clause for a specific variable
fn clone_query_with_label<'a>(
    query: &OpenCypherQueryAst<'a>,
    var_name: &str,
    label: &str,
    id_values: Option<Vec<String>>,
    schema: Option<&'a crate::graph_catalog::GraphSchema>,
) -> OpenCypherQueryAst<'a> {
    let label_static: &'a str = Box::leak(label.to_string().into_boxed_str());
    log::debug!("  Cloning query for label '{}' on variable '{}'", label, var_name);
    
    let mut cloned = query.clone();
    
    // Add label to MATCH clauses
    cloned.match_clauses = cloned
        .match_clauses
        .into_iter()
        .map(|mc| add_label_to_match_clause(mc, var_name, label_static))
        .collect();
    
    // Add label to reading_clauses
    cloned.reading_clauses = cloned
        .reading_clauses
        .into_iter()
        .map(|rc| match rc {
            crate::open_cypher_parser::ast::ReadingClause::Match(mc) => {
                crate::open_cypher_parser::ast::ReadingClause::Match(
                    add_label_to_match_clause(mc, var_name, label_static)
                )
            }
            crate::open_cypher_parser::ast::ReadingClause::OptionalMatch(omc) => {
                crate::open_cypher_parser::ast::ReadingClause::OptionalMatch(omc)
            }
        })
        .collect();
    
    // Replace id() expressions in WHERE clause with IN clause
    if let Some(id_values) = id_values {
        cloned.where_clause = cloned.where_clause.map(|wc| {
            crate::open_cypher_parser::ast::WhereClause {
                conditions: replace_id_expressions_with_in(
                    wc.conditions,
                    var_name,
                    label,
                    &id_values,
                    schema
                ),
            }
        });
    }
    
    cloned
}

/// Replace id() transformed expressions with a single IN clause
/// Removes OR chains of property equality checks and replaces with: var.id_property IN [values]
fn replace_id_expressions_with_in<'a>(
    expr: Expression<'a>,
    var_name: &str,
    label: &str,
    id_values: &[String],
    schema: Option<&'a crate::graph_catalog::GraphSchema>,
) -> Expression<'a> {
    use crate::open_cypher_parser::ast::{Literal, Operator, OperatorApplication, PropertyAccess};
    
    match expr {
        Expression::OperatorApplicationExp(op_app) => {
            match op_app.operator {
                // OR chains are likely from id() transformation - replace them
                Operator::Or => {
                    // Check if this looks like an id() transformed OR chain
                    // (multiple property equality checks)
                    let is_id_chain = op_app.operands.iter().all(|operand| {
                        matches!(operand, 
                            Expression::OperatorApplicationExp(OperatorApplication {
                                operator: Operator::Equal,
                                operands,
                            }) if operands.iter().any(|o| matches!(o, Expression::PropertyAccessExp(_)))
                        )
                    });
                    
                    if is_id_chain {
                        // Replace with IN clause
                        log::debug!("  Replacing OR chain with IN clause for {}.{}", var_name, label);
                        return build_property_in_clause(var_name, label, id_values.to_vec(), schema);
                    }
                    
                    // Not an id() chain, recursively process
                    Expression::OperatorApplicationExp(OperatorApplication {
                        operator: Operator::Or,
                        operands: op_app.operands.into_iter()
                            .map(|o| replace_id_expressions_with_in(o, var_name, label, id_values, schema))
                            .collect(),
                    })
                }
                // AND might contain id() expressions, recursively process
                Operator::And => {
                    let filtered: Vec<_> = op_app.operands.into_iter()
                        .filter_map(|operand| {
                            let processed = replace_id_expressions_with_in(operand, var_name, label, id_values, schema);
                            // Skip TRUE literals (filtered out conditions)
                            match processed {
                                Expression::Literal(Literal::Boolean(true)) => None,
                                _ => Some(processed),
                            }
                        })
                        .collect();
                    
                    match filtered.len() {
                        0 => Expression::Literal(Literal::Boolean(true)),
                        1 => filtered.into_iter().next().unwrap(),
                        _ => Expression::OperatorApplicationExp(OperatorApplication {
                            operator: Operator::And,
                            operands: filtered,
                        }),
                    }
                }
                // Other operators, pass through
                _ => Expression::OperatorApplicationExp(op_app),
            }
        }
        _ => expr,
    }
}

/// Build a property IN clause: var.id_property IN [values]
fn build_property_in_clause<'a>(
    var_name: &str,
    label: &str,
    id_values: Vec<String>,
    schema: Option<&'a crate::graph_catalog::GraphSchema>,
) -> Expression<'a> {
    // Get the id_property from schema
    let id_property = schema
        .and_then(|s| s.node_schema_opt(label))
        .and_then(|node_schema| {
            // Extract the first component of the node_id
            match &node_schema.node_id.id {
                crate::graph_catalog::config::Identifier::Single(col) => Some(col.as_str()),
                crate::graph_catalog::config::Identifier::Composite(cols) => {
                    cols.first().map(|s| s.as_str())
                }
            }
        })
        .unwrap_or("id");
    
    log::info!("  Building IN clause: {}.{} IN [{}]", var_name, id_property, id_values.join(", "));
    
    // Create static references for var_name and id_property
    let var_name_static: &'a str = Box::leak(var_name.to_string().into_boxed_str());
    let id_property_static: &'a str = Box::leak(id_property.to_string().into_boxed_str());
    
    // Create the property access: var.id_property
    let property_expr = Expression::PropertyAccessExp(
        crate::open_cypher_parser::ast::PropertyAccess {
            base: var_name_static,
            key: id_property_static,
        }
    );
    
    // Create the list of values
    let value_list = Expression::List(
        id_values
            .into_iter()
            .map(|v| {
                let v_static: &'a str = Box::leak(v.into_boxed_str());
                Expression::Literal(Literal::String(v_static))
            })
            .collect()
    );
    
    // Create the IN expression: var.id_property IN [values]
    Expression::OperatorApplicationExp(OperatorApplication {
        operator: Operator::In,
        operands: vec![property_expr, value_list],
    })
}

/// Add a label to node patterns with a specific variable name in a MATCH clause
fn add_label_to_match_clause<'a>(
    mut mc: crate::open_cypher_parser::ast::MatchClause<'a>,
    var_name: &str,
    label: &'a str,
) -> crate::open_cypher_parser::ast::MatchClause<'a> {
    use crate::open_cypher_parser::ast::PathPattern;
    
    mc.path_patterns = mc
        .path_patterns
        .into_iter()
        .map(|(path_var, pattern)| {
            let updated_pattern = match pattern {
                // Standalone node pattern
                PathPattern::Node(mut np) => {
                    if np.name == Some(var_name) {
                        np.labels = Some(vec![label]);
                    }
                    PathPattern::Node(np)
                }
                // Connected pattern with relationships
                PathPattern::ConnectedPattern(connected_patterns) => {
                    let updated_patterns = connected_patterns
                        .into_iter()
                        .map(|cp| {
                            // Update start_node if it matches
                            {
                                let mut start = cp.start_node.borrow_mut();
                                if start.name == Some(var_name) {
                                    start.labels = Some(vec![label]);
                                }
                            }
                            
                            // Update end_node if it matches
                            {
                                let mut end = cp.end_node.borrow_mut();
                                if end.name == Some(var_name) {
                                    end.labels = Some(vec![label]);
                                }
                            }
                            
                            cp
                        })
                        .collect();
                    
                    PathPattern::ConnectedPattern(updated_patterns)
                }
                // Pass through shortest path patterns unchanged
                PathPattern::ShortestPath(inner) => PathPattern::ShortestPath(inner),
                PathPattern::AllShortestPaths(inner) => PathPattern::AllShortestPaths(inner),
            };
            
            (path_var, updated_pattern)
        })
        .collect();
    
    mc
}
