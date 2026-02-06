//! AST Transformation Module
//!
//! Pre-planning transformations on the Cypher AST before logical plan generation.
//! This includes rewriting functions like `id()` that need session context.

pub mod id_function;

pub use id_function::IdFunctionTransformer;

use crate::open_cypher_parser::ast::{
    CypherStatement, Expression, Literal, MatchClause, OpenCypherQueryAst, ReadingClause,
    WithClause,
};
use crate::server::bolt_protocol::id_mapper::IdMapper;

/// Transform all id() function calls in a CypherStatement
pub fn transform_id_functions<'a>(
    stmt: CypherStatement<'a>,
    id_mapper: &'a IdMapper,
    schema: Option<&'a crate::graph_catalog::GraphSchema>,
) -> CypherStatement<'a> {
    log::info!("🔄 AST id() transformation starting");
    let transformer = IdFunctionTransformer::new(id_mapper, schema);

    match stmt {
        CypherStatement::Query {
            query,
            union_clauses,
        } => {
            log::debug!(
                "  Transforming main query with {} MATCH clauses",
                query.match_clauses.len()
            );
            let transformed = CypherStatement::Query {
                query: transform_query(query, &transformer),
                union_clauses: union_clauses
                    .into_iter()
                    .map(|uc| crate::open_cypher_parser::ast::UnionClause {
                        union_type: uc.union_type,
                        query: transform_query(uc.query, &transformer),
                    })
                    .collect(),
            };
            log::info!("✅ AST id() transformation complete");
            transformed
        }
        CypherStatement::ProcedureCall(pc) => {
            log::debug!("  Skipping transformation for procedure call");
            // Procedure calls don't have id() functions
            CypherStatement::ProcedureCall(pc)
        }
    }
}

/// Transform id() functions in a single query
fn transform_query<'a>(
    mut query: OpenCypherQueryAst<'a>,
    transformer: &IdFunctionTransformer<'a>,
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
    transformer: &IdFunctionTransformer<'a>,
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
    transformer: &IdFunctionTransformer<'a>,
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
