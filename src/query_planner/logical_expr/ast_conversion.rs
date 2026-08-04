//! AST to LogicalExpr Conversion
//!
//! This module contains all `From` and `TryFrom` implementations for converting
//! from `open_cypher_parser::ast` types to our logical expression types.
//!
//! Separating these conversions:
//! - Keeps `mod.rs` focused on type definitions
//! - Makes the conversion logic easier to test in isolation
//! - Clarifies the boundary between parser and planner

use std::sync::Arc;

use crate::{
    open_cypher_parser,
    query_planner::logical_plan::{Filter, GraphNode, GraphRel, LogicalPlan},
};

use super::{
    errors, AggregateFnCall, ConnectedPattern, Direction, ExistsSubquery, LambdaExpr, Literal,
    LogicalCase, LogicalExpr, NodePattern, Operator, OperatorApplication, PathPattern,
    PatternCount, Property, PropertyAccess, PropertyKVPair, ReduceExpr, RelationshipPattern,
    ScalarFnCall, TableAlias,
};

// =============================================================================
// Literal Conversions
// =============================================================================

impl<'a> From<open_cypher_parser::ast::Literal<'a>> for Literal {
    fn from(value: open_cypher_parser::ast::Literal) -> Self {
        match value {
            open_cypher_parser::ast::Literal::Integer(val) => Literal::Integer(val),
            open_cypher_parser::ast::Literal::Float(val) => Literal::Float(val),
            open_cypher_parser::ast::Literal::Boolean(val) => Literal::Boolean(val),
            open_cypher_parser::ast::Literal::String(val) => Literal::String(val.to_string()),
            open_cypher_parser::ast::Literal::Null => Literal::Null,
        }
    }
}

// =============================================================================
// Operator Conversions
// =============================================================================

impl From<open_cypher_parser::ast::Operator> for Operator {
    fn from(value: open_cypher_parser::ast::Operator) -> Self {
        match value {
            open_cypher_parser::ast::Operator::Addition => Operator::Addition,
            open_cypher_parser::ast::Operator::Subtraction => Operator::Subtraction,
            open_cypher_parser::ast::Operator::Multiplication => Operator::Multiplication,
            open_cypher_parser::ast::Operator::Division => Operator::Division,
            open_cypher_parser::ast::Operator::ModuloDivision => Operator::ModuloDivision,
            open_cypher_parser::ast::Operator::Exponentiation => Operator::Exponentiation,
            open_cypher_parser::ast::Operator::Equal => Operator::Equal,
            open_cypher_parser::ast::Operator::NotEqual => Operator::NotEqual,
            open_cypher_parser::ast::Operator::LessThan => Operator::LessThan,
            open_cypher_parser::ast::Operator::GreaterThan => Operator::GreaterThan,
            open_cypher_parser::ast::Operator::LessThanEqual => Operator::LessThanEqual,
            open_cypher_parser::ast::Operator::GreaterThanEqual => Operator::GreaterThanEqual,
            open_cypher_parser::ast::Operator::RegexMatch => Operator::RegexMatch,
            open_cypher_parser::ast::Operator::And => Operator::And,
            open_cypher_parser::ast::Operator::Or => Operator::Or,
            open_cypher_parser::ast::Operator::In => Operator::In,
            open_cypher_parser::ast::Operator::NotIn => Operator::NotIn,
            open_cypher_parser::ast::Operator::StartsWith => Operator::StartsWith,
            open_cypher_parser::ast::Operator::EndsWith => Operator::EndsWith,
            open_cypher_parser::ast::Operator::Contains => Operator::Contains,
            open_cypher_parser::ast::Operator::Not => Operator::Not,
            open_cypher_parser::ast::Operator::Distinct => Operator::Distinct,
            open_cypher_parser::ast::Operator::IsNull => Operator::IsNull,
            open_cypher_parser::ast::Operator::IsNotNull => Operator::IsNotNull,
        }
    }
}

// =============================================================================
// Property Access Conversions
// =============================================================================

impl<'a> From<open_cypher_parser::ast::PropertyAccess<'a>> for PropertyAccess {
    fn from(value: open_cypher_parser::ast::PropertyAccess<'a>) -> Self {
        let alias = value.base.to_string();
        let column = value.key.to_string();
        log::trace!(
            "PropertyAccess::from AST: alias='{}', column='{}'",
            alias,
            column
        );
        PropertyAccess {
            table_alias: TableAlias(alias),
            column: crate::graph_catalog::expression_parser::PropertyValue::Column(column),
        }
    }
}

// =============================================================================
// Direction Conversions
// =============================================================================

impl From<open_cypher_parser::ast::Direction> for Direction {
    fn from(value: open_cypher_parser::ast::Direction) -> Self {
        match value {
            open_cypher_parser::ast::Direction::Outgoing => Direction::Outgoing,
            open_cypher_parser::ast::Direction::Incoming => Direction::Incoming,
            open_cypher_parser::ast::Direction::Either => Direction::Either,
        }
    }
}

// =============================================================================
// Operator Application Conversions
// =============================================================================

impl<'a> TryFrom<open_cypher_parser::ast::OperatorApplication<'a>> for OperatorApplication {
    type Error = errors::LogicalExprError;

    fn try_from(
        value: open_cypher_parser::ast::OperatorApplication<'a>,
    ) -> Result<Self, Self::Error> {
        let operands = value
            .operands
            .into_iter()
            .map(LogicalExpr::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(OperatorApplication {
            operator: Operator::from(value.operator),
            operands,
        })
    }
}

// =============================================================================
// Function Call Conversions
// =============================================================================

/// #578: walk a raw AST path pattern looking for a variable-length hop
/// (`*1..2`) or a multi-type OR'd relationship (`[:A|B]`) on any hop — both
/// shapes that `size(...)`/PatternCount silently narrows today (see the
/// GUARDRAIL comment at the size() call site). Returns a human-readable
/// reason for the first offending hop found, or `None` if the whole pattern
/// is safe to convert. Recurses through multi-hop chains and
/// shortestPath()/allShortestPaths() wrappers.
fn path_pattern_has_vlp_or_multi_type(
    pattern: &open_cypher_parser::ast::PathPattern,
) -> Option<String> {
    use open_cypher_parser::ast::PathPattern as AstPathPattern;
    match pattern {
        AstPathPattern::Node(_) => None,
        AstPathPattern::ShortestPath(inner) | AstPathPattern::AllShortestPaths(inner) => {
            path_pattern_has_vlp_or_multi_type(inner)
        }
        AstPathPattern::ConnectedPattern(connected_patterns) => {
            for cp in connected_patterns {
                let rel = &cp.relationship;
                if rel.variable_length.is_some() {
                    return Some("has a variable-length relationship (e.g. *1..2)".to_string());
                }
                if let Some(labels) = rel.labels.as_ref() {
                    if labels.len() > 1 {
                        return Some(format!(
                            "has multiple OR'd relationship types ([:{}])",
                            labels.join("|")
                        ));
                    }
                }
            }
            None
        }
    }
}

impl<'a> TryFrom<open_cypher_parser::ast::FunctionCall<'a>> for LogicalExpr {
    type Error = errors::LogicalExprError;

    fn try_from(value: open_cypher_parser::ast::FunctionCall<'a>) -> Result<Self, Self::Error> {
        let name_lower = value.name.to_lowercase();

        // Special handling for size() with pattern argument
        // size((n)-[:REL]->()) should become PatternCount
        if name_lower == "size" && value.args.len() == 1 {
            if let open_cypher_parser::ast::Expression::PathPattern(ref pp) = value.args[0] {
                // GUARDRAIL (#578): `logical_expr::RelationshipPattern` (the type
                // PatternCount's `PathPattern` bottoms out at) has NO
                // `variable_length` field at all, so a `*1..2`-style hop bound
                // is silently dropped at this exact conversion boundary —
                // before render ever sees it. Multi-type OR'd relationship
                // labels (`[:A|B]`) survive this conversion but are then
                // silently narrowed to the FIRST type by the render-layer SQL
                // generators (`generate_pattern_count_sql` /
                // `generate_multi_hop_pattern_count_sql`), which only ever read
                // `labels.first()`. Either shape means `size(...)` silently
                // returns a count for a narrower pattern than the one written
                // (ground rule 1 violation). Until size()/PatternCount routes
                // through a pipeline that can carry a hop bound and enumerate
                // every OR'd type, reject both shapes loudly instead of
                // silently narrowing.
                if let Some(reason) = path_pattern_has_vlp_or_multi_type(pp) {
                    return Err(errors::LogicalExprError::UnsupportedExpression(format!(
                        "(#578) this size(...) pattern {reason}. ClickGraph does \
                         not yet convert this shape through a pipeline that can \
                         carry a hop bound or enumerate every OR'd relationship \
                         type, so the count would silently narrow to a smaller \
                         pattern than the one written — returning wrong results. \
                         Workaround: restructure the query to count via a \
                         top-level MATCH + count(*)/count(DISTINCT ...) instead \
                         of size(...)."
                    )));
                }
                return Ok(LogicalExpr::PatternCount(PatternCount {
                    pattern: PathPattern::try_from(pp.clone())?,
                }));
            }
        }

        // Standard Neo4j aggregate functions. Includes the standard-deviation
        // aggregates (stDev/stDevP) — #638/#600.3: without these, `stDev(x)` was
        // classified as a ScalarFnCall, so a post-WITH aggregation stage treated
        // it as a GROUPING KEY and emitted `GROUP BY stddevSamp(...)` (ClickHouse
        // Code 184). They are genuine aggregates per openCypher and render
        // correctly via the function registry (stddevSamp / stddevPop).
        //
        // percentileCont/percentileDisc are also genuine aggregates (#639). They
        // now render through `FunctionMapper::percentile_aggregate` as parametric
        // quantiles that HONOR the percentile arg (previously they dropped it and
        // rendered `median`, so classifying them as aggregates would have turned a
        // loud Code 184 into a silent wrong median — that hazard is now gone).
        let agg_fns = [
            "count",
            "min",
            "max",
            "avg",
            "sum",
            "collect",
            "stdev",
            "stdevp",
            "percentilecont",
            "percentiledisc",
        ];

        // Check if it's a standard aggregate function
        let is_standard_agg = agg_fns.contains(&name_lower.as_str());

        // Check if it's a native pass-through aggregate (any backend's prefix).
        // Dialect-agnostic on purpose: the planner runs before the dialect is
        // pinned, so we classify by recognizing every backend's prefixes —
        // `ch.`/`chagg.` (ClickHouse, registry + explicit) and `dbx.`
        // (Databricks, registry). Wrong-backend prefixes are rejected later, at
        // emit time. See `sql_generator::passthrough`.
        let is_passthrough_agg = matches!(
            crate::sql_generator::passthrough::classify_passthrough(&value.name),
            Some(crate::sql_generator::passthrough::PassthroughKind::Aggregate)
        );

        let args = value
            .args
            .into_iter()
            .map(LogicalExpr::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        // Scalar `exists(v.prop)` is the PROPERTY-existence function (distinct from
        // the `EXISTS { pattern }` subquery, which parses as `ExistsExpression`, not
        // `FunctionCallExp`). Cypher semantics: `exists(v.prop)` ≡ `v.prop IS NOT
        // NULL`. No SQL backend has a scalar `exists(x)` function, so left as a
        // `ScalarFnCall` it renders the invalid literal `exists(...)` (Code 46 on
        // Path A) or `WHERE false` (Path C VLP/pattern-comp — silently dropping
        // every row). Lower it here, at the single canonical AST conversion site, so
        // BOTH render paths get the correct `IS NOT NULL`. This is dialect-agnostic
        // by construction: it runs before the dialect is pinned and `IS NOT NULL` is
        // standard SQL, so no per-backend handling is needed.
        //
        // Gated STRICTLY to a single PropertyAccess argument — the only form with an
        // unambiguous, render-correct lowering. Any other shape (bare variable, a
        // computed expression, ≠1 args) stays a `ScalarFnCall` and hits the existing
        // unmapped-function path rather than guessing a semantics we can't prove.
        if name_lower == "exists"
            && args.len() == 1
            && matches!(args[0], LogicalExpr::PropertyAccessExp(_))
        {
            return Ok(super::combinators::is_not_null(
                args.into_iter().next().expect("len checked == 1"),
            ));
        }

        if is_standard_agg || is_passthrough_agg {
            Ok(LogicalExpr::AggregateFnCall(AggregateFnCall {
                name: value.name,
                args,
            }))
        } else {
            Ok(LogicalExpr::ScalarFnCall(ScalarFnCall {
                name: value.name,
                args,
            }))
        }
    }
}

// =============================================================================
// Path Pattern Conversions
// =============================================================================

impl<'a> TryFrom<open_cypher_parser::ast::PathPattern<'a>> for PathPattern {
    type Error = errors::LogicalExprError;

    fn try_from(value: open_cypher_parser::ast::PathPattern<'a>) -> Result<Self, Self::Error> {
        match value {
            open_cypher_parser::ast::PathPattern::Node(node) => {
                Ok(PathPattern::Node(NodePattern::try_from(node)?))
            }
            open_cypher_parser::ast::PathPattern::ConnectedPattern(vec_conn) => {
                let connected_patterns = vec_conn
                    .into_iter()
                    .map(ConnectedPattern::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(PathPattern::ConnectedPattern(connected_patterns))
            }
            open_cypher_parser::ast::PathPattern::ShortestPath(inner) => Ok(
                PathPattern::ShortestPath(Box::new(PathPattern::try_from(*inner)?)),
            ),
            open_cypher_parser::ast::PathPattern::AllShortestPaths(inner) => Ok(
                PathPattern::AllShortestPaths(Box::new(PathPattern::try_from(*inner)?)),
            ),
        }
    }
}

impl<'a> TryFrom<open_cypher_parser::ast::NodePattern<'a>> for NodePattern {
    type Error = errors::LogicalExprError;

    fn try_from(value: open_cypher_parser::ast::NodePattern<'a>) -> Result<Self, Self::Error> {
        let labels_vec = value.labels.map(|ls| {
            ls.into_iter()
                .map(|s| s.to_string())
                .collect::<Vec<String>>()
        });
        let first_label = labels_vec.as_ref().and_then(|ls| ls.first().cloned());

        let properties = value
            .properties
            .map(|props| {
                props
                    .into_iter()
                    .map(Property::try_from)
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?;

        Ok(NodePattern {
            name: value.name.map(|s| s.to_string()),
            label: first_label,
            labels: labels_vec,
            properties,
        })
    }
}

impl<'a> TryFrom<open_cypher_parser::ast::Property<'a>> for Property {
    type Error = errors::LogicalExprError;

    fn try_from(value: open_cypher_parser::ast::Property<'a>) -> Result<Self, Self::Error> {
        match value {
            open_cypher_parser::ast::Property::PropertyKV(kv) => {
                Ok(Property::PropertyKV(PropertyKVPair::try_from(kv)?))
            }
            open_cypher_parser::ast::Property::Param(s) => Ok(Property::Param(s.to_string())),
        }
    }
}

impl<'a> TryFrom<open_cypher_parser::ast::PropertyKVPair<'a>> for PropertyKVPair {
    type Error = errors::LogicalExprError;

    fn try_from(value: open_cypher_parser::ast::PropertyKVPair<'a>) -> Result<Self, Self::Error> {
        Ok(PropertyKVPair {
            key: value.key.to_string(),
            value: LogicalExpr::try_from(value.value)?,
        })
    }
}

impl<'a> TryFrom<open_cypher_parser::ast::ConnectedPattern<'a>> for ConnectedPattern {
    type Error = errors::LogicalExprError;

    fn try_from(value: open_cypher_parser::ast::ConnectedPattern<'a>) -> Result<Self, Self::Error> {
        Ok(ConnectedPattern {
            start_node: Arc::new(NodePattern::try_from(value.start_node.borrow().clone())?),
            relationship: RelationshipPattern::try_from(value.relationship)?,
            end_node: Arc::new(NodePattern::try_from(value.end_node.borrow().clone())?),
        })
    }
}

impl<'a> TryFrom<open_cypher_parser::ast::RelationshipPattern<'a>> for RelationshipPattern {
    type Error = errors::LogicalExprError;

    fn try_from(
        value: open_cypher_parser::ast::RelationshipPattern<'a>,
    ) -> Result<Self, Self::Error> {
        let properties = value
            .properties
            .map(|props| {
                props
                    .into_iter()
                    .map(Property::try_from)
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?;

        Ok(RelationshipPattern {
            name: value.name.map(|s| s.to_string()),
            direction: Direction::from(value.direction),
            labels: value
                .labels
                .map(|labels| labels.into_iter().map(|s| s.to_string()).collect()),
            properties,
        })
    }
}

// =============================================================================
// Case Expression Conversions
// =============================================================================

impl<'a> TryFrom<open_cypher_parser::ast::Case<'a>> for LogicalCase {
    type Error = errors::LogicalExprError;

    fn try_from(case: open_cypher_parser::ast::Case<'a>) -> Result<Self, Self::Error> {
        let expr = case
            .expr
            .map(|e| LogicalExpr::try_from(*e).map(Box::new))
            .transpose()?;
        let when_then = case
            .when_then
            .into_iter()
            .map(|(when, then)| {
                let w = LogicalExpr::try_from(when)?;
                let t = LogicalExpr::try_from(then)?;
                Ok((w, t))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let else_expr = case
            .else_expr
            .map(|e| LogicalExpr::try_from(*e).map(Box::new))
            .transpose()?;
        Ok(LogicalCase {
            expr,
            when_then,
            else_expr,
        })
    }
}

// =============================================================================
// EXISTS Subquery Conversions
// =============================================================================

impl<'a> TryFrom<open_cypher_parser::ast::ExistsSubquery<'a>> for ExistsSubquery {
    type Error = errors::LogicalExprError;

    fn try_from(exists: open_cypher_parser::ast::ExistsSubquery<'a>) -> Result<Self, Self::Error> {
        use open_cypher_parser::ast::PathPattern as AstPathPattern;

        let pattern = exists.pattern;

        let base_plan = match pattern {
            AstPathPattern::Node(node) => Arc::new(LogicalPlan::GraphNode(GraphNode {
                input: Arc::new(LogicalPlan::Empty),
                alias: node.name.unwrap_or("").to_string(),
                label: node.first_label().map(|s| s.to_string()),
                is_denormalized: false,
                projected_columns: None,
                node_types: None,
            })),
            AstPathPattern::ConnectedPattern(connected_patterns) => {
                if connected_patterns.is_empty() {
                    Arc::new(LogicalPlan::Empty)
                } else {
                    // GUARDRAIL (#574): the EXISTS-subquery conversion below only
                    // builds a single-hop, single-type `GraphRel`. Anything wider
                    // than that shape — a multi-hop chain, an OR'd relationship
                    // type list (`[:A|B]`), or a variable-length spec (`*1..2`) —
                    // would otherwise be SILENTLY narrowed to "first hop, first
                    // type, single edge" (ground rule 1 violation: dropped
                    // semantics without any signal). Until EXISTS { ... } routes
                    // through the same pattern-conversion pipeline as a top-level
                    // MATCH (which is what would be required to honor these
                    // shapes), reject loudly instead of guessing.
                    if connected_patterns.len() > 1 {
                        return Err(errors::LogicalExprError::UnsupportedExpression(format!(
                            "(#574) this EXISTS {{ ... }} pattern has a {}-hop \
                             relationship chain. ClickGraph does not yet convert \
                             multi-hop EXISTS patterns through the full MATCH \
                             pipeline, so only the first hop would be checked — \
                             silently dropping the remaining hops and returning \
                             wrong results. Workaround: express the extra hops as \
                             a separate correlated MATCH/WHERE, or restructure the \
                             query so the whole pattern is a top-level MATCH.",
                            connected_patterns.len()
                        )));
                    }
                    let cp = &connected_patterns[0];
                    let rel = &cp.relationship;
                    if let Some(labels) = rel.labels.as_ref() {
                        if labels.len() > 1 {
                            return Err(errors::LogicalExprError::UnsupportedExpression(format!(
                                "(#574) this EXISTS {{ ... }} pattern has multiple \
                                 OR'd relationship types ([:{}]). ClickGraph does \
                                 not yet convert multi-type EXISTS patterns through \
                                 the full MATCH pipeline, so only the first type \
                                 would be checked — silently narrowing the relationship \
                                 set and returning wrong results. Workaround: express \
                                 this as a top-level MATCH with a WHERE EXISTS-free \
                                 filter, or as multiple OR'd EXISTS {{ }} blocks, one \
                                 per relationship type.",
                                labels.join("|")
                            )));
                        }
                    }
                    if rel.variable_length.is_some() {
                        return Err(errors::LogicalExprError::UnsupportedExpression(
                            "(#574) this EXISTS { ... } pattern has a \
                             variable-length relationship (e.g. *1..2). \
                             ClickGraph does not yet convert variable-length \
                             EXISTS patterns through the full MATCH pipeline, so \
                             the hop bound would be silently dropped and only a \
                             single fixed hop checked — returning wrong results. \
                             Workaround: restructure the query so the \
                             variable-length pattern is a top-level MATCH \
                             (e.g. `MATCH (a)-[*1..2]->(b) WHERE ...`) instead of \
                             an EXISTS { } subquery."
                                .to_string(),
                        ));
                    }
                    let start = cp.start_node.borrow();
                    let end = cp.end_node.borrow();

                    let start_node = LogicalPlan::GraphNode(GraphNode {
                        input: Arc::new(LogicalPlan::Empty),
                        alias: start.name.unwrap_or("").to_string(),
                        label: start.first_label().map(|s| s.to_string()),
                        is_denormalized: false,
                        projected_columns: None,
                        node_types: None,
                    });

                    let rel_scan = LogicalPlan::Empty;

                    let end_node = LogicalPlan::GraphNode(GraphNode {
                        input: Arc::new(LogicalPlan::Empty),
                        alias: end.name.unwrap_or("").to_string(),
                        label: end.first_label().map(|s| s.to_string()),
                        is_denormalized: false,
                        projected_columns: None,
                        node_types: None,
                    });

                    let direction = match rel.direction {
                        open_cypher_parser::ast::Direction::Outgoing => Direction::Outgoing,
                        open_cypher_parser::ast::Direction::Incoming => Direction::Incoming,
                        open_cypher_parser::ast::Direction::Either => Direction::Either,
                    };

                    Arc::new(LogicalPlan::GraphRel(GraphRel {
                        left: Arc::new(start_node),
                        center: Arc::new(rel_scan),
                        right: Arc::new(end_node),
                        alias: rel.name.unwrap_or("").to_string(),
                        direction,
                        left_connection: start.name.unwrap_or("").to_string(),
                        right_connection: end.name.unwrap_or("").to_string(),
                        is_rel_anchor: false,
                        variable_length: None,
                        shortest_path_mode: None,
                        path_variable: None,
                        where_predicate: None,
                        labels: rel
                            .labels
                            .as_ref()
                            .map(|l| l.iter().map(|s| s.to_string()).collect()),
                        is_optional: None,
                        anchor_connection: None,
                        cte_references: std::collections::HashMap::new(),
                        pattern_combinations: None,
                        was_undirected: None,
                        // #586: an EXISTS-subquery pattern renders as a scope-isolated
                        // subquery and never shares PatternGraphMetadata with the outer
                        // pattern's edges, so a fixed clause index 0 cannot collide with
                        // the outer first clause. If EXISTS patterns are ever inlined into
                        // the outer metadata, this must instead inherit the outer index.
                        match_clause_index: 0,
                        optional_anchor_where: None, // #597: set by evaluate_optional_match_clause
                    }))
                }
            }
            AstPathPattern::ShortestPath(inner) | AstPathPattern::AllShortestPaths(inner) => {
                // #579: previously hardcoded `where_clause: None` here, silently
                // dropping the outer EXISTS's WHERE clause whenever its pattern
                // was wrapped in shortestPath()/allShortestPaths() — the shared
                // Filter-wrapping logic below never ran because this arm
                // returns early via recursion. Thread the outer where_clause
                // through so the recursive call applies it exactly as the
                // non-shortestPath arms do.
                let inner_exists = open_cypher_parser::ast::ExistsSubquery {
                    pattern: *inner,
                    where_clause: exists.where_clause,
                };
                return ExistsSubquery::try_from(inner_exists);
            }
        };

        let plan = if let Some(where_clause) = exists.where_clause {
            Arc::new(LogicalPlan::Filter(Filter {
                input: base_plan,
                predicate: LogicalExpr::try_from(where_clause.conditions)?,
            }))
        } else {
            base_plan
        };

        Ok(ExistsSubquery { subplan: plan })
    }
}

// =============================================================================
// Main Expression Conversion
// =============================================================================

impl<'a> std::convert::TryFrom<open_cypher_parser::ast::Expression<'a>> for LogicalExpr {
    type Error = errors::LogicalExprError;

    fn try_from(expr: open_cypher_parser::ast::Expression<'a>) -> Result<Self, Self::Error> {
        use open_cypher_parser::ast::Expression;
        match expr {
            Expression::Literal(lit) => Ok(LogicalExpr::Literal(Literal::from(lit))),
            Expression::Variable(s) => {
                if s == "*" {
                    Ok(LogicalExpr::Star)
                } else {
                    Ok(LogicalExpr::TableAlias(TableAlias(s.to_string())))
                }
            }
            Expression::Parameter(s) => Ok(LogicalExpr::Parameter(s.to_string())),
            Expression::List(exprs) => {
                let logical_exprs = exprs
                    .into_iter()
                    .map(Self::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(LogicalExpr::List(logical_exprs))
            }
            Expression::FunctionCallExp(fc) => LogicalExpr::try_from(fc),
            Expression::PropertyAccessExp(pa) => {
                Ok(LogicalExpr::PropertyAccessExp(PropertyAccess::from(pa)))
            }
            Expression::OperatorApplicationExp(oa) => Ok(LogicalExpr::OperatorApplicationExp(
                OperatorApplication::try_from(oa)?,
            )),
            Expression::PathPattern(pp) => {
                // GUARDRAIL (#588, mirrors #578): a bare pattern used as a
                // boolean — `WHERE NOT (a)-[:A|B]->(b)` / `WHERE NOT
                // (a)-[:R*1..3]->(b)` — bottoms out at `logical_expr::PathPattern`
                // / `RelationshipPattern`, which has NO `variable_length` field,
                // so a `*1..N` hop bound is silently dropped here, and multi-type
                // OR'd labels are then silently narrowed to the FIRST type by the
                // render-layer `generate_not_exists_from_path_pattern` (which only
                // reads `labels.first()` and emits a single-hop NOT EXISTS). Both
                // silently evaluate a NARROWER predicate than written — e.g. `NOT
                // (a)-[:FOLLOWS|FRIENDS_WITH]->(b)` checks only FOLLOWS, and `NOT
                // (a)-[:FOLLOWS*1..3]->(a)` checks only a single hop — returning
                // wrong rows (ground rule 1 violation). Until this shape routes
                // through a pipeline that carries a hop bound and enumerates every
                // OR'd type, reject both loudly instead of silently narrowing.
                // Single-type non-VLP patterns return None here and are
                // unaffected (the working `NOT (a)-[:FOLLOWS]->(b)` form).
                if let Some(reason) = path_pattern_has_vlp_or_multi_type(&pp) {
                    return Err(errors::LogicalExprError::UnsupportedExpression(format!(
                        "(#588) this boolean pattern predicate {reason}. ClickGraph \
                         does not yet convert a bare/NOT pattern-existence check \
                         through a pipeline that can carry a hop bound or enumerate \
                         every OR'd relationship type, so the predicate would \
                         silently narrow to a smaller pattern than the one written \
                         — returning wrong results. Workaround: express the check \
                         via a top-level (OPTIONAL) MATCH, or an EXISTS {{ ... }} \
                         subquery, instead of a bare pattern in WHERE."
                    )));
                }
                Ok(LogicalExpr::PathPattern(PathPattern::try_from(pp)?))
            }
            Expression::Case(case) => Ok(LogicalExpr::Case(LogicalCase::try_from(case)?)),
            Expression::ExistsExpression(exists) => Ok(LogicalExpr::ExistsSubquery(
                ExistsSubquery::try_from(*exists)?,
            )),
            Expression::ReduceExp(reduce) => Ok(LogicalExpr::ReduceExpr(ReduceExpr {
                accumulator: reduce.accumulator.to_string(),
                initial_value: Box::new(Self::try_from(*reduce.initial_value)?),
                variable: reduce.variable.to_string(),
                list: Box::new(Self::try_from(*reduce.list)?),
                expression: Box::new(Self::try_from(*reduce.expression)?),
            })),
            Expression::MapLiteral(entries) => {
                let logical_entries = entries
                    .into_iter()
                    .map(|(k, v)| Self::try_from(v).map(|lv| (k.to_string(), lv)))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(LogicalExpr::MapLiteral(logical_entries))
            }
            Expression::LabelExpression { variable, label } => Ok(LogicalExpr::LabelExpression {
                variable: variable.to_string(),
                label: label.to_string(),
            }),
            Expression::Lambda(lambda) => Ok(LogicalExpr::Lambda(LambdaExpr {
                params: lambda.params.iter().map(|s| s.to_string()).collect(),
                body: Box::new(Self::try_from(*lambda.body)?),
            })),
            Expression::PatternComprehension(_) => {
                Err(errors::LogicalExprError::PatternComprehensionNotRewritten)
            }
            Expression::ListComprehension(lc) => {
                // #866: lower a plain (scalar) list comprehension in projection
                // position to nested ClickHouse `arrayMap`/`arrayFilter` calls,
                // mirroring how `reduce(...)` lowers to `arrayFold`. The four
                // shapes:
                //   [x IN l WHERE p | e] -> arrayMap(x->e, arrayFilter(x->p, l))
                //   [x IN l WHERE p]     -> arrayFilter(x->p, l)
                //   [x IN l | e]         -> arrayMap(x->e, l)
                //   [x IN l]             -> l              (identity)
                // CONSERVATIVE GUARD: a graph pattern anywhere in the WHERE or
                // projection (`[p IN posts WHERE (p)-[:R]->()]`, or hidden inside
                // a CASE / exists() / function arg) has NO scalar per-element
                // lowering — the pattern is a graph traversal, not an array
                // predicate. Such comprehensions keep failing loud here
                // (`ListComprehensionNotRewritten`) rather than lowering into a
                // lambda body that renders a pattern in expression context (a
                // pre-existing `unimplemented!` panic at
                // render_expr.rs). `size([...])`-wrapped pattern comprehensions
                // are already intercepted upstream (with_clause.rs, #612/#629),
                // so this is the guard for the BARE projection-position form.
                if lc
                    .where_clause
                    .as_deref()
                    .is_some_and(expr_contains_path_pattern)
                    || lc
                        .projection
                        .as_deref()
                        .is_some_and(expr_contains_path_pattern)
                {
                    return Err(errors::LogicalExprError::ListComprehensionNotRewritten);
                }

                let var = lc.variable.to_string();
                let list = Self::try_from(*lc.list_expr)?;

                // Inner: filter, only when a (scalar) WHERE is present.
                let filtered = match lc.where_clause {
                    Some(where_expr) => LogicalExpr::ScalarFnCall(ScalarFnCall {
                        name: "arrayFilter".to_string(),
                        args: vec![
                            LogicalExpr::Lambda(LambdaExpr {
                                params: vec![var.clone()],
                                body: Box::new(Self::try_from(*where_expr)?),
                            }),
                            list,
                        ],
                    }),
                    None => list,
                };

                // Outer: map, only when a projection (`| expr`) is present.
                // `None` = identity projection, so the filtered list is the result.
                match lc.projection {
                    Some(proj) => Ok(LogicalExpr::ScalarFnCall(ScalarFnCall {
                        name: "arrayMap".to_string(),
                        args: vec![
                            LogicalExpr::Lambda(LambdaExpr {
                                params: vec![var],
                                body: Box::new(Self::try_from(*proj)?),
                            }),
                            filtered,
                        ],
                    })),
                    None => Ok(filtered),
                }
            }
            Expression::ArraySubscript { array, index } => Ok(LogicalExpr::ArraySubscript {
                array: Box::new(LogicalExpr::try_from(*array)?),
                index: Box::new(LogicalExpr::try_from(*index)?),
            }),
            Expression::ArraySlicing { array, from, to } => Ok(LogicalExpr::ArraySlicing {
                array: Box::new(LogicalExpr::try_from(*array)?),
                from: from
                    .map(|f| LogicalExpr::try_from(*f).map(Box::new))
                    .transpose()?,
                to: to
                    .map(|t| LogicalExpr::try_from(*t).map(Box::new))
                    .transpose()?,
            }),
        }
    }
}

/// #866: returns true if a list-comprehension WHERE or projection expression
/// contains a graph pattern anywhere — a bare `PathPattern`, or one nested
/// inside an operator / CASE / function arg / EXISTS / pattern-comprehension /
/// another comprehension. Such an expression has no scalar per-element
/// `arrayFilter`/`arrayMap` lowering (the pattern is a graph traversal, not an
/// array predicate), so the comprehension keeps failing loud
/// (`ListComprehensionNotRewritten`) instead of lowering into a lambda body
/// that renders a pattern in expression context — a pre-existing
/// `unimplemented!` panic at `render_expr.rs`.
///
/// The match is exhaustive (no catch-all) so a new `Expression` variant that
/// could carry a pattern is a compile error here until it is classified,
/// rather than silently slipping a pattern through to the panic path.
fn expr_contains_path_pattern(expr: &open_cypher_parser::ast::Expression<'_>) -> bool {
    use open_cypher_parser::ast::Expression;
    match expr {
        // Directly a pattern, or a node whose very presence implies a pattern.
        Expression::PathPattern(_)
        | Expression::ExistsExpression(_)
        | Expression::PatternComprehension(_) => true,

        // Recurse through every expression-bearing child.
        Expression::List(items) => items.iter().any(expr_contains_path_pattern),
        Expression::FunctionCallExp(fc) => fc.args.iter().any(expr_contains_path_pattern),
        Expression::OperatorApplicationExp(op) => {
            op.operands.iter().any(expr_contains_path_pattern)
        }
        Expression::Case(case) => {
            case.expr.as_deref().is_some_and(expr_contains_path_pattern)
                || case
                    .when_then
                    .iter()
                    .any(|(w, t)| expr_contains_path_pattern(w) || expr_contains_path_pattern(t))
                || case
                    .else_expr
                    .as_deref()
                    .is_some_and(expr_contains_path_pattern)
        }
        Expression::ReduceExp(r) => {
            expr_contains_path_pattern(&r.initial_value)
                || expr_contains_path_pattern(&r.list)
                || expr_contains_path_pattern(&r.expression)
        }
        Expression::MapLiteral(entries) => {
            entries.iter().any(|(_, v)| expr_contains_path_pattern(v))
        }
        Expression::Lambda(l) => expr_contains_path_pattern(&l.body),
        Expression::ListComprehension(lc) => {
            expr_contains_path_pattern(&lc.list_expr)
                || lc
                    .where_clause
                    .as_deref()
                    .is_some_and(expr_contains_path_pattern)
                || lc
                    .projection
                    .as_deref()
                    .is_some_and(expr_contains_path_pattern)
        }
        Expression::ArraySubscript { array, index } => {
            expr_contains_path_pattern(array) || expr_contains_path_pattern(index)
        }
        Expression::ArraySlicing { array, from, to } => {
            expr_contains_path_pattern(array)
                || from.as_deref().is_some_and(expr_contains_path_pattern)
                || to.as_deref().is_some_and(expr_contains_path_pattern)
        }

        // Leaves that cannot contain a pattern.
        Expression::Literal(_)
        | Expression::Variable(_)
        | Expression::Parameter(_)
        | Expression::PropertyAccessExp(_)
        | Expression::LabelExpression { .. } => false,
    }
}

#[cfg(test)]
mod listcomp_lowering_tests {
    //! #866: a plain (scalar) list comprehension in projection position lowers
    //! to nested `arrayMap`/`arrayFilter` `ScalarFnCall`s carrying `Lambda`
    //! args. A pattern-predicate WHERE has no scalar lowering and keeps failing
    //! loud (routed elsewhere for `size([...])`).
    use super::*;
    use crate::open_cypher_parser::ast::{
        Expression, ListComprehension, Literal as AstLiteral, NodePattern, Operator,
        OperatorApplication, PathPattern as AstPathPattern,
    };

    /// `[1, 2, 3]` as an AST list expression.
    fn list_123<'a>() -> Expression<'a> {
        Expression::List(vec![
            Expression::Literal(AstLiteral::Integer(1)),
            Expression::Literal(AstLiteral::Integer(2)),
            Expression::Literal(AstLiteral::Integer(3)),
        ])
    }

    /// `x <op> <int>` scalar predicate/projection over the iteration var.
    fn bin_x<'a>(op: Operator, rhs: i64) -> Expression<'a> {
        Expression::OperatorApplicationExp(OperatorApplication {
            operator: op,
            operands: vec![
                Expression::Variable("x"),
                Expression::Literal(AstLiteral::Integer(rhs)),
            ],
        })
    }

    fn lower(lc: ListComprehension<'_>) -> Result<LogicalExpr, errors::LogicalExprError> {
        LogicalExpr::try_from(Expression::ListComprehension(lc))
    }

    fn scalar_fn(expr: &LogicalExpr) -> &ScalarFnCall {
        match expr {
            LogicalExpr::ScalarFnCall(f) => f,
            other => panic!("expected ScalarFnCall, got {other:?}"),
        }
    }

    #[test]
    fn where_only_lowers_to_array_filter() {
        // [x IN [1,2,3] WHERE x > 1] -> arrayFilter(x -> x > 1, [1,2,3])
        let lowered = lower(ListComprehension {
            variable: "x",
            list_expr: Box::new(list_123()),
            where_clause: Some(Box::new(bin_x(Operator::GreaterThan, 1))),
            projection: None,
        })
        .unwrap();
        let f = scalar_fn(&lowered);
        assert_eq!(f.name, "arrayFilter");
        assert_eq!(f.args.len(), 2);
        match &f.args[0] {
            LogicalExpr::Lambda(l) => assert_eq!(l.params, vec!["x".to_string()]),
            other => panic!("expected Lambda, got {other:?}"),
        }
        assert!(matches!(&f.args[1], LogicalExpr::List(_)));
    }

    #[test]
    fn map_only_lowers_to_array_map() {
        // [x IN [1,2,3] | x * 2] -> arrayMap(x -> x * 2, [1,2,3])
        let lowered = lower(ListComprehension {
            variable: "x",
            list_expr: Box::new(list_123()),
            where_clause: None,
            projection: Some(Box::new(bin_x(Operator::Multiplication, 2))),
        })
        .unwrap();
        let f = scalar_fn(&lowered);
        assert_eq!(f.name, "arrayMap");
        assert_eq!(f.args.len(), 2);
        assert!(matches!(&f.args[0], LogicalExpr::Lambda(_)));
        assert!(matches!(&f.args[1], LogicalExpr::List(_)));
    }

    #[test]
    fn where_and_map_nests_map_over_filter() {
        // [x IN l WHERE p | e] -> arrayMap(x -> e, arrayFilter(x -> p, l))
        let lowered = lower(ListComprehension {
            variable: "x",
            list_expr: Box::new(list_123()),
            where_clause: Some(Box::new(bin_x(Operator::GreaterThan, 1))),
            projection: Some(Box::new(bin_x(Operator::Multiplication, 10))),
        })
        .unwrap();
        let outer = scalar_fn(&lowered);
        assert_eq!(outer.name, "arrayMap");
        // The map's collection arg is the inner arrayFilter.
        let inner = scalar_fn(&outer.args[1]);
        assert_eq!(inner.name, "arrayFilter");
        assert!(matches!(&inner.args[1], LogicalExpr::List(_)));
    }

    #[test]
    fn identity_lowers_to_bare_list() {
        // [x IN [1,2,3]] -> [1,2,3] (no filter, no map)
        let lowered = lower(ListComprehension {
            variable: "x",
            list_expr: Box::new(list_123()),
            where_clause: None,
            projection: None,
        })
        .unwrap();
        assert!(
            matches!(&lowered, LogicalExpr::List(items) if items.len() == 3),
            "expected bare List, got {lowered:?}"
        );
    }

    #[test]
    fn pattern_predicate_where_keeps_failing_loud() {
        // A graph-pattern WHERE has no scalar lowering — it must return the
        // ListComprehensionNotRewritten error unchanged (routed elsewhere for
        // `size([...])`, #612/#629), never silently emit wrong SQL. The guard
        // fires on any `Expression::PathPattern` in the WHERE; the inner pattern
        // shape is irrelevant, so a bare `PathPattern::Node` suffices here.
        let pattern = Expression::PathPattern(AstPathPattern::Node(NodePattern {
            name: Some("p"),
            labels: None,
            properties: None,
        }));
        let err = lower(ListComprehension {
            variable: "p",
            list_expr: Box::new(Expression::Variable("posts")),
            where_clause: Some(Box::new(pattern)),
            projection: None,
        })
        .unwrap_err();
        assert!(
            matches!(err, errors::LogicalExprError::ListComprehensionNotRewritten),
            "expected ListComprehensionNotRewritten, got {err:?}"
        );
    }

    #[test]
    fn pattern_predicate_nested_in_operator_still_fails_loud() {
        // The guard recurses through operator operands, so a pattern buried in
        // `AND`/`NOT`/`OR` (`WHERE x > 0 AND (p)-[...]->()`) is still caught.
        let pattern = Expression::PathPattern(AstPathPattern::Node(NodePattern {
            name: Some("p"),
            labels: None,
            properties: None,
        }));
        let where_clause = Expression::OperatorApplicationExp(OperatorApplication {
            operator: Operator::And,
            operands: vec![bin_x(Operator::GreaterThan, 0), pattern],
        });
        let err = lower(ListComprehension {
            variable: "p",
            list_expr: Box::new(Expression::Variable("posts")),
            where_clause: Some(Box::new(where_clause)),
            projection: None,
        })
        .unwrap_err();
        assert!(matches!(
            err,
            errors::LogicalExprError::ListComprehensionNotRewritten
        ));
    }

    #[test]
    fn pattern_hidden_in_case_fails_loud_not_panics() {
        // A pattern buried in a CASE inside the WHERE
        // (`[p IN l WHERE CASE WHEN (p)-[:R]->() THEN … END]`) has no scalar
        // lowering. Before the guard recursed into CASE, this lowered and then
        // PANICKED at render_expr.rs (pattern in expression context is
        // `unimplemented!`). It must fail loud instead.
        let pattern = Expression::PathPattern(AstPathPattern::Node(NodePattern {
            name: Some("p"),
            labels: None,
            properties: None,
        }));
        let case = Expression::Case(crate::open_cypher_parser::ast::Case {
            expr: None,
            when_then: vec![(pattern, Expression::Literal(AstLiteral::Boolean(true)))],
            else_expr: Some(Box::new(Expression::Literal(AstLiteral::Boolean(false)))),
        });
        let err = lower(ListComprehension {
            variable: "p",
            list_expr: Box::new(Expression::Variable("posts")),
            where_clause: Some(Box::new(case)),
            projection: None,
        })
        .unwrap_err();
        assert!(matches!(
            err,
            errors::LogicalExprError::ListComprehensionNotRewritten
        ));
    }

    #[test]
    fn pattern_in_projection_fails_loud() {
        // The guard also covers the projection (`| …`), not just the WHERE:
        // `[p IN l | CASE WHEN (p)-[:R]->() THEN 1 ELSE 0 END]` must fail loud.
        let pattern = Expression::PathPattern(AstPathPattern::Node(NodePattern {
            name: Some("p"),
            labels: None,
            properties: None,
        }));
        let projection = Expression::Case(crate::open_cypher_parser::ast::Case {
            expr: None,
            when_then: vec![(pattern, Expression::Literal(AstLiteral::Integer(1)))],
            else_expr: Some(Box::new(Expression::Literal(AstLiteral::Integer(0)))),
        });
        let err = lower(ListComprehension {
            variable: "p",
            list_expr: Box::new(list_123()),
            where_clause: None,
            projection: Some(Box::new(projection)),
        })
        .unwrap_err();
        assert!(matches!(
            err,
            errors::LogicalExprError::ListComprehensionNotRewritten
        ));
    }
}
