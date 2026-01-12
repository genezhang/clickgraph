use nom::{branch::alt, combinator::opt, IResult, Parser};

use super::{
    ast::{LimitClause, OrderByClause, SkipClause},
    errors::OpenCypherParsingError,
    limit_clause::parse_limit_clause,
    order_by_clause::parse_order_by_clause,
    skip_clause::parse_skip_clause,
};

/// Represents the ORDER BY and page clause components
#[derive(Debug, Clone)]
pub struct OrderByAndPageClause<'a> {
    pub order_by: Option<OrderByClause<'a>>,
    pub skip: Option<SkipClause>,
    pub limit: Option<LimitClause>,
}

/// Parse ORDER BY and pagination clause per OpenCypher spec:
/// <order by and page clause> ::= 
///     <order by clause> [ <offset clause> ] [ <limit clause> ]
///   | <offset clause> [ <limit clause> ]
///   | <limit clause>
///
/// OpenCypher spec requires SKIP before LIMIT (strict order).
/// Neo4j follows this spec strictly.
pub fn parse_order_by_and_page_clause<'a>(
    input: &'a str,
) -> IResult<&'a str, OrderByAndPageClause<'a>, OpenCypherParsingError<'a>> {
    // Try: ORDER BY [ SKIP ] [ LIMIT ] (spec-compliant order)
    let order_skip_limit = |input: &'a str| {
        let (input, order_by) = parse_order_by_clause.parse(input)?;
        let (input, skip) = opt(parse_skip_clause).parse(input)?;
        let (input, limit) = opt(parse_limit_clause).parse(input)?;
        Ok((
            input,
            OrderByAndPageClause {
                order_by: Some(order_by),
                skip,
                limit,
            },
        ))
    };

    // Try: SKIP [ LIMIT ] (spec-compliant)
    let skip_limit = |input: &'a str| {
        let (input, skip) = parse_skip_clause.parse(input)?;
        let (input, limit) = opt(parse_limit_clause).parse(input)?;
        Ok((
            input,
            OrderByAndPageClause {
                order_by: None,
                skip: Some(skip),
                limit,
            },
        ))
    };

    // Try: LIMIT only (no SKIP)
    let limit_only = |input: &'a str| {
        let (input, limit) = parse_limit_clause.parse(input)?;
        Ok((
            input,
            OrderByAndPageClause {
                order_by: None,
                skip: None,
                limit: Some(limit),
            },
        ))
    };

    // Try patterns in OpenCypher spec order
    alt((order_skip_limit, skip_limit, limit_only)).parse(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order_skip_limit() {
        let input = "ORDER BY n.name SKIP 5 LIMIT 10";
        let result = parse_order_by_and_page_clause(input);
        assert!(result.is_ok());
        let (remaining, clause) = result.unwrap();
        assert_eq!(remaining, "");
        assert!(clause.order_by.is_some());
        assert!(clause.skip.is_some());
        assert!(clause.limit.is_some());
    }

    #[test]
    fn test_order_limit_skip_rejected() {
        // Non-compliant order should be rejected (LIMIT before SKIP violates OpenCypher spec)
        let input = "ORDER BY n.name LIMIT 10 SKIP 5";
        let result = parse_order_by_and_page_clause(input);
        // Parser should stop after LIMIT, leaving "SKIP 5" unparsed
        assert!(result.is_ok());
        let (remaining, clause) = result.unwrap();
        assert_eq!(remaining, "SKIP 5"); // This should be left unparsed
        assert!(clause.order_by.is_some());
        assert!(clause.skip.is_none()); // No SKIP parsed
        assert!(clause.limit.is_some());
    }

    #[test]
    fn test_skip_limit() {
        let input = "SKIP 5 LIMIT 10";
        let result = parse_order_by_and_page_clause(input);
        assert!(result.is_ok());
        let (remaining, clause) = result.unwrap();
        assert_eq!(remaining, "");
        assert!(clause.order_by.is_none());
        assert!(clause.skip.is_some());
        assert!(clause.limit.is_some());
    }

    #[test]
    fn test_limit_skip_rejected() {
        // LIMIT before SKIP is NOT OpenCypher compliant and rejected by Neo4j
        // Parser should consume only LIMIT, leaving "SKIP 5" unparsed
        let input = "LIMIT 10 SKIP 5";
        let result = parse_order_by_and_page_clause(input);
        assert!(result.is_ok());
        let (remaining, clause) = result.unwrap();
        // Should leave "SKIP 5" unparsed because LIMIT comes before SKIP
        assert_eq!(remaining, "SKIP 5");
        assert!(clause.order_by.is_none());
        assert!(clause.skip.is_none());
        assert!(clause.limit.is_some());
    }

    #[test]
    fn test_only_limit() {
        let input = "LIMIT 10";
        let result = parse_order_by_and_page_clause(input);
        assert!(result.is_ok());
        let (remaining, clause) = result.unwrap();
        assert_eq!(remaining, "");
        assert!(clause.order_by.is_none());
        assert!(clause.skip.is_none());
        assert!(clause.limit.is_some());
    }

    #[test]
    fn test_only_order_by() {
        let input = "ORDER BY n.name";
        let result = parse_order_by_and_page_clause(input);
        assert!(result.is_ok());
        let (remaining, clause) = result.unwrap();
        assert_eq!(remaining, "");
        assert!(clause.order_by.is_some());
        assert!(clause.skip.is_none());
        assert!(clause.limit.is_none());
    }
}
