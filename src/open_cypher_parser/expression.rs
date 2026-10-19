use nom::{
    branch::alt,
    bytes::complete::{tag, take_while1},
    character::complete::multispace0,
    combinator::{cut, map, not, opt, peek},
    error::{Error, ErrorKind},
    multi::separated_list0,
    sequence::{delimited, preceded, separated_pair, terminated},
    IResult, Parser,
};

use nom::character::complete::char;

use crate::open_cypher_parser::common::{self, ws};

use super::{
    ast::{
        ExistsSubquery, Expression, FunctionCall, LambdaExpression, ListComprehension, Literal,
        Name, Operator, OperatorApplication, PathPattern, PatternComprehension, PropertyAccess,
        ReduceExpression,
    },
    path_pattern, where_clause,
};

pub fn parse_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    let (input, expression) = parse_logical_or.parse(input)?;
    Ok((input, expression))
}

/// A pattern used as an expression: a relationship pattern (a pattern
/// predicate, `(a)-[:R]->()`), `shortestPath(…)` / `allShortestPaths(…)`.
/// A lone node `(x)` is not one: in an expression it is a parenthesized
/// expression, `(a)` the variable, `(a:Person)` a label test, `(NOT false)` a
/// negation (#1398), as in Neo4j, where a pattern expression has a
/// relationship.
pub fn parse_path_pattern_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    let (rest, path_pattern) = path_pattern::parse_path_pattern(input)?;
    if matches!(path_pattern, PathPattern::Node(_)) {
        return Err(nom::Err::Error(Error::new(input, ErrorKind::Verify)));
    }
    Ok((rest, Expression::PathPattern(path_pattern)))
}

/// An atom and its postfix operators: `[i]`, `[a..b]`, `.key`, any number,
/// left to right (`nss[0][1].name`).
fn parse_postfix_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    // First, parse any primary expression (function call, literal, variable, etc.)
    let (mut input, mut expr) = alt((
        parse_exists_expression, // Must be before parse_function_call to catch EXISTS { }
        parse_case_expression,
        parse_reduce_expression, // Must be before parse_function_call to catch reduce(...)
        parse_pattern_comprehension, // Must be before parse_list_literal to catch [(pattern) | ...]
        parse_list_comprehension, // Must be before parse_list_literal to catch [x IN list WHERE ...]
        parse_path_pattern_expression,
        parse_list_predicate, // Must be before parse_function_call: any(x IN l WHERE p)
        parse_function_call,
        parse_map_literal, // Must be before list_literal (different brackets anyway)
        parse_list_literal,
        parse_parameter,
        parse_literal_or_variable_expression,
        delimited(ws(char('(')), parse_expression, ws(char(')'))),
    ))
    .parse(input)?;

    // Then, parse any postfix operations in a loop
    loop {
        // Try to parse slicing first: [from..to], [..to], [from..], or [..]
        let slicing_attempt = (
            multispace0,
            char('['),
            multispace0,
            // Optional from expression
            nom::combinator::opt(parse_expression),
            multispace0,
            tag(".."),
            multispace0,
            // Optional to expression
            nom::combinator::opt(parse_expression),
            multispace0,
            char(']'),
        )
            .parse(input);

        if let Ok((new_input, (_, _, _, from_expr, _, _, _, to_expr, _, _))) = slicing_attempt {
            // Successfully parsed slicing [from..to]
            expr = Expression::ArraySlicing {
                array: Box::new(expr),
                from: from_expr.map(Box::new),
                to: to_expr.map(Box::new),
            };
            input = new_input;
            continue; // Check for more postfix operations
        }

        // Try to parse array subscript: expr[index]
        let subscript_attempt = (
            multispace0,
            char('['),
            multispace0,
            parse_expression,
            multispace0,
            char(']'),
        )
            .parse(input);

        if let Ok((new_input, (_, _, _, index_expr, _, _))) = subscript_attempt {
            // Successfully parsed [index]
            expr = Expression::ArraySubscript {
                array: Box::new(expr),
                index: Box::new(index_expr),
            };
            input = new_input;
            continue; // Check for more postfix operations
        }

        // A property: `expr.key` (a variable's, `n.name`; a chain's, `m.a.b`;
        // any value's, `xs[1].name`, `head(l).name`, `(a).name`).
        if let Some((new_input, e)) = parse_property_postfix(input, &expr) {
            expr = e;
            input = new_input;
            continue;
        }

        // No more postfix operations found
        break;
    }

    Ok((input, expr))
}

/// The temporal accessors `.year` … `.nanosecond`, read at parse time as the
/// function of the same name (`d.year` is `year(d)`): a property named so
/// would be read as the accessor (escaped too: quoting never changes what a
/// name means, `` d.`year` `` is Neo4j's `d.year`), unless written
/// `n['year']`.
const TEMPORAL_ACCESSORS: [&str; 9] = [
    "year",
    "month",
    "day",
    "hour",
    "minute",
    "second",
    "millisecond",
    "microsecond",
    "nanosecond",
];

/// `.key` after `base` (the postfix loop of [`parse_postfix_expression`]).
///
/// * `v.key`, a variable's property: [`Expression::PropertyAccessExp`];
///   `v.*`: all of them.
/// * `e.key` of anything else, including a further key of a chain
///   (`m.a.b` is `(m.a).b`): [`Expression::PropertyOf`].
/// * A temporal accessor ([`TEMPORAL_ACCESSORS`]) is its function, as the parser has always read it: after a variable when it
///   ends the chain (`d.month`, but `x.month.day` is the property `month`'s
///   `day`) and after a chain's key, spelled in lower case; after any other
///   value (`datetime(s).Year`) in any case.
fn parse_property_postfix<'a>(
    input: &'a str,
    base: &Expression<'a>,
) -> Option<(&'a str, Expression<'a>)> {
    let after_dot = input.strip_prefix('.')?;
    if let (Expression::Variable(v), Some(rest)) = (base, after_dot.strip_prefix('*')) {
        if v != "*" {
            return Some((
                rest,
                Expression::PropertyAccessExp(PropertyAccess {
                    base: v.clone(),
                    key: "*".into(),
                }),
            ));
        }
    }
    let (rest, key) = common::symbolic_name::<Error<&str>>(after_dot).ok()?;
    if key == "*" {
        // The key `` `*` ``: carried as `v.*` (all properties), it would
        // read alike, so it does not parse.
        return None;
    }
    let exact = || TEMPORAL_ACCESSORS.iter().find(|a| **a == key);
    let accessor = match base {
        Expression::Variable(_) => {
            let continues = rest
                .strip_prefix('.')
                .is_some_and(|r| common::symbolic_name::<Error<&str>>(r).is_ok());
            exact().filter(|_| !continues)
        }
        Expression::PropertyAccessExp(_) | Expression::PropertyOf { .. } => exact(),
        _ => TEMPORAL_ACCESSORS
            .iter()
            .find(|a| a.eq_ignore_ascii_case(&key)),
    };
    let expr = match (accessor, base) {
        (Some(name), _) => Expression::FunctionCallExp(FunctionCall {
            name: name.to_string(),
            args: vec![base.clone()],
        }),
        (None, Expression::Variable(v)) if v != "*" => {
            Expression::PropertyAccessExp(PropertyAccess {
                base: v.clone(),
                key,
            })
        }
        (None, _) => Expression::PropertyOf {
            base: Box::new(base.clone()),
            key,
        },
    };
    Some((rest, expr))
}

/// Parse EXISTS subquery expression
/// Syntax: EXISTS { pattern } or EXISTS { MATCH pattern WHERE condition }
/// Examples:
///   EXISTS { (u)-[:FOLLOWS]->(:User) }
///   EXISTS { MATCH (u)-[:FOLLOWS]->(f) WHERE f.active = true }
fn parse_exists_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    // Parse EXISTS keyword
    let (input, _) = keyword("EXISTS").parse(input)?;

    // Parse opening brace
    let (input, _) = ws(char('{')).parse(input)?;

    // Optionally skip MATCH keyword if present
    let (input, _) = opt(keyword("MATCH")).parse(input)?;

    // Parse the pattern
    let (input, pattern) = ws(path_pattern::parse_path_pattern).parse(input)?;

    // Parse optional WHERE clause - convert the error type
    let (input, where_clause) = match opt(where_clause::parse_where_clause).parse(input) {
        Ok((rest, wc)) => (rest, wc),
        Err(nom::Err::Error(_)) | Err(nom::Err::Failure(_)) => (input, None),
        Err(nom::Err::Incomplete(n)) => return Err(nom::Err::Incomplete(n)),
    };

    // Parse closing brace
    let (input, _) = ws(char('}')).parse(input)?;

    Ok((
        input,
        Expression::ExistsExpression(Box::new(ExistsSubquery {
            pattern,
            where_clause: where_clause.map(Box::new),
        })),
    ))
}

fn parse_case_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    let (input, _) = keyword("CASE").parse(input)?;

    // A searched CASE starts with WHEN (in any case); a simple CASE has an
    // expression first.
    let is_searched = peek(keyword("WHEN")).parse(input).is_ok();

    let (input, case_expr) = if is_searched {
        // Searched CASE - no case_expr
        (input, None)
    } else {
        // Try to parse simple CASE expression
        match opt(parse_expression).parse(input) {
            Ok((input, expr)) => (input, expr),
            Err(_) => (input, None), // If parsing fails, assume searched CASE
        }
    };

    // Parse WHEN/THEN pairs
    let mut when_then = Vec::new();
    let mut remaining_input = input;

    loop {
        let res = preceded(
            keyword("WHEN"),
            separated_pair(parse_expression, keyword("THEN"), parse_expression),
        )
        .parse(remaining_input);

        match res {
            Ok((new_input, (when_expr, then_expr))) => {
                when_then.push((when_expr, then_expr));
                remaining_input = new_input;
            }
            Err(nom::Err::Error(_)) => break,
            Err(e) => return Err(e),
        }
    }

    // Optional ELSE clause
    let (input, else_expr) =
        opt(preceded(keyword("ELSE"), parse_expression)).parse(remaining_input)?;

    // END keyword
    let (input, _) = keyword("END").parse(input)?;

    Ok((
        input,
        Expression::Case(crate::open_cypher_parser::ast::Case {
            expr: case_expr.map(Box::new),
            when_then,
            else_expr: else_expr.map(Box::new),
        }),
    ))
}

/// A keyword: `word` in any case, not followed by a name character
/// (`WHERE` but not `WHEREx`).
fn keyword<'a>(
    word: &'static str,
) -> impl Parser<&'a str, Output = &'a str, Error = Error<&'a str>> {
    common::keyword(word)
}

/// Parse reduce expression
/// Syntax: reduce(accumulator = initial, variable IN list | expression)
/// Examples:
///   reduce(total = 0, x IN [1, 2, 3] | total + x)
///   reduce(s = '', name IN names | s + name)
///
/// No operator is spelled `|` or `)`, so each expression ends there.
fn parse_reduce_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    // Parse "reduce" keyword (case-insensitive)
    let (input, _) = keyword("REDUCE").parse(input)?;

    // Parse opening parenthesis
    let (input, _) = ws(char('(')).parse(input)?;

    // Parse accumulator = initial_value
    let (input, accumulator) = ws(common::symbolic_name).parse(input)?;
    let (input, _) = ws(char('=')).parse(input)?;
    let (input, initial_value) = ws(parse_expression).parse(input)?;

    // Parse comma separator
    let (input, _) = ws(char(',')).parse(input)?;

    // Parse variable IN list
    let (input, variable) = ws(common::symbolic_name).parse(input)?;
    let (input, _) = keyword("IN").parse(input)?;
    let (input, list) = parse_expression(input)?;

    // Parse '|' separator
    let (input, _) = ws(char('|')).parse(input)?;

    // Parse the expression (the body of the reduce)
    let (input, expression) = ws(parse_expression).parse(input)?;

    // Parse closing parenthesis
    let (input, _) = ws(char(')')).parse(input)?;

    Ok((
        input,
        Expression::ReduceExp(ReduceExpression {
            accumulator,
            initial_value: Box::new(initial_value),
            variable,
            list: Box::new(list),
            expression: Box::new(expression),
        }),
    ))
}

/// The list predicates `all`, `any`, `none`, `single` (`x IN list WHERE
/// predicate`), #1420. Each is read as what it means, three-valued as in
/// Neo4j, over the list comprehensions the lowering already has (on every
/// dialect and element kind). With `t`, `f`, `n` the numbers of elements whose
/// predicate is true, false, NULL:
///
/// * `any`: true if `t > 0`; else NULL if `n > 0`; else false (an empty list:
///   false).
/// * `all`: false if `f > 0`; else NULL if `n > 0`; else true (empty: true).
/// * `none`: false if `t > 0`; else NULL if `n > 0`; else true.
/// * `single`: false if `t > 1`; else NULL if `n > 0`; else `t = 1`.
/// * Of a NULL list (`t IS NULL`): NULL.
///
/// `t` is `size([x IN list WHERE predicate])`, `n` the same of `predicate
/// IS NULL`, `f` of `NOT predicate`: the list and the predicate are read once
/// for each (a predicate that is not deterministic, `rand() < 0.5`, may be
/// read differently by each). A list predicate without WHERE is a syntax
/// error, as in Neo4j ("any(...) requires a WHERE predicate").
fn parse_list_predicate<'a>(input: &'a str) -> IResult<&'a str, Expression<'a>> {
    #[derive(Clone, Copy)]
    enum Quantifier {
        All,
        Any,
        None,
        Single,
    }
    let (input, quantifier) = alt((
        map(keyword("ALL"), |_| Quantifier::All),
        map(keyword("ANY"), |_| Quantifier::Any),
        map(keyword("NONE"), |_| Quantifier::None),
        map(keyword("SINGLE"), |_| Quantifier::Single),
    ))
    .parse(input)?;
    let (input, _) = ws(char('(')).parse(input)?;
    // `q(…` is a list predicate: what does not follow is an error, not a call
    // of a function named `any` (Neo4j has none; the library's mappings of
    // `single(l)` / `none(l)` read a list of booleans, not Cypher).
    let (input, variable) = cut(ws(common::symbolic_name)).parse(input)?;
    let (input, _) = cut(keyword("IN")).parse(input)?;
    let (input, list) = cut(parse_expression).parse(input)?;
    let (input, predicate) = cut(preceded(keyword("WHERE"), parse_expression)).parse(input)?;
    let (input, _) = cut(ws(char(')'))).parse(input)?;

    let count = |condition: Expression<'a>| -> Expression<'a> {
        Expression::FunctionCallExp(FunctionCall {
            name: "size".to_string(),
            args: vec![Expression::ListComprehension(ListComprehension {
                variable: variable.clone(),
                list_expr: Box::new(list.clone()),
                where_clause: Some(Box::new(condition)),
                projection: None,
            })],
        })
    };
    let unary = |operator: Operator, operand: Expression<'a>| -> Expression<'a> {
        Expression::OperatorApplicationExp(OperatorApplication {
            operator,
            operands: vec![operand],
        })
    };
    let int = |i: i64| Expression::Literal(Literal::Integer(i));
    let boolean = |b: bool| Expression::Literal(Literal::Boolean(b));
    let null = || Expression::Literal(Literal::Null);
    let trues = || count(predicate.clone());
    let nulls = || count(unary(Operator::IsNull, predicate.clone()));
    let falses = || count(unary(Operator::Not, predicate.clone()));
    let some = |n: Expression<'a>| binary(Operator::GreaterThan, n, int(0));

    // Of a NULL list, NULL: the size of a comprehension over it is NULL
    // exactly then, read as the counts are (a list of nodes is not compared
    // with NULL itself on every dialect, and Spark's `size(NULL)` is -1).
    let mut when_then = vec![(unary(Operator::IsNull, trues()), null())];
    let else_expr = match quantifier {
        Quantifier::Any => {
            when_then.push((some(trues()), boolean(true)));
            when_then.push((some(nulls()), null()));
            boolean(false)
        }
        Quantifier::All => {
            when_then.push((some(falses()), boolean(false)));
            when_then.push((some(nulls()), null()));
            boolean(true)
        }
        Quantifier::None => {
            when_then.push((some(trues()), boolean(false)));
            when_then.push((some(nulls()), null()));
            boolean(true)
        }
        Quantifier::Single => {
            when_then.push((
                binary(Operator::GreaterThan, trues(), int(1)),
                boolean(false),
            ));
            when_then.push((some(nulls()), null()));
            binary(Operator::Equal, trues(), int(1))
        }
    };
    Ok((
        input,
        Expression::Case(crate::open_cypher_parser::ast::Case {
            expr: None,
            when_then,
            else_expr: Some(Box::new(else_expr)),
        }),
    ))
}

/// Parse pattern comprehension
/// Syntax: [(pattern) WHERE condition | projection]
/// Examples:
///   [(user)-[:FOLLOWS]->(follower) | follower.name]
///   [(a)-[:KNOWS]->(b) WHERE b.age > 25 | b.name]
///   [(n)-[r]->(m) | r.weight]
fn parse_pattern_comprehension(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    // Parse opening bracket '['
    let (input, _) = ws(char('[')).parse(input)?;

    // Parse the graph pattern (e.g., (a)-[:REL]->(b))
    let (input, pattern) =
        ws(crate::open_cypher_parser::path_pattern::parse_path_pattern).parse(input)?;

    // Optional WHERE clause
    let (input, where_clause) = opt(preceded(keyword("WHERE"), parse_expression)).parse(input)?;

    // Parse '|' separator
    let (input, _) = ws(char('|')).parse(input)?;

    // Parse the projection expression (it ends at the `]`)
    let (input, projection) = parse_expression(input)?;

    // Parse closing bracket ']'
    let (input, _) = ws(char(']')).parse(input)?;

    Ok((
        input,
        Expression::PatternComprehension(PatternComprehension {
            pattern: Box::new(pattern),
            where_clause: where_clause.map(Box::new),
            projection: Box::new(projection),
        }),
    ))
}

/// Parse list comprehension: [variable IN list WHERE condition | projection]
/// Examples:
///   [x IN range(1,10) WHERE x > 5]
///   [x IN list | x * 2]
///   [p IN posts WHERE (p)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)]
///   [`x y` IN list | `x y`.name]
///
/// Disambiguated from list literal by requiring `name IN` after `[`. The
/// list, the condition and the projection are expressions: none continues
/// past a `WHERE`, `|` or `]` (no operator is spelled so), and a string or
/// an escaped name holding one is a token of its own.
fn parse_list_comprehension(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    // Parse opening bracket '['
    let (input, _) = ws(char('[')).parse(input)?;

    // Parse variable name — must be followed by the IN keyword
    let (input, variable) = common::symbolic_name(input)?;
    let (input, _) = keyword("IN").parse(input)?;

    let (input, list_expr) = parse_expression(input)?;
    let (input, where_clause) = opt(preceded(keyword("WHERE"), parse_expression)).parse(input)?;
    let (input, projection) = opt(preceded(ws(char('|')), parse_expression)).parse(input)?;

    // Parse closing bracket ']'
    let (input, _) = ws(char(']')).parse(input)?;

    Ok((
        input,
        Expression::ListComprehension(ListComprehension {
            variable,
            list_expr: Box::new(list_expr),
            where_clause: where_clause.map(Box::new),
            projection: projection.map(Box::new),
        }),
    ))
}

// ----------------------------------------------------------------- operators
//
// The levels of Neo4j 5's expression grammar (Cypher5Parser `expression`
// … `expression2`), loosest first:
//
//   OR < XOR < AND < NOT < comparison (= <> != < > <= >=, chained)
//      < predicate (=~, STARTS WITH, ENDS WITH, CONTAINS, IN, NOT IN,
//                   IS [NOT] NULL: at most one, on a `+`/`-` operand)
//      < + - < * / % < ^ < unary - (and DISTINCT) < postfix (.key, [i],
//        [a..b]) < atom
//
// `a < b < c` is `a < b AND b < c` for any of the comparison operators,
// `<>` included, over any number of terms. A parenthesized operand is an
// operand: `(a < b) < c` compares a boolean.

#[cfg(test)]
pub fn parse_operator_symbols(input: &str) -> IResult<&str, Operator> {
    alt((
        comparison_operator,
        map(tag("=~"), |_| Operator::RegexMatch),
        map(tag("+"), |_| Operator::Addition),
        map(tag("-"), |_| Operator::Subtraction),
        map(tag("*"), |_| Operator::Multiplication),
        map(tag("/"), |_| Operator::Division),
        map(tag("%"), |_| Operator::ModuloDivision),
        map(tag("^"), |_| Operator::Exponentiation),
        map(preceded(keyword("STARTS"), keyword("WITH")), |_| {
            Operator::StartsWith
        }),
        map(preceded(keyword("ENDS"), keyword("WITH")), |_| {
            Operator::EndsWith
        }),
        map(keyword("CONTAINS"), |_| Operator::Contains),
        map(preceded(keyword("NOT"), keyword("IN")), |_| Operator::NotIn),
        map(keyword("IN"), |_| Operator::In),
    ))
    .parse(input)
}

fn binary<'a>(operator: Operator, lhs: Expression<'a>, rhs: Expression<'a>) -> Expression<'a> {
    Expression::OperatorApplicationExp(OperatorApplication {
        operator,
        operands: vec![lhs, rhs],
    })
}

/// A left-associative level: `operand (op operand)*`. An operator without
/// an operand ends the level before it (the error then points at the
/// operator: `… AND u.age > ? RETURN u`), as the levels always did.
fn left_assoc<'a>(
    input: &'a str,
    operand: fn(&'a str) -> IResult<&'a str, Expression<'a>>,
    mut op: impl Parser<&'a str, Output = Operator, Error = Error<&'a str>>,
) -> IResult<&'a str, Expression<'a>> {
    let (mut input, mut expr) = operand(input)?;
    loop {
        match op.parse(input) {
            Ok((rest, operator)) => match operand(rest) {
                Ok((rest, rhs)) => {
                    expr = binary(operator, expr, rhs);
                    input = rest;
                }
                Err(nom::Err::Error(_)) => return Ok((input, expr)),
                Err(e) => return Err(e),
            },
            Err(nom::Err::Error(_)) => return Ok((input, expr)),
            Err(e) => return Err(e),
        }
    }
}

fn parse_unary_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    alt((
        // Unary minus (negation)
        map(preceded(ws(tag("-")), parse_unary_expression), |expr| {
            binary(
                Operator::Subtraction,
                Expression::Literal(Literal::Integer(0)),
                expr,
            )
        }),
        // DISTINCT is a unary operator (`count(DISTINCT x)`)
        map(
            preceded(keyword("DISTINCT"), parse_unary_expression),
            |expr| {
                Expression::OperatorApplicationExp(OperatorApplication {
                    operator: Operator::Distinct,
                    operands: vec![expr],
                })
            },
        ),
        parse_postfix_expression,
    ))
    .parse(input)
}

// Exponentiation: ^ — binds tighter than * / %, looser than unary sign, per the
// openCypher grammar (`<arithmetic factor> ::= <arithmetic unary> |
// <arithmetic factor> <circumflex> <arithmetic unary>`). The production is
// left-recursive → left-associative, so `2 ^ 3 ^ 2` parses as `(2 ^ 3) ^ 2`.
// Operands are unary expressions, so `-2 ^ 2` parses as `(-2) ^ 2` (the sign
// binds tighter than `^`, matching the grammar's `<arithmetic unary>` operand).
fn parse_exponent_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    left_assoc(
        input,
        parse_unary_expression,
        ws(map(tag("^"), |_| Operator::Exponentiation)),
    )
}

// Multiplicative operators: * / %
fn parse_multiplicative_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    left_assoc(
        input,
        parse_exponent_expression,
        ws(alt((
            map(tag("*"), |_| Operator::Multiplication),
            map(tag("/"), |_| Operator::Division),
            map(tag("%"), |_| Operator::ModuloDivision),
        ))),
    )
}

// Additive operators: + -
fn parse_additive_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    left_assoc(
        input,
        parse_multiplicative_expression,
        ws(alt((
            map(tag("+"), |_| Operator::Addition),
            map(tag("-"), |_| Operator::Subtraction),
        ))),
    )
}

/// The string, list and null predicates (Neo4j's `expression7`): an additive
/// operand and at most one of `=~ x`, `STARTS WITH x`, `ENDS WITH x`,
/// `CONTAINS x`, `IN x`, `NOT IN x` (a ClickGraph spelling of
/// `NOT (… IN x)`), `IS [NOT] NULL`. They bind tighter than the comparisons
/// (`true = x IN l` is `true = (x IN l)`, `1 + 2 IS NULL` is
/// `(1 + 2) IS NULL`) and do not repeat (`x IN l IN m` does not parse, as in
/// Neo4j).
fn parse_predicate_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    let (input, lhs) = parse_additive_expression(input)?;
    let mut null_test = preceded(
        keyword("IS"),
        alt((
            map(preceded(keyword("NOT"), keyword("NULL")), |_| {
                Operator::IsNotNull
            }),
            map(keyword("NULL"), |_| Operator::IsNull),
        )),
    );
    if let Ok((rest, operator)) = null_test.parse(input) {
        return Ok((
            rest,
            Expression::OperatorApplicationExp(OperatorApplication {
                operator,
                operands: vec![lhs],
            }),
        ));
    }
    let predicate = alt((
        map(ws(tag("=~")), |_| Operator::RegexMatch),
        map(preceded(keyword("STARTS"), keyword("WITH")), |_| {
            Operator::StartsWith
        }),
        map(preceded(keyword("ENDS"), keyword("WITH")), |_| {
            Operator::EndsWith
        }),
        map(keyword("CONTAINS"), |_| Operator::Contains),
        map(preceded(keyword("NOT"), keyword("IN")), |_| Operator::NotIn),
        map(keyword("IN"), |_| Operator::In),
    ));
    match opt(predicate).parse(input)? {
        // Without an operand the predicate is not one: the error points at
        // it, as at any operator.
        (rest, Some(operator)) => match parse_additive_expression(rest) {
            Ok((rest, rhs)) => Ok((rest, binary(operator, lhs, rhs))),
            Err(nom::Err::Error(_)) => Ok((input, lhs)),
            Err(e) => Err(e),
        },
        (rest, None) => Ok((rest, lhs)),
    }
}

/// `=`, `<>`, `!=`, `<`, `>`, `<=`, `>=` (not the `=` of `=~`).
fn comparison_operator(input: &str) -> IResult<&str, Operator> {
    alt((
        map(tag(">="), |_| Operator::GreaterThanEqual),
        map(tag("<="), |_| Operator::LessThanEqual),
        map(tag("<>"), |_| Operator::NotEqual),
        map(tag("!="), |_| Operator::NotEqual),
        map(tag(">"), |_| Operator::GreaterThan),
        map(tag("<"), |_| Operator::LessThan),
        map(terminated(tag("="), not(char('~'))), |_| Operator::Equal),
    ))
    .parse(input)
}

/// The comparisons (Neo4j's `expression8`): `a op b op c …` with any of
/// [`comparison_operator`] is the conjunction of each adjacent pair,
/// `a op b AND b op c AND …` (`1 <> 2 <> 1` is true, `1 < 2 < 3 < 2` false,
/// as in Neo4j). One comparison is itself. Each operand is a
/// [`parse_predicate_expression`]; a parenthesized one is an operand like
/// any other (`(a < b) < c` compares a boolean, #1355).
fn parse_comparison_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    let (mut input, first) = parse_predicate_expression(input)?;
    let mut operands = vec![first];
    let mut operators = Vec::new();
    loop {
        match ws(comparison_operator).parse(input) {
            Ok((rest, operator)) => match parse_predicate_expression(rest) {
                Ok((rest, rhs)) => {
                    operators.push(operator);
                    operands.push(rhs);
                    input = rest;
                }
                Err(nom::Err::Error(_)) => break,
                Err(e) => return Err(e),
            },
            Err(nom::Err::Error(_)) => break,
            Err(e) => return Err(e),
        }
    }
    let mut pairs = operators
        .iter()
        .zip(operands.windows(2))
        .map(|(op, pair)| binary(*op, pair[0].clone(), pair[1].clone()));
    let Some(first_pair) = pairs.next() else {
        let only = operands
            .pop()
            .ok_or_else(|| nom::Err::Error(Error::new(input, ErrorKind::Verify)))?;
        return Ok((input, only));
    };
    let chain = pairs.fold(first_pair, |chain, pair| binary(Operator::And, chain, pair));
    Ok((input, chain))
}

// NOT operator - lower precedence than comparison
// This allows "NOT a = b" to parse as "NOT (a = b)" rather than "(NOT a) = b"
fn parse_not_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    alt((
        map(preceded(keyword("NOT"), parse_not_expression), |expr| {
            Expression::OperatorApplicationExp(OperatorApplication {
                operator: Operator::Not,
                operands: vec![expr],
            })
        }),
        parse_comparison_expression,
    ))
    .parse(input)
}

// Deprecated: test helper that redirects to parse_comparison_expression
#[cfg(test)]
fn parse_binary_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    parse_comparison_expression(input)
}

fn parse_logical_and(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    left_assoc(
        input,
        parse_not_expression,
        map(keyword("AND"), |_| Operator::And),
    )
}

fn parse_logical_xor(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    left_assoc(
        input,
        parse_logical_and,
        map(keyword("XOR"), |_| Operator::Xor),
    )
}

/// `OR`, the loosest (the keyword: never the `OR` of `ORDER`).
fn parse_logical_or(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    left_assoc(
        input,
        parse_logical_xor,
        map(keyword("OR"), |_| Operator::Or),
    )
}

pub fn parse_function_call(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    // First, parse the function name - support dotted (namespaced) names like
    // ch.arrayFilter; each part is a symbolic name (`` `toUpper`(x) ``).
    let start = input;
    let (input, name_parts) = separated_list0(char('.'), ws(common::symbolic_name)).parse(input)?;

    // If no dots found, need at least one identifier. A function's name is
    // letters, digits and `_`, escaped or not: a function the library does
    // not know is called by its name in the SQL (`ch.` / passthrough), so
    // another character (`` `foo bar`(x) ``, `` `a.b`(x) ``) is no call.
    if name_parts.is_empty()
        || name_parts
            .iter()
            .any(|p| p.is_empty() || !p.chars().all(|c| c.is_alphanumeric() || c == '_'))
    {
        return Err(nom::Err::Error(Error::new(start, ErrorKind::Alpha)));
    }

    let name = name_parts.join(".");

    // Then parse the comma-separated arguments within parentheses.
    // Need to try lambda first before regular expression
    let (input, args) = delimited(
        ws(char('(')),
        separated_list0(
            ws(char(',')),
            alt((parse_lambda_expression, parse_expression)),
        ),
        ws(char(')')),
    )
    .parse(input)?;

    Ok((
        input,
        Expression::FunctionCallExp(FunctionCall { name, args }),
    ))
}

/// Parse a lambda expression: param -> body or (param1, param2) -> body
/// Examples:
///   x -> x > 5
///   (x, y) -> x + y
///   elem -> elem.field = 'value'
pub fn parse_lambda_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    // Try single parameter: x ->
    let single_param = map(
        terminated(ws(common::symbolic_name), ws(tag("->"))),
        |param| vec![param],
    );

    // Try multiple parameters: (x, y) ->
    let multi_param = delimited(
        ws(char('(')),
        separated_list0(ws(char(',')), ws(common::symbolic_name)),
        delimited(ws(char(')')), ws(tag("->")), multispace0),
    );

    // Parse parameters (single or multiple)
    let (input, params) = alt((multi_param, single_param)).parse(input)?;

    // Parse the body expression
    let (input, body) = parse_expression(input)?;

    Ok((
        input,
        Expression::Lambda(LambdaExpression {
            params,
            body: Box::new(body),
        }),
    ))
}

/// Parse a map literal: {key1: value1, key2: value2}
/// Used in duration({days: 5}), point({x: 1, y: 2}), etc.
/// A key is a symbolic name (`` {`it's`: 1} ``).
pub fn parse_map_literal(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    // Parse content within { ... } as comma-separated key:value pairs
    let (input, entries) = delimited(
        // Opening brace with optional whitespace
        delimited(multispace0, char('{'), multispace0),
        // Zero or more key:value pairs separated by commas
        separated_list0(
            delimited(multispace0, char(','), multispace0),
            // Each entry is: key : value (using native tuple parser)
            (
                common::symbolic_name,                          // key
                delimited(multispace0, char(':'), multispace0), // colon
                parse_expression,                               // value (any expression)
            ),
        ),
        // Closing brace with optional whitespace
        delimited(multispace0, char('}'), multispace0),
    )
    .parse(input)?;

    // Transform (key, _, value) tuples into (key, value) pairs
    let pairs: Vec<(Name, Expression)> = entries
        .into_iter()
        .map(|(key, _, value)| (key, value))
        .collect();

    Ok((input, Expression::MapLiteral(pairs)))
}

pub fn parse_list_literal(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    // Parse content within [ ... ] as a comma-separated list of expressions.
    // Note: Pattern comprehensions [(pattern) | projection] are handled by
    // parse_pattern_comprehension which comes before this in the alt() chain.
    let (input, exprs) = delimited(
        // Opening bracket with optional whitespace afterwards
        delimited(multispace0, char('['), multispace0),
        // Zero or more expressions separated by commas (with optional whitespace)
        separated_list0(
            delimited(multispace0, char(','), multispace0),
            parse_expression,
        ),
        // Closing bracket with optional whitespace preceding it
        delimited(multispace0, char(']'), multispace0),
    )
    .parse(input)?;

    Ok((input, Expression::List(exprs)))
}

/// Helper function to determine if a character is valid in a parameter name.
fn is_param_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// `$name`: a symbolic name (`` $`a b` ``) or a number (`$0`).
/// `$name`: its name, a symbolic name (`` $`a b` ``) or a number (`$0`).
pub fn parse_parameter_name(input: &'_ str) -> IResult<&'_ str, Name<'_>> {
    preceded(
        tag("$"),
        alt((
            common::symbolic_name,
            map(take_while1(is_param_char), Name::Borrowed),
        )),
    )
    .parse(input)
}

/// A parameter. A property of it (`$p.year`, the temporal accessor; `$p.k`)
/// is the postfix loop's, as of any value.
pub fn parse_parameter(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    map(parse_parameter_name, Expression::Parameter).parse(input)
}

/// Check if a string is a reserved Cypher keyword that cannot be used as a variable
/// at the start of an expression. This catches cases like "WHERE AND ..." where AND
/// is incorrectly treated as a variable name.
///
/// We only block binary operators that require a left operand:
/// - Logical: AND, OR, XOR
/// - Note: NOT is a unary prefix operator, so it IS valid at expression start
fn is_binary_operator_keyword(s: &str) -> bool {
    let upper = s.to_uppercase();
    matches!(upper.as_str(), "AND" | "OR" | "XOR")
}

/// Parse a label expression: variable:Label
/// This checks if a variable has a specific label
/// Example: message:Comment, n:Person, `my node`:`My Label`
fn parse_label_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    // Parse variable name
    let (input, variable) = ws(common::symbolic_name).parse(input)?;
    // Parse colon
    let (input, _) = char(':').parse(input)?;
    // Parse label name
    let (input, label) = common::symbolic_name(input)?;

    Ok((input, Expression::LabelExpression { variable, label }))
}

pub fn parse_literal_or_variable_expression(input: &'_ str) -> IResult<&'_ str, Expression<'_>> {
    alt((
        map(ws(parse_string_literal), Expression::Literal),
        map(ws(parse_double_quoted_string_literal), Expression::Literal),
        // Try label expression first (variable:Label)
        parse_label_expression,
        // The star of `count(*)`, `RETURN *`, `WITH *`.
        map(ws(tag("*")), |_| Expression::Variable(Name::Borrowed("*"))),
        // Numbers: 123, -456, 3.14, 1e10.
        |input| {
            let (remaining, s) = ws(common::parse_numeric_literal).parse(input)?;
            if let Ok(i) = s.parse::<i64>() {
                Ok((remaining, Expression::Literal(Literal::Integer(i))))
            } else if let Ok(f) = s.parse::<f64>() {
                Ok((remaining, Expression::Literal(Literal::Float(f))))
            } else {
                Err(nom::Err::Error(Error::new(input, ErrorKind::Float)))
            }
        },
        // A name: a variable, or the keyword literals `null`, `true` and
        // `false` when unescaped (`` `true` `` is a variable). A binary
        // operator keyword is no expression ("WHERE AND ...").
        |input| {
            let (name_input, _) = multispace0(input)?;
            let (remaining, s) = ws(common::symbolic_name).parse(name_input)?;
            if common::is_escaped_name(name_input) {
                // The variable `` `*` ``: carried as the star of `RETURN *`
                // and `count(*)`, it would read alike, so it does not parse.
                if s == "*" {
                    return Err(nom::Err::Error(Error::new(input, ErrorKind::Tag)));
                }
                return Ok((remaining, Expression::Variable(s)));
            }
            if s.eq_ignore_ascii_case("null") {
                Ok((remaining, Expression::Literal(Literal::Null)))
            } else if s.eq_ignore_ascii_case("true") {
                Ok((remaining, Expression::Literal(Literal::Boolean(true))))
            } else if s.eq_ignore_ascii_case("false") {
                Ok((remaining, Expression::Literal(Literal::Boolean(false))))
            } else if is_binary_operator_keyword(&s) {
                Err(nom::Err::Error(Error::new(input, ErrorKind::Tag)))
            } else {
                Ok((remaining, Expression::Variable(s)))
            }
        },
    ))
    .parse(input)
}

/// Body of a quoted Cypher string literal: everything up to the first `quote` that is not
/// escaped. A backslash escapes the next character, so `\'` (and `\"`, `\\`) never ends the
/// literal. The body stays RAW (escapes undecoded) — the SQL emitter translates it
/// (`cypher_string_to_sql_literal`).
fn quoted_string_body(input: &str, quote: char) -> IResult<&str, &str> {
    let (rest, _) = char(quote).parse(input)?;
    let mut chars = rest.char_indices();
    while let Some((i, c)) = chars.next() {
        if c == '\\' {
            chars.next(); // the escaped character, whatever it is
        } else if c == quote {
            return Ok((&rest[i + c.len_utf8()..], &rest[..i]));
        }
    }
    Err(nom::Err::Error(Error::new(input, ErrorKind::TakeUntil)))
}

pub fn parse_string_literal(input: &'_ str) -> IResult<&'_ str, Literal<'_>> {
    let (input, s) = quoted_string_body(input, '\'')?;

    Ok((input, Literal::String(s)))
}

pub fn parse_double_quoted_string_literal(input: &'_ str) -> IResult<&'_ str, Literal<'_>> {
    let (input, s) = quoted_string_body(input, '"')?;

    Ok((input, Literal::String(s)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_literal_escaped_quote_does_not_end_the_literal_1173() {
        // the body stays raw; the SQL emitter translates the escapes
        for (input, body) in [
            (r"'it\'s' AS r", r"it\'s"),
            (r#""say \"hi\"" AS r"#, r#"say \"hi\""#),
            (r"'a\\b' AS r", r"a\\b"),
            (r"'tab\there' AS r", r"tab\there"),
            // an escaped backslash right before the closing quote does not escape it
            (r"'it\\' AS r", r"it\\"),
        ] {
            let lit = if input.starts_with('"') {
                parse_double_quoted_string_literal(input)
            } else {
                parse_string_literal(input)
            };
            let (rest, lit) = lit.unwrap_or_else(|e| panic!("{input}: {e:?}"));
            assert_eq!(rest, " AS r", "{input}");
            assert!(
                matches!(lit, Literal::String(b) if b == body),
                "{input}: {lit:?}"
            );
        }
    }

    #[test]
    fn unterminated_string_literal_still_fails_1173() {
        assert!(parse_string_literal(r"'it\'").is_err());
        assert!(parse_string_literal("'abc").is_err());
        assert!(parse_double_quoted_string_literal(r#""abc\""#).is_err());
    }

    #[test]
    fn test_parse_expression_pattern_comp_standalone() {
        // Test parsing pattern comprehension by itself
        let input = "[(p)-[:KNOWS]->(f) | f.firstName]";
        let result = parse_expression(input);
        match result {
            Ok((remaining, expr)) => {
                println!("Pattern comp standalone - Expression: {:?}", expr);
                println!("Pattern comp standalone - Remaining: {:?}", remaining);
                assert_eq!(remaining, "");
                assert!(matches!(expr, Expression::PatternComprehension(_)));
            }
            Err(e) => {
                panic!("Should parse pattern comprehension: {:?}", e);
            }
        }
    }

    #[test]
    fn test_parse_expression_with_pattern_comp_in_remainder() {
        // Test that when parsing WHERE clause expression, pattern comp in RETURN doesn't cause failure
        // This tests the fix for the legacy pattern comp detection code that was blocking valid queries
        let input = "true RETURN [(p)-[:KNOWS]->(f) | f.firstName]";
        let result = parse_expression(input);
        match result {
            Ok((remaining, expr)) => {
                assert!(matches!(expr, Expression::Literal(Literal::Boolean(true))));
                assert!(
                    remaining.trim().starts_with("RETURN"),
                    "Expected RETURN to remain after parsing 'true', got: {}",
                    remaining
                );
            }
            Err(e) => {
                panic!(
                    "parse_expression should parse 'true' and leave RETURN as remainder: {:?}",
                    e
                );
            }
        }
    }

    #[test]
    fn test_parse_expression_equality_with_pattern_comp_in_remainder() {
        // Test that equality expression stops before RETURN, even when pattern comp follows
        let input = "p.id = 1 RETURN [(p)-[:KNOWS]->(f) | f.firstName]";
        let result = parse_expression(input);
        match result {
            Ok((remaining, expr)) => {
                println!("Expression: {:?}", expr);
                println!("Remaining: {:?}", remaining);
                assert!(
                    remaining.trim().starts_with("RETURN"),
                    "Expected RETURN to remain, got: {}",
                    remaining
                );
            }
            Err(e) => {
                panic!("parse_expression failed: {:?}", e);
            }
        }
    }

    #[test]
    fn test_parse_operator_symbols() {
        let (rem, op) = parse_operator_symbols(">=").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::GreaterThanEqual);

        let (rem, op) = parse_operator_symbols("<=").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::LessThanEqual);

        let (rem, op) = parse_operator_symbols("<>").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::NotEqual);

        let (rem, op) = parse_operator_symbols("!=").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::NotEqual);

        let (rem, op) = parse_operator_symbols(">").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::GreaterThan);

        let (rem, op) = parse_operator_symbols("<").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::LessThan);

        let (rem, op) = parse_operator_symbols("=").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::Equal);

        let (rem, op) = parse_operator_symbols("+").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::Addition);

        let (rem, op) = parse_operator_symbols("-").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::Subtraction);

        let (rem, op) = parse_operator_symbols("*").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::Multiplication);

        let (rem, op) = parse_operator_symbols("/").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::Division);

        let (rem, op) = parse_operator_symbols("%").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::ModuloDivision);

        let (rem, op) = parse_operator_symbols("^").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::Exponentiation);

        let (rem, op) = parse_operator_symbols("IN").unwrap();
        assert_eq!(rem, "");
        assert_eq!(op, Operator::In);
    }

    // null predicates
    #[test]
    fn test_parse_predicate_expression_is_null() {
        let (rem, expr) = parse_predicate_expression("a IS NULL").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::OperatorApplicationExp(OperatorApplication {
            operator: Operator::IsNull,
            operands: vec![Expression::Variable("a".into())],
        });
        assert_eq!(&expr, &expected);
    }

    #[test]
    fn test_parse_predicate_expression_is_not_null() {
        let (rem, expr) = parse_predicate_expression("a IS NOT NULL").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::OperatorApplicationExp(OperatorApplication {
            operator: Operator::IsNotNull,
            operands: vec![Expression::Variable("a".into())],
        });
        assert_eq!(&expr, &expected);
    }

    // NOT operator (handled by parse_not_expression, not parse_unary_expression)
    #[test]
    fn test_parse_unary_expression_not() {
        let (rem, expr) = parse_not_expression("NOT a").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Not,
            operands: vec![Expression::Variable("a".into())],
        });
        assert_eq!(&expr, &expected);
    }

    // binary
    #[test]
    fn test_parse_binary_expression_addition() {
        let (rem, expr) = parse_binary_expression("a + b").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Addition,
            operands: vec![
                Expression::Variable("a".into()),
                Expression::Variable("b".into()),
            ],
        });
        assert_eq!(&expr, &expected);
    }

    // and
    #[test]
    fn test_parse_logical_and() {
        let (rem, expr) = parse_logical_and("a AND b").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::OperatorApplicationExp(OperatorApplication {
            operator: Operator::And,
            operands: vec![
                Expression::Variable("a".into()),
                Expression::Variable("b".into()),
            ],
        });
        assert_eq!(&expr, &expected);
    }

    // or
    #[test]
    fn test_parse_logical_or() {
        let (rem, expr) = parse_logical_or("a OR b").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Or,
            operands: vec![
                Expression::Variable("a".into()),
                Expression::Variable("b".into()),
            ],
        });
        assert_eq!(&expr, &expected);
    }

    // fn call
    #[test]
    fn test_parse_function_call() {
        // Testing a function call: foo(a, b)
        let (rem, expr) = parse_function_call("foo(a, b)").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::FunctionCallExp(FunctionCall {
            name: "foo".to_string(),
            args: vec![
                Expression::Variable("a".into()),
                Expression::Variable("b".into()),
            ],
        });
        assert_eq!(&expr, &expected);
    }

    #[test]
    fn test_parse_function_cal_count() {
        // Testing a function call: foo(a, b)
        let (rem, expr) = parse_function_call("count(*)").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::FunctionCallExp(FunctionCall {
            name: "count".to_string(),
            args: vec![Expression::Variable("*".into())],
        });
        assert_eq!(&expr, &expected);
    }

    // list
    #[test]
    fn test_parse_list_literal() {
        let (rem, expr) = parse_list_literal("[a, b]").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::List(vec![
            Expression::Variable("a".into()),
            Expression::Variable("b".into()),
        ]);
        assert_eq!(&expr, &expected);
    }

    //  property access
    fn prop<'a>(base: &'a str, key: &'a str) -> Expression<'a> {
        Expression::PropertyAccessExp(PropertyAccess {
            base: base.into(),
            key: key.into(),
        })
    }

    fn prop_of<'a>(base: Expression<'a>, key: &'a str) -> Expression<'a> {
        Expression::PropertyOf {
            base: Box::new(base),
            key: key.into(),
        }
    }

    #[test]
    fn test_parse_property_access() {
        let (rem, expr) = parse_expression("user.name").unwrap();
        assert_eq!(rem, "");
        assert_eq!(expr, prop("user", "name"));
    }

    #[test]
    fn test_parse_chained_property_access() {
        // latestLike.msg.id is (latestLike.msg).id: the conversion carries it as
        // the variable's key path `msg.id`.
        let (rem, expr) = parse_expression("latestLike.msg.id").unwrap();
        assert_eq!(rem, "");
        assert_eq!(expr, prop_of(prop("latestLike", "msg"), "id"));
    }

    #[test]
    fn test_parse_chained_property_access_with_remainder() {
        let (rem, expr) = parse_postfix_expression("latestLike.msg.id + 1").unwrap();
        assert_eq!(rem, " + 1");
        assert_eq!(expr, prop_of(prop("latestLike", "msg"), "id"));
    }

    #[test]
    fn test_parse_chained_property_with_temporal() {
        // latestLike.msg.creationDate.year → year(latestLike.msg.creationDate)
        let (rem, expr) = parse_expression("latestLike.msg.creationDate.year").unwrap();
        assert_eq!(rem, "");
        assert_eq!(
            expr,
            Expression::FunctionCallExp(FunctionCall {
                name: "year".to_string(),
                args: vec![prop_of(prop("latestLike", "msg"), "creationDate")],
            })
        );
    }

    /// Escaped names in a property access, and a property of any value
    /// (#1489, #1381).
    #[test]
    fn property_access_of_escaped_names_and_any_value_1489_1381() {
        for (input, expected) in [
            ("a.`name`", prop("a", "name")),
            ("`my node`.`my key`", prop("my node", "my key")),
            ("a.`q?`", prop("a", "q?")),
            ("a.`q.x`", prop("a", "q.x")),
            // An escaped temporal accessor is the accessor, as unescaped.
            (
                "d.`year`",
                Expression::FunctionCallExp(FunctionCall {
                    name: "year".to_string(),
                    args: vec![Expression::Variable("d".into())],
                }),
            ),
            ("_a.name", prop("_a", "name")),
            (
                "xs[1].name",
                prop_of(
                    Expression::ArraySubscript {
                        array: Box::new(Expression::Variable("xs".into())),
                        index: Box::new(Expression::Literal(Literal::Integer(1))),
                    },
                    "name",
                ),
            ),
            (
                "head(l).name",
                prop_of(
                    Expression::FunctionCallExp(FunctionCall {
                        name: "head".to_string(),
                        args: vec![Expression::Variable("l".into())],
                    }),
                    "name",
                ),
            ),
            ("(a).name", prop("a", "name")),
            ("(a.b).c", prop_of(prop("a", "b"), "c")),
        ] {
            let (rem, expr) = parse_expression(input).unwrap_or_else(|e| panic!("{input}: {e:?}"));
            assert_eq!(rem, "", "{input}");
            assert_eq!(expr, expected, "{input}");
        }
        // nss[0][1].name: a property of an element of an element.
        let (rem, expr) = parse_expression("nss[0][-1].name").unwrap();
        assert_eq!(rem, "");
        let Expression::PropertyOf { base, key } = expr else {
            panic!("{expr:?}");
        };
        assert_eq!(key, "name");
        assert!(matches!(
            *base,
            Expression::ArraySubscript { ref array, .. }
                if matches!(**array, Expression::ArraySubscript { .. })
        ));
    }
    // fn_call + operator
    #[test]
    fn test_parse_expression_fn_call_expression() {
        // Example: foo(a, b) + c
        let (rem, expr) = parse_expression("foo(a, b) + c").unwrap();
        assert_eq!(rem, "");
        let expected = Expression::OperatorApplicationExp(OperatorApplication {
            operator: Operator::Addition,
            operands: vec![
                Expression::FunctionCallExp(FunctionCall {
                    name: "foo".to_string(),
                    args: vec![
                        Expression::Variable("a".into()),
                        Expression::Variable("b".into()),
                    ],
                }),
                Expression::Variable("c".into()),
            ],
        });
        assert_eq!(&expr, &expected);
    }

    // reduce expression
    #[test]
    fn test_parse_reduce_expression_simple() {
        let (rem, expr) =
            parse_expression("reduce(total = 0, x IN [1, 2, 3] | total + x)").unwrap();
        assert_eq!(rem, "");

        if let Expression::ReduceExp(reduce) = expr {
            assert_eq!(reduce.accumulator, "total");
            assert_eq!(reduce.variable, "x");
            // Check initial value is 0
            if let Expression::Literal(Literal::Integer(n)) = *reduce.initial_value {
                assert_eq!(n, 0);
            } else {
                panic!("Expected integer literal for initial value");
            }
            // Check list has 3 elements
            if let Expression::List(items) = *reduce.list {
                assert_eq!(items.len(), 3);
            } else {
                panic!("Expected list for list expression");
            }
            // Check expression is addition
            if let Expression::OperatorApplicationExp(op) = *reduce.expression {
                assert_eq!(op.operator, Operator::Addition);
            } else {
                panic!("Expected operator application for expression");
            }
        } else {
            panic!("Expected ReduceExp variant");
        }
    }

    #[test]
    fn test_parse_reduce_expression_with_variable_list() {
        let (rem, expr) = parse_expression("reduce(s = '', name IN names | s + name)").unwrap();
        assert_eq!(rem, "");

        if let Expression::ReduceExp(reduce) = expr {
            assert_eq!(reduce.accumulator, "s");
            assert_eq!(reduce.variable, "name");
            // Check list is a variable reference
            if let Expression::Variable(var) = *reduce.list {
                assert_eq!(var, "names");
            } else {
                panic!("Expected variable for list expression");
            }
        } else {
            panic!("Expected ReduceExp variant");
        }
    }

    #[test]
    fn test_parse_map_literal_single_entry() {
        let (rem, expr) = parse_map_literal("{days: 5}").unwrap();
        assert_eq!(rem, "");

        if let Expression::MapLiteral(entries) = expr {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].0, "days");
            if let Expression::Literal(Literal::Integer(n)) = entries[0].1 {
                assert_eq!(n, 5);
            } else {
                panic!("Expected integer literal for value");
            }
        } else {
            panic!("Expected MapLiteral variant");
        }
    }

    #[test]
    fn test_parse_map_literal_multiple_entries() {
        let (rem, expr) = parse_map_literal("{days: 5, hours: 2}").unwrap();
        assert_eq!(rem, "");

        if let Expression::MapLiteral(entries) = expr {
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].0, "days");
            assert_eq!(entries[1].0, "hours");
        } else {
            panic!("Expected MapLiteral variant");
        }
    }

    #[test]
    fn test_parse_map_literal_with_expression_value() {
        let (rem, expr) = parse_map_literal("{offset: x + 1}").unwrap();
        assert_eq!(rem, "");

        if let Expression::MapLiteral(entries) = expr {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].0, "offset");
            // Value should be an operator application (x + 1)
            if let Expression::OperatorApplicationExp(op) = &entries[0].1 {
                assert_eq!(op.operator, Operator::Addition);
            } else {
                panic!("Expected operator application for value");
            }
        } else {
            panic!("Expected MapLiteral variant");
        }
    }

    #[test]
    fn test_parse_duration_with_map_arg() {
        let (rem, expr) = parse_expression("duration({days: 5})").unwrap();
        assert_eq!(rem, "");

        if let Expression::FunctionCallExp(fc) = expr {
            assert_eq!(fc.name, "duration");
            assert_eq!(fc.args.len(), 1);
            if let Expression::MapLiteral(entries) = &fc.args[0] {
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].0, "days");
            } else {
                panic!("Expected MapLiteral argument");
            }
        } else {
            panic!("Expected FunctionCallExp variant");
        }
    }

    #[test]
    fn test_parse_label_expression() {
        // Test basic label expression
        let (rem, expr) = parse_label_expression("u:User").unwrap();
        assert_eq!(rem, "");
        if let Expression::LabelExpression { variable, label } = expr {
            assert_eq!(variable, "u");
            assert_eq!(label, "User");
        } else {
            panic!("Expected LabelExpression, got {:?}", expr);
        }

        // Test with different casing
        let (rem, expr) = parse_label_expression("message:Comment").unwrap();
        assert_eq!(rem, "");
        if let Expression::LabelExpression { variable, label } = expr {
            assert_eq!(variable, "message");
            assert_eq!(label, "Comment");
        } else {
            panic!("Expected LabelExpression, got {:?}", expr);
        }
    }

    #[test]
    fn test_parse_label_expression_in_full_expression() {
        // Test label expression through parse_expression
        let (rem, expr) = parse_expression("u:User").unwrap();
        assert_eq!(rem, "");
        if let Expression::LabelExpression { variable, label } = expr {
            assert_eq!(variable, "u");
            assert_eq!(label, "User");
        } else {
            panic!("Expected LabelExpression, got {:?}", expr);
        }
    }

    #[test]
    fn test_parse_size_with_pattern() {
        // Test size() with a simple path pattern: size((n)-[:KNOWS]->())
        let (rem, expr) = parse_expression("size((n)-[:KNOWS]->())").unwrap();
        assert_eq!(rem, "");
        if let Expression::FunctionCallExp(fc) = expr {
            assert_eq!(fc.name, "size");
            assert_eq!(fc.args.len(), 1);
            if let Expression::PathPattern(_) = &fc.args[0] {
                // Good - the argument is a path pattern
            } else {
                panic!("Expected PathPattern argument, got {:?}", fc.args[0]);
            }
        } else {
            panic!("Expected FunctionCallExp, got {:?}", expr);
        }
    }

    #[test]
    fn test_parse_size_with_bidirectional_pattern() {
        // Test size() with anonymous bidirectional pattern: size((p)-[:KNOWS]-())
        let (rem, expr) = parse_expression("size((p)-[:KNOWS]-())").unwrap();
        assert_eq!(rem, "");
        if let Expression::FunctionCallExp(fc) = expr {
            assert_eq!(fc.name, "size");
            assert_eq!(fc.args.len(), 1);
            if let Expression::PathPattern(_) = &fc.args[0] {
                // Good - the argument is a path pattern
            } else {
                panic!("Expected PathPattern argument, got {:?}", fc.args[0]);
            }
        } else {
            panic!("Expected FunctionCallExp, got {:?}", expr);
        }
    }

    #[test]
    fn test_parse_lambda_single_param() {
        // Test single parameter lambda: x -> x > 5
        let (rem, lambda) = parse_lambda_expression("x -> x > 5").unwrap();
        assert_eq!(rem, "");
        if let Expression::Lambda(l) = lambda {
            assert_eq!(l.params.len(), 1);
            assert_eq!(l.params[0], "x");
            // Body should be an operator application: x > 5
            if let Expression::OperatorApplicationExp(op) = *l.body {
                assert_eq!(op.operator, Operator::GreaterThan);
            } else {
                panic!("Expected OperatorApplicationExp, got {:?}", l.body);
            }
        } else {
            panic!("Expected Lambda, got {:?}", lambda);
        }
    }

    #[test]
    fn test_parse_lambda_multi_param() {
        // Test multiple parameter lambda: (x, y) -> x + y
        let (rem, lambda) = parse_lambda_expression("(x, y) -> x + y").unwrap();
        assert_eq!(rem, "");
        if let Expression::Lambda(l) = lambda {
            assert_eq!(l.params.len(), 2);
            assert_eq!(l.params[0], "x");
            assert_eq!(l.params[1], "y");
            // Body should be an operator application: x + y
            if let Expression::OperatorApplicationExp(op) = *l.body {
                assert_eq!(op.operator, Operator::Addition);
            } else {
                panic!("Expected OperatorApplicationExp, got {:?}", l.body);
            }
        } else {
            panic!("Expected Lambda, got {:?}", lambda);
        }
    }

    #[test]
    fn test_parse_lambda_in_function_call() {
        // Test lambda as function argument: ch.arrayFilter(x -> x > 5, [1,2,3])
        let (rem, expr) = parse_expression("ch.arrayFilter(x -> x > 5, [1,2,3])").unwrap();
        assert_eq!(rem, "");
        if let Expression::FunctionCallExp(fc) = expr {
            assert_eq!(fc.name, "ch.arrayFilter");
            assert_eq!(fc.args.len(), 2);
            // First argument should be a lambda
            if let Expression::Lambda(l) = &fc.args[0] {
                assert_eq!(l.params.len(), 1);
                assert_eq!(l.params[0], "x");
            } else {
                panic!("Expected Lambda argument, got {:?}", fc.args[0]);
            }
            // Second argument should be a list
            if let Expression::List(_) = &fc.args[1] {
                // Good
            } else {
                panic!("Expected List argument, got {:?}", fc.args[1]);
            }
        } else {
            panic!("Expected FunctionCallExp, got {:?}", expr);
        }
    }

    #[test]
    #[ignore = "Pattern comprehension not yet implemented"]
    fn test_parse_size_with_pattern_comprehension() {
        // Test pattern comprehension: size([(t)-[r]-(f) | f])
        let (rem, expr) = parse_expression("size([(t)-[r]-(f) | f])").unwrap();
        assert_eq!(rem, "");
        assert!(matches!(expr, Expression::FunctionCallExp(_)));
    }

    #[test]
    #[ignore = "Pattern comprehension not yet implemented"]
    fn test_parse_multiplication_with_size_pattern() {
        // Test: 100 * size([(t)-[r]-(f) | f])
        // This is the failing case from bi-8
        let result = parse_expression("100 * size([(t)-[r]-(f) | f])");
        eprintln!("Parse result: {:#?}", result);

        match result {
            Ok((rem, expr)) => {
                eprintln!("SUCCESS - Remaining: '{}'", rem);
                eprintln!("Expression: {:#?}", expr);
                assert_eq!(rem, "");
                // Should be multiplication operator with 100 on left, size(...) on right
                if let Expression::OperatorApplicationExp(op) = expr {
                    assert_eq!(op.operator, Operator::Multiplication);
                    assert_eq!(op.operands.len(), 2);
                } else {
                    panic!("Expected OperatorApplicationExp, got {:?}", expr);
                }
            }
            Err(e) => {
                eprintln!("FAILED: {:?}", e);
                panic!("Parser should handle 100 * size([pattern])");
            }
        }
    }

    #[test]
    fn test_parse_exponentiation_basic() {
        // `2 ^ 3` parses as Exponentiation(2, 3).
        let (rem, expr) = parse_expression("2 ^ 3").unwrap();
        assert_eq!(rem, "");
        if let Expression::OperatorApplicationExp(op) = expr {
            assert_eq!(op.operator, Operator::Exponentiation);
            assert_eq!(op.operands.len(), 2);
        } else {
            panic!("Expected Exponentiation, got {:?}", expr);
        }
    }

    #[test]
    fn test_parse_exponentiation_binds_tighter_than_multiply() {
        // `2 ^ 3 * 4` → `(2 ^ 3) * 4`: outer op is Multiplication, left is `^`.
        let (rem, expr) = parse_expression("2 ^ 3 * 4").unwrap();
        assert_eq!(rem, "");
        let Expression::OperatorApplicationExp(op) = expr else {
            panic!("Expected operator, got {:?}", expr);
        };
        assert_eq!(op.operator, Operator::Multiplication);
        assert!(
            matches!(
                &op.operands[0],
                Expression::OperatorApplicationExp(inner) if inner.operator == Operator::Exponentiation
            ),
            "left of `*` should be the `^` subtree, got {:?}",
            op.operands[0]
        );
    }

    #[test]
    fn test_parse_exponentiation_binds_tighter_than_add() {
        // `1 + 2 ^ 3` → `1 + (2 ^ 3)`: outer op is Addition, right is `^`.
        let (rem, expr) = parse_expression("1 + 2 ^ 3").unwrap();
        assert_eq!(rem, "");
        let Expression::OperatorApplicationExp(op) = expr else {
            panic!("Expected operator, got {:?}", expr);
        };
        assert_eq!(op.operator, Operator::Addition);
        assert!(
            matches!(
                &op.operands[1],
                Expression::OperatorApplicationExp(inner) if inner.operator == Operator::Exponentiation
            ),
            "right of `+` should be the `^` subtree, got {:?}",
            op.operands[1]
        );
    }

    #[test]
    fn test_parse_exponentiation_left_associative() {
        // Grammar's `<arithmetic factor>` is left-recursive: `2 ^ 3 ^ 2`
        // parses as `(2 ^ 3) ^ 2` — the OUTER `^` has a `^` subtree on its LEFT.
        let (rem, expr) = parse_expression("2 ^ 3 ^ 2").unwrap();
        assert_eq!(rem, "");
        let Expression::OperatorApplicationExp(op) = expr else {
            panic!("Expected operator, got {:?}", expr);
        };
        assert_eq!(op.operator, Operator::Exponentiation);
        assert!(
            matches!(
                &op.operands[0],
                Expression::OperatorApplicationExp(inner) if inner.operator == Operator::Exponentiation
            ),
            "left-associative: outer `^` left operand should be a `^` subtree, got {:?}",
            op.operands[0]
        );
    }

    #[test]
    fn test_temporal_accessor_on_function_call() {
        // datetime('2013').year should parse as year(datetime('2013'))
        let input = "datetime('2013-01-01').year";
        let result = parse_expression(input);
        match result {
            Ok((remaining, expr)) => {
                assert_eq!(remaining, "");
                if let Expression::FunctionCallExp(fc) = &expr {
                    assert_eq!(fc.name, "year");
                    assert_eq!(fc.args.len(), 1);
                } else {
                    panic!("Expected FunctionCallExp(year), got {:?}", expr);
                }
            }
            Err(e) => panic!("Should parse datetime().year: {:?}", e),
        }
    }

    #[test]
    fn test_temporal_accessor_in_parenthesized_expr() {
        // (datetime('2013').year) should parse
        let input = "(datetime('2013-01-01').year)";
        let result = parse_expression(input);
        match result {
            Ok((remaining, expr)) => {
                assert_eq!(remaining, "");
                if let Expression::FunctionCallExp(fc) = &expr {
                    assert_eq!(fc.name, "year");
                } else {
                    panic!("Expected FunctionCallExp(year), got {:?}", expr);
                }
            }
            Err(e) => panic!("Should parse (datetime().year): {:?}", e),
        }
    }

    #[test]
    fn test_temporal_accessor_in_arithmetic() {
        // (datetime('2013').year - n.creationDate.year) should parse as subtraction
        let input = "(datetime('2013-01-01').year - n.creationDate.year)";
        let result = parse_expression(input);
        match result {
            Ok((remaining, expr)) => {
                assert_eq!(remaining, "");
                if let Expression::OperatorApplicationExp(op) = &expr {
                    assert_eq!(op.operator, Operator::Subtraction);
                } else {
                    panic!("Expected subtraction, got {:?}", expr);
                }
            }
            Err(e) => panic!("Should parse temporal arithmetic: {:?}", e),
        }
    }

    #[test]
    fn test_chained_comparison_desugaring() {
        // `a > b >= c` should desugar to `(a > b) AND (b >= c)`
        let input = "a > b >= c";
        let (remaining, expr) = parse_expression(input).unwrap();
        assert_eq!(remaining, "");
        if let Expression::OperatorApplicationExp(op) = &expr {
            assert_eq!(op.operator, Operator::And);
            assert_eq!(op.operands.len(), 2);
            // LHS: a > b
            if let Expression::OperatorApplicationExp(lhs) = &op.operands[0] {
                assert_eq!(lhs.operator, Operator::GreaterThan);
            } else {
                panic!("Expected LHS to be GreaterThan comparison");
            }
            // RHS: b >= c
            if let Expression::OperatorApplicationExp(rhs) = &op.operands[1] {
                assert_eq!(rhs.operator, Operator::GreaterThanEqual);
            } else {
                panic!("Expected RHS to be GreaterThanEqual comparison");
            }
        } else {
            panic!("Expected AND expression, got {:?}", expr);
        }
    }

    #[test]
    fn test_chained_comparison_with_properties() {
        // `$endDate > m.creationDate >= $startDate` — the LDBC complex-3 pattern
        let input = "$endDate > m.creationDate >= $startDate";
        let (remaining, expr) = parse_expression(input).unwrap();
        assert_eq!(remaining, "");
        if let Expression::OperatorApplicationExp(op) = &expr {
            assert_eq!(op.operator, Operator::And);
            assert_eq!(op.operands.len(), 2);
        } else {
            panic!("Expected AND expression, got {:?}", expr);
        }
    }

    #[test]
    fn test_non_chained_comparison_unchanged() {
        // `a > b` should stay as a simple comparison (no desugaring)
        let input = "a > b";
        let (remaining, expr) = parse_expression(input).unwrap();
        assert_eq!(remaining, "");
        if let Expression::OperatorApplicationExp(op) = &expr {
            assert_eq!(op.operator, Operator::GreaterThan);
            assert_eq!(op.operands.len(), 2);
        } else {
            panic!("Expected GreaterThan comparison, got {:?}", expr);
        }
    }

    #[test]
    fn test_not_equal_chains_1398() {
        // `a <> b <> c` chains like any comparison, as in Neo4j (`1 <> 2 <> 1`
        // is true): `a <> b AND b <> c` (#1398).
        let input = "a <> b <> c";
        let (remaining, expr) = parse_expression(input).unwrap();
        assert_eq!(remaining, "");
        let v = |n: &'static str| Expression::Variable(n.into());
        assert_eq!(
            expr,
            binary(
                Operator::And,
                binary(Operator::NotEqual, v("a"), v("b")),
                binary(Operator::NotEqual, v("b"), v("c")),
            )
        );
    }

    #[test]
    fn test_1355_parenthesized_comparison_equal_not_chained() {
        // #1355: `(a.age > 20) = (a.age > 28)` must parse as Equal(GT, GT),
        // not as chained `(a.age>20) AND (20 = (a.age>28))`.
        let input = "(a.age > 20) = (a.age > 28)";
        let (remaining, expr) = parse_expression(input).unwrap();
        assert_eq!(remaining, "");
        if let Expression::OperatorApplicationExp(op) = &expr {
            assert_eq!(op.operator, Operator::Equal);
            assert_eq!(op.operands.len(), 2);
            for side in &op.operands {
                if let Expression::OperatorApplicationExp(cmp) = side {
                    assert_eq!(cmp.operator, Operator::GreaterThan);
                } else {
                    panic!("Expected GreaterThan operand, got {:?}", side);
                }
            }
        } else {
            panic!("Expected Equal expression, got {:?}", expr);
        }
    }

    #[test]
    fn test_1355_parenthesized_lhs_scalar_rhs_not_chained() {
        // `(a > b) >= c` stays a plain comparison (LHS parenthesized).
        let input = "(a > b) >= c";
        let (remaining, expr) = parse_expression(input).unwrap();
        assert_eq!(remaining, "");
        if let Expression::OperatorApplicationExp(op) = &expr {
            assert_eq!(op.operator, Operator::GreaterThanEqual);
            if let Expression::OperatorApplicationExp(lhs) = &op.operands[0] {
                assert_eq!(lhs.operator, Operator::GreaterThan);
            } else {
                panic!("Expected GreaterThan LHS, got {:?}", expr);
            }
        } else {
            panic!("Expected GreaterThanEqual expression, got {:?}", expr);
        }
    }

    #[test]
    fn test_1355_bare_chain_parenthesized_rhs_still_chains() {
        // Review correction: in Cypher only a parenthesized LEFT operand
        // stops a chain. `a > b = (c > d)` chains to `a > b AND b = (c > d)`.
        let input = "a > b = (c > d)";
        let (remaining, expr) = parse_expression(input).unwrap();
        assert_eq!(remaining, "");
        if let Expression::OperatorApplicationExp(op) = &expr {
            assert_eq!(op.operator, Operator::And);
            assert_eq!(op.operands.len(), 2);
            // LHS: a > b
            if let Expression::OperatorApplicationExp(lhs) = &op.operands[0] {
                assert_eq!(lhs.operator, Operator::GreaterThan);
            } else {
                panic!("Expected GreaterThan LHS, got {:?}", expr);
            }
            // RHS: b = (c > d)
            if let Expression::OperatorApplicationExp(rhs) = &op.operands[1] {
                assert_eq!(rhs.operator, Operator::Equal);
                if let Expression::OperatorApplicationExp(inner) = &rhs.operands[1] {
                    assert_eq!(inner.operator, Operator::GreaterThan);
                } else {
                    panic!("Expected GreaterThan inner RHS, got {:?}", expr);
                }
            } else {
                panic!("Expected Equal RHS, got {:?}", expr);
            }
        } else {
            panic!("Expected And expression, got {:?}", expr);
        }
    }

    #[test]
    fn test_1355_oracle_chains_with_paren_rhs() {
        // Review oracle rows: these chain (bare lhs), evaluating to false.
        for input in ["3 > 2 = (1 > 0)", "2 = 2 = (1 = 1)"] {
            let (remaining, expr) = parse_expression(input).unwrap();
            assert_eq!(remaining, "", "input: {input}");
            assert!(
                matches!(
                    &expr,
                    Expression::OperatorApplicationExp(op) if op.operator == Operator::And
                ),
                "input {input} must chain to AND, got {:?}",
                expr
            );
        }
    }

    #[test]
    fn test_1355_boolean_literal_compared_to_paren_comparison() {
        // `true = (a.age > 28)` pins current-correct behavior.
        let input = "true = (a.age > 28)";
        let (remaining, expr) = parse_expression(input).unwrap();
        assert_eq!(remaining, "");
        if let Expression::OperatorApplicationExp(op) = &expr {
            assert_eq!(op.operator, Operator::Equal);
            if let Expression::OperatorApplicationExp(rhs) = &op.operands[1] {
                assert_eq!(rhs.operator, Operator::GreaterThan);
            } else {
                panic!("Expected GreaterThan RHS, got {:?}", expr);
            }
        } else {
            panic!("Expected Equal expression, got {:?}", expr);
        }
    }

    #[test]
    fn test_temporal_accessor_rejects_partial_identifiers() {
        // `yearly`, `months`, `year_start` are not the accessors `year` /
        // `month`: each is the property of that name (#1381: any value's).
        for key in ["yearly", "months", "year_start"] {
            let input = format!("datetime('2013-01-01').{key}");
            let (remaining, expr) = parse_expression(&input).unwrap();
            assert_eq!(remaining, "", "{input}");
            assert!(
                matches!(&expr, Expression::PropertyOf { key: k, .. } if k == key),
                "{input}: {expr:?}"
            );
        }
    }
    #[test]
    fn test_temporal_accessor_single_segment() {
        // birthday.month should parse as month(birthday)
        // This is critical for post-WITH temporal accessor resolution
        let input = "birthday.month";
        let result = parse_expression(input);
        match result {
            Ok((remaining, expr)) => {
                assert_eq!(remaining, "");
                if let Expression::FunctionCallExp(fc) = &expr {
                    assert_eq!(fc.name, "month");
                    assert_eq!(fc.args.len(), 1);
                    assert!(matches!(&fc.args[0], Expression::Variable(v) if v == "birthday"));
                } else {
                    panic!("Expected FunctionCallExp(month), got {:?}", expr);
                }
            }
            Err(e) => panic!("Should parse birthday.month as temporal accessor: {:?}", e),
        }

        // birthday.day should also work
        let input = "birthday.day";
        let result = parse_expression(input);
        match result {
            Ok((remaining, expr)) => {
                assert_eq!(remaining, "");
                if let Expression::FunctionCallExp(fc) = &expr {
                    assert_eq!(fc.name, "day");
                } else {
                    panic!("Expected FunctionCallExp(day), got {:?}", expr);
                }
            }
            Err(e) => panic!("Should parse birthday.day: {:?}", e),
        }
    }

    // ===== List Comprehension Parser Tests =====

    #[test]
    fn test_list_comprehension_identity() {
        // [x IN list] — identity, no WHERE or projection
        let input = "[x IN list]";
        let (remaining, expr) = parse_expression(input).unwrap();
        assert_eq!(remaining, "");
        if let Expression::ListComprehension(lc) = &expr {
            assert_eq!(lc.variable, "x");
            assert!(matches!(&*lc.list_expr, Expression::Variable(v) if v == "list"));
            assert!(lc.where_clause.is_none());
            assert!(lc.projection.is_none());
        } else {
            panic!("Expected ListComprehension, got {:?}", expr);
        }
    }

    #[test]
    fn test_list_comprehension_with_where() {
        // [x IN range(1, 10) WHERE x > 5]
        let input = "[x IN range(1, 10) WHERE x > 5]";
        let (remaining, expr) = parse_expression(input).unwrap();
        assert_eq!(remaining, "");
        if let Expression::ListComprehension(lc) = &expr {
            assert_eq!(lc.variable, "x");
            assert!(lc.where_clause.is_some());
            assert!(lc.projection.is_none());
        } else {
            panic!("Expected ListComprehension, got {:?}", expr);
        }
    }

    #[test]
    fn test_list_comprehension_with_projection() {
        // [x IN list | x * 2]
        let input = "[x IN list | x * 2]";
        let (remaining, expr) = parse_expression(input).unwrap();
        assert_eq!(remaining, "");
        if let Expression::ListComprehension(lc) = &expr {
            assert_eq!(lc.variable, "x");
            assert!(lc.where_clause.is_none());
            assert!(lc.projection.is_some());
        } else {
            panic!("Expected ListComprehension, got {:?}", expr);
        }
    }

    #[test]
    fn test_list_comprehension_where_and_projection() {
        // [x IN list WHERE x > 0 | x * 2]
        let input = "[x IN list WHERE x > 0 | x * 2]";
        let (remaining, expr) = parse_expression(input).unwrap();
        assert_eq!(remaining, "");
        if let Expression::ListComprehension(lc) = &expr {
            assert_eq!(lc.variable, "x");
            assert!(lc.where_clause.is_some());
            assert!(lc.projection.is_some());
        } else {
            panic!("Expected ListComprehension, got {:?}", expr);
        }
    }

    #[test]
    fn test_list_comprehension_pipe_without_spaces() {
        // `|` next to a name or a number, as Neo4j takes it
        for input in [
            "[x IN list|x * 2]",
            "[x IN [1, 2]|x]",
            "[x IN range(1, 3) WHERE x>1|x]",
        ] {
            let (remaining, expr) = parse_expression(input).unwrap();
            assert_eq!(remaining, "", "{input}");
            let Expression::ListComprehension(lc) = &expr else {
                panic!("{input}: expected ListComprehension, got {:?}", expr);
            };
            assert!(lc.projection.is_some(), "{input}");
        }
        // A keyword is still a word: `WHEREx` and `a_where_b` are names.
        let (remaining, expr) = parse_expression("[x IN a_where_b | x]").unwrap();
        assert_eq!(remaining, "");
        let Expression::ListComprehension(lc) = &expr else {
            panic!("expected ListComprehension, got {:?}", expr);
        };
        assert!(matches!(&*lc.list_expr, Expression::Variable(v) if v == "a_where_b"));
        // A name of other than ASCII letters is no stop either, and is read
        // without a panic (`i` inside a character).
        for input in ["[x IN léwhere | x]", "[x IN lé WHERE x > 0]"] {
            if let Ok((_, Expression::ListComprehension(lc))) = parse_expression(input) {
                assert!(lc.where_clause.is_none() || input.contains(" WHERE "));
            }
        }
        let (_, expr) = parse_expression("[x IN WHEREx]").unwrap();
        let Expression::ListComprehension(lc) = &expr else {
            panic!("expected ListComprehension, got {:?}", expr);
        };
        assert!(matches!(&*lc.list_expr, Expression::Variable(v) if v == "WHEREx"));
        assert!(lc.where_clause.is_none());
    }

    #[test]
    fn test_list_comprehension_with_pattern_where() {
        // [p IN posts WHERE (p)-[:HAS_TAG]->()]
        let input = "[p IN posts WHERE (p)-[:HAS_TAG]->()]";
        let (remaining, expr) = parse_expression(input).unwrap();
        assert_eq!(remaining, "");
        if let Expression::ListComprehension(lc) = &expr {
            assert_eq!(lc.variable, "p");
            assert!(matches!(&*lc.list_expr, Expression::Variable(v) if v == "posts"));
            assert!(lc.where_clause.is_some());
        } else {
            panic!("Expected ListComprehension, got {:?}", expr);
        }
    }

    #[test]
    fn test_list_comprehension_nested_brackets() {
        // [x IN [1, 2, 3] WHERE x > 1]
        let input = "[x IN [1, 2, 3] WHERE x > 1]";
        let (remaining, expr) = parse_expression(input).unwrap();
        assert_eq!(remaining, "");
        if let Expression::ListComprehension(lc) = &expr {
            assert_eq!(lc.variable, "x");
            assert!(matches!(&*lc.list_expr, Expression::List(_)));
            assert!(lc.where_clause.is_some());
        } else {
            panic!("Expected ListComprehension, got {:?}", expr);
        }
    }

    #[test]
    fn test_list_literal_not_misinterpreted() {
        // [1, 2, 3] must still parse as a list literal, not a list comprehension
        let input = "[1, 2, 3]";
        let (remaining, expr) = parse_expression(input).unwrap();
        assert_eq!(remaining, "");
        assert!(
            matches!(&expr, Expression::List(_)),
            "Expected List literal, got {:?}",
            expr
        );
    }

    #[test]
    fn test_list_comprehension_size_wrapper() {
        // size([p IN posts WHERE (p)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)])
        let input = "size([p IN posts WHERE (p)-[:HAS_TAG]->()<-[:HAS_INTEREST]-(person)])";
        let (remaining, expr) = parse_expression(input).unwrap();
        assert_eq!(remaining, "");
        if let Expression::FunctionCallExp(fc) = &expr {
            assert_eq!(fc.name.to_lowercase(), "size");
            assert_eq!(fc.args.len(), 1);
            assert!(matches!(&fc.args[0], Expression::ListComprehension(_)));
        } else {
            panic!("Expected FunctionCallExp(size), got {:?}", expr);
        }
    }

    #[test]
    fn test_list_comprehension_string_in_where() {
        // [x IN list WHERE x.name = "WHERE test"]
        // The "WHERE" inside the string must not be treated as a keyword stop
        let input = r#"[x IN list WHERE x.name = "WHERE test"]"#;
        let (remaining, expr) = parse_expression(input).unwrap();
        assert_eq!(remaining, "");
        if let Expression::ListComprehension(lc) = &expr {
            assert_eq!(lc.variable, "x");
            assert!(lc.where_clause.is_some());
        } else {
            panic!("Expected ListComprehension, got {:?}", expr);
        }
    }

    /// Escaped names in expressions: map keys (#1421), parameters, lambda
    /// parameters, label tests, comprehension variables (#1381), the
    /// keyword literals; a doubled backtick is one (#1489).
    #[test]
    fn escaped_names_in_expressions_1489_1421() {
        let (rem, e) = parse_expression("{`it's`: 1, `a``b`: 2, c: 3}").unwrap();
        assert_eq!(rem, "");
        let Expression::MapLiteral(entries) = e else {
            panic!("{e:?}");
        };
        let keys: Vec<&str> = entries.iter().map(|(k, _)| &**k).collect();
        assert_eq!(keys, ["it's", "a`b", "c"]);
        assert!(matches!(entries[1].0, Name::Owned(_)));

        let (_, e) = parse_expression("$`my param`").unwrap();
        assert_eq!(e, Expression::Parameter("my param".into()));
        let (_, e) = parse_expression("$0").unwrap();
        assert_eq!(e, Expression::Parameter("0".into()));

        let (_, e) = parse_expression("`n x`:`My Label`").unwrap();
        assert_eq!(
            e,
            Expression::LabelExpression {
                variable: "n x".into(),
                label: "My Label".into(),
            }
        );

        let (rem, e) = parse_expression("[`x]y` IN l WHERE `x]y` > 1 | `x]y`]").unwrap();
        assert_eq!(rem, "");
        let Expression::ListComprehension(lc) = e else {
            panic!("{e:?}");
        };
        assert_eq!(lc.variable, "x]y");
        assert_eq!(*lc.list_expr, Expression::Variable("l".into()));
        assert_eq!(
            lc.projection.as_deref(),
            Some(&Expression::Variable("x]y".into()))
        );

        let (rem, e) = parse_expression("reduce(`a c` = 0, `x|` IN l | `a c` + `x|`)").unwrap();
        assert_eq!(rem, "");
        let Expression::ReduceExp(r) = e else {
            panic!("{e:?}");
        };
        assert_eq!((&*r.accumulator, &*r.variable), ("a c", "x|"));

        // Escaped, a keyword literal is a variable; a binary operator
        // keyword is a name.
        for (input, name) in [("`true`", "true"), ("`NULL`", "NULL"), ("`AND`", "AND")] {
            assert_eq!(
                parse_expression(input).unwrap().1,
                Expression::Variable(name.into()),
                "{input}"
            );
        }
        assert_eq!(
            parse_expression("true").unwrap().1,
            Expression::Literal(Literal::Boolean(true))
        );
        // `` `*` `` would read as the star of `count(*)`: no parse.
        assert!(parse_expression("`*`").is_err());
        assert!(!matches!(parse_expression("a.`*`"), Ok(("", _))));
    }

    /// A string, or an escaped name, holding a `]`, `|`, `)` or `WHERE`
    /// ends no comprehension, reduce or pattern comprehension (#1489).
    #[test]
    fn brackets_inside_strings_and_names_end_nothing_1489() {
        for input in [
            "[x IN ['a'] | x + ']']",
            "[x IN ['a'] WHERE x <> '|' | x + ' WHERE ']",
            "reduce(s = '', x IN ['a'] | s + ')' + x)",
            "[(a)-[:R]->(c) | c.name + ']']",
            "[(a)-[:R]->(c) WHERE c.`x)` = ')' | c.`y]`]",
            "[`a]` IN [1] | `a]`]",
        ] {
            let (rem, _) = parse_expression(input).unwrap_or_else(|e| panic!("{input}: {e:?}"));
            assert_eq!(rem, "", "{input}");
        }
    }

    /// A function's name is letters, digits and `_`, escaped or not: an
    /// unknown function is called by its name in the SQL (#1489).
    #[test]
    fn escaped_function_names_1489() {
        let (_, e) = parse_expression("`toUpper`(x)").unwrap();
        assert!(matches!(e, Expression::FunctionCallExp(ref f) if f.name == "toUpper"));
        let (_, e) = parse_expression("ch.`arrayFilter`(x)").unwrap();
        assert!(matches!(e, Expression::FunctionCallExp(ref f) if f.name == "ch.arrayFilter"));
        for input in ["`foo bar`(x)", "`a.b`(x)", "`f;`(x)"] {
            assert!(!matches!(parse_expression(input), Ok(("", _))), "{input}");
        }
    }

    /// The operator levels follow Neo4j 5 (#1398): each input parses as the
    /// fully parenthesized one.
    #[test]
    fn operator_precedence_follows_neo4j_1398() {
        for (input, grouped) in [
            // A predicate binds tighter than a comparison.
            ("true = x IN l", "true = (x IN l)"),
            ("x IN l = true", "(x IN l) = true"),
            ("false = 1 IS NULL", "false = (1 IS NULL)"),
            ("a = b STARTS WITH c", "a = (b STARTS WITH c)"),
            ("a =~ b = c", "(a =~ b) = c"),
            // The predicate's operand is additive.
            ("1 + 2 IS NULL", "(1 + 2) IS NULL"),
            ("a + b IN l", "(a + b) IN l"),
            ("a * b CONTAINS c", "(a * b) CONTAINS c"),
            // NOT is looser than the comparisons, tighter than AND.
            ("NOT a = b", "NOT (a = b)"),
            ("NOT a IS NULL", "NOT (a IS NULL)"),
            ("NOT a AND b", "(NOT a) AND b"),
            ("(NOT false) = x", "(NOT false) = x"),
            // XOR between AND and OR.
            ("a OR b XOR c", "a OR (b XOR c)"),
            ("a XOR b AND c", "a XOR (b AND c)"),
            ("a XOR b XOR c", "(a XOR b) XOR c"),
            ("NOT a XOR b", "(NOT a) XOR b"),
            // Arithmetic.
            ("1 + 2 * 3 ^ 2", "1 + (2 * (3 ^ 2))"),
            ("2 ^ 3 ^ 2", "(2 ^ 3) ^ 2"),
            ("a - b - c", "(a - b) - c"),
            // Postfix binds tightest.
            ("-x.a", "-(x.a)"),
            ("x[0] IN l", "(x[0]) IN l"),
        ] {
            let (rem, got) = parse_expression(input).unwrap_or_else(|e| panic!("{input}: {e:?}"));
            assert_eq!(rem, "", "{input}");
            let (rem, want) = parse_expression(grouped).unwrap();
            assert_eq!(rem, "", "{grouped}");
            assert_eq!(got, want, "{input} should be {grouped}");
        }
    }

    /// A comparison chain of any length, `<>` included, is the conjunction of
    /// its adjacent comparisons; a parenthesized operand is an operand
    /// (#1398, #1355).
    #[test]
    fn comparison_chains_1398() {
        for (input, conjunction) in [
            ("a < b < c", "(a < b) AND (b < c)"),
            ("a <> b <> c", "(a <> b) AND (b <> c)"),
            ("a < b < c < b", "((a < b) AND (b < c)) AND (c < b)"),
            ("a = b = c", "(a = b) AND (b = c)"),
            ("a >= b > c <= d", "((a >= b) AND (b > c)) AND (c <= d)"),
            ("x < 30 < (40)", "(x < 30) AND (30 < (40))"),
            ("a > b = (c > d)", "(a > b) AND (b = (c > d))"),
            ("a = b IN l = c", "(a = (b IN l)) AND ((b IN l) = c)"),
        ] {
            let (rem, got) = parse_expression(input).unwrap_or_else(|e| panic!("{input}: {e:?}"));
            assert_eq!(rem, "", "{input}");
            assert_eq!(got, parse_expression(conjunction).unwrap().1, "{input}");
        }
        // A parenthesized comparison on the left is compared, not chained.
        let (_, got) = parse_expression("(a < b) < c").unwrap();
        let Expression::OperatorApplicationExp(op) = &got else {
            panic!("{got:?}");
        };
        assert_eq!(op.operator, Operator::LessThan);
    }

    /// A predicate does not repeat, as in Neo4j (`x IN l IN m` is a syntax
    /// error there): the second is left unparsed.
    #[test]
    fn predicates_do_not_repeat_1398() {
        for (input, rest) in [
            ("x IN l IN m", "IN m"),
            ("a IS NULL IS NULL", "IS NULL"),
            ("s STARTS WITH 'a' IS NULL", "IS NULL"),
            ("s =~ 'a' =~ 'b'", "=~ 'b'"),
        ] {
            let (rem, _) = parse_expression(input).unwrap();
            assert_eq!(rem.trim(), rest, "{input}");
        }
    }

    /// Keywords are words: `nothing` is no `NOT`, `order` no `OR`, `android`
    /// no `AND`, `index` no `IN`; and a lone node in an expression is a
    /// parenthesized expression, not a pattern (#1398).
    #[test]
    fn keywords_are_words_and_a_lone_node_is_parenthesized_1398() {
        for (input, name) in [
            ("nothing", "nothing"),
            ("notable", "notable"),
            ("android", "android"),
            ("orders", "orders"),
            ("index", "index"),
            ("isnull", "isnull"),
            ("xorx", "xorx"),
            ("cases", "cases"),
        ] {
            assert_eq!(
                parse_expression(input).unwrap(),
                ("", Expression::Variable(name.into())),
                "{input}"
            );
        }
        let (rem, e) = parse_expression("a ANDROID").unwrap();
        assert_eq!(
            (rem.trim(), e),
            ("ANDROID", Expression::Variable("a".into()))
        );
        // `(a)` is the variable, `(a:L)` the label test.
        assert_eq!(
            parse_expression("(a)").unwrap().1,
            Expression::Variable("a".into())
        );
        assert!(matches!(
            parse_expression("(a:L)").unwrap().1,
            Expression::LabelExpression { .. }
        ));
        // A relationship pattern is still a pattern.
        assert!(matches!(
            parse_expression("(a)-[:R]->()").unwrap().1,
            Expression::PathPattern(PathPattern::ConnectedPattern(_))
        ));
        // CASE / WHEN in any case.
        let (rem, e) = parse_expression("CASE When x THEN 1 eLsE 2 End").unwrap();
        assert_eq!(rem, "");
        assert!(matches!(e, Expression::Case(ref c) if c.expr.is_none()));
    }

    /// `all` / `any` / `none` / `single` (#1420): read as their three-valued
    /// CASE over the sizes of list comprehensions; WHERE is required, and a
    /// predicate without one is an error, no call of a function `any`.
    #[test]
    fn list_predicates_1420() {
        let (rem, got) = parse_expression("any(x IN l WHERE x > 1)").unwrap();
        assert_eq!(rem, "");
        let (_, want) = parse_expression(
            "CASE WHEN size([x IN l WHERE x > 1]) IS NULL THEN null \
             WHEN size([x IN l WHERE x > 1]) > 0 THEN true \
             WHEN size([x IN l WHERE (x > 1) IS NULL]) > 0 THEN null ELSE false END",
        )
        .unwrap();
        assert_eq!(got, want);
        for (input, form) in [
            (
                "all(x IN l WHERE p)",
                "CASE WHEN size([x IN l WHERE p]) IS NULL THEN null \
                 WHEN size([x IN l WHERE NOT p]) > 0 THEN false \
                 WHEN size([x IN l WHERE p IS NULL]) > 0 THEN null ELSE true END",
            ),
            (
                "NONE(x IN l WHERE p)",
                "CASE WHEN size([x IN l WHERE p]) IS NULL THEN null \
                 WHEN size([x IN l WHERE p]) > 0 THEN false \
                 WHEN size([x IN l WHERE p IS NULL]) > 0 THEN null ELSE true END",
            ),
            (
                "single(`x y` IN l WHERE `x y`.a)",
                "CASE WHEN size([`x y` IN l WHERE `x y`.a]) IS NULL THEN null \
                 WHEN size([`x y` IN l WHERE `x y`.a]) > 1 \
                 THEN false WHEN size([`x y` IN l WHERE `x y`.a IS NULL]) > 0 THEN null \
                 ELSE size([`x y` IN l WHERE `x y`.a]) = 1 END",
            ),
        ] {
            let (rem, got) = parse_expression(input).unwrap_or_else(|e| panic!("{input}: {e:?}"));
            assert_eq!(rem, "", "{input}");
            assert_eq!(got, parse_expression(form).unwrap().1, "{input}");
        }
        // Without WHERE: an error (Neo4j: "any(...) requires a WHERE predicate").
        assert!(matches!(
            parse_expression("any(x IN [true, false])"),
            Err(nom::Err::Failure(_))
        ));
        // Nor is a call of a function of that name (Neo4j has none).
        for input in ["all(x)", "single([true, true])", "none([true])"] {
            assert!(
                matches!(parse_expression(input), Err(nom::Err::Failure(_))),
                "{input}"
            );
        }
        // A variable named so is a variable.
        assert_eq!(
            parse_expression("single").unwrap().1,
            Expression::Variable("single".into())
        );
    }
}
