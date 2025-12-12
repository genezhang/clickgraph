use nom::{
    branch::alt,
    bytes::complete::{tag, take_until, take_while1},
    character::complete::{alphanumeric1, digit1, multispace0},
    combinator::{opt, recognize},
    error::ParseError,
    multi::many0,
    sequence::{delimited, pair},
    IResult, Parser,
};

use nom::character::complete::char;

pub fn ws<'a, O, E: ParseError<&'a str>, F>(inner: F) -> impl Parser<&'a str, Output = O, Error = E>
where
    F: Parser<&'a str, Output = O, Error = E>,
{
    delimited(multispace0, inner, multispace0)
}

// This parsed multuple dots as well. Keep it for now here
// pub fn parse_alphanumeric_with_underscore_dot_star(input: &str) -> IResult<&str, &str> {

//     alt((
//         // Single-quoted string: returns the inner content.
//         recognize((char('\''), take_until("\'"), char('\''))),
//         // Double-quoted string.
//         recognize((char('"'), take_until("\""), char('"'))),
//         // The star token, e.g. COUNT(*)
//         tag("*"),
//         // Otherwise, the usual unquoted identifier pattern.
//         recognize((
//             many0(tag("$")),
//             alphanumeric1,        // Must start with alphanumeric.
//             many0(tag("_")),      // Allow underscores.
//             many0(tag(".")),      // Allow dots.
//             many0(alphanumeric1), // And more alphanumerics.
//         )),
//     ))
//     .parse(input)
// }

// one or more alphanumerics followed by zero or more occurrences of an underscore and more alphanumerics.
// e.g., it will match "account", "creation", or "foo_bar".
fn identifier_core(input: &str) -> IResult<&str, &str> {
    recognize(pair(alphanumeric1, many0(pair(tag("_"), alphanumeric1)))).parse(input)
}

// identifier with optional leading '$'s and at most one dot.
// e.g. "$$foo_bar" (without any dot), or "$foo_bar.baz" (with exactly one dot)
fn unquoted_identifier(input: &str) -> IResult<&str, &str> {
    recognize(pair(
        many0(tag("$")),
        pair(
            identifier_core,
            // Optionally match a dot and a second identifier_core.
            opt(pair(tag("."), identifier_core)),
        ),
    ))
    .parse(input)
}

/// Parse a numeric literal (integer or float)
/// Matches: 123, -123, 3.14, -3.14, .5, -.5
fn parse_numeric_literal(input: &str) -> IResult<&str, &str> {
    recognize(pair(
        opt(char('-')),
        alt((
            // Float with integer part: 123.456 (must have digits after dot)
            recognize((digit1, char('.'), digit1)),
            // Float without integer part: .456
            recognize(pair(char('.'), digit1)),
            // Integer: 123 (no dot allowed - checked after float patterns)
            digit1,
        )),
    ))
    .parse(input)
}

pub fn parse_alphanumeric_with_underscore_dot_star(input: &str) -> IResult<&str, &str> {
    alt((
        // Single-quoted string: returns the whole thing including quotes.
        recognize(pair(char('\''), pair(take_until("'"), char('\'')))),
        // Double-quoted string.
        recognize(pair(char('"'), pair(take_until("\""), char('"')))),
        // The star token, e.g. COUNT(*)
        tag("*"),
        // Numeric literals: 123, -456, 3.14, -0.5
        parse_numeric_literal,
        // Unquoted identifier pattern.
        unquoted_identifier,
    ))
    .parse(input)
}

fn underscore1(input: &str) -> IResult<&str, &str> {
    take_while1(|c| c == '_')(input)
}

pub fn parse_alphanumeric_with_underscore(input: &str) -> IResult<&str, &str> {
    // recognize((
    //     alphanumeric1,        // First part must be alphanumeric
    //     many0(is_a("_")),     // Allow multiple underscores anywhere
    //     many0(alphanumeric1), // Allow more alphanumeric parts
    // ))
    // .parse(input)

    recognize(pair(alphanumeric1, many0(pair(underscore1, alphanumeric1)))).parse(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom::bytes::complete::tag;

    #[test]
    fn test_ws() {
        // both leading and trailing whitespace.
        assert_eq!(
            ws(tag::<&str, &str, nom::error::Error<&str>>("test")).parse("   test   "),
            Ok(("", "test"))
        );
        // only leading whitespace.
        assert_eq!(
            ws(tag::<&str, &str, nom::error::Error<&str>>("test")).parse("   test"),
            Ok(("", "test"))
        );
        // only trailing whitespace.
        assert_eq!(
            ws(tag::<&str, &str, nom::error::Error<&str>>("test")).parse("test   "),
            Ok(("", "test"))
        );
        // no whitespace.
        assert_eq!(
            ws(tag::<&str, &str, nom::error::Error<&str>>("test")).parse("test"),
            Ok(("", "test"))
        );
    }

    #[test]
    fn test_parse_alphanumeric_with_underscore_dot_star() {
        // single-quoted string input.
        assert_eq!(
            parse_alphanumeric_with_underscore_dot_star("'hello'"),
            Ok(("", "'hello'"))
        );
        // double-quoted string input.
        assert_eq!(
            parse_alphanumeric_with_underscore_dot_star("\"world\""),
            Ok(("", "\"world\""))
        );
        // the star token.
        assert_eq!(
            parse_alphanumeric_with_underscore_dot_star("*"),
            Ok(("", "*"))
        );
        // a simple unquoted identifier.
        assert_eq!(
            parse_alphanumeric_with_underscore_dot_star("abc.def.a"),
            Ok((".a", "abc.def"))
        );
        // when extra characters follow a valid identifier.
        assert_eq!(
            parse_alphanumeric_with_underscore_dot_star("abc123!"),
            Ok(("!", "abc123"))
        );
        // a failure case (input not matching any pattern).
        assert!(parse_alphanumeric_with_underscore_dot_star("!abc").is_err());

        // Numeric literals
        assert_eq!(
            parse_alphanumeric_with_underscore_dot_star("123"),
            Ok(("", "123"))
        );
        assert_eq!(
            parse_alphanumeric_with_underscore_dot_star("-456"),
            Ok(("", "-456"))
        );
        assert_eq!(
            parse_alphanumeric_with_underscore_dot_star("3.14"),
            Ok(("", "3.14"))
        );
        assert_eq!(
            parse_alphanumeric_with_underscore_dot_star("-0.5"),
            Ok(("", "-0.5"))
        );
    }

    #[test]
    fn test_parse_alphanumeric_with_underscore() {
        // Valid identifiers.
        assert_eq!(parse_alphanumeric_with_underscore("abc"), Ok(("", "abc")));
        assert_eq!(
            parse_alphanumeric_with_underscore("abc_def"),
            Ok(("", "abc_def"))
        );
        assert_eq!(
            parse_alphanumeric_with_underscore("abc___def"),
            Ok(("", "abc___def"))
        );
        // starting with digits.
        assert_eq!(
            parse_alphanumeric_with_underscore("123abc"),
            Ok(("", "123abc"))
        );
        // with trailing digits.
        assert_eq!(
            parse_alphanumeric_with_underscore("account_creation_date"),
            Ok(("", "account_creation_date"))
        );
        // with a mix of letters and numbers.
        assert_eq!(parse_alphanumeric_with_underscore("A1B2"), Ok(("", "A1B2")));
        // failure: starting with an underscore should be rejected.
        assert!(parse_alphanumeric_with_underscore("_abc").is_err());
    }
}
