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

/// Strip SQL-style comments from input before parsing
/// This handles both line comments (--) and block comments (/* */)
/// Respects string literals and identifiers - comments inside them are preserved
///
/// Cypher quote types:
/// - Single quotes ('): String literals
/// - Double quotes ("): Identifiers (property names, labels)
/// - Backticks (`): Identifiers (Neo4j style)
pub fn strip_comments(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    let mut in_string: Option<char> = None; // Track if we're in a string/identifier (and which quote type)
    let mut escape_next = false;

    while let Some(ch) = chars.next() {
        // Handle escape sequences in strings/identifiers
        if escape_next {
            result.push(ch);
            escape_next = false;
            continue;
        }

        // Check for escape character in strings/identifiers
        if in_string.is_some() && ch == '\\' {
            result.push(ch);
            escape_next = true;
            continue;
        }

        // Track string literal and identifier boundaries
        if ch == '\'' || ch == '"' || ch == '`' {
            if in_string == Some(ch) {
                // End of string/identifier
                in_string = None;
            } else if in_string.is_none() {
                // Start of string/identifier
                in_string = Some(ch);
            }
            result.push(ch);
            continue;
        }

        // If we're inside a string/identifier, preserve everything
        if in_string.is_some() {
            result.push(ch);
            continue;
        }

        // Now handle comments (only when NOT in a string/identifier)

        // Check for line comment (-- or //)
        if ch == '-' && chars.peek() == Some(&'-') {
            chars.next(); // consume second '-'
                          // Skip until newline
            while let Some(c) = chars.next() {
                if c == '\n' {
                    result.push('\n'); // preserve newline
                    break;
                }
            }
            continue;
        }

        // Check for block comment or line comment
        if ch == '/' {
            match chars.peek() {
                Some(&'*') => {
                    // Block comment /* */
                    chars.next(); // consume '*'
                                  // Skip until */
                    let mut found_end = false;
                    while let Some(c) = chars.next() {
                        if c == '*' && chars.peek() == Some(&'/') {
                            chars.next(); // consume '/'
                            found_end = true;
                            break;
                        }
                    }
                    if !found_end {
                        // Unclosed block comment - just continue
                    }
                    continue;
                }
                Some(&'/') => {
                    // Line comment //
                    chars.next(); // consume second '/'
                                  // Skip until newline
                    while let Some(c) = chars.next() {
                        if c == '\n' {
                            result.push('\n'); // preserve newline
                            break;
                        }
                    }
                    continue;
                }
                _ => {
                    // Just a regular '/' character
                }
            }
        }

        result.push(ch);
    }

    result
}

/// Whitespace-handling combinator (original version, no comment parsing)
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

/// Parse a numeric literal (integer or float with optional scientific notation)
/// Matches: 123, -123, 3.14, -3.14, .5, -.5, 1.5e10, -2.3e-5, 1e10
fn parse_numeric_literal(input: &str) -> IResult<&str, &str> {
    recognize(pair(
        opt(char('-')),
        pair(
            alt((
                // Float with integer part: 123.456 (must have digits after dot)
                recognize((digit1, char('.'), digit1)),
                // Float without integer part: .456
                recognize(pair(char('.'), digit1)),
                // Integer: 123 (no dot allowed - checked after float patterns)
                digit1,
            )),
            // Optional scientific notation: e10, e-5, E+3
            opt(recognize(pair(
                alt((char('e'), char('E'))),
                pair(opt(alt((char('+'), char('-')))), digit1),
            ))),
        ),
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

    #[test]
    fn test_strip_comments() {
        use super::strip_comments;

        // Line comments (-- style)
        assert_eq!(strip_comments("-- Comment\nMATCH"), "\nMATCH");

        // Line comments (// style - Cypher standard)
        assert_eq!(strip_comments("// Comment\nMATCH"), "\nMATCH");

        assert_eq!(
            strip_comments("// LDBC Query\n// Description\n\nMATCH (n) RETURN n"),
            "\n\n\nMATCH (n) RETURN n"
        );

        assert_eq!(
            strip_comments("-- LDBC Query\n-- Description\n\nMATCH (n) RETURN n"),
            "\n\n\nMATCH (n) RETURN n"
        );

        // Block comments
        assert_eq!(strip_comments("/* Comment */MATCH"), "MATCH");

        assert_eq!(strip_comments("/* Multi\nline\ncomment */MATCH"), "MATCH");

        // Mixed comments
        assert_eq!(strip_comments("-- Line\n/* Block */ MATCH"), "\n MATCH");

        assert_eq!(strip_comments("// Line\n/* Block */ MATCH"), "\n MATCH");

        // String literals with comment-like content (should NOT be stripped)
        assert_eq!(
            strip_comments("MATCH (n) WHERE n.url = 'http://test--page' RETURN n"),
            "MATCH (n) WHERE n.url = 'http://test--page' RETURN n"
        );

        assert_eq!(
            strip_comments("MATCH (n) WHERE n.note = \"test /* not a comment */ end\" RETURN n"),
            "MATCH (n) WHERE n.note = \"test /* not a comment */ end\" RETURN n"
        );

        // Backtick identifiers (Neo4j style)
        assert_eq!(
            strip_comments("MATCH (n:`Some--Label`) WHERE n.`prop--name` = 1 RETURN n"),
            "MATCH (n:`Some--Label`) WHERE n.`prop--name` = 1 RETURN n"
        );

        // Mixed: real comments + string literals
        assert_eq!(
            strip_comments(
                "-- Comment\nMATCH (n) WHERE n.url = 'test--value' -- another comment\nRETURN n"
            ),
            "\nMATCH (n) WHERE n.url = 'test--value' \nRETURN n"
        );

        // Escaped quotes in strings
        assert_eq!(
            strip_comments("WHERE n.text = 'it\\'s -- not a comment' RETURN n"),
            "WHERE n.text = 'it\\'s -- not a comment' RETURN n"
        );

        assert_eq!(
            strip_comments("WHERE n.text = \"test \\\"--\\\" value\" RETURN n"),
            "WHERE n.text = \"test \\\"--\\\" value\" RETURN n"
        );

        // Backticks with dashes
        assert_eq!(
            strip_comments("WHERE n.`prop-with-dashes` = 1 RETURN n"),
            "WHERE n.`prop-with-dashes` = 1 RETURN n"
        );
    }
}
