use nom::{
    branch::alt,
    character::complete::{digit1, multispace0},
    combinator::{opt, recognize},
    error::ParseError,
    sequence::{delimited, pair},
    IResult, Parser,
};

use nom::character::complete::char;
use std::borrow::Cow;

use super::ast::Name;

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

        // Check for escape character in strings (a backtick-quoted name reads
        // no backslash escapes: `a\` is the name `a\`)
        if matches!(in_string, Some('\'' | '"')) && ch == '\\' {
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
        // BUT: `-->`, `<--`, `--(`, and `--[` are NOT comments (they're Cypher relationship patterns!)
        if ch == '-' && chars.peek() == Some(&'-') {
            // Peek ahead to check if this is a relationship pattern, not a comment
            let mut lookahead = chars.clone();
            lookahead.next(); // skip second '-'

            // Check for `-->` (outgoing relationship)
            if lookahead.peek() == Some(&'>') {
                result.push(ch);
                continue;
            }

            // Check for `--(` (undirected relationship to a node)
            if lookahead.peek() == Some(&'(') {
                result.push(ch);
                continue;
            }

            // Check for `--[` (undirected relationship with properties/types)
            if lookahead.peek() == Some(&'[') {
                result.push(ch);
                continue;
            }

            // Check if previous character was `<` (making this `<--` incoming relationship)
            // We already pushed the `<`, so check the last char in result
            if result.ends_with('<') {
                result.push(ch);
                continue;
            }

            // It's actually a `--` comment
            chars.next(); // consume second '-'
                          // Skip until newline
            for c in chars.by_ref() {
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
                    for c in chars.by_ref() {
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

/// A symbolic name (Neo4j 5's `symbolicNameString`), used at every place a
/// query names something: variables, labels, relationship types, property
/// and map keys, aliases, parameters, function names.
///
/// * Unescaped: a letter or `_`, then letters, digits, `_` and `$`
///   (`a$b` is a name in Neo4j). Keywords are names too where the grammar
///   expects a name; the callers that read an expression keep their keyword
///   checks.
/// * Escaped: any characters between backticks, a doubled backtick standing
///   for one (`` `it``s` `` is the name ``it`s``). Neo4j reads no backslash
///   escapes in it (`` `a\nb` `` is the four characters `a\nb`), and an empty
///   escaped name is a name.
///
/// The name borrows from the input, except for an escaped name that holds a
/// doubled backtick.
pub fn symbolic_name<'a, E: ParseError<&'a str>>(input: &'a str) -> IResult<&'a str, Name<'a>, E> {
    let Some(body) = input.strip_prefix('`') else {
        return unescaped_symbolic_name(input).map(|(rest, s)| (rest, Cow::Borrowed(s)));
    };
    let mut from = 0;
    let mut doubled = false;
    while let Some(i) = body[from..].find('`').map(|i| from + i) {
        if body[i + 1..].starts_with('`') {
            doubled = true;
            from = i + 2;
            continue;
        }
        let text = &body[..i];
        let name = if doubled {
            Cow::Owned(text.replace("``", "`"))
        } else {
            Cow::Borrowed(text)
        };
        return Ok((&body[i + 1..], name));
    }
    // An unterminated escaped name.
    Err(nom::Err::Error(E::from_error_kind(
        input,
        nom::error::ErrorKind::Char,
    )))
}

/// Is the symbolic name at the start of `input` an escaped one?
pub fn is_escaped_name(input: &str) -> bool {
    input.starts_with('`')
}

/// An unescaped symbolic name (see [`symbolic_name`]).
fn unescaped_symbolic_name<'a, E: ParseError<&'a str>>(
    input: &'a str,
) -> IResult<&'a str, &'a str, E> {
    let mut chars = input.char_indices();
    match chars.next() {
        Some((_, c)) if c.is_alphabetic() || c == '_' => {}
        _ => {
            return Err(nom::Err::Error(E::from_error_kind(
                input,
                nom::error::ErrorKind::Alpha,
            )))
        }
    }
    let end = chars
        .find(|(_, c)| !(c.is_alphanumeric() || *c == '_' || *c == '$'))
        .map_or(input.len(), |(i, _)| i);
    Ok((&input[end..], &input[..end]))
}

/// Parse a numeric literal (integer or float with optional scientific notation)
/// Matches: 123, -123, 3.14, -3.14, .5, -.5, 1.5e10, -2.3e-5, 1e10
pub(super) fn parse_numeric_literal(input: &str) -> IResult<&str, &str> {
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
    fn test_parse_numeric_literal() {
        assert_eq!(parse_numeric_literal("123"), Ok(("", "123")));
        assert_eq!(parse_numeric_literal("-456"), Ok(("", "-456")));
        assert_eq!(parse_numeric_literal("3.14"), Ok(("", "3.14")));
        assert_eq!(parse_numeric_literal("-0.5"), Ok(("", "-0.5")));
        assert_eq!(parse_numeric_literal("1e10 x"), Ok((" x", "1e10")));
        // `1..3` is a range: the integer only.
        assert_eq!(parse_numeric_literal("1..3"), Ok(("..3", "1")));
        assert!(parse_numeric_literal("abc").is_err());
    }

    fn name(input: &str) -> IResult<&str, Name<'_>> {
        symbolic_name(input)
    }

    #[test]
    fn symbolic_name_unescaped_1489() {
        for (input, rest, text) in [
            ("abc", "", "abc"),
            ("abc_def rest", " rest", "abc_def"),
            ("_name", "", "_name"),
            ("__expand", "", "__expand"),
            ("a1B2", "", "a1B2"),
            ("a$b", "", "a$b"),
            ("é_x", "", "é_x"),
            ("abc123!", "!", "abc123"),
            ("abc.def", ".def", "abc"),
        ] {
            let (r, n) = name(input).unwrap_or_else(|e| panic!("{input}: {e:?}"));
            assert_eq!((r, &*n), (rest, text), "{input}");
            assert!(matches!(n, Cow::Borrowed(_)), "{input}");
        }
        // A name does not start with a digit, `$` or a symbol.
        for input in ["123abc", "$x", "!abc", "", " a"] {
            assert!(name(input).is_err(), "{input}");
        }
    }

    #[test]
    fn symbolic_name_escaped_1489() {
        for (input, rest, text) in [
            ("`my node` x", " x", "my node"),
            ("`q?`", "", "q?"),
            ("`q.x`.y", ".y", "q.x"),
            ("`it's`", "", "it's"),
            ("`a]b|c WHERE`", "", "a]b|c WHERE"),
            // No backslash escapes: `a\nb` is four characters.
            (r"`a\nb`", "", r"a\nb"),
            (r"`a\`", "", r"a\"),
            // The empty name.
            ("`` AS x", " AS x", ""),
        ] {
            let (r, n) = name(input).unwrap_or_else(|e| panic!("{input}: {e:?}"));
            assert_eq!((r, &*n), (rest, text), "{input}");
            assert!(matches!(n, Cow::Borrowed(_)), "{input}");
        }
        // A doubled backtick is one: the name is unescaped, not a slice.
        for (input, rest, text) in [
            ("`a``b`", "", "a`b"),
            ("`````` x", " x", "``"),
            ("`a````b```", "", "a``b`"),
        ] {
            let (r, n) = name(input).unwrap_or_else(|e| panic!("{input}: {e:?}"));
            assert_eq!((r, &*n), (rest, text), "{input}");
            assert!(matches!(n, Cow::Owned(_)), "{input}");
        }
        // Unterminated.
        for input in ["`abc", "`a``", "`"] {
            assert!(name(input).is_err(), "{input}");
        }
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

        // Relationship patterns (should NOT be treated as comments!)
        assert_eq!(
            strip_comments("MATCH p=()-->() RETURN p"),
            "MATCH p=()-->() RETURN p"
        );

        assert_eq!(
            strip_comments("MATCH p=()<--() RETURN p"),
            "MATCH p=()<--() RETURN p"
        );

        assert_eq!(
            strip_comments("MATCH (a)-->(b)-->(c) RETURN a"),
            "MATCH (a)-->(b)-->(c) RETURN a"
        );

        assert_eq!(
            strip_comments("MATCH (a)<--(b)<--(c) RETURN a"),
            "MATCH (a)<--(b)<--(c) RETURN a"
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

        // A backslash inside backticks escapes nothing (#1489): `a\` ends at
        // its second backtick, and the comment after it is one.
        assert_eq!(
            strip_comments("RETURN 1 AS `a\\` // c\n, 2 AS `b``--c`"),
            "RETURN 1 AS `a\\` \n, 2 AS `b``--c`"
        );

        // Undirected relationship patterns (should NOT be treated as comments!)
        // These patterns caused a bug where `--(` was treated as a comment
        assert_eq!(
            strip_comments("MATCH (a)--(b) RETURN a"),
            "MATCH (a)--(b) RETURN a"
        );

        assert_eq!(
            strip_comments("MATCH (a)--() RETURN a"),
            "MATCH (a)--() RETURN a"
        );

        // Pattern comprehension with undirected relationship
        assert_eq!(
            strip_comments("WITH a, size([(a)--() | 1]) AS cnt RETURN cnt"),
            "WITH a, size([(a)--() | 1]) AS cnt RETURN cnt"
        );

        // Undirected relationship with type
        assert_eq!(
            strip_comments("MATCH (a)-[:KNOWS]-(b) RETURN a"),
            "MATCH (a)-[:KNOWS]-(b) RETURN a"
        );
    }
}
