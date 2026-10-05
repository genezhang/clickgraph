//! Cypher string-literal text → SQL string literal.
//!
//! The parser keeps a Cypher string literal's body RAW (`'it\'s'` → `it\'s`, escapes
//! undecoded). Raw backslash escapes that ClickHouse reads the same way (`\\`, `\n`, `\t`, …)
//! pass through unchanged; the ones whose SQL spelling differs are translated here so the
//! literal stays valid and keeps the Cypher value:
//!
//! - `\'` → the active dialect's quote spelling (`FunctionMapper::string_literal_quote_escape`:
//!   `''` for ClickHouse, `\'` for Spark, where `'it''s'` is two adjacent literals reading `its`)
//! - `\"` → `"`
//! - `\uXXXX` / `\UXXXXXXXX` → the character itself (ClickHouse has no `\u` escape)
//! - a bare `'` (from a double-quoted literal) → the same quote spelling
//!
//! An escaped backslash is consumed as a pair, so `\\'` is a backslash followed by a bare quote.

/// Render the raw body of a Cypher string literal as a quoted SQL literal.
pub fn cypher_string_to_sql_literal(raw: &str) -> String {
    let quote = crate::sql_generator::function_mapper::for_dialect(
        crate::server::query_context::get_current_dialect(),
    )
    .string_literal_quote_escape();
    cypher_string_to_sql_literal_with(raw, quote)
}

/// [`cypher_string_to_sql_literal`] with an explicit spelling for an embedded quote.
pub fn cypher_string_to_sql_literal_with(raw: &str, quote: &str) -> String {
    let mut out = String::with_capacity(raw.len() + 2);
    out.push('\'');
    let mut chars = raw.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' => out.push_str(quote),
            '\\' => match chars.next() {
                Some('\'') => out.push_str(quote),
                Some('"') => out.push('"'),
                Some('\\') => out.push_str("\\\\"),
                Some(u @ ('u' | 'U')) => {
                    let width = if u == 'u' { 4 } else { 8 };
                    let hex: String = chars.clone().take(width).collect();
                    let decoded = (hex.len() == width)
                        .then(|| u32::from_str_radix(&hex, 16).ok())
                        .flatten()
                        .and_then(char::from_u32);
                    match decoded {
                        Some(ch) => {
                            chars.nth(width - 1);
                            match ch {
                                '\'' => out.push_str(quote),
                                '\\' => out.push_str("\\\\"),
                                other => out.push(other),
                            }
                        }
                        // not a valid escape: leave the text as written
                        None => {
                            out.push('\\');
                            out.push(u);
                        }
                    }
                }
                Some(other) => {
                    out.push('\\');
                    out.push(other);
                }
                None => out.push_str("\\\\"),
            },
            other => out.push(other),
        }
    }
    out.push('\'');
    out
}

/// The VALUE of a raw Cypher string-literal body: escapes decoded (`\'`, `\"`, `\\`, `\n`, `\t`,
/// `\r`, `\b`, `\f`, `\uXXXX`, `\UXXXXXXXX`). Unknown escapes keep their text. For comparing two
/// literals at translation time — `'it\'s' = "it's"` must see the same value on both sides.
pub fn cypher_string_value(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('t') => out.push('\t'),
            Some('r') => out.push('\r'),
            Some('b') => out.push('\u{8}'),
            Some('f') => out.push('\u{c}'),
            Some(u @ ('u' | 'U')) => {
                let width = if u == 'u' { 4 } else { 8 };
                let hex: String = chars.clone().take(width).collect();
                let decoded = (hex.len() == width)
                    .then(|| u32::from_str_radix(&hex, 16).ok())
                    .flatten()
                    .and_then(char::from_u32);
                match decoded {
                    Some(ch) => {
                        chars.nth(width - 1);
                        out.push(ch);
                    }
                    None => {
                        out.push('\\');
                        out.push(u);
                    }
                }
            }
            Some(c @ ('\'' | '"' | '\\')) => out.push(c),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::cypher_string_to_sql_literal as lit;

    #[test]
    fn plain_and_bare_quote() {
        assert_eq!(lit("abc"), "'abc'");
        assert_eq!(lit("it's"), "'it''s'");
    }

    #[test]
    fn quote_spelling_is_a_parameter_1217() {
        use super::cypher_string_to_sql_literal_with as lit_with;
        assert_eq!(lit_with("it's", r"\'"), r"'it\'s'");
        assert_eq!(lit_with(r"it\'s", r"\'"), r"'it\'s'");
        assert_eq!(lit_with(r"a\\'b", r"\'"), r"'a\\\'b'");
        assert_eq!(lit_with(r"\u0027", r"\'"), r"'\''");
        assert_eq!(lit_with("it's", "''"), "'it''s'");
    }

    #[test]
    fn escaped_quotes() {
        assert_eq!(lit(r"it\'s"), "'it''s'");
        assert_eq!(lit(r#"say \"hi\""#), r#"'say "hi"'"#);
    }

    #[test]
    fn escaped_backslash_is_a_pair() {
        assert_eq!(lit(r"a\\b"), r"'a\\b'");
        // `\\` then a bare quote — the quote is NOT escaped by the backslash before it
        assert_eq!(lit(r"a\\'b"), r"'a\\''b'");
        assert_eq!(lit(r"\\"), r"'\\'");
    }

    #[test]
    fn clickhouse_native_escapes_pass_through() {
        assert_eq!(lit(r"tab\there"), r"'tab\there'");
        assert_eq!(lit(r"x\ny"), r"'x\ny'");
    }

    #[test]
    fn unicode_escapes_decode() {
        assert_eq!(lit(r"Abc"), "'Abc'");
        assert_eq!(lit(r"\U0001F600"), "'😀'");
        // a decoded quote / backslash is re-escaped
        assert_eq!(lit(r"'"), "''''");
        assert_eq!(lit(r"\"), r"'\\'");
        // malformed: left as written
        assert_eq!(lit(r"\u00zz"), r"'\u00zz'");
        assert_eq!(lit(r"\u12"), r"'\u12'");
    }

    #[test]
    fn value_decodes_escapes() {
        use super::cypher_string_value as val;
        assert_eq!(val(r"it\'s"), "it's");
        assert_eq!(val(r#"say \"hi\""#), "say \"hi\"");
        assert_eq!(val(r"a\\b"), "a\\b");
        assert_eq!(val(r"x\ty\n"), "x\ty\n");
        assert_eq!(val(r"caf\u00e9"), "caf\u{e9}");
        assert_eq!(val(r"\u00zz"), r"\u00zz");
        assert_eq!(val(r"\d+"), r"\d+");
        // two spellings of one value compare equal
        assert_eq!(val(r"it\'s"), val("it's"));
        assert_ne!(val(r"it\\'s"), val("it's"));
    }

    #[test]
    fn trailing_lone_backslash_stays_valid() {
        assert_eq!(lit("a\\"), r"'a\\'");
    }
}
