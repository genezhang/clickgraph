//! CTE column naming utilities for unambiguous alias-property encoding.
//!
//! **Problem**: CTE columns like `person_1_name` are ambiguous when split on `_`.
//! Is the alias `person_1` with property `name`, or `person` with property `1_name`?
//!
//! **Solution**: Encode the alias length so parsing is deterministic.
//!
//! ## Format
//! `p{N}_{alias}_{property}`
//!
//! Where `N` is the character length of `alias` (decimal digits).
//!
//! ## Examples
//! - `("u", "name")` → `"p1_u_name"`
//! - `("person_1", "user_id")` → `"p8_person_1_user_id"`
//! - `("a", "user_id")` → `"p1_a_user_id"`
//! - `("my_var", "full_name")` → `"p6_my_var_full_name"`

/// Generate a CTE column name from alias and property.
///
/// Format: `p{alias_len}_{alias}_{property}`
///
/// # Examples
/// ```
/// use clickgraph::utils::cte_column_naming::cte_column_name;
///
/// assert_eq!(cte_column_name("u", "name"), "p1_u_name");
/// assert_eq!(cte_column_name("person_1", "user_id"), "p8_person_1_user_id");
/// assert_eq!(cte_column_name("a", "user_id"), "p1_a_user_id");
/// ```
pub fn cte_column_name(alias: &str, property: &str) -> String {
    format!("p{}_{alias}_{property}", alias.len())
}

/// Parse a CTE column name back into (alias, property).
///
/// Returns `None` if the name doesn't match the `p{N}_{alias}_{property}` format.
///
/// # Examples
/// ```
/// use clickgraph::utils::cte_column_naming::parse_cte_column;
///
/// assert_eq!(parse_cte_column("p1_u_name"), Some(("u".to_string(), "name".to_string())));
/// assert_eq!(parse_cte_column("p8_person_1_user_id"), Some(("person_1".to_string(), "user_id".to_string())));
/// assert_eq!(parse_cte_column("p1_a_user_id"), Some(("a".to_string(), "user_id".to_string())));
/// assert_eq!(parse_cte_column("invalid"), None);
/// assert_eq!(parse_cte_column("cnt"), None);
/// ```
pub fn parse_cte_column(col_name: &str) -> Option<(String, String)> {
    let rest = col_name.strip_prefix('p')?;

    // Read digits for alias length
    let digit_end = rest.find(|c: char| !c.is_ascii_digit())?;
    if digit_end == 0 {
        return None;
    }
    let alias_len: usize = rest[..digit_end].parse().ok()?;
    if alias_len == 0 {
        return None;
    }

    // After digits, expect underscore
    let after_digits = &rest[digit_end..];
    let after_underscore = after_digits.strip_prefix('_')?;

    // Extract alias (exactly alias_len chars)
    if after_underscore.len() < alias_len {
        return None;
    }
    let alias = &after_underscore[..alias_len];

    // After alias, expect underscore then property
    let after_alias = &after_underscore[alias_len..];
    let property = after_alias.strip_prefix('_')?;

    if property.is_empty() {
        return None;
    }

    Some((alias.to_string(), property.to_string()))
}

/// Check if a column name uses the CTE column naming format.
///
/// # Examples
/// ```
/// use clickgraph::utils::cte_column_naming::is_cte_column;
///
/// assert!(is_cte_column("p1_u_name"));
/// assert!(is_cte_column("p8_person_1_user_id"));
/// assert!(!is_cte_column("u_name"));
/// assert!(!is_cte_column("cnt"));
/// assert!(!is_cte_column("post_id"));
/// ```
pub fn is_cte_column(col_name: &str) -> bool {
    parse_cte_column(col_name).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_alias() {
        assert_eq!(cte_column_name("u", "name"), "p1_u_name");
        assert_eq!(
            parse_cte_column("p1_u_name"),
            Some(("u".to_string(), "name".to_string()))
        );
    }

    #[test]
    fn test_alias_with_underscore() {
        assert_eq!(
            cte_column_name("person_1", "user_id"),
            "p8_person_1_user_id"
        );
        assert_eq!(
            parse_cte_column("p8_person_1_user_id"),
            Some(("person_1".to_string(), "user_id".to_string()))
        );
    }

    #[test]
    fn test_property_with_underscore() {
        assert_eq!(cte_column_name("u", "full_name"), "p1_u_full_name");
        assert_eq!(
            parse_cte_column("p1_u_full_name"),
            Some(("u".to_string(), "full_name".to_string()))
        );
    }

    #[test]
    fn test_both_with_underscores() {
        assert_eq!(
            cte_column_name("my_var", "full_name"),
            "p6_my_var_full_name"
        );
        assert_eq!(
            parse_cte_column("p6_my_var_full_name"),
            Some(("my_var".to_string(), "full_name".to_string()))
        );
    }

    #[test]
    fn test_longer_alias() {
        assert_eq!(
            cte_column_name("very_long_alias", "x"),
            "p15_very_long_alias_x"
        );
        assert_eq!(
            parse_cte_column("p15_very_long_alias_x"),
            Some(("very_long_alias".to_string(), "x".to_string()))
        );
    }

    #[test]
    fn test_roundtrip() {
        let cases = vec![
            ("u", "name"),
            ("a", "user_id"),
            ("person_1", "user_id"),
            ("my_var", "full_name"),
            ("x", "y"),
            ("node_alias_123", "some_property_name"),
        ];
        for (alias, property) in cases {
            let encoded = cte_column_name(alias, property);
            let decoded = parse_cte_column(&encoded);
            assert_eq!(
                decoded,
                Some((alias.to_string(), property.to_string())),
                "Roundtrip failed for ({}, {}): encoded='{}', decoded={:?}",
                alias,
                property,
                encoded,
                decoded
            );
        }
    }

    #[test]
    fn test_non_cte_columns() {
        assert_eq!(parse_cte_column("cnt"), None);
        assert_eq!(parse_cte_column("u_name"), None); // Old format, not recognized
        assert_eq!(parse_cte_column("user_id"), None);
        assert_eq!(parse_cte_column("__label__"), None);
        assert_eq!(parse_cte_column(""), None);
    }

    #[test]
    fn test_is_cte_column() {
        assert!(is_cte_column("p1_u_name"));
        assert!(is_cte_column("p8_person_1_user_id"));
        assert!(!is_cte_column("u_name"));
        assert!(!is_cte_column("cnt"));
        assert!(!is_cte_column(""));
    }

    #[test]
    fn test_malformed_inputs() {
        assert_eq!(parse_cte_column("p"), None); // No digits
        assert_eq!(parse_cte_column("p_u_name"), None); // No digits after p
        assert_eq!(parse_cte_column("p0__name"), None); // Zero-length alias, empty alias
        assert_eq!(parse_cte_column("p1_u"), None); // No property (missing trailing _prop)
        assert_eq!(parse_cte_column("p99_u_name"), None); // alias_len > remaining
    }
}
