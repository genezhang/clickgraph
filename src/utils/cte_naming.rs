//! Centralized CTE naming utilities to ensure consistent naming across the codebase.
//!
//! **CRITICAL**: All CTE name generation MUST use these functions to avoid naming mismatches.
//!
//! ## The Problem
//! Previously, CTE names were generated in 7+ different locations with subtle differences:
//! - Some sorted aliases, some didn't
//! - Some added sequence numbers, some didn't
//! - Some handled empty aliases differently
//! This led to bugs where a CTE was created with one name but referenced with another.
//!
//! ## The Solution
//! This module provides a single source of truth for CTE naming:
//! - `generate_cte_name()` - Generate CTE name with counter
//! - `generate_cte_base_name()` - Generate base name without counter (for lookups)
//!
//! ## Naming Convention
//! Format: `with_{sorted_aliases}_cte_{counter}`
//! - Aliases are ALWAYS sorted alphabetically
//! - Joined with underscores
//! - Counter is optional (use base name when counter doesn't matter)
//!
//! Examples:
//! - `["p", "friends"]` → `"with_friends_p_cte_1"` (sorted: friends, p)
//! - `["a"]` → `"with_a_cte_1"`
//! - `[]` → `"with_cte_1"`

/// Generate a CTE name with a sequence counter.
///
/// **Format**: `with_{sorted_aliases}_cte_{counter}`
///
/// # Arguments
/// * `aliases` - List of exported aliases from WITH clause
/// * `counter` - Sequence number for uniqueness
///
/// # Examples
/// ```
/// use clickgraph::utils::cte_naming::generate_cte_name;
///
/// assert_eq!(generate_cte_name(&["p"], 1), "with_p_cte_1");
/// assert_eq!(generate_cte_name(&["p", "friends"], 1), "with_friends_p_cte_1");  // sorted!
/// assert_eq!(generate_cte_name(&[], 1), "with_cte_1");
/// ```
pub fn generate_cte_name(aliases: &[impl AsRef<str>], counter: usize) -> String {
    let base = generate_cte_base_name(aliases);
    format!("{}_{}", base, counter)
}

/// Generate a CTE base name without counter (for pattern matching/lookups).
///
/// **Format**: `with_{sorted_aliases}_cte`
///
/// # Arguments
/// * `aliases` - List of exported aliases from WITH clause
///
/// # Examples
/// ```
/// use clickgraph::utils::cte_naming::generate_cte_base_name;
///
/// assert_eq!(generate_cte_base_name(&["p"]), "with_p_cte");
/// assert_eq!(generate_cte_base_name(&["p", "friends"]), "with_friends_p_cte");  // sorted!
/// assert_eq!(generate_cte_base_name(&[]), "with_cte");
/// ```
pub fn generate_cte_base_name(aliases: &[impl AsRef<str>]) -> String {
    // Sort aliases to ensure consistent naming
    let mut sorted_aliases: Vec<String> = aliases.iter()
        .map(|s| s.as_ref().to_string())
        .collect();
    sorted_aliases.sort();

    if sorted_aliases.is_empty() {
        "with_cte".to_string()
    } else {
        format!("with_{}_cte", sorted_aliases.join("_"))
    }
}

/// Extract aliases from a CTE name.
///
/// Useful for reverse lookups and debugging.
///
/// # Arguments
/// * `cte_name` - CTE name like "with_friends_p_cte_1"
///
/// # Returns
/// * `Some(Vec<String>)` - List of aliases (in sorted order)
/// * `None` - If name doesn't match expected format
///
/// # Examples
/// ```
/// use clickgraph::utils::cte_naming::extract_aliases_from_cte_name;
///
/// assert_eq!(
///     extract_aliases_from_cte_name("with_friends_p_cte_1"),
///     Some(vec!["friends".to_string(), "p".to_string()])
/// );
/// assert_eq!(extract_aliases_from_cte_name("with_cte_1"), Some(vec![]));
/// assert_eq!(extract_aliases_from_cte_name("invalid"), None);
/// ```
pub fn extract_aliases_from_cte_name(cte_name: &str) -> Option<Vec<String>> {
    // Format: with_{aliases}_cte_{counter} or with_cte_{counter}
    let stripped = cte_name.strip_prefix("with_")?;
    
    // Check if it's the "with_cte_{counter}" format (no aliases)
    if stripped.starts_with("cte_") {
        return Some(vec![]);
    }
    
    // Find the last occurrence of "_cte"
    let cte_pos = stripped.rfind("_cte")?;
    let middle = &stripped[..cte_pos];
    
    if middle.is_empty() {
        // This shouldn't happen with the starts_with check above, but be safe
        Some(vec![])
    } else {
        // "with_friends_p_cte_1" case - split by underscore
        Some(middle.split('_').map(|s| s.to_string()).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_cte_name_single_alias() {
        assert_eq!(generate_cte_name(&["p"], 1), "with_p_cte_1");
        assert_eq!(generate_cte_name(&["p"], 2), "with_p_cte_2");
    }

    #[test]
    fn test_generate_cte_name_multiple_aliases() {
        // Should sort alphabetically
        assert_eq!(generate_cte_name(&["p", "friends"], 1), "with_friends_p_cte_1");
        assert_eq!(generate_cte_name(&["friends", "p"], 1), "with_friends_p_cte_1");
        assert_eq!(generate_cte_name(&["z", "a", "m"], 1), "with_a_m_z_cte_1");
    }

    #[test]
    fn test_generate_cte_name_empty() {
        assert_eq!(generate_cte_name(&[] as &[&str], 1), "with_cte_1");
    }

    #[test]
    fn test_generate_cte_base_name() {
        assert_eq!(generate_cte_base_name(&["p"]), "with_p_cte");
        assert_eq!(generate_cte_base_name(&["p", "friends"]), "with_friends_p_cte");
        assert_eq!(generate_cte_base_name(&Vec::<String>::new()), "with_cte");
    }

    #[test]
    fn test_extract_aliases() {
        assert_eq!(
            extract_aliases_from_cte_name("with_friends_p_cte_1"),
            Some(vec!["friends".to_string(), "p".to_string()])
        );
        assert_eq!(
            extract_aliases_from_cte_name("with_p_cte_1"),
            Some(vec!["p".to_string()])
        );
        assert_eq!(
            extract_aliases_from_cte_name("with_cte_1"),
            Some(vec![])
        );
        assert_eq!(extract_aliases_from_cte_name("invalid"), None);
        assert_eq!(extract_aliases_from_cte_name("with_only"), None);
    }

    #[test]
    fn test_roundtrip() {
        let aliases = vec!["p", "friends", "age"];
        let cte_name = generate_cte_name(&aliases, 1);
        let extracted = extract_aliases_from_cte_name(&cte_name).unwrap();
        
        // Should match sorted order
        let mut sorted = aliases.clone();
        sorted.sort();
        assert_eq!(extracted, sorted);
    }
}
