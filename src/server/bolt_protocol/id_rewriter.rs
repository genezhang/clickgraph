//! Query Rewriter for id() Function Support
//!
//! Neo4j Browser uses `id(node)` function to reference nodes by integer ID.
//! ClickGraph uses string-based element_ids (e.g., "Airport:LAX").
//!
//! This module rewrites queries containing `id(alias) = N` to use the actual
//! primary key from the element_id, enabling Neo4j Browser's expand/double-click.
//!
//! # How it works:
//! 1. Detect `id(alias) = N` or `id(alias) IN [...]` patterns
//! 2. Look up N in IdMapper → element_id (e.g., "Airport:LAX")
//! 3. Parse element_id → (label, id_value)
//! 4. Rewrite to: `alias.id = 'LAX'` (using "id" as generic primary key property)
//!    The query planner will resolve "id" to the actual column via schema

use super::id_mapper::IdMapper;
use crate::graph_catalog::element_id::parse_node_element_id;
use regex::Regex;
use std::sync::LazyLock;

/// Regex to match id(alias) = N pattern
/// Captures: (1) alias, (2) integer
static ID_EQUALS_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\bid\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\)\s*=\s*(\d+)").unwrap()
});

/// Regex to match id(alias) IN [...] pattern (including empty lists)
/// Captures: (1) alias, (2) the list content (may be empty)
static ID_IN_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\bid\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\)\s+IN\s*\[\s*([^\]]*)\s*\]").unwrap()
});

/// Regex to match NOT id(alias) IN [...] pattern (including empty lists)
/// Captures: (1) alias, (2) the list content (may be empty)
static NOT_ID_IN_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\bNOT\s+id\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\)\s+IN\s*\[\s*([^\]]*)\s*\]")
        .unwrap()
});

/// Regex to match ORDER BY id(alias)
/// Captures: (1) alias
static ORDER_BY_ID_PATTERN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)\bORDER\s+BY\s+id\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\)").unwrap()
});

/// Result of id() rewriting
#[derive(Debug)]
pub struct IdRewriteResult {
    /// The rewritten query (or original if no changes)
    pub query: String,
    /// Whether any rewrites were made
    pub was_rewritten: bool,
    /// IDs that were not found in the mapper (query will return empty result)
    pub missing_ids: Vec<i64>,
}

/// Rewrite id() predicates in a Cypher query using the IdMapper.
///
/// # Arguments
/// * `query` - The original Cypher query
/// * `id_mapper` - The session-scoped IdMapper with integer→element_id mappings
///
/// # Returns
/// An `IdRewriteResult` containing the rewritten query and metadata
pub fn rewrite_id_predicates(query: &str, id_mapper: &IdMapper) -> IdRewriteResult {
    let mut result = query.to_string();
    let mut was_rewritten = false;
    let mut missing_ids = Vec::new();

    // Rewrite id(alias) = N patterns
    result = rewrite_id_equals(&result, id_mapper, &mut was_rewritten, &mut missing_ids);

    // Rewrite id(alias) IN [...] patterns
    result = rewrite_id_in(
        &result,
        id_mapper,
        &mut was_rewritten,
        &mut missing_ids,
        false,
    );

    // Rewrite NOT id(alias) IN [...] patterns
    result = rewrite_id_in(
        &result,
        id_mapper,
        &mut was_rewritten,
        &mut missing_ids,
        true,
    );

    // Rewrite ORDER BY id(alias) patterns
    result = rewrite_order_by_id(&result, &mut was_rewritten);

    IdRewriteResult {
        query: result,
        was_rewritten,
        missing_ids,
    }
}

/// Parse element_id and generate a property filter expression
/// Returns (label, filter_expr) or None if parsing fails
fn element_id_to_filter(element_id: &str, alias: &str) -> Option<(String, String)> {
    match parse_node_element_id(element_id) {
        Ok((label, id_values)) => {
            // For single ID: alias:Label AND alias.id = 'value'
            // For composite ID: alias:Label AND alias.id = 'v1|v2|v3'
            let id_value = id_values.join("|");

            // Use "id" as the generic primary key property name
            // The schema defines node_id which maps to the actual column
            // Escape single quotes in id_value
            let escaped_id = id_value.replace('\'', "''");

            // Generate: (alias:Label AND alias.id = 'value')
            let filter = format!("({}:{} AND {}.id = '{}')", alias, label, alias, escaped_id);
            Some((label, filter))
        }
        Err(e) => {
            log::warn!("Failed to parse element_id '{}': {}", element_id, e);
            None
        }
    }
}

/// Rewrite id(alias) = N patterns
fn rewrite_id_equals(
    query: &str,
    id_mapper: &IdMapper,
    was_rewritten: &mut bool,
    missing_ids: &mut Vec<i64>,
) -> String {
    let mut result = query.to_string();

    // Find all matches and process them
    let matches: Vec<_> = ID_EQUALS_PATTERN
        .captures_iter(query)
        .map(|cap| {
            let full_match = cap.get(0).unwrap();
            let alias = cap.get(1).unwrap().as_str();
            let id_str = cap.get(2).unwrap().as_str();
            let id: i64 = id_str.parse().unwrap_or(0);
            (full_match.start(), full_match.end(), alias.to_string(), id)
        })
        .collect();

    // Process in reverse order to preserve string positions
    for (start, end, alias, id) in matches.into_iter().rev() {
        if let Some(element_id) = id_mapper.get_element_id(id) {
            // Parse element_id and generate filter
            if let Some((_label, filter)) = element_id_to_filter(element_id, &alias) {
                result.replace_range(start..end, &filter);
                *was_rewritten = true;
                log::info!("id() rewrite: id({}) = {} → {}", alias, id, filter);
            } else {
                // Fallback: use element_id string comparison
                let replacement = format!("1 = 0",);
                result.replace_range(start..end, &replacement);
                *was_rewritten = true;
            }
        } else {
            // ID not found - this node doesn't exist in this session
            // Replace with impossible condition
            let replacement = format!("1 = 0");
            result.replace_range(start..end, &replacement);
            *was_rewritten = true;
            missing_ids.push(id);
            log::warn!(
                "id() rewrite: id({}) = {} not found in session, returning empty",
                alias,
                id
            );
        }
    }

    result
}

/// Rewrite id(alias) IN [...] or NOT id(alias) IN [...] patterns
fn rewrite_id_in(
    query: &str,
    id_mapper: &IdMapper,
    was_rewritten: &mut bool,
    missing_ids: &mut Vec<i64>,
    is_negated: bool,
) -> String {
    let mut result = query.to_string();
    let pattern = if is_negated {
        &*NOT_ID_IN_PATTERN
    } else {
        &*ID_IN_PATTERN
    };

    // Find all matches and process them
    let matches: Vec<_> = pattern
        .captures_iter(query)
        .map(|cap| {
            let full_match = cap.get(0).unwrap();
            let alias = cap.get(1).unwrap().as_str();
            let list_content = cap.get(2).unwrap().as_str();
            (
                full_match.start(),
                full_match.end(),
                alias.to_string(),
                list_content.to_string(),
            )
        })
        .collect();

    // Process in reverse order to preserve string positions
    for (start, end, alias, list_content) in matches.into_iter().rev() {
        // Parse the list of integers
        let ids: Vec<i64> = list_content
            .split(',')
            .filter_map(|s| s.trim().parse::<i64>().ok())
            .collect();

        // Look up each ID and collect filters
        let mut filters: Vec<String> = Vec::new();
        for id in &ids {
            if let Some(element_id) = id_mapper.get_element_id(*id) {
                if let Some((_label, filter)) = element_id_to_filter(element_id, &alias) {
                    filters.push(filter);
                }
            } else {
                missing_ids.push(*id);
            }
        }

        if filters.is_empty() {
            // All IDs missing - use impossible/tautology condition
            let replacement = if is_negated {
                "1 = 1".to_string()
            } else {
                "1 = 0".to_string()
            };
            result.replace_range(start..end, &replacement);
        } else {
            // Create OR of all filters
            let combined = filters.join(" OR ");
            let replacement = if is_negated {
                format!("NOT ({})", combined)
            } else {
                format!("({})", combined)
            };
            result.replace_range(start..end, &replacement);
        }
        *was_rewritten = true;
        log::info!(
            "id() rewrite: {}id({}) IN [...] → {} filter(s)",
            if is_negated { "NOT " } else { "" },
            alias,
            filters.len()
        );
    }

    result
}

/// Rewrite ORDER BY id(alias) patterns
/// Since we don't have a stable ordering by integer ID, we use element_id-based ordering
fn rewrite_order_by_id(query: &str, was_rewritten: &mut bool) -> String {
    let mut result = query.to_string();

    let matches: Vec<_> = ORDER_BY_ID_PATTERN
        .captures_iter(query)
        .map(|cap| {
            let full_match = cap.get(0).unwrap();
            let alias = cap.get(1).unwrap().as_str();
            (full_match.start(), full_match.end(), alias.to_string())
        })
        .collect();

    // Process in reverse order
    for (start, end, alias) in matches.into_iter().rev() {
        // Replace with ORDER BY alias.id (the schema's primary key property)
        let replacement = format!("ORDER BY {}.id", alias);
        result.replace_range(start..end, &replacement);
        *was_rewritten = true;
        log::info!(
            "id() rewrite: ORDER BY id({}) → ORDER BY {}.id",
            alias,
            alias
        );
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_mapper() -> IdMapper {
        let mut mapper = IdMapper::new();
        // Simulate nodes that were returned in previous queries
        mapper.get_or_assign("Airport:LAX"); // id=1
        mapper.get_or_assign("Airport:JFK"); // id=2
        mapper.get_or_assign("Airport:SFO"); // id=3
        mapper.get_or_assign("User:alice"); // id=4
        mapper
    }

    #[test]
    fn test_id_equals_found() {
        let mapper = setup_mapper();
        let query = "MATCH (a) WHERE id(a) = 1 RETURN a";
        let result = rewrite_id_predicates(query, &mapper);

        assert!(result.was_rewritten);
        assert!(result.query.contains("a:Airport"));
        assert!(result.query.contains("a.id = 'LAX'"));
        assert!(result.missing_ids.is_empty());
    }

    #[test]
    fn test_id_equals_not_found() {
        let mapper = setup_mapper();
        let query = "MATCH (a) WHERE id(a) = 999 RETURN a";
        let result = rewrite_id_predicates(query, &mapper);

        assert!(result.was_rewritten);
        assert!(result.query.contains("1 = 0"));
        assert_eq!(result.missing_ids, vec![999]);
    }

    #[test]
    fn test_id_in_list() {
        let mapper = setup_mapper();
        let query = "MATCH (a) WHERE id(a) IN [1, 2, 3] RETURN a";
        let result = rewrite_id_predicates(query, &mapper);

        assert!(result.was_rewritten);
        assert!(result.query.contains("a:Airport"));
        assert!(result.query.contains("a.id = 'LAX'"));
        assert!(result.query.contains("a.id = 'JFK'"));
        assert!(result.query.contains("a.id = 'SFO'"));
    }

    #[test]
    fn test_not_id_in_list() {
        let mapper = setup_mapper();
        let query = "MATCH (a)--(o) WHERE NOT id(o) IN [1, 2] RETURN o";
        let result = rewrite_id_predicates(query, &mapper);

        assert!(result.was_rewritten);
        assert!(result.query.contains("NOT ("));
    }

    #[test]
    fn test_order_by_id() {
        let mapper = setup_mapper();
        let query = "MATCH (a) RETURN a ORDER BY id(a) LIMIT 10";
        let result = rewrite_id_predicates(query, &mapper);

        assert!(result.was_rewritten);
        assert!(result.query.contains("ORDER BY a.id"));
    }

    #[test]
    fn test_complex_query() {
        let mapper = setup_mapper();
        let query = r#"MATCH (a) WHERE id(a) = 2
WITH a
MATCH path = (a)--(o) WHERE NOT id(o) IN [1, 3]
RETURN path
ORDER BY id(o)
LIMIT 97"#;
        let result = rewrite_id_predicates(query, &mapper);

        assert!(result.was_rewritten);
        assert!(result.query.contains("a:Airport"));
        assert!(result.query.contains("a.id = 'JFK'"));
        assert!(result.query.contains("NOT ("));
        assert!(result.query.contains("ORDER BY o.id"));
    }

    #[test]
    fn test_no_id_predicates() {
        let mapper = setup_mapper();
        let query = "MATCH (n:User) RETURN n.name";
        let result = rewrite_id_predicates(query, &mapper);

        assert!(!result.was_rewritten);
        assert_eq!(result.query, query);
    }
}
