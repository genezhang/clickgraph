//! Type aliases for common patterns in render plan generation
//!
//! This module provides semantic type aliases for frequently-used complex generic
//! combinations, improving code readability and reducing cognitive load when reading
//! function signatures and type annotations.

use std::collections::{HashMap, HashSet};

// ============================================================================
// ID and Reference Mapping Types
// ============================================================================

/// Maps entity identifiers to their source information
/// 
/// **Usage**: Track which ID column comes from which table/source
/// 
/// **Example**: `{"user_id": ("users", "id"), "post_id": ("posts", "id")}`
pub type IdSourceMap = HashMap<String, (String, String)>;

/// Maps simple identifiers to identity mapping information
/// 
/// **Usage**: Track table/alias to table/column pairs for joins
/// 
/// **Example**: `{"a": [("users", "user_id"), ("users", "id")]}`
pub type IdentityMappingMap = HashMap<String, Vec<(String, String)>>;

// ============================================================================
// CTE and Join Context Types
// ============================================================================

/// Maps CTE names to their referenced aliases
/// 
/// **Usage**: Track which aliases appear in which CTEs
/// 
/// **Example**: `{"with_a_cte_1": ["a", "b"], "with_c_cte_2": ["c"]}`
pub type CTEReferenceMap = HashMap<String, Vec<String>>;

/// Maps entity aliases to type information
/// 
/// Inner tuple: (is_optional, Option<Vec<type_names>>)
/// - is_optional: Whether this entity is from an OPTIONAL MATCH
/// - type_names: List of possible type names (for polymorphic scenarios)
/// 
/// **Usage**: Track which aliases are optional and their possible types
pub type CTEEntityTypeMap = HashMap<String, HashMap<String, (bool, Option<Vec<String>>)>>;

// ============================================================================
// Edge and Relationship Context Types
// ============================================================================

/// Maps relationship information with optional edge properties
/// 
/// Inner tuple: (relationship_name, Option<Vec<property_names>>)
/// - relationship_name: Name of the relationship type
/// - property_names: Optional list of properties stored on the edge
/// 
/// **Usage**: Track edge types and whether they have properties
/// 
/// **Example**: `{"rel1": vec![("FOLLOWS", None), ("LIKES", Some(vec!["weight"]))]}`
pub type EdgePropertyMap = HashMap<String, Vec<(String, Option<Vec<String>>)>>;

/// Maps node/edge labels to their possible table names
/// 
/// **Usage**: Handle polymorphic schemas where multiple tables can have same label
pub type LabelToTableMap = HashMap<String, Vec<String>>;

// ============================================================================
// Grouping and Aggregation Types
// ============================================================================

/// Maps group keys to items in that group
/// 
/// **Usage**: Grouping entities by some identifier for aggregation/processing
pub type GroupingMap<T> = HashMap<String, Vec<T>>;

/// Alias for string-based grouping (most common case)
pub type StringGroupingMap = HashMap<String, Vec<String>>;

// ============================================================================
// Result and Index Types
// ============================================================================

/// Query result row - maps column names to string values
/// 
/// **Usage**: Represent a single row from a database query result
pub type QueryRow = HashMap<String, String>;

/// Complete query result - multiple rows
/// 
/// **Usage**: Represent full query results from ClickHouse
pub type QueryResult = Vec<QueryRow>;

/// Index mapping - maps identifiers to their positions/names
/// 
/// **Usage**: Build indexes for fast lookups
pub type IndexMap = HashMap<String, Vec<String>>;

// ============================================================================
// Appearance and Node Tracking Types
// ============================================================================

/// Tracks where nodes appear in query patterns
/// 
/// **Usage**: Track node appearances across multiple branches for shared node detection
/// 
/// Maps node label to list of (table_alias, query_path) tuples where it appears
pub type NodeAppearanceMap = HashMap<String, Vec<(String, String)>>;

/// Maps variable names to their appearance information
pub type VariableAppearanceMap = HashMap<String, Vec<String>>;

// ============================================================================
// Configuration and Metadata Types
// ============================================================================

/// Maps configuration keys to their string values
/// 
/// **Usage**: Store key-value configuration pairs
pub type ConfigMap = HashMap<String, String>;

/// Maps alias names to their string representations
/// 
/// **Usage**: Track alias renaming, remapping
pub type AliasMap = HashMap<String, String>;

/// Bidirectional alias mapping - both directions available
/// 
/// **Usage**: When you need to map both from→to and to→from
pub type BidirectionalAliasMap = (AliasMap, AliasMap);

// ============================================================================
// Collection Types for Common Patterns
// ============================================================================

/// Set of string identifiers (commonly used for deduplication)
pub type StringSet = HashSet<String>;

/// List of string values (preserve order, allow duplicates)
pub type StringList = Vec<String>;

/// Set of tuples for relationship pairs
pub type RelationshipPairSet = HashSet<(String, String)>;

/// List of tuples preserving order
pub type RelationshipPairList = Vec<(String, String)>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_aliases_compile() {
        // Verify type aliases are usable
        let mut id_sources: IdSourceMap = HashMap::new();
        id_sources.insert("user_id".to_string(), ("users".to_string(), "id".to_string()));
        assert_eq!(id_sources.len(), 1);

        let mut cte_refs: CTEReferenceMap = HashMap::new();
        cte_refs.insert("with_a_cte_1".to_string(), vec!["a".to_string(), "b".to_string()]);
        assert_eq!(cte_refs.len(), 1);

        let mut grouping: StringGroupingMap = HashMap::new();
        grouping.insert("group1".to_string(), vec!["item1".to_string(), "item2".to_string()]);
        assert_eq!(grouping.len(), 1);

        let result: QueryResult = vec![
            vec![("col1".to_string(), "val1".to_string())].into_iter().collect(),
        ];
        assert_eq!(result.len(), 1);
    }
}
