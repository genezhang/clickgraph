use std::collections::HashMap;

/// Per-query registry mapping CTE aliases and properties to their output column names.
/// Used during SQL rendering to resolve property accesses to CTE columns.
///
/// Example:
/// - CTE creates: SELECT full_name AS a_name FROM users AS a
/// - Registry stores: ("a", "name") → "a_name"
/// - SQL rendering uses: table_alias.a_name (not table_alias.full_name)
#[derive(Debug, PartialEq, Clone, Default)]
pub struct CteColumnRegistry {
    /// Map: (cte_alias, cypher_property) → cte_output_column
    /// Example: ("a", "name") → "a_name"
    pub alias_property_to_column: HashMap<(String, String), String>,

    /// Map: cte_alias → cte_name for context
    /// Used for debugging and validation
    pub alias_to_cte_name: HashMap<String, String>,

    /// Map: cypher_alias → FROM alias for column references
    /// This is determined at reference time (not CTE creation time)
    /// Example: "b" → "with_a_b_cte_1" (use CTE name as default)
    /// Can be overridden when FROM clause uses explicit alias like "AS a_b"
    pub alias_to_from_alias: HashMap<String, String>,
}

impl CteColumnRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            alias_property_to_column: HashMap::new(),
            alias_to_cte_name: HashMap::new(),
            alias_to_from_alias: HashMap::new(),
        }
    }

    /// Register a column mapping: (alias, property) → column
    pub fn register(
        &mut self,
        cte_alias: String,
        cte_name: String,
        property: String,
        column: String,
    ) {
        self.alias_property_to_column
            .insert((cte_alias.clone(), property), column);
        self.alias_to_cte_name.insert(cte_alias, cte_name);
    }

    /// Look up the CTE output column for an alias and property
    pub fn lookup(&self, cte_alias: &str, property: &str) -> Option<String> {
        self.alias_property_to_column
            .get(&(cte_alias.to_string(), property.to_string()))
            .cloned()
    }

    /// Check if this alias is registered as a CTE
    pub fn is_cte_alias(&self, alias: &str) -> bool {
        self.alias_to_cte_name.contains_key(alias)
    }

    /// Merge another registry into this one (for nested plans)
    pub fn merge(&mut self, other: &CteColumnRegistry) {
        self.alias_property_to_column
            .extend(other.alias_property_to_column.clone());
        self.alias_to_cte_name
            .extend(other.alias_to_cte_name.clone());
        self.alias_to_from_alias
            .extend(other.alias_to_from_alias.clone());
    }

    /// Populate registry from CTE column metadata.
    /// This is called after rendering a RenderPlan to collect all property-to-column mappings.
    pub fn populate_from_cte_metadata(
        &mut self,
        cte_name: &str,
        columns: &[crate::render_plan::CteColumnMetadata],
    ) {
        // Track unique cypher_aliases in this CTE
        let mut seen_aliases = std::collections::HashSet::new();
        
        for metadata in columns {
            self.register(
                metadata.cypher_alias.clone(),
                cte_name.to_string(),
                metadata.cypher_property.clone(),
                metadata.cte_column_name.clone(),
            );
            seen_aliases.insert(metadata.cypher_alias.clone());
        }
        
        // Set default FROM alias to CTE name for all unique aliases
        // This can be overridden later if FROM clause uses explicit "AS alias"
        for alias in seen_aliases {
            self.alias_to_from_alias.insert(alias, cte_name.to_string());
        }
    }
}
