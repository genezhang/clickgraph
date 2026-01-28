//! Property Requirements Tracking
//!
//! This module tracks which properties of each alias are required by downstream usage
//! in a query. Used by the property pruning optimization to avoid materializing
//! unnecessary columns.
//!
//! # Example
//!
//! ```rust
//! use clickgraph::query_planner::analyzer::property_requirements::PropertyRequirements;
//!
//! let mut reqs = PropertyRequirements::new();
//!
//! // Track that 'friend' alias needs firstName and lastName
//! reqs.require_property("friend", "firstName");
//! reqs.require_property("friend", "lastName");
//!
//! // Query requirements
//! let friend_props = reqs.get_requirements("friend").unwrap();
//! assert!(friend_props.contains("firstName"));
//! assert!(friend_props.contains("lastName"));
//! ```
//!
//! # Architecture
//!
//! PropertyRequirements is populated by PropertyRequirementsAnalyzer pass
//! which traverses the logical plan tree from root (RETURN) to leaves (MATCH),
//! collecting property references from expressions.

use std::collections::{HashMap, HashSet};

/// Tracks which properties of each alias are required by downstream usage
///
/// This is used by the property pruning optimization to avoid selecting/collecting
/// unnecessary columns when expanding node/relationship aliases.
///
/// # Examples
///
/// ```rust
/// # use clickgraph::query_planner::analyzer::property_requirements::PropertyRequirements;
/// let mut reqs = PropertyRequirements::new();
///
/// // Specific properties required
/// reqs.require_property("user", "name");
/// reqs.require_property("user", "email");
///
/// // Wildcard - all properties required
/// reqs.require_all("product");
///
/// assert_eq!(reqs.get_requirements("user").unwrap().len(), 2);
/// assert!(reqs.requires_all("product"));
/// ```
#[derive(Clone, Debug, Default)]
pub struct PropertyRequirements {
    /// Map: alias -> set of required property names
    ///
    /// Example: { "friend" -> {"firstName", "lastName", "id"} }
    ///
    /// If an alias has no entry, no requirements are known (defaults to all properties)
    required_properties: HashMap<String, HashSet<String>>,

    /// Aliases that require ALL properties (e.g., RETURN friend.* or RETURN friend)
    ///
    /// When an alias is in this set, get_requirements() returns None
    /// to indicate all properties should be included.
    wildcard_aliases: HashSet<String>,
}

impl PropertyRequirements {
    /// Create a new empty PropertyRequirements
    pub fn new() -> Self {
        Self {
            required_properties: HashMap::new(),
            wildcard_aliases: HashSet::new(),
        }
    }

    /// Mark that an alias needs a specific property
    ///
    /// # Arguments
    /// * `alias` - The alias name (e.g., "friend", "user", "p")
    /// * `property` - The property name (e.g., "firstName", "age")
    ///
    /// # Example
    ///
    /// ```rust
    /// # use clickgraph::query_planner::analyzer::property_requirements::PropertyRequirements;
    /// let mut reqs = PropertyRequirements::new();
    /// reqs.require_property("user", "firstName");
    /// reqs.require_property("user", "lastName");
    ///
    /// let props = reqs.get_requirements("user").unwrap();
    /// assert_eq!(props.len(), 2);
    /// ```
    pub fn require_property(&mut self, alias: &str, property: &str) {
        // If already marked as wildcard, no need to track individual properties
        if self.wildcard_aliases.contains(alias) {
            return;
        }

        self.required_properties
            .entry(alias.to_string())
            .or_default()
            .insert(property.to_string());
    }

    /// Mark that an alias requires ALL properties (wildcard)
    ///
    /// This happens when we see patterns like:
    /// - `RETURN friend` (without property access)
    /// - `RETURN friend.*` (explicit wildcard)
    ///
    /// # Arguments
    /// * `alias` - The alias name
    ///
    /// # Example
    ///
    /// ```rust
    /// # use clickgraph::query_planner::analyzer::property_requirements::PropertyRequirements;
    /// let mut reqs = PropertyRequirements::new();
    /// reqs.require_all("friend");
    ///
    /// assert!(reqs.requires_all("friend"));
    /// assert!(reqs.get_requirements("friend").is_none());
    /// ```
    pub fn require_all(&mut self, alias: &str) {
        self.wildcard_aliases.insert(alias.to_string());
        // Remove specific property requirements since we need all anyway
        self.required_properties.remove(alias);
    }

    /// Get the set of required properties for an alias
    ///
    /// # Returns
    /// - `Some(&HashSet<String>)` - Specific properties required
    /// - `None` - All properties required (wildcard) OR no requirements known
    ///
    /// # Example
    ///
    /// ```rust
    /// # use clickgraph::query_planner::analyzer::property_requirements::PropertyRequirements;
    /// let mut reqs = PropertyRequirements::new();
    /// reqs.require_property("user", "name");
    ///
    /// let props = reqs.get_requirements("user").unwrap();
    /// assert!(props.contains("name"));
    ///
    /// // Wildcard returns None
    /// reqs.require_all("product");
    /// assert!(reqs.get_requirements("product").is_none());
    /// ```
    pub fn get_requirements(&self, alias: &str) -> Option<&HashSet<String>> {
        // If marked as wildcard, return None (need all properties)
        if self.wildcard_aliases.contains(alias) {
            return None;
        }

        // Return specific requirements if any
        self.required_properties.get(alias)
    }

    /// Check if an alias requires all properties (wildcard)
    ///
    /// # Example
    ///
    /// ```rust
    /// # use clickgraph::query_planner::analyzer::property_requirements::PropertyRequirements;
    /// let mut reqs = PropertyRequirements::new();
    /// reqs.require_all("friend");
    ///
    /// assert!(reqs.requires_all("friend"));
    /// assert!(!reqs.requires_all("other"));
    /// ```
    pub fn requires_all(&self, alias: &str) -> bool {
        self.wildcard_aliases.contains(alias)
    }

    /// Merge requirements from another PropertyRequirements
    ///
    /// This is used when combining requirements from multiple query branches
    /// or multiple usage sites of the same alias.
    ///
    /// # Logic
    /// - If either has wildcard for an alias, result has wildcard
    /// - Otherwise, union of specific properties
    ///
    /// # Example
    ///
    /// ```rust
    /// # use clickgraph::query_planner::analyzer::property_requirements::PropertyRequirements;
    /// let mut reqs1 = PropertyRequirements::new();
    /// reqs1.require_property("user", "name");
    ///
    /// let mut reqs2 = PropertyRequirements::new();
    /// reqs2.require_property("user", "email");
    ///
    /// reqs1.merge(&reqs2);
    ///
    /// let props = reqs1.get_requirements("user").unwrap();
    /// assert_eq!(props.len(), 2);
    /// assert!(props.contains("name"));
    /// assert!(props.contains("email"));
    /// ```
    pub fn merge(&mut self, other: &PropertyRequirements) {
        // Merge wildcard aliases
        for alias in &other.wildcard_aliases {
            self.require_all(alias);
        }

        // Merge specific property requirements
        for (alias, props) in &other.required_properties {
            // Skip if this alias is already wildcard in self
            if self.wildcard_aliases.contains(alias) {
                continue;
            }

            // If other has wildcard for this alias, mark as wildcard
            if other.wildcard_aliases.contains(alias) {
                self.require_all(alias);
                continue;
            }

            // Otherwise, union the properties
            let self_props = self
                .required_properties
                .entry(alias.clone())
                .or_default();

            for prop in props {
                self_props.insert(prop.clone());
            }
        }
    }

    /// Get all aliases that have requirements (either specific or wildcard)
    ///
    /// # Example
    ///
    /// ```rust
    /// # use clickgraph::query_planner::analyzer::property_requirements::PropertyRequirements;
    /// let mut reqs = PropertyRequirements::new();
    /// reqs.require_property("user", "name");
    /// reqs.require_all("product");
    ///
    /// let aliases: Vec<_> = reqs.aliases().collect();
    /// assert_eq!(aliases.len(), 2);
    /// ```
    pub fn aliases(&self) -> impl Iterator<Item = &String> {
        self.required_properties
            .keys()
            .chain(self.wildcard_aliases.iter())
    }

    /// Check if there are any requirements at all
    ///
    /// # Example
    ///
    /// ```rust
    /// # use clickgraph::query_planner::analyzer::property_requirements::PropertyRequirements;
    /// let reqs = PropertyRequirements::new();
    /// assert!(reqs.is_empty());
    ///
    /// let mut reqs2 = PropertyRequirements::new();
    /// reqs2.require_property("user", "name");
    /// assert!(!reqs2.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.required_properties.is_empty() && self.wildcard_aliases.is_empty()
    }

    /// Get the total number of aliases with requirements
    ///
    /// # Example
    ///
    /// ```rust
    /// # use clickgraph::query_planner::analyzer::property_requirements::PropertyRequirements;
    /// let mut reqs = PropertyRequirements::new();
    /// reqs.require_property("user", "name");
    /// reqs.require_property("user", "email");
    /// reqs.require_all("product");
    ///
    /// assert_eq!(reqs.len(), 2); // user and product
    /// ```
    pub fn len(&self) -> usize {
        let mut aliases = HashSet::new();
        aliases.extend(self.required_properties.keys().cloned());
        aliases.extend(self.wildcard_aliases.iter().cloned());
        aliases.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_is_empty() {
        let reqs = PropertyRequirements::new();
        assert!(reqs.is_empty());
        assert_eq!(reqs.len(), 0);
    }

    #[test]
    fn test_require_property_single() {
        let mut reqs = PropertyRequirements::new();
        reqs.require_property("user", "firstName");

        let props = reqs.get_requirements("user").unwrap();
        assert_eq!(props.len(), 1);
        assert!(props.contains("firstName"));
    }

    #[test]
    fn test_require_property_multiple() {
        let mut reqs = PropertyRequirements::new();
        reqs.require_property("user", "firstName");
        reqs.require_property("user", "lastName");
        reqs.require_property("user", "email");

        let props = reqs.get_requirements("user").unwrap();
        assert_eq!(props.len(), 3);
        assert!(props.contains("firstName"));
        assert!(props.contains("lastName"));
        assert!(props.contains("email"));
    }

    #[test]
    fn test_require_property_multiple_aliases() {
        let mut reqs = PropertyRequirements::new();
        reqs.require_property("user", "name");
        reqs.require_property("product", "price");

        assert_eq!(reqs.len(), 2);

        let user_props = reqs.get_requirements("user").unwrap();
        assert_eq!(user_props.len(), 1);
        assert!(user_props.contains("name"));

        let product_props = reqs.get_requirements("product").unwrap();
        assert_eq!(product_props.len(), 1);
        assert!(product_props.contains("price"));
    }

    #[test]
    fn test_require_all() {
        let mut reqs = PropertyRequirements::new();
        reqs.require_all("friend");

        assert!(reqs.requires_all("friend"));
        assert!(reqs.get_requirements("friend").is_none());
    }

    #[test]
    fn test_require_all_overrides_specific() {
        let mut reqs = PropertyRequirements::new();
        reqs.require_property("user", "name");
        reqs.require_property("user", "email");

        // Mark as wildcard - should override specific properties
        reqs.require_all("user");

        assert!(reqs.requires_all("user"));
        assert!(reqs.get_requirements("user").is_none());
    }

    #[test]
    fn test_require_property_after_wildcard() {
        let mut reqs = PropertyRequirements::new();
        reqs.require_all("user");

        // Adding specific property after wildcard should be no-op
        reqs.require_property("user", "name");

        assert!(reqs.requires_all("user"));
        assert!(reqs.get_requirements("user").is_none());
    }

    #[test]
    fn test_get_requirements_unknown_alias() {
        let reqs = PropertyRequirements::new();
        assert!(reqs.get_requirements("unknown").is_none());
    }

    #[test]
    fn test_merge_properties() {
        let mut reqs1 = PropertyRequirements::new();
        reqs1.require_property("user", "firstName");

        let mut reqs2 = PropertyRequirements::new();
        reqs2.require_property("user", "lastName");

        reqs1.merge(&reqs2);

        let props = reqs1.get_requirements("user").unwrap();
        assert_eq!(props.len(), 2);
        assert!(props.contains("firstName"));
        assert!(props.contains("lastName"));
    }

    #[test]
    fn test_merge_wildcards() {
        let mut reqs1 = PropertyRequirements::new();
        reqs1.require_property("user", "name");

        let mut reqs2 = PropertyRequirements::new();
        reqs2.require_all("user");

        reqs1.merge(&reqs2);

        assert!(reqs1.requires_all("user"));
        assert!(reqs1.get_requirements("user").is_none());
    }

    #[test]
    fn test_merge_different_aliases() {
        let mut reqs1 = PropertyRequirements::new();
        reqs1.require_property("user", "name");

        let mut reqs2 = PropertyRequirements::new();
        reqs2.require_property("product", "price");

        reqs1.merge(&reqs2);

        assert_eq!(reqs1.len(), 2);
        assert!(reqs1.get_requirements("user").unwrap().contains("name"));
        assert!(reqs1.get_requirements("product").unwrap().contains("price"));
    }

    #[test]
    fn test_merge_empty() {
        let mut reqs1 = PropertyRequirements::new();
        reqs1.require_property("user", "name");

        let reqs2 = PropertyRequirements::new();
        reqs1.merge(&reqs2);

        // Should be unchanged
        assert_eq!(reqs1.len(), 1);
        assert!(reqs1.get_requirements("user").unwrap().contains("name"));
    }

    #[test]
    fn test_aliases_iterator() {
        let mut reqs = PropertyRequirements::new();
        reqs.require_property("user", "name");
        reqs.require_all("product");
        reqs.require_property("order", "id");

        let mut aliases: Vec<_> = reqs.aliases().map(|s| s.as_str()).collect();
        aliases.sort();

        assert_eq!(aliases, vec!["order", "product", "user"]);
    }

    #[test]
    fn test_len_with_duplicates() {
        let mut reqs = PropertyRequirements::new();
        reqs.require_property("user", "name");
        reqs.require_property("user", "email");

        // Should count user as one alias even with multiple properties
        assert_eq!(reqs.len(), 1);
    }
}
