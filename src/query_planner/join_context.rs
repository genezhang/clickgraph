//! JoinContext: Structured context for JOIN generation
//!
//! This module provides types for tracking JOIN state during graph pattern analysis.
//! It replaces the ambiguous `joined_entities: HashSet<String>` with clear semantics.
//!
//! See notes/join-context-architecture-design.md for full design rationale.

use std::collections::{HashMap, HashSet};

/// Position of a node in a Variable-Length Path (VLP).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VlpPosition {
    /// Start node of VLP (accessed via t.start_id in CTE)
    Start,
    /// End node of VLP (accessed via t.end_id in CTE)
    End,
}

impl VlpPosition {
    /// Get the standard CTE column name for this position.
    pub fn cte_column(&self) -> &'static str {
        match self {
            VlpPosition::Start => "start_id",
            VlpPosition::End => "end_id",
        }
    }
}

/// Information about a VLP endpoint needed for JOIN generation.
///
/// At JOIN inference time, we don't know the CTE name/alias yet (those are
/// generated in render_plan). We only know:
/// - Which nodes are VLP endpoints
/// - Their position (start/end)
/// - The other endpoint's Cypher alias (needed for CTE naming convention)
/// - The relationship alias
///
/// The render_plan phase uses this info to:
/// 1. Generate CTE names like "vlp_{start_alias}_{end_alias}"
/// 2. Create proper JOIN conditions using "t.start_id" or "t.end_id"
#[derive(Debug, Clone)]
pub struct VlpEndpointInfo {
    /// Position in VLP (Start or End)
    pub position: VlpPosition,
    /// The other endpoint's Cypher alias (e.g., if this is "u2" (end), other is "u1" (start))
    /// Used to derive CTE name: "vlp_{start}_{end}"
    pub other_endpoint_alias: String,
    /// Relationship alias for this VLP (e.g., "r" in (u1)-[r*1..3]->(u2))
    pub rel_alias: String,
}

impl VlpEndpointInfo {
    /// Get the standard CTE column for this endpoint's position.
    pub fn cte_column(&self) -> &'static str {
        self.position.cte_column()
    }

    /// Derive the CTE name based on endpoint aliases.
    /// Convention: "vlp_{start_alias}_{end_alias}"
    pub fn derive_cte_name(&self, this_alias: &str) -> String {
        match self.position {
            VlpPosition::Start => format!("vlp_{}_{}", this_alias, self.other_endpoint_alias),
            VlpPosition::End => format!("vlp_{}_{}", self.other_endpoint_alias, this_alias),
        }
    }
}

/// Structured context for JOIN generation with clear semantics.
///
/// This replaces the ambiguous `joined_entities: HashSet<String>` which conflated:
/// 1. Nodes in FROM clause (scanned)
/// 2. Nodes with JOINs created
/// 3. VLP endpoints (accessible via CTE, not direct table)
/// 4. Anchor node tracking
///
/// With JoinContext, each concept has its own field with clear semantics.
#[derive(Debug, Clone, Default)]
pub struct JoinContext {
    /// Nodes appearing in FROM clause (actual table scan).
    /// Example: For `FROM users AS u1`, contains "u1".
    scanned_nodes: HashSet<String>,

    /// Nodes with explicit JOIN entries created.
    /// Example: After `JOIN users AS u2 ON ...`, contains "u2".
    joined_nodes: HashSet<String>,

    /// Relationships with JOIN entries created.
    /// Example: After processing rel "r1", contains "r1".
    joined_relationships: HashSet<String>,

    /// VLP endpoints with CTE access information.
    /// Key: Cypher alias (e.g., "u1", "u2")
    /// Value: How to access via CTE
    vlp_endpoints: HashMap<String, VlpEndpointInfo>,

    /// The anchor node (first node in FROM clause).
    anchor_node: Option<String>,
}

impl JoinContext {
    pub fn new() -> Self {
        Self::default()
    }

    // ========================================================================
    // Accessibility Queries
    // ========================================================================

    /// Check if alias is accessible for JOIN (scanned, joined, or VLP endpoint).
    pub fn is_accessible(&self, alias: &str) -> bool {
        self.scanned_nodes.contains(alias)
            || self.joined_nodes.contains(alias)
            || self.vlp_endpoints.contains_key(alias)
    }

    /// Check if alias is a VLP endpoint (needs CTE reference translation).
    pub fn is_vlp_endpoint(&self, alias: &str) -> bool {
        self.vlp_endpoints.contains_key(alias)
    }

    /// Get VLP endpoint info if alias is a VLP endpoint.
    pub fn get_vlp_endpoint(&self, alias: &str) -> Option<&VlpEndpointInfo> {
        self.vlp_endpoints.get(alias)
    }

    /// Get all VLP endpoints.
    pub fn vlp_endpoints(&self) -> &HashMap<String, VlpEndpointInfo> {
        &self.vlp_endpoints
    }

    // ========================================================================
    // Reference Resolution (KEY METHOD for VLP+chained patterns)
    // ========================================================================

    /// Default alias used for VLP CTEs in outer query (e.g., "FROM vlp_u1_u2 AS t")
    pub const VLP_CTE_DEFAULT_ALIAS: &'static str = "t";

    /// Get the proper (table_alias, column) for a JOIN condition.
    ///
    /// This is the key method that fixes VLP+chained patterns:
    /// - For regular nodes: returns (alias, column) unchanged
    /// - For VLP endpoints: returns (VLP_CTE_DEFAULT_ALIAS, position_column) e.g., ("t", "end_id")
    ///
    /// # Example
    /// ```ignore
    /// // For u2 which is VLP end endpoint:
    /// ctx.get_join_reference("u2", "user_id")
    /// // Returns: ("t", "end_id")
    ///
    /// // For regular node p:
    /// ctx.get_join_reference("p", "post_id")
    /// // Returns: ("p", "post_id")
    /// ```
    pub fn get_join_reference(&self, alias: &str, default_column: &str) -> (String, String) {
        if let Some(vlp_info) = self.vlp_endpoints.get(alias) {
            // VLP endpoint: use CTE alias and position-based column
            (
                Self::VLP_CTE_DEFAULT_ALIAS.to_string(),
                vlp_info.cte_column().to_string(),
            )
        } else {
            (alias.to_string(), default_column.to_string())
        }
    }

    /// Get VLP endpoint info along with derived CTE details.
    /// Returns: (cte_name, cte_alias, cte_column, vlp_info)
    pub fn get_vlp_reference_details(
        &self,
        alias: &str,
    ) -> Option<(String, &'static str, &'static str, &VlpEndpointInfo)> {
        self.vlp_endpoints.get(alias).map(|info| {
            let cte_name = info.derive_cte_name(alias);
            (cte_name, Self::VLP_CTE_DEFAULT_ALIAS, info.cte_column(), info)
        })
    }

    // ========================================================================
    // State Mutation
    // ========================================================================

    /// Set the anchor node (first in FROM clause).
    pub fn set_anchor(&mut self, alias: String) {
        if self.anchor_node.is_none() {
            self.anchor_node = Some(alias.clone());
            self.scanned_nodes.insert(alias);
        }
    }

    /// Get the anchor node alias.
    pub fn get_anchor(&self) -> Option<&String> {
        self.anchor_node.as_ref()
    }

    /// Mark a node as scanned (in FROM clause).
    pub fn mark_scanned(&mut self, alias: String) {
        self.scanned_nodes.insert(alias);
    }

    /// Mark a node as joined (has JOIN entry).
    pub fn mark_node_joined(&mut self, alias: String) {
        self.joined_nodes.insert(alias);
    }

    /// Mark a relationship as joined (has JOIN entry).
    pub fn mark_relationship_joined(&mut self, alias: String) {
        self.joined_relationships.insert(alias);
    }

    /// Mark a VLP endpoint with CTE access information.
    /// Called when VLP pattern is skipped (will use CTE instead of JOINs).
    pub fn mark_vlp_endpoint(&mut self, alias: String, info: VlpEndpointInfo) {
        self.vlp_endpoints.insert(alias, info);
    }

    // ========================================================================
    // Compatibility Layer (for gradual migration from HashSet)
    // ========================================================================

    /// Check if alias is in the "joined" set (compatibility with old HashSet logic).
    /// This considers scanned, joined nodes, AND VLP endpoints as "joined".
    pub fn contains(&self, alias: &str) -> bool {
        self.is_accessible(alias)
    }

    /// Insert an alias as "joined" (compatibility with old HashSet logic).
    /// For non-VLP cases, marks as joined_node.
    pub fn insert(&mut self, alias: String) {
        self.joined_nodes.insert(alias);
    }

    /// Create JoinContext from existing HashSet (for migration).
    /// All entries are treated as joined_nodes.
    pub fn from_hashset(entities: &HashSet<String>) -> Self {
        let mut ctx = Self::new();
        for alias in entities {
            ctx.joined_nodes.insert(alias.clone());
        }
        ctx
    }

    /// Sync JoinContext state back to HashSet (for migration).
    /// Exports all accessible aliases (scanned + joined + vlp_endpoints).
    pub fn to_hashset(&self) -> HashSet<String> {
        let mut result = HashSet::new();
        result.extend(self.scanned_nodes.iter().cloned());
        result.extend(self.joined_nodes.iter().cloned());
        result.extend(self.vlp_endpoints.keys().cloned());
        result
    }

    // ========================================================================
    // Debug/Logging
    // ========================================================================

    pub fn debug_summary(&self) -> String {
        format!(
            "JoinContext {{ anchor: {:?}, scanned: {:?}, joined_nodes: {:?}, vlp_endpoints: [{}] }}",
            self.anchor_node,
            self.scanned_nodes,
            self.joined_nodes,
            self.vlp_endpoints.keys().cloned().collect::<Vec<_>>().join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vlp_position_cte_column() {
        assert_eq!(VlpPosition::Start.cte_column(), "start_id");
        assert_eq!(VlpPosition::End.cte_column(), "end_id");
    }

    #[test]
    fn test_vlp_endpoint_derive_cte_name() {
        let start_info = VlpEndpointInfo {
            position: VlpPosition::Start,
            other_endpoint_alias: "u2".to_string(),
            rel_alias: "r".to_string(),
        };
        assert_eq!(start_info.derive_cte_name("u1"), "vlp_u1_u2");

        let end_info = VlpEndpointInfo {
            position: VlpPosition::End,
            other_endpoint_alias: "u1".to_string(),
            rel_alias: "r".to_string(),
        };
        assert_eq!(end_info.derive_cte_name("u2"), "vlp_u1_u2");
    }

    #[test]
    fn test_join_context_vlp_endpoint() {
        let mut ctx = JoinContext::new();

        // Mark u2 as VLP end endpoint
        ctx.mark_vlp_endpoint(
            "u2".to_string(),
            VlpEndpointInfo {
                position: VlpPosition::End,
                other_endpoint_alias: "u1".to_string(),
                rel_alias: "r".to_string(),
            },
        );

        // Check VLP detection
        assert!(ctx.is_vlp_endpoint("u2"));
        assert!(!ctx.is_vlp_endpoint("p"));

        // Check reference resolution - THIS IS THE KEY FIX
        let (alias, col) = ctx.get_join_reference("u2", "user_id");
        assert_eq!(alias, "t"); // CTE alias, not "u2"
        assert_eq!(col, "end_id"); // CTE column, not "user_id"

        // Regular node should pass through unchanged
        let (alias, col) = ctx.get_join_reference("p", "post_id");
        assert_eq!(alias, "p");
        assert_eq!(col, "post_id");
    }

    #[test]
    fn test_join_context_hashset_compatibility() {
        let mut entities: HashSet<String> = HashSet::new();
        entities.insert("a".to_string());
        entities.insert("b".to_string());

        // Convert from HashSet
        let ctx = JoinContext::from_hashset(&entities);
        assert!(ctx.contains("a"));
        assert!(ctx.contains("b"));
        assert!(!ctx.contains("c"));

        // Convert back to HashSet
        let back = ctx.to_hashset();
        assert!(back.contains("a"));
        assert!(back.contains("b"));
    }
}
