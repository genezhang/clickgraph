//! JoinContext: Structured context for JOIN generation
//!
//! This module provides types for tracking JOIN state during graph pattern analysis.
//! It replaces the ambiguous `joined_entities: HashSet<String>` with clear semantics.
//!
//! See notes/join-context-architecture-design.md for full design rationale.
//!
//! ## ⚠️ VLP NAMING CONVENTIONS - SINGLE SOURCE OF TRUTH
//!
//! All VLP CTE naming conventions are defined as constants in this module:
//! - [`VLP_CTE_FROM_ALIAS`] = "t" (the FROM alias for VLP CTEs)
//! - [`VLP_START_ID_COLUMN`] = "start_id" (column for start node ID)
//! - [`VLP_END_ID_COLUMN`] = "end_id" (column for end node ID)
//!
//! **USAGE**: Import these constants wherever VLP CTEs are generated or referenced:
//! ```ignore
//! use crate::query_planner::join_context::{VLP_CTE_FROM_ALIAS, VLP_START_ID_COLUMN, VLP_END_ID_COLUMN};
//! ```
//!
//! **Files that MUST use these constants**:
//! - `variable_length_cte.rs` - generates the actual CTE SQL
//! - `multi_type_vlp_joins.rs` - generates multi-type VLP CTEs  
//! - `plan_builder_utils.rs` - fallback code for GROUP BY expansion
//! - `select_builder.rs` - SELECT clause generation
//! - `plan_ctx/mod.rs` - planning context

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};

// =============================================================================
// VLP CTE NAMING CONVENTIONS - SINGLE SOURCE OF TRUTH
// =============================================================================

/// Default FROM alias used for VLP CTEs in outer query.
/// Example: `FROM vlp_u1_u2 AS t` - the "t" is this constant.
///
/// All code generating or referencing VLP CTEs MUST use this constant.
pub const VLP_CTE_FROM_ALIAS: &str = "t";

/// Column name for the start node ID in VLP CTEs.
/// Example: `SELECT ... start_node.id AS start_id ...` in recursive CTE.
///
/// All code generating or referencing VLP CTEs MUST use this constant.
pub const VLP_START_ID_COLUMN: &str = "start_id";

/// Column name for the end node ID in VLP CTEs.
/// Example: `SELECT ... end_node.id AS end_id ...` in recursive CTE.
///
/// All code generating or referencing VLP CTEs MUST use this constant.
pub const VLP_END_ID_COLUMN: &str = "end_id";

// =============================================================================

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
    /// Returns [`VLP_START_ID_COLUMN`] or [`VLP_END_ID_COLUMN`].
    pub fn cte_column(&self) -> &'static str {
        match self {
            VlpPosition::Start => VLP_START_ID_COLUMN,
            VlpPosition::End => VLP_END_ID_COLUMN,
        }
    }
}

// =============================================================================
// VLP ALIAS GENERATION - Per-VLP unique outer-query aliases
// =============================================================================

/// Global counter for generating unique VLP CTE outer-query aliases.
/// Each VLP in a query gets a unique alias (t0, t1, t2, ...) to avoid
/// ambiguous references when multiple VLPs exist in the same query.
static VLP_ALIAS_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Generate the next unique VLP CTE alias (t0, t1, t2, ...).
/// Called once per VLP (not per endpoint) — both endpoints of the same VLP share the alias.
pub fn next_vlp_alias() -> String {
    let idx = VLP_ALIAS_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("vt{}", idx)
}

/// Reset the VLP alias counter. Call at the start of each query for deterministic output.
pub fn reset_vlp_alias_counter() {
    VLP_ALIAS_COUNTER.store(0, Ordering::Relaxed);
}

/// Information about a VLP endpoint needed for JOIN generation.
///
/// At JOIN inference time, we don't know the CTE name/alias yet (those are
/// generated in render_plan). We only know:
/// - Which nodes are VLP endpoints
/// - Their position (start/end)
/// - The other endpoint's Cypher alias (needed for CTE naming convention)
/// - The relationship alias
/// - The unique VLP alias for outer-query references
///
/// The render_plan phase uses this info to:
/// 1. Generate CTE names like "vlp_{start_alias}_{end_alias}"
/// 2. Create proper JOIN conditions using "{vlp_alias}.start_id" or "{vlp_alias}.end_id"
#[derive(Debug, Clone)]
pub struct VlpEndpointInfo {
    /// Position in VLP (Start or End)
    pub position: VlpPosition,
    /// The other endpoint's Cypher alias (e.g., if this is "u2" (end), other is "u1" (start))
    /// Used to derive CTE name: "vlp_{start}_{end}"
    pub other_endpoint_alias: String,
    /// Relationship alias for this VLP (e.g., "r" in (u1)-[r*1..3]->(u2))
    pub rel_alias: String,
    /// Unique outer-query alias for this VLP's CTE (e.g., "t0", "t1").
    /// Both endpoints of the same VLP share the same alias.
    /// Used instead of the hardcoded "t" to support multiple VLPs per query.
    pub vlp_alias: String,
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

    /// Backward compatibility alias for [`VLP_CTE_FROM_ALIAS`].
    /// Prefer using the module-level constant directly.
    #[deprecated(since = "0.6.2", note = "Use VLP_CTE_FROM_ALIAS constant instead")]
    pub const VLP_CTE_DEFAULT_ALIAS: &'static str = VLP_CTE_FROM_ALIAS;

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
            // VLP endpoint: use per-VLP unique alias and position-based column
            (
                vlp_info.vlp_alias.clone(),
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
    ) -> Option<(String, String, &'static str, &VlpEndpointInfo)> {
        self.vlp_endpoints.get(alias).map(|info| {
            let cte_name = info.derive_cte_name(alias);
            (cte_name, info.vlp_alias.clone(), info.cte_column(), info)
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
            vlp_alias: "t0".to_string(),
        };
        assert_eq!(start_info.derive_cte_name("u1"), "vlp_u1_u2");

        let end_info = VlpEndpointInfo {
            position: VlpPosition::End,
            other_endpoint_alias: "u1".to_string(),
            rel_alias: "r".to_string(),
            vlp_alias: "t0".to_string(),
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
                vlp_alias: "t0".to_string(),
            },
        );

        // Check VLP detection
        assert!(ctx.is_vlp_endpoint("u2"));
        assert!(!ctx.is_vlp_endpoint("p"));

        // Check reference resolution - uses per-VLP alias
        let (alias, col) = ctx.get_join_reference("u2", "user_id");
        assert_eq!(alias, "t0"); // Per-VLP CTE alias, not "u2"
        assert_eq!(col, "end_id"); // CTE column, not "user_id"

        // Regular node should pass through unchanged
        let (alias, col) = ctx.get_join_reference("p", "post_id");
        assert_eq!(alias, "p");
        assert_eq!(col, "post_id");
    }

    #[test]
    fn test_multiple_vlp_unique_aliases() {
        let mut ctx = JoinContext::new();

        // Mark endpoints from two different VLPs
        ctx.mark_vlp_endpoint(
            "u1".to_string(),
            VlpEndpointInfo {
                position: VlpPosition::Start,
                other_endpoint_alias: "u2".to_string(),
                rel_alias: "r1".to_string(),
                vlp_alias: "t0".to_string(),
            },
        );
        ctx.mark_vlp_endpoint(
            "u2".to_string(),
            VlpEndpointInfo {
                position: VlpPosition::End,
                other_endpoint_alias: "u1".to_string(),
                rel_alias: "r1".to_string(),
                vlp_alias: "t0".to_string(),
            },
        );
        ctx.mark_vlp_endpoint(
            "m1".to_string(),
            VlpEndpointInfo {
                position: VlpPosition::Start,
                other_endpoint_alias: "m2".to_string(),
                rel_alias: "r2".to_string(),
                vlp_alias: "t1".to_string(),
            },
        );
        ctx.mark_vlp_endpoint(
            "m2".to_string(),
            VlpEndpointInfo {
                position: VlpPosition::End,
                other_endpoint_alias: "m1".to_string(),
                rel_alias: "r2".to_string(),
                vlp_alias: "t1".to_string(),
            },
        );

        // VLP 1 endpoints both use "t0"
        let (alias1, _) = ctx.get_join_reference("u1", "id");
        let (alias2, _) = ctx.get_join_reference("u2", "id");
        assert_eq!(alias1, "t0");
        assert_eq!(alias2, "t0");

        // VLP 2 endpoints both use "t1"
        let (alias3, _) = ctx.get_join_reference("m1", "id");
        let (alias4, _) = ctx.get_join_reference("m2", "id");
        assert_eq!(alias3, "t1");
        assert_eq!(alias4, "t1");
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
