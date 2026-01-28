//! Table context for query planning
//!
//! `TableCtx` tracks information about a single table/alias in a query,
//! including labels, properties, filters, and projections.

use std::fmt;

use crate::query_planner::{
    logical_expr::{LogicalExpr, Property},
    logical_plan::ProjectionItem,
    plan_ctx::errors::PlanCtxError,
};

/// Context for a single table/alias in a query.
///
/// Tracks:
/// - Labels (for nodes) or types (for relationships)
/// - Properties being accessed
/// - Filter predicates
/// - Projection items
/// - CTE references (for WITH clause exports)
#[derive(Debug, PartialEq, Clone)]
pub struct TableCtx {
    alias: String,
    labels: Option<Vec<String>>,
    properties: Vec<Property>,
    filter_predicates: Vec<LogicalExpr>,
    projection_items: Vec<ProjectionItem>,
    is_rel: bool,
    explicit_alias: bool,
    /// If Some, this alias references a CTE instead of a base table
    /// Format: "with_a_cte1" or "with_a_b_cte2"
    cte_reference: Option<String>,
    /// For relationships: the label of the connected from_node (source)
    /// Used to resolve polymorphic relationships (e.g., Person LIKES Message)
    from_node_label: Option<String>,
    /// For relationships: the label of the connected to_node (target)
    /// Used to resolve polymorphic relationships
    to_node_label: Option<String>,
}

impl TableCtx {
    // ========================================================================
    // Constructors
    // ========================================================================

    /// Build a new TableCtx with the given parameters.
    pub fn build(
        alias: String,
        labels: Option<Vec<String>>,
        properties: Vec<Property>,
        is_rel: bool,
        explicit_alias: bool,
    ) -> Self {
        TableCtx {
            alias,
            labels,
            properties,
            filter_predicates: vec![],
            projection_items: vec![],
            is_rel,
            explicit_alias,
            cte_reference: None,
            from_node_label: None,
            to_node_label: None,
        }
    }

    /// Create a TableCtx that references a CTE instead of a base table.
    ///
    /// Used when an alias was exported from a WITH clause.
    ///
    /// # Arguments
    /// * `alias` - The alias name (e.g., "u", "tag")
    /// * `cte_name` - The CTE name (e.g., "with_u_cte_1")
    /// * `entity_info` - Optional tuple of (is_rel, labels) from CTE entity type registry
    ///
    /// # Example
    /// ```ignore
    /// // Look up entity info from plan_ctx before calling
    /// let entity_info = plan_ctx.get_cte_entity_type(&cte_name, &alias)
    ///     .map(|(r, l)| (*r, l.clone()));
    /// let table_ctx = TableCtx::new_with_cte_reference(alias, cte_name, entity_info);
    /// ```
    pub fn new_with_cte_reference(
        alias: String,
        cte_name: String,
        entity_info: Option<(bool, Option<Vec<String>>)>,
    ) -> Self {
        let (is_rel, labels) = entity_info.unwrap_or((false, None));

        log::info!(
            "🔧 Creating TableCtx for CTE reference '{}' → '{}': is_rel={}, labels={:?}",
            alias,
            cte_name,
            is_rel,
            labels
        );

        TableCtx {
            alias,
            labels,
            properties: vec![],
            filter_predicates: vec![],
            projection_items: vec![],
            is_rel,
            explicit_alias: true,
            cte_reference: Some(cte_name),
            from_node_label: None,
            to_node_label: None,
        }
    }

    // ========================================================================
    // Type Checking
    // ========================================================================

    /// Check if this is a relationship (edge) context.
    pub fn is_relation(&self) -> bool {
        self.is_rel
    }

    /// Check if this alias was explicitly specified in the query.
    pub fn is_explicit_alias(&self) -> bool {
        self.explicit_alias
    }

    /// Check if this alias references a CTE.
    pub fn is_cte_reference(&self) -> bool {
        self.cte_reference.is_some()
    }

    /// Check if this TableCtx represents a path variable (not a node or relationship).
    ///
    /// Path variables have no label and are not relationships.
    pub fn is_path_variable(&self) -> bool {
        !self.is_rel && self.labels.as_ref().map_or(true, |v| v.is_empty())
    }

    // ========================================================================
    // Label/Type Access
    // ========================================================================

    /// Get the first label as a String, or error if none.
    pub fn get_label_str(&self) -> Result<String, PlanCtxError> {
        self.labels
            .as_ref()
            .and_then(|v| v.first())
            .cloned()
            .ok_or_else(|| {
                if self.is_rel {
                    PlanCtxError::Type {
                        alias: self.alias.clone(),
                    }
                } else {
                    PlanCtxError::Label {
                        alias: self.alias.clone(),
                    }
                }
            })
    }

    /// Get the labels (for nodes) or types (for relationships).
    pub fn get_labels(&self) -> Option<&Vec<String>> {
        self.labels.as_ref()
    }

    /// Get the first label as an Option.
    pub fn get_label_opt(&self) -> Option<String> {
        self.labels.as_ref().and_then(|v| v.first()).cloned()
    }

    /// Set the labels.
    pub fn set_labels(&mut self, labels_opt: Option<Vec<String>>) {
        self.labels = labels_opt;
    }

    // ========================================================================
    // CTE Reference
    // ========================================================================

    /// Get the CTE name if this is a CTE reference.
    pub fn get_cte_name(&self) -> Option<&String> {
        self.cte_reference.as_ref()
    }

    /// Set the CTE reference for this alias.
    pub fn set_cte_reference(&mut self, cte_ref: Option<String>) {
        self.cte_reference = cte_ref;
    }

    // ========================================================================
    // Connected Node Labels (for relationships)
    // ========================================================================

    /// Set the connected node labels for a relationship.
    pub fn set_connected_nodes(&mut self, from: Option<String>, to: Option<String>) {
        self.from_node_label = from;
        self.to_node_label = to;
    }

    /// Get the from_node label for a relationship.
    pub fn get_from_node_label(&self) -> Option<&String> {
        self.from_node_label.as_ref()
    }

    /// Get the to_node label for a relationship.
    pub fn get_to_node_label(&self) -> Option<&String> {
        self.to_node_label.as_ref()
    }

    // ========================================================================
    // Projection Items
    // ========================================================================

    /// Get the projection items.
    pub fn get_projections(&self) -> &Vec<ProjectionItem> {
        &self.projection_items
    }

    /// Set the projection items.
    pub fn set_projections(&mut self, proj_items: Vec<ProjectionItem>) {
        self.projection_items = proj_items;
    }

    /// Insert a projection item if not already present.
    pub fn insert_projection(&mut self, proj_item: ProjectionItem) {
        if !self.projection_items.contains(&proj_item) {
            self.projection_items.push(proj_item);
        }
    }

    /// Append projection items.
    pub fn append_projection(&mut self, proj_items: &mut Vec<ProjectionItem>) {
        self.projection_items.append(proj_items);
    }

    // ========================================================================
    // Filter Predicates
    // ========================================================================

    /// Get the filter predicates.
    pub fn get_filters(&self) -> &Vec<LogicalExpr> {
        &self.filter_predicates
    }

    /// Insert a filter predicate if not already present.
    pub fn insert_filter(&mut self, filter_pred: LogicalExpr) {
        if !self.filter_predicates.contains(&filter_pred) {
            self.filter_predicates.push(filter_pred);
        }
    }

    /// Append filter predicates.
    pub fn append_filters(&mut self, filter_preds: &mut Vec<LogicalExpr>) {
        self.filter_predicates.append(filter_preds);
    }

    /// Clear the filters after they have been applied to a GraphRel.
    ///
    /// This prevents the same filters from being applied multiple times
    /// in multi-hop patterns.
    pub fn clear_filters(&mut self) {
        self.filter_predicates.clear();
    }

    // ========================================================================
    // Properties
    // ========================================================================

    /// Append properties.
    pub fn append_properties(&mut self, mut props: Vec<Property>) {
        self.properties.append(&mut props);
    }

    /// Get and clear properties (takes ownership).
    pub fn get_and_clear_properties(&mut self) -> Vec<Property> {
        std::mem::take(&mut self.properties)
    }
}

// ============================================================================
// Display Implementation
// ============================================================================

impl TableCtx {
    /// Format with indentation for nested display.
    pub(crate) fn fmt_with_indent(&self, f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result {
        let pad = " ".repeat(indent);
        writeln!(f, "{}         labels: {:?}", pad, self.labels)?;
        writeln!(f, "{}         properties: {:?}", pad, self.properties)?;
        writeln!(
            f,
            "{}         filter_predicates: {:?}",
            pad, self.filter_predicates
        )?;
        writeln!(
            f,
            "{}         projection_items: {:?}",
            pad, self.projection_items
        )?;
        writeln!(f, "{}         is_rel: {:?}", pad, self.is_rel)?;
        writeln!(
            f,
            "{}         explicit_alias: {:?}",
            pad, self.explicit_alias
        )?;
        Ok(())
    }
}
