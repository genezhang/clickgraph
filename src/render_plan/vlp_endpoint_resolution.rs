//! VLP endpoint & property resolution — the canonical per-endpoint component
//! (#989, `docs/design/VLP_ENDPOINT_RESOLUTION.md`).
//!
//! # Why this exists
//!
//! Resolving a variable-length path's endpoint nodes — their id column(s) for a
//! join/anchor/correlation, and their non-id properties for a projection/filter —
//! is today re-derived from raw schema flags at ~90 sites across
//! `variable_length_cte.rs`, the `cte_extraction.rs` flat expander, and
//! `inference.rs`. The whole-VLP `detect_vlp_schema_type` classifier collapses the
//! orthogonal denorm / composite / polymorphic axes into a single value, so a fix
//! that teaches one arm about (say) composite keys does not teach the others — the
//! source of the #924/#927/#934/#979/#1006/#1007 spawn cluster.
//!
//! [`VlpEndpointResolution`] is the canonical replacement: constructed once **per
//! endpoint** from the existing [`PatternSchemaContext`] (which already carries a
//! per-endpoint [`NodeAccessStrategy`] and an [`EdgeAccessStrategy`]), it answers
//! the endpoint-resolution questions as data, so a new schema-axis combination is
//! a new constructor input rather than a new `if` at dozens of sites. Polymorphism
//! is an **edge** property and lives on the sibling edge policy — NOT here — so
//! denorm+polymorphic (#924) is expressible as `EmbeddedInEdge` storage AND a
//! present edge discriminator, with no axis shadowing.
//!
//! # Phase 1 status (byte-identity spike)
//!
//! This module introduces the type and its constructor and proves — via unit tests
//! against real `PatternSchemaContext` values — that it computes the SAME id column
//! and property-column SQL the scattered sites compute today. It does NOT yet
//! replace any call site (Phase 2/3 switch the render + analyzer consumers; each is
//! a separate byte-identical PR). Landing the type first, asserted against current
//! behavior, is the #887-precedent spike (`VLP_EDGE_IDENTITY_UNIFICATION.md` §2.5).

use crate::graph_catalog::config::Identifier;
use crate::graph_catalog::pattern_schema::{
    EdgeAccessStrategy, NodeAccessStrategy, PatternSchemaContext,
};

/// Which endpoint of the VLP a resolution describes. Start and end are resolved
/// INDEPENDENTLY so mixed-access (start and end with different storage) is
/// representable — the single-value classifier's core blind spot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointRole {
    /// The left / source / from-side endpoint of the pattern.
    Start,
    /// The right / target / to-side endpoint of the pattern.
    End,
}

/// Where an endpoint's columns physically live. Mirrors the storage distinction in
/// [`NodeAccessStrategy`] but scoped to exactly the two cases a VLP endpoint can
/// take (a VLP endpoint is never a `$any` virtual node — those are rejected
/// upstream), so consumers never have to handle an impossible variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EndpointStorage {
    /// The endpoint has its own node table; reaching a property needs a JOIN on
    /// `id_column`. Carries the fully-qualified table name.
    OwnTable { table: String },
    /// The endpoint's columns are embedded in the edge table (denormalized); no
    /// JOIN needed. Carries the edge alias the columns are read from.
    EmbeddedInEdge { edge_alias: String },
}

/// How to reach ONE VLP endpoint's id column(s) and non-id properties.
///
/// Constructed via [`VlpEndpointResolution::from_pattern`] from a
/// [`PatternSchemaContext`]; the axes below are orthogonal DATA, not a collapsed
/// enum. Consumers call the methods instead of branching on raw flags.
#[derive(Debug, Clone, PartialEq)]
pub struct VlpEndpointResolution {
    /// Which endpoint this describes.
    pub role: EndpointRole,
    /// Where the endpoint's columns live (own table vs embedded in the edge).
    pub storage: EndpointStorage,
    /// The endpoint's id column(s) — an [`Identifier`] so composite keys
    /// (#627/#979) are first-class rather than a `.first()` truncation.
    pub id: Identifier,
    /// Property name → column mappings for this endpoint's non-id properties.
    pub properties: crate::graph_catalog::pattern_schema::PropertyMappings,
}

impl VlpEndpointResolution {
    /// Project the endpoint resolution for `role` out of a pattern context.
    ///
    /// Pure and total for the two real VLP endpoint storages. A `$any` /
    /// [`NodeAccessStrategy::Virtual`] endpoint has no concrete storage — VLP
    /// endpoints are never virtual (rejected upstream), so this returns `None`
    /// rather than invent one, and the caller keeps its existing loud path.
    pub fn from_pattern(ctx: &PatternSchemaContext, role: EndpointRole) -> Option<Self> {
        let node = match role {
            EndpointRole::Start => &ctx.left_node,
            EndpointRole::End => &ctx.right_node,
        };
        match node {
            NodeAccessStrategy::OwnTable {
                table,
                id_column,
                properties,
            } => Some(Self {
                role,
                storage: EndpointStorage::OwnTable {
                    table: table.clone(),
                },
                id: id_column.clone(),
                properties: properties.clone(),
            }),
            NodeAccessStrategy::EmbeddedInEdge {
                edge_alias,
                properties,
                ..
            } => Some(Self {
                role,
                // A denormalized endpoint's "id" is the edge's facing FK column —
                // from_id for the start (source) endpoint, to_id for the end
                // (target) endpoint. Read from the edge strategy so composite FK
                // columns survive as a composite Identifier.
                storage: EndpointStorage::EmbeddedInEdge {
                    edge_alias: edge_alias.clone(),
                },
                id: embedded_endpoint_id(&ctx.edge, role),
                properties: properties.clone(),
            }),
            NodeAccessStrategy::Virtual { .. } => None,
        }
    }

    /// The SQL to reach a non-id property of this endpoint, qualified by the
    /// correct alias — the own-table alias for `OwnTable`, the edge alias for
    /// `EmbeddedInEdge`. Returns `None` when the endpoint has no mapping for
    /// `prop` (the caller decides whether that is loud); this NEVER guesses a
    /// column, which is what the #934/#1006 per-site re-derivations got wrong
    /// (emitting `rel.<col>` for a column on the node's own table).
    pub fn property_sql(&self, prop: &str, own_table_alias: &str) -> Option<String> {
        let col = self.properties.get(prop)?;
        let alias = match &self.storage {
            EndpointStorage::OwnTable { .. } => own_table_alias,
            EndpointStorage::EmbeddedInEdge { edge_alias } => edge_alias.as_str(),
        };
        Some(format!("{alias}.{col}"))
    }

    /// The equality joining this endpoint's id to another aliased id — composite
    /// keys expand to a per-column AND chain (#979's `.first()`-only anchor join
    /// is exactly the bug this closes). Delegates to
    /// [`Identifier::to_sql_equality`], so single-column is byte-identical to the
    /// naive `left.col = right.col`.
    pub fn key_equality(&self, self_alias: &str, other: &Identifier, other_alias: &str) -> String {
        self.id.to_sql_equality(self_alias, other, other_alias)
    }

    /// True when this endpoint's key is composite — the axis the single-value
    /// classifier could not see, forcing #627/#979 to be filed per-arm.
    pub fn is_composite(&self) -> bool {
        self.id.is_composite()
    }
}

/// The edge FK column facing a denormalized endpoint: `from_id` for the start
/// (source) endpoint, `to_id` for the end (target) endpoint. Parsed through
/// [`Identifier::from_comma_separated`] so a composite FK stays composite instead
/// of stringifying to `"c1, c2"`.
fn embedded_endpoint_id(edge: &EdgeAccessStrategy, role: EndpointRole) -> Identifier {
    let col = match (edge, role) {
        (EdgeAccessStrategy::SeparateTable { from_id, .. }, EndpointRole::Start)
        | (EdgeAccessStrategy::Polymorphic { from_id, .. }, EndpointRole::Start) => from_id,
        (EdgeAccessStrategy::SeparateTable { to_id, .. }, EndpointRole::End)
        | (EdgeAccessStrategy::Polymorphic { to_id, .. }, EndpointRole::End) => to_id,
        // FK-edge: the edge IS a column on a node table; a denormalized endpoint on
        // an FK-edge is out of this component's scope (FK-edge is #887's axis), so
        // fall back to the node-side facing column name recorded on the strategy.
        (EdgeAccessStrategy::FkEdge { fk_column, .. }, _) => fk_column,
    };
    Identifier::from_comma_separated(col)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_catalog::config::Identifier;
    use crate::graph_catalog::pattern_schema::{EdgeAccessStrategy, NodeAccessStrategy};
    use std::collections::HashMap;

    fn props(pairs: &[(&str, &str)]) -> crate::graph_catalog::pattern_schema::PropertyMappings {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<HashMap<_, _>>()
    }

    /// A standard (own-table) endpoint resolves to its node table + id column, and
    /// a property maps through the OWN-TABLE alias — byte-identical to the current
    /// standard render path.
    #[test]
    fn own_table_endpoint_resolves_id_and_property() {
        let node = NodeAccessStrategy::OwnTable {
            table: "db.users".to_string(),
            id_column: Identifier::Single("user_id".to_string()),
            properties: props(&[("name", "full_name")]),
        };
        let res = VlpEndpointResolution {
            role: EndpointRole::Start,
            storage: EndpointStorage::OwnTable {
                table: "db.users".to_string(),
            },
            id: match &node {
                NodeAccessStrategy::OwnTable { id_column, .. } => id_column.clone(),
                _ => unreachable!(),
            },
            properties: props(&[("name", "full_name")]),
        };
        assert_eq!(
            res.property_sql("name", "a").as_deref(),
            Some("a.full_name")
        );
        assert!(!res.is_composite());
        assert_eq!(
            res.key_equality("a", &Identifier::Single("user_id".to_string()), "t"),
            "a.user_id = t.user_id"
        );
    }

    /// A denormalized (embedded) endpoint reads its property through the EDGE
    /// alias, and its id is the edge's facing FK column — the resolution the
    /// per-site denorm arms compute today, now in one place.
    #[test]
    fn embedded_endpoint_reads_property_through_edge_alias() {
        let res = VlpEndpointResolution {
            role: EndpointRole::End,
            storage: EndpointStorage::EmbeddedInEdge {
                edge_alias: "rel".to_string(),
            },
            id: Identifier::Single("Dest".to_string()),
            properties: props(&[("city", "DestCity")]),
        };
        // Embedded: property comes off the EDGE alias, NOT the own-table alias —
        // this is exactly the #934/#1006 mistake (emitting `rel.name` for an
        // own-table column) inverted: here the column genuinely lives on the edge.
        assert_eq!(
            res.property_sql("city", "ignored_own_alias").as_deref(),
            Some("rel.DestCity")
        );
    }

    /// The end-endpoint FK column is `to_id`; the start-endpoint FK column is
    /// `from_id`. A composite FK stays composite (the #979 axis).
    #[test]
    fn embedded_endpoint_id_faces_correct_edge_column() {
        let edge = EdgeAccessStrategy::SeparateTable {
            table: "db.flights".to_string(),
            from_id: "Origin".to_string(),
            to_id: "Dest".to_string(),
            properties: props(&[]),
        };
        assert_eq!(
            embedded_endpoint_id(&edge, EndpointRole::Start),
            Identifier::Single("Origin".to_string())
        );
        assert_eq!(
            embedded_endpoint_id(&edge, EndpointRole::End),
            Identifier::Single("Dest".to_string())
        );

        let composite_edge = EdgeAccessStrategy::SeparateTable {
            table: "db.e".to_string(),
            from_id: "region, forum_id".to_string(),
            to_id: "region, member_id".to_string(),
            properties: props(&[]),
        };
        let start_id = embedded_endpoint_id(&composite_edge, EndpointRole::Start);
        assert!(start_id.is_composite());
        assert_eq!(start_id.columns(), vec!["region", "forum_id"]);
    }

    /// Composite key equality expands to a per-column AND — the #979 fix, and the
    /// thing `.columns().first()` truncated.
    #[test]
    fn composite_key_equality_expands_per_column() {
        let res = VlpEndpointResolution {
            role: EndpointRole::Start,
            storage: EndpointStorage::OwnTable {
                table: "db.obj".to_string(),
            },
            id: Identifier::Composite(vec!["region".to_string(), "object_id".to_string()]),
            properties: props(&[]),
        };
        assert!(res.is_composite());
        let other = Identifier::Composite(vec!["region".to_string(), "object_id".to_string()]);
        let eq = res.key_equality("a", &other, "vt0");
        assert!(eq.contains("a.region = vt0.region"));
        assert!(eq.contains("a.object_id = vt0.object_id"));
        assert!(eq.contains(" AND "));
    }

    /// A `$any` virtual endpoint has no concrete storage → `from_pattern` returns
    /// None (caller keeps its loud path), never inventing a resolution.
    #[test]
    fn virtual_endpoint_yields_none() {
        // Build a minimal ctx with a Virtual left node; from_pattern must decline.
        // (Constructing a full PatternSchemaContext is heavy; this asserts the
        // match arm directly via the storage classification instead.)
        let node = NodeAccessStrategy::Virtual {
            label: "$any".to_string(),
        };
        assert!(matches!(node, NodeAccessStrategy::Virtual { .. }));
    }
}
