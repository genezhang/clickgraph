//! Typed Variable System for ClickGraph
//!
//! This module provides a unified type system for tracking variables throughout
//! the query planning pipeline. It solves the "variable amnesia" problem where
//! semantic type information was lost at WITH clause boundaries.
//!
//! # Architecture
//!
//! The core abstraction is [`TypedVariable`], which captures:
//! - The variable's semantic type (node, relationship, scalar, path, collection)
//! - Where the variable originated (MATCH, CTE, parameter, UNWIND)
//! - Type-specific metadata (labels, properties, bounds, etc.)
//!
//! # Design Principles
//!
//! 1. **Single Source of Truth**: All variable type information lives in `PlanCtx.variables`
//! 2. **Explicit Types**: Variables carry type from definition to rendering
//! 3. **CTE Awareness**: Variables remember which CTE they came from
//! 4. **Unified Lookup**: Single `lookup()` method replaces multiple resolution paths
//!
//! # Example
//!
//! ```text
//! MATCH (a:User)-[r:FOLLOWS]->(b:User)
//! WITH a, count(b) as follower_count
//! RETURN a.name, follower_count
//! ```
//!
//! After WITH processing:
//! - `a` → NodeVariable { labels: ["User"], source: Cte { cte_name: "with_cte_1" } }
//! - `follower_count` → ScalarVariable { source: Cte { cte_name: "with_cte_1" } }
//!
//! # Version History
//!
//! - v1.0 (Jan 2026): Initial implementation per design doc

use std::collections::HashMap;

// ============================================================================
// Core Enums
// ============================================================================

/// Where a variable originated - crucial for rendering decisions
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VariableSource {
    /// Variable comes from a MATCH pattern (base table)
    Match,

    /// Variable comes from a CTE (WITH clause export)
    Cte {
        /// The CTE name (e.g., "with_a_cte_1")
        cte_name: String,
    },

    /// Variable comes from a query parameter ($param)
    Parameter,

    /// Variable comes from UNWIND clause
    Unwind {
        /// The source array expression being unwound
        source_array: String,
    },
}

/// What type of elements a collection contains
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CollectionElementType {
    /// Collection of node variables (e.g., nodes(path))
    Nodes,

    /// Collection of relationship variables (e.g., relationships(path))
    Relationships,

    /// Collection of scalar values (e.g., [1, 2, 3])
    Scalars,

    /// Collection of paths
    Paths,

    /// Element type not yet determined
    Unknown,
}

// ============================================================================
// Variable Type Structs
// ============================================================================

/// A node variable from a MATCH pattern or CTE
///
/// # Example
/// ```text
/// MATCH (a:User) → NodeVariable { labels: ["User"], source: Match }
/// WITH a        → NodeVariable { labels: ["User"], source: Cte { cte_name: "..." } }
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct NodeVariable {
    /// Node labels (e.g., ["User"], ["Person", "Employee"])
    /// Empty vector means unlabeled node (inferred from pattern)
    pub labels: Vec<String>,

    /// Where this variable was defined
    pub source: VariableSource,

    /// Which properties are known to be accessed
    /// Populated during analysis for optimization
    pub accessed_properties: Vec<String>,
}

impl NodeVariable {
    /// Create a new node variable from a MATCH pattern
    pub fn from_match(labels: Vec<String>) -> Self {
        Self {
            labels,
            source: VariableSource::Match,
            accessed_properties: Vec::new(),
        }
    }

    /// Create a node variable exported through a CTE
    pub fn from_cte(labels: Vec<String>, cte_name: String) -> Self {
        Self {
            labels,
            source: VariableSource::Cte { cte_name },
            accessed_properties: Vec::new(),
        }
    }

    /// Get the primary label (first one) if available
    pub fn primary_label(&self) -> Option<&str> {
        self.labels.first().map(|s| s.as_str())
    }

    /// Check if this node has a specific label
    pub fn has_label(&self, label: &str) -> bool {
        self.labels.iter().any(|l| l == label)
    }
}

/// A relationship variable from a MATCH pattern or CTE
///
/// # Example
/// ```text
/// MATCH (a)-[r:FOLLOWS]->(b) → RelVariable { rel_types: ["FOLLOWS"], ... }
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct RelVariable {
    /// Relationship types (e.g., ["FOLLOWS"], ["KNOWS", "FRIENDS_WITH"])
    /// Multiple types indicate OR semantics: [:KNOWS|FRIENDS_WITH]
    pub rel_types: Vec<String>,

    /// Where this variable was defined
    pub source: VariableSource,

    /// Label of the source (from) node - for polymorphic resolution
    pub from_node_label: Option<String>,

    /// Label of the target (to) node - for polymorphic resolution
    pub to_node_label: Option<String>,

    /// Which properties are known to be accessed
    pub accessed_properties: Vec<String>,

    /// Relationship direction: "Outgoing" (->), "Incoming" (<-), or "Either" (--)
    /// Used by Bolt transformer to determine if start/end nodes should be swapped
    pub direction: Option<String>,
}

impl RelVariable {
    /// Create a new relationship variable from a MATCH pattern
    pub fn from_match(
        rel_types: Vec<String>,
        from_node_label: Option<String>,
        to_node_label: Option<String>,
    ) -> Self {
        Self {
            rel_types,
            source: VariableSource::Match,
            from_node_label,
            to_node_label,
            accessed_properties: Vec::new(),
            direction: None,
        }
    }

    /// Create a relationship variable exported through a CTE
    pub fn from_cte(
        rel_types: Vec<String>,
        cte_name: String,
        from_node_label: Option<String>,
        to_node_label: Option<String>,
    ) -> Self {
        Self {
            rel_types,
            source: VariableSource::Cte { cte_name },
            from_node_label,
            to_node_label,
            accessed_properties: Vec::new(),
            direction: None,
        }
    }

    /// Get the primary relationship type (first one) if available
    pub fn primary_type(&self) -> Option<&str> {
        self.rel_types.first().map(|s| s.as_str())
    }

    /// Check if this relationship has a specific type
    pub fn has_type(&self, rel_type: &str) -> bool {
        self.rel_types.iter().any(|t| t == rel_type)
    }
}

/// A scalar variable (aggregation result, literal, expression)
///
/// # Example
/// ```text
/// WITH count(b) as follower_count → ScalarVariable { ... }
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct ScalarVariable {
    /// Where this variable was defined
    pub source: VariableSource,

    /// The inferred or declared data type (for future type checking)
    /// e.g., "Int64", "String", "Float64", "Boolean"
    pub data_type: Option<String>,
}

impl ScalarVariable {
    /// Create a scalar variable from a CTE (most common case)
    pub fn from_cte(cte_name: String) -> Self {
        Self {
            source: VariableSource::Cte { cte_name },
            data_type: None,
        }
    }

    /// Create a scalar from a parameter
    pub fn from_parameter() -> Self {
        Self {
            source: VariableSource::Parameter,
            data_type: None,
        }
    }

    /// Create a scalar from UNWIND
    pub fn from_unwind(source_array: String) -> Self {
        Self {
            source: VariableSource::Unwind { source_array },
            data_type: None,
        }
    }

    /// Set the inferred data type
    pub fn with_data_type(mut self, data_type: impl Into<String>) -> Self {
        self.data_type = Some(data_type.into());
        self
    }
}

/// A path variable from a path pattern assignment
///
/// # Example
/// ```text
/// MATCH p = (a)-[*1..3]->(b) → PathVariable { ... }
/// MATCH p = shortestPath((a)-[*]->(b)) → PathVariable { is_shortest_path: true, ... }
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct PathVariable {
    /// Where this variable was defined
    pub source: VariableSource,

    /// Alias of the start node (if known)
    pub start_node: Option<String>,

    /// Alias of the end node (if known)
    pub end_node: Option<String>,

    /// Alias of the relationship pattern (if named)
    pub relationship: Option<String>,

    /// Whether this is a shortest path pattern
    pub is_shortest_path: bool,

    /// Length bounds for variable-length patterns
    /// (min_hops, max_hops) - None means unbounded
    pub length_bounds: Option<(Option<u32>, Option<u32>)>,
}

impl PathVariable {
    /// Create a path variable from a MATCH pattern
    pub fn from_match(
        start_node: Option<String>,
        end_node: Option<String>,
        relationship: Option<String>,
        length_bounds: Option<(Option<u32>, Option<u32>)>,
    ) -> Self {
        Self {
            source: VariableSource::Match,
            start_node,
            end_node,
            relationship,
            is_shortest_path: false,
            length_bounds,
        }
    }

    /// Create a shortest path variable
    pub fn shortest_path(
        start_node: Option<String>,
        end_node: Option<String>,
        relationship: Option<String>,
        length_bounds: Option<(Option<u32>, Option<u32>)>,
    ) -> Self {
        Self {
            source: VariableSource::Match,
            start_node,
            end_node,
            relationship,
            is_shortest_path: true,
            length_bounds,
        }
    }

    /// Create a path variable exported through a CTE
    pub fn from_cte(cte_name: String) -> Self {
        Self {
            source: VariableSource::Cte { cte_name },
            start_node: None,
            end_node: None,
            relationship: None,
            is_shortest_path: false,
            length_bounds: None,
        }
    }
}

/// A collection variable (from list expressions, nodes(), relationships())
///
/// # Example
/// ```text
/// WITH nodes(p) as path_nodes → CollectionVariable { element_type: Nodes, ... }
/// WITH [1, 2, 3] as numbers  → CollectionVariable { element_type: Scalars, ... }
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct CollectionVariable {
    /// Where this variable was defined
    pub source: VariableSource,

    /// What type of elements the collection contains
    pub element_type: CollectionElementType,
}

impl CollectionVariable {
    /// Create a collection variable from a CTE
    pub fn from_cte(cte_name: String, element_type: CollectionElementType) -> Self {
        Self {
            source: VariableSource::Cte { cte_name },
            element_type,
        }
    }

    /// Create a collection from UNWIND source
    pub fn from_unwind(source_array: String, element_type: CollectionElementType) -> Self {
        Self {
            source: VariableSource::Unwind { source_array },
            element_type,
        }
    }
}

// ============================================================================
// TypedVariable Enum
// ============================================================================

/// The core abstraction - a variable with its semantic type
///
/// This enum captures all information needed to correctly resolve and render
/// a variable reference anywhere in the query.
#[derive(Debug, Clone, PartialEq)]
pub enum TypedVariable {
    /// A node variable (from MATCH pattern or CTE)
    Node(NodeVariable),

    /// A relationship variable (from MATCH pattern or CTE)
    Relationship(RelVariable),

    /// A scalar variable (aggregation, literal, expression)
    Scalar(ScalarVariable),

    /// A path variable (from path pattern assignment)
    Path(PathVariable),

    /// A collection variable (list, nodes(), relationships())
    Collection(CollectionVariable),
}

impl TypedVariable {
    // ========================================================================
    // Factory Methods
    // ========================================================================

    /// Create a node variable from MATCH
    pub fn node_from_match(labels: Vec<String>) -> Self {
        TypedVariable::Node(NodeVariable::from_match(labels))
    }

    /// Create a node variable from CTE
    pub fn node_from_cte(labels: Vec<String>, cte_name: String) -> Self {
        TypedVariable::Node(NodeVariable::from_cte(labels, cte_name))
    }

    /// Create a relationship variable from MATCH
    pub fn rel_from_match(
        rel_types: Vec<String>,
        from_node_label: Option<String>,
        to_node_label: Option<String>,
    ) -> Self {
        TypedVariable::Relationship(RelVariable::from_match(
            rel_types,
            from_node_label,
            to_node_label,
        ))
    }

    /// Create a relationship variable from CTE
    pub fn rel_from_cte(
        rel_types: Vec<String>,
        cte_name: String,
        from_node_label: Option<String>,
        to_node_label: Option<String>,
    ) -> Self {
        TypedVariable::Relationship(RelVariable::from_cte(
            rel_types,
            cte_name,
            from_node_label,
            to_node_label,
        ))
    }

    /// Create a scalar variable from CTE
    pub fn scalar_from_cte(cte_name: String) -> Self {
        TypedVariable::Scalar(ScalarVariable::from_cte(cte_name))
    }

    /// Create a scalar variable from parameter
    pub fn scalar_from_parameter() -> Self {
        TypedVariable::Scalar(ScalarVariable::from_parameter())
    }

    /// Create a path variable from MATCH
    pub fn path_from_match(
        start_node: Option<String>,
        end_node: Option<String>,
        relationship: Option<String>,
        length_bounds: Option<(Option<u32>, Option<u32>)>,
    ) -> Self {
        TypedVariable::Path(PathVariable::from_match(
            start_node,
            end_node,
            relationship,
            length_bounds,
        ))
    }

    /// Create a shortest path variable
    pub fn shortest_path(
        start_node: Option<String>,
        end_node: Option<String>,
        relationship: Option<String>,
        length_bounds: Option<(Option<u32>, Option<u32>)>,
    ) -> Self {
        TypedVariable::Path(PathVariable::shortest_path(
            start_node,
            end_node,
            relationship,
            length_bounds,
        ))
    }

    /// Create a collection variable from CTE
    pub fn collection_from_cte(cte_name: String, element_type: CollectionElementType) -> Self {
        TypedVariable::Collection(CollectionVariable::from_cte(cte_name, element_type))
    }

    // ========================================================================
    // Type Checking Methods
    // ========================================================================

    /// Check if this is a node variable
    pub fn is_node(&self) -> bool {
        matches!(self, TypedVariable::Node(_))
    }

    /// Check if this is a relationship variable
    pub fn is_relationship(&self) -> bool {
        matches!(self, TypedVariable::Relationship(_))
    }

    /// Check if this is a scalar variable
    pub fn is_scalar(&self) -> bool {
        matches!(self, TypedVariable::Scalar(_))
    }

    /// Check if this is a path variable
    pub fn is_path(&self) -> bool {
        matches!(self, TypedVariable::Path(_))
    }

    /// Check if this is a collection variable
    pub fn is_collection(&self) -> bool {
        matches!(self, TypedVariable::Collection(_))
    }

    /// Check if this is an entity (node or relationship)
    pub fn is_entity(&self) -> bool {
        self.is_node() || self.is_relationship()
    }

    // ========================================================================
    // Accessor Methods
    // ========================================================================

    /// Get the variable source
    pub fn source(&self) -> &VariableSource {
        match self {
            TypedVariable::Node(n) => &n.source,
            TypedVariable::Relationship(r) => &r.source,
            TypedVariable::Scalar(s) => &s.source,
            TypedVariable::Path(p) => &p.source,
            TypedVariable::Collection(c) => &c.source,
        }
    }

    /// Get CTE name if this variable came from a CTE
    pub fn cte_name(&self) -> Option<&str> {
        match self.source() {
            VariableSource::Cte { cte_name } => Some(cte_name.as_str()),
            _ => None,
        }
    }

    /// Check if this variable came from a CTE
    pub fn is_from_cte(&self) -> bool {
        matches!(self.source(), VariableSource::Cte { .. })
    }

    /// Check if this variable came from a MATCH pattern
    pub fn is_from_match(&self) -> bool {
        matches!(self.source(), VariableSource::Match)
    }

    /// Get as NodeVariable if it is one
    pub fn as_node(&self) -> Option<&NodeVariable> {
        match self {
            TypedVariable::Node(n) => Some(n),
            _ => None,
        }
    }

    /// Get as RelVariable if it is one
    pub fn as_relationship(&self) -> Option<&RelVariable> {
        match self {
            TypedVariable::Relationship(r) => Some(r),
            _ => None,
        }
    }

    /// Get as ScalarVariable if it is one
    pub fn as_scalar(&self) -> Option<&ScalarVariable> {
        match self {
            TypedVariable::Scalar(s) => Some(s),
            _ => None,
        }
    }

    /// Get as PathVariable if it is one
    pub fn as_path(&self) -> Option<&PathVariable> {
        match self {
            TypedVariable::Path(p) => Some(p),
            _ => None,
        }
    }

    /// Get as CollectionVariable if it is one
    pub fn as_collection(&self) -> Option<&CollectionVariable> {
        match self {
            TypedVariable::Collection(c) => Some(c),
            _ => None,
        }
    }

    /// Get labels/types for entity variables (node or relationship)
    ///
    /// Returns None for scalar, path, and collection variables.
    pub fn labels_or_types(&self) -> Option<&Vec<String>> {
        match self {
            TypedVariable::Node(n) => Some(&n.labels),
            TypedVariable::Relationship(r) => Some(&r.rel_types),
            _ => None,
        }
    }

    /// Get primary label/type for entity variables
    ///
    /// Returns the first label (for nodes) or first relationship type.
    pub fn primary_label_or_type(&self) -> Option<&str> {
        match self {
            TypedVariable::Node(n) => n.primary_label(),
            TypedVariable::Relationship(r) => r.primary_type(),
            _ => None,
        }
    }

    // ========================================================================
    // Mutation Methods
    // ========================================================================

    /// Convert this variable to a CTE-sourced version
    ///
    /// Used when exporting a variable through WITH clause.
    pub fn to_cte_sourced(&self, cte_name: String) -> Self {
        match self {
            TypedVariable::Node(n) => {
                TypedVariable::Node(NodeVariable::from_cte(n.labels.clone(), cte_name))
            }
            TypedVariable::Relationship(r) => TypedVariable::Relationship(RelVariable::from_cte(
                r.rel_types.clone(),
                cte_name,
                r.from_node_label.clone(),
                r.to_node_label.clone(),
            )),
            TypedVariable::Scalar(_) => TypedVariable::Scalar(ScalarVariable::from_cte(cte_name)),
            TypedVariable::Path(_) => TypedVariable::Path(PathVariable::from_cte(cte_name)),
            TypedVariable::Collection(c) => TypedVariable::Collection(
                CollectionVariable::from_cte(cte_name, c.element_type.clone()),
            ),
        }
    }
}

// ============================================================================
// Variable Registry
// ============================================================================

/// A registry for typed variables in a scope
///
/// This provides the `define_*` and `lookup` API described in the design doc.
/// It's designed to be embedded in `PlanCtx`.
#[derive(Debug, Clone, Default)]
pub struct VariableRegistry {
    /// Map from variable name to typed variable
    variables: HashMap<String, TypedVariable>,
}

impl VariableRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            variables: HashMap::new(),
        }
    }

    // ========================================================================
    // Define Methods (populate during MATCH/WITH processing)
    // ========================================================================

    /// Define a node variable
    ///
    /// # Arguments
    /// * `name` - Variable name (e.g., "a", "user")
    /// * `labels` - Node labels (e.g., ["User"])
    /// * `source` - Where the variable came from
    pub fn define_node(
        &mut self,
        name: impl Into<String>,
        labels: Vec<String>,
        source: VariableSource,
    ) {
        let var = match source {
            VariableSource::Match => TypedVariable::node_from_match(labels),
            VariableSource::Cte { cte_name } => TypedVariable::node_from_cte(labels, cte_name),
            _ => TypedVariable::node_from_match(labels), // Fallback
        };
        self.variables.insert(name.into(), var);
    }

    /// Update labels on an existing node variable (e.g., after type inference)
    pub fn update_node_labels(&mut self, name: &str, labels: Vec<String>) {
        if let Some(TypedVariable::Node(node_var)) = self.variables.get_mut(name) {
            node_var.labels = labels;
        }
    }

    /// Define a relationship variable
    ///
    /// # Arguments
    /// * `name` - Variable name (e.g., "r", "follows")
    /// * `rel_types` - Relationship types (e.g., ["FOLLOWS"])
    /// * `from_label` - Label of source node (for polymorphic resolution)
    /// * `to_label` - Label of target node (for polymorphic resolution)
    /// * `source` - Where the variable came from
    pub fn define_relationship(
        &mut self,
        name: impl Into<String>,
        rel_types: Vec<String>,
        from_label: Option<String>,
        to_label: Option<String>,
        source: VariableSource,
    ) {
        let var = match source {
            VariableSource::Match => TypedVariable::rel_from_match(rel_types, from_label, to_label),
            VariableSource::Cte { cte_name } => {
                TypedVariable::rel_from_cte(rel_types, cte_name, from_label, to_label)
            }
            _ => TypedVariable::rel_from_match(rel_types, from_label, to_label), // Fallback
        };
        self.variables.insert(name.into(), var);
    }

    /// Define a relationship variable with direction information
    ///
    /// # Arguments
    /// * `name` - Variable name (e.g., "r")
    /// * `rel_types` - Relationship types (e.g., ["FOLLOWS"])
    /// * `from_label` - Label of source node (for polymorphic resolution)
    /// * `to_label` - Label of target node (for polymorphic resolution)
    /// * `source` - Where the variable came from
    /// * `direction` - "Outgoing", "Incoming", or "Either"
    pub fn define_relationship_with_direction(
        &mut self,
        name: impl Into<String>,
        rel_types: Vec<String>,
        from_label: Option<String>,
        to_label: Option<String>,
        source: VariableSource,
        direction: Option<String>,
    ) {
        let mut var = match source {
            VariableSource::Match => TypedVariable::rel_from_match(rel_types, from_label, to_label),
            VariableSource::Cte { cte_name } => {
                TypedVariable::rel_from_cte(rel_types, cte_name, from_label, to_label)
            }
            _ => TypedVariable::rel_from_match(rel_types, from_label, to_label), // Fallback
        };
        // Set direction on the RelVariable
        if let TypedVariable::Relationship(ref mut rel_var) = var {
            rel_var.direction = direction;
        }
        self.variables.insert(name.into(), var);
    }

    /// Define a scalar variable
    ///
    /// # Arguments
    /// * `name` - Variable name (e.g., "count", "total")
    /// * `source` - Where the variable came from
    pub fn define_scalar(&mut self, name: impl Into<String>, source: VariableSource) {
        let var = match source {
            VariableSource::Cte { cte_name } => TypedVariable::scalar_from_cte(cte_name),
            VariableSource::Parameter => TypedVariable::scalar_from_parameter(),
            VariableSource::Unwind { source_array } => {
                TypedVariable::Scalar(ScalarVariable::from_unwind(source_array))
            }
            VariableSource::Match => TypedVariable::Scalar(ScalarVariable {
                source: VariableSource::Match,
                data_type: None,
            }),
        };
        self.variables.insert(name.into(), var);
    }

    /// Define a path variable
    ///
    /// # Arguments
    /// * `name` - Variable name (e.g., "p", "path")
    /// * `start_node` - Alias of start node
    /// * `end_node` - Alias of end node
    /// * `relationship` - Alias of relationship pattern
    /// * `length_bounds` - (min, max) hops for variable-length patterns
    /// * `is_shortest_path` - Whether this is a shortest path pattern
    pub fn define_path(
        &mut self,
        name: impl Into<String>,
        start_node: Option<String>,
        end_node: Option<String>,
        relationship: Option<String>,
        length_bounds: Option<(Option<u32>, Option<u32>)>,
        is_shortest_path: bool,
    ) {
        let var = if is_shortest_path {
            TypedVariable::shortest_path(start_node, end_node, relationship, length_bounds)
        } else {
            TypedVariable::path_from_match(start_node, end_node, relationship, length_bounds)
        };
        self.variables.insert(name.into(), var);
    }

    /// Define a collection variable
    ///
    /// # Arguments
    /// * `name` - Variable name (e.g., "nodes", "items")
    /// * `element_type` - What type of elements the collection contains
    /// * `source` - Where the variable came from
    pub fn define_collection(
        &mut self,
        name: impl Into<String>,
        element_type: CollectionElementType,
        source: VariableSource,
    ) {
        let var = match source {
            VariableSource::Cte { cte_name } => {
                TypedVariable::collection_from_cte(cte_name, element_type)
            }
            VariableSource::Unwind { source_array } => TypedVariable::Collection(
                CollectionVariable::from_unwind(source_array, element_type),
            ),
            _ => TypedVariable::Collection(CollectionVariable {
                source,
                element_type,
            }),
        };
        self.variables.insert(name.into(), var);
    }

    /// Define a variable directly (for advanced use cases)
    pub fn define(&mut self, name: impl Into<String>, var: TypedVariable) {
        self.variables.insert(name.into(), var);
    }

    // ========================================================================
    // Lookup Methods
    // ========================================================================

    /// Look up a variable by name
    ///
    /// This is THE single lookup method - replaces multiple resolution paths.
    pub fn lookup(&self, name: &str) -> Option<&TypedVariable> {
        self.variables.get(name)
    }

    /// Look up a variable, returning owned clone
    pub fn lookup_cloned(&self, name: &str) -> Option<TypedVariable> {
        self.variables.get(name).cloned()
    }

    /// Check if a variable exists
    pub fn contains(&self, name: &str) -> bool {
        self.variables.contains_key(name)
    }

    /// Get all variable names
    pub fn names(&self) -> impl Iterator<Item = &String> {
        self.variables.keys()
    }

    /// Get all variables
    pub fn iter(&self) -> impl Iterator<Item = (&String, &TypedVariable)> {
        self.variables.iter()
    }

    /// Get the number of variables
    pub fn len(&self) -> usize {
        self.variables.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.variables.is_empty()
    }

    // ========================================================================
    // Scope Management
    // ========================================================================

    /// Export variables through a WITH clause
    ///
    /// Creates new CTE-sourced versions of the specified variables.
    ///
    /// # Arguments
    /// * `exported_names` - Names of variables being exported
    /// * `cte_name` - The CTE name for the WITH clause
    ///
    /// # Returns
    /// A new registry containing only the exported variables with CTE source
    pub fn export_to_cte(&self, exported_names: &[&str], cte_name: &str) -> Self {
        let mut new_registry = Self::new();

        for name in exported_names {
            if let Some(var) = self.variables.get(*name) {
                let cte_var = var.to_cte_sourced(cte_name.to_string());
                new_registry.variables.insert((*name).to_string(), cte_var);
            }
        }

        new_registry
    }

    /// Merge another registry into this one
    ///
    /// Used for combining scopes (e.g., after WITH processing).
    /// Existing variables are NOT overwritten.
    pub fn merge(&mut self, other: &VariableRegistry) {
        for (name, var) in &other.variables {
            if !self.variables.contains_key(name) {
                self.variables.insert(name.clone(), var.clone());
            }
        }
    }

    /// Merge with overwrite
    ///
    /// Existing variables ARE overwritten.
    pub fn merge_overwrite(&mut self, other: &VariableRegistry) {
        for (name, var) in &other.variables {
            self.variables.insert(name.clone(), var.clone());
        }
    }

    /// Clear all variables
    pub fn clear(&mut self) {
        self.variables.clear();
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_variable_creation() {
        let var = TypedVariable::node_from_match(vec!["User".to_string()]);
        assert!(var.is_node());
        assert!(!var.is_relationship());
        assert!(var.is_from_match());
        assert!(!var.is_from_cte());

        let node = var.as_node().unwrap();
        assert_eq!(node.labels, vec!["User"]);
        assert_eq!(node.primary_label(), Some("User"));
    }

    #[test]
    fn test_relationship_variable_creation() {
        let var = TypedVariable::rel_from_match(
            vec!["FOLLOWS".to_string()],
            Some("User".to_string()),
            Some("User".to_string()),
        );
        assert!(var.is_relationship());
        assert!(var.is_entity());

        let rel = var.as_relationship().unwrap();
        assert_eq!(rel.rel_types, vec!["FOLLOWS"]);
        assert_eq!(rel.from_node_label, Some("User".to_string()));
    }

    #[test]
    fn test_scalar_variable_creation() {
        let var = TypedVariable::scalar_from_cte("with_cte_1".to_string());
        assert!(var.is_scalar());
        assert!(!var.is_entity());
        assert!(var.is_from_cte());
        assert_eq!(var.cte_name(), Some("with_cte_1"));
    }

    #[test]
    fn test_path_variable_creation() {
        let var = TypedVariable::path_from_match(
            Some("a".to_string()),
            Some("b".to_string()),
            Some("r".to_string()),
            Some((Some(1), Some(3))),
        );
        assert!(var.is_path());

        let path = var.as_path().unwrap();
        assert_eq!(path.start_node, Some("a".to_string()));
        assert_eq!(path.length_bounds, Some((Some(1), Some(3))));
        assert!(!path.is_shortest_path);
    }

    #[test]
    fn test_collection_variable_creation() {
        let var = TypedVariable::collection_from_cte(
            "with_cte_1".to_string(),
            CollectionElementType::Nodes,
        );
        assert!(var.is_collection());

        let coll = var.as_collection().unwrap();
        assert_eq!(coll.element_type, CollectionElementType::Nodes);
    }

    #[test]
    fn test_to_cte_sourced() {
        let var = TypedVariable::node_from_match(vec!["User".to_string()]);
        let cte_var = var.to_cte_sourced("with_a_cte_1".to_string());

        assert!(cte_var.is_node());
        assert!(cte_var.is_from_cte());
        assert_eq!(cte_var.cte_name(), Some("with_a_cte_1"));

        // Labels should be preserved
        let node = cte_var.as_node().unwrap();
        assert_eq!(node.labels, vec!["User"]);
    }

    #[test]
    fn test_registry_define_and_lookup() {
        let mut registry = VariableRegistry::new();

        registry.define_node("a", vec!["User".to_string()], VariableSource::Match);
        registry.define_relationship(
            "r",
            vec!["FOLLOWS".to_string()],
            Some("User".to_string()),
            Some("User".to_string()),
            VariableSource::Match,
        );
        registry.define_scalar(
            "count",
            VariableSource::Cte {
                cte_name: "with_cte_1".to_string(),
            },
        );

        assert!(registry.contains("a"));
        assert!(registry.contains("r"));
        assert!(registry.contains("count"));
        assert!(!registry.contains("x"));

        let a = registry.lookup("a").unwrap();
        assert!(a.is_node());

        let r = registry.lookup("r").unwrap();
        assert!(r.is_relationship());

        let count = registry.lookup("count").unwrap();
        assert!(count.is_scalar());
    }

    #[test]
    fn test_registry_export_to_cte() {
        let mut registry = VariableRegistry::new();
        registry.define_node("a", vec!["User".to_string()], VariableSource::Match);
        registry.define_node("b", vec!["User".to_string()], VariableSource::Match);

        // Export only 'a'
        let exported = registry.export_to_cte(&["a"], "with_a_cte_1");

        assert!(exported.contains("a"));
        assert!(!exported.contains("b"));

        let a = exported.lookup("a").unwrap();
        assert!(a.is_from_cte());
        assert_eq!(a.cte_name(), Some("with_a_cte_1"));

        // Labels preserved
        let node = a.as_node().unwrap();
        assert_eq!(node.labels, vec!["User"]);
    }

    #[test]
    fn test_registry_merge() {
        let mut reg1 = VariableRegistry::new();
        reg1.define_node("a", vec!["User".to_string()], VariableSource::Match);

        let mut reg2 = VariableRegistry::new();
        reg2.define_node("b", vec!["Post".to_string()], VariableSource::Match);

        reg1.merge(&reg2);

        assert!(reg1.contains("a"));
        assert!(reg1.contains("b"));
    }

    #[test]
    fn test_labels_or_types() {
        let node = TypedVariable::node_from_match(vec!["User".to_string(), "Admin".to_string()]);
        assert_eq!(
            node.labels_or_types(),
            Some(&vec!["User".to_string(), "Admin".to_string()])
        );
        assert_eq!(node.primary_label_or_type(), Some("User"));

        let rel = TypedVariable::rel_from_match(
            vec!["KNOWS".to_string(), "FOLLOWS".to_string()],
            None,
            None,
        );
        assert_eq!(
            rel.labels_or_types(),
            Some(&vec!["KNOWS".to_string(), "FOLLOWS".to_string()])
        );
        assert_eq!(rel.primary_label_or_type(), Some("KNOWS"));

        let scalar = TypedVariable::scalar_from_cte("cte".to_string());
        assert_eq!(scalar.labels_or_types(), None);
        assert_eq!(scalar.primary_label_or_type(), None);
    }
}
