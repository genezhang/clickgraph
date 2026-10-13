use crate::graph_catalog::config::Identifier;
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::render_plan::variable_length::VariableLengthSpec;
use crate::render_plan::Cte;
use crate::sql_generator::function_mapper::current_function_mapper;

/// Emit a SQL expression that materializes a node's ID, composite-aware.
///
/// Returns the bare expression (no `AS` clause). Single-column IDs render as
/// `alias.col` (quoted); a composite ID is the dialect's text of the tuple of
/// its columns (`toString(tuple(alias.c1, alias.c2))`), stored in
/// `path_nodes` and used for `has()` cycle checks, which no two IDs share
/// (`('B|1','2')` and `('B','1|2')` would share a pipe-joined text, S8d2c).
fn emit_id_expr(table_alias: &str, id: &Identifier) -> String {
    match id {
        Identifier::Single(col) => format!(
            "{}.{}",
            table_alias,
            crate::clickhouse_query_generator::quote_identifier(col)
        ),
        Identifier::Composite(cols) => {
            let m = current_function_mapper();
            let columns: Vec<String> = cols
                .iter()
                .map(|c| {
                    format!(
                        "{}.{}",
                        table_alias,
                        crate::clickhouse_query_generator::quote_identifier(c)
                    )
                })
                .collect();
            let tuple = format!("{}({})", m.tuple_constructor(), columns.join(", "));
            match m.graph_values() {
                Some(g) => (g.tuple_value_text)(&tuple, cols.len()),
                None => format!("{}({tuple})", m.cast_string()),
            }
        }
    }
}

/// Emit the recursive-CTE cycle-check predicate: `NOT array_contains(vp.path_nodes, id)`.
/// `id_expr` should be the bare ID expression (typically from [`emit_id_expr`]).
fn emit_cycle_check(id_expr: &str) -> String {
    let array_contains = current_function_mapper().array_contains();
    format!("NOT {array_contains}(vp.path_nodes, {id_expr})")
}

/// Emit the recursive-CTE EDGE-uniqueness predicate: `NOT array_contains(vp.path_edges, edge_expr)`.
///
/// Enforces Cypher's default relationship-uniqueness (a path MAY revisit a node
/// but must not reuse the same edge), unlike [`emit_cycle_check`] which enforces
/// the stronger node-uniqueness. `edge_expr` is the edge-identity expression for
/// the hop being added this step (typically from [`spell_edge_identity`]).
fn emit_edge_cycle_check(edge_expr: &str) -> String {
    let array_contains = current_function_mapper().array_contains();
    format!("NOT {array_contains}(vp.path_edges, {edge_expr})")
}

/// Spell `tuple(e1, e2, …)` from pre-qualified `<alias>.<col>` parts. The
/// single spelling for "an edge identity that is a tuple" — shared by the
/// edge_id `Composite`/`None` arms of [`spell_edge_identity`] so the wrapper is
/// spelled in exactly one place. `parts` are already-qualified
/// column references; each caller decides its own quoting/column mapping.
fn spell_tuple_parts(tuple_ctor: &str, parts: Vec<String>) -> String {
    format!("{}({})", tuple_ctor, parts.join(", "))
}

/// Spell the edge-identity value for one hop, reading the edge columns off
/// `rel_alias`: "what makes one edge the same edge", shared by the walk's
/// [`EdgeIdentity`] and `bound_plan::lower::path` (#710: parallel edges
/// sharing one `(from, to)` node pair must not collapse).
///
/// Returns SQL like `tuple(r.from_id, r.to_id)` or `tuple(r.date, r.num, …)`.
/// Shape depends on `edge_id`: a `Single` column emits a bare `rel.col`; a
/// `Composite` key builds a `tuple(...)`; `None` defaults to the `(from, to)`
/// node pair. `map_col` is applied to EVERY referenced column.
pub fn spell_edge_identity(
    tuple_ctor: &str,
    edge_id: &Option<Identifier>,
    rel_alias: &str,
    from_col: &str,
    to_col: &str,
    map_col: impl Fn(&str) -> &str,
) -> String {
    match edge_id {
        Some(Identifier::Single(col)) => format!("{}.{}", rel_alias, map_col(col)),
        Some(Identifier::Composite(cols)) => {
            let tuple_elements: Vec<String> = cols
                .iter()
                .map(|col| format!("{}.{}", rel_alias, map_col(col)))
                .collect();
            spell_tuple_parts(tuple_ctor, tuple_elements)
        }
        None => spell_tuple_parts(
            tuple_ctor,
            vec![
                format!("{}.{}", rel_alias, map_col(from_col)),
                format!("{}.{}", rel_alias, map_col(to_col)),
            ],
        ),
    }
}

/// Empty-but-correctly-typed `path_edges` seed for a CLOSED `*0..N` zero-hop
/// base case (#628; #887 Phase 2b live fix).
///
/// A bare `[]` renders as ClickHouse `Array(Nothing)`, and the recursive CTE
/// engine REJECTS a base arm carrying it when the recursive arm's
/// `arrayConcat` produces `Array(Tuple(...))` for the same column — "Conversion
/// from Tuple(...) to Nothing is not supported" (CANNOT_CONVERT_TYPE), proven
/// live on #887. The old "`Array(Nothing)` is the bottom type, it unifies"
/// premise does NOT hold for recursive-CTE column unification: the zero-hop
/// seed must carry the recursive arm's REAL element type, which is only known
/// to the edge table. So spell a ONE-element identity tuple and take an EMPTY
/// slice of it, via an uncorrelated scalar subquery over the edge source
/// (ClickHouse hoists it — evaluated once, proven live):
///
/// ```sql
/// (SELECT arraySlice([tuple(__seed_edge.f1, __seed_edge.f2)], 1, 0)
///  FROM <edge source> AS __seed_edge LIMIT 1)
/// ```
///
/// All spellings are dialect-routed through the current `FunctionMapper`
/// (`arraySlice`/`slice`, `[...]`/`array(...)`, `tuple`/`struct`), so the
/// Databricks variant stays identically typed. `identity_expr` must be spelled
/// against the `__seed_edge` alias and carry exactly the recursive arm's
/// element shape (via `EdgeIdentity::spell` — the same spelling both arms
/// already share). `edge_source_sql` is the edge relation the identity columns
/// live on (`rel_source()`).
///
/// On an EMPTY edge source the `LIMIT 1` subquery yields NULL rather than an
/// empty typed array — harmless by construction: with no edges there are no
/// cycles, the recursive arm produces no rows, and the outer query reads only
/// `start_id`/`end_id` (never `path_edges`) off the zero-length self rows.
pub fn typed_empty_edges_seed(edge_source_sql: &str, identity_expr: &str) -> String {
    let mapper = current_function_mapper();
    format!(
        "(\n            SELECT {}\n            FROM {} AS __seed_edge\n            LIMIT 1\n        )",
        mapper.array_slice(&mapper.array_literal(identity_expr), "1", Some("0")),
        edge_source_sql
    )
}

/// The edge-identity VALUE of a walk: "what makes one edge the same edge"
/// (the `EdgeIdentity` spine of the #887 unification,
/// `docs/design/VLP_EDGE_IDENTITY_UNIFICATION.md`): the schema `edge_id` (a
/// `Single` column spells a bare `rel.col`; a `Composite` key a `tuple(...)`;
/// `None` the `(from, to)` node pair).
#[derive(Debug, Clone)]
pub enum EdgeIdentity {
    /// Schema-declared `edge_id` (Single → bare column; Composite → tuple),
    /// else the `(from, to)` node pair — read off one hop alias.
    EdgeIdColumns {
        edge_id: Option<Identifier>,
        from_col: String,
        to_col: String,
    },
}

impl EdgeIdentity {
    /// Spell the identity value for ONE hop, reading the edge columns off
    /// `rel_alias` ([`spell_edge_identity`]).
    pub fn spell(&self, tuple_ctor: &str, rel_alias: &str) -> String {
        match self {
            EdgeIdentity::EdgeIdColumns {
                edge_id,
                from_col,
                to_col,
            } => spell_edge_identity(tuple_ctor, edge_id, rel_alias, from_col, to_col, |c| c),
        }
    }
}

/// Whether a VLP recursive walk dedupes on the EDGES it has traversed or the
/// NODES it has visited — the uniqueness *decision* axis of the #887
/// unification (Phase 1–2 of `docs/design/VLP_EDGE_IDENTITY_UNIFICATION.md`).
///
/// EDGE-uniqueness (`NOT has(vp.path_edges, <identity>)`) lets a walk revisit a
/// node and return to its start, so real cycles / parallel edges are counted;
/// NODE-uniqueness (`NOT has(vp.path_nodes, <id>)`) structurally forbids
/// returning to a visited node (kept only for a `*0..0` walk, which has no
/// recursive arm).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniquenessKind {
    Edge,
    Node,
}

/// The single authority for a VLP recursive arm's relationship-uniqueness: the
/// [`UniquenessKind`] decision plus the [`EdgeIdentity`] spelling used when the
/// decision is `Edge`. Replaces the inline `uses_edge_uniqueness` copies (VLC
/// `&self` and CM `&CteGenerationContext`) that kept drifting apart
/// (#806/#628/#710/#808/#606/#978/#980 were each "one site disagreed").
///
/// Constructed from already-resolved PRIMITIVE inputs rather than a
/// `PatternSchemaContext`, because the two historical call sites read
/// `min_hops` / `shortest_path` from different owners (the VLC generator's own
/// fields vs `CteGenerationContext`); each caller passes its own values.
#[derive(Debug, Clone)]
pub struct EdgeUniquenessPolicy {
    kind: UniquenessKind,
    identity: EdgeIdentity,
}

impl EdgeUniquenessPolicy {
    /// Compute the uniqueness decision: EDGE unless the walk has no
    /// recursive arm.
    ///
    /// Cypher's relationship-uniqueness rule makes a `*N..M` match a TRAIL: an edge may not repeat,
    /// but a NODE may — a walk can return to its start through a cycle. That holds for every lower
    /// bound including zero. Until #1230 an OPEN zero-hop `*0..N` kept node-uniqueness ("its base
    /// row has no edge to seed a 1-hop `path_edges` literal"); the zero-hop base now seeds a
    /// typed-empty `path_edges` via `typed_empty_edges_seed` (the closed pattern already relied on
    /// it, #628/#887), so there is no reason left to forbid revisiting a node, and node-uniqueness
    /// silently DROPPED every trail that does (standard: `*0..3` returned 100 rows where 149 is
    /// correct on `social_integration`).
    ///
    /// Node-uniqueness stays where the caller passes `no_recursion`: a zero-hop walk with NO
    /// recursive arm (`*0..0`: nothing to dedupe, and the seed would only drag the edge table in).
    pub fn new(no_recursion: bool, identity: EdgeIdentity) -> Self {
        let is_edge = !no_recursion;
        Self {
            kind: if is_edge {
                UniquenessKind::Edge
            } else {
                UniquenessKind::Node
            },
            identity,
        }
    }

    /// Whether this arm threads/dedupes a `path_edges` column (EDGE-uniqueness).
    pub fn uses_edge_uniqueness(&self) -> bool {
        matches!(self.kind, UniquenessKind::Edge)
    }

    /// The recursive-CTE cycle-check predicate: the edge-identity spelled off
    /// `rel_alias` under EDGE-uniqueness, else the node-id fallback
    /// `node_id_expr`.
    pub fn recursive_cycle_predicate(&self, rel_alias: &str, node_id_expr: &str) -> String {
        match self.kind {
            UniquenessKind::Edge => emit_edge_cycle_check(
                &self
                    .identity
                    .spell(current_function_mapper().tuple_constructor(), rel_alias),
            ),
            UniquenessKind::Node => emit_cycle_check(node_id_expr),
        }
    }
}

/// Emit a dialect-correct array literal. CH: `[a, b]`. Spark: `array(a, b)`.
/// Thin shorthand over `FunctionMapper::array_literal` — VLP code builds
/// these in many places (path_nodes, path_relationships, scalar→array
/// wrappers for arrayConcat).
fn arr(elems: &str) -> String {
    current_function_mapper().array_literal(elems)
}

/// Property to include in the CTE (column name and which node it belongs to)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeProperty {
    pub cypher_alias: String, // "u1" or "u2" - which node this property is for
    pub column_name: String,  // Actual column name in the table (e.g., "full_name")
    pub alias: String,        // Output alias (e.g., "name" or "u1_name")
}

/// Each path's nodes and relationships as values (the bound plan's paths as
/// values, `docs/design/EXPLICIT_SCOPE.md` §4.11, S6c), accumulated like
/// `path_nodes` as `path_node_values` / `path_rel_values` by the standard
/// layout's arms. Empty unless a query returns a path or its lists.
#[derive(Debug, Clone, Default)]
pub struct PathValues {
    /// A node as a value: its SQL over the start node's alias, and over the
    /// end node's.
    pub node: Option<(String, String)>,
    /// A relationship as a value, SQL over the relationship's alias; and the
    /// SQL of an empty list of them (the path of no relationship).
    pub rel: Option<(String, String)>,
    /// A node / relationship as a list's element (a tuple of its columns,
    /// `bound_plan::lower::elements`), as `node` / `rel`: carried as
    /// `path_node_tuples` / `path_rel_tuples`.
    pub node_tuple: Option<(String, String)>,
    pub rel_tuple: Option<(String, String)>,
}

impl PathValues {
    /// The carried columns: (name, a node's (start, end) or a relationship's
    /// (one, empty list) SQL, whether a node's).
    fn columns(&self) -> Vec<(&'static str, &(String, String), bool)> {
        [
            ("path_node_values", &self.node, true),
            ("path_rel_values", &self.rel, false),
            ("path_node_tuples", &self.node_tuple, true),
            ("path_rel_tuples", &self.rel_tuple, false),
        ]
        .into_iter()
        .filter_map(|(c, v, node)| v.as_ref().map(|v| (c, v, node)))
        .collect()
    }
}

/// Drop exact-duplicate `NodeProperty` entries (same alias, column, and output
/// name), preserving first-seen order. A CLOSED variable-length pattern —
/// `(a)-[:R*2..2]->(a)` / `*2..3` — resolves the SAME endpoint property once for
/// each of the (identical) start and end connections, so the caller can hand the
/// generator two byte-identical `NodeProperty` rows. Emitting both projects the
/// same `end_<prop>` column twice → ClickHouse Code 44 (`column ... already
/// exists`). Deduping here is always safe: projecting an identical property
/// triple twice is never valid SQL. (#631)
fn dedup_node_properties(properties: Vec<NodeProperty>) -> Vec<NodeProperty> {
    let mut seen = std::collections::HashSet::new();
    properties
        .into_iter()
        .filter(|p| seen.insert(p.clone()))
        .collect()
}

/// Generates recursive CTE SQL for variable-length path traversal
pub struct VariableLengthCteGenerator<'a> {
    pub schema: &'a GraphSchema, // Schema for constraint compilation and property resolution
    pub spec: VariableLengthSpec,
    pub cte_name: String,
    pub start_node_table: String,
    pub start_node_id_column: String, // ID column for start node (e.g., "user_id")
    pub start_node_alias: String,
    pub relationship_table: String,
    pub relationship_from_column: String, // From column in relationship table
    pub relationship_to_column: String,   // To column in relationship table
    pub relationship_alias: String,
    pub end_node_table: String,
    pub end_node_id_column: String, // ID column for end node
    pub end_node_alias: String,
    pub start_cypher_alias: String, // Original Cypher query alias (e.g., "u1")
    pub end_cypher_alias: String,   // Original Cypher query alias (e.g., "u2")
    pub properties: Vec<NodeProperty>, // Properties to include in the CTE
    pub database: Option<String>,   // Optional database prefix
    pub start_node_filters: Option<String>, // WHERE clause for start node (e.g., "start_node.full_name = 'Alice'")
    pub relationship_filters: Option<String>, // WHERE clause for relationship (e.g., "rel.weight > 0.5")
    pub relationship_types: Option<Vec<String>>, // Relationship type labels (e.g., ["FOLLOWS", "FRIENDS_WITH"])
    pub edge_id: Option<Identifier>, // Edge ID columns for relationship uniqueness (None = use from_id, to_id)
    /// The bound plan's paths as values ([`PathValues`]); set
    /// post-construction like `path_node_properties`.
    pub path_values: PathValues,
    /// The relation the standard arms walk in place of the edge table
    /// (`CteGenerationContext::walk_relation`); set post-construction.
    pub walk_relation: Option<String>,
}

impl<'a> VariableLengthCteGenerator<'a> {
    /// The standard-layout walk the bound plan builds, for tests: no
    /// properties, no filters, one relationship type unnamed.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn for_test(
        schema: &'a GraphSchema,
        spec: VariableLengthSpec,
        start_table: &str,
        start_id_col: &str,
        relationship_table: &str,
        rel_from_col: &str,
        rel_to_col: &str,
        end_table: &str,
        end_id_col: &str,
        start_alias: &str,
        end_alias: &str,
        edge_id: Option<Identifier>,
    ) -> Self {
        Self::new(
            schema,
            spec,
            start_table,
            start_id_col,
            relationship_table,
            rel_from_col,
            rel_to_col,
            end_table,
            end_id_col,
            start_alias,
            end_alias,
            vec![],
            None,
            None,
            None,
            edge_id,
        )
    }

    /// A walk over the standard layout (`cte_manager`): the start node, the
    /// relationship and the end node, each in its own table.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        schema: &'a GraphSchema, // Schema for constraint compilation
        spec: VariableLengthSpec,
        start_table: &str,
        start_id_col: &str,
        relationship_table: &str,
        rel_from_col: &str,
        rel_to_col: &str,
        end_table: &str,
        end_id_col: &str,
        start_alias: &str,
        end_alias: &str,
        properties: Vec<NodeProperty>,
        start_node_filters: Option<String>,
        relationship_filters: Option<String>, // Filters on relationship properties
        relationship_types: Option<Vec<String>>,
        edge_id: Option<Identifier>,
    ) -> Self {
        // Try to get database from environment
        let database = std::env::var("CLICKHOUSE_DATABASE").ok();

        Self {
            schema,
            spec,
            cte_name: format!("vlp_{}_{}", start_alias, end_alias),
            start_node_table: start_table.to_string(),
            start_node_id_column: start_id_col.to_string(),
            start_node_alias: "start_node".to_string(),
            relationship_table: relationship_table.to_string(),
            relationship_from_column: rel_from_col.to_string(),
            relationship_to_column: rel_to_col.to_string(),
            relationship_alias: "rel".to_string(),
            end_node_table: end_table.to_string(),
            end_node_id_column: end_id_col.to_string(),
            end_node_alias: "end_node".to_string(),
            start_cypher_alias: start_alias.to_string(),
            end_cypher_alias: end_alias.to_string(),
            properties: dedup_node_properties(properties),
            database,
            start_node_filters,
            relationship_filters,
            relationship_types,
            edge_id,
            path_values: PathValues::default(),
            walk_relation: None,
        }
    }

    /// Helper to format table name with optional database prefix
    /// If table already contains a dot (already qualified), return as-is
    fn format_table_name(&self, table: &str) -> String {
        // If table is already qualified (contains a dot), don't add prefix again
        if table.contains('.') {
            return table.to_string();
        }

        if let Some(db) = &self.database {
            format!("{}.{}", db, table)
        } else {
            table.to_string()
        }
    }

    /// Generate edge constraint expression for JOIN/WHERE clause
    /// Compiles constraint from schema (e.g., "from.timestamp <= to.timestamp")
    /// into SQL (e.g., "start_node.created_at <= end_node.created_at")
    ///
    /// Constraints are added to:
    /// - Base case: WHERE clause (after node JOINs)
    /// - Recursive case: WHERE clause (validates each hop)
    ///
    /// Generate edge constraint filter with dynamic alias support
    ///
    /// For recursive cases, pass the actual aliases used in that SQL block.
    /// If None, defaults to self.start_node_alias and self.end_node_alias.
    fn generate_edge_constraint_filter(
        &self,
        from_alias: Option<&str>,
        to_alias: Option<&str>,
    ) -> Option<String> {
        // Get the first relationship type (multi-type not supported for constraints)
        if let Some(rel_types) = &self.relationship_types {
            if let Some(rel_type) = rel_types.first() {
                // Look up relationship schema
                if let Some(rel_schema) = self.schema.get_relationships_schema_opt(rel_type) {
                    // Check if constraints are defined
                    if let Some(ref constraint_expr) = rel_schema.constraints {
                        // Get node schemas for from/to nodes
                        if let (Some(from_node_schema), Some(to_node_schema)) = (
                            self.schema.node_schema_opt(&rel_schema.from_node),
                            self.schema.node_schema_opt(&rel_schema.to_node),
                        ) {
                            // Use provided aliases or fall back to defaults
                            let actual_from_alias = from_alias.unwrap_or(&self.start_node_alias);
                            let actual_to_alias = to_alias.unwrap_or(&self.end_node_alias);

                            // Compile the constraint expression
                            match crate::graph_catalog::constraint_compiler::compile_constraint(
                                constraint_expr,
                                from_node_schema,
                                to_node_schema,
                                actual_from_alias,
                                actual_to_alias,
                            ) {
                                Ok(compiled_sql) => {
                                    log::debug!(
                                        "✅ Compiled VLP edge constraint for {} (from={}, to={}): {} → {}",
                                        rel_type, actual_from_alias, actual_to_alias, constraint_expr, compiled_sql
                                    );
                                    // Parenthesized: it is AND-ed with the
                                    // step's other conditions (an `OR`
                                    // would escape them, S11h review).
                                    return Some(format!("({compiled_sql})"));
                                }
                                Err(e) => {
                                    log::warn!(
                                        "⚠️  Failed to compile VLP edge constraint for {}: {}",
                                        rel_type,
                                        e
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
        None
    }

    /// Generate edge constraint filter for recursive case
    ///
    /// In the recursive case, we don't have a `current_node` join (removed for performance).
    /// Instead, the "from" node properties come from the CTE (vp.end_<alias>) and
    /// the "to" node properties come from the joined end_node table.
    ///
    /// Maps:
    /// - `from.property` → `vp.end_<property_alias>` (from CTE columns)
    /// - `to.property` → `end_node.<column_name>` (from joined table)
    fn generate_edge_constraint_filter_recursive(&self) -> Option<String> {
        // Get the first relationship type (multi-type not supported for constraints)
        let rel_types = self.relationship_types.as_ref()?;
        let rel_type = rel_types.first()?;

        // Look up relationship schema
        let rel_schema = self.schema.get_relationships_schema_opt(rel_type)?;

        // Check if constraints are defined
        let constraint_expr = rel_schema.constraints.as_ref()?;

        // Get node schemas for property resolution
        let from_node_schema = self.schema.node_schema_opt(&rel_schema.from_node)?;
        let to_node_schema = self.schema.node_schema_opt(&rel_schema.to_node)?;

        // Build the constraint by replacing property references
        // Pattern: from.property -> vp.end_<alias>, to.property -> end_node.<column>
        let mut compiled = constraint_expr.clone();

        // Replace from.property references with vp.end_<property_alias>
        // We need to find the property alias from self.properties
        for (property_name, mapping) in &from_node_schema.property_mappings {
            let from_pattern = format!("from.{}", property_name);
            if compiled.contains(&from_pattern) {
                // Find the corresponding property alias in self.properties
                // The property alias is based on the cypher property name, not column name
                let column_name = match mapping {
                    crate::graph_catalog::expression_parser::PropertyValue::Column(col) => {
                        col.clone()
                    }
                    crate::graph_catalog::expression_parser::PropertyValue::Expression(_) => {
                        continue
                    }
                };

                // Find the alias used in CTE for this column
                // Properties in CTE are stored as end_<alias> where alias is the property's output name
                let cte_alias = self
                    .properties
                    .iter()
                    .find(|p| {
                        p.column_name == column_name && p.cypher_alias == self.end_cypher_alias
                    })
                    .map(|p| p.alias.clone())
                    .unwrap_or_else(|| property_name.clone());

                let replacement = format!("vp.end_{}", cte_alias);
                compiled = compiled.replace(&from_pattern, &replacement);
            }
        }

        // Replace to.property references with end_node.<column_name>
        for (property_name, mapping) in &to_node_schema.property_mappings {
            let to_pattern = format!("to.{}", property_name);
            if compiled.contains(&to_pattern) {
                let column_name = match mapping {
                    crate::graph_catalog::expression_parser::PropertyValue::Column(col) => {
                        col.clone()
                    }
                    crate::graph_catalog::expression_parser::PropertyValue::Expression(_) => {
                        continue
                    }
                };

                let replacement = format!("{}.{}", self.end_node_alias, column_name);
                compiled = compiled.replace(&to_pattern, &replacement);
            }
        }

        log::debug!(
            "✅ Compiled VLP edge constraint for recursive case: {} → {}",
            constraint_expr,
            compiled
        );

        // Parenthesized: it is AND-ed with the step's hop limit and edge
        // uniqueness (an `OR` would escape them, S11h review).
        Some(format!("({compiled})"))
    }

    /// Build the SQL expression for the end node ID.
    /// Returns expression like `end_node.PersonId` or `concat(toString(end_node.col1), ...)`.
    /// Used for node-uniqueness cycle detection in path_nodes arrays.
    fn build_end_node_id_expr(&self) -> String {
        let end_id_identifier = Identifier::from_comma_separated(&self.end_node_id_column);
        emit_id_expr(&self.end_node_alias, &end_id_identifier)
    }

    /// Whether the walk enforces EDGE-uniqueness (relationship-uniqueness —
    /// Cypher's default: a path MAY revisit a node but must not reuse the same
    /// edge) via a `path_edges` array, instead of node-uniqueness via
    /// `path_nodes`: every walk with a recursive arm (#598, #1230). The decision
    /// lives in [`EdgeUniquenessPolicy`] ([`Self::edge_uniqueness_policy`]).
    fn uses_edge_uniqueness(&self) -> bool {
        self.edge_uniqueness_policy().uses_edge_uniqueness()
    }

    /// Build the [`EdgeUniquenessPolicy`] for this arm from the generator's own
    /// fields (#887 Phase 1–2). The `identity` is [`Self::edge_identity`] — the
    /// same value `build_edge_tuple_recursive` already spells through — so a
    /// single-alias arm's cycle predicate is byte-identical whether taken from
    /// the (former) inline gate or `policy.recursive_cycle_predicate(...)`; the
    /// unit tests assert this equivalence.
    fn edge_uniqueness_policy(&self) -> EdgeUniquenessPolicy {
        EdgeUniquenessPolicy::new(
            // `*0..0` has no recursive arm: nothing to dedupe
            self.spec.min_hops == 0 && self.spec.max_hops == 0,
            self.edge_identity(),
        )
    }

    /// Whether this VLP is a CLOSED pattern (`(a)-[*..]->(a)`: equal endpoint aliases). Edge
    /// uniqueness no longer depends on it (#1230); kept for the tests that pin the closed shape.
    #[cfg(test)]
    fn is_closed_pattern(&self) -> bool {
        self.start_cypher_alias == self.end_cypher_alias
    }

    /// The relation the base/recursive arms join for each hop: the caller's
    /// [`Self::walk_relation`], else the edge table.
    fn rel_source(&self) -> String {
        match &self.walk_relation {
            Some(relation) => relation.clone(),
            None => self.format_table_name(&self.relationship_table),
        }
    }

    /// The edge identity for THIS walk as a policy value (the #887
    /// [`EdgeIdentity`]).
    fn edge_identity(&self) -> EdgeIdentity {
        EdgeIdentity::EdgeIdColumns {
            edge_id: self.edge_id.clone(),
            from_col: self.relationship_from_column.clone(),
            to_col: self.relationship_to_column.clone(),
        }
    }

    /// Build the edge-identity value for one hop, reading the edge columns off
    /// `rel_alias`. Used by both the base case (`rel_alias` =
    /// `self.relationship_alias`) and the recursive case.
    ///
    /// Returns SQL like `tuple(r.from_id, r.to_id)` or `tuple(r.date, r.num, …)`.
    /// The shape is the walk's policy value ([`Self::edge_identity`],
    /// [`EdgeIdentity`]): schema `edge_id` (Single → bare `rel.col`;
    /// Composite → `tuple(...)`), else the `(from, to)` node pair (#710).
    fn build_edge_tuple_recursive(&self, rel_alias: &str) -> String {
        self.edge_identity()
            .spell(current_function_mapper().tuple_constructor(), rel_alias)
    }

    /// Generate the recursive CTE for variable-length traversal
    pub fn generate_cte(&self) -> Cte {
        let cte_sql = self.generate_recursive_sql();

        Cte::new(
            self.cte_name.clone(),
            crate::render_plan::CteContent::RawSql(cte_sql),
            true, // is_recursive
        )
    }

    // Note: extract_simple_equality_filter was removed as dead code (never called)

    /// Generate the actual recursive SQL string
    fn generate_recursive_sql(&self) -> String {
        let min_hops = self.spec.min_hops;
        let max_hops = self.spec.max_hops;

        // The base case is hop 1 (or 0), so a lower bound above 1 is applied
        // by a wrapper over the recursion: the recursion is then `_inner`.
        let recursive_cte_name = if min_hops > 1 {
            format!("{}_inner", self.cte_name)
        } else {
            self.cte_name.clone()
        };

        // Generate the core recursive query body (without CTE name wrapper)
        let mut query_body = String::new();

        // Base case: ONE base case, either zero-hop or 1-hop depending on min_hops
        if min_hops == 0 {
            // Zero-hop base case for patterns like *0.., *0..5
            query_body.push_str(&self.generate_zero_hop_base_case());
        } else {
            // 1-hop base case for patterns like *, *1.., *2..
            // (recursion will extend to 2+ hops)
            query_body.push_str(&self.generate_base_case());
        }

        // Recursive case: Add if we need more than just the base case.
        // Skip if max_hops == Some(0) (only zero-hop allowed).
        //
        // The base case covers exactly ONE hop count: 0 when min_hops == 0,
        // otherwise 1 (see the base-case branch above). Recursion is required
        // whenever the pattern must reach a hop count BEYOND that base — i.e.
        // when max_hops exceeds the base hop count (or is unbounded).
        //
        // #603: for an EXACT bound like *2..2 (min == max == 2) the old
        // `max > min_hops` test was false, so no recursive arm was emitted and
        // the base 1-hop rows were then all filtered out by `hop_count >= 2`,
        // yielding an empty CTE; comparing against the BASE hop count (1)
        // instead of min_hops fixes it.
        let recursion_threshold = if min_hops == 0 { 0 } else { 1 };
        let needs_recursion = max_hops > recursion_threshold;

        if needs_recursion {
            // Note: UNION DISTINCT is not supported in ClickHouse recursive CTEs.
            // Using UNION ALL means duplicate edges in the data can cause exponential
            // row explosion. The path_edges tracking with `NOT has()` prevents cycles
            // but not duplicate edges between the same nodes.
            query_body.push_str("\n    UNION ALL\n");

            query_body.push_str(
                &self.generate_recursive_case_with_cte_name(max_hops, &recursive_cte_name),
            );
        }

        // Apply min_hops filtering if needed: the base case starts at hop 1 to
        // allow recursion, so `*2..` keeps only hop_count >= 2 (max_hops is
        // enforced by the recursion's termination condition).
        if min_hops > 1 {
            format!(
                "{}_inner AS (\n{}\n),\n{} AS (\n    SELECT * FROM {}_inner WHERE hop_count >= {}\n)",
                self.cte_name, query_body, self.cte_name, self.cte_name, min_hops
            )
        } else {
            format!("{} AS (\n{}\n)", self.cte_name, query_body)
        }
    }

    /// Generate base case for zero hops (self-loop)
    /// Used with shortest path functions when pattern is *0..
    fn generate_zero_hop_base_case(&self) -> String {
        // For zero-hop, start = end (the node hasn't moved; one node table).
        let empty_str_arr = current_function_mapper().empty_string_array_cast();
        // #1132: the id must go through the composite-aware `emit_id_expr` —
        // interpolating the raw comma-separated column string produced
        // `start_node.bank_id, acct_no as start_id` (two columns, alias on the
        // second; the pipe-joined concat never built) → Code 44 on every
        // composite `*0..N`. Single-column ids emit byte-identically to the
        // old string form.
        let start_id_identifier = Identifier::from_comma_separated(&self.start_node_id_column);
        let start_id_expr = emit_id_expr(&self.start_node_alias, &start_id_identifier);
        let mut select_items = vec![
            format!("{start_id_expr} as start_id"),
            // Same node for self-loop
            format!("{start_id_expr} as end_id"),
            "0 as hop_count".to_string(), // Zero hops
            format!("{empty_str_arr} as path_relationships"), // Minimal placeholder when path data not needed
            // Add path_nodes for UNWIND nodes(p) support - for zero hop, just the start node
            format!("{} as path_nodes", arr(&start_id_expr)),
        ];

        // The bound plan's paths as values: at hop 0 the start node, and no
        // relationship. Same position as in the 1-hop and recursive arms.
        for (column, (start_or_rel, empty), node) in self.path_values.columns() {
            select_items.push(if node {
                format!("{} as {column}", arr(start_or_rel))
            } else {
                format!("{empty} as {column}")
            });
        }

        // #628: a CLOSED `*0..N` walk enforces EDGE-uniqueness (so real cycles
        // survive — see `uses_edge_uniqueness`). The zero-hop base has no edge,
        // so it seeds an EMPTY-but-typed `path_edges` array. A bare `[]` is
        // ClickHouse's `Array(Nothing)` and the recursive engine REJECTS the
        // base arm against the recursive arm's concrete `Array(Tuple(...))`
        // ("Conversion from Tuple(...) to Nothing is not supported" —
        // `typed_empty_edges_seed` documents the live #887 proof), so the seed
        // must be an empty slice of a real edge-identity tuple, pulled from the
        // edge source itself. The recursive arm's `NOT has(path_edges, …)`
        // then dedupes edges from hop 1 onward. Gated identically to the base /
        // recursive arms via `uses_edge_uniqueness()` (a `*0..0` walk, with no
        // recursive arm, stays node-unique).
        if self.uses_edge_uniqueness() {
            let seed_identity = self
                .edge_identity()
                .spell(current_function_mapper().tuple_constructor(), "__seed_edge");
            select_items.push(format!(
                "{} as path_edges",
                typed_empty_edges_seed(&self.rel_source(), &seed_identity)
            ));
        }

        // #1132: the non-zero arms project each composite component as
        // `start_<col>`/`end_<col>` (grouped — #1130). The zero-hop arm must
        // emit the SAME columns in the SAME order or UNION ALL misbinds by
        // position. At hop 0 start == end, so both groups read the start node.
        if let Identifier::Composite(cols) = &start_id_identifier {
            for col in cols.iter() {
                select_items.push(format!(
                    "{}.{} as start_{}",
                    self.start_node_alias,
                    crate::clickhouse_query_generator::quote_identifier(col),
                    col
                ));
            }
            for col in cols.iter() {
                select_items.push(format!(
                    "{}.{} as end_{}",
                    self.start_node_alias,
                    crate::clickhouse_query_generator::quote_identifier(col),
                    col
                ));
            }
        }

        // Composite components already projected above — the props loop must
        // not emit them again (Code 44 column-already-exists). Mirrors the
        // non-zero arms, whose props loops skip id components too.
        let is_id_component = |col: &str| -> bool {
            matches!(&start_id_identifier, Identifier::Composite(cols)
                if cols.iter().any(|c| c == col))
        };

        // The end node's carried properties: at hop 0 the start node's row.
        for prop in &self.properties {
            if prop.cypher_alias == self.end_cypher_alias && !is_id_component(&prop.column_name) {
                select_items.push(format!(
                    "{}.{} as end_{}",
                    self.start_node_alias, prop.column_name, prop.alias
                ));
            }
        }

        let select_clause = select_items.join(",\n        ");

        // Build the zero-hop query - just select from start table.
        let mut query = format!(
            "    SELECT \n        {}\n    FROM {} AS {}",
            select_clause,
            self.format_table_name(&self.start_node_table),
            self.start_node_alias
        );

        // Apply start_node_filters (WHERE clause)
        let mut where_conditions = Vec::new();
        if let Some(ref filters) = self.start_node_filters {
            where_conditions.push(filters.clone());
        }

        if !where_conditions.is_empty() {
            query.push_str("\n    WHERE ");
            query.push_str(&where_conditions.join(" AND "));
        }

        query
    }

    /// The 1-hop base case: the start node, the edge and the end node, each
    /// in its own table.
    fn generate_base_case(&self) -> String {
        // Parse comma-separated column string to Identifier
        let parse_id_cols = Identifier::from_comma_separated;
        let empty_str_arr = current_function_mapper().empty_string_array_cast();

        // Parse ID identifiers
        let start_id_identifier = parse_id_cols(&self.start_node_id_column);
        let end_id_identifier = parse_id_cols(&self.end_node_id_column);

        // Generate start_id / end_id selections (composite-aware)
        let start_id_selection = format!(
            "{} as start_id",
            emit_id_expr(&self.start_node_alias, &start_id_identifier)
        );
        let end_id_selection = format!(
            "{} as end_id",
            emit_id_expr(&self.end_node_alias, &end_id_identifier)
        );

        // path_nodes: only emit a proper 2-element array when start and end
        // ID shapes match (both Single or both Composite); preserve the
        // pre-existing defensive fallback for mismatched shapes.
        let path_nodes_selection = match (&start_id_identifier, &end_id_identifier) {
            (Identifier::Single(_), Identifier::Single(_))
            | (Identifier::Composite(_), Identifier::Composite(_)) => {
                format!(
                    "{} as path_nodes",
                    arr(&format!(
                        "{}, {}",
                        emit_id_expr(&self.start_node_alias, &start_id_identifier),
                        emit_id_expr(&self.end_node_alias, &end_id_identifier),
                    ))
                )
            }
            _ => format!("{} as path_nodes", arr("")),
        };

        // Build property selections
        let mut select_items = vec![
            start_id_selection,
            end_id_selection,
            "1 as hop_count".to_string(),
        ];
        select_items.push(format!("{empty_str_arr} as path_relationships"));
        select_items.push(path_nodes_selection);

        // The bound plan's paths as values, seeded like `path_nodes`.
        for (column, (a, b), node) in self.path_values.columns() {
            select_items.push(if node {
                format!("{} as {column}", arr(&format!("{a}, {b}")))
            } else {
                format!("{} as {column}", arr(a))
            });
        }

        // #598 (part 2): seed path_edges with this hop's edge identity so the
        // recursive step can enforce relationship-uniqueness (Cypher default).
        // A non-empty 1-element array literal lets ClickHouse infer
        // Array(Tuple(...)) directly — no CAST needed. path_nodes is retained
        // above for nodes(p); path_edges drives the cycle check.
        if self.uses_edge_uniqueness() {
            select_items.push(format!(
                "{} as path_edges",
                arr(&self.build_edge_tuple_recursive(&self.relationship_alias))
            ));
        }

        // For composite IDs, add individual ID component columns
        // This allows queries like RETURN dest.bank_id, dest.account_number
        if let Identifier::Composite(cols) = &start_id_identifier {
            for col in cols.iter() {
                select_items.push(format!(
                    "{}.{} as start_{}",
                    self.start_node_alias,
                    crate::clickhouse_query_generator::quote_identifier(col),
                    col // Use column name as alias (e.g., "bank_id")
                ));
            }
        }
        if let Identifier::Composite(cols) = &end_id_identifier {
            for col in cols.iter() {
                select_items.push(format!(
                    "{}.{} as end_{}",
                    self.end_node_alias,
                    crate::clickhouse_query_generator::quote_identifier(col),
                    col // Use column name as alias (e.g., "bank_id")
                ));
            }
        }

        // Parse ID columns to check for composite ID components
        let end_id_cols: Vec<&str> = self
            .end_node_id_column
            .split(',')
            .map(|s| s.trim())
            .collect();

        // Add the end node's carried properties (the edge constraints'
        // `from.` reads, `bound_plan::lower::path`), skipping its ID columns.
        for prop in &self.properties {
            if prop.cypher_alias == self.end_cypher_alias
                && !end_id_cols.contains(&prop.column_name.as_str())
            {
                // Property belongs to end node (and is not the ID column)
                select_items.push(format!(
                    "{}.{} as end_{}",
                    self.end_node_alias, prop.column_name, prop.alias
                ));
            }
        }

        let select_clause = select_items.join(",\n        ");

        // Parse comma-separated column string to Identifier
        let parse_id_cols = Identifier::from_comma_separated;

        // Parse column identifiers for composite ID support
        let start_id_identifier = parse_id_cols(&self.start_node_id_column);
        let end_id_identifier = parse_id_cols(&self.end_node_id_column);
        let rel_from_identifier = parse_id_cols(&self.relationship_from_column);
        let rel_to_identifier = parse_id_cols(&self.relationship_to_column);

        // Generate JOIN ON clauses with composite ID support
        let join_on_rel = start_id_identifier.to_sql_equality(
            &self.start_node_alias,
            &rel_from_identifier,
            &self.relationship_alias,
        );
        let join_on_end = rel_to_identifier.to_sql_equality(
            &self.relationship_alias,
            &end_id_identifier,
            &self.end_node_alias,
        );

        // Build the base query without WHERE clause
        let mut query = format!(
            "    SELECT \n        {select}\n    FROM {start_table} AS {start}\n    JOIN {rel_table} AS {rel} ON {join_on_rel}\n    JOIN {end_table} AS {end} ON {join_on_end}",
            select = select_clause,
            start = self.start_node_alias,
            end = self.end_node_alias,
            rel = self.relationship_alias,
            start_table = self.format_table_name(&self.start_node_table),
            rel_table = self.rel_source(),
            end_table = self.format_table_name(&self.end_node_table),
            join_on_rel = join_on_rel,
            join_on_end = join_on_end
        );

        // Add WHERE clause with the start node's and the relationship's filters
        let mut where_conditions = Vec::new();

        // Add edge constraints if defined in schema (base case uses default aliases)
        if let Some(constraint_filter) = self.generate_edge_constraint_filter(None, None) {
            where_conditions.push(constraint_filter);
        }

        if let Some(ref filters) = self.start_node_filters {
            where_conditions.push(filters.clone());
        }
        // ✅ HOLISTIC FIX: Add relationship filters (e.g., WHERE r.weight > 0.5)
        // These filters apply to the relationship/edge table properties and must be applied
        // during traversal, not on the CTE output (which doesn't have these columns)
        if let Some(ref filters) = self.relationship_filters {
            log::debug!("Adding relationship filters to base case: {}", filters);
            where_conditions.push(filters.clone());
        }

        if !where_conditions.is_empty() {
            query.push_str(&format!("\n    WHERE {}", where_conditions.join(" AND ")));
        }

        query
    }

    fn generate_recursive_case_with_cte_name(&self, max_hops: u32, cte_name: &str) -> String {
        // Standard case: both nodes have their own tables
        // Parse comma-separated column string to Identifier
        let parse_id_cols = Identifier::from_comma_separated;
        let fmap = current_function_mapper();
        let ac = fmap.array_concat();
        let empty_str_arr = fmap.empty_string_array_cast();

        // Parse endpoint ID identifiers (start needed for the composite
        // pass-through block below — #1130 review).
        let start_id_identifier = parse_id_cols(&self.start_node_id_column);
        let end_id_identifier = parse_id_cols(&self.end_node_id_column);
        let end_id_expr_str = emit_id_expr(&self.end_node_alias, &end_id_identifier);

        // end_id selection (composite-aware) and path_nodes extension
        let end_id_selection = format!("{end_id_expr_str} as end_id");
        let path_nodes_selection = format!(
            "{ac}(vp.path_nodes, {}) as path_nodes",
            arr(&end_id_expr_str)
        );

        // Build property selections for recursive case
        let mut select_items = vec![
            "vp.start_id".to_string(),
            end_id_selection,
            "vp.hop_count + 1 as hop_count".to_string(),
        ];
        select_items.push(format!("{empty_str_arr} as path_relationships"));
        select_items.push(path_nodes_selection);

        // The bound plan's paths as values, extended like `path_nodes`.
        for (column, (a, b), node) in self.path_values.columns() {
            let one = if node { b } else { a };
            select_items.push(format!("{ac}(vp.{column}, {}) as {column}", arr(one)));
        }

        // #598 (part 2): accumulate this hop's edge identity so relationship-uniqueness
        // (Cypher default) can be enforced below. path_nodes is still accumulated
        // above for nodes(p). Must be projected on BOTH recursive and base arms —
        // gated identically via uses_edge_uniqueness().
        if self.uses_edge_uniqueness() {
            select_items.push(format!(
                "{ac}(vp.path_edges, {}) as path_edges",
                arr(&self.build_edge_tuple_recursive(&self.relationship_alias))
            ));
        }

        // For composite IDs, add individual ID component columns.
        //
        // #1130 review (CRITICAL): the arm ORDER must MATCH the base arm —
        // grouped (all `start_*`, then all `end_*`), never interleaved per
        // column. A recursive CTE's UNION ALL binds by POSITION (#908 class):
        // the old interleaved emission swapped `start_account_number` with
        // `end_bank_id` on every hop>=2 row, silently scrambling both
        // RETURN-position component reads (pre-existing on main) and the
        // wrapper end-filter (#1123) that binds to these columns. Also keyed
        // on the CORRECT side's identifier: the start pass-throughs iterate
        // the START composite (the old code iterated end's cols for both,
        // wrong whenever the two composites differ).
        if let Identifier::Composite(cols) = &start_id_identifier {
            for col in cols.iter() {
                // Start ID components pass through from vp
                select_items.push(format!("vp.start_{} as start_{}", col, col));
            }
        }
        if let Identifier::Composite(cols) = &end_id_identifier {
            for col in cols.iter() {
                // End ID components come from the newly joined node
                select_items.push(format!(
                    "{}.{} as end_{}",
                    self.end_node_alias,
                    crate::clickhouse_query_generator::quote_identifier(col),
                    col
                ));
            }
        }

        // The end node's carried properties come from the newly joined node,
        // skipping its ID columns.
        let end_id_cols: Vec<&str> = self
            .end_node_id_column
            .split(',')
            .map(|s| s.trim())
            .collect();

        for prop in &self.properties {
            if prop.cypher_alias == self.end_cypher_alias
                && !end_id_cols.contains(&prop.column_name.as_str())
            {
                // End node properties come from the newly joined node
                select_items.push(format!(
                    "{}.{} as end_{}",
                    self.end_node_alias, prop.column_name, prop.alias
                ));
            }
        }

        let select_clause = select_items.join(",\n        ");

        // Cycle prevention (issue #598, part 2).
        //
        // Standard directed range VLP now enforces Cypher's default RELATIONSHIP-
        // uniqueness: a path may revisit a node but must not reuse the same edge
        // (`NOT has(path_edges, <this hop's edge>)`). Node-uniqueness wrongly dropped
        // valid paths that revisit a node via a different edge (e.g. mutual follows 1↔2).
        //
        // #887 Phase 1–2: the edge-vs-node decision + identity spelling is the
        // shared EdgeUniquenessPolicy (byte-identical to the old inline gate,
        // proven by the spike in commit 1).
        let cycle_pred = self
            .edge_uniqueness_policy()
            .recursive_cycle_predicate(&self.relationship_alias, &self.build_end_node_id_expr());

        let mut where_conditions = vec![format!("vp.hop_count < {}", max_hops), cycle_pred];

        // Add edge constraints if defined in schema
        // Uses vp.end_* columns for the "from" node (previous end node) and
        // end_node.* columns for the "to" node (newly joined node)
        if let Some(constraint_filter) = self.generate_edge_constraint_filter_recursive() {
            where_conditions.push(constraint_filter);
        }

        // ✅ HOLISTIC FIX: Add relationship filters in recursive case too
        // This ensures relationship property filters (e.g., r.weight > 0.5) are applied
        // at every hop of the traversal, not just the base case
        if let Some(ref filters) = self.relationship_filters {
            log::debug!("Adding relationship filters to recursive case: {}", filters);
            where_conditions.push(filters.clone());
        }

        let where_clause = where_conditions.join("\n      AND ");

        // Parse comma-separated column string to Identifier
        let parse_id_cols = Identifier::from_comma_separated;

        // Parse column identifiers for composite ID support
        let end_id_identifier = parse_id_cols(&self.end_node_id_column);
        let rel_from_identifier = parse_id_cols(&self.relationship_from_column);
        let rel_to_identifier = parse_id_cols(&self.relationship_to_column);

        // Generate JOIN ON clauses with composite ID support
        // For VLP recursion, the CTE's end_id is a pipe-joined string.
        // For composite relationship from_id, we need to convert to pipe-joined format for comparison.
        // The bound plan's walks: the path's last node by its id's columns
        // (`end_<col>`), paired with the relationship's in order.
        let by_columns = match (&rel_from_identifier, &end_id_identifier) {
            (Identifier::Composite(cols), Identifier::Composite(ends))
                if ends.len() == cols.len() =>
            {
                Some(
                    ends.iter()
                        .zip(cols)
                        .map(|(e, c)| {
                            format!(
                                "vp.{} = {}.{}",
                                crate::clickhouse_query_generator::quote_identifier(&format!(
                                    "end_{e}"
                                )),
                                &self.relationship_alias,
                                crate::clickhouse_query_generator::quote_identifier(c)
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(" AND "),
                )
            }
            _ => None,
        };
        let join_on_rel = match (by_columns, &rel_from_identifier) {
            (Some(join), _) => join,
            (None, Identifier::Single(col)) => {
                // Single column: direct comparison
                format!(
                    "vp.end_id = {}.{}",
                    &self.relationship_alias,
                    crate::clickhouse_query_generator::quote_identifier(col)
                )
            }
            (None, Identifier::Composite(cols)) => {
                // Composite of another arity than the node's id: the lowering
                // refuses it first (`standard_edge`); the pipe-joined text kept.
                let concat_parts: Vec<String> = cols
                    .iter()
                    .map(|c| {
                        // toString -> Spark `string` cast alias via the FunctionMapper.
                        format!(
                            "{}({}.{})",
                            crate::sql_generator::function_mapper::current_function_mapper()
                                .cast_string(),
                            &self.relationship_alias,
                            crate::clickhouse_query_generator::quote_identifier(c)
                        )
                    })
                    .collect();
                format!(
                    "vp.end_id = concat({}, '|', {})",
                    concat_parts[0],
                    concat_parts[1..].join(", '|', ")
                )
            }
        };

        let join_on_end = rel_to_identifier.to_sql_equality(
            &self.relationship_alias,
            &end_id_identifier,
            &self.end_node_alias,
        );

        // PERF: no current_node JOIN — vp.end_id already holds the id joined
        // with the relationship table.
        format!(
            "    SELECT\n        {select}\n    FROM {cte_name} vp\n    JOIN {rel_table} AS {rel} ON {join_on_rel}\n    JOIN {end_table} AS {end} ON {join_on_end}\n    WHERE {where_clause}",
            select = select_clause,
            end = self.end_node_alias,
            cte_name = cte_name, // Use the passed parameter instead of self.cte_name
            rel_table = self.rel_source(),
            rel = self.relationship_alias,
            end_table = self.format_table_name(&self.end_node_table),
            join_on_rel = join_on_rel,
            join_on_end = join_on_end,
            where_clause = where_clause
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Helper to create a minimal test schema for VLC tests
    fn create_test_schema() -> GraphSchema {
        GraphSchema::build(1, "test_db".to_string(), HashMap::new(), HashMap::new())
    }

    /// #887 Phase 1–2: the `EdgeUniquenessPolicy` decision: EDGE for every
    /// walk with a recursive arm (every lower bound, #1230).
    #[test]
    fn edge_uniqueness_policy_decision_table_887() {
        let id = || EdgeIdentity::EdgeIdColumns {
            edge_id: None,
            from_col: "from_id".to_string(),
            to_col: "to_id".to_string(),
        };
        assert!(
            EdgeUniquenessPolicy::new(false, id()).uses_edge_uniqueness(),
            "any lower bound → edge"
        );
        assert!(
            !EdgeUniquenessPolicy::new(true, id()).uses_edge_uniqueness(),
            "a zero-hop arm with no recursion stays node-unique"
        );
    }

    /// #887 Phase 1–2: `recursive_cycle_predicate` spells the Edge branch via
    /// the identity off `rel_alias` and the Node branch via the given id expr —
    /// byte-identical to the former inline `emit_edge_cycle_check` /
    /// `emit_cycle_check` gates.
    #[test]
    fn edge_uniqueness_policy_recursive_predicate_spelling_887() {
        let id = EdgeIdentity::EdgeIdColumns {
            edge_id: Some(Identifier::from_comma_separated("follow_id")),
            from_col: "from_id".to_string(),
            to_col: "to_id".to_string(),
        };
        // Edge kind (min>=1): NOT has(vp.path_edges, rel.follow_id).
        let edge = EdgeUniquenessPolicy::new(false, id.clone());
        assert_eq!(
            edge.recursive_cycle_predicate("rel", "next.node_id"),
            "NOT has(vp.path_edges, rel.follow_id)"
        );
        // Node kind (no recursion): NOT has(vp.path_nodes, <id expr>).
        let node = EdgeUniquenessPolicy::new(true, id);
        assert_eq!(
            node.recursive_cycle_predicate("rel", "next.node_id"),
            "NOT has(vp.path_nodes, next.node_id)"
        );
    }

    #[test]
    fn dedup_node_properties_drops_exact_duplicates_keeps_distinct_631() {
        let np = |c: &str, col: &str, a: &str| NodeProperty {
            cypher_alias: c.to_string(),
            column_name: col.to_string(),
            alias: a.to_string(),
        };
        // Closed pattern: the same endpoint property resolved twice (identical) →
        // collapse to one (else Code 44 on the duplicated end_<prop> column).
        assert_eq!(
            dedup_node_properties(vec![
                np("a", "full_name", "name"),
                np("a", "full_name", "name")
            ]),
            vec![np("a", "full_name", "name")]
        );
        // Distinct properties on the same alias are all kept (order preserved).
        assert_eq!(
            dedup_node_properties(vec![
                np("a", "full_name", "name"),
                np("a", "country", "country"),
                np("a", "full_name", "name"),
            ]),
            vec![np("a", "full_name", "name"), np("a", "country", "country")]
        );
        // Different alias but same column/output is NOT a duplicate (open pattern
        // start vs end must both survive).
        assert_eq!(
            dedup_node_properties(vec![
                np("a", "full_name", "name"),
                np("b", "full_name", "name")
            ]),
            vec![np("a", "full_name", "name"), np("b", "full_name", "name")]
        );
    }

    #[test]
    fn test_variable_length_cte_generation() {
        let schema = create_test_schema();
        let spec = VariableLengthSpec::range(1, 3);
        let generator = VariableLengthCteGenerator::for_test(
            &schema,
            spec,
            "users",
            "user_id",
            "authored",
            "author_id",
            "post_id",
            "posts",
            "post_id",
            "u",
            "p",
            None,
        );

        let cte = generator.generate_cte();
        println!("Generated CTE: {}", cte.cte_name);

        // Test that CTE was created
        assert!(!cte.cte_name.is_empty());
        assert!(cte.cte_name.starts_with("vlp_"));
    }

    /// #628: a CLOSED `*0..N` VLP (same start/end alias) must count real cycles,
    /// which requires EDGE-uniqueness. The zero-hop base seeds an empty
    /// `path_edges` array; hops ≥ 1 accumulate and dedupe via `NOT has(...)`.
    /// An OPEN `*0..N` (distinct aliases) keeps NODE-uniqueness — unchanged.
    #[test]
    fn closed_zero_hop_vlp_uses_edge_uniqueness_628() {
        let schema = create_test_schema();
        let make = |start_alias: &str, end_alias: &str| {
            VariableLengthCteGenerator::for_test(
                &schema,
                VariableLengthSpec::range(0, 2),
                "users",
                "user_id",
                "follows",
                "follower_id",
                "followed_id",
                "users",
                "user_id",
                start_alias,
                end_alias,
                None,
            )
        };

        // CLOSED: same alias on both endpoints → edge-uniqueness.
        let closed = make("a", "a");
        assert!(closed.is_closed_pattern());
        assert!(
            closed.uses_edge_uniqueness(),
            "closed *0..N must use edge-uniqueness"
        );
        let closed_sql = closed.generate_recursive_sql();
        // Zero-hop base seeds an empty-but-typed path_edges array: a typed-empty
        // slice of a real edge-identity tuple pulled from the edge source via a
        // scalar subquery (a bare `[]` = Array(Nothing) fails ClickHouse
        // recursive-CTE unification — `typed_empty_edges_seed`, #887).
        assert!(
            closed_sql.contains("as path_edges")
                && closed_sql.contains("__seed_edge")
                && closed_sql.contains("LIMIT 1"),
            "closed *0..N zero-hop base must seed typed-empty path_edges; got:\n{closed_sql}"
        );
        assert!(
            !closed_sql.contains("[] as path_edges"),
            "closed *0..N zero-hop base must not seed bare `[]`; got:\n{closed_sql}"
        );
        // ...and the recursive arm enforces EDGE-uniqueness (not node).
        assert!(
            closed_sql.contains("NOT has(vp.path_edges,"),
            "closed *0..N recursive arm must dedupe on path_edges; got:\n{closed_sql}"
        );
        assert!(
            !closed_sql.contains("NOT has(vp.path_nodes,"),
            "closed *0..N must NOT use node-uniqueness; got:\n{closed_sql}"
        );

        // OPEN: distinct aliases → edge-unique too since #1230 (a `*0..N` trail may revisit a
        // node; node-uniqueness silently dropped every such trail), same typed-empty seed.
        let open = make("a", "b");
        assert!(!open.is_closed_pattern());
        assert!(
            open.uses_edge_uniqueness(),
            "open *0..N must be edge-unique (#1230)"
        );
        let open_sql = open.generate_recursive_sql();
        assert!(
            !open_sql.contains("[] as path_edges"),
            "open *0..N must not seed a bare `[] as path_edges`; got:\n{open_sql}"
        );
        assert!(
            open_sql.contains("NOT has(vp.path_edges,")
                && !open_sql.contains("NOT has(vp.path_nodes,"),
            "open *0..N must use edge-uniqueness (#1230); got:\n{open_sql}"
        );
    }
}
