use crate::graph_catalog::config::Identifier;
use crate::graph_catalog::graph_schema::GraphSchema;
use crate::query_planner::join_context::VLP_END_ID_COLUMN;
use crate::query_planner::logical_plan::VariableLengthSpec;
use crate::render_plan::Cte;
use crate::sql_generator::function_mapper::current_function_mapper;

// ===== VLP Performance and Safety Constants =====

/// Default maximum hops when unspecified in VLP queries.
/// Used as fallback when VariableLengthSpec.max_hops is None.
/// This prevents unbounded recursion in dense graphs.
/// Original value: 10, reduced to 5 for memory safety in dense graphs.
const DEFAULT_MAX_HOPS: u32 = 5;

/// Get configurable default max hops with environment variable override.
/// `CLICKGRAPH_VLP_MAX_HOPS` overrides the compiled-in default for all VLP queries.
/// Uses the same default for all variable-length patterns (including shortestPath)
/// to avoid silently changing query semantics.
fn get_default_max_hops() -> u32 {
    if let Ok(val) = std::env::var("CLICKGRAPH_VLP_MAX_HOPS") {
        if let Ok(n) = val.parse::<u32>() {
            return n;
        }
    }
    DEFAULT_MAX_HOPS
}

/// Emit a SQL expression that materializes a node's ID, composite-aware.
///
/// Returns the bare expression (no `AS` clause). Single-column IDs render as
/// `alias.col` (quoted); composite IDs collapse into a pipe-joined string via
/// `concat(toString(alias.c1), '|', toString(alias.c2), ...)` so they can be
/// stored in `path_nodes` and used for `has()` cycle checks.
fn emit_id_expr(table_alias: &str, id: &Identifier) -> String {
    match id {
        Identifier::Single(col) => format!(
            "{}.{}",
            table_alias,
            crate::clickhouse_query_generator::quote_identifier(col)
        ),
        Identifier::Composite(cols) => {
            let cast_str = current_function_mapper().cast_string();
            let parts: Vec<String> = cols
                .iter()
                .map(|c| {
                    format!(
                        "{cast_str}({}.{})",
                        table_alias,
                        crate::clickhouse_query_generator::quote_identifier(c)
                    )
                })
                .collect();
            format!("concat({}, '|', {})", parts[0], parts[1..].join(", '|', "))
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
/// the hop being added this step (typically from [`VariableLengthCteGenerator::build_edge_tuple_recursive`]).
fn emit_edge_cycle_check(edge_expr: &str) -> String {
    let array_contains = current_function_mapper().array_contains();
    format!("NOT {array_contains}(vp.path_edges, {edge_expr})")
}

/// Original-orientation edge-identity column names projected by the
/// doubled-edge CTE (#617). They carry the physical edge's `(from, to)`
/// regardless of which orientation a row represents, so trail-uniqueness
/// (`path_edges` for recursive walks, pairwise inequality for flat exact-bound
/// chains) treats both orientations of one edge as the same relationship.
/// Canonically defined in the schema catalog (which rejects colliding tables).
pub use crate::graph_catalog::graph_schema::{DOUBLED_EDGES_ORIG_FROM, DOUBLED_EDGES_ORIG_TO};

/// Name of the doubled-edge CTE for the pattern's endpoint cypher aliases +
/// edge table (#617). Deliberately NOT `vlp_`-prefixed: several render passes
/// special-case `vlp_`-named CTEs (column pruning, outer-alias mapping) and
/// must treat this one as a plain table source. Shared by the recursive-walk
/// generator and the flat exact-bound join expansion.
///
/// The (sanitized) edge-table suffix makes the name a pure function of the
/// CTE's CONTENT: alias-only naming let two Cypher-UNION arms with the same
/// endpoint aliases but DIFFERENT relationship types collide — the arm-merge
/// rename fix-up patches only `render.from`, not `joins[].table_name`, so the
/// second arm silently walked the first arm's edges (review finding). Same
/// name now implies same table, hence byte-identical body, making keep-one
/// dedup always semantics-preserving.
pub fn undirected_doubled_edges_cte_name(
    start_alias: &str,
    end_alias: &str,
    rel_table: &str,
) -> String {
    let table_key: String = rel_table
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    format!("undir_edges_{start_alias}_{end_alias}_{table_key}")
}

/// #617: enumerate the edge-table columns a doubled-edge CTE must project
/// besides the (swapped) from/to id columns: physical column list, mapped
/// property columns, and polymorphic discriminator / edge-id columns from the
/// resolved relationship schema. Sorted + deduped (BTreeSet) for deterministic
/// SQL; the from/to id columns themselves are excluded.
pub fn doubled_edges_passthrough_columns(
    schema: &crate::graph_catalog::GraphSchema,
    rel_type: Option<&str>,
    rel_table: &str,
) -> Vec<String> {
    let rel_table_name = rel_table.rsplit('.').next().unwrap_or_default();
    let by_type = rel_type.and_then(|t| {
        schema.get_relationships_schema_opt(t).or_else(|| {
            let plain = t.split("::").next().unwrap_or(t);
            schema.rel_schemas_for_type(plain).into_iter().next()
        })
    });
    let by_table = || {
        schema
            .get_relationships_schemas()
            .values()
            .find(|r| r.table_name == rel_table_name)
    };
    by_type
        .or_else(by_table)
        .map(|rel_schema| rel_schema.doubled_edge_passthrough_columns())
        .unwrap_or_default()
}

/// #617: body of a doubled-edge CTE — each physical edge emitted in both
/// orientations under the ORIGINAL from/to column names (so joins written for
/// the raw edge table work unchanged), plus original-identity columns (see
/// [`DOUBLED_EDGES_ORIG_FROM`]). `passthrough_cols` are additional edge-table
/// columns projected verbatim (property/discriminator/edge-id columns);
/// callers must exclude the from/to columns and provide deterministic order.
///
/// Dialect-neutral: plain column aliases + UNION ALL only.
pub fn build_doubled_edges_cte_body(
    table_ref: &str,
    from_col: &str,
    to_col: &str,
    passthrough_cols: &[String],
) -> String {
    let q = crate::clickhouse_query_generator::quote_identifier;
    // Every column reference MUST be table-qualified: the reverse arm aliases
    // `e.to AS from`, and an UNQUALIFIED later reference to `from` (for the
    // identity column) resolves to that new ALIAS in ClickHouse, silently
    // flipping the edge identity on reverse rows (breaks trail-uniqueness in
    // both directions — verified live). `e.<col>` binds to the raw column.
    let passthrough = passthrough_cols
        .iter()
        .map(|c| format!(", e.{}", q(c)))
        .collect::<Vec<_>>()
        .join("");
    let (from_q, to_q) = (q(from_col), q(to_col));
    format!(
        "    SELECT e.{from_q}, e.{to_q}{passthrough}, e.{from_q} AS {orig_from}, e.{to_q} AS {orig_to} FROM {table_ref} AS e\n    \
         UNION ALL\n    \
         SELECT e.{to_q} AS {from_q}, e.{from_q} AS {to_q}{passthrough}, e.{from_q} AS {orig_from}, e.{to_q} AS {orig_to} FROM {table_ref} AS e",
        orig_from = DOUBLED_EDGES_ORIG_FROM,
        orig_to = DOUBLED_EDGES_ORIG_TO,
    )
}

/// Emit a dialect-correct array literal. CH: `[a, b]`. Spark: `array(a, b)`.
/// Thin shorthand over `FunctionMapper::array_literal` — VLP code builds
/// these in many places (path_nodes, path_relationships, scalar→array
/// wrappers for arrayConcat).
fn arr(elems: &str) -> String {
    current_function_mapper().array_literal(elems)
}

/// Whether `s` looks like a bare integer literal (digits, optional leading
/// `-`). Used to scope the Spark BFS anchor cast to numeric IDs — column
/// references inherit their column's type and string-keyed IDs would break
/// under a numeric cast, so neither should be wrapped.
pub(crate) fn is_integer_literal(s: &str) -> bool {
    let bytes = s.as_bytes();
    match bytes.first() {
        Some(b'-') if bytes.len() > 1 => bytes[1..].iter().all(|b| b.is_ascii_digit()),
        Some(b) if b.is_ascii_digit() => bytes.iter().all(|b| b.is_ascii_digit()),
        _ => false,
    }
}

/// Property to include in the CTE (column name and which node it belongs to)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeProperty {
    pub cypher_alias: String, // "u1" or "u2" - which node this property is for
    pub column_name: String,  // Actual column name in the table (e.g., "full_name")
    pub alias: String,        // Output alias (e.g., "name" or "u1_name")
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
    pub relationship_cypher_alias: String, // Original Cypher relationship alias (e.g., "r" in [r:FOLLOWS*])
    pub properties: Vec<NodeProperty>,     // Properties to include in the CTE
    pub database: Option<String>,          // Optional database prefix
    pub shortest_path_mode: Option<ShortestPathMode>, // Shortest path optimization mode
    pub start_node_filters: Option<String>, // WHERE clause for start node (e.g., "start_node.full_name = 'Alice'")
    pub end_node_filters: Option<String>, // WHERE clause for end node (e.g., "end_full_name = 'Bob'")
    pub relationship_filters: Option<String>, // WHERE clause for relationship (e.g., "rel.weight > 0.5")
    pub path_variable: Option<String>, // Path variable name from MATCH clause (e.g., "p" in "MATCH p = ...")
    pub relationship_types: Option<Vec<String>>, // Relationship type labels (e.g., ["FOLLOWS", "FRIENDS_WITH"])
    pub edge_id: Option<Identifier>, // Edge ID columns for relationship uniqueness (None = use from_id, to_id)
    pub is_denormalized: bool,       // True if BOTH nodes are virtual (for backward compat)
    pub start_is_denormalized: bool, // True if start node is virtual (properties come from edge table)
    pub end_is_denormalized: bool, // True if end node is virtual (properties come from edge table)
    // FK-edge pattern: edge table = node table with FK column (e.g., parent_id -> object_id)
    pub is_fk_edge: bool, // True if relationship is via FK on node table (no separate edge table)
    // Polymorphic edge fields - for filtering unified edge tables by type
    pub type_column: Option<String>, // Discriminator column for relationship type (e.g., "interaction_type")
    pub from_label_column: Option<String>, // Discriminator column for source node type
    pub to_label_column: Option<String>, // Discriminator column for target node type
    pub from_node_label: Option<String>, // Expected value for from_label_column (e.g., "User")
    pub to_node_label: Option<String>, // Expected value for to_label_column (e.g., "Post")
    // Heterogeneous polymorphic path fields - for paths like Group→*→User where
    // intermediate hops traverse Group→Group and only the final hop goes to User
    pub intermediate_node_table: Option<String>, // Table for intermediate nodes (e.g., "groups")
    pub intermediate_node_id_column: Option<String>, // ID column for intermediate nodes (e.g., "group_id")
    pub intermediate_node_label: Option<String>, // Label value for intermediate hops (e.g., "Group")
    // Weighted shortest path: use a pre-computed edge weight CTE instead of direct edge table
    pub weight_cte: Option<WeightCteConfig>,
    /// Whether the query uses `relationships(path)` — controls path_relationships array growth.
    /// When false and path_variable is set, path_relationships is generated as `[]` (no growth),
    /// saving ~24 bytes/element/hop in the recursive CTE. Default: true for backwards compat.
    pub needs_path_relationships: bool,
    /// Lightweight BFS mode for shortestPath queries that only need length(path).
    /// Generates a global-visited-set BFS (node_id, hop) instead of per-path tracking.
    pub use_bfs_mode: bool,
    /// True when the original edge direction is Either (undirected).
    /// BFS mode generates two UNION ALL branches for both traversal directions.
    pub is_undirected: bool,
    /// #617: true when this undirected VLP was normalized by the analyzer to a
    /// SINGLE directed walk over a doubled-edge set. NOT set for the arms of a
    /// legacy two-arm split (which also carry `is_undirected`) — gating on
    /// `is_undirected` here would turn each monotone arm into a complete
    /// undirected walk and double-count every path.
    pub undirected_single_walk: bool,
}

/// Configuration for weighted shortest path using a pre-computed edge weight CTE
#[derive(Debug, Clone)]
pub struct WeightCteConfig {
    /// Name of the CTE containing edge weights (e.g., "with_source_target_weight_cte_1")
    pub cte_name: String,
    /// Column name for source node ID in the weight CTE
    pub source_column: String,
    /// Column name for target node ID in the weight CTE
    pub target_column: String,
    /// Column name for edge weight in the weight CTE
    pub weight_column: String,
}

/// Mode for shortest path queries
#[derive(Debug, Clone, PartialEq)]
pub enum ShortestPathMode {
    /// shortestPath() - return one shortest path
    Shortest,
    /// allShortestPaths() - return all paths with minimum length
    AllShortest,
}

// Conversion from logical plan's ShortestPathMode to SQL generator's ShortestPathMode
impl From<crate::query_planner::logical_plan::ShortestPathMode> for ShortestPathMode {
    fn from(mode: crate::query_planner::logical_plan::ShortestPathMode) -> Self {
        use crate::query_planner::logical_plan::ShortestPathMode as LogicalMode;
        match mode {
            LogicalMode::Shortest => ShortestPathMode::Shortest,
            LogicalMode::AllShortest => ShortestPathMode::AllShortest,
        }
    }
}

impl<'a> VariableLengthCteGenerator<'a> {
    #[allow(clippy::too_many_arguments)] // VLP CTE generation requires the full pattern (nodes, rel, aliases, filters, mode); each arg is a distinct schema/query input
    pub fn new(
        schema: &'a GraphSchema, // Schema for constraint compilation
        spec: VariableLengthSpec,
        start_table: &str,             // Actual table name (e.g., "users")
        start_id_col: &str,            // ID column name (e.g., "user_id")
        relationship_table: &str,      // Actual relationship table name
        rel_from_col: &str,            // Relationship from column (e.g., "follower_id")
        rel_to_col: &str,              // Relationship to column (e.g., "followed_id")
        end_table: &str,               // Actual table name (e.g., "users")
        end_id_col: &str,              // ID column name (e.g., "user_id")
        start_alias: &str,             // Cypher alias (e.g., "u1")
        end_alias: &str,               // Cypher alias (e.g., "u2")
        properties: Vec<NodeProperty>, // Properties to include in CTE
        shortest_path_mode: Option<ShortestPathMode>, // Shortest path mode
        start_node_filters: Option<String>, // WHERE clause for start node
        end_node_filters: Option<String>, // WHERE clause for end node
        path_variable: Option<String>, // Path variable name (e.g., "p")
        relationship_types: Option<Vec<String>>, // Relationship type labels (e.g., ["FOLLOWS", "FRIENDS_WITH"])
        edge_id: Option<Identifier>,             // Edge ID for relationship uniqueness
    ) -> Self {
        Self::new_with_polymorphic(
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
            "", // relationship_cypher_alias - default empty for backward compat
            properties,
            shortest_path_mode,
            start_node_filters,
            end_node_filters,
            None, // relationship_filters - default None for backward compat
            path_variable,
            relationship_types,
            edge_id,
            None, // type_column
            None, // from_label_column
            None, // to_label_column
            None, // from_node_label
            None, // to_node_label
        )
    }

    /// Create a generator with polymorphic edge support
    #[allow(clippy::too_many_arguments)] // adds polymorphic-edge fields on top of `new`'s params; each is a distinct schema input
    pub fn new_with_polymorphic(
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
        relationship_cypher_alias: &str, // Cypher relationship alias (e.g., "r" in [r:FOLLOWS*])
        properties: Vec<NodeProperty>,
        shortest_path_mode: Option<ShortestPathMode>,
        start_node_filters: Option<String>,
        end_node_filters: Option<String>,
        relationship_filters: Option<String>, // Filters on relationship properties
        path_variable: Option<String>,
        relationship_types: Option<Vec<String>>,
        edge_id: Option<Identifier>,
        type_column: Option<String>,
        from_label_column: Option<String>,
        to_label_column: Option<String>,
        from_node_label: Option<String>,
        to_node_label: Option<String>,
    ) -> Self {
        Self::new_with_fk_edge(
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
            relationship_cypher_alias,
            properties,
            shortest_path_mode,
            start_node_filters,
            end_node_filters,
            relationship_filters,
            path_variable,
            relationship_types,
            edge_id,
            type_column,
            from_label_column,
            to_label_column,
            from_node_label,
            to_node_label,
            false, // is_fk_edge defaults to false
        )
    }

    /// Create a generator with polymorphic edge support and FK-edge flag
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_fk_edge(
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
        relationship_cypher_alias: &str, // Cypher relationship alias
        properties: Vec<NodeProperty>,
        shortest_path_mode: Option<ShortestPathMode>,
        start_node_filters: Option<String>,
        end_node_filters: Option<String>,
        relationship_filters: Option<String>, // Filters on relationship properties
        path_variable: Option<String>,
        relationship_types: Option<Vec<String>>,
        edge_id: Option<Identifier>,
        type_column: Option<String>,
        from_label_column: Option<String>,
        to_label_column: Option<String>,
        from_node_label: Option<String>,
        to_node_label: Option<String>,
        is_fk_edge: bool,
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
            relationship_cypher_alias: relationship_cypher_alias.to_string(),
            properties: dedup_node_properties(properties),
            database,
            shortest_path_mode,
            start_node_filters,
            end_node_filters,
            relationship_filters,
            path_variable,
            relationship_types,
            edge_id,
            is_denormalized: false,
            start_is_denormalized: false,
            end_is_denormalized: false,
            is_fk_edge,
            type_column,
            from_label_column,
            to_label_column,
            from_node_label,
            to_node_label,
            // Heterogeneous polymorphic path fields - set later via setter method
            intermediate_node_table: None,
            intermediate_node_id_column: None,
            intermediate_node_label: None,
            weight_cte: None,
            needs_path_relationships: true,
            use_bfs_mode: false,
            is_undirected: false,
            undirected_single_walk: false,
        }
    }

    /// Create a generator for mixed patterns (one node denormalized, one standard)
    #[allow(clippy::too_many_arguments)]
    pub fn new_mixed(
        schema: &'a GraphSchema, // Schema for constraint compilation
        spec: VariableLengthSpec,
        start_table: &str,  // Start node table (or rel table if start is denorm)
        start_id_col: &str, // Start ID column
        relationship_table: &str, // Relationship table
        rel_from_col: &str, // Relationship from column
        rel_to_col: &str,   // Relationship to column
        end_table: &str,    // End node table (or rel table if end is denorm)
        end_id_col: &str,   // End ID column
        start_alias: &str,  // Cypher alias for start node
        end_alias: &str,    // Cypher alias for end node
        relationship_cypher_alias: &str, // Cypher relationship alias
        properties: Vec<NodeProperty>, // Properties to include
        shortest_path_mode: Option<ShortestPathMode>,
        start_node_filters: Option<String>,
        end_node_filters: Option<String>,
        relationship_filters: Option<String>, // Filters on relationship properties
        path_variable: Option<String>,
        relationship_types: Option<Vec<String>>,
        edge_id: Option<Identifier>,
        start_is_denormalized: bool, // Whether start node is denormalized
        end_is_denormalized: bool,   // Whether end node is denormalized
    ) -> Self {
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
            relationship_cypher_alias: relationship_cypher_alias.to_string(),
            properties: dedup_node_properties(properties),
            database,
            shortest_path_mode,
            start_node_filters,
            end_node_filters,
            relationship_filters,
            path_variable,
            relationship_types,
            edge_id,
            is_denormalized: start_is_denormalized && end_is_denormalized, // Both must be denorm for full denorm mode
            start_is_denormalized,
            end_is_denormalized,
            is_fk_edge: false, // Mixed mode is not FK-edge
            // Polymorphic edge fields - not used for mixed mode yet
            type_column: None,
            from_label_column: None,
            to_label_column: None,
            from_node_label: None,
            to_node_label: None,
            // Heterogeneous polymorphic path fields - not used for mixed mode
            intermediate_node_table: None,
            intermediate_node_id_column: None,
            intermediate_node_label: None,
            weight_cte: None,
            needs_path_relationships: true,
            use_bfs_mode: false,
            is_undirected: false,
            undirected_single_walk: false,
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

    /// Generate polymorphic edge filter condition for JOIN ON clause
    /// For polymorphic edges (unified table with type discriminator), adds filters like:
    /// - `rel.interaction_type = 'FOLLOWS'` (type filter)
    /// - `rel.from_label = 'User'` (source node type filter)
    /// - `rel.to_label = 'User'` (target node type filter)
    ///
    /// For multiple relationship types (e.g., [:FOLLOWS|LIKES]):
    /// - `rel.interaction_type IN ('FOLLOWS', 'LIKES')`
    fn generate_polymorphic_edge_filter(&self) -> Option<String> {
        // Base arm: the from-side label discriminator uses the START label
        // (`from_node_label`), because the first hop leaves the query's start node.
        self.generate_polymorphic_edge_filter_with_from_label(None)
    }

    /// #689: polymorphic edge filter for the RECURSIVE arm of a
    /// from-side-polymorphic cross-type VLP (see
    /// `is_from_side_polymorphic_cross_type`). Identical to the base filter
    /// EXCEPT the from-side label discriminator uses the END label
    /// (`to_node_label`, e.g. `'Group'`) instead of the base arm's START label
    /// (`from_node_label`, `'User'`): the recursive hops are Group→Group, so the
    /// edge's *source* is now a Group. The base arm keeps the start label.
    ///
    /// Falls back to the base filter (identical output) when `to_node_label` is
    /// absent/empty, so it is never worse than the pre-#689 behavior.
    fn generate_polymorphic_edge_filter_from_recursive(&self) -> Option<String> {
        match self.to_node_label {
            Some(ref l) if !l.is_empty() && self.is_from_side_polymorphic_cross_type() => {
                self.generate_polymorphic_edge_filter_with_from_label(Some(l.as_str()))
            }
            _ => self.generate_polymorphic_edge_filter(),
        }
    }

    /// Shared implementation of the VLP polymorphic edge filter. `from_label_override`
    /// replaces the from-side discriminator value (`from_node_label`) — `None` keeps
    /// the start label (base arm), `Some("Group")` is the recursing/end label used by
    /// the from-side-polymorphic recursive arm (#689). All schema-axis reads live in
    /// this single helper so both arms branch identically.
    fn generate_polymorphic_edge_filter_with_from_label(
        &self,
        from_label_override: Option<&str>,
    ) -> Option<String> {
        let mut filter_parts = Vec::new();

        // Add type filter if type_column is defined
        if let Some(ref type_col) = self.type_column {
            if let Some(ref rel_types) = self.relationship_types {
                if rel_types.len() == 1 {
                    // Single type: use equality
                    filter_parts.push(format!(
                        "{}.{} = '{}'",
                        self.relationship_alias, type_col, rel_types[0]
                    ));
                } else if rel_types.len() > 1 {
                    // Multiple types: use IN clause
                    let types_list = rel_types
                        .iter()
                        .map(|t| format!("'{}'", t))
                        .collect::<Vec<_>>()
                        .join(", ");
                    filter_parts.push(format!(
                        "{}.{} IN ({})",
                        self.relationship_alias, type_col, types_list
                    ));
                }
            }
        }

        // Add from-side label filter if that discriminator column is defined.
        // The value is the override (recursing/end label) when provided, else
        // the base start label.
        if let Some(ref from_label_col) = self.from_label_column {
            let from_label = from_label_override.or(self.from_node_label.as_deref());
            if let Some(from_label) = from_label {
                filter_parts.push(format!(
                    "{}.{} = '{}'",
                    self.relationship_alias, from_label_col, from_label
                ));
            }
        }

        // Add to_label filter if to_label_column is defined
        if let Some(ref to_label_col) = self.to_label_column {
            if let Some(ref to_label) = self.to_node_label {
                filter_parts.push(format!(
                    "{}.{} = '{}'",
                    self.relationship_alias, to_label_col, to_label
                ));
            }
        }

        if filter_parts.is_empty() {
            None
        } else {
            let filter = filter_parts.join(" AND ");
            crate::debug_print!("    🔹 VLP polymorphic edge filter: {}", filter);
            Some(filter)
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
                                    return Some(compiled_sql);
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

        Some(compiled)
    }

    /// Generate relationship type expression for a given hop
    ///
    /// Relationship types may arrive as composite schema keys
    /// (`TYPE::FromLabel::ToLabel`); only the Cypher-visible type name is
    /// emitted into path_relationships (#485). Composite keys stay internal —
    /// they exist to disambiguate schema lookups, never for query output.
    fn generate_relationship_type_for_hop(&self, _hop_count: u32) -> String {
        use crate::graph_catalog::composite_key_utils::extract_type_name;
        // For now, return the first relationship type if available, otherwise a placeholder
        if let Some(ref types) = self.relationship_types {
            if let Some(first_type) = types.first() {
                format!(
                    "{} as path_relationships",
                    arr(&format!("'{}'", extract_type_name(first_type)))
                )
            } else {
                format!("{} as path_relationships", arr(""))
            }
        } else {
            format!("{} as path_relationships", arr(""))
        }
    }

    /// Get relationship type array for appending in recursive case
    ///
    /// Emits the Cypher-visible type name, never the composite schema key
    /// (see `generate_relationship_type_for_hop`, #485).
    fn get_relationship_type_array(&self) -> String {
        use crate::graph_catalog::composite_key_utils::extract_type_name;
        let mapper = crate::sql_generator::function_mapper::current_function_mapper();
        if let Some(ref types) = self.relationship_types {
            if let Some(first_type) = types.first() {
                mapper.array_literal(&format!("'{}'", extract_type_name(first_type)))
            } else {
                mapper.array_literal("")
            }
        } else {
            mapper.array_literal("")
        }
    }

    /// Check if this is a heterogeneous polymorphic path (e.g., Group→*→User)
    /// where intermediate hops traverse through one type and final hop goes to another type.
    ///
    /// Conditions for heterogeneous polymorphic path:
    /// 1. has to_label_column (polymorphic edge with target type discriminator)
    /// 2. start_node_table != end_node_table (different node types)
    /// 3. intermediate_node_table is set (specifies intermediate traversal type)
    fn is_heterogeneous_polymorphic_path(&self) -> bool {
        self.to_label_column.is_some()
            && self.start_node_table != self.end_node_table
            && self.intermediate_node_table.is_some()
    }

    /// #689: is this a directed cross-type VLP over a *from-side-polymorphic*
    /// edge — i.e. the edge discriminates its SOURCE node via a from-side label
    /// column (e.g. `ds_memberships.member_type ∈ {User, Group}`) and has no
    /// target-side label column (the target node label is fixed, e.g.
    /// `MEMBER_OF → Group`)?
    ///
    /// For such an edge a `(:User)-[:MEMBER_OF*1..N]->(:Group)` traversal
    /// legitimately recurses **Group→Group** (a group can be a member of a
    /// group). The recursive arm must therefore (a) re-join the END-node table
    /// (`ds_groups`), exactly like the base arm, NOT the START table — see the
    /// `recursive_end_table` heuristic below; and (b) discriminate the edge's
    /// *from* side with the END label (`member_type = 'Group'`), not the base
    /// arm's start label (`'User'`). Both recursive-arm fixes gate on this
    /// predicate so they agree by construction.
    ///
    /// Deliberately narrow (from-side label present, target-side absent,
    /// cross-table) so it cannot touch a plain cross-type recursive VLP or the
    /// multi-type / heterogeneous generators.
    fn is_from_side_polymorphic_cross_type(&self) -> bool {
        self.start_node_table != self.end_node_table
            && self.from_label_column.is_some()
            && self.to_label_column.is_none()
    }

    /// Set intermediate node info for heterogeneous polymorphic paths
    pub fn set_intermediate_node(&mut self, table: &str, id_column: &str, label: &str) {
        self.intermediate_node_table = Some(table.to_string());
        self.intermediate_node_id_column = Some(id_column.to_string());
        self.intermediate_node_label = Some(label.to_string());
    }

    /// Set weight CTE configuration for weighted shortest path
    pub fn set_weight_cte(&mut self, config: WeightCteConfig) {
        self.weight_cte = Some(config);
    }

    /// Whether this VLP query needs full path return data (path_relationships).
    ///
    /// Cycle detection uses node-uniqueness via `path_nodes` arrays (NOT has(vp.path_nodes, end_id)).
    /// This is more memory-efficient than edge-uniqueness (no separate path_edges arrays) and
    /// equivalent for simple graphs (at most one edge per type between any node pair).
    /// `path_relationships` is populated whenever a path variable is bound
    /// (`MATCH p = ...`, `shortestPath(...)`). This ensures `relationships(p)` works
    /// even when called inside WITH clauses (where the root_plan detection can't see
    /// the path function usage above the VLP subtree).
    fn needs_path_data(&self) -> bool {
        self.path_variable.is_some()
    }

    /// Build the SQL expression for the end node ID.
    /// Returns expression like `end_node.PersonId` or `concat(toString(end_node.col1), ...)`.
    /// Used for node-uniqueness cycle detection in path_nodes arrays.
    fn build_end_node_id_expr(&self) -> String {
        let end_id_identifier = Identifier::from_comma_separated(&self.end_node_id_column);
        emit_id_expr(&self.end_node_alias, &end_id_identifier)
    }

    /// Whether the standard directed recursive VLP should enforce EDGE-uniqueness
    /// (relationship-uniqueness — Cypher's default: a path MAY revisit a node but
    /// must not reuse the same edge) via a `path_edges` array, instead of the
    /// stronger node-uniqueness via `path_nodes`.
    ///
    /// Scoped (issue #598, part 2) to the standard single-type directed recursive
    /// path. This helper is only consulted from the STANDARD arms of
    /// `generate_base_case` and `generate_recursive_case_with_cte_name`, which are
    /// reached only after those functions' early dispatch returns for the weighted
    /// / denormalized / mixed / FK-edge / heterogeneous-polymorphic strategies — so
    /// the pattern is already known to be standard here (no need to re-branch on raw
    /// schema flags). Those other strategies keep node-uniqueness in their own
    /// recursive generators (tracked as separate follow-ups).
    ///
    /// It stays `false` — keeping node-uniqueness — for:
    /// - shortestPath (revisiting a node can never shorten a path);
    /// - zero-hop base cases (`*0..N`, `effective_min_hops() == 0`), whose base row
    ///   has no edges and so cannot seed the 1-hop `path_edges` literal;
    /// - heterogeneous-polymorphic paths: these never reach the standard
    ///   base/recursive arms at all — `generate_recursive_sql` returns early via
    ///   `generate_heterogeneous_polymorphic_sql()` (a separate two-CTE builder)
    ///   whenever `is_heterogeneous_polymorphic_path()` is true. That builder does
    ///   NOT project `path_edges`, so this predicate stays `false` for them to keep
    ///   the standard arms (used by other patterns) node-unique and avoid seeding a
    ///   `path_edges` column the hetero path would never carry (cf. #469).
    ///
    /// When `true`, `path_edges` is threaded consistently through the base and
    /// recursive arms (and carried by `SELECT *` in the min-hops `_inner` wrapper),
    /// and the recursive cycle predicate switches to [`emit_edge_cycle_check`].
    fn uses_edge_uniqueness(&self) -> bool {
        self.shortest_path_mode.is_none()
            && !self.is_heterogeneous_polymorphic_path()
            && (self.spec.effective_min_hops() >= 1
                // #628: a CLOSED `*0..N` pattern (`(a)-[*0..N]->(a)`) must count
                // real cycles, which requires EDGE-uniqueness — node-uniqueness
                // structurally forbids returning to the start, so every real
                // cycle is dropped and only the zero-length self rows survive
                // (documented in filter_builder.rs, #625). The zero-hop base has
                // no edge, but it CAN seed an empty `path_edges` array (`[] as
                // path_edges`, ClickHouse `Array(Nothing)` which unifies with the
                // recursive arm's real tuple type on `arrayConcat`); hops ≥ 1
                // accumulate and dedupe normally. Scoped to the closed case so an
                // OPEN `*0..N` (whose node-uniqueness is a separate, non-cyclic
                // concern) is unchanged.
                || (self.spec.effective_min_hops() == 0 && self.is_closed_pattern()))
    }

    /// Whether this VLP is a CLOSED pattern — the same Cypher variable on both
    /// endpoints (`(a)-[*..]->(a)`). The planner leaves the two connection
    /// aliases equal for a same-variable pattern (never renaming one), so the
    /// generator sees `start_cypher_alias == end_cypher_alias`. A closed pattern
    /// counts cycles (the outer query adds `start_id = end_id`); see #625/#628.
    fn is_closed_pattern(&self) -> bool {
        self.start_cypher_alias == self.end_cypher_alias
    }

    /// #617: whether this VLP walks a DOUBLED-EDGE set instead of the raw edge
    /// table. True for the single-type undirected VLP the analyzer normalized
    /// (`was_undirected` → `is_undirected` here) on the standard/polymorphic
    /// strategy. Each physical edge appears in both orientations in a sibling
    /// `<cte>_edges` CTE, carrying its ORIGINAL `(from, to)` as
    /// `__cg_orig_from`/`__cg_orig_to` so trail-uniqueness still deduplicates a
    /// physical edge regardless of traversal orientation. The walk itself stays
    /// the ordinary directed recursion — per-hop direction freedom comes from
    /// the doubled rows (fixes the two-monotone-arm under-count).
    ///
    /// Excludes shortestPath (still on the legacy two-arm split — the reverse
    /// arm of that split also arrives here with `is_undirected` set), BFS and
    /// weighted modes, and every non-standard strategy (those generators keep
    /// their legacy behavior; the analyzer scope guarantees in-scope patterns
    /// are standard/polymorphic, so these conditions are belt-and-braces).
    fn uses_doubled_edges(&self) -> bool {
        // `undirected_single_walk` is derived from the ONE shared scope
        // predicate (`undirected_vlp_single_walk_core`), which already
        // guarantees a plain (standard/polymorphic) same-label edge table —
        // no raw schema-flag re-checks here (axis-dispatch rule). The mode
        // exclusions below are query-shape, not schema-pattern, axes.
        self.undirected_single_walk
            && self.shortest_path_mode.is_none()
            && !self.use_bfs_mode
            && self.weight_cte.is_none()
            && !self.is_heterogeneous_polymorphic_path()
    }

    /// Name of the doubled-edge sibling CTE (see [`Self::uses_doubled_edges`]).
    fn doubled_edges_cte_name(&self) -> String {
        undirected_doubled_edges_cte_name(
            &self.start_cypher_alias,
            &self.end_cypher_alias,
            &self.relationship_table,
        )
    }

    /// The relation the base/recursive standard arms join for each hop:
    /// the doubled-edge CTE when [`Self::uses_doubled_edges`], else the raw
    /// edge table.
    fn rel_source(&self) -> String {
        if self.uses_doubled_edges() {
            self.doubled_edges_cte_name()
        } else {
            self.format_table_name(&self.relationship_table)
        }
    }

    /// Passthrough columns the doubled-edge CTE must project besides the
    /// swapped from/to id columns. Delegates to the schema catalog via
    /// [`doubled_edges_passthrough_columns`] (the generator's discriminator
    /// and edge-id fields are themselves populated from that same schema, so
    /// no local merge is needed).
    fn doubled_edges_passthrough_columns(&self) -> Vec<String> {
        doubled_edges_passthrough_columns(
            self.schema,
            self.relationship_types
                .as_ref()
                .and_then(|types| types.first().map(String::as_str)),
            &self.relationship_table,
        )
    }

    /// Build the doubled-edge CTE definition text (`name AS (...)`, no trailing
    /// comma). See [`Self::uses_doubled_edges`] for semantics.
    fn generate_doubled_edges_cte(&self) -> String {
        format!(
            "{} AS (\n{}\n)",
            self.doubled_edges_cte_name(),
            build_doubled_edges_cte_body(
                &self.format_table_name(&self.relationship_table),
                &self.relationship_from_column,
                &self.relationship_to_column,
                &self.doubled_edges_passthrough_columns(),
            )
        )
    }

    /// Extract target node ID value from end_node_filters for early termination in recursive case.
    ///
    /// For shortestPath queries, once a path reaches the target node, extending it further
    /// is wasteful — any extended path would be longer. Returns `Some("vp.end_id != <value>")`
    /// when the end filter is a simple equality on the ID column.
    ///
    /// Only matches simple patterns like `end_node.id = <value>` to avoid false positives
    /// from complex multi-predicate filters.
    #[allow(dead_code)]
    fn extract_target_id_negation(&self) -> Option<String> {
        let filter = self.end_node_filters.as_ref()?;

        // Build the expected prefix: "end_node.id_column = "
        let id_prefix = format!("{}.{} = ", self.end_node_alias, self.end_node_id_column);

        // Only match simple single-equality filters (no AND/OR)
        let trimmed = filter.trim();
        if !trimmed.starts_with(&id_prefix) {
            return None;
        }
        // Check that there's no additional predicate (AND/OR)
        let value_part = trimmed[id_prefix.len()..].trim();
        if value_part.contains(" AND ")
            || value_part.contains(" OR ")
            || value_part.contains(" and ")
            || value_part.contains(" or ")
        {
            return None;
        }

        Some(format!("vp.end_id != {}", value_part))
    }

    /// #617: on the doubled-edge walk, an edge-identity column reference must
    /// resolve to the ORIGINAL-orientation value: the from/to id columns are
    /// swapped in reverse-orientation rows, so referencing them directly would
    /// give one physical edge two distinct identities (breaking
    /// trail-uniqueness). Any other column is orientation-independent and
    /// passes through unchanged.
    fn edge_identity_column<'c>(&self, col: &'c str) -> &'c str {
        if self.uses_doubled_edges() {
            if col == self.relationship_from_column {
                return DOUBLED_EDGES_ORIG_FROM;
            }
            if col == self.relationship_to_column {
                return DOUBLED_EDGES_ORIG_TO;
            }
        }
        col
    }

    /// Build the edge-identity tuple for one hop, reading the edge columns off
    /// `rel_alias`. Used by both the base case (`rel_alias` =
    /// `self.relationship_alias`) and the recursive case.
    ///
    /// Returns SQL like `tuple(r.from_id, r.to_id)` or `tuple(r.date, r.num, …)`.
    /// Shape depends on `self.edge_id`: a `Single` column emits a bare
    /// `rel.col`; a `Composite` key builds a `tuple(...)`; `None` defaults to the
    /// `(from, to)` node pair. #617 doubled-edge walk: from/to are SWAPPED in
    /// reverse-orientation rows, so the `None` identity comes from the
    /// original-orientation columns — otherwise the same physical edge traversed
    /// the other way would look like a different relationship and
    /// trail-uniqueness would not hold.
    fn build_edge_tuple_recursive(&self, rel_alias: &str) -> String {
        match &self.edge_id {
            Some(Identifier::Single(col)) => {
                format!("{}.{}", rel_alias, self.edge_identity_column(col))
            }
            Some(Identifier::Composite(cols)) => {
                let tuple_elements: Vec<String> = cols
                    .iter()
                    .map(|col| format!("{}.{}", rel_alias, self.edge_identity_column(col)))
                    .collect();
                format!(
                    "{}({})",
                    crate::sql_generator::function_mapper::current_function_mapper()
                        .tuple_constructor(),
                    tuple_elements.join(", ")
                )
            }
            None => {
                // #617: original-orientation identity on the doubled-edge walk
                // (see this fn's doc comment).
                let (from_c, to_c) = if self.uses_doubled_edges() {
                    (DOUBLED_EDGES_ORIG_FROM, DOUBLED_EDGES_ORIG_TO)
                } else {
                    (
                        self.relationship_from_column.as_str(),
                        self.relationship_to_column.as_str(),
                    )
                };
                format!(
                    "{}({}.{}, {}.{})",
                    crate::sql_generator::function_mapper::current_function_mapper()
                        .tuple_constructor(),
                    rel_alias,
                    from_c,
                    rel_alias,
                    to_c
                )
            }
        }
    }

    /// Build the `(from_id, to_id)` edge-identity tuple for ONE FK-edge hop,
    /// reading the two node-id columns off the given SQL aliases.
    ///
    /// FK-edge relationships have no separate edge table and no dedicated
    /// edge-id column — the edge IS the `from_alias.<from_id> → to_alias.<to_id>`
    /// node pair (e.g. `child.parent_id → parent.object_id`). So for #606
    /// relationship-uniqueness the ordered node-id pair uniquely identifies the
    /// physical edge row. `from_id_col`/`to_id_col` are the node id columns for
    /// each side (self-referencing FK-edge → same column name on both sides).
    ///
    /// #713: composite-aware. Each id column string may be comma-separated
    /// (`"region, object_id"`); we parse it to an `Identifier` and splice every
    /// physical column into the tuple, so the edge identity is the full ordered
    /// column list `(from.c1, from.c2, …, to.c1, to.c2, …)`. Single-column ids
    /// degrade byte-identically to `tuple(from.col, to.col)`.
    /// #902: return the FK column to follow for a self-referencing FK-edge hop.
    ///
    /// An FK-edge hop `(child)-[:R]->(parent)` joins `child.<FK> = parent.<PK>`
    /// where `<PK>` is the node_id and `<FK>` is the edge column pointing at it.
    /// The schemas declare the FK inconsistently across the from/to roles:
    ///   filesystem PARENT: node_id=object_id, from_id=parent_id (FK),
    ///     to_id=object_id (PK) → FK = from_id.
    ///   ldbc REPLY_OF: node_id=commentId, from_id=commentId (PK),
    ///     to_id=replyOfCommentId (FK) → FK = to_id.
    /// So the FK is whichever of {from_id, to_id} is NOT the node_id. The
    /// recursive/base FK-edge joins previously hardcoded `relationship_from_column`
    /// as the FK; when `from_id == node_id` that made every hop an IDENTITY
    /// self-join (`x.commentId = y.commentId`), collapsing all paths to phantom
    /// self-loops (#902). This mirrors the single-hop path
    /// (`join_generation.rs` #632/#646), which resolves the same way.
    ///
    /// Only self-referencing FK-edges (start table == end table) are ambiguous;
    /// for a non-self-ref FK-edge the two aliases are DIFFERENT tables and the
    /// original `relationship_from_column` pairing is correct, so this returns
    /// it unchanged (byte-identical). `node_id_col` is the node_id column set of
    /// the joined-to (PK) side. (Called only from the FK-edge base/append/prepend
    /// join sites, so the FK-edge-pattern precondition is already established by
    /// the caller — the self-ref-table test below is the only remaining gate.)
    fn fk_hop_fk_column(&self, node_id_col: &str) -> String {
        if self.start_node_table != self.end_node_table {
            return self.relationship_from_column.clone();
        }
        let from_id = Identifier::from_comma_separated(&self.relationship_from_column);
        let node_id = Identifier::from_comma_separated(node_id_col);
        // The PK side is whichever role's columns equal the node_id's; the FK is
        // the other. For single-column ids this is the plain `from_id == node_id`
        // test. Composite ids compare the full ordered column sets.
        if from_id.columns() == node_id.columns() {
            self.relationship_to_column.clone()
        } else {
            self.relationship_from_column.clone()
        }
    }

    fn build_fk_edge_tuple(
        &self,
        from_alias: &str,
        from_id_col: &str,
        to_alias: &str,
        to_id_col: &str,
    ) -> String {
        let tuple_ctor =
            crate::sql_generator::function_mapper::current_function_mapper().tuple_constructor();
        let from_id = Identifier::from_comma_separated(from_id_col);
        let to_id = Identifier::from_comma_separated(to_id_col);
        let mut parts: Vec<String> = Vec::new();
        for c in from_id.columns() {
            parts.push(format!(
                "{}.{}",
                from_alias,
                crate::clickhouse_query_generator::quote_identifier(c)
            ));
        }
        for c in to_id.columns() {
            parts.push(format!(
                "{}.{}",
                to_alias,
                crate::clickhouse_query_generator::quote_identifier(c)
            ));
        }
        format!("{}({})", tuple_ctor, parts.join(", "))
    }

    /// Get the ClickHouse array type for path_edges
    /// Returns type like: `Array(Tuple(UInt32, UInt32))` or `Array(Tuple(String, String, ...))`
    #[allow(dead_code)]
    fn get_path_edges_array_type(&self) -> String {
        match &self.edge_id {
            Some(Identifier::Single(_)) => {
                // For single column, we don't know the type - assume UInt64 for now
                // TODO: Get actual column type from schema
                "Array(Int64)".to_string()
            }
            Some(Identifier::Composite(cols)) => {
                // For composite keys, build tuple type
                // TODO: Get actual column types from schema - assuming String for now
                let type_elements = vec!["String"; cols.len()].join(", ");
                format!("Array(Tuple({}))", type_elements)
            }
            None => {
                // Default (from_id, to_id) - assume both are UInt64
                "Array(Tuple(Int64, Int64))".to_string()
            }
        }
    }

    /// Generate the recursive CTE for variable-length traversal
    pub fn generate_cte(&self) -> Cte {
        let cte_sql = self.generate_recursive_sql();

        Cte::new_vlp(
            self.cte_name.clone(),
            crate::render_plan::CteContent::RawSql(cte_sql),
            true, // is_recursive
            self.start_node_alias.clone(),
            self.end_node_alias.clone(),
            self.start_node_table.clone(),
            self.end_node_table.clone(),
            self.start_cypher_alias.clone(),   // Add Cypher alias
            self.end_cypher_alias.clone(),     // Add Cypher alias
            self.start_node_id_column.clone(), // 🔧 FIX: Pass actual ID columns (from rel schema)
            self.end_node_id_column.clone(),
            self.path_variable.clone(), // Path variable for length(p), nodes(p) rewriting
        )
    }

    /// Rewrite end node filter for use in intermediate CTEs
    /// Transforms "end_node.property" references to "end_property" column names
    /// #607: does a standard (non-shortestPath, non-denormalized) VLP apply its
    /// end-node WHERE filter in the OUTER wrapper CTE rather than in the base /
    /// recursive cases?
    ///
    /// The base/recursive `end_node` is the node reached at THIS hop — an
    /// intermediate node on any longer path — so filtering it there silently
    /// drops valid paths whose FINAL endpoint satisfies the predicate (#607).
    /// A standard VLP that can produce paths beyond hop 1 (has a recursive arm,
    /// or min_hops forces a wrapper) must therefore defer the end filter to the
    /// wrapper, which sees the true endpoint (`end_id`/`end_*`). This mirrors the
    /// denormalized and shortestPath families, which already wrap.
    ///
    /// Returns false only when there is no wrapper to carry the filter — a pure
    /// single-hop standard VLP with no recursion (base case IS the endpoint).
    /// (shortestPath and denormalized have their own dedicated wrapper handling
    /// and are excluded here; their end-filter placement is unchanged.)
    fn end_filter_applied_in_wrapper(&self) -> bool {
        if self.end_node_filters.is_none()
            || self.shortest_path_mode.is_some()
            || self.is_denormalized
        {
            return false;
        }
        // Standard VLP: the outer wrapper carries the end filter whenever the
        // VLP can reach a hop beyond the base case (recursive arm present) or a
        // min-hop wrapper already exists. Mirrors `needs_recursion` / the
        // `needs_inner_cte` decision in `generate_sql`.
        let min_hops = self.spec.effective_min_hops();
        let max_hops = self.spec.max_hops;
        let base_hop_count = if min_hops == 0 { 0 } else { 1 };
        let has_recursive_arm =
            max_hops != Some(0) && (max_hops.is_none() || max_hops.unwrap() > base_hop_count);
        has_recursive_arm || min_hops > 1
    }

    /// #607: should the end-node WHERE filter be injected in the base AND
    /// recursive cases (rather than the outer wrapper)?
    ///
    /// This preserves the ORIGINAL placement for every family except the one
    /// #607 fixes:
    /// - shortestPath: NO (its `_to_target` wrapper applies it — unchanged).
    /// - denormalized: YES here as before (its wrapper re-applies it too; that
    ///   pre-existing double-application is harmless and left untouched).
    /// - standard multi-hop-capable VLP: NO — deferred to the wrapper
    ///   (`end_filter_applied_in_wrapper`), the actual #607 fix.
    /// - standard pure single-hop VLP: YES — base case IS the endpoint, no
    ///   wrapper exists to carry the filter.
    fn end_filter_in_base_recursive_case(&self) -> bool {
        if self.shortest_path_mode.is_some() {
            return false;
        }
        !self.end_filter_applied_in_wrapper()
    }

    fn rewrite_end_filter_for_cte(&self, filter: &str) -> String {
        let mut rewritten = filter.replace(
            &format!("{}.{}", self.end_node_alias, self.end_node_id_column),
            VLP_END_ID_COLUMN,
        );

        // Replace end_node.{property} with end_{property} for each property
        // Try both ClickHouse column name and Cypher alias since filters can use either
        for prop in &self.properties {
            if prop.cypher_alias == self.end_cypher_alias {
                // Try ClickHouse column name (e.g., end_node.full_name → end_name)
                let pattern_col = format!("{}.{}", self.end_node_alias, prop.column_name);
                let replacement = format!("end_{}", prop.alias);
                rewritten = rewritten.replace(&pattern_col, &replacement);

                // Also try Cypher alias (e.g., end_node.name → end_name)
                let pattern_alias = format!("{}.{}", self.end_node_alias, prop.alias);
                rewritten = rewritten.replace(&pattern_alias, &replacement);
            }
        }

        rewritten
    }

    // Note: extract_simple_equality_filter was removed as dead code (never called)

    /// Extract an ID value from a simple equality filter like
    /// `"start_node.PersonId = CAST(14, 'UInt64')"` or `"start_node.PersonId = {person1Id}"`.
    /// Returns the RHS value expression.
    ///
    /// Rejects filters containing OR or multiple equality predicates beyond simple
    /// `alias.col = VALUE [AND ...]` form to avoid enabling BFS mode with compound
    /// expressions that would produce invalid SQL.
    pub fn extract_id_from_filter(
        filter: &str,
        node_alias: &str,
        id_column: &str,
    ) -> Option<String> {
        // Reject filters with OR (case-insensitive) — these are compound predicates
        // that cannot be safely reduced to a single ID value.
        if filter.contains(" OR ") || filter.contains(" or ") {
            return None;
        }

        // Try patterns: "alias.col = VALUE" or "alias.`col` = VALUE"
        let patterns = [
            format!("{}.{} = ", node_alias, id_column),
            format!(
                "{}.{} = ",
                node_alias,
                crate::clickhouse_query_generator::quote_identifier(id_column)
            ),
        ];
        for pattern in &patterns {
            if let Some(pos) = filter.find(pattern.as_str()) {
                let value_start = pos + pattern.len();
                // Extract value up to end or next AND
                let rest = &filter[value_start..];
                let value = if let Some(and_pos) = rest.find(" AND ") {
                    rest[..and_pos].trim()
                } else {
                    rest.trim()
                };
                // Strip trailing parentheses that might be from wrapping
                let value = value.trim_end_matches(')');
                // Reject if the extracted value contains operators (compound expression)
                if value.is_empty()
                    || value.contains(" OR ")
                    || value.contains(" or ")
                    || value.contains(" != ")
                    || value.contains(" <> ")
                {
                    return None;
                }
                return Some(value.to_string());
            }
        }
        None
    }

    /// Generate lightweight BFS SQL for shortestPath queries that only need length(path).
    ///
    /// Instead of per-path tracking with growing path_nodes arrays (~500M rows for KNOWS graph),
    /// this generates a global-visited-set BFS that tracks distinct reachable node_ids per hop
    /// level (~180K rows). The result CTE is compatible with the existing VLP pipeline.
    fn generate_bfs_shortest_path_sql(&self) -> String {
        let max_hops = self.spec.max_hops.unwrap_or_else(get_default_max_hops);
        let rel_table = self.format_table_name(&self.relationship_table);
        let from_col = &self.relationship_from_column;
        let to_col = &self.relationship_to_column;
        let fmap = current_function_mapper();
        let empty_str_arr = fmap.empty_string_array_cast();
        let empty_i64_arr = fmap.empty_int64_array_cast();
        let cast_u16 = fmap.cast_uint16();

        // Extract start and target IDs from filters
        let start_id = self
            .start_node_filters
            .as_ref()
            .and_then(|f| {
                Self::extract_id_from_filter(f, &self.start_node_alias, &self.start_node_id_column)
            })
            .unwrap_or_else(|| {
                // Fallback: use generic start filter expression
                format!("{}.{}", self.start_node_alias, self.start_node_id_column)
            });

        let target_id = self.end_node_filters.as_ref().and_then(|f| {
            Self::extract_id_from_filter(f, &self.end_node_alias, &self.end_node_id_column)
        });

        // Build the BFS recursive CTE name (without _inner suffix)
        let bfs_cte_name = format!("{}_bfs", self.cte_name);

        let stop_at_target = target_id
            .as_ref()
            .map(|t| format!("\n      AND b.node_id != {}", t))
            .unwrap_or_default();

        // Recursive body. ClickHouse accepts N-ary `UNION ALL` for recursive
        // CTEs, so an undirected edge is emitted as two separate recursive
        // branches (forward + reverse). Spark/Databricks requires exactly
        // two children under the recursive `UNION ALL` (anchor + recursive),
        // so the undirected case is collapsed to a single recursive branch
        // that joins against a bidirectional sub-`UNION ALL` over the rel
        // table. The bidirectional sub-`UNION ALL` is non-recursive, so it
        // doesn't run afoul of the 2-child rule.
        let dialect = crate::server::query_context::get_current_dialect();
        let recursive_body = if self.is_undirected
            && matches!(dialect, crate::sql_generator::SqlDialect::Databricks)
        {
            format!(
                "    SELECT DISTINCT neighbor.node_id AS node_id, b.hop + 1 AS hop\n    \
                 FROM {bfs_cte} b\n    \
                 JOIN (\n        \
                     SELECT {from_col} AS prev, {to_col} AS node_id FROM {rel_table}\n        \
                     UNION ALL\n        \
                     SELECT {to_col} AS prev, {from_col} AS node_id FROM {rel_table}\n    \
                 ) AS neighbor ON neighbor.prev = b.node_id\n    \
                 WHERE b.hop < {max_hops}\n      \
                 AND neighbor.node_id NOT IN (SELECT node_id FROM {bfs_cte}){stop_at_target}",
                bfs_cte = bfs_cte_name,
                rel_table = rel_table,
                from_col = from_col,
                to_col = to_col,
                max_hops = max_hops,
                stop_at_target = stop_at_target,
            )
        } else {
            // ClickHouse path — and Spark directed case — keep the original
            // 2- or 3-branch UNION ALL form (matches CH behavior byte-for-byte
            // when not undirected on Databricks).
            let forward = format!(
                "    SELECT DISTINCT rel.{to_col} AS node_id, b.hop + 1 AS hop\n    \
                 FROM {bfs_cte} b\n    \
                 JOIN {rel_table} rel ON rel.{from_col} = b.node_id\n    \
                 WHERE b.hop < {max_hops}\n      \
                 AND rel.{to_col} NOT IN (SELECT node_id FROM {bfs_cte}){stop_at_target}",
                to_col = to_col,
                bfs_cte = bfs_cte_name,
                rel_table = rel_table,
                from_col = from_col,
                max_hops = max_hops,
                stop_at_target = stop_at_target,
            );
            let reverse = if self.is_undirected {
                format!(
                    "\n    UNION ALL\n    \
                     SELECT DISTINCT rel.{from_col} AS node_id, b.hop + 1 AS hop\n    \
                     FROM {bfs_cte} b\n    \
                     JOIN {rel_table} rel ON rel.{to_col} = b.node_id\n    \
                     WHERE b.hop < {max_hops}\n      \
                     AND rel.{from_col} NOT IN (SELECT node_id FROM {bfs_cte}){stop_at_target}",
                    from_col = from_col,
                    bfs_cte = bfs_cte_name,
                    rel_table = rel_table,
                    to_col = to_col,
                    max_hops = max_hops,
                    stop_at_target = stop_at_target,
                )
            } else {
                String::new()
            };
            format!("{forward}{reverse}")
        };

        // Build BFS CTE. On Spark/Databricks a bare integer literal in the
        // anchor is inferred as `INT` (32-bit), but the recursive branch
        // pulls `node_id` from a BIGINT rel column — Spark refuses to merge
        // them under `UNION ALL` (`CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE`).
        // Explicitly cast the anchor to int64 there. Only apply when the
        // start_id is an integer literal (digits with optional leading `-`):
        // column references (`p.id`) already inherit the column's type, and
        // string literals (`'abc'`) would break under a numeric cast and
        // shouldn't be touched. ClickHouse promotes small-int literals
        // automatically, so its anchor stays unchanged either way.
        let anchor_start = if matches!(dialect, crate::sql_generator::SqlDialect::Databricks)
            && is_integer_literal(&start_id)
        {
            format!("{}({})", fmap.cast_int64(), start_id)
        } else {
            start_id.clone()
        };
        let bfs_cte_sql = format!(
            "{bfs_cte} AS (\n    \
             SELECT {anchor_start} AS node_id, {cast_u16}(0) AS hop\n    \
             UNION ALL\n{recursive_body}\n)",
            bfs_cte = bfs_cte_name,
            anchor_start = anchor_start,
            recursive_body = recursive_body,
        );

        // Build result wrapper CTE that matches VLP output schema.
        // This makes the BFS output compatible with existing rewrite_expr_for_vlp() logic:
        // - start_id, end_id for JOIN conditions
        // - hop_count for length(path) → t.hop_count rewriting
        // - NULL hop_count when target not reached → ifNull() pattern works
        let result_select = if let Some(ref target) = target_id {
            // BFS target branch — pair-rewrite the conditional count/min
            // through the FunctionMapper so both dialects emit valid SQL:
            //   CH:    countIf(cond) > 0, minIf(val, cond)
            //   Spark: count_if(cond) > 0, min(CASE WHEN cond THEN val END)
            // The outer CASE … ELSE NULL is redundant under standard `min`
            // semantics (empty match → NULL anyway) but kept for clarity
            // and to leave the CH emission byte-identical to its prior form.
            let cond = format!("node_id = {target}");
            let count_if = fmap.count_if();
            let conditional_min = fmap.min_if(&format!("{cast_u16}(hop)"), &cond);
            format!(
                "{name} AS (\n    SELECT\n        \
                 {start_id} AS start_id,\n        \
                 {target} AS end_id,\n        \
                 CASE WHEN {count_if}({cond}) > 0\n            \
                 THEN {conditional_min}\n            \
                 ELSE NULL END AS hop_count,\n        \
                 {empty_str_arr} AS path_relationships,\n        \
                 {empty_i64_arr} AS path_nodes\n    \
                 FROM {bfs_cte}\n)",
                name = self.cte_name,
                start_id = start_id,
                target = target,
                bfs_cte = bfs_cte_name,
            )
        } else {
            // No target filter — enumerate all reachable nodes with their min hop
            format!(
                "{name} AS (\n    SELECT\n        \
                 {start_id} AS start_id,\n        \
                 node_id AS end_id,\n        \
                 min({cast_u16}(hop)) AS hop_count,\n        \
                 {empty_str_arr} AS path_relationships,\n        \
                 {empty_i64_arr} AS path_nodes\n    \
                 FROM {bfs_cte}\n    \
                 WHERE node_id != {start_id}\n    \
                 GROUP BY node_id\n)",
                name = self.cte_name,
                start_id = start_id,
                bfs_cte = bfs_cte_name,
            )
        };

        format!("{},\n{}", bfs_cte_sql, result_select)
    }

    /// Generate two-phase BFS + backward reconstruction SQL for weighted shortestPath.
    ///
    /// Phase 1 (BFS): Lightweight frontier expansion using the bidirectional weight CTE.
    ///   Tracks only (node_id, hop) — no path arrays. ~67K rows max for LDBC KNOWS graph.
    ///
    /// Phase 2 (Reconstruction): Walk backward from target to source using BFS hop levels
    ///   as constraints. Builds path_nodes and accumulates total_weight. ~6 rows for typical
    ///   shortest paths.
    ///
    /// Result wrapper: Picks the cheapest reconstructed path (ORDER BY total_weight LIMIT 1).
    fn generate_weighted_bfs_reconstruction_sql(&self) -> String {
        let wc = self
            .weight_cte
            .as_ref()
            .expect("weight_cte required for weighted BFS");
        let max_hops = self.spec.max_hops.unwrap_or_else(get_default_max_hops);

        let source = &wc.source_column;
        let target_col = &wc.target_column;
        let weight = &wc.weight_column;
        let weight_cte = &wc.cte_name;

        // Extract start and target IDs from filters
        let start_id = self
            .start_node_filters
            .as_ref()
            .and_then(|f| {
                Self::extract_id_from_filter(f, &self.start_node_alias, &self.start_node_id_column)
            })
            .unwrap_or_else(|| format!("{}.{}", self.start_node_alias, self.start_node_id_column));

        let target_id = self
            .end_node_filters
            .as_ref()
            .and_then(|f| {
                Self::extract_id_from_filter(f, &self.end_node_alias, &self.end_node_id_column)
            })
            .unwrap_or_else(|| format!("{}.{}", self.end_node_alias, self.end_node_id_column));

        let bfs_cte = format!("{}_bfs", self.cte_name);
        let recon_cte = format!("{}_recon", self.cte_name);
        let fmap = current_function_mapper();
        let ac = fmap.array_concat();
        let empty_str_arr = fmap.empty_string_array_cast();
        let cast_i64 = fmap.cast_int64();
        let cast_u16 = fmap.cast_uint16();
        let cast_f64 = fmap.cast_float64();

        // CTE 1: BFS — lightweight frontier with global dedup
        let bfs_sql = format!(
            "{bfs_cte} AS (\n    \
             SELECT {cast_i64}({start_id}) AS node_id, {cast_u16}(0) AS hop\n    \
             UNION ALL\n    \
             SELECT DISTINCT ew.{target_col} AS node_id, b.hop + 1 AS hop\n    \
             FROM {bfs_cte} b\n    \
             JOIN {weight_cte} ew ON ew.{source} = b.node_id\n    \
             WHERE b.hop < {max_hops}\n      \
             AND ew.{target_col} NOT IN (SELECT node_id FROM {bfs_cte})\n      \
             AND b.node_id != {cast_i64}({target_id})\n)"
        );

        // CTE 2: Backward reconstruction — walk from target to source
        // IMPORTANT: Use FROM+WHERE to read BFS hop level instead of scalar subqueries.
        // Scalar subqueries referencing recursive CTEs inside other CTEs cause ClickHouse
        // to hang (CTE inlining re-evaluates the entire recursion chain).
        let target_nodes_arr = arr(target_id.as_str());
        let source_scalar_arr = arr(&format!("ew.{source}"));
        let path_nodes_cast = fmap.int64_array_cast(&target_nodes_arr);
        let recon_sql = format!(
            "{recon_cte} AS (\n    \
             SELECT {cast_i64}({target_id}) AS node_id,\n           \
             bfs_target.hop AS remaining,\n           \
             {path_nodes_cast} AS path_nodes,\n           \
             {cast_f64}(0) AS total_weight\n    \
             FROM {bfs_cte} AS bfs_target\n    \
             WHERE bfs_target.node_id = {cast_i64}({target_id})\n    \
             UNION ALL\n    \
             SELECT ew.{source} AS node_id, pr.remaining - 1 AS remaining,\n           \
             {ac}({source_scalar_arr}, pr.path_nodes) AS path_nodes,\n           \
             pr.total_weight + ew.{weight} AS total_weight\n    \
             FROM {recon_cte} pr\n    \
             JOIN {weight_cte} ew ON ew.{target_col} = pr.node_id\n    \
             JOIN {bfs_cte} b ON b.node_id = ew.{source} AND b.hop = pr.remaining - 1\n    \
             WHERE pr.remaining > 0\n      \
             AND ew.{source} NOT IN (SELECT node_id FROM {recon_cte})\n)"
        );

        // CTE 3: Result wrapper — select cheapest path
        // hop_count derived from path_nodes length (avoids scalar subquery on BFS CTE)
        let result_sql = format!(
            "{name} AS (\n    \
             SELECT {start_id} AS start_id, {target_id} AS end_id,\n           \
             {cast_u16}(length(path_nodes) - 1) AS hop_count,\n           \
             total_weight, path_nodes, {empty_str_arr} AS path_relationships\n    \
             FROM {recon_cte} WHERE remaining = 0\n    \
             ORDER BY total_weight ASC LIMIT 1\n)",
            name = self.cte_name,
        );

        format!("{},\n{},\n{}", bfs_sql, recon_sql, result_sql)
    }

    /// Generate the actual recursive SQL string
    fn generate_recursive_sql(&self) -> String {
        // Lightweight BFS mode for shortestPath + length(path)-only queries
        if self.use_bfs_mode {
            return self.generate_bfs_shortest_path_sql();
        }

        // Two-phase BFS + reconstruction for weighted shortestPath with known target
        if self.weight_cte.is_some()
            && self.shortest_path_mode.is_some()
            && self.end_node_filters.is_some()
        {
            return self.generate_weighted_bfs_reconstruction_sql();
        }

        // For heterogeneous polymorphic paths, use special two-CTE structure
        if self.is_heterogeneous_polymorphic_path() {
            return self.generate_heterogeneous_polymorphic_sql();
        }

        let min_hops = self.spec.effective_min_hops();
        let max_hops = self.spec.max_hops;

        // Determine if we need an _inner CTE wrapper
        // This is needed when we have:
        // 1. Shortest path mode (which requires post-processing)
        // 2. min_hops > 1 (base case generates hop 1, but we need to filter)
        // 3. Denormalized VLP with end_node_filters (can't filter in base case, must wrap)
        // 4. #607: standard multi-hop-capable VLP with an end_node filter — the
        //    filter is applied on the true endpoint in the wrapper, not on the
        //    intermediate `end_node` of each base/recursive hop.
        let denorm_needs_end_filter_wrapper = self.is_denormalized
            && self.end_node_filters.is_some()
            && self.shortest_path_mode.is_none();
        let needs_inner_cte = self.shortest_path_mode.is_some()
            || min_hops > 1
            || denorm_needs_end_filter_wrapper
            || self.end_filter_applied_in_wrapper();
        let recursive_cte_name = if needs_inner_cte {
            format!("{}_inner", self.cte_name)
        } else {
            self.cte_name.clone()
        };

        // Generate the core recursive query body (without CTE name wrapper)
        let mut query_body = String::new();

        // Special case: For shortest path self-loops (a to a), only zero-hop is needed
        let is_shortest_self_loop = self.shortest_path_mode.is_some()
            && min_hops == 0
            && self.start_cypher_alias == self.end_cypher_alias;

        // Base case: ONE base case, either zero-hop or 1-hop depending on min_hops
        if min_hops == 0 {
            // Zero-hop base case for patterns like *0.., *0..5
            query_body.push_str(&self.generate_zero_hop_base_case());
        } else {
            // 1-hop base case for patterns like *, *1.., *2..
            // (recursion will extend to 2+ hops)
            query_body.push_str(&self.generate_base_case(1));
        }

        // Recursive case: Add if we need more than just the base case.
        // Skip for shortest path self-loops (zero-hop is always the answer).
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
        // yielding an empty CTE (every optional endpoint NULL). Exact-bound VLP
        // only started reaching this recursive generator once OPTIONAL exact
        // VLP was routed here (previously it always used the flat self-join), so
        // comparing against the BASE hop count (1) instead of min_hops fixes it.
        //
        // Scope guard: this widening applies ONLY to the non-shortestPath VLP
        // family. shortestPath has its own exact-depth handling (BFS / ROW_NUMBER
        // over the inner CTE) and a separate pre-existing exact-depth defect
        // (the golden for `shortestPath(*3)` emits base-only, no recursion —
        // tracked separately). Keep the original `max > min_hops` test there so
        // this OPTIONAL-VLP fix does not silently alter shortestPath output.
        let recursion_threshold = if self.shortest_path_mode.is_some() {
            min_hops
        } else {
            // Non-shortestPath: recurse to reach any hop beyond the base case.
            if min_hops == 0 {
                0
            } else {
                1
            }
        };
        let needs_recursion = !is_shortest_self_loop
            && max_hops != Some(0)
            && (max_hops.is_none() || max_hops.unwrap() > recursion_threshold);

        if needs_recursion {
            // Note: UNION DISTINCT is not supported in ClickHouse recursive CTEs.
            // Using UNION ALL means duplicate edges in the data can cause exponential
            // row explosion. The path_edges tracking with `NOT has()` prevents cycles
            // but not duplicate edges between the same nodes.
            //
            // Mitigation: Ensure edge tables have unique (from_id, to_id) pairs,
            // or the application should enforce this constraint before loading data.
            query_body.push_str("\n    UNION ALL\n");

            let default_depth = max_hops.unwrap_or_else(|| {
                if min_hops == 0 {
                    3 // Lower limit for zero-hop base queries
                } else {
                    get_default_max_hops()
                }
            });

            query_body.push_str(
                &self.generate_recursive_case_with_cte_name(default_depth, &recursive_cte_name),
            );
        }

        // Build CTE structure based on shortest path mode and filters
        // For shortest path queries, end filters are now applied during path generation
        // in the inner CTE, so we don't need separate filtering steps
        // For weighted mode, ORDER BY total_weight instead of hop_count
        let order_by_column = if self.weight_cte.is_some() {
            "total_weight"
        } else {
            "hop_count"
        };
        let sql = match (&self.shortest_path_mode, &self.end_node_filters) {
            (Some(ShortestPathMode::Shortest), Some(end_filters)) => {
                // Rewrite end filter for use in intermediate CTE
                // Replace "end_node.property" with "end_property" (column names in CTE)
                let rewritten_filter = self.rewrite_end_filter_for_cte(end_filters);

                // Add min_hops and max_hops constraints if needed
                let min_hops = self.spec.effective_min_hops();
                let max_hops = self.spec.max_hops;

                let mut filter_with_bounds = rewritten_filter.clone();
                if min_hops > 1 {
                    filter_with_bounds =
                        format!("({}) AND hop_count >= {}", filter_with_bounds, min_hops);
                }
                if let Some(max) = max_hops {
                    filter_with_bounds =
                        format!("({}) AND hop_count <= {}", filter_with_bounds, max);
                }

                // CORRECT ORDER: Filter to target FIRST (with min/max_hops), then find shortest path from EACH start node
                // This ensures we get the shortest path TO THE TARGET within hop bounds from each source
                format!(
                    "{name}_inner AS (\n{body}\n),\n{name}_to_target AS (\n    SELECT * FROM {name}_inner WHERE {filter}\n),\n{name} AS (\n    SELECT * FROM (\n        SELECT *, ROW_NUMBER() OVER (PARTITION BY start_id ORDER BY {order_col} ASC) as rn\n        FROM {name}_to_target\n    ) WHERE rn = 1\n)",
                    name = self.cte_name,
                    body = query_body,
                    filter = filter_with_bounds,
                    order_col = order_by_column,
                )
            }
            (Some(ShortestPathMode::AllShortest), Some(end_filters)) => {
                // Rewrite end filter for use in intermediate CTE
                let rewritten_filter = self.rewrite_end_filter_for_cte(end_filters);

                // Add min_hops and max_hops constraints if needed
                let min_hops = self.spec.effective_min_hops();
                let max_hops = self.spec.max_hops;

                let mut filter_with_bounds = rewritten_filter.clone();
                if min_hops > 1 {
                    filter_with_bounds =
                        format!("({}) AND hop_count >= {}", filter_with_bounds, min_hops);
                }
                if let Some(max) = max_hops {
                    filter_with_bounds =
                        format!("({}) AND hop_count <= {}", filter_with_bounds, max);
                }

                // CORRECT ORDER: Filter to target FIRST (with min/max_hops), then find shortest path from EACH start node
                format!(
                    "{name}_inner AS (\n{body}\n),\n{name}_to_target AS (\n    SELECT * FROM {name}_inner WHERE {filter}\n),\n{name} AS (\n    SELECT * FROM (\n        SELECT *, ROW_NUMBER() OVER (PARTITION BY start_id ORDER BY {order_col} ASC) as rn\n        FROM {name}_to_target\n    ) WHERE rn = 1\n)",
                    name = self.cte_name,
                    body = query_body,
                    filter = filter_with_bounds,
                    order_col = order_by_column,
                )
            }
            (Some(ShortestPathMode::Shortest), None) => {
                // 2-tier: inner → select shortest path to EACH end node (no target filter)
                // Use window function to get the shortest path to each distinct end_id
                format!(
                    "{name}_inner AS (\n{body}\n),\n{name} AS (\n    SELECT * FROM (\n        SELECT *, ROW_NUMBER() OVER (PARTITION BY end_id ORDER BY {order_col} ASC) as rn\n        FROM {name}_inner\n    ) WHERE rn = 1\n)",
                    name = self.cte_name,
                    body = query_body,
                    order_col = order_by_column,
                )
            }
            (Some(ShortestPathMode::AllShortest), None) => {
                // 2-tier: inner → select all shortest (no target filter)
                format!(
                    "{name}_inner AS (\n{body}\n),\n{name} AS (\n    SELECT * FROM {name}_inner WHERE {order_col} = (SELECT MIN({order_col}) FROM {name}_inner)\n)",
                    name = self.cte_name,
                    body = query_body,
                    order_col = order_by_column,
                )
            }
            (None, Some(end_filters)) => {
                // For denormalized VLP, end filters are NOT applied in base/recursive cases
                // (to allow multi-hop paths). They must be applied in a wrapper CTE.
                //
                // For standard VLP, end filters ARE applied in base/recursive cases,
                // so we don't need to filter again here.
                if self.is_denormalized {
                    // Denormalized: Apply end filter in wrapper CTE
                    // The end_filters string uses "end_node.X" which maps to the CTE's output columns
                    // But for denormalized, CTE columns use physical names (e.g., "Dest" not "end_node.code")
                    // The filter was already rewritten during categorization, so it should use the rel alias
                    // which maps to the CTE alias in the wrapper
                    // 🔧 FIX: Rewrite end_node filter for denormalized VLP with prefixed columns
                    // Replace "end_node.property" with "vlp_inner.end_property" (prefixed columns)
                    let rewritten_filter =
                        end_filters.replace("end_node.", &format!("{}_inner.end_", self.cte_name));
                    // Also handle rel alias replacement (e.g., "rel.Dest" -> "vlp_inner.end_Dest")
                    // For denormalized schemas, the end node ID is in the "end_" prefixed column
                    let rewritten_filter = rewritten_filter.replace(
                        &format!("{}.", self.relationship_alias),
                        &format!("{}_inner.end_", self.cte_name),
                    );

                    let min_hops_filter = if min_hops > 1 {
                        format!(" AND hop_count >= {}", min_hops)
                    } else {
                        String::new()
                    };

                    format!(
                        "{}_inner AS (\n{}\n),\n{} AS (\n    SELECT * FROM {}_inner WHERE ({}){}\n)",
                        self.cte_name, query_body, self.cte_name, self.cte_name, rewritten_filter, min_hops_filter
                    )
                } else {
                    // Standard VLP.
                    // #607: when the VLP is multi-hop-capable, the end filter was
                    // NOT applied in the base/recursive cases (it would prune
                    // intermediate nodes); apply it here on the true endpoint,
                    // rewritten to the CTE's `end_id`/`end_*` output columns, and
                    // AND-combined with the min-hop bound.
                    if self.end_filter_applied_in_wrapper() {
                        let rewritten_filter = self.rewrite_end_filter_for_cte(end_filters);
                        let min_hops_clause = if min_hops > 1 {
                            format!(" AND hop_count >= {}", min_hops)
                        } else {
                            String::new()
                        };
                        format!(
                            "{}_inner AS (\n{}\n),\n{} AS (\n    SELECT * FROM {}_inner WHERE ({}){}\n)",
                            self.cte_name, query_body, self.cte_name, self.cte_name, rewritten_filter, min_hops_clause
                        )
                    } else if min_hops > 1 {
                        // No wrapper carries the end filter (pure single-hop): the
                        // filter is already in the base case. Only bound min-hops.
                        format!(
                            "{}_inner AS (\n{}\n),\n{} AS (\n    SELECT * FROM {}_inner WHERE hop_count >= {}\n)",
                            self.cte_name, query_body, self.cte_name, self.cte_name, min_hops
                        )
                    } else {
                        format!("{} AS (\n{}\n)", self.cte_name, query_body)
                    }
                }
            }
            (None, None) => {
                // Apply min_hops filtering if needed
                // Base case starts at hop 1 to allow recursion, but we need to filter
                // results to respect min_hops (e.g., *2.. should only return hop_count >= 2)
                // max_hops is already enforced by recursion termination condition
                if min_hops > 1 {
                    format!(
                        "{}_inner AS (\n{}\n),\n{} AS (\n    SELECT * FROM {}_inner WHERE hop_count >= {}\n)",
                        self.cte_name, query_body, self.cte_name, self.cte_name, min_hops
                    )
                } else {
                    // No filtering needed (min_hops <= 1)
                    format!("{} AS (\n{}\n)", self.cte_name, query_body)
                }
            }
        };

        // #617: the doubled-edge walk needs its sibling edge CTE defined before
        // the recursive CTE. Emit only when the body actually references it —
        // a *0..0 pattern (zero-hop seed only, no recursive arm) never joins
        // the edge relation.
        if self.uses_doubled_edges() {
            let edges_name = self.doubled_edges_cte_name();
            if query_body.contains(&edges_name) {
                return format!("{},\n{}", self.generate_doubled_edges_cte(), sql);
            }
        }

        sql
    }

    /// Generate SQL for heterogeneous polymorphic paths (e.g., Group→*→User)
    ///
    /// Uses a two-phase CTE structure:
    /// 1. `reachable_intermediates`: Recursively finds all intermediate nodes (Groups) reachable from start
    /// 2. Main CTE: Joins ALL reachable intermediates to end nodes (Users) via the relationship
    ///
    /// Key insight: At each intermediate node, we can either:
    /// - Continue recursion (target is another intermediate/Group)
    /// - Collect terminal result (target is end node/User)
    ///
    /// The final result includes Users reachable from ANY intermediate Group at ANY depth.
    fn generate_heterogeneous_polymorphic_sql(&self) -> String {
        let intermediate_table = self
            .intermediate_node_table
            .as_ref()
            .expect("intermediate_node_table must be set");
        let intermediate_id_col = self
            .intermediate_node_id_column
            .as_ref()
            .expect("intermediate_node_id_column must be set");
        let intermediate_label = self
            .intermediate_node_label
            .as_ref()
            .expect("intermediate_node_label must be set");

        let min_hops = self.spec.effective_min_hops();
        let max_hops = self.spec.max_hops.unwrap_or(DEFAULT_MAX_HOPS);
        let fmap = current_function_mapper();
        let empty_str_arr = fmap.empty_string_array_cast();
        let empty_i64_arr = fmap.empty_int64_array_cast();

        crate::debug_print!("    🔸 Generating heterogeneous polymorphic SQL (two-phase):");
        crate::debug_print!(
            "      - start_table: {}, intermediate_table: {}, end_table: {}",
            self.start_node_table,
            intermediate_table,
            self.end_node_table
        );
        crate::debug_print!(
            "      - intermediate_label: {}, to_node_label: {:?}",
            intermediate_label,
            self.to_node_label
        );
        crate::debug_print!("      - min_hops: {}, max_hops: {}", min_hops, max_hops);

        let reachable_cte_name = format!("{}_reachable", self.cte_name);

        // Build qualified table names
        let start_table_qualified = self.format_table_name(&self.start_node_table);
        let rel_table_qualified = self.format_table_name(&self.relationship_table);
        let intermediate_table_qualified = self.format_table_name(intermediate_table);
        let end_table_qualified = self.format_table_name(&self.end_node_table);

        // Build start node filter if exists
        // Replace "start_node." with the actual table name for the base case
        let start_filter = if let Some(ref filter) = self.start_node_filters {
            let rewritten = filter.replace("start_node.", &format!("{}.", start_table_qualified));
            format!("\n    WHERE {}", rewritten)
        } else {
            String::new()
        };

        // Build polymorphic filter for intermediate hops (member_type = 'Group')
        let intermediate_poly_filter = if let Some(ref to_label_col) = self.to_label_column {
            format!(
                "{}.{} = '{}'",
                self.relationship_alias, to_label_col, intermediate_label
            )
        } else {
            "1=1".to_string()
        };

        // Build polymorphic filter for final hop to end nodes (member_type = 'User')
        let end_poly_filter = if let Some(ref to_label_col) = self.to_label_column {
            if let Some(ref to_label) = self.to_node_label {
                format!(
                    "{}.{} = '{}'",
                    self.relationship_alias, to_label_col, to_label
                )
            } else {
                "1=1".to_string()
            }
        } else {
            "1=1".to_string()
        };

        // ============================================================
        // CTE 1: Find all reachable intermediate nodes (groups)
        // This includes the start node at depth 0, then recurses through
        // intermediate->intermediate relationships (Group->Group)
        // ============================================================
        let reachable_cte = format!(
            "{reachable_cte} AS (\n\
            -- Base case: Start nodes at depth 0\n\
            SELECT \n\
                {start_table}.{start_id} as node_id,\n\
                0 as depth\n\
            FROM {start_table}{start_filter}\n\
            \n\
            UNION ALL\n\
            \n\
            -- Recursive case: Traverse to child intermediates (Group->Group)\n\
            SELECT \n\
                {intermediate_table}.{intermediate_id} as node_id,\n\
                r.depth + 1 as depth\n\
            FROM {reachable_cte} r\n\
            JOIN {rel_table} {rel} ON r.node_id = {rel}.{from_col}\n\
            JOIN {intermediate_table} ON {rel}.{to_col} = {intermediate_table}.{intermediate_id}\n\
            WHERE r.depth < {max_hops}\n\
                AND {intermediate_poly_filter}\n\
        )",
            reachable_cte = reachable_cte_name,
            start_table = start_table_qualified,
            start_id = self.start_node_id_column,
            start_filter = start_filter,
            intermediate_table = intermediate_table_qualified,
            intermediate_id = intermediate_id_col,
            rel_table = rel_table_qualified,
            rel = self.relationship_alias,
            from_col = self.relationship_from_column,
            to_col = self.relationship_to_column,
            max_hops = max_hops,
            intermediate_poly_filter = intermediate_poly_filter,
        );

        // ============================================================
        // CTE 2: Collect end nodes (Users) from ALL reachable intermediates
        // Users at depth+1 from each reachable Group are included
        // This is the main CTE that produces the final result
        // ============================================================

        // Build property selections for end nodes
        let mut prop_selects = Vec::new();
        for prop in &self.properties {
            if prop.cypher_alias == self.end_cypher_alias {
                prop_selects.push(format!(
                    "{}.{} as end_{}",
                    self.end_node_alias, prop.column_name, prop.alias
                ));
            }
        }
        let props_clause = if prop_selects.is_empty() {
            String::new()
        } else {
            format!(",\n        {}", prop_selects.join(",\n        "))
        };

        // Build end node filter if exists (e.g., user property filters)
        let end_filter = if let Some(ref filter) = self.end_node_filters {
            format!("\n    AND {}", filter)
        } else {
            String::new()
        };

        // Apply min_hops and max_hops filters
        // The User is at depth+1 from the Group that contains them
        let hop_filter = format!(
            "\n    AND r.depth + 1 >= {} AND r.depth + 1 <= {}",
            min_hops, max_hops
        );

        let main_cte = format!(
            "{main_cte} AS (\n\
            -- Collect end nodes (Users) from all reachable intermediates (Groups)\n\
            SELECT \n\
                r.node_id as start_id,\n\
                {end_table}.{end_id} as end_id,\n\
                r.depth + 1 as hop_count,\n\
                {empty_str_arr} as path_relationships,\n\
                {empty_i64_arr} as path_nodes{props_clause}\n\
            FROM {reachable_cte} r\n\
            JOIN {rel_table} {rel} ON r.node_id = {rel}.{from_col}\n\
            JOIN {end_table} {end} ON {rel}.{to_col} = {end}.{end_id}\n\
            WHERE {end_poly_filter}{end_filter}{hop_filter}\n\
        )",
            main_cte = self.cte_name,
            reachable_cte = reachable_cte_name,
            end_table = end_table_qualified,
            end_id = self.end_node_id_column,
            props_clause = props_clause,
            rel_table = rel_table_qualified,
            rel = self.relationship_alias,
            from_col = self.relationship_from_column,
            to_col = self.relationship_to_column,
            end = self.end_node_alias,
            end_poly_filter = end_poly_filter,
            end_filter = end_filter,
            hop_filter = hop_filter,
        );

        format!("{},\n{}", reachable_cte, main_cte)
    }

    /// Generate base case for zero hops (self-loop)
    /// Used with shortest path functions when pattern is *0..
    fn generate_zero_hop_base_case(&self) -> String {
        // For zero-hop, start = end (the node hasn't moved).
        // When start and end tables differ (e.g., Comment vs Post in REPLY_OF*0..),
        // we need to handle properties carefully: end properties may not exist on
        // the start table (or vice versa). We collect column names available on
        // the start table and use NULL/empty for missing end columns.
        let empty_str_arr = current_function_mapper().empty_string_array_cast();
        let cross_type = self.start_node_table != self.end_node_table;
        let start_table_columns: std::collections::HashSet<String> = if cross_type {
            let mut cols: std::collections::HashSet<String> = self
                .properties
                .iter()
                .filter(|p| p.cypher_alias == self.start_cypher_alias)
                .map(|p| p.column_name.clone())
                .collect();
            cols.insert(self.start_node_id_column.clone());
            log::info!(
                "🔧 VLP zero-hop cross-type: start_table={}, end_table={}, start_columns={:?}",
                self.start_node_table,
                self.end_node_table,
                cols
            );
            cols
        } else {
            std::collections::HashSet::new() // Not needed when tables are same
        };

        let mut select_items = vec![
            format!(
                "{}.{} as start_id",
                self.start_node_alias, self.start_node_id_column
            ),
            format!(
                "{}.{} as end_id",
                self.start_node_alias,
                self.start_node_id_column // Same node for self-loop
            ),
            "0 as hop_count".to_string(), // Zero hops
            format!("{empty_str_arr} as path_relationships"), // Minimal placeholder when path data not needed
            // Add path_nodes for UNWIND nodes(p) support - for zero hop, just the start node
            format!(
                "{} as path_nodes",
                arr(&format!(
                    "{}.{}",
                    self.start_node_alias, self.start_node_id_column
                ))
            ),
        ];

        // #628: a CLOSED `*0..N` walk enforces EDGE-uniqueness (so real cycles
        // survive — see `uses_edge_uniqueness`). The zero-hop base has no edge,
        // so it seeds an EMPTY `path_edges` array. A bare `[]` is ClickHouse's
        // `Array(Nothing)`, the bottom type, which unifies with the recursive
        // arm's concrete `Array(Tuple(...))` on `arrayConcat` (a CAST to a
        // guessed element type would instead risk a NO_COMMON_TYPE error against
        // the real column types). The recursive arm's `NOT has(path_edges, …)`
        // then dedupes edges from hop 1 onward. Gated identically to the base /
        // recursive arms via `uses_edge_uniqueness()`, so a pattern that stays
        // node-unique (open `*0..N`, shortestPath) is byte-unchanged.
        if self.uses_edge_uniqueness() {
            select_items.push(format!("{} as path_edges", arr("")));
        }

        // Add properties for start node (which is also the end node)
        for prop in &self.properties {
            if prop.cypher_alias == self.start_cypher_alias {
                // Skip ID column when it's the actual id property (already added as start_id)
                // But keep it if it's a different property that happens to be the ID column
                // (e.g., "node_id" as a separate property in some schemas)
                if prop.column_name == self.start_node_id_column && prop.alias == "id" {
                    continue;
                }

                select_items.push(format!(
                    "{}.{} as start_{}",
                    self.start_node_alias, prop.column_name, prop.alias
                ));
            }
            // For zero-hop, end properties are same as start properties
            if prop.cypher_alias == self.end_cypher_alias {
                // Skip ID column when it's the actual id property (already added as end_id)
                if prop.column_name == self.end_node_id_column && prop.alias == "id" {
                    continue;
                }

                // When start and end tables differ, end properties may not exist
                // on the start table. Use the column if available, otherwise NULL.
                if cross_type && !start_table_columns.contains(&prop.column_name) {
                    log::info!(
                        "🔧 VLP zero-hop: end property '{}' (column '{}') not on start table, using NULL",
                        prop.alias, prop.column_name
                    );
                    select_items.push(format!("'' as end_{}", prop.alias));
                } else {
                    select_items.push(format!(
                        "{}.{} as end_{}",
                        self.start_node_alias, prop.column_name, prop.alias
                    ));
                }
            }
        }

        let select_clause = select_items.join(",\n        ");

        // Build the zero-hop query - just select from start table
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

        // For zero-hop self-loops, the end node is the same as start node,
        // so end_node_filters would also apply here — BUT these rows are also
        // the RECURSION SEED. Filtering the seed by the endpoint predicate
        // silently drops every longer path that starts from a node failing it
        // (#610: *0..2 WHERE b.id > 2 must still seed paths from node 1).
        // When the outer wrapper carries the end filter (#607), it filters
        // hop-0 rows too (end_id/end_* equal the start node at hop 0), so the
        // base must leave the seed unfiltered. Only apply the filter here when
        // no wrapper exists to carry it (e.g. *0..0, shortestPath, denorm —
        // whose placement is unchanged).
        if !self.end_filter_applied_in_wrapper() {
            if let Some(ref filters) = self.end_node_filters {
                // Rewrite end_node references to start_node references
                let rewritten = filters.replace(
                    &format!("{}.", self.end_node_alias),
                    &format!("{}.", self.start_node_alias),
                );
                where_conditions.push(rewritten);
            }
        }

        if !where_conditions.is_empty() {
            query.push_str("\n    WHERE ");
            query.push_str(&where_conditions.join(" AND "));
        }

        query
    }

    fn generate_base_case(&self, hop_count: u32) -> String {
        // Weighted shortest path: use pre-computed weight CTE instead of direct edge table
        if let Some(ref wc) = self.weight_cte {
            return self.generate_weighted_base_case(wc);
        }

        // Determine which pattern to use based on denormalization flags
        // (Fully-denormalized VLP never reaches this struct — cte_manager
        // intercepts it via DenormalizedCteStrategy before constructing the
        // generator; see cte_manager/mod.rs:3261. So there is no `is_denormalized`
        // arm here — only the mixed / FK-edge / standard cases remain.)
        // Mixed: one node virtual, one standard → use mixed generator
        // FK-edge: edge table = node table with FK column → 2-way join (no separate rel)
        // Full standard: both nodes standard → use standard generator

        // Check for mixed patterns (one side denormalized)
        if self.start_is_denormalized || self.end_is_denormalized {
            return self.generate_mixed_base_case(hop_count);
        }

        // FK-edge pattern: edge table = node table with FK column
        // Use direct 2-way join: start_node.fk_col = end_node.id_col
        if self.is_fk_edge {
            return self.generate_fk_edge_base_case(hop_count);
        }

        // Standard case: both nodes have their own tables
        if hop_count == 1 {
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
            if self.needs_path_data() {
                select_items.push(self.generate_relationship_type_for_hop(1));
            } else {
                select_items.push(format!("{empty_str_arr} as path_relationships"));
            }
            select_items.push(path_nodes_selection);

            // #598 (part 2): seed path_edges with this hop's edge identity so the
            // recursive step can enforce relationship-uniqueness (Cypher default).
            // A non-empty 1-element array literal lets ClickHouse infer
            // Array(Tuple(...)) directly — no CAST needed. path_nodes is retained
            // above for nodes(p); path_edges drives the cycle check. Kept for
            // shortestPath/non-standard strategies via uses_edge_uniqueness().
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
            let start_id_cols: Vec<&str> = self
                .start_node_id_column
                .split(',')
                .map(|s| s.trim())
                .collect();
            let end_id_cols: Vec<&str> = self
                .end_node_id_column
                .split(',')
                .map(|s| s.trim())
                .collect();

            // Add properties for start and end nodes
            // CRITICAL: Use separate if statements (not else-if) for self-loops
            // When start_cypher_alias == end_cypher_alias, both conditions are true
            // Safety check: Skip ID column based on DB column name (schema-independent)
            for prop in &self.properties {
                if prop.cypher_alias == self.start_cypher_alias
                    && !start_id_cols.contains(&prop.column_name.as_str())
                {
                    // Property belongs to start node (and is not the ID column)
                    select_items.push(format!(
                        "{}.{} as start_{}",
                        self.start_node_alias, prop.column_name, prop.alias
                    ));
                }
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

            // Add WHERE clause with start and end node filters
            // For shortest path queries, only include start filters in base case
            // End filters are applied in the _to_target wrapper CTE
            let mut where_conditions = Vec::new();

            // Add polymorphic edge filter if this is a polymorphic edge table
            if let Some(poly_filter) = self.generate_polymorphic_edge_filter() {
                where_conditions.push(poly_filter);
            }

            // Add edge constraints if defined in schema (base case uses default aliases)
            if let Some(constraint_filter) = self.generate_edge_constraint_filter(None, None) {
                where_conditions.push(constraint_filter);
            }

            if let Some(ref filters) = self.start_node_filters {
                where_conditions.push(filters.clone());
            }
            // #607: apply the end-node filter in the base case ONLY for a pure
            // single-hop standard VLP (base IS the endpoint). See
            // `end_filter_in_base_recursive_case` — it preserves the original
            // shortestPath exclusion and adds the multi-hop wrapper exclusion.
            if self.end_filter_in_base_recursive_case() {
                if let Some(ref filters) = self.end_node_filters {
                    where_conditions.push(filters.clone());
                }
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
        } else {
            // Multi-hop base case (for min_hops > 1)
            self.generate_multi_hop_base_case(hop_count)
        }
    }

    /// Generate multi-hop base case (more complex, chains multiple relationships)
    fn generate_multi_hop_base_case(&self, hop_count: u32) -> String {
        // This is a simplified version - in practice, we'd need to handle
        // different relationship types and intermediate node types
        let empty_arr = arr("");
        format!(
            "    -- Multi-hop base case for {hop_count} hops (simplified)\n    SELECT NULL as start_id, NULL as end_id, {hop_count} as hop_count, {empty_arr} as path_relationships\n    WHERE false  -- Placeholder"
        )
    }
    /// Generate weighted base case using pre-computed edge weight CTE
    fn generate_weighted_base_case(&self, wc: &WeightCteConfig) -> String {
        let mut where_conditions = Vec::new();
        if let Some(ref filters) = self.start_node_filters {
            // Rewrite start node filter: "start_node.id = $param" → "ew.source = $param"
            let rewritten = filters
                .replace(&format!("{}.", self.start_node_alias), "ew.")
                .replace("ew.id", &format!("ew.{}", wc.source_column));
            where_conditions.push(rewritten);
        }

        let where_clause = if where_conditions.is_empty() {
            String::new()
        } else {
            format!("\n    WHERE {}", where_conditions.join(" AND "))
        };

        let empty_str_arr = current_function_mapper().empty_string_array_cast();
        let path_rel_expr = format!("{empty_str_arr} AS path_relationships");
        let path_nodes_arr = arr(&format!("ew.{}, ew.{}", wc.source_column, wc.target_column));
        format!(
            "    SELECT\n        ew.{source} AS start_id,\n        ew.{target} AS end_id,\n        1 AS hop_count,\n        ew.{weight} AS total_weight,\n        {path_nodes_arr} AS path_nodes,\n        {path_rel_expr} \n    FROM {cte} ew{where_clause}",
            source = wc.source_column,
            target = wc.target_column,
            weight = wc.weight_column,
            cte = wc.cte_name,
            where_clause = where_clause,
        )
    }

    /// Generate weighted recursive case using pre-computed edge weight CTE
    fn generate_weighted_recursive_case(
        &self,
        wc: &WeightCteConfig,
        max_hops: u32,
        cte_name: &str,
    ) -> String {
        let fmap = current_function_mapper();
        let ac = fmap.array_concat();
        let empty_str_arr = fmap.empty_string_array_cast();
        let path_rel_col = if self.needs_path_data() {
            format!("{ac}(vp.path_relationships, {empty_str_arr}) AS path_relationships")
        } else {
            format!("{empty_str_arr} AS path_relationships")
        };
        let cycle_pred = emit_cycle_check(&format!("ew.{}", wc.target_column));
        let target_scalar_arr = arr(&format!("ew.{}", wc.target_column));

        format!(
            "    SELECT\n        vp.start_id,\n        ew.{target} AS end_id,\n        vp.hop_count + 1 AS hop_count,\n        vp.total_weight + ew.{weight} AS total_weight,\n        {ac}(vp.path_nodes, {target_scalar_arr}) AS path_nodes,\n        {path_rel_col}\n    FROM {cte_name} vp\n    JOIN {weight_cte} ew ON ew.{source} = vp.end_id\n    WHERE vp.hop_count < {max_hops}\n      AND {cycle_pred}",
            target = wc.target_column,
            weight = wc.weight_column,
            source = wc.source_column,
            cte_name = cte_name,
            weight_cte = wc.cte_name,
            max_hops = max_hops,
            path_rel_col = path_rel_col,
        )
    }

    /// Generate recursive case that extends existing paths
    /// Reserved for backward compatibility when default CTE name is used
    #[allow(dead_code)]
    fn generate_recursive_case(&self, max_hops: u32) -> String {
        // Delegate to the version that accepts CTE name
        // This maintains backward compatibility
        self.generate_recursive_case_with_cte_name(max_hops, &self.cte_name)
    }

    fn generate_recursive_case_with_cte_name(&self, max_hops: u32, cte_name: &str) -> String {
        // Weighted shortest path: join weight CTE instead of edge table
        if let Some(ref wc) = self.weight_cte {
            return self.generate_weighted_recursive_case(wc, max_hops, cte_name);
        }

        // (Fully-denormalized VLP never reaches this struct — intercepted by
        // cte_manager's DenormalizedCteStrategy, see mod.rs:3261 — so there is no
        // `is_denormalized` arm here.)

        // Check for mixed patterns (one side denormalized)
        if self.start_is_denormalized || self.end_is_denormalized {
            return self.generate_mixed_recursive_case(max_hops, cte_name);
        }

        // FK-edge pattern: edge table = node table with FK column
        if self.is_fk_edge {
            return self.generate_fk_edge_recursive_case(max_hops, cte_name);
        }

        // (Heterogeneous-polymorphic paths return earlier via
        // `generate_heterogeneous_polymorphic_sql()` in `generate_recursive_sql`,
        // before this dispatcher runs — so there is no heterogeneous arm here.)

        // Standard case: both nodes have their own tables
        // Parse comma-separated column string to Identifier
        let parse_id_cols = Identifier::from_comma_separated;
        let fmap = current_function_mapper();
        let ac = fmap.array_concat();
        let empty_str_arr = fmap.empty_string_array_cast();

        // Parse end node ID identifier
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
        if self.needs_path_data() {
            select_items.push(format!(
                "{ac}(vp.path_relationships, {}) as path_relationships",
                self.get_relationship_type_array()
            ));
        } else {
            select_items.push(format!("{empty_str_arr} as path_relationships"));
        }
        select_items.push(path_nodes_selection);

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

        // For composite IDs, add individual ID component columns
        // Pass through start ID components from vp, add end ID components from joined node
        if let Identifier::Composite(cols) = &end_id_identifier {
            for col in cols.iter() {
                // Start ID components pass through from vp
                select_items.push(format!("vp.start_{} as start_{}", col, col));
                // End ID components come from newly joined node
                select_items.push(format!(
                    "{}.{} as end_{}",
                    self.end_node_alias,
                    crate::clickhouse_query_generator::quote_identifier(col),
                    col
                ));
            }
        }

        // Add properties: start properties come from CTE, end properties from new joined node
        // CRITICAL: Use separate if statements (not else-if) for self-loops
        // When start_cypher_alias == end_cypher_alias, both conditions are true
        // Safety check: Skip ID column based on DB column name (schema-independent)

        // Parse ID columns to check for composite ID components
        let start_id_cols: Vec<&str> = self
            .start_node_id_column
            .split(',')
            .map(|s| s.trim())
            .collect();
        let end_id_cols: Vec<&str> = self
            .end_node_id_column
            .split(',')
            .map(|s| s.trim())
            .collect();

        for prop in &self.properties {
            if prop.cypher_alias == self.start_cypher_alias
                && !start_id_cols.contains(&prop.column_name.as_str())
            {
                // Start node properties pass through from CTE
                select_items.push(format!("vp.start_{} as start_{}", prop.alias, prop.alias));
            }
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
        // Node-uniqueness (`NOT has(path_nodes, end_id)`) is retained for shortestPath
        // (revisiting a node can never yield a shorter path) and for the non-standard
        // strategies, gated via uses_edge_uniqueness().
        let cycle_pred = if self.uses_edge_uniqueness() {
            emit_edge_cycle_check(&self.build_edge_tuple_recursive(&self.relationship_alias))
        } else {
            emit_cycle_check(&self.build_end_node_id_expr())
        };

        let mut where_conditions = vec![format!("vp.hop_count < {}", max_hops), cycle_pred];

        // Add polymorphic edge filter if this is a polymorphic edge table.
        // #689: the recursive arm uses the *from-recursive* variant, which flips
        // the from-label discriminator to the END label for from-side-polymorphic
        // cross-type edges (Group→Group hops); it is byte-identical to the base
        // filter for every other shape.
        if let Some(poly_filter) = self.generate_polymorphic_edge_filter_from_recursive() {
            where_conditions.push(poly_filter);
        }

        // Add edge constraints if defined in schema
        // Uses vp.end_* columns for the "from" node (previous end node) and
        // end_node.* columns for the "to" node (newly joined node)
        if let Some(constraint_filter) = self.generate_edge_constraint_filter_recursive() {
            where_conditions.push(constraint_filter);
        }

        // Note: We no longer skip zero-hop rows in recursion.
        // The recursion can now start from zero-hop base case and expand from there.
        // Cycle detection (NOT has) prevents infinite loops.

        // For shortest path queries, do NOT add end_node_filters in recursive case
        // End filters are applied in the _to_target wrapper CTE after recursion completes
        // This allows the recursion to explore all paths until the target is found.
        // #607: likewise, a standard multi-hop-capable VLP applies the end filter
        // only in its outer wrapper (the recursive-case `end_node` is an
        // intermediate node on any longer path). See
        // `end_filter_in_base_recursive_case`.
        if self.end_filter_in_base_recursive_case() {
            if let Some(ref filters) = self.end_node_filters {
                where_conditions.push(filters.clone());
            }
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
        let join_on_rel = match &rel_from_identifier {
            Identifier::Single(col) => {
                // Single column: direct comparison
                format!(
                    "vp.end_id = {}.{}",
                    &self.relationship_alias,
                    crate::clickhouse_query_generator::quote_identifier(col)
                )
            }
            Identifier::Composite(cols) => {
                // Composite: convert relationship columns to pipe-joined string
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

        // PERF: Removed redundant current_node JOIN - vp.end_id already contains the ID
        // we need to join with the relationship table. The extra JOIN was causing
        // ClickHouse to hang on recursive CTEs due to inefficient query planning.
        //
        // Cross-type VLP (e.g., Message→Post via REPLY_OF): the recursive step must
        // traverse through the START type (Message) to allow intermediate hops through
        // Comments. The Post constraint is enforced by the outer query's JOIN.
        //
        // #689 EXCEPTION: a from-side-polymorphic cross-type edge (e.g. MEMBER_OF,
        // `member_type ∈ {User, Group}`, target fixed = Group) recurses through the
        // END type (Group→Group), NOT the start type. The `start_node_table` swap
        // would join `ds_users` (which lacks the `group_id` column that `join_on_end`
        // equates) → ClickHouse 500. For that shape use the END table, matching the
        // base arm. Gate is narrow (see `is_from_side_polymorphic_cross_type`) so the
        // #142 REPLY_OF branch is untouched.
        let recursive_end_table = if self.start_node_table != self.end_node_table
            && !self.is_from_side_polymorphic_cross_type()
        {
            log::info!(
                "VLP cross-type recursion: using {} instead of {} for intermediate hops",
                self.start_node_table,
                self.end_node_table
            );
            self.format_table_name(&self.start_node_table)
        } else {
            self.format_table_name(&self.end_node_table)
        };
        format!(
            "    SELECT\n        {select}\n    FROM {cte_name} vp\n    JOIN {rel_table} AS {rel} ON {join_on_rel}\n    JOIN {end_table} AS {end} ON {join_on_end}\n    WHERE {where_clause}",
            select = select_clause,
            end = self.end_node_alias,
            cte_name = cte_name, // Use the passed parameter instead of self.cte_name
            rel_table = self.rel_source(),
            rel = self.relationship_alias,
            end_table = recursive_end_table,
            join_on_rel = join_on_rel,
            join_on_end = join_on_end,
            where_clause = where_clause
        )
    }

    // ======================================================================
    // FK-EDGE PATTERN GENERATION
    // ======================================================================
    // For FK-edge patterns, the edge is a foreign key column on the node table.
    // Both nodes come from the same table, and the relationship is:
    // start_node.fk_col = end_node.id_col (e.g., child.parent_id = parent.object_id)
    // No separate relationship table exists.

    /// Generate base case for FK-edge patterns (first hop)
    /// For FK-edge: FROM node_table start JOIN node_table end ON start.fk = end.id
    fn generate_fk_edge_base_case(&self, hop_count: u32) -> String {
        if hop_count != 1 {
            // Multi-hop base case not yet supported for FK-edge
            let empty_arr = arr("");
            return format!(
                "    -- Multi-hop base case for {hop_count} hops (FK-edge - not yet supported)\n    SELECT NULL as start_id, NULL as end_id, {hop_count} as hop_count, {empty_arr} as path_relationships, {empty_arr} as path_nodes\n    WHERE false"
            );
        }

        let empty_str_arr = current_function_mapper().empty_string_array_cast();
        // #713: parse the (possibly comma-separated composite) id columns once so
        // start_id/end_id/path_nodes are emitted composite-aware, mirroring the
        // standard non-FK base case. Single-column ids degrade byte-identically.
        let start_id_identifier = Identifier::from_comma_separated(&self.start_node_id_column);
        let end_id_identifier = Identifier::from_comma_separated(&self.end_node_id_column);
        // Build property selections
        let mut select_items = vec![
            format!(
                "{} as start_id",
                emit_id_expr(&self.start_node_alias, &start_id_identifier)
            ),
            format!(
                "{} as end_id",
                emit_id_expr(&self.end_node_alias, &end_id_identifier)
            ),
            "1 as hop_count".to_string(),
        ];
        if self.needs_path_data() {
            select_items.push(self.generate_relationship_type_for_hop(1));
        } else {
            select_items.push(format!("{empty_str_arr} as path_relationships"));
        }
        select_items.push(format!(
            "{} as path_nodes",
            arr(&format!(
                "{}, {}",
                emit_id_expr(&self.start_node_alias, &start_id_identifier),
                emit_id_expr(&self.end_node_alias, &end_id_identifier),
            ))
        ));

        // #606: seed path_edges with this hop's `(from_id, to_id)` edge tuple so
        // the recursive step can enforce relationship-uniqueness (Cypher default:
        // an edge may not repeat, but a node MAY be revisited). FK-edge has no
        // dedicated edge-id column, so the ordered node-id pair is the identity.
        // Gated by uses_edge_uniqueness() (shortestPath / zero-hop stay
        // node-unique via path_nodes, keeping base/recursive column shape agreed).
        if self.uses_edge_uniqueness() {
            select_items.push(format!(
                "{} as path_edges",
                arr(&self.build_fk_edge_tuple(
                    &self.start_node_alias,
                    &self.start_node_id_column,
                    &self.end_node_alias,
                    &self.end_node_id_column,
                ))
            ));
        }

        // Add properties for start and end nodes
        for prop in &self.properties {
            if prop.cypher_alias == self.start_cypher_alias {
                select_items.push(format!(
                    "{}.{} as start_{}",
                    self.start_node_alias, prop.column_name, prop.alias
                ));
            }
            if prop.cypher_alias == self.end_cypher_alias {
                select_items.push(format!(
                    "{}.{} as end_{}",
                    self.end_node_alias, prop.column_name, prop.alias
                ));
            }
        }

        let select_clause = select_items.join(",\n        ");

        // FK-edge pattern: direct 2-way join between start and end nodes
        // start_node.fk_col = end_node.id_col (e.g., child.parent_id = parent.object_id)
        // #713: composite-aware — the FK column set and the end-node id column set
        // are zipped per-column (`a.c1 = b.c1 AND a.c2 = b.c2`); single-column
        // degrades byte-identically to `a.col = b.col`.
        // #902: for a self-referencing FK-edge the FK is whichever role column is
        // NOT the node_id, not unconditionally `relationship_from_column` (which is
        // the PK when from_id == node_id, degenerating to an identity self-join).
        let base_join_on =
            Identifier::from_comma_separated(&self.fk_hop_fk_column(&self.end_node_id_column))
                .to_sql_equality(
                    &self.start_node_alias,
                    &Identifier::from_comma_separated(&self.end_node_id_column),
                    &self.end_node_alias,
                );
        let mut query = format!(
            "    SELECT \n        {select}\n    FROM {start_table} {start}\n    JOIN {end_table} {end} ON {join_on}",
            select = select_clause,
            start = self.start_node_alias,
            start_table = self.format_table_name(&self.start_node_table),
            end = self.end_node_alias,
            join_on = base_join_on,
            end_table = self.format_table_name(&self.end_node_table)
        );

        // Add WHERE clause with start and end node filters
        let mut where_conditions = Vec::new();

        // Add edge constraints if defined in schema
        // Uses default aliases (start_node, end_node) for base case
        if let Some(constraint_filter) = self.generate_edge_constraint_filter(None, None) {
            where_conditions.push(constraint_filter);
        }

        if let Some(ref filters) = self.start_node_filters {
            where_conditions.push(filters.clone());
        }
        if self.shortest_path_mode.is_none() {
            if let Some(ref filters) = self.end_node_filters {
                where_conditions.push(filters.clone());
            }
        }

        // ✅ HOLISTIC FIX: Add relationship filters in FK-edge base case
        // Note: In FK-edge patterns, relationship properties are typically embedded in the
        // start node table (the table with the FK column), so this filter will reference
        // the start_node_alias (or a separate rel alias if one is defined)
        if let Some(ref filters) = self.relationship_filters {
            log::debug!(
                "Adding relationship filters to FK-edge base case: {}",
                filters
            );
            where_conditions.push(filters.clone());
        }

        if !where_conditions.is_empty() {
            query.push_str(&format!("\n    WHERE {}", where_conditions.join(" AND ")));
        }

        query
    }

    /// Generate recursive case for FK-edge patterns
    ///
    /// The expansion direction depends on which side is filtered:
    ///
    /// **ANCESTORS query** (filter on start/child node, e.g., WHERE child.name = 'notes.txt'):
    /// - We want to find all ancestors (parents) of notes.txt
    /// - Base: notes.txt→Work (notes.txt.parent_id = Work.object_id)
    /// - Recurse: Work→Documents (Work.parent_id = Documents.object_id)
    /// - Strategy: APPEND expansion - add new edges at the END of the path
    /// - Anchor on end_id (the parent side), find their parents
    ///
    /// **DESCENDANTS query** (filter on end/parent node, e.g., WHERE parent.name = 'root'):
    /// - We want to find all descendants (children) of root
    /// - Base: Documents→root (Documents.parent_id = root.object_id)
    /// - Recurse: Work→Documents (Work.parent_id = Documents.object_id)
    /// - Strategy: PREPEND expansion - add new edges at the START of the path
    /// - Anchor on start_id (the child side), find their children
    fn generate_fk_edge_recursive_case(&self, max_hops: u32, cte_name: &str) -> String {
        // Determine expansion direction based on which side has filters
        // If start_node_filters is set, we're finding ancestors (APPEND expansion)
        // If end_node_filters is set, we're finding descendants (PREPEND expansion)
        let expand_toward_parents = self.start_node_filters.is_some();

        if expand_toward_parents {
            self.generate_fk_edge_recursive_append(max_hops, cte_name)
        } else {
            self.generate_fk_edge_recursive_prepend(max_hops, cte_name)
        }
    }

    /// APPEND expansion: Find ancestors by following parent_id chain
    /// Used when start_node has a filter (e.g., WHERE child.name = 'notes.txt')
    fn generate_fk_edge_recursive_append(&self, max_hops: u32, cte_name: &str) -> String {
        let fmap = current_function_mapper();
        let ac = fmap.array_concat();
        let empty_str_arr = fmap.empty_string_array_cast();
        // #713: composite-aware end-node id (single-col degrades byte-identically).
        let end_id_identifier = Identifier::from_comma_separated(&self.end_node_id_column);
        // Build property selections
        // start_id stays the same (notes.txt), end_id becomes new_end
        let mut select_items = vec![
            "vp.start_id".to_string(), // start stays the same
            format!("{} as end_id", emit_id_expr("new_end", &end_id_identifier)), // new parent
            "vp.hop_count + 1 as hop_count".to_string(),
        ];
        if self.needs_path_data() {
            select_items.push(format!(
                "{ac}(vp.path_relationships, {}) as path_relationships",
                self.get_relationship_type_array()
            ));
        } else {
            select_items.push(format!("{empty_str_arr} as path_relationships"));
        }
        // APPEND the new node to path_nodes
        select_items.push(format!(
            "{ac}(vp.path_nodes, {}) as path_nodes",
            arr(&emit_id_expr("new_end", &end_id_identifier))
        ));
        // #606: APPEND this hop's edge tuple. The hop is current_node -> new_end
        // (previous end following its FK to its parent), so the edge identity is
        // (current_node.<end_id>, new_end.<end_id>). Gated by uses_edge_uniqueness()
        // to agree with the base seed and the cycle check below.
        if self.uses_edge_uniqueness() {
            select_items.push(format!(
                "{ac}(vp.path_edges, {}) as path_edges",
                arr(&self.build_fk_edge_tuple(
                    "current_node",
                    &self.end_node_id_column,
                    "new_end",
                    &self.end_node_id_column,
                ))
            ));
        }

        // Add properties: start properties from CTE, end properties from new joined node
        for prop in &self.properties {
            if prop.cypher_alias == self.start_cypher_alias {
                select_items.push(format!("vp.start_{} as start_{}", prop.alias, prop.alias));
            }
            if prop.cypher_alias == self.end_cypher_alias {
                select_items.push(format!(
                    "{}.{} as end_{}",
                    "new_end", prop.column_name, prop.alias
                ));
            }
        }

        let select_clause = select_items.join(",\n        ");

        let mut where_conditions = vec![
            format!("vp.hop_count < {}", max_hops),
            // #606: edge-unique when uses_edge_uniqueness(), else legacy node-unique.
            if self.uses_edge_uniqueness() {
                emit_edge_cycle_check(&self.build_fk_edge_tuple(
                    "current_node",
                    &self.end_node_id_column,
                    "new_end",
                    &self.end_node_id_column,
                ))
            } else {
                emit_cycle_check(&emit_id_expr("new_end", &end_id_identifier))
            },
        ];

        // Add edge constraints if defined in schema
        // FK-edge APPEND: from=current_node (previous end), to=new_end (parent)
        if let Some(constraint_filter) =
            self.generate_edge_constraint_filter(Some("current_node"), Some("new_end"))
        {
            where_conditions.push(constraint_filter);
        }

        // ✅ HOLISTIC FIX: Add relationship filters in FK-edge recursive (append) case
        // In APPEND expansion, relationship properties are on current_node (the edge/FK table)
        // Rewrite filter alias from 'start_node' to 'current_node'
        if let Some(ref filters) = self.relationship_filters {
            let rewritten_filter = filters.replace("start_node.", "current_node.");
            log::debug!(
                "Adding relationship filters to FK-edge recursive (append) case: {} -> {}",
                filters,
                rewritten_filter
            );
            where_conditions.push(rewritten_filter);
        }

        let where_clause = where_conditions.join("\n      AND ");

        // APPEND expansion: anchor on end_id, find its parent
        // current_node = previous end (e.g., Work)
        // new_end = current_node's parent (e.g., Documents)
        // #713 composite-aware JOIN conditions:
        //  - `vp.end_id = current_node.<id>`: vp.end_id is the (possibly pipe-
        //    concatenated) id materialized by emit_id_expr, so the node side must
        //    use the same concat form to compare equal.
        //  - `current_node.<fk> = new_end.<id>`: both are real columns → per-column
        //    zip. Single-column ids degrade byte-identically to the old form.
        let anchor_on = format!(
            "vp.end_id = {}",
            emit_id_expr("current_node", &end_id_identifier)
        );
        let parent_on =
            Identifier::from_comma_separated(&self.fk_hop_fk_column(&self.end_node_id_column))
                .to_sql_equality(
                    "current_node",
                    &Identifier::from_comma_separated(&self.end_node_id_column),
                    "new_end",
                );
        format!(
            "    SELECT\n        {select}\n    FROM {cte_name} vp\n    JOIN {current_table} current_node ON {anchor_on}\n    JOIN {end_table} new_end ON {parent_on}\n    WHERE {where_clause}",
            select = select_clause,
            cte_name = cte_name,
            current_table = self.format_table_name(&self.end_node_table),
            anchor_on = anchor_on,
            end_table = self.format_table_name(&self.end_node_table),
            parent_on = parent_on,
            where_clause = where_clause
        )
    }

    /// PREPEND expansion: Find descendants by finding nodes whose parent_id points to current
    /// Used when end_node has a filter (e.g., WHERE parent.name = 'root')
    fn generate_fk_edge_recursive_prepend(&self, max_hops: u32, cte_name: &str) -> String {
        let fmap = current_function_mapper();
        let ac = fmap.array_concat();
        let empty_str_arr = fmap.empty_string_array_cast();
        // #713: composite-aware start-node id (single-col degrades byte-identically).
        let start_id_identifier = Identifier::from_comma_separated(&self.start_node_id_column);
        // Build property selections
        // The NEW start_id is new_start, end_id stays the same (root)
        let mut select_items = vec![
            format!(
                "{} as start_id",
                emit_id_expr("new_start", &start_id_identifier)
            ),
            "vp.end_id".to_string(), // end_id stays the same (root)
            "vp.hop_count + 1 as hop_count".to_string(),
        ];
        if self.needs_path_data() {
            select_items.push(format!(
                "{ac}({}, vp.path_relationships) as path_relationships",
                self.get_relationship_type_array()
            ));
        } else {
            select_items.push(format!("{empty_str_arr} as path_relationships"));
        }
        // PREPEND the new node to path_nodes
        select_items.push(format!(
            "{ac}({}, vp.path_nodes) as path_nodes",
            arr(&emit_id_expr("new_start", &start_id_identifier))
        ));
        // #606: PREPEND this hop's edge tuple (path grows at the front here). The
        // hop is new_start -> current_node (a child pointing via its FK to the
        // previous start), so the edge identity is
        // (new_start.<start_id>, current_node.<start_id>). Gated by
        // uses_edge_uniqueness() to agree with the base seed and cycle check.
        if self.uses_edge_uniqueness() {
            select_items.push(format!(
                "{ac}({}, vp.path_edges) as path_edges",
                arr(&self.build_fk_edge_tuple(
                    "new_start",
                    &self.start_node_id_column,
                    "current_node",
                    &self.start_node_id_column,
                ))
            ));
        }

        // Add properties: end properties from CTE, start properties from new joined node
        for prop in &self.properties {
            if prop.cypher_alias == self.start_cypher_alias {
                select_items.push(format!(
                    "{}.{} as start_{}",
                    "new_start", prop.column_name, prop.alias
                ));
            }
            if prop.cypher_alias == self.end_cypher_alias {
                select_items.push(format!("vp.end_{} as end_{}", prop.alias, prop.alias));
            }
        }

        let select_clause = select_items.join(",\n        ");

        let mut where_conditions = vec![
            format!("vp.hop_count < {}", max_hops),
            // #606: edge-unique when uses_edge_uniqueness(), else legacy node-unique.
            if self.uses_edge_uniqueness() {
                emit_edge_cycle_check(&self.build_fk_edge_tuple(
                    "new_start",
                    &self.start_node_id_column,
                    "current_node",
                    &self.start_node_id_column,
                ))
            } else {
                emit_cycle_check(&emit_id_expr("new_start", &start_id_identifier))
            },
        ];

        // Add edge constraints if defined in schema
        // FK-edge PREPEND: from=new_start (child), to=current_node (previous start)
        if let Some(constraint_filter) =
            self.generate_edge_constraint_filter(Some("new_start"), Some("current_node"))
        {
            where_conditions.push(constraint_filter);
        }

        // ✅ HOLISTIC FIX: Add relationship filters in FK-edge recursive (prepend) case
        // In PREPEND expansion, relationship properties are on new_start (the edge/FK table)
        // Rewrite filter alias from 'start_node' to 'new_start'
        if let Some(ref filters) = self.relationship_filters {
            let rewritten_filter = filters.replace("start_node.", "new_start.");
            log::debug!(
                "Adding relationship filters to FK-edge recursive (prepend) case: {} -> {}",
                filters,
                rewritten_filter
            );
            where_conditions.push(rewritten_filter);
        }

        let where_clause = where_conditions.join("\n      AND ");

        // PREPEND expansion: anchor on start_id, find nodes whose parent_id points to it
        // current_node = previous start (e.g., Documents)
        // new_start = a child of current (e.g., Work where Work.parent_id = Documents.object_id)
        // #713 composite-aware JOIN conditions (symmetric to APPEND):
        //  - `vp.start_id = current_node.<id>`: concat-form node side to match the
        //    materialized vp.start_id.
        //  - `new_start.<fk> = current_node.<id>`: per-column zip. Single-column
        //    ids degrade byte-identically to the old form.
        let anchor_on = format!(
            "vp.start_id = {}",
            emit_id_expr("current_node", &start_id_identifier)
        );
        let child_on =
            Identifier::from_comma_separated(&self.fk_hop_fk_column(&self.start_node_id_column))
                .to_sql_equality(
                    "new_start",
                    &Identifier::from_comma_separated(&self.start_node_id_column),
                    "current_node",
                );
        format!(
            "    SELECT\n        {select}\n    FROM {cte_name} vp\n    JOIN {current_table} current_node ON {anchor_on}\n    JOIN {start_table} new_start ON {child_on}\n    WHERE {where_clause}",
            select = select_clause,
            cte_name = cte_name,
            current_table = self.format_table_name(&self.start_node_table),
            anchor_on = anchor_on,
            start_table = self.format_table_name(&self.start_node_table),
            child_on = child_on,
            where_clause = where_clause
        )
    }

    // ======================================================================
    // MIXED PATTERN GENERATION
    // ======================================================================
    // For mixed patterns where one node is denormalized and the other is standard.
    // - Denorm → Standard: Start from rel table (no start table), end with standard table JOIN
    // - Standard → Denorm: Start from standard table, but end is denormalized (no end table JOIN)

    /// #908: Emit the mixed pattern's endpoint property columns into `select_items`
    /// in a FIXED order — every start-side property first, then every end-side
    /// property — for BOTH the base (`recursive == false`) and recursive
    /// (`recursive == true`) arms, and return the own-table JOIN(s) the denorm
    /// endpoint(s) need.
    ///
    /// Column ORDER must be identical across the two arms: a recursive CTE binds
    /// its `UNION ALL` columns BY POSITION, not by name, so any per-arm ordering
    /// difference silently swaps values between output columns on hops ≥ 2 (the
    /// #908-review defect: base emitted `[end_name, start_name]` while the
    /// recursive arm emitted `[start_name, end_name]`). Centralizing emission here
    /// guarantees the two arms agree.
    ///
    /// Per side, the source depends on standard-vs-denorm and base-vs-recursive:
    ///   - START (fixed across the recursion): base reads the standard node
    ///     (`start_node.<col>`) or the denorm own-table join; recursive CARRIES the
    ///     already-materialized column (`vp.start_<alias>`) in BOTH standard and
    ///     denorm cases.
    ///   - END (advances each hop): standard reads the joined end node
    ///     (`end_node.<col>`) in both arms; denorm re-derives via a fresh own-table
    ///     join in both arms.
    ///
    /// Returns `(start_join, end_join)` — the denorm own-table joins to append to
    /// FROM (a standard side or the recursive carried start yields `None`).
    fn emit_mixed_property_items(
        &self,
        recursive: bool,
        select_items: &mut Vec<String>,
    ) -> (Option<String>, Option<String>) {
        // START side first (stable position across arms).
        let start_join = if self.start_is_denormalized {
            if recursive {
                // Carried from the CTE (base seeded it via the own-table join).
                for prop in &self.properties {
                    if prop.cypher_alias == self.start_cypher_alias {
                        select_items
                            .push(format!("vp.start_{} as start_{}", prop.alias, prop.alias));
                    }
                }
                None
            } else {
                self.mixed_denorm_endpoint_property_items(true, select_items)
            }
        } else {
            for prop in &self.properties {
                if prop.cypher_alias == self.start_cypher_alias {
                    if recursive {
                        select_items
                            .push(format!("vp.start_{} as start_{}", prop.alias, prop.alias));
                    } else {
                        select_items.push(format!(
                            "{}.{} as start_{}",
                            self.start_node_alias, prop.column_name, prop.alias
                        ));
                    }
                }
            }
            None
        };

        // END side second.
        let end_join = if self.end_is_denormalized {
            // Denorm end re-derives via a fresh own-table join in BOTH arms.
            self.mixed_denorm_endpoint_property_items(false, select_items)
        } else {
            for prop in &self.properties {
                if prop.cypher_alias == self.end_cypher_alias {
                    select_items.push(format!(
                        "{}.{} as end_{}",
                        self.end_node_alias, prop.column_name, prop.alias
                    ));
                }
            }
            None
        };

        (start_join, end_join)
    }

    /// #908: Project a DENORMALIZED mixed endpoint's requested properties and
    /// return the own-table JOIN needed to resolve its non-id properties.
    ///
    /// `is_start` selects the start (true) or end (false) endpoint. Returns `None`
    /// (pushes nothing) unless that endpoint is denormalized.
    ///
    /// A denorm endpoint of a MIXED pattern is always the foreign-embedded /
    /// partial-role shape: its `*_node_table` is the EDGE table and its
    /// `*_node_id_column` is the EMBEDDED id column (e.g. `reports` / `mgr_id`),
    /// because only its id lives on the edge. Every non-id property lives on the
    /// node's OWN table (the fully-denormalized shape, where a role embeds all
    /// properties, is not mixed — it is intercepted earlier by
    /// `DenormalizedCteStrategy`). So the id property maps to the already-
    /// materialized `start_id`/`end_id`, and any other property is read from the
    /// own table (resolved from the schema: node label → table + node_id + column)
    /// via a LEFT JOIN back on the embedded id link
    /// (`own.<node_id> = rel.<embedded_id_col>`). Without this the outer SELECT
    /// references `t.start_<prop>` that the CTE never projects → ClickHouse Code 47.
    fn mixed_denorm_endpoint_property_items(
        &self,
        is_start: bool,
        select_items: &mut Vec<String>,
    ) -> Option<String> {
        let denormalized = if is_start {
            self.start_is_denormalized
        } else {
            self.end_is_denormalized
        };
        if !denormalized {
            return None;
        }
        let cypher_alias = if is_start {
            &self.start_cypher_alias
        } else {
            &self.end_cypher_alias
        };
        let out_prefix = if is_start { "start" } else { "end" };
        // The embedded id column on the edge (from_col for the start role, to_col
        // for the end role) and the id expression already used for start/end_id.
        let embedded_id_col = if is_start {
            &self.relationship_from_column
        } else {
            &self.relationship_to_column
        };
        let id_expr = format!("{}.{}", self.relationship_alias, embedded_id_col);

        // Resolve the denorm node's schema (own table + node_id) from the
        // relationship type and the from/to role. Without a schema match we cannot
        // know the own table, so fall back to the pre-#908 behavior (project
        // nothing — the outer projection gap is pre-existing).
        let rel_type = self.relationship_types.as_ref().and_then(|t| t.first())?;
        let rel_schema = self.schema.get_relationships_schema_opt(rel_type)?;
        let node_label = if is_start {
            &rel_schema.from_node
        } else {
            &rel_schema.to_node
        };
        let node_schema = self.schema.node_schema_opt(node_label)?;
        // Qualify the own table with the node's OWN database (the generator's
        // `format_table_name` uses the generator-level default database, which may
        // differ from or be absent for this node). `table_name` may already be
        // db-qualified — only prefix when it isn't.
        let own_table = if node_schema.table_name.contains('.') || node_schema.database.is_empty() {
            node_schema.table_name.clone()
        } else {
            format!("{}.{}", node_schema.database, node_schema.table_name)
        };
        // The own-table node_id. #908 review: only the single-column case is
        // supported here — the embedded id link (`embedded_id_col`) is a single
        // relationship column, so a COMPOSITE own node_id can't be matched against
        // it. Bail (return None → project nothing, the pre-#908 loud gap) rather
        // than silently joining on only the first column (wrong / fan-out). Also
        // bail if the node declares no id column at all, instead of emitting a
        // join on a fabricated `id` column that may not exist.
        let own_id_cols = node_schema.node_id.columns();
        if own_id_cols.len() != 1 {
            return None;
        }
        let own_node_id = own_id_cols[0];
        let own_alias = format!("{}_own", out_prefix);

        // The id property maps to the id expression; any non-id property is read
        // from the own table via the LEFT JOIN below (see the doc comment above).
        // Collect the non-id property columns so the join target can be a
        // deduplicated subquery (one row per node_id) — see the join comment.
        let mut own_cols: Vec<String> = Vec::new();
        for prop in &self.properties {
            if &prop.cypher_alias != cypher_alias {
                continue;
            }
            if prop.column_name == own_node_id || prop.column_name == *embedded_id_col {
                select_items.push(format!("{} as {}_{}", id_expr, out_prefix, prop.alias));
            } else {
                if !own_cols.contains(&prop.column_name) {
                    own_cols.push(prop.column_name.clone());
                }
                select_items.push(format!(
                    "{}.{} as {}_{}",
                    own_alias, prop.column_name, out_prefix, prop.alias
                ));
            }
        }

        if !own_cols.is_empty() {
            // LEFT JOIN (not INNER): the denorm endpoint's id is already fixed by
            // the edge, so this join only RESOLVES its own-table properties — it
            // must NOT add or drop path rows.
            //
            // #908 review: the join is emitted whenever a denorm-endpoint non-id
            // property is in `self.properties`, which the planner populates even for
            // `RETURN count(*)` / `RETURN b.name` (the unused `start_<prop>` SELECT
            // column is pruned downstream, but the JOIN would remain). Unlike the
            // standard `end_node` join (which already exists on `main` for the id),
            // a START-side own-table join is a NEW join surface, so on a node table
            // with a duplicated node_id it would FAN OUT and inflate `count(*)`
            // (review: 11 vs 9). To be provably row-preserving regardless of id
            // uniqueness, join a DEDUPLICATED subquery that yields exactly one row
            // per node_id (`GROUP BY node_id`, `any()`/`any_value()` on the property
            // columns) instead of the raw table.
            let any_fn = current_function_mapper().any();
            let projected: Vec<String> = own_cols
                .iter()
                .map(|c| format!("{any_fn}({c}) as {c}"))
                .collect();
            Some(format!(
                "LEFT JOIN (SELECT {node_id}, {cols} FROM {own_table} GROUP BY {node_id}) {own_alias} ON {own_alias}.{node_id} = {id_expr}",
                node_id = own_node_id,
                cols = projected.join(", "),
                own_table = own_table,
                own_alias = own_alias,
                id_expr = id_expr,
            ))
        } else {
            None
        }
    }

    /// Rewrite a denormalized-endpoint filter string (#934). The incoming
    /// `filters` string is already property-mapped to CH columns and prefixed
    /// with `node_prefix` (`start_node.` / `end_node.`). The endpoint's id
    /// property maps to the edge's own FK column (`id_col`, e.g. `mgr_id`), so
    /// it stays on the relationship alias (`rel.`); every OTHER (non-id) column
    /// lives on the node's own table and must resolve via the `*_own` LEFT JOIN
    /// (`own_prefix`, `start_own.` / `end_own.`) that #908 already emits.
    ///
    /// Order matters: the specific id-column replace runs first, then the
    /// general fallback maps any remaining `node_prefix.` to `own_prefix.`, so a
    /// combined predicate like `(start_node.mgr_id = 1 AND start_node.name =
    /// 'Alice')` splits correctly to `(rel.mgr_id = 1 AND start_own.name =
    /// 'Alice')`. Composite ids are handled by replacing each id column in turn.
    ///
    /// Step 1 uses a WORD-BOUNDARY-aware replace (`replace_column_token`) so an
    /// id column `mgr_id` does not corrupt a non-id column `mgr_id_extra` whose
    /// name has the id column as a prefix (`start_node.mgr_id_extra` must stay a
    /// non-id column, routed to `*_own` in step 2, not become `rel.mgr_id_extra`).
    /// Both steps run only OUTSIDE single-quoted string literals
    /// (`rewrite_outside_string_literals`) so a value literal containing the
    /// prefix text (e.g. `= 'end_node.x'`) is left intact.
    ///
    /// `own_join_present` guards the non-id fallback: the `*_own` join is only
    /// emitted when a denorm-endpoint non-id property is in `self.properties`
    /// (which the planner populates from WHERE predicates too). If it is somehow
    /// absent, fall back to the old whole-string `rel.` rewrite rather than
    /// emitting a dangling `*_own.` reference — preserves prior behavior for any
    /// path that only carried the id filter.
    fn rewrite_denorm_endpoint_filter(
        &self,
        filters: &str,
        node_prefix: &str,
        own_prefix: &str,
        id_col: &str,
        own_join_present: bool,
    ) -> String {
        if !own_join_present {
            // No own-table join in scope — nothing to resolve non-id columns
            // against. Keep the historical behavior (id-only filters still map
            // correctly onto the edge alias). Still literal-safe.
            let rel_prefix = format!("{}.", self.relationship_alias);
            return Self::rewrite_outside_string_literals(filters, |seg| {
                seg.replace(node_prefix, &rel_prefix)
            });
        }
        // Both rewrite steps run only on the NON-string-literal spans of the
        // predicate (#934 Defect 3): a value literal like `'end_node.x'` must not
        // be mangled by the column-prefix rewrites.
        let rel_alias = self.relationship_alias.clone();
        Self::rewrite_outside_string_literals(filters, |seg| {
            // Step 1: pin each id column to the relationship alias. `id_col` may
            // be a comma-separated composite (mirroring
            // `relationship_from/to_column`). Word-boundary aware so an id column
            // does not over-match a longer non-id column that has it as a prefix.
            let mut out = seg.to_string();
            for col in id_col.split(',') {
                let col = col.trim();
                if col.is_empty() {
                    continue;
                }
                out = Self::replace_column_token(
                    &out,
                    &format!("{}{}", node_prefix, col),
                    &format!("{}.{}", rel_alias, col),
                );
            }
            // Step 2: every remaining non-id column resolves via the own-table join.
            out.replace(node_prefix, own_prefix)
        })
    }

    /// Apply `rewrite` to every span of `s` that is OUTSIDE a single-quoted
    /// string literal, leaving literal contents untouched (#934 Defect 3). SQL
    /// string literals escape an embedded quote by doubling it (`''`); this
    /// scanner treats `''` inside a literal as an escaped quote, not a close.
    /// Column-prefix rewrites must not corrupt a value literal that happens to
    /// contain the prefix text (e.g. `WHERE b.name = 'end_node.x'`).
    fn rewrite_outside_string_literals(s: &str, rewrite: impl Fn(&str) -> String) -> String {
        let mut out = String::with_capacity(s.len());
        let mut buf = String::new();
        let mut in_literal = false;
        let mut chars = s.chars().peekable();
        while let Some(c) = chars.next() {
            if in_literal {
                out.push(c);
                if c == '\'' {
                    if chars.peek() == Some(&'\'') {
                        // Escaped quote ('') — consume the pair, stay in literal.
                        out.push(chars.next().unwrap());
                    } else {
                        in_literal = false;
                    }
                }
            } else if c == '\'' {
                // Flush the accumulated non-literal span through the rewrite.
                out.push_str(&rewrite(&buf));
                buf.clear();
                out.push(c);
                in_literal = true;
            } else {
                buf.push(c);
            }
        }
        out.push_str(&rewrite(&buf));
        out
    }

    /// Replace occurrences of `needle` (a `prefix.column` token) with
    /// `replacement`, but ONLY when the character immediately following `needle`
    /// is not an identifier character (`[A-Za-z0-9_]`) — i.e. `needle` is a whole
    /// column token, not a prefix of a longer column name. This prevents the id
    /// column `start_node.mgr_id` from matching inside `start_node.mgr_id_extra`
    /// (which is a distinct non-id column). A plain `str::replace` would over-match.
    fn replace_column_token(haystack: &str, needle: &str, replacement: &str) -> String {
        if needle.is_empty() {
            return haystack.to_string();
        }
        let mut out = String::with_capacity(haystack.len());
        let mut rest = haystack;
        while let Some(pos) = rest.find(needle) {
            out.push_str(&rest[..pos]);
            let after = &rest[pos + needle.len()..];
            let boundary_ok = after
                .chars()
                .next()
                .map(|c| !(c.is_ascii_alphanumeric() || c == '_'))
                .unwrap_or(true); // end-of-string is a boundary
            if boundary_ok {
                out.push_str(replacement);
            } else {
                out.push_str(needle);
            }
            rest = after;
        }
        out.push_str(rest);
        out
    }

    /// Generate base case for mixed patterns
    fn generate_mixed_base_case(&self, hop_count: u32) -> String {
        if hop_count != 1 {
            // Multi-hop base case not yet supported for mixed
            let empty_arr = arr("");
            return format!(
                "    -- Multi-hop base case for {hop_count} hops (mixed - not yet supported)\n    SELECT NULL as start_id, NULL as end_id, {hop_count} as hop_count, {empty_arr} as path_relationships, {empty_arr} as path_nodes\n    WHERE false"
            );
        }

        let empty_str_arr = current_function_mapper().empty_string_array_cast();
        // Determine start_id and end_id based on which side is denormalized
        let start_id_expr = if self.start_is_denormalized {
            // Start is denorm: ID comes from relationship table from_col
            format!(
                "{}.{}",
                self.relationship_alias, self.relationship_from_column
            )
        } else {
            // Start is standard: ID comes from start node table
            format!("{}.{}", self.start_node_alias, self.start_node_id_column)
        };

        let end_id_expr = if self.end_is_denormalized {
            // End is denorm: ID comes from relationship table to_col
            format!(
                "{}.{}",
                self.relationship_alias, self.relationship_to_column
            )
        } else {
            // End is standard: ID comes from end node table
            format!("{}.{}", self.end_node_alias, self.end_node_id_column)
        };

        let mut select_items = vec![
            format!("{} as start_id", start_id_expr),
            format!("{} as end_id", end_id_expr),
            "1 as hop_count".to_string(),
        ];
        if self.needs_path_data() {
            select_items.push(self.generate_relationship_type_for_hop(1));
        } else {
            select_items.push(format!("{empty_str_arr} as path_relationships"));
        }
        select_items.push(format!(
            "{} as path_nodes",
            arr(&format!("{}, {}", start_id_expr, end_id_expr))
        ));

        // #808/#606: seed `path_edges` with this hop's edge-identity tuple so the
        // recursive step can enforce Cypher's default RELATIONSHIP-uniqueness (an
        // edge may not be reused, but a node MAY be revisited). The mixed arm
        // previously carried only `path_nodes` and cycle-checked on the endpoint
        // node id — NODE-uniqueness — which silently dropped every valid path
        // that legitimately revisits a node, inconsistent with every sibling arm
        // (standard/FK/denorm all use edge-uniqueness). `rel` is joined in both
        // mixed FROM branches, so `build_edge_tuple_recursive` reads the edge_id
        // (or `(from,to)`) tuple off it directly. Gated by `uses_edge_uniqueness`
        // so shortestPath/zero-hop stay node-unique and byte-unchanged.
        if self.uses_edge_uniqueness() {
            select_items.push(format!(
                "{} as path_edges",
                arr(&self.build_edge_tuple_recursive(&self.relationship_alias))
            ));
        }

        // #908: Emit endpoint properties in a FIXED start-then-end order (base
        // arm) so the recursive arm's UNION ALL columns line up positionally, and
        // collect the denorm own-table join(s) to append to FROM. See
        // `emit_mixed_property_items`.
        let (denorm_start_join, denorm_end_join) =
            self.emit_mixed_property_items(false, &mut select_items);

        let select_clause = select_items.join(",\n        ");

        // Build FROM clause based on which nodes are denormalized
        let from_clause = if self.start_is_denormalized && !self.end_is_denormalized {
            // Denorm → Standard: FROM rel_table JOIN end_table
            format!(
                "FROM {rel_table} {rel}\n    JOIN {end_table} {end} ON {rel}.{to_col} = {end}.{end_id_col}",
                rel_table = self.format_table_name(&self.relationship_table),
                rel = self.relationship_alias,
                end_table = self.format_table_name(&self.end_node_table),
                end = self.end_node_alias,
                to_col = self.relationship_to_column,
                end_id_col = self.end_node_id_column
            )
        } else if !self.start_is_denormalized && self.end_is_denormalized {
            // Standard → Denorm: FROM start_table JOIN rel_table
            format!(
                "FROM {start_table} {start}\n    JOIN {rel_table} {rel} ON {start}.{start_id_col} = {rel}.{from_col}",
                start_table = self.format_table_name(&self.start_node_table),
                start = self.start_node_alias,
                rel_table = self.format_table_name(&self.relationship_table),
                rel = self.relationship_alias,
                start_id_col = self.start_node_id_column,
                from_col = self.relationship_from_column
            )
        } else {
            // Shouldn't reach here - handled by is_denormalized check
            format!(
                "FROM {rel_table} {rel}",
                rel_table = self.format_table_name(&self.relationship_table),
                rel = self.relationship_alias
            )
        };

        let mut query = format!(
            "    SELECT \n        {select}\n    {from_clause}",
            select = select_clause,
            from_clause = from_clause
        );

        // #908: append the denorm endpoint's own-table join (foreign-embedded
        // shape) so its non-id properties resolve. Only one of the two can be set
        // in a mixed pattern (exactly one endpoint is denormalized).
        // #934: capture presence BEFORE the `.or()` move — the WHERE rewrite below
        // routes non-id endpoint props to `*_own` only when its join is emitted.
        let start_own_join_present = denorm_start_join.is_some();
        let end_own_join_present = denorm_end_join.is_some();
        if let Some(join) = denorm_start_join.or(denorm_end_join) {
            query.push_str(&format!("\n    {}", join));
        }

        // Add WHERE conditions
        let mut where_conditions = Vec::new();
        if let Some(ref filters) = self.start_node_filters {
            // Rewrite for denorm start if needed.
            //
            // #934: the start filter string arrives already property-mapped to
            // CH columns and prefixed `start_node.<col>`. The old blanket
            // `start_node.` -> `rel.` rewrite was correct ONLY for the start
            // node's id property (which maps to the edge's own FK column,
            // `relationship_from_column`, e.g. `mgr_id`) — a NON-id property
            // (e.g. `name`) lives on the node's OWN table, not the edge, so
            // `rel.name` is an unknown identifier (Code 47). The `start_own`
            // LEFT JOIN (#908, `denorm_start_join`) already resolves those own
            // columns; route non-id start props to `start_own.` and keep the id
            // prop on `rel.`, mirroring the id-vs-non-id branch in
            // `mixed_denorm_endpoint_property_items`.
            let rewritten = if self.start_is_denormalized {
                self.rewrite_denorm_endpoint_filter(
                    filters,
                    "start_node.",
                    "start_own.",
                    &self.relationship_from_column,
                    start_own_join_present,
                )
            } else {
                filters.clone()
            };
            where_conditions.push(rewritten);
        }

        // #1003: only inject the end-node filter into the base arm when it is
        // NOT deferred to the outer wrapper. The mixed generators previously
        // injected unconditionally, so the end predicate constrained the
        // endpoint at EVERY hop (an intermediate node on any longer path) rather
        // than only the terminal endpoint — silently dropping valid paths (0 rows
        // where matches exist). `end_filter_in_base_recursive_case()` (the #607
        // gate the standard generators already consult at the analogous sites)
        // returns false for a multi-hop mixed VLP, because a mixed pattern has
        // exactly one embedded endpoint (not both), and
        // `end_filter_applied_in_wrapper()` emits the terminal-endpoint predicate
        // on the carried `end_name` column in the outer `vlp_* AS (SELECT * FROM
        // ..._inner WHERE end_name = ... AND hop_count >= min)` CTE. A pure
        // single-hop mixed `*1..1` renders a flat self-join (no CTE) and never
        // reaches this generator, so no wrapper-less shape loses the filter. Start
        // filters are untouched (the start is fixed across the recursion).
        if self.shortest_path_mode.is_none() && self.end_filter_in_base_recursive_case() {
            if let Some(ref filters) = self.end_node_filters {
                // Rewrite for denorm end if needed. #934: symmetric to the start
                // side — a non-id property of a denormalized END node resolves
                // via the `end_own` join (`denorm_end_join`), while the end id
                // maps to the edge's `relationship_to_column`.
                let rewritten = if self.end_is_denormalized {
                    self.rewrite_denorm_endpoint_filter(
                        filters,
                        "end_node.",
                        "end_own.",
                        &self.relationship_to_column,
                        end_own_join_present,
                    )
                } else {
                    filters.clone()
                };
                where_conditions.push(rewritten);
            }
        }

        // ✅ HOLISTIC FIX: Add relationship filters in mixed base case
        if let Some(ref filters) = self.relationship_filters {
            log::debug!(
                "Adding relationship filters to mixed base case: {}",
                filters
            );
            where_conditions.push(filters.clone());
        }

        if !where_conditions.is_empty() {
            query.push_str(&format!("\n    WHERE {}", where_conditions.join(" AND ")));
        }

        query
    }

    /// Generate recursive case for mixed patterns
    fn generate_mixed_recursive_case(&self, max_hops: u32, cte_name: &str) -> String {
        let fmap = current_function_mapper();
        let ac = fmap.array_concat();
        let empty_str_arr = fmap.empty_string_array_cast();
        // End ID expression based on denormalization
        let end_id_expr = if self.end_is_denormalized {
            format!(
                "{}.{}",
                self.relationship_alias, self.relationship_to_column
            )
        } else {
            format!("{}.{}", self.end_node_alias, self.end_node_id_column)
        };

        let mut select_items = vec![
            "vp.start_id".to_string(),
            format!("{} as end_id", end_id_expr),
            "vp.hop_count + 1 as hop_count".to_string(),
        ];
        if self.needs_path_data() {
            select_items.push(format!(
                "{ac}(vp.path_relationships, {}) as path_relationships",
                self.get_relationship_type_array()
            ));
        } else {
            select_items.push(format!("{empty_str_arr} as path_relationships"));
        }
        // path_nodes always maintained for cycle detection
        select_items.push(format!(
            "{ac}(vp.path_nodes, {}) as path_nodes",
            arr(&end_id_expr)
        ));

        // #808/#606: extend `path_edges` with this hop's edge-identity tuple so
        // the shape matches the base seed and the edge-uniqueness cycle check
        // below (Cypher relationship-uniqueness). Gated identically to the base
        // via `uses_edge_uniqueness` (shortestPath/zero-hop stay node-unique and
        // never carry a `path_edges` column, so nothing to extend there).
        if self.uses_edge_uniqueness() {
            select_items.push(format!(
                "{ac}(vp.path_edges, {}) as path_edges",
                arr(&self.build_edge_tuple_recursive(&self.relationship_alias))
            ));
        }

        // #908: Emit endpoint properties in the SAME fixed start-then-end order as
        // the base arm (recursive mode) so the UNION ALL columns line up
        // positionally. START columns are carried from the CTE; a denorm END
        // re-derives via a fresh own-table join appended to FROM below.
        let (_start_join, denorm_end_recursive_join) =
            self.emit_mixed_property_items(true, &mut select_items);

        let select_clause = select_items.join(",\n        ");

        // Build FROM/JOIN clause based on denormalization
        let from_clause = if self.start_is_denormalized && !self.end_is_denormalized {
            // Denorm → Standard: CTE → rel → end_table
            format!(
                "FROM {cte_name} vp\n    JOIN {rel_table} {rel} ON vp.end_id = {rel}.{from_col}\n    JOIN {end_table} {end} ON {rel}.{to_col} = {end}.{end_id_col}",
                cte_name = cte_name,
                rel_table = self.format_table_name(&self.relationship_table),
                rel = self.relationship_alias,
                from_col = self.relationship_from_column,
                end_table = self.format_table_name(&self.end_node_table),
                end = self.end_node_alias,
                to_col = self.relationship_to_column,
                end_id_col = self.end_node_id_column
            )
        } else if !self.start_is_denormalized && self.end_is_denormalized {
            // Standard → Denorm: CTE → rel (no end table)
            // PERF: Removed redundant current_node JOIN - vp.end_id already contains the ID
            format!(
                "FROM {cte_name} vp\n    JOIN {rel_table} {rel} ON vp.end_id = {rel}.{from_col}",
                cte_name = cte_name,
                rel_table = self.format_table_name(&self.relationship_table),
                rel = self.relationship_alias,
                from_col = self.relationship_from_column
            )
        } else {
            // Shouldn't reach here
            format!(
                "FROM {cte_name} vp\n    JOIN {rel_table} {rel} ON vp.end_id = {rel}.{from_col}",
                cte_name = cte_name,
                rel_table = self.format_table_name(&self.relationship_table),
                rel = self.relationship_alias,
                from_col = self.relationship_from_column
            )
        };

        // #808/#606: enforce EDGE-uniqueness (an edge may not be reused, but a
        // node MAY be revisited — Cypher's default) by testing the new hop's
        // edge-identity tuple against `path_edges`, matching every sibling arm.
        // Node-uniqueness (`NOT has(path_nodes, end_id)`) is retained only for
        // shortestPath / zero-hop via `uses_edge_uniqueness`, byte-unchanged.
        let cycle_check = if self.uses_edge_uniqueness() {
            emit_edge_cycle_check(&self.build_edge_tuple_recursive(&self.relationship_alias))
        } else {
            emit_cycle_check(&end_id_expr)
        };

        let mut where_conditions = vec![format!("vp.hop_count < {}", max_hops), cycle_check];

        // #1003: gate the recursive-arm end filter on the same #607 wrapper
        // decision as the base arm above. Without it the end predicate is
        // re-applied on every recursive hop's (intermediate) endpoint, dropping
        // valid longer paths; the terminal-endpoint filter lives in the outer
        // wrapper. Mixed-only (fully-denorm is handled by DenormalizedCteStrategy
        // and never constructs this struct).
        if self.shortest_path_mode.is_none() && self.end_filter_in_base_recursive_case() {
            if let Some(ref filters) = self.end_node_filters {
                // #934: mirror the base-case rewrite on the recursive arm. A
                // non-id property of a denormalized END node must resolve via the
                // `end_own` join (`denorm_end_recursive_join`, emitted just below
                // in the FROM clause) — the old blanket `end_node.` -> `rel.`
                // rewrite pointed a non-id column at the edge table (which lacks
                // it) → Code 47 on the recursive UNION arm for any `*n..m`, m >= 2.
                // The end id property still maps to the edge's `to` column.
                let rewritten = if self.end_is_denormalized {
                    self.rewrite_denorm_endpoint_filter(
                        filters,
                        "end_node.",
                        "end_own.",
                        &self.relationship_to_column,
                        denorm_end_recursive_join.is_some(),
                    )
                } else {
                    filters.clone()
                };
                where_conditions.push(rewritten);
            }
        }

        // ✅ HOLISTIC FIX: Add relationship filters in mixed recursive case
        if let Some(ref filters) = self.relationship_filters {
            log::debug!(
                "Adding relationship filters to mixed recursive case: {}",
                filters
            );
            where_conditions.push(filters.clone());
        }

        let where_clause = where_conditions.join("\n      AND ");

        // #908: append the denorm END endpoint's own-table join (foreign-embedded)
        // so its non-id properties resolve on each recursive hop.
        let from_clause = match denorm_end_recursive_join {
            Some(join) => format!("{from_clause}\n    {join}"),
            None => from_clause,
        };

        format!(
            "    SELECT\n        {select}\n    {from_clause}\n    WHERE {where_clause}",
            select = select_clause,
            from_clause = from_clause,
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

    /// #934: `replace_column_token` must be word-boundary aware so an id column
    /// does not corrupt a longer non-id column that has it as a prefix.
    #[test]
    fn replace_column_token_respects_word_boundary_934() {
        let rep = VariableLengthCteGenerator::replace_column_token;
        // Whole-token match at end-of-string and before an operator/space.
        assert_eq!(
            rep("start_node.mgr_id = 1", "start_node.mgr_id", "rel.mgr_id"),
            "rel.mgr_id = 1"
        );
        assert_eq!(
            rep("x = start_node.mgr_id", "start_node.mgr_id", "rel.mgr_id"),
            "x = rel.mgr_id"
        );
        // PREFIX COLLISION: `mgr_id` must NOT match inside `mgr_id_extra`.
        assert_eq!(
            rep(
                "start_node.mgr_id_extra = 1",
                "start_node.mgr_id",
                "rel.mgr_id"
            ),
            "start_node.mgr_id_extra = 1",
            "id column must not over-match a longer non-id column with the same prefix"
        );
        // Mixed: the real id token is rewritten, the colliding prefix is left alone.
        assert_eq!(
            rep(
                "(start_node.mgr_id = 1 AND start_node.mgr_id_extra = 2)",
                "start_node.mgr_id",
                "rel.mgr_id"
            ),
            "(rel.mgr_id = 1 AND start_node.mgr_id_extra = 2)"
        );
        // Trailing underscore/alnum are identifier chars → no match.
        assert_eq!(
            rep("start_node.mgr_id9", "start_node.mgr_id", "rel.mgr_id"),
            "start_node.mgr_id9"
        );
        assert_eq!(
            rep("start_node.mgr_id_", "start_node.mgr_id", "rel.mgr_id"),
            "start_node.mgr_id_"
        );
        // Closing paren / comparison operators are boundaries.
        assert_eq!(
            rep("(start_node.mgr_id)", "start_node.mgr_id", "rel.mgr_id"),
            "(rel.mgr_id)"
        );
        assert_eq!(
            rep("start_node.mgr_id>5", "start_node.mgr_id", "rel.mgr_id"),
            "rel.mgr_id>5"
        );
    }

    /// #934 Defect 3: `rewrite_outside_string_literals` must skip single-quoted
    /// literals so a value containing the column-prefix text is not corrupted.
    #[test]
    fn rewrite_outside_string_literals_skips_literals_934() {
        use VariableLengthCteGenerator as G;
        // Each call takes a distinct closure type, so call the method directly
        // rather than binding it to a single monomorphized `let`.
        // The prefix inside a literal is untouched; outside it is rewritten.
        assert_eq!(
            G::rewrite_outside_string_literals("start_node.name = 'start_node.x'", |s| s
                .replace("start_node.", "start_own.")),
            "start_own.name = 'start_node.x'"
        );
        // Escaped quote ('') inside a literal keeps the literal open.
        assert_eq!(
            G::rewrite_outside_string_literals(
                "a = 'it''s start_node.here' AND start_node.b = 1",
                |s| s.replace("start_node.", "X.")
            ),
            "a = 'it''s start_node.here' AND X.b = 1"
        );
        // No literals → whole string rewritten.
        assert_eq!(
            G::rewrite_outside_string_literals("start_node.a = start_node.b", |s| s
                .replace("start_node.", "X.")),
            "X.a = X.b"
        );
        // Only a literal → untouched.
        assert_eq!(
            G::rewrite_outside_string_literals("'start_node.only'", |s| s
                .replace("start_node.", "X.")),
            "'start_node.only'"
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
        let generator = VariableLengthCteGenerator::new(
            &schema, // Add schema parameter
            spec,
            "users",     // start table
            "user_id",   // start id column
            "authored",  // relationship table
            "author_id", // from column
            "post_id",   // to column
            "posts",     // end table
            "post_id",   // end id column
            "u",         // start alias
            "p",         // end alias
            vec![],      // no properties for test
            None,        // no shortest path mode
            None,        // no start node filters
            None,        // no end node filters
            None,        // no path variable
            None,        // no relationship types
            None,        // no edge_id (use default from_id, to_id)
        );

        let cte = generator.generate_cte();
        println!("Generated CTE: {}", cte.cte_name);

        // Test that CTE was created
        assert!(!cte.cte_name.is_empty());
        assert!(cte.cte_name.starts_with("vlp_"));
    }

    #[test]
    fn test_unbounded_variable_length() {
        let schema = create_test_schema();
        let spec = VariableLengthSpec::unbounded();
        let generator = VariableLengthCteGenerator::new(
            &schema, // Add schema parameter
            spec,
            "users",       // start table
            "user_id",     // start id column
            "follows",     // relationship table
            "follower_id", // from column
            "followed_id", // to column
            "users",       // end table
            "user_id",     // end id column
            "u1",          // start alias
            "u2",          // end alias
            vec![],        // no properties for test
            None,          // no shortest path mode
            None,          // no start node filters
            None,          // no end node filters
            None,          // no path variable
            None,          // no relationship types
            None,          // no edge_id (use default from_id, to_id)
        );

        let sql = generator.generate_recursive_sql();
        println!("Unbounded SQL:\n{}", sql);

        // Should contain recursive case
        assert!(sql.contains("UNION ALL"));
        assert!(sql.contains("hop_count < 5")); // DEFAULT_MAX_HOPS = 5 (reduced from 10 for memory safety)
    }

    /// #628: a CLOSED `*0..N` VLP (same start/end alias) must count real cycles,
    /// which requires EDGE-uniqueness. The zero-hop base seeds an empty
    /// `path_edges` array; hops ≥ 1 accumulate and dedupe via `NOT has(...)`.
    /// An OPEN `*0..N` (distinct aliases) keeps NODE-uniqueness — unchanged.
    #[test]
    fn closed_zero_hop_vlp_uses_edge_uniqueness_628() {
        let schema = create_test_schema();
        let make = |start_alias: &str, end_alias: &str| {
            VariableLengthCteGenerator::new(
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
                vec![],
                None,
                None,
                None,
                None,
                None,
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
        // Zero-hop base seeds an empty path_edges array...
        assert!(
            closed_sql.contains("[] as path_edges"),
            "closed *0..N zero-hop base must seed empty path_edges; got:\n{closed_sql}"
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

        // OPEN: distinct aliases → stays node-unique, no path_edges seed.
        let open = make("a", "b");
        assert!(!open.is_closed_pattern());
        assert!(
            !open.uses_edge_uniqueness(),
            "open *0..N must stay node-unique"
        );
        let open_sql = open.generate_recursive_sql();
        assert!(
            !open_sql.contains("path_edges"),
            "open *0..N must not project path_edges; got:\n{open_sql}"
        );
        assert!(
            open_sql.contains("NOT has(vp.path_nodes,"),
            "open *0..N must use node-uniqueness; got:\n{open_sql}"
        );
    }

    /// #808/#606: the MIXED-access VLP arm (one endpoint denormalized, the other
    /// standard — reached via a foreign-embedded self-loop with a one-sided role
    /// map) previously carried only `path_nodes` and cycle-checked on the
    /// endpoint node id (NODE-uniqueness), silently dropping every valid path
    /// that legitimately revisits a node. It must now enforce Cypher's default
    /// RELATIONSHIP-uniqueness on `path_edges`, consistent with the standard /
    /// FK / denormalized arms. Live-verified against a 3-cycle fixture: `*2..3`
    /// returns 6 relationship-unique trails (3 length-2 + 3 node-revisiting
    /// length-3) versus the old node-unique 3.
    /// #902: a SELF-REFERENCING FK-edge whose `from_id` equals the node_id (the
    /// PK) and whose `to_id` is the real FK (e.g. ldbc `REPLY_OF`:
    /// node_id=commentId, from_id=commentId, to_id=replyOfCommentId). The
    /// base/recursive FK-edge joins previously hardcoded `relationship_from_column`
    /// (=commentId) as the FK, degenerating every hop to an identity self-join
    /// (`x.commentId = y.commentId`) → phantom self-loops. They must instead follow
    /// the actual FK (`replyOfCommentId`), the way the single-hop path does.
    #[test]
    fn fk_edge_selfref_vlp_follows_fk_when_from_id_is_pk_902() {
        let schema = create_test_schema();
        // node_id=commentId, from_id=commentId (=PK), to_id=replyOfCommentId (=FK).
        let gen = VariableLengthCteGenerator::new_with_fk_edge(
            &schema,
            VariableLengthSpec::range(1, 2),
            "comment",          // start table
            "commentId",        // start id col (node_id)
            "comment",          // relationship table (same → self-ref)
            "commentId",        // rel from col (== node_id → the PK side)
            "replyOfCommentId", // rel to col (the real FK)
            "comment",          // end table
            "commentId",        // end id col
            "a",
            "b",
            "r",
            vec![],
            None,                                         // shortest_path_mode
            Some("start_node.commentId = 1".to_string()), // start filter → APPEND path
            None,
            None,
            None,
            Some(vec!["REPLY_OF".to_string()]),
            None, // edge_id
            None, // polymorphic discriminator col
            None, // polymorphic from-role label col
            None, // polymorphic to-role label col
            None, // from_node_label
            None, // to_node_label
            true, // FK-edge pattern (edge is an FK on the node table)
        );
        let sql = gen.generate_recursive_sql();
        // Base + recursive joins must follow the FK column, NOT the PK identity.
        assert!(
            sql.contains("start_node.replyOfCommentId = end_node.commentId"),
            "#902 base join must follow the FK (replyOfCommentId), not commentId identity; got:\n{sql}"
        );
        assert!(
            sql.contains("new_end.replyOfCommentId = ")
                || sql.contains("current_node.replyOfCommentId = "),
            "#902 recursive join must follow the FK (replyOfCommentId); got:\n{sql}"
        );
        assert!(
            !sql.contains("start_node.commentId = end_node.commentId"),
            "#902 must NOT emit the identity self-join start.commentId = end.commentId; got:\n{sql}"
        );
    }

    /// #902 (regression guard): a self-ref FK-edge whose `from_id` IS the real FK
    /// (filesystem PARENT: node_id=object_id, from_id=parent_id, to_id=object_id)
    /// must be UNCHANGED — the fix is a no-op when from_id != node_id, so the join
    /// still follows `parent_id`.
    #[test]
    fn fk_edge_selfref_vlp_keeps_from_id_when_it_is_the_fk_902() {
        let schema = create_test_schema();
        // node_id=object_id, from_id=parent_id (=FK), to_id=object_id (=PK).
        let gen = VariableLengthCteGenerator::new_with_fk_edge(
            &schema,
            VariableLengthSpec::range(1, 2),
            "fs_objects",
            "object_id",
            "fs_objects",
            "parent_id", // rel from col (the FK, differs from node_id)
            "object_id", // rel to col (== node_id → PK)
            "fs_objects",
            "object_id",
            "a",
            "b",
            "r",
            vec![],
            None,
            Some("start_node.object_id = 1".to_string()),
            None,
            None,
            None,
            Some(vec!["PARENT".to_string()]),
            None,
            None,
            None,
            None,
            None,
            None,
            true,
        );
        let sql = gen.generate_recursive_sql();
        assert!(
            sql.contains("start_node.parent_id = end_node.object_id"),
            "#902 no-op case must keep following parent_id (the FK); got:\n{sql}"
        );
    }

    #[test]
    fn mixed_vlp_uses_edge_uniqueness_808() {
        let schema = create_test_schema();
        // Denorm→Standard mixed: start embedded-in-edge, end own-table.
        let gen = VariableLengthCteGenerator::new_mixed(
            &schema,
            VariableLengthSpec::range(2, 3),
            "people",  // start table (denorm start reads from rel; passed for shape)
            "pid",     // start id col
            "reports", // relationship table
            "mgr_id",  // rel from col
            "emp_id",  // rel to col
            "people",  // end table (standard end own-table)
            "pid",     // end id col
            "a",
            "b",
            "",
            vec![],
            None, // shortest_path_mode
            None, // start_node_filters
            None, // end_node_filters
            None, // relationship_filters
            None, // path_variable
            Some(vec!["REPORTS_TO".to_string()]),
            None,  // edge_id (None → (from,to) identity)
            true,  // start endpoint is the denormalized (embedded-in-edge) side
            false, // end endpoint is the standard own-table side
        );
        assert!(
            gen.uses_edge_uniqueness(),
            "mixed range VLP (min_hops>=1, not shortestPath) must use edge-uniqueness"
        );
        let sql = gen.generate_recursive_sql();
        // Base seeds and recursive extends path_edges with the (from,to) tuple.
        assert!(
            sql.contains("[tuple(rel.mgr_id, rel.emp_id)] as path_edges"),
            "mixed base must seed path_edges with the edge tuple; got:\n{sql}"
        );
        assert!(
            sql.contains("arrayConcat(vp.path_edges, [tuple(rel.mgr_id, rel.emp_id)])"),
            "mixed recursive must extend path_edges with the edge tuple; got:\n{sql}"
        );
        // Cycle check is EDGE-unique on path_edges, NOT node-unique on the endpoint.
        assert!(
            sql.contains("NOT has(vp.path_edges, tuple(rel.mgr_id, rel.emp_id))"),
            "mixed cycle check must test edge-tuple membership; got:\n{sql}"
        );
        assert!(
            !sql.contains("NOT has(vp.path_nodes,"),
            "mixed range VLP must no longer enforce node-uniqueness; got:\n{sql}"
        );
    }

    #[test]
    fn test_fixed_length_spec() {
        let spec = VariableLengthSpec::fixed(2);
        assert_eq!(spec.effective_min_hops(), 2);
        assert_eq!(spec.max_hops, Some(2));
        assert!(!spec.is_single_hop());
    }

    #[test]
    fn test_polymorphic_edge_filter() {
        // Test single relationship type with polymorphic edge
        let schema = create_test_schema();
        let spec = VariableLengthSpec::range(1, 3);
        let generator = VariableLengthCteGenerator::new_with_polymorphic(
            &schema, // Add schema parameter
            spec,
            "users",                              // start table
            "user_id",                            // start id column
            "interactions",                       // relationship table (polymorphic)
            "from_id",                            // from column
            "to_id",                              // to column
            "users",                              // end table
            "user_id",                            // end id column
            "u1",                                 // start alias
            "u2",                                 // end alias
            "r",                                  // relationship_cypher_alias (missing parameter)
            vec![],                               // no properties for test
            None,                                 // no shortest path mode
            None,                                 // no start node filters
            None,                                 // no end node filters
            None,                                 // no relationship filters (missing parameter)
            None,                                 // no path variable
            Some(vec!["FOLLOWS".to_string()]),    // relationship type
            None,                                 // no edge_id
            Some("interaction_type".to_string()), // type_column
            None,                                 // no from_label_column
            None,                                 // no to_label_column
            Some("User".to_string()),             // from_node_label
            Some("User".to_string()),             // to_node_label
        );

        let sql = generator.generate_recursive_sql();
        println!("Polymorphic edge SQL:\n{}", sql);

        // Should contain the polymorphic type filter
        assert!(
            sql.contains("interaction_type = 'FOLLOWS'"),
            "Expected polymorphic filter in base case. SQL: {}",
            sql
        );
    }

    #[test]
    fn test_polymorphic_edge_filter_multiple_types() {
        // Test multiple relationship types with polymorphic edge
        let schema = create_test_schema();
        let spec = VariableLengthSpec::range(1, 3);
        let generator = VariableLengthCteGenerator::new_with_polymorphic(
            &schema, // Add schema parameter
            spec,
            "users",                                                // start table
            "user_id",                                              // start id column
            "interactions", // relationship table (polymorphic)
            "from_id",      // from column
            "to_id",        // to column
            "users",        // end table
            "user_id",      // end id column
            "u1",           // start alias
            "u2",           // end alias
            "r",            // relationship_cypher_alias (missing parameter)
            vec![],         // no properties for test
            None,           // no shortest path mode
            None,           // no start node filters
            None,           // no end node filters
            None,           // no relationship filters (missing parameter)
            None,           // no path variable
            Some(vec!["FOLLOWS".to_string(), "LIKES".to_string()]), // multiple types
            None,           // no edge_id
            Some("interaction_type".to_string()), // type_column
            None,           // no from_label_column
            None,           // no to_label_column
            Some("User".to_string()), // from_node_label
            Some("User".to_string()), // to_node_label
        );

        let sql = generator.generate_recursive_sql();
        println!("Polymorphic edge multiple types SQL:\n{}", sql);

        // Should contain the polymorphic type filter with IN clause
        assert!(
            sql.contains("interaction_type IN ('FOLLOWS', 'LIKES')"),
            "Expected polymorphic IN filter in base case. SQL: {}",
            sql
        );
    }

    /// #689: a directed cross-type VLP over a from-side-polymorphic edge
    /// (`member_type ∈ {User, Group}`, target fixed = Group) must recurse
    /// Group→Group: the recursive arm joins the END table (`ds_groups`) with the
    /// END discriminator (`member_type = 'Group'`), NOT the START table
    /// (`ds_users`) with the base discriminator (`member_type = 'User'`). The old
    /// `recursive_end_table` heuristic joined `ds_users AS end_node` on
    /// `rel.group_id = end_node.group_id` (a column ds_users lacks → CH 500), and
    /// reused the base `member_type = 'User'` filter (silent undercount).
    #[test]
    fn test_from_side_polymorphic_vlp_recursive_arm_689() {
        let schema = create_test_schema();
        let spec = VariableLengthSpec::range(1, 5);
        let generator = VariableLengthCteGenerator::new_with_polymorphic(
            &schema,
            spec,
            "ds_users",                          // start table
            "user_id",                           // start id column
            "ds_memberships",                    // relationship table (from-side polymorphic)
            "member_id",                         // from column
            "group_id",                          // to column
            "ds_groups",                         // end table (DIFFERENT from start)
            "group_id",                          // end id column
            "u",                                 // start alias
            "g",                                 // end alias
            "r",                                 // relationship_cypher_alias
            vec![],                              // no properties
            None,                                // no shortest path mode
            None,                                // no start node filters
            None,                                // no end node filters
            None,                                // no relationship filters
            None,                                // no path variable
            Some(vec!["MEMBER_OF".to_string()]), // relationship type
            None,                                // no edge_id
            None,                                // no rel-type discriminator col
            Some("member_type".to_string()),     // from-side label col (polymorphic source)
            None,                                // NO to-side label col (target fixed = Group)
            Some("User".to_string()),            // from_node_label (query start)
            Some("Group".to_string()),           // to_node_label (recursing/target type)
        );

        let sql = generator.generate_recursive_sql();
        println!("From-side-polymorphic VLP SQL:\n{}", sql);

        // Split base arm from recursive arm at the UNION ALL barrier so we can
        // assert per-arm.
        let (base_arm, recursive_arm) = sql
            .split_once("UNION ALL")
            .expect("recursive VLP must have a UNION ALL");

        // Defect 1: recursive arm joins the END table (ds_groups), never ds_users.
        assert!(
            recursive_arm.contains("ds_groups AS end_node"),
            "recursive arm must join ds_groups (end table). SQL:\n{}",
            sql
        );
        assert!(
            !recursive_arm.contains("ds_users AS end_node"),
            "recursive arm must NOT join ds_users as end_node (the #689 500). SQL:\n{}",
            sql
        );

        // Defect 2: recursive arm discriminates on the END label; base on START.
        assert!(
            base_arm.contains("member_type = 'User'"),
            "base arm must keep member_type = 'User' (first hop User→Group). SQL:\n{}",
            sql
        );
        assert!(
            recursive_arm.contains("member_type = 'Group'"),
            "recursive arm must use member_type = 'Group' (Group→Group hops). SQL:\n{}",
            sql
        );
        assert!(
            !recursive_arm.contains("member_type = 'User'"),
            "recursive arm must NOT keep the base member_type = 'User' filter. SQL:\n{}",
            sql
        );
    }

    #[test]
    fn test_weighted_shortest_path_base_case() {
        let schema = create_test_schema();
        let spec = VariableLengthSpec::unbounded();
        let mut generator = VariableLengthCteGenerator::new(
            &schema,
            spec,
            "users",       // start table
            "user_id",     // start id column
            "follows",     // relationship table
            "follower_id", // from column
            "followed_id", // to column
            "users",       // end table
            "user_id",     // end id column
            "u1",          // start alias
            "u2",          // end alias
            vec![],        // no properties
            Some(ShortestPathMode::Shortest),
            None, // no start node filters
            None, // no end node filters
            None, // no path variable
            None, // no relationship types
            None, // no edge_id
        );

        generator.set_weight_cte(WeightCteConfig {
            cte_name: "bidi_weight".to_string(),
            source_column: "source".to_string(),
            target_column: "target".to_string(),
            weight_column: "weight".to_string(),
        });

        let sql = generator.generate_recursive_sql();
        println!("Weighted shortest path SQL:\n{}", sql);

        // Base case should SELECT from weight CTE, not edge table
        assert!(
            sql.contains("FROM bidi_weight ew"),
            "Expected base case FROM weight CTE. SQL: {}",
            sql
        );
        // Should have total_weight column
        assert!(
            sql.contains("total_weight"),
            "Expected total_weight column. SQL: {}",
            sql
        );
        // Recursive case should ORDER BY total_weight (not hop_count)
        assert!(
            sql.contains("ORDER BY total_weight ASC"),
            "Expected ORDER BY total_weight ASC. SQL: {}",
            sql
        );
    }

    #[test]
    fn test_weighted_recursive_case_joins_weight_cte() {
        let schema = create_test_schema();
        let spec = VariableLengthSpec::range(1, 5);
        let mut generator = VariableLengthCteGenerator::new(
            &schema,
            spec,
            "users",
            "user_id",
            "follows",
            "follower_id",
            "followed_id",
            "users",
            "user_id",
            "u1",
            "u2",
            vec![],
            Some(ShortestPathMode::Shortest),
            None,
            None,
            None,
            None,
            None,
        );

        generator.set_weight_cte(WeightCteConfig {
            cte_name: "bidi_w".to_string(),
            source_column: "src".to_string(),
            target_column: "tgt".to_string(),
            weight_column: "w".to_string(),
        });

        let sql = generator.generate_recursive_sql();
        println!("Weighted recursive SQL:\n{}", sql);

        // Recursive case should JOIN weight CTE
        assert!(
            sql.contains("JOIN bidi_w ew ON ew.src = vp.end_id"),
            "Expected recursive JOIN on weight CTE. SQL: {}",
            sql
        );
        // Should accumulate weight
        assert!(
            sql.contains("vp.total_weight + ew.w AS total_weight"),
            "Expected weight accumulation. SQL: {}",
            sql
        );
        // Should respect max hops
        assert!(
            sql.contains("hop_count < 5"),
            "Expected max hops guard. SQL: {}",
            sql
        );
    }

    #[test]
    fn test_all_shortest_no_end_filter_uses_order_column() {
        let schema = create_test_schema();
        let spec = VariableLengthSpec::unbounded();
        let mut generator = VariableLengthCteGenerator::new(
            &schema,
            spec,
            "users",
            "user_id",
            "follows",
            "follower_id",
            "followed_id",
            "users",
            "user_id",
            "u1",
            "u2",
            vec![],
            Some(ShortestPathMode::AllShortest),
            None,
            None,
            None,
            None,
            None,
        );

        generator.set_weight_cte(WeightCteConfig {
            cte_name: "bidi_w".to_string(),
            source_column: "source".to_string(),
            target_column: "target".to_string(),
            weight_column: "weight".to_string(),
        });

        let sql = generator.generate_recursive_sql();
        println!("AllShortest weighted SQL:\n{}", sql);

        // AllShortest with no end filter should use total_weight (not hop_count) for weighted mode
        assert!(
            sql.contains("WHERE total_weight = (SELECT MIN(total_weight)"),
            "Expected AllShortest to filter by total_weight, not hop_count. SQL: {}",
            sql
        );
    }
}

/// Generates optimized chained JOIN SQL for exact hop count queries
/// This is much more efficient than recursive CTEs for fixed-length paths
pub struct ChainedJoinGenerator {
    pub hop_count: u32,
    pub start_node_table: String,
    pub start_node_id_column: String,
    pub relationship_table: String,
    pub relationship_from_column: String,
    pub relationship_to_column: String,
    pub end_node_table: String,
    pub end_node_id_column: String,
    pub start_cypher_alias: String,
    pub end_cypher_alias: String,
    pub properties: Vec<NodeProperty>,
    pub database: Option<String>,
}

impl ChainedJoinGenerator {
    #[allow(clippy::too_many_arguments)] // builds inline N-hop JOIN for fixed-length VLPs; each arg is a distinct table/column/alias from the matched pattern
    pub fn new(
        hop_count: u32,
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
    ) -> Self {
        let database = std::env::var("CLICKHOUSE_DATABASE").ok();

        Self {
            hop_count,
            start_node_table: start_table.to_string(),
            start_node_id_column: start_id_col.to_string(),
            relationship_table: relationship_table.to_string(),
            relationship_from_column: rel_from_col.to_string(),
            relationship_to_column: rel_to_col.to_string(),
            end_node_table: end_table.to_string(),
            end_node_id_column: end_id_col.to_string(),
            start_cypher_alias: start_alias.to_string(),
            end_cypher_alias: end_alias.to_string(),
            properties,
            database,
        }
    }

    /// Generate a CTE containing the chained JOIN query
    /// Even though it's not recursive, we wrap it in a CTE for consistency
    pub fn generate_cte(&self) -> Cte {
        let cte_name = format!(
            "chain_{}",
            crate::query_planner::logical_plan::generate_cte_id()
        );
        let cte_sql = self.generate_query();

        // Wrap the query body with CTE name, like recursive CTE does
        let wrapped_sql = format!("{} AS (\n{}\n)", cte_name, cte_sql);

        Cte::new(
            cte_name,
            crate::render_plan::CteContent::RawSql(wrapped_sql),
            false, // Chained JOINs don't need recursion
        )
    }

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

    /// Generate a SELECT query with chained JOINs for exact hop count
    pub fn generate_query(&self) -> String {
        if self.hop_count == 0 {
            // Special case: 0 hops means start node == end node
            return self.generate_zero_hop_query();
        }

        let mut sql = String::new();

        // Build SELECT clause with properties
        let mut select_items = vec![
            format!("s.{} as start_id", self.start_node_id_column),
            format!("e.{} as end_id", self.end_node_id_column),
        ];

        // Add start node properties
        for prop in &self.properties {
            if prop.cypher_alias == self.start_cypher_alias {
                select_items.push(format!("s.{} as start_{}", prop.column_name, prop.alias));
            }
        }

        // Add end node properties
        for prop in &self.properties {
            if prop.cypher_alias == self.end_cypher_alias {
                select_items.push(format!("e.{} as end_{}", prop.column_name, prop.alias));
            }
        }

        sql.push_str("SELECT \n    ");
        sql.push_str(&select_items.join(",\n    "));
        sql.push_str("\nFROM ");
        sql.push_str(&self.format_table_name(&self.start_node_table));
        sql.push_str(" s\n");

        // Parse comma-separated column string to Identifier
        let parse_id_cols = Identifier::from_comma_separated;

        // Parse column identifiers for composite ID support
        let start_id_identifier = parse_id_cols(&self.start_node_id_column);
        let end_id_identifier = parse_id_cols(&self.end_node_id_column);
        let rel_from_identifier = parse_id_cols(&self.relationship_from_column);
        let rel_to_identifier = parse_id_cols(&self.relationship_to_column);

        // Generate chain of JOINs
        for hop in 1..=self.hop_count {
            let rel_alias = format!("r{}", hop);
            let node_alias = if hop == self.hop_count {
                "e".to_string()
            } else {
                format!("m{}", hop)
            };

            let prev_node = if hop == 1 {
                "s".to_string()
            } else {
                format!("m{}", hop - 1)
            };

            // Determine which ID columns to use for this hop
            let (node_id_identifier, rel_to_identifier) = if hop == self.hop_count {
                (&end_id_identifier, &rel_to_identifier)
            } else {
                // Intermediate nodes use start node's ID columns
                (&start_id_identifier, &rel_to_identifier)
            };

            // Add relationship JOIN - use to_sql_equality for composite ID support
            let join_on_rel =
                start_id_identifier.to_sql_equality(&prev_node, &rel_from_identifier, &rel_alias);
            sql.push_str(&format!(
                "JOIN {} {} ON {}\n",
                self.format_table_name(&self.relationship_table),
                rel_alias,
                join_on_rel
            ));

            // Add node JOIN - use to_sql_equality for composite ID support
            let node_table = if hop == self.hop_count {
                &self.end_node_table
            } else {
                &self.start_node_table // Intermediate nodes are same type as start
            };

            let join_on_node =
                rel_to_identifier.to_sql_equality(&rel_alias, node_id_identifier, &node_alias);
            sql.push_str(&format!(
                "JOIN {} {} ON {}\n",
                self.format_table_name(node_table),
                node_alias,
                join_on_node
            ));
        }

        // Add WHERE clause for cycle prevention
        if self.hop_count > 1 {
            sql.push_str("WHERE ");
            let mut conditions = vec![];

            // Prevent start == end
            conditions.push(format!(
                "s.{} != e.{}",
                self.start_node_id_column, self.end_node_id_column
            ));

            // Prevent intermediate nodes from being start or end
            for hop in 1..self.hop_count {
                let mid_alias = format!("m{}", hop);
                conditions.push(format!(
                    "s.{} != {}.{}",
                    self.start_node_id_column, mid_alias, self.start_node_id_column
                ));
                conditions.push(format!(
                    "e.{} != {}.{}",
                    self.end_node_id_column, mid_alias, self.start_node_id_column
                ));
            }

            // Prevent intermediate nodes from repeating
            if self.hop_count > 2 {
                for i in 1..self.hop_count {
                    for j in (i + 1)..self.hop_count {
                        conditions.push(format!(
                            "m{}.{} != m{}.{}",
                            i, self.start_node_id_column, j, self.start_node_id_column
                        ));
                    }
                }
            }

            sql.push_str(&conditions.join("\n  AND "));
        }

        sql
    }

    fn generate_zero_hop_query(&self) -> String {
        let mut select_items = vec![
            format!("s.{} as start_id", self.start_node_id_column),
            format!("s.{} as end_id", self.start_node_id_column),
        ];

        // Add properties (both start and end reference same node)
        for prop in &self.properties {
            if prop.cypher_alias == self.start_cypher_alias {
                select_items.push(format!("s.{} as start_{}", prop.column_name, prop.alias));
            }
            if prop.cypher_alias == self.end_cypher_alias {
                select_items.push(format!("s.{} as end_{}", prop.column_name, prop.alias));
            }
        }

        format!(
            "SELECT \n    {}\nFROM {} s",
            select_items.join(",\n    "),
            self.format_table_name(&self.start_node_table)
        )
    }
}

#[cfg(test)]
mod chained_join_tests {
    use super::*;

    #[test]
    fn test_chained_join_2_hops() {
        let generator = ChainedJoinGenerator::new(
            2,
            "users",
            "user_id",
            "friendships",
            "user1_id",
            "user2_id",
            "users",
            "user_id",
            "u1",
            "u2",
            vec![],
        );

        let sql = generator.generate_query();
        println!("2-hop chained JOIN:\n{}", sql);

        assert!(sql.contains("FROM") && sql.contains("users"));
        assert!(sql.contains("JOIN") && sql.contains("friendships"));
        assert!(sql.contains("r1") && sql.contains("r2")); // 2 relationship aliases
        assert!(sql.contains("m1")); // 1 intermediate node
        assert!(sql.contains("WHERE")); // Cycle prevention
    }

    #[test]
    fn test_chained_join_3_hops() {
        let generator = ChainedJoinGenerator::new(
            3,
            "users",
            "user_id",
            "friendships",
            "user1_id",
            "user2_id",
            "users",
            "user_id",
            "u1",
            "u2",
            vec![],
        );

        let sql = generator.generate_query();
        println!("3-hop chained JOIN:\n{}", sql);

        assert!(sql.contains("r1") && sql.contains("r2") && sql.contains("r3"));
        assert!(sql.contains("m1") && sql.contains("m2")); // 2 intermediate nodes
    }

    #[test]
    fn test_chained_join_with_properties() {
        let properties = vec![
            NodeProperty {
                cypher_alias: "u1".to_string(),
                column_name: "full_name".to_string(),
                alias: "name".to_string(),
            },
            NodeProperty {
                cypher_alias: "u2".to_string(),
                column_name: "email_address".to_string(),
                alias: "email".to_string(),
            },
        ];

        let generator = ChainedJoinGenerator::new(
            2,
            "users",
            "user_id",
            "friendships",
            "user1_id",
            "user2_id",
            "users",
            "user_id",
            "u1",
            "u2",
            properties,
        );

        let sql = generator.generate_query();
        println!("2-hop with properties:\n{}", sql);

        assert!(sql.contains("s.full_name as start_name"));
        assert!(sql.contains("e.email_address as end_email"));
    }
}
