pub mod errors;

use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
};

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    query_planner::{
        logical_expr::{LogicalExpr, Property},
        logical_plan::ProjectionItem,
        plan_ctx::errors::PlanCtxError,
    },
};

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
}

impl TableCtx {
    pub fn is_relation(&self) -> bool {
        self.is_rel
    }

    pub fn is_explicit_alias(&self) -> bool {
        self.explicit_alias
    }

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
        }
    }

    /// Create a TableCtx that references a CTE instead of a base table.
    /// Used when an alias was exported from a WITH clause.
    pub fn new_with_cte_reference(alias: String, cte_name: String) -> Self {
        TableCtx {
            alias,
            labels: None, // Label will be resolved from CTE schema later
            properties: vec![],
            filter_predicates: vec![],
            projection_items: vec![],
            is_rel: false,
            explicit_alias: true,
            cte_reference: Some(cte_name),
        }
    }

    /// Check if this alias references a CTE
    pub fn is_cte_reference(&self) -> bool {
        self.cte_reference.is_some()
    }

    /// Get the CTE name if this is a CTE reference
    pub fn get_cte_name(&self) -> Option<&String> {
        self.cte_reference.as_ref()
    }

    /// Set the CTE reference for this alias
    pub fn set_cte_reference(&mut self, cte_ref: Option<String>) {
        self.cte_reference = cte_ref;
    }

    pub fn get_label_str(&self) -> Result<String, PlanCtxError> {
        self.labels
            .as_ref()
            .and_then(|v| v.first())
            .cloned()
            .ok_or(PlanCtxError::Label {
                alias: self.alias.clone(),
            })
    }

    pub fn get_labels(&self) -> Option<&Vec<String>> {
        self.labels.as_ref()
    }

    pub fn get_label_opt(&self) -> Option<String> {
        self.labels.as_ref().and_then(|v| v.first()).cloned()
    }

    /// Check if this TableCtx represents a path variable (not a node or relationship).
    /// Path variables have no label and are not relationships.
    pub fn is_path_variable(&self) -> bool {
        !self.is_rel && self.labels.as_ref().map_or(true, |v| v.is_empty())
    }

    pub fn set_labels(&mut self, labels_opt: Option<Vec<String>>) {
        self.labels = labels_opt;
    }

    pub fn get_projections(&self) -> &Vec<ProjectionItem> {
        &self.projection_items
    }

    pub fn set_projections(&mut self, proj_items: Vec<ProjectionItem>) {
        self.projection_items = proj_items;
    }

    pub fn insert_projection(&mut self, proj_item: ProjectionItem) {
        if !self.projection_items.contains(&proj_item) {
            self.projection_items.push(proj_item);
        }
    }

    pub fn append_projection(&mut self, proj_items: &mut Vec<ProjectionItem>) {
        self.projection_items.append(proj_items);
        // for proj_item in proj_items {
        //     if !self.projection_items.contains(&proj_item) {
        //         self.projection_items.push(proj_item);
        //     }
        // }
    }

    pub fn get_filters(&self) -> &Vec<LogicalExpr> {
        &self.filter_predicates
    }

    pub fn insert_filter(&mut self, filter_pred: LogicalExpr) {
        if !self.filter_predicates.contains(&filter_pred) {
            self.filter_predicates.push(filter_pred);
        }
    }

    pub fn append_filters(&mut self, filter_preds: &mut Vec<LogicalExpr>) {
        self.filter_predicates.append(filter_preds);
        // for filter_pred in filter_preds {
        //     if !self.filter_predicates.contains(&filter_pred) {
        //         self.filter_predicates.push(filter_pred);
        //     }
        // }
    }

    pub fn append_properties(&mut self, mut props: Vec<Property>) {
        self.properties.append(&mut props);
    }

    pub fn get_and_clear_properties(&mut self) -> Vec<Property> {
        std::mem::take(&mut self.properties)
        // self.properties
    }

    /// Clear the filters after they have been applied to a GraphRel
    /// This prevents the same filters from being applied multiple times
    /// in multi-hop patterns
    pub fn clear_filters(&mut self) {
        self.filter_predicates.clear();
    }
}

#[derive(Debug, Clone)]
pub struct PlanCtx {
    alias_table_ctx_map: HashMap<String, TableCtx>,
    /// Track which table aliases came from OPTIONAL MATCH for LEFT JOIN generation
    optional_aliases: HashSet<String>,
    /// Track projection aliases from WITH/aggregation clauses (alias_name -> original_expression)
    /// Used to identify when filters reference projection results (HAVING clause)
    projection_aliases: HashMap<String, LogicalExpr>,
    /// Flag to indicate we're currently processing an OPTIONAL MATCH clause
    /// All new aliases created during this mode should be marked as optional
    in_optional_match_mode: bool,
    /// Graph schema for this query (enables multi-schema support)
    schema: Arc<GraphSchema>,
    /// Tenant ID for multi-tenant deployments (passed to parameterized views)
    tenant_id: Option<String>,
    /// View parameter values for parameterized views (e.g., {"region": "US", "tier": "premium"})
    /// These are passed to table functions: table(region = 'US', tier = 'premium')
    view_parameter_values: Option<HashMap<String, String>>,
    /// Track denormalized node-to-edge mappings: node_alias -> (edge_alias, is_from_node, node_label, rel_type)
    /// Used for multi-hop denormalized patterns to create edge-to-edge JOINs
    denormalized_node_edges: HashMap<String, (String, bool, String, String)>,
    /// Parent scope for WITH clause nesting (enables proper variable scoping)
    /// Lookup chain: current scope → parent scope → ... → root scope (global schema)
    /// Example: MATCH (a) WITH a MATCH (b) → second MATCH has parent scope containing 'a'
    parent_scope: Option<Box<PlanCtx>>,
    /// Flag indicating this scope was created by WITH clause (acts as scope barrier)
    /// When true, variable lookup stops here and doesn't search parent scope
    /// Example: MATCH (a)-[]->(b) WITH a MATCH (a)-[]->(b)  // second b is different!
    is_with_scope: bool,
}

impl PlanCtx {
    pub fn insert_table_ctx(&mut self, alias: String, table_ctx: TableCtx) {
        crate::debug_print!(
            "DEBUG PlanCtx::insert_table_ctx: alias='{}', in_optional_match_mode={}",
            alias,
            self.in_optional_match_mode
        );
        self.alias_table_ctx_map.insert(alias.clone(), table_ctx);

        // Auto-mark as optional if we're in OPTIONAL MATCH mode
        if self.in_optional_match_mode {
            crate::debug_println!("DEBUG PlanCtx: Auto-marking '{}' as optional", alias);
            self.optional_aliases.insert(alias);
        }
    }

    /// Mark a table alias as coming from an OPTIONAL MATCH clause
    pub fn mark_as_optional(&mut self, alias: String) {
        self.optional_aliases.insert(alias);
    }

    /// Set the OPTIONAL MATCH processing mode
    /// When true, all new aliases will be automatically marked as optional
    pub fn set_optional_match_mode(&mut self, enabled: bool) {
        self.in_optional_match_mode = enabled;
    }

    /// Check if we're currently processing an OPTIONAL MATCH clause
    pub fn is_optional_match_mode(&self) -> bool {
        self.in_optional_match_mode
    }

    /// Register a projection alias (e.g., `follows` from `COUNT(b) as follows`)
    pub fn register_projection_alias(&mut self, alias: String, expression: LogicalExpr) {
        self.projection_aliases.insert(alias, expression);
    }

    /// Check if an alias is a projection alias
    pub fn is_projection_alias(&self, alias: &str) -> bool {
        self.projection_aliases.contains_key(alias)
    }

    /// Get the original expression for a projection alias
    pub fn get_projection_alias_expr(&self, alias: &str) -> Option<&LogicalExpr> {
        self.projection_aliases.get(alias)
    }

    /// Check if a table alias came from an OPTIONAL MATCH clause
    pub fn is_optional(&self, alias: &str) -> bool {
        self.optional_aliases.contains(alias)
    }

    /// Get a reference to the set of optional aliases
    pub fn get_optional_aliases(&self) -> &HashSet<String> {
        &self.optional_aliases
    }

    pub fn get_alias_table_ctx_map(&self) -> &HashMap<String, TableCtx> {
        &self.alias_table_ctx_map
    }

    pub fn get_mut_alias_table_ctx_map(&mut self) -> &mut HashMap<String, TableCtx> {
        &mut self.alias_table_ctx_map
    }

    /// Iterate over all table contexts (alias, TableCtx pairs)
    pub fn iter_table_contexts(&self) -> impl Iterator<Item = (&String, &TableCtx)> {
        self.alias_table_ctx_map.iter()
    }

    /// Get the graph schema for this query
    pub fn schema(&self) -> &GraphSchema {
        &self.schema
    }

    pub fn get_table_ctx(&self, alias: &str) -> Result<&TableCtx, PlanCtxError> {
        // Try current scope first
        if let Some(ctx) = self.alias_table_ctx_map.get(alias) {
            return Ok(ctx);
        }
        
        // WITH scope acts as a barrier - don't look beyond it
        // This implements WITH's shielding semantics: only exported variables are visible
        if self.is_with_scope {
            return Err(PlanCtxError::TableCtx {
                alias: alias.to_string(),
            });
        }
        
        // Search parent scope recursively (scope chain)
        if let Some(parent) = &self.parent_scope {
            return parent.get_table_ctx(alias);
        }
        
        // Not found in any scope
        Err(PlanCtxError::TableCtx {
            alias: alias.to_string(),
        })
    }

    pub fn get_table_ctx_from_alias_opt(
        &self,
        alias: &Option<String>,
    ) -> Result<&TableCtx, PlanCtxError> {
        let alias = alias.clone().ok_or(PlanCtxError::TableCtx {
            alias: "".to_string(),
        })?;
        self.alias_table_ctx_map
            .get(&alias)
            .ok_or(PlanCtxError::TableCtx {
                alias: alias.clone(),
            })
    }

    pub fn get_node_table_ctx(&self, node_alias: &str) -> Result<&TableCtx, PlanCtxError> {
        self.alias_table_ctx_map
            .get(node_alias)
            .ok_or(PlanCtxError::NodeTableCtx {
                alias: node_alias.to_string(),
            })
    }

    pub fn get_rel_table_ctx(&self, rel_alias: &str) -> Result<&TableCtx, PlanCtxError> {
        self.alias_table_ctx_map
            .get(rel_alias)
            .ok_or(PlanCtxError::RelTableCtx {
                alias: rel_alias.to_string(),
            })
    }

    /// Get mutable reference to table context in CURRENT SCOPE ONLY.
    /// 
    /// NOTE: This does NOT search parent scopes. Mutable access is restricted to
    /// the current scope to maintain proper scope isolation. If you need to mutate
    /// a variable from a parent scope (e.g., from WITH), it should already be in
    /// the current scope (copied during WITH processing).
    pub fn get_mut_table_ctx(&mut self, alias: &str) -> Result<&mut TableCtx, PlanCtxError> {
        self.alias_table_ctx_map
            .get_mut(alias)
            .ok_or(PlanCtxError::TableCtx {
                alias: alias.to_string(),
            })
    }

    // pub fn get_mut_table_ctx_from_alias_opt(
    //     &mut self,
    //     alias: &Option<String>,
    // ) -> Result<&mut TableCtx, PlanCtxError> {
    //     let alias = alias.clone().ok_or(PlanCtxError::TableCtx {
    //         alias: "".to_string(),
    //     })?;
    //     self.alias_table_ctx_map
    //         .get_mut(&alias)
    //         .ok_or(PlanCtxError::TableCtx {
    //             alias: alias.clone(),
    //         })
    // }

    /// Get optional mutable reference to table context in CURRENT SCOPE ONLY.
    /// 
    /// NOTE: This does NOT search parent scopes. See get_mut_table_ctx() for rationale.
    pub fn get_mut_table_ctx_opt(&mut self, alias: &str) -> Option<&mut TableCtx> {
        self.alias_table_ctx_map.get_mut(alias)
    }

    pub fn get_mut_table_ctx_opt_from_alias_opt(
        &mut self,
        alias: &Option<String>,
    ) -> Result<Option<&mut TableCtx>, PlanCtxError> {
        let alias = alias.clone().ok_or(PlanCtxError::TableCtx {
            alias: "".to_string(),
        })?;
        Ok(self.alias_table_ctx_map.get_mut(&alias))
    }

    /// Register a denormalized node alias with its associated edge
    /// Used for multi-hop denormalized patterns to create edge-to-edge JOINs
    pub fn register_denormalized_alias(
        &mut self,
        alias: String,
        rel_alias: String,
        is_from_node: bool,
        node_label: String,
        rel_type: String,
    ) {
        self.denormalized_node_edges
            .insert(alias, (rel_alias, is_from_node, node_label, rel_type));
    }

    /// Get denormalized alias info: returns (edge_alias, is_from_node, node_label, rel_type) if node is denormalized
    pub fn get_denormalized_alias_info(
        &self,
        node_alias: &str,
    ) -> Option<(String, bool, String, String)> {
        self.denormalized_node_edges.get(node_alias).cloned()
    }

    /// Get an iterator over all aliases and their TableCtx in the CURRENT scope only.
    /// Used for copying child scope state back to parent scope.
    pub fn iter_aliases(&self) -> impl Iterator<Item = (&String, &TableCtx)> {
        self.alias_table_ctx_map.iter()
    }
}

impl PlanCtx {
    /// Create a new PlanCtx with the given schema
    pub fn new(schema: Arc<GraphSchema>) -> Self {
        PlanCtx {
            alias_table_ctx_map: HashMap::new(),
            optional_aliases: HashSet::new(),
            projection_aliases: HashMap::new(),
            in_optional_match_mode: false,
            schema,
            tenant_id: None,
            view_parameter_values: None,
            denormalized_node_edges: HashMap::new(),
            parent_scope: None,
            is_with_scope: false,
        }
    }

    /// Create a new PlanCtx with the given schema and tenant ID
    pub fn with_tenant(schema: Arc<GraphSchema>, tenant_id: Option<String>) -> Self {
        PlanCtx {
            alias_table_ctx_map: HashMap::new(),
            optional_aliases: HashSet::new(),
            projection_aliases: HashMap::new(),
            in_optional_match_mode: false,
            schema,
            tenant_id,
            view_parameter_values: None,
            denormalized_node_edges: HashMap::new(),
            parent_scope: None,
            is_with_scope: false,
        }
    }

    /// Create a new PlanCtx with schema, tenant_id, and view_parameters
    pub fn with_parameters(
        schema: Arc<GraphSchema>,
        tenant_id: Option<String>,
        view_parameter_values: Option<HashMap<String, String>>,
    ) -> Self {
        PlanCtx {
            alias_table_ctx_map: HashMap::new(),
            optional_aliases: HashSet::new(),
            projection_aliases: HashMap::new(),
            in_optional_match_mode: false,
            schema,
            tenant_id,
            view_parameter_values,
            denormalized_node_edges: HashMap::new(),
            parent_scope: None,
            is_with_scope: false,
        }
    }

    /// Create a child scope with parent context (for WITH clause scoping)
    /// The child scope inherits schema, tenant_id, and view_parameters from parent
    /// but has its own alias_table_ctx_map for local variables
    ///
    /// **CRITICAL**: Set `is_with_scope=true` when creating scope for WITH clause!
    /// This makes the scope act as a barrier preventing lookup of parent variables.
    ///
    /// Example: MATCH (a)-[]->(b) WITH a MATCH (a)-[]->(b)
    ///   - Scope1 (before WITH): {a: User, b: User}
    ///   - Scope2 (WITH, is_with_scope=true): {a: User} - shields b from Scope1!
    ///   - Second MATCH creates NEW b in Scope2, different from Scope1's b
    pub fn with_parent_scope(parent: &PlanCtx, is_with_scope: bool) -> Self {
        PlanCtx {
            alias_table_ctx_map: HashMap::new(),
            optional_aliases: HashSet::new(),
            projection_aliases: HashMap::new(),
            in_optional_match_mode: false,
            schema: parent.schema.clone(),
            tenant_id: parent.tenant_id.clone(),
            view_parameter_values: parent.view_parameter_values.clone(),
            denormalized_node_edges: HashMap::new(),
            parent_scope: Some(Box::new(parent.clone())),
            is_with_scope,
        }
    }

    /// Create an empty PlanCtx with an empty schema (for tests only)
    pub fn default() -> Self {
        use crate::graph_catalog::graph_schema::GraphSchema;
        let empty_schema =
            GraphSchema::build(1, "test".to_string(), HashMap::new(), HashMap::new());
        PlanCtx {
            alias_table_ctx_map: HashMap::new(),
            optional_aliases: HashSet::new(),
            projection_aliases: HashMap::new(),
            in_optional_match_mode: false,
            schema: Arc::new(empty_schema),
            tenant_id: None,
            view_parameter_values: None,
            denormalized_node_edges: HashMap::new(),
            parent_scope: None,
            is_with_scope: false,
        }
    }

    /// Get the tenant ID for this query context
    pub fn tenant_id(&self) -> Option<&String> {
        self.tenant_id.as_ref()
    }

    /// Get the view parameter values for parameterized views
    pub fn view_parameter_values(&self) -> Option<&HashMap<String, String>> {
        self.view_parameter_values.as_ref()
    }

    /// Merge another PlanCtx into this one
    /// Used for UNION queries where each branch has its own context
    /// Note: This is a simple merge that may have alias conflicts if not careful
    pub fn merge(&mut self, other: PlanCtx) {
        // Merge alias-to-table mappings
        for (alias, table_ctx) in other.alias_table_ctx_map {
            // Only insert if not already present to avoid conflicts
            if !self.alias_table_ctx_map.contains_key(&alias) {
                self.alias_table_ctx_map.insert(alias, table_ctx);
            }
        }

        // Merge optional aliases
        for alias in other.optional_aliases {
            self.optional_aliases.insert(alias);
        }

        // Merge projection aliases
        for (alias, expr) in other.projection_aliases {
            if !self.projection_aliases.contains_key(&alias) {
                self.projection_aliases.insert(alias, expr);
            }
        }

        // Merge denormalized node edges
        for (alias, info) in other.denormalized_node_edges {
            if !self.denormalized_node_edges.contains_key(&alias) {
                self.denormalized_node_edges.insert(alias, info);
            }
        }
    }
}

impl fmt::Display for PlanCtx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "\n---- PlanCtx Starts Here ----")?;
        for (alias, table_ctx) in &self.alias_table_ctx_map {
            writeln!(f, "\n [{}]:", alias)?;
            table_ctx.fmt_with_indent(f, 2)?;
        }
        writeln!(f, "\n---- PlanCtx Ends Here ----")?;
        Ok(())
    }
}

impl TableCtx {
    fn fmt_with_indent(&self, f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result {
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
