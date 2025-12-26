pub mod errors;

use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
};

use crate::{
    graph_catalog::graph_schema::GraphSchema,
    query_planner::{
        analyzer::property_requirements::PropertyRequirements,
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
    /// For relationships: the label of the connected from_node (source)
    /// Used to resolve polymorphic relationships (e.g., Person LIKES Message)
    from_node_label: Option<String>,
    /// For relationships: the label of the connected to_node (target)
    /// Used to resolve polymorphic relationships
    to_node_label: Option<String>,
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
            from_node_label: None,
            to_node_label: None,
        }
    }

    /// Create a TableCtx that references a CTE instead of a base table.
    /// Used when an alias was exported from a WITH clause.
    ///
    /// **NEW (Dec 2025)**: Looks up entity type from CTE registry to preserve node/relationship types.
    /// This fixes the bug where `WITH tag ... RETURN tag.name` fails because tag loses its Tag label.
    pub fn new_with_cte_reference(alias: String, cte_name: String, plan_ctx: &PlanCtx) -> Self {
        // Look up entity type information for this alias
        let (is_rel, labels) = plan_ctx
            .get_cte_entity_type(&cte_name, &alias)
            .map(|(r, l)| (*r, l.clone()))
            .unwrap_or((false, None)); // Default: not a rel, no labels

        log::info!(
            "🔧 Creating TableCtx for CTE reference '{}' → '{}': is_rel={}, labels={:?}",
            alias,
            cte_name,
            is_rel,
            labels
        );

        TableCtx {
            alias,
            labels, // ✅ NOW PRESERVED FROM CTE!
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

    /// Set the connected node labels for a relationship
    pub fn set_connected_nodes(&mut self, from: Option<String>, to: Option<String>) {
        self.from_node_label = from;
        self.to_node_label = to;
    }

    /// Get the from_node label for a relationship
    pub fn get_from_node_label(&self) -> Option<&String> {
        self.from_node_label.as_ref()
    }

    /// Get the to_node label for a relationship
    pub fn get_to_node_label(&self) -> Option<&String> {
        self.to_node_label.as_ref()
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
    /// Counter for generating unique CTE names (ensures with_a_b_cte_0, with_a_b_cte_1, etc.)
    /// Incremented each time a WITH clause is processed to prevent duplicate CTE names
    pub(crate) cte_counter: usize,
    /// Track exported columns for each CTE
    /// Map: CTE name → (graph_property → cte_column_name)
    /// Example: "with_p_cte_1" → {"firstName" → "p_firstName", "age" → "p_age"}
    /// Note: CTE column names use underscore (variablename_alias),
    /// while final SELECT uses dot notation (variablename.alias)
    cte_columns: HashMap<String, HashMap<String, String>>,
    /// Track entity types (node/relationship labels) for each CTE alias
    /// Map: CTE name → (alias → (is_rel, labels))
    /// Example: "with_tag_cte_1" → {"tag" → (false, ["Tag"])}
    /// This preserves node/relationship type information across WITH boundaries,
    /// enabling property resolution after WITH (e.g., `WITH tag ... RETURN tag.name`)
    cte_entity_types: HashMap<String, HashMap<String, (bool, Option<Vec<String>>)>>,
    /// Property requirements tracking for optimization
    /// Populated by PropertyRequirementsAnalyzer pass (root-to-leaf traversal)
    /// Consumed by property expansion in renderer to prune unnecessary columns
    /// Example: If RETURN only uses friend.firstName, don't collect friend.* (200 columns)
    property_requirements: Option<PropertyRequirements>,
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
            cte_counter: 0,
            cte_columns: HashMap::new(),
            cte_entity_types: HashMap::new(),
            property_requirements: None,
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
            cte_counter: 0,
            cte_columns: HashMap::new(),
            cte_entity_types: HashMap::new(),
            property_requirements: None,
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
            cte_counter: 0,
            cte_columns: HashMap::new(),
            cte_entity_types: HashMap::new(),
            property_requirements: None,
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
            cte_counter: 0,
            cte_columns: HashMap::new(),
            cte_entity_types: HashMap::new(),
            property_requirements: None,
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
            cte_counter: 0,
            cte_columns: HashMap::new(),
            cte_entity_types: HashMap::new(),
            property_requirements: None,
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

    /// Register columns exported by a CTE
    ///
    /// # Arguments
    /// * `cte_name` - The CTE name (e.g., "with_p_cte_1")
    /// * `items` - The projection items from WITH clause
    ///
    /// This extracts property names from ProjectionItems and their aliases,
    /// using the naming convention: variablename_propertyname (e.g., "p_firstName")
    pub fn register_cte_columns(&mut self, cte_name: &str, items: &[ProjectionItem]) {
        let mut columns = HashMap::new();

        for item in items {
            // Extract property name from the expression
            if let LogicalExpr::PropertyAccessExp(prop_access) = &item.expression {
                let table_alias = prop_access.table_alias.0.as_str();

                // Extract property name from PropertyValue enum
                let property_name = match &prop_access.column {
                    crate::graph_catalog::expression_parser::PropertyValue::Column(col) => col.clone(),
                    crate::graph_catalog::expression_parser::PropertyValue::Expression(expr) => expr.clone(),
                };

                // CTE column name follows convention: variablename_propertyname
                // e.g., p.firstName → p_firstName
                let cte_column = if let Some(alias) = &item.col_alias {
                    // If user provided alias, use it
                    alias.0.clone()
                } else {
                    // Otherwise, generate: variablename_propertyname
                    format!("{}_{}", table_alias, property_name)
                };

                columns.insert(property_name, cte_column);
            }
        }

        log::info!("📊 Registered CTE '{}' with {} columns: {:?}", cte_name, columns.len(), columns);
        self.cte_columns.insert(cte_name.to_string(), columns);
    }

    /// Register a single column mapping for a CTE
    ///
    /// # Arguments
    /// * `cte_name` - The CTE name
    /// * `schema_column` - The schema-specific column name (e.g., "PersonId", "CommentId")
    /// * `cte_column` - The standardized CTE column name (e.g., "from_node_id", "to_node_id")
    ///
    /// Used for multi-variant relationship CTEs that need to map multiple schema columns
    /// to standardized names.
    pub fn register_cte_column(&mut self, cte_name: &str, schema_column: &str, cte_column: &str) {
        self.cte_columns
            .entry(cte_name.to_string())
            .or_insert_with(HashMap::new)
            .insert(schema_column.to_string(), cte_column.to_string());
    }

    /// Get the CTE column name for a property
    ///
    /// # Arguments
    /// * `cte_name` - The CTE name
    /// * `property` - The graph property name (e.g., "firstName")
    ///
    /// # Returns
    /// The CTE column name (e.g., "p_firstName") or None if not found
    pub fn get_cte_column(&self, cte_name: &str, property: &str) -> Option<&str> {
        self.cte_columns
            .get(cte_name)?
            .get(property)
            .map(|s| s.as_str())
    }

    /// Check if a table name is a CTE reference
    pub fn is_cte(&self, name: &str) -> bool {
        self.cte_columns.contains_key(name)
    }

    /// Register entity types for aliases exported by a CTE
    ///
    /// # Arguments
    /// * `cte_name` - The CTE name (e.g., "with_tag_cte_1")
    /// * `exported_aliases` - The aliases exported by WITH (e.g., ["tag", "post"])
    ///
    /// This preserves node/relationship type information across WITH boundaries.
    /// Example: WITH tag, post → stores tag: (false, ["Tag"]), post: (false, ["Post"])
    ///
    /// This enables property resolution after WITH: `WITH tag ... RETURN tag.name`
    pub fn register_cte_entity_types(
        &mut self,
        cte_name: &str,
        exported_aliases: &[String],
    ) {
        let mut entity_types = HashMap::new();

        for alias in exported_aliases {
            // Look up the TableCtx for this alias in current scope
            if let Ok(table_ctx) = self.get_table_ctx(alias) {
                let is_rel = table_ctx.is_relation();
                let labels = table_ctx.get_labels().cloned();

                log::info!(
                    "📊 Registering entity type for CTE '{}' alias '{}': is_rel={}, labels={:?}",
                    cte_name,
                    alias,
                    is_rel,
                    labels
                );

                entity_types.insert(alias.clone(), (is_rel, labels));
            } else {
                // Alias not found in current scope - might be from parent scope or error
                log::warn!(
                    "⚠️  CTE '{}' exports alias '{}' but no TableCtx found in scope",
                    cte_name,
                    alias
                );
            }
        }

        self.cte_entity_types.insert(cte_name.to_string(), entity_types);
    }

    /// Get entity type information for a CTE alias
    ///
    /// # Arguments
    /// * `cte_name` - The CTE name
    /// * `alias` - The exported alias
    ///
    /// # Returns
    /// Some((is_rel, labels)) if found, None otherwise
    pub fn get_cte_entity_type(&self, cte_name: &str, alias: &str) -> Option<&(bool, Option<Vec<String>>)> {
        self.cte_entity_types
            .get(cte_name)?
            .get(alias)
    }

    /// Get property requirements for optimization
    ///
    /// Returns None if not yet populated by PropertyRequirementsAnalyzer pass
    pub fn get_property_requirements(&self) -> Option<&PropertyRequirements> {
        self.property_requirements.as_ref()
    }

    /// Set property requirements (called by PropertyRequirementsAnalyzer pass)
    ///
    /// This should be called once after analyzing the query plan to determine
    /// which properties are actually needed for each alias.
    pub fn set_property_requirements(&mut self, requirements: PropertyRequirements) {
        self.property_requirements = Some(requirements);
    }

    /// Check if property requirements have been populated
    ///
    /// Returns true if PropertyRequirementsAnalyzer pass has run and set requirements
    pub fn has_property_requirements(&self) -> bool {
        self.property_requirements.is_some()
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
