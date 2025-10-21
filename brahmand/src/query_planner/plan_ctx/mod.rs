pub mod errors;

use std::{collections::{HashMap, HashSet}, fmt};

use crate::query_planner::{
    logical_expr::{LogicalExpr, Property},
    logical_plan::ProjectionItem,
    plan_ctx::errors::PlanCtxError,
};

#[derive(Debug, PartialEq, Clone)]
pub struct TableCtx {
    alias: String,
    labels: Option<Vec<String>>,
    properties: Vec<Property>,
    filter_predicates: Vec<LogicalExpr>,
    projection_items: Vec<ProjectionItem>,
    is_rel: bool,
    use_edge_list: bool,
    explicit_alias: bool,
}

impl TableCtx {
    pub fn is_relation(&self) -> bool {
        self.is_rel
    }

    pub fn set_use_edge_list(&mut self, use_edge_list: bool) {
        self.use_edge_list = use_edge_list;
    }

    pub fn should_use_edge_list(&self) -> bool {
        self.use_edge_list
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
            use_edge_list: false,
            explicit_alias,
        }
    }

    pub fn get_label_str(&self) -> Result<String, PlanCtxError> {
        self.labels.as_ref().and_then(|v| v.first()).cloned().ok_or(PlanCtxError::Label {
            alias: self.alias.clone(),
        })
    }

    pub fn get_labels(&self) -> Option<&Vec<String>> {
        self.labels.as_ref()
    }

    pub fn get_label_opt(&self) -> Option<String> {
        self.labels.as_ref().and_then(|v| v.first()).cloned()
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
}

#[derive(Debug, PartialEq, Clone)]
pub struct PlanCtx {
    alias_table_ctx_map: HashMap<String, TableCtx>,
    /// Track which table aliases came from OPTIONAL MATCH for LEFT JOIN generation
    optional_aliases: HashSet<String>,
}

impl PlanCtx {
    pub fn insert_table_ctx(&mut self, alias: String, table_ctx: TableCtx) {
        self.alias_table_ctx_map.insert(alias, table_ctx);
    }

    /// Mark a table alias as coming from an OPTIONAL MATCH clause
    pub fn mark_as_optional(&mut self, alias: String) {
        self.optional_aliases.insert(alias);
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

    pub fn get_table_ctx(&self, alias: &str) -> Result<&TableCtx, PlanCtxError> {
        self.alias_table_ctx_map
            .get(alias)
            .ok_or(PlanCtxError::TableCtx {
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
}

impl PlanCtx {
    pub fn default() -> Self {
        PlanCtx {
            alias_table_ctx_map: HashMap::new(),
            optional_aliases: HashSet::new(),
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
        writeln!(f, "{}         use_edge_list: {:?}", pad, self.use_edge_list)?;
        writeln!(
            f,
            "{}         explicit_alias: {:?}",
            pad, self.explicit_alias
        )?;
        Ok(())
    }
}
