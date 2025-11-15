//! View-specific render plan structures

use super::render_expr::RenderExpr;
use serde::{Deserialize, Serialize};

/// Represents a view-based table reference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewTableRef {
    /// The original table name
    pub table_name: String,
    /// Optional table alias
    pub table_alias: Option<String>,
    /// Whether this is a view-based reference
    pub is_view: bool,
    /// The source query for view-based references
    pub source_query: Option<String>,
    /// View-specific filter conditions
    pub view_filter: Option<RenderExpr>,
}

impl ViewTableRef {
    /// Create a new direct table reference
    pub fn new_table(table_name: String, alias: Option<String>) -> Self {
        Self {
            table_name,
            table_alias: alias,
            is_view: false,
            source_query: None,
            view_filter: None,
        }
    }

    /// Create a new view-based table reference
    pub fn new_view(
        table_name: String,
        alias: Option<String>,
        source_query: String,
        view_filter: Option<RenderExpr>,
    ) -> Self {
        Self {
            table_name,
            table_alias: alias,
            is_view: true,
            source_query: Some(source_query),
            view_filter,
        }
    }
}
