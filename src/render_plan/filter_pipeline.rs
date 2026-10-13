//! The filters a variable-length walk applies at every step, pre-rendered.

/// A walk's pre-rendered filters (`bound_plan::lower::path`): the start
/// node's, applied to the walk's first node, and the relationship's,
/// applied at every hop.
#[derive(Debug, Clone, Default)]
pub struct CategorizedFilters {
    pub start_sql: Option<String>,
    pub relationship_sql: Option<String>,
}
