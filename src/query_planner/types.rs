#[derive(Debug, PartialEq, Clone)]
pub enum QueryType {
    Ddl,
    Read,
    Update,
    Delete,
    Call,
    /// Standalone procedure call (e.g., CALL db.labels())
    Procedure,
}
