use thiserror::Error;

#[derive(Debug, Clone, Error)]
pub enum OptimizerError {
    #[error("No graph for anchor node.")]
    MissingAnchorNodeGraphTraversal,
    #[error("No logical table data found for a given uid.")]
    NoLogicalTableDataForUid,
    #[error(
        "No label found. Currently we need label to identify the table. This will change in future."
    )]
    MissingLabel,
    // Below is required in standalone node as of now
    #[error(
        "No node name found. Currently we need node name to identify the table. This will change in future."
    )]
    MissingNodeName,
    // #[error("No traversal sequence found.")]
    // NoTravelsalSequence,
    #[error("No traversal graph found.")]
    NoTravelsalGraph,
    #[error("No relationship schema found.")]
    NoRelationSchemaFound,
    #[error("No node schema found.")]
    NoNodeSchemaFound,
}
