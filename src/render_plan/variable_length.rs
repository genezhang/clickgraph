//! A variable-length relationship's hop bounds, as the VLP CTE generator
//! reads them.

use serde::{Deserialize, Serialize};

/// Specification for variable-length path relationships: the bounds of the
/// walk's hop count (`bound_plan::lower::path` passes a large bound for a
/// missing maximum).
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct VariableLengthSpec {
    pub min_hops: u32,
    pub max_hops: u32,
}

impl Default for VariableLengthSpec {
    /// Default is a single hop (normal relationship)
    fn default() -> Self {
        Self {
            min_hops: 1,
            max_hops: 1,
        }
    }
}

impl VariableLengthSpec {
    /// Create a range spec: *1..3 becomes min=1, max=3
    pub fn range(min: u32, max: u32) -> Self {
        Self {
            min_hops: min,
            max_hops: max,
        }
    }
}
