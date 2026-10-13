//! A variable-length relationship's hop bounds and its shortest-path mode,
//! as the VLP CTE generator reads them.

use serde::{Deserialize, Serialize};

/// Mode for shortest path queries
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum ShortestPathMode {
    /// shortestPath() - return one shortest path
    Shortest,
    /// allShortestPaths() - return all paths with minimum length
    AllShortest,
}

/// Specification for variable-length path relationships
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct VariableLengthSpec {
    pub min_hops: Option<u32>,
    pub max_hops: Option<u32>,
}

impl Default for VariableLengthSpec {
    /// Default is a single hop (normal relationship)
    fn default() -> Self {
        Self {
            min_hops: Some(1),
            max_hops: Some(1),
        }
    }
}

impl VariableLengthSpec {
    /// Create a range spec: *1..3 becomes min=1, max=3
    pub fn range(min: u32, max: u32) -> Self {
        Self {
            min_hops: Some(min),
            max_hops: Some(max),
        }
    }

    /// Create an unbounded spec: * becomes min=1, max=None (unlimited)
    pub fn unbounded() -> Self {
        Self {
            min_hops: Some(1),
            max_hops: None,
        }
    }

    /// Get effective minimum hops (defaults to 1)
    pub fn effective_min_hops(&self) -> u32 {
        self.min_hops.unwrap_or(1)
    }
}
