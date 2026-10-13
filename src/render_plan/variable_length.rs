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
    /// Create a fixed-length spec: *2 becomes min=2, max=2
    pub fn fixed(hops: u32) -> Self {
        Self {
            min_hops: Some(hops),
            max_hops: Some(hops),
        }
    }

    /// Create a range spec: *1..3 becomes min=1, max=3
    pub fn range(min: u32, max: u32) -> Self {
        Self {
            min_hops: Some(min),
            max_hops: Some(max),
        }
    }

    /// Create an upper-bounded spec: *..5 becomes min=1, max=5
    pub fn max_only(max: u32) -> Self {
        Self {
            min_hops: Some(1),
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

    /// Check if this is a single-hop relationship (normal relationship)
    pub fn is_single_hop(&self) -> bool {
        matches!(
            (self.min_hops, self.max_hops),
            (Some(1), Some(1)) | (None, None)
        )
    }

    /// Get effective minimum hops (defaults to 1)
    pub fn effective_min_hops(&self) -> u32 {
        self.min_hops.unwrap_or(1)
    }

    /// Check if there's an upper bound
    pub fn has_max_bound(&self) -> bool {
        self.max_hops.is_some()
    }

    /// Check if this is an exact hop count (e.g., *2, *3, *5)
    /// Returns Some(n) if min == max == n, None otherwise
    pub fn exact_hop_count(&self) -> Option<u32> {
        match (self.min_hops, self.max_hops) {
            // #1135: `*0..0` is NOT a fixed-length traversal — it is the
            // zero-hop identity (a = b, no edge). Every consumer of this
            // function treats `Some(n)` as an n-edge flat chain (r1..rN
            // self-joins), which has no zero-length form: the flat path
            // silently rendered `*0..0` as ONE hop (returned the 1-hop pairs
            // where the answer is the identity rows). Returning None routes it
            // through `is_range()` to the recursive CTE, whose zero-hop seed
            // with `max_hops == Some(0)` (no recursive arm) is exactly this
            // shape — the generator already handles it.
            (Some(0), Some(0)) => None,
            (Some(min), Some(max)) if min == max => Some(min),
            _ => None,
        }
    }

    /// Check if this requires a range (not exact hop count)
    pub fn is_range(&self) -> bool {
        self.exact_hop_count().is_none()
    }
}

impl From<crate::open_cypher_parser::ast::VariableLengthSpec> for VariableLengthSpec {
    fn from(ast_spec: crate::open_cypher_parser::ast::VariableLengthSpec) -> Self {
        Self {
            min_hops: ast_spec.min_hops,
            max_hops: ast_spec.max_hops,
        }
    }
}
