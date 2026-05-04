//! Combination-limit configuration shared by TypeInference's untyped-node
//! expansion (Phase 2). Caps how many type combinations the pass enumerates
//! before the schema-direction validity filter prunes them.
//!
//! These constants used to live alongside the now-deleted `pattern_resolver`
//! module; the file name is preserved for git-blame continuity.

use std::sync::OnceLock;

/// Maximum number of raw (pre-validity-filter) type combinations to generate
/// before applying schema-based relationship direction validation.
///
/// Set high enough that the cartesian product of typical schemas (e.g. 20 node
/// types × 4 untyped variables = 20^4 = 160_000) is generated in full so that
/// the validity filter can eliminate impossible combinations correctly.
pub const MAX_RAW_COMBINATIONS: usize = 200_000;

/// Default maximum number of *valid* (post-filter) UNION branches to emit.
///
/// The validity filter (`is_valid_combination_with_direction`) drastically
/// reduces the raw combination count — typically from tens-of-thousands to
/// tens-of-valid-arms.  This limit guards against pathological schemas where
/// even after filtering the branch count would be unreasonably large.
pub const DEFAULT_MAX_COMBINATIONS: usize = 500;

/// Environment variable name for configuring max branch combinations
const ENV_MAX_COMBINATIONS: &str = "CLICKGRAPH_MAX_TYPE_COMBINATIONS";

/// Cached max combinations value (initialized once)
static MAX_COMBINATIONS: OnceLock<usize> = OnceLock::new();

/// Get configured max *valid* branch combinations (cached after first call).
///
/// Reads from CLICKGRAPH_MAX_TYPE_COMBINATIONS environment variable,
/// falls back to DEFAULT_MAX_COMBINATIONS if not set or invalid.
pub fn get_max_combinations() -> usize {
    *MAX_COMBINATIONS.get_or_init(|| {
        std::env::var(ENV_MAX_COMBINATIONS)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_MAX_COMBINATIONS)
            .max(1)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_max_combinations() {
        let max = get_max_combinations();
        assert!(max > 0);
    }

    #[test]
    fn test_defaults_are_sensible() {
        assert!(DEFAULT_MAX_COMBINATIONS >= 100);
        assert!(MAX_RAW_COMBINATIONS >= DEFAULT_MAX_COMBINATIONS);
    }
}
