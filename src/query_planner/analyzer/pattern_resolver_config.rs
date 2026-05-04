//! Combination-limit configuration shared by TypeInference's untyped-node
//! expansion (Phase 2). This module defines a two-stage limit:
//! `MAX_RAW_COMBINATIONS` caps raw type-combination enumeration before the
//! schema-direction validity filter runs, and `get_max_combinations()`
//! caps how many valid (post-filter) UNION branches may be emitted.
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
    *MAX_COMBINATIONS.get_or_init(read_max_combinations_from_env)
}

/// Inner parsing logic split out so tests can exercise it without touching
/// the process-wide `OnceLock`. Reads the env var, parses to `usize`, falls
/// back to `DEFAULT_MAX_COMBINATIONS` on missing/unparseable, and clamps
/// the floor to 1.
fn read_max_combinations_from_env() -> usize {
    std::env::var(ENV_MAX_COMBINATIONS)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_MAX_COMBINATIONS)
        .max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    /// Verifies the cached public entry point returns a sane value. The
    /// `OnceLock` makes per-test mutation of the cache impossible, so the
    /// behavioural coverage lives on `read_max_combinations_from_env`
    /// below — this just pins that the cache wrapper itself runs.
    #[test]
    fn get_max_combinations_returns_positive() {
        let max = get_max_combinations();
        assert!(max > 0, "max combinations must be positive, got {max}");
    }

    /// `#[serial]`: all tests in this block mutate the same env var, so
    /// they must not run in parallel with each other.
    #[test]
    #[serial]
    fn read_env_falls_back_to_default_when_unset() {
        // SAFETY: this test owns CLICKGRAPH_MAX_TYPE_COMBINATIONS for its
        // duration via `#[serial]`.
        std::env::remove_var(ENV_MAX_COMBINATIONS);
        assert_eq!(read_max_combinations_from_env(), DEFAULT_MAX_COMBINATIONS);
    }

    #[test]
    #[serial]
    fn read_env_honours_valid_override() {
        std::env::set_var(ENV_MAX_COMBINATIONS, "12345");
        assert_eq!(read_max_combinations_from_env(), 12345);
        std::env::remove_var(ENV_MAX_COMBINATIONS);
    }

    #[test]
    #[serial]
    fn read_env_falls_back_to_default_on_unparseable() {
        std::env::set_var(ENV_MAX_COMBINATIONS, "not-a-number");
        assert_eq!(read_max_combinations_from_env(), DEFAULT_MAX_COMBINATIONS);
        std::env::remove_var(ENV_MAX_COMBINATIONS);
    }

    #[test]
    #[serial]
    fn read_env_clamps_zero_to_one() {
        std::env::set_var(ENV_MAX_COMBINATIONS, "0");
        assert_eq!(read_max_combinations_from_env(), 1);
        std::env::remove_var(ENV_MAX_COMBINATIONS);
    }
}
