//! Configuration for PatternResolver
//!
//! Manages configuration for systematic pattern type resolution,
//! particularly the maximum number of type combinations to generate.

use std::sync::OnceLock;

/// Default maximum type combinations (set to 38 as requested by user)
pub const DEFAULT_MAX_COMBINATIONS: usize = 38;

/// Environment variable name for configuring max combinations
const ENV_MAX_COMBINATIONS: &str = "CLICKGRAPH_MAX_TYPE_COMBINATIONS";

/// Cached max combinations value (initialized once)
static MAX_COMBINATIONS: OnceLock<usize> = OnceLock::new();

/// Get configured max combinations (cached after first call)
///
/// Reads from CLICKGRAPH_MAX_TYPE_COMBINATIONS environment variable,
/// falls back to DEFAULT_MAX_COMBINATIONS (38) if not set or invalid.
///
/// # Examples
///
/// ```bash
/// # Use default (38)
/// cargo run
///
/// # Set custom limit
/// export CLICKGRAPH_MAX_TYPE_COMBINATIONS=100
/// cargo run
/// ```
pub fn get_max_combinations() -> usize {
    *MAX_COMBINATIONS.get_or_init(|| {
        let value = std::env::var(ENV_MAX_COMBINATIONS)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_MAX_COMBINATIONS);
        value.clamp(1, 1000)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_max_combinations() {
        let max = get_max_combinations();
        // Should be > 0 and reasonable (not crazy high)
        assert!(max > 0);
        assert!(max <= 1000, "Max combinations should be reasonable");
    }

    #[test]
    fn test_default_is_38() {
        // Verify our "good number" default
        assert_eq!(DEFAULT_MAX_COMBINATIONS, 38);
    }
}
