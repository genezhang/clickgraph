//! Feature flags for plan_builder.rs refactoring
//!
//! This module provides runtime feature flags to control the gradual rollout
//! of plan_builder.rs extractions. Each flag corresponds to a module extraction
//! and allows for safe rollback if issues are discovered.

use std::collections::HashMap;
use std::env;

/// Feature flags for plan_builder.rs refactoring phases
#[derive(Debug, Clone)]
pub struct PlanBuilderFeatureFlags {
    /// Enable extraction of pure utilities to plan_builder_utils.rs
    pub extract_utilities: bool,

    /// Enable extraction of JOIN logic to join_builder.rs
    pub extract_join_builder: bool,

    /// Enable extraction of SELECT logic to select_builder.rs
    pub extract_select_builder: bool,

    /// Enable extraction of FROM logic to from_builder.rs
    pub extract_from_builder: bool,

    /// Enable extraction of filter logic to filter_builder.rs
    pub extract_filter_builder: bool,

    /// Enable extraction of GROUP BY logic to group_by_builder.rs
    pub extract_group_by_builder: bool,

    /// Enable extraction of ORDER BY logic to order_by_builder.rs
    pub extract_order_by_builder: bool,

    /// Enable extraction of UNION logic to union_builder.rs
    pub extract_union_builder: bool,
}

impl Default for PlanBuilderFeatureFlags {
    fn default() -> Self {
        Self {
            extract_utilities: false,
            extract_join_builder: false,
            extract_select_builder: false,
            extract_from_builder: false,
            extract_filter_builder: false,
            extract_group_by_builder: false,
            extract_order_by_builder: false,
            extract_union_builder: false,
        }
    }
}

impl PlanBuilderFeatureFlags {
    /// Create flags from environment variable
    /// Format: "extract_utilities:true,extract_join_builder:false"
    pub fn from_env() -> Self {
        let mut flags = Self::default();

        if let Ok(env_var) = env::var("PLAN_BUILDER_FEATURE_FLAGS") {
            let parsed = Self::parse_env_string(&env_var);
            flags = Self { ..parsed };
        }

        log::info!("PlanBuilderFeatureFlags: {:?}", flags);
        flags
    }

    /// Parse environment variable string
    fn parse_env_string(env_str: &str) -> Self {
        let mut flags = Self::default();

        for pair in env_str.split(',') {
            let parts: Vec<&str> = pair.split(':').collect();
            if parts.len() == 2 {
                let flag_name = parts[0].trim();
                let flag_value = parts[1].trim().to_lowercase();

                let enabled = matches!(flag_value.as_str(), "true" | "1" | "yes" | "on");

                match flag_name {
                    "extract_utilities" => flags.extract_utilities = enabled,
                    "extract_join_builder" => flags.extract_join_builder = enabled,
                    "extract_select_builder" => flags.extract_select_builder = enabled,
                    "extract_from_builder" => flags.extract_from_builder = enabled,
                    "extract_filter_builder" => flags.extract_filter_builder = enabled,
                    "extract_group_by_builder" => flags.extract_group_by_builder = enabled,
                    "extract_order_by_builder" => flags.extract_order_by_builder = enabled,
                    "extract_union_builder" => flags.extract_union_builder = enabled,
                    _ => log::warn!("Unknown feature flag: {}", flag_name),
                }
            }
        }

        flags
    }

    /// Validate flag combinations for consistency
    pub fn validate(&self) -> Result<(), String> {
        // Add validation rules here as needed
        // For example, ensure certain flags aren't enabled without prerequisites

        Ok(())
    }

    /// Get enabled flags as a map for debugging
    pub fn enabled_flags(&self) -> HashMap<&str, bool> {
        let mut map = HashMap::new();
        map.insert("extract_utilities", self.extract_utilities);
        map.insert("extract_join_builder", self.extract_join_builder);
        map.insert("extract_select_builder", self.extract_select_builder);
        map.insert("extract_from_builder", self.extract_from_builder);
        map.insert("extract_filter_builder", self.extract_filter_builder);
        map.insert("extract_group_by_builder", self.extract_group_by_builder);
        map.insert("extract_order_by_builder", self.extract_order_by_builder);
        map.insert("extract_union_builder", self.extract_union_builder);
        map
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_flags() {
        let flags = PlanBuilderFeatureFlags::default();
        assert!(!flags.extract_utilities);
        assert!(!flags.extract_join_builder);
    }

    #[test]
    fn test_parse_env_string() {
        let flags = PlanBuilderFeatureFlags::parse_env_string(
            "extract_utilities:true,extract_join_builder:false",
        );
        assert!(flags.extract_utilities);
        assert!(!flags.extract_join_builder);
    }

    #[test]
    fn test_enabled_flags() {
        let mut flags = PlanBuilderFeatureFlags::default();
        flags.extract_utilities = true;
        let enabled = flags.enabled_flags();
        assert!(enabled["extract_utilities"]);
        assert!(!enabled["extract_join_builder"]);
    }
}
