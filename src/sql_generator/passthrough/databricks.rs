//! Databricks / Spark SQL pass-through policy.
//!
//! A **single** prefix, `dbx.`, for both scalar and aggregate native
//! functions. There is no `dbxagg.` counterpart to `chagg.`: Spark's
//! aggregate surface is bounded and enumerable, so the
//! [`SPARK_AGGREGATE_FUNCTIONS`] registry is authoritative and the system
//! infers scalar-vs-aggregate itself. A missing aggregate is fixed by
//! adding it here, not by asking users to learn a second prefix.
//!
//! Spelling reference:
//! <https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-functions-builtin>

use std::collections::HashSet;
use std::sync::LazyLock;

use super::PassthroughPolicy;

/// Pass-through prefix for Databricks / Spark SQL native functions.
/// Usage: `dbx.functionName(args)` → `functionName(args)`.
pub const DBX_PASSTHROUGH_PREFIX: &str = "dbx.";

/// Known Spark / Databricks SQL built-in **aggregate** functions (lowercased).
/// Used to decide whether a `dbx.` call needs GROUP BY. Scalar functions are
/// everything else — they are not enumerated.
///
/// Bounded, unlike ClickHouse's combinator-driven aggregate space, so this
/// set can be effectively complete. UDAFs a user registers in their own
/// warehouse won't be here; add them as they surface.
static SPARK_AGGREGATE_FUNCTIONS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut s = HashSet::new();

    // Basic
    s.insert("count");
    s.insert("count_if");
    s.insert("sum");
    s.insert("avg");
    s.insert("mean");
    s.insert("min");
    s.insert("max");
    s.insert("min_by");
    s.insert("max_by");
    s.insert("first");
    s.insert("first_value");
    s.insert("last");
    s.insert("last_value");
    s.insert("any_value");
    s.insert("mode");
    s.insert("product");

    // Collection
    s.insert("collect_list");
    s.insert("collect_set");
    s.insert("array_agg"); // alias of collect_list

    // Distinct / sketch cardinality
    s.insert("approx_count_distinct");
    s.insert("count_min_sketch");
    s.insert("hll_sketch_agg");
    s.insert("hll_union_agg");

    // Percentiles / quantiles
    s.insert("median");
    s.insert("percentile");
    s.insert("percentile_approx");
    s.insert("approx_percentile");
    s.insert("histogram_numeric");

    // Statistics
    s.insert("stddev");
    s.insert("std");
    s.insert("stddev_samp");
    s.insert("stddev_pop");
    s.insert("variance");
    s.insert("var_samp");
    s.insert("var_pop");
    s.insert("skewness");
    s.insert("kurtosis");
    s.insert("corr");
    s.insert("covar_samp");
    s.insert("covar_pop");

    // Regression
    s.insert("regr_avgx");
    s.insert("regr_avgy");
    s.insert("regr_count");
    s.insert("regr_intercept");
    s.insert("regr_r2");
    s.insert("regr_slope");
    s.insert("regr_sxx");
    s.insert("regr_sxy");
    s.insert("regr_syy");

    // Boolean / bitwise
    s.insert("any");
    s.insert("some");
    s.insert("every");
    s.insert("bool_and");
    s.insert("bool_or");
    s.insert("bit_and");
    s.insert("bit_or");
    s.insert("bit_xor");
    s.insert("bitmap_construct_agg");
    s.insert("bitmap_or_agg");

    // Grouping (used with GROUPING SETS / ROLLUP / CUBE)
    s.insert("grouping");
    s.insert("grouping_id");

    // try_* aggregate variants
    s.insert("try_avg");
    s.insert("try_sum");

    s
});

/// Whether `fn_name` (without the `dbx.` prefix) is a known Spark aggregate.
pub fn is_spark_aggregate_function(fn_name: &str) -> bool {
    SPARK_AGGREGATE_FUNCTIONS.contains(fn_name.to_lowercase().as_str())
}

pub(crate) struct DatabricksPassthrough;

impl PassthroughPolicy for DatabricksPassthrough {
    fn scalar_prefix(&self) -> &'static str {
        DBX_PASSTHROUGH_PREFIX // "dbx."
    }

    fn agg_prefix(&self) -> Option<&'static str> {
        None // single-prefix design — the registry is authoritative
    }

    fn is_aggregate(&self, stripped: &str) -> bool {
        is_spark_aggregate_function(stripped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_prefix_no_agg_variant() {
        let p = DatabricksPassthrough;
        assert_eq!(p.scalar_prefix(), "dbx.");
        assert_eq!(p.agg_prefix(), None);
    }

    #[test]
    fn aggregate_detection_is_case_insensitive() {
        assert!(is_spark_aggregate_function("percentile_approx"));
        assert!(is_spark_aggregate_function("PERCENTILE_APPROX"));
        assert!(is_spark_aggregate_function("collect_list"));
        assert!(is_spark_aggregate_function("approx_count_distinct"));
        assert!(is_spark_aggregate_function("max_by"));
    }

    #[test]
    fn scalar_functions_are_not_aggregates() {
        assert!(!is_spark_aggregate_function("get_json_object"));
        assert!(!is_spark_aggregate_function("upper"));
        assert!(!is_spark_aggregate_function("element_at"));
        assert!(!is_spark_aggregate_function("array_contains"));
    }
}
