use super::function_registry::get_function_mapping;
/// Neo4j Function Translator
///
/// Translates Neo4j function calls to ClickHouse SQL equivalents
use std::collections::HashSet;
use std::sync::LazyLock;

/// Prefix for ClickHouse pass-through functions (scalar or auto-detected aggregates)
/// Usage: ch.functionName(args) -> functionName(args) passed directly to ClickHouse
/// Uses dot notation for Neo4j ecosystem compatibility (like apoc.*, gds.*)
pub const CH_PASSTHROUGH_PREFIX: &str = "ch.";

/// Prefix for explicit ClickHouse aggregate functions
/// Usage: chagg.functionName(args) -> functionName(args) with automatic GROUP BY
/// Use this for ANY aggregate function, including custom or new ones not in the registry
pub const CH_AGG_PREFIX: &str = "chagg.";

/// Registry of known ClickHouse aggregate functions
/// These functions require GROUP BY when used with non-aggregated columns
///
/// NOTE: For functions not in this registry, use ch.agg.functionName() to explicitly
/// mark them as aggregates.
///
/// Categories:
/// - Basic: count, sum, avg, min, max, any, anyLast, first_value, last_value
/// - Unique counting: uniq, uniqExact, uniqCombined, uniqCombined64, uniqHLL12, uniqTheta
/// - Quantiles: quantile, quantiles, quantileExact, quantileTDigest, quantileBFloat16, quantileGK, quantileDD, etc.
/// - Array: groupArray, groupArraySample, groupUniqArray, groupArrayMovingSum, groupArrayMovingAvg
/// - Statistics: varPop, varSamp, stddevPop, stddevSamp, covarPop, covarSamp, corr, skewPop, kurtPop
/// - TopK: topK, topKWeighted, approx_top_k, approx_top_sum
/// - ArgMin/Max: argMin, argMax, argAndMin, argAndMax
/// - Funnel: windowFunnel, retention, sequenceMatch, sequenceCount, sequenceNextNode
/// - Bitmap: groupBitmap, groupBitmapAnd, groupBitmapOr, groupBitmapXor, groupBitAnd, groupBitOr, groupBitXor
/// - Map: sumMap, minMap, maxMap, avgMap
/// - Statistical tests: mannWhitneyUTest, studentTTest, welchTTest, kolmogorovSmirnovTest
/// - Other: simpleLinearRegression, stochasticLinearRegression, entropy, sparkbar, groupConcat
static CH_AGGREGATE_FUNCTIONS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut s = HashSet::new();

    // Basic aggregates
    s.insert("count");
    s.insert("sum");
    s.insert("avg");
    s.insert("min");
    s.insert("max");
    s.insert("any");
    s.insert("anylast");
    s.insert("anyheavy");
    s.insert("first_value");
    s.insert("last_value");

    // Unique counting (HyperLogLog variants)
    s.insert("uniq");
    s.insert("uniqexact");
    s.insert("uniqcombined");
    s.insert("uniqcombined64");
    s.insert("uniqhll12");
    s.insert("uniqtheta");

    // Quantiles and percentiles (comprehensive - all ClickHouse quantile variants)
    s.insert("quantile");
    s.insert("quantiles");
    s.insert("quantileexact");
    s.insert("quantileexactlow");
    s.insert("quantileexacthigh");
    s.insert("quantileexactweighted");
    s.insert("quantileexactexclusive");
    s.insert("quantileexactinclusive");
    s.insert("quantileexactweightedinterpolated");
    s.insert("quantiletdigest");
    s.insert("quantiletdigestweighted");
    s.insert("quantilebfloat16");
    s.insert("quantilebfloat16weighted");
    s.insert("quantiletiming");
    s.insert("quantiletimingweighted");
    s.insert("quantiledeterministic");
    s.insert("quantilegk"); // Greenwald-Khanna algorithm
    s.insert("quantiledd"); // DDSketch algorithm
    s.insert("quantileinterpolatedweighted");
    s.insert("quantileprometheushistogram");
    s.insert("quantilesexactexclusive");
    s.insert("quantilesexactinclusive");
    s.insert("quantilesgk");
    s.insert("median");
    s.insert("medianexact");
    s.insert("medianexactlow");
    s.insert("medianexacthigh");
    s.insert("medianexactweighted");
    s.insert("mediantiming");
    s.insert("mediantdigest");
    s.insert("medianbfloat16");
    s.insert("mediandeterministic");

    // Array collection
    s.insert("grouparray");
    s.insert("grouparraysample");
    s.insert("groupuniqarray");
    s.insert("grouparrayinsertat");
    s.insert("grouparraymovingsum");
    s.insert("grouparraymovingavg");
    s.insert("grouparrayarray");

    // Statistics
    s.insert("varpop");
    s.insert("varsamp");
    s.insert("stddevpop");
    s.insert("stddevsamp");
    s.insert("covarpop");
    s.insert("covarsamp");
    s.insert("corr");
    s.insert("skewpop");
    s.insert("skewsamp");
    s.insert("kurtpop");
    s.insert("kurtsamp");

    // TopK
    s.insert("topk");
    s.insert("topkweighted");

    // ArgMin/Max
    s.insert("argmin");
    s.insert("argmax");

    // Funnel and retention analysis
    s.insert("windowfunnel");
    s.insert("retention");
    s.insert("sequencematch");
    s.insert("sequencecount");
    s.insert("sequencenextnode");

    // Bitmap aggregates
    s.insert("groupbitmap");
    s.insert("groupbitmapand");
    s.insert("groupbitmapor");
    s.insert("groupbitmapxor");
    s.insert("groupbitand");
    s.insert("groupbitor");
    s.insert("groupbitxor");

    // Map aggregates
    s.insert("summap");
    s.insert("minmap");
    s.insert("maxmap");
    s.insert("avgmap");
    s.insert("summapwithoverflow");
    s.insert("sumwithoverflow");

    // Histogram
    s.insert("histogram");

    // Regression
    s.insert("simplelinearregression");
    s.insert("stochasticlinearregression");
    s.insert("stochasticlogisticregression");

    // Statistical tests
    s.insert("studentttest");
    s.insert("studentttestonesample");
    s.insert("welchttest");
    s.insert("kolmogorovsmirnovtest");
    s.insert("meanztest");
    s.insert("analysisofvariance");

    // Other useful aggregates
    s.insert("entropy");
    s.insert("mannwhitneyutest");
    s.insert("rankcorr");
    s.insert("exponentialmovingaverage");
    s.insert("exponentialtimedecayedavg");
    s.insert("exponentialtimedecayedcount");
    s.insert("exponentialtimedecayedmax");
    s.insert("exponentialtimedecayedsum");
    s.insert("intervallengthsum");
    s.insert("boundingratio");
    s.insert("contingency");
    s.insert("cramersv");
    s.insert("cramersvbiascorrected");
    s.insert("theilsu");
    s.insert("maxintersections");
    s.insert("maxintersectionsposition");
    s.insert("sparkbar");
    s.insert("groupconcat");
    s.insert("singlevalueornull");
    s.insert("categoricalinformationvalue");
    s.insert("sumkahan");
    s.insert("sumcount");
    s.insert("avgweighted");
    s.insert("largesttrianglethreebuckets");
    s.insert("flamegraph");

    // Approx TopK
    s.insert("approx_top_k");
    s.insert("approx_top_sum");

    // ArgAnd variants
    s.insert("argandmin");
    s.insert("argandmax");

    // Array variants
    s.insert("grouparraylast");
    s.insert("grouparraysorted");
    s.insert("grouparrayintersect");
    s.insert("timeseriesgrouparray");

    // Matrix functions
    s.insert("corrmatrix");
    s.insert("covarpopmatrix");
    s.insert("covarsampmatrix");

    // Stable variants (numerically stable algorithms)
    s.insert("corrstable");
    s.insert("varpopstable");
    s.insert("varsampstable");
    s.insert("stddevpopstable");
    s.insert("stddevsampstable");
    s.insert("covarpopstable");
    s.insert("covarsampstable");

    // Delta/rate functions
    s.insert("deltasumtimestamp");
    s.insert("deltasum");

    // Merge functions (for combining partial aggregation states)
    s.insert("summerge");
    s.insert("countmerge");
    s.insert("avgmerge");
    s.insert("uniqmerge");

    // Time series functions
    s.insert("timeseriesdeltaagrid");
    s.insert("timeseriesinstantdeltatogrid");
    s.insert("timeseriesinstantratetogrid");
    s.insert("timeserieslasttwosamples");
    s.insert("timeseriesratetogrid");
    s.insert("timeseriesresampletoGridwithstaleness");
    s.insert("timeseriesderivtogrid");
    s.insert("timeseriespredictlineartogrid");
    s.insert("timeserieschangestogrid");
    s.insert("timeseriesresetstogrid");

    s
});

/// Check if a function name (without ch. prefix) is a known ClickHouse aggregate.
/// Consulted by the ClickHouse [`PassthroughPolicy`] to classify `ch.` calls.
///
/// [`PassthroughPolicy`]: crate::sql_generator::passthrough::PassthroughPolicy
pub fn is_ch_aggregate_function(fn_name: &str) -> bool {
    CH_AGGREGATE_FUNCTIONS.contains(fn_name.to_lowercase().as_str())
}
/// Map a single Neo4j duration unit + already-rendered value expression to the
/// active dialect's interval constructor. Returns `None` for an unrecognized
/// unit so each caller keeps its own unknown-unit policy (error vs skip).
///
/// ClickHouse uses `toInterval*(n)`; sub-second units fold into
/// `toIntervalSecond(n / scale)` since CH lacks ms/us/ns intervals. Databricks
/// uses `make_dt_interval(days, hours, mins, secs)` / `make_ym_interval(years,
/// months)` — both accept fractional/expression args, so sub-second precision
/// maps onto the fractional `secs` field.
///
/// Limitations (both shared with the consuming `render_interval_arithmetic`):
/// - Spark rejects adding a year-month interval to a day-time interval, so a
///   `duration({months: m, days: d})` that mixes the two families produces SQL
///   that errors at execution on Databricks. Single-family and single-unit
///   durations are the validated, supported cases.
/// - Only single-level interval arithmetic is supported: `x + duration(..)`.
///   Nested forms like `x + duration(..) + duration(..)` are mis-handled on
///   both dialects because the consumer detects the interval operand by
///   substring (`toInterval` / `make_*_interval`) and the inner result string
///   still contains that marker. Pre-existing for ClickHouse; not addressed here.
pub(crate) fn interval_expr_for_unit(
    unit_lower: &str,
    value_sql: &str,
    dialect: crate::sql_generator::SqlDialect,
) -> Option<String> {
    use crate::sql_generator::SqlDialect;
    Some(match dialect {
        SqlDialect::Databricks => match unit_lower {
            "years" | "year" => format!("make_ym_interval({}, 0)", value_sql),
            "months" | "month" => format!("make_ym_interval(0, {})", value_sql),
            "weeks" | "week" => format!("make_dt_interval(7 * ({}), 0, 0, 0)", value_sql),
            "days" | "day" => format!("make_dt_interval({}, 0, 0, 0)", value_sql),
            "hours" | "hour" => format!("make_dt_interval(0, {}, 0, 0)", value_sql),
            "minutes" | "minute" => format!("make_dt_interval(0, 0, {}, 0)", value_sql),
            "seconds" | "second" => format!("make_dt_interval(0, 0, 0, {})", value_sql),
            "milliseconds" | "millisecond" => {
                format!("make_dt_interval(0, 0, 0, {} / 1000.0)", value_sql)
            }
            "microseconds" | "microsecond" => {
                format!("make_dt_interval(0, 0, 0, {} / 1000000.0)", value_sql)
            }
            "nanoseconds" | "nanosecond" => {
                format!("make_dt_interval(0, 0, 0, {} / 1000000000.0)", value_sql)
            }
            _ => return None,
        },
        _ => match unit_lower {
            "years" | "year" => format!("toIntervalYear({})", value_sql),
            "months" | "month" => format!("toIntervalMonth({})", value_sql),
            "weeks" | "week" => format!("toIntervalWeek({})", value_sql),
            "days" | "day" => format!("toIntervalDay({})", value_sql),
            "hours" | "hour" => format!("toIntervalHour({})", value_sql),
            "minutes" | "minute" => format!("toIntervalMinute({})", value_sql),
            "seconds" | "second" => format!("toIntervalSecond({})", value_sql),
            "milliseconds" | "millisecond" => format!("toIntervalSecond({} / 1000.0)", value_sql),
            "microseconds" | "microsecond" => {
                format!("toIntervalSecond({} / 1000000.0)", value_sql)
            }
            "nanoseconds" | "nanosecond" => {
                format!("toIntervalSecond({} / 1000000000.0)", value_sql)
            }
            _ => return None,
        },
    })
}

/// Check if a function is supported (has a mapping)
pub fn is_function_supported(fn_name: &str) -> bool {
    get_function_mapping(fn_name).is_some()
}

/// Get list of all supported Neo4j functions
pub fn get_supported_functions() -> Vec<&'static str> {
    // This would need to be updated when we add lazy_static iteration
    // For now, return a static list
    vec![
        // DateTime
        "datetime",
        "date",
        "timestamp",
        // String
        "toUpper",
        "toLower",
        "trim",
        "substring",
        "size",
        "split",
        "replace",
        "reverse",
        "left",
        "right",
        // Math
        "abs",
        "ceil",
        "floor",
        "round",
        "sqrt",
        "rand",
        "sign",
        // List
        "head",
        "tail",
        "last",
        "range",
        // Type Conversion
        "toInteger",
        "toFloat",
        "toString",
        "toBoolean",
        // Aggregation
        "collect",
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interval_expr_for_unit_clickhouse_spellings() {
        use crate::sql_generator::SqlDialect::ClickHouse;
        assert_eq!(
            interval_expr_for_unit("days", "5", ClickHouse).unwrap(),
            "toIntervalDay(5)"
        );
        assert_eq!(
            interval_expr_for_unit("month", "1", ClickHouse).unwrap(),
            "toIntervalMonth(1)"
        );
        assert_eq!(
            interval_expr_for_unit("milliseconds", "1500", ClickHouse).unwrap(),
            "toIntervalSecond(1500 / 1000.0)"
        );
        assert!(interval_expr_for_unit("fortnights", "1", ClickHouse).is_none());
    }

    #[test]
    fn interval_expr_for_unit_databricks_spellings() {
        use crate::sql_generator::SqlDialect::Databricks;
        // day-time family -> make_dt_interval(days, hours, mins, secs)
        assert_eq!(
            interval_expr_for_unit("days", "5", Databricks).unwrap(),
            "make_dt_interval(5, 0, 0, 0)"
        );
        assert_eq!(
            interval_expr_for_unit("hours", "2", Databricks).unwrap(),
            "make_dt_interval(0, 2, 0, 0)"
        );
        assert_eq!(
            interval_expr_for_unit("weeks", "2", Databricks).unwrap(),
            "make_dt_interval(7 * (2), 0, 0, 0)"
        );
        assert_eq!(
            interval_expr_for_unit("milliseconds", "1500", Databricks).unwrap(),
            "make_dt_interval(0, 0, 0, 1500 / 1000.0)"
        );
        // year-month family -> make_ym_interval(years, months)
        assert_eq!(
            interval_expr_for_unit("year", "3", Databricks).unwrap(),
            "make_ym_interval(3, 0)"
        );
        assert_eq!(
            interval_expr_for_unit("months", "1", Databricks).unwrap(),
            "make_ym_interval(0, 1)"
        );
        assert!(interval_expr_for_unit("fortnights", "1", Databricks).is_none());
    }

    #[test]
    fn test_is_function_supported() {
        assert!(is_function_supported("toUpper"));
        assert!(is_function_supported("TOUPPER")); // Case insensitive
        assert!(is_function_supported("abs"));
        assert!(!is_function_supported("unknownFunc"));
    }

    #[test]
    fn test_get_supported_functions() {
        let supported = get_supported_functions();
        assert!(supported.contains(&"toUpper"));
        assert!(supported.contains(&"abs"));
        assert!(supported.contains(&"datetime"));
        assert!(supported.len() >= 20); // Should have 20+ functions
    }

    // ===== ClickHouse Aggregate Function Tests =====

    #[test]
    fn test_is_ch_aggregate_function() {
        // Basic aggregates
        assert!(is_ch_aggregate_function("uniq"));
        assert!(is_ch_aggregate_function("uniqExact"));
        assert!(is_ch_aggregate_function("UNIQ")); // Case insensitive
        assert!(is_ch_aggregate_function("quantile"));
        assert!(is_ch_aggregate_function("topK"));
        assert!(is_ch_aggregate_function("argMax"));
        assert!(is_ch_aggregate_function("groupArray"));
        assert!(is_ch_aggregate_function("windowFunnel"));
        assert!(is_ch_aggregate_function("retention"));
        assert!(is_ch_aggregate_function("simpleLinearRegression"));

        // Not aggregates
        assert!(!is_ch_aggregate_function("cityHash64"));
        assert!(!is_ch_aggregate_function("JSONExtract"));
        assert!(!is_ch_aggregate_function("upper"));
    }

    #[test]
    fn test_ch_aggregate_categories() {
        // Unique counting
        assert!(is_ch_aggregate_function("uniq"));
        assert!(is_ch_aggregate_function("uniqExact"));
        assert!(is_ch_aggregate_function("uniqCombined"));
        assert!(is_ch_aggregate_function("uniqHLL12"));

        // Quantiles
        assert!(is_ch_aggregate_function("quantile"));
        assert!(is_ch_aggregate_function("quantileExact"));
        assert!(is_ch_aggregate_function("quantileTDigest"));
        assert!(is_ch_aggregate_function("median"));

        // Array collection
        assert!(is_ch_aggregate_function("groupArray"));
        assert!(is_ch_aggregate_function("groupUniqArray"));
        assert!(is_ch_aggregate_function("groupArraySample"));

        // Statistics
        assert!(is_ch_aggregate_function("varPop"));
        assert!(is_ch_aggregate_function("stddevSamp"));
        assert!(is_ch_aggregate_function("corr"));

        // Funnel analysis
        assert!(is_ch_aggregate_function("windowFunnel"));
        assert!(is_ch_aggregate_function("retention"));
        assert!(is_ch_aggregate_function("sequenceMatch"));

        // Map aggregates
        assert!(is_ch_aggregate_function("sumMap"));
        assert!(is_ch_aggregate_function("avgMap"));
    }
}
