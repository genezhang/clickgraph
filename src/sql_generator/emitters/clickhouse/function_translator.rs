use super::errors::ClickhouseQueryGeneratorError;
use super::function_registry::get_function_mapping;
use super::to_sql::ToSql;
/// Neo4j Function Translator
///
/// Translates Neo4j function calls to ClickHouse SQL equivalents
use crate::query_planner::logical_expr::{LogicalExpr, ScalarFnCall};
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

/// Translate a Neo4j scalar function call to ClickHouse SQL
pub fn translate_scalar_function(
    fn_call: &ScalarFnCall,
) -> Result<String, ClickhouseQueryGeneratorError> {
    let fn_name = &fn_call.name;

    // Native-function pass-through, keyed by the active dialect (`ch.`/`chagg.`
    // for ClickHouse, `dbx.` for Databricks). A prefix belonging to a *different*
    // backend is rejected here rather than leaking into the generated SQL.
    if let Some(bare) = crate::sql_generator::passthrough::strip_passthrough(
        fn_name,
        crate::server::query_context::get_current_dialect(),
    )
    .map_err(|e| ClickhouseQueryGeneratorError::SchemaError(e.to_string()))?
    {
        let args_sql: Vec<String> = fn_call
            .args
            .iter()
            .map(|e| e.to_sql())
            .collect::<Result<_, _>>()
            .map_err(|e| {
                ClickhouseQueryGeneratorError::schema_error_with_context(
                    format!("Failed to convert arguments to SQL: {}", e),
                    format!(
                        "in {} pass-through function with {} arguments",
                        fn_call.name,
                        fn_call.args.len()
                    ),
                )
            })?;
        log::debug!(
            "native pass-through: {}(..) -> {}({})",
            fn_call.name,
            bare,
            args_sql.join(", ")
        );
        return Ok(format!("{}({})", bare, args_sql.join(", ")));
    }

    let fn_name_lower = fn_name.to_lowercase();

    // Special handling for datetime({epochMillis: x}) -> identity pass-through
    // The epochMillis value is already an Int64 epoch timestamp, so just return it directly.
    // Temporal accessors like .month/.day will wrap it via fromUnixTimestamp64Milli().
    if fn_name_lower == "datetime" && fn_call.args.len() == 1 {
        if let LogicalExpr::MapLiteral(entries) = &fn_call.args[0] {
            if entries.len() == 1 && entries[0].0.to_lowercase() == "epochmillis" {
                return entries[0].1.to_sql();
            }
        }
        // Fall through to normal function_registry handling
    }

    // Special handling for duration() with map argument
    // Neo4j: duration({days: 5, hours: 2}) -> ClickHouse: (toIntervalDay(5) + toIntervalHour(2))
    if fn_name_lower == "duration" {
        return translate_duration_function(fn_call);
    }

    // Look up function mapping
    match get_function_mapping(&fn_name_lower) {
        Some(mapping) => {
            // Convert arguments to SQL
            let args_sql: Result<Vec<String>, _> =
                fn_call.args.iter().map(|e| e.to_sql()).collect();

            let args_sql = args_sql.map_err(|e| {
                ClickhouseQueryGeneratorError::SchemaError(format!(
                    "Failed to convert function arguments to SQL: {}",
                    e
                ))
            })?;

            // Apply argument transformation if provided
            let transformed_args = if let Some(transform_fn) = mapping.arg_transform {
                transform_fn(&args_sql)
            } else {
                args_sql
            };

            // Generate function call using the dialect-appropriate name
            Ok(format!(
                "{}({})",
                mapping.name_for(crate::server::query_context::get_current_dialect()),
                transformed_args.join(", ")
            ))
        }
        None => {
            // Function not mapped - try direct passthrough with warning
            log::warn!(
                "Neo4j function '{}' is not mapped to ClickHouse. Attempting direct passthrough. \
                 This may fail if ClickHouse doesn't support this function name.",
                fn_call.name
            );

            // Convert arguments and attempt passthrough
            let args_sql: Result<Vec<String>, _> =
                fn_call.args.iter().map(|e| e.to_sql()).collect();

            let args_sql = args_sql.map_err(|e| {
                ClickhouseQueryGeneratorError::SchemaError(format!(
                    "Failed to convert function arguments to SQL: {}",
                    e
                ))
            })?;

            Ok(format!("{}({})", fn_call.name, args_sql.join(", ")))
        }
    }
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

/// Translate a Neo4j `duration({...})` map into the active dialect's combined
/// interval expression, delegating the per-unit spelling to
/// [`interval_expr_for_unit`].
///
/// Neo4j duration supports (plural and singular): years, months, weeks, days,
/// hours, minutes, seconds, milliseconds, microseconds, nanoseconds.
///
/// Examples (ClickHouse):
///   duration({days: 5}) -> toIntervalDay(5)
///   duration({days: 5, hours: 2}) -> (toIntervalDay(5) + toIntervalHour(2))
///   duration({months: 1, days: 15}) -> (toIntervalMonth(1) + toIntervalDay(15))
fn translate_duration_function(
    fn_call: &ScalarFnCall,
) -> Result<String, ClickhouseQueryGeneratorError> {
    // duration() expects exactly one argument which should be a map literal
    if fn_call.args.len() != 1 {
        return Err(ClickhouseQueryGeneratorError::SchemaError(
            "duration() requires exactly one map argument, e.g., duration({days: 5})".to_string(),
        ));
    }

    // Extract the map argument
    match &fn_call.args[0] {
        LogicalExpr::MapLiteral(entries) => {
            if entries.is_empty() {
                return Err(ClickhouseQueryGeneratorError::SchemaError(
                    "duration() requires at least one time unit, e.g., duration({days: 5})"
                        .to_string(),
                ));
            }

            // Map Neo4j duration units to the active dialect's interval
            // constructors (ClickHouse `toInterval*`, Databricks `make_*_interval`).
            let dialect = crate::server::query_context::get_current_dialect();
            let interval_parts: Result<Vec<String>, _> = entries
                .iter()
                .map(|(key, value)| {
                    let value_sql = value.to_sql()?;
                    let key_lower = key.to_lowercase();
                    interval_expr_for_unit(&key_lower, &value_sql, dialect).ok_or_else(|| {
                        ClickhouseQueryGeneratorError::SchemaError(format!(
                            "Unknown duration unit '{}'. Supported: years, months, weeks, days, hours, minutes, seconds, milliseconds, microseconds, nanoseconds",
                            key
                        ))
                    })
                })
                .collect();

            let parts = interval_parts?;

            // Combine multiple intervals with + operator
            if parts.len() == 1 {
                Ok(parts[0].clone())
            } else {
                Ok(format!("({})", parts.join(" + ")))
            }
        }
        _ => {
            // If not a map literal, try to use it as a duration string (e.g., "P1D")
            // This is an ISO 8601 duration format that Neo4j also supports
            let arg_sql = fn_call.args[0].to_sql()?;
            log::warn!(
                "duration() called with non-map argument: {}. This may not work correctly in ClickHouse.",
                arg_sql
            );
            // Attempt to parse as ISO 8601 duration - ClickHouse doesn't natively support this,
            // but we could potentially support it via string parsing
            Err(ClickhouseQueryGeneratorError::SchemaError(format!(
                "duration() requires a map argument like duration({{days: 5}}), got: {}. \
                 ISO 8601 duration strings are not yet supported.",
                arg_sql
            )))
        }
    }
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
    use crate::query_planner::logical_expr::{Literal, LogicalExpr};

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
    fn test_translate_simple_function() {
        // toUpper('hello') -> upper('hello')
        let fn_call = ScalarFnCall {
            name: "toUpper".to_string(),
            args: vec![LogicalExpr::Literal(Literal::String("hello".to_string()))],
        };

        let result = translate_scalar_function(&fn_call).unwrap();
        assert_eq!(result, "upper('hello')");
    }

    #[test]
    fn test_translate_math_function() {
        // abs(-5) -> abs(-5)
        let fn_call = ScalarFnCall {
            name: "abs".to_string(),
            args: vec![LogicalExpr::Literal(Literal::Integer(-5))],
        };

        let result = translate_scalar_function(&fn_call).unwrap();
        assert_eq!(result, "abs(-5)");
    }

    #[test]
    fn test_translate_function_with_transformation() {
        // left('hello', 3) -> substring('hello', 1, 3)
        let fn_call = ScalarFnCall {
            name: "left".to_string(),
            args: vec![
                LogicalExpr::Literal(Literal::String("hello".to_string())),
                LogicalExpr::Literal(Literal::Integer(3)),
            ],
        };

        let result = translate_scalar_function(&fn_call).unwrap();
        assert_eq!(result, "substring('hello', 1, 3)");
    }

    #[test]
    fn test_unsupported_function_passthrough() {
        // unknownFunc(arg) -> unknownFunc(arg) with warning
        let fn_call = ScalarFnCall {
            name: "unknownFunc".to_string(),
            args: vec![LogicalExpr::Literal(Literal::Integer(42))],
        };

        let result = translate_scalar_function(&fn_call).unwrap();
        assert_eq!(result, "unknownFunc(42)");
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

    // ===== ClickHouse Pass-through Tests =====

    #[test]
    fn test_ch_passthrough_simple() {
        // ch.cityHash64('test') -> cityHash64('test')
        let fn_call = ScalarFnCall {
            name: "ch.cityHash64".to_string(),
            args: vec![LogicalExpr::Literal(Literal::String("test".to_string()))],
        };

        let result = translate_scalar_function(&fn_call).unwrap();
        assert_eq!(result, "cityHash64('test')");
    }

    #[test]
    fn test_ch_passthrough_multiple_args() {
        // ch.substring('hello', 2, 3) -> substring('hello', 2, 3)
        let fn_call = ScalarFnCall {
            name: "ch.substring".to_string(),
            args: vec![
                LogicalExpr::Literal(Literal::String("hello".to_string())),
                LogicalExpr::Literal(Literal::Integer(2)),
                LogicalExpr::Literal(Literal::Integer(3)),
            ],
        };

        let result = translate_scalar_function(&fn_call).unwrap();
        assert_eq!(result, "substring('hello', 2, 3)");
    }

    #[test]
    fn test_ch_passthrough_json_function() {
        // ch.JSONExtractString(data, 'field') -> JSONExtractString(data, 'field')
        let fn_call = ScalarFnCall {
            name: "ch.JSONExtractString".to_string(),
            args: vec![
                LogicalExpr::Literal(Literal::String(r#"{"name":"Alice"}"#.to_string())),
                LogicalExpr::Literal(Literal::String("name".to_string())),
            ],
        };

        let result = translate_scalar_function(&fn_call).unwrap();
        assert_eq!(result, r#"JSONExtractString('{"name":"Alice"}', 'name')"#);
    }

    #[test]
    fn test_ch_passthrough_no_args() {
        // ch.now() -> now()
        let fn_call = ScalarFnCall {
            name: "ch.now".to_string(),
            args: vec![],
        };

        let result = translate_scalar_function(&fn_call).unwrap();
        assert_eq!(result, "now()");
    }

    #[test]
    fn test_ch_passthrough_empty_name_error() {
        // ch. (empty) -> error
        let fn_call = ScalarFnCall {
            name: "ch.".to_string(),
            args: vec![],
        };

        let result = translate_scalar_function(&fn_call);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("requires a function name"));
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
    fn test_chagg_translate_function() {
        // chagg.customAggregate(x) -> customAggregate(x)
        let fn_call = ScalarFnCall {
            name: "chagg.myCustomAgg".to_string(),
            args: vec![LogicalExpr::Literal(Literal::String("test".to_string()))],
        };

        let result = translate_scalar_function(&fn_call).unwrap();
        assert_eq!(result, "myCustomAgg('test')");
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

    // ===== Duration Function Tests =====

    #[test]
    fn test_translate_datetime_epoch_millis_passthrough() {
        // datetime({epochMillis: friend.birthday}) -> friend.birthday (identity)
        let fn_call = ScalarFnCall {
            name: "datetime".to_string(),
            args: vec![LogicalExpr::MapLiteral(vec![(
                "epochMillis".to_string(),
                LogicalExpr::PropertyAccessExp(
                    crate::query_planner::logical_expr::PropertyAccess {
                        table_alias: crate::query_planner::logical_expr::TableAlias(
                            "friend".to_string(),
                        ),
                        column: crate::graph_catalog::expression_parser::PropertyValue::Column(
                            "birthday".to_string(),
                        ),
                    },
                ),
            )])],
        };

        let result = translate_scalar_function(&fn_call).unwrap();
        assert_eq!(result, "friend.birthday");
    }

    #[test]
    fn test_translate_datetime_epoch_millis_literal() {
        use crate::query_planner::logical_expr::Literal;

        // datetime({epochMillis: 1234567890}) -> 1234567890
        let fn_call = ScalarFnCall {
            name: "datetime".to_string(),
            args: vec![LogicalExpr::MapLiteral(vec![(
                "epochMillis".to_string(),
                LogicalExpr::Literal(Literal::Integer(1234567890)),
            )])],
        };

        let result = translate_scalar_function(&fn_call).unwrap();
        assert_eq!(result, "1234567890");
    }

    #[test]
    fn test_translate_duration_single_days() {
        use crate::query_planner::logical_expr::Literal;

        // duration({days: 5}) -> toIntervalDay(5)
        let fn_call = ScalarFnCall {
            name: "duration".to_string(),
            args: vec![LogicalExpr::MapLiteral(vec![(
                "days".to_string(),
                LogicalExpr::Literal(Literal::Integer(5)),
            )])],
        };

        let result = translate_scalar_function(&fn_call).unwrap();
        assert_eq!(result, "toIntervalDay(5)");
    }

    #[test]
    fn test_translate_duration_multiple_units() {
        use crate::query_planner::logical_expr::Literal;

        // duration({days: 5, hours: 2}) -> (toIntervalDay(5) + toIntervalHour(2))
        let fn_call = ScalarFnCall {
            name: "duration".to_string(),
            args: vec![LogicalExpr::MapLiteral(vec![
                (
                    "days".to_string(),
                    LogicalExpr::Literal(Literal::Integer(5)),
                ),
                (
                    "hours".to_string(),
                    LogicalExpr::Literal(Literal::Integer(2)),
                ),
            ])],
        };

        let result = translate_scalar_function(&fn_call).unwrap();
        assert_eq!(result, "(toIntervalDay(5) + toIntervalHour(2))");
    }

    #[test]
    fn test_translate_duration_all_units() {
        use crate::query_planner::logical_expr::Literal;

        // Test various time units
        let test_cases = vec![
            (vec![("years", 1)], "toIntervalYear(1)"),
            (vec![("months", 2)], "toIntervalMonth(2)"),
            (vec![("weeks", 3)], "toIntervalWeek(3)"),
            (vec![("days", 4)], "toIntervalDay(4)"),
            (vec![("hours", 5)], "toIntervalHour(5)"),
            (vec![("minutes", 6)], "toIntervalMinute(6)"),
            (vec![("seconds", 7)], "toIntervalSecond(7)"),
        ];

        for (entries, expected) in test_cases {
            let fn_call = ScalarFnCall {
                name: "duration".to_string(),
                args: vec![LogicalExpr::MapLiteral(
                    entries
                        .iter()
                        .map(|(k, v)| (k.to_string(), LogicalExpr::Literal(Literal::Integer(*v))))
                        .collect(),
                )],
            };

            let result = translate_scalar_function(&fn_call).unwrap();
            assert_eq!(result, expected, "Failed for unit: {:?}", entries);
        }
    }

    #[test]
    fn test_translate_duration_invalid_args() {
        use crate::query_planner::logical_expr::Literal;

        // No arguments -> error
        let fn_call = ScalarFnCall {
            name: "duration".to_string(),
            args: vec![],
        };
        assert!(translate_scalar_function(&fn_call).is_err());

        // Non-map argument -> error
        let fn_call = ScalarFnCall {
            name: "duration".to_string(),
            args: vec![LogicalExpr::Literal(Literal::Integer(5))],
        };
        assert!(translate_scalar_function(&fn_call).is_err());

        // Empty map -> error
        let fn_call = ScalarFnCall {
            name: "duration".to_string(),
            args: vec![LogicalExpr::MapLiteral(vec![])],
        };
        assert!(translate_scalar_function(&fn_call).is_err());
    }
}
