use super::errors::ClickhouseQueryGeneratorError;
use super::function_registry::get_function_mapping;
use super::to_sql::ToSql;
/// Neo4j Function Translator
///
/// Translates Neo4j function calls to ClickHouse SQL equivalents
use crate::query_planner::logical_expr::ScalarFnCall;
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
    s.insert("quantilegk");  // Greenwald-Khanna algorithm
    s.insert("quantiledd");  // DDSketch algorithm
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

/// Check if a function name (without ch. prefix) is a known ClickHouse aggregate
pub fn is_ch_aggregate_function(fn_name: &str) -> bool {
    CH_AGGREGATE_FUNCTIONS.contains(fn_name.to_lowercase().as_str())
}

/// Check if a function uses the explicit chagg. prefix
/// chagg.functionName() is ALWAYS treated as an aggregate, no registry lookup needed
pub fn is_explicit_ch_aggregate(fn_name: &str) -> bool {
    fn_name.starts_with(CH_AGG_PREFIX)
}

/// Check if a ch. prefixed function is an aggregate
/// Returns true if:
/// 1. Function starts with chagg. (explicit aggregate), OR
/// 2. Function starts with ch. and the underlying function is in the aggregate registry
pub fn is_ch_passthrough_aggregate(fn_name: &str) -> bool {
    // Explicit chagg. prefix - always an aggregate
    if fn_name.starts_with(CH_AGG_PREFIX) {
        return true;
    }
    // ch. prefix - check registry
    if fn_name.starts_with(CH_PASSTHROUGH_PREFIX) {
        let ch_fn_name = &fn_name[CH_PASSTHROUGH_PREFIX.len()..];
        return is_ch_aggregate_function(ch_fn_name);
    }
    false
}

/// Get the raw ClickHouse function name from a ch. or chagg. prefixed name
/// Returns None if not a ch./chagg. prefixed function
pub fn get_ch_function_name(fn_name: &str) -> Option<&str> {
    if fn_name.starts_with(CH_AGG_PREFIX) {
        Some(&fn_name[CH_AGG_PREFIX.len()..])
    } else if fn_name.starts_with(CH_PASSTHROUGH_PREFIX) {
        Some(&fn_name[CH_PASSTHROUGH_PREFIX.len()..])
    } else {
        None
    }
}

/// Translate a Neo4j scalar function call to ClickHouse SQL
pub fn translate_scalar_function(
    fn_call: &ScalarFnCall,
) -> Result<String, ClickhouseQueryGeneratorError> {
    let fn_name = &fn_call.name;
    
    // Check for ClickHouse pass-through prefixes (chagg. or ch.)
    if fn_name.starts_with(CH_AGG_PREFIX) || fn_name.starts_with(CH_PASSTHROUGH_PREFIX) {
        return translate_ch_passthrough(fn_call);
    }
    
    let fn_name_lower = fn_name.to_lowercase();

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

            // Generate ClickHouse function call
            Ok(format!(
                "{}({})",
                mapping.clickhouse_name,
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

/// Translate a ClickHouse pass-through function (ch. prefix)
/// 
/// The ch. prefix allows direct access to any ClickHouse function without
/// requiring a Neo4j mapping. Uses dot notation for Neo4j ecosystem compatibility
/// (consistent with apoc.*, gds.* patterns). Arguments still undergo property
/// mapping and parameter substitution.
/// 
/// # Examples
/// ```cypher
/// // Scalar functions
/// RETURN ch.cityHash64(u.email) AS hash
/// RETURN ch.JSONExtractString(u.metadata, 'field') AS field
/// 
/// // URL functions
/// RETURN ch.domain(u.url) AS domain
/// 
/// // IP functions  
/// RETURN ch.IPv4NumToString(u.ip) AS ip_str
/// 
/// // Geo functions
/// RETURN ch.greatCircleDistance(lat1, lon1, lat2, lon2) AS distance
/// ```
fn translate_ch_passthrough(
    fn_call: &ScalarFnCall,
) -> Result<String, ClickhouseQueryGeneratorError> {
    // Strip the ch. or chagg. prefix to get the raw ClickHouse function name
    let ch_fn_name = get_ch_function_name(&fn_call.name)
        .ok_or_else(|| ClickhouseQueryGeneratorError::SchemaError(
            format!("Expected ch. or chagg. prefix in function name: {}", fn_call.name)
        ))?;
    
    if ch_fn_name.is_empty() {
        return Err(ClickhouseQueryGeneratorError::SchemaError(
            "ch./chagg. prefix requires a function name (e.g., ch.cityHash64, chagg.myAgg)".to_string()
        ));
    }
    
    // Convert arguments to SQL (this preserves property mapping)
    let args_sql: Result<Vec<String>, _> =
        fn_call.args.iter().map(|e| e.to_sql()).collect();
    
    let args_sql = args_sql.map_err(|e| {
        ClickhouseQueryGeneratorError::SchemaError(format!(
            "Failed to convert {} arguments to SQL: {}",
            fn_call.name, e
        ))
    })?;
    
    log::debug!(
        "ClickHouse pass-through: {}({}) -> {}({})",
        fn_call.name,
        fn_call.args.iter().map(|a| format!("{:?}", a)).collect::<Vec<_>>().join(", "),
        ch_fn_name,
        args_sql.join(", ")
    );
    
    // Generate ClickHouse function call directly
    Ok(format!("{}({})", ch_fn_name, args_sql.join(", ")))
}

/// Check if a function uses ClickHouse pass-through (ch. prefix)
pub fn is_ch_passthrough(fn_name: &str) -> bool {
    fn_name.starts_with(CH_PASSTHROUGH_PREFIX)
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
        assert!(result.unwrap_err().to_string().contains("requires a function name"));
    }

    #[test]
    fn test_is_ch_passthrough() {
        assert!(is_ch_passthrough("ch.cityHash64"));
        assert!(is_ch_passthrough("ch.JSONExtract"));
        assert!(!is_ch_passthrough("cityHash64"));
        assert!(!is_ch_passthrough("toUpper"));
        assert!(!is_ch_passthrough("CH.test")); // Case sensitive
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
    fn test_is_ch_passthrough_aggregate() {
        // ch. prefixed aggregates
        assert!(is_ch_passthrough_aggregate("ch.uniq"));
        assert!(is_ch_passthrough_aggregate("ch.quantile"));
        assert!(is_ch_passthrough_aggregate("ch.topK"));
        assert!(is_ch_passthrough_aggregate("ch.groupArray"));
        
        // ch. prefixed non-aggregates
        assert!(!is_ch_passthrough_aggregate("ch.cityHash64"));
        assert!(!is_ch_passthrough_aggregate("ch.JSONExtract"));
        
        // Non ch. prefixed
        assert!(!is_ch_passthrough_aggregate("uniq"));
        assert!(!is_ch_passthrough_aggregate("count"));
    }

    #[test]
    fn test_chagg_explicit_aggregate_prefix() {
        // chagg. prefix is ALWAYS an aggregate, regardless of function name
        assert!(is_ch_passthrough_aggregate("chagg.customAggregate"));
        assert!(is_ch_passthrough_aggregate("chagg.mySpecialFunc"));
        assert!(is_ch_passthrough_aggregate("chagg.uniq")); // Also works for known ones
        assert!(is_ch_passthrough_aggregate("chagg.anyNewFunction"));
        
        // chagg. prefix starts_with check
        assert!(is_explicit_ch_aggregate("chagg.test"));
        assert!(!is_explicit_ch_aggregate("ch.test"));
        assert!(!is_explicit_ch_aggregate("test"));
    }

    #[test]
    fn test_get_ch_function_name_both_prefixes() {
        // ch. prefix
        assert_eq!(get_ch_function_name("ch.uniq"), Some("uniq"));
        assert_eq!(get_ch_function_name("ch.cityHash64"), Some("cityHash64"));
        
        // chagg. prefix
        assert_eq!(get_ch_function_name("chagg.customAgg"), Some("customAgg"));
        assert_eq!(get_ch_function_name("chagg.uniq"), Some("uniq"));
        
        // No prefix
        assert_eq!(get_ch_function_name("uniq"), None);
        assert_eq!(get_ch_function_name("count"), None);
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
    fn test_get_ch_function_name() {
        assert_eq!(get_ch_function_name("ch.uniq"), Some("uniq"));
        assert_eq!(get_ch_function_name("ch.cityHash64"), Some("cityHash64"));
        assert_eq!(get_ch_function_name("ch."), Some(""));
        assert_eq!(get_ch_function_name("uniq"), None);
        assert_eq!(get_ch_function_name("count"), None);
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
