# ClickHouse Function Pass-Through Design Notes

**Status**: Planning (for v0.6.0+)  
**Created**: December 7, 2025

## Overview

Enable direct ClickHouse function calls in Cypher expressions via `ch::functionName()` syntax.

## Challenges

### 1. Function Type Classification

```cypher
-- Scalar (per-row)
RETURN ch::cityHash64(u.email)

-- Aggregate (needs GROUP BY)
RETURN ch::quantile(0.95)(u.price)  -- Also has special (param)(arg) syntax!

-- Array with Lambda
RETURN ch::arrayMap(x -> x * 2, u.scores)
```

### 2. Property Mapping & Alias Resolution

```cypher
-- User writes: ch::length(u.name)
-- But schema maps: name → full_name
-- Must generate: length(u.full_name)
```

### 3. Parameter Substitution

```cypher
-- User writes: ch::substring(u.email, $start, $len)
-- Must substitute: ch::substring(u.email, 5, 10)
```

### 4. ClickHouse-Specific Syntax

- **Parametric aggregates**: `quantile(0.95)(price)` - two sets of parens
- **Lambda expressions**: `arrayMap(x -> x + 1, arr)`
- **Named arguments**: `formatDateTime(dt, '%Y-%m-%d')`
- **Combinators**: `sumIf`, `countIf`, `avgMerge`

### 5. Type Safety

- Should we validate argument types?
- What about return type inference for further processing?

## Design Options

| Approach | Complexity | Safety | Flexibility |
|----------|------------|--------|-------------|
| **A: Simple prefix strip** | Low | ❌ Dangerous | High |
| **B: Categorized registry** | Medium | ✅ Good | Medium |
| **C: Full type system** | High | ✅✅ Best | Medium |

## Recommended: Option B (Categorized Registry)

Create a CH function registry with categories:

```rust
enum ChFunctionKind {
    Scalar,           // cityHash64, length, etc.
    Aggregate,        // sum, avg, quantile, etc.
    ArrayTransform,   // arrayMap, arrayFilter (need lambda)
    Parametric,       // quantile(0.95)(x), topK(10)(x)
}

struct ChFunctionInfo {
    kind: ChFunctionKind,
    min_args: u8,
    max_args: Option<u8>,
    // Future: argument types, return type
}
```

### Benefits

- Property mapping still works (we process args)
- Parameter substitution still works
- Aggregates properly flagged for GROUP BY
- Lambda functions get special parsing
- Clear error for unknown functions

## Implementation Plan

1. **Phase 1**: Define `ChFunctionKind` enum and basic registry
2. **Phase 2**: Add common scalar functions (100+)
3. **Phase 3**: Add aggregate functions with proper GROUP BY handling
4. **Phase 4**: Add array functions with lambda support
5. **Phase 5**: Add parametric aggregate syntax support

## ClickHouse Function Categories (Reference)

### Scalar Functions (~400+)
- String: `length`, `concat`, `substring`, `lower`, `upper`, `trim`, `reverse`, `replaceAll`
- Math: `abs`, `ceil`, `floor`, `round`, `sqrt`, `pow`, `log`, `exp`, `sin`, `cos`
- Date/Time: `toDate`, `toDateTime`, `now`, `today`, `formatDateTime`, `dateDiff`
- Type conversion: `toString`, `toInt64`, `toFloat64`, `toUUID`
- Hash: `cityHash64`, `sipHash64`, `MD5`, `SHA256`
- URL: `domain`, `protocol`, `path`, `extractURLParameter`
- IP: `IPv4NumToString`, `IPv4StringToNum`, `isIPv4String`
- JSON: `JSONExtract`, `JSONExtractString`, `JSONExtractInt`
- Geo: `greatCircleDistance`, `geoToH3`, `h3ToGeo`

### Aggregate Functions (~50+)
- Basic: `count`, `sum`, `avg`, `min`, `max`
- Statistical: `stddevPop`, `stddevSamp`, `varPop`, `varSamp`, `corr`, `covarPop`
- Array: `groupArray`, `groupUniqArray`, `groupArrayMovingSum`
- Approximate: `uniq`, `uniqExact`, `uniqHLL12`, `uniqCombined`
- Parametric: `quantile(0.5)`, `quantiles(0.25, 0.5, 0.75)`, `topK(10)`

### Array Functions (~60+)
- Basic: `length`, `empty`, `notEmpty`, `arrayElement`, `has`
- Transform: `arrayMap`, `arrayFilter`, `arrayReduce`
- Set: `arrayUnion`, `arrayIntersect`, `arrayDistinct`
- Aggregate: `arraySum`, `arrayAvg`, `arrayMin`, `arrayMax`

## Resources

- [ClickHouse Functions Reference](https://clickhouse.com/docs/en/sql-reference/functions)
- [Aggregate Functions](https://clickhouse.com/docs/en/sql-reference/aggregate-functions)
- [Array Functions](https://clickhouse.com/docs/en/sql-reference/functions/array-functions)
