# Neo4j Functions Implementation Plan

**Goal**: Implement 20+ core Neo4j functions for Phase 1  
**Timeline**: 3 nights (Nov 13-15)  
**Status**: Starting Tonight!

---

## Architecture Overview

### Current State
- ✅ Function parsing: `FunctionCall` in AST
- ✅ Logical representation: `ScalarFnCall` and `AggregateFnCall`
- ⚠️ SQL generation: Direct passthrough (no translation)

### What We Need
1. **Function Registry**: Map Neo4j functions → ClickHouse equivalents
2. **Translation Layer**: Convert function calls with argument transformations
3. **Type Handling**: Ensure correct ClickHouse types
4. **Error Messages**: Clear unsupported function warnings

---

## Implementation Strategy

### File Structure
```
brahmand/src/
└── clickhouse_query_generator/
    ├── to_sql.rs              (modify ScalarFnCall handling)
    ├── function_registry.rs   (NEW - function mappings)
    └── function_translator.rs (NEW - translation logic)
```

### Core Components

#### 1. Function Registry (`function_registry.rs`)
```rust
pub struct FunctionMapping {
    neo4j_name: &'static str,
    clickhouse_name: &'static str,
    arg_transform: Option<fn(&[String]) -> Vec<String>>,
}

pub fn get_function_mapping(neo4j_fn: &str) -> Option<FunctionMapping> {
    // Lookup table
}
```

#### 2. Translation Logic (`function_translator.rs`)
```rust
pub fn translate_function(
    neo4j_name: &str,
    args: &[LogicalExpr]
) -> Result<String, Error> {
    // 1. Lookup mapping
    // 2. Convert args to SQL
    // 3. Apply transformations
    // 4. Return ClickHouse SQL
}
```

---

## Night 1: DateTime + String Functions (10 functions)

### DateTime Functions (5 functions) ⏰

| Neo4j | ClickHouse | Args | Notes |
|-------|------------|------|-------|
| `datetime()` | `parseDateTime64BestEffort()` | ISO8601 string | Parse ISO datetime |
| `date()` | `toDate()` | datetime/string | Extract date |
| `timestamp()` | `toUnixTimestamp()` | datetime | Unix timestamp |
| `datetime().year` | `toYear()` | datetime | Extract year |
| `datetime().month` | `toMonth()` | datetime | Extract month |

**Implementation**:
```rust
// datetime() -> parseDateTime64BestEffort(arg, 3)
"datetime" => FunctionMapping {
    neo4j_name: "datetime",
    clickhouse_name: "parseDateTime64BestEffort",
    arg_transform: Some(|args| vec![args[0].clone(), "'UTC'".to_string()])
}

// date() -> toDate(arg)
"date" => FunctionMapping {
    neo4j_name: "date",
    clickhouse_name: "toDate",
    arg_transform: None
}

// timestamp() -> toUnixTimestamp(arg)
"timestamp" => FunctionMapping {
    neo4j_name: "timestamp",
    clickhouse_name: "toUnixTimestamp",
    arg_transform: None
}
```

### String Functions (5 functions) 📝

| Neo4j | ClickHouse | Args | Notes |
|-------|------------|------|-------|
| `toUpper()` | `upper()` | string | Uppercase |
| `toLower()` | `lower()` | string | Lowercase |
| `trim()` | `trim()` | string | Remove whitespace |
| `substring()` | `substring()` | string, start, len | Extract substring |
| `size()` | `length()` | string/array | String/array length |

**Implementation**:
```rust
// toUpper() -> upper(arg)
"toUpper" | "toupper" => FunctionMapping {
    neo4j_name: "toUpper",
    clickhouse_name: "upper",
    arg_transform: None
}

// size() -> length(arg) for strings
// size() -> length(arg) for arrays (same in ClickHouse!)
"size" => FunctionMapping {
    neo4j_name: "size",
    clickhouse_name: "length",
    arg_transform: None
}
```

### Testing Strategy (Night 1)
```cypher
// DateTime tests
RETURN datetime('2024-01-15T10:30:00') AS dt
RETURN date('2024-01-15') AS d
RETURN timestamp(datetime('2024-01-15T10:30:00')) AS ts
MATCH (n) RETURN datetime(n.created_date).year AS year

// String tests
RETURN toUpper('hello') AS upper_text
RETURN toLower('WORLD') AS lower_text
RETURN trim('  spaces  ') AS trimmed
RETURN substring('Hello World', 0, 5) AS sub
RETURN size('test') AS str_len, size([1,2,3]) AS arr_len
```

---

## Night 2: Math + More Strings (10 functions)

### Math Functions (6 functions) ➗

| Neo4j | ClickHouse | Args | Notes |
|-------|------------|------|-------|
| `abs()` | `abs()` | number | Absolute value |
| `ceil()` | `ceil()` | number | Round up |
| `floor()` | `floor()` | number | Round down |
| `round()` | `round()` | number | Round nearest |
| `sqrt()` | `sqrt()` | number | Square root |
| `rand()` | `rand()` | none | Random 0-1 |

**Implementation**:
```rust
// Direct 1:1 mappings (easy!)
"abs" | "ceil" | "floor" | "round" | "sqrt" => FunctionMapping {
    neo4j_name: neo4j_name,
    clickhouse_name: neo4j_name, // Same name!
    arg_transform: None
}

// rand() -> rand() / 4294967295.0 (normalize to 0-1)
"rand" => FunctionMapping {
    neo4j_name: "rand",
    clickhouse_name: "rand",
    arg_transform: Some(|_| vec!["rand() / 4294967295.0".to_string()])
}
```

### Advanced String Functions (4 functions) 📝

| Neo4j | ClickHouse | Args | Notes |
|-------|------------|------|-------|
| `split()` | `splitByChar()` | string, delim | Split into array |
| `replace()` | `replaceAll()` | string, from, to | Replace substring |
| `reverse()` | `reverse()` | string | Reverse string |
| `left()` | `substring()` | string, length | First N chars |

**Implementation**:
```rust
// split(',', str) in Neo4j -> splitByChar(',', str) in ClickHouse
"split" => FunctionMapping {
    neo4j_name: "split",
    clickhouse_name: "splitByChar",
    arg_transform: Some(|args| vec![args[1].clone(), args[0].clone()]) // Swap args!
}

// left(str, n) -> substring(str, 1, n)
"left" => FunctionMapping {
    neo4j_name: "left",
    clickhouse_name: "substring",
    arg_transform: Some(|args| vec![args[0].clone(), "1".to_string(), args[1].clone()])
}
```

### Testing Strategy (Night 2)
```cypher
// Math tests
RETURN abs(-5) AS absolute
RETURN ceil(4.3) AS ceiling
RETURN floor(4.7) AS floored
RETURN round(4.567, 2) AS rounded
RETURN sqrt(16) AS square_root
RETURN rand() AS random_val

// Advanced string tests
RETURN split('a,b,c', ',') AS parts
RETURN replace('Hello World', 'World', 'Cypher') AS replaced
RETURN reverse('Hello') AS reversed
RETURN left('Hello World', 5) AS first_five
```

---

## Night 3: List + Type Conversion (6+ functions)

### List Functions (3 functions) 📋

| Neo4j | ClickHouse | Args | Notes |
|-------|------------|------|-------|
| `head()` | `arrayElement(arr, 1)` | array | First element |
| `tail()` | `arraySlice(arr, 2)` | array | All but first |
| `last()` | `arrayElement(arr, -1)` | array | Last element |

### Type Conversion (3+ functions) 🔄

| Neo4j | ClickHouse | Args | Notes |
|-------|------------|------|-------|
| `toInteger()` | `toInt64()` | any | Convert to int |
| `toFloat()` | `toFloat64()` | any | Convert to float |
| `toString()` | `toString()` | any | Convert to string |

### Testing & Polish
- Edge case handling
- Error messages for unsupported functions
- Documentation
- Integration tests

---

## Success Metrics

**Night 1 Done When**:
- ✅ 10 functions working (5 datetime + 5 string)
- ✅ Function registry architecture in place
- ✅ 10 passing test queries

**Night 2 Done When**:
- ✅ 20 functions working total
- ✅ All math functions tested
- ✅ Complex string operations working

**Night 3 Done When**:
- ✅ 23+ functions complete
- ✅ Comprehensive test suite
- ✅ Documentation updated
- ✅ Ready for Phase 1 completion!

---

## Implementation Order (Prioritized)

### Must-Have (Core 20)
1. DateTime: `datetime()`, `date()`, `timestamp()`
2. String: `toUpper()`, `toLower()`, `trim()`, `substring()`, `size()`
3. Math: `abs()`, `ceil()`, `floor()`, `round()`, `sqrt()`, `rand()`
4. String Advanced: `split()`, `replace()`, `reverse()`, `left()`
5. List: `head()`, `tail()`, `last()`
6. Type: `toInteger()`, `toFloat()`, `toString()`

### Nice-to-Have (Bonus)
- `right()` (last N chars)
- `ltrim()`, `rtrim()` (left/right trim)
- `range()` (number range)
- `sign()` (number sign)
- Property extraction: `datetime().year`, `.month`, `.day`

---

## Files to Create/Modify

### NEW Files
1. `brahmand/src/clickhouse_query_generator/function_registry.rs` (~200 lines)
   - Static function mappings
   - Lookup functions

2. `brahmand/src/clickhouse_query_generator/function_translator.rs` (~150 lines)
   - Translation logic
   - Argument transformations

### MODIFY Files
1. `brahmand/src/clickhouse_query_generator/to_sql.rs` (~10 lines change)
   - Update `ScalarFnCall` handling to use translator

2. `brahmand/src/clickhouse_query_generator/mod.rs` (~5 lines)
   - Add new module exports

### TEST Files
1. `tests/integration/test_neo4j_functions.py` (NEW)
   - Comprehensive function tests

2. Manual test queries via HTTP or Bolt

---

## Ready to Start?

**Tonight's Goal**: Get 10 functions working! 🎯

1. Create `function_registry.rs` with mappings
2. Create `function_translator.rs` with logic
3. Modify `to_sql.rs` to use translator
4. Test with simple queries
5. Commit progress: "feat: Add Neo4j datetime and string functions (10/20)"

Let's do this! 💪
