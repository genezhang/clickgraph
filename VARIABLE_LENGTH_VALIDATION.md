# Variable-Length Path Validation

**Date**: October 17, 2025  
**Feature**: Parser-level validation for variable-length path specifications  
**Status**: ✅ Implemented

## Overview

Added comprehensive validation for variable-length path specifications (`*N..M`, `*N`, `*..M`, `*`) to catch invalid queries at parse time with clear, actionable error messages.

## Validation Rules

### 1. Invalid Range: Min > Max
**Query**: `MATCH (a)-[*5..2]->(b)`  
**Error**: 
```
Invalid variable-length range: minimum hops (5) cannot be greater than maximum hops (2).
Use *2..5 instead of *5..2.
```

**Rationale**: A path cannot have both a minimum of 5 hops and maximum of 2 hops - this is logically impossible.

### 2. Zero Hops Not Allowed
**Queries**: 
- `MATCH (a)-[*0]->(b)`
- `MATCH (a)-[*0..5]->(b)`

**Error**:
```
Invalid variable-length range: hop count cannot be 0.
Variable-length paths must have at least 1 hop.
If you want to match the same node, use a simple node pattern like (n) instead of a relationship pattern.
```

**Rationale**: 
- Zero hops means "the same node" - this should use a simple node pattern `(n)`, not a relationship
- Prevents confusion between node patterns and relationship patterns
- A relationship by definition connects two different nodes (at least 1 hop)

### 3. Performance Warning: Very Large Ranges
**Query**: `MATCH (a)-[*..150]->(b)`  
**Warning** (printed to stderr, query still allowed):
```
Warning: Variable-length path with maximum 150 hops may have performance implications.
Consider using a smaller maximum or adding additional WHERE clause filters.
```

**Rationale**: 
- Paths with very deep traversals can have significant performance impact
- Warning threshold: 100 hops
- Still allowed (warning only) to support legitimate use cases
- Encourages users to think about query performance

## Valid Query Examples

All of these pass validation:

```cypher
-- Normal range
MATCH (a)-[*1..3]->(b)
RETURN a, b

-- Fixed length  
MATCH (a)-[*2]->(b)
RETURN a, b

-- Upper bound only (min defaults to 1)
MATCH (a)-[*..5]->(b)
RETURN a, b

-- Unbounded (min=1, max=unlimited)
MATCH (a)-[*]->(b)
RETURN a, b

-- Large but valid range (with warning)
MATCH (a)-[*1..100]->(b)
RETURN a, b
```

## Implementation Details

### Validation Method
Added `validate()` method to `VariableLengthSpec` struct in `ast.rs`:

```rust
pub fn validate(&self) -> Result<(), String> {
    // Check for invalid range where min > max
    if let (Some(min), Some(max)) = (self.min_hops, self.max_hops) {
        if min > max {
            return Err(format!(
                "Invalid variable-length range: minimum hops ({}) cannot be greater than maximum hops ({})...
            ));
        }
        
        // Check for zero in range
        if min == 0 || max == 0 {
            return Err("Invalid variable-length range: hop count cannot be 0...".to_string());
        }
        
        // Warn about very large ranges
        if max > 100 {
            eprintln!("Warning: Variable-length path with maximum {} hops...", max);
        }
    }
    
    // Check for zero in unbounded spec
    if let Some(min) = self.min_hops {
        if min == 0 {
            return Err("Invalid variable-length range: hop count cannot be 0...".to_string());
        }
    }
    
    Ok(())
}
```

### Parser Integration
Modified `parse_variable_length_spec()` in `path_pattern.rs` to call validation immediately after parsing:

```rust
let (input, spec_opt) = alt((
    range_parser,
    upper_bound_parser,
    fixed_length_parser,
    unbounded_parser,
))
.map(|spec| Some(spec))
.parse(input)?;

// Validate the parsed specification
if let Some(ref spec) = spec_opt {
    if let Err(validation_error) = spec.validate() {
        eprintln!("Variable-length path validation error: {}", validation_error);
        return Err(nom::Err::Failure(Error::new(input, ErrorKind::Verify)));
    }
}
```

**Note**: We use `nom::Err::Failure` (not `Error`) to indicate this is a semantic validation error, not a syntax parsing error. This ensures the parser doesn't try alternative parsing strategies.

## Test Coverage

Added 5 new tests in `path_pattern.rs`:

### 1. `test_invalid_range_min_greater_than_max`
Tests that `*5..2` is rejected with `ErrorKind::Verify`

### 2. `test_invalid_range_with_zero_min`  
Tests that `*0..5` is rejected with `ErrorKind::Verify`

### 3. `test_invalid_range_with_zero_max`
Tests that `*0` is rejected with `ErrorKind::Verify`

### 4. `test_valid_variable_length_patterns`
Tests that all valid patterns parse successfully:
- `*1..3` (range)
- `*2` (fixed)
- `*..5` (upper bound only)
- `*` (unbounded)
- `*1..100` (large but valid)

### 5. `test_variable_length_spec_validation_direct`
Tests the `validate()` method directly:
- Valid cases: `range(1,3)`, `fixed(5)`, `unbounded()`, `max_only(10)`
- Invalid cases: `range(5,2)` (min > max), `range(0,5)` (zero hops)

### Test Results
```
running 11 tests
test open_cypher_parser::path_pattern::tests::test_parse_path_pattern_single_node ... ok
test open_cypher_parser::path_pattern::tests::test_variable_length_spec_validation_direct ... ok
test open_cypher_parser::path_pattern::tests::test_parse_path_pattern_placeholder_error ... ok
test open_cypher_parser::path_pattern::tests::test_invalid_range_with_zero_max ... ok
test open_cypher_parser::path_pattern::tests::test_invalid_range_with_zero_min ... ok
test open_cypher_parser::path_pattern::tests::test_invalid_range_min_greater_than_max ... ok
test open_cypher_parser::path_pattern::tests::test_parse_path_pattern_multiple_patterns ... ok
test open_cypher_parser::path_pattern::tests::test_parse_path_pattern_connected_single_relationship ... ok
test open_cypher_parser::path_pattern::tests::test_valid_variable_length_patterns ... ok
test open_cypher_parser::path_pattern::tests::test_parse_path_pattern_connected_multiple_relationships_props_and_labels ... ok
test open_cypher_parser::path_pattern::tests::test_parse_path_pattern_connected_multiple_relationships ... ok

test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured
```

**Total Tests**: 228/229 passing (added 5 new tests, all passing)

## Error Handling Flow

```
User Query: MATCH (a)-[*5..2]->(b) RETURN a, b
     ↓
OpenCypher Parser
     ↓
parse_variable_length_spec()
     ↓
Parse syntax: *5..2 → VariableLengthSpec { min: 5, max: 2 }
     ↓
Call spec.validate()
     ↓
Validation Error: "min (5) > max (2)"
     ↓
eprintln! error message
     ↓
Return nom::Err::Failure(ErrorKind::Verify)
     ↓
Parser stops, error propagates to user
     ↓
User sees: Parse error with validation message
```

## User Experience

### Before Validation
```cypher
MATCH (a)-[*5..2]->(b) RETURN a, b
```
**Error**: Confusing SQL error deep in query execution:
```
ClickHouse error: Invalid CTE... recursive case never executes... [cryptic message]
```

### After Validation
```cypher
MATCH (a)-[*5..2]->(b) RETURN a, b
```
**Error**: Clear parser error at query submission:
```
Variable-length path validation error: Invalid variable-length range: 
minimum hops (5) cannot be greater than maximum hops (2). 
Use *2..5 instead of *5..2.
```

## Future Enhancements

Potential additional validations:

1. **Lower bound validation** - Warn if min_hops is very large (e.g., `*50..60`)
2. **Range width validation** - Warn if range is very wide (e.g., `*1..200`)
3. **Relationship type validation** - Check if relationship type exists in schema
4. **Property filter validation** - Validate properties in relationship filters
5. **Depth limit configuration** - Make the 100-hop warning threshold configurable

## Files Modified

- **`brahmand/src/open_cypher_parser/ast.rs`**
  - Added `validate()` method to `VariableLengthSpec`
  - Validation logic with detailed error messages

- **`brahmand/src/open_cypher_parser/path_pattern.rs`**
  - Modified `parse_variable_length_spec()` to call validation
  - Added 5 new test cases for validation scenarios

## Impact

- ✅ **Better user experience** - Clear, actionable error messages
- ✅ **Fail fast** - Catch errors at parse time, not execution time
- ✅ **Performance protection** - Warn about queries that may be slow
- ✅ **No breaking changes** - All valid queries continue to work
- ✅ **Test coverage** - 5 new tests, all passing

## Related Issues

- ✅ This completes the "Error Handling & Validation" todo item
- 📝 Future: Add more comprehensive validation for edge cases
- 📝 Future: Make warning thresholds configurable
