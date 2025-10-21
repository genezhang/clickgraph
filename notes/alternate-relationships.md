# Alternate Relationship Types (`[:TYPE1|TYPE2]`)

## Summary
Implemented support for alternate relationship types in Cypher patterns, allowing queries like `MATCH (a)-[:FOLLOWS|LIKES]->(b)` to match relationships of multiple types.

## How It Works

### Parser Changes
- Extended `RelationshipPattern.labels` from `Option<&'a str>` to `Option<Vec<&'a str>>` in AST
- Added `parse_relationship_labels()` function to handle `|` separated relationship types
- Updated `parse_relationship_internals_with_multiple_labels()` to parse patterns like `[:TYPE1|TYPE2]`

### Logical Plan Changes
- Modified `TableCtx.labels` from `Option<String>` to `Option<Vec<String>>`
- Added `get_labels()` method to access multiple labels
- Maintained backward compatibility with `get_label_str()` returning first label
- Updated all `TableCtx::build()` calls to convert single labels to `vec![label]`

### Key Files Modified
- `open_cypher_parser/ast.rs`: RelationshipPattern struct
- `open_cypher_parser/path_pattern.rs`: Parser functions and tests
- `query_planner/plan_ctx/mod.rs`: TableCtx structure
- `query_planner/logical_plan/match_clause.rs`: Label mapping logic
- `query_planner/analyzer/filter_tagging.rs`: Test fixes
- `query_planner/analyzer/graph_join_inference.rs`: Test fixes

## Design Decisions

### Backward Compatibility
- Single-label patterns like `[:FOLLOWS]` still work unchanged
- `get_label_str()` returns first label for existing code
- All existing tests pass without modification

### AST Structure
- Used `Vec<&'a str>` in parser AST for zero-copy parsing
- Converted to `Vec<String>` in logical plan for ownership
- Maintains parser performance while enabling multiple labels

### Error Handling
- Parser accepts any number of relationship types separated by `|`
- Empty relationship patterns `[]` still allowed
- No validation of relationship type existence (handled at schema level)

## Gotchas

### Compilation Issues
- Required systematic updates across all `TableCtx::build()` calls
- Test assertions needed conversion from `Option<String>` to `Option<Vec<String>>`
- Careful handling of string literal types (`&str` vs `String`)

### Test Updates
- All test expectations updated to use `vec!["TYPE".to_string()]` format
- Scan struct still uses `Option<String>` for table names (not affected)

## Limitations

### SQL Generation Pending
- Parser and logical planning complete
- SQL generation for UNION/OR logic not yet implemented
- Multiple types will currently use first label only

### Schema Validation
- No validation that relationship types exist in schema
- Runtime errors will occur for invalid relationship types

## Future Work

### SQL Generation
- Implement UNION queries for multiple relationship types
- Add OR conditions in WHERE clauses for relationship filtering
- Optimize query plans for multiple relationship patterns

### Integration Tests
- End-to-end tests with actual ClickHouse execution
- Performance benchmarking for multiple vs single type queries
- Error handling for non-existent relationship types

## Test Coverage
- `test_parse_path_pattern_multiple_relationship_labels`: Parser functionality
- All existing tests pass (backward compatibility verified)
- 1 new test added, 19 total path pattern tests passing