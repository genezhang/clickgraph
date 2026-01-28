# Alternate Relationship Types (`[:TYPE1|TYPE2]`)

*Completed: October 21, 2025*

## Summary
Implemented support for alternate relationship types in Cypher patterns, allowing queries like `MATCH (a)-[:FOLLOWS|FRIENDS_WITH]->(b)` to match relationships of multiple types using UNION SQL generation.

## How It Works

### Parser Changes
- Extended `RelationshipPattern` AST to store multiple relationship labels as `Vec<&'a str>`
- Updated nom parser in `path_pattern.rs` to handle `[:TYPE1|TYPE2|TYPE3]` syntax
- Maintains backward compatibility with single relationship types

### Logical Plan Changes
- Added `labels: Option<Vec<String>>` field to `GraphRel` struct
- Updated all `GraphRel` construction sites to preserve relationship labels from AST
- Modified `match_clause.rs` and optimizer passes to handle multiple labels

### SQL Generation Changes
- Added `rel_types_to_table_names()` helper function to map relationship types to table names
- Modified `extract_ctes_with_context()` in `plan_builder.rs` to detect multiple relationship types
- Generates UNION ALL CTE for multiple types: `SELECT from_node_id, to_node_id FROM table1 UNION ALL SELECT from_node_id, to_node_id FROM table2`
- Single relationship types continue to work without UNION generation

## Key Files
- `open_cypher_parser/ast.rs` - AST structure for multiple labels
- `open_cypher_parser/path_pattern.rs` - Parser for `[:TYPE1|TYPE2]` syntax
- `query_planner/logical_plan/mod.rs` - GraphRel struct with labels field
- `query_planner/logical_plan/match_clause.rs` - GraphRel construction with labels
- `render_plan/plan_builder.rs` - UNION CTE generation logic
- `render_plan/tests/multiple_relationship_tests.rs` - Test coverage

## Design Decisions
- **UNION ALL vs OR**: Chose UNION ALL for better performance and cleaner SQL generation
- **CTE approach**: Used CTE wrapper for multiple relationships to maintain consistent query structure
- **Backward compatibility**: Single relationship types unchanged, Option<Vec<String>> for labels
- **Table mapping**: Leverages existing `rel_type_to_table_name()` function for schema-aware mapping

## Gotchas
- Requires relationship tables to have consistent `from_node_id`/`to_node_id` column structure
- UNION CTE naming uses pattern `rel_{left_alias}_{right_alias}` for uniqueness
- Only works for single-hop relationships; variable-length paths with multiple types not yet supported

## Limitations
- Variable-length paths with multiple relationship types not implemented
- Assumes all relationship tables have identical column structure
- No optimization for common relationship properties across types

## Future Work
- Support for variable-length paths: `(a)-[:TYPE1|TYPE2*1..3]->(b)`
- Property filtering across multiple relationship types
- Query optimization for UNION queries (e.g., merge identical subqueries)

## Test Coverage
- `test_multiple_relationship_types_union()`: Verifies UNION CTE generation for multiple types
- `test_single_relationship_type_no_union()`: Ensures backward compatibility
- All 44 render plan tests passing
- Integration tests with actual ClickHouse execution pending
- Backward compatibility for single types
- Integration tests with actual SQL execution


