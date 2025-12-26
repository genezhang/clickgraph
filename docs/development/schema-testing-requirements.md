# Schema Testing Requirements

## Problem Statement

ClickGraph supports multiple schema patterns that require different SQL generation strategies. Historically, changes optimized for one schema type have broken others (notably: denormalized VLP breakage on Dec 22, 2025).

## Schema Types and Their Characteristics

### 1. Traditional Schema
- **Node tables**: Separate physical tables (e.g., `users`)
- **Node ID**: Physical column in node table (`users.user_id`)
- **VLP Strategy**: Use `node_schema.node_id.column()` for CTE generation

### 2. Denormalized Schema  
- **Node tables**: Virtual (point to edge table)
- **Node ID**: Logical property name (e.g., `Airport.code = "code"`)
- **Physical ID**: In edge table (e.g., `flights.Origin`)
- **VLP Strategy**: Use relationship columns (`from_id`/`to_id`) for CTE generation
- **Critical**: `is_denormalized = true` flag

### 3. FK-Edge Schema
- **Node tables**: Physical tables
- **Edge tables**: Foreign keys reference node tables
- **VLP Strategy**: Similar to traditional, but with FK constraints

## Testing Requirements

### Rule 1: Multi-Schema Test Coverage

**Any change to these modules MUST test ALL schema types:**
- `src/render_plan/cte_extraction.rs` (VLP CTE generation)
- `src/render_plan/plan_builder.rs` (SQL JOIN generation)
- `src/clickhouse_query_generator/variable_length_cte.rs` (CTE rendering)
- `src/query_planner/analyzer/*` (Query planning)

**Test Files:**
- Traditional: `tests/integration/test_variable_paths.py`
- Denormalized: `tests/integration/test_denormalized_edges.py::TestDenormalizedVariableLengthPaths`
- FK-Edge: (add specific tests)

### Rule 2: No xfail on Critical Features

**NEVER mark these features as xfail:**
- Variable-length paths (VLP) - core feature
- OPTIONAL MATCH - advertised feature
- Basic relationship traversal - fundamental functionality

**If a test fails:**
1. Fix immediately OR
2. Revert the breaking change OR
3. Create issue and mark PR as draft until fixed

### Rule 3: Schema-Specific Test Markers

Use pytest markers to ensure coverage:

```python
@pytest.mark.schema_type("traditional")
def test_vlp_traditional():
    ...

@pytest.mark.schema_type("denormalized")  
def test_vlp_denormalized():
    ...
```

Run: `pytest -m schema_type` to validate all schema types tested

### Rule 4: CI/CD Integration

Add to CI pipeline:
```bash
# Must pass BOTH before merge
pytest tests/integration/test_variable_paths.py -v
pytest tests/integration/test_denormalized_edges.py::TestDenormalizedVariableLengthPaths -v
```

## Code Review Checklist

When reviewing PRs that touch VLP or relationship traversal:

- [ ] Tests include traditional schema example
- [ ] Tests include denormalized schema example  
- [ ] No new xfail markers on critical features
- [ ] Comments explain schema-specific logic
- [ ] `is_denormalized` checks present where needed

## Historical Incidents

### Dec 22, 2025: Denormalized VLP Breakage
- **Commit**: 6fc1506
- **Issue**: Changed node ID column selection without checking `is_denormalized`
- **Impact**: 3 denormalized VLP tests marked xfail
- **Root Cause**: Only tested traditional schemas, missed denormalized
- **Fix**: Check `is_denormalized` flag before using `node_schema.node_id`
- **Lesson**: Multi-schema testing is MANDATORY

## Future Improvements

1. **Type-Level Distinction**: Consider separate types for TraditionalSchema vs DenormalizedSchema
2. **Property Testing**: Generate random queries for both schema types
3. **Schema Migration Tests**: Test upgrading between schema versions
4. **Performance Benchmarks**: Track query performance across schema types

## References

- [Variable-Length Paths Guide](../variable-length-paths-guide.md)
- [Denormalized Edge Tables](../denormalized-edge-tables.md)
- [Schema Reference](../schema-reference.md)
