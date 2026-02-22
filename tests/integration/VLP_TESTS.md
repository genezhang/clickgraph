## VLP Fix Regression Tests

This document describes the tests added to prevent regression of the VLP (Variable-Length Path) fix.

### ClickGraph Integration Tests

**File**: `tests/integration/test_vlp_relationship_return.py`

Tests cover:
1. **test_single_type_vlp_depth_1_return_r** - Single-type VLP depth=1 with RETURN r (main regression test)
2. **test_single_type_vlp_depth_2_return_r** - Single-type VLP depth=2 with RETURN r
3. **test_single_type_vlp_return_nodes_only** - Single-type VLP returning nodes only
4. **test_vlp_path_variable_length** - VLP with path variable and length function
5. **test_regular_relationship_properties** - Baseline test for regular (non-VLP) property access
6. **test_single_type_vlp_with_type_info** - Documents that VLP provides type info but not properties
7. **test_single_type_vlp_different_edge_type** - Single-type VLP with AUTHORED edge
8. **test_multi_type_vlp_same_target_type** - Multi-type VLP with same target type

### GraphRAG Service Tests

**File**: `tests/unit_tests.rs` (updated)

Additional tests for VLP query generation:
- `test_generate_expansion_query_in_multi_hop` - In-direction multi-hop VLP
- `test_generate_expansion_query_multi_hop` - Out-direction multi-hop VLP
- `test_generate_expansion_query_both_direction` - Both-direction VLP

### Running Tests

```bash
# ClickGraph integration tests (requires ClickGraph + ClickHouse running)
cd clickgraph
pytest tests/integration/test_vlp_relationship_return.py -v

# GraphRAG service unit tests
cd graphrag-service
cargo test

# ClickGraph cargo tests
cd clickgraph
cargo test --release
```

### Regression Prevention

The fix changed the condition in `src/render_plan/select_builder.rs` from:
```rust
if labels.len() > 1 && uses_cte {
```
to:
```rust
if uses_cte {
```

If this condition is accidentally changed back, tests 1 and 2 in 
`test_vlp_relationship_return.py` will fail with errors like:
```
Unknown expression identifier `r.follower_id`
```

### Known Limitation

Single-type VLP does NOT return edge properties. The `rel_properties` column is only 
available for multi-type VLP or pattern_combinations.

```cypher
# Works - regular relationship
MATCH (u)-[r:FOLLOWS]->(n) RETURN r.created_at

# Does NOT work - single-type VLP
MATCH (u)-[r:FOLLOWS*1..2]->(n) RETURN r.created_at

# Works - VLP returns type info
MATCH (u)-[r:FOLLOWS*1..2]->(n) RETURN r.type, r.start_id, r.end_id
```
