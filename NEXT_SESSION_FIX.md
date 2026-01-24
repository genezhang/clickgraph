# Next Session: Denormalized Edge Property Alias Mapping

**Created**: January 23, 2026  
**Priority**: HIGH (quick fix - 1 failing test)  
**Estimated Time**: 30-60 minutes

## Summary

Fixed 2 of 3 integration test failures in this session:
- ✅ Timeout in 3-hop queries (test data corruption)
- ✅ Wildcard expansion bug in GROUP BY queries
- ⚠️ Denormalized edge JOINs (partially - duplicate eliminated, alias mapping needed)

**Current Status**: 232/273 tests passing (85% pass rate)

## The Remaining Problem

**Test**: `test_with_cross_table[social_benchmark]`  
**Query**:
```cypher
MATCH (a:User)-[r1:FOLLOWS]->(b)
WITH a, b
MATCH (c:User)-[r2:AUTHORED]->(d)
WHERE a.user_id = c.user_id
RETURN a, b, d LIMIT 10
```

**Error**:
```
Code: 47. DB::Exception: Unknown expression identifier `d` in scope WITH
```

**Root Cause**:
- Schema defines AUTHORED as denormalized: `posts_bench` table serves as both edge AND target node
- Fix #3 eliminated the duplicate JOIN to posts_bench, so `d` alias is never registered
- But RETURN clause still tries to access `d.*` properties
- Need to map `d` references to `r2` (the edge table which IS the target node)

## The Fix

**When denormalized edge detected** (in `join_builder.rs` line 1874-1901):
- Skip the second JOIN (✅ already done)
- Register an alias mapping: `d → r2`
- Store mapping in context so RETURN clause rendering can resolve `d` properties to `r2` columns

**Implementation Pattern**:

1. **Where**: `src/render_plan/join_builder.rs` (where we added denormalized check)
2. **What**: Store mapping in a new rendering context:
   ```rust
   if end_table != rel_table {
       joins.push(Join { /* normal case */ });
   } else {
       // Denormalized edge: map end node alias to edge alias
       // Example: d → r2
       store_alias_mapping(
           &graph_rel.right_connection,  // "d" (target node alias)
           &graph_rel.alias              // "r2" (edge alias)
       );
   }
   ```

3. **Context Storage**: Add to `RenderPlanBuilder` context or task-local storage
   - Similar to `MULTI_TYPE_VLP_ALIASES` pattern already used in codebase
   - Or: `DENORMALIZED_EDGE_ALIASES: HashMap<String, String>`

4. **Resolution**: In property rendering (`select_builder.rs`), check mapping before accessing properties:
   ```rust
   if let Some(mapped_alias) = get_denormalized_alias_mapping("d") {
       // Use mapped_alias "r2" instead of "d"
   }
   ```

## Related Files

- ✅ `src/render_plan/join_builder.rs` - Already has denormalized check
- 📝 Need to modify: `src/render_plan/select_builder.rs` - Property expansion
- 📝 May need: `src/render_plan/properties_builder.rs` - Property resolution
- ✅ Reference: `src/render_plan/mod.rs` - Task-local storage pattern (MULTI_TYPE_VLP_ALIASES)

## Test Verification

Run after fix:
```bash
cargo test --lib                  # Should still be 787/787 ✅
python3 -m pytest tests/integration/matrix/test_comprehensive.py::TestWithChaining::test_with_cross_table[social_benchmark] -xvs
# Expected: PASSED

python3 -m pytest tests/integration/matrix/test_comprehensive.py -v --tb=no
# Expected: 233 PASSED (up from 232)
```

## Implementation Notes

- **Pattern**: Similar to `MULTI_TYPE_VLP_ALIASES` task-local storage in `render_plan/mod.rs`
- **Lifetime**: Mapping only needs to live for duration of single query execution
- **Safety**: Use task-local! to ensure async-safety
- **Cleanup**: Clear mapping after each query renders (in render_plan main function)

## Success Criteria

- ✅ All 787 unit tests pass
- ✅ `test_with_cross_table[social_benchmark]` passes
- ✅ Full integration suite: 233/273 passing (85%+)
- ✅ No new failures introduced

---

**Branch**: `fix/integration-test-failures`  
**PR Ready**: Yes - ready to merge after this quick fix
