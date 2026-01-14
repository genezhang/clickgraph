# PR: Fix Denormalized Node Property Lookup

## Summary
Fixes bug where denormalized node patterns with Union wrappers failed to expand properties in RETURN clause, causing "No select items found" error.

**Branch**: `fix/denormalized-node-return` → `main`

## Problem
When querying denormalized schemas (e.g., ontime_flights), queries like:
```cypher
MATCH (a:Airport)-[r:FLIGHT]->(b:Airport) RETURN a
```
Failed with "No select items found" because property expansion didn't handle Union wrappers around ViewScan nodes.

## Solution

### 1. Union Handling in Property Expansion (c3a5ba5)
**File**: `src/render_plan/plan_builder.rs`
- Enhanced `get_properties_with_table_alias()` to traverse Union wrapper
- Now correctly expands properties from `from_node_properties` and `to_node_properties`
- Result: `RETURN a` now generates correct SELECT items

### 2. Union Handling in ID Column Lookup (7ef60a8)
**File**: `src/render_plan/plan_builder.rs`
- Enhanced `find_id_column_for_alias()` to handle Union branches
- Added documentation about limitation: branches may have different ID columns
- Example: OriginAirportID (from_node) vs DestAirportID (to_node)

### 3. Position-Aware ID Column API (6e42361)
**File**: `src/render_plan/plan_builder.rs`
- Added `find_id_column_for_from_node()` - always returns from_position ID
- Added `find_id_column_for_to_node()` - always returns to_position ID
- Trait method signatures with deprecation warning for old method
- **Design Pattern**: Unified API that works across all schema variations
  - Traditional schemas: both methods return same value (node_id)
  - Denormalized schemas: different values (OriginAirportID vs DestAirportID)
  - No call-site conditionals needed - API handles variation internally

## Testing

**Manual Testing**:
```bash
# Query that previously failed now works
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH (a:Airport)-[r:FLIGHT]->(b:Airport) RETURN a, b LIMIT 5"}'

# Generated SQL correctly uses:
# - OriginAirportID, Origin (from_node properties)
# - DestAirportID, Dest (to_node properties)
```

**Validation**:
- ✅ Properties expanded correctly from Union branches
- ✅ Both from_node and to_node properties accessible
- ✅ No regression on traditional schemas (position-aware methods return same value)

## Architecture Impact

**Positive**:
- Introduces position-aware API pattern (parallels existing JoinStrategy enum approach)
- Eliminates need for scattered `is_denormalized` checks at call sites
- Encodes schema variation handling in method signatures

**Future Work**:
- Position-aware methods not yet utilized in codebase (ready for adoption)
- Serves as foundation for larger refactoring (see schema_consolidation_analysis.md)

## Files Changed
- `src/render_plan/plan_builder.rs`: 3 methods enhanced, 2 methods added

## Commits
1. `c3a5ba5` - fix: Handle Union wrapper in denormalized node property lookup
2. `7ef60a8` - docs: clarify limitation of find_id_column_for_alias for Union nodes
3. `6e42361` - feat: add position-aware ID column lookup methods

## Risk Assessment
- **Low Risk**: Changes isolated to property expansion logic
- **Backwards Compatible**: Traditional schemas unaffected
- **Well-Scoped**: No changes to AST, parsing, or query planning

## Next Steps After Merge
Create separate branch for Phase 1 refactoring:
- `refactor/schema-consolidation-phase1` 
- Focus: Eliminate scattered `is_denormalized` checks using position-aware patterns
- Reference: notes/schema_consolidation_analysis.md
