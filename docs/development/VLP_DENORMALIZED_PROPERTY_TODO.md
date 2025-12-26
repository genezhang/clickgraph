# Denormalized VLP Property Handling - TODO

**Created**: December 25, 2025  
**Status**: Root cause fixed, properties not yet in CTE SQL  
**Priority**: HIGH - Core feature for no-ETL value proposition

---

## Current Status (Dec 25, 2025)

### ✅ Completed
1. **Root Cause Fixed**: Node ID column selection now checks `is_denormalized` flag
   - Traditional schemas: Use `node_schema.node_id` (e.g., `user_id`)
   - Denormalized schemas: Use relationship columns (e.g., `Origin`, `Dest`)
   - Location: `src/render_plan/cte_extraction.rs` lines 967-1020
   - Commit: d557e40

2. **Schema Lookup Fixed**: Handles both `table` and `database.table` formats
   - Location: `src/render_plan/cte_extraction.rs` lines 1287-1325
   - Now correctly matches `test_integration.flights` with `flights`

3. **Property Extraction Working**: 6 properties extracted from schema
   - From node: `code → Origin`, `state → OriginState`, `city → OriginCityName`
   - To node: `code → Dest`, `state → DestState`, `city → DestCityName`
   - Debug output confirms: "Final all_denorm_properties count: 6"

4. **Properties Passed to Generator**: Constructor receives 6 properties
   - Location: `src/clickhouse_query_generator/variable_length_cte.rs` line 254

5. **Test Results**: 14/18 passing (78%)
   - ✅ All basic denormalized queries working
   - ❌ 4 VLP with properties tests xfailed

### ❌ Not Working Yet
Properties are extracted (6) but NOT appearing in generated CTE SQL (0).

**Current SQL Output**:
```sql
WITH RECURSIVE vlp_cte3 AS (
  SELECT 
    rel.Origin as start_id,
    rel.Dest as end_id,
    1 as hop_count,
    -- ❌ MISSING: OriginCityName, DestCityName, etc.
  FROM test_integration.flights AS rel
)
```

**Expected SQL**:
```sql
WITH RECURSIVE vlp_cte3 AS (
  SELECT 
    rel.Origin as start_id,
    rel.Dest as end_id,
    rel.OriginCityName as start_city,    -- ✅ Should be here
    rel.DestCityName as end_city,        -- ✅ Should be here
    1 as hop_count,
    ...
  FROM test_integration.flights AS rel
)
```

---

## The Mystery: Where Do Properties Disappear?

### Property Flow (What We Know)
```
Schema (6 properties)
    ↓
Extractor extracts (6 properties) ✅
    ↓
Constructor receives (6 properties) ✅
    ↓
generate_denormalized_base_case() called
    ↓
Property loop (lines 1972-2000)
    ↓
??? (Properties should be added to select_items)
    ↓
SQL output (0 properties) ❌
```

### Key Questions
1. Is `self.properties` empty when `generate_denormalized_base_case()` is called?
2. Is `map_denormalized_property()` returning `Err` silently?
3. Are properties added to `select_items` but then lost in SQL generation?
4. Is there a different code path for denormalized that skips property handling?

---

## User's Key Architectural Insight

**From Dec 25 session**: 
> "wait - if the CTE provides the origin and dest, we don't need join in the main SELECT, just pick the right properties..."

**This is CORRECT!** Denormalized optimization should:
- ✅ Carry properties **IN** the CTE (efficient, no duplicated data)
- ❌ NOT use JOINs in final SELECT (defeats denormalized optimization)

### Current Implementation (Wrong Approach)
We've been adding JOINs for denormalized endpoints:
```rust
// src/render_plan/plan_builder.rs lines 11760-11822
// Removed `if !start_is_denorm` check - now adds JOINs for ALL
```

This is a **band-aid fix** that defeats the purpose of denormalized schemas!

### Correct Implementation (Systematic)
Properties should be:
1. Extracted from schema ✅ (done)
2. Added to CTE SELECT clause ❌ (broken)
3. Carried through recursive case ❌ (broken)
4. Accessed directly from CTE (no JOINs) ❌ (not implemented)

---

## Code Locations

### Files to Focus On

1. **src/clickhouse_query_generator/variable_length_cte.rs**
   - Line 254: `new_denormalized()` constructor (receives properties)
   - Lines 1945-2040: `generate_denormalized_base_case()` (property loop exists but not working)
   - Lines 2045-2115: `generate_denormalized_recursive_case()` (should carry properties)
   - Lines 690-730: `map_denormalized_property()` helper (maps logical→physical)

2. **src/render_plan/cte_extraction.rs**
   - Lines 1287-1325: Property extraction (working ✅)
   - Passes `all_denorm_properties` to generator

3. **src/render_plan/plan_builder.rs**
   - Lines 11760-11822: Endpoint JOIN generation
   - Currently adds JOINs for denormalized (wrong approach)
   - Should be reverted once CTE properties working

### Pattern Comparison Needed

Compare these implementations to understand why one works and another doesn't:

**FK-Edge Pattern** (WORKS - adds properties to CTE):
```rust
// Lines 1695-1700 in variable_length_cte.rs
for prop in &self.properties {
    if prop.cypher_alias == self.start_cypher_alias {
        select_items.push(format!(
            "{}.{} as start_{}",
            self.start_node_alias, prop.column_name, prop.alias
        ));
    }
}
```

**Denormalized Pattern** (BROKEN - same loop but doesn't add to SQL):
```rust
// Lines 1972-2000 in variable_length_cte.rs
for prop in &self.properties {
    if prop.cypher_alias == self.start_cypher_alias {
        if let Ok(physical_col) = self.map_denormalized_property(&prop.alias, true) {
            select_items.push(format!(
                "{}.{} as {}",
                self.relationship_alias, physical_col, physical_col
            ));
        }
    }
}
```

**Question**: Why does FK-edge work but denormalized doesn't when they use the same pattern?

---

## Next Session Goals

### Phase 1: Debug (1-2 hours)
1. Add debug output in `generate_denormalized_base_case()`:
   ```rust
   eprintln!("🔧 BASE_CASE: self.properties.len() = {}", self.properties.len());
   for prop in &self.properties {
       eprintln!("🔧 BASE_CASE: prop = {:?}", prop);
       if prop.cypher_alias == self.start_cypher_alias {
           let result = self.map_denormalized_property(&prop.alias, true);
           eprintln!("🔧 BASE_CASE: map_denormalized_property result = {:?}", result);
       }
   }
   eprintln!("🔧 BASE_CASE: final select_items = {:?}", select_items);
   ```

2. Run test and check logs to identify exact failure point

3. Compare with FK-edge pattern execution to see differences

### Phase 2: Fix (2-3 hours)
Based on debug findings:
- **If properties empty**: Fix property passing between constructor and method
- **If map fails**: Fix from_properties/to_properties lookup
- **If added but lost**: Fix SQL string generation

### Phase 3: Cleanup (1 hour)
- Remove unnecessary JOINs from plan_builder.rs
- Remove debug eprintln! statements
- Update tests (remove xfail markers)

### Phase 4: Verify (1 hour)
- Run all 18 denormalized tests (target: 18/18 ✅)
- Run OnTime benchmark VLP queries
- Run full test suite to ensure no regressions

---

## Testing Commands

```bash
# Single test with debug output
RUST_LOG=debug cargo run --bin clickgraph 2>&1 | tee /tmp/clickgraph.log
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH p=(a:Airport)-[*1..2]->(b:Airport) WHERE a.code=\"LAX\" RETURN a.city, b.city","sql_only":true}' | jq

# Check logs
grep "🔧 BASE_CASE:" /tmp/clickgraph.log

# Run test suite
pytest tests/integration/test_denormalized_edges.py::TestDenormalizedVariableLengthPaths -v

# OnTime benchmark
cd benchmarks/ontime_flights
./run_queries.sh
```

---

## Prevention Measures (Already Implemented)

1. **Documentation**: `docs/development/schema-testing-requirements.md`
2. **Meta Tests**: `tests/meta/test_schema_coverage.py`
3. **Pre-commit Hook**: `scripts/hooks/pre-commit.sh`

These will prevent future breakage of denormalized VLP.

---

## Related Documents

- **Incident Report**: `KNOWN_ISSUES.md` lines 135-170
- **Fix Plan**: `/tmp/denorm_vlp_fix_plan.md` (session artifact)
- **Session Findings**: `SESSION_DEC_25_2025_FINDINGS.md`
- **Testing Requirements**: `docs/development/schema-testing-requirements.md`
- **VLP Guide**: `docs/variable-length-paths-guide.md`

---

## Key Takeaways

1. **Root cause is fixed** - node ID selection now works for both schema types
2. **Properties are being extracted** - no more schema lookup failures
3. **The mystery is small** - just need to debug why property loop doesn't add to SQL
4. **User insight is crucial** - properties should be IN CTE, not via JOINs
5. **Path forward is clear** - debug → fix → cleanup → verify

**Estimated Time to Complete**: 5-7 hours (1 session)

**Critical for**: OnTime benchmark, no-ETL value proposition, denormalized schema support
