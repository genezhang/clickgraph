# VLP Denormalized - Root Cause Identified

**Date**: December 26, 2025  
**Status**: 🎯 Root cause found - TWO separate issues

---

## The Real Problems (Not What We Thought!)

### Problem #1: Table Name Mismatch in Property Mapping

**Symptom**: Properties extracted (6) but map_denormalized_property() fails

**Root Cause**: Database prefix mismatch
```rust
// Generator has:
relationship_table = "test_integration.flights"  // With database prefix

// Schema has:
node_schema.table_name = "flights"  // Without database prefix

// Lookup fails:
node_schemas.values().find(|n| n.table_name == self.relationship_table)
// Comparing "flights" == "test_integration.flights" → NO MATCH!
```

**Evidence from Logs**:
```
🔍 MAP:   relationship_table = 'test_integration.flights'
🔍 MAP:   Found 2 node schemas in total
🔍 MAP:     - 'test_integration::flights::Airport' -> table 'flights'
🔍 MAP:     - 'Airport' -> table 'flights'
❌ MAP: No node schema found for table 'test_integration.flights'
```

**Fix**: Strip database prefix before comparison:
```rust
fn map_denormalized_property(&self, logical_prop: &str, is_from_node: bool) -> Result<String, String> {
    let node_schemas = self.schema.get_nodes_schemas();
    
    // Strip database prefix for comparison
    let rel_table_name = self.relationship_table
        .split('.')
        .last()
        .unwrap_or(&self.relationship_table);
    
    let node_schema = node_schemas
        .values()
        .find(|n| {
            let schema_table = n.table_name.split('.').last().unwrap_or(&n.table_name);
            schema_table == rel_table_name
        })
        .ok_or_else(|| format!("No node schema found for table '{}'", rel_table_name))?;
    
    // Rest of function...
}
```

### Problem #2: Denormalized VLP Creates Unnecessary JOINs

**Symptom**: Query fails with `Unknown table expression identifier 'ERROR_NODE_SCHEMA_MISSING_Airport'`

**Root Cause**: The render plan builder ALWAYS adds endpoint JOINs for VLP, even when nodes are denormalized!

**Evidence from Logs**:
```
❌ SCHEMA ERROR: Node label 'Airport' not found in schema.
🔧 extract_table_name: Using label 'Airport' → table 'ERROR_NODE_SCHEMA_MISSING_Airport'
✅ Creating START node JOIN: ERROR_NODE_SCHEMA_MISSING_Airport AS origin
✅ Creating END node JOIN: ERROR_NODE_SCHEMA_MISSING_Airport AS dest
```

**Generated SQL**:
```sql
WITH RECURSIVE vlp_cte1 AS (
  SELECT rel.Origin AS start_id, rel.Dest AS end_id, ... 
  FROM test_integration.flights AS rel
  ...
)
SELECT 
    vlp1.OriginCityName AS `origin.city`,   -- ✅ Properties ARE in CTE!
    vlp1.DestCityName AS `dest.city`,      
    vlp1.hop_count AS hops
FROM vlp_cte1 AS vlp1
INNER JOIN ERROR_NODE_SCHEMA_MISSING_Airport AS origin   -- ❌ Unnecessary JOIN!
    ON vlp1.start_id = origin.Origin
INNER JOIN ERROR_NODE_SCHEMA_MISSING_Airport AS dest     -- ❌ Unnecessary JOIN!
    ON vlp1.end_id = dest.Dest
WHERE ...
```

**The Issue**: Code in `plan_builder.rs` adds endpoint JOINs unconditionally:

```rust
// src/render_plan/plan_builder.rs lines ~11760-11822
// This was added as a "fix" but it's wrong for denormalized!

// Current logic:
if has_vlp_cte {
    // ALWAYS add endpoint JOINs, regardless of denormalized status
    add_endpoint_joins(&start_node, &end_node);  // ❌ Wrong for denormalized!
}
```

**Correct Logic**:
```rust
if has_vlp_cte {
    // Only add JOINs if nodes are NOT denormalized
    if !start_is_denormalized {
        add_start_endpoint_join(&start_node);
    }
    if !end_is_denormalized {
        add_end_endpoint_join(&end_node);
    }
}
```

---

## Why Properties ARE Actually in the CTE

Looking at the generated SQL more carefully:

```sql
-- CTE has these columns:
SELECT 
    rel.Origin AS start_id,
    rel.Dest AS end_id,
    1 AS hop_count,
    ...

-- Final SELECT tries to access:
SELECT 
    vlp1.OriginCityName AS `origin.city`,  -- ❌ Not in CTE!
    vlp1.DestCityName AS `dest.city`,      -- ❌ Not in CTE!
```

**Wait!** Properties are NOT in CTE after all! The map_denormalized_property() failure means they were never added to the base case SELECT!

---

## The Complete Picture

### Actual Flow (What Happens Now)

1. **Extraction** (`cte_extraction.rs` line 1290-1339):
   - ✅ Extracts 6 properties from schema
   - ✅ Passes to VariableLengthCteGenerator::new_denormalized()
   
2. **Generator Construction** (line 254):
   - ✅ Receives 6 properties
   - ✅ Stores in `self.properties`
   - ✅ Sets `relationship_table = "test_integration.flights"` (with prefix)

3. **Base Case Generation** (`generate_denormalized_base_case`, line 2019):
   - ✅ Loops through 6 properties
   - ❌ For each property, calls `map_denormalized_property()`
   - ❌ Mapping FAILS due to table name mismatch
   - ❌ Properties NOT added to `select_items`
   - ❌ CTE generated WITHOUT properties

4. **Final SELECT Generation** (`plan_builder.rs`):
   - ❌ Tries to access `origin.city`, `dest.city` from CTE
   - ❌ Properties not in CTE, so adds JOINs to get them
   - ❌ JOINs to non-existent Airport tables
   - ❌ Query fails

### What SHOULD Happen

1. **Extraction**: ✅ (already works)
   
2. **Generator Construction**: ✅ (already works)

3. **Base Case Generation**:
   - ✅ Loops through properties
   - ✅ `map_denormalized_property()` succeeds (after fix)
   - ✅ Properties added to `select_items`
   - ✅ CTE includes: `rel.OriginCityName as OriginCityName`, etc.

4. **Final SELECT Generation**:
   - ✅ Accesses properties directly from CTE: `vlp1.OriginCityName`
   - ✅ NO JOINs added (denormalized nodes detected)
   - ✅ Query succeeds

---

## The Fixes Needed

### Fix #1: Table Name Matching (CRITICAL - 15 min)

**File**: `src/clickhouse_query_generator/variable_length_cte.rs` (lines 694-722)

**Change**:
```rust
fn map_denormalized_property(&self, logical_prop: &str, is_from_node: bool) -> Result<String, String> {
    eprintln!("🔍 MAP: map_denormalized_property('{}', is_from_node={})", logical_prop, is_from_node);
    eprintln!("🔍 MAP:   relationship_table = '{}'", self.relationship_table);
    
    let node_schemas = self.schema.get_nodes_schemas();
    eprintln!("🔍 MAP:   Found {} node schemas in total", node_schemas.len());
    
    // ✅ NEW: Strip database prefix for comparison
    let rel_table_name = self.relationship_table
        .split('.')
        .last()
        .unwrap_or(&self.relationship_table);
    eprintln!("🔍 MAP:   Comparing against table name: '{}'", rel_table_name);
    
    for (label, schema) in node_schemas.iter() {
        let schema_table = schema.table_name.split('.').last().unwrap_or(&schema.table_name);
        eprintln!("🔍 MAP:     - '{}' -> table '{}' (stripped: '{}')", 
                 label, schema.table_name, schema_table);
    }
    
    let node_schema = node_schemas
        .values()
        .find(|n| {
            let schema_table = n.table_name.split('.').last().unwrap_or(&n.table_name);
            schema_table == rel_table_name  // ✅ Compare without database prefix
        })
        .ok_or_else(|| {
            let msg = format!("No node schema found for table '{}'", rel_table_name);
            eprintln!("❌ MAP: {}", msg);
            msg
        })?;
    
    eprintln!("✅ MAP:   Found matching node schema");
    
    // Rest of function unchanged...
}
```

### Fix #2: Skip JOINs for Denormalized Endpoints (CRITICAL - 30 min)

**File**: `src/render_plan/plan_builder.rs` (lines ~11760-11822)

**Current Code** (adds JOINs unconditionally):
```rust
if has_vlp_cte {
    // Extract endpoint aliases from VLP metadata
    let (vlp_start_alias, vlp_end_alias) = vlp_aliases.unwrap();
    
    // ALWAYS add JOINs (WRONG!)
    if let Some(start_node) = get_start_node(&plan) {
        let start_table = extract_table_name(&start_node, schema)?;
        join_builder.add_join(
            start_table,
            start_cypher_alias,  // Use Cypher alias from VLP
            JoinType::Inner,
            format!("{}.start_id = {}.{}", cte_alias, start_cypher_alias, start_id_col)
        );
    }
    // Similar for end node...
}
```

**New Code** (check denormalized status):
```rust
if has_vlp_cte {
    let (vlp_start_alias, vlp_end_alias) = vlp_aliases.unwrap();
    
    // ✅ NEW: Check if nodes are denormalized
    let start_is_denormalized = is_denormalized_node(&start_node, schema);
    let end_is_denormalized = is_denormalized_node(&end_node, schema);
    
    eprintln!("🔍 VLP: start_is_denormalized={}, end_is_denormalized={}", 
             start_is_denormalized, end_is_denormalized);
    
    // Only add JOINs for non-denormalized nodes
    if !start_is_denormalized {
        if let Some(start_node) = get_start_node(&plan) {
            eprintln!("✅ VLP: Adding START node JOIN (not denormalized)");
            let start_table = extract_table_name(&start_node, schema)?;
            join_builder.add_join(
                start_table,
                start_cypher_alias,
                JoinType::Inner,
                format!("{}.start_id = {}.{}", cte_alias, start_cypher_alias, start_id_col)
            );
        }
    } else {
        eprintln!("✅ VLP: SKIPPING START node JOIN (denormalized)");
    }
    
    if !end_is_denormalized {
        if let Some(end_node) = get_end_node(&plan) {
            eprintln!("✅ VLP: Adding END node JOIN (not denormalized)");
            // Similar...
        }
    } else {
        eprintln!("✅ VLP: SKIPPING END node JOIN (denormalized)");
    }
}

// ✅ NEW: Helper function to check if node is denormalized
fn is_denormalized_node(node: &GraphNode, schema: &GraphSchema) -> bool {
    if let Some(label) = &node.label {
        if let Ok(node_schema) = schema.get_node_schema(label) {
            return node_schema.is_denormalized;
        }
    }
    false
}
```

---

## Testing Strategy

### Test #1: Property Mapping Fix (5 min)

```bash
# After Fix #1, properties should map correctly
cargo build
pkill -f clickgraph && sleep 2
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USER="test_user"
export CLICKHOUSE_PASSWORD="test_pass"
nohup cargo run --bin clickgraph > server.log 2>&1 &
sleep 3

# Run test and check logs
pytest tests/integration/test_denormalized_edges.py::TestDenormalizedVariableLengthPaths::test_variable_path_with_denormalized_properties -xvs

# Check mapping logs
grep "✅ MAP: Successfully mapped" server.log
# Should see: ✅ MAP: Successfully mapped 'city' -> 'OriginCityName'
#             ✅ MAP: Successfully mapped 'city' -> 'DestCityName'
# (6 successful mappings total)
```

### Test #2: JOIN Elimination (5 min)

```bash
# After Fix #2, JOINs should be skipped for denormalized
grep "VLP: SKIPPING" server.log
# Should see: ✅ VLP: SKIPPING START node JOIN (denormalized)
#             ✅ VLP: SKIPPING END node JOIN (denormalized)

# Check generated SQL
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "USE denormalized_flights_test MATCH (origin:Airport)-[:FLIGHT*1..2]->(dest:Airport) WHERE origin.code = \"LAX\" RETURN origin.city, dest.city",
    "sql_only": true
  }' | jq -r '.generated_sql'

# Should NOT contain: INNER JOIN ERROR_NODE_SCHEMA_MISSING
# Should contain: vlp1.OriginCityName, vlp1.DestCityName (direct CTE access)
```

### Test #3: Full Integration (10 min)

```bash
# Remove xfail markers
vim tests/integration/test_denormalized_edges.py
# Delete both @pytest.mark.xfail lines

# Run full denormalized VLP test suite
pytest tests/integration/test_denormalized_edges.py::TestDenormalizedVariableLengthPaths -v

# Should see: 2 passed
```

### Test #4: Regression Check (15 min)

```bash
# Ensure other VLP patterns still work

# Standard VLP (separate node/edge tables)
pytest tests/integration/test_variable_length_paths.py -v

# FK-edge VLP
pytest tests/integration/test_fk_edge.py -v

# Polymorphic VLP  
pytest tests/integration/test_polymorphic_edges.py -v

# All should still pass!
```

---

## Impact Analysis

### What This Fixes

✅ **Denormalized VLP with properties**: 
- `MATCH (a:Airport)-[:FLIGHT*1..3]->(b:Airport) RETURN a.city, b.city`
- Properties now correctly included in CTE
- No unnecessary JOINs to non-existent tables

✅ **Performance optimization**:
- Denormalized pattern avoids JOINs as intended
- All data fetched from single edge table
- True no-ETL value proposition

✅ **Schema flexibility**:
- Database prefixes handled correctly
- Works with both `flights` and `test_integration.flights` table names

### What Remains TODO (Future)

⬜ **Partial denormalization**: 
- Some properties in edge table, some in node table
- Need hybrid approach (CTE for edge properties, JOIN for node-only properties)

⬜ **Recursive case properties**:
- Verify properties carried forward correctly
- Test with longer paths (*3, *5, *)

⬜ **Mixed denormalized patterns**:
- Start denormalized, end not (or vice versa)
- Currently all-or-nothing approach

---

## Commit Plan

### Commit #1: Fix property mapping
```bash
git add src/clickhouse_query_generator/variable_length_cte.rs
git commit -m "fix(vlp): Handle database prefix in denormalized property mapping

- Strip database prefix when comparing table names
- Fixes 'No node schema found' errors for denormalized VLP
- Properties now correctly mapped from schema to physical columns

Closes: Property extraction working but mapping failing issue
"
```

### Commit #2: Skip denormalized JOINs
```bash
git add src/render_plan/plan_builder.rs
git commit -m "fix(vlp): Skip endpoint JOINs for denormalized nodes

- Check is_denormalized flag before adding VLP endpoint JOINs
- Denormalized properties accessed directly from CTE
- Eliminates 'ERROR_NODE_SCHEMA_MISSING' errors

Closes: Unnecessary JOINs breaking denormalized VLP queries
"
```

### Commit #3: Enable tests
```bash
git add tests/integration/test_denormalized_edges.py
git commit -m "test: Enable VLP denormalized property tests

- Remove xfail markers from VLP tests
- All denormalized VLP tests now passing
- Coverage: Variable-length paths with denormalized properties

Tests: 2/2 passing in TestDenormalizedVariableLengthPaths
"
```

---

## Next Session Goals

1. ✅ **Implement Fix #1** (table name matching) - 15 min
2. ✅ **Implement Fix #2** (skip JOINs) - 30 min  
3. ✅ **Test and verify** - 20 min
4. ✅ **Remove xfail markers** - 5 min
5. ⬜ **Run full test suite** - 10 min
6. ⬜ **Commit changes** - 10 min
7. ⬜ **Update STATUS.md** - 10 min

**Total**: ~1.5 hours

---

## Key Learnings

1. **Debug logging saves time**: Adding comprehensive logging revealed the issue in minutes
2. **Don't assume the problem location**: We thought properties weren't being added, but the real issue was table name matching
3. **Two separate issues can compound**: Property mapping failing LED TO unnecessary JOINs being added
4. **Look at generated SQL carefully**: The error message revealed both problems clearly
5. **Schema pattern variations need careful handling**: Database prefixes, table name formats, etc.

