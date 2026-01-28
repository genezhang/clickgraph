# Comma-Pattern Shared Alias Bug Investigation

**Date**: December 20, 2025  
**Status**: Root cause identified, fix strategy defined

## Problem Statement

Comma-separated MATCH patterns with shared node aliases fail when the alias is used in patterns that scan different tables.

### Failing Query Example
```cypher
MATCH (srcip:IP)-[:REQUESTED]->(d:Domain), 
      (srcip)-[:ACCESSED]->(dest:IP)
WHERE srcip.ip = '192.168.1.10'
RETURN DISTINCT srcip.ip, d.name, dest.ip
```

### Expected Behavior
- First pattern: Scans `dns_log` table (aliased as t1686)
- Second pattern: Scans `conn_log` table (aliased as t1687)  
- `srcip` should represent the SAME logical entity but from TWO different table scans
- SQL should include BOTH tables in FROM clause
- WHERE clause should correlate them: `t1686.orig_h = t1687.orig_h`

### Actual Behavior (Bug)
Generated SQL references `t1686.orig_h` but only includes `t1687` (conn_log) in FROM clause:
```sql
SELECT DISTINCT t1686.orig_h AS `srcip.ip`, 
                t1686.query AS `d.name`, 
                t1687.resp_h AS `dest.ip` 
FROM test_zeek.conn_log AS t1687 
WHERE t1686.orig_h = '192.168.1.10'
```
Error: `Unknown expression identifier t1686.orig_h`

## Root Cause

**File**: `src/query_planner/logical_plan/match_clause.rs`  
**Function**: `traverse_node_pattern()` (lines 2050-2165)

### The Bug (lines 2074-2080)
```rust
// if alias already present in ctx map then just add its conditions and do not add it in the logical plan
if let Some(table_ctx) = plan_ctx.get_mut_table_ctx_opt(&node_alias) {
    if node_label.is_some() {
        table_ctx.set_labels(node_label.map(|l| vec![l]));
    }
    if !node_props.is_empty() {
        table_ctx.append_properties(node_props);
    }
    Ok(plan)  // ❌ BUG: Returns without adding scan to plan!
}
```

**Why it's wrong**: When processing the second occurrence of `srcip` in the conn_log pattern, the code:
1. Finds `srcip` already exists in `TableCtx` map (from dns_log scan)
2. Updates its metadata
3. **Returns the plan unchanged** - never adds the conn_log scan!
4. Result: First table (dns_log) is missing from FROM clause

### Working Workaround
The test `test_predicate_correlation` PASSES by using DIFFERENT aliases:
```cypher
MATCH (srcip1:IP)-[:REQUESTED]->(d:Domain), 
      (srcip2:IP)-[:ACCESSED]->(destip:IP)
WHERE srcip1.ip = srcip2.ip
```
This works because `srcip1` and `srcip2` are distinct aliases, so both get added to the plan.

## Fix Strategy

### Option 1: Detect Cross-Table Alias Reuse (Recommended)
When encountering a reused alias in a comma-separated pattern:

1. **Check if it's the same table**:
   - Look up the relationship type to determine which table it scans
   - If `(srcip:IP)-[:REQUESTED]` → dns_log and `(srcip)-[:ACCESSED]` → conn_log, they're different

2. **If different tables, generate internal alias**:
   ```rust
   // Generate: srcip_dns_log_1 and srcip_conn_log_2
   let internal_alias = format!("{}_{}_{}",  node_alias, table_name, unique_id);
   ```

3. **Track correlation**:
   ```rust
   // In PlanCtx, track: srcip → [srcip_dns_log_1, srcip_conn_log_2]
   plan_ctx.add_correlated_alias(node_alias, internal_alias, table_scan);
   ```

4. **Generate equality predicates**:
   ```rust
   // WHERE srcip_dns_log_1.orig_h = srcip_conn_log_2.orig_h
   // This correlates the two scans
   ```

5. **Project with original alias**:
   ```sql
   SELECT srcip_dns_log_1.orig_h AS `srcip.ip`  -- Use either internal alias
   ```

### Option 2: Generate Self-Join Explicitly
- For shared aliases, always create explicit self-join structure
- Less optimal SQL but clearer semantics

### Option 3: Transform to WITH Clause
- Rewrite query internally to use WITH + WHERE correlation
- This matches the working pattern

## Implementation Plan

### Phase 1: Detection (1-2 hours)
1. In `traverse_node_pattern()`, when alias exists:
   - Check if current pattern's table differs from existing table
   - Add logging to identify when cross-table reuse occurs

### Phase 2: Generate Internal Aliases (2-3 hours)
1. Create `generate_internal_alias()` helper
2. Modify `traverse_node_pattern()` to generate internal alias for second occurrence
3. Track correlation in `PlanCtx`

### Phase 3: Add Correlation Predicates (2-3 hours)
1. After all patterns processed, detect correlated aliases
2. Generate equality predicates for node_id columns
3. Add to WHERE clause

### Phase 4: Update Property Resolution (2-3 hours)
1. When resolving `srcip.ip`, determine which internal alias to use
2. Prefer the one from the "main" pattern (heuristic: first occurrence)

### Phase 5: Testing (2-3 hours)
1. Run failing cross-table tests
2. Ensure no regressions on other tests
3. Add unit tests for alias correlation logic

**Total Estimate**: 2-3 days

## Test Cases to Fix

1. `test_comma_pattern_cross_table` - Basic 2-pattern comma with shared alias
2. `test_comma_pattern_full_dns_path` - DNS path + connection
3. `test_sequential_match_same_node` - Multiple MATCH clauses
4. `test_with_match_correlation` - WITH + MATCH
5. `test_dns_then_connect_to_resolved_ip` - Full threat pattern

## Alternative: Documentation Fix

If implementation is too complex, document that users should:
- Use different aliases for nodes in different patterns
- Add WHERE clause to correlate them
- Example: `(srcip1:IP)..., (srcip2:IP)... WHERE srcip1.ip = srcip2.ip`

**Status of Alternative**: Already works! (test_predicate_correlation passes)
