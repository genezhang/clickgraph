# Cross-Table Branching Pattern Fix

**Date**: December 14, 2025  
**Status**: Design Phase  
**Related Tests**: 6 skipped tests in `TestCrossTableCorrelation`

## Problem Summary

Branching patterns with shared nodes across different tables fail to generate JOINs:

```cypher
MATCH (srcip:IP)-[:REQUESTED]->(d:Domain), (srcip)-[:ACCESSED]->(dest:IP)
WHERE srcip.ip = '192.168.1.10'
RETURN srcip.ip, d.name, dest.ip
```

**Pattern Structure** (branching from shared node):
```
      srcip (IP node)
       /         \
      /           \
  REQUESTED     ACCESSED
     |             |
     v             v
  Domain(d)     IP(dest)
     |             |
  dns_log       conn_log
   (t3)          (t4)
```

**Current Behavior**: ❌
- Only includes conn_log (t4) in FROM clause
- References dns_log (t3) in SELECT but t3 not defined
- GraphJoinInference returns 0 joins
- Error: "Unknown expression identifier t3"

**Expected Behavior**: ✅
```sql
FROM test_zeek.dns_log AS t3
JOIN test_zeek.conn_log AS t4 ON t3.orig_h = t4.orig_h
```

## Root Cause Analysis

### Current Architecture
1. **VariableResolver** (analyzer phase): Resolves variable references ✅ Working
2. **GraphJoinInference** (analyzer phase): Detects relationships and generates JOINs ❌ BROKEN for branching
3. **FilterTagging** (analyzer phase): Maps WHERE clause to table aliases ✅ Working
4. **AliasResolver** (renderer phase): Unified alias resolution ✅ Working

### Plan Structure for Branching Patterns
```rust
LogicalPlan::GraphRel {
    // OUTER relationship (ACCESSED)
    left: Box::new(LogicalPlan::GraphRel {
        // INNER relationship (REQUESTED)
        left: Box::new(GraphNode { name: "srcip", ... }),
        right: Box::new(GraphNode { name: "d", ... }),
        left_connection: "srcip",
        alias: "t3",  // dns_log
        ...
    }),
    right: Box::new(GraphNode { name: "dest", ... }),
    left_connection: "srcip",  // 🔍 SHARED with inner!
    alias: "t4",  // conn_log
    ...
}
```

**Key Observation**: Both inner and outer GraphRel have `left_connection: "srcip"`

### Current GraphJoinInference Behavior
File: `src/query_planner/analyzer/graph_join_inference.rs`

```rust
fn collect_graph_joins(&mut self, plan: &LogicalPlan, ...) -> Result<()> {
    match plan {
        LogicalPlan::GraphRel(graph_rel) => {
            // Step 1: Process LEFT branch (inner GraphRel)
            self.collect_graph_joins(graph_rel.left, ...)?;
            
            // Step 2: Infer JOIN for CURRENT relationship
            self.infer_graph_join(graph_rel, ...)?;  // ⚠️ Only linear patterns!
            
            // Step 3: Process RIGHT branch
            self.collect_graph_joins(graph_rel.right, ...)?;
        }
        ...
    }
}
```

**Problem**: `infer_graph_join()` only handles single relationship (node-edge-node), doesn't detect cross-branch sharing!

## Solution Design

### Approach: Cross-Branch Shared Node Tracking

**Core Idea**: Track which nodes appear in which GraphRel branches, detect sharing, generate JOINs.

### Implementation Strategy

#### Phase 1: Add Node-to-Branch Tracking
Extend `GraphJoinCollector` struct:

```rust
struct GraphJoinCollector {
    // Existing fields...
    
    /// Maps node variable name -> Vec<(table_alias, node_column)>
    /// Example: "srcip" -> [("t3", "orig_h"), ("t4", "orig_h")]
    node_appearances: HashMap<String, Vec<(String, String)>>,
}
```

#### Phase 2: Track Node Appearances During Collection

In `collect_graph_joins`, when processing GraphRel:

```rust
LogicalPlan::GraphRel(graph_rel) => {
    // Process branches first
    self.collect_graph_joins(graph_rel.left, ...)?;
    self.collect_graph_joins(graph_rel.right, ...)?;
    
    // Extract shared node info
    let left_node_var = &graph_rel.left_connection;
    let table_alias = &graph_rel.alias;
    
    // Resolve node column name from schema
    let node_column = self.resolve_node_column(
        &graph_rel.table_name,
        left_node_var,  // e.g., "srcip"
        "left",         // edge side
    )?;
    
    // Record this appearance
    self.node_appearances
        .entry(left_node_var.clone())
        .or_insert_with(Vec::new)
        .push((table_alias.clone(), node_column));
    
    // Check if this node already appeared in a sibling branch
    if let Some(appearances) = self.node_appearances.get(left_node_var) {
        if appearances.len() > 1 {
            // Generate cross-branch JOIN!
            self.generate_cross_branch_join(left_node_var, appearances)?;
        }
    }
    
    // Continue with regular single-relationship JOIN inference
    self.infer_graph_join(graph_rel, ...)?;
}
```

#### Phase 3: Generate Cross-Branch JOINs

```rust
fn generate_cross_branch_join(
    &mut self,
    shared_node: &str,
    appearances: &[(String, String)],
) -> Result<()> {
    // Join first appearance with each subsequent appearance
    let (first_alias, first_column) = &appearances[0];
    
    for (other_alias, other_column) in &appearances[1..] {
        let join = GraphJoin {
            left_table: first_alias.clone(),
            left_column: first_column.clone(),
            right_table: other_alias.clone(),
            right_column: other_column.clone(),
            join_type: JoinType::Inner,
            join_reason: format!(
                "Cross-branch shared node: {} ({}.{} = {}.{})",
                shared_node, first_alias, first_column, other_alias, other_column
            ),
        };
        
        self.joins.push(join);
        tracing::debug!("Generated cross-branch JOIN for {}", shared_node);
    }
    
    Ok(())
}
```

#### Phase 4: Column Name Resolution

Need to resolve node variable → column name in edge table:

```rust
fn resolve_node_column(
    &self,
    table_name: &str,
    node_var: &str,
    edge_side: &str,  // "left" or "right"
) -> Result<String> {
    // Lookup: database::table::label → NodeMapping
    // For denormalized edge: get from_column or to_column
    // For edge with node table: get the ID column
    
    let composite_key = format!(
        "{}::{}::{}",
        self.database,
        table_name,
        node_label  // Need to get label from node_var via scope!
    );
    
    let node_mapping = self.schema.node_mappings.get(&composite_key)?;
    
    match edge_side {
        "left" => Ok(node_mapping.from_column.clone()),
        "right" => Ok(node_mapping.to_column.clone()),
        _ => Err(...)
    }
}
```

**Challenge**: Need node label from variable name. May need to pass scope or track variable→label mapping.

## Test Cases

### Test 1: Basic Cross-Table Branching
```cypher
MATCH (srcip:IP)-[:REQUESTED]->(d:Domain), (srcip)-[:ACCESSED]->(dest:IP)
WHERE srcip.ip = '192.168.1.10'
RETURN srcip.ip, d.name, dest.ip
```

**Expected SQL**:
```sql
SELECT t3.orig_h, t3.query, t4.resp_h
FROM test_zeek.dns_log AS t3
JOIN test_zeek.conn_log AS t4 ON t3.orig_h = t4.orig_h
WHERE t3.orig_h = '192.168.1.10'
```

### Test 2: Three-Way Branching
```cypher
MATCH (src:IP)-[:DNS]->(d), (src)-[:HTTP]->(h), (src)-[:CONN]->(c)
RETURN src, d, h, c
```

**Expected**: Chain JOINs: `t1 JOIN t2 ON ... JOIN t3 ON ...`

### Test 3: Different Column Names
If IP node uses different columns in different tables:
- dns_log: `orig_h`
- http_log: `source_ip`

**Expected**: `t3.orig_h = t4.source_ip`

## Implementation Phases

### Phase 1: Node Appearance Tracking ✅ Design Complete
- [ ] Add `node_appearances: HashMap<String, Vec<(String, String)>>` to `GraphJoinCollector`
- [ ] Initialize in `new()` or `build_graph_joins()`
- [ ] Record appearances during GraphRel processing

### Phase 2: Cross-Branch Detection ⏳ Design Complete
- [ ] Check `node_appearances` for multiple entries
- [ ] Determine if entries are from sibling branches (not ancestor-descendant)
- [ ] Generate JOIN when cross-branch sharing detected

### Phase 3: Column Resolution 🤔 Needs Design
- [ ] Resolve variable name → node label (from scope or plan context)
- [ ] Lookup composite key: `database::table::label`
- [ ] Get appropriate column (from_column/to_column based on edge side)
- [ ] Handle different column names across tables

### Phase 4: Testing 📋 Pending
- [ ] Re-enable 6 skipped tests
- [ ] Run integration test suite
- [ ] Manual testing with debug logs
- [ ] Verify SQL correctness in ClickHouse

### Phase 5: Edge Cases 🔮 Future
- [ ] Three-way+ branching
- [ ] Mixed denormalized/normalized patterns
- [ ] Optional cross-branch matches
- [ ] Self-joins (same table, different aliases)

## Key Files

1. **src/query_planner/analyzer/graph_join_inference.rs** (~1900 lines)
   - `struct GraphJoinCollector` - Add node tracking field
   - `build_graph_joins()` - Entry point
   - `collect_graph_joins()` - Add node appearance tracking
   - `generate_cross_branch_join()` - NEW METHOD
   - `resolve_node_column()` - NEW METHOD (or extend existing)

2. **src/graph_catalog/graph_schema.rs**
   - `NodeMapping` struct - Has `from_column`, `to_column`, `id_column`
   - Used for column name lookups

3. **tests/integration/test_zeek_merged.py**
   - 6 cross-table tests to re-enable
   - All follow branching pattern with shared nodes

## Design Decisions

### Decision 1: When to Generate Cross-Branch JOINs?
**Options**:
1. During branch processing (immediate)
2. After all branches collected (post-processing)
3. In separate pass after `collect_graph_joins`

**Choice**: Option 1 (immediate) - simpler, clearer causality

### Decision 2: How to Detect Sibling vs Ancestor Branches?
**Challenge**: Recursive processing makes it hard to know if two GraphRels are siblings or nested.

**Solution**: Track during recursion with context parameter:
```rust
fn collect_graph_joins(
    &mut self,
    plan: &LogicalPlan,
    depth: usize,           // NEW: track recursion depth
    parent_nodes: &HashSet<String>,  // NEW: nodes from parent branch
) -> Result<()>
```

Alternative: Use appearance order - if same node appears in multiple GraphRels at same depth level, they're siblings.

### Decision 3: Column Name Resolution
**Challenge**: Need to map variable→label→column.

**Options**:
1. Pass scope through `collect_graph_joins`
2. Use plan_ctx's variable registry
3. Track variable→label mapping in collector

**Choice**: TBD - need to examine current scope flow

## Open Questions

1. ~~**Variable→Label Resolution**: How to get node label from variable name in analyzer?~~ ✅ SOLVED
   - Use `plan_ctx.get_table_ctx_from_alias_opt(node_alias)?.get_label_str()`
   - Already works in `compute_pattern_context` at line 1958-1965

2. **Sibling Detection**: How to distinguish sibling branches from nested branches? ⏳ TO IMPLEMENT
   - **Strategy**: Track all node appearances globally, generate JOINs for any shared nodes
   - Don't need sibling detection - if same node appears in 2+ GraphRels, JOIN them
   - Edge cases: Nested patterns (a→b→c) should NOT join a's appearances (they're same node!)
   - **Solution**: Track which GraphRel "owns" each node, only JOIN when different GraphRels reference same node

3. **Join Order**: For three-way branching, what order to generate JOINs?
   - Sequential (first with second, result with third)? ← **THIS**
   - Star pattern (all to first)?
   - Optimize based on cardinality?
   - **Decision**: Sequential for simplicity - first JOIN is base, add others sequentially

4. ~~**Scope Impact**: Does adding cross-branch JOINs affect variable resolution?~~ ✅ NO IMPACT
   - VariableResolver already ran before GraphJoinInference
   - Just adding JOINs to existing structure - no scope changes needed

## Success Criteria

1. ✅ All 6 cross-table correlation tests passing
2. ✅ Generated SQL includes appropriate JOINs
3. ✅ No regression in 18 single-table tests
4. ✅ Proper error messages for disconnected patterns (CartesianProduct)
5. ✅ Debug logging shows cross-branch detection logic

## Next Steps

1. Examine variable→label resolution in existing code
2. Prototype node appearance tracking in GraphJoinCollector
3. Implement cross-branch JOIN generation
4. Test with single cross-table query
5. Iterate and refine
6. Run full test suite
7. Document in CHANGELOG and STATUS.md

## References

- Issue: Cross-table patterns were working before refactor
- User clarification: "cross table join does not mean CartesianJoins... if they share nodes, they will be joined"
- Architecture improvement: Variable resolution moved to analyzer phase
- Plan structure: Nested GraphRel represents branching patterns
