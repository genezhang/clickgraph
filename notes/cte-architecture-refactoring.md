# CTE Architecture Refactoring Plan

**Created**: November 22, 2025  
**Status**: Ready to implement  
**Goal**: Holistic CTE design - use CTEs only where semantically appropriate

## Design Principles

### CTEs Should Only Be Used For:

1. **Cypher WITH clauses** (Natural query block boundaries)
   - Explicit user intent for subquery boundaries
   - Implemented via `LogicalPlan::Cte`
   
2. **Truly variable-length paths** (Recursion required)
   - Patterns: `*1..`, `*0..`, `*1..N`, `*..N`
   - MUST use `WITH RECURSIVE` - no alternative
   
3. **Shortest path functions** (May benefit from recursion)
   - `shortestPath()`, `allShortestPaths()`
   - Can use recursive CTE for efficiency

### CTEs Should NOT Be Used For:

1. **Fixed-length paths** (No recursion needed)
   - Patterns: `*2`, `*3`, `*N` (exact hop count)
   - Should use inline JOINs directly
   
2. **Property access optimizations**
   - Handle in FROM/JOIN clause, not CTE wrapper

## Current Problems

### Problem 1: Fixed-Length Paths Use CTE Framework

**Current Code** (`ChainedJoinGenerator`):
```rust
// Generates unnecessary CTE wrapper:
WITH t AS (
    SELECT start_id, end_id FROM (chained JOINs)
)
SELECT ... FROM ... JOIN t ...
```

**Issue**: Adds overhead, requires ORDER BY rewriting, causes confusion

### Problem 2: Intermediate Node JOIN Overhead

**Current**: All intermediate nodes JOINed for cycle prevention
```sql
JOIN users m1 ... JOIN users m2 ...  -- Even when properties unused
WHERE m1.user_id != m2.user_id
```

**Optimization**: Use relationship ID columns for cycle prevention
```sql
-- No intermediate node JOINs
WHERE r1.followed_id != r2.followed_id
```

## Refactoring Phases

### Phase 1: Fixed-Length Path Refactoring ⭐ **PRIMARY FOCUS**

**Goal**: Remove CTE wrapper for fixed-length patterns

#### 1.1 Modify `extract_ctes()` to Branch Early

**File**: `src/render_plan/cte_extraction.rs`

```rust
pub fn extract_ctes(
    plan: &Arc<LogicalPlan>,
    graph_schema: &GraphSchema,
    plan_ctx: &mut PlanCtx,
) -> Result<Vec<Cte>, RenderBuildError> {
    match plan.as_ref() {
        LogicalPlan::Cte(logical_cte) => {
            // Case 1: Cypher WITH clause - NATURAL CTE!
            let render_plan = logical_cte.input.to_render_plan(graph_schema)?;
            Ok(vec![Cte {
                cte_name: logical_cte.name.clone(),
                content: CteContent::Structured(render_plan),
                is_recursive: false,
            }])
        }
        
        LogicalPlan::GraphRel(rel) => {
            // Check for variable-length patterns
            if let Some(var_len) = has_variable_length_rel(plan) {
                let spec = get_variable_length_spec(plan)?;
                let shortest_mode = get_shortest_path_mode(plan);
                
                // DECISION POINT: CTE or inline JOINs?
                if let Some(exact_hops) = spec.exact_hop_count() {
                    if shortest_mode.is_none() {
                        // Fixed-length, non-shortest-path → NO CTE!
                        println!(
                            "Fixed-length pattern (*{}) - using inline JOINs",
                            exact_hops
                        );
                        return Ok(vec![]);  // Empty CTE list
                    }
                }
                
                // Truly variable-length or shortest path → RECURSIVE CTE!
                // ... existing recursive CTE generation ...
            }
            
            // Non-variable-length relationships
            // ... existing logic ...
        }
        
        _ => Ok(vec![])
    }
}
```

#### 1.2 Implement Inline JOIN Expansion

**File**: `src/render_plan/cte_extraction.rs` (new function)

```rust
/// Generate inline JOINs for fixed-length path patterns
/// Returns JOIN items to be added to the FROM clause
pub fn expand_fixed_length_joins(
    plan: &Arc<LogicalPlan>,
    graph_schema: &GraphSchema,
    plan_ctx: &mut PlanCtx,
) -> Result<Vec<Join>, RenderBuildError> {
    let spec = get_variable_length_spec(plan)?
        .ok_or(RenderBuildError::VariableLengthNotFound)?;
    
    let exact_hops = spec.exact_hop_count()
        .ok_or(RenderBuildError::NotFixedLength)?;
    
    // Extract relationship and node info
    let rel_info = extract_relationship_info(plan, graph_schema, plan_ctx)?;
    
    let mut joins = Vec::new();
    
    for hop in 1..=exact_hops {
        // Generate relationship JOIN
        let rel_alias = format!("r{}", hop);
        let prev_alias = if hop == 1 {
            rel_info.start_alias.clone()
        } else {
            format!("m{}", hop - 1)  // Or skip if bridge-only!
        };
        
        joins.push(Join {
            join_type: JoinType::Inner,
            table_name: rel_info.rel_table.clone(),
            table_alias: Some(rel_alias.clone()),
            joining_on: format!(
                "{}.{} = {}.{}",
                prev_alias,
                rel_info.start_id_col,
                rel_alias,
                rel_info.rel_from_col
            ),
        });
        
        // Check if intermediate node needed
        if hop < exact_hops {
            if is_intermediate_node_needed(hop, plan, plan_ctx)? {
                let node_alias = format!("m{}", hop);
                joins.push(Join {
                    join_type: JoinType::Inner,
                    table_name: rel_info.node_table.clone(),
                    table_alias: Some(node_alias.clone()),
                    joining_on: format!(
                        "{}.{} = {}.{}",
                        rel_alias,
                        rel_info.rel_to_col,
                        node_alias,
                        rel_info.node_id_col
                    ),
                });
            }
            // else: Bridge directly - no node JOIN!
        }
    }
    
    // Always add final node JOIN (the endpoint)
    let last_rel = format!("r{}", exact_hops);
    joins.push(Join {
        join_type: JoinType::Inner,
        table_name: rel_info.end_node_table.clone(),
        table_alias: Some(rel_info.end_alias.clone()),
        joining_on: format!(
            "{}.{} = {}.{}",
            last_rel,
            rel_info.rel_to_col,
            rel_info.end_alias,
            rel_info.end_id_col
        ),
    });
    
    Ok(joins)
}
```

#### 1.3 Helper Function: Check if Intermediate Node Needed

```rust
/// Determines if an intermediate node in a path needs to be JOINed
/// Returns true if the node's properties or identity are referenced
fn is_intermediate_node_needed(
    hop_number: usize,
    plan: &Arc<LogicalPlan>,
    plan_ctx: &mut PlanCtx,
) -> Result<bool, RenderBuildError> {
    let node_alias = format!("m{}", hop_number);
    
    // Check if node is referenced in any clause
    let referenced = is_node_referenced_in_return(plan, &node_alias)
        || is_node_referenced_in_where(plan, &node_alias)
        || is_node_referenced_in_order_by(plan, &node_alias)
        || is_node_referenced_in_with(plan, &node_alias);
    
    if referenced {
        println!("Intermediate node {} needed - properties referenced", node_alias);
        return Ok(true);
    }
    
    // Not referenced - can bridge through relationship IDs
    println!("Intermediate node {} not needed - bridging directly", node_alias);
    Ok(false)
}
```

#### 1.4 Add Cycle Prevention with Relationship IDs

**When intermediate nodes skipped**, add WHERE clause:

```rust
fn generate_cycle_prevention_without_nodes(
    hops: usize,
    rel_info: &RelInfo,
) -> Vec<String> {
    let mut conditions = Vec::new();
    
    // Start != End
    conditions.push(format!(
        "{}.{} != {}.{}",
        rel_info.start_alias, rel_info.start_id_col,
        rel_info.end_alias, rel_info.end_id_col
    ));
    
    // Prevent intermediate repeats using relationship target IDs
    for i in 1..=hops {
        let rel_alias = format!("r{}", i);
        
        // Start != rel[i].target
        conditions.push(format!(
            "{}.{} != {}.{}",
            rel_info.start_alias, rel_info.start_id_col,
            rel_alias, rel_info.rel_to_col
        ));
        
        // End != rel[i].target (except last)
        if i < hops {
            conditions.push(format!(
                "{}.{} != {}.{}",
                rel_info.end_alias, rel_info.end_id_col,
                rel_alias, rel_info.rel_to_col
            ));
        }
        
        // rel[i].target != rel[j].target (prevent repeats)
        for j in (i+1)..=hops {
            let other_rel = format!("r{}", j);
            conditions.push(format!(
                "{}.{} != {}.{}",
                rel_alias, rel_info.rel_to_col,
                other_rel, rel_info.rel_to_col
            ));
        }
    }
    
    conditions
}
```

#### 1.5 Modify `extract_joins()` to Use Fixed-Length Expansion

**File**: `src/render_plan/plan_builder.rs`

```rust
fn extract_joins(plan: &Arc<LogicalPlan>, ...) -> Result<Vec<Join>, ...> {
    // Check if this is a fixed-length pattern (no CTE)
    if let Some(spec) = get_variable_length_spec(plan) {
        if spec.exact_hop_count().is_some() && get_shortest_path_mode(plan).is_none() {
            // Use inline JOIN expansion instead of CTE
            return expand_fixed_length_joins(plan, graph_schema, plan_ctx);
        }
    }
    
    // ... existing logic for other cases ...
}
```

#### 1.6 Remove ORDER BY Rewrite Hack

**Current hack** (in `plan_builder.rs`): Skip ORDER BY rewrite for chained JOINs

**After refactoring**: No longer needed! Fixed-length uses inline JOINs, so `a.name` references work naturally.

**Remove**:
```rust
// Check if this uses chained JOINs (exact hop count, non-shortest-path)
let uses_chained_join = ...;  // DELETE THIS LOGIC
```

### Phase 2: Intermediate Node Elimination 🎯 **NEXT PRIORITY**

**Prerequisite**: Phase 1 complete (inline JOINs working)

#### 2.1 Implement `UnusedNodeElimination` Optimizer Pass

**File**: `src/query_planner/analyzer/unused_node_elimination.rs` (new file)

```rust
use crate::query_planner::logical_plan::LogicalPlan;
use crate::query_planner::plan_ctx::PlanCtx;
use std::sync::Arc;
use std::collections::HashSet;

pub struct UnusedNodeElimination;

impl OptimizerPass for UnusedNodeElimination {
    fn optimize(
        &self,
        plan: Arc<LogicalPlan>,
        ctx: &mut PlanCtx,
    ) -> OptimizerResult<Transformed<Arc<LogicalPlan>>> {
        // 1. Collect all node aliases that are actually referenced
        let referenced_nodes = collect_referenced_nodes(&plan)?;
        
        // 2. Find intermediate nodes in path patterns
        let intermediate_nodes = find_intermediate_nodes(&plan)?;
        
        // 3. Mark intermediate nodes as bridge-only if not referenced
        for node_alias in intermediate_nodes {
            if !referenced_nodes.contains(&node_alias) {
                if let Some(table_ctx) = ctx.get_mut_table_ctx_from_alias(&node_alias)? {
                    table_ctx.is_bridge_only = true;
                    println!(
                        "Marking node {} as bridge-only (properties not referenced)",
                        node_alias
                    );
                }
            }
        }
        
        Ok(Transformed::No(plan))
    }
}
```

#### 2.2 Add `is_bridge_only` Field to TableCtx

**File**: `src/query_planner/plan_ctx/mod.rs`

```rust
pub struct TableCtx {
    pub label: String,
    pub alias: Option<String>,
    pub table_name: Option<String>,
    // ... existing fields ...
    
    /// True if this node is only used as a bridge in a path pattern
    /// and its properties are never referenced elsewhere
    pub is_bridge_only: bool,
}
```

#### 2.3 Use `is_bridge_only` in JOIN Generation

**File**: `src/render_plan/cte_extraction.rs`

Modify `is_intermediate_node_needed()` to check:
```rust
fn is_intermediate_node_needed(...) -> Result<bool, ...> {
    // Check TableCtx metadata
    if let Some(table_ctx) = plan_ctx.get_table_ctx_from_alias(&node_alias)? {
        if table_ctx.is_bridge_only {
            return Ok(false);  // Skip the JOIN!
        }
    }
    
    // ... existing reference checking logic ...
}
```

### Phase 3: Testing & Validation ✅

#### 3.1 Unit Tests for Fixed-Length Inline JOINs

**File**: `src/render_plan/tests/fixed_length_inline_tests.rs` (new)

```rust
#[test]
fn test_exact_two_hops_inline_joins() {
    let query = "MATCH (a:User)-[:FOLLOWS*2]->(c:User) RETURN a.name, c.name";
    let sql = generate_sql(query);
    
    // Should NOT have CTE
    assert!(!sql.contains("WITH"));
    
    // Should have inline JOINs
    assert!(sql.contains("JOIN follows r1"));
    assert!(sql.contains("JOIN follows r2"));
    assert!(sql.contains("JOIN users c"));
    
    // Should NOT have intermediate node JOIN (if properties unused)
    assert!(!sql.contains("JOIN users m1"));
}

#[test]
fn test_exact_hops_with_intermediate_property() {
    let query = "MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User) 
                 RETURN a.name, b.name, c.name";
    let sql = generate_sql(query);
    
    // Should have intermediate node JOIN (properties referenced)
    assert!(sql.contains("JOIN users b"));
}
```

#### 3.2 Integration Tests

- Verify results identical before/after refactoring
- Test cycle prevention correctness
- Test with large hop counts (*10, *20)
- Test optional relationships
- Test shortest path combinations

#### 3.3 Performance Benchmarks

**Compare**:
- Query planning time
- SQL generation time  
- ClickHouse execution time
- Memory usage
- Result correctness

**Queries to benchmark**:
```cypher
MATCH (a)-[:FOLLOWS*2]->(c) RETURN a.name, c.name
MATCH (a)-[:FOLLOWS*5]->(f) RETURN a.name, f.name
MATCH (a)-[:FOLLOWS*10]->(j) RETURN a.name, j.name
```

## Implementation Timeline

### Week 1: Phase 1 Core (Fixed-Length Inline JOINs)
- ✅ Day 1: Branch early in `extract_ctes()`
- ✅ Day 2: Implement `expand_fixed_length_joins()`
- ✅ Day 3: Add cycle prevention with relationship IDs
- ✅ Day 4: Unit tests for inline JOINs
- ✅ Day 5: Integration tests, fix bugs

### Week 2: Phase 1 Polish + Phase 2 Start
- ✅ Day 1: Remove ORDER BY rewrite hack
- ✅ Day 2: Full test suite passing
- ✅ Day 3: Implement `UnusedNodeElimination` pass
- ✅ Day 4: Add `is_bridge_only` field and logic
- ✅ Day 5: Tests for intermediate node elimination

### Week 3: Validation & Documentation
- ✅ Day 1-2: Performance benchmarks
- ✅ Day 3: Update STATUS.md, CHANGELOG.md
- ✅ Day 4: Update user documentation
- ✅ Day 5: Code review, final polish

## Success Criteria

1. ✅ Fixed-length patterns use inline JOINs (no CTE wrapper)
2. ✅ Truly variable-length still use recursive CTEs
3. ✅ Cypher WITH clauses still generate CTEs
4. ✅ Intermediate nodes skipped when properties unused
5. ✅ Cycle prevention works with relationship IDs
6. ✅ All tests pass (unit + integration)
7. ✅ Performance improves (30-50% for multi-hop queries)
8. ✅ Generated SQL is cleaner and more readable

## Rollback Plan

If issues arise:
1. Feature flag: `ENABLE_INLINE_FIXED_LENGTH_JOINS` (default: false)
2. Gradual rollout: Enable only for `*2`, then `*3`, etc.
3. Monitoring: Track query success rates, performance
4. Quick revert: Keep old CTE-based code path as fallback

## Files to Modify

### Core Changes:
- `src/render_plan/cte_extraction.rs` - Branch logic, inline JOIN expansion
- `src/render_plan/plan_builder.rs` - Remove ORDER BY hack, use inline JOINs
- `src/query_planner/plan_ctx/mod.rs` - Add `is_bridge_only` field
- `src/query_planner/analyzer/unused_node_elimination.rs` - New optimizer pass

### Tests:
- `src/render_plan/tests/fixed_length_inline_tests.rs` - New unit tests
- `tests/integration/test_variable_length_paths.py` - Update expectations

### Documentation:
- `STATUS.md` - Update feature list
- `CHANGELOG.md` - Document changes
- `docs/wiki/Cypher-Multi-Hop-Traversals.md` - Update examples
- `notes/intermediate-node-elimination.md` - Implementation complete

## References

- **Related Notes**: 
  - `notes/intermediate-node-elimination.md` - Detailed optimization analysis
  - `notes/viewscan.md` - Similar optimization pattern
  
- **Key Code**:
  - `src/clickhouse_query_generator/variable_length_cte.rs` - Current implementation
  - `src/render_plan/cte_extraction.rs` - CTE generation
  - `src/query_planner/analyzer/` - Optimizer passes

- **User Discussion**: Conversation on Nov 22, 2025 - Holistic CTE design
