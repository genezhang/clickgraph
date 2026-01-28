# WITH Clause Complete - November 8, 2025

## Summary

Achieved **100% WITH clause test coverage** (12/12 tests passing) by fixing three critical bugs in multi-hop pattern handling, ORDER BY/LIMIT preservation, and alias resolution. Session took ~2.5 hours with clean, surgical fixes that maintained 325/325 unit test success rate.

## What Works Now

### All WITH Clause Patterns ✅

```cypher
-- 1. Basic aggregation with HAVING
MATCH (u:User)-[:AUTHORED]->(p:Post)
WITH u, COUNT(p) as post_count
WHERE post_count > 5
RETURN u.name, post_count

-- 2. WITH → MATCH pattern
MATCH (u:User)
WITH u.name as user_name
MATCH (u2:User) WHERE u2.name = user_name
RETURN u2.user_id

-- 3. Non-aggregation projection (aliases resolved!)
MATCH (a:User)-[:FOLLOWS]->(b:User)
WITH a, b.name as friend_name
RETURN a.name, friend_name

-- 4. Multiple aggregations
MATCH (u:User)-[:AUTHORED]->(p:Post)
WITH u, COUNT(p) as posts, COUNT(DISTINCT p.post_id) as unique_posts
RETURN u.name, posts, unique_posts

-- 5. ORDER BY + LIMIT with CTE
MATCH (u:User)-[:AUTHORED]->(p:Post)
WITH u, COUNT(p) as post_count
RETURN u.name, post_count
ORDER BY post_count DESC
LIMIT 5

-- 6. WITH relationship data
MATCH (u:User)-[f:FOLLOWS]->(u2:User)
WITH u, f, u2, u2.name as followed_name
RETURN u.name, followed_name

-- 7. WITH filter → MATCH with WHERE
MATCH (u:User)
WITH u WHERE u.age > 25
MATCH (u)-[:AUTHORED]->(p:Post)
RETURN u.name, p.title

-- 8. Collecting node IDs
MATCH (u:User)-[:FOLLOWS]->(f:User)
WITH u, collect(f.user_id) as following_ids
RETURN u.name, following_ids

-- 9. Multiple WITH clauses chained
MATCH (u:User)
WITH u WHERE u.age > 25
WITH u, u.age * 2 as double_age
RETURN u.name, double_age

-- 10. Multi-hop pattern (MOST COMPLEX!)
MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User)
WITH a, COUNT(DISTINCT c) as second_degree
RETURN a.name, second_degree

-- 11. Computed expressions
MATCH (u:User)
WITH u, u.age * 2 as doubled, u.age + 10 as plus_ten
RETURN u.name, doubled, plus_ten

-- 12. WITH → MATCH → aggregation in RETURN
MATCH (u:User)
WITH u WHERE u.age > 25
MATCH (u)-[:AUTHORED]->(p:Post)
RETURN u.name, COUNT(p) as post_count
```

## Three Major Fixes

### 1. Multi-hop Pattern Recursive JOIN Extraction (~60 min)

**The Problem**:
Query: `MATCH (a)-[:FOLLOWS]->(b)-[:FOLLOWS]->(c) WITH a, COUNT(DISTINCT c) as second_degree`

Generated SQL was missing first relationship:
```sql
-- WRONG (only 2 JOINs, condition references undefined 'b'):
FROM users AS a
INNER JOIN follows AS rel2 ON rel2.follower_id = b.follower_id  -- b undefined!
INNER JOIN users AS c ON c.user_id = rel2.followed_id
```

**Root Causes**:
1. **GraphJoins used pre-computed joins**: `GraphJoins.extract_joins()` returned pre-computed `graph_joins.joins` from analyzer
2. **No recursion in GraphRel**: `GraphRel.extract_joins()` didn't check if left side was another GraphRel
3. **Wrong ID column lookup**: `extract_id_column()` returned relationship column for nested patterns

**The Fix** (3 parts):

**Part 1**: GraphJoins delegation (`plan_builder.rs` lines 1588-1656)
```rust
LogicalPlan::GraphJoins(graph_joins) => {
    // OLD: return graph_joins.joins.clone()  ← Pre-computed (WRONG!)
    // NEW: Delegate to underlying GraphRel
    if has_multiple_relationships(&graph_joins.input) {
        // CTE case
    } else {
        graph_joins.input.extract_joins()?  // Recursive delegation
    }
}
```

**Part 2**: GraphRel recursion (`plan_builder.rs` lines 1657-1748)
```rust
LogicalPlan::GraphRel(graph_rel) => {
    let mut joins = vec![];
    
    // NEW: Check if left is nested GraphRel, recurse first
    if let LogicalPlan::GraphRel(_) = graph_rel.left.as_ref() {
        let mut left_joins = graph_rel.left.extract_joins()?;
        joins.append(&mut left_joins);  // Get first hop JOINs
    }
    
    // Determine start table (handle nested case)
    let start_table = if let LogicalPlan::GraphRel(_) = graph_rel.left.as_ref() {
        // Multi-hop: get end node from previous hop
        graph_rel.left.extract_from()?.last().unwrap().table.clone()
    } else {
        graph_rel.left.extract_from()?.first().unwrap().table.clone()
    };
    
    // ID column lookup fix
    let start_id_col = if let LogicalPlan::GraphRel(_) = graph_rel.left.as_ref() {
        // Multi-hop: use table-based lookup (not extract_id_column)
        table_to_id_column(&start_table)
    } else {
        extract_id_column(&graph_rel.left).unwrap_or_else(|| table_to_id_column(&start_table))
    };
    
    // Add THIS relationship's joins
    joins.push(/* relationship JOIN using start_id_col */);
    joins.push(/* end node JOIN */);
    
    joins  // All joins in correct order
}
```

**Part 3**: Deprecation comment (`logical_plan/mod.rs` lines 261-270)
```rust
pub struct GraphJoins {
    pub input: Arc<LogicalPlan>,
    /// DEPRECATED: These pre-computed joins are incorrect for multi-hop patterns.
    /// The analyzer pre-computes these joins by flattening the GraphRel tree,
    /// which loses the nested structure needed for correct JOIN generation.
    /// Only used as fallback for extract_from() now.
    /// The correct approach is to call input.extract_joins() which handles
    /// nested GraphRel recursively. TODO: Remove this field after validating
    /// all tests pass and updating extract_from() to not rely on it.
    pub joins: Vec<Join>,
}
```

**Generated SQL** (Now Correct!):
```sql
FROM users AS a
INNER JOIN follows AS rel1 ON rel1.follower_id = a.user_id      -- First hop
INNER JOIN users AS b ON b.user_id = rel1.followed_id           -- Intermediate node
INNER JOIN follows AS rel2 ON rel2.follower_id = b.user_id      -- Second hop
INNER JOIN users AS c ON c.user_id = rel2.followed_id           -- End node
```

### 2. ORDER BY + LIMIT Preservation with CTE (~30 min)

**The Problem**:
Query: `WITH u, COUNT(p) as post_count RETURN u.name, post_count ORDER BY post_count DESC LIMIT 5`

CTE not generated, SQL had ORDER BY/LIMIT directly on joined tables instead of on CTE result.

**Root Cause**:
In `try_build_join_based_plan()`, ORDER BY/LIMIT wrapper nodes were processed BEFORE checking for GraphJoins pattern:
```rust
// OLD (WRONG):
fn try_build_join_based_plan(&self) -> Result<RenderPlan> {
    if let LogicalPlan::GraphJoins(graph_joins) = self {  // Never reached!
        // CTE logic
    }
    // ... ORDER BY handling comes later (too late!)
}
```

When plan is `Limit -> OrderBy -> GraphJoins`, the `self` is Limit, not GraphJoins, so CTE detection fails.

**The Fix** (`plan_builder.rs` lines 1831-1895):

Unwrap ORDER BY/LIMIT/SKIP BEFORE checking pattern:
```rust
fn try_build_join_based_plan(&self) -> Result<RenderPlan> {
    // Extract wrappers BEFORE pattern matching
    let (core_plan, order_by_items, limit_val, skip_val) = match self {
        LogicalPlan::Limit(limit_node) => {
            match limit_node.input.as_ref() {
                LogicalPlan::OrderBy(order_node) => {
                    // Limit -> OrderBy -> core_plan
                    (order_node.input.as_ref(), 
                     Some(&order_node.items), 
                     Some(limit_node.count), 
                     None)
                }
                other => {
                    // Limit -> core_plan
                    (other, None, Some(limit_node.count), None)
                }
            }
        }
        LogicalPlan::OrderBy(order_node) => {
            // OrderBy -> core_plan
            (order_node.input.as_ref(), Some(&order_node.items), None, None)
        }
        other => (other, None, None, None)
    };
    
    // Check GraphJoins on unwrapped core_plan
    if let LogicalPlan::GraphJoins(graph_joins) = core_plan {
        let mut plan = graph_joins.input.try_build_join_based_plan()?;
        
        // Rewrite ORDER BY expressions for CTE context
        if let Some(items) = order_by_items {
            let rewritten_items = items.iter().map(|item| {
                // alias -> grouped_data.alias
                let rewritten_expr = rewrite_expr_for_cte(&item.expr);
                OrderByItem {
                    expr: rewritten_expr,
                    order: item.order.clone()
                }
            }).collect();
            plan.order_by = OrderByItems(rewritten_items);
        }
        
        plan.limit = LimitItem(limit_val);
        plan.skip = SkipItem(skip_val);
        return Ok(plan);
    }
    
    // ... fallback logic
}
```

**Generated SQL**:
```sql
WITH grouped_data AS (
    SELECT a.name, COUNT(p.post_id) as post_count
    FROM ...
    GROUP BY a.name
)
SELECT name, post_count
FROM grouped_data
ORDER BY post_count DESC
LIMIT 5
```

### 3. WITH Alias Resolution for Non-aggregation (~30 min)

**The Problem**:
Query: `WITH a, b.name as friend_name RETURN a.name, friend_name`

Generated SQL: `SELECT a.name, friend_name` where `friend_name` is undefined.

Should be: `SELECT a.name, b.name`

**Root Cause**:
Non-aggregation WITH creates aliases, but analyzer changes `Projection(kind: With)` to `Projection(kind: Return)`. The `extract_select_items()` function didn't know to look for aliases because it checked the `kind` field.

**The Fix** (`plan_builder.rs` lines 1041-1087):

Don't rely on `kind` field. Instead, collect aliases from inner Projection regardless of kind:
```rust
LogicalPlan::Projection(projection) => {
    // Collect aliases from inner Projection (might be a WITH that became a Return)
    let with_aliases: HashMap<String, LogicalExpr> = match projection.input.as_ref() {
        LogicalPlan::Projection(inner_proj) => {
            // Check if this Projection has aliases (sign of a WITH)
            let has_aliases = inner_proj.items.iter().any(|item| item.col_alias.is_some());
            if has_aliases {
                // Collect: friend_name -> b.name
                inner_proj.items.iter()
                    .filter_map(|item| {
                        item.col_alias.map(|alias| (alias.0, item.expression))
                    })
                    .collect()
            } else {
                HashMap::new()
            }
        }
        LogicalPlan::GraphJoins(graph_joins) => {
            // Look through wrapper for nested WITH
            if let LogicalPlan::Projection(inner_proj) = graph_joins.input.as_ref() {
                // ... same collection logic
            }
        }
        _ => HashMap::new()
    };
    
    // For each item, resolve BEFORE converting to RenderExpr
    for item in &projection.items {
        let resolved_expr = if let LogicalExpr::TableAlias(alias) = item.expression {
            // Look up: friend_name -> b.name
            with_aliases.get(&alias.0).cloned().unwrap_or(item.expression.clone())
        } else {
            item.expression.clone()
        };
        
        let render_expr = resolved_expr.as_render_expr()?;
        // ... use render_expr
    }
}
```

**Key Insight**: Resolve aliases BEFORE converting to RenderExpr, because RenderExpr::Alias doesn't have lookup context.

## Design Decisions

### Why Recursive Descent for GraphRel?

**Alternative Considered**: Keep pre-computed joins from analyzer

**Why Rejected**: 
- Analyzer flattens nested GraphRel tree into linear join list
- Loses hierarchical information needed for correct JOIN ordering
- Cannot distinguish first hop vs second hop

**Recursive Approach Benefits**:
- Preserves nested structure
- Processes JOINs in correct order (bottom-up)
- Each GraphRel knows only about its own relationship
- Composable: works for any depth (a->b->c->d->e...)

### Why Deprecate Instead of Remove?

**Question**: Should we remove `GraphJoins.joins` entirely?

**Decision**: Add deprecation comment, keep field for now

**Rationale**:
- `extract_from()` still uses `joins` as fallback
- Big refactor would require updating analyzer too
- Current fix achieves 100% test coverage quickly
- Can clean up in future when validated across all cases

**Trade-off**: Slight technical debt vs fast success

## Gotchas

### 1. Analyzer Modifies Projection Kind

**Issue**: Non-aggregation WITH becomes `Projection(kind: Return)`

**Symptom**: Can't detect WITH by checking `kind` field

**Solution**: Check for aliases in inner Projection regardless of `kind`

**Lesson**: Don't rely on `kind` field as source of truth, use structure instead

### 2. ID Column Lookup for Nested Patterns

**Issue**: `extract_id_column()` returns relationship column for nested GraphRel

**Symptom**: JOIN condition uses wrong column (e.g., `rel2.follower_id = b.follower_id`)

**Solution**: Use table-based lookup (`table_to_id_column()`) for multi-hop instead

**Lesson**: Nested structures need different extraction logic than flat structures

### 3. JOIN Counting in Tests

**Issue**: Test counted "INNER JOIN" twice (as "INNER JOIN" and as "JOIN")

**Symptom**: Test expected 4 JOINs but counted 8

**Solution**: Count only full keywords: `"INNER JOIN"` + `"LEFT JOIN"`

**Lesson**: String matching in tests needs to be exact, substring matches can double-count

## Key Files Modified

### `src/render_plan/plan_builder.rs`

**Lines 1831-1895**: ORDER BY/LIMIT unwrapping
- Extracts wrappers BEFORE pattern detection
- Preserves and rewrites for CTE context
- Handles combinations: Limit->OrderBy, OrderBy only, Limit only

**Lines 1041-1087**: WITH alias resolution
- Collects aliases from inner Projection
- Resolves TableAlias before RenderExpr conversion
- Looks through GraphJoins wrapper

**Lines 1588-1656**: GraphJoins delegation
- Changed from `graph_joins.joins.clone()` to `graph_joins.input.extract_joins()`
- Handles multi-hop patterns correctly

**Lines 1657-1748**: GraphRel recursive extraction
- Checks if left is nested GraphRel
- Recursively extracts left joins first
- Fixes ID column lookup for multi-hop
- Builds joins bottom-up

### `src/query_planner/logical_plan/mod.rs`

**Lines 261-270**: GraphJoins deprecation comment
- Documents why pre-computed joins are wrong
- Explains future migration path
- Clear TODO for cleanup

### `src/render_plan/tests/multiple_relationship_tests.rs`

**Lines 258-270**: Fixed JOIN counting
- Count "INNER JOIN" and "LEFT JOIN" separately
- Don't count substring "JOIN"
- Verify ON clause count matches JOIN count

### `tests/integration/test_with_clause.py`

**New file**: Comprehensive WITH clause test suite
- 12 tests covering all patterns
- Test 3: Non-aggregation aliases
- Test 5: ORDER BY + LIMIT
- Test 10: Multi-hop pattern
- All 12 passing (100%)

## Limitations

### Known Edge Cases

1. **Very deep nesting**: Tested up to 2 hops, may need validation for 3+ hops
2. **Mixed hop patterns**: e.g., `(a)-[]->(b), (b)-[]->(c)-[]->(d)` not explicitly tested
3. **OPTIONAL MATCH with multi-hop**: Interaction not fully validated

### Future Work

1. **Remove pre-computed joins**: Clean up `GraphJoins.joins` after full validation
2. **Update analyzer**: Don't generate pre-computed joins at all
3. **Test deep patterns**: Validate 3+ hop patterns work correctly
4. **Performance optimization**: Recursive calls may have overhead for very deep patterns

## Performance Impact

**Positive**:
- No additional runtime overhead (same SQL generated)
- Recursive calls only happen at plan build time (not query execution)

**Neutral**:
- Slightly more computation during plan building (negligible)
- No change to ClickHouse query performance

**No Regressions**:
- All 325 unit tests still passing (100%)
- All 24 existing integration tests still passing

## Testing Strategy

### Unit Tests
- Fixed JOIN counting logic in `test_two_hop_traversal_has_all_on_clauses`
- All existing tests still passing (no regressions)

### Integration Tests
- Created comprehensive test suite: `tests/integration/test_with_clause.py`
- 12 tests covering all WITH clause patterns
- Focus on edge cases: aliases, ORDER BY, multi-hop, chaining

### Manual Validation
- SQL inspection for each fix
- Debug logging to trace recursive calls
- ClickHouse query execution to verify correctness

## Conclusion

**Achievement**: 100% WITH clause test coverage in 2.5 hours! 🎉

**Key Successes**:
1. Systematic debugging (isolated each test failure)
2. Surgical fixes (minimal code changes)
3. No regressions (all existing tests still passing)
4. Clear documentation of technical debt

**Lessons Learned**:
1. Pre-computed optimizations can break complex cases
2. Nested structures require recursive algorithms
3. Don't rely on metadata fields, use structure instead
4. Quick fixes + documentation > big refactors

**Next Steps**:
1. Complete integration test coverage (24/35 → 35/35)
2. Validate WITH clauses in production benchmarks
3. Future: Remove pre-computed joins after full validation



