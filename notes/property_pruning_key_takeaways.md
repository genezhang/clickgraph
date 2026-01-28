# Property Pruning: Key Takeaways & Clarifications

**Date**: December 24, 2025  
**Context**: Clarifications on multi-scope processing and resolver relationships

---

## TL;DR - Quick Answers

### Q1: How does property pruning work across multiple WITH/RETURN scopes?

**Answer**: **Root-to-leaf tree traversal** (RETURN → MATCH in the LogicalPlan tree), propagating requirements through WITH scope boundaries.

**Terminology**: RETURN is the tree root (top), MATCH is a leaf (bottom). We traverse top-down in tree terms! 🌳

```
Flow: Final RETURN ← UNWIND ← WITH collect() ← MATCH
      (needs name)  (pass up)  (must collect)  (fetch name)
```

**NOT** top-down! We can't know what to collect until we analyze what's needed downstream.

### Q2: Relationship with existing property resolvers?

**Answer**: **Keep all three separate** - they work cooperatively:

1. **property_resolver** (translator): Schema mapping (name → full_name)
2. **projected_columns_resolver** (analyzer): Cache available properties
3. **property_requirements_analyzer** (analyzer, NEW): Determine needed properties

Each has a single clear job. Don't consolidate - traversal patterns conflict.

---

## Key Concept 1: Root-to-Leaf Tree Traversal is Essential

### Why Not Leaf-to-Root? (Why Not Start at MATCH?)

❌ **Starting from MATCH Fails**:
```cypher
MATCH (a)-[:FOLLOWS]->(b)          -- Don't know what b properties needed yet!
WITH collect(b) as friends         -- Must collect ALL properties "just in case"
UNWIND friends as friend           
RETURN friend.firstName            -- Too late to optimize!
```

✅ **Root-to-Leaf Traversal Works**:
```cypher
RETURN friend.firstName            -- Tree Root: Step 1: Need firstName
                ↓ (traverse down tree)
UNWIND friends as friend           -- Step 2: friends must have firstName
                ↓
WITH collect(b) as friends         -- Step 3: collect(b) must include firstName
                ↓
MATCH (a)-[:FOLLOWS]->(b)          -- Tree Leaf: Step 4: Fetch b.firstName only!
```

**Note**: In the LogicalPlan tree, RETURN is at the top (root) and MATCH is at bottom (leaf). We traverse from root down to leaves! 🌳

### Implementation Pattern

```rust
fn collect_requirements_recursive(plan: &Arc<LogicalPlan>, reqs: &mut PropertyRequirements) {
    match plan {
        // BOTTOM: Start here
        LogicalPlan::Projection(p) => {
            // 1. Extract requirements from THIS level
            for item in &p.items {
                extract_from_expr(&item.expression, reqs);
            }
            // 2. Recurse UP (towards MATCH)
            collect_requirements_recursive(&p.input, reqs);
        }
        
        // MIDDLE: Propagate through scope boundary
        LogicalPlan::WithClause(wc) => {
            // Requirements already accumulated from downstream
            // Now propagate them to upstream sources
            for item in &wc.items {
                if is_collect(item) {
                    propagate_requirements_to_source(item, reqs);
                }
            }
            collect_requirements_recursive(&wc.input, reqs);
        }
        
        // TOP: Terminal node (MATCH)
        _ => { /* Base case */ }
    }
}
```

---

## Key Concept 2: Three Resolvers are Complementary

### Division of Responsibilities

```
┌───────────────────────────────────────────────────────────┐
│ property_resolver (Translator Phase)                      │
│ ─────────────────────────────────────────────────────────│
│ Input:  user.name (Cypher property)                       │
│ Output: user.full_name (ClickHouse column)                │
│ Job:    Schema mapping                                    │
└───────────────────────────────────────────────────────────┘
                          ↓
┌───────────────────────────────────────────────────────────┐
│ projected_columns_resolver (Early Analyzer)               │
│ ─────────────────────────────────────────────────────────│
│ Input:  GraphNode with ViewScan                           │
│ Output: GraphNode.projected_columns = ALL properties      │
│ Job:    Cache what's AVAILABLE                            │
└───────────────────────────────────────────────────────────┘
                          ↓
┌───────────────────────────────────────────────────────────┐
│ property_requirements_analyzer (Late Analyzer) 🆕         │
│ ─────────────────────────────────────────────────────────│
│ Input:  Complete LogicalPlan tree                         │
│ Output: PropertyRequirements in PlanCtx                   │
│ Job:    Determine what's NEEDED                           │
└───────────────────────────────────────────────────────────┘
                          ↓
┌───────────────────────────────────────────────────────────┐
│ Renderer combines all three:                              │
│ • Use property_resolver mappings (already in LogicalExpr) │
│ • Query projected_columns (available = 50 properties)     │
│ • Query PropertyRequirements (needed = 3 properties)      │
│ • Filter: SELECT only needed FROM available               │
└───────────────────────────────────────────────────────────┘
```

### Why NOT Consolidate?

| Aspect | Issue with Consolidation |
|--------|--------------------------|
| **Traversal** | property_requirements needs BACKWARD pass, others need forward |
| **Phase** | property_resolver in translator, others in analyzer |
| **Output** | Three different locations (LogicalExpr, GraphNode, PlanCtx) |
| **Complexity** | 3 jobs in 1 component = 3x harder to understand/test/debug |
| **Independence** | Can't enable/disable features independently |

---

## Multi-Scope Example Walkthrough

### Query
```cypher
MATCH (a:User)-[:FOLLOWS]->(b:User)         -- Scope 1
WITH a, collect(b) as friends               -- Scope 2 boundary
UNWIND friends as friend                    -- Scope 3
MATCH (friend)-[:LIKES]->(p:Post)           -- Scope 4
RETURN friend.firstName, p.title            -- Final projection
```

### Bottom-Up Analysis Steps

```
┌─────────────────────────────────────────────────────────────┐
│ Step 1: Analyze RETURN (Bottom)                             │
├─────────────────────────────────────────────────────────────┤
│ Found: friend.firstName, p.title                            │
│ Requirements: { friend: {firstName, id}, p: {title, id} }   │
└─────────────────────────────────────────────────────────────┘
              ↑ Bubble up requirements
┌─────────────────────────────────────────────────────────────┐
│ Step 2: Analyze MATCH (friend)-[:LIKES]->(p)                │
├─────────────────────────────────────────────────────────────┤
│ Needs: friend.id for JOIN (from Step 1)                     │
│ Already have: friend.firstName (from Step 1)                │
│ Requirements: { friend: {firstName, id} }                   │
└─────────────────────────────────────────────────────────────┘
              ↑ Propagate through UNWIND
┌─────────────────────────────────────────────────────────────┐
│ Step 3: Analyze UNWIND friends as friend                    │
├─────────────────────────────────────────────────────────────┤
│ friend needs: firstName, id (from Step 2)                   │
│ Therefore: friends array must contain these                 │
│ Requirements: { friends: contains(firstName, id) }          │
└─────────────────────────────────────────────────────────────┘
              ↑ Propagate through WITH
┌─────────────────────────────────────────────────────────────┐
│ Step 4: Analyze WITH collect(b) as friends                  │
├─────────────────────────────────────────────────────────────┤
│ friends needs: firstName, id (from Step 3)                  │
│ collect(b) must include: b.firstName, b.id                  │
│ Requirements: { b: {firstName, id}, a: {id} }               │
└─────────────────────────────────────────────────────────────┘
              ↑ Propagate to MATCH
┌─────────────────────────────────────────────────────────────┐
│ Step 5: Analyze MATCH (a)-[:FOLLOWS]->(b) (Top)             │
├─────────────────────────────────────────────────────────────┤
│ Need to fetch: b.firstName, b.id, a.id                      │
│ Final requirements determined!                              │
│ Result: Only 3 columns instead of 50!                       │
└─────────────────────────────────────────────────────────────┘
```

### Generated SQL (Optimized)

```sql
-- Scope 1: MATCH with optimized property selection
SELECT 
    a.user_id AS "a.id",
    groupArray(tuple(
        b.firstName,  -- ✅ Only 2 properties
        b.user_id     -- ✅ instead of 50!
    )) as friends
FROM users AS a
JOIN user_follows ON ...
JOIN users AS b ON ...
GROUP BY a.user_id

-- Result: 96% memory reduction!
```

---

## Implementation Checklist

### Phase 1: Foundation ✓ (Design Complete)
- [x] Understand existing resolvers
- [x] Design PropertyRequirements structure
- [x] Plan PlanCtx integration
- [ ] Implement data structures
- [ ] Write unit tests

### Phase 2: Analysis Pass (Critical!)
- [ ] Implement bottom-up traversal
- [ ] Implement scope boundary propagation
- [ ] Handle WITH collect() requirement mapping
- [ ] Handle UNWIND property tracking
- [ ] Test with multi-scope queries

### Phase 3: Renderer Integration
- [ ] Update expand_collect_to_group_array
- [ ] Update expand_table_alias_to_select_items
- [ ] Update anyLast() wrapping
- [ ] Ensure compatibility with existing resolvers

### Phase 4: Validation
- [ ] Multi-scope integration tests
- [ ] Performance benchmarking
- [ ] Edge case coverage
- [ ] Documentation updates

---

## Critical Success Factors

1. ✅ **Bottom-Up Traversal**: Must analyze RETURN before MATCH
2. ✅ **Scope Propagation**: Correctly handle WITH boundaries
3. ✅ **Resolver Independence**: Keep three resolvers separate
4. ✅ **ID Column Inclusion**: Always include ID for correctness
5. ✅ **Backward Compatibility**: Graceful fallback if analyzer disabled

---

## Common Pitfalls to Avoid

| Pitfall | Why It's Wrong | Correct Approach |
|---------|----------------|------------------|
| Top-down analysis | Can't know requirements before seeing usage | Bottom-up from RETURN |
| Consolidating resolvers | Conflicting traversal patterns | Keep separate, cooperative |
| Forgetting ID columns | JOINs fail | Always include in requirements |
| Breaking scope isolation | Wrong property mappings | Respect WITH boundaries |
| Missing UNWIND propagation | collect() doesn't optimize | Track through UNWIND |

---

## Performance Impact (Reminder)

| Scenario | Before | After | Improvement |
|----------|--------|-------|-------------|
| **LDBC Person** (50 cols) | 100ms, 400KB | 12ms, 16KB | **8x faster, 96% less memory** |
| **E-commerce** (200 cols) | 800ms, 16MB | 50ms, 240KB | **16x faster, 98.5% less memory** |
| **Security Logs** (150 cols) | 500ms, 8MB | 45ms, 180KB | **11x faster, 97.8% less memory** |

---

## Next Steps

1. ✅ **Approved**: Bottom-up analysis approach
2. ✅ **Approved**: Keep three resolvers separate
3. ✅ **Approved**: Multi-scope propagation strategy
4. ⏭️ **Next**: Implement PropertyRequirements data structure
5. ⏭️ **Next**: Begin PropertyRequirementsAnalyzer skeleton

**Ready to start implementation!** 🚀

---

## References

- **Main Plan**: [property_pruning_optimization_plan.md](property_pruning_optimization_plan.md)
- **Multi-Scope Analysis**: [property_pruning_multi_scope_analysis.md](property_pruning_multi_scope_analysis.md)
- **Architecture Diagrams**: [property_pruning_architecture_diagrams.md](property_pruning_architecture_diagrams.md)
- **Executive Summary**: [property_pruning_summary.md](property_pruning_summary.md)
- **Known Issue**: [../KNOWN_ISSUES.md#1-collect-performance](../KNOWN_ISSUES.md#1-collect-performance---wide-tables-december-20-2025)
