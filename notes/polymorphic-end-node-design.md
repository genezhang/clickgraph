# Polymorphic End Node Support for Multi-Type VLP

**Date**: December 27, 2025  
**Status**: Design - Ready for Implementation  
**Issue**: Multi-type VLP fails when end node has no label: `MATCH (u:User)-[:FOLLOWS|AUTHORED*1..2]->(x)`

---

## Problem Statement

### Current Failure
```cypher
MATCH (u:User)-[:FOLLOWS|AUTHORED*1..2]->(x)
WHERE u.user_id = 1
RETURN x
```

**Error**: `RENDER_ERROR: Missing table information for end node in VLP`

**Root Cause**:
- `FOLLOWS`: User→User (x is User table)
- `AUTHORED`: User→Post (x is Post table)
- Code at `cte_extraction.rs:903` calls `extract_table_name(&graph_rel.right)` which returns None for unlabeled `x`

### Why This Matters
This is **THE core GraphRAG use case**: expanding from seed nodes through multiple relationship types where end node type varies!

---

## Solution: Two-Part Implementation

### Part 1: Support Explicit Multi-Label Syntax ⭐ (Do This First!)

**Cypher Syntax**: `(x:User|Post)` or `(x:User:Post)`

**Why First**:
- Easier to implement (extends existing multi-label relationship logic)
- Provides immediate workaround for users
- Foundation for auto-inference

**Parser Changes**:
```rust
// In ast.rs - NodePattern already has single label
pub struct NodePattern<'a> {
    pub name: Option<&'a str>,
    pub label: Option<&'a str>,  // ❌ Change this
    pub labels: Option<Vec<&'a str>>,  // ✅ To this (like RelationshipPattern)
    pub properties: Option<Vec<Property<'a>>>,
}
```

**Implementation Steps**:
1. ✅ Update `NodePattern` to use `labels: Vec` instead of `label: String`
2. ✅ Update parser to accept `(x:Label1|Label2)` or `(x:Label1:Label2)`
3. ✅ Update query planner to handle multi-label nodes
4. ✅ Update CTE generation to UNION over each label combination

**Example Query**:
```cypher
-- User explicitly specifies possible end node types
MATCH (u:User)-[:FOLLOWS|AUTHORED*1..2]->(x:User|Post)
WHERE u.user_id = 1
RETURN x
```

---

### Part 2: Auto-Infer End Node Types (After Part 1)

**When**: No end node label specified → infer from relationship schemas

**Logic**:
```rust
// For each relationship type, determine possible end node types
let possible_end_node_types = HashSet::new();

for rel_type in &relationship_types {
    if let Ok(rel_schema) = schema.get_rel_schema(rel_type) {
        possible_end_node_types.insert(rel_schema.to_node.clone());
    }
}

// If unlabeled, treat as multi-label with inferred types
if node.labels.is_none() && !possible_end_node_types.is_empty() {
    node.labels = Some(possible_end_node_types.into_iter().collect());
}
```

**Example**:
```cypher
-- User writes this (no end label)
MATCH (u:User)-[:FOLLOWS|AUTHORED*1..2]->(x)

-- System infers (internally becomes):
MATCH (u:User)-[:FOLLOWS|AUTHORED*1..2]->(x:User|Post)
```

---

## SQL Generation Strategy

### ⚠️ CRITICAL: Type Safety Prevents Recursive CTE!

**The ID Collision Problem**:
```sql
-- In recursive CTE with polymorphic types:
end_id = 3, end_label = 'User'   → User #3
end_id = 3, end_label = 'Post'   → Post #3 (DIFFERENT entity!)

-- JOIN becomes unsafe:
FROM vlp JOIN users_bench ON vlp.end_id = users_bench.user_id
-- This matches BOTH rows even with WHERE end_label = 'User'
-- Because JOIN evaluates before WHERE on the CTE side
```

**User.user_id and Post.post_id are DIFFERENT domains**. Even though both are integers, they have different semantics. An accidental match (User ID 5 = Post ID 5) would create nonsensical results.

### Correct Approach: JOIN Expansion (Not Recursive CTE)

For multi-type VLP with polymorphic end nodes:
1. **Enumerate all valid path combinations** (schema-validated)
2. **Generate explicit type-safe JOINs** for each path
3. **UNION the results**

This is the **ONLY** type-safe approach when mixing node types!

### Example: `[:FOLLOWS|AUTHORED*1..2]`

**Valid Paths** (after schema validation):
1. FOLLOWS (1-hop): User→User
2. AUTHORED (1-hop): User→Post
3. FOLLOWS→FOLLOWS (2-hop): User→User→User
4. FOLLOWS→AUTHORED (2-hop): User→User→Post (**mixed types!**)

**Generated SQL**:
```sql
-- Path 1: FOLLOWS (1-hop) → User
SELECT 
  u2.user_id as result_id,
  u2.name,
  'User' as node_type,
  1 as hop_count
FROM brahmand.users_bench u1
JOIN brahmand.user_follows_bench r1 ON u1.user_id = r1.follower_id
JOIN brahmand.users_bench u2 ON r1.followed_id = u2.user_id  -- Type-safe!
WHERE u1.user_id = 1

UNION ALL

-- Path 2: AUTHORED (1-hop) → Post  
SELECT 
  p.post_id as result_id,
  p.title as name,
  'Post' as node_type,
  1 as hop_count
FROM brahmand.users_bench u1
JOIN brahmand.posts_bench p ON u1.user_id = p.author_id  -- Type-safe!
WHERE u1.user_id = 1

UNION ALL

-- Path 3: FOLLOWS→FOLLOWS (2-hop) → User
SELECT 
  u3.user_id as result_id,
  u3.name,
  'User' as node_type,
  2 as hop_count
FROM brahmand.users_bench u1
JOIN brahmand.user_follows_bench r1 ON u1.user_id = r1.follower_id
JOIN brahmand.users_bench u2 ON r1.followed_id = u2.user_id  -- Type-safe!
JOIN brahmand.user_follows_bench r2 ON u2.user_id = r2.follower_id
JOIN brahmand.users_bench u3 ON r2.followed_id = u3.user_id  -- Type-safe!
WHERE u1.user_id = 1

UNION ALL

-- Path 4: FOLLOWS→AUTHORED (2-hop) → Post
SELECT 
  p.post_id as result_id,
  p.title as name,
  'Post' as node_type,
  2 as hop_count
FROM brahmand.users_bench u1
JOIN brahmand.user_follows_bench r1 ON u1.user_id = r1.follower_id
JOIN brahmand.users_bench u2 ON r1.followed_id = u2.user_id  -- Type-safe!
JOIN brahmand.posts_bench p ON u2.user_id = p.author_id      -- Type-safe!
WHERE u1.user_id = 1
```

**Key Advantages**:
- ✅ **Type-safe**: Each JOIN uses correct ID column for that node type
- ✅ **No accidental matches**: User ID 5 never matches Post ID 5
- ✅ **Schema-validated**: Only generate JOINs for valid type combinations
- ✅ **Clear semantics**: Each UNION branch is one specific path pattern

### When to Use Each Strategy

| Pattern | Node Types | Strategy | Why |
|---------|-----------|----------|-----|
| `[:FOLLOWS*1..10]` | Homogeneous (User→User) | **Recursive CTE** | Same ID type, can recurse deeply |
| `[:FOLLOWS\|AUTHORED*1..2]` | Polymorphic (User OR Post) | **JOIN Expansion** | Different ID types, limited hops |
| `[*1..2]` (generic) | Polymorphic | **JOIN Expansion** | Different ID types, enumerate all |

**Limitation**: JOIN expansion only practical for **limited hops** (*1..3 max) due to path enumeration.
- 2 rel types, 2 hops = 4 paths (manageable)
- 5 rel types, 3 hops = 125 paths (explosion!)

**This is why we limit to 5 relationship types!**

---

## Implementation Plan

### Phase A: Parser & AST (1-2 hours)

**Files to Change**:
- `src/open_cypher_parser/ast.rs`: Change NodePattern.label → labels
- `src/open_cypher_parser/path_pattern.rs`: Parse `(x:L1|L2)` or `(x:L1:L2)`

**Test Cases**:
```rust
#[test]
fn test_multi_label_node_pipe() {
    let input = "(x:User|Post)";
    // Should parse to labels: Some(vec!["User", "Post"])
}

#[test]
fn test_multi_label_node_colon() {
    let input = "(x:User:Post)";  // Neo4j style
    // Should parse to labels: Some(vec!["User", "Post"])
}
```

### Phase B: Query Planning (2-3 hours)

**Files to Change**:
- `src/query_planner/analyzer/graph_traversal_planning.rs`: Detect multi-type + polymorphic case
- `src/query_planner/logical_plan/plan_nodes.rs`: Update GraphNode to store Vec<String> labels

**Logic**:
1. Detect multi-type VLP with polymorphic end node
2. Set flag: `use_join_expansion = true` (not recursive CTE)
3. Pass to renderer with path enumeration parameters

### Phase C: JOIN Expansion Implementation (6-8 hours) - MOST COMPLEX!

**Create New Module**: `src/render_plan/multi_type_vlp_expansion.rs`

**Key Functions**:
- `enumerate_vlp_paths()`: Generate all valid path combinations
- `validate_path()`: Check type compatibility at each hop
- `generate_type_safe_joins()`: Build JOIN chain for one path
- `union_all_paths()`: Combine all path queries

**Reuse Existing Logic**:
- Chained JOIN generation (already exists for `*2`, `*3`)
- Just extend to handle multiple relationship types per hop

### Phase D: Auto-Inference (2 hours)

**File**: `src/query_planner/analyzer/type_inference.rs` (new or in existing analyzer)

**Logic**:
```rust
fn infer_end_node_types(
    rel_types: &[String],
    schema: &GraphSchema
) -> Vec<String> {
    let mut possible_types = HashSet::new();
    
    for rel_type in rel_types {
        if let Ok(rel_schema) = schema.get_rel_schema(rel_type) {
            possible_types.insert(rel_schema.to_node.clone());
        }
    }
    
    possible_types.into_iter().collect()
}
```

**When to Apply**:
- During query planning, after relationship types are known
- Before JOIN expansion
- Only if end node has no labels specified

**Core Insight**: Cannot use recursive CTE for polymorphic types due to ID type safety!

**Files to Change**:
- `src/render_plan/cte_extraction.rs`: Detect multi-type + polymorphic end node case
- `src/render_plan/join_expansion.rs`: Generate explicit JOINs for each valid path
- `src/clickhouse_query_generator/join_builder.rs`: Build type-safe JOIN chains

**Algorithm**:

**1. Enumerate Valid Paths**:
```rust
fn enumerate_vlp_paths(
    rel_types: &[String],
    min_hops: usize,
    max_hops: usize,
    schema: &GraphSchema,
    start_label: &str,
) -> Vec<PathSpec> {
    let mut paths = Vec::new();
    
    // For each hop count in range
    for hop_count in min_hops..=max_hops {
        // Generate all permutations of relationship types
        let permutations = generate_permutations(rel_types, hop_count);
        
        for perm in permutations {
            // Validate path: check each hop's from_node → to_node types match
            if let Some(path_spec) = validate_and_build_path(&perm, start_label, schema) {
                paths.push(path_spec);
            }
        }
    }
    
    paths
}

// PathSpec = Vec of (rel_type, from_label, to_label)
// Example: [("FOLLOWS", "User", "User"), ("AUTHORED", "User", "Post")]
```

**2. Validate Each Path**:
```rust
fn validate_and_build_path(
    rel_types: &[String],
    start_label: &str,
    schema: &GraphSchema
) -> Option<PathSpec> {
    let mut path = Vec::new();
    let mut current_label = start_label.to_string();
    
    for rel_type in rel_types {
        let rel_schema = schema.get_rel_schema(rel_type).ok()?;
        
        // Check if current node type can use this relationship
        if rel_schema.from_node != current_label {
            return None;  // Invalid: type mismatch
        }
        
        // Valid hop
        path.push((
            rel_type.clone(),
            current_label.clone(),
            rel_schema.to_node.clone()
        ));
        
        // Update current type for next hop
        current_label = rel_schema.to_node.clone();
    }
    
    Some(path)
}
```

**3. Generate Type-Safe JOINs**:
```rust
fn generate_join_for_path(
    path_spec: &PathSpec,
    schema: &GraphSchema,
    where_filter: &str,
) -> String {
    let mut joins = Vec::new();
    let mut from_clause = String::new();
    
    // Start node
    let start_label = &path_spec[0].1;
    let start_node_schema = schema.get_node_schema(start_label).unwrap();
    from_clause = format!(
        "{}.{} AS n0",
        start_node_schema.database,
        start_node_schema.table_name
    );
    
    // For each hop
    for (i, (rel_type, from_label, to_label)) in path_spec.iter().enumerate() {
        let rel_schema = schema.get_rel_schema(rel_type).unwrap();
        let to_node_schema = schema.get_node_schema(to_label).unwrap();
        
        // Relationship JOIN
        joins.push(format!(
            "JOIN {}.{} AS r{} ON n{}.{} = r{}.{}",
            rel_schema.database,
            rel_schema.table_name,
            i,
            i,
            get_node_id_col(from_label, schema),  // Type-specific!
            i,
            rel_schema.from_id
        ));
        
        // Next node JOIN
        joins.push(format!(
            "JOIN {}.{} AS n{} ON r{}.{} = n{}.{}",
            to_node_schema.database,
            to_node_schema.table_name,
            i + 1,
            i,
            rel_schema.to_id,
            i + 1,
            get_node_id_col(to_label, schema)  // Type-specific!
        ));
    }
    
    // Build SELECT
    let hop_count = path_spec.len();
    let final_node = format!("n{}", hop_count);
    let end_label = &path_spec.last().unwrap().2;
    
    format!(
        "SELECT {final_node}.*, '{end_label}' as node_type, {hop_count} as hop_count\n\
         FROM {from_clause}\n\
         {joins}\n\
         WHERE {where_filter}",
        final_node = final_node,
        end_label = end_label,
        hop_count = hop_count,
        from_clause = from_clause,
        joins = joins.join("\n"),
        where_filter = where_filter
    )
}
```

**4. UNION All Valid Paths**:
```rust
fn generate_multi_type_vlp_sql(
    rel_types: &[String],
    min_hops: usize,
    max_hops: usize,
    start_label: &str,
    schema: &GraphSchema,
    where_filter: &str,
) -> String {
    let valid_paths = enumerate_vlp_paths(rel_types, min_hops, max_hops, schema, start_label);
    
    let path_queries: Vec<String> = valid_paths.iter()
        .map(|path| generate_join_for_path(path, schema, where_filter))
        .collect();
    
    path_queries.join("\n\nUNION ALL\n\n")
}
```

**Example Output for `[:FOLLOWS|AUTHORED*1..2]`**:
- enumerate_vlp_paths returns 4 valid paths
- generate_join_for_path creates type-safe JOINs for each
- Result: 4 UNION branches, each with correct table/column references

### Phase D: Auto-Inference (2 hours)

**File**: `src/query_planner/analyzer/type_inference.rs` (new or in existing analyzer)

**Logic**:
```rust
fn infer_end_node_types(
    rel_types: &[String],
    schema: &GraphSchema
) -> Vec<String> {
    let mut possible_types = HashSet::new();
    
    for rel_type in rel_types {
        if let Ok(rel_schema) = schema.get_rel_schema(rel_type) {
            possible_types.insert(rel_schema.to_node.clone());
        }
    }
    
    possible_types.into_iter().collect()
}
```

**When to Apply**:
- During query planning, after relationship types are known
- Before CTE generation
- Only if end node has no labels specified

---

## Testing Strategy

### Unit Tests (Rust)

```rust
#[test]
fn test_parse_multi_label_node() {
    // Test: (x:User|Post)
}

#[test]
fn test_infer_end_node_types() {
    // Given: [:FOLLOWS|AUTHORED]
    // FOLLOWS: User→User
    // AUTHORED: User→Post
    // Expected: ["User", "Post"]
}

#[test]
fn test_filter_invalid_combinations() {
    // Given: [:FOLLOWS] (User→User) + end_labels ["User", "Post"]
    CTE Structure Complexity

**Not a branch explosion!** Single recursive CTE with UNION at each level:

**Base case**: N relationship types → N UNION branches
**Recursive case**: N relationship types → N UNION branches  
**Final SELECT**: M end node types → M UNION branches

**Example**: 
- `[:FOLLOWS|AUTHORED]` (2 types) + `[User|Post]` (2 end labels)
- **Base case**: 2 UNION branches
- **Recursive case**: 2 UNION branches (with type guards)
- **Final SELECT**: 2 UNION branches
- **Total**: 6 UNION branches (linear, not exponential!)

**Actual paths generated**: Schema + type guards filter naturally
- FOLLOWS → User ✅
- AUTHORED → Post ✅
- FOLLOWS → FOLLOWS → User ✅
- FOLLOWS → AUTHORED → Post ✅ (mixed types!)
- AUTHORED → FOLLOWS ❌ (filtered by type guard)
- AUTHORED → AUTHORED ❌ (filtered by type guard
    GROUP BY labels(x)
    """
    result = execute_cypher(query, schema_name="social_benchmark")
    assert result["status"] == "success"
    # Should find both User and Post nodes

def test_multi_type_vlp_auto_infer():
    """System infers end node types automatically."""
    query = """
    MATCH (u:User)-[:FOLLOWS|AUTHORED*1..2]->(x)
    WHERE u.user_id = 1
    RETURN labels(x), count(*) as cnt
    GROUP BY labels(x)
    """
    result = execute_cypher(query, schema_name="social_benchmark")
    assert result["status"] == "success"
    # Should find both User and Post nodes (auto-inferred)

def test_vlp_sql_generation():
    """Verify SQL shows UNION over valid combinations."""
    query = """
    MATCH (u:User)-[:FOLLOWS|AUTHORED*1..2]->(x)
    WHERE u.user_id = 1
    RETURN x LIMIT 5
    """
    result = execute_cypher(query, schema_name="social_benchmark", sql_only=True)
    sql = result["generated_sql"]
    
    # Should have UNION of valid branches
    assert "UNION ALL" in sql
    # Should have CTEs for valid combinations only
    assert "vlp_follows" in sql.lower()
    assert "vlp_authored" in sql.lower()
```

---

## Performance Considerations

### Branch Explosion

**Problem**: N relationship types × M end node types = N×M branches

**Mitigation**:
1. ✅ Schema validation filters invalid combinations (often 50%+ reduction)
2. ✅ Limit relationship types to 5 (per user requirement)
3. ✅ Most real schemas: 2-3 relationship types → 2-4 branches total

**Example**: 
- `[:FOLLOWS|AUTHORED]` (2 types) × `[User|Post]` (2 labels) = 4 combinations
- Schema filters to 2 valid: FOLLOWS→User, AUTHORED→Post
- **Final: 2 CTE branches** (manageable!)

### SQL Size

**Estimate**: Each branch ~150 lines × 2-4 branches = 300-600 lines SQL
- Within ClickHouse limits
- Negligible parsing overhead

---

## Edge Cases & Limitations

### Case 1: Truly Generic Pattern

```cypher
-- No relationship types, no end labels
MATCH (u:User)-[*1..2]->(x)
```

**Handling**:
- Relationship types: Infer ALL types from schema (limit 5)
- End node types: Infer from those relationship types
- Could produce 5+ branches - acceptable with limit

### Case 2: Start Node Also Multi-Type

```cypher
MATCH (u:User|Admin)-[:FOLLOWS*1..2]->(x)
```

**Handling**: 
- Cartesian product of start types × relationship types × end types
- Use same UNION strategy recursively
- Schema validation critical to avoid explosion

### Case 3: Mid-Path Type Constraints

```cypher
MATCH (u:User)-[:FOLLOWS*1..2]->(intermediate:User)-[:AUTHORED]->(x:Post)
```

**Handling**:
- This is fixed 2-hop + 1-hop (not VLP issue)
- Already handled by existing chained JOIN expansion
- No changes needed

---

## Migration Path

### Week 1: Part 1 - Explicit Multi-Labels
- Day 1-2: Parser changes (labels Vec)
- Day 2-3: Query planner updates
- Day 3-4: CTE generation with UNION
- Day 4-5: Testing & documentation

### Week 2: Part 1 Completion + Part 2 Start
- Day 1: Fix any regressions from Part 1
- Day 2-3: Implement auto-inference
- Day 4: Integration testing
- Day 5: Performance testing & optimization

### Week 3: Generic Patterns
- Build on Part 1+2 infrastructure
- Implement `[*1..2]` (all edge types)
- Same UNION logic, different inference

---

## Success Criteria

### Must Have (v0.7.0)
- ✅ Parse `(x:User|Post)` syntax
- ✅ Generate correct SQL with UNION over valid combinations
- ✅ Schema validation filters invalid branches
- ✅ Auto-infer end node types from relationships
- ✅ Multi-type VLP with unlabeled end node works
- ✅ All existing tests pass (no regressions)

### Nice to Have
- ✅ Performance: <500ms for 5 relationship types
- ✅ SQL size: <1000 lines for typical queries
- ✅ Documentation: Comprehensive examples in Cypher guide

---

## Open Questions

1. **Neo4j Compatibility**: Does Neo4j support `(x:Label1|Label2)` or `(x:Label1:Label2)`?
   - **Answer**: Neo4j uses `:` for multiple labels (inheritance model)
   - **Decision**: Support both `|` (our multi-type syntax) and `:` (Neo4j compat)

2. **Ordering of Results**: When UNIONing branches, preserve hop order?
   - **Answer**: Use `ORDER BY hop_count, ...` in final SELECT
   - **Decision**: Document that results are ordered by path length first

3. **Relationship Type in Results**: Should `type(r)` show all types in path?
   - **Answer**: YES - path_relationships array tracks all types
   - **Decision**: No changes needed, existing path tracking handles it

---

## Files to Change (Summary)

**Parser** (2 files):
- `src/open_cypher_parser/ast.rs` - NodePattern.labels
- `src/open_cypher_parser/path_pattern.rs` - Parse multi-label syntax

**Planner** (3 files):
- `src/query_planner/logical_plan/plan_nodes.rs` - GraphNode labels Vec
- `src/query_planner/analyzer/graph_traversal_planning.rs` - Handle multi-label
- `src/query_planner/analyzer/type_inference.rs` - New file for auto-inference

**Renderer** (1 file):
- `src/render_plan/cte_extraction.rs` - UNION generation for multi-label end nodes

**Tests** (2 files):
- `tests/integration/test_graphrag_multi_type.py` - Update with new cases
- `tests/unit/...` - Add Rust unit tests for each component

---

## Next Steps

1. **Get Approval**: Review this design with team/user
2. **Start Implementation**: Begin with Phase A (parser)
3. **Incremental Testing**: Test after each phase
4. **Document as We Go**: Update Cypher guide with examples

**Estimated Total Time**: 10-15 hours spread over 2-3 days

---

## Appendix: Related Issues

- Generic patterns `[*1..2]`: Depends on this infrastructure (Part 2)
- Undirected VLP bug: Separate issue (hardcoded columns)
- FK-edge VLP: Already works, no changes needed
- Denormalized edge VLP: Already works, no changes needed
