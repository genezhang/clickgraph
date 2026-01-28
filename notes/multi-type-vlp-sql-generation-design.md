# Multi-Type VLP SQL Generation Design (Part 1D)

## Status: Design Complete, Implementation Deferred

**Date**: December 27, 2025  
**Priority**: High (GraphRAG requirement)  
**Complexity**: High (requires significant refactoring of CTE generation logic)

## Problem Statement

Variable-length paths with multiple node types or relationship types face a critical type safety issue:
- Different node types have different ID domains (User.user_id=3 ≠ Post.post_id=3)
- Recursive CTEs with polymorphic endpoints are **unsafe** - they can match IDs from different type domains
- Current implementation uses `WITH RECURSIVE` for all VLP queries

## Solution: JOIN Expansion with UNION

Instead of recursive CTEs, enumerate all valid paths and generate explicit type-safe JOINs:

### Example Query
```cypher
MATCH (u:User)-[:FOLLOWS|AUTHORED*1..2]->(x:User|Post)
RETURN x
```

### Valid Paths (from Part 1C enumeration)
1. User-[FOLLOWS]->User (1 hop)
2. User-[AUTHORED]->Post (1 hop)
3. User-[FOLLOWS]->User-[FOLLOWS]->User (2 hops)
4. User-[FOLLOWS]->User-[AUTHORED]->Post (2 hops)

### Generated SQL Structure
```sql
-- Path 1: User-[FOLLOWS]->User (1 hop)
SELECT 
    u1.user_id as x_user_id,
    u2.user_id as x_id,
    'User' as x_type,
    u2.full_name as x_name,
    u2.email_address as x_email
FROM users_bench u1
INNER JOIN user_follows_bench f1 
    ON u1.user_id = f1.follower_id
INNER JOIN users_bench u2
    ON f1.followed_id = u2.user_id
WHERE u1.user_id = 1

UNION ALL

-- Path 2: User-[AUTHORED]->Post (1 hop)
SELECT
    u1.user_id as x_user_id,
    p1.post_id as x_id,
    'Post' as x_type,
    p1.title as x_name,
    NULL as x_email  -- Post doesn't have email
FROM users_bench u1
INNER JOIN authored_bench a1
    ON u1.user_id = a1.user_id
INNER JOIN posts_bench p1
    ON a1.post_id = p1.post_id
WHERE u1.user_id = 1

UNION ALL

-- Path 3: User-[FOLLOWS]->User-[FOLLOWS]->User (2 hops)
SELECT
    u1.user_id as x_user_id,
    u3.user_id as x_id,
    'User' as x_type,
    u3.full_name as x_name,
    u3.email_address as x_email
FROM users_bench u1
INNER JOIN user_follows_bench f1 
    ON u1.user_id = f1.follower_id
INNER JOIN users_bench u2
    ON f1.followed_id = u2.user_id
INNER JOIN user_follows_bench f2
    ON u2.user_id = f2.follower_id
INNER JOIN users_bench u3
    ON f2.followed_id = u3.user_id
WHERE u1.user_id = 1

UNION ALL

-- Path 4: User-[FOLLOWS]->User-[AUTHORED]->Post (2 hops)
SELECT
    u1.user_id as x_user_id,
    p1.post_id as x_id,
    'Post' as x_type,
    p1.title as x_name,
    NULL as x_email
FROM users_bench u1
INNER JOIN user_follows_bench f1
    ON u1.user_id = f1.follower_id
INNER JOIN users_bench u2
    ON f1.followed_id = u2.user_id
INNER JOIN authored_bench a1
    ON u2.user_id = a1.user_id
INNER JOIN posts_bench p1
    ON a1.post_id = p1.post_id
WHERE u1.user_id = 1
```

## Implementation Plan

### Phase 1: Detection Logic (in `cte_extraction.rs`)

Add logic before calling `VariableLengthCteGenerator` to detect multi-type patterns:

```rust
// Check if end node has multiple types or if multi-rel-types lead to different end types
fn should_use_join_expansion(
    graph_rel: &GraphRel,
    rel_types: &[String],
    end_node_label: &str,
    schema: &GraphSchema,
) -> bool {
    // Case 1: End node has multiple explicit labels
    // TODO: Check graph_rel.right (the end node ViewScan) for multi-labels
    
    // Case 2: Single unlabeled end node but multiple relationship types
    // that connect to different node types
    if rel_types.len() > 1 {
        let mut end_types = std::collections::HashSet::new();
        for rel_type in rel_types {
            if let Some(rel_schema) = schema.get_relationships_schema_opt(rel_type) {
                end_types.insert(&rel_schema.to_node);
            }
        }
        if end_types.len() > 1 {
            return true; // Multi-type endpoints
        }
    }
    
    false
}
```

### Phase 2: SQL Generation (new module)

Create `src/clickhouse_query_generator/multi_type_vlp_joins.rs`:

```rust
pub struct MultiTypeVlpJoinGenerator<'a> {
    schema: &'a GraphSchema,
    path_enumerations: Vec<PathEnumeration>,
    start_filters: Option<String>,
    property_projections: Vec<PropertyProjection>,
    // ... other fields
}

impl<'a> MultiTypeVlpJoinGenerator<'a> {
    pub fn generate_sql(&self) -> String {
        let mut sql_branches = Vec::new();
        
        for path in &self.path_enumerations {
            let branch_sql = self.generate_path_branch(path);
            sql_branches.push(branch_sql);
        }
        
        // Combine with UNION ALL
        sql_branches.join("\nUNION ALL\n")
    }
    
    fn generate_path_branch(&self, path: &PathEnumeration) -> String {
        // Generate SELECT clause with property projections
        // Generate FROM + JOINs for each hop
        // Apply filters
        // Handle property resolution for heterogeneous types
    }
}
```

### Phase 3: Property Projection Handling

Challenge: Different node types have different properties.

Solution: Generate NULL for missing properties, add type discriminator column:

```rust
struct PropertyProjection {
    cypher_name: String,      // "x.name"
    user_column: Option<String>,  // Some("full_name")
    post_column: Option<String>,  // Some("title")
}

// In SELECT clause:
// CASE 
//   WHEN x_type = 'User' THEN u.full_name
//   WHEN x_type = 'Post' THEN p.title
//   ELSE NULL
// END as x_name
```

### Phase 4: Integration Points

1. **cte_extraction.rs** (line ~1200-1500):
   - Before calling `VariableLengthCteGenerator::new_*()`
   - Check if multi-type pattern
   - If yes, call `MultiTypeVlpJoinGenerator` instead
   - Return CTE with UNION-based SQL

2. **query_planner/analyzer/graph_traversal_planning.rs**:
   - Extract end node labels from NodePattern
   - Pass to SQL generator via GraphRel or context

## Limitations & Constraints

1. **Maximum Hops**: Limit to 3 hops for multi-type VLP
   - Reason: Exponential path explosion
   - With 2 rel types and 2 end types: 2^3 = 8 paths max at 3 hops
   
2. **Performance**: 
   - UNION ALL of explicit JOINs may be slower than recursive CTE for single-type paths
   - Only use JOIN expansion when necessary (multi-type scenarios)

3. **Property Resolution**:
   - Must handle heterogeneous property sets gracefully
   - Return NULL for properties not present in a given type

## Testing Strategy

1. **Unit Tests** (in `multi_type_vlp_joins.rs`):
   - Path branch SQL generation
   - Property projection with NULLs
   - Type discriminator column

2. **Integration Tests** (Part 1E):
   - `tests/integration/test_graphrag_explicit_multi_label.py`
   - Query: `MATCH (u:User)-[:FOLLOWS|AUTHORED*1..2]->(x:User|Post) RETURN x`
   - Verify SQL correctness
   - Verify result correctness

## Current Status

- ✅ Part 1A: AST changes complete
- ✅ Part 1B: Parser support complete  
- ✅ Part 1C: Path enumeration complete
- ⏸️ Part 1D: Design complete, implementation deferred
  - **Reason**: Requires extensive refactoring of CTE generation
  - **Estimate**: 2-3 days of focused development
  - **Decision**: Document design, proceed to Part 2A (auto-inference) which is simpler

## Next Steps (When Resuming Part 1D)

1. Create `multi_type_vlp_joins.rs` module
2. Implement `MultiTypeVlpJoinGenerator` with path branch generation
3. Add detection logic in `cte_extraction.rs`
4. Handle property projection with type discriminator
5. Add comprehensive unit tests
6. Run integration tests from Part 1E

## References

- Path enumeration: `src/query_planner/analyzer/multi_type_vlp_expansion.rs`
- Current VLP generation: `src/clickhouse_query_generator/variable_length_cte.rs`
- CTE extraction: `src/render_plan/cte_extraction.rs` (lines 1200-1500)
- Design discussion: `notes/graphrag-requirements-analysis.md`
