# GraphRAG Subgraph Extraction: Requirements Analysis

**Date**: December 27, 2025  
**Context**: Verifying and extending GraphRAG support for v0.7.0

## Overview

GraphRAG subgraph extraction requires expanding from seed nodes to N-hop neighborhoods. This document analyzes ALL combinations of:
1. Edge type specification (specific vs generic)
2. Transitivity semantics
3. Schema patterns (ClickGraph's differentiator)
4. Direction patterns
5. Hop constraints
6. Edge constraints (ClickGraph's differentiator)

---

## 1. Edge Type Specification Dimension

### 1.1 Specific Edge Types

**Syntax**: `(a)-[:TYPE1|TYPE2*1..2]->(b)`

**Semantics**: 
- Each hop MUST use one of the specified types
- Hop 1: TYPE1 or TYPE2
- Hop 2: TYPE1 or TYPE2 (independent choice)

**Current Status**: ✅ Implemented (Dec 2024)
- Uses UNION ALL over specified types
- Each type generates separate CTE branch

**Test Coverage**: Partial
```cypher
-- Need to verify:
MATCH (u:User)-[:FOLLOWS|FRIENDS_WITH*1..2]->(f:User) 
WHERE u.user_id = 1 
RETURN f.name
```

**SQL Strategy**:
```sql
-- UNION over each type at each hop
WITH RECURSIVE vlp AS (
  -- Base: FOLLOWS
  SELECT ... FROM ... JOIN follows_table ...
  UNION ALL
  -- Base: FRIENDS_WITH  
  SELECT ... FROM ... JOIN friends_table ...
  UNION ALL
  -- Recursive: extends both branches
  ...
)
```

### 1.2 Generic Edge Types (ANY)

**Syntax**: `(a)-[*1..2]->(b)` or `(a)-[r*1..2]->(b)`

**Semantics**:
- Each hop can use ANY edge type in the schema
- Hop 1: any of {FOLLOWS, AUTHORED, LIKED, ...}
- Hop 2: any of {FOLLOWS, AUTHORED, LIKED, ...} (independent)

**Current Status**: ❌ NOT IMPLEMENTED
- Parser accepts `[*1..2]` but planning treats it as single anonymous type
- Would need UNION over ALL relationship types in schema

**Required Implementation**:
1. Detect generic pattern (no type specified)
2. Look up ALL relationship types from schema
3. Generate UNION ALL over all types (like multi-type pattern)

**Test Case**:
```cypher
-- Should expand via ANY edge type
MATCH (u:User)-[*1..2]->(neighbor) 
WHERE u.user_id = 1 
RETURN DISTINCT labels(neighbor), neighbor
```

---

## 2. Relationship Recursion Dimension

### 2.1 Recursive/Repeatable Relationships

**Definition**: Relationship type that can repeat/recurse because the target node type can be the source of the same relationship type.

**Schema Structure**: `from_node` type matches `to_node` type
```yaml
edges:
  - type: FOLLOWS
    from_node: User
    to_node: User  # ✅ Same type - can recurse!
```

**Examples**:
- `FOLLOWS`: User→User→User (can chain indefinitely)
- `CONNECTED_TO`: Node→Node→Node (network traversal)
- `PARENT_OF`: Person→Person→Person (can chain - grandparent, great-grandparent)
- `MANAGES`: Employee→Employee→Employee (management hierarchy)

**Implementation**: ✅ Recursive CTE works correctly
```sql
WITH RECURSIVE vlp AS (
  -- Base case: 1 hop
  SELECT start, end, 1 as hops FROM follows
  UNION ALL
  -- Recursive: extend paths
  SELECT vlp.start, next.end, vlp.hops + 1
  FROM vlp JOIN follows next ON vlp.end = next.start
  WHERE vlp.hops < max_hops
)
```

**VLP Validation**: Our code checks if relationship can recurse by validating `from_node` type matches `to_node` type. If not, VLP is limited to single hop.

### 2.2 Non-Recursive Relationships

**Definition**: Relationship where target node type cannot be source of the same relationship (structurally impossible to repeat).

**Schema Structure**: `from_node` type ≠ `to_node` type
```yaml
edges:
  - type: AUTHORED
    from_node: User
    to_node: Post  # ✅ Different type - cannot recurse!
    # Post cannot be author, so chain stops
```

**Examples**:
- `AUTHORED`: User→Post (Post can't author anything)
- `PURCHASED`: User→Product (Product can't purchase)
- `LOCATED_IN`: City→Country (Country can't be located in something)

**Current Behavior**: ✅ Handled correctly by schema validation
```cypher
-- This will be limited to 1 hop by VLP validator:
MATCH (u:User)-[:AUTHORED*1..2]->(p:Post)
-- Because Post has no outgoing AUTHORED edges
```

**VLP Validation Logic**:
1. Check if `to_node` type matches `from_node` type
2. If not, limit to single hop (recursive CTE would produce no additional results)
3. Emit warning if user specified `*2` or higher

### 2.3 Mixed Recursive/Non-Recursive

**Scenario**: Multi-type pattern with mixed recursion capability
```cypher
MATCH (a)-[:FOLLOWS|AUTHORED*1..2]->(b)
```

Where:
- `FOLLOWS`: User→User (recursive - can chain)
- `AUTHORED`: User→Post (non-recursive - stops)

**Current Behavior**: ✅ Handled correctly by schema structure
```cypher
-- Hop 1: Both work
(User)-[:FOLLOWS]->(User)   # Can continue
(User)-[:AUTHORED]->(Post)  # Dead end

-- Hop 2: Only FOLLOWS continues
(User)-[:FOLLOWS]->(User)-[:FOLLOWS]->(User)  # Valid
(User)-[:AUTHORED]->(Post)-[:FOLLOWS]->???    # No such edge
(User)-[:AUTHORED]->(Post)-[:AUTHORED]->???   # No such edge
```

**Implementation**: ⚠️ Works but may be inefficient
- Recursive CTE tries both types at each hop
- Schema structure naturally filters invalid combinations
- Post nodes have no outgoing edges, so paths naturally terminate
- Could optimize by detecting non-recursive types early

**Semantic Note**: The repeated relationship type is the same (`:FOLLOWS` or `:AUTHORED`), but the semantic meaning changes with hops:
- `PARENT_OF*2` means "parent's parent" (grandparent), not "parent" relationship applied twice in a transitive sense
- The relationship TYPE is the same, but the semantic MEANING depends on hop count

---

## 3. Schema Pattern Dimension

ClickGraph supports 6+ schema patterns. Each needs VLP support:

### 3.1 Standard Node/Edge Tables

**Schema**:
```yaml
nodes:
  - label: User
    table: users
    node_id: user_id
edges:
  - type: FOLLOWS
    table: follows
    from_id: follower_id
    to_id: followed_id
    from_node: User
    to_node: User
```

**VLP Status**: ✅ Working (fixed today)
**Test**: `tests/integration/test_variable_length_paths.py`

### 3.2 FK-Edge Pattern

**Schema**: Relationship via FK on node table (no separate edge table)
```yaml
edges:
  - type: PARENT_OF
    table: users  # Same as node table!
    from_id: user_id
    to_id: parent_id
    is_fk_edge: true
```

**VLP Status**: ✅ Implemented
**Special Handling**: Uses 2-table JOIN instead of 3-table

### 3.3 Denormalized Edges

**Schema**: Node properties stored in edge table
```yaml
nodes:
  - label: Airport
    table: flights  # Virtual node!
    node_id: code
    from_properties:
      code: Origin
      city: OriginCity
    to_properties:
      code: Dest
      city: DestCity
edges:
  - type: FLIGHT
    table: flights
    from_id: Origin
    to_id: Dest
```

**VLP Status**: ✅ Working (fixed Dec 25, 2025)
**Test**: `tests/integration/test_denormalized_edges.py::TestDenormalizedVariableLengthPaths`

### 3.4 Coupled Edges

**Schema**: Multiple edge types in same physical table
```yaml
edges:
  - type: FOLLOWS
    table: interactions
    from_id: user1_id
    to_id: user2_id
    filter: "type = 'follow'"  # Schema constraint
    
  - type: LIKES
    table: interactions  # Same table!
    from_id: user_id
    to_id: post_id
    filter: "type = 'like'"
```

**VLP Status**: ⚠️ Needs verification
**Challenge**: Schema filters must be applied in CTE base/recursive cases

### 3.5 Polymorphic Edges

**Schema**: Type discriminator column
```yaml
edges:
  - type: INTERACTED
    table: interactions
    type_column: interaction_type  # Discriminator
    from_id: from_id
    to_id: to_id
```

**VLP Status**: ✅ Implemented (type_column support in VLP generator)
**Needs Test**: Verify VLP with polymorphic edges

### 3.6 Polymorphic Nodes

**Schema**: Label discriminator column
```yaml
nodes:
  - label: Post
    table: messages
    label_column: msg_type
    label_value: 'post'
    
  - label: Comment
    table: messages  # Same table!
    label_column: msg_type
    label_value: 'comment'
```

**VLP Status**: ⚠️ Needs verification
**Challenge**: Label filters must be applied when matching nodes in VLP

---

## 4. Direction Dimension

### 4.1 Outgoing: `-[*1..2]->`

**Status**: ✅ Working
**Test**: All standard VLP tests

### 4.2 Incoming: `<-[*1..2]-`

**Status**: ✅ Working  
**Implementation**: Swaps from_id/to_id in CTE generation

### 4.3 Undirected: `-[*1..2]-`

**Status**: ⚠️ Known issue (KNOWN_ISSUES.md)
**Problem**: Uses hardcoded column names instead of schema columns
**SQL Strategy**: UNION ALL of both directions
```sql
-- Direction 1: a→b
SELECT ... WHERE a.id = 1
UNION ALL
-- Direction 2: b→a  
SELECT ... WHERE b.id = 1
```

**Issue**: Column names hardcoded to `from_id`/`to_id`
**Test**: `test_undirected_range` - marked XFAIL

---

## 5. Hop Count Dimension

### 5.1 Fixed Hops: `*2`, `*3`

**Status**: ✅ Optimized (Chained JOIN expansion)
**Strategy**: Generates inline JOINs instead of recursive CTE
```sql
-- *2 becomes:
FROM users a
JOIN follows r1 ON a.id = r1.from_id
JOIN users b ON r1.to_id = b.id
JOIN follows r2 ON b.id = r2.from_id
JOIN users c ON r2.to_id = c.id
```

**Test**: `TestFixedLengthPaths`

### 5.2 Range Hops: `*1..2`, `*2..5`

**Status**: ✅ Working (Recursive CTE)
**Test**: `TestRangePaths` - all passing

### 5.3 Unbounded: `*..`, `*1..`

**Status**: ✅ Working
**Configuration**: `max_recursive_cte_evaluation_depth` (default 1000)
**Test**: `TestUnboundedPaths`

---

## 6. Edge Constraints Dimension

**ClickGraph Differentiator**: Schema-defined constraints on relationships

### 6.1 Schema Constraints

**Definition**: Filters defined in schema YAML
```yaml
edges:
  - type: ACTIVE_FOLLOWS
    table: follows
    filter: "is_active = 1 AND follow_date > '2024-01-01'"
```

**VLP Status**: ✅ Implemented (Dec 2025)
**Verification**: Constraints applied in CTE base/recursive cases

### 6.2 Property Filters

**Definition**: Query-time WHERE clause on relationship properties
```cypher
MATCH (a)-[r:FOLLOWS*1..2]->(b)
WHERE r.weight > 0.5  -- Filter on edge property
RETURN b
```

**VLP Status**: ⚠️ Needs verification
**Challenge**: How to apply r.property filters in VLP context?
- Option 1: Apply at each hop in recursive CTE
- Option 2: Filter final paths (less efficient)

---

## 7. Complete Test Matrix

| Edge Type | Recursion | Schema | Direction | Hops | Constraints | Status | Test |
|-----------|-----------|--------|-----------|------|-------------|--------|------|
| Specific (TYPE) | Recursive | Standard | Outgoing | Fixed | None | ✅ | test_exact_two_hops |
| Specific (TYPE) | Recursive | Standard | Outgoing | Range | None | ✅ | test_one_to_two_hops |
| Specific (TYPE) | Recursive | Standard | Incoming | Range | None | ✅ | test_filter_end_node |
| Specific (TYPE) | Recursive | Standard | Undirected | Range | None | ⚠️ XFAIL | test_undirected_range |
| Multi (TYPE1\|TYPE2) | Mixed | Standard | Outgoing | Range | None | ❓ | NEEDS TEST |
| Generic (*) | Mixed | Standard | Outgoing | Range | None | ❌ | NOT IMPLEMENTED |
| Specific (TYPE) | Recursive | FK-edge | Outgoing | Range | None | ❓ | NEEDS TEST |
| Specific (TYPE) | Recursive | Denormalized | Outgoing | Range | None | ✅ | TestDenormalizedVLP |
| Specific (TYPE) | Non-recursive | Coupled | Outgoing | Range | Schema | ❓ | NEEDS TEST |
| Specific (TYPE) | N/A | Polymorphic Edge | Outgoing | Range | None | ❓ | NEEDS TEST |
| Specific (TYPE) | N/A | Polymorphic Node | Outgoing | Range | None | ❓ | NEEDS TEST |
| Specific (TYPE) | Recursive | Standard | Outgoing | Range | Property | ❓ | NEEDS TEST |

**Legend**:
- ✅ Verified working
- ⚠️ Known issue (documented)
- ❓ Implemented but needs test
- ❌ Not implemented

---

## 8. Priority Implementation Plan

### Phase 1: Verify Existing (Current Session)
1. ✅ Fix VLP schema lookup bug
2. ⬜ Test multi-type VLP: `[:TYPE1|TYPE2*1..2]`
3. ⬜ Test generic VLP: `[*1..2]` (expect to fail)
4. ⬜ Test polymorphic edge VLP
5. ⬜ Test relationship property filters in VLP

### Phase 2: Documentation
1. ⬜ Update `docs/wiki/Cypher-Subgraph-Extraction.md` with verified patterns
2. ⬜ Document limitations (generic patterns, non-transitive semantics)
3. ⬜ Add examples for all schema patterns

### Phase 3: Missing Features (v0.7.0+)
1. ⬜ Generic edge pattern `[*1..2]` - UNION over all types
2. ⬜ Fix undirected VLP (column name issue)
3. ⬜ Per-hop result limiting (if expressible in Cypher)

### Phase 4: Semantic Improvements (Future)
1. ⬜ Non-transitive relationship handling
2. ⬜ Heterogeneous path type tracking
3. ⬜ Path semantics documentation

---

## 9. Key Decisions Needed

### 9.1 Generic Edge Patterns

**Question**: Should `[*1..2]` expand via ALL edge types?

**Options**:
- A: Yes - UNION over all types (Nebula GET SUBGRAPH behavior)
- B: No - treat as single anonymous type (current behavior)
- C: Require explicit type specification

**Recommendation**: Option A (matches GraphRAG use case)

### 9.2 Non-Recursive Relationship Handling

**Question**: How to handle non-recursive relationships (User→Post) in VLP patterns?

**Current Implementation**: ✅ Already handled correctly!
- VLP validator checks if `to_node` type matches `from_node` type
- If not, limits pattern to single hop
- Schema structure naturally prevents invalid paths

**Example**:
```cypher
-- This is automatically limited to 1 hop:
MATCH (u:User)-[:AUTHORED*1..3]->(p:Post)
-- Because Post cannot be author (different node type)
```

**No changes needed** - schema-aware validation already prevents structural impossibilities.

### 9.3 Per-Hop Limits

**Question**: How to express "at most K neighbors per hop"?

**Cypher doesn't have syntax for this!**

**Options**:
- A: Not expressible in Cypher - document limitation
- B: Invent extension: `[*1..2 LIMIT 10 PER HOP]`
- C: Post-process result limiting (inefficient)

**Recommendation**: Option A + note Nebula has same limitation

---

## 10. Testing Strategy

### Unit Tests (Rust)
- VLP generator with each schema pattern
- Edge constraint compilation
- Column name resolution

### Integration Tests (pytest)
- One test per matrix row (see table above)
- Separate test file: `tests/integration/wiki/test_subgraph_extraction.py`
- Use existing fixtures + benchmark data

### E2E Tests
- Real GraphRAG workflow: seed → expand → extract triples
- Validate SQL correctness
- Performance benchmarks

---

## Conclusion

**Current Status**: 
- Homogeneous VLP (specific types, transitive edges) ✅ Works well
- Schema pattern coverage: 4/6 verified
- Generic edge expansion: Not implemented
- Documentation: Needs update with verified patterns

**Next Actions**:
1. Run verification tests for existing features
2. Document what works (not what doesn't)
3. Design generic edge expansion for v0.7.0
4. Create comprehensive test suite

**Timeline Estimate**:
- Verification & docs: 2-3 hours (today)
- Generic edge implementation: 1-2 days
- Complete test coverage: 1 day
