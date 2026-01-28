# GraphRAG Support: Verification & Extension Plan

**Date**: December 27, 2025  
**Goal**: Systematically verify, test, and extend GraphRAG subgraph extraction for v0.7.0  
**Reference**: `notes/graphrag-requirements-analysis.md`

---

## Phase 1: Verify Core Recursive Patterns (Today - 2-3 hours)

**Goal**: Confirm what already works with comprehensive tests

### 1.1 Multi-Type Recursive Patterns (45 min)
**Status**: Implemented but untested

**Test Cases**:
```cypher
-- Test 1: Two types, both recursive (User→User)
MATCH (u:User)-[:FOLLOWS|FRIENDS_WITH*1..2]->(f:User)
WHERE u.user_id = 1
RETURN f.user_id, f.name

-- Test 2: Mixed recursive + non-recursive
MATCH (u:User)-[:FOLLOWS|AUTHORED*1..2]->(x)
WHERE u.user_id = 1
RETURN labels(x), x

-- Test 3: Verify UNION ALL generation in SQL
```

**Implementation**:
- [ ] Create `tests/integration/test_graphrag_patterns.py`
- [ ] Add `TestMultiTypeRecursive` class
- [ ] Verify SQL shows UNION ALL for each type
- [ ] Check results include both relationship types

**Acceptance Criteria**:
- ✅ Both types appear in results
- ✅ SQL shows separate UNION branches
- ✅ Hop counts respected (1..2 means max 2 hops)

### 1.2 Schema Pattern Coverage (60 min)
**Status**: Most patterns implemented, need systematic testing

**Test Cases by Schema Pattern**:

```python
# Test FK-Edge pattern
def test_fk_edge_recursive():
    """User table has parent_id FK pointing to itself"""
    query = """
    MATCH (u:User)-[:REPORTS_TO*1..2]->(manager:User)
    WHERE u.user_id = 1
    RETURN manager.name
    """
    # Should use 2-table JOIN instead of 3-table

# Test Coupled Edges
def test_coupled_edges_recursive():
    """Multiple edge types in same table with filter"""
    query = """
    MATCH (u:User)-[:INTERACTED*1..2]->(v:User)
    WHERE u.user_id = 1
    RETURN v.name
    """
    # Schema filter should apply in CTE

# Test Polymorphic Edges
def test_polymorphic_edge_recursive():
    """Edge with type_column discriminator"""
    query = """
    MATCH (u:User)-[:SOCIAL_ACTION*1..2]->(v)
    WHERE u.user_id = 1
    RETURN type(SOCIAL_ACTION), v
    """
    # type_column should be in CTE
```

**Implementation**:
- [ ] Add test fixtures for FK-edge, coupled, polymorphic patterns
- [ ] Load into test_integration database
- [ ] Create schema YAMLs for each pattern
- [ ] Write 3 integration tests (one per pattern)

**Acceptance Criteria**:
- ✅ FK-edge VLP works without separate edge table
- ✅ Coupled edges respect schema filters
- ✅ Polymorphic edges use type_column correctly

### 1.3 Relationship Property Filters (30 min)
**Status**: Unknown - needs verification

**Test Cases**:
```cypher
-- Test 1: Filter on relationship property
MATCH (u:User)-[r:FOLLOWS*1..2]->(f:User)
WHERE u.user_id = 1 AND r.weight > 0.5
RETURN f.name

-- Test 2: Multiple filters
MATCH (u:User)-[r:FOLLOWS*1..2]->(f:User)
WHERE u.user_id = 1 
  AND r.follow_date > '2024-01-01'
  AND r.is_active = 1
RETURN f.name
```

**Questions to Answer**:
- [ ] Where are filters applied? (base case, recursive case, or post-filtering?)
- [ ] Do filters appear in both CTE parts?
- [ ] Performance impact?

**Implementation**:
- [ ] Add `follow_weight` column to `user_follows_bench` table
- [ ] Test query with property filter
- [ ] Examine generated SQL
- [ ] Document behavior in `docs/wiki/Cypher-Subgraph-Extraction.md`

---

## Phase 2: Document What Works (Today - 1 hour)

**Goal**: Update documentation with verified patterns

### 2.1 Update Cypher Language Reference (30 min)
**File**: `docs/wiki/Cypher-Language-Reference.md`

**Add Section**: "Variable-Length Paths - Multi-Type Patterns"
```markdown
### Multi-Type Variable-Length Paths

**Syntax**: `[:TYPE1|TYPE2|...*min..max]`

**Supported Scenarios**:
- All recursive types: `[:FOLLOWS|FRIENDS_WITH*1..2]`
- Mixed recursive/non-recursive: `[:FOLLOWS|AUTHORED*1..2]`

**SQL Generation**: UNION ALL over each type

**Examples**: [include verified test cases]

**Limitations**:
- Generic patterns `[*1..2]` not yet supported (v0.7.0 planned)
- Non-recursive edges automatically limited to 1 hop
```

### 2.2 Update Subgraph Extraction Guide (30 min)
**File**: `docs/wiki/Cypher-Subgraph-Extraction.md`

**Add Sections**:
- Schema pattern support matrix (tested patterns)
- Relationship property filters (if working)
- Performance notes for different patterns
- Limitations and workarounds

---

## Phase 3: Generic Pattern Implementation (2-3 days)

**Goal**: Implement `[*1..2]` - expand via ALL edge types

### 3.1 Design & Planning (4 hours)

**Key Design Questions**:

1. **Type Discovery**: How to get all relationship types?
   ```rust
   // In query_planner or cte_extraction
   let all_rel_types = schema.get_all_relationship_types();
   ```

2. **UNION Generation**: Reuse multi-type logic?
   ```rust
   // Treat generic pattern as multi-type with all types
   if rel_types.is_empty() {
       rel_types = schema.get_all_relationship_types();
   }
   ```

3. **Node Type Validation**: Some types may not connect
   - `User-[:FOLLOWS]->User` ✅
   - `User-[:AUTHORED]->Post` ✅
   - `Post-[:FOLLOWS]->User` ❌ (invalid per schema)
   
   **Need to filter**: Only include types where `from_node` matches start pattern

4. **Performance**: Many types = large UNION
   - Document performance characteristics
   - Consider max type limit (e.g., 10 types)
   - Add warning for queries with >N types

**Design Document Tasks**:
- [ ] Document schema lookup strategy
- [ ] Sketch SQL generation changes
- [ ] Identify affected files (parser, planner, generator)
- [ ] Create examples with 2, 5, 10 relationship types
- [ ] Performance testing plan

### 3.2 Implementation (1 day)

**Component Changes**:

```rust
// 1. Parser (open_cypher_parser/relationship_pattern.rs)
// Already accepts empty type list for [*1..2]
// No changes needed ✅

// 2. Query Planner (query_planner/analyzer/graph_traversal_planning.rs)
// Detect generic pattern and expand to all types
if relationship_types.is_empty() {
    // Generic pattern detected
    relationship_types = schema
        .get_all_relationship_types()
        .filter(|rt| is_valid_for_pattern(rt, start_label))
        .collect();
}

// 3. CTE Generation (render_plan/cte_extraction.rs)
// Already handles multiple types via UNION
// No changes needed ✅

// 4. SQL Generation (clickhouse_query_generator/variable_length_cte.rs)
// Already handles multiple types
// No changes needed ✅
```

**Implementation Steps**:
- [ ] Add `GraphSchema::get_all_relationship_types()` method
- [ ] Add validation filter: `is_valid_for_start_node()`
- [ ] Update graph traversal planning for generic patterns
- [ ] Add unit tests for type discovery
- [ ] Add integration test for generic pattern

### 3.3 Testing (4 hours)

**Test Cases**:
```cypher
-- Test 1: Generic 1-hop (baseline)
MATCH (u:User)-[*1]->(x)
WHERE u.user_id = 1
RETURN labels(x), COUNT(*)

-- Test 2: Generic 2-hop
MATCH (u:User)-[*2]->(x)
WHERE u.user_id = 1
RETURN labels(x), COUNT(*)

-- Test 3: Generic range
MATCH (u:User)-[*1..2]->(x)
WHERE u.user_id = 1
RETURN labels(x), x

-- Test 4: With relationship variable
MATCH (u:User)-[r*1..2]->(x)
WHERE u.user_id = 1
RETURN type(r), labels(x), x

-- Test 5: Large schema (10+ types)
-- Verify performance and SQL size
```

**Performance Tests**:
- [ ] Benchmark generic vs specific patterns
- [ ] Test with 2, 5, 10, 20 relationship types
- [ ] Document SQL size growth
- [ ] Set reasonable limits (max types?)

---

## Phase 4: Fix Known Issues (1 day)

### 4.1 Undirected VLP (4 hours)

**Current Issue**: Hardcoded column names in undirected pattern
**File**: `src/render_plan/cte_extraction.rs` or `variable_length_cte.rs`

**Fix Strategy**:
```rust
// Generate UNION of both directions using schema columns
let forward = generate_cte_direction(
    from_col, to_col,  // from schema
    Direction::Outgoing
);
let backward = generate_cte_direction(
    from_col, to_col,  // from schema  
    Direction::Incoming
);

// UNION ALL both CTEs
```

**Test**:
```cypher
MATCH (u:User)-[:FOLLOWS*1..2]-(f:User)
WHERE u.user_id = 1
RETURN f.name
```

**Implementation**:
- [ ] Locate hardcoded column names in undirected logic
- [ ] Replace with schema-aware column lookup
- [ ] Update test to remove XFAIL marker
- [ ] Verify with both standard and denormalized schemas

### 4.2 Edge Constraint Verification (2 hours)

**Goal**: Confirm schema filters work in recursive CTEs

**Test Case**:
```yaml
# Schema with constraint
edges:
  - type: ACTIVE_FOLLOWS
    table: follows
    filter: "is_active = 1 AND follow_date > '2024-01-01'"
```

```cypher
MATCH (u:User)-[:ACTIVE_FOLLOWS*1..2]->(f:User)
WHERE u.user_id = 1
RETURN f.name
```

**Verify**:
- [ ] Filter appears in base case CTE
- [ ] Filter appears in recursive case CTE
- [ ] Results respect constraints

---

## Phase 5: Comprehensive Test Suite (1 day)

**Goal**: Create exhaustive test coverage for all patterns

### 5.1 Test File Structure

```
tests/integration/graphrag/
├── __init__.py
├── test_recursive_patterns.py       # Single-type recursive
├── test_multi_type_recursive.py     # Multiple types
├── test_generic_patterns.py         # [*1..2] patterns (Phase 3)
├── test_schema_patterns.py          # FK-edge, coupled, polymorphic
├── test_directions.py               # Outgoing, incoming, undirected
├── test_property_filters.py         # WHERE r.prop filters
└── test_edge_constraints.py         # Schema-defined constraints
```

### 5.2 Test Matrix Coverage

**Goal**: One test per matrix row from requirements doc

```python
# Systematic test generation
PATTERNS = [
    ("specific", "recursive", "standard", "outgoing", "range"),
    ("multi", "mixed", "standard", "outgoing", "range"),
    ("generic", "mixed", "standard", "outgoing", "range"),
    # ... all 12+ combinations
]

@pytest.mark.parametrize("edge,recursion,schema,direction,hops", PATTERNS)
def test_pattern_combination(edge, recursion, schema, direction, hops):
    # Generate query based on parameters
    # Execute and validate results
    pass
```

### 5.3 Documentation Tests

**Goal**: Every example in documentation must have passing test

- [ ] Extract code blocks from `Cypher-Subgraph-Extraction.md`
- [ ] Create test case for each example
- [ ] Link tests back to docs (comments with line numbers)
- [ ] Add CI check: "docs must have test coverage"

---

## Phase 6: Performance & Optimization (Ongoing)

### 6.1 Benchmark Suite

**Create**: `benchmarks/graphrag/`

```python
# Benchmark different patterns
benchmarks = [
    ("1-hop specific", "MATCH (u)-[:FOLLOWS]->(f) RETURN f"),
    ("2-hop specific", "MATCH (u)-[:FOLLOWS*2]->(f) RETURN f"),
    ("1..2 range", "MATCH (u)-[:FOLLOWS*1..2]->(f) RETURN f"),
    ("multi-type", "MATCH (u)-[:FOLLOWS|AUTHORED*1..2]->(x) RETURN x"),
    ("generic 1-hop", "MATCH (u)-[*1]->(x) RETURN x"),
    ("generic 2-hop", "MATCH (u)-[*2]->(x) RETURN x"),
]

# Measure:
# - Query planning time
# - SQL generation time  
# - Execution time
# - Result set size
# - SQL string size
```

### 6.2 Optimization Opportunities

**Identified**:
1. Non-recursive types in multi-type patterns
   - Currently: Include in UNION, let schema filter
   - Optimize: Detect early, separate into non-VLP branch
   
2. Generic patterns with many types
   - Currently: UNION ALL over everything
   - Optimize: Consider limit on type count, or dynamic filtering

3. Property filters in CTEs
   - Currently: Applied at each hop (?)
   - Optimize: Might be redundant in some cases

**Priority**: Measure first, optimize bottlenecks only

---

## Success Criteria

### Phase 1-2 (Today)
- ✅ Multi-type recursive patterns tested and working
- ✅ FK-edge, coupled, polymorphic schemas tested with VLP
- ✅ Relationship property filters verified (or documented as limitation)
- ✅ Documentation updated with verified patterns
- ✅ Test coverage: 80%+ for existing features

### Phase 3 (Next)
- ✅ Generic patterns `[*1..2]` implemented
- ✅ All relationship types discovered from schema
- ✅ UNION generation working for N types
- ✅ Performance acceptable for reasonable schemas (<20 types)

### Phase 4 (Next)
- ✅ Undirected VLP working with schema-aware columns
- ✅ Edge constraints verified in recursive CTEs
- ✅ All known issues resolved or documented

### Phase 5 (Final)
- ✅ 100% test coverage for all matrix combinations
- ✅ Every documentation example has passing test
- ✅ CI pipeline validates GraphRAG patterns

### Phase 6 (Ongoing)
- ✅ Benchmark suite established
- ✅ Performance characteristics documented
- ✅ Optimization roadmap created

---

## Timeline

**Today (Dec 27)**:
- Morning: Phase 1 (verify core patterns) - 3 hours
- Afternoon: Phase 2 (documentation) - 1 hour
- **Deliverable**: Know exactly what works, what doesn't

**Next Session**:
- Phase 3: Generic patterns design - 4 hours
- **Deliverable**: Implementation plan for `[*1..2]`

**Following Sessions**:
- Phase 3: Generic patterns implementation - 1 day
- Phase 4: Fix known issues - 1 day
- Phase 5: Comprehensive tests - 1 day
- **Deliverable**: Production-ready GraphRAG support

**Total Estimate**: 4-5 days of focused work

---

## Immediate Next Steps (Right Now)

1. **Create test file**: `tests/integration/test_graphrag_patterns.py`
2. **Write first test**: Multi-type recursive pattern
3. **Run test**: Verify or debug
4. **Iterate**: Next pattern from Phase 1

**Command to start**:
```bash
# 1. Create test file
touch tests/integration/test_graphrag_patterns.py

# 2. Start with multi-type test
# (we'll write this together)

# 3. Run it
pytest tests/integration/test_graphrag_patterns.py -v
```

---

## Questions to Resolve

Before starting implementation:

1. **Generic pattern priority**: Is `[*1..2]` critical for v0.7.0, or can it wait?
2. **Type limits**: Should we limit UNION to N types for performance?
3. **Non-recursive filtering**: Optimize early or let schema handle it?
4. **Property filters**: Are these already working? Need to verify.
5. **Test data**: Can we reuse social_benchmark, or need new fixtures?

Let me know when you're ready to start Phase 1! 🚀
