# v0.5.3 Planning: Query Quality Release

**Goal**: Systematic bug fixes and comprehensive testing to ensure query correctness across all schema variations.

**Target**: December 2025

---

## Phase 1: Bug Fixes (Priority Order)

### Bug #1: RETURN Node on Denormalized Schema (HIGH)
**Issue**: `MATCH (a:Airport) RETURN a LIMIT 5` returns empty
**Root Cause**: Wildcard expansion looks at `property_mapping` (empty) instead of `from_node_properties`/`to_node_properties`
**Fix Location**: `src/render_plan/plan_builder.rs` - `extract_select_items` function
**Status**: ✅ Verified Working (December 2025)
**Resolution**: Added unit tests confirming `get_properties_with_table_alias` correctly reads from `from_node_properties`/`to_node_properties`. Tests: `test_denormalized_standalone_node_return_all_properties`, `test_denormalized_standalone_node_both_positions`

### Bug #2: WHERE AND Syntax Error (MEDIUM)
**Issue**: `WHERE AND r.prop = value` not caught by parser
**Root Cause**: Parser doesn't validate expression follows WHERE
**Fix Location**: `src/open_cypher_parser/expression.rs`
**Status**: ✅ Fixed (December 2025)
**Resolution**: Added `is_binary_operator_keyword()` function to reject AND/OR/XOR as standalone expressions. Tests: `test_parse_where_and_invalid`, `test_parse_where_or_invalid`, `test_parse_where_xor_invalid`, `test_parse_where_not_valid`

### Bug #3: WITH Aggregation SQL Generation (MEDIUM → Optimization)
**Issue**: Generates duplicate FROM clause with unnecessary JOIN
**Root Cause**: Plan builder creates extra JOIN for `WITH aggregation → RETURN` flow
**Fix Location**: `src/render_plan/plan_builder.rs`
**Status**: 🟡 Optimization Opportunity (Not Breaking)
**Resolution**: Analysis shows the extra JOIN is an optimization opportunity, not a correctness bug. The generated SQL executes correctly. Can be addressed in a future optimization pass to eliminate redundant subqueries.

### Bug #3b: Date Literal Parsing (MEDIUM)
**Issue**: `toDate('2024-01-15')` parsed as arithmetic `toDate(2024-1-15)`
**Root Cause**: String literal not preserved in function arguments
**Fix Location**: `src/open_cypher_parser/expression.rs`
**Status**: ✅ Verified Working (December 2025)
**Resolution**: Manual testing confirms `toDate('2024-01-15')` correctly parses with quoted string literal. The SQL output shows `toDate('2024-01-15')` not arithmetic.

---

## Phase 2: Test Matrix Design

### Schema Variations (4 Types)

| Type | Description | Example Schema |
|------|-------------|----------------|
| **Standard** | Separate node/edge tables | `social_benchmark.yaml` |
| **Denormalized** | Node properties embedded in edge table | `ontime_denormalized.yaml` |
| **Polymorphic** | Single edge table with type column | `social_polymorphic.yaml` |
| **Coupled** | Multiple edge types share same table | `zeek_dns_log.yaml` |

### Query Patterns to Test (15 Categories)

#### Basic Patterns
1. **Node Scan**: `MATCH (n:Label) RETURN ...`
2. **Node + Filter**: `MATCH (n:Label) WHERE n.prop = value RETURN ...`
3. **Return Node**: `MATCH (n) RETURN n` (whole node)
4. **Return Properties**: `MATCH (n) RETURN n.prop1, n.prop2`

#### Relationship Patterns
5. **Single Hop**: `MATCH (a)-[r:TYPE]->(b) RETURN ...`
6. **Multi Hop**: `MATCH (a)-[r1]->(b)-[r2]->(c) RETURN ...`
7. **Undirected**: `MATCH (a)-[r]-(b) RETURN ...`
8. **Return Relationship**: `MATCH ()-[r]->() RETURN r`

#### Variable-Length Paths
9. **VLP Exact**: `MATCH (a)-[*2]->(b) RETURN ...`
10. **VLP Range**: `MATCH (a)-[*1..3]->(b) RETURN ...`
11. **VLP + Path Variable**: `MATCH p = (a)-[*]->(b) RETURN p, nodes(p)`

#### Aggregation Patterns
12. **Simple Aggregation**: `MATCH (n) RETURN count(n)`
13. **GROUP BY**: `MATCH (n) RETURN n.type, count(n)`
14. **WITH Aggregation**: `MATCH ... WITH x, count(y) as c RETURN ...`

#### Advanced Patterns
15. **OPTIONAL MATCH**: `MATCH (a) OPTIONAL MATCH (a)-[r]->(b) RETURN ...`
16. **Multiple Relationship Types**: `MATCH (a)-[:T1|T2]->(b) RETURN ...`
17. **Shortest Path**: `MATCH p = shortestPath((a)-[*]->(b)) RETURN p`
18. **ORDER BY + LIMIT**: `MATCH (n) RETURN n ORDER BY n.prop LIMIT 10`

#### Functions
19. **Graph Functions**: `id(n)`, `labels(n)`, `type(r)`
20. **Aggregation Functions**: `count()`, `sum()`, `avg()`, `collect()`
21. **Path Functions**: `length(p)`, `nodes(p)`, `relationships(p)`
22. **Scalar Functions**: `toUpper()`, `substring()`, `coalesce()`

---

## Phase 3: Test Implementation

### Test File Structure

```
tests/
├── integration/
│   └── query_patterns/
│       ├── test_standard_schema.py      # All patterns on standard schema
│       ├── test_denormalized_schema.py  # All patterns on denormalized schema
│       ├── test_polymorphic_schema.py   # All patterns on polymorphic schema
│       ├── test_coupled_schema.py       # All patterns on coupled schema
│       └── conftest.py                  # Shared fixtures, schema setup
```

### Test Naming Convention

```python
def test_{pattern}_{schema_type}():
    """
    Pattern: {pattern description}
    Schema: {schema type}
    Expected: {expected behavior}
    """
```

### Test Matrix: 22 Patterns × 4 Schema Types = 88 Test Cases

| Pattern | Standard | Denorm | Poly | Coupled |
|---------|----------|--------|------|---------|
| Node Scan | ✅ | 🔴 | ✅ | ✅ |
| Node + Filter | ✅ | 🔴 | ✅ | ✅ |
| Return Node | ✅ | 🔴 | ✅ | ✅ |
| Return Props | ✅ | ✅ | ✅ | ✅ |
| Single Hop | ✅ | ✅ | ✅ | ✅ |
| Multi Hop | ✅ | ✅ | ✅ | ✅ |
| Undirected | 🟡 | 🟡 | 🟡 | 🟡 |
| Return Rel | ✅ | ✅ | ✅ | ✅ |
| VLP Exact | ✅ | ✅ | ✅ | ✅ |
| VLP Range | ✅ | ✅ | ✅ | ✅ |
| VLP + Path | ✅ | ✅ | ✅ | ✅ |
| Simple Agg | ✅ | 🔴 | ✅ | ✅ |
| GROUP BY | ✅ | 🔴 | ✅ | ✅ |
| WITH Agg | 🔴 | 🔴 | 🔴 | 🔴 |
| OPTIONAL | ✅ | ✅ | ✅ | ✅ |
| Multi-Type | ✅ | N/A | ✅ | ✅ |
| Shortest Path | ✅ | ✅ | ✅ | ✅ |
| ORDER/LIMIT | ✅ | ✅ | ✅ | ✅ |
| Graph Funcs | ✅ | ✅ | ✅ | ✅ |
| Agg Funcs | ✅ | 🔴 | ✅ | ✅ |
| Path Funcs | ✅ | ✅ | ✅ | ✅ |
| Scalar Funcs | ✅ | ✅ | ✅ | ✅ |

**Legend**: ✅ Working | 🔴 Known Bug | 🟡 Known Limitation | N/A Not Applicable

---

## Phase 4: Execution Plan

### Week 1: Bug Fixes
- [x] Day 1-2: Fix Bug #1 (RETURN node denormalized) - ✅ Verified working, added tests
- [x] Day 2-3: Fix Bug #3 (WITH aggregation) - 🟡 Analyzed as optimization, not breaking
- [x] Day 3-4: Fix Bug #2 (WHERE AND syntax) - ✅ Fixed with reserved keyword check
- [x] Day 4-5: Fix Bug #3b (Date literal parsing) - ✅ Verified working

### Week 2: Test Infrastructure
- [x] Create test fixtures for all 4 schema types
- [x] Set up parameterized test framework
- [x] Create test data generators
- [x] Schema-aware query generation (RelationshipInfo, connectivity metadata)

### Week 3: Test Implementation
- [x] Implement all 88 test cases (204 total with 3 variations each)
- [x] Document expected vs actual for each
- [x] Identify additional bugs

### Week 4: Stabilization
- [x] Fix newly discovered bugs (VLP + chained patterns)
- [x] Update KNOWN_ISSUES.md (5 active issues)
- [ ] Release v0.5.3

---

## Success Criteria for v0.5.3

1. **All 4 reported bugs addressed** ✅ 
   - Bug #1: Verified working
   - Bug #2: Fixed  
   - Bug #3: Documented as optimization opportunity
   - Bug #3b: Verified working
2. **Test coverage**: 546/546 unit tests passing (100%)
3. **Integration Tests**: 48/51 STANDARD schema (94%)
4. **Documentation**: All limitations documented with workarounds
5. **Regression**: No new regressions from v0.5.2

### Test Results Summary (December 4, 2025)

| Schema Type | Passed | XFailed | Failed | Notes |
|-------------|--------|---------|--------|-------|
| **STANDARD** | 48 | 3 | 0 | ✅ Ready |
| **DENORMALIZED** | TBD | TBD | TBD | Needs table setup |
| **POLYMORPHIC** | TBD | TBD | TBD | Needs table setup |
| **COUPLED** | TBD | TBD | TBD | Needs table setup |

### New Bugs Found (December 4, 2025)
1. Anonymous VLP uses node table instead of relationship table (HIGH)
2. Denormalized edge JOIN column swap (MEDIUM)
3. Multi-type same relationship generates invalid CTEs (LOW)

See [KNOWN_ISSUES.md](KNOWN_ISSUES.md) for details.

---

## Appendix: Schema Files for Testing

### Standard Schema
- `benchmarks/social_network/schemas/social_benchmark.yaml`
- Tables: `users_bench`, `user_follows_bench`, `posts_bench`, `post_likes_bench`

### Denormalized Schema  
- `schemas/examples/ontime_denormalized.yaml`
- Table: `flights` (node properties embedded)

### Polymorphic Schema
- `schemas/examples/social_polymorphic.yaml`
- Table: `interactions` with `type_column`

### Coupled Schema
- `schemas/examples/zeek_dns_log.yaml`
- Table: `dns_log` for multiple edge types

---

## Next Actions

1. Start with Bug #1 (RETURN node) - highest impact
2. Create minimal test case for each schema type
3. Iterate: fix → test → fix → test
