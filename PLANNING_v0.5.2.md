# ClickGraph v0.5.2 Planning Document

**Created**: November 21, 2025  
**Target Release**: December 2025 (2-3 weeks)  
**Focus**: Bug fixes, Neo4j compatibility, quality improvements

---

## 🎯 Release Goals

**Primary Objectives**:
1. ✅ Fix critical Neo4j compatibility bugs (node uniqueness, disconnected patterns)
2. 🚀 Improve query correctness and user experience
3. 📈 Increase integration test pass rate (currently 64%)
4. 🧪 Maintain 100% unit test coverage
5. 📚 Document known limitations clearly

**Non-Goals** (defer to v0.6.0):
- ❌ New major features (Vector search, GraphRAG)
- ❌ Performance optimizations (already excellent)
- ❌ Breaking API changes
- ❌ New Cypher language features

---

## 📊 Current State Analysis

### Test Coverage
- **Unit Tests**: 423/423 (100%) ✅
- **Integration Tests**: 197/308 (64%) ⚠️
- **E2E Tests**: 4/4 Bolt protocol (100%) ✅
- **Benchmark Tests**: 14/14 (100%) ✅

### Known Issues Summary
**From KNOWN_ISSUES.md (Nov 20, 2025)**:
- 🐛 **2 Bugs**: Node uniqueness, disconnected patterns
- 💡 **1 Enhancement**: Polymorphic schema
- ✅ **Already Completed in v0.5.1**: Role-based connection pooling
- 📝 **Multiple feature gaps**: Inline properties, map literals, list comprehensions

### User Feedback & Requests
**Based on documentation and roadmap analysis**:
1. Node uniqueness violations in friends-of-friends queries (discovered in testing)
2. Confusing error messages for disconnected patterns
3. Need for better Neo4j compatibility testing
4. Request for inline property syntax support
5. Anonymous node pattern support (partially done)

---

## 🎯 Proposed Features for v0.5.2

### Priority 1: Critical Bug Fixes (Must-Have) 🔥

#### 1. Fix Node Uniqueness in MATCH Patterns 🐛
**Issue**: Friends-of-friends queries return the start node as a result  
**Impact**: HIGH - Violates Neo4j/OpenCypher semantics  
**Effort**: 2-3 days  
**Files**: `match_clause.rs`, `plan_builder.rs`, SQL generator

**User Story**:
```cypher
-- Current (WRONG): Returns user_id=1 in results
MATCH (user:User)-[:FOLLOWS]-()-[:FOLLOWS]-(fof:User)
WHERE user.user_id = 1
RETURN fof.user_id

-- Expected: Should exclude user_id=1
```

**Implementation**:
- Track all node aliases in pattern context
- Generate exclusion predicates: `WHERE fof.user_id <> user.user_id`
- Only apply within single MATCH clause (not across multiple MATCHes)
- Add comprehensive tests

**Deliverables**:
- ✅ Node exclusion logic in SQL generation
- ✅ Unit tests (5+ test cases)
- ✅ Integration tests (Neo4j comparison)
- ✅ Update KNOWN_ISSUES.md
- ✅ Add note to docs/wiki/Multi-Hop-Traversals.md

**Acceptance Criteria**:
- Friends-of-friends query excludes start node
- 3+ hop patterns enforce uniqueness
- Multiple MATCH clauses do NOT enforce cross-clause uniqueness
- Performance impact < 5%

---

#### 2. Fix Disconnected Pattern Detection 🐛
**Issue**: Comma-separated patterns without shared nodes generate invalid SQL  
**Impact**: MEDIUM - Breaks queries instead of clear error  
**Effort**: 1-2 days  
**Files**: `match_clause.rs`, error handling

**User Story**:
```cypher
-- Current (WRONG): Generates invalid SQL
MATCH (user:User), (other:User)
WHERE user.user_id = 1
RETURN other.user_id
-- SQL Error: Unknown identifier 'user.user_id'

-- Expected: Clear error message
Error: Disconnected patterns found in MATCH clause. 
Patterns must share at least one node variable.
```

**Implementation**:
- Fix pattern connectivity detection in `match_clause.rs`
- Collect aliases from all patterns
- Check intersection between patterns
- Improve error message with query context

**Deliverables**:
- ✅ Working disconnected pattern detection
- ✅ Clear error message with query context
- ✅ Test case already exists (line 1342)
- ✅ Add E2E test with actual execution
- ✅ Update KNOWN_ISSUES.md

**Acceptance Criteria**:
- Disconnected patterns throw clear error
- Error message shows which patterns are disconnected
- Existing test passes
- User-facing error is helpful

---

### Priority 2: Quality Improvements (Should-Have) 🌟

#### 3. Improve Integration Test Pass Rate 🧪
**Goal**: Increase from 64% (197/308) to 75%+ (231/308)  
**Effort**: 3-4 days  
**Approach**: Triage and fix

**Analysis Needed**:
1. Categorize failures:
   - ❌ Real bugs to fix
   - 📝 Feature gaps (document, defer)
   - ✅ Incorrect test expectations (fix tests)
   - 🔮 Future features (skip/disable)

2. Quick wins (fix tests with wrong expectations):
   - Tests expecting old behavior
   - Tests for unimplemented features
   - Tests with schema mismatches

3. Feature gaps to document:
   - Inline property syntax
   - Map literals
   - List comprehensions
   - Specific functions

**Deliverables**:
- ✅ Integration test triage report
- ✅ Fix 10-20 test expectation issues
- ✅ Document 5-10 feature gaps
- ✅ Update TEST_COVERAGE_PLAN.md
- ✅ Disable/skip tests for future features

**Acceptance Criteria**:
- Pass rate 75%+
- All failures documented with reasoning
- Clear roadmap for remaining gaps

---

#### 4. Neo4j Compatibility Testing Infrastructure 🔬
**Goal**: Automated comparison with Neo4j for validation  
**Effort**: 2-3 days  
**Enhancement from KNOWN_ISSUES.md**

**User Story**:
As a developer, I want to validate ClickGraph behavior matches Neo4j so that users can migrate confidently.

**Implementation**:
- Add Neo4j container to `docker-compose.test.yaml`
- Create `scripts/test/neo4j_comparison.py`
- Run same queries on both ClickGraph and Neo4j
- Compare results (handle ordering differences)
- Generate compatibility report

**Test Cases** (start with 10-15 queries):
1. Simple node matching
2. Relationship traversals (1-3 hops)
3. Friends-of-friends (node uniqueness)
4. OPTIONAL MATCH
5. Variable-length paths
6. Shortest paths
7. Aggregations
8. ORDER BY / LIMIT
9. Functions (string, math, datetime)
10. Multiple patterns (connected)

**Deliverables**:
- ✅ Neo4j container in test infrastructure
- ✅ Comparison test script
- ✅ Initial test suite (10-15 queries)
- ✅ Compatibility report generation
- ✅ Documentation in TESTING_GUIDE.md

**Acceptance Criteria**:
- Can run queries on both systems
- Results comparison automated
- Report shows differences clearly
- Runs in CI/CD (optional for v0.5.2)

---

#### 5. Enhanced Error Messages 📝
**Goal**: Make errors more helpful for users  
**Effort**: 2 days  
**Low-hanging fruit**

**Examples**:

**Before**:
```
Error: No From Table.
```

**After**:
```
Error: Cannot resolve label 'User' in query. 
Schema 'default' has no label 'User' defined.
Available labels: Product, Customer, Order

Query: MATCH (u:User) RETURN u
                 ^^^^
```

**Implementation**:
1. Add query context to errors
2. Add "did you mean?" suggestions
3. Show available labels/properties
4. Point to exact location in query

**Deliverables**:
- ✅ Enhanced error messages for:
  - Label not found
  - Property not found
  - Schema not found
  - Disconnected patterns (already in #2)
- ✅ Query context in error responses
- ✅ Update error handling documentation

---

### Priority 3: Nice-to-Have Features (Could-Have) 💡

#### 6. Anonymous Node Patterns (Complete) ✨
**Status**: Partially implemented  
**Effort**: 1-2 days  
**From KNOWN_ISSUES.md line 739**

**Current State**:
- ✅ Anonymous edges: `()-[r]->()` works
- ⏳ Anonymous nodes: `(a)-[r]->()` needs completion

**User Story**:
```cypher
-- Anonymous end node (don't care about properties)
MATCH (user:User)-[:FOLLOWS]->()
WHERE user.user_id = 1
RETURN count(*) AS follower_count
```

**Implementation**:
- Generate unique alias for anonymous nodes: `_anon_1`, `_anon_2`
- Don't include anonymous nodes in SELECT clause
- Allow in WHERE clause traversal

**Deliverables**:
- ✅ Anonymous node support complete
- ✅ Update KNOWN_ISSUES.md (move to RESOLVED)
- ✅ Add tests
- ✅ Update docs/wiki/Cypher-Basic-Patterns.md

---

#### 7. Polymorphic Schema Support 🎭
**Status**: Proposed enhancement  
**Effort**: 3-4 days  
**From KNOWN_ISSUES.md line 298**

**Goal**: Support multiple tables backing single label  
**Use Case**: Sharded/partitioned data

**Example**:
```yaml
labels:
  - name: User
    tables:
      - users_2023      # Partition 1
      - users_2024      # Partition 2
    property_mappings:
      user_id: user_id
      name: full_name
```

**Generated SQL** (UNION ALL):
```sql
SELECT user_id, full_name FROM users_2023
UNION ALL
SELECT user_id, full_name FROM users_2024
```

**Implementation**:
1. Extend schema YAML: `tables: []` instead of `table: ""`
2. Update schema validation
3. Generate UNION ALL in SQL generator
4. Test performance with 2-5 partitions

**Deliverables**:
- ✅ Multi-table label support
- ✅ UNION ALL SQL generation
- ✅ Schema validation
- ✅ Example schema
- ✅ Documentation

**Acceptance Criteria**:
- Works with 2-10 tables per label
- Performance acceptable (within 10% overhead)
- Maintains relationship joins

---

## 📅 Proposed Timeline

**Total Duration**: 2-3 weeks (December 2025)

### Week 1: Critical Bugs + Testing Infrastructure
- **Days 1-2**: Fix node uniqueness (#1) 🔥
- **Days 3-4**: Fix disconnected patterns (#2) 🔥
- **Day 5**: Neo4j comparison infrastructure (#4) 🌟

### Week 2: Quality Improvements
- **Days 1-2**: Integration test triage (#3) 🌟
- **Days 3-4**: Enhanced error messages (#5) 🌟
- **Day 5**: Buffer/testing

### Week 3: Optional Enhancements (if time permits)
- **Days 1-2**: Anonymous nodes (#6) 💡
- **Days 3-4**: Polymorphic schema (#7) 💡
- **Day 5**: Documentation + release prep

---

## 🎯 Success Metrics

**Must Achieve** (for v0.5.2 release):
- ✅ Node uniqueness bug fixed (100% correct)
- ✅ Disconnected pattern error working (100% correct)
- ✅ Integration tests 75%+ pass rate
- ✅ Neo4j comparison infrastructure working
- ✅ 0 regressions in existing tests

**Nice to Achieve**:
- ✅ Enhanced error messages implemented
- ✅ Anonymous node support complete
- ✅ Polymorphic schema support (if time permits)

**Quality Gates**:
- 100% unit test coverage maintained (423/423)
- All benchmarks still passing (14/14)
- No performance regressions (within 5%)
- Documentation updated
- CHANGELOG.md updated

---

## 🚫 Out of Scope (Defer to v0.6.0)

### Major Features (Phase 3 Roadmap)
- ❌ Vector search integration
- ❌ GraphRAG support
- ❌ Advanced Neo4j functions (graph algorithms beyond PageRank)
- ❌ Inline property syntax (`{prop: value}`)
- ❌ Map literals and list comprehensions
- ❌ Subqueries and CALL procedures
- ❌ CREATE/UPDATE/DELETE operations (read-only engine)

### Performance Optimizations
- ❌ Query plan caching improvements (already excellent)
- ❌ Parallel query execution
- ❌ Materialized views
- ❌ Index recommendations

### Infrastructure
- ❌ Kubernetes operators
- ❌ Monitoring dashboards
- ❌ Distributed tracing
- ❌ Load testing suite

---

## 💬 User Feedback Requested

**Questions for Users**:

1. **Bug Priority**: Are node uniqueness and disconnected patterns the most painful issues?
2. **Feature Requests**: What's missing that blocks your use case?
3. **Neo4j Compatibility**: How important is 100% Neo4j compatibility vs. ClickHouse-specific features?
4. **Error Messages**: What errors are most confusing right now?
5. **Performance**: Any queries that are slower than expected?

**How to Provide Feedback**:
- GitHub Issues: https://github.com/genezhang/clickgraph/issues
- Discussions: https://github.com/genezhang/clickgraph/discussions
- Email: [if available]

---

## 📝 Decision Log

**Date**: November 21, 2025

**Decision 1: Focus on Neo4j Compatibility**
- **Rationale**: Users expect graph query semantics to match Neo4j
- **Impact**: Prioritize node uniqueness and comparison testing
- **Trade-off**: Defer new features to maintain quality

**Decision 2: Target 75% Integration Test Pass Rate**
- **Rationale**: 100% unrealistic due to feature gaps, 75% shows progress
- **Impact**: Clear documentation of what's not supported
- **Trade-off**: Some features deferred to v0.6.0

**Decision 3: Optional Enhancements Based on Timeline**
- **Rationale**: Critical bugs must be fixed, enhancements are best-effort
- **Impact**: May release with fewer features if quality gates not met
- **Trade-off**: Quality over feature count

---

## 🔗 References

**Source Documents**:
- `KNOWN_ISSUES.md` - Bug tracking and enhancement proposals
- `ROADMAP.md` - Long-term feature planning
- `CHANGELOG.md` - Recent changes and patterns
- `STATUS.md` - Current implementation status

**Test Results**:
- Unit tests: 423/423 (100%)
- Integration tests: 197/308 (64%)
- Benchmark tests: 14/14 (100%)

**User Feedback Channels**:
- GitHub Issues (bug reports)
- GitHub Discussions (feature requests)
- Docker Hub (deployment feedback)

---

## ✅ Next Steps

**Immediate Actions** (Today/Tomorrow):
1. 📋 Review this planning document with team/stakeholders
2. 💬 Gather user feedback on priorities
3. 🎯 Confirm scope for v0.5.2
4. 📅 Create detailed task breakdown for Week 1
5. 🏗️ Set up Neo4j test infrastructure

**Before Starting Development**:
1. ✅ Get approval on scope
2. ✅ Confirm timeline (2-3 weeks acceptable?)
3. ✅ Identify any blocking dependencies
4. ✅ Set up issue tracking in GitHub
5. ✅ Create v0.5.2 milestone

**Communication**:
1. Announce v0.5.2 planning on GitHub Discussions
2. Invite user feedback on priorities
3. Share progress updates weekly
4. Create release notes template

---

**Last Updated**: November 21, 2025  
**Next Review**: After user feedback (1-2 days)  
**Owner**: Development Team  
**Status**: 📋 DRAFT - Pending Approval
