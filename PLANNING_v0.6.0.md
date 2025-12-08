# ClickGraph v0.6.0 Planning

**Created**: December 7, 2025  
**Current Version**: v0.5.4

---

## 🔍 Post-v0.5.4 Assessment

### What Went Well
- ✅ Cross-table query support (Issue #12) - major user request delivered
- ✅ Smart type inference - reduces schema boilerplate
- ✅ FK-Edge patterns - enables file system/org chart use cases
- ✅ 1,378 tests passing - solid test coverage
- ✅ 20M row OnTime benchmark validated
- ✅ PatternSchemaContext abstraction started (v2 architecture)

### Technical Debt Identified

**1. Compiler Warnings: 100 warnings**
```
- Unused imports: ~15
- Unused variables: ~40  
- Unreachable patterns: ~5
- Mutable variables that don't need to be: ~5
- Misc: ~35
```

**2. Large/Complex Files (17K lines in 4 files)**
| File | Lines | Issue |
|------|-------|-------|
| `graph_join_inference.rs` | 5,555 | Largest file, multiple schema strategies |
| `plan_builder.rs` | 5,698 | SQL generation, many render methods |
| `match_clause.rs` | 3,292 | Pattern parsing complexity |
| `cte_extraction.rs` | 2,429 | CTE generation logic |

**3. Architecture Improvements Pending**
- PatternSchemaContext v2 needs to become default (currently behind env toggle)
- Debug prints in query planner still present (acceptable for v0.5.4)
- Some duplication between v1 and v2 code paths

### Roadmap Drift Analysis

**Original Roadmap (from ROADMAP.md)**:
- Phase 3: LDBC SNB Benchmarks (January-February 2026) - NOT STARTED
- Phase 4: AI/ML Integration (March-April 2026) - NOT STARTED
- Phase 5: Advanced Features (Q3 2026+) - PARTIAL

**What Actually Got Built (User-Driven)**:
- ✅ Denormalized edge support (zeek logs, ontime flights)
- ✅ Polymorphic edge patterns
- ✅ Cross-table queries
- ✅ FK-Edge patterns
- ✅ Smart inference
- ✅ String predicates (STARTS WITH, ENDS WITH, CONTAINS)
- ✅ EXISTS subqueries (from Phase 5!)
- ✅ WITH+MATCH chaining

**Assessment**: User-driven development delivered high-value features, but deviated from benchmark-focused roadmap. Both approaches have merit.

---

## 📋 v0.6.0 Goals

### Option A: Code Quality Focus
**Theme**: "Solid Foundation"

| Priority | Task | Effort | Impact |
|----------|------|--------|--------|
| 1️⃣ | **Fix 100 compiler warnings** | 2-3 hours | Clean builds |
| 2️⃣ | **Make PatternSchemaContext v2 default** | 1 day | Cleaner architecture |
| 3️⃣ | **Remove v1 code paths** | 2-3 days | -2,000 lines |
| 4️⃣ | **Split large files** | 3-5 days | Maintainability |
| 5️⃣ | **Remove remaining debug prints** | 1 hour | Clean output |
| 6️⃣ | **Documentation cleanup** | 1 day | Accurate docs |

**Deliverables**:
- Zero compiler warnings
- Single code path (v2 only)
- No files over 2,000 lines
- Clean release builds (no debug output)

### Option B: Feature Focus
**Theme**: "Complete the Graph"

| Priority | Feature | Effort | Impact |
|----------|---------|--------|--------|
| 1️⃣ | **LDBC SNB SF0.1 validation** | 2 weeks | Credibility |
| 2️⃣ | **Graph algorithms (centrality)** | 1-2 weeks | Analytics |
| 3️⃣ | **UNWIND support** | 3-5 days | List operations |
| 4️⃣ | **Map projections** | 2-3 days | Convenience |

### Option C: Balanced (Recommended)
**Theme**: "Quality + Value"

**Week 1: Code Quality Sprint**
- [ ] Fix all 100 compiler warnings (`cargo fix` + manual)
- [ ] Make PatternSchemaContext v2 default
- [ ] Remove debug prints from query planner
- [ ] Update ROADMAP.md to reflect reality

**Week 2-3: Strategic Features**
- [ ] Choose ONE of:
  - LDBC SNB SF0.1 (credibility)
  - Graph algorithms (analytics value)
  - UNWIND + Map projections (Cypher completeness)

**Week 4: Polish**
- [ ] Documentation sync
- [ ] Test coverage for new features
- [ ] Release preparation

---

## 🛠️ Code Quality Tasks (Detailed)

### 1. Fix Compiler Warnings
```bash
# Auto-fix what's possible
cargo fix --lib -p clickgraph --allow-dirty

# Manual fixes needed for:
# - Unused variables in match arms (use _ prefix)
# - Unreachable patterns (remove dead code)
# - Suspicious double ref operations
```

### 2. PatternSchemaContext v2 Migration
```
Current state:
- v2 code exists in graph_join_inference.rs
- Toggle: USE_PATTERN_SCHEMA_V2=1
- Tests pass with both paths

Migration steps:
1. Run full test suite with v2 enabled
2. Remove env toggle, make v2 default
3. Remove v1 code paths
4. Update documentation
```

### 3. File Splitting Candidates

**graph_join_inference.rs (5,555 lines)**:
```
Suggested split:
├── graph_join_inference/
│   ├── mod.rs              # Main entry, orchestration
│   ├── traditional.rs      # Traditional (own-table) patterns
│   ├── denormalized.rs     # Denormalized edge patterns
│   ├── polymorphic.rs      # Polymorphic edge handling
│   ├── fk_edge.rs          # FK-edge patterns
│   └── helpers.rs          # Shared utilities
```

**plan_builder.rs (5,698 lines)**:
```
Suggested split:
├── plan_builder/
│   ├── mod.rs              # Main entry
│   ├── select_builder.rs   # SELECT clause generation
│   ├── from_builder.rs     # FROM/JOIN clause generation
│   ├── where_builder.rs    # WHERE clause generation
│   ├── group_order.rs      # GROUP BY, ORDER BY, LIMIT
│   └── cte_builder.rs      # CTE/WITH clause generation
```

---

## 📊 Updated Roadmap Proposal

### Near Term (v0.6.0 - December 2025)
- Code quality sprint (warnings, v2 migration)
- ONE strategic feature (TBD based on user feedback)

### Medium Term (v0.7.0 - Q1 2026)
- LDBC SNB SF0.1-SF1 validation
- Graph algorithms (degree centrality, PageRank improvements)
- Performance optimization based on benchmark findings

### Long Term (v0.8.0+ - Q2 2026)
- AI/ML integration (vector search, GraphRAG)
- Advanced Cypher features
- Billion-scale benchmarks

---

## 🧠 Vector Search / GraphRAG Architecture (Future)

**Research Finding (Dec 7, 2025)**: ClickHouse has NO built-in embedding generation.

### What ClickHouse Provides
- ✅ Vector storage: `Array(Float32)` columns
- ✅ Distance functions: `cosineDistance()`, `L2Distance()`, `dotProduct()`
- ✅ Approximate search: HNSW indexes (`vector_similarity` index type)
- ❌ NO embedding generation (no `encode()` or similar)

### Architecture Decision: Deferred Plugin Approach

**Embedding generation must be external:**
```
Client App                    ClickGraph              ClickHouse
    │                             │                       │
    │ Text: "find similar..."     │                       │
    │ ───────────────────────────>│                       │
    │                             │                       │
    │ [Plugin: OpenAI/Cohere/etc] │                       │
    │ ←── embedding vector ───────│                       │
    │                             │                       │
    │ MATCH (n) WHERE             │ SELECT ... ORDER BY   │
    │ cosine(n.embedding, $vec)   │ cosineDistance(...)   │
    │ ───────────────────────────>│ ─────────────────────>│
```

**What ClickGraph Can Do Now (v0.5.5/v0.6.0)**:
- Map `gds.similarity.cosine()` → `1 - cosineDistance(a, b)`
- Map `gds.similarity.euclidean()` → `L2Distance(a, b)`
- Support `ORDER BY ... LIMIT k` for top-k similarity

**What Requires Plugin (v0.8.0+)**:
- Text-to-embedding conversion (OpenAI, Cohere, local models)
- Automatic embedding on INSERT (write support needed)
- Hybrid search (vector + graph traversal)

**Decision**: Defer embedding plugin to v0.8.0. Focus on:
1. Distance function mappings (easy, v0.5.5)
2. GraphRAG query patterns (v0.7.0)
3. Embedding service integration (v0.8.0)

---

## 📝 Decision Needed

**Question**: For v0.6.0, which approach?

1. **Option A**: Pure code quality (no new features)
2. **Option B**: Feature-focused (defer cleanup)
3. **Option C**: Balanced (quality sprint + 1 feature)

**Recommendation**: Option C - Clean up the codebase first (week 1), then add strategic value (weeks 2-4). This maintains momentum while reducing technical debt.

---

## 🎯 Success Criteria for v0.6.0

- [ ] Zero compiler warnings
- [ ] PatternSchemaContext v2 is default (no toggle)
- [ ] All tests still passing (1,378+)
- [ ] At least one new feature or benchmark
- [ ] Updated documentation
- [ ] Clean release (no debug output)
