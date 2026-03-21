# ClickGraph Roadmap

**Last Updated**: March 21, 2026
**Current Version**: v0.6.5-dev (Phase 3 Complete, Phase 4 In Progress)

This document outlines delivered features, current status, and planned enhancements for ClickGraph.

---

## 🎯 Current Status

### Phase 3 Complete ✅ (November 2025 – February 2026)

**LDBC Social Network Benchmark**: 36/37 queries passing

- ✅ LDBC SNB schema & data loading (SF0.1–SF10)
- ✅ All 14 Interactive Complex queries (IC-1 through IC-14)
- ✅ 7/7 Interactive Short queries (IS-1 through IS-7)
- ✅ 15+ Business Intelligence queries (BI-1 through BI-20)
- ✅ Official Cypher queries from LDBC reference implementation
- ✅ Weighted shortest path (complex-14)
- ✅ Baseline performance analysis documented

**Advanced Cypher features delivered during Phase 3**:
- ✅ UNWIND (ARRAY JOIN) support
- ✅ Pattern comprehension (correlated subqueries)
- ✅ List comprehension with `arrayCount`, `arrayConcat`
- ✅ CASE expressions (simple and searched forms)
- ✅ NOT EXISTS subquery splitting
- ✅ Map property access (`node.prop.subprop`)
- ✅ Chained WITH clauses (5+ WITH chains)
- ✅ Supertype inference for polymorphic nodes
- ✅ Variable scope resolution redesign (foundational)
- ✅ Composite ID support in VLP

**Tooling & compatibility delivered during Phase 3**:
- ✅ `apoc.meta.schema()` for MCP server compatibility
- ✅ LLM-powered schema design tool
- ✅ Neo4j Browser demo with click-to-expand
- ✅ Graph-notebook (Jupyter) demo
- ✅ GraphRAG `format: "Graph"` structured output (nodes/edges/stats)
- ✅ Schema-parameterized SQL generation tests (76 tests, 6 schemas)
- ✅ Browser interaction tests with full schema variant coverage

### Phase 2 Complete ✅ (November 18, 2025)
- ✅ Multi-tenancy with parameterized views (99% cache memory reduction)
- ✅ SET ROLE RBAC support (ClickHouse native column-level security)
- ✅ Auto-schema discovery via `system.columns`
- ✅ ReplacingMergeTree + FINAL support
- ✅ HTTP schema loading API (`POST /schemas/load`)
- ✅ Bolt Protocol 5.8 query execution
- ✅ Anonymous pattern support
- ✅ Complete documentation (19 wiki pages, comprehensive API reference)

### Phase 1 Complete ✅ (November 15, 2025)
- ✅ Parameter support & query cache (10-100x speedup)
- ✅ Bolt 5.8 protocol implementation
- ✅ 25+ Neo4j function mappings
- ✅ Benchmark suite (social network, ontime flight, LDBC SNB)
- ✅ plan_builder.rs modularization
- ✅ Undirected relationships (Direction::Either)

### Test Coverage
- 1,338 Rust unit/integration tests passing (100%)
- 1,748 Python integration test functions across 89 test files
- 36/37 LDBC SNB queries passing (97%)
- 76 schema-parameterized SQL generation tests across 6 schemas

### What's Working Well
- Core graph traversal patterns (MATCH, WHERE, RETURN, WITH)
- Variable-length paths (`*`, `*1..3`, `*..5`, `*0..`)
- Shortest path algorithms (`shortestPath()`, `allShortestPaths()`, weighted)
- OPTIONAL MATCH (LEFT JOIN semantics)
- Multiple relationship types (`[:TYPE1|TYPE2]`)
- UNWIND, pattern comprehension, list comprehension
- PageRank algorithm
- Multi-schema architecture with USE clause
- Neo4j Bolt protocol v5.8
- View-based graph model (YAML configuration)
- Query cache with LRU eviction (10-100x speedup)
- Multi-tenancy & RBAC (parameterized views + SET ROLE)
- Auto-schema discovery (zero-config column mapping)
- GraphRAG structured output (`format: "Graph"`)
- ClickHouse function pass-through (`ch.`/`chagg.` prefixes, ~150 aggregates)
- Vector similarity via `ch.cosineDistance()`, `ch.L2Distance()`, `ch.dotProduct()`
- `apoc.meta.schema()` for MCP/LLM tool integration

---

## 📋 Implementation Roadmap

### ✅ Phase 1: Foundation & Quick Wins (v0.4.0 — November 2025) **COMPLETE**

| Feature | Status |
|---------|--------|
| Parameter support & query cache | ✅ Complete |
| Bolt Protocol 5.8 query execution | ✅ Complete |
| Neo4j Functions (Phase 1: 25+ core) | ✅ Complete |
| Benchmark suite (social network, ontime flight, LDBC SNB) | ✅ Complete |
| Code refactoring (plan_builder.rs) | ✅ Complete |
| Undirected relationships | ✅ Complete |

---

### ✅ Phase 2: Enterprise Readiness (v0.5.0 — November 2025) **COMPLETE**

| Feature | Status |
|---------|--------|
| RBAC & Row-Level Security | ✅ Complete |
| Multi-Tenant Support | ✅ Complete |
| Wiki Documentation (19 pages) | ✅ Complete |
| ReplacingMergeTree & FINAL | ✅ Complete |
| Auto-Schema Discovery | ✅ Complete |

---

### ✅ Phase 3: Industry Benchmarks & Correctness (v0.6.x — Nov 2025 – Feb 2026) **COMPLETE**

| Feature | Status |
|---------|--------|
| LDBC SNB schema & data loading | ✅ Complete (SF0.1–SF10) |
| LDBC SNB Interactive Complex (IC-1–IC-14) | ✅ 14/14 |
| LDBC SNB Interactive Short (IS-1–IS-7) | ✅ 7/7 |
| LDBC SNB Business Intelligence | ✅ 15+ queries |
| Overall LDBC score | ✅ 36/37 (97%) |
| Performance baseline analysis | ✅ Documented |
| UNWIND support | ✅ Complete |
| Pattern & list comprehension | ✅ Complete |
| NOT EXISTS subqueries | ✅ Complete |
| Map property access | ✅ Complete |
| Variable scope redesign | ✅ Complete |
| Composite ID support in VLP | ✅ Complete |
| `apoc.meta.schema()` for MCP | ✅ Complete |
| LLM-powered schema design tool | ✅ Complete |
| Neo4j Browser & graph-notebook demos | ✅ Complete |
| GraphRAG `format: "Graph"` output | ✅ Complete |
| Schema-parameterized test infrastructure | ✅ 76 tests, 6 schemas |

**Remaining**: 1 LDBC query (IS-2, blocked by WITH+VLP CTE reference edge case)

---

### 🎯 Phase 4: AI/ML Integration & Scale (v0.6.3–v0.6.6 — Q1-Q2 2026) **IN PROGRESS**

**Focus**: Vector search, GraphRAG enhancements, billion-scale performance

| Priority | Feature | Status | Notes |
|----------|---------|--------|-------|
| ~~1️⃣~~ | ~~GraphRAG structured output~~ | ✅ **Complete** | `format: "Graph"` with deduplication |
| ~~2️⃣~~ | ~~`apoc.meta.schema()` for MCP~~ | ✅ **Complete** | LLM tool integration |
| ~~3️⃣~~ | ~~LLM-powered schema design~~ | ✅ **Complete** | Interactive schema generation |
| ~~4️⃣~~ | ~~**Vector similarity search**~~ | ✅ **Complete** | Via `ch.cosineDistance()`, `ch.L2Distance()`, etc. |
| ~~5️⃣~~ | ~~**ClickHouse function pass-through**~~ | ✅ **Complete** | `ch.` scalar + `chagg.` aggregate prefixes, lambda support |
| ~~6️⃣~~ | ~~**Neo4j Functions (55+)**~~ | ✅ **Complete** | Full function parity achieved |
| ~~7️⃣~~ | ~~**Query-time timezone support**~~ | ✅ **Complete** | Via ClickHouse function support |
| ~~8️⃣~~ | ~~**Cluster load balancing** (`CLICKHOUSE_CLUSTER`)~~ | ✅ **Complete** | Round-robin across cluster replicas via `system.clusters` |
| 9️⃣ | **GraphRAG Phase 2** (search procedures) | 🔶 **In Progress** | Vector search ✅, fulltext search ✅, context ranking/citation remaining |
| 🔟 | **Go bindings** (`clickgraph-go` via UniFFI) | ✅ **Complete** | Auto-generated via UniFFI + uniffi-bindgen-go, idiomatic Go API |
| 1️⃣1️⃣ | **Billion-scale benchmarks** (SF100+) | 💰 Seeking sponsorship | Requires dedicated infrastructure |

---

### 🎯 Phase 5: Customer-Driven Features & Performance (v0.6.7+ — Q3 2026+)

**Focus**: Features driven by customer needs and production feedback

| Feature | Status | Notes |
|---------|--------|-------|
| Query optimizer improvements | 📋 Planned | CTE reduction, JOIN ordering, filter pushdown |
| Customer-requested Cypher patterns | 📋 On demand | Based on production usage feedback |
| Performance tuning for specific workloads | 📋 On demand | Query-plan-level optimizations |

---

## 🔄 Feature Status Summary

### Delivered Features

| Feature | Phase | Delivered |
|---------|-------|-----------|
| Parameter support & query cache | Phase 1 | Nov 2025 |
| Bolt Protocol 5.8 | Phase 1 | Nov 2025 |
| Neo4j functions (25+ core) | Phase 1 | Nov 2025 |
| Undirected relationships | Phase 1 | Nov 2025 |
| Multi-tenant support | Phase 2 | Nov 2025 |
| RBAC & row-level security | Phase 2 | Nov 2025 |
| ReplacingMergeTree + FINAL | Phase 2 | Nov 2025 |
| Auto-schema discovery | Phase 2 | Nov 2025 |
| Wiki documentation (19 pages) | Phase 2 | Nov 2025 |
| LDBC SNB benchmark (36/37) | Phase 3 | Feb 2026 |
| UNWIND support | Phase 3 | Jan 2026 |
| Pattern comprehension | Phase 3 | Feb 2026 |
| List comprehension | Phase 3 | Feb 2026 |
| NOT EXISTS subqueries | Phase 3 | Feb 2026 |
| Map property access | Phase 3 | Feb 2026 |
| Variable scope redesign | Phase 3 | Jan 2026 |
| Composite ID support | Phase 3 | Jan 2026 |
| `apoc.meta.schema()` for MCP | Phase 3 | Mar 2026 |
| LLM-powered schema design | Phase 3 | Feb 2026 |
| Neo4j Browser demo | Phase 3 | Feb 2026 |
| GraphRAG `format: "Graph"` | Phase 4 | Mar 2026 |
| Vector similarity search | Phase 4 | Nov 2025 |
| ClickHouse function pass-through (`ch.`/`chagg.`) | Phase 4 | Nov 2025 |
| Lambda expression support for CH functions | Phase 4 | Dec 2025 |
| Neo4j functions (55+, full parity) | Phase 4 | Nov 2025 |
| Query-time timezone support | Phase 4 | Nov 2025 |
| Go bindings (UniFFI) | Phase 4 | Mar 2026 |

### Planned Features

| Feature | Target Phase | Priority |
|---------|-------------|----------|
| Cluster load balancing (`CLICKHOUSE_CLUSTER`) | Phase 4 | 🔥 High |
| GraphRAG Phase 2 (ranking, citation) | Phase 4 | 🌟 Medium-High |
| Billion-scale benchmarks | Phase 4 | 💰 Seeking sponsorship |
| Query optimizer improvements | Phase 5 | 🔥 High |
| Customer-requested Cypher patterns | Phase 5 | On demand |
| Performance tuning for workloads | Phase 5 | On demand |

---

## 📊 Key Metrics

| Metric | Value |
|--------|-------|
| Rust unit/integration tests | 1,338 |
| Python integration tests | 1,748 |
| LDBC SNB queries passing | 36/37 (97%) |
| Neo4j function mappings | 55+ |
| ClickHouse pass-through functions | ~150 aggregates + all scalar |
| Wiki documentation pages | 19 |
| Schema test variants | 6 |
| Supported schema patterns | 5 (standard, FK-edge, denormalized, polymorphic, coupled) |
