# ClickGraph Roadmap

**Last Updated**: November 18, 2025  
**Current Version**: v0.5.0 (Phase 2 Complete! 🎉)

This document outlines planned features, enhancements, and benchmark tasks for ClickGraph development.

---

## 🎯 Current Status

**Phase 2 Complete** ✅ (November 18, 2025):
- ✅ Multi-tenancy with parameterized views (99% cache memory reduction)
- ✅ SET ROLE RBAC support (ClickHouse native column-level security)
- ✅ Auto-schema discovery via `system.columns`
- ✅ ReplacingMergeTree + FINAL support
- ✅ HTTP schema loading API (`POST /schemas/load`)
- ✅ Bolt Protocol 5.8 query execution (all 4 E2E tests passing)
- ✅ Anonymous pattern support (`()-[r]->()` and `MATCH (a)-[]->(b)`)
- ✅ Complete documentation (19 wiki pages, comprehensive API reference)

**Phase 1 Complete** ✅ (November 15, 2025):
- ✅ Parameter support & query cache (10-100x speedup)
- ✅ Bolt 5.8 protocol implementation
- ✅ 25+ Neo4j function mappings
- ✅ Benchmark suite for scale 1-10 (1K-10K users)
- ✅ **Major refactoring**: plan_builder.rs modularization (Nov 14-15)
- ✅ **Undirected relationships**: Direction::Either support (Nov 15)

**What's Working Well**:
- ✅ Core graph traversal patterns (MATCH, WHERE, RETURN)
- ✅ Variable-length paths (`*`, `*1..3`, `*..5`)
- ✅ Shortest path algorithms (`shortestPath()`, `allShortestPaths()`)
- ✅ OPTIONAL MATCH (LEFT JOIN semantics)
- ✅ Multiple relationship types (`[:TYPE1|TYPE2]`)
- ✅ PageRank algorithm
- ✅ Multi-schema architecture with USE clause
- ✅ **Neo4j Bolt protocol v5.8 support** - Nov 12, 2025
- ✅ View-based graph model (YAML configuration)
- ✅ **Query Cache with LRU eviction (10-100x speedup)** - Nov 10, 2025
- ✅ **Undirected relationships** (`(a)-[r]-(b)` patterns) - Nov 15, 2025
- ✅ **Multi-tenancy & RBAC** (parameterized views + SET ROLE) - Nov 17, 2025
- ✅ **Auto-schema discovery** (zero-config column mapping) - Nov 17, 2025

**Test Coverage**:
- 422/422 Rust unit tests passing (100%) ✨
- 236/400 Python integration tests passing (59% for implemented features)
- 14/14 benchmark queries passing (100%)
- 6/6 query cache unit tests + 5/5 e2e tests (100%)
- 4/4 Bolt 5.8 E2E tests passing (100%)
- 11 multi-tenancy test classes (comprehensive coverage)

**Performance Baseline** (Nov 13, 2025):
- Scale 1 (1K users, 100K edges): 2077ms mean
- Scale 10 (10K users, 1M edges): 2088ms mean
- Overhead: Only 0.5% for 10x data scale
- Multi-tenant cache: 2x speedup (18ms → 9ms)

---

## 📋 Implementation Roadmap & Prioritization

*Phased approach based on dependencies, impact, and effort*

### 🎯 Phase 1: Foundation & Quick Wins ✅ **COMPLETE** (v0.4.0 - November 2025)
**Focus**: High-impact features with low dependencies  
**Duration**: 4-6 weeks (Oct 1 - Nov 15, 2025)

| Priority | Feature | Effort | Impact | Status |
|----------|---------|--------|--------|--------|
| ~~1️⃣~~ | ~~**#9 Parameter Support & Query Cache**~~ | ~~2-3 weeks~~ | ~~🔥 Critical~~ | ✅ **COMPLETE** (Nov 10, 2025) |
| ~~2️⃣~~ | ~~**Bolt Protocol Query Execution**~~ | ~~1-2 days~~ | ~~🔥 High~~ | ✅ **COMPLETE** (Nov 12, 2025) |
| ~~3️⃣~~ | ~~**#2 Neo4j Functions** (Phase 1: Core)~~ | ~~1-2 weeks~~ | ~~🔥 High~~ | ✅ **COMPLETE** (Nov 12, 2025) |
| ~~4️⃣~~ | ~~**Benchmark Suite** (Small/Medium)~~ | ~~1 week~~ | ~~🔥 High~~ | ✅ **COMPLETE** (Nov 13, 2025) |
| ~~5️⃣~~ | ~~**Code Quality & Refactoring**~~ | ~~2 days~~ | ~~🌟 Medium~~ | ✅ **COMPLETE** (Nov 14-15, 2025) |
| ~~6️⃣~~ | ~~**Undirected Relationships**~~ | ~~1 day~~ | ~~🌟 Medium~~ | ✅ **COMPLETE** (Nov 15, 2025) |

**Phase 1 Deliverables** ✅:
- ✅ Parameters working in HTTP API (Nov 10, 2025)
- ✅ Query plan cache reducing latency by 10-100x (Nov 10, 2025)
- ✅ Bolt 5.8 protocol complete with E2E tests (Nov 12, 2025)
- ✅ 25+ Neo4j functions supported (datetime, string, math) (Nov 12, 2025)
- ✅ Reproducible benchmarks for 1K-10K scale (Nov 13, 2025)
- ✅ Major code refactoring: plan_builder.rs modularization (Nov 14-15, 2025)
- ✅ Undirected relationship support with OR JOIN logic (Nov 15, 2025)

**v0.4.0 Release Ready** 🚀:
- ✅ Production-ready query caching
- ✅ Neo4j Bolt 5.8 protocol compatibility
- ✅ Neo4j function compatibility improved (25+ functions)
- ✅ Performance baseline established (14/14 queries, 2077-2088ms)
- ✅ Improved code maintainability (22% size reduction in plan_builder.rs)
- ✅ Enhanced feature completeness (undirected relationships)

---

### 🎯 Phase 2: Enterprise Readiness (v0.5.0 - January-February 2026) ✅ **COMPLETE**
**Focus**: Security, multi-tenancy, documentation  
**Duration**: 8-10 weeks  
**Started**: November 15, 2025  
**Completed**: November 17, 2025  
**Progress**: 5/5 complete (100%)

| Priority | Feature | Effort | Impact | Status |
|----------|---------|--------|--------|--------|
| ~~1️⃣~~ | ~~**#5 RBAC & Row-Level Security**~~ | ~~3-4 weeks~~ | ~~🔥 Critical~~ | ✅ **COMPLETE** (Nov 15-17, 2025) |
| ~~2️⃣~~ | ~~**#1 Multi-Tenant Support**~~ | ~~2-3 weeks~~ | ~~🔥 Critical~~ | ✅ **COMPLETE** (Nov 15-17, 2025) |
| ~~3️⃣~~ | ~~**Wiki Documentation (Foundation)**~~ | ~~1 week~~ | ~~🔥 High~~ | ✅ **COMPLETE** (Nov 17, 2025) |
| ~~4️⃣~~ | ~~**#6 ReplacingMergeTree & FINAL**~~ | ~~1-2 weeks~~ | ~~🌟 Medium-High~~ | ✅ **COMPLETE** (Nov 16, 2025) |
| ~~5️⃣~~ | ~~**Auto-Schema Discovery**~~ | ~~1-2 weeks~~ | ~~🌟 Medium~~ | ✅ **COMPLETE** (Nov 16, 2025) |

**Phase 2 Deliverables**:
- ✅ Complete RBAC system with role definitions (SET ROLE support)
- ✅ Row-level security with parameterized views (99% cache memory reduction)
- ✅ Multi-tenant query isolation working (HTTP + Bolt protocols)
- ✅ Complete documentation: `docs/multi-tenancy.md` + example schemas
- ✅ Integration test suite (11 test classes)
- ✅ **Wiki foundation complete**: 16 pages (14K+ lines) with schema-aligned examples
- ✅ **Critical bug fix**: RETURN whole node property expansion
- ✅ **Validation infrastructure**: Automated wiki query validation system
- ✅ **ReplacingMergeTree support with FINAL**: Mutable graph data support
- ✅ **Auto-schema from ClickHouse**: `DESCRIBE TABLE` with caching

**Completed Features (Nov 15-17, 2025)**:

**1. RBAC & Row-Level Security** ✅:
- Parameterized views: `view_parameters: [tenant_id, region, ...]`
- SQL generation with placeholders: `view_name(param=$paramName)`
- Cache optimization: Single template shared across all tenants
- SET ROLE support for ClickHouse native RBAC
- Multi-parameter views (unlimited parameters)
- Performance: 99% memory reduction, 2x faster on cache hits
- Commits: 805db43, 5d0f712, 7ea4a05, 2d1cb04

**2. Multi-Tenant Support** ✅:
- HTTP API: `view_parameters` field in requests
- Bolt protocol: Extract from RUN message metadata
- Complete documentation: `docs/multi-tenancy.md` (300+ lines)
- Example schemas: simple + encrypted multi-tenancy
- Integration tests: 11 test classes, E2E validation
- 5 multi-tenant patterns documented
- Commits: fa215e3, 5a1303d, 4ad7563, 8c21fca, a639049

**3. Wiki Documentation Foundation** ✅ (Nov 17, 2025):
- **16 wiki pages created**: 14,300+ lines of content
  - Quick Start Guide, Cypher Basic Patterns, Functions, Multi-Hop Traversals
  - Use cases: Social Network, Fraud Detection, Knowledge Graphs
  - Production: Docker/K8s Deployment, Best Practices, Troubleshooting
  - Advanced: Schema Configuration, Multi-Tenancy, RBAC, Performance
  - Architecture: Internals documentation
- **Schema alignment**: All examples use benchmark schema properties
- **HTML comment strategy**: Future features preserved but hidden
- **Validation infrastructure**:
  - `scripts/validate_wiki_docs.py`: Automated query testing
  - `docs/FUTURE_FEATURES_TRACKER.md`: Tracks commented examples
  - `scripts/utils/find_commented_examples.py`: Discovery utility
- **Critical bug fix**: RETURN whole node property expansion
  - Fixed "No select items found" error for `RETURN u` queries
  - Added `expand_node_properties()` helper
  - Now generates complete SELECT clause with all properties
- **Quality assurance**: PowerShell scripts for wiki validation workflow
- Commits: cc5bd6f (bug fix), b9b09f5 (docs), b10be71 (scripts)

**4. ReplacingMergeTree & FINAL Support** ✅ (Nov 16, 2025):
- Support for mutable ClickHouse tables with `ReplacingMergeTree` engine
- Automatic `FINAL` clause generation for deduplicated reads
- Enables graph data updates and deletions
- Compatible with CDC patterns

**5. Auto-Schema Discovery** ✅ (Nov 16, 2025):
- Automatic schema detection from ClickHouse `DESCRIBE TABLE`
- Schema caching for performance
- Reduces manual YAML configuration
- Supports dynamic table structures

**Phase 2 Summary**:
- **Duration**: 2 weeks (Nov 15-17, 2025)
- **All goals achieved**: Security, multi-tenancy, documentation, mutable data, auto-discovery
- **Ready for v0.5.0 release**

---

**v0.5.0 Release Goals**:
- ✅ Enterprise security features complete
- ✅ Multi-tenant SaaS deployments enabled
- ✅ Wiki documentation foundation established (16 pages, 14K+ lines)
- ✅ Critical query bugs fixed (RETURN whole node)
- ✅ Schema evolution without YAML updates (auto-discovery)
- ✅ Mutable graph data support (ReplacingMergeTree + FINAL)
- ⏳ Wiki published to GitHub (ready for publishing)

**🚀 v0.5.0 RELEASE READY** - All Phase 2 features complete!

---

### 🎯 Phase 3: AI/ML Integration (v0.6.0 - March-April 2026)
**Focus**: Vector search, GraphRAG, advanced functions  
**Duration**: 6-8 weeks

| Priority | Feature | Effort | Impact | Rationale |
|----------|---------|--------|--------|-----------|
| 1️⃣ | **#7 Vector Similarity Search** | 2-3 weeks | 🔥 High | **AI Enabler**: High demand for RAG/embeddings. Independent implementation. |
| 2️⃣ | **#8 GraphRAG Support** (Phase 1) | 2-3 weeks | 🌟 Medium-High | **Depends on**: Vector search (#7). Define core requirements first. |
| 3️⃣ | **#2 Neo4j Functions** (Phase 2: Advanced) | 1-2 weeks | 🌟 Medium | **Completion**: List, aggregation, predicate functions. |
| 4️⃣ | **#3 Query-Time Timezone** | 1 week | 🌟 Medium | **Global Apps**: Depends on datetime functions (#2 Phase 1). |

**Deliverables**:
- ✅ Vector storage and KNN search
- ✅ Cosine/euclidean similarity functions
- ✅ GraphRAG retrieval patterns defined
- ✅ 50+ Neo4j functions supported
- ✅ Timezone-aware queries

**v0.6.0 Release Goals**:
- AI/ML workloads fully supported
- GraphRAG foundations in place
- Near-complete Neo4j function parity

---

### 🎯 Phase 4: Scale & Performance (v0.7.0 - May-June 2026)
**Focus**: Billion-scale benchmarks, optimization, flexibility  
**Duration**: 6-8 weeks

| Priority | Feature | Effort | Impact | Rationale |
|----------|---------|--------|--------|-----------|
| 1️⃣ | **Benchmark Suite** (Large/Ultra 1B+) | 2-3 weeks | 🔥 High | **Proof of Scale**: Validate billion-edge performance. Drives optimizations. |
| 2️⃣ | **#10 SQL Callback Hooks** | 1-2 weeks | 🌟 Medium | **Power Users**: Low complexity, high flexibility. No dependencies. |
| 3️⃣ | **#4 ClickHouse Pass-Through** | 1-2 weeks | 🔥 High | **Unlock CH Power**: Access 500+ CH functions. Independent feature. |
| 4️⃣ | **Optimization Pass** | 2-3 weeks | 🔥 High | **Performance**: Based on benchmark findings. CTE reduction, JOIN ordering, filter pushdown. |

**Deliverables**:
- ✅ 1B+ edge benchmarks published
- ✅ SQL hook system for customization
- ✅ ClickHouse function pass-through (`ch::` namespace)
- ✅ Query optimizer improvements
- ✅ Performance comparison vs Neo4j

**v0.7.0 Release Goals**:
- Proven billion-scale performance
- Advanced customization capabilities
- Optimized query execution

---

### 🎯 Phase 5: Advanced Features (v0.8.0+ - Q3 2026+)
**Focus**: Advanced Cypher, algorithms, patterns  
**Duration**: Ongoing

| Priority | Feature | Effort | Impact | Rationale |
|----------|---------|--------|--------|-----------|
| 1️⃣ | **Graph Algorithms** | 6-8 weeks | 🌟 Medium-High | **Analytics**: Centrality, community detection, connected components. Can be incremental. |
| 2️⃣ | **Path Comprehensions** | 3-5 days | 🌟 Medium | **Expressiveness**: Depends on list functions (#2). Nice-to-have. |
| 3️⃣ | **UNWIND Support** | 1 week | 🌟 Medium | **Convenience**: Standalone feature, moderate demand. |
| 4️⃣ | **Map Projections** | 3-5 days | 💡 Low-Medium | **Convenience**: Syntactic sugar, low priority. |
| 5️⃣ | **EXISTS Subqueries** | 1-2 weeks | 🌟 Medium | **Advanced Filtering**: Complex implementation, niche use case. |
| 6️⃣ | **CASE Enhancements** | 2-3 days | 💡 Low | **Polish**: Basic support exists, edge cases remain. |
| 7️⃣ | **Query Hints** | 2-3 weeks | 💡 Low | **Optimization**: For expert users only. Future. |

**Deliverables** (Incremental):
- Graph algorithms released one at a time
- Advanced Cypher features as needed
- Continuous optimization improvements

---

## 🔄 Dependency Analysis

### Critical Path Dependencies

```
Phase 1 Foundation:
#9 Parameters ──┬──> #1 Multi-Tenant
               ├──> #5 RBAC/RLS
               └──> (All query features)

#2 Functions (Phase 1) ──> #3 Timezone Support

Phase 2 Enterprise:
#9 Parameters + #5 RBAC ──> #1 Multi-Tenant

Phase 3 AI/ML:
#7 Vector Search ──> #8 GraphRAG
#2 Functions (Phase 1) ──> #3 Timezone

Phase 4 Scale:
Benchmarks ──> Optimization Pass
(No blockers for #10 SQL Hooks or #4 CH Pass-Through)
```

### Parallel Development Opportunities

**Can Work Simultaneously**:
- Documentation (#v0.5.0 Wiki) can run parallel to all phases
- Benchmarks can run alongside feature work
- Graph algorithms are independent of each other
- Neo4j functions can be added incrementally

---

## 🎯 Quick Reference: Feature Priority Matrix

### 🔥 Critical (Must-Have for Production)
1. **#9 Parameters & Cache** - Foundation for everything
2. **#5 RBAC & RLS** - Enterprise security requirement
3. **#1 Multi-Tenant** - SaaS/commercial blocker
4. **#7 Vector Search** - AI/ML demand is high
5. **#4 CH Pass-Through** - Unlock ClickHouse power
6. **Benchmark Suite** - Validate performance claims
7. **v0.5.0 Wiki** - Adoption blocker

### 🌟 High Value (Should-Have)
8. **#2 Neo4j Functions** - Compatibility & usability
9. **#6 ReplacingMergeTree** - Real-world data patterns
10. **#3 Timezone Support** - Global applications
11. **#10 SQL Hooks** - Power user flexibility
12. **#8 GraphRAG** - Emerging AI use case
13. **Graph Algorithms** - Analytics capabilities

### 💡 Nice-to-Have (Future)
14. **Path Comprehensions** - Convenience
15. **UNWIND** - Moderate demand
16. **EXISTS Subqueries** - Advanced scenarios
17. **Map Projections** - Syntactic sugar
18. **CASE Enhancements** - Edge cases
19. **Query Hints** - Expert optimization

---

## 🌍 Real-World Application Features

*Features driven by production use cases and enterprise requirements*

### High Priority (Business Critical)

#### 1. **Multi-Tenant Support** 🏢
**Status**: Not started  
**Estimated Effort**: 2-3 weeks  
**Impact**: High - Critical for SaaS deployments  
**Priority**: 🔥 High

**Requirements**:
- [ ] Parameterized views in ClickHouse for tenant isolation
- [ ] Tenant context injection in query execution
- [ ] Schema-per-tenant or shared schema with tenant_id patterns
- [ ] Performance isolation between tenants
- [ ] Query routing based on tenant context

**Design Considerations**:
- Explore ClickHouse parameterized views vs materialized views
- Evaluate tenant_id filtering at query plan level
- Consider multi-database vs single-database-with-filtering approaches
- Security: Ensure no cross-tenant data leakage

**Use Cases**:
- SaaS platforms with multiple customers
- Department-level data isolation in enterprises
- Partner/supplier data segregation

---

#### 2. **Neo4j Functions & Expressions** 🔧
**Status**: Partial (basic expressions work)  
**Estimated Effort**: 2-4 weeks  
**Impact**: High - Neo4j compatibility and feature parity  
**Priority**: 🔥 High

**Goal**: Support all Neo4j functions that can be translated to ClickHouse equivalents

**Function Categories**:

**Temporal Functions** ⏰:
- [ ] `datetime()` - Create datetime from string or components
- [ ] `date()` - Extract or create date
- [ ] `time()` - Extract or create time
- [ ] `timestamp()` - Unix timestamp conversion
- [ ] `duration()` - Time duration calculations
- [ ] Date arithmetic: `datetime() + duration({days: 7})`
- [ ] Date component extraction: `datetime().year`, `datetime().month`, etc.
- [ ] `localdatetime()` - Timezone-naive datetime

**String Functions**:
- [ ] `substring()`, `left()`, `right()` → ClickHouse `substring()`, etc.
- [ ] `toLower()`, `toUpper()` → ClickHouse `lower()`, `upper()`
- [ ] `trim()`, `ltrim()`, `rtrim()` → ClickHouse equivalents
- [ ] `reverse()`, `replace()`, `split()`
- [ ] String pattern matching functions

**Mathematical Functions**:
- [ ] `abs()`, `ceil()`, `floor()`, `round()`, `sign()`
- [ ] `sqrt()`, `exp()`, `log()`, `log10()`
- [ ] `sin()`, `cos()`, `tan()`, `asin()`, `acos()`, `atan()`
- [ ] `rand()` → ClickHouse `rand()`
- [ ] `pi()`, `e()`

**List Functions**:
- [ ] `size()` → ClickHouse `length()` for arrays
- [ ] `head()`, `tail()`, `last()`
- [ ] `range()` → ClickHouse `range()`
- [ ] `reduce()` → Array aggregations
- [ ] `extract()` → Array transformations

**Aggregation Functions** (beyond basic COUNT/SUM/AVG):
- [ ] `collect()` → ClickHouse `groupArray()`
- [ ] `percentileCont()`, `percentileDisc()` → ClickHouse percentile functions
- [ ] `stDev()`, `stDevP()` → ClickHouse `stddevSamp()`, `stddevPop()`

**Type Conversion Functions**:
- [ ] `toInteger()`, `toFloat()`, `toString()`, `toBoolean()`
- [ ] Map to ClickHouse `toInt64()`, `toFloat64()`, `toString()`, etc.

**Predicate Functions**:
- [ ] `exists()` - Check property existence
- [ ] `isEmpty()` - Check if list/string is empty
- [ ] Map to ClickHouse NULL checks and array operations

**ClickHouse Mapping Strategy**:
- Create function translation table: Neo4j function → ClickHouse equivalent
- Handle function signature differences (parameter order, types)
- Add unsupported function detection with clear error messages
- Document function compatibility matrix

---

#### 3. **Query-Time Timezone Support** 🌐
**Status**: Not started  
**Estimated Effort**: 1 week  
**Impact**: Medium-High - Critical for global applications  
**Priority**: 🔥 High

**Requirements**:
- [ ] Per-query timezone parameter: `SET timezone = 'America/New_York'`
- [ ] Timezone conversion in expressions: `datetime('2024-01-01', 'UTC', 'America/Los_Angeles')`
- [ ] Timezone-aware datetime comparisons
- [ ] Support for named timezones (IANA timezone database)
- [ ] Default timezone configuration per schema/tenant

**Implementation**:
- Leverage ClickHouse `toTimeZone()` function
- Add timezone context to query execution state
- Handle timezone in datetime parsing and formatting

**Use Cases**:
- Multi-region deployments
- Financial trading systems (market hours)
- Event scheduling across timezones
- Compliance reporting with local time requirements

---

#### 4. **ClickHouse Function Pass-Through** 🔌
**Status**: Not started  
**Estimated Effort**: 1-2 weeks  
**Impact**: High - Unlock full ClickHouse power  
**Priority**: 🔥 High

**Requirements**:
- [ ] Allow direct ClickHouse function calls in Cypher expressions
- [ ] Syntax: `ch::functionName(args)` or similar namespace
- [ ] Support all ClickHouse scalar functions
- [ ] Support ClickHouse aggregate functions
- [ ] Type mapping between Cypher and ClickHouse types

**Examples**:
```cypher
// Use ClickHouse-specific functions
MATCH (u:User)
WHERE ch::cityHash64(u.email) % 100 = 42
RETURN u.name, ch::formatReadableSize(u.data_size)

// Aggregate functions
MATCH (u:User)-[:PURCHASED]->(p:Product)
RETURN u.name, ch::quantile(0.95)(p.price) AS p95_price
```

**Benefits**:
- Access ClickHouse's rich function library (500+ functions)
- Use ClickHouse-specific optimizations
- Avoid reimplementing complex functions

---

#### 5. **RBAC and Row-Level Security** 🔒
**Status**: Not started  
**Estimated Effort**: 3-4 weeks  
**Impact**: High - Enterprise security requirement  
**Priority**: 🔥 High

**Requirements**:

**Role-Based Access Control (RBAC)**:
- [ ] Define user roles (admin, analyst, viewer, etc.)
- [ ] Permission model: schema-level, label-level, property-level
- [ ] Query authorization before execution
- [ ] Audit logging of access attempts

**Row-Level Security (RLS)**:
- [ ] Filter results based on user context
- [ ] Policy definitions: `users can see only their department's data`
- [ ] Automatic WHERE clause injection based on RLS policies
- [ ] Support for dynamic policies (time-based, attribute-based)

**Result Obfuscation**:
- [ ] Field masking: `email` → `****@example.com`
- [ ] Data redaction for sensitive properties
- [ ] Configurable obfuscation rules per role
- [ ] PII protection (names, addresses, SSNs, etc.)

**Implementation Approaches**:
- Integrate with ClickHouse RBAC (users, roles, grants)
- Custom authorization layer in ClickGraph
- Policy engine for RLS rules
- Result post-processing for obfuscation

**Use Cases**:
- Healthcare: HIPAA compliance, patient data protection
- Finance: PCI-DSS compliance, transaction privacy
- HR systems: Employee data confidentiality
- Multi-tenant SaaS: Cross-customer data isolation

---

#### 6. **ReplacingMergeTree & FINAL Keyword Support** 🔄
**Status**: Not started  
**Estimated Effort**: 1-2 weeks  
**Impact**: Medium-High - Critical for mutable data patterns  
**Priority**: 🌟 Medium

**Background**:
ClickHouse `ReplacingMergeTree` and `CollapsingMergeTree` require `FINAL` modifier to get deduplicated/collapsed results.

**Requirements**:
- [ ] Support `FINAL` keyword in generated SQL when needed
- [ ] Auto-detect ReplacingMergeTree tables from schema
- [ ] Option to force FINAL: `USE SCHEMA xyz WITH FINAL`
- [ ] Performance warnings (FINAL can be expensive)
- [ ] Consider ClickHouse views as abstraction layer

**Examples**:
```cypher
// Cypher stays the same, but generated SQL uses FINAL
MATCH (u:User)
WHERE u.status = 'active'
RETURN u.name

// Generated SQL for ReplacingMergeTree:
SELECT name FROM users FINAL WHERE status = 'active'
```

**Schema Detection**:
- [ ] Parse ClickHouse table DDL to detect engine type
- [ ] Store engine type in graph schema metadata
- [ ] Configurable FINAL behavior per table/schema

**Alternative Approach**:
- Define ClickHouse views with FINAL already applied
- Map Cypher labels to views instead of raw tables
- Pros: Simpler implementation, ClickHouse handles optimization
- Cons: Extra view layer, potential performance impact

**Use Cases**:
- User profile updates (latest version)
- Inventory systems (current stock levels)
- CDC (Change Data Capture) pipelines
- Event sourcing with compaction

---

#### 7. **Auto-Schema Discovery from ClickHouse** 🔍
**Status**: Tier 2 implemented (identity fallback), Tier 3 planned  
**Estimated Effort**: 1-2 weeks  
**Impact**: Medium - Improves DX for wide tables  
**Priority**: 🌟 Medium

**Background**:
Schema YAML files require explicit `property_mappings` for each column. For wide tables (100+ columns), this creates significant YAML maintenance burden and prevents schema evolution.

**Progressive Schema Evolution Tiers**:

**✅ Tier 1: Manual/Explicit (CURRENT - Fully Working)**
- User explicitly maps properties in YAML
- Example: `property_mappings: { name: full_name, email: email_address }`
- **Status**: Production-ready ✅

**✅ Tier 2: Identity Fallback (COMPLETED - Nov 10, 2025)**
- Unmapped properties pass through using identity mapping (property name = column name)
- No ClickHouse lookup, just assume matching names
- Fast, simple, works immediately
- **Status**: Implemented in `view_resolver.rs` ✅
- **Benefit**: Wide table support without hundreds of YAML mappings

**⏳ Tier 3: Auto-Schema from ClickHouse (FUTURE)**
- Query ClickHouse metadata: `DESCRIBE TABLE users`
- Discover all columns and data types automatically
- Cache schema information for performance
- Full schema evolution support without YAML updates
- **Status**: Planned for Phase 2 (v0.5.0)

**Implementation Approach (Tier 3)**:
```rust
// On schema load or refresh:
let columns = clickhouse_client
    .query("DESCRIBE TABLE ?")
    .bind(table_name)
    .fetch_all::<ColumnInfo>()
    .await?;

// Cache in schema:
node_schema.discovered_columns = columns;

// Property resolution becomes:
fn resolve_property(property: &str) -> Result<String> {
    // 1. Try explicit mapping
    if let Some(mapped) = property_mappings.get(property) {
        return Ok(mapped.clone());
    }
    
    // 2. Check discovered columns
    if discovered_columns.contains(property) {
        return Ok(property.to_string());
    }
    
    // 3. Error if not found
    Err(PropertyNotFound)
}
```

**Requirements**:
- [ ] Add `ColumnInfo` struct with name, data_type, default_value
- [ ] Query `DESCRIBE TABLE` or `system.columns` on schema load
- [ ] Cache discovered columns in `NodeSchema`/`RelationshipSchema`
- [ ] LRU cache with TTL for performance (default 5 minutes)
- [ ] Configuration option: `auto_discover_schema: true` in YAML
- [ ] Handle ClickHouse connection errors gracefully (fallback to YAML only)

**Schema YAML Evolution**:
```yaml
nodes:
  - label: User
    table: users
    id_column: user_id
    auto_discover: true  # Enable auto-discovery for this table
    property_mappings:
      # Only map the few that differ from column names
      name: full_name
      email: email_address
      # All other 198 columns automatically discovered from ClickHouse
```

**Performance Considerations**:
- Cache `DESCRIBE TABLE` results (TTL: 5 minutes default)
- Lazy loading: Only query ClickHouse when property accessed first time
- Option to pre-warm cache on server startup
- Configurable cache size and TTL

**Use Cases**:
- **Wide tables**: Analytics tables with 100+ columns
- **Schema evolution**: Add columns without updating YAML
- **Rapid prototyping**: Quick schema setup without manual mapping
- **Data warehouse integration**: Existing tables with many columns

**Alternative: Schema Registry Pattern**:
- External schema registry (e.g., Confluent Schema Registry)
- Centralized schema management across services
- Version control for schema evolution
- Considered for future enterprise features

---

#### 8. **Vector Similarity Search for AI Applications** 🤖
**Status**: Not started  
**Estimated Effort**: 2-3 weeks  
**Impact**: High - Critical for AI/ML workloads  
**Priority**: 🔥 High

**Requirements**:
- [ ] Store and query high-dimensional vectors (embeddings)
- [ ] Vector similarity functions: cosine, euclidean, dot product
- [ ] K-nearest neighbors (KNN) search
- [ ] Integration with graph traversal (find similar nodes, then traverse)
- [ ] Efficient vector indexing strategies

**Cypher Syntax**:
```cypher
// Find similar documents using cosine similarity
MATCH (d:Document)
WHERE d.embedding IS NOT NULL
WITH d, vectorSimilarity.cosine(d.embedding, $queryEmbedding) AS score
WHERE score > 0.8
ORDER BY score DESC
LIMIT 10
RETURN d.title, score

// Combined graph + vector search
MATCH (u:User)-[:INTERESTED_IN]->(topic:Topic)
WHERE vectorSimilarity.cosine(topic.embedding, $userPreferences) > 0.7
RETURN u.name, topic.name, collect(topic) AS relevant_topics
```

**ClickHouse Integration**:
- [ ] Use ClickHouse Array(Float32) for vector storage
- [ ] Leverage ClickHouse vector functions:
  - `cosineDistance()` - Cosine similarity
  - `L2Distance()` - Euclidean distance  
  - `dotProduct()` - Dot product
- [ ] Optimize with ClickHouse vector indices (if available)
- [ ] Consider ANN (Approximate Nearest Neighbor) for large-scale

**Use Cases**:
- **Semantic Search**: Find documents/products by meaning, not keywords
- **Recommendation Systems**: Similar users, similar items
- **Fraud Detection**: Find anomalous patterns in embeddings
- **Knowledge Graphs**: Entity disambiguation, similarity-based linking
- **RAG (Retrieval-Augmented Generation)**: Retrieve relevant context for LLMs

**Performance Considerations**:
- Vector dimensionality (768, 1536, 4096, etc.)
- Index strategies for billion-scale vectors
- Query performance vs. accuracy tradeoffs (exact vs. approximate)

---

#### 8. **GraphRAG Support** 🧠
**Status**: Design phase  
**Estimated Effort**: TBD (depends on scope)  
**Impact**: High - Emerging AI use case  
**Priority**: 🌟 Medium-High

**GraphRAG Context**:
GraphRAG (Graph Retrieval-Augmented Generation) combines knowledge graphs with LLM retrieval for more accurate, context-aware AI responses.

**Potential Requirements** (To Be Defined):

**Graph-Based Retrieval**:
- [ ] Traverse graph to gather context for LLM prompts
- [ ] Multi-hop reasoning: "Find experts on topic X who worked with person Y"
- [ ] Subgraph extraction for context windows
- [ ] Path-based evidence gathering

**Integration Points**:
- [ ] Vector similarity for initial retrieval (see #7)
- [ ] Graph traversal to expand context
- [ ] Relationship-aware context ranking
- [ ] Citation/provenance tracking (which nodes contributed to answer)

**Example Workflow**:
```cypher
// 1. Vector search for relevant entities
MATCH (doc:Document)
WHERE vectorSimilarity.cosine(doc.embedding, $query) > 0.7
WITH doc LIMIT 20

// 2. Expand to related entities
MATCH (doc)-[:REFERENCES]->(entity:Entity)-[:RELATED_TO*1..2]-(related)
RETURN doc.content, entity.name, collect(related.name) AS context

// 3. Rank by relevance and relationships
ORDER BY doc.relevance_score DESC, count(related) DESC
```

**Use Cases**:
- **Enterprise Knowledge Base**: Answer questions using internal docs + org chart
- **Scientific Research**: Find connections between papers, authors, concepts
- **Legal Discovery**: Connect cases, statutes, precedents
- **Customer Support**: Historical tickets + product relationships + user context

**Open Questions** (to prioritize):
- What specific GraphRAG patterns are most needed?
- Integration with LangChain / LlamaIndex?
- Custom functions like `buildContext()` or `findEvidence()`?
- Streaming results for large context windows?

---

#### 9. **Neo4j-Style Parameter Support & Query Cache** ⚡
**Status**: Not started  
**Estimated Effort**: 2-3 weeks  
**Impact**: High - Performance and usability  
**Priority**: 🔥 High

**Parameter Support**:

**Cypher Parameters** (Neo4j-compatible):
```cypher
// Parameters prevent SQL injection and enable query caching
MATCH (u:User)
WHERE u.email = $email AND u.age > $minAge
RETURN u.name, u.email
```

**Requirements**:
- [ ] Parse `$paramName` syntax in Cypher queries
- [ ] Accept parameters via HTTP API: `{"query": "...", "parameters": {"email": "alice@example.com", "minAge": 25}}`
- [ ] Accept parameters via Bolt protocol
- [ ] Type coercion and validation
- [ ] Support nested parameters: `$user.email`, `$filters.dateRange`
- [ ] Array parameters: `WHERE u.id IN $userIds`

**Query Cache**:

**Requirements**:
- [ ] **Query Plan Cache**: Cache parsed AST and logical plans
  - Key: Normalized Cypher query (parameters replaced with placeholders)
  - Value: Parsed AST + logical plan
  - Eviction: LRU with configurable size limit
  
- [ ] **Prepared Statements**: Cache parameterized SQL queries
  - Key: Cypher query signature
  - Value: SQL template with parameter slots
  - Benefit: Skip Cypher→SQL translation on repeated queries

- [ ] **Result Cache** (optional): Cache query results
  - Key: Cypher query + parameters + schema version
  - Value: Result set
  - TTL: Configurable expiration
  - Invalidation: On schema changes

**Cache Configuration**:
- [ ] Cache size limits (memory-based)
- [ ] TTL (time-to-live) settings
- [ ] Cache hit/miss metrics
- [ ] Cache warming strategies
- [ ] Per-tenant cache isolation (for multi-tenant)

**Performance Benefits**:
- **Query Plan Cache**: 10-100x faster for repeated queries
- **Prepared Statements**: Reduce SQL generation overhead
- **Result Cache**: Instant responses for common queries
- **Reduced Load**: Lower CPU usage on ClickHouse

**Monitoring**:
- [ ] Cache hit rate metrics
- [ ] Query execution time breakdown (parse, plan, execute)
- [ ] Most frequently cached queries
- [ ] Memory usage per cache type

**Example HTTP API**:
```json
POST /query
{
  "query": "MATCH (u:User) WHERE u.email = $email RETURN u",
  "parameters": {
    "email": "alice@example.com"
  },
  "options": {
    "useCache": true,
    "cacheTTL": 300
  }
}
```

**Security Considerations**:
- Parameters prevent SQL injection
- Validate parameter types before execution
- Sanitize parameter values in logs

---

#### 10. **SQL Query Inspection & Modification Callback** 🔍
**Status**: Not started  
**Estimated Effort**: 1-2 weeks  
**Impact**: Medium-High - Power user flexibility  
**Priority**: 🌟 Medium

**Requirements**:
- [ ] Hook/callback mechanism before SQL execution
- [ ] Pass original Cypher query + generated SQL to callback
- [ ] Allow user code to inspect and modify SQL
- [ ] Return modified SQL for execution
- [ ] Support multiple callback registration (chain of hooks)
- [ ] Optional: Allow rejection/abort of queries

**Use Cases**:

**Query Optimization**:
- Add custom query hints not supported by Cypher
- Inject ClickHouse-specific optimizations
- Force specific JOIN orders or algorithms

**Compliance & Auditing**:
- Log generated SQL for compliance review
- Add audit columns to queries
- Inject additional WHERE clauses for data filtering

**Multi-Tenant Customization**:
- Add tenant-specific filtering at SQL level
- Inject tenant context into queries
- Apply per-tenant query transformations

**Debugging & Development**:
- Inspect SQL during development
- Add EXPLAIN PLAN prefixes
- Test query variations

**Implementation Approaches**:

**Option 1: HTTP Webhook**:
```json
POST /query
{
  "query": "MATCH (u:User) RETURN u.name",
  "parameters": {},
  "hooks": {
    "beforeExecution": "https://myapp.com/sql-hook"
  }
}

// ClickGraph calls webhook:
POST https://myapp.com/sql-hook
{
  "cypher": "MATCH (u:User) RETURN u.name",
  "sql": "SELECT name FROM users",
  "metadata": {
    "schema": "default",
    "user": "alice"
  }
}

// Webhook responds with modified SQL:
{
  "sql": "SELECT name FROM users WHERE tenant_id = 123",
  "allow": true
}
```

**Option 2: Server-Side Plugins**:
```rust
// Rust plugin interface
trait SqlTransformPlugin {
    fn transform(&self, ctx: QueryContext, sql: String) -> Result<String>;
}

// User implements plugin
struct MyCustomPlugin;
impl SqlTransformPlugin for MyCustomPlugin {
    fn transform(&self, ctx: QueryContext, sql: String) -> Result<String> {
        // Inspect and modify SQL
        Ok(format!("{} SETTINGS max_threads = 4", sql))
    }
}
```

**Option 3: Embedded Scripts** (Lua/WASM):
```lua
-- Lua callback function
function on_sql_generation(cypher, sql, context)
    -- Add custom WHERE clause
    if context.user_role ~= "admin" then
        sql = sql:gsub("FROM users", "FROM users WHERE department = '" .. context.department .. "'")
    end
    return sql
end
```

**Configuration**:
```yaml
# Schema configuration with SQL hooks
schemas:
  default:
    sql_hooks:
      before_execution:
        type: webhook
        url: https://myapp.com/sql-hook
        timeout: 1000ms
        retry: false
      
      # Or plugin-based
      before_execution:
        type: plugin
        class: com.myapp.SqlTransformer
        config:
          max_threads: 8
```

**Safety & Validation**:
- [ ] Syntax validation of modified SQL
- [ ] Timeout for webhooks/callbacks (prevent hang)
- [ ] Error handling: fallback to original SQL or abort?
- [ ] Security: Prevent SQL injection in modifications
- [ ] Rate limiting for webhook calls
- [ ] Caching: Should modified queries be cached differently?

**Monitoring**:
- [ ] Track callback execution time
- [ ] Log SQL modifications for audit
- [ ] Metrics: modification frequency, types of changes
- [ ] Alert on callback failures

**Example Use Case - Multi-Tenant Filtering**:
```
Original Cypher: MATCH (u:User) WHERE u.age > 25 RETURN u.name
Generated SQL:   SELECT name FROM users WHERE age > 25

Callback modifies to:
SELECT name FROM users WHERE age > 25 AND tenant_id = 123

Executed SQL:    SELECT name FROM users WHERE age > 25 AND tenant_id = 123
```

**Trade-offs**:
- **Pros**: Maximum flexibility, custom optimizations, compliance controls
- **Cons**: Complexity, potential performance overhead, security risks
- **Recommendation**: Start with webhook approach for simplicity, add plugins if needed

---

## 🚀 Planned Features

### High Priority (Next 1-2 Months)

#### 1. **Additional Graph Algorithms** 📊
**Status**: Not started  
**Estimated Effort**: 1-2 weeks per algorithm  
**Impact**: High - Expands graph analytics capabilities

**Algorithms to Implement**:
- [ ] **Betweenness Centrality**: Measure node importance based on shortest paths
  - Cypher: `CALL gds.betweenness.stream(...)` 
  - Use case: Find influential nodes in network
  
- [ ] **Closeness Centrality**: Measure average distance to all other nodes
  - Cypher: `CALL gds.closeness.stream(...)`
  - Use case: Identify central/accessible nodes
  
- [ ] **Degree Centrality**: Count incoming/outgoing relationships
  - Cypher: `CALL gds.degree.stream(...)`
  - Use case: Basic node importance metric
  
- [ ] **Community Detection (Louvain)**: Find clusters/communities in graph
  - Cypher: `CALL gds.louvain.stream(...)`
  - Use case: Social network analysis, recommendation systems
  
- [ ] **Connected Components**: Find disconnected subgraphs
  - Cypher: `CALL gds.wcc.stream(...)` (Weakly Connected Components)
  - Use case: Network topology analysis

**Implementation Notes**:
- Follow PageRank implementation pattern in `query_planner/logical_plan/call.rs`
- Use recursive CTEs or ClickHouse-specific array functions where appropriate
- Each algorithm needs comprehensive testing with various graph sizes

---

#### 2. **Path Comprehensions** 🛤️
**Status**: Not started  
**Estimated Effort**: 3-5 days  
**Impact**: Medium - Enables more expressive path queries

**Feature**:
```cypher
// Collect specific properties along a path
MATCH p = (a:User)-[:FOLLOWS*1..3]->(b:User)
RETURN [(node IN nodes(p)) | node.name] AS user_names

// Filter and transform relationships
MATCH p = (a)-[rels:FOLLOWS*]-(b)
RETURN [rel IN rels WHERE rel.since > 2020 | rel.weight] AS recent_weights
```

**Implementation Areas**:
- [ ] Extend parser in `open_cypher_parser/expression.rs` for list comprehension syntax
- [ ] Add `ListComprehension` variant to AST
- [ ] Implement SQL generation with array functions (`arrayMap`, `arrayFilter`)
- [ ] Handle nested path expressions

---

#### 3. **CASE Expression Enhancements** 🔀
**Status**: Basic support exists  
**Estimated Effort**: 2-3 days  
**Impact**: Medium - Improve conditional logic support

**Current State**: Simple and searched CASE work in some contexts  
**Gaps**:
- [ ] CASE in WHERE clauses with complex conditions
- [ ] Nested CASE expressions
- [ ] CASE with graph path expressions
- [ ] NULL handling in CASE branches

**Test Coverage**: Need comprehensive test suite for all CASE contexts

---

### Medium Priority (2-4 Months)

#### 4. **UNWIND Support** 🔄
**Status**: Not started  
**Estimated Effort**: 1 week  
**Impact**: Medium - Enables list expansion queries

**Feature**:
```cypher
// Expand array values into rows
UNWIND [1, 2, 3] AS x
RETURN x

// Expand path nodes
MATCH p = (a)-[:FOLLOWS*]-(b)
UNWIND nodes(p) AS node
RETURN node.name
```

**Implementation**:
- [ ] Add UNWIND clause to parser
- [ ] Generate ClickHouse array unnesting SQL (`arrayJoin()`)
- [ ] Handle UNWIND in query plan building

---

#### 5. **Map Projections** 🗺️
**Status**: Not started  
**Estimated Effort**: 3-5 days  
**Impact**: Low-Medium - Convenience feature

**Feature**:
```cypher
// Project subset of node properties
MATCH (u:User)
RETURN u{.name, .email, age: u.age * 2} AS user_data

// Nested projections
MATCH (u:User)-[:FOLLOWS]->(f:User)
RETURN u{.name, follower: f{.name, .email}} AS result
```

---

#### 6. **Aggregation Enhancements** 📈
**Status**: Partial support  
**Estimated Effort**: 1-2 weeks  
**Impact**: Medium - Better analytics capabilities

**Improvements Needed**:
- [ ] `collect()` function - aggregate values into arrays
- [ ] `percentileCont()`, `percentileDisc()` - statistical functions
- [ ] `stDev()`, `stDevP()` - standard deviation
- [ ] Aggregation with DISTINCT
- [ ] Multiple aggregations in single RETURN clause

---

### Low Priority (Future)

#### 7. **Query Hints and Optimization Directives** ⚡
**Status**: Not started  
**Estimated Effort**: 2-3 weeks  
**Impact**: Low - Performance tuning

**Feature**:
```cypher
// Index hints
MATCH (u:User)
USING INDEX u:User(email)
WHERE u.email = 'alice@example.com'
RETURN u

// Join hints
MATCH (a)-[r]->(b)
USING JOIN ON r
RETURN a, b
```

---

#### 8. **EXISTS Subqueries** 🔍
**Status**: Not started  
**Estimated Effort**: 1-2 weeks  
**Impact**: Medium - Advanced filtering

**Feature**:
```cypher
// Filter based on pattern existence
MATCH (u:User)
WHERE EXISTS {
  MATCH (u)-[:FOLLOWS]->(other:User)
  WHERE other.age > 30
}
RETURN u.name
```

---

## 🏋️ Benchmark Tasks

### Performance Testing Infrastructure

#### 1. **Large-Scale Benchmark Suite** 📊
**Status**: Partial (3 benchmark tests exist but need datasets)  
**Priority**: High  
**Estimated Effort**: 1 week

**Objectives**:
- [ ] Create reproducible benchmark datasets
  - Small: 1K nodes, 5K edges
  - Medium: 10K nodes, 50K edges
  - Large: 5M nodes, 50M edges
  - Extra Large: 100M nodes, 100M edges
  - **Ultra Large: 1B+ edges** (Real-world scale testing)

- [ ] Benchmark query patterns:
  - [ ] Direct relationship lookups (1-hop)
  - [ ] Multi-hop traversals (2-5 hops)
  - [ ] Variable-length paths with filters
  - [ ] Shortest path computations
  - [ ] PageRank on various graph sizes
  - [ ] Aggregations with GROUP BY
  - [ ] Complex OPTIONAL MATCH patterns

- [ ] Performance metrics to track:
  - [ ] Query execution time (p50, p95, p99)
  - [ ] Memory usage
  - [ ] ClickHouse CPU utilization
  - [ ] Result set size impact

**Current Blockers**:
- `test_benchmark_final.py` - Needs large dataset setup
- `test_medium_benchmark.py` - Requires 10K user dataset
- Need automated dataset generation scripts

---

#### 2. **Comparison Benchmarks** ⚖️
**Status**: Not started  
**Priority**: Medium  
**Estimated Effort**: 2-3 weeks

**Objectives**:
- [ ] Compare ClickGraph vs Neo4j (same queries)
- [ ] Compare ClickGraph vs native ClickHouse SQL
- [ ] Measure overhead of Cypher→SQL translation
- [ ] Identify optimization opportunities

**Metrics**:
- Query latency comparison
- Throughput (queries per second)
- Resource utilization
- Scalability curves

---

#### 3. **Optimization Targets** 🎯
**Status**: Ongoing  
**Priority**: Medium

**Areas for Optimization**:
- [ ] **CTE Generation**: Reduce redundant subqueries
- [ ] **JOIN Ordering**: Optimize JOIN sequence based on cardinality
- [ ] **Filter Pushdown**: Move WHERE clauses closer to data source
- [ ] **Index Utilization**: Better leverage ClickHouse indexes
- [ ] **Parallel Execution**: Explore parallel query execution strategies

---

## 🧪 Testing Improvements

### Test Coverage Expansion

#### 1. **Edge Case Testing** 🔬
**Priority**: Medium  
**Estimated Effort**: 1 week

**Missing Coverage**:
- [ ] Zero-length paths (`*0`)
- [ ] Negative hop ranges (should error gracefully)
- [ ] Circular path detection
- [ ] Very deep traversals (>10 hops)
- [ ] Empty graph scenarios
- [ ] Single-node graphs
- [ ] Disconnected graph components

---

#### 2. **Property Filtering Tests** 🔍
**Priority**: Medium  
**Estimated Effort**: 3-5 days

**Scenarios**:
- [ ] Complex WHERE clauses with AND/OR/NOT
- [ ] Property comparisons across nodes
- [ ] NULL property handling
- [ ] Array property operations
- [ ] String pattern matching (CONTAINS, STARTS WITH, ENDS WITH)
- [ ] Numeric range queries

---

#### 3. **Multi-Pattern Queries** 🔗
**Priority**: Low-Medium  
**Estimated Effort**: 1 week

**Scenarios**:
- [ ] Multiple variable-length patterns in single query
- [ ] Combining OPTIONAL MATCH with variable-length paths
- [ ] Multiple shortest path computations
- [ ] Complex graph pattern combinations

---

## 📚 Documentation Needs

### 🎯 v0.5.0 Milestone: Comprehensive Wiki Content

**Target**: By v0.5.0, provide production-ready documentation in GitHub Wiki  
**Status**: Not started  
**Estimated Effort**: 3-4 weeks  
**Priority**: 🔥 High - Critical for adoption

**Wiki Structure**:

**1. Getting Started** (1 week)
- [ ] **Quick Start Guide**: 5-minute setup to first query
- [ ] **Installation**: Docker, native binary, Kubernetes
- [ ] **Configuration**: Environment variables, config files, CLI options
- [ ] **First Graph**: Tutorial creating simple social network
- [ ] **Neo4j Migration**: Step-by-step migration from Neo4j

**2. User Documentation** (1.5 weeks)
- [ ] **Cypher Reference**: Complete syntax guide
  - Pattern matching: MATCH, OPTIONAL MATCH
  - Variable-length paths: `*`, `*1..3`, shortest path
  - Functions: All supported Neo4j functions
  - Expressions: CASE, list comprehensions, etc.
  - Aggregations: GROUP BY, ORDER BY, LIMIT
- [ ] **Schema Design Guide**: 
  - YAML configuration best practices
  - Mapping ClickHouse tables to graph entities
  - Multi-schema setups
  - ReplacingMergeTree considerations
- [ ] **Query Optimization**:
  - Performance tips and anti-patterns
  - Understanding generated SQL
  - Index strategies
  - Recursive CTE depth tuning
- [ ] **Multi-Tenant Guide**: Strategies and configurations
- [ ] **Vector Search Guide**: Embeddings and similarity queries
- [ ] **Security**: RBAC, RLS, authentication

**3. API Documentation** (3-4 days)
- [ ] **HTTP API Reference**: Complete endpoint documentation
  - `/query` - Execute Cypher queries
  - `/health` - Health check
  - `/schema` - Schema management
  - `/metrics` - Query performance metrics
- [ ] **Bolt Protocol Guide**: Connecting with Neo4j drivers
- [ ] **Parameter Support**: Neo4j-style parameters
- [ ] **Error Codes**: Complete error reference
- [ ] **Response Formats**: JSON structure documentation

**4. Deployment & Operations** (3-4 days)
- [ ] **Production Deployment**: Best practices
- [ ] **Docker Compose**: Multi-container setup
- [ ] **Kubernetes**: Helm charts and manifests
- [ ] **Monitoring**: Metrics, logging, alerting
- [ ] **Backup & Recovery**: ClickHouse data management
- [ ] **Scaling**: Horizontal scaling strategies
- [ ] **Performance Tuning**: ClickHouse optimization for graphs

**5. Developer Documentation** (1 week)
- [ ] **Architecture Deep Dive**: Component overview
  - Parser → Planner → Optimizer → SQL Generator flow
  - Query execution lifecycle
  - Schema management internals
- [ ] **Contributing Guide**: 
  - Development setup
  - Code organization
  - Testing requirements
  - PR process
- [ ] **Adding Features**: Step-by-step guides
  - New Cypher syntax
  - New functions
  - New algorithms
  - Optimizer passes
- [ ] **Debugging Guide**: 
  - Enabling debug output
  - Reading SQL queries
  - Common pitfalls
  - Performance profiling

**6. Use Case Examples** (3-4 days)
- [ ] **Social Network Analysis**: User relationships, communities
- [ ] **Recommendation Systems**: Collaborative filtering with graphs
- [ ] **Fraud Detection**: Pattern-based anomaly detection
- [ ] **Knowledge Graphs**: Entity relationships, RAG integration
- [ ] **Supply Chain**: Multi-hop logistics analysis
- [ ] **Cybersecurity**: Network traffic analysis
- [ ] **E-commerce**: Product recommendations, customer journeys

**7. Benchmark Results** (2-3 days)
- [ ] **Performance Benchmarks**: Published results
  - Small/Medium/Large/Ultra datasets
  - Query latency distributions
  - Comparison with Neo4j
  - Scaling characteristics
- [ ] **Reproducible Benchmarks**: Scripts and datasets
- [ ] **Optimization Case Studies**: Real-world improvements

**Wiki Maintenance**:
- [ ] Automated wiki generation from markdown docs
- [ ] Version-specific documentation (v0.3, v0.4, v0.5)
- [ ] Search functionality
- [ ] Code examples with syntax highlighting
- [ ] Interactive query playground (future)

---

### Additional Documentation Tasks

### User-Facing Documentation

- [ ] **Query Guide**: Comprehensive Cypher syntax reference (→ Wiki)
- [ ] **Performance Tuning**: Best practices for query optimization (→ Wiki)
- [ ] **Schema Design**: Guidelines for YAML schema configuration (→ Wiki)
- [ ] **Migration Guide**: From Neo4j to ClickGraph (→ Wiki)
- [ ] **API Reference**: Complete HTTP and Bolt protocol documentation (→ Wiki)

### Developer Documentation

- [ ] **Architecture Overview**: System design deep dive
- [ ] **Contributing Guide**: How to add new features
- [ ] **Testing Guide**: Expansion of current testing documentation
- [ ] **Debugging Guide**: Troubleshooting query issues

---

## 🚫 Out of Scope

**Explicitly NOT on Roadmap** (Read-Only Engine):
- ❌ Write operations: `CREATE`, `SET`, `DELETE`, `MERGE`
- ❌ Schema modifications: `CREATE INDEX`, `CREATE CONSTRAINT`
- ❌ Transaction management: `BEGIN`, `COMMIT`, `ROLLBACK`
- ❌ Data mutations of any kind
- ❌ User/role management

---

## 🤝 Contributing

Want to help with any of these roadmap items? See `CONTRIBUTING.md` for guidelines on submitting pull requests.

**Priority Labels**:
- 🔥 **High**: Next 1-2 months, significant impact
- 🌟 **Medium**: 2-4 months, moderate impact  
- 💡 **Low**: Future consideration, nice-to-have

---

## 📊 Progress Tracking

| Feature | Status | Priority | ETA |
|---------|--------|----------|-----|
| **Phase 1 (v0.4.0)** | ✅ **COMPLETE** | 🔥 High | ✅ Nov 2025 |
| ├─ Parameter Support | ✅ Complete | 🔥 High | ✅ Nov 10, 2025 |
| ├─ Query Cache | ✅ Complete | 🔥 High | ✅ Nov 10, 2025 |
| ├─ Bolt Protocol | ✅ Complete | 🔥 High | ✅ Nov 12, 2025 |
| ├─ Neo4j Functions (25+) | ✅ Complete | 🔥 High | ✅ Nov 12, 2025 |
| └─ Benchmark Suite (1-10K) | ✅ Complete | 🔥 High | ✅ Nov 13, 2025 |
| **Phase 2 (v0.5.0)** | Not Started | 🔥 High | Q1 2026 |
| Graph Algorithms | Not Started | 🔥 High | Q1 2026 |
| Path Comprehensions | Not Started | 🔥 High | Q1 2026 |
| UNWIND Support | Not Started | 🌟 Medium | Q1-Q2 2026 |
| Map Projections | Not Started | 🌟 Medium | Q2 2026 |
| EXISTS Subqueries | Not Started | 💡 Low | Future |
| Query Hints | Not Started | 💡 Low | Future |

---

**Questions or Suggestions?** Open an issue on GitHub to discuss roadmap priorities!


