# Industry Benchmark Integration Plan for ClickGraph

## Overview

This document outlines the plan to integrate industry-standard graph database benchmarks into ClickGraph's testing and performance evaluation suite.

## Target Benchmarks

### 1. LDBC Social Network Benchmark (SNB)
**Source**: [LDBC Council](https://ldbcouncil.org/benchmarks/snb/)  
**Status**: Gold standard for graph database benchmarking, auditable
**License**: Apache 2.0

#### Why LDBC SNB?
- ✅ **Industry standard** - Used by Neo4j, TigerGraph, PostgreSQL, DuckDB, GraphDB
- ✅ **Auditable** - Official LDBC auditing process for credible performance claims
- ✅ **Comprehensive** - 22 complex queries + 7 update operations
- ✅ **Real-world schema** - Social network domain (similar to our current benchmark)
- ✅ **Multiple scale factors** - SF0.1 to SF1000+ available
- ✅ **Reference implementations** - Neo4j (Cypher), PostgreSQL (SQL) for validation

### 2. GraphBenchmark.com Microbenchmarks
**Source**: [GraphBenchmark.com](https://graphbenchmark.com/)  
**Status**: Academic research project (University of Trento)
**Paper**: "Beyond Macrobenchmarks: Microbenchmark-based Graph Database Evaluation" (VLDB 2018)

### 3. OnTime Flight Data (Backlog) ✈️
**Source**: [ClickHouse OnTime Dataset](https://clickhouse.com/docs/en/getting-started/example-datasets/ontime)  
**Status**: Backlog - Post-Phase 3 consideration  
**Effort**: 3-5 days (schema + queries only)

**Why Backlog?**
- ✅ **Zero ETL** - ClickHouse provides complete schema and 1-line data import
- ✅ **Real-world scale** - 200M+ rows (1987-present flight data)
- ✅ **Different domain** - Logistics/transportation vs social networks
- ✅ **Community-friendly** - Simple to implement (just needs YAML schema)
- ⚠️ Lower priority than industry-standard LDBC SNB
- ⚠️ No reference Cypher queries (need to create our own)

**What's Already Available**:
```sql
-- Table schema: 110+ columns provided by ClickHouse
-- Data loading: Single S3 import command
INSERT INTO ontime SELECT * FROM s3('https://clickhouse-public-datasets.s3.amazonaws.com/ontime/csv_by_year/*.csv.gz', CSVWithNames);
```

**What We Need to Build**:
- Graph schema YAML (map airports, carriers, routes)
- 10+ graph analytics queries (multi-hop routes, delay propagation, hub analysis)
- Performance benchmarks vs Neo4j (if comparable graph DB implementations exist)

---
- **22 complex read queries** (IC1-IC14: Complex reads, IS1-IS7: Short reads)
- **7 update operations** (❌ Out of scope - ClickGraph is read-only)
- **Focus**: Transactional graph processing, neighborhood traversals
- **Characteristics**: Deep graph traversals, pattern matching, aggregations

**Business Intelligence Workload** (Secondary target):
- **20 analytical queries** (aggregation-heavy, large graph scans)
- **Microbatches of updates** (❌ Out of scope)
- **Focus**: Graph analytics, OLAP-style queries
- **Characteristics**: Complex joins, aggregations, multi-hop analysis

### 2. GraphBenchmark.com Microbenchmarks
**Source**: [GraphBenchmark.com](https://graphbenchmark.com/)  
**Status**: Academic research project (University of Trento)
**Paper**: "Beyond Macrobenchmarks: Microbenchmark-based Graph Database Evaluation" (VLDB 2018)

#### Why GraphBenchmark?
- ✅ **Primitive operations** - 35+ classes of fundamental graph operations
- ✅ **Diagnostic value** - Pinpoints specific performance bottlenecks
- ✅ **Multiple datasets** - Yeast, MiCo, Freebase (various sizes), LDBC
- ✅ **Open source** - Docker-based test suite available
- ✅ **Complements LDBC** - Micro-level insights vs macro-level benchmarks

#### Operation Categories
- **Traversal**: BFS, DFS, neighborhood expansion
- **Pattern matching**: Triangle counting, k-hop patterns
- **Aggregation**: Count, sum, group by
- **Join operations**: Node-edge joins, multi-hop joins
- **Filtering**: Property filters, structural filters

## Implementation Plan

### Phase 1: LDBC SNB Read-Only Queries (Priority)
**Timeline**: 2-3 weeks  
**Scope**: Implement read-only subset of LDBC SNB Interactive workload

#### Tasks:
1. **Data Loading** (3 days)
   - [ ] Download LDBC SNB SF0.1 (validation size)
   - [ ] Create ClickHouse schema for SNB nodes (Person, Post, Comment, Forum, etc.)
   - [ ] Create ClickHouse schema for SNB edges (KNOWS, LIKES, HAS_CREATOR, etc.)
   - [ ] Write bulk loader: CSV → ClickHouse
   - [ ] Add edge_id columns for optimal performance

2. **Schema Configuration** (2 days)
   - [ ] Create `benchmarks/schemas/ldbc_snb.yaml`
   - [ ] Map SNB schema to ClickGraph YAML format
   - [ ] Add all node types (Person, Post, Comment, Forum, Tag, Place, Organization, TagClass)
   - [ ] Add all relationship types (KNOWS, LIKES, HAS_CREATOR, etc.)

3. **Query Implementation** (5 days)
   - [ ] Implement IC1-IC14 (14 complex interactive read queries)
   - [ ] Skip IS1-IS7 for now (short reads - lower priority)
   - [ ] Create Cypher versions of SQL reference queries
   - [ ] Document query conversion process

4. **Validation** (2 days)
   - [ ] Compare results against Neo4j reference implementation
   - [ ] Use LDBC validation parameters (SF0.1 validation file)
   - [ ] Document discrepancies and correctness

5. **Performance Benchmarking** (3 days)
   - [ ] Run queries on SF0.1, SF1, SF3, SF10
   - [ ] Measure: Execution time, memory usage, query plans
   - [ ] Compare against Neo4j, PostgreSQL reference implementations
   - [ ] Generate performance report

### Phase 2: GraphBenchmark Microbenchmarks (Secondary)
**Timeline**: 1-2 weeks  
**Scope**: Implement primitive operation benchmarks

#### Tasks:
1. **Dataset Preparation** (2 days)
   - [ ] Download Yeast, MiCo, Freebase-Small datasets
   - [ ] Convert GraphSON → ClickHouse tables
   - [ ] Create schema YAML files

2. **Primitive Operation Implementation** (4 days)
   - [ ] **Traversal**: BFS, DFS, k-hop neighborhood
   - [ ] **Pattern Matching**: Triangle enumeration, 4-cliques
   - [ ] **Aggregation**: Degree distribution, centrality measures
   - [ ] **Join**: Multi-hop path enumeration

3. **Benchmark Execution** (2 days)
   - [ ] Run microbenchmark suite
   - [ ] Compare against published results (Neo4j, OrientDB, Titan, etc.)
   - [ ] Identify performance bottlenecks

### Phase 3: Continuous Integration (Ongoing)
**Timeline**: 1 week setup + ongoing  
**Scope**: Automate benchmark execution

#### Tasks:
1. **CI Pipeline** (3 days)
   - [ ] Add benchmark runs to GitHub Actions
   - [ ] Run subset of LDBC queries on each PR
   - [ ] Alert on performance regressions

2. **Performance Tracking** (2 days)
   - [ ] Store benchmark results in database
   - [ ] Create performance dashboard
   - [ ] Track metrics over time

3. **Documentation** (2 days)
   - [ ] Document benchmark setup
   - [ ] Publish performance results
   - [ ] Create reproducibility guide

## Benchmark Data Locations

### LDBC SNB Data
- **Pre-generated datasets**: [SURF/CWI Repository](https://repository.surfsara.nl/datasets/cwi/ldbc-snb-interactive-v1-datagen-v100)
- **Direct links**: [LDBC Data Sets](https://ldbcouncil.org/data-sets-surf-repository/snb-interactive-v1-datagen-v100)
- **Validation parameters**: [SF0.1 to SF10](https://datasets.ldbcouncil.org/interactive-v1/validation_params-interactive-v1.0.0-sf0.1-to-sf10.tar.zst)
- **Scale factors**: SF0.1 (test), SF1, SF3, SF10, SF30, SF100, SF300, SF1000

### GraphBenchmark Data
- **Official dump**: [Zenodo](https://doi.org/10.5281/zenodo.15571202)
- **Google Drive**: [Dataset Collection](https://drive.google.com/drive/folders/0BwX66B9ISrt4UXZrXzhIRGV2V3M)
- **Datasets**: Yeast (2.3K nodes), MiCo (0.1M nodes), Freebase (1.9M to 28.4M nodes), LDBC (0.18M nodes)

## Reference Implementations

### LDBC SNB Cypher Queries
**Repository**: [ldbc/ldbc_snb_interactive_v1_impls](https://github.com/ldbc/ldbc_snb_interactive_v1_impls)

**Key directories**:
- `cypher/queries/` - Neo4j Cypher implementations
- `postgres/queries/` - PostgreSQL SQL implementations (useful for SQL comparison)
- `cypher/test-data/` - Small test dataset

**How to use**:
1. Study Cypher queries as reference
2. Adapt to ClickGraph's syntax (should be minimal changes)
3. Use validation parameters to verify correctness
4. Compare performance against Neo4j baseline

### GraphBenchmark Test Suite
**Repository**: [kuzeko/graph-databases-testsuite](https://github.com/kuzeko/graph-databases-testsuite)

**Key features**:
- Docker-based setup
- 35+ operation classes
- 70+ different tests
- Automated result collection

## Success Metrics

### Phase 1 (LDBC SNB)
- ✅ All 14 complex read queries (IC1-IC14) implemented
- ✅ Results match Neo4j reference implementation within 1% tolerance
- ✅ Performance measured on SF1, SF3, SF10
- ✅ Performance report published comparing to Neo4j, PostgreSQL

### Phase 2 (GraphBenchmark)
- ✅ 10+ microbenchmark operations implemented
- ✅ Results comparable to published benchmarks
- ✅ Performance bottlenecks identified and documented

### Phase 3 (CI)
- ✅ Automated benchmark runs on each PR
- ✅ Performance tracking dashboard live
- ✅ No more than 10% performance regression between releases

## Expected Outcomes

### Technical Benefits
1. **Credible performance claims** - Industry-standard benchmarks
2. **Competitive analysis** - Direct comparison with Neo4j, TigerGraph, etc.
3. **Performance insights** - Identify optimization opportunities
4. **Validation** - Verify correctness against reference implementations

### Marketing Benefits
1. **Auditable results** - Potential for official LDBC audit (requires LDBC membership)
2. **Comparable metrics** - Same benchmarks as competitors
3. **Published results** - Performance reports on website
4. **Academic credibility** - Research paper potential

### Development Benefits
1. **Regression detection** - Catch performance degradations early
2. **Optimization guidance** - Know which queries to optimize
3. **Feature validation** - Ensure new features don't break benchmarks
4. **Scalability testing** - Test with large-scale datasets (SF10+)

## Cost-Benefit Analysis

### Investment Required
- **Development time**: ~4-6 weeks (across 3 phases)
- **Infrastructure**: Storage for benchmark datasets (~50GB for SF10)
- **Compute**: CI resources for automated benchmarks
- **Optional**: LDBC membership for auditing (€3,000 + auditor fees)

### Benefits
- **High credibility**: Industry-standard benchmarks
- **Competitive positioning**: Direct comparisons with Neo4j, etc.
- **Performance insights**: Data-driven optimization
- **Community adoption**: Researchers and enterprises trust LDBC

## Next Steps

### Immediate Actions (This Week)
1. ✅ Research LDBC SNB and GraphBenchmark (DONE)
2. [ ] Download LDBC SNB SF0.1 dataset
3. [ ] Study Neo4j Cypher reference queries
4. [ ] Create `benchmarks/schemas/ldbc_snb.yaml` skeleton

### Short-term (Next 2 Weeks)
1. [ ] Implement LDBC data loader
2. [ ] Implement first 3 LDBC queries (IC1, IC2, IC3)
3. [ ] Validate against reference results

### Medium-term (Next Month)
1. [ ] Complete all 14 LDBC complex read queries
2. [ ] Run benchmarks on SF1, SF3, SF10
3. [ ] Publish initial performance report

### Long-term (Next Quarter)
1. [ ] Implement GraphBenchmark microbenchmarks
2. [ ] Set up CI pipeline for automated benchmarking
3. [ ] Consider LDBC membership and auditing

## Resources

### Documentation
- [LDBC SNB Specification (PDF)](https://ldbcouncil.org/ldbc_snb_docs/ldbc-snb-specification.pdf)
- [LDBC SNB arXiv Paper](https://arxiv.org/pdf/2001.02299.pdf)
- [GraphBenchmark VLDB Paper](http://people.cs.aau.dk/~matteo//publications/journal/2018-vldb-gdb.html)
- [LDBC Auditing Process](https://ldbcouncil.org/docs/ldbc-snb-auditing-process.pdf)

### Repositories
- [LDBC SNB Reference Implementations](https://github.com/ldbc/ldbc_snb_interactive_v1_impls)
- [LDBC SNB Data Generator](https://github.com/ldbc/ldbc_snb_datagen_hadoop)
- [GraphBenchmark Test Suite](https://github.com/kuzeko/graph-databases-testsuite)

### Data
- [LDBC SNB Datasets](https://ldbcouncil.org/data-sets-surf-repository/snb-interactive-v1-datagen-v100)
- [GraphBenchmark Datasets (Zenodo)](https://doi.org/10.5281/zenodo.15571202)

---

*Created: November 22, 2025*  
*Status: Planning phase*  
*Owner: ClickGraph Development Team*
