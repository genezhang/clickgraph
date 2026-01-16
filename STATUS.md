# ClickGraph Status

*Updated: January 15, 2026*

## Current Version

**v0.6.1** - Production-ready graph query engine for ClickHouse

**LDBC SNB Benchmark Status**: 15/41 queries passing (37%)
- Interactive Short: 7/7 (100%) ✅
- Interactive Complex: 4/14 (29%) - IC-1, IC-2, IC-4, IC-6 working
- Business Intelligence: 4/20 (20%) - BI-5, BI-11, BI-12, BI-19 working

**Known Issues**: 3 active bugs (see [KNOWN_ISSUES.md](KNOWN_ISSUES.md))
- 🔥 **High Priority**: VLP CTE column scoping (blocks 7+ queries)

## What Works Now

### Core Query Capabilities ✅

**Basic Patterns**
```cypher
-- Node/relationship patterns
MATCH (n:User)-[:FOLLOWS]->(m:User) RETURN n, m

-- Multiple relationships
MATCH (a)-[:FOLLOWS|FRIENDS_WITH]->(b) RETURN a, b

-- Property filtering
MATCH (n:User) WHERE n.age > 25 AND n.country = 'USA' RETURN n

-- OPTIONAL MATCH (LEFT JOIN)
MATCH (n:User)
OPTIONAL MATCH (n)-[:FOLLOWS]->(m)
RETURN n, m
```

**Variable-Length Paths (VLP)**
```cypher
-- Any length
MATCH (a)-[*]->(b) RETURN a, b

-- Bounded ranges
MATCH (a)-[*1..3]->(b) RETURN a, b
MATCH (a)-[*..5]->(b) RETURN a, b
MATCH (a)-[*2..]->(b) RETURN a, b

-- With path variables
MATCH path = (a)-[*1..3]->(b)
RETURN path, length(path), nodes(path), relationships(path)

-- Shortest paths
MATCH path = shortestPath((a)-[*]->(b))
RETURN path

-- With relationship filters
MATCH (a)-[r:FOLLOWS*1..3 {status: 'active'}]->(b) RETURN a, b
```

**Aggregations & Functions**
```cypher
-- Standard aggregations
MATCH (n:User) RETURN COUNT(n), AVG(n.age), SUM(n.score)

-- Grouping
MATCH (u:User) RETURN u.country, COUNT(*) AS user_count

-- COLLECT
MATCH (u:User)-[:FOLLOWS]->(f)
RETURN u.name, COLLECT(f.name) AS friends

-- DISTINCT
MATCH (n)-[:FOLLOWS]->(m)
RETURN COUNT(DISTINCT m)
```

**Advanced Features**
```cypher
-- WITH clause
MATCH (n:User)
WITH n WHERE n.age > 25
MATCH (n)-[:FOLLOWS]->(m)
RETURN n, m

-- UNWIND
UNWIND [1, 2, 3] AS x
UNWIND [10, 20] AS y
RETURN x, y

-- Pattern comprehensions
MATCH (u:User)
RETURN u.name, [(u)-[:FOLLOWS]->(f) | f.name] AS friends

-- Multiple consecutive MATCH with per-MATCH WHERE
MATCH (m:Message) WHERE m.id = 123
MATCH (m)<-[:REPLY_OF]-(c:Comment)
RETURN m, c

-- Neo4j-compatible field aliases (expressions without AS)
MATCH (a:User)
RETURN a.name, substring(a.email, 0, 10), a.age * 2
-- Result fields: "a.name", "substring(a.email, 0, 10)", "a.age * 2"
```

**Multi-Schema Support**
```cypher
-- Select schema
USE ldbc_snb
MATCH (p:Person) RETURN p

-- Or via API parameter
{"query": "MATCH (n) RETURN n", "schema_name": "ldbc_snb"}
```

**Graph Algorithms**
```cypher
-- PageRank
CALL pagerank(
  node_label='User',
  relationship_type='FOLLOWS',
  max_iterations=20
) RETURN node_id, rank
```

### Internal Architecture ✅

**CTE Unification (Phase 3 Complete)**
- Unified recursive CTE generation across all schema patterns
- **TraditionalCteStrategy**: Standard node/edge table patterns
- **DenormalizedCteStrategy**: Single-table denormalized schemas  
- **FkEdgeCteStrategy**: Hierarchical FK relationships
- **MixedAccessCteStrategy**: Hybrid embedded/JOIN access patterns
- **EdgeToEdgeCteStrategy**: Multi-hop denormalized edge-to-edge patterns
- **CoupledCteStrategy**: Coupled edges in same physical row
- **Progress**: 14/14 TODOs completed (ID column resolution + 6 RenderExpr conversions + 7 parameter extractions)
- **Status**: Production-ready CTE unification using existing infrastructure

### Schema Support ✅

**All schema patterns supported**:
- Standard node/edge tables (typical many-to-many)
- FK-edge patterns (one-to-many/many-to-one/one-to-one)
- Denormalized edges (node properties in edge table)
- Coupled edges (multiple edge types in one table)
- Polymorphic edges (type discriminator column)
- Polymorphic labels (same label across multiple tables)
- Edge constraints (temporal, spatial, custom filters)

**Schema features**:
- Parameterized ClickHouse views as nodes/edges
- Column-level filters on tables
- Custom edge constraints spanning from_node and to_node
- Property mappings (Cypher property → ClickHouse column)

### Test Coverage ✅

**Integration Tests**: 3,538 tests collected (pytest framework with matrix parameterization)
- Core Cypher features: 549 base tests
- Variable-length paths: 24 base tests
- Pattern comprehensions: 5 base tests
- Property expressions: 28 base tests
- Security graphs: 94 base tests
- Matrix-expanded tests: ~2,000 additional test variations (same tests run against multiple schema patterns)

**LDBC SNB Benchmark**:
- Interactive Short (IS): 4/5 passing (IS-1, IS-2, IS-3, IS-5)
- Interactive Complex (IC): 3/4 tested passing (IC-2, IC-6, IC-12)
- Business Intelligence (BI): Testing in progress

### Parser Features ✅

**OpenCypher compliance**:
- Full Cypher grammar support (read operations only)
- Multiple comment styles: `--`, `/* */`, `//`
- Per-MATCH WHERE clauses (OpenCypher grammar compliant)
- Property expressions with nested access
- Pattern comprehensions

**Parameter support**:
- Named parameters: `$paramName`
- All common data types (string, int, float, bool, lists)

## Current Limitations

### Known Issues

**See [KNOWN_ISSUES.md](KNOWN_ISSUES.md) for detailed information.**

**Critical Issues**:
1. **Scalar aggregates in WITH + GROUP BY** - TableAlias architecture limitation
2. **CTE column naming inconsistency** - WITH DISTINCT uses underscores, WHERE uses dots
3. **OPTIONAL MATCH + inline property** - Generates invalid SQL (affects LDBC IS-7)

**Parser Limitations**:
- No write operations (`CREATE`, `SET`, `DELETE`, `MERGE`)
- No schema DDL (`CREATE INDEX`, `CREATE CONSTRAINT`)
- Some complex nested subqueries
- CASE expressions (in progress)

**Query Planning**:
- Path functions in WITH clause CTEs need special handling
- Property resolution in WITH scopes (edge cases)
- Some complex multi-hop WITH patterns

### Scope: Read-Only Engine

**Out of Scope** (by design):
- ❌ Write operations
- ❌ Schema modifications  
- ❌ Transaction management
- ❌ Data mutations

ClickGraph is a **read-only analytical query engine**. Use ClickHouse directly for data loading and updates.

## Schema Consolidation Progress

**Status**: 65% Complete (Phase 1B in progress)

### ✅ Completed Phases

**Phase 0**: Analyzer Pass Reordering
- Moved `GraphJoinInference` from Step 15 → Step 4
- `PatternSchemaContext` now available for downstream passes
- Commit: `eced0a0` (Jan 14, 2026)

**Phase 1A-2**: Infrastructure
- `PatternSchemaContext` unified abstraction for all schema variations
- `JoinStrategy` and `NodeAccessStrategy` enums
- Schema-agnostic property resolution framework

**Phase 3**: CTE Unification (Partial)
- New `cte_manager` module with 6 strategy implementations
- `TraditionalCteStrategy`, `DenormalizedCteStrategy`, `FkEdgeCteStrategy`
- `MixedAccessCteStrategy`, `EdgeToEdgeCteStrategy`, `CoupledCteStrategy`
- Production-ready with comprehensive testing

### 🔄 Phase 1B: Property Resolution Refactoring (In Progress)

**Goal**: Eliminate scattered conditional logic in 20+ files

**✅ Completed Refactors** (2/20+ files):
- `src/query_planner/analyzer/projected_columns_resolver.rs` - Replaced `if view_scan.is_denormalized` with `NodeAccessStrategy` matching
- `src/query_planner/analyzer/filter_tagging.rs` - Refactored `is_node_denormalized()` to use `plan_ctx.get_node_strategy()`

**Files needing refactoring** (18+ remaining):
- `src/query_planner/analyzer/view_resolver.rs` - Schema-specific view resolution
- `src/render_plan/property_expansion.rs` - Table alias logic for denormalized nodes
- `src/query_planner/logical_plan/view_scan.rs` - `is_denormalized` field usage
- `src/query_planner/logical_plan/mod.rs` - Denormalized flag propagation
- `src/query_planner/translator/property_resolver.rs` - Property mapping conditionals
- `src/query_planner/analyzer/filter_tagging.rs` - Additional denormalized logic (apply_property_mapping)
- `src/graph_catalog/config.rs` - `is_denormalized` calculations
- `src/graph_catalog/pattern_schema.rs` - Denormalized detection logic
- `src/render_plan/cte_manager/mod.rs` - CTE strategy conditionals

**Migration Pattern**:
```rust
// OLD: Scattered conditionals
if view_scan.is_denormalized {
    // denormalized logic
} else {
    // standard logic
}

// NEW: Unified PatternSchemaContext
match pattern_ctx.node_access_strategy(node_alias) {
    NodeAccessStrategy::Direct => { /* standard logic */ }
    NodeAccessStrategy::Embedded(from_rel, role) => { /* denormalized logic */ }
}
```

**Remaining Work**:
- Phase 2: Consolidate `cte_extraction.rs` scattered logic
- Phase 3-4: JOIN ordering optimization and comprehensive testing

## Next Priorities

### Immediate (This Week)
1. Fix IC-9 CTE column naming issue (WITH DISTINCT + WHERE)
2. Fix scalar aggregate WITH + GROUP BY (TableAlias refactoring)
3. Test remaining LDBC IC/BI queries
4. Address OPTIONAL MATCH + inline property bug

### Short Term (This Month)
1. Complete LDBC benchmark suite testing
2. Improve property resolution in WITH scopes
3. Add CASE expression support
4. FROM clause propagation improvements

### Medium Term
1. Additional graph algorithms (centrality, community detection)
2. Path comprehension enhancements
3. Performance optimizations for large graphs
4. Query result caching

## Architecture

### Component Overview

```
Cypher Query
    ↓
Parser (open_cypher_parser/)
    ↓
Logical Plan (query_planner/)
    ↓
Optimizer (query_planner/optimizer/)
    ↓
SQL Generator (clickhouse_query_generator/)
    ↓
ClickHouse Client
    ↓
Results
```

### Key Modules

- **open_cypher_parser/**: Parses Cypher into AST
- **query_planner/**: Converts AST to logical plan
  - `analyzer/`: Query validation and analysis
  - `logical_plan/`: Core planning structures
  - `optimizer/`: Query optimization passes
- **clickhouse_query_generator/**: Generates ClickHouse SQL
- **graph_catalog/**: Schema management
- **server/**: HTTP API (port 8080) and Bolt protocol (port 7687)

### Schema Architecture

**View-Based Model**: Map existing ClickHouse tables to graph structure via YAML configuration. No special graph tables required.

**Multi-Schema**: Load multiple independent schemas from single YAML file. Select via USE clause or API parameter.

## Documentation

### User Documentation
- [README.md](README.md) - Project overview and quick start
- [docs/wiki/](docs/wiki/) - Complete user guide
  - Getting Started, API Reference, Cypher Language Reference
  - Schema Configuration, Deployment Guides
  - Performance Optimization, Use Cases

### Developer Documentation
- [DEVELOPMENT_PROCESS.md](DEVELOPMENT_PROCESS.md) - 5-phase development workflow
- [TESTING.md](TESTING.md) - Testing procedures
- [docs/development/](docs/development/) - Architecture and design docs
- [notes/](notes/) - Feature implementation details

### Benchmarks
- [benchmarks/ldbc_snb/](benchmarks/ldbc_snb/) - LDBC Social Network Benchmark
- [benchmarks/social_network/](benchmarks/social_network/) - Social network test suite

## Getting Started

### Quick Start

```bash
# Start ClickHouse
docker-compose up -d

# Configure environment
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USER="default"
export CLICKHOUSE_PASSWORD="default"
export GRAPH_CONFIG_PATH="./schemas/examples/social_network.yaml"

# Start ClickGraph
cargo run --release --bin clickgraph

# Test query
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (n:User) RETURN n LIMIT 5"}'
```

### Connect with Neo4j Tools

ClickGraph implements Neo4j Bolt protocol v5.8, enabling connection from Neo4j Browser, Cypher Shell, and other Bolt clients:

```bash
# Neo4j Browser: bolt://localhost:7687
# Cypher Shell
cypher-shell -a bolt://localhost:7687 -u neo4j -p password
```

See [docs/wiki/](docs/wiki/) for detailed setup and configuration.

## Release History

See [CHANGELOG.md](CHANGELOG.md) for complete release history.

**Recent releases**:
- **v0.6.1** (Jan 2026) - WITH clause fixes, GraphRAG multi-type VLP, LDBC SNB benchmark progress
- **v0.6.0** (Dec 2025) - Edge constraints, VLP improvements, semantic validation
- **v0.5.x** (Oct 2025) - Multi-schema, pattern comprehensions, PageRank

## Contributing

ClickGraph follows a disciplined development process:

1. **Design** - Understand spec, sketch SQL examples
2. **Implement** - AST → Parser → Planner → SQL Generator
3. **Test** - Manual smoke test → Unit tests → Integration tests
4. **Debug** - Add debug output, validate SQL
5. **Document** - Update docs, CHANGELOG, feature notes

See [DEVELOPMENT_PROCESS.md](DEVELOPMENT_PROCESS.md) for complete workflow.

## License

See [LICENSE](LICENSE) file.
