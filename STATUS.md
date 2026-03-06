# ClickGraph Status

*Updated: March 5, 2026*

## Current Version: v0.6.3-dev

Read-only Cypher-to-ClickHouse SQL query engine with Neo4j Browser compatibility.
Supports both remote ClickHouse and embedded (in-process) mode via chdb.

**Unit Tests**: 1,180 passing (100%)
**Integration Tests**: 183 passing (100%)
**Embedded Tests**: 31 passing (unit + e2e)
**LDBC SNB**: 36/37 queries passing at sf0.003 and sf1 (97%) — bi-16 blocked by CALL subquery
**Benchmark**: 14/14 queries (100%)
**E2E Tests**: Bolt 4/4, Cache 5/5 (100%)

### Integration Test Breakdown

| Category | Tests | Status |
|----------|-------|--------|
| Core query patterns | ~2,400 | Passing |
| Matrix cross-schema | ~350 | Passing |
| VLP / shortest path | ~150 | Passing |
| GraphRAG / vector | ~80 | Passing |
| Schema variations | ~90 | ~37 need schemas not in default config |
| Environment-dependent | ~70 | Need specific ClickHouse tables/fixtures |

## What Works

- **Cypher queries**: MATCH, WHERE, RETURN, WITH, ORDER BY, LIMIT, SKIP, DISTINCT, OPTIONAL MATCH, UNWIND, UNION ALL
- **Graph patterns**: Node/relationship patterns, variable-length paths (`*1..3`), shortest path, multi-hop traversals
- **Path functions**: `length(p)`, `nodes(p)`, `relationships(p)`, `cost(p)` (weighted shortest path)
- **Aggregations**: count, sum, avg, min, max, collect — with GROUP BY
- **Functions**: String, numeric, date, type coercion, list operations, CASE expressions
- **List comprehension**: `[x IN list WHERE cond | expr]` with `arrayCount()` optimization
- **Pattern comprehension**: `size([(a)-[:R]->(b) | b.prop])` via pre-aggregated CTEs
- **Multi-relationship**: `[:TYPE1|TYPE2]` with UNION SQL generation
- **Map literals**: `collect({key: val})` with map property access (`top.score`)
- **Variable Scope Resolution**: `VariableScope` correctly resolves variables across WITH barriers — CTE-scoped vars use CTE columns, table vars use schema columns; covers SELECT, WHERE, ORDER BY, GROUP BY, HAVING, JOINs
- **Scope-Aware CTE/UNION Rendering**: Task-local `VariableRegistry` with `property_mapping` on `VariableSource::Cte` enables correct column resolution during SQL rendering; per-CTE save/restore; WITH barrier scope clearing; UNION branch recursion
- **Anchor-Aware Join Generation**: Generic 64-line loop + 810-line module replaces ~1200 lines of per-strategy handlers; topological sort on schema-independent join graph; handles OPTIONAL MATCH shared-node patterns without cartesian products
- **Unified Type Inference**: Single 4-phase pass (SchemaInference merged Feb 2026) with direction-aware UNION generation
  - **Phase 0**: Relationship-based label inference
  - **Phase 1**: Filter→GraphRel UNION with WHERE constraint extraction
  - **Phase 2**: Untyped node UNION with direction validation
  - **Phase 3**: ViewScan resolution (Empty → table scans, denormalized nodes deferred to render phase)
- **Property pruning**: Untyped queries skip tables missing referenced properties (10x–50x speedup)
- **Multi-schema**: USE clause, per-request schema selection, GLOBAL_SCHEMAS registry
- **Multi-tenancy**: Parameterized views with `tenant_id`, session commands (`CALL sys.set`)
- **Neo4j Bolt v5.8**: Browser click-to-expand, schema procedures, session commands, EXPLAIN handling
- **Schema variations**: Standard, denormalized, FK-edge, polymorphic, composite ID, multi-tenant
- **Query cache**: Keyed by query + schema + tenant_id + view_parameters
- **LLM-powered schema discovery**: Interactive schema generation from natural language via Anthropic/OpenAI
- **GraphRAG structured output**: `format: "Graph"` returns deduplicated nodes, edges, and stats
- **ClickHouse cluster load balancing**: `CLICKHOUSE_CLUSTER` for auto-discovery and load balancing
- **Embedded mode** (`--features embedded`): `QueryExecutor` trait + `ChdbExecutor` + `clickgraph-embedded` crate — run Cypher queries in-process over Parquet/Iceberg/Delta/S3 without a ClickHouse server. Kuzu-compatible Rust API (`Database`, `Connection`, `QueryResult`). `source:` URI field in YAML schema. S3/GCS/Azure credential support via `StorageCredentials`.
- **APOC Export Procedures**: Neo4j-compatible `CALL apoc.export.{csv|json|parquet}.query(cypher, destination, config)` — translates inner Cypher to SQL, resolves destination URI (local file, S3, GCS, Azure, HTTP), wraps in `INSERT INTO FUNCTION`. Works in server mode (HTTP + Bolt) and embedded mode.

## Current Limitations

See [KNOWN_ISSUES.md](KNOWN_ISSUES.md) for details.

- ❌ Write operations (CREATE, SET, DELETE, MERGE) — out of scope by design
- ⚠️ CALL subquery not supported (blocks LDBC bi-16)
- ⚠️ Shortest path may OOM on dense graphs — use bounded ranges `*1..5`
- ⚠️ Multiple standalone UNWIND without MATCH partially supported (single UNWIND works)
- ⚠️ Neo4j Desktop/NeoDash: works with `--neo4j-compat-mode`; `startNode`/`endNode` and `WITH *` now implemented for node expansion

## Architecture

```
Cypher → Parser → Logical Plan → Optimizer → SQL Generator → ClickHouse → Results
```

**Modules**: `open_cypher_parser/` (AST), `query_planner/` (planning + analysis), `render_plan/` (SQL rendering), `graph_catalog/` (YAML schemas), `server/` (HTTP:8080 + Bolt:7687)

## Documentation

- [docs/wiki/](docs/wiki/) — User guide, API, Cypher reference, schema config
- [DEVELOPMENT_PROCESS.md](DEVELOPMENT_PROCESS.md) — 5-phase workflow
- [CHANGELOG.md](CHANGELOG.md) — Release history
- [KNOWN_ISSUES.md](KNOWN_ISSUES.md) — Active issues
