# ClickGraph Status

*Updated: February 19, 2026*

## Current Version: v0.6.1

Read-only Cypher-to-ClickHouse SQL query engine with Neo4j Browser compatibility.

**Tests**: 1,032 unit + integration at main parity  
**Latest Work**: Variable scope resolution redesign (branch `fix/variable-scope-resolution`) — zero regressions, LDBC +4 queries  
**Benchmark**: 18/18 queries (100%) at 5000 scale (954.9M rows)  
**Architecture**: ✅ VariableScope replaces reverse_mapping hack (-1,362 net lines)

## What Works

- **Cypher queries**: MATCH, WHERE, RETURN, WITH, ORDER BY, LIMIT, SKIP, DISTINCT, OPTIONAL MATCH, UNWIND, UNION ALL
- **Graph patterns**: Node/relationship patterns, variable-length paths (`*1..3`), shortest path, multi-hop traversals
- **Path functions**: `length(p)`, `nodes(p)`, `relationships(p)`
- **Aggregations**: count, sum, avg, min, max, collect — with GROUP BY
- **Functions**: String, numeric, date, type coercion, list operations
- **Multi-relationship**: `[:TYPE1|TYPE2]` with UNION SQL generation
- **Variable Scope Resolution**: `VariableScope` correctly resolves variables across WITH barriers — CTE-scoped vars use CTE columns, table vars use schema columns; covers SELECT, WHERE, ORDER BY, GROUP BY, HAVING, JOINs
- **Unified Type Inference**: Single 4-phase pass (SchemaInference merged Feb 2026) with direction-aware UNION generation
  - **Phase 0**: Relationship-based label inference
  - **Phase 1**: Filter→GraphRel UNION with WHERE constraint extraction
  - **Phase 2**: Untyped node UNION with direction validation (🎯 Neo4j Browser expand fix)
  - **Phase 3**: ViewScan resolution (Empty → table scans)
- **Property pruning**: Untyped queries skip tables missing referenced properties (10x–50x speedup)
- **Multi-schema**: USE clause, per-request schema selection, GLOBAL_SCHEMAS registry
- **Multi-tenancy**: Parameterized views with `tenant_id`, session commands (`CALL sys.set`)
- **Neo4j Bolt v5.8**: Browser click-to-expand, schema procedures, session commands, EXPLAIN handling
- **Schema variations**: Standard, denormalized, FK-edge, polymorphic, composite ID, multi-tenant
- **Query cache**: Keyed by query + schema + tenant_id + view_parameters

## Current Limitations

See [KNOWN_ISSUES.md](KNOWN_ISSUES.md) for details.

- ❌ Write operations (CREATE, SET, DELETE, MERGE) — out of scope by design
- ⚠️ Shortest path may OOM on dense graphs — use bounded ranges `*1..5`
- ⚠️ CASE expressions not yet supported
- ⚠️ Neo4j Desktop/NeoDash WebSocket connection (issue #57)

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
