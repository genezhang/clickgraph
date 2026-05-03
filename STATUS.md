# ClickGraph Status

*Updated: May 2, 2026*

## Current Version: v0.6.7-dev

Cypher-to-ClickHouse SQL query engine with Neo4j Browser compatibility.
Supports server mode, embedded (in-process chdb), remote ClickHouse, and SQL-only translation.
**Embedded mode is now read-write** (v0.6.7+): `CREATE`, `SET`, `DELETE`, and `REMOVE` against ClickGraph-managed tables; server / remote / sql_only modes remain read-only and reject writes upstream.
`cg` CLI tool for agent/script-oriented use (`clickgraph-tool` workspace crate).

**Unit Tests**: 1,601 passing (100%)
**Integration Tests**: 183 passing (100%)
**Embedded Tests**: 152 passing (unit + integration + e2e)
**Go Binding Tests**: 14 passing (100%)
**openCypher TCK**: 383/402 scenarios passing (95.3%), 0 failures, 19 skipped — see `clickgraph-tck/`
**LDBC SNB**: 36/37 queries passing at sf0.003 and sf1 (97%) — bi-16 blocked by CALL subquery
**Benchmark**: 14/14 queries (100%)
**E2E Tests**: Bolt 4/4, Cache 5/5 (100%)
**Endurance**: 24h / 518M requests / 0 server errors / P50=1ms / 27 MB RSS (jemalloc) — [details](tests/stress/RESULTS.md)

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
- **Embedded mode** (`--features embedded`): `QueryExecutor` trait + `ChdbExecutor` + `clickgraph-embedded` crate — run Cypher queries in-process over Parquet/Iceberg/Delta/S3 without a ClickHouse server. Kuzu-compatible Rust API (`Database`, `Connection`, `QueryResult`). `source:` URI field in YAML schema. S3/GCS/Azure credential support via `StorageCredentials`. The `embedded` feature is **opt-in** (default off) so dependent crates can use sql_only/remote modes without pulling in chdb.
- **Cypher writes (embedded mode)** (v0.6.7+): `CREATE`, `SET`, `DELETE` / `DETACH DELETE`, and `REMOVE` against ClickGraph-managed nodes. Translates to ClickHouse lightweight `INSERT` / `UPDATE` / `DELETE`. Writable tables get `enable_block_number_column = 1, enable_block_offset_column = 1` in DDL automatically. Per-node `id_generation` schema attribute (`uuid` default / `provided` / `snowflake`). Returns Neo4j-compatible counters (`nodes_created`, `properties_set`, `nodes_deleted`, `relationships_deleted`). Server / remote / sql_only modes reject writes upstream via the `write_guard` admission check; `source:`-backed nodes/edges remain read-only. `MERGE`, relationship CREATE, `CREATE … RETURN`, edge-alias DELETE, `SET a += {…}` map-merge, and `REMOVE a:Label` are not implemented yet.
- **Remote mode** (`Database::new_remote()`): Cypher translated locally, executed against external ClickHouse. No chdb required. Available without any feature flags.
- **Hybrid remote query + local storage**: `RemoteConfig` in `SystemConfig` enables embedded mode to query a remote ClickHouse cluster via `query_remote()` / `query_remote_graph()`, decompose results into `GraphResult` (nodes + edges), and store subgraphs locally via `store_subgraph()` for fast re-querying. `query_graph()` returns structured graph results for local queries. Available in Rust, Python, and Go.
- **`cg` CLI tool** (`clickgraph-tool`): Agent/script-oriented CLI. `cg sql` (Cypher→SQL), `cg validate` (parse+plan), `cg query` (execute via remote CH), `cg nl` (NL→Cypher via LLM), `cg schema show/validate/discover/diff`. Config via `~/.config/cg/config.toml` or `CG_*` env vars. LLM: Anthropic default, any OpenAI-compatible endpoint. Does not require chdb or a running ClickGraph server.
- **APOC Export Procedures**: Neo4j-compatible `CALL apoc.export.{csv|json|parquet}.query(cypher, destination, config)` — translates inner Cypher to SQL, resolves destination URI (local file, S3, GCS, Azure, HTTP), wraps in `INSERT INTO FUNCTION`. Works in server mode (HTTP + Bolt) and embedded mode.
- **COPY TO Export Syntax**: Kuzu/DuckDB-compatible `COPY (MATCH ...) TO 'path' WITH (format='csv')` — alternative to APOC procedures for exporting query results. Supports CSV, JSON, Parquet, NDJSON. Works in server mode and embedded mode.
- **Vector Search Procedure**: Neo4j-compatible `CALL db.index.vector.queryNodes('index-name', k, [embedding...]) YIELD node, score` — translates to ClickHouse's `cosineDistance()` / `L2Distance()`. Configured via `vector_indexes` section in schema YAML. Supports dimension validation and USE clause schema selection.
- **Full-text Search Procedure**: Neo4j-compatible `CALL db.index.fulltext.queryNodes('index-name', 'search query') YIELD node, score` — translates to ClickHouse's `ngramDistance()`, `multiSearchAnyCaseInsensitive()`, and `hasToken()`. Supports three analyzers: standard (fuzzy + pre-filter), ngram (pure fuzzy), exact (token match). Multi-property search across multiple columns. Configured via `fulltext_indexes` section in schema YAML.
- **Python bindings** (`clickgraph-py`): UniFFI-based Python package via `clickgraph-ffi` shared library. `Database`, `Connection`, `QueryResult` classes with dict-style and Kuzu-compatible tuple-style iteration. 72 Parquet-based chdb e2e tests + 40 sql_only tests.
- **Go bindings** (`clickgraph-go`): UniFFI-generated Go package via `clickgraph-ffi` C ABI. `Open()`, `Connect()`, `Query()`, `QueryToSQL()`, `Export()`. Native Go types, cursor and bulk APIs.

## Current Limitations

See [KNOWN_ISSUES.md](KNOWN_ISSUES.md) for details.

- ✅ Write operations (CREATE, SET, DELETE, REMOVE) — embedded mode only (v0.6.7+); server / remote / sql_only modes remain read-only
- ❌ `MERGE` clause — planned for v0.7.x
- ❌ Relationship `CREATE`, `CREATE … RETURN`, edge-alias `DELETE r`, `SET a += {…}` map-merge, `REMOVE a:Label` — planned
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
