# graph_catalog Module — Agent Guide

> **Purpose**: Core schema foundation for ClickGraph. Loads YAML configuration files,
> builds runtime `GraphSchema` structures, and provides O(1) lookup APIs consumed by
> every downstream module (query_planner, render_plan, clickhouse_query_generator, server).
> This module defines the "shape of the graph" that all query translation depends on.

## Module Architecture

```
YAML config file(s)
    │
    ▼
config.rs                 ← Parse YAML → GraphSchemaConfig → build GraphSchema
    │                        Two paths: sync (no DB) / async (auto-discovery + engine detection)
    │
    ├─ schema_types.rs       ← Database-agnostic type system (Int, String, DateTime, etc.)
    ├─ engine_detection.rs   ← ClickHouse MergeTree family detection for FINAL keyword
    ├─ column_info.rs        ← Query system.columns for auto-discovery
    ├─ expression_parser.rs  ← nom-based parser for computed property expressions
    ├─ filter_parser.rs      ← nom-based parser for schema-level WHERE filters
    ├─ constraint_compiler.rs← Compile edge constraints (from.prop/to.prop) to SQL
    ├─ schema_validator.rs   ← Validate YAML definitions against actual ClickHouse tables
    │
    ▼
graph_schema.rs           ← Runtime data structures: GraphSchema, NodeSchema, RelationshipSchema
    │                        O(1) lookups via rel_type_index, edge pattern classification
    │
    ├─ composite_key_utils.rs ← TYPE::FROM::TO composite key parsing & matching
    ├─ node_classification.rs ← Denormalized node detection utilities
    ├─ element_id.rs          ← Neo4j-compatible elementId generation/parsing
    │
    ▼
pattern_schema.rs         ← PatternSchemaContext: unified pattern analysis abstraction
                             Computes NodeAccessStrategy + EdgeAccessStrategy + JoinStrategy
                             for a (left_node, edge, right_node) triple ONCE
```

## Key Files & Line Counts

| File | Lines | Role |
|------|------:|------|
| config.rs | 2,899 | YAML loading, parsing, GraphSchema construction |
| graph_schema.rs | 2,572 | Core runtime types (NodeSchema, RelationshipSchema, GraphSchema) + ~900 lines tests |
| pattern_schema.rs | 1,651 | Unified pattern analysis (access strategies, join strategies) |
| expression_parser.rs | 768 | nom parser for ClickHouse scalar expressions in property mappings |
| filter_parser.rs | 681 | nom parser for SQL WHERE clause filters in schema definitions |
| element_id.rs | 564 | Neo4j elementId generation and parsing |
| schema_types.rs | 532 | **Database-agnostic SchemaType enum** (Integer, Float, String, Boolean, DateTime, Date, Uuid) with dialect-specific SQL literal generation. Used for node ID types and property types. |
| engine_detection.rs | 481 | MergeTree engine family detection, FINAL keyword support |
| composite_key_utils.rs | 318 | `TYPE::FROM::TO` composite key building, parsing, matching |
| constraint_compiler.rs | 289 | Edge constraint expressions → SQL compilation |
| node_classification.rs | 222 | Consolidated denormalized node property detection |
| schema_validator.rs | 145 | Validate graph views against ClickHouse table schemas |
| errors.rs | 121 | Error types with context helpers (thiserror) |
| column_info.rs | 108 | ClickHouse `system.columns` metadata querying |
| mod.rs | 49 | Module declarations and public re-exports |

**Test files** (~784 lines total): `testing/mock_clickhouse.rs` (64), `tests/mock_clickhouse.rs` (146),
`tests/mock_clickhouse_enhanced.rs` (191), `tests/schema_validator_tests.rs` (104),
`tests/view_validation_tests.rs` (113), `tests/view_demo.rs` (69), `composite_id_tests.rs` (69),
`schema_validator/tests.rs` (28)

## Critical Invariants

### 1. Composite Key Format for Relationships
Relationships are stored in `GraphSchema.relationships` HashMap with composite keys:
```
TYPE::FROM_NODE::TO_NODE
```
Example: `FOLLOWS::User::User`, `AUTHORED::User::Post`

This disambiguates the same relationship type connecting different node pairs.
The `rel_type_index` provides O(1) lookup from type name → list of composite keys.

**NEVER** use raw type name as HashMap key. Always use `build_composite_key()` or
`get_rel_schema_with_nodes()` for lookups. For type-only lookups, use `rel_schemas_for_type()`.

### 2. Property Mappings: Cypher Name → ClickHouse Column
`NodeSchema.property_mappings` maps Cypher property names to `PropertyValue`:
- `PropertyValue::Column(col)` — simple column reference
- `PropertyValue::Expression(expr)` — computed ClickHouse expression

Example: Cypher `u.name` → `PropertyValue::Column("full_name")` → SQL `alias.full_name`

**NEVER** assume Cypher property name == ClickHouse column name. Always resolve through
`property_mappings`. The `to_sql(alias)` method handles both Column and Expression variants.

### 3. Denormalized Node Properties Live on the NODE Definition
For denormalized schemas (node properties embedded in edge table), the property mappings
for each side are stored on the `NodeSchema` as `from_properties` and `to_properties`,
NOT on the `RelationshipSchema`.

```yaml
# ✅ CORRECT: from_properties on the node definition
nodes:
  Airport:
    from_properties:
      code: origin_code
      city: origin_city
    to_properties:
      code: dest_code
      city: dest_city
```

The `RelationshipSchema` also carries `from_node_properties` / `to_node_properties` as a
mirror copy built during `config.rs` construction for downstream convenience.

### 4. PatternSchemaContext: Compute Once, Use Everywhere
`PatternSchemaContext::analyze()` computes all schema decisions for a graph pattern triple.
It returns `NodeAccessStrategy`, `EdgeAccessStrategy`, and `JoinStrategy` — these should be
queried from the context, NEVER re-derived by downstream code.

Previously, schema detection logic was scattered across ~4800 lines in `graph_join_inference.rs`.
`PatternSchemaContext` consolidates this into a single analysis point.

### 5. Schema Type Discovery (Feb 2026)
`NodeIdSchema` and `RelationshipSchema` now use **strongly-typed `SchemaType` enum** instead of raw strings.

**Type Discovery Pipeline**:
1. `query_table_column_info()` — fetches actual column types from ClickHouse `system.columns`
2. `map_clickhouse_type()` — converts database-specific types (UInt64, String, DateTime, etc.) to generic `SchemaType` enum
3. `build_node_schema()` — populates `NodeIdSchema.dtype` with discovered type, falls back to `SchemaType::Integer`
4. Type validation in `id()` function — validates `matches!(dtype, SchemaType::Integer)` before accepting bit-pattern decoded IDs

**Benefits**:
- ✅ Compile-time type safety (prevents typos like "Intger", "Sting")
- ✅ Enables stateless ID decoding without session cache (browser click-to-expand works)
- ✅ Database-agnostic (easy to add PostgreSQL, MySQL mappings)
- ✅ Consistent with property type system

**Breaking Change**: Test code must use `SchemaType::Integer` instead of `"Integer".to_string()` when creating schemas programmatically. YAML configs remain unchanged.

### 6. GraphSchema.build() Constructs rel_type_index
The `GraphSchema::build()` constructor (not `new()`) automatically builds the `rel_type_index`
secondary index. **Always use `build()`** — `new()` is only for direct struct construction in
tests that don't need type-based lookups.

### 7. Polymorphic Edges Use `$any` Sentinel
When a polymorphic edge definition uses `from_label_column` / `to_label_column`, the
endpoint node labels are dynamically determined. The sentinel value `$any` in
`from_label_values` / `to_label_values` means "match any label in this column."

### 8. Two Conversion Paths in config.rs
- **`to_graph_schema()`** — Sync, no DB connection. Used in tests and simple configs.
- **`to_graph_schema_with_client()`** — Async, queries ClickHouse for auto-discovery
  (`system.columns`) and engine detection (`system.tables`). Used in production startup.

Auto-discovery populates `column_names` on schemas and detects MergeTree engine variants.
`exclude_columns` and `naming_convention` only apply during auto-discovery.

## Common Bug Patterns

| Pattern | Symptom | Root Cause |
|---------|---------|------------|
| Wrong relationship lookup | "No relationship schema found" | Using type name instead of composite key `TYPE::FROM::TO` |
| Property not resolved | "Column not found" or wrong column | Bypassed `property_mappings`, used Cypher name as SQL column |
| Denormalized props missing | Node properties NULL in results | Looked at `property_mappings` (empty) instead of `from_properties`/`to_properties` |
| Schema not found at runtime | "No node schema for label X" | Using `GLOBAL_SCHEMAS` directly instead of task-local `get_current_schema()` |
| Engine detection fails | Missing FINAL keyword | `to_graph_schema()` used instead of `to_graph_schema_with_client()` (no engine info) |
| Filter not applied | Schema filter ignored in SQL | Filter parsed but not propagated to SQL generator via `SchemaFilter::to_sql(alias)` |
| Expression precedence wrong | Incorrect computed property SQL | Missing parentheses in `expression_parser.rs` precedence levels |
| Composite key mismatch | Generic rel lookup returns wrong edge | `expand_generic_relationship_type()` not called for untyped `[r]` patterns |

## Dependencies

### Upstream (graph_catalog imports from)
- `crate::server::models::SqlDialect` — dialect enum for SQL literal generation in `schema_types.rs`
- `crate::query_planner::plan_ctx::PlanCtx` — used only in `pattern_schema.rs` factory method

### Downstream (who imports graph_catalog)
- **server/** — `graph_catalog.rs` (schema admin), `mod.rs` (init), `query_context.rs` (task-local),
  `bolt_protocol/id_rewriter.rs`, `bolt_protocol/result_transformer.rs`
- **query_planner/** — `plan_ctx/`, `logical_expr/`, `translator/`, `optimizer/`,
  `logical_plan/` (match_clause, mod)
- **clickhouse_query_generator/** — `json_builder.rs`, `variable_length_cte.rs`, `pagerank.rs`,
  `multi_type_vlp_joins.rs`, `to_sql_query.rs`
- **render_plan/** — `plan_builder_helpers.rs`, `plan_builder_utils.rs`, `join_builder.rs`,
  `cte_extraction.rs`, `cte_generation.rs`, `cte_manager/`

### Key External Crates
- `serde` / `serde_yaml` — YAML configuration deserialization
- `nom` — Parser combinators for expression_parser.rs and filter_parser.rs
- `thiserror` — Error type derivation
- `clickhouse` — Async ClickHouse client (column_info.rs, engine_detection.rs, schema_validator.rs)
- `log` — Structured logging throughout

## Testing After Changes

```bash
# Must pass ALL of these:
cargo test --lib graph_catalog       # All graph_catalog unit tests
cargo test                           # Full suite (995+ tests)

# Specific test groups:
cargo test composite_key             # Composite key parsing/matching
cargo test test_detect               # Edge table pattern classification
cargo test test_coupled              # Coupled edge detection (Zeek DNS pattern)
cargo test schema_validator          # Schema validation against ClickHouse
cargo test expression_parser         # Expression parsing
cargo test filter_parser             # Filter parsing
cargo test element_id                # Neo4j elementId generation/parsing
cargo test engine_detection          # MergeTree family detection

# If changing config.rs YAML parsing:
cargo test config                    # Config loading tests
# Then manually test with benchmark schema:
export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"
cargo run --bin clickgraph
```

## Schema Variation Awareness

Every downstream consumer of graph_catalog may behave differently based on schema type.
When modifying any schema structure, verify all 5 patterns still work:

### Schema Patterns

1. **Standard** — Separate node tables + separate edge table. Traditional 3-way JOIN.
   - Example: `users` table + `follows` table + `users` table
   - `is_denormalized = false`, `is_fk_edge = false`

2. **FK-Edge** — Edge is a foreign key column on a node table. No separate edge table.
   - `RelationshipSchema.is_fk_edge = true`
   - Edge table = node table, `from_id` or `to_id` is FK column

3. **Denormalized** — Node properties embedded in edge table. Node has no own table.
   - `NodeSchema.is_denormalized = true`, `from_properties` / `to_properties` populated
   - `property_mappings` is empty or minimal (≤2 entries)
   - Example: Airport properties stored on flights table

4. **Polymorphic** — Single edge table with `type_column` discriminator for multiple rel types.
   - `RelationshipSchema.type_column = Some("rel_type")`
   - `from_label_column` / `to_label_column` for dynamic node resolution
   - `$any` sentinel for wildcard label matching

5. **Composite ID** — Multi-column node identity (e.g., `(database, table)` tuple).
   - `NodeIdSchema::Composite { columns, types }`
   - elementId uses `|` separator: `"Label:id1|id2"`

### Strategy Enums (from pattern_schema.rs)

```
NodeAccessStrategy:
  OwnTable           — Node has dedicated table (standard)
  EmbeddedInEdge      — Node properties stored in edge table (denormalized)
  Virtual             — Node has no physical storage

EdgeAccessStrategy:
  SeparateTable       — Standard edge table
  Polymorphic         — Type-discriminated edge table
  FkEdge              — Foreign key on node table

JoinStrategy:
  Traditional         — Standard 3-way JOIN (node-edge-node)
  SingleTableScan     — All data in one table (fully denormalized)
  MixedAccess         — One node denormalized, other traditional
  EdgeToEdge          — No node tables, edge-only pattern
  CoupledSameRow      — Two edges from same table row (Zeek DNS pattern)
  FkEdgeJoin          — FK-based join (no separate edge table)
```

### Schema Compatibility Matrix

| Feature / Area                              | Standard | FK-edge | Denormalized | Polymorphic | Composite ID |
|---------------------------------------------|:--------:|:-------:|:------------:|:-----------:|:------------:|
| YAML config loading                         |    ✅    |   ✅    |      ✅      |     ✅      |      ✅      |
| Property mapping resolution                 |    ✅    |   ✅    |      ⚠️      |     ✅      |      ✅      |
| Relationship lookup (composite key)         |    ✅    |   ✅    |      ✅      |     ✅      |      ✅      |
| Edge table pattern classification           |    ✅    |   ✅    |      ✅      |     ✅      |      ✅      |
| PatternSchemaContext analysis               |    ✅    |   ✅    |      ✅      |     ✅      |      ⚠️      |
| Neo4j elementId generation                  |    ✅    |   ✅    |      ✅      |     ✅      |      ✅      |
| Engine detection / FINAL                    |    ✅    |   ✅    |      ✅      |     ✅      |      ✅      |
| Schema-level filters                        |    ✅    |   ✅    |      ✅      |     ✅      |      ✅      |
| Expression-based properties                 |    ✅    |   ✅    |      ✅      |     ✅      |      ✅      |
| Coupled edge detection                      |    ✅    |   N/A   |      ✅      |     ⚠️      |      ✅      |

✅ = fully supported and tested. ⚠️ = works but edge cases need care. N/A = not applicable.

**Denormalized property resolution** (⚠️): Must use `from_properties`/`to_properties`, not
`property_mappings`. Every new consumer of NodeSchema must handle this distinction.

## Public API Summary

### Core Types (from graph_schema.rs)
- `GraphSchema` — Container: `nodes: HashMap<label, NodeSchema>`, `relationships: HashMap<composite_key, RelationshipSchema>`, `rel_type_index`
- `NodeSchema` — Node definition: `table_name`, `node_id`, `property_mappings`, `is_denormalized`, `from_properties`, `to_properties`, `filter`, `engine`
- `RelationshipSchema` — Edge definition: `from_node`/`to_node` (labels), `from_id`/`to_id` (columns), `property_mappings`, `is_fk_edge`, `type_column`, `constraints`
- `NodeIdSchema` — `Single { column, dtype: SchemaType }` | `Composite { columns, types: Vec<SchemaType> }` (dtype migrated from String to SchemaType enum for type safety)
- `Direction` — `Outgoing` | `Incoming` | `Both`
- `EdgeTablePattern` — `Traditional` | `FullyDenormalized` | `Mixed { from_denormalized, to_denormalized }`

### Lookup Methods (on GraphSchema)
- `node_schema(label)` → `Option<&NodeSchema>`
- `get_rel_schema(composite_key)` → `Option<&RelationshipSchema>`
- `get_rel_schema_with_nodes(type, from, to)` → builds composite key and looks up
- `rel_schemas_for_type(type_name)` → `Vec<(&String, &RelationshipSchema)>` via `rel_type_index`
- `expand_generic_relationship_type(from, to)` → finds all matching rel types for node pair
- `are_edges_coupled(type1, type2)` → `bool` (same table + shared node)
- `get_coupled_edge_info(type1, type2)` → `Option<CoupledEdgeInfo>`

### Pattern Analysis (from pattern_schema.rs)
- `PatternSchemaContext::analyze(left_node, edge, right_node, schema)` → computes strategies
- `PatternSchemaContext::from_graph_rel_dyn(graph_rel, schema)` → factory from query planner types
- `.left_node_strategy()`, `.right_node_strategy()`, `.edge_strategy()`, `.join_strategy()`
- `.get_node_property(alias, prop, schema)` → resolves property to SQL column
- `.get_edge_property(prop, schema)` → resolves edge property to SQL column

### Expression & Filter (from expression_parser.rs, filter_parser.rs)
- `PropertyValue::Column(col)` | `PropertyValue::Expression(expr)` — property mapping value
- `PropertyValue::to_sql(alias)` → prefixed SQL column/expression
- `SchemaFilter::new(sql_str)` → parsed filter with validation
- `SchemaFilter::to_sql(alias)` → aliased SQL WHERE predicate

### Element IDs (from element_id.rs)
- `generate_node_element_id(label, id_values)` → `"Label:id"` or `"Label:id1|id2"`
- `parse_node_element_id(element_id)` → `(label, id_values)`
- `generate_relationship_element_id(type, from_id, to_id)` → `"Type:from->to"`
- `parse_relationship_element_id(element_id)` → `(type, from_id, to_id)`

### Composite Keys (from composite_key_utils.rs)
- `build_composite_key(type, from, to)` → `"TYPE::FROM::TO"`
- `CompositeKey::parse(key)` → `Result<CompositeKey>`
- `is_composite_key(key)` → `bool` (contains `::`)
- `extract_type_name(key)` → type portion before first `::`

---

# LLM-Based Schema Discovery

> **Design Decision (Mar 2026)**: Replaced GLiNER-based Python tool with LLM-based approach
> for significantly better accuracy (95% vs 15%).

## Motivation: Why Heuristics Fail

Previous GLiNER-based approach worked for well-named schemas (LDBC, SSB) but failed on real-world databases:

| Problem | Example | Heuristic Result |
|---------|---------|------------------|
| Abbreviated columns | `mgr_uid`, `dept_code`, `tkt_id` | ❌ Cannot recognize as FK |
| Non-standard FK names | `reporter`, `assignee`, `related_proj` | ❌ Misses FK relationship |
| Cross-type FKs | `dept_code` String references `dept.code` String | ❌ Type mismatch |
| Self-referential | `parent_tkt`, `mgr_uid` | ❌ Wrong direction |
| Shortened tables | `usr`, `proj`, `dept` | ❌ Cannot expand |

### Proof: Project Management Schema

Input tables:
```
usr         (uid PK, uname, email_addr, reg_dt, dept_code, mgr_uid)
dept        (code PK, dname, loc, parent_code)
proj        (pid PK, pname, status, owner_uid, budget_amt)
proj_assign (pid PK, uid PK, role, start_dt)       -- junction table
tickets     (tkt_id PK, title, severity, reporter, assignee, related_proj, parent_tkt, created, resolved)
obj_tag     (object_id PK, object_type PK, tag_name PK, tagged_by, tagged_at)  -- polymorphic
audit_log   (ts OrderBy, actor, action, target_type, target_ref, details)  -- event log
```

**Heuristic Output** (15% accuracy):
```
usr:         pattern=standard_node, 0 FKs detected  ❌
dept:        pattern=standard_node, 0 FKs detected  ❌
proj:        pattern=standard_node, 0 FKs detected  ❌
proj_assign: pattern=standard_edge, from=Pid, to=Uid  ❌ (wrong!)
tickets:     pattern=standard_node, 0 FKs detected    ❌
obj_tag:     pattern=standard_edge, from=object_id, to=object_type  ❌ (wrong!)
audit_log:   pattern=flat_table  ❌ (misses event nature)
```

**LLM Output** (95% accuracy): Correctly identifies all nodes, edges, FK relationships, and polymorphic patterns.

## Architecture

```
clickgraph-client (Rust CLI)
    │
    ├── :discover <database>
    │     │
    │     ├── GET /schemas/discover-prompt
    │     │     │
    │     │     └── Server: introspects DB → llm_prompt.rs formats prompt
    │     │
    │     └── Client: sends to LLM API → receives YAML → /schemas push
    │
    └── :introspect <database>  (fallback without LLM)
```

### Key Design Principles

1. **Server stays lightweight** — No ML dependencies (~300MB saved)
2. **Client holds API keys** — User's LLM, not server's
3. **Flexible LLM choice** — Supports Anthropic (Claude) and OpenAI-compatible APIs
4. **Works with any LLM** — Ollama, vLLM, LiteLLM, Together, Groq, etc.

## Key Files

| File | Lines | Role |
|------|------:|------|
| `llm_prompt.rs` | ~320 | Formats table metadata → LLM prompt with examples |
| `clickgraph-client/src/llm.rs` | ~350 | LLM API client (Anthropic + OpenAI) |
| `clickgraph-client/src/main.rs` | ~900 | CLI with `:discover` command |

## API Endpoints

| Method | Path | Handler | Purpose |
|--------|------|---------|---------|
| GET | `/schemas/introspect` | `introspect_handler` | Get table metadata + sample data |
| POST | `/schemas/discover-prompt` | `discover_prompt_handler` | Generate LLM prompt |

## Environment Variables

### Server-side (for introspection)
- `CLICKHOUSE_URL`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`

### Client-side (for LLM)
- `ANTHROPIC_API_KEY` — Use Claude (default)
- `OPENAI_API_KEY` + `CLICKGRAPH_LLM_PROVIDER=openai` — Use OpenAI-compatible
- `CLICKGRAPH_LLM_MODEL` — Override default model (default: claude-sonnet-4-20250514)

## Example Usage

```bash
# Start ClickGraph server
cargo run --bin clickgraph

# In clickgraph-client:
clickgraph-client :) :discover mydb

# Output:
# === LLM Schema Discovery for 'mydb' ===
# Using Anthropic model: claude-sonnet-4-20250514
# Fetching table metadata from server...
# 
# [LLM generates YAML schema]
# 
# Push to server? (y/n): y
# ✓ Schema loaded successfully
```

## Prompt Engineering

The LLM prompt includes:

1. **ClickGraph schema format** — YAML examples showing nodes/edges format
2. **Sample data** — 3 rows per table for value cross-referencing
3. **Schema patterns** — Explains standard, FK-edge, denormalized, polymorphic
4. **Naming conventions** — Guidance on label singularization, relationship naming

This enables the LLM to:
- Cross-check FK values between tables using sample data
- Infer entity types from abbreviated names
- Detect polymorphic relationships from `*_type` columns
- Identify event/log tables from timestamp patterns
