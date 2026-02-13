# procedures Module — Agent Guide

> **Purpose**: Neo4j-compatible metadata procedure execution.
> Handles `CALL db.labels()`, `CALL dbms.components()`, etc.
> These **bypass the query planner entirely** — they read from GraphSchema directly.

## Module Architecture

```
procedures/
├── mod.rs                          (170 lines) ← ProcedureRegistry, type aliases, re-exports
├── executor.rs                     (501 lines) ← Routing, execution, UNION handling, detection
├── return_evaluator.rs             (449 lines) ← RETURN clause evaluation (COLLECT, maps, slicing)
├── db_labels.rs                    (81 lines)  ← db.labels() → all node labels
├── db_property_keys.rs             (86 lines)  ← db.propertyKeys() → all Cypher property names
├── db_relationship_types.rs        (81 lines)  ← db.relationshipTypes() → all edge types
├── db_schema_node_type_properties.rs (112 lines) ← db.schema.nodeTypeProperties() → per-label props
├── db_schema_rel_type_properties.rs  (110 lines) ← db.schema.relTypeProperties() → per-type props
├── dbms_components.rs              (76 lines)  ← dbms.components() → ClickGraph version/edition
└── dbms_stubs.rs                   (63 lines)  ← Browser compatibility stubs (clientConfig, etc.)
```

**Total**: ~1,730 lines

## Execution Flow

```
Cypher query "CALL db.labels()"
    │
    ▼
Parser → CypherStatement::ProcedureCall or CypherStatement::Query with call_clause
    │
    ▼
Handler (HTTP or Bolt) detects procedure-only query
    │  uses is_procedure_only_statement() / is_procedure_union_query()
    ▼
executor.rs routes to ProcedureRegistry
    │
    ▼
Specific procedure function (e.g., db_labels::execute)
    │  receives &GraphSchema, returns Vec<HashMap<String, JsonValue>>
    ▼
If RETURN clause present → return_evaluator.rs transforms results
    │  handles COLLECT(), map literals, array slicing
    ▼
Results formatted as JSON (HTTP) or Bolt records (Bolt protocol)
```

## Key Files

### mod.rs — Registry & Types
- `ProcedureRegistry` — HashMap of procedure name → function pointer
- Registers 10 built-in procedures (6 core + 4 stubs)
- `ProcedureResult = Result<Vec<HashMap<String, JsonValue>>, String>`
- `ProcedureFn = Arc<dyn Fn(&GraphSchema) -> ProcedureResult + Send + Sync>`

### executor.rs — Execution Engine
**Detection functions** (used by handlers to decide execution path):
- `is_procedure_only_statement(&CypherStatement)` — main routing decision
- `is_procedure_only_query(&OpenCypherQueryAst)` — checks single query
- `is_procedure_union_query(&CypherStatement)` — detects CALL UNION ALL CALL

**Execution functions**:
- `execute_procedure_by_name()` — core executor, gets schema, calls procedure fn
- `execute_procedure()` — variant taking parsed AST
- `execute_procedure_query()` — handles CALL + RETURN (applies return_evaluator)
- `execute_procedure_union()` — executes multiple procedures, combines results
- `execute_procedure_union_with_return()` — UNION with per-branch RETURN evaluation

**Schema access**: Uses task-local `get_current_schema()` first, falls back to `GLOBAL_SCHEMAS`.

### return_evaluator.rs — RETURN Clause Processing
Handles the transformation of procedure results through RETURN expressions:
- `apply_return_clause()` — entry point, detects aggregation vs row-by-row
- `evaluate_aggregation_expr()` — handles COLLECT, COUNT, map literals with aggregations
- `evaluate_expr()` — non-aggregation: variables, literals, property access, maps
- `apply_array_slicing()` — `COLLECT(label)[..1000]` support
- `EvalContext` — variable bindings for expression evaluation

### Procedure Implementations (db_*.rs, dbms_*.rs)
Each follows the same pattern:
```rust
pub fn execute(schema: &GraphSchema) -> Result<Vec<HashMap<String, JsonValue>>, String>
```

Key details:
- **db.labels()** — extracts labels from `schema.all_node_schemas()`, handles `::` namespacing
- **db.relationshipTypes()** — extracts from `schema.get_relationships_schemas()`
- **db.propertyKeys()** — collects all Cypher property names (mapping keys, not column names)
- **db.schema.nodeTypeProperties()** — returns per-label property metadata (name, type, mandatory)
- **db.schema.relTypeProperties()** — returns per-type property metadata
- **dbms.components()** — returns `{"name":"ClickGraph","versions":[CARGO_PKG_VERSION],"edition":"community"}`
- **dbms_stubs** — minimal responses for Browser compatibility (clientConfig, showCurrentUser, etc.)

## Critical Invariants

### 1. Procedures NEVER Touch SQL
Procedures read directly from `GraphSchema` metadata. They do **not** generate SQL,
do **not** query ClickHouse, and do **not** go through the query planner.

### 2. Schema Source Priority
`executor::get_schema()` uses:
1. Task-local `get_current_schema()` (set by HTTP/Bolt handlers)
2. Fallback: `GLOBAL_SCHEMAS` lookup by `schema_name` parameter

### 3. Property Keys Are Cypher Names
`db.propertyKeys()` returns keys from `property_mappings` (Cypher-side names),
**not** the underlying ClickHouse column names. This is correct behavior.

### 4. UNION Detection Requirements
`is_procedure_union_query()` requires ALL of:
- Main query has `call_clause`, no `match_clauses`, no `where_clause`, no `with_clause`
- All union branches have the same constraints
- All unions are `UNION ALL` (not DISTINCT)

### 5. Aggregation Detection
`return_evaluator` checks for aggregation functions recursively in RETURN expressions.
If **any** return item contains COLLECT/COUNT/SUM/AVG/MIN/MAX, the entire result set
is aggregated into a single output record.

## Dependencies

**What this module uses**:
- `graph_catalog::graph_schema::GraphSchema` — schema metadata
- `open_cypher_parser` — AST types and parsing (for detection functions)
- `server::query_context` — task-local schema access
- `server::GLOBAL_SCHEMAS` — fallback schema access
- `serde_json` — result value types

**What uses this module**:
- `server/handlers.rs` — HTTP query handler (procedure detection + execution)
- `server/bolt_protocol/handler.rs` — Bolt query handler (procedure detection + execution)

## Public API

```rust
// Detection (re-exported from mod.rs)
pub fn is_procedure_only_statement(stmt: &CypherStatement) -> bool;
pub fn is_procedure_only_query(query: &OpenCypherQueryAst) -> bool;
pub fn is_procedure_union_query(stmt: &CypherStatement) -> bool;

// Execution (re-exported from mod.rs)
pub fn execute_procedure_query(...) -> ProcedureResult;
pub fn execute_procedure_union(...) -> ProcedureResult;
pub fn execute_procedure_union_with_return(...) -> ProcedureResult;
pub fn extract_procedure_names_from_union(query: &str) -> Result<Vec<String>, String>;

// RETURN evaluation (re-exported from mod.rs)
pub fn apply_return_clause(results, return_clause) -> ProcedureResult;

// Registry
pub struct ProcedureRegistry { ... }
impl ProcedureRegistry {
    pub fn new() -> Self;        // creates with all built-ins
    pub fn get(&self, name: &str) -> Option<&ProcedureFn>;
    pub fn contains(&self, name: &str) -> bool;
    pub fn names(&self) -> Vec<&str>;
}
```

## Testing Guidance

- Each `db_*.rs` has unit tests with empty schema (format validation)
- `executor.rs` has tests for statement detection (procedure-only, union, mixed)
- `return_evaluator.rs` has tests for COLLECT, map+COLLECT, variable passthrough
- Run with: `cargo test --lib procedures`
- Integration testing: requires running server with loaded schema, use curl/Bolt client

## When to Modify

- **Adding a new procedure**: Create `new_procedure.rs`, register in `ProcedureRegistry::new()`
- **YIELD clause filtering**: Currently not implemented (TODO in executor.rs)
- **New aggregation functions**: Add to `has_aggregation_in_expr()` match and `evaluate_aggregation_expr()`
- **Schema changes**: If `GraphSchema` API changes, update all `db_*.rs` implementations
