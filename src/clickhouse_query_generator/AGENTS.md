# clickhouse_query_generator Module — Agent Guide

> **Purpose**: Converts `RenderPlan` → ClickHouse SQL string.
> Contains VLP (variable-length path) CTE generation — the most schema-sensitive code.

## Module Architecture

```
RenderPlan (from render_plan)
    │
    ▼
to_sql_query.rs (3.2K)       ← Main SQL renderer: SELECT/FROM/JOIN/WHERE/GROUP BY/ORDER BY
    │                            Also: VLP alias rewriting, denormalized ORDER BY resolution
    │
    ├─ variable_length_cte.rs (3.4K) ← Recursive CTE generator for *1..N path patterns
    │                                   4 base-case generators × 5 schema variations = complexity
    │
    ├─ multi_type_vlp_joins.rs (1.3K) ← UNION ALL of explicit JOINs for multi-type traversals
    │                                    Used when path crosses node types (User→Post via LIKES)
    │
    ├─ function_translator.rs (952)   ← Cypher→ClickHouse function mapping
    ├─ function_registry.rs (1.2K)    ← Function signatures & type info
    ├─ json_builder.rs (331)          ← formatRowNoNewline JSON blob generation
    ├─ pagerank.rs (387)              ← PageRank SQL generation
    └─ mod.rs (50)                    ← Entry point: generate_sql()
```

## variable_length_cte.rs — The Core

### What It Does
Generates `WITH RECURSIVE` CTEs for Cypher patterns like:
```cypher
MATCH (a:User)-[:FOLLOWS*1..3]->(b:User)
MATCH path = (a)--(o)           -- browser expand (1-hop, all types)
```

### The Generator Struct
```rust
VariableLengthCteGenerator {
    schema: &GraphSchema,
    start_node_alias, end_node_alias,     // Cypher aliases
    rel_type, start_label, end_label,     // Type info
    min_hops, max_hops,                   // Range bounds
    is_fk_edge: bool,                     // FK column = edge
    start_is_denormalized: bool,          // Start node in edge table
    end_is_denormalized: bool,            // End node in edge table
    type_column: Option<String>,          // Polymorphic discriminator
    shortest_path_mode: Option<...>,      // shortestPath optimization
    // ... more fields
}
```

### 5 Schema Variations × 2 Cases = 10 Code Paths

| Variation | Base Case | Recursive Case |
|-----------|-----------|----------------|
| **Standard** | 3-way JOIN (start→edge→end) | Recursive JOIN on prev end_id |
| **FK-edge** | 2-way JOIN (node→FK target) | Recursive on FK column |
| **Denormalized** | Single-table scan | Recursive single-table |
| **Mixed denorm** | Hybrid JOIN | Hybrid recursive |
| **Polymorphic** | Standard + WHERE type_column = 'X' | Recursive + type filter |

### Key Functions

```
generate_cte()
  └─ generate_recursive_sql()
       ├─ generate_heterogeneous_polymorphic_sql()  // 2-CTE approach
       └─ standard path:
            ├─ generate_base_case()                 // First hop
            └─ generate_recursive_case_with_cte_name()  // Subsequent hops
```

### Critical Branching Points

```rust
// These booleans control EVERYTHING:
if self.is_fk_edge {
    // No separate edge table — FK column on node table
    // JOIN: start_table.fk_col = end_table.id
}
if self.start_is_denormalized {
    // Start node properties come from edge table, not node table
    // SELECT: edge.start_col AS start_prop (not node.col)
}
if self.type_column.is_some() {
    // Polymorphic: add WHERE type_column = 'REL_TYPE'
    // Critical: must appear in BOTH base AND recursive case
}
if self.is_heterogeneous_polymorphic_path() {
    // Intermediate hops use different type than final hop
    // Generates TWO CTEs instead of one recursive CTE
}
```

## multi_type_vlp_joins.rs — Browser Expand

### What It Does
When browser sends `MATCH path = (a)--(o)` (undirected, all types), generates:
```sql
SELECT ... FROM users a JOIN user_follows r ON ... JOIN users u2 ON ...
UNION ALL
SELECT ... FROM users a JOIN post_likes r ON ... JOIN posts p2 ON ...
UNION ALL
SELECT ... FROM users a JOIN posts p2 ON a.user_id = p2.user_id  -- FK-edge
```

### Key Function
```
generate_cte_sql(cte_name)
  └─ for each path in enumerate_vlp_paths():
       generate_path_branch_sql(path, idx)
         └─ generate_select_items(node_type, mode)
```

### PropertySelectionMode
```rust
enum PropertySelectionMode {
    IdOnly,      // Just start_id, end_id
    Individual,  // Named columns per type
    WholeNode,   // JSON blob (formatRowNoNewline)
}
```
Browser expand uses `WholeNode` for heterogeneous end nodes (User vs Post).

## to_sql_query.rs — VLP Rewriting

### VLP Alias Rewriting
After CTEs are generated, SELECT items reference Cypher aliases (`a.name`, `o.name`).
These must be rewritten to CTE columns (`t.start_name`, `t.end_properties`).

**Critical detection**:
```rust
// Standard VLP: FROM is the VLP CTE
if from_ref.name.starts_with("vlp_") { ... }

// OPTIONAL VLP: FROM is anchor table, VLP is LEFT JOINed
// Must NOT rewrite anchor node properties!

// WITH+VLP: FROM is VLP CTE, WITH CTE is JOINed
// Must rewrite JOIN column to WITH CTE's actual ID column
```

## Common Bug Patterns

| Pattern | Symptom | Where |
|---------|---------|-------|
| Type filter missing in recursive case | Traverses wrong relationship types | `generate_recursive_case`: polymorphic WHERE |
| FK-edge self-JOIN | Redundant JOIN on same table | `generate_base_case`: `is_fk_edge` + same table |
| Wrong property source | Column not found | `start_is_denormalized` vs node table |
| Heterogeneous path filter loss | Wrong intermediate nodes | `generate_heterogeneous_polymorphic_sql` |
| JSON vs individual columns | Mismatched SELECT in UNION ALL | `PropertySelectionMode` mismatch across branches |
| VLP rewriting on WITH CTE | Overwrites WITH CTE columns | `rewrite_vlp_select_aliases` not checking FROM type |

## Testing After Changes

```bash
# VLP-specific tests:
cargo test variable_length    # VLP unit tests
cargo test multi_type_vlp     # Multi-type VLP tests
cargo test test_vlp_with_cte  # VLP+WITH regression

# Manual: test the browser expand query
curl -X POST localhost:8080/query -H "Content-Type: application/json" \
  -d '{"query": "MATCH (a:User) WHERE a.user_id = 1 WITH a, size([(a)--() | 1]) AS c MATCH path = (a)--(o) RETURN path, c LIMIT 10", "sql_only": true}'

# Check: no "a_start_id", must have "a_user_id" in JOIN condition
```

## Schema Variation Checklist

When modifying VLP generation, verify SQL output for:
- [ ] Standard: `MATCH (a:User)-[:FOLLOWS*1..3]->(b:User)`
- [ ] FK-edge: `MATCH (o:Order)-[:PLACED_BY]->(c:Customer)`
- [ ] Denormalized: `MATCH (a:Airport)-[:FLIGHT*1..2]->(b:Airport)`
- [ ] Polymorphic: `MATCH (u:User)-[:FOLLOWS]->(f:User)` on `social_polymorphic` schema
- [ ] Multi-type expand: `MATCH (a:User)--(o)` (browser pattern)
- [ ] Undirected: `MATCH (a:User)-[r]-(b:User)` (UNION ALL both directions)
