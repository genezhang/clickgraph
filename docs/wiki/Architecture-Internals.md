# Architecture & Extension Development

Deep dive into ClickGraph's internal architecture and guide for extending functionality.

## Table of Contents
- [Architecture Overview](#architecture-overview)
- [Core Components](#core-components)
- [Query Execution Pipeline](#query-execution-pipeline)
- [Extension Points](#extension-points)
- [Custom Functions](#custom-functions)
- [Protocol Extensions](#protocol-extensions)
- [Contributing Guide](#contributing-guide)

---

## Architecture Overview

### High-Level Architecture

```mermaid
flowchart TD
    subgraph Clients
        A1[Neo4j Browser]
        A2[Python Client]
        A3[JavaScript Client]
    end
    
    subgraph "ClickGraph Server"
        B1[HTTP API<br/>Port 8080]
        B2[Bolt Protocol<br/>Port 7687]
        C[Request Router]
        D[Cypher Parser]
        E[Binder + Lowering]
        F[SQL Printer]
        G[ClickHouse Client]
    end
    
    subgraph "ClickHouse Database"
        H[MergeTree Tables]
        I[Query Engine]
    end
    
    A1 & A2 & A3 -->|HTTP REST| B1
    A1 -->|Bolt Protocol| B2
    B1 & B2 --> C
    C --> D
    D -->|AST| E
    E -->|Render plan| F
    F -->|SQL| G
    G -->|Query| H
    H --> I
    I -->|Results| G
    G -->|Transform| C
    C -->|JSON/PackStream| Clients
    
    style B1 fill:#e1f5ff
    style B2 fill:#e1f5ff
    style E fill:#fff4e1
    style H fill:#e8f5e9
```

### Design Principles

1. **Stateless Architecture**: No query state stored in ClickGraph (scales horizontally)
2. **View-Based Model**: Map existing tables to graphs (non-invasive)
3. **Explicit Scope**: Each clause's names are bound once, then lowered clause by clause to SQL
4. **Protocol Agnostic**: Support HTTP REST and Neo4j Bolt protocol
5. **Rust Performance**: Zero-cost abstractions, memory safety, concurrency

---

## Core Components

### 1. open_cypher_parser/

**Purpose**: Parse Cypher queries into Abstract Syntax Tree (AST).

**Key Files**:
- `ast.rs` - AST node definitions (MatchClause, WhereClause, ReturnClause, etc.)
- `mod.rs` - Parser entry point using Pest grammar
- `cypher.pest` - Cypher grammar specification

**Example AST**:
```rust
// Input: MATCH (u:User {user_id: 1})-[:FOLLOWS]->(f:User) RETURN f.name

QueryAst {
    match_clause: MatchClause {
        patterns: vec![
            Pattern {
                nodes: vec![
                    NodePattern {
                        variable: Some("u"),
                        labels: vec!["User"],
                        properties: PropertyMap { user_id: 1 }
                    },
                    NodePattern {
                        variable: Some("f"),
                        labels: vec!["User"],
                        properties: PropertyMap::empty()
                    }
                ],
                relationships: vec![
                    RelationshipPattern {
                        direction: Outgoing,
                        types: vec!["FOLLOWS"],
                        properties: PropertyMap::empty()
                    }
                ]
            }
        ]
    },
    return_clause: ReturnClause {
        items: vec![
            ReturnItem::Property("f", "name")
        ]
    }
}
```

### 2. bound_plan/

**Purpose**: Bind the parsed clauses once and lower them clause by clause to the SQL IR (`docs/design/EXPLICIT_SCOPE.md`).

**Key Files**:
- `binder.rs`, `expr.rs` - Names → bindings, scopes (WITH, UNION, subqueries), expressions
- `labels.rs` - Label inference over the schema
- `lower/` - Bound statement → `RenderPlan` (CTEs, joins, SELECT); dialect spellings via `FunctionMapper`
- `lower/path.rs` - Variable-length walks (the generator in `sql_generator/emitters/clickhouse/variable_length_cte.rs`)
- `write.rs` - Embedded writes → `WriteRenderPlan`

`translate.rs` is the one read translation every entry point (HTTP, Bolt, embedded, `cg`) calls.

### 3. render_plan/ and the SQL printer

**Purpose**: `render_plan/` is the SQL IR the lowering builds; `sql_generator/emitters/clickhouse/to_sql_query.rs` prints it as ClickHouse or Databricks SQL.

**Example SQL** (`MATCH (u:User)-[:FOLLOWS]->(:User)-[:FOLLOWS]->(f:User) WHERE u.user_id = 1 RETURN f.name`, illustrative):
```sql
SELECT t2.full_name AS name
FROM users_bench t0
JOIN user_follows_bench r0 ON t0.user_id = r0.follower_id
JOIN users_bench t1 ON r0.followed_id = t1.user_id
JOIN user_follows_bench r1 ON t1.user_id = r1.follower_id
JOIN users_bench t2 ON r1.followed_id = t2.user_id
WHERE t0.user_id = 1;
```

**Variable-length paths** become a recursive CTE of the walks (start, end, hops), joined back to the end node's table.

### 4. graph_catalog/

**Purpose**: Manage graph schema metadata.

**Key Files**:
- `graph_schema.rs` - Schema data structures
- `schema_loader.rs` - Load YAML schemas
- `schema_validator.rs` - Validate schema correctness

**Schema Data Structures**:
```rust
pub struct GraphSchema {
    pub name: String,
    pub databases: Vec<Database>,
    pub views: Vec<View>,
}

pub struct View {
    pub name: String,
    pub view_parameters: Vec<String>,  // e.g., ["tenant_id"]
    pub nodes: HashMap<String, NodeDefinition>,
    pub relationships: HashMap<String, RelationshipDefinition>,
}

pub struct NodeDefinition {
    pub source_table: String,
    pub identifier_property: String,
    pub property_mappings: HashMap<String, String>,
    pub filters: Vec<String>,  // e.g., ["tenant_id = ${tenant_id}"]
}

pub struct RelationshipDefinition {
    pub source_table: String,
    pub from_node: String,
    pub to_node: String,
    pub from_property: String,
    pub to_property: String,
    pub property_mappings: HashMap<String, String>,
    pub filters: Vec<String>,
}
```

### 5. server/

**Purpose**: HTTP and Bolt protocol servers.

**Key Files**:
- `handlers.rs` - HTTP REST API handlers
- `bolt_protocol/` - Neo4j Bolt protocol implementation
  - `connection.rs` - Connection state management
  - `messages.rs` - Bolt message types
  - `auth.rs` - Authentication handlers
- `clickhouse_client.rs` - ClickHouse connection pooling
- `../packstream/` - PackStream encoding/decoding (vendored)

**HTTP API**:
```rust
// POST /query
pub async fn execute_query(
    State(app_state): State<AppState>,
    Json(request): Json<QueryRequest>
) -> Result<Json<QueryResponse>, StatusCode> {
    // 1. Parse Cypher
    let ast = parse_cypher(&request.query)?;
    
    // 2. Bind, lower and print SQL (translate.rs, the one read seam)
    let sql = translate_read(ast, &schema, options)?.sql;
    
    // 4. Execute on ClickHouse
    let rows = app_state.clickhouse_client.query(&sql).await?;
    
    // 5. Transform results
    let results = transform_results(rows)?;
    
    Ok(Json(QueryResponse { results }))
}
```

---

## Query Execution Pipeline

### Step-by-Step Execution

**Example Query**: `MATCH (u:User {user_id: 1})-[:FOLLOWS]->(f:User) RETURN f.name LIMIT 10`

**1. HTTP Request Received**
```json
POST /query
{
  "query": "MATCH (u:User {user_id: 1})-[:FOLLOWS]->(f:User) RETURN f.name LIMIT 10",
  "schema_name": "social_graph"
}
```

**2. Schema Resolution**
```rust
// Load schema "social_graph"
let schema = GLOBAL_SCHEMAS.get("social_graph")?;
```

**3. Cypher Parsing**
```rust
// Parse Cypher → AST
let ast = CypherParser::parse(&query)?;

// AST structure:
QueryAst {
    match_clause: MatchClause {
        patterns: [
            Pattern {
                nodes: [
                    Node { var: "u", label: "User", props: {user_id: 1} },
                    Node { var: "f", label: "User", props: {} }
                ],
                rels: [
                    Rel { type: "FOLLOWS", direction: Outgoing }
                ]
            }
        ]
    },
    return_clause: ReturnClause { items: ["f.name"] },
    limit: Some(10)
}
```

**4. Schema Lookup**

The schema maps `User` to its table (`users_bench`) and properties to columns (`name` → `full_name`), and `FOLLOWS` to its table (`user_follows_bench`) and endpoint columns (`follower_id` → `followed_id`).

**5. Binding and Lowering**

The binder resolves `u`, `f` and the relationship to bindings and their labels; the lowering builds the render plan: `users_bench AS t0` filtered on `user_id = 1`, joined through `user_follows_bench` to `users_bench AS t1`, projecting `t1.full_name AS name` with `LIMIT 10`.

**6. SQL Printing**
```sql
-- Generated ClickHouse SQL
SELECT t1.full_name AS name
FROM users_bench t0
JOIN user_follows_bench r0 ON t0.user_id = r0.follower_id
JOIN users_bench t1 ON r0.followed_id = t1.user_id
WHERE t0.user_id = 1
LIMIT 10;
```

**7. ClickHouse Execution**
```rust
let rows = clickhouse_client.query(&sql).await?;
// → [[Row { name: "Alice" }], [Row { name: "Bob" }], ...]
```

**8. Result Transformation**
```json
{
  "results": [
    {"name": "Alice"},
    {"name": "Bob"},
    ...
  ],
  "stats": {
    "rows_returned": 10,
    "execution_time_ms": 42
  }
}
```

---

## Extension Points

### 1. Lowering a New Construct

A new Cypher construct is bound in `bound_plan/binder.rs` / `expr.rs` (refuse what is not supported with `BindError::Unsupported`) and lowered in `bound_plan/lower/`; dialect-specific SQL goes through `FunctionMapper`, schema layouts through `PatternSchemaContext`. Add the query to the corpus (`tests/corpus/`) so its SQL is locked by a golden.

### 2. Custom Cypher Functions

**Add Function to AST**:
```rust
// src/open_cypher_parser/ast.rs

#[derive(Debug, Clone)]
pub enum Expression {
    // ... existing variants
    CustomFunction { name: String, args: Vec<Expression> },
}
```

**Implement Function Logic**:
```rust
// src/sql_generator/emitters/clickhouse/function_registry.rs (illustrative)

pub fn generate_custom_function(
    name: &str,
    args: &[Expression]
) -> Result<String, Error> {
    match name {
        "my_custom_func" => {
            let arg_sql: Vec<String> = args.iter()
                .map(|arg| generate_expression(arg))
                .collect::<Result<_, _>>()?;
            Ok(format!("myClickHouseUDF({})", arg_sql.join(", ")))
        }
        _ => Err(Error::UnknownFunction(name.to_string()))
    }
}
```

**Usage**:
```cypher
MATCH (u:User)
RETURN u.name, my_custom_func(u.age, u.score)
```

### 3. Custom Graph Algorithms

**Implement Algorithm**: graph algorithms are procedures that generate their SQL directly, as PageRank does (`sql_generator/emitters/clickhouse/pagerank.rs`, its CALL arguments read by `query_planner::evaluate_call_query`); other procedures live in `src/procedures/`.

**Register as Cypher Procedure**:
```cypher
CALL my_algorithm(start_node, {param1: value1, param2: value2})
YIELD result
RETURN result
```

---

## Custom Functions

### Example: Levenshtein Distance

**1. Add to AST**:
```rust
// ast.rs
Expression::Function {
    name: "levenshtein".to_string(),
    args: vec![str1, str2]
}
```

**2. Generate ClickHouse SQL**:
```rust
// function_registry.rs (illustrative)
"levenshtein" => {
    format!("editDistance({}, {})", 
        generate_expression(&args[0])?,
        generate_expression(&args[1])?)
}
```

**3. Use in Queries**:
```cypher
MATCH (u1:User), (u2:User)
WHERE levenshtein(u1.name, u2.name) < 3
RETURN u1.name, u2.name
```

---

## Protocol Extensions

### Custom Bolt Message Types

**1. Define Message**:
```rust
// server/bolt_protocol/messages.rs

#[derive(Debug)]
pub enum CustomMessage {
    GetStats,
    CacheStatus,
}

impl BoltMessage for CustomMessage {
    fn encode(&self) -> Vec<u8> {
        // PackStream encoding
        match self {
            CustomMessage::GetStats => vec![0xB1, 0x70],  // Structure: 1 field, tag 0x70
            CustomMessage::CacheStatus => vec![0xB1, 0x71],
        }
    }
}
```

**2. Handle Message**:
```rust
// server/bolt_protocol/handler.rs

async fn handle_custom_message(
    msg: CustomMessage,
    state: &mut ConnectionState
) -> Result<Response, Error> {
    match msg {
        CustomMessage::GetStats => {
            let stats = get_query_stats();
            Ok(Response::Success { metadata: stats })
        }
        CustomMessage::CacheStatus => {
            let cache_info = get_cache_info();
            Ok(Response::Success { metadata: cache_info })
        }
    }
}
```

---

## Contributing Guide

### Development Setup

```bash
# 1. Clone repository
git clone https://github.com/yourusername/clickgraph.git
cd clickgraph

# 2. Install Rust toolchain
rustup install stable
rustup default stable

# 3. Install development tools
cargo install cargo-watch
cargo install cargo-audit

# 4. Start dependencies
docker-compose up -d clickhouse

# 5. Build project
cargo build

# 6. Run tests
cargo test --all

# 7. Run in development mode
cargo watch -x run
```

### Code Style

**Format Code**:
```bash
cargo fmt
```

**Lint Code**:
```bash
cargo clippy -- -D warnings
```

### Testing Guidelines

**Unit Tests**:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_match_clause() {
        let query = "MATCH (u:User) RETURN u.name";
        let ast = parse_cypher(query).unwrap();
        
        assert_eq!(ast.match_clause.patterns.len(), 1);
        assert_eq!(ast.return_clause.items.len(), 1);
    }
}
```

**Integration Tests**:
```rust
// tests/integration/test_query_execution.rs

#[tokio::test]
async fn test_simple_query() {
    let client = ClickGraphClient::new("http://localhost:8080");
    
    let result = client.query("MATCH (u:User) RETURN count(u)").await.unwrap();
    
    assert!(result.rows.len() > 0);
}
```

### Pull Request Process

1. **Create Feature Branch**:
   ```bash
   git checkout -b feature/my-feature
   ```

2. **Make Changes**:
   - Write tests first (TDD)
   - Implement feature
   - Update documentation

3. **Commit**:
   ```bash
   git add .
   git commit -m "feat: Add custom function support"
   ```

4. **Push & Create PR**:
   ```bash
   git push origin feature/my-feature
   ```

5. **PR Checklist**:
   - [ ] All tests passing
   - [ ] Code formatted (`cargo fmt`)
   - [ ] No clippy warnings
   - [ ] Documentation updated
   - [ ] CHANGELOG.md updated

---

## Architecture Decision Records

### ADR-001: Stateless Architecture

**Decision**: ClickGraph is stateless - no query state stored between requests.

**Rationale**:
- Enables horizontal scaling (run N instances)
- Simplifies deployment (no session management)
- Fault tolerance (restart without losing state)

**Consequences**:
- ✅ Easy to scale
- ✅ No distributed state management
- ❌ No transaction support across queries
- ❌ No cached results between requests

### ADR-002: View-Based Model

**Decision**: Map existing tables to graph via YAML schemas (no special graph tables).

**Rationale**:
- Non-invasive (works with existing ClickHouse databases)
- Flexible (multiple views of same data)
- Leverages ClickHouse performance

**Consequences**:
- ✅ No schema migration needed
- ✅ Works with existing data
- ❌ No graph-specific optimizations (e.g., adjacency lists)
- ❌ Relationship lookups require JOINs

### ADR-003: Rust Implementation

**Decision**: Implement in Rust (not Python, Go, or Java).

**Rationale**:
- Memory safety without garbage collection
- Zero-cost abstractions
- Excellent async support (Tokio)
- Fast compilation and runtime performance

**Consequences**:
- ✅ High performance
- ✅ Low memory footprint
- ❌ Steeper learning curve for contributors
- ❌ Smaller ecosystem than Java/Python

---

## Next Steps

Now that you understand ClickGraph's architecture:

- **[Contributing Guidelines](Contributing.md)** - Start contributing
- **[Testing Guide](Testing-Guide.md)** - Write tests
- **[Performance Optimization](Performance-Query-Optimization.md)** - Optimize queries
- **[Custom Functions Guide](Custom-Functions.md)** - Extend functionality

---

[← Back: Multi-Tenancy](Multi-Tenancy-RBAC.md) | [Home](Home.md) | [Next: Contributing →](Contributing.md)
