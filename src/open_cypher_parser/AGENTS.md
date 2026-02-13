# open_cypher_parser Module — Agent Guide

> **Purpose**: Parses OpenCypher query strings into a typed AST (`OpenCypherQueryAst`).
> This is the first stage of the query pipeline — all downstream modules depend on correct parsing.
> Uses the [nom](https://docs.rs/nom) parser combinator library. Zero-copy: AST nodes borrow from the input `&str`.

## Module Architecture

```
Cypher query string (&str)
    │
    ├─ common::strip_comments()     ← Pre-processing: remove --, //, /* */ comments
    │                                  (respects string literals and relationship patterns like -->)
    │
    ▼
mod.rs::parse_cypher_statement()    ← Top-level entry point
    │
    ├─ Standalone CALL?  → standalone_procedure_call.rs → CypherStatement::ProcedureCall
    │
    └─ Regular query     → parse_query_with_nom()       → CypherStatement::Query
         │
         ├─ use_clause.rs          → UseClause
         ├─ match_clause.rs        → MatchClause  (with path_pattern.rs + where_clause.rs)
         ├─ optional_match_clause  → OptionalMatchClause
         ├─ where_clause.rs        → WhereClause (standalone, after MATCH clauses)
         ├─ call_clause.rs         → CallClause (CALL ... YIELD)
         ├─ unwind_clause.rs       → UnwindClause
         ├─ with_clause.rs         → WithClause (recursive: can nest MATCH, WITH, UNWIND)
         ├─ create_clause.rs       → CreateClause  ← parsed but out of scope (read-only engine)
         ├─ set_clause.rs          → SetClause     ← parsed but out of scope
         ├─ remove_clause.rs       → RemoveClause  ← parsed but out of scope
         ├─ delete_clause.rs       → DeleteClause  ← parsed but out of scope
         ├─ return_clause.rs       → ReturnClause
         └─ order_by_and_page.rs   → ORDER BY + SKIP + LIMIT (unified)
              ├─ order_by_clause.rs
              ├─ skip_clause.rs
              └─ limit_clause.rs

Shared infrastructure:
  ├─ ast.rs              ← All AST types (Expression, PathPattern, NodePattern, etc.)
  ├─ expression.rs       ← Expression parser: operators, functions, literals, comprehensions
  ├─ path_pattern.rs     ← Graph pattern parser: nodes, relationships, shortest path
  ├─ common.rs           ← ws(), identifiers, string parsing, comment stripping
  └─ errors.rs           ← OpenCypherParsingError (nom-compatible)
```

## Key Files with Line Counts

| File | Lines | Responsibility |
|------|------:|----------------|
| mod.rs | 1,734 | Top-level parsers (`parse_cypher_statement`, `parse_query_with_nom`, `parse_query`), UNION support, extensive integration tests |
| ast.rs | 706 | All AST type definitions — the contract between parser and downstream modules |
| expression.rs | 1,596 | Expression parsing: precedence climbing, operators, functions, CASE, EXISTS, reduce, pattern comprehension, lambda, map/list literals, property access, label expressions |
| path_pattern.rs | 1,537 | Node/relationship/connected pattern parsing, variable-length specs (`*1..3`), `shortestPath()`/`allShortestPaths()`, multi-label support, inline properties, depth limit (50 hops) |
| common.rs | 481 | `strip_comments()` (comment removal respecting strings/relationship patterns), `ws()` combinator, identifier parsers |
| return_clause.rs | 403 | RETURN item parsing with `original_text` capture for Neo4j-compatible default aliases |
| with_clause.rs | 332 | WITH clause with ORDER BY/SKIP/LIMIT/WHERE, recursive nesting (subsequent MATCH, UNWIND, WITH) |
| match_clause.rs | 282 | MATCH clause parsing with optional path variable (`p = ...`), WHERE per MATCH |
| where_clause.rs | 257 | WHERE clause → delegates to `expression::parse_expression` |
| order_by_clause.rs | 211 | ORDER BY items with ASC/DESC |
| set_clause.rs | 179 | SET parsing (out of scope for runtime) |
| optional_match_clause.rs | 177 | OPTIONAL MATCH with two-word keyword parsing |
| order_by_and_page_clause.rs | 156 | Unified ORDER BY + SKIP + LIMIT per OpenCypher spec order |
| standalone_procedure_call.rs | 156 | `CALL db.labels()`, `CALL dbms.components()` with YIELD |
| remove_clause.rs | 156 | REMOVE parsing (out of scope for runtime) |
| delete_clause.rs | 150 | DELETE / DETACH DELETE (out of scope for runtime) |
| unwind_clause.rs | 136 | UNWIND expression AS alias |
| skip_clause.rs | 132 | SKIP integer |
| create_clause.rs | 129 | CREATE parsing (out of scope for runtime) |
| limit_clause.rs | 127 | LIMIT integer |
| call_clause.rs | 125 | In-query CALL with named arguments (`key: value` or `key => value`) |
| use_clause.rs | 107 | USE database_name (schema selection) |
| errors.rs | 44 | `OpenCypherParsingError` struct implementing nom's `ParseError` and `ContextError` traits |
| **Total** | **9,308** | |

## Public API

### Entry Points (exposed via `mod.rs`)

```rust
// Primary entry point — handles regular queries, UNION, and standalone CALL
pub fn parse_cypher_statement(input: &str)
    -> IResult<&str, CypherStatement<'_>, OpenCypherParsingError<'_>>

// Legacy single-query parser (backward compatibility)
pub fn parse_statement(input: &str)
    -> IResult<&str, OpenCypherQueryAst<'_>, OpenCypherParsingError<'_>>

// Convenience wrapper that checks all input is consumed
pub fn parse_query(input: &str) -> Result<OpenCypherQueryAst<'_>, OpenCypherParsingError<'_>>

// Pre-processing: strip comments before parsing
pub fn strip_comments(input: &str) -> String
```

### Key Public Types (from `ast.rs`)

- `CypherStatement<'a>` — top-level: `Query { query, union_clauses }` or `ProcedureCall`
- `OpenCypherQueryAst<'a>` — the full query: match/optional match/with/where/return/order/skip/limit clauses
- `Expression<'a>` — recursive enum: `Literal`, `Variable`, `PropertyAccessExp`, `FunctionCallExp`, `OperatorApplicationExp`, `PathPattern`, `Case`, `ExistsExpression`, `ReduceExp`, `MapLiteral`, `LabelExpression`, `Lambda`, `PatternComprehension`, `List`, `ArraySubscript`, `ArraySlicing`, `Parameter`
- `PathPattern<'a>` — `Node`, `ConnectedPattern`, `ShortestPath`, `AllShortestPaths`
- `NodePattern<'a>` — name, labels (multi-label via `|`), inline properties
- `RelationshipPattern<'a>` — name, direction, labels (multi-type via `|`), properties, variable_length
- `VariableLengthSpec` — min/max hops with validation
- `ConnectedPattern<'a>` — uses `Rc<RefCell<NodePattern>>` for node sharing in chains

## AST Structure

```
CypherStatement
├── Query                          OR   ProcedureCall (standalone CALL)
│   ├── query: OpenCypherQueryAst
│   └── union_clauses: Vec<UnionClause>
│
OpenCypherQueryAst
├── use_clause: Option<UseClause>           "USE dbname"
├── match_clauses: Vec<MatchClause>         legacy separate list
├── optional_match_clauses: Vec<OptionalMatchClause>  legacy separate list
├── reading_clauses: Vec<ReadingClause>     unified, preserves order (preferred)
├── call_clause: Option<CallClause>         "CALL proc(args) YIELD ..."
├── unwind_clauses: Vec<UnwindClause>       "UNWIND expr AS alias"
├── with_clause: Option<WithClause>         "WITH items ORDER BY SKIP LIMIT WHERE"
│   └── (recursive sub-structure)           subsequent_match, subsequent_with, etc.
├── where_clause: Option<WhereClause>       standalone WHERE (after WITH)
├── create/set/remove/delete_clause         parsed but out of scope
├── return_clause: Option<ReturnClause>     "RETURN [DISTINCT] items"
├── order_by_clause: Option<OrderByClause>
├── skip_clause: Option<SkipClause>
└── limit_clause: Option<LimitClause>

Expression (recursive)
├── Literal(Integer|Float|Boolean|String|Null)
├── Variable(&str)
├── Parameter(&str)                         $param
├── PropertyAccessExp { base, key }         n.name
├── FunctionCallExp { name, args }          count(*), toUpper(n.name)
├── OperatorApplicationExp { op, operands } a + b, NOT x, x IS NULL
├── List(Vec<Expression>)                   [1, 2, 3]
├── MapLiteral(Vec<(key, Expression)>)      {days: 5}
├── Case { expr, when_then, else_expr }
├── ExistsExpression(ExistsSubquery)        EXISTS { (n)-[:REL]->() }
├── ReduceExp(ReduceExpression)             reduce(acc=0, x IN list | acc+x)
├── LabelExpression { variable, label }     n:Person
├── Lambda(LambdaExpression)                x -> x > 5
├── PatternComprehension                    [(a)-[:REL]->(b) | b.name]
├── PathPattern(PathPattern)                (a)-[]->(b) used in expressions
├── ArraySubscript { array, index }         list[0]
└── ArraySlicing { array, from, to }        list[1..5]
```

## Critical Invariants

### 1. Zero-Copy Parsing
All `&'a str` references in the AST borrow from the original input string.
**Never** modify or deallocate the input string while the AST is alive.
`FunctionCall.name` is the exception — it's `String` (owned) because dotted names like `ch.arrayFilter` are joined from parts.

### 2. Operator Precedence (expression.rs)
The expression parser implements standard precedence via recursive descent:
```
Lowest  → parse_logical_or      (OR)
          parse_logical_and     (AND)
          parse_not_expression  (NOT)
          parse_comparison      (= <> < > <= >= =~ IN STARTS WITH ENDS WITH CONTAINS)
          parse_additive        (+ -)
          parse_multiplicative  (* / %)
          parse_unary           (unary - , DISTINCT)
Highest → parse_postfix         (IS NULL, IS NOT NULL, [index], [from..to])
          parse_primary         (literals, variables, function calls, CASE, EXISTS, reduce, etc.)
```
**OR must not consume ORDER**: `parse_logical_or` uses `terminated(tag_no_case("OR"), not(peek(alphanumeric1)))` to avoid matching the "OR" in "ORDER".

### 3. ConnectedPattern Node Sharing
In multi-hop patterns like `(a)-[]->(b)-[]->(c)`, the intermediate node `(b)` is shared between consecutive `ConnectedPattern` entries via `Rc<RefCell<NodePattern>>`. The second pattern's `start_node` is `clone()` of the first pattern's `end_node`.

### 4. WITH Clause Recursive Nesting
`WithClause` can contain `subsequent_match`, `subsequent_unwind`, and `subsequent_with` (boxed). This represents chained patterns:
```cypher
-- This becomes nested WithClause structs:
MATCH (a) WITH a MATCH (a)-[]->(b) WITH a, b RETURN a, b
```

### 5. WHERE Clause Placement
WHERE can appear in multiple positions per OpenCypher spec:
- **Per MATCH**: `MatchClause.where_clause` — after that specific MATCH's patterns
- **After WITH**: `WithClause.where_clause` — filters WITH results
- **Standalone**: `OpenCypherQueryAst.where_clause` — after all MATCH clauses (overridden if WHERE after WITH exists)

### 6. Reading Clause Ordering
`reading_clauses: Vec<ReadingClause>` preserves interleaving order of MATCH and OPTIONAL MATCH.
When populated, this takes precedence over the legacy `match_clauses` / `optional_match_clauses` vectors (which are still populated for backward compatibility).

### 7. Comment Stripping Safety
`strip_comments()` must NOT strip:
- `-->`, `<--`, `--(`, `--[` — these are Cypher relationship patterns, not comments
- Content inside single quotes `'`, double quotes `"`, or backticks `` ` `` — these are string literals/identifiers

### 8. Relationship Chain Depth Limit
`MAX_RELATIONSHIP_CHAIN_DEPTH = 50` in path_pattern.rs. Chains longer than this cause a `TooLarge` parse error. This prevents stack overflow on adversarial inputs.

### 9. Binary Operator Keywords as Variables
`is_binary_operator_keyword()` blocks `AND`, `OR`, `XOR` from being parsed as variable names at expression start. This prevents `WHERE AND x = 1` from treating `AND` as a variable. `NOT` is intentionally not blocked (it's a valid unary prefix operator).

## Common Bug Patterns

| Pattern | Symptom | Root Cause |
|---------|---------|------------|
| WHERE consumes too much | RETURN clause not found | Expression parser doesn't stop at clause boundaries; `parse_expression` must naturally terminate before keywords |
| OR matches ORDER | "ORDER BY" parsed as "OR DER BY..." | Missing `not(peek(alphanumeric1))` guard after "OR" |
| `--` parsed as comment | Relationship pattern `(a)--(b)` disappears | `strip_comments` didn't check for `(` or `[` after `--` |
| Pattern comprehension vs list | `[(a)--() | 1]` fails to parse or parsed as list | Pattern comprehension must be tried before list literal in `alt()` chain |
| AS keyword consumed by expression | `RETURN x AS alias` — `AS` treated as variable | Expression parser must stop at clause boundary keywords; keyword sensitivity is managed by what the callers (return_clause, with_clause) handle |
| STARTS WITH/ENDS WITH | Multi-word operators misparsed | Must be parsed before single-word `IN` in the `alt()` chain |
| NOT IN vs IN | `NOT IN` parsed as NOT + IN separately | `NOT IN` must appear before `IN` in operator alternatives |
| Identifiers starting with keywords | `ORDER_date` parsed as ORDER keyword | Identifiers use `take_while1(is_identifier_char)` which includes `_`, so `ORDER_date` is consumed as one token, but `ORDER ` triggers the keyword |

## Dependencies

### External Crates
- **nom** (parser combinators) — core parsing machinery (`IResult`, `tag`, `alt`, `many0`, etc.)
- **serde** — `Serialize`/`Deserialize` on `Direction` (for serialization in plans)

### Internal: What This Module Depends On
- `crate::debug_print!` macro — used in `VariableLengthSpec::validate()` for warnings

### Internal: What Depends On This Module

| Consumer Module | What It Uses |
|----------------|--------------|
| `server/handlers.rs` | `parse_cypher_statement()`, `strip_comments()` — HTTP query entry point |
| `server/bolt_protocol/handler.rs` | `parse_cypher_statement()` — Bolt protocol entry point |
| `query_planner/ast_transform/` | AST types for query rewriting |
| `query_planner/logical_plan/` | AST types for logical plan building (MatchClause, WithClause, ReturnClause, Expression, etc.) |
| `query_planner/logical_expr/` | `Expression`, `Operator`, `Literal` for logical expression modeling |
| `query_planner/analyzer/` | `Expression`, `WhereClause` for property extraction, match type inference |
| `query_planner/optimizer/` | AST types for union pruning, filter pushdown |
| `render_plan/` | `Direction` for SQL generation |
| `procedures/executor.rs` | `CypherStatement`, `StandaloneProcedureCall` for procedure dispatch |
| `procedures/return_evaluator.rs` | `Expression`, `ReturnClause` for evaluating RETURN on procedure results |

## Testing Guidance

### Running Tests
```bash
# All parser unit tests (~150+ tests across all files)
cargo test --lib open_cypher_parser

# Specific file's tests
cargo test --lib open_cypher_parser::expression::tests
cargo test --lib open_cypher_parser::path_pattern::tests
cargo test --lib open_cypher_parser::mod::tests

# Full query integration tests (in mod.rs)
cargo test --lib test_parse_full_read_query
cargo test --lib test_parse_full_query
cargo test --lib test_parse_cypher_statement_union

# All tests (includes downstream consumers that will break if parser changes)
cargo test
```

### What to Test After Changes

- **Changed expression.rs**: Run all expression tests + `test_parse_full_read_query` + `test_parse_where_*` tests. Also run downstream: `cargo test --lib query_planner` and `cargo test --lib render_plan`.
- **Changed path_pattern.rs**: Run path pattern tests + `test_parse_match_clause_*` + shortest path tests.
- **Changed a clause parser** (return/with/match/etc.): Run that file's tests + `mod.rs` integration tests.
- **Changed ast.rs**: Run the full `cargo test` — every downstream module uses these types.
- **Changed common.rs**: Run `test_strip_comments` + full parser suite (comment stripping affects everything).

### Test Structure
Each clause file has its own `#[cfg(test)] mod tests` with:
1. Happy path tests (simple input)
2. Edge cases (whitespace, case sensitivity)
3. Error cases (missing keywords, invalid input)
4. Integration with related clauses (e.g., MATCH with WHERE)

`mod.rs` has comprehensive integration tests parsing complete multi-clause queries and asserting the full AST structure.

## Design Decisions & Gotchas

### Why `Rc<RefCell<NodePattern>>` in ConnectedPattern?
Node sharing in multi-hop paths. In `(a)-[]->(b)-[]->(c)`, node `b` appears as `end_node` of the first pattern and `start_node` of the second. Using `Rc<RefCell<>>` avoids cloning while allowing the same node to be referenced from both positions.

### Why are CREATE/SET/DELETE/REMOVE parsed?
Historical: the parser was originally part of a full Cypher engine. These clauses are parsed into AST nodes but never executed — ClickGraph is read-only. They remain for grammar completeness and potential future use.

### Why two representations for reading clauses?
`reading_clauses: Vec<ReadingClause>` preserves the exact order of MATCH and OPTIONAL MATCH.
`match_clauses` and `optional_match_clauses` are separate vectors for backward compatibility with older code that processes them independently. New code should use `reading_clauses`.

### Why `FunctionCall.name` is `String` not `&str`?
Dotted function names like `ch.arrayFilter` are parsed as separate identifier parts joined by `.`. The joined result is an owned `String` because it doesn't correspond to any contiguous slice in the original input.

### Temporal Accessor Desugaring
`$param.year` or `n.date.month` are desugared into `FunctionCall` nodes during parsing:
`n.registration_date.year` → `FunctionCallExp { name: "year", args: [PropertyAccess(n, registration_date)] }`
This simplifies downstream handling since temporal accessors behave like function calls.
