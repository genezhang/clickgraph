# clickgraph-tck

openCypher [Technology Compatibility Kit (TCK)](https://github.com/opencypher/openCypher/tree/master/tck) runner for ClickGraph, using the embedded chdb engine.

## Current status

**383 / 402 scenarios passing (95.3%)** — 19 skipped (`@NegativeTests` / `@skip`), 0 failing.

## What it tests

The TCK is the openCypher project's official conformance suite. Each scenario is a Gherkin `.feature` file that specifies a graph setup, a Cypher query, and expected results. This crate runs a subset of those scenarios against ClickGraph's embedded query engine.

**Current coverage** — 402 scenarios across 20 feature files:

| Category | Feature files | Scenarios |
|----------|--------------|-----------|
| `MATCH` / `OPTIONAL MATCH` | Match1–3, MatchWhere1–2 | 66 |
| `RETURN` / `ORDER BY` / `SKIP`+`LIMIT` | Return1–3, ReturnOrderBy1, ReturnSkipLimit1 | 46 |
| `WITH` | With1–3 | 9 |
| Aggregation (`count`, `min`, `max`) | Aggregation1–2 | 14 |
| Boolean expressions | Boolean1 | 7 |
| Comparison expressions | Comparison1 | 13 |
| List expressions | List1 | 5 |
| Null handling | Null1 | 5 |
| String functions | String1 | 1 |

Scenarios tagged `@NegativeTests`, `@skip`, `@fails`, `@crash`, or `@wip` are skipped.

### Known gaps

Write operations (`SET`, `DELETE`, `MERGE`) are not covered — ClickGraph is a read-query engine. The TCK's write-oriented feature files are not included.

## How it works

1. **Schema generation** — at startup, all `.feature` files are scanned to extract every `CREATE` block. Node labels and relationship types are collected into a universal schema (`SchemaCatalog`), which is written as a ClickGraph YAML schema and used to create `ReplacingMergeTree` tables in chdb.

2. **One chdb session per process** — chdb supports only one active session per process. A single `Database` is created at startup and shared across all scenarios via `LazyLock`. Tables are **truncated** between scenarios rather than recreated.

3. **Test execution** — each scenario follows the standard Cucumber lifecycle:
   - *Given* `an empty graph` / `having executed:` — truncates tables, then runs Cypher `CREATE` statements to populate data
   - *When* `executing query:` — translates Cypher to SQL and executes it via the embedded engine
   - *Then* `the result should be (in any order / in order)` — normalises output (bools, nulls, floats) and compares with the expected Gherkin table

## Running

```bash
# Requires CLICKGRAPH_CHDB_TESTS=1 to opt in to chdb e2e tests
CLICKGRAPH_CHDB_TESTS=1 cargo test -p clickgraph-tck --test tck

# Show SQL generated for failing scenarios (written to /tmp/tck_failing_sql.txt)
CLICKGRAPH_CHDB_TESTS=1 cargo test -p clickgraph-tck --test tck 2>&1 | grep FAIL
```

> **Important**: Never run multiple instances of these tests concurrently. chdb is a
> full in-process ClickHouse engine and is memory-intensive. The test harness caps
> each session to 4 threads and 4 GiB per query; running several instances in
> parallel will still saturate available RAM.

## Adding feature files

1. Copy the `.feature` file from the [openCypher TCK](https://github.com/opencypher/openCypher/tree/master/tck/features) into `tests/features/clauses/` or `tests/features/expressions/`.
2. Update `tests/features/FEATURES_VERSION` with the source commit.
3. Run the tests — the schema generator picks up new labels/rel-types automatically.
4. Tag scenarios that rely on unsupported features with `@skip` and add a comment explaining why.

## Directory structure

```
clickgraph-tck/
├── Cargo.toml
└── tests/
    ├── tck.rs             # Cucumber test harness (step definitions, world state)
    ├── create_parser.rs   # Re-export of the embedded Cypher CREATE parser
    ├── schema_gen.rs      # Universal schema inference from feature files
    ├── result_fmt.rs      # Result normalisation and Gherkin table parsing
    └── features/
        ├── FEATURES_VERSION
        ├── clauses/       # MATCH, WITH, RETURN, ORDER BY, SKIP/LIMIT
        └── expressions/   # Aggregation, Boolean, Comparison, List, Null, String
```
