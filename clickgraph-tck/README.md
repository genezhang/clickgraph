# clickgraph-tck

openCypher [Technology Compatibility Kit (TCK)](https://github.com/opencypher/openCypher/tree/master/tck) runner for ClickGraph, using the embedded chdb engine.

## Current status

**402 / 402 read scenarios passing (100%)** — 0 failing.

Write feature files (`Create*`, `Set*`, `Delete*`, `Remove*` — 21 files / 205 scenarios) were imported from upstream openCypher TCK on 2026-05-02 as part of Phase 4 of the embedded-writes work. Phase 5a (#281) added the side-effect step. Phase 5b (this commit) added anonymous-node support (`CREATE ()`, `CREATE (n {...})` route to the `__Unlabeled` table catalogued by `schema_gen.rs`), lifting the file-level `@wip` from `Create1.feature` so its un-gated scenarios run. Remaining write scenarios stay `@wip` per-scenario until the corresponding capability lands. See [`docs/design/embedded-writes.md`](../docs/design/embedded-writes.md) Appendix (Phase 5).

## What it tests

The TCK is the openCypher project's official conformance suite. Each scenario is a Gherkin `.feature` file that specifies a graph setup, a Cypher query, and expected results. This crate runs a subset of those scenarios against ClickGraph's embedded query engine.

**Current coverage** — 402 read scenarios across 20 feature files (write feature files imported but `@wip`-gated):

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

Scenarios tagged `@NegativeTests`, `@skip`, `@fails`, `@crash`, `@wip`, or `@unsupported-label-mutation` are skipped. The first five are *temporary* — `@wip` flags features still being triaged for support, and the others mark scenarios that need a one-off look. `@unsupported-label-mutation` is *permanent*: ClickGraph does not support runtime label mutations (`SET n:Label` / `REMOVE n:Label`) because labels are part of the table identity in ClickGraph (one writable table per label), not a runtime property.

### Write feature files (imported, `@wip`-gated)

| Category | Feature files | Scenarios |
|----------|--------------|-----------|
| `CREATE` | Create1–6 | ~70 |
| `SET` | Set1–6 | ~80 |
| `DELETE` | Delete1–6 | ~40 |
| `REMOVE` | Remove1–3 | ~15 |

These exercise the Cypher write pipeline added in v0.6.7 (`CREATE` / `SET` / `DELETE` / `REMOVE` against ClickGraph-managed embedded tables — see [`docs/wiki/Cypher-Language-Reference.md#write-clauses`](../docs/wiki/Cypher-Language-Reference.md#write-clauses)).

Phase 5a (this commit) ships the harness extensions to start running them:

- **Side-effect step** — `the side effects should be:` now parses the Gherkin table and asserts against the four `QueryResult` counter columns returned by `handle_write_async` (`nodes_created` / `properties_set` / `nodes_deleted` / `relationships_deleted`). `+nodes` / `+properties` / `-nodes` / `-relationships` / `-properties` map directly. Unmappable side effects (label mutations, future `+relationships` for relationship CREATE) mark the scenario as skipped via `world.skip_reason` so they surface as triage candidates rather than hard-failing the run.
- **Counter capture** — `when_executing_query` detects the four-column counter shape returned by writes and stashes the row in `TckWorld::write_counters` so the side-effect step can read it. Read queries leave `write_counters = None`.
- **Permanent skip tag `@unsupported-label-mutation`** — added to `Set3.feature` (label-add scenarios) and `Remove2.feature` (label-remove scenarios). The cucumber filter and `schema_gen::feature_is_filtered()` both recognise it. Re-tagging these from `@wip` reflects that label mutations will never be supported (labels are part of the table identity in ClickGraph), so they should be filtered out structurally, not held in triage limbo.

Still pending before more imports unlock:

1. **Anonymous nodes** are catalogued under the `__Unlabeled` synthetic label in `schema_gen.rs` (since the initial Phase 4 import) and now route through the write pipeline (Phase 5b). `Create1` scenarios [1] [2] [3] [4] [7] [9] are running; the rest stay `@wip` per-scenario behind the issues below.
2. **Per-scenario disposition** for combinations the write pipeline rejects deliberately (`CREATE … RETURN`, multi-label CREATE (`CREATE (:A:B:C:D)`), relationship `CREATE`, `DELETE r` for an edge alias, `SET a += {…}` / `SET a = {…}` map-merge / full-map, `MERGE`, expected SyntaxError diagnostics). Each is rejected with an explicit error today; Phase 5c+ will lift the implementation gaps and document the rest as out-of-scope.

`MERGE` (Merge1..6 upstream) is not imported yet — tracked for v0.7.x Phase 5.

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
