# LDBC SNB Benchmark for ClickGraph

This directory contains the official LDBC Social Network Benchmark (SNB)
queries, the ClickHouse schema that maps them to ClickGraph, and a reproducible
runner that records pass/fail and latency per query.

## Current status

| Scale | Date | Ran without error | Returned rows | Total | Report |
| --- | --- | --- | --- | --- | --- |
| SF1 | 2026-11-26 | 33 | 33 | 41 | [`results/20261126_042821_sf1.md`](results/20261126_042821_sf1.md) |
| SF1 | 2026-08-31 | 26 | 25 | 41 | [`results/SF1_SF10_POSTFIX_2026-08-31.md`](results/SF1_SF10_POSTFIX_2026-08-31.md) |
| SF10 | 2026-08-31 | 26 | 25 | 41 | [`results/SF1_SF10_POSTFIX_2026-08-31.md`](results/SF1_SF10_POSTFIX_2026-08-31.md) |
| SF1 | 2026-08-30 | 24 | 23 | 41 | [`results/SF1_REAL_RUN_2026-08-30.md`](results/SF1_REAL_RUN_2026-08-30.md) |

All four runs are on a 32-core, 121 GB Linux host. The November run uses the
schema `schemas/ldbc_snb_complete.yaml` and ClickHouse's default memory settings.
*Ran without error* is what the harness can decide; it does **not** check that the
rows are the right ones (see [What a pass means](#what-a-pass-means)).

Eight SF1 queries fail. Seven use constructs ClickGraph does not support: GDS/APOC
procedures (`complex-14`, `bi-10`, `bi-15`), `CALL {}` subqueries (`bi-4`, `bi-16`),
a procedure call inside a query (`bi-19`) and a parser limitation (`bi-20`). `bi-17`
runs into ClickGraph's 300 s query timeout (HTTP 408).

## Layout

| Path | Purpose |
| --- | --- |
| `queries/official/` | 41 official IS, IC and BI queries, each with a `.params.json` |
| `queries/adapted/` | Development workarounds; not used by the harness |
| `schemas/` | ClickGraph schema YAML and ClickHouse DDL variants |
| `data/mini_dataset.sql` | Small dataset for smoke tests |
| `scripts/` | Data generation, loading and older benchmark scripts |
| `run.py` | Current benchmark harness |
| `results/` | JSON and Markdown reports |

## Reproduce a run

Prerequisites: Docker, a built `clickgraph` binary, and Python 3.10+.

```bash
# 1. Start ClickHouse. On a host with about 8 GB for Docker, add
#    `-f docker-compose.8gb.yaml` (see "Memory" below).
cd benchmarks/ldbc_snb
docker compose -f docker-compose.ldbc.yaml up -d

# 2. Generate the SF1 CSV files. This takes a while and needs disk space.
bash ./scripts/download_data.sh sf1 --generate

# 3. Load the CSVs over ClickHouse's HTTP interface. This applies
#    schemas/clickhouse_ddl.sql and loads every table.
python3 scripts/load_data.py --scale-factor sf1

# 4. Start ClickGraph with the complete LDBC schema.
cd ../..
cargo build --release --bin clickgraph
CLICKHOUSE_URL=http://localhost:8123 \
CLICKHOUSE_USER=default \
CLICKHOUSE_PASSWORD=default \
CLICKHOUSE_DATABASE=ldbc \
GRAPH_CONFIG_PATH=benchmarks/ldbc_snb/schemas/ldbc_snb_complete.yaml \
  ./target/release/clickgraph --http-port 7475

# 5. In another terminal, run the benchmark. --meta records facts the harness
#    cannot see (it talks to ClickGraph only).
python3 benchmarks/ldbc_snb/run.py --schema ldbc_snb --label sf1 --iterations 5 \
  --meta clickhouse=25.8.11 --meta memory_profile=default-unlimited
```

If the ClickHouse container already has a volume from an older schema, remove
it with `docker compose -f docker-compose.ldbc.yaml down -v` before step 1.
Use a different `--http-port` if 7475 is taken.

### Which schema

Use `schemas/ldbc_snb_complete.yaml`. It declares `Message` (Post and Comment),
`Place` (City, Country, Continent) and `Organisation` (Company, University) as union
labels, as in Neo4j's LDBC graph. The older `schemas/ldbc_snb.yaml` types them
separately, so patterns such as `(:City)-[:IS_PART_OF]->(:Country)` and
`(:Message)-[:REPLY_OF]->(:Post)` match nothing: seven official queries
(`short-6`, `complex-11`, `bi-3`, `bi-9`, `bi-11`, `bi-14`, `bi-17`) then translate
to a constant-empty plan and answer in a few milliseconds with no rows (`bi-11`: one
zero count). The schema name is
`ldbc_snb` in both files.

### Memory

The default compose file leaves ClickHouse's memory settings alone. For a host with
about 8 GB for Docker, `docker-compose.8gb.yaml` adds `schemas/clickhouse_memory.xml`
(3 GB per query, four threads, spilling to disk). With it, `short-2`, `bi-6`, `bi-8`,
`bi-12`, `bi-13`, `bi-14` and `bi-17` fail at SF1 with "Query memory limit
exceeded", so such a run says more about the host than about ClickGraph. Without it,
`bi-8` takes about 16 s and peaks above 20 GiB, and `short-2` and `bi-6` need 2.5 to 2.7 GiB.

## Harness

```text
python3 benchmarks/ldbc_snb/run.py [options]
```

| Option | Default | Meaning |
| --- | --- | --- |
| `--url` | `http://localhost:7475` | ClickGraph HTTP endpoint |
| `--schema` | server default | Schema name to send with each query |
| `--iterations` | `5` | Measured runs per query |
| `--warmup` | `1` | Unmeasured runs per query |
| `--timeout` | `300` | Seconds per request |
| `--query` | all 41 | Repeatable query id, for example `short-1` or `bi-5` |
| `--params-file` | none | JSON object mapping query id to parameter overrides |
| `--param` | none | Repeatable `KEY=VALUE`; JSON values are parsed |
| `--out-dir` | `results/` | Directory for the JSON and Markdown reports |
| `--label` | `ldbc` | Report name and metadata |
| `--strict` | off | Exit 1 if any query fails (an empty result is not a failure) |
| `--meta` | none | Repeatable `KEY=VALUE` recorded in the report, e.g. `clickhouse=25.8.11`; may not reuse a built-in key |

The harness reads `foo.params.json` beside each `foo.cypher`, strips comments,
checks that every `$parameter` has a value, runs the warm-up, then records
`min`, `median` and `max` wall-clock time and the row count. A query that fails
is kept in the report with ClickGraph's own error text; it is never skipped
silently.

Outputs are a diffable `YYYYMMDD_HHMMSS_<label>.json` and a matching `.md`.
The JSON is the source of truth; the Markdown is for reading. The metadata records
the server version (from `/health`), the harness's git commit, the interpreter and
platform, and any `--meta` pairs. `schema_version` is 2: it adds the `empty` status
and renames `commit` to `harness_commit` (a version 1 report counts a zero-row
result as a pass).

### What a pass means

Each query ends as one of three statuses:

- **pass**: it ran without an error and returned at least one row.
- **empty**: it ran without an error but returned no rows. That can be a true empty
  answer, a parameter that matches nothing, or a plan that cannot match the schema
  (see "Which schema"); its latency may measure nothing. It is counted separately
  and is not a failure for `--strict`.
- **fail**: any error, with ClickGraph's own text.

The harness checks that a query runs and how long it takes. It does **not** compare
the rows with a reference, so a pass is not proof of a correct answer. A query that
returns one aggregate row, such as a zero count, is never reported as empty.

### After a timeout

When ClickGraph's query timeout fires (HTTP 408, 300 s by default), ClickGraph stops
waiting but ClickHouse keeps running the query (it does not cancel a query when the
client goes away). At SF1, `bi-17` kept about 30 cores and 10 GiB busy for 18
minutes and made every later query several times slower. The harness prints a note
when this may have happened; check `system.processes` and `KILL QUERY` before
trusting later timings. The committed report was run with `bi-17` last for this
reason.

### Mini smoke test

The mini dataset is useful for checking the harness itself without generating
SF1. Load `data/mini_dataset.sql` into the ClickHouse container, start ClickGraph
with `schemas/ldbc_snb_mini.yaml` (and `CLICKHOUSE_DATABASE=ldbc_mini`), then
override the sample IDs:

```bash
docker exec -i clickgraph-ldbc-clickhouse clickhouse-client \
  --user default --password default --multiquery < data/mini_dataset.sql
```

```json
{"short-1": {"personId": 1}, "short-3": {"personId": 1}}
```

```bash
python3 benchmarks/ldbc_snb/run.py \
  --query short-1 --query short-3 \
  --params-file /tmp/mini-params.json \
  --label mini --iterations 3 --warmup 1
```

## Legacy material

`scripts/` still contains older benchmark helpers (`comprehensive_benchmark.py`,
`benchmark_query_performance.sh`, `embedded_benchmark.py`, and others). They
have hardcoded paths and are kept for history. Use `run.py` for new results.

The `embedded_benchmark.py` path exercises the embedded Parquet/Iceberg mode
rather than the server and is outside this harness.
