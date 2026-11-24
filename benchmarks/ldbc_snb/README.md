# LDBC SNB Benchmark for ClickGraph

This directory contains the official LDBC Social Network Benchmark (SNB)
queries, the ClickHouse schema that maps them to ClickGraph, and a reproducible
runner that records pass/fail and latency per query.

## Current status

| Scale | Date | Passed | Total | Report |
| --- | --- | --- | --- | --- |
| SF1 | 2026-11-24 | 29 | 41 | [`results/20261124_185005_sf1.md`](results/20261124_185005_sf1.md) |
| SF1 | 2026-08-30 | 26 | 41 | [`results/SF1_REAL_RUN_2026-08-30.md`](results/SF1_REAL_RUN_2026-08-30.md) |
| SF10 | 2026-08-31 | 26 | 41 | [`results/SF1_SF10_POSTFIX_2026-08-31.md`](results/SF1_SF10_POSTFIX_2026-08-31.md) |

The November run used an 18-core Apple M5 Pro and an 8 GB Docker VM, so its
latencies are not comparable with the August runs on a 32-core, 121 GB host.

Nine of the twelve SF1 failures are unsupported Cypher constructs: GDS/APOC
procedures (`complex-14`, `bi-10`, `bi-15`), `CALL {}` subqueries (`bi-4`,
`bi-16`), node values (`complex-3`, `bi-13`), a procedure call in a query
(`bi-19`) and a parser limitation (`bi-20`). The other three (`short-2`,
`bi-6`, `bi-8`) hit the benchmark profile's 3 GB per-query memory cap while
building a join; raise the limits in `schemas/clickhouse_memory.xml` on a host
with more memory.

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
# 1. Start ClickHouse.
cd benchmarks/ldbc_snb
docker compose -f docker-compose.ldbc.yaml up -d

# 2. Generate the SF1 CSV files. This takes a while and needs disk space.
bash ./scripts/download_data.sh sf1 --generate

# 3. Load the CSVs over ClickHouse's HTTP interface. This applies
#    schemas/clickhouse_ddl.sql and verifies row counts.
python3 scripts/load_data.py --scale-factor sf1

# 4. Start ClickGraph with the LDBC schema.
cd ../..
cargo build --release --bin clickgraph
CLICKHOUSE_URL=http://localhost:8123 \
CLICKHOUSE_USER=default \
CLICKHOUSE_PASSWORD=default \
CLICKHOUSE_DATABASE=ldbc \
GRAPH_CONFIG_PATH=benchmarks/ldbc_snb/schemas/ldbc_snb.yaml \
  ./target/release/clickgraph --http-port 7475

# 5. In another terminal, run the benchmark.
python3 benchmarks/ldbc_snb/run.py --schema ldbc_snb --label sf1 --iterations 5
```

If the ClickHouse container already has a volume from an older schema, remove
it with `docker compose -f docker-compose.ldbc.yaml down -v` before step 1.
Use a different `--http-port` if 7475 is taken.

The compose file mounts `schemas/clickhouse_memory.xml`, which caps each query
at 3 GB, uses four threads and spills large aggregations and sorts to disk.
These settings keep the heavy BI queries inside an 8 GB Docker VM; raise them
on a larger host to measure default ClickHouse behaviour.

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
| `--strict` | off | Exit 1 if any query fails |

The harness reads `foo.params.json` beside each `foo.cypher`, strips comments,
checks that every `$parameter` has a value, runs the warm-up, then records
`min`, `median` and `max` wall-clock time and the row count. A query that fails
is kept in the report with ClickGraph's own error text; it is never skipped
silently.

Outputs are a diffable `YYYYMMDD_HHMMSS_<label>.json` and a matching `.md`.
The JSON is the source of truth; the Markdown is for reading.

### Mini smoke test

The mini dataset is useful for checking the harness itself without generating
SF1. Load `data/mini_dataset.sql` into ClickHouse, start ClickGraph with
`schemas/ldbc_snb_mini.yaml`, then override the sample IDs:

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
