# LDBC SNB Benchmark for ClickGraph

This directory contains the official LDBC Social Network Benchmark (SNB)
queries, the ClickHouse schema that maps them to ClickGraph, and a reproducible
runner that records pass/fail and latency per query.

## Current status

The last full runs are from August 2026:

| Scale | Passed | Total |
| --- | --- | --- |
| SF1 | 26 | 41 |
| SF10 | 26 | 41 |

Read the reports:

- [`results/SF1_REAL_RUN_2026-08-30.md`](results/SF1_REAL_RUN_2026-08-30.md)
- [`results/SF1_SF10_POSTFIX_2026-08-31.md`](results/SF1_SF10_POSTFIX_2026-08-31.md)

The 15 remaining failures are structural, not scale-driven: GDS/APOC
procedures, `CALL {}` subqueries, a polymorphic schema-mapping gap, one planner
limitation, and five residual variable-length-path shapes. Check the reports
for the exact list.

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
# 1. Start ClickHouse and create the LDBC tables.
cd benchmarks/ldbc_snb
docker compose -f docker-compose.ldbc.yaml up -d

# 2. Generate the SF1 CSV files. This takes a while and needs disk space.
./scripts/download_data.sh sf1 --generate

# 3. Load the CSVs through the running ClickHouse container.
./schemas/sf1_load_data.sh \
  http://localhost:8123 default default \
  /data/sf1/graphs/csv/interactive/composite-projected-fk

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
python3 benchmarks/ldbc_snb/run.py --label sf1 --iterations 5
```

If the ClickHouse container already has a volume from an older schema, remove
it with `docker compose -f docker-compose.ldbc.yaml down -v` before step 1.
Use a different `--http-port` if 7475 is taken.

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
