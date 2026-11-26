# Observability stack

A local Prometheus, Tempo and Grafana stack for ClickGraph:

- Prometheus scrapes ClickGraph's `/metrics` endpoint.
- Tempo receives the opt-in OTLP query spans (`cypher.query`).
- Grafana provisions both data sources and the **ClickGraph Overview** dashboard.

## Start

```bash
cd observability
docker compose up -d
```

Open <http://localhost:3000>. If port 3000 is taken, set another one:

```bash
GRAFANA_PORT=3001 docker compose up -d
```

Prometheus is on <http://localhost:9090> and Tempo's HTTP API on
<http://localhost:3200>. Every port is published on `127.0.0.1` only, because Grafana allows
anonymous admin access and Prometheus and Tempo have no authentication.

## Send data from ClickGraph

Metrics work without extra configuration, except the ClickHouse read counters
(see below). Traces need the OTLP endpoint:

```bash
CLICKGRAPH_OTLP_ENDPOINT=http://localhost:4318 \
CLICKGRAPH_METRICS_CH_SUMMARY=true \
CLICKHOUSE_URL=http://localhost:8123 \
CLICKHOUSE_USER=default \
CLICKHOUSE_PASSWORD=default \
CLICKHOUSE_DATABASE=ldbc \
GRAPH_CONFIG_PATH=benchmarks/ldbc_snb/schemas/ldbc_snb_complete.yaml \
  ./target/release/clickgraph --http-port 7475
```

ClickGraph posts to `{endpoint}/v1/traces`; Tempo's OTLP HTTP receiver listens
on 4318. Set `CLICKGRAPH_OTLP_INCLUDE_QUERY=1` to put the literals-stripped
Cypher text in each span. Query text is never included by default.

`CLICKGRAPH_METRICS_CH_SUMMARY=true` makes ClickGraph read ClickHouse's
`X-ClickHouse-Summary` header, which fills `clickgraph_clickhouse_read_rows_total`
and `clickgraph_clickhouse_read_bytes_total`. It is off by default; without it the
**ClickHouse rows/s** panel and the `read` line of **ClickHouse bytes/s** stay at 0.
The value must be `true` (not `1`).

Then send a few queries, for example through the MCP server or:

```bash
curl -s http://localhost:7475/query \
  -H 'Content-Type: application/json' \
  --data '{"query":"MATCH (p:Person) RETURN count(p) AS n","schema_name":"ldbc_snb"}'
```

## Dashboard

`grafana/dashboards/clickgraph.json` has:

- query rate, failure rate, in-flight queries and p95 latency;
- query rate by type and p95 latency by phase;
- errors by class, cache hit rate, ClickHouse rows/s and read/network bytes/s;
- p50/p95/p99 latency;
- recent `cypher.query` traces and traces slower than 500 ms.

The trace tables query Tempo with TraceQL, so they stay empty until ClickGraph
runs with `CLICKGRAPH_OTLP_ENDPOINT` set and has served a query.

## Verify

With the stack up and ClickGraph serving queries, run the smoke check:

```bash
GRAFANA_PORT=3001 ./smoke.py
```

It checks ClickGraph's `/health`, the Prometheus scrape target and
`clickgraph_queries_total`, Tempo and its received `cypher.query` spans, and the
provisioned Grafana dashboard. `GRAFANA_PORT` only needs to be set when Grafana
is not on port 3000. `PROMETHEUS_URL`, `TEMPO_URL`, `GRAFANA_URL` and
`CLICKGRAPH_URL` override the defaults.

## Configuration

- Prometheus scrapes `host.docker.internal:7475`. Change `prometheus.yml` if
  ClickGraph listens elsewhere or runs in another container.
- Prometheus keeps 6 hours of metrics and Tempo keeps 24 hours of traces, both
  in Docker volumes. `docker compose down -v` removes them.
- Grafana's anonymous admin is controlled by `GF_AUTH_ANONYMOUS_*` in
  `docker-compose.yaml`. Remove those variables, set a password and drop the
  `127.0.0.1:` prefix from the port mappings before exposing the stack beyond
  localhost.
