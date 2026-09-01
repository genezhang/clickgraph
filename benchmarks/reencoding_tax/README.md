# Re-encoding tax: native FK-join SQL vs. the same graph in Apache AGE

**Question (Claim B — representation).** Hold the engine, hardware, and data
constant and change *only* how the graph is represented. How much does re-encoding
a relationship that already exists as an indexed foreign key into generic
node/edge tables cost? This is the paper's "representation" axis, isolated from
the "execution engine" axis: both sides here run on the **same** PostgreSQL
instance, so no columnar-engine advantage is in play — only the representation
differs.

**Answer.** On identical data and identical results, native FK-join SQL is
**2.4×–6.8× faster** than the same graph re-encoded into Apache AGE's label-based
node/edge tables. The tax is largest exactly where the absolute latency is
smallest (the seed-node lookups), and on the whole-graph analytical scan it turns
a 0.56 s query into a 2.2 s one.

## Method

One PostgreSQL + Apache AGE container (`apache/age:latest`, **PostgreSQL 18.6**).
A synthetic social graph of **100,000 persons** and **2,000,000 KNOWS edges**
(out-degree 20, deterministic hash, self-loops dropped) is loaded **twice over**:

- **Native** — `person(id, name, country)` + `knows(src, dst)` with an indexed
  foreign key; answered by ordinary FK-join SQL (`bench_native.sql`).
- **AGE** — byte-for-byte the same data re-encoded into AGE's internal
  `social."Person"` / `social."KNOWS"` tables (indexed on `start_id`/`end_id`);
  answered by the identical traversal in Cypher (`bench_age_load.sql`).

Only the representation differs. Both sides return **identical counts** per query
(verified at load time — the 2-hop-from-42 parity check), and each query is timed
as the **median of 5** server-side runs via psql `\timing`
(`bench_reencoding_tax.sh`).

## Results (median of 5)

| Query | Native SQL (FK join) | AGE (node/edge Cypher) | Re-encoding tax | Result count |
|---|---|---|---|---|
| 2-hop from a seed node   | **1.9 ms**   | 13.0 ms     | 6.8× | 400 |
| 3-hop from a seed node   | **10.3 ms**  | 24.3 ms     | 2.4× | 8,000 |
| 2-hop over the whole graph | **560.6 ms** | 2,211.7 ms | 3.9× | 40,000,000 |

Same engine, same data, representation only. This isolates the re-encoding tax
from any columnar-engine advantage — both sides run on the same row-store
PostgreSQL — and independently reproduces the folklore result that plain
relational SQL outperforms a property-graph overlay on the same database.

Reference host: single node, 32 cores, 121 GB RAM, Linux 7.0.0; `apache/age:latest`
reporting `PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2)`.

## Reproduce

```bash
# Docker required. From the repo root:
bash benchmarks/reencoding_tax/setup.sh              # start container, load both sides (~1 min)
bash benchmarks/reencoding_tax/bench_reencoding_tax.sh   # print the table above
docker rm -f age-bench                                # tidy up
```

Tune size with `N=<persons> D=<out-degree> bash .../setup.sh` (default
`N=100000 D=20`).

## Scope

This measures the **representation** tax on a single row-store engine, by design.
A whole-system comparison (ClickGraph on columnar ClickHouse vs. AGE on
PostgreSQL) would compound the engine and representation axes and is deliberately
left out so this number isolates representation alone. A separate micro-benchmark,
[`../schema_mapping_cost/`](../schema_mapping_cost/), addresses the orthogonal
question of ClickGraph's compile-time schema-mapping vs. runtime SQL views on
ClickHouse.
