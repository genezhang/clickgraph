# LDBC Social Network Benchmark (SNB) Interactive v1

This benchmark implements the [LDBC SNB Interactive v1](https://ldbcouncil.org/benchmarks/snb/) workload for ClickGraph.

## Overview

The LDBC SNB models a social network with:
- **Person** nodes with properties like name, birthday, location
- **Post** and **Comment** nodes (collectively called Message)
- **Forum** containers for posts
- **Tag** and **TagClass** for content classification
- **Place** (City, Country, Continent) for locations
- **Organisation** (University, Company) for affiliations

## Data

### Scale Factors

| Scale Factor | Persons | Messages | Approx Size |
|-------------|---------|----------|-------------|
| SF0.003     | ~50     | ~500     | ~1 MB       |
| SF0.1       | ~1K     | ~30K     | ~50 MB      |
| SF1         | ~10K    | ~300K    | ~500 MB     |
| SF10        | ~100K   | ~3M      | ~5 GB       |
| SF100       | ~1M     | ~30M     | ~50 GB      |

### Generate Data

Use Docker to generate LDBC SNB data locally (recommended):

```bash
cd benchmarks/ldbc_snb

# Generate tiny dataset for quick testing (~1 min)
./scripts/download_data.sh sf0.003 --generate

# Generate small dataset for development (~5 min)
./scripts/download_data.sh sf0.1 --generate

# Generate medium dataset for benchmarking (~30 min)
./scripts/download_data.sh sf1 --generate
```

**Note**: The script uses the official LDBC Datagen Docker image (`ldbc/datagen-standalone`).
Docker must be installed and running.

## Setup

### 1. Start ClickHouse

```bash
docker-compose -f docker-compose.yaml up -d
```

### 2. Create Tables

Use the schema that matches datagen output format:

```bash
clickhouse-client --multiquery < benchmarks/ldbc_snb/schemas/clickhouse_ddl_datagen.sql
```

### 3. Load Data

```bash
cd benchmarks/ldbc_snb
python scripts/load_data.py --scale-factor sf0.003
```

### 4. Start ClickGraph

```bash
export GRAPH_CONFIG_PATH="./benchmarks/ldbc_snb/schemas/ldbc_snb_datagen.yaml"
cargo run --release
```

## Files

| File | Purpose |
|------|---------|
| `schemas/clickhouse_ddl_datagen.sql` | ClickHouse DDL matching datagen output |
| `schemas/ldbc_snb_datagen.yaml` | ClickGraph graph schema matching datagen |
| `schemas/clickhouse_ddl.sql` | Original LDBC spec DDL (reference only) |
| `schemas/ldbc_snb.yaml` | Original ClickGraph schema (reference only) |
| `scripts/download_data.sh` | Data generation script (Docker-based) |
| `scripts/load_data.py` | Data loader for ClickHouse |
| `scripts/run_benchmark.py` | Benchmark runner |

## Queries

### Interactive Complex Queries (IC1-IC14)

These are the main read-heavy queries that test complex graph patterns:

| Query | Description | Key Features |
|-------|-------------|--------------|
| IC1 | Friends by first name | Variable-length paths, filtering |
| IC2 | Recent messages by friends | Multi-hop, temporal filtering |
| IC3 | Friends in countries | 2-hop traversal, location filtering |
| IC4 | New topics | Tag analysis, temporal ranges |
| IC5 | New groups | Forum membership analysis |
| IC6 | Tag co-occurrence | Complex aggregation |
| IC7 | Recent likers | Temporal analysis |
| IC8 | Recent replies | Comment threading |
| IC9 | Recent messages by FoF | 2-hop friends-of-friends |
| IC10 | Friend recommendation | Birthday matching, scoring |
| IC11 | Job referral | Work history analysis |
| IC12 | Expert search | Tag class hierarchy |
| IC13 | Shortest path | Path finding |
| IC14 | Trusted connection | Weighted paths |

### Business Intelligence (BI) Queries

LDBC BI queries are designed for complex analytics - perfect for ClickHouse's columnar storage.
These queries test aggregations, joins, and analytical patterns.

| Query | Description | Focus Area |
|-------|-------------|------------|
| BI-1 | Posting Summary | Message count analysis |
| BI-2 | Tag Evolution | Tag popularity tracking |
| BI-3 | Popular Topics | Forum content analysis |
| BI-4 | Top Message Creators | Forum activity ranking |
| BI-5 | Most Active Posters | Tag-based engagement |
| BI-6 | Authoritative Users | Like-based authority |
| BI-7 | Related Topics | Tag correlation |
| BI-8 | Central Person | Interest network |
| BI-9 | Top Thread Initiators | Discussion starters |
| BI-10 | Experts in Social Circle | FoF expertise |
| BI-11 | Friend Triangles | Triangle counting |
| BI-12 | Message Distribution | Content distribution |
| BI-14 | International Dialog | Cross-location connections |
| BI-18 | Friend Recommendation | Mutual friend analysis |

**Benchmark Status**: 26/26 queries passing (100%)

```bash
# Run BI queries
python scripts/run_bi_queries.py

# Run specific BI query
python scripts/run_bi_queries.py --query bi-3

# List all available queries
python scripts/run_bi_queries.py --list

# Run with verbose output
python scripts/run_bi_queries.py --query bi-5 -v
```

### Interactive Short Queries (IS1-IS7)

Simple lookup queries for testing point access:

| Query | Description |
|-------|-------------|
| IS1 | Profile of a person |
| IS2 | Recent messages of a person |
| IS3 | Friends of a person |
| IS4 | Content of a message |
| IS5 | Creator of a message |
| IS6 | Forum of a message |
| IS7 | Replies of a message |

## Running Benchmarks

```bash
# Run all IC queries
python scripts/run_benchmark.py --queries ic

# Run specific query
python scripts/run_benchmark.py --query ic1

# Run with timing
python scripts/run_benchmark.py --queries all --timing

# Run BI benchmark with JSON output
python scripts/run_bi_queries.py --output results.json
```

## ClickGraph-Specific Notes

### Supported Features

ClickGraph supports all read queries with these patterns:
- Variable-length paths: `[:KNOWS*1..2]`
- OPTIONAL MATCH for left joins
- Aggregations with GROUP BY
- Complex WHERE filtering
- ORDER BY with LIMIT
- COUNT(DISTINCT ...) for deduplication
- Multi-hop traversals

### Limitations

Since ClickGraph is a read-only engine:
- Update queries (IU1-IU8) are not supported
- The benchmark focuses on the Interactive Complex, Short, and BI queries

## References

- [LDBC SNB Specification](https://arxiv.org/abs/2001.02299)
- [LDBC SNB Interactive v1 Implementations](https://github.com/ldbc/ldbc_snb_interactive_v1_impls)
- [LDBC SNB BI Workload](https://github.com/ldbc/ldbc_snb_bi)
- [LDBC Data Sets](https://ldbcouncil.org/data-sets-surf-repository/)
