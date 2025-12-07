# ClickGraph Benchmarks

This directory contains benchmark suites for validating ClickGraph's Cypher-to-SQL translation across different schema patterns.

## Benchmark Suites

| Benchmark | Pattern | Description |
|-----------|---------|-------------|
| [social_network](./social_network/) | Traditional (normalized) | Separate node/edge tables with property mappings |
| [ontime_flights](./ontime_flights/) | Denormalized edge | Virtual nodes with properties embedded in edge table |

## Prerequisites

1. **ClickHouse running** (via `docker-compose up -d` from project root)
2. **Environment variables** (adjust for your ClickHouse setup):

```bash
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USER="test_user"
export CLICKHOUSE_PASSWORD="test_pass"
export CLICKHOUSE_DATABASE="brahmand"
```

## Directory Structure

```
benchmarks/
├── README.md                 # This file
├── social_network/           # Traditional node/edge pattern
│   ├── README.md             # Full setup & run instructions
│   ├── data/                 # Data generation scripts
│   ├── queries/              # Benchmark query suites
│   ├── results/              # Benchmark results
│   └── schemas/              # Schema YAML files
│
└── ontime_flights/           # Denormalized edge pattern
    ├── README.md             # Full setup & run instructions
    ├── queries/              # Benchmark queries
    ├── results/              # Benchmark results
    └── schemas/              # Schema YAML files
```

## Quick Links

- **Detailed benchmark analysis**: [`../notes/benchmarking.md`](../notes/benchmarking.md)
- **Project status**: [`../STATUS.md`](../STATUS.md)
