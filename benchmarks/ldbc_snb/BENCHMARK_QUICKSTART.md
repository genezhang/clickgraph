# LDBC SNB Benchmark Quick Reference

## One-Command Full Benchmark
```bash
cd benchmarks/ldbc_snb/scripts
./run_full_benchmark.sh sf10
```

## Prerequisites Setup
```bash
# Start ClickHouse
cd benchmarks/ldbc_snb
docker-compose up -d

# Start ClickGraph
export CLICKHOUSE_URL='http://localhost:8123'
export CLICKHOUSE_USER='default'
export CLICKHOUSE_PASSWORD='default'
export CLICKHOUSE_DATABASE='ldbc'
export GRAPH_CONFIG_PATH='./benchmarks/ldbc_snb/schemas/ldbc_snb.yaml'
cargo run --release &
```

## Individual Benchmarks

### Data Loading
```bash
./benchmark_data_loading.sh sf10
# Measures: rows/sec, MB/sec, per-table timing
# Output: results/loading/loading_benchmark_*.json
```

### Query Performance
```bash
export WARMUP_RUNS=1 BENCHMARK_RUNS=3
./benchmark_query_performance.sh
# Tests: 29 LDBC queries
# Measures: min/max/avg/median duration, rows returned
# Output: results/performance/query_benchmark_*.json
```

### Concurrent Load
```bash
export TEST_DURATION=30 CONNECTION_COUNTS="1 2 4 8 16 32"
./benchmark_concurrent_load.sh
# Tests: 5 representative queries at each concurrency level
# Measures: QPS, P50/P90/P99 latency, error rate
# Output: results/concurrency/concurrent_benchmark_*.json
```

## Scale Factors
- **sf0.003** - Quick validation (3K persons, ~5min total)
- **sf0.01** - Small test (10K persons, ~10min total)
- **sf10** - Realistic benchmark (3.3M persons, ~1-2hr total) ⭐ Recommended
- **sf100** - Large scale (33M persons, ~8-10hr total)

## Expected Results (sf10)
- **Loading**: 20K-50K rows/sec, 3-10min total
- **Query Performance**: 10ms-5s depending on complexity, 90%+ success rate
- **Concurrent QPS**: 100-200 QPS for simple queries, 20-50 QPS for multi-hop

## Results Location
```
benchmarks/ldbc_snb/results/
├── loading/        # Data loading metrics
├── performance/    # Query timing
├── concurrency/    # QPS and latency
└── benchmark_report_*.md  # Combined report
```

## Troubleshooting

**Server not running**:
```bash
curl http://localhost:8080/health
# If fails: cargo run --release &
```

**Data not loaded**:
```bash
cd benchmarks/ldbc_snb
./scripts/load_data.sh sf10
```

**High error rates**:
- Check ClickHouse memory limits
- Reduce CONNECTION_COUNTS
- Run in release mode

## More Info
See `benchmarks/ldbc_snb/scripts/README.md` for complete documentation.
