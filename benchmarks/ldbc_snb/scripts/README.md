# LDBC SNB Benchmark Scripts

This directory contains comprehensive benchmarking tools for measuring ClickGraph performance on LDBC SNB datasets.

## Overview

The benchmark suite consists of three phases:
1. **Data Loading Benchmark** - Measures data import performance
2. **Query Performance Benchmark** - Measures individual query execution time
3. **Concurrent Load Testing** - Measures QPS and scalability under concurrent load

## Quick Start

### Prerequisites

1. **ClickHouse Running**:
   ```bash
   cd benchmarks/ldbc_snb
   docker-compose up -d
   ```

2. **ClickGraph Server Running**:
   ```bash
   export CLICKHOUSE_URL='http://localhost:8123'
   export CLICKHOUSE_USER='default'
   export CLICKHOUSE_PASSWORD='default'
   export CLICKHOUSE_DATABASE='ldbc'
   export GRAPH_CONFIG_PATH='./benchmarks/ldbc_snb/schemas/ldbc_snb.yaml'
   
   cargo run --release &
   ```

### Run Full Benchmark Suite

```bash
cd benchmarks/ldbc_snb/scripts
./run_full_benchmark.sh sf10
```

This will:
- Load all sf10 data and measure loading time
- Run 29 supported queries 3 times each (with warmup)
- Test concurrent load at 1, 2, 4, 8, 16, 32 connections
- Generate a comprehensive markdown report

**Duration**: ~1-2 hours for sf10 (depending on hardware)

## Individual Benchmark Scripts

### 1. Data Loading Benchmark

**Script**: `benchmark_data_loading.sh`

Measures how fast data loads into ClickHouse from CSV files.

**Usage**:
```bash
./benchmark_data_loading.sh sf10
```

**Metrics Captured**:
- Per-table loading duration (ms)
- CSV file size vs. storage size
- Row count per table
- Throughput (rows/sec, MB/sec)
- Total loading duration

**Output**:
- JSON: `../results/loading/loading_benchmark_sf10_TIMESTAMP.json`
- Summary: `../results/loading/loading_summary_sf10_TIMESTAMP.txt`

**Environment Variables**:
- `CLICKHOUSE_HOST` (default: localhost)
- `CLICKHOUSE_PORT` (default: 8123)
- `CLICKHOUSE_USER` (default: default)
- `CLICKHOUSE_PASSWORD` (default: default)
- `CLICKHOUSE_DATABASE` (default: ldbc)

**Example Output**:
```
Loading Person_0_0.csv...
  ✅ Loaded in 2345ms - 9890 rows (4216 rows/sec, 1.2 MB/sec)
```

### 2. Query Performance Benchmark

**Script**: `benchmark_query_performance.sh`

Measures execution time for all supported LDBC queries.

**Usage**:
```bash
./benchmark_query_performance.sh
```

**Configuration** (environment variables):
```bash
export SCALE_FACTOR="sf10"
export WARMUP_RUNS=1
export BENCHMARK_RUNS=3
export CLICKGRAPH_URL="http://localhost:8080"
export SCHEMA_NAME="ldbc_snb"
```

**Queries Tested** (29 queries):
- Interactive Short: IS1-IS7 (7 queries)
- Interactive Complex: IC1-IC9, IC11-IC13 (12 queries)
- Business Intelligence: BI1-3, BI5-7, BI9, BI11-14, BI17-18 (10 queries)

**Metrics Captured**:
- Average duration (ms)
- Median duration (ms)
- Min/Max duration (ms)
- Average row count returned
- Success/failure status

**Output**:
- JSON: `../results/performance/query_benchmark_sf10_TIMESTAMP.json`
- Summary: `../results/performance/query_summary_sf10_TIMESTAMP.txt`

**Example Output**:
```
Benchmarking IS1...
  ✅ PASS - Avg: 45.23ms, Median: 43ms, Min: 41ms, Max: 52ms, Rows: 8
```

### 3. Concurrent Load Testing

**Script**: `benchmark_concurrent_load.sh`

Measures QPS (queries per second) and latency under concurrent load.

**Usage**:
```bash
./benchmark_concurrent_load.sh
```

**Configuration** (environment variables):
```bash
export SCALE_FACTOR="sf10"
export TEST_DURATION=30  # seconds per test
export CONNECTION_COUNTS="1 2 4 8 16 32"
export CLICKGRAPH_URL="http://localhost:8080"
export SCHEMA_NAME="ldbc_snb"
```

**Test Queries**:
- `simple_count` - Count all persons
- `simple_filter` - Filter person by ID
- `one_hop` - Single relationship traversal
- `two_hop` - Two-hop relationship traversal
- `is2` - Interactive Short query 2

Each query tested at each concurrency level (1, 2, 4, 8, 16, 32 connections).

**Metrics Captured**:
- QPS (queries per second)
- Average latency (ms)
- Latency percentiles: P50, P90, P99
- Error rate (%)
- Total requests/errors

**Output**:
- JSON: `../results/concurrency/concurrent_benchmark_sf10_TIMESTAMP.json`
- Summary: `../results/concurrency/concurrent_summary_sf10_TIMESTAMP.txt`

**Example Output**:
```
Testing simple_count with 8 connections...
  QPS: 145.67, Avg Latency: 54.82ms, P50: 52ms, P90: 68ms, P99: 95ms, Errors: 0.00%
```

### 4. Master Benchmark Script

**Script**: `run_full_benchmark.sh`

Orchestrates all three benchmark phases and generates a comprehensive report.

**Usage**:
```bash
./run_full_benchmark.sh sf10
```

**Process**:
1. Runs data loading benchmark
2. Waits 10s for ClickHouse to stabilize
3. Runs query performance benchmark (3 runs per query)
4. Runs concurrent load testing (30s per concurrency level)
5. Generates markdown report with all results

**Output**:
- Markdown Report: `../results/benchmark_report_sf10_TIMESTAMP.md`
- All individual benchmark results preserved

**Example Report Sections**:
- Executive Summary (environment, versions, system specs)
- Data Loading Performance (rows, duration, throughput)
- Query Performance (success rate, fastest/slowest queries)
- Concurrent Load Testing (peak QPS per query type)

## Results Directory Structure

```
benchmarks/ldbc_snb/results/
├── loading/
│   ├── loading_benchmark_sf10_20250112_140523.json
│   └── loading_summary_sf10_20250112_140523.txt
├── performance/
│   ├── query_benchmark_sf10_20250112_143045.json
│   └── query_summary_sf10_20250112_143045.txt
├── concurrency/
│   ├── concurrent_benchmark_sf10_20250112_150234.json
│   └── concurrent_summary_sf10_20250112_150234.txt
└── benchmark_report_sf10_20250112_153012.md
```

## Scale Factors

LDBC SNB datasets come in various sizes:

| Scale Factor | Persons  | Comments | Posts   | Relationships |
|-------------|----------|----------|---------|---------------|
| sf0.003     | 3,000    | ~6K      | ~3K     | ~50K          |
| sf0.01      | 10,000   | ~20K     | ~10K    | ~160K         |
| sf0.1       | 73,000   | ~150K    | ~70K    | ~1.2M         |
| sf1         | 327,000  | ~670K    | ~320K   | ~5.5M         |
| sf10        | 3,300,000| ~6.8M    | ~3.3M   | ~55M          |
| sf30        | 9,900,000| ~20M     | ~9.9M   | ~165M         |
| sf100       | 33,000,000| ~68M    | ~33M    | ~550M         |

**Recommendation**: Start with sf0.003 or sf0.01 for quick validation, use sf10 for realistic benchmarking.

## Performance Tuning

### ClickHouse Configuration

For better benchmark performance, adjust ClickHouse settings in `docker-compose.yaml`:

```yaml
environment:
  - CLICKHOUSE_DB=ldbc
  - CLICKHOUSE_USER=default
  - CLICKHOUSE_PASSWORD=default
  # Performance tuning
  - max_threads=8
  - max_memory_usage=16000000000  # 16GB
  - max_execution_time=300  # 5 minutes
```

### ClickGraph Configuration

Run in release mode for optimal performance:
```bash
cargo build --release
cargo run --release &
```

### System Recommendations

For sf10 benchmarking:
- **CPU**: 8+ cores
- **RAM**: 16+ GB
- **Storage**: 20+ GB free (SSD recommended)

For sf100 benchmarking:
- **CPU**: 16+ cores
- **RAM**: 64+ GB
- **Storage**: 100+ GB (NVMe SSD recommended)

## Troubleshooting

### Server Not Running
```
❌ ClickGraph server is not running
```
**Solution**: Start ClickGraph with correct environment variables:
```bash
export CLICKHOUSE_URL='http://localhost:8123'
export GRAPH_CONFIG_PATH='./benchmarks/ldbc_snb/schemas/ldbc_snb.yaml'
cargo run --release &
```

### Data Not Loaded
```
❌ Table Person is empty
```
**Solution**: Load data first:
```bash
cd benchmarks/ldbc_snb
./scripts/load_data.sh sf10
```

### High Error Rates in Concurrent Tests
```
QPS: 50.12, ... Errors: 45.00%
```
**Possible Causes**:
- ClickHouse overloaded (reduce `CONNECTION_COUNTS`)
- Memory limits hit (increase `max_memory_usage`)
- Timeout issues (increase `max_execution_time`)

### Slow Query Performance
**Check**:
1. Are you running in release mode? (`cargo run --release`)
2. Is ClickHouse configured for performance? (see tuning section)
3. Are tables using proper engines? (should be MergeTree, not Memory)

## Integration with CI/CD

### GitHub Actions Example

```yaml
name: LDBC Benchmark
on: [push]
jobs:
  benchmark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Start ClickHouse
        run: |
          cd benchmarks/ldbc_snb
          docker-compose up -d
      - name: Load Data
        run: |
          cd benchmarks/ldbc_snb/scripts
          ./load_data.sh sf0.003
      - name: Run Benchmark
        run: |
          export GRAPH_CONFIG_PATH='./benchmarks/ldbc_snb/schemas/ldbc_snb.yaml'
          cargo run --release &
          sleep 10
          cd benchmarks/ldbc_snb/scripts
          ./run_full_benchmark.sh sf0.003
      - name: Upload Results
        uses: actions/upload-artifact@v2
        with:
          name: benchmark-results
          path: benchmarks/ldbc_snb/results/
```

## Best Practices

1. **Always use release builds** for benchmarking
2. **Run warmup queries** to populate caches
3. **Run multiple iterations** to account for variance
4. **Monitor system resources** during benchmarks
5. **Document environment** (CPU, RAM, storage type)
6. **Version control results** for comparison over time
7. **Test scale factors incrementally** (sf0.003 → sf0.01 → sf10)

## Advanced Usage

### Custom Query Set

To benchmark specific queries:

```bash
# Edit benchmark_query_performance.sh
# Comment out query categories you don't want to test

# Example: Test only IS queries
./benchmark_query_performance.sh
```

### Custom Connection Counts

```bash
# Test specific concurrency levels
export CONNECTION_COUNTS="4 8 16"
./benchmark_concurrent_load.sh
```

### Extended Test Duration

```bash
# Run longer concurrent tests for more accurate results
export TEST_DURATION=60  # 60 seconds per test
./benchmark_concurrent_load.sh
```

### Compare Scale Factors

```bash
# Benchmark multiple scale factors
for sf in sf0.003 sf0.01 sf0.1 sf1; do
    ./load_data.sh $sf
    ./run_full_benchmark.sh $sf
done
```

## Contributing

When adding new benchmark scripts:
1. Follow existing naming conventions (`benchmark_*.sh`)
2. Output JSON results for machine parsing
3. Generate human-readable summary files
4. Document all environment variables
5. Include error handling and validation
6. Update this README with usage examples

## License

Same as ClickGraph project (see LICENSE in repository root).

---

**Last Updated**: January 2025
**ClickGraph Version**: v0.5.x
**LDBC SNB Version**: v0.3.3
