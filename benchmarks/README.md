# ClickGraph Benchmarks

**Last Updated**: December 7, 2025

---

## Benchmark Suites

| Benchmark | Pattern | Description |
|-----------|---------|-------------|
| [social_network](./social_network/) | Traditional (normalized) | Separate node/edge tables, property mappings |
| [ontime_flights](./ontime_flights/) | Denormalized edge | Virtual nodes, properties embedded in edge table |

## Directory Structure

```
benchmarks/
├── social_network/           # Traditional node/edge pattern
│   ├── data/                 # Data generation scripts
│   ├── queries/              # Benchmark query suites
│   ├── results/              # Benchmark results
│   ├── schemas/              # Schema YAML files
│   ├── docker-compose.benchmark.yaml
│   ├── run_benchmark.ps1     # Windows benchmark runner
│   └── run_regression.ps1    # Windows regression test
│
└── ontime_flights/           # Denormalized edge pattern
    ├── data/                 # Data setup instructions
    ├── queries/              # Benchmark queries
    ├── results/              # Benchmark results
    └── schemas/              # Schema YAML files
```

## Quick Start

### Prerequisites

1. **ClickHouse running** (via `docker-compose up -d` from project root)
2. **Environment variables set**:

```bash
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USER="test_user"
export CLICKHOUSE_PASSWORD="test_pass"
export CLICKHOUSE_DATABASE="brahmand"
```

### Social Network Benchmark (Default)

```bash
# Generate data and run
cd /path/to/clickgraph
python3 benchmarks/social_network/data/setup_unified.py --scale 1
export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"
cargo run --release --bin clickgraph &
python3 benchmarks/social_network/queries/suite.py
```

### OnTime Flights Benchmark

See [ontime_flights/README.md](./ontime_flights/README.md) for data download and setup.

```bash
export GRAPH_CONFIG_PATH="./benchmarks/ontime_flights/schemas/ontime_benchmark.yaml"
cargo run --release --bin clickgraph &
cd benchmarks/ontime_flights/queries
python3 run_ontime_benchmark.py
```

---

## 🚀 Running Benchmarks (Windows)

### Prerequisites
1. ClickHouse running (via `docker-compose up -d` from project root)
2. Python 3.x installed
3. ClickGraph built (`cargo build --release`)

### Run Complete Benchmark

```powershell
# Small benchmark (1K users) - Quick validation
.\benchmarks\social_network\run_benchmark.ps1 -Scale 1 -Iterations 3

# Medium benchmark (10K users) - Performance testing
.\benchmarks\social_network\run_benchmark.ps1 -Scale 10 -Iterations 5

# Large benchmark (100K users) - Stress testing
.\benchmarks\social_network\run_benchmark.ps1 -Scale 100 -Iterations 3

# XLarge benchmark (1M users) - Production scale
.\benchmarks\social_network\run_benchmark.ps1 -Scale 1000 -Iterations 3
```

The script automatically:
1. ✅ Starts ClickGraph server in background (if not running)
2. ✅ Loads benchmark schema (`social_benchmark.yaml`)
3. ✅ Generates data using **MergeTree tables** (Windows-compatible!)
4. ✅ Runs benchmark queries
5. ✅ Saves results to benchmark-specific results directory

### Regression Testing

Before releases or after major changes, run the regression test:

```powershell
# Quick regression test (scale 1, 1 iteration, ~1 minute)
.\benchmarks\social_network\run_regression.ps1
```

This validates that all working queries still pass. Exits with code 1 if any regression detected.
- ~2 second average query time

### Manual Steps (Advanced)

If you prefer to run steps manually:

```powershell
# 1. Start server (uses Start-Job for proper Windows background handling)
$job = Start-Job -ScriptBlock {
    $env:CLICKHOUSE_URL = "http://localhost:8123"
    $env:CLICKHOUSE_USER = "default"
    $env:CLICKHOUSE_PASSWORD = ""
    $env:CLICKHOUSE_DATABASE = "brahmand"
    Set-Location $using:PWD
    cargo run --release --bin clickgraph
}

# 2. Wait for server (check: Invoke-RestMethod http://localhost:8080/health)

# 3. Load schema
python scripts/utils/load_schema.py benchmarks/social_network/schemas/social_benchmark.yaml

# 4. Generate data (MergeTree for persistence)
python benchmarks/data/setup_unified.py --scale 10 --engine MergeTree

# 5. Run benchmarks
python benchmarks/queries/suite.py --scale 10 --iterations 5 --output results.json

# 6. Stop server when done
Stop-Job -Id $job.Id; Remove-Job -Id $job.Id
```

---

## 📊 Quick Reference

For **complete benchmark results and analysis**, see:

### **[`notes/benchmarking.md`](../notes/benchmarking.md)** ⭐

This comprehensive document includes:
- ✅ **Large Benchmark** (5M users, 50M follows): 90% success at enterprise scale
- ✅ **Medium Benchmark** (10K users, 50K follows): 100% success with performance metrics
- ✅ **Small Benchmark** (1K users, 5K follows): 100% success with detailed analysis
- 📈 **Scalability Analysis**: Performance comparison across all scales
- 🔧 **Tooling Documentation**: Data generation and testing scripts
- 📊 **Performance Tables**: Detailed timing and statistics

---

## Summary Table

| Benchmark | Users | Follows | Posts | Success Rate | Performance |
|-----------|-------|---------|-------|--------------|-------------|
| **Large** | 5,000,000 | 50,000,000 | 25,000,000 | 9/10 (90%) | ~2-4s per query |
| **Medium** | 10,000 | 50,000 | 5,000 | 10/10 (100%) | ~2s per query |
| **Small** | 1,000 | 4,997 | 2,000 | 10/10 (100%) | <1s per query |

---

## Key Findings

✅ **Enterprise-Scale Validation**: Successfully tested on 5 million users and 50 million relationships  
✅ **Near-Linear Scaling**: 5000x data increase results in only 2-4x query time increase  
✅ **All Query Types Working**: Node lookups, multi-hop, variable-length, aggregations, patterns  
✅ **Production Ready**: Consistent performance from development to enterprise scale  

**Note**: Only shortest path queries hit memory limits on 5M dataset (ClickHouse configuration tuning recommended).

---

## Quick Links

- **Detailed Results**: [`../notes/benchmarking.md`](../notes/benchmarking.md)
- **Data Generation**: [`data/setup_unified.py`](data/setup_unified.py) - Unified scale factor approach
- **Query Suites**: 
  - [`queries/suite.py`](queries/suite.py) - 16 unified queries (NEW!)
  - [`queries/final.py`](queries/final.py) - Final benchmark
  - [`queries/medium.py`](queries/medium.py) - Medium benchmark
- **Schemas**: [`schemas/social_benchmark.yaml`](schemas/social_benchmark.yaml)
- **Project Status**: [`../STATUS.md`](../STATUS.md)
- **Change Log**: [`CHANGELOG.md`](../CHANGELOG.md)

---

## Running Benchmarks

### Small Benchmark (1K users)
```bash
# Load data
python check_tables.py  # Verify existing data

# Run tests
python test_benchmark_final.py
```

### Medium Benchmark (10K users)
```bash
# Generate and load data
python generate_medium_benchmark_data.py > setup_medium_benchmark_data.sql
Get-Content setup_medium_benchmark_data.sql | docker exec -i clickhouse clickhouse-client --database=brahmand --multiquery

# Run tests with performance metrics
python test_medium_benchmark.py
```

### Large Benchmark (5M users)
```bash
# Load data using ClickHouse native generation (~5 minutes)
python load_large_benchmark.py

# Run tests (may take several minutes)
python test_benchmark_final.py
```

---

**For full documentation, methodology, and detailed results, see [`notes/benchmarking.md`](../notes/benchmarking.md)**


