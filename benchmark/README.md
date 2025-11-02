# ClickGraph Benchmark Suite

This directory contains the comprehensive benchmarking suite for ClickGraph performance evaluation.

---

## 📊 Latest Benchmark Results

**See: [`../notes/benchmarking.md`](../notes/benchmarking.md)** for complete validation results across all scales!

### Quick Summary (November 1, 2025)

| Benchmark | Dataset Size | Success Rate | Documentation |
|-----------|-------------|--------------|---------------|
| **Large** | 5M users, 50M follows | 9/10 (90%) | [`notes/benchmarking.md`](../notes/benchmarking.md#large-benchmark-results-5m-users-50m-follows) |
| **Medium** | 10K users, 50K follows | 10/10 (100%) | [`notes/benchmarking.md`](../notes/benchmarking.md#medium-benchmark-results-10k-users-50k-follows) |
| **Small** | 1K users, 5K follows | 10/10 (100%) | [`notes/benchmarking.md`](../notes/benchmarking.md#small-benchmark-results-1k-users-5k-follows) |

**Key Achievement**: ✅ Enterprise-scale validation on 5 million users and 50 million relationships!

---

## Directory Structure

```
benchmark/
├── benchmark.py              # Main benchmark execution script
├── setup_benchmark_data.py   # Benchmark data generation
├── run_benchmarks.py         # Automated benchmark runner
├── BENCHMARK_README.md       # Detailed documentation
└── benchmark_results/        # Generated benchmark results
```

## Quick Start

1. **Set up environment**:
   ```bash
   # Start ClickHouse
   docker-compose up -d

   # Start ClickGraph server
   cargo run --bin clickgraph
   ```

2. **Generate test data**:
   ```bash
   python benchmark/setup_benchmark_data.py --dataset social --size small
   ```

3. **Run benchmarks**:
   ```bash
   python benchmark/run_benchmarks.py --quick
   ```

## Available Scripts

- **`benchmark.py`**: Core benchmarking tool with detailed performance metrics
- **`setup_benchmark_data.py`**: Generate scalable test datasets
- **`run_benchmarks.py`**: Automated benchmark execution with multiple configurations

## Results

**Latest benchmark results**: See [`../notes/benchmarking.md`](../notes/benchmarking.md) for comprehensive analysis!

This directory (`benchmark_results/`) contains historical benchmark run outputs in JSON format for automated testing and comparison.

For human-readable results with analysis:
- **Primary documentation**: [`../notes/benchmarking.md`](../notes/benchmarking.md)
- **Performance metrics**: Query timing, success rates, scalability analysis
- **All three scales**: Small (1K), Medium (10K), Large (5M users)

## Documentation

See `BENCHMARK_README.md` for comprehensive usage instructions, performance expectations, and troubleshooting.