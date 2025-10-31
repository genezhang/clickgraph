# ClickGraph Benchmark Suite

This directory contains the comprehensive benchmarking suite for ClickGraph performance evaluation.

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

Benchmark results are saved in `benchmark_results/` with timestamps for easy comparison across runs.

## Documentation

See `BENCHMARK_README.md` for comprehensive usage instructions, performance expectations, and troubleshooting.