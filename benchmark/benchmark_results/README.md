# Benchmark Test Measurements

This directory contains raw JSON output from benchmark test runs.

## Files

- `quick_test.json` - Quick validation test results (automated benchmark suite)
- `quick_validation_20251030_205630.json` - Historical validation run from Oct 30, 2025
- `full_test.json` - Full benchmark suite results

## Format

These JSON files contain raw test execution data including:
- Query text
- Execution success/failure
- Response data
- Timestamps
- Test configuration

## Human-Readable Results

**For comprehensive analysis and documentation**, see:
- **[`../../notes/benchmarking.md`](../../notes/benchmarking.md)** - Complete benchmark results with analysis
- **[`../../BENCHMARKS.md`](../../BENCHMARKS.md)** - Quick reference and summary

## Purpose

These JSON files are useful for:
- Automated test validation
- Historical comparison
- CI/CD integration
- Regression detection
- Debugging test failures

They are **not** intended for human consumption - use the markdown documentation above for readable results.
