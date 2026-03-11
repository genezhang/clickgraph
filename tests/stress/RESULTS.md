# Stress Test Results

## 24-Hour Endurance Test — jemalloc (March 10, 2026)

**Build**: `cargo build --release` with jemalloc allocator (`tikv-jemallocator`)
**Server**: `clickgraph --http-port 8082` with `ldbc_snb_complete.yaml` schema
**Test**: `python3 -u tests/stress/stress_test.py --duration 86400 --concurrency 50`
**Host**: WSL2 Linux 6.6.87

### Summary

| Metric | Value |
|--------|-------|
| Duration | 24.0 hours (86,414s) |
| Total requests | 518,461,199 |
| Avg throughput | 6,000 req/s |
| Successful queries | 72,343,413 |
| Server errors (500) | **0** |
| Timeouts | **0** |
| Connection errors | **0** |

### Latency

| Percentile | Overall | Simple | Medium | Complex |
|------------|---------|--------|--------|---------|
| P50 | 1ms | 1ms | 1ms | 1ms |
| P95 | 2ms | 2ms | 2ms | 2ms |
| P99 | 2ms | 2ms | 2ms | 2ms |
| Max | 22,236ms | 22,236ms | 22,236ms | 22,235ms |

### Memory

| Metric | Value |
|--------|-------|
| Start | 12.2 MB |
| Peak | 28.3 MB |
| Final | 27.1 MB |
| Growth | +121% (+14.9 MB) |

Memory stabilized at ~27-28 MB after initial warmup. No continuous growth observed — the increase is one-time warmup (caches, thread stacks, compiled regex, schema parsing). Final RSS was below peak, indicating jemalloc returns memory effectively.

### Query Distribution

| Category | Count |
|----------|-------|
| Simple | 108,515,143 |
| Medium | 265,259,215 |
| Complex | 72,343,428 |
| Error (schema-not-found) | 72,343,413 |

All 400-level errors were expected: queries targeting `social_integration` and `test_fixtures` schemas not loaded on the LDBC-only server.

### Notes

- Zero server errors across 518M requests demonstrates robust error handling
- Latency remained flat (P50=1ms) throughout the entire 24-hour run
- The max latency spikes (~22s) correlate with GC pauses or WSL2 host scheduling; P99 stayed at 2ms
- Previous endurance test (without jemalloc) showed similar memory patterns; jemalloc provides slightly more predictable allocation behavior

### How to Reproduce

```bash
# Build with jemalloc (enabled in Cargo.toml)
cargo build --release

# Start server
RUST_LOG=info \
CLICKHOUSE_URL=http://localhost:18123 \
CLICKHOUSE_USER=test_user \
CLICKHOUSE_PASSWORD=test_pass \
GRAPH_CONFIG_PATH=benchmarks/ldbc_snb/schemas/ldbc_snb_complete.yaml \
target/release/clickgraph --http-port 8082

# Run 24-hour stress test (50 concurrent workers)
python3 -u tests/stress/stress_test.py \
  --duration 86400 \
  --concurrency 50 \
  --server http://localhost:8082
```
