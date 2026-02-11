# Development Schemas

This directory contains modified schemas used for testing and development that don't belong in the benchmarks.

## Files

- `social_dev.yaml` - Extended social network schema with test-only additions:
  - ZeekLog node (for UNWIND array column testing)
  - PatternCompUser node (for pattern comprehension testing)
  
## Usage

For development/testing:
```bash
export GRAPH_CONFIG_PATH="./schemas/dev/social_dev.yaml"
```

For benchmarks:
```bash
export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"
```

**Important**: Keep benchmark schemas pristine! All test-specific schema modifications should go here.
