# Tests Directory Structure

**Last Updated**: December 20, 2025

## Directory Organization

```
tests/
├── unit/                    # Unit tests (Rust/Python)
├── integration/             # Integration test suites
│   ├── conftest.py         # pytest fixtures
│   ├── suites/             # Organized test suites
│   │   ├── social_benchmark/
│   │   ├── optional_match/
│   │   ├── shortest_paths/
│   │   ├── variable_paths/
│   │   └── test_integration/
│   ├── matrix/             # Schema matrix tests
│   ├── wiki/               # Wiki examples as tests
│   ├── query_patterns/     # Pattern-based tests
│   └── fixtures/           # Test data & schemas
│       ├── data/
│       ├── data_legacy/    # Legacy test data
│       ├── schemas/        # Test schemas
│       ├── cypher/         # Cypher test queries
│       └── sql/            # SQL test queries
├── e2e/                    # End-to-end tests
├── regression/             # Regression test data
├── rust/                   # Rust-specific tests
├── legacy/                 # Old Python test scripts (pre-reorganization)
└── private/                # Private/local test data (gitignored)
```

## Quick Reference

### Running Tests

```bash
# All integration tests
pytest tests/integration/ -v

# Specific suite
pytest tests/integration/suites/optional_match/ -v

# Single test file
pytest tests/integration/test_basic_queries.py -v

# With coverage
pytest tests/integration/ --cov=clickgraph --cov-report=html
```

### Test Fixtures

- **Schemas**: `tests/integration/fixtures/schemas/` - Test-specific schema definitions
- **Data**: `tests/integration/fixtures/data/` - Test data fixtures
- **Cypher**: `tests/integration/fixtures/cypher/` - Example Cypher queries
- **SQL**: `tests/integration/fixtures/sql/` - Expected SQL outputs

### Adding New Tests

1. **Integration test**: Create in `tests/integration/` with `test_` prefix
2. **Suite-specific test**: Add to appropriate suite directory
3. **End-to-end test**: Create in `tests/e2e/`
4. **Unit test**: Add to `tests/unit/`

### Test Schema

Most tests use the benchmark schema:
```bash
export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"
```

Suite-specific schemas are in `tests/integration/suites/*/schema.yaml`

## Documentation

- **Test planning**: See `docs/testing/` for regression plans and assessments
- **Test reports**: Results and baselines in `docs/testing/`

## Cleanup History

- **Dec 20, 2025**: Reorganized tests/ directory
  - Moved `tests/*.md` → `docs/testing/`
  - Moved `tests/debug_*.py` → `scripts/debug/`
  - Moved `tests/test_*.py` (root) → `tests/integration/`
  - Moved `tests/cypher/`, `tests/sql/`, `tests/data/` → `tests/fixtures/`
  - Renamed `tests/python/` → `tests/legacy/`
  - Added `tests/private/` to .gitignore
