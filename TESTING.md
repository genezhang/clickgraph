# ClickGraph Testing Guide

## Quick Start: Running Integration Tests

### Prerequisites
- Docker with ClickHouse running (`docker-compose up -d`)
- ClickHouse must have databases created (done by setup script)

### One-Command Setup & Test

**IMPORTANT**: Tests use conftest.py to auto-start the ClickGraph server. You should NOT have a server already running on port 8080!

```bash
# 1. Load all test data (creates databases, loads fixtures, includes small-scale benchmark data)
bash scripts/test/setup_all_test_data.sh

# 2. Run all integration tests (conftest.py automatically starts server)
pytest tests/integration/

# 3. Run specific test suites
pytest tests/integration/wiki/              # Wiki documentation tests (60 tests)
pytest tests/integration/matrix/            # Matrix tests (2400+ tests)
pytest tests/integration/test_*.py          # Individual test files
```

**Expected baseline**: ~2600 passing, ~900 failing, ~25 skipped (Dec 26, 2025)

### Manual Server Testing (for development/debugging)

If you need to test manually without pytest:
```bash
# 1. Setup test data
bash scripts/test/setup_all_test_data.sh

# 2. Start ClickGraph server (MUST set environment variables!)
CLICKHOUSE_URL="http://localhost:8123" \
CLICKHOUSE_USER="test_user" \
CLICKHOUSE_PASSWORD="test_pass" \
target/debug/clickgraph --http-port 8080 &

# Wait for server to start
sleep 3

# 3. Load test schemas (schemas are in-memory, must reload after each restart!)
CLICKGRAPH_URL=http://localhost:8080 python3 scripts/test/load_test_schemas.py

# 4. Test manually
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query":"USE social_benchmark MATCH (u:User) RETURN u.name LIMIT 3"}'
```

### Test Data Setup Details

The `setup_all_test_data.sh` script creates and loads:
- **test_integration database**: Core test tables (users, follows, products, etc.)
- **brahmand database**: Multi-tenant, polymorphic, and **small-scale benchmark tables**
- **zeek database**: Security log data (DNS, connections)
- **data_security database**: Security graph example (users, groups, files, folders)
- **ldbc database**: LDBC SNB benchmark tables (if separately loaded)

**Test data includes**:
- **Benchmark data**: 8 users, 5 posts, 10 follows, 8 likes (users_bench, posts_bench tables)
- Integration test fixtures: 8 users, 5 follows, 5 products  
- Filesystem data: 10 objects, 9 parent relationships  
- Group membership: 5 groups, 11 memberships
- Polymorphic interactions: 16 multi-type relationships
- Multi-tenant data: 3 tenants, 8 users, 7 orders
- Zeek logs: DNS and connection records
- Security graph: Users, groups, files, folders with access control

**Note**: Small-scale benchmark data (~8 users) is auto-loaded for tests. For large-scale performance benchmarks, use:
```bash
# Generate large benchmark data (1K-5M users) for performance testing
python3 benchmarks/social_network/data/setup_unified.py --scale 1  # 1K users minimum
```

### Multi-Schema Architecture (Dec 26, 2025)

**Schema Configuration**: Tests now use a **multi-schema architecture** for better isolation:

**Primary Multi-Schema Config** (`schemas/test/unified_test_multi_schema.yaml`):
- Loaded automatically by ClickGraph server via `GRAPH_CONFIG_PATH` environment variable
- Contains 6 isolated schemas:
  - `social_benchmark` - Social network benchmark (users_bench, posts_bench in brahmand DB)
  - `test_fixtures` - Core test fixtures (TestUser, TestProduct in brahmand DB)
  - `ldbc_snb` - LDBC SNB benchmark (Person, Comment, Forum in ldbc DB)
  - `denormalized_flights` - Denormalized flight data (travel DB)
  - `zeek_logs` - Network security logs (zeek DB)
  - `pattern_comp` - Pattern comprehension tests (brahmand DB)

**Additional Schemas** (loaded separately by conftest.py):
- `data_security` - Security graph (data_security DB)
- `ontime_flights` - OnTime flight benchmark (default DB)
- `property_expressions` - Property expression tests (test_integration DB)
- `group_membership` - Group membership tests (test_integration DB)
- `multi_tenant` - Multi-tenant parameterized views (brahmand DB)

**Schema Usage in Tests**:
- Tests use explicit `USE <schema_name>` clauses or `schema_name` parameter
- Each schema is isolated - no cross-schema interference
- Example: `USE social_benchmark MATCH (u:User) RETURN u.name`
- Default schema (if no USE clause): `social_benchmark`

### Test Categories

**Integration Tests** (`tests/integration/`):
- **Wiki Tests** (60): Cypher patterns from documentation
- **Matrix Tests** (2400+): Comprehensive query validation
- **Feature Tests**: Specific functionality (VLP, shortest paths, OPTIONAL MATCH, etc.)

**Unit Tests** (`cargo test`):
- Parser tests
- Query planner tests
- SQL generation tests

### Common Test Commands

```bash
# Run tests with specific markers
pytest tests/integration/ -k "not matrix"     # Exclude matrix tests
pytest tests/integration/ -m "not slow"        # Exclude slow tests

# Run with output
pytest tests/integration/ -v                   # Verbose
pytest tests/integration/ -vv                  # Extra verbose
pytest tests/integration/ -s                   # Show print statements

# Run with coverage
pytest tests/integration/ --cov=src --cov-report=html

# Stop on first failure
pytest tests/integration/ -x

# Show only failures
pytest tests/integration/ --tb=short
pytest tests/integration/ --tb=line
```

### Test Data Reset

If tests are failing due to corrupted data:
```bash
# Drop all test databases
curl -s "http://localhost:8123/" --user "test_user:test_pass" -d "DROP DATABASE IF EXISTS test_integration"
curl -s "http://localhost:8123/" --user "test_user:test_pass" -d "DROP DATABASE IF EXISTS brahmand"
curl -s "http://localhost:8123/" --user "test_user:test_pass" -d "DROP DATABASE IF EXISTS zeek"

# Reload all test data
bash scripts/test/setup_all_test_data.sh
```

### Environment Variables

```bash
# ClickHouse connection
export CLICKHOUSE_URL="http://localhost:8123"
export CLICKHOUSE_USER="test_user"
export CLICKHOUSE_PASSWORD="test_pass"

# ClickGraph server (for manual testing)
export CLICKGRAPH_URL="http://localhost:8080"

# Graph schema (for testing - multi-schema config)
export GRAPH_CONFIG_PATH="./schemas/test/unified_test_multi_schema.yaml"

# Or for manual queries (single schema):
# export GRAPH_CONFIG_PATH="./benchmarks/social_network/schemas/social_benchmark.yaml"
```

### Troubleshooting

**Tests hang or timeout**:
- Check if ClickGraph server is already running (kill it first)
- Check Docker container: `docker-compose ps`

**Schema errors**:
- Ensure test data is loaded: `bash scripts/test/setup_all_test_data.sh`
- Check ClickHouse tables: `curl http://localhost:8123 --user test_user:test_pass -d "SHOW TABLES FROM test_integration"`

**Connection refused**:
- Start ClickHouse: `docker-compose up -d`
- Wait for health check: `docker-compose ps` (should show "healthy")

### Test Infrastructure Files

**Primary Scripts**:
- `scripts/test/setup_all_test_data.sh` - **Main test data setup script**
- `tests/conftest.py` - Pytest fixtures and server management

**Test Data Files**:
- `tests/fixtures/data/test_integration_data.sql` - Core test tables
- `tests/fixtures/data/filesystem_test_data.sql` - Filesystem schema tests
- `tests/fixtures/data/group_membership_test_data.sql` - Group membership tests
- `tests/fixtureunified_test_multi_schema.yaml` - **PRIMARY test schema config** (6 schemas)
- `schemas/test/*.yaml` - Individual test schemas (loaded by conftest.py)
- `benchmarks/social_network/schemas/` - Benchmark schemas

### Current Test Status (Dec 26, 2025)

- **Total Integration Tests**: 3581
- **Passing**: ~2600+ (73%)
- **Known Issues**: Multi-hop 3+ patterns, VLP edge cases, some matrix tests

**Multi-Schema Migration** (Dec 26, 2025):
- Migrated from single `unified_test_schema.yaml` to multi-schema architecture
- All tests now use explicit `USE <schema_name>` clauses or `schema_name` parameters
- 6 primary schemas in unified multi-schema config + 9 additional schemas loaded by conftest
- Tests properly isolated by schema - no cross-contamination
- **Total Integration Tests**: 3341
- **Passing**: ~2600 (78%)
- **Known Issues**: Multi-hop 3+ patterns, VLP edge cases

See `STATUS.md` for detailed test statistics and known issues.
