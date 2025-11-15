# E2E Test Framework

## Overview

Organized, self-contained E2E testing framework for ClickGraph with automatic schema registration, data setup/teardown, and independent test buckets.

## Structure

```
tests/
├── e2e_framework.py          # Core framework and pytest fixtures
├── e2e/
│   ├── test_param_func_e2e.py   # Parameter + function E2E tests
│   ├── test_[feature]_e2e.py    # Other feature tests
│   └── buckets/              # Test bucket definitions
│       ├── param_func/       # Parameter + function bucket
│       │   ├── schema.yaml   # Graph schema definition
│       │   ├── setup.sql     # Table creation + data insertion
│       │   └── teardown.sql  # Cleanup script
│       └── [feature]/        # Other feature buckets
```

## Test Buckets

Each test bucket is **self-contained** with:
- **schema.yaml**: Graph schema mapping (nodes, relationships, properties)
- **setup.sql**: ClickHouse table creation and test data insertion
- **teardown.sql**: Cleanup script (drops tables)

### Available Buckets

1. **param_func** - Parameter + Function Integration
   - Database: `test_param_func`
   - Tables: `users`, `products`, `orders`
   - Tests: Parameter substitution with Neo4j functions

## Running Tests

### Run All E2E Tests
```bash
pytest tests/e2e/ -v
```

### Run Specific Test Suite
```bash
pytest tests/e2e/test_param_func_e2e.py -v
```

### Debug Mode (Preserve Data)
```bash
# Data will NOT be cleaned up after tests - useful for debugging
CLICKGRAPH_DEBUG=1 pytest tests/e2e/test_param_func_e2e.py -v

# Manually inspect data in ClickHouse
docker exec clickhouse clickhouse-client --database test_param_func --query "SELECT * FROM users"

# Force cleanup when done debugging
CLICKGRAPH_DEBUG=0 pytest tests/e2e/test_param_func_e2e.py --setup-only
```

### Run Specific Test
```bash
pytest tests/e2e/test_param_func_e2e.py::TestParameterFunctionBasics::test_function_in_return_with_parameter_filter -v
```

## Prerequisites

### 1. ClickHouse Running
```bash
docker start clickhouse
# Or: docker-compose up -d
```

### 2. ClickGraph Server Running
```bash
# Set environment variables
$env:CLICKHOUSE_URL="http://localhost:8123"
$env:CLICKHOUSE_USER="test_user"
$env:CLICKHOUSE_PASSWORD="test_pass"
$env:CLICKHOUSE_DATABASE="default"
$env:RUST_LOG="info"

# Start server
cargo run --release --bin clickgraph -- --http-port 8080

# Or use background job (Windows)
$job = Start-Job -ScriptBlock { 
    Set-Location "C:\Users\GenZ\clickgraph"
    $env:CLICKHOUSE_URL="http://localhost:8123"
    $env:CLICKHOUSE_USER="test_user"  
    $env:CLICKHOUSE_PASSWORD="test_pass"
    & ".\target\release\clickgraph.exe" --http-port 8080 
}
```

### 3. Python Dependencies
```bash
pip install pytest requests clickhouse-connect pyyaml
```

## Creating New Test Buckets

### 1. Create Bucket Directory
```bash
mkdir tests/e2e/buckets/my_feature
```

### 2. Create Schema (schema.yaml)
```yaml
name: my_feature_schema
version: "1.0"
description: "Schema for my feature tests"

graph_schema:
  nodes:
    - label: MyNode
      database: test_my_feature
      table: my_nodes
      id_column: id
      property_mappings:
        id: id
        name: name
```

### 3. Create Setup SQL (setup.sql)
```sql
-- Create tables and insert test data
CREATE TABLE IF NOT EXISTS test_my_feature.my_nodes (
    id UInt32,
    name String
) ENGINE = Memory;

INSERT INTO test_my_feature.my_nodes VALUES
    (1, 'Test Node 1'),
    (2, 'Test Node 2');
```

### 4. Create Teardown SQL (teardown.sql)
```sql
-- Clean up tables
DROP TABLE IF EXISTS test_my_feature.my_nodes;
```

### 5. Create Test File (test_my_feature_e2e.py)
```python
import pytest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from e2e_framework import TestBucket, clickgraph_client, e2e_framework

BUCKET_DIR = Path(__file__).parent / "buckets" / "my_feature"
MY_FEATURE_BUCKET = {
    "name": "my_feature",
    "database": "test_my_feature",
    "schema_file": BUCKET_DIR / "schema.yaml",
    "setup_sql": BUCKET_DIR / "setup.sql",
    "teardown_sql": BUCKET_DIR / "teardown.sql"
}

@pytest.fixture(scope="module")
def my_feature_bucket(e2e_framework):
    bucket = TestBucket(**MY_FEATURE_BUCKET)
    if not e2e_framework.setup_bucket(bucket):
        pytest.fail("Failed to setup bucket")
    yield bucket
    e2e_framework.teardown_bucket(bucket)

def test_my_feature(clickgraph_client, my_feature_bucket):
    result = clickgraph_client.query_json(
        "MATCH (n:MyNode) RETURN n.name",
        schema_name=my_feature_bucket.schema_name
    )
    assert len(result["data"]) == 2
```

## Framework Features

### Automatic Setup
- ✅ Creates ClickHouse database
- ✅ Runs setup SQL (tables + data)
- ✅ Registers schema with ClickGraph
- ✅ Verifies server is ready

### Automatic Teardown
- ✅ Runs teardown SQL
- ✅ Drops test database
- ✅ Cleans up resources
- ⚠️ **Skipped in DEBUG_MODE** for debugging

### Pytest Fixtures

#### `e2e_framework` (session-scoped)
Core framework instance for entire test session.

#### `test_bucket` (function-scoped)
Creates temporary test buckets for individual tests.

#### `clickgraph_client`
Simple client for querying ClickGraph:
```python
def test_example(clickgraph_client):
    result = clickgraph_client.query_json(
        "MATCH (n) RETURN n",
        parameters={"param": "value"},
        schema_name="my_schema"
    )
```

#### `clickhouse_client`
Direct ClickHouse client access for advanced queries.

## Best Practices

### 1. Use Module-Scoped Buckets
```python
@pytest.fixture(scope="module")
def my_bucket(e2e_framework):
    # Bucket persists for entire test module
    # Reduces setup/teardown overhead
```

### 2. Use Memory Engine for Speed
```sql
CREATE TABLE ... ENGINE = Memory;
-- Fast, no disk I/O, perfect for tests
```

### 3. Use DEBUG Mode for Development
```bash
# Preserve data for inspection
CLICKGRAPH_DEBUG=1 pytest tests/e2e/test_my_feature_e2e.py -v

# Check data in ClickHouse
docker exec clickhouse clickhouse-client --database test_my_feature

# Clean up manually when done
docker exec clickhouse clickhouse-client --query "DROP DATABASE test_my_feature"
```

### 4. Test Realistic Query Patterns
- Use `MATCH` patterns (not standalone `RETURN`)
- Test relationship traversals
- Include WHERE filters with parameters
- Test aggregations and functions

### 5. Keep Test Data Small
- Minimal data for each test case
- Fast setup/teardown
- Easy to understand test scenarios

## Troubleshooting

### "ClickGraph server not running"
```bash
# Start server first
cargo run --release --bin clickgraph

# Or check if already running
curl http://localhost:8080/health
```

### "Failed to setup test bucket"
- Check ClickHouse is running: `docker ps | grep clickhouse`
- Check database name doesn't already exist
- Verify schema.yaml is valid YAML
- Check setup.sql for syntax errors

### Tests Pass But Data Missing
- Check DEBUG_MODE isn't enabled unintentionally
- Verify schema_name matches registered schema
- Check table names in queries match schema.yaml

### Schema Not Registered
- Check schema.yaml path is correct
- Verify ClickGraph server is running
- Check server logs: `Receive-Job -Id <job_id> -Keep`

## Environment Variables

- `CLICKGRAPH_URL` - Server URL (default: `http://localhost:8080`)
- `CLICKHOUSE_HOST` - ClickHouse host (default: `localhost`)
- `CLICKHOUSE_PORT` - ClickHouse port (default: `8123`)
- `CLICKHOUSE_USER` - ClickHouse username (default: `test_user`)
- `CLICKHOUSE_PASSWORD` - ClickHouse password (default: `test_pass`)
- `CLICKGRAPH_DEBUG` - Skip teardown if `1` (default: `0`)

## Future Enhancements

- [ ] Parallel test execution with isolated databases
- [ ] Test data generators/fixtures
- [ ] Performance benchmarking within E2E tests
- [ ] Snapshot testing for query results
- [ ] Auto-generated test reports with coverage


