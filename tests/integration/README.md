# Integration Tests

Comprehensive integration tests for ClickGraph functionality.

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Start Required Services

```bash
# Start ClickHouse and ClickGraph
docker-compose up -d

# OR run natively:
# Terminal 1: Start ClickHouse
docker-compose up -d clickhouse

# Terminal 2: Start ClickGraph
cargo run --bin clickgraph
```

### 3. Configure Environment (Optional)

```bash
# Default values work for docker-compose setup
export CLICKGRAPH_URL="http://localhost:8080"
export CLICKHOUSE_HOST="localhost"
export CLICKHOUSE_PORT="8123"
export CLICKHOUSE_USER="test_user"
export CLICKHOUSE_PASSWORD="test_pass"
```

## Running Tests

### Run All Tests

```bash
pytest tests/integration/
```

### Run Specific Test File

```bash
pytest tests/integration/test_basic_queries.py
```

### Run Specific Test Class

```bash
pytest tests/integration/test_basic_queries.py::TestBasicMatch
```

### Run Specific Test

```bash
pytest tests/integration/test_basic_queries.py::TestBasicMatch::test_match_all_nodes
```

### Run with Coverage

```bash
pytest tests/integration/ --cov=brahmand --cov-report=html
```

### Run with Verbose Output

```bash
pytest tests/integration/ -v
```

### Run with Output Capture Disabled

```bash
pytest tests/integration/ -s
```

## Test Structure

```
tests/integration/
├── conftest.py                     # Pytest configuration and fixtures
├── requirements.txt                # Python dependencies
├── README.md                       # This file
├── test_basic_queries.py          # Basic MATCH, WHERE, RETURN tests
├── test_relationships.py          # Relationship traversal tests
├── test_variable_length_paths.py  # Variable-length path tests
├── test_shortest_paths.py         # Shortest path algorithm tests
├── test_optional_match.py         # OPTIONAL MATCH tests
├── test_aggregations.py           # Aggregation function tests
├── test_case_expressions.py       # CASE expression tests
├── test_path_variables.py         # Path variable tests
├── test_multi_database.py         # Multi-database tests
└── test_error_handling.py         # Error handling tests
```

## Fixtures

### Database Fixtures

- `clickhouse_client`: ClickHouse client for direct database access
- `test_database`: Test database name
- `clean_database`: Cleans test database before each test
- `simple_graph`: Creates a simple 5-node graph with relationships

### Helper Functions

- `execute_cypher(query, schema_name)`: Execute Cypher query via HTTP API
- `assert_query_success(response)`: Assert query executed successfully
- `assert_row_count(response, expected)`: Assert result row count
- `assert_column_exists(response, column)`: Assert column present
- `assert_contains_value(response, column, value)`: Assert column contains value

## Writing New Tests

### Basic Test Template

```python
def test_my_feature(simple_graph):
    """Test description."""
    response = execute_cypher(
        "MATCH (n:User) RETURN n.name",
        schema_name=simple_graph["database"]
    )
    
    assert_query_success(response)
    assert_row_count(response, expected_count)
    assert_contains_value(response, "n.name", "Alice")
```

### Custom Graph Fixture

```python
@pytest.fixture
def my_custom_graph(clickhouse_client, test_database, clean_database):
    """Create custom graph structure."""
    # Create tables
    clickhouse_client.command(f"""
        CREATE TABLE {test_database}.my_table (
            id UInt32,
            value String
        ) ENGINE = Memory
    """)
    
    # Insert data
    clickhouse_client.command(f"""
        INSERT INTO {test_database}.my_table VALUES
            (1, 'test')
    """)
    
    return {"database": test_database, ...}
```

## Test Coverage Goals

- [x] Basic query patterns (MATCH, WHERE, RETURN)
- [ ] Relationship traversals
- [ ] Variable-length paths
- [ ] Shortest path algorithms
- [ ] OPTIONAL MATCH patterns
- [ ] Aggregation functions
- [ ] CASE expressions
- [ ] Path variables and functions
- [ ] Multi-database support
- [ ] Error handling

## Troubleshooting

### ClickGraph Not Running

```
Error: ClickGraph server not responding
```

**Solution**: Start ClickGraph server:
```bash
cargo run --bin clickgraph
```

### ClickHouse Connection Error

```
Error: Could not connect to ClickHouse
```

**Solution**: Start ClickHouse:
```bash
docker-compose up -d clickhouse
```

### Test Database Permissions

```
Error: Access denied for user
```

**Solution**: Check ClickHouse user permissions in docker-compose.yaml

### Stale Test Data

```
Error: Unexpected row count
```

**Solution**: Tests should use `clean_database` fixture to ensure clean state

## Continuous Integration

These tests are designed to run in CI/CD pipelines:

```yaml
# .github/workflows/test.yml
- name: Run Integration Tests
  run: |
    docker-compose up -d
    pytest tests/integration/ -v
```

## Performance Expectations

- Basic queries: < 100ms
- Relationship traversals: < 500ms
- Variable-length paths: < 2s
- Aggregations: < 1s

If tests run slower, check:
1. ClickHouse performance
2. Network latency
3. Dataset size

## Contributing

When adding new features to ClickGraph:

1. Add integration tests to appropriate test file
2. Use existing fixtures when possible
3. Follow naming conventions: `test_<feature>_<scenario>`
4. Add docstrings explaining what's being tested
5. Run full test suite before submitting PR
