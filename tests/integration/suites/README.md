# Integration Test Suites

## Overview

Self-contained integration test suites for ClickGraph. Each suite includes its own schema, data setup, and teardown scripts.

## Directory Structure

```
tests/integration/suites/
├── suite_manager.py              # Setup/teardown automation script
├── social_benchmark/             # Benchmark schema tests
│   ├── schema.yaml              # Graph schema (nodes, relationships)
│   ├── setup.sql                # Create tables + insert data
│   └── teardown.sql             # Drop tables
├── test_integration/            # Basic integration tests
│   ├── schema.yaml
│   ├── setup.sql
│   └── teardown.sql
├── optional_match/              # OPTIONAL MATCH (LEFT JOIN) tests
│   ├── schema.yaml
│   ├── setup.sql
│   └── teardown.sql
├── variable_paths/              # Variable-length path patterns (*2, *1..3)
│   ├── schema.yaml
│   ├── setup.sql
│   └── teardown.sql
└── shortest_paths/              # shortestPath() function tests
    ├── schema.yaml
    ├── setup.sql
    └── teardown.sql
```

## Available Suites

### 1. **social_benchmark**
- **Database**: `brahmand`
- **Purpose**: Benchmark schema for performance testing and comprehensive query patterns
- **Tables**: 
  - `users_bench` (users with profiles)
  - `user_follows_bench` (follow relationships)
  - `posts_bench` (user posts)
  - `post_likes_bench` (post likes)
  - `friendships` (friendship relationships)
  - `zeek_logs` (for array testing)
- **Use Cases**: Multi-hop traversals, relationship patterns, aggregations

### 2. **test_integration**
- **Database**: `test_integration`
- **Purpose**: Basic integration testing with simple graph patterns
- **Tables**:
  - `users` (5 users: Alice, Bob, Charlie, Diana, Eve)
  - `follows` (path testing: Alice→Bob→Diana→Eve)
  - `products` (e-commerce items)
  - `purchases` (user purchases)
  - `friendships` (user friendships)
- **Use Cases**: Simple queries, path testing, basic traversals

### 3. **optional_match**
- **Database**: `test_optional_match`
- **Purpose**: Testing OPTIONAL MATCH (LEFT JOIN semantics)
- **Tables**:
  - `users` (some with follows/posts, some without)
  - `follows` (sparse relationships)
  - `posts` (only some users have posts)
- **Use Cases**: LEFT JOIN behavior, null handling, optional patterns

### 4. **variable_paths**
- **Database**: `test_vlp`
- **Purpose**: Variable-length path pattern testing (`*2`, `*1..3`, `*..5`)
- **Tables**:
  - `users` (6 users in a chain)
  - `follows` (creates 5-hop chain + shortcuts)
- **Use Cases**: Variable-length paths, recursive CTEs, path counting

### 5. **shortest_paths**
- **Database**: `test_shortest`
- **Purpose**: Testing `shortestPath()` and `allShortestPaths()` functions
- **Tables**:
  - `users` (5 users)
  - `follows` (multiple paths of different lengths)
- **Use Cases**: Shortest path algorithms, graph distance calculations

## Quick Start

### Prerequisites

Make sure ClickGraph server and ClickHouse are running:

```bash
# Check ClickGraph is running
curl http://localhost:8080/health

# Set environment variables (if needed)
export CLICKHOUSE_HOST="localhost"
export CLICKHOUSE_PORT="8123"
export CLICKHOUSE_USER="default"
export CLICKHOUSE_PASSWORD="default"
export CLICKGRAPH_URL="http://localhost:8080"
```

### Setup All Suites

```bash
# From the suites directory
cd tests/integration/suites
python suite_manager.py setup-all
```

Output:
```
🚀 Setting up 5 test suites...

🚀 Setting up suite: optional_match
  ✓ Database 'test_optional_match' created
  ✓ Tables created and data inserted
  ✓ Schema 'optional_match_test' registered
✅ Suite 'optional_match' ready!

...

✅ Setup complete: 5/5 suites ready
```

### Setup Specific Suite

```bash
python suite_manager.py setup social_benchmark
```

### Teardown Specific Suite

```bash
python suite_manager.py teardown social_benchmark
```

### Teardown All Suites

```bash
python suite_manager.py teardown-all
```

### List Available Suites

```bash
python suite_manager.py list
```

Output:
```
📦 Available test suites (5):

  • optional_match      - Schema for testing OPTIONAL MATCH LEFT JOIN semantics
  • shortest_paths      - Schema for testing shortestPath() and allShortestPaths() functions
  • social_benchmark    - Benchmark schema for social network analysis
  • test_integration    - Integration test schema for ClickGraph test suite
  • variable_paths      - Schema for testing variable-length path patterns (*2, *1..3, etc.)
```

## Running Tests

### Run All Integration Tests (After Setup)

```bash
cd /home/gz/clickgraph

# With proper credentials
export CLICKHOUSE_USER="default"
export CLICKHOUSE_PASSWORD="default"
export CLICKHOUSE_HOST="localhost"
export CLICKHOUSE_PORT="8123"
export CLICKGRAPH_URL="http://localhost:8080"

# Run pytest
pytest tests/integration/ -v
```

### Run Tests for Specific Suite

```bash
# After setting up social_benchmark suite
pytest tests/integration/test_basic_queries.py -v
pytest tests/integration/query_patterns/test_pattern_matrix.py::TestStandardSchema -v
```

### Run with Coverage

```bash
pytest tests/integration/ --cov=src --cov-report=html
```

## Creating New Suites

### 1. Create Suite Directory

```bash
mkdir tests/integration/suites/my_feature
```

### 2. Create schema.yaml

```yaml
name: my_feature_test
version: "1.0"
description: "My feature test schema"

graph_schema:
  nodes:
    - label: MyNode
      database: test_my_feature
      table: my_table
      node_id: id
      property_mappings:
        id: id
        name: name
  
  relationships:
    - type: MY_REL
      database: test_my_feature
      table: my_relationships
      from_id: from_id
      to_id: to_id
      from_node: MyNode
      to_node: MyNode
```

### 3. Create setup.sql

```sql
-- Setup SQL for My Feature Tests
-- Database: test_my_feature

CREATE TABLE IF NOT EXISTS test_my_feature.my_table (
    id UInt32,
    name String
) ENGINE = Memory;

CREATE TABLE IF NOT EXISTS test_my_feature.my_relationships (
    from_id UInt32,
    to_id UInt32
) ENGINE = Memory;

INSERT INTO test_my_feature.my_table VALUES
    (1, 'Node1'),
    (2, 'Node2');

INSERT INTO test_my_feature.my_relationships VALUES
    (1, 2);
```

### 4. Create teardown.sql

```sql
-- Teardown SQL for My Feature Tests

DROP TABLE IF EXISTS test_my_feature.my_relationships;
DROP TABLE IF EXISTS test_my_feature.my_table;
```

### 5. Test Your Suite

```bash
python suite_manager.py setup my_feature
python suite_manager.py teardown my_feature
```

## Best Practices

### Suite Design

1. **Keep suites focused**: Each suite should test one feature/pattern
2. **Use Memory engine**: Fast, non-persistent (perfect for tests)
3. **Minimal data**: Use just enough data to test the feature
4. **Clear naming**: Tables should indicate their purpose

### Data Setup

1. **Predictable data**: Use simple patterns (e.g., numbers 1-5)
2. **Test edge cases**: Include nulls, empty relationships, etc.
3. **Document patterns**: Comment SQL to explain the graph structure

### Teardown

1. **Always provide teardown.sql**: Clean up after tests
2. **Drop in reverse order**: Drop dependent tables first
3. **Use IF EXISTS**: Avoid errors if table doesn't exist

## Troubleshooting

### Suite setup fails with "Authentication failed"

**Solution**: Check your ClickHouse credentials:
```bash
export CLICKHOUSE_USER="default"
export CLICKHOUSE_PASSWORD="default"
```

### Schema registration fails

**Solution**: Make sure ClickGraph server is running:
```bash
curl http://localhost:8080/health
```

### Tables already exist errors

**Solution**: Run teardown first:
```bash
python suite_manager.py teardown <suite_name>
python suite_manager.py setup <suite_name>
```

### Tests can't find schema

**Solution**: Make sure the schema is loaded:
```bash
curl http://localhost:8080/schemas | jq '.schemas[] | .name'
```

## Integration with pytest

### Using Suite Fixtures

The integration test suite can use pytest fixtures defined in `conftest.py`:

```python
import pytest

@pytest.fixture(scope="session", autouse=True)
def setup_all_suites():
    """Auto-setup all suites before running tests."""
    import subprocess
    subprocess.run([
        "python",
        "tests/integration/suites/suite_manager.py",
        "setup-all"
    ], check=True)
```

### Suite-Specific Tests

Create test files that depend on specific suites:

```python
# tests/integration/test_optional_match.py

import pytest

@pytest.fixture(scope="module")
def optional_match_suite():
    """Ensure optional_match suite is set up."""
    # Setup code here
    yield
    # Teardown code here

def test_optional_match_basic(optional_match_suite):
    # Your test code
    pass
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Integration Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Start ClickHouse
        run: docker-compose up -d clickhouse
      
      - name: Start ClickGraph
        run: docker-compose up -d clickgraph
      
      - name: Setup test suites
        run: |
          python tests/integration/suites/suite_manager.py setup-all
      
      - name: Run integration tests
        run: pytest tests/integration/ -v
      
      - name: Teardown test suites
        if: always()
        run: |
          python tests/integration/suites/suite_manager.py teardown-all
```

## Contributing

When adding new tests:

1. Identify which suite the test belongs to
2. If no suite fits, create a new one following the structure above
3. Document the test data and expected behavior
4. Add your test to the appropriate test file
5. Update this README if adding a new suite

## References

- [E2E Test Framework](../../e2e/README.md) - Similar structure for E2E tests
- [ClickGraph Documentation](../../../docs/)
- [Cypher Language Reference](../../../docs/cypher-reference.md)
