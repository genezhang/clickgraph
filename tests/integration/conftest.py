"""
pytest configuration and fixtures for integration tests.

This module provides:
- ClickHouse connection management
- Test database setup/teardown
- ClickGraph server communication utilities
- Common test data fixtures
"""

import pytest
import requests
import clickhouse_connect
import time
import os
from typing import Dict, Any, List


# Configuration
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "test_user")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "test_pass")


@pytest.fixture(scope="session")
def clickhouse_client():
    """Provides a ClickHouse client for the entire test session."""
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD
    )
    yield client
    client.close()


@pytest.fixture(scope="session")
def test_database():
    """Returns the test database name."""
    return "test_integration"


@pytest.fixture(scope="session", autouse=True)
def setup_test_database(clickhouse_client, test_database):
    """Create test database at the start of test session."""
    # Create database if not exists
    clickhouse_client.command(f"CREATE DATABASE IF NOT EXISTS {test_database}")
    yield
    # Cleanup is optional - comment out to inspect data after tests
    # clickhouse_client.command(f"DROP DATABASE IF EXISTS {test_database}")


@pytest.fixture
def clean_database(clickhouse_client, test_database):
    """Clean all tables in test database before each test."""
    # Get all tables in test database
    tables = clickhouse_client.query(
        f"SELECT name FROM system.tables WHERE database = '{test_database}'"
    ).result_rows
    
    # Drop all tables
    for (table_name,) in tables:
        clickhouse_client.command(f"DROP TABLE IF EXISTS {test_database}.{table_name}")
    
    yield
    
    # Optional: Clean after test as well
    tables = clickhouse_client.query(
        f"SELECT name FROM system.tables WHERE database = '{test_database}'"
    ).result_rows
    for (table_name,) in tables:
        clickhouse_client.command(f"DROP TABLE IF EXISTS {test_database}.{table_name}")


def execute_cypher(query: str, schema_name: str = "default") -> Dict[str, Any]:
    """
    Execute a Cypher query via ClickGraph HTTP API.
    
    Args:
        query: Cypher query string
        schema_name: Schema/database name to query
        
    Returns:
        Response JSON with results, columns, and performance metrics
        
    Raises:
        requests.HTTPError: If query execution fails
    """
    response = requests.post(
        f"{CLICKGRAPH_URL}/query",
        json={"query": query, "schema_name": schema_name},
        headers={"Content-Type": "application/json"}
    )
    response.raise_for_status()
    return response.json()


def wait_for_clickgraph(timeout: int = 30) -> bool:
    """
    Wait for ClickGraph server to be ready.
    
    Args:
        timeout: Maximum seconds to wait
        
    Returns:
        True if server is ready, False if timeout
    """
    start = time.time()
    while time.time() - start < timeout:
        try:
            response = requests.post(
                f"{CLICKGRAPH_URL}/query",
                json={"query": "RETURN 1 as test"},
                timeout=5
            )
            if response.status_code == 200:
                return True
        except requests.exceptions.RequestException:
            time.sleep(1)
    return False


@pytest.fixture(scope="session", autouse=True)
def verify_clickgraph_running():
    """Verify ClickGraph server is running before tests."""
    if not wait_for_clickgraph():
        pytest.fail(
            f"ClickGraph server not responding at {CLICKGRAPH_URL}. "
            "Please start the server before running integration tests."
        )


@pytest.fixture
def simple_graph(clickhouse_client, test_database, clean_database):
    """
    Create a simple graph with users and follows relationships.
    
    Schema:
        - users: user_id, name, age
        - follows: follower_id, followed_id, since
        
    Data:
        - 5 users (Alice, Bob, Charlie, Diana, Eve)
        - 6 follow relationships forming a small social network
    """
    # Create users table
    clickhouse_client.command(f"""
        CREATE TABLE {test_database}.users (
            user_id UInt32,
            name String,
            age UInt32
        ) ENGINE = Memory
    """)
    
    # Create follows table
    clickhouse_client.command(f"""
        CREATE TABLE {test_database}.follows (
            follower_id UInt32,
            followed_id UInt32,
            since String
        ) ENGINE = Memory
    """)
    
    # Insert test data
    clickhouse_client.command(f"""
        INSERT INTO {test_database}.users VALUES
            (1, 'Alice', 30),
            (2, 'Bob', 25),
            (3, 'Charlie', 35),
            (4, 'Diana', 28),
            (5, 'Eve', 32)
    """)
    
    clickhouse_client.command(f"""
        INSERT INTO {test_database}.follows VALUES
            (1, 2, '2023-01-01'),  -- Alice follows Bob
            (1, 3, '2023-01-15'),  -- Alice follows Charlie
            (2, 3, '2023-02-01'),  -- Bob follows Charlie
            (3, 4, '2023-02-15'),  -- Charlie follows Diana
            (4, 5, '2023-03-01'),  -- Diana follows Eve
            (2, 4, '2023-03-15')   -- Bob follows Diana
    """)
    
    # Return schema configuration
    return {
        "database": test_database,
        "nodes": {
            "User": {
                "table": "users",
                "id_column": "user_id",
                "properties": ["name", "age"]
            }
        },
        "relationships": {
            "FOLLOWS": {
                "table": "follows",
                "from_id": "follower_id",
                "to_id": "followed_id",
                "properties": ["since"]
            }
        }
    }


@pytest.fixture
def create_graph_schema(clickhouse_client, test_database):
    """
    Helper to create YAML schema file for a graph.
    
    Usage:
        schema_path = create_graph_schema(
            nodes={...},
            relationships={...}
        )
    """
    import yaml
    import tempfile
    
    def _create_schema(nodes: Dict, relationships: Dict, name: str = "test_graph") -> str:
        """Create YAML schema file and return path."""
        schema = {
            "name": name,
            "version": "1.0",
            "views": [{
                "name": test_database,
                "nodes": {},
                "relationships": {}
            }]
        }
        
        # Add nodes
        for label, config in nodes.items():
            schema["views"][0]["nodes"][label] = {
                "source_table": config["table"],
                "id_column": config["id_column"],
                "property_mappings": {
                    prop: prop for prop in config.get("properties", [])
                }
            }
        
        # Add relationships
        for rel_type, config in relationships.items():
            schema["views"][0]["relationships"][rel_type] = {
                "source_table": config["table"],
                "from_node": config["from_node"],
                "to_node": config["to_node"],
                "from_id": config["from_id"],
                "to_id": config["to_id"]
            }
        
        # Write to temp file
        fd, path = tempfile.mkstemp(suffix='.yaml')
        with os.fdopen(fd, 'w') as f:
            yaml.dump(schema, f)
        
        return path
    
    return _create_schema


# Assertion helpers

def assert_query_success(response: Dict[str, Any]):
    """Assert that a query response indicates success."""
    assert "error" not in response, f"Query failed: {response.get('error')}"
    assert "results" in response, "Response missing 'results' field"


def assert_row_count(response: Dict[str, Any], expected: int):
    """Assert that query returns expected number of rows."""
    assert_query_success(response)
    actual = len(response["results"])
    assert actual == expected, f"Expected {expected} rows, got {actual}"


def assert_column_exists(response: Dict[str, Any], column: str):
    """Assert that response contains a specific column."""
    assert_query_success(response)
    columns = response.get("columns", [])
    assert column in columns, f"Column '{column}' not found. Available: {columns}"


def assert_contains_value(response: Dict[str, Any], column: str, value: Any):
    """Assert that a column contains a specific value."""
    assert_query_success(response)
    results = response["results"]
    
    # Handle both dict and list result formats
    if results and isinstance(results[0], dict):
        values = [row.get(column) for row in results]
    else:
        # List format - need column index
        col_idx = response["columns"].index(column)
        values = [row[col_idx] for row in results]
    
    assert value in values, f"Value '{value}' not found in column '{column}'. Values: {values}"
