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
    
    # Print error details if request failed
    if response.status_code != 200:
        print(f"\nError Response ({response.status_code}):")
        print(f"Request: query={query}, schema_name={schema_name}")
        print(f"Response: {response.text}")
    
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
            # Just check health endpoint instead of running a query
            response = requests.get(
                f"{CLICKGRAPH_URL}/health",
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
    Create a complete test graph matching test_integration.yaml schema.
    
    Schema:
        - users: user_id, name, age
        - follows: follower_id, followed_id, since
        - products: product_id, name, price, category
        - purchases: user_id, product_id, purchase_date, quantity
        - friendships: user_id_1, user_id_2, since
        
    Data:
        - 5 users (Alice, Bob, Charlie, Diana, Eve)
        - 6 follow relationships forming a small social network
        - 3 products (Laptop, Mouse, Keyboard)
        - 5 purchase relationships
        - 3 friendship relationships
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
    
    # Create products table
    clickhouse_client.command(f"""
        CREATE TABLE {test_database}.products (
            product_id UInt32,
            name String,
            price Float32,
            category String
        ) ENGINE = Memory
    """)
    
    # Create purchases table
    clickhouse_client.command(f"""
        CREATE TABLE {test_database}.purchases (
            user_id UInt32,
            product_id UInt32,
            purchase_date String,
            quantity UInt32
        ) ENGINE = Memory
    """)
    
    # Create friendships table
    clickhouse_client.command(f"""
        CREATE TABLE {test_database}.friendships (
            user_id_1 UInt32,
            user_id_2 UInt32,
            since String
        ) ENGINE = Memory
    """)
    
    # Insert users
    clickhouse_client.command(f"""
        INSERT INTO {test_database}.users VALUES
            (1, 'Alice', 30),
            (2, 'Bob', 25),
            (3, 'Charlie', 35),
            (4, 'Diana', 28),
            (5, 'Eve', 32)
    """)
    
    # Insert follows relationships
    clickhouse_client.command(f"""
        INSERT INTO {test_database}.follows VALUES
            (1, 2, '2023-01-01'),
            (1, 3, '2023-01-15'),
            (2, 3, '2023-02-01'),
            (3, 4, '2023-02-15'),
            (4, 5, '2023-03-01'),
            (2, 4, '2023-03-15')
    """)
    
    # Insert products
    clickhouse_client.command(f"""
        INSERT INTO {test_database}.products VALUES
            (101, 'Laptop', 999.99, 'Electronics'),
            (102, 'Mouse', 29.99, 'Electronics'),
            (103, 'Keyboard', 79.99, 'Electronics')
    """)
    
    # Insert purchases
    clickhouse_client.command(f"""
        INSERT INTO {test_database}.purchases VALUES
            (1, 101, '2024-01-15', 1),
            (2, 102, '2024-01-20', 2),
            (3, 101, '2024-02-01', 1),
            (4, 103, '2024-02-10', 1),
            (1, 102, '2024-02-15', 3)
    """)
    
    # Insert friendships
    clickhouse_client.command(f"""
        INSERT INTO {test_database}.friendships VALUES
            (1, 2, '2022-05-10'),
            (3, 4, '2022-08-15'),
            (2, 5, '2023-01-20')
    """)
    
    # NOTE: Schema is already loaded by server at startup via GRAPH_CONFIG_PATH
    # The YAML schema name is "test_graph_schema" (intentionally different from DB name)
    # This ensures tests don't confuse schema name with database name
    
    # Return complete schema configuration matching test_integration.yaml
    return {
        "schema_name": "test_graph_schema",  # Logical schema identifier from YAML
        "database": "test_integration",      # Physical ClickHouse database where tables exist
        "nodes": {
            "User": {
                "table": "users",
                "id_column": "user_id",
                "properties": ["name", "age"]
            },
            "Product": {
                "table": "products",
                "id_column": "product_id",
                "properties": ["name", "price", "category"]
            }
        },
        "relationships": {
            "FOLLOWS": {
                "table": "follows",
                "from_id": "follower_id",
                "to_id": "followed_id",
                "from_node": "User",
                "to_node": "User",
                "properties": ["since"]
            },
            "PURCHASED": {
                "table": "purchases",
                "from_id": "user_id",
                "to_id": "product_id",
                "from_node": "User",
                "to_node": "Product",
                "properties": ["purchase_date", "quantity"]
            },
            "FRIENDS_WITH": {
                "table": "friendships",
                "from_id": "user_id_1",
                "to_id": "user_id_2",
                "from_node": "User",
                "to_node": "User",
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
    # Check for error field
    if isinstance(response, dict) and "error" in response:
        pytest.fail(f"Query failed: {response.get('error')}")
    
    # ClickGraph returns results as a list directly (not wrapped in {"results": [...]} object)
    # Accept both formats for compatibility
    if isinstance(response, list):
        # Direct list response - this is valid
        return
    elif isinstance(response, dict):
        # Dictionary response - check for results or error
        if "error" in response:
            pytest.fail(f"Query failed: {response.get('error')}")
        # If it's a dict, it should have results or be valid response
        return
    else:
        pytest.fail(f"Unexpected response type: {type(response)}, value: {response}")


def assert_row_count(response: Dict[str, Any], expected: int):
    """Assert that query returns expected number of rows."""
    assert_query_success(response)
    # Handle both list and dict responses
    if isinstance(response, list):
        actual = len(response)
    else:
        actual = len(response.get("results", []))
    assert actual == expected, f"Expected {expected} rows, got {actual}"


def assert_column_exists(response: Dict[str, Any], column: str):
    """
    Assert that response contains a specific column.
    
    Handles both prefixed (u.name) and unprefixed (name) column names.
    Checks both the original column name AND the normalized (unprefixed) version.
    """
    assert_query_success(response)
    
    # Normalize column name - strip alias prefix if present (e.g., "u.name" → "name")
    normalized_column = column.split('.')[-1] if '.' in column else column
    
    # For list responses, columns are embedded in the result dicts
    if isinstance(response, list):
        if response and isinstance(response[0], dict):
            # Check both original and normalized column names
            assert column in response[0] or normalized_column in response[0], (
                f"Column '{column}' (normalized: '{normalized_column}') not found in results. "
                f"Available: {list(response[0].keys())}"
            )
    else:
        # Check if results are embedded in dict
        results = response.get("results", [])
        if results and isinstance(results[0], dict):
            # Check both original and normalized column names
            assert column in results[0] or normalized_column in results[0], (
                f"Column '{column}' (normalized: '{normalized_column}') not found in results. "
                f"Available: {list(results[0].keys())}"
            )
        else:
            columns = response.get("columns", [])
            assert column in columns or normalized_column in columns, (
                f"Column '{column}' (normalized: '{normalized_column}') not found. "
                f"Available: {columns}"
            )


def assert_contains_value(response: Dict[str, Any], column: str, value: Any):
    """
    Assert that a column contains a specific value.
    
    Handles:
    - Column name normalization (checks both prefixed and unprefixed)
    - Type conversion for aggregation results (COUNT returns string, converts to int)
    """
    assert_query_success(response)
    
    # Normalize column name - strip alias prefix if present (e.g., "u.name" → "name")
    normalized_column = column.split('.')[-1] if '.' in column else column
    
    # Handle list response format
    if isinstance(response, list):
        results = response
    else:
        results = response.get("results", [])
    
    # Handle both dict and list result formats
    if results and isinstance(results[0], dict):
        # Try both original and normalized column names
        if column in results[0]:
            values = [row.get(column) for row in results]
        else:
            values = [row.get(normalized_column) for row in results]
    else:
        # List format - need column index
        if isinstance(response, dict):
            # Try both names
            try:
                col_idx = response["columns"].index(column)
            except ValueError:
                col_idx = response["columns"].index(normalized_column)
        else:
            raise ValueError("Cannot find column index for list-only response")
        values = [row[col_idx] for row in results]
    
    # Type conversion: if value is int and actual values are strings, convert
    # This handles COUNT(*) which ClickHouse JSONEachRow returns as string
    if isinstance(value, int) and values and isinstance(values[0], str):
        try:
            values = [int(v) for v in values]
        except (ValueError, TypeError):
            pass  # Keep original values if conversion fails
    
    assert value in values, (
        f"Value '{value}' not found in column '{column}' (normalized: '{normalized_column}'). "
        f"Values: {values}"
    )


def get_column_values(response: Dict[str, Any], column: str, convert_to_int: bool = False) -> List[Any]:
    """
    Extract values from a column in the response.
    
    Args:
        response: Query response
        column: Column name (can be prefixed like "u.name" or simple like "name")
        convert_to_int: If True, convert string values to int (useful for COUNT)
        
    Returns:
        List of values from the specified column
        
    Handles:
    - Column name normalization (checks both prefixed and unprefixed)
    - Type conversion for aggregation results
    """
    assert_query_success(response)
    
    # Normalize column name - strip alias prefix if present (e.g., "u.name" → "name")
    normalized_column = column.split('.')[-1] if '.' in column else column
    
    # Handle list response format
    if isinstance(response, list):
        results = response
    else:
        results = response.get("results", [])
    
    # Extract values
    if results and isinstance(results[0], dict):
        # Try both original and normalized column names
        if column in results[0]:
            values = [row.get(column) for row in results]
        else:
            values = [row.get(normalized_column) for row in results]
    else:
        # List format - need column index
        if isinstance(response, dict):
            col_idx = response["columns"].index(normalized_column)
        else:
            raise ValueError("Cannot find column index for list-only response")
        values = [row[col_idx] for row in results]
    
    # Type conversion if requested
    if convert_to_int:
        try:
            values = [int(v) if v is not None else None for v in values]
        except (ValueError, TypeError) as e:
            raise ValueError(f"Cannot convert column '{column}' values to int: {e}. Values: {values}")
    
    return values


def get_single_value(response: Dict[str, Any], column: str, convert_to_int: bool = False) -> Any:
    """
    Extract a single value from a single-row response.
    
    Args:
        response: Query response  
        column: Column name (can be prefixed like "u.name" or simple like "name")
        convert_to_int: If True, convert string value to int (useful for COUNT)
        
    Returns:
        The value from the specified column in the first (and presumably only) row
        
    Useful for aggregation queries that return a single row.
    """
    values = get_column_values(response, column, convert_to_int=convert_to_int)
    if not values:
        raise ValueError(f"No results found in response")
    return values[0]


