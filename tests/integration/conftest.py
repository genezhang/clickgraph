"""
pytest configuration and fixtures for integration tests.

This module provides:
- ClickHouse connection management
- Test database setup/teardown
- ClickGraph server communication utilities
- Common test data fixtures

MULTI-SCHEMA APPROACH (v0.6.1+):
- Default GRAPH_CONFIG_PATH points to schemas/test/unified_test_multi_schema.yaml
- All tests use explicit "USE <schema_name>" clause to select schema
- 6 schemas available: social_integration, test_fixtures, ldbc_snb, 
  denormalized_flights, pattern_comp, zeek_logs
- Complete schema isolation - no label conflicts between schemas
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

# Set multi-schema config as default (can be overridden by env var)
if "GRAPH_CONFIG_PATH" not in os.environ:
    # Get path relative to project root (tests/integration/ → ../../schemas/test/)
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    multi_schema_path = os.path.join(project_root, "schemas/test/unified_test_multi_schema.yaml")
    if os.path.exists(multi_schema_path):
        os.environ["GRAPH_CONFIG_PATH"] = multi_schema_path
        print(f"✓ Using multi-schema config: {multi_schema_path}")
        print(f"  Available schemas: social_integration, test_fixtures, ldbc_snb, denormalized_flights, pattern_comp, zeek_logs")
    else:
        print(f"⚠ Warning: Multi-schema config not found at {multi_schema_path}")


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


# Alias for backward compatibility with some test files
@pytest.fixture(scope="module")
def clickhouse_conn(clickhouse_client):
    """Alias for clickhouse_client fixture (module-scoped for test isolation)."""
    return clickhouse_client


@pytest.fixture
def clickgraph_client():
    """HTTP client for ClickGraph server at localhost:8080."""
    class ClickGraphClient:
        def __init__(self, base_url=CLICKGRAPH_URL):
            self.base_url = base_url
        
        def post(self, endpoint, json=None, headers=None):
            """POST request to ClickGraph."""
            url = f"{self.base_url}{endpoint}"
            return requests.post(url, json=json, headers=headers)
        
        def get(self, endpoint, headers=None):
            """GET request to ClickGraph."""
            url = f"{self.base_url}{endpoint}"
            return requests.get(url, headers=headers)
    
    return ClickGraphClient()


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
    """Clean simple_graph tables in test database before each test.
    
    Only drops tables created by the simple_graph fixture to avoid
    destroying session-scoped test data used by other test files.
    """
    simple_graph_tables = ['users', 'products', 'purchases', 'follows', 'friendships']
    
    for table_name in simple_graph_tables:
        try:
            clickhouse_client.command(f"DROP TABLE IF EXISTS {test_database}.{table_name}")
        except Exception:
            pass
    
    yield
    
    # Clean after test as well
    for table_name in simple_graph_tables:
        try:
            clickhouse_client.command(f"DROP TABLE IF EXISTS {test_database}.{table_name}")
        except Exception:
            pass


def execute_cypher(query: str, schema_name: str = "social_integration", raise_on_error: bool = True) -> Dict[str, Any]:
    """
    Execute a Cypher query via ClickGraph HTTP API.
    
    MULTI-SCHEMA MODE (CLEAN SEPARATION):
    - Uses explicit "USE <schema_name>" clause in queries
    - Auto-prepends USE clause if not present
    - Default schema: social_integration (User, Post, FOLLOWS from brahmand DB)
    - Each schema is a separate self-contained graph
    
    Available schemas:
    - social_integration: Primary social network (User, Post, FOLLOWS)
    - test_fixtures: Test data (TestUser, TestProduct, TEST_FOLLOWS)
    - denormalized_flights: Denormalized Airport→FLIGHT graph
    - data_security, property_expressions, etc.: Specialized test schemas
    
    Args:
        query: Cypher query string (USE clause auto-prepended if missing)
        schema_name: Schema name to use in USE clause (default: "social_integration")
        raise_on_error: If True, raise HTTPError on failure. If False, return error in response dict.
        
    Returns:
        Response JSON with results, columns, and performance metrics
        
    Raises:
        requests.HTTPError: If query execution fails and raise_on_error=True
    """
    # Auto-prepend USE clause if not already present
    query_upper = query.strip().upper()
    if not query_upper.startswith("USE "):
        query = f"USE {schema_name} {query}"
    
    response = requests.post(
        f"{CLICKGRAPH_URL}/query",
        json={"query": query},
        headers={"Content-Type": "application/json"}
    )
    
    # Print error details if request failed
    if response.status_code != 200:
        print(f"\nError Response ({response.status_code}):")
        print(f"Request: query={query}")
        print(f"Response: {response.text}")
        
        # For error handling tests, return error info instead of raising
        if not raise_on_error:
            try:
                error_json = response.json()
                return {"status": "error", "error": error_json, "status_code": response.status_code}
            except:
                return {"status": "error", "error": response.text, "status_code": response.status_code}
    
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


@pytest.fixture(scope="session", autouse=True)
def load_all_test_schemas():
    """
    Load all test schemas into ClickGraph server at session start.
    
    This ensures all tests can run regardless of which schema was initially loaded via GRAPH_CONFIG_PATH.
    Schemas are loaded dynamically via /schemas/load endpoint and stored in GLOBAL_SCHEMAS.
    
    Schema mappings (clean separation - each schema is a self-contained graph):
    - social_integration: Primary social network (User, Post, FOLLOWS) from brahmand DB
    - test_fixtures: Test data (TestUser, TestProduct) from test_integration DB
    - denormalized_flights: Denormalized Airport→FLIGHT graph from test_integration DB
    - data_security: Security graph (User, Group, File, Folder, polymorphic relationships)
    - property_expressions: Property expression tests
    - Other specialized test schemas
    """
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    
    # Define schemas to load: (schema_name, yaml_path)
    # NOTE: Each schema is a separate graph - tests use USE clause to select schema
    schemas_to_load = [
        # Core test schemas (clean separation)
        ("social_integration", "schemas/test/social_integration.yaml"),
        ("test_fixtures", "schemas/test/test_fixtures.yaml"),
        ("denormalized_flights_test", "schemas/test/denormalized_flights.yaml"),  # Comprehensive denormalized FROM/TO properties
        
        # Specialized test schemas
        ("data_security", "examples/data_security/data_security.yaml"),
        ("property_expressions", "schemas/test/property_expressions.yaml"),
        ("property_expressions_simple", "schemas/test/property_expressions_simple.yaml"),
        ("group_membership", "schemas/test/group_membership_simple.yaml"),
        ("multi_tenant", "schemas/test/multi_tenant.yaml"),
        ("mixed_denorm_test", "schemas/test/mixed_denorm_test.yaml"),
        ("filesystem", "schemas/examples/filesystem.yaml"),  # File storage system graph
        
        # NOTE: zeek_merged removed because:
        # 1. The 'zeek' database doesn't exist in integration test environment
        # 2. zeek tests have their own dedicated test file (test_zeek_merged.py) with proper data setup
        
        # Benchmark schemas
        ("ontime_flights", "schemas/examples/ontime_denormalized.yaml"),
        
        # NOTE: unified_test_multi_schema.yaml is loaded as default via GRAPH_CONFIG_PATH
        # It contains 6 schemas loaded automatically by the server at startup
        # (test_fixtures, social_integration, ldbc_snb, denormalized_flights, zeek_logs, pattern_comp)
    ]
    
    loaded_count = 0
    failed_schemas = []
    
    for schema_name, yaml_path in schemas_to_load:
        full_path = os.path.join(project_root, yaml_path)
        if not os.path.exists(full_path):
            print(f"⚠ Schema file not found: {yaml_path}")
            failed_schemas.append((schema_name, "File not found"))
            continue
            
        try:
            with open(full_path, 'r') as f:
                yaml_content = f.read()
            
            response = requests.post(
                f"{CLICKGRAPH_URL}/schemas/load",
                json={
                    "schema_name": schema_name,
                    "config_content": yaml_content
                },
                timeout=10
            )
            
            if response.status_code == 200:
                loaded_count += 1
                print(f"✓ Loaded schema: {schema_name}")
            else:
                print(f"✗ Failed to load schema '{schema_name}': {response.text}")
                failed_schemas.append((schema_name, response.text))
                
        except Exception as e:
            print(f"✗ Error loading schema '{schema_name}': {e}")
            failed_schemas.append((schema_name, str(e)))
    
    print(f"\n📊 Schema loading summary: {loaded_count}/{len(schemas_to_load)} schemas loaded successfully")
    
    if failed_schemas:
        print("\n⚠ Failed schemas:")
        for name, error in failed_schemas:
            print(f"  - {name}: {error}")
    
    # Don't fail tests if some schemas fail to load - tests will fail individually if needed
    # This allows partial test runs even if some schemas are missing


@pytest.fixture(scope="session", autouse=True)
def load_all_test_data(clickhouse_client, test_database, setup_test_database):
    """
    Load ALL test data for all schemas at session start.
    This ensures all integration tests have data available without needing explicit fixtures.
    
    This fixture runs automatically (autouse=True) once per test session.
    Each schema's data is loaded independently to prevent one failure from blocking others.
    """
    print("\n🔧 Loading comprehensive test data...")
    
    def load_test_integration_data():
        """Load basic test_integration data for initial tests."""
        try:
            # Create test_integration database and tables
            clickhouse_client.command("""
                CREATE TABLE IF NOT EXISTS test_integration.users (
                    user_id UInt32,
                    name String,
                    age UInt32,
                    email String,
                    registration_date Date,
                    is_active UInt8,
                    country String,
                    city String
                ) ENGINE = Memory
            """)
            
            clickhouse_client.command("""
                CREATE TABLE IF NOT EXISTS test_integration.follows (
                    follower_id UInt32,
                    followed_id UInt32,
                    since Date
                ) ENGINE = Memory
            """)
            
            clickhouse_client.command("""
                CREATE TABLE IF NOT EXISTS test_integration.products (
                    product_id UInt32,
                    name String,
                    price Float32,
                    category String
                ) ENGINE = Memory
            """)
            
            clickhouse_client.command("""
                CREATE TABLE IF NOT EXISTS test_integration.purchases (
                    user_id UInt32,
                    product_id UInt32,
                    purchase_date Date,
                    quantity UInt32
                ) ENGINE = Memory
            """)
            
            clickhouse_client.command("""
                CREATE TABLE IF NOT EXISTS test_integration.friendships (
                    user_id_1 UInt32,
                    user_id_2 UInt32,
                    since String
                ) ENGINE = Memory
            """)
            
            # Insert test_integration data
            clickhouse_client.command("""
                INSERT INTO test_integration.users VALUES
                    (1, 'Alice', 30, 'alice@example.com', '2023-01-01', 1, 'USA', 'New York'),
                    (2, 'Bob', 25, 'bob@example.com', '2023-02-01', 1, 'USA', 'San Francisco'),
                    (3, 'Charlie', 35, 'charlie@example.com', '2023-03-01', 1, 'UK', 'London'),
                    (4, 'Diana', 28, 'diana@example.com', '2023-04-01', 1, 'Canada', 'Toronto'),
                    (5, 'Eve', 32, 'eve@example.com', '2023-05-01', 1, 'USA', 'Seattle')
            """)
            
            clickhouse_client.command("""
                INSERT INTO test_integration.follows VALUES
                    (1, 2, '2023-01-15'),
                    (1, 3, '2023-01-20'),
                    (2, 3, '2023-02-10'),
                    (3, 4, '2023-03-05'),
                    (4, 5, '2023-04-15'),
                    (2, 4, '2023-02-20')
            """)
            
            clickhouse_client.command("""
                INSERT INTO test_integration.products VALUES
                    (101, 'Laptop', 999.99, 'Electronics'),
                    (102, 'Mouse', 29.99, 'Electronics'),
                    (103, 'Keyboard', 79.99, 'Electronics')
            """)
            
            clickhouse_client.command("""
                INSERT INTO test_integration.purchases VALUES
                    (1, 101, '2024-01-15', 1),
                    (2, 102, '2024-01-20', 2),
                    (3, 101, '2024-02-01', 1),
                    (4, 103, '2024-02-10', 1),
                    (1, 102, '2024-02-15', 3)
            """)
            
            clickhouse_client.command("""
                INSERT INTO test_integration.friendships VALUES
                    (1, 2, '2022-05-10'),
                    (3, 4, '2022-08-15'),
                    (2, 5, '2023-01-20')
            """)
            
            print("  ✓ test_integration (basic) data loaded")
        except Exception as e:
            print(f"  ⚠ test_integration (basic) data load failed: {e}")
    
    def load_brahmand_data():
        """Load brahmand database data for social_integration schema."""
        try:
            # Create brahmand database and social benchmark tables
            clickhouse_client.command("CREATE DATABASE IF NOT EXISTS brahmand")
            
            clickhouse_client.command("""
                CREATE TABLE IF NOT EXISTS brahmand.users_bench (
                    user_id UInt32,
                    full_name String,
                    email_address String,
                    registration_date Date,
                    is_active UInt8,
                    country String,
                    city String
                ) ENGINE = Memory
            """)
            
            clickhouse_client.command("""
                CREATE TABLE IF NOT EXISTS brahmand.user_follows_bench (
                    follower_id UInt32,
                    followed_id UInt32,
                    follow_date Date
                ) ENGINE = Memory
            """)
            
            clickhouse_client.command("""
                CREATE TABLE IF NOT EXISTS brahmand.posts_bench (
                    post_id UInt32,
                    user_id UInt32,
                    content String,
                    created_at DateTime
                ) ENGINE = Memory
            """)
            
            clickhouse_client.command("""
                CREATE TABLE IF NOT EXISTS brahmand.post_likes_bench (
                    user_id UInt32,
                    post_id UInt32,
                    liked_at DateTime
                ) ENGINE = Memory
            """)
            
            # Insert brahmand data
            clickhouse_client.command("""
                INSERT INTO brahmand.users_bench VALUES
                    (1, 'Alice Smith', 'alice@example.com', '2023-01-01', 1, 'USA', 'New York'),
                    (2, 'Bob Johnson', 'bob@example.com', '2023-02-01', 1, 'USA', 'San Francisco'),
                    (3, 'Charlie Brown', 'charlie@example.com', '2023-03-01', 1, 'UK', 'London'),
                    (4, 'Diana Prince', 'diana@example.com', '2023-04-01', 1, 'Canada', 'Toronto'),
                    (5, 'Eve Wilson', 'eve@example.com', '2023-05-01', 1, 'USA', 'Seattle')
            """)
            
            clickhouse_client.command("""
                INSERT INTO brahmand.user_follows_bench VALUES
                    (1, 2, '2023-01-15'),
                    (1, 3, '2023-01-20'),
                    (2, 3, '2023-02-10'),
                    (3, 4, '2023-03-05'),
                    (4, 5, '2023-04-15'),
                    (2, 4, '2023-02-20')
            """)
            
            clickhouse_client.command("""
                INSERT INTO brahmand.posts_bench VALUES
                    (1, 1, 'Hello world!', '2024-01-01 10:00:00'),
                    (2, 2, 'My first post', '2024-01-02 11:00:00'),
                    (3, 3, 'Testing ClickGraph', '2024-01-03 12:00:00')
            """)
            
            clickhouse_client.command("""
                INSERT INTO brahmand.post_likes_bench VALUES
                    (2, 1, '2024-01-01 11:00:00'),
                    (3, 1, '2024-01-01 12:00:00'),
                    (1, 2, '2024-01-02 13:00:00')
            """)
            
            print("  ✓ brahmand (social_integration) data loaded")
        except Exception as e:
            print(f"  ⚠ brahmand (social_integration) data load failed: {e}")
    
    def load_filesystem_data():
        """Load filesystem schema tables (in test_integration database)."""
        try:
            # Schema: fs_objects (nodes) and fs_parent (relationships)
            clickhouse_client.command("""
                CREATE TABLE IF NOT EXISTS test_integration.fs_objects (
                    object_id UInt32,
                    name String,
                    object_type String,  -- 'file' or 'folder'
                    size_bytes UInt64,   -- 0 for folders
                    mime_type Nullable(String),
                    created_at DateTime,
                    modified_at DateTime,
                    owner_id String      -- owner/user
                ) ENGINE = MergeTree()
                ORDER BY object_id
            """)
            
            clickhouse_client.command("""
                CREATE TABLE IF NOT EXISTS test_integration.fs_parent (
                    child_id UInt32,
                    parent_id UInt32
                ) ENGINE = MergeTree()
                ORDER BY (child_id, parent_id)
            """)
            
            # Insert filesystem test data
            # Root folder structure:
            # /root/ (id=1, folder)
            # ├── /Documents/ (id=2, folder)
            # │   ├── report.pdf (id=4, file)
            # │   └── notes.txt (id=5, file)
            # ├── /Downloads/ (id=3, folder)
            # │   └── image.jpg (id=6, file)
            
            clickhouse_client.command("""
                INSERT INTO test_integration.fs_objects VALUES
                    (1, 'root', 'folder', 0, NULL, '2023-01-01 00:00:00', '2023-01-01 00:00:00', 'admin'),
                    (2, 'Documents', 'folder', 0, NULL, '2023-01-02 10:00:00', '2023-01-05 15:30:00', 'user1'),
                    (3, 'Downloads', 'folder', 0, NULL, '2023-01-03 11:00:00', '2023-01-06 14:00:00', 'user1'),
                    (4, 'report.pdf', 'file', 1024000, 'application/pdf', '2023-01-10 09:00:00', '2023-01-10 09:00:00', 'user1'),
                    (5, 'notes.txt', 'file', 2048, 'text/plain', '2023-01-11 10:30:00', '2023-01-12 11:00:00', 'user1'),
                    (6, 'image.jpg', 'file', 5242880, 'image/jpeg', '2023-01-15 14:00:00', '2023-01-15 14:00:00', 'user1')
            """)
            
            clickhouse_client.command("""
                INSERT INTO test_integration.fs_parent VALUES
                    (2, 1),
                    (3, 1),
                    (4, 2),
                    (5, 2),
                    (6, 3)
            """)
            
            print("  ✓ test_integration (filesystem schema) data loaded")
        except Exception as e:
            print(f"  ⚠ test_integration (filesystem schema) data load failed: {e}")
    
    def load_group_membership_data():
        """Load group_membership schema tables (in test_integration database)."""
        try:
            # Schema: gm_users (User nodes), gm_groups (Group nodes), gm_memberships (MEMBER_OF relationships)
            # Uses gm_ prefix to avoid conflicts with basic test fixtures
            clickhouse_client.command("""
                CREATE TABLE IF NOT EXISTS test_integration.gm_users (
                    user_id UInt32,
                    name String,
                    email String
                ) ENGINE = MergeTree()
                ORDER BY user_id
            """)
            
            clickhouse_client.command("""
                CREATE TABLE IF NOT EXISTS test_integration.gm_groups (
                    id UInt32,
                    name String,
                    description String
                ) ENGINE = MergeTree()
                ORDER BY id
            """)
            
            clickhouse_client.command("""
                CREATE TABLE IF NOT EXISTS test_integration.gm_memberships (
                    user_id UInt32,
                    group_id UInt32,
                    joined_at DateTime,
                    role String
                ) ENGINE = MergeTree()
                ORDER BY (user_id, group_id)
            """)
            
            # Insert group_membership test data
            # 5 users, 3 groups, ~10 memberships with different roles
            clickhouse_client.command("""
                INSERT INTO test_integration.gm_users VALUES
                    (1, 'Alice', 'alice@example.com'),
                    (2, 'Bob', 'bob@example.com'),
                    (3, 'Charlie', 'charlie@example.com'),
                    (4, 'Diana', 'diana@example.com'),
                    (5, 'Eve', 'eve@example.com')
            """)
            
            clickhouse_client.command("""
                INSERT INTO test_integration.gm_groups VALUES
                    (101, 'Engineering', 'Engineering team'),
                    (102, 'Product', 'Product team'),
                    (103, 'Sales', 'Sales team')
            """)
            
            clickhouse_client.command("""
                INSERT INTO test_integration.gm_memberships VALUES
                    (1, 101, '2024-01-01 00:00:00', 'admin'),
                    (2, 101, '2024-01-05 00:00:00', 'member'),
                    (3, 101, '2024-01-10 00:00:00', 'member'),
                    (1, 102, '2024-01-15 00:00:00', 'member'),
                    (4, 102, '2024-01-20 00:00:00', 'admin'),
                    (5, 102, '2024-01-25 00:00:00', 'viewer'),
                    (2, 103, '2024-02-01 00:00:00', 'member'),
                    (3, 103, '2024-02-05 00:00:00', 'member'),
                    (4, 103, '2024-02-10 00:00:00', 'member'),
                    (5, 101, '2024-02-15 00:00:00', 'viewer')
            """)
            
            print("  ✓ test_integration (group_membership schema) data loaded")
        except Exception as e:
            print(f"  ⚠ test_integration (group_membership schema) data load failed: {e}")
    
    def load_social_integration_data():
        """Load social_integration schema tables (test_integration.*_test).
        
        These tables are separate from the test_fixtures tables (users, follows, etc.)
        and use 'test_integration' database with '*_test' suffix to avoid confusion
        with the brahmand database format.
        
        DDL based on scripts/test/setup_social_integration_data.sh
        """
        try:
            # Users table
            clickhouse_client.command("""
                CREATE TABLE IF NOT EXISTS test_integration.users_test (
                    user_id UInt32,
                    full_name String,
                    email_address String,
                    age UInt8,
                    registration_date Date,
                    is_active UInt8,
                    country String,
                    city String
                ) ENGINE = Memory
            """)

            # Posts table
            clickhouse_client.command("""
                CREATE TABLE IF NOT EXISTS test_integration.posts_test (
                    post_id UInt32,
                    post_title String,
                    post_content String,
                    post_date Date,
                    author_id UInt32
                ) ENGINE = Memory
            """)

            # User follows table
            clickhouse_client.command("""
                CREATE TABLE IF NOT EXISTS test_integration.user_follows_test (
                    follow_id UInt32,
                    follower_id UInt32,
                    followed_id UInt32,
                    follow_date Date
                ) ENGINE = Memory
            """)

            # Post likes table
            clickhouse_client.command("""
                CREATE TABLE IF NOT EXISTS test_integration.post_likes_test (
                    like_id UInt32,
                    user_id UInt32,
                    post_id UInt32,
                    like_date Date
                ) ENGINE = Memory
            """)

            # Insert users (30 rows)
            clickhouse_client.command("""
                INSERT INTO test_integration.users_test VALUES
                    (1, 'Alice Johnson', 'alice@example.com', 28, '2020-01-15', 1, 'USA', 'New York'),
                    (2, 'Bob Smith', 'bob@example.com', 32, '2019-05-20', 1, 'UK', 'London'),
                    (3, 'Carol White', 'carol@example.com', 25, '2021-03-10', 1, 'Canada', 'Toronto'),
                    (4, 'David Brown', 'david@example.com', 35, '2018-11-25', 1, 'Australia', 'Sydney'),
                    (5, 'Eve Davis', 'eve@example.com', 29, '2020-07-08', 1, 'USA', 'San Francisco'),
                    (6, 'Frank Miller', 'frank@example.com', 31, '2019-09-14', 1, 'Germany', 'Berlin'),
                    (7, 'Grace Lee', 'grace@example.com', 27, '2020-12-01', 1, 'South Korea', 'Seoul'),
                    (8, 'Henry Wilson', 'henry@example.com', 33, '2019-02-18', 1, 'USA', 'Chicago'),
                    (9, 'Iris Martinez', 'iris@example.com', 26, '2021-06-22', 1, 'Spain', 'Madrid'),
                    (10, 'Jack Taylor', 'jack@example.com', 30, '2020-04-11', 1, 'USA', 'Boston'),
                    (11, 'Kate Anderson', 'kate@example.com', 28, '2020-08-19', 1, 'UK', 'Manchester'),
                    (12, 'Liam Thomas', 'liam@example.com', 34, '2018-10-05', 1, 'Ireland', 'Dublin'),
                    (13, 'Mia Jackson', 'mia@example.com', 24, '2021-01-30', 1, 'USA', 'Austin'),
                    (14, 'Noah Harris', 'noah@example.com', 36, '2017-12-12', 1, 'Canada', 'Vancouver'),
                    (15, 'Olivia Clark', 'olivia@example.com', 29, '2020-05-25', 1, 'Australia', 'Melbourne'),
                    (16, 'Paul Lewis', 'paul@example.com', 31, '2019-07-14', 1, 'USA', 'Seattle'),
                    (17, 'Quinn Walker', 'quinn@example.com', 27, '2020-11-08', 1, 'UK', 'Edinburgh'),
                    (18, 'Rachel Hall', 'rachel@example.com', 25, '2021-02-16', 1, 'USA', 'Portland'),
                    (19, 'Sam Allen', 'sam@example.com', 32, '2019-04-22', 1, 'Canada', 'Montreal'),
                    (20, 'Tina Young', 'tina@example.com', 28, '2020-09-03', 1, 'USA', 'Denver'),
                    (21, 'Uma King', 'uma@example.com', 26, '2021-05-17', 1, 'India', 'Mumbai'),
                    (22, 'Victor Wright', 'victor@example.com', 35, '2018-08-29', 1, 'USA', 'Miami'),
                    (23, 'Wendy Lopez', 'wendy@example.com', 30, '2020-03-12', 1, 'Mexico', 'Mexico City'),
                    (24, 'Xavier Hill', 'xavier@example.com', 33, '2019-06-05', 1, 'USA', 'Dallas'),
                    (25, 'Yara Scott', 'yara@example.com', 27, '2020-10-20', 1, 'UAE', 'Dubai'),
                    (26, 'Zack Green', 'zack@example.com', 29, '2020-02-14', 1, 'USA', 'Phoenix'),
                    (27, 'Amy Adams', 'amy@example.com', 31, '2019-11-30', 1, 'UK', 'Bristol'),
                    (28, 'Ben Baker', 'ben@example.com', 28, '2020-07-25', 1, 'USA', 'Atlanta'),
                    (29, 'Chloe Carter', 'chloe@example.com', 26, '2021-04-08', 1, 'Canada', 'Calgary'),
                    (30, 'Dan Foster', 'dan@example.com', 34, '2018-09-16', 0, 'USA', 'Detroit')
            """)

            # Insert posts (20 rows - representative subset)
            clickhouse_client.command("""
                INSERT INTO test_integration.posts_test VALUES
                    (1, 'Introduction', 'Hello everyone!', '2023-01-01', 1),
                    (2, 'First Post', 'My first post here', '2023-01-02', 2),
                    (3, 'Tech News', 'Latest in technology', '2023-01-03', 3),
                    (4, 'Travel Blog', 'My trip to Europe', '2023-01-04', 4),
                    (5, 'Cooking Tips', 'Best pasta recipe', '2023-01-05', 5),
                    (6, 'Music Review', 'New album review', '2023-01-06', 6),
                    (7, 'Book Club', 'This months book', '2023-01-07', 7),
                    (8, 'Fitness Journey', 'Week 1 progress', '2023-01-08', 8),
                    (9, 'Photography', 'Sunset shots', '2023-01-09', 9),
                    (10, 'Gaming News', 'New game release', '2023-01-10', 10),
                    (11, 'Movie Review', 'Latest blockbuster', '2023-01-11', 1),
                    (12, 'DIY Projects', 'Home improvement', '2023-01-12', 2),
                    (13, 'Pet Stories', 'My dogs adventure', '2023-01-13', 3),
                    (14, 'Fashion Trends', 'Spring collection', '2023-01-14', 4),
                    (15, 'Career Advice', 'Job hunting tips', '2023-01-15', 5),
                    (16, 'Investment Tips', 'Stock market basics', '2023-01-16', 6),
                    (17, 'Gardening', 'Growing tomatoes', '2023-01-17', 7),
                    (18, 'Art Exhibition', 'Local art show', '2023-01-18', 8),
                    (19, 'Science Facts', 'Space discoveries', '2023-01-19', 9),
                    (20, 'History Lesson', 'Ancient Rome', '2023-01-20', 10)
            """)

            # Insert follows (20 rows - representative subset)
            clickhouse_client.command("""
                INSERT INTO test_integration.user_follows_test VALUES
                    (1, 1, 2, '2023-01-01'), (2, 1, 3, '2023-01-02'), (3, 1, 5, '2023-01-03'),
                    (4, 2, 1, '2023-01-04'), (5, 2, 4, '2023-01-05'), (6, 2, 6, '2023-01-06'),
                    (7, 3, 1, '2023-01-07'), (8, 3, 7, '2023-01-08'), (9, 3, 8, '2023-01-09'),
                    (10, 4, 2, '2023-01-10'), (11, 4, 9, '2023-01-11'), (12, 4, 10, '2023-01-12'),
                    (13, 5, 1, '2023-01-13'), (14, 5, 11, '2023-01-14'), (15, 5, 12, '2023-01-15'),
                    (16, 6, 2, '2023-01-16'), (17, 6, 13, '2023-01-17'), (18, 6, 14, '2023-01-18'),
                    (19, 7, 3, '2023-01-19'), (20, 7, 15, '2023-01-20')
            """)

            # Insert likes (20 rows - representative subset)
            clickhouse_client.command("""
                INSERT INTO test_integration.post_likes_test VALUES
                    (1, 1, 2, '2023-01-02'), (2, 1, 3, '2023-01-03'), (3, 1, 5, '2023-01-05'),
                    (4, 2, 1, '2023-01-01'), (5, 2, 3, '2023-01-03'), (6, 2, 6, '2023-01-06'),
                    (7, 3, 1, '2023-01-01'), (8, 3, 7, '2023-01-07'), (9, 3, 8, '2023-01-08'),
                    (10, 4, 2, '2023-01-02'), (11, 4, 9, '2023-01-09'), (12, 4, 10, '2023-01-10'),
                    (13, 5, 1, '2023-01-01'), (14, 5, 11, '2023-01-11'), (15, 5, 12, '2023-01-12'),
                    (16, 6, 2, '2023-01-02'), (17, 6, 13, '2023-01-13'), (18, 6, 14, '2023-01-14'),
                    (19, 7, 3, '2023-01-03'), (20, 7, 15, '2023-01-15')
            """)

            print("  ✓ test_integration (social_integration schema) data loaded")
        except Exception as e:
            print(f"  ⚠ test_integration (social_integration schema) data load failed: {e}")

    def load_data_security_data():
        """Load data_security schema tables from setup SQL script."""
        try:
            import re
            with open('examples/data_security/setup_schema.sql', 'r') as f:
                sql = f.read()
            # Remove comments and split by semicolon
            sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
            stmts = [s.strip() for s in sql.split(';') if s.strip()]
            clickhouse_client.command("CREATE DATABASE IF NOT EXISTS data_security")
            for stmt in stmts:
                if stmt.upper().startswith(('CREATE', 'DROP', 'INSERT')):
                    clickhouse_client.command(stmt)
            print("  ✓ data_security schema data loaded")
        except Exception as e:
            print(f"  ⚠ data_security schema data load failed: {e}")

    def load_property_expressions_data():
        """Load property_expressions schema tables from setup SQL script."""
        try:
            import re
            with open('tests/fixtures/data/setup_property_expressions.sql', 'r') as f:
                sql = f.read()
            # Remove comments and USE statements, replace brahmand with test_integration
            sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
            sql = re.sub(r'USE \w+;?\s*', '', sql)
            sql = sql.replace('brahmand.', 'test_integration.')
            stmts = [s.strip() for s in sql.split(';') if s.strip()]
            for stmt in stmts:
                if stmt.upper().startswith(('CREATE', 'DROP', 'INSERT')):
                    # Prefix bare table names with test_integration
                    if 'test_integration.' not in stmt:
                        stmt = stmt.replace('users_expressions_test', 'test_integration.users_expressions_test')
                        stmt = stmt.replace('follows_expressions_test', 'test_integration.follows_expressions_test')
                    clickhouse_client.command(stmt)
            print("  ✓ test_integration (property_expressions) data loaded")
        except Exception as e:
            print(f"  ⚠ test_integration (property_expressions) data load failed: {e}")

    # Load each schema's data independently
    load_test_integration_data()
    load_brahmand_data()
    load_filesystem_data()
    load_group_membership_data()
    load_social_integration_data()
    load_data_security_data()
    load_property_expressions_data()
    
    print("✅ All test data loaded successfully\n")


@pytest.fixture
def setup_benchmark_data(load_all_test_data):
    """
    Fixture that ensures benchmark data is loaded before tests.
    
    This is a simple pass-through that depends on load_all_test_data.
    Tests that need benchmark data can depend on this fixture to ensure
    social_integration tables (users_bench, posts_bench, user_follows_bench, post_likes_bench)
    are available in the brahmand database.
    """
    # Just return control after load_all_test_data has run
    yield
    # No cleanup needed - data persists for other tests


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
    
    # Return complete schema configuration
    # NOTE: Uses test_fixtures schema which contains TestUser/TestProduct labels
    # mapped to brahmand database (from multi-schema config)
    return {
        "schema_name": "test_fixtures",  # Use test_fixtures schema from multi-schema config
        "database": "test_integration",  # Physical ClickHouse database where tables exist
        "nodes": {
            "TestUser": {  # Changed from "User" to match unified schema
                "table": "users",
                "id_column": "user_id",
                "properties": ["name", "age"]
            },
            "TestProduct": {  # Changed from "Product" to match unified schema
                "table": "products",
                "id_column": "product_id",
                "properties": ["name", "price", "category"]
            }
        },
        "relationships": {
            "TEST_FOLLOWS": {  # Changed from "FOLLOWS"
                "table": "follows",
                "from_id": "follower_id",
                "to_id": "followed_id",
                "from_node": "TestUser",  # Changed to match unified schema
                "to_node": "TestUser",  # Changed to match unified schema
                "properties": ["since"]
            },
            "TEST_PURCHASED": {  # Changed from "PURCHASED"
                "table": "purchases",
                "from_id": "user_id",
                "to_id": "product_id",
                "from_node": "TestUser",  # Changed to match unified schema
                "to_node": "TestProduct",  # Changed to match unified schema
                "properties": ["purchase_date", "quantity"]
            },
            "TEST_FRIENDS_WITH": {  # Changed from "FRIENDS_WITH"
                "table": "friendships",
                "from_id": "user_id_1",
                "to_id": "user_id_2",
                "from_node": "TestUser",  # Changed to match unified schema
                "to_node": "TestUser",  # Changed to match unified schema
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


