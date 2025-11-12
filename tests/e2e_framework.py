"""
E2E Test Framework for ClickGraph

Provides infrastructure for organized, self-contained E2E test suites with:
- Automatic schema registration
- Test data setup/teardown
- Independent test buckets
- Development mode (skip teardown for debugging)
- Reusable test fixtures
"""

import pytest
import requests
import clickhouse_connect
import yaml
import os
import time
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass


# Test configuration
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "test_user")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "test_pass")

# Test mode: set CLICKGRAPH_DEBUG=1 to skip teardown for debugging
DEBUG_MODE = os.getenv("CLICKGRAPH_DEBUG", "0") == "1"


@dataclass
class TestBucket:
    """
    A self-contained test bucket with its own schema and data.
    
    Attributes:
        name: Unique bucket identifier
        database: ClickHouse database name
        schema_file: Path to YAML schema file
        setup_sql: SQL script for creating tables and data
        teardown_sql: SQL script for cleanup (optional)
    """
    name: str
    database: str
    schema_file: Path
    setup_sql: Optional[Path] = None
    teardown_sql: Optional[Path] = None
    schema_name: Optional[str] = None  # Registered schema name in ClickGraph


class E2ETestFramework:
    """
    Manages E2E test infrastructure with automatic setup/teardown.
    """
    
    def __init__(self):
        self.clickhouse_client = None
        self.registered_schemas = set()
        self.created_databases = set()
        
    def connect_clickhouse(self):
        """Establish ClickHouse connection."""
        if not self.clickhouse_client:
            self.clickhouse_client = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT,
                username=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD
            )
        return self.clickhouse_client
    
    def close(self):
        """Close connections."""
        if self.clickhouse_client:
            self.clickhouse_client.close()
            self.clickhouse_client = None
    
    def wait_for_clickgraph(self, timeout=30):
        """Wait for ClickGraph server to be ready."""
        start = time.time()
        while time.time() - start < timeout:
            try:
                response = requests.get(f"{CLICKGRAPH_URL}/health", timeout=2)
                if response.status_code == 200:
                    return True
            except Exception:
                pass
            time.sleep(0.5)
        return False
    
    def setup_bucket(self, bucket: TestBucket) -> bool:
        """
        Set up a test bucket: database, schema, and data.
        
        Returns:
            True if setup successful, False otherwise
        """
        print(f"\n[SETUP] Setting up test bucket: {bucket.name}")
        
        client = self.connect_clickhouse()
        
        # Step 1: Create database
        print(f"  [OK] Creating database: {bucket.database}")
        try:
            client.command(f"CREATE DATABASE IF NOT EXISTS {bucket.database}")
            self.created_databases.add(bucket.database)
        except Exception as e:
            print(f"  [ERROR] Failed to create database: {e}")
            return False
        
        # Step 2: Run setup SQL if provided
        if bucket.setup_sql and bucket.setup_sql.exists():
            print(f"  [OK] Running setup SQL: {bucket.setup_sql.name}")
            try:
                sql_content = bucket.setup_sql.read_text()
                # Execute each statement separately
                for statement in sql_content.split(';'):
                    statement = statement.strip()
                    if statement:
                        client.command(statement)
            except Exception as e:
                print(f"  [ERROR] Failed to run setup SQL: {e}")
                return False
        
        # Step 3: Register schema in ClickGraph via /schemas/load endpoint
        if bucket.schema_file.exists():
            print(f"  [OK] Loading schema: {bucket.schema_file.name}")
            try:
                schema_yaml = bucket.schema_file.read_text()
                schema_data = yaml.safe_load(schema_yaml)
                
                # Use schema name from bucket or from YAML
                schema_name = bucket.schema_name or schema_data.get('name', bucket.name)
                
                response = requests.post(
                    f"{CLICKGRAPH_URL}/schemas/load",
                    json={
                        "schema_name": schema_name,
                        "config_content": schema_yaml,
                        "validate_schema": False  # Skip validation for faster loading
                    },
                    timeout=10
                )
                
                if response.status_code == 200:
                    self.registered_schemas.add(schema_name)
                    bucket.schema_name = schema_name
                    print(f"  [OK] Schema loaded: {schema_name}")
                else:
                    print(f"  [ERROR] Failed to load schema: {response.status_code}")
                    print(f"    Response: {response.text}")
                    return False
            except Exception as e:
                print(f"  [ERROR] Failed to load schema: {e}")
                return False
        
        print(f"[COMPLETE] Test bucket '{bucket.name}' ready!")
        return True
    
    def teardown_bucket(self, bucket: TestBucket, force=False):
        """
        Tear down a test bucket (unless in DEBUG_MODE).
        
        Args:
            bucket: TestBucket to tear down
            force: Force teardown even in DEBUG_MODE
        """
        if DEBUG_MODE and not force:
            print(f"\n[DEBUG] DEBUG MODE: Skipping teardown for '{bucket.name}'")
            print(f"   Database '{bucket.database}' preserved for debugging")
            return
        
        print(f"\n[CLEANUP] Tearing down test bucket: {bucket.name}")
        
        client = self.connect_clickhouse()
        
        # Run teardown SQL if provided
        if bucket.teardown_sql and bucket.teardown_sql.exists():
            print(f"  [OK] Running teardown SQL: {bucket.teardown_sql.name}")
            try:
                sql_content = bucket.teardown_sql.read_text()
                for statement in sql_content.split(';'):
                    statement = statement.strip()
                    if statement:
                        client.command(statement)
            except Exception as e:
                print(f"  [WARNING] Warning during teardown SQL: {e}")
        
        # Drop database
        try:
            client.command(f"DROP DATABASE IF EXISTS {bucket.database}")
            self.created_databases.discard(bucket.database)
            print(f"  [OK] Database dropped: {bucket.database}")
        except Exception as e:
            print(f"  [WARNING] Warning during database drop: {e}")
        
        print(f"[COMPLETE] Test bucket '{bucket.name}' cleaned up")
    
    def cleanup_all(self, force=False):
        """Clean up all created resources."""
        if DEBUG_MODE and not force:
            print("\n[DEBUG] DEBUG MODE: Skipping global cleanup")
            print(f"   Preserved databases: {', '.join(self.created_databases)}")
            return
        
        print("\n[CLEANUP] Cleaning up all test resources...")
        client = self.connect_clickhouse()
        
        for database in list(self.created_databases):
            try:
                client.command(f"DROP DATABASE IF EXISTS {database}")
                print(f"  [OK] Dropped database: {database}")
            except Exception as e:
                print(f"  [WARNING] Warning dropping {database}: {e}")
        
        self.created_databases.clear()
        self.registered_schemas.clear()


# Pytest fixtures for E2E tests

@pytest.fixture(scope="session")
def e2e_framework():
    """
    Session-scoped E2E framework instance.
    Provides test infrastructure for entire test session.
    """
    framework = E2ETestFramework()
    
    # Verify ClickGraph is running
    if not framework.wait_for_clickgraph():
        pytest.skip("ClickGraph server not running")
    
    yield framework
    
    # Cleanup at end of session
    framework.cleanup_all()
    framework.close()


@pytest.fixture
def test_bucket(e2e_framework):
    """
    Function-scoped test bucket fixture.
    Creates a temporary test bucket for a single test.
    """
    buckets = []
    
    def create_bucket(
        name: str,
        database: str,
        schema_file: Path,
        setup_sql: Optional[Path] = None,
        teardown_sql: Optional[Path] = None
    ) -> TestBucket:
        """Create and setup a test bucket."""
        bucket = TestBucket(
            name=name,
            database=database,
            schema_file=schema_file,
            setup_sql=setup_sql,
            teardown_sql=teardown_sql
        )
        
        if not e2e_framework.setup_bucket(bucket):
            pytest.fail(f"Failed to setup test bucket: {name}")
        
        buckets.append(bucket)
        return bucket
    
    yield create_bucket
    
    # Teardown all buckets created in this test
    for bucket in buckets:
        e2e_framework.teardown_bucket(bucket)


@pytest.fixture
def clickgraph_client():
    """
    Provides a simple client for querying ClickGraph.
    """
    class ClickGraphClient:
        def query(self, cypher: str, parameters: Optional[Dict] = None, 
                  schema_name: str = "default") -> requests.Response:
            """Execute a Cypher query."""
            payload = {
                "query": cypher,
                "schema_name": schema_name
            }
            if parameters:
                payload["parameters"] = parameters
            
            return requests.post(f"{CLICKGRAPH_URL}/query", json=payload)
        
        def query_json(self, cypher: str, parameters: Optional[Dict] = None,
                       schema_name: str = "default") -> Dict:
            """Execute a query and return JSON response."""
            response = self.query(cypher, parameters, schema_name)
            response.raise_for_status()
            return response.json()
    
    return ClickGraphClient()


# Helper function to query ClickHouse directly
@pytest.fixture
def clickhouse_client(e2e_framework):
    """Provides direct ClickHouse client access."""
    return e2e_framework.connect_clickhouse()


if __name__ == "__main__":
    # Quick test of framework
    print("Testing E2E Framework...")
    
    framework = E2ETestFramework()
    
    if not framework.wait_for_clickgraph():
        print("❌ ClickGraph server not running!")
    else:
        print("[COMPLETE] ClickGraph server is ready")
    
    framework.close()
