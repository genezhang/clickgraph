"""
Pytest fixtures for E2E tests.
"""
import pytest
from neo4j import GraphDatabase
import os

BOLT_URL = os.getenv("BOLT_URL", "bolt://localhost:7687")


@pytest.fixture(scope="module")
def driver():
    """Neo4j driver fixture for Bolt protocol tests."""
    driver = GraphDatabase.driver(
        BOLT_URL,
        auth=("neo4j", "password"),
        database="ecommerce_demo"
    )
    try:
        driver.verify_connectivity()
    except Exception as e:
        pytest.skip(f"Cannot connect to Bolt server at {BOLT_URL}: {e}")
    
    yield driver
    driver.close()
