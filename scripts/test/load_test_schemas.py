#!/usr/bin/env python3
"""
Load all schemas needed for integration tests.

This script loads multiple schemas into ClickGraph for testing purposes.
Run this after starting the ClickGraph server to enable full test suite.

Usage:
    python scripts/test/load_test_schemas.py
    
    # With custom URL:
    CLICKGRAPH_URL=http://localhost:8765 python scripts/test/load_test_schemas.py
"""

import requests
import os
from pathlib import Path

CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")

# Schemas to load for testing
# Note: Includes benchmark schemas because matrix tests use them for cross-schema validation
# The benchmark WORKLOADS (performance testing) are separate under /benchmarks/
SCHEMAS_TO_LOAD = [
    # Unified test schema - primary schema for most tests (TestUser, TestProduct, etc.)
    ("schemas/test/unified_test_schema.yaml", "unified_test_schema"),
    # Benchmark schemas - used by matrix tests for cross-schema coverage
    ("benchmarks/social_network/schemas/social_benchmark.yaml", "social_benchmark"),
    # Schema variations - matrix tests
    ("schemas/examples/filesystem.yaml", "filesystem"),
    ("schemas/test/group_membership_simple.yaml", "group_membership"),
    # Example schemas
    ("examples/data_security/data_security.yaml", "data_security"),
    # Denormalized schema (ontime_flights)
    ("schemas/examples/ontime_denormalized.yaml", "ontime_flights"),
    # Polymorphic schema
    ("schemas/examples/social_polymorphic.yaml", "social_polymorphic"),
    # Coupled/DNS schema
    ("schemas/examples/zeek_dns_log.yaml", "zeek_dns"),
    # Other useful schemas
    ("schemas/examples/zeek_conn_log.yaml", "zeek_conn"),
    ("schemas/examples/zeek_merged.yaml", "zeek_merged"),
    ("schemas/examples/ecommerce_simple.yaml", "ecommerce"),
]


def load_schema(schema_path: str, expected_name: str) -> bool:
    """Load a schema from file and register it with ClickGraph."""
    project_root = Path(__file__).parent.parent.parent
    full_path = project_root / schema_path
    
    if not full_path.exists():
        print(f"  ⚠️  Schema file not found: {full_path}")
        return False
    
    with open(full_path, "r") as f:
        yaml_content = f.read()
    
    # The schema name comes from the YAML file's `name` field
    response = requests.post(
        f"{CLICKGRAPH_URL}/schemas/load",
        json={
            "config_content": yaml_content,
            "schema_name": expected_name,
        },
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    
    if response.status_code == 200:
        print(f"  ✅ Loaded: {expected_name}")
        return True
    else:
        print(f"  ❌ Failed to load {expected_name}: {response.text[:100]}")
        return False


def main():
    print(f"Loading test schemas into ClickGraph ({CLICKGRAPH_URL})...")
    print()
    
    # Check server health first
    try:
        health = requests.get(f"{CLICKGRAPH_URL}/health", timeout=5)
        if health.status_code != 200:
            print(f"❌ Server health check failed: {health.status_code}")
            return 1
        print(f"✅ Server is healthy")
    except requests.exceptions.ConnectionError:
        print(f"❌ Cannot connect to server at {CLICKGRAPH_URL}")
        print("   Make sure ClickGraph is running first.")
        return 1
    
    print()
    print("Loading schemas:")
    
    success_count = 0
    fail_count = 0
    
    for schema_path, expected_name in SCHEMAS_TO_LOAD:
        if load_schema(schema_path, expected_name):
            success_count += 1
        else:
            fail_count += 1
    
    print()
    print(f"Summary: {success_count} loaded, {fail_count} failed")
    
    if fail_count > 0:
        print("\n⚠️  Some schemas failed to load. Tests using those schemas may fail.")
        return 1
    
    print("\n✅ All schemas loaded successfully!")
    print("   You can now run the full test suite.")
    return 0


if __name__ == "__main__":
    exit(main())
