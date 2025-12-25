#!/usr/bin/env python3
"""
Integration tests for edge constraints feature.

Tests that edge constraints are:
1. Compiled correctly from schema
2. Added to generated SQL  
3. Filter query results correctly

Tests five schema patterns:
1. Standard edge table (lineage: DataFile -[COPIED_BY]-> DataFile)
2. Standard node/edge (social: User -[FOLLOWS]-> User)
3. FK-edge pattern (filesystem: File -[IN_FOLDER]-> Folder)
4. Denormalized edge (travel: Airport -[FLIGHT]-> Airport)
5. Polymorphic edge (community: Member -[MENTORS|REVIEWS|HELPS]-> Member)

Usage:
    # Run with pytest (self-contained - loads own schemas/data)
    pytest tests/integration/test_edge_constraints.py -v
    
    # Run as part of full suite
    pytest tests/integration/ -v
    
    # Run only edge constraint tests by marker
    pytest -m edge_constraints -v
"""

import pytest
import requests
import json
import os
import clickhouse_connect

# Use environment variables with defaults
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "test_user")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "test_pass")


# ============================================================================
# Test Suite Setup - Self-Contained Schema & Data Loading
# ============================================================================

@pytest.fixture(scope="module")
def edge_constraints_schemas():
    """
    Load edge constraint test schemas into ClickGraph server.
    
    This fixture makes the test suite self-contained - it loads all required
    schemas and returns their names for use in tests.
    """
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    
    # Schema definitions: (schema_name, yaml_path, description)
    schemas = [
        ("lineage", "schemas/examples/lineage_schema.yaml", "Standard edge table with timestamp constraint"),
        ("social_constraints", "schemas/test/social_constraints.yaml", "Standard node/edge with age constraint"),
        ("filesystem_constraints", "schemas/test/filesystem_constraints.yaml", "FK-edge with security level constraint"),
        ("travel_denormalized", "schemas/test/travel_denormalized_constraints.yaml", "Denormalized with timezone constraint"),
        ("community_polymorphic", "schemas/test/community_polymorphic_constraints.yaml", "Polymorphic edge with reputation constraint"),
    ]
    
    loaded_schemas = {}
    
    for schema_name, yaml_path, description in schemas:
        full_path = os.path.join(project_root, yaml_path)
        
        if not os.path.exists(full_path):
            pytest.skip(f"Schema file not found: {yaml_path}")
        
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
            loaded_schemas[schema_name] = description
            print(f"  ✓ Loaded schema: {schema_name}")
        else:
            pytest.fail(f"Failed to load schema {schema_name}: {response.text}")
    
    print(f"\n✅ Loaded {len(loaded_schemas)}/5 edge constraint schemas")
    return loaded_schemas


@pytest.fixture(scope="module")
def edge_constraints_data(edge_constraints_schemas):
    """
    Load test data for edge constraint tests.
    
    Executes SQL setup script to create databases and populate test tables.
    """
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    setup_script = os.path.join(project_root, "scripts/test/setup_edge_constraints_e2e.sql")
    
    if not os.path.exists(setup_script):
        pytest.skip(f"Test data script not found: {setup_script}")
    
    # Connect to ClickHouse and execute setup script
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD
    )
    
    try:
        with open(setup_script, 'r') as f:
            sql_content = f.read()
        
        # Execute SQL statements (split by semicolon, filter empty)
        statements = [s.strip() for s in sql_content.split(';') if s.strip()]
        for statement in statements:
            if statement:
                client.command(statement)
        
        print(f"  ✓ Loaded test data from {setup_script}")
        return True
        
    finally:
        client.close()


# ============================================================================
# Test Cases - Pattern 1: Standard Edge Table (Lineage)
# ============================================================================

@pytest.mark.edge_constraints
def test_edge_constraint_sql_generation(edge_constraints_schemas, edge_constraints_data):
    """Test that edge constraint appears in generated SQL for single-hop query"""
    query = """
        USE lineage MATCH (f:DataFile)-[r:COPIED_BY]->(t:DataFile) 
        WHERE f.file_id = 1 
        RETURN f.path, t.path
    """
    
    response = requests.post(
        f"{CLICKGRAPH_URL}/query",
        json={
            "query": query,
            "params": {},
            "sql_only": True
        }
    )
    
    assert response.status_code == 200, f"Query failed: {response.text}"
    result = response.json()
    sql = result["generated_sql"]
    
    print("Generated SQL:")
    print(sql)
    print()
    
    # Verify constraint is in the SQL
    assert "f.created_timestamp <= t.created_timestamp" in sql, \
        "Edge constraint not found in generated SQL"
    
    print("✅ Test passed: Edge constraint in SQL")


@pytest.mark.edge_constraints
def test_edge_constraint_filtering(edge_constraints_schemas, edge_constraints_data):
    """Test that edge constraint actually filters query results"""
    query = """
        USE lineage MATCH (f:DataFile)-[r:COPIED_BY]->(t:DataFile) 
        WHERE f.file_id = 1 
        RETURN f.path, t.path, f.timestamp AS from_ts, t.timestamp AS to_ts
    """
    
    response = requests.post(
        f"{CLICKGRAPH_URL}/query",
        json={
            "query": query,
            "params": {}
        }
    )
    
    assert response.status_code == 200, f"Query failed: {response.text}"
    result = response.json()
    
    rows = result.get("rows", [])
    print(f"Query returned {len(rows)} rows")
    
    # If there are results, verify constraint is satisfied
    for row in rows:
        from_ts = row.get("from_ts")
        to_ts = row.get("to_ts")
        if from_ts and to_ts:
            assert from_ts <= to_ts, f"Constraint violated: {from_ts} > {to_ts}"
    
    print("✅ Test passed: Edge constraint filtering")


@pytest.mark.edge_constraints
def test_edge_constraint_vlp(edge_constraints_schemas, edge_constraints_data):
    """Test that edge constraint works with variable-length paths"""
    query = """
        USE lineage MATCH (f:DataFile)-[r:COPIED_BY*1..3]->(t:DataFile) 
        WHERE f.file_id = 1 
        RETURN f.path, t.path
    """
    
    response = requests.post(
        f"{CLICKGRAPH_URL}/query",
        json={
            "query": query,
            "params": {},
            "sql_only": True
        }
    )
    
    assert response.status_code == 200, f"Query failed: {response.text}"
    result = response.json()
    sql = result["generated_sql"]
    
    print("Generated SQL (VLP):")
    print(sql)
    print()
    
    # For VLP, the constraint should be in the recursive CTE
    assert "WITH RECURSIVE" in sql, "Expected recursive CTE for VLP"
    
    # The constraint should appear in the CTE's WHERE clauses
    assert "created_timestamp <=" in sql, \
        "Edge constraint timestamp check not found in VLP SQL"
    
    print("✅ Test passed: Edge constraint in VLP")


@pytest.mark.edge_constraints
def test_query_without_constraint(edge_constraints_schemas, edge_constraints_data):
    """Test query for relationship type without constraints - should work normally"""
    # AUTHORED relationship has no constraints in lineage_schema.yaml
    query = """
        USE lineage MATCH (d:DataFile)-[r:AUTHORED]->(u:User) 
        WHERE d.file_id = 1 
        RETURN d.path, u.name
    """
    
    response = requests.post(
        f"{CLICKGRAPH_URL}/query",
        json={
            "query": query,
            "params": {},
            "sql_only": True
        }
    )
    
    assert response.status_code == 200, f"Query failed: {response.text}"
    result = response.json()
    sql = result["generated_sql"]
    
    print("Generated SQL (no constraints):")
    print(sql)
    print()
    
    # Should NOT contain timestamp constraint
    assert "created_timestamp <=" not in sql, \
        "Unexpected constraint in relationship without constraints"
    
    print("✅ Test passed: Query without constraints")


@pytest.mark.edge_constraints
def test_social_network_constraints(edge_constraints_schemas, edge_constraints_data):
    """Test standard node/edge pattern with age constraint"""
    print(f"\n--- Testing Social Network (User FOLLOWS User) ---")
    
    query = """
        USE social_constraints MATCH (a:User)-[f:FOLLOWS]->(b:User)
        RETURN a.username, a.age, b.username, b.age
    """
    
    response = requests.post(
        f"{CLICKGRAPH_URL}/query",
        json={"query": query, "sql_only": True}
    )
    
    assert response.status_code == 200, f"Query failed: {response.text}"
    sql = response.json()["generated_sql"]
    
    print("Generated SQL (social):")
    print(sql)
    
    # Verify constraint in SQL
    assert "a.age > b.age" in sql, "Social network age constraint not found in SQL"
    print("✅ Social network constraint found in SQL")


@pytest.mark.edge_constraints
def test_filesystem_fk_edge_constraints(edge_constraints_schemas, edge_constraints_data):
    """Test FK-edge pattern with security level constraint"""
    print(f"\n--- Testing Filesystem (File IN_FOLDER Folder) ---")
    
    query = """
        USE filesystem_constraints MATCH (f:File)-[r:IN_FOLDER]->(folder:Folder)
        RETURN f.name, f.security_level AS file_sec, folder.name, folder.security_level AS folder_sec
    """
    
    response = requests.post(
        f"{CLICKGRAPH_URL}/query",
        json={"query": query, "sql_only": True}
    )
    
    assert response.status_code == 200, f"Query failed: {response.text}"
    sql = response.json()["generated_sql"]
    
    print("Generated SQL (filesystem):")
    print(sql)
    
    # Verify constraint in SQL
    assert "f.security_level <= folder.security_level" in sql, \
           "Filesystem security constraint not found in SQL"
    print("✅ Filesystem constraint found in SQL")


@pytest.mark.edge_constraints
def test_denormalized_edge_constraints(edge_constraints_schemas, edge_constraints_data):
    """Test denormalized pattern where nodes are embedded in edge table"""
    print(f"\n--- Testing Denormalized Pattern (Airport FLIGHT Airport) ---")
    
    query = """
        USE travel_denormalized MATCH (origin:Airport)-[f:FLIGHT]->(dest:Airport)
        RETURN origin.code, origin.timezone_offset AS orig_tz, dest.code, dest.timezone_offset AS dest_tz
    """
    
    response = requests.post(
        f"{CLICKGRAPH_URL}/query",
        json={"query": query, "sql_only": True}
    )
    
    assert response.status_code == 200, f"Query failed: {response.text}"
    sql = response.json()["generated_sql"]
    
    print("Generated SQL (denormalized):")
    print(sql)
    
    # Verify constraint in SQL  
    assert "origin_timezone_offset" in sql and "dest_timezone_offset" in sql, \
           "Denormalized timezone columns not found in SQL"
    print("✅ Denormalized constraint found in SQL")


@pytest.mark.edge_constraints
def test_polymorphic_edge_constraints(edge_constraints_schemas, edge_constraints_data):
    """Test polymorphic edge pattern (multiple edge types in one table)"""
    print(f"\n--- Testing Polymorphic Pattern (Member interactions) ---")
    
    # Test MENTORS relationship specifically
    query = """
        USE community_polymorphic MATCH (mentor:Member)-[r:MENTORS]->(mentee:Member)
        RETURN mentor.username, mentor.reputation AS mentor_rep, mentee.username, mentee.reputation AS mentee_rep
    """
    
    response = requests.post(
        f"{CLICKGRAPH_URL}/query",
        json={"query": query, "sql_only": True}
    )
    
    assert response.status_code == 200, f"Query failed: {response.text}"
    sql = response.json()["generated_sql"]
    
    print("Generated SQL (polymorphic MENTORS):")
    print(sql)
    
    # Verify constraint in SQL
    assert "reputation" in sql, "Polymorphic reputation columns not found in SQL"
    
    # Verify type discriminator
    assert "interaction_type" in sql and "MENTORS" in sql, \
           "Polymorphic type discriminator not found in SQL"
    
    print("✅ Polymorphic constraint found in SQL")

