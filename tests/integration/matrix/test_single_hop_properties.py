"""
Single-Hop Property Selection Tests

Tests single-hop patterns with specific property selection across all schema types.
This addresses the gap discovered in Jan 2026 where denormalized schemas failed with
single-hop property queries due to missing alias transfer.

Test Pattern: MATCH (a:Label1)-[r:REL]->(b:Label2) RETURN a.prop1, b.prop2

Run with: pytest tests/integration/matrix/test_single_hop_properties.py -v
"""

import pytest
import requests
import os

# Import from local matrix conftest
from .conftest import (
    SCHEMAS, SchemaConfig, SchemaType,
    execute_query, check_server_health, CLICKGRAPH_URL
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture(scope="session")
def server_running():
    """Ensure ClickGraph server is running"""
    if not check_server_health():
        pytest.skip("ClickGraph server not running")
    return True


@pytest.fixture(scope="session", autouse=True)
def load_all_schemas():
    """Load all required schemas before tests."""
    import yaml
    for schema_name, schema_config in SCHEMAS.items():
        yaml_path = schema_config.yaml_path
        try:
            with open(yaml_path, 'r') as f:
                schema_yaml = f.read()
            response = requests.post(
                f"{CLICKGRAPH_URL}/schemas/load",
                json={
                    "schema_name": schema_name,
                    "config_content": schema_yaml
                },
                timeout=10
            )
            if response.status_code != 200:
                print(f"Warning: Failed to load schema {schema_name}: {response.text}")
        except Exception as e:
            print(f"Warning: Error loading schema {schema_name}: {e}")


# =============================================================================
# Test Class
# =============================================================================

class TestSingleHopPropertySelection:
    """
    Test single-hop patterns with specific property selection.
    
    Pattern: MATCH (a)-[r]->(b) RETURN a.prop1, b.prop2
    
    This specifically tests the denormalized alias transfer fix (Jan 2026).
    Before fix: Failed with "Unknown expression identifier 't.prop'"
    After fix: Correctly uses edge alias 'r' for denormalized schemas
    """
    
    @pytest.mark.parametrize("schema_name", list(SCHEMAS.keys()))
    def test_single_hop_both_node_properties(self, server_running, schema_name):
        """
        Test: MATCH (a)-[r]->(b) RETURN a.prop, b.prop
        
        Tests both left and right node property access in single-hop pattern.
        This is the primary pattern that was failing with denormalized schemas.
        """
        schema = SCHEMAS[schema_name]
        
        # Skip schemas without appropriate structure
        if schema.schema_type == SchemaType.MULTI_TABLE_LABEL:
            pytest.skip("MULTI_TABLE_LABEL schemas have limited standalone query support")
        
        # Get first node label and edge type
        if not schema.node_labels or not schema.edge_types:
            pytest.skip(f"Schema {schema_name} has no node labels or edge types")
        
        label = schema.node_labels[0]
        edge = schema.edge_types[0]
        
        # Get properties for this label
        if label not in schema.node_properties or not schema.node_properties[label]:
            pytest.skip(f"Schema {schema_name} has no properties for label {label}")
        
        # Get first two properties (or same property twice if only one)
        props = schema.node_properties[label]
        prop1 = props[0][0]
        prop2 = props[1][0] if len(props) > 1 else props[0][0]
        
        # Build query: MATCH (a:Label)-[r:EDGE]->(b:Label) RETURN a.prop1, b.prop2
        query = f"MATCH (a:{label})-[r:{edge}]->(b) RETURN a.{prop1}, b.{prop2} LIMIT 10"
        
        result = execute_query(query, schema_name=schema_name)
        assert result["success"], (
            f"Single-hop both-node property query failed for {schema_name} ({schema.schema_type.name})\n"
            f"Query: {query}\n"
            f"Error: {result['body']}"
        )
    
    @pytest.mark.parametrize("schema_name", list(SCHEMAS.keys()))
    def test_single_hop_left_node_only(self, server_running, schema_name):
        """
        Test: MATCH (a)-[r]->(b) RETURN a.prop
        
        Tests only left node property access.
        """
        schema = SCHEMAS[schema_name]
        
        if schema.schema_type == SchemaType.MULTI_TABLE_LABEL:
            pytest.skip("MULTI_TABLE_LABEL schemas have limited standalone query support")
        
        if not schema.node_labels or not schema.edge_types:
            pytest.skip(f"Schema {schema_name} has no node labels or edge types")
        
        label = schema.node_labels[0]
        edge = schema.edge_types[0]
        
        if label not in schema.node_properties or not schema.node_properties[label]:
            pytest.skip(f"Schema {schema_name} has no properties for label {label}")
        
        prop = schema.node_properties[label][0][0]
        
        query = f"MATCH (a:{label})-[r:{edge}]->(b) RETURN a.{prop} LIMIT 10"
        
        result = execute_query(query, schema_name=schema_name)
        assert result["success"], (
            f"Single-hop left-node property query failed for {schema_name} ({schema.schema_type.name})\n"
            f"Query: {query}\n"
            f"Error: {result['body']}"
        )
    
    @pytest.mark.parametrize("schema_name", list(SCHEMAS.keys()))
    def test_single_hop_right_node_only(self, server_running, schema_name):
        """
        Test: MATCH (a)-[r]->(b) RETURN b.prop
        
        Tests only right node property access.
        """
        schema = SCHEMAS[schema_name]
        
        if schema.schema_type == SchemaType.MULTI_TABLE_LABEL:
            pytest.skip("MULTI_TABLE_LABEL schemas have limited standalone query support")
        
        if not schema.node_labels or not schema.edge_types:
            pytest.skip(f"Schema {schema_name} has no node labels or edge types")
        
        label = schema.node_labels[0]
        edge = schema.edge_types[0]
        
        if label not in schema.node_properties or not schema.node_properties[label]:
            pytest.skip(f"Schema {schema_name} has no properties for label {label}")
        
        prop = schema.node_properties[label][0][0]
        
        query = f"MATCH (a:{label})-[r:{edge}]->(b) RETURN b.{prop} LIMIT 10"
        
        result = execute_query(query, schema_name=schema_name)
        assert result["success"], (
            f"Single-hop right-node property query failed for {schema_name} ({schema.schema_type.name})\n"
            f"Query: {query}\n"
            f"Error: {result['body']}"
        )
    
    @pytest.mark.parametrize("schema_name", list(SCHEMAS.keys()))
    def test_single_hop_with_property_filter(self, server_running, schema_name):
        if schema_name == 'group_membership':
            pytest.xfail("Code bug: group_membership schema generates invalid SQL for single-hop queries")
        """
        Test: MATCH (a)-[r]->(b) WHERE a.prop = value RETURN a.prop, b.prop
        
        Tests property access with WHERE clause filtering.
        """
        schema = SCHEMAS[schema_name]
        
        if schema.schema_type == SchemaType.MULTI_TABLE_LABEL:
            pytest.skip("MULTI_TABLE_LABEL schemas have limited standalone query support")
        
        if not schema.node_labels or not schema.edge_types:
            pytest.skip(f"Schema {schema_name} has no node labels or edge types")
        
        label = schema.node_labels[0]
        edge = schema.edge_types[0]
        
        if label not in schema.node_properties or not schema.node_properties[label]:
            pytest.skip(f"Schema {schema_name} has no properties for label {label}")
        
        # Get property and sample value for WHERE clause
        props = schema.node_properties[label]
        prop = props[0][0]
        prop_type = props[0][1]
        
        # Build simple filter based on type
        if prop_type in ["int", "integer", "uint32", "uint64"]:
            filter_expr = f"a.{prop} > 0"
        elif prop_type in ["string", "str"]:
            filter_expr = f"a.{prop} IS NOT NULL"
        elif prop_type == "bool":
            filter_expr = f"a.{prop} = true"
        else:
            filter_expr = f"a.{prop} IS NOT NULL"
        
        query = f"MATCH (a:{label})-[r:{edge}]->(b) WHERE {filter_expr} RETURN a.{prop}, b.{prop} LIMIT 10"
        
        result = execute_query(query, schema_name=schema_name)
        assert result["success"], (
            f"Single-hop filtered property query failed for {schema_name} ({schema.schema_type.name})\n"
            f"Query: {query}\n"
            f"Error: {result['body']}"
        )
    
    @pytest.mark.parametrize("schema_name", list(SCHEMAS.keys()))
    def test_single_hop_edge_property(self, server_running, schema_name):
        if schema_name == 'group_membership':
            pytest.xfail("Code bug: group_membership schema generates invalid SQL for single-hop queries")
        """
        Test: MATCH (a)-[r]->(b) RETURN r.prop
        
        Tests edge property access (if schema has edge properties).
        """
        schema = SCHEMAS[schema_name]
        
        if schema.schema_type == SchemaType.MULTI_TABLE_LABEL:
            pytest.skip("MULTI_TABLE_LABEL schemas have limited standalone query support")
        
        if not schema.node_labels or not schema.edge_types:
            pytest.skip(f"Schema {schema_name} has no node labels or edge types")
        
        label = schema.node_labels[0]
        edge = schema.edge_types[0]
        
        # Skip if no edge properties
        if edge not in schema.edge_properties or not schema.edge_properties[edge]:
            pytest.skip(f"Schema {schema_name} has no properties for edge {edge}")
        
        edge_prop = schema.edge_properties[edge][0][0]
        
        query = f"MATCH (a:{label})-[r:{edge}]->(b) RETURN r.{edge_prop} LIMIT 10"
        
        result = execute_query(query, schema_name=schema_name)
        assert result["success"], (
            f"Single-hop edge property query failed for {schema_name} ({schema.schema_type.name})\n"
            f"Query: {query}\n"
            f"Error: {result['body']}"
        )
    
    @pytest.mark.parametrize("schema_name", list(SCHEMAS.keys()))
    def test_single_hop_mixed_properties(self, server_running, schema_name):
        if schema_name == 'group_membership':
            pytest.xfail("Code bug: group_membership schema generates invalid SQL for single-hop queries")
        """
        Test: MATCH (a)-[r]->(b) RETURN a.prop1, r.prop2, b.prop3
        
        Tests mixed node and edge property access.
        """
        schema = SCHEMAS[schema_name]
        
        if schema.schema_type == SchemaType.MULTI_TABLE_LABEL:
            pytest.skip("MULTI_TABLE_LABEL schemas have limited standalone query support")
        
        if not schema.node_labels or not schema.edge_types:
            pytest.skip(f"Schema {schema_name} has no node labels or edge types")
        
        label = schema.node_labels[0]
        edge = schema.edge_types[0]
        
        if label not in schema.node_properties or not schema.node_properties[label]:
            pytest.skip(f"Schema {schema_name} has no properties for label {label}")
        
        # Skip if no edge properties
        if edge not in schema.edge_properties or not schema.edge_properties[edge]:
            pytest.skip(f"Schema {schema_name} has no properties for edge {edge}")
        
        node_prop = schema.node_properties[label][0][0]
        edge_prop = schema.edge_properties[edge][0][0]
        
        query = f"MATCH (a:{label})-[r:{edge}]->(b) RETURN a.{node_prop}, r.{edge_prop}, b.{node_prop} LIMIT 10"
        
        result = execute_query(query, schema_name=schema_name)
        assert result["success"], (
            f"Single-hop mixed property query failed for {schema_name} ({schema.schema_type.name})\n"
            f"Query: {query}\n"
            f"Error: {result['body']}"
        )


# =============================================================================
# Schema-Specific Tests (Regression Tests for Known Issues)
# =============================================================================

class TestDenormalizedSingleHop:
    """
    Regression tests for denormalized schema bug (Jan 2026).
    
    Bug: Single-hop denormalized queries generated SQL with wrong table alias
    Fix: Transfer denormalized aliases from PlanCtx to task-local storage
    """
    
    def test_social_integration_follows(self, server_running):
        """Test User-[FOLLOWS]->User pattern (traditional schema)"""
        query = "MATCH (a:User)-[r:FOLLOWS]->(b:User) RETURN a.name, b.name LIMIT 10"
        result = execute_query(query, schema_name="social_integration")
        assert result["success"], f"Query failed: {query}\nError: {result['body']}"
    
    def test_filesystem_parent(self, server_running):
        """Test Object-[PARENT]->Object pattern (FK-edge schema)"""
        query = "MATCH (a:Object)-[r:PARENT]->(b:Object) RETURN a.name, b.name LIMIT 10"
        result = execute_query(query, schema_name="filesystem")
        assert result["success"], f"Query failed: {query}\nError: {result['body']}"
    
    @pytest.mark.xfail(reason="Code bug: group_membership denormalized schema generates invalid SQL")
    def test_group_membership(self, server_running):
        """Test User-[MEMBER_OF]->Group pattern (traditional schema)"""
        query = "MATCH (a:User)-[r:MEMBER_OF]->(b:Group) RETURN a.name, b.name LIMIT 10"
        result = execute_query(query, schema_name="group_membership")
        assert result["success"], f"Query failed: {query}\nError: {result['body']}"
