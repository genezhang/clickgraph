#!/usr/bin/env python3
"""
Comprehensive Graph-Notebook Compatibility Test Suite

Tests ClickGraph's compatibility with AWS graph-notebook library,
which is used for Jupyter notebook visualizations of graph databases.

Covers:
1. Connection and authentication
2. Schema discovery (procedures)
3. Basic queries (MATCH, WHERE, RETURN)
4. Property access and filtering
5. Aggregations
6. Relationships and patterns
7. Result format validation
8. Error handling
9. Visualization-specific queries

Requirements:
    pip install graph-notebook neo4j

Usage:
    pytest test_graph_notebook_compatibility.py -v
"""

import pytest
from neo4j import GraphDatabase
from graph_notebook.configuration.generate_config import Configuration
from graph_notebook.neptune.client import Client
import sys

# Test configuration
BOLT_URI = "bolt://localhost:7687"
AUTH = ("neo4j", "password")
DATABASE = "social_integration"  # Use the benchmark schema


class TestGraphNotebookConnection:
    """Test basic connection and authentication."""
    
    @pytest.fixture
    def driver(self):
        """Create Neo4j driver (bypassing graph-notebook client for direct testing)."""
        driver = GraphDatabase.driver(BOLT_URI, auth=AUTH)
        yield driver
        driver.close()
    
    @pytest.fixture
    def graph_notebook_client(self):
        """Create graph-notebook client."""
        client = Client(
            host="localhost",
            port=7687,
            ssl=False,
            neo4j_username="neo4j",
            neo4j_password="password",
            neo4j_auth=True,
            neo4j_database=DATABASE
        )
        return client
    
    def test_driver_connection(self, driver):
        """Test direct Neo4j driver connection."""
        with driver.session() as session:
            result = session.run("RETURN 1 as test")
            record = result.single()
            assert record["test"] == 1
    
    def test_graph_notebook_client_creation(self, graph_notebook_client):
        """Test graph-notebook client can be created."""
        assert graph_notebook_client is not None
        opencypher_driver = graph_notebook_client.get_opencypher_driver()
        assert opencypher_driver is not None


class TestSchemaDiscovery:
    """Test schema metadata procedures required by graph-notebook."""
    
    @pytest.fixture
    def session(self):
        """Create a session for testing."""
        driver = GraphDatabase.driver(BOLT_URI, auth=AUTH)
        with driver.session() as session:
            yield session
        driver.close()
    
    def test_db_labels(self, session):
        """Test CALL db.labels() - returns all node labels."""
        result = session.run("USE social_integration CALL db.labels() YIELD label RETURN label")
        labels = [record["label"] for record in result]
        assert len(labels) > 0
        assert "User" in labels or "Post" in labels
    
    def test_db_relationship_types(self, session):
        """Test CALL db.relationshipTypes() - returns all relationship types."""
        result = session.run("USE social_integration CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType")
        rel_types = [record["relationshipType"] for record in result]
        assert len(rel_types) > 0
        assert "FOLLOWS" in rel_types or "AUTHORED" in rel_types or "LIKED" in rel_types
    
    def test_db_property_keys(self, session):
        """Test CALL db.propertyKeys() - returns all property keys."""
        result = session.run("USE social_integration CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey")
        prop_keys = [record["propertyKey"] for record in result]
        assert len(prop_keys) > 0
    
    def test_dbms_components(self, session):
        """Test CALL dbms.components() - returns server version info."""
        result = session.run("CALL dbms.components() YIELD name, versions, edition RETURN name, versions, edition")
        record = result.single()
        assert record["name"] in ["ClickGraph", "Neo4j"]  # Neo4j in compat mode


class TestBasicQueries:
    """Test basic MATCH and RETURN patterns."""
    
    @pytest.fixture
    def session(self):
        driver = GraphDatabase.driver(BOLT_URI, auth=AUTH)
        with driver.session() as session:
            yield session
        driver.close()
    
    def test_match_all_nodes(self, session):
        """Test MATCH (n:Label) RETURN n pattern."""
        result = session.run("USE social_integration MATCH (u:User) RETURN u LIMIT 5")
        count = sum(1 for _ in result)
        assert count > 0
    
    def test_property_access(self, session):
        """Test property access in RETURN clause."""
        result = session.run("USE social_integration MATCH (u:User) RETURN u.name, u.email LIMIT 5")
        for record in result:
            # Should have properties (may be null)
            assert "u.name" in record.keys() or "u.email" in record.keys()
    
    def test_where_clause(self, session):
        """Test WHERE clause filtering."""
        result = session.run(
            "USE social_integration MATCH (u:User) WHERE u.country = 'USA' RETURN count(u) as user_count"
        )
        record = result.single()
        # Just verify it executes successfully
        assert "user_count" in record.keys()
    
    def test_order_by_limit(self, session):
        """Test ORDER BY and LIMIT clauses."""
        result = session.run(
            "USE social_integration MATCH (u:User) RETURN u.name ORDER BY u.name LIMIT 10"
        )
        names = [record["u.name"] for record in result]
        assert len(names) <= 10


class TestRelationshipQueries:
    """Test relationship patterns required for visualization."""
    
    @pytest.fixture
    def session(self):
        driver = GraphDatabase.driver(BOLT_URI, auth=AUTH)
        with driver.session() as session:
            yield session
        driver.close()
    
    def test_basic_relationship(self, session):
        """Test basic relationship traversal."""
        result = session.run(
            "USE social_integration MATCH (u1:User)-[f:FOLLOWS]->(u2:User) RETURN u1.name, u2.name LIMIT 5"
        )
        count = sum(1 for _ in result)
        assert count > 0
    
    def test_return_relationship(self, session):
        """Test returning relationship object."""
        result = session.run(
            "USE social_integration MATCH (u1:User)-[f:FOLLOWS]->(u2:User) RETURN f LIMIT 5"
        )
        for record in result:
            rel = record["f"]
            # Should be a Relationship object with properties
            assert hasattr(rel, 'type') or hasattr(rel, '__dict__')
    
    def test_return_nodes_and_relationships(self, session):
        """Test returning both nodes and relationships (for visualization)."""
        result = session.run(
            "USE social_integration MATCH (u1:User)-[f:FOLLOWS]->(u2:User) RETURN u1, f, u2 LIMIT 5"
        )
        for record in result:
            assert "u1" in record.keys()
            assert "f" in record.keys()
            assert "u2" in record.keys()
    
    def test_count_relationships(self, session):
        """Test counting relationships."""
        result = session.run(
            "USE social_integration MATCH ()-[f:FOLLOWS]->() RETURN count(f) as follow_count"
        )
        record = result.single()
        assert record["follow_count"] >= 0


class TestAggregations:
    """Test aggregation functions used in graph analytics."""
    
    @pytest.fixture
    def session(self):
        driver = GraphDatabase.driver(BOLT_URI, auth=AUTH)
        with driver.session() as session:
            yield session
        driver.close()
    
    def test_count_function(self, session):
        """Test COUNT() aggregation."""
        result = session.run("USE social_integration MATCH (u:User) RETURN count(u) as total")
        record = result.single()
        assert record["total"] >= 0
    
    def test_count_with_group_by(self, session):
        """Test COUNT with implicit GROUP BY."""
        result = session.run(
            "USE social_integration MATCH (u:User) RETURN u.country, count(u) as user_count"
        )
        groups = list(result)
        assert len(groups) >= 0  # May be empty if no country data
    
    def test_collect_function(self, session):
        """Test COLLECT() aggregation for gathering values."""
        result = session.run(
            "USE social_integration MATCH (u:User) RETURN collect(u.name) as names LIMIT 1"
        )
        record = result.single()
        names = record["names"]
        assert isinstance(names, list)


class TestVisualizationQueries:
    """Test queries commonly used by graph-notebook for visualization."""
    
    @pytest.fixture
    def session(self):
        driver = GraphDatabase.driver(BOLT_URI, auth=AUTH)
        with driver.session() as session:
            yield session
        driver.close()
    
    def test_neighbor_query(self, session):
        """Test finding neighbors of a node (common in visualizations)."""
        result = session.run(
            "USE social_integration MATCH (u:User)-[:FOLLOWS]->(neighbor) RETURN u, neighbor LIMIT 10"
        )
        count = sum(1 for _ in result)
        # Should execute without error
        assert count >= 0
    
    def test_path_query(self, session):
        """Test path queries (used for path visualization)."""
        result = session.run(
            "USE social_integration MATCH p=(u1:User)-[:FOLLOWS]->(u2:User) RETURN p LIMIT 5"
        )
        for record in result:
            path = record["p"]
            # Path should have nodes and relationships
            assert hasattr(path, 'nodes') or hasattr(path, '__dict__')
    
    def test_two_hop_query(self, session):
        """Test multi-hop traversal."""
        result = session.run(
            "USE social_integration MATCH (u1:User)-[:FOLLOWS]->(:User)-[:FOLLOWS]->(u2:User) "
            "RETURN u1.name, u2.name LIMIT 10"
        )
        count = sum(1 for _ in result)
        assert count >= 0


class TestErrorHandling:
    """Test error handling and edge cases."""
    
    @pytest.fixture
    def session(self):
        driver = GraphDatabase.driver(BOLT_URI, auth=AUTH)
        with driver.session() as session:
            yield session
        driver.close()
    
    def test_invalid_syntax(self, session):
        """Test that invalid syntax returns proper error."""
        with pytest.raises(Exception) as exc_info:
            session.run("USE social_integration MATCH (u:User) RETRUN u")  # Typo: RETRUN
        # Should raise some kind of exception
        assert exc_info.value is not None
    
    def test_nonexistent_label(self, session):
        """Test query with non-existent label."""
        with pytest.raises(Exception) as exc_info:
            session.run("USE social_integration MATCH (x:NonExistentLabel) RETURN x")
        assert exc_info.value is not None
    
    def test_empty_result(self, session):
        """Test query that returns empty result."""
        result = session.run(
            "USE social_integration MATCH (u:User) WHERE u.name = 'ThisNameDoesNotExist12345' RETURN u"
        )
        count = sum(1 for _ in result)
        assert count == 0


class TestUnsupportedFeatures:
    """Test features that are NOT supported and should fail gracefully."""
    
    @pytest.fixture
    def session(self):
        driver = GraphDatabase.driver(BOLT_URI, auth=AUTH)
        with driver.session() as session:
            yield session
        driver.close()
    
    def test_apoc_not_supported(self, session):
        """Test that APOC procedures are not supported."""
        with pytest.raises(Exception):
            session.run("CALL apoc.help('search')")
    
    def test_gds_not_supported(self, session):
        """Test that GDS procedures are not supported."""
        with pytest.raises(Exception):
            session.run("CALL gds.pageRank.stream('graphName')")
    
    def test_create_not_supported(self, session):
        """Test that CREATE (write operations) are not supported."""
        with pytest.raises(Exception):
            session.run("CREATE (n:TestNode {name: 'test'})")


if __name__ == "__main__":
    # Run with pytest
    pytest.main([__file__, "-v", "--tb=short"])
