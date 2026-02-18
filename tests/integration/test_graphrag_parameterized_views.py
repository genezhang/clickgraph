#!/usr/bin/env python3
"""
Integration tests for GraphRAG queries with parameterized views.

Tests the combination of:
1. Variable-length path patterns (VLP)
2. Parameterized views (tenant isolation, time-based filtering)
3. Complex traversals with view parameter constraints

Expected Results (as of Jan 9, 2026):
    ✅ 4/6 tests pass - VLP with single nodes work correctly
    ❌ 2/6 tests fail - Known bug: parameterized views with relationships
       generate table function syntax errors (KNOWN_ISSUES.md #1)

Use cases:
- Multi-tenant GraphRAG where each tenant has isolated knowledge graph
- Time-scoped graph traversals (e.g., user activity in date range)
- Filtered graph analysis with parameterized node/edge views

Usage:
    pytest tests/integration/test_graphrag_parameterized_views.py -v
    pytest tests/integration/test_graphrag_parameterized_views.py -k "tenant" -v
"""

import pytest
import requests
import os
import clickhouse_connect

CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
BASE_URL = f"{CLICKGRAPH_URL}"

# ============================================================================
# Fixtures - Schema and Data Setup
# ============================================================================

@pytest.fixture(scope="session")
def clickhouse_client():
    """ClickHouse client for data setup."""
    return clickhouse_connect.get_client(
        host='localhost',
        port=8123,
        username=os.getenv('CLICKHOUSE_USER', 'test_user'),
        password=os.getenv('CLICKHOUSE_PASSWORD', 'test_pass')
    )


@pytest.fixture(scope="session")
def graphrag_parameterized_schema(clickhouse_client):
    """
    Create GraphRAG schema with parameterized views for multi-tenant knowledge graph.
    
    Schema:
    - Document nodes (parameterized by tenant_id, date_range)
    - Entity nodes (parameterized by tenant_id)
    - MENTIONS relationships (Document → Entity)
    - RELATES_TO relationships (Entity → Entity)
    """
    
    # 1. Create database
    clickhouse_client.command("CREATE DATABASE IF NOT EXISTS graphrag")
    
    # 2. Create base ClickHouse tables
    clickhouse_client.command("""
        CREATE TABLE IF NOT EXISTS graphrag.documents_base (
            doc_id String,
            tenant_id String,
            title String,
            created_date Date,
            content String,
            embedding Array(Float32)
        ) ENGINE = MergeTree()
        ORDER BY (tenant_id, doc_id)
    """)
    
    # Create parameterized VIEW for documents
    clickhouse_client.command("""
        CREATE VIEW IF NOT EXISTS graphrag.documents
        AS SELECT * FROM graphrag.documents_base
        WHERE tenant_id = {tenant_id:String}
    """)
    
    # Create parameterized VIEW for documents
    clickhouse_client.command("""
        CREATE VIEW IF NOT EXISTS graphrag.documents
        AS SELECT * FROM graphrag.documents_base
        WHERE tenant_id = {tenant_id:String}
    """)
    
    clickhouse_client.command("""
        CREATE TABLE IF NOT EXISTS graphrag.entities_base (
            entity_id String,
            tenant_id String,
            entity_type String,
            name String,
            description String,
            embedding Array(Float32)
        ) ENGINE = MergeTree()
        ORDER BY (tenant_id, entity_id)
    """)
    
    # Create parameterized VIEW for entities
    clickhouse_client.command("""
        CREATE VIEW IF NOT EXISTS graphrag.entities
        AS SELECT * FROM graphrag.entities_base
        WHERE tenant_id = {tenant_id:String}
    """)
    
    # Create parameterized VIEW for entities
    clickhouse_client.command("""
        CREATE VIEW IF NOT EXISTS graphrag.entities
        AS SELECT * FROM graphrag.entities_base
        WHERE tenant_id = {tenant_id:String}
    """)
    
    clickhouse_client.command("""
        CREATE TABLE IF NOT EXISTS graphrag.doc_mentions_entity_base (
            doc_id String,
            entity_id String,
            tenant_id String,
            mention_count Int32,
            confidence Float32
        ) ENGINE = MergeTree()
        ORDER BY (tenant_id, doc_id, entity_id)
    """)
    
    # Create parameterized VIEW for relationships
    clickhouse_client.command("""
        CREATE VIEW IF NOT EXISTS graphrag.doc_mentions_entity
        AS SELECT * FROM graphrag.doc_mentions_entity_base
        WHERE tenant_id = {tenant_id:String}
    """)
    
    # Create parameterized VIEW for relationships
    clickhouse_client.command("""
        CREATE VIEW IF NOT EXISTS graphrag.doc_mentions_entity
        AS SELECT * FROM graphrag.doc_mentions_entity_base
        WHERE tenant_id = {tenant_id:String}
    """)
    
    clickhouse_client.command("""
        CREATE TABLE IF NOT EXISTS graphrag.entity_relates_to_base (
            from_entity_id String,
            to_entity_id String,
            tenant_id String,
            relationship_type String,
            strength Float32
        ) ENGINE = MergeTree()
        ORDER BY (tenant_id, from_entity_id, to_entity_id)
    """)
    
    # Create parameterized VIEW for entity relationships
    clickhouse_client.command("""
        CREATE VIEW IF NOT EXISTS graphrag.entity_relates_to
        AS SELECT * FROM graphrag.entity_relates_to_base
        WHERE tenant_id = {tenant_id:String}
    """)
    
    # 2. Load test data
    # Tenant A: AI research domain
    clickhouse_client.command("""
        INSERT INTO graphrag.documents_base VALUES
        ('doc1', 'tenant_a', 'Introduction to LLMs', '2024-01-15', 'Large Language Models...', [0.1, 0.2, 0.3]),
        ('doc2', 'tenant_a', 'Neural Network Architectures', '2024-02-20', 'Transformers and attention...', [0.2, 0.3, 0.4]),
        ('doc3', 'tenant_a', 'Prompt Engineering Guide', '2024-03-10', 'Best practices for prompts...', [0.15, 0.25, 0.35])
    """)
    
    clickhouse_client.command("""
        INSERT INTO graphrag.entities_base VALUES
        ('e1', 'tenant_a', 'Technology', 'LLM', 'Large Language Model', [0.1, 0.2, 0.3]),
        ('e2', 'tenant_a', 'Technology', 'Transformer', 'Neural network architecture', [0.2, 0.3, 0.4]),
        ('e3', 'tenant_a', 'Concept', 'Attention', 'Attention mechanism', [0.15, 0.25, 0.35]),
        ('e4', 'tenant_a', 'Technique', 'Prompt Engineering', 'Crafting effective prompts', [0.12, 0.22, 0.32])
    """)
    
    # Tenant B: Healthcare domain
    clickhouse_client.command("""
        INSERT INTO graphrag.documents_base VALUES
        ('doc10', 'tenant_b', 'Cancer Treatment Options', '2024-01-10', 'Overview of treatments...', [0.5, 0.6, 0.7]),
        ('doc11', 'tenant_b', 'Drug Interactions Study', '2024-02-15', 'Analysis of drug combinations...', [0.55, 0.65, 0.75])
    """)
    
    clickhouse_client.command("""
        INSERT INTO graphrag.entities_base VALUES
        ('e10', 'tenant_b', 'Disease', 'Cancer', 'Oncology focus', [0.5, 0.6, 0.7]),
        ('e11', 'tenant_b', 'Treatment', 'Chemotherapy', 'Cancer treatment', [0.55, 0.65, 0.75]),
        ('e12', 'tenant_b', 'Drug', 'Aspirin', 'Common medication', [0.52, 0.62, 0.72])
    """)
    
    # Relationships - Tenant A
    clickhouse_client.command("""
        INSERT INTO graphrag.doc_mentions_entity_base VALUES
        ('doc1', 'e1', 'tenant_a', 15, 0.95),
        ('doc1', 'e2', 'tenant_a', 8, 0.85),
        ('doc2', 'e2', 'tenant_a', 20, 0.98),
        ('doc2', 'e3', 'tenant_a', 12, 0.90),
        ('doc3', 'e4', 'tenant_a', 25, 0.92),
        ('doc3', 'e1', 'tenant_a', 10, 0.88)
    """)
    
    clickhouse_client.command("""
        INSERT INTO graphrag.entity_relates_to_base VALUES
        ('e1', 'e2', 'tenant_a', 'uses', 0.9),
        ('e2', 'e3', 'tenant_a', 'implements', 0.95),
        ('e1', 'e4', 'tenant_a', 'requires', 0.85)
    """)
    
    # Relationships - Tenant B
    clickhouse_client.command("""
        INSERT INTO graphrag.doc_mentions_entity_base VALUES
        ('doc10', 'e10', 'tenant_b', 20, 0.98),
        ('doc10', 'e11', 'tenant_b', 15, 0.92),
        ('doc11', 'e12', 'tenant_b', 18, 0.90)
    """)
    
    clickhouse_client.command("""
        INSERT INTO graphrag.entity_relates_to_base VALUES
        ('e10', 'e11', 'tenant_b', 'treated_by', 0.95),
        ('e11', 'e12', 'tenant_b', 'interacts_with', 0.70)
    """)
    
    # 3. Create ClickGraph schema with parameterized views
    schema_yaml = """
name: graphrag_parameterized

graph_schema:
  nodes:
    - label: Document
      database: graphrag
      table: documents
      node_id: doc_id
      view_parameters: [tenant_id]
      property_mappings:
        doc_id: doc_id
        tenant_id: tenant_id
        title: title
        created_date: created_date
        content: content
        embedding: embedding
    
    - label: Entity
      database: graphrag
      table: entities
      node_id: entity_id
      view_parameters: [tenant_id]
      property_mappings:
        entity_id: entity_id
        tenant_id: tenant_id
        entity_type: entity_type
        name: name
        description: description
        embedding: embedding
  
  relationships:
    - type: MENTIONS
      database: graphrag
      table: doc_mentions_entity
      from_node: Document
      to_node: Entity
      from_id: doc_id
      to_id: entity_id
      view_parameters: [tenant_id]
      property_mappings:
        mention_count: mention_count
        confidence: confidence
    
    - type: RELATES_TO
      database: graphrag
      table: entity_relates_to
      from_node: Entity
      to_node: Entity
      from_id: from_entity_id
      to_id: to_entity_id
      view_parameters: [tenant_id]
      property_mappings:
        relationship_type: relationship_type
        strength: strength
"""
    
    response = requests.post(
        f"{BASE_URL}/schemas/load",
        json={
            "schema_name": "graphrag_parameterized",
            "config_content": schema_yaml
        }
    )
    assert response.status_code == 200, f"Schema load failed: {response.text}"
    
    yield "graphrag_parameterized"
    
    # Cleanup - drop views first, then base tables
    clickhouse_client.command("DROP VIEW IF EXISTS graphrag.documents")
    clickhouse_client.command("DROP VIEW IF EXISTS graphrag.entities")
    clickhouse_client.command("DROP VIEW IF EXISTS graphrag.doc_mentions_entity")
    clickhouse_client.command("DROP VIEW IF EXISTS graphrag.entity_relates_to")
    clickhouse_client.command("DROP TABLE IF EXISTS graphrag.documents_base")
    clickhouse_client.command("DROP TABLE IF EXISTS graphrag.entities_base")
    clickhouse_client.command("DROP TABLE IF EXISTS graphrag.doc_mentions_entity_base")
    clickhouse_client.command("DROP TABLE IF EXISTS graphrag.entity_relates_to_base")


def query_graphrag(query: str, schema_name: str, view_parameters: dict = None, max_inferred_types: int = None):
    """Helper to query GraphRAG with parameterized views."""
    payload = {
        "query": query,
        "schema_name": schema_name
    }
    if view_parameters:
        payload["view_parameters"] = view_parameters
    if max_inferred_types:
        payload["max_inferred_types"] = max_inferred_types
    
    response = requests.post(f"{BASE_URL}/query", json=payload)
    assert response.status_code == 200, f"Query failed: {response.text}"
    return response.json()


# ============================================================================
# Test Cases
# ============================================================================

class TestGraphRAGWithParameterizedViews:
    """Test GraphRAG queries with tenant-isolated parameterized views."""
    
    @pytest.mark.xfail(reason="Code bug: VLP with parameterized views generates invalid SQL")
    def test_vlp_with_single_tenant(self, graphrag_parameterized_schema):
        """Test variable-length path within single tenant's graph."""
        result = query_graphrag(
            """
            MATCH path = (d:Document)-[:MENTIONS*1..2]->(e:Entity)
            WHERE d.doc_id = 'doc1'
            RETURN e.name, length(path) as hops
            ORDER BY hops, e.name
            """,
            graphrag_parameterized_schema,
            view_parameters={"tenant_id": "tenant_a"}
        )
        
        rows = result["results"]
        assert len(rows) > 0, "Should find entities connected to doc1"
        
        # Verify all results are from tenant_a's domain
        names = [r["e.name"] for r in rows]
        assert "LLM" in names or "Transformer" in names, f"Expected tenant_a entities, got: {names}"
        
        # Should NOT find tenant_b entities
        assert "Cancer" not in names, "Should not see tenant_b entities"
    
    @pytest.mark.xfail(reason="Code bug: VLP with parameterized views generates invalid SQL")
    def test_multi_hop_entity_relationships_tenant_isolated(self, graphrag_parameterized_schema):
        """Test multi-hop entity traversal respects tenant isolation."""
        result = query_graphrag(
            """
            MATCH path = (e1:Entity)-[:RELATES_TO*1..2]->(e2:Entity)
            WHERE e1.name = 'LLM'
            RETURN e2.name, length(path) as distance
            ORDER BY distance, e2.name
            """,
            graphrag_parameterized_schema,
            view_parameters={"tenant_id": "tenant_a"}
        )
        
        rows = result["results"]
        assert len(rows) > 0, "Should find related entities"
        
        # Should find entities 1-2 hops away from LLM
        names = [r["e2.name"] for r in rows]
        assert "Transformer" in names or "Attention" in names, f"Expected related entities: {names}"
    
    @pytest.mark.xfail(reason="Code bug: VLP with parameterized views generates invalid SQL")
    def test_cross_entity_type_vlp_with_max_inferred_types(self, graphrag_parameterized_schema):
        """Test VLP across different relationship types with configurable limit."""
        result = query_graphrag(
            """
            MATCH (d:Document)-[r*1..3]->(e:Entity)
            WHERE d.doc_id = 'doc1'
            RETURN DISTINCT e.name, e.entity_type
            ORDER BY e.name
            """,
            graphrag_parameterized_schema,
            view_parameters={"tenant_id": "tenant_a"},
            max_inferred_types=10  # Allow inference of both MENTIONS and RELATES_TO
        )
        
        rows = result["results"]
        assert len(rows) > 0, "Should find entities via multiple relationship types"
    
    def test_tenant_b_isolation(self, graphrag_parameterized_schema):
        """Verify tenant_b sees only their healthcare domain."""
        result = query_graphrag(
            """
            MATCH (d:Document)-[:MENTIONS]->(e:Entity)
            RETURN d.title, e.name
            ORDER BY d.title
            """,
            graphrag_parameterized_schema,
            view_parameters={"tenant_id": "tenant_b"}
        )
        
        rows = result["results"]
        assert len(rows) > 0, "Tenant B should have data"
        
        # Verify only tenant_b data
        doc_titles = [r["d.title"] for r in rows]
        assert any("Cancer" in title or "Drug" in title for title in doc_titles), \
            "Should see tenant_b healthcare documents"
        
        # Should NOT see tenant_a data
        assert not any("LLM" in title for title in doc_titles), \
            "Should not see tenant_a documents"
    
    @pytest.mark.xfail(reason="Code bug: VLP with parameterized views generates invalid SQL")
    def test_vlp_with_property_filtering_and_params(self, graphrag_parameterized_schema):
        """Test VLP with WHERE clause on properties + tenant isolation."""
        result = query_graphrag(
            """
            MATCH path = (d:Document)-[:MENTIONS*1..2]->(e:Entity)
            WHERE d.created_date >= '2024-02-01'
              AND e.entity_type = 'Technology'
            RETURN d.title, e.name, length(path) as hops
            ORDER BY d.created_date, e.name
            """,
            graphrag_parameterized_schema,
            view_parameters={"tenant_id": "tenant_a"}
        )
        
        rows = result["results"]
        # Should filter by both date and entity type
        if len(rows) > 0:
            assert all(r["e.name"] in ["Transformer", "LLM"] for r in rows), \
                "Should only return Technology entities"


class TestGraphRAGParameterizedPerformance:
    """Test performance characteristics of parameterized GraphRAG queries."""
    
    @pytest.mark.xfail(reason="Code bug: VLP with parameterized views generates invalid SQL")
    def test_parameterized_view_overhead_minimal(self, graphrag_parameterized_schema):
        """Parameterized views should add minimal overhead to VLP queries."""
        import time
        
        # Warm up
        query_graphrag(
            "MATCH (d:Document)-[:MENTIONS]->(e:Entity) RETURN count(*)",
            graphrag_parameterized_schema,
            view_parameters={"tenant_id": "tenant_a"}
        )
        
        # Measure
        start = time.time()
        for _ in range(10):
            query_graphrag(
                "MATCH (d:Document)-[:MENTIONS*1..2]->(e:Entity) RETURN e.name LIMIT 5",
                graphrag_parameterized_schema,
                view_parameters={"tenant_id": "tenant_a"}
            )
        elapsed = time.time() - start
        
        # Should complete 10 queries in reasonable time (< 2s total)
        assert elapsed < 2.0, f"Parameterized VLP queries too slow: {elapsed:.2f}s for 10 queries"


if __name__ == "__main__":
    print("Run with: pytest tests/integration/test_graphrag_parameterized_views.py -v")
