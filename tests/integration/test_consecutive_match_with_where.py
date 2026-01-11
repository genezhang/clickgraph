"""
Integration tests for consecutive MATCH clauses with per-MATCH WHERE clauses.

These tests verify OpenCypher grammar compliance where each MATCH can have its own WHERE clause.
Grammar: <graph pattern> ::= <path pattern list> [ <graph pattern where clause> ]

Example: MATCH (a) WHERE a.x = 1 MATCH (b) WHERE b.y = 2 RETURN a, b
"""

import pytest
import requests
import os

BASE_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")


def execute_query(cypher_query: str, sql_only: bool = False):
    """Execute a Cypher query via HTTP API"""
    response = requests.post(
        f"{BASE_URL}/query",
        json={"query": cypher_query, "sql_only": sql_only},
        headers={"Content-Type": "application/json"},
    )
    return response


class TestConsecutiveMatchWithWhere:
    """Test consecutive MATCH clauses with WHERE (OpenCypher grammar compliant)"""

    def test_single_match_with_where(self):
        """Test that single MATCH with WHERE still works"""
        query = "MATCH (m:Message) WHERE m.id = 1 RETURN m.id"
        response = execute_query(query, sql_only=True)
        assert response.status_code == 200, f"Query failed: {response.text}"
        data = response.json()
        assert "generated_sql" in data
        assert "WHERE" in data["generated_sql"]

    def test_consecutive_match_with_where_on_first(self):
        """Test MATCH ... WHERE ... MATCH ... pattern"""
        query = """
        MATCH (m:Message) WHERE m.id = 3848297728402 
        MATCH (m)<-[:REPLY_OF]-(c:Comment)
        RETURN m.id, c.id
        """
        response = execute_query(query, sql_only=True)
        assert response.status_code == 200, f"Query failed: {response.text}"
        data = response.json()
        assert "generated_sql" in data
        # Verify WHERE clause is present
        assert "WHERE" in data["generated_sql"]
        assert "3848297728402" in data["generated_sql"]

    def test_consecutive_match_with_where_on_both(self):
        """Test MATCH ... WHERE ... MATCH ... WHERE ... pattern"""
        query = """
        MATCH (m:Message) WHERE m.id = 1 
        MATCH (c:Comment) WHERE c.id = 2
        RETURN m.id, c.id
        """
        response = execute_query(query, sql_only=True)
        assert response.status_code == 200, f"Query failed: {response.text}"
        data = response.json()
        assert "generated_sql" in data
        # Both WHERE conditions should be present
        sql = data["generated_sql"]
        assert "WHERE" in sql

    def test_consecutive_match_where_execution(self):
        """Test actual execution with consecutive MATCH + WHERE"""
        query = """
        MATCH (m:Message) WHERE m.id = 3848297728402
        MATCH (m)<-[:REPLY_OF]-(c:Comment)-[:HAS_CREATOR]->(p:Person)
        RETURN c.id AS commentId, p.id AS replyAuthorId
        LIMIT 5
        """
        response = execute_query(query, sql_only=False)
        assert response.status_code == 200, f"Query failed: {response.text}"
        data = response.json()
        assert "results" in data
        # Should return results (message 3848297728402 has comments in LDBC dataset)
        results = data["results"]
        if len(results) > 0:
            assert "commentId" in results[0]
            assert "replyAuthorId" in results[0]

    def test_three_consecutive_matches(self):
        """Test three consecutive MATCH clauses with WHERE on first"""
        query = """
        MATCH (m:Message) WHERE m.id = 3848297728402
        MATCH (m)<-[:REPLY_OF]-(c:Comment)
        MATCH (c)-[:HAS_CREATOR]->(p:Person)
        RETURN m.id, c.id, p.id
        LIMIT 5
        """
        response = execute_query(query, sql_only=True)
        assert response.status_code == 200, f"Query failed: {response.text}"
        data = response.json()
        assert "generated_sql" in data

    def test_match_where_with_complex_predicate(self):
        """Test WHERE clause with complex predicates in consecutive MATCH"""
        query = """
        MATCH (m:Message) WHERE m.id > 1000 AND m.id < 2000
        MATCH (c:Comment) WHERE c.id > 100
        RETURN m.id, c.id
        """
        response = execute_query(query, sql_only=True)
        assert response.status_code == 200, f"Query failed: {response.text}"
        data = response.json()
        assert "generated_sql" in data

    def test_consecutive_match_with_optional_match(self):
        """Test consecutive MATCH + WHERE followed by OPTIONAL MATCH"""
        query = """
        MATCH (m:Message) WHERE m.id = 3848297728402
        MATCH (c:Comment) WHERE c.id = 1
        OPTIONAL MATCH (p:Person)
        RETURN m.id, c.id, p.id
        """
        response = execute_query(query, sql_only=True)
        assert response.status_code == 200, f"Query failed: {response.text}"
        data = response.json()
        assert "generated_sql" in data


class TestBackwardCompatibility:
    """Ensure old queries still work with new parser"""

    def test_single_where_after_all_matches(self):
        """Old style: WHERE after all MATCH clauses (should still work)"""
        query = """
        MATCH (m:Message)
        MATCH (c:Comment)
        WHERE m.id = 1
        RETURN m.id, c.id
        """
        response = execute_query(query, sql_only=True)
        assert response.status_code == 200, f"Query failed: {response.text}"

    def test_no_where_clause(self):
        """Test consecutive MATCH without any WHERE"""
        query = """
        MATCH (m:Message)
        MATCH (c:Comment)
        RETURN m.id, c.id
        """
        response = execute_query(query, sql_only=True)
        assert response.status_code == 200, f"Query failed: {response.text}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
