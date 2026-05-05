"""
Integration tests for consecutive MATCH clauses with per-MATCH WHERE clauses.

These tests verify OpenCypher grammar compliance where each MATCH can have its own WHERE clause.
Grammar: <graph pattern> ::= <path pattern list> [ <graph pattern where clause> ]

Example: MATCH (a) WHERE a.x = 1 MATCH (b) WHERE b.y = 2 RETURN a, b

Uses ldbc_snb schema (Comment, Person, Forum, Tag labels).
"""

import pytest
import requests
import os

BASE_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:7475")


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
        query = "USE ldbc_snb MATCH (c:Comment) WHERE c.commentId = 1 RETURN c.commentId"
        response = execute_query(query, sql_only=True)
        assert response.status_code == 200, f"Query failed: {response.text}"
        data = response.json()
        assert "generated_sql" in data
        assert "WHERE" in data["generated_sql"]

    def test_consecutive_match_with_where_on_first(self):
        """Test MATCH ... WHERE ... MATCH ... pattern"""
        query = """
        USE ldbc_snb
        MATCH (c:Comment) WHERE c.commentId = 100
        MATCH (c)-[:HAS_CREATOR]->(p:Person)
        RETURN c.commentId, p.personId
        """
        response = execute_query(query, sql_only=True)
        assert response.status_code == 200, f"Query failed: {response.text}"
        data = response.json()
        assert "generated_sql" in data
        # Verify WHERE clause is present
        assert "WHERE" in data["generated_sql"]
        assert "100" in data["generated_sql"]

    def test_consecutive_match_with_where_on_both(self):
        """Test MATCH ... WHERE ... MATCH ... WHERE ... pattern"""
        query = """
        USE ldbc_snb
        MATCH (c:Comment) WHERE c.commentId = 1
        MATCH (p:Person) WHERE p.personId = 2
        RETURN c.commentId, p.personId
        """
        response = execute_query(query, sql_only=True)
        assert response.status_code == 200, f"Query failed: {response.text}"
        data = response.json()
        assert "generated_sql" in data
        # Both WHERE conditions should be present
        sql = data["generated_sql"]
        assert "WHERE" in sql

    @pytest.mark.xfail(reason="Requires LDBC database with actual data loaded")
    def test_consecutive_match_where_execution(self):
        """Test actual execution with consecutive MATCH + WHERE"""
        query = """
        USE ldbc_snb
        MATCH (c:Comment) WHERE c.commentId = 100
        MATCH (c)-[:HAS_CREATOR]->(p:Person)
        RETURN c.commentId AS commentId, p.personId AS creatorId
        LIMIT 5
        """
        response = execute_query(query, sql_only=False)
        assert response.status_code == 200, f"Query failed: {response.text}"
        data = response.json()
        assert "results" in data
        results = data["results"]
        if len(results) > 0:
            assert "commentId" in results[0]
            assert "creatorId" in results[0]

    def test_three_consecutive_matches(self):
        """Test three consecutive MATCH clauses with WHERE on first"""
        query = """
        USE ldbc_snb
        MATCH (c:Comment) WHERE c.commentId = 100
        MATCH (c)<-[:REPLY_OF]-(reply:Comment)
        MATCH (reply)-[:HAS_CREATOR]->(p:Person)
        RETURN c.commentId, reply.commentId, p.personId
        LIMIT 5
        """
        response = execute_query(query, sql_only=True)
        assert response.status_code == 200, f"Query failed: {response.text}"
        data = response.json()
        assert "generated_sql" in data

    def test_match_where_with_complex_predicate(self):
        """Test WHERE clause with complex predicates in consecutive MATCH"""
        query = """
        USE ldbc_snb
        MATCH (c:Comment) WHERE c.commentId > 1000 AND c.commentId < 2000
        MATCH (p:Person) WHERE p.personId > 100
        RETURN c.commentId, p.personId
        """
        response = execute_query(query, sql_only=True)
        assert response.status_code == 200, f"Query failed: {response.text}"
        data = response.json()
        assert "generated_sql" in data

    def test_consecutive_match_with_optional_match(self):
        """Test consecutive MATCH + WHERE followed by OPTIONAL MATCH"""
        query = """
        USE ldbc_snb
        MATCH (c:Comment) WHERE c.commentId = 100
        MATCH (p:Person) WHERE p.personId = 1
        OPTIONAL MATCH (p)-[:KNOWS]->(friend:Person)
        RETURN c.commentId, p.personId, friend.personId
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
        USE ldbc_snb
        MATCH (c:Comment)
        MATCH (p:Person)
        WHERE c.commentId = 1
        RETURN c.commentId, p.personId
        """
        response = execute_query(query, sql_only=True)
        assert response.status_code == 200, f"Query failed: {response.text}"

    def test_no_where_clause(self):
        """Test consecutive MATCH without any WHERE"""
        query = """
        USE ldbc_snb
        MATCH (c:Comment)
        MATCH (p:Person)
        RETURN c.commentId, p.personId
        """
        response = execute_query(query, sql_only=True)
        assert response.status_code == 200, f"Query failed: {response.text}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
