"""
End-to-end test for node uniqueness bug fix (v0.5.2)

Tests that MATCH clauses properly exclude duplicate nodes within the same pattern.
This ensures that friends-of-friends queries don't return the starting user.
"""

import pytest
import requests
import os

BASE_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")


def test_friends_of_friends_excludes_start_node():
    """
    Test that user_id=1 is NOT in the results when querying friends-of-friends.
    
    This tests the core bug fix: node uniqueness within a single MATCH clause.
    """
    query = {
        "query": """
            MATCH (user:User)-[:FOLLOWS]->(mid:User)-[:FOLLOWS]->(fof:User)
            WHERE user.user_id = 1
            RETURN DISTINCT fof.user_id
            ORDER BY fof.user_id
        """
    }
    
    response = requests.post(f"{BASE_URL}/query", json=query)
    assert response.status_code == 200
    
    result = response.json()
    fof_ids = [row["fof.user_id"] for row in result["data"]]
    
    # user_id=1 should NOT appear in the results
    assert 1 not in fof_ids, "Starting user (user_id=1) should not appear in friends-of-friends results"
    
    # We should have some results (assuming user_id=1 has connections)
    assert len(fof_ids) > 0, "Should have some friends-of-friends"
    
    print(f"✅ Friends-of-friends correctly excludes start node")
    print(f"   Found {len(fof_ids)} unique friends-of-friends")


def test_sql_contains_uniqueness_constraints():
    """
    Test that the generated SQL contains uniqueness constraints.
    """
    query = {
        "query": """
            MATCH (user:User)-[:FOLLOWS]->(mid:User)-[:FOLLOWS]->(fof:User)
            WHERE user.user_id = 1
            RETURN fof.name
        """
    }
    
    response = requests.post(f"{BASE_URL}/query/sql", json=query)
    assert response.status_code == 200
    
    sql = response.json()["sql"]
    
    # SQL should contain uniqueness constraints (in either order)
    has_user_mid = ("user.user_id <> mid.user_id" in sql) or ("mid.user_id <> user.user_id" in sql)
    has_mid_fof = ("mid.user_id <> fof.user_id" in sql) or ("fof.user_id <> mid.user_id" in sql)
    has_user_fof = ("user.user_id <> fof.user_id" in sql) or ("fof.user_id <> user.user_id" in sql)
    
    assert has_user_mid, "SQL should contain user <> mid constraint"
    assert has_mid_fof, "SQL should contain mid <> fof constraint"
    assert has_user_fof, "SQL should contain user <> fof constraint"
    
    print("✅ Generated SQL contains all uniqueness constraints")


def test_three_hop_uniqueness():
    """
    Test uniqueness in longer paths (3 hops).
    """
    query = {
        "query": """
            MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User)-[:FOLLOWS]->(d:User)
            WHERE a.user_id = 1
            RETURN DISTINCT a.user_id, b.user_id, c.user_id, d.user_id
        """
    }
    
    response = requests.post(f"{BASE_URL}/query", json=query)
    assert response.status_code == 200
    
    result = response.json()
    
    # Check each result row for uniqueness
    for row in result["data"]:
        ids = [row["a.user_id"], row["b.user_id"], row["c.user_id"], row["d.user_id"]]
        unique_ids = set(ids)
        
        assert len(unique_ids) == 4, f"All node IDs should be unique in path: {ids}"
    
    print(f"✅ Three-hop paths maintain node uniqueness")
    print(f"   Checked {len(result['data'])} paths")


def test_anonymous_nodes_uniqueness():
    """
    Test that anonymous nodes also maintain uniqueness.
    """
    query = {
        "query": """
            MATCH (user:User)-[:FOLLOWS]->(:User)-[:FOLLOWS]->(fof:User)
            WHERE user.user_id = 1
            RETURN DISTINCT fof.user_id
            ORDER BY fof.user_id
        """
    }
    
    response = requests.post(f"{BASE_URL}/query", json=query)
    assert response.status_code == 200
    
    result = response.json()
    fof_ids = [row["fof.user_id"] for row in result["data"]]
    
    # user_id=1 should NOT appear even with anonymous middle node
    assert 1 not in fof_ids, "Starting user should not appear in results (anonymous middle node)"
    
    print(f"✅ Anonymous nodes maintain uniqueness")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
