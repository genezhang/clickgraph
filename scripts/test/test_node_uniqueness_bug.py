"""
Test to verify node uniqueness bug in friends-of-friends queries.

According to OpenCypher spec and Neo4j behavior:
- Relationship uniqueness IS enforced (same rel can't appear twice)
- Node uniqueness within a pattern IS expected (start node shouldn't appear as end node)

Current Bug:
    MATCH (user:User)-[:FOLLOWS]-()-[:FOLLOWS]-(fof:User)
    WHERE user.user_id = 1
    RETURN DISTINCT fof.user_id
    
    Returns: [0, 1, 2, 3, ...]  <- user_id 1 appears!
    Expected: [0, 2, 3, ...]    <- user_id 1 should NOT appear
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from tests.integration.conftest import execute_cypher
import pytest


def test_node_uniqueness_friends_of_friends():
    """
    Test that the start node (user_id=1) does NOT appear in friends-of-friends results.
    This tests node uniqueness within a single MATCH pattern.
    """
    query = """
        MATCH (user:User)-[:FOLLOWS]-()-[:FOLLOWS]-(fof:User)
        WHERE user.user_id = 1
        RETURN DISTINCT fof.user_id
        ORDER BY fof.user_id
        LIMIT 20
    """
    
    print("\nQuery:")
    print(query)
    
    response = execute_cypher(query)
    
    if isinstance(response, dict) and "error" in response:
        pytest.fail(f"Query failed: {response['error']}")
    
    results = response if isinstance(response, list) else response.get("data", [])
    
    print(f"\nResults: {len(results)} rows")
    fof_ids = [row["fof.user_id"] for row in results]
    print(f"Friend-of-friend IDs: {fof_ids[:20]}")
    
    # THE BUG: user_id=1 should NOT appear in its own friends-of-friends
    if 1 in fof_ids:
        print(f"\n❌ BUG CONFIRMED: user_id=1 appears in its own friends-of-friends results!")
        print(f"   This violates Neo4j/OpenCypher node uniqueness semantics.")
        pytest.fail("Node uniqueness bug: start node appears as result node")
    else:
        print(f"\n✅ CORRECT: user_id=1 does not appear in results")


def test_node_uniqueness_with_named_intermediate():
    """
    Test node uniqueness with explicitly named intermediate node.
    Same pattern but with named intermediate: (user)-[:FOLLOWS]->(friend)-[:FOLLOWS]->(fof)
    """
    query = """
        MATCH (user:User)-[:FOLLOWS]->(friend)-[:FOLLOWS]->(fof:User)
        WHERE user.user_id = 1
        RETURN DISTINCT fof.user_id
        ORDER BY fof.user_id
        LIMIT 20
    """
    
    print("\nQuery with named intermediate node:")
    print(query)
    
    response = execute_cypher(query)
    
    if isinstance(response, dict) and "error" in response:
        pytest.fail(f"Query failed: {response['error']}")
    
    results = response if isinstance(response, list) else response.get("data", [])
    
    print(f"\nResults: {len(results)} rows")
    fof_ids = [row["fof.user_id"] for row in results]
    print(f"Friend-of-friend IDs: {fof_ids[:20]}")
    
    # THE BUG: user_id=1 should NOT appear
    if 1 in fof_ids:
        print(f"\n❌ BUG: user_id=1 appears in results!")
        pytest.fail("Node uniqueness bug: start node appears as result node")
    else:
        print(f"\n✅ CORRECT: user_id=1 does not appear in results")


def test_node_uniqueness_three_hops():
    """
    Test node uniqueness with 3-hop pattern.
    Start node should not appear anywhere in the path results.
    """
    query = """
        MATCH (user:User)-[:FOLLOWS]->()-[:FOLLOWS]->()-[:FOLLOWS]->(fof:User)
        WHERE user.user_id = 1
        RETURN DISTINCT fof.user_id
        ORDER BY fof.user_id
        LIMIT 20
    """
    
    print("\nQuery (3 hops):")
    print(query)
    
    response = execute_cypher(query)
    
    if isinstance(response, dict) and "error" in response:
        pytest.fail(f"Query failed: {response['error']}")
    
    results = response if isinstance(response, list) else response.get("data", [])
    
    print(f"\nResults: {len(results)} rows")
    fof_ids = [row["fof.user_id"] for row in results if "fof.user_id" in row]
    print(f"3-hop friend IDs: {fof_ids[:20]}")
    
    # user_id=1 should NOT appear
    if 1 in fof_ids:
        print(f"\n❌ BUG: user_id=1 appears in 3-hop results!")
        pytest.fail("Node uniqueness bug in 3-hop pattern")
    else:
        print(f"\n✅ CORRECT: user_id=1 does not appear in results")


def test_node_uniqueness_NOT_enforced_across_match_clauses():
    """
    Test that node uniqueness is NOT enforced ACROSS multiple MATCH clauses.
    This is CORRECT Neo4j behavior - uniqueness only within single MATCH.
    """
    query = """
        MATCH (user:User)-[:FOLLOWS]->(friend)
        MATCH (friend)-[:FOLLOWS]->(fof:User)
        WHERE user.user_id = 1
        RETURN DISTINCT fof.user_id
        ORDER BY fof.user_id
        LIMIT 20
    """
    
    print("\nQuery (two separate MATCH clauses):")
    print(query)
    
    response = execute_cypher(query)
    
    if isinstance(response, dict) and "error" in response:
        pytest.fail(f"Query failed: {response['error']}")
    
    results = response if isinstance(response, list) else response.get("data", [])
    
    print(f"\nResults: {len(results)} rows")
    fof_ids = [row["fof.user_id"] for row in results]
    print(f"Friend-of-friend IDs: {fof_ids[:20]}")
    
    # With TWO MATCH clauses, user_id=1 CAN appear (no uniqueness constraint)
    # This is CORRECT behavior per Neo4j semantics
    print(f"\nNote: user_id=1 appearing here is CORRECT (no cross-MATCH uniqueness)")
    if 1 in fof_ids:
        print(f"✅ user_id=1 appears - this is expected with separate MATCH clauses")
    else:
        print(f"⚠️  user_id=1 doesn't appear - unusual but not necessarily wrong")


if __name__ == "__main__":
    print("="*70)
    print("Node Uniqueness Bug Verification")
    print("="*70)
    
    try:
        print("\n" + "="*70)
        print("TEST 1: Friends-of-friends (anonymous intermediate)")
        print("="*70)
        test_node_uniqueness_friends_of_friends()
    except AssertionError as e:
        print(f"Test failed (expected): {e}")
    
    try:
        print("\n" + "="*70)
        print("TEST 2: Friends-of-friends (named intermediate)")
        print("="*70)
        test_node_uniqueness_with_named_intermediate()
    except AssertionError as e:
        print(f"Test failed (expected): {e}")
    
    try:
        print("\n" + "="*70)
        print("TEST 3: Three-hop pattern")
        print("="*70)
        test_node_uniqueness_three_hops()
    except AssertionError as e:
        print(f"Test failed (expected): {e}")
    
    try:
        print("\n" + "="*70)
        print("TEST 4: Multiple MATCH clauses (should allow node reuse)")
        print("="*70)
        test_node_uniqueness_NOT_enforced_across_match_clauses()
    except AssertionError as e:
        print(f"Test failed: {e}")
    
    print("\n" + "="*70)
    print("DONE")
    print("="*70)
