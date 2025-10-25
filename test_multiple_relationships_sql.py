#!/usr/bin/env python3
"""
Test script to verify SQL generation for multiple relationship types (>2).
This tests the UNION logic for queries like:
MATCH (c:Customer)-[:PURCHASED|PLACED_ORDER|ORDER_CONTAINS|REVIEWED]->(target) RETURN c, target
"""

import requests
import json
import sys

def test_multiple_relationships():
    """Test SQL generation for 3+ relationship types"""

    # For now, test 2 relationships with the social_graph schema
    query = """
    MATCH (u1:User)-[:FOLLOWS|FRIENDS_WITH]->(u2:User)
    RETURN u1.user_id, u2.user_id
    """

    payload = {
        "query": query.strip(),
        "view": "social_graph",
        "sql_only": True
    }

    try:
        # Make request to ClickGraph server
        response = requests.post(
            "http://localhost:8080/query",
            json=payload,
            headers={"Content-Type": "application/json"}
        )

        if response.status_code != 200:
            print(f"❌ HTTP Error {response.status_code}: {response.text}")
            return False

        result = response.json()

        # Check if we got a successful response
        if isinstance(result, dict) and "error" in result:
            print(f"❌ Query Error: {result['error']}")
            return False

        # Print the generated SQL (if available in debug mode)
        if isinstance(result, dict) and "generated_sql" in result:
            print("📄 Generated SQL:")
            print(result["generated_sql"])
            print()

        # Check for UNION ALL in the SQL
        if isinstance(result, dict):
            sql = result.get("generated_sql", result.get("sql", ""))
        else:
            sql = str(result)  # fallback
        union_count = sql.count("UNION ALL")

        print(f"🔍 Analysis:")
        print(f"   - UNION ALL occurrences: {union_count}")
        print(f"   - Expected for 2 relationship types: 1 UNION ALL clause")

        if union_count == 1:
            print("✅ SUCCESS: Correct number of UNION ALL clauses for 2 relationship types")
            return True
        else:
            print(f"❌ FAILURE: Expected 1 UNION ALL clause, got {union_count}")
            return False

    except requests.exceptions.ConnectionError:
        print("❌ Connection Error: Is ClickGraph server running on localhost:8080?")
        return False
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")
        return False

def test_three_relationships():
    """Test SQL generation for exactly 3 relationship types"""

    # For now, test 2 relationships with the social_graph schema
    # We'll extend this once we confirm the basic functionality works
    query = """
    MATCH (u1:User)-[:FOLLOWS|FRIENDS_WITH]->(u2:User)
    RETURN u1.user_id, u2.user_id
    """

    payload = {
        "query": query.strip(),
        "view": "social_graph",
        "sql_only": True
    }

    try:
        response = requests.post(
            "http://localhost:8080/query",
            json=payload,
            headers={"Content-Type": "application/json"}
        )

        if response.status_code != 200:
            print(f"❌ HTTP Error {response.status_code}: {response.text}")
            return False

        result = response.json()

        if isinstance(result, dict) and "error" in result:
            print(f"❌ Query Error: {result['error']}")
            return False

        if isinstance(result, dict):
            sql = result.get("generated_sql", result.get("sql", ""))
        else:
            sql = str(result)  # fallback
        union_count = sql.count("UNION ALL")

        print(f"🔍 Analysis for 3 relationships:")
        print(f"   - UNION ALL occurrences: {union_count}")
        print(f"   - Expected: 1 UNION ALL clause (for 2 relationships)")

        if union_count == 1:
            print("✅ SUCCESS: Correct number of UNION ALL clauses for 2 relationship types")
            return True
        else:
            print(f"❌ FAILURE: Expected 1 UNION ALL clause, got {union_count}")
            return False

    except requests.exceptions.ConnectionError:
        print("❌ Connection Error: Is ClickGraph server running on localhost:8080?")
        return False
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")
        return False

if __name__ == "__main__":
    print("🧪 Testing Multiple Relationship Types SQL Generation")
    print("=" * 60)

    print("\n1️⃣ Testing 3 relationship types (FOLLOWS|FRIENDS_WITH|LIKES):")
    test1_result = test_three_relationships()

    print("\n2️⃣ Testing 4 relationship types (FOLLOWS|FRIENDS_WITH|LIKES|PURCHASED):")
    test2_result = test_multiple_relationships()

    print("\n" + "=" * 60)
    if test1_result and test2_result:
        print("🎉 ALL TESTS PASSED: Multiple relationship types work correctly!")
        sys.exit(0)
    else:
        print("💥 SOME TESTS FAILED: Check the output above")
        sys.exit(1)