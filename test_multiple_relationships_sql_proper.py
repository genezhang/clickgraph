#!/usr/bin/env python3
"""
Test script to verify SQL generation for multiple relationship types (>2).
This tests the UNION logic for queries like:
MATCH (c:Customer)-[:PURCHASED|PLACED_ORDER|ORDER_CONTAINS|REVIEWED]->(target) RETURN c, target
"""

import requests
import json
import sys

def test_three_relationships():
    """Test SQL generation for exactly 3 relationship types"""

    query = """
    MATCH (c:Customer)-[:PURCHASED|PLACED_ORDER|ORDER_CONTAINS]->(p:Product)
    RETURN c.customer_id, p.product_id
    """

    payload = {
        "query": query.strip(),
        "view": "ecommerce_graph",
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
            sql = str(result)
        union_count = sql.count("UNION ALL")

        print(f"🔍 Analysis for 3 relationships:")
        print(f"   - UNION ALL occurrences: {union_count}")
        print(f"   - Expected: 2 UNION ALL clauses (for 3 relationships)")

        if union_count == 2:
            print("✅ SUCCESS: Correct number of UNION ALL clauses for 3 relationship types")
            return True
        else:
            print(f"❌ FAILURE: Expected 2 UNION ALL clauses, got {union_count}")
            return False

    except requests.exceptions.ConnectionError:
        print("❌ Connection Error: Is ClickGraph server running on localhost:8080?")
        return False
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")
        return False

def test_four_relationships():
    """Test SQL generation for exactly 4 relationship types"""

    query = """
    MATCH (c:Customer)-[:PURCHASED|PLACED_ORDER|ORDER_CONTAINS|REVIEWED]->(p:Product)
    RETURN c.customer_id, p.product_id
    """

    payload = {
        "query": query.strip(),
        "view": "ecommerce_graph",
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
            sql = str(result)
        union_count = sql.count("UNION ALL")

        print(f"📄 Generated SQL:")
        print(sql)
        print()

        print(f"🔍 Analysis:")
        print(f"   - UNION ALL occurrences: {union_count}")
        print(f"   - Expected for 4 relationship types: 3 UNION ALL clauses")

        if union_count == 3:
            print("✅ SUCCESS: Correct number of UNION ALL clauses for 4 relationship types")
            return True
        else:
            print(f"❌ FAILURE: Expected 3 UNION ALL clauses, got {union_count}")
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

    print("\n1️⃣ Testing 3 relationship types (PURCHASED|PLACED_ORDER|ORDER_CONTAINS):")
    test1_result = test_three_relationships()

    print("\n2️⃣ Testing 4 relationship types (PURCHASED|PLACED_ORDER|ORDER_CONTAINS|REVIEWED):")
    test2_result = test_four_relationships()

    print("\n" + "=" * 60)
    if test1_result and test2_result:
        print("🎉 ALL TESTS PASSED: Multiple relationship types work correctly!")
        sys.exit(0)
    else:
        print("💥 SOME TESTS FAILED: Check the output above")
        sys.exit(1)