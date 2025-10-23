import requests
import json
import time

def test_multiple_relationship_types():
    """Test multiple relationship types [:FOLLOWS|FRIENDS_WITH]"""

    # Test query using multiple relationship types
    query = {
        "query": "MATCH (a:User)-[:FOLLOWS|FRIENDS_WITH]->(b:User) RETURN a.name, b.name"
    }

    print("Testing multiple relationship types query:")
    print(f"Query: {query['query']}")
    print()

    try:
        response = requests.post('http://localhost:8080/query', json=query, timeout=10)
        print(f"Status Code: {response.status_code}")

        if response.status_code == 200:
            result = response.json()
            print("✅ Query executed successfully!")
            print("\nResults:")
            print(json.dumps(result, indent=2))

            # Verify we got expected results
            if isinstance(result, list) and len(result) > 0:
                print(f"\n✅ Found {len(result)} relationships")
                # Expected relationships: 8 FOLLOWS + 2 FRIENDS_WITH = 10 total
                expected_count = 10
                if len(result) == expected_count:
                    print(f"✅ Correct number of relationships found: {expected_count}")
                else:
                    print(f"❌ Expected {expected_count} relationships, got {len(result)}")
            else:
                print("❌ No data returned")
        else:
            print("❌ Query failed")
            print(f"Response: {response.text}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")
        print("Make sure ClickGraph server is running on localhost:8080")

def test_single_relationship_type():
    """Test single relationship type for comparison"""

    query = {
        "query": "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.name, b.name"
    }

    print("\n" + "="*50)
    print("Testing single relationship type (FOLLOWS only):")
    print(f"Query: {query['query']}")
    print()

    try:
        response = requests.post('http://localhost:8080/query', json=query, timeout=10)
        print(f"Status Code: {response.status_code}")

        if response.status_code == 200:
            result = response.json()
            print("✅ Query executed successfully!")
            print("\nResults:")
            print(json.dumps(result, indent=2))

            # Expected relationships: All 8 FOLLOWS relationships
            expected_count = 8
            if isinstance(result, list) and len(result) == expected_count:
                print(f"✅ Correct number of relationships found: {expected_count}")
            else:
                actual_count = len(result) if isinstance(result, list) else 0
                print(f"❌ Expected {expected_count} relationships, got {actual_count}")
        else:
            print("❌ Query failed")
            print(f"Response: {response.text}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")

if __name__ == "__main__":
    print("🧪 Testing Multiple Relationship Types Feature")
    print("="*50)

    # Test multiple relationship types
    test_multiple_relationship_types()

    # Test single relationship type for comparison
    test_single_relationship_type()

    print("\n" + "="*50)
    print("Test completed!")