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
        response = requests.post('http://localhost:8081/query', json=query, timeout=10)
        print(f"Status Code: {response.status_code}")

        if response.status_code == 200:
            result = response.json()
            print("✅ Query executed successfully!")
            print("\nResults:")
            print(json.dumps(result, indent=2))

            # Verify we got expected results
            if 'data' in result and len(result['data']) > 0:
                print(f"\n✅ Found {len(result['data'])} relationships")
                # Expected relationships:
                # Alice -> Bob (FOLLOWS)
                # Alice -> Charlie (FOLLOWS)
                # Alice -> Bob (FRIENDS_WITH)
                # Bob -> Charlie (FRIENDS_WITH)
                expected_count = 4
                if len(result['data']) == expected_count:
                    print(f"✅ Correct number of relationships found: {expected_count}")
                else:
                    print(f"❌ Expected {expected_count} relationships, got {len(result['data'])}")
            else:
                print("❌ No data returned")
        else:
            print("❌ Query failed")
            print(f"Response: {response.text}")

    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")
        print("Make sure ClickGraph server is running on localhost:8081")

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
        response = requests.post('http://localhost:8081/query', json=query, timeout=10)
        print(f"Status Code: {response.status_code}")

        if response.status_code == 200:
            result = response.json()
            print("✅ Query executed successfully!")
            print("\nResults:")
            print(json.dumps(result, indent=2))

            # Expected relationships: Alice -> Bob, Alice -> Charlie
            expected_count = 2
            if 'data' in result and len(result['data']) == expected_count:
                print(f"✅ Correct number of relationships found: {expected_count}")
            else:
                actual_count = len(result['data']) if 'data' in result else 0
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