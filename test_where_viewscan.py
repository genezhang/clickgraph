import requests
import json

# Test WHERE clause with ViewScan (simple MATCH)
response = requests.post(
    'http://localhost:8081/query',
    json={
        'schema_name': 'test_graph_schema',
        'query': 'MATCH (u:User) WHERE u.name = "Alice" RETURN u.name'
    }
)

print(f"Status: {response.status_code}")
print(f"Response text: {response.text[:500]}")
if response.status_code == 200:
    result = response.json()
    print(json.dumps(result, indent=2))
    
    # Check if filter worked
    if 'data' in result:
        row_count = len(result['data'])
        print(f"\n✓ Returned {row_count} row(s)")
        if row_count == 1:
            print("✓ WHERE clause WORKS! (expected 1 row, got 1)")
        else:
            print(f"✗ WHERE clause BROKEN (expected 1 row, got {row_count})")
else:
    print(f"Error response: {response.text}")
