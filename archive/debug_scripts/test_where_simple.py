import requests
import json

# Test with debug logging to see what's in plan_ctx
response = requests.post(
    'http://localhost:8080/query',
    json={
        'schema_name': 'test_graph_schema',
        'query': 'MATCH (u:User) WHERE u.name = "Alice" RETURN u.name'
    }
)

print(f"Status: {response.status_code}")
if response.status_code == 200:
    result = response.json()
    row_count = len(result)
    print(f"Returned {row_count} rows")
    print(json.dumps(result, indent=2))
    
    if row_count == 1:
        print("\n✓ WHERE clause WORKS!")
    else:
        print(f"\n✗ WHERE clause BROKEN (expected 1 row, got {row_count})")
else:
    print(f"Error: {response.text}")
