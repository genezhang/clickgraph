import requests
import json

# Test ViewScan query
query = {
    "query": "MATCH (u:User) RETURN u.name LIMIT 3"
}

try:
    response = requests.post('http://localhost:8080/query', json=query)
    print("Status Code:", response.status_code)
    print("\nResponse:")
    print(json.dumps(response.json(), indent=2))
except Exception as e:
    print(f"Error: {e}")
