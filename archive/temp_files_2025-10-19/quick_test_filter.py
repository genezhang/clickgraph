import requests
import json

SERVER_URL = "http://localhost:8080/query"

query = "MATCH (a:Person)-[*1..2]->(b:Person) WHERE a.full_name = 'Alice' RETURN b"

response = requests.post(
    SERVER_URL,
    json={"query": query, "sql_only": True},
    headers={"Content-Type": "application/json"},
    timeout=10
)

print(f"Status: {response.status_code}")
print(f"Response:")
print(json.dumps(response.json(), indent=2))
