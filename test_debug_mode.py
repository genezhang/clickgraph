import requests
import json

# Test with debug mode
query = """
MATCH (u1:User)-[:FOLLOWS|FRIENDS_WITH]->(u2:User)
RETURN u1, u2
"""

payload = {
    "query": query.strip(),
    "view": "social_graph",
    "debug": True
}

response = requests.post("http://localhost:8080/query", json=payload)
print(f"Status: {response.status_code}\n")

result = response.json()
print(json.dumps(result, indent=2))
