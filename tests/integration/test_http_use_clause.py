import requests
import os
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")

# Test USE clause via HTTP API
response = requests.post(
    f"{CLICKGRAPH_URL}/query",
    json={"query": "USE ecommerce_demo MATCH (c:Customer) RETURN c.first_name AS name LIMIT 1"}
)

print(f"HTTP Status: {response.status_code}")
if response.status_code == 200:
    result = response.json()
    print(f"Success! Result: {result}")
else:
    print(f"Error: {response.text}")
