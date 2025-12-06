import requests
import os
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")

# Test parameter + function
query = "RETURN toUpper($name) AS upper_name"
params = {"name": "alice"}

response = requests.post(
    f"{CLICKGRAPH_URL}/query",
    json={
        "query": query,
        "parameters": params,
        "schema_name": "social_network_demo"
    }
)

print(f"Status: {response.status_code}")
print(f"Response text: {response.text}")

if response.status_code == 200:
    print(f"JSON: {response.json()}")
