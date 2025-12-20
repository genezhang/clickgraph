import requests
import os
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
import json

# Test path variables query
query = """
MATCH p = (u1:User)-[:FOLLOWS*1..3]->(u2:User)
RETURN p, length(p) AS path_length, nodes(p) AS path_nodes, relationships(p) AS path_relationships
LIMIT 5
"""

response = requests.post(
    f"{CLICKGRAPH_URL}/query",
    json={"query": query},
    headers={"Content-Type": "application/json"}
)

print("Status Code:", response.status_code)
print("\nResponse:")
print(json.dumps(response.json(), indent=2))

if response.status_code == 500:
    error_msg = response.json().get("error", "")
    if "SQL" in error_msg or "SELECT" in error_msg:
        print("\nGenerated SQL visible in error:")
        # Extract SQL if present
        start = error_msg.find("SELECT")
        if start != -1:
            sql = error_msg[start:start+500]
            print(sql)
