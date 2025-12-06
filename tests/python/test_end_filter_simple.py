import requests
import os
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
import json

query = 'MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WHERE b.name = "David Lee" RETURN a.name, b.name'

response = requests.post(f'{CLICKGRAPH_URL}/query',
                        json={'query': query},
                        headers={'Content-Type': 'application/json'})

print('Status Code:', response.status_code)
print('Response Text:', response.text)