import requests
import json

query = 'MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WHERE b.name = "David Lee" RETURN a.name, b.name'

response = requests.post('http://localhost:8080/query',
                        json={'query': query},
                        headers={'Content-Type': 'application/json'})

print('Status Code:', response.status_code)
print('Response Text:', response.text)