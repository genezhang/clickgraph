import requests

query = """
MATCH (u:User)
WHERE u.city = 'NYC'
OPTIONAL MATCH (u)-[f:FRIENDS_WITH]->(friend:User)
RETURN u.name, u.city, friend.name
LIMIT 5
"""

r = requests.post('http://localhost:7475/query', json={'query': query})
print(f"Status: {r.status_code}")
print(f"Response: {r.text}")
