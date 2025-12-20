import requests
import os
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")

# First check if there are any users
query1 = {'query': 'MATCH (u:User) RETURN u.name LIMIT 5'}
response1 = requests.post(f'{CLICKGRAPH_URL}/query', json=query1)
print('Users query status:', response1.status_code)
print('Users:', response1.text)

# Try a simpler CASE expression
query2 = {'query': 'RETURN CASE 1 WHEN 1 THEN "one" ELSE "other" END'}
response2 = requests.post(f'{CLICKGRAPH_URL}/query', json=query2)
print('Simple CASE status:', response2.status_code)
print('Simple CASE result:', response2.text)