import requests
import os
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")

query = {
    'query': 'MATCH (u:User) RETURN CASE u.age WHEN 25 THEN "young" ELSE "other" END LIMIT 1'
}

try:
    response = requests.post(f'{CLICKGRAPH_URL}/query', json=query)
    print('Status:', response.status_code)
    print('Response:', response.text)
except Exception as e:
    print('Error:', e)