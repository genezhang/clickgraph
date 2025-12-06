import requests
import os
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")

query = {'query': 'RETURN CASE WHEN 1=1 THEN "true" ELSE "false" END'}
response = requests.post(f'{CLICKGRAPH_URL}/query', json=query)
print('Status:', response.status_code)
print('Response:', response.text)