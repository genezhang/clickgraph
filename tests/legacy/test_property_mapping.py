import requests
import os
CLICKGRAPH_URL = os.getenv("CLICKGRAPH_URL", "http://localhost:8080")
import json

# Test reversed order
query = '''MATCH (b:User), (a:User)
WHERE a.name = "Alice" AND b.name = "Charlie"
RETURN a, b'''

response = requests.post(f'{CLICKGRAPH_URL}/query',
                        json={'query': query, 'sql_only': True})
print('Status:', response.status_code)
print('SQL:', response.json().get('generated_sql', 'No SQL generated'))