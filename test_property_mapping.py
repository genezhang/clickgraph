import requests
import json

# Test reversed order
query = '''MATCH (b:User), (a:User)
WHERE a.name = "Alice" AND b.name = "Charlie"
RETURN a, b'''

response = requests.post('http://localhost:8080/query',
                        json={'query': query, 'sql_only': True})
print('Status:', response.status_code)
print('SQL:', response.json().get('generated_sql', 'No SQL generated'))