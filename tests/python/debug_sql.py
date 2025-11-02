import requests
import sys

if len(sys.argv) > 1:
    query_text = sys.argv[1]
else:
    query_text = "MATCH (a:User)-[:FOLLOWS|FRIENDS_WITH]->(b:User) RETURN a.name, b.name"

query = {
    "query": query_text,
    "sql_only": True
}

r = requests.post('http://localhost:8080/query', json=query, timeout=10)
if r.status_code == 200:
    result = r.json()
    print('Generated SQL:')
    print(result.get('sql', result))
else:
    print(f'Error: {r.status_code}')
    print(r.text)
