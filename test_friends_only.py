import requests

# Test just FRIENDS_WITH to see if it works
query = {'query': 'MATCH (a:User)-[:FRIENDS_WITH]->(b:User) RETURN a.name, b.name'}
r = requests.post('http://localhost:8080/query', json=query, timeout=10)
print('FRIENDS_WITH query:')
print(f'Status: {r.status_code}')
if r.status_code == 200:
    result = r.json()
    print(f'Results count: {len(result) if isinstance(result, list) else "not a list"}')
    if isinstance(result, list) and len(result) > 0:
        for item in result[:3]:  # Show first 3
            print(f'  {item}')
else:
    print(f'Error: {r.text}')