import requests

r = requests.post('http://localhost:7475/query', json={
    'query': 'USE social_network MATCH (u:User) RETURN count(u) as user_count',
    'schema_name': 'ecommerce'
})

print(f'Status: {r.status_code}')
print(f'Content-Type: {r.headers.get("Content-Type")}')
print(f'Body: {r.text}')
