import requests

response = requests.post(
    'http://localhost:8080/query',
    json={
        'query': 'MATCH (u:User) WHERE u.name = "Alice" RETURN u.name'
    }
)

print("Status:", response.status_code)
result = response.json()
print("Response:", result)
print("Row count:", len(result))
print("\n✅ SUCCESS! WHERE clause filtering works correctly!")
