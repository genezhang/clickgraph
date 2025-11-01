import requests

# Test WHERE clause with property mapping
print('Test 3 - WHERE clause with property:')
response = requests.post('http://localhost:8080/query',
                        json={'query': 'MATCH (u:User) WHERE u.country = "UK" RETURN u.name, u.city LIMIT 1'})
print(response.text)

# Test COUNT query
print('\nTest 4 - COUNT query:')
response = requests.post('http://localhost:8080/query',
                        json={'query': 'MATCH (u:User) RETURN count(u) as user_count'})
print(response.text)