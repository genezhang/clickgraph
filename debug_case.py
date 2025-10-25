import requests

query = {
    'query': 'MATCH (u:User) RETURN CASE u.age WHEN 25 THEN "young" ELSE "other" END LIMIT 1'
}

try:
    response = requests.post('http://localhost:8080/query', json=query)
    print('Status:', response.status_code)
    print('Response:', response.text)
except Exception as e:
    print('Error:', e)