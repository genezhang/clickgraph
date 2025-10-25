import requests

query = {'query': 'RETURN CASE WHEN 1=1 THEN "true" ELSE "false" END'}
response = requests.post('http://localhost:8080/query', json=query)
print('Status:', response.status_code)
print('Response:', response.text)