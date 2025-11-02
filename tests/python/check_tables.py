import requests

query = "SELECT database, name, total_rows FROM system.tables WHERE name LIKE '%bench%'"
response = requests.post('http://localhost:8123', data=query, auth=('test_user', 'test_pass'))
print("Benchmark tables:")
print(response.text)