import requests

# Load social_network schema content
with open('schemas/demo/social_network.yaml', 'r', encoding='utf-8') as f:
    schema_yaml = f.read()

response = requests.post('http://localhost:8080/schemas/load', json={
    'schema_name': 'social_network',
    'config_content': schema_yaml
})

print(f'Status: {response.status_code}')
if response.status_code == 200:
    print(f'Success: {response.json()}')
else:
    print(f'Error: {response.text}')
