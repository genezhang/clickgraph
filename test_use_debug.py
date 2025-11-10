import requests

query = {
    "query": "USE social_network MATCH (u:User) RETURN count(u) as user_count",
    "schema_name": "ecommerce"
}

response = requests.post("http://localhost:8080/query", json=query)
print(f"Status: {response.status_code}")
print(f"Headers: {response.headers}")
print(f"Text: '{response.text}'")
print(f"Content: {response.content}")
