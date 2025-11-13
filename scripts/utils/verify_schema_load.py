import requests

# Load schema
print("Loading schema...")
response = requests.post(
    "http://localhost:8080/schemas/load",
    json={
        "schema_name": "ecommerce_demo",
        "config_content": open("ecommerce_simple.yaml").read()
    }
)
print(f"Load response: {response.status_code} - {response.json()}")

# List schemas to verify
print("\nListing schemas...")
list_response = requests.get("http://localhost:8080/schemas")
print(f"List response: {list_response.status_code}")
if list_response.status_code == 200:
    schemas = list_response.json().get("schemas", [])
    print(f"Available schemas: {schemas}")
else:
    print(f"Error: {list_response.text}")
