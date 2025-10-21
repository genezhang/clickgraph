import requests

query = "MATCH (a:User)-[:FOLLOWS*1..2]->(b:User) WHERE a.name = 'Alice Johnson' RETURN b"

response = requests.post(
    "http://localhost:8080/query",
    json={"query": query, "sql_only": True},
    timeout=10
)

import json
data = response.json()
sql = data.get("generated_sql", "")

print("Query:", query)
print("\n" + "="*80)
print("Generated SQL:")
print("="*80)
print(sql)
print("="*80)

# Check for our debug prints
if "WHERE" in sql and "Alice Johnson" in sql:
    print("\n[SUCCESS] Filter is present in SQL!")
else:
    print("\n[ISSUE] Filter is NOT in SQL")
    print("Looking for: 'WHERE' and 'Alice Johnson'")
