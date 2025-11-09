import requests
import json

BASE_URL = "http://localhost:8080"

tests = [
    {
        "name": "Basic MATCH (a)-[]->(b)",
        "query": "MATCH (a:User)-[:FOLLOWS]->(b:User) RETURN a.name, b.name ORDER BY a.name, b.name"
    },
    {
        "name": "MATCH with WHERE + OPTIONAL MATCH",
        "query": "MATCH (a:User) WHERE a.name = 'Alice' OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User) RETURN a.name, b.name"
    },
    {
        "name": "Multi-hop (a)-[]->(b)-[]->(c)",
        "query": "MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User) RETURN a.name, b.name, c.name"
    }
]

print("="*80)
print("ANCHOR SELECTION FIX VALIDATION")
print("="*80)

passed = 0
failed = 0

for test in tests:
    print(f"\nTest: {test['name']}")
    print(f"Query: {test['query']}")
    
    try:
        response = requests.post(
            f"{BASE_URL}/query",
            json={"query": test["query"]},
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"PASS - Rows returned: {len(result.get('results', []))}")
            passed += 1
        else:
            print(f"FAIL - Error: {response.json().get('error', 'Unknown error')}")
            failed += 1
            
    except Exception as e:
        print(f"FAIL - Exception: {str(e)}")
        failed += 1
    
    print("-"*80)

print(f"\nSUMMARY: {passed} passed, {failed} failed out of {len(tests)} tests")
