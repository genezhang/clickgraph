#!/usr/bin/env python
"""
Test multi-clause JOIN ordering
"""
import requests
import json

CLICKGRAPH_URL = "http://localhost:8080"

def test_query(cypher_query, schema_name="test_graph_schema"):
    """Execute a Cypher query and print the generated SQL."""
    print(f"\n{'='*80}")
    print(f"QUERY: {cypher_query}")
    print(f"{'='*80}")
    
    response = requests.post(
        f"{CLICKGRAPH_URL}/query",
        json={"query": cypher_query, "schema_name": schema_name},
        timeout=10
    )
    
    print(f"Status: {response.status_code}")
    print(f"Response text: {response.text}")
    
    if response.status_code == 500 or response.status_code == 400:
        try:
            result = response.json()
            print(f"\n❌ ERROR:")
            print(json.dumps(result, indent=2))
        except:
            print(f"\n❌ ERROR (raw): {response.text}")
        
        # Try to extract SQL from error message
        if "exception" in result:
            error_msg = result["exception"]
            if "SELECT" in error_msg:
                print(f"\n📝 GENERATED SQL (from error):")
                # Extract SQL between quotes or after "in scope"
                if "in scope" in error_msg:
                    sql = error_msg.split("in scope")[1].split(".")[0].strip()
                    print(sql)
    else:
        result = response.json()
        print(f"\n✅ SUCCESS:")
        print(json.dumps(result, indent=2))
    
    return response

# Test 1: Basic multi-clause (MATCH then OPTIONAL MATCH)
test_query("""
    MATCH (a:User)
    WHERE a.name = 'Alice'
    OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
    RETURN a.name, b.name
""")

# Test 2: Interleaved MATCH and OPTIONAL MATCH
test_query("""
    MATCH (a:User)
    WHERE a.name = 'Alice'
    OPTIONAL MATCH (a)-[:FOLLOWS]->(b:User)
    MATCH (x:User)
    WHERE x.name = 'Bob'
    RETURN a.name, b.name, x.name
""")

print(f"\n{'='*80}")
print("DONE")
print(f"{'='*80}")
