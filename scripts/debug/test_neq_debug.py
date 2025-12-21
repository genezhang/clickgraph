#!/usr/bin/env python3
"""
Debug script to compare != vs <> operator SQL generation.
Runs both queries and captures server output to identify where operands are lost.
"""

import requests
import json

def test_query(query):
    """Send query to ClickGraph server and return response"""
    url = "http://localhost:8080/query"
    headers = {"Content-Type": "application/json"}
    data = {"query": query}
    
    try:
        response = requests.post(url, headers=headers, json=data)
        return response.status_code, response.text
    except Exception as e:
        return None, str(e)

def main():
    print("=" * 80)
    print("Testing != vs <> operators")
    print("=" * 80)
    
    queries = [
        ("!=", "MATCH (u:User) WHERE u.user_id != 1 RETURN u.name"),
        ("<>", "MATCH (u:User) WHERE u.user_id <> 1 RETURN u.name"),
    ]
    
    for op, query in queries:
        print(f"\n[{op}] Query: {query}")
        status, response = test_query(query)
        print(f"Status: {status}")
        print(f"Response: {response[:200]}...")
        
        # Try to parse as JSON
        try:
            result = json.loads(response)
            if "results" in result:
                print(f"✓ Success: {len(result['results'])} rows")
            elif "error" in result:
                print(f"✗ Error: {result['error']}")
        except:
            if "exception" in response:
                print(f"✗ ClickHouse Error (check SQL generation)")

if __name__ == "__main__":
    main()
