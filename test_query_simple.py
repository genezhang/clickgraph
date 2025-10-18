#!/usr/bin/env python3
"""Simple test script to send a query to ClickGraph server"""

import requests
import json

def test_query():
    url = "http://localhost:8080/query"
    payload = {"query": "MATCH (u:User) RETURN u.name LIMIT 3"}
    
    print("\n🎯 Sending query to ClickGraph...")
    print(f"Query: {payload['query']}\n")
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ SUCCESS!")
            print("\nResponse:")
            print(json.dumps(response.json(), indent=2))
        else:
            print(f"❌ ERROR: HTTP {response.status_code}")
            print(f"Response: {response.text}")
            
    except requests.exceptions.ConnectionError:
        print("❌ ERROR: Could not connect to server at localhost:8080")
        print("Make sure the server is running!")
    except Exception as e:
        print(f"❌ ERROR: {e}")

if __name__ == "__main__":
    test_query()
