#!/usr/bin/env python3
"""
Test OPTIONAL MATCH queries with ViewScan implementation.
Verifies that LEFT JOIN works correctly with view-based table names.
"""
import requests
import json

def test_query(query, description):
    """Execute a query and display results."""
    print(f"\n{'='*70}")
    print(f"[TEST] Test: {description}")
    print(f"{'='*70}")
    print(f"Query: {query}\n")
    
    try:
        response = requests.post(
            "http://localhost:8080/query",
            json={"query": query},
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"[OK] SUCCESS!\n")
            print(f"Response:")
            print(json.dumps(result, indent=2))
            return True
        else:
            print(f"[FAIL] ERROR: HTTP {response.status_code}\n")
            print(f"Response: {response.text}")
            return False
            
    except requests.exceptions.ConnectionError:
        print(f"[FAIL] FAILED: Could not connect to ClickGraph server at http://localhost:8080")
        print(f"   Make sure the server is running with: .\\start_server_new_window.bat")
        return False
    except Exception as e:
        print(f"[FAIL] FAILED: {type(e).__name__}: {str(e)}")
        return False

def main():
    """Run all OPTIONAL MATCH tests."""
    print("\n" + "="*70)
    print(" OPTIONAL MATCH Test Suite with ViewScan")
    print("="*70)
    
    tests = [
        {
            "query": "MATCH (u:User) RETURN u.name LIMIT 3",
            "description": "Baseline: Simple MATCH with ViewScan"
        },
        {
            "query": "OPTIONAL MATCH (u:User) RETURN u.name LIMIT 3",
            "description": "OPTIONAL MATCH on single node"
        },
        {
            "query": "MATCH (u:User) OPTIONAL MATCH (u)-[r:FRIENDS_WITH]->(f:User) RETURN u.name, f.name LIMIT 5",
            "description": "MATCH + OPTIONAL MATCH with relationship"
        },
        {
            "query": "OPTIONAL MATCH (u:User)-[r:FRIENDS_WITH]->(f:User) RETURN u.name, f.name LIMIT 5",
            "description": "Pure OPTIONAL MATCH with relationship"
        },
        {
            "query": "MATCH (u:User) WHERE u.name = 'Alice' OPTIONAL MATCH (u)-[r:FRIENDS_WITH]->(f:User) RETURN u.name, f.name",
            "description": "Specific user with OPTIONAL relationship"
        }
    ]
    
    results = []
    for test in tests:
        success = test_query(test["query"], test["description"])
        results.append({
            "description": test["description"],
            "success": success
        })
    
    # Summary
    print("\n" + "="*70)
    print(" Test Summary")
    print("="*70)
    
    passed = sum(1 for r in results if r["success"])
    total = len(results)
    
    for i, result in enumerate(results, 1):
        status = "[OK] PASS" if result["success"] else "[FAIL] FAIL"
        print(f"{i}. {status} - {result['description']}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All OPTIONAL MATCH tests passed!")
    else:
        print(f"\n⚠️  {total - passed} test(s) failed")

if __name__ == "__main__":
    main()
