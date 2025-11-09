#!/usr/bin/env python3
"""
Test CASE expressions in ClickGraph
Tests both simple CASE (CASE x WHEN val THEN result) and searched CASE (CASE WHEN condition THEN result)
"""

import requests
import json
import sys
import os

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_case_expressions():
    """Test both simple and searched CASE expressions using existing User data"""

    # Simple CASE expression test - using existing User data
    simple_case_query = """
    MATCH (u:User)
    RETURN u.name,
           CASE u.name
               WHEN 'Alice Johnson' THEN 'Admin'
               WHEN 'Bob Smith' THEN 'Moderator'
               ELSE 'User'
           END as role
    LIMIT 5
    """

    # Searched CASE expression test
    searched_case_query = """
    MATCH (u:User)
    RETURN u.name,
           CASE
               WHEN u.name = 'Alice Johnson' THEN 'VIP'
               WHEN u.name = 'Bob Smith' THEN 'Premium'
               ELSE 'Standard'
           END as membership_level
    LIMIT 5
    """

    # Test CASE in WHERE clause
    case_in_where_query = """
    MATCH (u:User)
    WHERE CASE u.name
              WHEN 'Alice Johnson' THEN true
              WHEN 'Bob Smith' THEN true
              ELSE false
          END
    RETURN u.name
    LIMIT 3
    """

    # Test CASE inside a function
    case_in_function_query = """
    MATCH (u:User)
    RETURN u.name,
           length(CASE u.name
                     WHEN 'Alice Johnson' THEN 'Administrator'
                     WHEN 'Bob Smith' THEN 'Moderator'
                     ELSE 'Regular User'
                  END) as title_length
    LIMIT 3
    """

    # Test CASE in complex expression
    case_in_complex_query = """
    MATCH (u:User)
    RETURN u.name,
           CASE u.name
               WHEN 'Alice Johnson' THEN 'VIP'
               ELSE 'Standard'
           END || ' User' as user_type
    LIMIT 3
    """

    print("Testing CASE expressions in ClickGraph...")
    print("=" * 50)

    try:
        # Test simple CASE expression
        print("\nTesting simple CASE expression...")
        print(f"Query: {simple_case_query.strip()}")
        response = requests.post(
            "http://localhost:8080/query",
            json={"query": simple_case_query},
            headers={"Content-Type": "application/json"}
        )

        if response.status_code == 200:
            result = response.json()
            print("[OK] Simple CASE expression executed successfully")
            print(f"Results: {json.dumps(result, indent=2)}")
        else:
            print(f"✗ Simple CASE expression failed: {response.text}")
            return False

        # Test searched CASE expression
        print("\nTesting searched CASE expression...")
        print(f"Query: {searched_case_query.strip()}")
        response = requests.post(
            "http://localhost:8080/query",
            json={"query": searched_case_query},
            headers={"Content-Type": "application/json"}
        )

        if response.status_code == 200:
            result = response.json()
            print("[OK] Searched CASE expression executed successfully")
            print(f"Results: {json.dumps(result, indent=2)}")
        else:
            print(f"✗ Searched CASE expression failed: {response.text}")
            return False

        # Test CASE in WHERE clause
        print("\nTesting CASE expression in WHERE clause...")
        print(f"Query: {case_in_where_query.strip()}")
        response = requests.post(
            "http://localhost:8080/query",
            json={"query": case_in_where_query},
            headers={"Content-Type": "application/json"}
        )

        if response.status_code == 200:
            result = response.json()
            print("[OK] CASE in WHERE clause executed successfully")
            print(f"Results: {json.dumps(result, indent=2)}")
        else:
            print(f"✗ CASE in WHERE clause failed: {response.text}")
            return False

        # Test CASE inside a function
        print("\nTesting CASE expression inside a function...")
        print(f"Query: {case_in_function_query.strip()}")
        response = requests.post(
            "http://localhost:8080/query",
            json={"query": case_in_function_query},
            headers={"Content-Type": "application/json"}
        )

        if response.status_code == 200:
            result = response.json()
            print("[OK] CASE inside function executed successfully")
            print(f"Results: {json.dumps(result, indent=2)}")
        else:
            print(f"✗ CASE inside function failed: {response.text}")
            return False

        # Test CASE in complex expression
        print("\nTesting CASE expression in complex expression...")
        print(f"Query: {case_in_complex_query.strip()}")
        response = requests.post(
            "http://localhost:8080/query",
            json={"query": case_in_complex_query},
            headers={"Content-Type": "application/json"}
        )

        if response.status_code == 200:
            result = response.json()
            print("[OK] CASE in complex expression executed successfully")
            print(f"Results: {json.dumps(result, indent=2)}")
        else:
            print(f"✗ CASE in complex expression failed: {response.text}")
            return False

        print("\n" + "=" * 50)
        print("[OK] All CASE expression tests passed!")
        return True

    except requests.exceptions.ConnectionError:
        print("✗ Could not connect to ClickGraph server. Is it running?")
        print("Start the server with: .\start_server_background.ps1")
        return False
    except Exception as e:
        print(f"✗ Test failed with error: {e}")
        return False

if __name__ == "__main__":
    success = test_case_expressions()
    sys.exit(0 if success else 1)