"""
Test basic property expression functionality
Tests simple column and expression parsing with SQL generation
"""
import requests
import json

BASE_URL = "http://localhost:8080"

def test_simple_column():
    """Test that simple column still works"""
    query = '''MATCH (u:User) WHERE u.user_id = 1 RETURN u.name'''
    
    response = requests.post(f'{BASE_URL}/query',
                           json={'query': query, 'sql_only': True})
    
    print("=" * 80)
    print("TEST: Simple Column Access (u.name → full_name)")
    print("=" * 80)
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        result = response.json()
        sql = result.get('generated_sql', '')
        print(f"SQL:\n{sql}\n")
        
        # Check that it maps to full_name column
        if 'full_name' in sql:
            print("✅ PASS: Property mapped to correct column")
        else:
            print(f"❌ FAIL: Expected 'full_name' in SQL")
    else:
        print(f"❌ FAIL: {response.text}")
    
def test_expression():
    """Test that expression gets applied correctly"""
    # Using concat expression from schema
    query = '''MATCH (u:User) WHERE u.user_id = 1 RETURN u.full_display_name'''
    
    response = requests.post(f'{BASE_URL}/query',
                           json={'query': query, 'sql_only': True})
    
    print("=" * 80)
    print("TEST: Expression (full_display_name → concat(...))")
    print("=" * 80)
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        result = response.json()
        sql = result.get('generated_sql', '')
        print(f"SQL:\n{sql}\n")
        
        # Check that concat function is used
        if 'concat(' in sql.lower():
            print("✅ PASS: Expression generated concat() function")
        else:
            print(f"❌ FAIL: Expected 'concat(' in SQL")
    else:
        print(f"❌ FAIL: {response.text}")

if __name__ == '__main__':
    print("\n" + "="*80)
    print("PROPERTY EXPRESSION BASIC TESTS")
    print("="*80 + "\n")
    
    test_simple_column()
    print()
    test_expression()
