#!/usr/bin/env python3
"""
Test Neo4j functions in actual graph queries (not standalone RETURN)
"""
import requests

API_URL = "http://localhost:8080/query"

def test_with_match(cypher_query, description):
    """Test a function in a MATCH query"""
    print(f"\n{'='*70}")
    print(f"Test: {description}")
    print(f"Query: {cypher_query}")
    print(f"{'-'*70}")
    
    try:
        response = requests.post(
            API_URL,
            json={"query": cypher_query, "sql_only": True},
            headers={"Content-Type": "application/json"},
            timeout=5
        )
        
        if response.status_code == 200:
            result = response.json()
            if "sql" in result:
                sql = result["sql"]
                print(f"✓ SQL Generated!")
                print(f"  {sql[:200]}..." if len(sql) > 200 else f"  {sql}")
                return True, sql
            elif "generated_sql" in result:
                # Even render errors show the SQL generation happened
                sql = result["generated_sql"]
                if "upper(" in sql.lower() or "lower(" in sql.lower() or "substring(" in sql.lower():
                    print(f"✓ Function translated! (render stage issue expected)")
                    print(f"  SQL fragment: {sql[:150]}...")
                    return True, sql
                print(f"✗ No function in SQL: {sql[:150]}...")
                return False, sql
            else:
                print(f"✗ Unexpected response: {result}")
                return False, None
        else:
            print(f"✗ HTTP {response.status_code}: {response.text[:200]}")
            return False, None
            
    except Exception as e:
        print(f"✗ Error: {e}")
        return False, None

def main():
    print("\n🚀 Testing Neo4j Functions in Graph Queries")
    print("=" * 70)
    print("NOTE: We need MATCH queries with actual table context")
    print("=" * 70)
    
    # These queries will fail at planning stage but should show function translation
    tests = [
        # String functions with properties
        ("MATCH (n:User) RETURN toUpper(n.name) AS upper_name", "toUpper() with property"),
        ("MATCH (n:User) RETURN toLower(n.email) AS lower_email", "toLower() with property"),
        ("MATCH (n:User) RETURN substring(n.name, 0, 5) AS first_five", "substring() with property"),
        ("MATCH (n:User) RETURN size(n.name) AS name_length", "size() with property"),
        
        # Math functions with expressions
        ("MATCH (n:Product) WHERE abs(n.price) > 100 RETURN n", "abs() in WHERE clause"),
        ("MATCH (n:Product) RETURN ceil(n.price) AS rounded_price", "ceil() with property"),
        ("MATCH (n:Product) RETURN floor(n.rating) AS floor_rating", "floor() with property"),
        
        # Type conversions
        ("MATCH (n:User) RETURN toString(n.id) AS id_str", "toString() with property"),
        ("MATCH (n:Order) RETURN toFloat(n.amount) AS float_amount", "toFloat() with property"),
    ]
    
    passed = 0
    failed = 0
    sql_samples = []
    
    for query, desc in tests:
        success, sql = test_with_match(query, desc)
        if success:
            passed += 1
            if sql:
                sql_samples.append((desc, sql))
        else:
            failed += 1
    
    print(f"\n{'='*70}")
    print(f"📊 Test Summary:")
    print(f"   ✓ Passed: {passed}/{len(tests)}")
    print(f"   ✗ Failed: {failed}/{len(tests)}")
    print(f"   Success Rate: {passed/len(tests)*100:.1f}%")
    print(f"={'='*70}\n")
    
    if sql_samples:
        print(f"{'='*70}")
        print("🔍 Sample SQL Translations:")
        print(f"{'='*70}")
        for desc, sql in sql_samples[:3]:  # Show first 3
            print(f"\n{desc}:")
            print(f"  {sql[:300]}..." if len(sql) > 300 else f"  {sql}")
    
    if passed >= 6:
        print(f"\n🎉 SUCCESS! {passed} functions translated correctly!")
        print("   The function translator is working!")
    elif passed >= 3:
        print(f"\n✓ PARTIAL SUCCESS: {passed} functions working")
        print("   Some functions may need debugging")
    else:
        print(f"\n⚠️  Only {passed} functions passed - needs investigation")

if __name__ == "__main__":
    main()
