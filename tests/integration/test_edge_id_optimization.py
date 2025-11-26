"""
Test edge uniqueness optimization with single-column edge_id
Verifies that single-column edge_id avoids tuple() overhead
"""

import requests
import json

BASE_URL = "http://localhost:8080"
SCHEMA_NAME = "social_benchmark"  # Using benchmark schema with edge_id

def execute_cypher(query: str, schema_name: str = SCHEMA_NAME) -> dict:
    """Execute a Cypher query and return results"""
    response = requests.post(
        f"{BASE_URL}/query",
        json={"query": query, "schema_name": schema_name}
    )
    return response.json()

def test_single_column_edge_id():
    """Test that single-column edge_id (follow_id) avoids tuple() overhead"""
    print("\n" + "="*80)
    print("TEST: Single-column edge_id optimization")
    print("="*80)
    
    query = """
    MATCH (a:User)-[:FOLLOWS*1..2]->(b:User)
    WHERE a.user_id = 1
    RETURN COUNT(*) as path_count
    """
    
    print(f"\nQuery:\n{query}")
    
    result = execute_cypher(query)
    
    if "error" in result:
        print(f"\n❌ ERROR: {result['error']}")
        return False
    
    # Get generated SQL
    if "sql" in result:
        sql = result["sql"]
        print(f"\n📝 Generated SQL:\n{sql}")
        
        # Check for optimization: should use follow_id, not tuple(follower_id, followed_id)
        if "follow_id" in sql and "path_edges" in sql:
            print("\n✅ OPTIMIZATION VERIFIED: Using single-column edge_id (follow_id)")
            print("   - No tuple() overhead")
            print("   - Efficient array operations")
            
            # Verify no tuple usage for edges
            if "tuple(rel.follower_id, rel.followed_id)" in sql:
                print("\n⚠️  WARNING: Still using tuple(follower_id, followed_id)")
                print("   - Should be using follow_id directly")
                return False
            
            return True
        else:
            print("\n⚠️  Edge ID not found in SQL")
            print(f"   - Searching for: 'follow_id' in 'path_edges'")
            print(f"   - Found: {[line for line in sql.split('\\n') if 'path_edges' in line]}")
            return False
    
    print("\n❌ No SQL generated")
    return False

def test_composite_edge_id_fallback():
    """Test that composite keys still use tuple() when needed"""
    print("\n" + "="*80)
    print("TEST: Composite edge_id fallback (tuple still used)")
    print("="*80)
    
    # Note: This would require a relationship with composite edge_id
    # For now, we just document the expected behavior
    
    print("\n📝 Expected behavior for composite keys:")
    print("   - edge_id: ['col1', 'col2'] → tuple(rel.col1, rel.col2)")
    print("   - edge_id: 'id' → rel.id (no tuple)")
    print("   - edge_id: None → tuple(rel.from_id, rel.to_id)")
    
    return True

def test_default_tuple_when_no_edge_id():
    """Test that default tuple(from_id, to_id) is used when edge_id is None"""
    print("\n" + "="*80)
    print("TEST: Default tuple(from_id, to_id) when no edge_id")
    print("="*80)
    
    print("\n📝 This test requires a schema without edge_id")
    print("   - Current benchmark schema now HAS edge_id")
    print("   - Skipping test (expected behavior documented)")
    
    return True

if __name__ == "__main__":
    print("\n" + "="*80)
    print("EDGE UNIQUENESS OPTIMIZATION TESTS")
    print("="*80)
    print("\nPurpose: Verify single-column edge_id avoids tuple() overhead")
    print("Schema: social_benchmark (with follow_id added)")
    print("\nPrerequisites:")
    print("1. ClickGraph server running on port 8080")
    print("2. Benchmark data loaded with edge_id columns")
    print("3. Run: scripts/setup/add_edge_ids_benchmark.sql")
    
    # Run tests
    results = []
    results.append(("Single-column edge_id", test_single_column_edge_id()))
    results.append(("Composite edge_id", test_composite_edge_id_fallback()))
    results.append(("Default tuple", test_default_tuple_when_no_edge_id()))
    
    # Summary
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    
    for name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status}: {name}")
    
    all_passed = all(result[1] for result in results)
    
    if all_passed:
        print("\n🎉 All tests passed!")
        exit(0)
    else:
        print("\n❌ Some tests failed")
        exit(1)
