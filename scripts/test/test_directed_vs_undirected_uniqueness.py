"""
Test ClickGraph's current behavior for directed multi-hop patterns.

Question: Does ClickGraph enforce full pairwise uniqueness (a != c) for directed patterns?
"""

import requests
import json

CLICKGRAPH_URL = "http://localhost:8080/query"

def test_query(query_name, cypher_query):
    print(f"\n{'='*80}")
    print(f"TEST: {query_name}")
    print(f"{'='*80}")
    print(f"Query: {cypher_query}")
    
    response = requests.post(
        CLICKGRAPH_URL,
        json={"query": cypher_query}
    )
    
    if response.status_code == 200:
        result = response.json()
        
        # Check if sql_only mode or has results
        if "sql" in result:
            print("\nGenerated SQL:")
            print(result["sql"])
            
            # Look for uniqueness filters
            sql = result["sql"].lower()
            has_a_ne_b = "a.user_id <> b.user_id" in sql or "user.user_id <> intermediate" in sql
            has_b_ne_c = "b.user_id <> c.user_id" in sql or "intermediate.user_id <> fof" in sql
            has_a_ne_c = "a.user_id <> c.user_id" in sql or "user.user_id <> fof" in sql
            
            print("\nUniqueness Filters Analysis:")
            print(f"  a != b (adjacent): {'[OK]' if has_a_ne_b else '[MISSING]'}")
            print(f"  b != c (adjacent): {'[OK]' if has_b_ne_c else '[MISSING]'}")
            print(f"  a != c (overall):  {'[OK]' if has_a_ne_c else '[MISSING]'}")
        
        if "results" in result:
            print(f"\nResults: {len(result['results'])} rows")
            for row in result["results"][:5]:
                print(f"  {row}")
    else:
        print(f"\nERROR: {response.status_code}")
        print(response.text)

def main():
    print("="*80)
    print("Testing ClickGraph: Directed vs Undirected Multi-Hop Patterns")
    print("="*80)
    
    # Test 1: Directed explicit 2-hop
    test_query(
        "Directed Explicit 2-Hop",
        """
        MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]->(c:User)
        WHERE a.user_id = 1
        RETURN a.user_id, b.user_id, c.user_id
        LIMIT 10
        """
    )
    
    # Test 2: Undirected explicit 2-hop (for comparison)
    test_query(
        "Undirected Explicit 2-Hop",
        """
        MATCH (a:User)-[:FOLLOWS]-(b:User)-[:FOLLOWS]-(c:User)
        WHERE a.user_id = 1
        RETURN a.user_id, b.user_id, c.user_id
        LIMIT 10
        """
    )
    
    # Test 3: Mixed direction
    test_query(
        "Mixed Direction 2-Hop",
        """
        MATCH (a:User)-[:FOLLOWS]->(b:User)-[:FOLLOWS]-(c:User)
        WHERE a.user_id = 1
        RETURN a.user_id, b.user_id, c.user_id
        LIMIT 10
        """
    )
    
    # Test 4: Directed variable-length (already working)
    test_query(
        "Directed Variable-Length *2",
        """
        MATCH (a:User)-[:FOLLOWS*2]->(c:User)
        WHERE a.user_id = 1
        RETURN a.user_id, c.user_id
        LIMIT 10
        """
    )
    
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print("""
Expected Behavior (Neo4j verified):
1. Directed explicit 2-hop: Should enforce a!=b, b!=c, AND a!=c
2. Undirected explicit 2-hop: Should enforce a!=b, b!=c, AND a!=c
3. Mixed direction: Should enforce a!=b, b!=c, AND a!=c
4. Variable-length: Already enforces a!=c (working!)

Key Question: Do explicit multi-hop patterns (1-3) enforce overall a!=c?
    """)

if __name__ == "__main__":
    main()
