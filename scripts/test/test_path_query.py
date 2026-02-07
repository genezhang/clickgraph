#!/usr/bin/env python3
"""Test path queries with Bolt protocol"""

import os
from neo4j import GraphDatabase
import json

# Connect to ClickGraph with configurable port
bolt_port = os.getenv("CLICKGRAPH_BOLT_PORT", "7688")
driver = GraphDatabase.driver(f"bolt://localhost:{bolt_port}", auth=("", ""))

try:
    with driver.session() as session:
        # Test 1: Initial query to get nodes
        print("Test 1: Get initial nodes")
        result = session.run("MATCH (u:User) RETURN u LIMIT 5")
        nodes = [record["u"] for record in result]
        print(f"✓ Got {len(nodes)} nodes")
        for node in nodes[:2]:
            print(f"  Node: {node}")
        
        # Get first node ID
        if nodes:
            first_id = nodes[0].id
            print(f"\n Test 2: Path query with node id = {first_id}")
            
            # Test 2: Path expansion query (simulates Neo4j Browser double-click)
            query = f"MATCH path = (a)--(o) WHERE id(a) = {first_id} RETURN path"
            print(f"Query: {query}")
            result = session.run(query)
            
            paths = [record["path"] for record in result]
            print(f"✓ Got {len(paths)} paths")
            
            if paths:
                first_path = paths[0]
                print(f"\nFirst path details:")
                print(f"  Nodes: {first_path.nodes}")
                print(f"  Relationships: {first_path.relationships}")
                
                # Check if nodes have properties
                for i, node in enumerate(first_path.nodes):
                    print(f"\n  Node {i}: labels={node.labels}, id={node.id}")
                    print(f"    Properties: {dict(node)}")
                    
                # Check if relationships have properties  
                for i, rel in enumerate(first_path.relationships):
                    print(f"\n  Relationship {i}: type={rel.type}, id={rel.id}")
                    print(f"    Properties: {dict(rel)}")
            
            print(f"\n✅ SUCCESS: Path query worked!")
        else:
            print("❌ No nodes returned")
            
finally:
    driver.close()
