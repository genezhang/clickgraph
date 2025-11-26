#!/usr/bin/env python3
"""
Test script to validate new edge schema parsing
Tests both denormalized and polymorphic edge configurations
"""

import yaml
import sys
from pathlib import Path

def test_denormalized_schema():
    """Test OnTime denormalized schema parsing"""
    print("=" * 60)
    print("Testing: Denormalized Edge Schema (OnTime Flights)")
    print("=" * 60)
    
    schema_path = Path("schemas/examples/ontime_denormalized.yaml")
    
    if not schema_path.exists():
        print(f"❌ Schema file not found: {schema_path}")
        return False
    
    try:
        with open(schema_path) as f:
            schema = yaml.safe_load(f)
        
        print(f"✓ Schema loaded: {schema['name']}")
        
        # Check nodes
        nodes = schema['graph_schema']['nodes']
        print(f"✓ Nodes: {len(nodes)}")
        for node in nodes:
            print(f"  - {node['label']} (table: {node['table']})")
        
        # Check edges
        edges = schema['graph_schema']['edges']
        print(f"✓ Edges: {len(edges)}")
        for edge in edges:
            print(f"  - {edge['type']}")
            print(f"    Table: {edge['table']}")
            print(f"    From: {edge['from_node']} (ID: {edge['from_id']})")
            print(f"    To: {edge['to_node']} (ID: {edge['to_id']})")
            
            # Check composite ID
            if 'edge_id' in edge:
                edge_id = edge['edge_id']
                if isinstance(edge_id, list):
                    print(f"    Composite ID: {edge_id}")
                else:
                    print(f"    Single ID: {edge_id}")
            
            # Check denormalized properties
            if 'from_node_properties' in edge:
                props = edge['from_node_properties']
                print(f"    From Node Properties: {list(props.keys())}")
            
            if 'to_node_properties' in edge:
                props = edge['to_node_properties']
                print(f"    To Node Properties: {list(props.keys())}")
        
        # Validation check: Node and edge share same table
        node_table = nodes[0]['table']
        edge_table = edges[0]['table']
        if node_table == edge_table:
            print(f"\n✓ Denormalized detected: Node and edge share table '{node_table}'")
        
        # Validation check: Has from_node_properties and to_node_properties
        has_from_props = 'from_node_properties' in edges[0]
        has_to_props = 'to_node_properties' in edges[0]
        if has_from_props and has_to_props:
            print("✓ Required denormalized properties present")
        
        print("\n✅ Denormalized schema test PASSED\n")
        return True
        
    except Exception as e:
        print(f"\n❌ Error: {e}\n")
        return False

def test_polymorphic_schema():
    """Test polymorphic edge schema parsing"""
    print("=" * 60)
    print("Testing: Polymorphic Edge Schema (Social Graph)")
    print("=" * 60)
    
    schema_path = Path("schemas/examples/social_polymorphic.yaml")
    
    if not schema_path.exists():
        print(f"❌ Schema file not found: {schema_path}")
        return False
    
    try:
        with open(schema_path) as f:
            schema = yaml.safe_load(f)
        
        print(f"✓ Schema loaded: {schema['name']}")
        
        # Check nodes
        nodes = schema['graph_schema']['nodes']
        print(f"✓ Nodes: {len(nodes)}")
        for node in nodes:
            print(f"  - {node['label']}")
        
        # Check edges
        edges = schema['graph_schema']['edges']
        print(f"✓ Edges: {len(edges)}")
        for edge in edges:
            if 'polymorphic' in edge and edge['polymorphic']:
                print(f"  - Polymorphic Edge Discovery")
                print(f"    Table: {edge['table']}")
                print(f"    Type Column: {edge['type_column']}")
                print(f"    From Label Column: {edge['from_label_column']}")
                print(f"    To Label Column: {edge['to_label_column']}")
                
                if 'type_values' in edge:
                    types = edge['type_values']
                    print(f"    Expected Types: {types}")
                
                # Check composite ID
                if 'edge_id' in edge:
                    edge_id = edge['edge_id']
                    if isinstance(edge_id, list):
                        print(f"    Composite ID: {edge_id}")
                
                print(f"    Shared Properties: {list(edge['property_mappings'].keys())}")
        
        # Validation checks
        poly_edge = edges[0]
        has_type_col = 'type_column' in poly_edge
        has_from_label = 'from_label_column' in poly_edge
        has_to_label = 'to_label_column' in poly_edge
        
        if has_type_col and has_from_label and has_to_label:
            print("\n✓ Required polymorphic discovery columns present")
        
        print("\n✅ Polymorphic schema test PASSED\n")
        return True
        
    except Exception as e:
        print(f"\n❌ Error: {e}\n")
        return False

def main():
    """Run all schema tests"""
    print("\n" + "=" * 60)
    print("Edge Schema Validation Tests")
    print("=" * 60 + "\n")
    
    results = []
    
    # Test denormalized schema
    results.append(("Denormalized", test_denormalized_schema()))
    
    # Test polymorphic schema
    results.append(("Polymorphic", test_polymorphic_schema()))
    
    # Summary
    print("=" * 60)
    print("Test Summary")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {name} Schema")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    print("=" * 60 + "\n")
    
    return 0 if passed == total else 1

if __name__ == "__main__":
    sys.exit(main())
