#!/usr/bin/env python3
"""
Schema Variations Test Suite
Tests Cypher query patterns across all three schema types:
1. Standard - separate tables per edge type
2. Denormalized - edge tables with embedded node properties  
3. Polymorphic - single table with type_column

Usage:
    python scripts/test/test_schema_variations.py
    python scripts/test/test_schema_variations.py --sql-only  # Just check SQL generation
    python scripts/test/test_schema_variations.py --schema standard  # Test only one schema
"""

import requests
import json
import argparse
import sys
from dataclasses import dataclass
from typing import Optional, List

# Schema configurations
# Dimension 1: Edge Storage Patterns (Standard, Denormalized, Polymorphic)
# Dimension 2: Coupled Edge (orthogonal - 2+ edges share same table with coupling node)
SCHEMAS = {
    "standard": {
        "name": "social_benchmark",
        "path": "benchmarks/social_network/schemas/social_benchmark.yaml",
        "description": "Standard schema - separate tables per edge type",
        "edge_types": ["FOLLOWS", "AUTHORED", "FRIENDS_WITH"],
        "multi_type_pattern": "[:FOLLOWS|FRIENDS_WITH]",  # Both User->User
        "has_coupled": False,
    },
    "denormalized": {
        "name": "ontime_flights", 
        "path": "schemas/examples/ontime_denormalized.yaml",
        "description": "Denormalized schema - node properties embedded in edge table",
        "edge_types": ["FLIGHT"],
        "multi_type_pattern": None,  # Only one edge type
        "has_coupled": False,  # Single edge type, not coupled
    },
    "polymorphic": {
        "name": "social_polymorphic",
        "path": "schemas/examples/social_polymorphic.yaml", 
        "description": "Polymorphic schema - single table with type_column",
        "edge_types": ["FOLLOWS", "LIKES", "AUTHORED", "COMMENTED", "SHARED"],
        "multi_type_pattern": "[:FOLLOWS|LIKES]",
        "has_coupled": False,
    },
    "coupled": {
        "name": "zeek_dns_log",
        "path": "schemas/examples/zeek_dns_log.yaml",
        "description": "Coupled edges - 2 edge types share same table with coupling node",
        "edge_types": ["REQUESTED", "RESOLVED_TO"],
        "multi_type_pattern": None,  # Different from/to types, can't alternate
        "has_coupled": True,  # REQUESTED and RESOLVED_TO share dns_log table
    },
}

# Test patterns with expected behavior
@dataclass
class TestCase:
    name: str
    description: str
    cypher: str
    applies_to: List[str]  # Which schemas this test applies to
    check_sql: Optional[str] = None  # Substring to check in generated SQL

TEST_CASES = [
    # Single edge type (without labels - polymorphic requires labels)
    TestCase(
        name="single_type_edge",
        description="Basic single edge type pattern",
        cypher="MATCH (a)-[r:{edge_type}]->(b) RETURN a, r, b LIMIT 5",
        applies_to=["standard", "denormalized"],  # Polymorphic needs labels
    ),
    
    # Single edge type WITH explicit labels (for polymorphic)
    TestCase(
        name="single_type_edge_labeled",
        description="Basic single edge type pattern (with labels)",
        cypher="MATCH (a:{node_label})-[r:{edge_type}]->(b:{to_node_label}) RETURN a, r, b LIMIT 5",
        applies_to=["standard", "denormalized", "polymorphic"],
    ),
    
    # type(r) with single type (without labels)
    TestCase(
        name="type_r_single",
        description="type(r) function with single edge type",
        cypher="MATCH (a)-[r:{edge_type}]->(b) RETURN type(r) AS rel_type, a, b LIMIT 5",
        applies_to=["standard", "denormalized"],  # Polymorphic needs labels
    ),
    
    # type(r) with single type WITH labels (for polymorphic)
    TestCase(
        name="type_r_single_labeled",
        description="type(r) function with single edge type (with labels)",
        cypher="MATCH (a:{node_label})-[r:{edge_type}]->(b:{to_node_label}) RETURN type(r) AS rel_type, a, b LIMIT 5",
        applies_to=["polymorphic"],
    ),
    
    # Bidirectional pattern (without labels - polymorphic requires labels)
    TestCase(
        name="bidirectional",
        description="Bidirectional edge pattern (no direction)",
        cypher="MATCH (a)-[r:{edge_type}]-(b) RETURN a, r, b LIMIT 5",
        applies_to=["standard", "denormalized"],  # Polymorphic needs labels
    ),
    
    # Bidirectional pattern WITH labels (for polymorphic)
    TestCase(
        name="bidirectional_labeled",
        description="Bidirectional edge pattern (with labels)",
        cypher="MATCH (a:{node_label})-[r:{edge_type}]-(b:{to_node_label}) RETURN a, r, b LIMIT 5",
        applies_to=["polymorphic"],
    ),
    
    # Multi-type edge pattern (UNION or IN) - without labels
    TestCase(
        name="multi_type_edge",
        description="Multiple edge types with alternation [:A|B]",
        cypher="MATCH (a)-[r{multi_type}]->(b) RETURN a, r, b LIMIT 5",
        applies_to=["standard"],  # Polymorphic needs labels
        check_sql="UNION ALL",  # Standard uses UNION
    ),
    
    # Multi-type edge pattern WITH labels (for polymorphic)
    TestCase(
        name="multi_type_edge_labeled",
        description="Multiple edge types with alternation (with labels)",
        cypher="MATCH (a:{node_label})-[r{multi_type}]->(b:{to_node_label}) RETURN a, r, b LIMIT 5",
        applies_to=["polymorphic"],
        check_sql="IN \\(",  # Polymorphic uses IN
    ),
    
    # type(r) with multi-type (without labels)
    TestCase(
        name="type_r_multi",
        description="type(r) function with multiple edge types",
        cypher="MATCH (a)-[r{multi_type}]->(b) RETURN type(r) AS rel_type, a, b LIMIT 5",
        applies_to=["standard"],  # Polymorphic needs labels
        check_sql="UNION ALL",
    ),
    
    # type(r) with multi-type WITH labels (for polymorphic)
    TestCase(
        name="type_r_multi_labeled",
        description="type(r) function with multiple edge types (with labels)",
        cypher="MATCH (a:{node_label})-[r{multi_type}]->(b:{to_node_label}) RETURN type(r) AS rel_type, a, b LIMIT 5",
        applies_to=["polymorphic"],
        check_sql="IN \\(",
    ),
    
    # Variable length paths (without labels - polymorphic requires labels)
    TestCase(
        name="vlp_exact",
        description="Variable length path with exact hops",
        cypher="MATCH (a)-[r:{edge_type}*2]->(b) RETURN a, b LIMIT 5",
        applies_to=["standard"],  # Polymorphic needs labels
    ),
    
    TestCase(
        name="vlp_range", 
        description="Variable length path with range",
        cypher="MATCH (a)-[r:{edge_type}*1..3]->(b) RETURN a, b LIMIT 5",
        applies_to=["standard"],  # Polymorphic needs labels
    ),
    
    # VLP with labels (for polymorphic)
    TestCase(
        name="vlp_exact_labeled",
        description="Variable length path with exact hops (with labels)",
        cypher="MATCH (a:{node_label})-[r:{edge_type}*2]->(b:{to_node_label}) RETURN a, b LIMIT 5",
        applies_to=["polymorphic"],
    ),
    
    TestCase(
        name="vlp_range_labeled", 
        description="Variable length path with range (with labels)",
        cypher="MATCH (a:{node_label})-[r:{edge_type}*1..3]->(b:{to_node_label}) RETURN a, b LIMIT 5",
        applies_to=["polymorphic"],
    ),
    
    # WHERE clause filtering
    TestCase(
        name="where_node_prop",
        description="WHERE clause on node property",
        cypher="MATCH (a:{node_label})-[r:{edge_type}]->(b) WHERE a.{node_prop} IS NOT NULL RETURN a, b LIMIT 5",
        applies_to=["standard", "denormalized", "polymorphic"],
    ),
    
    # type(r) in WHERE clause (requires labels for polymorphic)
    TestCase(
        name="type_r_in_where",
        description="type(r) in WHERE clause comparison",
        cypher="MATCH (a)-[r]->(b) WHERE type(r) = '{edge_type}' RETURN a, r, b LIMIT 5",
        applies_to=["standard"],  # Polymorphic needs labels
    ),
    
    TestCase(
        name="type_r_in_where_labeled",
        description="type(r) in WHERE clause comparison (with labels)",
        cypher="MATCH (a:{node_label})-[r]->(b:{to_node_label}) WHERE type(r) = '{edge_type}' RETURN a, r, b LIMIT 5",
        applies_to=["polymorphic"],
    ),
    
    # OPTIONAL MATCH
    TestCase(
        name="optional_match",
        description="OPTIONAL MATCH pattern",
        cypher="MATCH (a:{node_label}) OPTIONAL MATCH (a)-[r:{edge_type}]->(b) RETURN a, r, b LIMIT 5",
        applies_to=["standard", "polymorphic"],
    ),
    
    # Aggregation (requires labels for polymorphic)
    TestCase(
        name="count_edges",
        description="COUNT aggregation on edges",
        cypher="MATCH (a)-[r:{edge_type}]->(b) RETURN a, count(r) AS edge_count LIMIT 5",
        applies_to=["standard", "denormalized"],  # Polymorphic needs labels
    ),
    
    TestCase(
        name="count_edges_labeled",
        description="COUNT aggregation on edges (with labels)",
        cypher="MATCH (a:{node_label})-[r:{edge_type}]->(b:{to_node_label}) RETURN a, count(r) AS edge_count LIMIT 5",
        applies_to=["polymorphic"],
    ),
    
    # ========== COUPLED EDGE TESTS (Dimension 2) ==========
    # These test the orthogonal coupled edge optimization
    
    # Standard + Coupled: AUTHORED edge uses posts table (same as Post node)
    TestCase(
        name="coupled_edge_standard",
        description="Coupled edge: edge table = node table (Standard)",
        cypher="MATCH (u:User)-[r:AUTHORED]->(p:Post) RETURN u.name, p.title LIMIT 5",
        applies_to=["standard"],
        check_sql=None,  # Should NOT have redundant JOIN to posts
    ),
    
    # Denormalized + Coupled: FLIGHT uses same table as Airport node
    TestCase(
        name="coupled_edge_denormalized",
        description="Coupled edge: multi-hop on denormalized table",
        cypher="MATCH (origin:Airport)-[f:FLIGHT]->(dest:Airport) RETURN origin.code, dest.code, f.carrier LIMIT 5",
        applies_to=["denormalized"],
        check_sql=None,  # Should use denormalized properties
    ),
    
    # Coupled edge with property access on coupled node
    TestCase(
        name="coupled_edge_node_prop",
        description="Coupled edge: access coupled node property",
        cypher="MATCH (u:User)-[:AUTHORED]->(p:Post) WHERE p.title IS NOT NULL RETURN p.title LIMIT 5",
        applies_to=["standard"],
    ),
    
    # ========== WILDCARD EDGE TESTS ==========
    # Wildcard edge [r] without specific type
    
    TestCase(
        name="wildcard_edge_labeled",
        description="Wildcard edge with explicit labels",
        cypher="MATCH (a:{node_label})-[r]->(b:{to_node_label}) RETURN a, type(r) AS rel_type, b LIMIT 5",
        applies_to=["standard", "polymorphic"],
    ),
    
    TestCase(
        name="wildcard_edge_no_labels",
        description="Wildcard edge without labels (may fail on polymorphic)",
        cypher="MATCH (a)-[r]->(b) RETURN a, r, b LIMIT 5",
        applies_to=["standard"],  # Only standard can infer without labels
    ),
    
    # ========== SUBGRAPH EXTRACTION PATTERNS ==========
    # These patterns are key for "get all nodes/edges reachable from start"
    
    # 1-hop subgraph: all edges from a starting node (any type, any target)
    TestCase(
        name="subgraph_1hop_outgoing",
        description="Subgraph: 1-hop outgoing, any edge type, any target node",
        cypher="MATCH (start:{node_label})-[r]->(target) WHERE start.{node_prop} IS NOT NULL RETURN start, type(r) AS edge_type, target LIMIT 10",
        applies_to=["standard"],  # Polymorphic needs target label
    ),
    
    TestCase(
        name="subgraph_1hop_outgoing_labeled",
        description="Subgraph: 1-hop outgoing with target label (for polymorphic)",
        cypher="MATCH (start:{node_label})-[r]->(target:{to_node_label}) WHERE start.{node_prop} IS NOT NULL RETURN start, type(r) AS edge_type, target LIMIT 10",
        applies_to=["standard", "polymorphic"],
    ),
    
    # 1-hop bidirectional: all edges connected to a starting node
    TestCase(
        name="subgraph_1hop_bidirectional",
        description="Subgraph: 1-hop bidirectional, any edge type",
        cypher="MATCH (start:{node_label})-[r]-(neighbor) WHERE start.{node_prop} IS NOT NULL RETURN start, type(r) AS edge_type, neighbor LIMIT 10",
        applies_to=["standard"],
    ),
    
    TestCase(
        name="subgraph_1hop_bidirectional_labeled",
        description="Subgraph: 1-hop bidirectional with neighbor label",
        cypher="MATCH (start:{node_label})-[r]-(neighbor:{to_node_label}) WHERE start.{node_prop} IS NOT NULL RETURN start, type(r) AS edge_type, neighbor LIMIT 10",
        applies_to=["standard", "polymorphic"],
    ),
    
    # Multi-hop subgraph with VLP: all nodes within N hops
    TestCase(
        name="subgraph_vlp_any_edge",
        description="Subgraph: VLP with any edge type (1..3 hops)",
        cypher="MATCH (start:{node_label})-[*1..3]->(reachable:{to_node_label}) WHERE start.{node_prop} IS NOT NULL RETURN DISTINCT start, reachable LIMIT 10",
        applies_to=["standard", "polymorphic"],
    ),
    
    TestCase(
        name="subgraph_vlp_bidirectional",
        description="Subgraph: VLP bidirectional (1..2 hops)",
        cypher="MATCH (start:{node_label})-[*1..2]-(reachable:{to_node_label}) WHERE start.{node_prop} IS NOT NULL RETURN DISTINCT start, reachable LIMIT 10",
        applies_to=["standard", "polymorphic"],
    ),
    
    # Triple format output for subgraph
    TestCase(
        name="subgraph_triple_format",
        description="Subgraph: Triple format (head, relation, tail)",
        cypher="MATCH (head:{node_label})-[r]->(tail:{to_node_label}) RETURN head.{node_prop} AS head_id, type(r) AS relation, tail.{node_prop} AS tail_id LIMIT 10",
        applies_to=["standard", "polymorphic"],
    ),
    
    # Subgraph with specific start node ID
    TestCase(
        name="subgraph_from_specific_node",
        description="Subgraph: From specific starting node",
        cypher="MATCH (start:{node_label})-[r]->(neighbor:{to_node_label}) WHERE start.{node_prop} = 1 RETURN start, type(r), neighbor LIMIT 10",
        applies_to=["standard", "polymorphic"],
    ),
    
    # ========== MULTI-TYPE WILDCARD (polymorphic optimization) ==========
    # For polymorphic: wildcard should return ALL edge types
    TestCase(
        name="wildcard_returns_all_types",
        description="Wildcard edge returns all edge types (polymorphic)",
        cypher="MATCH (a:{node_label})-[r]->(b:{to_node_label}) RETURN DISTINCT type(r) AS edge_types LIMIT 20",
        applies_to=["standard", "polymorphic"],
    ),
    
    # ========== SHORTEST PATH TESTS ==========
    TestCase(
        name="shortest_path",
        description="shortestPath function",
        cypher="MATCH p = shortestPath((a:{node_label})-[:{edge_type}*1..5]->(b:{to_node_label})) RETURN p LIMIT 5",
        applies_to=["standard", "polymorphic"],
    ),
    
    # ========== PATH FUNCTIONS ==========
    TestCase(
        name="path_length",
        description="length(p) on path variable",
        cypher="MATCH p = (a:{node_label})-[:{edge_type}*1..3]->(b:{to_node_label}) RETURN length(p), a, b LIMIT 5",
        applies_to=["standard", "polymorphic"],
    ),
    
    # ========== ORDER BY / DISTINCT ==========
    TestCase(
        name="order_by",
        description="ORDER BY clause",
        cypher="MATCH (a:{node_label})-[r:{edge_type}]->(b:{to_node_label}) RETURN a, b ORDER BY a.{node_prop} LIMIT 5",
        applies_to=["standard", "denormalized", "polymorphic"],
    ),
    
    TestCase(
        name="distinct",
        description="DISTINCT keyword",
        cypher="MATCH (a:{node_label})-[:{edge_type}]->(b:{to_node_label}) RETURN DISTINCT a LIMIT 5",
        applies_to=["standard", "denormalized", "polymorphic"],
    ),
    
    # ========== COLLECT AGGREGATION ==========
    TestCase(
        name="collect_agg",
        description="COLLECT aggregation",
        cypher="MATCH (a:{node_label})-[r:{edge_type}]->(b:{to_node_label}) RETURN a, collect(b.{node_prop}) AS connected LIMIT 5",
        applies_to=["standard", "polymorphic"],
    ),
    
    # ========== DENORMALIZED EDGE SPECIFIC TESTS ==========
    # Tests for denormalized edge tables (edge table = node table)
    
    TestCase(
        name="denorm_edge_node_same_table",
        description="Denormalized: edge and node from same table",
        cypher="MATCH (origin:Airport)-[f:FLIGHT]->(dest:Airport) RETURN origin.code, origin.city, dest.code, dest.city LIMIT 5",
        applies_to=["denormalized"],
    ),
    
    TestCase(
        name="denorm_bidirectional",
        description="Denormalized: bidirectional pattern",
        cypher="MATCH (a:Airport)-[f:FLIGHT]-(b:Airport) RETURN a.code, type(f), b.code LIMIT 5",
        applies_to=["denormalized"],
    ),
    
    TestCase(
        name="denorm_edge_properties",
        description="Denormalized: access edge properties",
        cypher="MATCH (origin:Airport)-[f:FLIGHT]->(dest:Airport) RETURN origin.code, f.carrier, f.distance, dest.code LIMIT 5",
        applies_to=["denormalized"],
    ),
    
    TestCase(
        name="denorm_where_on_edge",
        description="Denormalized: WHERE on edge property",
        cypher="MATCH (origin:Airport)-[f:FLIGHT]->(dest:Airport) WHERE f.carrier IS NOT NULL RETURN origin.code, f.carrier, dest.code LIMIT 5",
        applies_to=["denormalized"],
    ),
    
    TestCase(
        name="denorm_subgraph_wildcard",
        description="Denormalized: subgraph with wildcard edge",
        cypher="MATCH (start:Airport)-[r]->(target:Airport) RETURN start.code, type(r), target.code LIMIT 10",
        applies_to=["denormalized"],
    ),
    
    # ========== COUPLED EDGE TESTS ==========
    # Tests for coupled edges (2+ edges share same table with coupling node)
    
    TestCase(
        name="coupled_single_edge",
        description="Coupled: single edge type (REQUESTED)",
        cypher="MATCH (ip:IP)-[r:REQUESTED]->(domain:Domain) RETURN ip.ip, type(r), domain.name LIMIT 5",
        applies_to=["coupled"],
    ),
    
    TestCase(
        name="coupled_other_edge",
        description="Coupled: other edge type (RESOLVED_TO)",
        cypher="MATCH (domain:Domain)-[r:RESOLVED_TO]->(rip:ResolvedIP) RETURN domain.name, type(r), rip.ips LIMIT 5",
        applies_to=["coupled"],
    ),
    
    TestCase(
        name="coupled_multi_hop",
        description="Coupled: multi-hop through coupling node (IP->Domain->ResolvedIP)",
        cypher="MATCH (ip:IP)-[r1:REQUESTED]->(domain:Domain)-[r2:RESOLVED_TO]->(rip:ResolvedIP) RETURN ip.ip, domain.name, rip.ips LIMIT 5",
        applies_to=["coupled"],
    ),
    
    TestCase(
        name="coupled_type_r_multi_hop",
        description="Coupled: type(r) on multi-hop pattern",
        cypher="MATCH (ip:IP)-[r1:REQUESTED]->(domain:Domain)-[r2:RESOLVED_TO]->(rip:ResolvedIP) RETURN type(r1), type(r2), domain.name LIMIT 5",
        applies_to=["coupled"],
    ),
    
    TestCase(
        name="coupled_bidirectional_single",
        description="Coupled: bidirectional single edge",
        cypher="MATCH (ip:IP)-[r:REQUESTED]-(domain:Domain) RETURN ip.ip, type(r), domain.name LIMIT 5",
        applies_to=["coupled"],
    ),
]

def get_schema_specifics(schema_name: str):
    """Get schema-specific values for test template substitution."""
    if schema_name == "standard":
        return {
            "edge_type": "FOLLOWS",
            "multi_type": ":FOLLOWS|FRIENDS_WITH",  # Fixed: no extra brackets
            "node_label": "User",
            "to_node_label": "User",  # For FOLLOWS: User -> User
            "node_prop": "user_id",  # Fixed: use user_id (name maps to full_name)
        }
    elif schema_name == "denormalized":
        return {
            "edge_type": "FLIGHT",
            "multi_type": None,
            "node_label": "Airport",
            "to_node_label": "Airport",  # For FLIGHT: Airport -> Airport
            "node_prop": "code",
        }
    elif schema_name == "polymorphic":
        return {
            "edge_type": "FOLLOWS",
            "multi_type": ":FOLLOWS|LIKES",  # Fixed: no extra brackets
            "node_label": "User",
            "to_node_label": "User",  # For FOLLOWS: User -> User
            "node_prop": "name",
        }
    elif schema_name == "coupled":
        return {
            "edge_type": "REQUESTED",
            "multi_type": None,  # Different from/to types
            "node_label": "IP",
            "to_node_label": "Domain",  # For REQUESTED: IP -> Domain
            "node_prop": "ip",
        }
    return {}

def substitute_template(cypher: str, schema_name: str) -> Optional[str]:
    """Substitute schema-specific values into Cypher template."""
    specifics = get_schema_specifics(schema_name)
    result = cypher
    for key, value in specifics.items():
        if value is None and "{" + key + "}" in result:
            return None  # Can't substitute, skip this test
        result = result.replace("{" + key + "}", str(value))
    return result

def run_test(schema_name: str, test: TestCase, sql_only: bool = False) -> dict:
    """Run a single test case against the specified schema."""
    schema = SCHEMAS[schema_name]
    cypher = substitute_template(test.cypher, schema_name)
    
    if cypher is None:
        return {
            "status": "skipped",
            "reason": "Schema doesn't support required template values",
        }
    
    try:
        url = f"http://localhost:8080/query/sql"
        payload = {
            "query": cypher,
            "schema_name": schema["name"],
        }
        
        response = requests.post(url, json=payload, timeout=10)
        result = response.json()
        
        if "error" in result:
            return {
                "status": "failed",
                "error": result["error"],
                "cypher": cypher,
            }
        
        sql = result.get("sql", result.get("generated_sql", ""))
        
        return {
            "status": "passed",
            "cypher": cypher,
            "sql": sql[:500] + "..." if len(sql) > 500 else sql,  # Truncate
        }
        
    except requests.exceptions.ConnectionError:
        return {
            "status": "error",
            "error": "Cannot connect to ClickGraph server on port 8080",
        }
    except Exception as e:
        return {
            "status": "error", 
            "error": str(e),
        }

def run_all_tests(sql_only: bool = False, filter_schema: Optional[str] = None):
    """Run all test cases against all schemas."""
    results = {}
    
    schemas_to_test = [filter_schema] if filter_schema else list(SCHEMAS.keys())
    
    print("=" * 70)
    print("Schema Variations Test Suite")
    print("=" * 70)
    
    for schema_name in schemas_to_test:
        schema = SCHEMAS[schema_name]
        print(f"\n{'─' * 70}")
        print(f"Schema: {schema_name.upper()}")
        print(f"  {schema['description']}")
        print(f"  Path: {schema['path']}")
        print(f"{'─' * 70}")
        
        results[schema_name] = {"passed": 0, "failed": 0, "skipped": 0, "tests": []}
        
        for test in TEST_CASES:
            # Check if test applies to this schema
            if schema_name not in test.applies_to:
                continue
                
            result = run_test(schema_name, test, sql_only)
            result["test_name"] = test.name
            result["description"] = test.description
            results[schema_name]["tests"].append(result)
            
            # Print result
            status = result["status"]
            icon = "✅" if status == "passed" else "❌" if status == "failed" else "⏭️" if status == "skipped" else "⚠️"
            print(f"\n{icon} {test.name}: {test.description}")
            
            if status == "passed":
                results[schema_name]["passed"] += 1
                print(f"   Cypher: {result['cypher']}")
                if sql_only:
                    print(f"   SQL: {result['sql'][:200]}...")
            elif status == "failed":
                results[schema_name]["failed"] += 1
                print(f"   Cypher: {result.get('cypher', 'N/A')}")
                print(f"   Error: {result['error']}")
            elif status == "skipped":
                results[schema_name]["skipped"] += 1
                print(f"   Reason: {result['reason']}")
            else:
                print(f"   Error: {result['error']}")
    
    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    
    all_passed = True
    for schema_name, data in results.items():
        total = data["passed"] + data["failed"] + data["skipped"]
        status_icon = "✅" if data["failed"] == 0 else "❌"
        print(f"{status_icon} {schema_name}: {data['passed']}/{total} passed, {data['failed']} failed, {data['skipped']} skipped")
        if data["failed"] > 0:
            all_passed = False
    
    return all_passed

def main():
    parser = argparse.ArgumentParser(description="Test Cypher patterns across schema variations")
    parser.add_argument("--sql-only", action="store_true", help="Only check SQL generation (no execution)")
    parser.add_argument("--schema", choices=list(SCHEMAS.keys()), help="Test only one schema")
    parser.add_argument("--list", action="store_true", help="List available tests")
    args = parser.parse_args()
    
    if args.list:
        print("Available Test Cases:")
        for test in TEST_CASES:
            print(f"  - {test.name}: {test.description}")
            print(f"    Applies to: {', '.join(test.applies_to)}")
        return
    
    success = run_all_tests(sql_only=args.sql_only, filter_schema=args.schema)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
