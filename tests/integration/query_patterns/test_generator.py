#!/usr/bin/env python3
"""
Query Pattern × Schema Variation Test Generator

This module generates comprehensive test cases by combining:
- 22+ query patterns (from query-pattern-completeness.md)
- 4 schema variations (standard, denormalized, polymorphic, coupled)

Goal: Ensure every pattern works correctly on every schema type.
"""

import itertools
import random
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from enum import Enum


class SchemaType(Enum):
    """Schema variation types"""
    STANDARD = "standard"           # Separate node/edge tables
    DENORMALIZED = "denormalized"   # Node properties embedded in edge table
    POLYMORPHIC = "polymorphic"     # Single edge table with type column
    COUPLED = "coupled"             # Multiple edge types share same table


class QueryCategory(Enum):
    """Query pattern categories"""
    NODE_SCAN = "node_scan"
    NODE_FILTER = "node_filter"
    RETURN_NODE = "return_node"
    RETURN_PROPS = "return_props"
    SINGLE_HOP = "single_hop"
    MULTI_HOP = "multi_hop"
    UNDIRECTED = "undirected"
    RETURN_REL = "return_rel"
    VLP_EXACT = "vlp_exact"
    VLP_RANGE = "vlp_range"
    VLP_PATH_VAR = "vlp_path_var"
    SIMPLE_AGG = "simple_agg"
    GROUP_BY = "group_by"
    WITH_AGG = "with_agg"
    OPTIONAL_MATCH = "optional_match"
    MULTI_TYPE = "multi_type"
    SHORTEST_PATH = "shortest_path"
    ORDER_LIMIT = "order_limit"
    GRAPH_FUNCS = "graph_funcs"
    AGG_FUNCS = "agg_funcs"
    PATH_FUNCS = "path_funcs"
    SCALAR_FUNCS = "scalar_funcs"


@dataclass
class SchemaConfig:
    """Configuration for a specific schema type"""
    name: str
    schema_type: SchemaType
    yaml_path: str
    node_labels: List[str]
    rel_types: List[str]
    node_props: Dict[str, List[str]]  # label -> properties
    rel_props: Dict[str, List[str]]   # type -> properties
    
    # For polymorphic schemas
    type_column: Optional[str] = None
    
    # For coupled schemas  
    shared_edge_table: Optional[str] = None


# Schema configurations for testing
SCHEMAS = {
    SchemaType.STANDARD: SchemaConfig(
        name="social_benchmark",
        schema_type=SchemaType.STANDARD,
        yaml_path="benchmarks/schemas/social_benchmark.yaml",
        node_labels=["User", "Post"],
        rel_types=["FOLLOWS", "AUTHORED", "LIKED"],
        node_props={
            "User": ["user_id", "name", "email", "country", "city", "is_active"],
            "Post": ["post_id", "content", "created_at"],
        },
        rel_props={
            "FOLLOWS": ["follow_date"],
            "AUTHORED": ["authored_date"],
            "LIKED": ["liked_date"],
        },
    ),
    
    SchemaType.DENORMALIZED: SchemaConfig(
        name="ontime_flights",
        schema_type=SchemaType.DENORMALIZED,
        yaml_path="schemas/examples/ontime_denormalized.yaml",
        node_labels=["Airport"],
        rel_types=["FLIGHT"],
        node_props={
            "Airport": ["code", "city", "state", "airport"],
        },
        rel_props={
            "FLIGHT": ["flight_date", "flight_num", "carrier", "distance", "delay"],
        },
    ),
    
    SchemaType.POLYMORPHIC: SchemaConfig(
        name="social_polymorphic",
        schema_type=SchemaType.POLYMORPHIC,
        yaml_path="schemas/examples/social_polymorphic.yaml",
        node_labels=["User"],
        rel_types=["FOLLOWS", "LIKES", "AUTHORED"],
        node_props={
            "User": ["user_id", "name", "email"],
        },
        rel_props={
            "FOLLOWS": ["timestamp"],
            "LIKES": ["timestamp"],
            "AUTHORED": ["timestamp"],
        },
        type_column="interaction_type",
    ),
    
    SchemaType.COUPLED: SchemaConfig(
        name="zeek_dns",
        schema_type=SchemaType.COUPLED,
        yaml_path="schemas/examples/zeek_dns_log.yaml",
        node_labels=["IP", "Domain", "ResolvedIP"],
        rel_types=["REQUESTED", "RESOLVED_TO"],
        node_props={
            "IP": ["ip_address"],
            "Domain": ["domain_name"],
            "ResolvedIP": ["resolved_ip"],
        },
        rel_props={
            "REQUESTED": ["timestamp", "query_type"],
            "RESOLVED_TO": ["ttl"],
        },
        shared_edge_table="dns_log",
    ),
}


@dataclass
class QueryTemplate:
    """Template for generating test queries"""
    category: QueryCategory
    template: str
    description: str
    placeholders: List[str]
    expected_to_work: Dict[SchemaType, bool]
    known_issues: Dict[SchemaType, str]


# Query templates with placeholders
QUERY_TEMPLATES = [
    # Basic Node Patterns
    QueryTemplate(
        category=QueryCategory.NODE_SCAN,
        template="MATCH (n:{label}) RETURN n.{prop} LIMIT 10",
        description="Basic node scan with single property",
        placeholders=["label", "prop"],
        expected_to_work={
            SchemaType.STANDARD: True,
            SchemaType.DENORMALIZED: True,
            SchemaType.POLYMORPHIC: True,
            SchemaType.COUPLED: True,
        },
        known_issues={},
    ),
    
    QueryTemplate(
        category=QueryCategory.RETURN_NODE,
        template="MATCH (n:{label}) RETURN n LIMIT 5",
        description="Return whole node (wildcard expansion)",
        placeholders=["label"],
        expected_to_work={
            SchemaType.STANDARD: True,
            SchemaType.DENORMALIZED: False,  # Known bug
            SchemaType.POLYMORPHIC: True,
            SchemaType.COUPLED: True,
        },
        known_issues={
            SchemaType.DENORMALIZED: "Bug: Wildcard expansion doesn't use from/to_node_properties",
        },
    ),
    
    QueryTemplate(
        category=QueryCategory.NODE_FILTER,
        template="MATCH (n:{label}) WHERE n.{prop} IS NOT NULL RETURN n.{prop} LIMIT 10",
        description="Node scan with IS NOT NULL filter",
        placeholders=["label", "prop"],
        expected_to_work={
            SchemaType.STANDARD: True,
            SchemaType.DENORMALIZED: True,
            SchemaType.POLYMORPHIC: True,
            SchemaType.COUPLED: True,
        },
        known_issues={},
    ),
    
    # Relationship Patterns
    QueryTemplate(
        category=QueryCategory.SINGLE_HOP,
        template="MATCH (a:{from_label})-[r:{rel_type}]->(b:{to_label}) RETURN a.{from_prop}, b.{to_prop} LIMIT 10",
        description="Single hop relationship",
        placeholders=["from_label", "to_label", "rel_type", "from_prop", "to_prop"],
        expected_to_work={
            SchemaType.STANDARD: True,
            SchemaType.DENORMALIZED: True,
            SchemaType.POLYMORPHIC: True,
            SchemaType.COUPLED: True,
        },
        known_issues={},
    ),
    
    QueryTemplate(
        category=QueryCategory.RETURN_REL,
        template="MATCH (a:{from_label})-[r:{rel_type}]->(b:{to_label}) RETURN r LIMIT 5",
        description="Return whole relationship",
        placeholders=["from_label", "to_label", "rel_type"],
        expected_to_work={
            SchemaType.STANDARD: True,
            SchemaType.DENORMALIZED: True,
            SchemaType.POLYMORPHIC: True,
            SchemaType.COUPLED: True,
        },
        known_issues={},
    ),
    
    QueryTemplate(
        category=QueryCategory.MULTI_HOP,
        template="MATCH (a:{label})-[r1:{rel1}]->(b:{label})-[r2:{rel2}]->(c:{label}) RETURN a.{prop}, c.{prop} LIMIT 5",
        description="Multi-hop traversal",
        placeholders=["label", "rel1", "rel2", "prop"],
        expected_to_work={
            SchemaType.STANDARD: True,
            SchemaType.DENORMALIZED: True,
            SchemaType.POLYMORPHIC: True,
            SchemaType.COUPLED: True,
        },
        known_issues={},
    ),
    
    # VLP Patterns
    QueryTemplate(
        category=QueryCategory.VLP_EXACT,
        template="MATCH (a:{label})-[*2]->(b:{label}) RETURN a.{prop}, b.{prop} LIMIT 10",
        description="Variable-length path with exact hops",
        placeholders=["label", "prop"],
        expected_to_work={
            SchemaType.STANDARD: True,
            SchemaType.DENORMALIZED: True,
            SchemaType.POLYMORPHIC: True,
            SchemaType.COUPLED: True,
        },
        known_issues={},
    ),
    
    QueryTemplate(
        category=QueryCategory.VLP_RANGE,
        template="MATCH (a:{label})-[*1..3]->(b:{label}) RETURN a.{prop}, b.{prop} LIMIT 10",
        description="Variable-length path with range",
        placeholders=["label", "prop"],
        expected_to_work={
            SchemaType.STANDARD: True,
            SchemaType.DENORMALIZED: True,
            SchemaType.POLYMORPHIC: True,
            SchemaType.COUPLED: True,
        },
        known_issues={},
    ),
    
    QueryTemplate(
        category=QueryCategory.VLP_PATH_VAR,
        template="MATCH p = (a:{label})-[*1..3]->(b:{label}) RETURN length(p), nodes(p) LIMIT 5",
        description="Path variable with functions",
        placeholders=["label"],
        expected_to_work={
            SchemaType.STANDARD: True,
            SchemaType.DENORMALIZED: True,
            SchemaType.POLYMORPHIC: True,
            SchemaType.COUPLED: True,
        },
        known_issues={},
    ),
    
    # Aggregation Patterns
    QueryTemplate(
        category=QueryCategory.SIMPLE_AGG,
        template="MATCH (n:{label}) RETURN count(n)",
        description="Simple count aggregation",
        placeholders=["label"],
        expected_to_work={
            SchemaType.STANDARD: True,
            SchemaType.DENORMALIZED: True,
            SchemaType.POLYMORPHIC: True,
            SchemaType.COUPLED: True,
        },
        known_issues={},
    ),
    
    QueryTemplate(
        category=QueryCategory.GROUP_BY,
        template="MATCH (n:{label}) RETURN n.{group_prop}, count(n) AS cnt ORDER BY cnt DESC LIMIT 10",
        description="GROUP BY with count",
        placeholders=["label", "group_prop"],
        expected_to_work={
            SchemaType.STANDARD: True,
            SchemaType.DENORMALIZED: True,
            SchemaType.POLYMORPHIC: True,
            SchemaType.COUPLED: True,
        },
        known_issues={},
    ),
    
    QueryTemplate(
        category=QueryCategory.WITH_AGG,
        template="MATCH (a:{from_label})-[r:{rel_type}]->(b:{to_label}) WITH a.{prop} AS prop, count(r) AS cnt RETURN prop, cnt ORDER BY cnt DESC LIMIT 10",
        description="WITH clause aggregation",
        placeholders=["from_label", "to_label", "rel_type", "prop"],
        expected_to_work={
            SchemaType.STANDARD: False,  # Known bug
            SchemaType.DENORMALIZED: False,  # Known bug
            SchemaType.POLYMORPHIC: False,  # Known bug
            SchemaType.COUPLED: False,  # Known bug
        },
        known_issues={
            SchemaType.STANDARD: "Bug: WITH aggregation generates incorrect SQL",
            SchemaType.DENORMALIZED: "Bug: WITH aggregation generates incorrect SQL",
            SchemaType.POLYMORPHIC: "Bug: WITH aggregation generates incorrect SQL",
            SchemaType.COUPLED: "Bug: WITH aggregation generates incorrect SQL",
        },
    ),
    
    # OPTIONAL MATCH
    QueryTemplate(
        category=QueryCategory.OPTIONAL_MATCH,
        template="MATCH (a:{label}) OPTIONAL MATCH (a)-[r:{rel_type}]->(b) RETURN a.{prop}, count(r) AS rel_count",
        description="OPTIONAL MATCH with aggregation",
        placeholders=["label", "rel_type", "prop"],
        expected_to_work={
            SchemaType.STANDARD: True,
            SchemaType.DENORMALIZED: True,
            SchemaType.POLYMORPHIC: True,
            SchemaType.COUPLED: True,
        },
        known_issues={},
    ),
    
    # Graph Functions
    QueryTemplate(
        category=QueryCategory.GRAPH_FUNCS,
        template="MATCH (a:{from_label})-[r:{rel_type}]->(b:{to_label}) RETURN type(r), id(a), labels(a) LIMIT 5",
        description="Graph functions (type, id, labels)",
        placeholders=["from_label", "to_label", "rel_type"],
        expected_to_work={
            SchemaType.STANDARD: True,
            SchemaType.DENORMALIZED: True,
            SchemaType.POLYMORPHIC: True,
            SchemaType.COUPLED: True,
        },
        known_issues={},
    ),
    
    # Multi-type relationships (polymorphic specific)
    QueryTemplate(
        category=QueryCategory.MULTI_TYPE,
        template="MATCH (a:{label})-[r:{rel1}|{rel2}]->(b:{label}) RETURN type(r), count(*) AS cnt",
        description="Multiple relationship types",
        placeholders=["label", "rel1", "rel2"],
        expected_to_work={
            SchemaType.STANDARD: True,
            SchemaType.DENORMALIZED: False,  # N/A - only one type
            SchemaType.POLYMORPHIC: True,
            SchemaType.COUPLED: True,
        },
        known_issues={
            SchemaType.DENORMALIZED: "N/A: Denormalized schema has single relationship type",
        },
    ),
    
    # Order and Limit
    QueryTemplate(
        category=QueryCategory.ORDER_LIMIT,
        template="MATCH (n:{label}) WHERE n.{prop} IS NOT NULL RETURN n.{prop} ORDER BY n.{prop} DESC LIMIT 10 SKIP 5",
        description="ORDER BY with LIMIT and SKIP",
        placeholders=["label", "prop"],
        expected_to_work={
            SchemaType.STANDARD: True,
            SchemaType.DENORMALIZED: True,
            SchemaType.POLYMORPHIC: True,
            SchemaType.COUPLED: True,
        },
        known_issues={},
    ),
    
    # Shortest Path
    QueryTemplate(
        category=QueryCategory.SHORTEST_PATH,
        template="MATCH p = shortestPath((a:{label})-[*1..5]->(b:{label})) WHERE a.{prop} <> b.{prop} RETURN length(p) LIMIT 5",
        description="Shortest path query",
        placeholders=["label", "prop"],
        expected_to_work={
            SchemaType.STANDARD: True,
            SchemaType.DENORMALIZED: True,
            SchemaType.POLYMORPHIC: True,
            SchemaType.COUPLED: True,
        },
        known_issues={},
    ),
]


def generate_query(template: QueryTemplate, schema: SchemaConfig) -> Tuple[str, bool, str]:
    """
    Generate a concrete query from a template and schema.
    
    Returns:
        Tuple of (query_string, expected_to_work, known_issue_or_empty)
    """
    # Get random values from schema
    label = random.choice(schema.node_labels)
    props = schema.node_props.get(label, ["id"])
    prop = random.choice(props) if props else "id"
    
    from_label = label
    to_label = random.choice(schema.node_labels)
    
    rel_type = random.choice(schema.rel_types) if schema.rel_types else "REL"
    rel1 = schema.rel_types[0] if len(schema.rel_types) >= 1 else "REL1"
    rel2 = schema.rel_types[1] if len(schema.rel_types) >= 2 else rel1
    
    from_props = schema.node_props.get(from_label, ["id"])
    to_props = schema.node_props.get(to_label, ["id"])
    from_prop = random.choice(from_props) if from_props else "id"
    to_prop = random.choice(to_props) if to_props else "id"
    group_prop = prop
    
    # Fill in placeholders
    query = template.template
    replacements = {
        "{label}": label,
        "{prop}": prop,
        "{from_label}": from_label,
        "{to_label}": to_label,
        "{rel_type}": rel_type,
        "{rel1}": rel1,
        "{rel2}": rel2,
        "{from_prop}": from_prop,
        "{to_prop}": to_prop,
        "{group_prop}": group_prop,
    }
    
    for placeholder, value in replacements.items():
        query = query.replace(placeholder, value)
    
    expected = template.expected_to_work.get(schema.schema_type, True)
    issue = template.known_issues.get(schema.schema_type, "")
    
    return query, expected, issue


def generate_test_matrix() -> List[Dict]:
    """
    Generate complete test matrix: all patterns × all schemas.
    
    Returns:
        List of test cases with metadata
    """
    test_cases = []
    
    for schema_type, schema in SCHEMAS.items():
        for template in QUERY_TEMPLATES:
            # Generate multiple variations with randomness
            for variation in range(3):  # 3 variations per pattern
                query, expected, issue = generate_query(template, schema)
                
                test_cases.append({
                    "id": f"{schema.name}_{template.category.value}_{variation}",
                    "schema_type": schema_type.value,
                    "schema_name": schema.name,
                    "yaml_path": schema.yaml_path,
                    "category": template.category.value,
                    "description": template.description,
                    "query": query,
                    "expected_to_work": expected,
                    "known_issue": issue,
                })
    
    return test_cases


def generate_pytest_file(test_cases: List[Dict]) -> str:
    """Generate pytest file content from test cases"""
    
    lines = [
        '"""',
        'Auto-generated Query Pattern × Schema Variation Tests',
        '',
        'Generated by: tests/integration/query_patterns/test_generator.py',
        '',
        'This file tests every query pattern against every schema type.',
        '"""',
        '',
        'import pytest',
        'import requests',
        'import os',
        '',
        '',
        'CLICKGRAPH_URL = os.environ.get("CLICKGRAPH_URL", "http://localhost:8080")',
        '',
        '',
        'def execute_query(query: str, schema_name: str = None) -> dict:',
        '    """Execute a Cypher query against ClickGraph"""',
        '    payload = {"query": query}',
        '    if schema_name:',
        '        payload["schema_name"] = schema_name',
        '    ',
        '    response = requests.post(',
        '        f"{CLICKGRAPH_URL}/query",',
        '        json=payload,',
        '        headers={"Content-Type": "application/json"},',
        '        timeout=30,',
        '    )',
        '    return response.json() if response.status_code == 200 else {"error": response.text}',
        '',
        '',
    ]
    
    # Group by schema type
    by_schema = {}
    for tc in test_cases:
        schema = tc["schema_type"]
        if schema not in by_schema:
            by_schema[schema] = []
        by_schema[schema].append(tc)
    
    # Generate test class for each schema
    for schema_type, cases in by_schema.items():
        lines.append(f'class Test{schema_type.title().replace("_", "")}Schema:')
        lines.append(f'    """Tests for {schema_type} schema type"""')
        lines.append('')
        lines.append(f'    SCHEMA_YAML = "{cases[0]["yaml_path"]}"')
        lines.append('')
        
        for tc in cases:
            test_name = f"test_{tc['category']}_{tc['id'].split('_')[-1]}"
            
            # Add xfail if known issue
            if not tc["expected_to_work"]:
                issue = tc["known_issue"].replace('"', '\\"')
                lines.append(f'    @pytest.mark.xfail(reason="{issue}")')
            
            lines.append(f'    def {test_name}(self):')
            lines.append(f'        """')
            lines.append(f'        {tc["description"]}')
            lines.append(f'        Schema: {tc["schema_name"]}')
            lines.append(f'        """')
            query_escaped = tc["query"].replace('"', '\\"')
            lines.append(f'        query = "{query_escaped}"')
            lines.append(f'        result = execute_query(query, "{tc["schema_name"]}")')
            lines.append(f'        assert "error" not in result, f"Query failed: {{result}}"')
            lines.append('')
        
        lines.append('')
    
    return '\n'.join(lines)


if __name__ == "__main__":
    import json
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--pytest":
        # Generate pytest file
        test_cases = generate_test_matrix()
        pytest_content = generate_pytest_file(test_cases)
        print(pytest_content)
    elif len(sys.argv) > 1 and sys.argv[1] == "--json":
        # Generate JSON for inspection
        test_cases = generate_test_matrix()
        print(json.dumps(test_cases, indent=2))
    else:
        # Summary
        test_cases = generate_test_matrix()
        
        print("=" * 60)
        print("Query Pattern × Schema Variation Test Matrix")
        print("=" * 60)
        print(f"\nTotal test cases: {len(test_cases)}")
        print(f"Query templates: {len(QUERY_TEMPLATES)}")
        print(f"Schema types: {len(SCHEMAS)}")
        print(f"Variations per pattern: 3")
        
        # Count expected failures
        expected_pass = sum(1 for tc in test_cases if tc["expected_to_work"])
        expected_fail = len(test_cases) - expected_pass
        
        print(f"\nExpected to pass: {expected_pass}")
        print(f"Expected to fail (known issues): {expected_fail}")
        
        # Show known issues summary
        print("\n" + "-" * 60)
        print("Known Issues by Schema:")
        print("-" * 60)
        
        issues_by_schema = {}
        for tc in test_cases:
            if tc["known_issue"]:
                schema = tc["schema_type"]
                if schema not in issues_by_schema:
                    issues_by_schema[schema] = set()
                issues_by_schema[schema].add(tc["known_issue"])
        
        for schema, issues in issues_by_schema.items():
            print(f"\n{schema}:")
            for issue in issues:
                print(f"  - {issue}")
        
        print("\n" + "=" * 60)
        print("Run with --pytest to generate test file")
        print("Run with --json to see full test cases")
        print("=" * 60)
