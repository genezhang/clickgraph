"""
Comprehensive Integration Test Framework for ClickGraph

This framework generates test cases from a matrix of:
- Schema types (traditional, denormalized, FK-edge, polymorphic, etc.)
- Query patterns (MATCH, WITH, aggregations, VLP, etc.)
- Expression variations (comparisons, functions, arithmetic)
- Edge cases and error conditions

Goal: 1000+ integration tests covering all supported features
"""

import pytest
import os
import sys
import requests
import random
import string
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum, auto

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

CLICKGRAPH_URL = os.environ.get("CLICKGRAPH_URL", "http://localhost:8080")


class SchemaType(Enum):
    """Schema architecture types we support"""
    TRADITIONAL = auto()      # Separate node + edge tables
    DENORMALIZED = auto()     # Nodes embedded in edge table (from/to_node_properties)
    FK_EDGE = auto()          # Self-referencing FK (filesystem, org chart)
    POLYMORPHIC = auto()      # Multiple node types on same edge
    MULTI_TABLE_LABEL = auto() # Same label in multiple tables (zeek_merged)
    EDGE_TO_EDGE = auto()     # No separate node table, edge-to-edge joins


@dataclass
class SchemaConfig:
    """Configuration for a test schema"""
    name: str
    schema_type: SchemaType
    yaml_path: str
    database: str
    node_labels: List[str]
    edge_types: List[str]
    # Sample properties per label/type for generating expressions
    node_properties: Dict[str, List[Tuple[str, str]]]  # label -> [(prop_name, type)]
    edge_properties: Dict[str, List[Tuple[str, str]]]  # type -> [(prop_name, type)]
    # For generating valid test queries
    sample_node_ids: Dict[str, List[Any]] = field(default_factory=dict)
    sample_values: Dict[str, List[Any]] = field(default_factory=dict)


# =============================================================================
# Schema Configurations
# =============================================================================

SCHEMAS: Dict[str, SchemaConfig] = {
    "social_benchmark": SchemaConfig(
        name="social_benchmark",
        schema_type=SchemaType.TRADITIONAL,
        yaml_path="benchmarks/social_network/schemas/social_benchmark.yaml",
        database="brahmand",
        node_labels=["User", "Post"],
        edge_types=["FOLLOWS", "AUTHORED", "LIKED"],
        node_properties={
            "User": [("user_id", "int"), ("name", "string"), ("email", "string"), 
                     ("country", "string"), ("city", "string"), ("is_active", "bool")],
            "Post": [("post_id", "int"), ("content", "string"), ("created_at", "datetime")],
        },
        edge_properties={
            "FOLLOWS": [("follow_date", "date")],
            "AUTHORED": [("authored_at", "datetime")],
            "LIKED": [("liked_at", "datetime")],
        },
        sample_node_ids={"User": [1, 2, 3, 100], "Post": [1, 2, 3]},
        sample_values={"country": ["US", "UK", "CA"], "city": ["NYC", "LA", "London"]},
    ),
    
    "ontime_flights": SchemaConfig(
        name="ontime_flights",
        schema_type=SchemaType.DENORMALIZED,
        yaml_path="schemas/examples/ontime_denormalized.yaml",
        database="test_integration",
        node_labels=["Airport"],
        edge_types=["FLIGHT"],
        node_properties={
            "Airport": [("id", "int"), ("code", "string"), ("city", "string"), ("state", "string")],
        },
        edge_properties={
            "FLIGHT": [("year", "int"), ("month", "int"), ("flight_date", "date"),
                       ("crs_arrival_time", "int"), ("crs_departure_time", "int"),
                       ("arrival_time", "int"), ("departure_time", "int"),
                       ("tail_num", "string")],
        },
        sample_node_ids={"Airport": [12892, 12953, 10397]},  # ATL, LAX, JFK
        sample_values={"year": [2021, 2022, 2023], "month": list(range(1, 13))},
    ),
    
    "zeek_merged": SchemaConfig(
        name="zeek_merged",
        schema_type=SchemaType.MULTI_TABLE_LABEL,
        yaml_path="schemas/examples/zeek_merged.yaml",
        database="zeek",
        node_labels=["IP", "Domain"],
        edge_types=["DNS_REQUESTED", "CONNECTED_TO"],
        node_properties={
            "IP": [("ip", "string"), ("port", "int")],
            "Domain": [("name", "string"), ("resolved_ips", "array")],
        },
        edge_properties={
            "DNS_REQUESTED": [("uid", "string"), ("timestamp", "datetime"), ("qtype", "string")],
            "CONNECTED_TO": [("uid", "string"), ("timestamp", "datetime"), ("protocol", "string")],
        },
        sample_values={"protocol": ["tcp", "udp"], "qtype": ["A", "AAAA", "MX"]},
    ),
    
    "filesystem": SchemaConfig(
        name="filesystem",
        schema_type=SchemaType.FK_EDGE,
        yaml_path="schemas/examples/filesystem.yaml",
        database="test_integration",  # Fixed: was "test", should be "test_integration"
        node_labels=["Object"],  # Fixed: was "FSObject", schema uses "Object"
        edge_types=["PARENT"],  # Fixed: was "PARENT_OF", schema uses "PARENT"
        node_properties={
            "Object": [("object_id", "int"), ("name", "string"), ("type", "string"), 
                       ("size", "int"), ("created_at", "datetime")],
        },
        edge_properties={
            "PARENT": [],  # FK-edge has no separate edge properties
        },
        sample_values={"type": ["file", "folder"], "size": [0, 100, 1000, 10000]},
    ),
    
    "group_membership": SchemaConfig(
        name="group_membership",
        schema_type=SchemaType.TRADITIONAL,  # Simple User->Group via junction, not polymorphic
        yaml_path="schemas/test/group_membership_simple.yaml",
        database="test_integration",
        node_labels=["User", "Group"],
        edge_types=["MEMBER_OF"],
        node_properties={
            "User": [("id", "int"), ("name", "string"), ("email", "string")],
            "Group": [("id", "int"), ("name", "string"), ("description", "string")],
        },
        edge_properties={
            "MEMBER_OF": [("joined_at", "datetime"), ("role", "string")],
        },
        sample_values={"role": ["admin", "member", "viewer"]},
    ),
}


# =============================================================================
# Query Pattern Definitions
# =============================================================================

class QueryPattern(Enum):
    """Query pattern categories"""
    # Basic patterns
    SIMPLE_NODE = "simple_node"
    SIMPLE_EDGE = "simple_edge"
    FILTERED_NODE = "filtered_node"
    FILTERED_EDGE = "filtered_edge"
    
    # Multi-hop patterns
    TWO_HOP = "two_hop"
    THREE_HOP = "three_hop"
    
    # Variable-length paths
    VLP_STAR = "vlp_star"
    VLP_EXACT = "vlp_exact"
    VLP_RANGE = "vlp_range"
    VLP_OPEN_END = "vlp_open_end"
    
    # Shortest path
    SHORTEST_PATH = "shortest_path"
    ALL_SHORTEST_PATHS = "all_shortest_paths"
    
    # Optional patterns
    OPTIONAL_MATCH = "optional_match"
    OPTIONAL_WITH_FILTER = "optional_with_filter"
    
    # WITH chaining
    WITH_SIMPLE = "with_simple"
    WITH_AGGREGATION = "with_aggregation"
    WITH_CROSS_TABLE = "with_cross_table"
    
    # Aggregations
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    SUM_AVG = "sum_avg"
    COLLECT = "collect"
    MIN_MAX = "min_max"
    
    # Grouping
    GROUP_BY = "group_by"
    GROUP_BY_HAVING = "group_by_having"
    
    # Ordering and pagination
    ORDER_BY = "order_by"
    ORDER_LIMIT = "order_limit"
    ORDER_LIMIT_SKIP = "order_limit_skip"
    
    # Multiple patterns
    COMMA_PATTERN = "comma_pattern"
    MULTI_REL_TYPE = "multi_rel_type"
    
    # Expressions
    ARITHMETIC = "arithmetic"
    STRING_PREDICATES = "string_predicates"
    NULL_HANDLING = "null_handling"
    IN_LIST = "in_list"
    CASE_EXPRESSION = "case_expression"
    REGEX = "regex"
    
    # Functions
    ID_FUNCTION = "id_function"
    TYPE_FUNCTION = "type_function"
    LABELS_FUNCTION = "labels_function"
    
    # Path variables
    PATH_VARIABLE = "path_variable"
    PATH_LENGTH = "path_length"
    PATH_NODES = "path_nodes"
    
    # Parameters
    PARAMETER_SIMPLE = "parameter_simple"
    PARAMETER_COMPLEX = "parameter_complex"
    
    # UNWIND
    UNWIND_SIMPLE = "unwind_simple"
    UNWIND_WITH_MATCH = "unwind_with_match"
    
    # Subqueries
    EXISTS_SUBQUERY = "exists_subquery"
    
    # Undirected
    UNDIRECTED_SIMPLE = "undirected_simple"
    UNDIRECTED_MULTI_HOP = "undirected_multi_hop"


# =============================================================================
# Expression Generators
# =============================================================================

class ExpressionGenerator:
    """Generates random valid expressions for testing"""
    
    COMPARISON_OPS = ["=", "<>", "<", ">", "<=", ">="]
    LOGICAL_OPS = ["AND", "OR"]
    ARITHMETIC_OPS = ["+", "-", "*", "/"]
    STRING_OPS = ["STARTS WITH", "ENDS WITH", "CONTAINS"]
    
    def __init__(self, schema: SchemaConfig, seed: int = None):
        self.schema = schema
        if seed is not None:
            random.seed(seed)
    
    def random_string(self, length: int = 8) -> str:
        return ''.join(random.choices(string.ascii_lowercase, k=length))
    
    def random_int(self, min_val: int = 0, max_val: int = 1000) -> int:
        return random.randint(min_val, max_val)
    
    def property_comparison(self, alias: str, prop_name: str, prop_type: str) -> str:
        """Generate a comparison expression for a property"""
        op = random.choice(self.COMPARISON_OPS)
        
        if prop_type == "int":
            value = self.random_int()
            return f"{alias}.{prop_name} {op} {value}"
        elif prop_type == "string":
            value = self.random_string()
            return f"{alias}.{prop_name} {op} '{value}'"
        elif prop_type == "bool":
            value = random.choice(["true", "false"])
            return f"{alias}.{prop_name} = {value}"
        elif prop_type == "date":
            return f"{alias}.{prop_name} {op} '2022-01-01'"
        elif prop_type == "datetime":
            return f"{alias}.{prop_name} {op} '2022-01-01 00:00:00'"
        else:
            return f"{alias}.{prop_name} IS NOT NULL"
    
    def arithmetic_expression(self, alias: str, prop_name: str) -> str:
        """Generate an arithmetic expression"""
        op = random.choice(self.ARITHMETIC_OPS)
        offset = self.random_int(1, 100)
        return f"{alias}.{prop_name} {op} {offset}"
    
    def cross_alias_comparison(self, alias1: str, prop1: str, alias2: str, prop2: str) -> str:
        """Generate a comparison between two aliases (for JOINs)"""
        op = random.choice(self.COMPARISON_OPS)
        return f"{alias1}.{prop1} {op} {alias2}.{prop2}"
    
    def cross_alias_arithmetic(self, alias1: str, prop1: str, alias2: str, prop2: str) -> str:
        """Generate arithmetic comparison across aliases (like ontime benchmark)"""
        offset = self.random_int(0, 200)
        return f"{alias1}.{prop1} + {offset} <= {alias2}.{prop2}"
    
    def string_predicate(self, alias: str, prop_name: str) -> str:
        """Generate a string predicate"""
        op = random.choice(self.STRING_OPS)
        value = self.random_string(4)
        return f"{alias}.{prop_name} {op} '{value}'"
    
    def in_list_expression(self, alias: str, prop_name: str, prop_type: str) -> str:
        """Generate an IN list expression"""
        if prop_type == "int":
            values = [self.random_int() for _ in range(random.randint(2, 5))]
            return f"{alias}.{prop_name} IN [{', '.join(map(str, values))}]"
        elif prop_type == "string":
            values = [f"'{self.random_string()}'" for _ in range(random.randint(2, 5))]
            return f"{alias}.{prop_name} IN [{', '.join(values)}]"
        else:
            return f"{alias}.{prop_name} IS NOT NULL"
    
    def null_check(self, alias: str, prop_name: str) -> str:
        """Generate a NULL check"""
        check = random.choice(["IS NULL", "IS NOT NULL"])
        return f"{alias}.{prop_name} {check}"
    
    def case_expression(self, alias: str, prop_name: str, prop_type: str) -> str:
        """Generate a CASE expression"""
        if prop_type == "int":
            return f"CASE WHEN {alias}.{prop_name} > 50 THEN 'high' ELSE 'low' END"
        elif prop_type == "string":
            return f"CASE WHEN {alias}.{prop_name} IS NOT NULL THEN {alias}.{prop_name} ELSE 'unknown' END"
        else:
            return f"CASE WHEN {alias}.{prop_name} IS NOT NULL THEN 1 ELSE 0 END"
    
    def random_where_clause(self, aliases_with_props: List[Tuple[str, str, str]], 
                            complexity: int = 1) -> str:
        """Generate a random WHERE clause with given complexity (1-3)"""
        conditions = []
        
        for _ in range(complexity):
            alias, prop, prop_type = random.choice(aliases_with_props)
            expr_type = random.choice(["comparison", "null", "arithmetic"])
            
            if expr_type == "comparison":
                conditions.append(self.property_comparison(alias, prop, prop_type))
            elif expr_type == "null":
                conditions.append(self.null_check(alias, prop))
            elif expr_type == "arithmetic" and prop_type == "int":
                conditions.append(f"{self.arithmetic_expression(alias, prop)} > 0")
            else:
                conditions.append(self.property_comparison(alias, prop, prop_type))
        
        if len(conditions) == 1:
            return conditions[0]
        else:
            op = random.choice(self.LOGICAL_OPS)
            return f" {op} ".join(conditions)


# =============================================================================
# Query Template Generator
# =============================================================================

class QueryGenerator:
    """Generates Cypher queries from patterns and schemas"""
    
    def __init__(self, schema: SchemaConfig):
        self.schema = schema
        self.expr_gen = ExpressionGenerator(schema)
    
    def _get_node_alias(self, idx: int = 0) -> str:
        return chr(ord('a') + idx)
    
    def _get_rel_alias(self, idx: int = 0) -> str:
        return f"r{idx + 1}" if idx > 0 else "r"
    
    def _get_label(self, idx: int = 0) -> str:
        return self.schema.node_labels[idx % len(self.schema.node_labels)]
    
    def _get_edge_type(self, idx: int = 0) -> str:
        return self.schema.edge_types[idx % len(self.schema.edge_types)]
    
    def _get_node_prop(self, label: str) -> Tuple[str, str]:
        props = self.schema.node_properties.get(label, [("id", "int")])
        return random.choice(props)
    
    def _get_id_prop(self, label: str) -> Tuple[str, str]:
        """Get an integer ID property for a label (safer for ID comparisons)"""
        props = self.schema.node_properties.get(label, [("id", "int")])
        int_props = [p for p in props if p[1] == "int"]
        return int_props[0] if int_props else props[0]
    
    def _get_edge_prop(self, edge_type: str) -> Optional[Tuple[str, str]]:
        props = self.schema.edge_properties.get(edge_type, [])
        return random.choice(props) if props else None
    
    # -------------------------------------------------------------------------
    # Basic Patterns
    # -------------------------------------------------------------------------
    
    def simple_node(self) -> str:
        # MULTI_TABLE_LABEL schemas (like zeek_merged) don't support standalone node queries
        # Nodes only exist in relationship tables, not as standalone entities
        if self.schema.schema_type == SchemaType.MULTI_TABLE_LABEL:
            pytest.skip(f"Standalone node queries not supported for MULTI_TABLE_LABEL schema type ({self.schema.name})")
        
        label = self._get_label()
        return f"MATCH (n:{label}) RETURN n LIMIT 10"
    
    def simple_edge(self) -> str:
        label = self._get_label()
        edge = self._get_edge_type()
        return f"MATCH (a:{label})-[r:{edge}]->(b) RETURN a, r, b LIMIT 10"
    
    def filtered_node(self) -> str:
        # MULTI_TABLE_LABEL schemas don't support standalone node queries
        if self.schema.schema_type == SchemaType.MULTI_TABLE_LABEL:
            pytest.skip(f"Standalone node queries not supported for MULTI_TABLE_LABEL schema type ({self.schema.name})")
        
        label = self._get_label()
        prop, prop_type = self._get_node_prop(label)
        filter_expr = self.expr_gen.property_comparison("n", prop, prop_type)
        return f"MATCH (n:{label}) WHERE {filter_expr} RETURN n LIMIT 10"
    
    def filtered_edge(self) -> str:
        label = self._get_label()
        edge = self._get_edge_type()
        prop, prop_type = self._get_node_prop(label)
        filter_expr = self.expr_gen.property_comparison("a", prop, prop_type)
        return f"MATCH (a:{label})-[r:{edge}]->(b) WHERE {filter_expr} RETURN a, b LIMIT 10"
    
    # -------------------------------------------------------------------------
    # Multi-hop Patterns
    # -------------------------------------------------------------------------
    
    def two_hop(self) -> str:
        label = self._get_label()
        edge = self._get_edge_type()
        return f"MATCH (a:{label})-[r1:{edge}]->(b)-[r2:{edge}]->(c) RETURN a, b, c LIMIT 10"
    
    def two_hop_with_cross_filter(self) -> str:
        """The pattern that was broken in ontime benchmark"""
        label = self._get_label()
        edge = self._get_edge_type()
        edge_prop = self._get_edge_prop(edge)
        
        if edge_prop:
            prop, _ = edge_prop
            # Cross-table inequality comparison
            filter_expr = f"r1.{prop} < r2.{prop}"
        else:
            filter_expr = "true"
        
        return f"MATCH (a:{label})-[r1:{edge}]->(b)-[r2:{edge}]->(c) WHERE {filter_expr} RETURN a, c LIMIT 10"
    
    def three_hop(self) -> str:
        label = self._get_label()
        edge = self._get_edge_type()
        return f"MATCH (a:{label})-[r1:{edge}]->(b)-[r2:{edge}]->(c)-[r3:{edge}]->(d) RETURN a, d LIMIT 10"
    
    # -------------------------------------------------------------------------
    # Variable-Length Paths
    # -------------------------------------------------------------------------
    
    def vlp_star(self) -> str:
        label = self._get_label()
        edge = self._get_edge_type()
        return f"MATCH (a:{label})-[r:{edge}*]->(b) RETURN a, b LIMIT 10"
    
    def vlp_exact(self, hops: int = None) -> str:
        label = self._get_label()
        edge = self._get_edge_type()
        hops = hops or random.randint(2, 4)
        return f"MATCH (a:{label})-[r:{edge}*{hops}]->(b) RETURN a, b LIMIT 10"
    
    def vlp_range(self, min_hops: int = None, max_hops: int = None) -> str:
        label = self._get_label()
        edge = self._get_edge_type()
        min_hops = min_hops or random.randint(1, 2)
        max_hops = max_hops or min_hops + random.randint(1, 3)
        return f"MATCH (a:{label})-[r:{edge}*{min_hops}..{max_hops}]->(b) RETURN a, b LIMIT 10"
    
    def vlp_open_end(self, min_hops: int = None) -> str:
        label = self._get_label()
        edge = self._get_edge_type()
        min_hops = min_hops or random.randint(1, 3)
        return f"MATCH (a:{label})-[r:{edge}*{min_hops}..]->(b) RETURN a, b LIMIT 10"
    
    # -------------------------------------------------------------------------
    # Shortest Path
    # -------------------------------------------------------------------------
    
    def shortest_path(self) -> str:
        label = self._get_label()
        edge = self._get_edge_type()
        prop, prop_type = self._get_id_prop(label)
        id_val = self.schema.sample_node_ids.get(label, [1])[0]
        return f"MATCH p = shortestPath((a:{label})-[:{edge}*]->(b:{label})) WHERE a.{prop} = {id_val} RETURN p LIMIT 10"
    
    def all_shortest_paths(self) -> str:
        label = self._get_label()
        edge = self._get_edge_type()
        prop, prop_type = self._get_id_prop(label)
        ids = self.schema.sample_node_ids.get(label, [1, 2])
        return f"MATCH p = allShortestPaths((a:{label})-[:{edge}*]->(b:{label})) WHERE a.{prop} = {ids[0]} AND b.{prop} = {ids[-1]} RETURN p LIMIT 10"
    
    # -------------------------------------------------------------------------
    # Optional Match
    # -------------------------------------------------------------------------
    
    def optional_match(self) -> str:
        label = self._get_label()
        edge = self._get_edge_type()
        return f"MATCH (a:{label}) OPTIONAL MATCH (a)-[r:{edge}]->(b) RETURN a, r, b LIMIT 10"
    
    def optional_with_filter(self) -> str:
        label = self._get_label()
        edge = self._get_edge_type()
        prop, prop_type = self._get_node_prop(label)
        filter_expr = self.expr_gen.property_comparison("a", prop, prop_type)
        return f"MATCH (a:{label}) WHERE {filter_expr} OPTIONAL MATCH (a)-[r:{edge}]->(b) RETURN a, count(b) as cnt LIMIT 10"
    
    # -------------------------------------------------------------------------
    # WITH Chaining
    # -------------------------------------------------------------------------
    
    def with_simple(self) -> str:
        label = self._get_label()
        edge = self._get_edge_type()
        return f"MATCH (a:{label})-[r:{edge}]->(b) WITH a, count(b) as cnt RETURN a, cnt ORDER BY cnt DESC LIMIT 10"
    
    def with_aggregation(self) -> str:
        label = self._get_label()
        edge = self._get_edge_type()
        return f"MATCH (a:{label})-[r:{edge}]->(b) WITH a, count(b) as cnt WHERE cnt > 1 RETURN a, cnt LIMIT 10"
    
    def with_cross_table(self) -> str:
        """Cross-table WITH pattern - the one we just fixed"""
        if len(self.schema.edge_types) < 2:
            return self.with_simple()
        
        label = self._get_label()
        edge1 = self.schema.edge_types[0]
        edge2 = self.schema.edge_types[1] if len(self.schema.edge_types) > 1 else edge1
        prop, _ = self._get_node_prop(label)
        
        return f"""MATCH (a:{label})-[r1:{edge1}]->(b) 
WITH a, b 
MATCH (c:{label})-[r2:{edge2}]->(d) 
WHERE a.{prop} = c.{prop} 
RETURN a, b, d LIMIT 10"""
    
    # -------------------------------------------------------------------------
    # Aggregations
    # -------------------------------------------------------------------------
    
    def count_simple(self) -> str:
        label = self._get_label()
        return f"MATCH (n:{label}) RETURN count(n) as cnt"
    
    def count_distinct(self) -> str:
        label = self._get_label()
        prop, _ = self._get_node_prop(label)
        return f"MATCH (n:{label}) RETURN count(DISTINCT n.{prop}) as unique_count"
    
    def sum_avg(self) -> str:
        label = self._get_label()
        props = self.schema.node_properties.get(label, [])
        int_props = [p for p in props if p[1] == "int"]
        if not int_props:
            return self.count_simple()
        prop, _ = random.choice(int_props)
        return f"MATCH (n:{label}) RETURN sum(n.{prop}) as total, avg(n.{prop}) as average"
    
    def collect_agg(self) -> str:
        label = self._get_label()
        prop, _ = self._get_node_prop(label)
        return f"MATCH (n:{label}) RETURN collect(n.{prop})[0..10] as items"
    
    def min_max(self) -> str:
        label = self._get_label()
        props = self.schema.node_properties.get(label, [])
        int_props = [p for p in props if p[1] == "int"]
        if not int_props:
            return self.count_simple()
        prop, _ = random.choice(int_props)
        return f"MATCH (n:{label}) RETURN min(n.{prop}) as minimum, max(n.{prop}) as maximum"
    
    # -------------------------------------------------------------------------
    # Group By
    # -------------------------------------------------------------------------
    
    def group_by(self) -> str:
        label = self._get_label()
        props = self.schema.node_properties.get(label, [])
        string_props = [p for p in props if p[1] == "string"]
        if not string_props:
            return self.count_simple()
        prop, _ = random.choice(string_props)
        return f"MATCH (n:{label}) RETURN n.{prop} as group_key, count(*) as cnt ORDER BY cnt DESC LIMIT 10"
    
    def group_by_having(self) -> str:
        label = self._get_label()
        props = self.schema.node_properties.get(label, [])
        string_props = [p for p in props if p[1] == "string"]
        if not string_props:
            return self.count_simple()
        prop, _ = random.choice(string_props)
        return f"MATCH (n:{label}) WITH n.{prop} as group_key, count(*) as cnt WHERE cnt > 1 RETURN group_key, cnt ORDER BY cnt DESC LIMIT 10"
    
    # -------------------------------------------------------------------------
    # Order, Limit, Skip
    # -------------------------------------------------------------------------
    
    def order_by(self) -> str:
        label = self._get_label()
        prop, _ = self._get_node_prop(label)
        direction = random.choice(["ASC", "DESC"])
        return f"MATCH (n:{label}) RETURN n ORDER BY n.{prop} {direction} LIMIT 10"
    
    def order_limit_skip(self) -> str:
        label = self._get_label()
        prop, _ = self._get_node_prop(label)
        skip_val = random.randint(0, 5)
        return f"MATCH (n:{label}) RETURN n ORDER BY n.{prop} SKIP {skip_val} LIMIT 10"
    
    # -------------------------------------------------------------------------
    # Multiple Patterns
    # -------------------------------------------------------------------------
    
    def multi_rel_type(self) -> str:
        if len(self.schema.edge_types) < 2:
            return self.simple_edge()
        label = self._get_label()
        types = "|".join(self.schema.edge_types[:3])  # Max 3 types
        return f"MATCH (a:{label})-[r:{types}]->(b) RETURN a, type(r), b LIMIT 10"
    
    # -------------------------------------------------------------------------
    # Expressions
    # -------------------------------------------------------------------------
    
    def arithmetic_in_where(self) -> str:
        label = self._get_label()
        edge = self._get_edge_type()
        edge_props = self.schema.edge_properties.get(edge, [])
        int_props = [p for p in edge_props if p[1] == "int"]
        
        if len(int_props) < 2:
            return self.two_hop()
        
        prop1, _ = int_props[0]
        prop2, _ = int_props[1] if len(int_props) > 1 else int_props[0]
        offset = random.randint(10, 100)
        
        return f"MATCH (a:{label})-[r1:{edge}]->(b)-[r2:{edge}]->(c) WHERE r1.{prop1} + {offset} <= r2.{prop2} RETURN a, c LIMIT 10"
    
    def string_predicates(self) -> str:
        label = self._get_label()
        props = self.schema.node_properties.get(label, [])
        string_props = [p for p in props if p[1] == "string"]
        if not string_props:
            return self.simple_node()
        prop, _ = random.choice(string_props)
        op = random.choice(["STARTS WITH", "ENDS WITH", "CONTAINS"])
        value = self.expr_gen.random_string(3)
        return f"MATCH (n:{label}) WHERE n.{prop} {op} '{value}' RETURN n LIMIT 10"
    
    def null_handling(self) -> str:
        label = self._get_label()
        prop, _ = self._get_node_prop(label)
        check = random.choice(["IS NULL", "IS NOT NULL"])
        return f"MATCH (n:{label}) WHERE n.{prop} {check} RETURN n LIMIT 10"
    
    def in_list(self) -> str:
        label = self._get_label()
        props = self.schema.node_properties.get(label, [])
        int_props = [p for p in props if p[1] == "int"]
        if not int_props:
            return self.simple_node()
        prop, _ = random.choice(int_props)
        values = [random.randint(1, 100) for _ in range(3)]
        return f"MATCH (n:{label}) WHERE n.{prop} IN [{', '.join(map(str, values))}] RETURN n LIMIT 10"
    
    def case_expression(self) -> str:
        label = self._get_label()
        props = self.schema.node_properties.get(label, [])
        int_props = [p for p in props if p[1] == "int"]
        if not int_props:
            return self.simple_node()
        prop, _ = random.choice(int_props)
        return f"MATCH (n:{label}) RETURN n.{prop}, CASE WHEN n.{prop} > 50 THEN 'high' ELSE 'low' END as category LIMIT 10"
    
    def regex_match(self) -> str:
        label = self._get_label()
        props = self.schema.node_properties.get(label, [])
        string_props = [p for p in props if p[1] == "string"]
        if not string_props:
            return self.simple_node()
        prop, _ = random.choice(string_props)
        return f"MATCH (n:{label}) WHERE n.{prop} =~ '.*test.*' RETURN n LIMIT 10"
    
    # -------------------------------------------------------------------------
    # Functions
    # -------------------------------------------------------------------------
    
    def id_function(self) -> str:
        label = self._get_label()
        ids = self.schema.sample_node_ids.get(label, [1])
        id_val = random.choice(ids)
        return f"MATCH (n:{label}) WHERE id(n) = {id_val} RETURN n"
    
    def type_function(self) -> str:
        label = self._get_label()
        edge = self._get_edge_type()
        return f"MATCH (a:{label})-[r:{edge}]->(b) RETURN type(r), count(*) as cnt LIMIT 10"
    
    def labels_function(self) -> str:
        label = self._get_label()
        return f"MATCH (n:{label}) RETURN labels(n), count(*) as cnt LIMIT 10"
    
    # -------------------------------------------------------------------------
    # Path Variables
    # -------------------------------------------------------------------------
    
    def path_variable(self) -> str:
        label = self._get_label()
        edge = self._get_edge_type()
        return f"MATCH p = (a:{label})-[r:{edge}*1..3]->(b) RETURN p LIMIT 10"
    
    def path_length(self) -> str:
        label = self._get_label()
        edge = self._get_edge_type()
        return f"MATCH p = (a:{label})-[r:{edge}*1..3]->(b) RETURN length(p) as path_len, count(*) as cnt"
    
    def path_nodes(self) -> str:
        label = self._get_label()
        edge = self._get_edge_type()
        return f"MATCH p = (a:{label})-[r:{edge}*1..2]->(b) RETURN nodes(p) LIMIT 10"
    
    # -------------------------------------------------------------------------
    # Parameters
    # -------------------------------------------------------------------------
    
    def parameter_simple(self) -> str:
        """Use an integer property for parameter testing to ensure type compatibility"""
        label = self._get_label()
        prop, _ = self._get_id_prop(label)  # Use ID prop which is int
        return f"MATCH (n:{label}) WHERE n.{prop} = $param1 RETURN n LIMIT 10"
    
    def parameter_complex(self) -> str:
        """Use integer property for IN and > operations to ensure type compatibility"""
        label = self._get_label()
        prop, _ = self._get_id_prop(label)  # Use ID prop which is int
        return f"MATCH (n:{label}) WHERE n.{prop} IN $param_list AND n.{prop} > $min_val RETURN n LIMIT 10"
    
    # -------------------------------------------------------------------------
    # UNWIND
    # -------------------------------------------------------------------------
    
    def unwind_simple(self) -> str:
        return "UNWIND [1, 2, 3, 4, 5] AS x RETURN x, x * 2 as doubled"
    
    def unwind_with_match(self) -> str:
        label = self._get_label()
        prop, _ = self._get_node_prop(label)
        ids = self.schema.sample_node_ids.get(label, [1, 2, 3])
        return f"UNWIND {ids} AS id MATCH (n:{label}) WHERE n.{prop} = id RETURN n"
    
    # -------------------------------------------------------------------------
    # EXISTS Subquery
    # -------------------------------------------------------------------------
    
    def exists_subquery(self) -> str:
        label = self._get_label()
        edge = self._get_edge_type()
        return f"MATCH (a:{label}) WHERE EXISTS {{ MATCH (a)-[:{edge}]->() }} RETURN a LIMIT 10"
    
    # -------------------------------------------------------------------------
    # Undirected
    # -------------------------------------------------------------------------
    
    def undirected_simple(self) -> str:
        label = self._get_label()
        edge = self._get_edge_type()
        return f"MATCH (a:{label})-[r:{edge}]-(b) RETURN a, b LIMIT 10"
    
    def undirected_multi_hop(self) -> str:
        label = self._get_label()
        edge = self._get_edge_type()
        return f"MATCH (a:{label})-[r1:{edge}]-(b)-[r2:{edge}]-(c) RETURN a, b, c LIMIT 10"


# =============================================================================
# Negative Test Cases (Invalid Queries)
# =============================================================================

class NegativeTestGenerator:
    """Generates invalid queries that should return errors"""
    
    @staticmethod
    def get_invalid_queries() -> List[Tuple[str, str]]:
        """Returns list of (query, expected_error_type) tuples"""
        return [
            # Syntax errors
            ("MATCH (n:User RETURN n", "syntax"),
            ("MATCH n:User) RETURN n", "syntax"),
            ("MATCH (n:User) RETURN", "syntax"),
            ("MATCH (n:User) WHERE RETURN n", "syntax"),
            ("MATCH (n:User) WHERE AND n.id = 1 RETURN n", "syntax"),
            ("MATCH (n:User) WHERE OR n.id = 1 RETURN n", "syntax"),
            
            # Missing labels/types (when required)
            ("MATCH ()-[r]->() RETURN r", "inference"),  # May work with inference or fail
            
            # Invalid property access
            ("MATCH (n:User) WHERE n. = 1 RETURN n", "syntax"),
            ("MATCH (n:User) WHERE .name = 'test' RETURN n", "syntax"),
            
            # Invalid operators
            ("MATCH (n:User) WHERE n.id === 1 RETURN n", "syntax"),
            ("MATCH (n:User) WHERE n.id <=> 1 RETURN n", "syntax"),
            
            # Invalid aggregation usage
            ("MATCH (n:User) WHERE count(n) > 1 RETURN n", "semantic"),  # count in WHERE
            
            # Invalid path patterns
            ("MATCH (a)-[*0]->(b) RETURN a, b", "syntax"),  # *0 is invalid
            ("MATCH (a)-[*-1..5]->(b) RETURN a, b", "syntax"),  # negative
            
            # Undefined aliases
            ("MATCH (a:User) RETURN b", "undefined"),
            ("MATCH (a:User) WHERE b.name = 'test' RETURN a", "undefined"),
            
            # Type mismatches (might be caught at execution)
            ("MATCH (n:User) WHERE n.user_id = 'not_an_int' RETURN n", "type"),
            
            # Invalid function usage
            ("MATCH (n:User) RETURN count()", "syntax"),  # count needs argument
            ("MATCH (n:User) RETURN unknownFunction(n)", "unknown_function"),
            
            # Invalid CASE syntax
            ("MATCH (n:User) RETURN CASE n.id END", "syntax"),
            ("MATCH (n:User) RETURN CASE WHEN THEN 1 END", "syntax"),
            
            # Duplicate aliases
            ("MATCH (a:User), (a:Post) RETURN a", "duplicate"),
            
            # Empty patterns
            ("MATCH () RETURN count(*)", "empty"),
            
            # Invalid relationship patterns
            ("MATCH (a:User)-->(b:User) RETURN a", "syntax"),  # missing []
            ("MATCH (a:User)-[r:]->(b:User) RETURN a", "syntax"),  # empty type after :
            
            # Invalid variable-length syntax
            ("MATCH (a)-[*1..0]->(b) RETURN a", "range"),  # min > max
            ("MATCH (a)-[*abc]->(b) RETURN a", "syntax"),  # non-numeric
            
            # Unclosed strings
            ("MATCH (n:User) WHERE n.name = 'unclosed RETURN n", "syntax"),
            
            # Invalid keywords
            ("MATCH (n:User) RETURNS n", "syntax"),  # RETURNS instead of RETURN
            ("METCH (n:User) RETURN n", "syntax"),  # METCH instead of MATCH
            
            # Missing required clauses
            ("MATCH (n:User)", "syntax"),  # no RETURN
            ("RETURN 1", "syntax"),  # no MATCH (might work as literal)
            
            # Invalid WITH usage
            ("MATCH (n:User) WITH RETURN n", "syntax"),
            ("WITH a MATCH (n:User) RETURN n", "undefined"),  # a not defined
            
            # Invalid LIMIT/SKIP
            ("MATCH (n:User) RETURN n LIMIT -1", "syntax"),
            ("MATCH (n:User) RETURN n SKIP -5", "syntax"),
            ("MATCH (n:User) RETURN n LIMIT 'ten'", "syntax"),
        ]


# =============================================================================
# Test Runner Helpers
# =============================================================================

def execute_query(query: str, params: Dict = None, execution_mode: str = "sql_only", schema_name: str = None) -> Dict:
    """Execute a query against ClickGraph and return result.
    
    Uses USE clause convention - auto-prepends USE clause if schema_name provided
    and query doesn't already have it.
    """
    # Auto-prepend USE clause if schema_name provided and not already in query
    if schema_name and not query.strip().upper().startswith("USE "):
        query = f"USE {schema_name} {query}"
    
    payload = {
        "query": query,
        "execution_mode": execution_mode,
    }
    if params:
        payload["parameters"] = params
    # Note: schema_name is now in the USE clause, not sent as parameter
    
    try:
        response = requests.post(
            f"{CLICKGRAPH_URL}/query",
            json=payload,
            timeout=30
        )
        return {
            "status_code": response.status_code,
            "body": response.json() if response.headers.get("content-type", "").startswith("application/json") else response.text,
            "success": response.status_code == 200 and "error" not in response.text.lower(),
        }
    except Exception as e:
        return {
            "status_code": 0,
            "body": str(e),
            "success": False,
            "exception": True,
        }


def check_server_health() -> bool:
    """Check if ClickGraph server is running"""
    try:
        response = requests.get(f"{CLICKGRAPH_URL}/health", timeout=5)
        return response.status_code == 200
    except:
        return False
