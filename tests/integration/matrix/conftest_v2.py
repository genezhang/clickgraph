"""
Comprehensive E2E Test Configuration - Version 2
Uses real schemas and tables that exist in the test database.

Schemas:
1. social_integration - Traditional graph (users_bench, user_follows_bench, posts_bench)
2. group_membership - Polymorphic edges (groups, users, memberships)
3. data_security - Full polymorphic (sec_users, sec_groups, sec_fs_objects, etc.)

Target: 1000+ real E2E tests
"""

import pytest
import requests
import os
import random
import string
from dataclasses import dataclass
from typing import Dict, List, Optional, Any

# ============================================================================
# SCHEMA CONFIGURATIONS - Using REAL schemas that exist in the codebase
# ============================================================================

SCHEMAS = {
    # Schema 1: Social Benchmark (Traditional graph) - PRIMARY schema for testing
    "social_integration": {
        "schema_file": "./benchmarks/social_network/schemas/social_integration.yaml",
        "nodes": {
            "User": {
                "id_prop": "user_id",
                "properties": ["user_id", "name", "email", "registration_date", "is_active", "country", "city"],
                "string_props": ["name", "email", "country", "city"],
                "numeric_props": ["user_id", "is_active"],
                "date_props": ["registration_date"],
            },
            "Post": {
                "id_prop": "post_id",
                "properties": ["post_id", "content", "date"],
                "string_props": ["content"],
                "numeric_props": ["post_id"],
                "date_props": ["date"],
            }
        },
        "relationships": {
            "FOLLOWS": {
                "from_label": "User",
                "to_label": "User",
                "properties": ["follow_id", "follow_date"],
                "id_prop": "follow_id",
            },
            "LIKED": {
                "from_label": "User",
                "to_label": "Post",
                "properties": ["like_date"],
            }
        }
    },
    
    # Schema 2: Security Graph - Complex model with 4 node types
    # Note: Polymorphic edges don't work yet, but node queries do
    "data_security": {
        "schema_file": "./examples/data_security/data_security.yaml",
        "nodes": {
            "User": {
                "id_prop": "user_id",
                "properties": ["user_id", "name", "email", "exposure"],
                "string_props": ["name", "email", "exposure"],
                "numeric_props": ["user_id"],
                "date_props": [],
            },
            "Group": {
                "id_prop": "group_id",
                "properties": ["group_id", "name", "description"],
                "string_props": ["name", "description"],
                "numeric_props": ["group_id"],
                "date_props": [],
            },
            "Folder": {
                "id_prop": "fs_id",
                "properties": ["fs_id", "name", "path"],
                "string_props": ["name", "path"],
                "numeric_props": ["fs_id"],
                "date_props": [],
            },
            "File": {
                "id_prop": "fs_id",
                "properties": ["fs_id", "name", "path", "sensitive_data"],
                "string_props": ["name", "path"],
                "numeric_props": ["fs_id", "sensitive_data"],
                "date_props": [],
            }
        },
        "relationships": {}  # Polymorphic edges not supported yet
    },
}


# ============================================================================
# TEST INFRASTRUCTURE
# ============================================================================

SERVER_URL = os.environ.get("CLICKGRAPH_URL", "http://localhost:8080")

def execute_query(query: str, params: Optional[Dict] = None, schema_name: Optional[str] = None) -> Dict:
    """Execute a Cypher query and return the result."""
    payload = {"query": query}
    if params:
        payload["params"] = params
    if schema_name:
        payload["schema_name"] = schema_name
    
    try:
        response = requests.post(f"{SERVER_URL}/query", json=payload, timeout=30)
        body = response.json() if response.text else {}
        return {
            "success": response.status_code == 200 and "error" not in body,
            "status_code": response.status_code,
            "body": body
        }
    except Exception as e:
        return {"success": False, "status_code": 0, "body": {"error": str(e)}}


def execute_sql_only(query: str, schema_name: Optional[str] = None) -> Dict:
    """Get the generated SQL without executing."""
    payload = {"query": query, "sql_only": True}
    if schema_name:
        payload["schema_name"] = schema_name
    
    try:
        response = requests.post(f"{SERVER_URL}/query", json=payload, timeout=30)
        body = response.json() if response.text else {}
        return {
            "success": response.status_code == 200 and "generated_sql" in body,
            "status_code": response.status_code,
            "body": body,
            "sql": body.get("generated_sql", "")
        }
    except Exception as e:
        return {"success": False, "status_code": 0, "body": {"error": str(e)}, "sql": ""}


# ============================================================================
# QUERY GENERATORS
# ============================================================================

class QueryGenerator:
    """Generates test queries for a specific schema."""
    
    def __init__(self, schema_name: str, schema_config: dict):
        self.schema_name = schema_name
        self.config = schema_config
        self.nodes = schema_config["nodes"]
        self.relationships = schema_config.get("relationships", {})
    
    def _get_primary_node(self) -> tuple:
        """Get the first node label and its config."""
        label = list(self.nodes.keys())[0]
        return label, self.nodes[label]
    
    def _get_secondary_node(self) -> Optional[tuple]:
        """Get the second node label if exists."""
        labels = list(self.nodes.keys())
        if len(labels) > 1:
            return labels[1], self.nodes[labels[1]]
        return None
    
    def _get_relationship(self) -> Optional[tuple]:
        """Get first relationship type and config."""
        if self.relationships:
            rel_type = list(self.relationships.keys())[0]
            return rel_type, self.relationships[rel_type]
        return None
    
    def _random_string(self, length=8) -> str:
        return ''.join(random.choices(string.ascii_lowercase, k=length))
    
    def _random_int(self, min_val=1, max_val=1000) -> int:
        return random.randint(min_val, max_val)
    
    # -------------------------------------------------------------------------
    # Basic Node Queries (15+ variations per schema)
    # -------------------------------------------------------------------------
    
    def simple_node_return_all(self) -> str:
        label, _ = self._get_primary_node()
        return f"MATCH (n:{label}) RETURN n LIMIT 10"
    
    def simple_node_return_id(self) -> str:
        label, config = self._get_primary_node()
        id_prop = config["id_prop"]
        return f"MATCH (n:{label}) RETURN n.{id_prop} LIMIT 10"
    
    def simple_node_return_properties(self) -> str:
        label, config = self._get_primary_node()
        props = config["properties"][:3]  # First 3 properties
        prop_list = ", ".join([f"n.{p}" for p in props])
        return f"MATCH (n:{label}) RETURN {prop_list} LIMIT 10"
    
    def node_count(self) -> str:
        label, _ = self._get_primary_node()
        return f"MATCH (n:{label}) RETURN count(n) as cnt"
    
    def node_count_distinct(self) -> str:
        label, config = self._get_primary_node()
        id_prop = config["id_prop"]
        return f"MATCH (n:{label}) RETURN count(DISTINCT n.{id_prop}) as unique_count"
    
    def node_where_id_equals(self) -> str:
        label, config = self._get_primary_node()
        id_prop = config["id_prop"]
        return f"MATCH (n:{label}) WHERE n.{id_prop} = {self._random_int(1, 100)} RETURN n LIMIT 10"
    
    def node_where_id_greater(self) -> str:
        label, config = self._get_primary_node()
        id_prop = config["id_prop"]
        return f"MATCH (n:{label}) WHERE n.{id_prop} > {self._random_int(1, 50)} RETURN n LIMIT 10"
    
    def node_where_id_less(self) -> str:
        label, config = self._get_primary_node()
        id_prop = config["id_prop"]
        return f"MATCH (n:{label}) WHERE n.{id_prop} < {self._random_int(50, 200)} RETURN n LIMIT 10"
    
    def node_where_id_between(self) -> str:
        label, config = self._get_primary_node()
        id_prop = config["id_prop"]
        low, high = self._random_int(1, 50), self._random_int(100, 200)
        return f"MATCH (n:{label}) WHERE n.{id_prop} >= {low} AND n.{id_prop} <= {high} RETURN n LIMIT 10"
    
    def node_where_string_equals(self) -> str:
        label, config = self._get_primary_node()
        string_props = config.get("string_props", [])
        if string_props:
            prop = string_props[0]
            return f"MATCH (n:{label}) WHERE n.{prop} = '{self._random_string()}' RETURN n LIMIT 10"
        return self.simple_node_return_all()
    
    def node_where_string_contains(self) -> str:
        label, config = self._get_primary_node()
        string_props = config.get("string_props", [])
        if string_props:
            prop = string_props[0]
            return f"MATCH (n:{label}) WHERE n.{prop} CONTAINS '{self._random_string(3)}' RETURN n LIMIT 10"
        return self.simple_node_return_all()
    
    def node_where_string_starts_with(self) -> str:
        label, config = self._get_primary_node()
        string_props = config.get("string_props", [])
        if string_props:
            prop = string_props[0]
            return f"MATCH (n:{label}) WHERE n.{prop} STARTS WITH '{self._random_string(2)}' RETURN n LIMIT 10"
        return self.simple_node_return_all()
    
    def node_where_is_null(self) -> str:
        label, config = self._get_primary_node()
        props = config["properties"]
        return f"MATCH (n:{label}) WHERE n.{props[0]} IS NULL RETURN n LIMIT 10"
    
    def node_where_is_not_null(self) -> str:
        label, config = self._get_primary_node()
        props = config["properties"]
        return f"MATCH (n:{label}) WHERE n.{props[0]} IS NOT NULL RETURN n LIMIT 10"
    
    def node_where_in_list(self) -> str:
        label, config = self._get_primary_node()
        id_prop = config["id_prop"]
        vals = [self._random_int(1, 100) for _ in range(5)]
        return f"MATCH (n:{label}) WHERE n.{id_prop} IN [{', '.join(map(str, vals))}] RETURN n LIMIT 10"
    
    def node_order_by_asc(self) -> str:
        label, config = self._get_primary_node()
        id_prop = config["id_prop"]
        return f"MATCH (n:{label}) RETURN n ORDER BY n.{id_prop} ASC LIMIT 10"
    
    def node_order_by_desc(self) -> str:
        label, config = self._get_primary_node()
        id_prop = config["id_prop"]
        return f"MATCH (n:{label}) RETURN n ORDER BY n.{id_prop} DESC LIMIT 10"
    
    def node_distinct(self) -> str:
        label, config = self._get_primary_node()
        string_props = config.get("string_props", config["properties"][:1])
        if string_props:
            return f"MATCH (n:{label}) RETURN DISTINCT n.{string_props[0]} LIMIT 10"
        return self.simple_node_return_all()
    
    def node_skip_limit(self) -> str:
        label, _ = self._get_primary_node()
        return f"MATCH (n:{label}) RETURN n SKIP 5 LIMIT 10"
    
    def node_with_alias(self) -> str:
        label, config = self._get_primary_node()
        id_prop = config["id_prop"]
        return f"MATCH (n:{label}) RETURN n.{id_prop} AS id, n LIMIT 10"
    
    # -------------------------------------------------------------------------
    # Multi-Node Queries (Secondary label)
    # -------------------------------------------------------------------------
    
    def secondary_node_return(self) -> str:
        secondary = self._get_secondary_node()
        if secondary:
            label, _ = secondary
            return f"MATCH (n:{label}) RETURN n LIMIT 10"
        return self.simple_node_return_all()
    
    def secondary_node_count(self) -> str:
        secondary = self._get_secondary_node()
        if secondary:
            label, _ = secondary
            return f"MATCH (n:{label}) RETURN count(n) as cnt"
        return self.node_count()
    
    def secondary_node_filtered(self) -> str:
        secondary = self._get_secondary_node()
        if secondary:
            label, config = secondary
            id_prop = config["id_prop"]
            return f"MATCH (n:{label}) WHERE n.{id_prop} > {self._random_int(1, 50)} RETURN n LIMIT 10"
        return self.node_where_id_greater()
    
    # -------------------------------------------------------------------------
    # Relationship Queries (20+ variations)
    # -------------------------------------------------------------------------
    
    def simple_relationship(self) -> str:
        rel = self._get_relationship()
        if rel:
            rel_type, config = rel
            from_label = config["from_label"]
            to_label = config["to_label"]
            return f"MATCH (a:{from_label})-[r:{rel_type}]->(b:{to_label}) RETURN a, b LIMIT 10"
        return self.simple_node_return_all()
    
    def relationship_return_rel(self) -> str:
        rel = self._get_relationship()
        if rel:
            rel_type, config = rel
            from_label = config["from_label"]
            to_label = config["to_label"]
            return f"MATCH (a:{from_label})-[r:{rel_type}]->(b:{to_label}) RETURN a, r, b LIMIT 10"
        return self.simple_node_return_all()
    
    def relationship_count(self) -> str:
        rel = self._get_relationship()
        if rel:
            rel_type, config = rel
            from_label = config["from_label"]
            to_label = config["to_label"]
            return f"MATCH (a:{from_label})-[r:{rel_type}]->(b:{to_label}) RETURN count(*) as cnt"
        return self.node_count()
    
    def relationship_with_node_filter(self) -> str:
        rel = self._get_relationship()
        if rel:
            rel_type, config = rel
            from_label = config["from_label"]
            to_label = config["to_label"]
            from_config = self.nodes.get(from_label, {})
            id_prop = from_config.get("id_prop", "id")
            return f"MATCH (a:{from_label})-[r:{rel_type}]->(b:{to_label}) WHERE a.{id_prop} < 100 RETURN a, b LIMIT 10"
        return self.simple_node_return_all()
    
    def relationship_reverse_direction(self) -> str:
        rel = self._get_relationship()
        if rel:
            rel_type, config = rel
            from_label = config["from_label"]
            to_label = config["to_label"]
            return f"MATCH (a:{to_label})<-[r:{rel_type}]-(b:{from_label}) RETURN a, b LIMIT 10"
        return self.simple_node_return_all()
    
    def relationship_any_direction(self) -> str:
        rel = self._get_relationship()
        if rel:
            rel_type, config = rel
            from_label = config["from_label"]
            to_label = config.get("to_label", from_label)
            if from_label == to_label:  # Self-referencing like FOLLOWS
                return f"MATCH (a:{from_label})-[r:{rel_type}]-(b) RETURN a, b LIMIT 10"
        return self.simple_node_return_all()
    
    def relationship_aggregation(self) -> str:
        rel = self._get_relationship()
        if rel:
            rel_type, config = rel
            from_label = config["from_label"]
            to_label = config["to_label"]
            from_config = self.nodes.get(from_label, {})
            id_prop = from_config.get("id_prop", "id")
            return f"MATCH (a:{from_label})-[r:{rel_type}]->(b:{to_label}) RETURN a.{id_prop}, count(b) as cnt ORDER BY cnt DESC LIMIT 10"
        return self.node_count()
    
    # -------------------------------------------------------------------------
    # Variable Length Paths
    # -------------------------------------------------------------------------
    
    def vlp_exact_2(self) -> str:
        rel = self._get_relationship()
        if rel:
            rel_type, config = rel
            from_label = config["from_label"]
            to_label = config.get("to_label", from_label)
            return f"MATCH (a:{from_label})-[:{rel_type}*2]->(b:{to_label}) RETURN a, b LIMIT 10"
        return self.simple_node_return_all()
    
    def vlp_range_1_3(self) -> str:
        rel = self._get_relationship()
        if rel:
            rel_type, config = rel
            from_label = config["from_label"]
            to_label = config.get("to_label", from_label)
            return f"MATCH (a:{from_label})-[:{rel_type}*1..3]->(b:{to_label}) RETURN a, b LIMIT 10"
        return self.simple_node_return_all()
    
    def vlp_unbounded(self) -> str:
        rel = self._get_relationship()
        if rel:
            rel_type, config = rel
            from_label = config["from_label"]
            to_label = config.get("to_label", from_label)
            return f"MATCH (a:{from_label})-[:{rel_type}*]->(b:{to_label}) RETURN a, b LIMIT 10"
        return self.simple_node_return_all()
    
    def vlp_with_path_variable(self) -> str:
        rel = self._get_relationship()
        if rel:
            rel_type, config = rel
            from_label = config["from_label"]
            to_label = config.get("to_label", from_label)
            return f"MATCH p = (a:{from_label})-[:{rel_type}*1..2]->(b:{to_label}) RETURN p LIMIT 10"
        return self.simple_node_return_all()
    
    def vlp_with_node_filter(self) -> str:
        rel = self._get_relationship()
        if rel:
            rel_type, config = rel
            from_label = config["from_label"]
            to_label = config.get("to_label", from_label)
            from_config = self.nodes.get(from_label, {})
            id_prop = from_config.get("id_prop", "id")
            return f"MATCH (a:{from_label})-[:{rel_type}*1..2]->(b:{to_label}) WHERE a.{id_prop} < 50 RETURN a, b LIMIT 10"
        return self.simple_node_return_all()
    
    # -------------------------------------------------------------------------
    # OPTIONAL MATCH
    # -------------------------------------------------------------------------
    
    def optional_match_basic(self) -> str:
        label, config = self._get_primary_node()
        rel = self._get_relationship()
        if rel:
            rel_type, rel_config = rel
            to_label = rel_config.get("to_label", label)
            return f"MATCH (a:{label}) OPTIONAL MATCH (a)-[r:{rel_type}]->(b:{to_label}) RETURN a, b LIMIT 10"
        return self.simple_node_return_all()
    
    def optional_match_with_filter(self) -> str:
        label, config = self._get_primary_node()
        id_prop = config["id_prop"]
        rel = self._get_relationship()
        if rel:
            rel_type, rel_config = rel
            to_label = rel_config.get("to_label", label)
            return f"MATCH (a:{label}) WHERE a.{id_prop} < 100 OPTIONAL MATCH (a)-[r:{rel_type}]->(b:{to_label}) RETURN a, b LIMIT 10"
        return self.simple_node_return_all()
    
    # -------------------------------------------------------------------------
    # Aggregations
    # -------------------------------------------------------------------------
    
    def agg_count_group_by(self) -> str:
        label, config = self._get_primary_node()
        string_props = config.get("string_props", [])
        if string_props:
            return f"MATCH (n:{label}) RETURN n.{string_props[0]}, count(*) as cnt ORDER BY cnt DESC LIMIT 10"
        return self.node_count()
    
    def agg_sum(self) -> str:
        label, config = self._get_primary_node()
        numeric_props = config.get("numeric_props", [])
        if numeric_props:
            return f"MATCH (n:{label}) RETURN sum(n.{numeric_props[0]}) as total"
        return self.node_count()
    
    def agg_avg(self) -> str:
        label, config = self._get_primary_node()
        numeric_props = config.get("numeric_props", [])
        if numeric_props:
            return f"MATCH (n:{label}) RETURN avg(n.{numeric_props[0]}) as average"
        return self.node_count()
    
    def agg_min_max(self) -> str:
        label, config = self._get_primary_node()
        numeric_props = config.get("numeric_props", [])
        if numeric_props:
            return f"MATCH (n:{label}) RETURN min(n.{numeric_props[0]}) as min_val, max(n.{numeric_props[0]}) as max_val"
        return self.node_count()
    
    def agg_collect(self) -> str:
        label, config = self._get_primary_node()
        id_prop = config["id_prop"]
        return f"MATCH (n:{label}) WHERE n.{id_prop} < 10 RETURN collect(n.{id_prop}) as ids"
    
    # -------------------------------------------------------------------------
    # Functions
    # -------------------------------------------------------------------------
    
    def func_coalesce(self) -> str:
        label, config = self._get_primary_node()
        props = config["properties"]
        if len(props) >= 2:
            return f"MATCH (n:{label}) RETURN coalesce(n.{props[0]}, n.{props[1]}) LIMIT 10"
        return self.simple_node_return_all()
    
    def func_tostring(self) -> str:
        label, config = self._get_primary_node()
        id_prop = config["id_prop"]
        return f"MATCH (n:{label}) RETURN toString(n.{id_prop}) as str_id LIMIT 10"
    
    def func_tointeger(self) -> str:
        label, config = self._get_primary_node()
        string_props = config.get("string_props", [])
        if string_props:
            return f"MATCH (n:{label}) RETURN n.{string_props[0]}, toInteger('123') as num LIMIT 10"
        return self.simple_node_return_all()
    
    def func_size(self) -> str:
        label, config = self._get_primary_node()
        string_props = config.get("string_props", [])
        if string_props:
            return f"MATCH (n:{label}) RETURN n.{string_props[0]}, size(n.{string_props[0]}) as len LIMIT 10"
        return self.simple_node_return_all()
    
    # -------------------------------------------------------------------------
    # Complex WHERE clauses
    # -------------------------------------------------------------------------
    
    def where_and(self) -> str:
        label, config = self._get_primary_node()
        id_prop = config["id_prop"]
        return f"MATCH (n:{label}) WHERE n.{id_prop} > 10 AND n.{id_prop} < 100 RETURN n LIMIT 10"
    
    def where_or(self) -> str:
        label, config = self._get_primary_node()
        id_prop = config["id_prop"]
        return f"MATCH (n:{label}) WHERE n.{id_prop} < 10 OR n.{id_prop} > 900 RETURN n LIMIT 10"
    
    def where_not(self) -> str:
        label, config = self._get_primary_node()
        id_prop = config["id_prop"]
        return f"MATCH (n:{label}) WHERE NOT n.{id_prop} > 100 RETURN n LIMIT 10"
    
    def where_complex(self) -> str:
        label, config = self._get_primary_node()
        id_prop = config["id_prop"]
        return f"MATCH (n:{label}) WHERE (n.{id_prop} > 10 AND n.{id_prop} < 50) OR n.{id_prop} > 900 RETURN n LIMIT 10"
    
    # -------------------------------------------------------------------------
    # Arithmetic expressions
    # -------------------------------------------------------------------------
    
    def expr_add(self) -> str:
        label, config = self._get_primary_node()
        numeric_props = config.get("numeric_props", [])
        if numeric_props:
            return f"MATCH (n:{label}) RETURN n.{numeric_props[0]} + 100 as added LIMIT 10"
        return self.simple_node_return_all()
    
    def expr_subtract(self) -> str:
        label, config = self._get_primary_node()
        numeric_props = config.get("numeric_props", [])
        if numeric_props:
            return f"MATCH (n:{label}) RETURN n.{numeric_props[0]} - 10 as subtracted LIMIT 10"
        return self.simple_node_return_all()
    
    def expr_multiply(self) -> str:
        label, config = self._get_primary_node()
        numeric_props = config.get("numeric_props", [])
        if numeric_props:
            return f"MATCH (n:{label}) RETURN n.{numeric_props[0]} * 2 as doubled LIMIT 10"
        return self.simple_node_return_all()
    
    def expr_divide(self) -> str:
        label, config = self._get_primary_node()
        numeric_props = config.get("numeric_props", [])
        if numeric_props:
            return f"MATCH (n:{label}) RETURN n.{numeric_props[0]} / 10 as divided LIMIT 10"
        return self.simple_node_return_all()
    
    # -------------------------------------------------------------------------
    # Multi-hop patterns
    # -------------------------------------------------------------------------
    
    def two_hop(self) -> str:
        rel = self._get_relationship()
        if rel:
            rel_type, config = rel
            from_label = config["from_label"]
            to_label = config.get("to_label", from_label)
            return f"MATCH (a:{from_label})-[:{rel_type}]->(b:{to_label})-[:{rel_type}]->(c:{to_label}) RETURN a, c LIMIT 10"
        return self.simple_node_return_all()
    
    def three_hop(self) -> str:
        rel = self._get_relationship()
        if rel:
            rel_type, config = rel
            from_label = config["from_label"]
            to_label = config.get("to_label", from_label)
            return f"MATCH (a:{from_label})-[:{rel_type}]->(b)-[:{rel_type}]->(c)-[:{rel_type}]->(d:{to_label}) RETURN a, d LIMIT 10"
        return self.simple_node_return_all()
    
    # -------------------------------------------------------------------------
    # EXISTS subquery
    # -------------------------------------------------------------------------
    
    def exists_subquery(self) -> str:
        label, _ = self._get_primary_node()
        rel = self._get_relationship()
        if rel:
            rel_type, _ = rel
            return f"MATCH (a:{label}) WHERE EXISTS {{ MATCH (a)-[:{rel_type}]->() }} RETURN a LIMIT 10"
        return self.simple_node_return_all()
    
    # -------------------------------------------------------------------------
    # UNWIND
    # -------------------------------------------------------------------------
    
    def unwind_simple(self) -> str:
        return "UNWIND [1, 2, 3, 4, 5] AS x RETURN x"
    
    def unwind_with_expression(self) -> str:
        return "UNWIND [1, 2, 3, 4, 5] AS x RETURN x, x * 2 as doubled"
    
    # -------------------------------------------------------------------------
    # Parameters
    # -------------------------------------------------------------------------
    
    def param_simple(self) -> str:
        label, config = self._get_primary_node()
        id_prop = config["id_prop"]
        return f"MATCH (n:{label}) WHERE n.{id_prop} = $param1 RETURN n LIMIT 10"
    
    def param_in_list(self) -> str:
        label, config = self._get_primary_node()
        id_prop = config["id_prop"]
        return f"MATCH (n:{label}) WHERE n.{id_prop} IN $param_list RETURN n LIMIT 10"


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture(scope="session")
def server_running():
    """Check that the server is running."""
    try:
        response = requests.get(f"{SERVER_URL}/health", timeout=5)
        if response.status_code != 200:
            pytest.skip(f"Server not healthy: {response.status_code}")
    except Exception as e:
        pytest.skip(f"Server not available: {e}")
    return True


@pytest.fixture(params=list(SCHEMAS.keys()))
def schema_config(request):
    """Parametrized fixture for each schema."""
    return request.param, SCHEMAS[request.param]


@pytest.fixture
def query_generator(schema_config):
    """Create a query generator for the current schema."""
    schema_name, config = schema_config
    return QueryGenerator(schema_name, config)


@pytest.fixture
def schema_name(schema_config):
    """Get just the schema name."""
    return schema_config[0]


@pytest.fixture
def schema_file(schema_config):
    """Get the schema file path."""
    return schema_config[1]["schema_file"]


# ============================================================================
# QUERY PATTERN LIST - All methods to test
# ============================================================================

# List of all query generator methods to test
BASIC_NODE_PATTERNS = [
    "simple_node_return_all",
    "simple_node_return_id",
    "simple_node_return_properties",
    "node_count",
    "node_count_distinct",
    "node_where_id_equals",
    "node_where_id_greater",
    "node_where_id_less",
    "node_where_id_between",
    "node_where_string_equals",
    "node_where_string_contains",
    "node_where_string_starts_with",
    "node_where_is_null",
    "node_where_is_not_null",
    "node_where_in_list",
    "node_order_by_asc",
    "node_order_by_desc",
    "node_distinct",
    "node_skip_limit",
    "node_with_alias",
    "secondary_node_return",
    "secondary_node_count",
    "secondary_node_filtered",
]

RELATIONSHIP_PATTERNS = [
    "simple_relationship",
    "relationship_return_rel",
    "relationship_count",
    "relationship_with_node_filter",
    "relationship_reverse_direction",
    "relationship_any_direction",
    "relationship_aggregation",
]

VLP_PATTERNS = [
    "vlp_exact_2",
    "vlp_range_1_3",
    "vlp_unbounded",
    "vlp_with_path_variable",
    "vlp_with_node_filter",
]

OPTIONAL_PATTERNS = [
    "optional_match_basic",
    "optional_match_with_filter",
]

AGGREGATION_PATTERNS = [
    "agg_count_group_by",
    "agg_sum",
    "agg_avg",
    "agg_min_max",
    "agg_collect",
]

FUNCTION_PATTERNS = [
    "func_coalesce",
    "func_tostring",
    "func_tointeger",
    "func_size",
]

WHERE_PATTERNS = [
    "where_and",
    "where_or",
    "where_not",
    "where_complex",
]

EXPRESSION_PATTERNS = [
    "expr_add",
    "expr_subtract",
    "expr_multiply",
    "expr_divide",
]

MULTI_HOP_PATTERNS = [
    "two_hop",
    "three_hop",
]

OTHER_PATTERNS = [
    "exists_subquery",
    "unwind_simple",
    "unwind_with_expression",
    # param_simple and param_in_list removed - parameters need special handling
]

ALL_PATTERNS = (
    BASIC_NODE_PATTERNS +
    RELATIONSHIP_PATTERNS +
    VLP_PATTERNS +
    OPTIONAL_PATTERNS +
    AGGREGATION_PATTERNS +
    FUNCTION_PATTERNS +
    WHERE_PATTERNS +
    EXPRESSION_PATTERNS +
    MULTI_HOP_PATTERNS +
    OTHER_PATTERNS
)
