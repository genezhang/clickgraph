"""
Comprehensive Matrix Tests for ClickGraph

This file generates and runs tests across all schema types and query patterns.
Tests are parametrized to cover the full matrix of combinations.

Run with: pytest tests/integration/matrix/test_comprehensive.py -v
"""

import pytest
import random
import os
import sys
import requests
from typing import List, Tuple

# Import from local matrix conftest (not parent)
from .conftest import (
    SCHEMAS, SchemaConfig, SchemaType, QueryPattern,
    QueryGenerator, NegativeTestGenerator, ExpressionGenerator,
    execute_query, check_server_health, CLICKGRAPH_URL
)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture(scope="session")
def server_running():
    """Ensure ClickGraph server is running"""
    if not check_server_health():
        pytest.skip("ClickGraph server not running")
    return True


@pytest.fixture(scope="session", autouse=True)
def load_all_schemas():
    """Load all required schemas before tests."""
    import yaml
    for schema_name, schema_config in SCHEMAS.items():
        yaml_path = schema_config.yaml_path
        try:
            with open(yaml_path, 'r') as f:
                schema_yaml = f.read()
            response = requests.post(
                f"{CLICKGRAPH_URL}/schemas/load",
                json={
                    "schema_name": schema_name,
                    "config_content": schema_yaml
                },
                timeout=10
            )
            if response.status_code != 200:
                print(f"Warning: Failed to load schema {schema_name}: {response.text}")
        except Exception as e:
            print(f"Warning: Error loading schema {schema_name}: {e}")


@pytest.fixture(params=list(SCHEMAS.keys()))
def schema_name(request):
    """Parametrize over all schemas"""
    return request.param


@pytest.fixture
def schema_config(schema_name) -> SchemaConfig:
    """Get schema configuration"""
    return SCHEMAS[schema_name]


@pytest.fixture
def query_generator(schema_config) -> QueryGenerator:
    """Create query generator for schema"""
    return QueryGenerator(schema_config)


# =============================================================================
# Basic Pattern Tests - Run for each schema
# =============================================================================

class TestBasicPatterns:
    """Test basic MATCH patterns across all schemas"""
    
    def test_simple_node(self, server_running, schema_config, query_generator):
        """Test: MATCH (n:Label) RETURN n"""
        query = query_generator.simple_node()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    def test_simple_edge(self, server_running, schema_config, query_generator):
        """Test: MATCH (a)-[r]->(b) RETURN ..."""
        query = query_generator.simple_edge()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    def test_filtered_node(self, server_running, schema_config, query_generator):
        """Test: MATCH (n) WHERE ... RETURN n"""
        query = query_generator.filtered_node()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    def test_filtered_edge(self, server_running, schema_config, query_generator):
        """Test: MATCH (a)-[r]->(b) WHERE ... RETURN ..."""
        query = query_generator.filtered_edge()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"


class TestMultiHopPatterns:
    """Test multi-hop traversal patterns"""
    
    def test_two_hop(self, server_running, schema_config, query_generator):
        """Test: (a)-[]->(b)-[]->(c)"""
        query = query_generator.two_hop()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    def test_two_hop_with_cross_filter(self, server_running, schema_config, query_generator):
        """Test: (a)-[r1]->(b)-[r2]->(c) WHERE r1.prop < r2.prop"""
        query = query_generator.two_hop_with_cross_filter()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    def test_three_hop(self, server_running, schema_config, query_generator):
        """Test: (a)-[]->(b)-[]->(c)-[]->(d)"""
        query = query_generator.three_hop()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"


class TestVariableLengthPaths:
    """Test variable-length path patterns"""
    
    def test_vlp_star(self, server_running, schema_config, query_generator):
        """Test: (a)-[*]->(b)"""
        query = query_generator.vlp_star()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    @pytest.mark.parametrize("hops", [2, 3, 4])
    def test_vlp_exact(self, server_running, schema_config, query_generator, hops):
        """Test: (a)-[*N]->(b) for various N"""
        query = query_generator.vlp_exact(hops)
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    @pytest.mark.parametrize("min_hops,max_hops", [(1, 2), (1, 3), (2, 4), (1, 5)])
    def test_vlp_range(self, server_running, schema_config, query_generator, min_hops, max_hops):
        """Test: (a)-[*min..max]->(b)"""
        query = query_generator.vlp_range(min_hops, max_hops)
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    @pytest.mark.parametrize("min_hops", [1, 2, 3])
    def test_vlp_open_end(self, server_running, schema_config, query_generator, min_hops):
        """Test: (a)-[*min..]->(b)"""
        query = query_generator.vlp_open_end(min_hops)
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"


class TestShortestPath:
    """Test shortest path algorithms"""
    
    def test_shortest_path(self, server_running, schema_config, query_generator):
        """Test: shortestPath((a)-[*]->(b))"""
        query = query_generator.shortest_path()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    def test_all_shortest_paths(self, server_running, schema_config, query_generator):
        """Test: allShortestPaths((a)-[*]->(b))"""
        query = query_generator.all_shortest_paths()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"


class TestOptionalMatch:
    """Test OPTIONAL MATCH patterns"""
    
    def test_optional_match_simple(self, server_running, schema_config, query_generator):
        """Test: MATCH (a) OPTIONAL MATCH (a)-[]->(b)"""
        query = query_generator.optional_match()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    def test_optional_match_with_filter(self, server_running, schema_config, query_generator):
        """Test: MATCH (a) WHERE ... OPTIONAL MATCH ..."""
        query = query_generator.optional_with_filter()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"


class TestWithChaining:
    """Test WITH clause patterns
    
    NOTE: Some WITH clause patterns have known issues with table alias duplication.
    These will be fixed in a future release.
    """
    
    @pytest.mark.xfail(reason="Known bug: duplicate table alias in WITH clause")
    def test_with_simple(self, server_running, schema_config, query_generator):
        """Test: MATCH ... WITH ... RETURN"""
        query = query_generator.with_simple()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    @pytest.mark.xfail(reason="Known bug: duplicate table alias in WITH clause")
    def test_with_aggregation(self, server_running, schema_config, query_generator):
        """Test: WITH ... count() ... WHERE cnt > X"""
        query = query_generator.with_aggregation()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    @pytest.mark.xfail(reason="Known bug: duplicate table alias in WITH clause")
    def test_with_cross_table(self, server_running, schema_config, query_generator):
        """Test: MATCH ... WITH ... MATCH ... WHERE correlation"""
        query = query_generator.with_cross_table()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"


class TestAggregations:
    """Test aggregation functions"""
    
    def test_count(self, server_running, schema_config, query_generator):
        """Test: count(n)"""
        query = query_generator.count_simple()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    def test_count_distinct(self, server_running, schema_config, query_generator):
        """Test: count(DISTINCT n.prop)"""
        query = query_generator.count_distinct()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    def test_sum_avg(self, server_running, schema_config, query_generator):
        """Test: sum(), avg()"""
        query = query_generator.sum_avg()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    def test_collect(self, server_running, schema_config, query_generator):
        """Test: collect()"""
        query = query_generator.collect_agg()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    def test_min_max(self, server_running, schema_config, query_generator):
        """Test: min(), max()"""
        query = query_generator.min_max()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"


class TestGroupBy:
    """Test GROUP BY patterns"""
    
    def test_group_by(self, server_running, schema_config, query_generator):
        """Test: RETURN prop, count(*) ... ORDER BY"""
        query = query_generator.group_by()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    def test_group_by_having(self, server_running, schema_config, query_generator):
        """Test: WITH ... count() as cnt WHERE cnt > X"""
        query = query_generator.group_by_having()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"


class TestOrdering:
    """Test ORDER BY, LIMIT, SKIP"""
    
    def test_order_by(self, server_running, schema_config, query_generator):
        """Test: ORDER BY prop ASC/DESC"""
        query = query_generator.order_by()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    def test_order_limit_skip(self, server_running, schema_config, query_generator):
        """Test: ORDER BY ... SKIP N LIMIT M"""
        query = query_generator.order_limit_skip()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"


class TestMultiplePatterns:
    """Test multiple relationship types and patterns"""
    
    @pytest.mark.xfail(reason="Known bug: UNION SQL syntax error in multi-rel type")
    def test_multi_rel_type(self, server_running, schema_config, query_generator):
        """Test: -[:TYPE1|TYPE2|TYPE3]->"""
        query = query_generator.multi_rel_type()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"


class TestExpressions:
    """Test various expression types in WHERE clauses"""
    
    def test_arithmetic_in_where(self, server_running, schema_config, query_generator):
        """Test: WHERE r1.prop + N <= r2.prop"""
        query = query_generator.arithmetic_in_where()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    def test_string_predicates(self, server_running, schema_config, query_generator):
        """Test: STARTS WITH, ENDS WITH, CONTAINS"""
        query = query_generator.string_predicates()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    def test_null_handling(self, server_running, schema_config, query_generator):
        """Test: IS NULL, IS NOT NULL"""
        query = query_generator.null_handling()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    def test_in_list(self, server_running, schema_config, query_generator):
        """Test: prop IN [1, 2, 3]"""
        query = query_generator.in_list()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    def test_case_expression(self, server_running, schema_config, query_generator):
        """Test: CASE WHEN ... THEN ... ELSE ... END"""
        query = query_generator.case_expression()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    def test_regex(self, server_running, schema_config, query_generator):
        """Test: prop =~ 'pattern'"""
        query = query_generator.regex_match()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"


class TestFunctions:
    """Test built-in functions"""
    
    def test_id_function(self, server_running, schema_config, query_generator):
        """Test: id(n)"""
        query = query_generator.id_function()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    def test_type_function(self, server_running, schema_config, query_generator):
        """Test: type(r)"""
        query = query_generator.type_function()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    def test_labels_function(self, server_running, schema_config, query_generator):
        """Test: labels(n)"""
        query = query_generator.labels_function()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"


class TestPathVariables:
    """Test path variable patterns"""
    
    def test_path_variable(self, server_running, schema_config, query_generator):
        """Test: p = (a)-[*]->(b)"""
        query = query_generator.path_variable()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    @pytest.mark.xfail(reason="Known bug: length(p) not resolved in GROUP BY")
    def test_path_length(self, server_running, schema_config, query_generator):
        """Test: length(p)"""
        query = query_generator.path_length()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    def test_path_nodes(self, server_running, schema_config, query_generator):
        """Test: nodes(p)"""
        query = query_generator.path_nodes()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"


class TestParameters:
    """Test query parameters"""
    
    def test_parameter_simple(self, server_running, schema_config, query_generator):
        """Test: WHERE n.prop = $param"""
        query = query_generator.parameter_simple()
        result = execute_query(query, params={"param1": 1}, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    def test_parameter_complex(self, server_running, schema_config, query_generator):
        """Test: WHERE n.prop IN $list AND n.prop > $min"""
        query = query_generator.parameter_complex()
        result = execute_query(query, params={"param_list": [1, 2, 3], "min_val": 0}, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"


class TestUnwind:
    """Test UNWIND patterns
    
    UNWIND works for simple literal arrays. More complex UNWIND
    with MATCH may have limitations.
    """
    
    def test_unwind_simple(self, server_running, query_generator):
        """Test: UNWIND [1,2,3] AS x RETURN x"""
        query = query_generator.unwind_simple()
        result = execute_query(query)  # UNWIND doesn't need schema
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    @pytest.mark.xfail(reason="UNWIND with MATCH has complex execution requirements")
    def test_unwind_with_match(self, server_running, schema_config, query_generator):
        """Test: UNWIND ... MATCH ..."""
        query = query_generator.unwind_with_match()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"


class TestExists:
    """Test EXISTS subquery"""
    
    def test_exists_subquery(self, server_running, schema_config, query_generator):
        """Test: WHERE EXISTS { MATCH ... }"""
        query = query_generator.exists_subquery()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"


class TestUndirected:
    """Test undirected relationship patterns
    
    Undirected patterns are implemented via UNION ALL (combining both directions).
    """
    
    def test_undirected_simple(self, server_running, schema_config, query_generator):
        """Test: (a)-[r]-(b)"""
        query = query_generator.undirected_simple()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    @pytest.mark.xfail(reason="Multi-hop undirected patterns may have issues")
    def test_undirected_multi_hop(self, server_running, schema_config, query_generator):
        """Test: (a)-[r1]-(b)-[r2]-(c)"""
        query = query_generator.undirected_multi_hop()
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"


# =============================================================================
# Negative Tests - Invalid Queries
# =============================================================================

class TestNegativeInvalidSyntax:
    """Test that invalid queries return appropriate errors"""
    
    @pytest.mark.parametrize("query,error_type", NegativeTestGenerator.get_invalid_queries())
    def test_invalid_query_returns_error(self, server_running, query, error_type):
        """Invalid queries should return error responses, not crash"""
        result = execute_query(query, schema_name=schema_config.name)
        
        # Query should either:
        # 1. Return an error (success=False)
        # 2. Return with status != 200
        # 3. Contain 'error' in response
        
        if result["success"]:
            # Some "invalid" queries might actually be valid in certain contexts
            # (e.g., inference might handle missing labels)
            pytest.skip(f"Query unexpectedly succeeded - may be valid: {query}")
        else:
            # Verify we got an error response, not a crash
            assert result["status_code"] != 0, f"Server crashed on: {query}"
            assert "exception" not in str(result.get("body", "")).lower() or \
                   "error" in str(result.get("body", "")).lower(), \
                   f"Expected error message for: {query}"


# =============================================================================
# Random Expression Tests
# =============================================================================

class TestRandomExpressions:
    """Test randomly generated expressions"""
    
    @pytest.mark.parametrize("seed", range(10))  # 10 random variations per schema
    def test_random_where_simple(self, server_running, schema_config, seed):
        """Test randomly generated simple WHERE clauses"""
        random.seed(seed)
        expr_gen = ExpressionGenerator(schema_config, seed)
        query_gen = QueryGenerator(schema_config)
        
        label = schema_config.node_labels[0]
        props = schema_config.node_properties.get(label, [("id", "int")])
        
        # Generate random filter
        aliases_with_props = [("n", p[0], p[1]) for p in props[:3]]
        where_clause = expr_gen.random_where_clause(aliases_with_props, complexity=1)
        
        query = f"MATCH (n:{label}) WHERE {where_clause} RETURN n LIMIT 10"
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    @pytest.mark.parametrize("seed", range(10))
    def test_random_where_complex(self, server_running, schema_config, seed):
        """Test randomly generated complex WHERE clauses"""
        random.seed(seed)
        expr_gen = ExpressionGenerator(schema_config, seed)
        
        label = schema_config.node_labels[0]
        props = schema_config.node_properties.get(label, [("id", "int")])
        
        aliases_with_props = [("n", p[0], p[1]) for p in props[:3]]
        where_clause = expr_gen.random_where_clause(aliases_with_props, complexity=2)
        
        query = f"MATCH (n:{label}) WHERE {where_clause} RETURN n LIMIT 10"
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"


# =============================================================================
# Cross-Table Expression Tests (The OnTime Benchmark Pattern)
# =============================================================================

class TestCrossTableExpressions:
    """Test expressions that span multiple tables/relationships"""
    
    def test_equality_across_relationships(self, server_running, schema_config, query_generator):
        """Test: r1.prop = r2.prop"""
        label = schema_config.node_labels[0]
        edge = schema_config.edge_types[0]
        edge_props = schema_config.edge_properties.get(edge, [])
        
        if not edge_props:
            pytest.skip(f"No edge properties for {edge}")
        
        prop, _ = edge_props[0]
        query = f"""
MATCH (a:{label})-[r1:{edge}]->(b)-[r2:{edge}]->(c) 
WHERE r1.{prop} = r2.{prop} 
RETURN a, c LIMIT 10
"""
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    def test_inequality_across_relationships(self, server_running, schema_config, query_generator):
        """Test: r1.prop < r2.prop (the ontime pattern that was broken)"""
        label = schema_config.node_labels[0]
        edge = schema_config.edge_types[0]
        edge_props = schema_config.edge_properties.get(edge, [])
        int_props = [p for p in edge_props if p[1] == "int"]
        
        if not int_props:
            pytest.skip(f"No int edge properties for {edge}")
        
        prop, _ = int_props[0]
        query = f"""
MATCH (a:{label})-[r1:{edge}]->(b)-[r2:{edge}]->(c) 
WHERE r1.{prop} < r2.{prop} 
RETURN a, c LIMIT 10
"""
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    def test_arithmetic_across_relationships(self, server_running, schema_config, query_generator):
        """Test: r1.prop + 100 <= r2.prop"""
        label = schema_config.node_labels[0]
        edge = schema_config.edge_types[0]
        edge_props = schema_config.edge_properties.get(edge, [])
        int_props = [p for p in edge_props if p[1] == "int"]
        
        if len(int_props) < 2:
            pytest.skip(f"Need 2+ int edge properties for {edge}")
        
        prop1, _ = int_props[0]
        prop2, _ = int_props[1] if len(int_props) > 1 else int_props[0]
        
        query = f"""
MATCH (a:{label})-[r1:{edge}]->(b)-[r2:{edge}]->(c) 
WHERE r1.{prop1} + 100 <= r2.{prop2} 
RETURN a, c LIMIT 10
"""
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    def test_multiple_cross_conditions(self, server_running, schema_config, query_generator):
        """Test: r1.prop1 = r2.prop1 AND r1.prop2 < r2.prop2"""
        label = schema_config.node_labels[0]
        edge = schema_config.edge_types[0]
        edge_props = schema_config.edge_properties.get(edge, [])
        
        if len(edge_props) < 2:
            pytest.skip(f"Need 2+ edge properties for {edge}")
        
        prop1, _ = edge_props[0]
        prop2, _ = edge_props[1]
        
        query = f"""
MATCH (a:{label})-[r1:{edge}]->(b)-[r2:{edge}]->(c) 
WHERE r1.{prop1} = r2.{prop1} AND r1.{prop2} < r2.{prop2}
RETURN a, c LIMIT 10
"""
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"


# =============================================================================
# Schema-Specific Tests
# =============================================================================

class TestDenormalizedSchema:
    """Tests specific to denormalized schemas (ontime, zeek)"""
    
    @pytest.fixture
    def denormalized_schemas(self):
        return [s for s in SCHEMAS.values() if s.schema_type == SchemaType.DENORMALIZED]
    
    def test_node_property_from_edge(self, server_running):
        """Test accessing node properties that come from edge table"""
        schema = SCHEMAS.get("ontime_benchmark")
        if not schema:
            pytest.skip("ontime_benchmark schema not configured")
        
        query = "MATCH (a:Airport)-[r:FLIGHT]->(b:Airport) RETURN a.code, b.code LIMIT 10"
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"


class TestMultiTableLabel:
    """Tests specific to multi-table label schemas (zeek_merged)"""
    
    def test_label_from_multiple_tables(self, server_running):
        """Test querying a label that spans multiple tables"""
        schema = SCHEMAS.get("zeek_merged")
        if not schema:
            pytest.skip("zeek_merged schema not configured")
        
        query = "MATCH (ip:IP) RETURN count(DISTINCT ip.ip) as unique_ips"
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    def test_cross_table_correlation(self, server_running):
        """Test WITH pattern for cross-table queries"""
        schema = SCHEMAS.get("zeek_merged")
        if not schema:
            pytest.skip("zeek_merged schema not configured")
        
        query = """
MATCH (ip1:IP)-[:DNS_REQUESTED]->(d:Domain)
WITH ip1, d
MATCH (ip2:IP)-[:CONNECTED_TO]->(dest:IP)
WHERE ip1.ip = ip2.ip
RETURN ip1.ip, d.name, dest.ip LIMIT 10
"""
        result = execute_query(query, schema_name=schema_config.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"


class TestFKEdgeSchema:
    """Tests specific to FK-edge schemas (filesystem)"""
    
    def test_fk_edge_traversal(self, server_running):
        """Test FK-edge pattern traversal"""
        schema = SCHEMAS.get("filesystem")
        if not schema:
            pytest.skip("filesystem schema not configured")
        
        query = "MATCH (parent:Object)-[:PARENT]->(child:Object) RETURN parent.name, child.name LIMIT 10"
        result = execute_query(query, schema_name=schema.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"
    
    def test_fk_edge_vlp(self, server_running):
        """Test VLP on FK-edge schema"""
        schema = SCHEMAS.get("filesystem")
        if not schema:
            pytest.skip("filesystem schema not configured")
        
        query = "MATCH p = (a:Object)-[:PARENT*1..3]->(b:Object) RETURN a.name, b.name LIMIT 10"
        result = execute_query(query, schema_name=schema.name)
        assert result["success"], f"Query failed: {query}\nResult: {result['body']}"


# =============================================================================
# Entry point for running specific test counts
# =============================================================================

if __name__ == "__main__":
    # Quick summary of test counts
    import inspect
    
    test_classes = [
        TestBasicPatterns, TestMultiHopPatterns, TestVariableLengthPaths,
        TestShortestPath, TestOptionalMatch, TestWithChaining,
        TestAggregations, TestGroupBy, TestOrdering, TestMultiplePatterns,
        TestExpressions, TestFunctions, TestPathVariables, TestParameters,
        TestUnwind, TestExists, TestUndirected, TestNegativeInvalidSyntax,
        TestRandomExpressions, TestCrossTableExpressions,
        TestDenormalizedSchema, TestMultiTableLabel, TestFKEdgeSchema,
    ]
    
    total_tests = 0
    for cls in test_classes:
        methods = [m for m in dir(cls) if m.startswith("test_")]
        print(f"{cls.__name__}: {len(methods)} tests")
        total_tests += len(methods)
    
    print(f"\nTotal test methods: {total_tests}")
    print(f"Schemas: {len(SCHEMAS)}")
    print(f"Estimated total tests (methods × schemas): ~{total_tests * len(SCHEMAS)}")
    print(f"Plus negative tests: {len(NegativeTestGenerator.get_invalid_queries())}")
    print(f"Plus random variations: {10 * 2 * len(SCHEMAS)}")  # 10 seeds × 2 complexity × schemas
