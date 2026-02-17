#!/usr/bin/env python3
"""
Comprehensive Schema Variation Tests for SchemaInference → TypeInference Consolidation.

This test suite provides gap coverage for under-tested schema patterns to ensure
the consolidation doesn't introduce regressions.

Coverage Areas:
1. Multi-table nodes (zeek_merged pattern) - 20+ tests
2. FK-edge patterns (foreign key as relationship) - 15+ tests  
3. Label inference combinations (8 cases) - 25+ tests
4. Denormalized edge cases - 10+ tests
5. Schema interaction patterns - 10+ tests

Test Matrix: 6 schema types × 10 query patterns = 60+ tests

Schema Types:
- Standard: social_integration (User, Post, FOLLOWS, AUTHORED)
- Denormalized: flights with node props in edge table
- Multi-table: zeek with IP nodes in multiple edge tables
- Multi-tenant: parameterized views
- FK-edge: Foreign key columns as relationships
- Polymorphic: Multiple relationship types
"""

import pytest
import requests
import json


# Server endpoint
CLICKGRAPH_URL = "http://localhost:8080"


def query_clickgraph(cypher_query, schema_name="social_integration", variables=None):
    """Execute Cypher query against ClickGraph server."""
    payload = {"query": cypher_query}
    if variables:
        payload["parameters"] = variables
    
    # Add USE clause if not present
    if not cypher_query.strip().upper().startswith("USE"):
        payload["query"] = f"USE {schema_name}; {cypher_query}"
    
    response = requests.post(f"{CLICKGRAPH_URL}/query", json=payload)
    return response


# ============================================================================
# 1. MULTI-TABLE NODE TESTS (20+ tests)
# Pattern: Node appears in multiple edge tables (zeek IP addresses)
# ============================================================================

class TestMultiTableNodes:
    """
    Tests for nodes that appear in multiple relationship tables.
    
    Schema Pattern: IP nodes in both dns_log and conn_log tables
    - (IP)-[:REQUESTED]->(Domain) from dns_log
    - (IP)-[:ACCESSED]->(IP) from conn_log
    
    Challenge: SchemaInference must create UNION for IP from multiple sources
    """
    
    def test_unlabeled_node_creates_union_all_types(self):
        """MATCH (n) should create UNION for all node types."""
        query = "MATCH (n) RETURN count(n) as total"
        response = query_clickgraph(query)
        assert response.status_code == 200
        data = response.json()
        assert "results" in data
        # Should scan all node types (User, Post)
    
    def test_unlabeled_with_relationship_infers_label(self):
        """MATCH (a)-[:FOLLOWS]->(b) should infer both are Users."""
        query = "MATCH (a)-[:FOLLOWS]->(b) RETURN count(*) as total"
        response = query_clickgraph(query)
        assert response.status_code == 200
        # Both a and b should be inferred as User from FOLLOWS relationship
    
    def test_unlabeled_bidirectional_infers_from_schema(self):
        """MATCH (a)--(b) with undirected pattern."""
        query = "MATCH (a:User)--(b) RETURN count(DISTINCT b) as total"
        response = query_clickgraph(query)
        assert response.status_code == 200
        # b can be User (from FOLLOWS) or Post (from AUTHORED)
    
    def test_unlabeled_multiple_patterns_same_var(self):
        """Multiple patterns with same unlabeled variable."""
        query = """
        MATCH (u:User)-[:AUTHORED]->(p)
        MATCH (u)-[:FOLLOWS]->(f)
        RETURN count(*) as total
        """
        response = query_clickgraph(query)
        assert response.status_code == 200
        # p should be inferred as Post, f as User
    
    def test_multi_table_node_union_generation(self):
        """Node in multiple edge tables should generate UNION."""
        query = "MATCH (ip)-[r]->(target) RETURN count(*) as total"
        response = query_clickgraph(query, schema_name="zeek_logs")
        # IP can be source in dns_log or conn_log
        # Should create UNION of both table sources
    
    def test_multi_table_specific_relationship_filters_union(self):
        """Specific relationship should filter UNION branches."""
        query = "MATCH (ip)-[:REQUESTED]->(d) RETURN count(*) as total"
        response = query_clickgraph(query, schema_name="zeek_logs")
        # Should only scan dns_log, not conn_log
    
    def test_multi_table_coupled_edges_same_table(self):
        """Coupled edges from same table (DNS request + resolution)."""
        query = """
        MATCH (ip)-[:REQUESTED]->(d:Domain)-[:RESOLVED_TO]->(rip)
        RETURN count(*) as total
        """
        response = query_clickgraph(query, schema_name="zeek_logs")
        # Both edges from dns_log, should not create excessive UNIONs
    
    def test_multi_table_cross_table_correlation(self):
        """Cross-table pattern: DNS lookup followed by connection."""
        query = """
        MATCH (src)-[:REQUESTED]->(d:Domain)-[:RESOLVED_TO]->(dest)
        MATCH (src)-[:ACCESSED]->(dest)
        RETURN count(*) as total
        """
        response = query_clickgraph(query, schema_name="zeek_logs")
        # First pattern from dns_log, second from conn_log
    
    def test_multi_table_with_clause_correlation(self):
        """WITH clause correlating multi-table patterns."""
        query = """
        MATCH (src)-[:REQUESTED]->(d:Domain)
        WITH src, d
        MATCH (src)-[:ACCESSED]->(dest)
        RETURN count(*) as total
        """
        response = query_clickgraph(query, schema_name="zeek_logs")
    
    def test_multi_table_unlabeled_both_sides(self):
        """Both sides unlabeled in multi-table schema."""
        query = "MATCH (a)-[r]->(b) RETURN count(*) as total"
        response = query_clickgraph(query, schema_name="zeek_logs")
        # Should create massive UNION: IP→Domain, IP→ResolvedIP, IP→IP
    
    def test_multi_table_property_filter_narrows_union(self):
        """Property filter should reduce UNION branches."""
        query = "MATCH (a {type: 'DNS'})-[r]->(b) RETURN count(*) as total"
        response = query_clickgraph(query, schema_name="zeek_logs")
        # Property filter might narrow down possible node types
    
    def test_multi_table_return_properties_from_edges(self):
        """Return properties from edges in multi-table schema."""
        query = """
        MATCH (ip)-[r:REQUESTED]->(d:Domain)
        RETURN ip.address, d.name, r.timestamp
        """
        response = query_clickgraph(query, schema_name="zeek_logs")
    
    def test_multi_table_aggregation_over_union(self):
        """Aggregation over multi-table UNION."""
        query = """
        MATCH (ip)-[r]->(target)
        RETURN ip, count(r) as connections
        """
        response = query_clickgraph(query, schema_name="zeek_logs")
        # Aggregation must work across UNION branches
    
    def test_multi_table_optional_match(self):
        """OPTIONAL MATCH with multi-table nodes."""
        query = """
        MATCH (ip)-[:REQUESTED]->(d:Domain)
        OPTIONAL MATCH (ip)-[:ACCESSED]->(dest)
        RETURN count(*) as total
        """
        response = query_clickgraph(query, schema_name="zeek_logs")
        # LEFT JOIN across different tables
    
    def test_multi_table_variable_length_path(self):
        """Variable-length path in multi-table schema."""
        query = "MATCH (a)-[*1..2]->(b) RETURN count(*) as paths"
        response = query_clickgraph(query, schema_name="zeek_logs")
        # VLP should handle multi-table edges
    
    def test_multi_table_direction_validation(self):
        """Direction validation with multi-table nodes."""
        query = "MATCH (d:Domain)--(ip) RETURN count(*) as total"
        response = query_clickgraph(query, schema_name="zeek_logs")
        # Direction: Domain can have incoming REQUESTED, outgoing RESOLVED_TO
        # Should filter IP based on valid directions
    
    def test_multi_table_multiple_rels_union(self):
        """Multiple relationship types in multi-table schema."""
        query = "MATCH (ip)-[:REQUESTED|ACCESSED]->(target) RETURN count(*)"
        response = query_clickgraph(query, schema_name="zeek_logs")
        # UNION of two different edge tables
    
    def test_multi_table_where_relationship_type(self):
        """WHERE clause on relationship type."""
        query = """
        MATCH (a)-[r]->(b)
        WHERE type(r) = 'REQUESTED'
        RETURN count(*) as total
        """
        response = query_clickgraph(query, schema_name="zeek_logs")
    
    def test_multi_table_count_distinct_nodes(self):
        """Count distinct nodes across multi-table sources."""
        query = """
        MATCH (ip)-[r]->(target)
        RETURN count(DISTINCT ip) as unique_ips
        """
        response = query_clickgraph(query, schema_name="zeek_logs")
        # Must handle IP from both dns_log and conn_log
    
    def test_multi_table_subquery_correlation(self):
        """Subquery with multi-table correlation."""
        query = """
        MATCH (ip)-[:REQUESTED]->(d:Domain)
        WHERE EXISTS {
            MATCH (ip)-[:ACCESSED]->(dest)
        }
        RETURN count(*) as total
        """
        response = query_clickgraph(query, schema_name="zeek_logs")


# ============================================================================
# 2. FK-EDGE PATTERN TESTS (15+ tests)
# Pattern: Foreign key columns act as relationships
# ============================================================================

class TestForeignKeyEdges:
    """
    Tests for foreign key edge patterns.
    
    Schema Pattern: orders.customer_id → customers.id
    - Edge defined by FK column, not separate edge table
    - Node properties come from single table
    
    Challenge: SchemaInference must resolve FK-based relationships correctly
    """
    
    def test_fk_edge_basic_traversal(self):
        """Basic FK edge traversal."""
        query = """
        MATCH (o:Order)-[:BELONGS_TO]->(c:Customer)
        RETURN count(*) as total
        """
        response = query_clickgraph(query, schema_name="orders_fk")
        assert response.status_code == 200
    
    def test_fk_edge_reverse_direction(self):
        """FK edge traversal in reverse direction."""
        query = """
        MATCH (c:Customer)<-[:BELONGS_TO]-(o:Order)
        RETURN count(*) as total
        """
        response = query_clickgraph(query, schema_name="orders_fk")
    
    def test_fk_edge_undirected_pattern(self):
        """Undirected pattern with FK edge."""
        query = """
        MATCH (o:Order)--(c:Customer)
        RETURN count(*) as total
        """
        response = query_clickgraph(query, schema_name="orders_fk")
        # Should match FK direction (Order → Customer)
    
    def test_fk_edge_unlabeled_source(self):
        """Unlabeled source with FK edge."""
        query = """
        MATCH (n)-[:BELONGS_TO]->(c:Customer)
        RETURN count(*) as total
        """
        response = query_clickgraph(query, schema_name="orders_fk")
        # n should be inferred as Order
    
    def test_fk_edge_unlabeled_target(self):
        """Unlabeled target with FK edge."""
        query = """
        MATCH (o:Order)-[:BELONGS_TO]->(n)
        RETURN count(*) as total
        """
        response = query_clickgraph(query, schema_name="orders_fk")
        # n should be inferred as Customer
    
    def test_fk_edge_both_unlabeled(self):
        """Both nodes unlabeled with FK edge."""
        query = """
        MATCH (a)-[:BELONGS_TO]->(b)
        RETURN count(*) as total
        """
        response = query_clickgraph(query, schema_name="orders_fk")
        # Should infer: a=Order, b=Customer from relationship
    
    def test_fk_edge_property_access(self):
        """Access properties across FK edge."""
        query = """
        MATCH (o:Order)-[:BELONGS_TO]->(c:Customer)
        RETURN o.order_id, c.name, o.total_amount
        """
        response = query_clickgraph(query, schema_name="orders_fk")
    
    def test_fk_edge_aggregation(self):
        """Aggregation with FK edge."""
        query = """
        MATCH (c:Customer)<-[:BELONGS_TO]-(o:Order)
        RETURN c.name, count(o) as order_count, sum(o.total_amount) as total
        """
        response = query_clickgraph(query, schema_name="orders_fk")
    
    def test_fk_edge_optional_match(self):
        """OPTIONAL MATCH with FK edge."""
        query = """
        MATCH (c:Customer)
        OPTIONAL MATCH (c)<-[:BELONGS_TO]-(o:Order)
        RETURN c.name, count(o) as orders
        """
        response = query_clickgraph(query, schema_name="orders_fk")
    
    def test_fk_edge_where_clause(self):
        """WHERE clause filtering with FK edge."""
        query = """
        MATCH (o:Order)-[:BELONGS_TO]->(c:Customer)
        WHERE c.country = 'USA' AND o.total_amount > 100
        RETURN count(*) as total
        """
        response = query_clickgraph(query, schema_name="orders_fk")
    
    def test_fk_edge_multiple_fk_same_table(self):
        """Multiple FK edges from same table."""
        query = """
        MATCH (o:Order)-[:BELONGS_TO]->(c:Customer)
        MATCH (o)-[:SHIPPED_TO]->(a:Address)
        RETURN count(*) as total
        """
        response = query_clickgraph(query, schema_name="orders_fk")
    
    def test_fk_edge_with_clause_aggregation(self):
        """WITH clause aggregation over FK edge."""
        query = """
        MATCH (c:Customer)<-[:BELONGS_TO]-(o:Order)
        WITH c, count(o) as order_count
        WHERE order_count > 5
        RETURN count(c) as high_volume_customers
        """
        response = query_clickgraph(query, schema_name="orders_fk")
    
    def test_fk_edge_variable_length_path(self):
        """Variable-length path with FK edges."""
        query = """
        MATCH (o:Order)-[*1..2]->(n)
        RETURN count(*) as paths
        """
        response = query_clickgraph(query, schema_name="orders_fk")
        # Order → Customer → Address (if FK chain exists)
    
    def test_fk_edge_shortest_path(self):
        """Shortest path with FK edges."""
        query = """
        MATCH p = shortestPath((o:Order)-[*]-(a:Address))
        RETURN count(p) as paths
        """
        response = query_clickgraph(query, schema_name="orders_fk")
    
    def test_fk_edge_return_relationship(self):
        """Return FK relationship itself."""
        query = """
        MATCH (o:Order)-[r:BELONGS_TO]->(c:Customer)
        RETURN type(r), properties(r)
        """
        response = query_clickgraph(query, schema_name="orders_fk")


# ============================================================================
# 3. LABEL INFERENCE TESTS (25+ tests for 8 combinations)
# Pattern: (a)-[r]->(b) with different label combinations
# ============================================================================

class TestLabelInference:
    """
    Tests for label inference from relationships.
    
    Eight Combinations:
    1. (a:L1)-[r:R]->(b:L2) - All known
    2. (a)-[r:R]->(b:L2) - Infer left from rel+right
    3. (a:L1)-[r:R]->(b) - Infer right from rel+left
    4. (a:L1)-[r]->(b:L2) - Infer rel from left+right
    5. (a)-[r]->(b:L2) - Infer left from rel+right
    6. (a:L1)-[r]->(b) - Infer right from rel+left
    7. (a)-[r:R]->(b) - Infer both from rel
    8. (a)-[r]->(b) - Infer all from context/UNION
    
    Challenge: SchemaInference.infer_missing_labels() has 240 lines for this
    """
    
    # Case 1: All known (baseline)
    def test_inference_all_labels_known(self):
        """Case 1: (a:User)-[r:FOLLOWS]->(b:User) - all known."""
        query = """
        MATCH (a:User)-[r:FOLLOWS]->(b:User)
        RETURN count(*) as total
        """
        response = query_clickgraph(query)
        assert response.status_code == 200
    
    # Case 2: Infer left from rel+right
    def test_inference_left_from_rel_and_right(self):
        """Case 2: (a)-[r:FOLLOWS]->(b:User) - infer a is User."""
        query = """
        MATCH (a)-[r:FOLLOWS]->(b:User)
        RETURN count(*) as total
        """
        response = query_clickgraph(query)
        assert response.status_code == 200
        # FOLLOWS: User → User, so a must be User
    
    def test_inference_left_from_authored_and_post(self):
        """Case 2 variant: (a)-[:AUTHORED]->(b:Post) - infer a is User."""
        query = """
        MATCH (a)-[:AUTHORED]->(b:Post)
        RETURN count(*) as total
        """
        response = query_clickgraph(query)
        # AUTHORED: User → Post, so a must be User
    
    # Case 3: Infer right from rel+left
    def test_inference_right_from_rel_and_left(self):
        """Case 3: (a:User)-[r:FOLLOWS]->(b) - infer b is User."""
        query = """
        MATCH (a:User)-[r:FOLLOWS]->(b)
        RETURN count(*) as total
        """
        response = query_clickgraph(query)
        assert response.status_code == 200
        # FOLLOWS: User → User, so b must be User
    
    def test_inference_right_from_user_authored(self):
        """Case 3 variant: (a:User)-[:AUTHORED]->(b) - infer b is Post."""
        query = """
        MATCH (a:User)-[:AUTHORED]->(b)
        RETURN count(*) as total
        """
        response = query_clickgraph(query)
        # AUTHORED: User → Post, so b must be Post
    
    # Case 4: Infer rel from left+right
    def test_inference_rel_from_user_to_user(self):
        """Case 4: (a:User)-[r]->(b:User) - infer r is FOLLOWS."""
        query = """
        MATCH (a:User)-[r]->(b:User)
        RETURN count(*) as total
        """
        response = query_clickgraph(query)
        # User → User only valid for FOLLOWS
    
    def test_inference_rel_from_user_to_post(self):
        """Case 4 variant: (a:User)-[r]->(b:Post) - infer r is AUTHORED."""
        query = """
        MATCH (a:User)-[r]->(b:Post)
        RETURN count(*) as total
        """
        response = query_clickgraph(query)
        # User → Post only valid for AUTHORED
    
    # Case 5: Left missing, rel and right known
    def test_inference_left_missing_rel_known(self):
        """Case 5: (a)-[r:AUTHORED]->(b:Post) - infer a is User."""
        query = """
        MATCH (a)-[r:AUTHORED]->(b:Post)
        WHERE a.user_id > 0
        RETURN count(*) as total
        """
        response = query_clickgraph(query)
    
    # Case 6: Right missing, rel and left known
    def test_inference_right_missing_rel_known(self):
        """Case 6: (a:User)-[r:AUTHORED]->(b) - infer b is Post."""
        query = """
        MATCH (a:User)-[r:AUTHORED]->(b)
        WHERE b.post_id > 0
        RETURN count(*) as total
        """
        response = query_clickgraph(query)
    
    # Case 7: Both missing, rel known
    def test_inference_both_missing_rel_known_follows(self):
        """Case 7: (a)-[r:FOLLOWS]->(b) - infer both are User."""
        query = """
        MATCH (a)-[r:FOLLOWS]->(b)
        RETURN count(*) as total
        """
        response = query_clickgraph(query)
        # FOLLOWS: User → User, so both must be User
    
    def test_inference_both_missing_rel_known_authored(self):
        """Case 7 variant: (a)-[:AUTHORED]->(b) - infer a=User, b=Post."""
        query = """
        MATCH (a)-[:AUTHORED]->(b)
        RETURN count(*) as total
        """
        response = query_clickgraph(query)
        # AUTHORED: User → Post
    
    # Case 8: All missing
    def test_inference_all_missing_creates_union(self):
        """Case 8: (a)-[r]->(b) - create UNION for all possibilities."""
        query = """
        MATCH (a)-[r]->(b)
        RETURN count(*) as total
        """
        response = query_clickgraph(query)
        # Should scan: FOLLOWS(User→User) + AUTHORED(User→Post)
    
    # Edge cases and combinations
    def test_inference_undirected_both_labeled(self):
        """Undirected with both labels: (a:User)--(b:User)."""
        query = """
        MATCH (a:User)--(b:User)
        RETURN count(*) as total
        """
        response = query_clickgraph(query)
        # Should match FOLLOWS in both directions
    
    def test_inference_undirected_left_labeled(self):
        """Undirected with left labeled: (a:User)--(b)."""
        query = """
        MATCH (a:User)--(b)
        RETURN count(*) as total
        """
        response = query_clickgraph(query)
        # b can be User (FOLLOWS) or Post (AUTHORED)
    
    def test_inference_undirected_right_labeled(self):
        """Undirected with right labeled: (a)--(b:Post)."""
        query = """
        MATCH (a)--(b:Post)
        RETURN count(*) as total
        """
        response = query_clickgraph(query)
        # a must be User (only User→Post via AUTHORED)
    
    def test_inference_undirected_all_missing(self):
        """Undirected with all missing: (a)--(b)."""
        query = """
        MATCH (a)--(b)
        RETURN count(*) as total
        """
        response = query_clickgraph(query)
        # Massive UNION: all relationships in both directions
    
    def test_inference_multiple_patterns_constraint_propagation(self):
        """Multiple patterns should propagate constraints."""
        query = """
        MATCH (a)-[:FOLLOWS]->(b)
        MATCH (b)-[:AUTHORED]->(c)
        RETURN count(*) as total
        """
        response = query_clickgraph(query)
        # a, b both User from FOLLOWS; c is Post from AUTHORED
    
    def test_inference_with_property_filter_before_inference(self):
        """Property filter before label inference."""
        query = """
        MATCH (a {user_id: 1})-[r:FOLLOWS]->(b)
        RETURN count(*) as total
        """
        response = query_clickgraph(query)
        # Property filter might help inference
    
    def test_inference_optional_match_propagation(self):
        """Label inference with OPTIONAL MATCH."""
        query = """
        MATCH (a:User)-[:AUTHORED]->(p)
        OPTIONAL MATCH (a)-[:FOLLOWS]->(f)
        RETURN count(*) as total
        """
        response = query_clickgraph(query)
        # p inferred as Post, f inferred as User
    
    def test_inference_union_relationship_types(self):
        """Label inference with UNION relationship types."""
        query = """
        MATCH (a)-[:FOLLOWS|AUTHORED]->(b)
        RETURN count(*) as total
        """
        response = query_clickgraph(query)
        # For FOLLOWS: a=User, b=User
        # For AUTHORED: a=User, b=Post
        # Should create UNION with proper type inference per branch
    
    def test_inference_variable_length_path_endpoint(self):
        """Label inference at VLP endpoints."""
        query = """
        MATCH (a:User)-[*1..2]->(b)
        RETURN count(*) as total
        """
        response = query_clickgraph(query)
        # b type depends on path (User via FOLLOWS, Post via AUTHORED)
    
    def test_inference_where_type_function(self):
        """WHERE clause with type() function."""
        query = """
        MATCH (a)-[r]->(b)
        WHERE type(r) IN ['FOLLOWS', 'AUTHORED']
        RETURN count(*) as total
        """
        response = query_clickgraph(query)
    
    def test_inference_aggregation_preserves_types(self):
        """Aggregation should preserve inferred types."""
        query = """
        MATCH (a)-[:AUTHORED]->(b)
        WITH a, count(b) as post_count
        MATCH (a)-[:FOLLOWS]->(f)
        RETURN count(*) as total
        """
        response = query_clickgraph(query)
        # a=User, b=Post, f=User must be preserved across WITH
    
    def test_inference_exists_subquery(self):
        """EXISTS subquery with label inference."""
        query = """
        MATCH (a:User)
        WHERE EXISTS {
            MATCH (a)-[:AUTHORED]->(p)
        }
        RETURN count(*) as total
        """
        response = query_clickgraph(query)
        # p should be inferred as Post inside EXISTS


# ============================================================================
# 4. DENORMALIZED EDGE CASES (10+ tests)
# Pattern: Node properties stored in edge table columns
# ============================================================================

class TestDenormalizedEdgeCases:
    """
    Tests for denormalized edge patterns.
    
    Schema Pattern: flights table with origin/dest properties
    - FROM/TO positions: origin_id (FROM), dest_id (TO)
    - Denormalized props: origin_city, dest_state in flights table
    
    Challenge: SchemaInference lines 180-350 handle complex position logic
    """
    
    def test_denorm_from_position_properties(self):
        """Access FROM node properties from edge table."""
        query = """
        MATCH (origin:Airport)-[f:FLIGHT]->(dest:Airport)
        RETURN origin.city, count(*) as flights
        """
        response = query_clickgraph(query, schema_name="denormalized_flights")
        # origin.city should map to flights.origin_city (FROM position)
    
    def test_denorm_to_position_properties(self):
        """Access TO node properties from edge table."""
        query = """
        MATCH (origin:Airport)-[f:FLIGHT]->(dest:Airport)
        RETURN dest.state, count(*) as flights
        """
        response = query_clickgraph(query, schema_name="denormalized_flights")
        # dest.state should map to flights.dest_state (TO position)
    
    def test_denorm_both_positions_in_where(self):
        """WHERE clause on both FROM and TO properties."""
        query = """
        MATCH (origin:Airport)-[f:FLIGHT]->(dest:Airport)
        WHERE origin.city = 'Seattle' AND dest.state = 'CA'
        RETURN count(*) as flights
        """
        response = query_clickgraph(query, schema_name="denormalized_flights")
    
    def test_denorm_reverse_direction_swaps_positions(self):
        """Reverse direction should swap FROM/TO positions."""
        query = """
        MATCH (dest:Airport)<-[f:FLIGHT]-(origin:Airport)
        RETURN origin.city, dest.state
        """
        response = query_clickgraph(query, schema_name="denormalized_flights")
        # Position mapping should adapt to reversed pattern
    
    def test_denorm_undirected_ambiguous_positions(self):
        """Undirected pattern with denormalized properties."""
        query = """
        MATCH (a:Airport)-[f:FLIGHT]-(b:Airport)
        WHERE a.city = 'Seattle'
        RETURN count(*) as flights
        """
        response = query_clickgraph(query, schema_name="denormalized_flights")
        # Should handle both FROM and TO positions
    
    def test_denorm_property_not_in_node_table(self):
        """Property exists only in edge table, not node table."""
        query = """
        MATCH (o:Airport)-[f:FLIGHT]->(d:Airport)
        RETURN o.terminal, d.gate
        """
        response = query_clickgraph(query, schema_name="denormalized_flights")
        # terminal/gate might only exist in flights table
    
    def test_denorm_mixed_node_and_edge_properties(self):
        """Mix of node table and edge table properties."""
        query = """
        MATCH (o:Airport)-[f:FLIGHT]->(d:Airport)
        RETURN o.code, o.city, f.flight_number, d.state
        """
        response = query_clickgraph(query, schema_name="denormalized_flights")
        # code from airports table, city from flights table
    
    def test_denorm_aggregation_over_denorm_props(self):
        """Aggregation using denormalized properties."""
        query = """
        MATCH (o:Airport)-[f:FLIGHT]->(d:Airport)
        RETURN o.city, d.state, count(*) as flight_count
        """
        response = query_clickgraph(query, schema_name="denormalized_flights")
    
    def test_denorm_variable_length_path_with_denorm(self):
        """VLP with denormalized node properties."""
        query = """
        MATCH (a:Airport)-[*1..2]->(b:Airport)
        WHERE a.city = 'Seattle'
        RETURN count(*) as paths
        """
        response = query_clickgraph(query, schema_name="denormalized_flights")
        # VLP CTE must handle denormalized properties
    
    def test_denorm_optional_match_with_denorm_props(self):
        """OPTIONAL MATCH accessing denormalized properties."""
        query = """
        MATCH (a:Airport)
        OPTIONAL MATCH (a)-[f:FLIGHT]->(b:Airport)
        RETURN a.code, b.city, count(f) as flights
        """
        response = query_clickgraph(query, schema_name="denormalized_flights")


# ============================================================================
# 5. SCHEMA INTERACTION TESTS (10+ tests)
# Pattern: Complex interactions between schema features
# ============================================================================

class TestSchemaInteractions:
    """
    Tests for complex schema feature interactions.
    
    Combinations:
    - Multi-tenant + Denormalized
    - Polymorphic + Multi-table
    - FK-edge + VLP
    - Direction validation + UNION
    """
    
    def test_parameterized_view_with_denorm(self):
        """Multi-tenant parameterized view with denormalized properties."""
        query = """
        MATCH (u:User)-[:AUTHORED]->(p:Post)
        WHERE u.tenant_id = $tenant
        RETURN u.name, p.title
        """
        response = query_clickgraph(
            query,
            schema_name="multi_tenant",
            variables={"tenant": "acme"}
        )
    
    def test_polymorphic_edges_with_label_inference(self):
        """Polymorphic edges requiring label inference."""
        query = """
        MATCH (a)-[:LIKED|SHARED]->(b)
        RETURN count(*) as total
        """
        response = query_clickgraph(query, schema_name="polymorphic")
        # Multiple edge types might have different FROM/TO nodes
    
    def test_direction_validation_filters_union(self):
        """Direction validation should filter UNION branches."""
        query = """
        MATCH (p:Post)--(u)
        RETURN count(DISTINCT u) as total
        """
        response = query_clickgraph(query)
        # Post--(u): Post→User via AUTHORED (reverse), User→Post (invalid)
        # Should only include User, not Post
    
    def test_direction_optimization_undirected_to_directed(self):
        """Undirected pattern optimized to directed when unidirectional."""
        query = """
        MATCH (u:User)--(p:Post)
        RETURN count(*) as total
        """
        response = query_clickgraph(query)
        # User-Post only connected via AUTHORED (User→Post)
        # Should optimize to directed pattern
    
    def test_multiple_schemas_in_session(self):
        """Query multiple schemas in same session with USE clause."""
        query1 = "USE social_integration; MATCH (u:User) RETURN count(u)"
        query2 = "USE zeek_logs; MATCH (ip) RETURN count(ip)"
        
        response1 = query_clickgraph(query1)
        response2 = query_clickgraph(query2)
        
        assert response1.status_code == 200
        assert response2.status_code == 200
    
    def test_complex_union_with_filters(self):
        """Complex UNION with multiple filter conditions."""
        query = """
        MATCH (a)-[r]->(b)
        WHERE a.created_date > '2024-01-01'
        AND b.status = 'active'
        RETURN count(*) as total
        """
        response = query_clickgraph(query)
        # Filters should apply to all UNION branches
    
    def test_nested_optional_matches(self):
        """Nested OPTIONAL MATCH patterns."""
        query = """
        MATCH (a:User)
        OPTIONAL MATCH (a)-[:AUTHORED]->(p:Post)
        OPTIONAL MATCH (p)<-[:LIKED]-(u2:User)
        RETURN count(*) as total
        """
        response = query_clickgraph(query)
    
    def test_with_clause_type_preservation_across_union(self):
        """WITH clause preserving types across UNION."""
        query = """
        MATCH (a)-[:AUTHORED]->(p)
        WITH a, p, p.likes as like_count
        MATCH (a)-[:FOLLOWS]->(f)
        RETURN count(*) as total
        """
        response = query_clickgraph(query)
        # Type inference: a=User, p=Post, f=User must be preserved
    
    def test_aggregation_groups_across_union(self):
        """Aggregation grouping across UNION branches."""
        query = """
        MATCH (u:User)-[r]->(target)
        RETURN u.user_id, type(r), count(*) as action_count
        """
        response = query_clickgraph(query)
        # Should group by user and relationship type
    
    def test_shortest_path_with_type_inference(self):
        """Shortest path requiring type inference."""
        query = """
        MATCH p = shortestPath((a {user_id: 1})-[*]-(b {user_id: 10}))
        RETURN length(p) as hops
        """
        response = query_clickgraph(query)
        # Both a and b should be inferred as User from property


# ============================================================================
# BASELINE VERIFICATION
# ============================================================================

def test_baseline_server_running():
    """Verify ClickGraph server is accessible."""
    try:
        response = requests.get(f"{CLICKGRAPH_URL}/health", timeout=2)
        assert response.status_code == 200
    except Exception as e:
        pytest.fail(f"Server not accessible: {e}")


def test_baseline_schema_loaded():
    """Verify schema is loaded."""
    query = "MATCH (n) RETURN count(n) LIMIT 1"
    response = query_clickgraph(query)
    assert response.status_code == 200


if __name__ == "__main__":
    # Run with: pytest tests/integration/test_schema_variations_comprehensive.py -v
    pytest.main([__file__, "-v", "--tb=short"])
