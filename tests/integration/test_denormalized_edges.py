"""
Integration tests for denormalized edge table feature.

Tests the denormalized property access pattern where node properties
are stored directly in edge tables (OnTime flights style).

Key features tested:
1. Direct property access from edge table (no JOIN needed)
2. Variable-length paths with denormalized properties
3. Fallback to node table when property not denormalized
4. Query performance optimization (no unnecessary JOINs)
"""

import os
import pytest
from conftest import (
    execute_cypher,
    assert_query_success,
    assert_row_count,
    assert_column_exists,
    CLICKGRAPH_URL
)


@pytest.fixture
def denormalized_flights_graph(clickhouse_client, test_database):
    """
    Create denormalized flights graph for testing.
    
    Schema:
        - flights: Single table containing BOTH edge and node data
          * Edge data: flight_number, airline, times, distance  
          * Origin node properties: origin_code, origin_city, origin_state
          * Dest node properties: dest_code, dest_city, dest_state
        - Airport: Virtual node derived from flights table (no separate table)
    
    This is the denormalized edge table pattern - eliminates JOINs by storing
    all node properties directly in the edge table.
    """
    # Clean only the flights table (don't drop other test tables)
    try:
        clickhouse_client.command(f"DROP TABLE IF EXISTS {test_database}.flights")
    except Exception:
        pass  # Table might not exist yet
    
    # Read and execute setup SQL
    with open('scripts/test/setup_denormalized_test_data.sql', 'r') as f:
        setup_sql = f.read()
    
    # Remove comment-only lines, then split by semicolon
    lines = [line for line in setup_sql.split('\n') if not line.strip().startswith('--') and line.strip()]
    cleaned_sql = '\n'.join(lines)
    
    # Execute each statement
    statement_count = 0
    for statement in cleaned_sql.split(';'):
        statement = statement.strip()
        if statement:
            print(f"Executing: {statement[:50]}...")
            clickhouse_client.command(statement)
            statement_count += 1
    print(f"Executed {statement_count} SQL statements")
    
    # Verify flights table was created
    result = clickhouse_client.command(
        f"SELECT name FROM system.tables WHERE database = '{test_database}' AND name = 'flights' FORMAT TabSeparated"
    )
    tables = [line.strip() for line in result.split('\n') if line.strip()]
    print(f"Flights table after setup: {tables}")
    assert 'flights' in tables, f"Expected 'flights' table to be created"
    
    # Load schema via API
    import requests
    import yaml
    
    # Read schema file
    with open('schemas/test/denormalized_flights.yaml', 'r') as f:
        schema_content = f.read()
    
    response = requests.post(
        f'{CLICKGRAPH_URL}/schemas/load',
        json={
            'schema_name': 'denormalized_flights',
            'config_content': schema_content
        }
    )
    assert response.status_code == 200, f"Failed to load schema: {response.text}"
    
    return {
        "schema_name": "denormalized_flights",
        "database": test_database
    }


class TestDenormalizedPropertyAccess:
    """Test direct property access from denormalized edge tables."""
    
    def test_simple_flight_query(self, denormalized_flights_graph, clickhouse_client, test_database):
        """Test basic flight query returning edge properties."""
        # Debug: verify tables exist
        result = clickhouse_client.command(f"SELECT name FROM system.tables WHERE database = '{test_database}' FORMAT TabSeparated")
        tables = [line.strip() for line in result.split('\n') if line.strip()]
        print(f"Tables before query: {tables}")
        
        response = execute_cypher(
            """
            MATCH (origin:Airport)-[f:FLIGHT]->(dest:Airport)
            RETURN f.flight_num, f.carrier
            """,
            schema_name=denormalized_flights_graph["schema_name"]
        )
        
        assert_query_success(response)
        # All 6 flights should be returned
        assert_row_count(response, 6)
        assert_column_exists(response, "f.flight_num")
        assert_column_exists(response, "f.carrier")
    
    def test_denormalized_origin_properties(self, denormalized_flights_graph):
        """Test accessing origin airport properties from flights table."""
        response = execute_cypher(
            """
            MATCH (origin:Airport)-[f:FLIGHT]->(dest:Airport)
            WHERE f.flight_num = 'AA100'
            RETURN origin.city, origin.state
            """,
            schema_name=denormalized_flights_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        assert_column_exists(response, "origin.city")
        assert_column_exists(response, "origin.state")
        
        # Verify correct values
        row = response['results'][0]
        assert row['origin.city'] == 'Los Angeles'
        assert row['origin.state'] == 'CA'
    
    def test_denormalized_dest_properties(self, denormalized_flights_graph):
        """Test accessing destination airport properties from flights table."""
        response = execute_cypher(
            """
            MATCH (origin:Airport)-[f:FLIGHT]->(dest:Airport)
            WHERE f.flight_num = 'AA100'
            RETURN dest.city, dest.state
            """,
            schema_name=denormalized_flights_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        assert_column_exists(response, "dest.city")
        assert_column_exists(response, "dest.state")
        
        # Verify correct values
        row = response['results'][0]
        assert row['dest.city'] == 'San Francisco'
        assert row['dest.state'] == 'CA'
    
    def test_both_origin_and_dest_properties(self, denormalized_flights_graph):
        """Test accessing both origin and destination properties together."""
        response = execute_cypher(
            """
            MATCH (origin:Airport)-[f:FLIGHT]->(dest:Airport)
            WHERE origin.state = 'CA' AND dest.state = 'NY'
            RETURN origin.city, dest.city, f.flight_num
            ORDER BY f.flight_num
            """,
            schema_name=denormalized_flights_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)  # Only SFO -> JFK
        
        row = response['results'][0]
        assert row['origin.city'] == 'San Francisco'
        assert row['dest.city'] == 'New York'
        assert row['f.flight_num'] == 'UA200'
    
    def test_sql_has_no_joins(self, denormalized_flights_graph):
        """Verify that denormalized queries generate SQL without JOINs."""
        import requests
        
        # Use sql_only mode to get generated SQL
        query = """
            MATCH (origin:Airport)-[f:FLIGHT]->(dest:Airport)
            RETURN origin.city, dest.city, f.carrier
            LIMIT 1
        """
        schema_name = denormalized_flights_graph["schema_name"]
        
        response = requests.post(
            "http://localhost:7475/query",
            json={"query": f"USE {schema_name} {query}", "sql_only": True}
        )
        response.raise_for_status()
        result = response.json()
        
        # sql_only returns SQL in generated_sql field
        sql = result.get('generated_sql', '')
        
        # Check SQL - should NOT contain JOIN (all data in flights table)
        # Match the JOIN keyword, not substrings such as the trailing
        # `SETTINGS join_use_nulls = 1` that SQL-only output carries (#1314).
        import re
        assert not re.search(r'\bJOIN\b', sql.upper()), f"SQL should not contain JOINs for denormalized query: {sql}"
        
        # Should query only flights table
        assert 'flights' in sql.lower()


class TestDenormalizedWithFilters:
    """Test filtering on denormalized properties."""
    
    def test_filter_on_origin_city(self, denormalized_flights_graph):
        """Filter flights by origin city (denormalized property)."""
        response = execute_cypher(
            """
            MATCH (origin:Airport)-[f:FLIGHT]->(dest:Airport)
            WHERE origin.city = 'Los Angeles'
            RETURN f.flight_num, dest.city
            ORDER BY f.flight_num
            """,
            schema_name=denormalized_flights_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 2)  # LAX -> SFO and LAX -> ORD
        
        flight_nums = [row['f.flight_num'] for row in response['results']]
        assert 'AA100' in flight_nums
        assert 'UA600' in flight_nums
    
    def test_filter_on_dest_state(self, denormalized_flights_graph):
        """Filter flights by destination state (denormalized property)."""
        response = execute_cypher(
            """
            MATCH (origin:Airport)-[f:FLIGHT]->(dest:Airport)
            WHERE dest.state = 'CA'
            RETURN COUNT(f) as flight_count
            """,
            schema_name=denormalized_flights_graph["schema_name"]
        )
        
        assert_query_success(response)
        row = response['results'][0]
        assert row['flight_count'] == 3  # SFO, LAX x2
    
    def test_complex_filter_denormalized_and_edge_props(self, denormalized_flights_graph):
        """Filter on both denormalized node properties and edge properties."""
        response = execute_cypher(
            """
            MATCH (origin:Airport)-[f:FLIGHT]->(dest:Airport)
            WHERE origin.state = 'CA' 
              AND dest.state = 'CA'
              AND f.carrier = 'American Airlines'
            RETURN origin.city, dest.city, f.flight_num
            """,
            schema_name=denormalized_flights_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)  # Only LAX -> SFO (AA100)
        
        row = response['results'][0]
        assert row['f.flight_num'] == 'AA100'


class TestDenormalizedVariableLengthPaths:
    """Test variable-length paths with denormalized properties."""
    
    def test_variable_path_with_denormalized_properties(self, denormalized_flights_graph):
        """Test variable-length path returning denormalized properties."""
        response = execute_cypher(
            """
            MATCH path = (origin:Airport)-[f:FLIGHT*1..2]->(dest:Airport)
            WHERE origin.code = 'LAX' AND dest.code = 'ATL'
            RETURN origin.city, dest.city, length(path) as hops
            ORDER BY hops
            LIMIT 1
            """,
            schema_name=denormalized_flights_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        
        row = response['results'][0]
        assert row['origin.city'] == 'Los Angeles'
        assert row['dest.city'] == 'Atlanta'
        assert row['hops'] == 2  # LAX -> ORD -> ATL
    
    def test_zero_hop_start_filter_constrains_the_seed_1166(self, denormalized_flights_graph):
        """#1166: `*0..N` must start ONLY from the filtered node.

        The zero-hop seed used to drop the start predicate, so the recursion
        started from every airport. Oracle (flights: LAX->SFO, SFO->JFK,
        JFK->LAX, ORD->ATL, ATL->LAX, LAX->ORD; a denorm `*0..N` is
        node-unique): from ATL -> ATL(0), LAX(1), then SFO and ORD(2).
        """
        response = execute_cypher(
            """
            MATCH (a:Airport)-[:FLIGHT*0..2]->(b:Airport)
            WHERE a.code = 'ATL'
            RETURN b.code AS code
            ORDER BY code
            """,
            schema_name=denormalized_flights_graph["schema_name"]
        )

        assert_query_success(response)
        assert [r['code'] for r in response['results']] == ['ATL', 'LAX', 'ORD', 'SFO']

    def test_zero_hop_start_filter_on_role_dependent_property_1166(self, denormalized_flights_graph):
        """#1166: `city` is a different physical column per role (OriginCityName /
        DestCityName); the start filter must resolve in BOTH seed scans. From
        Chicago (ORD): ORD(0), ATL(1), LAX(2)."""
        response = execute_cypher(
            """
            MATCH (a:Airport)-[:FLIGHT*0..2]->(b:Airport)
            WHERE a.city = 'Chicago' AND a.state = 'IL'
            RETURN b.code AS code
            ORDER BY code
            """,
            schema_name=denormalized_flights_graph["schema_name"]
        )

        assert_query_success(response)
        assert [r['code'] for r in response['results']] == ['ATL', 'LAX', 'ORD']

    @pytest.mark.parametrize("rng, expected", [
        ("*0..2", ['ATL', 'LAX', 'ORD', 'SFO']),
        ("*1..2", ['LAX', 'ORD', 'SFO']),
        ("*2..2", ['ORD', 'SFO']),
    ])
    def test_function_call_start_predicate_1169(self, denormalized_flights_graph, rng, expected):
        """#1169: a function call around the start node's property
        (`toLower(a.city) = 'atlanta'`) must constrain the start exactly as the
        plain predicate does, for every hop range. `*0..N` used to drop it
        (every airport started a path) and `*N..M` spelled the unresolved node
        alias (`a.OriginCityName`, Code 47). From ATL: ATL(0), LAX(1), SFO and
        ORD(2)."""
        response = execute_cypher(
            f"""
            MATCH (a:Airport)-[:FLIGHT{rng}]->(b:Airport)
            WHERE toLower(a.city) = 'atlanta'
            RETURN b.code AS code
            ORDER BY code
            """,
            schema_name=denormalized_flights_graph["schema_name"]
        )

        assert_query_success(response)
        assert [r['code'] for r in response['results']] == expected

    def test_function_call_conjunct_is_not_dropped_1169(self, denormalized_flights_graph):
        """#1169: `a.city = 'Atlanta' AND toLower(a.state) = 'xx'` is empty; the
        function conjunct used to vanish while the plain one stayed (4 rows)."""
        response = execute_cypher(
            """
            MATCH (a:Airport)-[:FLIGHT*0..2]->(b:Airport)
            WHERE a.city = 'Atlanta' AND toLower(a.state) = 'xx'
            RETURN b.code AS code
            """,
            schema_name=denormalized_flights_graph["schema_name"]
        )

        assert_query_success(response)
        assert response['results'] == []

    def test_function_call_on_both_endpoints_1169(self, denormalized_flights_graph):
        """#1169: a function call over BOTH endpoints is a whole-path predicate;
        it must resolve on the final endpoint pair (it used to spell the unresolved
        `a.OriginCityName`). Closed 3-cycles in the fixture: LAX->SFO->JFK->LAX,
        LAX->ORD->ATL->LAX and their rotations; no shorter path returns to its
        own city."""
        plain = execute_cypher(
            """
            MATCH (a:Airport)-[:FLIGHT*1..3]->(b:Airport)
            WHERE a.city = b.city
            RETURN a.code AS a, b.code AS b
            ORDER BY a, b
            """,
            schema_name=denormalized_flights_graph["schema_name"]
        )
        fn = execute_cypher(
            """
            MATCH (a:Airport)-[:FLIGHT*1..3]->(b:Airport)
            WHERE toLower(a.city) = toLower(b.city)
            RETURN a.code AS a, b.code AS b
            ORDER BY a, b
            """,
            schema_name=denormalized_flights_graph["schema_name"]
        )

        assert_query_success(plain)
        assert_query_success(fn)
        assert len(plain['results']) == 6
        assert fn['results'] == plain['results']

    def test_fixed_hop_before_vlp_keeps_its_where_filter_1170(self, denormalized_flights_graph):
        """#1170: a FIXED hop chained in front of a CTE-backed VLP lost its WHERE
        filter (the outer query returned `None` for "filters already in the CTE").

        Oracle (flights: LAX->SFO, SFO->JFK, JFK->LAX, ORD->ATL, ATL->LAX, LAX->ORD):
        c = Chicago (ORD); fixed ORD->ATL so a = Atlanta; VLP *1..2 from ATL reaches
        LAX (1 hop), then SFO and ORD (2 hops).
        """
        response = execute_cypher(
            """
            MATCH (c:Airport)-[:FLIGHT]->(a:Airport)-[:FLIGHT*1..2]->(b:Airport)
            WHERE c.city = 'Chicago'
            RETURN a.city AS a, b.city AS b
            ORDER BY b
            """,
            schema_name=denormalized_flights_graph["schema_name"]
        )

        assert_query_success(response)
        assert [(r['a'], r['b']) for r in response['results']] == [
            ('Atlanta', 'Chicago'),
            ('Atlanta', 'Los Angeles'),
            ('Atlanta', 'San Francisco'),
        ]

    def test_shared_node_filter_uses_the_fixed_hops_role_1170(self, denormalized_flights_graph):
        """#1170: `a` is the DESTINATION of the fixed hop but the ORIGIN of the VLP;
        its city must resolve against the fixed hop's role (`DestCityName`). Mapped
        against the VLP it became `OriginCityName` re-aliased onto the fixed hop —
        the wrong node's value, so nothing (or the wrong rows) came back.

        Oracle: a = Los Angeles is entered by JFK->LAX (c = New York) and ATL->LAX
        (c = Atlanta); the VLP from LAX (*1..2) reaches SFO, ORD (1 hop), then JFK and
        ATL (2 hops) — 2 starts x 4 ends = 8 rows.
        """
        response = execute_cypher(
            """
            MATCH (c:Airport)-[:FLIGHT]->(a:Airport)-[:FLIGHT*1..2]->(b:Airport)
            WHERE a.city = 'Los Angeles'
            RETURN c.city AS c, b.city AS b
            ORDER BY c, b
            """,
            schema_name=denormalized_flights_graph["schema_name"]
        )

        assert_query_success(response)
        assert [(r['c'], r['b']) for r in response['results']] == [
            ('Atlanta', 'Atlanta'),
            ('Atlanta', 'Chicago'),
            ('Atlanta', 'New York'),
            ('Atlanta', 'San Francisco'),
            ('New York', 'Atlanta'),
            ('New York', 'Chicago'),
            ('New York', 'New York'),
            ('New York', 'San Francisco'),
        ]

    # --- #1174: 2+ fixed hops in front of a CTE-backed VLP --------------------
    #
    # The VLP CTE was never joined to the chain (`JOIN flights AS t1 ON 1 = 1`), so
    # every chain row paired with every CTE row. Brute-force oracle over the fixture's
    # six flights, with Cypher relationship-uniqueness across a fixed hop and the VLP
    # (#1175): the rows must equal the oracle exactly.

    _FLIGHTS_1174 = [('LAX', 'SFO'), ('SFO', 'JFK'), ('JFK', 'LAX'),
                     ('ORD', 'ATL'), ('ATL', 'LAX'), ('LAX', 'ORD')]
    _CITY_1174 = {'LAX': 'Los Angeles', 'SFO': 'San Francisco', 'JFK': 'New York',
                  'ORD': 'Chicago', 'ATL': 'Atlanta'}

    @classmethod
    def _chain_rows_1174(cls, directions, lo, hi, cross_unique):
        """Rows (n0, ..., nk) of `n0 -fixed...-> n(k-1) -[*lo..hi]-> nk` as city tuples.

        `directions` = one 'out'/'in' per fixed hop; the VLP is outgoing. Fixed hops
        are pairwise edge-unique and the VLP is internally edge-unique; with
        `cross_unique` the VLP also may not reuse a fixed hop's edge (full Cypher).
        """
        edges = cls._FLIGHTS_1174
        rows = []

        def vlp(node, used, depth, forbidden):
            if depth >= lo:
                yield node
            if depth == hi:
                return
            for i, (f, t) in enumerate(edges):
                if f == node and i not in used and i not in forbidden:
                    yield from vlp(t, used | {i}, depth + 1, forbidden)

        def walk(h, node, bound, fixed_used):
            if h == len(directions):
                for end in vlp(node, frozenset(), 0, fixed_used if cross_unique else frozenset()):
                    rows.append(tuple(cls._CITY_1174[x] for x in bound + [end]))
                return
            for i, (f, t) in enumerate(edges):
                if i in fixed_used:
                    continue
                if directions[h] == 'out' and f == node:
                    walk(h + 1, t, bound + [t], fixed_used | {i})
                elif directions[h] == 'in' and t == node:
                    walk(h + 1, f, bound + [f], fixed_used | {i})

        for start in sorted({x for e in edges for x in e}):
            walk(0, start, [start], frozenset())
        return rows

    def _assert_chain_bracketed_1174(self, response, directions, ncols):
        """The rows equal the full-Cypher oracle (a fixed hop and the VLP never share an
        edge, #1175); this used to be a lower/upper bracket while #1175 was open."""
        from collections import Counter
        assert_query_success(response)
        names = ['n%d' % i for i in range(ncols)]
        got = Counter(tuple(r[n] for n in names) for r in response['results'])
        expected = Counter(self._chain_rows_1174(directions, 1, 2, True))
        assert got == expected, (
            f"missing: {sorted((expected - got).elements())}; "
            f"unexpected: {sorted((got - expected).elements())}"
        )

    def test_two_fixed_hops_before_vlp_are_joined_to_the_cte_1174(self, denormalized_flights_graph):
        """#1174: `(z)-[]->(c)-[]->(a)-[*1..2]->(b)`; the second hop must bind `a` to
        the CTE (`<hop>.dest = t.start_id`)."""
        response = execute_cypher(
            """
            MATCH (n0:Airport)-[:FLIGHT]->(n1:Airport)-[:FLIGHT]->(n2:Airport)
                  -[:FLIGHT*1..2]->(n3:Airport)
            RETURN n0.city AS n0, n1.city AS n1, n2.city AS n2, n3.city AS n3
            """,
            schema_name=denormalized_flights_graph["schema_name"]
        )
        self._assert_chain_bracketed_1174(response, ['out', 'out'], 4)

    def test_two_incoming_hops_before_vlp_are_joined_to_the_cte_1174(self, denormalized_flights_graph):
        """#1174: same, hops written backwards — the shared node is the FROM endpoint
        of the second hop, so the other edge column binds."""
        response = execute_cypher(
            """
            MATCH (n0:Airport)<-[:FLIGHT]-(n1:Airport)<-[:FLIGHT]-(n2:Airport)
                  -[:FLIGHT*1..2]->(n3:Airport)
            RETURN n0.city AS n0, n1.city AS n1, n2.city AS n2, n3.city AS n3
            """,
            schema_name=denormalized_flights_graph["schema_name"]
        )
        self._assert_chain_bracketed_1174(response, ['in', 'in'], 4)

    def test_variable_path_cte_uses_denormalized_props(self, denormalized_flights_graph):
        """Verify CTEs for variable paths use denormalized properties."""
        # Use sql_only mode to get SQL back
        import requests
        query = """
        USE denormalized_flights
        MATCH (origin:Airport)-[f:FLIGHT*1..2]->(dest:Airport)
        WHERE origin.city = 'Los Angeles'
        RETURN dest.city, COUNT(*) as path_count
        """
        
        response = requests.post(
            f"{CLICKGRAPH_URL}/query",
            json={"query": query, "sql_only": True},
            headers={"Content-Type": "application/json"}
        )
        
        assert response.status_code == 200, f"Query failed: {response.text}"
        result = response.json()
        
        # SQL should use denormalized columns in CTE
        sql = result.get('generated_sql', '')
        assert sql, f"No SQL found in result. Result keys: {result.keys()}"
        assert 'WITH RECURSIVE' in sql or 'WITH' in sql, f"No CTE found. SQL: {sql[:500]}"
        
        # Should reference denormalized columns (schema uses OriginCityName/DestCityName)
        assert 'OriginCityName' in sql or 'DestCityName' in sql, \
            f"SQL should contain denormalized columns. SQL: {sql[:500]}"


class TestPerformanceOptimization:
    """Test that denormalized queries are optimized (no unnecessary JOINs)."""
    
    def test_single_hop_no_joins(self, denormalized_flights_graph):
        """Single-hop query should not generate JOINs."""
        response = execute_cypher(
            """
            MATCH (origin:Airport)-[f:FLIGHT]->(dest:Airport)
            RETURN origin.city, dest.city, f.carrier
            """,
            schema_name=denormalized_flights_graph["schema_name"]
        )
        
        assert_query_success(response)
        
        sql = response.get('sql', '').upper()
        join_count = sql.count('JOIN')
        assert join_count == 0, f"Expected 0 JOINs, found {join_count} in: {sql}"
    
    def test_filtered_query_no_joins(self, denormalized_flights_graph):
        """Filtered query on denormalized properties should not generate JOINs."""
        response = execute_cypher(
            """
            MATCH (origin:Airport)-[f:FLIGHT]->(dest:Airport)
            WHERE origin.state = 'CA' AND dest.state = 'NY'
            RETURN origin.city, dest.city
            """,
            schema_name=denormalized_flights_graph["schema_name"]
        )
        
        assert_query_success(response)
        
        sql = response.get('sql', '').upper()
        assert 'JOIN' not in sql, f"Should not contain JOINs: {sql}"


class TestMixedDenormalizedAndNormal:
    """Test queries mixing denormalized and non-denormalized properties."""
    
    def test_denormalized_property_exists_not_in_node_table(self, denormalized_flights_graph):
        """Denormalized property should work even if not in node table."""
        # City is denormalized in flights, may or may not be in airports table
        response = execute_cypher(
            """
            MATCH (origin:Airport)-[f:FLIGHT]->(dest:Airport)
            WHERE origin.city = 'Los Angeles'
            RETURN COUNT(*) as count
            """,
            schema_name=denormalized_flights_graph["schema_name"]
        )
        
        assert_query_success(response)
        row = response['results'][0]
        assert row['count'] == 2  # Two flights from LAX


class TestEdgeCases:
    """Test edge cases for denormalized properties."""
    
    def test_property_in_both_from_and_to_nodes(self, denormalized_flights_graph):
        """Test property that appears in both from_node and to_node."""
        response = execute_cypher(
            """
            MATCH (origin:Airport)-[f:FLIGHT]->(dest:Airport)
            WHERE origin.state = dest.state
            RETURN origin.city, dest.city, origin.state as state
            ORDER BY origin.city
            """,
            schema_name=denormalized_flights_graph["schema_name"]
        )
        
        assert_query_success(response)
        # LAX -> SFO (both CA)
        assert_row_count(response, 1)
        
        row = response['results'][0]
        assert row['state'] == 'CA'
    
    def test_return_all_denormalized_properties(self, denormalized_flights_graph):
        """Return all available denormalized properties."""
        response = execute_cypher(
            """
            MATCH (origin:Airport)-[f:FLIGHT]->(dest:Airport)
            WHERE f.flight_num = 'AA100'
            RETURN origin.code, origin.city, origin.state,
                   dest.code, dest.city, dest.state,
                   f.flight_num, f.carrier, f.distance
            """,
            schema_name=denormalized_flights_graph["schema_name"]
        )
        
        assert_query_success(response)
        assert_row_count(response, 1)
        
        row = response['results'][0]
        assert row['origin.code'] == 'LAX'
        assert row['origin.city'] == 'Los Angeles'
        assert row['origin.state'] == 'CA'
        assert row['dest.code'] == 'SFO'
        assert row['dest.city'] == 'San Francisco'
        assert row['dest.state'] == 'CA'
        assert row['f.carrier'] == 'American Airlines'


class TestCompositeEdgeIds:
    """Test composite edge ID support in denormalized schemas."""
    
    def test_composite_edge_id_in_schema(self, denormalized_flights_graph):
        """Verify schema loads with composite edge_id."""
        # Schema loading should succeed (tested in fixture)
        # This test verifies no errors during query execution
        response = execute_cypher(
            """
            MATCH (origin:Airport)-[f:FLIGHT]->(dest:Airport)
            RETURN COUNT(*) as total
            """,
            schema_name=denormalized_flights_graph["schema_name"]
        )
        
        assert_query_success(response)
        row = response['results'][0]
        assert row['total'] == 6  # Total flights in test data
    
    def test_variable_path_with_composite_edge_id(self, denormalized_flights_graph):
        """Test variable-length paths respect composite edge IDs for cycle prevention."""
        response = execute_cypher(
            """
            MATCH (origin:Airport)-[f:FLIGHT*1..3]->(dest:Airport)
            WHERE origin.code = 'LAX'
            RETURN COUNT(DISTINCT dest.code) as dest_count
            """,
            schema_name=denormalized_flights_graph["schema_name"]
        )
        
        assert_query_success(response)
        # Should find paths to multiple destinations without cycles
        row = response['results'][0]
        assert row['dest_count'] >= 2  # At least SFO and ORD reachable
    
    def test_composite_id_prevents_duplicate_edges(self, denormalized_flights_graph):
        """Verify composite IDs are used for cycle prevention in CTEs."""
        response = execute_cypher(
            """
            MATCH path = (origin:Airport)-[f:FLIGHT*2]->(dest:Airport)
            WHERE origin.code = 'LAX' AND dest.code = 'ATL'
            RETURN length(path) as hops
            LIMIT 1
            """,
            schema_name=denormalized_flights_graph["schema_name"]
        )
        
        assert_query_success(response)
        
        # Check SQL for composite ID in cycle prevention
        sql = response.get('sql', '')
        if 'WITH RECURSIVE' in sql or 'WITH' in sql:
            # CTE should reference composite ID columns
            assert 'flight_id' in sql.lower() or 'flight_number' in sql.lower()

