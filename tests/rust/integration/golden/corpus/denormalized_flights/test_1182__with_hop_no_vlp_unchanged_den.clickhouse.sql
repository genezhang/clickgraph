WITH RECURSIVE vlp_a_b AS (
    SELECT
        t0.Origin as start_id,
        t0.Dest as end_id,
        1 as hop_count,
        [tuple(t0.flight_id, t0.flight_number)] as path_edges,
        [t0.Origin, t0.Dest] as path_nodes,
        [] as path_relationships
    FROM test_integration.flights AS t0
    WHERE 1 <= 2
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        arrayConcat(vp.path_edges, [tuple(next.flight_id, next.flight_number)]),
        arrayConcat(vp.path_nodes, [next.Dest]),
        [] as path_relationships
    FROM vlp_a_b vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT has(vp.path_edges, tuple(next.flight_id, next.flight_number))
), 
with_b_cte_0 AS (SELECT 
      end_code AS "p1_b_code"
FROM vlp_a_b AS t
)
SELECT 
      b.p1_b_code AS "b.code", 
      t1.Dest AS "d.code"
FROM test_integration.flights AS t1
INNER JOIN with_b_cte_0 AS b ON t1.Origin = b.p1_b_code
