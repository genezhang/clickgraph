WITH RECURSIVE with_c_cte_0 AS (SELECT 
      t0.Dest AS "p1_c_code"
FROM test_integration.flights AS t0
), 
vlp_n2_n1 AS (
    SELECT
        t1.Origin as start_id,
        t1.Dest as end_id,
        1 as hop_count,
        [tuple(t1.flight_id, t1.flight_number)] as path_edges,
        [t1.Origin, t1.Dest] as path_nodes,
        [] as path_relationships
    FROM test_integration.flights AS t1
    WHERE 1 <= 2
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        arrayConcat(vp.path_edges, [tuple(next.flight_id, next.flight_number)]),
        arrayConcat(vp.path_nodes, [next.Dest]),
        [] as path_relationships
    FROM vlp_n2_n1 vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT has(vp.path_edges, tuple(next.flight_id, next.flight_number))
)
SELECT 
      c.p1_c_code AS "c.code", 
      t2.Origin AS "n3.code"
FROM vlp_n2_n1 AS t
INNER JOIN test_integration.flights AS t3 ON t3.Dest = t.end_id
INNER JOIN test_integration.flights AS t2 ON t2.Dest = t.start_id
INNER JOIN with_c_cte_0 AS c ON t2.flight_id <> t3.flight_id OR t2.flight_number <> t3.flight_number
WHERE (NOT has(t.path_edges, tuple(t2.flight_id, t2.flight_number)) AND NOT has(t.path_edges, tuple(t3.flight_id, t3.flight_number)))
