WITH RECURSIVE with_c_cte_0 AS (SELECT 
      t0.Dest AS "p1_c_code"
FROM test_integration.flights AS t0
), 
with_a_cte_1 AS (SELECT 
      t1.Dest AS "p1_a_code"
FROM with_c_cte_0 AS c
INNER JOIN test_integration.flights AS t1 ON t1.Origin = c.p1_c_code
), 
vlp_b_d AS (
    SELECT
        t2.Origin as start_id,
        t2.Dest as end_id,
        1 as hop_count,
        [tuple(t2.flight_id, t2.flight_number)] as path_edges,
        [t2.Origin, t2.Dest] as path_nodes,
        [] as path_relationships,
        t2."Dest" as "end_Dest"
    FROM test_integration.flights AS t2
    WHERE 1 <= 2
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        arrayConcat(vp.path_edges, [tuple(next.flight_id, next.flight_number)]),
        arrayConcat(vp.path_nodes, [next.Dest]),
        [] as path_relationships,
        next."Dest" as "end_Dest"
    FROM vlp_b_d vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT has(vp.path_edges, tuple(next.flight_id, next.flight_number))
)
SELECT 
      a.p1_a_code AS "a.code", 
      t.end_Dest AS "d.code"
FROM vlp_b_d AS t
INNER JOIN with_a_cte_1 AS a ON 1 = 1
INNER JOIN test_integration.flights AS t3 ON t3.Dest = t.start_id AND t3.Origin = a.p1_a_code
WHERE NOT has(t.path_edges, tuple(t3.flight_id, t3.flight_number))
