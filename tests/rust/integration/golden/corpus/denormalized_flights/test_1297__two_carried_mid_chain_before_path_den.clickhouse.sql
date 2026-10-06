WITH RECURSIVE with_c_z_cte_0 AS (SELECT 
      t0.Dest AS "p1_c_code", 
      t0.Origin AS "p1_z_code"
FROM test_integration.flights AS t0
), 
vlp_c_b AS (
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
    FROM vlp_c_b vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT has(vp.path_edges, tuple(next.flight_id, next.flight_number))
)
SELECT 
      count(*) AS "k"
FROM vlp_c_b AS t
INNER JOIN with_c_z_cte_0 AS c_z ON toString(t.start_id) = toString(c_z.p1_c_code)
INNER JOIN test_integration.flights AS t2 ON t2.Dest = c_z.p1_z_code
INNER JOIN test_integration.flights AS t3 ON t3.Origin = t2.Dest AND t3.Dest = t.start_id
WHERE ((NOT has(t.path_edges, tuple(t3.flight_id, t3.flight_number)) AND NOT has(t.path_edges, tuple(t2.flight_id, t2.flight_number))) AND (t3.flight_id <> t2.flight_id OR t3.flight_number <> t2.flight_number))
