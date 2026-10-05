WITH RECURSIVE with_c_cte_0 AS (SELECT 
      t0.Dest AS `p1_c_code`
FROM test_integration.flights AS t0
), 
vlp_a_b_inner AS (
    SELECT
        t1.Origin as start_id,
        t1.Dest as end_id,
        1 as hop_count,
        array(struct(t1.flight_id, t1.flight_number)) as path_edges,
        array(t1.Origin, t1.Dest) as path_nodes,
        array() as path_relationships,
        t1.`Dest` as `end_Dest`
    FROM test_integration.flights AS t1
    WHERE 1 <= 2
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        concat(vp.path_edges, array(struct(next.flight_id, next.flight_number))),
        concat(vp.path_nodes, array(next.Dest)),
        array() as path_relationships,
        next.`Dest` as `end_Dest`
    FROM vlp_a_b_inner vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT array_contains(vp.path_edges, struct(next.flight_id, next.flight_number))
),
vlp_a_b AS (
    SELECT * FROM vlp_a_b_inner WHERE hop_count >= 2
)
SELECT 
      c.p1_c_code AS `c.code`, 
      t.end_Dest AS `b.code`
FROM vlp_a_b AS t
INNER JOIN with_c_cte_0 AS c ON 1 = 1
INNER JOIN test_integration.flights AS t2 ON t2.Dest = t.start_id AND t2.Origin = c.p1_c_code
WHERE NOT array_contains(t.path_edges, struct(t2.flight_id, t2.flight_number))
