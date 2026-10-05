WITH RECURSIVE with_c_cte_0 AS (SELECT 
      t0.Dest AS `p1_c_code`
FROM test_integration.flights AS t0
), 
with_a_c_cte_1 AS (SELECT 
      c.p1_c_code AS `p1_c_code`, 
      t1.Dest AS `p1_a_code`
FROM with_c_cte_0 AS c
INNER JOIN test_integration.flights AS t1 ON t1.Origin = c.code
), 
vlp_a_b AS (
    SELECT
        t2.Origin as start_id,
        t2.Dest as end_id,
        1 as hop_count,
        array(struct(t2.flight_id, t2.flight_number)) as path_edges,
        array(t2.Origin, t2.Dest) as path_nodes,
        array() as path_relationships,
        t2.`Dest` as `end_Dest`
    FROM test_integration.flights AS t2
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
    FROM vlp_a_b vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT array_contains(vp.path_edges, struct(next.flight_id, next.flight_number))
)
SELECT 
      a_c.p1_c_code AS `c.code`, 
      a_c.p1_a_code AS `a.code`, 
      t.end_Dest AS `b.code`
FROM vlp_a_b AS t
INNER JOIN with_a_c_cte_1 AS a_c ON string(t.start_id) = string(a_c.p1_a_start_id)
