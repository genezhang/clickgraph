WITH RECURSIVE with_c_z_cte_0 AS (SELECT 
      t0.Dest AS `p1_c_code`, 
      t0.Origin AS `p1_z_code`
FROM test_integration.flights AS t0
), 
vlp_z_c AS (
    SELECT
        t1.Origin as start_id,
        t1.Dest as end_id,
        1 as hop_count,
        array(struct(t1.flight_id, t1.flight_number)) as path_edges,
        array(t1.Origin, t1.Dest) as path_nodes,
        array() as path_relationships
    FROM test_integration.flights AS t1
    WHERE 1 <= 2
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        concat(vp.path_edges, array(struct(next.flight_id, next.flight_number))),
        concat(vp.path_nodes, array(next.Dest)),
        array() as path_relationships
    FROM vlp_z_c vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT array_contains(vp.path_edges, struct(next.flight_id, next.flight_number))
)
SELECT 
      c_z.p1_z_code AS `z.code`, 
      c_z.p1_c_code AS `c.code`
FROM vlp_z_c AS t
INNER JOIN with_c_z_cte_0 AS c_z ON string(t.end_id) = string(c_z.p1_c_end_id)
