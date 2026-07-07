WITH RECURSIVE vlp_a_b AS (
    SELECT
        t0.Origin as start_id,
        t0.Dest as end_id,
        1 as hop_count,
        array(t0.Origin) as path_edges,
        array(t0.Origin, t0.Dest) as path_nodes
    FROM default.flights AS t0
    WHERE hop_count <= 3
    UNION ALL
    SELECT
        next.Origin as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        concat(vp.path_edges, array(next.Origin)),
        concat(vp.path_nodes, array(next.Dest))
    FROM vlp_a_b vp
    JOIN default.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 3 AND NOT array_contains(vp.path_nodes, next.Dest)
)
SELECT 
      t.start_airport AS `a.airport`, 
      t.end_airport AS `b.airport`
FROM vlp_a_b AS t
LIMIT 10