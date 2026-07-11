WITH RECURSIVE vlp_a_b AS (
    SELECT
        t0.Origin as start_id,
        t0.Dest as end_id,
        1 as hop_count,
        array(t0.Origin) as path_edges,
        array(t0.Origin, t0.Dest) as path_nodes,
        array('FLIGHT') as path_relationships
    FROM default.flights AS t0
    WHERE t0.OriginCityName != t0.DestCityName AND hop_count <= 5
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        concat(vp.path_edges, array(next.Origin)),
        concat(vp.path_nodes, array(next.Dest)),
        concat(vp.path_relationships, array('FLIGHT')) as path_relationships
    FROM vlp_a_b vp
    JOIN default.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 5 AND NOT array_contains(vp.path_nodes, next.Dest)
)
SELECT 
      t.hop_count AS `length(p)`
FROM vlp_a_b AS t
LIMIT 5