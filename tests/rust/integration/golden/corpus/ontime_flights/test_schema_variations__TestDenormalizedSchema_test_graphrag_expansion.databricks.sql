WITH RECURSIVE vlp_a_dest AS (
    SELECT
        t0.Origin as start_id,
        t0.Dest as end_id,
        1 as hop_count,
        array(t0.Origin) as path_edges,
        array(t0.Origin, t0.Dest) as path_nodes,
        array('FLIGHT') as path_relationships,
        t0.Dest as end_Dest
    FROM default.flights AS t0
    WHERE t0.Origin = 'JFK' AND 1 <= 2
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        concat(vp.path_edges, array(next.Origin)),
        concat(vp.path_nodes, array(next.Dest)),
        concat(vp.path_relationships, array('FLIGHT')) as path_relationships,
        next.Dest as end_Dest
    FROM vlp_a_dest vp
    JOIN default.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT array_contains(vp.path_nodes, next.Dest)
)
SELECT 
      t.hop_count AS `length(p)`, 
      t.end_Dest AS `dest.code`
FROM vlp_a_dest AS t
LIMIT 10