WITH RECURSIVE vlp_origin_dest AS (
    SELECT
        f.Origin as start_id,
        f.Dest as end_id,
        1 as hop_count,
        array(f.Origin) as path_edges,
        array(f.Origin, f.Dest) as path_nodes,
        array() as path_relationships,
        f.Dest as end_Dest
    FROM test_integration.flights AS f
    WHERE f.Origin = 'LAX' AND 1 <= 3
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        concat(vp.path_edges, array(next.Origin)),
        concat(vp.path_nodes, array(next.Dest)),
        array() as path_relationships,
        next.Dest as end_Dest
    FROM vlp_origin_dest vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 3 AND NOT array_contains(vp.path_nodes, next.Dest)
)
SELECT 
      count(DISTINCT t.end_Dest) AS `dest_count`
FROM vlp_origin_dest AS t
