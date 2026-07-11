WITH RECURSIVE vlp_origin_dest AS (
    SELECT
        f.Origin as start_id,
        f.Dest as end_id,
        1 as hop_count,
        array(f.Origin) as path_edges,
        array(f.Origin, f.Dest) as path_nodes,
        array() as path_relationships,
        f.DestCityName as end_DestCityName
    FROM test_integration.flights AS f
    WHERE f.OriginCityName = 'Los Angeles' AND hop_count <= 2
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        concat(vp.path_edges, array(next.Origin)),
        concat(vp.path_nodes, array(next.Dest)),
        array() as path_relationships,
        next.DestCityName as end_DestCityName
    FROM vlp_origin_dest vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT array_contains(vp.path_nodes, next.Dest)
)
SELECT 
      t.end_DestCityName AS `dest.city`, 
      count(*) AS `path_count`
FROM vlp_origin_dest AS t
GROUP BY t.end_DestCityName
