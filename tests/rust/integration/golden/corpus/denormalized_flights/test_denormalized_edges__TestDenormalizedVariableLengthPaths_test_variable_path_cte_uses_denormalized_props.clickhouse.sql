WITH RECURSIVE vlp_origin_dest AS (
    SELECT
        f.Origin as start_id,
        f.Dest as end_id,
        1 as hop_count,
        [f.Origin] as path_edges,
        [f.Origin, f.Dest] as path_nodes,
        f.DestCityName as end_DestCityName
    FROM test_integration.flights AS f
    WHERE f.OriginCityName = 'Los Angeles' AND hop_count <= 2
    UNION ALL
    SELECT
        next.Origin as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        arrayConcat(vp.path_edges, [next.Origin]),
        arrayConcat(vp.path_nodes, [next.Dest]),
        next.DestCityName as end_DestCityName
    FROM vlp_origin_dest vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT has(vp.path_nodes, next.Dest)
)
SELECT 
      t.end_DestCityName AS "dest.city", 
      count(*) AS "path_count"
FROM vlp_origin_dest AS t
GROUP BY t.end_DestCityName
