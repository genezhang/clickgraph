WITH RECURSIVE vlp_origin_dest_inner AS (
    SELECT
        f.Origin as start_id,
        f.Dest as end_id,
        1 as hop_count,
        [f.Origin] as path_edges,
        [f.Origin, f.Dest] as path_nodes,
        ['FLIGHT'] as path_relationships,
        f.OriginCityName as start_OriginCityName,
        f.DestCityName as end_DestCityName,
        f.Dest as end_Dest
    FROM test_integration.flights AS f
    WHERE f.Origin = 'LAX' AND hop_count <= 2
    UNION ALL
    SELECT
        next.Origin as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        arrayConcat(vp.path_edges, [next.Origin]),
        arrayConcat(vp.path_nodes, [next.Dest]),
        arrayConcat(vp.path_relationships, ['FLIGHT']) as path_relationships,
        vp.start_OriginCityName as start_OriginCityName,
        next.DestCityName as end_DestCityName,
        next.Dest as end_Dest
    FROM vlp_origin_dest_inner vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT has(vp.path_nodes, next.Dest)
),
vlp_origin_dest AS (
    SELECT * FROM vlp_origin_dest_inner WHERE end_Dest = 'ATL'
)
SELECT 
      t.start_OriginCityName AS "origin.city", 
      t.end_DestCityName AS "dest.city", 
      t.hop_count AS "hops"
FROM vlp_origin_dest AS t
ORDER BY hops ASC
LIMIT 1