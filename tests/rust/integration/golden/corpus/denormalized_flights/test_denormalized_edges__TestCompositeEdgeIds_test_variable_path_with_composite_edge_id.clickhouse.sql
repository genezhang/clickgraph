WITH RECURSIVE vlp_origin_dest AS (
    SELECT
        f.Origin as start_id,
        f.Dest as end_id,
        1 as hop_count,
        [f.Origin] as path_edges,
        [f.Origin, f.Dest] as path_nodes,
        [] as path_relationships,
        f.Dest as end_Dest
    FROM test_integration.flights AS f
    WHERE f.Origin = 'LAX' AND hop_count <= 3
    UNION ALL
    SELECT
        next.Origin as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        arrayConcat(vp.path_edges, [next.Origin]),
        arrayConcat(vp.path_nodes, [next.Dest]),
        [] as path_relationships,
        next.Dest as end_Dest
    FROM vlp_origin_dest vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 3 AND NOT has(vp.path_nodes, next.Dest)
)
SELECT 
      count(DISTINCT t.end_Dest) AS "dest_count"
FROM vlp_origin_dest AS t
