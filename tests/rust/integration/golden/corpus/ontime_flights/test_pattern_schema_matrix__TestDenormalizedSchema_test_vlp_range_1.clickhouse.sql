WITH RECURSIVE vlp_a_b AS (
    SELECT
        t0.Origin as start_id,
        t0.Dest as end_id,
        1 as hop_count,
        [t0.Origin] as path_edges,
        [t0.Origin, t0.Dest] as path_nodes,
        [] as path_relationships,
        t0.OriginCityName as start_OriginCityName,
        t0.DestCityName as end_DestCityName
    FROM default.flights AS t0
    WHERE hop_count <= 3
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        arrayConcat(vp.path_edges, [next.Origin]),
        arrayConcat(vp.path_nodes, [next.Dest]),
        [] as path_relationships,
        vp.start_OriginCityName as start_OriginCityName,
        next.DestCityName as end_DestCityName
    FROM vlp_a_b vp
    JOIN default.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 3 AND NOT has(vp.path_nodes, next.Dest)
)
SELECT 
      t.start_OriginCityName AS "a.city", 
      t.end_DestCityName AS "b.city"
FROM vlp_a_b AS t
LIMIT 10