WITH RECURSIVE vlp_a1_a2 AS (
    SELECT
        t0.Origin as start_id,
        t0.Dest as end_id,
        1 as hop_count,
        [t0.Origin] as path_edges,
        [t0.Origin, t0.Dest] as path_nodes,
        ['FLIGHT'] as path_relationships,
        t0.OriginCityName as start_OriginCityName
    FROM test_integration.flights AS t0
    WHERE t0.Origin IN ('JFK', 'LAX') AND hop_count <= 2
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        arrayConcat(vp.path_edges, [next.Origin]),
        arrayConcat(vp.path_nodes, [next.Dest]),
        arrayConcat(vp.path_relationships, ['FLIGHT']) as path_relationships,
        vp.start_OriginCityName as start_OriginCityName
    FROM vlp_a1_a2 vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT has(vp.path_nodes, next.Dest)
)
SELECT 
      t.start_OriginCityName AS "a1.city", 
      count(*) AS "path_count"
FROM vlp_a1_a2 AS t
GROUP BY t.start_OriginCityName
