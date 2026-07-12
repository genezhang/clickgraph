WITH RECURSIVE vlp_a1_a2 AS (
    SELECT
        t0.Origin as start_id,
        t0.Dest as end_id,
        1 as hop_count,
        array(t0.Origin) as path_edges,
        array(t0.Origin, t0.Dest) as path_nodes,
        array('FLIGHT') as path_relationships,
        t0.`DestCityName` as `end_DestCityName`
    FROM test_integration.flights AS t0
    WHERE t0.OriginCityName = 'New York' AND 1 <= 2
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        concat(vp.path_edges, array(next.Origin)),
        concat(vp.path_nodes, array(next.Dest)),
        concat(vp.path_relationships, array('FLIGHT')) as path_relationships,
        next.`DestCityName` as `end_DestCityName`
    FROM vlp_a1_a2 vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT array_contains(vp.path_nodes, next.Dest)
)
SELECT 
      t.end_DestCityName AS `a2.city`, 
      count(*) AS `count`
FROM vlp_a1_a2 AS t
GROUP BY t.end_DestCityName
