WITH RECURSIVE vlp_a1_a2 AS (
    SELECT
        t0.Origin as start_id,
        t0.Dest as end_id,
        1 as hop_count,
        array(t0.Origin) as path_edges,
        array(t0.Origin, t0.Dest) as path_nodes,
        t0.OriginCityName as start_OriginCityName,
        t0.OriginState as start_OriginState,
        t0.DestCityName as end_DestCityName,
        t0.DestState as end_DestState
    FROM test_integration.flights AS t0
    WHERE t0.Origin = 'JFK' AND hop_count <= 2
    UNION ALL
    SELECT
        next.Origin as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        concat(vp.path_edges, array(next.Origin)),
        concat(vp.path_nodes, array(next.Dest)),
        vp.start_OriginCityName as start_OriginCityName,
        vp.start_OriginState as start_OriginState,
        next.DestCityName as end_DestCityName,
        next.DestState as end_DestState
    FROM vlp_a1_a2 vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT array_contains(vp.path_nodes, next.Dest)
)
SELECT 
      t.start_OriginCityName AS `a1.city`, 
      t.start_OriginState AS `a1.state`, 
      t.end_DestCityName AS `a2.city`, 
      t.end_DestState AS `a2.state`
FROM vlp_a1_a2 AS t
LIMIT 5