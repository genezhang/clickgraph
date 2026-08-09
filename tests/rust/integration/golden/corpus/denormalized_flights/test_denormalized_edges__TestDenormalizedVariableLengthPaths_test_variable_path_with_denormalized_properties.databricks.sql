WITH RECURSIVE vlp_origin_dest_inner AS (
    SELECT
        f.Origin as start_id,
        f.Dest as end_id,
        1 as hop_count,
        array(struct(f.flight_id, f.flight_number)) as path_edges,
        array(f.Origin, f.Dest) as path_nodes,
        array('FLIGHT') as path_relationships,
        f.`OriginCityName` as `start_OriginCityName`,
        f.`DestCityName` as `end_DestCityName`,
        f.`Dest` as `end_Dest`
    FROM test_integration.flights AS f
    WHERE f.Origin = 'LAX' AND 1 <= 2
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        concat(vp.path_edges, array(struct(next.flight_id, next.flight_number))),
        concat(vp.path_nodes, array(next.Dest)),
        concat(vp.path_relationships, array('FLIGHT')) as path_relationships,
        vp.`start_OriginCityName` as `start_OriginCityName`,
        next.`DestCityName` as `end_DestCityName`,
        next.`Dest` as `end_Dest`
    FROM vlp_origin_dest_inner vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT array_contains(vp.path_edges, struct(next.flight_id, next.flight_number))
),
vlp_origin_dest AS (
    SELECT * FROM vlp_origin_dest_inner WHERE end_Dest = 'ATL'
)
SELECT 
      t.start_OriginCityName AS `origin.city`, 
      t.end_DestCityName AS `dest.city`, 
      t.hop_count AS `hops`
FROM vlp_origin_dest AS t
ORDER BY hops ASC NULLS LAST
LIMIT 1