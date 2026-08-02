WITH RECURSIVE vlp_a_dest AS (
    SELECT
        t0.Origin as start_id,
        t0.Dest as end_id,
        1 as hop_count,
        array(struct(t0.flight_id, t0.flight_number)) as path_edges,
        array(t0.Origin, t0.Dest) as path_nodes,
        array() as path_relationships,
        t0.`DestCityName` as `end_DestCityName`,
        t0.`Dest` as `end_Dest`
    FROM default.flights AS t0
    WHERE t0.Origin = 'JFK' AND 1 <= 2
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        concat(vp.path_edges, array(struct(next.flight_id, next.flight_number))),
        concat(vp.path_nodes, array(next.Dest)),
        array() as path_relationships,
        next.`DestCityName` as `end_DestCityName`,
        next.`Dest` as `end_Dest`
    FROM vlp_a_dest vp
    JOIN default.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT array_contains(vp.path_edges, struct(next.flight_id, next.flight_number))
)
SELECT DISTINCT 
      t.end_Dest AS `dest.code`, 
      t.end_DestCityName AS `dest.city`
FROM vlp_a_dest AS t
LIMIT 20