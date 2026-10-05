WITH RECURSIVE vlp_a_b AS (
    SELECT
        t0.Origin as start_id,
        t0.Dest as end_id,
        1 as hop_count,
        array(struct(t0.flight_id, t0.flight_number)) as path_edges,
        array(t0.Origin, t0.Dest) as path_nodes,
        array() as path_relationships,
        t0.`Dest` as `end_Dest`
    FROM test_integration.flights AS t0
    WHERE lower(t0.OriginCityName) = 'atlanta' AND 1 <= 2
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        concat(vp.path_edges, array(struct(next.flight_id, next.flight_number))),
        concat(vp.path_nodes, array(next.Dest)),
        array() as path_relationships,
        next.`Dest` as `end_Dest`
    FROM vlp_a_b vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT array_contains(vp.path_edges, struct(next.flight_id, next.flight_number))
)
SELECT 
      t.end_Dest AS `b.code`
FROM vlp_a_b AS t
