WITH RECURSIVE vlp_b_a AS (
    SELECT
        t0.Origin as start_id,
        t0.Dest as end_id,
        1 as hop_count,
        array(struct(t0.flight_id, t0.flight_number)) as path_edges,
        array(t0.Origin, t0.Dest) as path_nodes,
        array() as path_relationships,
        t0.`Dest` as `end_Dest`
    FROM test_integration.flights AS t0
    WHERE 1 <= 2
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        concat(vp.path_edges, array(struct(next.flight_id, next.flight_number))),
        concat(vp.path_nodes, array(next.Dest)),
        array() as path_relationships,
        next.`Dest` as `end_Dest`
    FROM vlp_b_a vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT array_contains(vp.path_edges, struct(next.flight_id, next.flight_number))
)
SELECT 
      t.end_Dest AS `a.code`, 
      t1.Origin AS `d.code`
FROM vlp_b_a AS t
INNER JOIN test_integration.flights AS t2 ON t2.Dest = t.start_id
INNER JOIN test_integration.flights AS t1 ON t1.Dest = t2.Origin
WHERE (t1.flight_id <> t2.flight_id OR t1.flight_number <> t2.flight_number)
