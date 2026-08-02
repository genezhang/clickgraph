WITH RECURSIVE vlp_a_b_inner AS (
    SELECT
        t0.Origin as start_id,
        t0.Dest as end_id,
        1 as hop_count,
        array(struct(t0.flight_id, t0.flight_number)) as path_edges,
        array(t0.Origin, t0.Dest) as path_nodes,
        array() as path_relationships,
        t0.`Origin` as `start_Origin`,
        t0.`Dest` as `end_Dest`
    FROM test_integration.flights AS t0
    WHERE 1 <= 3
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        concat(vp.path_edges, array(struct(next.flight_id, next.flight_number))),
        concat(vp.path_nodes, array(next.Dest)),
        array() as path_relationships,
        vp.`start_Origin` as `start_Origin`,
        next.`Dest` as `end_Dest`
    FROM vlp_a_b_inner vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 3 AND NOT array_contains(vp.path_edges, struct(next.flight_id, next.flight_number))
),
vlp_a_b AS (
    SELECT * FROM vlp_a_b_inner WHERE hop_count >= 2
)
SELECT 
      t.start_Origin AS `a.code`, 
      t.end_Dest AS `b.code`
FROM vlp_a_b AS t
