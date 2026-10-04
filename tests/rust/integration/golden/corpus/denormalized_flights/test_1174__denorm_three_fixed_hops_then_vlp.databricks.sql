WITH RECURSIVE vlp_a_b AS (
    SELECT
        t0.Origin as start_id,
        t0.Dest as end_id,
        1 as hop_count,
        array(struct(t0.flight_id, t0.flight_number)) as path_edges,
        array(t0.Origin, t0.Dest) as path_nodes,
        array() as path_relationships
    FROM test_integration.flights AS t0
    WHERE 1 <= 2
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        concat(vp.path_edges, array(struct(next.flight_id, next.flight_number))),
        concat(vp.path_nodes, array(next.Dest)),
        array() as path_relationships
    FROM vlp_a_b vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT array_contains(vp.path_edges, struct(next.flight_id, next.flight_number))
)
SELECT 
      count(*) AS `count(*)`
FROM vlp_a_b AS t
JOIN test_integration.flights AS t1 ON 1 = 1
INNER JOIN test_integration.flights AS t2 ON t2.Origin = t1.Dest
INNER JOIN test_integration.flights AS t3 ON t3.Origin = t2.Dest AND t3.Dest = t.start_id
WHERE (((t3.flight_id <> t2.flight_id OR t3.flight_number <> t2.flight_number) AND (t3.flight_id <> t1.flight_id OR t3.flight_number <> t1.flight_number)) AND (t2.flight_id <> t1.flight_id OR t2.flight_number <> t1.flight_number))
