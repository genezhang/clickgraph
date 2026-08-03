WITH RECURSIVE vlp_a_a_inner AS (
    SELECT
        t0.Origin as start_id,
        t0.Dest as end_id,
        1 as hop_count,
        [tuple(t0.flight_id, t0.flight_number)] as path_edges,
        [t0.Origin, t0.Dest] as path_nodes,
        [] as path_relationships
    FROM test_integration.flights AS t0
    WHERE 1 <= 3
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        arrayConcat(vp.path_edges, [tuple(next.flight_id, next.flight_number)]),
        arrayConcat(vp.path_nodes, [next.Dest]),
        [] as path_relationships
    FROM vlp_a_a_inner vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 3 AND NOT has(vp.path_edges, tuple(next.flight_id, next.flight_number))
),
vlp_a_a AS (
    SELECT * FROM vlp_a_a_inner WHERE hop_count >= 2
)
SELECT 
      count(*) AS "count(*)"
FROM vlp_a_a AS t
WHERE t.start_id = t.end_id
