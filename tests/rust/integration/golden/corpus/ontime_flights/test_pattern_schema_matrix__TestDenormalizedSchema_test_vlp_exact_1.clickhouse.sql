WITH RECURSIVE vlp_a_b_inner AS (
    SELECT
        t0.Origin as start_id,
        t0.Dest as end_id,
        1 as hop_count,
        [t0.Origin] as path_edges,
        [t0.Origin, t0.Dest] as path_nodes,
        [] as path_relationships
    FROM default.flights AS t0
    WHERE hop_count <= 2
    UNION ALL
    SELECT
        next.Origin as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        arrayConcat(vp.path_edges, [next.Origin]),
        arrayConcat(vp.path_nodes, [next.Dest]),
        [] as path_relationships
    FROM vlp_a_b_inner vp
    JOIN default.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT has(vp.path_nodes, next.Dest)
),
vlp_a_b AS (
    SELECT * FROM vlp_a_b_inner WHERE hop_count >= 2
)
SELECT 
      t.start_airport AS "a.airport", 
      t.end_airport AS "b.airport"
FROM vlp_a_b AS t
LIMIT 10