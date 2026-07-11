WITH RECURSIVE vlp_a_b AS (
    SELECT
        t0.origin_code as start_id,
        t0.dest_code as end_id,
        1 as hop_count,
        array(t0.origin_code) as path_edges,
        array(t0.origin_code, t0.dest_code) as path_nodes,
        array('FLIGHT') as path_relationships
    FROM db_denormalized.flights_denorm AS t0
    WHERE 1 <= 2
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.dest_code as end_id,
        vp.hop_count + 1,
        concat(vp.path_edges, array(next.origin_code)),
        concat(vp.path_nodes, array(next.dest_code)),
        concat(vp.path_relationships, array('FLIGHT')) as path_relationships
    FROM vlp_a_b vp
    JOIN db_denormalized.flights_denorm next ON next.origin_code = vp.end_id
    WHERE vp.hop_count < 2 AND NOT array_contains(vp.path_nodes, next.dest_code)
)
SELECT 
      struct(t.path_nodes, t.path_relationships, t.hop_count) AS `p`
FROM vlp_a_b AS t
LIMIT 5