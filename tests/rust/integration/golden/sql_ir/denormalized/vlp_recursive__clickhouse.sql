WITH RECURSIVE vlp_a_b AS (
    SELECT
        t0.origin_code as start_id,
        t0.dest_code as end_id,
        1 as hop_count,
        [t0.origin_code] as path_edges,
        [t0.origin_code, t0.dest_code] as path_nodes,
        t0.dest_code as end_dest_code
    FROM db_denormalized.flights_denorm AS t0
    WHERE hop_count <= 2
    UNION ALL
    SELECT
        next.origin_code as start_id,
        next.dest_code as end_id,
        vp.hop_count + 1,
        arrayConcat(vp.path_edges, [next.origin_code]),
        arrayConcat(vp.path_nodes, [next.dest_code]),
        next.dest_code as end_dest_code
    FROM vlp_a_b vp
    JOIN db_denormalized.flights_denorm next ON next.origin_code = vp.end_id
    WHERE vp.hop_count < 2 AND NOT has(vp.path_nodes, next.dest_code)
)
SELECT 
      t.end_dest_code AS "b.code"
FROM vlp_a_b AS t
